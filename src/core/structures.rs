////////////////////////////////////////////////////////////////////
// Structure trait
////////////////////////////////////////////////////////////////////

use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};

use crate::codec::Codec;
use crate::errors::Errors::Exact;
use crate::machine::Machine;
use crate::model_row_collection::ModelRowCollection;
use crate::parameter::Parameter;
use crate::rows::Row;
use crate::table_columns::Column;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::*;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Represents a JSON-like data structure
pub trait Structure {
    fn contains(&self, name: &str) -> bool {
        self.get_tuples().iter()
            .find(|(k, _)| *k == *name)
            .is_some()
    }

    /// Encodes the [Structure] into a byte vector
    fn encode(&self) -> std::io::Result<Vec<u8>>;

    fn get(&self, name: &str) -> TypedValue {
        self.get_tuples().iter()
            .find(|(my_name, _)| *my_name == *name)
            .map(|(_, v)| v.to_owned())
            .unwrap_or(Undefined)
    }

    fn get_parameters(&self) -> Vec<Parameter>;

    fn get_tuples(&self) -> Vec<(String, TypedValue)>;

    fn get_values(&self) -> Vec<TypedValue>;

    fn pollute(&self, ms0: Machine) -> Machine {
        // add all scope variables (includes functions)
        let tuples = self.get_tuples();
        let ms1 = tuples.iter()
            .fold(ms0, |ms, (name, value)| {
                ms.with_variable(name, value.to_owned())
            });
        // add self-reference
        ms1.with_variable("self", StructureSoft(SoftStructure::from_tuples(tuples)))
    }

    fn to_code(&self) -> String;

    fn to_hash_map(&self) -> HashMap<String, Value> {
        self.get_tuples().iter()
            .map(|(name, value)| (name.to_string(), value.to_json()))
            .fold(HashMap::new(), |mut m, (name, value)| {
                m.insert(name, value);
                m
            })
    }

    fn to_json(&self) -> Value;

    fn to_row(&self) -> Row {
        Row::new(0, self.get_values())
    }

    fn to_table(&self) -> ModelRowCollection {
        let row = self.to_row();
        let columns = Column::from_parameters(&self.get_parameters())
            .unwrap_or(Vec::new());
        ModelRowCollection::from_rows(columns, vec![row])
    }
}

////////////////////////////////////////////////////////////////////
// HardStructure class
////////////////////////////////////////////////////////////////////

/// Represents a schema-based JSON-like data structure
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HardStructure {
    columns: Vec<Column>,
    values: Vec<TypedValue>,
    variables: HashMap<String, TypedValue>,
}

impl HardStructure {

    ////////////////////////////////////////////////////////////////////
    //  Constructors
    ////////////////////////////////////////////////////////////////////

    pub fn empty() -> Self {
        Self::from_fields(Vec::new(), Vec::new(), HashMap::new())
    }

    pub fn new(fields: Vec<Column>, values: Vec<TypedValue>) -> Self {
        Self::from_fields(fields, values, HashMap::new())
    }

    ////////////////////////////////////////////////////////////////////
    //  Static Methods
    ////////////////////////////////////////////////////////////////////

    pub fn decode(bytes: Vec<u8>) -> std::io::Result<Self> {
        Codec::unwrap_as_result(bincode::deserialize(bytes.as_slice()))
    }

    pub fn from_fields(
        fields: Vec<Column>,
        values: Vec<TypedValue>,
        variables: HashMap<String, TypedValue>,
    ) -> Self {
        let my_values = match (fields.len(), values.len()) {
            // more fields than values?
            (f, v) if f > v => {
                let mut my_values = values;
                for n in v..f { my_values.push(fields[n].get_default_value()) }
                my_values
            }
            // more values than fields?
            (f, v) if f < v => values[0..f].to_vec(),
            // same length
            (..) => values
        };
        Self { columns: fields, values: my_values, variables }
    }

    pub fn from_parameters(
        parameters: &Vec<Parameter>
    ) -> std::io::Result<HardStructure> {
        let fields = Column::from_parameters(parameters)?;
        let values = parameters.iter().map(|c|
            c.get_default_value().to_owned()
                .map(|s| TypedValue::wrap_value(s.as_str())
                    .unwrap_or_else(|err| ErrorValue(Exact(err.to_string()))))
                .unwrap_or(Null)
        ).collect();
        Ok(Self::new(fields, values))
    }

    pub fn from_parameters_and_values(
        columns: &Vec<Parameter>,
        values: Vec<TypedValue>,
    ) -> std::io::Result<HardStructure> {
        let fields = Column::from_parameters(columns)?;
        Ok(Self::new(fields, values))
    }

    pub fn from_row(columns: Vec<Column>, row: &Row) -> HardStructure {
        Self::new(columns, row.get_values())
    }

    ////////////////////////////////////////////////////////////////////
    //  Instance Methods
    ////////////////////////////////////////////////////////////////////

    pub fn get_columns(&self) -> Vec<Column> {
        self.columns.to_owned()
    }

    pub fn with_variable(&self, name: &str, value: TypedValue) -> Self {
        let mut hs = self.clone();
        hs.variables.insert(name.into(), value);
        hs
    }
}

impl Display for HardStructure {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mapping = self.columns.iter().zip(self.values.iter())
            .map(|(c, v)| format!("\"{}\":{}", c.get_name(), v.to_code()))
            .collect::<Vec<_>>();
        write!(f, "{{{}}}", mapping.join(","))
    }
}

impl Structure for HardStructure {
    /// Encodes the [HardStructure] into a byte vector
    fn encode(&self) -> std::io::Result<Vec<u8>> {
        Codec::unwrap_as_result(bincode::serialize(self))
    }

    fn get_parameters(&self) -> Vec<Parameter> {
        Parameter::from_columns(&self.columns)
    }

    fn get_tuples(&self) -> Vec<(String, TypedValue)> {
        let mut tuples = Vec::new();
        tuples.extend(self.variables.iter()
            .map(|(c, v)| (c.to_string(), v.to_owned()))
            .collect::<Vec<_>>());
        tuples.extend(self.columns.iter().zip(self.values.iter())
            .map(|(c, v)| (c.get_name().to_string(), v.to_owned()))
            .collect::<Vec<_>>());
        tuples
    }

    fn get_values(&self) -> Vec<TypedValue> {
        self.values.to_owned()
    }

    /// ex: struct(symbol: String(8), exchange: String(8), last_sale: f64)
    fn to_code(&self) -> String {
        let inside = self.columns.iter().zip(self.values.iter())
            .map(|(field, value)| {
                let field_name = field.get_name();
                let type_name = field.get_data_type()
                    .to_type_declaration()
                    .map_or_else(String::new, |name| format!(": {name}"));
                let field_value = match value {
                    Null | Undefined =>
                        match field.get_default_value() {
                            Null | Undefined => String::new(),
                            other => other.to_code()
                        }
                    other => format!(" = {}", other.to_code())
                };
                format!("{field_name}{type_name}{field_value}")
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!("struct({inside})")
    }

    fn to_json(&self) -> Value {
        let mapping = self.columns.iter().zip(self.values.iter())
            .map(|(f, v)|
                (f.get_name().to_string(), match f.get_default_value() {
                    Null | Undefined => v.to_json(),
                    v => v.to_json()
                }))
            .fold(Map::new(), |mut m, (name, value)| {
                m.insert(name, value);
                m
            });
        Value::Object(mapping)
    }

    fn to_table(&self) -> ModelRowCollection {
        ModelRowCollection::from_rows(self.columns.clone(), vec![self.to_row()])
    }
}

////////////////////////////////////////////////////////////////////
// SoftStructure class
////////////////////////////////////////////////////////////////////

/// Represents a structure JSON-like data structure
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SoftStructure {
    tuples: Vec<(String, TypedValue)>,
}

impl SoftStructure {
    pub fn decode(bytes: Vec<u8>) -> std::io::Result<Self> {
        Codec::unwrap_as_result(bincode::deserialize(bytes.as_slice()))
    }

    pub fn empty() -> Self {
        Self { tuples: Vec::new() }
    }

    pub fn from_tuples(tuples: Vec<(String, TypedValue)>) -> Self {
        Self { tuples }
    }

    pub fn new(tuples: &Vec<(&str, TypedValue)>) -> Self {
        let tuples = tuples.iter()
            .map(|(k, v)| (k.to_string(), v.to_owned()))
            .collect::<Vec<_>>();
        Self { tuples }
    }

    pub fn with_variable(&self, name: &str, value: TypedValue) -> Self {
        let tup_maybe = self.tuples.iter()
            .find(|(k, v)| *k == *name);
        let new_tuples = match tup_maybe {
            None => {
                let mut new_tuples = self.tuples.clone();
                new_tuples.push((name.into(), value));
                new_tuples
            }
            Some(_) =>
                self.tuples.iter().map(|(k, v)| if *k == *name {
                    (k.to_string(), value.clone())
                } else {
                    (k.to_string(), v.to_owned())
                }).collect::<Vec<_>>()
        };
        Self {
            tuples: new_tuples,
        }
    }
}

impl Display for SoftStructure {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_code())
    }
}

impl Structure for SoftStructure {
    /// Encodes the [SoftStructure] into a byte vector
    fn encode(&self) -> std::io::Result<Vec<u8>> {
        Codec::unwrap_as_result(bincode::serialize(self))
    }

    fn get_parameters(&self) -> Vec<Parameter> {
        self.tuples.iter()
            .map(|(k, v)| Parameter::from_tuple(k, v.to_owned()))
            .collect::<Vec<_>>()
    }

    fn get_tuples(&self) -> Vec<(String, TypedValue)> {
        self.tuples.clone()
    }

    fn get_values(&self) -> Vec<TypedValue> {
        self.tuples.iter()
            .map(|(_, v)| v.to_owned())
            .collect()
    }

    fn to_code(&self) -> String {
        format!("{{{}}}", self.tuples.iter()
            .map(|(name, value)| (name.to_string(), value.to_code()))
            .map(|(k, v)| format!("{k}: {v}"))
            .collect::<Vec<_>>()
            .join(", "))
    }

    fn to_json(&self) -> Value {
        let mapping = self.tuples.iter()
            .map(|(name, value)| (name.to_string(), value.to_json()))
            .fold(Map::new(), |mut m, (name, value)| {
                m.insert(name, value);
                m
            });
        Value::Object(mapping)
    }
}

/// Unit tests
#[cfg(test)]
mod structure_tests {}

/// Unit tests
#[cfg(test)]
mod hard_structure_tests {
    use crate::data_types::DataType::*;
    use crate::data_types::SizeTypes;
    use crate::number_kind::NumberKind::F64Kind;
    use crate::numbers::NumberValue::F64Value;
    use crate::structures::{HardStructure, Structure};
    use crate::table_columns::Column;
    use crate::testdata::{make_quote, make_quote_columns, make_quote_parameters};
    use crate::typed_values::TypedValue::*;
    use std::collections::HashMap;

    #[test]
    fn test_encode_decode_nonempty() {
        let columns = make_quote_parameters();
        let phys_columns = Column::from_parameters(&columns).unwrap();
        let model = HardStructure::new(phys_columns, vec![
            StringValue("EDF".to_string()),
            StringValue("NYSE".to_string()),
            Number(F64Value(11.11)),
        ]);
        assert_eq!(model.to_json().to_string(), r#"{"exchange":"NYSE","last_sale":11.11,"symbol":"EDF"}"#.to_string());

        let bytes = model.encode().unwrap();
        assert_eq!(model, HardStructure::decode(bytes).unwrap())
    }

    #[test]
    fn test_from_logical_columns_and_values() {
        let columns = make_quote_columns();
        let structure = HardStructure::new(columns, vec![
            StringValue("ABC".to_string()),
            StringValue("NYSE".to_string()),
            Number(F64Value(11.11)),
        ]);
        assert_eq!(structure.get("symbol"), StringValue("ABC".to_string()));
        assert_eq!(structure.get("exchange"), StringValue("NYSE".to_string()));
        assert_eq!(structure.get("last_sale"), Number(F64Value(11.11)));
        assert_eq!(structure.get_values(), vec![
            StringValue("ABC".to_string()),
            StringValue("NYSE".to_string()),
            Number(F64Value(11.11)),
        ]);
        assert_eq!(structure.to_string(), r#"{"symbol":"ABC","exchange":"NYSE","last_sale":11.11}"#)
    }

    #[test]
    fn test_from_physical_columns_and_values() {
        let columns = make_quote_parameters();
        let phys_columns = Column::from_parameters(&columns).unwrap();
        let structure = HardStructure::new(phys_columns, vec![
            StringValue("ABC".to_string()),
            StringValue("NYSE".to_string()),
            Number(F64Value(11.11)),
        ]);
        assert_eq!(structure.get("symbol"), StringValue("ABC".to_string()));
        assert_eq!(structure.get("exchange"), StringValue("NYSE".to_string()));
        assert_eq!(structure.get("last_sale"), Number(F64Value(11.11)));
        assert_eq!(structure.get_values(), vec![
            StringValue("ABC".to_string()),
            StringValue("NYSE".to_string()),
            Number(F64Value(11.11)),
        ]);
        assert_eq!(structure.to_string(), r#"{"symbol":"ABC","exchange":"NYSE","last_sale":11.11}"#)
    }

    #[test]
    fn test_from_row() {
        let columns = make_quote_parameters();
        let phys_columns = Column::from_parameters(&columns).unwrap();
        let structure = HardStructure::from_row(phys_columns.clone(),
                                                &make_quote(0, "ABC", "AMEX", 11.77),
        );
        assert_eq!(structure.get("symbol"), StringValue("ABC".to_string()));
        assert_eq!(structure.get("exchange"), StringValue("AMEX".to_string()));
        assert_eq!(structure.get("last_sale"), Number(F64Value(11.77)));
        assert_eq!(structure.get_values(), vec![
            StringValue("ABC".to_string()),
            StringValue("AMEX".to_string()),
            Number(F64Value(11.77)),
        ]);
        assert_eq!(structure.to_string(), r#"{"symbol":"ABC","exchange":"AMEX","last_sale":11.77}"#)
    }

    #[test]
    fn test_new() {
        let structure = HardStructure::new(make_quote_columns(), Vec::new());
        assert_eq!(structure.get_columns(), vec![
            Column::new("symbol", StringType(SizeTypes::Fixed(8)), Null, 9),
            Column::new("exchange", StringType(SizeTypes::Fixed(8)), Null, 26),
            Column::new("last_sale", NumberType(F64Kind), Null, 43),
        ]);
        assert_eq!(structure.get("symbol"), Null);
        assert_eq!(structure.get("exchange"), Null);
        assert_eq!(structure.get("last_sale"), Null);
        assert_eq!(structure.get_values(), vec![Null, Null, Null]);
        assert_eq!(structure.to_string(), r#"{"symbol":null,"exchange":null,"last_sale":null}"#)
    }

    #[test]
    fn test_structure_from_columns() {
        let structure = HardStructure::from_parameters(&make_quote_parameters()).unwrap();
        assert_eq!(structure.get("symbol"), Null);
        assert_eq!(structure.get("exchange"), Null);
        assert_eq!(structure.get("last_sale"), Null);
        assert_eq!(structure.get_values(), vec![Null, Null, Null]);
        assert_eq!(structure.to_string(), r#"{"symbol":null,"exchange":null,"last_sale":null}"#)
    }

    #[test]
    fn test_to_code_1() {
        let structure = HardStructure::from_fields(make_quote_columns(), Vec::new(), HashMap::new());
        assert_eq!(
            structure.to_code(),
            r#"struct(symbol: String(8), exchange: String(8), last_sale: f64)"#)
    }

    #[test]
    fn test_to_code_2() {
        let structure = HardStructure::new(make_quote_columns(), vec![
            StringValue("ZZY".into()),
            StringValue("NYSE".into()),
            Number(F64Value(77.66)),
        ]);
        assert_eq!(
            structure.to_code(),
            r#"struct(symbol: String(8) = "ZZY", exchange: String(8) = "NYSE", last_sale: f64 = 77.66)"#)
    }

    #[test]
    fn test_to_json() {
        let structure = HardStructure::new(make_quote_columns(), vec![
            StringValue("WSKY".into()),
            StringValue("NASDAQ".into()),
            Number(F64Value(17.76)),
        ]);
        assert_eq!(structure.to_json().to_string(), r#"{"exchange":"NASDAQ","last_sale":17.76,"symbol":"WSKY"}"#)
    }

    #[test]
    fn test_to_row() {
        let structure = HardStructure::new(make_quote_columns(), vec![
            StringValue("ZZY".into()),
            StringValue("NYSE".into()),
            Number(F64Value(77.66)),
        ]);
        let values = structure.to_row().get_values();
        assert_eq!(values, vec![
            StringValue("ZZY".into()),
            StringValue("NYSE".into()),
            Number(F64Value(77.66)),
        ])
    }

    #[test]
    fn test_to_table() {
        let phys_columns = make_quote_columns();
        let structure = HardStructure::new(phys_columns.clone(), vec![
            StringValue("ABB".into()),
            StringValue("NYSE".into()),
            Number(F64Value(37.25)),
        ]);
        let table = structure.to_table();
        let values = table.get_rows().iter()
            .map(|row| row.get_values())
            .collect::<Vec<_>>();
        assert_eq!(values, vec![
            vec![
                StringValue("ABB".to_string()),
                StringValue("NYSE".to_string()),
                Number(F64Value(37.25)),
            ]
        ])
    }

    #[test]
    fn test_with_variable() {
        let phys_columns = make_quote_columns();
        let structure = HardStructure::new(phys_columns.clone(), vec![
            StringValue("ICE".to_string()),
            StringValue("NASDAQ".to_string()),
            Number(F64Value(22.11)),
        ]);

        let actual =
            structure.with_variable("name", StringValue("Oxide".into()));

        let mut variables = HashMap::new();
        variables.insert("name".to_string(), StringValue("Oxide".into()));

        let expected = HardStructure::from_fields(phys_columns, vec![
            StringValue("ICE".to_string()),
            StringValue("NASDAQ".to_string()),
            Number(F64Value(22.11)),
        ], variables);
        assert_eq!(actual, expected);
    }
}

/// Unit tests
#[cfg(test)]
mod soft_structure_tests {
    use super::*;
    use crate::numbers::NumberValue::U8Value;
    use crate::typed_values::TypedValue::{Number, StringValue};

    #[test]
    fn test_encode_decode_empty() {
        let model = SoftStructure::empty();
        assert_eq!(model.to_json().to_string(), r#"{}"#.to_string());

        let bytes = model.encode().unwrap();
        assert_eq!(model, SoftStructure::decode(bytes).unwrap())
    }

    #[test]
    fn test_encode_decode_nonempty() {
        let model = SoftStructure::new(&vec![
            ("first_name", StringValue("Thomas".into())),
            ("last_name", StringValue("Brady".into())),
            ("age", Number(U8Value(41))),
        ]);

        assert_eq!(
            model.to_code(),
            r#"{first_name: "Thomas", last_name: "Brady", age: 41}"#.to_string());

        assert_eq!(
            model.to_json().to_string(),
            r#"{"age":41,"first_name":"Thomas","last_name":"Brady"}"#.to_string());

        let bytes = model.encode().unwrap();
        assert_eq!(model, SoftStructure::decode(bytes).unwrap())
    }
}