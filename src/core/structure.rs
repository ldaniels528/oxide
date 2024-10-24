////////////////////////////////////////////////////////////////////
// structures
////////////////////////////////////////////////////////////////////

use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};

use crate::errors::Errors::Exact;
use crate::machine::Machine;
use crate::model_row_collection::ModelRowCollection;
use crate::numbers::NumberValue::U16Value;
use crate::parameter::Parameter;
use crate::rows::Row;
use crate::table_columns::Column;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::*;
use serde::{Deserialize, Serialize};

/// Represents a user-defined record or data object
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Structure {
    fields: Vec<Column>,
    values: Vec<TypedValue>,
    variables: HashMap<String, TypedValue>,
}

impl Structure {

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
        Self { fields, values: my_values, variables }
    }

    pub fn from_parameters(
        parameters: &Vec<Parameter>
    ) -> std::io::Result<Structure> {
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
    ) -> std::io::Result<Structure> {
        let fields = Column::from_parameters(columns)?;
        Ok(Self::new(fields, values))
    }

    pub fn from_row(columns: Vec<Column>, row: &Row) -> Structure {
        Self::new(columns, row.get_values())
    }

    ////////////////////////////////////////////////////////////////////
    //  Instance Methods
    ////////////////////////////////////////////////////////////////////

    /// Encodes the [Structure] into a byte vector
    pub fn encode(&self) -> Vec<u8> {
        let mut encode_values = Vec::new();
        encode_values.push(Number(U16Value(self.values.len() as u16)));
        encode_values.extend(self.values.to_owned());
        encode_values.iter().flat_map(|v| v.encode()).collect()
    }

    pub fn get(&self, name: &str) -> TypedValue {
        self.fields.iter().zip(self.values.iter())
            .find(|(c, _)| c.get_name() == name)
            .map(|(_, v)| v.to_owned())
            .unwrap_or(Undefined)
    }

    pub fn get_fields(&self) -> Vec<Column> {
        self.fields.to_owned()
    }

    pub fn get_values(&self) -> Vec<TypedValue> {
        self.values.to_owned()
    }

    pub fn pollute(&self, ms: Machine) -> Machine {
        // add all scope variables (includes functions)
        let ms = self.variables.iter()
            .fold(ms, |ms, (name, value)| {
            ms.with_variable(name, value.to_owned())
        });
        // add all structure fields
        let ms = self.fields.iter().zip(self.values.iter())
            .fold(ms, |ms, (field, value)| {
            ms.with_variable(field.get_name(), value.to_owned())
        });
        // add self-reference
        ms.with_variable("self", StructureValue(self.clone()))
    }

    pub fn to_row(&self) -> Row {
        Row::new(0, self.values.to_owned())
    }

    pub fn to_table(&self) -> ModelRowCollection {
        ModelRowCollection::from_rows(self.fields.clone(), vec![self.to_row()])
    }

    pub fn with_variable(&self, name: &str, value: TypedValue) -> Self {
        let mut ss = self.clone();
        ss.variables.insert(name.into(), value);
        ss
    }
}

impl Display for Structure {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mapping = self.fields.iter().zip(self.values.iter())
            .map(|(c, v)| format!("\"{}\":{}", c.get_name(), v.to_code()))
            .collect::<Vec<_>>();
        write!(f, "{{{}}}", mapping.join(","))
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::data_types::DataType::*;
    use crate::data_types::SizeTypes;
    use crate::number_kind::NumberKind::F64Kind;
    use crate::numbers::NumberValue::F64Value;
    use crate::structure::Structure;
    use crate::table_columns::Column;
    use crate::testdata::{make_quote, make_quote_columns, make_quote_parameters};
    use crate::typed_values::TypedValue::*;

    #[test]
    fn test_encode() {
        let columns = make_quote_parameters();
        let phys_columns = Column::from_parameters(&columns).unwrap();
        let structure = Structure::new(phys_columns, vec![
            StringValue("EDF".to_string()),
            StringValue("NYSE".to_string()),
            Number(F64Value(11.11)),
        ]);
        assert_eq!(
            structure.encode(),
            vec![
                0, 3,
                0, 0, 0, 0, 0, 0, 0, 3, b'E', b'D', b'F',
                0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E',
                64, 38, 56, 81, 235, 133, 30, 184,
            ])
    }

    #[test]
    fn test_from_logical_columns_and_values() {
        let columns = make_quote_columns();
        let structure = Structure::new(columns, vec![
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
        let structure = Structure::new(phys_columns, vec![
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
        let structure = Structure::from_row(phys_columns.clone(),
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
        let structure = Structure::new(make_quote_columns(), Vec::new());
        assert_eq!(structure.get_fields(), vec![
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
        let structure = Structure::from_parameters(&make_quote_parameters()).unwrap();
        assert_eq!(structure.get("symbol"), Null);
        assert_eq!(structure.get("exchange"), Null);
        assert_eq!(structure.get("last_sale"), Null);
        assert_eq!(structure.get_values(), vec![Null, Null, Null]);
        assert_eq!(structure.to_string(), r#"{"symbol":null,"exchange":null,"last_sale":null}"#)
    }

    #[test]
    fn test_to_row() {
        let phys_columns = make_quote_columns();
        let structure = Structure::new(phys_columns.clone(), vec![
            StringValue("ZZY".to_string()),
            StringValue("NYSE".to_string()),
            Number(F64Value(77.66)),
        ]);
        let values = structure.to_row().get_values();
        assert_eq!(values, vec![
            StringValue("ZZY".to_string()),
            StringValue("NYSE".to_string()),
            Number(F64Value(77.66)),
        ])
    }

    #[test]
    fn test_to_table() {
        let phys_columns = make_quote_columns();
        let structure = Structure::new(phys_columns.clone(), vec![
            StringValue("ABB".to_string()),
            StringValue("NYSE".to_string()),
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
        let structure = Structure::new(phys_columns.clone(), vec![
            StringValue("ICE".to_string()),
            StringValue("NASDAQ".to_string()),
            Number(F64Value(22.11)),
        ]);

        let actual =
            structure.with_variable("name", StringValue("Oxide".into()));

        let expected = Structure::from_fields(phys_columns, vec![
            StringValue("ICE".to_string()),
            StringValue("NASDAQ".to_string()),
            Number(F64Value(22.11)),
        ], {
                                            let mut variables = HashMap::new();
                                            variables.insert("name".to_string(), StringValue("Oxide".into()));
                                            variables
                                        });
        assert_eq!(actual, expected);
    }
}