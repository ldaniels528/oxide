////////////////////////////////////////////////////////////////////
// structures
////////////////////////////////////////////////////////////////////

use std::collections::HashMap;
use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use crate::model_row_collection::ModelRowCollection;
use crate::numbers::NumberValue::U16Value;
use crate::rows::Row;
use crate::server::ColumnJs;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::*;

/// Represents a user-defined record or data object
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Structure {
    fields: Vec<TableColumn>,
    values: Vec<TypedValue>,
    variables: HashMap<String, TypedValue>,
}

impl Structure {

    ////////////////////////////////////////////////////////////////////
    //  Constructors
    ////////////////////////////////////////////////////////////////////

    pub fn build(
        fields: Vec<TableColumn>,
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

    pub fn new(fields: Vec<TableColumn>, values: Vec<TypedValue>) -> Self {
        Self::build(fields, values, HashMap::new())
    }

    ////////////////////////////////////////////////////////////////////
    //  Static Methods
    ////////////////////////////////////////////////////////////////////

    pub fn from_columns(
        columns: &Vec<ColumnJs>
    ) -> std::io::Result<Structure> {
        let fields = TableColumn::from_columns(columns)?;
        let values = columns.iter().map(|c|
            c.get_default_value().to_owned()
                .map(|s| TypedValue::wrap_value(s.as_str())
                    .unwrap_or_else(|err| ErrorValue(err.to_string())))
                .unwrap_or(Null)
        ).collect();
        Ok(Self::new(fields, values))
    }

    pub fn from_columns_and_values(
        columns: &Vec<ColumnJs>,
        values: Vec<TypedValue>,
    ) -> std::io::Result<Structure> {
        let fields = TableColumn::from_columns(columns)?;
        Ok(Self::new(fields, values))
    }

    pub fn from_row(columns: Vec<TableColumn>, row: &Row) -> Structure {
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

    pub fn get_columns(&self) -> Vec<TableColumn> {
        self.fields.to_owned()
    }

    pub fn get_values(&self) -> Vec<TypedValue> {
        self.values.to_owned()
    }

    pub fn to_row(&self) -> Row {
        Row::new(0, self.values.to_owned())
    }

    pub fn to_string(&self) -> String {
        let mapping = self.fields.iter().zip(self.values.iter())
            .map(|(c, v)| format!("\"{}\":{}", c.get_name(), v.to_json().to_string()))
            .collect::<Vec<_>>();
        format!("{{{}}}", mapping.join(","))
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

// Unit tests
#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::data_types::DataType::*;
    use crate::number_kind::NumberKind::F64Kind;
    use crate::numbers::NumberValue::F64Value;
    use crate::row_collection::RowCollection;
    use crate::structure::Structure;
    use crate::table_columns::TableColumn;
    use crate::testdata::{make_quote, make_quote_columns, make_table_columns};
    use crate::typed_values::TypedValue::*;

    #[test]
    fn test_encode() {
        let columns = make_quote_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
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
        let columns = make_table_columns();
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
        let columns = make_quote_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
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
        let columns = make_quote_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let structure = Structure::from_row(phys_columns.clone(),
            &make_quote(0, "ABC", "AMEX", 11.77)
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
        let structure = Structure::new(make_table_columns(), Vec::new());
        assert_eq!(structure.get_columns(), vec![
            TableColumn::new("symbol", StringType(8), Null, 9),
            TableColumn::new("exchange", StringType(8), Null, 26),
            TableColumn::new("last_sale", NumberType(F64Kind), Null, 43),
        ]);
        assert_eq!(structure.get("symbol"), Null);
        assert_eq!(structure.get("exchange"), Null);
        assert_eq!(structure.get("last_sale"), Null);
        assert_eq!(structure.get_values(), vec![Null, Null, Null]);
        assert_eq!(structure.to_string(), r#"{"symbol":null,"exchange":null,"last_sale":null}"#)
    }

    #[test]
    fn test_structure_from_columns() {
        let structure = Structure::from_columns(&make_quote_columns()).unwrap();
        assert_eq!(structure.get("symbol"), Null);
        assert_eq!(structure.get("exchange"), Null);
        assert_eq!(structure.get("last_sale"), Null);
        assert_eq!(structure.get_values(), vec![Null, Null, Null]);
        assert_eq!(structure.to_string(), r#"{"symbol":null,"exchange":null,"last_sale":null}"#)
    }

    #[test]
    fn test_to_row() {
        let phys_columns = make_table_columns();
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
        let phys_columns = make_table_columns();
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
        let phys_columns = make_table_columns();
        let structure = Structure::new(phys_columns.clone(), vec![
            StringValue("ICE".to_string()),
            StringValue("NASDAQ".to_string()),
            Number(F64Value(22.11)),
        ]);

        let actual =
            structure.with_variable("name", StringValue("Oxide".into()));

        let expected = Structure::build(phys_columns, vec![
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