////////////////////////////////////////////////////////////////////
// structures
////////////////////////////////////////////////////////////////////

use std::collections::HashMap;
use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use crate::model_row_collection::ModelRowCollection;
use crate::rows::Row;
use crate::server::ColumnJs;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{Null, UInt16Value, Undefined};

/// Represents a user-defined record or data object
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Structure {
    //name: Option<String>,
    columns: Vec<TableColumn>,
    values: Vec<TypedValue>,
    variables: HashMap<String, TypedValue>,
}

impl Structure {

    ////////////////////////////////////////////////////////////////////
    //  Constructors
    ////////////////////////////////////////////////////////////////////

    pub fn new(columns: Vec<TableColumn>) -> Self {
        let values = columns.iter().map(|_| Null).collect();
        Self { columns, values, variables: HashMap::new() }
    }

    ////////////////////////////////////////////////////////////////////
    //  Static Methods
    ////////////////////////////////////////////////////////////////////

    pub fn from_logical_columns(
        columns: &Vec<ColumnJs>
    ) -> std::io::Result<Structure> {
        let values = columns.iter().map(|_| Null).collect();
        Self::from_logical_columns_and_values(columns, values)
    }

    pub fn from_logical_columns_and_values(
        columns: &Vec<ColumnJs>,
        values: Vec<TypedValue>,
    ) -> std::io::Result<Structure> {
        Ok(Structure {
            columns: TableColumn::from_columns(columns)?,
            values,
            variables: HashMap::new()
        })
    }

    pub fn from_physical_columns_and_values(
        columns: Vec<TableColumn>,
        values: Vec<TypedValue>,
    ) -> Structure {
        Structure { columns, values, variables: HashMap::new() }
    }

    pub fn from_row(row: &Row) -> Structure {
        Self::from_physical_columns_and_values(row.get_columns().clone(), row.get_values())
    }

    ////////////////////////////////////////////////////////////////////
    //  Instance Methods
    ////////////////////////////////////////////////////////////////////

    /// Encodes the [Structure] into a byte vector
    pub fn encode(&self) -> Vec<u8> {
        let mut encode_values = vec![];
        encode_values.push(UInt16Value(self.values.len() as u16));
        encode_values.extend(self.values.clone());
        encode_values.iter().flat_map(|v| v.encode()).collect()
    }

    pub fn get(&self, name: &str) -> TypedValue {
        self.columns.iter().zip(self.values.iter())
            .find(|(c, _)| c.get_name() == name)
            .map(|(_, v)| v.clone())
            .unwrap_or(Undefined)
    }

    pub fn get_columns(&self) -> Vec<TableColumn> {
        self.columns.clone()
    }

    pub fn get_values(&self) -> Vec<TypedValue> {
        self.values.clone()
    }

    pub fn to_row(&self) -> Row {
        Row::new(0, self.columns.clone(), self.values.clone())
    }

    pub fn to_string(&self) -> String {
        let mapping = self.columns.iter().zip(self.values.iter())
            .map(|(c, v)| format!("\"{}\": {}", c.get_name(), v.to_json().to_string()))
            .collect::<Vec<String>>();
        format!("{{ {} }}", mapping.join(", "))
    }

    pub fn with_rows(&self, rows: Vec<Row>) -> ModelRowCollection {
        let mut my_rows = vec![];
        my_rows.push(self.to_row());
        my_rows.extend(rows);
        let my_rows = my_rows.iter()
            .zip(0..my_rows.len())
            .map(|(r, id)| r.with_row_id(id))
            .collect();
        ModelRowCollection::from_rows(my_rows)
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::data_types::DataType::{Float64Type, StringType};
    use crate::row_collection::RowCollection;
    use crate::structure::Structure;
    use crate::table_columns::TableColumn;
    use crate::testdata::{make_quote, make_quote_columns, make_table_columns};
    use crate::typed_values::TypedValue::{Float64Value, Null, StringValue};

    #[test]
    fn test_new_empty_structure() {
        let structure = Structure::new(make_table_columns());
        assert_eq!(structure.get_columns(), vec![
            TableColumn::new("symbol", StringType(8), Null, 9),
            TableColumn::new("exchange", StringType(8), Null, 26),
            TableColumn::new("last_sale", Float64Type, Null, 43),
        ]);
        assert_eq!(structure.get("symbol"), Null);
        assert_eq!(structure.get("exchange"), Null);
        assert_eq!(structure.get("last_sale"), Null);
        assert_eq!(structure.get_values(), vec![Null, Null, Null]);
        assert_eq!(structure.to_string(), r#"{ "symbol": null, "exchange": null, "last_sale": null }"#)
    }

    #[test]
    fn test_from_logical_columns_and_values() {
        let columns = make_quote_columns();
        let structure = Structure::from_logical_columns_and_values(&columns, vec![
            StringValue("ABC".to_string()),
            StringValue("NYSE".to_string()),
            Float64Value(11.11),
        ]).unwrap();
        assert_eq!(structure.get("symbol"), StringValue("ABC".to_string()));
        assert_eq!(structure.get("exchange"), StringValue("NYSE".to_string()));
        assert_eq!(structure.get("last_sale"), Float64Value(11.11));
        assert_eq!(structure.get_values(), vec![
            StringValue("ABC".to_string()),
            StringValue("NYSE".to_string()),
            Float64Value(11.11),
        ]);
        assert_eq!(structure.to_string(), r#"{ "symbol": "ABC", "exchange": "NYSE", "last_sale": 11.11 }"#)
    }

    #[test]
    fn test_from_physical_columns_and_values() {
        let columns = make_quote_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let structure = Structure::from_physical_columns_and_values(phys_columns, vec![
            StringValue("ABC".to_string()),
            StringValue("NYSE".to_string()),
            Float64Value(11.11),
        ]);
        assert_eq!(structure.get("symbol"), StringValue("ABC".to_string()));
        assert_eq!(structure.get("exchange"), StringValue("NYSE".to_string()));
        assert_eq!(structure.get("last_sale"), Float64Value(11.11));
        assert_eq!(structure.get_values(), vec![
            StringValue("ABC".to_string()),
            StringValue("NYSE".to_string()),
            Float64Value(11.11),
        ]);
        assert_eq!(structure.to_string(), r#"{ "symbol": "ABC", "exchange": "NYSE", "last_sale": 11.11 }"#)
    }

    #[test]
    fn test_from_row() {
        let columns = make_quote_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let structure = Structure::from_row(
            &make_quote(0, &phys_columns, "ABC", "AMEX", 11.77)
        );
        assert_eq!(structure.get("symbol"), StringValue("ABC".to_string()));
        assert_eq!(structure.get("exchange"), StringValue("AMEX".to_string()));
        assert_eq!(structure.get("last_sale"), Float64Value(11.77));
        assert_eq!(structure.get_values(), vec![
            StringValue("ABC".to_string()),
            StringValue("AMEX".to_string()),
            Float64Value(11.77),
        ]);
        assert_eq!(structure.to_string(), r#"{ "symbol": "ABC", "exchange": "AMEX", "last_sale": 11.77 }"#)
    }

    #[test]
    fn test_with_rows() {
        let columns = make_quote_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let structure = Structure::from_physical_columns_and_values(phys_columns.clone(), vec![
            StringValue("ICE".to_string()),
            StringValue("NASDAQ".to_string()),
            Float64Value(22.11),
        ]);
        let table = structure.with_rows(vec![
            make_quote(0, &phys_columns, "ABC", "AMEX", 11.77),
            make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
            make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66),
        ]);
        assert_eq!(
            table.read_active_rows().unwrap(),
            vec![
                make_quote(0, &phys_columns, "ICE", "NASDAQ", 22.11),
                make_quote(1, &phys_columns, "ABC", "AMEX", 11.77),
                make_quote(2, &phys_columns, "UNO", "OTC", 0.2456),
                make_quote(3, &phys_columns, "BIZ", "NYSE", 23.66),
            ]);
        assert_eq!(
            table.to_string(),
            r#"[{ "symbol": "ICE", "exchange": "NASDAQ", "last_sale": 22.11 }, { "symbol": "ABC", "exchange": "AMEX", "last_sale": 11.77 }, { "symbol": "UNO", "exchange": "OTC", "last_sale": 0.2456 }, { "symbol": "BIZ", "exchange": "NYSE", "last_sale": 23.66 }]"#
        )
    }

    #[test]
    fn test_encode() {
        let columns = make_quote_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let structure = Structure::from_physical_columns_and_values(phys_columns, vec![
            StringValue("EDF".to_string()),
            StringValue("NYSE".to_string()),
            Float64Value(11.11),
        ]);
        assert_eq!(
            structure.encode(),
            vec![
                0, 3,
                0, 0, 0, 0, 0, 0, 0, 3, b'E', b'D', b'F',
                0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E',
                64, 38, 56, 81, 235, 133, 30, 184
            ])
    }

}