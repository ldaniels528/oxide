////////////////////////////////////////////////////////////////////
// structures
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};
use crate::rows::Row;

use crate::server::ColumnJs;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{Null, UInt16Value, Undefined};

/// Represents an user-defined record or data object
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Structure {
    columns: Vec<TableColumn>,
    values: Vec<TypedValue>,
}

impl Structure {

    ////////////////////////////////////////////////////////////////////
    //  Static Methods
    ////////////////////////////////////////////////////////////////////

    pub fn construct(columns: &Vec<ColumnJs>, values: Vec<TypedValue>) -> Structure {
        match TableColumn::from_columns(columns) {
            Ok(columns) => Structure { columns, values },
            Err(err) => panic!("{}", err.to_string())
        }
    }

    pub fn from_logical_columns(columns: &Vec<ColumnJs>) -> Structure {
        let fields = columns.iter().map(|_| Null).collect();
        Self::construct(columns, fields)
    }

    pub fn from_physical_columns(columns: Vec<TableColumn>) -> Structure {
        let values = columns.iter().map(|_| Null).collect();
        Structure { columns, values }
    }

    pub fn from_row(row: &Row) -> Structure {
        Self::new(row.get_columns().clone(), row.get_values())
    }

    pub fn new(columns: Vec<TableColumn>, values: Vec<TypedValue>) -> Structure {
        Structure { columns, values }
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
            .find(|(c, v)| c.get_name() == name)
            .map(|(c, v)| v.clone())
            .unwrap_or(Undefined)
    }

    pub fn get_columns(&self) -> Vec<TableColumn> {
        self.columns.clone()
    }

    pub fn get_values(&self) -> Vec<TypedValue> {
        self.values.clone()
    }

    pub fn to_string(&self) -> String {
        let mapping = self.columns.iter().zip(self.values.iter())
            .map(|(c, v)| format!("\"{}\": {}", c.get_name(), v.to_json().to_string()))
            .collect::<Vec<String>>();
        format!("{{ {} }}", mapping.join(", "))
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::structure::Structure;
    use crate::testdata::{make_quote_columns, make_table_columns};
    use crate::typed_values::TypedValue::{Float64Value, StringValue};

    #[test]
    fn verify_construct() {
        let structure = Structure::construct(&make_quote_columns(), vec![
            StringValue("ABC".to_string()),
            StringValue("NYSE".to_string()),
            Float64Value(11.11),
        ]);
        assert_eq!(structure.to_string(), r#"{ "symbol": "ABC", "exchange": "NYSE", "last_sale": 11.11 }"#)
    }

    #[test]
    fn verify_to_string() {
        let structure = Structure::from_physical_columns(make_table_columns());
        assert_eq!(structure.to_string(), r#"{ "symbol": null, "exchange": null, "last_sale": null }"#)
    }
}