////////////////////////////////////////////////////////////////////
// server module
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

use shared_lib::RowJs;

use crate::codec;
use crate::rows::Row;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::Undefined;

pub const VERSION: &str = "0.1.0";

// JSON representation of a column
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ColumnJs {
    name: String,
    column_type: String,
    default_value: Option<String>,
}

impl ColumnJs {
    pub fn decode(buf: Vec<u8>) -> Self {
        let name = codec::decode_string(&buf, 0, 64);
        let column_type = codec::decode_string(&buf, 64, 64);
        let default_value = Some(codec::decode_string(&buf, 128, 64));
        Self::new(name, column_type, default_value)
    }

    pub fn encode(&self) -> Vec<u8> {
        let default_value = self.default_value.to_owned().unwrap_or("".to_string());
        let mut buf: Vec<u8> = Vec::new();
        buf.push(self.name.len() as u8);
        buf.extend(self.name.bytes());
        buf.push(self.column_type.len() as u8);
        buf.extend(self.column_type.bytes());
        buf.push(default_value.len() as u8);
        buf.extend(default_value.bytes());
        buf
    }

    pub fn from_physical_column(column: &TableColumn) -> Self {
        ColumnJs::new(column.get_name(), column.data_type.to_column_type(), match &column.default_value {
            TypedValue::Null | Undefined => None,
            v => Some(v.to_json().to_string())
        })
    }

    pub fn from_physical_columns(phys_columns: &Vec<TableColumn>) -> Vec<Self> {
        phys_columns.iter().map(|c| Self::from_physical_column(c)).collect()
    }

    pub fn get_name(&self) -> &String { &self.name }

    pub fn get_column_type(&self) -> &String { &self.column_type }

    pub fn get_default_value(&self) -> &Option<String> { &self.default_value }

    pub fn new(name: impl Into<String>,
               column_type: impl Into<String>,
               default_value: Option<String>) -> Self {
        ColumnJs { name: name.into(), column_type: column_type.into(), default_value }
    }

    pub fn render_columns(columns: &Vec<ColumnJs>) -> String {
        columns.iter().map(|c| c.to_code()).collect::<Vec<String>>().join(", ")
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.get_name(),
            "column_type": self.get_column_type(),
            "default_value": self.get_default_value()
        })
    }

    pub fn to_code(&self) -> String {
        format!("{}: {}", self.name, self.column_type)
    }
}

// JSON representation of Oxide system information
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SystemInfoJs {
    title: String,
    version: String,
}

impl SystemInfoJs {
    pub fn new() -> SystemInfoJs {
        SystemInfoJs {
            title: "Oxide".into(),
            version: VERSION.into(),
        }
    }
}

pub fn determine_column_value(form: &RowJs, name: &str) -> TypedValue {
    for c in form.get_fields() {
        if c.get_name() == name { return TypedValue::from_json(c.get_value()); }
    }
    Undefined
}

// Unit tests
#[cfg(test)]
mod tests {
    use shared_lib::FieldJs;

    use crate::data_types::DataType::{Float64Type, StringType};
    use crate::row;
    use crate::server::SystemInfoJs;
    use crate::testdata::{make_quote_columns, make_table_columns};
    use crate::typed_values::TypedValue::{Float64Value, Null, StringValue};

    use super::*;

    #[test]
    fn test_create_system_info() {
        assert_eq!(SystemInfoJs::new(), SystemInfoJs {
            title: "Oxide".into(),
            version: VERSION.into(),
        })
    }

    #[test]
    fn test_column_conversion() {
        let columns = ColumnJs::from_physical_columns(&make_table_columns());
        assert_eq!(columns, make_quote_columns());
    }

    #[test]
    fn test_columns_json() {
        let columns = vec![
            ColumnJs::new("symbol", "String(8)", None),
            ColumnJs::new("exchange", "String(10)", None),
            ColumnJs::new("last_sale", "f64", Some("0.0".into())),
        ];
        assert_eq!(columns, vec![
            ColumnJs {
                name: "symbol".into(),
                column_type: "String(8)".into(),
                default_value: None,
            },
            ColumnJs {
                name: "exchange".into(),
                column_type: "String(10)".into(),
                default_value: None,
            },
            ColumnJs {
                name: "last_sale".into(),
                column_type: "f64".into(),
                default_value: Some("0.0".into()),
            },
        ])
    }

    #[test]
    fn test_conversion_between_row_and_row_json() {
        // define a row
        let columns = vec![
            TableColumn::new("symbol", StringType(4), Null, 9),
            TableColumn::new("exchange", StringType(4), Null, 22),
            TableColumn::new("last_sale", Float64Type, Null, 35),
        ];
        let row = row!(123, columns, vec![
            StringValue("FOX".into()), StringValue("NYSE".into()), Float64Value(37.65),
        ]);
        // define a row-js
        let fields_js = vec![
            FieldJs::new("symbol", serde_json::json!("FOX")),
            FieldJs::new("exchange", serde_json::json!("NYSE")),
            FieldJs::new("last_sale", serde_json::json!(37.65)),
        ];
        let row_js = RowJs::new(Some(123), fields_js.to_owned());
        // verify the accessors
        assert_eq!(row_js.get_id(), Some(123));
        assert_eq!(row_js.get_fields(), &vec![
            FieldJs::new("symbol", serde_json::json!("FOX")),
            FieldJs::new("exchange", serde_json::json!("NYSE")),
            FieldJs::new("last_sale", serde_json::json!(37.65)),
        ]);
        assert_eq!(row_js, RowJs::new(Some(123), fields_js));
        // verify the value extraction
        assert_eq!(determine_column_value(&row_js, "symbol"), StringValue("FOX".into()));
        assert_eq!(determine_column_value(&row_js, "exchange"), StringValue("NYSE".into()));
        assert_eq!(determine_column_value(&row_js, "last_sale"), Float64Value(37.65));
        // cross-convert and verify
        assert_eq!(row.to_row_js(), row_js.to_owned());
        assert_eq!(Row::from_row_js(&columns, &row_js), row);
    }
}