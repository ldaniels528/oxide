////////////////////////////////////////////////////////////////////
// server module
////////////////////////////////////////////////////////////////////

use actix::{Actor, StreamHandler};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::fields::Field;
use crate::rows::Row;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::Undefined;

pub const VERSION: &str = "0.1.0";

// JSON representation of a column
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnJs {
    pub(crate) name: String,
    pub(crate) column_type: String,
    pub(crate) default_value: Option<String>,
}

impl ColumnJs {
    pub fn new(name: impl Into<String>,
               column_type: impl Into<String>,
               default_value: Option<String>) -> Self {
        ColumnJs { name: name.into(), column_type: column_type.into(), default_value }
    }

    pub fn get_name(&self) -> &String { &self.name }

    pub fn get_column_type(&self) -> &String { &self.column_type }

    pub fn get_default_value(&self) -> &Option<String> { &self.default_value }
}

// JSON representation of a field
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldJs {
    name: String,
    value: Value,
}

impl FieldJs {
    pub fn new(name: &str, value: Value) -> Self {
        Self { name: name.into(), value }
    }

    pub fn get_name(&self) -> &String {
        &self.name
    }

    pub fn get_value(&self) -> Value {
        self.value.clone()
    }
}

// JSON representation of a row
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RowJs {
    id: Option<usize>,
    fields: Vec<FieldJs>,
}

impl RowJs {
    pub fn new(id: Option<usize>, fields: Vec<FieldJs>) -> Self { Self { id, fields } }

    pub fn get_id(&self) -> Option<usize> { self.id }

    pub fn get_fields(&self) -> &Vec<FieldJs> { &self.fields }
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

pub fn to_row(columns: &Vec<TableColumn>, form: RowJs, id: usize) -> Row {
    let mut fields = vec![];
    for tc in columns {
        fields.push(Field::with_value(determine_column_value(&form, tc.get_name())));
    }
    Row::new(id, columns.clone(), fields)
}

pub fn to_row_json(row: Row) -> RowJs {
    RowJs::new(Some(row.id), row.fields.iter().zip(row.get_columns())
        .map(|(f, c)| FieldJs::new(c.get_name(), f.value.to_json())).collect())
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::data_types::DataType::{Float64Type, StringType};
    use crate::row;
    use crate::server::SystemInfoJs;
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
    fn test_columns_json() {
        let columns = vec![
            ColumnJs::new("symbol", "String(8)", None),
            ColumnJs::new("exchange", "String(10)", None),
            ColumnJs::new("lastSale", "Double", Some("0.0".into())),
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
                name: "lastSale".into(),
                column_type: "Double".into(),
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
            TableColumn::new("lastSale", Float64Type, Null, 35),
        ];
        let row = row!(123, columns, vec![
            StringValue("FOX".into()), StringValue("NYSE".into()), Float64Value(37.65),
        ]);
        // define a row-js
        let fields_js = vec![
            FieldJs::new("symbol", serde_json::json!("FOX")),
            FieldJs::new("exchange", serde_json::json!("NYSE")),
            FieldJs::new("lastSale", serde_json::json!(37.65)),
        ];
        let row_js = RowJs::new(Some(123), fields_js.clone());
        // verify the accessors
        assert_eq!(row_js.get_id(), Some(123));
        assert_eq!(row_js.get_fields(), &vec![
            FieldJs {
                name: "symbol".to_string(),
                value: serde_json::json!("FOX"),
            },
            FieldJs {
                name: "exchange".to_string(),
                value: serde_json::json!("NYSE"),
            },
            FieldJs {
                name: "lastSale".to_string(),
                value: serde_json::json!(37.65),
            }]);
        assert_eq!(row_js, RowJs { id: Some(123), fields: fields_js });
        // verify the value extraction
        assert_eq!(determine_column_value(&row_js, "symbol"), StringValue("FOX".into()));
        assert_eq!(determine_column_value(&row_js, "exchange"), StringValue("NYSE".into()));
        assert_eq!(determine_column_value(&row_js, "lastSale"), Float64Value(37.65));
        // cross-convert and verify
        assert_eq!(to_row_json(row.clone()), row_js.clone());
        assert_eq!(to_row(&columns, row_js, 123), row);
    }
}