////////////////////////////////////////////////////////////////////
// server module
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::fields::Field;
use crate::rows::Row;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{Null, Undefined};

const VERSION: &str = "0.1.0";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RowForm {
    columns: Vec<ColumnJs>,
}

impl RowForm {
    pub fn new(columns: Vec<ColumnJs>) -> Self {
        Self { columns }
    }

    pub fn get_columns(&self) -> &Vec<ColumnJs> { &self.columns }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RowJs {
    id: usize,
    columns: Vec<ColumnJs>,
}

impl RowJs {
    pub fn new(id: usize, columns: Vec<ColumnJs>) -> Self {
        Self { id, columns }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnJs {
    name: String,
    value: Value,
}

impl ColumnJs {
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SystemInfoJs {
    title: String,
    version: String,
}

impl SystemInfoJs {
    pub fn new() -> SystemInfoJs {
        SystemInfoJs {
            title: "Oxide".into(),
            version: "0.1.0".into(),
        }
    }
}

pub fn determine_column_value(form: &RowForm, name: &str) -> TypedValue {
    for c in form.get_columns() {
        if c.get_name() == name { return TypedValue::from_json(c.get_value()); }
    }
    Undefined
}

pub fn to_json_row(row: Row) -> RowJs {
    RowJs::new(row.id, row.fields.iter().zip(&row.columns)
        .map(|(f, c)| ColumnJs::new(&c.name, f.value.to_json())).collect())
}

pub fn to_row(columns: &Vec<TableColumn>, form: RowForm, id: usize) -> Row {
    let mut fields = vec![];
    for tc in columns {
        let field = Field::with_value(determine_column_value(&form, tc.get_name()));
        fields.push(field);
    }
    Row::new(id, columns.clone(), fields)
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::data_types::DataType::{Float64Type, StringType};
    use crate::typed_values::TypedValue::{Boolean, Float64Value, StringValue};

    use super::*;

    #[test]
    fn test_create_system_info() {
        assert_eq!(SystemInfoJs::new(), SystemInfoJs {
            title: "Oxide".into(),
            version: VERSION.into(),
        })
    }

    #[test]
    fn test_determine_column_value() {
        let form = RowForm::new(vec![
            ColumnJs::new("symbol", serde_json::json!("ABC")),
            ColumnJs::new("lastSale", serde_json::json!(37.65)),
            ColumnJs::new("updated", serde_json::json!(true)),
        ]);
        assert_eq!(determine_column_value(&form, "symbol"), StringValue("ABC".into()));
        assert_eq!(determine_column_value(&form, "lastSale"), Float64Value(37.65));
        assert_eq!(determine_column_value(&form, "updated"), Boolean(true));
    }

    #[test]
    fn test_to_row() {
        let columns = vec![
            TableColumn::new("symbol", StringType(4), Null, 9),
            TableColumn::new("exchange", StringType(4), Null, 22),
            TableColumn::new("lastSale", Float64Type, Null, 35),
        ];
        let form = RowForm::new(vec![
            ColumnJs::new("symbol", serde_json::json!("ABC")),
            ColumnJs::new("exchange", serde_json::json!("BOOM")),
            ColumnJs::new("lastSale", serde_json::json!(37.65)),
        ]);
        let expected = Row::new(100, columns.clone(), vec![
            Field::new(StringValue("ABC".into())),
            Field::new(StringValue("BOOM".into())),
            Field::new(Float64Value(37.65)),
        ]);
        let actual = to_row(&columns, form, 100);
        assert_eq!(actual, expected);
    }
}