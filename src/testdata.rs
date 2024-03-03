////////////////////////////////////////////////////////////////////
// test data
////////////////////////////////////////////////////////////////////

use std::error::Error;

use crate::columns::Column;
use crate::data_types::DataType::{Float64Type, StringType};
use crate::dataframe_config::DataFrameConfig;
use crate::dataframes::DataFrame;
use crate::fields::Field;
use crate::namespaces::Namespace;
use crate::row_metadata::RowMetadata;
use crate::rows::Row;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue::{Float64Value, NullValue, StringValue};

pub fn create_test_dataframe(database: &str, schema: &str, name: &str) -> Result<DataFrame, Box<dyn Error>> {
    let ns: Namespace = Namespace::new(database, schema, name);
    DataFrame::create(ns, create_test_dataframe_config())
}

pub fn create_test_dataframe_config() -> DataFrameConfig {
    DataFrameConfig::new(create_test_columns(), Vec::new(), Vec::new())
}

pub fn create_test_columns() -> Vec<Column> {
    vec![
        Column::new("symbol", "String(4)", ""),
        Column::new("exchange", "String(4)", ""),
        Column::new("lastSale", "Double", ""),
    ]
}

pub fn create_test_table_columns() -> Vec<TableColumn> {
    vec![
        TableColumn::new("symbol", StringType(4), NullValue),
        TableColumn::new("exchange", StringType(4), NullValue),
        TableColumn::new("lastSale", Float64Type, NullValue),
    ]
}

pub fn create_test_row() -> Row {
    let metadata: RowMetadata = RowMetadata::new(true);
    let columns: Vec<TableColumn> = create_test_table_columns();
    let fields: Vec<Field> = vec![
        Field::with_value(StringValue("AMD".to_string())),
        Field::with_value(StringValue("NYSE".to_string())),
        Field::with_value(Float64Value(78.35)),
    ];
    Row::new(214, metadata, columns, fields)
}

