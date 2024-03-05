////////////////////////////////////////////////////////////////////
// test data module
////////////////////////////////////////////////////////////////////

use std::error::Error;
use std::fs::File;
use std::io::{Seek, Write};

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

type TestError = Box<dyn Error>;

pub fn make_columns() -> Vec<Column> {
    vec![
        Column::new("symbol", "String(4)", ""),
        Column::new("exchange", "String(4)", ""),
        Column::new("lastSale", "Double", ""),
    ]
}

pub fn make_dataframe(database: &str, schema: &str, name: &str) -> Result<DataFrame, Box<dyn Error>> {
    let ns: Namespace = Namespace::new(database, schema, name);
    let mut df: DataFrame = DataFrame::create(ns, make_dataframe_config())?;
    df.resize(0)?;
    Ok(df)
}

pub fn make_dataframe_config() -> DataFrameConfig {
    DataFrameConfig::new(make_columns(), Vec::new(), Vec::new())
}

pub fn make_row(id: usize) -> Row {
    let metadata: RowMetadata = RowMetadata::new(true);
    let columns: Vec<TableColumn> = make_table_columns();
    let fields: Vec<Field> = vec![
        Field::with_value(StringValue("AMD".to_string())),
        Field::with_value(StringValue("NYSE".to_string())),
        Field::with_value(Float64Value(78.35)),
    ];
    Row::new(id, metadata, columns, fields)
}

pub fn make_rows_from_bytes(database: &str, schema: &str, name: &str, row_data: &[u8]) -> Result<DataFrame, TestError> {
    let mut df: DataFrame = make_dataframe(database, schema, name)?;
    df.file.set_len(0)?;
    df.file.write_all(row_data)?;
    df.file.flush()?;
    Ok(df)
}

pub fn make_table_columns() -> Vec<TableColumn> {
    vec![
        TableColumn::new("symbol", StringType(4), NullValue, 9),
        TableColumn::new("exchange", StringType(4), NullValue, 22),
        TableColumn::new("lastSale", Float64Type, NullValue, 35),
    ]
}

pub fn make_table_file_from_bytes(database: &str, schema: &str, name: &str, row_data: &[u8]) -> (File, Vec<TableColumn>, usize) {
    let (mut file, columns, record_size) = make_table_file(database, schema, name);
    file.write_all(row_data).unwrap();
    file.flush().unwrap();
    (file, columns, record_size)
}

pub fn make_table_file(database: &str, schema: &str, name: &str) -> (File, Vec<TableColumn>, usize) {
    let mut df: DataFrame = make_dataframe(database, schema, name).unwrap();
    df.file.set_len(0).unwrap();
    (df.file, df.columns, df.record_size)
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_differences() {
        let generated: Vec<TableColumn> = TableColumn::from_columns(&make_columns()).unwrap();
        let natural: Vec<TableColumn> = vec![
            TableColumn::new("symbol", StringType(4), NullValue, 9),
            TableColumn::new("exchange", StringType(4), NullValue, 22),
            TableColumn::new("lastSale", Float64Type, NullValue, 35),
        ];
        assert_eq!(generated, natural);
    }
}