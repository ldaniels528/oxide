////////////////////////////////////////////////////////////////////
// test data module
////////////////////////////////////////////////////////////////////

use std::error::Error;
use std::fs::File;
use std::io;
use std::io::Write;

use crate::columns::Column;
use crate::data_types::DataType::{Float64Type, StringType};
use crate::dataframe_config::DataFrameConfig;
use crate::dataframes::DataFrame;
use crate::fields::Field;
use crate::namespaces::Namespace;
use crate::rows::Row;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue::{Float64Value, Null, StringValue};

pub fn make_columns() -> Vec<Column> {
    vec![
        Column::new("symbol", "String(4)", None),
        Column::new("exchange", "String(4)", None),
        Column::new("lastSale", "Double", None),
    ]
}

pub fn make_dataframe(database: &str, schema: &str, name: &str, columns: Vec<Column>) -> io::Result<DataFrame> {
    let ns = Namespace::new(database, schema, name);
    let mut df = DataFrame::create(ns, make_dataframe_config(columns))?;
    df.resize(0)?;
    Ok(df)
}

pub fn make_dataframe_config(columns: Vec<Column>) -> DataFrameConfig {
    DataFrameConfig::new(columns, Vec::new(), Vec::new())
}

pub fn make_quote(id: usize,
                  phys_columns: &Vec<TableColumn>,
                  symbol: &str,
                  exchange: &str,
                  last_sale: f64) -> Row {
    Row::new(id, phys_columns.clone(), vec![
        Field::new(StringValue(symbol.into())),
        Field::new(StringValue(exchange.into())),
        Field::new(Float64Value(last_sale))])
}

pub fn make_rows_from_bytes(database: &str,
                            schema: &str,
                            name: &str,
                            columns: Vec<Column>,
                            row_data: &[u8]) -> io::Result<DataFrame> {
    let mut df = make_dataframe(database, schema, name, columns)?;
    df.file.set_len(0)?;
    df.file.write_all(row_data)?;
    df.file.flush()?;
    Ok(df)
}

pub fn make_table_columns() -> Vec<TableColumn> {
    vec![
        TableColumn::new("symbol", StringType(4), Null, 9),
        TableColumn::new("exchange", StringType(4), Null, 22),
        TableColumn::new("lastSale", Float64Type, Null, 35),
    ]
}

pub fn make_table_file_from_bytes(database: &str,
                                  schema: &str,
                                  name: &str,
                                  columns: Vec<Column>,
                                  row_data: &[u8]) -> (File, Vec<TableColumn>, usize) {
    let (mut file, columns, record_size) =
        make_table_file(database, schema, name, columns);
    file.write_all(row_data).unwrap();
    file.flush().unwrap();
    (file, columns, record_size)
}

pub fn make_table_file(database: &str,
                       schema: &str,
                       name: &str,
                       columns: Vec<Column>) -> (File, Vec<TableColumn>, usize) {
    let df = make_dataframe(database, schema, name, columns).unwrap();
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
            TableColumn::new("symbol", StringType(4), Null, 9),
            TableColumn::new("exchange", StringType(4), Null, 22),
            TableColumn::new("lastSale", Float64Type, Null, 35),
        ];
        assert_eq!(generated, natural);
    }
}