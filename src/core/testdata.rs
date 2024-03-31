////////////////////////////////////////////////////////////////////
// test data module
////////////////////////////////////////////////////////////////////

use std::fs::File;
use std::io::Write;

use crate::data_types::DataType::{Float64Type, StringType};
use crate::dataframe_config::DataFrameConfig;
use crate::dataframes::DataFrame;
use crate::file_row_collection::FileRowCollection;
use crate::namespaces::Namespace;
use crate::row;
use crate::row_collection::RowCollection;
use crate::rows::Row;
use crate::server::ColumnJs;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue::{Float64Value, Null, StringValue};

pub fn make_columns() -> Vec<ColumnJs> {
    vec![
        ColumnJs::new("symbol", "String(4)", None),
        ColumnJs::new("exchange", "String(4)", None),
        ColumnJs::new("lastSale", "Double", None),
    ]
}

pub fn make_dataframe(database: &str, schema: &str, name: &str, columns: Vec<ColumnJs>) -> std::io::Result<DataFrame> {
    let ns = Namespace::new(database, schema, name);
    let mut df = DataFrame::create(ns, make_dataframe_config(columns))?;
    df.resize(0)?;
    Ok(df)
}

pub fn make_dataframe_config(columns: Vec<ColumnJs>) -> DataFrameConfig {
    DataFrameConfig::new(columns, Vec::new(), Vec::new())
}

pub fn make_quote(id: usize,
                  phys_columns: &Vec<TableColumn>,
                  symbol: &str,
                  exchange: &str,
                  last_sale: f64) -> Row {
    row!(id, phys_columns, vec![
        StringValue(symbol.into()), StringValue(exchange.into()), Float64Value(last_sale)
    ])
}

pub fn make_rows_from_bytes(database: &str,
                            schema: &str,
                            name: &str,
                            columns: Vec<ColumnJs>,
                            row_data: &[u8]) -> std::io::Result<DataFrame> {
    let (file, table_columns, _) =
        make_table_file_from_bytes(database, schema, name, columns, row_data);
    let device = Box::new(<dyn RowCollection>::from_file(table_columns.clone(), file));
    let df = DataFrame::new(Namespace::new(database, schema, name), table_columns, device);
    Ok(df)
}

pub fn make_table(database: &str,
                  schema: &str,
                  name: &str,
                  columns: Vec<ColumnJs>) -> (DataFrame, Vec<TableColumn>, usize) {
    let mut df = make_dataframe(database, schema, name, columns).unwrap();
    df.resize(0).unwrap();
    let table_columns = df.get_columns().clone();
    let record_size = Row::compute_record_size(&table_columns);
    (df, table_columns, record_size)
}

pub fn make_table_columns() -> Vec<TableColumn> {
    vec![
        TableColumn::new("symbol", StringType(4), Null, 9),
        TableColumn::new("exchange", StringType(4), Null, 22),
        TableColumn::new("lastSale", Float64Type, Null, 35),
    ]
}

pub fn make_table_file(database: &str,
                       schema: &str,
                       name: &str,
                       columns: Vec<ColumnJs>) -> (File, Vec<TableColumn>, usize) {
    let table_columns = TableColumn::from_columns(&columns).unwrap();
    let record_size = Row::compute_record_size(&table_columns);
    let ns = Namespace::new(database, schema, name);
    let file = FileRowCollection::open_crw(&ns).unwrap();
    file.set_len(0).unwrap();
    (file, table_columns, record_size)
}

pub fn make_table_file_from_bytes(database: &str,
                                  schema: &str,
                                  name: &str,
                                  columns: Vec<ColumnJs>,
                                  row_data: &[u8]) -> (File, Vec<TableColumn>, usize) {
    let table_columns = TableColumn::from_columns(&columns).unwrap();
    let record_size = Row::compute_record_size(&table_columns);
    let ns = Namespace::new(database, schema, name);
    let mut file = FileRowCollection::open_crw(&ns).unwrap();
    file.set_len(0).unwrap();
    file.write_all(row_data).unwrap();
    (file, table_columns, record_size)
}

pub fn make_table_from_bytes(database: &str,
                             schema: &str,
                             name: &str,
                             columns: Vec<ColumnJs>,
                             row_data: &[u8]) -> (DataFrame, Vec<TableColumn>, usize) {
    let (mut df, columns, record_size) =
        make_table(database, schema, name, columns);
    let (row, _) = Row::decode(&row_data.to_vec(), &columns);
    df.overwrite(row).unwrap();
    (df, columns, record_size)
}
