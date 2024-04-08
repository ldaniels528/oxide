////////////////////////////////////////////////////////////////////
// test data module
////////////////////////////////////////////////////////////////////

use std::fs::File;
use std::io::Write;

use crate::dataframe_config::DataFrameConfig;
use crate::dataframes::DataFrame;
use crate::file_row_collection::FileRowCollection;
use crate::namespaces::Namespace;
use crate::row;
use crate::rows::Row;
use crate::server::ColumnJs;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue::{Float64Value, StringValue};

pub fn make_columns() -> Vec<ColumnJs> {
    vec![
        ColumnJs::new("symbol", "String(8)", None),
        ColumnJs::new("exchange", "String(8)", None),
        ColumnJs::new("lastSale", "Double", None),
    ]
}

pub fn make_dataframe(database: &str, schema: &str, name: &str, columns: Vec<ColumnJs>) -> std::io::Result<DataFrame> {
    let ns = Namespace::new(database, schema, name);
    DataFrame::create(ns, make_dataframe_config(columns))
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

pub fn make_table_columns() -> Vec<TableColumn> {
    TableColumn::from_columns(&make_columns()).unwrap()
}

pub fn make_table_file(
    database: &str,
    schema: &str,
    name: &str,
    columns: Vec<ColumnJs>,
) -> (File, Vec<TableColumn>, usize) {
    let table_columns = TableColumn::from_columns(&columns).unwrap();
    let record_size = Row::compute_record_size(&table_columns);
    let ns = Namespace::new(database, schema, name);
    let file = FileRowCollection::open_crw(&ns).unwrap();
    (file, table_columns, record_size)
}

pub fn make_table_file_from_bytes(
    database: &str,
    schema: &str,
    name: &str,
    columns: Vec<ColumnJs>,
    row_data: &[u8],
) -> (File, Vec<TableColumn>, usize) {
    let table_columns = TableColumn::from_columns(&columns).unwrap();
    let record_size = Row::compute_record_size(&table_columns);
    let ns = Namespace::new(database, schema, name);
    let mut file = FileRowCollection::open_crw(&ns).unwrap();
    file.write_all(row_data).unwrap();
    (file, table_columns, record_size)
}
