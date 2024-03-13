////////////////////////////////////////////////////////////////////
// test data module
////////////////////////////////////////////////////////////////////

use std::error::Error;
use std::fs::File;
use std::io;
use std::io::Write;

use chrono::Utc;
use rand::{Rng, RngCore, thread_rng};
use rand::distributions::Uniform;
use rand::prelude::ThreadRng;
use serde::{Deserialize, Serialize};

use crate::columns::Column;
use crate::data_types::DataType::{Float64Type, StringType};
use crate::dataframe_config::DataFrameConfig;
use crate::dataframes::DataFrame;
use crate::fields::Field;
use crate::namespaces::Namespace;
use crate::rows::Row;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue::{Float64Value, Null, StringValue};

struct StockQuote;

impl StockQuote {
    pub fn new() -> Row {
        make_quote(0, &make_table_columns(),
                   &Self::generate_random_symbol(),
                   &Self::generate_random_exchange(),
                   Self::generate_random_last_sale())
    }

    fn generate_quote_from_symbol_and_exchange(symbol: &str, exchange: &str) -> Row {
        make_quote(0, &make_table_columns(), symbol, exchange, Self::generate_random_last_sale())
    }

    fn get_exchange_from_index(exchange_index: usize) -> String {
        let exchanges = ["AMEX", "NASDAQ", "NYSE", "OTCBB", "OTHEROTC"];
        exchanges[exchange_index % exchanges.len()].parse().unwrap()
    }

    fn generate_random_exchange() -> String {
        let mut rng: ThreadRng = thread_rng();
        let exchange_index = rng.next_u32();
        Self::get_exchange_from_index(exchange_index as usize)
    }

    fn generate_random_last_sale() -> f64 {
        let mut rng: ThreadRng = thread_rng();
        400.0 * rng.sample(Uniform::new(0.0, 1.0))
    }

    fn generate_random_symbol() -> String {
        let mut rng: ThreadRng = thread_rng();
        (0..5)
            .map(|_| rng.gen_range(b'A'..=b'Z') as char)
            .collect()
    }

    fn generate_random_transaction_time() -> i64 {
        let mut rng: ThreadRng = thread_rng();
        Utc::now().timestamp_millis() - (rng.sample(Uniform::new(0, 10000)) as i64)
    }
}

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