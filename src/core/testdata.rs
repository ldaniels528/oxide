////////////////////////////////////////////////////////////////////
// test data module
////////////////////////////////////////////////////////////////////

use std::fs::File;
use std::io::Write;

use chrono::Utc;
use rand::{Rng, RngCore, thread_rng};
use rand::distributions::Uniform;
use rand::prelude::ThreadRng;
use serde::{Deserialize, Serialize};

use crate::dataframe_config::DataFrameConfig;
use crate::dataframes::DataFrame;
use crate::file_row_collection::FileRowCollection;
use crate::namespaces::Namespace;
use crate::row;
use crate::rows::Row;
use crate::server::ColumnJs;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue::{Boolean, Float64Value, StringValue, UInt64Value};

pub fn make_quote_columns() -> Vec<ColumnJs> {
    vec![
        ColumnJs::new("symbol", "String(8)", None),
        ColumnJs::new("exchange", "String(8)", None),
        ColumnJs::new("last_sale", "f64", None),
    ]
}

pub fn make_dataframe(database: &str, schema: &str, name: &str, columns: Vec<ColumnJs>) -> std::io::Result<DataFrame> {
    make_dataframe_ns(Namespace::new(database, schema, name), columns)
}

pub fn make_dataframe_ns(ns: Namespace, columns: Vec<ColumnJs>) -> std::io::Result<DataFrame> {
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
        StringValue(symbol.into()),
        StringValue(exchange.into()),
        Float64Value(last_sale)
    ])
}

pub fn make_scan_quote(id: usize,
                       columns: &Vec<TableColumn>,
                       symbol: &str,
                       exchange: &str,
                       last_sale: f64,
                       _active: bool) -> Row {
    row!(id, columns, vec![
                StringValue(symbol.into()),
                StringValue(exchange.into()),
                Float64Value(last_sale),
                UInt64Value(id as u64),
                Boolean(_active)
            ])
}

pub fn make_table_columns() -> Vec<TableColumn> {
    TableColumn::from_columns(&make_quote_columns()).unwrap()
}

pub fn make_table_file(
    database: &str,
    schema: &str,
    name: &str,
    columns: Vec<ColumnJs>,
) -> (String, File, Vec<TableColumn>, usize) {
    let table_columns = TableColumn::from_columns(&columns).unwrap();
    let record_size = Row::compute_record_size(&table_columns);
    let ns = Namespace::new(database, schema, name);
    let file = FileRowCollection::table_file_create(&ns).unwrap();
    (ns.get_table_file_path(), file, table_columns, record_size)
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
    let mut file = FileRowCollection::table_file_create(&ns).unwrap();
    file.write_all(row_data).unwrap();
    (file, table_columns, record_size)
}

/////////////////////////////////////////////////////////////
//      STOCK QUOTE GENERATION
/////////////////////////////////////////////////////////////

#[derive(Clone, Debug, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct StockQuote {
    pub symbol: String,
    pub exchange: String,
    pub last_sale: f64,
    pub transaction_time: i64,
}

impl StockQuote {
    pub fn generate_quote() -> StockQuote {
        Self::generate_quote_from_symbol_and_exchange(
            Self::generate_random_symbol(),
            Self::generate_random_exchange())
    }

    fn generate_quote_array(symbol: String) -> Box<[StockQuote]> {
        let mut rng: ThreadRng = thread_rng();
        let count: usize = rng.sample(Uniform::new(2, 5));
        (0..count).map(|index|
            StockQuote {
                symbol: symbol.to_string(),
                exchange: Self::get_exchange_from_index(index),
                last_sale: 400.0 * rng.sample(Uniform::new(0.0, 1.0)),
                transaction_time: Utc::now().timestamp_millis() + rng.sample(Uniform::new(0, 1000)),
            }).collect()
    }

    fn generate_quote_from_symbol(symbol: String) -> StockQuote {
        StockQuote {
            symbol,
            exchange: Self::generate_random_exchange(),
            last_sale: Self::generate_random_last_sale(),
            transaction_time: Self::generate_random_transaction_time(),
        }
    }

    fn generate_quote_from_symbol_and_exchange(symbol: String, exchange: String) -> StockQuote {
        StockQuote {
            symbol,
            exchange,
            last_sale: Self::generate_random_last_sale(),
            transaction_time: Self::generate_random_transaction_time(),
        }
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
        let size: usize = rng.sample(Uniform::new(3, 5));
        (0..size)
            .map(|_| rng.gen_range(b'A'..=b'Z') as char)
            .collect()
    }

    fn generate_random_transaction_time() -> i64 {
        let mut rng: ThreadRng = thread_rng();
        Utc::now().timestamp_millis() - (rng.sample(Uniform::new(0, 10000)) as i64)
    }
}

#[cfg(test)]
mod tests {}