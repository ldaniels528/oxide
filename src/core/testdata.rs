////////////////////////////////////////////////////////////////////
// test data module
////////////////////////////////////////////////////////////////////

use std::fs::File;
use std::io;
use std::io::Write;

use rand::{Rng, RngCore, thread_rng};
use rand::distributions::Uniform;
use rand::prelude::ThreadRng;

use crate::data_types::DataType::{Float64Type, StringType};
use crate::dataframe_config::DataFrameConfig;
use crate::dataframes::DataFrame;
use crate::fields::Field;
use crate::namespaces::Namespace;
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

pub fn make_dataframe(database: &str, schema: &str, name: &str, columns: Vec<ColumnJs>) -> io::Result<DataFrame> {
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
    Row::new(id, phys_columns.clone(), vec![
        Field::new(StringValue(symbol.into())),
        Field::new(StringValue(exchange.into())),
        Field::new(Float64Value(last_sale))])
}

pub fn make_rows_from_bytes(database: &str,
                            schema: &str,
                            name: &str,
                            columns: Vec<ColumnJs>,
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
                                  columns: Vec<ColumnJs>,
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
                       columns: Vec<ColumnJs>) -> (File, Vec<TableColumn>, usize) {
    let df = make_dataframe(database, schema, name, columns).unwrap();
    df.file.set_len(0).unwrap();
    (df.file, df.columns, df.record_size)
}

// Unit tests
#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::cnv_error;

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

    #[ignore]
    #[test]
    fn performance_test() -> io::Result<()> {
        let columns = vec![
            TableColumn::new("symbol", StringType(4), Null, 9),
            TableColumn::new("exchange", StringType(8), Null, 22),
            TableColumn::new("lastSale", Float64Type, Null, 39),
        ];
        let total = 1_000_000;
        let mut df = make_dataframe(
            "dataframes", "performance_test", "quotes", make_columns()).unwrap();

        test_write_performance(&mut df, &columns, total)?;
        test_read_performance(&df)?;
        Ok(())
    }

    fn test_write_performance(df: &mut DataFrame, columns: &Vec<TableColumn>, total: usize) -> io::Result<()> {
        let exchanges = ["AMEX", "NASDAQ", "NYSE", "OTCBB", "OTHEROTC"];
        let mut rng: ThreadRng = thread_rng();
        let start_time = SystemTime::now().duration_since(UNIX_EPOCH)
            .map_err(|e| cnv_error!(e))?.as_millis();
        for n in 0..total {
            let symbol: String = (0..4)
                .map(|_| rng.gen_range(b'A'..=b'Z') as char)
                .collect();
            let exchange = exchanges[rng.next_u32() as usize % exchanges.len()];
            let last_sale = 400.0 * rng.sample(Uniform::new(0.0, 1.0));
            let row = make_quote(0, &columns, &symbol, exchange, last_sale);
            df.append(&row)?;
        }
        let end_time = SystemTime::now().duration_since(UNIX_EPOCH)
            .map_err(|e| cnv_error!(e))?.as_millis();
        let elapsed_time = end_time - start_time;
        let elapsed_time_sec = elapsed_time as f64 / 1000.;
        let rpm = total as f64 / elapsed_time as f64;
        println!("wrote {} row(s) in {} msec ({:.2} seconds, {:.2} records/msec)",
                 total, elapsed_time, elapsed_time_sec, rpm);
        Ok(())
    }

    fn test_read_performance(df: &DataFrame) -> io::Result<()> {
        let limit = df.size()?;
        let mut total = 0;
        let start_time = SystemTime::now().duration_since(UNIX_EPOCH)
            .map_err(|e| cnv_error!(e))?.as_millis();
        for id in 0..limit {
            let (row, rmd) = df.read_row(id)?;
            if rmd.is_allocated { total += 1; }
        }
        let end_time = SystemTime::now().duration_since(UNIX_EPOCH)
            .map_err(|e| cnv_error!(e))?.as_millis();
        let elapsed_time = end_time - start_time;
        let elapsed_time_sec = elapsed_time as f64 / 1000.;
        let rpm = total as f64 / elapsed_time as f64;
        println!("read  {} row(s) in {} msec ({:.2} seconds, {:.2} records/msec)",
                 total, elapsed_time, elapsed_time_sec, rpm);
        Ok(())
    }
}