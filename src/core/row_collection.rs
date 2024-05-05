////////////////////////////////////////////////////////////////////
// row-collection module
////////////////////////////////////////////////////////////////////

use std::fmt::Debug;
use std::fs::File;
use std::sync::Arc;

use crate::byte_row_collection::ByteRowCollection;
use crate::file_row_collection::FileRowCollection;
use crate::row_metadata::RowMetadata;
use crate::rows::Row;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue;

/// represents the underlying storage resource for the dataframe
pub trait RowCollection: Debug {
    /// Returns true, if the given item matches a [Row] found within it
    fn contains(&self, item: &Row) -> bool { self.index_of(item).is_some() }

    /// returns the columns that represent device
    fn get_columns(&self) -> &Vec<TableColumn>;

    /// returns the record size of the device
    fn get_record_size(&self) -> usize;

    /// Returns true, if the given item matches a [Row] found within it
    fn index_of(&self, item: &Row) -> Option<usize>;

    /// returns the number of active rows in the table
    fn len(&self) -> std::io::Result<usize>;

    /// overwrites the specified row by ID
    fn overwrite(&mut self, id: usize, row: &Row) -> std::io::Result<usize>;

    /// overwrites the metadata of a specified row by ID
    fn overwrite_metadata(&mut self, id: usize, metadata: &RowMetadata) -> std::io::Result<usize>;

    fn read(&self, id: usize) -> std::io::Result<(Row, RowMetadata)>;

    fn read_field(&self, id: usize, column_id: usize) -> std::io::Result<TypedValue>;

    fn read_range(&self, index: std::ops::Range<usize>) -> std::io::Result<Vec<Row>>;

    /// resizes (shrink or grow) the table
    fn resize(&mut self, new_size: usize) -> std::io::Result<()>;

    fn to_row_offset(&self, id: usize) -> u64 { (id * self.get_record_size()) as u64 }
}

impl dyn RowCollection {
    /// creates a new in-memory [RowCollection] from a byte vector.
    pub fn from_bytes(columns: Vec<TableColumn>, rows: Vec<Vec<u8>>) -> impl RowCollection {
        ByteRowCollection::new(columns, rows)
    }

    /// creates a new [RowCollection] from a file.
    pub fn from_file(columns: Vec<TableColumn>, file: File) -> impl RowCollection {
        FileRowCollection::new(columns, Arc::new(file))
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use log::info;
    use rand::{Rng, RngCore, thread_rng};

    use shared_lib::cnv_error;

    use crate::model_row_collection::ModelRowCollection;
    use crate::namespaces::Namespace;
    use crate::row;
    use crate::testdata::{make_quote_columns, make_quote, make_table_columns, make_table_file};
    use crate::typed_values::TypedValue::{Float64Value, StringValue};

    use super::*;

    #[test]
    fn test_from_bytes() {
        // determine the record size of the row
        let columns = make_table_columns();
        let row = make_quote(0, &columns, "RICE", "NYSE", 78.78);
        let mut rc = <dyn RowCollection>::from_bytes(columns.clone(), vec![]);

        // create a new row
        assert_eq!(rc.overwrite(row.get_id(), &row).unwrap(), 1);

        // read and verify the row
        let (row, rmd) = rc.read(0).unwrap();
        assert!(rmd.is_allocated);
        assert_eq!(row, row!(0, make_table_columns(), vec![
            StringValue("RICE".into()), StringValue("NYSE".into()), Float64Value(78.78)
        ]))
    }

    #[test]
    fn test_from_file() {
        let (file, columns, _) =
            make_table_file("rows", "append_row", "stocks", make_quote_columns());
        let mut rc = <dyn RowCollection>::from_file(columns.clone(), file);
        rc.overwrite(0, &make_quote(0, &columns, "BEAM", "NYSE", 78.35)).unwrap();

        // read and verify the row
        let (row, rmd) = rc.read(0).unwrap();
        assert!(rmd.is_allocated);
        assert_eq!(row, row!(0, make_table_columns(), vec![
            StringValue("BEAM".into()), StringValue("NYSE".into()), Float64Value(78.35)
        ]))
    }

    #[test]
    fn test_write_then_read_row() {
        let columns = make_table_columns();

        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            // write a new row
            let row = make_quote(2, &columns, "AMD", "NYSE", 88.78);
            assert_eq!(rc.overwrite(row.get_id(), &row).unwrap(), 1);

            // read and verify the row
            let (new_row, meta) = rc.read(row.get_id()).unwrap();
            assert!(meta.is_allocated);
            assert_eq!(new_row, row);
        }

        // test the variants
        verify_variants("write_then_read", columns.clone(), test_variant);
    }

    #[test]
    fn test_write_then_read_field() {
        let columns = make_table_columns();

        fn test_variant(label: &str, mut frc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            // write two rows
            assert_eq!(1, frc.overwrite(0, &make_quote(0, &columns, "INTC", "NYSE", 66.77)).unwrap());
            assert_eq!(1, frc.overwrite(1, &make_quote(1, &columns, "AMD", "NASDAQ", 77.66)).unwrap());

            // read the first column of the first row
            assert_eq!(frc.read_field(0, 0).unwrap(), StringValue("INTC".into()));

            // read the second column of the second row
            assert_eq!(frc.read_field(1, 1).unwrap(), StringValue("NASDAQ".into()));
        }

        // test the variants
        verify_variants("read_field", columns.clone(), test_variant);
    }

    #[test]
    fn test_write_delete_then_read_range() {
        let columns = make_table_columns();

        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            // write some rows
            assert_eq!(1, rc.overwrite(0, &make_quote(0, &columns, "GE", "NYSE", 21.22)).unwrap());
            assert_eq!(1, rc.overwrite(1, &make_quote(1, &columns, "ATT", "NYSE", 98.44)).unwrap());
            assert_eq!(1, rc.overwrite(2, &make_quote(2, &columns, "H", "OTC_BB", 0.0076)).unwrap());
            assert_eq!(1, rc.overwrite(3, &make_quote(3, &columns, "GG", "NASDAQ", 33.33)).unwrap());

            // delete a row
            assert_eq!(1, rc.overwrite_metadata(2, &RowMetadata::new(false)).unwrap());

            // retrieve the entire range of rows
            let rows = rc.read_range(0..rc.len().unwrap()).unwrap();
            assert_eq!(rows, vec![
                make_quote(0, &columns, "GE", "NYSE", 21.22),
                make_quote(1, &columns, "ATT", "NYSE", 98.44),
                make_quote(3, &columns, "GG", "NASDAQ", 33.33),
            ]);
        }

        // test the variants
        verify_variants("delete", columns.clone(), test_variant);
    }

    #[test]
    fn test_resize_shrink() {
        let columns = make_table_columns();

        fn test_variant(label: &str, mut frc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            // insert some rows and verify the size
            assert_eq!(1, frc.overwrite(5, &make_quote(0, &columns, "DUMMY", "OTC_BB", 0.0001)).unwrap());
            assert_eq!(6, frc.len().unwrap());

            // resize and verify
            let _ = frc.resize(0).unwrap();
            assert_eq!(frc.len().unwrap(), 0);
        }

        // test the variants
        verify_variants("resize_shrink", columns.clone(), test_variant);
    }

    #[test]
    fn test_resize_grow() {
        let columns = make_table_columns();

        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            rc.resize(0).unwrap();

            // insert some rows and verify the size
            assert_eq!(1, rc.overwrite(5, &make_quote(0, &columns, "DUMMY", "OTC_BB", 0.0001)).unwrap());
            assert!(rc.len().unwrap() >= 6);

            // resize and verify
            let _ = rc.resize(50).unwrap();
            assert!(rc.len().unwrap() >= 50);
        }

        // test the variants
        verify_variants("resize_grow", columns.clone(), test_variant);
    }

    #[ignore]
    #[test]
    fn test_performance() {
        let columns = make_table_columns();

        fn test_variant(label: &str, mut frc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            test_write_performance(label, &mut frc, &columns, 10_000).unwrap();
            test_read_performance(label, &frc).unwrap();
        }

        // test the variants
        verify_variants("performance", columns.clone(), test_variant);
    }

    fn test_write_performance(label: &str, rc: &mut Box<dyn RowCollection>, columns: &Vec<TableColumn>, total: usize) -> std::io::Result<()> {
        use rand::distributions::Uniform;
        use rand::prelude::ThreadRng;
        let exchanges = ["AMEX", "NASDAQ", "NYSE", "OTCBB", "OTHEROTC"];
        let mut rng: ThreadRng = thread_rng();
        let start_time = SystemTime::now().duration_since(UNIX_EPOCH)
            .map_err(|e| cnv_error!(e))?.as_millis();
        for id in 0..total {
            let symbol: String = (0..4)
                .map(|_| rng.gen_range(b'A'..=b'Z') as char)
                .collect();
            let exchange = exchanges[rng.next_u32() as usize % exchanges.len()];
            let last_sale = 400.0 * rng.sample(Uniform::new(0.0, 1.0));
            let row = make_quote(0, &columns, &symbol, exchange, last_sale);
            rc.overwrite(id, &row)?;
        }
        let end_time = SystemTime::now().duration_since(UNIX_EPOCH)
            .map_err(|e| cnv_error!(e))?.as_millis();
        let elapsed_time = end_time - start_time;
        let elapsed_time_sec = elapsed_time as f64 / 1000.;
        let rpm = total as f64 / elapsed_time as f64;
        info!("{} wrote {} row(s) in {} msec ({:.2} seconds, {:.2} records/msec)",
                 label, total, elapsed_time, elapsed_time_sec, rpm);
        Ok(())
    }

    fn test_read_performance(label: &str, rc: &Box<dyn RowCollection>) -> std::io::Result<()> {
        let limit = rc.len()?;
        let mut total = 0;
        let start_time = SystemTime::now().duration_since(UNIX_EPOCH)
            .map_err(|e| cnv_error!(e))?.as_millis();
        for id in 0..limit {
            let (_row, rmd) = rc.read(id)?;
            if rmd.is_allocated { total += 1; }
        }
        let end_time = SystemTime::now().duration_since(UNIX_EPOCH)
            .map_err(|e| cnv_error!(e))?.as_millis();
        let elapsed_time = end_time - start_time;
        let elapsed_time_sec = elapsed_time as f64 / 1000.;
        let rpm = total as f64 / elapsed_time as f64;
        info!("{} read {} row(s) in {} msec ({:.2} seconds, {:.2} records/msec)",
                 label, total, elapsed_time, elapsed_time_sec, rpm);
        Ok(())
    }

    fn verify_variants(name: &str, columns: Vec<TableColumn>, test_variant: fn(&str, Box<dyn RowCollection>, Vec<TableColumn>) -> ()) {
        verify_file_variant(name, columns.clone(), test_variant);
        verify_memory_variant(columns.clone(), test_variant);
        verify_model_variant(columns, test_variant);
    }

    fn verify_file_variant(name: &str, columns: Vec<TableColumn>, test_variant: fn(&str, Box<dyn RowCollection>, Vec<TableColumn>) -> ()) {
        let ns = Namespace::new("file_row_collection", name, "stocks");
        let frc = FileRowCollection::create(ns, columns.clone()).unwrap();
        test_variant("Disk", Box::new(frc), columns.clone());
    }

    fn verify_memory_variant(columns: Vec<TableColumn>, test_variant: fn(&str, Box<dyn RowCollection>, Vec<TableColumn>) -> ()) {
        let brc = ByteRowCollection::new(columns.clone(), vec![]);
        test_variant("Bytes", Box::new(brc), columns);
    }

    fn verify_model_variant(columns: Vec<TableColumn>, test_variant: fn(&str, Box<dyn RowCollection>, Vec<TableColumn>) -> ()) {
        let mrc = ModelRowCollection::new(columns.clone(), vec![]);
        test_variant("Model", Box::new(mrc), columns);
    }
}
