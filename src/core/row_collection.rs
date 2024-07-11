////////////////////////////////////////////////////////////////////
// row-collection module
////////////////////////////////////////////////////////////////////

use std::fmt::Debug;
use std::fs::File;
use std::sync::Arc;

use crate::byte_row_collection::ByteRowCollection;
use crate::fields::Field;
use crate::file_row_collection::FileRowCollection;
use crate::row_metadata::RowMetadata;
use crate::rows::Row;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue;

/// represents the underlying storage resource for the dataframe
pub trait RowCollection: Debug {
    /// Appends the given row to the end of the table
    fn append(&mut self, row: Row) -> std::io::Result<TypedValue> {
        let id = self.len()?;
        let row = row.with_row_id(id);
        self.overwrite(id, &row)
    }

    /// Returns true, if the given item matches a [Row] found within it
    fn contains(&self, item: &Row) -> bool { self.index_of(item).is_some() }

    /// deletes an existing row by ID from the table
    fn delete_row(&mut self, id: usize) -> std::io::Result<TypedValue> {
        self.overwrite_metadata(id, &RowMetadata::new(false))
    }

    /// Removes rows that satisfy the include function
    fn delete_rows(&mut self, include: fn(&Row, &RowMetadata) -> bool) -> std::io::Result<TypedValue> {
        let (mut id, mut removals) = (0, 0);
        let eof = self.len()?;
        while id < eof {
            let (row, metadata) = self.read(id)?;
            if metadata.is_allocated && include(&row, &metadata) {
                if self.overwrite_metadata(row.get_id(), &metadata.as_delete())? == TypedValue::Ack {
                    removals += 1
                }
            }
            id += 1;
        }
        Ok(TypedValue::RowsAffected(removals))
    }

    /// Evaluates a callback function for each active row in the table
    fn fold_left(
        &self,
        initial: TypedValue,
        callback: fn(TypedValue, Row) -> TypedValue,
    ) -> TypedValue {
        match self.len() {
            Ok(eof) => {
                let mut result = initial;
                let mut id = 0;
                while id < eof {
                    if let Ok((row, metadata)) = self.read(id) {
                        if metadata.is_allocated {
                            result = callback(result, row)
                        }
                    }
                    id += 1
                }
                result
            }
            Err(err) => TypedValue::ErrorValue(err.to_string())
        }
    }

    /// Evaluates a callback function for each active row in the table
    fn fold_right(
        &self,
        initial: TypedValue,
        callback: fn(TypedValue, Row) -> TypedValue,
    ) -> TypedValue {
        match self.len() {
            Ok(eof) => {
                let mut result = initial;
                let mut id = 1;
                while id <= eof {
                    let row_id = eof - id;
                    if let Ok((row, metadata)) = self.read(row_id) {
                        if metadata.is_allocated {
                            result = callback(result, row)
                        }
                    }
                    id += 1
                }
                result
            }
            Err(err) => TypedValue::ErrorValue(err.to_string())
        }
    }

    /// returns the columns that represent device
    fn get_columns(&self) -> &Vec<TableColumn>;

    /// returns the record size of the device
    fn get_record_size(&self) -> usize;

    /// Returns true, if the given item matches a [Row] found within it
    fn index_of(&self, item: &Row) -> Option<usize>;

    /// returns the number of active rows in the table
    fn len(&self) -> std::io::Result<usize>;

    /// replaces the specified row by ID
    fn overwrite(&mut self, id: usize, row: &Row) -> std::io::Result<TypedValue>;

    /// overwrites the metadata of a specified row by ID
    fn overwrite_metadata(&mut self, id: usize, metadata: &RowMetadata) -> std::io::Result<TypedValue>;

    fn read(&self, id: usize) -> std::io::Result<(Row, RowMetadata)>;

    fn read_all_rows(&self) -> std::io::Result<Vec<Row>> {
        self.read_range(0..self.len()?)
    }

    fn read_field(&self, id: usize, column_id: usize) -> std::io::Result<TypedValue>;

    fn read_range(&self, index: std::ops::Range<usize>) -> std::io::Result<Vec<Row>>;

    /// resizes (shrink or grow) the table
    fn resize(&mut self, new_size: usize) -> std::io::Result<TypedValue>;

    fn to_row_offset(&self, id: usize) -> u64 { (id * self.get_record_size()) as u64 }

    /// overwrites the specified row by ID
    fn update(&mut self, id: usize, row: &Row) -> std::io::Result<TypedValue> {
        // retrieve the original record
        let (row0, rmd0) = self.read(id)?;
        // verify compatibility between the columns of the incoming row vs. table row
        let (cols0, cols1) = (row0.get_columns(), row.get_columns());
        match TableColumn::validate_compatibility(cols0, cols1) {
            TypedValue::ErrorValue(err) => Ok(TypedValue::ErrorValue(err)),
            _ => {
                // if it is deleted, then use the incoming row
                let row: Row = if !rmd0.is_allocated { row.with_row_id(id) } else {
                    // otherwise, construct a new composite row
                    Row::new(id, cols1.clone(), row0.get_fields().iter().zip(row.get_fields().iter())
                        .map(|(field0, field1)| {
                            Field::new(match (field0.value.clone(), field1.value.clone()) {
                                (a, TypedValue::Undefined) => a,
                                (_, b) => b
                            })
                        }).collect::<Vec<Field>>())
                };
                self.overwrite(id, &row)
            }
        }
    }
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
    use crate::testdata::{make_quote, make_quote_columns, make_table_columns, make_table_file};
    use crate::typed_values::TypedValue::{Float64Value, StringValue};

    use super::*;

    #[test]
    fn test_from_bytes() {
        // determine the record size of the row
        let columns = make_table_columns();
        let row = make_quote(0, &columns, "RICE", "NYSE", 78.78);
        let mut rc = <dyn RowCollection>::from_bytes(columns.clone(), vec![]);

        // create a new row
        assert_eq!(rc.overwrite(row.get_id(), &row).unwrap(), TypedValue::Ack);

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
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            // write a new row
            let row = make_quote(2, &columns, "AMD", "NYSE", 88.78);
            assert_eq!(rc.overwrite(row.get_id(), &row).unwrap(), TypedValue::Ack);

            // read and verify the row
            let (new_row, meta) = rc.read(row.get_id()).unwrap();
            assert!(meta.is_allocated);
            assert_eq!(new_row, row);
        }

        // test the variants
        verify_variants("write_then_read", make_table_columns(), test_variant);
    }

    #[test]
    fn test_write_then_read_field() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            // write two rows
            assert_eq!(TypedValue::Ack, rc.append(make_quote(0, &columns, "INTC", "NYSE", 66.77)).unwrap());
            assert_eq!(TypedValue::Ack, rc.append(make_quote(1, &columns, "AMD", "NASDAQ", 77.66)).unwrap());

            // read the first column of the first row
            assert_eq!(rc.read_field(0, 0).unwrap(), StringValue("INTC".into()));

            // read the second column of the second row
            assert_eq!(rc.read_field(1, 1).unwrap(), StringValue("NASDAQ".into()));
        }

        // test the variants
        verify_variants("read_field", make_table_columns(), test_variant);
    }

    #[test]
    fn test_delete_row() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            // write some rows
            assert_eq!(TypedValue::Ack, rc.append(make_quote(0, &columns, "GE", "NYSE", 21.22)).unwrap());
            assert_eq!(TypedValue::Ack, rc.append(make_quote(1, &columns, "ATT", "NYSE", 98.44)).unwrap());
            assert_eq!(TypedValue::Ack, rc.append(make_quote(2, &columns, "H", "OTC_BB", 0.0076)).unwrap());
            assert_eq!(TypedValue::Ack, rc.append(make_quote(3, &columns, "X", "NASDAQ", 33.33)).unwrap());
            assert_eq!(TypedValue::Ack, rc.append(make_quote(4, &columns, "OXIDE", "OSS", 0.00)).unwrap());

            // delete even rows
            assert_eq!(TypedValue::Ack, rc.delete_row(0).unwrap());
            assert_eq!(TypedValue::Ack, rc.delete_row(2).unwrap());
            assert_eq!(TypedValue::Ack, rc.delete_row(4).unwrap());

            // retrieve the entire range of rows
            let rows = rc.read_all_rows().unwrap();
            assert_eq!(rows, vec![
                make_quote(1, &columns, "ATT", "NYSE", 98.44),
                make_quote(3, &columns, "X", "NASDAQ", 33.33),
            ]);
        }

        // test the variants
        verify_variants("delete_row", make_table_columns(), test_variant);
    }

    #[test]
    fn test_delete_rows() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            // write some rows
            assert_eq!(TypedValue::Ack, rc.append(make_quote(0, &columns, "GE", "NYSE", 21.22)).unwrap());
            assert_eq!(TypedValue::Ack, rc.append(make_quote(1, &columns, "ATT", "NYSE", 98.44)).unwrap());
            assert_eq!(TypedValue::Ack, rc.append(make_quote(2, &columns, "H", "OTC_BB", 0.0076)).unwrap());
            assert_eq!(TypedValue::Ack, rc.append(make_quote(3, &columns, "X", "NASDAQ", 33.33)).unwrap());
            assert_eq!(TypedValue::Ack, rc.append(make_quote(4, &columns, "OXIDE", "OSS", 0.00)).unwrap());

            // delete even rows
            assert_eq!(TypedValue::RowsAffected(3), rc.delete_rows(|row, meta| {
                row.get_id() % 2 == 0
            }).unwrap());

            // retrieve the entire range of rows
            let rows = rc.read_all_rows().unwrap();
            assert_eq!(rows, vec![
                make_quote(1, &columns, "ATT", "NYSE", 98.44),
                make_quote(3, &columns, "X", "NASDAQ", 33.33),
            ]);
        }

        // test the variants
        verify_variants("delete_rows", make_table_columns(), test_variant);
    }

    #[test]
    fn test_fold_left() {
        use TypedValue::*;

        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            // write some rows
            assert_eq!(Ack, rc.append(make_quote(0, &columns, "GE", "NYSE", 21.22)).unwrap());
            assert_eq!(Ack, rc.append(make_quote(0, &columns, "ATT", "NYSE", 98.44)).unwrap());
            assert_eq!(Ack, rc.append(make_quote(0, &columns, "H", "OTC_BB", 0.0076)).unwrap());
            assert_eq!(Ack, rc.append(make_quote(0, &columns, "X", "NASDAQ", 33.33)).unwrap());
            assert_eq!(Ack, rc.append(make_quote(0, &columns, "OXIDE", "OSS", 0.00)).unwrap());

            // resize and verify
            assert_eq!(Float64Value(152.99759999999998),
                       rc.fold_left(Float64Value(0.), |agg, row| agg + row.get("last_sale")));
        }

        // test the variants
        verify_variants("fold_left", make_table_columns(), test_variant);
    }

    #[test]
    fn test_fold_right() {
        use TypedValue::*;

        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            // write some rows
            assert_eq!(Ack, rc.append(make_quote(0, &columns, "GE", "NYSE", 21.22)).unwrap());
            assert_eq!(Ack, rc.append(make_quote(0, &columns, "ATT", "NYSE", 98.44)).unwrap());
            assert_eq!(Ack, rc.append(make_quote(0, &columns, "H", "OTC_BB", 0.0076)).unwrap());
            assert_eq!(Ack, rc.append(make_quote(0, &columns, "X", "NASDAQ", 33.33)).unwrap());
            assert_eq!(Ack, rc.append(make_quote(0, &columns, "OXIDE", "OSS", 0.00)).unwrap());

            // resize and verify
            assert_eq!(Float64Value(347.00239999999997),
                       rc.fold_right(Float64Value(500.), |agg, row| agg - row.get("last_sale")));
        }

        // test the variants
        verify_variants("fold_right", make_table_columns(), test_variant);
    }

    #[test]
    fn test_update_row() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            // write some rows
            assert_eq!(TypedValue::Ack, rc.append(make_quote(0, &columns, "GE", "NYSE", 21.22)).unwrap());
            assert_eq!(TypedValue::Ack, rc.append(make_quote(0, &columns, "ATT", "NYSE", 98.44)).unwrap());
            assert_eq!(TypedValue::Ack, rc.append(make_quote(0, &columns, "H", "OTC_BB", 0.0076)).unwrap());
            assert_eq!(TypedValue::Ack, rc.append(make_quote(0, &columns, "X", "NASDAQ", 33.33)).unwrap());
            assert_eq!(TypedValue::Ack, rc.append(make_quote(0, &columns, "OXIDE", "OSS", 0.00)).unwrap());

            // retrieve the entire range of rows
            let rows = rc.read_all_rows().unwrap();
            assert_eq!(rows, vec![
                make_quote(0, &columns, "GE", "NYSE", 21.22),
                make_quote(1, &columns, "ATT", "NYSE", 98.44),
                make_quote(2, &columns, "H", "OTC_BB", 0.0076),
                make_quote(3, &columns, "X", "NASDAQ", 33.33),
                make_quote(4, &columns, "OXIDE", "OSS", 0.00),
            ]);

            // update a row
            assert_eq!(TypedValue::Ack,
                       rc.update(2, &make_quote(2, &columns, "H.Q", "OTC_BB", 0.0001)).unwrap());

            // retrieve the entire range of rows
            let rows = rc.read_all_rows().unwrap();
            assert_eq!(rows, vec![
                make_quote(0, &columns, "GE", "NYSE", 21.22),
                make_quote(1, &columns, "ATT", "NYSE", 98.44),
                make_quote(2, &columns, "H.Q", "OTC_BB", 0.0001),
                make_quote(3, &columns, "X", "NASDAQ", 33.33),
                make_quote(4, &columns, "OXIDE", "OSS", 0.00),
            ]);
        }

        // test the variants
        verify_variants("update", make_table_columns(), test_variant);
    }

    #[test]
    fn test_write_delete_then_read_range() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            // write some rows
            assert_eq!(TypedValue::Ack, rc.append(make_quote(0, &columns, "GE", "NYSE", 21.22)).unwrap());
            assert_eq!(TypedValue::Ack, rc.append(make_quote(0, &columns, "ATT", "NYSE", 98.44)).unwrap());
            assert_eq!(TypedValue::Ack, rc.append(make_quote(0, &columns, "H", "OTC_BB", 0.0076)).unwrap());
            assert_eq!(TypedValue::Ack, rc.append(make_quote(0, &columns, "GG", "NASDAQ", 33.33)).unwrap());

            // verify the initial state
            let rows = rc.read_all_rows().unwrap();
            assert_eq!(rows, vec![
                make_quote(0, &columns, "GE", "NYSE", 21.22),
                make_quote(1, &columns, "ATT", "NYSE", 98.44),
                make_quote(2, &columns, "H", "OTC_BB", 0.0076),
                make_quote(3, &columns, "GG", "NASDAQ", 33.33),
            ]);

            // delete a row
            assert_eq!(TypedValue::Ack, rc.overwrite_metadata(2, &RowMetadata::new(false)).unwrap());

            // verify the current state
            let rows = rc.read_all_rows().unwrap();
            assert_eq!(rows, vec![
                make_quote(0, &columns, "GE", "NYSE", 21.22),
                make_quote(1, &columns, "ATT", "NYSE", 98.44),
                make_quote(3, &columns, "GG", "NASDAQ", 33.33),
            ]);
        }

        // test the variants
        verify_variants("delete", make_table_columns(), test_variant);
    }

    #[test]
    fn test_resize_shrink() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            // insert some rows and verify the size
            assert_eq!(TypedValue::Ack, rc.overwrite(5, &make_quote(0, &columns, "DUMMY", "OTC_BB", 0.0001)).unwrap());
            assert_eq!(6, rc.len().unwrap());

            // resize and verify
            let _ = rc.resize(0).unwrap();
            assert_eq!(rc.len().unwrap(), 0);
        }

        // test the variants
        verify_variants("resize_shrink", make_table_columns(), test_variant);
    }

    #[test]
    fn test_resize_grow() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            rc.resize(0).unwrap();

            // insert some rows and verify the size
            assert_eq!(TypedValue::Ack, rc.overwrite(5, &make_quote(0, &columns, "DUMMY", "OTC_BB", 0.0001)).unwrap());
            assert!(rc.len().unwrap() >= 6);

            // resize and verify
            let _ = rc.resize(50).unwrap();
            assert!(rc.len().unwrap() >= 50);
        }

        // test the variants
        verify_variants("resize_grow", make_table_columns(), test_variant);
    }

    #[ignore]
    #[test]
    fn test_performance() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            test_write_performance(label, &mut rc, &columns, 10_000).unwrap();
            test_read_performance(label, &rc).unwrap();
        }

        // test the variants
        verify_variants("performance", make_table_columns(), test_variant);
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
