////////////////////////////////////////////////////////////////////
// row-collection module
////////////////////////////////////////////////////////////////////

use std::fmt::Debug;
use std::fs::File;
use std::ops::Range;
use std::sync::Arc;

use shared_lib::fail;

use crate::byte_row_collection::ByteRowCollection;
use crate::fields::Field;
use crate::file_row_collection::FileRowCollection;
use crate::model_row_collection::ModelRowCollection;
use crate::row_metadata::RowMetadata;
use crate::rows::Row;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{ErrorValue, RowsAffected};

/// represents the underlying storage resource for the dataframe
pub trait RowCollection: Debug {
    /// Appends the given row to the end of the table
    fn append_row(&mut self, row: Row) -> std::io::Result<TypedValue> {
        match self.len() {
            Ok(id) => self.overwrite_row(id, row.with_row_id(id)),
            Err(err) => Ok(ErrorValue(err.to_string()))
        }
    }

    /// Appends the vector of rows to the end of the table
    fn append_rows(&mut self, rows: Vec<Row>) -> std::io::Result<TypedValue> {
        let mut affected_count = 0;
        for row in rows {
            let id = self.len()?;
            match self.overwrite_row(id, row.with_row_id(id)) {
                Ok(ErrorValue(message)) => return Ok(ErrorValue(message)),
                Ok(RowsAffected(n)) => affected_count += n,
                Ok(..) => {}
                Err(err) => return Ok(ErrorValue(err.to_string())),
            }
        }
        Ok(RowsAffected(affected_count))
    }

    fn compute_offset(&self, id: usize) -> u64 {
        (id * self.get_record_size()) as u64
    }

    /// Returns true, if the given item matches a [Row] found within it
    fn contains(&self, item: &Row) -> bool { self.index_of(item).is_some() }

    /// deletes an existing row by ID from the table
    fn delete_row(&mut self, id: usize) -> std::io::Result<TypedValue> {
        self.overwrite_row_metadata(id, RowMetadata::new(false))
    }

    /// Removes rows that satisfy the include function
    fn delete_rows(&mut self, include: fn(Row, RowMetadata) -> bool) -> std::io::Result<TypedValue> {
        let mut removals = 0;
        for id in self.get_indices()? {
            let (row, metadata) = self.read_row(id)?;
            if metadata.is_allocated && include(row.clone(), metadata) {
                if self.overwrite_row_metadata(row.get_id(), metadata.as_delete())?.is_ok() {
                    removals += 1
                }
            }
        }
        Ok(RowsAffected(removals))
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
                for id in 0..eof {
                    if let Ok((row, metadata)) = self.read_row(id) {
                        if metadata.is_allocated {
                            result = callback(result, row)
                        }
                    }
                }
                result
            }
            Err(err) => ErrorValue(err.to_string())
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
                for id in 1..=eof {
                    let row_id = eof - id;
                    if let Ok((row, metadata)) = self.read_row(row_id) {
                        if metadata.is_allocated { result = callback(result, row) }
                    }
                }
                result
            }
            Err(err) => ErrorValue(err.to_string())
        }
    }

    /// returns true if all allocated rows satisfy the provided function
    fn for_all(&self, callback: fn(Row) -> bool) -> bool {
        match self.len() {
            Ok(eof) => {
                for id in 0..eof {
                    if let Ok((row, metadata)) = self.read_row(id) {
                        if metadata.is_allocated && !callback(row) { return false; }
                    }
                }
                true
            }
            Err(_) => false
        }
    }

    /// Evaluates a callback function for each active row in the table
    fn for_each(&self, callback: fn(Row) -> ()) {
        match self.len() {
            Ok(eof) =>
                for id in 0..eof {
                    if let Ok((row, metadata)) = self.read_row(id) {
                        if metadata.is_allocated { callback(row) }
                    }
                }
            Err(_) => ()
        }
    }

    /// returns the columns that represent device
    fn get_columns(&self) -> &Vec<TableColumn>;

    fn get_indices(&self) -> std::io::Result<Range<usize>> {
        Ok(0..self.len()?)
    }

    fn get_indices_with_limit(&self, limit: TypedValue) -> std::io::Result<Range<usize>> {
        Ok(0..limit.assume_usize().unwrap_or(self.len()?))
    }

    /// returns the record size of the device
    fn get_record_size(&self) -> usize;

    /// Returns true, if the given item matches a [Row] found within it
    fn index_of(&self, item: &Row) -> Option<usize>;

    /// returns the number of active rows in the table
    fn len(&self) -> std::io::Result<usize>;

    /// replaces the specified row by ID
    fn overwrite_row(&mut self, id: usize, row: Row) -> std::io::Result<TypedValue>;

    /// overwrites the metadata of a specified row by ID
    fn overwrite_row_metadata(&mut self, id: usize, metadata: RowMetadata) -> std::io::Result<TypedValue>;

    /// replaces rows that satisfy the include function
    fn overwrite_rows(
        &mut self,
        include: fn(Row, RowMetadata) -> bool,
        transform: fn(Row) -> Row,
    ) -> std::io::Result<TypedValue> {
        let mut affected_count = 0;
        for id in self.get_indices()? {
            let (row, metadata) = self.read_row(id)?;
            if !metadata.is_allocated && include(row.clone(), metadata) {
                match self.overwrite_row(id, transform(row)) {
                    Ok(ErrorValue(message)) => return Ok(ErrorValue(message)),
                    Ok(RowsAffected(n)) => affected_count += n,
                    Ok(..) => {}
                    Err(err) => return Ok(ErrorValue(err.to_string())),
                }
            }
        }
        Ok(RowsAffected(affected_count))
    }

    fn read_all_rows(&self) -> std::io::Result<Vec<Row>> {
        self.read_range(0..self.len()?)
    }

    /// reads a field by column position from an active row by ID
    fn read_field(&self, id: usize, column_id: usize) -> std::io::Result<TypedValue>;

    /// reads an active row by ID
    fn read_one(&self, id: usize) -> std::io::Result<Option<Row>> {
        let (row, metadata) = self.read_row(id)?;
        Ok(if metadata.is_allocated { Some(row) } else { None })
    }

    /// reads a span/range of rows
    fn read_range(&self, index: std::ops::Range<usize>) -> std::io::Result<Vec<Row>>;

    /// reads a row by ID
    fn read_row(&self, id: usize) -> std::io::Result<(Row, RowMetadata)>;

    /// reads the metadata of a specified row by ID
    fn read_row_metadata(&mut self, id: usize) -> std::io::Result<RowMetadata>;

    /// resizes (shrink or grow) the table
    fn resize(&mut self, new_size: usize) -> std::io::Result<TypedValue>;

    /// returns a reverse-order copy of the table
    fn reverse(&self) -> std::io::Result<Box<dyn RowCollection>> {
        use TypedValue::{ErrorValue, TableValue};
        let mrc = ModelRowCollection::new(self.get_columns().clone(), vec![]);
        let result = self.fold_right(TableValue(mrc), |tv, row| {
            match tv {
                ErrorValue(message) => ErrorValue(message),
                TableValue(mut rc) =>
                    match rc.append_row(row) {
                        Ok(ErrorValue(message)) => ErrorValue(message),
                        Ok(..) => TableValue(rc),
                        Err(err) => ErrorValue(err.to_string())
                    }
                z => ErrorValue(format!("Expected table value near {}", z.unwrap_value()))
            }
        });
        match result {
            TableValue(rc) => Ok(Box::new(rc)),
            ErrorValue(message) => fail(message),
            z => fail(format!("Expected table value near {}", z.unwrap_value()))
        }
    }

    fn to_row_offset(&self, id: usize) -> u64 { (id * self.get_record_size()) as u64 }

    /// Restores a deleted row by ID to an active state within the table
    fn undelete_row(&mut self, id: usize) -> std::io::Result<TypedValue> {
        self.overwrite_row_metadata(id, RowMetadata::new(true))
    }

    /// Restores deleted rows that satisfy the include function
    fn undelete_rows(&mut self, include: fn(Row, RowMetadata) -> bool) -> std::io::Result<TypedValue> {
        let mut restorations = 0;
        for id in self.get_indices()? {
            let (row, metadata) = self.read_row(id)?;
            if !metadata.is_allocated && include(row.clone(), metadata) {
                if self.overwrite_row_metadata(row.get_id(), metadata.as_undelete())?.is_ok() {
                    restorations += 1
                }
            }
        }
        Ok(RowsAffected(restorations))
    }

    /// modifies the specified row by ID
    fn update_row(&mut self, id: usize, row: Row) -> std::io::Result<TypedValue> {
        // retrieve the original record
        let (row0, rmd0) = self.read_row(id)?;
        // verify compatibility between the columns of the incoming row vs. table row
        let (cols0, cols1) = (row0.get_columns(), row.get_columns());
        match TableColumn::validate_compatibility(cols0, cols1) {
            ErrorValue(err) => Ok(ErrorValue(err)),
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
                self.overwrite_row(id, row)
            }
        }
    }

    /// Updates rows that satisfy the include function
    fn update_rows(
        &mut self,
        include: fn(Row, RowMetadata) -> bool,
        transform: fn(Row) -> Row,
    ) -> std::io::Result<TypedValue> {
        let mut modified = 0;
        for id in self.get_indices()? {
            let (row, metadata) = self.read_row(id)?;
            if !metadata.is_allocated && include(row.clone(), metadata) {
                if self.update_row(id, transform(row))?.is_ok() {
                    modified += 1
                }
            }
        }
        Ok(RowsAffected(modified))
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
    use crate::testdata::{make_dataframe, make_quote, make_quote_columns, make_table_columns, make_table_file};
    use crate::typed_values::TypedValue::{Float64Value, StringValue};

    use super::*;

    #[test]
    fn test_from_bytes() {
        // determine the record size of the row
        let columns = make_table_columns();
        let row = make_quote(0, &columns, "RICE", "NYSE", 78.78);
        let mut rc = <dyn RowCollection>::from_bytes(columns.clone(), vec![]);

        // create a new row
        assert_eq!(rc.overwrite_row(row.get_id(), row).unwrap(), TypedValue::RowsAffected(1));

        // read and verify the row
        let (row, rmd) = rc.read_row(0).unwrap();
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
        rc.overwrite_row(0, make_quote(0, &columns, "BEAM", "NYSE", 78.35)).unwrap();

        // read and verify the row
        let (row, rmd) = rc.read_row(0).unwrap();
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
            assert_eq!(rc.overwrite_row(row.get_id(), row.clone()).unwrap(), TypedValue::RowsAffected(1));

            // read and verify the row
            let (new_row, meta) = rc.read_row(row.get_id()).unwrap();
            assert!(meta.is_allocated);
            assert_eq!(new_row, row);
        }

        // test the variants
        verify_variants("write_then_read_row", make_table_columns(), test_variant);
    }

    #[test]
    fn test_write_then_read_row_metadata() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            // write a new row
            let row = make_quote(2, &columns, "BOX", "AMEX", 777.9311);
            assert_eq!(rc.overwrite_row(row.get_id(), row.clone()).unwrap(), TypedValue::RowsAffected(1));

            // read and verify the row metadata
            let meta = rc.read_row_metadata(row.get_id()).unwrap();
            assert!(meta.is_allocated);
        }

        // test the variants
        verify_variants("write_then_read_metadata", make_table_columns(), test_variant);
    }

    #[test]
    fn test_write_then_read_field() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            // write two rows
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "INTC", "NYSE", 66.77)).unwrap());
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(1, &columns, "AMD", "NASDAQ", 77.66)).unwrap());

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
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "GE", "NYSE", 21.22)).unwrap());
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(1, &columns, "ATT", "NYSE", 98.44)).unwrap());
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(2, &columns, "H", "OTC_BB", 0.0076)).unwrap());
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(3, &columns, "X", "NASDAQ", 33.33)).unwrap());
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(4, &columns, "OXIDE", "OSS", 0.00)).unwrap());

            // delete even rows
            assert_eq!(RowsAffected(1), rc.delete_row(0).unwrap());
            assert_eq!(RowsAffected(1), rc.delete_row(2).unwrap());
            assert_eq!(RowsAffected(1), rc.delete_row(4).unwrap());

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
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "GE", "NYSE", 21.22)).unwrap());
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(1, &columns, "ATT", "NYSE", 98.44)).unwrap());
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(2, &columns, "H", "OTC_BB", 0.0076)).unwrap());
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(3, &columns, "X", "NASDAQ", 33.33)).unwrap());
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(4, &columns, "OXIDE", "OSS", 0.00)).unwrap());

            // delete even rows
            assert_eq!(RowsAffected(3), rc.delete_rows(|row, meta| {
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
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "GE", "NYSE", 21.22)).unwrap());
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "ATT", "NYSE", 98.44)).unwrap());
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "H", "OTC_BB", 0.0076)).unwrap());
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "X", "NASDAQ", 33.33)).unwrap());
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "OXIDE", "OSS", 0.00)).unwrap());

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
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "GE", "NYSE", 21.22)).unwrap());
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "ATT", "NYSE", 98.44)).unwrap());
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "H", "OTC_BB", 0.0076)).unwrap());
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "X", "NASDAQ", 33.33)).unwrap());
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "OXIDE", "OSS", 0.00)).unwrap());

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
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "GE", "NYSE", 21.22)).unwrap());
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "ATT", "NYSE", 98.44)).unwrap());
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "H", "OTC_BB", 0.0076)).unwrap());
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "X", "NASDAQ", 33.33)).unwrap());
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "OXIDE", "OSS", 0.00)).unwrap());

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
            assert_eq!(RowsAffected(1),
                       rc.update_row(2, make_quote(2, &columns, "H.Q", "OTC_BB", 0.0001)).unwrap());

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
            assert_eq!(RowsAffected(4), rc.append_rows(vec![
                make_quote(0, &columns, "GE", "NYSE", 21.22),
                make_quote(0, &columns, "ATT", "NYSE", 98.44),
                make_quote(0, &columns, "H", "OTC_BB", 0.0076),
                make_quote(0, &columns, "GG", "NASDAQ", 33.33),
            ]).unwrap());

            // verify the initial state
            let rows = rc.read_all_rows().unwrap();
            assert_eq!(rows, vec![
                make_quote(0, &columns, "GE", "NYSE", 21.22),
                make_quote(1, &columns, "ATT", "NYSE", 98.44),
                make_quote(2, &columns, "H", "OTC_BB", 0.0076),
                make_quote(3, &columns, "GG", "NASDAQ", 33.33),
            ]);

            // delete a row
            assert_eq!(RowsAffected(1), rc.overwrite_row_metadata(2, RowMetadata::new(false)).unwrap());

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
            assert_eq!(RowsAffected(1), rc.overwrite_row(5, make_quote(0, &columns, "DUMMY", "OTC_BB", 0.0001)).unwrap());
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
            assert_eq!(RowsAffected(1), rc.overwrite_row(5, make_quote(0, &columns, "DUMMY", "OTC_BB", 0.0001)).unwrap());
            assert!(rc.len().unwrap() >= 6);

            // resize and verify
            let _ = rc.resize(50).unwrap();
            assert!(rc.len().unwrap() >= 50);
        }

        // test the variants
        verify_variants("resize_grow", make_table_columns(), test_variant);
    }

    #[test]
    fn test_reverse() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            let columns = make_quote_columns();
            let phys_columns = TableColumn::from_columns(&columns).unwrap();
            rc.append_row(make_quote(0, &phys_columns, "ABC", "AMEX", 12.33)).unwrap();
            rc.append_row(make_quote(1, &phys_columns, "UNO", "OTC", 0.2456)).unwrap();
            rc.append_row(make_quote(2, &phys_columns, "BIZ", "NYSE", 9.775)).unwrap();
            rc.append_row(make_quote(3, &phys_columns, "GOTO", "OTC", 0.1442)).unwrap();
            rc.append_row(make_quote(4, &phys_columns, "XYZ", "NYSE", 0.0289)).unwrap();

            // produce the reverse order
            let rrc = rc.reverse().unwrap();
            assert_eq!(rrc.read_all_rows().unwrap(), vec![
                make_quote(0, &phys_columns, "XYZ", "NYSE", 0.0289),
                make_quote(1, &phys_columns, "GOTO", "OTC", 0.1442),
                make_quote(2, &phys_columns, "BIZ", "NYSE", 9.775),
                make_quote(3, &phys_columns, "UNO", "OTC", 0.2456),
                make_quote(4, &phys_columns, "ABC", "AMEX", 12.33),
            ])
        }

        // test the variants
        verify_variants("reverse", make_table_columns(), test_variant);
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
            rc.overwrite_row(id, row)?;
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
            let (_row, rmd) = rc.read_row(id)?;
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
