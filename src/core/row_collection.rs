////////////////////////////////////////////////////////////////////
// row-collection module
////////////////////////////////////////////////////////////////////

use std::fmt::Debug;
use std::fs::File;
use std::ops::{AddAssign, Range};
use std::sync::Arc;

use shared_lib::fail;

use crate::byte_row_collection::ByteRowCollection;
use crate::compiler::fail_value;
use crate::data_types::DataType;
use crate::data_types::DataType::{BooleanType, UInt64Type};
use crate::file_row_collection::FileRowCollection;
use crate::hashindex::HashingRowCollection;
use crate::model_row_collection::ModelRowCollection;
use crate::namespaces::Namespace;
use crate::row_metadata::RowMetadata;
use crate::rows::Row;
use crate::server::ColumnJs;
use crate::table_columns::TableColumn;
use crate::table_renderer::TableRenderer;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{Boolean, ErrorValue, RowsAffected, StringValue, TableValue, UInt64Value, Undefined};

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

    fn compact(&mut self) -> std::io::Result<TypedValue> {
        let (mut affected, mut row_id, mut eof) = (RowsAffected(0), 0, self.len()?);
        while row_id < eof {
            // attempt to read the row metadata
            if let Ok(metadata) = self.read_row_metadata(row_id) {
                // if row is unallocated, replace it
                if !metadata.is_allocated {
                    if let Ok(Some((row, _, id))) = self
                        .scan_reverse(eof, |(_, md)| md.is_allocated) {
                        eof = id;
                        let a = self.overwrite_row(row_id, row.with_row_id(row_id))?;
                        let b = self.delete_row(id)?;
                        affected = affected + (a + b)
                    }
                }
            };
            row_id += 1;
        }
        Ok(affected)
    }

    /// Returns true, if the given item matches a [Row] found within it
    fn contains(&self, item: &Row) -> bool { self.index_of(item).is_some() }

    fn create_hash_table(
        &self,
        key_column_index: usize,
    ) -> std::io::Result<Box<dyn RowCollection>> {
        let src_column = &self.get_columns()[key_column_index];
        let hash_columns = <dyn RowCollection>::get_hash_table_columns(src_column)?;
        let mrc = ModelRowCollection::new(hash_columns, vec![]);
        Ok(Box::new(mrc))
    }

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

    /// Returns a table that describes the structure of the host table
    fn describe(&self) -> std::io::Result<TypedValue> {
        let columns = vec![
            ColumnJs::new("name", "String(128)", None),
            ColumnJs::new("type", "String(128)", None),
            ColumnJs::new("default_value", "String(128)", None),
            ColumnJs::new("is_nullable", "Boolean", None),
        ];
        let physical_columns = TableColumn::from_columns(&columns)?;
        let mut mrc = ModelRowCollection::construct(&columns);
        for column in self.get_columns() {
            mrc.append_row(Row::new(0, physical_columns.clone(), vec![
                StringValue(column.get_name().to_string()),
                StringValue(column.data_type.to_column_type()),
                StringValue(column.default_value.unwrap_value()),
                Boolean(true),
            ]))?;
        }
        Ok(TableValue(mrc))
    }

    fn find_row(
        &self,
        search_column_index: usize,
        search_column_value: &TypedValue,
    ) -> std::io::Result<Option<Row>> {
        self.find_next(search_column_index, search_column_value, 0)
    }

    fn find_next(
        &self,
        search_column_index: usize,
        search_column_value: &TypedValue,
        initial_offset: usize,
    ) -> std::io::Result<Option<Row>> {
        let (mut row_id, eof) = (initial_offset, self.len()?);
        while row_id < eof {
            if let Some(row) = self.read_one(row_id)? {
                let value = row[search_column_index].clone();
                if *search_column_value == value {
                    return Ok(Some(row));
                }
            }
            row_id += 1;
        }
        Ok(None)
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
                        if metadata.is_allocated { result = callback(result, row) }
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
    fn index_of(&self, item: &Row) -> Option<usize> {
        for id in 0..self.len().unwrap_or(0) {
            if let Ok((row, metadata)) = self.read_row(id) {
                if metadata.is_allocated && &row == item { return Some(id); }
            }
        }
        None
    }

    /// returns the number of active rows in the table
    fn len(&self) -> std::io::Result<usize>;

    fn overwrite_field(&mut self, id: usize, column_id: usize, new_value: TypedValue) -> std::io::Result<TypedValue>;

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

    /// reads all active rows from the table
    fn read_active_rows(&self) -> std::io::Result<Vec<Row>> {
        let (mut row_id, eof, mut rows) = (0, self.len()?, vec![]);
        while row_id < eof {
            if let Ok(Some(row)) = self.read_one(row_id) { rows.push(row) }
            row_id += 1;
        }
        Ok(rows)
    }

    /// reads a field by column position from an active row by ID
    fn read_field(&self, id: usize, column_id: usize) -> std::io::Result<TypedValue>;

    /// reads an active row by ID
    fn read_one(&self, id: usize) -> std::io::Result<Option<Row>> {
        let (row, metadata) = self.read_row(id)?;
        Ok(if metadata.is_allocated { Some(row) } else { None })
    }

    /// reads a span/range of rows
    fn read_range(&self, index: std::ops::Range<usize>) -> std::io::Result<Box<dyn RowCollection>> {
        let mut rows = Vec::with_capacity(index.len());
        for id in index {
            let (row, metadata) = self.read_row(id)?;
            if metadata.is_allocated {
                rows.push((metadata, row));
            }
        }
        Ok(Box::new(ModelRowCollection::new(self.get_columns().clone(), rows)))
    }

    /// reads a row by ID
    fn read_row(&self, id: usize) -> std::io::Result<(Row, RowMetadata)>;

    /// reads the metadata of a specified row by ID
    fn read_row_metadata(&self, id: usize) -> std::io::Result<RowMetadata>;

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

    fn scan_all_rows(&self) -> std::io::Result<Vec<Row>> {
        match self.scan_range(self.get_indices()?) {
            Ok(TableValue(mrc)) => Ok(mrc.get_rows()),
            Ok(z) => fail_value("Table", &z),
            Err(err) => Err(err)
        }
    }

    fn scan_forward(
        &self,
        start_index: usize,
        f: fn((&Row, &RowMetadata)) -> bool,
    ) -> std::io::Result<Option<(Row, RowMetadata, usize)>> {
        let (mut row_id, eof) = (start_index, self.len()?);
        while row_id < eof {
            // attempt to read the row with its metadata
            if let Ok((row, metadata)) = self.read_row(row_id) {
                // if it's a match, return it
                if metadata.is_allocated && f((&row, &metadata)) {
                    return Ok(Some((row, metadata, row_id)));
                }
            }
            row_id += 1;
        }
        Ok(None)
    }

    fn scan_range(&self, range: std::ops::Range<usize>) -> std::io::Result<TypedValue> {
        // create the augmented columns
        let mut columns = self.get_columns().clone();
        let record_size = self.get_record_size();
        columns.push(TableColumn::new("_id", UInt64Type, Undefined, record_size));
        columns.push(TableColumn::new("_active", BooleanType, Undefined, 8 + record_size));

        // gather the row data
        let mut row_data = vec![];
        let (mut row_id, eof) = (range.start, range.end);
        while row_id < eof {
            // read the row with its metadata
            let (row, meta) = self.read_row(row_id)?;
            // augment the values with the extras
            let mut values = row.get_values();
            values.push(UInt64Value(row_id as u64));
            values.push(Boolean(meta.is_allocated));
            // build a new row
            let row = Row::new(row_id, columns.clone(), values);
            let meta = meta.with_allocated(true);
            row_data.push((meta, row));
            row_id += 1
        }
        Ok(TableValue(ModelRowCollection::new(columns, row_data)))
    }

    fn scan_reverse(
        &self,
        start_index: usize,
        f: fn((&Row, &RowMetadata)) -> bool,
    ) -> std::io::Result<Option<(Row, RowMetadata, usize)>> {
        let (mut row_id, mut done) = (start_index, false);
        while !done {
            // attempt to read the row with its metadata
            if let Ok((row, metadata)) = self.read_row(row_id) {
                // if it's a match, return it
                if metadata.is_allocated && f((&row, &metadata)) {
                    return Ok(Some((row, metadata, row_id)));
                }
            }
            if row_id > 0 { row_id -= 1; } else { done = true }
        }
        Ok(None)
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
                    Row::new(id, cols1.clone(), row0.get_values().iter().zip(row.get_values().iter())
                        .map(|(field0, field1)| {
                            match (field0.clone(), field1.clone()) {
                                (a, TypedValue::Undefined) => a,
                                (_, b) => b
                            }
                        }).collect::<Vec<TypedValue>>())
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
    pub fn from_file(
        columns: Vec<TableColumn>,
        file: File,
        file_path: &str,
    ) -> impl RowCollection {
        FileRowCollection::new(columns, Arc::new(file), file_path)
    }

    /// Generates the columns for the index base on the source column
    pub(crate) fn get_hash_table_columns(src_column: &TableColumn) -> std::io::Result<Vec<TableColumn>> {
        TableColumn::from_columns(&vec![
            ColumnJs::new("__row_id__", DataType::UInt64Type.to_column_type(), Some("null".into())),
            ColumnJs::new(src_column.get_name(), src_column.data_type.to_column_type(), Some(src_column.default_value.unwrap_value())),
        ])
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use log::info;
    use rand::{Rng, RngCore, thread_rng};

    use shared_lib::cnv_error;

    use crate::hashindex::HashingRowCollection;
    use crate::model_row_collection::ModelRowCollection;
    use crate::namespaces::Namespace;
    use crate::row;
    use crate::table_renderer::TableRenderer;
    use crate::testdata::{make_dataframe, make_quote, make_quote_columns, make_scan_quote, make_table_columns, make_table_file};
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
        let (path, file, columns, _) =
            make_table_file("rows", "append_row", "stocks", make_quote_columns());
        let mut rc = <dyn RowCollection>::from_file(columns.clone(), file, path.as_str());
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
            let rows = rc.read_active_rows().unwrap();
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
            let rows = rc.read_active_rows().unwrap();
            assert_eq!(rows, vec![
                make_quote(1, &columns, "ATT", "NYSE", 98.44),
                make_quote(3, &columns, "X", "NASDAQ", 33.33),
            ]);
        }

        // test the variants
        verify_variants("delete_rows", make_table_columns(), test_variant);
    }

    #[test]
    fn test_describe_table() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            // write a row
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "GE", "NYSE", 21.22)).unwrap());

            // describe the table
            let mrc = rc.describe().unwrap().to_table().unwrap();
            let mrc_columns = mrc.get_columns().clone();
            let mrc_rows = mrc.read_active_rows().unwrap();

            for s in TableRenderer::from_collection(rc) {
                println!("{}", s)
            }
            for s in TableRenderer::from_collection(mrc) {
                println!("{}", s)
            }

            assert_eq!(mrc_rows, vec![
                Row::new(0, mrc_columns.clone(), vec![
                    StringValue("symbol".to_string()),
                    StringValue("String(8)".to_string()),
                    StringValue("null".to_string()),
                    Boolean(true),
                ]),
                Row::new(1, mrc_columns.clone(), vec![
                    StringValue("exchange".to_string()),
                    StringValue("String(8)".to_string()),
                    StringValue("null".to_string()),
                    Boolean(true),
                ]),
                Row::new(2, mrc_columns.clone(), vec![
                    StringValue("last_sale".to_string()),
                    StringValue("f64".to_string()),
                    StringValue("null".to_string()),
                    Boolean(true),
                ]),
            ]);
        }

        // test the variants
        verify_variants("describe", make_table_columns(), test_variant);
    }

    #[test]
    fn test_find() {
        use TypedValue::*;

        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            // write some rows
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "GE", "NYSE", 21.22)).unwrap());
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "ATT", "NYSE", 98.44)).unwrap());
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "H", "OTC_BB", 0.0076)).unwrap());
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "X", "NASDAQ", 33.33)).unwrap());
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "OXIDE", "OSS", 0.00)).unwrap());

            // resize and verify
            assert_eq!(
                Some(make_quote(3, &columns, "X", "NASDAQ", 33.33)),
                rc.find_row(0, &StringValue("X".into())).unwrap()
            );
        }

        // test the variants
        verify_variants("find", make_table_columns(), test_variant);
    }

    #[test]
    fn test_find_next() {
        use TypedValue::*;

        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            // write some rows
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "GE", "NYSE", 21.22)).unwrap());
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "ATT", "NYSE", 98.44)).unwrap());
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "H", "OTC_BB", 0.0076)).unwrap());
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "X", "NASDAQ", 33.33)).unwrap());
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "OXIDE", "OSS", 0.00)).unwrap());

            // resize and verify
            assert_eq!(
                Some(make_quote(3, &columns, "X", "NASDAQ", 33.33)),
                rc.find_next(0, &StringValue("X".into()), 0).unwrap()
            );
        }

        // test the variants
        verify_variants("find_next", make_table_columns(), test_variant);
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
                       rc.fold_left(Float64Value(0.), |agg, row| agg + row.get_value_by_name("last_sale")));
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

            // fold and verify
            assert_eq!(Float64Value(347.00239999999997),
                       rc.fold_right(Float64Value(500.), |agg, row| agg - row.get_value_by_name("last_sale")));
        }

        // test the variants
        verify_variants("fold_right", make_table_columns(), test_variant);
    }

    #[test]
    fn test_overwrite_field() {
        use TypedValue::*;

        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            // write some rows
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "GE", "NYSE", 21.22)).unwrap());
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "H", "OTC_BB", 0.0076)).unwrap());
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "X", "NASDAQ", 33.33)).unwrap());
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "BAT", "AMEX", 1.66)).unwrap());

            // overwrite the field at (0, 0)
            assert_eq!(RowsAffected(1), rc.overwrite_field(0, 1, StringValue("AMEX".to_string())).unwrap());

            // verify the row
            assert_eq!(rc.read_one(0).unwrap(), Some(
                make_quote(0, &columns, "GE", "AMEX", 21.22)
            ));
        }

        // test the variants
        verify_variants("overwrite_field", make_table_columns(), test_variant);
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
            let rows = rc.read_active_rows().unwrap();
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
            let rows = rc.read_active_rows().unwrap();
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
            let rows = rc.read_active_rows().unwrap();
            assert_eq!(rows, vec![
                make_quote(0, &columns, "GE", "NYSE", 21.22),
                make_quote(1, &columns, "ATT", "NYSE", 98.44),
                make_quote(2, &columns, "H", "OTC_BB", 0.0076),
                make_quote(3, &columns, "GG", "NASDAQ", 33.33),
            ]);

            // delete a row
            assert_eq!(RowsAffected(1), rc.delete_row(2).unwrap());

            // verify the current state
            let rows = rc.read_active_rows().unwrap();
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
            assert_eq!(TypedValue::Ack, rc.resize(0).unwrap());
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
    fn test_resize_compact() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            // write some rows
            assert_eq!(RowsAffected(4), rc.append_rows(vec![
                make_quote(0, &columns, "GE", "NYSE", 21.22),
                make_quote(0, &columns, "ATT", "NYSE", 98.44),
                make_quote(0, &columns, "H", "OTC_BB", 0.0076),
                make_quote(0, &columns, "GG", "NASDAQ", 33.33),
            ]).unwrap());

            // verify the initial state
            assert_eq!(rc.read_active_rows().unwrap(), vec![
                make_quote(0, &columns, "GE", "NYSE", 21.22),
                make_quote(1, &columns, "ATT", "NYSE", 98.44),
                make_quote(2, &columns, "H", "OTC_BB", 0.0076),
                make_quote(3, &columns, "GG", "NASDAQ", 33.33),
            ]);

            // delete a row
            assert_eq!(RowsAffected(1), rc.delete_row(1).unwrap());
            assert_eq!(RowsAffected(1), rc.delete_row(3).unwrap());

            // verify the current state
            let rows = rc.read_active_rows().unwrap();
            assert_eq!(rows, vec![
                make_quote(0, &columns, "GE", "NYSE", 21.22),
                make_quote(2, &columns, "H", "OTC_BB", 0.0076),
            ]);

            // compact the table
            assert_eq!(RowsAffected(2), rc.compact().unwrap());

            // verify the final state
            let rows = rc.read_active_rows().unwrap();
            assert_eq!(rows, vec![
                make_quote(0, &columns, "GE", "NYSE", 21.22),
                make_quote(1, &columns, "H", "OTC_BB", 0.0076),
            ]);
        }

        // test the variants
        verify_variants("compact", make_table_columns(), test_variant);
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
            let rows = rrc.read_active_rows().unwrap();
            assert_eq!(rows, vec![
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

    #[test]
    fn test_scan() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            let columns = make_quote_columns();
            let phys_columns = TableColumn::from_columns(&columns).unwrap();
            rc.append_row(make_quote(0, &phys_columns, "ABC", "AMEX", 12.33)).unwrap();
            rc.append_row(make_quote(1, &phys_columns, "TED", "OTC", 0.2456)).unwrap();
            rc.append_row(make_quote(2, &phys_columns, "BIZ", "NYSE", 9.775)).unwrap();
            rc.append_row(make_quote(3, &phys_columns, "GOTO", "OTC", 0.1442)).unwrap();
            rc.append_row(make_quote(4, &phys_columns, "XYZ", "NASDAQ", 0.0289)).unwrap();

            // delete some rows
            for id in [0, 2, 4] {
                assert_eq!(RowsAffected(1), rc.delete_row(id).unwrap());
            }

            // produce the scan
            let rows = rc.scan_all_rows().unwrap();
            let scan_columns = rows[0].get_columns();
            for s in TableRenderer::from_rows(rows.clone()) { println!("{}", s); }
            assert_eq!(rows, vec![
                make_scan_quote(0, scan_columns, "ABC", "AMEX", 12.33, false),
                make_scan_quote(1, scan_columns, "TED", "OTC", 0.2456, true),
                make_scan_quote(2, scan_columns, "BIZ", "NYSE", 9.775, false),
                make_scan_quote(3, scan_columns, "GOTO", "OTC", 0.1442, true),
                make_scan_quote(4, scan_columns, "XYZ", "NASDAQ", 0.0289, false),
            ])
        }

        // test the variants
        verify_variants("scan", make_table_columns(), test_variant);
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
        println!("file_variant:");
        verify_file_variant(name, columns.clone(), test_variant);
        println!("hashing_variant:");
        verify_hashing_variant(name, columns.clone(), test_variant);
        println!("byte_array_variant:");
        verify_byte_array_variant(columns.clone(), test_variant);
        println!("model_variant:");
        verify_model_variant(columns, test_variant);
    }

    fn verify_file_variant(name: &str, columns: Vec<TableColumn>, test_variant: fn(&str, Box<dyn RowCollection>, Vec<TableColumn>) -> ()) {
        let ns = Namespace::new("file_row_collection", name, "stocks");
        let frc = FileRowCollection::create_table(&ns, columns.clone()).unwrap();
        test_variant("Disk", Box::new(frc), columns.clone());
    }

    fn verify_hashing_variant(name: &str, columns: Vec<TableColumn>, test_variant: fn(&str, Box<dyn RowCollection>, Vec<TableColumn>) -> ()) {
        let ns = Namespace::new("hashing_row_collection", name, "stocks");
        let frc = FileRowCollection::create_table(&ns, columns.clone()).unwrap();
        let hrc = HashingRowCollection::create(0, 1000, Box::new(frc)).unwrap();
        test_variant("Hashing", Box::new(hrc), columns.clone());
    }

    fn verify_byte_array_variant(columns: Vec<TableColumn>, test_variant: fn(&str, Box<dyn RowCollection>, Vec<TableColumn>) -> ()) {
        let brc = ByteRowCollection::new(columns.clone(), vec![]);
        test_variant("Bytes", Box::new(brc), columns);
    }

    fn verify_model_variant(columns: Vec<TableColumn>, test_variant: fn(&str, Box<dyn RowCollection>, Vec<TableColumn>) -> ()) {
        let mrc = ModelRowCollection::new(columns.clone(), vec![]);
        test_variant("Model", Box::new(mrc), columns);
    }
}
