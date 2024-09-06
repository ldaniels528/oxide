////////////////////////////////////////////////////////////////////
// row-collection module
////////////////////////////////////////////////////////////////////

use std::fmt::Debug;
use std::fs::File;
use std::ops::Range;
use std::sync::Arc;

use shared_lib::fail;

use crate::byte_row_collection::ByteRowCollection;
use crate::compiler::fail_value;
use crate::data_types::DataType::{BooleanType, UInt64Type};
use crate::field_metadata::FieldMetadata;
use crate::file_row_collection::FileRowCollection;
use crate::model_row_collection::ModelRowCollection;
use crate::row_metadata::RowMetadata;
use crate::rows::Row;
use crate::server::ColumnJs;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{Boolean, ErrorValue, RowsAffected, StringValue, StructureValue, TableValue, UInt64Value, Undefined};

/// represents the underlying storage resource for the dataframe
pub trait RowCollection: Debug {
    /// Appends the given row to the end of the table
    fn append_row(&mut self, row: Row) -> TypedValue {
        match self.len() {
            Ok(id) => {
                let _ = self.overwrite_row(id, row.with_row_id(id));
                RowsAffected(id)
            }
            Err(err) => ErrorValue(err.to_string())
        }
    }

    /// Appends the vector of rows to the end of the table
    fn append_rows(&mut self, rows: Vec<Row>) -> TypedValue {
        let mut affected_count = 0;
        for row in rows {
            let row_id = match self.len() {
                Ok(row_id) => row_id,
                Err(err) => return ErrorValue(err.to_string())
            };
            match self.overwrite_row(row_id, row.with_row_id(row_id)) {
                ErrorValue(message) => return ErrorValue(message),
                RowsAffected(_) => affected_count += 1,
                _ => {}
            }
        }
        RowsAffected(affected_count)
    }

    /// Appends the source table to the end of the host table
    fn append_table(
        &mut self,
        table: Box<dyn RowCollection>,
    ) -> TypedValue {
        let len = match self.len() {
            Ok(len) => len,
            Err(err) => return ErrorValue(err.to_string())
        };
        let mut affected = 0;
        for row in table.iter() {
            let row_id = row.get_id();
            let new_row_id = len + row_id;
            if let RowsAffected(n) = self.overwrite_row(new_row_id, row.with_row_id(new_row_id)) {
                affected += n
            }
        }
        RowsAffected(affected)
    }

    /// Eliminates all deleted rows; re-ordering the table in the process.
    fn compact(&mut self) -> TypedValue {
        let len = match self.len() {
            Ok(n) => n,
            Err(err) => return ErrorValue(err.to_string())
        };
        let (mut affected, mut row_id, mut eof) = (RowsAffected(0), 0, len);
        while row_id < eof {
            // read the row metadata
            let metadata = match self.read_row_metadata(row_id) {
                Ok(md) => md,
                Err(err) => return ErrorValue(err.to_string())
            };
            // if row is unallocated, replace it
            if !metadata.is_allocated {
                match self.find_previous(eof, |(_, md)| md.is_allocated) {
                    Ok(Some((row, _, id))) => {
                        eof = id;
                        let a = self.overwrite_row(row_id, row.with_row_id(row_id));
                        let b = self.delete_row(id);
                        affected = affected + (a + b)
                    }
                    Ok(None) => {}
                    Err(err) => return ErrorValue(err.to_string())
                }
            }
            row_id += 1;
        }
        affected
    }

    /// Returns true, if the given item matches a [Row] found within it
    fn contains(&self, item: &Row) -> TypedValue {
        Boolean(self.index_of(item) != Undefined)
    }

    /// Translates a rowID into a 64-bit offset
    fn convert_rowid_to_offset(&self, id: usize) -> u64 {
        (id * self.get_record_size()) as u64
    }

    fn create_related_structure(
        &self,
        columns: Vec<TableColumn>,
        _extension: &str,
    ) -> std::io::Result<Box<dyn RowCollection>> {
        Ok(Box::new(ModelRowCollection::with_rows(columns, Vec::new())))
    }

    /// deletes an existing row by ID from the table
    fn delete_row(&mut self, id: usize) -> TypedValue {
        self.overwrite_row_metadata(id, RowMetadata::new(false))
    }

    /// Removes rows that satisfy the include function
    fn delete_rows(&mut self, include: fn(&Row) -> bool) -> TypedValue {
        let mut removals = 0;
        let range = match self.get_indices() {
            Ok(result) => result,
            Err(err) => return ErrorValue(err.to_string())
        };
        let metadata = RowMetadata::new(false);
        for row_id in range {
            match self.read_one(row_id) {
                Ok(Some(row)) if include(&row) =>
                    match self.overwrite_row_metadata(row.get_id(), metadata.to_owned()) {
                        RowsAffected(n) => removals += n,
                        ErrorValue(msg) => return ErrorValue(msg),
                        _ => {}
                    }
                Ok(_) => {}
                Err(err) => return ErrorValue(err.to_string())
            };
        }
        RowsAffected(removals)
    }

    /// Returns a table that describes the structure of the host table
    fn describe(&self) -> TypedValue {
        let columns = vec![
            ColumnJs::new("name", "String(128)", None),
            ColumnJs::new("type", "String(128)", None),
            ColumnJs::new("default_value", "String(128)", None),
            ColumnJs::new("is_nullable", "Boolean", None),
        ];
        let physical_columns = match TableColumn::from_columns(&columns) {
            Ok(columns) => columns,
            Err(err) => return ErrorValue(err.to_string())
        };
        let mut mrc = ModelRowCollection::construct(&columns);
        for column in self.get_columns() {
            mrc.append_row(Row::new(0, physical_columns.to_owned(), vec![
                StringValue(column.get_name().to_string()),
                StringValue(column.data_type.to_column_type()),
                StringValue(column.default_value.unwrap_value()),
                Boolean(true),
            ]));
        }
        TableValue(mrc)
    }

    fn examine_range(&self, range: std::ops::Range<usize>) -> TypedValue {
        // create the augmented columns
        let mut columns = self.get_columns().to_owned();
        let record_size = self.get_record_size();
        columns.push(TableColumn::new("_id", UInt64Type, Undefined, record_size));
        columns.push(TableColumn::new("_active", BooleanType, Undefined, 8 + record_size));

        // gather the row data
        let mut row_data = Vec::new();
        for row_id in range {
            // read the row with its metadata
            let (row, meta) = match self.read_row(row_id) {
                Ok(result) => result,
                Err(err) => return ErrorValue(err.to_string())
            };
            // augment the values with the extras
            let mut values = row.get_values();
            values.push(UInt64Value(row_id as u64));
            values.push(Boolean(meta.is_allocated));
            // build a new row
            let row = Row::new(row_id, columns.to_owned(), values);
            let meta = meta.with_allocated(true);
            row_data.push((row, meta));
        }
        TableValue(ModelRowCollection::with_rows(columns, row_data))
    }

    /// Reads active and inactive (deleted) rows
    fn examine_rows(&self) -> std::io::Result<Vec<Row>> {
        match self.examine_range(self.get_indices()?) {
            ErrorValue(msg) => return fail(msg),
            TableValue(mrc) => Ok(mrc.get_rows()),
            z => fail_value("Table", &z)
        }
    }

    /// Returns true if at least one active row satisfies the provided function
    fn exists(&self, callback: fn(Row) -> bool) -> TypedValue {
        match self.len() {
            Ok(eof) => {
                for row_id in 0..eof {
                    match self.read_one(row_id) {
                        Ok(Some(row)) => if callback(row) { return Boolean(true); },
                        Ok(None) => {}
                        Err(err) => return ErrorValue(err.to_string())
                    }
                }
                Boolean(false)
            }
            Err(err) => ErrorValue(err.to_string())
        }
    }

    /// Returns an option of the first row that satisfies the given function
    fn find_first(
        &self,
        f: fn(&Row) -> bool,
    ) -> std::io::Result<Option<Row>> {
        self.find_next(0, f)
    }

    fn find_first_active_row(&self) -> std::io::Result<Option<Row>> {
        match self.find_first_active_row_id()? {
            Some(row_id) => self.read_one(row_id),
            None => Ok(None)
        }
    }

    fn find_first_active_row_id(&self) -> std::io::Result<Option<usize>> {
        let (mut row_id, mut is_active, eof) = (0, false, self.len()?);

        // find the first active row ID
        while !is_active && row_id < eof {
            let metadata = self.read_row_metadata(row_id)?;
            is_active = metadata.is_allocated;
            row_id += 1;
        }

        // did we find one?
        match is_active {
            true => Ok(Some(row_id)),
            false => Ok(None)
        }
    }

    fn find_last_active_row(&self) -> std::io::Result<Option<Row>> {
        match self.find_last_active_row_id()? {
            Some(row_id) => self.read_one(row_id),
            None => Ok(None)
        }
    }

    fn find_last_active_row_id(&self) -> std::io::Result<Option<usize>> {
        let (mut row_id, mut is_active) = (self.len()?, false);

        // find the last active row ID
        while !is_active && row_id > 0 {
            row_id -= 1;
            let metadata = self.read_row_metadata(row_id)?;
            is_active = metadata.is_allocated;
        }

        // did we find one?
        match is_active {
            true => Ok(Some(row_id)),
            false => Ok(None)
        }
    }

    /// Returns an option of the next row that satisfies the function
    /// starting with the initial_row_id.
    fn find_next(
        &self,
        initial_row_id: usize,
        f: fn(&Row) -> bool,
    ) -> std::io::Result<Option<Row>> {
        for row_id in initial_row_id..self.len()? {
            if let Some(row) = self.read_one(row_id)? {
                if f(&row) { return Ok(Some(row)); }
            }
        }
        Ok(None)
    }

    fn find_previous(
        &self,
        initial_row_id: usize,
        f: fn((&Row, &RowMetadata)) -> bool,
    ) -> std::io::Result<Option<(Row, RowMetadata, usize)>> {
        let (mut row_id, mut done) = (initial_row_id, false);
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

    /// Evaluates a callback function for each active row in the table
    fn fold_left(
        &self,
        initial: TypedValue,
        callback: fn(TypedValue, Row) -> TypedValue,
    ) -> TypedValue {
        match self.len() {
            Ok(eof) => {
                let mut result = initial;
                for row_id in 0..eof {
                    match self.read_one(row_id) {
                        Ok(Some(row)) => result = callback(result, row),
                        Ok(None) => {}
                        Err(err) => return ErrorValue(err.to_string())
                    }
                }
                result
            }
            Err(err) => return ErrorValue(err.to_string())
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
                    match self.read_one(row_id) {
                        Ok(Some(row)) => result = callback(result, row),
                        Ok(None) => {}
                        Err(err) => return ErrorValue(err.to_string())
                    }
                }
                result
            }
            Err(err) => return ErrorValue(err.to_string())
        }
    }

    /// returns true if all allocated rows satisfy the provided function
    fn for_all(&self, callback: fn(Row) -> bool) -> TypedValue {
        match self.len() {
            Ok(eof) => {
                for id in 0..eof {
                    match self.read_one(id) {
                        Ok(Some(row)) => if !callback(row) { return Boolean(false); },
                        Ok(None) => {}
                        Err(err) => return ErrorValue(err.to_string())
                    }
                }
                Boolean(true)
            }
            Err(err) => ErrorValue(err.to_string())
        }
    }

    /// Evaluates a callback function for each active row in the table
    fn for_each(&self, callback: fn(Row) -> ()) -> TypedValue {
        match self.len() {
            Ok(eof) => {
                for row_id in 0..eof {
                    match self.read_one(row_id) {
                        Ok(Some(row)) => callback(row),
                        Ok(None) => {}
                        Err(err) => return ErrorValue(err.to_string())
                    }
                }
                Undefined
            }
            Err(err) => ErrorValue(err.to_string())
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
    fn index_of(&self, item: &Row) -> TypedValue {
        match self.len() {
            Ok(len) => {
                for row_id in 0..len {
                    match self.read_one(row_id) {
                        Ok(Some(row)) => if row == *item { return UInt64Value(row_id as u64); },
                        Ok(None) => {}
                        Err(err) => return ErrorValue(err.to_string())
                    }
                }
                Undefined
            }
            Err(err) => ErrorValue(err.to_string())
        }
    }

    /// Returns an iterator of all active rows
    fn iter(&self) -> Box<dyn Iterator<Item=Row> + '_> {
        let mut row_id = 0;
        let iter = std::iter::from_fn(move || {
            match self.find_next(row_id, |_| true) {
                Ok(Some(row)) => {
                    row_id = row.get_id() + 1;
                    Some(row)
                }
                Ok(None) => None,
                Err(_) => None
            }
        });
        Box::new(iter)
    }

    /// returns the number of active rows in the table
    fn len(&self) -> std::io::Result<usize>;

    fn overwrite_field(&mut self, id: usize, column_id: usize, new_value: TypedValue) -> TypedValue;

    fn overwrite_field_metadata(&mut self, id: usize, column_id: usize, metadata: FieldMetadata) -> TypedValue;

    /// replaces the specified row by ID
    fn overwrite_row(&mut self, id: usize, row: Row) -> TypedValue;

    /// overwrites the metadata of a specified row by ID
    fn overwrite_row_metadata(&mut self, id: usize, metadata: RowMetadata) -> TypedValue;

    /// replaces rows that satisfy the include function
    fn overwrite_rows(
        &mut self,
        include: fn(&Row) -> bool,
        transform: fn(Row) -> Row,
    ) -> TypedValue {
        let mut affected_count = 0;
        match self.get_indices() {
            Ok(range) => {
                for row_id in range {
                    match self.read_one(row_id) {
                        Ok(Some(row)) => {
                            if include(&row) {
                                match self.overwrite_row(row_id, transform(row)) {
                                    ErrorValue(message) => return ErrorValue(message),
                                    RowsAffected(n) => affected_count += n,
                                    _ => {}
                                }
                            }
                        }
                        Ok(None) => {}
                        Err(err) => return ErrorValue(err.to_string()),
                    }
                }
                RowsAffected(affected_count)
            }
            Err(err) => return ErrorValue(err.to_string()),
        }
    }

    /// Retrieves the last row in the table; deleting the row from the table
    /// during the process.
    fn pop_row(&mut self) -> TypedValue {
        match self.find_last_active_row() {
            Ok(Some(row)) => {
                let _ = self.delete_row(row.get_id());
                StructureValue(row.to_struct())
            }
            Ok(None) => Undefined,
            Err(err) => ErrorValue(err.to_string())
        }
    }

    /// Appends the given row to the end of the table
    fn push_row(&mut self, row: Row) -> TypedValue {
        self.append_row(row)
    }

    /// reads all active rows from the table
    fn read_active_rows(&self) -> std::io::Result<Vec<Row>> {
        let mut rows = Vec::new();
        for row_id in self.get_indices()? {
            if let Ok(Some(row)) = self.read_one(row_id) { rows.push(row) }
        }
        Ok(rows)
    }

    /// reads a field by column position from an active row by ID
    fn read_field(&self, id: usize, column_id: usize) -> TypedValue;

    fn read_field_metadata(
        &self,
        id: usize,
        column_id: usize,
    ) -> std::io::Result<FieldMetadata>;

    /// reads an active row by ID
    fn read_one(&self, id: usize) -> std::io::Result<Option<Row>> {
        let (row, metadata) = self.read_row(id)?;
        Ok(if metadata.is_allocated { Some(row) } else { None })
    }

    /// reads a span/range of rows
    fn read_range(&self, index: std::ops::Range<usize>) -> std::io::Result<Vec<Row>> {
        let mut rows = Vec::with_capacity(index.len());
        for row_id in index {
            let (row, metadata) = self.read_row(row_id)?;
            if metadata.is_allocated {
                rows.push(row);
            }
        }
        Ok(rows)
    }

    /// reads a row by ID
    fn read_row(&self, id: usize) -> std::io::Result<(Row, RowMetadata)>;

    /// reads the metadata of a specified row by ID
    fn read_row_metadata(&self, id: usize) -> std::io::Result<RowMetadata>;

    /// resizes (shrink or grow) the table
    fn resize(&mut self, new_size: usize) -> TypedValue;

    /// returns a reverse-order copy of the table
    fn reverse(&self) -> std::io::Result<Box<dyn RowCollection>> {
        use TypedValue::{ErrorValue, TableValue};
        let mrc = ModelRowCollection::with_rows(self.get_columns().to_owned(), Vec::new());
        let result = self.fold_right(TableValue(mrc), |tv, row| {
            match tv {
                ErrorValue(message) => ErrorValue(message),
                TableValue(mut rc) =>
                    match rc.append_row(row) {
                        ErrorValue(message) => ErrorValue(message),
                        _ => TableValue(rc),
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

    /// Returns an option of the first row that matches the given search_column_value
    /// for a given search_column_index.
    fn scan_first(
        &self,
        search_column_index: usize,
        search_column_value: &TypedValue,
    ) -> std::io::Result<Option<Row>> {
        self.scan_next(search_column_index, search_column_value, 0)
    }

    /// Returns an option of the last row that matches the given search_column_value
    /// for a given search_column_index.
    fn scan_last(
        &self,
        search_column_index: usize,
        search_column_value: &TypedValue,
    ) -> std::io::Result<Option<Row>> {
        let eof = self.len()?;
        let (mut is_done, mut row_id) = (false, eof);
        while !is_done {
            if let Some(row) = self.read_one(row_id)? {
                let value = &row[search_column_index];
                if search_column_value == value {
                    return Ok(Some(row));
                }
            }
            is_done = row_id == 0;
            if !is_done { row_id -= 1; }
        }
        Ok(None)
    }

    /// Returns an option of the next row that matches the given search_column_value
    /// for a given search_column_index starting with the initial_row_id.
    fn scan_next(
        &self,
        search_column_index: usize,
        search_column_value: &TypedValue,
        initial_row_id: usize,
    ) -> std::io::Result<Option<Row>> {
        for row_id in initial_row_id..self.len()? {
            if let Some(row) = self.read_one(row_id)? {
                let value = &row[search_column_index];
                if search_column_value == value { return Ok(Some(row)); }
            }
        }
        Ok(None)
    }

    /// Returns an option of the previous row that matches the given search_column_value
    /// for a given search_column_index starting with the initial_row_id.
    fn scan_previous(
        &self,
        search_column_index: usize,
        search_column_value: &TypedValue,
        initial_row_id: usize,
    ) -> std::io::Result<Option<Row>> {
        for row_id in (initial_row_id..self.len()?).rev() {
            if let Some(row) = self.read_one(row_id)? {
                let value = &row[search_column_index];
                if search_column_value == value { return Ok(Some(row)); }
            }
        }
        Ok(None)
    }

    fn to_string(&self) -> String {
        match self.read_active_rows() {
            Ok(rows) => {
                let mut buf = String::new();
                for row in rows {
                    if !buf.is_empty() { buf += ", " }
                    buf += row.to_string().as_str();
                }
                format!("[{}]", buf)
            }
            Err(err) => return err.to_string()
        }
    }

    /// Restores a deleted row by ID to an active state within the table
    fn undelete_row(&mut self, id: usize) -> TypedValue {
        self.overwrite_row_metadata(id, RowMetadata::new(true))
    }

    /// Restores deleted rows that satisfy the include function
    fn undelete_rows(&mut self, include: fn(Row, RowMetadata) -> bool) -> TypedValue {
        let mut restorations = 0;
        let len = match self.len() {
            Ok(len) => len,
            Err(err) => return ErrorValue(err.to_string())
        };
        for id in 0..len {
            let (row, metadata) = match self.read_row(id) {
                Ok(result) => result,
                Err(err) => return ErrorValue(err.to_string())
            };
            if !metadata.is_allocated && include(row.to_owned(), metadata) {
                match self.overwrite_row_metadata(id, metadata.as_undelete()) {
                    RowsAffected(n) => restorations += n,
                    ErrorValue(msg) => return ErrorValue(msg),
                    _ => {}
                }
            }
        }
        RowsAffected(restorations)
    }

    /// modifies the specified row by ID
    fn update_row(&mut self, id: usize, row: Row) -> TypedValue {
        // retrieve the original record
        let (row0, rmd0) = match self.read_row(id) {
            Ok(result) => result,
            Err(err) => return ErrorValue(err.to_string())
        };
        // verify compatibility between the columns of the incoming row vs. table row
        let (cols0, cols1) = (row0.get_columns(), row.get_columns());
        match TableColumn::validate_compatibility(cols0, cols1) {
            ErrorValue(err) => return ErrorValue(err),
            _ => {
                // if it is deleted, then use the incoming row
                let row: Row = if !rmd0.is_allocated { row.with_row_id(id) } else {
                    // otherwise, construct a new composite row
                    Row::new(id, cols1.to_owned(), row0.get_values().iter().zip(row.get_values().iter())
                        .map(|(field0, field1)| {
                            match (field0.to_owned(), field1.to_owned()) {
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
    ) -> TypedValue {
        let mut modified = 0;
        let len = match self.len() {
            Ok(len) => len,
            Err(err) => return ErrorValue(err.to_string())
        };
        for id in 0..len {
            let (row, metadata) = match self.read_row(id) {
                Ok(result) => result,
                Err(err) => return ErrorValue(err.to_string())
            };
            if !metadata.is_allocated && include(row.to_owned(), metadata) {
                match self.update_row(id, transform(row)) {
                    ErrorValue(err) => return ErrorValue(err),
                    RowsAffected(n) => modified += n,
                    _ => {}
                }
            }
        }
        RowsAffected(modified)
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
}

// Unit tests
#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use rand::{Rng, RngCore, thread_rng};

    use shared_lib::cnv_error;

    use crate::hash_table_row_collection::HashTableRowCollection;
    use crate::model_row_collection::ModelRowCollection;
    use crate::namespaces::Namespace;
    use crate::row;
    use crate::table_renderer::TableRenderer;
    use crate::testdata::{make_quote, make_quote_columns, make_scan_quote, make_table_columns, make_table_file};
    use crate::typed_values::TypedValue::{Ack, Float64Value, Null, StringValue};

    use super::*;

    #[test]
    fn test_from_bytes() {
        // determine the record size of the row
        let columns = make_table_columns();
        let row = make_quote(0, &columns, "RICE", "NYSE", 78.78);
        let mut rc = <dyn RowCollection>::from_bytes(columns.to_owned(), Vec::new());

        // create a new row
        assert_eq!(rc.overwrite_row(row.get_id(), row), RowsAffected(1));

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
        let mut rc = <dyn RowCollection>::from_file(columns.to_owned(), file, path.as_str());
        rc.overwrite_row(0, make_quote(0, &columns, "BEAM", "NYSE", 78.35));

        // read and verify the row
        let (row, rmd) = rc.read_row(0).unwrap();
        assert!(rmd.is_allocated);
        assert_eq!(row, row!(0, make_table_columns(), vec![
            StringValue("BEAM".into()), StringValue("NYSE".into()), Float64Value(78.35)
        ]))
    }

    #[test]
    fn test_condition_exists_in_table() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            // append some rows to the host table (rc)
            assert_eq!(RowsAffected(4), rc.append_rows(vec![
                make_quote(0, &columns, "HOCK", "AMEX", 0.0076),
                make_quote(1, &columns, "XIE", "NASDAQ", 33.33),
                make_quote(2, &columns, "AAA", "NYSE", 22.44),
                make_quote(3, &columns, "XYZ", "NASDAQ", 66.67),
            ]));

            // verify: there is at least one row where exchange is "NYSE"
            assert_eq!(rc.exists(|row| matches!(
                row.get("exchange"),
                StringValue(s) if s == "NYSE"
            )), Boolean(true));

            // verify: there are no rows where exchange starts with "OTC"
            assert_eq!(rc.exists(|row| matches!(
                row.get("exchange"),
                StringValue(s) if s.starts_with("OTC")
            )), Boolean(false));
        }

        // test the variants
        verify_variants("condition_exists", make_table_columns(), test_variant);
    }

    #[test]
    fn test_write_then_read_row() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            // write a new row
            let row = make_quote(2, &columns, "AMD", "NYSE", 88.78);
            assert_eq!(rc.overwrite_row(row.get_id(), row.to_owned()), RowsAffected(1));

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
            assert_eq!(rc.overwrite_row(row.get_id(), row.to_owned()), RowsAffected(1));

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
            assert_eq!(RowsAffected(0), rc.append_row(make_quote(0, &columns, "INTC", "NYSE", 66.77)));
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(1, &columns, "AMD", "NASDAQ", 77.66)));

            // read the first column of the first row
            assert_eq!(rc.read_field(0, 0), StringValue("INTC".into()));

            // read the second column of the second row
            assert_eq!(rc.read_field(1, 1), StringValue("NASDAQ".into()));
        }

        // test the variants
        verify_variants("write_then_read_field", make_table_columns(), test_variant);
    }

    #[test]
    fn test_table_plus_table() {
        let columns = make_table_columns();

        // create a second table for which to append
        let mrc_a = ModelRowCollection::from_rows(vec![
            make_quote(0, &columns, "AAB", "NYSE", 22.44),
            make_quote(1, &columns, "WXYZ", "NASDAQ", 66.67),
            make_quote(2, &columns, "SSO", "NYSE", 123.44),
            make_quote(3, &columns, "RAND", "AMEX", 11.33),
        ]);

        // create a second table for which to append
        let mrc_b = ModelRowCollection::from_rows(vec![
            make_quote(0, &columns, "IBM", "NYSE", 21.22),
            make_quote(1, &columns, "ATT", "NYSE", 98.44),
            make_quote(2, &columns, "HOCK", "AMEX", 0.0076),
            make_quote(3, &columns, "XIE", "NASDAQ", 33.33),
        ]);

        // perform: table0 + table1
        let mrc_ab = TableValue(mrc_a) + TableValue(mrc_b);
        let rows = match mrc_ab {
            TableValue(mrc) => mrc.get_rows(),
            _ => Vec::new()
        };

        // verify the results
        assert_eq!(rows, vec![
            make_quote(0, &columns, "AAB", "NYSE", 22.44),
            make_quote(1, &columns, "WXYZ", "NASDAQ", 66.67),
            make_quote(2, &columns, "SSO", "NYSE", 123.44),
            make_quote(3, &columns, "RAND", "AMEX", 11.33),
            make_quote(4, &columns, "IBM", "NYSE", 21.22),
            make_quote(5, &columns, "ATT", "NYSE", 98.44),
            make_quote(6, &columns, "HOCK", "AMEX", 0.0076),
            make_quote(7, &columns, "XIE", "NASDAQ", 33.33),
        ]);
    }

    #[test]
    fn test_append_table() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            // append some rows to the host table (rc)
            assert_eq!(RowsAffected(4), rc.append_rows(vec![
                make_quote(0, &columns, "IBM", "NYSE", 21.22),
                make_quote(1, &columns, "ATT", "NYSE", 98.44),
                make_quote(2, &columns, "HOCK", "AMEX", 0.0076),
                make_quote(3, &columns, "XIE", "NASDAQ", 33.33),
            ]));

            // create a second table for which to append
            let mrc = ModelRowCollection::from_rows(vec![
                make_quote(0, &columns, "AAA", "NYSE", 22.44),
                make_quote(1, &columns, "XYZ", "NASDAQ", 66.67),
                make_quote(2, &columns, "SSO", "NYSE", 123.44),
                make_quote(3, &columns, "RAND", "AMEX", 11.33),
            ]);

            // append the second table to the host
            assert_eq!(RowsAffected(4), rc.append_table(Box::new(mrc)));

            // retrieve the entire range of rows
            assert_eq!(rc.read_active_rows().unwrap(), vec![
                make_quote(0, &columns, "IBM", "NYSE", 21.22),
                make_quote(1, &columns, "ATT", "NYSE", 98.44),
                make_quote(2, &columns, "HOCK", "AMEX", 0.0076),
                make_quote(3, &columns, "XIE", "NASDAQ", 33.33),
                make_quote(4, &columns, "AAA", "NYSE", 22.44),
                make_quote(5, &columns, "XYZ", "NASDAQ", 66.67),
                make_quote(6, &columns, "SSO", "NYSE", 123.44),
                make_quote(7, &columns, "RAND", "AMEX", 11.33),
            ]);
        }

        // test the variants
        verify_variants("append_table", make_table_columns(), test_variant);
    }

    #[test]
    fn test_delete_row() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            // write some rows
            assert_eq!(RowsAffected(0), rc.append_row(make_quote(0, &columns, "GE", "NYSE", 21.22)));
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(1, &columns, "ATT", "NYSE", 98.44)));
            assert_eq!(RowsAffected(2), rc.append_row(make_quote(2, &columns, "H", "OTC_BB", 0.0076)));
            assert_eq!(RowsAffected(3), rc.append_row(make_quote(3, &columns, "X", "NASDAQ", 33.33)));
            assert_eq!(RowsAffected(4), rc.append_row(make_quote(4, &columns, "OXIDE", "OSS", 0.00)));

            // delete even rows
            assert_eq!(RowsAffected(1), rc.delete_row(0));
            assert_eq!(RowsAffected(1), rc.delete_row(2));
            assert_eq!(RowsAffected(1), rc.delete_row(4));

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
            assert_eq!(RowsAffected(0), rc.append_row(make_quote(0, &columns, "GE", "NYSE", 21.22)));
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(1, &columns, "ATT", "NYSE", 98.44)));
            assert_eq!(RowsAffected(2), rc.append_row(make_quote(2, &columns, "H", "OTC_BB", 0.0076)));
            assert_eq!(RowsAffected(3), rc.append_row(make_quote(3, &columns, "X", "NASDAQ", 33.33)));
            assert_eq!(RowsAffected(4), rc.append_row(make_quote(4, &columns, "OXIDE", "OSS", 0.00)));

            // delete even rows
            assert_eq!(RowsAffected(3), rc.delete_rows(|row| row.get_id() % 2 == 0));

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
            assert_eq!(RowsAffected(0), rc.append_row(make_quote(0, &columns, "GE", "NYSE", 21.22)));

            // describe the table
            let mrc = rc.describe().to_table().unwrap();
            let mrc_columns = mrc.get_columns().to_owned();
            let mrc_rows = mrc.read_active_rows().unwrap();

            for s in TableRenderer::from_collection(rc) {
                println!("{}", s)
            }
            for s in TableRenderer::from_collection(mrc) {
                println!("{}", s)
            }

            assert_eq!(mrc_rows, vec![
                Row::new(0, mrc_columns.to_owned(), vec![
                    StringValue("symbol".to_string()),
                    StringValue("String(8)".to_string()),
                    StringValue("null".to_string()),
                    Boolean(true),
                ]),
                Row::new(1, mrc_columns.to_owned(), vec![
                    StringValue("exchange".to_string()),
                    StringValue("String(8)".to_string()),
                    StringValue("null".to_string()),
                    Boolean(true),
                ]),
                Row::new(2, mrc_columns.to_owned(), vec![
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
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            // write some rows
            assert_eq!(RowsAffected(0), rc.append_row(make_quote(0, &columns, "GE", "NYSE", 21.22)));
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "ATT", "NYSE", 98.44)));
            assert_eq!(RowsAffected(2), rc.append_row(make_quote(0, &columns, "H", "OTC_BB", 0.0076)));
            assert_eq!(RowsAffected(3), rc.append_row(make_quote(0, &columns, "X", "NASDAQ", 33.33)));
            assert_eq!(RowsAffected(4), rc.append_row(make_quote(0, &columns, "OXIDE", "OSS", 0.00)));

            // resize and verify
            assert_eq!(
                Some(make_quote(3, &columns, "X", "NASDAQ", 33.33)),
                rc.scan_first(0, &StringValue("X".into())).unwrap()
            );
        }

        // test the variants
        verify_variants("find", make_table_columns(), test_variant);
    }

    #[test]
    fn test_find_next() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            // write some rows
            assert_eq!(RowsAffected(0), rc.append_row(make_quote(0, &columns, "GE", "NYSE", 21.22)));
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "ATT", "NYSE", 98.44)));
            assert_eq!(RowsAffected(2), rc.append_row(make_quote(0, &columns, "H", "OTC_BB", 0.0076)));
            assert_eq!(RowsAffected(3), rc.append_row(make_quote(0, &columns, "X", "NASDAQ", 33.33)));
            assert_eq!(RowsAffected(4), rc.append_row(make_quote(0, &columns, "OXIDE", "OSS", 0.00)));

            // resize and verify
            assert_eq!(
                Some(make_quote(2, &columns, "H", "OTC_BB", 0.0076)),
                rc.find_next(0, |row| row[0] == StringValue("H".to_string())).unwrap()
            );
        }

        // test the variants
        verify_variants("find_next_too", make_table_columns(), test_variant);
    }

    #[test]
    fn test_fold_left() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            // write some rows
            assert_eq!(RowsAffected(0), rc.append_row(make_quote(0, &columns, "GE", "NYSE", 21.22)));
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "ATT", "NYSE", 98.44)));
            assert_eq!(RowsAffected(2), rc.append_row(make_quote(0, &columns, "H", "OTC_BB", 0.0076)));
            assert_eq!(RowsAffected(3), rc.append_row(make_quote(0, &columns, "X", "NASDAQ", 33.33)));
            assert_eq!(RowsAffected(4), rc.append_row(make_quote(0, &columns, "OXIDE", "OSS", 0.00)));

            // resize and verify
            assert_eq!(Float64Value(152.99759999999998),
                       rc.fold_left(Float64Value(0.), |agg, row| agg + row.get("last_sale")));
        }

        // test the variants
        verify_variants("fold_left", make_table_columns(), test_variant);
    }

    #[test]
    fn test_fold_right() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            // write some rows
            assert_eq!(RowsAffected(0), rc.append_row(make_quote(0, &columns, "GE", "NYSE", 21.22)));
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "ATT", "NYSE", 98.44)));
            assert_eq!(RowsAffected(2), rc.append_row(make_quote(0, &columns, "H", "OTC_BB", 0.0076)));
            assert_eq!(RowsAffected(3), rc.append_row(make_quote(0, &columns, "X", "NASDAQ", 33.33)));
            assert_eq!(RowsAffected(4), rc.append_row(make_quote(0, &columns, "OXIDE", "OSS", 0.00)));

            // fold and verify
            assert_eq!(Float64Value(347.00239999999997),
                       rc.fold_right(Float64Value(500.), |agg, row| agg - row.get("last_sale")));
        }

        // test the variants
        verify_variants("fold_right", make_table_columns(), test_variant);
    }

    #[test]
    fn test_iterator() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            // write some rows
            assert_eq!(RowsAffected(0), rc.append_row(make_quote(0, &columns, "A", "NYSE", 100.74)));
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "B", "NYSE", 50.19)));
            assert_eq!(RowsAffected(2), rc.append_row(make_quote(0, &columns, "C", "AMEX", 35.11)));
            assert_eq!(RowsAffected(3), rc.append_row(make_quote(0, &columns, "D", "NASDAQ", 16.45)));
            assert_eq!(RowsAffected(4), rc.append_row(make_quote(0, &columns, "E", "NYSE", 0.26)));

            // use an iterator
            let mut total = Float64Value(0.);
            for row in rc.iter() {
                total = total + row[2].to_owned();
            }

            // fold and verify
            assert_eq!(total, Float64Value(202.75));
        }

        // test the variants
        verify_variants("iterator", make_table_columns(), test_variant);
    }

    #[test]
    fn test_overwrite_field() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            // write some rows
            assert_eq!(RowsAffected(0), rc.append_row(make_quote(0, &columns, "GE", "NYSE", 21.22)));
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "H", "OTC_BB", 0.0076)));
            assert_eq!(RowsAffected(2), rc.append_row(make_quote(0, &columns, "X", "NASDAQ", 33.33)));
            assert_eq!(RowsAffected(3), rc.append_row(make_quote(0, &columns, "BAT", "AMEX", 1.66)));

            // overwrite the field at (0, 0)
            assert_eq!(RowsAffected(1), rc.overwrite_field(0, 1, StringValue("AMEX".to_string())));

            // verify the row
            assert_eq!(rc.read_one(0).unwrap(), Some(
                make_quote(0, &columns, "GE", "AMEX", 21.22)
            ));
        }

        // test the variants
        verify_variants("overwrite_field", make_table_columns(), test_variant);
    }

    #[test]
    fn test_overwrite_field_metadata() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            // write a row at 0
            assert_eq!(RowsAffected(1), rc.overwrite_row(0, make_quote(0, &columns, "GE", "NYSE", 21.22)));

            // overwrite the field metadata at (0, 1)
            assert_eq!(RowsAffected(1), rc.overwrite_field_metadata(0, 1, FieldMetadata::new(false)));

            // re-read the field metadata at (0, 1)
            assert_eq!(
                FieldMetadata::new(false),
                rc.read_field_metadata(0, 1).unwrap());

            // verify the row
            for s in TableRenderer::from_rows(rc.read_active_rows().unwrap()) {
                println!("{}", s)
            }
            assert_eq!(
                rc.read_one(0).unwrap(),
                Some(
                    Row::new(0, rc.get_columns().to_owned(), vec![
                        StringValue("GE".to_string()),
                        Null,
                        Float64Value(21.22),
                    ]),
                ));
        }

        // test the variants
        verify_variants("overwrite_field_metadata", make_table_columns(), test_variant);
    }

    #[test]
    fn test_update_row() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            // write some rows
            assert_eq!(RowsAffected(0), rc.append_row(make_quote(0, &columns, "GE", "NYSE", 21.22)));
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "ATT", "NYSE", 98.44)));
            assert_eq!(RowsAffected(2), rc.append_row(make_quote(0, &columns, "H", "OTC_BB", 0.0076)));
            assert_eq!(RowsAffected(3), rc.append_row(make_quote(0, &columns, "X", "NASDAQ", 33.33)));
            assert_eq!(RowsAffected(4), rc.append_row(make_quote(0, &columns, "OXIDE", "OSS", 0.00)));

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
            assert_eq!(
                RowsAffected(1),
                rc.update_row(2, make_quote(2, &columns, "H.Q", "OTC_BB", 0.0001)));

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
    fn test_write_delete_then_read_rows() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            // write some rows
            assert_eq!(RowsAffected(4), rc.append_rows(vec![
                make_quote(0, &columns, "GE", "NYSE", 21.22),
                make_quote(0, &columns, "ATT", "NYSE", 98.44),
                make_quote(0, &columns, "H", "OTC_BB", 0.0076),
                make_quote(0, &columns, "GG", "NASDAQ", 33.33),
            ]));

            // verify the initial state
            let rows = rc.read_active_rows().unwrap();
            assert_eq!(rows, vec![
                make_quote(0, &columns, "GE", "NYSE", 21.22),
                make_quote(1, &columns, "ATT", "NYSE", 98.44),
                make_quote(2, &columns, "H", "OTC_BB", 0.0076),
                make_quote(3, &columns, "GG", "NASDAQ", 33.33),
            ]);

            // delete a row
            assert_eq!(RowsAffected(1), rc.delete_row(2));

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
    fn test_push_pop() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            let columns = make_quote_columns();
            let phys_columns = TableColumn::from_columns(&columns).unwrap();
            assert_eq!(RowsAffected(0), rc.push_row(make_quote(0, &phys_columns, "BILL", "AMEX", 12.33)));
            assert_eq!(RowsAffected(1), rc.push_row(make_quote(1, &phys_columns, "TED", "NYSE", 56.2456)));
            assert_eq!(
                rc.pop_row(),
                StructureValue(make_quote(1, &phys_columns, "TED", "NYSE", 56.2456).to_struct()));
            assert_eq!(
                rc.pop_row(),
                StructureValue(make_quote(0, &phys_columns, "BILL", "AMEX", 12.33).to_struct()));
            assert_eq!(rc.pop_row(), Undefined);
        }

        // test the variants
        verify_variants("push_pop", make_table_columns(), test_variant);
    }

    #[test]
    fn test_read_range() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            let columns = make_quote_columns();
            let phys_columns = TableColumn::from_columns(&columns).unwrap();
            rc.append_row(make_quote(0, &phys_columns, "ABC", "AMEX", 12.33));
            rc.append_row(make_quote(1, &phys_columns, "TED", "OTC", 0.2456));
            rc.append_row(make_quote(2, &phys_columns, "BIZ", "NYSE", 9.775));
            rc.append_row(make_quote(3, &phys_columns, "GOTO", "OTC", 0.1442));
            rc.append_row(make_quote(4, &phys_columns, "XYZ", "NASDAQ", 0.0289));

            // produce the scan
            let rows = rc.read_range(1..4).unwrap();
            assert_eq!(rows, vec![
                make_quote(1, &phys_columns, "TED", "OTC", 0.2456),
                make_quote(2, &phys_columns, "BIZ", "NYSE", 9.775),
                make_quote(3, &phys_columns, "GOTO", "OTC", 0.1442),
            ]);
        }

        // test the variants
        verify_variants("read_range", make_table_columns(), test_variant);
    }

    #[test]
    fn test_resize_shrink() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            // insert some rows and verify the size
            assert_eq!(RowsAffected(1), rc.overwrite_row(5, make_quote(0, &columns, "DUMMY", "OTC_BB", 0.0001)));
            assert_eq!(6, rc.len().unwrap());

            // resize and verify
            assert_eq!(TypedValue::Ack, rc.resize(0));
            assert_eq!(rc.len().unwrap(), 0);
        }

        // test the variants
        verify_variants("resize_shrink", make_table_columns(), test_variant);
    }

    #[test]
    fn test_resize_grow() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            assert_eq!(Ack, rc.resize(0));

            // insert some rows and verify the size
            assert_eq!(RowsAffected(1), rc.overwrite_row(5, make_quote(0, &columns, "DUMMY", "OTC_BB", 0.0001)));
            assert!(rc.len().unwrap() >= 6);

            // resize and verify
            let _ = rc.resize(50);
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
                make_quote(0, &columns, "ATT", "NYSE", 98.45),
                make_quote(0, &columns, "H", "OTC_BB", 0.0076),
                make_quote(0, &columns, "GG", "NASDAQ", 33.33),
            ]));

            // verify the initial state
            assert_eq!(rc.read_active_rows().unwrap(), vec![
                make_quote(0, &columns, "GE", "NYSE", 21.22),
                make_quote(1, &columns, "ATT", "NYSE", 98.45),
                make_quote(2, &columns, "H", "OTC_BB", 0.0076),
                make_quote(3, &columns, "GG", "NASDAQ", 33.33),
            ]);

            // delete a row
            assert_eq!(RowsAffected(1), rc.delete_row(1));
            assert_eq!(RowsAffected(1), rc.delete_row(3));

            // verify the current state
            let rows = rc.read_active_rows().unwrap();
            assert_eq!(rows, vec![
                make_quote(0, &columns, "GE", "NYSE", 21.22),
                make_quote(2, &columns, "H", "OTC_BB", 0.0076),
            ]);

            // compact the table
            assert_eq!(RowsAffected(2), rc.compact());

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
            rc.append_row(make_quote(0, &phys_columns, "ABC", "AMEX", 12.33));
            rc.append_row(make_quote(1, &phys_columns, "UNO", "OTC", 0.2456));
            rc.append_row(make_quote(2, &phys_columns, "BIZ", "NYSE", 9.775));
            rc.append_row(make_quote(3, &phys_columns, "GOTO", "OTC", 0.1442));
            rc.append_row(make_quote(4, &phys_columns, "XYZ", "NYSE", 0.0289));

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
    fn test_scan_all_rows() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            let columns = make_quote_columns();
            let phys_columns = TableColumn::from_columns(&columns).unwrap();
            assert_eq!(RowsAffected(5), rc.append_rows(vec![
                make_quote(0, &phys_columns, "ABC", "AMEX", 12.33),
                make_quote(1, &phys_columns, "TED", "OTC", 0.2456),
                make_quote(2, &phys_columns, "BIZ", "NYSE", 9.775),
                make_quote(3, &phys_columns, "GOTO", "OTC", 0.1442),
                make_quote(4, &phys_columns, "XYZ", "NASDAQ", 0.0289),
            ]));

            // delete some rows
            for id in [0, 2, 4] {
                assert_eq!(RowsAffected(1), rc.delete_row(id));
            }

            // produce the scan
            let rows = rc.examine_rows().unwrap();
            let scan_columns = rows[0].get_columns();
            for s in TableRenderer::from_rows(rows.to_owned()) { println!("{}", s); }

            // verify row states: active or inactive
            assert_eq!(rows, vec![
                make_scan_quote(0, scan_columns, "ABC", "AMEX", 12.33, false),
                make_scan_quote(1, scan_columns, "TED", "OTC", 0.2456, true),
                make_scan_quote(2, scan_columns, "BIZ", "NYSE", 9.775, false),
                make_scan_quote(3, scan_columns, "GOTO", "OTC", 0.1442, true),
                make_scan_quote(4, scan_columns, "XYZ", "NASDAQ", 0.0289, false),
            ])
        }

        // test the variants
        verify_variants("scan_all_rows", make_table_columns(), test_variant);
    }

    #[test]
    fn test_scan_forward() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            let columns = make_quote_columns();
            let phys_columns = TableColumn::from_columns(&columns).unwrap();
            assert_eq!(RowsAffected(5), rc.append_rows(vec![
                make_quote(0, &phys_columns, "TED", "OTC", 0.2456),
                make_quote(1, &phys_columns, "ABC", "AMEX", 12.33),
                make_quote(2, &phys_columns, "BIZ", "NYSE", 9.775),
                make_quote(3, &phys_columns, "GOTO", "OTC", 0.1442),
                make_quote(4, &phys_columns, "XYZ", "NASDAQ", 0.0289),
            ]));

            // delete some rows
            for id in [0, 2, 4] {
                assert_eq!(RowsAffected(1), rc.delete_row(id));
            }

            // perform a forward scan
            let result = rc.find_next(2, |row| {
                matches!(row.get("exchange"), StringValue(s) if s == "OTC".to_string())
            }).unwrap();

            // verify the result
            let (scan_columns, scan_row) = result
                .map(|row| (row.get_columns().to_owned(), row))
                .unwrap();
            assert_eq!(
                scan_row,
                make_quote(3, &scan_columns, "GOTO", "OTC", 0.1442)
            );
        }

        // test the variants
        verify_variants("scan_forward", make_table_columns(), test_variant);
    }

    #[test]
    fn test_scan_next() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            // write some rows
            assert_eq!(RowsAffected(0), rc.append_row(make_quote(0, &columns, "GE", "NYSE", 21.22)));
            assert_eq!(RowsAffected(1), rc.append_row(make_quote(0, &columns, "ATT", "NYSE", 98.44)));
            assert_eq!(RowsAffected(2), rc.append_row(make_quote(0, &columns, "H", "OTC_BB", 0.0076)));
            assert_eq!(RowsAffected(3), rc.append_row(make_quote(0, &columns, "X", "NASDAQ", 33.33)));
            assert_eq!(RowsAffected(4), rc.append_row(make_quote(0, &columns, "OXIDE", "OSS", 0.00)));

            // resize and verify
            assert_eq!(
                Some(make_quote(3, &columns, "X", "NASDAQ", 33.33)),
                rc.scan_next(0, &StringValue("X".into()), 0).unwrap()
            );
        }

        // test the variants
        verify_variants("find_next", make_table_columns(), test_variant);
    }

    #[test]
    fn test_scan_reverse() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<TableColumn>) {
            let columns = make_quote_columns();
            let phys_columns = TableColumn::from_columns(&columns).unwrap();
            assert_eq!(RowsAffected(5), rc.append_rows(vec![
                make_quote(0, &phys_columns, "TED", "OTC", 0.2456),
                make_quote(1, &phys_columns, "GOTO", "OTC", 0.1442),
                make_quote(2, &phys_columns, "BIZ", "NYSE", 9.775),
                make_quote(3, &phys_columns, "ABC", "AMEX", 12.33),
                make_quote(4, &phys_columns, "XYZ", "NASDAQ", 0.0289),
            ]));

            // delete some rows
            for id in [0, 2, 4] {
                assert_eq!(RowsAffected(1), rc.delete_row(id));
            }

            // perform a reverse scan
            let result = rc.find_previous(4, |(row, _)| {
                matches!(row.get("exchange"), StringValue(s) if s == "OTC".to_string())
            }).unwrap();

            // verify the result
            let (scan_columns, scan_row) = result
                .map(|(row, _, _)| (row.get_columns().to_owned(), row))
                .unwrap();
            assert_eq!(
                scan_row,
                make_quote(1, &scan_columns, "GOTO", "OTC", 0.1442)
            );
        }

        // test the variants
        verify_variants("scan_reverse", make_table_columns(), test_variant);
    }

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
            rc.overwrite_row(id, row);
        }
        let end_time = SystemTime::now().duration_since(UNIX_EPOCH)
            .map_err(|e| cnv_error!(e))?.as_millis();
        let elapsed_time = end_time - start_time;
        let elapsed_time_sec = elapsed_time as f64 / 1000.;
        let rpm = total as f64 / elapsed_time as f64;
        println!("{} wrote {} row(s) in {} msec ({:.2} seconds, {:.2} records/msec)",
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
        println!("{} read {} row(s) in {} msec ({:.2} seconds, {:.2} records/msec)",
                 label, total, elapsed_time, elapsed_time_sec, rpm);
        Ok(())
    }

    fn verify_variants(name: &str, columns: Vec<TableColumn>, test_variant: fn(&str, Box<dyn RowCollection>, Vec<TableColumn>) -> ()) {
        println!("file_variant:");
        verify_file_variant(name, columns.to_owned(), test_variant);
        println!("byte_array_variant:");
        verify_byte_array_variant(columns.to_owned(), test_variant);
        println!("model_variant:");
        verify_model_variant(columns.to_owned(), test_variant);
        println!("hash_table_variant:");
        verify_hash_table_variant(name, columns, test_variant);
    }

    fn verify_byte_array_variant(columns: Vec<TableColumn>, test_variant: fn(&str, Box<dyn RowCollection>, Vec<TableColumn>) -> ()) {
        let brc = ByteRowCollection::new(columns.to_owned(), Vec::new());
        test_variant("Bytes", Box::new(brc), columns);
    }

    fn verify_file_variant(name: &str, columns: Vec<TableColumn>, test_variant: fn(&str, Box<dyn RowCollection>, Vec<TableColumn>) -> ()) {
        let ns = Namespace::new("file_row_collection", name, "stocks");
        let frc = FileRowCollection::create_table(&ns, columns.to_owned()).unwrap();
        test_variant("Disk", Box::new(frc), columns.to_owned());
    }

    fn verify_hash_table_variant(name: &str, columns: Vec<TableColumn>, test_variant: fn(&str, Box<dyn RowCollection>, Vec<TableColumn>) -> ()) {
        let ns = Namespace::new("hashing_row_collection", name, "stocks");
        let frc = FileRowCollection::create_table(&ns, columns.to_owned()).unwrap();
        let hrc = HashTableRowCollection::new(0, Box::new(frc)).unwrap();
        test_variant("HashTable", Box::new(hrc), columns.to_owned());
    }

    fn verify_model_variant(columns: Vec<TableColumn>, test_variant: fn(&str, Box<dyn RowCollection>, Vec<TableColumn>) -> ()) {
        let mrc = ModelRowCollection::with_rows(columns.to_owned(), Vec::new());
        test_variant("Model", Box::new(mrc), columns);
    }
}
