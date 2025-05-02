#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// RowCollection module
////////////////////////////////////////////////////////////////////

use crate::byte_row_collection::ByteRowCollection;
use crate::columns::Column;
use crate::data_types::DataType::*;
use crate::dataframe::Dataframe::Model;
use crate::errors::Errors::{InvalidNamespace, TypeMismatch};
use crate::errors::TypeMismatchErrors::TableExpected;
use crate::errors::{throw, Errors};
use crate::expression::Conditions;
use crate::field::FieldMetadata;
use crate::file_row_collection::FileRowCollection;
use crate::machine::Machine;
use crate::model_row_collection::ModelRowCollection;
use crate::number_kind::NumberKind::U64Kind;
use crate::numbers::Numbers;
use crate::numbers::Numbers::{RowId, RowsAffected, U64Value};
use crate::parameter::Parameter;
use crate::platform::PlatformOps;
use crate::row_metadata::RowMetadata;
use crate::sequences::Array;
use crate::structures::Row;
use crate::structures::Structures::{Firm, Hard};
use crate::table_scan::{TableScanPlan, TableScanTypes};
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::*;
use serde::{Deserialize, Serialize};
use shared_lib::fail;
use std::fmt::Debug;
use std::fs::File;
use std::ops::Range;
use std::sync::Arc;

/// represents the underlying storage resource for the dataframe
pub trait RowCollection: Debug {
    /// Appends the given row to the end of the table
    fn append_row(&mut self, row: Row) -> TypedValue {
        match self.len() {
            Ok(id) => {
                let _ = self.overwrite_row(id, row.with_row_id(id));
                Number(RowId(id as u64))
            }
            Err(err) => ErrorValue(Errors::Exact(err.to_string()))
        }
    }

    /// Appends the vector of rows to the end of the table
    fn append_rows(&mut self, rows: Vec<Row>) -> TypedValue {
        let mut affected_count = 0;
        for row in rows {
            let row_id = match self.len() {
                Ok(row_id) => row_id,
                Err(err) => return ErrorValue(Errors::Exact(err.to_string()))
            };
            match self.overwrite_row(row_id, row.with_row_id(row_id)) {
                ErrorValue(message) => return ErrorValue(message),
                Number(n) => affected_count += n.to_i64(),
                _ => {}
            }
        }
        Number(RowsAffected(affected_count))
    }

    /// Appends the source table to the end of the host table
    fn append_table(
        &mut self,
        table: Box<dyn RowCollection>,
    ) -> TypedValue {
        let len = match self.len() {
            Ok(len) => len,
            Err(err) => return ErrorValue(Errors::Exact(err.to_string()))
        };
        let mut affected: Numbers = RowsAffected(0);
        for row in table.iter() {
            let row_id = row.get_id();
            let new_row_id = len + row_id;
            if let Number(n) = self.overwrite_row(new_row_id, row.with_row_id(new_row_id)) {
                affected = affected + n
            }
        }
        Number(RowsAffected(affected.to_i64()))
    }

    /// Eliminates all deleted rows; re-ordering the table in the process.
    fn compact(&mut self) -> TypedValue {
        let len = match self.len() {
            Ok(n) => n,
            Err(err) => return ErrorValue(Errors::Exact(err.to_string()))
        };
        let (mut affected, mut row_id, mut eof) = (
            Number(RowsAffected(0)), 0, len
        );
        while row_id < eof {
            // read the row metadata
            let metadata = match self.read_row_metadata(row_id) {
                Ok(md) => md,
                Err(err) => return ErrorValue(Errors::Exact(err.to_string()))
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
                    Err(err) => return ErrorValue(Errors::Exact(err.to_string()))
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

    fn count(&self, f: fn(Row) -> bool) -> u64 {
        self.iter().fold(0, |n, row| if f(row) { n + 1 } else { n })
    }

    fn create_related_structure(
        &self,
        columns: Vec<Column>,
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
            Err(err) => return ErrorValue(Errors::Exact(err.to_string()))
        };
        let metadata = RowMetadata::new(false);
        for row_id in range {
            match self.read_one(row_id) {
                Ok(Some(row)) if include(&row) =>
                    match self.overwrite_row_metadata(row.get_id(), metadata.to_owned()) {
                        Number(oc) => removals += oc.to_i64(),
                        ErrorValue(msg) => return ErrorValue(msg),
                        _ => {}
                    }
                Ok(_) => {}
                Err(err) => return ErrorValue(Errors::Exact(err.to_string()))
            };
        }
        Number(RowsAffected(removals))
    }

    /// Returns a table that describes the structure of the host table
    fn describe(&self) -> TypedValue {
        let params = PlatformOps::get_tools_describe_parameters();
        let mut mrc = ModelRowCollection::from_parameters(&params);
        for column in self.get_columns() {
            mrc.append_row(Row::new(0, vec![
                StringValue(column.get_name().to_string()),
                StringValue(column.get_data_type().to_code()),
                StringValue(column.get_default_value().unwrap_value()),
                Boolean(true),
            ]));
        }
        TableValue(Model(mrc))
    }

    fn examine_range(&self, range: std::ops::Range<usize>) -> TypedValue {
        // create the augmented columns
        let mut columns = self.get_columns().to_owned();
        let record_size = self.get_record_size();
        columns.push(Column::new("_id", NumberType(U64Kind), Undefined, record_size));
        columns.push(Column::new("_active", BooleanType, Undefined, 8 + record_size));

        // gather the row data
        let mut row_data = Vec::new();
        for row_id in range {
            // read the row with its metadata
            let (row, meta) = match self.read_row(row_id) {
                Ok(result) => result,
                Err(err) => return ErrorValue(Errors::Exact(err.to_string()))
            };
            // augment the values with the extras
            let mut values = row.get_values();
            values.push(Number(U64Value(row_id as u64)));
            values.push(Boolean(meta.is_allocated));
            // build a new row
            let row = Row::new(row_id, values);
            let meta = meta.with_allocated(true);
            row_data.push((row, meta));
        }
        TableValue(Model(ModelRowCollection::with_rows(columns, row_data)))
    }

    /// Reads active and inactive (deleted) rows
    fn examine_rows(&self) -> std::io::Result<Vec<Row>> {
        match self.examine_range(self.get_indices()?) {
            ErrorValue(err) => throw(err),
            TableValue(rcv) => Ok(rcv.get_rows()),
            z => throw(TypeMismatch(TableExpected(String::new(), z.to_code())))
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
                        Err(err) => return ErrorValue(Errors::Exact(err.to_string()))
                    }
                }
                Boolean(false)
            }
            Err(err) => ErrorValue(Errors::Exact(err.to_string()))
        }
    }

    fn filter_rows(&self, condition: &Conditions) -> std::io::Result<Vec<Row>> {
        let machine = Machine::empty();
        let result = self.iter().filter(|row| {
            let machine = machine.with_row(self.get_columns(), &row);
            match machine.do_condition(condition) {
                Ok((_, Boolean(true))) => true,
                _ => false
            }
        }).collect::<Vec<_>>();
        Ok(result)
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
                        Err(err) => return ErrorValue(Errors::Exact(err.to_string()))
                    }
                }
                result
            }
            Err(err) => return ErrorValue(Errors::Exact(err.to_string()))
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
                        Err(err) => return ErrorValue(Errors::Exact(err.to_string()))
                    }
                }
                result
            }
            Err(err) => return ErrorValue(Errors::Exact(err.to_string()))
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
                        Err(err) => return ErrorValue(Errors::Exact(err.to_string()))
                    }
                }
                Boolean(true)
            }
            Err(err) => ErrorValue(Errors::Exact(err.to_string()))
        }
    }

    /// Evaluates a callback function for each active row in the table
    fn for_each(&self, callback: Box<dyn Fn(Row) -> ()>) -> TypedValue {
        match self.len() {
            Ok(eof) => {
                for row_id in 0..eof {
                    match self.read_one(row_id) {
                        Ok(Some(row)) => callback(row),
                        Ok(None) => {}
                        Err(err) => return ErrorValue(Errors::Exact(err.to_string()))
                    }
                }
                Undefined
            }
            Err(err) => ErrorValue(Errors::Exact(err.to_string()))
        }
    }

    fn for_left_where(
        &self,
        condition: &Conditions,
        initial_value: TypedValue,
        callback: fn(&TypedValue, Row) -> TypedValue,
    ) -> TypedValue {
        let mut result = initial_value;
        let machine = Machine::empty();
        let columns = self.get_columns();
        for row in self.iter() {
            let ms = machine.with_row(columns, &row);
            match ms.do_condition(condition) {
                Ok((_ms, Boolean(true) | Boolean(true))) => {
                    match callback(&result, row) {
                        ErrorValue(msg) => return ErrorValue(msg),
                        value => result = value
                    }
                }
                Ok(..) => {}
                Err(err) => return ErrorValue(Errors::Exact(err.to_string()))
            }
        }
        result
    }

    /// returns the columns that represent device
    fn get_columns(&self) -> &Vec<Column>;

    fn get_eof(&self) -> std::io::Result<usize> {
        let len = self.len()?;
        Ok(if len > 0 { len - 1 } else { len })
    }

    fn get_indices(&self) -> std::io::Result<Range<usize>> {
        self.get_indices_with_limit(Null)
    }

    fn get_indices_with_limit(&self, limit: TypedValue) -> std::io::Result<Range<usize>> {
        match limit {
            Number(n) => Ok(0..n.to_usize()),
            ErrorValue(err) => fail(err.to_string()),
            _ => Ok(0..self.len()?)
        }
    }

    fn get_parameters(&self) -> Vec<Parameter> {
        Parameter::from_columns(self.get_columns())
    }

    /// returns the record size of the device
    fn get_record_size(&self) -> usize;

    /// Returns all active rows
    fn get_rows(&self) -> Vec<Row>;

    /// Returns true, if the given item matches a [Row] found within it
    fn index_of(&self, item: &Row) -> TypedValue {
        let mut it = self.iter();
        while let Some(row) = it.next() {
            if row.get_values() == item.get_values() {
                return Number(U64Value(row.get_id() as u64));
            }
        }
        Undefined
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
                                    Number(oc) => affected_count += oc.to_i64(),
                                    _ => {}
                                }
                            }
                        }
                        Ok(None) => {}
                        Err(err) => return ErrorValue(Errors::Exact(err.to_string())),
                    }
                }
                Number(RowsAffected(affected_count))
            }
            Err(err) => ErrorValue(Errors::Exact(err.to_string())),
        }
    }

    /// Retrieves the last row in the table; deleting the row from the table
    /// during the process.
    fn pop_row(&mut self, parameters: Vec<Parameter>) -> TypedValue {
        match self.find_last_active_row() {
            Ok(Some(row)) => {
                let _ = self.delete_row(row.get_id());
                Structured(Firm(row, parameters))
            }
            Ok(None) => Undefined,
            Err(err) => ErrorValue(Errors::Exact(err.to_string()))
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

    fn read_column_slice(
        &self,
        column_id: usize,
    ) -> std::io::Result<Vec<TypedValue>> {
        self.read_column_slice_range(column_id, 0..self.len()?)
    }

    fn read_column_slice_range(
        &self,
        column_id: usize,
        index: std::ops::Range<usize>,
    ) -> std::io::Result<Vec<TypedValue>> {
        let mut values = vec![];
        let mut row_id = index.start;
        while row_id < index.end {
            values.push(self.read_field(row_id, column_id));
            row_id += 1
        }
        Ok(values)
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
        match self.reverse_table_value() {
            TableValue(rcv) => Ok(Box::new(rcv)),
            ErrorValue(err) => fail(err.to_string()),
            z => fail(format!("Expected table value near {}", z.unwrap_value()))
        }
    }

    /// returns a reverse-order copy of the table
    fn reverse_table_value(&self) -> TypedValue {
        use TypedValue::{ErrorValue, TableValue};
        let mrc = ModelRowCollection::with_rows(self.get_columns().to_owned(), Vec::new());
        self.fold_right(TableValue(Model(mrc)), |tv, row| {
            match tv {
                ErrorValue(message) => return ErrorValue(message),
                TableValue(mut rcv) =>
                    match rcv.append_row(row) {
                        ErrorValue(message) => return ErrorValue(message),
                        _ => TableValue(rcv),
                    }
                z => return ErrorValue(InvalidNamespace(z.unwrap_value()))
            }
        })
    }

    /// Returns an option of the first row that matches the given [TableScanPlan]
    fn scan_first(
        &self,
        plan: TableScanPlan,
    ) -> std::io::Result<Option<Row>> {
        match plan.get_scan_type() {
            TableScanTypes::ColumnScan { column_index, column_value } =>
                self.scan_next(TableScanPlan::column_scan(column_index, column_value)),
            TableScanTypes::ConditionScan { machine, condition } =>
                self.scan_next(TableScanPlan::condition_scan(machine, condition)),
            TableScanTypes::CompleteScan =>
                self.scan_next(TableScanPlan::complete_scan())
        }
    }

    /// Returns an option of the last row that matches the given [TableScanPlan]
    fn scan_last(
        &self,
        plan: TableScanPlan,
    ) -> std::io::Result<Option<Row>> {
        let eof = self.get_eof()?;
        match plan.get_scan_type() {
            TableScanTypes::ColumnScan { column_index, column_value } =>
                self.scan_next(TableScanPlan::new(eof, false, TableScanTypes::ColumnScan {
                    column_index,
                    column_value,
                })),
            TableScanTypes::ConditionScan { machine, condition } =>
                self.scan_next(TableScanPlan::new(eof, false, TableScanTypes::ConditionScan {
                    machine,
                    condition,
                })),
            TableScanTypes::CompleteScan =>
                self.scan_next(TableScanPlan::new(eof, false, TableScanTypes::CompleteScan))
        }
    }

    /// Returns an option of the next row that matches the given [TableScanPlan]
    fn scan_next(
        &self,
        plan: TableScanPlan,
    ) -> std::io::Result<Option<Row>> {
        let eof = self.get_eof()?;
        let mut row_id = plan.get_position();
        let is_forward = plan.is_forward();

        // determine the matching function for the search method
        let is_match: Box<dyn Fn(&Row) -> bool> = match plan.get_scan_type() {
            TableScanTypes::ColumnScan { column_index, column_value } =>
                Box::new(move |row: &Row| column_value == row[column_index]),
            TableScanTypes::ConditionScan { machine, condition } =>
                Box::new(move |row: &Row| matches!(
                        machine.with_row(self.get_columns(), row).do_condition(&condition),
                        Ok((_ms, Boolean(true)))
                    )),
            TableScanTypes::CompleteScan => Box::new(|_row: &Row| true)
        };

        // attempt to find the next match
        while (is_forward && row_id < eof) || (!is_forward && row_id as isize >= 0) {
            if let Some(row) = self.read_one(row_id)? {
                if is_match(&row) { return Ok(Some(row)); }
            }
            row_id = if is_forward { row_id + 1 } else { row_id.wrapping_sub(1) };
        }

        Ok(None)
    }

    /// Returns an option of the previous row that matches the given [TableScanPlan]
    fn scan_previous(
        &self,
        plan: TableScanPlan,
    ) -> std::io::Result<Option<Row>> {
        match plan.get_scan_type() {
            TableScanTypes::ColumnScan { column_index, column_value } =>
                self.scan_next(TableScanPlan::new(
                    plan.get_position(),
                    !plan.is_forward(),
                    TableScanTypes::ColumnScan { column_index, column_value },
                )),
            TableScanTypes::ConditionScan { machine, condition } =>
                self.scan_next(TableScanPlan::new(
                    plan.get_position(),
                    !plan.is_forward(),
                    TableScanTypes::ConditionScan { machine, condition },
                )),
            TableScanTypes::CompleteScan =>
                self.scan_next(TableScanPlan::new(
                    plan.get_position(),
                    !plan.is_forward(),
                    TableScanTypes::CompleteScan,
                )),
        }
    }

    fn to_array(&self) -> Array {
        let columns = self.get_parameters();
        Array::from(self.iter().map(|row| Structured(Hard(row.as_hard(&columns)))).collect())
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
            Err(err) => return ErrorValue(Errors::Exact(err.to_string()))
        };
        for id in 0..len {
            let (row, metadata) = match self.read_row(id) {
                Ok(result) => result,
                Err(err) => return ErrorValue(Errors::Exact(err.to_string()))
            };
            if !metadata.is_allocated && include(row.to_owned(), metadata) {
                match self.overwrite_row_metadata(id, metadata.as_undelete()) {
                    Number(oc) => restorations += oc.to_i64(),
                    ErrorValue(msg) => return ErrorValue(msg),
                    _ => {}
                }
            }
        }
        Number(RowsAffected(restorations))
    }

    /// modifies the specified row by ID
    fn update_row(&mut self, id: usize, row: Row) -> TypedValue {
        // retrieve the original record
        let (row0, rmd0) = match self.read_row(id) {
            Ok(result) => result,
            Err(err) => return ErrorValue(Errors::Exact(err.to_string()))
        };
        // verify compatibility between the columns of the incoming row vs. table row
        let columns = self.get_columns();
        let (cols0, cols1) = (columns, columns);
        match Column::validate_compatibility(cols0, cols1) {
            ErrorValue(err) => return ErrorValue(err),
            _ => {
                // if it is deleted, then use the incoming row
                let row: Row = if !rmd0.is_allocated { row.with_row_id(id) } else {
                    // otherwise, construct a new composite row
                    Row::new(id, row0.get_values().iter().zip(row.get_values().iter())
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
            Err(err) => return ErrorValue(Errors::Exact(err.to_string()))
        };
        for id in 0..len {
            let (row, metadata) = match self.read_row(id) {
                Ok(result) => result,
                Err(err) => return ErrorValue(Errors::Exact(err.to_string()))
            };
            if !metadata.is_allocated && include(row.to_owned(), metadata) {
                match self.update_row(id, transform(row)) {
                    ErrorValue(err) => return ErrorValue(err),
                    Number(oc) => modified += oc.to_i64(),
                    _ => {}
                }
            }
        }
        Number(RowsAffected(modified))
    }
}

impl dyn RowCollection {
    /// creates a new in-memory [RowCollection] from a byte vector.
    pub fn from_bytes(columns: Vec<Column>, rows: Vec<Vec<u8>>) -> impl RowCollection {
        ByteRowCollection::from_bytes(columns, rows)
    }

    /// creates a new [RowCollection] from a file.
    pub fn from_file(
        columns: Vec<Column>,
        file: File,
        file_path: &str,
    ) -> impl RowCollection {
        FileRowCollection::new(columns, Arc::new(file), file_path)
    }
}

/// Row Encoding interface
pub trait RowEncoding {
    /// Returns an empty binary field
    fn empty_cell(column: &Column) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.resize(column.get_fixed_size(), 0u8);
        bytes
    }

    fn read_at(&self, offset: u64, count: usize) -> std::io::Result<Vec<u8>>;

    fn write_at(&self, offset: u64, bytes: &Vec<u8>) -> std::io::Result<Numbers>;
}

/// Unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::byte_code_compiler::ByteCodeCompiler;
    use crate::dataframe::Dataframe;
    use crate::expression::Conditions::Equal;
    use crate::expression::Expression::{Literal, Variable};
    use crate::hash_table_row_collection::HashTableRowCollection;
    use crate::hybrid_row_collection::HybridRowCollection;
    use crate::journaling::EventSourceRowCollection;
    use crate::model_row_collection::ModelRowCollection;
    use crate::namespaces::Namespace;
    use crate::number_kind::NumberKind::F64Kind;
    use crate::numbers::Numbers::{F64Value, RowId, RowsAffected};
    use crate::parameter::Parameter;
    use crate::structures::Structures::Firm;
    use crate::table_renderer::TableRenderer;
    use crate::table_scan::TableScanTypes::ColumnScan;
    use crate::testdata::*;
    use chrono::Local;
    use num_traits::ToPrimitive;
    use rand::{thread_rng, Rng, RngCore};
    use shared_lib::{cnv_error, compute_time_millis};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn test_from_bytes() {
        // determine the record size of the row
        let columns = make_quote_columns();
        let row = make_quote(0, "RICE", "NYSE", 78.78);
        let mut rc = <dyn RowCollection>::from_bytes(columns.to_owned(), Vec::new());

        // create a new row
        assert_eq!(rc.overwrite_row(row.get_id(), row), Number(RowsAffected(1)));

        // read and verify the row
        let (row, rmd) = rc.read_row(0).unwrap();
        assert!(rmd.is_allocated);
        assert_eq!(row, Row::new(0, vec![
            StringValue("RICE".into()), StringValue("NYSE".into()), Number(F64Value(78.78)),
        ]))
    }

    #[test]
    fn test_from_file() {
        let (path, file, columns, _) =
            make_table_file("rows", "append_row", "stocks", make_quote_parameters());
        let mut rc = <dyn RowCollection>::from_file(columns.to_owned(), file, path.as_str());
        rc.overwrite_row(0, make_quote(0, "BEAM", "NYSE", 78.35));

        // read and verify the row
        let (row, rmd) = rc.read_row(0).unwrap();
        assert!(rmd.is_allocated);
        assert_eq!(row, Row::new(0, vec![
            StringValue("BEAM".into()), StringValue("NYSE".into()), Number(F64Value(78.35)),
        ]))
    }

    #[test]
    fn test_condition_exists_in_table() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<Column>) -> u64 {
            // append some rows to the host table (rc)
            assert_eq!(Number(RowsAffected(4)), rc.append_rows(vec![
                make_quote(0, "HOCK", "AMEX", 0.0076),
                make_quote(1, "XIE", "NASDAQ", 33.33),
                make_quote(2, "AAA", "NYSE", 22.44),
                make_quote(3, "XYZ", "NASDAQ", 66.67),
            ]));

            // verify: there is at least one row where exchange is "NYSE"
            assert_eq!(rc.exists(|row| matches!(
                row.get(1),
                StringValue(s) if s == "NYSE"
            )), Boolean(true));

            // verify: there are no rows where exchange starts with "OTC"
            assert_eq!(rc.exists(|row| matches!(
                row.get(1),
                StringValue(s) if s.starts_with("OTC")
            )), Boolean(false));

            rc.len().unwrap() as u64
        }

        // test the variants
        verify_variants("condition_exists", make_quote_columns(), test_variant);
    }

    #[test]
    fn test_table_encode_decode() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<Column>) -> u64 {
            // append some rows to the host table (rc)
            assert_eq!(Number(RowsAffected(4)), rc.append_rows(vec![
                make_quote(0, "IBM", "NYSE", 21.22),
                make_quote(1, "ATT", "NYSE", 98.44),
                make_quote(2, "HOCK", "AMEX", 0.0076),
                make_quote(3, "XIE", "NASDAQ", 33.33),
            ]));

            // encode the table
            let encoded_table = ByteCodeCompiler::encode_rc(&rc);
            assert_eq!(encoded_table.len(), 208);

            // reconstitute the table
            let new_rc = ModelRowCollection::decode(columns, encoded_table);
            let boxed_rc: Box<dyn RowCollection> = Box::from(new_rc);
            assert_eq!(TableRenderer::from_table_with_ids(&boxed_rc).unwrap(), vec![
                "|------------------------------------|",
                "| id | symbol | exchange | last_sale |",
                "|------------------------------------|",
                "| 0  | IBM    | NYSE     | 21.22     |",
                "| 1  | ATT    | NYSE     | 98.44     |",
                "| 2  | HOCK   | AMEX     | 0.0076    |",
                "| 3  | XIE    | NASDAQ   | 33.33     |",
                "|------------------------------------|"]);

            rc.len().unwrap() as u64
        }

        // test the variants
        verify_variants("table_encode_decode", make_quote_columns(), test_variant);
    }

    #[test]
    fn test_write_then_read_row() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<Column>) -> u64 {
            // write a new row
            let row = make_quote(2, "AMD", "NYSE", 88.78);
            assert_eq!(rc.overwrite_row(row.get_id(), row.to_owned()), Number(RowsAffected(1)));

            // read and verify the row
            let (new_row, meta) = rc.read_row(row.get_id()).unwrap();
            assert!(meta.is_allocated);
            assert_eq!(new_row, row);

            rc.len().unwrap() as u64
        }

        // test the variants
        verify_variants("write_then_read_row", make_quote_columns(), test_variant);
    }

    #[test]
    fn test_write_then_read_row_metadata() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<Column>) -> u64 {
            // write a new row
            let row = make_quote(2, "BOX", "AMEX", 777.9311);
            assert_eq!(rc.overwrite_row(row.get_id(), row.to_owned()), Number(RowsAffected(1)));

            // read and verify the row metadata
            let meta = rc.read_row_metadata(row.get_id()).unwrap();
            assert!(meta.is_allocated);

            rc.len().unwrap() as u64
        }

        // test the variants
        verify_variants("write_then_read_metadata", make_quote_columns(), test_variant);
    }

    #[test]
    fn test_write_then_read_field() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<Column>) -> u64 {
            // write two rows
            assert_eq!(Number(RowId(0)), rc.append_row(make_quote(0, "INTC", "NYSE", 66.77)));
            assert_eq!(Number(RowId(1)), rc.append_row(make_quote(1, "AMD", "NASDAQ", 77.66)));

            // read the first column of the first row
            assert_eq!(rc.read_field(0, 0), StringValue("INTC".into()));

            // read the second column of the second row
            assert_eq!(rc.read_field(1, 1), StringValue("NASDAQ".into()));

            rc.len().unwrap() as u64
        }

        // test the variants
        verify_variants("write_then_read_field", make_quote_columns(), test_variant);
    }

    #[test]
    fn test_table_plus_table() {
        let columns = make_quote_columns();

        // create a second table for which to append
        let mrc_a = ModelRowCollection::from_columns_and_rows(&columns, &vec![
            make_quote(0, "AAB", "NYSE", 22.44),
            make_quote(1, "WXYZ", "NASDAQ", 66.67),
            make_quote(2, "SSO", "NYSE", 123.44),
            make_quote(3, "RAND", "AMEX", 11.33),
        ]);

        // create a second table for which to append
        let mrc_b = ModelRowCollection::from_columns_and_rows(&columns, &vec![
            make_quote(0, "IBM", "NYSE", 21.22),
            make_quote(1, "ATT", "NYSE", 98.44),
            make_quote(2, "HOCK", "AMEX", 0.0076),
            make_quote(3, "XIE", "NASDAQ", 33.33),
        ]);

        // perform: table0 + table1
        let mrc_ab = TableValue(Model(mrc_a)) + TableValue(Model(mrc_b));
        let rows = match mrc_ab {
            TableValue(rcv) => rcv.get_rows(),
            _ => Vec::new()
        };

        // verify the results
        assert_eq!(rows, vec![
            make_quote(0, "AAB", "NYSE", 22.44),
            make_quote(1, "WXYZ", "NASDAQ", 66.67),
            make_quote(2, "SSO", "NYSE", 123.44),
            make_quote(3, "RAND", "AMEX", 11.33),
            make_quote(4, "IBM", "NYSE", 21.22),
            make_quote(5, "ATT", "NYSE", 98.44),
            make_quote(6, "HOCK", "AMEX", 0.0076),
            make_quote(7, "XIE", "NASDAQ", 33.33),
        ]);
    }

    #[test]
    fn test_append_table() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<Column>) -> u64 {
            // append some rows to the host table (rc)
            assert_eq!(Number(RowsAffected(4)), rc.append_rows(vec![
                make_quote(0, "IBM", "NYSE", 21.22),
                make_quote(1, "ATT", "NYSE", 98.44),
                make_quote(2, "HOCK", "AMEX", 0.0076),
                make_quote(3, "XIE", "NASDAQ", 33.33),
            ]));

            // create a second table for which to append
            let mrc = ModelRowCollection::from_columns_and_rows(&columns, &vec![
                make_quote(0, "AAA", "NYSE", 22.44),
                make_quote(1, "XYZ", "NASDAQ", 66.67),
                make_quote(2, "SSO", "NYSE", 123.44),
                make_quote(3, "RAND", "AMEX", 11.33),
            ]);

            // append the second table to the host
            assert_eq!(Number(RowsAffected(4)), rc.append_table(Box::new(mrc)));

            // retrieve the entire range of rows
            assert_eq!(rc.read_active_rows().unwrap(), vec![
                make_quote(0, "IBM", "NYSE", 21.22),
                make_quote(1, "ATT", "NYSE", 98.44),
                make_quote(2, "HOCK", "AMEX", 0.0076),
                make_quote(3, "XIE", "NASDAQ", 33.33),
                make_quote(4, "AAA", "NYSE", 22.44),
                make_quote(5, "XYZ", "NASDAQ", 66.67),
                make_quote(6, "SSO", "NYSE", 123.44),
                make_quote(7, "RAND", "AMEX", 11.33),
            ]);

            rc.len().unwrap() as u64
        }

        // test the variants
        verify_variants("append_table", make_quote_columns(), test_variant);
    }

    #[test]
    fn test_delete_row() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<Column>) -> u64 {
            // write some rows
            assert_eq!(Number(RowId(0)), rc.append_row(make_quote(0, "GE", "NYSE", 21.22)));
            assert_eq!(Number(RowId(1)), rc.append_row(make_quote(1, "ATT", "NYSE", 98.44)));
            assert_eq!(Number(RowId(2)), rc.append_row(make_quote(2, "H", "OTC_BB", 0.0076)));
            assert_eq!(Number(RowId(3)), rc.append_row(make_quote(3, "X", "NASDAQ", 33.33)));
            assert_eq!(Number(RowId(4)), rc.append_row(make_quote(4, "OXIDE", "OSS", 0.00)));

            // delete even rows
            assert_eq!(Number(RowsAffected(1)), rc.delete_row(0));
            assert_eq!(Number(RowsAffected(1)), rc.delete_row(2));
            assert_eq!(Number(RowsAffected(1)), rc.delete_row(4));

            // retrieve the entire range of rows
            let rows = rc.read_active_rows().unwrap();
            assert_eq!(rows, vec![
                make_quote(1, "ATT", "NYSE", 98.44),
                make_quote(3, "X", "NASDAQ", 33.33),
            ]);

            rc.len().unwrap() as u64
        }

        // test the variants
        verify_variants("delete_row", make_quote_columns(), test_variant);
    }

    #[test]
    fn test_delete_rows() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<Column>) -> u64 {
            // write some rows
            assert_eq!(Number(RowId(0)), rc.append_row(make_quote(0, "GE", "NYSE", 21.22)));
            assert_eq!(Number(RowId(1)), rc.append_row(make_quote(1, "ATT", "NYSE", 98.44)));
            assert_eq!(Number(RowId(2)), rc.append_row(make_quote(2, "H", "OTC_BB", 0.0076)));
            assert_eq!(Number(RowId(3)), rc.append_row(make_quote(3, "X", "NASDAQ", 33.33)));
            assert_eq!(Number(RowId(4)), rc.append_row(make_quote(4, "OXIDE", "OSS", 0.00)));

            // delete even rows
            assert_eq!(Number(RowsAffected(3)), rc.delete_rows(|row| row.get_id() % 2 == 0));

            // retrieve the entire range of rows
            let rows = rc.read_active_rows().unwrap();
            assert_eq!(rows, vec![
                make_quote(1, "ATT", "NYSE", 98.44),
                make_quote(3, "X", "NASDAQ", 33.33),
            ]);

            rc.len().unwrap() as u64
        }

        // test the variants
        verify_variants("delete_rows", make_quote_columns(), test_variant);
    }

    #[test]
    fn test_describe_table() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<Column>) -> u64 {
            // write a row
            assert_eq!(Number(RowId(0)), rc.append_row(make_quote(0, "GE", "NYSE", 21.22)));

            // describe the table
            let mrc = rc.describe().to_table_impl().unwrap();
            let mrc_columns = mrc.get_columns().to_owned();
            let mrc_rows = mrc.read_active_rows().unwrap();
            let count = rc.count(|_| true);

            for s in TableRenderer::from_collection(rc) {
                println!("{}", s)
            }
            for s in TableRenderer::from_collection(mrc) {
                println!("{}", s)
            }

            assert_eq!(mrc_rows, vec![
                Row::new(0, vec![
                    StringValue("symbol".to_string()),
                    StringValue("String(8)".to_string()),
                    StringValue("null".to_string()),
                    Boolean(true),
                ]),
                Row::new(1, vec![
                    StringValue("exchange".to_string()),
                    StringValue("String(8)".to_string()),
                    StringValue("null".to_string()),
                    Boolean(true),
                ]),
                Row::new(2, vec![
                    StringValue("last_sale".to_string()),
                    StringValue("f64".to_string()),
                    StringValue("null".to_string()),
                    Boolean(true),
                ]),
            ]);

            count
        }

        // test the variants
        verify_variants("describe", make_quote_columns(), test_variant);
    }

    #[test]
    fn test_find() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<Column>) -> u64 {
            // write some rows
            assert_eq!(Number(RowId(0)), rc.append_row(make_quote(0, "GE", "NYSE", 21.22)));
            assert_eq!(Number(RowId(1)), rc.append_row(make_quote(0, "ATT", "NYSE", 98.44)));
            assert_eq!(Number(RowId(2)), rc.append_row(make_quote(0, "HAZ", "OTC_BB", 0.0076)));
            assert_eq!(Number(RowId(3)), rc.append_row(make_quote(0, "XMT", "NASDAQ", 33.33)));
            assert_eq!(Number(RowId(4)), rc.append_row(make_quote(0, "OXIDE", "OSS", 0.001)));

            // resize and verify
            let expected = vec![
                StringValue("XMT".to_string()),
                StringValue("NASDAQ".to_string()),
                Number(F64Value(33.33)),
            ];
            let actual = rc.scan_first(TableScanPlan::new(0, true, ColumnScan {
                column_index: 0,
                column_value: StringValue("XMT".into()),
            })).unwrap()
                .map(|row| row.get_values())
                .unwrap();
            assert_eq!(actual, expected);

            rc.len().unwrap() as u64
        }

        // test the variants
        verify_variants("find", make_quote_columns(), test_variant);
    }

    #[test]
    fn test_find_next() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<Column>) -> u64 {
            // write some rows
            assert_eq!(Number(RowId(0)), rc.append_row(make_quote(0, "GE", "NYSE", 21.22)));
            assert_eq!(Number(RowId(1)), rc.append_row(make_quote(0, "ATT", "NYSE", 98.44)));
            assert_eq!(Number(RowId(2)), rc.append_row(make_quote(0, "H", "OTC_BB", 0.0076)));
            assert_eq!(Number(RowId(3)), rc.append_row(make_quote(0, "X", "NASDAQ", 33.33)));
            assert_eq!(Number(RowId(4)), rc.append_row(make_quote(0, "OXIDE", "OSS", 0.00)));

            // resize and verify
            assert_eq!(
                Some(make_quote(2, "H", "OTC_BB", 0.0076)),
                rc.find_next(0, |row| row[0] == StringValue("H".to_string())).unwrap()
            );

            rc.len().unwrap() as u64
        }

        // test the variants
        verify_variants("find_next_too", make_quote_columns(), test_variant);
    }

    #[test]
    fn test_fold_left() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<Column>) -> u64 {
            // write some rows
            assert_eq!(Number(RowId(0)), rc.append_row(make_quote(0, "GE", "NYSE", 21.22)));
            assert_eq!(Number(RowId(1)), rc.append_row(make_quote(0, "ATT", "NYSE", 98.44)));
            assert_eq!(Number(RowId(2)), rc.append_row(make_quote(0, "H", "OTC_BB", 0.0076)));
            assert_eq!(Number(RowId(3)), rc.append_row(make_quote(0, "X", "NASDAQ", 33.33)));
            assert_eq!(Number(RowId(4)), rc.append_row(make_quote(0, "OXIDE", "OSS", 0.00)));

            // resize and verify
            assert_eq!(Number(F64Value(152.99759999999998)),
                       rc.fold_left(Number(F64Value(0.)), |agg, row| agg + row.get(2)));

            rc.len().unwrap() as u64
        }

        // test the variants
        verify_variants("fold_left", make_quote_columns(), test_variant);
    }

    #[test]
    fn test_fold_left_where() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<Column>) -> u64 {
            // write some rows
            assert_eq!(Number(RowId(0)), rc.append_row(make_quote(0, "ABC", "NYSE", 21.22)));
            assert_eq!(Number(RowId(1)), rc.append_row(make_quote(0, "XCI", "NYSE", 98.44)));
            assert_eq!(Number(RowId(2)), rc.append_row(make_quote(0, "JJJ", "NASDAQ", 0.0076)));
            assert_eq!(Number(RowId(3)), rc.append_row(make_quote(0, "BMX", "NASDAQ", 33.33)));
            assert_eq!(Number(RowId(4)), rc.append_row(make_quote(0, "RIDE", "NASDAQ", 0.00)));

            // fold and verify
            let condition = Equal(
                Box::new(Variable("exchange".into())),
                Box::new(Literal(StringValue("NYSE".into()))));
            assert_eq!(
                rc.for_left_where(
                    &condition,
                    Number(F64Value(0.)),
                    |accum, row| accum.to_owned() + row.get(2)),
                Number(F64Value(119.66)),
            );

            rc.len().unwrap() as u64
        }

        // test the variants
        verify_variants("fold_left_where", make_quote_columns(), test_variant);
    }

    #[test]
    fn test_fold_right() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<Column>) -> u64 {
            // write some rows
            assert_eq!(Number(RowId(0)), rc.append_row(make_quote(0, "GE", "NYSE", 21.22)));
            assert_eq!(Number(RowId(1)), rc.append_row(make_quote(0, "ATT", "NYSE", 98.44)));
            assert_eq!(Number(RowId(2)), rc.append_row(make_quote(0, "H", "OTC_BB", 0.0076)));
            assert_eq!(Number(RowId(3)), rc.append_row(make_quote(0, "X", "NASDAQ", 33.33)));
            assert_eq!(Number(RowId(4)), rc.append_row(make_quote(0, "OXIDE", "OSS", 0.00)));

            // fold and verify
            assert_eq!(
                Number(F64Value(347.00239999999997)),
                rc.fold_right(Number(F64Value(500.)), |agg, row| agg - row.get(2))
            );

            rc.len().unwrap() as u64
        }

        // test the variants
        verify_variants("fold_right", make_quote_columns(), test_variant);
    }

    #[test]
    fn test_iterator() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<Column>) -> u64 {
            // write some rows
            assert_eq!(Number(RowId(0)), rc.append_row(make_quote(0, "A", "NYSE", 100.74)));
            assert_eq!(Number(RowId(1)), rc.append_row(make_quote(0, "B", "NYSE", 50.19)));
            assert_eq!(Number(RowId(2)), rc.append_row(make_quote(0, "C", "AMEX", 35.11)));
            assert_eq!(Number(RowId(3)), rc.append_row(make_quote(0, "D", "NASDAQ", 16.45)));
            assert_eq!(Number(RowId(4)), rc.append_row(make_quote(0, "E", "NYSE", 0.26)));

            // use an iterator
            let mut total = Number(F64Value(0.));
            for row in rc.iter() {
                total = total + row[2].to_owned();
            }

            // fold and verify
            assert_eq!(total, Number(F64Value(202.75)));

            rc.len().unwrap() as u64
        }

        // test the variants
        verify_variants("iterator", make_quote_columns(), test_variant);
    }

    #[test]
    fn test_overwrite_field() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<Column>) -> u64 {
            // write some rows
            assert_eq!(Number(RowId(0)), rc.append_row(make_quote(0, "GE", "NYSE", 21.22)));
            assert_eq!(Number(RowId(1)), rc.append_row(make_quote(0, "H", "OTC_BB", 0.0076)));
            assert_eq!(Number(RowId(2)), rc.append_row(make_quote(0, "X", "NASDAQ", 33.33)));
            assert_eq!(Number(RowId(3)), rc.append_row(make_quote(0, "BAT", "AMEX", 1.66)));

            // overwrite the field at (0, 0)
            assert_eq!(Number(RowsAffected(1)), rc.overwrite_field(0, 1, StringValue("AMEX".to_string())));

            // verify the row
            assert_eq!(rc.read_one(0).unwrap(), Some(
                make_quote(0, "GE", "AMEX", 21.22)
            ));

            rc.len().unwrap() as u64
        }

        // test the variants
        verify_variants("overwrite_field", make_quote_columns(), test_variant);
    }

    #[test]
    fn test_overwrite_field_metadata() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<Column>) -> u64 {
            // write a row at 0
            assert_eq!(Number(RowsAffected(1)), rc.overwrite_row(0, make_quote(0, "GE", "NYSE", 21.22)));

            // overwrite the field metadata at (0, 1)
            assert_eq!(Number(RowsAffected(1)), rc.overwrite_field_metadata(0, 1, FieldMetadata::new(false)));

            // re-read the field metadata at (0, 1)
            assert_eq!(
                FieldMetadata::new(false),
                rc.read_field_metadata(0, 1).unwrap());

            // verify the row
            for s in TableRenderer::from_table(&rc) { println!("{}", s) }
            assert_eq!(
                rc.read_one(0).unwrap(),
                Some(
                    Row::new(0, vec![
                        StringValue("GE".to_string()),
                        Null,
                        Number(F64Value(21.22)),
                    ]),
                ));

            rc.len().unwrap() as u64
        }

        // test the variants
        verify_variants("overwrite_field_metadata", make_quote_columns(), test_variant);
    }

    #[test]
    fn test_update_row() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<Column>) -> u64 {
            // write some rows
            assert_eq!(Number(RowId(0)), rc.append_row(make_quote(0, "GE", "NYSE", 21.22)));
            assert_eq!(Number(RowId(1)), rc.append_row(make_quote(0, "ATT", "NYSE", 98.44)));
            assert_eq!(Number(RowId(2)), rc.append_row(make_quote(0, "H", "OTC_BB", 0.0076)));
            assert_eq!(Number(RowId(3)), rc.append_row(make_quote(0, "X", "NASDAQ", 33.33)));
            assert_eq!(Number(RowId(4)), rc.append_row(make_quote(0, "OXIDE", "OSS", 0.00)));

            // retrieve the entire range of rows
            let rows = rc.read_active_rows().unwrap();
            assert_eq!(rows, vec![
                make_quote(0, "GE", "NYSE", 21.22),
                make_quote(1, "ATT", "NYSE", 98.44),
                make_quote(2, "H", "OTC_BB", 0.0076),
                make_quote(3, "X", "NASDAQ", 33.33),
                make_quote(4, "OXIDE", "OSS", 0.00),
            ]);

            // update a row
            assert_eq!(
                Number(RowsAffected(1)),
                rc.update_row(2, make_quote(2, "H.Q", "OTC_BB", 0.0001)));

            // retrieve the entire range of rows
            let rows = rc.read_active_rows().unwrap();
            assert_eq!(rows, vec![
                make_quote(0, "GE", "NYSE", 21.22),
                make_quote(1, "ATT", "NYSE", 98.44),
                make_quote(2, "H.Q", "OTC_BB", 0.0001),
                make_quote(3, "X", "NASDAQ", 33.33),
                make_quote(4, "OXIDE", "OSS", 0.00),
            ]);

            rc.len().unwrap() as u64
        }

        // test the variants
        verify_variants("update", make_quote_columns(), test_variant);
    }

    #[test]
    fn test_write_delete_then_read_rows() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<Column>) -> u64 {
            // write some rows
            assert_eq!(Number(RowsAffected(4)), rc.append_rows(vec![
                make_quote(0, "GE", "NYSE", 21.22),
                make_quote(0, "ATT", "NYSE", 98.44),
                make_quote(0, "H", "OTC_BB", 0.0076),
                make_quote(0, "GG", "NASDAQ", 33.33),
            ]));

            // verify the initial state
            let rows = rc.read_active_rows().unwrap();
            assert_eq!(rows, vec![
                make_quote(0, "GE", "NYSE", 21.22),
                make_quote(1, "ATT", "NYSE", 98.44),
                make_quote(2, "H", "OTC_BB", 0.0076),
                make_quote(3, "GG", "NASDAQ", 33.33),
            ]);

            // delete a row
            assert_eq!(Number(RowsAffected(1)), rc.delete_row(2));

            // verify the current state
            let rows = rc.read_active_rows().unwrap();
            assert_eq!(rows, vec![
                make_quote(0, "GE", "NYSE", 21.22),
                make_quote(1, "ATT", "NYSE", 98.44),
                make_quote(3, "GG", "NASDAQ", 33.33),
            ]);

            rc.len().unwrap() as u64
        }

        // test the variants
        verify_variants("delete", make_quote_columns(), test_variant);
    }

    #[test]
    fn test_push_pop() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<Column>) -> u64 {
            let params = make_quote_parameters();
            assert_eq!(Number(RowId(0)), rc.push_row(make_quote(0, "BILL", "AMEX", 12.33)));
            assert_eq!(Number(RowId(1)), rc.push_row(make_quote(1, "TED", "NYSE", 56.2456)));
            assert_eq!(
                rc.pop_row(params.clone()),
                Structured(Firm(make_quote(1, "TED", "NYSE", 56.2456), params.clone())));
            assert_eq!(
                rc.pop_row(params.clone()),
                Structured(Firm(make_quote(0, "BILL", "AMEX", 12.33), params.clone())));
            assert_eq!(rc.pop_row(params.clone()), Undefined);

            rc.len().unwrap() as u64
        }

        // test the variants
        verify_variants("push_pop", make_quote_columns(), test_variant);
    }

    #[test]
    fn test_read_column_slice() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<Column>) -> u64 {
            rc.append_row(make_quote(0, "ABC", "AMEX", 12.33));
            rc.append_row(make_quote(1, "TED", "OTC", 0.2456));
            rc.append_row(make_quote(2, "BIZ", "NYSE", 9.775));
            rc.append_row(make_quote(3, "GOTO", "OTC", 0.1442));
            rc.append_row(make_quote(4, "XYZ", "NASDAQ", 0.0289));

            // produce the scan
            let values = rc.read_column_slice(0).unwrap();
            assert_eq!(values, vec![
                StringValue("ABC".into()),
                StringValue("TED".into()),
                StringValue("BIZ".into()),
                StringValue("GOTO".into()),
                StringValue("XYZ".into()),
            ]);
            rc.len().unwrap() as u64
        }

        // test the variants
        verify_variants("read_column_slice", make_quote_columns(), test_variant);
    }

    #[test]
    fn test_read_range() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<Column>) -> u64 {
            rc.append_row(make_quote(0, "ABC", "AMEX", 12.33));
            rc.append_row(make_quote(1, "TED", "OTC", 0.2456));
            rc.append_row(make_quote(2, "BIZ", "NYSE", 9.775));
            rc.append_row(make_quote(3, "GOTO", "OTC", 0.1442));
            rc.append_row(make_quote(4, "XYZ", "NASDAQ", 0.0289));

            // produce the scan
            let rows = rc.read_range(1..4).unwrap();
            assert_eq!(rows, vec![
                make_quote(1, "TED", "OTC", 0.2456),
                make_quote(2, "BIZ", "NYSE", 9.775),
                make_quote(3, "GOTO", "OTC", 0.1442),
            ]);

            rc.len().unwrap() as u64
        }

        // test the variants
        verify_variants("read_range", make_quote_columns(), test_variant);
    }

    #[test]
    fn test_resize_shrink() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<Column>) -> u64 {
            // insert some rows and verify the size
            assert_eq!(Number(RowsAffected(1)), rc.overwrite_row(5, make_quote(0, "DUMMY", "OTC_BB", 0.0001)));
            assert_eq!(6, rc.len().unwrap());

            // resize and verify
            assert_eq!(Boolean(true), rc.resize(0));
            assert_eq!(rc.len().unwrap(), 0);

            rc.len().unwrap() as u64
        }

        // test the variants
        verify_variants("resize_shrink", make_quote_columns(), test_variant);
    }

    #[test]
    fn test_resize_grow() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<Column>) -> u64 {
            assert_eq!(Boolean(true), rc.resize(0));

            // insert some rows and verify the size
            assert_eq!(Number(RowsAffected(1)), rc.overwrite_row(5, make_quote(0, "DUMMY", "OTC_BB", 0.0001)));
            assert!(rc.len().unwrap() >= 6);

            // resize and verify
            let _ = rc.resize(50);
            assert!(rc.len().unwrap() >= 50);

            rc.len().unwrap() as u64
        }

        // test the variants
        verify_variants("resize_grow", make_quote_columns(), test_variant);
    }

    #[test]
    fn test_resize_compact() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<Column>) -> u64 {
            // write some rows
            assert_eq!(Number(RowsAffected(4)), rc.append_rows(vec![
                make_quote(0, "GE", "NYSE", 21.22),
                make_quote(0, "ATT", "NYSE", 98.45),
                make_quote(0, "H", "OTC_BB", 0.0076),
                make_quote(0, "GG", "NASDAQ", 33.33),
            ]));

            // verify the initial state
            assert_eq!(rc.read_active_rows().unwrap(), vec![
                make_quote(0, "GE", "NYSE", 21.22),
                make_quote(1, "ATT", "NYSE", 98.45),
                make_quote(2, "H", "OTC_BB", 0.0076),
                make_quote(3, "GG", "NASDAQ", 33.33),
            ]);

            // delete a row
            assert_eq!(Number(RowsAffected(1)), rc.delete_row(1));
            assert_eq!(Number(RowsAffected(1)), rc.delete_row(3));

            // verify the current state
            let rows = rc.read_active_rows().unwrap();
            assert_eq!(rows, vec![
                make_quote(0, "GE", "NYSE", 21.22),
                make_quote(2, "H", "OTC_BB", 0.0076),
            ]);

            // compact the table
            assert_eq!(Number(F64Value(2.0)), rc.compact());

            // verify the final state
            let rows = rc.read_active_rows().unwrap();
            assert_eq!(rows, vec![
                make_quote(0, "GE", "NYSE", 21.22),
                make_quote(1, "H", "OTC_BB", 0.0076),
            ]);

            rc.len().unwrap() as u64
        }

        // test the variants
        verify_variants("compact", make_quote_columns(), test_variant);
    }

    #[test]
    fn test_reverse() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<Column>) -> u64 {
            rc.append_row(make_quote(0, "ABC", "AMEX", 12.33));
            rc.append_row(make_quote(1, "UNO", "OTC", 0.2456));
            rc.append_row(make_quote(2, "BIZ", "NYSE", 9.775));
            rc.append_row(make_quote(3, "GOTO", "OTC", 0.1442));
            rc.append_row(make_quote(4, "XYZ", "NYSE", 0.0289));

            // produce the reverse order
            let rrc = rc.reverse().unwrap();
            let rows = rrc.read_active_rows().unwrap();
            assert_eq!(rows, vec![
                make_quote(0, "XYZ", "NYSE", 0.0289),
                make_quote(1, "GOTO", "OTC", 0.1442),
                make_quote(2, "BIZ", "NYSE", 9.775),
                make_quote(3, "UNO", "OTC", 0.2456),
                make_quote(4, "ABC", "AMEX", 12.33),
            ]);

            rc.len().unwrap() as u64
        }

        // test the variants
        verify_variants("reverse", make_quote_columns(), test_variant);
    }

    #[test]
    fn test_scan_all_rows() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<Column>) -> u64 {
            assert_eq!(Number(RowsAffected(5)), rc.append_rows(vec![
                make_quote(0, "ABC", "AMEX", 12.33),
                make_quote(1, "TED", "OTC", 0.2456),
                make_quote(2, "BIZ", "NYSE", 9.775),
                make_quote(3, "GOTO", "OTC", 0.1442),
                make_quote(4, "XYZ", "NASDAQ", 0.0289),
            ]));

            // delete some rows
            for id in [0, 2, 4] {
                assert_eq!(Number(RowsAffected(1)), rc.delete_row(id));
            }

            // produce the scan
            let rows = rc.examine_rows().unwrap();
            let scan_columns = rc.get_columns();
            for s in TableRenderer::from_rows(scan_columns, &rows) { println!("{}", s); }

            // verify row states: active or inactive
            assert_eq!(rows, vec![
                make_scan_quote(0, "ABC", "AMEX", 12.33, false),
                make_scan_quote(1, "TED", "OTC", 0.2456, true),
                make_scan_quote(2, "BIZ", "NYSE", 9.775, false),
                make_scan_quote(3, "GOTO", "OTC", 0.1442, true),
                make_scan_quote(4, "XYZ", "NASDAQ", 0.0289, false),
            ]);

            rc.len().unwrap() as u64
        }

        // test the variants
        verify_variants("scan_all_rows", make_quote_columns(), test_variant);
    }

    #[test]
    fn test_find_next_fn() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<Column>) -> u64 {
            assert_eq!(Number(RowsAffected(5)), rc.append_rows(vec![
                make_quote(0, "TED", "OTC", 0.2456),
                make_quote(1, "ABC", "AMEX", 12.33),
                make_quote(2, "BIZ", "NYSE", 9.775),
                make_quote(3, "GOTO", "OTC", 0.1442),
                make_quote(4, "XYZ", "NASDAQ", 0.0289),
            ]));

            // delete some rows
            for id in [0, 2, 4] {
                assert_eq!(Number(RowsAffected(1)), rc.delete_row(id));
            }

            // perform a forward scan
            let result = rc.find_next(2, |row| {
                matches!(row.get(1), StringValue(s) if s == "OTC".to_string())
            }).unwrap();

            // verify the result
            let (scan_columns, scan_row) = result
                .map(|row| (rc.get_columns().to_owned(), row))
                .unwrap();
            assert_eq!(
                scan_row,
                make_quote(3, "GOTO", "OTC", 0.1442)
            );

            rc.len().unwrap() as u64
        }

        // test the variants
        verify_variants("scan_forward", make_quote_columns(), test_variant);
    }

    #[test]
    fn test_scan_next() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<Column>) -> u64 {
            // write some rows
            assert_eq!(Number(RowId(0)), rc.append_row(make_quote(0, "GE", "NYSE", 21.22)));
            assert_eq!(Number(RowId(1)), rc.append_row(make_quote(0, "ATT", "NYSE", 98.44)));
            assert_eq!(Number(RowId(2)), rc.append_row(make_quote(0, "H", "OTC_BB", 0.0076)));
            assert_eq!(Number(RowId(3)), rc.append_row(make_quote(0, "X", "NASDAQ", 33.33)));
            assert_eq!(Number(RowId(4)), rc.append_row(make_quote(0, "OXIDE", "OSS", 0.00)));

            // resize and verify
            assert_eq!(
                Some(make_quote(3, "X", "NASDAQ", 33.33)),
                rc.scan_next(TableScanPlan::column_scan(0, StringValue("X".into())))
                    .unwrap()
            );

            rc.len().unwrap() as u64
        }

        // test the variants
        verify_variants("scan_next", make_quote_columns(), test_variant);
    }

    #[test]
    fn test_find_previous() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<Column>) -> u64 {
            assert_eq!(Number(RowsAffected(5)), rc.append_rows(vec![
                make_quote(0, "TED", "OTC", 0.2456),
                make_quote(1, "GOTO", "OTC", 0.1442),
                make_quote(2, "BIZ", "NYSE", 9.775),
                make_quote(3, "ABC", "AMEX", 12.33),
                make_quote(4, "XYZ", "NASDAQ", 0.0289),
            ]));

            // delete some rows
            for id in [0, 2, 4] {
                assert_eq!(Number(RowsAffected(1)), rc.delete_row(id));
            }

            // perform a reverse scan
            let result = rc.find_previous(4, |(row, _)| {
                matches!(row.get(1), StringValue(s) if s == "OTC".to_string())
            }).unwrap();

            // verify the result
            let (scan_columns, scan_row) = result
                .map(|(row, _, _)| (columns, row))
                .unwrap();
            assert_eq!(
                scan_row,
                make_quote(1, "GOTO", "OTC", 0.1442)
            );

            rc.len().unwrap() as u64
        }

        // test the variants
        verify_variants("scan_reverse", make_quote_columns(), test_variant);
    }

    #[test]
    fn test_performance() {
        fn test_variant(label: &str, mut rc: Box<dyn RowCollection>, columns: Vec<Column>) -> u64 {
            rc.resize(0);
            test_write_performance(label, &mut rc, &columns, 20_000).unwrap();
            test_read_performance(label, &rc).unwrap();
            rc.len().unwrap() as u64
        }

        // test the variants
        verify_variants("performance", make_quote_columns(), test_variant);
    }

    fn test_write_performance(label: &str, rc: &mut Box<dyn RowCollection>, columns: &Vec<Column>, total: usize) -> std::io::Result<()> {
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
            let row = make_quote(0, &symbol, exchange, last_sale);
            rc.overwrite_row(id, row);
        }
        let end_time = SystemTime::now().duration_since(UNIX_EPOCH)
            .map_err(|e| cnv_error!(e))?.as_millis();
        let elapsed_time = end_time - start_time;
        let elapsed_time_sec = elapsed_time as f64 / 1000.;
        let rpm = total as f64 / elapsed_time as f64;
        println!("* {} wrote {} row(s) in {} msec ({:.2} seconds, {:.2} records/msec)",
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
        println!("* {} read {} row(s) in {} msec ({:.2} seconds, {:.2} records/msec)",
                 label, total, elapsed_time, elapsed_time_sec, rpm);
        Ok(())
    }

    fn verify_variants(name: &str, columns: Vec<Column>, test: fn(&str, Box<dyn RowCollection>, Vec<Column>) -> u64) {
        // rounds a number to 2-significant figures
        fn round_2sf(value: f64) -> f64 { (value * 100.0).round() / 100.0 }

        // report function
        fn work(
            mut mrc: ModelRowCollection,
            name: &str,
            kind: &str,
            columns: &Vec<Column>,
            tester: fn(&str, &str, Vec<Column>, fn(&str, Box<dyn RowCollection>, Vec<Column>) -> u64) -> u64,
            test: fn(&str, Box<dyn RowCollection>, Vec<Column>) -> u64,
        ) -> ModelRowCollection {
            println!("Testing: {name} -> {kind}");
            let t0 = Local::now();
            let processed = tester(name, kind, columns.clone(), test);
            let execution_time = compute_time_millis(Local::now() - t0);

            // record this test
            let rate = processed.to_f64().unwrap_or(0.) / execution_time;
            mrc.append_row(Row::new(0, vec![
                StringValue(name.to_owned()),
                StringValue(kind.to_owned()),
                Number(U64Value(processed)),
                Number(F64Value(execution_time)),
                Number(F64Value(round_2sf(rate))),
            ]));
            mrc
        }

        let mut mrc = ModelRowCollection::from_parameters(&vec![
            Parameter::new("name", StringType(64)),
            Parameter::new("kind", StringType(20)),
            Parameter::new("processed", NumberType(U64Kind)),
            Parameter::new("process_time_millis", NumberType(F64Kind)),
            Parameter::new("rows_per_millis", NumberType(F64Kind)),
        ]);
        mrc = work(mrc, name, "Dataframe|Binary", &columns, verify_dataframe_binary_variant, test);
        mrc = work(mrc, name, "Dataframe|Disk", &columns, verify_dataframe_file_variant, test);
        mrc = work(mrc, name, "Dataframe|Model", &columns, verify_dataframe_model_variant, test);
        mrc = work(mrc, name, "Binary", &columns, verify_byte_array_variant, test);
        mrc = work(mrc, name, "EventSource", &columns, verify_event_sourcing_variant, test);
        mrc = work(mrc, name, "File", &columns, verify_file_variant, test);
        mrc = work(mrc, name, "HashTable", &columns, verify_hash_table_variant, test);
        mrc = work(mrc, name, "Hybrid", &columns, verify_hybrid_table_variant, test);
        mrc = work(mrc, name, "Model", &columns, verify_model_variant, test);

        let rc: Box<dyn RowCollection> = Box::new(mrc);
        for s in TableRenderer::from_table_with_ids(&rc).unwrap() {
            println!("{}", s)
        }
    }

    fn verify_byte_array_variant(name: &str, kind: &str, columns: Vec<Column>, test_variant: fn(&str, Box<dyn RowCollection>, Vec<Column>) -> u64) -> u64 {
        let brc = ByteRowCollection::from_bytes(columns.to_owned(), Vec::new());
        test_variant(kind, Box::new(brc), columns)
    }

    fn verify_dataframe_binary_variant(name: &str, kind: &str, columns: Vec<Column>, test_variant: fn(&str, Box<dyn RowCollection>, Vec<Column>) -> u64) -> u64 {
        let brc = Dataframe::Binary(ByteRowCollection::from_bytes(columns.to_owned(), Vec::new()));
        test_variant(kind, Box::new(brc), columns)
    }

    fn verify_dataframe_file_variant(name: &str, kind: &str, columns: Vec<Column>, test_variant: fn(&str, Box<dyn RowCollection>, Vec<Column>) -> u64) -> u64 {
        let ns = Namespace::new("disk", name, "stocks");
        let params = Parameter::from_columns(&columns);
        let frc = Dataframe::Disk(FileRowCollection::create_table(&ns, &params).unwrap());
        test_variant(kind, Box::new(frc), columns.to_owned())
    }

    fn verify_dataframe_model_variant(name: &str, kind: &str, columns: Vec<Column>, test_variant: fn(&str, Box<dyn RowCollection>, Vec<Column>) -> u64) -> u64 {
        let mrc = Dataframe::Model(ModelRowCollection::with_rows(columns.to_owned(), Vec::new()));
        test_variant(kind, Box::new(mrc), columns)
    }

    fn verify_event_sourcing_variant(name: &str, kind: &str, columns: Vec<Column>, test_variant: fn(&str, Box<dyn RowCollection>, Vec<Column>) -> u64) -> u64 {
        let ns = Namespace::new("journaled", name, "stocks");
        let params = Parameter::from_columns(&columns);
        let mut jrc = EventSourceRowCollection::new(&ns, &params).unwrap();
        jrc.resize(0);
        test_variant(kind, Box::new(jrc), columns.to_owned())
    }

    fn verify_file_variant(name: &str, kind: &str, columns: Vec<Column>, test_variant: fn(&str, Box<dyn RowCollection>, Vec<Column>) -> u64) -> u64 {
        let ns = Namespace::new("file", name, "stocks");
        let params = Parameter::from_columns(&columns);
        let frc = FileRowCollection::create_table(&ns, &params).unwrap();
        test_variant(kind, Box::new(frc), columns.to_owned())
    }

    fn verify_hash_table_variant(name: &str, kind: &str, columns: Vec<Column>, test_variant: fn(&str, Box<dyn RowCollection>, Vec<Column>) -> u64) -> u64 {
        let ns = Namespace::new("hashing", name, "stocks");
        let params = Parameter::from_columns(&columns);
        let frc = FileRowCollection::create_table(&ns, &params).unwrap();
        let hrc = HashTableRowCollection::new(0, Box::new(frc)).unwrap();
        test_variant(kind, Box::new(hrc), columns.to_owned())
    }

    fn verify_hybrid_table_variant(name: &str, kind: &str, columns: Vec<Column>, test_variant: fn(&str, Box<dyn RowCollection>, Vec<Column>) -> u64) -> u64 {
        let ns = Namespace::new("hybrid", name, "stocks");
        let params = Parameter::from_columns(&columns);
        let hrc = HybridRowCollection::new(&ns, &params, 100).unwrap();
        test_variant(kind, Box::new(hrc), columns.to_owned())
    }

    fn verify_model_variant(name: &str, kind: &str, columns: Vec<Column>, test_variant: fn(&str, Box<dyn RowCollection>, Vec<Column>) -> u64) -> u64 {
        let mrc = ModelRowCollection::with_rows(columns.to_owned(), Vec::new());
        test_variant(kind, Box::new(mrc), columns)
    }
}
