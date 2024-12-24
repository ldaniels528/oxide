#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Dataframe class
////////////////////////////////////////////////////////////////////

use crate::byte_row_collection::ByteRowCollection;
use crate::columns::Column;
use crate::dataframe::Dataframe::Disk;
use crate::dataframe_config::DataFrameConfig;
use crate::expression::{Conditions, Expression};
use crate::field_metadata::FieldMetadata;
use crate::file_row_collection::FileRowCollection;
use crate::machine::Machine;
use crate::model_row_collection::ModelRowCollection;
use crate::namespaces::Namespace;
use crate::numbers::Numbers::RowsAffected;
use crate::parameter::Parameter;
use crate::row_collection::RowCollection;
use crate::row_metadata::RowMetadata;
use crate::structures::Row;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::Number;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// DataFrame is a logical representation of table
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Dataframe {
    Binary(ByteRowCollection),
    Disk(FileRowCollection),
    Model(ModelRowCollection),
}

impl Dataframe {
    /// Creates a new table within the specified namespace and having the specified columns
    pub fn create_table(ns: &Namespace, params: &Vec<Parameter>) -> std::io::Result<Self> {
        let path = ns.get_table_file_path();
        let columns = Column::from_parameters(params);
        let config = DataFrameConfig::build(&params);
        config.save(&ns)?;
        let file = Arc::new(FileRowCollection::table_file_create(ns)?);
        Ok(Disk(FileRowCollection::new(columns, file, path.as_str())))
    }

    /// deletes rows from the table based on a condition
    pub fn delete_where(
        mut self,
        machine: &Machine,
        condition: &Option<Conditions>,
        limit: TypedValue,
    ) -> std::io::Result<TypedValue> {
        let mut deleted = 0;
        for id in self.get_indices_with_limit(limit)? {
            // read an active row
            if let Some(row) = self.read_one(id)? {
                // if the predicate matches the condition, delete the row.
                if row.matches(machine, condition, self.get_columns()) {
                    deleted += self.delete_row(id).to_result(|v| v.to_i64())?;
                }
            }
        }
        Ok(Number(RowsAffected(deleted)))
    }

    /// overwrites rows that match the supplied criteria
    pub fn overwrite_where(
        df: Dataframe,
        machine: &Machine,
        fields: &Vec<Expression>,
        values: &Vec<Expression>,
        condition: &Option<Conditions>,
        limit: TypedValue,
    ) -> std::io::Result<(Dataframe, TypedValue)> {
        let mut overwritten = 0;
        let mut df = df;
        for id in df.get_indices_with_limit(limit)? {
            // read an active row
            if let Some(row) = df.read_one(id)? {
                // if the predicate matches the condition, overwrite the row.
                if row.matches(machine, condition, df.get_columns()) {
                    let (machine, my_fields) =
                        machine.with_row(df.get_columns(), &row).evaluate_as_atoms(fields)?;
                    if let (_, TypedValue::ArrayValue(my_values)) = machine.evaluate_array(values)? {
                        let new_row = row.transform(df.get_columns(), &my_fields, my_values.values())?;
                        overwritten += df.overwrite_row(row.get_id(), new_row).to_result(|v| v.to_i64())?;
                    }
                }
            }
        }
        Ok((df, Number(RowsAffected(overwritten))))
    }

    /// restores rows from the table based on a condition
    pub fn undelete_where(
        &mut self,
        machine: &Machine,
        condition: &Option<Conditions>,
        limit: TypedValue,
    ) -> std::io::Result<TypedValue> {
        let mut restored = 0;
        for id in self.get_indices_with_limit(limit)? {
            // read a row with its metadata
            let (row, metadata) = self.read_row(id)?;
            // if the row is inactive and the predicate matches the condition, restore the row.
            if !metadata.is_allocated && row.matches(machine, condition, self.get_columns()) {
                if self.undelete_row(id).is_ok() {
                    restored += 1
                }
            }
        }
        Ok(Number(RowsAffected(restored)))
    }

    /// updates rows that match the supplied criteria
    pub fn update_where(
        mut rc: Dataframe,
        ms: &Machine,
        fields: &Vec<Expression>,
        values: &Vec<Expression>,
        condition: &Option<Conditions>,
        limit: TypedValue,
    ) -> std::io::Result<TypedValue> {
        let columns = rc.get_columns().clone();
        let mut updated = 0;
        for id in rc.get_indices_with_limit(limit)? {
            // read an active row
            if let Some(row) = rc.read_one(id)? {
                // if the predicate matches the condition, update the row.
                if row.matches(ms, condition, &columns) {
                    let (ms, field_names) =
                        ms.with_row(&columns, &row).evaluate_as_atoms(fields)?;
                    if let (_, TypedValue::ArrayValue(field_values)) = ms.evaluate_array(values)? {
                        let new_row = row.transform(&columns, &field_names, field_values.values())?;
                        let result = rc.overwrite_row(id, new_row);
                        if result.is_ok() { updated += 1 }
                    }
                }
            }
        }
        Ok(Number(RowsAffected(updated)))
    }
}

impl RowCollection for Dataframe {
    fn encode(&self) -> Vec<u8> {
        match self {
            Self::Binary(rc) => rc.encode(),
            Self::Disk(rc) => rc.encode(),
            Self::Model(rc) => rc.encode(),
        }
    }

    fn get_columns(&self) -> &Vec<Column> {
        match self {
            Self::Binary(rc) => rc.get_columns(),
            Self::Disk(rc) => rc.get_columns(),
            Self::Model(rc) => rc.get_columns(),
        }
    }

    fn get_record_size(&self) -> usize {
        match self {
            Self::Binary(rc) => rc.get_record_size(),
            Self::Disk(rc) => rc.get_record_size(),
            Self::Model(rc) => rc.get_record_size(),
        }
    }

    fn get_rows(&self) -> Vec<Row> {
        match self {
            Self::Binary(rc) => rc.get_rows(),
            Self::Disk(rc) => rc.get_rows(),
            Self::Model(rc) => rc.get_rows(),
        }
    }

    fn len(&self) -> std::io::Result<usize> {
        match self {
            Self::Binary(rc) => rc.len(),
            Self::Disk(rc) => rc.len(),
            Self::Model(rc) => rc.len(),
        }
    }

    fn overwrite_field(&mut self, id: usize, column_id: usize, new_value: TypedValue) -> TypedValue {
        match self {
            Self::Binary(rc) => rc.overwrite_field(id, column_id, new_value),
            Self::Disk(rc) => rc.overwrite_field(id, column_id, new_value),
            Self::Model(rc) => rc.overwrite_field(id, column_id, new_value),
        }
    }

    fn overwrite_field_metadata(&mut self, id: usize, column_id: usize, metadata: FieldMetadata) -> TypedValue {
        match self {
            Self::Binary(rc) => rc.overwrite_field_metadata(id, column_id, metadata),
            Self::Disk(rc) => rc.overwrite_field_metadata(id, column_id, metadata),
            Self::Model(rc) => rc.overwrite_field_metadata(id, column_id, metadata),
        }
    }

    fn overwrite_row(&mut self, id: usize, row: Row) -> TypedValue {
        match self {
            Self::Binary(rc) => rc.overwrite_row(id, row),
            Self::Disk(rc) => rc.overwrite_row(id, row),
            Self::Model(rc) => rc.overwrite_row(id, row),
        }
    }

    fn overwrite_row_metadata(&mut self, id: usize, metadata: RowMetadata) -> TypedValue {
        match self {
            Self::Binary(rc) => rc.overwrite_row_metadata(id, metadata),
            Self::Disk(rc) => rc.overwrite_row_metadata(id, metadata),
            Self::Model(rc) => rc.overwrite_row_metadata(id, metadata),
        }
    }

    fn read_field(&self, id: usize, column_id: usize) -> TypedValue {
        match self {
            Self::Binary(rc) => rc.read_field(id, column_id),
            Self::Disk(rc) => rc.read_field(id, column_id),
            Self::Model(rc) => rc.read_field(id, column_id),
        }
    }

    fn read_field_metadata(&self, id: usize, column_id: usize) -> std::io::Result<FieldMetadata> {
        match self {
            Self::Binary(rc) => rc.read_field_metadata(id, column_id),
            Self::Disk(rc) => rc.read_field_metadata(id, column_id),
            Self::Model(rc) => rc.read_field_metadata(id, column_id),
        }
    }

    fn read_row(&self, id: usize) -> std::io::Result<(Row, RowMetadata)> {
        match self {
            Self::Binary(rc) => rc.read_row(id),
            Self::Disk(rc) => rc.read_row(id),
            Self::Model(rc) => rc.read_row(id),
        }
    }

    fn read_row_metadata(&self, id: usize) -> std::io::Result<RowMetadata> {
        match self {
            Self::Binary(rc) => rc.read_row_metadata(id),
            Self::Disk(rc) => rc.read_row_metadata(id),
            Self::Model(rc) => rc.read_row_metadata(id),
        }
    }

    fn resize(&mut self, new_size: usize) -> TypedValue {
        match self {
            Self::Binary(rc) => rc.resize(new_size),
            Self::Disk(rc) => rc.resize(new_size),
            Self::Model(rc) => rc.resize(new_size),
        }
    }
}