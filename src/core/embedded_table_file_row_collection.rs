#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Embedded table file row-collection module
////////////////////////////////////////////////////////////////////

use crate::byte_row_collection::ByteRowCollection;
use crate::columns::Column;
use crate::dataframe::Dataframe;
use crate::errors::throw;
use crate::errors::Errors::{Exact, TypeMismatch};
use crate::errors::TypeMismatchErrors::TableExpected;
use crate::field::FieldMetadata;
use crate::file_row_collection::FileRowCollection;
use crate::row_collection::RowCollection;
use crate::row_metadata::RowMetadata;
use crate::structures::Row;
use crate::typed_values::TypedValue;
use serde::{Deserialize, Serialize};

/// File-based Embedded Table RowCollection implementation
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct EmbeddedTableFileRowCollection {
    columns: Vec<Column>,
    record_size: usize,
    capacity: usize,
    watermark: usize,
    // host fields
    host_frc: FileRowCollection,
    host_column_id: usize,
    host_row_id: usize,
}

impl EmbeddedTableFileRowCollection {
    /// Creates a new Embedded Table RowCollection
    pub fn new(
        columns: Vec<Column>,
        capacity: usize,
        host_frc: FileRowCollection,
        host_column_id: usize,
        host_row_id: usize,
    ) -> Self {
        Self {
            record_size: Row::compute_record_size(&columns),
            columns,
            watermark: 0,
            host_frc,
            host_column_id,
            host_row_id,
            capacity,
        }
    }

    fn find_embedded_table(&self) -> std::io::Result<Option<Dataframe>> {
        // read the host row
        match self.host_frc.read_one(self.host_row_id)? {
            Some(host_row) =>
                // get the table from the host column
                match host_row.get(self.host_column_id) {
                    TypedValue::TableValue(df) => Ok(Some(df)),
                    other => throw(TypeMismatch(TableExpected(other.to_code())))
                }
            None => Ok(None)
        }
    }

    fn get_embedded_table(&self) -> std::io::Result<Dataframe> {
        Ok(match self.find_embedded_table()? {
            Some(df) => df,
            None => Dataframe::Binary(ByteRowCollection::new(self.columns.clone(), self.capacity))
        })
    }

    fn save_embedded_table(&mut self, df: Dataframe) -> std::io::Result<i64> {
        self.overwrite_field(
            self.host_row_id, 
            self.host_column_id, 
            TypedValue::TableValue(df)
        )
    }
}

impl RowCollection for EmbeddedTableFileRowCollection {
    fn get_columns(&self) -> &Vec<Column> {
        &self.columns
    }

    fn get_record_size(&self) -> usize {
        self.record_size
    }

    fn get_rows(&self) -> Vec<Row> {
        self.get_embedded_table().ok()
            .map(|df| df.get_rows()).unwrap_or(vec![])
    }

    fn len(&self) -> std::io::Result<usize> {
        Ok(self.capacity)
    }

    fn overwrite_field(&mut self, id: usize, column_id: usize, new_value: TypedValue) -> std::io::Result<i64> {
        let mut df = self.get_embedded_table()?;
        let result = df.overwrite_field(id, column_id, new_value)?;
        self.save_embedded_table(df).map(|_| result)
    }

    fn overwrite_field_metadata(
        &mut self,
        id: usize,
        column_id: usize,
        metadata: FieldMetadata
    ) -> std::io::Result<i64> {
        let mut df = self.get_embedded_table()?;
        let result = df.overwrite_field_metadata(id, column_id, metadata)?;
        self.save_embedded_table(df).map(|_| result)
    }

    fn overwrite_row(&mut self, id: usize, row: Row) -> std::io::Result<i64> {
        let mut df = self.get_embedded_table()?;
        let result = df.overwrite_row(id, row)?;
        self.save_embedded_table(df).map(|_| result)
    }

    fn overwrite_row_metadata(&mut self, id: usize, metadata: RowMetadata) -> std::io::Result<i64> {
        let mut df = self.get_embedded_table()?;
        let result = df.overwrite_row_metadata(id, metadata)?;
        self.save_embedded_table(df).map(|_| result)
    }

    fn read_field(&self, id: usize, column_id: usize) -> std::io::Result<TypedValue> {
        self.get_embedded_table()?.read_field(id, column_id)
    }

    fn read_field_metadata(&self, id: usize, column_id: usize) -> std::io::Result<FieldMetadata> {
        self.get_embedded_table()?.read_field_metadata(id, column_id)
    }

    fn read_row(&self, id: usize) -> std::io::Result<(Row, RowMetadata)> {
        self.get_embedded_table()?.read_row(id)
    }

    fn read_row_metadata(&self, id: usize) -> std::io::Result<RowMetadata> {
        self.get_embedded_table()?.read_row_metadata(id)
    }

    fn resize(&mut self, new_size: usize) -> std::io::Result<bool> {
        if new_size <= self.capacity {
            self.watermark = new_size;
            Ok(true)
        } else {
            throw(Exact(format!("Cannot be resized to {}", new_size)))
        }
    }
}

/// Unit tests
#[cfg(test)]
mod tests {

}