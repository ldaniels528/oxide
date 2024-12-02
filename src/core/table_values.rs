////////////////////////////////////////////////////////////////////
// RowCollectionValue module
////////////////////////////////////////////////////////////////////

use crate::byte_row_collection::ByteRowCollection;
use crate::field_metadata::FieldMetadata;
use crate::file_row_collection::FileRowCollection;
use crate::model_row_collection::ModelRowCollection;
use crate::row_collection::RowCollection;
use crate::row_metadata::RowMetadata;
use crate::rows::Row;
use crate::table_columns::Column;
use crate::typed_values::TypedValue;
use serde::{Deserialize, Serialize};

/// Table Value Types
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum TableValues {
    Binary(ByteRowCollection),
    Disk(FileRowCollection),
    Model(ModelRowCollection),
}

impl TableValues {
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Self::Binary(rc) => rc.encode(),
            Self::Disk(rc) => rc.encode(),
            Self::Model(rc) => rc.encode(),
        }
    }
}

impl RowCollection for TableValues {
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