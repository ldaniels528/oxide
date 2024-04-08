////////////////////////////////////////////////////////////////////
// byte row-collection module
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

use crate::fields::Field;
use crate::row_collection::RowCollection;
use crate::row_metadata::RowMetadata;
use crate::rows::Row;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue;

/// Byte-vector-based RowCollection implementation
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ByteRowCollection {
    columns: Vec<TableColumn>,
    rows: Vec<Vec<u8>>,
    record_size: usize,
    watermark: usize,
}

impl ByteRowCollection {
    pub fn new(columns: Vec<TableColumn>, rows: Vec<Vec<u8>>) -> ByteRowCollection {
        ByteRowCollection {
            record_size: Row::compute_record_size(&columns),
            columns,
            rows,
            watermark: 0,
        }
    }
}

impl RowCollection for ByteRowCollection {
    fn get_record_size(&self) -> usize { self.record_size }

    fn len(&self) -> std::io::Result<usize> { Ok(self.watermark) }

    fn overwrite(&mut self, id: usize, row: &Row) -> std::io::Result<usize> {
        // resize the rows to prevent overflow
        if self.rows.len() <= id {
            self.rows.resize(id + 1, vec![]);
        }

        // set the block, update the watermark
        self.rows[id] = row.encode();
        if self.watermark <= id {
            self.watermark = id + 1;
        }
        Ok(1)
    }

    fn overwrite_metadata(&mut self, id: usize, metadata: &RowMetadata) -> std::io::Result<usize> {
        self.rows[id][0] = metadata.encode();
        Ok(1)
    }

    fn read(&self, id: usize) -> std::io::Result<(Row, RowMetadata)> {
        Ok(Row::decode(&self.rows[id], &self.columns))
    }

    fn read_field(&self, id: usize, column_id: usize) -> std::io::Result<TypedValue> {
        let column = &self.columns[column_id];
        let buffer = self.rows[id][column.offset..(column.offset + column.max_physical_size)].to_vec();
        let field = Field::decode(&column.data_type, &buffer, 0);
        Ok(field.value)
    }

    fn read_range(&self, index: std::ops::Range<usize>) -> std::io::Result<Vec<Row>> {
        let rows = self.rows[index].iter().flat_map(|b| {
            let (row, meta) = Row::decode(b, &self.columns);
            if meta.is_allocated { Some(row) } else { None }
        }).collect();
        Ok(rows)
    }

    fn resize(&mut self, new_size: usize) -> std::io::Result<()> {
        self.rows.resize(new_size, vec![]);
        self.watermark = new_size;
        Ok(())
    }
}
