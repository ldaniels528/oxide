////////////////////////////////////////////////////////////////////
// byte row-collection module
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

use crate::field_metadata::FieldMetadata;
use crate::row_collection::RowCollection;
use crate::row_metadata::RowMetadata;
use crate::rows::Row;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::Undefined;

/// Byte-vector-based RowCollection implementation
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ByteRowCollection {
    columns: Vec<TableColumn>,
    row_data: Vec<Vec<u8>>,
    record_size: usize,
    watermark: usize,
}

impl ByteRowCollection {
    /// Returns true, if the given item matches a [Row] found within it
    pub fn contains(&self, item: &Row) -> bool { self.index_of(item) != Undefined }

    /// Decodes a byte vector into a [ByteRowCollection]
    pub fn decode(columns: Vec<TableColumn>, bytes: Vec<u8>) -> Self {
        let record_size = Row::compute_record_size(&columns);
        let row_bytes = bytes.chunks(record_size)
            .map(|chunk| chunk.to_vec())
            .collect();
        Self::new(columns, row_bytes)
    }

    /// Encodes the [ByteRowCollection] into a byte vector
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        for row in &self.row_data { bytes.extend(row) }
        bytes
    }

    /// Creates a new [ByteRowCollection] from the specified rows
    pub fn from_rows(columns: Vec<TableColumn>, rows: Vec<Row>) -> Self {
        let mut encoded_rows = Vec::new();
        for row in rows { encoded_rows.push(row.encode(&columns)) }
        Self::new(columns, encoded_rows)
    }

    pub fn get_rows(&self) -> Vec<Row> {
        let mut rows = Vec::new();
        for buf in &self.row_data {
            let (row, rmd) = Row::decode(buf, &self.columns);
            if rmd.is_allocated { rows.push(row) }
        }
        rows
    }

    /// Creates a new [ByteRowCollection] from the specified row data
    pub fn new(columns: Vec<TableColumn>, rows: Vec<Vec<u8>>) -> Self {
        ByteRowCollection {
            record_size: Row::compute_record_size(&columns),
            watermark: rows.len(),
            columns,
            row_data: rows,
        }
    }
}

impl RowCollection for ByteRowCollection {
    fn get_columns(&self) -> &Vec<TableColumn> { &self.columns }

    fn get_record_size(&self) -> usize { self.record_size }

    fn len(&self) -> std::io::Result<usize> { Ok(self.watermark) }

    fn overwrite_field(
        &mut self,
        id: usize,
        column_id: usize,
        new_value: TypedValue,
    ) -> TypedValue {
        let column = &self.columns[column_id];
        let offset = self.convert_rowid_to_offset(id) + column.offset as u64;
        let buffer = Row::encode_value(
            &new_value,
            &FieldMetadata::new(true),
            column.max_physical_size,
        );
        let mut encoded_row = self.row_data[id].to_owned();
        let start = offset as usize;
        let end = start + buffer.len();
        encoded_row[start..end].copy_from_slice(buffer.as_slice());
        self.row_data[id] = encoded_row;
        TypedValue::RowsAffected(1)
    }

    fn overwrite_field_metadata(
        &mut self,
        id: usize,
        column_id: usize,
        metadata: FieldMetadata,
    ) -> TypedValue {
        let column = &self.columns[column_id];
        self.row_data[id][column.offset] = metadata.encode();
        TypedValue::RowsAffected(1)
    }

    fn overwrite_row(&mut self, id: usize, row: Row) -> TypedValue {
        // resize the rows to prevent overflow
        if self.row_data.len() <= id {
            self.row_data.resize(id + 1, Vec::new());
        }

        // set the block, update the watermark
        self.row_data[id] = row.encode(&self.columns);
        if self.watermark <= id {
            self.watermark = id + 1;
        }
        TypedValue::RowsAffected(1)
    }

    fn overwrite_row_metadata(&mut self, id: usize, metadata: RowMetadata) -> TypedValue {
        self.row_data[id][0] = metadata.encode();
        TypedValue::RowsAffected(1)
    }

    fn read_field(&self, id: usize, column_id: usize) -> TypedValue {
        let column = &self.columns[column_id];
        let buffer = self.row_data[id][column.offset..(column.offset + column.max_physical_size)].to_vec();
        Row::decode_value(&column.data_type, &buffer, 0)
    }

    fn read_field_metadata(
        &self,
        id: usize,
        column_id: usize,
    ) -> std::io::Result<FieldMetadata> {
        let column = &self.columns[column_id];
        let code = self.row_data[id][column.offset];
        let meta = FieldMetadata::decode(code);
        Ok(meta)
    }

    fn read_row(&self, id: usize) -> std::io::Result<(Row, RowMetadata)> {
        if id < self.row_data.len() {
            Ok(Row::decode(&self.row_data[id], &self.columns))
        } else {
            Ok((Row::empty(&self.columns), RowMetadata::new(false)))
        }
    }

    fn read_row_metadata(&self, id: usize) -> std::io::Result<RowMetadata> {
        let metadata = if id < self.row_data.len() {
            RowMetadata::decode(self.row_data[id][0])
        } else { RowMetadata::new(false) };
        Ok(metadata)
    }

    fn resize(&mut self, new_size: usize) -> TypedValue {
        self.row_data.resize(new_size, Vec::new());
        self.watermark = new_size;
        TypedValue::Ack
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::byte_row_collection::ByteRowCollection;
    use crate::numbers::NumberValue::U64Value;
    use crate::row_collection::RowCollection;
    use crate::table_columns::TableColumn;
    use crate::testdata::{make_quote, make_table_columns};
    use crate::typed_values::TypedValue::Number;

    #[test]
    fn test_contains() {
        let (brc, phys_columns) = create_data_set();
        let row = make_quote(4, "XYZ", "NYSE", 0.0289);
        assert!(brc.contains(&row));
    }

    #[test]
    fn test_encode_decode() {
        let (brc, phys_columns) = create_data_set();
        let encoded = brc.encode();
        assert_eq!(ByteRowCollection::decode(phys_columns, encoded), brc)
    }

    #[test]
    fn test_get_rows() {
        let (brc, phys_columns) = create_data_set();
        assert_eq!(brc.get_rows(), vec![
            make_quote(0, "ABC", "AMEX", 12.33),
            make_quote(1, "UNO", "OTC", 0.2456),
            make_quote(2, "BIZ", "NYSE", 9.775),
            make_quote(3, "GOTO", "OTC", 0.1442),
            make_quote(4, "XYZ", "NYSE", 0.0289),
        ])
    }

    #[test]
    fn test_index_of() {
        let (brc, phys_columns) = create_data_set();
        let row = make_quote(4, "XYZ", "NYSE", 0.0289);
        assert_eq!(brc.index_of(&row), Number(U64Value(4)));
    }

    fn create_data_set() -> (ByteRowCollection, Vec<TableColumn>) {
        let phys_columns = make_table_columns();
        let brc = ByteRowCollection::from_rows(phys_columns.clone(), vec![
            make_quote(0, "ABC", "AMEX", 12.33),
            make_quote(1, "UNO", "OTC", 0.2456),
            make_quote(2, "BIZ", "NYSE", 9.775),
            make_quote(3, "GOTO", "OTC", 0.1442),
            make_quote(4, "XYZ", "NYSE", 0.0289),
        ]);
        (brc, phys_columns)
    }
}