////////////////////////////////////////////////////////////////////
// rows module
////////////////////////////////////////////////////////////////////



use std::mem::size_of;

use serde::Serialize;

use crate::codec;
use crate::fields::Field;
use crate::row_metadata::RowMetadata;
use crate::table_columns::TableColumn;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Row {
    pub(crate) id: usize,
    pub(crate) metadata: RowMetadata,
    pub(crate) columns: Vec<TableColumn>,
    pub(crate) fields: Vec<Field>,
}

impl Row {
    pub fn decode(id: usize, buffer: &Vec<u8>, columns: &Vec<TableColumn>) -> Self {
        // if the buffer is empty, just return an empty row
        if buffer.len() == 0 {
            return Row {
                id,
                metadata: RowMetadata::decode(0x00),
                columns: columns.clone(),
                fields: vec![],
            };
        }
        let metadata: RowMetadata = RowMetadata::from_bytes(buffer, 0);
        let _id = codec::decode_row_id(buffer, 1);
        let fields: Vec<Field> = columns.iter().map(|t| {
            Field::decode(&t.data_type, &buffer, t.offset)
        }).collect();
        Self::new(_id, metadata, columns.clone(), fields)
    }

    pub fn decode_rows(columns: &Vec<TableColumn>, row_data: Vec<Vec<u8>>) -> Vec<Row> {
        let mut rows: Vec<Row> = Vec::new();
        for row_bytes in row_data {
            let id: usize = codec::decode_row_id(&row_bytes, 1);
            let row: Row = Row::decode(id, &row_bytes, &columns);
            if row.metadata.is_allocated {
                rows.push(row);
            }
        }
        rows
    }

    pub fn encode(&self) -> Vec<u8> {
        let capacity = self.record_size();
        let mut buf: Vec<u8> = Vec::with_capacity(capacity);
        // include the field metadata and row ID
        buf.push(self.metadata.encode());
        buf.extend(codec::encode_row_id(self.id));
        // include the fields
        let bb: Vec<u8> = self.fields.iter().zip(self.columns.iter())
            .flat_map(|(f, t)| f.encode(t.max_physical_size))
            .collect();
        buf.extend(bb);
        buf.resize(capacity, 0u8);
        buf
    }

    // Constructor
    pub fn new(id: usize, metadata: RowMetadata, columns: Vec<TableColumn>, fields: Vec<Field>) -> Self {
        Row { id, metadata, columns, fields }
    }

    /// represents the number of bytes before the start of column data, which includes
    /// the embedded row metadata (1-byte) and row ID (4- or 8-bytes)
    pub fn overhead() -> usize {
        1 + size_of::<usize>()
    }

    pub fn record_size(&self) -> usize {
        let mut total = Row::overhead();
        total += self.columns.iter().map(|c| c.max_physical_size).sum::<usize>();
        return total;
    }

    pub fn with_row_id(&self, id: usize) -> Row {
        Self::new(id, self.metadata, self.columns.clone(), self.fields.clone())
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::data_types::DataType::*;
    use crate::field_metadata::FieldMetadata;
    use crate::testdata::{make_row, make_table_columns};
    use crate::typed_values::TypedValue::*;

    use super::*;

    #[test]
    fn test_decode() {
        let buf: Vec<u8> = vec![
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 187,
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'M', b'A', b'N', b'A',
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'Y', b'H', b'W', b'H',
            0b1000_0000, 64, 83, 150, 102, 102, 102, 102, 102,
        ];
        let row: Row = Row::decode(187, &buf, &make_table_columns());
        let fmd: FieldMetadata = FieldMetadata::decode(0x80);
        assert_eq!(row, Row {
            id: 187,
            metadata: RowMetadata::decode(0x80),
            columns: vec![
                TableColumn::new("symbol", StringType(4), NullValue, 9),
                TableColumn::new("exchange", StringType(4), NullValue, 22),
                TableColumn::new("lastSale", Float64Type, NullValue, 35),
            ],
            fields: vec![
                Field::new(fmd.clone(), StringValue("MANA".into())),
                Field::new(fmd.clone(), StringValue("YHWH".into())),
                Field::new(fmd.clone(), Float64Value(78.35)),
            ],
        });
    }

    #[test]
    fn test_encode() {
        let row: Row = make_row(199);
        assert_eq!(row.encode(), vec![
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 199,
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 3, b'A', b'M', b'D', 0,
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E',
            0b1000_0000, 64, 83, 150, 102, 102, 102, 102, 102,
        ]);
    }
}