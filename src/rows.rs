////////////////////////////////////////////////////////////////////
// rows
////////////////////////////////////////////////////////////////////

use std::error::Error;
use std::io::Read;
use std::mem::size_of;

use serde::Serialize;

use crate::codec;
use crate::fields::Field;
use crate::row_metadata::RowMetadata;
use crate::table_columns::TableColumn;

#[derive(Debug, Clone, Serialize)]
pub struct Row {
    pub id: usize,
    pub metadata: RowMetadata,
    pub columns: Vec<TableColumn>,
    pub fields: Vec<Field>,
}

impl Row {
    pub fn decode(id: usize, buffer: &Vec<u8>, columns: Vec<TableColumn>) -> Self {
        let metadata: RowMetadata = RowMetadata::decode(buffer[0]);
        let _id = codec::decode_row_id(buffer, 1);
        let mut offset: usize = 0;
        let mut prev: usize = 1 + size_of::<usize>();
        let fields = columns.iter().map(|t| {
            offset = prev;
            prev += t.max_physical_size();
            Field::decode(&t.data_type, &buffer, offset)
        }).collect();
        Self::new(id, metadata, columns.clone(), fields)
    }

    pub fn encode(&self) -> Vec<u8> {
        let capacity = self.record_size();
        let mut buf: Vec<u8> = Vec::with_capacity(capacity);
        // include the field metadata and row ID
        buf.push(self.metadata.encode());
        buf.extend(codec::encode_row_id(self.id));
        // include the fields
        let bb: Vec<u8> = self.fields.iter().zip(self.columns.iter())
            .flat_map(|(f, t)| f.encode(t.max_physical_size()))
            .collect();
        buf.extend(bb);
        buf.resize(capacity, 0u8);
        buf
    }

    // Constructor
    pub fn new(id: usize, metadata: RowMetadata, columns: Vec<TableColumn>, fields: Vec<Field>) -> Self {
        Self { id, metadata, columns, fields }
    }

    pub fn record_size(&self) -> usize {
        let mut size = 1 + size_of::<usize>(); // includes row metadata byte and row ID
        size += self.columns.iter().map(|c| c.max_physical_size()).sum::<usize>();
        return size;
    }

    pub fn with_row_id(&self, id: usize) -> Row {
        Row {
            id,
            metadata: self.metadata,
            columns: self.columns.clone(),
            fields: self.fields.clone(),
        }
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::data_types::DataType::*;
    use crate::testdata::create_test_row;
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
        let row: Row = Row::decode(187, &buf, vec![
            TableColumn::new("symbol", StringType(4), NullValue),
            TableColumn::new("exchange", StringType(4), NullValue),
            TableColumn::new("lastSale", Float64Type, NullValue),
        ]);
        println!("{:?}", row);
        assert_eq!(row.id, 187);
        assert_eq!(row.metadata.is_allocated, true);
        assert_eq!(row.metadata.is_blob, false);
        assert_eq!(row.metadata.is_encrypted, false);
        assert_eq!(row.metadata.is_replicated, false);
        assert_eq!(row.fields[0].metadata.is_active, true);
        assert_eq!(row.fields[0].metadata.is_compressed, false);
        assert_eq!(row.fields[0].value, StringValue("MANA".to_string()));
        assert_eq!(row.fields[1].metadata.is_active, true);
        assert_eq!(row.fields[1].metadata.is_compressed, false);
        assert_eq!(row.fields[1].value, StringValue("YHWH".to_string()));
        assert_eq!(row.fields[2].metadata.is_active, true);
        assert_eq!(row.fields[2].metadata.is_compressed, false);
        assert_eq!(row.fields[2].value, Float64Value(78.35));
    }

    #[test]
    fn test_encode() {
        let row: Row = create_test_row();
        assert_eq!(row.encode(), vec![
            0b1001_0000, 0, 0, 0, 0, 0, 0, 0, 214,
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 3, b'A', b'M', b'D', 0,
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E',
            0b1000_0000, 64, 83, 150, 102, 102, 102, 102, 102,
        ]);
        assert_eq!(row.record_size(), 44)
    }
}