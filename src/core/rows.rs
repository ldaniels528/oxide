////////////////////////////////////////////////////////////////////
// rows module
////////////////////////////////////////////////////////////////////

use std::collections::HashMap;
use std::mem::size_of;
use std::ops::Index;

use serde::{Deserialize, Serialize};

use crate::codec;
use crate::fields::Field;
use crate::row_metadata::RowMetadata;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::RecordNumber;

/// Represents a row of a table structure.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Row {
    pub(crate) id: usize,
    pub(crate) columns: Vec<TableColumn>,
    pub(crate) fields: Vec<Field>,
}

impl Index<usize> for Row {
    type Output = Field;

    fn index(&self, id: usize) -> &Self::Output {
        &self.fields[id]
    }
}

impl Row {
    // Primary Constructor
    pub fn new(id: usize, columns: Vec<TableColumn>, fields: Vec<Field>) -> Self {
        Self { id, columns, fields }
    }

    /// Decodes the supplied buffer returning a row and its metadata
    pub fn decode(buffer: &Vec<u8>, columns: &Vec<TableColumn>) -> (Self, RowMetadata) {
        // if the buffer is empty, just return an empty row
        if buffer.len() == 0 {
            return (Self::empty(columns), RowMetadata::new(false));
        }
        let metadata = RowMetadata::from_bytes(buffer, 0);
        let id = codec::decode_row_id(buffer, 1);
        let fields: Vec<Field> = columns.iter().map(|t| {
            Field::decode(&t.data_type, &buffer, t.offset)
        }).collect();
        (Self::new(id, columns.clone(), fields), metadata)
    }

    /// Decodes the supplied buffer returning a collection of rows.
    pub fn decode_rows(columns: &Vec<TableColumn>, row_data: Vec<Vec<u8>>) -> Vec<Self> {
        let mut rows = Vec::new();
        for row_bytes in row_data {
            let (row, metadata) = Self::decode(&row_bytes, &columns);
            if metadata.is_allocated {
                rows.push(row);
            }
        }
        rows
    }

    /// Returns an empty row.
    pub fn empty(columns: &Vec<TableColumn>) -> Self {
        Self::new(0, columns.clone(), vec![])
    }

    /// Returns the binary-encoded equivalent of the row.
    pub fn encode(&self) -> Vec<u8> {
        let capacity = self.record_size();
        let mut buf = Vec::with_capacity(capacity);
        // include the field metadata and row ID
        buf.push(RowMetadata::new(true).encode());
        buf.extend(codec::encode_row_id(self.id));
        // include the fields
        let bb: Vec<u8> = self.fields.iter().zip(self.columns.iter())
            .flat_map(|(f, t)| f.encode(t.max_physical_size))
            .collect();
        buf.extend(bb);
        buf.resize(capacity, 0u8);
        buf
    }

    pub fn find_field_by_name(&self, name: &str) -> Option<TypedValue> {
        self.columns.iter().zip(self.fields.iter())
            .find_map(|(c, f)| {
                if c.name == name { Some(f.value.clone()) } else { None }
            })
    }

    /// Represents the number of bytes before the start of column data, which includes
    /// the embedded row metadata (1-byte) and row ID (4- or 8-bytes)
    pub fn overhead() -> usize {
        1 + size_of::<usize>()
    }

    pub fn record_size(&self) -> usize {
        Self::overhead() + self.columns.iter().map(|c| c.max_physical_size).sum::<usize>()
    }

    /// Returns a [HashMap] containing name-values pairs that represent the row's internal state.
    pub fn to_hash_map(&self) -> HashMap<String, TypedValue> {
        let mut mapping = HashMap::new();
        mapping.insert("_id".into(), RecordNumber(self.id));
        for (field, column) in self.fields.iter().zip(&self.columns) {
            mapping.insert(column.name.to_string(), field.value.clone());
        }
        mapping
    }

    /// Returns a [Vec] containing the values in order of the fields within the row.
    pub fn unwrap(&self) -> Vec<&TypedValue> {
        let mut values = vec![];
        for field in &self.fields {
            values.push(&field.value);
        }
        values
    }

    pub fn with_row_id(&self, id: usize) -> Self {
        Self::new(id, self.columns.clone(), self.fields.clone())
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::data_types::DataType::*;
    use crate::testdata::{make_quote, make_table_columns};
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
        let (row, rmd) = Row::decode(&buf, &make_table_columns());
        assert!(rmd.is_allocated);
        assert_eq!(row, Row {
            id: 187,
            columns: vec![
                TableColumn::new("symbol", StringType(4), Null, 9),
                TableColumn::new("exchange", StringType(4), Null, 22),
                TableColumn::new("lastSale", Float64Type, Null, 35),
            ],
            fields: vec![
                Field::new(StringValue("MANA".into())),
                Field::new(StringValue("YHWH".into())),
                Field::new(Float64Value(78.35)),
            ],
        });
    }

    #[test]
    fn test_encode() {
        let row = make_quote(255, &make_table_columns(), "RED", "NYSE", 78.35);
        assert_eq!(row.encode(), vec![
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 255,
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 3, b'R', b'E', b'D', 0,
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E',
            0b1000_0000, 64, 83, 150, 102, 102, 102, 102, 102,
        ]);
    }

    #[test]
    fn test_fields_by_index() {
        let row = make_quote(213, &make_table_columns(), "YRU", "OTC", 88.44);
        assert_eq!(row.id, 213);
        assert_eq!(row[0].value, StringValue("YRU".into()));
        assert_eq!(row[1].value, StringValue("OTC".into()));
        assert_eq!(row[2].value, Float64Value(88.44));
    }

    #[test]
    fn test_to_hash_map() {
        use maplit::hashmap;
        let row = make_quote(111, &make_table_columns(), "AAA", "TCE", 1230.78);
        assert_eq!(row.to_hash_map(), hashmap!(
            "_id".into() => RecordNumber(111),
            "symbol".into() => StringValue("AAA".into()),
            "exchange".into() => StringValue("TCE".into()),
            "lastSale".into() => Float64Value(1230.78),
        ));
    }

    #[test]
    fn test_unwrap() {
        let row = make_quote(100, &make_table_columns(), "ZZZ", "AMEX", 0.9876);
        assert_eq!(row.id, 100);
        assert_eq!(row.unwrap(), vec![
            &StringValue("ZZZ".into()), &StringValue("AMEX".into()), &Float64Value(0.9876),
        ]);
    }
}