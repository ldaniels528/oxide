////////////////////////////////////////////////////////////////////
// rows module
////////////////////////////////////////////////////////////////////

use std::collections::HashMap;
use std::fmt::Display;
use std::mem::size_of;
use std::ops::Index;

use serde::{Deserialize, Serialize};

use shared_lib::{FieldJs, RowJs};

use crate::byte_buffer::ByteBuffer;
use crate::codec;
use crate::expression::Expression;
use crate::fields::Field;
use crate::machine::MachineState;
use crate::row_metadata::RowMetadata;
use crate::server::determine_column_value;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue;

/// Represents a row of a table structure.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Row {
    id: usize,
    columns: Vec<TableColumn>,
    fields: Vec<Field>,
}

impl Row {

    ////////////////////////////////////////////////////////////////////
    //      Constructors
    ////////////////////////////////////////////////////////////////////

    /// Primary Constructor
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
            if metadata.is_allocated { rows.push(row); }
        }
        rows
    }

    /// Returns an empty row.
    pub fn empty(columns: &Vec<TableColumn>) -> Self {
        Self::new(0, columns.clone(), columns.iter().map(|_| Field::new(TypedValue::Null)).collect())
    }

    pub fn from_buffer(
        columns: &Vec<TableColumn>,
        buffer: &mut ByteBuffer,
    ) -> std::io::Result<(Self, RowMetadata)> {
        // if the buffer is empty, just return an empty row
        let size = buffer.next_u64();
        if size == 0 {
            return Ok((Self::empty(columns), RowMetadata::new(false)));
        }
        let metadata = RowMetadata::decode(buffer.next_u8());
        let id = buffer.next_row_id();
        let mut fields: Vec<Field> = vec![];
        for col in columns {
            let field = Field::from_buffer(&col.data_type, buffer, col.offset)?;
            fields.push(field);
        }
        Ok((Self::new(id, columns.clone(), fields), metadata))
    }

    pub fn from_row_js(columns: &Vec<TableColumn>, form: &RowJs) -> Self {
        let mut fields = vec![];
        for tc in columns {
            fields.push(Field::with_value(determine_column_value(form, tc.get_name())));
        }
        Row::new(form.id.unwrap_or(0), columns.clone(), fields)
    }

    pub fn from_tuples(
        id: usize,
        columns: &Vec<TableColumn>,
        tuples: &Vec<(String, TypedValue)>,
    ) -> Self {
        // build a cache of the tuples as a hashmap
        let mut cache = HashMap::new();
        for (name, value) in tuples {
            cache.insert(name.to_string(), value.clone());
        }
        // construct the fields
        let mut fields = vec![];
        for c in columns {
            if let Some(value) = cache.get(c.get_name()) {
                fields.push(Field::with_value(value.clone()));
            } else {
                fields.push(Field::with_value(TypedValue::Undefined))
            }
        }
        Row::new(id, columns.clone(), fields)
    }

    ////////////////////////////////////////////////////////////////////
    //      Utilities
    ////////////////////////////////////////////////////////////////////

    /// Computes the total record size (in bytes)
    pub fn compute_record_size(columns: &Vec<TableColumn>) -> usize {
        Row::overhead() + columns.iter().map(|c| c.max_physical_size).sum::<usize>()
    }

    /// Returns the binary-encoded equivalent of the row.
    pub fn encode(&self) -> Vec<u8> {
        let capacity = self.get_record_size();
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
                if c.get_name() == name { Some(f.value.clone()) } else { None }
            })
    }

    pub fn get(&self, name: &str) -> TypedValue {
        self.find_field_by_name(name).unwrap_or(TypedValue::Undefined)
    }

    pub fn get_columns(&self) -> &Vec<TableColumn> { &self.columns }

    pub fn get_fields(&self) -> &Vec<Field> { &self.fields }

    pub fn get_id(&self) -> usize { self.id }

    /// returns the total record size (in bytes)
    pub fn get_record_size(&self) -> usize { Self::compute_record_size(&self.columns) }

    pub fn matches(&self, ms: &MachineState, condition: &Option<Box<Expression>>) -> bool {
        if let Some(condition) = condition {
            let ms = ms.with_row(self);
            if let Ok((_, TypedValue::Boolean(v))) = ms.evaluate(condition) { v } else { true }
        } else { true }
    }

    /// Represents the number of bytes before the start of column data, which includes
    /// the embedded row metadata (1-byte) and row ID (4- or 8-bytes)
    pub fn overhead() -> usize { 1 + size_of::<usize>() }

    /// Returns a [HashMap] containing name-values pairs that represent the row's internal state.
    pub fn to_hash_map(&self) -> HashMap<String, TypedValue> {
        let mut mapping = HashMap::new();
        mapping.insert("_id".into(), TypedValue::RowsAffected(self.id));
        for (field, column) in self.fields.iter().zip(&self.columns) {
            mapping.insert(column.get_name().to_string(), field.value.clone());
        }
        mapping
    }

    pub fn to_row_js(&self) -> shared_lib::RowJs {
        shared_lib::RowJs::new(Some(self.get_id()), self.get_fields().iter().zip(self.get_columns())
            .map(|(f, c)| FieldJs::new(c.get_name(), f.value.to_json())).collect())
    }

    fn to_row_offset(&self, id: usize) -> u64 { (id as u64) * (Self::compute_record_size(&self.columns) as u64) }

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

impl Display for Row {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let data = self.columns.iter().zip(self.fields.iter())
            .map(|(c, f)| format!("{}: {}", c.get_name(), f.value.to_json().to_string()))
            .collect::<Vec<String>>().join(", ");
        write!(f, "Row {{ {} }}", data)
    }
}

impl Index<usize> for Row {
    type Output = Field;

    fn index(&self, id: usize) -> &Self::Output {
        &self.fields[id]
    }
}

#[macro_export]
macro_rules! row {
    ($id:expr, $columns:expr, $values:expr) => {
        crate::rows::Row::new($id, $columns.clone(), $values.iter()
            .map(|v| crate::fields::Field::new(v.clone())).collect())
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
    fn test_make_quote() {
        let row = make_quote(187, &make_table_columns(), "KING", "YHWH", 78.35);
        assert_eq!(row, Row {
            id: 187,
            columns: vec![
                TableColumn::new("symbol", StringType(8), Null, 9),
                TableColumn::new("exchange", StringType(8), Null, 26),
                TableColumn::new("last_sale", Float64Type, Null, 43),
            ],
            fields: vec![
                Field::new(StringValue("KING".into())),
                Field::new(StringValue("YHWH".into())),
                Field::new(Float64Value(78.35)),
            ],
        });
    }

    #[test]
    fn test_decode() {
        let buf: Vec<u8> = vec![
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 187,
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'M', b'A', b'N', b'A', 0, 0, 0, 0,
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E', 0, 0, 0, 0,
            0b1000_0000, 64, 83, 150, 102, 102, 102, 102, 102,
        ];
        let (row, rmd) = Row::decode(&buf, &make_table_columns());
        assert!(rmd.is_allocated);
        assert_eq!(row, make_quote(187, &make_table_columns(), "MANA", "NYSE", 78.35));
    }

    #[test]
    fn test_decode_rows() {
        let columns = make_table_columns();
        let rows_a = vec![
            make_quote(0, &make_table_columns(), "BEAM", "NYSE", 11.99),
            make_quote(1, &make_table_columns(), "LITE", "AMEX", 78.35),
        ];
        let rows_b = Row::decode_rows(&columns, vec![vec![
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 0,
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'B', b'E', b'A', b'M', 0, 0, 0, 0,
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E', 0, 0, 0, 0,
            0b1000_0000, 64, 39, 250, 225, 71, 174, 20, 123,
        ], vec![
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 1,
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'L', b'I', b'T', b'E', 0, 0, 0, 0,
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'A', b'M', b'E', b'X', 0, 0, 0, 0,
            0b1000_0000, 64, 83, 150, 102, 102, 102, 102, 102,
        ]]);
        assert_eq!(rows_a, rows_b);
    }

    #[test]
    fn test_empty() {
        let columns = make_table_columns();
        let row_a = Row::empty(&columns);
        let row_b = row!(0, &columns, vec![Null, Null, Null]);
        assert_eq!(row_a, row_b);
    }

    #[test]
    fn test_encode() {
        let row = make_quote(255, &make_table_columns(), "RED", "NYSE", 78.35);
        assert_eq!(row.encode(), vec![
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 255,
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 3, b'R', b'E', b'D', 0, 0, 0, 0, 0,
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E', 0, 0, 0, 0,
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
    fn test_find_field_by_name() {
        let row = row!(111, make_table_columns(), vec![
            StringValue("GE".into()), StringValue("NYSE".into()), Float64Value(48.88),
        ]);
        assert_eq!(row.find_field_by_name("symbol"), Some(StringValue("GE".into())));
        assert_eq!(row.find_field_by_name("exchange"), Some(StringValue("NYSE".into())));
        assert_eq!(row.find_field_by_name("last_sale"), Some(Float64Value(48.88)));
        assert_eq!(row.find_field_by_name("rating"), None);
    }

    #[test]
    fn test_get_by_name() {
        let row = row!(111, make_table_columns(), vec![
            StringValue("GE".into()), StringValue("NYSE".into()), Float64Value(48.88),
        ]);
        assert_eq!(row.get("symbol"), StringValue("GE".into()));
        assert_eq!(row.get("exchange"), StringValue("NYSE".into()));
        assert_eq!(row.get("last_sale"), Float64Value(48.88));
        assert_eq!(row.get("rating"), Undefined);
    }

    #[test]
    fn test_to_hash_map() {
        use maplit::hashmap;
        let row = make_quote(111, &make_table_columns(), "AAA", "TCE", 1230.78);
        assert_eq!(row.to_hash_map(), hashmap!(
            "_id".into() => TypedValue::RowsAffected(111),
            "symbol".into() => TypedValue::StringValue("AAA".into()),
            "exchange".into() => TypedValue::StringValue("TCE".into()),
            "last_sale".into() => TypedValue::Float64Value(1230.78),
        ));
    }

    #[test]
    fn test_to_row_offset() {
        let row = row!(111, make_table_columns(), vec![
            StringValue("GE".into()), StringValue("NYSE".into()), Float64Value(48.88),
        ]);
        assert_eq!(row.to_row_offset(2), 2 * row.get_record_size() as u64);
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