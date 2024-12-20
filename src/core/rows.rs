////////////////////////////////////////////////////////////////////
// rows module
////////////////////////////////////////////////////////////////////

use std::collections::HashMap;
use std::fmt::Display;
use std::mem::size_of;
use std::ops::Index;

use crate::byte_code_compiler::ByteCodeCompiler;
use crate::data_types::DataType;
use crate::expression::Conditions;
use crate::field_metadata::FieldMetadata;
use crate::machine::Machine;
use crate::row_metadata::RowMetadata;
use crate::structures::HardStructure;
use crate::table_columns::Column;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{Boolean, Null, Undefined};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use shared_lib::fail;

/// Represents a row of a table structure.
#[derive(Clone, Debug, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Row {
    id: usize,
    values: Vec<TypedValue>,
}

impl Row {

    ////////////////////////////////////////////////////////////////////
    //      Constructors
    ////////////////////////////////////////////////////////////////////

    pub fn from_json(columns: &Vec<Column>, value: &Value) -> Row {
        let values = columns.iter()
            .map(|column| value.get(column.get_name())
                .map(|value| TypedValue::from_json(value))
                .unwrap_or(Undefined))
            .collect::<Vec<_>>();
        Row::new(0, values)
    }

    /// Primary Constructor
    pub fn new(id: usize, values: Vec<TypedValue>) -> Self {
        Self { id, values }
    }

    ////////////////////////////////////////////////////////////////////
    //      Static Methods
    ////////////////////////////////////////////////////////////////////

    /// Computes the total record size (in bytes)
    pub fn compute_record_size(columns: &Vec<Column>) -> usize {
        Row::overhead() + columns.iter()
            .map(|c| c.get_fixed_size()).sum::<usize>()
    }

    /// Decodes the supplied buffer returning a row and its metadata
    pub fn decode(buffer: &Vec<u8>, columns: &Vec<Column>) -> (Self, RowMetadata) {
        // if the buffer is empty, just return an empty row
        if buffer.len() == 0 {
            return (Self::empty(columns), RowMetadata::new(false));
        }
        let metadata = RowMetadata::from_bytes(buffer, 0);
        let id = ByteCodeCompiler::decode_row_id(buffer, 1);
        let values: Vec<TypedValue> = columns.iter().map(|t| {
            Self::decode_value(&t.get_data_type(), &buffer, t.get_offset())
        }).collect();
        (Self::new(id, values), metadata)
    }

    pub fn decode_value(data_type: &DataType, buffer: &Vec<u8>, offset: usize) -> TypedValue {
        let metadata = FieldMetadata::decode(buffer[offset]);
        if metadata.is_active {
            data_type.decode(buffer, offset + 1)
        } else { Null }
    }

    /// Decodes the supplied buffer returning a collection of rows.
    pub fn decode_rows(columns: &Vec<Column>, row_data: Vec<Vec<u8>>) -> Vec<Self> {
        let mut rows = Vec::new();
        for row_bytes in row_data {
            let (row, metadata) = Self::decode(&row_bytes, &columns);
            if metadata.is_allocated { rows.push(row); }
        }
        rows
    }

    /// Returns an empty row.
    pub fn empty(columns: &Vec<Column>) -> Self {
        Self::new(0, columns.iter().map(|_| Null).collect())
    }

    pub fn encode_cell(
        value: &TypedValue,
        metadata: &FieldMetadata,
        capacity: usize,
    ) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::with_capacity(capacity);
        buf.push(metadata.encode());
        buf.extend(value.encode());
        buf.resize(capacity, 0u8);
        buf
    }

    pub fn from_buffer(
        columns: &Vec<Column>,
        buffer: &mut ByteCodeCompiler,
    ) -> std::io::Result<(Self, RowMetadata)> {
        // if the buffer is empty, just return an empty row
        let size = buffer.next_u64();
        if size == 0 {
            return Ok((Self::empty(columns), RowMetadata::new(false)));
        }
        let metadata = RowMetadata::decode(buffer.next_u8());
        let id = buffer.next_row_id();
        let mut values = Vec::new();
        for col in columns {
            let field = Self::from_buffer_to_value(&col.get_data_type(), buffer, col.get_offset())?;
            values.push(field);
        }
        Ok((Self::new(id as usize, values), metadata))
    }

    pub fn from_buffer_to_value(data_type: &DataType, bcc: &mut ByteCodeCompiler, offset: usize) -> std::io::Result<TypedValue> {
        let metadata: FieldMetadata = FieldMetadata::decode(bcc[offset]);
        let value: TypedValue = if metadata.is_active {
            data_type.decode_bcc(bcc)?
        } else { Null };
        Ok(value)
    }

    pub fn from_tuples(
        id: usize,
        columns: &Vec<Column>,
        tuples: &Vec<(String, TypedValue)>,
    ) -> Self {
        // build a cache of the tuples as a hashmap
        let mut cache = HashMap::new();
        for (name, value) in tuples {
            cache.insert(name.to_string(), value.to_owned());
        }
        // construct the fields
        let mut values = Vec::new();
        for c in columns {
            if let Some(value) = cache.get(c.get_name()) {
                values.push(value.to_owned());
            } else {
                values.push(Undefined)
            }
        }
        Row::new(id, values)
    }

    /// Represents the number of bytes before the start of column data, which includes
    /// the embedded row metadata (1-byte) and row ID (4- or 8-bytes)
    pub fn overhead() -> usize { 1 + size_of::<usize>() }

    ////////////////////////////////////////////////////////////////////
    //      Instance Methods
    ////////////////////////////////////////////////////////////////////

    /// Returns the binary-encoded equivalent of the row.
    pub fn encode(&self, phys_columns: &Vec<Column>) -> Vec<u8> {
        let capacity = Self::compute_record_size(phys_columns);
        let mut buf = Vec::with_capacity(capacity);
        // include the field metadata and row ID
        buf.push(RowMetadata::new(true).encode());
        buf.extend(ByteCodeCompiler::encode_row_id(self.id));
        // include the fields
        let bb: Vec<u8> = self.values.iter().zip(phys_columns.iter())
            .flat_map(|(v, c)| Self::encode_cell(
                v, &FieldMetadata::new(true), c.get_fixed_size()))
            .collect();
        buf.extend(bb);
        buf.resize(capacity, 0u8);
        buf
    }

    pub fn get(&self, index: usize) -> TypedValue {
        let value_len = self.values.len();
        if index < value_len { self.values[index].to_owned() } else { Undefined }
    }

    fn get_row_offset(&self, columns: &Vec<Column>, id: usize) -> u64 {
        (id as u64) * (Self::compute_record_size(columns) as u64)
    }

    pub fn get_values(&self) -> Vec<TypedValue> { self.values.to_owned() }

    pub fn get_id(&self) -> usize { self.id }

    pub fn matches(
        &self,
        machine: &Machine,
        condition: &Option<Conditions>,
        columns: &Vec<Column>,
    ) -> bool {
        if let Some(condition) = condition {
            let machine = machine.with_row(columns, &self);
            match machine.evaluate_cond(condition) {
                Ok((_, Boolean(true) | Null | Undefined)) => true,
                Ok(_) => false,
                Err(..) => false
            }
        } else { true }
    }

    pub fn pollute(&self, ms: &Machine, columns: &Vec<Column>) -> Machine {
        columns.iter().zip(self.values.iter())
            .fold(ms.clone(), |ms, (column, value)|
                ms.with_variable(column.get_name(), value.clone()))
    }

    pub fn replace_undefined_with_null(&self, columns: &Vec<Column>) -> Row {
        Row::new(self.get_id(), columns.iter().zip(self.get_values().iter())
            .map(|(column, value)| {
                match value {
                    Null | Undefined => column.get_default_value().to_owned(),
                    v => v.to_owned()
                }
            }).collect())
    }

    pub fn rows_to_json(columns: &Vec<Column>, rows: &Vec<Row>) -> Vec<HashMap<String, Value>> {
        rows.iter().fold(Vec::new(), |mut vec, row| {
            vec.push(row.to_json(columns));
            vec
        })
    }

    /// Transforms the row into CSV format
    pub fn to_csv(&self) -> String {
        self.get_values().iter()
            .map(|v| v.to_code())
            .collect::<Vec<_>>().join(",")
    }

    /// Transforms the row into JSON format
    pub fn to_json(&self, columns: &Vec<Column>) -> HashMap<String, Value> {
        columns.iter().zip(self.get_values().iter())
            .fold(HashMap::new(), |mut hm, (c, v)| {
                hm.insert(c.get_name().to_string(), v.to_json());
                hm
            })
    }

    /// Transforms the row into JSON
    pub fn to_json_string(&self, columns: &Vec<Column>) -> String {
        let inside = columns.iter().zip(self.values.iter())
            .map(|(k, v)|
                format!(r#""{}":{}"#, k.get_name(), v.to_code()))
            .collect::<Vec<_>>()
            .join(",");
        format!("{{{}}}", inside)
    }

    pub fn to_string(&self) -> String {
        format!("[{}]", self.values.iter()
            .map(|tv| tv.to_code())
            .collect::<Vec<_>>().join(", "))
    }

    pub fn to_struct(&self, columns: &Vec<Column>) -> HardStructure {
        HardStructure::new(columns.to_owned(), self.values.to_owned())
    }

    /// Creates a new [Row] from the supplied fields and values
    pub fn transform(
        &self,
        columns: &Vec<Column>,
        field_names: &Vec<String>,
        field_values: &Vec<TypedValue>,
    ) -> std::io::Result<Row> {
        // field and value vectors must have the same length
        let src_values = self.get_values();
        if field_names.len() != field_values.len() {
            return fail(format!("Data mismatch: fields ({}) vs values ({})", field_names.len(), field_values.len()));
        }
        // build a cache (mapping) of field names to values
        let cache = field_names.iter().zip(field_values.iter())
            .fold(HashMap::new(), |mut m, (k, v)| {
                m.insert(k.to_string(), v.to_owned());
                m
            });
        // build the new fields vector
        let new_field_values = columns.iter().zip(src_values.iter())
            .map(|(column, value)| match cache.get(column.get_name()) {
                Some(Null | Undefined) => value.to_owned(),
                Some(tv) => tv.to_owned(),
                None => value.to_owned()
            })
            .collect::<Vec<TypedValue>>();
        // return the transformed row
        Ok(Row::new(self.get_id(), new_field_values))
    }

    /// Returns a [Vec] containing the values in order of the fields within the row.
    pub fn unwrap(&self) -> Vec<&TypedValue> {
        let mut values = Vec::new();
        for value in &self.values { values.push(value) }
        values
    }

    pub fn with_row_id(&self, id: usize) -> Self {
        Self::new(id, self.values.to_owned())
    }

    pub fn with_values(&self, values: Vec<TypedValue>) -> Self {
        Self::new(self.id, values)
    }
}

impl Display for Row {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

impl Index<usize> for Row {
    type Output = TypedValue;

    fn index(&self, id: usize) -> &Self::Output {
        &self.values[id]
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::numbers::Numbers::*;
    use crate::testdata::{make_quote, make_quote_columns};
    use crate::typed_values::TypedValue::*;

    use super::*;

    #[test]
    fn test_make_quote() {
        let row = make_quote(187, "KING", "YHWH", 78.35);
        assert_eq!(row, Row {
            id: 187,
            values: vec![
                StringValue("KING".into()),
                StringValue("YHWH".into()),
                Number(F64Value(78.35)),
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
        let (row, rmd) = Row::decode(&buf, &make_quote_columns());
        assert!(rmd.is_allocated);
        assert_eq!(row, make_quote(187, "MANA", "NYSE", 78.35));
    }

    #[test]
    fn test_decode_rows() {
        let columns = make_quote_columns();
        let rows_a = vec![
            make_quote(0, "BEAM", "NYSE", 11.99),
            make_quote(1, "LITE", "AMEX", 78.35),
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
        let columns = make_quote_columns();
        let row_a = Row::empty(&columns);
        let row_b = Row::new(0, vec![Null, Null, Null]);
        assert_eq!(row_a, row_b);
    }

    #[test]
    fn test_encode() {
        let phys_columns = make_quote_columns();
        let row = make_quote(255, "RED", "NYSE", 78.35);
        assert_eq!(row.encode(&phys_columns), vec![
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 255,
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 3, b'R', b'E', b'D', 0, 0, 0, 0, 0,
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E', 0, 0, 0, 0,
            0b1000_0000, 64, 83, 150, 102, 102, 102, 102, 102,
        ]);
    }

    #[test]
    fn test_fields_by_index() {
        let row = make_quote(213, "YRU", "OTC", 88.44);
        assert_eq!(row.id, 213);
        assert_eq!(row[0], StringValue("YRU".into()));
        assert_eq!(row[1], StringValue("OTC".into()));
        assert_eq!(row[2], Number(F64Value(88.44)));
    }

    #[test]
    fn test_find_field_by_name() {
        let row = Row::new(111, vec![
            StringValue("GE".into()), StringValue("NYSE".into()), Number(F64Value(48.88)),
        ]);
        assert_eq!(row.get(0), StringValue("GE".into()));
        assert_eq!(row.get(1), StringValue("NYSE".into()));
        assert_eq!(row.get(2), Number(F64Value(48.88)));
        assert_eq!(row.get(3), Undefined);
    }

    #[test]
    fn test_get() {
        let row = Row::new(111, vec![
            StringValue("GE".into()), StringValue("NYSE".into()), Number(F64Value(48.88)),
        ]);
        assert_eq!(row.get(0), StringValue("GE".into()));
        assert_eq!(row.get(1), StringValue("NYSE".into()));
        assert_eq!(row.get(2), Number(F64Value(48.88)));
        assert_eq!(row.get(3), Undefined);
    }

    #[test]
    fn test_to_row_offset() {
        let phys_columns = make_quote_columns();
        let row = Row::new(111, vec![
            StringValue("GE".into()), StringValue("NYSE".into()), Number(F64Value(48.88)),
        ]);
        assert_eq!(row.get_row_offset(&phys_columns, 2), 2 * Row::compute_record_size(&phys_columns) as u64);
    }

    #[test]
    fn test_to_string() {
        let row = make_quote(106, "XRS", "NYSE", 55.44);
        assert_eq!(row.to_string(), r#"["XRS", "NYSE", 55.44]"#.to_string());
    }

    #[test]
    fn test_unwrap() {
        let row = make_quote(100, "ZZZ", "AMEX", 0.9876);
        assert_eq!(row.id, 100);
        assert_eq!(row.unwrap(), vec![
            &StringValue("ZZZ".into()), &StringValue("AMEX".into()), &Number(F64Value(0.9876)),
        ]);
    }
}