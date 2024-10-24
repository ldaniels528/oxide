////////////////////////////////////////////////////////////////////
// rows module
////////////////////////////////////////////////////////////////////

use std::collections::HashMap;
use std::fmt::Display;
use std::mem::size_of;
use std::ops::Index;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use shared_lib::cnv_error;

use crate::byte_code_compiler::ByteCodeCompiler;
use crate::codec;
use crate::data_types::DataType;
use crate::expression::Conditions;
use crate::field_metadata::FieldMetadata;
use crate::machine::Machine;
use crate::row_metadata::RowMetadata;
use crate::structure::Structure;
use crate::table_columns::Column;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{Boolean, Null, Undefined};

/// Represents a row of a table structure.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Row {
    id: usize,
    values: Vec<TypedValue>,
}

impl Row {

    ////////////////////////////////////////////////////////////////////
    //      Constructors
    ////////////////////////////////////////////////////////////////////

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
            .map(|c| c.get_max_physical_size()).sum::<usize>()
    }

    /// Decodes the supplied buffer returning a row and its metadata
    pub fn decode(buffer: &Vec<u8>, columns: &Vec<Column>) -> (Self, RowMetadata) {
        // if the buffer is empty, just return an empty row
        if buffer.len() == 0 {
            return (Self::empty(columns), RowMetadata::new(false));
        }
        let metadata = RowMetadata::from_bytes(buffer, 0);
        let id = codec::decode_row_id(buffer, 1);
        let values: Vec<TypedValue> = columns.iter().map(|t| {
            Self::decode_value(&t.get_data_type(), &buffer, t.get_offset())
        }).collect();
        (Self::new(id, values), metadata)
    }

    pub fn decode_value(data_type: &DataType, buffer: &Vec<u8>, offset: usize) -> TypedValue {
        let metadata = FieldMetadata::decode(buffer[offset]);
        if metadata.is_active {
            TypedValue::decode(&data_type, buffer, offset + 1)
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

    pub fn encode_value(
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
        Ok((Self::new(id, values), metadata))
    }

    pub fn from_buffer_to_value(data_type: &DataType, buffer: &mut ByteCodeCompiler, offset: usize) -> std::io::Result<TypedValue> {
        let metadata: FieldMetadata = FieldMetadata::decode(buffer[offset]);
        let value: TypedValue = if metadata.is_active {
            TypedValue::from_buffer(&data_type, buffer)?
        } else { Null };
        Ok(value)
    }

    pub fn from_row_js(columns: &Vec<Column>, form: &RowJs) -> Self {
        let mut values = Vec::new();
        for tc in columns {
            values.push(form.determine_column_value(tc.get_name()));
        }
        Row::new(form.id.unwrap_or(0), values)
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
        buf.extend(codec::encode_row_id(self.id));
        // include the fields
        let bb: Vec<u8> = self.values.iter().zip(phys_columns.iter())
            .flat_map(|(v, c)|
                Self::encode_value(v, &FieldMetadata::new(true), c.get_max_physical_size()))
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

    /// Transforms the row into CSV
    pub fn to_csv(&self) -> String {
        self.get_values().iter()
            .map(|v| v.to_code())
            .collect::<Vec<_>>().join(",")
    }

    /// Transforms the row into JSON
    pub fn to_json(&self, columns: &Vec<Column>) -> String {
        let inside = columns.iter().zip(self.values.iter())
            .map(|(k, v)|
                format!(r#""{}":{}"#, k.get_name(), v.to_code()))
            .collect::<Vec<_>>()
            .join(",");
        format!("{{{}}}", inside)
    }

    /// Transforms the row into a Row model
    pub fn to_row_js(&self, columns: &Vec<Column>) -> RowJs {
        RowJs::new(Some(self.get_id()), self.get_values().iter().zip(columns)
            .map(|(v, c)| FieldJs::new(c.get_name(), v.to_json())).collect())
    }

    pub fn to_string(&self) -> String {
        format!("[{}]", self.values.iter()
            .map(|tv| tv.to_code())
            .collect::<Vec<_>>().join(", "))
    }

    pub fn to_struct(&self, columns: &Vec<Column>) -> Structure {
        Structure::new(columns.to_owned(), self.values.to_owned())
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

// JSON representation of a field
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FieldJs {
    name: String,
    value: Value,
}

impl FieldJs {
    pub fn new(name: &str, value: Value) -> Self {
        Self { name: name.into(), value }
    }

    pub fn get_name(&self) -> &String {
        &self.name
    }

    pub fn get_value(&self) -> Value {
        self.value.to_owned()
    }
}

// JSON representation of a row
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RowJs {
    pub id: Option<usize>,
    pub fields: Vec<FieldJs>,
}

impl RowJs {

    pub fn determine_column_value(&self, name: &str) -> TypedValue {
        for c in &self.fields {
            if *c.get_name() == *name { return TypedValue::from_json(c.get_value()); }
        }
        Undefined
    }

    pub fn from_string(json_string: &str) -> std::io::Result<Self> {
        serde_json::from_str(json_string).map_err(|e| cnv_error!(e))
    }

    pub fn vec_from_string(json_string: &str) -> std::io::Result<Vec<Self>> {
        serde_json::from_str(json_string).map_err(|e| cnv_error!(e))
    }

    pub fn new(id: Option<usize>, fields: Vec<FieldJs>) -> Self { Self { id, fields } }

    pub fn get_id(&self) -> Option<usize> { self.id }

    pub fn get_fields(&self) -> &Vec<FieldJs> { &self.fields }

    pub fn tabulate_body_cells_from_rows(rows: &Vec<RowJs>) -> Vec<Vec<String>> {
        let mut body_cells = Vec::new();
        for row in rows {
            let mut row_vec = Vec::new();
            for field in row.get_fields() {
                row_vec.push(format!(" {} ", field.get_value()))
            }
            body_cells.push(row_vec)
        }
        body_cells
    }

    pub fn tabulate_header_cells(rows: &Vec<RowJs>) -> Vec<Vec<String>> {
        let mut headers = Vec::new();
        for field in rows[0].get_fields() {
            headers.push(format!(" {} ", field.get_name()))
        }

        let mut header_cells = Vec::new();
        header_cells.push(headers);
        header_cells
    }

    pub fn to_json_string(&self) -> String { serde_json::to_string(self).unwrap() }

}

// Unit tests
#[cfg(test)]
mod tests {
    use shared_lib::tabulate_cells;
    use crate::data_types::DataType::{NumberType, StringType};
    use crate::data_types::SizeTypes;
    use crate::number_kind::NumberKind::F64Kind;
    use crate::numbers::NumberValue::*;
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

    #[test]
    fn test_tabulate_cells() {
        let rows = RowJs::vec_from_string(r#"
            [{"fields":[{"name":"symbol","value":"ABC"},{"name":"exchange","value":"AMEX"},
            {"name":"last_sale","value":11.77}],"id":0},{"fields":[{"name":"symbol","value":"BIZ"},
            {"name":"exchange","value":"NYSE"},{"name":"last_sale","value":23.66}],"id":2},
            {"fields":[{"name":"symbol","value":"BOOM"},{"name":"exchange","value":"NASDAQ"},
            {"name":"last_sale","value":56.87}],"id":4}]
        "#).unwrap();
        let header_cells = RowJs::tabulate_header_cells(&rows);
        let body_cells = RowJs::tabulate_body_cells_from_rows(&rows);
        let lines = tabulate_cells(header_cells, body_cells);
        assert_eq!(lines, vec![
            "|-------------------------------|".to_string(),
            "| symbol | exchange | last_sale |".to_string(),
            "|-------------------------------|".to_string(),
            "| \"ABC\"  | \"AMEX\"   | 11.77     |".to_string(),
            "| \"BIZ\"  | \"NYSE\"   | 23.66     |".to_string(),
            "| \"BOOM\" | \"NASDAQ\" | 56.87     |".to_string(),
            "|-------------------------------|".to_string(),
        ])
    }

    #[test]
    fn test_transformations_between_row_and_row_json() {
        // define a row
        let columns = vec![
            Column::new("symbol", StringType(SizeTypes::Fixed(4)), Null, 9),
            Column::new("exchange", StringType(SizeTypes::Fixed(4)), Null, 22),
            Column::new("last_sale", NumberType(F64Kind), Null, 35),
        ];
        let row = Row::new(123, vec![
            StringValue("FOX".into()), StringValue("NYSE".into()), Number(F64Value(37.65)),
        ]);
        // define a row-js
        let fields_js = vec![
            FieldJs::new("symbol", serde_json::json!("FOX")),
            FieldJs::new("exchange", serde_json::json!("NYSE")),
            FieldJs::new("last_sale", serde_json::json!(37.65)),
        ];
        let row_js = RowJs::new(Some(123), fields_js.to_owned());
        // verify the accessors
        assert_eq!(row_js.get_id(), Some(123));
        assert_eq!(row_js.get_fields(), &vec![
            FieldJs::new("symbol", serde_json::json!("FOX")),
            FieldJs::new("exchange", serde_json::json!("NYSE")),
            FieldJs::new("last_sale", serde_json::json!(37.65)),
        ]);
        assert_eq!(row_js, RowJs::new(Some(123), fields_js));
        // verify the value extraction
        assert_eq!(row_js.determine_column_value("symbol"), StringValue("FOX".into()));
        assert_eq!(row_js.determine_column_value("exchange"), StringValue("NYSE".into()));
        assert_eq!(row_js.determine_column_value("last_sale"), Number(F64Value(37.65)));
        // cross-convert and verify
        assert_eq!(row.to_row_js(&columns), row_js.to_owned());
        assert_eq!(Row::from_row_js(&columns, &row_js), row);
    }

}