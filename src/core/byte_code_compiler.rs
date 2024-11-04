////////////////////////////////////////////////////////////////////
// ByteCodeCompiler - responsible for assembling and disassembling
//                    expressions to and from byte code.
////////////////////////////////////////////////////////////////////

use std::ops::Index;

use crate::backdoor::BackDoorKey;
use crate::codec;
use crate::codec::Codec;
use crate::expression::*;
use crate::model_row_collection::ModelRowCollection;
use crate::parameter::Parameter;
use crate::rows::Row;
use crate::structures::HardStructure;
use crate::table_columns::Column;
use crate::typed_values::*;

/// A JVM-inspired ByteBuffer-like utility (Big Endian)
pub struct ByteCodeCompiler {
    buf: Vec<u8>,
    limit: usize,
    offset: usize,
}

impl ByteCodeCompiler {

    ////////////////////////////////////////////////////////////////
    //      Static Methods
    ////////////////////////////////////////////////////////////////

    /// creates a new [ByteCodeCompiler]
    pub fn new(size: usize) -> Self {
        ByteCodeCompiler { offset: 0, limit: 0, buf: vec![0u8; size] }
    }

    /// creates a new ready-to-be-written [ByteCodeCompiler] by wrapping an existing vector
    pub fn from_bytes(buf: &Vec<u8>, capacity: usize) -> Self {
        let mut new_buf = Vec::with_capacity(capacity);
        new_buf.extend(buf);
        ByteCodeCompiler { offset: new_buf.len(), limit: 0, buf: new_buf }
    }

    /// creates a new ready-to-be-read [ByteCodeCompiler] by wrapping an existing vector
    pub fn wrap(buf: Vec<u8>) -> Self {
        ByteCodeCompiler { offset: 0, limit: buf.len(), buf }
    }

    ////////////////////////////////////////////////////////////////
    //      Instance Methods
    ////////////////////////////////////////////////////////////////

    pub fn flip(&mut self) {
        self.limit = self.offset;
        self.offset = 0;
    }

    pub fn has_more(&self, number: usize) -> bool { self.offset + number < self.limit }

    pub fn has_next(&self) -> bool { self.offset < self.limit }

    pub fn len(&self) -> usize { self.buf.len() }

    pub fn limit(&self) -> usize { self.limit }

    pub fn move_rel(&mut self, delta: isize) {
        match self.offset as isize + delta {
            n if n >= 0 => self.offset = n as usize,
            _ => self.offset = 0
        }
    }

    /// returns a 64-bit typed-value array
    pub fn next_array(&mut self) -> std::io::Result<Vec<TypedValue>> {
        let length = self.next_u32();
        let mut array = Vec::new();
        for _ in 0..length {
            array.push(self.next_value()?);
        }
        Ok(array)
    }

    pub fn next_backdoor_fn(&mut self) -> BackDoorKey {
        BackDoorKey::from_u8(self.next_u8())
    }

    /// returns a 64-bit byte array
    pub fn next_blob(&mut self) -> Vec<u8> {
        let length = self.next_u64() as usize;
        self.next_bytes(length)
    }

    pub fn next_bool(&mut self) -> bool {
        let size = self.validate(1);
        let result = codec::decode_u8(&self.buf, self.offset, |b| b);
        self.offset += size;
        result != 0
    }

    pub fn next_bytes(&mut self, length: usize) -> Vec<u8> {
        let start = self.offset;
        let end = self.offset + self.validate(length);
        let bytes = self.buf[start..end].to_vec();
        self.offset = end;
        bytes
    }

    /// returns a 64-bit character array
    pub fn next_clob(&mut self) -> Vec<char> {
        let length = self.next_u64() as usize;
        let bytes = self.next_bytes(length);
        String::from_utf8(bytes).unwrap().chars().collect()
    }

    pub fn next_f32(&mut self) -> f32 {
        let size = self.validate(4);
        let result = codec::decode_u8x4(&self.buf, self.offset, |buf| f32::from_be_bytes(buf));
        self.offset += size;
        result
    }

    pub fn next_f64(&mut self) -> f64 {
        let size = self.validate(8);
        let result = codec::decode_u8x8(&self.buf, self.offset, |buf| f64::from_be_bytes(buf));
        self.offset += size;
        result
    }

    pub fn next_i8(&mut self) -> i8 {
        let size = self.validate(1);
        let result = codec::decode_u8(&self.buf, self.offset, |b| b as i8);
        self.offset += size;
        result
    }

    pub fn next_i16(&mut self) -> i16 {
        let size = self.validate(2);
        let result = codec::decode_u8x2(&self.buf, self.offset, |buf| i16::from_be_bytes(buf));
        self.offset += size;
        result
    }

    pub fn next_i32(&mut self) -> i32 {
        let size = self.validate(4);
        let result = codec::decode_u8x4(&self.buf, self.offset, |buf| i32::from_be_bytes(buf));
        self.offset += size;
        result
    }

    pub fn next_i64(&mut self) -> i64 {
        let size = self.validate(8);
        let result = codec::decode_u8x8(&self.buf, self.offset, |buf| i64::from_be_bytes(buf));
        self.offset += size;
        result
    }

    pub fn next_i128(&mut self) -> i128 {
        let size = self.validate(16);
        let result = codec::decode_u8x16(&self.buf, self.offset, |buf| i128::from_be_bytes(buf));
        self.offset += size;
        result
    }

    pub fn next_parameter(&mut self) -> Parameter {
        let name = self.next_string();
        let param_type = match self.next_string() {
            s if s.is_empty() => None,
            s => Some(s)
        };
        let default_value = self.next_string_opt();
        Parameter::new(name, param_type, default_value)
    }

    pub fn next_parameters(&mut self) -> Vec<Parameter> {
        let length = self.next_u16();
        let mut parameters = Vec::new();
        for _ in 0..length {
            parameters.push(self.next_parameter());
        }
        parameters
    }

    pub fn disassemble(buf: &mut ByteCodeCompiler) -> std::io::Result<Expression> {
        let bytes = buf.to_array();
        Ok(Codec::decode(&bytes))
    }

    pub fn next_json(&mut self) -> std::io::Result<Vec<(String, TypedValue)>> {
        let length = self.next_u64();
        let mut list = Vec::new();
        for _ in 0..length {
            let name = self.next_string();
            let value = self.next_value()?;
            list.push((name, value));
        }
        Ok(list)
    }

    pub fn next_row_id(&mut self) -> usize {
        let size = self.validate(4);
        let result = codec::decode_u8x4(&self.buf, self.offset, |buf| u32::from_be_bytes(buf));
        self.offset += size;
        result as usize
    }

    pub fn next_rows_with_columns(
        &mut self,
        parameters: &Vec<Parameter>,
    ) -> std::io::Result<Vec<Row>> {
        let columns = Column::from_parameters(parameters)?;
        let n_rows = self.next_u64();
        let mut rows = Vec::new();
        for _ in 0..n_rows {
            let (row, rmd) = Row::from_buffer(&columns, self)?;
            if rmd.is_allocated {
                rows.push(row)
            }
        }
        Ok(rows)
    }

    /// returns a 64-bit character string
    pub fn next_string(&mut self) -> String {
        let length = self.next_u64();
        let bytes = self.next_bytes(length as usize);
        String::from_utf8(bytes).unwrap()
    }

    pub fn next_string_opt(&mut self) -> Option<String> {
        match self.next_u64() as usize {
            0 => None,
            n => String::from_utf8(self.next_bytes(n)).ok()
        }
    }

    pub fn next_struct_with_parameters(
        &mut self,
        parameters: &Vec<Parameter>,
    ) -> std::io::Result<HardStructure> {
        let fields = Column::from_parameters(parameters)?;
        Ok(HardStructure::new(fields, self.next_array()?))
    }

    pub fn next_table_with_columns(
        &mut self,
        parameters: &Vec<Parameter>,
    ) -> std::io::Result<ModelRowCollection> {
        let phys_columns = Column::from_parameters(&parameters)?;
        let rows = self.next_rows_with_columns(parameters)?;
        Ok(ModelRowCollection::from_rows(phys_columns, rows))
    }

    pub fn next_u8(&mut self) -> u8 {
        let size = self.validate(1);
        let result = codec::decode_u8(&self.buf, self.offset, |b| b);
        self.offset += size;
        result
    }

    pub fn next_u16(&mut self) -> u16 {
        let size = self.validate(2);
        let result = codec::decode_u8x2(&self.buf, self.offset, |buf| u16::from_be_bytes(buf));
        self.offset += size;
        result
    }

    pub fn next_u32(&mut self) -> u32 {
        let size = self.validate(4);
        let result = codec::decode_u8x4(&self.buf, self.offset, |buf| u32::from_be_bytes(buf));
        self.offset += size;
        result
    }

    pub fn next_u64(&mut self) -> u64 {
        let size = self.validate(8);
        let result = codec::decode_u8x8(&self.buf, self.offset, |buf| u64::from_be_bytes(buf));
        self.offset += size;
        result
    }

    pub fn next_u128(&mut self) -> u128 {
        let size = self.validate(16);
        let result = codec::decode_u8x16(&self.buf, self.offset, |buf| u128::from_be_bytes(buf));
        self.offset += size;
        result
    }

    pub fn next_value(&mut self) -> std::io::Result<TypedValue> {
        let bytes = self.buf[self.offset..].to_vec();
        Ok(Codec::decode_value(&bytes))
    }

    pub fn position(&self) -> usize { self.offset }

    pub fn put_bytes(&mut self, bytes: &Vec<u8>) -> &Self {
        let required = self.offset + bytes.len();
        assert!(required <= self.buf.capacity());
        let mut pos = self.offset;
        if required > self.buf.len() { self.buf.resize(self.offset + bytes.len(), 0u8); }
        for byte in bytes {
            self.buf[pos] = *byte;
            pos += 1;
        }
        self.offset = pos;
        self
    }

    pub fn put_column(&mut self, column: &Parameter) -> &Self {
        self.put_string(column.get_name());
        self.put_string(column.get_param_type().unwrap_or("".to_string()).as_str());
        self.put_string_opt(column.get_default_value());
        self
    }

    pub fn put_parameters(&mut self, parameters: &Vec<Parameter>) -> &Self {
        self.put_u16(parameters.len() as u16);
        for column in parameters {
            self.put_column(&column);
        }
        self
    }

    pub fn put_f32(&mut self, value: f32) -> &Self {
        let bytes = value.to_be_bytes();
        self.put_bytes(&bytes.to_vec())
    }

    pub fn put_f64(&mut self, value: f64) -> &Self {
        let bytes = value.to_be_bytes();
        self.put_bytes(&bytes.to_vec())
    }

    pub fn put_i8(&mut self, value: i8) -> &Self {
        self.buf[self.offset] = value as u8;
        self.offset += 1;
        self
    }

    pub fn put_i16(&mut self, value: i16) -> &Self {
        let bytes = value.to_be_bytes();
        self.put_bytes(&bytes.to_vec())
    }

    pub fn put_i32(&mut self, value: i32) -> &Self {
        let bytes = value.to_be_bytes();
        self.put_bytes(&bytes.to_vec())
    }

    pub fn put_i64(&mut self, value: i64) -> &Self {
        let bytes = value.to_be_bytes();
        self.put_bytes(&bytes.to_vec())
    }

    pub fn put_i128(&mut self, value: i128) -> &Self {
        let bytes = value.to_be_bytes();
        self.put_bytes(&bytes.to_vec())
    }

    pub fn put_string(&mut self, string: &str) -> &Self {
        let bytes: Vec<u8> = string.bytes().collect();
        self.put_u64(bytes.len() as u64);
        self.put_bytes(&bytes);
        self
    }

    pub fn put_string_opt(&mut self, string: &Option<String>) -> &Self {
        let bytes: Vec<u8> = string.to_owned().map(|s| s.bytes().collect()).unwrap_or(Vec::new());
        self.put_u64(bytes.len() as u64);
        self.put_bytes(&bytes);
        self
    }

    pub fn put_u8(&mut self, value: u8) -> &Self {
        self.buf[self.offset] = value;
        self.offset += 1;
        self
    }

    pub fn put_u16(&mut self, value: u16) -> &Self {
        let bytes = value.to_be_bytes();
        self.put_bytes(&bytes.to_vec())
    }

    pub fn put_u32(&mut self, value: u32) -> &Self {
        let bytes = value.to_be_bytes();
        self.put_bytes(&bytes.to_vec())
    }

    pub fn put_u64(&mut self, value: u64) -> &Self {
        let bytes = value.to_be_bytes();
        self.put_bytes(&bytes.to_vec())
    }

    pub fn put_u128(&mut self, value: u128) -> &Self {
        let bytes = value.to_be_bytes();
        self.put_bytes(&bytes.to_vec())
    }

    pub fn remaining(&self) -> usize {
        self.limit - self.offset
    }

    /// changes the capacity to the buffer
    pub fn resize(&mut self, new_size: usize) {
        self.buf.resize(new_size, 0u8);
    }

    /// returns a vector contains all bytes from the current position until the end of the buffer
    pub fn to_array(&self) -> Vec<u8> {
        self.buf[self.offset..self.limit].to_vec()
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.buf[self.offset..self.limit].to_vec()
    }

    fn validate(&self, delta: usize) -> usize {
        let new_offset = self.offset + delta;
        if new_offset > self.limit {
            panic!("Buffer underflow: {} + {} ({}) > {}", self.offset, delta, new_offset, self.limit)
        }
        delta
    }
}

impl Index<usize> for ByteCodeCompiler {
    type Output = u8;

    fn index(&self, index: usize) -> &Self::Output {
        &self.buf[index]
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::testdata::make_quote_parameters;

    use super::*;

    #[test]
    fn test_parameters() {
        let mut buffer = ByteCodeCompiler::new(512);
        buffer.put_parameters(&make_quote_parameters());
        assert_eq!(buffer.position(), 118);

        buffer.flip();
        assert_eq!(buffer.next_parameters(), vec![
            Parameter::new("symbol", Some("String(8)".into()), None),
            Parameter::new("exchange", Some("String(8)".into()), None),
            Parameter::new("last_sale", Some("f64".into()), None),
        ])
    }

    #[test]
    fn test_len() {
        let buffer = ByteCodeCompiler::wrap(vec![
            0xBA, 0xBE, 0xFA, 0xCE, 0xD0, 0xCA, 0xFE, 0x00,
        ]);
        assert_eq!(buffer.len(), 8)
    }

    #[test]
    fn test_next_f32() {
        let mut buffer = ByteCodeCompiler::wrap(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(buffer.limit(), 4);
        assert_eq!(buffer.next_f32(), -6.2598534e18)
    }

    #[test]
    fn test_next_f64() {
        let mut buffer = ByteCodeCompiler::wrap(vec![
            0xBA, 0xBE, 0xFA, 0xCE, 0xD0, 0xCA, 0xFE, 0x00,
        ]);
        assert_eq!(buffer.next_f64(), -1.0010086845065389e-25)
    }

    #[test]
    fn test_next_i8() {
        let mut buffer = ByteCodeCompiler::wrap(vec![0xAA]);
        assert_eq!(buffer.next_i8(), -86)
    }

    #[test]
    fn test_next_i16() {
        let mut buffer = ByteCodeCompiler::wrap(vec![0xBE, 0xEF]);
        assert_eq!(buffer.next_i16(), -16657)
    }

    #[test]
    fn test_next_i32() {
        let mut buffer = ByteCodeCompiler::wrap(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(buffer.next_i32(), -559038737)
    }

    #[test]
    fn test_next_i64() {
        let mut buffer = ByteCodeCompiler::wrap(vec![
            0xBA, 0xBE, 0xFA, 0xCE, 0xD0, 0xCA, 0xFE, 0x00,
        ]);
        assert_eq!(buffer.next_i64(), -4990275570906759680)
    }

    #[test]
    fn test_next_i128() {
        let mut buffer = ByteCodeCompiler::wrap(vec![
            0xBA, 0xBE, 0xFA, 0xCE, 0xBA, 0xDD, 0xEE, 0x00,
            0xCA, 0xFE, 0xDE, 0xAD, 0xBE, 0xEF, 0xBE, 0xAD,
        ]);
        assert_eq!(buffer.next_i128(), -92054336320587594627030856550656786771i128)
    }

    #[test]
    fn test_next_row_id() {
        let mut buffer = ByteCodeCompiler::wrap(vec![0x00, 0x00, 0x00, 0x07]);
        assert_eq!(buffer.next_row_id(), 7)
    }

    #[test]
    fn test_next_u8() {
        let mut buffer = ByteCodeCompiler::wrap(vec![0xAA]);
        assert_eq!(buffer.next_u8(), 0xAA)
    }

    #[test]
    fn test_next_u16() {
        let mut buffer = ByteCodeCompiler::wrap(vec![0xBE, 0xEF]);
        assert_eq!(buffer.next_u16(), 0xBEEF)
    }

    #[test]
    fn test_next_u32() {
        let mut buffer = ByteCodeCompiler::wrap(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(buffer.next_u32(), 0xDEADBEEF)
    }

    #[test]
    fn test_next_u64() {
        let mut buffer = ByteCodeCompiler::wrap(vec![
            0xBA, 0xBE, 0xFA, 0xCE, 0xD0, 0xCA, 0xFE, 0x00,
        ]);
        assert_eq!(buffer.next_u64(), 0xBABE_FACED_0_CAFE_00)
    }

    #[test]
    fn test_next_u128() {
        let bytes = vec![
            0xBA, 0xBE, 0xFA, 0xCE, 0xBA, 0xDD, 0xEE, 0x00,
            0xCA, 0xFE, 0xDE, 0xAD, 0xBE, 0xEF, 0xBE, 0xAD,
        ];
        let mut buffer = ByteCodeCompiler::from_bytes(&bytes, bytes.len());
        buffer.flip();
        assert_eq!(buffer.next_u128(), 0xBABE_FACE_BADDEE_00_CAFE_DEAD_BEEF_BEAD)
    }

    #[test]
    fn test_put_f32() {
        let mut buffer = ByteCodeCompiler::new(4);
        buffer.put_f32(125.0);
        buffer.flip();
        assert_eq!(buffer.next_f32(), 125.0);
    }

    #[test]
    fn test_put_f64() {
        let mut buffer = ByteCodeCompiler::new(8);
        buffer.put_f64(4_000_000.0);
        buffer.flip();
        assert_eq!(buffer.next_f64(), 4_000_000.0);
    }

    #[test]
    fn test_put_i8() {
        let mut buffer = ByteCodeCompiler::new(1);
        buffer.put_i8(99);
        buffer.flip();
        assert_eq!(buffer.next_i8(), 99);
    }

    #[test]
    fn test_put_i16() {
        let mut buffer = ByteCodeCompiler::new(2);
        buffer.put_i16(16235);
        buffer.flip();
        assert_eq!(buffer.next_i16(), 16235);
    }

    #[test]
    fn test_put_i32() {
        let mut buffer = ByteCodeCompiler::new(4);
        buffer.put_i32(125_000);
        buffer.flip();
        assert_eq!(buffer.next_i32(), 125_000);
    }

    #[test]
    fn test_put_i64() {
        let mut buffer = ByteCodeCompiler::new(8);
        buffer.put_i64(4_000_000);
        buffer.flip();
        assert_eq!(buffer.next_i64(), 4_000_000);
    }

    #[test]
    fn test_put_i128() {
        let mut buffer = ByteCodeCompiler::new(16);
        buffer.put_i128(4_000_000_000_000);
        buffer.flip();
        assert_eq!(buffer.next_i128(), 4_000_000_000_000);
    }

    #[test]
    fn test_put_u8() {
        let mut buffer = ByteCodeCompiler::new(8);
        buffer.put_u8(0x88);
        buffer.flip();
        assert_eq!(buffer.next_u8(), 0x88);
    }

    #[test]
    fn test_put_u16() {
        let mut buffer = ByteCodeCompiler::new(8);
        buffer.put_u16(0x8877);
        buffer.flip();
        assert_eq!(buffer.next_u16(), 0x8877);
    }

    #[test]
    fn test_put_u32() {
        let mut buffer = ByteCodeCompiler::new(8);
        buffer.put_u32(0x88776655);
        buffer.flip();
        assert_eq!(buffer.next_u32(), 0x88776655);
    }

    #[test]
    fn test_put_u64() {
        let mut buffer = ByteCodeCompiler::new(8);
        buffer.put_u64(0x88776655_44332211);
        buffer.flip();
        assert_eq!(buffer.next_u64(), 0x88776655_44332211);
    }

    #[test]
    fn test_put_u128() {
        let mut buffer = ByteCodeCompiler::new(16);
        buffer.put_u128(0xBABE_FACE_BADDEE_CAFE_DEAD_BEEF_BEAD_00);
        buffer.flip();
        assert_eq!(buffer.next_u128(), 0xBABE_FACE_BADDEE_CAFE_DEAD_BEEF_BEAD_00);
    }

    #[test]
    fn test_next_string() {
        let mut buffer = ByteCodeCompiler::new(20);
        buffer.put_string("Hello World!");
        buffer.flip();
        assert_eq!(buffer.to_array(), vec![
            0, 0, 0, 0, 0, 0, 0, 12,
            b'H', b'e', b'l', b'l', b'o', b' ', b'W', b'o', b'r', b'l', b'd', b'!',
        ]);
        assert_eq!(buffer.next_string(), "Hello World!");
    }

    #[test]
    fn test_next_string_opt() {
        let mut buffer = ByteCodeCompiler::wrap(vec![
            0, 0, 0, 0, 0, 0, 0, 12,
            b'H', b'e', b'l', b'l', b'o', b' ', b'W', b'o', b'r', b'l', b'd', b'!',
        ]);
        assert_eq!(buffer.next_string_opt(), Some("Hello World!".into()));
    }

    #[test]
    fn test_next_string_opt_is_empty() {
        let mut buffer = ByteCodeCompiler::wrap(vec![
            0, 0, 0, 0, 0, 0, 0, 0,
        ]);
        assert_eq!(buffer.next_string_opt(), None);
    }

    #[test]
    fn test_reading() {
        let mut buffer = ByteCodeCompiler::wrap(vec![
            0xBA, 0xBE, 0xFA, 0xCE, 0xCA, 0xFE, 0xDE, 0xAD,
        ]);
        assert_eq!(buffer.position(), 0);
        assert_eq!(buffer.next_u32(), 0xBABE_FACE);
        assert_eq!(buffer.position(), 4);
        assert_eq!(buffer.next_u16(), 0xCAFE);
        assert_eq!(buffer.position(), 6);
        assert_eq!(buffer.next_u16(), 0xDEAD);
        assert_eq!(buffer.position(), 8);
    }

    #[test]
    fn test_resize() {
        let mut buffer = ByteCodeCompiler::wrap(vec![
            0xBA, 0xBE, 0xFA, 0xCE, 0xD0, 0xCA, 0xFE, 0x00,
        ]);
        buffer.resize(13);
        assert_eq!(buffer.len(), 13)
    }

    #[test]
    fn test_to_array() {
        let mut buffer = ByteCodeCompiler::new(8);
        buffer.put_u64(0xBABE_FACE_CAFE_DEAD);
        buffer.flip();
        buffer.next_u32(); // skip 4 bytes
        assert_eq!(buffer.to_array(), vec![0xCA, 0xFE, 0xDE, 0xAD])
    }

    #[test]
    fn test_writing() {
        let mut buffer = ByteCodeCompiler::from_bytes(&vec![0xBA, 0xBE], 8);
        assert_eq!(buffer.position(), 2);
        buffer.put_u16(0xFACE);
        assert_eq!(buffer.position(), 4);
        buffer.put_u16(0xCAFE);
        assert_eq!(buffer.position(), 6);
        buffer.put_u16(0xDEAD);
        assert_eq!(buffer.position(), 8);
        buffer.flip();
        assert_eq!(buffer.next_u64(), 0xBABE_FACE_CAFE_DEAD)
    }
}