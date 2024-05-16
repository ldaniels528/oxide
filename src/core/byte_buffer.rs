////////////////////////////////////////////////////////////////////
// byte buffer module - responsible for encoding/decoding bytes
////////////////////////////////////////////////////////////////////

use crate::codec;
use crate::model_row_collection::ModelRowCollection;
use crate::server::ColumnJs;
use crate::typed_values::*;
use crate::typed_values::TypedValue::*;

/// A JVM-inspired Byte Buffer utility (Big Endian)
pub struct ByteBuffer {
    buf: Vec<u8>,
    limit: usize,
    offset: usize,
}

impl ByteBuffer {

    ////////////////////////////////////////////////////////////////
    //      Static Methods
    ////////////////////////////////////////////////////////////////

    /// creates a new [ByteBuffer]
    pub fn new(size: usize) -> Self {
        ByteBuffer { offset: 0, limit: 0, buf: vec![0u8; size] }
    }

    /// creates a new ready-to-be-written [ByteBuffer] by wrapping an existing vector
    pub fn from_bytes(buf: &Vec<u8>, capacity: usize) -> Self {
        let mut new_buf = Vec::with_capacity(capacity);
        new_buf.extend(buf);
        ByteBuffer { offset: new_buf.len(), limit: 0, buf: new_buf }
    }

    /// creates a new ready-to-be-read [ByteBuffer] by wrapping an existing vector
    pub fn wrap(buf: Vec<u8>) -> Self {
        ByteBuffer { offset: 0, limit: buf.len(), buf }
    }

    ////////////////////////////////////////////////////////////////
    //      Instance Methods
    ////////////////////////////////////////////////////////////////

    pub fn move_rel(&mut self, delta: isize) {
        match self.offset as isize + delta {
            n if n >= 0 => self.offset = n as usize,
            _ => self.offset = 0
        }
    }

    pub fn flip(&mut self) {
        self.limit = self.offset;
        self.offset = 0;
    }

    pub fn has_more(&self, number: usize) -> bool { self.offset + number < self.limit }

    pub fn has_next(&self) -> bool { self.offset < self.limit }

    pub fn limit(&self) -> usize { self.limit }

    /// returns a 64-bit typed-value array
    pub fn next_array(&mut self) -> Vec<TypedValue> {
        let length = self.next_u32();
        let mut array = vec![];
        for _ in 0..length {
            array.push(self.next_value());
        }
        array
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

    pub fn next_column(&mut self) -> ColumnJs {
        let name = self.next_string();
        let column_type = self.next_string();
        let default_value = self.next_string_opt();
        ColumnJs::new(name, column_type, default_value)
    }

    pub fn next_columns(&mut self) -> Vec<ColumnJs> {
        let length = self.next_u16();
        let mut columns = vec![];
        for _ in 0..length {
            columns.push(self.next_column());
        }
        columns
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

    pub fn next_json(&mut self) -> Vec<(String, TypedValue)> {
        let length = self.next_u64();
        let mut list = vec![];
        for _ in 0..length {
            let name = self.next_string();
            let value = self.next_value();
            list.push((name, value));
        }
        list
    }

    /// returns a 64-bit character string
    pub fn next_string(&mut self) -> String {
        let length = self.next_u64();
        let bytes = self.next_bytes(length as usize);
        String::from_utf8(bytes).unwrap()
    }

    /// returns a 32-bit string array
    pub fn next_string_array(&mut self) -> Vec<String> {
        let length = self.next_u64();
        let mut array = vec![];
        for _ in 0..length {
            array.push(self.next_string());
        }
        array
    }

    pub fn next_string_opt(&mut self) -> Option<String> {
        match self.next_u32() as usize {
            0 => None,
            n => {
                let bytes = self.next_bytes(n);
                Some(String::from_utf8(bytes).unwrap())
            }
        }
    }

    pub fn next_table(&mut self) -> ModelRowCollection {
        todo!()
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

    pub fn next_uuid(&mut self) -> [u8; 16] {
        let mut uuid = [0u8; 16];
        let bytes = self.next_bytes(16);
        for i in 0..16 {
            uuid[i] = bytes[i]
        }
        uuid
    }

    pub fn next_value(&mut self) -> TypedValue {
        use crate::typed_values::*;
        use TypedValue::*;
        match self.next_u8() {
            V_UNDEFINED => Undefined,
            V_NULL => Null,
            V_BLOB => BLOB(self.next_blob()),
            V_BOOLEAN => Boolean(self.next_bool()),
            V_CLOB => CLOB(self.next_clob()),
            V_DATE => DateValue(self.next_i64()),
            V_FLOAT32 => Float32Value(self.next_f32()),
            V_FLOAT64 => Float64Value(self.next_f64()),
            V_INT8 => Int8Value(self.next_u8()),
            V_INT16 => Int16Value(self.next_i16()),
            V_INT32 => Int32Value(self.next_i32()),
            V_INT64 => Int64Value(self.next_i64()),
            V_RECORD_NUMBER => RecordNumber(self.next_u32() as usize),
            V_STRING => StringValue(self.next_string()),
            V_UUID => UUIDValue(self.next_uuid()),
            V_ARRAY => Array(self.next_array()),
            V_JSON_VALUE => JSONValue(self.next_json()),
            V_TABLE_REF => TableRef(self.next_string()),
            V_TABLE => TableValue(self.next_table()),
            V_TUPLE => TupleValue(self.next_array()),
            z => panic!("Unhandled value code {}", z)
        }
    }

    pub fn peek(&self) -> u8 {
        self.validate(1);
        codec::decode_u8(&self.buf, self.offset, |b| b)
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
        self.put_u64(string.len() as u64);
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

    pub fn put_value(&mut self, value: &TypedValue) -> &Self {
        self.put_u8(value.ordinal());
        self.put_bytes(&value.encode())
    }

    /// changes the capacity to the buffer
    pub fn resize(&mut self, new_size: usize) {
        self.buf.resize(new_size, 0u8);
    }

    /// returns a vector contains all bytes from the current position until the end of the buffer
    pub fn to_array(&self) -> Vec<u8> {
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

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_next_f32() {
        let mut buffer = ByteBuffer::wrap(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(buffer.limit(), 4);
        assert_eq!(buffer.next_f32(), -6.2598534e18)
    }

    #[test]
    fn test_next_f64() {
        let mut buffer = ByteBuffer::wrap(vec![
            0xBA, 0xBE, 0xFA, 0xCE, 0xD0, 0xCA, 0xFE, 0x00,
        ]);
        assert_eq!(buffer.next_f64(), -1.0010086845065389e-25)
    }

    #[test]
    fn test_next_i8() {
        let mut buffer = ByteBuffer::wrap(vec![0xAA]);
        assert_eq!(buffer.next_i8(), -86)
    }

    #[test]
    fn test_next_i16() {
        let mut buffer = ByteBuffer::wrap(vec![0xBE, 0xEF]);
        assert_eq!(buffer.next_i16(), -16657)
    }

    #[test]
    fn test_next_i32() {
        let mut buffer = ByteBuffer::wrap(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(buffer.next_i32(), -559038737)
    }

    #[test]
    fn test_next_i64() {
        let mut buffer = ByteBuffer::wrap(vec![
            0xBA, 0xBE, 0xFA, 0xCE, 0xD0, 0xCA, 0xFE, 0x00,
        ]);
        assert_eq!(buffer.next_i64(), -4990275570906759680)
    }

    #[test]
    fn test_next_i128() {
        let mut buffer = ByteBuffer::wrap(vec![
            0xBA, 0xBE, 0xFA, 0xCE, 0xBA, 0xDD, 0xEE, 0x00,
            0xCA, 0xFE, 0xDE, 0xAD, 0xBE, 0xEF, 0xBE, 0xAD,
        ]);
        assert_eq!(buffer.next_i128(), -92054336320587594627030856550656786771i128)
    }

    #[test]
    fn test_next_u8() {
        let mut buffer = ByteBuffer::wrap(vec![0xAA]);
        assert_eq!(buffer.next_u8(), 0xAA)
    }

    #[test]
    fn test_next_u16() {
        let mut buffer = ByteBuffer::wrap(vec![0xBE, 0xEF]);
        assert_eq!(buffer.next_u16(), 0xBEEF)
    }

    #[test]
    fn test_next_u32() {
        let mut buffer = ByteBuffer::wrap(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(buffer.next_u32(), 0xDEADBEEF)
    }

    #[test]
    fn test_next_u64() {
        let mut buffer = ByteBuffer::wrap(vec![
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
        let mut buffer = ByteBuffer::from_bytes(&bytes, bytes.len());
        buffer.flip();
        assert_eq!(buffer.next_u128(), 0xBABE_FACE_BADDEE_00_CAFE_DEAD_BEEF_BEAD)
    }

    #[test]
    fn test_put_f32() {
        let mut buffer = ByteBuffer::new(4);
        buffer.put_f32(125.0);
        buffer.flip();
        assert_eq!(buffer.next_f32(), 125.0);
    }

    #[test]
    fn test_put_f64() {
        let mut buffer = ByteBuffer::new(8);
        buffer.put_f64(4_000_000.0);
        buffer.flip();
        assert_eq!(buffer.next_f64(), 4_000_000.0);
    }

    #[test]
    fn test_put_i8() {
        let mut buffer = ByteBuffer::new(1);
        buffer.put_i8(99);
        buffer.flip();
        assert_eq!(buffer.next_i8(), 99);
    }

    #[test]
    fn test_put_i16() {
        let mut buffer = ByteBuffer::new(2);
        buffer.put_i16(16235);
        buffer.flip();
        assert_eq!(buffer.next_i16(), 16235);
    }

    #[test]
    fn test_put_i32() {
        let mut buffer = ByteBuffer::new(4);
        buffer.put_i32(125_000);
        buffer.flip();
        assert_eq!(buffer.next_i32(), 125_000);
    }

    #[test]
    fn test_put_i64() {
        let mut buffer = ByteBuffer::new(8);
        buffer.put_i64(4_000_000);
        buffer.flip();
        assert_eq!(buffer.next_i64(), 4_000_000);
    }

    #[test]
    fn test_put_i128() {
        let mut buffer = ByteBuffer::new(16);
        buffer.put_i128(4_000_000_000_000);
        buffer.flip();
        assert_eq!(buffer.next_i128(), 4_000_000_000_000);
    }

    #[test]
    fn test_put_u8() {
        let mut buffer = ByteBuffer::new(8);
        buffer.put_u8(0x88);
        buffer.flip();
        assert_eq!(buffer.next_u8(), 0x88);
    }

    #[test]
    fn test_put_u16() {
        let mut buffer = ByteBuffer::new(8);
        buffer.put_u16(0x8877);
        buffer.flip();
        assert_eq!(buffer.next_u16(), 0x8877);
    }

    #[test]
    fn test_put_u32() {
        let mut buffer = ByteBuffer::new(8);
        buffer.put_u32(0x88776655);
        buffer.flip();
        assert_eq!(buffer.next_u32(), 0x88776655);
    }

    #[test]
    fn test_put_u64() {
        let mut buffer = ByteBuffer::new(8);
        buffer.put_u64(0x88776655_44332211);
        buffer.flip();
        assert_eq!(buffer.next_u64(), 0x88776655_44332211);
    }

    #[test]
    fn test_put_u128() {
        let mut buffer = ByteBuffer::new(16);
        buffer.put_u128(0xBABE_FACE_BADDEE_CAFE_DEAD_BEEF_BEAD_00);
        buffer.flip();
        assert_eq!(buffer.next_u128(), 0xBABE_FACE_BADDEE_CAFE_DEAD_BEEF_BEAD_00);
    }

    #[test]
    fn test_next_string() {
        let mut buffer = ByteBuffer::new(20);
        buffer.put_string("Hello World!");
        buffer.flip();
        assert_eq!(buffer.to_array(), vec![
            0, 0, 0, 0, 0, 0, 0, 12,
            b'H', b'e', b'l', b'l', b'o', b' ', b'W', b'o', b'r', b'l', b'd', b'!',
        ]);
        assert_eq!(buffer.next_string(), "Hello World!");
    }

    #[test]
    fn test_to_array() {
        let mut buffer = ByteBuffer::new(8);
        buffer.put_u64(0xBABE_FACE_CAFE_DEAD);
        buffer.flip();
        buffer.next_u32(); // skip 4 bytes
        assert_eq!(buffer.to_array(), vec![0xCA, 0xFE, 0xDE, 0xAD])
    }

    #[test]
    fn test_reading() {
        let mut buffer = ByteBuffer::wrap(vec![
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
    fn test_writing() {
        let mut buffer = ByteBuffer::from_bytes(&vec![0xBA, 0xBE], 8);
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