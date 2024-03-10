////////////////////////////////////////////////////////////////////
// typed values module
////////////////////////////////////////////////////////////////////

use std::{i32, io};
use std::error::Error;

use chrono::{DateTime, NaiveDateTime};
use regex::Regex;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::codec;
use crate::data_types::DataType;
use crate::data_types::DataType::*;
use crate::typed_values::TypedValue::*;

const ISO_DATE_FORMAT: &str = r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?(([+-]\d\d:\d\d)|Z)?$";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum TypedValue {
    BLOBValue(Vec<u8>),
    BooleanValue(bool),
    CLOBValue(Vec<char>),
    DateValue(i64),
    Float32Value(f32),
    Float64Value(f64),
    Int8Value(u8),
    Int16Value(i16),
    Int32Value(i32),
    Int64Value(i64),
    NullValue,
    RecordNumberValue(usize),
    StringValue(String),
    UUIDValue([u8; 16]),
}

impl TypedValue {
    pub fn decode(data_type: &DataType, buffer: &Vec<u8>, offset: usize) -> TypedValue {
        match data_type {
            BLOBType(_size) => BLOBValue(vec![]),
            BooleanType => codec::decode_u8(buffer, offset, |b| BooleanValue(b == 1)),
            CLOBType(_size) => CLOBValue(vec![]),
            DateType => codec::decode_u8x8(buffer, offset, |b| DateValue(i64::from_be_bytes(b))),
            EnumType(_) => todo!(), // TODO enum support
            Int8Type => codec::decode_u8(buffer, offset, |b| Int8Value(b)),
            Int16Type => codec::decode_u8x2(buffer, offset, |b| Int16Value(i16::from_be_bytes(b))),
            Int32Type => codec::decode_u8x4(buffer, offset, |b| Int32Value(i32::from_be_bytes(b))),
            Int64Type => codec::decode_u8x8(buffer, offset, |b| Int64Value(i64::from_be_bytes(b))),
            Float32Type => codec::decode_u8x4(buffer, offset, |b| Float32Value(f32::from_be_bytes(b))),
            Float64Type => codec::decode_u8x8(buffer, offset, |b| Float64Value(f64::from_be_bytes(b))),
            RecordNumberType => Int64Value(codec::decode_row_id(&buffer, 1) as i64),
            StringType(size) => StringValue(codec::decode_string(buffer, offset, *size).to_string()),
            StructType(_) => todo!(), // TODO struct support
            TableType(_) => todo!(), // TODO table support (struct table stocks ...)
            UUIDType => codec::decode_u8x16(buffer, offset, |b| UUIDValue(b))
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        TypedValue::encode_value(self)
    }

    pub fn encode_value(value: &TypedValue) -> Vec<u8> {
        match value {
            BLOBValue(bytes) => codec::encode_u8x_n(bytes.to_vec()),
            BooleanValue(v) => [if *v { 1 } else { 0 }].to_vec(),
            CLOBValue(chars) => codec::encode_chars(chars.to_vec()),
            DateValue(number) => number.to_be_bytes().to_vec(),
            Float32Value(number) => number.to_be_bytes().to_vec(),
            Float64Value(number) => number.to_be_bytes().to_vec(),
            Int8Value(number) => number.to_be_bytes().to_vec(),
            Int16Value(number) => number.to_be_bytes().to_vec(),
            Int32Value(number) => number.to_be_bytes().to_vec(),
            Int64Value(number) => number.to_be_bytes().to_vec(),
            NullValue => [0u8; 0].to_vec(),
            RecordNumberValue(id) => id.to_be_bytes().to_vec(),
            StringValue(string) => codec::encode_u8x_n(string.bytes().collect()),
            UUIDValue(guid) => guid.to_vec()
        }
    }

    fn millis_to_iso_date(millis: i64) -> String {
        let seconds = millis / 1000;
        let nanoseconds = (millis % 1000) * 1_000_000;
        let datetime = NaiveDateTime::from_timestamp_opt(seconds, nanoseconds as u32).unwrap();
        let iso_date = datetime.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
        iso_date
    }

    pub fn to_json(&self) -> serde_json::Value {
        match self {
            BLOBValue(bytes) => serde_json::json!(bytes),
            BooleanValue(b) => serde_json::json!(b),
            CLOBValue(chars) => serde_json::json!(chars),
            DateValue(millis) => serde_json::json!(Self::millis_to_iso_date(*millis)),
            Float32Value(number) => serde_json::json!(number),
            Float64Value(number) => serde_json::json!(number),
            Int8Value(number) => serde_json::json!(number),
            Int16Value(number) => serde_json::json!(number),
            Int32Value(number) => serde_json::json!(number),
            Int64Value(number) => serde_json::json!(number),
            NullValue => serde_json::Value::Null,
            RecordNumberValue(number) => serde_json::json!(number),
            StringValue(string) => serde_json::json!(string),
            UUIDValue(guid) => serde_json::json!(Uuid::from_bytes(*guid).to_string())
        }
    }

    pub fn unwrap_value(&self) -> String {
        match self {
            BLOBValue(bytes) => hex::encode(bytes),
            BooleanValue(b) => (if *b { "true" } else { "false" }).into(),
            CLOBValue(chars) => chars.into_iter().collect(),
            DateValue(millis) => Self::millis_to_iso_date(*millis),
            Float32Value(number) => number.to_string(),
            Float64Value(number) => number.to_string(),
            Int8Value(number) => number.to_string(),
            Int16Value(number) => number.to_string(),
            Int32Value(number) => number.to_string(),
            Int64Value(number) => number.to_string(),
            NullValue => "null".into(),
            RecordNumberValue(number) => number.to_string(),
            StringValue(string) => string.into(),
            UUIDValue(guid) => Uuid::from_bytes(*guid).to_string()
        }
    }

    pub fn wrap_value(raw_value: &str) -> io::Result<TypedValue> {
        let decimal_regex = Regex::new(r"^-?\d+\.\d+$")
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let int_regex = Regex::new(r"^-?\d+$")
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let iso_date_regex = Regex::new(ISO_DATE_FORMAT)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let uuid_regex = Regex::new("^[0-9a-fA-F]{8}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{12}$")
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let result = match raw_value {
            s if s == "false" => BooleanValue(false),
            s if s == "null" => NullValue,
            s if s == "true" => BooleanValue(true),
            s if s.is_empty() => NullValue,
            s if int_regex.is_match(s) => Int64Value(s.parse::<i64>()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?),
            s if decimal_regex.is_match(s) => Float64Value(s.parse::<f64>()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?),
            s if iso_date_regex.is_match(s) =>
                DateValue(DateTime::parse_from_rfc3339(s)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?.timestamp_millis()),
            s if uuid_regex.is_match(s) => UUIDValue(codec::decode_uuid(s)?),
            s => StringValue(s.to_string()),
        };
        Ok(result)
    }

    pub fn wrap_value_opt(opt_value: &Option<String>) -> io::Result<TypedValue> {
        match opt_value {
            Some(value) => Self::wrap_value(value.trim()),
            None => Ok(NullValue)
        }
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode() {
        let buf: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 5, b'H', b'e', b'l', b'l', b'o'];
        assert_eq!(TypedValue::decode(&StringType(5), &buf, 0), StringValue("Hello".into()))
    }

    #[test]
    fn test_encode() {
        let expected: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 5, b'H', b'e', b'l', b'l', b'o'];
        assert_eq!(TypedValue::encode_value(&StringValue("Hello".into())), expected);
    }

    #[test]
    fn test_to_json() {
        verify_to_json(BooleanValue(false), serde_json::json!(false));
        verify_to_json(BooleanValue(true), serde_json::json!(true));
        verify_to_json(DateValue(1709163679081), serde_json::Value::String("2024-02-28T23:41:19.081Z".into()));
        verify_to_json(Float64Value(100.1), serde_json::json!(100.1));
        verify_to_json(Int64Value(100), serde_json::json!(100));
        verify_to_json(NullValue, serde_json::Value::Null);
        verify_to_json(StringValue("Hello World".into()), serde_json::Value::String("Hello World".into()));
        verify_to_json(UUIDValue([
            0x67, 0x45, 0x23, 0x01, 0xAB, 0xCD, 0xEF, 0x89,
            0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF,
        ]), serde_json::Value::String("67452301-abcd-ef89-1234-567890abcdef".into()));
    }

    #[test]
    fn test_wrap_and_unwrap_value() {
        verify_wrap_unwrap("false", BooleanValue(false));
        verify_wrap_unwrap("true", BooleanValue(true));
        verify_wrap_unwrap("2024-02-28T23:41:19.081Z", DateValue(1709163679081));
        verify_wrap_unwrap("100.1", Float64Value(100.1));
        verify_wrap_unwrap("100", Int64Value(100));
        verify_wrap_unwrap("null", NullValue);
        verify_wrap_unwrap("Hello World", StringValue("Hello World".into()));
        verify_wrap_unwrap("67452301-abcd-ef89-1234-567890abcdef", UUIDValue([
            0x67, 0x45, 0x23, 0x01, 0xAB, 0xCD, 0xEF, 0x89,
            0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF,
        ]))
    }

    #[test]
    fn test_unwrap_optional_value() {
        assert_eq!(TypedValue::wrap_value_opt(&Some("123.45".into())).unwrap(), Float64Value(123.45))
    }

    fn verify_to_json(typed_value: TypedValue, expected: serde_json::Value) {
        assert_eq!(typed_value.to_json(), expected)
    }

    fn verify_wrap_unwrap(string_value: &str, typed_value: TypedValue) {
        assert_eq!(typed_value.unwrap_value(), string_value);
        match TypedValue::wrap_value(string_value) {
            Ok(value) => assert_eq!(value, typed_value),
            Err(message) => panic!("{}", message)
        };
    }
}