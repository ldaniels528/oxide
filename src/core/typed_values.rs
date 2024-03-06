////////////////////////////////////////////////////////////////////
// typed values module
////////////////////////////////////////////////////////////////////

use std::error::Error;
use std::i32;

use chrono::DateTime;
use regex::Regex;
use serde::Serialize;

use crate::codec;
use crate::data_types::DataType;
use crate::data_types::DataType::*;
use crate::typed_values::TypedValue::*;

const ISO_DATE_FORMAT: &str = r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?(([+-]\d\d:\d\d)|Z)?$";

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum TypedValue {
    BLOBValue(Vec<u8>),
    CLOBValue(Vec<char>),
    DateValue(i64),
    Float32Value(f32),
    Float64Value(f64),
    Int8Value(u8),
    Int16Value(i16),
    Int32Value(i32),
    Int64Value(i64),
    NullValue,
    StringValue(String),
    UUIDValue([u8; 16]),
}

impl TypedValue {
    pub fn decode(data_type: &DataType, buffer: &Vec<u8>, offset: usize) -> TypedValue {
        match data_type {
            BLOBType(size) => BLOBValue(vec![]),
            CLOBType(size) => CLOBValue(vec![]),
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
            CLOBValue(chars) => codec::encode_chars(chars.to_vec()),
            DateValue(number) => number.to_be_bytes().to_vec(),
            Float32Value(number) => number.to_be_bytes().to_vec(),
            Float64Value(number) => number.to_be_bytes().to_vec(),
            Int8Value(number) => number.to_be_bytes().to_vec(),
            Int16Value(number) => number.to_be_bytes().to_vec(),
            Int32Value(number) => number.to_be_bytes().to_vec(),
            Int64Value(number) => number.to_be_bytes().to_vec(),
            NullValue => [0u8; 0].to_vec(),
            StringValue(string) => codec::encode_u8x_n(string.bytes().collect()),
            UUIDValue(guid) => guid.to_vec()
        }
    }

    pub fn wrap_value(raw_value: &str) -> Result<TypedValue, Box<dyn Error>> {
        let decimal_regex = Regex::new(r"^-?\d+\.\d+$")?;
        let int_regex = Regex::new(r"^-?\d+$")?;
        let iso_date_regex = Regex::new(ISO_DATE_FORMAT)?;
        let uuid_regex = Regex::new("^[0-9a-fA-F]{8}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{12}$")?;
        match raw_value {
            s if s.is_empty() => Ok(NullValue),
            s if int_regex.is_match(s) => Ok(Int64Value(s.parse::<i64>()?)),
            s if decimal_regex.is_match(s) => Ok(Float64Value(s.parse::<f64>()?)),
            s if iso_date_regex.is_match(s) =>
                Ok(DateValue(DateTime::parse_from_rfc3339(s)?.timestamp_millis())),
            s if uuid_regex.is_match(s) => Ok(UUIDValue(codec::decode_uuid(s)?)),
            s => Ok(StringValue(s.to_string())),
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
        let actual: TypedValue = TypedValue::decode(&StringType(5), &buf, 0);
        let expected = StringValue("Hello".to_string());
        assert_eq!(actual, expected)
    }

    #[test]
    fn test_encode() {
        let value = StringValue("Hello".to_string());
        let actual: Vec<u8> = TypedValue::encode_value(&value);
        let expected: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 5, b'H', b'e', b'l', b'l', b'o'];
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_wrap_value() {
        verify("100", Int64Value(100));
        verify("100.0", Float64Value(100.0));
        verify("2024-02-28T23:41:19.081Z", DateValue(1709163679081));
        verify("Hello World", StringValue("Hello World".to_string()));
    }

    fn verify(raw_value: &str, expected_value: TypedValue) {
        match TypedValue::wrap_value(raw_value) {
            Ok(value) => assert_eq!(value, expected_value),
            Err(message) => panic!("{}", message)
        };
    }
}