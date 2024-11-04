////////////////////////////////////////////////////////////////////
// Codec class - responsible for encoding/decoding bytes
////////////////////////////////////////////////////////////////////

use std::mem::size_of;

use uuid::Uuid;

use shared_lib::fail;

use crate::errors::Errors::Exact;
use crate::expression::Expression;
use crate::expression::Expression::Literal;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::ErrorValue;

/// Oxide Codec
pub struct Codec;

impl Codec {
    pub fn decode(bytes: &Vec<u8>) -> Expression {
        Self::unwrap_as_value(bincode::deserialize(bytes),
                              |err| Literal(ErrorValue(Exact(err.to_string()))))
    }

    pub fn encode(model: &Expression) -> std::io::Result<Vec<u8>> {
        Self::unwrap_as_result(bincode::serialize(model))
    }

    pub fn decode_value(bytes: &Vec<u8>) -> TypedValue {
        Self::unwrap_as_value(bincode::deserialize(bytes),
                              |err| ErrorValue(Exact(err.to_string())))
    }

    pub fn encode_value(model: &TypedValue) -> std::io::Result<Vec<u8>> {
        Self::unwrap_as_result(bincode::serialize(model))
    }

    pub fn unwrap_as_result<T>(result: bincode::Result<T>) -> std::io::Result<T> {
        match result {
            Ok(bytes) => Ok(bytes),
            Err(err) => fail(err.to_string())
        }
    }

    pub fn unwrap_as_value<T>(result: bincode::Result<T>, f: fn(bincode::Error) -> T) -> T {
        result.unwrap_or_else(|err| f(err))
    }
}

pub fn decode_row_id(buffer: &Vec<u8>, offset: usize) -> usize {
    let mut id_array = [0u8; 8];
    id_array.copy_from_slice(&buffer[offset..(offset + 8)]);
    usize::from_be_bytes(id_array)
}

pub fn decode_string(buffer: &Vec<u8>, offset: usize, max_size: usize) -> String {
    let a: usize = offset + size_of::<usize>();
    let b: usize = a + max_size;
    let mut data: &[u8] = &buffer[a..b];
    while data.len() > 0 && data[data.len() - 1] == 0 {
        data = &data[0..(data.len() - 1)];
    }
    match std::str::from_utf8(&*data) {
        Ok(string) => string.to_string(),
        Err(err) => panic!("data: '{:?}' -> {}", data, err),
    }
}

pub(crate) fn decode_u8<A>(buffer: &Vec<u8>, offset: usize, f: fn(u8) -> A) -> A {
    f(buffer[offset])
}

pub(crate) fn decode_u8x2<A>(buffer: &Vec<u8>, offset: usize, f: fn([u8; 2]) -> A) -> A {
    let mut scratch = [0; 2];
    let limit = offset + scratch.len();
    scratch.copy_from_slice(&buffer[offset..limit]);
    f(scratch)
}

pub(crate) fn decode_u8x4<A>(buffer: &Vec<u8>, offset: usize, f: fn([u8; 4]) -> A) -> A {
    let mut scratch = [0; 4];
    let limit = offset + scratch.len();
    scratch.copy_from_slice(&buffer[offset..limit]);
    f(scratch)
}

pub(crate) fn decode_u8x8<A>(buffer: &Vec<u8>, offset: usize, f: fn([u8; 8]) -> A) -> A {
    let mut scratch = [0; 8];
    let limit = offset + scratch.len();
    scratch.copy_from_slice(&buffer[offset..limit]);
    f(scratch)
}

pub(crate) fn decode_u8x16<A>(buffer: &Vec<u8>, offset: usize, f: fn([u8; 16]) -> A) -> A {
    let mut scratch = [0; 16];
    let limit = offset + scratch.len();
    scratch.copy_from_slice(&buffer[offset..limit]);
    f(scratch)
}

pub(crate) fn decode_uuid(uuid_str: &str) -> std::io::Result<u128> {
    match Uuid::parse_str(uuid_str) {
        Ok(uuid) => Ok(uuid.as_u128()),
        Err(err) => fail(err.to_string())
    }
}

pub fn encode_chars(chars: Vec<char>) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::with_capacity(chars.len());
    for ch in chars {
        buf.extend(ch.encode_utf8(&mut [0; 4]).bytes());
    }
    encode_u8x_n(buf)
}

pub fn encode_row_id(id: usize) -> Vec<u8> {
    id.to_be_bytes().to_vec()
}

pub fn encode_string(string: &str) -> Vec<u8> {
    encode_u8x_n(string.bytes().collect())
}

pub fn encode_u8x_n(bytes: Vec<u8>) -> Vec<u8> {
    let width = bytes.len();
    let overhead = width.to_be_bytes();
    let mut buf: Vec<u8> = Vec::with_capacity(width + overhead.len());
    buf.extend(width.to_be_bytes());
    buf.extend(bytes);
    buf
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::Codec;
    use crate::expression::Conditions::{Equal, GreaterThan, LessOrEqual};
    use crate::expression::Expression::{Condition, If, JSONExpression, Literal, Multiply, Plus, Quarry, Variable, Via};
    use crate::expression::{Excavation, Mutation, Queryable};
    use crate::model_row_collection::ModelRowCollection;
    use crate::numbers::NumberValue::{F64Value, I64Value};
    use crate::testdata::{make_quote, make_quote_columns};
    use crate::typed_values::TypedValue::{Number, StringValue, TableValue};

    #[test]
    fn test_table_codec() {
        let phys_columns = make_quote_columns();
        let expected = TableValue(ModelRowCollection::from_rows(phys_columns.clone(), vec![
            make_quote(0, "ABC", "AMEX", 11.77),
            make_quote(1, "UNO", "OTC", 0.2456),
            make_quote(2, "BIZ", "NYSE", 23.66),
            make_quote(3, "GOTO", "OTC", 0.1428),
            make_quote(4, "BOOM", "NASDAQ", 56.87)]));
        let bytes = Codec::encode_value(&expected).unwrap();
        let actual = Codec::decode_value(&bytes);
        assert_eq!(actual, expected)
    }

    #[test]
    fn test_if_else() {
        let model = If {
            condition: Box::new(Condition(GreaterThan(
                Box::new(Variable("num".into())),
                Box::new(Literal(Number(I64Value(25)))),
            ))),
            a: Box::new(Literal(StringValue("Yes".into()))),
            b: Some(Box::new(Literal(StringValue("No".into())))),
        };

        let bytes = Codec::encode(&model).unwrap();
        let actual = Codec::decode(&bytes);
        assert_eq!(actual, model)
    }

    #[test]
    fn test_expression_delete() {
        let model = Quarry(Excavation::Mutate(Mutation::Delete {
            path: Box::new(Variable("stocks".into())),
            condition: Some(LessOrEqual(
                Box::new(Variable("last_sale".into())),
                Box::new(Literal(Number(F64Value(1.0)))),
            )),
            limit: Some(Box::new(Literal(Number(I64Value(100))))),
        }));
        let byte_code = Codec::encode(&model).unwrap();
        let actual = Codec::decode(&byte_code);
        assert_eq!(actual, model);
    }

    #[test]
    fn test_literal() {
        let model = Literal(StringValue("hello".into()));
        let byte_code = Codec::encode(&model).unwrap();
        let actual = Codec::decode(&byte_code);
        assert_eq!(actual, model);
    }

    #[test]
    fn test_variable() {
        let model = Variable("name".into());
        let byte_code = Codec::encode(&model).unwrap();
        let actual = Codec::decode(&byte_code);
        assert_eq!(actual, model);
    }

    #[test]
    fn test_math_ops() {
        let model = Plus(Box::new(Literal(Number(I64Value(2)))),
                         Box::new(Multiply(Box::new(Literal(Number(I64Value(4)))),
                                           Box::new(Literal(Number(I64Value(3)))))));
        let byte_code = Codec::encode(&model).unwrap();
        let actual = Codec::decode(&byte_code);
        assert_eq!(actual, model);
    }

    #[test]
    fn test_select() {
        let model = Quarry(Excavation::Query(Queryable::Select {
            fields: vec![Variable("symbol".into()), Variable("exchange".into()), Variable("last_sale".into())],
            from: Some(Box::new(Variable("stocks".into()))),
            condition: Some(LessOrEqual(
                Box::new(Variable("last_sale".into())),
                Box::new(Literal(Number(F64Value(1.0)))),
            )),
            group_by: None,
            having: None,
            order_by: Some(vec![Variable("symbol".into())]),
            limit: Some(Box::new(Literal(Number(I64Value(5))))),
        }));
        let byte_code = Codec::encode(&model).unwrap();
        let actual = Codec::decode(&byte_code);
        assert_eq!(actual, model);
    }

    #[test]
    fn test_update() {
        let model = Quarry(Excavation::Mutate(Mutation::Update {
            path: Box::new(Variable("stocks".into())),
            source: Box::new(Via(Box::new(JSONExpression(vec![
                ("last_sale".into(), Literal(Number(F64Value(0.1111)))),
            ])))),
            condition: Some(Equal(
                Box::new(Variable("exchange".into())),
                Box::new(Literal(StringValue("NYSE".into()))),
            )),
            limit: Some(Box::new(Literal(Number(I64Value(10))))),
        }));
        let byte_code = Codec::encode(&model).unwrap();
        let actual = Codec::decode(&byte_code);
        assert_eq!(actual, model);
    }

    #[test]
    fn test_json_literal() {
        let model = JSONExpression(vec![
            ("symbol".to_string(), Literal(StringValue("TRX".into()))),
            ("exchange".to_string(), Literal(StringValue("NYSE".into()))),
            ("last_sale".to_string(), Literal(Number(F64Value(11.1111)))),
        ]);
        let byte_code = Codec::encode(&model).unwrap();
        let actual = Codec::decode(&byte_code);
        assert_eq!(actual, model);
    }

    #[test]
    fn test_decode_row_id() {
        let buf: Vec<u8> = vec![0xDE, 0xAD, 0xCA, 0xFE, 0xBE, 0xEF, 0xBA, 0xBE];
        let id = decode_row_id(&buf, 0);
        assert_eq!(id, 0xDEAD_CAFE_BEEF_BABE)
    }

    #[test]
    fn test_decode_u8() {
        let buf: Vec<u8> = vec![64];
        let value = decode_u8(&buf, 0, |b| b);
        assert_eq!(value, 64)
    }

    #[test]
    fn test_decode_u8x2() {
        let buf: Vec<u8> = vec![255, 255];
        let value: u16 = decode_u8x2(&buf, 0, |b| u16::from_be_bytes(b));
        assert_eq!(value, 65535)
    }

    #[test]
    fn test_decode_u8x4() {
        let buf: Vec<u8> = vec![64, 64, 112, 163];
        let value: i32 = decode_u8x4(&buf, 0, |b| i32::from_be_bytes(b));
        assert_eq!(value, 1077964963)
    }

    #[test]
    fn test_decode_u8x8() {
        let buf: Vec<u8> = vec![64, 64, 112, 163, 215, 10, 61, 113];
        let value: f64 = decode_u8x8(&buf, 0, |b| f64::from_be_bytes(b));
        assert_eq!(value, 32.88)
    }

    #[test]
    fn test_decode_u8x16() {
        let buf: Vec<u8> = vec![0x29, 0x92, 0xbb, 0x53, 0xcc, 0x3c, 0x4f, 0x30, 0x8a, 0x4c, 0xc1, 0xa6, 0x66, 0xaf, 0xcc, 0x46];
        let value: Uuid = decode_u8x16(&buf, 0, |b| Uuid::from_bytes(b));
        assert_eq!(value, Uuid::parse_str("2992bb53-cc3c-4f30-8a4c-c1a666afcc46").unwrap())
    }

    #[test]
    fn test_decode_string_max_length() {
        let buf: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 4, b'Y', b'H', b'W', b'H'];
        assert_eq!(decode_string(&buf, 0, 4), "YHWH")
    }

    #[test]
    fn test_decode_string_less_than_max_length() {
        let buf: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 4, b'Y', b'A', b'H', 0];
        assert_eq!(decode_string(&buf, 0, 4), "YAH")
    }

    #[test]
    fn test_decode_uuid() {
        let uuid = decode_uuid("2992bb53-cc3c-4f30-8a4c-c1a666afcc46").unwrap();
        assert_eq!(uuid, 0x2992bb53cc3c4f308a4cc1a666afcc46u128)
    }

    #[test]
    fn test_encode_chars() {
        let expected: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 30, 84, 104, 101, 32, 108, 105, 116, 116, 108, 101, 32, 121, 111, 114, 107, 105, 101, 32, 98, 97, 114, 107, 101, 100, 32, 97, 116, 32, 109, 101];
        let actual: Vec<u8> = encode_chars("The little yorkie barked at me".chars().collect());
        assert_eq!(actual, expected)
    }

    #[test]
    fn test_encode_row_id() {
        let expected: Vec<u8> = vec![0xDE, 0xAD, 0xCA, 0xFE, 0xBE, 0xEF, 0xBA, 0xBE];
        let id = 0xDEAD_CAFE_BEEF_BABE;
        assert_eq!(encode_row_id(id), expected)
    }
}