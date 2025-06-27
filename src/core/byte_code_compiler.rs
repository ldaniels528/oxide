////////////////////////////////////////////////////////////////////
// ByteCodeCompiler - responsible for assembling and disassembling
//                    expressions to and from byte code.
////////////////////////////////////////////////////////////////////

use crate::columns::Column;
use crate::dataframe::Dataframe;

use crate::dataframe::Dataframe::Model;
use crate::errors::throw;
use crate::errors::Errors::Exact;
use crate::expression::Expression::Literal;
use crate::expression::*;
use crate::field::FieldMetadata;
use crate::model_row_collection::ModelRowCollection;
use crate::row_collection::RowCollection;
use crate::row_metadata::RowMetadata;
use crate::structures::Row;
use crate::typed_values::TypedValue::ErrorValue;
use crate::typed_values::*;
use std::ops::{Deref, Index};
use uuid::Uuid;

/// A JVM-inspired ByteBuffer-like utility (Big Endian)
pub struct ByteCodeCompiler;

impl ByteCodeCompiler {

    ////////////////////////////////////////////////////////////////
    //      Instance Methods
    ////////////////////////////////////////////////////////////////

    pub fn decode(bytes: &Vec<u8>) -> Expression {
        Self::unwrap_as_value(bincode::deserialize(bytes),
                              |err| Literal(ErrorValue(Exact(err.to_string()))))
    }

    /// Decodes the supplied buffer returning a row and its metadata
    pub fn decode_row(params: &Vec<Column>, buffer: &Vec<u8>) -> (Row, RowMetadata) {
        // if the buffer is empty, return an empty row
        if buffer.len() == 0 {
            return (Row::create(0, params), RowMetadata::new(false));
        }
        let metadata = RowMetadata::from_bytes(buffer, 0);
        let id = ByteCodeCompiler::decode_row_id(buffer, 1);
        let values = params.iter().map(|column| {
            let data_type = column.get_data_type();
            data_type.decode_field_value(&buffer, column.get_offset())
        }).collect();
        (Row::new(id, values), metadata)
    }

    /// Decodes the supplied buffer returning a collection of rows.
    pub fn decode_rows(columns: &Vec<Column>, row_data: Vec<Vec<u8>>) -> Vec<Row> {
        let mut rows = Vec::new();
        for row_bytes in row_data {
            let (row, metadata) = Self::decode_row(&columns, &row_bytes);
            if metadata.is_allocated { rows.push(row); }
        }
        rows
    }

    pub fn encode(model: &Expression) -> std::io::Result<Vec<u8>> {
        Self::unwrap_as_result(bincode::serialize(model))
    }

    pub fn encode_df(df: &Dataframe) -> Vec<u8> {
        let mut encoded = vec![];
        let columns = df.get_columns();
        for row in df.iter() {
            encoded.extend(Self::encode_row(&row, columns));
        }
        encoded
    }

    pub fn encode_rc(rc: &Box<dyn RowCollection>) -> Vec<u8> {
        //Self::unwrap_as_result(bincode::serialize(df))
        let mut encoded = vec![];
        let columns = rc.get_columns();
        for row in rc.iter() {
            encoded.extend(Self::encode_row(&row, columns));
        }
        encoded
    }

    /// Returns the binary-encoded equivalent of the row.
    pub fn encode_row(row: &Row, phys_columns: &Vec<Column>) -> Vec<u8> {
        let capacity = Row::compute_record_size(&phys_columns);
        let mut fixed_buf = Vec::with_capacity(capacity);
        // include the field metadata and row ID
        fixed_buf.push(RowMetadata::new(true).encode());
        fixed_buf.extend(ByteCodeCompiler::encode_row_id(row.get_id()));
        // include the fields
        let fmd = FieldMetadata::new(true);
        let bb: Vec<u8> = row.get_values().iter().zip(phys_columns.iter())
            .flat_map(|(value, column)| {
                column.get_data_type().encode_field(value, &fmd, column.get_fixed_size())
            }).collect();
        fixed_buf.extend(bb);
        fixed_buf.resize(capacity, 0u8);
        fixed_buf
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
            Err(err) => throw(Exact(err.to_string()))
        }
    }

    pub fn unwrap_as_value<T>(result: bincode::Result<T>, f: fn(bincode::Error) -> T) -> T {
        result.unwrap_or_else(|err| f(err))
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

    pub fn decode_u8<A>(buffer: &Vec<u8>, offset: usize, f: fn(u8) -> A) -> A {
        f(buffer[offset])
    }

    pub fn decode_u8x2<A>(buffer: &Vec<u8>, offset: usize, f: fn([u8; 2]) -> A) -> A {
        let mut scratch = [0; 2];
        let limit = offset + scratch.len();
        scratch.copy_from_slice(&buffer[offset..limit]);
        f(scratch)
    }

    pub fn decode_u8x4<A>(buffer: &Vec<u8>, offset: usize, f: fn([u8; 4]) -> A) -> A {
        let mut scratch = [0; 4];
        let limit = offset + scratch.len();
        scratch.copy_from_slice(&buffer[offset..limit]);
        f(scratch)
    }

    pub fn decode_u8x8<A>(buffer: &Vec<u8>, offset: usize, f: fn([u8; 8]) -> A) -> A {
        let mut scratch = [0; 8];
        let limit = offset + scratch.len();
        scratch.copy_from_slice(&buffer[offset..limit]);
        f(scratch)
    }

    pub fn decode_u8x16<A>(buffer: &Vec<u8>, offset: usize, f: fn([u8; 16]) -> A) -> A {
        let mut scratch = [0; 16];
        let limit = offset + scratch.len();
        scratch.copy_from_slice(&buffer[offset..limit]);
        f(scratch)
    }

    pub fn decode_uuid(uuid_str: &str) -> std::io::Result<u128> {
        match Uuid::parse_str(uuid_str) {
            Ok(uuid) => Ok(uuid.as_u128()),
            Err(err) => throw(Exact(err.to_string()))
        }
    }

    pub fn encode_chars(chars: Vec<char>) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::with_capacity(chars.len());
        for ch in chars {
            buf.extend(ch.encode_utf8(&mut [0; 4]).bytes());
        }
        Self::encode_u8x_n(buf)
    }

    pub fn encode_row_id(id: usize) -> Vec<u8> {
        id.to_be_bytes().to_vec()
    }

    pub fn encode_string(string: &str) -> Vec<u8> {
        Self::encode_u8x_n(string.bytes().collect())
    }

    pub fn encode_u8x_n(bytes: Vec<u8>) -> Vec<u8> {
        let width = bytes.len();
        let overhead = width.to_be_bytes();
        let mut buf: Vec<u8> = Vec::with_capacity(width + overhead.len());
        buf.extend(width.to_be_bytes());
        buf.extend(bytes);
        buf
    }
}

/// Unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataframe::Dataframe::Model;
    use crate::expression::Conditions::{GreaterThan, LessOrEqual};
    use crate::expression::Expression::{Condition, Identifier, If, Limit, Literal, Multiply, Plus, StructureExpression, Where};
    use crate::model_row_collection::ModelRowCollection;
    use crate::numbers::Numbers::{F64Value, I64Value};
    use crate::testdata::{make_quote, make_quote_columns};
    use crate::typed_values::TypedValue::{Number, StringValue, TableValue};

    #[test]
    fn test_table_codec() {
        let phys_columns = make_quote_columns();
        let expected = TableValue(Model(ModelRowCollection::from_columns_and_rows(&phys_columns, &vec![
            make_quote(0, "ABC", "AMEX", 11.77),
            make_quote(1, "UNO", "OTC", 0.2456),
            make_quote(2, "BIZ", "NYSE", 23.66),
            make_quote(3, "GOTO", "OTC", 0.1428),
            make_quote(4, "BOOM", "NASDAQ", 56.87)])));
        let bytes = ByteCodeCompiler::encode_value(&expected).unwrap();
        let actual = ByteCodeCompiler::decode_value(&bytes);
        assert_eq!(actual, expected)
    }

    #[test]
    fn test_if_else() {
        let model = If {
            condition: Box::new(Condition(GreaterThan(
                Box::new(Identifier("num".into())),
                Box::new(Literal(Number(I64Value(25)))),
            ))),
            a: Box::new(Literal(StringValue("Yes".into()))),
            b: Some(Box::new(Literal(StringValue("No".into())))),
        };

        let bytes = ByteCodeCompiler::encode(&model).unwrap();
        let actual = ByteCodeCompiler::decode(&bytes);
        assert_eq!(actual, model)
    }

    #[test]
    fn test_expression_delete() {
        let model = Limit {
            limit: Literal(Number(I64Value(100))).into(),
            from: Where {
                condition: LessOrEqual(
                    Identifier("last_sale".into()).into(),
                    Literal(Number(F64Value(1.0))).into(),
                ),
                from: Expression::Delete {
                    from: Identifier("stocks".into()).into(),
                }.into()
            }.into()
        };
        let byte_code = ByteCodeCompiler::encode(&model).unwrap();
        let actual = ByteCodeCompiler::decode(&byte_code);
        assert_eq!(actual, model);
    }

    #[test]
    fn test_literal() {
        let model = Literal(StringValue("hello".into()));
        let byte_code = ByteCodeCompiler::encode(&model).unwrap();
        let actual = ByteCodeCompiler::decode(&byte_code);
        assert_eq!(actual, model);
    }

    #[test]
    fn test_variable() {
        let model = Identifier("name".into());
        let byte_code = ByteCodeCompiler::encode(&model).unwrap();
        let actual = ByteCodeCompiler::decode(&byte_code);
        assert_eq!(actual, model);
    }

    #[test]
    fn test_math_ops() {
        let model = Plus(Box::new(Literal(Number(I64Value(2)))),
                         Box::new(Multiply(Box::new(Literal(Number(I64Value(4)))),
                                           Box::new(Literal(Number(I64Value(3)))))));
        let byte_code = ByteCodeCompiler::encode(&model).unwrap();
        let actual = ByteCodeCompiler::decode(&byte_code);
        assert_eq!(actual, model);
    }

    #[test]
    fn test_json_literal() {
        let model = StructureExpression(vec![
            ("symbol".to_string(), Literal(StringValue("TRX".into()))),
            ("exchange".to_string(), Literal(StringValue("NYSE".into()))),
            ("last_sale".to_string(), Literal(Number(F64Value(11.1111)))),
        ]);
        let byte_code = ByteCodeCompiler::encode(&model).unwrap();
        let actual = ByteCodeCompiler::decode(&byte_code);
        assert_eq!(actual, model);
    }

    #[test]
    fn test_decode_row_id() {
        let buf: Vec<u8> = vec![0xDE, 0xAD, 0xCA, 0xFE, 0xBE, 0xEF, 0xBA, 0xBE];
        let id = ByteCodeCompiler::decode_row_id(&buf, 0);
        assert_eq!(id, 0xDEAD_CAFE_BEEF_BABE)
    }

    #[test]
    fn test_decode_u8() {
        let buf: Vec<u8> = vec![64];
        let value = ByteCodeCompiler::decode_u8(&buf, 0, |b| b);
        assert_eq!(value, 64)
    }

    #[test]
    fn test_decode_u8x2() {
        let buf: Vec<u8> = vec![255, 255];
        let value: u16 = ByteCodeCompiler::decode_u8x2(&buf, 0, |b| u16::from_be_bytes(b));
        assert_eq!(value, 65535)
    }

    #[test]
    fn test_decode_u8x4() {
        let buf: Vec<u8> = vec![64, 64, 112, 163];
        let value: i32 = ByteCodeCompiler::decode_u8x4(&buf, 0, |b| i32::from_be_bytes(b));
        assert_eq!(value, 1077964963)
    }

    #[test]
    fn test_decode_u8x8() {
        let buf: Vec<u8> = vec![64, 64, 112, 163, 215, 10, 61, 113];
        let value: f64 = ByteCodeCompiler::decode_u8x8(&buf, 0, |b| f64::from_be_bytes(b));
        assert_eq!(value, 32.88)
    }

    #[test]
    fn test_decode_u8x16() {
        let buf: Vec<u8> = vec![0x29, 0x92, 0xbb, 0x53, 0xcc, 0x3c, 0x4f, 0x30, 0x8a, 0x4c, 0xc1, 0xa6, 0x66, 0xaf, 0xcc, 0x46];
        let value: Uuid = ByteCodeCompiler::decode_u8x16(&buf, 0, |b| Uuid::from_bytes(b));
        assert_eq!(value, Uuid::parse_str("2992bb53-cc3c-4f30-8a4c-c1a666afcc46").unwrap())
    }

    #[test]
    fn test_decode_string_max_length() {
        let buf: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 4, b'Y', b'H', b'W', b'H'];
        assert_eq!(ByteCodeCompiler::decode_string(&buf, 0, 4), "YHWH")
    }

    #[test]
    fn test_decode_string_less_than_max_length() {
        let buf: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 4, b'Y', b'A', b'H', 0];
        assert_eq!(ByteCodeCompiler::decode_string(&buf, 0, 4), "YAH")
    }

    #[test]
    fn test_decode_uuid() {
        let uuid = ByteCodeCompiler::decode_uuid("2992bb53-cc3c-4f30-8a4c-c1a666afcc46").unwrap();
        assert_eq!(uuid, 0x2992bb53cc3c4f308a4cc1a666afcc46u128)
    }

    #[test]
    fn test_encode_chars() {
        let expected: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 30, 84, 104, 101, 32, 108, 105, 116, 116, 108, 101, 32, 121, 111, 114, 107, 105, 101, 32, 98, 97, 114, 107, 101, 100, 32, 97, 116, 32, 109, 101];
        let actual: Vec<u8> = ByteCodeCompiler::encode_chars("The little yorkie barked at me".chars().collect());
        assert_eq!(actual, expected)
    }

    #[test]
    fn test_encode_row() {
        let phys_columns = make_quote_columns();
        let row = make_quote(255, "RED", "NYSE", 78.35);
        let bytes = ByteCodeCompiler::encode_row(&row, &phys_columns);
        assert_eq!(bytes, vec![
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 255,
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 3, b'R', b'E', b'D', 0, 0, 0, 0, 0,
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E', 0, 0, 0, 0,
            0b1000_0000, 64, 83, 150, 102, 102, 102, 102, 102,
        ]);
    }

    #[test]
    fn test_encode_row_id() {
        let expected: Vec<u8> = vec![0xDE, 0xAD, 0xCA, 0xFE, 0xBE, 0xEF, 0xBA, 0xBE];
        let id = 0xDEAD_CAFE_BEEF_BABE;
        assert_eq!(ByteCodeCompiler::encode_row_id(id), expected)
    }
}