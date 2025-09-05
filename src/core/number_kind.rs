#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
//  NumberKind enumeration
////////////////////////////////////////////////////////////////////

use crate::byte_code_compiler::ByteCodeCompiler;
use crate::data_types::DataType::NumberType;
use crate::errors::Errors::TypeMismatch;
use crate::errors::{throw, TypeMismatchErrors};
use crate::numbers::Numbers;
use crate::typed_values::TypedValue;
use serde::{Deserialize, Serialize};
use shared_lib::cnv_error;

// Represents a numeric type or kind of value
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum NumberKind {
    AnyKind,
    I8Kind,
    U8Kind,
    I16Kind,
    U16Kind,
    I32Kind,
    U32Kind,
    F64Kind,
    I64Kind,
    U64Kind,
    I128Kind,
    U128Kind,
    NaNKind,
}

impl NumberKind {
    pub fn compute_fixed_size(&self) -> usize {
        match self {
            Self::U8Kind | Self::I8Kind => 1,
            Self::I16Kind | Self::U16Kind => 2,
            Self::I32Kind | Self::U32Kind => 4,
            Self::F64Kind | Self::I64Kind | Self::U64Kind => 8,
            Self::AnyKind | Self::I128Kind | Self::U128Kind => 16,
            Self::NaNKind => 0,
        }
    }

    pub fn convert_from(&self, value: &TypedValue) -> std::io::Result<Numbers> {
        Ok(match value {
            TypedValue::StringValue(s) =>
                match self {
                    Self::AnyKind => Numbers::I128Value(s.parse().map_err(|e| cnv_error!(e))?),
                    Self::U8Kind => Numbers::U8Value(s.parse().map_err(|e| cnv_error!(e))?),
                    Self::I8Kind => Numbers::I8Value(s.parse().map_err(|e| cnv_error!(e))?),
                    Self::I16Kind => Numbers::I16Value(s.parse().map_err(|e| cnv_error!(e))?),
                    Self::U16Kind => Numbers::U16Value(s.parse().map_err(|e| cnv_error!(e))?),
                    Self::I32Kind => Numbers::I32Value(s.parse().map_err(|e| cnv_error!(e))?),
                    Self::U32Kind => Numbers::U32Value(s.parse().map_err(|e| cnv_error!(e))?),
                    Self::F64Kind => Numbers::F64Value(s.parse().map_err(|e| cnv_error!(e))?),
                    Self::I64Kind => Numbers::I64Value(s.parse().map_err(|e| cnv_error!(e))?),
                    Self::U64Kind => Numbers::U64Value(s.parse().map_err(|e| cnv_error!(e))?),
                    Self::I128Kind => Numbers::I128Value(s.parse().map_err(|e| cnv_error!(e))?),
                    Self::U128Kind => Numbers::U128Value(s.parse().map_err(|e| cnv_error!(e))?),
                    Self::NaNKind => Numbers::NaNValue,
                }
            z => return throw(TypeMismatch(TypeMismatchErrors::UnsupportedType(NumberType(Self::F64Kind), z.get_type()))),
        })
    }

    /// Decodes the typed value from the supplied buffer using the offset
    pub fn decode(&self, buffer: &Vec<u8>, offset: usize) -> Numbers {
        match self {
            Self::AnyKind =>
                ByteCodeCompiler::decode_u8x16(buffer, offset, |b| Numbers::I128Value(i128::from_be_bytes(b))),
            Self::U8Kind =>
                Numbers::U8Value(buffer[offset]),
            Self::I8Kind =>
                Numbers::I8Value(buffer[offset] as i8),
            Self::I16Kind =>
                ByteCodeCompiler::decode_u8x2(buffer, offset, |b| Numbers::I16Value(i16::from_be_bytes(b))),
            Self::U16Kind =>
                ByteCodeCompiler::decode_u8x2(buffer, offset, |b| Numbers::U16Value(u16::from_be_bytes(b))),
            Self::I32Kind =>
                ByteCodeCompiler::decode_u8x4(buffer, offset, |b| Numbers::I32Value(i32::from_be_bytes(b))),
            Self::U32Kind =>
                ByteCodeCompiler::decode_u8x4(buffer, offset, |b| Numbers::U32Value(u32::from_be_bytes(b))),
            Self::F64Kind =>
                ByteCodeCompiler::decode_u8x8(buffer, offset, |b| Numbers::F64Value(f64::from_be_bytes(b))),
            Self::I64Kind =>
                ByteCodeCompiler::decode_u8x8(buffer, offset, |b| Numbers::I64Value(i64::from_be_bytes(b))),
            Self::U64Kind =>
                ByteCodeCompiler::decode_u8x8(buffer, offset, |b| Numbers::U64Value(u64::from_be_bytes(b))),
            Self::I128Kind =>
                ByteCodeCompiler::decode_u8x16(buffer, offset, |b| Numbers::I128Value(i128::from_be_bytes(b))),
            Self::U128Kind =>
                ByteCodeCompiler::decode_u8x16(buffer, offset, |b| Numbers::U128Value(u128::from_be_bytes(b))),
            Self::NaNKind =>
                Numbers::NaNValue,
        }
    }

    pub fn get_default_value(&self) -> Numbers {
        match self {
            Self::AnyKind => Numbers::I64Value(0),
            Self::U8Kind => Numbers::U8Value(0),
            Self::I8Kind => Numbers::I8Value(0),
            Self::I16Kind => Numbers::I16Value(0),
            Self::U16Kind => Numbers::U16Value(0),
            Self::I32Kind => Numbers::I32Value(0),
            Self::U32Kind => Numbers::U32Value(0),
            Self::F64Kind => Numbers::F64Value(0.0),
            Self::I64Kind => Numbers::I64Value(0),
            Self::U64Kind => Numbers::U64Value(0),
            Self::I128Kind => Numbers::I128Value(0),
            Self::U128Kind => Numbers::U128Value(0),
            Self::NaNKind => Numbers::NaNValue,
        }
    }

    pub fn get_type_name(&self) -> String {
        match self {
            Self::AnyKind => "Number",
            Self::U8Kind => "u8",
            Self::I8Kind => "i8",
            Self::I16Kind => "i16",
            Self::U16Kind => "u16",
            Self::I32Kind => "i32",
            Self::U32Kind => "u32",
            Self::F64Kind => "f64",
            Self::I64Kind => "i64",
            Self::U64Kind => "u64",
            Self::I128Kind => "i128",
            Self::U128Kind => "u128",
            Self::NaNKind => "NaN",
        }.to_string()
    }
}

/// Unit tests
#[cfg(test)]
mod tests {
    use crate::number_kind::NumberKind;
    use crate::number_kind::NumberKind::*;
    use crate::numbers::Numbers;
    use crate::numbers::Numbers::*;
    use crate::typed_values::TypedValue::StringValue;

    #[test]
    fn test_compute_fixed_size() {
        assert_eq!(AnyKind.compute_fixed_size(), 16);
        assert_eq!(NaNKind.compute_fixed_size(), 0);

        assert_eq!(I8Kind.compute_fixed_size(), 1);
        assert_eq!(I16Kind.compute_fixed_size(), 2);
        assert_eq!(I32Kind.compute_fixed_size(), 4);
        assert_eq!(I64Kind.compute_fixed_size(), 8);
        assert_eq!(I128Kind.compute_fixed_size(), 16);

        assert_eq!(U8Kind.compute_fixed_size(), 1);
        assert_eq!(U16Kind.compute_fixed_size(), 2);
        assert_eq!(U32Kind.compute_fixed_size(), 4);
        assert_eq!(U64Kind.compute_fixed_size(), 8);
        assert_eq!(U128Kind.compute_fixed_size(), 16);

        assert_eq!(F64Kind.compute_fixed_size(), 8);
    }

    #[test]
    fn test_convert_from_i8() {
        verify_convert_from("127", I8Kind, I8Value(127));
    }

    #[test]
    fn test_convert_from_i16() {
        verify_convert_from("32767", I16Kind, I16Value(32767));
    }

    #[test]
    fn test_convert_from_i32() {
        verify_convert_from("123456", I32Kind, I32Value(123456));
    }

    #[test]
    fn test_convert_from_i64() {
        verify_convert_from("123456", I64Kind, I64Value(123456));
    }

    #[test]
    fn test_convert_from_i128() {
        verify_convert_from("123456", I128Kind, I128Value(123456));
    }

    #[test]
    fn test_convert_from_u8() {
        verify_convert_from("255", U8Kind, U8Value(255));
    }

    #[test]
    fn test_convert_from_u16() {
        verify_convert_from("65535", U16Kind, U16Value(65535));
    }

    #[test]
    fn test_convert_from_u32() {
        verify_convert_from("123456", U32Kind, U32Value(123456));
    }

    #[test]
    fn test_convert_from_u64() {
        verify_convert_from("123456", U64Kind, U64Value(123456));
    }

    #[test]
    fn test_convert_from_u128() {
        verify_convert_from("123456", U128Kind, U128Value(123456));
    }

    #[test]
    fn test_encode_decode_nan() {
        verify_encode_decode(NaNValue, NaNKind);
    }

    #[test]
    fn test_encode_decode_f64() {
        verify_encode_decode(F64Value(std::f64::consts::PI), F64Kind);
    }

    #[test]
    fn test_encode_decode_i8() {
        verify_encode_decode(I8Value(127), I8Kind);
    }

    #[test]
    fn test_encode_decode_i16() {
        verify_encode_decode(I16Value(3456), I16Kind);
    }

    #[test]
    fn test_encode_decode_i32() {
        verify_encode_decode(I32Value(3456), I32Kind);
    }

    #[test]
    fn test_encode_decode_i64() {
        verify_encode_decode(I64Value(3456), I64Kind);
    }

    #[test]
    fn test_encode_decode_i128() {
        verify_encode_decode(I128Value(3456), I128Kind);
    }

    #[test]
    fn test_encode_decode_u8() {
        verify_encode_decode(U8Value(255), U8Kind);
    }

    #[test]
    fn test_encode_decode_u16() {
        verify_encode_decode(U16Value(65535), U16Kind);
    }

    #[test]
    fn test_encode_decode_u32() {
        verify_encode_decode(U32Value(3456), U32Kind);
    }

    #[test]
    fn test_encode_decode_u64() {
        verify_encode_decode(U64Value(3456), U64Kind);
    }

    #[test]
    fn test_encode_decode_u128() {
        verify_encode_decode(U128Value(3456), U128Kind);
    }

    fn verify_convert_from(number: &str, kind: NumberKind, expected: Numbers) {
        let value = kind.convert_from(&StringValue(number.into())).unwrap();
        assert_eq!(value, expected);
    }

    fn verify_encode_decode(value: Numbers, kind: NumberKind) {
        let bytes = value.encode();
        let decoded = kind.decode(&bytes, 0);
        assert_eq!(value, decoded);
    }

}