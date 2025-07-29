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
        let result = match value {
            TypedValue::StringValue(s) => match self {
                Self::AnyKind    => Numbers::I128Value(s.parse().map_err(|e| cnv_error!(e))?),
                Self::U8Kind     => Numbers::U8Value(s.parse().map_err(|e| cnv_error!(e))?),
                Self::I8Kind     => Numbers::I8Value(s.parse().map_err(|e| cnv_error!(e))?),
                Self::I16Kind    => Numbers::I16Value(s.parse().map_err(|e| cnv_error!(e))?),
                Self::U16Kind    => Numbers::U16Value(s.parse().map_err(|e| cnv_error!(e))?),
                Self::I32Kind    => Numbers::I32Value(s.parse().map_err(|e| cnv_error!(e))?),
                Self::U32Kind    => Numbers::U32Value(s.parse().map_err(|e| cnv_error!(e))?),
                Self::F64Kind    => Numbers::F64Value(s.parse().map_err(|e| cnv_error!(e))?),
                Self::I64Kind    => Numbers::I64Value(s.parse().map_err(|e| cnv_error!(e))?),
                Self::U64Kind    => Numbers::U64Value(s.parse().map_err(|e| cnv_error!(e))?),
                Self::I128Kind   => Numbers::I128Value(s.parse().map_err(|e| cnv_error!(e))?),
                Self::U128Kind   => Numbers::U128Value(s.parse().map_err(|e| cnv_error!(e))?),
                Self::NaNKind    => Numbers::NaNValue,
            },
            z => return throw(TypeMismatch(TypeMismatchErrors::UnsupportedType(NumberType(Self::F64Kind), z.get_type()))),
        };
        Ok(result)
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
            Self::AnyKind    => Numbers::I64Value(0),
            Self::U8Kind     => Numbers::U8Value(0),
            Self::I8Kind     => Numbers::I8Value(0),
            Self::I16Kind    => Numbers::I16Value(0),
            Self::U16Kind    => Numbers::U16Value(0),
            Self::I32Kind    => Numbers::I32Value(0),
            Self::U32Kind    => Numbers::U32Value(0),
            Self::F64Kind    => Numbers::F64Value(0.0),
            Self::I64Kind    => Numbers::I64Value(0),
            Self::U64Kind    => Numbers::U64Value(0),
            Self::I128Kind   => Numbers::I128Value(0),
            Self::U128Kind   => Numbers::U128Value(0),
            Self::NaNKind    => Numbers::NaNValue,
        }
    }

    pub fn get_type_name(&self) -> String {
        match self {
            Self::AnyKind    => "Any",
            Self::U8Kind     => "u8",
            Self::I8Kind     => "i8",
            Self::I16Kind    => "i16",
            Self::U16Kind    => "u16",
            Self::I32Kind    => "i32",
            Self::U32Kind    => "u32",
            Self::F64Kind    => "f64",
            Self::I64Kind    => "i64",
            Self::U64Kind    => "u64",
            Self::I128Kind   => "i128",
            Self::U128Kind   => "u128",
            Self::NaNKind    => "NaN",
        }
            .to_string()
    }
}

/// Unit tests
#[cfg(test)]
mod tests {
    use crate::number_kind::NumberKind::*;
    use crate::numbers::Numbers::*;

    #[test]
    fn test_decode_f64() {
        assert_eq!(F64Kind.decode(
            &vec![0xba, 0xbe, 0xfa, 0xce, 0xde, 0xad, 0xbe, 0xef], 0),
                   F64Value(-1.001008711249533e-25))
    }

    #[test]
    fn test_decode_i64() {
        assert_eq!(I64Kind.decode(
            &vec![0xba, 0xbe, 0xfa, 0xce, 0xde, 0xad, 0xbe, 0xef], 0),
                   I64Value(-4990275570673795345))
    }

    #[test]
    fn test_decode_i128() {
        assert_eq!(I128Kind.decode(
            &vec![
                0xba, 0xbe, 0xfa, 0xce, 0xde, 0xad, 0xbe, 0xef,
                0xba, 0xbe, 0xfa, 0xce, 0xde, 0xad, 0xbe, 0xef
            ], 0),
                   I128Value(-92054336309504384978794185377862271249))
    }

    #[test]
    fn test_decode_u8() {
        assert_eq!(U8Kind.decode(&vec![0x80], 0), U8Value(0x80))
    }

    #[test]
    fn test_decode_u64() {
        assert_eq!(U64Kind.decode(
            &vec![0xba, 0xbe, 0xfa, 0xce, 0xde, 0xad, 0xbe, 0xef], 0),
                   U64Value(0xbabe_face_dead_beef))
    }

    #[test]
    fn test_decode_u128() {
        assert_eq!(U128Kind.decode(
            &vec![
                0xba, 0xbe, 0xfa, 0xce, 0xde, 0xad, 0xbe, 0xef,
                0xba, 0xbe, 0xfa, 0xce, 0xde, 0xad, 0xbe, 0xef
            ], 0),
                   U128Value(0xbabe_face_dead_beef_babe_face_dead_beef))
    }

    #[test]
    fn test_decode_nan() {
        assert_eq!(NaNKind.decode(&vec![], 0), NaNValue)
    }
}