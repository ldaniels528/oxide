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
    U8Kind = 0,
    F64Kind = 1,
    I64Kind = 2,
    U64Kind = 3,
    NaNKind = 4,
    I128Kind = 5,
    U128Kind = 6,
}

impl NumberKind {
    pub fn compute_fixed_size(&self) -> usize {
        match self {
            Self::U8Kind => 1,
            Self::F64Kind 
            | Self::I64Kind 
            | Self::U64Kind => 8,
            Self::I128Kind 
            | Self::U128Kind => 16,
            Self::NaNKind => 0,
        }
    }

    pub fn convert_from(&self, value: &TypedValue) -> std::io::Result<Numbers> {
        let result = match value {
            TypedValue::StringValue(s) => {
                match self {
                    Self::U8Kind => Numbers::F64Value(s.parse().map_err(|e| cnv_error!(e))?),
                    Self::F64Kind => Numbers::F64Value(s.parse().map_err(|e| cnv_error!(e))?),
                    Self::I64Kind => Numbers::I64Value(s.parse().map_err(|e| cnv_error!(e))?),
                    Self::I128Kind => Numbers::I128Value(s.parse().map_err(|e| cnv_error!(e))?),
                    Self::U64Kind => Numbers::U64Value(s.parse().map_err(|e| cnv_error!(e))?),
                    Self::U128Kind => Numbers::U128Value(s.parse().map_err(|e| cnv_error!(e))?),
                    Self::NaNKind => Numbers::NaNValue,
                }
            }
            z => return throw(TypeMismatch(TypeMismatchErrors::UnsupportedType(NumberType(Self::F64Kind), z.get_type())))
        };
        Ok(result)
    }

    /// decodes the typed value based on the supplied data type and buffer
    pub fn decode(&self, buffer: &Vec<u8>, offset: usize) -> Numbers {
        match self {
            Self::U8Kind => 
                Numbers::U8Value(buffer[offset].clone()),
            Self::F64Kind =>
                ByteCodeCompiler::decode_u8x8(buffer, offset, |b| Numbers::F64Value(f64::from_be_bytes(b))),
            Self::I64Kind =>
                ByteCodeCompiler::decode_u8x8(buffer, offset, |b| Numbers::I64Value(i64::from_be_bytes(b))),
            Self::I128Kind =>
                ByteCodeCompiler::decode_u8x16(buffer, offset, |b| Numbers::I128Value(i128::from_be_bytes(b))),
            Self::U64Kind =>
                ByteCodeCompiler::decode_u8x8(buffer, offset, |b| Numbers::U64Value(u64::from_be_bytes(b))),
            Self::U128Kind =>
                ByteCodeCompiler::decode_u8x16(buffer, offset, |b| Numbers::U128Value(u128::from_be_bytes(b))),
            Self::NaNKind =>
                Numbers::NaNValue,
        }
    }

    pub fn get_default_value(&self) -> Numbers {
        match self {
            Self::U8Kind => Numbers::U8Value(0),
            Self::F64Kind => Numbers::F64Value(0.),
            Self::I64Kind => Numbers::I64Value(0),
            Self::I128Kind => Numbers::I128Value(0),
            Self::NaNKind => Numbers::I64Value(0),
            Self::U64Kind => Numbers::U64Value(0),
            Self::U128Kind => Numbers::U128Value(0),
        }
    }

    pub fn get_type_name(&self) -> String {
        let name = match self {
            Self::U8Kind => "u8",
            Self::F64Kind => "f64",
            Self::I64Kind => "i64",
            Self::NaNKind => "NaN",
            Self::I128Kind => "i128",
            Self::U64Kind => "u64",
            Self::U128Kind => "u128",
        };
        name.to_string()
    }
}