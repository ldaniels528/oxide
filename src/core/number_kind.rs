#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
//  NumberKind enumeration
////////////////////////////////////////////////////////////////////

use crate::byte_code_compiler::ByteCodeCompiler;
use crate::numbers::Numbers;
use serde::{Deserialize, Serialize};

// Represents a numeric type or kind of value
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum NumberKind {
    F64Kind = 1,
    I64Kind = 2,
    I128Kind = 3,
    RowIdKind = 4,
    U128Kind = 5,
    NaNKind = 6,
}

impl NumberKind {
    pub fn compute_fixed_size(&self) -> usize {
        use NumberKind::*;
        match self {
            RowIdKind => 2,
            F64Kind | I64Kind => 8,
            I128Kind | U128Kind => 16,
            NaNKind => 0,
        }
    }

    /// decodes the typed value based on the supplied data type and buffer
    pub fn decode(&self, buffer: &Vec<u8>, offset: usize) -> Numbers {
        match self {
            NumberKind::RowIdKind => 
                ByteCodeCompiler::decode_u8x8(buffer, offset, |b| Numbers::RowId(u64::from_be_bytes(b))),
            NumberKind::F64Kind => 
                ByteCodeCompiler::decode_u8x8(buffer, offset, |b| Numbers::F64Value(f64::from_be_bytes(b))),
            NumberKind::I64Kind => 
                ByteCodeCompiler::decode_u8x8(buffer, offset, |b| Numbers::I64Value(i64::from_be_bytes(b))),
            NumberKind::I128Kind => 
                ByteCodeCompiler::decode_u8x16(buffer, offset, |b| Numbers::I128Value(i128::from_be_bytes(b))),
            NumberKind::U128Kind => 
                ByteCodeCompiler::decode_u8x16(buffer, offset, |b| Numbers::U128Value(u128::from_be_bytes(b))),
            NumberKind::NaNKind =>
                Numbers::NaNValue,
        }
    }

    pub fn get_default_value(&self) -> Numbers {
        match self {
            NumberKind::RowIdKind => Numbers::RowId(0),
            NumberKind::F64Kind => Numbers::F64Value(0.),
            NumberKind::I64Kind => Numbers::I64Value(0),
            NumberKind::I128Kind => Numbers::I128Value(0),
            NumberKind::U128Kind => Numbers::U128Value(0),
            NumberKind::NaNKind => Numbers::NaNValue
        }
    }

    pub fn get_type_name(&self) -> String {
        let name = match self {
            NumberKind::RowIdKind => "RowId",
            NumberKind::F64Kind => "f64",
            NumberKind::I64Kind => "i64",
            NumberKind::I128Kind => "i128",
            NumberKind::U128Kind => "u128",
            NumberKind::NaNKind => "NaN"
        };
        name.to_string()
    }
}