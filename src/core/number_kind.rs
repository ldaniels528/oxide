#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
//  NumberKind enumeration
////////////////////////////////////////////////////////////////////

use crate::byte_code_compiler::ByteCodeCompiler;
use crate::number_kind::NumberKind::{U128Kind, UUIDKind};
use crate::numbers::Numbers;
use crate::numbers::Numbers::*;
use chrono::Local;
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// Represents a numeric type or kind of value
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum NumberKind {
    RowIdKind = 15,
    DateKind = 0,
    F32Kind = 1,
    F64Kind = 2,
    I8Kind = 3,
    I16Kind = 4,
    I32Kind = 5,
    I64Kind = 6,
    I128Kind = 7,
    U8Kind = 8,
    U16Kind = 9,
    U32Kind = 10,
    U64Kind = 11,
    U128Kind = 12,
    UUIDKind = 13,
    NaNKind = 14,
}

impl NumberKind {
    pub fn compute_fixed_size(&self) -> usize {
        use NumberKind::*;
        match self {
            RowIdKind => 2,
            I8Kind | U8Kind => 1,
            I16Kind | U16Kind => 2,
            F32Kind | I32Kind | U32Kind => 4,
            DateKind | F64Kind | I64Kind | U64Kind => 8,
            I128Kind | U128Kind | UUIDKind => 16,
            NaNKind => 0,
        }
    }

    /// decodes the typed value based on the supplied data type and buffer
    pub fn decode(&self, buffer: &Vec<u8>, offset: usize) -> Numbers {
        match self {
            NumberKind::RowIdKind => ByteCodeCompiler::decode_u8x8(buffer, offset, |b| RowId(u64::from_be_bytes(b))),
            NumberKind::DateKind => ByteCodeCompiler::decode_u8x8(buffer, offset, |b| DateValue(i64::from_be_bytes(b))),
            NumberKind::F32Kind => ByteCodeCompiler::decode_u8x4(buffer, offset, |b| F32Value(f32::from_be_bytes(b))),
            NumberKind::F64Kind => ByteCodeCompiler::decode_u8x8(buffer, offset, |b| F64Value(f64::from_be_bytes(b))),
            NumberKind::I8Kind => ByteCodeCompiler::decode_u8(buffer, offset, |b| I8Value(b.to_i8().unwrap())),
            NumberKind::I16Kind => ByteCodeCompiler::decode_u8x2(buffer, offset, |b| I16Value(i16::from_be_bytes(b))),
            NumberKind::I32Kind => ByteCodeCompiler::decode_u8x4(buffer, offset, |b| I32Value(i32::from_be_bytes(b))),
            NumberKind::I64Kind => ByteCodeCompiler::decode_u8x8(buffer, offset, |b| I64Value(i64::from_be_bytes(b))),
            NumberKind::I128Kind => ByteCodeCompiler::decode_u8x16(buffer, offset, |b| I128Value(i128::from_be_bytes(b))),
            NumberKind::U8Kind => ByteCodeCompiler::decode_u8(buffer, offset, |b| U8Value(b)),
            NumberKind::U16Kind => ByteCodeCompiler::decode_u8x2(buffer, offset, |b| U16Value(u16::from_be_bytes(b))),
            NumberKind::U32Kind => ByteCodeCompiler::decode_u8x4(buffer, offset, |b| U32Value(u32::from_be_bytes(b))),
            NumberKind::U64Kind => ByteCodeCompiler::decode_u8x8(buffer, offset, |b| U64Value(u64::from_be_bytes(b))),
            NumberKind::U128Kind => ByteCodeCompiler::decode_u8x16(buffer, offset, |b| U128Value(u128::from_be_bytes(b))),
            NumberKind::UUIDKind => ByteCodeCompiler::decode_u8x16(buffer, offset, |b| UUIDValue(u128::from_be_bytes(b))),
            NumberKind::NaNKind => NaNValue,
        }
    }

    pub fn decode_buffer(&self, bcc: &mut ByteCodeCompiler) -> std::io::Result<Numbers> {
        let result = match self {
            NumberKind::RowIdKind => RowId(bcc.next_u64()),
            NumberKind::DateKind => DateValue(bcc.next_i64()),
            NumberKind::F32Kind => F32Value(bcc.next_f32()),
            NumberKind::F64Kind => F64Value(bcc.next_f64()),
            NumberKind::I8Kind => I8Value(bcc.next_i8()),
            NumberKind::I16Kind => I16Value(bcc.next_i16()),
            NumberKind::I32Kind => I32Value(bcc.next_i32()),
            NumberKind::I64Kind => I64Value(bcc.next_i64()),
            NumberKind::I128Kind => I128Value(bcc.next_i128()),
            NumberKind::NaNKind => NaNValue,
            NumberKind::U8Kind => U8Value(bcc.next_u8()),
            NumberKind::U16Kind => U16Value(bcc.next_u16()),
            NumberKind::U32Kind => U32Value(bcc.next_u32()),
            NumberKind::U64Kind => U64Value(bcc.next_u64()),
            NumberKind::U128Kind => U128Value(bcc.next_u128()),
            NumberKind::UUIDKind => UUIDValue(bcc.next_u128()),
        };
        Ok(result)
    }

    pub fn get_default_value(&self) -> Numbers {
        match self {
            NumberKind::RowIdKind => RowId(0),
            NumberKind::DateKind => DateValue(Local::now().timestamp_millis()),
            NumberKind::F32Kind => F32Value(0.),
            NumberKind::F64Kind => F64Value(0.),
            NumberKind::I8Kind => I8Value(0),
            NumberKind::I16Kind => I16Value(0),
            NumberKind::I32Kind => I32Value(0),
            NumberKind::I64Kind => I64Value(0),
            NumberKind::I128Kind => I128Value(0),
            NumberKind::U8Kind => U8Value(0),
            NumberKind::U16Kind => U16Value(0),
            NumberKind::U32Kind => U32Value(0),
            NumberKind::U64Kind => U64Value(0),
            NumberKind::U128Kind => U128Value(0),
            NumberKind::UUIDKind => UUIDValue(Uuid::new_v4().as_u128()),
            NumberKind::NaNKind => NaNValue
        }
    }

    pub fn get_type_name(&self) -> String {
        use NumberKind::*;
        let name = match self {
            RowIdKind => "RowId",
            DateKind => "Date",
            F32Kind => "f32",
            F64Kind => "f64",
            I8Kind => "i8",
            I16Kind => "i16",
            I32Kind => "i32",
            I64Kind => "i64",
            I128Kind => "i128",
            U8Kind => "u8",
            U16Kind => "u16",
            U32Kind => "u32",
            U64Kind => "u64",
            U128Kind => "u128",
            UUIDKind => "UUID",
            NaNKind => "NaN"
        };
        name.to_string()
    }
}