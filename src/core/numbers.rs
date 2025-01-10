#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Numbers class
////////////////////////////////////////////////////////////////////

use crate::number_kind::NumberKind;
use crate::number_kind::NumberKind::*;
use crate::numbers::Numbers::*;
use crate::typed_values::TypedValue;
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::Bound;
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::ops::*;

/// Represents a numeric value
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum Numbers {
    Ack,
    RowId(u64),
    RowsAffected(i64),
    DateValue(i64),
    F32Value(f32),
    F64Value(f64),
    I8Value(i8),
    I16Value(i16),
    I32Value(i32),
    I64Value(i64),
    I128Value(i128),
    U8Value(u8),
    U16Value(u16),
    U32Value(u32),
    U64Value(u64),
    U128Value(u128),
    UUIDValue(u128),
    NaNValue,
}

impl Eq for Numbers {}

impl Ord for Numbers {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

impl Hash for Numbers {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Numbers::Ack => 1.hash(state),
            Numbers::RowId(id) => id.hash(state),
            Numbers::RowsAffected(n) => n.hash(state),
            Numbers::DateValue(t) => t.hash(state),
            Numbers::F32Value(v) => v.to_bits().hash(state), // Convert to bits for hashing
            Numbers::F64Value(v) => v.to_bits().hash(state), // Convert to bits for hashing
            Numbers::I8Value(v) => v.hash(state),
            Numbers::I16Value(v) => v.hash(state),
            Numbers::I32Value(v) => v.hash(state),
            Numbers::I64Value(v) => v.hash(state),
            Numbers::I128Value(v) => v.hash(state),
            Numbers::U8Value(v) => v.hash(state),
            Numbers::U16Value(v) => v.hash(state),
            Numbers::U32Value(v) => v.hash(state),
            Numbers::U64Value(v) => v.hash(state),
            Numbers::U128Value(v) => v.hash(state),
            Numbers::UUIDValue(v) => v.hash(state),
            Numbers::NaNValue => 0.hash(state), // Use a fixed hash for NaN values
        }
    }
}

impl Numbers {

    ////////////////////////////////////////////////////////////////////
    //  STATIC METHODS
    ////////////////////////////////////////////////////////////////////

    /// decodes the typed value based on the supplied data type and buffer
    pub fn decode(buffer: &Vec<u8>, offset: usize, kind: NumberKind) -> Numbers {
        kind.decode(buffer, offset)
    }

    pub fn from_string(number_str: String) -> Numbers {
        match number_str.parse::<f64>() {
            Ok(num) => match number_str.parse::<i64>() {
                Ok(num) => I64Value(num),
                Err(_) => F64Value(num),
            }
            Err(_) => NaNValue,
        }
    }

    ////////////////////////////////////////////////////////////////////
    //  INSTANCE METHODS
    ////////////////////////////////////////////////////////////////////

    /// encodes the numeric value
    pub fn encode(&self) -> Vec<u8> {
        use Numbers::*;
        match self.to_owned() {
            Ack => vec![],
            RowId(id) => id.to_be_bytes().to_vec(),
            RowsAffected(n) => n.to_be_bytes().to_vec(),
            DateValue(number) => number.to_be_bytes().to_vec(),
            F32Value(number) => number.to_be_bytes().to_vec(),
            F64Value(number) => number.to_be_bytes().to_vec(),
            I8Value(number) => number.to_be_bytes().to_vec(),
            I16Value(number) => number.to_be_bytes().to_vec(),
            I32Value(number) => number.to_be_bytes().to_vec(),
            I64Value(number) => number.to_be_bytes().to_vec(),
            I128Value(number) => number.to_be_bytes().to_vec(),
            U8Value(number) => number.to_be_bytes().to_vec(),
            U16Value(number) => number.to_be_bytes().to_vec(),
            U32Value(number) => number.to_be_bytes().to_vec(),
            U64Value(number) => number.to_be_bytes().to_vec(),
            U128Value(number) => number.to_be_bytes().to_vec(),
            UUIDValue(number) => number.to_be_bytes().to_vec(),
            NaNValue => Vec::new(),
        }
    }

    pub fn get_type_name(&self) -> String {
        let result = match *self {
            Ack => "Ack",
            RowId(..) => "RowId",
            RowsAffected(..) => "RowsAffected",
            DateValue(..) => "Date",
            F32Value(..) => "f32",
            F64Value(..) => "f64",
            I8Value(..) => "i8",
            I16Value(..) => "i16",
            I32Value(..) => "i32",
            I64Value(..) => "i64",
            I128Value(..) => "i128",
            NaNValue => "NaN",
            U8Value(..) => "u8",
            U16Value(..) => "u16",
            U32Value(..) => "u32",
            U64Value(..) => "u64",
            U128Value(..) => "u128",
            UUIDValue(..) => "UUID",
        };
        result.to_string()
    }

    pub fn is_effectively_zero(&self) -> bool {
        match *self {
            Ack => true,
            RowId(id) => id == 0,
            RowsAffected(n) => n == 0,
            DateValue(n) => n == 0,
            F32Value(n) => n == 0.,
            F64Value(n) => n == 0.,
            I8Value(n) => n == 0,
            I16Value(n) => n == 0,
            I32Value(n) => n == 0,
            I64Value(n) => n == 0,
            I128Value(n) => n == 0,
            NaNValue => true,
            U8Value(n) => n == 0,
            U16Value(n) => n == 0,
            U32Value(n) => n == 0,
            U64Value(n) => n == 0,
            U128Value(n) => n == 0,
            UUIDValue(n) => n == 0,
        }
    }

    pub fn pow(&self, rhs: &Self) -> Self {
        F64Value(num_traits::pow(self.to_f64(), rhs.to_usize()))
    }

    pub fn kind(&self) -> NumberKind {
        match *self {
            Ack => AckKind,
            RowId(..) => RowIdKind,
            RowsAffected(..) => RowsAffectedKind,
            DateValue(..) => DateKind,
            F32Value(..) => F32Kind,
            F64Value(..) => F64Kind,
            I8Value(..) => I8Kind,
            I16Value(..) => I16Kind,
            I32Value(..) => I32Kind,
            I64Value(..) => I64Kind,
            I128Value(..) => I128Kind,
            NaNValue => NaNKind,
            U8Value(..) => U8Kind,
            U16Value(..) => U16Kind,
            U32Value(..) => U32Kind,
            U64Value(..) => U64Kind,
            U128Value(..) => U128Kind,
            UUIDValue(..) => UUIDKind,
        }
    }

    pub fn to_f32(&self) -> f32 {
        match *self {
            Ack => 0.,
            RowId(id) => id as f32,
            RowsAffected(n) => n as f32,
            DateValue(n) => n as f32,
            F32Value(n) => n,
            F64Value(n) => n as f32,
            I8Value(n) => n as f32,
            I16Value(n) => n as f32,
            I32Value(n) => n as f32,
            I64Value(n) => n as f32,
            I128Value(n) => n as f32,
            NaNValue => 0.0,
            U8Value(n) => n as f32,
            U16Value(n) => n as f32,
            U32Value(n) => n as f32,
            U64Value(n) => n as f32,
            U128Value(n) => n as f32,
            UUIDValue(n) => n as f32,
        }
    }

    pub fn to_f64(&self) -> f64 {
        match *self {
            Ack => 0.,
            RowId(id) => id as f64,
            RowsAffected(n) => n as f64,
            DateValue(t) => t as f64,
            F32Value(n) => n as f64,
            F64Value(n) => n,
            I8Value(n) => n as f64,
            I16Value(n) => n as f64,
            I32Value(n) => n as f64,
            I64Value(n) => n as f64,
            I128Value(n) => n as f64,
            NaNValue => 0.0,
            U8Value(n) => n as f64,
            U16Value(n) => n as f64,
            U32Value(n) => n as f64,
            U64Value(n) => n as f64,
            U128Value(n) => n as f64,
            UUIDValue(n) => n as f64,
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Ack => serde_json::json!(0),
            RowId(id) => serde_json::json!(id),
            RowsAffected(n) => serde_json::json!(n),
            DateValue(ms) => serde_json::json!(TypedValue::millis_to_iso_date(*ms)
                .unwrap_or(ms.to_string())),
            F32Value(number) => serde_json::json!(number),
            F64Value(number) => serde_json::json!(number),
            I8Value(number) => serde_json::json!(number),
            I16Value(number) => serde_json::json!(number),
            I32Value(number) => serde_json::json!(number),
            I64Value(number) => serde_json::json!(number),
            I128Value(number) => serde_json::json!(number),
            NaNValue => serde_json::json!("NaN"),
            U8Value(number) => serde_json::json!(number),
            U16Value(number) => serde_json::json!(number),
            U32Value(number) => serde_json::json!(number),
            U64Value(number) => serde_json::json!(number),
            U128Value(number) => serde_json::json!(number),
            UUIDValue(number) => serde_json::json!(number),
        }
    }

    pub fn to_i8(&self) -> i8 {
        match *self {
            Ack => 0,
            RowId(id) => id as i8,
            RowsAffected(n) => n as i8,
            DateValue(t) => t as i8,
            F32Value(n) => n as i8,
            F64Value(n) => n as i8,
            I8Value(n) => n,
            I16Value(n) => n as i8,
            I32Value(n) => n as i8,
            I64Value(n) => n as i8,
            I128Value(n) => n as i8,
            NaNValue => 0i8,
            U8Value(n) => n as i8,
            U16Value(n) => n as i8,
            U32Value(n) => n as i8,
            U64Value(n) => n as i8,
            U128Value(n) => n as i8,
            UUIDValue(n) => n as i8,
        }
    }

    pub fn to_i16(&self) -> i16 {
        match *self {
            Ack => 0,
            RowId(id) => id as i16,
            RowsAffected(n) => n as i16,
            DateValue(t) => t as i16,
            F32Value(n) => n as i16,
            F64Value(n) => n as i16,
            I8Value(n) => n as i16,
            I16Value(n) => n,
            I32Value(n) => n as i16,
            I64Value(n) => n as i16,
            I128Value(n) => n as i16,
            NaNValue => 0i16,
            U8Value(n) => n as i16,
            U16Value(n) => n as i16,
            U32Value(n) => n as i16,
            U64Value(n) => n as i16,
            U128Value(n) => n as i16,
            UUIDValue(n) => n as i16,
        }
    }

    pub fn to_i32(&self) -> i32 {
        match *self {
            Ack => 0,
            RowId(id) => id as i32,
            RowsAffected(n) => n as i32,
            DateValue(t) => t as i32,
            F32Value(n) => n as i32,
            F64Value(n) => n as i32,
            I8Value(n) => n as i32,
            I16Value(n) => n as i32,
            I32Value(n) => n,
            I64Value(n) => n as i32,
            I128Value(n) => n as i32,
            NaNValue => 0i32,
            U8Value(n) => n as i32,
            U16Value(n) => n as i32,
            U32Value(n) => n as i32,
            U64Value(n) => n as i32,
            U128Value(n) => n as i32,
            UUIDValue(n) => n as i32,
        }
    }

    pub fn to_i64(&self) -> i64 {
        match *self {
            Ack => 0,
            RowId(id) => id as i64,
            RowsAffected(n) => n as i64,
            DateValue(t) => t as i64,
            F32Value(n) => n as i64,
            F64Value(n) => n as i64,
            I8Value(n) => n as i64,
            I16Value(n) => n as i64,
            I32Value(n) => n as i64,
            I64Value(n) => n,
            I128Value(n) => n as i64,
            NaNValue => 0i64,
            U8Value(n) => n as i64,
            U16Value(n) => n as i64,
            U32Value(n) => n as i64,
            U64Value(n) => n as i64,
            U128Value(n) => n as i64,
            UUIDValue(n) => n as i64,
        }
    }

    pub fn to_i128(&self) -> i128 {
        match *self {
            Ack => 0,
            RowId(id) => id as i128,
            RowsAffected(n) => n as i128,
            DateValue(t) => t as i128,
            F32Value(n) => n as i128,
            F64Value(n) => n as i128,
            I8Value(n) => n as i128,
            I16Value(n) => n as i128,
            I32Value(n) => n as i128,
            I64Value(n) => n as i128,
            I128Value(n) => n,
            NaNValue => 0i128,
            U8Value(n) => n as i128,
            U16Value(n) => n as i128,
            U32Value(n) => n as i128,
            U64Value(n) => n as i128,
            U128Value(n) => n as i128,
            UUIDValue(n) => n as i128,
        }
    }

    pub fn to_u8(&self) -> u8 {
        match *self {
            Ack => 0,
            RowId(id) => id as u8,
            RowsAffected(n) => n as u8,
            DateValue(t) => t as u8,
            F32Value(n) => n as u8,
            F64Value(n) => n as u8,
            I8Value(n) => n as u8,
            I16Value(n) => n as u8,
            I32Value(n) => n as u8,
            I64Value(n) => n as u8,
            I128Value(n) => n as u8,
            NaNValue => 0u8,
            U8Value(n) => n,
            U16Value(n) => n as u8,
            U32Value(n) => n as u8,
            U64Value(n) => n as u8,
            U128Value(n) => n as u8,
            UUIDValue(n) => n as u8,
        }
    }

    pub fn to_u16(&self) -> u16 {
        match *self {
            Ack => 0,
            RowId(id) => id as u16,
            RowsAffected(n) => n as u16,
            DateValue(t) => t as u16,
            F32Value(n) => n as u16,
            F64Value(n) => n as u16,
            I8Value(n) => n as u16,
            I16Value(n) => n as u16,
            I32Value(n) => n as u16,
            I64Value(n) => n as u16,
            I128Value(n) => n as u16,
            NaNValue => 0u16,
            U8Value(n) => n as u16,
            U16Value(n) => n,
            U32Value(n) => n as u16,
            U64Value(n) => n as u16,
            U128Value(n) => n as u16,
            UUIDValue(n) => n as u16,
        }
    }

    pub fn to_u32(&self) -> u32 {
        match *self {
            Ack => 0,
            RowId(id) => id as u32,
            RowsAffected(n) => n as u32,
            DateValue(t) => t as u32,
            F32Value(n) => n as u32,
            F64Value(n) => n as u32,
            I8Value(n) => n as u32,
            I16Value(n) => n as u32,
            I32Value(n) => n as u32,
            I64Value(n) => n as u32,
            I128Value(n) => n as u32,
            NaNValue => 0u32,
            U8Value(n) => n as u32,
            U16Value(n) => n as u32,
            U32Value(n) => n,
            U64Value(n) => n as u32,
            U128Value(n) => n as u32,
            UUIDValue(n) => n as u32,
        }
    }

    pub fn to_u64(&self) -> u64 {
        match *self {
            Ack => 0,
            RowId(id) => id as u64,
            RowsAffected(n) => n as u64,
            DateValue(t) => t as u64,
            F32Value(n) => n as u64,
            F64Value(n) => n as u64,
            I8Value(n) => n as u64,
            I16Value(n) => n as u64,
            I32Value(n) => n as u64,
            I64Value(n) => n as u64,
            I128Value(n) => n as u64,
            NaNValue => 0u64,
            U8Value(n) => n as u64,
            U16Value(n) => n as u64,
            U32Value(n) => n as u64,
            U64Value(n) => n,
            U128Value(n) => n as u64,
            UUIDValue(n) => n as u64,
        }
    }

    pub fn to_u128(&self) -> u128 {
        match *self {
            Ack => 0,
            RowId(id) => id as u128,
            RowsAffected(n) => n as u128,
            DateValue(t) => t as u128,
            F32Value(n) => n as u128,
            F64Value(n) => n as u128,
            I8Value(n) => n as u128,
            I16Value(n) => n as u128,
            I32Value(n) => n as u128,
            I64Value(n) => n as u128,
            I128Value(n) => n as u128,
            NaNValue => 0,
            U8Value(n) => n as u128,
            U16Value(n) => n as u128,
            U32Value(n) => n as u128,
            U64Value(n) => n as u128,
            U128Value(n) => n as u128,
            UUIDValue(n) => n as u128,
        }
    }

    pub fn to_usize(&self) -> usize {
        match *self {
            Ack => 0,
            RowId(id) => id as usize,
            RowsAffected(n) => n as usize,
            DateValue(t) => t as usize,
            F32Value(n) => n as usize,
            F64Value(n) => n as usize,
            I8Value(n) => n as usize,
            I16Value(n) => n as usize,
            I32Value(n) => n as usize,
            I64Value(n) => n as usize,
            I128Value(n) => n as usize,
            NaNValue => 0usize,
            U8Value(n) => n as usize,
            U16Value(n) => n as usize,
            U32Value(n) => n as usize,
            U64Value(n) => n as usize,
            U128Value(n) => n as usize,
            UUIDValue(n) => n as usize,
        }
    }

    pub fn unwrap_value(&self) -> String {
        fn u128_to_uuid(uuid: u128) -> String {
            // Extract each group using bit shifts and masks
            let time_low = (uuid >> 96) as u32;
            let time_mid = (uuid >> 80) as u16;
            let time_hi_and_version = (uuid >> 64) as u16;
            let clk_seq = (uuid >> 48) as u16;
            let node = uuid as u64 & 0xFFFFFFFFFFFF;
            // Format into UUID string
            format!(
                "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
                time_low, time_mid, time_hi_and_version, clk_seq, node
            )
        }
        match self {
            Ack => "Ack".into(),
            RowId(id) => id.to_string(),
            RowsAffected(n) => n.to_string(),
            DateValue(ms) => TypedValue::millis_to_iso_date(*ms).unwrap_or("".into()),
            F32Value(number) => if *number == 0. { "0.0".into() } else { number.to_string() },
            F64Value(number) => if *number == 0. { "0.0".into() } else { number.to_string() },
            I8Value(number) => number.to_string(),
            I16Value(number) => number.to_string(),
            I32Value(number) => number.to_string(),
            I64Value(number) => number.to_string(),
            I128Value(number) => number.to_string(),
            NaNValue => "NaN".to_string(),
            U8Value(number) => number.to_string(),
            U16Value(number) => number.to_string(),
            U32Value(number) => number.to_string(),
            U64Value(number) => number.to_string(),
            U128Value(number) => u128_to_uuid(*number),
            UUIDValue(guid) => u128_to_uuid(*guid)
        }
    }
}

impl Add for Numbers {
    type Output = Numbers;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (F32Value(a), F32Value(b)) => F32Value(a + b),
            (F64Value(a), F64Value(b)) => F64Value(a + b),
            (I128Value(a), I128Value(b)) => I128Value(a + b),
            (I64Value(a), I64Value(b)) => I64Value(a + b),
            (I32Value(a), I32Value(b)) => I32Value(a + b),
            (I16Value(a), I16Value(b)) => I16Value(a + b),
            (I8Value(a), I8Value(b)) => I8Value(a + b),
            (NaNValue, _) => NaNValue,
            (_, NaNValue) => NaNValue,
            (U128Value(a), U128Value(b)) => U128Value(a + b),
            (U64Value(a), U64Value(b)) => U64Value(a + b),
            (U32Value(a), U32Value(b)) => U32Value(a + b),
            (U16Value(a), U16Value(b)) => U16Value(a + b),
            (U8Value(a), U8Value(b)) => U8Value(a + b),
            (a, b) => F64Value(a.to_f64() + b.to_f64())
        }
    }
}

impl BitAnd for Numbers {
    type Output = Numbers;

    fn bitand(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (F32Value(a), F32Value(b)) => U64Value((a as u64) & (b as u64)),
            (F64Value(a), F64Value(b)) => U64Value((a as u64) & (b as u64)),
            (I128Value(a), I128Value(b)) => I128Value(a & b),
            (I64Value(a), I64Value(b)) => I64Value(a & b),
            (I32Value(a), I32Value(b)) => I32Value(a & b),
            (I16Value(a), I16Value(b)) => I16Value(a & b),
            (I8Value(a), I8Value(b)) => I8Value(a & b),
            (NaNValue, _) => NaNValue,
            (_, NaNValue) => NaNValue,
            (U128Value(a), U128Value(b)) => U128Value(a & b),
            (U64Value(a), U64Value(b)) => U64Value(a & b),
            (U32Value(a), U32Value(b)) => U32Value(a & b),
            (U16Value(a), U16Value(b)) => U16Value(a & b),
            (U8Value(a), U8Value(b)) => U8Value(a & b),
            (a, b) => U64Value(a.to_u64() & b.to_u64()),
        }
    }
}

impl BitOr for Numbers {
    type Output = Numbers;

    fn bitor(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (F32Value(a), F32Value(b)) => U64Value((a as u64) | (b as u64)),
            (F64Value(a), F64Value(b)) => U64Value((a as u64) | (b as u64)),
            (I128Value(a), I128Value(b)) => I128Value(a | b),
            (I64Value(a), I64Value(b)) => I64Value(a | b),
            (I32Value(a), I32Value(b)) => I32Value(a | b),
            (I16Value(a), I16Value(b)) => I16Value(a | b),
            (I8Value(a), I8Value(b)) => I8Value(a | b),
            (NaNValue, _) => NaNValue,
            (_, NaNValue) => NaNValue,
            (U128Value(a), U128Value(b)) => U128Value(a | b),
            (U64Value(a), U64Value(b)) => U64Value(a | b),
            (U32Value(a), U32Value(b)) => U32Value(a | b),
            (U16Value(a), U16Value(b)) => U16Value(a | b),
            (U8Value(a), U8Value(b)) => U8Value(a | b),
            (a, b) => U64Value(a.to_u64() | b.to_u64()),
        }
    }
}

impl BitXor for Numbers {
    type Output = Numbers;

    fn bitxor(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (F32Value(a), F32Value(b)) => U64Value((a as u64) ^ (b as u64)),
            (F64Value(a), F64Value(b)) => U64Value((a as u64) ^ (b as u64)),
            (I128Value(a), I128Value(b)) => I128Value(a ^ b),
            (I64Value(a), I64Value(b)) => I64Value(a ^ b),
            (I32Value(a), I32Value(b)) => I32Value(a ^ b),
            (I16Value(a), I16Value(b)) => I16Value(a ^ b),
            (I8Value(a), I8Value(b)) => I8Value(a ^ b),
            (NaNValue, _) => NaNValue,
            (_, NaNValue) => NaNValue,
            (U128Value(a), U128Value(b)) => U128Value(a ^ b),
            (U64Value(a), U64Value(b)) => U64Value(a ^ b),
            (U32Value(a), U32Value(b)) => U32Value(a ^ b),
            (U16Value(a), U16Value(b)) => U16Value(a ^ b),
            (U8Value(a), U8Value(b)) => U8Value(a ^ b),
            (a, b) => U64Value(a.to_u64() ^ b.to_u64()),
        }
    }
}

impl Display for Numbers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.unwrap_value())
    }
}

impl Div for Numbers {
    type Output = Numbers;

    fn div(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (_, b) if b.is_effectively_zero() => NaNValue,
            (F32Value(a), F32Value(b)) => F32Value(a / b),
            (F64Value(a), F64Value(b)) => F64Value(a / b),
            (I128Value(a), I128Value(b)) => I128Value(a / b),
            (I64Value(a), I64Value(b)) => I64Value(a / b),
            (I32Value(a), I32Value(b)) => I32Value(a / b),
            (I16Value(a), I16Value(b)) => I16Value(a / b),
            (I8Value(a), I8Value(b)) => I8Value(a / b),
            (NaNValue, _) => NaNValue,
            (_, NaNValue) => NaNValue,
            (U128Value(a), U128Value(b)) => U128Value(a / b),
            (U64Value(a), U64Value(b)) => U64Value(a / b),
            (U32Value(a), U32Value(b)) => U32Value(a / b),
            (U16Value(a), U16Value(b)) => U16Value(a / b),
            (U8Value(a), U8Value(b)) => U8Value(a / b),
            (a, b) => F64Value(a.to_f64() / b.to_f64())
        }
    }
}

impl Mul for Numbers {
    type Output = Numbers;

    fn mul(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (F32Value(a), F32Value(b)) => F32Value(a * b),
            (F64Value(a), F64Value(b)) => F64Value(a * b),
            (I128Value(a), I128Value(b)) => I128Value(a * b),
            (I64Value(a), I64Value(b)) => I64Value(a * b),
            (I32Value(a), I32Value(b)) => I32Value(a * b),
            (I16Value(a), I16Value(b)) => I16Value(a * b),
            (I8Value(a), I8Value(b)) => I8Value(a * b),
            (NaNValue, _) => NaNValue,
            (_, NaNValue) => NaNValue,
            (U128Value(a), U128Value(b)) => U128Value(a * b),
            (U64Value(a), U64Value(b)) => U64Value(a * b),
            (U32Value(a), U32Value(b)) => U32Value(a * b),
            (U16Value(a), U16Value(b)) => U16Value(a * b),
            (U8Value(a), U8Value(b)) => U8Value(a * b),
            (a, b) => F64Value(a.to_f64() * b.to_f64())
        }
    }
}

impl Neg for Numbers {
    type Output = Numbers;

    fn neg(self) -> Self::Output {
        match self {
            Ack => I64Value(0),
            RowId(n) => I64Value(-(n as i64)),
            RowsAffected(n) => I64Value(-n),
            DateValue(d) => I64Value(-d),
            F32Value(n) => F32Value(-n),
            F64Value(n) => F64Value(-n),
            I8Value(n) => I8Value(-n),
            I16Value(n) => I16Value(-n),
            I32Value(n) => I32Value(-n),
            I64Value(n) => I64Value(-n),
            I128Value(n) => I128Value(-n),
            NaNValue => NaNValue,
            U8Value(n) => I8Value(-(n as i8)),
            U16Value(n) => I16Value(-(n as i16)),
            U32Value(n) => I32Value(-(n as i32)),
            U64Value(n) => I64Value(-(n as i64)),
            U128Value(n) => I128Value(-(n as i128)),
            UUIDValue(u) => I128Value(-(u as i128)),
        }
    }
}

impl Not for Numbers {
    type Output = Numbers;

    fn not(self) -> Self::Output {
        match self {
            Ack => I64Value(0),
            RowId(n) => I64Value(-(n as i64)),
            RowsAffected(n) => I64Value(-(n as i64)),
            DateValue(d) => I64Value(-d),
            F32Value(n) => F32Value(-n),
            F64Value(n) => F64Value(-n),
            I8Value(n) => I8Value(-n),
            I16Value(n) => I16Value(-n),
            I32Value(n) => I32Value(-n),
            I64Value(n) => I64Value(-n),
            I128Value(n) => I128Value(-n),
            NaNValue => NaNValue,
            U8Value(n) => I8Value(-(n as i8)),
            U16Value(n) => I16Value(-(n as i16)),
            U32Value(n) => I32Value(-(n as i32)),
            U64Value(n) => I64Value(-(n as i64)),
            U128Value(n) => I128Value(-(n as i128)),
            UUIDValue(u) => I128Value(-(u as i128)),
        }
    }
}

impl RangeBounds<Numbers> for Numbers {
    fn start_bound(&self) -> Bound<&Numbers> {
        std::ops::Bound::Included(&self)
    }

    fn end_bound(&self) -> Bound<&Numbers> {
        std::ops::Bound::Excluded(&self)
    }
}

impl Rem for Numbers {
    type Output = Numbers;

    fn rem(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (F32Value(a), F32Value(b)) => F32Value(a % b),
            (F64Value(a), F64Value(b)) => F64Value(a % b),
            (I128Value(a), I128Value(b)) => I128Value(a % b),
            (I64Value(a), I64Value(b)) => I64Value(a % b),
            (I32Value(a), I32Value(b)) => I32Value(a % b),
            (I16Value(a), I16Value(b)) => I16Value(a % b),
            (I8Value(a), I8Value(b)) => I8Value(a % b),
            (NaNValue, _) => NaNValue,
            (_, NaNValue) => NaNValue,
            (U128Value(a), U128Value(b)) => U128Value(a % b),
            (U64Value(a), U64Value(b)) => U64Value(a % b),
            (U32Value(a), U32Value(b)) => U32Value(a % b),
            (U16Value(a), U16Value(b)) => U16Value(a % b),
            (U8Value(a), U8Value(b)) => U8Value(a % b),
            (a, b) => F64Value(a.to_f64() % b.to_f64())
        }
    }
}

impl PartialOrd for Numbers {
    fn partial_cmp(&self, rhs: &Self) -> Option<Ordering> {
        match (self, rhs) {
            (F32Value(a), F32Value(b)) => a.partial_cmp(b),
            (F64Value(a), F64Value(b)) => a.partial_cmp(b),
            (I8Value(a), I8Value(b)) => a.partial_cmp(b),
            (I16Value(a), I16Value(b)) => a.partial_cmp(b),
            (I32Value(a), I32Value(b)) => a.partial_cmp(b),
            (I64Value(a), I64Value(b)) => a.partial_cmp(b),
            (I128Value(a), I128Value(b)) => a.partial_cmp(b),
            (U8Value(a), U8Value(b)) => a.partial_cmp(b),
            (U16Value(a), U16Value(b)) => a.partial_cmp(b),
            (U32Value(a), U32Value(b)) => a.partial_cmp(b),
            (U64Value(a), U64Value(b)) => a.partial_cmp(b),
            (U128Value(a), U128Value(b)) => a.partial_cmp(b),
            (a, b) => a.to_f64().partial_cmp(&b.to_f64())
        }
    }
}

impl Shl for Numbers {
    type Output = Numbers;

    fn shl(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (F32Value(a), F32Value(b)) => U64Value((a as u64) << (b as u64)),
            (F64Value(a), F64Value(b)) => U64Value((a as u64) << (b as u64)),
            (I128Value(a), I128Value(b)) => I128Value(a << b),
            (I64Value(a), I64Value(b)) => I64Value(a << b),
            (I32Value(a), I32Value(b)) => I32Value(a << b),
            (I16Value(a), I16Value(b)) => I16Value(a << b),
            (I8Value(a), I8Value(b)) => I8Value(a << b),
            (NaNValue, _) => NaNValue,
            (_, NaNValue) => NaNValue,
            (U128Value(a), U128Value(b)) => U128Value(a << b),
            (U64Value(a), U64Value(b)) => U64Value(a << b),
            (U32Value(a), U32Value(b)) => U32Value(a << b),
            (U16Value(a), U16Value(b)) => U16Value(a << b),
            (U8Value(a), U8Value(b)) => U8Value(a << b),
            (a, b) => U64Value(a.to_u64() << b.to_u64()),
        }
    }
}

impl Shr for Numbers {
    type Output = Numbers;

    fn shr(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (F32Value(a), F32Value(b)) => U64Value((a as u64) >> (b as u64)),
            (F64Value(a), F64Value(b)) => U64Value((a as u64) >> (b as u64)),
            (I128Value(a), I128Value(b)) => I128Value(a >> b),
            (I64Value(a), I64Value(b)) => I64Value(a >> b),
            (I32Value(a), I32Value(b)) => I32Value(a >> b),
            (I16Value(a), I16Value(b)) => I16Value(a >> b),
            (I8Value(a), I8Value(b)) => I8Value(a >> b),
            (NaNValue, _) => NaNValue,
            (_, NaNValue) => NaNValue,
            (U128Value(a), U128Value(b)) => U128Value(a >> b),
            (U64Value(a), U64Value(b)) => U64Value(a >> b),
            (U32Value(a), U32Value(b)) => U32Value(a >> b),
            (U16Value(a), U16Value(b)) => U16Value(a >> b),
            (U8Value(a), U8Value(b)) => U8Value(a >> b),
            (a, b) => U64Value(a.to_u64() >> b.to_u64()),
        }
    }
}

impl Sub for Numbers {
    type Output = Numbers;

    fn sub(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (F32Value(a), F32Value(b)) => F32Value(a - b),
            (F64Value(a), F64Value(b)) => F64Value(a - b),
            (I128Value(a), I128Value(b)) => I128Value(a - b),
            (I64Value(a), I64Value(b)) => I64Value(a - b),
            (I32Value(a), I32Value(b)) => I32Value(a - b),
            (I16Value(a), I16Value(b)) => I16Value(a - b),
            (I8Value(a), I8Value(b)) => I8Value(a - b),
            (NaNValue, _) => NaNValue,
            (_, NaNValue) => NaNValue,
            (U128Value(a), U128Value(b)) => U128Value(a - b),
            (U64Value(a), U64Value(b)) => U64Value(a - b),
            (U32Value(a), U32Value(b)) => U32Value(a - b),
            (U16Value(a), U16Value(b)) => U16Value(a - b),
            (U8Value(a), U8Value(b)) => U8Value(a - b),
            (a, b) => F64Value(a.to_f64() - b.to_f64())
        }
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_addition() {
        assert_eq!(F64Value(45.0) + F64Value(32.7), F64Value(77.7));
        assert_eq!(F32Value(45.7) + F32Value(32.0), F32Value(77.7));

        assert_eq!(I8Value(45) + I8Value(32), I8Value(77));
        assert_eq!(I16Value(45) + I16Value(32), I16Value(77));
        assert_eq!(I32Value(45) + I32Value(32), I32Value(77));
        assert_eq!(I64Value(45) + I64Value(32), I64Value(77));
        assert_eq!(I128Value(45) + I128Value(32), I128Value(77));

        assert_eq!(U8Value(45) + U8Value(32), U8Value(77));
        assert_eq!(U16Value(45) + U16Value(32), U16Value(77));
        assert_eq!(U32Value(45) + U32Value(32), U32Value(77));
        assert_eq!(U64Value(45) + U64Value(32), U64Value(77));
        assert_eq!(U128Value(45) + U128Value(32), U128Value(77));
    }

    #[test]
    fn test_division() {
        assert_eq!(F64Value(53.2) / F64Value(8.), F64Value(6.65));
        assert_eq!(F32Value(13.8) / F32Value(4.6), F32Value(3.));

        assert_eq!(I8Value(15) / I8Value(7), I8Value(2));
        assert_eq!(I16Value(33) / I16Value(3), I16Value(11));
        assert_eq!(I32Value(66) / I32Value(6), I32Value(11));
        assert_eq!(I64Value(45) / I64Value(5), I64Value(9));
        assert_eq!(I128Value(45) / I128Value(2), I128Value(22));

        assert_eq!(U8Value(13) / U8Value(2), U8Value(6));
        assert_eq!(U16Value(45) / U16Value(3), U16Value(15));
        assert_eq!(U32Value(40) / U32Value(4), U32Value(10));
        assert_eq!(U64Value(22) / U64Value(2), U64Value(11));
        assert_eq!(U128Value(22) / U128Value(3), U128Value(7));
    }

    #[test]
    fn test_equal() {
        assert_eq!(F32Value(45.0), F32Value(45.0));
        assert_eq!(F64Value(45.0), F64Value(45.0));

        assert_eq!(I8Value(0x3A), I8Value(0x3A));
        assert_eq!(I16Value(0x7ACE), I16Value(0x7ACE));
        assert_eq!(I32Value(0x1111_BEEF), I32Value(0x1111_BEEF));
        assert_eq!(I64Value(0x5555_FACE_CAFE_BABE), I64Value(0x5555_FACE_CAFE_BABE));
        assert_eq!(I128Value(0x5555_FACE_CAFE_BABE), I128Value(0x5555_FACE_CAFE_BABE));

        assert_eq!(U8Value(0xCE), U8Value(0xCE));
        assert_eq!(U16Value(0x7ACE), U16Value(0x7ACE));
        assert_eq!(U32Value(0x1111_BEEF), U32Value(0x1111_BEEF));
        assert_eq!(U64Value(0x5555_FACE_CAFE_BABE), U64Value(0x5555_FACE_CAFE_BABE));
        assert_eq!(U128Value(0x5555_FACE_CAFE_BABE), U128Value(0x5555_FACE_CAFE_BABE));
    }

    #[test]
    fn test_encode_decode() {
        fn verify_codec(expected: Numbers) {
            let actual = expected.kind().decode(&expected.encode(), 0);
            assert_eq!(actual, expected)
        }

        verify_codec(F32Value(126.0));
        verify_codec(F64Value(747.7));

        verify_codec(I8Value(126));
        verify_codec(I16Value(1445));
        verify_codec(I32Value(65535));
        verify_codec(I64Value(0xCafeBabe));
        verify_codec(I128Value(0xDeadFacedBabe));

        verify_codec(U8Value(255));
        verify_codec(U16Value(1445));
        verify_codec(U32Value(65535));
        verify_codec(U64Value(0xDeadBeef));
        verify_codec(U128Value(0xDefaceDaCafeBabe));
    }

    #[test]
    fn test_exponent() {
        assert_eq!(F32Value(5.).pow(&F32Value(2.)), F64Value(25.));
        assert_eq!(F64Value(5.).pow(&F64Value(2.)), F64Value(25.));

        assert_eq!(I8Value(5).pow(&I8Value(2)), F64Value(25.));
        assert_eq!(I16Value(5).pow(&I16Value(2)), F64Value(25.));
        assert_eq!(I32Value(5).pow(&I32Value(2)), F64Value(25.));
        assert_eq!(I64Value(5).pow(&I64Value(2)), F64Value(25.));
        assert_eq!(I128Value(5).pow(&I128Value(2)), F64Value(25.));

        assert_eq!(U8Value(5).pow(&U8Value(2)), F64Value(25.));
        assert_eq!(U16Value(5).pow(&U16Value(2)), F64Value(25.));
        assert_eq!(U32Value(5).pow(&U32Value(2)), F64Value(25.));
        assert_eq!(U64Value(5).pow(&U64Value(2)), F64Value(25.));
        assert_eq!(U128Value(5).pow(&U128Value(2)), F64Value(25.));
    }

    #[test]
    fn test_greater_than() {
        assert!(F32Value(287.11) > F32Value(45.3867));
        assert!(F64Value(359.7854) > F64Value(99.992));

        assert!(I8Value(0x7E) > I8Value(0x33));
        assert!(I16Value(0x7ACE) > I16Value(0x1111));
        assert!(I32Value(0x1111_BEEF) > I32Value(0x0ABC_BEEF));
        assert!(I64Value(0x5555_FACE_CAFE_BABE) > I64Value(0x0000_FACE_CAFE_BABE));
        assert!(I128Value(0x1111_FACE_CAFE_BABE) > I128Value(0x0000_FACE_CAFE_BABE));

        assert!(U8Value(0xCE) > U8Value(0xAA));
        assert!(U16Value(0x7ACE) > U16Value(0x1111));
        assert!(U32Value(0x1111_BEEF) > U32Value(0x0ABC_BEEF));
        assert!(U64Value(0x5555_FACE_CAFE_BABE) > U64Value(0x0000_FACE_CAFE_BABE));
        assert!(U128Value(0x7654_FACE_CAFE_BABE) > U128Value(0x0000_FACE_CAFE_BABE));
    }

    #[test]
    fn test_greater_than_or_equal() {
        assert!(F32Value(287.11) >= F32Value(45.3867));
        assert!(F64Value(359.7854) >= F64Value(99.992));

        assert!(I8Value(0x7E) >= I8Value(0x33));
        assert!(I16Value(0x7ACE) >= I16Value(0x1111));
        assert!(I32Value(0x1111_BEEF) >= I32Value(0x0ABC_BEEF));
        assert!(I64Value(0x5555_FACE_CAFE_BABE) >= I64Value(0x0000_FACE_CAFE_BABE));
        assert!(I128Value(0x1111_FACE_CAFE_BABE) >= I128Value(0x0000_FACE_CAFE_BABE));

        assert!(U8Value(0xCE) >= U8Value(0xAA));
        assert!(U16Value(0x7ACE) >= U16Value(0x1111));
        assert!(U32Value(0x1111_BEEF) >= U32Value(0x0ABC_BEEF));
        assert!(U64Value(0x5555_FACE_CAFE_BABE) >= U64Value(0x0000_FACE_CAFE_BABE));
        assert!(U128Value(0x7654_FACE_CAFE_BABE) >= U128Value(0x0000_FACE_CAFE_BABE));
    }

    #[test]
    fn test_less_than() {
        assert!(F32Value(27.11) < F32Value(45.3867));
        assert!(F64Value(39.7854) < F64Value(99.992));

        assert!(I8Value(0x06) < I8Value(0x43));
        assert!(I16Value(0x0ACE) < I16Value(0x1111));
        assert!(I32Value(0x0111_BEEF) < I32Value(0x0ABC_BEEF));
        assert!(I64Value(0x0000_FACE_CAFE_BABE) < I64Value(0x5555_FACE_CAFE_BABE));
        assert!(I128Value(0x0000_FACE_CAFE_BABE) < I128Value(0x1111_FACE_CAFE_BABE));

        assert!(U8Value(0x71) < U8Value(0xAA));
        assert!(U16Value(0xDCE) < U16Value(0x1111));
        assert!(U32Value(0x0000_BEEF) < U32Value(0x1111_BEEF));
        assert!(U64Value(0x0000_FACE_CAFE_BABE) < U64Value(0xA111_FACE_CAFE_BABE));
        assert!(U128Value(0x0000_FACE_CAFE_BABE) < U128Value(0x2233_FACE_CAFE_BABE));
    }

    #[test]
    fn test_less_than_or_equal() {
        assert!(F32Value(27.11) <= F32Value(45.3867));
        assert!(F64Value(39.7854) <= F64Value(99.992));

        assert!(I8Value(0x06) <= I8Value(0x43));
        assert!(I16Value(0x0ACE) <= I16Value(0x1111));
        assert!(I32Value(0x0111_BEEF) <= I32Value(0x0ABC_BEEF));
        assert!(I64Value(0x0000_FACE_CAFE_BABE) <= I64Value(0x5555_FACE_CAFE_BABE));
        assert!(I128Value(0x0000_FACE_CAFE_BABE) <= I128Value(0x1111_FACE_CAFE_BABE));

        assert!(U8Value(0x71) <= U8Value(0xAA));
        assert!(U16Value(0xDCE) <= U16Value(0x1111));
        assert!(U32Value(0x0000_BEEF) <= U32Value(0x1111_BEEF));
        assert!(U64Value(0x0000_FACE_CAFE_BABE) <= U64Value(0xA111_FACE_CAFE_BABE));
        assert!(U128Value(0x0000_FACE_CAFE_BABE) <= U128Value(0x2233_FACE_CAFE_BABE));
    }

    #[test]
    fn test_modulus() {
        assert_eq!(I8Value(10) % I8Value(3), I8Value(1));
        assert_eq!(I16Value(10) % I16Value(3), I16Value(1));
        assert_eq!(I32Value(10) % I32Value(3), I32Value(1));
        assert_eq!(I64Value(10) % I64Value(3), I64Value(1));
        assert_eq!(I128Value(10) % I128Value(3), I128Value(1));

        assert_eq!(U8Value(10) % U8Value(3), U8Value(1));
        assert_eq!(U16Value(10) % U16Value(3), U16Value(1));
        assert_eq!(U32Value(10) % U32Value(3), U32Value(1));
        assert_eq!(U64Value(10) % U64Value(3), U64Value(1));
        assert_eq!(U128Value(10) % U128Value(3), U128Value(1));
    }

    #[test]
    fn test_multiplication() {
        assert_eq!(F64Value(53.2) * F64Value(32.8), F64Value(1744.96));
        assert_eq!(F32Value(13.8) * F32Value(4.6), F32Value(63.48));

        assert_eq!(I8Value(5) * I8Value(7), I8Value(35));
        assert_eq!(I16Value(11) * I16Value(3), I16Value(33));
        assert_eq!(I32Value(45) * I32Value(10), I32Value(450));
        assert_eq!(I64Value(45) * I64Value(5), I64Value(225));
        assert_eq!(I128Value(45) * I128Value(2), I128Value(90));

        assert_eq!(U8Value(13) * U8Value(2), U8Value(26));
        assert_eq!(U16Value(45) * U16Value(3), U16Value(135));
        assert_eq!(U32Value(45) * U32Value(4), U32Value(180));
        assert_eq!(U64Value(22) * U64Value(2), U64Value(44));
        assert_eq!(U128Value(22) * U128Value(3), U128Value(66));
    }

    #[test]
    fn test_not_equal() {
        assert_ne!(F32Value(45.0), F32Value(45.7));
        assert_ne!(F64Value(99.142857), F64Value(19.48));

        assert_ne!(I8Value(0x7E), I8Value(0x23));
        assert_ne!(I16Value(0x7ACE), I16Value(0xACE));
        assert_ne!(I32Value(0x1111_BEEF), I32Value(0xBEEF));
        assert_ne!(I64Value(0x5555_FACE_CAFE_BABE), I64Value(0xFACE_CAFE_BABE));
        assert_ne!(I128Value(0x5555_FACE_CAFE_BABE), I128Value(0xFACE_CAFE_BABE));

        assert_ne!(U8Value(0xCE), U8Value(0x00));
        assert_ne!(U16Value(0x7ACE), U16Value(0xACE));
        assert_ne!(U32Value(0x1111_BEEF), U32Value(0xBEEF));
        assert_ne!(U64Value(0x5555_FACE_CAFE_BABE), U64Value(0xFACE_CAFE_BABE));
        assert_ne!(U128Value(0x5555_FACE_CAFE_BABE), U128Value(0xFACE_CAFE_BABE));
    }

    #[test]
    fn test_shift_left() {
        assert_eq!(I8Value(1) << I8Value(5), I8Value(32));
        assert_eq!(I16Value(1) << I16Value(5), I16Value(32));
        assert_eq!(I32Value(1) << I32Value(5), I32Value(32));
        assert_eq!(I64Value(1) << I64Value(5), I64Value(32));
        assert_eq!(I128Value(1) << I128Value(5), I128Value(32));

        assert_eq!(U8Value(1) << U8Value(5), U8Value(32));
        assert_eq!(U16Value(1) << U16Value(5), U16Value(32));
        assert_eq!(U32Value(1) << U32Value(5), U32Value(32));
        assert_eq!(U64Value(1) << U64Value(5), U64Value(32));
        assert_eq!(U128Value(1) << U128Value(5), U128Value(32));
    }

    #[test]
    fn test_shift_right() {
        assert_eq!(I8Value(32) >> I8Value(5), I8Value(1));
        assert_eq!(I16Value(32) >> I16Value(5), I16Value(1));
        assert_eq!(I32Value(32) >> I32Value(5), I32Value(1));
        assert_eq!(I64Value(32) >> I64Value(5), I64Value(1));
        assert_eq!(I128Value(32) >> I128Value(5), I128Value(1));

        assert_eq!(U8Value(32) >> U8Value(5), U8Value(1));
        assert_eq!(U16Value(32) >> U16Value(5), U16Value(1));
        assert_eq!(U32Value(32) >> U32Value(5), U32Value(1));
        assert_eq!(U64Value(32) >> U64Value(5), U64Value(1));
        assert_eq!(U128Value(32) >> U128Value(5), U128Value(1));
    }

    #[test]
    fn test_subtraction() {
        assert_eq!(F64Value(45.0) - F64Value(32.0), F64Value(13.0));
        assert_eq!(F32Value(45.0) - F32Value(32.0), F32Value(13.0));

        assert_eq!(I8Value(45) - I8Value(32), I8Value(13));
        assert_eq!(I16Value(45) - I16Value(32), I16Value(13));
        assert_eq!(I32Value(45) - I32Value(32), I32Value(13));
        assert_eq!(I64Value(45) - I64Value(32), I64Value(13));
        assert_eq!(I128Value(45) - I128Value(32), I128Value(13));

        assert_eq!(U8Value(45) - U8Value(32), U8Value(13));
        assert_eq!(U16Value(45) - U16Value(32), U16Value(13));
        assert_eq!(U32Value(45) - U32Value(32), U32Value(13));
        assert_eq!(U64Value(45) - U64Value(32), U64Value(13));
        assert_eq!(U128Value(45) - U128Value(32), U128Value(13));
    }
}