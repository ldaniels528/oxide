////////////////////////////////////////////////////////////////////
// typed values module
////////////////////////////////////////////////////////////////////

use std::cmp::Ordering;
use std::collections::Bound;
use std::fmt::Display;
use std::ops::*;

use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};

use crate::codec;
use crate::number_kind::NumberKind;
use crate::number_kind::NumberKind::*;
use crate::numbers::NumberValue::*;

/// Represents a numeric value
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum NumberValue {
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
    NaNValue,
}

impl NumberValue {
    /// decodes the typed value based on the supplied data type and buffer
    pub fn decode(buffer: &Vec<u8>, offset: usize, kind: NumberKind) -> NumberValue {
        match kind {
            F32Kind => codec::decode_u8x4(buffer, offset, |b| F32Value(f32::from_be_bytes(b))),
            F64Kind => codec::decode_u8x8(buffer, offset, |b| F64Value(f64::from_be_bytes(b))),
            I8Kind => codec::decode_u8(buffer, offset, |b| I8Value(b.to_i8().unwrap())),
            I16Kind => codec::decode_u8x2(buffer, offset, |b| I16Value(i16::from_be_bytes(b))),
            I32Kind => codec::decode_u8x4(buffer, offset, |b| I32Value(i32::from_be_bytes(b))),
            I64Kind => codec::decode_u8x8(buffer, offset, |b| I64Value(i64::from_be_bytes(b))),
            I128Kind => codec::decode_u8x16(buffer, offset, |b| I128Value(i128::from_be_bytes(b))),
            U8Kind => codec::decode_u8(buffer, offset, |b| U8Value(b)),
            U16Kind => codec::decode_u8x2(buffer, offset, |b| U16Value(u16::from_be_bytes(b))),
            U32Kind => codec::decode_u8x4(buffer, offset, |b| U32Value(u32::from_be_bytes(b))),
            U64Kind => codec::decode_u8x8(buffer, offset, |b| U64Value(u64::from_be_bytes(b))),
            U128Kind => codec::decode_u8x16(buffer, offset, |b| U128Value(u128::from_be_bytes(b))),
            NaNKind => NaNValue,
        }
    }

    /// encodes the numeric value
    pub fn encode(&self) -> Vec<u8> {
        use NumberValue::*;
        match self.to_owned() {
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
            NaNValue => Vec::new(),
        }
    }

    pub fn from_string(number_str: String) -> NumberValue {
        match number_str.parse::<f64>() {
            Ok(num) => match number_str.parse::<i64>() {
                Ok(num) => I64Value(num),
                Err(_) => F64Value(num),
            }
            Err(_) => NaNValue,
        }
    }

    pub fn get_type_name(&self) -> String {
        let result = match *self {
            F32Value(..) => "f32",
            F64Value(..) => "f64",
            I8Value(..) => "i8",
            I16Value(..) => "i16",
            I32Value(..) => "i32",
            I64Value(..) => "i64",
            I128Value(..) => "i128",
            U8Value(..) => "u8",
            U16Value(..) => "u16",
            U32Value(..) => "u32",
            U64Value(..) => "u64",
            U128Value(..) => "u128",
            NaNValue => "NaN"
        };
        result.to_string()
    }

    pub fn is_effectively_zero(&self) -> bool {
        match *self {
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
        }
    }

    pub fn pow(&self, rhs: &Self) -> Self {
        F64Value(num_traits::pow(self.to_f64(), rhs.to_usize()))
    }

    pub fn kind(&self) -> NumberKind {
        match *self {
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
        }
    }

    pub fn to_f64(&self) -> f64 {
        match *self {
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
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        match self {
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
        }
    }

    pub fn to_i64(&self) -> i64 {
        match *self {
            F32Value(n) => n as i64,
            F64Value(n) => n as i64,
            I8Value(n) => n as i64,
            I16Value(n) => n as i64,
            I32Value(n) => n as i64,
            I64Value(n) => n,
            I128Value(n) => n as i64,
            NaNValue => 0,
            U8Value(n) => n as i64,
            U16Value(n) => n as i64,
            U32Value(n) => n as i64,
            U64Value(n) => n as i64,
            U128Value(n) => n as i64,
        }
    }

    pub fn to_u64(&self) -> u64 {
        match *self {
            F32Value(n) => n as u64,
            F64Value(n) => n as u64,
            I8Value(n) => n as u64,
            I16Value(n) => n as u64,
            I32Value(n) => n as u64,
            I64Value(n) => n as u64,
            I128Value(n) => n as u64,
            NaNValue => 0,
            U8Value(n) => n as u64,
            U16Value(n) => n as u64,
            U32Value(n) => n as u64,
            U64Value(n) => n,
            U128Value(n) => n as u64,
        }
    }

    pub fn to_usize(&self) -> usize {
        match *self {
            F32Value(n) => n as usize,
            F64Value(n) => n as usize,
            I8Value(n) => n as usize,
            I16Value(n) => n as usize,
            I32Value(n) => n as usize,
            I64Value(n) => n as usize,
            I128Value(n) => n as usize,
            NaNValue => 0,
            U8Value(n) => n as usize,
            U16Value(n) => n as usize,
            U32Value(n) => n as usize,
            U64Value(n) => n as usize,
            U128Value(n) => n as usize,
        }
    }

    pub fn unwrap_value(&self) -> String {
        match self {
            F32Value(number) => number.to_string(),
            F64Value(number) => number.to_string(),
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
            U128Value(number) => number.to_string(),
        }
    }
}

impl Add for NumberValue {
    type Output = NumberValue;

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

impl BitAnd for NumberValue {
    type Output = NumberValue;

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

impl BitOr for NumberValue {
    type Output = NumberValue;

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

impl BitXor for NumberValue {
    type Output = NumberValue;

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

impl Display for NumberValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.unwrap_value())
    }
}

impl Div for NumberValue {
    type Output = NumberValue;

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

impl Mul for NumberValue {
    type Output = NumberValue;

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

impl Neg for NumberValue {
    type Output = NumberValue;

    fn neg(self) -> Self::Output {
        match self {
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
        }
    }
}

impl Not for NumberValue {
    type Output = NumberValue;

    fn not(self) -> Self::Output {
        match self {
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
        }
    }
}

impl RangeBounds<NumberValue> for NumberValue {
    fn start_bound(&self) -> Bound<&NumberValue> {
        std::ops::Bound::Included(&self)
    }

    fn end_bound(&self) -> Bound<&NumberValue> {
        std::ops::Bound::Excluded(&self)
    }
}

impl Rem for NumberValue {
    type Output = NumberValue;

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

impl PartialOrd for NumberValue {
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

impl Shl for NumberValue {
    type Output = NumberValue;

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

impl Shr for NumberValue {
    type Output = NumberValue;

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

impl Sub for NumberValue {
    type Output = NumberValue;

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
    use crate::data_types::DataType::StringType;
    use crate::data_types::SizeTypes;
    use crate::typed_values::TypedValue;
    use crate::typed_values::TypedValue::StringValue;

    use super::*;

    #[test]
    fn test_add() {
        assert_eq!(F64Value(45.0) + F64Value(32.7), F64Value(77.7));
        assert_eq!(F32Value(45.7) + F32Value(32.0), F32Value(77.7));
        assert_eq!(I64Value(45) + I64Value(32), I64Value(77));
        assert_eq!(I32Value(45) + I32Value(32), I32Value(77));
        assert_eq!(I16Value(45) + I16Value(32), I16Value(77));
        assert_eq!(U8Value(45) + U8Value(32), U8Value(77));
    }

    #[test]
    fn test_eq() {
        assert_eq!(U8Value(0xCE), U8Value(0xCE));
        assert_eq!(I16Value(0x7ACE), I16Value(0x7ACE));
        assert_eq!(I32Value(0x1111_BEEF), I32Value(0x1111_BEEF));
        assert_eq!(I64Value(0x5555_FACE_CAFE_BABE), I64Value(0x5555_FACE_CAFE_BABE));
        assert_eq!(F32Value(45.0), F32Value(45.0));
        assert_eq!(F64Value(45.0), F64Value(45.0));
    }

    #[test]
    fn test_ne() {
        assert_ne!(U8Value(0xCE), U8Value(0x00));
        assert_ne!(I16Value(0x7ACE), I16Value(0xACE));
        assert_ne!(I32Value(0x1111_BEEF), I32Value(0xBEEF));
        assert_ne!(I64Value(0x5555_FACE_CAFE_BABE), I64Value(0xFACE_CAFE_BABE));
        assert_ne!(F32Value(45.0), F32Value(45.7));
        assert_ne!(F64Value(99.142857), F64Value(19.48));
    }

    #[test]
    fn test_gt() {
        assert!(U8Value(0xCE) > U8Value(0xAA));
        assert!(I16Value(0x7ACE) > I16Value(0x1111));
        assert!(I32Value(0x1111_BEEF) > I32Value(0x0ABC_BEEF));
        assert!(I64Value(0x5555_FACE_CAFE_BABE) > I64Value(0x0000_FACE_CAFE_BABE));
        assert!(F32Value(287.11) > F32Value(45.3867));
        assert!(F64Value(359.7854) > F64Value(99.992));
    }

    #[test]
    fn test_rem() {
        assert_eq!(I64Value(10) % I64Value(3), I64Value(1));
    }

    #[test]
    fn test_pow() {
        let a = I64Value(5);
        let b = I64Value(2);
        assert_eq!(a.pow(&b), F64Value(25.))
    }

    #[test]
    fn test_shl() {
        assert_eq!(I64Value(1) << I64Value(5), I64Value(32));
    }

    #[test]
    fn test_shr() {
        assert_eq!(I64Value(32) >> I64Value(5), I64Value(1));
    }

    #[test]
    fn test_sub() {
        assert_eq!(F64Value(45.0) - F64Value(32.0), F64Value(13.0));
        assert_eq!(F32Value(45.0) - F32Value(32.0), F32Value(13.0));
        assert_eq!(I64Value(45) - I64Value(32), I64Value(13));
        assert_eq!(I32Value(45) - I32Value(32), I32Value(13));
        assert_eq!(I16Value(45) - I16Value(32), I16Value(13));
        assert_eq!(U8Value(45) - U8Value(32), U8Value(13));
    }

    #[test]
    fn test_decode() {
        let buf: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 5, b'H', b'e', b'l', b'l', b'o'];
        assert_eq!(TypedValue::decode(&StringType(SizeTypes::Fixed(5)), &buf, 0), StringValue("Hello".into()))
    }

}