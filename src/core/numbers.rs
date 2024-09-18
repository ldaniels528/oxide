////////////////////////////////////////////////////////////////////
// typed values module
////////////////////////////////////////////////////////////////////

use std::cmp::Ordering;
use std::collections::Bound;
use std::fmt::Display;
use std::ops::{Add, BitAnd, BitOr, BitXor, Div, Index, Mul, Neg, Not, RangeBounds, Rem, Shl, Shr, Sub};

use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};

use crate::codec;
use crate::numbers::NumberValue::{Float32Value, Float64Value, Int128Value, Int16Value, Int32Value, Int64Value, Int8Value, NotANumber, UInt128Value, UInt16Value, UInt32Value, UInt64Value, UInt8Value};
use crate::typed_values::TypedValue;

/// Represents a numeric value
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum NumberValue {
    Float32Value(f32),
    Float64Value(f64),
    Int8Value(i8),
    Int16Value(i16),
    Int32Value(i32),
    Int64Value(i64),
    Int128Value(i128),
    UInt8Value(u8),
    UInt16Value(u16),
    UInt32Value(u32),
    UInt64Value(u64),
    UInt128Value(u128),
    NotANumber,
}

impl NumberValue {
    /// decodes the typed value based on the supplied data type and buffer
    pub fn decode(buffer: &Vec<u8>, offset: usize, kind: NumberKind) -> NumberValue {
        use NumberKind::*;
        match kind {
            F32Kind => codec::decode_u8x4(buffer, offset, |b| Float32Value(f32::from_be_bytes(b))),
            F64Kind => codec::decode_u8x8(buffer, offset, |b| Float64Value(f64::from_be_bytes(b))),
            I8Kind => codec::decode_u8(buffer, offset, |b| Int8Value(b.to_i8().unwrap())),
            I16Kind => codec::decode_u8x2(buffer, offset, |b| Int16Value(i16::from_be_bytes(b))),
            I32Kind => codec::decode_u8x4(buffer, offset, |b| Int32Value(i32::from_be_bytes(b))),
            I64Kind => codec::decode_u8x8(buffer, offset, |b| Int64Value(i64::from_be_bytes(b))),
            I128Kind => codec::decode_u8x16(buffer, offset, |b| Int128Value(i128::from_be_bytes(b))),
            U8Kind => codec::decode_u8(buffer, offset, |b| UInt8Value(b)),
            U16Kind => codec::decode_u8x2(buffer, offset, |b| UInt16Value(u16::from_be_bytes(b))),
            U32Kind => codec::decode_u8x4(buffer, offset, |b| UInt32Value(u32::from_be_bytes(b))),
            U64Kind => codec::decode_u8x8(buffer, offset, |b| UInt64Value(u64::from_be_bytes(b))),
            U128Kind => codec::decode_u8x16(buffer, offset, |b| UInt128Value(u128::from_be_bytes(b))),
            NaNKind => NotANumber,
        }
    }

    /// encodes the numeric value
    pub fn encode(&self) -> Vec<u8> {
        use NumberValue::*;
        match self.to_owned() {
            Float32Value(number) => number.to_be_bytes().to_vec(),
            Float64Value(number) => number.to_be_bytes().to_vec(),
            Int8Value(number) => number.to_be_bytes().to_vec(),
            Int16Value(number) => number.to_be_bytes().to_vec(),
            Int32Value(number) => number.to_be_bytes().to_vec(),
            Int64Value(number) => number.to_be_bytes().to_vec(),
            Int128Value(number) => number.to_be_bytes().to_vec(),
            UInt8Value(number) => number.to_be_bytes().to_vec(),
            UInt16Value(number) => number.to_be_bytes().to_vec(),
            UInt32Value(number) => number.to_be_bytes().to_vec(),
            UInt64Value(number) => number.to_be_bytes().to_vec(),
            UInt128Value(number) => number.to_be_bytes().to_vec(),
            NotANumber => Vec::new(),
        }
    }

    pub fn encode_with_type(&self) -> Vec<u8> {
        let mut code = vec![self.kind().to_u8()];
        code.extend(self.encode());
        code
    }

    pub fn get_type_name(&self) -> String {
        use NumberValue::*;
        let result = match *self {
            Float32Value(..) => "f32",
            Float64Value(..) => "f64",
            Int8Value(..) => "i8",
            Int16Value(..) => "i16",
            Int32Value(..) => "i32",
            Int64Value(..) => "i64",
            Int128Value(..) => "i128",
            UInt8Value(..) => "u8",
            UInt16Value(..) => "u16",
            UInt32Value(..) => "u32",
            UInt64Value(..) => "u64",
            UInt128Value(..) => "u128",
            NotANumber => "NaN"
        };
        result.to_string()
    }

    fn numeric_op_1f(lhs: &Self, ff: fn(f64) -> f64) -> Option<Self> {
        use NumberValue::*;
        match lhs {
            Float64Value(a) => Some(Float64Value(ff(*a))),
            Float32Value(a) => Some(Float32Value(ff(*a as f64) as f32)),
            Int128Value(a) => Some(Int128Value(ff(*a as f64) as i128)),
            Int64Value(a) => Some(Int64Value(ff(*a as f64) as i64)),
            Int32Value(a) => Some(Int32Value(ff(*a as f64) as i32)),
            Int16Value(a) => Some(Int16Value(ff(*a as f64) as i16)),
            Int8Value(a) => Some(Int8Value(ff(*a as f64) as i8)),
            UInt128Value(a) => Some(UInt128Value(ff(*a as f64) as u128)),
            UInt64Value(a) => Some(UInt64Value(ff(*a as f64) as u64)),
            UInt32Value(a) => Some(UInt32Value(ff(*a as f64) as u32)),
            UInt16Value(a) => Some(UInt16Value(ff(*a as f64) as u16)),
            UInt8Value(a) => Some(UInt8Value(ff(*a as f64) as u8)),
            NotANumber => Some(NotANumber),
        }
    }

    fn numeric_op_2i(lhs: &Self, rhs: &Self, ff: fn(i64, i64) -> i64) -> Option<Self> {
        match (lhs, rhs) {
            (Float64Value(a), Float64Value(b)) => Some(Float64Value(ff(*a as i64, *b as i64) as f64)),
            (Float32Value(a), Float32Value(b)) => Some(Float32Value(ff(*a as i64, *b as i64) as f32)),
            (Int128Value(a), Int128Value(b)) => Some(Int128Value(ff(*a as i64, *b as i64) as i128)),
            (Int64Value(a), Int64Value(b)) => Some(Int64Value(ff(*a as i64, *b as i64) as i64)),
            (Int32Value(a), Int32Value(b)) => Some(Int32Value(ff(*a as i64, *b as i64) as i32)),
            (Int16Value(a), Int16Value(b)) => Some(Int16Value(ff(*a as i64, *b as i64) as i16)),
            (Int8Value(a), Int8Value(b)) => Some(Int8Value(ff(*a as i64, *b as i64) as i8)),
            (UInt128Value(a), UInt128Value(b)) => Some(UInt128Value(ff(*a as i64, *b as i64) as u128)),
            (UInt64Value(a), UInt64Value(b)) => Some(UInt64Value(ff(*a as i64, *b as i64) as u64)),
            (UInt32Value(a), UInt32Value(b)) => Some(UInt32Value(ff(*a as i64, *b as i64) as u32)),
            (UInt16Value(a), UInt16Value(b)) => Some(UInt16Value(ff(*a as i64, *b as i64) as u16)),
            (UInt8Value(a), UInt8Value(b)) => Some(UInt8Value(ff(*a as i64, *b as i64) as u8)),
            (NotANumber, _) => Some(NotANumber),
            (_, NotANumber) => Some(NotANumber),
            _ => Some(Int64Value(ff(lhs.to_i64(), rhs.to_i64())))
        }
    }

    fn numeric_op_2f(lhs: &Self, rhs: &Self, ff: fn(f64, f64) -> f64) -> Option<Self> {
        match (lhs, rhs) {
            (Float64Value(a), Float64Value(b)) => Some(Float64Value(ff(*a, *b))),
            (Float32Value(a), Float32Value(b)) => Some(Float32Value(ff(*a as f64, *b as f64) as f32)),
            (Int128Value(a), Int128Value(b)) => Some(Int128Value(ff(*a as f64, *b as f64) as i128)),
            (Int64Value(a), Int64Value(b)) => Some(Int64Value(ff(*a as f64, *b as f64) as i64)),
            (Int32Value(a), Int32Value(b)) => Some(Int32Value(ff(*a as f64, *b as f64) as i32)),
            (Int16Value(a), Int16Value(b)) => Some(Int16Value(ff(*a as f64, *b as f64) as i16)),
            (Int8Value(a), Int8Value(b)) => Some(Int8Value(ff(*a as f64, *b as f64) as i8)),
            (UInt128Value(a), UInt128Value(b)) => Some(UInt128Value(ff(*a as f64, *b as f64) as u128)),
            (UInt64Value(a), UInt64Value(b)) => Some(UInt64Value(ff(*a as f64, *b as f64) as u64)),
            (UInt32Value(a), UInt32Value(b)) => Some(UInt32Value(ff(*a as f64, *b as f64) as u32)),
            (UInt16Value(a), UInt16Value(b)) => Some(UInt16Value(ff(*a as f64, *b as f64) as u16)),
            (UInt8Value(a), UInt8Value(b)) => Some(UInt8Value(ff(*a as f64, *b as f64) as u8)),
            _ => Some(Float64Value(ff(lhs.to_f64(), rhs.to_f64())))
        }
    }

    pub fn pow(&self, rhs: &Self) -> Self {
        Float64Value(num_traits::pow(self.to_f64(), rhs.to_usize()))
    }

    pub fn kind(&self) -> NumberKind {
        use NumberKind::*;
        match *self {
            Float32Value(..) => F32Kind,
            Float64Value(..) => F64Kind,
            Int8Value(..) => I8Kind,
            Int16Value(..) => I16Kind,
            Int32Value(..) => I32Kind,
            Int64Value(..) => I64Kind,
            Int128Value(..) => I128Kind,
            UInt8Value(..) => U8Kind,
            UInt16Value(..) => U16Kind,
            UInt32Value(..) => U32Kind,
            UInt64Value(..) => U64Kind,
            UInt128Value(..) => U128Kind,
            NotANumber => NaNKind,
        }
    }

    pub fn to_f64(&self) -> f64 {
        use NumberValue::*;
        match &self {
            Float32Value(n) => *n as f64,
            Float64Value(n) => *n,
            Int8Value(n) => *n as f64,
            Int16Value(n) => *n as f64,
            Int32Value(n) => *n as f64,
            Int64Value(n) => *n as f64,
            Int128Value(n) => *n as f64,
            UInt8Value(n) => *n as f64,
            UInt16Value(n) => *n as f64,
            UInt32Value(n) => *n as f64,
            UInt64Value(n) => *n as f64,
            UInt128Value(n) => *n as f64,
            NotANumber => 0.0
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        use NumberValue::*;
        match self {
            Float32Value(number) => serde_json::json!(number),
            Float64Value(number) => serde_json::json!(number),
            Int8Value(number) => serde_json::json!(number),
            Int16Value(number) => serde_json::json!(number),
            Int32Value(number) => serde_json::json!(number),
            Int64Value(number) => serde_json::json!(number),
            Int128Value(number) => serde_json::json!(number),
            NotANumber => serde_json::json!("NaN"),
            UInt8Value(number) => serde_json::json!(number),
            UInt16Value(number) => serde_json::json!(number),
            UInt32Value(number) => serde_json::json!(number),
            UInt64Value(number) => serde_json::json!(number),
            UInt128Value(number) => serde_json::json!(number),
        }
    }

    pub fn to_i64(&self) -> i64 {
        use NumberValue::*;
        match &self {
            Float32Value(n) => *n as i64,
            Float64Value(n) => *n as i64,
            Int8Value(n) => *n as i64,
            Int16Value(n) => *n as i64,
            Int32Value(n) => *n as i64,
            Int64Value(n) => *n,
            Int128Value(n) => *n as i64,
            UInt8Value(n) => *n as i64,
            UInt16Value(n) => *n as i64,
            UInt32Value(n) => *n as i64,
            UInt64Value(n) => *n as i64,
            UInt128Value(n) => *n as i64,
            NotANumber => 0
        }
    }

    pub fn to_u64(&self) -> u64 {
        use NumberValue::*;
        match &self {
            Float32Value(n) => *n as u64,
            Float64Value(n) => *n as u64,
            Int8Value(n) => *n as u64,
            Int16Value(n) => *n as u64,
            Int32Value(n) => *n as u64,
            Int64Value(n) => *n as u64,
            Int128Value(n) => *n as u64,
            UInt8Value(n) => *n as u64,
            UInt16Value(n) => *n as u64,
            UInt32Value(n) => *n as u64,
            UInt64Value(n) => *n,
            UInt128Value(n) => *n as u64,
            NotANumber => 0
        }
    }

    pub fn to_usize(&self) -> usize {
        use NumberValue::*;
        match &self {
            Float32Value(n) => *n as usize,
            Float64Value(n) => *n as usize,
            Int8Value(n) => *n as usize,
            Int16Value(n) => *n as usize,
            Int32Value(n) => *n as usize,
            Int64Value(n) => *n as usize,
            Int128Value(n) => *n as usize,
            UInt8Value(n) => *n as usize,
            UInt16Value(n) => *n as usize,
            UInt32Value(n) => *n as usize,
            UInt64Value(n) => *n as usize,
            UInt128Value(n) => *n as usize,
            NotANumber => 0
        }
    }

    pub fn unwrap_value(&self) -> String {
        use NumberValue::*;
        match self {
            Float32Value(number) => number.to_string(),
            Float64Value(number) => number.to_string(),
            Int8Value(number) => number.to_string(),
            Int16Value(number) => number.to_string(),
            Int32Value(number) => number.to_string(),
            Int64Value(number) => number.to_string(),
            Int128Value(number) => number.to_string(),
            NotANumber => "NaN".to_string(),
            UInt8Value(number) => number.to_string(),
            UInt16Value(number) => number.to_string(),
            UInt32Value(number) => number.to_string(),
            UInt64Value(number) => number.to_string(),
            UInt128Value(number) => number.to_string(),
        }
    }
}

impl Add for NumberValue {
    type Output = NumberValue;

    fn add(self, rhs: Self) -> Self::Output {
        use NumberValue::*;
        match (&self, &rhs) {
            (Float32Value(a), Float32Value(b)) => Float32Value(*a + *b),
            (Float64Value(a), Float64Value(b)) => Float64Value(*a + *b),
            (Int128Value(a), Int128Value(b)) => Int128Value(*a + *b),
            (Int64Value(a), Int64Value(b)) => Int64Value(*a + *b),
            (Int32Value(a), Int32Value(b)) => Int32Value(*a + *b),
            (Int16Value(a), Int16Value(b)) => Int16Value(*a + *b),
            (Int8Value(a), Int8Value(b)) => Int8Value(*a + *b),
            (NotANumber, _) => NotANumber,
            (_, NotANumber) => NotANumber,
            (UInt128Value(a), UInt128Value(b)) => UInt128Value(*a + *b),
            (UInt64Value(a), UInt64Value(b)) => UInt64Value(*a + *b),
            (UInt32Value(a), UInt32Value(b)) => UInt32Value(*a + *b),
            (UInt16Value(a), UInt16Value(b)) => UInt16Value(*a + *b),
            (UInt8Value(a), UInt8Value(b)) => UInt8Value(*a + *b),
            _ => Self::numeric_op_2f(&self, &rhs, |a, b| a + b).unwrap_or(NotANumber)
        }
    }
}

impl BitAnd for NumberValue {
    type Output = NumberValue;

    fn bitand(self, rhs: Self) -> Self::Output {
        Self::numeric_op_2i(&self, &rhs, |a, b| a & b).unwrap_or(NotANumber)
    }
}

impl BitOr for NumberValue {
    type Output = NumberValue;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self::numeric_op_2i(&self, &rhs, |a, b| a | b).unwrap_or(NotANumber)
    }
}

impl BitXor for NumberValue {
    type Output = NumberValue;

    fn bitxor(self, rhs: Self) -> Self::Output {
        Self::numeric_op_2i(&self, &rhs, |a, b| a ^ b).unwrap_or(NotANumber)
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
        Self::numeric_op_2f(&self, &rhs, |a, b| a / b).unwrap_or(NotANumber)
    }
}

impl Mul for NumberValue {
    type Output = NumberValue;

    fn mul(self, rhs: Self) -> Self::Output {
        Self::numeric_op_2f(&self, &rhs, |a, b| a * b).unwrap_or(NotANumber)
    }
}

impl Neg for NumberValue {
    type Output = NumberValue;

    fn neg(self) -> Self::Output {
        match self {
            Float32Value(n) => Float32Value(-n),
            Float64Value(n) => Float64Value(-n),
            Int8Value(n) => Int8Value(-n),
            Int16Value(n) => Int16Value(-n),
            Int32Value(n) => Int32Value(-n),
            Int64Value(n) => Int64Value(-n),
            Int128Value(n) => Int128Value(-n),
            UInt8Value(n) => Int8Value(-(n as i8)),
            UInt16Value(n) => Int16Value(-(n as i16)),
            UInt32Value(n) => Int32Value(-(n as i32)),
            UInt64Value(n) => Int64Value(-(n as i64)),
            UInt128Value(n) => Int128Value(-(n as i128)),
            NotANumber => NotANumber
        }
    }
}

impl Not for NumberValue {
    type Output = NumberValue;

    fn not(self) -> Self::Output {
        match self {
            Float32Value(n) => Float32Value(-n),
            Float64Value(n) => Float64Value(-n),
            Int8Value(n) => Int8Value(-n),
            Int16Value(n) => Int16Value(-n),
            Int32Value(n) => Int32Value(-n),
            Int64Value(n) => Int64Value(-n),
            Int128Value(n) => Int128Value(-n),
            UInt8Value(n) => Int8Value(-(n as i8)),
            UInt16Value(n) => Int16Value(-(n as i16)),
            UInt32Value(n) => Int32Value(-(n as i32)),
            UInt64Value(n) => Int64Value(-(n as i64)),
            UInt128Value(n) => Int128Value(-(n as i128)),
            NotANumber => NotANumber
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
        Self::numeric_op_2i(&self, &rhs, |a, b| a % b).unwrap_or(NotANumber)
    }
}

impl PartialOrd for NumberValue {
    fn partial_cmp(&self, rhs: &Self) -> Option<Ordering> {
        use NumberValue::*;
        match (&self, &rhs) {
            (Float32Value(a), Float32Value(b)) => a.partial_cmp(b),
            (Float64Value(a), Float64Value(b)) => a.partial_cmp(b),
            (Int8Value(a), Int8Value(b)) => a.partial_cmp(b),
            (Int16Value(a), Int16Value(b)) => a.partial_cmp(b),
            (Int32Value(a), Int32Value(b)) => a.partial_cmp(b),
            (Int64Value(a), Int64Value(b)) => a.partial_cmp(b),
            (Int128Value(a), Int128Value(b)) => a.partial_cmp(b),
            (UInt8Value(a), UInt8Value(b)) => a.partial_cmp(b),
            (UInt16Value(a), UInt16Value(b)) => a.partial_cmp(b),
            (UInt32Value(a), UInt32Value(b)) => a.partial_cmp(b),
            (UInt64Value(a), UInt64Value(b)) => a.partial_cmp(b),
            (UInt128Value(a), UInt128Value(b)) => a.partial_cmp(b),
            (a, b) => a.to_f64().partial_cmp(&b.to_f64())
        }
    }
}

impl Shl for NumberValue {
    type Output = NumberValue;

    fn shl(self, rhs: Self) -> Self::Output {
        Self::numeric_op_2i(&self, &rhs, |a, b| a << b).unwrap_or(NotANumber)
    }
}

impl Shr for NumberValue {
    type Output = NumberValue;

    fn shr(self, rhs: Self) -> Self::Output {
        Self::numeric_op_2i(&self, &rhs, |a, b| a >> b).unwrap_or(NotANumber)
    }
}

impl Sub for NumberValue {
    type Output = NumberValue;

    fn sub(self, rhs: Self) -> Self::Output {
        match (&self, &rhs) {
            _ => Self::numeric_op_2f(&self, &rhs, |a, b| a - b).unwrap_or(NotANumber)
        }
    }
}

// Represents a numeric type or kind of value
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum NumberKind {
    F32Kind = 0,
    F64Kind = 1,
    I8Kind = 2,
    I16Kind = 3,
    I32Kind = 4,
    I64Kind = 5,
    I128Kind = 6,
    U8Kind = 7,
    U16Kind = 8,
    U32Kind = 9,
    U64Kind = 10,
    U128Kind = 11,
    NaNKind = 12,
}

impl NumberKind {
    pub fn from_u8(index: u8) -> NumberKind {
        Self::values()[index as usize]
    }

    pub fn to_u8(&self) -> u8 {
        *self as u8
    }

    pub fn values() -> Vec<NumberKind> {
        use NumberKind::*;
        vec![
            F32Kind, F64Kind,
            I8Kind, I16Kind, I32Kind, I64Kind, I128Kind,
            U8Kind, U16Kind, U32Kind, U64Kind, U128Kind,
            NaNKind,
        ]
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::data_types::DataType::StringType;
    use crate::numbers::NumberValue::*;
    use crate::typed_values::TypedValue;
    use crate::typed_values::TypedValue::StringValue;

    use super::*;

    #[test]
    fn test_add() {
        assert_eq!(Float64Value(45.0) + Float64Value(32.7), Float64Value(77.7));
        assert_eq!(Float32Value(45.7) + Float32Value(32.0), Float32Value(77.7));
        assert_eq!(Int64Value(45) + Int64Value(32), Int64Value(77));
        assert_eq!(Int32Value(45) + Int32Value(32), Int32Value(77));
        assert_eq!(Int16Value(45) + Int16Value(32), Int16Value(77));
        assert_eq!(UInt8Value(45) + UInt8Value(32), UInt8Value(77));
    }

    #[test]
    fn test_eq() {
        assert_eq!(UInt8Value(0xCE), UInt8Value(0xCE));
        assert_eq!(Int16Value(0x7ACE), Int16Value(0x7ACE));
        assert_eq!(Int32Value(0x1111_BEEF), Int32Value(0x1111_BEEF));
        assert_eq!(Int64Value(0x5555_FACE_CAFE_BABE), Int64Value(0x5555_FACE_CAFE_BABE));
        assert_eq!(Float32Value(45.0), Float32Value(45.0));
        assert_eq!(Float64Value(45.0), Float64Value(45.0));
    }

    #[test]
    fn test_ne() {
        assert_ne!(UInt8Value(0xCE), UInt8Value(0x00));
        assert_ne!(Int16Value(0x7ACE), Int16Value(0xACE));
        assert_ne!(Int32Value(0x1111_BEEF), Int32Value(0xBEEF));
        assert_ne!(Int64Value(0x5555_FACE_CAFE_BABE), Int64Value(0xFACE_CAFE_BABE));
        assert_ne!(Float32Value(45.0), Float32Value(45.7));
        assert_ne!(Float64Value(99.142857), Float64Value(19.48));
    }

    #[test]
    fn test_gt() {
        assert!(UInt8Value(0xCE) > UInt8Value(0xAA));
        assert!(Int16Value(0x7ACE) > Int16Value(0x1111));
        assert!(Int32Value(0x1111_BEEF) > Int32Value(0x0ABC_BEEF));
        assert!(Int64Value(0x5555_FACE_CAFE_BABE) > Int64Value(0x0000_FACE_CAFE_BABE));
        assert!(Float32Value(287.11) > Float32Value(45.3867));
        assert!(Float64Value(359.7854) > Float64Value(99.992));
    }

    #[test]
    fn test_rem() {
        assert_eq!(Int64Value(10) % Int64Value(3), Int64Value(1));
    }

    #[test]
    fn test_pow() {
        let a = Int64Value(5);
        let b = Int64Value(2);
        assert_eq!(a.pow(&b), Float64Value(25.))
    }

    #[test]
    fn test_shl() {
        assert_eq!(Int64Value(1) << Int64Value(5), Int64Value(32));
    }

    #[test]
    fn test_shr() {
        assert_eq!(Int64Value(32) >> Int64Value(5), Int64Value(1));
    }

    #[test]
    fn test_sub() {
        assert_eq!(Float64Value(45.0) - Float64Value(32.0), Float64Value(13.0));
        assert_eq!(Float32Value(45.0) - Float32Value(32.0), Float32Value(13.0));
        assert_eq!(Int64Value(45) - Int64Value(32), Int64Value(13));
        assert_eq!(Int32Value(45) - Int32Value(32), Int32Value(13));
        assert_eq!(Int16Value(45) - Int16Value(32), Int16Value(13));
        assert_eq!(UInt8Value(45) - UInt8Value(32), UInt8Value(13));
    }

    #[test]
    fn test_decode() {
        let buf: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 5, b'H', b'e', b'l', b'l', b'o'];
        assert_eq!(TypedValue::decode(&StringType(5), &buf, 0), StringValue("Hello".into()))
    }

    // #[test]
    // fn test_numbers() {
    //     let encoded = UInt32Value(0xDEADBEEF).encode();
    //     let decoded = Numbers::decode();
    //     assert_eq!(TypedValue::decode(&StringType(5), &buf, 0), StringValue("Hello".into()))
    // }
}