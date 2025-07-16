#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Numbers class
////////////////////////////////////////////////////////////////////

use crate::number_kind::NumberKind;
use crate::number_kind::NumberKind::*;
use crate::numbers::Numbers::*;
use num_traits::real::Real;
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
    U8Value(u8),
    F64Value(f64),
    I64Value(i64),
    U64Value(u64),
    I128Value(i128),
    U128Value(u128),
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
            Numbers::U8Value(id) => id.hash(state),
            Numbers::F64Value(v) => v.to_bits().hash(state),
            Numbers::I64Value(v) => v.hash(state),
            Numbers::U64Value(v) => v.hash(state),
            Numbers::I128Value(v) => v.hash(state),
            Numbers::U128Value(v) => v.hash(state),
            Numbers::NaNValue => 0.hash(state), // Use a fixed hash for NaN values
        }
    }
}

impl Numbers {

    ////////////////////////////////////////////////////////////////////
    //  INSTANCE METHODS
    ////////////////////////////////////////////////////////////////////

    pub fn abs(&self) -> Numbers {
        match self {
            F64Value(n) => F64Value(n.abs()),
            I64Value(n) => I64Value(n.abs()),
            I128Value(n) => I128Value(n.abs()),
            _ => self.clone()
        }
    }

    pub fn ceil(&self) -> Numbers {
        match self.clone() {
            F64Value(n) => F64Value(n.ceil()),
            _ => self.clone()
        }
    }

    pub fn floor(&self) -> Numbers {
        match self {
            F64Value(n) => F64Value(n.floor()),
            _ => self.clone()
        }
    }

    pub fn max(&self, other: &Self) -> Numbers {
        match self {
            U8Value(n) => U8Value(*n.max(&other.to_u8())),
            F64Value(n) => F64Value(n.max(other.to_f64())),
            I64Value(n) => I64Value(*n.max(&other.to_i64())),
            U64Value(n) => U64Value(*n.max(&other.to_u64())),
            I128Value(n) => I128Value(*n.max(&other.to_i128())),
            U128Value(n) => U128Value(*n.max(&other.to_u128())),
            _ => self.clone()
        }
    }

    pub fn min(&self, other: &Self) -> Numbers {
        match self {
            F64Value(n) => F64Value(n.min(other.to_f64())),
            I64Value(n) => I64Value(*n.min(&other.to_i64())),
            U64Value(n) => U64Value(*n.min(&other.to_u64())),
            I128Value(n) => I128Value(*n.min(&other.to_i128())),
            U128Value(n) => U128Value(*n.min(&other.to_u128())),
            _ => self.clone()
        }
    }

    pub fn round(&self) -> Numbers {
        match self.clone() {
            F64Value(n) => F64Value(n.round()),
            _ => self.clone()
        }
    }

    pub fn sqrt(&self) -> Numbers {
        match self.clone() {
            F64Value(n) => F64Value(n.sqrt()),
            n => F64Value(n.to_f64().sqrt()),
        }
    }

    pub fn convert_to(&self, kind: &NumberKind) -> Numbers {
        match kind {
            U8Kind => U8Value(self.to_u8()),
            F64Kind => F64Value(self.to_f64()),
            I64Kind => I64Value(self.to_i64()),
            U64Kind => U64Value(self.to_u64()),
            I128Kind => I128Value(self.to_i128()),
            U128Kind => U128Value(self.to_u128()),
            NaNKind => self.clone(),
        }
    }

    /// encodes the numeric value
    pub fn encode(&self) -> Vec<u8> {
        match self.to_owned() {
            U8Value(number) => number.to_be_bytes().to_vec(),
            F64Value(number) => number.to_be_bytes().to_vec(),
            I64Value(number) => number.to_be_bytes().to_vec(),
            I128Value(number) => number.to_be_bytes().to_vec(),
            U64Value(number) => number.to_be_bytes().to_vec(),
            U128Value(number) => number.to_be_bytes().to_vec(),
            NaNValue => Vec::new(),
        }
    }

    pub fn get_type_name(&self) -> String {
        let result = match *self {
            U8Value(..) => "u8",
            F64Value(..) => "f64",
            I64Value(..) => "i64",
            I128Value(..) => "i128",
            NaNValue => "NaN",
            U64Value(..) => "u64",
            U128Value(..) => "u128",
        };
        result.to_string()
    }

    pub fn is_effectively_zero(&self) -> bool {
        match *self {
            U8Value(number) => number == 0,
            F64Value(number) => number == 0.,
            I64Value(number) => number == 0,
            I128Value(number) => number == 0,
            NaNValue => true,
            U64Value(number) => number == 0,
            U128Value(number) => number == 0,
        }
    }

    pub fn pow(&self, rhs: &Self) -> Self {
        F64Value(num_traits::pow(self.to_f64(), rhs.to_usize()))
    }

    pub fn kind(&self) -> NumberKind {
        match *self {
            U8Value(..) => U8Kind,
            F64Value(..) => F64Kind,
            I64Value(..) => I64Kind,
            I128Value(..) => I128Kind,
            NaNValue => NaNKind,
            U64Value(..) => U64Kind,
            U128Value(..) => U128Kind,
        }
    }

    pub fn to_f64(&self) -> f64 {
        match *self {
            U8Value(number) => number as f64,
            F64Value(number) => number,
            I64Value(number) => number as f64,
            I128Value(number) => number as f64,
            NaNValue => 0.0,
            U64Value(number) => number as f64,
            U128Value(number) => number as f64,
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        match self {
            U8Value(number) => serde_json::json!(number),
            F64Value(number) => serde_json::json!(number),
            I64Value(number) => serde_json::json!(number),
            I128Value(number) => serde_json::json!(number),
            NaNValue => serde_json::json!("NaN"),
            U64Value(number) => serde_json::json!(number),
            U128Value(number) => serde_json::json!(number),
        }
    }

    pub fn to_i64(&self) -> i64 {
        match *self {
            U8Value(number) => number as i64,
            F64Value(number) => number as i64,
            I64Value(number) => number,
            I128Value(number) => number as i64,
            NaNValue => 0i64,
            U64Value(number) => number as i64,
            U128Value(number) => number as i64,
        }
    }

    pub fn to_i128(&self) -> i128 {
        match *self {
            U8Value(number) => number as i128,
            F64Value(number) => number as i128,
            I64Value(number) => number as i128,
            I128Value(number) => number,
            NaNValue => 0i128,
            U64Value(id) => id as i128,
            U128Value(number) => number as i128,
        }
    }

    pub fn to_u8(&self) -> u8 {
        match *self {
            U8Value(number) => number as u8,
            F64Value(number) => number as u8,
            I64Value(number) => number as u8,
            I128Value(number) => number as u8,
            NaNValue => 0u8,
            U64Value(number) => number as u8,
            U128Value(number) => number as u8,
        }
    }

    pub fn to_u16(&self) -> u16 {
        match *self {
            U8Value(number) => number as u16,
            F64Value(number) => number as u16,
            I64Value(number) => number as u16,
            I128Value(number) => number as u16,
            NaNValue => 0u16,
            U64Value(number) => number as u16,
            U128Value(number) => number as u16,
        }
    }

    pub fn to_u32(&self) -> u32 {
        match *self {
            U8Value(number) => number as u32,
            F64Value(number) => number as u32,
            I64Value(number) => number as u32,
            I128Value(number) => number as u32,
            NaNValue => 0u32,
            U64Value(number) => number as u32,
            U128Value(number) => number as u32,
        }
    }

    pub fn to_u64(&self) -> u64 {
        match *self {
            U8Value(number) => number as u64,
            F64Value(number) => number as u64,
            I64Value(number) => number as u64,
            I128Value(number) => number as u64,
            NaNValue => 0u64,
            U64Value(number) => number as u64,
            U128Value(number) => number as u64,
        }
    }

    pub fn to_u128(&self) -> u128 {
        match *self {
            U8Value(number) => number as u128,
            F64Value(number) => number as u128,
            I64Value(number) => number as u128,
            I128Value(number) => number as u128,
            NaNValue => 0,
            U64Value(number) => number as u128,
            U128Value(number) => number,
        }
    }

    pub fn to_usize(&self) -> usize {
        match *self {
            U8Value(number) => number as usize,
            F64Value(number) => number as usize,
            I64Value(number) => number as usize,
            I128Value(number) => number as usize,
            NaNValue => 0usize,
            U64Value(number) => number as usize,
            U128Value(number) => number as usize,
        }
    }

    pub fn unwrap_value(&self) -> String {
        match self {
            U8Value(number) => format!("0x{:02x}", number),
            F64Value(number) => if *number == 0. { "0.0".into() } else { number.to_string() },
            I64Value(number) => number.to_string(),
            I128Value(number) => number.to_string(),
            NaNValue => "NaN".to_string(),
            U64Value(number) => number.to_string(),
            U128Value(number) => number.to_string(),
        }
    }
}

impl Add for Numbers {
    type Output = Numbers;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (NaNValue, _) => NaNValue,
            (_, NaNValue) => NaNValue,
            (U8Value(a), U8Value(b)) => U8Value(a + b),
            (F64Value(a), F64Value(b)) => F64Value(a + b),
            (I128Value(a), I128Value(b)) => I128Value(a + b),
            (I64Value(a), I64Value(b)) => I64Value(a + b),
            (U64Value(a), U64Value(b)) => U64Value(a + b),
            (U128Value(a), U128Value(b)) => U128Value(a + b),
            (a, b) => F64Value(a.to_f64() + b.to_f64())
        }
    }
}

impl BitAnd for Numbers {
    type Output = Numbers;

    fn bitand(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (NaNValue, _) => NaNValue,
            (_, NaNValue) => NaNValue,
            (U8Value(a), U8Value(b)) => U8Value(a & b),
            (F64Value(a), F64Value(b)) => I64Value((a as i64) & (b as i64)),
            (I64Value(a), I64Value(b)) => I64Value(a & b),
            (U64Value(a), U64Value(b)) => U64Value(a & b),
            (I128Value(a), I128Value(b)) => I128Value(a & b),
            (U128Value(a), U128Value(b)) => U128Value(a & b),
            (a, b) => I64Value(a.to_i64() & b.to_i64()),
        }
    }
}

impl BitOr for Numbers {
    type Output = Numbers;

    fn bitor(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (F64Value(a), F64Value(b)) => I64Value((a as i64) | (b as i64)),
            (I128Value(a), I128Value(b)) => I128Value(a | b),
            (I64Value(a), I64Value(b)) => I64Value(a | b),
            (NaNValue, _) => NaNValue,
            (_, NaNValue) => NaNValue,
            (U64Value(a), U64Value(b)) => U64Value(a | b),
            (U128Value(a), U128Value(b)) => U128Value(a | b),
            (a, b) => I64Value(a.to_i64() | b.to_i64()),
        }
    }
}

impl BitXor for Numbers {
    type Output = Numbers;

    fn bitxor(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (F64Value(a), F64Value(b)) => I64Value((a as i64) ^ (b as i64)),
            (I128Value(a), I128Value(b)) => I128Value(a ^ b),
            (I64Value(a), I64Value(b)) => I64Value(a ^ b),
            (NaNValue, _) => NaNValue,
            (_, NaNValue) => NaNValue,
            (U64Value(a), U64Value(b)) => U64Value(a ^ b),
            (U128Value(a), U128Value(b)) => U128Value(a ^ b),
            (a, b) => I64Value(a.to_i64() ^ b.to_i64()),
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
            (NaNValue, _) => NaNValue,
            (_, NaNValue) => NaNValue,
            (_, b) if b.is_effectively_zero() => NaNValue,
            (U8Value(a), U8Value(b)) => U8Value(a / b),
            (F64Value(a), F64Value(b)) => F64Value(a / b),
            (I64Value(a), I64Value(b)) => I64Value(a / b),
            (U64Value(a), U64Value(b)) => U64Value(a / b),
            (I128Value(a), I128Value(b)) => I128Value(a / b),
            (U128Value(a), U128Value(b)) => U128Value(a / b),
            (a, b) => F64Value(a.to_f64() / b.to_f64())
        }
    }
}

impl Mul for Numbers {
    type Output = Numbers;

    fn mul(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (NaNValue, _) => NaNValue,
            (_, NaNValue) => NaNValue,
            (U8Value(a), U8Value(b)) => U8Value(a * b),
            (F64Value(a), F64Value(b)) => F64Value(a * b),
            (I64Value(a), I64Value(b)) => I64Value(a * b),
            (U64Value(a), U64Value(b)) => U64Value(a * b),
            (I128Value(a), I128Value(b)) => I128Value(a * b),
            (U128Value(a), U128Value(b)) => U128Value(a * b),
            (a, b) => F64Value(a.to_f64() * b.to_f64())
        }
    }
}

impl Neg for Numbers {
    type Output = Numbers;

    fn neg(self) -> Self::Output {
        match self {
            NaNValue => NaNValue,
            U8Value(number) => I64Value(-(number as i64)),
            F64Value(number) => F64Value(-number),
            I64Value(number) => I64Value(-number),
            U64Value(number) => I64Value(-(number as i64)),
            I128Value(number) => I128Value(-number),
            U128Value(number) => I128Value(-(number as i128)),
        }
    }
}

impl Not for Numbers {
    type Output = Numbers;

    fn not(self) -> Self::Output {
        match self {
            NaNValue => NaNValue,
            U8Value(number) => I64Value((number as i64).not()),
            F64Value(number) => F64Value(number),
            I64Value(number) => I64Value(number.not()),
            U64Value(number) => I64Value((number as i64).not()),
            I128Value(number) => I128Value(number.not()),
            U128Value(number) => I128Value((number as i128).not()),
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
            (NaNValue, _) => NaNValue,
            (_, NaNValue) => NaNValue,
            (U8Value(a), U8Value(b)) => U8Value(a % b),
            (F64Value(a), F64Value(b)) => F64Value(a % b),
            (I64Value(a), I64Value(b)) => I64Value(a % b),
            (U64Value(a), U64Value(b)) => U64Value(a % b),
            (I128Value(a), I128Value(b)) => I128Value(a % b),
            (U128Value(a), U128Value(b)) => U128Value(a % b),
            (a, b) => F64Value(a.to_f64() % b.to_f64())
        }
    }
}

impl PartialOrd for Numbers {
    fn partial_cmp(&self, rhs: &Self) -> Option<Ordering> {
        match (self, rhs) {
            (U8Value(a), U8Value(b)) => a.partial_cmp(b),
            (F64Value(a), F64Value(b)) => a.partial_cmp(b),
            (I64Value(a), I64Value(b)) => a.partial_cmp(b),
            (U64Value(a), U64Value(b)) => a.partial_cmp(b),
            (I128Value(a), I128Value(b)) => a.partial_cmp(b),
            (U128Value(a), U128Value(b)) => a.partial_cmp(b),
            (a, b) => a.to_i64().partial_cmp(&b.to_i64())
        }
    }
}

impl Shl for Numbers {
    type Output = Numbers;

    fn shl(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (NaNValue, _) => NaNValue,
            (_, NaNValue) => NaNValue,
            (U8Value(a), U8Value(b)) => U8Value(a << b),
            (F64Value(a), F64Value(b)) => I64Value((a as i64) << (b as i64)),
            (I64Value(a), I64Value(b)) => I64Value(a << b),
            (U64Value(a), U64Value(b)) => U64Value(a << b),
            (I128Value(a), I128Value(b)) => I128Value(a << b),
            (U128Value(a), U128Value(b)) => U128Value(a << b),
            (a, b) => I64Value(a.to_i64() << b.to_i64()),
        }
    }
}

impl Shr for Numbers {
    type Output = Numbers;

    fn shr(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (NaNValue, _) => NaNValue,
            (_, NaNValue) => NaNValue,
            (U8Value(a), U8Value(b)) => U8Value(a >> b),
            (F64Value(a), F64Value(b)) => I64Value((a as i64) >> (b as i64)),
            (I64Value(a), I64Value(b)) => I64Value(a >> b),
            (U64Value(a), U64Value(b)) => U64Value(a >> b),
            (I128Value(a), I128Value(b)) => I128Value(a >> b),
            (U128Value(a), U128Value(b)) => U128Value(a >> b),
            (a, b) => I64Value(a.to_i64() >> b.to_i64()),
        }
    }
}

impl Sub for Numbers {
    type Output = Numbers;

    fn sub(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (NaNValue, _) => NaNValue,
            (_, NaNValue) => NaNValue,
            (U8Value(a), U8Value(b)) => U8Value(a - b),
            (F64Value(a), F64Value(b)) => F64Value(a - b),
            (I64Value(a), I64Value(b)) => I64Value(a - b),
            (U64Value(a), U64Value(b)) => U64Value(a - b),
            (I128Value(a), I128Value(b)) => I128Value(a - b),
            (U128Value(a), U128Value(b)) => U128Value(a - b),
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
        assert_eq!(U8Value(45) + U8Value(32), U8Value(77));
        assert_eq!(F64Value(45.0) + F64Value(32.7), F64Value(77.7));
        assert_eq!(I64Value(45) + I64Value(32), I64Value(77));
        assert_eq!(U64Value(45) + U64Value(32), U64Value(77));
        assert_eq!(I128Value(45) + I128Value(32), I128Value(77));
        assert_eq!(U128Value(45) + U128Value(32), U128Value(77));
    }

    #[test]
    fn test_division() {
        assert_eq!(U8Value(45) / U8Value(5), U8Value(9));
        assert_eq!(F64Value(53.2) / F64Value(8.), F64Value(6.65));
        assert_eq!(I64Value(45) / I64Value(5), I64Value(9));
        assert_eq!(U64Value(45) / U64Value(5), U64Value(9));
        assert_eq!(I128Value(45) / I128Value(2), I128Value(22));
        assert_eq!(U128Value(22) / U128Value(3), U128Value(7));
    }

    #[test]
    fn test_equal() {
        assert_eq!(U8Value(45), U8Value(45));
        assert_eq!(F64Value(45.0), F64Value(45.0));
        assert_eq!(I64Value(0x5555_FACE_CAFE_BABE), I64Value(0x5555_FACE_CAFE_BABE));
        assert_eq!(U64Value(0x5555_FACE_CAFE_BABE), U64Value(0x5555_FACE_CAFE_BABE));
        assert_eq!(I128Value(0x5555_FACE_CAFE_BABE), I128Value(0x5555_FACE_CAFE_BABE));
        assert_eq!(U128Value(0x5555_FACE_CAFE_BABE), U128Value(0x5555_FACE_CAFE_BABE));
    }

    #[test]
    fn test_encode_decode() {
        fn verify_codec(expected: Numbers) {
            let encoded = expected.encode();
            println!("verify_codec: encoded {:?}", encoded);
            let actual = expected.kind().decode(&encoded, 0);
            assert_eq!(actual, expected)
        }

        verify_codec(U8Value(47));
        verify_codec(F64Value(747.7));
        verify_codec(I64Value(0xCafeBabe));
        verify_codec(U64Value(0xCafeBabe));
        verify_codec(I128Value(0xDeadFacedBabe));
        verify_codec(U128Value(0xDefaceDaCafeBabe));
    }

    #[test]
    fn test_exponent() {
        assert_eq!(U8Value(5).pow(&U8Value(2)), F64Value(25.));
        assert_eq!(F64Value(5.).pow(&F64Value(2.)), F64Value(25.));
        assert_eq!(I64Value(5).pow(&I64Value(2)), F64Value(25.));
        assert_eq!(U64Value(5).pow(&U64Value(2)), F64Value(25.));
        assert_eq!(I128Value(5).pow(&I128Value(2)), F64Value(25.));
        assert_eq!(U128Value(5).pow(&U128Value(2)), F64Value(25.));
    }

    #[test]
    fn test_greater_than() {
        assert!(U8Value(35) > U8Value(17));
        assert!(F64Value(359.7854) > F64Value(99.992));
        assert!(I64Value(0x5555_FACE_CAFE_BABE) > I64Value(0x0000_FACE_CAFE_BABE));
        assert!(U64Value(0x5555_FACE_CAFE_BABE) > U64Value(0x0000_FACE_CAFE_BABE));
        assert!(I128Value(0x1111_FACE_CAFE_BABE) > I128Value(0x0000_FACE_CAFE_BABE));
        assert!(U128Value(0x7654_FACE_CAFE_BABE) > U128Value(0x0000_FACE_CAFE_BABE));
    }

    #[test]
    fn test_greater_than_or_equal() {
        assert!(U8Value(0xCE) >= U8Value(0xAA));
        assert!(F64Value(359.7854) >= F64Value(99.992));
        assert!(I64Value(0x5555_FACE_CAFE_BABE) >= I64Value(0x0000_FACE_CAFE_BABE));
        assert!(U64Value(0x5555_FACE_CAFE_BABE) >= U64Value(0x0000_FACE_CAFE_BABE));
        assert!(I128Value(0x1111_FACE_CAFE_BABE) >= I128Value(0x0000_FACE_CAFE_BABE));
        assert!(U128Value(0x7654_FACE_CAFE_BABE) >= U128Value(0x0000_FACE_CAFE_BABE));
    }

    #[test]
    fn test_less_than() {
        assert!(U8Value(0x71) < U8Value(0xAA));
        assert!(F64Value(39.7854) < F64Value(99.992));
        assert!(I64Value(0x0000_FACE_CAFE_BABE) < I64Value(0x5555_FACE_CAFE_BABE));
        assert!(U64Value(0x0000_FACE_CAFE_BABE) < U64Value(0x5555_FACE_CAFE_BABE));
        assert!(I128Value(0x0000_FACE_CAFE_BABE) < I128Value(0x1111_FACE_CAFE_BABE));
        assert!(U128Value(0x0000_FACE_CAFE_BABE) < U128Value(0x2233_FACE_CAFE_BABE));
    }

    #[test]
    fn test_less_than_or_equal() {
        assert!(U8Value(0x71) <= U8Value(0xAA));
        assert!(F64Value(39.7854) <= F64Value(99.992));
        assert!(I64Value(0x0000_FACE_CAFE_BABE) <= I64Value(0x5555_FACE_CAFE_BABE));
        assert!(U64Value(0x0000_FACE_CAFE_BABE) <= U64Value(0x5555_FACE_CAFE_BABE));
        assert!(I128Value(0x0000_FACE_CAFE_BABE) <= I128Value(0x1111_FACE_CAFE_BABE));
        assert!(U128Value(0x0000_FACE_CAFE_BABE) <= U128Value(0x2233_FACE_CAFE_BABE));
    }

    #[test]
    fn test_modulus() {
        assert_eq!(U8Value(10) % U8Value(3), U8Value(1));
        assert_eq!(I64Value(10) % I64Value(3), I64Value(1));
        assert_eq!(U64Value(10) % U64Value(3), U64Value(1));
        assert_eq!(I128Value(10) % I128Value(3), I128Value(1));
        assert_eq!(U128Value(10) % U128Value(3), U128Value(1));
    }

    #[test]
    fn test_multiplication() {
        assert_eq!(U8Value(13) * U8Value(2), U8Value(26));
        assert_eq!(F64Value(53.2) * F64Value(32.8), F64Value(1744.96));
        assert_eq!(I64Value(45) * I64Value(5), I64Value(225));
        assert_eq!(U64Value(45) * U64Value(5), U64Value(225));
        assert_eq!(I128Value(45) * I128Value(2), I128Value(90));
        assert_eq!(U128Value(22) * U128Value(3), U128Value(66));
    }

    #[test]
    fn test_not_equal() {
        assert_ne!(U8Value(0xCE), U8Value(0x00));
        assert_ne!(F64Value(99.142857), F64Value(19.48));
        assert_ne!(I64Value(0x5555_FACE_CAFE_BABE), I64Value(0xFACE_CAFE_BABE));
        assert_ne!(U64Value(0x5555_FACE_CAFE_BABE), U64Value(0xFACE_CAFE_BABE));
        assert_ne!(I128Value(0x5555_FACE_CAFE_BABE), I128Value(0xFACE_CAFE_BABE));
        assert_ne!(U128Value(0x5555_FACE_CAFE_BABE), U128Value(0xFACE_CAFE_BABE));
    }

    #[test]
    fn test_shift_left() {
        assert_eq!(U8Value(1) << U8Value(5), U8Value(32));
        assert_eq!(I64Value(1) << I64Value(5), I64Value(32));
        assert_eq!(U64Value(1) << U64Value(5), U64Value(32));
        assert_eq!(I128Value(1) << I128Value(5), I128Value(32));
        assert_eq!(U128Value(1) << U128Value(5), U128Value(32));
    }

    #[test]
    fn test_shift_right() {
        assert_eq!(U8Value(32) >> U8Value(5), U8Value(1));
        assert_eq!(I64Value(32) >> I64Value(5), I64Value(1));
        assert_eq!(U64Value(32) >> U64Value(5), U64Value(1));
        assert_eq!(I128Value(32) >> I128Value(5), I128Value(1));
        assert_eq!(U128Value(32) >> U128Value(5), U128Value(1));
    }

    #[test]
    fn test_subtraction() {
        assert_eq!(U8Value(45) - U8Value(32), U8Value(13));
        assert_eq!(F64Value(45.0) - F64Value(32.0), F64Value(13.0));
        assert_eq!(I64Value(45) - I64Value(32), I64Value(13));
        assert_eq!(U64Value(45) - U64Value(32), U64Value(13));
        assert_eq!(I128Value(45) - I128Value(32), I128Value(13));
        assert_eq!(U128Value(45) - U128Value(32), U128Value(13));
    }
}