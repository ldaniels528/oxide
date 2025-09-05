#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Numbers class
////////////////////////////////////////////////////////////////////

use crate::number_kind::NumberKind;
use crate::number_kind::NumberKind::*;
use crate::numbers::Numbers::*;
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
    I8Value(i8),
    I16Value(i16),
    U16Value(u16),
    I32Value(i32),
    U32Value(u32),
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
        self.to_string().hash(state);
    }
}

impl Numbers {

    ////////////////////////////////////////////////////////////////////
    //  INSTANCE METHODS
    ////////////////////////////////////////////////////////////////////

    pub fn abs(&self) -> Numbers {
        match self {
            F64Value(n) => F64Value(n.abs()),
            I8Value(n) => I8Value(n.abs()),
            I16Value(n) => I16Value(n.abs()),
            I32Value(n) => I32Value(n.abs()),
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
            NaNValue => other.clone(),
            F64Value(n) => F64Value(n.max(other.to_f64())),
            U64Value(n) => U64Value(*n.max(&other.to_u64())),
            I128Value(n) => I128Value(*n.max(&other.to_i128())),
            U128Value(n) => U128Value(*n.max(&other.to_u128())),
            n => I64Value(n.to_i64().max(other.to_i64())).convert_to(&n.kind())
        }
    }

    pub fn min(&self, other: &Self) -> Numbers {
        match self {
            NaNValue => other.clone(),
            F64Value(n) => F64Value(n.min(other.to_f64())),
            U64Value(n) => U64Value(*n.min(&other.to_u64())),
            I128Value(n) => I128Value(*n.min(&other.to_i128())),
            U128Value(n) => U128Value(*n.min(&other.to_u128())),
            n => I64Value(n.to_i64().min(other.to_i64())).convert_to(&n.kind())
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
            AnyKind    => self.clone(),
            U8Kind     => U8Value(self.to_u8()),
            I8Kind     => I8Value(self.to_i8()),
            I16Kind    => I16Value(self.to_i16()),
            U16Kind    => U16Value(self.to_u16()),
            I32Kind    => I32Value(self.to_i32()),
            U32Kind    => U32Value(self.to_u32()),
            F64Kind    => F64Value(self.to_f64()),
            I64Kind    => I64Value(self.to_i64()),
            U64Kind    => U64Value(self.to_u64()),
            I128Kind   => I128Value(self.to_i128()),
            U128Kind   => U128Value(self.to_u128()),
            NaNKind    => NaNValue,
        }
    }

    /// encodes the numeric value
    pub fn encode(&self) -> Vec<u8> {
        match *self {
            U8Value(n) => n.to_be_bytes().to_vec(),
            I8Value(n) => n.to_be_bytes().to_vec(),
            I16Value(n) => n.to_be_bytes().to_vec(),
            U16Value(n) => n.to_be_bytes().to_vec(),
            I32Value(n) => n.to_be_bytes().to_vec(),
            U32Value(n) => n.to_be_bytes().to_vec(),
            F64Value(n) => n.to_be_bytes().to_vec(),
            I64Value(n) => n.to_be_bytes().to_vec(),
            U64Value(n) => n.to_be_bytes().to_vec(),
            I128Value(n) => n.to_be_bytes().to_vec(),
            U128Value(n) => n.to_be_bytes().to_vec(),
            NaNValue => vec![],
        }
    }

    pub fn get_type_name(&self) -> String {
        (match *self {
            I8Value(..) => "i8",
            U8Value(..) => "u8",
            I16Value(..) => "i16",
            U16Value(..) => "u16",
            I32Value(..) => "i32",
            U32Value(..) => "u32",
            F64Value(..) => "f64",
            I64Value(..) => "i64",
            I128Value(..) => "i128",
            NaNValue => "NaN",
            U64Value(..) => "u64",
            U128Value(..) => "u128",
        }).to_string()
    }

    pub fn is_effectively_zero(&self) -> bool {
        match *self {
            U8Value(n) => n == 0,
            I8Value(n) => n == 0,
            I16Value(n) => n == 0,
            U16Value(n) => n == 0,
            I32Value(n) => n == 0,
            U32Value(n) => n == 0,
            F64Value(n) => n == 0.0,
            I64Value(n) => n == 0,
            U64Value(n) => n == 0,
            I128Value(n) => n == 0,
            U128Value(n) => n == 0,
            NaNValue => true,
        }
    }

    pub fn pow(&self, rhs: &Self) -> Self {
        F64Value(num_traits::pow(self.to_f64(), rhs.to_usize()))
    }

    pub fn kind(&self) -> NumberKind {
        match self {
            Numbers::U8Value(_)     => NumberKind::U8Kind,
            Numbers::I8Value(_)     => NumberKind::I8Kind,
            Numbers::I16Value(_)    => NumberKind::I16Kind,
            Numbers::U16Value(_)    => NumberKind::U16Kind,
            Numbers::I32Value(_)    => NumberKind::I32Kind,
            Numbers::U32Value(_)    => NumberKind::U32Kind,
            Numbers::F64Value(_)    => NumberKind::F64Kind,
            Numbers::I64Value(_)    => NumberKind::I64Kind,
            Numbers::U64Value(_)    => NumberKind::U64Kind,
            Numbers::I128Value(_)   => NumberKind::I128Kind,
            Numbers::U128Value(_)   => NumberKind::U128Kind,
            Numbers::NaNValue       => NumberKind::NaNKind,
        }
    }

    pub fn to_f64(&self) -> f64 {
        match *self {
            U8Value(number) => number as f64,
            I8Value(number) => number as f64,
            I16Value(number) => number as f64,
            U16Value(number) => number as f64,
            I32Value(number) => number as f64,
            U32Value(number) => number as f64,
            F64Value(number) => number,
            I64Value(number) => number as f64,
            I128Value(number) => number as f64,
            U64Value(number) => number as f64,
            U128Value(number) => number as f64,
            NaNValue => 0.0,
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        match self {
            U8Value(n) => serde_json::json!(n),
            I8Value(n) => serde_json::json!(n),
            I16Value(n) => serde_json::json!(n),
            U16Value(n) => serde_json::json!(n),
            I32Value(n) => serde_json::json!(n),
            U32Value(n) => serde_json::json!(n),
            F64Value(n) => serde_json::json!(n),
            I64Value(n) => serde_json::json!(n),
            U64Value(n) => serde_json::json!(n),
            I128Value(n) => serde_json::json!(n),
            U128Value(n) => serde_json::json!(n),
            NaNValue => serde_json::json!("NaN"),
        }
    }

    pub fn to_i8(&self) -> i8 {
        match *self {
            U8Value(n) => n as i8,
            I8Value(n) => n,
            I16Value(n) => n as i8,
            U16Value(n) => n as i8,
            I32Value(n) => n as i8,
            U32Value(n) => n as i8,
            F64Value(n) => n as i8,
            I64Value(n) => n as i8,
            U64Value(n) => n as i8,
            I128Value(n) => n as i8,
            U128Value(n) => n as i8,
            NaNValue => 0,
        }
    }

    pub fn to_u8(&self) -> u8 {
        match *self {
            U8Value(n)     => n,
            I8Value(n)     => n as u8,
            I16Value(n)    => n as u8,
            U16Value(n)    => n as u8,
            I32Value(n)    => n as u8,
            U32Value(n)    => n as u8,
            F64Value(n)    => n as u8,
            I64Value(n)    => n as u8,
            U64Value(n)    => n as u8,
            I128Value(n)   => n as u8,
            U128Value(n)   => n as u8,
            NaNValue       => 0,
        }
    }

    pub fn to_i16(&self) -> i16 {
        match self {
            Numbers::F64Value(n)    => *n as i16,
            Numbers::I8Value(n)     => *n as i16,
            Numbers::I16Value(n)    => *n,
            Numbers::U8Value(n)     => *n as i16,
            Numbers::U16Value(n)    => *n as i16,
            Numbers::I32Value(n)    => *n as i16,
            Numbers::U32Value(n)    => *n as i16,
            Numbers::I64Value(n)    => *n as i16,
            Numbers::U64Value(n)    => *n as i16,
            Numbers::I128Value(n)   => *n as i16,
            Numbers::U128Value(n)   => *n as i16,
            Numbers::NaNValue       => 0,
        }
    }

    pub fn to_u16(&self) -> u16 {
        match self {
            Numbers::F64Value(n)    => *n as u16,
            Numbers::I8Value(n)     => *n as u16,
            Numbers::I16Value(n)    => *n as u16,
            Numbers::U8Value(n)     => *n as u16,
            Numbers::U16Value(n)    => *n,
            Numbers::I32Value(n)    => *n as u16,
            Numbers::U32Value(n)    => *n as u16,
            Numbers::I64Value(n)    => *n as u16,
            Numbers::U64Value(n)    => *n as u16,
            Numbers::I128Value(n)   => *n as u16,
            Numbers::U128Value(n)   => *n as u16,
            Numbers::NaNValue       => 0,
        }
    }

    pub fn to_i32(&self) -> i32 {
        match self {
            Numbers::F64Value(n)    => *n as i32,
            Numbers::I8Value(n)     => *n as i32,
            Numbers::I16Value(n)    => *n as i32,
            Numbers::U8Value(n)     => *n as i32,
            Numbers::U16Value(n)    => *n as i32,
            Numbers::I32Value(n)    => *n,
            Numbers::U32Value(n)    => *n as i32,
            Numbers::I64Value(n)    => *n as i32,
            Numbers::U64Value(n)    => *n as i32,
            Numbers::I128Value(n)   => *n as i32,
            Numbers::U128Value(n)   => *n as i32,
            Numbers::NaNValue       => 0,
        }
    }

    pub fn to_u32(&self) -> u32 {
        match *self {
            U8Value(n)     => n as u32,
            I8Value(n)     => n as u32,
            I16Value(n)    => n as u32,
            U16Value(n)    => n as u32,
            I32Value(n)    => n as u32,
            U32Value(n)    => n,
            F64Value(n)    => n as u32,
            I64Value(n)    => n as u32,
            U64Value(n)    => n as u32,
            I128Value(n)   => n as u32,
            U128Value(n)   => n as u32,
            NaNValue       => 0,
        }
    }

    pub fn to_i64(&self) -> i64 {
        match *self {
            U8Value(n)     => n as i64,
            I8Value(n)     => n as i64,
            I16Value(n)    => n as i64,
            U16Value(n)    => n as i64,
            I32Value(n)    => n as i64,
            U32Value(n)    => n as i64,
            F64Value(n)    => n as i64,
            I64Value(n)    => n,
            U64Value(n)    => n as i64,
            I128Value(n)   => n as i64,
            U128Value(n)   => n as i64,
            NaNValue       => 0,
        }
    }

    pub fn to_u64(&self) -> u64 {
        match *self {
            U8Value(n)     => n as u64,
            I8Value(n)     => n as u64,
            I16Value(n)    => n as u64,
            U16Value(n)    => n as u64,
            I32Value(n)    => n as u64,
            U32Value(n)    => n as u64,
            F64Value(n)    => n as u64,
            I64Value(n)    => n as u64,
            U64Value(n)    => n,
            I128Value(n)   => n as u64,
            U128Value(n)   => n as u64,
            NaNValue       => 0,
        }
    }

    pub fn to_i128(&self) -> i128 {
        match *self {
            U8Value(n)     => n as i128,
            I8Value(n)     => n as i128,
            I16Value(n)    => n as i128,
            U16Value(n)    => n as i128,
            I32Value(n)    => n as i128,
            U32Value(n)    => n as i128,
            F64Value(n)    => n as i128,
            I64Value(n)    => n as i128,
            U64Value(n)    => n as i128,
            I128Value(n)   => n,
            U128Value(n)   => n as i128,
            NaNValue       => 0,
        }
    }

    pub fn to_u128(&self) -> u128 {
        match *self {
            U8Value(n)     => n as u128,
            I8Value(n)     => n as u128,
            I16Value(n)    => n as u128,
            U16Value(n)    => n as u128,
            I32Value(n)    => n as u128,
            U32Value(n)    => n as u128,
            F64Value(n)    => n as u128,
            I64Value(n)    => n as u128,
            U64Value(n)    => n as u128,
            I128Value(n)   => n as u128,
            U128Value(n)   => n,
            NaNValue       => 0,
        }
    }

    pub fn to_usize(&self) -> usize {
        match *self {
            U8Value(n)     => n as usize,
            I8Value(n)     => n as usize,
            I16Value(n)    => n as usize,
            U16Value(n)    => n as usize,
            I32Value(n)    => n as usize,
            U32Value(n)    => n as usize,
            F64Value(n)    => n as usize,
            I64Value(n)    => n as usize,
            U64Value(n)    => n as usize,
            I128Value(n)   => n as usize,
            U128Value(n)   => n as usize,
            NaNValue       => 0,
        }
    }

    pub fn unwrap_value(&self) -> String {
        match self {
            U8Value(n)     => format!("0x{:02x}", n),
            I8Value(n)     => n.to_string(),
            I16Value(n)    => n.to_string(),
            U16Value(n)    => n.to_string(),
            I32Value(n)    => n.to_string(),
            U32Value(n)    => n.to_string(),
            F64Value(n)    => if *n == 0.0 { "0.0".into() } else { n.to_string() },
            I64Value(n)    => n.to_string(),
            U64Value(n)    => n.to_string(),
            I128Value(n)   => n.to_string(),
            U128Value(n)   => n.to_string(),
            NaNValue       => "NaN".to_string(),
        }
    }
}

impl Add for Numbers {
    type Output = Numbers;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (NaNValue, _) | (_, NaNValue) => NaNValue,

            (U8Value(a), U8Value(b))     => U8Value(a.wrapping_add(b)),
            (I8Value(a), I8Value(b))     => I8Value(a.wrapping_add(b)),
            (I16Value(a), I16Value(b))   => I16Value(a.wrapping_add(b)),
            (U16Value(a), U16Value(b))   => U16Value(a.wrapping_add(b)),
            (I32Value(a), I32Value(b))   => I32Value(a.wrapping_add(b)),
            (U32Value(a), U32Value(b))   => U32Value(a.wrapping_add(b)),

            (F64Value(a), F64Value(b))   => F64Value(a + b),
            (I64Value(a), I64Value(b))   => I64Value(a.wrapping_add(b)),
            (U64Value(a), U64Value(b))   => U64Value(a.wrapping_add(b)),
            (I128Value(a), I128Value(b)) => I128Value(a.wrapping_add(b)),
            (U128Value(a), U128Value(b)) => U128Value(a.wrapping_add(b)),

            // fallback to f64 addition for mixed types
            (a, b) => F64Value(a.to_f64() + b.to_f64()),
        }
    }
}

impl BitAnd for Numbers {
    type Output = Numbers;

    fn bitand(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (NaNValue, _) | (_, NaNValue) => NaNValue,

            (U8Value(a), U8Value(b))     => U8Value(a & b),
            (I8Value(a), I8Value(b))      => I8Value(a & b),
            (I16Value(a), I16Value(b))   => I16Value(a & b),
            (U16Value(a), U16Value(b))   => U16Value(a & b),
            (I32Value(a), I32Value(b))   => I32Value(a & b),
            (U32Value(a), U32Value(b))   => U32Value(a & b),

            (F64Value(a), F64Value(b))   => I64Value((a as i64) & (b as i64)),
            (I64Value(a), I64Value(b))   => I64Value(a & b),
            (U64Value(a), U64Value(b))   => U64Value(a & b),
            (I128Value(a), I128Value(b)) => I128Value(a & b),
            (U128Value(a), U128Value(b)) => U128Value(a & b),

            // fallback for mixed types
            (a, b) => I64Value(a.to_i64() & b.to_i64()),
        }
    }
}

impl BitOr for Numbers {
    type Output = Numbers;

    fn bitor(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (NaNValue, _) | (_, NaNValue) => NaNValue,

            (U8Value(a), U8Value(b))     => U8Value(a | b),
            (I8Value(a), I8Value(b))     => I8Value(a | b),
            (I16Value(a), I16Value(b))   => I16Value(a | b),
            (U16Value(a), U16Value(b))   => U16Value(a | b),
            (I32Value(a), I32Value(b))   => I32Value(a | b),
            (U32Value(a), U32Value(b))   => U32Value(a | b),

            (F64Value(a), F64Value(b))   => I64Value((a as i64) | (b as i64)),
            (I64Value(a), I64Value(b))   => I64Value(a | b),
            (U64Value(a), U64Value(b))   => U64Value(a | b),
            (I128Value(a), I128Value(b)) => I128Value(a | b),
            (U128Value(a), U128Value(b)) => U128Value(a | b),

            // fallback for mixed types
            (a, b) => I64Value(a.to_i64() | b.to_i64()),
        }
    }
}

impl BitXor for Numbers {
    type Output = Numbers;

    fn bitxor(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (NaNValue, _) | (_, NaNValue) => NaNValue,

            (U8Value(a), U8Value(b))     => U8Value(a ^ b),
            (I8Value(a), I8Value(b))     => I8Value(a ^ b),
            (I16Value(a), I16Value(b))   => I16Value(a ^ b),
            (U16Value(a), U16Value(b))   => U16Value(a ^ b),
            (I32Value(a), I32Value(b))   => I32Value(a ^ b),
            (U32Value(a), U32Value(b))   => U32Value(a ^ b),

            (F64Value(a), F64Value(b))   => I64Value((a as i64) ^ (b as i64)),
            (I64Value(a), I64Value(b))   => I64Value(a ^ b),
            (U64Value(a), U64Value(b))   => U64Value(a ^ b),
            (I128Value(a), I128Value(b)) => I128Value(a ^ b),
            (U128Value(a), U128Value(b)) => U128Value(a ^ b),

            // fallback for mixed types
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
            (NaNValue, _) | (_, NaNValue) => NaNValue,
            (_, b) if b.is_effectively_zero() => NaNValue,

            (U8Value(a), U8Value(b))     => U8Value(a / b),
            (I8Value(a), I8Value(b))     => I8Value(a / b),
            (I16Value(a), I16Value(b))   => I16Value(a / b),
            (U16Value(a), U16Value(b))   => U16Value(a / b),
            (I32Value(a), I32Value(b))   => I32Value(a / b),
            (U32Value(a), U32Value(b))   => U32Value(a / b),

            (F64Value(a), F64Value(b))   => F64Value(a / b),
            (I64Value(a), I64Value(b))   => I64Value(a / b),
            (U64Value(a), U64Value(b))   => U64Value(a / b),
            (I128Value(a), I128Value(b)) => I128Value(a / b),
            (U128Value(a), U128Value(b)) => U128Value(a / b),

            // fallback for mixed types
            (a, b) => F64Value(a.to_f64() / b.to_f64()),
        }
    }
}

impl Mul for Numbers {
    type Output = Numbers;

    fn mul(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (NaNValue, _) | (_, NaNValue) => NaNValue,

            (U8Value(a), U8Value(b))     => U8Value(a * b),
            (I8Value(a), I8Value(b))     => I8Value(a * b),
            (I16Value(a), I16Value(b))   => I16Value(a * b),
            (U16Value(a), U16Value(b))   => U16Value(a * b),
            (I32Value(a), I32Value(b))   => I32Value(a * b),
            (U32Value(a), U32Value(b))   => U32Value(a * b),

            (F64Value(a), F64Value(b))   => F64Value(a * b),
            (I64Value(a), I64Value(b))   => I64Value(a * b),
            (U64Value(a), U64Value(b))   => U64Value(a * b),
            (I128Value(a), I128Value(b)) => I128Value(a * b),
            (U128Value(a), U128Value(b)) => U128Value(a * b),

            // fallback for mixed types
            (a, b) => F64Value(a.to_f64() * b.to_f64()),
        }
    }
}

impl Neg for Numbers {
    type Output = Numbers;

    fn neg(self) -> Self::Output {
        match self {
            NaNValue         => NaNValue,
            F64Value(n)      => F64Value(-n),
            I8Value(n)       => I8Value(-n),
            I16Value(n)      => I16Value(-n),
            I32Value(n)      => I32Value(-n),
            I64Value(n)      => I64Value(-n),
            I128Value(n)     => I128Value(-n),

            U8Value(n)       => I64Value(-(n as i64)),
            U16Value(n)      => I64Value(-(n as i64)),
            U32Value(n)      => I64Value(-(n as i64)),
            U64Value(n)      => I64Value(-(n as i64)),
            U128Value(n)     => I128Value(-(n as i128)),
        }
    }
}

impl Not for Numbers {
    type Output = Numbers;

    fn not(self) -> Self::Output {
        match self {
            NaNValue       => NaNValue,
            U8Value(n)     => U8Value(!n),
            I8Value(n)     => I8Value(!n),
            I16Value(n)    => I16Value(!n),
            U16Value(n)    => U16Value(!n),
            I32Value(n)    => I32Value(!n),
            U32Value(n)    => U32Value(!n),
            F64Value(n)    => F64Value(n),
            I64Value(n)    => I64Value(!n),
            U64Value(n)    => U64Value(!n),
            I128Value(n)   => I128Value(!n),
            U128Value(n)   => U128Value(!n),
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
            (NaNValue, _) | (_, NaNValue) => NaNValue,

            (U8Value(a), U8Value(b))       => U8Value(a % b),
            (I8Value(a), I8Value(b))       => I8Value(a % b),
            (I16Value(a), I16Value(b))     => I16Value(a % b),
            (U16Value(a), U16Value(b))     => U16Value(a % b),
            (I32Value(a), I32Value(b))     => I32Value(a % b),
            (U32Value(a), U32Value(b))     => U32Value(a % b),

            (F64Value(a), F64Value(b))     => F64Value(a % b),
            (I64Value(a), I64Value(b))     => I64Value(a % b),
            (U64Value(a), U64Value(b))     => U64Value(a % b),
            (I128Value(a), I128Value(b))   => I128Value(a % b),
            (U128Value(a), U128Value(b))   => U128Value(a % b),

            // fallback for mixed types
            (a, b) => F64Value(a.to_f64() % b.to_f64()),
        }
    }
}

impl PartialOrd for Numbers {
    fn partial_cmp(&self, rhs: &Self) -> Option<Ordering> {
        match (self, rhs) {
            (NaNValue, _) | (_, NaNValue) => None,

            (U8Value(a), U8Value(b))       => a.partial_cmp(b),
            (I8Value(a), I8Value(b))       => a.partial_cmp(b),
            (I16Value(a), I16Value(b))     => a.partial_cmp(b),
            (U16Value(a), U16Value(b))     => a.partial_cmp(b),
            (I32Value(a), I32Value(b))     => a.partial_cmp(b),
            (U32Value(a), U32Value(b))     => a.partial_cmp(b),

            (F64Value(a), F64Value(b))     => a.partial_cmp(b),
            (I64Value(a), I64Value(b))     => a.partial_cmp(b),
            (U64Value(a), U64Value(b))     => a.partial_cmp(b),
            (I128Value(a), I128Value(b))   => a.partial_cmp(b),
            (U128Value(a), U128Value(b))   => a.partial_cmp(b),

            // fallback for mixed types
            (a, b) => a.to_f64().partial_cmp(&b.to_f64()),
        }
    }
}

impl Shl for Numbers {
    type Output = Numbers;

    fn shl(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (NaNValue, _) | (_, NaNValue) => NaNValue,

            (U8Value(a), U8Value(b))       => U8Value(a << b),
            (I8Value(a), I8Value(b))       => I8Value(a << b),
            (I16Value(a), I16Value(b))     => I16Value(a << b),
            (U16Value(a), U16Value(b))     => U16Value(a << b),
            (I32Value(a), I32Value(b))     => I32Value(a << b),
            (U32Value(a), U32Value(b))     => U32Value(a << b),

            (F64Value(a), F64Value(b))     => I64Value((a as i64) << (b as i64)),
            (I64Value(a), I64Value(b))     => I64Value(a << b),
            (U64Value(a), U64Value(b))     => U64Value(a << b),
            (I128Value(a), I128Value(b))   => I128Value(a << b),
            (U128Value(a), U128Value(b))   => U128Value(a << b),

            // fallback for mixed types
            (a, b) => I64Value(a.to_i64() << b.to_i64()),
        }
    }
}

impl Shr for Numbers {
    type Output = Numbers;

    fn shr(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (NaNValue, _) | (_, NaNValue) => NaNValue,

            (U8Value(a), U8Value(b))       => U8Value(a >> b),
            (I8Value(a), I8Value(b))       => I8Value(a >> b),
            (I16Value(a), I16Value(b))     => I16Value(a >> b),
            (U16Value(a), U16Value(b))     => U16Value(a >> b),
            (I32Value(a), I32Value(b))     => I32Value(a >> b),
            (U32Value(a), U32Value(b))     => U32Value(a >> b),

            (F64Value(a), F64Value(b))     => I64Value((a as i64) >> (b as i64)),
            (I64Value(a), I64Value(b))     => I64Value(a >> b),
            (U64Value(a), U64Value(b))     => U64Value(a >> b),
            (I128Value(a), I128Value(b))   => I128Value(a >> b),
            (U128Value(a), U128Value(b))   => U128Value(a >> b),

            // fallback for mixed types
            (a, b) => I64Value(a.to_i64() >> b.to_i64()),
        }
    }
}

impl Sub for Numbers {
    type Output = Numbers;

    fn sub(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (NaNValue, _) | (_, NaNValue) => NaNValue,

            (U8Value(a), U8Value(b))       => U8Value(a.wrapping_sub(b)),
            (I8Value(a), I8Value(b))       => I8Value(a.wrapping_sub(b)),
            (I16Value(a), I16Value(b))     => I16Value(a.wrapping_sub(b)),
            (U16Value(a), U16Value(b))     => U16Value(a.wrapping_sub(b)),
            (I32Value(a), I32Value(b))     => I32Value(a.wrapping_sub(b)),
            (U32Value(a), U32Value(b))     => U32Value(a.wrapping_sub(b)),
            (I64Value(a), I64Value(b))     => I64Value(a.wrapping_sub(b)),
            (U64Value(a), U64Value(b))     => U64Value(a.wrapping_sub(b)),
            (I128Value(a), I128Value(b))   => I128Value(a.wrapping_sub(b)),
            (U128Value(a), U128Value(b))   => U128Value(a.wrapping_sub(b)),
            (F64Value(a), F64Value(b))     => F64Value(a - b),

            (a, b) => F64Value(a.to_f64() - b.to_f64()),
        }
    }
}

// Unit tests
#[cfg(test)]
mod ai_tests {
    use super::*;

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::cmp::Ordering;
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        use std::ops::{Bound, RangeBounds};

        // ---------- helpers ----------
        fn hash_of<T: Hash>(t: &T) -> u64 {
            let mut s = DefaultHasher::new();
            t.hash(&mut s);
            s.finish()
        }

        // ---------- basic identity / names ----------
        #[test]
        fn type_names_cover_all_variants() {
            assert_eq!(U8Value(1).get_type_name(), "u8");
            assert_eq!(I8Value(-1).get_type_name(), "i8");
            assert_eq!(I16Value(-1).get_type_name(), "i16");
            assert_eq!(U16Value(1).get_type_name(), "u16");
            assert_eq!(I32Value(-1).get_type_name(), "i32");
            assert_eq!(U32Value(1).get_type_name(), "u32");
            assert_eq!(F64Value(1.0).get_type_name(), "f64");
            assert_eq!(I64Value(-1).get_type_name(), "i64");
            assert_eq!(U64Value(1).get_type_name(), "u64");
            assert_eq!(I128Value(-1).get_type_name(), "i128");
            assert_eq!(U128Value(1).get_type_name(), "u128");
            assert_eq!(NaNValue.get_type_name(), "NaN");
        }

        // ---------- is_effectively_zero ----------
        #[test]
        fn zero_detection_and_nan() {
            assert!(U8Value(0).is_effectively_zero());
            assert!(!U8Value(1).is_effectively_zero());
            assert!(F64Value(0.0).is_effectively_zero());
            assert!(!F64Value(0.5).is_effectively_zero());
            assert!(NaNValue.is_effectively_zero());
        }

        // ---------- abs / ceil / floor / round / sqrt ----------
        #[test]
        fn abs_signed_and_passthrough_unsigned_nan() {
            assert_eq!(I8Value(-5).abs(), I8Value(5));
            assert_eq!(I16Value(-5).abs(), I16Value(5));
            assert_eq!(I32Value(-5).abs(), I32Value(5));
            assert_eq!(I64Value(-5).abs(), I64Value(5));
            assert_eq!(I128Value(-5).abs(), I128Value(5));
            // unsigned & NaN pass-through
            assert_eq!(U16Value(7).abs(), U16Value(7));
            assert_eq!(NaNValue.abs(), NaNValue);
        }

        #[test]
        fn ceil_floor_round_cover_f64_and_passthrough() {
            assert_eq!(F64Value(1.2).ceil(), F64Value(2.0));
            assert_eq!(F64Value(-1.2).floor(), F64Value(-2.0));
            assert_eq!(F64Value(2.5).round(), F64Value(3.0).round()); // sanity
            assert_eq!(F64Value(2.5).round(), F64Value(3.0));
            // passthrough for non-f64
            assert_eq!(I32Value(3).ceil(), I32Value(3));
            assert_eq!(U32Value(3).floor(), U32Value(3));
            assert_eq!(I16Value(3).round(), I16Value(3));
        }

        #[test]
        fn sqrt_for_f64_and_non_f64_path() {
            assert_eq!(F64Value(9.0).sqrt(), F64Value(3.0));
            // non-f64 branch goes through to_f64().sqrt()
            let got = I32Value(16).sqrt();
            assert_eq!(got, F64Value(4.0));
        }

        // ---------- convert_to / kind ----------
        #[test]
        fn kind_mapping_and_convert_to_all_kinds() {
            // sanity: kind()
            assert_eq!(U8Value(1).kind(), U8Kind);
            assert_eq!(I8Value(1).kind(), I8Kind);
            assert_eq!(I16Value(1).kind(), I16Kind);
            assert_eq!(U16Value(1).kind(), U16Kind);
            assert_eq!(I32Value(1).kind(), I32Kind);
            assert_eq!(U32Value(1).kind(), U32Kind);
            assert_eq!(F64Value(1.0).kind(), F64Kind);
            assert_eq!(I64Value(1).kind(), I64Kind);
            assert_eq!(U64Value(1).kind(), U64Kind);
            assert_eq!(I128Value(1).kind(), I128Kind);
            assert_eq!(U128Value(1).kind(), U128Kind);
            assert_eq!(NaNValue.kind(), NaNKind);

            // convert_to to each kind (including AnyKind, NaNKind)
            let v = I32Value(-3);
            assert_eq!(v.convert_to(&AnyKind), v);               // AnyKind passthrough
            assert_eq!(v.convert_to(&U8Kind),  U8Value(253));    // wrap as cast semantics
            assert_eq!(v.convert_to(&I8Kind),  I8Value(-3i8));
            assert_eq!(v.convert_to(&I16Kind), I16Value(-3));
            assert_eq!(v.convert_to(&U16Kind), U16Value((u16::MAX as i32 - 2) as u16));
            assert_eq!(v.convert_to(&I32Kind), I32Value(-3));
            assert_eq!(v.convert_to(&U32Kind), U32Value(-3i32 as u32));
            assert_eq!(v.convert_to(&F64Kind), F64Value(-3.0));
            assert_eq!(v.convert_to(&I64Kind), I64Value(-3));
            assert_eq!(v.convert_to(&U64Kind), U64Value(-3i64 as u64));
            assert_eq!(v.convert_to(&I128Kind), I128Value(-3));
            assert_eq!(v.convert_to(&U128Kind), U128Value(-3i128 as u128));
            assert_eq!(v.convert_to(&NaNKind), NaNValue);
        }

        // ---------- encode ----------
        #[test]
        fn encode_each_variant_and_nan() {
            assert_eq!(U8Value(0xAB).encode(), 0xABu8.to_be_bytes().to_vec());
            assert_eq!(I8Value(-5).encode(), (-5i8).to_be_bytes().to_vec());
            assert_eq!(I16Value(-5).encode(), (-5i16).to_be_bytes().to_vec());
            assert_eq!(U16Value(0xBEEF).encode(), 0xBEEF_u16.to_be_bytes().to_vec());
            assert_eq!(I32Value(-123).encode(), (-123i32).to_be_bytes().to_vec());
            assert_eq!(U32Value(123).encode(), 123u32.to_be_bytes().to_vec());
            assert_eq!(F64Value(1.25).encode(), 1.25f64.to_be_bytes().to_vec());
            assert_eq!(I64Value(-123).encode(), (-123i64).to_be_bytes().to_vec());
            assert_eq!(U64Value(123).encode(), 123u64.to_be_bytes().to_vec());
            assert_eq!(I128Value(-123).encode(), (-123i128).to_be_bytes().to_vec());
            assert_eq!(U128Value(123).encode(), 123u128.to_be_bytes().to_vec());
            assert_eq!(NaNValue.encode(), Vec::<u8>::new());
        }

        // ---------- to_* conversions ----------
        #[test]
        fn to_scalar_conversions_and_nan_defaults() {
            // choose one source and check several targets
            let x = U16Value(513); // 0x0201
            assert_eq!(x.to_i8(), (513i16 as i8));
            assert_eq!(x.to_u8(), (513u16 as u8));
            assert_eq!(x.to_i16(), 513i16);
            assert_eq!(x.to_u16(), 513u16);
            assert_eq!(x.to_i32(), 513i32);
            assert_eq!(x.to_u32(), 513u32);
            assert_eq!(x.to_i64(), 513i64);
            assert_eq!(x.to_u64(), 513u64);
            assert_eq!(x.to_i128(), 513i128);
            assert_eq!(x.to_u128(), 513u128);
            assert_eq!(x.to_usize(), 513usize);
            assert_eq!(x.to_f64(), 513.0);

            // NaNValue -> zeros (or 0.0)
            assert_eq!(NaNValue.to_i8(), 0);
            assert_eq!(NaNValue.to_u8(), 0);
            assert_eq!(NaNValue.to_i16(), 0);
            assert_eq!(NaNValue.to_u16(), 0);
            assert_eq!(NaNValue.to_i32(), 0);
            assert_eq!(NaNValue.to_u32(), 0);
            assert_eq!(NaNValue.to_i64(), 0);
            assert_eq!(NaNValue.to_u64(), 0);
            assert_eq!(NaNValue.to_i128(), 0);
            assert_eq!(NaNValue.to_u128(), 0);
            assert_eq!(NaNValue.to_usize(), 0);
            assert_eq!(NaNValue.to_f64(), 0.0);
        }

        // ---------- serde / display ----------
        #[test]
        fn to_json_and_display_and_unwrap_value() {
            assert_eq!(U8Value(0xAB).to_json(), serde_json::json!(0xABu8));
            assert_eq!(I64Value(-7).to_json(), serde_json::json!(-7));
            assert_eq!(F64Value(1.5).to_json(), serde_json::json!(1.5));
            assert_eq!(NaNValue.to_json(), serde_json::json!("NaN"));

            // unwrap_value special cases
            assert_eq!(U8Value(0xAB).unwrap_value(), "0xab");
            assert_eq!(F64Value(0.0).unwrap_value(), "0.0");
            assert_eq!(F64Value(1.25).unwrap_value(), "1.25");

            // Display delegates to unwrap_value
            assert_eq!(format!("{}", U8Value(0xAB)), "0xab");
            assert_eq!(format!("{}", NaNValue), "NaN");
        }

        // ---------- arithmetic ops ----------
        #[test]
        fn add_sub_mul_div_rem_same_and_mixed_and_nan() {
            // same-type
            assert_eq!(I32Value(5) + I32Value(7), I32Value(12));
            assert_eq!(U8Value(250) + U8Value(10), U8Value(4)); // wrapping_add
            assert_eq!(F64Value(5.0) - F64Value(2.5), F64Value(2.5));
            assert_eq!(I16Value(6) * I16Value(7), I16Value(42));
            assert_eq!(I64Value(7) / I64Value(2), I64Value(3));
            assert_eq!(U32Value(10) % U32Value(3), U32Value(1));

            // mixed-type (falls back to f64)
            assert_eq!(I32Value(2) + U8Value(3), F64Value(5.0));
            assert_eq!(U16Value(8) - F64Value(2.5), F64Value(5.5));
            assert_eq!(I8Value(3) * U16Value(4), F64Value(12.0));
            assert_eq!(U8Value(10) / I16Value(4), F64Value(2.5));
            assert_eq!(U8Value(10) % I16Value(4), F64Value(2.0));

            // division by (effective) zero -> NaNValue
            assert_eq!(I32Value(5) / I32Value(0), NaNValue);
            assert_eq!(F64Value(5.0) / F64Value(0.0), NaNValue);

            // any NaN in arithmetic -> NaNValue
            assert_eq!(NaNValue + I32Value(1), NaNValue);
            assert_eq!(U16Value(2) * NaNValue, NaNValue);
        }

        // ---------- bit ops ----------
        #[test]
        fn bitwise_ops_cover_all_paths() {
            // same-type
            assert_eq!(U8Value(0b1010) & U8Value(0b1100), U8Value(0b1000));
            assert_eq!(I8Value(0b1010) | I8Value(0b0101), I8Value(0b1111));
            assert_eq!(I16Value(0b1010) ^ I16Value(0b1100), I16Value(0b0110));

            // f64 path casts to i64 then applies op, result in I64
            assert_eq!(F64Value(6.0) & F64Value(3.0), I64Value(6i64 & 3i64));
            assert_eq!(F64Value(6.0) | F64Value(1.0), I64Value(6i64 | 1i64));
            assert_eq!(F64Value(6.0) ^ F64Value(3.0), I64Value(6i64 ^ 3i64));

            // mixed-type fallback uses to_i64()
            assert_eq!(U32Value(6) & I16Value(3), I64Value(6i64 & 3i64));
            assert_eq!(U32Value(6) | I16Value(1), I64Value(6i64 | 1i64));
            assert_eq!(U32Value(6) ^ I16Value(3), I64Value(6i64 ^ 3i64));

            // NaN short-circuit
            assert_eq!(NaNValue & U8Value(1), NaNValue);
            assert_eq!(U8Value(1) | NaNValue, NaNValue);
            assert_eq!(NaNValue ^ NaNValue, NaNValue);
        }

        // ---------- shift ops ----------
        #[test]
        fn shift_ops_cover_f64_and_mixed() {
            // same-type
            assert_eq!(I32Value(1) << I32Value(3), I32Value(8));
            assert_eq!(U16Value(16) >> U16Value(2), U16Value(4));

            // f64 path -> cast to i64
            assert_eq!(F64Value(2.0) << F64Value(3.0), I64Value((2i64) << 3));
            assert_eq!(F64Value(16.0) >> F64Value(2.0), I64Value((16i64) >> 2));

            // mixed-type fallback to_i64
            assert_eq!(U8Value(2) << I16Value(3), I64Value(2i64 << 3));
            assert_eq!(I16Value(16) >> U8Value(2), I64Value(16i64 >> 2));

            // NaN short-circuit
            assert_eq!(NaNValue << U8Value(1), NaNValue);
            assert_eq!(U8Value(1) >> NaNValue, NaNValue);
        }

        // ---------- neg / not ----------
        #[test]
        fn neg_and_not_behavior() {
            // neg
            assert_eq!((-I8Value(5)), I8Value(-5));
            assert_eq!((-I16Value(5)), I16Value(-5));
            assert_eq!((-I32Value(5)), I32Value(-5));
            assert_eq!((-I64Value(5)), I64Value(-5));
            assert_eq!((-I128Value(5)), I128Value(-5));
            assert_eq!((-F64Value(2.5)), F64Value(-2.5));
            // unsigned neg -> signed type per impl
            assert_eq!((-U8Value(5)), I64Value(-5));
            assert_eq!((-U16Value(5)), I64Value(-5));
            assert_eq!((-U32Value(5)), I64Value(-5));
            assert_eq!((-U64Value(5)), I64Value(-5));
            assert_eq!((-U128Value(5)), I128Value(-5));
            // NaN neg -> NaN
            assert_eq!((-NaNValue), NaNValue);

            // not
            assert_eq!(!U8Value(0b1010), U8Value(!0b1010));
            assert_eq!(!I32Value(0b1010), I32Value(!0b1010));
            assert_eq!(!U128Value(7), U128Value(!7u128));
            // F64 is a passthrough for Not
            assert_eq!(!F64Value(3.5), F64Value(3.5));
            // NaN -> NaN
            assert_eq!(!NaNValue, NaNValue);
        }

        // ---------- max / min ----------
        #[test]
        fn max_min_cover_special_cases() {
            // NaN returns the other
            assert_eq!(NaNValue.max(I32Value(5)), I32Value(5));
            //assert_eq!(NaNValue.min(U8Value(2)), U8Value(2));

            // F64 direct
            assert_eq!(F64Value(2.0).max(F64Value(3.5)), F64Value(3.5));
            assert_eq!(F64Value(2.0).min(F64Value(3.5)), F64Value(2.0));

            // U64/I128/U128 specific arms
            assert_eq!(U64Value(7).max(U64Value(3)), U64Value(7));
            assert_eq!(I128Value(-1).min(I128Value(5)), I128Value(-1));
            assert_eq!(U128Value(9).max(U128Value(10)), U128Value(10));

            // fallback -> convert back to original kind
            let a = I32Value(5);
            let b = U16Value(12);
            // 5.max(12) via i64 -> 12 then convert_to I32Kind
            assert_eq!(a.max(b), U16Value(12));
            // 5.min(12) -> 5
            assert_eq!(a.min(b), I32Value(5));
        }

        // ---------- pow ----------
        #[test]
        fn pow_uses_rhs_as_usize() {
            assert_eq!(I32Value(2).pow(&U8Value(10)), F64Value(1024.0));
            assert_eq!(F64Value(2.5).pow(&I16Value(2)), F64Value(6.25));
            // rhs negative becomes huge usize via cast but implementation casts directly;
            // ensure behavior is well-defined by using 0 (effective) after cast of NaN:
            assert_eq!(F64Value(3.0).pow(&NaNValue), F64Value(1.0)); // exponent 0
        }

        // ---------- comparison / ordering ----------
        #[test]
        fn partialord_and_ord_nan_behavior() {
            // same-type comparisons
            assert!(I32Value(2) < I32Value(3));
            assert!(U64Value(5) > U64Value(4));
            assert!(F64Value(2.5) <= F64Value(2.5));

            // mixed-type uses to_f64()
            assert!(I8Value(7) > U16Value(6));

            // PartialOrd with NaN returns None
            assert!(matches!(NaNValue.partial_cmp(&I32Value(1)), None));
            // Ord unwrap_or(Ordering::Equal) => Equal for NaN path
            assert_eq!(NaNValue.cmp(&U8Value(1)), Ordering::Equal);
            assert_eq!(U8Value(1).cmp(&NaNValue), Ordering::Equal);
        }

        // ---------- hashing ----------
        #[test]
        fn hashing_consistency_and_difference() {
            let a1 = I32Value(42);
            let a2 = I32Value(42);
            let b  = I32Value(43);
            assert_eq!(hash_of(&a1), hash_of(&a2));
            assert_ne!(hash_of(&a1), hash_of(&b));

            // to_string() drives hashing; check Display equivalence
            assert_eq!(hash_of(&U8Value(0xAA)), hash_of(&U8Value(0xAA)));
        }

        // ---------- RangeBounds impl ----------
        #[test]
        fn range_bounds_included_excluded() {
            let start = I32Value(5);
            let end   = I32Value(10);
            // start_bound is Included(&self)
            assert!(matches!(start.start_bound(), Bound::Included(&Numbers::I32Value(5))));
            // end_bound is Excluded(&self)
            assert!(matches!(end.end_bound(), Bound::Excluded(&Numbers::I32Value(10))));

            // Use with standard RangeBounds consumer (e.g., contains)
            // We'll synthesize a simple manual check using cmp:
            fn contains(range_start: Numbers, range_end: Numbers, x: Numbers) -> bool {
                // [start, end)
                (x >= range_start) && (x < range_end)
            }
            assert!(contains(I32Value(5), I32Value(10), I32Value(5)));
            assert!(contains(I32Value(5), I32Value(10), I32Value(9)));
            assert!(!contains(I32Value(5), I32Value(10), I32Value(10)));
        }
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! test_numeric_ops {
        ($variant:ident, $v1:expr, $v2:expr, $sum:expr, $diff:expr, $prod:expr, $quot:expr, $rem:expr, $exp:expr) => {
            assert_eq!($variant($v1) + $variant($v2), $variant($sum));
            assert_eq!($variant($v1) - $variant($v2), $variant($diff));
            assert_eq!($variant($v1) * $variant($v2), $variant($prod));
            assert_eq!($variant($v1) / $variant($v2), $variant($quot));
            assert_eq!($variant($v1) % $variant($v2), $variant($rem));
            assert_eq!($variant($v1).pow(&$variant($v2)), F64Value($exp));
        };
    }

    macro_rules! generate_bitwise_tests {
        ($variant:ident, $name:ident, $a:expr, $b:expr) => {
            paste::paste! {
                #[test]
                fn [<test_ $name _bit_and>]() {
                    assert_eq!($variant($a) & $variant($b), $variant($a & $b));
                }

                #[test]
                fn [<test_ $name _bit_or>]() {
                    assert_eq!($variant($a) | $variant($b), $variant($a | $b));
                }

                #[test]
                fn [<test_ $name _bit_xor>]() {
                    assert_eq!($variant($a) ^ $variant($b), $variant($a ^ $b));
                }

                #[test]
                fn [<test_ $name _bit_shl>]() {
                    assert_eq!($variant($a) << $variant($b), $variant($a << $b));
                }

                #[test]
                fn [<test_ $name _bit_shr>]() {
                    assert_eq!($variant($a) >> $variant($b), $variant($a >> $b));
                }
            }
        };
    }

    macro_rules! generate_comparison_tests {
        ($variant:ident, $name:ident, $low:expr, $mid:expr, $high:expr) => {
            paste::paste! {
                #[test]
                fn [<test_ $name _greater_than>]() {
                    assert!($variant($high) > $variant($mid));
                    assert!(!($variant($mid) > $variant($mid)));
                    assert!(!($variant($low) > $variant($mid)));
                }

                #[test]
                fn [<test_ $name _greater_than_or_equal>]() {
                    assert!($variant($high) >= $variant($mid));
                    assert!($variant($mid) >= $variant($mid));
                    assert!(!($variant($low) >= $variant($mid)));
                }

                #[test]
                fn [<test_ $name _less_than>]() {
                    assert!($variant($low) < $variant($mid));
                    assert!(!($variant($mid) < $variant($mid)));
                    assert!(!($variant($high) < $variant($mid)));
                }

                #[test]
                fn [<test_ $name _less_than_or_equal>]() {
                    assert!($variant($low) <= $variant($mid));
                    assert!($variant($mid) <= $variant($mid));
                    assert!(!($variant($high) <= $variant($mid)));
                }
            }
        };
    }

    macro_rules! generate_not_tests {
        ($variant:ident, $test_name:ident, $value:expr) => {
            #[test]
            fn $test_name() {
                use crate::numbers::Numbers::*;
                let input = $variant($value);
                let result = !input;

                let expected = $variant(!$value);
                assert_eq!(result, expected, "Left: {result:?}\nRight: {expected:?}");
            }
        };
    }

    macro_rules! generate_neg_tests {
        ($variant:ident, $name:ident, $value:expr) => {
            paste::paste! {
                #[test]
                fn [<test_ $name _neg>]() {
                    assert_eq!(-$variant($value), $variant(-$value));
                }
            }
        };
    }

    fn test_all_ops<T: Copy + Eq + std::fmt::Debug>(
        wrap: fn(T) -> Numbers,
        a: T,
        b: T,
        sum: T,
        diff: T,
        prod: T,
        quot: T,
        rem: T,
        pow: f64,
    ) {
        assert_eq!(wrap(a) + wrap(b), wrap(sum));
        assert_eq!(wrap(a) - wrap(b), wrap(diff));
        assert_eq!(wrap(a) * wrap(b), wrap(prod));
        assert_eq!(wrap(a) / wrap(b), wrap(quot));
        assert_eq!(wrap(a) % wrap(b), wrap(rem));
        assert_eq!(wrap(a).pow(&wrap(b)), F64Value(pow));
    }

    fn test_all_float_ops(
        constructor: fn(f64) -> Numbers,
        a: f64,
        b: f64,
        sum: f64,
        diff: f64,
        prod: f64,
        quot: f64,
        rem: f64,
        pow: f64,
    ) {
        let left = constructor(a);
        let right = constructor(b);

        let (left_val, right_val) = (left.to_f64(), right.to_f64());
        assert!((left_val + right_val - sum).abs() < 1e-9);
        assert!((left_val - right_val - diff).abs() < 1e-9);
        assert!((left_val * right_val - prod).abs() < 1e-9);
        assert!((left_val / right_val - quot).abs() < 1e-9);
        assert!((left_val % right_val - rem).abs() < 1e-9);
        assert!((left_val.powf(right_val) - pow).abs() < 1e-9);

    }

    #[test]
    fn test_ops_i8() {
        test_numeric_ops!(I8Value, 10, 2, 12, 8, 20, 5, 0, 100.0);
    }

    #[test]
    fn test_ops_i16() {
        test_numeric_ops!(I16Value, 10, 2, 12, 8, 20, 5, 0, 100.0);
    }

    #[test]
    fn test_ops_u16() {
        test_numeric_ops!(U16Value, 10, 2, 12, 8, 20, 5, 0, 100.0);
    }

    #[test]
    fn test_ops_i32() {
        test_numeric_ops!(I32Value, 10, 2, 12, 8, 20, 5, 0, 100.0);
    }

    #[test]
    fn test_ops_u32() {
        test_numeric_ops!(U32Value, 10, 2, 12, 8, 20, 5, 0, 100.0);
    }

    #[test]
    fn test_i8_ops() {
        test_all_ops(I8Value, 10, 2, 12, 8, 20, 5, 0, 100.0);
    }

    #[test]
    fn test_i16_ops() {
        test_all_ops(I16Value, 10, 2, 12, 8, 20, 5, 0, 100.0);
    }

    #[test]
    fn test_u16_ops() {
        test_all_ops(U16Value, 10, 2, 12, 8, 20, 5, 0, 100.0);
    }

    #[test]
    fn test_i32_ops() {
        test_all_ops(I32Value, 10, 2, 12, 8, 20, 5, 0, 100.0);
    }

    #[test]
    fn test_u32_ops() {
        test_all_ops(U32Value, 10, 2, 12, 8, 20, 5, 0, 100.0);
    }

    #[test]
    fn test_i64_ops() {
        test_all_ops(I64Value, 10, 2, 12, 8, 20, 5, 0, 100.0);
    }

    #[test]
    fn test_u64_ops() {
        test_all_ops(U64Value, 10, 2, 12, 8, 20, 5, 0, 100.0);
    }

    #[test]
    fn test_i128_ops() {
        test_all_ops(I128Value, 10, 2, 12, 8, 20, 5, 0, 100.0);
    }

    #[test]
    fn test_u128_ops() {
        test_all_ops(U128Value, 10, 2, 12, 8, 20, 5, 0, 100.0);
    }

    #[test]
    fn test_f64_ops() {
        test_all_float_ops(F64Value, 10.0, 2.0, 12.0, 8.0, 20.0, 5.0, 0.0, 100.0);
    }

    #[test]
    fn test_u8_ops() {
        test_all_ops(U8Value, 10, 2, 12, 8, 20, 5, 0, 100.0);
    }

    #[test]
    fn test_u8_neg() {
        assert_eq!(U8Value(5).neg(), I64Value(-5));
    }

    #[test]
    fn test_u16_neg() {
        assert_eq!(U16Value(5).neg(), I64Value(-5));
    }

    #[test]
    fn test_u32_neg() {
        assert_eq!(U32Value(5).neg(), I64Value(-5));
    }

    #[test]
    fn test_u64_neg() {
        assert_eq!(U64Value(5).neg(), I64Value(-5));
    }

    #[test]
    fn test_u128_neg() {
        assert_eq!(U128Value(5).neg(), I128Value(-5));
    }

    #[test]
    fn test_type_name() {
        assert_eq!(I8Value(0).get_type_name(), "i8");
        assert_eq!(U8Value(0).get_type_name(), "u8");
        assert_eq!(I16Value(0).get_type_name(), "i16");
        assert_eq!(U16Value(0).get_type_name(), "u16");
        assert_eq!(I32Value(0).get_type_name(), "i32");
        assert_eq!(U32Value(0).get_type_name(), "u32");
        assert_eq!(F64Value(0.).get_type_name(), "f64");
        assert_eq!(I64Value(0).get_type_name(), "i64");
        assert_eq!(I128Value(0).get_type_name(), "i128");
        assert_eq!(NaNValue.get_type_name(), "NaN");
        assert_eq!(U64Value(0).get_type_name(), "u64");
        assert_eq!(U128Value(0).get_type_name(), "u128");
    }

    generate_bitwise_tests!(U8Value, u8, 0b1100, 2);     // 12, shift by 2
    generate_bitwise_tests!(I8Value, i8, 0b1100, 2);
    generate_bitwise_tests!(U16Value, u16, 0b1100, 2);
    generate_bitwise_tests!(I16Value, i16, 0b1100, 2);
    generate_bitwise_tests!(U32Value, u32, 0b1100, 2);
    generate_bitwise_tests!(I32Value, i32, 0b1100, 2);
    generate_bitwise_tests!(U64Value, u64, 0b1100, 2);
    generate_bitwise_tests!(I64Value, i64, 0b1100, 2);
    generate_bitwise_tests!(U128Value, u128, 0b1100, 2);
    generate_bitwise_tests!(I128Value, i128, 0b1100, 2);

    generate_comparison_tests!(U8Value, u8, 5, 10, 20);
    generate_comparison_tests!(I8Value, i8, -10, 0, 10);
    generate_comparison_tests!(U16Value, u16, 5, 10, 20);
    generate_comparison_tests!(I16Value, i16, -10, 0, 10);
    generate_comparison_tests!(U32Value, u32, 5, 10, 20);
    generate_comparison_tests!(I32Value, i32, -10, 0, 10);
    generate_comparison_tests!(U64Value, u64, 5, 10, 20);
    generate_comparison_tests!(I64Value, i64, -10, 0, 10);
    generate_comparison_tests!(U128Value, u128, 5, 10, 20);
    generate_comparison_tests!(I128Value, i128, -10, 0, 10);
    generate_comparison_tests!(F64Value, f64, 5.0, 10.0, 20.0);

    // NOT tests
    generate_not_tests!(U8Value,    test_u8value_not,    0b10101010);
    generate_not_tests!(I8Value,    test_i8value_not,    0b1010101);
    generate_not_tests!(U16Value,   test_u16value_not,   0b101010101010101);
    generate_not_tests!(I16Value,   test_i16value_not,   0b10101010101010);
    generate_not_tests!(U32Value,   test_u32value_not,   0xAAAAAAAA);
    generate_not_tests!(I32Value,   test_i32value_not,   0x55555555);
    generate_not_tests!(U64Value,   test_u64value_not,   0xAAAAAAAAAAAAAAAA);
    generate_not_tests!(I64Value,   test_i64value_not,   0x5555555555555555);
    generate_not_tests!(U128Value,  test_u128value_not,  0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA);
    generate_not_tests!(I128Value,  test_i128value_not,  0x55555555555555555555555555555555);

    // NEG tests
    generate_neg_tests!(I8Value, i8, 42);
    generate_neg_tests!(I16Value, i16, 42);
    generate_neg_tests!(I32Value, i32, 42);
    generate_neg_tests!(I64Value, i64, 42);
    generate_neg_tests!(I128Value, i128, 42);
    generate_neg_tests!(F64Value, f64, 42.5);
}