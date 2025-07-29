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
        match self {
            Numbers::U8Value(id) => id.hash(state),
            Numbers::I8Value(id) => id.hash(state),
            Numbers::I16Value(id) => id.hash(state),
            Numbers::U16Value(id) => id.hash(state),
            Numbers::I32Value(id) => id.hash(state),
            Numbers::U32Value(id) => id.hash(state),
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