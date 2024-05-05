////////////////////////////////////////////////////////////////////
// typed values module
////////////////////////////////////////////////////////////////////

use std::{i32, io};
use std::cmp::Ordering;
use std::collections::Bound;
use std::fmt::Display;
use std::ops::*;

use chrono::DateTime;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use shared_lib::RowJs;

use crate::cnv_error;
use crate::codec;
use crate::data_types::DataType;
use crate::data_types::DataType::*;
use crate::model_row_collection::ModelRowCollection;
use crate::row_collection::RowCollection;
use crate::rows::Row;
use crate::typed_values::TypedValue::*;

const ISO_DATE_FORMAT: &str =
    r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?(([+-]\d\d:\d\d)|Z)?$";
const DECIMAL_FORMAT: &str = r"^-?(?:\d+(?:_\d)*|\d+)(?:\.\d+)?$";
const INTEGER_FORMAT: &str = r"^-?(?:\d+(?:_\d)*)?$";
const UUID_FORMAT: &str =
    "^[0-9a-fA-F]{8}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{12}$";

/// Basic value unit
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum TypedValue {
    Undefined,
    Null,
    BLOB(Vec<u8>),
    Boolean(bool),
    CLOB(Vec<char>),
    DateValue(i64),
    Float32Value(f32),
    Float64Value(f64),
    Int8Value(u8),
    Int16Value(i16),
    Int32Value(i32),
    Int64Value(i64),
    RecordNumber(usize),
    StringValue(String),
    UUIDValue([u8; 16]),
    // complex types
    Array(Vec<TypedValue>),
    JSONValue(Vec<(String, TypedValue)>),
    StructureValue(RowJs),
    TableRef(String),
    TableValue(ModelRowCollection),
    TupleValue(Vec<TypedValue>),
}

impl TypedValue {
    /// returns true, if:
    /// 1. the host value is an array, and the item value is found within it,
    /// 2. the host value is a table, and the item value matches a row found within it,
    pub fn contains(&self, value: &TypedValue) -> bool {
        match &self {
            Array(items) => items.contains(value),
            JSONValue(_items) => todo!(),
            TableValue(mrc) =>
                match value {
                    JSONValue(tuples) =>
                        mrc.contains(&Row::from_tuples(0, mrc.get_columns(), tuples)),
                    _ => false
                }
            _ => false
        }
    }

    /// decodes the typed value based on the supplied data type and buffer
    pub fn decode(data_type: &DataType, buffer: &Vec<u8>, offset: usize) -> Self {
        match data_type {
            BLOBType(_size) => BLOB(vec![]),
            BooleanType => codec::decode_u8(buffer, offset, |b| Boolean(b == 1)),
            CLOBType(_size) => CLOB(vec![]),
            DateType => codec::decode_u8x8(buffer, offset, |b| DateValue(i64::from_be_bytes(b))),
            EnumType(labels) => {
                let index = codec::decode_u8x4(buffer, offset, |b| i32::from_be_bytes(b));
                StringValue(labels[index as usize].to_string())
            }
            Int8Type => codec::decode_u8(buffer, offset, |b| Int8Value(b)),
            Int16Type => codec::decode_u8x2(buffer, offset, |b| Int16Value(i16::from_be_bytes(b))),
            Int32Type => codec::decode_u8x4(buffer, offset, |b| Int32Value(i32::from_be_bytes(b))),
            Int64Type => codec::decode_u8x8(buffer, offset, |b| Int64Value(i64::from_be_bytes(b))),
            Float32Type => codec::decode_u8x4(buffer, offset, |b| Float32Value(f32::from_be_bytes(b))),
            Float64Type => codec::decode_u8x8(buffer, offset, |b| Float64Value(f64::from_be_bytes(b))),
            RecordNumberType => RecordNumber(codec::decode_row_id(&buffer, 1)),
            StringType(size) => StringValue(codec::decode_string(buffer, offset, *size).to_string()),
            StructureType(_columns) => todo!(),
            TableType(_columns) => todo!(),
            UUIDType => codec::decode_u8x16(buffer, offset, |b| UUIDValue(b))
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        Self::encode_value(self)
    }

    pub fn encode_value(value: &TypedValue) -> Vec<u8> {
        match value {
            Array(items) => {
                let mut bytes = vec![];
                bytes.extend(items.len().to_be_bytes());
                for item in items { bytes.extend(item.encode()); }
                bytes
            }
            BLOB(bytes) => codec::encode_u8x_n(bytes.to_vec()),
            Boolean(v) => [if *v { 1 } else { 0 }].to_vec(),
            CLOB(chars) => codec::encode_chars(chars.to_vec()),
            DateValue(number) => number.to_be_bytes().to_vec(),
            JSONValue(pairs) => {
                let mut bytes = vec![];
                for (name, value) in pairs {
                    bytes.extend(name.bytes().collect::<Vec<u8>>());
                    bytes.extend(Self::encode_value(value))
                }
                bytes
            }
            Float32Value(number) => number.to_be_bytes().to_vec(),
            Float64Value(number) => number.to_be_bytes().to_vec(),
            Int8Value(number) => number.to_be_bytes().to_vec(),
            Int16Value(number) => number.to_be_bytes().to_vec(),
            Int32Value(number) => number.to_be_bytes().to_vec(),
            Int64Value(number) => number.to_be_bytes().to_vec(),
            TableValue(rc) => rc.encode(),
            Null | Undefined => [0u8; 0].to_vec(),
            RecordNumber(id) => id.to_be_bytes().to_vec(),
            StringValue(string) => codec::encode_string(string),
            StructureValue(_row) => todo!(),
            TableRef(path) => codec::encode_string(path),
            TupleValue(items) => {
                let mut bytes = vec![];
                bytes.extend(items.len().to_be_bytes());
                for item in items { bytes.extend(item.encode()); }
                bytes
            }
            UUIDValue(guid) => guid.to_vec(),
        }
    }

    fn intercept_unknowns(a: &TypedValue, b: &TypedValue) -> Option<TypedValue> {
        match (a, b) {
            (Undefined, _) => Some(Undefined),
            (_, Undefined) => Some(Undefined),
            (Null, _) => Some(Null),
            (_, Null) => Some(Null),
            _ => None
        }
    }

    fn millis_to_iso_date(millis: i64) -> Option<String> {
        let seconds = millis / 1000;
        let nanoseconds = (millis % 1000) * 1_000_000;
        let datetime = DateTime::from_timestamp(seconds, nanoseconds as u32)?;
        let iso_date = datetime.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
        Some(iso_date)
    }

    pub fn from_json(j_value: Value) -> Self {
        match j_value {
            Value::Null => Null,
            Value::Bool(b) => Boolean(b),
            Value::Number(n) => n.as_f64().map(Float64Value).unwrap_or(Null),
            Value::String(s) => StringValue(s),
            Value::Array(a) => Array(a.iter().map(|v| Self::from_json(v.clone())).collect()),
            Value::Object(_) => todo!()
        }
    }

    pub fn from_numeric(text: &str) -> io::Result<TypedValue> {
        let decimal_regex = Regex::new(DECIMAL_FORMAT).map_err(|e| cnv_error!(e))?;
        let int_regex = Regex::new(INTEGER_FORMAT).map_err(|e| cnv_error!(e))?;
        let number: String = text.chars()
            .filter(|c| *c != '_' && *c != ',')
            .collect();
        match number.trim() {
            s if int_regex.is_match(s) => Ok(Int64Value(s.parse().map_err(|e| cnv_error!(e))?)),
            s if decimal_regex.is_match(s) => Ok(Float64Value(s.parse().map_err(|e| cnv_error!(e))?)),
            s => Ok(StringValue(s.to_string()))
        }
    }

    pub fn ordinal(&self) -> u8 {
        match *self {
            Undefined => 0,
            Null => 1,
            Array(_) => 2,
            BLOB(_) => 3,
            Boolean(_) => 4,
            CLOB(_) => 5,
            DateValue(_) => 6,
            JSONValue(_) => 7,
            Float32Value(_) => 8,
            Float64Value(_) => 9,
            Int8Value(_) => 10,
            Int16Value(_) => 11,
            Int32Value(_) => 12,
            Int64Value(_) => 13,
            TableValue(_) => 14,
            RecordNumber(_) => 15,
            StringValue(_) => 16,
            StructureValue(_) => 17,
            TableRef(_) => 18,
            TupleValue(_) => 19,
            UUIDValue(_) => 20,
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Array(items) =>
                serde_json::json!(items.iter().map(|v|v.to_json()).collect::<Vec<Value>>()),
            BLOB(bytes) => serde_json::json!(bytes),
            Boolean(b) => serde_json::json!(b),
            CLOB(chars) => serde_json::json!(chars),
            DateValue(millis) => serde_json::json!(Self::millis_to_iso_date(*millis)),
            JSONValue(pairs) => serde_json::json!(pairs),
            Float32Value(number) => serde_json::json!(number),
            Float64Value(number) => serde_json::json!(number),
            Int8Value(number) => serde_json::json!(number),
            Int16Value(number) => serde_json::json!(number),
            Int32Value(number) => serde_json::json!(number),
            Int64Value(number) => serde_json::json!(number),
            TableValue(mrc) => {
                let rows = mrc.get_rows().iter()
                    .map(|r| r.to_row_js())
                    .collect::<Vec<RowJs>>();
                serde_json::json!(rows)
            }
            Null => serde_json::Value::Null,
            RecordNumber(number) => serde_json::json!(number),
            StringValue(string) => serde_json::json!(string),
            StructureValue(row) => serde_json::json!(row),
            TableRef(path) => serde_json::json!(path),
            TupleValue(items) =>
                serde_json::json!(items.iter().map(|v|v.to_json()).collect::<Vec<Value>>()),
            Undefined => serde_json::Value::Null,
            UUIDValue(guid) => serde_json::json!(Uuid::from_bytes(*guid).to_string()),
        }
    }

    pub fn unwrap_value(&self) -> String {
        match self {
            Array(items) => {
                let values: Vec<String> = items.iter().map(|v| v.unwrap_value()).collect();
                format!("[{}]", values.join(", "))
            }
            BLOB(bytes) => hex::encode(bytes),
            Boolean(b) => (if *b { "true" } else { "false" }).into(),
            CLOB(chars) => chars.into_iter().collect(),
            DateValue(millis) => Self::millis_to_iso_date(*millis).unwrap_or("".into()),
            JSONValue(pairs) => {
                let values: Vec<String> = pairs.iter()
                    .map(|(k, v)| format!("{}:{}", k, v.unwrap_value()))
                    .collect();
                format!("{{{}}}", values.join(", "))
            }
            Float32Value(number) => number.to_string(),
            Float64Value(number) => number.to_string(),
            Int8Value(number) => number.to_string(),
            Int16Value(number) => number.to_string(),
            Int32Value(number) => number.to_string(),
            Int64Value(number) => number.to_string(),
            TableValue(mrc) => serde_json::json!(mrc.get_rows()).to_string(),
            Null => "null".into(),
            RecordNumber(number) => number.to_string(),
            StringValue(string) => string.into(),
            StructureValue(row) => serde_json::json!(row).to_string(),
            TableRef(path) => path.into(),
            TupleValue(items) => {
                let values: Vec<String> = items.iter().map(|v| v.unwrap_value()).collect();
                format!("({})", values.join(", "))
            }
            Undefined => "undefined".into(),
            UUIDValue(guid) => Uuid::from_bytes(*guid).to_string(),
        }
    }

    pub fn is_numeric(value: &str) -> io::Result<bool> {
        let decimal_regex = Regex::new(DECIMAL_FORMAT)
            .map_err(|e| cnv_error!(e))?;
        Ok(decimal_regex.is_match(value))
    }

    pub fn wrap_value(raw_value: &str) -> io::Result<Self> {
        let iso_date_regex = Regex::new(ISO_DATE_FORMAT).map_err(|e| cnv_error!(e))?;
        let uuid_regex = Regex::new(UUID_FORMAT).map_err(|e| cnv_error!(e))?;
        let result = match raw_value {
            s if s == "false" => Boolean(false),
            s if s == "null" => Null,
            s if s == "true" => Boolean(true),
            s if s == "undefined" => Undefined,
            s if s.is_empty() => Null,
            s if Self::is_numeric(s)? => Self::from_numeric(s)?,
            s if iso_date_regex.is_match(s) =>
                DateValue(DateTime::parse_from_rfc3339(s)
                    .map_err(|e| cnv_error!(e))?.timestamp_millis()),
            s if uuid_regex.is_match(s) => UUIDValue(codec::decode_uuid(s)?),
            s => StringValue(s.to_string()),
        };
        Ok(result)
    }

    pub fn wrap_value_opt(opt_value: &Option<String>) -> io::Result<Self> {
        match opt_value {
            Some(value) => Self::wrap_value(value),
            None => Ok(Null)
        }
    }

    ///////////////////////////////////////////////////////////////
    //      CONDITIONAL OPERATIONS
    ///////////////////////////////////////////////////////////////

    pub fn and(&self, rhs: &TypedValue) -> Option<TypedValue> {
        Some(Boolean(self.assume_bool()? && rhs.assume_bool()?))
    }

    pub fn factorial(&self) -> Option<TypedValue> {
        fn fact_f64(n: f64) -> TypedValue {
            fn fact_f(n: f64) -> f64 { if n <= 1. { 1. } else { n * fact_f(n - 1.) } }
            Float64Value(fact_f(n))
        }
        let num = self.assume_f64()?;
        Some(fact_f64(num))
    }

    pub fn ne(&self, rhs: &Self) -> Option<Self> {
        Some(Boolean(self.assume_f64()? != rhs.assume_f64()?))
    }

    pub fn not(&self) -> Option<Self> {
        Some(Boolean(!self.assume_bool()?))
    }

    pub fn or(&self, rhs: &Self) -> Option<Self> {
        Some(Boolean(self.assume_bool()? || rhs.assume_bool()?))
    }

    pub fn pow(&self, rhs: &Self) -> Option<Self> {
        Self::numeric_op_2f(self, rhs, |a, b| num_traits::pow(a, b as usize))
    }

    pub fn range(&self, rhs: &Self) -> Option<Self> {
        let mut values = vec![];
        for n in self.assume_i64()?..rhs.assume_i64()? { values.push(Int64Value(n)) }
        Some(Array(values))
    }

    ///////////////////////////////////////////////////////////////
    //      UTILITY METHODS
    ///////////////////////////////////////////////////////////////

    fn assume_bool(&self) -> Option<bool> {
        match &self {
            Boolean(b) => Some(*b),
            _ => None
        }
    }

    fn assume_f64(&self) -> Option<f64> {
        match &self {
            Float32Value(n) => Some(*n as f64),
            Float64Value(n) => Some(*n),
            Int8Value(n) => Some(*n as f64),
            Int16Value(n) => Some(*n as f64),
            Int32Value(n) => Some(*n as f64),
            Int64Value(n) => Some(*n as f64),
            RecordNumber(n) => Some(*n as f64),
            _ => None
        }
    }

    fn assume_i64(&self) -> Option<i64> {
        match &self {
            Float32Value(n) => Some(*n as i64),
            Float64Value(n) => Some(*n as i64),
            Int8Value(n) => Some(*n as i64),
            Int16Value(n) => Some(*n as i64),
            Int32Value(n) => Some(*n as i64),
            Int64Value(n) => Some(*n),
            RecordNumber(n) => Some(*n as i64),
            _ => None
        }
    }

    pub fn assume_usize(&self) -> Option<usize> {
        match &self {
            Float32Value(n) => Some(*n as usize),
            Float64Value(n) => Some(*n as usize),
            Int8Value(n) => Some(*n as usize),
            Int16Value(n) => Some(*n as usize),
            Int32Value(n) => Some(*n as usize),
            Int64Value(n) => Some(*n as usize),
            RecordNumber(n) => Some(*n),
            _ => None
        }
    }

    fn numeric_op_1f(lhs: &Self, ff: fn(f64) -> f64) -> Option<Self> {
        match lhs {
            Float64Value(a) => Some(Float64Value(ff(*a))),
            Float32Value(a) => Some(Float32Value(ff(*a as f64) as f32)),
            Int64Value(a) => Some(Int64Value(ff(*a as f64) as i64)),
            Int32Value(a) => Some(Int32Value(ff(*a as f64) as i32)),
            Int16Value(a) => Some(Int16Value(ff(*a as f64) as i16)),
            Int8Value(a) => Some(Int8Value(ff(*a as f64) as u8)),
            RecordNumber(a) => Some(RecordNumber(ff(*a as f64) as usize)),
            _ => Some(Float64Value(lhs.assume_f64()?))
        }
    }

    fn numeric_op_2f(lhs: &Self, rhs: &Self, ff: fn(f64, f64) -> f64) -> Option<Self> {
        match Self::intercept_unknowns(lhs, rhs) {
            Some(value) => Some(value),
            None => {
                match (lhs, rhs) {
                    (Float64Value(a), Float64Value(b)) => Some(Float64Value(ff(*a, *b))),
                    (Float32Value(a), Float32Value(b)) => Some(Float32Value(ff(*a as f64, *b as f64) as f32)),
                    (Int64Value(a), Int64Value(b)) => Some(Int64Value(ff(*a as f64, *b as f64) as i64)),
                    (Int32Value(a), Int32Value(b)) => Some(Int32Value(ff(*a as f64, *b as f64) as i32)),
                    (Int16Value(a), Int16Value(b)) => Some(Int16Value(ff(*a as f64, *b as f64) as i16)),
                    (Int8Value(a), Int8Value(b)) => Some(Int8Value(ff(*a as f64, *b as f64) as u8)),
                    (RecordNumber(a), RecordNumber(b)) => Some(RecordNumber(ff(*a as f64, *b as f64) as usize)),
                    _ => Some(Float64Value(ff(lhs.assume_f64()?, rhs.assume_f64()?)))
                }
            }
        }
    }

    fn numeric_op_2i(lhs: &Self, rhs: &Self, ff: fn(i64, i64) -> i64) -> Option<Self> {
        match Self::intercept_unknowns(lhs, rhs) {
            Some(value) => Some(value),
            None => {
                match (lhs, rhs) {
                    (Float64Value(a), Float64Value(b)) => Some(Float64Value(ff(*a as i64, *b as i64) as f64)),
                    (Float32Value(a), Float32Value(b)) => Some(Float32Value(ff(*a as i64, *b as i64) as f32)),
                    (Int64Value(a), Int64Value(b)) => Some(Int64Value(ff(*a as i64, *b as i64) as i64)),
                    (Int32Value(a), Int32Value(b)) => Some(Int32Value(ff(*a as i64, *b as i64) as i32)),
                    (Int16Value(a), Int16Value(b)) => Some(Int16Value(ff(*a as i64, *b as i64) as i16)),
                    (Int8Value(a), Int8Value(b)) => Some(Int8Value(ff(*a as i64, *b as i64) as u8)),
                    (RecordNumber(a), RecordNumber(b)) => Some(RecordNumber(ff(*a as i64, *b as i64) as usize)),
                    _ => Some(Int64Value(ff(lhs.assume_i64()?, rhs.assume_i64()?)))
                }
            }
        }
    }
}

impl Add for TypedValue {
    type Output = TypedValue;

    fn add(self, rhs: Self) -> Self::Output {
        match (&self, &rhs) {
            (Boolean(a), Boolean(b)) => Boolean(a | b),
            (StringValue(a), StringValue(b)) => StringValue(a.to_string() + b),
            _ => Self::numeric_op_2f(&self, &rhs, |a, b| a + b).unwrap_or(Undefined)
        }
    }
}

impl BitAnd for TypedValue {
    type Output = TypedValue;

    fn bitand(self, rhs: Self) -> Self::Output {
        Self::numeric_op_2i(&self, &rhs, |a, b| a & b).unwrap_or(Undefined)
    }
}

impl BitOr for TypedValue {
    type Output = TypedValue;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self::numeric_op_2i(&self, &rhs, |a, b| a | b).unwrap_or(Undefined)
    }
}

impl BitXor for TypedValue {
    type Output = TypedValue;

    fn bitxor(self, rhs: Self) -> Self::Output {
        Self::numeric_op_2i(&self, &rhs, |a, b| a ^ b).unwrap_or(Undefined)
    }
}

impl Display for TypedValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.unwrap_value())
    }
}

impl Div for TypedValue {
    type Output = TypedValue;

    fn div(self, rhs: Self) -> Self::Output {
        Self::numeric_op_2f(&self, &rhs, |a, b| a / b).unwrap_or(Undefined)
    }
}

impl Index<usize> for TypedValue {
    type Output = TypedValue;

    fn index(&self, _index: usize) -> &Self::Output {
        match self {
            Array(items) => &items[_index],
            //MemoryTable(brc) => &StructValue(brc[_index]),
            Null => &Null,
            Undefined => &Undefined,
            x => panic!("illegal value for index {}", x)
        }
    }
}

impl Mul for TypedValue {
    type Output = TypedValue;

    fn mul(self, rhs: Self) -> Self::Output {
        Self::numeric_op_2f(&self, &rhs, |a, b| a * b).unwrap_or(Undefined)
    }
}

impl Neg for TypedValue {
    type Output = TypedValue;

    fn neg(self) -> Self::Output {
        Self::numeric_op_1f(&self, |a| -a).unwrap_or(Undefined)
    }
}

impl Not for TypedValue {
    type Output = TypedValue;

    fn not(self) -> Self::Output {
        match self.assume_bool() {
            Some(v) => Boolean(!v),
            None => Undefined
        }
    }
}

impl RangeBounds<TypedValue> for TypedValue {
    fn start_bound(&self) -> Bound<&TypedValue> {
        std::ops::Bound::Included(&self)
    }

    fn end_bound(&self) -> Bound<&TypedValue> {
        std::ops::Bound::Excluded(&self)
    }
}

impl Rem for TypedValue {
    type Output = TypedValue;

    fn rem(self, rhs: Self) -> Self::Output {
        Self::numeric_op_2i(&self, &rhs, |a, b| a % b).unwrap_or(Undefined)
    }
}

impl PartialOrd for TypedValue {
    fn partial_cmp(&self, rhs: &Self) -> Option<Ordering> {
        match (&self, &rhs) {
            (Array(a), Array(b)) => a.partial_cmp(b),
            (BLOB(a), BLOB(b)) => a.partial_cmp(b),
            (Boolean(a), Boolean(b)) => a.partial_cmp(b),
            (CLOB(a), CLOB(b)) => a.partial_cmp(b),
            (DateValue(a), DateValue(b)) => a.partial_cmp(b),
            (Float32Value(a), Float32Value(b)) => a.partial_cmp(b),
            (Float64Value(a), Float64Value(b)) => a.partial_cmp(b),
            (Int8Value(a), Int8Value(b)) => a.partial_cmp(b),
            (Int16Value(a), Int16Value(b)) => a.partial_cmp(b),
            (Int32Value(a), Int32Value(b)) => a.partial_cmp(b),
            (Int64Value(a), Int64Value(b)) => a.partial_cmp(b),
            (RecordNumber(a), RecordNumber(b)) => a.partial_cmp(b),
            (StringValue(a), StringValue(b)) => a.partial_cmp(b),
            (UUIDValue(a), UUIDValue(b)) => a.partial_cmp(b),
            (Null, Null) => Some(Ordering::Equal),
            (Null, Undefined) => Some(Ordering::Greater),
            (Undefined, Null) => Some(Ordering::Less),
            (Undefined, Undefined) => Some(Ordering::Equal),
            _ => None
        }
    }
}

impl Shl for TypedValue {
    type Output = TypedValue;

    fn shl(self, rhs: Self) -> Self::Output {
        Self::numeric_op_2i(&self, &rhs, |a, b| a << b).unwrap_or(Undefined)
    }
}

impl Shr for TypedValue {
    type Output = TypedValue;

    fn shr(self, rhs: Self) -> Self::Output {
        Self::numeric_op_2i(&self, &rhs, |a, b| a >> b).unwrap_or(Undefined)
    }
}

impl Sub for TypedValue {
    type Output = TypedValue;

    fn sub(self, rhs: Self) -> Self::Output {
        match (&self, &rhs) {
            (Boolean(a), Boolean(b)) => Boolean(a & b),
            _ => Self::numeric_op_2f(&self, &rhs, |a, b| a - b).unwrap_or(Undefined)
        }
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add() {
        assert_eq!(Float64Value(45.0) + Float64Value(32.7), Float64Value(77.7));
        assert_eq!(Float32Value(45.7) + Float32Value(32.0), Float32Value(77.7));
        assert_eq!(Int64Value(45) + Int64Value(32), Int64Value(77));
        assert_eq!(Int32Value(45) + Int32Value(32), Int32Value(77));
        assert_eq!(Int16Value(45) + Int16Value(32), Int16Value(77));
        assert_eq!(Int8Value(45) + Int8Value(32), Int8Value(77));
        assert_eq!(Boolean(true) + Boolean(true), Boolean(true));
        assert_eq!(StringValue("Hello".into()) + StringValue(" World".into()), StringValue("Hello World".into()));
    }

    #[test]
    fn test_eq() {
        assert_eq!(Int8Value(0xCE), Int8Value(0xCE));
        assert_eq!(Int16Value(0x7ACE), Int16Value(0x7ACE));
        assert_eq!(Int32Value(0x1111_BEEF), Int32Value(0x1111_BEEF));
        assert_eq!(Int64Value(0x5555_FACE_CAFE_BABE), Int64Value(0x5555_FACE_CAFE_BABE));
        assert_eq!(Float32Value(45.0), Float32Value(45.0));
        assert_eq!(Float64Value(45.0), Float64Value(45.0));
    }

    #[test]
    fn test_ne() {
        assert_ne!(Int8Value(0xCE), Int8Value(0x00));
        assert_ne!(Int16Value(0x7ACE), Int16Value(0xACE));
        assert_ne!(Int32Value(0x1111_BEEF), Int32Value(0xBEEF));
        assert_ne!(Int64Value(0x5555_FACE_CAFE_BABE), Int64Value(0xFACE_CAFE_BABE));
        assert_ne!(Float32Value(45.0), Float32Value(45.7));
        assert_ne!(Float64Value(99.142857), Float64Value(19.48));
    }

    #[test]
    fn test_gt() {
        assert!(Array(vec![Int8Value(0xCE), Int8Value(0xCE)]) > Array(vec![Int8Value(0x23), Int8Value(0xBE)]));
        assert!(Int8Value(0xCE) > Int8Value(0xAA));
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
        assert_eq!(a.pow(&b), Some(Int64Value(25)))
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
        assert_eq!(Int8Value(45) - Int8Value(32), Int8Value(13));
    }

    #[test]
    fn test_decode() {
        let buf: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 5, b'H', b'e', b'l', b'l', b'o'];
        assert_eq!(TypedValue::decode(&StringType(5), &buf, 0), StringValue("Hello".into()))
    }

    #[test]
    fn test_encode() {
        let expected: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 5, b'H', b'e', b'l', b'l', b'o'];
        assert_eq!(TypedValue::encode_value(&StringValue("Hello".into())), expected);
    }

    #[test]
    fn test_to_json() {
        verify_to_json(Boolean(false), serde_json::json!(false));
        verify_to_json(Boolean(true), serde_json::json!(true));
        verify_to_json(DateValue(1709163679081), serde_json::Value::String("2024-02-28T23:41:19.081Z".into()));
        verify_to_json(Float64Value(100.1), serde_json::json!(100.1));
        verify_to_json(Int64Value(100), serde_json::json!(100));
        verify_to_json(Null, serde_json::Value::Null);
        verify_to_json(StringValue("Hello World".into()), serde_json::Value::String("Hello World".into()));
        verify_to_json(UUIDValue([
            0x67, 0x45, 0x23, 0x01, 0xAB, 0xCD, 0xEF, 0x89,
            0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF,
        ]), serde_json::Value::String("67452301-abcd-ef89-1234-567890abcdef".into()));
    }

    #[test]
    fn test_wrap_and_unwrap_value() {
        verify_wrap_unwrap("false", Boolean(false));
        verify_wrap_unwrap("true", Boolean(true));
        verify_wrap_unwrap("2024-02-28T23:41:19.081Z", DateValue(1709163679081));
        verify_wrap_unwrap("100.1", Float64Value(100.1));
        verify_wrap_unwrap("100", Int64Value(100));
        verify_wrap_unwrap("null", Null);
        verify_wrap_unwrap("Hello World", StringValue("Hello World".into()));
        verify_wrap_unwrap("67452301-abcd-ef89-1234-567890abcdef", UUIDValue([
            0x67, 0x45, 0x23, 0x01, 0xAB, 0xCD, 0xEF, 0x89,
            0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF,
        ]))
    }

    #[test]
    fn test_unwrap_optional_value() {
        assert_eq!(TypedValue::wrap_value_opt(&Some("123.45".into())).unwrap(), Float64Value(123.45))
    }

    fn verify_to_json(typed_value: TypedValue, expected: serde_json::Value) {
        assert_eq!(typed_value.to_json(), expected)
    }

    fn verify_wrap_unwrap(string_value: &str, typed_value: TypedValue) {
        assert_eq!(typed_value.unwrap_value(), string_value);
        match TypedValue::wrap_value(string_value) {
            Ok(value) => assert_eq!(value, typed_value),
            Err(message) => panic!("{}", message)
        };
    }
}