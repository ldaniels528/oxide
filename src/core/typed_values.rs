#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// TypedValue class
////////////////////////////////////////////////////////////////////

use crate::byte_code_compiler::ByteCodeCompiler;
use crate::connections::Connections::{BLOBStoreHandle, WebServerHandle, WebSocketHandle};
use crate::connections::{ConnectionTypes, Connections};
use crate::conversions::Conversions;
use crate::data_types::DataType::*;
use crate::data_types::*;
use crate::dataframe::Dataframe;
use crate::dataframe::Dataframe::ModelTable;
use crate::errors::Errors::{CannotSubtract, Exact, IncompatibleParameters, Multiple, TypeMismatch};
use crate::errors::TypeMismatchErrors::{CannotBeNegated, UnsupportedType};
use crate::errors::{throw, Errors, TypeMismatchErrors};
use crate::expression::Expression;
use crate::model_row_collection::ModelRowCollection;
use crate::numbers::Numbers;
use crate::numbers::Numbers::*;
use crate::packages::PackageOps;
use crate::parameter::Parameter;
use crate::row_collection::RowCollection;
use crate::sequences::*;
use crate::structures::Row;
use crate::structures::Structures::{Firm, Hard, Soft};
use crate::structures::*;
use crate::typed_values::TypedValue::*;
use crate::utils::*;
use log::error;
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::Bound;
use std::fmt::Display;
use std::hash::{DefaultHasher, Hasher};
use std::ops::*;

/// Basic value unit
#[derive(Clone, Debug, Eq, Ord, PartialEq, Serialize, Deserialize)]
pub enum TypedValue {
    ArrayValue(Array),
    Boolean(bool),
    ByteStringValue(Vec<u8>),
    CharValue(char),
    Connection(Connections),
    DateTimeValue(i64),
    EnumValue(Option<u16>, Vec<Parameter>),
    ErrorValue(Errors),
    Function { params: Vec<Parameter>, body: Box<Expression>, returns: DataType },
    Kind(DataType),
    Null,
    Number(Numbers),
    PackageFunction(PackageOps),
    StringValue(String),
    Structured(Structures),
    TableValue(Dataframe),
    TupleValue(Vec<TypedValue>),
    Undefined,
    UUIDValue(u128),
}

impl TypedValue {

    ////////////////////////////////////////////////////////////////////
    //  Static Methods
    ////////////////////////////////////////////////////////////////////

    pub fn exclusive_range(a: Self, b: Self, step: Self) -> Vec<Self> {
        let mut items = Vec::new();
        let mut n = a;
        while n < b {
            items.push(n.clone());
            n = n + step.clone();
        }
        items
    }

    pub fn inclusive_range(a: Self, b: Self, step: Self) -> Vec<Self> {
        let mut items = Vec::new();
        let mut n = a;
        while n <= b {
            items.push(n.clone());
            n = n + step.clone();
        }
        items
    }

    pub fn from_json(j_value: &serde_json::Value) -> Self {
        Conversions::convert_from_json(j_value)
    }

    pub fn from_numeric(text: &str) -> std::io::Result<TypedValue> {
        Conversions::convert_from_numeric(text)
    }

    pub fn from_result(result: std::io::Result<TypedValue>) -> TypedValue {
        result.unwrap_or_else(|err| ErrorValue(Exact(err.to_string())))
    }

    ////////////////////////////////////////////////////////////////////
    //  Instance Methods
    ////////////////////////////////////////////////////////////////////

    pub fn coalesce(&self, other: TypedValue) -> TypedValue {
        match self {
            Null | Undefined => other,
            _ => self.clone()
        }
    }

    /// returns true, if:
    /// 1. the host value is an array, and the item value is found within it,
    /// 2. the host value is a table, and the item value matches a row found within it,
    /// 3. the host value is a struct, and the item value matches a name (key) found within it,
    pub fn contains(&self, value: &TypedValue) -> bool {
        match (&self, value) {
            (ArrayValue(items), item) => items.contains(item),
            (ByteStringValue(items), item) => items.contains(&item.to_u8()),
            (Number(a), Number(b)) => a.to_f64() == b.to_f64(),
            (StringValue(a), StringValue(b)) => a.contains(b),
            (StringValue(a), CharValue(b)) => a.contains(&b.to_string()),
            (Structured(s), StringValue(name)) => s.contains(name),
            (TableValue(ModelTable(mrc)), Structured(s)) => mrc.contains(&Structures::transform_row(
                &s.get_parameters(),
                &s.get_values(),
                &mrc.get_parameters()
            )),
            (TupleValue(values), item) => values.contains(item),
            _ => false
        }
    }

    pub fn convert_to(&self, dest_type: &DataType) -> std::io::Result<TypedValue> {
        Conversions::convert_to_datatype(self, dest_type)
    }

    pub fn encode(&self) -> std::io::Result<Vec<u8>> {
        ByteCodeCompiler::encode_value(self)
    }

    pub fn equals(&self, other: &TypedValue) -> bool {
        fn check(value: &str, index: u16, params: &Vec<Parameter>) -> bool {
            match index.to_usize() {
                None => false,
                Some(n) => n < params.len() && params[n].get_name() == value
            }
        }

        match (self, other) {
            (StringValue(s), EnumValue(Some(n), params)) => check(s, *n, params),
            (EnumValue(Some(n), params), StringValue(s)) => check(s, *n, params),
            _ => *self == *other
        }
    }

    pub fn get_type(&self) -> DataType {
        match self {
            ArrayValue(a) => FixedSizeType(ArrayType(a.get_component_type().into()).into(), a.len()),
            Boolean(..) => BooleanType,
            ByteStringValue(bytes) => FixedSizeType(ByteStringType.into(), bytes.len()),
            CharValue(..) => CharType,
            Connection(conn) => ConnectionType(conn.get_type()),
            DateTimeValue(..) => DateTimeType,
            EnumValue(_, params) => EnumType(params.clone()),
            ErrorValue(..) => ErrorType,
            Function { params, returns, .. } =>
                FunctionType(params.clone(), Box::from(returns.clone())),
            Kind(data_type) => data_type.clone(),
            Null | Undefined => RuntimeResolvedType,
            Number(n) => NumberType(n.kind()),
            PackageFunction(op) => op.get_type(),
            Structured(s) => StructureType(s.get_parameters()),
            StringValue(s) => FixedSizeType(StringType.into(), s.chars().count()),
            TableValue(tv) => TableType(Parameter::from_columns(tv.get_columns())),
            TupleValue(items) => TupleType(items.iter()
                .map(|i| i.get_type())
                .collect()),
            UUIDValue(..) => UUIDType,
        }
    }

    pub fn get_type_decl(&self) -> String {
        self.get_type().to_code()
    }

    pub fn hash_code(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        hasher.write(&self.encode().unwrap_or_else(|err| {
            error!("{}", err.to_string());
            vec![]
        }));
        hasher.finish()
    }

    pub fn head(&self) -> TypedValue {
        match self {
            ArrayValue(array) =>
                if array.is_empty() { Undefined } else { array.get(0).unwrap_or(Undefined) },
            StringValue(s) => s.chars().nth(0).map(|c| CharValue(c)).unwrap_or(Undefined),
            TableValue(df) =>
                match df.find_first_active_row() {
                    Ok(maybe_row) => maybe_row
                        .map(|row| Structured(Firm(row, df.get_parameters())))
                        .unwrap_or(Undefined),
                    Err(err) => ErrorValue(Exact(err.to_string())),
                }
            TupleValue(items) =>
                if items.is_empty() { Undefined } else { items[0].clone() },
            _ => Undefined,
        }
    }

    pub fn is_ok(&self) -> bool {
        matches!(self, Boolean(true) | Number(..) | TableValue(..))
    }

    pub fn is_pure(&self) -> bool {
        match self {
            Function { params, body, .. } =>
                body.is_pure() &&
                    params.iter().all(|p| p.get_default_value().is_pure()),
            PackageFunction(..) => false,
            TableValue(df) => df.is_pure(),
            TupleValue(items) => items.iter().all(|i| i.is_pure()),
            _ => true,
        }
    }

    pub fn is_true(&self) -> bool {
        matches!(self, Boolean(true))
    }

    pub fn tail(&self) -> TypedValue {
        match self {
            ArrayValue(array) => ArrayValue(array.tail()),
            StringValue(s) => StringValue(if s.len() < 2 { "".into() } else { s[1..].into() }),
            TableValue(df) =>
                match df.tail() {
                    Ok(df) => TableValue(df),
                    Err(err) => ErrorValue(Exact(err.to_string())),
                }
            TupleValue(items) => TupleValue(if items.len() < 2 { vec![] } else { items[1..].to_vec() }),
            _ => Undefined,
        }
    }

    pub fn matches(&self, other: &Self) -> TypedValue {
        match (self, other) {
            (a, b) if *a == *b => Boolean(true),
            (Structured(Soft(a)), Structured(Soft(b))) => {
                Boolean(a.to_name_values_mapping() == b.to_name_values_mapping())
            }
            (Structured(Hard(a)), Structured(Hard(b))) => Boolean(a == b),
            _ => Boolean(false)
        }
    }

    pub fn sublist(&self, start: usize, end: usize) -> Self {
        match self {
            ArrayValue(array) => ArrayValue(array.sublist(start, end)),
            ByteStringValue(bytes) => ByteStringValue(bytes[start..end].to_vec()),
            StringValue(s) => StringValue(s[start..end].to_string()),
            TableValue(df) => TableValue(df.sublist(start, end)),
            TupleValue(items) => TupleValue(items[start..end].to_vec()),
            other => other.to_owned()
        }
    }

    pub fn to_array(&self) -> std::io::Result<TypedValue> {
        self.to_vec().map(|items| ArrayValue(Array::from(items)))
    }

    pub fn to_bool(&self) -> bool {
        match self {
            Boolean(b) => *b,
            _ => false
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            ArrayValue(array) => array.get_values().iter()
                .map(|v| v.to_u8()).collect::<Vec<_>>(),
            ByteStringValue(bytes) => bytes.to_vec(),
            CharValue(c) => {
                let mut buf = [0; 4];
                let s = c.encode_utf8(&mut buf);
                s.as_bytes().to_vec()
            }
            DateTimeValue(t) => t.to_be_bytes().to_vec(),
            Number(n) => n.encode(),
            StringValue(s) => s.bytes().collect(),
            TableValue(df) => df.to_bytes(),
            UUIDValue(uuid) => u128::to_be_bytes(*uuid).to_vec(),
            z => z.encode().unwrap_or_else(|err| {
                error!("{}", err.to_string());
                vec![]
            })
        }
    }

    pub fn to_code(&self) -> String {
        match self {
            ArrayValue(items) =>
                format!("[{}]", items.iter()
                    .map(|v| v.to_code())
                    .collect::<Vec<_>>().join(", ")),
            CharValue(c) => format!("'{c}'"),
            PackageFunction(pf) => pf.to_code(),
            Structured(Hard(hs)) => hs.to_code(),
            Structured(Soft(ss)) => ss.to_code(),
            StringValue(s) => format!("\"{s}\""),
            TupleValue(items) =>
                format!("({})", items.iter()
                    .map(|v| v.to_code())
                    .collect::<Vec<_>>().join(", ")),
            other => other.unwrap_value()
        }
    }

    pub fn to_char(&self) -> Option<char> {
        match self {
            CharValue(c) => Some(*c),
            StringValue(s) => s.chars().next(),
            _ => None
        }
    }

    pub fn to_dataframe(&self) -> std::io::Result<Dataframe> {
        match self {
            ArrayValue(items) => Conversions::convert_values_to_table(&items.get_values()),
            Structured(s) => Ok(s.to_dataframe()),
            TableValue(df) => Ok(df.to_owned()),
            z => throw(TypeMismatch(TypeMismatchErrors::TableExpected(z.to_code())))
        }
    }

    pub fn to_f64(&self) -> f64 {
        self.to_number().map(|n| n.to_f64()).unwrap_or(0.)
    }

    pub fn to_i64(&self) -> i64 {
        self.to_number().map(|n| n.to_i64()).unwrap_or(0)
    }

    pub fn to_i128(&self) -> i128 {
        self.to_number().map(|n| n.to_i128()).unwrap_or(0)
    }

    pub fn to_json(&self) -> serde_json::Value {
        Conversions::convert_to_json(self)
    }

    pub fn to_number(&self) -> Option<Numbers> {
        match self {
            ByteStringValue(bytes) => u8_vec_to_u128(bytes.to_vec()).map(U128Value),
            Connection(conn) =>
                match conn {
                    BLOBStoreHandle(uuid) => Some(U128Value(*uuid)),
                    WebSocketHandle(uuid) => Some(U128Value(*uuid)),
                    WebServerHandle(port) => Some(U16Value(*port)),
                }
            DateTimeValue(t) => Some(I64Value(*t)),
            Number(nv) => Some(*nv),
            StringValue(string) => u8_vec_to_u128(string.bytes().collect::<Vec<_>>()).map(U128Value),
            UUIDValue(uuid) => Some(U128Value(*uuid)),
            _ => None
        }
    }

    pub fn to_sequence(&self) -> std::io::Result<Sequences> {
        Conversions::convert_to_sequence(self)
    }

    pub fn to_table(&self) -> std::io::Result<Box<dyn RowCollection>> {
        Ok(Box::new(self.to_dataframe()?))
    }

    pub fn to_table_with_schema(&self, params: &Vec<Parameter>) -> std::io::Result<TypedValue> {
        let df = self.to_dataframe()?;
        let my_params = df.get_parameters();
        match Parameter::are_compatible(params, &my_params) {
            true => Ok(TableValue(df)),
            false => throw(IncompatibleParameters(my_params))
        }
    }

    pub fn to_u8(&self) -> u8 {
        self.to_number().map(|n| n.to_u8()).unwrap_or(0)
    }

    pub fn to_u16(&self) -> u16 {
        self.to_number().map(|n| n.to_u16()).unwrap_or(0)
    }

    pub fn to_u32(&self) -> u32 {
        self.to_number().map(|n| n.to_u32()).unwrap_or(0)
    }

    pub fn to_u64(&self) -> u64 {
        self.to_number().map(|n| n.to_u64()).unwrap_or(0)
    }

    pub fn to_u128(&self) -> u128 {
        self.to_number().map(|n| n.to_u128()).unwrap_or(0)
    }

    pub fn to_usize(&self) -> usize {
        self.to_number().map(|n| n.to_usize()).unwrap_or(0)
    }

    pub fn to_vec(&self) -> std::io::Result<Vec<TypedValue>> {
       let items = match self {
            ArrayValue(items) => items.get_values(),
            ByteStringValue(bytes) => u8_vec_to_values(bytes),
            ErrorValue(err) => return throw(err.to_owned()),
            StringValue(s) => string_to_char_values(s),
            Structured(st) => st.to_name_value_tuples(),
            TableValue(df) => df.to_array().get_values(),
            TupleValue(items) => items.to_vec(),
            z => return throw(TypeMismatch(UnsupportedType(TableType(vec![]), z.get_type())))
        };
        Ok(items)
    }

    pub fn unwrap_value(&self) -> String {
        Conversions::unwrap_value(self)
    }

    ///////////////////////////////////////////////////////////////
    //      CONDITIONAL OPERATIONS
    ///////////////////////////////////////////////////////////////

    pub fn and(&self, rhs: &TypedValue) -> Option<TypedValue> {
        Some(Boolean(self.to_bool() && rhs.to_bool()))
    }
    
    pub fn or(&self, rhs: &Self) -> Option<Self> {
        Some(Boolean(self.to_bool() || rhs.to_bool()))
    }

    pub fn pow(&self, rhs: &Self) -> Option<Self> {
        match (self.clone(), rhs.clone()) {
            (TupleValue(a), TupleValue(b)) => {
                Some(TupleValue(pow_vec(a, b)))
            }
            (..) => {
                let n = num_traits::pow(self.to_f64(), rhs.to_usize());
                Some(Number(F64Value(n)))
            }
        }
    }

    pub fn range_exclusive(&self, rhs: &Self) -> Option<Self> {
        let mut values = Vec::new();
        match (self, rhs) {
            (CharValue(a), CharValue(b)) =>
                for c in (*a)..(*b) { values.push(CharValue(c)) }
            (Number(a), Number(b)) =>
                for n in a.to_i64()..b.to_i64() { values.push(Number(I64Value(n))) }
            (DateTimeValue(a), DateTimeValue(b)) =>
                for n in *a..*b { values.push(DateTimeValue(n)) }
            _ => return None
        }
        Some(ArrayValue(Array::from(values)))
    }

    pub fn range_inclusive(&self, rhs: &Self) -> Option<Self> {
        let mut values = Vec::new();
        match (self, rhs) {
            (CharValue(a), CharValue(b)) =>
                for c in (*a)..=(*b) { values.push(CharValue(c)) }
            (Number(a), Number(b)) =>
                for n in a.to_i64()..=b.to_i64() { values.push(Number(I64Value(n))) },
            (DateTimeValue(a), DateTimeValue(b)) =>
                for n in *a..=*b { values.push(DateTimeValue(n)) }
            _ => return None
        }
        Some(ArrayValue(Array::from(values)))
    }
}

impl Add for TypedValue {
    type Output = TypedValue;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (ArrayValue(a), b) => ArrayValue(Array::from(a.get_values().iter()
                .map(|i| i.clone() + b.clone())
                .collect::<Vec<_>>())),
            (b, ArrayValue(a)) => ArrayValue(Array::from(a.get_values().iter()
                .map(|i| i.clone() + b.clone())
                .collect::<Vec<_>>())),
            (Boolean(a), Boolean(b)) => Boolean(a | b),
            (CharValue(a), Number(b)) => char_map(a, b.to_u32(), |a, b| a + b),
            (CharValue(a), StringValue(b)) => StringValue(a.to_string() + b.as_str()),
            (DateTimeValue(dt), Number(n)) => DateTimeValue(dt + n.to_i64()),
            (ErrorValue(Multiple(mut errors0)), ErrorValue(Multiple(errors1))) => {
                errors0.extend(errors1);
                ErrorValue(Multiple(errors0))
            }
            (ErrorValue(err), ErrorValue(Multiple(mut errors))) => {
                errors.push(err);
                ErrorValue(Multiple(errors))
            }
            (ErrorValue(Multiple(mut errors)), ErrorValue(err)) => {
                errors.push(err);
                ErrorValue(Multiple(errors))
            }
            (ErrorValue(a), ErrorValue(b)) => ErrorValue(Multiple(vec![a, b])),
            (ErrorValue(a), _) => ErrorValue(a),
            (_, ErrorValue(b)) => ErrorValue(b),
            (Number(n), DateTimeValue(dt)) => DateTimeValue(dt + n.to_i64()),
            (Number(a), CharValue(b)) => CharValue(b).add(Number(a)),
            (Number(a), Number(b)) => Number(a + b),
            (StringValue(a), StringValue(b)) => StringValue(a + b.as_str()),
            (StringValue(a), CharValue(b)) => StringValue(a + b.to_string().as_str()),
            (TableValue(a), Structured(Hard(b))) => {
                let mut mrc = match ModelRowCollection::from_table(Box::new(&a)) {
                    Ok(mrc) => mrc,
                    Err(err) => return ErrorValue(Exact(err.to_string()))
                };
                match mrc.append_row(Structures::transform_row(
                    &b.get_parameters(),
                    &b.get_values(),
                    &mrc.get_parameters()
                )) {
                    Err(err) => ErrorValue(Exact(err.to_string())),
                    Ok(_) => TableValue(ModelTable(mrc)),
                }
            }
            (TableValue(a), TableValue(b)) =>
                match ModelRowCollection::combine(a.get_columns().to_owned(), vec![&a, &b]) {
                    Ok(mrc) => TableValue(ModelTable(mrc)),
                    Err(err) => ErrorValue(Exact(err.to_string()))
                },
            (TupleValue(a), TupleValue(b)) => TupleValue(add_vec(a, b)),
            (a, b) => {
                println!("add: {a:?} vs. {b:?}");
                ErrorValue(TypeMismatch(UnsupportedType(a.get_type(), b.get_type())))
            }
        }
    }
}

impl BitAnd for TypedValue {
    type Output = TypedValue;

    fn bitand(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (CharValue(a), Number(b)) => char_map(a, b.to_u32(), |a, b| a & b),
            (Number(a), CharValue(b)) => CharValue(b).bitand(Number(a)),
            (Number(a), Number(b)) => Number(a & b),
            (TableValue(a), TableValue(b)) =>
                match a.intersect(&b) {
                    Ok(df) => TableValue(df),
                    Err(err) => ErrorValue(Exact(err.to_string()))
                }
            (TupleValue(a), TupleValue(b)) => TupleValue(bitand_vec(a, b)),
            _ => Undefined
        }
    }
}

impl BitOr for TypedValue {
    type Output = TypedValue;

    fn bitor(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (CharValue(a), Number(b)) => char_map(a, b.to_u32(), |a, b| a | b),
            (Number(a), CharValue(b)) => CharValue(b).bitor(Number(a)),
            (Number(a), Number(b)) => Number(a | b),
            (TableValue(a), TableValue(b)) => TableValue(a.union(&b)),
            (TupleValue(a), TupleValue(b)) => TupleValue(bitor_vec(a, b)),
            _ => Undefined
        }
    }
}

impl BitXor for TypedValue {
    type Output = TypedValue;

    fn bitxor(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (CharValue(a), Number(b)) => char_map(a, b.to_u32(), |a, b| a ^ b),
            (Number(a), CharValue(b)) => CharValue(b).bitxor(Number(a)),
            (Number(a), Number(b)) => Number(a ^ b),
            (TupleValue(a), TupleValue(b)) => TupleValue(bitxor_vec(a, b)),
            _ => Undefined
        }
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
        match (self, rhs) {
            // divide a character by a number
            (CharValue(a), Number(b)) => char_div(a, b.to_u32()),
            (Number(a), CharValue(b)) => CharValue(b).div(Number(a)),
            // divide two numbers
            (Number(..), Number(b)) if b.is_effectively_zero() => Number(NaNValue),
            (Number(a), Number(b)) => Number(a / b),
            // divide two tuples
            (TupleValue(a), TupleValue(b)) => TupleValue(div_vec(a, b)),
            _ => Undefined
        }
    }
}

fn fetch(mrc: &Box<dyn RowCollection>, index: usize) -> TypedValue {
    let parameters = mrc.get_parameters();
    let columns = mrc.get_columns();
    match mrc.read_row(index) {
        Ok((row, meta)) => {
            Structured(match meta.is_allocated {
                true => Firm(row, parameters.to_owned()),
                false => Firm(Row::create(row.get_id(), &columns), parameters),
            })
        }
        Err(err) => ErrorValue(Exact(err.to_string())),
    }
}

impl Mul for TypedValue {
    type Output = TypedValue;

    fn mul(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            // multiply each element of the array by the other value
            (ArrayValue(a), b) => ArrayValue(Array::from(a.get_values().iter()
                .map(|i| i.clone() * b.clone())
                .collect::<Vec<_>>())),
            (b, ArrayValue(a)) => ArrayValue(Array::from(a.get_values().iter()
                .map(|i| i.clone() * b.clone())
                .collect::<Vec<_>>())),
            // multiply a character by a number
            (CharValue(a), Number(b)) => char_map(a, b.to_u32(), |a, b| a * b),
            (Number(a), CharValue(b)) => CharValue(b).mul(Number(a)),
            // multiply two numbers
            (Number(a), Number(b)) => Number(a * b),
            // repeat a string `n` times
            (StringValue(a), Number(n)) => StringValue(a.repeat(n.to_usize())),
            (TableValue(a), TableValue(b)) => TableValue(a.product(&b)),
            (TupleValue(a), TupleValue(b)) => TupleValue(mul_vec(a, b)),
            // fallback for unsupported types
            _ => Undefined,
        }
    }
}

impl Neg for TypedValue {
    type Output = TypedValue;

    fn neg(self) -> Self::Output {
        let error: fn(String) -> TypedValue = |s| ErrorValue(TypeMismatch(CannotBeNegated(s)));
        match self {
            ArrayValue(v) => ArrayValue(v.map(|tv| tv.to_owned().neg())),
            Boolean(n) => Boolean(!n),
            ByteStringValue(..) => error("Bytes".into()),
            CharValue(c) => CharValue(c),
            Connection(..) => error("Connection".into()),
            DateTimeValue(dt) => Number(I64Value(dt.neg())),
            EnumValue(..) => error("Enum".into()),
            ErrorValue(msg) => ErrorValue(msg),
            Function { .. } => error("Function".into()),
            Kind(data_type) => error(data_type.to_code()),
            Null => Null,
            Number(nv) => Number(nv.neg()),
            PackageFunction(..) => error("PlatformFunction".into()),
            StringValue(..) => error("String".into()),
            Structured(..) => error("Structure".into()),
            TableValue(..) => error("Table".into()),
            TupleValue(a) => TupleValue(neg_vec(a)),
            Undefined => Undefined,
            UUIDValue(..) => error("UUID".into()),
        }
    }
}

impl Not for TypedValue {
    type Output = TypedValue;

    fn not(self) -> Self::Output {
        match self {
            ArrayValue(a) => ArrayValue(a.map(|i| i.to_owned().neg())),
            Boolean(v) => Boolean(!v),
            Number(a) => Number(-a),
            TupleValue(a) => TupleValue(not_vec(a)),
            _ => Undefined
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
        match (self, rhs) {
            (CharValue(a), Number(b)) => char_map(a, b.to_u32(), |a, b| a % b),
            (Number(a), CharValue(b)) => CharValue(b).rem(Number(a)),
            (Number(a), Number(b)) => Number(a % b),
            (TupleValue(a), TupleValue(b)) => TupleValue(rem_vec(a, b)),
            _ => Undefined
        }
    }
}

impl PartialOrd for TypedValue {
    fn partial_cmp(&self, rhs: &Self) -> Option<Ordering> {
        match (&self, &rhs) {
            (ArrayValue(a), ArrayValue(b)) => a.partial_cmp(b),
            (ByteStringValue(a), ByteStringValue(b)) => a.partial_cmp(b),
            (Boolean(a), Boolean(b)) => a.partial_cmp(b),
            (Number(a), Number(b)) => a.partial_cmp(b),
            (StringValue(a), StringValue(b)) => a.partial_cmp(b),
            (TupleValue(a), TupleValue(b)) => a.partial_cmp(b),
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
        match (self, rhs) {
            (CharValue(a), Number(b)) => char_map(a, b.to_u32(), |a, b| a << b),
            (Number(a), CharValue(b)) => CharValue(b).shl(Number(a)),
            (Number(a), Number(b)) => Number(a << b),
            (TupleValue(a), TupleValue(b)) => TupleValue(shl_vec(a, b)),
            _ => Undefined
        }
    }
}

impl Shr for TypedValue {
    type Output = TypedValue;

    fn shr(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            // multiply a character by a number
            (CharValue(a), Number(b)) => char_map(a, b.to_u32(), |a, b| a >> b),
            (Number(a), CharValue(b)) => CharValue(b).shr(Number(a)),
            (Number(a), Number(b)) => Number(a >> b),
            (TupleValue(a), TupleValue(b)) => TupleValue(shr_vec(a, b)),
            _ => Undefined
        }
    }
}

impl Sub for TypedValue {
    type Output = TypedValue;

    fn sub(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Boolean(a), Boolean(b)) => Boolean(a & b),
            (CharValue(a), Number(b)) => char_map(a, b.to_u32(), |a, b| a - b),
            (DateTimeValue(a), DateTimeValue(b)) => Number(I64Value(a - b)),
            (DateTimeValue(dt), Number(n)) => DateTimeValue(dt - n.to_i64()),
            (Number(a), CharValue(b)) => CharValue(b).sub(Number(a)),
            (Number(a), Number(b)) => Number(a - b),
            (TupleValue(a), TupleValue(b)) => TupleValue(sub_vec(a, b)),
            (a, b) => ErrorValue(CannotSubtract(a.to_code(), b.to_code()))
        }
    }
}

/// Unit tests
#[cfg(test)]
mod tests {

    /// equality tests
    #[cfg(test)]
    mod equality_tests {
        use crate::test_util::interpret;
        use crate::typed_values::TypedValue;

        #[test]
        fn test_equals_i64_vs_i64() {
            let a = interpret("100");
            let b = interpret("100");
            assert!(a.equals(&b));
        }

        #[test]
        fn test_equals_string_vs_enum() {
            let a = TypedValue::StringValue("NYSE".into());
            let b = interpret(r#"Enum(AMEX, NYSE, NASDAQ, OTCBB)::new("NYSE")"#);
            assert!(a.equals(&b));
            assert!(b.equals(&a));
        }

        #[test]
        fn test_not_equal_string_vs_enum() {
            let a = TypedValue::StringValue("NYSE".into());
            let b = interpret(r#"Enum(AMEX, NYSE, NASDAQ, OTCBB)::new()"#);
            assert!(!a.equals(&b));
        }

    }

    /// collection tests
    #[cfg(test)]
    mod collection_tests {
        use crate::data_types::DataType::*;
        use crate::numbers::Numbers::*;
        use crate::sequences::{Sequence, Sequences};
        use crate::structures::SoftStructure;
        use crate::structures::Structures::Soft;
        use crate::test_util::interpret;
        use crate::typed_values::TypedValue::*;

        #[test]
        fn test_bounds() {
            let a = interpret("1");
            let b = interpret("5");
            let bounds = a..b;
            assert_eq!(bounds.start, Number(I64Value(1)));
            assert_eq!(bounds.end, Number(I64Value(5)));
        }

        #[test]
        fn test_contains_array() {
            let array = interpret("[11, 234, 78, 7, 77]");
            assert!(array.contains(&Number(I64Value(234))));

            let sequence = array.to_sequence().unwrap();
            assert!(sequence.contains(&Number(I64Value(78))));

            let item = sequence.get_or_else(3, Undefined);
            assert_eq!(item, Number(I64Value(7)));
        }

        #[test]
        fn test_contains_bytestring() {
            let bytestring = interpret("0B48656c6c6f20576f726c64");
            assert!(bytestring.contains(&Number(U8Value(b'W'))));
        }

        #[test]
        fn test_contains_string() {
            let string = StringValue("Hello World".into());
            assert!(string.contains(&CharValue('W')));
        }

        #[test]
        fn test_contains_range() {
            let range = Sequences::TheRange(Number(I64Value(0)).into(), Number(I64Value(10)).into(), true);
            assert!(range.contains(&Number(I64Value(6))));

            let item = range.get_or_else(3, Undefined);
            assert_eq!(item, Number(I64Value(3)));
        }

        #[test]
        fn test_contains_table() {
            let table = interpret(r#"
                |--------------------------------|
                | symbol | exchange  | last_sale |
                |--------------------------------|
                | GIF    | NYSE      | 11.77     |
                | TRX    | NASDAQ    | 32.97     |
                | RLP    | NYSE      | 23.66     |
                | GTO    | NASDAQ    | 51.23     |
                | BST    | NASDAQ    | 214.88    |
                |--------------------------------|
            "#);
            let item = Structured(Soft(SoftStructure::new(&vec![
                ("symbol", StringValue("GTO".into())),
                ("exchange", StringValue("NASDAQ".into())),
                ("last_sale", Number(F64Value(51.23)))
            ])));
            assert!(table.contains(&item));

            let sequence = table.to_sequence().unwrap();
            assert!(sequence.contains(&item));

            let new_item = sequence.get_or_else(3, Undefined);
            assert_eq!(new_item, TupleValue(vec![
                StringValue("GTO".into()),
                StringValue("NASDAQ".into()),
                Number(F64Value(51.23))
            ]));
        }

        #[test]
        fn test_contains_tuple() {
            let tuple = interpret(r#"(1, 'C', "D")"#);
            assert!(tuple.contains(&CharValue('C')));

            let sequence = tuple.to_sequence().unwrap();
            assert!(sequence.contains(&StringValue(String::from("D"))));

            let item = sequence.get_or_else(0, Undefined);
            assert_eq!(item, Number(I64Value(1)));
        }

        #[test]
        fn test_sublist_array() {
            let string = StringValue("Hello World".into());
            let arr = string.to_array().unwrap();

            assert_eq!(arr.sublist(0, 5), interpret("['H', 'e', 'l', 'l', 'o']"));
        }

        #[test]
        fn test_sublist_bytestring() {
            let string = StringValue("Hello World".into());
            let bytestring = string.convert_to(&ByteStringType).unwrap();
            assert_eq!(bytestring.sublist(6, 11), ByteStringValue(b"World".to_vec()));
        }

        #[test]
        fn test_sublist_string() {
            let string = StringValue("Hello World".into());
            assert_eq!(string.sublist(2, 7), StringValue("llo W".into()));
        }

        #[test]
        fn test_sublist_table() {
            let table = interpret(r#"
                |--------------------------------|
                | symbol | exchange  | last_sale |
                |--------------------------------|
                | GIF    | NYSE      | 11.77     |
                | TRX    | NASDAQ    | 32.97     |
                | RLP    | NYSE      | 23.66     |
                | GTO    | NASDAQ    | 51.23     |
                | BST    | NASDAQ    | 214.88    |
                |--------------------------------|
            "#);
            let expected = interpret(r#"
                |--------------------------------|
                | symbol | exchange  | last_sale |
                |--------------------------------|
                | TRX    | NASDAQ    | 32.97     |
                | RLP    | NYSE      | 23.66     |
                |--------------------------------|
            "#);
            assert_eq!(table.sublist(1, 3), expected);
        }

        #[test]
        fn test_sublist_tuple() {
            let tuple = TupleValue(vec![
                Number(I32Value(3)),
                Number(U32Value(5)),
                Number(I8Value(8)),
            ]);
            assert_eq!(tuple.sublist(0, 2), TupleValue(vec![
                Number(I32Value(3)),
                Number(U32Value(5)),
            ]));
        }

        #[test]
        fn test_to_bytes_from_array() {
            let array = interpret(r#"[6, 8, 12, 16]"#);
            assert_eq!(array.to_bytes(), vec![6u8, 8u8, 12u8, 16u8]);
        }

        #[test]
        fn test_to_bytes_from_bytestring() {
            let bytestring = interpret(r#"0B06080c10"#);
            assert_eq!(bytestring.to_bytes(), vec![6u8, 8u8, 12u8, 16u8]);
        }

        #[test]
        fn test_to_bytes_from_char() {
            let char = interpret(r#"'🔥'"#);
            assert_eq!(char.to_bytes(), vec![240u8, 159u8, 148u8, 165u8]);
        }

        #[test]
        fn test_to_bytes_from_date() {
            let date = interpret("2025-08-18T21:48:28.603Z".into());
            assert_eq!(date.to_bytes(), [0u8, 0u8, 1u8, 152u8, 191u8, 39u8, 186u8, 59u8]);
        }

        #[test]
        fn test_to_bytes_from_f64() {
            let float = interpret("32.58".into());
            assert_eq!(float.to_bytes(), [64u8, 64u8, 74u8, 61u8, 112u8, 163u8, 215u8, 10u8]);
        }

        #[test]
        fn test_to_bytes_from_string() {
            let string = StringValue("Hello".into());
            assert_eq!(string.to_bytes(), [72u8, 101u8, 108u8, 108u8, 111u8]);
        }

        #[test]
        fn test_to_bytes_from_uuid() {
            let string = interpret("1ed9a6d6-7927-49a7-ac4c-adb8f81a5823".into());
            assert_eq!(string.to_bytes(), [
                30u8, 217u8, 166u8, 214u8, 121u8, 39u8, 73u8, 167u8,
                172u8, 76u8, 173u8, 184u8, 248u8, 26u8, 88u8, 35u8
            ]);
        }

    }

    /// core tests
    #[cfg(test)]
    mod core_tests {
        use crate::conversions::Conversions;
        use crate::dataframe::Dataframe::ModelTable;
        use crate::model_row_collection::ModelRowCollection;
        use crate::numbers::Numbers::*;
        use crate::sequences::Array;
        use crate::structures::Structures::{Hard, Soft};
        use crate::structures::{HardStructure, SoftStructure};
        use crate::test_util::*;
        use crate::typed_values::TypedValue;
        use crate::typed_values::TypedValue::*;
        use serde_json::json;

        #[test]
        fn test_exclusive_range() {
            let (a, b, c) = (
                Number(I64Value(1)), Number(I64Value(5)), Number(I64Value(1))
            );
            let list = TypedValue::exclusive_range(a, b, c);
            assert_eq!(list, vec![
                Number(I64Value(1)), Number(I64Value(2)), Number(I64Value(3)),
                Number(I64Value(4)),
            ])
        }

        #[test]
        fn test_inclusive_range() {
            let (a, b, c) = (
                Number(I64Value(1)), Number(I64Value(5)), Number(I64Value(1))
            );
            let list = TypedValue::inclusive_range(a, b, c);
            assert_eq!(list, vec![
                Number(I64Value(1)), Number(I64Value(2)), Number(I64Value(3)),
                Number(I64Value(4)), Number(I64Value(5)),
            ])
        }

        #[test]
        fn test_array_contains() {
            let array = ArrayValue(Array::from(vec![
                StringValue("ABC".into()),
                StringValue("R2X2".into()),
                StringValue("123".into()),
                StringValue("Hello".into()),
            ]));
            assert_eq!(array.contains(&StringValue("123".into())), true)
        }

        #[test]
        fn test_hard_structure_contains_key() {
            let structure = Structured(Hard(HardStructure::new(make_quote_parameters(), vec![
                StringValue("PEW".into()),
                StringValue("NASDAQ".into()),
                Number(F64Value(304.69)),
            ])));
            assert_eq!(structure.contains(&StringValue("symbol".into())), true)
        }

        #[test]
        fn test_soft_structure_contains_key() {
            let structure = Structured(Soft(SoftStructure::new(&vec![
                ("name", StringValue("symbol".into())),
                ("param_type", StringValue("String(8)".into())),
                ("default_value", Null),
            ])));
            assert_eq!(structure.contains(&StringValue("name".into())), true)
        }

        #[test]
        fn test_table_contains_hard_structure() {
            let phys_columns = make_quote_columns();
            let table = TableValue(ModelTable(ModelRowCollection::from_columns_and_rows(&phys_columns, &vec![
                make_quote(0, "BOX", "AMEX", 11.77),
                make_quote(1, "UNO", "OTC", 0.2456),
                make_quote(2, "BIZ", "NYSE", 23.67),
                make_quote(3, "GOTO", "OTC", 0.1421),
                make_quote(4, "BOOM", "NASDAQ", 56.87),
                make_quote(5, "TRX", "NASDAQ", 7.9311),
            ])));
            let hs = Structured(Hard(HardStructure::new(make_quote_parameters(), vec![
                StringValue("GOTO".into()),
                StringValue("OTC".into()),
                Number(F64Value(0.1421)),
            ])));
            assert_eq!(table.contains(&hs), true)
        }

        #[test]
        fn test_table_contains_soft_structure() {
            let phys_columns = make_quote_columns();
            let table = TableValue(ModelTable(ModelRowCollection::from_columns_and_rows(&phys_columns, &vec![
                make_quote(0, "ABC", "AMEX", 11.77),
                make_quote(1, "UNO", "OTC", 0.2456),
                make_quote(2, "BIZ", "NYSE", 23.67),
                make_quote(3, "GOTO", "OTC", 0.1421),
                make_quote(4, "BOOM", "NASDAQ", 56.87),
                make_quote(5, "TRX", "NASDAQ", 7.9311),
            ])));
            let ss = Structured(Soft(SoftStructure::new(&vec![
                ("symbol", StringValue("BIZ".into())),
                ("exchange", StringValue("NYSE".into())),
                ("last_sale", Number(F64Value(23.67))),
            ])));
            assert_eq!(table.contains(&ss), true)
        }

        #[test]
        fn test_eq() {
            assert_eq!(F64Value(45.0), F64Value(45.0));
            assert_eq!(I64Value(0x5555_FACE_CAFE_BABE), I64Value(0x5555_FACE_CAFE_BABE));
            assert_eq!(I128Value(0x5555_FACE_CAFE_BABE), I128Value(0x5555_FACE_CAFE_BABE));
            assert_eq!(I64Value(0x5555_FACE_CAFE_BABE), I64Value(0x5555_FACE_CAFE_BABE));
            assert_eq!(U128Value(0x5555_FACE_CAFE_BABE), U128Value(0x5555_FACE_CAFE_BABE));
        }

        #[test]
        fn test_ne() {
            assert_ne!(I64Value(0x5555_FACE_CAFE_BABE), I64Value(0xFACE_CAFE_BABE));
            assert_ne!(F64Value(99.142857), F64Value(19.48));
        }

        #[test]
        fn test_gt() {
            assert!({
                let a = ArrayValue(Array::from(vec![Number(I64Value(0xCE)), Number(I64Value(0xCE))]));
                let b = ArrayValue(Array::from(vec![Number(I64Value(0x23)), Number(I64Value(0xBE))]));
                a > b
            });
            assert!(I64Value(0x5555_FACE_CAFE_BABE) > I64Value(0x0000_FACE_CAFE_BABE));
            assert!(F64Value(359.7854) > F64Value(99.992));
        }

        #[test]
        fn test_numeric_conversion() {
            let v0 = Number(F64Value(12.56));
            assert_eq!(v0.to_bool(), false);
            assert_eq!(v0.to_code(), "12.56".to_string());
            assert_eq!(v0.to_f64(), 12.56);
            //assert_eq!(v0.to_i16(), 12);
            //assert_eq!(v0.to_i32(), 12);
            assert_eq!(v0.to_i64(), 12);
            assert_eq!(v0.to_i128(), 12);
            assert_eq!(v0.to_json(), json!(12.56));
            assert_eq!(v0.to_u16(), 12);
            assert_eq!(v0.to_u32(), 12);
            assert_eq!(v0.to_u64(), 12);
            assert_eq!(v0.to_u128(), 12);
            assert_eq!(v0.to_usize(), 12);
            assert_eq!(v0.unwrap_value(), "12.56".to_string());
        }

        #[test]
        fn test_hash_code() {
            let value = StringValue("Hello".into());
            assert_eq!(value.hash_code(), 2401575248544212836)
        }

        #[test]
        fn test_strings() {
            verify_encode_decode(&StringValue("Hello".into()));
        }

        #[test]
        fn test_string_contains() {
            let a = StringValue("Compare Array contents: Equal".into());
            let b = StringValue("Array".into());
            assert!(a.contains(&b))
        }
        
        #[test]
        fn test_wrap_and_unwrap_value() {
            verify_wrap_unwrap("false", Boolean(false));
            verify_wrap_unwrap("true", Boolean(true));
            verify_wrap_unwrap("2024-02-28T23:41:19.081Z", DateTimeValue(1709163679081));
            verify_wrap_unwrap("100.1", Number(F64Value(100.1)));
            verify_wrap_unwrap("100", Number(I64Value(100)));
            verify_wrap_unwrap("null", Null);
            verify_wrap_unwrap("\"Hello World\"", StringValue("Hello World".into()));
            verify_wrap_unwrap("undefined", Undefined);
        }

        #[test]
        fn test_unwrap_optional_value() {
            assert_eq!(
                Conversions::wrap_value_opt(&Some("123.45".into())).unwrap(),
                Number(F64Value(123.45))
            )
        }

        fn verify_encode_decode(expected: &TypedValue) {
            let data_type = expected.get_type();
            let bytes = expected.encode().unwrap();
            let actual = data_type.decode(&bytes, 0);
            assert_eq!(actual, *expected)
        }

        fn verify_wrap_unwrap(string_value: &str, typed_value: TypedValue) {
            assert_eq!(typed_value.to_code(), string_value);
            match Conversions::wrap_value(string_value) {
                Ok(value) => assert_eq!(value, typed_value),
                Err(message) => panic!("{}", message)
            };
        }
    }

    /// to_xxx tests
    #[cfg(test)]
    mod to_xxx_tests {
        use crate::data_types::DataType::{FixedSizeType, NumberType, StringType};
        use crate::number_kind::NumberKind::I64Kind;
        use crate::numbers::Numbers::{F64Value, I128Value, I64Value, U128Value};
        use crate::parameter::Parameter;
        use crate::structures::HardStructure;
        use crate::typed_values::TypedValue;
        use crate::typed_values::TypedValue::{Boolean, DateTimeValue, Null, Number, StringValue, UUIDValue, Undefined};
        use serde_json::Value;

        #[test]
        fn test_from_json() {
            let structure = HardStructure::from_parameters(vec![
                Parameter::new_with_default("name", FixedSizeType(StringType.into(), 4), StringValue("John".into())),
                Parameter::new_with_default("age", NumberType(I64Kind), Number(I64Value(40)))
            ]);
            let js_value0: Value = serde_json::from_str(r##"{"name":"John","age":40}"##).unwrap();
            let js_value1: Value = serde_json::from_str(structure.to_string().as_str()).unwrap();
            assert_eq!(js_value1, js_value0)
        }

        #[test]
        fn test_to_json() {
            verify_to_json(Boolean(false), serde_json::Value::Bool(false));
            verify_to_json(Boolean(true), serde_json::Value::Bool(true));
            verify_to_json(DateTimeValue(1709163679081), serde_json::Value::String("2024-02-28T23:41:19.081Z".into()));
            verify_to_json(Number(F64Value(100.1)), serde_json::json!(100.1));
            verify_to_json(Number(I64Value(-123_456_789)), serde_json::json!(-123_456_789));
            verify_to_json(Number(I128Value(-123_456_789)), serde_json::json!(-123_456_789));
            verify_to_json(Number(I64Value(100)), serde_json::json!(100));
            verify_to_json(Number(I64Value(123_456_789)), serde_json::json!(123_456_789));
            verify_to_json(Number(U128Value(123_456_789)), serde_json::json!(123_456_789));
            verify_to_json(Null, serde_json::Value::Null);
            verify_to_json(StringValue("Hello World".into()), serde_json::Value::String("Hello World".into()));
            verify_to_json(Undefined, serde_json::Value::Null);
        }

        #[test]
        fn test_to_number_date() {
            let ts = 1709163679081;
            let date = DateTimeValue(ts);
            assert_eq!(date.to_number(), Some(I64Value(ts)))
        }

        #[test]
        fn test_to_number_float() {
            let number = F64Value(17.091);
            let value = Number(number.clone());
            assert_eq!(value.to_number(), Some(number));
        }

        #[test]
        fn test_to_number_uuid() {
            let uuid = 0x4f4e96b0_e535_49cd_a0e5_3ebac7f48a34;
            let value = UUIDValue(uuid);
            assert_eq!(value.to_number(), Some(U128Value(uuid)));
        }

        #[test]
        fn test_to_string() {
            assert_eq!(format!("{}", StringValue("Hello".into())), "Hello");
        }

        fn verify_to_json(typed_value: TypedValue, expected: serde_json::Value) {
            assert_eq!(typed_value.to_json(), expected)
        }
        
    }
    
    /// math tests
    #[cfg(test)]
    mod math_tests {
        use crate::numbers::Numbers::*;
        use crate::test_util::verify_exact_code;
        use crate::typed_values::TypedValue::*;

        #[test]
        fn test_addition_f64_to_f64() {
            assert_eq!(Number(F64Value(45.0)) + Number(F64Value(32.7)), Number(F64Value(77.7)));
        }

        #[test]
        fn test_addition_i128_to_i128() {
            assert_eq!(Number(I128Value(45)) + Number(I128Value(32)), Number(I128Value(77)));
        }

        #[test]
        fn test_addition_i64_to_i64() {
            assert_eq!(Number(I64Value(45)) + Number(I64Value(32)), Number(I64Value(77)));
        }

        #[test]
        fn test_addition_u128_to_u128() {
            assert_eq!(Number(U128Value(45)) + Number(U128Value(32)), Number(U128Value(77)));
        }

        #[test]
        fn test_addition_bool_to_bool() {
            assert_eq!(Boolean(true) + Boolean(true), Boolean(true));
        }

        #[test]
        fn test_addition_string_to_string() {
            assert_eq!(StringValue("Hello".into()) + StringValue(" World".into()), StringValue("Hello World".into()));
        }

        #[test]
        fn test_division_char_to_i64() {
            verify_exact_code("'f' / 2", "'3'")
        }

        #[test]
        fn test_subtraction_f64_from_f64() {
            assert_eq!(Number(F64Value(45.0)) - Number(F64Value(32.5)), Number(F64Value(12.5)));
        }

        #[test]
        fn test_subtraction_i8_from_i8() {
            assert_eq!(Number(I8Value(45)) - Number(I8Value(32)), Number(I8Value(13)));
        }

        #[test]
        fn test_subtraction_i16_from_i16() {
            assert_eq!(Number(I16Value(45)) - Number(I16Value(32)), Number(I16Value(13)));
        }

        #[test]
        fn test_subtraction_i32_from_i32() {
            assert_eq!(Number(I32Value(45)) - Number(I32Value(32)), Number(I32Value(13)));
        }

        #[test]
        fn test_subtraction_i64_from_i64() {
            assert_eq!(Number(I64Value(45)) - Number(I64Value(32)), Number(I64Value(13)));
        }

        #[test]
        fn test_subtraction_i128_from_i128() {
            assert_eq!(Number(U128Value(45)) - Number(U128Value(32)), Number(U128Value(13)));
        }

        #[test]
        fn test_subtraction_u8_from_u8() {
            assert_eq!(Number(U8Value(45)) - Number(U8Value(32)), Number(U8Value(13)));
        }

        #[test]
        fn test_subtraction_u16_from_u16() {
            assert_eq!(Number(U16Value(45)) - Number(U16Value(32)), Number(U16Value(13)));
        }

        #[test]
        fn test_subtraction_u32_from_u32() {
            assert_eq!(Number(U32Value(45)) - Number(U32Value(32)), Number(U32Value(13)));
        }

        #[test]
        fn test_subtraction_u64_from_u64() {
            assert_eq!(Number(U64Value(45)) - Number(U64Value(32)), Number(U64Value(13)));
        }

        #[test]
        fn test_subtraction_u128_from_u128() {
            assert_eq!(Number(U128Value(45)) - Number(U128Value(32)), Number(U128Value(13)));
        }
    }

}