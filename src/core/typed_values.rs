#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// TypedValue class
////////////////////////////////////////////////////////////////////

use std::cmp::Ordering;
use std::collections::btree_map::IntoKeys;
use std::collections::Bound;
use std::fmt::Display;
use std::fs::File;
use std::hash::{DefaultHasher, Hasher};
use std::i32;
use std::io::{Error, Read};
use std::ops::*;

use chrono::{DateTime, Utc};
use log::error;
use num_traits::abs;
use regex::Regex;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::byte_code_compiler::ByteCodeCompiler;
use crate::byte_row_collection::ByteRowCollection;
use crate::cnv_error;
use crate::columns::Column;
use crate::data_types::DataType::*;

use crate::data_types::*;
use crate::dataframe::Dataframe;
use crate::dataframe::Dataframe::{Disk, EventSource, Model, TableFn};

use crate::errors::Errors::{CannotSubtract, Exact, IncompatibleParameters, Multiple, SyntaxError, TypeMismatch};
use crate::errors::TypeMismatchErrors::{ArgumentsMismatched, CannotBeNegated, StructsOneOrMoreExpected, UnsupportedType};
use crate::errors::{throw, Errors, SyntaxErrors, TypeMismatchErrors};
use crate::expression::Expression;
use crate::field::FieldMetadata;
use crate::file_row_collection::FileRowCollection;
use crate::journaling::{EventSourceRowCollection, TableFunction};
use crate::machine::Machine;
use crate::model_row_collection::ModelRowCollection;
use crate::namespaces::Namespace;
use crate::number_kind::NumberKind;
use crate::number_kind::NumberKind::*;
use crate::numbers::Numbers;
use crate::numbers::Numbers::*;
use crate::object_config::ObjectConfig;
use crate::packages::OxidePkg::UUID;
use crate::parameter::Parameter;
use crate::platform::PackageOps;
use crate::row_collection::RowCollection;
use crate::row_metadata::RowMetadata;
use crate::sequences::*;
use crate::structures::Row;
use crate::structures::Structures::{Firm, Hard, Soft};
use crate::structures::*;
use crate::tokens::Token;
use crate::typed_values::TypedValue::*;

const ISO_DATE_FORMAT: &str =
    r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?(([+-]\d\d:\d\d)|Z)?$";
const DECIMAL_FORMAT: &str = r"^-?(?:\d+(?:_\d)*|\d+)(?:\.\d+)?$";
const INTEGER_FORMAT: &str = r"^-?(?:\d+(?:_\d)*)?$";
const UUID_FORMAT: &str =
    "^[0-9a-fA-F]{8}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{12}$";

/// Basic value unit
#[derive(Clone, Debug, Eq, Ord, PartialEq, Serialize, Deserialize)]
pub enum TypedValue {
    ArrayValue(Array),
    BinaryValue(Vec<u8>),
    Boolean(bool),
    CharValue(char),
    DateValue(i64),
    ErrorValue(Errors),
    Function { params: Vec<Parameter>, body: Box<Expression>, returns: DataType },
    Kind(DataType),
    Null,
    Number(Numbers),
    PlatformOp(PackageOps),
    Sequenced(Sequences),
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
        match j_value {
            serde_json::Value::Null => Null,
            serde_json::Value::Bool(b) => Boolean(*b),
            serde_json::Value::Number(n) => n.as_f64().map(|v| Number(F64Value(v))).unwrap_or(Null),
            serde_json::Value::String(s) => StringValue(s.to_owned()),
            serde_json::Value::Array(a) => ArrayValue(Array::from(a.iter().map(Self::from_json).collect())),
            serde_json::Value::Object(args) => Structured(Hard(
                HardStructure::from_parameters(args.iter()
                    .map(|(name, value)| {
                        let tv = TypedValue::from_json(value);
                        Parameter::new_with_default(name, tv.get_type().clone(), tv)
                    }).collect::<Vec<_>>())
            ))
        }
    }

    pub fn from_numeric(text: &str) -> std::io::Result<TypedValue> {
        let decimal_regex = Regex::new(DECIMAL_FORMAT).map_err(|e| cnv_error!(e))?;
        let int_regex = Regex::new(INTEGER_FORMAT).map_err(|e| cnv_error!(e))?;
        let number: String = text.chars()
            .filter(|c| *c != '_' && *c != ',')
            .collect();
        
        fn convert_lex_num(num_str: &str, radix: u32) -> std::io::Result<TypedValue> {
            let number = u128::from_str_radix(&num_str[2..], radix)
                .map_err(|e| cnv_error!(e))?;
            let result = match number { 
                n if n <= i64::MAX as u128 => I64Value(n as i64),
                n => U128Value(n)
            };
            Ok(Number(result))
        }
        
        match number.trim() {
            s if s.starts_with("0b") => convert_lex_num(s, 2),
            s if s.starts_with("0o") => convert_lex_num(s, 8),
            s if s.starts_with("0x") => convert_lex_num(s, 16),
            s if int_regex.is_match(s) => Ok(Number(I64Value(s.parse().map_err(|e| cnv_error!(e))?))),
            s if decimal_regex.is_match(s) => Ok(Number(F64Value(s.parse().map_err(|e| cnv_error!(e))?))),
            s => Ok(StringValue(s.to_string()))
        }
    }

    pub fn from_result(result: std::io::Result<TypedValue>) -> TypedValue {
        result.unwrap_or_else(|err| ErrorValue(Exact(err.to_string())))
    }

    pub fn from_results(result: std::io::Result<(Machine, TypedValue)>) -> (Machine, TypedValue) {
        result.unwrap_or_else(|err| (Machine::empty(), ErrorValue(Exact(err.to_string()))))
    }

    pub fn is_numeric_value(value: &str) -> std::io::Result<bool> {
        let decimal_regex = Regex::new(DECIMAL_FORMAT)
            .map_err(|e| cnv_error!(e))?;
        Ok(decimal_regex.is_match(value))
    }

    pub fn millis_to_iso_date(millis: i64) -> Option<String> {
        let seconds = millis / 1000;
        let nanoseconds = (millis % 1000) * 1_000_000;
        let datetime = DateTime::from_timestamp(seconds, nanoseconds as u32)?;
        let iso_date = datetime.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
        Some(iso_date)
    }
    
    /// Normalizes the value as [Sequenced] or [TableValue] if possible.
    pub fn normalize(&self) -> TypedValue {
        match self.clone() {
            ArrayValue(array) => Sequenced(Sequences::TheArray(array)),
            Structured(s) => Structured(s),
            TableValue(df) => TableValue(df),
            TupleValue(tuple) => Sequenced(Sequences::TheTuple(tuple)),
            other => other
        }
    }

    pub fn parse_two_args(
        args: Vec<TypedValue>
    ) -> std::io::Result<(TypedValue, TypedValue)> {
        match args.as_slice() {
            [a, b] => Ok((a.clone(), b.clone())),
            z => throw(TypeMismatch(ArgumentsMismatched(2, z.len())))
        }
    }

    pub fn parse_three_args(
        args: Vec<TypedValue>
    ) -> std::io::Result<(TypedValue, TypedValue, TypedValue)> {
        match args.as_slice() {
            [a, b, c] => Ok((a.clone(), b.clone(), c.clone())),
            z => throw(TypeMismatch(ArgumentsMismatched(3, z.len())))
        }
    }

    pub fn wrap_value(raw_value: &str) -> std::io::Result<Self> {
        let iso_date_regex = Regex::new(ISO_DATE_FORMAT).map_err(|e| cnv_error!(e))?;
        let uuid_regex = Regex::new(UUID_FORMAT).map_err(|e| cnv_error!(e))?;
        fn is_quoted(s: &str) -> bool {
            (s.starts_with("\"") && s.ends_with("\"")) ||
                (s.starts_with("'") && s.ends_with("'"))
        }
        let result = match raw_value.trim() {
            "" => Null,
            "false" => Boolean(false),
            "null" => Null,
            "true" => Boolean(true),
            "undefined" => Undefined,
            s if Self::is_numeric_value(s)? => Self::from_numeric(s)?,
            s if iso_date_regex.is_match(s) =>
                DateValue(DateTime::parse_from_rfc3339(s)
                    .map_err(|e| cnv_error!(e))?.timestamp_millis()),
            s if uuid_regex.is_match(s) => Number(U128Value(ByteCodeCompiler::decode_uuid(s)?)),
            s if is_quoted(s) => StringValue(s[1..s.len() - 1].to_string()),
            s => return throw(SyntaxError(SyntaxErrors::LiteralExpected(s.to_string()))),
        };
        Ok(result)
    }

    pub fn wrap_value_opt(opt_value: &Option<String>) -> std::io::Result<Self> {
        match opt_value {
            Some(value) => Self::wrap_value(value),
            None => Ok(Null)
        }
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
            (Number(a), Number(b)) => a.to_f64() == b.to_f64(),
            (Sequenced(items), item) => items.contains(item),
            (StringValue(a), StringValue(b)) => a.contains(b),
            (Structured(s), StringValue(name)) => s.contains(name),
            (TableValue(Model(mrc)), Structured(s)) => mrc.contains(&Structures::transform_row(
                &s.get_parameters(),
                &s.get_values(),
                &mrc.get_parameters()
            )),
            _ => false
        }
    }

    pub fn convert_to(&self, dest_type: DataType) -> std::io::Result<TypedValue> {
        Ok(match self { 
            ErrorValue(err) => ErrorValue(err.clone()),
            Null => Null,
            Undefined => Undefined,
            value =>
                match dest_type {
                    ArrayType(..) => match value {
                        ArrayValue(array) => ArrayValue(array.to_owned()),
                        StringValue(s) => ArrayValue(Array::from(
                            s.chars().map(|c| StringValue(c.to_string())).collect::<Vec<_>>()
                        )),
                        Structured(s) => ArrayValue(s.to_array()),
                        TableValue(df) => ArrayValue(df.to_array()),
                        TupleValue(items) => ArrayValue(Array::from(items.to_owned())),
                        UUIDValue(uuid) => ArrayValue(Array::from(uuid.to_be_bytes()
                            .iter().map(|b| Number(I64Value(*b as i64)))
                            .collect::<Vec<_>>())),
                        _ => Undefined,
                    }
                    BinaryType => match value {
                        BinaryValue(bytes) => BinaryValue(bytes.to_owned()),
                        Number(number) => BinaryValue(number.encode()),
                        StringValue(s) => BinaryValue(s.as_bytes().to_owned()),
                        TableValue(df) => BinaryValue(df.to_bytes()),
                        UUIDValue(uuid) => BinaryValue(uuid.to_be_bytes().to_vec()),
                        _ => Undefined
                    },
                    BooleanType => match value {
                        Boolean(b) => Boolean(*b),
                        Number(n) => Boolean(n.to_f64() != 0.),
                        StringValue(s) => Boolean(s == "true"),
                        _ => Undefined
                    },
                    DataType::CharType => match value {
                        CharValue(c) => CharValue(*c),
                        other => other.to_char().map(|c| CharValue(c)).unwrap_or(Undefined),
                    }
                    DateTimeType => match value {
                        DateValue(dt) => DateValue(*dt),
                        Number(n) => DateValue(n.to_i64()),
                        StringValue(s) => {
                            let datetime: DateTime<Utc> = s.parse().map_err(|e| cnv_error!(e))?;
                            DateValue(datetime.timestamp_millis())
                        },
                        _ => Undefined
                    }
                    EnumType(params) => match value {
                        StringValue(text) =>
                            match params.iter().position(|p| p.get_name() == text) {
                                Some(index) => Number(I64Value(index as i64)),
                                None => Null
                            },
                        _ => Undefined,
                    }
                    ErrorType => ErrorValue(Exact(value.unwrap_value())),
                    FixedSizeType(underlying_type, max_len) => {
                        value.convert_to(*underlying_type)?.sublist(0, max_len)
                    }
                    FunctionType(my_params, my_returns) =>
                        match value {
                            ErrorValue(err) => ErrorValue(err.clone()),
                            Function { body, .. } => {
                                Function {
                                    params: my_params,
                                    body: body.clone(),
                                    returns: my_returns.deref().clone(),
                                }
                            }
                            other => ErrorValue(TypeMismatch(TypeMismatchErrors::FunctionExpected(other.to_code())))
                        }
                    NumberType(kind) => {
                        let result = match value {
                            Boolean(b) => I64Value(if *b { 1 } else { 0 }),
                            DateValue(dt) => I64Value(*dt),
                            Number(number) => *number,
                            UUIDValue(uuid) => U128Value(*uuid).convert_to(kind),
                            _ => NaNValue,
                        };
                        Number(result.convert_to(kind))
                    }
                    PlatformOpsType(kind) => match value {
                        PlatformOp(..) => PlatformOp(kind.clone()),
                        _ => Undefined
                    }
                    StringType => StringValue(value.unwrap_value()),
                    StructureType(..) => match value {
                        Structured(s) => Structured(s.clone()),
                        _ => Undefined
                    }
                    TableType(params) => match value {
                        ArrayValue(..) => value.to_table_with_schema(&params)?,
                        Structured(..) => value.to_table_with_schema(&params)?,
                        TableValue(..) => value.to_table_with_schema(&params)?,
                        _ => Undefined
                    }
                    TupleType(..) => match value {
                        ArrayValue(array) => TupleValue(array.get_values()),
                        Structured(s) => TupleValue(s.get_values()),
                        TupleValue(items) => TupleValue(items.to_owned()),
                        _ => Undefined
                    }
                    DataType::UnresolvedType => value.clone(),
                    DataType::UUIDType => match value {
                        BinaryValue(bytes) => UUIDValue(Uuid::from_slice(&bytes).map_err(|e| cnv_error!(e))?.as_u128()),
                        Number(number) => UUIDValue(number.to_u128()),
                        StringValue(text) => UUIDValue(Uuid::parse_str(text.as_str()).map_err(|e| cnv_error!(e))?.as_u128()),
                        UUIDValue(uuid) => UUIDValue(*uuid),
                        _ => Undefined
                    }
                }
        })
    }

    fn u128_to_uuid(uuid: u128) -> String {
        // extract each group using bit shifts and masks
        let time_low = (uuid >> 96) as u32;
        let time_mid = (uuid >> 80) as u16;
        let time_hi_and_version = (uuid >> 64) as u16;
        let clk_seq = (uuid >> 48) as u16;
        let node = uuid as u64 & 0xFFFFFFFFFFFF;
        // format into a UUID string
        format!(
            "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
            time_low, time_mid, time_hi_and_version, clk_seq, node
        )
    }

    pub fn encode(&self) -> Vec<u8> {
        match self {
            ArrayValue(items) => {
                let mut bytes = Vec::new();
                bytes.extend(items.len().to_be_bytes());
                for item in items.iter() { bytes.extend(item.encode()); }
                bytes
            }
            BinaryValue(bytes) => ByteCodeCompiler::encode_u8x_n(bytes.to_vec()),
            Boolean(ok) => vec![if *ok { 1 } else { 0 }],
            DateValue(dt) => ByteCodeCompiler::encode_u8x_n(dt.to_be_bytes().to_vec()),
            ErrorValue(err) => ByteCodeCompiler::encode_string(err.to_string().as_str()),
            Number(number) => number.encode(),
            PlatformOp(pf) => pf.encode().unwrap_or(vec![]),
            StringValue(string) => ByteCodeCompiler::encode_string(string),
            TableValue(df) => ByteCodeCompiler::encode_df(df),
            TupleValue(items) => {
                let mut bytes = Vec::new();
                bytes.extend(items.len().to_be_bytes());
                for item in items.iter() { bytes.extend(item.encode()); }
                bytes
            }
            UUIDValue(uuid) => uuid.to_be_bytes().to_vec(),
            other => ByteCodeCompiler::encode_value(other).unwrap_or_else(|err| {
                error!("decode error: {err}");
                Vec::new()
            })
        }
    }

    pub fn get_type(&self) -> DataType {
        match self {
            ArrayValue(a) => FixedSizeType(ArrayType(a.get_component_type().into()).into(), a.len()),
            BinaryValue(bytes) => FixedSizeType(BinaryType.into(), bytes.len()),
            Boolean(..) => BooleanType,
            CharValue(..) => CharType,
            DateValue(..) => DateTimeType,
            ErrorValue(..) => ErrorType,
            Function { params, returns, .. } =>
                FunctionType(params.clone(), Box::from(returns.clone())),
            Kind(data_type) => data_type.clone(),
            Null | Undefined => UnresolvedType,
            Number(n) => NumberType(n.kind()),
            PlatformOp(op) => op.get_type(),
            Sequenced(seq) => seq.get_type(),
            Structured(s) => StructureType(s.get_parameters()),
            StringValue(s) => FixedSizeType(StringType.into(), s.len()),
            TableValue(tv) => TableType(Parameter::from_columns(tv.get_columns())),
            TupleValue(items) => TupleType(items.iter()
                .map(|i| i.get_type())
                .collect()),
            UUIDValue(..) => UUIDType,
        }
    }

    pub fn get_type_name(&self) -> String {
        self.get_type().to_code()
    }

    pub fn hash_code(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        hasher.write(&self.encode());
        hasher.finish()
    }

    pub fn is_ok(&self) -> bool {
        matches!(self, Boolean(true) | Number(..) | TableValue(..))
    }

    pub fn is_true(&self) -> bool {
        matches!(self, Boolean(true))
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
            BinaryValue(bytes) => BinaryValue(bytes[start..end].to_vec()),
            Sequenced(s) => Sequenced(s.sublist(start, end)),
            StringValue(s) => StringValue(s[start..end].to_string()),
            TableValue(df) => TableValue(df.sublist(start, end)),
            other => other.to_owned()
        }
    }

    pub fn to_array(&self) -> TypedValue {
        match self {
            ArrayValue(items) => ArrayValue(items.to_owned()),
            ErrorValue(err) => ErrorValue(err.to_owned()),
            StringValue(s) => ArrayValue(Array::from(s.chars()
                .map(|c| StringValue(c.to_string())).collect())),
            Structured(Hard(hs)) => ArrayValue(hs.to_table().to_array()),
            Structured(Soft(ss)) => ArrayValue(ss.to_table().to_array()),
            TableValue(rc) => ArrayValue(rc.to_array()),
            TupleValue(items) => ArrayValue(Array::from(items.clone())),
            z => ErrorValue(TypeMismatch(UnsupportedType(TableType(vec![]), z.get_type())))
        }
    }

    pub fn to_bool(&self) -> bool {
        match self {
            Boolean(b) => *b,
            _ => false
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            TypedValue::BinaryValue(bytes) => bytes.to_vec(),
            TypedValue::StringValue(s) => s.bytes().collect(),
            TypedValue::UUIDValue(uuid) => u128::to_be_bytes(*uuid).to_vec(),
            z => z.encode()
        }
    }

    pub fn to_code(&self) -> String {
        match self {
            ArrayValue(items) =>
                format!("[{}]", items.iter()
                    .map(|v| v.to_code())
                    .collect::<Vec<_>>().join(", ")),
            CharValue(c) => format!("'{c}'"),
            PlatformOp(pf) => pf.to_code(),
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
            StringValue(s) => s.chars().next(),
            _ => None
        }
    }

    pub fn to_dataframe(&self) -> std::io::Result<Dataframe> {
        match self {
            ArrayValue(items) => self.convert_array_to_table(&items.get_values()),
            Structured(s) => Ok(s.to_dataframe()),
            TableValue(df) => Ok(df.to_owned()),
            z => throw(TypeMismatch(TypeMismatchErrors::TableExpected(z.to_code())))
        }
    }

    pub fn to_f64(&self) -> f64 {
        match self {
            Boolean(true) => 1.,
            Number(nv) => nv.to_f64(),
            UUIDValue(uuid) => *uuid as f64,
            _ => 0.
        }
    }
    
    pub fn to_i64(&self) -> i64 {
        match self {
            Boolean(true) => 1,
            Number(nv) => nv.to_i64(),
            UUIDValue(uuid) => *uuid as i64,
            _ => 0
        }
    }

    pub fn to_i128(&self) -> i128 {
        match self {
            Boolean(true) => 1,
            Number(nv) => nv.to_i128(),
            UUIDValue(uuid) => *uuid as i128,
            _ => 0
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Self::ArrayValue(items) => serde_json::json!(items.iter()
                    .map(|v|v.to_json())
                    .collect::<Vec<_>>()),
            Self::BinaryValue(bytes) => serde_json::json!(bytes),
            Self::CharValue(c) => serde_json::json!(c),
            Self::Boolean(b) => serde_json::json!(b),
            Self::DateValue(dt) => serde_json::json!(Self::millis_to_iso_date(*dt).unwrap_or_else(|| dt.to_string())),
            Self::ErrorValue(message) => serde_json::json!(message),
            Self::Function { params, body: code, returns } => {
                let my_params = serde_json::Value::Array(params.iter()
                    .map(|c| c.to_json()).collect());
                serde_json::json!({ "params": my_params, "code": code.to_code(), "returns": returns.to_type_declaration() })
            }
            Self::Kind(data_type) => serde_json::json!(data_type.to_code()),
            Self::Null => serde_json::Value::Null,
            Self::Number(nv) => nv.to_json(),
            Self::PlatformOp(nf) => serde_json::json!(nf),
            Self::Sequenced(seq) => serde_json::json!(seq),
            Self::StringValue(s) => serde_json::json!(s),
            Self::Structured(s) => s.to_json(),
            Self::TableValue(df) => {
                let parameters = df.get_parameters();
                let rows = df.iter()
                    .map(|r| r.to_hash_json_value(&parameters))
                    .collect::<Vec<_>>();
                serde_json::json!(rows)
            }
            Self::TupleValue(items) => serde_json::json!(
                items.iter()
                    .map(|v|v.to_json())
                    .collect::<Vec<_>>()),
            Self::Undefined => serde_json::Value::Null,
            Self::UUIDValue(uuid) => serde_json::json!(Self::u128_to_uuid(*uuid)),
        }
    }

    pub fn to_result<A>(&self, f: fn(&TypedValue) -> A) -> std::io::Result<A> {
        match self {
            ErrorValue(err) => throw(Exact(err.to_string())),
            other => Ok(f(other))
        }
    }

    pub fn to_sequence(&self) -> std::io::Result<Sequences> {
        match self {
            ArrayValue(array) => Ok(Sequences::TheArray(array.clone())),
            StringValue(s) => Ok(Sequences::TheArray(Array::from(s.chars()
                .map(|c| StringValue(c.to_string()))
                .collect::<Vec<_>>()
            ))),
            TableValue(df) => Ok(Sequences::TheDataframe(df.clone())),
            TupleValue(t) => Ok(Sequences::TheTuple(t.to_vec())),
            z => throw(TypeMismatch(UnsupportedType(TableType(vec![]), z.get_type())))
        }
    }

    pub fn to_table(&self) -> std::io::Result<Box<dyn RowCollection>> {
        Ok(Box::new(self.to_dataframe()?))
    }

    pub fn to_table_or_value(&self) -> TypedValue {
        match self.to_dataframe() {
            Ok(df) => TableValue(df),
            Err(_) => self.clone()
        }
    }

    pub fn to_table_with_schema(&self, params: &Vec<Parameter>) -> std::io::Result<TypedValue> {
        let df = self.to_dataframe()?;
        let my_params = df.get_parameters();
        match Parameter::are_compatible(params, &my_params) {
            true => Ok(TableValue(df)),
            false => throw(IncompatibleParameters(my_params))
        }
    }

    fn convert_array_to_table(&self, items: &Vec<TypedValue>) -> std::io::Result<Dataframe> {
        // gather each struct in the array as a table
        fn value_parameters(value: &TypedValue) -> Vec<Parameter> {
            vec![Parameter::new("value", value.get_type().clone())]
        }
        let dataframes = items.iter()
            .fold(Vec::new(), |mut tables, tv| match tv {
                Structured(ss) => {
                    tables.push(Model(ss.to_table()));
                    tables
                }
                TableValue(Model(mrc)) => {
                    tables.push(Model(mrc.to_owned()));
                    tables
                }
                z => {
                    let mut mrc = ModelRowCollection::from_parameters(&value_parameters(z));
                    mrc.append_row(Row::new(0, vec![z.clone()]));
                    tables.push(Model(mrc));
                    tables
                }
            });

        // process the dataframes
        match dataframes.as_slice() {
            [] => throw(TypeMismatch(StructsOneOrMoreExpected)),
            [df] => Ok(df.to_owned()),
            dfs => Ok(Dataframe::combine_tables(dfs.to_vec()))
        }
    }

    pub fn to_u8(&self) -> u8 {
        match self {
            Boolean(true) => 1,
            Number(n) => n.to_u8(),
            UUIDValue(uuid) => *uuid as u8,
            _ => 0
        }
    }

    pub fn to_u16(&self) -> u16 {
        match self {
            Boolean(true) => 1,
            Number(n) => n.to_u16(),
            UUIDValue(uuid) => *uuid as u16,
            _ => 0
        }
    }

    pub fn to_u32(&self) -> u32 {
        match self {
            Boolean(true) => 1,
            Number(n) => n.to_u32(),
            UUIDValue(uuid) => *uuid as u32,
            _ => 0
        }
    }

    pub fn to_u64(&self) -> u64 {
        match self {
            Boolean(true) => 1,
            Number(n) => n.to_u64(),
            UUIDValue(uuid) => *uuid as u64,
            _ => 0
        }
    }

    pub fn to_u128(&self) -> u128 {
        match self {
            Boolean(true) => 1,
            Number(n) => n.to_u128(),
            UUIDValue(uuid) => *uuid,
            _ => 0
        }
    }

    pub fn to_usize(&self) -> usize {
        match self {
            Boolean(true) => 1,
            Number(n) => n.to_usize(),
            _ => 0
        }
    }

    fn unwrap_as_bytes(&self, max_len: usize) -> std::io::Result<Vec<u8>> {
        let bytes = match self {
            ArrayValue(_) => vec![],
            BinaryValue(bytes) => bytes.clone(),
            Boolean(b) => vec![if *b { 1u8 } else { 0u8 }],
            CharValue(c) => {
                let mut buf = [0; 4]; // max UTF-8 length for char is 4 bytes
                let encoded = c.encode_utf8(&mut buf);
                encoded.as_bytes().to_vec()
            }
            DateValue(dt) => i64::from(*dt).to_be_bytes().to_vec(),
            ErrorValue(err) => return throw(err.to_owned()),
            Function { .. } => vec![],
            Kind(data_type) => return throw(Exact(format!("{} cannot be converted to Binary", data_type.to_code()))),
            Null => vec![],
            Number(_) => vec![],
            PlatformOp(_) => vec![],
            Sequenced(_) => vec![],
            StringValue(s) => s.as_bytes().to_vec(),
            Structured(_) => vec![],
            TableValue(_) => vec![],
            TupleValue(_) => vec![],
            TypedValue::Undefined => vec![],
            UUIDValue(uuid) => u128::to_be_bytes(*uuid).to_vec(), 
        };
        Ok(match max_len {
            n if n < 1 || n > bytes.len() => bytes,
            n => (&bytes[..n]).to_vec()
        })
    }

    /// converts the value to a [String] with a maximum length
    fn unwrap_as_string(&self, max_len: usize) -> String {
        let s = self.unwrap_value();
        match max_len {
            n if n < 1 || n > s.len() => s,
            n => (&s[..n]).to_string()
        }
    }

    pub fn unwrap_value(&self) -> String {
        fn quoted(items: &Vec<TypedValue>) -> Vec<String> {
            items.iter()
                .map(|value| match value {
                    StringValue(s) => format!(r#""{s}""#),
                    //UUIDValue(n) => format!(r#""{}""#, TypedValue::u128_to_uuid(*n)),
                    v => v.unwrap_value()
                })
                .collect::<Vec<_>>()
        }
        
        match self {
            Self::ArrayValue(array) => 
                format!("[{}]", quoted(&array.get_values()).join(", ")),
            Self::BinaryValue(bytes) =>
                format!("0v{}", bytes.iter().map(|b| format!("{:02x}", b))
                    .collect::<Vec<_>>().join("")),
            Self::Boolean(b) => (if *b { "true" } else { "false" }).into(),
            Self::CharValue(c) => c.to_string(),
            Self::DateValue(dt) => Self::millis_to_iso_date(*dt).unwrap_or_else(|| dt.to_string()),
            Self::ErrorValue(message) => message.to_string(),
            Self::Function { params, body: code, returns } =>
                format!("(fn({}){} => {})",
                        params.iter().map(|c| c.to_code()).collect::<Vec<_>>().join(", "),
                        match returns.to_code().as_str() {
                            "" => "".to_string(),
                            s => format!(": {}", s),
                        },
                        code.to_code()),
            Self::Kind(data_type) => data_type.to_code(),
            Self::Null => "null".into(),
            Self::Number(number) => number.unwrap_value(),
            Self::PlatformOp(nf) => nf.to_code(),
            Self::Sequenced(seq) => seq.unwrap_value(),
            Self::StringValue(string) => string.into(),
            Self::Structured(structure) => structure.to_json().to_string(),
            Self::TableValue(rcv) => {
                let params = rcv.get_parameters();
                serde_json::json!(rcv.iter().map(|r| r.to_hash_json_value(&params))
                    .collect::<Vec<_>>()).to_string()
            }
            Self::TupleValue(items) => format!("({})", quoted(items).join(", ")),
            Self::Undefined => "undefined".into(),
            Self::UUIDValue(uuid) => Self::u128_to_uuid(*uuid),
        }
    }

    ///////////////////////////////////////////////////////////////
    //      CONDITIONAL OPERATIONS
    ///////////////////////////////////////////////////////////////

    pub fn and(&self, rhs: &TypedValue) -> Option<TypedValue> {
        Some(Boolean(self.to_bool() && rhs.to_bool()))
    }

    pub fn ne(&self, rhs: &Self) -> Option<Self> {
        Some(Boolean(self.to_f64() != rhs.to_f64()))
    }

    pub fn not(&self) -> Option<Self> {
        Some(Boolean(!self.to_bool()))
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
            (a, b) => {
                a.to_char().and_then(|aa| b.to_char()
                    .and_then(|bb| Some((aa, bb)))) 
                    .iter().for_each(|(aa, bb)| {
                        for c in *aa..(*bb) {
                            values.push(StringValue(c.to_string()))
                        }
                    })
            }
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
            (a, b) => {
                a.to_char().and_then(|aa| b.to_char()
                    .and_then(|bb| Some((aa, bb))))
                    .iter().for_each(|(aa, bb)| {
                    for c in *aa..=(*bb) {
                        values.push(StringValue(c.to_string()))
                    }
                })
            }
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
            (Number(a), Number(b)) => Number(a + b),
            (StringValue(a), StringValue(b)) => StringValue(a + b.as_str()),
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
                    ErrorValue(s) => ErrorValue(s),
                    _ => TableValue(Model(mrc)),
                }
            }
            (TableValue(a), TableValue(b)) =>
                match ModelRowCollection::combine(a.get_columns().to_owned(), vec![&a, &b]) {
                    Ok(mrc) => TableValue(Model(mrc)),
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
            (Number(a), Number(b)) => Number(a & b),
            (TableValue(a), TableValue(b)) => TableValue(a.intersect(&b)),
            (TupleValue(a), TupleValue(b)) => TupleValue(bitand_vec(a, b)),
            _ => Undefined
        }
    }
}

impl BitOr for TypedValue {
    type Output = TypedValue;

    fn bitor(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
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
            (Number(..), Number(b)) if b.is_effectively_zero() => Number(NaNValue),
            (Number(a), Number(b)) => Number(a / b),
            (TupleValue(a), TupleValue(b)) => TupleValue(div_vec(a, b)),
            _ => Undefined
        }
    }
}

impl Index<usize> for TypedValue {
    type Output = TypedValue;

    fn index(&self, index: usize) -> &Self::Output {
        match self {
            ArrayValue(items) =>
                Box::leak(Box::new(idx_vec(&items.get_values(), index))),
            Null => &Null,
            TableValue(rcv) => {
                let rc: Box<dyn RowCollection> = Box::from(rcv.to_owned());
                Box::leak(Box::new(fetch(&rc, index)))
            }
            TupleValue(items) =>
                Box::leak(Box::new(idx_vec(items, index))),
            _ => &Undefined,
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
            BinaryValue(..) => error("Binary".into()),
            Boolean(n) => Boolean(!n),
            CharValue(c) => CharValue(c),
            DateValue(dt) => Number(I64Value(dt.neg())),
            ErrorValue(msg) => ErrorValue(msg),
            Function { .. } => error("Function".into()),
            Kind(data_type) => error(data_type.to_code()),
            Null => Null,
            Number(nv) => Number(nv.neg()),
            PlatformOp(..) => error("PlatformFunction".into()),
            Sequenced(seq) => Sequenced(seq),
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
            (BinaryValue(a), BinaryValue(b)) => a.partial_cmp(b),
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
            (Number(a), Number(b)) => Number(a - b),
            (TupleValue(a), TupleValue(b)) => TupleValue(sub_vec(a, b)),
            (a, b) => ErrorValue(CannotSubtract(a.to_code(), b.to_code()))
        }
    }
}

/// Unit tests
#[cfg(test)]
mod core_tests {
    use crate::testdata::{make_quote, make_quote_columns, make_quote_parameters};
    use serde_json::{json, Value};

    use super::*;

    #[test]
    fn test_express_range() {
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
    fn test_decode() {
        let buf: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 5, b'H', b'e', b'l', b'l', b'o'];
        assert_eq!(FixedSizeType(StringType.into(), 5).decode(&buf, 0), StringValue("Hello".into()))
    }

    #[test]
    fn test_addition() {
        use Numbers::*;
        assert_eq!(Number(F64Value(45.0)) + Number(F64Value(32.7)), Number(F64Value(77.7)));
        assert_eq!(Number(I128Value(45)) + Number(I128Value(32)), Number(I128Value(77)));
        assert_eq!(Number(I64Value(45)) + Number(I64Value(32)), Number(I64Value(77)));
        assert_eq!(Number(U128Value(45)) + Number(U128Value(32)), Number(U128Value(77)));
        assert_eq!(Boolean(true) + Boolean(true), Boolean(true));
        assert_eq!(StringValue("Hello".into()) + StringValue(" World".into()), StringValue("Hello World".into()));
    }

    #[test]
    fn test_subtraction() {
        use Numbers::*;
        assert_eq!(Number(F64Value(45.0)) - Number(F64Value(32.5)), Number(F64Value(12.5)));
        assert_eq!(Number(I128Value(45)) - Number(I128Value(32)), Number(I128Value(13)));
        assert_eq!(Number(I64Value(45)) - Number(I64Value(32)), Number(I64Value(13)));
        assert_eq!(Number(U128Value(45)) - Number(U128Value(32)), Number(U128Value(13)));
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
        let table = TableValue(Model(ModelRowCollection::from_columns_and_rows(&phys_columns, &vec![
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
        let table = TableValue(Model(ModelRowCollection::from_columns_and_rows(&phys_columns, &vec![
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
    fn test_table_index() {
        let params = make_quote_parameters();
        let mrc = ModelRowCollection::from_parameters_and_rows(&params, &vec![
            make_quote(0, "ABC", "AMEX", 11.77),
            make_quote(1, "UNO", "OTC", 0.2456),
            make_quote(2, "BIZ", "NYSE", 23.66),
            make_quote(3, "GOTO", "OTC", 0.1428),
            make_quote(4, "BOOM", "NASDAQ", 56.87)]);
        assert_eq!(
            TableValue(Model(mrc))[0],
            Structured(Firm(Row::new(
                0, vec![
                    StringValue("ABC".into()),
                    StringValue("AMEX".into()),
                    Number(F64Value(11.77)),
                ],
            ), params)))
    }

    #[test]
    fn test_eq() {
        use Numbers::*;
        assert_eq!(F64Value(45.0), F64Value(45.0));
        assert_eq!(I64Value(0x5555_FACE_CAFE_BABE), I64Value(0x5555_FACE_CAFE_BABE));
        assert_eq!(I128Value(0x5555_FACE_CAFE_BABE), I128Value(0x5555_FACE_CAFE_BABE));
        assert_eq!(I64Value(0x5555_FACE_CAFE_BABE), I64Value(0x5555_FACE_CAFE_BABE));
        assert_eq!(U128Value(0x5555_FACE_CAFE_BABE), U128Value(0x5555_FACE_CAFE_BABE));
    }

    #[test]
    fn test_ne() {
        use Numbers::*;
        assert_ne!(I64Value(0x5555_FACE_CAFE_BABE), I64Value(0xFACE_CAFE_BABE));
        assert_ne!(F64Value(99.142857), F64Value(19.48));
    }

    #[test]
    fn test_gt() {
        use Numbers::*;
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
        assert_eq!(v0.to_i64(), 12);
        assert_eq!(v0.to_json(), json!(12.56));
        assert_eq!(v0.to_u64(), 12);
        assert_eq!(v0.to_usize(), 12);
        assert_eq!(v0.unwrap_value(), "12.56".to_string());
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
    fn test_outcomes() {
        verify_encode_decode(&Number(Numbers::RowId(1013)));
        verify_encode_decode(&Number(Numbers::I64Value(19)));
    }

    #[test]
    fn test_to_json() {
        use Numbers::*;
        verify_to_json(Boolean(false), serde_json::Value::Bool(false));
        verify_to_json(Boolean(true), serde_json::Value::Bool(true));
        verify_to_json(DateValue(1709163679081), serde_json::Value::String("2024-02-28T23:41:19.081Z".into()));
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
    fn test_to_string() {
        assert_eq!(format!("{}", StringValue("Hello".into())), "Hello");
    }

    #[test]
    fn test_wrap_and_unwrap_value() {
        verify_wrap_unwrap("false", Boolean(false));
        verify_wrap_unwrap("true", Boolean(true));
        verify_wrap_unwrap("2024-02-28T23:41:19.081Z", DateValue(1709163679081));
        verify_wrap_unwrap("100.1", Number(F64Value(100.1)));
        verify_wrap_unwrap("100", Number(I64Value(100)));
        verify_wrap_unwrap("null", Null);
        verify_wrap_unwrap("\"Hello World\"", StringValue("Hello World".into()));
        verify_wrap_unwrap("undefined", Undefined);
    }

    #[test]
    fn test_unwrap_optional_value() {
        assert_eq!(
            TypedValue::wrap_value_opt(&Some("123.45".into())).unwrap(),
            Number(F64Value(123.45))
        )
    }

    fn verify_encode_decode(expected: &TypedValue) {
        let data_type = expected.get_type();
        let bytes = expected.encode();
        let actual = data_type.decode(&bytes, 0);
        assert_eq!(actual, *expected)
    }

    fn verify_to_json(typed_value: TypedValue, expected: serde_json::Value) {
        assert_eq!(typed_value.to_json(), expected)
    }

    fn verify_wrap_unwrap(string_value: &str, typed_value: TypedValue) {
        assert_eq!(typed_value.to_code(), string_value);
        match TypedValue::wrap_value(string_value) {
            Ok(value) => assert_eq!(value, typed_value),
            Err(message) => panic!("{}", message)
        };
    }
}

/// conversion tests
#[cfg(test)]
mod conversion_tests {
    use crate::data_types::DataType;
    use crate::data_types::DataType::*;
    use crate::dataframe::Dataframe;
    use crate::errors::Errors::Exact;
    use crate::model_row_collection::ModelRowCollection;
    use crate::number_kind::NumberKind::*;
    use crate::numbers::Numbers::*;
    use crate::parameter::Parameter;
    use crate::sequences::Array;
    use crate::structures::Structures::Hard;
    use crate::structures::{HardStructure, Row};
    use crate::typed_values::TypedValue;
    use crate::typed_values::TypedValue::*;

    #[test]
    fn test_array_to_table() {
        let params = vec![
            Parameter::new_with_default("symbol", FixedSizeType(StringType.into(), 3), StringValue("BIZ".into())),
            Parameter::new_with_default("exchange", FixedSizeType(StringType.into(), 4), StringValue("NYSE".into())),
            Parameter::new_with_default("last_sale", NumberType(F64Kind), Number(F64Value(23.66))),
        ];
        let array = ArrayValue(Array::from(vec![
            Structured(Hard(HardStructure::new(params.clone(), vec![
                StringValue("BIZ".into()), StringValue("NYSE".into()), Number(F64Value(23.66))
            ]))),
            Structured(Hard(HardStructure::new(params.clone(), vec![
                StringValue("DMX".into()), StringValue("OTC_BB".into()), Number(F64Value(1.17))
            ])))
        ]));
        verify(array, TableType(params.clone()), TableValue(Dataframe::Model(
            ModelRowCollection::from_parameters_and_rows(&params, &vec![
                Row::new(0, vec![
                    StringValue("BIZ".into()), StringValue("NYSE".into()), Number(F64Value(23.66)),
                ]),
                Row::new(1, vec![
                    StringValue("DMX".into()), StringValue("OTC_BB".into()), Number(F64Value(1.17)),
                ])
            ])
        )))
    }

    #[test]
    fn test_boolean_to_i64() {
        verify(Boolean(true), NumberType(I64Kind), Number(I64Value(1)));
        verify(Boolean(false), NumberType(I64Kind), Number(I64Value(0)));
    }

    #[test]
    fn test_boolean_to_string() {
        verify(Boolean(true), StringType, StringValue("true".into()));
        verify(Boolean(false), StringType, StringValue("false".into()));
    }

    #[test]
    fn test_date_to_i64() {
        verify(DateValue(17453558321123), NumberType(I64Kind), Number(I64Value(17453558321123)));
    }

    #[test]
    fn test_date_to_string() {
        verify(DateValue(17453558321123), StringType, StringValue("2523-01-30T18:38:41.123Z".into()));
    }

    #[test]
    fn test_f64_to_string() {
        verify(Number(F64Value(12.35)), FixedSizeType(StringType.into(), 5), StringValue("12.35".into()));
    }

    #[test]
    fn test_i64_to_boolean() {
        verify(Number(I64Value(0)), BooleanType, Boolean(false));
        verify(Number(I64Value(1)), BooleanType, Boolean(true));
    }

    #[test]
    fn test_string_to_boolean() {
        verify(StringValue("true".into()), BooleanType, Boolean(true));
    }

    #[test]
    fn test_string_to_binary() {
        verify(StringValue("Hello there".into()), BinaryType, BinaryValue(b"Hello there".into()));
    }

    #[test]
    fn test_string_to_error() {
        verify(StringValue("This is an error".into()),
               ErrorType, ErrorValue(Exact("This is an error".into())));
    }

    #[test]
    fn test_u128_to_string() {
        verify(Number(U128Value(0xfeed_dead_beef_deaf_fade_cafe_babe_face_u128)),
               StringType, StringValue("338859001745337648252653219454709070542".into()));
    }

    #[test]
    fn test_u128_to_uuid() {
        verify(Number(U128Value(0xfeed_dead_beef_deaf_fade_cafe_babe_face_u128)),
               UUIDType, UUIDValue(0xfeed_dead_beef_deaf_fade_cafe_babe_face_u128));
    }

    #[test]
    fn test_uuid_to_binary() {
        verify(UUIDValue(0xfeed_dead_beef_deaf_fade_cafe_babe_face_u128),
               BinaryType,
               BinaryValue(vec![
                   0xfe, 0xed, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xaf, 
                   0xfa, 0xde, 0xca, 0xfe, 0xba, 0xbe, 0xfa, 0xce
               ]));
    }


    #[test]
    fn test_uuid_to_u128() {
        verify(UUIDValue(0xfeed_dead_beef_deaf_fade_cafe_babe_face_u128),
               NumberType(U128Kind), Number(U128Value(0xfeed_dead_beef_deaf_fade_cafe_babe_face_u128)));
    }

    #[test]
    fn test_uuid_to_string() {
        verify(UUIDValue(0xfeed_dead_beef_deaf_fade_cafe_babe_face_u128),
               StringType, StringValue("feeddead-beef-deaf-fade-cafebabeface".into()));
    }

    fn verify(from_value: TypedValue, to_type: DataType, to_value: TypedValue) {
        assert_eq!(from_value.convert_to(to_type).unwrap(), to_value);
    }
}

/// unwrapping tests
#[cfg(test)]
mod unwrapping_tests {
    use crate::data_types::DataType::*;
    use crate::errors::Errors::Exact;
    use crate::number_kind::NumberKind::*;
    use crate::numbers::Numbers::*;
    use crate::parameter::Parameter;
    use crate::sequences::Array;
    use crate::structures::HardStructure;
    use crate::structures::Structures::Hard;
    use crate::typed_values::TypedValue;
    use crate::typed_values::TypedValue::*;

    #[test]
    fn test_array_strings() {
        let array = ArrayValue(Array::from(vec![
            StringValue("123".into()), StringValue("abc".into()),
            StringValue("xyz".into()), StringValue("897".into()),
        ]));
        verify(array, r#"["123", "abc", "xyz", "897"]"#)
    }

    #[test]
    fn test_array_structure() {
        let params = vec![
            Parameter::new_with_default("symbol", FixedSizeType(StringType.into(), 3), StringValue("BIZ".into())),
            Parameter::new_with_default("exchange", FixedSizeType(StringType.into(), 4), StringValue("NYSE".into())),
            Parameter::new_with_default("last_sale", NumberType(F64Kind), Number(F64Value(23.66))),
        ];
        let array = ArrayValue(Array::from(vec![
            Structured(Hard(HardStructure::new(params.clone(), vec![
                StringValue("BIZ".into()), StringValue("NYSE".into()), Number(F64Value(23.66))
            ]))),
            Structured(Hard(HardStructure::new(params.clone(), vec![
                StringValue("DMX".into()), StringValue("OTC_BB".into()), Number(F64Value(1.17))
            ])))
        ]));
        verify_diff(
            array,
            r#"[{"exchange":"NYSE","last_sale":23.66,"symbol":"BIZ"}, {"exchange":"NYSE","last_sale":23.66,"symbol":"BIZ"}]"#,
            r#"[Struct(symbol: String(3) = "BIZ", exchange: String(4) = "NYSE", last_sale: f64 = 23.66), Struct(symbol: String(3) = "DMX", exchange: String(4) = "OTC_BB", last_sale: f64 = 1.17)]"#)
    }

    #[test]
    fn test_binary() {
        verify(BinaryValue(vec![
            0xde, 0xad, 0xbe, 0xef, 0xfa, 0xce
        ]), "0vdeadbeefface");
    }

    #[test]
    fn test_boolean() {
        verify(Boolean(true), "true");
        verify(Boolean(false), "false");
    }

    #[test]
    fn test_date() {
        verify(DateValue(17453558321123), "2523-01-30T18:38:41.123Z");
    }

    #[test]
    fn test_error() {
        verify(ErrorValue(Exact("This is an error".into())), "This is an error");
    }

    #[test]
    fn test_tuple() {
        let tuple = TupleValue(vec![
            DateValue(17453558321123),
            StringValue("hello".into()), 
            UUIDValue(0xfeed_dead_beef_deaf_fade_cafe_babe_face_u128)
        ]);
        verify(tuple, r#"(2523-01-30T18:38:41.123Z, "hello", feeddead-beef-deaf-fade-cafebabeface)"#)
    }

    #[test]
    fn test_uuid() {
        verify(
            UUIDValue(0xfeed_dead_beef_deaf_fade_cafe_babe_face_u128),
            "feeddead-beef-deaf-fade-cafebabeface")
    }

    fn verify(value: TypedValue, text: &str) {
        assert_eq!(value.unwrap_value(), text);
        assert_eq!(value.to_code(), text);
    }

    fn verify_diff(value: TypedValue, unwrap_text: &str, code_text: &str) {
        assert_eq!(value.unwrap_value(), unwrap_text);
        assert_eq!(value.to_code(), code_text);
    }
}