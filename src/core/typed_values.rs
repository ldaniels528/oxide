#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// TypedValue class
////////////////////////////////////////////////////////////////////

use std::cmp::Ordering;
use std::collections::Bound;
use std::fmt::{format, Display};
use std::hash::{DefaultHasher, Hasher};
use std::i32;
use std::ops::*;

use chrono::DateTime;
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
use crate::dataframe::Dataframe::{Disk, Model};

use crate::errors::Errors::{CannotSubtract, Exact, Multiple, Syntax, TypeMismatch};
use crate::errors::TypeMismatchErrors::{CannotBeNegated, StructsOneOrMoreExpected, UnsupportedType};
use crate::errors::{throw, Errors};
use crate::expression::Expression;
use crate::field::FieldMetadata;
use crate::file_row_collection::FileRowCollection;
use crate::inferences::Inferences;
use crate::machine::Machine;
use crate::model_row_collection::ModelRowCollection;
use crate::namespaces::Namespace;
use crate::number_kind::NumberKind;
use crate::number_kind::NumberKind::*;
use crate::numbers::Numbers;
use crate::numbers::Numbers::*;
use crate::object_config::ObjectConfig;
use crate::parameter::Parameter;
use crate::platform::PlatformOps;
use crate::row_collection::RowCollection;
use crate::row_metadata::RowMetadata;
use crate::sequences::*;
use crate::structures::Row;
use crate::structures::Structures::{Firm, Hard, Soft};
use crate::structures::*;
use crate::tokens::Token;
use crate::typed_values::TypedValue::*;
use shared_lib::fail;

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
    ASCII(Vec<char>),
    Binary(Vec<u8>),
    Boolean(bool),
    ErrorValue(Errors),
    Function { params: Vec<Parameter>, body: Box<Expression>, returns: DataType },
    Kind(DataType),
    NamespaceValue(Namespace),
    Null,
    Number(Numbers),
    PlatformOp(PlatformOps),
    StringValue(String),
    Structured(Structures),
    TableValue(Dataframe),
    TupleValue(Vec<TypedValue>),
    Undefined,
}

impl TypedValue {

    ////////////////////////////////////////////////////////////////////
    //  Static Methods
    ////////////////////////////////////////////////////////////////////

    pub fn express_range(a: Self, b: Self, step: Self) -> Vec<Self> {
        let mut items = Vec::new();
        let mut n = a;
        while n < b {
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
                HardStructure::from_parameters(&args.iter()
                    .map(|(name, value)| {
                        let tv = TypedValue::from_json(value);
                        Parameter::with_default(name, tv.get_type().clone(), tv)
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
        match number.trim() {
            s if s.starts_with("0b") => Ok(Number(U64Value(u64::from_str_radix(&s[2..], 2).map_err(|e| cnv_error!(e))?))),
            s if s.starts_with("0o") => Ok(Number(U64Value(u64::from_str_radix(&s[2..], 8).map_err(|e| cnv_error!(e))?))),
            s if s.starts_with("0x") => Ok(Number(U64Value(u64::from_str_radix(&s[2..], 16).map_err(|e| cnv_error!(e))?))),
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

    pub fn from_token(token: &Token) -> std::io::Result<TypedValue> {
        match token {
            Token::Atom { .. } => throw(Syntax("literal expected".to_string())),
            Token::Backticks { .. } => throw(Syntax("literal expected".to_string())),
            Token::DoubleQuoted { text, .. } => Ok(StringValue(text.to_string())),
            Token::Numeric { text, .. } => Self::from_numeric(text),
            Token::Operator { .. } => throw(Syntax("literal expected".to_string())),
            Token::SingleQuoted { text, .. } => Ok(StringValue(text.to_string())),
        }
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

    pub fn wrap_value(raw_value: &str) -> std::io::Result<Self> {
        let iso_date_regex = Regex::new(ISO_DATE_FORMAT).map_err(|e| cnv_error!(e))?;
        let uuid_regex = Regex::new(UUID_FORMAT).map_err(|e| cnv_error!(e))?;
        fn is_quoted(s: &str) -> bool {
            (s.starts_with("\"") && s.ends_with("\"")) ||
                (s.starts_with("'") && s.ends_with("'"))
        }
        let result = match raw_value.trim() {
            "" => Null,
            "Ack" => Number(Ack),
            "false" => Boolean(false),
            "null" => Null,
            "true" => Boolean(true),
            "undefined" => Undefined,
            s if Self::is_numeric_value(s)? => Self::from_numeric(s)?,
            s if iso_date_regex.is_match(s) =>
                Number(DateValue(DateTime::parse_from_rfc3339(s)
                    .map_err(|e| cnv_error!(e))?.timestamp_millis())),
            s if uuid_regex.is_match(s) => Number(U128Value(ByteCodeCompiler::decode_uuid(s)?)),
            s if is_quoted(s) => StringValue(s[1..s.len() - 1].to_string()),
            s => return throw(Syntax(format!("Literal value expected near {}", s)))
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

    /// returns true, if:
    /// 1. the host value is an array, and the item value is found within it,
    /// 2. the host value is a table, and the item value matches a row found within it,
    /// 3. the host value is a struct, and the item value matches a name (key) found within it,
    pub fn contains(&self, value: &TypedValue) -> TypedValue {
        match &self {
            ArrayValue(items) => Boolean(items.contains(value)),
            Structured(Hard(hard)) => match value {
                StringValue(name) => Boolean(hard.contains(name)),
                _ => Boolean(false)
            },
            Structured(Soft(soft)) => match value {
                StringValue(name) => Boolean(soft.contains(name)),
                _ => Boolean(false)
            },
            TableValue(Model(mrc)) => match value {
                Structured(Soft(soft)) => mrc.contains(&soft.to_row()),
                Structured(Hard(hard)) => mrc.contains(&hard.to_row()),
                _ => Boolean(false)
            }
            _ => Boolean(false)
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        match self {
            ArrayValue(items) => {
                let mut bytes = Vec::new();
                bytes.extend(items.len().to_be_bytes());
                for item in items.iter() { bytes.extend(item.encode()); }
                bytes
            }
            Binary(bytes) => ByteCodeCompiler::encode_u8x_n(bytes.to_vec()),
            Boolean(ok) => vec![if *ok { 1 } else { 0 }],
            ErrorValue(err) => ByteCodeCompiler::encode_string(err.to_string().as_str()),
            NamespaceValue(ns) =>
                ByteCodeCompiler::encode_string(ns.get_full_name().as_str()),
            Number(number) => number.encode(),
            PlatformOp(pf) => pf.encode().unwrap_or(vec![]),
            StringValue(string) => ByteCodeCompiler::encode_string(string),
            TableValue(rc) => ByteCodeCompiler::encode_df(rc),
            other => ByteCodeCompiler::encode_value(other).unwrap_or_else(|err| {
                error!("decode error: {err}");
                Vec::new()
            })
        }
    }

    pub fn get_type(&self) -> DataType {
        match self {
            ArrayValue(a) => ArrayType(a.len()),
            ASCII(chars) => ASCIIType(chars.len()),
            Binary(bytes) => BinaryType(bytes.len()),
            Boolean(..) => BooleanType,
            ErrorValue(..) => ErrorType,
            Function { params, returns, .. } =>
                FunctionType(params.clone(), Box::from(returns.clone())),
            Kind(dt) => dt.clone(),
            NamespaceValue(..) => TableType(Vec::new(), 0),
            Null | Undefined => VaryingType(Vec::new()),
            Number(n) => NumberType(n.kind()),
            PlatformOp(op) => op.get_type(),
            Structured(s) => StructureType(s.get_parameters()),
            StringValue(s) => StringType(s.len()),
            TableValue(tv) => TableType(Parameter::from_columns(tv.get_columns()), 0),
            TupleValue(items) => TupleType(items.iter()
                .map(|i| i.get_type())
                .collect()),
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
        matches!(self, Number(Numbers::Ack) | Boolean(true) | Number(..) | NamespaceValue(..) | TableValue(..))
    }

    pub fn is_true(&self) -> bool {
        matches!(self, Number(Numbers::Ack) | Boolean(true))
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
            z => ErrorValue(TypeMismatch(UnsupportedType(TableType(vec![], 0), z.get_type())))
        }
    }

    pub fn to_bool(&self) -> bool {
        match self {
            Number(Numbers::Ack) => true,
            Boolean(b) => *b,
            _ => false
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Binary(bytes) => bytes.to_vec(),
            StringValue(s) => s.bytes().collect(),
            z => z.encode()
        }
    }

    pub fn to_code(&self) -> String {
        match self {
            ArrayValue(items) =>
                format!("[{}]", items.iter()
                    .map(|v| v.to_code())
                    .collect::<Vec<_>>().join(", ")),
            PlatformOp(pf) => pf.to_code(),
            Structured(Hard(hs)) => hs.to_code(),
            Structured(Soft(ss)) => ss.to_code(),
            StringValue(s) => format!("\"{s}\""),
            other => other.unwrap_value()
        }
    }

    pub fn to_f32(&self) -> f32 {
        match self {
            Boolean(true) => 1.,
            Number(nv) => nv.to_f32(),
            _ => 0.
        }
    }

    pub fn to_f64(&self) -> f64 {
        match self {
            Boolean(true) => 1.,
            Number(nv) => nv.to_f64(),
            _ => 0.
        }
    }

    pub fn to_i8(&self) -> i8 {
        match self {
            Boolean(true) => 1,
            Number(nv) => nv.to_i8(),
            _ => 0
        }
    }

    pub fn to_i16(&self) -> i16 {
        match self {
            Boolean(true) => 1,
            Number(nv) => nv.to_i16(),
            _ => 0
        }
    }

    pub fn to_i32(&self) -> i32 {
        match self {
            Boolean(true) => 1,
            Number(nv) => nv.to_i32(),
            _ => 0
        }
    }

    pub fn to_i64(&self) -> i64 {
        match self {
            Boolean(true) => 1,
            Number(nv) => nv.to_i64(),
            _ => 0
        }
    }

    pub fn to_i128(&self) -> i128 {
        match self {
            Boolean(true) => 1,
            Number(nv) => nv.to_i128(),
            _ => 0
        }
    }

    pub fn to_result<A>(&self, f: fn(&TypedValue) -> A) -> std::io::Result<A> {
        match self {
            ErrorValue(err) => fail(err.to_string()),
            other => Ok(f(other))
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        match self {
            ArrayValue(items) => serde_json::json!(items.iter()
                    .map(|v|v.to_json())
                    .collect::<Vec<_>>()),
            ASCII(chars) => serde_json::json!(chars),
            Binary(bytes) => serde_json::json!(bytes),
            Boolean(b) => serde_json::json!(b),
            ErrorValue(message) => serde_json::json!(message),
            Function { params, body: code, returns } => {
                let my_params = serde_json::Value::Array(params.iter()
                    .map(|c| c.to_json()).collect());
                serde_json::json!({ "params": my_params, "code": code.to_code(), "returns": returns.to_type_declaration() })
            }
            Kind(dt) => serde_json::json!(dt),
            NamespaceValue(ns) => serde_json::json!(ns.get_full_name()),
            Null => serde_json::Value::Null,
            Number(nv) => nv.to_json(),
            PlatformOp(nf) => serde_json::json!(nf),
            StringValue(s) => serde_json::json!(s),
            Structured(s) => s.to_json(),
            TableValue(df) => {
                let columns = df.get_columns();
                let rows = df.iter()
                    .map(|r| r.to_hash_json_value(columns))
                    .collect::<Vec<_>>();
                serde_json::json!(rows)
            }
            TupleValue(items) => serde_json::json!(
                items.iter()
                    .map(|v|v.to_json())
                    .collect::<Vec<_>>()),
            Undefined => serde_json::Value::Null,
        }
    }

    pub fn to_table(&self) -> std::io::Result<Box<dyn RowCollection>> {
        match self.to_table_value() {
            ErrorValue(error) => throw(error),
            TableValue(df) => Ok(Box::new(df)),
            z => throw(TypeMismatch(UnsupportedType(TableType(vec![], 0), z.get_type())))
        }
    }

    pub fn to_table_value(&self) -> TypedValue {
        match self {
            ArrayValue(items) => self.convert_array_to_table(&items.get_values()),
            ErrorValue(err) => ErrorValue(err.to_owned()),
            NamespaceValue(ns) =>
                match ObjectConfig::load(ns) {
                    Ok(ObjectConfig::TableConfig { .. }) =>
                        match FileRowCollection::open(ns) {
                            Ok(frc) => TableValue(Disk(frc)),
                            Err(err) => ErrorValue(Exact(err.to_string())),
                        },
                    Err(err) => ErrorValue(Exact(err.to_string()))
                }
            Structured(s) => TableValue(Model(s.to_table())),
            TableValue(df) => TableValue(df.to_owned()),
            z => z.to_owned()
        }
    }

    fn convert_array_to_table(&self, items: &Vec<TypedValue>) -> TypedValue {
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
            [] => ErrorValue(TypeMismatch(StructsOneOrMoreExpected)),
            [df] => TableValue(df.to_owned()),
            dfs => TableValue(Dataframe::combine_tables(dfs.to_vec()))
        }
    }

    pub fn to_u8(&self) -> u8 {
        match self {
            Number(Numbers::Ack) => 1,
            Boolean(true) => 1,
            Number(n) => n.to_u8(),
            _ => 0
        }
    }

    pub fn to_u16(&self) -> u16 {
        match self {
            Number(Numbers::Ack) => 1,
            Boolean(true) => 1,
            Number(n) => n.to_u16(),
            _ => 0
        }
    }

    pub fn to_u32(&self) -> u32 {
        match self {
            Number(Numbers::Ack) => 1,
            Boolean(true) => 1,
            Number(n) => n.to_u32(),
            _ => 0
        }
    }

    pub fn to_u64(&self) -> u64 {
        match self {
            Number(Numbers::Ack) => 1,
            Boolean(true) => 1,
            Number(n) => n.to_u64(),
            _ => 0
        }
    }

    pub fn to_u128(&self) -> u128 {
        match self {
            Number(Numbers::Ack) => 1,
            Boolean(true) => 1,
            Number(n) => n.to_u128(),
            _ => 0
        }
    }

    pub fn to_usize(&self) -> usize {
        match self {
            Number(Numbers::Ack) => 1,
            Boolean(true) => 1,
            Number(n) => n.to_usize(),
            _ => 0
        }
    }

    pub fn unwrap_value(&self) -> String {
        match self {
            TypedValue::ArrayValue(av) => {
                let values = av.iter().map(|v| v.unwrap_value()).collect::<Vec<_>>();
                format!("[{}]", values.join(", "))
            }
            TypedValue::Binary(bytes) => hex::encode(bytes),
            TypedValue::ASCII(chars) => format!("{:#?}", chars),
            TypedValue::Boolean(b) => (if *b { "true" } else { "false" }).into(),
            TypedValue::ErrorValue(message) => message.to_string(),
            TypedValue::Function { params, body: code, returns } =>
                format!("(fn({}){} => {})",
                        params.iter().map(|c| c.to_code()).collect::<Vec<_>>().join(", "),
                        match returns.to_code().as_str() {
                            "" => "".to_string(),
                            s => format!(": {}", s),
                        },
                        code.to_code()),
            TypedValue::Kind(dt) => dt.to_code(),
            TypedValue::NamespaceValue(ns) => ns.get_full_name(),
            TypedValue::Null => "null".into(),
            TypedValue::Number(number) => number.unwrap_value(),
            TypedValue::PlatformOp(nf) => nf.to_code(),
            TypedValue::StringValue(string) => string.into(),
            TypedValue::Structured(structure) => structure.to_json().to_string(),
            TypedValue::TableValue(rcv) => {
                let columns = rcv.get_columns();
                serde_json::json!(rcv.iter().map(|r|r.to_hash_json_value(columns))
                    .collect::<Vec<_>>()).to_string()
            }
            TypedValue::TupleValue(items) =>
                format!("({})", items.iter().map(|i| i.to_code())
                    .collect::<Vec<_>>().join(", ")),
            TypedValue::Undefined => "undefined".into(),
        }
    }

    ///////////////////////////////////////////////////////////////
    //      CONDITIONAL OPERATIONS
    ///////////////////////////////////////////////////////////////

    pub fn and(&self, rhs: &TypedValue) -> Option<TypedValue> {
        Some(Boolean(self.to_bool() && rhs.to_bool()))
    }

    pub fn factorial(&self) -> TypedValue {
        fn fact_u128(n: u128) -> TypedValue {
            fn fact_f(n: u128) -> u128 { if n <= 1 { 1 } else { n * fact_f(n - 1) } }
            Number(U128Value(fact_f(n)))
        }
        fact_u128(self.to_u128())
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

    pub fn range(&self, rhs: &Self) -> Option<Self> {
        let mut values = Vec::new();
        for n in self.to_i64()..rhs.to_i64() { values.push(Number(I64Value(n))) }
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
            (Boolean(..), Number(Numbers::Ack)) => Boolean(true),
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
            (Number(Numbers::Ack), Boolean(b)) => Boolean(b),
            (StringValue(a), StringValue(b)) => StringValue(a + b.as_str()),
            (TableValue(a), Structured(Hard(b))) => {
                let mut mrc = match ModelRowCollection::from_table(Box::new(&a)) {
                    Ok(mrc) => mrc,
                    Err(err) => return ErrorValue(Exact(err.to_string()))
                };
                match mrc.append_row(b.to_row()) {
                    ErrorValue(s) => ErrorValue(s),
                    _ => TableValue(Model(mrc)),
                }
            }
            (TableValue(Model(a)), TableValue(Model(b))) =>
                match ModelRowCollection::combine(a.get_columns().to_owned(), vec![&a, &b]) {
                    Ok(mrc) => TableValue(Model(mrc)),
                    Err(err) => ErrorValue(Exact(err.to_string()))
                },
            (TupleValue(a), TupleValue(b)) => TupleValue(add_vec(a, b)),
            (a, b) => ErrorValue(TypeMismatch(UnsupportedType(a.get_type(), b.get_type())))
        }
    }
}

impl BitAnd for TypedValue {
    type Output = TypedValue;

    fn bitand(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Number(a), Number(b)) => Number(a & b),
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
    let columns = mrc.get_columns();
    match mrc.read_row(index) {
        Ok((row, meta)) => {
            Structured(match meta.is_allocated {
                true => Firm(row, columns.to_owned()),
                false => Firm(Row::create(row.get_id(), columns), columns.to_owned()),
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
            Binary(..) => error("Binary".into()),
            ASCII(..) => error("ASCII".into()),
            Boolean(n) => Boolean(!n),
            ErrorValue(msg) => ErrorValue(msg),
            Function { .. } => error("Function".into()),
            Kind(dt) => Kind(dt),
            NamespaceValue(ns) => error(format!("ns({})", ns.get_full_name())),
            Null => Null,
            Number(nv) => Number(nv.neg()),
            Number(oc) => Number(I64Value(-oc.to_i64())),
            PlatformOp(..) => error("PlatformFunction".into()),
            StringValue(..) => error("String".into()),
            Structured(..) => error("Structure".into()),
            TableValue(..) => error("Table".into()),
            TupleValue(a) => TupleValue(neg_vec(a)),
            Undefined => Undefined,
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
            (Binary(a), Binary(b)) => a.partial_cmp(b),
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
mod tests {
    use crate::testdata::{make_quote, make_quote_columns};
    use serde_json::{json, Value};

    use super::*;

    #[test]
    fn test_express_range() {
        let (a, b, c) = (
            Number(I64Value(1)), Number(I64Value(5)), Number(I64Value(1))
        );
        let list = TypedValue::express_range(a, b, c);
        assert_eq!(list, vec![
            Number(I64Value(1)), Number(I64Value(2)), Number(I64Value(3)),
            Number(I64Value(4)),
        ])
    }

    #[test]
    fn test_decode() {
        let buf: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 5, b'H', b'e', b'l', b'l', b'o'];
        assert_eq!(StringType(5).decode(&buf, 0), StringValue("Hello".into()))
    }

    #[test]
    fn test_addition() {
        use Numbers::*;
        assert_eq!(Number(F64Value(45.0)) + Number(F64Value(32.7)), Number(F64Value(77.7)));
        assert_eq!(Number(F32Value(45.7)) + Number(F32Value(32.0)), Number(F32Value(77.7)));
        assert_eq!(Number(I128Value(45)) + Number(I128Value(32)), Number(I128Value(77)));
        assert_eq!(Number(I64Value(45)) + Number(I64Value(32)), Number(I64Value(77)));
        assert_eq!(Number(I32Value(45)) + Number(I32Value(32)), Number(I32Value(77)));
        assert_eq!(Number(I16Value(45)) + Number(I16Value(32)), Number(I16Value(77)));
        assert_eq!(Number(I8Value(45)) + Number(I8Value(32)), Number(I8Value(77)));
        assert_eq!(Number(U128Value(45)) + Number(U128Value(32)), Number(U128Value(77)));
        assert_eq!(Number(U64Value(45)) + Number(U64Value(32)), Number(U64Value(77)));
        assert_eq!(Number(U32Value(45)) + Number(U32Value(32)), Number(U32Value(77)));
        assert_eq!(Number(U16Value(45)) + Number(U16Value(32)), Number(U16Value(77)));
        assert_eq!(Number(U8Value(45)) + Number(U8Value(32)), Number(U8Value(77)));
        assert_eq!(Boolean(true) + Boolean(true), Boolean(true));
        assert_eq!(StringValue("Hello".into()) + StringValue(" World".into()), StringValue("Hello World".into()));
    }

    #[test]
    fn test_subtraction() {
        use Numbers::*;
        assert_eq!(Number(F64Value(45.0)) - Number(F64Value(32.5)), Number(F64Value(12.5)));
        assert_eq!(Number(F32Value(45.5)) - Number(F32Value(32.0)), Number(F32Value(13.5)));
        assert_eq!(Number(I128Value(45)) - Number(I128Value(32)), Number(I128Value(13)));
        assert_eq!(Number(I64Value(45)) - Number(I64Value(32)), Number(I64Value(13)));
        assert_eq!(Number(I32Value(45)) - Number(I32Value(32)), Number(I32Value(13)));
        assert_eq!(Number(I16Value(45)) - Number(I16Value(32)), Number(I16Value(13)));
        assert_eq!(Number(I8Value(45)) - Number(I8Value(32)), Number(I8Value(13)));
        assert_eq!(Number(U128Value(45)) - Number(U128Value(32)), Number(U128Value(13)));
        assert_eq!(Number(U64Value(45)) - Number(U64Value(32)), Number(U64Value(13)));
        assert_eq!(Number(U32Value(45)) - Number(U32Value(32)), Number(U32Value(13)));
        assert_eq!(Number(U16Value(45)) - Number(U16Value(32)), Number(U16Value(13)));
        assert_eq!(Number(U8Value(45)) - Number(U8Value(32)), Number(U8Value(13)));
    }

    #[test]
    fn test_array_contains() {
        let array = ArrayValue(Array::from(vec![
            StringValue("ABC".into()),
            StringValue("R2X2".into()),
            StringValue("123".into()),
            StringValue("Hello".into()),
        ]));
        assert_eq!(array.contains(&StringValue("123".into())), Boolean(true))
    }

    #[test]
    fn test_from_json() {
        let structure = HardStructure::from_parameters(&vec![
            Parameter::with_default("name", StringType(4), StringValue("John".into())),
            Parameter::with_default("age", NumberType(I64Kind), Number(I64Value(40)))
        ]);
        let js_value0: Value = serde_json::from_str(r##"{"name":"John","age":40}"##).unwrap();
        let js_value1: Value = serde_json::from_str(structure.to_string().as_str()).unwrap();
        assert_eq!(js_value1, js_value0)
    }

    #[test]
    fn test_hard_structure_contains_key() {
        let structure = Structured(Hard(HardStructure::new(make_quote_columns(), vec![
            StringValue("PEW".into()),
            StringValue("NASDAQ".into()),
            Number(F64Value(304.69)),
        ])));
        assert_eq!(structure.contains(&StringValue("symbol".into())), Boolean(true))
    }

    #[test]
    fn test_soft_structure_contains_key() {
        let structure = Structured(Soft(SoftStructure::new(&vec![
            ("name", StringValue("symbol".into())),
            ("param_type", StringValue("String(8)".into())),
            ("default_value", Null),
        ])));
        assert_eq!(structure.contains(&StringValue("name".into())), Boolean(true))
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
        let hs = Structured(Hard(HardStructure::new(make_quote_columns(), vec![
            StringValue("GOTO".into()),
            StringValue("OTC".into()),
            Number(F64Value(0.1421)),
        ])));
        assert_eq!(table.contains(&hs), Boolean(true))
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
        assert_eq!(table.contains(&ss), Boolean(true))
    }

    #[test]
    fn test_table_index() {
        let phys_columns = make_quote_columns();
        let mrc = ModelRowCollection::from_columns_and_rows(&phys_columns, &vec![
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
            ), phys_columns)))
    }

    #[test]
    fn test_eq() {
        use Numbers::*;
        assert_eq!(F32Value(45.0), F32Value(45.0));
        assert_eq!(F64Value(45.0), F64Value(45.0));

        assert_eq!(I8Value(0x1B), I8Value(0x1B));
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
    fn test_ne() {
        use Numbers::*;
        assert_ne!(U8Value(0xCE), U8Value(0x00));
        assert_ne!(I16Value(0x7ACE), I16Value(0xACE));
        assert_ne!(I32Value(0x1111_BEEF), I32Value(0xBEEF));
        assert_ne!(I64Value(0x5555_FACE_CAFE_BABE), I64Value(0xFACE_CAFE_BABE));
        assert_ne!(F32Value(45.0), F32Value(45.7));
        assert_ne!(F64Value(99.142857), F64Value(19.48));
    }

    #[test]
    fn test_gt() {
        use Numbers::*;
        assert!({
            let a = ArrayValue(Array::from(vec![Number(U8Value(0xCE)), Number(U8Value(0xCE))]));
            let b = ArrayValue(Array::from(vec![Number(U8Value(0x23)), Number(U8Value(0xBE))]));
            a > b
        });
        assert!(U8Value(0xCE) > U8Value(0xAA));
        assert!(I16Value(0x7ACE) > I16Value(0x1111));
        assert!(I32Value(0x1111_BEEF) > I32Value(0x0ABC_BEEF));
        assert!(I64Value(0x5555_FACE_CAFE_BABE) > I64Value(0x0000_FACE_CAFE_BABE));
        assert!(F32Value(287.11) > F32Value(45.3867));
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
    fn test_outcomes() {
        verify_encode_decode(&Number(Numbers::Ack));
        verify_encode_decode(&Number(Numbers::RowId(1013)));
        verify_encode_decode(&Number(Numbers::RowsAffected(19)));
    }

    #[test]
    fn test_to_json() {
        use Numbers::*;
        verify_to_json(Number(Numbers::Ack), json!(0));
        verify_to_json(Boolean(false), serde_json::Value::Bool(false));
        verify_to_json(Boolean(true), serde_json::Value::Bool(true));
        verify_to_json(Number(DateValue(1709163679081)), serde_json::Value::String("2024-02-28T23:41:19.081Z".into()));
        verify_to_json(Number(F32Value(38.15999984741211)), serde_json::json!(38.15999984741211));
        verify_to_json(Number(F64Value(100.1)), serde_json::json!(100.1));
        verify_to_json(Number(I8Value(-100)), serde_json::json!(-100));
        verify_to_json(Number(I16Value(-1000)), serde_json::json!(-1000));
        verify_to_json(Number(I32Value(-123_456)), serde_json::json!(-123_456));
        verify_to_json(Number(I64Value(-123_456_789)), serde_json::json!(-123_456_789));
        verify_to_json(Number(I128Value(-123_456_789)), serde_json::json!(-123_456_789));
        verify_to_json(Number(U8Value(100)), serde_json::json!(100));
        verify_to_json(Number(U16Value(1000)), serde_json::json!(1000));
        verify_to_json(Number(U32Value(123_456)), serde_json::json!(123_456));
        verify_to_json(Number(U64Value(123_456_789)), serde_json::json!(123_456_789));
        verify_to_json(Number(U128Value(123_456_789)), serde_json::json!(123_456_789));
        verify_to_json(Null, serde_json::Value::Null);
        verify_to_json(StringValue("Hello World".into()), serde_json::Value::String("Hello World".into()));
        //verify_to_json()
        verify_to_json(Undefined, serde_json::Value::Null);
    }

    #[test]
    fn test_to_string() {
        assert_eq!(format!("{}", StringValue("Hello".into())), "Hello");
    }

    #[test]
    fn test_wrap_and_unwrap_value() {
        verify_wrap_unwrap("Ack", Number(Ack));
        verify_wrap_unwrap("false", Boolean(false));
        verify_wrap_unwrap("true", Boolean(true));
        verify_wrap_unwrap("2024-02-28T23:41:19.081Z", Number(DateValue(1709163679081)));
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