////////////////////////////////////////////////////////////////////
// TypedValue class
////////////////////////////////////////////////////////////////////

use std::cmp::Ordering;
use std::collections::Bound;
use std::fmt::Display;
use std::hash::{DefaultHasher, Hasher};
use std::i32;
use std::ops::*;

use chrono::DateTime;
use log::error;
use num_traits::abs;
use regex::Regex;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use shared_lib::fail;

use crate::byte_code_compiler::ByteCodeCompiler;
use crate::cnv_error;
use crate::codec;
use crate::codec::Codec;
use crate::compiler::fail_value;
use crate::data_types::DataType::*;
use crate::data_types::StorageTypes::{FixedSize, BLOB};
use crate::data_types::*;
use crate::errors::Errors;
use crate::errors::Errors::{CannotSubtract, Exact, TypeMismatch, Various};
use crate::expression::Expression;
use crate::file_row_collection::FileRowCollection;
use crate::inferences::Inferences;
use crate::model_row_collection::ModelRowCollection;
use crate::namespaces::Namespace;
use crate::number_kind::NumberKind::*;
use crate::numbers::NumberValue;
use crate::numbers::NumberValue::*;
use crate::outcomes::{OutcomeKind, Outcomes};
use crate::parameter::Parameter;
use crate::platform::PlatformFunctions;
use crate::row_collection::RowCollection;
use crate::rows::Row;
use crate::structures::*;
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
    Array(Vec<TypedValue>),
    Boolean(bool),
    ByteArray(Vec<u8>),
    DateValue(i64),
    ErrorValue(Errors),
    Function { params: Vec<Parameter>, code: Box<Expression> },
    NamespaceValue(String, String, String),
    Null,
    Number(NumberValue),
    Outcome(Outcomes),
    PlatformFunction(PlatformFunctions),
    StructureHard(HardStructure),
    StructureSoft(SoftStructure),
    StringValue(String),
    TableValue(ModelRowCollection),
    Undefined,
}

impl TypedValue {

    ////////////////////////////////////////////////////////////////////
    //  Static Methods
    ////////////////////////////////////////////////////////////////////

    /// decodes the typed value based on the supplied data type and buffer
    pub fn decode(data_type: &DataType, buffer: &Vec<u8>, offset: usize) -> Self {
        match data_type {
            ArrayType(_kind) => Array(Vec::new()),
            ByteArrayType(_size) => ByteArray(Vec::new()),
            BooleanType => codec::decode_u8(buffer, offset, |b| Boolean(b == 1)),
            DateType => codec::decode_u8x8(buffer, offset, |b| DateValue(i64::from_be_bytes(b))),
            // EnumType(labels) => {
            //     let index = codec::decode_u8x4(buffer, offset, |b| i32::from_be_bytes(b));
            //     StringValue(labels[index as usize].to_string())
            // }
            ErrorType => ErrorValue(Exact(codec::decode_string(buffer, offset, 255).to_string())),
            //FunctionType(_columns) => Codec::decode_value(&buffer[offset..].to_vec()),
            //JSONType => Codec::decode_value(&buffer[offset..].to_vec()),
            LazyType => Null,
            NumberType(kind) => Number(NumberValue::decode(buffer, offset, *kind)),
            // OutcomeType(kind) => match kind {
            //     OutcomeKind::Acked => Outcome(Outcomes::Ack),
            //     OutcomeKind::RowInserted => Outcome(Outcomes::RowId(codec::decode_row_id(&buffer, 1))),
            //     OutcomeKind::RowsUpdated => Outcome(Outcomes::RowsAffected(codec::decode_row_id(&buffer, 1))),
            // },
            PlatformFunctionType(pf) => PlatformFunction(pf.to_owned()),
            StringType(size) => StringValue(codec::decode_string(buffer, offset, size.to_size()).to_string()),
            StructureType(params) =>
                match HardStructure::from_parameters(params) {
                    Ok(structure) => StructureHard(structure),
                    Err(err) => ErrorValue(Errors::Exact(err.to_string()))
                }
            TableType(columns, ..) => TableValue(ModelRowCollection::construct(columns)),
            LazyType => Undefined,
            _ => Codec::decode_value(&buffer[offset..].to_vec())
        }
    }

    /// decodes the typed value based on the supplied data type and buffer
    pub fn from_buffer(
        data_type: &DataType,
        buffer: &mut ByteCodeCompiler,
    ) -> std::io::Result<TypedValue> {
        let tv = match data_type {
            ArrayType(..) => Array(buffer.next_array()?),
            ByteArrayType(..) => ByteArray(buffer.next_blob()),
            BooleanType => Boolean(buffer.next_bool()),
            DateType => DateValue(buffer.next_i64()),
            EnumType(labels) => {
                let index = buffer.next_u32() as usize;
                StringValue(labels[index].get_name().to_string())
            }
            ErrorType => ErrorValue(Exact(buffer.next_string())),
            FunctionType(columns) => Function {
                params: columns.to_owned(),
                code: Box::new(ByteCodeCompiler::disassemble(buffer)?),
            },
            LazyType => Null,
            NumberType(kind) =>
                Number(match *kind {
                    F32Kind => F32Value(buffer.next_f32()),
                    F64Kind => F64Value(buffer.next_f64()),
                    I8Kind => I8Value(buffer.next_i8()),
                    I16Kind => I16Value(buffer.next_i16()),
                    I32Kind => I32Value(buffer.next_i32()),
                    I64Kind => I64Value(buffer.next_i64()),
                    I128Kind => I128Value(buffer.next_i128()),
                    U8Kind => U8Value(buffer.next_u8()),
                    U16Kind => U16Value(buffer.next_u16()),
                    U32Kind => U32Value(buffer.next_u32()),
                    U64Kind => U64Value(buffer.next_u64()),
                    U128Kind => U128Value(buffer.next_u128()),
                    NaNKind => NaNValue
                }),
            OutcomeType(kind) => match kind {
                OutcomeKind::Acked => Outcome(Outcomes::Ack),
                OutcomeKind::RowInserted => Outcome(Outcomes::RowId(buffer.next_row_id())),
                OutcomeKind::RowsUpdated => Outcome(Outcomes::RowsAffected(buffer.next_row_id())),
            },
            PlatformFunctionType(pf) => PlatformFunction(pf.to_owned()),
            StringType(..) => StringValue(buffer.next_string()),
            StructureType(params) => StructureHard(buffer.next_struct_with_parameters(params)?),
            TableType(columns, ..) => TableValue(buffer.next_table_with_columns(columns)?),
            LazyType => Undefined,
        };
        Ok(tv)
    }

    pub fn from_json(j_value: serde_json::Value) -> Self {
        match j_value {
            serde_json::Value::Null => Null,
            serde_json::Value::Bool(b) => Boolean(b),
            serde_json::Value::Number(n) => n.as_f64().map(|v| Number(F64Value(v))).unwrap_or(Null),
            serde_json::Value::String(s) => StringValue(s),
            serde_json::Value::Array(a) => Array(a.iter().map(|v| Self::from_json(v.to_owned())).collect()),
            serde_json::Value::Object(args) =>
                match HardStructure::from_parameters(&args.iter()
                    .map(|(name, value)| {
                        let value = TypedValue::from_json(value.to_owned());
                        let data_type = value.get_type().to_type_declaration();
                        Parameter::new(name, data_type, Some(value.to_code()))
                    }).collect::<Vec<_>>()) {
                    Ok(structure) => StructureHard(structure),
                    Err(err) => ErrorValue(Exact(err.to_string()))
                }
        }
    }

    pub fn from_numeric(text: &str) -> std::io::Result<TypedValue> {
        let decimal_regex = Regex::new(DECIMAL_FORMAT).map_err(|e| cnv_error!(e))?;
        let int_regex = Regex::new(INTEGER_FORMAT).map_err(|e| cnv_error!(e))?;
        let number: String = text.chars()
            .filter(|c| *c != '_' && *c != ',')
            .collect();
        match number.trim() {
            s if int_regex.is_match(s) => Ok(Number(I64Value(s.parse().map_err(|e| cnv_error!(e))?))),
            s if decimal_regex.is_match(s) => Ok(Number(F64Value(s.parse().map_err(|e| cnv_error!(e))?))),
            s => Ok(StringValue(s.to_string()))
        }
    }

    pub fn is_numeric_value(value: &str) -> std::io::Result<bool> {
        let decimal_regex = Regex::new(DECIMAL_FORMAT)
            .map_err(|e| cnv_error!(e))?;
        Ok(decimal_regex.is_match(value))
    }

    fn millis_to_iso_date(millis: i64) -> Option<String> {
        let seconds = millis / 1000;
        let nanoseconds = (millis % 1000) * 1_000_000;
        let datetime = DateTime::from_timestamp(seconds, nanoseconds as u32)?;
        let iso_date = datetime.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
        Some(iso_date)
    }

    pub fn wrap_value(raw_value: &str) -> std::io::Result<Self> {
        let iso_date_regex = Regex::new(ISO_DATE_FORMAT).map_err(|e| cnv_error!(e))?;
        let uuid_regex = Regex::new(UUID_FORMAT).map_err(|e| cnv_error!(e))?;
        let result = match raw_value {
            "ack" => Outcome(Outcomes::Ack),
            s if s == "false" => Boolean(false),
            s if s == "null" => Null,
            s if s == "true" => Boolean(true),
            s if s == "undefined" => Undefined,
            s if s.is_empty() => Null,
            s if Self::is_numeric_value(s)? => Self::from_numeric(s)?,
            s if iso_date_regex.is_match(s) =>
                DateValue(DateTime::parse_from_rfc3339(s)
                    .map_err(|e| cnv_error!(e))?.timestamp_millis()),
            s if uuid_regex.is_match(s) => Number(U128Value(codec::decode_uuid(s)?)),
            s => StringValue(s.to_string()),
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
    pub fn contains(&self, value: &TypedValue) -> TypedValue {
        match &self {
            Array(items) => Boolean(items.contains(value)),
            StructureHard(hard) => match value {
                StringValue(name) => Boolean(hard.contains(name)),
                _ => Boolean(false)
            },
            StructureSoft(soft) => match value {
                StringValue(name) => Boolean(soft.contains(name)),
                _ => Boolean(false)
            },
            TableValue(mrc) => match value {
                StructureSoft(soft) => mrc.contains(&soft.to_row()),
                StructureHard(hard) => mrc.contains(&hard.to_row()),
                _ => Boolean(false)
            }
            _ => Boolean(false)
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        match self {
            Array(items) => {
                let mut bytes = Vec::new();
                bytes.extend(items.len().to_be_bytes());
                for item in items { bytes.extend(item.encode()); }
                bytes
            }
            PlatformFunction(pf) => pf.encode().unwrap_or(vec![]),
            ByteArray(bytes) => codec::encode_u8x_n(bytes.to_vec()),
            Boolean(ok) => vec![if *ok { 1 } else { 0 }],
            DateValue(number) => number.to_be_bytes().to_vec(),
            ErrorValue(err) => codec::encode_string(err.to_string().as_str()),
            // Function { params, code } =>
            //     Self::encode_function(params, code),
            // JSONValue(pairs) => {
            //     let mut bytes = Vec::new();
            //     for (name, value) in pairs {
            //         bytes.extend(name.bytes().collect::<Vec<u8>>());
            //         bytes.extend(Self::encode_value(value))
            //     }
            //     bytes
            // }
            NamespaceValue(a, b, c) =>
                codec::encode_string(format!("{a}.{b}.{c}").as_str()),
            Number(number) => number.encode(),
            // Outcome(Outcomes::Ack) | Null | Undefined => [0u8; 0].to_vec(),
            // Outcome(oc) => oc.encode().unwrap(),
            StringValue(string) => codec::encode_string(string),
            //StructureHard(structure) => codec::encode_string(structure.to_string().as_str()),
            TableValue(rc) => rc.encode(),
            other => Codec::encode_value(other).unwrap_or_else(|err| {
                error!("decode error: {err}");
                Vec::new()
            })
        }
    }

    pub fn get_type(&self) -> DataType {
        match self.to_owned() {
            Array(items) => ArrayType(Box::new(Inferences::infer_values(&items))),
            ByteArray(v) => ByteArrayType(FixedSize(v.len())),
            Boolean(..) => BooleanType,
            DateValue(..) => DateType,
            ErrorValue(..) => ErrorType,
            Function { params, .. } => FunctionType(params),
            NamespaceValue(..) => TableType(Vec::new(), BLOB),
            Null | Undefined => LazyType,
            Number(nv) => NumberType(nv.kind()),
            Outcome(oc) => match oc {
                Outcomes::Ack => OutcomeType(OutcomeKind::Acked),
                Outcomes::RowId(..) => OutcomeType(OutcomeKind::RowInserted),
                Outcomes::RowsAffected(..) => OutcomeType(OutcomeKind::RowsUpdated),
            },
            PlatformFunction(pf) => pf.get_type(),
            StructureHard(hard) => StructureType(hard.get_parameters()),
            StructureSoft(soft) => StructureType(soft.get_parameters()),
            StringValue(s) => StringType(FixedSize(s.len())),
            TableValue(mrc) => TableType(Parameter::from_columns(mrc.get_columns()), BLOB),
        }
    }

    pub fn get_type_name(&self) -> String {
        self.get_type().to_type_declaration().unwrap_or("".to_string())
    }

    pub fn hash_code(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        hasher.write(&self.encode());
        hasher.finish()
    }

    pub fn is_ok(&self) -> bool {
        matches!(self, Outcome(Outcomes::Ack) | Boolean(true) | Outcome(..) | NamespaceValue(..) | TableValue(..))
    }

    pub fn is_true(&self) -> bool {
        matches!(self, Outcome(Outcomes::Ack) | Boolean(true))
    }

    pub fn matches(&self, other: &Self) -> TypedValue {
        match (self, other) {
            (a, b) if *a == *b => Boolean(true),
            (StructureSoft(a), StructureSoft(b)) => {
                Boolean(a.to_hash_map() == b.to_hash_map())
            }
            (StructureHard(a), StructureHard(b)) => Boolean(a == b),
            _ => Boolean(false)
        }
    }

    pub fn to_bool(&self) -> bool {
        match self {
            Outcome(Outcomes::Ack) => true,
            Boolean(b) => *b,
            Outcome(oc) => oc.to_bool(),
            _ => false
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            ByteArray(bytes) => bytes.to_vec(),
            StringValue(s) => s.bytes().collect(),
            z => z.encode()
        }
    }

    pub fn to_code(&self) -> String {
        match self {
            Array(items) =>
                format!("[{}]", items.iter()
                    .map(|v| v.to_code())
                    .collect::<Vec<_>>().join(", ")),
            PlatformFunction(pf) => pf.to_code(),
            StructureHard(hs) => hs.to_code(),
            StructureSoft(ss) => ss.to_code(),
            StringValue(s) => format!("\"{s}\""),
            other => other.unwrap_value()
        }
    }

    pub fn to_f32(&self) -> f32 {
        match self {
            Outcome(Outcomes::Ack) => 1.,
            Boolean(true) => 1.,
            DateValue(dt) => *dt as f32,
            Number(nv) => nv.to_f32(),
            Outcome(oc) => oc.to_f32(),
            _ => 0.
        }
    }

    pub fn to_f64(&self) -> f64 {
        match self {
            Outcome(Outcomes::Ack) => 1.,
            Boolean(true) => 1.,
            DateValue(dt) => *dt as f64,
            Number(nv) => nv.to_f64(),
            Outcome(oc) => oc.to_f64(),
            _ => 0.
        }
    }

    pub fn to_i8(&self) -> i8 {
        match self {
            Outcome(Outcomes::Ack) => 1,
            Boolean(true) => 1,
            DateValue(dt) => *dt as i8,
            Number(nv) => nv.to_i8(),
            Outcome(oc) => oc.to_i8(),
            _ => 0
        }
    }

    pub fn to_i16(&self) -> i16 {
        match self {
            Outcome(Outcomes::Ack) => 1,
            Boolean(true) => 1,
            DateValue(dt) => *dt as i16,
            Number(nv) => nv.to_i16(),
            Outcome(oc) => oc.to_i16(),
            _ => 0
        }
    }

    pub fn to_i32(&self) -> i32 {
        match self {
            Outcome(Outcomes::Ack) => 1,
            Boolean(true) => 1,
            DateValue(dt) => *dt as i32,
            Number(nv) => nv.to_i32(),
            Outcome(oc) => oc.to_i32(),
            _ => 0
        }
    }

    pub fn to_i64(&self) -> i64 {
        match self {
            Outcome(Outcomes::Ack) => 1,
            Boolean(true) => 1,
            DateValue(dt) => *dt,
            Number(nv) => nv.to_i64(),
            Outcome(oc) => oc.to_i64(),
            _ => 0
        }
    }

    pub fn to_i128(&self) -> i128 {
        match self {
            Outcome(Outcomes::Ack) => 1,
            Boolean(true) => 1,
            DateValue(dt) => *dt as i128,
            Number(nv) => nv.to_i128(),
            Outcome(oc) => oc.to_i128(),
            _ => 0
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Outcome(Outcomes::Ack) => serde_json::Value::Bool(true),
            Array(items) =>
                serde_json::json!(items.iter().map(|v|v.to_json()).collect::<Vec<serde_json::Value>>()),
            PlatformFunction(nf) => serde_json::json!(nf),
            ByteArray(bytes) => serde_json::json!(bytes),
            Boolean(b) => serde_json::json!(b),
            DateValue(millis) => serde_json::json!(Self::millis_to_iso_date(*millis)),
            ErrorValue(message) => serde_json::json!(message),
            Function { params, code } => {
                let my_params = serde_json::Value::Array(params.iter()
                    .map(|c| c.to_json()).collect());
                serde_json::json!({ "params": my_params, "code": code.to_code() })
            }
            NamespaceValue(d, s, n) => serde_json::json!(format!("{d}.{s}.{n}")),
            Null => serde_json::Value::Null,
            Number(value) => value.to_json(),
            Outcome(oc) => serde_json::json!(oc.to_u64()),
            StringValue(string) => serde_json::json!(string),
            StructureHard(s) => s.to_json(),
            StructureSoft(s) => s.to_json(),
            TableValue(mrc) => {
                let rows = mrc.get_rows().iter()
                    .map(|r| r.to_row_js(mrc.get_columns()))
                    .collect::<Vec<_>>();
                serde_json::json!(rows)
            }
            Undefined => serde_json::Value::Null,
        }
    }

    pub fn to_table(&self) -> std::io::Result<Box<dyn RowCollection>> {
        match self {
            ErrorValue(message) => fail(message.to_string()),
            NamespaceValue(d, s, n) => {
                let ns = Namespace::new(d, s, n);
                let frc = FileRowCollection::open(&ns)?;
                Ok(Box::new(frc))
            }
            StructureHard(s) => Ok(Box::new(s.to_table())),
            StructureSoft(s) => Ok(Box::new(s.to_table())),
            TableValue(mrc) => Ok(Box::new(mrc.to_owned())),
            z => fail_value("Table", z)
        }
    }

    pub fn to_table_value(&self) -> TypedValue {
        match self {
            Array(items) => self.convert_array_to_table(items),
            ErrorValue(message) => ErrorValue(message.to_owned()),
            NamespaceValue(d, s, n) =>
                match FileRowCollection::open(&Namespace::new(d, s, n)) {
                    Ok(frc) => match frc.read_active_rows() {
                        Ok(rows) => TableValue(ModelRowCollection::from_rows(frc.get_columns().clone(), rows)),
                        Err(err) => ErrorValue(Exact(err.to_string()))
                    }
                    Err(err) => ErrorValue(Exact(err.to_string()))
                }
            StructureHard(s) => TableValue(s.to_table()),
            StructureSoft(s) => TableValue(s.to_table()),
            TableValue(mrc) => TableValue(mrc.to_owned()),
            z => ErrorValue(TypeMismatch("Table".into(), z.to_code()))
        }
    }

    fn convert_array_to_table(&self, items: &Vec<TypedValue>) -> TypedValue {
        // gather each struct in the array as a table
        fn value_parameters(value: &TypedValue) -> Vec<Parameter> {
            vec![Parameter::new("value".to_string(), value.get_type().to_type_declaration(), None)]
        }
        let tables = items.iter()
            .fold(Vec::new(), |mut tables, tv| match tv {
                StructureHard(hs) => {
                    tables.push(hs.to_table());
                    tables
                }
                StructureSoft(ss) => {
                    tables.push(ss.to_table());
                    tables
                }
                TableValue(mrc) => {
                    tables.push(mrc.to_owned());
                    tables
                }
                z => {
                    let mut mrc = ModelRowCollection::construct(&value_parameters(z));
                    mrc.append_row(Row::new(0, vec![z.clone()]));
                    tables.push(mrc);
                    tables
                }
            });

        // process the tables
        match tables.as_slice() {
            [] => ErrorValue(Exact("At least one struct is required.".into())),
            [mrc] => TableValue(mrc.to_owned()),
            rcs => {
                let columns = rcs[0].get_columns().to_owned();
                TableValue(rcs.iter().fold(
                    ModelRowCollection::new(columns),
                    |mut mrc, rc| {
                        match mrc.append_rows(rc.get_rows()) {
                            ErrorValue(err) => error!("{}", err),
                            _ => {}
                        }
                        mrc
                    }))
            }
        }
    }

    pub fn to_u8(&self) -> u8 {
        match self {
            Outcome(Outcomes::Ack) => 1,
            Boolean(true) => 1,
            Number(n) => n.to_u8(),
            Outcome(oc) => oc.to_u8(),
            _ => 0
        }
    }

    pub fn to_u16(&self) -> u16 {
        match self {
            Outcome(Outcomes::Ack) => 1,
            Boolean(true) => 1,
            Number(n) => n.to_u16(),
            Outcome(oc) => oc.to_u16(),
            _ => 0
        }
    }

    pub fn to_u32(&self) -> u32 {
        match self {
            Outcome(Outcomes::Ack) => 1,
            Boolean(true) => 1,
            Number(n) => n.to_u32(),
            Outcome(oc) => oc.to_u32(),
            _ => 0
        }
    }

    pub fn to_u64(&self) -> u64 {
        match self {
            Outcome(Outcomes::Ack) => 1,
            Boolean(true) => 1,
            Number(n) => n.to_u64(),
            Outcome(oc) => oc.to_u64(),
            _ => 0
        }
    }

    pub fn to_u128(&self) -> u128 {
        match self {
            Outcome(Outcomes::Ack) => 1,
            Boolean(true) => 1,
            Number(n) => n.to_u128(),
            Outcome(oc) => oc.to_u64() as u128,
            _ => 0
        }
    }

    pub fn to_usize(&self) -> usize {
        match self {
            Outcome(Outcomes::Ack) => 1,
            Boolean(true) => 1,
            Number(n) => n.to_usize(),
            Outcome(oc) => oc.to_usize(),
            _ => 0
        }
    }

    pub fn unwrap_value(&self) -> String {
        match self {
            Outcome(Outcomes::Ack) => "ack".to_string(),
            Array(items) => {
                let values: Vec<String> = items.iter().map(|v| v.unwrap_value()).collect();
                format!("[{}]", values.join(", "))
            }
            PlatformFunction(nf) => format!("{:?}", nf),
            ByteArray(bytes) => hex::encode(bytes),
            Boolean(b) => (if *b { "true" } else { "false" }).into(),
            DateValue(millis) => Self::millis_to_iso_date(*millis).unwrap_or("".into()),
            ErrorValue(message) => message.to_string(),
            Function { params, code } =>
                format!("(fn({}) => {})",
                        params.iter().map(|c| c.to_code()).collect::<Vec<String>>().join(", "),
                        code.to_code()),
            TableValue(mrc) => serde_json::json!(mrc.get_rows()).to_string(),
            NamespaceValue(d, s, n) => format!("{d}.{s}.{n}"),
            Null => "null".into(),
            Number(number) => number.unwrap_value(),
            Outcome(outcome) => outcome.to_string(),
            StringValue(string) => string.into(),
            StructureHard(structure) => structure.to_code(),
            StructureSoft(structure) => structure.to_code(),
            Undefined => "undefined".into(),
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
        let n = num_traits::pow(self.to_f64(), rhs.to_usize());
        Some(Number(F64Value(n)))
    }

    pub fn range(&self, rhs: &Self) -> Option<Self> {
        let mut values = Vec::new();
        for n in self.to_i64()..rhs.to_i64() { values.push(Number(I64Value(n))) }
        Some(Array(values))
    }
}

impl Add for TypedValue {
    type Output = TypedValue;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Array(a), Array(b)) => {
                let mut c = a.clone();
                c.extend(b);
                Array(c)
            }
            (Boolean(..), Outcome(Outcomes::Ack)) => Boolean(true),
            (Boolean(a), Boolean(b)) => Boolean(a | b),
            (ErrorValue(Various(mut errors0)), ErrorValue(Various(errors1))) => {
                errors0.extend(errors1);
                ErrorValue(Various(errors0))
            }
            (ErrorValue(err), ErrorValue(Various(mut errors))) => {
                errors.push(err);
                ErrorValue(Various(errors))
            }
            (ErrorValue(Various(mut errors)), ErrorValue(err)) => {
                errors.push(err);
                ErrorValue(Various(errors))
            }
            (ErrorValue(a), ErrorValue(b)) => ErrorValue(Various(vec![a, b])),
            (ErrorValue(a), _) => ErrorValue(a),
            (_, ErrorValue(b)) => ErrorValue(b),
            (Number(a), Number(b)) => Number(a + b),
            (Outcome(Outcomes::Ack), Boolean(..)) => Boolean(true),
            (Outcome(a), Outcome(b)) => Outcome(Outcomes::RowsAffected(a.to_update_count() + b.to_update_count())),
            (StringValue(a), StringValue(b)) => StringValue(a + b.as_str()),
            (TableValue(a), StructureHard(b)) => {
                let mut mrc = match ModelRowCollection::from_table(Box::new(&a)) {
                    Ok(mrc) => mrc,
                    Err(err) => return ErrorValue(Exact(err.to_string()))
                };
                match mrc.append_row(b.to_row()) {
                    ErrorValue(s) => ErrorValue(s),
                    _ => TableValue(mrc),
                }
            }
            (TableValue(a), TableValue(b)) =>
                match ModelRowCollection::combine(a.get_columns().to_owned(), vec![&a, &b]) {
                    Ok(mrc) => TableValue(mrc),
                    Err(err) => ErrorValue(Exact(err.to_string()))
                },
            (a, b) => ErrorValue(TypeMismatch(a.unwrap_value(), b.unwrap_value()))
        }
    }
}

impl BitAnd for TypedValue {
    type Output = TypedValue;

    fn bitand(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Number(a), Number(b)) => Number(a & b),
            _ => Undefined
        }
    }
}

impl BitOr for TypedValue {
    type Output = TypedValue;

    fn bitor(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Number(a), Number(b)) => Number(a | b),
            _ => Undefined
        }
    }
}

impl BitXor for TypedValue {
    type Output = TypedValue;

    fn bitxor(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Number(a), Number(b)) => Number(a ^ b),
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
            _ => Undefined
        }
    }
}

impl Index<usize> for TypedValue {
    type Output = TypedValue;

    fn index(&self, index: usize) -> &Self::Output {
        match self {
            Array(items) =>
                if index < items.len() { &items[index] } else { &Undefined },
            Null => &Null,
            TableValue(mrc) => Box::leak(Box::new(fetch(mrc, index))),
            Undefined => &Undefined,
            _ => &Undefined,
        }
    }
}

fn fetch(mrc: &ModelRowCollection, index: usize) -> TypedValue {
    let columns = mrc.get_columns();
    match mrc.read_row(index) {
        Ok((row, meta)) => {
            StructureHard(match meta.is_allocated {
                true => row.to_struct(columns),
                false => Row::empty(columns).to_struct(columns),
            })
        }
        Err(err) => ErrorValue(Exact(err.to_string())),
    }
}

impl Mul for TypedValue {
    type Output = TypedValue;

    fn mul(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Array(a), Number(n)) => {
                let mut b = a.clone();
                for _ in 1..n.to_usize() {
                    b.extend(a.clone());
                }
                Array(b)
            }
            (Number(a), Number(b)) => Number(a * b),
            _ => Undefined
        }
    }
}

impl Neg for TypedValue {
    type Output = TypedValue;

    fn neg(self) -> Self::Output {
        let error: fn(String) -> TypedValue = |s| ErrorValue(Exact(format!("{} cannot be negated", s)));
        match self {
            Array(v) => Array(v.iter().map(|tv| tv.to_owned().neg()).collect::<Vec<_>>()),
            PlatformFunction(..) => error("BackDoor".into()),
            ByteArray(..) => error("BLOB".into()),
            Boolean(n) => Boolean(!n),
            DateValue(v) => Number(I64Value(-v)),
            ErrorValue(msg) => ErrorValue(msg),
            Function { .. } => error("Function".into()),
            NamespaceValue(a, b, c) => error(format!("ns({}, {}, {})", a, b, c)),
            Null => Null,
            Number(nv) => Number(match nv {
                F32Value(n) => F32Value(-n),
                F64Value(n) => F64Value(-n),
                I8Value(n) => I8Value(-n),
                I16Value(n) => I16Value(-n),
                I32Value(n) => I32Value(-n),
                I64Value(n) => I64Value(-n),
                I128Value(n) => I128Value(-n),
                NaNValue => NaNValue,
                U8Value(n) => I16Value(-(n as i16)),
                U16Value(n) => I32Value(-(n as i32)),
                U32Value(n) => I64Value(-(n as i64)),
                U64Value(n) => I128Value(-(n as i128)),
                U128Value(z) => I128Value(-(z as i128)),
            }),
            Outcome(oc) => Number(I64Value(-oc.to_i64())),
            StringValue(..) => error("String".into()),
            StructureHard(..) => error("Structure".into()),
            StructureSoft(..) => error("Structure".into()),
            TableValue(..) => error("Table".into()),
            Undefined => Undefined,
        }
    }
}

impl Not for TypedValue {
    type Output = TypedValue;

    fn not(self) -> Self::Output {
        match self {
            Boolean(v) => Boolean(!v),
            Number(a) => Number(-a),
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
            _ => Undefined
        }
    }
}

impl PartialOrd for TypedValue {
    fn partial_cmp(&self, rhs: &Self) -> Option<Ordering> {
        match (&self, &rhs) {
            (Array(a), Array(b)) => a.partial_cmp(b),
            (ByteArray(a), ByteArray(b)) => a.partial_cmp(b),
            (Boolean(a), Boolean(b)) => a.partial_cmp(b),
            (DateValue(a), DateValue(b)) => a.partial_cmp(b),
            (Number(a), Number(b)) => a.partial_cmp(b),
            (Outcome(a), Outcome(b)) => a.partial_cmp(b),
            (StringValue(a), StringValue(b)) => a.partial_cmp(b),
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
            _ => Undefined
        }
    }
}

impl Shr for TypedValue {
    type Output = TypedValue;

    fn shr(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Number(a), Number(b)) => Number(a >> b),
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
            (Outcome(a), Outcome(b)) =>
                Outcome(Outcomes::RowsAffected({
                    let a = a.to_update_count() as i64;
                    let b = b.to_update_count() as i64;
                    abs(a - b)
                } as usize)),
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
    fn test_decode() {
        let buf: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 5, b'H', b'e', b'l', b'l', b'o'];
        assert_eq!(TypedValue::decode(&StringType(StorageTypes::FixedSize(5)), &buf, 0), StringValue("Hello".into()))
    }

    #[test]
    fn test_addition() {
        use NumberValue::*;
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
        use NumberValue::*;
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
        let array = Array(vec![
            StringValue("ABC".into()),
            StringValue("R2X2".into()),
            StringValue("123".into()),
            StringValue("Hello".into()),
        ]);
        assert_eq!(array.contains(&StringValue("123".into())), Boolean(true))
    }

    #[test]
    fn test_from_json() {
        let structure = HardStructure::from_parameters(&vec![
            Parameter::new("name", Some("String(4)".into()), Some("John".into())),
            Parameter::new("age", Some("i64".into()), Some("40".into()))
        ]).unwrap();
        let js_value0: Value = serde_json::from_str(r##"{"name":"John","age":40}"##).unwrap();
        let js_value1: Value = serde_json::from_str(structure.to_string().as_str()).unwrap();
        assert_eq!(js_value1, js_value0)
    }

    #[test]
    fn test_json_object_contains_string() {
        let js_value = StructureSoft(SoftStructure::new(&vec![
            ("name", StringValue("symbol".to_string())),
            ("param_type", StringValue("String(8)".to_string())),
            ("default_value", Null),
        ]));
        assert_eq!(js_value.contains(&StringValue("name".into())), Boolean(true))
    }

    #[test]
    fn test_table_contains_json_object() {
        let phys_columns = make_quote_columns();
        let table = TableValue(ModelRowCollection::from_rows(phys_columns.clone(), vec![
            make_quote(0, "ABC", "AMEX", 11.77),
            make_quote(1, "UNO", "OTC", 0.2456),
            make_quote(2, "BIZ", "NYSE", 23.67),
            make_quote(3, "GOTO", "OTC", 0.1421),
            make_quote(4, "BOOM", "NASDAQ", 56.87),
            make_quote(5, "TRX", "NASDAQ", 7.9311),
        ]));
        let js_obj = StructureSoft(SoftStructure::new(&vec![
            ("symbol", StringValue("BIZ".into())),
            ("exchange", StringValue("NYSE".into())),
            ("last_sale", Number(F64Value(23.67))),
        ]));
        assert_eq!(table.contains(&js_obj), Boolean(true))
    }

    #[test]
    fn test_table_contains_struct_object() {
        let phys_columns = make_quote_columns();
        let table = TableValue(ModelRowCollection::from_rows(phys_columns.clone(), vec![
            make_quote(0, "ABC", "AMEX", 11.77),
            make_quote(1, "UNO", "OTC", 0.2456),
            make_quote(2, "GOTO", "OTC", 0.1421),
            make_quote(3, "BOOM", "NASDAQ", 56.87),
            make_quote(4, "TRX", "NASDAQ", 7.9311),
            make_quote(5, "BIZ", "NYSE", 23.66),
        ]));
        let obj = StructureHard(HardStructure::new(phys_columns, vec![
            StringValue("BIZ".into()),
            StringValue("NYSE".into()),
            Number(F64Value(23.66)),
        ]));
        assert_eq!(table.contains(&obj), Boolean(true))
    }

    #[test]
    fn test_table_index() {
        let phys_columns = make_quote_columns();
        let mrc = ModelRowCollection::from_rows(phys_columns.clone(), vec![
            make_quote(0, "ABC", "AMEX", 11.77),
            make_quote(1, "UNO", "OTC", 0.2456),
            make_quote(2, "BIZ", "NYSE", 23.66),
            make_quote(3, "GOTO", "OTC", 0.1428),
            make_quote(4, "BOOM", "NASDAQ", 56.87)]);
        assert_eq!(TableValue(mrc)[0], StructureHard(HardStructure::new(
            phys_columns, vec![
                StringValue("ABC".into()),
                StringValue("AMEX".into()),
                Number(F64Value(11.77)),
            ],
        )))
    }

    #[test]
    fn test_eq() {
        use NumberValue::*;
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
        use NumberValue::*;
        assert_ne!(U8Value(0xCE), U8Value(0x00));
        assert_ne!(I16Value(0x7ACE), I16Value(0xACE));
        assert_ne!(I32Value(0x1111_BEEF), I32Value(0xBEEF));
        assert_ne!(I64Value(0x5555_FACE_CAFE_BABE), I64Value(0xFACE_CAFE_BABE));
        assert_ne!(F32Value(45.0), F32Value(45.7));
        assert_ne!(F64Value(99.142857), F64Value(19.48));
    }

    #[test]
    fn test_gt() {
        use NumberValue::*;
        assert!(Array(vec![Number(U8Value(0xCE)), Number(U8Value(0xCE))]) > Array(vec![Number(U8Value(0x23)), Number(U8Value(0xBE))]));
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
        verify_encode_decode(&Outcome(Outcomes::Ack));
        verify_encode_decode(&Outcome(Outcomes::RowId(1013)));
        verify_encode_decode(&Outcome(Outcomes::RowsAffected(19)));
    }

    #[test]
    fn test_to_json() {
        use NumberValue::*;
        verify_to_json(Outcome(Outcomes::Ack), serde_json::Value::Bool(true));
        verify_to_json(Boolean(false), serde_json::Value::Bool(false));
        verify_to_json(Boolean(true), serde_json::Value::Bool(true));
        verify_to_json(DateValue(1709163679081), serde_json::Value::String("2024-02-28T23:41:19.081Z".into()));
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
        verify_wrap_unwrap("ack", Outcome(Outcomes::Ack));
        verify_wrap_unwrap("false", Boolean(false));
        verify_wrap_unwrap("true", Boolean(true));
        verify_wrap_unwrap("2024-02-28T23:41:19.081Z", DateValue(1709163679081));
        verify_wrap_unwrap("100.1", Number(F64Value(100.1)));
        verify_wrap_unwrap("100", Number(I64Value(100)));
        verify_wrap_unwrap("null", Null);
        verify_wrap_unwrap("Hello World", StringValue("Hello World".into()));
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
        let actual = TypedValue::decode(&data_type, &bytes, 0);
        assert_eq!(actual, *expected)
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