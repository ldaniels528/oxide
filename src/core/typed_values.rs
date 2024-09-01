////////////////////////////////////////////////////////////////////
// typed values module
////////////////////////////////////////////////////////////////////

use std::{i32, io};
use std::cmp::Ordering;
use std::collections::{Bound, HashMap};
use std::fmt::Display;
use std::hash::{DefaultHasher, Hasher};
use std::ops::*;

use chrono::DateTime;
use num_traits::ToPrimitive;
use regex::Regex;
use reqwest::Response;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use shared_lib::{fail, RowJs};

use crate::{cnv_error, serialization};
use crate::byte_buffer::ByteBuffer;
use crate::codec;
use crate::compiler::fail_value;
use crate::data_types::*;
use crate::data_types::DataType::*;
use crate::expression::{ACK, Expression};
use crate::file_row_collection::FileRowCollection;
use crate::model_row_collection::ModelRowCollection;
use crate::namespaces::Namespace;
use crate::row_collection::RowCollection;
use crate::rows::Row;
use crate::serialization::disassemble;
use crate::server::ColumnJs;
use crate::structure::Structure;
use crate::typed_values::BackDoorFunction::{Assert, Eval, Matches, StdErr, StdOut, SysCall, ToCSV, TypeOf};
use crate::typed_values::TypedValue::*;

const ISO_DATE_FORMAT: &str =
    r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?(([+-]\d\d:\d\d)|Z)?$";
const DECIMAL_FORMAT: &str = r"^-?(?:\d+(?:_\d)*|\d+)(?:\.\d+)?$";
const INTEGER_FORMAT: &str = r"^-?(?:\d+(?:_\d)*)?$";
const UUID_FORMAT: &str =
    "^[0-9a-fA-F]{8}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{12}$";

/// Represents a backdoor (hook) for calling native functions
#[repr(u8)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum BackDoorFunction {
    Assert = 0,
    Eval = 1,
    Matches = 2,
    StdErr = 3,
    StdOut = 4,
    SysCall = 5,
    ToCSV = 6,
    TypeOf = 7,
}

impl BackDoorFunction {
    pub fn decode(code: u8) -> Self {
        Self::from(Self::values()[code as usize].to_owned())
    }

    pub fn ordinal(&self) -> u8 {
        self.to_owned() as u8
    }

    pub fn values() -> Vec<BackDoorFunction> {
        vec![
            Assert, Eval, Matches, StdErr, StdOut, SysCall,
            ToCSV, TypeOf,
        ]
    }
}

/// Basic value unit
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum TypedValue {
    Ack,
    Array(Vec<TypedValue>),
    BackDoor(BackDoorFunction),
    BLOB(Vec<u8>),
    Boolean(bool),
    CLOB(Vec<char>),
    DateValue(i64),
    ErrorValue(String),
    Float32Value(f32),
    Float64Value(f64),
    Function { params: Vec<ColumnJs>, code: Box<Expression> },
    Int8Value(i8),
    Int16Value(i16),
    Int32Value(i32),
    Int64Value(i64),
    Int128Value(i128),
    JSONValue(Vec<(String, TypedValue)>),
    NamespaceValue(String),
    RowsAffected(usize),
    StringValue(String),
    StructureValue(Structure),
    TableValue(ModelRowCollection),
    TupleValue(Vec<TypedValue>),
    UInt8Value(u8),
    UInt16Value(u16),
    UInt32Value(u32),
    UInt64Value(u64),
    UInt128Value(u128),
    UUIDValue([u8; 16]),
    Null,
    Undefined,
}

impl TypedValue {
    /// returns true, if:
    /// 1. the host value is an array, and the item value is found within it,
    /// 2. the host value is a table, and the item value matches a row found within it,
    pub fn contains(&self, value: &TypedValue) -> TypedValue {
        match &self {
            Array(items) => Boolean(items.contains(value)),
            JSONValue(items) =>
                match value {
                    StringValue(name) => items.iter()
                        .find(|(k, v)| k == name)
                        .map(|_| Boolean(true))
                        .unwrap_or(Boolean(false)),
                    _ => Boolean(false)
                },
            TableValue(mrc) =>
                match value {
                    JSONValue(tuples) =>
                        mrc.contains(&Row::from_tuples(0, mrc.get_columns(), tuples)),
                    _ => Boolean(false)
                }
            _ => Boolean(false)
        }
    }

    /// decodes the typed value based on the supplied data type and buffer
    pub fn decode(data_type: &DataType, buffer: &Vec<u8>, offset: usize) -> Self {
        match data_type {
            AckType => Ack,
            BackDoorType => codec::decode_u8(buffer, offset, |b| BackDoor(BackDoorFunction::decode(b))),
            BLOBType(_size) => BLOB(Vec::new()),
            BooleanType => codec::decode_u8(buffer, offset, |b| Boolean(b == 1)),
            CLOBType(_size) => CLOB(Vec::new()),
            DateType => codec::decode_u8x8(buffer, offset, |b| DateValue(i64::from_be_bytes(b))),
            EnumType(labels) => {
                let index = codec::decode_u8x4(buffer, offset, |b| i32::from_be_bytes(b));
                StringValue(labels[index as usize].to_string())
            }
            ErrorType => ErrorValue(codec::decode_string(buffer, offset, 255).to_string()),
            Float32Type => codec::decode_u8x4(buffer, offset, |b| Float32Value(f32::from_be_bytes(b))),
            Float64Type => codec::decode_u8x8(buffer, offset, |b| Float64Value(f64::from_be_bytes(b))),
            FunctionType(columns) => {
                let mut buf = ByteBuffer::wrap(buffer.clone());
                buf.move_rel(offset as isize);
                match disassemble(&mut buf) {
                    Ok(expr) =>
                        Function { params: columns.to_owned(), code: Box::new(expr) },
                    Err(err) => ErrorValue(err.to_string())
                }
            }
            Int8Type => codec::decode_u8(buffer, offset, |b| Int8Value(b.to_i8().unwrap())),
            Int16Type => codec::decode_u8x2(buffer, offset, |b| Int16Value(i16::from_be_bytes(b))),
            Int32Type => codec::decode_u8x4(buffer, offset, |b| Int32Value(i32::from_be_bytes(b))),
            Int64Type => codec::decode_u8x8(buffer, offset, |b| Int64Value(i64::from_be_bytes(b))),
            Int128Type => codec::decode_u8x16(buffer, offset, |b| Int128Value(i128::from_be_bytes(b))),
            JSONType => JSONValue(Vec::new()), // TODO implement me
            RowsAffectedType => RowsAffected(codec::decode_row_id(&buffer, 1)),
            StringType(size) => StringValue(codec::decode_string(buffer, offset, *size).to_string()),
            StructureType(columns) =>
                match Structure::from_logical_columns(columns) {
                    Ok(structure) => StructureValue(structure),
                    Err(err) => ErrorValue(err.to_string())
                }
            TableType(columns) => TableValue(ModelRowCollection::construct(columns)),
            UInt8Type => codec::decode_u8(buffer, offset, |b| UInt8Value(b)),
            UInt16Type => codec::decode_u8x2(buffer, offset, |b| UInt16Value(u16::from_be_bytes(b))),
            UInt32Type => codec::decode_u8x4(buffer, offset, |b| UInt32Value(u32::from_be_bytes(b))),
            UInt64Type => codec::decode_u8x8(buffer, offset, |b| UInt64Value(u64::from_be_bytes(b))),
            UInt128Type => codec::decode_u8x16(buffer, offset, |b| UInt128Value(u128::from_be_bytes(b))),
            UUIDType => codec::decode_u8x16(buffer, offset, |b| UUIDValue(b))
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        Self::encode_value(self)
    }

    pub fn encode_value(value: &TypedValue) -> Vec<u8> {
        match value {
            Ack | Null | Undefined => [0u8; 0].to_vec(),
            Array(items) => {
                let mut bytes = Vec::new();
                bytes.extend(items.len().to_be_bytes());
                for item in items { bytes.extend(item.encode()); }
                bytes
            }
            BackDoor(nf) => vec![nf.ordinal()],
            BLOB(bytes) => codec::encode_u8x_n(bytes.to_vec()),
            Boolean(ok) => vec![if *ok { 1 } else { 0 }],
            CLOB(chars) => codec::encode_chars(chars.to_vec()),
            DateValue(number) => number.to_be_bytes().to_vec(),
            ErrorValue(message) => codec::encode_string(message),
            Float32Value(number) => number.to_be_bytes().to_vec(),
            Float64Value(number) => number.to_be_bytes().to_vec(),
            Function { params, code } =>
                Self::encode_function(params, code),
            Int8Value(number) => number.to_be_bytes().to_vec(),
            Int16Value(number) => number.to_be_bytes().to_vec(),
            Int32Value(number) => number.to_be_bytes().to_vec(),
            Int64Value(number) => number.to_be_bytes().to_vec(),
            Int128Value(number) => number.to_be_bytes().to_vec(),
            JSONValue(pairs) => {
                let mut bytes = Vec::new();
                for (name, value) in pairs {
                    bytes.extend(name.bytes().collect::<Vec<u8>>());
                    bytes.extend(Self::encode_value(value))
                }
                bytes
            }
            TableValue(rc) => rc.encode(),
            RowsAffected(id) => id.to_be_bytes().to_vec(),
            StringValue(string) => codec::encode_string(string),
            StructureValue(structure) => codec::encode_string(structure.to_string().as_str()),
            NamespaceValue(path) => codec::encode_string(path),
            TupleValue(items) => {
                let mut bytes = Vec::new();
                bytes.extend(items.len().to_be_bytes());
                for item in items { bytes.extend(item.encode()); }
                bytes
            }
            UInt8Value(number) => number.to_be_bytes().to_vec(),
            UInt16Value(number) => number.to_be_bytes().to_vec(),
            UInt32Value(number) => number.to_be_bytes().to_vec(),
            UInt64Value(number) => number.to_be_bytes().to_vec(),
            UInt128Value(number) => number.to_be_bytes().to_vec(),
            UUIDValue(guid) => guid.to_vec(),
        }
    }

    fn encode_function(params: &Vec<ColumnJs>, code: &Expression) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend(params.len().to_be_bytes());
        for param in params { bytes.extend(param.encode()); }
        bytes.extend(code.encode());
        bytes
    }

    /// decodes the typed value based on the supplied data type and buffer
    pub fn from_buffer(
        data_type: &DataType,
        buffer: &mut ByteBuffer,
    ) -> std::io::Result<TypedValue> {
        let tv = match data_type {
            AckType => Ack,
            BackDoorType => BackDoor(buffer.next_backdoor_fn()),
            BLOBType(..) => BLOB(buffer.next_blob()),
            BooleanType => Boolean(buffer.next_bool()),
            CLOBType(..) => CLOB(buffer.next_clob()),
            DateType => DateValue(buffer.next_i64()),
            EnumType(labels) => {
                let index = buffer.next_u32();
                StringValue(labels[index as usize].to_string())
            }
            ErrorType => ErrorValue(buffer.next_string()),
            Float32Type => Float32Value(buffer.next_f32()),
            Float64Type => Float64Value(buffer.next_f64()),
            FunctionType(columns) => Function {
                params: columns.to_owned(),
                code: Box::new(disassemble(buffer)?),
            },
            Int8Type => Int8Value(buffer.next_i8()),
            Int16Type => Int16Value(buffer.next_i16()),
            Int32Type => Int32Value(buffer.next_i32()),
            Int64Type => Int64Value(buffer.next_i64()),
            Int128Type => Int128Value(buffer.next_i128()),
            JSONType => JSONValue(buffer.next_json()?),
            RowsAffectedType => RowsAffected(buffer.next_row_id()),
            StringType(..) => StringValue(buffer.next_string()),
            StructureType(columns) => StructureValue(buffer.next_struct_with_columns(columns)?),
            TableType(columns) => TableValue(buffer.next_table_with_columns(columns)?),
            UInt8Type => UInt8Value(buffer.next_u8()),
            UInt16Type => UInt16Value(buffer.next_u16()),
            UInt32Type => UInt32Value(buffer.next_u32()),
            UInt64Type => UInt64Value(buffer.next_u64()),
            UInt128Type => UInt128Value(buffer.next_u128()),
            UUIDType => UUIDValue(buffer.next_uuid()),
        };
        Ok(tv)
    }

    pub async fn from_response(response: Response) -> Self {
        if response.status().is_success() {
            match response.text().await {
                Ok(body) => StringValue(body),
                Err(err) => ErrorValue(format!("Error reading response body: {}", err)),
            }
        } else {
            ErrorValue(format!("Request failed with status: {}", response.status()))
        }
    }

    pub fn get_raw_value(&self) -> String {
        match self {
            Array(items) => {
                let values: Vec<String> = items.iter().map(|v| v.get_raw_value()).collect();
                format!("[{}]", values.join(", "))
            }
            JSONValue(pairs) => {
                let values: Vec<String> = pairs.iter()
                    .map(|(k, v)| format!("\"{}\":{}", k, v.get_raw_value()))
                    .collect();
                format!("{{{}}}", values.join(", "))
            }
            StringValue(s) => format!("\"{s}\""),
            TupleValue(items) => {
                let values: Vec<String> = items.iter().map(|v| v.get_raw_value()).collect();
                format!("({})", values.join(", "))
            }
            z => z.unwrap_value()
        }
    }

    fn intercept_unknowns(a: &TypedValue, b: &TypedValue) -> Option<TypedValue> {
        match (a, b) {
            (Ack, Boolean(v)) => Some(Boolean(*v)),
            (Boolean(v), Ack) => Some(Boolean(*v)),
            (Undefined, _) => Some(Undefined),
            (_, Undefined) => Some(Undefined),
            (Null, _) => Some(Null),
            (_, Null) => Some(Null),
            _ => None
        }
    }

    pub fn is_boolean(&self) -> bool {
        matches!(self, Ack | Boolean(..))
    }

    pub fn is_compatible(&self, other: &TypedValue) -> bool {
        let (a, b) = (self, other);
        a == b
            || a.unwrap_value() == b.unwrap_value()
            || a.is_boolean() && b.is_boolean()
            || a.is_numeric() && b.is_numeric()
            || a.is_string() && b.is_string()
    }

    pub fn is_false(&self) -> bool { !self.is_true() }

    pub fn is_numeric(&self) -> bool {
        use TypedValue::*;
        matches!(self, Ack | RowsAffected(..)
            | Int8Value(..) | Int16Value(..) | Int32Value(..)
            | Int64Value(..) | Int128Value(..)
            | UInt8Value(..) | UInt16Value(..) | UInt32Value(..)
            | UInt64Value(..) | UInt128Value(..))
    }

    pub fn is_ok(&self) -> bool {
        matches!(self, Ack | Boolean(true) | RowsAffected(..) | NamespaceValue(..) | TableValue(..))
    }

    pub fn is_string(&self) -> bool {
        matches!(self, StringValue(..))
    }

    pub fn is_true(&self) -> bool {
        matches!(self, Ack | Boolean(true))
    }

    fn millis_to_iso_date(millis: i64) -> Option<String> {
        let seconds = millis / 1000;
        let nanoseconds = (millis % 1000) * 1_000_000;
        let datetime = DateTime::from_timestamp(seconds, nanoseconds as u32)?;
        let iso_date = datetime.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
        Some(iso_date)
    }

    pub fn from_json(j_value: serde_json::Value) -> Self {
        match j_value {
            serde_json::Value::Null => Null,
            serde_json::Value::Bool(b) => Boolean(b),
            serde_json::Value::Number(n) => n.as_f64().map(Float64Value).unwrap_or(Null),
            serde_json::Value::String(s) => StringValue(s),
            serde_json::Value::Array(a) => Array(a.iter().map(|v| Self::from_json(v.to_owned())).collect()),
            serde_json::Value::Object(..) => todo!()
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

    pub fn get_type_name(&self) -> String {
        let result = match *self {
            Ack => "Ack",
            Undefined => "Undefined",
            Null => "Null",
            BackDoor(..) => "BackDoor",
            Function { .. } => "Function",
            Array(..) => "Array",
            BLOB(..) => "BLOB",
            Boolean(..) => "Boolean",
            CLOB(..) => "CLOB",
            DateValue(..) => "Date",
            ErrorValue(..) => "Error",
            JSONValue(..) => "JSON",
            Float32Value(..) => "f32",
            Float64Value(..) => "f64",
            Int8Value(..) => "i8",
            Int16Value(..) => "i16",
            Int32Value(..) => "i32",
            Int64Value(..) => "i64",
            Int128Value(..) => "i128",
            RowsAffected(..) => "RowsAffected",
            StringValue(..) => "String",
            StructureValue(..) => "Struct",
            NamespaceValue(..) => "TablePtr",
            TableValue(..) => "Table",
            TupleValue(..) => "Tuple",
            UInt8Value(..) => "u8",
            UInt16Value(..) => "u16",
            UInt32Value(..) => "u32",
            UInt64Value(..) => "u64",
            UInt128Value(..) => "u128",
            UUIDValue(..) => "UUID",
        };
        result.to_string()
    }

    pub fn hash_code(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        hasher.write(&self.encode());
        hasher.finish()
    }

    pub fn matches(&self, other: &Self) -> TypedValue {
        match (self, other) {
            (a, b) if *a == *b => Boolean(true),
            (JSONValue(aa), JSONValue(bb)) => {
                let ha: HashMap<_, _> = aa.to_owned().into_iter().collect();
                let hb: HashMap<_, _> = bb.to_owned().into_iter().collect();
                Boolean(ha == hb)
            }
            _ => Boolean(false)
        }
    }

    pub fn ordinal(&self) -> u8 {
        match *self {
            Ack => T_ACK,
            Undefined => T_UNDEFINED,
            Null => T_NULL,
            Function { .. } => T_FUNCTION,
            Array(..) => T_ARRAY,
            BackDoor(..) => T_BACK_DOOR,
            BLOB(..) => T_BLOB,
            Boolean(..) => T_BOOLEAN,
            CLOB(..) => T_CLOB,
            DateValue(..) => T_DATE,
            ErrorValue(..) => T_ERROR,
            JSONValue(..) => T_JSON_OBJECT,
            Float32Value(..) => T_FLOAT32,
            Float64Value(..) => T_FLOAT64,
            Int8Value(..) => T_INT8,
            Int16Value(..) => T_INT16,
            Int32Value(..) => T_INT32,
            Int64Value(..) => T_INT64,
            Int128Value(..) => T_INT128,
            NamespaceValue(..) => T_NAMESPACE,
            RowsAffected(..) => T_ROWS_AFFECTED,
            StringValue(..) => T_STRING,
            StructureValue(..) => T_STRUCTURE,
            TableValue(..) => T_TABLE_VALUE,
            TupleValue(..) => T_TUPLE,
            UInt8Value(..) => T_UINT8,
            UInt16Value(..) => T_UINT16,
            UInt32Value(..) => T_UINT32,
            UInt64Value(..) => T_UINT64,
            UInt128Value(..) => T_UINT128,
            UUIDValue(..) => T_UUID,
        }
    }

    pub fn to_code(&self) -> String {
        match self {
            Undefined => "undefined".to_string(),
            StringValue(s) => format!("\"{}\"", s),
            v => v.unwrap_value()
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Ack => serde_json::Value::Bool(true),
            Array(items) =>
                serde_json::json!(items.iter().map(|v|v.to_json()).collect::<Vec<serde_json::Value>>()),
            BackDoor(nf) => serde_json::json!(nf),
            BLOB(bytes) => serde_json::json!(bytes),
            Boolean(b) => serde_json::json!(b),
            CLOB(chars) => serde_json::json!(chars),
            DateValue(millis) => serde_json::json!(Self::millis_to_iso_date(*millis)),
            ErrorValue(message) => serde_json::json!(message),
            Float32Value(number) => serde_json::json!(number),
            Float64Value(number) => serde_json::json!(number),
            Function { params, code } => {
                let my_params = serde_json::Value::Array(params.iter().map(|c| c.to_json()).collect());
                let my_code = code.to_code();
                serde_json::json!({ "params": my_params, "code": my_code })
            }
            Int8Value(number) => serde_json::json!(number),
            Int16Value(number) => serde_json::json!(number),
            Int32Value(number) => serde_json::json!(number),
            Int64Value(number) => serde_json::json!(number),
            Int128Value(number) => serde_json::json!(number),
            JSONValue(pairs) => serde_json::json!(pairs),
            Null => serde_json::Value::Null,
            RowsAffected(number) => serde_json::json!(number),
            StringValue(string) => serde_json::json!(string),
            StructureValue(structure) => serde_json::json!(structure),
            NamespaceValue(path) => serde_json::json!(path),
            TableValue(mrc) => {
                let rows = mrc.get_rows().iter()
                    .map(|r| r.to_row_js())
                    .collect::<Vec<RowJs>>();
                serde_json::json!(rows)
            }
            TupleValue(items) =>
                serde_json::json!(items.iter().map(|v|v.to_json()).collect::<Vec<serde_json::Value>>()),
            Undefined => serde_json::Value::Null,
            UInt8Value(number) => serde_json::json!(number),
            UInt16Value(number) => serde_json::json!(number),
            UInt32Value(number) => serde_json::json!(number),
            UInt64Value(number) => serde_json::json!(number),
            UInt128Value(number) => serde_json::json!(number),
            UUIDValue(guid) => serde_json::json!(Uuid::from_bytes(*guid).to_string()),
        }
    }

    pub fn to_table(&self) -> std::io::Result<Box<dyn RowCollection>> {
        match self {
            NamespaceValue(path) => {
                let ns = Namespace::parse(path.as_str())?;
                let frc = FileRowCollection::open(&ns)?;
                Ok(Box::new(frc))
            }
            TableValue(mrc) => Ok(Box::new(mrc.to_owned())),
            ErrorValue(message) => fail(message),
            z => fail_value("Table", z)
        }
    }

    pub fn unwrap_value(&self) -> String {
        match self {
            Ack => "ack".to_string(),
            Array(items) => {
                let values: Vec<String> = items.iter().map(|v| v.unwrap_value()).collect();
                format!("[{}]", values.join(", "))
            }
            BackDoor(nf) => format!("{:?}", nf),
            BLOB(bytes) => hex::encode(bytes),
            Boolean(b) => (if *b { "true" } else { "false" }).into(),
            CLOB(chars) => chars.into_iter().collect(),
            DateValue(millis) => Self::millis_to_iso_date(*millis).unwrap_or("".into()),
            ErrorValue(message) => message.into(),
            Function { params, code } =>
                format!("(({}) => {})",
                        params.iter().map(|c| c.to_code()).collect::<Vec<String>>().join(", "),
                        code.to_code()),
            Float32Value(number) => number.to_string(),
            Float64Value(number) => number.to_string(),
            Int8Value(number) => number.to_string(),
            Int16Value(number) => number.to_string(),
            Int32Value(number) => number.to_string(),
            Int64Value(number) => number.to_string(),
            Int128Value(number) => number.to_string(),
            JSONValue(pairs) => {
                let values: Vec<String> = pairs.iter()
                    .map(|(k, v)| format!("{}:{}", k, v.unwrap_value()))
                    .collect();
                format!("{{{}}}", values.join(", "))
            }
            TableValue(mrc) => serde_json::json!(mrc.get_rows()).to_string(),
            Null => "null".into(),
            RowsAffected(number) => number.to_string(),
            StringValue(string) => string.into(),
            StructureValue(structure) => structure.to_string(),
            NamespaceValue(path) => path.into(),
            TupleValue(items) => {
                let values: Vec<String> = items.iter().map(|v| v.unwrap_value()).collect();
                format!("({})", values.join(", "))
            }
            Undefined => "undefined".into(),
            UUIDValue(guid) => Uuid::from_bytes(*guid).to_string(),
            UInt8Value(number) => number.to_string(),
            UInt16Value(number) => number.to_string(),
            UInt32Value(number) => number.to_string(),
            UInt64Value(number) => number.to_string(),
            UInt128Value(number) => number.to_string(),
        }
    }

    pub fn is_numeric_value(value: &str) -> io::Result<bool> {
        let decimal_regex = Regex::new(DECIMAL_FORMAT)
            .map_err(|e| cnv_error!(e))?;
        Ok(decimal_regex.is_match(value))
    }

    pub fn wrap_value(raw_value: &str) -> io::Result<Self> {
        let iso_date_regex = Regex::new(ISO_DATE_FORMAT).map_err(|e| cnv_error!(e))?;
        let uuid_regex = Regex::new(UUID_FORMAT).map_err(|e| cnv_error!(e))?;
        let result = match raw_value {
            s if s == "ack" => Ack,
            s if s == "false" => Boolean(false),
            s if s == "null" => Null,
            s if s == "true" => Boolean(true),
            s if s == "undefined" => Undefined,
            s if s.is_empty() => Null,
            s if Self::is_numeric_value(s)? => Self::from_numeric(s)?,
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
        let mut values = Vec::new();
        for n in self.assume_i64()?..rhs.assume_i64()? { values.push(Int64Value(n)) }
        Some(Array(values))
    }

    ///////////////////////////////////////////////////////////////
    //      UTILITY METHODS
    ///////////////////////////////////////////////////////////////

    pub fn assume_bool(&self) -> Option<bool> {
        match &self {
            Ack => Some(true),
            Boolean(b) => Some(*b),
            RowsAffected(n) => Some(*n > 0),
            _ => None
        }
    }

    pub fn assume_f64(&self) -> Option<f64> {
        match &self {
            Ack => Some(1f64),
            Float32Value(n) => Some(*n as f64),
            Float64Value(n) => Some(*n),
            Int8Value(n) => Some(*n as f64),
            Int16Value(n) => Some(*n as f64),
            Int32Value(n) => Some(*n as f64),
            Int64Value(n) => Some(*n as f64),
            Int128Value(n) => Some(*n as f64),
            RowsAffected(n) => Some(*n as f64),
            UInt8Value(n) => Some(*n as f64),
            UInt16Value(n) => Some(*n as f64),
            UInt32Value(n) => Some(*n as f64),
            UInt64Value(n) => Some(*n as f64),
            UInt128Value(n) => Some(*n as f64),
            _ => None
        }
    }

    pub fn assume_i64(&self) -> Option<i64> {
        match &self {
            Ack => Some(1),
            Float32Value(n) => Some(*n as i64),
            Float64Value(n) => Some(*n as i64),
            Int8Value(n) => Some(*n as i64),
            Int16Value(n) => Some(*n as i64),
            Int32Value(n) => Some(*n as i64),
            Int64Value(n) => Some(*n),
            Int128Value(n) => Some(*n as i64),
            UInt8Value(n) => Some(*n as i64),
            UInt16Value(n) => Some(*n as i64),
            UInt32Value(n) => Some(*n as i64),
            UInt64Value(n) => Some(*n as i64),
            UInt128Value(n) => Some(*n as i64),
            RowsAffected(n) => Some(*n as i64),
            _ => None
        }
    }

    pub fn assume_u64(&self) -> Option<u64> {
        match &self {
            Ack => Some(1),
            Float32Value(n) => Some(*n as u64),
            Float64Value(n) => Some(*n as u64),
            Int8Value(n) => Some(*n as u64),
            Int16Value(n) => Some(*n as u64),
            Int32Value(n) => Some(*n as u64),
            Int64Value(n) => Some(*n as u64),
            Int128Value(n) => Some(*n as u64),
            UInt8Value(n) => Some(*n as u64),
            UInt16Value(n) => Some(*n as u64),
            UInt32Value(n) => Some(*n as u64),
            UInt64Value(n) => Some(*n),
            UInt128Value(n) => Some(*n as u64),
            RowsAffected(n) => Some(*n as u64),
            _ => None
        }
    }

    pub fn assume_usize(&self) -> Option<usize> {
        match &self {
            Ack => Some(1),
            Float32Value(n) => Some(*n as usize),
            Float64Value(n) => Some(*n as usize),
            Int8Value(n) => Some(*n as usize),
            Int16Value(n) => Some(*n as usize),
            Int32Value(n) => Some(*n as usize),
            Int64Value(n) => Some(*n as usize),
            Int128Value(n) => Some(*n as usize),
            UInt8Value(n) => Some(*n as usize),
            UInt16Value(n) => Some(*n as usize),
            UInt32Value(n) => Some(*n as usize),
            UInt64Value(n) => Some(*n as usize),
            UInt128Value(n) => Some(*n as usize),
            RowsAffected(n) => Some(*n),
            _ => None
        }
    }

    fn numeric_op_1f(lhs: &Self, ff: fn(f64) -> f64) -> Option<Self> {
        match lhs {
            Ack => Some(Int64Value(1)),
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
            RowsAffected(a) => Some(RowsAffected(ff(*a as f64) as usize)),
            _ => Some(Float64Value(lhs.assume_f64()?))
        }
    }

    fn numeric_op_2f(lhs: &Self, rhs: &Self, ff: fn(f64, f64) -> f64) -> Option<Self> {
        match Self::intercept_unknowns(lhs, rhs) {
            Some(value) => Some(value),
            None =>
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
                    (RowsAffected(a), RowsAffected(b)) => Some(RowsAffected(ff(*a as f64, *b as f64) as usize)),
                    _ => Some(Float64Value(ff(lhs.assume_f64()?, rhs.assume_f64()?)))
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
                    (RowsAffected(a), RowsAffected(b)) => Some(RowsAffected(ff(*a as i64, *b as i64) as usize)),
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
            (Ack, Boolean(..)) => Boolean(true),
            (Ack, RowsAffected(n)) => RowsAffected(*n),
            (Boolean(..), Ack) => Boolean(true),
            (Boolean(a), Boolean(b)) => Boolean(a | b),
            (ErrorValue(a), ErrorValue(b)) => ErrorValue(format!("{a}\n{b}")),
            (ErrorValue(a), _) => ErrorValue(a.to_string()),
            (_, ErrorValue(b)) => ErrorValue(b.to_string()),
            (Float32Value(a), Float32Value(b)) => Float32Value(*a + *b),
            (Float64Value(a), Float64Value(b)) => Float64Value(*a + *b),
            (Int128Value(a), Int128Value(b)) => Int128Value(*a + *b),
            (Int64Value(a), Int64Value(b)) => Int64Value(*a + *b),
            (Int32Value(a), Int32Value(b)) => Int32Value(*a + *b),
            (Int16Value(a), Int16Value(b)) => Int16Value(*a + *b),
            (Int8Value(a), Int8Value(b)) => Int8Value(*a + *b),
            (UInt128Value(a), UInt128Value(b)) => UInt128Value(*a + *b),
            (UInt64Value(a), UInt64Value(b)) => UInt64Value(*a + *b),
            (UInt32Value(a), UInt32Value(b)) => UInt32Value(*a + *b),
            (UInt16Value(a), UInt16Value(b)) => UInt16Value(*a + *b),
            (UInt8Value(a), UInt8Value(b)) => UInt8Value(*a + *b),
            (RowsAffected(n), Ack) => RowsAffected(*n),
            (RowsAffected(a), RowsAffected(b)) => RowsAffected(*a + *b),
            (StringValue(a), StringValue(b)) => StringValue(a.to_string() + b),
            (TableValue(a), TableValue(b)) => {
                match ModelRowCollection::combine(a.get_columns().to_owned(), vec![a, b]) {
                    Ok(mrc) => TableValue(mrc),
                    Err(err) => ErrorValue(err.to_string())
                }
            }
            (TableValue(a), StructureValue(b)) => {
                let mut mrc = match ModelRowCollection::from_table(Box::new(a)) {
                    Ok(mrc) => mrc,
                    Err(err) => return ErrorValue(err.to_string())
                };
                match mrc.append_row(b.to_row()) {
                    ErrorValue(s) => ErrorValue(s),
                    _ => TableValue(mrc),
                }
            }
            // (a, b) => ErrorValue(format!("Type mismatch: cannot perform {} + {}", a.unwrap_value(), b.unwrap_value()))
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
            (Int128Value(a), Int128Value(b)) => a.partial_cmp(b),
            (UInt8Value(a), UInt8Value(b)) => a.partial_cmp(b),
            (UInt16Value(a), UInt16Value(b)) => a.partial_cmp(b),
            (UInt32Value(a), UInt32Value(b)) => a.partial_cmp(b),
            (UInt64Value(a), UInt64Value(b)) => a.partial_cmp(b),
            (UInt128Value(a), UInt128Value(b)) => a.partial_cmp(b),
            (RowsAffected(a), RowsAffected(b)) => a.partial_cmp(b),
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
        assert_eq!(UInt8Value(45) + UInt8Value(32), UInt8Value(77));
        assert_eq!(Boolean(true) + Boolean(true), Boolean(true));
        assert_eq!(StringValue("Hello".into()) + StringValue(" World".into()), StringValue("Hello World".into()));
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
    fn test_json_contains() {
        let js_value = JSONValue(vec![
            ("name".to_string(), StringValue("symbol".to_string())),
            ("column_type".to_string(), StringValue("String(8)".to_string())),
            ("default_value".to_string(), Null),
        ]);
        assert_eq!(js_value.contains(&StringValue("name".into())), Boolean(true))
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
        assert!(Array(vec![UInt8Value(0xCE), UInt8Value(0xCE)]) > Array(vec![UInt8Value(0x23), UInt8Value(0xBE)]));
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
        assert_eq!(UInt8Value(45) - UInt8Value(32), UInt8Value(13));
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
        verify_to_json(Ack, serde_json::Value::Bool(true));
        verify_to_json(Boolean(false), serde_json::Value::Bool(false));
        verify_to_json(Boolean(true), serde_json::Value::Bool(true));
        verify_to_json(DateValue(1709163679081), serde_json::Value::String("2024-02-28T23:41:19.081Z".into()));
        verify_to_json(Float32Value(38.15999984741211), serde_json::json!(38.15999984741211));
        verify_to_json(Float64Value(100.1), serde_json::json!(100.1));
        verify_to_json(Int8Value(-100), serde_json::json!(-100));
        verify_to_json(Int16Value(-1000), serde_json::json!(-1000));
        verify_to_json(Int32Value(-123_456), serde_json::json!(-123_456));
        verify_to_json(Int64Value(-123_456_789), serde_json::json!(-123_456_789));
        verify_to_json(Int128Value(-123_456_789), serde_json::json!(-123_456_789));
        verify_to_json(UInt8Value(100), serde_json::json!(100));
        verify_to_json(UInt16Value(1000), serde_json::json!(1000));
        verify_to_json(UInt32Value(123_456), serde_json::json!(123_456));
        verify_to_json(UInt64Value(123_456_789), serde_json::json!(123_456_789));
        verify_to_json(UInt128Value(123_456_789), serde_json::json!(123_456_789));
        verify_to_json(Null, serde_json::Value::Null);
        verify_to_json(StringValue("Hello World".into()), serde_json::Value::String("Hello World".into()));
        verify_to_json(Undefined, serde_json::Value::Null);
        verify_to_json(UUIDValue([
            0x67, 0x45, 0x23, 0x01, 0xAB, 0xCD, 0xEF, 0x89,
            0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF,
        ]), serde_json::Value::String("67452301-abcd-ef89-1234-567890abcdef".into()));
    }

    #[test]
    fn test_wrap_and_unwrap_value() {
        verify_wrap_unwrap("ack", Ack);
        verify_wrap_unwrap("false", Boolean(false));
        verify_wrap_unwrap("true", Boolean(true));
        verify_wrap_unwrap("2024-02-28T23:41:19.081Z", DateValue(1709163679081));
        verify_wrap_unwrap("100.1", Float64Value(100.1));
        verify_wrap_unwrap("100", Int64Value(100));
        verify_wrap_unwrap("null", Null);
        verify_wrap_unwrap("Hello World", StringValue("Hello World".into()));
        verify_wrap_unwrap("undefined", Undefined);
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