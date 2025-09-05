#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Conversions - helper utility for converting values between types
////////////////////////////////////////////////////////////////////

use crate::byte_code_compiler::ByteCodeCompiler;
use crate::connections::ConnectionTypes::WebSocketHandleType;
use crate::connections::Connections::{BLOBStoreHandle, WebServerHandle, WebSocketHandle};
use crate::connections::{ConnectionTypes, Connections};
use crate::data_types::DataType;
use crate::data_types::DataType::TableType;
use crate::dataframe::Dataframe;
use crate::dataframe::Dataframe::ModelTable;
use crate::errors::Errors::{Exact, SyntaxError, TypeMismatch};
use crate::errors::TypeMismatchErrors::{StructsOneOrMoreExpected, UnsupportedType};
use crate::errors::{throw, SyntaxErrors, TypeMismatchErrors};
use crate::extractions::pull_number_u64_vec;
use crate::model_row_collection::ModelRowCollection;
use crate::number_kind::NumberKind;
use crate::numbers::Numbers::{F64Value, I64Value, NaNValue, U128Value, U16Value, U64Value, U8Value};
use crate::parameter::Parameter;
use crate::row_collection::RowCollection;
use crate::sequences::{Array, Sequence, Sequences};
use crate::structures::Structures::Hard;
use crate::structures::{HardStructure, Row, Structure};
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::*;
use crate::utils::*;
use chrono::{DateTime, Utc};
use num_traits::ToPrimitive;
use shared_lib::cnv_error;
use uuid::Uuid;

pub struct Conversions;

impl Conversions {

    pub fn convert_from_json(j_value: &serde_json::Value) -> TypedValue {
        match j_value {
            serde_json::Value::Null => Null,
            serde_json::Value::Bool(b) => Boolean(*b),
            serde_json::Value::Number(n) => n.as_f64().map(|v| Number(F64Value(v))).unwrap_or(Null),
            serde_json::Value::String(s) => StringValue(s.to_owned()),
            serde_json::Value::Array(a) => ArrayValue(Array::from(a.iter().map(Self::convert_from_json).collect())),
            serde_json::Value::Object(args) => Structured(Hard(
                HardStructure::from_parameters(args.iter()
                    .map(|(name, value)| {
                        let tv = Self::convert_from_json(value);
                        Parameter::new_with_default(name, tv.get_type().clone(), tv)
                    }).collect::<Vec<_>>())
            ))
        }
    }

    pub fn convert_from_numeric(text: &str) -> std::io::Result<TypedValue> {
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
            s if is_integer(s)? => Ok(Number(I64Value(s.parse().map_err(|e| cnv_error!(e))?))),
            s if is_decimal(s)? => Ok(Number(F64Value(s.parse().map_err(|e| cnv_error!(e))?))),
            s => Ok(StringValue(s.to_string()))
        }
    }

    pub fn convert_to(items: &[TypedValue], datatype: &DataType) -> std::io::Result<Vec<TypedValue>> {
        let mut values = Vec::with_capacity(items.len());
        for item in items { values.push(item.convert_to(datatype)?); }
        Ok(values)
    }

    pub fn convert_to_datatype(value: &TypedValue, dest_type: &DataType) -> std::io::Result<TypedValue> {
        Ok(match value {
            ErrorValue(..) if !matches!(dest_type, DataType::StringType) => value.to_owned(),
            Null | Undefined => value.to_owned(),
            value =>
                match dest_type {
                    DataType::ArrayType(..) => Self::convert_to_array(value)?,
                    DataType::BooleanType => Self::convert_to_boolean(value)?,
                    DataType::ByteStringType => Self::convert_to_bytestring(value)?,
                    DataType::CharType => Self::convert_to_char(value)?,
                    DataType::ConnectionType(kind) => Self::convert_to_connection_type(value, kind)?,
                    DataType::DateTimeType => Self::convert_to_datetime(value)?,
                    DataType::EnumType(params) => Self::convert_to_enum(value, params)?,
                    DataType::ErrorType => ErrorValue(Exact(value.unwrap_value())),
                    DataType::FixedSizeType(underlying_type, max_len) =>
                        value.convert_to(underlying_type)?.sublist(0, *max_len),
                    DataType::FunctionType(my_params, my_returns) =>
                        Self::convert_to_function(value, my_params, my_returns)?,
                    DataType::NumberType(kind) => Self::convert_to_number(value, &kind)?,
                    DataType::PackageFunctionType(kind) => match value {
                        PackageFunction(..) => PackageFunction(kind.clone()),
                        _ => Undefined
                    }
                    DataType::StringType => Self::convert_to_string(value)?,
                    DataType::StructureType(params) => Self::convert_to_struct(value, params)?,
                    DataType::TableType(params) => Self::convert_to_table(value, params)?,
                    DataType::TupleType(types) => Self::convert_to_tuple(value, types)?,
                    DataType::RuntimeResolvedType => value.clone(),
                    DataType::UUIDType => Self::convert_to_uuid(value)?,
                }
        })
    }

    fn convert_to_array(value: &TypedValue) -> std::io::Result<TypedValue> {
        let result = match value {
            ArrayValue(array) => ArrayValue(array.to_owned()),
            ByteStringValue(bb) => ArrayValue(Array::from(u8_vec_to_values(&bb))),
            StringValue(s) => ArrayValue(Array::from(
                s.chars().map(|c| CharValue(c)).collect::<Vec<_>>()
            )),
            Structured(s) => ArrayValue(s.to_array()),
            TableValue(df) => ArrayValue(df.to_array()),
            TupleValue(items) => ArrayValue(Array::from(items.to_owned())),
            UUIDValue(uuid) => ArrayValue(Array::from(uuid.to_be_bytes()
                .iter().map(|b| Number(U8Value(*b)))
                .collect::<Vec<_>>())),
            _ => Undefined,
        };
        Ok(result)
    }

    fn convert_to_connection_type(value: &TypedValue, kind: &ConnectionTypes) -> std::io::Result<TypedValue> {
        let result = match value {
            Number(port) =>
                match kind {
                    ConnectionTypes::BLOBStoreHandleType => Undefined,
                    ConnectionTypes::WebServerHandleType => Connection(WebServerHandle(port.to_u16())),
                    ConnectionTypes::WebSocketHandleType => Undefined,
                }
            UUIDValue(uuid) =>
                match kind {
                    ConnectionTypes::BLOBStoreHandleType => Connection(BLOBStoreHandle(*uuid)),
                    ConnectionTypes::WebServerHandleType => Undefined,
                    ConnectionTypes::WebSocketHandleType => Connection(WebSocketHandle(*uuid))
                }
            _ => Undefined
        };
        Ok(result)
    }

    fn convert_to_boolean(value: &TypedValue) -> std::io::Result<TypedValue> {
        let result = match value {
            Boolean(b) => Boolean(*b),
            Number(n) => Boolean(n.to_f64() != 0.),
            StringValue(s) => Boolean(s == "true"),
            _ => Undefined
        };
        Ok(result)
    }

    fn convert_to_bytestring(value: &TypedValue) -> std::io::Result<TypedValue> {
        let result = match value {
            ArrayValue(array) => ByteStringValue(values_to_u8_vec(&array.get_values())),
            ByteStringValue(bytes) => ByteStringValue(bytes.to_owned()),
            CharValue(c) => ByteStringValue(c.to_string().bytes().collect()),
            DateTimeValue(epoch) => ByteStringValue(epoch.to_be_bytes().to_vec()),
            Number(number) => ByteStringValue(number.encode()),
            StringValue(s) => ByteStringValue(s.as_bytes().to_owned()),
            TableValue(df) => ByteStringValue(df.to_bytes()),
            UUIDValue(uuid) => ByteStringValue(uuid.to_be_bytes().to_vec()),
            _ => Undefined
        };
        Ok(result)
    }

    fn convert_to_char(value: &TypedValue) -> std::io::Result<TypedValue> {
        let result = match value {
            CharValue(c) => CharValue(*c),
            ByteStringValue(b) => u8_vec_to_char(b).map(|c| CharValue(c)).unwrap_or(Undefined),
            Number(n) => u32_to_char(n.to_u32()).map(|c| CharValue(c)).unwrap_or(Undefined),
            other => other.to_char().map(|c| CharValue(c)).unwrap_or(Undefined),
        };
        Ok(result)
    }

    fn convert_to_datetime(value: &TypedValue) -> std::io::Result<TypedValue> {
        match value {
            ByteStringValue(bytes) =>
                Ok(ByteCodeCompiler::decode_u8x8(bytes, 0, |b| DateTimeValue(i64::from_be_bytes(b)))),
            DateTimeValue(dt) => Ok(DateTimeValue(*dt)),
            Number(n) => Ok(DateTimeValue(n.to_i64())),
            StringValue(s) => {
                let datetime: DateTime<Utc> = s.parse().map_err(|e| cnv_error!(e))?;
                Ok(DateTimeValue(datetime.timestamp_millis()))
            }
            _ => throw(Exact(format!("{} cannot be converted to {}", value.unwrap_value(), DataType::DateTimeType.get_name()))),
        }
    }

    fn convert_to_enum(value: &TypedValue, params: &Vec<Parameter>) -> std::io::Result<TypedValue> {
        let result = match value {
            StringValue(text) =>
                match params.iter().position(|p| p.get_name() == text) {
                    Some(index) => Number(I64Value(index as i64)),
                    None => Null
                }
            _ => Undefined,
        };
        Ok(result)
    }

    fn convert_to_function(
        value: &TypedValue,
        params: &Vec<Parameter>,
        returns: &DataType,
    ) -> std::io::Result<TypedValue> {
        let result = match value {
            ErrorValue(err) => return throw(err.clone()),
            Function { body, .. } =>
                Function {
                    params: params.clone(),
                    body: body.clone(),
                    returns: returns.clone(),
                },
            other => return throw(TypeMismatch(TypeMismatchErrors::FunctionExpected(other.to_code())))
        };
        Ok(result)
    }

    pub fn convert_to_json(value: &TypedValue) -> serde_json::Value {
        match value {
            ArrayValue(items) => serde_json::json!(items.iter().map(|v|v.to_json()).collect::<Vec<_>>()),
            ByteStringValue(bytes) => serde_json::json!(bytes),
            CharValue(c) => serde_json::json!(c),
            Connection(conn) => conn.to_json(),
            Boolean(b) => serde_json::json!(b),
            DateTimeValue(dt) => serde_json::json!(millis_to_iso_date(*dt).unwrap_or_else(|| dt.to_string())),
            EnumValue(id, ..) => id.map(|_| serde_json::json!(value.unwrap_value())).unwrap_or(serde_json::Value::Null),
            ErrorValue(message) => serde_json::json!(message),
            Function { params, body: code, returns } => {
                let my_params = serde_json::Value::Array(params.iter()
                    .map(|c| c.to_json()).collect());
                serde_json::json!({ "params": my_params, "code": code.to_code(), "returns": returns.to_type_declaration() })
            }
            Kind(data_type) => serde_json::json!(data_type.to_code()),
            TypedValue::Null => serde_json::Value::Null,
            Number(nv) => nv.to_json(),
            PackageFunction(nf) => serde_json::json!(nf),
            StringValue(s) => serde_json::json!(s),
            Structured(s) => s.to_json(),
            TableValue(df) => {
                let parameters = df.get_parameters();
                let rows = df.iter()
                    .map(|r| r.to_hash_json_value(&parameters))
                    .collect::<Vec<_>>();
                serde_json::json!(rows)
            }
            TupleValue(items) => serde_json::json!(items.iter().map(|v|v.to_json()).collect::<Vec<_>>()),
            TypedValue::Undefined => serde_json::Value::Null,
            UUIDValue(uuid) => serde_json::json!(u128_to_uuid(*uuid)),
        }
    }

    pub fn convert_to_number(value: &TypedValue, kind: &NumberKind) -> std::io::Result<TypedValue> {
        let result = match value {
            Boolean(b) => I64Value(if *b { 1 } else { 0 }),
            ByteStringValue(bytes) => kind.decode(bytes, 0),
            CharValue(c) => U64Value(unicode_char_to_u64(*c)),
            DateTimeValue(epoch) => I64Value(*epoch),
            EnumValue(n, _) => n.map(|n| U16Value(n)).unwrap_or(NaNValue),
            Number(number) => *number,
            StringValue(..) => kind.convert_from(value)?,
            UUIDValue(uuid) => U128Value(*uuid).convert_to(kind),
            _ => NaNValue,
        };
        Ok(Number(result.convert_to(kind)))
    }

    pub fn convert_to_sequence(value: &TypedValue) -> std::io::Result<Sequences> {
        match value {
            ArrayValue(array) => Ok(Sequences::TheArray(array.clone())),
            ByteStringValue(bytes) => Ok(Sequences::TheArray(Array::from(u8_vec_to_values(bytes)))),
            StringValue(s) => Ok(Sequences::TheArray(Array::from(string_to_char_values(s)))),
            Structured(s) => Ok(Sequences::TheArray(Array::from(s.to_name_value_tuples()))),
            TableValue(df) => Ok(Sequences::TheDataframe(df.clone())),
            TupleValue(t) => Ok(Sequences::TheTuple(t.to_vec())),
            z => throw(TypeMismatch(UnsupportedType(TableType(vec![]), z.get_type())))
        }
    }

    fn convert_to_string(value: &TypedValue) -> std::io::Result<TypedValue> {
        let result = match value {
            ByteStringValue(bytes) =>
                StringValue(String::from_utf8(bytes.clone())
                    .map_err(|e| cnv_error!(e))?),
            _ => StringValue(value.unwrap_value())
        };
        Ok(result)
    }

    fn convert_to_struct(value: &TypedValue, _params: &Vec<Parameter>) -> std::io::Result<TypedValue> {
        let result = match value {
            Structured(s) => Structured(s.clone()),
            _ => Undefined
        };
        Ok(result)
    }

    fn convert_to_table(value: &TypedValue, params: &Vec<Parameter>) -> std::io::Result<TypedValue> {
        let result = match value {
            ArrayValue(..) => value.to_table_with_schema(params)?,
            Structured(..) => value.to_table_with_schema(params)?,
            TableValue(..) => value.to_table_with_schema(params)?,
            _ => Undefined
        };
        Ok(result)
    }

    fn convert_to_tuple(value: &TypedValue, _types: &Vec<DataType>) -> std::io::Result<TypedValue> {
        let result = match value {
            ArrayValue(array) => TupleValue(array.get_values()),
            ByteStringValue(bytes) => TupleValue(u8_vec_to_values(bytes)),
            StringValue(s) => TupleValue(string_to_char_values(s)),
            Structured(s) => TupleValue(s.get_values()),
            TupleValue(items) => TupleValue(items.to_owned()),
            _ => Undefined
        };
        Ok(result)
    }

    fn convert_to_uuid(value: &TypedValue) -> std::io::Result<TypedValue> {
        let result = match value {
            ByteStringValue(bytes) => UUIDValue(Uuid::from_slice(&bytes).map_err(|e| cnv_error!(e))?.as_u128()),
            Connection(conn) =>
                match conn {
                    BLOBStoreHandle(uuid) => UUIDValue(*uuid),
                    WebServerHandle(port) => UUIDValue(*port as u128),
                    WebSocketHandle(uuid) => UUIDValue(*uuid),
                }
            Number(number) => UUIDValue(number.to_u128()),
            StringValue(text) => string_to_uuid_value(text.as_str())?,
            UUIDValue(uuid) => UUIDValue(*uuid),
            _ => Undefined
        };
        Ok(result)
    }

    pub fn convert_values_to_table(items: &[TypedValue]) -> std::io::Result<Dataframe> {
        let mut dataframes = Vec::new();
        for item in items {
            let mrc = match item {
                Structured(ss) => ModelTable(ss.to_table()),
                TableValue(ModelTable(mrc)) => ModelTable(mrc.to_owned()),
                TupleValue(tuples) => {
                    let (mut params, mut values) = (Vec::new(), Vec::new());
                    for (n, value) in tuples.iter().enumerate() {
                        let name = format!("t{n}");
                        let data_type = value.get_type().clone();
                        params.push(Parameter::new(name, data_type));
                        values.push(value.clone());
                    }
                    let mut mrc = ModelRowCollection::from_parameters(&params);
                    mrc.append_row(Row::new(0, values))?;
                    ModelTable(mrc)
                }
                value => {
                    let mut mrc = ModelRowCollection::from_parameters(&vec![
                        Parameter::new("value", value.get_type().clone())
                    ]);
                    mrc.append_row(Row::new(0, vec![value.clone()]))?;
                    ModelTable(mrc)
                }
            };
            dataframes.push(mrc);
        }

        // process the dataframes
        match dataframes.as_slice() {
            [] => throw(TypeMismatch(StructsOneOrMoreExpected)),
            [df] => Ok(df.to_owned()),
            dfs => Dataframe::combine_tables(dfs.to_vec())
        }
    }

    pub fn unwrap_value(value: &TypedValue) -> String {
        /// handle special case values
        fn quoted(items: &[TypedValue]) -> Vec<String> {
            items.iter()
                .map(|value| match value {
                    CharValue(c) => format!(r#"'{c}'"#),
                    StringValue(s) => format!(r#""{s}""#),
                    v => v.unwrap_value()
                })
                .collect::<Vec<_>>()
        }

        match value {
            TypedValue::ArrayValue(array) =>
                format!("[{}]", quoted(&array.get_values()).join(", ")),
            TypedValue::Connection(conn) => conn.unwrap_value(),
            TypedValue::Boolean(b) => (if *b { "true" } else { "false" }).into(),
            TypedValue::ByteStringValue(bytes) => to_bytestring(bytes),
            TypedValue::CharValue(c) => c.to_string(),
            TypedValue::DateTimeValue(dt) => millis_to_iso_date(*dt).unwrap_or_else(|| dt.to_string()),
            TypedValue::EnumValue(id, params) =>
                match id.and_then(|id| id.to_usize()) {
                    None => "null".into(),
                    Some(id) => params[id].get_name().into(),
                }
            TypedValue::ErrorValue(message) => message.to_string(),
            TypedValue::Function { params, body: code, returns } =>
                format!("(({}){} -> {})",
                        params.iter().map(|c| c.to_code()).collect::<Vec<_>>().join(", "),
                        match returns.to_code().as_str() {
                            "" => "".to_string(),
                            s => format!(": {}", s),
                        },
                        code.to_code()),
            TypedValue::Kind(data_type) => data_type.to_code(),
            TypedValue::Null => "null".into(),
            TypedValue::Number(number) => number.unwrap_value(),
            TypedValue::PackageFunction(nf) => nf.to_code(),
            TypedValue::StringValue(string) => string.into(),
            TypedValue::Structured(structure) => structure.to_json().to_string(),
            TypedValue::TableValue(rcv) => {
                let params = rcv.get_parameters();
                serde_json::json!(rcv.iter().map(|r| r.to_hash_json_value(&params))
                    .collect::<Vec<_>>()).to_string()
            }
            TypedValue::TupleValue(items) => format!("({})", quoted(items).join(", ")),
            TypedValue::Undefined => "undefined".into(),
            TypedValue::UUIDValue(uuid) => u128_to_uuid(*uuid),
        }
    }

    pub fn wrap_value(raw_value: &str) -> std::io::Result<TypedValue> {
        let result = match raw_value.trim() {
            "" => Null,
            "false" => Boolean(false),
            "null" => Null,
            "true" => Boolean(true),
            "undefined" => Undefined,
            s if is_numeric_value(s)? => Self::convert_from_numeric(s)?,
            s if is_iso8601(s)? =>
                DateTimeValue(DateTime::parse_from_rfc3339(s)
                    .map_err(|e| cnv_error!(e))?.timestamp_millis()),
            s if is_uuid(s)? => Number(U128Value(ByteCodeCompiler::decode_uuid(s)?)),
            s if is_quoted(s) => StringValue(s[1..s.len() - 1].to_string()),
            s => return throw(SyntaxError(SyntaxErrors::LiteralExpected(s.to_string()))),
        };
        Ok(result)
    }

    pub fn wrap_value_opt(opt_value: &Option<String>) -> std::io::Result<TypedValue> {
        match opt_value {
            Some(value) => Self::wrap_value(value),
            None => Ok(Null)
        }
    }

}

/// Unit tests
#[cfg(test)]
mod tests {

    /// conversion tests
    #[cfg(test)]
    mod conversion_tests {
        use crate::compiler::Compiler;
        use crate::connections::ConnectionTypes::WebSocketHandleType;
        use crate::connections::Connections::{BLOBStoreHandle, WebSocketHandle};
        use crate::conversions::Conversions;
        use crate::data_types::DataType;
        use crate::data_types::DataType::*;
        use crate::dataframe::Dataframe;
        use crate::dataframe::Dataframe::ModelTable;
        use crate::errors::Errors::Exact;
        use crate::model_row_collection::ModelRowCollection;
        use crate::number_kind::NumberKind::*;
        use crate::numbers::Numbers::*;
        use crate::parameter::Parameter;
        use crate::sequences::Array;
        use crate::structures::Structures::{Hard, Soft};
        use crate::structures::{HardStructure, Row, SoftStructure};
        use crate::test_util::interpret;
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
            verify(array, TableType(params.clone()), TableValue(Dataframe::ModelTable(
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
        fn test_array_to_tuple() {
            verify(
                ArrayValue(Array::from(vec![
                    CharValue('A'),
                    CharValue('B'),
                    CharValue('C'),
                ])),
                TupleType(vec![
                    CharType.into(),
                    CharType.into(),
                    CharType.into(),
                ]),
                TupleValue(vec![
                    CharValue('A'),
                    CharValue('B'),
                    CharValue('C'),
                ]));
        }

        #[test]
        fn test_blob_store_to_uuid() {
            let uuid = 0xcafe_babe_face_u128;
            assert_eq!(Connection(BLOBStoreHandle(uuid))
                           .convert_to(&UUIDType).unwrap(), UUIDValue(uuid));
        }

        #[test]
        fn test_boolean_to_number() {
            verify(Boolean(true), NumberType(I64Kind), Number(I64Value(1)));
            verify(Boolean(false), NumberType(I64Kind), Number(I64Value(0)));
        }

        #[test]
        fn test_boolean_to_string() {
            verify(Boolean(true), StringType, StringValue("true".into()));
            verify(Boolean(false), StringType, StringValue("false".into()));
        }

        #[test]
        fn test_bytestring_to_number() {
            verify(
                ByteStringValue(vec![0, 0, 15, 223, 185, 36, 207, 227]),
                NumberType(I64Kind), Number(I64Value(17453558321123)));
            verify(
                ByteStringValue(vec![
                    0xfe, 0xed, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xaf,
                    0xfa, 0xde, 0xca, 0xfe, 0xba, 0xbe, 0xfa, 0xce
                ]), NumberType(U128Kind),
                Number(U128Value(0xfeed_dead_beef_deaf_fade_cafe_babe_face_u128)),
            );
        }

        #[test]
        fn test_bytestring_to_string() {
            verify(
                ByteStringValue(b"Hello there".to_vec()),
                StringType, StringValue("Hello there".into()));
        }

        #[test]
        fn test_bytestring_to_uuid() {
            verify(ByteStringValue(vec![
                0xfe, 0xed, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xaf,
                0xfa, 0xde, 0xca, 0xfe, 0xba, 0xbe, 0xfa, 0xce
            ]), UUIDType, UUIDValue(0xfeed_dead_beef_deaf_fade_cafe_babe_face_u128));
        }
        
        #[test]
        fn test_convert_to_enum() {
            let params = vec![
                Parameter::new_with_default("AMEX", NumberType(U16Kind), interpret("0")),
                Parameter::new_with_default("NASDAQ", NumberType(U16Kind), interpret("1")),
                Parameter::new_with_default("NYSE", NumberType(U16Kind), interpret("2")),
                Parameter::new_with_default("OTCBB", NumberType(U16Kind), interpret("3")),
            ];
            let my_enum = Conversions::convert_to_enum(
                &interpret("\"NYSE\""),
                &params,
            ).unwrap();
            assert_eq!(my_enum, Number(I64Value(2)));
        }

        #[test]
        fn test_convert_to_function() {
            let params = vec![Parameter::new("x", NumberType(F64Kind).into())];
            let fx = Conversions::convert_to_function(
                &interpret("x -> x + 1"),
                &params,
                &NumberType(F64Kind),
            ).unwrap();
            assert_eq!(fx, Function {
                params,
                body: Compiler::build("x + 1").unwrap().into(),
                returns: NumberType(F64Kind).into(),
            });
        }

        #[test]
        fn test_date_to_bytestring() {
            verify(DateTimeValue(17453558321123), ByteStringType, ByteStringValue(vec![
                0, 0, 15, 223, 185, 36, 207, 227
            ]));
        }

        #[test]
        fn test_date_to_number() {
            verify(DateTimeValue(17453558321123), NumberType(I64Kind), Number(I64Value(17453558321123)));
        }

        #[test]
        fn test_date_to_string() {
            verify(DateTimeValue(17453558321123), StringType, StringValue("2523-01-30T18:38:41.123Z".into()));
        }

        #[test]
        fn test_number_to_boolean_false() {
            verify(Number(I64Value(0)), BooleanType, Boolean(false));
        }

        #[test]
        fn test_number_to_boolean_true() {
            verify(Number(I64Value(1)), BooleanType, Boolean(true));
        }

        #[test]
        fn test_number_to_bytestring() {
            verify(Number(I64Value(17453558321123)), ByteStringType, ByteStringValue(vec![
                0, 0, 15, 223, 185, 36, 207, 227
            ]));
            verify(Number(U128Value(0xfeed_dead_beef_deaf_fade_cafe_babe_face_u128)),
                   ByteStringType, ByteStringValue(vec![
                    0xfe, 0xed, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xaf,
                    0xfa, 0xde, 0xca, 0xfe, 0xba, 0xbe, 0xfa, 0xce
                ]));
        }

        #[test]
        fn test_number_to_string_f64() {
            verify(Number(F64Value(12.35)), FixedSizeType(StringType.into(), 5), StringValue("12.35".into()));
        }

        #[test]
        fn test_number_to_string_i64() {
            verify(Number(I64Value(-128)), FixedSizeType(StringType.into(), 4), StringValue("-128".into()));
        }

        #[test]
        fn test_number_to_string_u64() {
            verify(Number(U64Value(128)), FixedSizeType(StringType.into(), 3), StringValue("128".into()));
        }

        #[test]
        fn test_number_to_string_u128() {
            verify(Number(U128Value(0xfeed_dead_beef_deaf_fade_cafe_babe_face_u128)),
                   StringType, StringValue("338859001745337648252653219454709070542".into()));
        }

        #[test]
        fn test_number_to_uuid() {
            verify(Number(U128Value(0xfeed_dead_beef_deaf_fade_cafe_babe_face_u128)),
                   UUIDType, UUIDValue(0xfeed_dead_beef_deaf_fade_cafe_babe_face_u128));
        }

        #[test]
        fn test_string_to_boolean() {
            verify(StringValue("true".into()), BooleanType, Boolean(true));
        }

        #[test]
        fn test_string_to_bytestring() {
            verify(StringValue("Hello there".into()), ByteStringType, ByteStringValue(b"Hello there".into()));
        }

        #[test]
        fn test_string_to_error() {
            verify(StringValue("This is an error".into()),
                   ErrorType, ErrorValue(Exact("This is an error".into())));
        }

        #[test]
        fn test_struct_to_array() {
            let stock = Structured(Soft(SoftStructure::new(&vec![
                ("symbol", StringValue("XYZ".into())),
                ("exchange", StringValue("AMEX".into())),
                ("last_sale", Number(F64Value(17.76))),
            ])));
            verify(
                stock,
                ArrayType(TupleType(vec![StringType, StringType, NumberType(F64Kind)]).into()),
                ArrayValue(Array::from(vec![
                    TupleValue(vec![
                        StringValue("symbol".into()),
                        StringValue("XYZ".into()),
                    ]),
                    TupleValue(vec![
                        StringValue("exchange".into()),
                        StringValue("AMEX".into()),
                    ]),
                    TupleValue(vec![
                        StringValue("last_sale".into()),
                        Number(F64Value(17.76)),
                    ]),
                ])));
        }

        #[test]
        fn test_struct_to_table() {
            let params = vec![
                Parameter::new_with_default("symbol", FixedSizeType(StringType.into(), 3), StringValue("ABC".into())),
                Parameter::new_with_default("exchange", FixedSizeType(StringType.into(), 4), StringValue("NYSE".into())),
                Parameter::new_with_default("last_sale", NumberType(F64Kind), Number(F64Value(14.92)))
            ];
            let stock = Structured(Soft(SoftStructure::new(&vec![
                ("symbol", StringValue("ABC".into())),
                ("exchange", StringValue("NYSE".into())),
                ("last_sale", Number(F64Value(14.92))),
            ])));
            let table = TableValue(ModelTable(
                ModelRowCollection::from_parameters_and_rows(
                    &params, &vec![
                        Row::new(0, vec![
                            StringValue("ABC".into()),
                            StringValue("NYSE".into()),
                            Number(F64Value(14.92))
                        ])
                    ],
                )
            ));
            verify(stock, TableType(params), table);
        }

        #[test]
        fn test_struct_to_tuple() {
            let stock = Structured(Soft(SoftStructure::new(&vec![
                ("symbol", StringValue("DTR".into())),
                ("exchange", StringValue("TSE".into())),
                ("last_sale", Number(F64Value(78.89))),
            ])));
            verify(
                stock,
                TupleType(vec![StringType, StringType, NumberType(F64Kind)]),
                TupleValue(vec![
                    StringValue("DTR".into()),
                    StringValue("TSE".into()),
                    Number(F64Value(78.89))
                ]));
        }

        #[test]
        fn test_tuple_to_array() {
            verify(
                TupleValue(vec![
                    CharValue('A'),
                    CharValue('B'),
                    CharValue('C'),
                ]),
                ArrayType(CharType.into()),
                ArrayValue(Array::from(vec![
                    CharValue('A'),
                    CharValue('B'),
                    CharValue('C'),
                ])));
        }

        #[test]
        fn test_uuid_to_bytestring() {
            verify(UUIDValue(0xfeed_dead_beef_deaf_fade_cafe_babe_face_u128),
                   ByteStringType,
                   ByteStringValue(vec![
                       0xfe, 0xed, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xaf,
                       0xfa, 0xde, 0xca, 0xfe, 0xba, 0xbe, 0xfa, 0xce
                   ]));
        }


        #[test]
        fn test_uuid_to_number() {
            verify(UUIDValue(0xfeed_dead_beef_deaf_fade_cafe_babe_face_u128),
                   NumberType(U128Kind), Number(U128Value(0xfeed_dead_beef_deaf_fade_cafe_babe_face_u128)));
        }

        #[test]
        fn test_uuid_to_string() {
            verify(UUIDValue(0xfeed_dead_beef_deaf_fade_cafe_babe_face_u128),
                   StringType, StringValue("feeddead-beef-deaf-fade-cafebabeface".into()));
        }

        #[test]
        fn test_uuid_to_websocket() {
            let uuid = 0xcafe_babe_face_u128;
            assert_eq!(
                UUIDValue(uuid).convert_to(&ConnectionType(WebSocketHandleType)).unwrap(),
                Connection(WebSocketHandle(uuid)));
        }

        #[test]
        fn test_websocket_to_uuid() {
            let uuid = 0xcafe_babe_face_u128;
            assert_eq!(
                Connection(WebSocketHandle(uuid)).convert_to(&UUIDType).unwrap(),
                UUIDValue(uuid));
        }

        fn verify(from_value: TypedValue, to_type: DataType, to_value: TypedValue) {
            assert_eq!(from_value.convert_to(&to_type).unwrap(), to_value);
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
            verify_unwrap(array, r#"["123", "abc", "xyz", "897"]"#)
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
            verify_unwrap_diff(
                array,
                r#"[{"exchange":"NYSE","last_sale":23.66,"symbol":"BIZ"}, {"exchange":"NYSE","last_sale":23.66,"symbol":"BIZ"}]"#,
                r#"[Struct(symbol: String(3) = "BIZ", exchange: String(4) = "NYSE", last_sale: f64 = 23.66), Struct(symbol: String(3) = "DMX", exchange: String(4) = "OTC_BB", last_sale: f64 = 1.17)]"#)
        }

        #[test]
        fn test_boolean() {
            verify_unwrap(Boolean(true), "true");
            verify_unwrap(Boolean(false), "false");
        }

        #[test]
        fn test_byte_string() {
            verify_unwrap(ByteStringValue(vec![
                0xde, 0xad, 0xbe, 0xef, 0xfa, 0xce
            ]), "0Bdeadbeefface");
        }

        #[test]
        fn test_date() {
            verify_unwrap(DateTimeValue(17453558321123), "2523-01-30T18:38:41.123Z");
        }

        #[test]
        fn test_enum() {
            verify_unwrap(EnumValue(Some(0), vec![
                Parameter::new_with_default("AMEX", NumberType(U16Kind), Number(U16Value(0))),
                Parameter::new_with_default("NYSE", NumberType(U16Kind), Number(U16Value(1))),
                Parameter::new_with_default("NASDAQ", NumberType(U16Kind), Number(U16Value(2))),
                Parameter::new_with_default("OTCBB", NumberType(U16Kind), Number(U16Value(3))),
            ]), "AMEX");
        }

        #[test]
        fn test_error() {
            verify_unwrap(ErrorValue(Exact("This is an error".into())), "This is an error");
        }

        #[test]
        fn test_tuple() {
            let tuple = TupleValue(vec![
                DateTimeValue(17453558321123),
                StringValue("hello".into()),
                UUIDValue(0xfeed_dead_beef_deaf_fade_cafe_babe_face_u128)
            ]);
            verify_unwrap(tuple, r#"(2523-01-30T18:38:41.123Z, "hello", feeddead-beef-deaf-fade-cafebabeface)"#)
        }

        #[test]
        fn test_uuid() {
            verify_unwrap(
                UUIDValue(0xfeed_dead_beef_deaf_fade_cafe_babe_face_u128),
                "feeddead-beef-deaf-fade-cafebabeface")
        }

        fn verify_unwrap(value: TypedValue, text: &str) {
            assert_eq!(value.unwrap_value(), text);
            assert_eq!(value.to_code(), text);
        }

        fn verify_unwrap_diff(value: TypedValue, unwrap_text: &str, code_text: &str) {
            assert_eq!(value.unwrap_value(), unwrap_text);
            assert_eq!(value.to_code(), code_text);
        }
    }

}