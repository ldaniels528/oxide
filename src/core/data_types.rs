#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// DataType class
////////////////////////////////////////////////////////////////////

use crate::byte_code_compiler::ByteCodeCompiler;
use crate::compiler::Compiler;
use crate::connections::ConnectionTypes;
use crate::data_types::DataType::*;
use crate::dataframe::Dataframe;
use crate::errors::Errors::Exact;
use crate::expression::Expression::*;
use crate::field::FieldMetadata;
use crate::model_row_collection::ModelRowCollection;
use crate::number_kind::NumberKind;
use crate::packages::PackageOps;
use crate::parameter::Parameter;
use crate::sequences::Array;
use crate::structures::HardStructure;
use crate::structures::Structures::Hard;
use crate::type_engine::TypeEngine;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::*;
use crate::utils::remove_last_char;
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display};
use std::ops::Deref;

const PTR_LEN: usize = 8;

/// Represents an Oxide-native datatype
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum DataType {
    ArrayType(Box<DataType>),
    BooleanType,
    ByteStringType,
    CharType,
    ConnectionType(ConnectionTypes),
    DateTimeType,
    EnumType(Vec<Parameter>),
    ErrorType,
    FixedSizeType(Box<DataType>, usize),
    FunctionType(Vec<Parameter>, Box<DataType>),
    NumberType(NumberKind),
    PackageFunctionType(PackageOps),
    RuntimeResolvedType,
    StringType,
    StructureType(Vec<Parameter>),
    TableType(Vec<Parameter>),
    TupleType(Vec<DataType>),
    UUIDType,
}

impl DataType {
    ////////////////////////////////////////////////////////////////////
    //  STATIC METHODS
    ////////////////////////////////////////////////////////////////////

    /// decodes the typed value based on the supplied data type and buffer
    pub fn decode(&self, buffer: &Vec<u8>, offset: usize) -> TypedValue {
        use crate::typed_values::TypedValue::*;
        match self {
            ArrayType(..) => ArrayValue(Array::new()),
            Self::BooleanType => ByteCodeCompiler::decode_u8(buffer, offset, |b| Boolean(b == 1)),
            Self::ByteStringType => ByteStringValue(Vec::new()),
            Self::CharType =>
                std::str::from_utf8(&buffer[offset..offset+4].to_vec()).ok()
                    .and_then(|s| {
                        let mut chars = s.chars();
                        chars.next()
                    })
                    .map(|c| CharValue(c))
                    .unwrap_or(Undefined),
            ConnectionType(kind) => kind.decode(buffer, offset),
            Self::DateTimeType => ByteCodeCompiler::decode_u8x8(buffer, offset, |b| DateTimeValue(i64::from_be_bytes(b))),
            EnumType(params) =>
                match ByteCodeCompiler::decode_u8x2(&buffer, offset, |b| u16::from_be_bytes(b)) {
                    0 => EnumValue(None, params.to_vec()),
                    n => EnumValue(Some(n - 1), params.to_vec())
                }
            Self::ErrorType => ErrorValue(Exact(ByteCodeCompiler::decode_string(buffer, offset, 255).to_string())),
            FixedSizeType(data_type, size) => match data_type.deref() {
                StringType => StringValue(ByteCodeCompiler::decode_string(buffer, offset, *size).to_string()),
                TableType(params) => TableValue(Dataframe::ModelTable(ModelRowCollection::from_bytes(params, buffer.to_vec()))),
                _ => data_type.decode(buffer, offset)
            },
            FunctionType(params, returns) => Function { 
                params: params.to_owned(),
                body: Literal(ByteCodeCompiler::decode_value(&buffer[offset..].to_vec())).into(),
                returns: returns.deref().to_owned(),
            },
            NumberType(kind) => Number(kind.decode(buffer, offset)),
            PackageFunctionType(pops) => PackageFunction(pops.to_owned()),
            Self::StringType => StringValue(ByteCodeCompiler::decode_string(buffer, offset, 255)),
            StructureType(params) => Structured(Hard(HardStructure::from_parameters(params.to_vec()))),
            TableType(params) => TableValue(Dataframe::ModelTable(ModelRowCollection::from_bytes(params, buffer.to_vec()))),
            TupleType(params) => TupleValue(params.iter().map(|dt| dt.decode(buffer, offset)).collect()),
            Self::RuntimeResolvedType => ByteCodeCompiler::decode_value(&buffer[offset..].to_vec()),
            Self::UUIDType => ByteCodeCompiler::decode_u8x16(buffer, offset, |b| UUIDValue(u128::from_be_bytes(b))),
        }
    }

    pub fn decode_field_value(&self, buffer: &Vec<u8>, offset: usize) -> TypedValue {
        let metadata = FieldMetadata::decode(buffer[offset]);
        if metadata.is_active {
            self.decode(buffer, offset + 1)
        } else { Null }
    }

    pub fn encode(&self, value: &TypedValue) -> std::io::Result<Vec<u8>> {
        match self {
            EnumType(params) if !matches!(value, EnumValue(..)) => {
                let name = value.unwrap_value();
                let index = params.iter()
                    .position(|p| p.get_name() == name)
                    .and_then(|n| n.to_u16());
                EnumValue(index, params.to_vec()).encode()
            }
            _ => value.encode()
        }
    }

    pub fn encode_field(
        &self,
        value: &TypedValue,
        metadata: &FieldMetadata,
        capacity: usize,
    ) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::with_capacity(capacity);
        buf.push(metadata.encode());
        buf.extend(self.encode(value).unwrap_or_else(|_| vec![]));
        buf.resize(capacity, 0u8);
        buf
    }

    /// parses a datatype expression (e.g. "String(20)")
    pub fn from_str(param_type: &str) -> std::io::Result<DataType> {
        let model = Compiler::build(param_type)?;
        TypeEngine::decipher_model(&model)
    }
    
    ////////////////////////////////////////////////////////////////////
    //  INSTANCE METHODS
    ////////////////////////////////////////////////////////////////////

    /// computes and returns the maximum physical size of this datatype
    pub fn compute_fixed_size(&self) -> usize {
        let width: usize = match self {
            ArrayType(..) => PTR_LEN,
            Self::BooleanType => 1,
            Self::ByteStringType => PTR_LEN,
            Self::CharType => 4,
            ConnectionType(kind) => kind.compute_fixed_size(),
            Self::DateTimeType => 8,
            EnumType(..) => 2,
            Self::ErrorType => 64,
            FixedSizeType(data_type, size) =>
                match data_type.deref() {
                    ByteStringType | StringType =>
                        match size {
                            0 => PTR_LEN,
                            size => *size + size.to_be_bytes().len(),
                        }
                    _ => data_type.compute_fixed_size() * size
                }
            FunctionType(columns, ..) => columns.len() * PTR_LEN,
            NumberType(nk) => nk.compute_fixed_size(),
            PackageFunctionType(..) => 8,
            Self::StringType => PTR_LEN,
            StructureType(columns) => columns.len() * PTR_LEN,
            TableType(columns) => columns.len() * PTR_LEN,
            TupleType(types) => types.iter().map(|t| t.compute_fixed_size()).sum(),
            Self::RuntimeResolvedType => PTR_LEN,
            Self::UUIDType => 16,
        };
        width + 1 // +1 for field metadata
    }

    pub fn get_name(&self) -> String {
        TypeEngine::get_type_name(self)
    }

    /// Instantiates this type with the specified arguments
    pub fn instantiate(
        &self,
        args: Vec<TypedValue>,
    ) -> std::io::Result<TypedValue> {
        TypeEngine::instantiate(self, args)
    }

    pub fn is_a(&self, other: &DataType) -> bool {
        TypeEngine::is_a(self, other)
    }
    
    pub fn is_compatible(&self, other: &DataType) -> bool {
        TypeEngine::is_compatible(self, other)
    }

    pub fn render(types: &Vec<DataType>) -> String {
        Self::render_f(types, |t| t.to_code())
    }

    pub fn render_f(types: &Vec<DataType>, f: fn(&DataType) -> String) -> String {
        types.iter().map(|dt| f(dt))
            .collect::<Vec<String>>()
            .join(", ")
    }

    pub fn to_code(&self) -> String {
        fn parameterized(name: &str, params: &Vec<Parameter>, is_enum: bool) -> String {
            match params.len() {
                0 => name.to_string(),
                _ => format!("{name}({})", if is_enum {
                    Parameter::render_f(params, |p| p.to_code_enum())
                } else {
                    Parameter::render(params)
                })
            }
        }
        fn sized(name: &str, size: usize) -> String {
            match size {
                0 => name.to_string(),
                n if name.ends_with(")") => format!("{}, {})", remove_last_char(name), n),
                n => format!("{name}({n})"),
            }
        }
        fn typed(name: &str, params: &Vec<DataType>) -> String {
            match params.len() {
                0 => name.to_string(),
                _ => format!("{name}({})", DataType::render(params))
            }
        }
        match self {
            ArrayType(data_type) => format!("Array({})", data_type.to_code()),
            Self::BooleanType => "Boolean".into(),
            Self::ByteStringType => "Bytes".into(), // UTF8
            Self::CharType => "Char".into(),
            ConnectionType(kind) => kind.to_code(),
            Self::DateTimeType => "DateTime".into(),
            EnumType(labels) => parameterized("Enum", labels, true),
            Self::ErrorType => "Error".into(),
            FixedSizeType(data_type, size) => sized(data_type.to_code().as_str(), *size),
            FunctionType(params, returns) =>
                format!("fn({}){}", Parameter::render(params),
                        match returns.to_code() {
                            s if !s.is_empty() => format!(": {}", s),
                            _ => String::new()
                        }),
            NumberType(nk) => nk.get_type_name(),
            PackageFunctionType(pf) => pf.to_code(),
            Self::StringType => "String".into(),
            StructureType(params) => parameterized("Struct", params, false),
            TableType(params) => parameterized("Table", params, false),
            TupleType(types) => typed("", types),
            Self::RuntimeResolvedType => String::new(),
            Self::UUIDType => "UUID".into(),
        }
    }

    pub fn to_type_declaration(&self) -> Option<String> {
        match self.to_code() {
            s if s.is_empty() => None,
            s => Some(s)
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_code())
    }
}

/// Unit tests
#[cfg(test)]
mod tests {

    /// Compilation Unit tests
    mod compilation_tests {
        use crate::data_types::DataType;
        use crate::data_types::DataType::*;
        use crate::number_kind::NumberKind::*;
        use crate::numbers::Numbers::U16Value;
        use crate::parameter::Parameter;
        use crate::test_util::make_quote_parameters;
        use crate::typed_values::TypedValue::Number;

        #[test]
        fn test_array() {
            verify_type_construction(
                "Array()",
                ArrayType(RuntimeResolvedType.into()).into()
            );
        }

        #[test]
        fn test_array_of_type() {
            verify_type_construction(
                "Array(DateTime)",
                ArrayType(DateTimeType.into()).into()
            );
        }

        #[test]
        fn test_array_fixed_of_type() {
            verify_type_construction(
                "Array(DateTime, 12)",
                FixedSizeType(ArrayType(DateTimeType.into()).into(), 12)
            );
        }
        
        #[test]
        fn test_boolean() {
            verify_type_construction("Boolean", BooleanType);
        }

        #[test]
        fn test_bytes() {
            verify_type_construction("Bytes(5566)", FixedSizeType(ByteStringType.into(), 5566));
        }

        #[test]
        fn test_datetime() {
            verify_type_construction("DateTime", DateTimeType);
        }

        #[test]
        fn test_enums_inferred() {
            verify_type_construction_ab(
                "Enum(A, B, C)",
                "Enum(A = 0, B = 1, C = 2)",
                EnumType(vec![
                    Parameter::new_with_default("A", NumberType(U16Kind), Number(U16Value(0))),
                    Parameter::new_with_default("B", NumberType(U16Kind), Number(U16Value(1))),
                    Parameter::new_with_default("C", NumberType(U16Kind), Number(U16Value(2))),
                ]));
        }

        // #[test]
        // fn test_enums_custom_i64() {
        //     verify_type_construction(
        //         "Enum(AMEX = 10, NASDAQ = 20, NYSE = 30, OTCBB = 40)",
        //         EnumType(vec![
        //             Parameter::new_with_default("AMEX", NumberType(I64Kind), Number(I64Value(10))),
        //             Parameter::new_with_default("NASDAQ", NumberType(I64Kind), Number(I64Value(20))),
        //             Parameter::new_with_default("NYSE", NumberType(I64Kind), Number(I64Value(30))),
        //             Parameter::new_with_default("OTCBB", NumberType(I64Kind), Number(I64Value(40))),
        //         ]));
        // }

        // #[test]
        // fn test_enums_custom_char() {
        //     verify_type_construction(
        //         "Enum(AMEX = 'A', NASDAQ = 'N', NYSE = 'Y', OTCBB = 'O')",
        //         EnumType(vec![
        //             Parameter::new_with_default("AMEX", CharType, CharValue('A')),
        //             Parameter::new_with_default("NASDAQ", CharType, CharValue('N')),
        //             Parameter::new_with_default("NYSE", CharType, CharValue('Y')),
        //             Parameter::new_with_default("OTCBB", CharType, CharValue('O')),
        //         ]));
        // }

        #[test]
        fn test_f64() {
            verify_type_construction("f64", NumberType(F64Kind));
        }

        #[test]
        fn test_fn() {
            verify_type_construction(
                "fn(symbol: String(8), exchange: String(8), last_sale: f64)",
                FunctionType(make_quote_parameters(), Box::from(RuntimeResolvedType)));
        }

        #[test]
        fn test_i64() {
            verify_type_construction("i64", NumberType(I64Kind));
        }

        #[test]
        fn test_i128() {
            verify_type_construction("i128", NumberType(I128Kind));
        }

        #[test]
        fn test_tuple_3() {
            verify_type_construction("(f64, i64, u64)", TupleType(vec![
                NumberType(F64Kind),
                NumberType(I64Kind),
                NumberType(U64Kind),
            ]));
        }

        #[test]
        fn test_string() {
            verify_type_construction("String", StringType);
            verify_type_construction("String(10)", FixedSizeType(StringType.into(), 10));
        }

        #[test]
        fn test_struct() {
            verify_type_construction(
                "Struct(symbol: String(8), exchange: String(8), last_sale: f64)",
                StructureType(make_quote_parameters()));
        }

        #[test]
        fn test_table() {
            verify_type_construction(
                "Table(symbol: String(8), exchange: String(8), last_sale: f64)",
                TableType(make_quote_parameters()));
        }

        #[test]
        fn test_u128() {
            verify_type_construction("u128", NumberType(U128Kind));
        }

        fn verify_type_construction(type_decl: &str, data_type: DataType) {
            let dt: DataType = DataType::from_str(type_decl)
                .expect(format!("Failed to parse type {}", data_type).as_str());
            assert_eq!(dt, data_type);
            assert_eq!(data_type.to_code(), type_decl);
            assert_eq!(format!("{}", data_type), type_decl.to_string())
        }

        fn verify_type_construction_ab(type_decl_a: &str, type_decl_b: &str, data_type: DataType) {
            let dt: DataType = DataType::from_str(type_decl_a)
                .expect(format!("Failed to parse type {}", data_type).as_str());
            assert_eq!(dt, data_type);
            assert_eq!(data_type.to_code(), type_decl_b);
            assert_eq!(format!("{}", data_type), type_decl_b.to_string())
        }
    }

    /// Instantiation tests
    #[cfg(test)]
    mod instantiation_tests {
        use crate::connections::Connections::{BLOBStoreHandle, WebSocketHandle};
        use crate::data_types::DataType::NumberType;
        use crate::dataframe::Dataframe::ModelTable;
        use crate::errors::Errors;
        use crate::errors::Errors::Exact;
        use crate::model_row_collection::ModelRowCollection;
        use crate::number_kind::NumberKind::U16Kind;
        use crate::numbers::Numbers::*;
        use crate::parameter::Parameter;
        use crate::sequences::Array;
        use crate::test_util::*;
        use crate::typed_values::TypedValue::*;

        #[test]
        fn test_array() {
            verify_exact_value(r#"
                Array::new()
            "#, ArrayValue(Array::new()));
        }

        #[test]
        fn test_array_from_values() {
            verify_exact_value(r#"
                Array::new(1, 3, 5, 7, 9)
            "#, ArrayValue(Array::from(vec![
                Number(I64Value(1)),
                Number(I64Value(3)),
                Number(I64Value(5)),
                Number(I64Value(7)),
                Number(I64Value(9)),
            ])));
        }

        #[test]
        fn test_blob_store() {
            verify_exact_value(r#"
                BLOBStore::new(9643b4f6-4577-4338-a6ea-123125dab90d)
            "#, Connection(BLOBStoreHandle(0x9643b4f6_4577_4338_a6ea_123125dab90du128)));
        }

        #[test]
        fn test_boolean() {
            verify_exact_value(r#"
                Boolean::new()
            "#, Boolean(false));
        }

        #[test]
        fn test_boolean_from_literal_false() {
            verify_exact_value(r#"
                Boolean::new(false)
            "#, Boolean(false));
        }

        #[test]
        fn test_boolean_from_literal_true() {
            verify_exact_value(r#"
                Boolean::new(true)
            "#, Boolean(true));
        }

        #[test]
        fn test_bytes() {
            verify_exact_value(r#"
                Bytes::new()
            "#, ByteStringValue(vec![]));
        }

        #[test]
        fn test_bytes_from_bytes() {
            verify_exact_value(r#"
                Bytes::new(0B68656c6c6f)
            "#, ByteStringValue(vec![
                0x68, 0x65, 0x6c, 0x6c, 0x6f
            ]));
        }

        #[test]
        fn test_bytes_from_chars() {
            verify_exact_value(r#"
                Bytes::new("hello")
            "#, ByteStringValue(vec![
                0x68, 0x65, 0x6c, 0x6c, 0x6f
            ]));
        }

        #[test]
        fn test_char() {
            verify_exact_value(r#"
                Char::new()
            "#, CharValue('\0'));
        }

        #[test]
        fn test_char_from_literal() {
            verify_exact_value(r#"
                Char::new('A')
            "#, CharValue('A'));
        }

        #[test]
        fn test_datetime() {
            verify_exact_value_where(r#"
                DateTime::new()
            "#, |v| matches!(v, DateTimeValue(..)));
        }

        #[test]
        fn test_datetime_from_epoch() {
            verify_exact_code(r#"
                DateTime::new(1752122250353)
            "#, "2025-07-10T04:37:30.353Z");
        }

        #[test]
        fn test_datetime_from_iso8601() {
            verify_exact_value(r#"
                DateTime::new(2025-07-10T04:37:30.353Z)
            "#, DateTimeValue(1752122250353));
        }

        #[test]
        fn test_enum_default() {
            verify_exact_value(r#"
                Enum(AMEX, NYSE, NASDAQ, OTCBB)::new
            "#, EnumValue(None, vec![
                Parameter::new_with_default("AMEX", NumberType(U16Kind), Number(U16Value(0))),
                Parameter::new_with_default("NYSE", NumberType(U16Kind), Number(U16Value(1))),
                Parameter::new_with_default("NASDAQ", NumberType(U16Kind), Number(U16Value(2))),
                Parameter::new_with_default("OTCBB", NumberType(U16Kind), Number(U16Value(3))),
            ]));
        }

        #[test]
        fn test_enum_by_index() {
            verify_exact_value(r#"
                Enum(AMEX, NYSE, NASDAQ, OTCBB)::new(2)
            "#, EnumValue(Some(2), vec![
                Parameter::new_with_default("AMEX", NumberType(U16Kind), Number(U16Value(0))),
                Parameter::new_with_default("NYSE", NumberType(U16Kind), Number(U16Value(1))),
                Parameter::new_with_default("NASDAQ", NumberType(U16Kind), Number(U16Value(2))),
                Parameter::new_with_default("OTCBB", NumberType(U16Kind), Number(U16Value(3))),
            ]));
        }

        #[test]
        fn test_enum_by_name() {
            verify_exact_value(r#"
                Enum(AMEX, NYSE, NASDAQ, OTCBB)::new("NYSE")
            "#, EnumValue(Some(1), vec![
                Parameter::new_with_default("AMEX", NumberType(U16Kind), Number(U16Value(0))),
                Parameter::new_with_default("NYSE", NumberType(U16Kind), Number(U16Value(1))),
                Parameter::new_with_default("NASDAQ", NumberType(U16Kind), Number(U16Value(2))),
                Parameter::new_with_default("OTCBB", NumberType(U16Kind), Number(U16Value(3))),
            ]));
        }

        #[test]
        fn test_enum_to_u16() {
            verify_exact_code(r#"
                let exchange = Enum(AMEX, NYSE, NASDAQ, OTCBB)::new(2)
                exchange::to(u16)
            "#, "2");
        }

        #[test]
        fn test_enum_to_string() {
            verify_exact_code(r#"
                let exchange = Enum(AMEX, NYSE, NASDAQ, OTCBB)::new(3)
                exchange::to(String)
            "#, "\"OTCBB\"");
        }

        #[test]
        fn test_error() {
            verify_exact_value(r#"
                Error::new()
            "#, ErrorValue(Errors::Empty));
        }

        #[test]
        fn test_error_from_literal() {
            verify_exact_value(r#"
                Error::new("Boom!")
            "#, ErrorValue(Exact("Boom!".into())));
        }

        #[test]
        fn test_f64() {
            verify_exact_value(r#"
                f64::new()
            "#, Number(F64Value(0.)));
        }

        #[test]
        fn test_f64_from_literal() {
            verify_exact_value(r#"
                f64::new(0.142857)
            "#, Number(F64Value(0.142857)));
        }

        #[test]
        fn test_i8() {
            verify_exact_value(r#"
                i8::new()
            "#, Number(I8Value(0)));
        }

        #[test]
        fn test_i8_from_literal() {
            verify_exact_value(r#"
                i8::new(-77)
            "#, Number(I8Value(-77)));
        }

        #[test]
        fn test_i16() {
            verify_exact_value(r#"
                i16::new()
            "#, Number(I16Value(0)));
        }

        #[test]
        fn test_i16_from_literal() {
            verify_exact_value(r#"
                i16::new(-7779)
            "#, Number(I16Value(-7779)));
        }

        #[test]
        fn test_i32() {
            verify_exact_value(r#"
                i32::new()
            "#, Number(I32Value(0)));
        }

        #[test]
        fn test_i32_from_literal() {
            verify_exact_value(r#"
                i32::new(-7779311)
            "#, Number(I32Value(-7779311)));
        }

        #[test]
        fn test_i64() {
            verify_exact_value(r#"
                i64::new()
            "#, Number(I64Value(0)));
        }

        #[test]
        fn test_i64_from_literal() {
            verify_exact_value(r#"
                i64::new(-7779311)
            "#, Number(I64Value(-7779311)));
        }

        #[test]
        fn test_i128() {
            verify_exact_value(r#"
                i128::new()
            "#, Number(I128Value(0)));
        }

        #[test]
        fn test_i128_from_literal() {
            verify_exact_value(r#"
                i128::new(-818_7779_311)
            "#, Number(I128Value(-818_7779_311)));
        }

        #[test]
        fn test_u8() {
            verify_exact_value(r#"
                u8::new()
            "#, Number(U8Value(0)));
        }

        #[test]
        fn test_u8_from_literal() {
            verify_exact_value(r#"
                u8::new(0x7f)
            "#, Number(U8Value(0x7f)));
        }

        #[test]
        fn test_u16() {
            verify_exact_value(r#"
                u16::new()
            "#, Number(U16Value(0)));
        }

        #[test]
        fn test_u16_from_literal() {
            verify_exact_value(r#"
                u16::new(65535)
            "#, Number(U16Value(65535)));
        }

        #[test]
        fn test_u32() {
            verify_exact_value(r#"
                u32::new()
            "#, Number(U32Value(0)));
        }

        #[test]
        fn test_u32_from_literal() {
            verify_exact_value(r#"
                u32::new(65535)
            "#, Number(U32Value(65535)));
        }

        #[test]
        fn test_u64() {
            verify_exact_value(r#"
                u64::new()
            "#, Number(U64Value(0)));
        }

        #[test]
        fn test_u64_from_literal() {
            verify_exact_value(r#"
                u64::new(0xdeadbeefcafebabe)
            "#, Number(U64Value(0xdeadbeefcafebabe)));
        }

        #[test]
        fn test_u128() {
            verify_exact_value(r#"
                u128::new()
            "#, Number(U128Value(0)));
        }

        #[test]
        fn test_u128_from_literal() {
            verify_exact_value(r#"
                u128::new(0xbabe_face_cafe_badd)
            "#, Number(U128Value(0xbabe_face_cafe_badd)));
        }

        #[test]
        fn test_number() {
            verify_exact_value(r#"
                Number::new()
            "#, Number(I64Value(0)));
        }

        #[test]
        fn test_number_from_i64_literal() {
            verify_exact_value(r#"
                Number::new(5)
            "#, Number(I64Value(5)));
        }

        #[test]
        fn test_number_from_f64_literal() {
            verify_exact_value(r#"
                Number::new(76.9)
            "#, Number(F64Value(76.9)));
        }

        #[test]
        fn test_string() {
            verify_exact_value(r#"
                String::new()
            "#, StringValue("".into()));
        }

        #[test]
        fn test_string_from_value() {
            verify_exact_value(r#"
                String::new("Hello")
            "#, StringValue("Hello".into()));
        }

        #[test]
        fn test_struct() {
            verify_exact_unwrapped(r#"
                let StockT = Struct(symbol: String(8), exchange: String(8), last_sale: f64)
                StockT::new("ABC", "AMEX", 34.56)
            "#, r#"{"exchange":"AMEX","last_sale":34.56,"symbol":"ABC"}"#);
        }

        #[test]
        fn test_struct_with_defaults() {
            verify_exact_unwrapped(r#"
                Struct(
                    symbol: String(8) = "ABC", 
                    exchange: String(8) = "AMEX", 
                    last_sale: f64 = 6.7123
                )::new
            "#, r#"{"exchange":"AMEX","last_sale":6.7123,"symbol":"ABC"}"#);
        }

        #[test]
        fn test_table() {
            verify_exact_value(r#"
                Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
            "#, TableValue(ModelTable(ModelRowCollection::new(make_quote_columns()))));
        }

        #[test]
        fn test_tuple_named() {
            verify_exact_value(r#"
                let StockT = Tuple(String(8), String(8), f64)
                StockT::new("ABC", "NYSE", 17.11)
            "#, TupleValue(vec![
                StringValue("ABC".into()),
                StringValue("NYSE".into()),
                Number(F64Value(17.11))
            ]));
        }

        #[test]
        fn test_tuple_unnamed() {
            verify_exact_value(r#"
                let StockT = (String(8), String(8), f64)
                StockT::new("ABC", "NYSE", 17.11)
            "#, TupleValue(vec![
                StringValue("ABC".into()),
                StringValue("NYSE".into()),
                Number(F64Value(17.11))
            ]));
        }

        #[test]
        fn test_uuid() {
            verify_exact_value_where(r#"
                UUID::new()
            "#, |v| matches!(v, UUIDValue(..)));
        }

        #[test]
        fn test_uuid_from_value() {
            verify_exact_value(r#"
                UUID::new(0Bf1f465f5e6fd4a5ab3fdd30640fe4647)
            "#, UUIDValue(0xf1f465f5e6fd4a5ab3fdd30640fe4647u128));
        }

        #[test]
        fn test_websocket() {
            verify_exact_value(r#"
                WebSocket::new(9643b4f6-4577-4338-a6ea-123125dab90d)
            "#, Connection(WebSocketHandle(0x9643b4f6_4577_4338_a6ea_123125dab90du128)));
        }
    }

    /// Type tests
    #[cfg(test)]
    mod type_of_tests {
        use crate::test_util::verify_exact_code;

        #[test]
        fn test_type_of_user_defined_type() {
            verify_exact_code(r#"
                Cards = Table(face: String(2), suit: String(2))
                Cards::get_type
            "#, "Table(face: String(2), suit: String(2))");
        }
    }
}