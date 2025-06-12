#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// DataType class
////////////////////////////////////////////////////////////////////

use crate::byte_code_compiler::ByteCodeCompiler;
use crate::compiler;
use crate::compiler::Compiler;
use crate::data_types::DataType::*;
use crate::dataframe::Dataframe;
use crate::dataframe::Dataframe::Model;
use crate::errors::Errors::{Exact, SyntaxError, TypeMismatch};
use crate::errors::TypeMismatchErrors::{ArgumentsMismatched, UnrecognizedTypeName};
use crate::errors::{throw, Errors, SyntaxErrors};
use crate::expression::Expression;
use crate::expression::Expression::*;
use crate::field::FieldMetadata;
use crate::model_row_collection::ModelRowCollection;
use crate::number_kind::NumberKind;
use crate::number_kind::NumberKind::*;
use crate::numbers::Numbers::I64Value;
use crate::parameter::Parameter;
use crate::platform::PackageOps;
use crate::row_collection::RowCollection;
use crate::sequences::Array;
use crate::structures::Structures::Hard;
use crate::structures::{HardStructure, Structure};
use crate::typed_values::TypedValue;
use chrono::Local;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display};
use std::ops::Deref;
use uuid::Uuid;

const PTR_LEN: usize = 8;

/// Represents an Oxide-native datatype
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum DataType {
    ArrayType,
    BinaryType,
    BooleanType,
    DateTimeType,
    EnumType(Vec<Parameter>),
    ErrorType,
    FixedSizeType(Box<DataType>, usize),
    FunctionType(Vec<Parameter>, Box<DataType>),
    NumberType(NumberKind),
    PlatformOpsType(PackageOps),
    StringType,
    StructureType(Vec<Parameter>),
    TableType(Vec<Parameter>),
    TupleType(Vec<DataType>),
    UnresolvedType, // Polymorphic
    UUIDType,
}

impl DataType {
    ////////////////////////////////////////////////////////////////////
    //  STATIC METHODS
    ////////////////////////////////////////////////////////////////////

    pub fn are_compatible(
        types_a: &Vec<DataType>,
        types_b: &Vec<DataType>,
    ) -> bool {
        types_a.iter().zip(types_b.iter()).all(|(a, b)| a.is_compatible(b))
    }

    /// provides type resolution for the given [Vec<DataType>]
    pub fn best_fit(types: Vec<DataType>) -> DataType {
        match types.len() {
            0 => Self::UnresolvedType,
            1 => types[0].to_owned(),
            _ => types[1..].iter().fold(types[0].to_owned(), |agg, t|
                match (agg, t) {
                    (FixedSizeType(a, size_a), FixedSizeType(b, size_b)) if a == *b => 
                        FixedSizeType(a.to_owned(), size_a.max(*size_b)),
                    (_, t) => t.to_owned()
                })
        }
    }

    /// deciphers a datatype from an expression (e.g. "String" | "String(20)")
    pub fn decipher_type(model: &Expression) -> std::io::Result<DataType> {
        decipher_model(model)
    }

    /// decodes the typed value based on the supplied data type and buffer
    pub fn decode(&self, buffer: &Vec<u8>, offset: usize) -> TypedValue {
        use crate::typed_values::TypedValue::*;
        match self {
            ArrayType => ArrayValue(Array::new()),
            BinaryType => BinaryValue(Vec::new()),
            BooleanType => ByteCodeCompiler::decode_u8(buffer, offset, |b| Boolean(b == 1)),
            DateTimeType => ByteCodeCompiler::decode_u8x8(buffer, offset, |b| DateValue(i64::from_be_bytes(b))),
            EnumType(_) => ByteCodeCompiler::decode_value(&buffer[offset..].to_vec()),
            ErrorType => ErrorValue(Exact(ByteCodeCompiler::decode_string(buffer, offset, 255).to_string())),
            FixedSizeType(data_type, size) => match data_type.deref() {
                StringType => StringValue(ByteCodeCompiler::decode_string(buffer, offset, *size).to_string()),
                TableType(params) => TableValue(Dataframe::Model(ModelRowCollection::from_bytes(params, buffer.to_vec()))),
                _ => data_type.decode(buffer, offset)
            },
            FunctionType(params, returns) => Function { 
                params: params.to_owned(),
                body: Literal(ByteCodeCompiler::decode_value(&buffer[offset..].to_vec())).into(),
                returns: returns.deref().to_owned(),
            },
            NumberType(kind) => Number(kind.decode(buffer, offset)),
            PlatformOpsType(pf) => PlatformOp(pf.to_owned()),
            StringType => StringValue(ByteCodeCompiler::decode_string(buffer, offset, 255)),
            StructureType(params) => Structured(Hard(HardStructure::from_parameters(params.to_vec()))),
            TableType(params) => TableValue(Dataframe::Model(ModelRowCollection::from_bytes(params, buffer.to_vec()))),
            TupleType(params) => TupleValue(params.iter().map(|dt| dt.decode(buffer, offset)).collect()),
            Self::UnresolvedType => ByteCodeCompiler::decode_value(&buffer[offset..].to_vec()),
            Self::UUIDType => ByteCodeCompiler::decode_u8x16(buffer, offset, |b| UUIDValue(u128::from_be_bytes(b))),
        }
    }

    /// decodes the typed value based on the supplied data type and buffer
    pub fn decode_bcc(&self, bcc: &mut ByteCodeCompiler) -> std::io::Result<TypedValue> {
        use crate::typed_values::TypedValue::*;
        let tv = match self {
            ArrayType => ArrayValue(Array::from(bcc.next_array()?)),
            BinaryType => BinaryValue(bcc.next_blob()),
            BooleanType => Boolean(bcc.next_bool()),
            DateTimeType => DateValue(bcc.next_i64()),
            EnumType(labels) => {
                let index = bcc.next_u32() as usize;
                StringValue(labels[index].get_name().to_string())
            }
            ErrorType => ErrorValue(Exact(bcc.next_string())),
            FixedSizeType(data_type, size) => {
                // TODO revisit
                data_type.decode_bcc(bcc)?
            }
            FunctionType(columns, returns) => Function {
                params: columns.to_owned(),
                body: Box::new(ByteCodeCompiler::disassemble(bcc)?),
                returns: returns.deref().clone(),
            },
            NumberType(kind) => Number(kind.decode_buffer(bcc)?),
            PlatformOpsType(pf) => PlatformOp(pf.to_owned()),
            StringType => StringValue(bcc.next_string()),
            StructureType(params) => Structured(Hard(bcc.next_struct_with_parameters(params.to_vec())?)),
            TableType(columns, ..) => TableValue(Model(bcc.next_table_with_columns(columns)?)),
            TupleType(..) => TupleValue(bcc.next_array()?),
            DataType::UnresolvedType => Undefined,
            DataType::UUIDType => UUIDValue(bcc.next_u128()),
        };
        Ok(tv)
    }

    pub fn decode_field_value(&self, buffer: &Vec<u8>, offset: usize) -> TypedValue {
        let metadata = FieldMetadata::decode(buffer[offset]);
        if metadata.is_active {
            self.decode(buffer, offset + 1)
        } else { TypedValue::Null }
    }

    pub fn decode_field_value_bcc(&self, bcc: &mut ByteCodeCompiler, offset: usize) -> std::io::Result<TypedValue> {
        let metadata: FieldMetadata = FieldMetadata::decode(bcc[offset]);
        let value: TypedValue = if metadata.is_active {
            self.decode_bcc(bcc)?
        } else { TypedValue::Null };
        Ok(value)
    }

    pub fn encode(&self, value: &TypedValue) -> std::io::Result<Vec<u8>> {
        Ok(value.encode())
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
        Self::decipher_type(&model)
    }
    
    ////////////////////////////////////////////////////////////////////
    //  INSTANCE METHODS
    ////////////////////////////////////////////////////////////////////

    /// computes and returns the maximum physical size of this datatype
    pub fn compute_fixed_size(&self) -> usize {
        use crate::data_types::DataType::*;
        let width: usize = match self {
            ArrayType => PTR_LEN,
            BinaryType => PTR_LEN,
            BooleanType => 1,
            DateTimeType => 8,
            EnumType(..) => 2,
            ErrorType => 256,
            FixedSizeType(data_type, size) =>
                match data_type.deref() {
                    BinaryType | StringType =>
                        match size {
                            size => *size + size.to_be_bytes().len(),
                            0 => PTR_LEN
                        }
                    _ => data_type.compute_fixed_size() * size
                }
            FunctionType(columns, ..) => columns.len() * PTR_LEN,
            NumberType(nk) => nk.compute_fixed_size(),
            PlatformOpsType(..) => 4,
            StringType => PTR_LEN,
            StructureType(columns) => columns.len() * PTR_LEN,
            TableType(columns) => columns.len() * PTR_LEN,
            TupleType(types) => types.iter().map(|t| t.compute_fixed_size()).sum(),
            Self::UnresolvedType => PTR_LEN,
            Self::UUIDType => 16,
        };
        width + 1 // +1 for field metadata
    }

    pub fn get_default_value(&self) -> TypedValue {
        use crate::typed_values::TypedValue::*;
        match self {
            ArrayType => ArrayValue(Array::new()),
            BinaryType => BinaryValue(vec![]),
            BooleanType => Boolean(false),
            DateTimeType => DateValue(Local::now().timestamp_millis()),
            EnumType(..) => Number(I64Value(0)),
            ErrorType => ErrorValue(Errors::Empty),
            FixedSizeType(data_type, _) => data_type.get_default_value(),
            FunctionType(params, returns) => Function {
                params: params.to_vec(),
                body: Box::new(Literal(returns.get_default_value())),
                returns: returns.deref().clone(),
            },
            NumberType(kind) => Number(kind.get_default_value()),
            PlatformOpsType(kind) => PlatformOp(kind.clone()),
            StringType => StringValue(String::new()),
            StructureType(params) =>
                Structured(Hard(HardStructure::from_parameters(params.to_vec()))),
            TableType(params) =>
                TableValue(Model(ModelRowCollection::from_parameters(params))),
            TupleType(dts) => TupleValue(dts.iter()
                .map(|dt| dt.get_default_value()).collect()),
            Self::UnresolvedType => Null,
            Self::UUIDType => UUIDValue(Uuid::new_v4().as_u128())
        }
    }

    pub fn is_compatible(&self, other: &DataType) -> bool {
        match (self, other) {
            (ArrayType, ArrayType) |
            (BinaryType, BinaryType) |
            (StringType, StringType) |
            (UnresolvedType, _) | (_, UnresolvedType) => true,
            (EnumType(a), EnumType(b)) => Parameter::are_compatible(a, b),
            (FixedSizeType(a, _), b) => a.is_compatible(b),
            (a, FixedSizeType(b, _)) => a.is_compatible(b),
            (FunctionType(a, dta), FunctionType(b, dtb)) =>
                Parameter::are_compatible(a, b) && dta.is_compatible(dtb),
            (NumberType(..), NumberType(..)) => true,
            // TODO add a case for when the source struct|table has a subset of the target fields?
            (StructureType(a) | TableType(a), StructureType(b) | TableType(b)) =>
                Parameter::are_compatible(a, b),
            (TupleType(a), TupleType(b)) => DataType::are_compatible(a, b),
            (a, b) => a == b,
        }
    }

    pub fn is_table(&self) -> bool {
        match self {
            TableType(..) => true,
            FixedSizeType(underlying, ..) => underlying.is_table(),
            _ => false
        }
    }

    /// Indicates whether the given name represents a known type
    pub fn is_type_name(name: &str) -> bool {
        decipher_model_variable(name).is_ok()
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
            ArrayType => "Array".into(),
            BinaryType => "Binary".into(), // UTF8
            BooleanType => "Boolean".into(),
            DateTimeType => "Date".into(),
            EnumType(labels) => parameterized("Enum", labels, true),
            ErrorType => "Error".into(),
            FixedSizeType(data_type, size) => sized(data_type.to_code().as_str(), *size),
            FunctionType(params, returns) =>
                format!("fn({}){}", Parameter::render(params),
                        match returns.to_code() {
                            s if !s.is_empty() => format!(": {}", s),
                            _ => String::new()
                        }),
            NumberType(nk) => nk.get_type_name(),
            PlatformOpsType(pf) => pf.to_code(),
            StringType => "String".into(),
            StructureType(params) => parameterized("Struct", params, false),
            TableType(params) => parameterized("Table", params, false),
            TupleType(types) => typed("", types),
            Self::UnresolvedType => String::new(),
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

fn decipher_model(model: &Expression) -> std::io::Result<DataType> {
    use crate::typed_values::TypedValue::Structured;
    match model {
        // e.g. [String, String, f64]
        ArrayExpression(params) => decipher_model_array(params),
        // e.g. String(80)
        FunctionCall { fx, args } => decipher_model_function_call(fx, args),
        // e.g. Struct(symbol: String, exchange: String, last_sale: f64)
        Literal(Structured(s)) => Ok(StructureType(s.get_parameters())),
        // e.g. fn(a, b, c)
        Literal(TypedValue::Kind(data_type)) => Ok(data_type.clone()),
        // e.g. (f64, f64, f64)
        TupleExpression(params) => decipher_model_tuple(params),
        // e.g. i64
        Variable(name) => decipher_model_variable(name),
        other => throw(SyntaxError(SyntaxErrors::TypeIdentifierExpected(other.to_code())))
    }
}

fn decipher_model_array(items: &Vec<Expression>) -> std::io::Result<DataType> {
    let mut kinds = vec![];
    for item in items {
        kinds.push(decipher_model(item)?); 
    }
    Ok(FixedSizeType(ArrayType.into(), kinds.len()))
}

fn decipher_model_function_call(
    fx: &Expression,
    args: &Vec<Expression>,
) -> std::io::Result<DataType> {
    use crate::typed_values::TypedValue::Number;

    fn expect_size(args: &Vec<Expression>, data_type: DataType) -> std::io::Result<DataType> {
        match args.as_slice() {
            [] => Ok(data_type),
            [Literal(Number(n))] => Ok(FixedSizeType(data_type.into(), n.to_usize())),
            [other] => throw(SyntaxError(SyntaxErrors::TypeIdentifierExpected(other.to_code()))),
            other => throw(TypeMismatch(ArgumentsMismatched(1, other.len())))
        }
    }
    fn expect_types(args: &Vec<Expression>, f: fn(Vec<DataType>) -> DataType) -> std::io::Result<DataType> {
        let mut data_types = vec![];
        for arg in args {
            data_types.push(decipher_model(arg)?);
        }
        Ok(f(data_types))
    }
    fn expect_params(args: &Vec<Expression>, f: fn(Vec<Parameter>) -> DataType) -> std::io::Result<DataType> {
        let params = compiler::convert_to_parameters(args.to_vec())?;
        Ok(f(params))
    }

    // decode the type
    match fx {
        Variable(name) =>
            match name.as_str() {
                "Array" => expect_size(args, ArrayType),
                "Binary" => expect_size(args, BinaryType),
                "Enum" => expect_params(args, |params| EnumType(params)),
                "fn" => expect_params(args, |params| FunctionType(params, UnresolvedType.into())),
                "String" => expect_size(args, StringType),
                "Struct" => expect_params(args, |params| StructureType(params)),
                "Table" => expect_params(args, |params| TableType(params)),
                "Tuple" => expect_types(args, |types| TupleType(types)),
                type_name if args.is_empty() => decipher_model_variable(type_name),
                type_name => throw(SyntaxError(SyntaxErrors::TypeIdentifierExpected(type_name.into())))
            }
        other => throw(SyntaxError(SyntaxErrors::TypeIdentifierExpected(other.to_code())))
    }
}

fn decipher_model_tuple(items: &Vec<Expression>) -> std::io::Result<DataType> {
    let mut kinds = vec![];
    for item in items {
        kinds.push(decipher_model(item)?);
    }
    Ok(TupleType(kinds))
}

fn decipher_model_variable(name: &str) -> std::io::Result<DataType> {
    match name {
        "Boolean" => Ok(BooleanType),
        "Date" => Ok(DateTimeType),
        "Enum" => Ok(EnumType(vec![])),
        "Error" => Ok(ErrorType),
        "f64" => Ok(NumberType(F64Kind)),
        "Fn" => Ok(FunctionType(vec![], Box::new(UnresolvedType))),
        "i64" => Ok(NumberType(I64Kind)),
        "i128" => Ok(NumberType(I128Kind)),
        "RowId" => Ok(NumberType(RowIdKind)),
        "String" => Ok(StringType),
        "Struct" => Ok(StructureType(vec![])),
        "Table" => Ok(TableType(vec![])),
        "UUID" => Ok(UUIDType),
        "u128" => Ok(NumberType(U128Kind)),
        type_name => throw(TypeMismatch(UnrecognizedTypeName(type_name.to_string())))
    }
}

/// Unit tests
#[cfg(test)]
mod tests {
    /// Compatible Types Unit tests
    mod compatible_types_tests {
        use crate::data_types::DataType;
        use crate::data_types::DataType::*;
        use crate::number_kind::NumberKind::*;

        #[test]
        fn test_arrays() {
            verify_compatibility(
                FixedSizeType(ArrayType.into(), 800), 
                ArrayType)
        }

        #[test]
        fn test_ascii_and_strings() {
            verify_compatibility(FixedSizeType(StringType.into(), 80), StringType);
        }

        #[test]
        fn test_best_fit() {
            let kind = DataType::best_fit(vec![
                FixedSizeType(StringType.into(), 11),
                FixedSizeType(StringType.into(), 110),
                FixedSizeType(StringType.into(), 55)
            ]);
            assert_eq!(kind, FixedSizeType(StringType.into(), 110))
        }

        #[test]
        fn test_numbers() {
            verify_compatibility(NumberType(I64Kind), NumberType(I64Kind));
            verify_incompatibility(NumberType(I64Kind), BooleanType);
        }

        fn verify_compatibility(a: DataType, b: DataType) {
            println!("Verifying compatibility of {} and {}", a, b);
            assert!(a.is_compatible(&b));
        }

        fn verify_incompatibility(a: DataType, b: DataType) {
            assert!(!a.is_compatible(&b));
        }
    }

    /// Compilation Unit tests
    mod compilation_tests {
        use crate::data_types::DataType;
        use crate::data_types::DataType::*;
        use crate::number_kind::NumberKind::*;
        use crate::numbers::Numbers::I64Value;
        use crate::parameter::Parameter;
        use crate::testdata::make_quote_parameters;
        use crate::typed_values::TypedValue::Number;

        #[test]
        fn test_array() {
            verify_type_construction("Array(12)", FixedSizeType(ArrayType.into(), 12));
        }

        #[test]
        fn test_binary() {
            verify_type_construction("Binary(5566)", FixedSizeType(BinaryType.into(), 5566));
        }

        #[test]
        fn test_boolean() {
            verify_type_construction("Boolean", BooleanType);
        }

        #[test]
        fn test_date() {
            verify_type_construction("Date", DateTimeType);
        }

        #[test]
        fn test_enums_0() {
            verify_type_construction(
                "Enum(A, B, C)",
                EnumType(vec![
                    Parameter::add("A"),
                    Parameter::add("B"),
                    Parameter::add("C"),
                ]));
        }

        #[test]
        fn test_enums_1() {
            verify_type_construction(
                "Enum(AMEX = 1, NASDAQ = 2, NYSE = 3, OTCBB = 4)",
                EnumType(vec![
                    Parameter::new_with_default("AMEX", NumberType(I64Kind), Number(I64Value(1))),
                    Parameter::new_with_default("NASDAQ", NumberType(I64Kind), Number(I64Value(2))),
                    Parameter::new_with_default("NYSE", NumberType(I64Kind), Number(I64Value(3))),
                    Parameter::new_with_default("OTCBB", NumberType(I64Kind), Number(I64Value(4))),
                ]));
        }

        #[test]
        fn test_f64() {
            verify_type_construction("f64", NumberType(F64Kind));
        }

        #[test]
        fn test_fn() {
            verify_type_construction(
                "fn(symbol: String(8), exchange: String(8), last_sale: f64)",
                FunctionType(make_quote_parameters(), Box::from(UnresolvedType)));
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
            verify_type_construction("(i64, i64, i64)", TupleType(vec![
                NumberType(I64Kind),
                NumberType(I64Kind),
                NumberType(I64Kind),
            ]));
        }

        #[test]
        fn test_outcome() {
            verify_type_construction("RowId", NumberType(RowIdKind));
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

        fn verify_type_construction(typedef: &str, data_type: DataType) {
            let dt: DataType = DataType::from_str(typedef)
                .expect(format!("Failed to parse type {}", data_type).as_str());
            assert_eq!(dt, data_type);
            assert_eq!(data_type.to_code(), typedef);
            assert_eq!(format!("{}", data_type), typedef.to_string())
        }
    }

    /// Default-value Unit tests
    mod defaults_tests {
        use crate::data_types::DataType::*;
        use crate::dataframe::Dataframe::Model;
        use crate::model_row_collection::ModelRowCollection;
        use crate::number_kind::NumberKind::*;
        use crate::numbers::Numbers::*;
        use crate::testdata::{make_quote_columns, make_quote_parameters};
        use crate::typed_values::TypedValue::*;

        #[test]
        fn test_get_default_value_binary() {
            assert!(matches!(
                FixedSizeType(BinaryType.into(), 128).get_default_value(),
                BinaryValue(..)
            ));
        }

        #[test]
        fn test_get_default_value_date() {
            assert!(matches!(
                DateTimeType.get_default_value(),
                DateValue(..)
            ));
        }

        #[test]
        fn test_get_default_value_enum() {
            assert!(matches!(
                EnumType(make_quote_parameters()).get_default_value(),
                Number(I64Value(0))
            ));
        }

        #[test]
        fn test_get_default_value_i32() {
            assert_eq!(NumberType(I64Kind).get_default_value(), Number(I64Value(0)));
        }

        #[test]
        fn test_get_default_value_i64() {
            assert_eq!(NumberType(I64Kind).get_default_value(), Number(I64Value(0)));
        }

        #[test]
        fn test_get_default_value_i128() {
            assert_eq!(NumberType(I128Kind).get_default_value(), Number(I128Value(0)));
        }

        #[test]
        fn test_get_default_value_u128() {
            assert_eq!(NumberType(U128Kind).get_default_value(), Number(U128Value(0)));
        }

        #[test]
        fn test_get_default_value_uuid() {
            assert!(matches!(UUIDType.get_default_value(), UUIDValue(..)));
        }

        #[test]
        fn test_get_default_value_string() {
            assert_eq!(StringType.get_default_value(), StringValue(String::new()));
        }

        #[test]
        fn test_get_default_value_table() {
            assert_eq!(
                TableType(make_quote_parameters()).get_default_value(),
                TableValue(Model(ModelRowCollection::new(make_quote_columns()))));
        }
    }

    /// Instantiation tests
    #[cfg(test)]
    mod instantiation_tests {
        use crate::compiler::Compiler;
        use crate::data_types::DataType::{FixedSizeType, NumberType, StringType};
        use crate::dataframe::Dataframe::Model;
        use crate::errors::Errors;
        use crate::model_row_collection::ModelRowCollection;
        use crate::number_kind::NumberKind::F64Kind;
        use crate::numbers::Numbers::*;
        use crate::parameter::Parameter;
        use crate::sequences::Array;
        use crate::structures::HardStructure;
        use crate::structures::Structures::Hard;
        use crate::testdata::{make_quote_columns, verify_exact_value, verify_exact_value_where};
        use crate::typed_values::TypedValue;
        use crate::typed_values::TypedValue::{ArrayValue, BinaryValue, Boolean, DateValue, ErrorValue, Number, StringValue, Structured, TableValue, TupleValue};

        #[test]
        fn test_array() {
            verify_exact_value(r#"
                Array::new()
            "#, ArrayValue(Array::new()));
        }

        #[test]
        fn test_binary() {
            verify_exact_value(r#"
                Binary::new()
            "#, BinaryValue(vec![]));
        }
        
        #[test]
        fn test_boolean() {
            verify_exact_value(r#"
                Boolean::new()
            "#, Boolean(false));
        }

        #[test]
        fn test_date() {
            verify_exact_value_where(r#"
                Date::new()
            "#, |v| matches!(v, DateValue(..)));
        }

        #[test]
        fn test_enum() {
            verify_exact_value(r#"
                Enum::new(AMEX, NYSE, NASDAQ, OTCBB)
            "#, Number(I64Value(0)));
        }

        #[test]
        fn test_error() {
            verify_exact_value(r#"
                Error::new()
            "#, ErrorValue(Errors::Empty));
        }

        #[test]
        fn test_f64() {
            verify_exact_value(r#"
                f64::new()
            "#, Number(F64Value(0.)));
        }

        #[test]
        fn test_i64() {
            verify_exact_value(r#"
                i64::new()
            "#, Number(I64Value(0)));
        }

        #[test]
        fn test_string() {
            verify_exact_value(r#"
                String::new(80)
            "#, StringValue(String::new()));
        }

        #[test]
        fn test_struct() {
           let model = Compiler::build(r#"
                Struct::new(
                    symbol: String(8) = "ABC",
                    exchange: String(8) = "AMEX",
                    last_sale: f64 = 6.7123
                )
            "#);

            verify_exact_value(r#"
                Struct::new(
                    symbol: String(8) = "ABC", 
                    exchange: String(8) = "AMEX", 
                    last_sale: f64 = 6.7123
                )
            "#, Structured(Hard(HardStructure::new(
                vec![
                    Parameter::new_with_default("symbol", FixedSizeType(StringType.into(), 8), StringValue("ABC".into())),
                    Parameter::new_with_default("exchange", FixedSizeType(StringType.into(), 8), StringValue("AMEX".into())),
                    Parameter::new_with_default("last_sale", NumberType(F64Kind), Number(F64Value(6.7123))),
                ],
                vec![
                    StringValue("ABC".into()),
                    StringValue("AMEX".into()),
                    Number(F64Value(6.7123))
                ]))));
        }

        #[test]
        fn test_table() {
            verify_exact_value(r#"
                Table::new(symbol: String(8), exchange: String(8), last_sale: f64)
            "#, TableValue(Model(ModelRowCollection::new(make_quote_columns()))));
        }

        #[test]
        fn test_tuple() {
            verify_exact_value(r#"
                Tuple::new(String(8), String(8), f64)
            "#, TupleValue(vec![
                StringValue("".into()),
                StringValue("".into()),
                Number(F64Value(0.0))
            ]));
        }

        #[test]
        fn test_uuid() {
            verify_exact_value_where(r#"
                UUID::new()
            "#, |v| matches!(v, TypedValue::UUIDValue(..)));
        }
    }
    
}