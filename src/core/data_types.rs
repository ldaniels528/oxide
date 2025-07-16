#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// DataType class
////////////////////////////////////////////////////////////////////

use crate::blobs::{BLOBStore, BLOB};
use crate::byte_code_compiler::ByteCodeCompiler;
use crate::compiler;
use crate::compiler::Compiler;
use crate::data_types::DataType::*;
use crate::dataframe::Dataframe;
use crate::dataframe::Dataframe::ModelTable;
use crate::errors::Errors::{Exact, SyntaxError, TypeMismatch};
use crate::errors::TypeMismatchErrors::{ArgumentsMismatched, UnrecognizedTypeName};
use crate::errors::{throw, Errors, SyntaxErrors};
use crate::expression::Expression;
use crate::expression::Expression::*;
use crate::field::FieldMetadata;
use crate::model_row_collection::ModelRowCollection;
use crate::namespaces::Namespace;
use crate::number_kind::NumberKind;
use crate::number_kind::NumberKind::*;
use crate::numbers::Numbers::I64Value;
use crate::packages::PackageOps;
use crate::parameter::Parameter;
use crate::row_collection::RowCollection;
use crate::sequences::Array;
use crate::structures::Structures::Hard;
use crate::structures::{HardStructure, Structure};
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{ArrayValue, ByteStringValue, CharValue, DateTimeValue, ErrorValue, Number, Structured, TableValue, TupleValue, UUIDValue};
use crate::utils::{pull_number, pull_string};
use chrono::Local;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display};
use std::ops::Deref;
use uuid::Uuid;

const PTR_LEN: usize = 8;

/// Represents an Oxide-native datatype
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum DataType {
    ArrayType(Box<DataType>),
    ByteStringType,
    BlobType(Box<DataType>),
    BooleanType,
    CharType,
    DateTimeType,
    EnumType(Vec<Parameter>),
    ErrorType,
    FixedSizeType(Box<DataType>, usize),
    FunctionType(Vec<Parameter>, Box<DataType>),
    NumberType(NumberKind),
    PlatformOpsType(PackageOps),
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

    pub fn are_compatible(
        types_a: &Vec<DataType>,
        types_b: &Vec<DataType>,
    ) -> bool {
        types_a.iter().zip(types_b.iter()).all(|(a, b)| a.is_compatible(b))
    }

    /// provides type resolution for the given [Vec<DataType>]
    pub fn best_fit(types: Vec<DataType>) -> DataType {
        match types.len() {
            0 => Self::RuntimeResolvedType,
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
            ArrayType(..) => ArrayValue(Array::new()),
            Self::ByteStringType => ByteStringValue(Vec::new()),
            BlobType(..) => ByteCodeCompiler::decode_value(buffer),
            Self::BooleanType => ByteCodeCompiler::decode_u8(buffer, offset, |b| Boolean(b == 1)),
            Self::CharType => {
                std::str::from_utf8(&buffer[offset..offset+4].to_vec()).ok()
                    .and_then(|s| {
                        let mut chars = s.chars();
                        chars.next()
                    })
                    .map(|c| CharValue(c))
                    .unwrap_or(Undefined)
            }
            Self::DateTimeType => ByteCodeCompiler::decode_u8x8(buffer, offset, |b| DateTimeValue(i64::from_be_bytes(b))),
            EnumType(..) => ByteCodeCompiler::decode_value(&buffer[offset..].to_vec()),
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
            PlatformOpsType(pf) => PlatformOp(pf.to_owned()),
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
        } else { TypedValue::Null }
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
            ArrayType(..) => PTR_LEN,
            Self::ByteStringType => PTR_LEN,
            BlobType(..) => PTR_LEN,
            Self::BooleanType => 1,
            Self::CharType => 4,
            Self::DateTimeType => 8,
            EnumType(..) => 2,
            Self::ErrorType => 256,
            FixedSizeType(data_type, size) =>
                match data_type.deref() {
                    ByteStringType | StringType =>
                        match size {
                            size => *size + size.to_be_bytes().len(),
                            0 => PTR_LEN
                        }
                    _ => data_type.compute_fixed_size() * size
                }
            FunctionType(columns, ..) => columns.len() * PTR_LEN,
            NumberType(nk) => nk.compute_fixed_size(),
            PlatformOpsType(..) => 4,
            Self::StringType => PTR_LEN,
            StructureType(columns) => columns.len() * PTR_LEN,
            TableType(columns) => columns.len() * PTR_LEN,
            TupleType(types) => types.iter().map(|t| t.compute_fixed_size()).sum(),
            Self::RuntimeResolvedType => PTR_LEN,
            Self::UUIDType => 16,
        };
        width + 1 // +1 for field metadata
    }

    fn construct<F, G>(
        args: Vec<TypedValue>,
        f0: F,
        f1: G,
    ) -> std::io::Result<TypedValue>
    where
        F: Fn() -> TypedValue,
        G: Fn(&TypedValue) -> TypedValue,
    {
        match args.as_slice() {
            [] => Ok(f0()),
            [item] => Ok(f1(item)),
            items => throw(TypeMismatch(ArgumentsMismatched(1, items.len()))),
        }
    }

    fn construct_ok<F, G>(
        args: Vec<TypedValue>,
        f0: F,
        f1: G,
    ) -> std::io::Result<TypedValue>
    where
        F: Fn() -> TypedValue,
        G: Fn(&TypedValue) -> std::io::Result<TypedValue>,
    {
        match args.as_slice() {
            [] => Ok(f0()),
            [item] => f1(item),
            items => throw(TypeMismatch(ArgumentsMismatched(1, items.len()))),
        }
    }

    pub fn instantiate(
        &self,
        args: Vec<TypedValue>,
    ) -> std::io::Result<TypedValue> {
        use crate::typed_values::TypedValue::*;
        match self {
            Self::ArrayType(datatype) => Ok(match args.as_slice() {  
                [] => ArrayValue(Array::new()),
                items => 
                    ArrayValue(Array::from({
                        let mut values = Vec::with_capacity(items.len());
                        for item in items { values.push( item.convert_to(datatype)?) }
                        values 
                    })),
            }),
            Self::ByteStringType => Self::construct(
                args,
                || ByteStringValue(vec![]),
                |bytes| ByteStringValue(bytes.to_bytes()),
            ),
            Self::BlobType(data_type) => match args.as_slice() {
                [path_v, offset_v] => {
                    let (path, offset) = (pull_string(path_v)?, pull_number(offset_v)?);
                    let ns = Namespace::parse(path.as_str())?;
                    let bs = BLOBStore::open(&ns)?;
                    let bmd = bs.read_metadata(offset.to_u64())?;
                    Ok(BLOBValue(BLOB::new(bs, bmd, data_type.deref().clone())))
                }
                items => throw(TypeMismatch(ArgumentsMismatched(2, items.len()))),
            }
            Self::BooleanType => Self::construct(
                args,
                || Boolean(false),
                |item| Boolean(item.to_bool()),
            ),
            Self::CharType => Self::construct(
                args,
                || CharValue('\0'),
                |item| item.to_char().map(CharValue).unwrap_or(CharValue('\0')),
            ),
            Self::DateTimeType => Self::construct(
                args,
                || DateTimeValue(Local::now().timestamp_millis()),
                |item| DateTimeValue(item.to_i64()),
            ),
            Self::EnumType(..) => Ok(Number(I64Value(0))),
            Self::ErrorType => Self::construct(
                args,
                || ErrorValue(Errors::Empty),
                |item| ErrorValue(Exact(item.to_string())),
            ),
            Self::FixedSizeType(data_type, ..) => data_type.instantiate(args),
            Self::FunctionType(params, returns) => Ok(Function {
                params: params.to_vec(),
                body: Literal(returns.instantiate(vec![])?).into(),
                returns: returns.deref().clone(),
            }),
            Self::NumberType(kind) =>
                Self::construct_ok(
                    args,
                    || Number(kind.get_default_value()),
                    |item| item.convert_to_number(kind)),
            Self::PlatformOpsType(kind) => Ok(PlatformOp(kind.clone())),
            Self::StringType => Self::construct(
                args,
                || StringValue(String::new()),
                |item| StringValue(item.unwrap_value()),
            ),
            Self::StructureType(params) => Self::instantiate_struct(params, args),
            Self::TableType(params) => Ok(TableValue(ModelTable(ModelRowCollection::from_parameters(params)))),
            Self::TupleType(types) => Self::instantiate_tuple(types, args),
            Self::RuntimeResolvedType => throw(Exact("Type cannot be instantiated".into())),
            Self::UUIDType => Self::construct(
                args,
                || UUIDValue(Uuid::new_v4().as_u128()),
                |item| UUIDValue(item.to_u128()),
            ),
        }
    }

    fn instantiate_struct(
        params: &Vec<Parameter>,
        args: Vec<TypedValue>,
    ) -> std::io::Result<TypedValue> {
        if args.len() > params.len() {
            return throw(TypeMismatch(ArgumentsMismatched(params.len(), args.len())))
        }
        let mut values = vec![];
        for (n, param) in params.iter().enumerate() {
            let arg = if n < args.len() { args[n].clone() } else { param.get_default_value() };
            values.push(arg);
        }
        Ok(Structured(Hard(HardStructure::from_parameters_and_values(params.to_vec(), values))))
    }

    fn instantiate_tuple(
        data_types: &Vec<DataType>,
        args: Vec<TypedValue>,
    ) -> std::io::Result<TypedValue> {
        if args.len() > data_types.len() {
            return throw(TypeMismatch(ArgumentsMismatched(data_types.len(), args.len())))
        }
        let mut values = vec![];
        for (n, data_type) in data_types.iter().enumerate() {
            let arg = if n < args.len() { args[n].clone() } else { data_type.instantiate(vec![])? };
            values.push(data_type.instantiate(vec![arg])?);
        }
        Ok(TupleValue(values))
    }

    pub fn is_compatible(&self, other: &DataType) -> bool {
        match (self, other) {
            (ArrayType(..), ArrayType(..)) |
            (ByteStringType, ByteStringType) |
            (StringType, StringType) |
            (RuntimeResolvedType, _) | (_, RuntimeResolvedType) => true,
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
        decipher_model_identifier(name).is_ok()
    }

    pub fn get_name(&self) -> String {
        match self {
            Self::ArrayType(..) => "Array".to_string(),
            Self::ByteStringType => "ByteString".to_string(),
            Self::BlobType(..) => "BLOB".to_string(),
            Self::BooleanType => "Boolean".to_string(),
            Self::CharType => "Char".to_string(),
            Self::DateTimeType => "DateTime".to_string(),
            Self::EnumType(..) => "Enum".to_string(),
            Self::ErrorType => "Error".to_string(),
            Self::FixedSizeType(dt, ..) => dt.get_name(),
            Self::FunctionType(..) => "Function".to_string(),
            Self::NumberType(..) => "Number".to_string(),
            Self::PlatformOpsType(..) => "PlatformFunction".to_string(),
            Self::RuntimeResolvedType => "Runtime".to_string(),
            Self::StringType => "String".to_string(),
            Self::StructureType(..) => "Struct".to_string(),
            Self::TableType(..) => "Table".to_string(),
            Self::TupleType(..) => "Tuple".to_string(),
            Self::UUIDType => "UUID".to_string(),
        }
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
                n if name.ends_with(")") => format!("{name}:::({n})"),
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
            Self::ByteStringType => "ByteString".into(), // UTF8
            BlobType(data_type) => format!("Blob({})", data_type.to_code()),
            Self::BooleanType => "Boolean".into(),
            Self::CharType => "Char".into(),
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
            PlatformOpsType(pf) => pf.to_code(),
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

fn decipher_model(model: &Expression) -> std::io::Result<DataType> {
    use crate::typed_values::TypedValue::Structured;
    match model {
        // e.g. Array(DateTime):::(12)
        ColonColonColon(model, size) => {
            match size.deref() {
                Literal(TypedValue::Number(n)) =>
                    Ok(FixedSizeType(decipher_model(model)?.into(), n.to_usize())),
                _ => throw(Exact(format!("Invalid type declaration: try {}:::({})", model, 20)))
            }
        }
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
        Identifier(name) => decipher_model_identifier(name),
        other => throw(SyntaxError(SyntaxErrors::TypeIdentifierExpected(other.to_code())))
    }
}

fn decipher_model_array(items: &Vec<Expression>) -> std::io::Result<DataType> {
    let mut kinds = vec![];
    for item in items {
        kinds.push(decipher_model(item)?); 
    }
    match kinds.as_slice() { 
        [kind] => Ok(ArrayType(kind.clone().into())),
        other => throw(TypeMismatch(ArgumentsMismatched(1, other.len())))
    }
}

fn decipher_model_function_call(
    fx: &Expression,
    args: &Vec<Expression>,
) -> std::io::Result<DataType> {
    use crate::typed_values::TypedValue::Number;
    fn expect_params(args: &Vec<Expression>, f: fn(Vec<Parameter>) -> DataType) -> std::io::Result<DataType> {
        let params = compiler::convert_to_parameters(args.to_vec())?;
        Ok(f(params))
    }
    fn expect_size(args: &Vec<Expression>, data_type: DataType) -> std::io::Result<DataType> {
        match args.as_slice() {
            [] => Ok(data_type),
            [Literal(Number(n))] => Ok(FixedSizeType(data_type.into(), n.to_usize())),
            [other] => throw(SyntaxError(SyntaxErrors::TypeIdentifierExpected(other.to_code()))),
            other => throw(TypeMismatch(ArgumentsMismatched(1, other.len())))
        }
    }
    fn expect_type(args: &Vec<Expression>, f: fn(DataType) -> DataType) -> std::io::Result<DataType> {
        let data_type = match args.as_slice() {
            [] => RuntimeResolvedType,
            [arg] => decipher_model(arg)?,
            other => return throw(TypeMismatch(ArgumentsMismatched(1, other.len())))
        };
        Ok(f(data_type))
    }
    fn expect_types(args: &Vec<Expression>, f: fn(Vec<DataType>) -> DataType) -> std::io::Result<DataType> {
        let mut data_types = vec![];
        for arg in args {
            data_types.push(decipher_model(arg)?);
        }
        Ok(f(data_types))
    }

    // decode the type
    match fx {
        Identifier(name) =>
            match name.as_str() {
                "Array" => expect_type(args, |data_type| ArrayType(data_type.into())),
                "BLOB" => expect_type(args, |data_type| BlobType(data_type.into())),
                "ByteString" => expect_size(args, ByteStringType),
                "Enum" => expect_params(args, |params| EnumType(params)),
                "fn" => expect_params(args, |params| FunctionType(params, RuntimeResolvedType.into())),
                "String" => expect_size(args, StringType),
                "Struct" => expect_params(args, |params| StructureType(params)),
                "Table" => expect_params(args, |params| TableType(params)),
                "Tuple" => expect_types(args, |types| TupleType(types)),
                type_name if args.is_empty() => decipher_model_identifier(type_name),
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

fn decipher_model_identifier(name: &str) -> std::io::Result<DataType> {
    match name {
        "Array" => Ok(ArrayType(RuntimeResolvedType.into())),
        "BLOB" => Ok(BlobType(RuntimeResolvedType.into())),
        "ByteString" => Ok(ByteStringType),
        "Boolean" => Ok(BooleanType),
        "Char" => Ok(CharType),
        "DateTime" => Ok(DateTimeType),
        "Enum" => Ok(EnumType(vec![])),
        "Error" => Ok(ErrorType),
        "f64" => Ok(NumberType(F64Kind)),
        "Fn" => Ok(FunctionType(vec![], RuntimeResolvedType.into())),
        "i64" => Ok(NumberType(I64Kind)),
        "i128" => Ok(NumberType(I128Kind)),
        "Number" => Ok(NumberType(NaNKind)),
        "String" => Ok(StringType),
        "Struct" => Ok(StructureType(vec![])),
        "Table" => Ok(TableType(vec![])),
        "Tuple" => Ok(TupleType(vec![])),
        "UUID" => Ok(UUIDType),
        "u8" => Ok(NumberType(U8Kind)),
        "u64" => Ok(NumberType(U64Kind)),
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
                FixedSizeType(ArrayType(DateTimeType.into()).into(), 800),
                ArrayType(DateTimeType.into())
            )
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
            assert!(a.is_compatible(&b), "{} is not compatible with {}", a, b);
        }

        fn verify_incompatibility(a: DataType, b: DataType) {
            assert!(!a.is_compatible(&b), "{} is compatible with {}", a, b);
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
                "Array(DateTime):::(12)",
                FixedSizeType(ArrayType(DateTimeType.into()).into(), 12)
            );
        }

        #[test]
        fn test_binary() {
            verify_type_construction("ByteString(5566)", FixedSizeType(ByteStringType.into(), 5566));
        }

        #[test]
        fn test_boolean() {
            verify_type_construction("Boolean", BooleanType);
        }

        #[test]
        fn test_date() {
            verify_type_construction("DateTime", DateTimeType);
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
    }

    /// Instantiation tests
    #[cfg(test)]
    mod instantiation_tests {
        use crate::dataframe::Dataframe::ModelTable;
        use crate::errors::Errors;
        use crate::errors::Errors::Exact;
        use crate::model_row_collection::ModelRowCollection;
        use crate::numbers::Numbers::*;
        use crate::sequences::Array;
        use crate::testdata::{make_quote_columns, verify_exact_code, verify_exact_unwrapped, verify_exact_value, verify_exact_value_where};
        use crate::typed_values::TypedValue;
        use crate::typed_values::TypedValue::{ArrayValue, Boolean, ByteStringValue, CharValue, DateTimeValue, ErrorValue, Number, StringValue, TableValue, TupleValue};

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
        fn test_byte_string() {
            verify_exact_value(r#"
                ByteString::new()
            "#, ByteStringValue(vec![]));
        }

        #[test]
        fn test_byte_string_from_bytes() {
            verify_exact_value(r#"
                ByteString::new(0B68656c6c6f)
            "#, ByteStringValue(vec![
                0x68, 0x65, 0x6c, 0x6c, 0x6f
            ]));
        }

        #[test]
        fn test_byte_string_from_chars() {
            verify_exact_value(r#"
                ByteString::new("hello")
            "#, ByteStringValue(vec![
                0x68, 0x65, 0x6c, 0x6c, 0x6f
            ]));
        }
        
        #[test]
        fn test_boolean() {
            verify_exact_value(r#"
                Boolean::new()
            "#, Boolean(false));
        }

        #[test]
        fn test_boolean_from_literal() {
            verify_exact_value(r#"
                Boolean::new(true)
            "#, Boolean(true));
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
        fn test_error_from_literal() {
            verify_exact_value(r#"
                Error::new("Boom!")
            "#, ErrorValue(Exact("Boom!".into())));
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
                let StockClass = Struct(symbol: String(8), exchange: String(8), last_sale: f64)
                StockClass::new("ABC", "AMEX", 34.56)
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
        fn test_tuple() {
            verify_exact_value(r#"
                let StockTuple = Tuple(String(8), String(8), f64)
                StockTuple::new("ABC", "NYSE", 17.11)
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
            "#, |v| matches!(v, TypedValue::UUIDValue(..)));
        }

        #[test]
        fn test_uuid_from_value() {
            verify_exact_value(r#"
                UUID::new(0Bf1f465f5e6fd4a5ab3fdd30640fe4647)
            "#, TypedValue::UUIDValue(0xf1f465f5e6fd4a5ab3fdd30640fe4647u128));
        }
    }

    /// "type_of" tests
    #[cfg(test)]
    mod type_of_tests {
        use crate::numbers::Numbers::*;
        use crate::testdata::verify_exact_code;

        #[test]
        fn test_type_of_user_defined_type() {
            verify_exact_code(r#"
                Cards = Table(face: String(2), suit: String(2))
                type_of Cards
            "#, "Table(face: String(2), suit: String(2))");
        }
    }
}