#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// DataType class
////////////////////////////////////////////////////////////////////

use crate::byte_code_compiler::ByteCodeCompiler;
use crate::compiler::Compiler;
use crate::data_types::DataType::*;
use crate::dataframe::Dataframe::Model;
use crate::errors::Errors::{Exact, InstantiationError, SyntaxError, TypeMismatch};
use crate::errors::TypeMismatchErrors::{ArgumentsMismatched, UnrecognizedTypeName, UnsupportedType};
use crate::errors::{throw, Errors, SyntaxErrors};
use crate::expression::Expression::*;
use crate::expression::{Expression, UNDEFINED};
use crate::field::FieldMetadata;
use crate::model_row_collection::ModelRowCollection;
use crate::number_kind::NumberKind;
use crate::number_kind::NumberKind::*;
use crate::numbers::Numbers::I32Value;
use crate::parameter::Parameter;
use crate::platform::PlatformOps;
use crate::row_collection::RowCollection;
use crate::sequences::Array;
use crate::structures::Structures::Hard;
use crate::structures::{HardStructure, Structure};
use crate::typed_values::TypedValue;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display};
use std::ops::Deref;

const PTR_LEN: usize = 8;

/// Represents an Oxide-native datatype
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum DataType {
    ArrayType(usize),
    ASCIIType(usize),
    BinaryType(usize),
    BooleanType,
    DynamicType, // Polymorphic
    EnumType(Vec<Parameter>),
    ErrorType,
    FunctionType(Vec<Parameter>, Box<DataType>),
    NumberType(NumberKind),
    PlatformOpsType(PlatformOps),
    StringType(usize),
    StructureType(Vec<Parameter>),
    TableType(Vec<Parameter>, usize),
    TupleType(Vec<DataType>),
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
        fn larger(a: &usize, b: &usize) -> usize {
            (if a > b { a } else { b }).to_owned()
        }

        match types.len() {
            0 => DynamicType,
            1 => types[0].to_owned(),
            _ => types[1..].iter().fold(types[0].to_owned(), |agg, t|
                match (agg, t) {
                    (BinaryType(a), BinaryType(b)) => StringType(larger(&a, b)),
                    (StringType(a), StringType(b)) => StringType(larger(&a, b)),
                    (_, t) => t.to_owned()
                })
        }
    }

    /// deciphers a datatype from an expression (e.g. "String" | "String(20)")
    pub fn decipher_type(model: &Expression) -> std::io::Result<DataType> {
        decode_model(model)
    }

    /// decodes the typed value based on the supplied data type and buffer
    pub fn decode(&self, buffer: &Vec<u8>, offset: usize) -> TypedValue {
        use crate::typed_values::TypedValue::*;
        match self {
            ArrayType(..) => ArrayValue(Array::new()),
            BinaryType(..) => Binary(Vec::new()),
            BooleanType => ByteCodeCompiler::decode_u8(buffer, offset, |b| Boolean(b == 1)),
            ErrorType => ErrorValue(Exact(ByteCodeCompiler::decode_string(buffer, offset, 255).to_string())),
            NumberType(kind) => Number(kind.decode(buffer, offset)),
            PlatformOpsType(pf) => PlatformOp(pf.to_owned()),
            StringType(size) => StringValue(ByteCodeCompiler::decode_string(buffer, offset, *size).to_string()),
            StructureType(params) => Structured(Hard(HardStructure::from_parameters(params.to_vec()))),
            TableType(columns, ..) => TableValue(Model(ModelRowCollection::from_parameters(columns))),
            _ => ByteCodeCompiler::decode_value(&buffer[offset..].to_vec())
        }
    }

    /// decodes the typed value based on the supplied data type and buffer
    pub fn decode_bcc(&self, bcc: &mut ByteCodeCompiler) -> std::io::Result<TypedValue> {
        use crate::typed_values::TypedValue::*;
        let tv = match self {
            ArrayType(..) => ArrayValue(Array::from(bcc.next_array()?)),
            ASCIIType(..) => ASCII(bcc.next_clob()),
            BinaryType(..) => Binary(bcc.next_blob()),
            BooleanType => Boolean(bcc.next_bool()),
            DataType::DynamicType => Undefined,
            EnumType(labels) => {
                let index = bcc.next_u32() as usize;
                StringValue(labels[index].get_name().to_string())
            }
            ErrorType => ErrorValue(Exact(bcc.next_string())),
            FunctionType(columns, returns) => Function {
                params: columns.to_owned(),
                body: Box::new(ByteCodeCompiler::disassemble(bcc)?),
                returns: returns.deref().clone(),
            },
            NumberType(kind) => Number(kind.decode_buffer(bcc)?),
            PlatformOpsType(pf) => PlatformOp(pf.to_owned()),
            StringType(..) => StringValue(bcc.next_string()),
            StructureType(params) => Structured(Hard(bcc.next_struct_with_parameters(params.to_vec())?)),
            TableType(columns, ..) => TableValue(Model(bcc.next_table_with_columns(columns)?)),
            TupleType(..) => TupleValue(bcc.next_array()?),
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
        match self {
            DataType::ArrayType(_) => match value {
                TypedValue::ArrayValue(_) => Ok(value.encode()),
                z => throw(TypeMismatch(UnsupportedType(self.clone(), z.get_type())))
            }
            DataType::BinaryType(_) => Ok(value.encode()),
            DataType::ASCIIType(_) => Ok(value.encode()),
            DataType::BooleanType => Ok(value.encode()),
            DataType::EnumType(_) => Ok(value.encode()),
            DataType::ErrorType => Ok(value.encode()),
            DataType::FunctionType(..) => Ok(value.encode()),
            DataType::NumberType(_) => Ok(value.encode()),
            DataType::PlatformOpsType(_) => Ok(value.encode()),
            DataType::StringType(_) => Ok(value.encode()),
            DataType::StructureType(_) => Ok(value.encode()),
            DataType::TableType(..) => {
                let df = value.to_dataframe()?;
                Ok(ByteCodeCompiler::encode_df(&df))
            }
            _ => Ok(value.encode()),
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
        Self::decipher_type(&model)
    }

    pub fn get_type_names() -> Vec<String> {
        vec![
            "Array", "ASCII", "Binary", "Boolean", "Date", "Enum", "Error", "Fn",
            "String", "Struct", "Table", //, "RowId",
            "f32", "f64", "i8", "i16", "i32", "i64", "i128",
            "u8", "u16", "u32", "u64", "u128", "UUID",
        ].iter().map(|s| s.to_string()).collect()
    }

    ////////////////////////////////////////////////////////////////////
    //  INSTANCE METHODS
    ////////////////////////////////////////////////////////////////////

    /// computes and returns the maximum physical size of the value of this datatype
    pub fn compute_fixed_size(&self) -> usize {
        use crate::data_types::DataType::*;
        let width: usize = match self {
            ArrayType(size) => *size,
            ASCIIType(size) => match size {
                size => *size + size.to_be_bytes().len(),
                0 => PTR_LEN
            },
            BinaryType(size) => *size,
            BooleanType => 1,
            EnumType(..) => 2,
            ErrorType => 256,
            FunctionType(columns, ..) => columns.len() * 8,
            DynamicType => 8,
            NumberType(nk) => nk.compute_fixed_size(),
            PlatformOpsType(..) => 4,
            StringType(size) => match size {
                size => *size + size.to_be_bytes().len(),
                0 => PTR_LEN
            },
            StructureType(columns) => columns.len() * 8,
            TableType(columns, ..) => columns.len() * 8,
            TupleType(types) => types.iter().map(|t| t.compute_fixed_size()).sum(),
        };
        width + 1 // +1 for field metadata
    }

    pub fn get_default_value(&self) -> TypedValue {
        use crate::typed_values::TypedValue::*;
        match self {
            ArrayType(..) => ArrayValue(Array::new()),
            ASCIIType(..) => ASCII(vec![]),
            BinaryType(..) => Binary(vec![]),
            BooleanType => Boolean(false),
            DataType::DynamicType => Null,
            EnumType(..) => Number(I32Value(0)),
            ErrorType => ErrorValue(Errors::Empty),
            FunctionType(params, returns) => Function {
                params: params.to_vec(),
                body: Box::new(Literal(returns.get_default_value())),
                returns: returns.deref().clone(),
            },
            NumberType(kind) => Number(kind.get_default_value()),
            PlatformOpsType(kind) => PlatformOp(kind.clone()),
            StringType(..) => StringValue(String::new()),
            StructureType(params) =>
                Structured(Hard(HardStructure::from_parameters(params.to_vec()))),
            TableType(params, _) =>
                TableValue(Model(ModelRowCollection::from_parameters(params))),
            TupleType(dts) => TupleValue(dts.iter()
                .map(|dt| dt.get_default_value()).collect()),
        }
    }

    pub fn instantiate(&self) -> std::io::Result<TypedValue> {
        use crate::typed_values::TypedValue::*;
        match self {
            ArrayType(_) => Ok(ArrayValue(Array::new())),
            ASCIIType(_) => Ok(ASCII(vec![])),
            BinaryType(_) => Ok(Binary(vec![])),
            BooleanType => Ok(Boolean(false)),
            DynamicType => throw(InstantiationError(self.clone())),
            EnumType(_) => throw(InstantiationError(self.clone())),
            ErrorType => throw(InstantiationError(self.clone())),
            FunctionType(params, kind) =>
                Ok(Function {
                    params: params.clone(),
                    body: Box::new(UNDEFINED),
                    returns: kind.deref().clone(),
                }),
            NumberType(kind) => Ok(Number(kind.get_default_value())),
            PlatformOpsType(_) => throw(InstantiationError(self.clone())),
            StringType(..) => Ok(StringValue(String::new())),
            StructureType(params) =>
                Ok(Structured(Hard(HardStructure::from_parameters(params.to_vec())))),
            TableType(params, _len) =>
                Ok(TableValue(Model(ModelRowCollection::from_parameters(params)))),
            TupleType(types) => {
                let mut values = vec![];
                for a_type in types { values.push(a_type.get_default_value()) }
                Ok(TupleValue(values))
            }
        }
    }

    pub fn is_compatible(&self, other: &DataType) -> bool {
        match (self, other) {
            (ArrayType(..), ArrayType(..)) |
            (ASCIIType(..) | StringType(..), ASCIIType(..) | StringType(..)) |
            (BinaryType(..), BinaryType(..)) |
            (DynamicType, _) | (_, DynamicType) => true,
            (EnumType(a), EnumType(b)) => Parameter::are_compatible(a, b),
            (FunctionType(a, dta), FunctionType(b, dtb)) =>
                Parameter::are_compatible(a, b) && dta.is_compatible(dtb),
            (NumberType(..), NumberType(..)) => true,
            // TODO add a case for when the source struct|table has a subset of the target fields?
            (StructureType(a) | TableType(a, _), StructureType(b) | TableType(b, _)) =>
                Parameter::are_compatible(a, b),
            (TupleType(a), TupleType(b)) => DataType::are_compatible(a, b),
            (a, b) => a == b,
        }
    }

    /// Indicates whether the given name represents a known type
    pub fn is_type_name(name: &str) -> bool {
        decode_model_variable(name).is_ok()
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
            ArrayType(size) => sized("Array", *size),
            ASCIIType(size) => sized("ASCII", *size),
            BinaryType(size) => sized("Binary", *size), // UTF8
            BooleanType => "Boolean".into(),
            DynamicType => String::new(),
            EnumType(labels) => parameterized("Enum", labels, true),
            ErrorType => "Error".into(),
            FunctionType(params, returns) =>
                format!("fn({}){}", Parameter::render(params),
                        match returns.to_code() {
                            s if !s.is_empty() => format!(": {}", s),
                            _ => String::new()
                        }),
            NumberType(nk) => nk.get_type_name(),
            PlatformOpsType(pf) => pf.to_code(),
            StringType(size) => sized("String", *size),
            StructureType(params) => parameterized("Struct", params, false),
            TableType(params, ..) => parameterized("Table", params, false),
            TupleType(types) => typed("", types),
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

fn decode_model(model: &Expression) -> std::io::Result<DataType> {
    use crate::typed_values::TypedValue::Structured;
    match model {
        // e.g. [String, String, f64]
        ArrayExpression(params) => decode_model_array(params),
        // e.g. fn(a, b, c)
        FnExpression { params, returns, .. } =>
            Ok(FunctionType(params.clone(), returns.clone().into())),
        // e.g. String(80)
        FunctionCall { fx, args } => decode_model_function_call(fx, args),
        // e.g. Structure(symbol: String, exchange: String, last_sale: f64)
        Literal(Structured(s)) => Ok(StructureType(s.get_parameters())),
        // e.g. (f64, f64, f64)
        TupleExpression(params) => decode_model_tuple(params),
        // e.g. i64
        Variable(name) => decode_model_variable(name),
        other => throw(SyntaxError(SyntaxErrors::TypeIdentifierExpected(other.to_code())))
    }
}

fn decode_model_array(items: &Vec<Expression>) -> std::io::Result<DataType> {
    let mut kinds = vec![];
    for item in items {
        match decode_model(item) {
            Ok(kind) => kinds.push(kind),
            Err(err) => return throw(Exact(err.to_string()))
        }
    }
    Ok(ArrayType(kinds.len()))
}

fn decode_model_function_call(
    fx: &Expression,
    args: &Vec<Expression>,
) -> std::io::Result<DataType> {
    use crate::typed_values::TypedValue::Number;

    fn expect_size(args: &Vec<Expression>, f: fn(usize) -> DataType) -> std::io::Result<DataType> {
        match args.as_slice() {
            [] => Ok(f(0)),
            [Literal(Number(n))] => Ok(f(n.to_usize())),
            [other] => throw(SyntaxError(SyntaxErrors::TypeIdentifierExpected(other.to_code()))),
            other => throw(TypeMismatch(ArgumentsMismatched(1, other.len())))
        }
    }
    fn expect_type(args: &Vec<Expression>, f: fn(DataType) -> DataType) -> std::io::Result<DataType> {
        match args.as_slice() {
            [item] => Ok(f(decode_model(item)?)),
            other => throw(TypeMismatch(ArgumentsMismatched(1, other.len())))
        }
    }
    fn expect_params(args: &Vec<Expression>, f: fn(Vec<Parameter>) -> DataType) -> std::io::Result<DataType> {
        let mut params: Vec<Parameter> = vec![];
        for arg in args {
            let param = match arg {
                AsValue(name, model) => Parameter::new(name, decode_model(model)?),
                SetVariables(var_expr, expr) =>
                    match var_expr.deref() {
                        Variable(name) => Parameter::from_tuple(name, expr.to_pure()?),
                        z => return throw(Exact(format!("Decomposition is not allowed near {}", z.to_code())))
                    }
                Variable(name) => Parameter::add(name),
                other => return throw(SyntaxError(SyntaxErrors::ParameterExpected(other.to_code())))
            };
            params.push(param);
        }
        Ok(f(params))
    }

    // decode the type
    match fx {
        Variable(name) =>
            match name.as_str() {
                "Array" => expect_size(args, |size| ArrayType(size)),
                "ASCII" => expect_size(args, |size| ASCIIType(size)),
                "Binary" => expect_size(args, |size| BinaryType(size)),
                "Enum" => expect_params(args, |params| EnumType(params)),
                "fn" => expect_params(args, |params| FunctionType(params, DynamicType.into())),
                "String" => expect_size(args, |size| StringType(size)),
                "Struct" => expect_params(args, |params| StructureType(params)),
                "Table" => expect_params(args, |params| TableType(params, 0)),
                type_name if args.is_empty() => decode_model_variable(type_name),
                type_name => throw(SyntaxError(SyntaxErrors::TypeIdentifierExpected(type_name.into())))
            }
        other => throw(SyntaxError(SyntaxErrors::TypeIdentifierExpected(other.to_code())))
    }
}

fn decode_model_tuple(items: &Vec<Expression>) -> std::io::Result<DataType> {
    let mut kinds = vec![];
    for item in items {
        match decode_model(item) {
            Ok(kind) => kinds.push(kind),
            Err(err) => return throw(Exact(err.to_string()))
        }
    }
    Ok(TupleType(kinds))
}

fn decode_model_variable(name: &str) -> std::io::Result<DataType> {
    match name {
        "Boolean" => Ok(BooleanType),
        "Date" => Ok(NumberType(DateKind)),
        "Enum" => Ok(EnumType(vec![])),
        "Error" => Ok(ErrorType),
        "f32" => Ok(NumberType(F32Kind)),
        "f64" => Ok(NumberType(F64Kind)),
        "Fn" => Ok(FunctionType(vec![], Box::new(DynamicType))),
        "i8" => Ok(NumberType(I8Kind)),
        "i16" => Ok(NumberType(I16Kind)),
        "i32" => Ok(NumberType(I32Kind)),
        "i64" => Ok(NumberType(I64Kind)),
        "i128" => Ok(NumberType(I128Kind)),
        "RowId" => Ok(NumberType(RowIdKind)),
        "String" => Ok(StringType(0)),
        "Struct" => Ok(StructureType(vec![])),
        "Table" => Ok(TableType(vec![], 0)),
        "UUID" => Ok(NumberType(UUIDKind)),
        "u8" => Ok(NumberType(U8Kind)),
        "u16" => Ok(NumberType(U16Kind)),
        "u32" => Ok(NumberType(U32Kind)),
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
            verify_compatibility(ArrayType(800), ArrayType(0))
        }

        #[test]
        fn test_ascii_and_strings() {
            verify_compatibility(ASCIIType(0), ASCIIType(8000));
            verify_compatibility(ASCIIType(0), StringType(8000));
            verify_compatibility(StringType(0), ASCIIType(8000));
            verify_compatibility(StringType(80), StringType(0));
        }

        #[test]
        fn test_best_fit() {
            let kind = DataType::best_fit(vec![
                StringType(11),
                StringType(110),
                StringType(55)
            ]);
            assert_eq!(kind, StringType(110))
        }

        #[test]
        fn test_numbers() {
            verify_compatibility(NumberType(I64Kind), NumberType(I32Kind));
            verify_incompatibility(NumberType(I64Kind), BooleanType);
        }


        fn verify_compatibility(a: DataType, b: DataType) {
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
            verify_type_construction("Array(12)", ArrayType(12));
        }

        #[test]
        fn test_binary() {
            verify_type_construction("Binary(5566)", BinaryType(5566));
        }

        #[test]
        fn test_ascii() {
            verify_type_construction("ASCII(1000)", ASCIIType(1000));
        }

        #[test]
        fn test_boolean() {
            verify_type_construction("Boolean", BooleanType);
        }

        #[test]
        fn test_date() {
            verify_type_construction("Date", NumberType(DateKind));
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
                "Enum(AMEX := 1, NASDAQ := 2, NYSE := 3, OTCBB := 4)",
                EnumType(vec![
                    Parameter::with_default("AMEX", NumberType(I64Kind), Number(I64Value(1))),
                    Parameter::with_default("NASDAQ", NumberType(I64Kind), Number(I64Value(2))),
                    Parameter::with_default("NYSE", NumberType(I64Kind), Number(I64Value(3))),
                    Parameter::with_default("OTCBB", NumberType(I64Kind), Number(I64Value(4))),
                ]));
        }

        #[test]
        fn test_f32() {
            verify_type_construction("f32", NumberType(F32Kind));
        }

        #[test]
        fn test_f64() {
            verify_type_construction("f64", NumberType(F64Kind));
        }

        #[test]
        fn test_fn() {
            verify_type_construction(
                "fn(symbol: String(8), exchange: String(8), last_sale: f64)",
                FunctionType(make_quote_parameters(), Box::from(DynamicType)));
        }

        #[test]
        fn test_i8() {
            verify_type_construction("i8", NumberType(I8Kind));
        }

        #[test]
        fn test_i16() {
            verify_type_construction("i16", NumberType(I16Kind));
        }

        #[test]
        fn test_i32() {
            verify_type_construction("i32", NumberType(I32Kind));
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
            verify_type_construction("String", StringType(0));
            verify_type_construction("String(10)", StringType(10));
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
                TableType(make_quote_parameters(), 0));
        }

        #[test]
        fn test_u8() {
            verify_type_construction("u8", NumberType(U8Kind));
        }

        #[test]
        fn test_u16() {
            verify_type_construction("u16", NumberType(U16Kind));
        }

        #[test]
        fn test_u32() {
            verify_type_construction("u32", NumberType(U32Kind));
        }

        #[test]
        fn test_u64() {
            verify_type_construction("u64", NumberType(U64Kind));
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
        fn test_get_default_value_ascii() {
            assert!(matches!(
                ASCIIType(128).get_default_value(),
                ASCII(..)
            ));
        }

        #[test]
        fn test_get_default_value_binary() {
            assert!(matches!(
                BinaryType(128).get_default_value(),
                Binary(..)
            ));
        }

        #[test]
        fn test_get_default_value_date() {
            assert!(matches!(
                NumberType(DateKind).get_default_value(),
                Number(DateValue(..))
            ));
        }

        #[test]
        fn test_get_default_value_enum() {
            assert!(matches!(
                EnumType(make_quote_parameters()).get_default_value(),
                Number(I32Value(0))
            ));
        }

        #[test]
        fn test_get_default_value_i8() {
            assert_eq!(NumberType(I8Kind).get_default_value(), Number(I8Value(0)));
        }

        #[test]
        fn test_get_default_value_i16() {
            assert_eq!(NumberType(I16Kind).get_default_value(), Number(I16Value(0)));
        }

        #[test]
        fn test_get_default_value_i32() {
            assert_eq!(NumberType(I32Kind).get_default_value(), Number(I32Value(0)));
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
        fn test_get_default_value_u8() {
            assert_eq!(NumberType(U8Kind).get_default_value(), Number(U8Value(0)));
        }

        #[test]
        fn test_get_default_value_u16() {
            assert_eq!(NumberType(U16Kind).get_default_value(), Number(U16Value(0)));
        }

        #[test]
        fn test_get_default_value_u32() {
            assert_eq!(NumberType(U32Kind).get_default_value(), Number(U32Value(0)));
        }

        #[test]
        fn test_get_default_value_u64() {
            assert_eq!(NumberType(U64Kind).get_default_value(), Number(U64Value(0)));
        }

        #[test]
        fn test_get_default_value_u128() {
            assert_eq!(NumberType(U128Kind).get_default_value(), Number(U128Value(0)));
        }

        #[test]
        fn test_get_default_value_uuid() {
            assert!(matches!(
                NumberType(UUIDKind).get_default_value(),
                Number(UUIDValue(..))
            ));
        }

        #[test]
        fn test_get_default_value_string() {
            assert_eq!(StringType(0).get_default_value(), StringValue(String::new()));
        }

        #[test]
        fn test_get_default_value_table() {
            assert_eq!(
                TableType(make_quote_parameters(), 0).get_default_value(),
                TableValue(Model(ModelRowCollection::new(make_quote_columns()))));
        }
    }

    /// Instantiation tests
    #[cfg(test)]
    mod instantiation_tests {
        use crate::dataframe::Dataframe::Model;
        use crate::model_row_collection::ModelRowCollection;
        use crate::numbers::Numbers::*;
        use crate::testdata::{make_quote_columns, verify_exact_value, verify_exact_value_where};
        use crate::typed_values::TypedValue::{Number, StringValue, TableValue};

        #[test]
        fn test_date() {
            verify_exact_value_where(r#"
                new Date()
            "#, |v| matches!(v, Number(DateValue(..))));
        }

        #[test]
        fn test_f64() {
            verify_exact_value(r#"
                new f64()
            "#, Number(F64Value(0.)));
        }

        #[test]
        fn test_i64() {
            verify_exact_value(r#"
                new i64()
            "#, Number(I64Value(0)));
        }

        #[test]
        fn test_string() {
            verify_exact_value(r#"
                new String(80)
            "#, StringValue(String::new()));
        }

        #[test]
        fn test_table() {
            verify_exact_value(r#"
                new Table(symbol: String(8), exchange: String(8), last_sale: f64)
            "#, TableValue(Model(ModelRowCollection::new(make_quote_columns()))));
        }

        #[test]
        fn test_uuid() {
            verify_exact_value_where(r#"
                new UUID()
            "#, |v| matches!(v, Number(UUIDValue(..))));
        }
    }
}