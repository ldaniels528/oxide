#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// DataType class
////////////////////////////////////////////////////////////////////

use crate::arrays::Array;
use crate::byte_code_compiler::ByteCodeCompiler;
use crate::compiler::Compiler;
use crate::data_types::DataType::*;
use crate::dataframe::Dataframe::Model;
use crate::errors::throw;
use crate::errors::Errors::{Exact, Syntax, TypeMismatch};
use crate::errors::TypeMismatchErrors::{ArgumentsMismatched, UnsupportedType};
use crate::expression::Expression;
use crate::expression::Expression::{AsValue, FunctionCall, Literal, SetVariable, Variable};
use crate::field::FieldMetadata;
use crate::model_row_collection::ModelRowCollection;
use crate::number_kind::NumberKind;
use crate::number_kind::NumberKind::*;
use crate::numbers::Numbers;
use crate::parameter::Parameter;
use crate::platform::PlatformOps;
use crate::row_collection::RowCollection;
use crate::structures::Structures::Hard;
use crate::structures::{HardStructure, Structure};
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{ArrayValue, Binary, Boolean, ErrorValue, Function, Null, Number, PlatformOp, StringValue, Structured, TableValue, Undefined, BLOB};
use serde::{Deserialize, Serialize};
use shared_lib::fail;
use std::fmt::{Debug, Display};
use std::ops::Deref;

const PTR_LEN: usize = 8;

/// Represents an Oxide-native datatype
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum DataType {
    ArrayType(usize),
    BinaryType(usize),
    BLOBType(Box<DataType>),
    BooleanType,
    EnumType(Vec<Parameter>),
    ErrorType,
    FunctionType(Vec<Parameter>),
    Indeterminate,
    NumberType(NumberKind),
    PlatformOpsType(PlatformOps),
    StringType(usize),
    StructureType(Vec<Parameter>),
    TableType(Vec<Parameter>, usize),
    VaryingType(Vec<DataType>),
}

impl DataType {
    ////////////////////////////////////////////////////////////////////
    //  STATIC METHODS
    ////////////////////////////////////////////////////////////////////

    /// parses a datatype expression (e.g. "String(20)")
    pub fn decipher_type(model: &Expression) -> std::io::Result<DataType> {
        fn expect_size(args: &Vec<Expression>, f: fn(usize) -> DataType) -> std::io::Result<DataType> {
            match args.as_slice() {
                [] => Ok(f(0)),
                [Literal(Number(n))] => Ok(f(n.to_usize())),
                [other] => throw(Syntax(other.to_code())),
                other => throw(TypeMismatch(ArgumentsMismatched(1, other.len())))
            }
        }
        fn expect_type(args: &Vec<Expression>, f: fn(DataType) -> DataType) -> std::io::Result<DataType> {
            match args.as_slice() {
                [item] => Ok(f(decipher(item)?)),
                other => throw(TypeMismatch(ArgumentsMismatched(1, other.len())))
            }
        }
        fn expect_params(args: &Vec<Expression>, f: fn(Vec<Parameter>) -> DataType) -> std::io::Result<DataType> {
            let mut params: Vec<Parameter> = vec![];
            for arg in args {
                let param = match arg {
                    AsValue(name, model) => Parameter::new(name, decipher(model)?),
                    SetVariable(name, expr) => Parameter::from_tuple(name, expr.to_pure()?),
                    Variable(name) => Parameter::build(name),
                    other => return throw(Syntax(other.to_code()))
                };
                params.push(param);
            }
            Ok(f(params))
        }
        fn decipher(model: &Expression) -> std::io::Result<DataType> {
            match model {
                Literal(Number(Numbers::Ack)) => Ok(NumberType(AckKind)),
                Literal(Function { params, .. }) =>
                    Ok(FunctionType(params.clone())),
                Literal(Structured(s)) => Ok(StructureType(s.get_parameters())),
                Variable(name) =>
                    match name.as_str() {
                        "Ack" => Ok(NumberType(AckKind)),
                        "Boolean" => Ok(BooleanType),
                        "Date" => Ok(NumberType(DateKind)),
                        "Enum" => Ok(EnumType(vec![])),
                        "Error" => Ok(ErrorType),
                        "f32" => Ok(NumberType(F32Kind)),
                        "f64" => Ok(NumberType(F64Kind)),
                        "Fn" => Ok(FunctionType(vec![])),
                        "i8" => Ok(NumberType(I8Kind)),
                        "i16" => Ok(NumberType(I16Kind)),
                        "i32" => Ok(NumberType(I32Kind)),
                        "i64" => Ok(NumberType(I64Kind)),
                        "i128" => Ok(NumberType(I128Kind)),
                        "RowId" => Ok(NumberType(RowIdKind)),
                        "RowsAffected" => Ok(NumberType(RowsAffectedKind)),
                        "Struct" => Ok(StructureType(vec![])),
                        "u8" => Ok(NumberType(U8Kind)),
                        "u16" => Ok(NumberType(U16Kind)),
                        "u32" => Ok(NumberType(U32Kind)),
                        "u64" => Ok(NumberType(U64Kind)),
                        "u128" => Ok(NumberType(U128Kind)),
                        type_name => fail(format!("unrecognized type {}", type_name))
                    }
                FunctionCall { fx, args } =>
                    match fx.deref() {
                        Variable(name) =>
                            match name.as_str() {
                                "Array" => expect_size(args, |size| ArrayType(size)),
                                "Binary" => expect_size(args, |size| BinaryType(size)),
                                "BLOB" => expect_type(args, |kind| BLOBType(Box::new(kind))),
                                "Enum" => expect_params(args, |params| EnumType(params)),
                                "fn" => expect_params(args, |params| FunctionType(params)),
                                "String" => expect_size(args, |size| StringType(size)),
                                "Struct" => expect_params(args, |params| StructureType(params)),
                                "Table" => expect_params(args, |params| TableType(params, 0)),
                                type_name => throw(Syntax(type_name.into()))
                            }
                        other => throw(Syntax(other.to_code()))
                    }
                other => throw(Syntax(other.to_code()))
            }
        }
        decipher(model)
    }

    /// decodes the typed value based on the supplied data type and buffer
    pub fn decode(&self, buffer: &Vec<u8>, offset: usize) -> TypedValue {
        match self {
            ArrayType(..) => ArrayValue(Array::new()),
            BinaryType(..) => Binary(Vec::new()),
            BooleanType => ByteCodeCompiler::decode_u8(buffer, offset, |b| Boolean(b == 1)),
            ErrorType => ErrorValue(Exact(ByteCodeCompiler::decode_string(buffer, offset, 255).to_string())),
            NumberType(kind) => Number(kind.decode(buffer, offset)),
            PlatformOpsType(pf) => PlatformOp(pf.to_owned()),
            StringType(size) => StringValue(ByteCodeCompiler::decode_string(buffer, offset, *size).to_string()),
            StructureType(params) => Structured(Hard(HardStructure::from_parameters(params))),
            TableType(columns, ..) => TableValue(Model(ModelRowCollection::from_parameters(columns))),
            _ => ByteCodeCompiler::decode_value(&buffer[offset..].to_vec())
        }
    }

    /// decodes the typed value based on the supplied data type and buffer
    pub fn decode_bcc(&self, bcc: &mut ByteCodeCompiler) -> std::io::Result<TypedValue> {
        let tv = match self {
            ArrayType(..) => ArrayValue(Array::from(bcc.next_array()?)),
            BinaryType(..) => Binary(bcc.next_blob()),
            BLOBType(dt) => BLOB(Box::from(Self::decode_bcc(dt, bcc)?)),
            BooleanType => Boolean(bcc.next_bool()),
            EnumType(labels) => {
                let index = bcc.next_u32() as usize;
                StringValue(labels[index].get_name().to_string())
            }
            ErrorType => ErrorValue(Exact(bcc.next_string())),
            FunctionType(columns) => Function {
                params: columns.to_owned(),
                code: Box::new(ByteCodeCompiler::disassemble(bcc)?),
            },
            NumberType(kind) => Number(kind.decode_buffer(bcc)?),
            PlatformOpsType(pf) => PlatformOp(pf.to_owned()),
            StringType(..) => StringValue(bcc.next_string()),
            StructureType(params) => Structured(Hard(bcc.next_struct_with_parameters(params)?)),
            TableType(columns, ..) => TableValue(Model(bcc.next_table_with_columns(columns)?)),
            _ => Undefined,
        };
        Ok(tv)
    }

    pub fn decode_field_value(&self, buffer: &Vec<u8>, offset: usize) -> TypedValue {
        let metadata = FieldMetadata::decode(buffer[offset]);
        if metadata.is_active {
            self.decode(buffer, offset + 1)
        } else { Null }
    }

    pub fn decode_field_value_bcc(&self, bcc: &mut ByteCodeCompiler, offset: usize) -> std::io::Result<TypedValue> {
        let metadata: FieldMetadata = FieldMetadata::decode(bcc[offset]);
        let value: TypedValue = if metadata.is_active {
            self.decode_bcc(bcc)?
        } else { Null };
        Ok(value)
    }

    pub fn encode(&self, value: &TypedValue) -> std::io::Result<Vec<u8>> {
        match self {
            DataType::ArrayType(_) => match value {
                ArrayValue(_) => Ok(value.encode()),
                z => throw(TypeMismatch(UnsupportedType(self.clone(), z.get_type())))
            }
            DataType::BinaryType(_) => Ok(value.encode()),
            DataType::BLOBType(_) => Ok(value.encode()),
            DataType::BooleanType => Ok(value.encode()),
            DataType::EnumType(_) => Ok(value.encode()),
            DataType::ErrorType => Ok(value.encode()),
            DataType::FunctionType(_) => Ok(value.encode()),
            DataType::NumberType(_) => Ok(value.encode()),
            DataType::PlatformOpsType(_) => Ok(value.encode()),
            DataType::StringType(_) => Ok(value.encode()),
            DataType::StructureType(_) => Ok(value.encode()),
            DataType::TableType(..) =>
                match value.to_table_value() {
                    TableValue(df) => Ok(ByteCodeCompiler::encode_df(&df)),
                    z => throw(TypeMismatch(UnsupportedType(self.clone(), z.get_type())))
                },
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

    ////////////////////////////////////////////////////////////////////
    //  INSTANCE METHODS
    ////////////////////////////////////////////////////////////////////

    /// computes and returns the maximum physical size of the value of this datatype
    pub fn compute_fixed_size(&self) -> usize {
        use crate::data_types::DataType::*;
        let width: usize = match self {
            ArrayType(size) => *size,
            BinaryType(size) => *size,
            BLOBType(..) => 8,
            BooleanType => 1,
            EnumType(..) => 2,
            ErrorType => 256,
            FunctionType(columns) => columns.len() * 8,
            Indeterminate => 8,
            NumberType(nk) => nk.compute_max_physical_size(),
            NumberType(ok) => ok.compute_max_physical_size(),
            PlatformOpsType(..) => 4,
            StringType(size) => match size {
                size => *size + size.to_be_bytes().len(),
                0 => PTR_LEN
            },
            StructureType(columns) => columns.len() * 8,
            TableType(columns, ..) => columns.len() * 8,
            VaryingType(dts) => dts.iter()
                .map(|t| t.compute_fixed_size())
                .max().unwrap_or(0),
        };
        width + 1 // +1 for field metadata
    }

    pub fn to_type_declaration(&self) -> Option<String> {
        let type_name = match self {
            ArrayType(size) => format!("Array({size})"),
            BinaryType(st) => format!("Binary({})", st),
            BLOBType(dt) => format!("BLOB({})", dt.to_type_declaration().unwrap_or(String::new())),
            BooleanType => "Boolean".into(),
            EnumType(labels) => format!("Enum({})", Parameter::render_f(labels, |p| p.to_code_enum())),
            ErrorType => "Error".into(),
            FunctionType(params) => format!("fn({})", Parameter::render(params)),
            Indeterminate => String::new(),
            VaryingType(kinds) => kinds.iter()
                .flat_map(|k| k.to_type_declaration())
                .collect::<Vec<_>>().join("|"),
            NumberType(nk) => nk.get_type_name(),
            NumberType(ok) => ok.get_type_name(),
            PlatformOpsType(pf) => pf.to_code(),
            StringType(st) => format!("String({})", st),
            StructureType(params) => format!("Struct({})", Parameter::render(params)),
            TableType(params, ..) => format!("Table({})", Parameter::render(params)),
        };
        if type_name.is_empty() { None } else { Some(type_name) }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_type_declaration().unwrap_or(String::new()))
    }
}

/// Unit tests
#[cfg(test)]
mod tests {
    /// Experimental Unit tests
    mod exp_tests {
        #[test]
        fn test_model() {}
    }

    /// Core Unit tests
    mod core_tests {
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
        fn test_blob() {
            verify_type_construction(
                "BLOB(Table(symbol: String(8), exchange: String(8), last_sale: f64))",
                BLOBType(Box::from(TableType(make_quote_parameters(), 0))));
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
                    Parameter::build("A"),
                    Parameter::build("B"),
                    Parameter::build("C"),
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
                FunctionType(make_quote_parameters()));
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
        fn test_outcome() {
            verify_type_construction("RowId", NumberType(RowIdKind));
            verify_type_construction("RowsAffected", NumberType(RowsAffectedKind));
            verify_type_construction("Ack", NumberType(AckKind));
        }

        #[test]
        fn test_string() {
            verify_type_construction("String(0)", StringType(0));
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

        fn verify_type_construction(type_decl: &str, data_type: DataType) {
            let dt: DataType = DataType::from_str(type_decl)
                .expect(format!("Failed to parse type {}", data_type).as_str());
            assert_eq!(dt, data_type);
            assert_eq!(data_type.to_type_declaration(), Some(type_decl.into()));
            assert_eq!(format!("{}", data_type), type_decl.to_string())
        }
    }
}