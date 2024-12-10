////////////////////////////////////////////////////////////////////
// DataType class
////////////////////////////////////////////////////////////////////

use crate::arrays::Array;
use crate::byte_code_compiler::ByteCodeCompiler;
use crate::compiler::Compiler;
use crate::data_types::DataType::*;
use crate::data_types::StorageTypes::{BLOBSized, FixedSize};
use crate::dataframe::Dataframe::Model;
use crate::errors::Errors::{ArgumentsMismatched, Exact, Syntax};
use crate::errors::{throw, Errors};
use crate::expression::Expression;
use crate::expression::Expression::{AsValue, FunctionCall, Literal, SetVariable, Variable};
use crate::model_row_collection::ModelRowCollection;
use crate::number_kind::NumberKind;
use crate::number_kind::NumberKind::*;
use crate::numbers::Numbers;
use crate::parameter::Parameter;
use crate::platform::PlatformOps;
use crate::structures::{HardStructure, Structure};
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{ArrayValue, Binary, Boolean, ErrorValue, Function, Null, Number, PlatformOp, StringValue, StructureHard, TableValue, Undefined, BLOB};
use serde::{Deserialize, Serialize};
use shared_lib::fail;
use std::fmt::{Debug, Display};
use std::io;
use std::ops::Deref;

const PTR_LEN: usize = 8;

/// Represents a Size Type
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum StorageTypes {
    FixedSize(usize),
    BLOBSized,
}

impl StorageTypes {
    pub fn to_size(&self) -> usize {
        match self {
            FixedSize(size) => *size,
            BLOBSized => PTR_LEN
        }
    }
}

impl Display for StorageTypes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", match self {
            FixedSize(size) => size.to_string(),
            BLOBSized => "".to_string()
        })
    }
}

/// Represents an Oxide-native datatype
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum DataType {
    ArrayType(Box<DataType>),
    BinaryType(StorageTypes),
    BLOBType(Box<DataType>),
    BooleanType,
    EnumType(Vec<Parameter>),
    ErrorType,
    FunctionType(Vec<Parameter>),
    NumberType(NumberKind),
    PlatformOpsType(PlatformOps),
    StringType(StorageTypes),
    StructureType(Vec<Parameter>),
    TableType(Vec<Parameter>, StorageTypes),
    UnionType(Vec<DataType>),
}

impl DataType {
    ////////////////////////////////////////////////////////////////////
    //  STATIC METHODS
    ////////////////////////////////////////////////////////////////////

    /// parses a datatype expression (e.g. "String(20)")
    fn decipher_type(model: &Expression) -> std::io::Result<DataType> {
        fn expect_size(args: &Vec<Expression>, f: fn(StorageTypes) -> DataType) -> std::io::Result<DataType> {
            match args.as_slice() {
                [] => Ok(f(BLOBSized)),
                [Literal(Number(n))] => Ok(f(FixedSize(n.to_usize()))),
                [other] => throw(Syntax(other.to_code())),
                other => throw(ArgumentsMismatched(1, other.len()))
            }
        }
        fn expect_type(args: &Vec<Expression>, f: fn(DataType) -> DataType) -> std::io::Result<DataType> {
            match args.as_slice() {
                [item] => Ok(f(decipher(item)?)),
                other => throw(ArgumentsMismatched(1, other.len()))
            }
        }
        fn expect_params(args: &Vec<Expression>, f: fn(Vec<Parameter>) -> DataType) -> std::io::Result<DataType> {
            let mut params: Vec<Parameter> = vec![];
            for arg in args {
                let param = match arg {
                    AsValue(name, model) => Parameter::new(name, decipher(model)?.to_type_declaration(), None),
                    SetVariable(name, value) => Parameter::new(name, None, Some(value.to_code())),
                    Variable(name) => Parameter::new(name, None, None),
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
                Literal(StructureHard(hs)) =>
                    Ok(StructureType(hs.get_parameters())),
                Variable(name) => match name.as_str() {
                    "Ack" => Ok(NumberType(AckKind)),
                    "Boolean" => Ok(BooleanType),
                    "Date" => Ok(NumberType(DateKind)),
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
                                "Array" => expect_type(args, |kind| match kind {
                                    StructureType(p) => TableType(p, BLOBSized),
                                    kind => ArrayType(Box::new(kind))
                                }),
                                "Binary" => expect_size(args, |size| BinaryType(size)),
                                "BLOB" => expect_type(args, |kind| BLOBType(Box::new(kind))),
                                "enum" => expect_params(args, |params| EnumType(params)),
                                "fn" => expect_params(args, |params| FunctionType(params)),
                                "String" => expect_size(args, |size| StringType(size)),
                                "struct" => expect_params(args, |params| StructureType(params)),
                                "Table" => expect_params(args, |params| TableType(params, BLOBSized)),
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
            StringType(size) => StringValue(ByteCodeCompiler::decode_string(buffer, offset, size.to_size()).to_string()),
            StructureType(params) =>
                match HardStructure::from_parameters(params) {
                    Ok(structure) => StructureHard(structure),
                    Err(err) => ErrorValue(Errors::Exact(err.to_string()))
                }
            TableType(columns, ..) => TableValue(Model(ModelRowCollection::construct(columns))),
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
            UnionType(..) => Null,
            NumberType(kind) => Number(kind.decode_buffer(bcc)?),
            PlatformOpsType(pf) => PlatformOp(pf.to_owned()),
            StringType(..) => StringValue(bcc.next_string()),
            StructureType(params) => StructureHard(bcc.next_struct_with_parameters(params)?),
            TableType(columns, ..) => TableValue(Model(bcc.next_table_with_columns(columns)?)),
            UnionType(..) => Undefined,
        };
        Ok(tv)
    }

    /// parses a datatype expression (e.g. "String(20)")
    pub fn from_str(param_type: &str) -> io::Result<DataType> {
        let model = Compiler::build(param_type)?;
        Self::decipher_type(&model)
    }

    ////////////////////////////////////////////////////////////////////
    //  INSTANCE METHODS
    ////////////////////////////////////////////////////////////////////

    /// computes and returns the maximum physical size of the value of this datatype
    pub fn compute_max_physical_size(&self) -> usize {
        use crate::data_types::DataType::*;
        let width: usize = match self {
            ArrayType(kind) => 10 * kind.compute_max_physical_size(),
            BinaryType(size) => size.to_size(),
            BLOBType(..) => 8,
            BooleanType => 1,
            EnumType(..) => 2,
            ErrorType => 256,
            FunctionType(columns) => columns.len() * 8,
            NumberType(nk) => nk.compute_max_physical_size(),
            NumberType(ok) => ok.compute_max_physical_size(),
            PlatformOpsType(..) => 4,
            StringType(size) => match size {
                FixedSize(size) => *size + size.to_be_bytes().len(),
                BLOBSized => PTR_LEN
            },
            StructureType(columns) => columns.len() * 8,
            TableType(columns, ..) => columns.len() * 8,
            UnionType(dts) => dts.iter()
                .map(|t| t.compute_max_physical_size())
                .max().unwrap_or(0),
        };
        width + 1 // +1 for field metadata
    }

    pub fn to_type_declaration(&self) -> Option<String> {
        let type_name = match self {
            ArrayType(dt) =>
                format!("Array({})", dt.to_type_declaration()
                    .unwrap_or("".to_string())),
            BinaryType(st) => format!("Binary({})", st),
            BLOBType(dt) => format!("BLOB({})", dt.to_type_declaration().unwrap_or("".to_string())),
            BooleanType => "Boolean".into(),
            EnumType(labels) => format!("enum({})", Parameter::render(labels)),
            ErrorType => "Error".into(),
            FunctionType(params) => format!("fn({})", Parameter::render(params)),
            UnionType(kinds) => kinds.iter()
                .flat_map(|k| k.to_type_declaration())
                .collect::<Vec<_>>().join("|"),
            NumberType(nk) => nk.get_type_name(),
            NumberType(ok) => ok.get_type_name(),
            PlatformOpsType(pf) => pf.to_code(),
            StringType(st) => format!("String({})", st),
            StructureType(params) => format!("struct({})", Parameter::render(params)),
            TableType(params, ..) => format!("Table({})", Parameter::render(params)),
        };
        if type_name.is_empty() { None } else { Some(type_name) }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_type_declaration().unwrap_or("".to_string()))
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
        use crate::data_types::StorageTypes::{BLOBSized, FixedSize};
        use crate::number_kind::NumberKind::*;
        use crate::parameter::Parameter;
        use crate::testdata::make_quote_parameters;

        #[test]
        fn test_array() {
            verify_type_construction(
                "Array(String(12))",
                ArrayType(Box::from(StringType(FixedSize(12)))));
        }

        #[test]
        fn test_binary() {
            verify_type_construction("Binary(5566)", BinaryType(FixedSize(5566)));
        }

        #[test]
        fn test_blob() {
            verify_type_construction(
                "BLOB(Table(symbol: String(8), exchange: String(8), last_sale: f64))",
                BLOBType(Box::from(TableType(make_quote_parameters(), BLOBSized))));
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
                "enum(A, B, C)",
                EnumType(vec![
                    Parameter::new("A", None, None),
                    Parameter::new("B", None, None),
                    Parameter::new("C", None, None),
                ]));
        }

        #[test]
        fn test_enums_1() {
            verify_type_construction(
                "enum(AMEX := 1, NASDAQ := 2, NYSE := 3, OTCBB := 4)",
                EnumType(vec![
                    Parameter::new("AMEX", None, Some("1".to_string())),
                    Parameter::new("NASDAQ", None, Some("2".to_string())),
                    Parameter::new("NYSE", None, Some("3".to_string())),
                    Parameter::new("OTCBB", None, Some("4".to_string())),
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
            verify_type_construction("String()", StringType(BLOBSized));
            verify_type_construction("String(10)", StringType(FixedSize(10)));
        }

        #[test]
        fn test_struct() {
            verify_type_construction(
                "struct(symbol: String(8), exchange: String(8), last_sale: f64)",
                StructureType(make_quote_parameters()));
        }

        #[test]
        fn test_table() {
            verify_type_construction(
                "Table(symbol: String(8), exchange: String(8), last_sale: f64)",
                TableType(make_quote_parameters(), BLOBSized));
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