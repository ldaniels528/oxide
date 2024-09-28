////////////////////////////////////////////////////////////////////
// data types module
////////////////////////////////////////////////////////////////////

use std::fmt::Debug;
use std::io;

use serde::{Deserialize, Serialize};

use shared_lib::fail;

use crate::compiler::{Compiler, fail_expr, fail_near};
use crate::data_type_kind::DataTypeKind::*;
use crate::data_type_kind::T_NUMBER_START;
use crate::data_types::DataType::*;
use crate::expression::Expression;
use crate::expression::Expression::{ColumnSet, Literal};
use crate::number_kind::NumberKind::*;
use crate::number_kind::NumberKind;
use crate::server::ColumnJs;
use crate::token_slice::TokenSlice;
use crate::tokens::Token;
use crate::tokens::Token::Atom;
use crate::typed_values::*;
use crate::typed_values::TypedValue::Number;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    AckType,
    BackDoorType,
    BLOBType(usize),
    BooleanType,
    CLOBType(usize),
    DateType,
    EnumType(Vec<String>),
    ErrorType,
    FunctionType(Vec<ColumnJs>),
    JSONType,
    NumberType(NumberKind),
    RowsAffectedType,
    StringType(usize),
    StructureType(Vec<ColumnJs>),
    TableType(Vec<ColumnJs>),
    UUIDType,
}

impl DataType {
    /// parses a datatype expression (e.g. "String(20)")
    pub fn compile(column_type: &str) -> io::Result<DataType> {
        let ts = TokenSlice::from_string(column_type);

        fn column_parameters(ts: TokenSlice, f: fn(Vec<ColumnJs>) -> DataType) -> io::Result<DataType> {
            let mut compiler = Compiler::new();
            if let (ColumnSet(params), _) = compiler.expect_parameters(ts)? {
                //assert!(ts.is_empty());
                Ok(f(params))
            } else { fail("parameters are expected for this type") }
        }

        fn extract_params<A>(expr: &Expression, f: fn(&TypedValue) -> A) -> std::io::Result<A> {
            match expr {
                Literal(value) => Ok(f(value)),
                _ => fail_expr("a numeric value was expected", &expr)
            }
        }

        fn size_parameter(ts: TokenSlice, f: fn(usize) -> DataType) -> io::Result<DataType> {
            let mut compiler = Compiler::new();
            let (args, ts) = compiler.expect_arguments(ts)?;
            if args.len() == 1 {
                match args[0].to_owned() {
                    Literal(Number(value)) => Ok(f(value.to_usize())),
                    _ => fail_near("a numeric value was expected", &ts)
                }
            } else { fail("a single parameter was expected for this type") }
        }

        fn string_parameters(ts: TokenSlice, f: fn(Vec<String>) -> DataType) -> io::Result<DataType> {
            let mut compiler = Compiler::new();
            let (args, _) = compiler.expect_atom_arguments(ts)?;
            Ok(f(args))
        }

        if let (Some(Atom { text: name, .. }), ts) = ts.next() {
            match name.as_str() {
                "Ack" => Ok(AckType),
                "BackDoor" => Ok(BackDoorType),
                "BLOB" => size_parameter(ts, |size| BLOBType(size)),
                "Boolean" => Ok(BooleanType),
                "CLOB" => size_parameter(ts, |size| CLOBType(size)),
                "Date" => Ok(DateType),
                "Enum" => string_parameters(ts, |params| EnumType(params)),
                "Error" => Ok(ErrorType),
                "f32" => Ok(NumberType(F32Kind)),
                "f64" => Ok(NumberType(F64Kind)),
                "fn" => column_parameters(ts, |columns| FunctionType(columns)),
                "i8" => Ok(NumberType(I8Kind)),
                "i16" => Ok(NumberType(I16Kind)),
                "i32" => Ok(NumberType(I32Kind)),
                "i64" => Ok(NumberType(I64Kind)),
                "i128" => Ok(NumberType(I128Kind)),
                "JSON" => Ok(JSONType),
                "RowsAffected" => Ok(RowsAffectedType),
                "String" => size_parameter(ts, |size| StringType(size)),
                "Struct" => column_parameters(ts, |columns| StructureType(columns)),
                "Table" => column_parameters(ts, |columns| TableType(columns)),
                "u8" => Ok(NumberType(U8Kind)),
                "u16" => Ok(NumberType(U16Kind)),
                "u32" => Ok(NumberType(U32Kind)),
                "u64" => Ok(NumberType(U64Kind)),
                "u128" => Ok(NumberType(U128Kind)),
                "UUID" => Ok(UUIDType),
                type_name => Err(io::Error::new(io::ErrorKind::Other, format!("unrecognized type {}", type_name)))
            }
        } else {
            fail_near("identifier expected", &ts)
        }
    }

    /// computes and returns the maximum physical size of the value of this datatype
    pub fn compute_max_physical_size(&self) -> usize {
        use crate::data_types::DataType::*;
        let width: usize = match self {
            AckType => 0,
            BackDoorType => 1,
            BLOBType(size) => *size,
            BooleanType => 1,
            CLOBType(size) => *size,
            DateType => 8,
            EnumType(..) => 2,
            ErrorType => 256,
            FunctionType(columns) => columns.len() * 8,
            JSONType => 512,
            NumberType(kind) => match kind {
                I8Kind | U8Kind => 1,
                I16Kind | U16Kind => 2,
                F32Kind | I32Kind | U32Kind => 4,
                F64Kind | I64Kind | U64Kind => 8,
                I128Kind | U128Kind => 16,
                NaNKind => 0,
            },
            RowsAffectedType => 8,
            StringType(size) => *size + size.to_be_bytes().len(),
            StructureType(columns) => columns.len() * 8,
            TableType(columns) => columns.len() * 8,
            UUIDType => 16
        };
        width + 1 // +1 for field metadata
    }

    pub fn ordinal(&self) -> u8 {
        match self {
            AckType => TxAck.to_u8(),
            BackDoorType => TxBackDoor.to_u8(),
            BLOBType(..) => TxBlob.to_u8(),
            BooleanType => TxBoolean.to_u8(),
            CLOBType(..) => TxClob.to_u8(),
            DateType => TxDate.to_u8(),
            EnumType(..) => TxEnum.to_u8(),
            ErrorType => TxError.to_u8(),
            FunctionType(..) => TxFunction.to_u8(),
            JSONType => TxJsonObject.to_u8(),
            NumberType(kind) => T_NUMBER_START + kind.to_u8(),
            RowsAffectedType => TxRowsAffected.to_u8(),
            StringType(..) => TxString.to_u8(),
            StructureType(..) => TxStructure.to_u8(),
            TableType(..) => TxTableValue.to_u8(),
            UUIDType => TxUUID.to_u8()
        }
    }

    pub fn to_column_type(&self) -> String {
        match self {
            AckType => "Ack".into(),
            BackDoorType => "BackDoor".into(),
            BLOBType(size) => format!("BLOB({})", size),
            BooleanType => "Boolean".into(),
            CLOBType(size) => format!("CLOB({})", size),
            DateType => "Date".into(),
            EnumType(labels) => format!("Enum({:?})", labels),
            ErrorType => "Error".into(),
            FunctionType(columns) => format!("fn({})", ColumnJs::render_columns(columns)),
            JSONType => "struct".into(),
            NumberType(index) => {
                match *index {
                    F32Kind => "f32".into(),
                    F64Kind => "f64".into(),
                    I8Kind => "i8".into(),
                    I16Kind => "i16".into(),
                    I32Kind => "i32".into(),
                    I64Kind => "i64".into(),
                    I128Kind => "i128".into(),
                    U8Kind => "u8".into(),
                    U16Kind => "u16".into(),
                    U32Kind => "u32".into(),
                    U64Kind => "u64".into(),
                    U128Kind => "u128".into(),
                    NaNKind => "NaN".into()
                }
            }
            RowsAffectedType => "RowsAffected".into(),
            StringType(size) => format!("String({})", size),
            StructureType(columns) => format!("struct({})", ColumnJs::render_columns(columns)),
            TableType(columns) => format!("Table({})", ColumnJs::render_columns(columns)),
            UUIDType => "UUID".into()
        }
    }

    fn transfer_to_string_array(token_slice: &[Token]) -> Vec<&str> {
        use Token::*;
        token_slice.into_iter()
            .fold(Vec::new(), |mut acc, t| {
                match t {
                    Operator { text: value, .. } if value == "," => acc,
                    Atom { text: value, .. } => {
                        acc.push(value);
                        acc
                    }
                    Numeric { text: value, .. } => {
                        acc.push(value);
                        acc
                    }
                    _ => acc
                }
            })
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::data_types::DataType;
    use crate::data_types::DataType::*;
    use crate::number_kind::NumberKind::*;
    use crate::testdata::make_quote_columns;

    #[test]
    fn test_blob() {
        verify_type_construction("BLOB(5566)", BLOBType(5566));
    }

    #[test]
    fn test_boolean() {
        verify_type_construction("Boolean", BooleanType);
    }

    #[test]
    fn test_clob() {
        verify_type_construction("CLOB(3377)", CLOBType(3377));
    }

    #[test]
    fn test_date() {
        verify_type_construction("Date", DateType);
    }

    #[test]
    fn test_enum() {
        verify_type_construction(
            "Enum(A,B,C)",
            EnumType(vec!["A".to_owned(), "B".to_owned(), "C".to_owned()]));
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
            FunctionType(make_quote_columns()));
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
    fn test_json_object() {
        verify_type_construction("JSON", JSONType);
    }

    #[test]
    fn test_rows_affected() {
        verify_type_construction("RowsAffected", RowsAffectedType);
    }

    #[test]
    fn test_string() {
        verify_type_construction("String(10)", StringType(10));
    }

    #[test]
    fn test_struct() {
        verify_type_construction(
            "Struct(symbol: String(8), exchange: String(8), last_sale: f64)",
            StructureType(make_quote_columns()));
    }

    #[test]
    fn test_table() {
        verify_type_construction(
            "Table(symbol: String(8), exchange: String(8), last_sale: f64)",
            TableType(make_quote_columns()));
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

    #[test]
    fn test_uuid() {
        verify_type_construction("UUID", UUIDType);
    }

    fn verify_type_construction(type_decl: &str, data_type: DataType) {
        let dt: DataType = DataType::compile(type_decl)
            .expect("Failed to parse column type");
        assert_eq!(dt, data_type)
    }
}