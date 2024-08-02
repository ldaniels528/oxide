////////////////////////////////////////////////////////////////////
// data types module
////////////////////////////////////////////////////////////////////

use std::fmt::Debug;
use std::io;

use serde::{Deserialize, Serialize};

use shared_lib::fail;

use crate::compiler::{Compiler, fail_near};
use crate::data_types::DataType::*;
use crate::expression::Expression::{ColumnSet, Literal};
use crate::server::ColumnJs;
use crate::token_slice::TokenSlice;
use crate::tokens::Token;
use crate::tokens::Token::Atom;

pub const T_UNDEFINED: u8 = 0;
pub const T_ACK: u8 = 0;
pub const T_NULL: u8 = 4;
pub const T_ARRAY: u8 = 8;
pub const T_BLOB: u8 = 12;
pub const T_BOOLEAN: u8 = 16;
pub const T_CLOB: u8 = 20;
pub const T_DATE: u8 = 24;
pub const T_ERROR: u8 = 28;
pub const T_ENUM: u8 = 32;
pub const T_FLOAT32: u8 = 36;
pub const T_FLOAT64: u8 = 40;
pub const T_FUNCTION: u8 = 44;
pub const T_INT8: u8 = 48;
pub const T_INT16: u8 = 52;
pub const T_INT32: u8 = 56;
pub const T_INT64: u8 = 60;
pub const T_INT128: u8 = 64;
pub const T_JSON_OBJECT: u8 = 68;
pub const T_ROWS_AFFECTED: u8 = 72;
pub const T_STRING: u8 = 76;
pub const T_STRUCTURE: u8 = 80;
pub const T_TABLE_NS: u8 = 84;
pub const T_TABLE_VALUE: u8 = 88;
pub const T_TUPLE: u8 = 92;
pub const T_UINT8: u8 = 96;
pub const T_UINT16: u8 = 100;
pub const T_UINT32: u8 = 104;
pub const T_UINT64: u8 = 108;
pub const T_UINT128: u8 = 112;
pub const T_UUID: u8 = 116;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    AckType,
    BLOBType(usize),
    BooleanType,
    CLOBType(usize),
    DateType,
    EnumType(Vec<String>),
    ErrorType,
    Float32Type,
    Float64Type,
    FuncType(Vec<ColumnJs>),
    Int8Type,
    Int16Type,
    Int32Type,
    Int64Type,
    Int128Type,
    JSONObjectType,
    RowsAffectedType,
    StringType(usize),
    StructureType(Vec<ColumnJs>),
    TableType(Vec<ColumnJs>),
    UInt8Type,
    UInt16Type,
    UInt32Type,
    UInt64Type,
    UInt128Type,
    UUIDType,
}

impl DataType {
    /// parses a datatype expression (e.g. "String(20)")
    pub fn compile(column_type: &str) -> io::Result<DataType> {
        let ts = TokenSlice::from_string(column_type);

        fn column_parameters(ts: TokenSlice, f: fn(Vec<ColumnJs>) -> DataType) -> io::Result<DataType> {
            let mut compiler = Compiler::new();
            if let (ColumnSet(params), ts) = compiler.expect_parameters(ts)? {
                //assert!(ts.is_empty());
                Ok(f(params))
            } else { fail("parameters are expected for this type") }
        }

        fn size_parameter(ts: TokenSlice, f: fn(usize) -> DataType) -> io::Result<DataType> {
            let mut compiler = Compiler::new();
            let (args, ts) = compiler.expect_arguments(ts)?;
            if args.len() == 1 {
                match args[0].clone() {
                    Literal(value) => {
                        match value.assume_usize() {
                            Some(size) => Ok(f(size)),
                            _ => fail_near("a numeric value was expected", &ts)
                        }
                    }
                    _ => fail_near("a numeric value was expected", &ts)
                }
            } else { fail("a single parameter was expected for this type") }
        }

        fn string_parameters(ts: TokenSlice, f: fn(Vec<String>) -> DataType) -> io::Result<DataType> {
            let mut compiler = Compiler::new();
            let (args, ts) = compiler.expect_atom_arguments(ts)?;
            Ok(f(args))
        }

        if let (Some(Atom { text: name, .. }), ts) = ts.next() {
            match name.as_str() {
                "Ack" => Ok(AckType),
                "BLOB" => size_parameter(ts, |size| BLOBType(size)),
                "Boolean" => Ok(BooleanType),
                "CLOB" => size_parameter(ts, |size| CLOBType(size)),
                "Date" => Ok(DateType),
                "Enum" => string_parameters(ts, |params| EnumType(params)),
                "Error" => Ok(ErrorType),
                "f32" => Ok(Float32Type),
                "f64" => Ok(Float64Type),
                "fn" => column_parameters(ts, |columns| FuncType(columns)),
                "i8" => Ok(Int8Type),
                "i16" => Ok(Int16Type),
                "i32" => Ok(Int32Type),
                "i64" => Ok(Int64Type),
                "i128" => Ok(Int128Type),
                "JSON" => Ok(JSONObjectType),
                "RowsAffected" => Ok(RowsAffectedType),
                "String" => size_parameter(ts, |size| StringType(size)),
                "Struct" => column_parameters(ts, |columns| StructureType(columns)),
                "Table" => column_parameters(ts, |columns| TableType(columns)),
                "u8" => Ok(UInt8Type),
                "u16" => Ok(UInt16Type),
                "u32" => Ok(UInt32Type),
                "u64" => Ok(UInt64Type),
                "u128" => Ok(UInt128Type),
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
            BLOBType(size) => *size,
            BooleanType => 1,
            CLOBType(size) => *size,
            DateType => 8,
            EnumType(..) => 2,
            ErrorType => 256,
            Float32Type => 4,
            Float64Type => 8,
            FuncType(columns) => columns.len() * 8,
            Int8Type => 1,
            Int16Type => 2,
            Int32Type => 4,
            Int64Type => 8,
            Int128Type => 16,
            JSONObjectType => 512,
            RowsAffectedType => 8,
            StringType(size) => *size + size.to_be_bytes().len(),
            StructureType(columns) => columns.len() * 8,
            TableType(columns) => columns.len() * 8,
            UInt8Type => 1,
            UInt16Type => 2,
            UInt32Type => 4,
            UInt64Type => 8,
            UInt128Type => 16,
            UUIDType => 16
        };
        width + 1 // +1 for field metadata
    }

    pub fn ordinal(&self) -> u8 {
        match self {
            AckType => T_ACK,
            BLOBType(..) => T_BLOB,
            BooleanType => T_BOOLEAN,
            CLOBType(..) => T_CLOB,
            DateType => T_DATE,
            EnumType(..) => T_ENUM,
            ErrorType => T_ERROR,
            Float32Type => T_FLOAT32,
            Float64Type => T_FLOAT64,
            FuncType(..) => T_FUNCTION,
            Int8Type => T_INT8,
            Int16Type => T_INT16,
            Int32Type => T_INT32,
            Int64Type => T_INT64,
            Int128Type => T_INT128,
            JSONObjectType => T_JSON_OBJECT,
            RowsAffectedType => T_ROWS_AFFECTED,
            StringType(..) => T_STRING,
            StructureType(..) => T_STRUCTURE,
            TableType(..) => T_TABLE_VALUE,
            UInt8Type => T_UINT8,
            UInt16Type => T_UINT16,
            UInt32Type => T_UINT32,
            UInt64Type => T_UINT64,
            UInt128Type => T_UINT128,
            UUIDType => T_UUID
        }
    }

    pub fn to_column_type(&self) -> String {
        match self {
            AckType => "Ack".into(),
            BLOBType(size) => format!("BLOB({})", size),
            BooleanType => "Boolean".into(),
            CLOBType(size) => format!("CLOB({})", size),
            DateType => "Date".into(),
            EnumType(labels) => format!("Enum({:?})", labels),
            ErrorType => "Error".into(),
            Float32Type => "f32".into(),
            Float64Type => "f64".into(),
            FuncType(columns) => format!("fn({})", ColumnJs::render_columns(columns)),
            Int8Type => "i8".into(),
            Int16Type => "i16".into(),
            Int32Type => "i32".into(),
            Int64Type => "i64".into(),
            Int128Type => "i128".into(),
            JSONObjectType => "struct".into(),
            RowsAffectedType => "RowsAffected".into(),
            StringType(size) => format!("String({})", size),
            StructureType(columns) => format!("struct({})", ColumnJs::render_columns(columns)),
            TableType(columns) => format!("Table({})", ColumnJs::render_columns(columns)),
            UInt8Type => "u8".into(),
            UInt16Type => "u16".into(),
            UInt32Type => "u32".into(),
            UInt64Type => "u64".into(),
            UInt128Type => "u128".into(),
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
        verify_type_construction("f32", Float32Type);
    }

    #[test]
    fn test_f64() {
        verify_type_construction("f64", Float64Type);
    }

    #[test]
    fn test_fn() {
        verify_type_construction(
            "fn(symbol: String(8), exchange: String(8), last_sale: f64)",
            FuncType(make_quote_columns()));
    }

    #[test]
    fn test_i8() {
        verify_type_construction("i8", Int8Type);
    }

    #[test]
    fn test_i16() {
        verify_type_construction("i16", Int16Type);
    }

    #[test]
    fn test_i32() {
        verify_type_construction("i32", Int32Type);
    }

    #[test]
    fn test_i64() {
        verify_type_construction("i64", Int64Type);
    }

    #[test]
    fn test_i128() {
        verify_type_construction("i128", Int128Type);
    }

    #[test]
    fn test_json_object() {
        verify_type_construction("JSON", JSONObjectType);
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
        verify_type_construction("u8", UInt8Type);
    }

    #[test]
    fn test_u16() {
        verify_type_construction("u16", UInt16Type);
    }

    #[test]
    fn test_u32() {
        verify_type_construction("u32", UInt32Type);
    }

    #[test]
    fn test_u64() {
        verify_type_construction("u64", UInt64Type);
    }

    #[test]
    fn test_u128() {
        verify_type_construction("u128", UInt128Type);
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