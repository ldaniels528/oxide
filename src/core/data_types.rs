////////////////////////////////////////////////////////////////////
// data types module
////////////////////////////////////////////////////////////////////

use std::fmt::Debug;
use std::io;

use serde::{Deserialize, Serialize};

use shared_lib::fail;

use crate::compiler::{CompilerState, fail_near};
use crate::data_types::DataType::*;
use crate::expression::Expression::{ColumnSet, Literal};
use crate::server::ColumnJs;
use crate::token_slice::TokenSlice;
use crate::tokens::Token;
use crate::tokens::Token::Atom;

pub const T_UNDEFINED: u8 = 0;
pub const T_NULL: u8 = 1;
pub const T_ARRAY: u8 = 2;
pub const T_BLOB: u8 = 3;
pub const T_BOOLEAN: u8 = 4;
pub const T_CLOB: u8 = 5;
pub const T_DATE: u8 = 6;
pub const T_ENUM: u8 = 7;
pub const T_FLOAT32: u8 = 8;
pub const T_FLOAT64: u8 = 9;
pub const T_FUNC: u8 = 10;
pub const T_INT8: u8 = 11;
pub const T_INT16: u8 = 12;
pub const T_INT32: u8 = 13;
pub const T_INT64: u8 = 14;
pub const T_INT128: u8 = 15;
pub const T_JSON_OBJECT: u8 = 16;
pub const T_RECORD_NUMBER: u8 = 17;
pub const T_STRING: u8 = 18;
pub const T_STRUCT: u8 = 19;
pub const T_TABLE_REF: u8 = 20;
pub const T_TABLE: u8 = 21;
pub const T_TUPLE: u8 = 22;
pub const T_UINT8: u8 = 23;
pub const T_UINT16: u8 = 24;
pub const T_UINT32: u8 = 25;
pub const T_UINT64: u8 = 26;
pub const T_UINT128: u8 = 27;
pub const T_UUID: u8 = 28;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    BLOBType(usize),
    BooleanType,
    CLOBType(usize),
    DateType,
    EnumType(Vec<String>),
    Float32Type,
    Float64Type,
    FuncType(Vec<ColumnJs>),
    Int8Type,
    Int16Type,
    Int32Type,
    Int64Type,
    Int128Type,
    JSONObjectType,
    RecordNumberType,
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
            let mut compiler = CompilerState::new();
            if let (ColumnSet(params), ts) = compiler.expect_parameters(ts)? {
                //assert!(ts.is_empty());
                Ok(f(params))
            } else { fail("parameters are expected for this type") }
        }

        fn size_parameter(ts: TokenSlice, f: fn(usize) -> DataType) -> io::Result<DataType> {
            let mut compiler = CompilerState::new();
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
            let mut compiler = CompilerState::new();
            let (args, ts) = compiler.expect_atom_arguments(ts)?;
            Ok(f(args))
        }

        if let (Some(Atom { text: name, .. }), ts) = ts.next() {
            match name.as_str() {
                "BLOB" => size_parameter(ts, |size| BLOBType(size)),
                "Boolean" => Ok(BooleanType),
                "CLOB" => size_parameter(ts, |size| CLOBType(size)),
                "Date" => Ok(DateType),
                "Enum" => string_parameters(ts, |params| EnumType(params)),
                "f32" => Ok(Float32Type),
                "f64" => Ok(Float64Type),
                "fn" => column_parameters(ts, |columns| FuncType(columns)),
                "i8" => Ok(Int8Type),
                "i16" => Ok(Int16Type),
                "i32" => Ok(Int32Type),
                "i64" => Ok(Int64Type),
                "i128" => Ok(Int128Type),
                "JSON" => Ok(JSONObjectType),
                "RecordNumber" => Ok(RecordNumberType),
                "String" => size_parameter(ts, |size| StringType(size)),
                "struct" => column_parameters(ts, |columns| StructureType(columns)),
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

    /// computes and returns the maximum physical size of a value of this datatype
    pub fn compute_max_physical_size(&self) -> usize {
        use crate::data_types::DataType::*;
        let width: usize = match self {
            BLOBType(size) => *size,
            BooleanType => 1,
            CLOBType(size) => *size,
            DateType => 8,
            EnumType(..) => 2,
            Float32Type => 4,
            Float64Type => 8,
            FuncType(columns) => columns.len() * 8,
            Int8Type => 1,
            Int16Type => 2,
            Int32Type => 4,
            Int64Type => 8,
            Int128Type => 16,
            JSONObjectType => 512,
            RecordNumberType => 8,
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
            BLOBType(..) => T_BLOB,
            BooleanType => T_BOOLEAN,
            CLOBType(..) => T_CLOB,
            DateType => T_DATE,
            EnumType(..) => T_ENUM,
            Float32Type => T_FLOAT32,
            Float64Type => T_FLOAT64,
            FuncType(..) => T_FUNC,
            Int8Type => T_INT8,
            Int16Type => T_INT16,
            Int32Type => T_INT32,
            Int64Type => T_INT64,
            Int128Type => T_INT128,
            JSONObjectType => T_JSON_OBJECT,
            RecordNumberType => T_RECORD_NUMBER,
            StringType(..) => T_STRING,
            StructureType(..) => T_STRUCT,
            TableType(..) => T_TABLE,
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
            BLOBType(size) => format!("BLOB({})", size),
            BooleanType => "Boolean".into(),
            CLOBType(size) => format!("CLOB({})", size),
            DateType => "Date".into(),
            EnumType(labels) => format!("Enum({:?})", labels),
            Float32Type => "f32".into(),
            Float64Type => "f64".into(),
            FuncType(columns) => format!("fn({})", ColumnJs::render_columns(columns)),
            Int8Type => "i8".into(),
            Int16Type => "i16".into(),
            Int32Type => "i32".into(),
            Int64Type => "i64".into(),
            Int128Type => "i128".into(),
            JSONObjectType => "struct".into(),
            RecordNumberType => "RecordNumber".into(),
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
        token_slice.into_iter()
            .fold(Vec::new(), |mut acc, t| {
                match t {
                    Token::Operator { text: value, .. } if value == "," => acc,
                    Token::Atom { text: value, .. } => {
                        acc.push(value);
                        acc
                    }
                    Token::Numeric { text: value, .. } => {
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
        verify("BLOB(5566)", BLOBType(5566));
    }

    #[test]
    fn test_boolean() {
        verify("Boolean", BooleanType);
    }

    #[test]
    fn test_clob() {
        verify("CLOB(3377)", CLOBType(3377));
    }

    #[test]
    fn test_date() {
        verify("Date", DateType);
    }

    #[test]
    fn test_enum() {
        verify(
            "Enum(A,B,C)",
            EnumType(vec!["A".to_owned(), "B".to_owned(), "C".to_owned()]));
    }

    #[test]
    fn test_f32() {
        verify("f32", Float32Type);
    }

    #[test]
    fn test_f64() {
        verify("f64", Float64Type);
    }

    #[test]
    fn test_fn() {
        verify(
            "fn(symbol: String(8), exchange: String(8), last_sale: f64)",
            FuncType(make_quote_columns()));
    }

    #[test]
    fn test_i8() {
        verify("i8", Int8Type);
    }

    #[test]
    fn test_i16() {
        verify("i16", Int16Type);
    }

    #[test]
    fn test_i32() {
        verify("i32", Int32Type);
    }

    #[test]
    fn test_i64() {
        verify("i64", Int64Type);
    }

    #[test]
    fn test_i128() {
        verify("i128", Int128Type);
    }

    #[test]
    fn test_json_object() {
        verify("JSON", JSONObjectType);
    }

    #[test]
    fn test_row_id() {
        verify("RecordNumber", RecordNumberType);
    }

    #[test]
    fn test_string() {
        verify("String(10)", StringType(10));
    }

    #[test]
    fn test_struct() {
        verify(
            "struct(symbol: String(8), exchange: String(8), last_sale: f64)",
            StructureType(make_quote_columns()));
    }

    #[test]
    fn test_table() {
        verify(
            "Table(symbol: String(8), exchange: String(8), last_sale: f64)",
            TableType(make_quote_columns()));
    }

    #[test]
    fn test_u8() {
        verify("u8", UInt8Type);
    }

    #[test]
    fn test_u16() {
        verify("u16", UInt16Type);
    }

    #[test]
    fn test_u32() {
        verify("u32", UInt32Type);
    }

    #[test]
    fn test_u64() {
        verify("u64", UInt64Type);
    }

    #[test]
    fn test_u128() {
        verify("u128", UInt128Type);
    }

    #[test]
    fn test_uuid() {
        verify("UUID", UUIDType);
    }

    fn verify(type_decl: &str, data_type: DataType) {
        let dt: DataType = DataType::compile(type_decl)
            .expect("Failed to parse column type");
        assert_eq!(dt, data_type)
    }
}