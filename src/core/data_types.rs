////////////////////////////////////////////////////////////////////
// DataType class
////////////////////////////////////////////////////////////////////

use std::fmt::{Debug, Display};
use std::io;

use serde::{Deserialize, Serialize};

use shared_lib::fail;

use crate::compiler::{fail_near, Compiler};
use crate::data_types::DataType::*;
use crate::data_types::SizeTypes::{Fixed, Unlimited};
use crate::errors::Errors::Syntax;
use crate::expression::Expression::{Literal, Parameters};
use crate::number_kind::NumberKind;
use crate::number_kind::NumberKind::*;
use crate::outcomes::OutcomeKind;
use crate::parameter::Parameter;
use crate::token_slice::TokenSlice;
use crate::tokens::Token::Atom;
use crate::typed_values::TypedValue::{ErrorValue, Number};

const PTR_LEN: usize = 8;

/// Represents a Size Type
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum SizeTypes {
    Fixed(usize),
    Unlimited,
}

impl SizeTypes {
    pub fn to_size(&self) -> usize {
        match self {
            Fixed(size) => *size,
            Unlimited => PTR_LEN
        }
    }
}

impl Display for SizeTypes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", match self {
            Fixed(size) => size.to_string(),
            Unlimited => "".to_string()
        })
    }
}

/// Represents a Type
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum DataType {
    ArrayType(Box<DataType>),
    BLOBType(SizeTypes),
    BooleanType,
    DateType,
    EnumType(Vec<Parameter>),
    ErrorType,
    FunctionType(Vec<Parameter>),
    InferredType,
    NumberType(NumberKind),
    OutcomeType(OutcomeKind),
    StringType(SizeTypes),
    StructureType(Vec<Parameter>),
    TableType(Vec<Parameter>),
    UUIDType,
}

impl DataType {

    ////////////////////////////////////////////////////////////////////
    //  STATIC METHODS
    ////////////////////////////////////////////////////////////////////

    /// parses a datatype expression (e.g. "String(20)")
    pub fn compile(param_type: &str) -> io::Result<DataType> {
        let ts = TokenSlice::from_string(param_type);
        Self::compile_tokens(ts)
    }

    /// parses a datatype expression (e.g. "String(20)")
    pub fn compile_tokens(ts: TokenSlice) -> io::Result<DataType> {
        if let (Some(Atom { text: name, .. }), ts) = ts.next() {
            match name.as_str() {
                "Ack" => Ok(OutcomeType(OutcomeKind::Acked)),
                "Array" => DataType::compile_type(ts, |kind| ArrayType(Box::new(kind))),
                "BLOB" => DataType::compile_size(ts, |size| BLOBType(size)),
                "Boolean" => Ok(BooleanType),
                "Date" => Ok(DateType),
                "enum" => DataType::compile_parameters(ts, |params| EnumType(params)),
                "Error" => Ok(ErrorType),
                "f32" => Ok(NumberType(F32Kind)),
                "f64" => Ok(NumberType(F64Kind)),
                "fn" => DataType::compile_parameters(ts, |params| FunctionType(params)),
                "i8" => Ok(NumberType(I8Kind)),
                "i16" => Ok(NumberType(I16Kind)),
                "i32" => Ok(NumberType(I32Kind)),
                "i64" => Ok(NumberType(I64Kind)),
                "i128" => Ok(NumberType(I128Kind)),
                "RowId" => Ok(OutcomeType(OutcomeKind::RowInserted)),
                "RowsAffected" => Ok(OutcomeType(OutcomeKind::RowsUpdated)),
                "String" => DataType::compile_size(ts, |size| StringType(size)),
                "struct" => DataType::compile_parameters(ts, |params| StructureType(params)),
                "Table" => DataType::compile_parameters(ts, |params| TableType(params)),
                "u8" => Ok(NumberType(U8Kind)),
                "u16" => Ok(NumberType(U16Kind)),
                "u32" => Ok(NumberType(U32Kind)),
                "u64" => Ok(NumberType(U64Kind)),
                "u128" => Ok(NumberType(U128Kind)),
                "UUID" => Ok(UUIDType),
                type_name => fail(format!("unrecognized type {}", type_name))
            }
        } else {
            fail_near("identifier expected", &ts)
        }
    }

    fn compile_parameters(ts: TokenSlice, f: fn(Vec<Parameter>) -> DataType) -> io::Result<DataType> {
        let mut compiler = Compiler::new();
        if let (Parameters(params), _) = compiler.expect_parameters(ts)? {
            //assert!(ts.is_empty());
            Ok(f(params))
        } else { fail("parameters are expected for this type") }
    }

    fn compile_size(ts: TokenSlice, f: fn(SizeTypes) -> DataType) -> io::Result<DataType> {
        let mut compiler = Compiler::new();
        let (args, ts) = compiler.expect_arguments(ts)?;
        let kind = match args.as_slice() {
            // String()
            [] => Unlimited,
            // String(40)
            [Literal(Number(size))] => Fixed(size.to_usize()),
            // enum(a, b, c)?
            other => return fail(ErrorValue(
                Syntax(other.iter()
                    .map(|e| e.to_code())
                    .collect::<Vec<_>>()
                    .join(", "))
            ).to_string())
        };
        Ok(f(kind))
    }

    fn compile_type(ts: TokenSlice, f: fn(DataType) -> DataType) -> io::Result<DataType> {
        let mut args = Vec::new();
        let mut ts = ts.expect("<")?;
        while ts.has_next() && ts.isnt(">") {
            match ts.next() {
                (Some(tok), new_ts) => {
                    args.push(tok);
                    ts = new_ts;
                }
                (None, ts) =>
                    return fail_near("Type declaration expected", &ts)
            }
        }
        let ts = ts.expect(">")?;
        if !ts.is_empty() {
            return fail_near("Syntax error", &ts);
        }

        let kind = if args.is_empty() { InferredType } else { Self::compile_tokens(TokenSlice::new(args))? };
        Ok(f(kind))
    }

    ////////////////////////////////////////////////////////////////////
    //  INSTANCE METHODS
    ////////////////////////////////////////////////////////////////////

    /// computes and returns the maximum physical size of the value of this datatype
    pub fn compute_max_physical_size(&self) -> usize {
        use crate::data_types::DataType::*;
        let width: usize = match self {
            ArrayType(kind) => 10 * kind.compute_max_physical_size(),
            BLOBType(size) => size.to_size(),
            BooleanType => 1,
            DateType => 8,
            EnumType(..) => 2,
            ErrorType => 256,
            FunctionType(columns) => columns.len() * 8,
            InferredType => 0,
            NumberType(kind) => match kind {
                I8Kind | U8Kind => 1,
                I16Kind | U16Kind => 2,
                F32Kind | I32Kind | U32Kind => 4,
                F64Kind | I64Kind | U64Kind => 8,
                I128Kind | U128Kind => 16,
                NaNKind => 0,
            },
            OutcomeType(kind) => match kind {
                OutcomeKind::Acked => 8,
                OutcomeKind::RowInserted => 16,
                OutcomeKind::RowsUpdated => 16,
            },
            StringType(size) => match size {
                Fixed(size) => *size + size.to_be_bytes().len(),
                Unlimited => PTR_LEN
            },
            StructureType(columns) => columns.len() * 8,
            TableType(columns) => columns.len() * 8,
            InferredType => 0,
            UUIDType => 16
        };
        width + 1 // +1 for field metadata
    }

    pub fn to_type_declaration(&self) -> Option<String> {
        let type_name = match self {
            ArrayType(kind) =>
                format!("Array<{}>", kind.to_type_declaration().unwrap_or("".to_string())),
            BLOBType(size) => format!("BLOB({})", size),
            BooleanType => "Boolean".into(),
            DateType => "Date".into(),
            EnumType(labels) => format!("enum({})", Parameter::render(labels)),
            ErrorType => "Error".into(),
            FunctionType(columns) => format!("fn({})", Parameter::render(columns)),
            InferredType => "".into(),
            NumberType(index) => match *index {
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
            },
            OutcomeType(kind) => match kind {
                OutcomeKind::Acked => "Ack".into(),
                OutcomeKind::RowInserted => "RowId".into(),
                OutcomeKind::RowsUpdated => "RowsAffected".into(),
            },
            StringType(size) => format!("String({})", size),
            StructureType(params) => format!("struct({})", Parameter::render(params)),
            TableType(columns) => format!("Table({})", Parameter::render(columns)),
            UUIDType => "UUID".into(),
        };
        if type_name.is_empty() { None } else { Some(type_name) }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_type_declaration().unwrap_or("".to_string()))
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::data_types::DataType;
    use crate::data_types::DataType::*;
    use crate::data_types::SizeTypes::Fixed;
    use crate::number_kind::NumberKind::*;
    use crate::outcomes::OutcomeKind;
    use crate::parameter::Parameter;
    use crate::testdata::make_quote_parameters;

    #[test]
    fn test_blob() {
        verify_type_construction("BLOB(5566)", BLOBType(Fixed(5566)));
    }

    #[test]
    fn test_boolean() {
        verify_type_construction("Boolean", BooleanType);
    }

    #[test]
    fn test_date() {
        verify_type_construction("Date", DateType);
    }

    #[test]
    fn test_enums() {
        verify_type_construction(
            "enum(A, B, C)",
            EnumType(vec![
                Parameter::new("A", None, None),
                Parameter::new("B", None, None),
                Parameter::new("C", None, None),
            ]));

        verify_type_construction(
            "enum(AMEX = 1, NASDAQ = 2, NYSE = 3, OTCBB = 4)",
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
        verify_type_construction("Ack", OutcomeType(OutcomeKind::Acked));
        verify_type_construction("RowId", OutcomeType(OutcomeKind::RowInserted));
        verify_type_construction("RowsAffected", OutcomeType(OutcomeKind::RowsUpdated));
    }

    #[test]
    fn test_string() {
        verify_type_construction("String(10)", StringType(Fixed(10)));
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
            TableType(make_quote_parameters()));
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
        assert_eq!(dt, data_type);
        assert_eq!(data_type.to_type_declaration(), Some(type_decl.into()));
        assert_eq!(format!("{}", data_type), type_decl.to_string())
    }
}