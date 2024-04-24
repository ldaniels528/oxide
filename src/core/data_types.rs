////////////////////////////////////////////////////////////////////
// data types module
////////////////////////////////////////////////////////////////////

use std::fmt::Debug;
use std::io;

use serde::{Deserialize, Serialize};

use shared_lib::{cnv_error, fail};

use crate::data_types::DataType::*;
use crate::server::ColumnJs;
use crate::tokenizer::parse_fully;
use crate::tokens::Token;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    BLOBType(usize),
    BooleanType,
    CLOBType(usize),
    DateType,
    EnumType(Vec<String>),
    Int8Type,
    Int16Type,
    Int32Type,
    Int64Type,
    Float32Type,
    Float64Type,
    RecordNumberType,
    StringType(usize),
    StructureType(Vec<ColumnJs>),
    TableType(Vec<ColumnJs>),
    UUIDType,
}

impl DataType {
    /// computes and returns the maximum physical size of a value of this datatype
    pub fn compute_max_physical_size(&self) -> usize {
        use crate::data_types::DataType::*;
        let width: usize = match self {
            BLOBType(size) => *size,
            BooleanType => 1,
            CLOBType(size) => *size,
            DateType => 8,
            EnumType(..) => 2,
            Int8Type => 1,
            Int16Type => 2,
            Int32Type => 4,
            Int64Type => 8,
            Float32Type => 4,
            Float64Type => 8,
            RecordNumberType => 8,
            StringType(size) => *size + size.to_be_bytes().len(),
            StructureType(..) => 8,
            TableType(..) => 8,
            UUIDType => 16
        };
        width + 1 // +1 for field metadata
    }

    /// parses a datatype expression (e.g. "String(20)")
    pub fn parse(column_type: &str) -> io::Result<DataType> {
        let tokens: Vec<Token> = parse_fully(column_type);
        let token_slice: &[Token] = tokens.as_slice();
        match token_slice {
            // ex: Int
            [Token::Atom { text: name, .. }] =>
                DataType::resolve(name, &[]),
            // ex: String(60)
            [Token::Atom { text: name, .. },
            Token::Operator { text: op0, .. },
            Token::Numeric { text: arg, .. },
            Token::Operator { text: op1, .. }] if op0 == "(" && op1 == ")" =>
                DataType::resolve(name, &[arg]),
            // ex: Struct(symbol String(10), exchange String(10), last Double)
            [Token::Atom { text: name, .. },
            Token::Operator { text: op0, .. }, ..,
            Token::Operator { text: op1, .. }] if op0 == "(" && op1 == ")" => {
                let arg_tokens: &[Token] = &token_slice[1..(token_slice.len() - 1)];
                DataType::resolve(name, DataType::transfer_to_string_array(arg_tokens).as_slice())
            }
            // unrecognized - syntax error?
            tok => fail(format!("malformed type definition near {}", tok[0]))
        }
    }

    /// resolves a datatype by name
    pub fn resolve(name: &str, args: &[&str]) -> io::Result<DataType> {
        fn parameterless(data_type: DataType, args: &[&str]) -> io::Result<DataType> {
            if args.is_empty() { Ok(data_type) } else { Err(io::Error::new(io::ErrorKind::Other, "Parameters are not supported for this type")) }
        }

        fn size_parameter(f: fn(usize) -> DataType, args: &[&str]) -> io::Result<DataType> {
            if args.len() == 1 {
                Ok(f(args[0].parse::<usize>().map_err(|e| cnv_error!(e))?))
            } else { fail("a single parameter was expected for this type") }
        }

        match name {
            "BLOB" => size_parameter(|size| BLOBType(size), args),
            "Boolean" => parameterless(BooleanType, args),
            "Byte" => parameterless(Int8Type, args),
            "CLOB" => size_parameter(|size| CLOBType(size), args),
            "Date" => parameterless(DateType, args),
            "Double" => parameterless(Float64Type, args),
            "Enum" => Ok(EnumType(args.iter().map(|s| s.to_string()).collect())),
            "Float" => parameterless(Float32Type, args),
            "Int" => parameterless(Int32Type, args),
            "Long" => parameterless(Int64Type, args),
            "RecordNumber" => parameterless(RecordNumberType, args),
            "Short" => parameterless(Int16Type, args),
            "String" => size_parameter(|size| StringType(size), args),
            "Struct" => Err(io::Error::new(io::ErrorKind::Other, "Struct is not yet implemented")),
            "Table" => Err(io::Error::new(io::ErrorKind::Other, "Table is not yet implemented")),
            "UUID" => parameterless(UUIDType, args),
            type_name => Err(io::Error::new(io::ErrorKind::Other, format!("unrecognized type {}", type_name)))
        }
    }

    pub fn to_column_type(&self) -> String {
        match self {
            BLOBType(size) => format!("BLOB({})", size),
            BooleanType => "Boolean".into(),
            CLOBType(size) => format!("CLOB({})", size),
            DateType => "Date".into(),
            EnumType(values) => format!("Enum({:?})", values),
            Int8Type => "Int8".into(),
            Int16Type => "Int16".into(),
            Int32Type => "Int32".into(),
            Int64Type => "Int64".into(),
            Float32Type => "Float32".into(),
            Float64Type => "Double".into(),
            RecordNumberType => "RecordNumber".into(),
            StringType(size) => format!("String({})", size),
            StructureType(columns) => format!("Struct({:?})", columns),
            TableType(columns) => format!("Table({:?})", columns),
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

    #[test]
    fn test_parse() {
        fn verify(type_decl: &str, data_type: DataType) {
            let dt: DataType = DataType::parse(type_decl)
                .expect("Failed to parse column type");
            assert_eq!(dt, data_type)
        }

        verify("BLOB(5566)", BLOBType(5566));
        verify("Boolean", BooleanType);
        verify("Byte", Int8Type);
        verify("CLOB(3377)", CLOBType(3377));
        verify("Date", DateType);
        verify("Double", Float64Type);
        verify("Enum(A,B,C)", EnumType(vec!["A".to_owned(), "B".to_owned(), "C".to_owned()]));
        verify("Float", Float32Type);
        verify("Short", Int16Type);
        verify("Int", Int32Type);
        verify("Long", Int64Type);
        verify("RecordNumber", RecordNumberType);
        verify("String(10)", StringType(10));
        verify("UUID", UUIDType);
    }
}