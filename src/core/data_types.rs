////////////////////////////////////////////////////////////////////
// data types module
////////////////////////////////////////////////////////////////////

use std::error::Error;
use std::fmt::Debug;

use serde::Serialize;

use crate::columns::Column;
use crate::data_types::DataType::*;
use crate::tokenizer::parse_fully;
use crate::tokens::Token;

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum DataType {
    BLOBType(usize),
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
    StructType(Vec<Column>),
    TableType(Vec<Column>),
    UUIDType,
}

impl DataType {
    /// computes and returns the maximum physical size of a value of this datatype
    pub fn compute_max_physical_size(&self) -> usize {
        use crate::data_types::DataType::*;
        let width: usize = match self {
            BLOBType(size) => *size,
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
            StructType(..) => 8,
            TableType(..) => 8,
            UUIDType => 16
        };
        width + 1 // +1 for field metadata
    }

    /// parses a datatype expression (e.g. "String(20)")
    pub fn parse(column_type: &str) -> Result<DataType, Box<dyn Error>> {
        let tokens: Vec<Token> = parse_fully(column_type)?;
        let token_slice: &[Token] = tokens.as_slice();
        match token_slice {
            // ex: Int
            [Token::AlphaNumeric(name, ..)] =>
                DataType::resolve(name, &[]),
            // ex: String(60)
            [Token::AlphaNumeric(name, ..),
            Token::Operator(op0, ..),
            Token::Numeric(arg, ..),
            Token::Operator(op1, ..)] if op0 == "(" && op1 == ")" =>
                DataType::resolve(name, &[arg]),
            // ex: Struct(symbol String(10), exchange String(10), last Double)
            [Token::AlphaNumeric(name, ..),
            Token::Operator(op0, ..), ..,
            Token::Operator(op1, ..)] if op0 == "(" && op1 == ")" => {
                let arg_tokens: &[Token] = &token_slice[1..(token_slice.len() - 1)];
                DataType::resolve(name, DataType::transfer_to_string_array(arg_tokens).as_slice())
            }
            // unrecognized - syntax error?
            _ => Err("malformed type definition".into())
        }
    }

    /// resolves a datatype by name
    pub fn resolve(name: &str, args: &[&str]) -> Result<DataType, Box<dyn Error>> {
        fn parameterless(data_type: DataType, args: &[&str]) -> Result<DataType, Box<dyn Error>> {
            if args.is_empty() { Ok(data_type) } else { Err("Parameters are not supported for this type".into()) }
        }

        fn size_parameter(f: fn(usize) -> DataType, args: &[&str]) -> Result<DataType, Box<dyn Error>> {
            if args.len() == 1 {
                Ok(f(args[0].parse::<usize>()?))
            } else { Err("a single parameter was expected for this type".into()) }
        }

        match name {
            "BLOB" => size_parameter(|size| BLOBType(size), args),
            "Byte" => parameterless(Int8Type, args),
            "CLOB" => size_parameter(|size| CLOBType(size), args),
            "Date" => parameterless(DateType, args),
            "Double" => parameterless(Float64Type, args),
            "Enum" => {
                let values = args.iter().map(|s| s.to_string()).collect();
                Ok(EnumType(values))
            }
            "Float" => parameterless(Float32Type, args),
            "Int" => parameterless(Int32Type, args),
            "Long" => parameterless(Int64Type, args),
            "RecordNumber" => parameterless(RecordNumberType, args),
            "Short" => parameterless(Int16Type, args),
            "String" => size_parameter(|size| StringType(size), args),
            "Struct" => Err("Struct is not yet implemented".into()),
            "Table" => Err("Table is not yet implemented".into()),
            "UUID" => parameterless(UUIDType, args),
            type_name => Err(("unrecognized type ".to_owned() + type_name).into())
        }
    }

    pub fn to_column_type(&self) -> String {
        match self {
            BLOBType(size) => format!("BLOB({})", size),
            CLOBType(size) => format!("CLOB({})", size),
            DateType => "Date".to_string(),
            EnumType(values) => format!("Enum({:?})", values),
            Int8Type => "Int8".to_string(),
            Int16Type => "Int16".to_string(),
            Int32Type => "Int32".to_string(),
            Int64Type => "Int64".to_string(),
            Float32Type => "Float32".to_string(),
            Float64Type => "Float64".to_string(),
            RecordNumberType => "RecordNumber".to_string(),
            StringType(size) => format!("String({})", size),
            StructType(columns) => format!("Struct({:?})", columns),
            TableType(columns) => format!("Table({:?})", columns),
            UUIDType => "UUID".to_string()
        }
    }

    fn transfer_to_string_array(token_slice: &[Token]) -> Vec<&str> {
        token_slice.into_iter()
            .fold(Vec::new(), |mut acc, t| {
                match t {
                    Token::Symbol(value, ..) if value == "," => acc,
                    Token::AlphaNumeric(value, ..) => {
                        acc.push(value);
                        acc
                    }
                    Token::Numeric(value, ..) => {
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
        verify("Byte", Int8Type);
        verify("CLOB(3377)", CLOBType(3377));
        verify("Date", DateType);
        verify("Double", Float64Type);
        verify("Enum(A,B,C)", EnumType(vec!["A".to_owned(), "B".to_owned(), "C".to_owned()]));
        verify("Float", Float32Type);
        verify("Short", Int16Type);
        verify("Int", Int32Type);
        verify("Long", Int64Type);
        verify("String(10)", StringType(10));
        verify("UUID", UUIDType);
    }
}