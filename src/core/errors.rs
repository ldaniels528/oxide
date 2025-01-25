#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Errors class
////////////////////////////////////////////////////////////////////

use std::fmt::{Debug, Display, Formatter};

use crate::data_types::DataType;
use crate::platform::PlatformOps;
use crate::tokens::Token;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum TypeMismatchErrors {
    ArgumentsMismatched(usize, usize),
    BooleanExpected(String),
    CannotBeNegated(String),
    CodeBlockExpected(String),
    CollectionExpected(String),
    ColumnExpected(String),
    ConstantValueExpected(String),
    DateExpected(String),
    FunctionArgsExpected(String),
    IdentifierExpected(String),
    OutcomeExpected(String),
    ParameterExpected(String),
    QueryableExpected(String),
    RowsAffectedExpected(String),
    StringExpected(String),
    StructExpected(String, String),
    StructsOneOrMoreExpected,
    TableExpected(String, String),
    UnexpectedResult(String),
    UnrecognizedTypeName(String),
    UnsupportedType(DataType, DataType),
    VariableExpected(Token),
}

impl Display for TypeMismatchErrors {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let text = match self {
            TypeMismatchErrors::ArgumentsMismatched(a, b) =>
                format!("Mismatched number of arguments: {a} vs. {b}"),
            TypeMismatchErrors::BooleanExpected(a) =>
                format!("Boolean value expected near {a}"),
            TypeMismatchErrors::CannotBeNegated(a) =>
                format!("{a} cannot be negated"),
            TypeMismatchErrors::CodeBlockExpected(a) =>
                format!("Scope {{ ... }} expected near {a}"),
            TypeMismatchErrors::CollectionExpected(a) =>
                format!("Iterable expected near {a}"),
            TypeMismatchErrors::ColumnExpected(expr) =>
                format!("Expected a column, but found \"{expr}\" instead"),
            TypeMismatchErrors::ConstantValueExpected(value) =>
                format!("Expected a constant value, but found \"{value}\" instead"),
            TypeMismatchErrors::DateExpected(expr) =>
                format!("Expected a timestamp, but found \"{expr}\" instead"),
            TypeMismatchErrors::FunctionArgsExpected(other) =>
                format!("Function arguments expected, but found {}", other),
            TypeMismatchErrors::IdentifierExpected(other) =>
                format!("Identifier expected, but got {}", other),
            TypeMismatchErrors::OutcomeExpected(expr) =>
                format!("Expected an outcome (Ack, RowId or RowsAffected) near {expr}"),
            TypeMismatchErrors::ParameterExpected(expr) =>
                format!("Expected a parameter, got \"{expr}\" instead"),
            TypeMismatchErrors::QueryableExpected(expr) =>
                format!("Expected a queryable near {expr}"),
            TypeMismatchErrors::RowsAffectedExpected(expr) =>
                format!("Expected RowsAffected(..) near {expr}"),
            TypeMismatchErrors::StringExpected(expr) =>
                format!("Expected a String near {expr}"),
            TypeMismatchErrors::StructExpected(name, expr) =>
                format!("{name} is not a Struct ({expr})"),
            TypeMismatchErrors::StructsOneOrMoreExpected =>
                String::from("At least one Struct is required."),
            TypeMismatchErrors::TableExpected(a, b) =>
                format!("{a} is not a Table ({b})"),
            TypeMismatchErrors::UnexpectedResult(result) =>
                format!("Unexpected result near {result}"),
            TypeMismatchErrors::UnrecognizedTypeName(name) =>
                format!("Unrecognized type '{name}'"),
            TypeMismatchErrors::UnsupportedType(a, b) =>
                format!("{a} is not convertible to {b}"),
            TypeMismatchErrors::VariableExpected(a) =>
                format!("Variable identifier expected near {a}"),
        };
        write!(f, "Type Mismatch: {text}")
    }
}

/// Represents an Error Message
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Errors {
    AssertionError(String, String),
    CannotSubtract(String, String),
    Empty,
    Exact(String),
    ExactNear(String, Token),
    HashTableOverflow(usize, String),
    IllegalExpression(String),
    IllegalOperator(Token),
    IndexOutOfRange(String, usize, usize),
    InvalidNamespace(String),
    Multiple(Vec<Errors>),
    NotImplemented(String),
    PackageNotFound(String),
    PlatformOpError(PlatformOps),
    Syntax(String),
    TypeMismatch(TypeMismatchErrors),
    UnsupportedPlatformOps(PlatformOps),
    ViewsCannotBeResized,
    WriteProtected,
}

impl Display for Errors {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let text = match self {
            Errors::AssertionError(a, b) =>
                format!("Assertion Error: {a} was not {b}"),
            Errors::CannotSubtract(a, b) =>
                format!("Cannot subtract {b} from {a}"),
            Errors::Empty => String::from("Doh."),
            Errors::Exact(message) => format!("{message}"),
            Errors::ExactNear(message, token) =>
                format!("{message} on line {} column {}",
                        token.get_line_number(), token.get_column_number()),
            Errors::HashTableOverflow(rid, value) =>
                format!("Hash table overflow detected (rid: {rid}, key: {value})"),
            Errors::IndexOutOfRange(name, idx, len) =>
                format!("{name} index is out of range ({idx} >= {len})"),
            Errors::IllegalExpression(expr) =>
                format!("Illegal expression: {expr}"),
            Errors::IllegalOperator(token) =>
                format!("Illegal use of operator '{token}'"),
            Errors::InvalidNamespace(expr) =>
                format!("Invalid namespace reference {expr}"),
            Errors::Multiple(errors) =>
                format!("Multiple errors detected:\n{}", errors.iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join("\n")),
            Errors::NotImplemented(expr) =>
                format!("Not yet implemented - {expr}"),
            Errors::PackageNotFound(name) =>
                format!("Package '{name}' not found"),
            Errors::PlatformOpError(op) =>
                format!("Conversion error: \"{}\"", op.to_code()),
            Errors::Syntax(message) =>
                format!("Syntax error: {message}"),
            Errors::TypeMismatch(mismatch) =>
                format!("{}", mismatch),
            Errors::UnsupportedPlatformOps(pops) =>
                format!("Unsupported operation {}", pops.to_code()),
            Errors::ViewsCannotBeResized =>
                String::from("Views cannot be resized"),
            Errors::WriteProtected =>
                String::from("Write operations are not allowed"),
        };
        write!(f, "{text}")
    }
}

pub fn throw<A>(error: Errors) -> std::io::Result<A> {
    Err(std::io::Error::new(std::io::ErrorKind::Other, error.to_string()))
}

/// Unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_types::DataType::{NumberType, StructureType};
    use crate::errors::TypeMismatchErrors::*;
    use crate::number_kind::NumberKind::I128Kind;
    use Errors::*;

    #[test]
    fn test_general_errors() {
        verify(AssertionError("true".into(), "false".into()),
               "Assertion Error: true was not false");
        verify(CannotSubtract("a".into(), "b".into()),
               "Cannot subtract b from a");
        verify(PlatformOpError(PlatformOps::UtilHex),
               "Conversion error: \"util::hex(x)\"");
        verify(Exact("Something bad happened".into()),
               "Something bad happened");
        verify(ExactNear(
            "Something bad happened".into(),
            Token::operator(".".into(), 145, 146, 13, 5)),
               "Something bad happened on line 13 column 5");
        verify(HashTableOverflow(100, "AAA".into()),
               "Hash table overflow detected (rid: 100, key: AAA)");
        verify(IllegalExpression("2 ~ 3".into()),
               "Illegal expression: 2 ~ 3");
        verify(IllegalOperator(Token::Atom {
            text: "+".to_string(),
            start: 12,
            end: 14,
            line_number: 1,
            column_number: 18,
        }), "Illegal use of operator '+'");
        verify(InvalidNamespace("a.b.c".into()), "Invalid namespace reference a.b.c");
        verify(NotImplemented("magic()".into()), "Not yet implemented - magic()");
        verify(PackageNotFound("wth".into()), "Package 'wth' not found");
        verify(IndexOutOfRange("bytes".into(), 5, 4),
               "bytes index is out of range (5 >= 4)");
        verify(Syntax("cannot do it".into()), "Syntax error: cannot do it");
    }

    #[test]
    fn test_type_mismatch_errors() {
        verify(TypeMismatch(ArgumentsMismatched(2, 1)),
               "Type Mismatch: Mismatched number of arguments: 2 vs. 1");
        verify(TypeMismatch(BooleanExpected("457".into())),
               "Type Mismatch: Boolean value expected near 457");
        verify(TypeMismatch(CodeBlockExpected("[]".into())),
               "Type Mismatch: Scope { ... } expected near []");
        verify(TypeMismatch(CollectionExpected("1".into())),
               "Type Mismatch: Iterable expected near 1");
        verify(TypeMismatch(ColumnExpected("^".into())),
               "Type Mismatch: Expected a column, but found \"^\" instead");
        verify(TypeMismatch(DateExpected("Tom".into())),
               r#"Type Mismatch: Expected a timestamp, but found "Tom" instead"#);
        verify(TypeMismatch(FunctionArgsExpected("Tom".into())),
               "Type Mismatch: Function arguments expected, but found Tom");
        verify(TypeMismatch(OutcomeExpected("wth".into())),
               "Type Mismatch: Expected an outcome (Ack, RowId or RowsAffected) near wth");
        verify(TypeMismatch(ParameterExpected("@".into())),
               "Type Mismatch: Expected a parameter, got \"@\" instead");
        verify(TypeMismatch(QueryableExpected("reset".into())),
               "Type Mismatch: Expected a queryable near reset");
        verify(TypeMismatch(RowsAffectedExpected("xyz".into())),
               "Type Mismatch: Expected RowsAffected(..) near xyz");
        verify(TypeMismatch(StringExpected("reset".into())),
               "Type Mismatch: Expected a String near reset");
        verify(TypeMismatch(StructExpected("count".into(), "i64".into())),
               "Type Mismatch: count is not a Struct (i64)");
        verify(TypeMismatch(TableExpected("stocks".into(), "Date".into())),
               "Type Mismatch: stocks is not a Table (Date)");
        verify(TypeMismatch(UnsupportedType(StructureType(vec![]), NumberType(I128Kind))),
               "Type Mismatch: Struct is not convertible to i128");
        verify(
            Multiple(vec![ViewsCannotBeResized, WriteProtected]),
            "Multiple errors detected:\nViews cannot be resized\nWrite operations are not allowed");
        verify(ViewsCannotBeResized, "Views cannot be resized");
        verify(WriteProtected, "Write operations are not allowed");
    }

    fn verify(error: Errors, message: &str) {
        assert_eq!(error.to_string().as_str(), message)
    }
}