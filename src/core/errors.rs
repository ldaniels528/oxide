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
    CodeBlockExpected(String),
    CollectionExpected(String),
    ColumnExpected(String),
    DateExpected(String),
    FunctionArgsExpected(String),
    IdentifierExpected(String),
    OutcomeExpected(String),
    ParameterExpected(String),
    QueryableExpected(String),
    RowsAffectedExpected(String),
    StringExpected(String),
    StructExpected(String, String),
    TableExpected(String, String),
    UnsupportedType(DataType, DataType),
    VariableExpected(Token),
}

impl Display for TypeMismatchErrors {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use TypeMismatchErrors::*;
        let header = "Type Mismatch: ";
        match self {
            ArgumentsMismatched(a, b) =>
                write!(f, "{header}mismatched number of arguments: {a} vs. {b}"),
            BooleanExpected(a) =>
                write!(f, "{header}Boolean value expected near {a}"),
            CodeBlockExpected(a) =>
                write!(f, "{header}code block expected near {a}"),
            CollectionExpected(a) =>
                write!(f, "{header}Iterable expected near {a}"),
            ColumnExpected(expr) =>
                write!(f, "{header}Expected a column, got \"{expr}\" instead"),
            DateExpected(expr) =>
                write!(f, "Expected a timestamp, got \"{expr}\" instead"),
            FunctionArgsExpected(other) =>
                write!(f, "{header}Function arguments expected, but got {}", other),
            IdentifierExpected(other) =>
                write!(f, "{header}Identifier expected, but got {}", other),
            OutcomeExpected(expr) =>
                write!(f, "{header}Expected an outcome (Ack, RowId or RowsAffected) near {expr}"),
            ParameterExpected(expr) =>
                write!(f, "{header}Expected a parameter, got \"{expr}\" instead"),
            QueryableExpected(expr) =>
                write!(f, "{header}Expected a queryable near {expr}"),
            RowsAffectedExpected(expr) =>
                write!(f, "{header}Expected RowsAffected(..) near {expr}"),
            StringExpected(expr) =>
                write!(f, "{header}Expected a string near {expr}"),
            StructExpected(name, expr) =>
                write!(f, "{header}{name} is not a structure ({expr})"),
            TableExpected(a, b) =>
                write!(f, "{header}{a} is not a table ({b})"),
            UnsupportedType(a, b) =>
                write!(f, "{header}{a} is not convertible to {b}"),
            VariableExpected(a) =>
                write!(f, "{header}Variable identifier expected near {a}"),
        }
    }
}

/// Represents an Error Message
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Errors {
    AssertionError(String, String),
    CannotSubtract(String, String),
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
        use Errors::*;
        match self {
            AssertionError(a, b) =>
                write!(f, "Assertion Error: {a} was not {b}"),
            CannotSubtract(a, b) =>
                write!(f, "Cannot subtract {b} from {a}"),
            PlatformOpError(op) =>
                write!(f, "Conversion error: \"{}\"", op.to_code()),
            Exact(message) =>
                write!(f, "{message}"),
            ExactNear(message, token) =>
                write!(f, "{message} on line {} column {}",
                       token.get_line_number(), token.get_column_number()),
            HashTableOverflow(rid, value) =>
                write!(f, "Hash table overflow detected (rid: {rid}, key: {value})"),
            IllegalExpression(expr) =>
                write!(f, "Illegal expression: {expr}"),
            IllegalOperator(token) =>
                write!(f, "Illegal use of operator '{token}'"),
            InvalidNamespace(expr) =>
                write!(f, "Invalid namespace reference {expr}"),
            NotImplemented(expr) =>
                write!(f, "Not yet implemented - {expr}"),
            PackageNotFound(name) =>
                write!(f, "Package '{name}' not found"),
            IndexOutOfRange(name, idx, len) =>
                write!(f, "{name} index is out of range ({idx} >= {len})"),
            Syntax(message) =>
                write!(f, "Syntax error: {message}"),
            TypeMismatch(mismatch) =>
                write!(f, "{}", mismatch),
            UnsupportedPlatformOps(pops) =>
                write!(f, "Unsupported operation {}", pops.to_code()),
            Multiple(errors) =>
                write!(f, "Multiple errors detected:\n{}", errors.iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join("\n")),
            Errors::ViewsCannotBeResized =>
                write!(f, "Views cannot be resized"),
            Errors::WriteProtected =>
                write!(f, "Write operations are not allowed"),
        }
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
               "Type Mismatch: mismatched number of arguments: 2 vs. 1");
        verify(TypeMismatch(BooleanExpected("457".into())),
               "Type Mismatch: Boolean value expected near 457");
        verify(TypeMismatch(CodeBlockExpected("[]".into())),
               "Type Mismatch: code block expected near []");
        verify(TypeMismatch(CollectionExpected("1".into())),
               "Type Mismatch: Iterable expected near 1");
        verify(TypeMismatch(ColumnExpected("^".into())),
               "Type Mismatch: Expected a column, got \"^\" instead");
        verify(TypeMismatch(DateExpected("Tom".into())),
               "Expected a timestamp, got \"Tom\" instead");
        verify(TypeMismatch(FunctionArgsExpected("Tom".into())),
               "Type Mismatch: Function arguments expected, but got Tom");
        verify(TypeMismatch(OutcomeExpected("wth".into())),
               "Type Mismatch: Expected an outcome (Ack, RowId or RowsAffected) near wth");
        verify(TypeMismatch(ParameterExpected("@".into())),
               "Type Mismatch: Expected a parameter, got \"@\" instead");
        verify(TypeMismatch(QueryableExpected("reset".into())),
               "Type Mismatch: Expected a queryable near reset");
        verify(TypeMismatch(RowsAffectedExpected("xyz".into())),
               "Type Mismatch: Expected RowsAffected(..) near xyz");
        verify(TypeMismatch(StringExpected("reset".into())),
               "Type Mismatch: Expected a string near reset");
        verify(TypeMismatch(StructExpected("count".into(), "i64".into())),
               "Type Mismatch: count is not a structure (i64)");
        verify(TypeMismatch(TableExpected("stocks".into(), "Date".into())),
               "Type Mismatch: stocks is not a table (Date)");
        verify(TypeMismatch(UnsupportedType(StructureType(vec![]), NumberType(I128Kind))),
               "Type Mismatch: Struct() is not convertible to i128");
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