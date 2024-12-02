////////////////////////////////////////////////////////////////////
// Errors class
////////////////////////////////////////////////////////////////////

use std::fmt::{Display, Formatter};

use crate::tokens::Token;
use serde::{Deserialize, Serialize};
use crate::data_types::DataType;

/// Represents an Error Message
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Errors {
    ArgumentsMismatched(usize, usize),
    AssertionError(String, String),
    CannotSubtract(String, String),
    CollectionExpected(String),
    ColumnExpected(String),
    ConversionError(String),
    DateExpected(String),
    Exact(String),
    ExactNear(String, String),
    FunctionArgsExpected(String),
    HashTableOverflow(usize, String),
    IllegalExpression(String),
    IllegalOperator(Token),
    InvalidNamespace(String),
    NotImplemented,
    OutcomeExpected(String),
    PackageNotFound(String),
    ParameterExpected(String),
    QueryableExpected(String),
    RowsAffectedExpected(String),
    StringExpected(String),
    IndexOutOfRange(String, usize, usize),
    StructExpected(String, String),
    Syntax(String),
    TableExpected(String, String),
    TypeMismatch(DataType, DataType),
    Various(Vec<Errors>),
    ViewsCannotBeResized,
    WriteProtected,
}

impl Display for Errors {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use Errors::*;
        match self {
            ArgumentsMismatched(a, b) =>
                write!(f, "Mismatched number of arguments: {a} vs. {b}"),
            AssertionError(a, b) =>
                write!(f, "Assertion Error: {a} was not {b}"),
            CannotSubtract(a, b) =>
                write!(f, "Cannot subtract {b} from {a}"),
            CollectionExpected(a) =>
                write!(f, "Iterable expected near {a}"),
            ColumnExpected(expr) =>
                write!(f, "Expected a column, got \"{expr}\" instead"),
            ConversionError(expr) =>
                write!(f, "Conversion error: {:?}", expr),
            DateExpected(expr) =>
                write!(f, "Expected a timestamp, got \"{expr}\" instead"),
            Exact(message) =>
                write!(f, "{message}"),
            ExactNear(message, here) =>
                write!(f, "{message} near {here}"),
            FunctionArgsExpected(other) =>
                write!(f, "Function arguments expected, but got {}", other),
            HashTableOverflow(rid, value) =>
                write!(f, "Hash table overflow detected (rid: {rid}, key: {value})"),
            IllegalExpression(expr) =>
                write!(f, "Illegal expression: {expr}"),
            IllegalOperator(token) =>
                write!(f, "Illegal use of operator '{token}'"),
            InvalidNamespace(expr) =>
                write!(f, "Invalid namespace reference {expr}"),
            NotImplemented =>
                write!(f, "Not yet implemented"),
            OutcomeExpected(expr) =>
                write!(f, "Expected an outcome (Ack, RowId or RowsAffected) near {expr}"),
            PackageNotFound(name) =>
                write!(f, "Package '{name}' not found"),
            ParameterExpected(expr) =>
                write!(f, "Expected a parameter, got \"{expr}\" instead"),
            QueryableExpected(expr) =>
                write!(f, "Expected a queryable near {expr}"),
            RowsAffectedExpected(expr) =>
                write!(f, "Expected RowsAffected(..) near {expr}"),
            StringExpected(expr) =>
                write!(f, "Expected a string near {expr}"),
            IndexOutOfRange(name, idx, len) =>
                write!(f, "{name} index is out of range ({idx} >= {len})"),
            StructExpected(name, expr) =>
                write!(f, "{name} is not a structure ({expr})"),
            Syntax(message) =>
                write!(f, "Syntax error: {message}"),
            TableExpected(a, b) =>
                write!(f, "{a} is not a table ({b})"),
            TypeMismatch(a, b) =>
                write!(f, "TypeMismatch: {a} is not convertible to {b}"),
            Various(errors) =>
                write!(f, "Multiple errors detected:\n{}", errors.iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join("\n")),
            ViewsCannotBeResized =>
                write!(f, "Views cannot be resized"),
            WriteProtected =>
                write!(f, "Write operations are not allowed"),
        }
    }
}

/// Unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use Errors::*;
    use crate::data_types::DataType::{NumberType, StructureType};
    use crate::number_kind::NumberKind::I128Kind;

    #[test]
    fn test_errors() {
        verify(ArgumentsMismatched(2, 1), "Mismatched number of arguments: 2 vs. 1");
        verify(AssertionError("true".into(), "false".into()), "Assertion Error: true was not false");
        verify(CannotSubtract("a".into(), "b".into()), "Cannot subtract b from a");
        verify(CollectionExpected("1".into()), "Iterable expected near 1");
        verify(ColumnExpected("^".into()), "Expected a column, got \"^\" instead");
        verify(ConversionError("i32".into()), "Conversion error: \"i32\"");
        verify(DateExpected("Tom".into()), "Expected a timestamp, got \"Tom\" instead");
        verify(Exact("Something bad happened".into()), "Something bad happened");
        verify(ExactNear("Something bad happened".into(), "here".into()), "Something bad happened near here");
        verify(FunctionArgsExpected("Tom".into()), "Function arguments expected, but got Tom");
        verify(HashTableOverflow(100, "AAA".into()), "Hash table overflow detected (rid: 100, key: AAA)");
        verify(IllegalExpression("2 ~ 3".into()), "Illegal expression: 2 ~ 3");
        verify(IllegalOperator(Token::Atom {
            text: "+".to_string(),
            start: 12,
            end: 14,
            line_number: 1,
            column_number: 18,
        }), "Illegal use of operator '+'");
        verify(InvalidNamespace("a.b.c".into()), "Invalid namespace reference a.b.c");
        verify(NotImplemented, "Not yet implemented");
        verify(OutcomeExpected("wth".into()), "Expected an outcome (Ack, RowId or RowsAffected) near wth");
        verify(PackageNotFound("wth".into()), "Package 'wth' not found");
        verify(ParameterExpected("@".into()), "Expected a parameter, got \"@\" instead");
        verify(QueryableExpected("reset".into()), "Expected a queryable near reset");
        verify(RowsAffectedExpected("xyz".into()), "Expected RowsAffected(..) near xyz");
        verify(StringExpected("reset".into()), "Expected a string near reset");
        verify(IndexOutOfRange("bytes".into(), 5, 4), "bytes index is out of range (5 >= 4)");
        verify(StructExpected("count".into(), "i64".into()), "count is not a structure (i64)");
        verify(Syntax("cannot do it".into()), "Syntax error: cannot do it");
        verify(TableExpected("stocks".into(), "Date".into()), "stocks is not a table (Date)");
        verify(TypeMismatch(StructureType(vec![]), NumberType(I128Kind)), "TypeMismatch: struct() is not convertible to i128");
        verify(
            Various(vec![ViewsCannotBeResized, WriteProtected]),
            "Multiple errors detected:\nViews cannot be resized\nWrite operations are not allowed");
        verify(ViewsCannotBeResized, "Views cannot be resized");
        verify(WriteProtected, "Write operations are not allowed");
    }

    fn verify(error: Errors, message: &str) {
        assert_eq!(error.to_string().as_str(), message)
    }
}