////////////////////////////////////////////////////////////////////
// ErrorMessage class
////////////////////////////////////////////////////////////////////

use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::tokens::Token;

/// Represents an Error Message
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Errors {
    ArgumentsMismatched(usize, usize),
    AssertionError(String, String),
    AsynchronousContextRequired,
    CollectionExpected(String),
    ColumnExpected(String),
    DateExpected(String),
    Exact(String),
    ExactNear(String, String),
    HashTableOverflow(usize, String),
    IllegalExpression(String),
    IllegalOperator(Token),
    InvalidNamespace(String),
    NotImplemented,
    OutcomeExpected(String),
    PackageNotFound(String),
    ParameterExpected(String),
    QueryableExpected(String),
    StringExpected(String),
    IndexOutOfRange(String, usize, usize),
    StructExpected(String, String),
    Syntax(String),
    TableExpected(String, String),
    TypeMismatch(String, String),
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
            AsynchronousContextRequired =>
                write!(f, "Asynchronous context required"),
            CollectionExpected(a) =>
                write!(f, "Iterable expected near {a}"),
            ColumnExpected(expr) =>
                write!(f, "Expected a column, got \"{}\" instead", expr),
            DateExpected(expr) =>
                write!(f, "Expected a timestamp, got \"{}\" instead", expr),
            Exact(message) =>
                write!(f, "{message}"),
            ExactNear(message, here) =>
                write!(f, "{message} near {here}"),
            HashTableOverflow(rid, value) =>
                write!(f, "Hash table overflow detected (rid: {rid}, key: {value})"),
            IllegalExpression(expr) =>
                write!(f, "Illegal expression: {}", expr),
            IllegalOperator(token) =>
                write!(f, "Illegal use of operator '{}'", token),
            InvalidNamespace(expr) =>
                write!(f, "Invalid namespace reference {}", expr),
            NotImplemented =>
                write!(f, "Not yet implemented"),
            OutcomeExpected(expr) =>
                write!(f, "Expected an outcome (Ack, RowId or RowsAffected) near {expr}"),
            PackageNotFound(name) =>
                write!(f, "Package '{name}' not found"),
            ParameterExpected(expr) =>
                write!(f, "Expected a parameter, got \"{}\" instead", expr),
            QueryableExpected(expr) =>
                write!(f, "Expected a queryable near {expr}"),
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
                write!(f, "Multiple errors detected:\n {}", errors.iter()
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