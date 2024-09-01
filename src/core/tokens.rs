////////////////////////////////////////////////////////////////////
// tokens module
////////////////////////////////////////////////////////////////////

use std::fmt::Display;
use std::io;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use shared_lib::fail;

use crate::tokens::Token::*;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Token {
    Atom { text: String, start: usize, end: usize, line_number: usize, column_number: usize },
    Backticks { text: String, start: usize, end: usize, line_number: usize, column_number: usize },
    DoubleQuoted { text: String, start: usize, end: usize, line_number: usize, column_number: usize },
    Numeric { text: String, start: usize, end: usize, line_number: usize, column_number: usize },
    Operator { text: String, start: usize, end: usize, line_number: usize, column_number: usize, precedence: usize, is_postfix: bool, is_barrier: bool },
    SingleQuoted { text: String, start: usize, end: usize, line_number: usize, column_number: usize },
}

impl Token {
    ////////////////////////////////////////////////////////////////
    // static methods
    ////////////////////////////////////////////////////////////////

    /// creates a new atom token
    pub fn atom(text: String, start: usize, end: usize, line_number: usize, column_number: usize) -> Token {
        Atom { text, start, end, line_number, column_number }
    }

    /// creates a new backticks-quoted token
    pub fn backticks(text: String, start: usize, end: usize, line_number: usize, column_number: usize) -> Token {
        Backticks { text, start, end, line_number, column_number }
    }

    /// creates a new double-quoted token
    pub fn double_quoted(text: String, start: usize, end: usize, line_number: usize, column_number: usize) -> Token {
        DoubleQuoted { text, start, end, line_number, column_number }
    }

    /// creates a new numeric token
    pub fn numeric(text: String, start: usize, end: usize, line_number: usize, column_number: usize) -> Token {
        Numeric { text, start, end, line_number, column_number }
    }

    /// creates a new operator token
    pub fn operator(text: String, start: usize, end: usize, line_number: usize, column_number: usize) -> Token {
        let barrier_symbols = vec![
            ",", ";", "[", "]", "(", ")", "{", "}", "=>",
        ].iter().map(|s| s.to_string()).collect::<Vec<String>>();
        let precedence = Self::determine_precedence(text.as_str());
        let is_postfix = text == "¡" || text == "²" || text == "³";
        let is_barrier = barrier_symbols.contains(&text.to_owned());
        Operator { text, start, end, line_number, column_number, precedence, is_postfix, is_barrier }
    }

    /// creates a new single-quoted token
    pub fn single_quoted(text: String, start: usize, end: usize, line_number: usize, column_number: usize) -> Token {
        SingleQuoted { text, start, end, line_number, column_number }
    }

    pub fn determine_precedence(symbol: impl Into<String>) -> usize {
        match symbol.into().as_str() {
            "¡" | "**" | "²" | "³" => 3,
            "×" | "*" | "÷" | "/" | "%" | ">>" | "<<" => 2,
            "+" | "-" | "|" | "&" | "^" => 1,
            _ => 0,
        }
    }

    ////////////////////////////////////////////////////////////////
    // instance methods
    ////////////////////////////////////////////////////////////////

    pub fn contains(&self, text: &str) -> bool {
        let contents = self.get_raw_value();
        text == contents.as_str() || contents.contains(text)
    }

    /// Returns the column number of the [Token]
    pub fn get_column_number(&self) -> usize {
        match &self {
            Atom { column_number, .. }
            | Backticks { column_number, .. }
            | DoubleQuoted { column_number, .. }
            | Numeric { column_number, .. }
            | Operator { column_number, .. }
            | SingleQuoted { column_number, .. } => *column_number
        }
    }

    /// Returns the line number of the [Token]
    pub fn get_line_number(&self) -> usize {
        match &self {
            Atom { line_number, .. }
            | Backticks { line_number, .. }
            | DoubleQuoted { line_number, .. }
            | Numeric { line_number, .. }
            | Operator { line_number, .. }
            | SingleQuoted { line_number, .. } => *line_number
        }
    }

    /// Returns the "raw" value of the [Token]
    pub fn get_raw_value(&self) -> String {
        (match &self {
            Atom { text, .. }
            | Backticks { text, .. }
            | DoubleQuoted { text, .. }
            | Numeric { text, .. }
            | Operator { text, .. }
            | SingleQuoted { text, .. } => text
        }).to_string()
    }

    /// Indicates whether the token is alphanumeric.
    pub fn is_alphanumeric(&self) -> bool {
        match self {
            Atom { .. } => true,
            _ => false
        }
    }

    /// Indicates whether the token is an atom (alphanumeric or backticks-quoted).
    pub fn is_atom(&self) -> bool {
        self.is_alphanumeric() || self.is_backticks()
    }

    /// Indicates whether the token is a string; which could be backticks-quoted,
    /// double-quoted or single-quoted.
    pub fn is_string(&self) -> bool {
        self.is_double_quoted() || self.is_single_quoted()
    }

    /// Indicates whether the token is a backticks-quoted string.
    pub fn is_backticks(&self) -> bool {
        match self {
            Backticks { .. } => true,
            _ => false
        }
    }

    /// Indicates whether the token is a double-quoted string.
    pub fn is_double_quoted(&self) -> bool {
        match self {
            DoubleQuoted { .. } => true,
            _ => false
        }
    }

    /// Indicates whether the token is a single-quoted string.
    pub fn is_single_quoted(&self) -> bool {
        match self {
            SingleQuoted { .. } => true,
            _ => false
        }
    }

    /// Indicates whether the token is numeric.
    pub fn is_numeric(&self) -> bool {
        match self {
            Numeric { .. } => true,
            _ => false
        }
    }

    /// Indicates whether the token is an operator.
    pub fn is_operator(&self) -> bool {
        match self {
            Operator { .. } => true,
            _ => false
        }
    }

    pub fn is_type(&self, variant: &str) -> bool {
        match (self, variant) {
            (Atom { .. }, "AlphaNumeric")
            | (Backticks { .. }, "BackticksQuoted")
            | (DoubleQuoted { .. }, "DoubleQuoted")
            | (Numeric { .. }, "Numeric")
            | (Operator { .. }, "Operator")
            | (SingleQuoted { .. }, "SingleQuoted") => true,
            _ => false,
        }
    }

    /// Returns the numeric value of the [Token]
    pub fn to_numeric<A: FromStr>(&self) -> io::Result<A> {
        match self {
            Atom { text, .. } | Numeric { text, .. } => {
                let s: String = text.chars().filter(|c| *c != '_').collect();
                match s.parse::<A>() {
                    Ok(number) => Ok(number),
                    Err(_) => fail(format!("Cannot convert '{}' to numeric", s))
                }
            }
            t => fail(format!("Cannot convert {} to numeric", t.get_raw_value()))
        }
    }
}

impl Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.get_raw_value())
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::tokenizer::parse_fully;
    use crate::tokens::Token;

    #[test]
    fn test_is_alphanumeric() {
        assert!(Token::atom("World".into(), 11, 16, 1, 13).is_alphanumeric());
    }

    #[test]
    fn test_is_atom() {
        assert!(Token::atom("x".into(), 11, 16, 1, 13).is_atom());
        assert!(Token::backticks("x".into(), 11, 16, 1, 13).is_atom());
    }

    #[test]
    fn test_is_string_backticks() {
        assert!(Token::backticks("the".into(), 0, 3, 1, 2).is_backticks());
    }

    #[test]
    fn test_is_numeric() {
        assert!(Token::numeric("123".into(), 0, 3, 1, 2).is_numeric());
    }

    #[test]
    fn test_is_operator() {
        assert!(Token::operator(".".into(), 0, 3, 1, 2).is_operator());
    }

    #[test]
    fn test_is_string() {
        assert!(Token::double_quoted("the".into(), 0, 3, 1, 2).is_string());
        assert!(Token::single_quoted("little".into(), 0, 3, 1, 2).is_string());
        assert!(Token::double_quoted("red".into(), 0, 3, 1, 2).is_double_quoted());
        assert!(Token::single_quoted("fox".into(), 0, 3, 1, 2).is_single_quoted());
    }

    #[test]
    fn test_is_type() {
        assert!(Token::atom("World".into(), 11, 16, 1, 13).is_type("AlphaNumeric"));
        assert!(Token::backticks("`World`".into(), 11, 16, 1, 13).is_type("BackticksQuoted"));
        assert!(Token::double_quoted("\"World\"".into(), 11, 16, 1, 13).is_type("DoubleQuoted"));
        assert!(Token::numeric("123".into(), 0, 3, 1, 2).is_type("Numeric"));
        assert!(Token::operator(".".into(), 0, 3, 1, 2).is_type("Operator"));
        assert!(Token::single_quoted("'World'".into(), 11, 16, 1, 13).is_type("SingleQuoted"));
    }

    #[test]
    fn test_precedence() {
        let tokens = parse_fully("3 + 4 * (2 - 1)^2");
        let mut n = 1;
        for token in tokens {
            println!("{} tok {:?}", n, token);
            n += 1;
        }
    }
}