////////////////////////////////////////////////////////////////////
// tokens module
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

use crate::tokens::Token::*;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Token {
    // text, start, end, line_number, column_number
    AlphaNumeric(String, usize, usize, usize, usize),
    BackticksQuoted(String, usize, usize, usize, usize),
    DoubleQuoted(String, usize, usize, usize, usize),
    Numeric(String, usize, usize, usize, usize),
    Operator(String, usize, usize, usize, usize),
    SingleQuoted(String, usize, usize, usize, usize),
    Symbol(String, usize, usize, usize, usize),
}

impl Token {
    /// Returns the "raw" value of the [Token]
    pub fn get_raw_value(&self) -> String {
        let result = match &self {
            AlphaNumeric(s, ..) => s,
            BackticksQuoted(s, ..) => s,
            DoubleQuoted(s, ..) => s,
            Numeric(s, ..) => s,
            Operator(s, ..) => s,
            SingleQuoted(s, ..) => s,
            Symbol(s, ..) => s,
        };
        result.to_string()
    }

    /// Indicates whether the token is alphanumeric.
    pub fn is_alphanumeric(&self) -> bool {
        match self {
            Token::AlphaNumeric(..) => true,
            _ => false
        }
    }

    /// Indicates whether the token is an atom (alphanumeric or backticks-quoted).
    pub fn is_atom(&self) -> bool {
        self.is_alphanumeric() || self.is_string_backticks()
    }

    /// Indicates whether the token is a string; which could be backticks-quoted,
    /// double-quoted or single-quoted.
    pub fn is_string(&self) -> bool {
        self.is_string_double_quoted() || self.is_string_single_quoted()
    }

    /// Indicates whether the token is a backticks-quoted string.
    pub fn is_string_backticks(&self) -> bool {
        match self {
            Token::BackticksQuoted(..) => true,
            _ => false
        }
    }

    /// Indicates whether the token is a double-quoted string.
    pub fn is_string_double_quoted(&self) -> bool {
        match self {
            Token::DoubleQuoted(..) => true,
            _ => false
        }
    }

    /// Indicates whether the token is a single-quoted string.
    pub fn is_string_single_quoted(&self) -> bool {
        match self {
            Token::SingleQuoted(..) => true,
            _ => false
        }
    }

    /// Indicates whether the token is numeric.
    pub fn is_numeric(&self) -> bool {
        match self {
            Token::Numeric(..) => true,
            _ => false
        }
    }

    /// Indicates whether the token is an operator.
    pub fn is_operator(&self) -> bool {
        match self {
            Token::Operator(..) => true,
            _ => false
        }
    }

    /// Indicates whether the token is a symbol.
    pub fn is_symbol(&self) -> bool {
        match self {
            Token::Symbol(..) => true,
            _ => false
        }
    }

    pub fn is_type(&self, variant: &str) -> bool {
        match (self, variant) {
            (Token::AlphaNumeric(..), "AlphaNumeric")
            | (Token::BackticksQuoted(..), "BackticksQuoted")
            | (Token::DoubleQuoted(..), "DoubleQuoted")
            | (Token::Numeric(..), "Numeric")
            | (Token::Operator(..), "Operator")
            | (Token::SingleQuoted(..), "SingleQuoted")
            | (Token::Symbol(..), "Symbol") => true,
            _ => false,
        }
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::tokens::Token;

    use super::*;

    #[test]
    fn test_is_alphanumeric() {
        assert!(Token::AlphaNumeric("World".into(), 11, 16, 1, 13).is_alphanumeric());
    }

    #[test]
    fn test_is_atom() {
        assert!(Token::AlphaNumeric("x".into(), 11, 16, 1, 13).is_atom());
        assert!(Token::BackticksQuoted("x".into(), 11, 16, 1, 13).is_atom());
    }

    #[test]
    fn test_is_string_backticks() {
        assert!(Token::BackticksQuoted("the".into(), 0, 3, 1, 2).is_string_backticks());
    }

    #[test]
    fn test_is_numeric() {
        assert!(Token::Numeric("123".into(), 0, 3, 1, 2).is_numeric());
    }

    #[test]
    fn test_is_operator() {
        assert!(Token::Operator(".".into(), 0, 3, 1, 2).is_operator());
    }

    #[test]
    fn test_is_string() {
        assert!(Token::DoubleQuoted("the".into(), 0, 3, 1, 2).is_string());
        assert!(Token::SingleQuoted("little".into(), 0, 3, 1, 2).is_string());
        assert!(Token::DoubleQuoted("red".into(), 0, 3, 1, 2).is_string_double_quoted());
        assert!(Token::SingleQuoted("fox".into(), 0, 3, 1, 2).is_string_single_quoted());
    }

    #[test]
    fn test_is_symbol() {
        assert!(Token::Symbol(",".into(), 3, 4, 1, 5).is_symbol());
    }

    #[test]
    fn test_is_type() {
        assert!(Token::AlphaNumeric("World".into(), 11, 16, 1, 13).is_type("AlphaNumeric"));
        assert!(Token::BackticksQuoted("`World`".into(), 11, 16, 1, 13).is_type("BackticksQuoted"));
        assert!(Token::DoubleQuoted("\"World\"".into(), 11, 16, 1, 13).is_type("DoubleQuoted"));
        assert!(Token::Numeric("123".into(), 0, 3, 1, 2).is_type("Numeric"));
        assert!(Token::Operator(".".into(), 0, 3, 1, 2).is_type("Operator"));
        assert!(Token::SingleQuoted("'World'".into(), 11, 16, 1, 13).is_type("SingleQuoted"));
        assert!(Token::Symbol("÷".into(), 3, 4, 1, 5).is_type("Symbol"));
    }
}