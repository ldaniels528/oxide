#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Token class
////////////////////////////////////////////////////////////////////

use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::tokens::Token::*;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Token {
    Atom { text: String, start: usize, end: usize, line_number: usize, column_number: usize },
    BinaryLiteral { text: String, start: usize, end: usize, line_number: usize, column_number: usize },
    Backticks { text: String, start: usize, end: usize, line_number: usize, column_number: usize },
    DataframeLiteral { cells: Vec<Vec<String>>, start: usize, end: usize, line_number: usize, column_number: usize },
    DateLiteral { text: String, start: usize, end: usize, line_number: usize, column_number: usize },
    DoubleQuoted { text: String, start: usize, end: usize, line_number: usize, column_number: usize },
    Numeric { text: String, start: usize, end: usize, line_number: usize, column_number: usize },
    Operator { text: String, start: usize, end: usize, line_number: usize, column_number: usize },
    SingleQuoted { text: String, start: usize, end: usize, line_number: usize, column_number: usize },
    URL { text: String, start: usize, end: usize, line_number: usize, column_number: usize },
}

impl Token {
    ////////////////////////////////////////////////////////////////
    // static methods
    ////////////////////////////////////////////////////////////////

    /// creates a new atom token
    pub fn atom(text: String, start: usize, end: usize, line_number: usize, column_number: usize) -> Token {
        Atom { text, start, end, line_number, column_number }
    }

    /// creates a new binary literal token
    pub fn binary_literal(text: String, start: usize, end: usize, line_number: usize, column_number: usize) -> Token {
        BinaryLiteral { text, start, end, line_number, column_number }
    }
    
    /// creates a new backticks-quoted token
    pub fn backticks(text: String, start: usize, end: usize, line_number: usize, column_number: usize) -> Token {
        Backticks { text, start, end, line_number, column_number }
    }

    /// creates a new date-literal token
    pub fn date_literal(text: String, start: usize, end: usize, line_number: usize, column_number: usize) -> Token {
        DateLiteral { text, start, end, line_number, column_number }
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
        Operator { text, start, end, line_number, column_number }
    }

    /// creates a new single-quoted token
    pub fn single_quoted(text: String, start: usize, end: usize, line_number: usize, column_number: usize) -> Token {
        SingleQuoted { text, start, end, line_number, column_number }
    }

    /// creates a new numeric token
    pub fn url(text: String, start: usize, end: usize, line_number: usize, column_number: usize) -> Token {
        URL { text, start, end, line_number, column_number }
    }

    ////////////////////////////////////////////////////////////////
    // instance methods
    ////////////////////////////////////////////////////////////////
    
    /// Returns the column number of the [Token]
    pub fn get_column_number(&self) -> usize {
        match self {
            Atom { column_number, .. }
            | Backticks { column_number, .. }
            | BinaryLiteral { column_number, .. }
            | DataframeLiteral { column_number, .. }
            | DateLiteral { column_number, .. }
            | DoubleQuoted { column_number, .. }
            | Numeric { column_number, .. }
            | Operator { column_number, .. }
            | SingleQuoted { column_number, .. }
            | URL { column_number, .. } => *column_number
        }
    }

    /// Returns the line number of the [Token]
    pub fn get_line_number(&self) -> usize {
        match self {
            Atom { line_number, .. }
            | Backticks { line_number, .. }
            | BinaryLiteral { line_number, .. }
            | DataframeLiteral { line_number, .. }
            | DateLiteral { line_number, .. }
            | DoubleQuoted { line_number, .. }
            | Numeric { line_number, .. }
            | Operator { line_number, .. }
            | SingleQuoted { line_number, .. }
            | URL { line_number, .. } => *line_number
        }
    }

    /// Returns the "raw" value of the [Token]
    pub fn get(&self) -> String {
        match self.clone() {
            Atom { text, .. }
            | Backticks { text, .. }
            | BinaryLiteral { text, .. }
            | DoubleQuoted { text, .. }
            | DateLiteral { text, .. }
            | Numeric { text, .. }
            | Operator { text, .. }
            | SingleQuoted { text, .. }
            | URL { text, .. } => text,
            DataframeLiteral { cells, .. } => { 
                let mut lines: Vec<String> = vec![];
                for row in cells {
                    let line = row.join("|");
                    lines.push(line);
                }
                lines.join("\n")
            }
        }
    }

    pub fn is(&self, text: &str) -> bool {
        text == self.get().as_str()
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

    pub fn is_type(&self, variant: &str) -> bool {
        match (self, variant) {
            (Atom { .. }, "Atom")
            | (Backticks { .. }, "BackticksQuoted")
            | (DoubleQuoted { .. }, "DoubleQuoted")
            | (Numeric { .. }, "Numeric")
            | (Operator { .. }, "Operator")
            | (SingleQuoted { .. }, "SingleQuoted")
            | (URL { .. }, "URL") => true,
            _ => false,
        }
    }
}

impl Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.get())
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
    fn test_is_string() {
        assert!(Token::double_quoted("the".into(), 0, 3, 1, 2).is_string());
        assert!(Token::single_quoted("little".into(), 0, 3, 1, 2).is_string());
        assert!(Token::double_quoted("red".into(), 0, 3, 1, 2).is_double_quoted());
        assert!(Token::single_quoted("fox".into(), 0, 3, 1, 2).is_single_quoted());
    }

    #[test]
    fn test_is_type() {
        assert!(Token::atom("World".into(), 11, 16, 1, 13).is_type("Atom"));
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