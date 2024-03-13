////////////////////////////////////////////////////////////////////
// token slice module - responsible for matching/parsing language
////////////////////////////////////////////////////////////////////

use std::collections::HashMap;
use std::error::Error;
use std::ops::Index;

use serde::{Deserialize, Serialize};

use crate::tokenizer;
use crate::tokens::Token;
use crate::tokens::Token::*;

/// TokenSlice is a logic sequence of tokens.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TokenSlice {
    tokens: Vec<Token>,
    pos: usize,
}

impl Index<usize> for TokenSlice {
    type Output = Token;

    fn index(&self, index: usize) -> &Self::Output {
        &self.tokens[index]
    }
}

impl TokenSlice {
    /// Creates a new Token Slice via a vector of tokens.
    pub fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    /// Creates a new Token Slice from a string.
    pub fn from_string(text: &str) -> Self {
        Self::new(tokenizer::parse_fully(text))
    }

    pub fn capture(&mut self, start: &str, end: &str, delim: Option<&str>) -> Vec<&Token> {
        let inputs = &self.tokens;
        let mut pos = self.pos;
        let mut tokens = vec![];
        if inputs[pos].get_raw_value() == start {
            pos += 1;
            while (pos < inputs.len()) && inputs[pos].get_raw_value() != end {
                tokens.push(&inputs[pos]);
                pos += 1;
                match delim {
                    Some(_delim) if pos < inputs.len() && inputs[pos].get_raw_value() == _delim => pos += 1,
                    _ => {}
                }
            }
        }
        self.pos = pos;
        tokens
    }

    /// Returns the option of a Token at the current position within the slice.
    pub fn current(&self) -> Option<&Token> {
        if self.pos < self.tokens.len() {
            return Some(&self.tokens[self.pos]);
        }
        None
    }

    pub fn exists(&self, f: fn(&Token) -> bool) -> bool {
        match self.current() {
            Some(token) => f(token),
            None => false
        }
    }

    /// Extracts and returns values starting at the current position within the slice; which
    /// moves the cursor forward within the slice.
    pub fn extract(&self, template: impl Into<String>) -> HashMap<String, Token> {
        let m = HashMap::new();
        // TODO finish me
        m
    }

    /// Indicates whether at least one more token remains before the end of the slice.
    pub fn has_next(&self) -> bool { self.pos + 1 < self.tokens.len() }

    /// Indicates whether at least one more token remains before the beginning of the slice.
    pub fn has_previous(&self) -> bool { self.pos > 0 }

    pub fn is(&self, text: &str) -> bool {
        match self.current() {
            Some(tok) => tok.get_raw_value() == text,
            None => false
        }
    }

    /// Indicates whether the underlying tokens match the supplied template.
    pub fn matches(&self, template: impl Into<String>) -> bool {
        todo!()
    }

    /// Returns the option of a Token at the next position within the slice.
    pub fn next(&mut self) -> Option<&Token> {
        if self.has_next() {
            let n = self.pos + 1;
            self.pos = n;
            return Some(&self.tokens[n]);
        }
        None
    }

    /// Returns the option of a Token at the previous position within the slice.
    pub fn previous(&mut self) -> Option<&Token> {
        if self.has_previous() {
            let n = self.pos - 1;
            self.pos = n;
            return Some(&self.tokens[n]);
        }
        None
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_capture_with_delimiter() {
        let mut ts = TokenSlice::from_string("(123, 'Hello', abc)");
        let tokens = ts.capture("(", ")", Some(","));
        assert_eq!(tokens, vec![
            &Numeric("123".into(), 1, 4, 1, 3),
            &SingleQuoted("'Hello'".into(), 6, 13, 1, 8),
            &AlphaNumeric("abc".into(), 15, 18, 1, 17),
        ])
    }

    #[test]
    fn test_capture_without_delimiter() {
        let mut ts = TokenSlice::from_string("(123, 'Hello', abc)");
        let tokens = ts.capture("(", ")", None);
        assert_eq!(tokens, vec![
            &Numeric("123".into(), 1, 4, 1, 3),
            &Symbol(",".into(), 4, 5, 1, 6),
            &SingleQuoted("'Hello'".into(), 6, 13, 1, 8),
            &Symbol(",".into(), 13, 14, 1, 15),
            &AlphaNumeric("abc".into(), 15, 18, 1, 17),
        ])
    }

    #[test]
    fn test_current_next_and_previous() {
        let mut ts = TokenSlice::from_string("123, Hello World");
        assert_eq!(ts.current(), Some(&Numeric("123".into(), 0, 3, 1, 2)));
        assert_eq!(ts.next(), Some(&Symbol(",".into(), 3, 4, 1, 5)));
        assert_eq!(ts.next(), Some(&AlphaNumeric("Hello".into(), 5, 10, 1, 7)));
        assert_eq!(ts.next(), Some(&AlphaNumeric("World".into(), 11, 16, 1, 13)));
        assert_eq!(ts.next(), None);
        assert_eq!(ts.current(), Some(&AlphaNumeric("World".into(), 11, 16, 1, 13)));
        assert_eq!(ts.previous(), Some(&AlphaNumeric("Hello".into(), 5, 10, 1, 7)));
    }

    #[test]
    fn test_from_string() {
        let ts = TokenSlice::from_string("123 Hello World");
        assert_eq!(ts, TokenSlice {
            tokens: vec![
                Numeric("123".into(), 0, 3, 1, 2),
                AlphaNumeric("Hello".into(), 4, 9, 1, 6),
                AlphaNumeric("World".into(), 10, 15, 1, 12),
            ],
            pos: 0,
        })
    }

    #[test]
    fn test_indexing() {
        let ts = TokenSlice::from_string("the little brown fox");
        assert_eq!(ts[1], AlphaNumeric("little".into(), 4, 10, 1, 6))
    }

    #[test]
    pub fn test_template_extraction() {
        let mut ts = TokenSlice::from_string(r#"
        select symbol, exchange
        from stocks
        where lastSale < 1.0
        "#);
        let m = ts.extract(r#"
        select %E:fields ?from +?%q:source
        "#);
        println!("m {:?}", m)
    }
}