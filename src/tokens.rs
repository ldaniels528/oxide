////////////////////////////////////////////////////////////////////
// tokens
////////////////////////////////////////////////////////////////////

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
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

