////////////////////////////////////////////////////////////////////
// tokenizer module
////////////////////////////////////////////////////////////////////

use std::error::Error;

use crate::tokens::Token;

type CharSlice = Vec<char>;

type NewToken = fn(String, usize, usize, usize, usize) -> Token;

type ParserFunction = fn(&CharSlice, &mut usize) -> Option<Token>;

type ValidationFunction = fn(&CharSlice, &mut usize) -> bool;

pub fn parse_fully(text: &str) -> Vec<Token> {
    let inputs: CharSlice = text.chars().collect();
    let mut pos: usize = 0;
    let mut tokens: Vec<Token> = Vec::new();
    while has_next(&inputs, &mut pos) {
        let tok_opt: Option<Token> = next_token(&inputs, &mut pos);
        if tok_opt.is_some() {
            tokens.push(tok_opt.unwrap())
        }
    }
    tokens
}

fn determine_code_position(inputs: &CharSlice, start: usize) -> (usize, usize) {
    let mut line_no: usize = 1;
    let mut column_no: usize = 1;
    for pos in 0..=start {
        match inputs[pos] {
            '\n' => {
                line_no += 1;
                column_no = 1;
            }
            _ => column_no += 1
        }
    }
    (line_no, column_no)
}

fn generate_token(inputs: &CharSlice,
                  start: usize,
                  end: usize,
                  make_token: NewToken) -> Option<Token> {
    let (line_no, column_no) = determine_code_position(&inputs, start);
    return Some(make_token(inputs[start..end].iter().collect(), start, end, line_no, column_no));
}

fn has_more(inputs: &CharSlice, pos: &mut usize) -> bool {
    *pos < (*inputs).len()
}

fn has_next(inputs: &CharSlice, pos: &mut usize) -> bool {
    while is_whitespace(inputs, pos) {
        *pos += 1
    }
    has_more(&inputs, pos)
}

// Define the helper functions is_whitespace, has_more, and next_token here
fn is_whitespace(inputs: &CharSlice, pos: &mut usize) -> bool {
    let chars = &['\t', '\n', '\r', ' '];
    has_more(inputs, pos) && chars.contains(&inputs[*pos])
}

fn next_token(inputs: &CharSlice, pos: &mut usize) -> Option<Token> {
    let parsers: Vec<ParserFunction> = vec![
        next_operator_token,
        next_numeric_token,
        next_alphanumeric_token,
        next_backticks_quoted_token,
        next_double_quoted_token,
        next_single_quoted_token,
        next_symbol_token,
    ];

    // find and return the token
    return parsers.iter().find_map(|&p| p(&inputs, pos));
}

fn next_alphanumeric_token(inputs: &CharSlice, pos: &mut usize) -> Option<Token> {
    next_eligible_token(inputs, pos, Token::AlphaNumeric, |inputs, pos| has_more(inputs, pos) && (inputs[*pos].is_alphanumeric()))
}

fn next_eligible_token(inputs: &CharSlice,
                       pos: &mut usize,
                       make_token: NewToken,
                       is_valid: ValidationFunction) -> Option<Token> {
    let start = *pos;
    if has_more(inputs, pos) && is_valid(inputs, pos) {
        while has_more(inputs, pos) && is_valid(inputs, pos) {
            *pos += 1;
        }
        let end = *pos;
        return generate_token(inputs, start, end, make_token);
    }
    None
}

fn next_backticks_quoted_token(inputs: &CharSlice, pos: &mut usize) -> Option<Token> {
    next_quoted_string_token(inputs, pos, Token::BackticksQuoted, '`')
}

fn next_double_quoted_token(inputs: &CharSlice, pos: &mut usize) -> Option<Token> {
    next_quoted_string_token(inputs, pos, Token::DoubleQuoted, '"')
}

fn next_glyph_token(inputs: &CharSlice,
                    pos: &mut usize,
                    make_token: NewToken,
                    is_valid: ValidationFunction) -> Option<Token> {
    let start = *pos;
    if has_more(inputs, pos) && is_valid(inputs, pos) {
        *pos += 1;
        let end = *pos;
        return generate_token(inputs, start, end, make_token);
    }
    None
}

fn next_numeric_token(inputs: &CharSlice, pos: &mut usize) -> Option<Token> {
    next_eligible_token(inputs, pos, Token::Numeric, |inputs, pos| {
        has_more(inputs, pos) && (inputs[*pos].is_numeric())
    })
}

fn next_quoted_string_token(inputs: &CharSlice, pos: &mut usize, make_token: NewToken, q: char) -> Option<Token> {
    if has_more(inputs, pos) && inputs[*pos] == q {
        let start = *pos;
        *pos += 1;
        while has_more(inputs, pos) && inputs[*pos] != q {
            *pos += 1;
        }
        *pos += 1;
        let end = *pos;
        return generate_token(inputs, start, end, make_token);
    }
    None
}

fn next_operator_token(inputs: &CharSlice, pos: &mut usize) -> Option<Token> {
    next_glyph_token(inputs, pos, Token::Operator,
                     |inputs, pos| {
                         let chars = ['!', '%', '*', '&', '/', '+', '-', '<', '>', '=', '[', ']', '(', ')'];
                         has_more(inputs, pos) && chars.contains(&inputs[*pos])
                     })
}

fn next_single_quoted_token(inputs: &CharSlice, pos: &mut usize) -> Option<Token> {
    next_quoted_string_token(inputs, pos, Token::SingleQuoted, '\'')
}

fn next_symbol_token(inputs: &CharSlice, pos: &mut usize) -> Option<Token> {
    next_glyph_token(inputs, pos, Token::Symbol, has_more)
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_determine_code_position() {
        let text = "this\n is\n 1 \n\"way of the world\"\n - `x` + '%'";
        let inputs = text.chars().collect();
        let start = 32;
        let (line_no, column_no) = determine_code_position(&inputs, start);
        assert_eq!(line_no, 5);
        assert_eq!(column_no, 2);
    }

    #[test]
    fn test_parse_fully() {
        let text = "this\n is\n 1 \n\"way of the world\"\n ! `x` + '%' $";
        let tokens = parse_fully(text);
        for (i, tok) in tokens.iter().enumerate() {
            println!("token[{}]: {:?}", i, tok)
        }
        assert_eq!(tokens.len(), 9);
    }
}