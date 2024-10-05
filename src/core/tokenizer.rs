////////////////////////////////////////////////////////////////////
// tokenizer module
////////////////////////////////////////////////////////////////////

use crate::tokens::Token;

const COMPOUND_OPERATORS: [&str; 23] = [
    "&&", "**", "||", "::", "..", "==", ">>", "<<",
    "->", "<-", ">=", "<=", "=>",
    "+=", "-=", "*=", "/=", "%=", "&=", "^=", "!=", ":=",
    "~>",
];

const COMPOUND_SYMBOLS: [&str; 6] = [
    "[^]", "[!]", "[+]", "[-]", "[~]", "[_]",
];

const SIMPLE_OPERATORS: [char; 39] = [
    '!', '¡', '@', '#', '$', '%', '^', '&', '×', '*', '÷', '/', '+', '-', '=',
    '(', ')', '<', '>', '{', '}', '[', ']', ',', ';', '?', '\\', '|', '~',
    '⁰', '¹', '²', '³', '⁴', '⁵', '⁶', '⁷', '⁸', '⁹',
];

type NewToken = fn(String, usize, usize, usize, usize) -> Token;

type ParserFunction = fn(&Vec<char>, &mut usize) -> Option<Token>;

type ValidationFunction = fn(&Vec<char>, &mut usize) -> bool;

pub fn parse_fully(text: &str) -> Vec<Token> {
    let inputs: Vec<char> = text.chars().collect();
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

fn determine_code_position(inputs: &Vec<char>, start: usize) -> (usize, usize) {
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

fn generate_token(inputs: &Vec<char>,
                  start: usize,
                  end: usize,
                  make_token: NewToken) -> Option<Token> {
    let (line_no, column_no) = determine_code_position(&inputs, start);
    return Some(make_token(inputs[start..end].iter().collect(), start, end, line_no, column_no));
}

fn has_at_least(inputs: &Vec<char>, pos: &mut usize, n_chars: usize) -> bool {
    *pos + n_chars <= (*inputs).len()
}

fn has_more(inputs: &Vec<char>, pos: &mut usize) -> bool {
    *pos < (*inputs).len()
}

fn has_next(inputs: &Vec<char>, pos: &mut usize) -> bool {
    while is_whitespace(inputs, pos) {
        *pos += 1
    }
    has_more(&inputs, pos)
}

fn is_operator(inputs: &Vec<char>, pos: usize) -> bool {
    SIMPLE_OPERATORS.contains(&inputs[pos])
}

fn is_whitespace(inputs: &Vec<char>, pos: &mut usize) -> bool {
    let chars = &['\t', '\n', '\r', ' '];
    has_more(inputs, pos) && chars.contains(&inputs[*pos])
}

fn next_token(inputs: &Vec<char>, pos: &mut usize) -> Option<Token> {
    let parsers: Vec<ParserFunction> = vec![
        skip_comments,
        next_compound_symbol_token,
        next_compound_operator_token,
        next_operator_token,
        next_numeric_token,
        next_atom_token,
        next_backticks_quoted_token,
        next_double_quoted_token,
        next_single_quoted_token,
        next_symbol_token,
    ];

    // find and return the token
    parsers.iter().find_map(|&p| p(inputs, pos))
}

fn next_atom_token(inputs: &Vec<char>, pos: &mut usize) -> Option<Token> {
    next_eligible_token(inputs, pos, Token::atom,
                        |inputs, pos| has_more(inputs, pos) &&
                            !is_operator(inputs, *pos) &&
                            (inputs[*pos].is_alphanumeric() || inputs[*pos] == '_'))
}

fn next_eligible_token(inputs: &Vec<char>,
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

fn next_backticks_quoted_token(inputs: &Vec<char>, pos: &mut usize) -> Option<Token> {
    next_quoted_string_token(inputs, pos, Token::backticks, '`')
}

fn next_double_quoted_token(inputs: &Vec<char>, pos: &mut usize) -> Option<Token> {
    next_quoted_string_token(inputs, pos, Token::double_quoted, '"')
}

fn next_glyph_token(inputs: &Vec<char>,
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

fn next_numeric_token(inputs: &Vec<char>, pos: &mut usize) -> Option<Token> {
    fn is_digit(c: char) -> bool { "0123456789".contains(c) }
    fn is_digit_or_underscore(c: char) -> bool { is_digit(c) || c == '_' }
    fn is_dot(c: char) -> bool { c == '.' }

    // can start with '.' or '0'..'9' (e.g. 123, 1_000.00_000, .5, 5.)
    let start = *pos;
    if has_more(inputs, pos) && (is_dot(inputs[start]) || is_digit(inputs[start])) {
        // after the first '.' it cannot occur again
        let mut has_dot = is_dot(inputs[start]);
        *pos += 1;

        // collect digits, underscores and up-to one dot.
        while has_more(inputs, pos) && (is_digit_or_underscore(inputs[*pos]) ||
            (!has_dot && is_dot(inputs[*pos]))) {
            has_dot |= is_dot(inputs[*pos]);
            *pos += 1
        }

        // is this a range (e.g. "..")?
        if *pos < inputs.len() && *pos > 0 && has_dot &&
            is_dot(inputs[*pos]) && is_dot(inputs[*pos - 1]) {
            *pos -= 1;
            return generate_token(inputs, start, *pos, Token::numeric);
        }

        // was the length only one?
        match (*pos - start, inputs[start]) {
            // if just an underscore, it must be an atom
            (1, '_') => {
                *pos = start;
                next_atom_token(inputs, pos)
            }
            (1, '.') => generate_token(inputs, start, *pos, Token::operator),
            _ => generate_token(inputs, start, *pos, Token::numeric)
        }
    } else { None }
}

fn next_quoted_string_token(inputs: &Vec<char>, pos: &mut usize, make_token: NewToken, q: char) -> Option<Token> {
    if has_more(inputs, pos) && inputs[*pos] == q {
        let start = *pos;
        *pos += 1;
        while has_more(inputs, pos) && inputs[*pos] != q {
            *pos += 1;
        }
        *pos += 1;
        let end = *pos;
        return generate_token(inputs, start + 1, end - 1, make_token);
    }
    None
}

fn is_same(a: &[char], b: &str) -> bool {
    let aa: String = a.iter().collect();
    let bb: String = b.into();
    aa == bb
}

fn next_compound_operator_token(inputs: &Vec<char>, pos: &mut usize) -> Option<Token> {
    let symbol_len = 2;
    let start = *pos;
    if has_at_least(inputs, pos, symbol_len) &&
        COMPOUND_OPERATORS.iter().any(|gl| is_same(&inputs[start..(start + symbol_len)], gl)) {
        *pos += symbol_len;
        let end = *pos;
        generate_token(&inputs, start, end, Token::operator)
    } else { None }
}

fn next_compound_symbol_token(inputs: &Vec<char>, pos: &mut usize) -> Option<Token> {
    let symbol_len = 3;
    let start = *pos;
    if has_at_least(inputs, pos, symbol_len) &&
        COMPOUND_SYMBOLS.iter().any(|gl| is_same(&inputs[start..(start + symbol_len)], gl)) {
        *pos += symbol_len;
        let end = *pos;
        generate_token(&inputs, start, end, Token::atom)
    } else { None }
}

fn next_operator_token(inputs: &Vec<char>, pos: &mut usize) -> Option<Token> {
    next_glyph_token(inputs, pos, Token::operator,
                     |inputs, pos| {
                         has_more(inputs, pos) && SIMPLE_OPERATORS.contains(&inputs[*pos])
                     })
}

fn next_single_quoted_token(inputs: &Vec<char>, pos: &mut usize) -> Option<Token> {
    next_quoted_string_token(inputs, pos, Token::single_quoted, '\'')
}

fn next_symbol_token(inputs: &Vec<char>, pos: &mut usize) -> Option<Token> {
    next_glyph_token(inputs, pos, Token::operator, has_more)
}

fn skip_comments(inputs: &Vec<char>, pos: &mut usize) -> Option<Token> {
    let symbol1 = "//";
    let start = *pos;
    if has_at_least(inputs, pos, symbol1.len()) && is_same(&inputs[start..(start + symbol1.len())], symbol1) {
        *pos += symbol1.len();
        while has_more(inputs, pos) && inputs[*pos] != '\n' { *pos += 1 }
        *pos += 1;
        has_next(inputs, pos);
    }
    None
}

// Unit tests
#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;

    #[test]
    fn test_compound_operators() {
        for op in COMPOUND_OPERATORS {
            assert_eq!(parse_fully(op), vec![
                Token::operator(op.to_string(), 0, op.len(), 1, op.len())
            ])
        }

        assert_eq!(parse_fully("true && false"), vec![
            Token::atom("true".into(), 0, 4, 1, 2),
            Token::operator("&&".into(), 5, 7, 1, 7),
            Token::atom("false".into(), 8, 13, 1, 10),
        ])
    }

    #[test]
    fn test_exponent_literals() {
        assert_eq!(parse_fully("x³ + 2y²"), vec![
            Token::atom("x".to_string(), 0, 1, 1, 2),
            Token::operator("³".to_string(), 1, 2, 1, 3),
            Token::operator("+".to_string(), 3, 4, 1, 5),
            Token::numeric("2".to_string(), 5, 6, 1, 7),
            Token::atom("y".to_string(), 6, 7, 1, 8),
            Token::operator("²".to_string(), 7, 8, 1, 9),
        ])
    }

    #[test]
    fn test_numbers() {
        assert_eq!(parse_fully("1 10 100.0 100_000_000 _ace .3567"), vec![
            Token::numeric("1".to_string(), 0, 1, 1, 2),
            Token::numeric("10".to_string(), 2, 4, 1, 4),
            Token::numeric("100.0".to_string(), 5, 10, 1, 7),
            Token::numeric("100_000_000".to_string(), 11, 22, 1, 13),
            Token::atom("_ace".to_string(), 23, 27, 1, 25),
            Token::numeric(".3567".to_string(), 28, 33, 1, 30),
        ])
    }

    #[test]
    fn test_range() {
        assert_eq!(parse_fully("100..1000"), vec![
            Token::numeric("100".to_string(), 0, 3, 1, 2),
            Token::operator("..".to_string(), 3, 5, 1, 5),
            Token::numeric("1000".to_string(), 5, 9, 1, 7),
        ])
    }

    #[test]
    fn test_symbols() {
        assert_eq!(parse_fully("_"), vec![
            Token::atom("_".to_string(), 0, 1, 1, 2)
        ])
    }

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

    #[test]
    fn test_skip_comments() {
        assert_eq!(parse_fully(r#"
            // this is a comment
            one
            "#), vec![
            Token::atom("one".into(), 46, 49, 3, 14)
        ])
    }
    
}