////////////////////////////////////////////////////////////////////
// tokenizer module
////////////////////////////////////////////////////////////////////

use crate::tokens::Token;

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

// Define the helper functions is_whitespace, has_more, and next_token here
fn is_whitespace(inputs: &Vec<char>, pos: &mut usize) -> bool {
    let chars = &['\t', '\n', '\r', ' '];
    has_more(inputs, pos) && chars.contains(&inputs[*pos])
}

fn next_token(inputs: &Vec<char>, pos: &mut usize) -> Option<Token> {
    let parsers: Vec<ParserFunction> = vec![
        next_compound_operator_token,
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

fn next_alphanumeric_token(inputs: &Vec<char>, pos: &mut usize) -> Option<Token> {
    next_eligible_token(inputs, pos, Token::alpha,
                        |inputs, pos| has_more(inputs, pos) && (inputs[*pos].is_alphanumeric() || inputs[*pos] == '_'))
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
    if inputs[*pos].is_numeric() || inputs[*pos] == '.' ||
        (*pos + 1 < inputs.len() && inputs[*pos] == '_' && inputs[*pos + 1].is_numeric()) {
        next_eligible_token(inputs, pos, Token::numeric, |inputs, pos| {
            has_more(inputs, pos) && (inputs[*pos].is_numeric() || inputs[*pos] == '.' || inputs[*pos] == '_')
        })
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
        return generate_token(inputs, start, end, make_token);
    }
    None
}

fn next_compound_operator_token(inputs: &Vec<char>, pos: &mut usize) -> Option<Token> {
    fn same(a: &[char], b: &str) -> bool {
        let aa: String = a.iter().collect();
        let bb: String = b.into();
        aa == bb
    }
    let compounds = [
        "&&", "**", "!!", "||", "::", "..",
        "->", "<-", "+=", "-=", "*=", "/=", "%=", "&=", "^=", "!=", "=="
    ];
    let symbol_len = 2;
    let start = *pos;
    if has_at_least(inputs, pos, symbol_len) &&
        compounds.iter().any(|gl| same(&inputs[start..(start + symbol_len)], gl)) {
        *pos += symbol_len;
        let end = *pos;
        generate_token(&inputs, start, end, Token::operator)
    } else { None }
}

fn next_operator_token(inputs: &Vec<char>, pos: &mut usize) -> Option<Token> {
    next_glyph_token(inputs, pos, Token::operator,
                     |inputs, pos| {
                         let chars = [
                             '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '/', '+', '-',
                             '<', '>', '=', '{', '}', '[', ']', ';', '?', '\\', '|', '~'
                         ];
                         has_more(inputs, pos) && chars.contains(&inputs[*pos])
                     })
}

fn next_single_quoted_token(inputs: &Vec<char>, pos: &mut usize) -> Option<Token> {
    next_quoted_string_token(inputs, pos, Token::single_quoted, '\'')
}

fn next_symbol_token(inputs: &Vec<char>, pos: &mut usize) -> Option<Token> {
    next_glyph_token(inputs, pos, Token::symbol, has_more)
}

// Unit tests
#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;

    #[test]
    fn test_compound_operators() {
        let compounds = [
            "&&", "**", "!!", "||", "->", "<-",
            "+=", "-=", "*=", "/=", "%=", "&=", "^=", "!=", "=="
        ];
        for op in compounds {
            assert_eq!(parse_fully(op), vec![
                Token::operator(op.to_string(), 0, op.len(), 1, op.len())
            ])
        }

        assert_eq!(parse_fully("true && false"), vec![
            Token::alpha("true".into(), 0, 4, 1, 2),
            Token::operator("&&".into(), 5, 7, 1, 7),
            Token::alpha("false".into(), 8, 13, 1, 10),
        ])
    }

    #[test]
    fn test_numbers() {
        assert_eq!(parse_fully("1 10 100.0 100_000_000 _ace .3567"), vec![
            Token::numeric("1".to_string(), 0, 1, 1, 2),
            Token::numeric("10".to_string(), 2, 4, 1, 4),
            Token::numeric("100.0".to_string(), 5, 10, 1, 7),
            Token::numeric("100_000_000".to_string(), 11, 22, 1, 13),
            Token::alpha("_ace".to_string(), 23, 27, 1, 25),
            Token::numeric(".3567".to_string(), 28, 33, 1, 30),
        ])
    }

    #[test]
    fn test_symbols() {
        assert_eq!(parse_fully("_"), vec![
            Token::alpha("_".to_string(), 0, 1, 1, 2)
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
}