#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// tokenizer module
////////////////////////////////////////////////////////////////////

use crate::tokens::Token;
use crate::tokens::Token::DataframeLiteral;
use regex::Regex;
use uuid::Uuid;

/// Single-character operators
const OPERATORS_1: [char; 38] = [
    '!', '@', '#', '$', '%', '^', '&', '×', '*', '÷', '/', '+', '-', '=',
    '(', ')', '<', '>', '{', '}', '[', ']', ',', ';', '?', '\\', '|', '~',
    '⁰', '¹', '²', '³', '⁴', '⁵', '⁶', '⁷', '⁸', '⁹',
];

/// Dual-character operators
const OPERATORS_2: [&str; 28] = [
    "++", "&&", "**", "||", "::", "..", "==", ">>", "<<", "|>",
    "->", "<-", ">=", "<=", "=>",
    "+=", "-=", "*=", "/=", "%=", "&=", "^=", "!=", ":=", "?=", "!?",
    "~>", "<~",
];

/// Triple-character operators
const OPERATORS_3: [&str; 10] = [
    "~>>", "<<~", "|>>", "<|>", ":::", 
    "..=", "&&=", "||=", "<<=", ">>=",
];

/// Pseudo-numerical prefixes
const PSEUDO_NUMERICAL: [&str; 4] = ["0x", "0B", "0b", "0o"];

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
    Some(make_token(inputs[start..end].iter().collect(), start, end, line_no, column_no))
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


fn is_whitespace(inputs: &Vec<char>, pos: &mut usize) -> bool {
    let chars = &['\t', '\n', '\r', ' '];
    has_more(inputs, pos) && chars.contains(&inputs[*pos])
}

fn next_token(inputs: &Vec<char>, pos: &mut usize) -> Option<Token> {
    let parsers: Vec<ParserFunction> = vec![
        skip_comments,
        next_date_literal_token,
        next_uuid_literal_token,
        next_dataframe_literal_token,
        next_compound_3_operator_token,
        next_compound_2_operator_token,
        next_operator_token,
        next_pseudo_numeric_token,
        next_numeric_token,
        next_url_token,
        next_atom_token,
        next_backticks_quoted_token,
        next_double_quoted_token,
        next_single_quoted_token,
        next_symbol_token,
    ];

    // find and return the token
    parsers.iter().find_map(|parser| parser(inputs, pos))
}

fn next_atom_token(inputs: &Vec<char>, pos: &mut usize) -> Option<Token> {
    next_eligible_token(inputs, pos, Token::atom,
                        |inputs, pos| has_more(inputs, pos) &&
                            !OPERATORS_1.contains(&inputs[*pos]) &&
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

/// Parses a dataframe-literal
/// ```
/// |--------------------------------------|
/// | symbol | exchange | last_sale | rank |
/// |--------------------------------------|
/// | BOOM   | NYSE     | 113.76    | 1    |
/// | ABC    | AMEX     | 24.98     | 2    |
/// | JET    | NASDAQ   | 64.24     | 3    |
/// |--------------------------------------|
/// ```
fn next_dataframe_literal_token(inputs: &Vec<char>, pos: &mut usize) -> Option<Token> {
    /// Checks if the current line is a dataframe-literal header line
    fn is_header_line(inputs: &Vec<char>, pos: &mut usize) -> bool {
        is_valid_line(inputs, pos) && *pos + 1 < inputs.len() && inputs[*pos + 1] == '-'
    }

    /// Checks if the current line is a dataframe-literal data line
    fn is_valid_line(inputs: &Vec<char>, pos: &mut usize) -> bool {
        has_more(inputs, pos) && inputs[*pos] == '|'
    }
    
    fn create_cells(lines: Vec<String>) -> Vec<Vec<String>> {
        let mut cells = vec![];
        for line in lines.iter().filter(|s| !s.starts_with("|-")) {
            let items = line.split('|').map(|s| s.trim().to_string()).collect::<Vec<_>>();
            let row = items[1..items.len() - 1].to_vec();
            cells.push(row);       
        }
        cells
    }
    
    // is this a dataframe-literal?
    if is_header_line(inputs, pos) {
        let start = *pos;
        let mut lines = vec![];

        // capture the all lines
        while is_valid_line(inputs, pos) {
            let current = *pos;
            // find the end of the line
            while has_more(inputs, pos) && inputs[*pos] != '\n' { *pos += 1; }
            // capture the line
            let line = String::from_iter(inputs[current..*pos].iter());
            lines.push(line);
            // skip the whitespace
            while is_whitespace(inputs, pos) { *pos += 1; }
        }
        let end = *pos;
        let (line_number, column_number) = determine_code_position(&inputs, start);
        let cells = create_cells(lines);
        return Some(DataframeLiteral { cells, start, end, line_number, column_number })
    }
    None
}

fn next_date_literal_token(inputs: &Vec<char>, pos: &mut usize) -> Option<Token> {
    let start = *pos;
    if start + 24 <= inputs.len() {
        let date = &inputs[*pos..(*pos + 24)];
        let iso_pattern = r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z";
        let re = Regex::new(iso_pattern).unwrap();
        let date_str: String = date.iter().collect();
        if re.is_match(date_str.as_str()) {
            *pos += date.len();
            return generate_token(inputs, start, *pos, Token::date_literal)
        }
    }
    None
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

fn next_pseudo_numeric_token(inputs: &Vec<char>, pos: &mut usize) -> Option<Token> {
    /// Parses the alphanumeric digits
    fn parse_digits(
        inputs: &Vec<char>,
        start: usize,
        pos: &mut usize,
        generator: NewToken,
        is_digit: fn(char) -> bool
    ) -> Option<Token> {
        while has_more(inputs, pos) && is_digit(inputs[*pos]) { *pos += 1 }
        let end = *pos;
        generate_token(&inputs, start, end, generator)
    }

    // can start with '0x', '0b', '0o' or '0B'
    let symbol_len = 2;
    let start = *pos;
    let limit = start + symbol_len;
    if has_at_least(inputs, pos, symbol_len) && PSEUDO_NUMERICAL.iter()
        .any(|pn| is_same(&inputs[start..limit], pn)) {
        *pos += symbol_len;
        match String::from_iter(&inputs[start..limit]).as_str() {
            "0b" => parse_digits(inputs, start, pos, Token::numeric, |c| "01_".contains(c)),
            "0o" => parse_digits(inputs, start, pos, Token::numeric, |c| "01234567_".contains(c)),
            "0B" => parse_digits(inputs, start, pos, Token::binary_literal, |c| "0123456789_ABCDEFabcdef".contains(c)),
            "0x" => parse_digits(inputs, start, pos, Token::numeric, |c| "0123456789_ABCDEFabcdef".contains(c)),
            _ => None
        }
    } else { None }
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

        // if length (*pos - start) and initial character are ...
        match (*pos - start, inputs[start]) {
            // single character '_' => it's an atom
            (1, '_') => {
                *pos = start;
                next_atom_token(inputs, pos)
            }
            // single character '.' => it's an operator
            (1, '.') => generate_token(inputs, start, *pos, Token::operator),
            // otherwise, assume numeric
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

fn next_compound_2_operator_token(inputs: &Vec<char>, pos: &mut usize) -> Option<Token> {
    next_compound_operator_token(inputs, pos, OPERATORS_2.as_slice(), 2)
}

fn next_compound_3_operator_token(inputs: &Vec<char>, pos: &mut usize) -> Option<Token> {
    next_compound_operator_token(inputs, pos, OPERATORS_3.as_slice(), 3)
}

fn next_compound_operator_token(
    inputs: &Vec<char>,
    pos: &mut usize,
    operators: &[&str],
    symbol_len: usize,
) -> Option<Token> {
    let start = *pos;
    if has_at_least(inputs, pos, symbol_len) &&
        operators.iter().any(|gl| is_same(&inputs[start..(start + symbol_len)], gl)) {
        *pos += symbol_len;
        let end = *pos;
        generate_token(&inputs, start, end, Token::operator)
    } else { None }
}

fn next_operator_token(inputs: &Vec<char>, pos: &mut usize) -> Option<Token> {
    next_glyph_token(inputs, pos, Token::operator,
                     |inputs, pos| {
                         has_more(inputs, pos) && OPERATORS_1.contains(&inputs[*pos])
                     })
}

fn next_single_quoted_token(inputs: &Vec<char>, pos: &mut usize) -> Option<Token> {
    next_quoted_string_token(inputs, pos, Token::single_quoted, '\'')
}

fn next_symbol_token(inputs: &Vec<char>, pos: &mut usize) -> Option<Token> {
    next_glyph_token(inputs, pos, Token::operator, has_more)
}

fn next_uuid_literal_token(inputs: &Vec<char>, pos: &mut usize) -> Option<Token> {
    fn is_valid_uuid(input: &str) -> bool {
        Uuid::parse_str(input).is_ok()
    }
    
    let (start, end) = (*pos, *pos + 36);
    if is_valid_uuid(span(&inputs, start, end).as_str()) {
        *pos = end;
        generate_token(&inputs, start, end, Token::uuid)
    } else { 
        None
    }
}

fn next_url_token(inputs: &Vec<char>, pos: &mut usize) -> Option<Token> {
    fn is_eligible(c: char) -> bool {
        c.is_alphanumeric() || matches!(c,
            '?' | '%' | '/' | '$' | '_' | '.' | '=' | '#' | '+' | '-' | '*' |
            '!' | '&' | '|' | '^' | '<' | '>' | '~' | ':'
        )
    }

    fn parse_url(prefix: &str, inputs: &Vec<char>, pos: &mut usize) -> Option<Token> {
        let start = *pos;
        if has_at_least(inputs, pos, prefix.len()) &&
            is_same(&inputs[start..(start + prefix.len())], prefix) {
            *pos += prefix.len();
            while has_more(inputs, pos) && is_eligible(inputs[*pos]) { *pos += 1 }
            let end = *pos;
            generate_token(&inputs, start, end, Token::url)
        } else {
            None
        }
    }

    let prefixes = vec![
        "file://", "http://", "https://", "ws://", "wss://",
    ];
    prefixes.iter().fold(None, |maybe_token, prefix| {
        if maybe_token.is_some() { maybe_token } else { parse_url(prefix, inputs, pos) }
    })
}

fn skip_comments(inputs: &Vec<char>, pos: &mut usize) -> Option<Token> {
    let symbol1 = "//";
    let start = *pos;
    if has_at_least(inputs, pos, symbol1.len()) &&
        is_same(&inputs[start..(start + symbol1.len())], symbol1) {
        *pos += symbol1.len();
        while has_more(inputs, pos) && inputs[*pos] != '\n' { *pos += 1 }
        *pos += 1;
        has_next(inputs, pos);
    }
    None
}

fn span(inputs: &Vec<char>, start: usize, end: usize) -> String {
    if end <= inputs.len() {
        inputs[start..end].iter().collect()
    } else {
        inputs[start..inputs.len()].iter().collect()
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::tokens::Token::DateLiteral;
    use std::vec;

    #[test]
    fn test_binary_literal() {
        assert_eq!(parse_fully("0Bfe_ed_de_ad_be_ef_de_af_fa_de_ca_fe_ba_be_fa_ce ca"), vec![
            Token::BinaryLiteral {
                text: "0Bfe_ed_de_ad_be_ef_de_af_fa_de_ca_fe_ba_be_fa_ce".into(),
                start: 0,
                end: 49,
                line_number: 1,
                column_number: 2
            },
            Token::Atom {
                text: "ca".into(),
                start: 50,
                end: 52,
                line_number: 1,
                column_number: 52
            }
        ])
    }

    #[test]
    fn test_compound_operators() {
        fn stringify(values: &[&str]) -> Vec<String> {
            values.iter().map(|s| s.to_string()).collect()
        }

        let mut compound_operators = vec![];
        compound_operators.extend(stringify(OPERATORS_2.as_slice()));
        compound_operators.extend(stringify(OPERATORS_3.as_slice()));

        for operator in compound_operators {
            let tokens = parse_fully(operator.as_str());
            println!("tokens {:?}", tokens);
            assert_eq!(tokens, vec![
                Token::operator(operator.clone(), 0, operator.len(), 1, operator.len().min(2))
            ])
        }

        assert_eq!(parse_fully("true && false"), vec![
            Token::atom("true".into(), 0, 4, 1, 2),
            Token::operator("&&".into(), 5, 7, 1, 7),
            Token::atom("false".into(), 8, 13, 1, 10),
        ])
    }

    #[test]
    fn test_condition_not_equals() {
        assert_eq!(parse_fully("100!=1000"), vec![
            Token::Numeric { text: "100".into(), start: 0, end: 3, line_number: 1, column_number: 2 },
            Token::Operator { text: "!=".into(), start: 3, end: 5, line_number: 1, column_number: 5 },
            Token::Numeric { text: "1000".into(), start: 5, end: 9, line_number: 1, column_number: 7 }
        ])
    }

    #[test]
    fn test_date_literal() {
        let tokens = parse_fully("2025-01-13T03:25:47.350Z");
        assert_eq!(tokens, vec![
            DateLiteral { 
                text: "2025-01-13T03:25:47.350Z".into(), 
                start: 0, 
                end: 24, 
                line_number: 1, 
                column_number: 2 
            }
        ])
    }

    #[test]
    fn test_dataframe_literal() {
        let tokens = parse_fully(r#"
        |--------------------------------------|
        | symbol | exchange | last_sale | rank |
        |--------------------------------------|
        | BOOM   | NYSE     | 113.76    | 1    |
        | ABC    | AMEX     | 24.98     | 2    |
        | JET    | NASDAQ   | 64.24     | 3    |
        |--------------------------------------|
        "#);
        assert_eq!(tokens, vec![
            DataframeLiteral {
                cells: vec![
                    vec!["symbol".into(), "exchange".into(), "last_sale".into(), "rank".into()],
                    vec!["BOOM".into(), "NYSE".into(), "113.76".into(), "1".into()],
                    vec!["ABC".into(), "AMEX".into(), "24.98".into(), "2".into()],
                    vec!["JET".into(), "NASDAQ".into(), "64.24".into(), "3".into()]
                ],
                start: 9,
                end: 352,
                line_number: 2,
                column_number: 10
            }]
        )
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
    fn test_next_pseudo_numbers() {
        assert_eq!(parse_fully("100 0xdead_beef_babe_face 0b1101 0o1234675"), vec![
            Token::numeric("100".to_string(), 0, 3, 1, 2),
            Token::numeric("0xdead_beef_babe_face".to_string(), 4, 25, 1, 6),
            Token::numeric("0b1101".to_string(), 26, 32, 1, 28),
            Token::numeric("0o1234675".to_string(), 33, 42, 1, 35),
        ])
    }

    #[test]
    fn test_range_exclusive() {
        assert_eq!(parse_fully("100..1000"), vec![
            Token::numeric("100".to_string(), 0, 3, 1, 2),
            Token::operator("..".to_string(), 3, 5, 1, 5),
            Token::numeric("1000".to_string(), 5, 9, 1, 7),
        ])
    }

    #[test]
    fn test_range_inclusive() {
        assert_eq!(parse_fully("100..=1000"), vec![
            Token::Numeric { text: "100".into(), start: 0, end: 3, line_number: 1, column_number: 2 },
            Token::Operator { text: "..=".into(), start: 3, end: 6, line_number: 1, column_number: 5 },
            Token::Numeric { text: "1000".into(), start: 6, end: 10, line_number: 1, column_number: 8 }
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

    #[test]
    fn test_span() {
        let text = "this is a comment";
        let inputs = text.chars().collect::<Vec<char>>();
        assert_eq!(span(&inputs, 5, 7), "is")
    }

    #[test]
    fn test_uuid() {
        assert_eq!(parse_fully(r#"
            550e8400-e29b-41d4-a716-446655440000 
            abc
        "#), vec![
            Token::uuid("550e8400-e29b-41d4-a716-446655440000".into(),
                        13, 49, 2, 14),
            Token::atom("abc".into(),
                        63, 66, 3, 14)
        ])
    }

    #[test]
    fn test_url() {
        assert_eq!(parse_fully(r#"
            https://api.example.com/products?category=electronics abc
        "#), vec![
            Token::url("https://api.example.com/products?category=electronics".into(),
                       13, 66, 2, 14),
            Token::atom("abc".into(),
                        67, 70, 2, 68)
        ])
    }
}