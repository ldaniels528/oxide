#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Sprintf class
////////////////////////////////////////////////////////////////////

use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{Number, StringValue};
use std::fmt::Write;

#[derive(Debug)]
pub struct StringPrinter;

impl StringPrinter {
    pub fn format(format: &str, args: Vec<TypedValue>) -> Result<String, String> {
        let mut result = String::new();
        let mut chars = format.chars().peekable();
        let mut arg_index = 0;

        while let Some(ch) = chars.next() {
            if ch == '%' {
                if let Some(&next) = chars.peek() {
                    if next == '%' {
                        result.push('%');
                        chars.next();
                        continue;
                    }

                    // Step 1: Try to parse positional argument (e.g., %2$s)
                    let mut pos_digits = String::new();
                    let mut preview = chars.clone();
                    while let Some(&c) = preview.peek() {
                        if c.is_ascii_digit() {
                            pos_digits.push(c);
                            preview.next();
                        } else if c == '$' {
                            preview.next();
                            chars = preview;
                            break;
                        } else {
                            pos_digits.clear();
                            break;
                        }
                    }

                    let pos_arg_index = if !pos_digits.is_empty() {
                        Some(pos_digits.parse::<usize>().unwrap() - 1)
                    } else {
                        None
                    };

                    // Step 2: Parse optional flag
                    let mut flag = None;
                    if let Some(&c) = chars.peek() {
                        if c == '-' || c == '0' {
                            flag = Some(c);
                            chars.next();
                        }
                    }

                    // Step 3: Parse width
                    let mut width_str = String::new();
                    while let Some(&c) = chars.peek() {
                        if c.is_ascii_digit() {
                            width_str.push(c);
                            chars.next();
                        } else {
                            break;
                        }
                    }

                    // Step 4: Parse precision
                    let mut precision = None;
                    if let Some(&'.') = chars.peek() {
                        chars.next();
                        let mut prec_str = String::new();
                        while let Some(&c) = chars.peek() {
                            if c.is_ascii_digit() {
                                prec_str.push(c);
                                chars.next();
                            } else {
                                break;
                            }
                        }
                        precision = prec_str.parse::<usize>().ok();
                    }

                    // Step 5: Final specifier
                    let spec = chars.next().ok_or("Unexpected end of format string")?;
                    let index = pos_arg_index.unwrap_or(arg_index);

                    if index >= args.len() {
                        return Err("Not enough arguments provided".to_string());
                    }
                    if pos_arg_index.is_none() {
                        arg_index += 1;
                    }

                    let arg = &args[index];
                    let width_val = width_str.parse::<usize>().unwrap_or(0);

                    match (spec, arg) {
                        ('d', Number(i)) => {
                            if flag == Some('0') {
                                write!(result, "{:0width$}", i.to_i64(), width = width_val).unwrap()
                            } else {
                                write!(result, "{:width$}", i.to_i64(), width = width_val).unwrap()
                            }
                        }
                        ('u', Number(u)) => {
                            if flag == Some('0') {
                                write!(result, "{:0width$}", u.to_u64(), width = width_val).unwrap()
                            } else {
                                write!(result, "{:width$}", u.to_u64(), width = width_val).unwrap()
                            }
                        }
                        ('x', Number(i)) => write!(result, "{:x}", i.to_i64()).unwrap(),
                        ('X', Number(i)) => write!(result, "{:X}", i.to_i64()).unwrap(),
                        ('o', Number(i)) => write!(result, "{:o}", i.to_i64()).unwrap(),
                        ('b', Number(i)) => write!(result, "{:b}", i.to_i64()).unwrap(),
                        ('c', StringValue(c)) => result.push(c.chars().next().unwrap_or('\0')),
                        ('s', StringValue(s)) => {
                            let expanded = StringPrinter::expand_control_chars(s);
                            if width_val > 0 {
                                if flag == Some('-') {
                                    write!(result, "{:<width$}", expanded, width = width_val).unwrap();
                                } else {
                                    write!(result, "{:>width$}", expanded, width = width_val).unwrap();
                                }
                            } else {
                                result.push_str(&expanded);
                            }
                        }
                        ('f', Number(f)) => {
                            match (width_val, precision) {
                                (0, Some(p)) => write!(result, "{:.p$}", f.to_f64(), p = p).unwrap(),
                                (w, Some(p)) => write!(result, "{:>width$.p$}", f.to_f64(), width = w, p = p).unwrap(),
                                (w, None) => write!(result, "{:>width$}", f.to_f64(), width = w).unwrap(),
                            }
                        }
                        ('?', _) => write!(result, "{:?}", arg).unwrap(),
                        (_, _) => result.push_str("<?>"),
                    }
                } else {
                    return Err("Unexpected end of format string after '%'".to_string());
                }
            } else if ch == '\\' {
                if let Some(escaped) = chars.next() {
                    match escaped {
                        'n' => result.push('\n'),
                        'r' => result.push('\r'),
                        't' => result.push('\t'),
                        'b' => result.push('\x08'), // backspace
                        other => {
                            result.push('\\');
                            result.push(other);
                        }
                    }
                } else {
                    result.push('\\');
                }
            } else {
                result.push(ch);
            }
        }

        Ok(result)
    }

    fn expand_control_chars(s: &str) -> String {
        let mut result = String::new();
        let mut chars = s.chars().peekable();
        while let Some(ch) = chars.next() {
            if ch == '\\' {
                if let Some(escaped) = chars.next() {
                    match escaped {
                        'n' => result.push('\n'),
                        'r' => result.push('\r'),
                        't' => result.push('\t'),
                        'b' => result.push('\x08'),
                        other => {
                            result.push('\\');
                            result.push(other);
                        }
                    }
                } else {
                    result.push('\\');
                }
            } else {
                result.push(ch);
            }
        }
        result
    }

}

/// inference unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::numbers::Numbers::{F64Value, I64Value};

    #[test]
    fn test_float_precision() {
        let num = 3.6667;
        let s = StringPrinter::format("%0.2f", vec![Number(F64Value(num))]).unwrap();
        assert_eq!(s, "3.67");
    }

    #[test]
    fn test_string_padding_right() {
        let s = StringPrinter::format("%40s", vec![StringValue("Hello".into())]).unwrap();
        assert_eq!(s, format!("{:>40}", "Hello"));
    }

    #[test]
    fn test_string_padding_left() {
        let s = StringPrinter::format("%-40s", vec![StringValue("Hello".into())]).unwrap();
        assert_eq!(s, format!("{:<40}", "Hello"));
    }

    #[test]
    fn test_hex_lower() {
        let s = StringPrinter::format("%x", vec![Number(I64Value(255))]).unwrap();
        assert_eq!(s, "ff");
    }

    #[test]
    fn test_hex_upper() {
        let s = StringPrinter::format("%X", vec![Number(I64Value(255))]).unwrap();
        assert_eq!(s, "FF");
    }

    #[test]
    fn test_octal() {
        let s = StringPrinter::format("%o", vec![Number(I64Value(255))]).unwrap();
        assert_eq!(s, "377");
    }

    #[test]
    fn test_binary() {
        let s = StringPrinter::format("%b", vec![Number(I64Value(255))]).unwrap();
        assert_eq!(s, "11111111");
    }

    #[test]
    fn test_unsigned() {
        let s = StringPrinter::format("%u", vec![Number(I64Value(1234567890))]).unwrap();
        assert_eq!(s, "1234567890");
    }

    #[test]
    fn test_positional_args() {
        let s = StringPrinter::format("Argument: %2$s %1$d", vec![
            Number(I64Value(42)),
            StringValue("Value".into())
        ]).unwrap();
        assert_eq!(s, "Argument: Value 42");
    }

    #[test]
    fn test_literal_percent() {
        let s = StringPrinter::format("Escape percent: %%", vec![]).unwrap();
        assert_eq!(s, "Escape percent: %");
    }

    #[test]
    fn test_debug_fallback() {
        let s = StringPrinter::format("Debug: %?", vec![Number(F64Value(1.2345))]).unwrap();
        assert_eq!(s, "Debug: Number(F64Value(1.2345))");
    }

    #[test]
    fn test_control_chars_in_format_string() {
        let result = StringPrinter::format("Hello\\nWorld", vec![]).unwrap();
        assert_eq!(result, "Hello\nWorld");
    }

    #[test]
    fn test_control_chars_in_string_argument() {
        let result = StringPrinter::format("%s", vec![StringValue("Line1\\nLine2".into())]).unwrap();
        assert_eq!(result, "Line1\nLine2");
    }

    #[test]
    fn test_tab_and_backspace_escape() {
        let result = StringPrinter::format("%s", vec![StringValue("Column1\\tColumn2\\b".into())]).unwrap();
        assert_eq!(result, "Column1\tColumn2\x08");
    }
}