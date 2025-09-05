#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
//      Utility Functions
////////////////////////////////////////////////////////////////////

use crate::errors::throw;
use crate::errors::Errors::IndexOutOfRange;
use crate::expression::Conditions::{AssumedBoolean, False, True};
use crate::expression::Expression::{Condition, Literal};
use crate::expression::{Conditions, Expression};
use crate::numbers::Numbers::U8Value;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{Boolean, CharValue, Number, UUIDValue, Undefined};
use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeDelta};
use num_traits::ToPrimitive;
use regex::Regex;
use shared_lib::cnv_error;
use uuid::Uuid;

const DECIMAL_FORMAT: &str = r"^-?(?:\d+(?:_\d)*|\d+)(?:\.\d+)?$";
const INTEGER_FORMAT: &str = r"^-?(?:\d+(?:_\d)*)?$";
const ISO_DATE_FORMAT: &str =
    r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?(([+-]\d\d:\d\d)|Z)?$";
const UUID_FORMAT: &str =
    "^[0-9a-fA-F]{8}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{12}$";

pub fn char_div(c: char, divisor: u32) -> TypedValue {
    match divisor {
        0 => Undefined,
        div => char::from_u32(c as u32 / div)
            .map(|c| CharValue(c)).unwrap_or(Undefined)
    }
}

pub fn char_map(c: char, n: u32, f: fn(u32, u32) -> u32) -> TypedValue {
    char::from_u32(f(c as u32, n)).map(|c| CharValue(c)).unwrap_or(Undefined)
}

pub fn compute_time_millis(dt: TimeDelta) -> f64 {
    match dt.num_nanoseconds() {
        Some(nano) => nano.to_f64().map(|t| t / 1e+6).unwrap_or(0.),
        None => dt.num_milliseconds().to_f64().unwrap_or(0.)
    }
}

pub fn elem_at<T>(
    type_name: &str,
    items: T,
    index: TypedValue,
    len: fn(&T) -> std::io::Result<usize>,
    get: fn(&T, usize) -> std::io::Result<TypedValue>,
) -> std::io::Result<TypedValue> {
    let (idx, size) = (index.to_usize(), len(&items)?);
    if idx < size {
        get(&items, idx)
    } else {
        throw(IndexOutOfRange(type_name.to_string(), idx, size))
    }
}

pub fn decode_base36(input: &str) -> std::io::Result<u128> {
    u128::from_str_radix(input, 36).map_err(|e| cnv_error!(e))
}

pub fn encode_base36(mut num: u128) -> std::io::Result<String> {
    const BASE: u128 = 36;
    const CHARSET: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let mut result = Vec::new();

    while num > 0 {
        let rem = (num % BASE) as usize;
        result.push(CHARSET[rem]);
        num /= BASE;
    }

    result.reverse();
    String::from_utf8(result).map_err(|e| cnv_error!(e))
}

pub fn expand_escapes(input: &str) -> String {
    // Fast path: no escapes present
    if !input.as_bytes().contains(&b'\\') {
        return input.to_owned();
    }

    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();

    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }

        match chars.next() {
            Some('n')  => out.push('\n'),
            Some('r')  => out.push('\r'),
            Some('t')  => out.push('\t'),
            Some('0')  => out.push('\0'),
            Some('\\') => out.push('\\'),
            Some('\'') => out.push('\''),
            Some('"')  => out.push('"'),

            // Unknown escape: keep it literal (\X)
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }

    out
}

pub fn generate_uuid() -> u128 {
    Uuid::new_v4().as_u128()
}

pub fn is_decimal(value: &str) -> std::io::Result<bool> {
    let decimal_regex = Regex::new(DECIMAL_FORMAT).map_err(|e| cnv_error!(e))?;
    Ok(decimal_regex.is_match(value))
}

pub fn is_integer(value: &str) -> std::io::Result<bool> {
    let int_regex = Regex::new(INTEGER_FORMAT).map_err(|e| cnv_error!(e))?;
    Ok(int_regex.is_match(value))
}

pub fn is_iso8601(value: &str) -> std::io::Result<bool> {
    let iso_date_regex = Regex::new(ISO_DATE_FORMAT).map_err(|e| cnv_error!(e))?;
    Ok(iso_date_regex.is_match(value))
}

pub fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

pub fn is_numeric_value(value: &str) -> std::io::Result<bool> {
    let decimal_regex = Regex::new(DECIMAL_FORMAT)
        .map_err(|e| cnv_error!(e))?;
    Ok(decimal_regex.is_match(value))
}

pub fn is_quoted(s: &str) -> bool {
    (s.starts_with("\"") && s.ends_with("\"")) ||
        (s.starts_with("'") && s.ends_with("'"))
}

pub fn is_uuid(value: &str) -> std::io::Result<bool> {
    let uuid_regex = Regex::new(UUID_FORMAT).map_err(|e| cnv_error!(e))?;
    Ok(uuid_regex.is_match(value))
}

/// Tests whether as string could be converted into an u16
pub fn is_u16(s: &str) -> bool { s.parse::<u16>().is_ok() }

pub fn lift_condition(condition_expr: &Expression) -> std::io::Result<Conditions> {
    Ok(match condition_expr {
        Condition(condition) => condition.clone(),
        Literal(Boolean(yes)) => if *yes { True } else { False },
        expr => AssumedBoolean(expr.clone().into())
    })
}

pub fn millis_to_iso_date(millis: i64) -> Option<String> {
    let seconds = millis / 1000;
    let nanoseconds = (millis % 1000) * 1_000_000;
    let datetime = DateTime::from_timestamp(seconds, nanoseconds as u32)?;
    let iso_date = datetime.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
    Some(iso_date)
}

pub fn millis_to_naive_date(millis: i64) -> Option<NaiveDate> {
    // Convert milliseconds to seconds and nanoseconds
    let secs = millis / 1000;
    let nanos = (millis % 1000) * 1_000_000;

    // Build a NaiveDateTime
    NaiveDateTime::from_timestamp_opt(secs, nanos as u32)
        .map(|dt| dt.date())
}

/// Converts the contents of a string to u16
pub fn parse_u16(s: &str) -> std::io::Result<u16> {
    s.parse::<u16>().map_err(|e| cnv_error!(e))
}

pub fn remove_last_char(s: &str) -> &str {
    s[0..s.len() - 1].trim()
}

pub fn string_to_char_values(s: &str) -> Vec<TypedValue> {
    s.chars().into_iter().map(|c| CharValue(c)).collect()
}

pub fn string_to_uuid(text: &str) -> std::io::Result<u128> {
    Ok(Uuid::parse_str(text).map_err(|e| cnv_error!(e))?.as_u128())
}

pub fn string_to_uuid_value(text: &str) -> std::io::Result<TypedValue> {
    Ok(UUIDValue(string_to_uuid(text)?))
}

pub fn strip_margin(input: &str, margin_char: char) -> String {
    input
        .lines()
        .map(|line| {
            if let Some(pos) = line.find(margin_char) {
                line[pos + 1..].to_string()
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn superscript(nth: usize) -> String {
    if nth == 0 {
        return "⁰".into();
    }

    let digits = ["⁰", "¹", "²", "³", "⁴", "⁵", "⁶", "⁷", "⁸", "⁹"];
    let mut result = String::new();
    let mut stack = Vec::new();
    let mut n = nth;
    while n > 0 {
        stack.push(n % 10);
        n /= 10;
    }

    while let Some(digit) = stack.pop() {
        result.push_str(digits[digit]);
    }
    result
}

pub fn to_bytestring(bytes: &Vec<u8>) -> String {
    format!("0B{}", bytes.iter().map(|b| format!("{:02x}", b))
        .collect::<Vec<_>>().join(""))
}

pub fn u128_to_uuid(uuid: u128) -> String {
    // extract each group using bit shifts and masks
    let time_low = (uuid >> 96) as u32;
    let time_mid = (uuid >> 80) as u16;
    let time_hi_and_version = (uuid >> 64) as u16;
    let clk_seq = (uuid >> 48) as u16;
    let node = uuid as u64 & 0xFFFFFFFFFFFF;
    // format into a UUID string
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        time_low, time_mid, time_hi_and_version, clk_seq, node
    )
}

pub fn u32_to_char(n: u32) -> Option<char> {
    if n <= 0x10FFFF {
        char::from_u32(n)
    } else {
        u64_to_unicode_char(n as u64)
    }
}

pub fn u64_to_unicode_char(value: u64) -> Option<char> {
    let bytes = value.to_le_bytes();             // Convert to 8-byte array
    let len = bytes.iter().position(|&b| b == 0).unwrap_or(8); // Find actual UTF-8 length
    std::str::from_utf8(&bytes[..len])
        .ok()
        .and_then(|s| s.chars().next())
}

pub fn u8_vec_to_values(bytes: &Vec<u8>) -> Vec<TypedValue> {
    bytes.into_iter().map(|b| Number(U8Value(*b))).collect()
}

pub fn u8_vec_to_char(bytes: &Vec<u8>) -> Option<char> {
    let len = bytes.len();
    let bb = if len > 4 { &bytes[(len - 4)..] } else { bytes.as_slice() };
    std::str::from_utf8(bb).ok()?.chars().next()
}

pub fn u8_vec_to_u128(bytes: Vec<u8>) -> Option<u128> {
    if bytes.len() > 16 {
        return None; // u128 can only hold 16 bytes
    }

    let mut buf = [0u8; 16];
    let start = 16 - bytes.len();
    buf[start..].copy_from_slice(&bytes);

    Some(u128::from_be_bytes(buf))
}

pub fn unicode_char_to_u64(c: char) -> u64 {
    let mut buf = [0u8; 4];                     // UTF-8 of a char fits in 4 bytes
    let utf8_bytes = c.encode_utf8(&mut buf);   // Encode char into UTF-8 bytes
    let bytes = utf8_bytes.as_bytes();          // Get the actual byte slice

    // Pad the bytes to 8 bytes for u64 conversion
    let mut padded = [0u8; 8];
    for i in 0..bytes.len() {
        padded[i] = bytes[i];
    }

    u64::from_le_bytes(padded) // Convert 8-byte array to u64 (little-endian)
}

pub fn values_to_u8_vec(values: &[TypedValue]) -> Vec<u8> {
    values.into_iter().map(|v| v.to_u8()).collect()
}

/// Unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::interpret;

    #[test]
    fn test_decode_base36() {
       assert_eq!(decode_base36("C3PO").unwrap(), 564684);
    }

    #[test]
    fn test_encode_base36() {
        assert_eq!(encode_base36(564684).unwrap(), "C3PO");
    }

    #[test]
    fn test_expand_escape_sequences() {
        let sequences = vec![
            ("\\n", "\n"), ("\\r", "\r"), ("\\t", "\t"),
            ("\\", "\\"), ("\\\"", "\""), ("\\'", "'"),
        ];
        for (input, expected) in sequences {
            assert_eq!(expand_escapes(input), expected);
        }
    }

    #[test]
    fn test_is_u16() {
        assert!(is_u16("456"))
    }

    #[test]
    fn test_is_u16_vs_float() {
        assert!(!is_u16("113.76"))
    }

    #[test]
    fn test_parse_u16() {
        assert_eq!(parse_u16("8766").unwrap(), 8766)
    }

    #[test]
    fn test_superscript() {
        assert_eq!(superscript(5), "⁵");
        assert_eq!(superscript(23), "²³");
        assert_eq!(superscript(960), "⁹⁶⁰");
        assert_eq!(superscript(1874), "¹⁸⁷⁴");
    }

    #[test]
    fn test_values_to_u8_vec() {
        assert_eq!(values_to_u8_vec(vec![
            interpret("100"),
            interpret("1000"),
            interpret("33"),
            interpret("-1")
        ].as_slice()), vec![100u8, 232u8, 33u8, 255u8]);
    }
}