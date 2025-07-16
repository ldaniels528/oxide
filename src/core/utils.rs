#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
//      Utility Functions
////////////////////////////////////////////////////////////////////

use crate::data_types::DataType::NumberType;
use crate::errors::Errors::{IndexOutOfRange, SyntaxError, TypeMismatch};
use crate::errors::TypeMismatchErrors::*;
use crate::errors::{throw, SyntaxErrors};
use crate::expression::Conditions::{AssumedBoolean, False, True};
use crate::expression::Expression::{Condition, Identifier, Literal};
use crate::expression::{Conditions, Expression};
use crate::machine::Machine;
use crate::number_kind::NumberKind::F64Kind;
use crate::numbers::Numbers;
use crate::numbers::Numbers::U8Value;
use crate::sequences::Sequences::{TheArray, TheRange, TheTuple};
use crate::sequences::{range_to_vec, Array, Sequence};
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{ArrayValue, Boolean, CharValue, Number, StringValue, TupleValue, UUIDValue};
use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeDelta};
use num_traits::ToPrimitive;
use shared_lib::cnv_error;
use uuid::Uuid;

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

pub fn expand_escapes(input: &str) -> String {
    let mut output = String::new();
    let mut chars = input.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('n') => output.push('\n'),
                Some('r') => output.push('\r'),
                Some('t') => output.push('\t'),
                Some('0') => output.push('\0'),
                Some('\\') => output.push('\\'),
                Some('\'') => output.push('\''),
                Some('\"') => output.push('\"'),
                Some('x') => {
                    let hi = chars.next();
                    let lo = chars.next();
                    if let (Some(h), Some(l)) = (hi, lo) {
                        if let Ok(byte) = u8::from_str_radix(&format!("{h}{l}"), 16) {
                            output.push(byte as char);
                        }
                    }
                }
                Some(other) => {
                    output.push('\\');
                    output.push(other);
                }
                None => output.push('\\'),
            }
        } else {
            output.push(c);
        }
    }

    output
}

pub fn extract_value_fn0<F>(
    ms: Machine,
    args: Vec<TypedValue>,
    f: F,
) -> std::io::Result<(Machine, TypedValue)>
where
    F: Fn(Machine) -> std::io::Result<(Machine, TypedValue)>,
{
    match args.len() {
        0 => f(ms),
        n => throw(TypeMismatch(ArgumentsMismatched(0, n)))
    }
}

pub fn extract_value_fn1<F>(
    ms: Machine,
    args: Vec<TypedValue>,
    f: F,
) -> std::io::Result<(Machine, TypedValue)>
where
    F: Fn(Machine, &TypedValue) -> std::io::Result<(Machine, TypedValue)>,
{
    match args.as_slice() {
        [value] => f(ms, value),
        args => throw(TypeMismatch(ArgumentsMismatched(1, args.len())))
    }
}

pub fn extract_value_fn1_or_2<F>(
    ms: Machine,
    args: Vec<TypedValue>,
    f: F,
) -> std::io::Result<(Machine, TypedValue)>
where
    F: Fn(Machine, &TypedValue, Option<&TypedValue>) -> std::io::Result<(Machine, TypedValue)>,
{
    match args.as_slice() {
        [value0] => f(ms, value0, None),
        [value0, value1] => f(ms, value0, Some(value1)),
        args => throw(TypeMismatch(ArgumentsMismatched(2, args.len())))
    }
}

pub fn extract_value_fn2<F>(
    ms: Machine,
    args: Vec<TypedValue>,
    f: F,
) -> std::io::Result<(Machine, TypedValue)>
where
    F: Fn(Machine, &TypedValue, &TypedValue) -> std::io::Result<(Machine, TypedValue)>,
{
    match args.as_slice() {
        [a, b] => f(ms, a, b),
        args => throw(TypeMismatch(ArgumentsMismatched(2, args.len())))
    }
}

pub fn extract_value_fn3<F>(
    ms: Machine,
    args: Vec<TypedValue>,
    f: F,
) -> std::io::Result<(Machine, TypedValue)>
where
    F: Fn(Machine, &TypedValue, &TypedValue, &TypedValue) -> std::io::Result<(Machine, TypedValue)>,
{
    match args.as_slice() {
        [a, b, c] => f(ms, a, b, c),
        args => throw(TypeMismatch(ArgumentsMismatched(3, args.len())))
    }
}

pub fn extract_array_fn1<F>(
    ms: Machine,
    args: Vec<TypedValue>,
    f: F,
) -> std::io::Result<(Machine, TypedValue)>
where
    F: Fn(Array) -> TypedValue,
{
    extract_value_fn1(ms, args, |ms, value|
        Ok((ms, f(value.to_sequence()?.to_array()))))
}

pub fn extract_number_fn1<F>(
    ms: Machine,
    args: Vec<TypedValue>,
    f: F,
) -> std::io::Result<(Machine, TypedValue)>
where
    F: Fn(&Numbers) -> Numbers,
{
    extract_value_fn1(ms, args, |ms, value| {
        let n = pull_number(value)?;
        Ok((ms, Number(f(&n))))
    })
}

pub fn extract_number_fn2<F>(
    ms: Machine,
    args: Vec<TypedValue>,
    f: F,
) -> std::io::Result<(Machine, TypedValue)>
where
    F: Fn(&Numbers, &Numbers) -> Numbers,
{
    extract_value_fn2(ms, args, |ms, value0, value1| {
        let n = pull_number(value0)?;
        let m = pull_number(value1)?;
        Ok((ms, Number(f(&n, &m))))
    })
}

pub fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

pub fn is_quoted(s: &str) -> bool {
    (s.starts_with("\"") && s.ends_with("\"")) ||
        (s.starts_with("'") && s.ends_with("'"))
}

pub fn lift_condition(condition_expr: &Expression) -> std::io::Result<Conditions> {
    Ok(match condition_expr {
        Condition(condition) => condition.clone(),
        Literal(Boolean(yes)) => if *yes { True } else { False },
        expr => AssumedBoolean(expr.clone().into())
    })
}

pub fn maybe_a_or_b<T>(a: Option<T>, b: Option<T>) -> Option<T> {
    if a.is_some() { a } else { b }
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
    let nsecs = (millis % 1000) * 1_000_000;

    // Build a NaiveDateTime
    NaiveDateTime::from_timestamp_opt(secs, nsecs as u32)
        .map(|dt| dt.date())
}

pub fn pull_array(value: &TypedValue) -> std::io::Result<Array> {
    match value.to_sequence()? {
        TheArray(array) => Ok(array),
        TheRange(a, b, incl) => Ok(Array::from(range_to_vec(&a, &b, incl))),
        z => throw(TypeMismatch(ArrayExpected(z.unwrap_value())))
    }
}

pub fn pull_bool(value: &TypedValue) -> std::io::Result<bool> {
    match value {
        Boolean(state) => Ok(*state),
        z => throw(TypeMismatch(BooleanExpected(z.to_code())))
    }
}

pub fn pull_identifier_name(expr: &Expression) -> std::io::Result<String> {
    match expr {
        Identifier(name) => Ok(name.clone()),
        z => throw(SyntaxError(SyntaxErrors::TypeIdentifierExpected(z.to_code())))
    }
}

pub fn pull_name(expr: &Expression) -> std::io::Result<String> {
    match expr {
        Literal(StringValue(name)) => Ok(name.clone()),
        Identifier(name) => Ok(name.clone()),
        x => throw(TypeMismatch(StringExpected(x.to_code())))
    }
}

pub fn pull_number(value: &TypedValue) -> std::io::Result<Numbers> {
    match value {
        Number(n) => Ok(n.clone()),
        other => throw(TypeMismatch(UnsupportedType(NumberType(F64Kind), other.get_type())))
    }
}

pub fn pull_sequence(value: &TypedValue) -> std::io::Result<Array> {
    match value.to_sequence()? {
        TheArray(array) => Ok(array),
        TheRange(a, b, incl) => Ok(Array::from(range_to_vec(&a, &b, incl))),
        TheTuple(values) => Ok(Array::from(values)),
        z => throw(TypeMismatch(ArrayExpected(z.unwrap_value())))
    }
}

pub fn pull_string_lit(expr: &Expression) -> std::io::Result<String> {
    match expr {
        Literal(CharValue(c)) => Ok(c.to_string()),
        Literal(StringValue(s)) => Ok(s.clone()),
        x => throw(TypeMismatch(StringExpected(x.to_code())))
    }
}

pub fn pull_string(value: &TypedValue) -> std::io::Result<String> {
    match value {
        CharValue(c) => Ok(c.to_string()),
        StringValue(s) => Ok(s.clone()),
        x => throw(TypeMismatch(StringExpected(x.to_code())))
    }
}

pub fn pull_vec(value: &TypedValue) -> std::io::Result<Vec<TypedValue>> {
    match value {
        ArrayValue(array) => Ok(array.get_values()),
        TupleValue(items) => Ok(items.clone()),
        z => throw(TypeMismatch(ArrayExpected(z.to_code())))
    }
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

pub fn u8_vec_to_values(bytes: &Vec<u8>) -> Vec<TypedValue> {
    bytes.into_iter().map(|b| Number(U8Value(*b))).collect()
}

pub fn values_to_u8_vec(values: &Vec<TypedValue>) -> Vec<u8> {
    values.into_iter().map(|v| v.to_u8()).collect()
}

/// Unit tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_superscript() {
        assert_eq!(superscript(5), "⁵");
        assert_eq!(superscript(23), "²³");
        assert_eq!(superscript(960), "⁹⁶⁰");
        assert_eq!(superscript(1874), "¹⁸⁷⁴");
    }
}