#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
//      Utility Functions
////////////////////////////////////////////////////////////////////

use crate::bit_array::BitSet;
use crate::data_types::DataType;
use crate::data_types::DataType::{NumberType, UUIDType};
use crate::errors::Errors::{IndexOutOfRange, SyntaxError, TypeMismatch};
use crate::errors::TypeMismatchErrors::*;
use crate::errors::{throw, SyntaxErrors};
use crate::expression::Conditions::{AssumedBoolean, False, True};
use crate::expression::Expression::{Condition, Identifier, Literal};
use crate::expression::{Conditions, Expression};
use crate::machine::Machine;
use crate::number_kind::NumberKind::{F64Kind, U128Kind, U64Kind};
use crate::numbers::Numbers;
use crate::numbers::Numbers::{U64Value, U8Value};
use crate::sequences::Sequences::{TheArray, TheRange, TheTuple};
use crate::sequences::{range_to_vec, Array, Sequence};
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{ArrayValue, BLOBStoreHandle, BitSetValue, Boolean, ByteStringValue, CharValue, Kind, Number, StringValue, TupleValue, UUIDValue, Undefined, WebSocketHandle};
use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeDelta};
use num_traits::ToPrimitive;
use shared_lib::cnv_error;
use std::future::Future;
use uuid::Uuid;

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

pub fn convert_to(items: &Vec<TypedValue>, datatype: &DataType) -> std::io::Result<Vec<TypedValue>> {
    let mut values = Vec::with_capacity(items.len());
    for item in items { values.push(item.convert_to(datatype)?); }
    Ok(values)
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

pub async fn extract_value_fn1_async<F, Fut>(
    ms: Machine,
    args: Vec<TypedValue>,
    f: F,
) -> std::io::Result<(Machine, TypedValue)>
where
    F: Fn(Machine, TypedValue) -> Fut,
    Fut: Future<Output = std::io::Result<(Machine, TypedValue)>>,
{
    match args.as_slice() {
        [a] => f(ms, a.clone()).await,
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

pub async fn extract_value_fn1_or_2_async<F, Fut>(
    ms: Machine,
    args: Vec<TypedValue>,
    f: F,
) -> std::io::Result<(Machine, TypedValue)>
where
    F: Fn(Machine, TypedValue, Option<TypedValue>) -> Fut,
    Fut: Future<Output = std::io::Result<(Machine, TypedValue)>>,
{
    match args.as_slice() {
        [value0] => f(ms, value0.clone(), None).await,
        [value0, value1] => f(ms, value0.clone(), Some(value1.clone())).await,
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

pub async fn extract_value_fn2_async<F, Fut>(
    ms: Machine,
    args: Vec<TypedValue>,
    f: F,
) -> std::io::Result<(Machine, TypedValue)>
where
    F: Fn(Machine, TypedValue, TypedValue) -> Fut,
    Fut: Future<Output = std::io::Result<(Machine, TypedValue)>>,
{
    match args.as_slice() {
        [a, b] => f(ms, a.clone(), b.clone()).await,
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

pub async fn extract_value_fn3_async<F, Fut>(
    ms: Machine,
    args: Vec<TypedValue>,
    f: F,
) -> std::io::Result<(Machine, TypedValue)>
where
    F: Fn(Machine, TypedValue, TypedValue, TypedValue) -> Fut,
    Fut: Future<Output = std::io::Result<(Machine, TypedValue)>>,
{
    match args.as_slice() {
        [a, b, c] => f(ms, a.clone(), b.clone(), c.clone()).await,
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

pub fn extract_bitset_fn1<F>(
    ms: Machine,
    args: Vec<TypedValue>,
    f: F,
) -> std::io::Result<(Machine, TypedValue)>
where
    F: Fn(Machine, BitSet) -> std::io::Result<(Machine, TypedValue)>,
{
    extract_value_fn1(ms, args, |ms, value0| {
        let bits = pull_bitset(value0.clone())?;
        f(ms, bits)
    })
}

pub fn extract_bitset_fn2<F>(
    ms: Machine,
    args: Vec<TypedValue>,
    f: F,
) -> std::io::Result<(Machine, TypedValue)>
where
    F: Fn(Machine, BitSet, TypedValue) -> std::io::Result<(Machine, TypedValue)>,
{
    extract_value_fn2(ms, args, |ms, value0, value1| {
        let bits = pull_bitset(value0.clone())?;
        f(ms, bits, value1.clone())
    })
}

pub fn extract_char_fn1<F>(
    ms: Machine,
    args: Vec<TypedValue>,
    f: F,
) -> std::io::Result<(Machine, TypedValue)>
where
    F: Fn(Machine, char) -> std::io::Result<(Machine, TypedValue)>,
{
    extract_value_fn1(ms, args, |ms, value0| {
        let c = pull_char(value0.clone())?;
        f(ms, c)
    })
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

pub fn generate_uuid() -> u128 {
    Uuid::new_v4().as_u128()
}

pub fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

pub fn is_quoted(s: &str) -> bool {
    (s.starts_with("\"") && s.ends_with("\"")) ||
        (s.starts_with("'") && s.ends_with("'"))
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

/// Converts the contents of a string to u16
pub fn parse_u16(s: &str) -> std::io::Result<u16> {
    s.parse::<u16>().map_err(|e| cnv_error!(e))
}

pub fn pull_array(value: &TypedValue) -> std::io::Result<Array> {
    match value.to_sequence()? {
        TheArray(array) => Ok(array),
        TheRange(a, b, incl) => Ok(Array::from(range_to_vec(&a, &b, incl))),
        z => throw(TypeMismatch(ArrayExpected(z.unwrap_value())))
    }
}

pub fn pull_bitset(value: TypedValue) -> std::io::Result<BitSet> {
    match value {
        BitSetValue(bits) => Ok(bits),
        z => throw(TypeMismatch(BitsetExpected(z.to_code())))
    }
}

pub fn pull_char(value: TypedValue) -> std::io::Result<char> {
    match value {
        CharValue(c) => Ok(c),
        z => throw(TypeMismatch(CharExpected(z.to_code())))
    }
}

pub fn pull_blobstore_uuid(value: &TypedValue) -> std::io::Result<u128> {
    match value {
        BLOBStoreHandle(uuid) => Ok(*uuid),
        other => throw(TypeMismatch(UnsupportedType(NumberType(U128Kind), other.get_type())))
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

pub fn pull_kind(value: &TypedValue) -> std::io::Result<DataType> {
    match value {
        Kind(kind) => Ok(kind.clone()),
        other => throw(TypeMismatch(UnsupportedType(NumberType(F64Kind), other.get_type())))
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
        UUIDValue(uuid) => Ok(Numbers::U128Value(*uuid)),
        other => throw(TypeMismatch(UnsupportedType(NumberType(F64Kind), other.get_type())))
    }
}

pub fn pull_number_lit(expr: &Expression) -> std::io::Result<Numbers> {
    match expr {
        Literal(Number(n)) => Ok(n.clone()),
        x => throw(TypeMismatch(NumericValueExpected(x.to_code())))
    }
}

pub fn pull_number_u64(value: &TypedValue) -> std::io::Result<u64> {
    match value {
        CharValue(c) => Ok(unicode_char_to_u64(*c)),
        Number(n) => Ok(n.to_u64()),
        other => throw(TypeMismatch(UnsupportedType(NumberType(U64Kind), other.get_type())))
    }
}

pub fn pull_number_u64_vec(value: &TypedValue) -> std::io::Result<Vec<u64>> {
    match value {
        ArrayValue(array) => Ok({
            let mut result = Vec::new();
            for item in array.get_values() {
                result.push(pull_number_u64(&item)?);
            }
            result
        }),
        BitSetValue(bits) => Ok(bits.to_vec()),
        ByteStringValue(bytes) =>
            Ok(bytes.to_vec().iter()
                .map(|n| n.to_u64().unwrap_or(0))
                .collect::<Vec<_>>()),
        StringValue(s) => Ok(s.chars().map(|c| unicode_char_to_u64(c)).collect::<Vec<_>>()),
        other => pull_number_u64(other).map(|n| vec![n])
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

pub fn pull_uuid(value: &TypedValue) -> std::io::Result<u128> {
    match value {
        BLOBStoreHandle(uuid) => Ok(*uuid),
        UUIDValue(uuid) => Ok(*uuid),
        WebSocketHandle(uuid) => Ok(*uuid),
        other => throw(TypeMismatch(UnsupportedType(UUIDType, other.get_type())))
    }
}

pub fn pull_vec(value: &TypedValue) -> std::io::Result<Vec<TypedValue>> {
    match value {
        ArrayValue(array) => Ok(array.get_values()),
        TupleValue(items) => Ok(items.clone()),
        z => throw(TypeMismatch(ArrayExpected(z.to_code())))
    }
}

pub fn pull_ws_handle(value: &TypedValue) -> std::io::Result<u128> {
    match value {
        WebSocketHandle(uuid) => Ok(*uuid),
        z => throw(TypeMismatch(ArrayExpected(z.to_code())))
    }
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

pub fn u64_vec_to_values(values: &Vec<u64>) -> Vec<TypedValue> {
    values.into_iter().map(|n| Number(U64Value(*n))).collect::<Vec<_>>()
}

pub fn u64_vec_to_u8_vec(values: &Vec<u64>) -> Vec<u8> {
    values.into_iter().map(|v| v.to_u8().unwrap_or(0)).collect::<Vec<_>>()
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

pub fn values_to_bitset(items: Vec<TypedValue>) -> std::io::Result<TypedValue> {
    // collect the u64 values
    let mut values: Vec<u64> = Vec::with_capacity(items.len());
    for item in items {
       let array = match item {
           CharValue(c) => vec![unicode_char_to_u64(c)],
           StringValue(s) => s.chars().map(|c| unicode_char_to_u64(c)).collect(),
            _ => pull_number_u64_vec(&item)?
        };
        values.extend(array)
    }
    values.sort();

    // create the bitset
    let mut bits = BitSet::new(values.len(), values[0]);
    bits.add(values.as_slice());
    Ok(BitSetValue(bits))
}

pub fn values_to_u8_vec(values: &Vec<TypedValue>) -> Vec<u8> {
    values.into_iter().map(|v| v.to_u8()).collect()
}

/// Unit tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_base36() {
       assert_eq!(decode_base36("C3PO").unwrap(), 564684);
    }

    #[test]
    fn test_encode_base36() {
        assert_eq!(encode_base36(564684).unwrap(), "C3PO");
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
}