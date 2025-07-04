#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
//      Utility Functions
////////////////////////////////////////////////////////////////////

use crate::data_types::DataType;
use crate::data_types::DataType::NumberType;
use crate::dataframe::Dataframe;
use crate::errors::Errors::{Exact, SyntaxError, TypeMismatch};
use crate::errors::TypeMismatchErrors::*;
use crate::errors::{throw, SyntaxErrors};
use crate::expression::Conditions::{AssumedBoolean, False, True};
use crate::expression::Expression::{Condition, Identifier, Literal};
use crate::expression::{Conditions, Expression};
use crate::machine::Machine;
use crate::number_kind::NumberKind::F64Kind;
use crate::numbers::Numbers;
use crate::sequences::Sequences::{TheArray, TheRange};
use crate::sequences::{range_to_vec, Array, Sequence};
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{ArrayValue, Boolean, CharValue, Kind, Number, StringValue, TableValue, TupleValue};
use chrono::TimeDelta;
use num_traits::ToPrimitive;

pub fn compute_time_millis(dt: TimeDelta) -> f64 {
    match dt.num_nanoseconds() {
        Some(nano) => nano.to_f64().map(|t| t / 1e+6).unwrap_or(0.),
        None => dt.num_milliseconds().to_f64().unwrap_or(0.)
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

pub fn extract_array_fn2<F>(
    ms: Machine,
    args: Vec<TypedValue>,
    f: F,
) -> std::io::Result<(Machine, TypedValue)>
where
    F: Fn(Array, &TypedValue) -> TypedValue,
{
    extract_value_fn2(ms, args, |ms, value0, value1|
        match value0.to_sequence()? {
            TheArray(array) => Ok((ms, f(array, value1))),
            other => throw(TypeMismatch(ArrayExpected(other.unwrap_value())))
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

pub fn lift_condition(condition_expr: &Expression) -> std::io::Result<Conditions> {
    Ok(match condition_expr {
        Condition(condition) => condition.clone(),
        Literal(Boolean(yes)) => if *yes { True } else { False },
        expr => AssumedBoolean(expr.clone().into())
    })
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

pub fn pull_table(value: &TypedValue) -> std::io::Result<Dataframe> {
    match value {
        TableValue(df) => Ok(df.clone()),
        x => throw(TypeMismatch(TableExpected(x.to_code())))
    }
}

pub fn pull_type(value: &TypedValue) -> std::io::Result<DataType> {
    match value {
        Kind(data_type) => Ok(data_type.clone()),
        x => throw(Exact(format!("Expected type near {}", x.to_code())))
    }
}

pub fn pull_vec(value: &TypedValue) -> std::io::Result<Vec<TypedValue>> {
    match value {
        ArrayValue(array) => Ok(array.get_values()),
        TupleValue(items) => Ok(items.clone()),
        z => throw(TypeMismatch(ArrayExpected(z.to_code())))
    }
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