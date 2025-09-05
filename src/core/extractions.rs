#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Extractions - helper utility for extracting values
////////////////////////////////////////////////////////////////////

use crate::connections::ConnectionTypes::BLOBStoreHandleType;
use crate::connections::Connections::{BLOBStoreHandle, WebSocketHandle};
use crate::data_types::DataType;
use crate::data_types::DataType::{ConnectionType, NumberType, UUIDType};
use crate::dataframe::Dataframe;
use crate::errors::Errors::{SyntaxError, TypeMismatch};
use crate::errors::TypeMismatchErrors::*;
use crate::errors::{throw, SyntaxErrors};
use crate::expression::Expression;
use crate::expression::Expression::{FunctionCall, Identifier, Literal};
use crate::machine::Machine;
use crate::number_kind::NumberKind::{F64Kind, U64Kind};
use crate::numbers::Numbers;
use crate::sequences::Sequences::{TheArray, TheRange, TheTuple};
use crate::sequences::{range_to_vec, Array, Sequence};
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::*;
use crate::utils::unicode_char_to_u64;
use num_traits::ToPrimitive;
use std::future::Future;
use std::ops::Deref;

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
        Ok((ms, Number(f(&pull_number(value)?))))
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
        Ok((ms, Number(f(&pull_number(value0)?, &pull_number(value1)?))))
    })
}

pub fn extract_table_fn1<F>(
    ms: Machine,
    args: Vec<TypedValue>,
    f: F,
) -> std::io::Result<(Machine, TypedValue)>
where
    F: Fn(Machine, Dataframe) -> std::io::Result<(Machine, TypedValue)>,
{
    match args.as_slice() {
        [value] => f(ms, value.to_dataframe()?),
        args => throw(TypeMismatch(ArgumentsMismatched(1, args.len())))
    }
}

pub fn extract_table_fn3<F>(
    ms: Machine,
    args: Vec<TypedValue>,
    f: F,
) -> std::io::Result<(Machine, TypedValue)>
where
    F: Fn(Machine, Dataframe, &TypedValue, &TypedValue) -> std::io::Result<(Machine, TypedValue)>,
{
    match args.as_slice() {
        [a, b, c] => f(ms, a.to_dataframe()?, b, c),
        args => throw(TypeMismatch(ArgumentsMismatched(3, args.len())))
    }
}

pub fn extract_table_fn2<F>(
    ms: Machine,
    args: Vec<TypedValue>,
    f: F,
) -> std::io::Result<(Machine, TypedValue)>
where
    F: Fn(Machine, Dataframe, &TypedValue) -> std::io::Result<(Machine, TypedValue)>,
{
    match args.as_slice() {
        [a, b] => f(ms, a.to_dataframe()?, b),
        args => throw(TypeMismatch(ArgumentsMismatched(2, args.len())))
    }
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

pub fn find_name(expr: &Expression) -> Option<String> {
    match expr {
        FunctionCall { fx, .. } =>
            match fx.deref() {
                Identifier(name) => Some(name.into()),
                _ => None,
            },
        Identifier(name) => Some(name.into()),
        _ => None,
    }
}

pub fn find_name_and_args(expr: &Expression) -> Option<(String, Vec<Expression>)> {
    match expr {
        FunctionCall { fx, args } =>
            match fx.deref() {
                Identifier(fx_name) => Some((fx_name.into(), args.clone())),
                _ => None
            }
        Identifier(fx_name) => Some((fx_name.into(), vec![])),
        _ => None
    }
}

pub fn find_string(expr: &Expression) -> Option<String> {
    match expr {
        Literal(StringValue(s)) => Some(s.clone()),
        _ => None,
    }
}

pub fn pull_array(value: &TypedValue) -> std::io::Result<Array> {
    match value.to_sequence()? {
        TheArray(array) => Ok(array),
        TheRange(a, b, incl) => Ok(Array::from(range_to_vec(&a, &b, incl))),
        z => throw(TypeMismatch(ArrayExpected(z.unwrap_value())))
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
        Connection(BLOBStoreHandle(uuid)) => Ok(*uuid),
        Number(Numbers::U128Value(uuid)) => Ok(*uuid),
        UUIDValue(uuid) => Ok(*uuid),
        other => throw(TypeMismatch(UnsupportedType(ConnectionType(BLOBStoreHandleType), other.get_type())))
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
    match value.to_number() {
        Some(number) => Ok(number),
        None => throw(TypeMismatch(UnsupportedType(NumberType(F64Kind), value.get_type())))
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

pub fn pull_strings(values: &[TypedValue]) -> std::io::Result<Vec<String>> {
    let mut result = Vec::with_capacity(values.len());
    for value in values {
        result.push(pull_string(value)?);
    }
    Ok(result)
}

pub fn pull_uuid(value: &TypedValue) -> std::io::Result<u128> {
    match value {
        Connection(BLOBStoreHandle(uuid)) => Ok(*uuid),
        Connection(WebSocketHandle(uuid)) => Ok(*uuid),
        UUIDValue(uuid) => Ok(*uuid),
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
