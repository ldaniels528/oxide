#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// test data module
////////////////////////////////////////////////////////////////////

use crate::columns::Column;
use crate::compiler::Compiler;
use crate::data_types::DataType;
use crate::data_types::DataType::*;
use crate::dataframe::Dataframe::{ModelTable, TestReport};

use crate::connections::webservers;
use crate::errors::Errors;
use crate::file_row_collection::FileRowCollection;
use crate::interpreter::Interpreter;
use crate::namespaces::Namespace;
use crate::number_kind::NumberKind::{F64Kind, I64Kind};
use crate::numbers::Numbers::{F64Value, I64Value};
use crate::parameter::Parameter;
use crate::structures::Row;
use crate::table_renderer::TableRenderer;
use crate::test_engine::TestEngine;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::*;
use log::warn;
use serde_json::Value;
use std::fs::File;

pub fn interpret(source: &str) -> TypedValue {
    let mut interpreter = Interpreter::new();
    interpreter.evaluate(source).unwrap()
}

pub fn make_lines_from_table(table_value: TypedValue) -> Vec<String> {
    let mut lines = Vec::new();
    if let TableValue(df) = table_value { lines.extend(TableRenderer::from_dataframe(&df)) }
    lines
}

pub fn make_quote(id: usize,
                  symbol: &str,
                  exchange: &str,
                  last_sale: f64) -> Row {
    Row::new(id, vec![
        StringValue(symbol.into()),
        StringValue(exchange.into()),
        Number(F64Value(last_sale))
    ])
}

pub fn make_quote_columns() -> Vec<Column> {
    Column::from_parameters(&make_quote_parameters())
}

pub fn make_quote_parameters() -> Vec<Parameter> {
    vec![
        Parameter::new("symbol", FixedSizeType(StringType.into(), 8)),
        Parameter::new("exchange", FixedSizeType(StringType.into(), 8)),
        Parameter::new("last_sale", NumberType(F64Kind)),
    ]
}

pub fn make_scan_quote(
    id: usize,
    symbol: &str,
    exchange: &str,
    last_sale: f64,
    _active: bool,
) -> Row {
    Row::new(id, vec![
        StringValue(symbol.into()),
        StringValue(exchange.into()),
        Number(F64Value(last_sale)),
        Number(I64Value(id as i64)),
        Boolean(_active)
    ])
}

pub fn make_table_file(
    database: &str,
    schema: &str,
    name: &str,
    columns: Vec<Parameter>,
) -> (String, File, Vec<Column>, usize) {
    let table_columns = Column::from_parameters(&columns);
    let record_size = Row::compute_record_size(&table_columns);
    let ns = Namespace::new(database, schema, name);
    let file = FileRowCollection::table_file_create(&ns).unwrap();
    (ns.get_table_file_path(), file, table_columns, record_size)
}

pub async fn start_test_server_async() -> std::io::Result<u16> {
    let port = webservers::start_server_on_random_port().await?;
    Ok(port)
}

pub fn verify_bit_operator(op: &str) {
    verify_data_type(format!("5 {} 9", op).as_str(), NumberType(I64Kind));
    verify_data_type(format!("a {} b", op).as_str(), RuntimeResolvedType);
}

pub fn verify_data_type(code: &str, expected: DataType) {
    let model = Compiler::build(code).unwrap();
    assert_eq!(model.infer_type(), expected);
}

pub fn verify_exact_code(code: &str, expected: &str) {
    verify_exact_code_with(Interpreter::new(), code, expected);
}

pub fn verify_exact_code_and_inferred_type(code: &str, expected: &str, expected_type: &str) {
    let expr = Compiler::build(code).unwrap();
    let actual = Interpreter::new().invoke(&expr).unwrap();
    assert_eq!(actual.to_code(), expected);
    assert_eq!(expr.infer_type().to_code(), expected_type);
}

pub fn verify_exact_code_and_inferred_type_with(
    mut interpreter: Interpreter,
    code: &str, 
    expected: &str, 
    expected_type: &str
) -> Interpreter {
    let expr = Compiler::build(code).unwrap();
    let actual = interpreter.invoke(&expr).unwrap();
    assert_eq!(actual.to_code(), expected);
    assert_eq!(expr.infer_type().to_code(), expected_type);
    verify_code(code);
    interpreter
}

fn verify_code(code: &str) {
    let expr = Compiler::build(code).unwrap();
    if expr.to_code() != code {
        warn!("Expected:\n{}\nActual:\n{}", code, expr.to_code())
    }
}

pub async fn verify_exact_code_async(code: &str, expected: &str) {
    verify_exact_code_with_async(Interpreter::new(), code, expected).await;
}

pub async fn verify_exact_code_async_and_sync(code: &str, expected: &str) {
    verify_exact_code_with(Interpreter::new(), code, expected);
    verify_exact_code_with_async(Interpreter::new(), code, expected).await;
}

pub fn verify_exact_code_with(
    mut interpreter: Interpreter, 
    code: &str, 
    expected: &str
) -> Interpreter {
    let actual = interpreter.evaluate(code).unwrap();
    assert_eq!(actual.to_code(), expected);
    verify_code(code);
    interpreter
}

pub async fn verify_exact_code_with_async(
    mut interpreter: Interpreter,
    code: &str,
    expected: &str
) -> Interpreter {
    let actual = interpreter.evaluate_async(code).await.unwrap();
    assert_eq!(actual.to_code(), expected);
    verify_code(code);
    interpreter
}

pub fn verify_exact_json(code: &str, expected: Value) {
    verify_exact_json_with(Interpreter::new(), code, expected);
}

pub fn verify_exact_json_with(
    mut interpreter: Interpreter,
    code: &str,
    expected: Value
) -> Interpreter {
    let actual = interpreter.evaluate(code).unwrap();
    assert_eq!(actual.to_json(), expected);
    verify_code(code);
    interpreter
}

pub async fn verify_exact_json_with_async(
    mut interpreter: Interpreter,
    code: &str,
    expected: Value
) -> Interpreter {
    let actual = interpreter.evaluate_async(code).await.unwrap();
    assert_eq!(actual.to_json(), expected);
    verify_code(code);
    interpreter
}

pub fn verify_exact_report(code: &str, expected: Vec<&str>) {
    verify_exact_report_with(Interpreter::new(), code, expected);
}

pub fn verify_exact_report_with(
    mut interpreter: Interpreter,
    code: &str,
    expected: Vec<&str>,
) -> Interpreter {
    let report = interpreter.evaluate(code)
        .unwrap();
    let actual = match report {
        TableValue(TestReport(mrc, state)) => {
            let mut report = TestEngine::generate_summary(&state);
            report.push("".to_string());
            report.extend(TestEngine::generate_report(ModelTable(mrc)));
            report.iter()
                .map(|s| s.replace("\"", "'"))
                .collect::<Vec<_>>()
        }
        other => other.unwrap_value().split('\n')
            .map(|s| s.replace("\"", "'"))
            .collect::<Vec<_>>()
    };
    for s in &actual { println!("{}", s) }
    are_equal_unordered(expected, actual);
    interpreter
}

pub fn verify_exact_table(code: &str, expected: Vec<&str>) {
    verify_exact_table_with(Interpreter::new(), code, expected);
}

pub async fn verify_exact_table_async(code: &str, expected: Vec<&str>) {
    verify_exact_table_with_async(Interpreter::new(), code, expected).await;
}

pub async fn verify_exact_table_async_and_sync(code: &str, expected: Vec<&str>) {
    verify_exact_table_with(Interpreter::new(), code, expected.clone());
    verify_exact_table_with_async(Interpreter::new(), code, expected).await;
}

pub fn verify_exact_table_with(
    mut interpreter: Interpreter,
    code: &str,
    expected: Vec<&str>,
) -> Interpreter {
    let result = interpreter.evaluate(code)
        .unwrap().to_table().unwrap();
    let actual = TableRenderer::from_table_with_ids(&result).unwrap();
    for s in &actual { println!("{}", s) }
    assert_eq!(actual, expected);
    verify_code(code);
    interpreter
}

pub async fn verify_exact_table_with_async(
    mut interpreter: Interpreter,
    code: &str,
    expected: Vec<&str>,
) -> Interpreter {
    let result = interpreter.evaluate_async(code).await
        .unwrap().to_table().unwrap();
    let actual = TableRenderer::from_table_with_ids(&result).unwrap();
    for s in &actual { println!("{}", s) }
    assert_eq!(actual, expected);
    verify_code(code);
    interpreter
}

pub fn verify_exact_unwrapped(code: &str, expected: &str) {
    verify_exact_unwrapped_with(Interpreter::new(), code, expected);
}

pub fn verify_exact_unwrapped_with(
    mut interpreter: Interpreter,
    code: &str,
    expected: &str
) -> Interpreter {
    let actual = interpreter.evaluate(code).unwrap();
    assert_eq!(actual.unwrap_value(), expected);
    verify_code(code);
    interpreter
}

pub async  fn verify_exact_unwrapped_with_async(
    mut interpreter: Interpreter,
    code: &str,
    expected: &str
) -> Interpreter {
    let actual = interpreter.evaluate_async(code).await.unwrap();
    assert_eq!(actual.unwrap_value(), expected);
    verify_code(code);
    interpreter
}

pub fn verify_exact_value(code: &str, expected: TypedValue) {
    verify_exact_value_with(Interpreter::new(), code, expected);
}

pub async  fn verify_exact_value_async(code: &str, expected: TypedValue) {
    verify_exact_value_with_async(Interpreter::new(), code, expected).await;
}

pub fn verify_exact_value_whence(
    interpreter: Interpreter,
    code: &str,
    f: fn(TypedValue) -> bool,
) -> Interpreter {
    let mut my_interpreter = interpreter;
    let actual = my_interpreter.evaluate(code).unwrap();
    assert!(f(actual));
    verify_code(code);
    my_interpreter
}

pub fn verify_exact_value_where(code: &str, f: fn(TypedValue) -> bool) {
    let mut interpreter = Interpreter::new();
    let actual = TypedValue::from_result(interpreter.evaluate(code));
    println!("verify: {} -> {}", code, actual);
    assert!(f(actual));
    verify_code(code);
}

pub async fn verify_exact_value_where_async(code: &str, f: fn(TypedValue) -> bool) {
    let mut interpreter = Interpreter::new();
    let actual = TypedValue::from_result(interpreter.evaluate_async(code).await);
    println!("verify: {} -> {}", code, actual);
    assert!(f(actual));
    verify_code(code);
}

pub fn verify_exact_value_with(
    mut interpreter: Interpreter,
    code: &str,
    expected: TypedValue,
) -> Interpreter {
    match interpreter.evaluate(code) {
        Ok(actual) => assert_eq!(actual, expected),
        Err(err) => assert_eq!(ErrorValue(Errors::Exact(err.to_string())), expected),
    }
    verify_code(code);
    interpreter
}

pub async fn verify_exact_value_with_async(
    mut interpreter: Interpreter,
    code: &str,
    expected: TypedValue,
) -> Interpreter {
    let actual = interpreter.evaluate(code).unwrap();
    assert_eq!(actual, expected);
    verify_code(code);
    interpreter
}

pub fn verify_math_operator(op: &str) {
    verify_data_type(format!("5 {} 9", op).as_str(), NumberType(I64Kind));
    verify_data_type(format!("9.4 {} 3.7", op).as_str(), NumberType(F64Kind));
    verify_data_type(format!("a {} b", op).as_str(), RuntimeResolvedType);
}

/////////////////////////////////////////////////////////////
//      Private Functions
/////////////////////////////////////////////////////////////

fn are_equal_unordered(expected: Vec<&str>, actual: Vec<String>)  {
    let mut a_norm: Vec<_> = expected.iter().map(|s| s.trim().to_string()).collect();
    let mut b_norm: Vec<_> = actual.iter().map(|s| s.trim().to_string()).collect();
    a_norm.sort();
    b_norm.sort();
    if a_norm != b_norm { assert_eq!(actual, expected) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_columns() {
        let columns = make_quote_columns();
        assert_eq!(columns, Column::from_parameters(&make_quote_parameters()));
    }

    #[test]
    fn test_parameters() {
        let parameters = make_quote_parameters();
        assert_eq!(parameters, Parameter::from_columns(&make_quote_columns()));
    }
}