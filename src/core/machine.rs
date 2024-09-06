////////////////////////////////////////////////////////////////////
// state machine module
////////////////////////////////////////////////////////////////////

use std::{fs, thread};
use std::collections::HashMap;
use std::convert::From;
use std::fmt::format;
use std::fs::File;
use std::io::Read;
use std::ops::Deref;
use std::process::Output;

use log::{error, info};
use reqwest::{Client, RequestBuilder};
use reqwest::multipart::{Form, Part};
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;

use shared_lib::fail;

use crate::compiler::{fail_expr, fail_unexpected, fail_value};
use crate::compiler::Compiler;
use crate::cursor::Cursor;
use crate::dataframe_config::{DataFrameConfig, HashIndexConfig};
use crate::dataframes::DataFrame;
use crate::expression::{Expression, Infrastructure, Mutation, Queryable, UNDEFINED};
use crate::expression::CreationEntity::{IndexEntity, TableEntity};
use crate::expression::Expression::{ArrayLiteral, AsValue, ColumnSet, HTTP, Literal, Ns, Perform, SERVE, SetVariable, Variable, Via};
use crate::expression::Infrastructure::Declare;
use crate::expression::MutateTarget::{IndexTarget, TableTarget};
use crate::file_row_collection::FileRowCollection;
use crate::model_row_collection::ModelRowCollection;
use crate::namespaces::Namespace;
use crate::rest_server::SharedState;
use crate::row_collection::RowCollection;
use crate::rows::Row;
use crate::server::ColumnJs;
use crate::structure::Structure;
use crate::table_columns::TableColumn;
use crate::typed_values::{BackDoorFunction, TypedValue};
use crate::typed_values::TypedValue::*;
use crate::web_routes;

/// Represents the state of the machine.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Machine {
    stack: Vec<TypedValue>,
    variables: HashMap<String, TypedValue>,
}

impl Machine {

    ////////////////////////////////////////////////////////////////
    //  constructors
    ////////////////////////////////////////////////////////////////

    /// creates a new empty state machine
    pub fn empty() -> Self {
        Self::construct(Vec::new(), HashMap::new())
    }

    /// creates a new state machine prepopulated with platform functions
    pub fn new() -> Self {
        Self::empty()
            .with_variable("assert", BackDoor(BackDoorFunction::Assert))
            .with_variable("eval", BackDoor(BackDoorFunction::Eval))
            .with_variable("format", BackDoor(BackDoorFunction::Format))
            .with_variable("iff", BackDoor(BackDoorFunction::If))
            .with_variable("matches", BackDoor(BackDoorFunction::Matches))
            .with_variable("__reset", BackDoor(BackDoorFunction::Reset))
            .with_variable("println", BackDoor(BackDoorFunction::StdOut))
            .with_variable("stderr", BackDoor(BackDoorFunction::StdErr))
            .with_variable("stdout", BackDoor(BackDoorFunction::StdOut))
            .with_variable("syscall", BackDoor(BackDoorFunction::SysCall))
            .with_variable("to_csv", BackDoor(BackDoorFunction::ToCSV))
            .with_variable("to_json", BackDoor(BackDoorFunction::ToJSON))
            .with_variable("type_of", BackDoor(BackDoorFunction::TypeOf))
            .with_variable("__variables", BackDoor(BackDoorFunction::Variables))
    }

    /// creates a new state machine
    fn construct(stack: Vec<TypedValue>, variables: HashMap<String, TypedValue>) -> Self {
        Self { stack, variables }
    }

    ////////////////////////////////////////////////////////////////
    //  static methods
    ////////////////////////////////////////////////////////////////

    fn enrich_request(
        builder: RequestBuilder,
        body_opt: Option<String>,
    ) -> RequestBuilder {
        let builder = builder
            .header("Content-Type", "application/json");
        let builder = match body_opt {
            Some(body) => builder.body(body),
            None => builder
        };
        builder
    }

    fn error_expr(message: impl Into<String>, expr: &Expression) -> TypedValue {
        ErrorValue(format!("{} near {}", message.into(), expr.to_code()))
    }

    fn error_unexpected(expected_type: impl Into<String>, value: &TypedValue) -> TypedValue {
        ErrorValue(format!("Expected a(n) {}, but got {:?}", expected_type.into(), value))
    }

    fn extract_string_tuples(value: TypedValue) -> std::io::Result<Vec<(String, String)>> {
        Self::extract_value_tuples(value)
            .map(|values| values.iter()
                .map(|(k, v)| (k.to_string(), v.unwrap_value()))
                .collect())
    }

    fn extract_value_tuples(value: TypedValue) -> std::io::Result<Vec<(String, TypedValue)>> {
        match value {
            JSONValue(values) => {
                Ok(values.iter()
                    .map(|(k, v)| (k.to_string(), v.to_owned()))
                    .collect()
                )
            }
            z => return fail_value("Type mismatch", &z),
        }
    }

    fn orchestrate_rc(
        machine: Self,
        table: TypedValue,
        f: fn(Self, Box<dyn RowCollection>) -> std::io::Result<(Self, TypedValue)>,
    ) -> std::io::Result<(Self, TypedValue)> {
        use TypedValue::*;
        match table {
            ErrorValue(msg) => Ok((machine, ErrorValue(msg))),
            NamespaceValue(d, s, n) => {
                let ns = Namespace::new(d, s, n);
                let frc = FileRowCollection::open(&ns)?;
                f(machine, Box::new(frc))
            }
            TableValue(mrc) => f(machine, Box::new(mrc)),
            z =>
                Ok((machine, ErrorValue(format!("Type mismatch: expected an iterable near {}", z))))
        }
    }

    fn orchestrate_io(
        machine: Self,
        table: TypedValue,
        fields: &Vec<Expression>,
        values: &Vec<Expression>,
        condition: &Option<Box<Expression>>,
        limit: TypedValue,
        f: fn(Self, &mut DataFrame, &Vec<Expression>, &Vec<Expression>, &Option<Box<Expression>>, TypedValue) -> std::io::Result<(Self, TypedValue)>,
    ) -> std::io::Result<(Self, TypedValue)> {
        use TypedValue::*;
        match table {
            NamespaceValue(d, s, n) => {
                let ns = Namespace::new(d, s, n);
                let mut df = DataFrame::load(ns)?;
                f(machine, &mut df, fields, values, condition, limit)
            }
            TableValue(mrc) => {
                let mut df = DataFrame::from_row_collection(Box::new(mrc));
                f(machine, &mut df, fields, values, condition, limit)
            }
            z =>
                Ok((machine, ErrorValue(format!("Type mismatch: expected an iterable near {}", z))))
        }
    }

    fn split(row: &Row) -> (Vec<Expression>, Vec<Expression>) {
        let my_fields = row.get_columns().iter()
            .map(|tc| Variable(tc.get_name().to_string()))
            .collect::<Vec<Expression>>();
        let my_values = row.get_values().iter()
            .map(|v| Literal(v.to_owned()))
            .collect::<Vec<Expression>>();
        (my_fields, my_values)
    }

    fn split_first<T>(vec: Vec<T>) -> Option<(T, Vec<T>)> {
        let mut iter = vec.into_iter();
        iter.next().map(|first| (first, iter.collect()))
    }

    ////////////////////////////////////////////////////////////////
    //  instance methods
    ////////////////////////////////////////////////////////////////

    fn assume_table_value_or_reference<A>(
        &self,
        table: &TypedValue,
        f: fn(Box<dyn RowCollection>) -> std::io::Result<A>,
    ) -> std::io::Result<A> {
        match table {
            NamespaceValue(d, s, n) => {
                let ns = Namespace::new(d, s, n);
                let frc = FileRowCollection::open(&ns)?;
                f(Box::new(frc))
            }
            TableValue(mrc) => f(Box::new(mrc.to_owned())),
            z =>
                return fail_value(format!("{} is not a table", z), z)
        }
    }

    fn assume_dataframe<A>(
        &self,
        table: &TypedValue,
        f: fn(DataFrame) -> std::io::Result<A>,
    ) -> std::io::Result<A> {
        match table {
            NamespaceValue(d, s, n) => {
                let ns = Namespace::new(d, s, n);
                f(DataFrame::load(ns)?)
            }
            TableValue(mrc) =>
                f(DataFrame::new(Box::new(mrc.to_owned()))),
            z =>
                return fail_value(format!("{} is not a table", z), z)
        }
    }

    fn convert_to_csv_or_json(
        &self,
        value: &TypedValue,
        is_csv: bool,
    ) -> (Self, TypedValue) {
        match value {
            NamespaceValue(d, s, n) =>
                match FileRowCollection::open(&Namespace::new(d, s, n)) {
                    Ok(frc) => {
                        let rc = Box::new(frc);
                        if is_csv { self.evaluate_to_csv(rc) } else { self.evaluate_to_json(rc) }
                    }
                    Err(err) => (self.to_owned(), ErrorValue(err.to_string()))
                }
            TableValue(mrc) => {
                let rc = Box::new(mrc.to_owned());
                if is_csv { self.evaluate_to_csv(rc) } else { self.evaluate_to_json(rc) }
            }
            _ => (self.to_owned(), ErrorValue(format!("Cannot convert to {}", if is_csv { "CSV" } else { "JSON" })))
        }
    }

    /// evaluates the specified [Expression]; returning a [TypedValue] result.
    pub fn evaluate(
        &self,
        expression: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        use crate::expression::Expression::*;
        match expression {
            And(a, b) =>
                self.evaluate_expr_2(a, b, |aa, bb| aa.and(&bb).unwrap_or(Undefined)),
            ArrayLiteral(items) => self.evaluate_array(items),
            AsValue(name, expr) => {
                let (machine, tv) = self.evaluate(expr)?;
                Ok((machine.with_variable(name, tv.to_owned()), tv))
            }
            Between(a, b, c) =>
                self.evaluate_expr_3(a, b, c, |aa, bb, cc| Boolean((aa >= bb) && (aa <= cc))),
            Betwixt(a, b, c) =>
                self.evaluate_expr_3(a, b, c, |aa, bb, cc| Boolean((aa >= bb) && (aa < cc))),
            BitwiseAnd(a, b) =>
                self.evaluate_expr_2(a, b, |aa, bb| aa & bb),
            BitwiseOr(a, b) =>
                self.evaluate_expr_2(a, b, |aa, bb| aa | bb),
            BitwiseXor(a, b) =>
                self.evaluate_expr_2(a, b, |aa, bb| aa ^ bb),
            CodeBlock(ops) => self.evaluate_scope(ops),
            ColumnSet(columns) => {
                let machine = self.to_owned();
                let values = columns.iter()
                    .map(|c| machine.variables.to_owned().get(c.get_name()).map(|c| c.to_owned())
                        .unwrap_or(Undefined))
                    .collect::<Vec<TypedValue>>();
                Ok((machine, Array(values)))
            }
            Contains(a, b) => self.evaluate_contains(a, b),
            Divide(a, b) =>
                self.evaluate_expr_2(a, b, |aa, bb| aa / bb),
            ElementAt(a, b) => self.evaluate_index_of_collection(a, b),
            Equal(a, b) =>
                self.evaluate_expr_2(a, b, |aa, bb| Boolean(aa == bb)),
            Factorial(a) => self.evaluate_expr_1(a, |aa| aa.factorial().unwrap_or(Undefined)),
            Feature { title, scenarios } => self.evaluate_feature(title, scenarios),
            From(src) => self.evaluate_table_row_query(src, &UNDEFINED, Undefined),
            FunctionCall { fx, args } =>
                self.evaluate_function_call(fx, args),
            GreaterThan(a, b) =>
                self.evaluate_expr_2(a, b, |aa, bb| Boolean(aa > bb)),
            GreaterOrEqual(a, b) =>
                self.evaluate_expr_2(a, b, |aa, bb| Boolean(aa >= bb)),
            HTTP { .. } =>
                Ok((self.to_owned(), ErrorValue("HTTP is only supported in an async context".to_string()))),
            If { condition, a, b } =>
                self.evaluate_if_then_else(condition, a, b),
            Include(path) => self.evaluate_include(path),
            Inquire(q) => self.evaluate_inquiry(q),
            JSONLiteral(items) => self.evaluate_json(items),
            LessThan(a, b) =>
                self.evaluate_expr_2(a, b, |aa, bb| Boolean(aa < bb)),
            LessOrEqual(a, b) =>
                self.evaluate_expr_2(a, b, |aa, bb| Boolean(aa <= bb)),
            Literal(value) => Ok((self.to_owned(), value.to_owned())),
            Minus(a, b) =>
                self.evaluate_expr_2(a, b, |aa, bb| aa - bb),
            Modulo(a, b) =>
                self.evaluate_expr_2(a, b, |aa, bb| aa % bb),
            Multiply(a, b) =>
                self.evaluate_expr_2(a, b, |aa, bb| aa * bb),
            MustAck(a) => self.evaluate_directive_ack(a),
            MustDie(a) => self.evaluate_directive_die(a),
            MustIgnoreAck(a) => self.evaluate_directive_ignore_ack(a),
            MustNotAck(a) => self.evaluate_directive_not_ack(a),
            Mutate(m) => self.evaluate_mutation(m),
            Neg(a) => self.evaluate_neg(a),
            Not(a) => self.evaluate_expr_1(a, |aa| !aa),
            NotEqual(a, b) =>
                self.evaluate_expr_2(a, b, |aa, bb| Boolean(aa != bb)),
            Ns(a) => self.evaluate_table_ns(a),
            Or(a, b) =>
                self.evaluate_expr_2(a, b, |aa, bb| aa.or(&bb).unwrap_or(Undefined)),
            Perform(i) => self.evaluate_infrastructure(i),
            Plus(a, b) =>
                self.evaluate_expr_2(a, b, |aa, bb| aa + bb),
            Pow(a, b) =>
                self.evaluate_expr_2(a, b, |aa, bb| aa.pow(&bb).unwrap_or(Undefined)),
            Range(a, b) =>
                self.evaluate_expr_2(a, b, |aa, bb| aa.range(&bb).unwrap_or(Undefined)),
            Return(a) => {
                let (machine, result) = self.evaluate_array(a)?;
                Ok((machine, result))
            }
            Scenario { .. } => Ok((self.to_owned(), ErrorValue("Scenario should not be called directly".into()))),
            SERVE(a) => self.evaluate_serve(a),
            SetVariable(name, expr) => {
                let (machine, value) = self.evaluate(expr)?;
                Ok((machine.set(name, value), Ack))
            }
            ShiftLeft(a, b) =>
                self.evaluate_expr_2(a, b, |aa, bb| aa << bb),
            ShiftRight(a, b) =>
                self.evaluate_expr_2(a, b, |aa, bb| aa >> bb),
            TupleLiteral(values) => self.evaluate_array(values),
            Variable(name) => Ok((self.to_owned(), self.get(&name).unwrap_or(Undefined))),
            Via(src) => self.evaluate_table_row_query(src, &UNDEFINED, Undefined),
            While { condition, code } =>
                self.evaluate_while(condition, code),
        }
    }

    /// evaluates the specified [Expression]; returning an array ([TypedValue]) result.
    pub fn evaluate_array(
        &self,
        ops: &Vec<Expression>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, results) = ops.iter()
            .fold((self.to_owned(), Vec::new()),
                  |(machine, mut array), op| match machine.evaluate(op) {
                      Ok((machine, tv)) => {
                          array.push(tv);
                          (machine, array)
                      }
                      Err(err) => panic!("{}", err.to_string())
                  });
        Ok((machine, Array(results)))
    }

    /// evaluates the specified [Expression]; returning an array ([String]) result.
    pub fn evaluate_as_atoms(
        &self,
        ops: &Vec<Expression>,
    ) -> std::io::Result<(Self, Vec<String>)> {
        let (machine, results, errors) = ops.iter()
            .fold((self.to_owned(), Vec::new(), Vec::new()),
                  |(ma, mut array, mut errors), op| match op {
                      Variable(name) => {
                          array.push(name.to_owned());
                          (ma, array, errors)
                      }
                      expr => {
                          errors.push(ErrorValue(format!("Expected a column, got \"{}\" instead", expr)));
                          (ma, array, errors)
                      }
                  });

        // if errors occurred ...
        if !errors.is_empty() {
            fail(errors.iter()
                .map(|v| v.unwrap_value())
                .collect::<Vec<_>>()
                .join("\n"))
        } else {
            Ok((machine, results))
        }
    }

    /// asynchronously evaluates the specified [Expression]; returning a [TypedValue] result.
    pub async fn evaluate_async(
        &self,
        expression: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        match expression {
            AsValue(name, expr) => {
                let (machine, tv) = self.evaluate(expr)?;
                Ok((machine.with_variable(name, tv.to_owned()), tv))
            }
            HTTP { method, url, body, headers, multipart } =>
                self.evaluate_http(method, url, body, headers, multipart).await,
            SetVariable(name, expr) => {
                let (machine, value) = self.evaluate(expr)?;
                Ok((machine.set(name, value), Ack))
            }
            other => self.evaluate(other)
        }
    }


    fn evaluate_back_door(
        &self,
        bdf: &BackDoorFunction,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Self, TypedValue)> {
        match bdf {
            BackDoorFunction::Assert =>
                self.evaluate_back_door_fn1(args, |ms, value| match value {
                    ErrorValue(msg) => (ms, ErrorValue(msg.to_owned())),
                    Boolean(false) => (ms, ErrorValue("Assertion was false".to_owned())),
                    z => (ms, z.to_owned())
                }),
            BackDoorFunction::Eval =>
                self.evaluate_back_door_fn1(args, |ms, value| match value {
                    StringValue(ql) =>
                        match Compiler::compile_script(ql.as_str()) {
                            Ok(opcode) =>
                                match ms.evaluate(&opcode) {
                                    Ok((machine, tv)) => (machine, tv),
                                    Err(err) => (ms, ErrorValue(err.to_string()))
                                }
                            Err(err) => (ms, ErrorValue(err.to_string()))
                        }
                    x => (ms, ErrorValue(format!("Type mismatch - expected String, got {}", x.get_type_name())))
                }),
            BackDoorFunction::Format => Ok(self.evaluate_format_string(args)),
            BackDoorFunction::If =>
                self.evaluate_back_door_fn3(args, |ms, a, b, c| (ms, (if a.is_ok() { b } else { c }).to_owned())),
            BackDoorFunction::Matches =>
                self.evaluate_back_door_fn2(args, |ms, a, b| (ms, a.matches(b))),
            BackDoorFunction::Reset =>
                self.evaluate_back_door_fn0(args, |ms| (Machine::new(), Ack)),
            BackDoorFunction::StdErr =>
                self.evaluate_back_door_fn1(args, |ms, value| {
                    println!("{}", value.unwrap_value());
                    (ms, Ack)
                }),
            BackDoorFunction::StdOut =>
                self.evaluate_back_door_fn1(args, |ms, value| {
                    println!("{}", value.unwrap_value());
                    (ms, Ack)
                }),
            BackDoorFunction::SysCall => self.evaluate_syscall(args),
            BackDoorFunction::ToCSV =>
                self.evaluate_back_door_fn1(args, |ms, value| ms.convert_to_csv_or_json(value, true)),
            BackDoorFunction::ToJSON =>
                self.evaluate_back_door_fn1(args, |ms, value| ms.convert_to_csv_or_json(value, false)),
            BackDoorFunction::TypeOf =>
                self.evaluate_back_door_fn1(args, |ms, value| {
                    (ms, StringValue(value.get_type_name()))
                }),
            BackDoorFunction::Variables =>
                self.evaluate_back_door_fn0(args, |ms| (ms.to_owned(), TableValue(ms.get_variables()))),
        }
    }

    fn evaluate_back_door_fn0(
        &self,
        args: Vec<TypedValue>,
        f: fn(Self) -> (Self, TypedValue),
    ) -> std::io::Result<(Self, TypedValue)> {
        let ms = self.to_owned();
        match args.len() {
            0 => Ok(f(ms)),
            n => Ok((ms, ErrorValue(format!("{} arguments passed, where none were expected", n))))
        }
    }

    fn evaluate_back_door_fn1(
        &self,
        args: Vec<TypedValue>,
        f: fn(Self, &TypedValue) -> (Self, TypedValue),
    ) -> std::io::Result<(Self, TypedValue)> {
        let ms = self.to_owned();
        match args.as_slice() {
            [value] => Ok(f(ms, value)),
            _ => Ok((ms, ErrorValue(format!("argument mismatch: expected 1 not {} parameters", args.len()))))
        }
    }

    fn evaluate_back_door_fn2(
        &self,
        args: Vec<TypedValue>,
        f: fn(Self, &TypedValue, &TypedValue) -> (Self, TypedValue),
    ) -> std::io::Result<(Self, TypedValue)> {
        let ms = self.to_owned();
        match args.as_slice() {
            [a, b] => Ok(f(ms, a, b)),
            _ => Ok((ms, ErrorValue(format!("argument mismatch: expected 2 not {} parameters", args.len()))))
        }
    }

    fn evaluate_back_door_fn3(
        &self,
        args: Vec<TypedValue>,
        f: fn(Self, &TypedValue, &TypedValue, &TypedValue) -> (Self, TypedValue),
    ) -> std::io::Result<(Self, TypedValue)> {
        let ms = self.to_owned();
        match args.as_slice() {
            [a, b, c] => Ok(f(ms, a, b, c)),
            _ => Ok((ms, ErrorValue(format!("argument mismatch: expected 3 not {} parameters", args.len()))))
        }
    }

    fn evaluate_contains(
        &self,
        a: &Expression,
        b: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, a) = self.evaluate(a)?;
        let (machine, b) = machine.evaluate(b)?;
        Ok((machine, a.contains(&b)))
    }

    fn evaluate_directive_ack(
        &self,
        expr: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        info!("{}", expr.to_code());
        let (machine, value) = self.evaluate(expr)?;
        match value {
            v if v.is_ok() => Ok((machine, v.to_owned())),
            v =>
                Ok((machine, ErrorValue(format!("Expected true, Table(..) or RowsAffected(_ >= 1), but got {}", &v))))
        }
    }

    fn evaluate_directive_die(
        &self,
        expr: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        info!("{}", expr.to_code());
        let (machine, value) = self.evaluate(expr)?;
        Ok((machine, ErrorValue(value.unwrap_value())))
    }

    fn evaluate_directive_ignore_ack(
        &self,
        expr: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        info!("{}", expr.to_code());
        let (machine, _) = self.evaluate(expr)?;
        Ok((machine, Ack))
    }

    fn evaluate_directive_not_ack(
        &self,
        expr: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        info!("{}", expr.to_code());
        let (machine, value) = self.evaluate(expr)?;
        match value {
            v if v.is_ok() =>
                Ok((machine, ErrorValue(format!("Expected a non-success value, but got {}", &v)))),
            v => Ok((machine, v))
        }
    }

    /// evaluates expression `a` then applies function `f`. ex: f(a)
    fn evaluate_expr_1(
        &self,
        a: &Expression,
        f: fn(TypedValue) -> TypedValue,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, aa) = self.evaluate(a)?;
        Ok((machine, f(aa)))
    }

    /// evaluates expressions `a` and `b` then applies function `f`. ex: f(a, b)
    fn evaluate_expr_2(
        &self,
        a: &Expression,
        b: &Expression,
        f: fn(TypedValue, TypedValue) -> TypedValue,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, aa) = self.evaluate(a)?;
        let (machine, bb) = machine.evaluate(b)?;
        Ok((machine, f(aa, bb)))
    }

    /// evaluates expressions `a`, `b` and `c` then applies function `f`. ex: f(a, b, c)
    fn evaluate_expr_3(
        &self,
        a: &Expression,
        b: &Expression,
        c: &Expression,
        f: fn(TypedValue, TypedValue, TypedValue) -> TypedValue,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, aa) = self.evaluate(a)?;
        let (machine, bb) = machine.evaluate(b)?;
        let (machine, cc) = machine.evaluate(c)?;
        Ok((machine, f(aa, bb, cc)))
    }

    fn evaluate_feature(
        &self,
        title: &Box<Expression>,
        scenarios: &Vec<Expression>,
    ) -> std::io::Result<(Self, TypedValue)> {
        // create a table and capture function to store the verification report
        let verification_columns = TableColumn::from_columns(&vec![
            ColumnJs::new("level", "u16", None),
            ColumnJs::new("item", "String(256)", None),
            ColumnJs::new("passed", "Boolean", None),
            ColumnJs::new("result", "String(256)", None),
        ])?;
        let mut report = ModelRowCollection::new(verification_columns.to_owned());
        let mut capture = |level: u16, text: String, passed: bool, result: TypedValue| {
            report.push_row(Row::new(0, verification_columns.to_owned(), vec![
                UInt16Value(level), StringValue(text), Boolean(passed), result,
            ])).assume_usize().unwrap_or(0)
        };

        // feature processing
        let (mut ms, title) = self.evaluate(title)?;
        capture(0, title.unwrap_value(), true, Ack);

        // scenario processing
        for scenario in scenarios {
            match &scenario {
                // scenarios require specialized processing
                Expression::Scenario { title, verifications, inherits } => {
                    // scenario::title
                    let (msb, subtitle) = ms.evaluate(title)?;
                    ms = msb;

                    // update the report
                    capture(1, subtitle.unwrap_value(), true, Ack);

                    // verification processing
                    let level: u16 = 2;
                    let mut errors = 0;
                    for verification in verifications {
                        let (msb, result) = ms.evaluate(verification)?;
                        ms = msb;
                        match result {
                            ErrorValue(msg) => {
                                capture(level, verification.to_code(), false, ErrorValue(msg));
                                errors += 1;
                            }
                            result => {
                                capture(level, verification.to_code(), true, result);
                            }
                        };
                    }
                }
                other => {
                    let (msb, result) = ms.evaluate(other)?;
                    ms = msb;
                    capture(2, other.to_code(), true, result);
                }
            };
        }
        Ok((ms, TableValue(report)))
    }

    /// Formats a string based on a template
    /// Ex: format("This {} the {}", "is", "way") => "This is the way"
    fn evaluate_format_string(&self, args: Vec<TypedValue>) -> (Self, TypedValue) {
        // internal parsing function
        fn format_text(
            ms: Machine,
            template: String,
            replacements: Vec<TypedValue>,
        ) -> (Machine, TypedValue) {
            let mut result = String::from(template);
            let mut replacement_iter = replacements.iter();

            // replace each placeholder "{}" with the next element from the vector
            while let Some(pos) = result.find("{}") {
                // get the next replacement, if available
                if let Some(replacement) = replacement_iter.next() {
                    result.replace_range(pos..pos + 2, replacement.unwrap_value().as_str()); // Replace the "{}" with the replacement
                } else {
                    break; // no more replacements available, break out of the loop
                }
            }

            (ms, StringValue(result))
        }

        // parse the arguments
        let ms = Machine::new();
        if args.is_empty() { (ms, StringValue("".to_string())) } else {
            match (args[0].to_owned(), args[1..].to_owned()) {
                (StringValue(format_str), format_args) =>
                    format_text(ms, format_str, format_args),
                (other, ..) =>
                    (ms, ErrorValue(format!("Expected format string near {}", other.unwrap_value())))
            }
        }
    }

    fn evaluate_function_call(
        &self,
        fx: &Expression,
        args: &Vec<Expression>,
    ) -> std::io::Result<(Self, TypedValue)> {
        // extract the arguments
        println!("evaluate_function_call ms0 {:?}", self);
        if let (ms, Array(args)) = self.evaluate_array(args)? {
            println!("evaluate_function_call ms1 {:?}", ms);
            // evaluate the anonymous- or named-function
            match ms.evaluate(fx)? {
                (ms, BackDoor(bdf)) =>
                    ms.evaluate_back_door(&bdf, args),
                (ms, Function { params, code }) =>
                    ms.evaluate_function_arguments(params, args).evaluate(&code),
                _ => fail(format!("'{}' is not a function", fx.to_code()))
            }
        } else {
            fail(format!("Function arguments expected, but got {}", ArrayLiteral(args.to_owned())))
        }
    }

    fn evaluate_function_arguments(
        &self,
        params: Vec<ColumnJs>,
        args: Vec<TypedValue>,
    ) -> Self {
        assert_eq!(params.len(), args.len());
        params.iter().zip(args.iter())
            .fold(self.to_owned(), |machine, (c, v)|
                machine.with_variable(c.get_name(), v.to_owned()))
    }


    async fn evaluate_http(
        &self,
        method: &Expression,
        url: &Expression,
        body: &Option<Box<Expression>>,
        headers: &Option<Box<Expression>>,
        multipart: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, method) = self.evaluate(method)?;
        let (ms, url) = ms.evaluate(url)?;
        let (ms, body) = ms.evaluate_optional_map(body)?;
        let (ms, headers) = ms.evaluate_optional_map(headers)?;
        let (ms, multipart) = ms.evaluate_optional_map(multipart)?;
        ms.evaluate_http_method_call(
            method.unwrap_value(),
            url.unwrap_value(),
            body.map(|tv| tv.get_raw_value()),
            match headers {
                Some(v) => Self::extract_string_tuples(v)?,
                None => Vec::new()
            },
            match multipart {
                Some(JSONValue(values)) => {
                    Some(values.iter().fold(Form::new(), |form, (name, value)| {
                        form.part(name.to_owned(), Part::text(value.unwrap_value()))
                    }))
                }
                Some(z) => return fail_value("Type mismatch", &z),
                None => None
            },
        ).await
    }

    async fn evaluate_http_method_call(
        &self,
        method: String,
        url: String,
        body: Option<String>,
        headers: Vec<(String, String)>,
        multipart: Option<Form>,
    ) -> std::io::Result<(Self, TypedValue)> {
        match method.to_ascii_uppercase().as_str() {
            "DELETE" => self.evaluate_http_delete(url.as_str(), headers, body).await,
            "GET" => self.evaluate_http_get(url.as_str(), headers, body).await,
            "HEAD" => self.evaluate_http_head(url.as_str(), headers, body).await,
            "PATCH" => self.evaluate_http_patch(url.as_str(), headers, body).await,
            "POST" => self.evaluate_http_post(url.as_str(), headers, body, multipart).await,
            "PUT" => self.evaluate_http_put(url.as_str(), headers, body).await,
            method => Ok((self.to_owned(), ErrorValue(format!("Invalid HTTP method '{method}'"))))
        }
    }

    async fn evaluate_http_rest_call(
        request: RequestBuilder,
        headers: Vec<(String, String)>,
        body_opt: Option<String>,
    ) -> TypedValue {
        let request = Self::enrich_request(request, body_opt);
        let request = headers.iter().fold(request, |request, (k, v)| {
            request.header(k, v)
        });
        match request.send().await {
            Ok(response) => TypedValue::from_response(response).await,
            Err(err) => ErrorValue(format!("Error making request: {}", err)),
        }
    }

    async fn evaluate_http_delete(
        &self,
        url: &str,
        headers: Vec<(String, String)>,
        body_opt: Option<String>,
    ) -> std::io::Result<(Self, TypedValue)> {
        Ok((self.to_owned(), Self::evaluate_http_rest_call(Client::new().delete(url), headers, body_opt).await))
    }

    async fn evaluate_http_get(
        &self,
        url: &str,
        headers: Vec<(String, String)>,
        body_opt: Option<String>,
    ) -> std::io::Result<(Self, TypedValue)> {
        Ok((self.to_owned(), Self::evaluate_http_rest_call(Client::new().get(url), headers, body_opt).await))
    }

    async fn evaluate_http_head(
        &self,
        url: &str,
        headers: Vec<(String, String)>,
        body_opt: Option<String>,
    ) -> std::io::Result<(Self, TypedValue)> {
        Ok((self.to_owned(), Self::evaluate_http_rest_call(Client::new().head(url), headers, body_opt).await))
    }

    async fn evaluate_http_patch(
        &self,
        url: &str,
        headers: Vec<(String, String)>,
        body_opt: Option<String>,
    ) -> std::io::Result<(Self, TypedValue)> {
        Ok((self.to_owned(), Self::evaluate_http_rest_call(Client::new().patch(url), headers, body_opt).await))
    }

    async fn evaluate_http_post(
        &self,
        url: &str,
        headers: Vec<(String, String)>,
        body_opt: Option<String>,
        form_opt: Option<Form>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let request = match form_opt {
            Some(form) => Client::new().post(url).multipart(form),
            None => Client::new().post(url),
        };
        Ok((self.to_owned(), Self::evaluate_http_rest_call(request, headers, body_opt).await))
    }

    async fn evaluate_http_put(
        &self,
        url: &str,
        headers: Vec<(String, String)>,
        body_opt: Option<String>,
    ) -> std::io::Result<(Self, TypedValue)> {
        Ok((self.to_owned(), Self::evaluate_http_rest_call(Client::new().put(url), headers, body_opt).await))
    }

    /// Evaluates an if-then-else expression
    fn evaluate_if_then_else(
        &self,
        condition: &Expression,
        a: &Expression,
        b: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        use TypedValue::*;
        let ms0 = self.to_owned();
        let (machine, result) = ms0.evaluate(condition)?;
        if result.is_ok() {
            machine.evaluate(a)
        } else if let Some(b) = b {
            machine.evaluate(b)
        } else {
            Ok((ms0, Undefined))
        }
    }

    fn evaluate_include(
        &self,
        path: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        // evaluate the script path
        let (machine, path_value) = self.evaluate(path)?;
        if let TypedValue::StringValue(script_path) = path_value {
            // read the script file contents into the string
            let mut file = File::open(script_path)?;
            let mut script_code = String::new();
            file.read_to_string(&mut script_code)?;
            // compile and execute the string (script code)
            let opcode = Compiler::compile_script(script_code.as_str())?;
            machine.evaluate(&opcode)
        } else {
            Ok((machine, ErrorValue(format!("Type mismatch - expected String, got {}",
                                            path_value.get_type_name()))))
        }
    }


    /// Evaluates the index of a collection (array, string or table)
    fn evaluate_index_of_collection(
        &self,
        collection_expr: &Expression,
        index_expr: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, index) = self.evaluate(index_expr)?;
        let (machine, collection) = machine.evaluate(collection_expr)?;
        let value = match collection {
            Array(items) => {
                let idx = index.assume_usize().unwrap_or(0);
                if idx < items.len() { items[idx].to_owned() } else { ErrorValue(format!("Array element index is out of range ({} >= {})", idx, items.len())) }
            }
            NamespaceValue(d, s, n) => {
                let id = index.assume_usize().unwrap_or(0);
                let ns = Namespace::new(d, s, n);
                let frc = FileRowCollection::open(&ns)?;
                match frc.read_one(id)? {
                    Some(row) => StructureValue(Structure::from_row(&row)),
                    None => StructureValue(Structure::new(frc.get_columns().to_owned()))
                }
            }
            StringValue(string) => {
                let idx = index.assume_usize().unwrap_or(0);
                if idx < string.len() { StringValue(string[idx..idx].to_string()) } else { ErrorValue(format!("String character index is out of range ({} >= {})", idx, string.len())) }
            }
            StructureValue(structure) => {
                let idx = index.assume_usize().unwrap_or(0);
                let values = structure.get_values();
                if idx < values.len() { values[idx].to_owned() } else { ErrorValue(format!("Structure element index is out of range ({} >= {})", idx, values.len())) }
            }
            TableValue(mrc) => {
                let id = index.assume_usize().unwrap_or(0);
                match mrc.read_one(id)? {
                    Some(row) => StructureValue(Structure::from_row(&row)),
                    None => StructureValue(Structure::new(mrc.get_columns().to_owned()))
                }
            }
            other =>
                ErrorValue(format!("Type mismatch: Table or Array expected near {}", other))
        };
        Ok((machine, value))
    }

    fn evaluate_infrastructure(
        &self,
        expression: &Infrastructure,
    ) -> std::io::Result<(Self, TypedValue)> {
        use Infrastructure::*;
        match expression {
            Create { path, entity: IndexEntity { columns } } =>
                self.evaluate_table_create_index(path, columns),
            Create { path, entity: TableEntity { columns, from } } =>
                self.evaluate_table_create_table(path, columns, from),
            Declare(IndexEntity { columns }) =>
                self.evaluate_table_declare_index(columns),
            Declare(TableEntity { columns, from }) =>
                self.evaluate_table_declare_table(columns, from),
            Drop(IndexTarget { path }) => self.evaluate_table_drop(path),
            Drop(TableTarget { path }) => self.evaluate_table_drop(path),
        }
    }

    fn evaluate_inquiry(&self, expression: &Queryable) -> std::io::Result<(Self, TypedValue)> {
        use Queryable::*;
        match expression {
            Describe(table) => self.evaluate_table_describe(table),
            Limit { from, limit } => {
                let (machine, limit) = self.evaluate(limit)?;
                machine.evaluate_table_row_query(from, &UNDEFINED, limit)
            }
            Reverse(a) => self.evaluate_table_row_reverse(a),
            Select { fields, from, condition, group_by, having, order_by, limit } =>
                self.evaluate_table_row_selection(fields, from, condition, group_by, having, order_by, limit),
            Where { from, condition } =>
                self.evaluate_table_row_query(from, condition, Undefined),
        }
    }

    fn evaluate_json(
        &self,
        items: &Vec<(String, Expression)>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let mut elems = Vec::new();
        for (name, expr) in items {
            let (_, value) = self.evaluate(expr)?;
            elems.push((name.to_string(), value))
        }
        Ok((self.to_owned(), JSONValue(elems)))
    }

    fn evaluate_mutation(&self, expression: &Mutation) -> std::io::Result<(Self, TypedValue)> {
        use Mutation::*;
        match expression {
            Append { path, source } =>
                self.evaluate_table_row_append(path, source),
            Compact { path } =>
                self.evaluate_table_compact(path),
            Delete { path, condition, limit } =>
                self.evaluate_table_row_delete(path, condition, limit),
            IntoNs(source, target) =>
                self.evaluate_table_into(target, source),
            Overwrite { path, source, condition, limit } =>
                self.evaluate_table_row_overwrite(path, source, condition, limit),
            Scan { path } =>
                self.evaluate_table_scan(path),
            Truncate { path, limit } =>
                match limit {
                    None => self.evaluate_table_row_resize(path, Boolean(false)),
                    Some(limit) => {
                        let (machine, limit) = self.evaluate(limit)?;
                        machine.evaluate_table_row_resize(path, limit)
                    }
                }
            Undelete { path, condition, limit } =>
                self.evaluate_table_row_undelete(path, condition, limit),
            Update { path, source, condition, limit } =>
                self.evaluate_table_row_update(path, source, condition, limit),
        }
    }

    fn evaluate_neg(
        &self,
        expr: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, result) = self.evaluate(expr)?;
        let neg_result = match result {
            Ack => Boolean(false),
            Boolean(n) => Boolean(!n),
            Int8Value(n) => Int8Value(-n),
            Int16Value(n) => Int16Value(-n),
            Int32Value(n) => Int32Value(-n),
            Int64Value(n) => Int64Value(-n),
            Int128Value(n) => Int128Value(-n),
            UInt8Value(n) => Int16Value(-(n as i16)),
            UInt16Value(n) => Int32Value(-(n as i32)),
            UInt32Value(n) => Int64Value(-(n as i64)),
            UInt64Value(n) => Int128Value(-(n as i128)),
            UInt128Value(n) => ErrorValue(format!("{} cannot be negated", n)),
            Float32Value(n) => Float32Value(-n),
            Float64Value(n) => Float64Value(-n),
            RowsAffected(n) => Int64Value(-(n as i64)),
            z => ErrorValue(format!("{} cannot be negated", z.unwrap_value())),
        };
        Ok((machine, neg_result))
    }

    /// evaluates the specified [Expression]; returning a [TypedValue] result.
    fn evaluate_optional(machine: Self, expr: &Option<Box<Expression>>) -> std::io::Result<(Self, TypedValue)> {
        match expr {
            Some(item) => machine.evaluate(item),
            None => Ok((machine, TypedValue::Undefined))
        }
    }

    /// evaluates the specified [Expression]; returning an option of a [TypedValue] result.
    fn evaluate_optional_map(
        &self,
        opt_of_expr: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, Option<TypedValue>)> {
        Ok(match opt_of_expr {
            Some(expr) => {
                let (ms, tv) = self.evaluate(&expr)?;
                (ms, Some(tv))
            }
            None => (self.to_owned(), None)
        })
    }

    /// evaluates the specified [Expression]; returning a [TypedValue] result.
    fn evaluate_scope(&self, ops: &Vec<Expression>) -> std::io::Result<(Self, TypedValue)> {
        Ok(ops.iter().fold((self.to_owned(), Undefined),
                           |(m, _), op| match m.evaluate(op) {
                               Ok((m, ErrorValue(msg))) => (m, ErrorValue(msg)),
                               Ok((m, tv)) => (m, tv),
                               Err(err) => (m, ErrorValue(err.to_string()))
                           }))
    }

    fn evaluate_serve(
        &self,
        port_expr: &Expression,
    ) -> std::io::Result<(Machine, TypedValue)> {
        use actix_web::web;
        use crate::rest_server::*;
        let (_ms, port) = self.evaluate(port_expr)?;
        thread::spawn(move || {
            let port = port.assume_usize()
                .expect("port: an integer value was expected");
            let server = actix_web::HttpServer::new(move || web_routes!(SharedState::new()))
                .bind(format!("{}:{}", "0.0.0.0", port))
                .expect(format!("Can not bind to port {port}").as_str())
                .run();
            Runtime::new()
                .expect("Failed to create a Runtime instance")
                .block_on(server)
                .expect(format!("Failed while blocking on port {port}").as_str());
        });
        Ok((self.to_owned(), Ack))
    }

    fn evaluate_syscall(
        &self,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let ms = self.to_owned();
        let items: Vec<_> = args.iter().map(|i| i.unwrap_value()).collect();
        if let Some((command, cmd_args)) = Self::split_first(items) {
            let output: Output = std::process::Command::new(command).args(cmd_args).output()?;
            let result: TypedValue =
                if output.status.success() {
                    let raw_text = String::from_utf8_lossy(&output.stdout);
                    StringValue(raw_text.to_string())
                } else {
                    let message = String::from_utf8_lossy(&output.stderr);
                    ErrorValue(message.to_string())
                };
            Ok((ms, result))
        } else {
            Ok((ms, ErrorValue(format!("Expected array but got {:?}", args))))
        }
    }

    fn evaluate_table_create_index(
        &self,
        index: &Expression,
        columns: &Vec<Expression>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, result) = self.evaluate(index)?;
        match result {
            ErrorValue(msg) => Ok((machine, ErrorValue(msg))),
            Null | Undefined => Ok((machine, result)),
            TableValue(_mrc) =>
                Ok((machine, ErrorValue("Memory collections do not yet support indexes".to_string()))),
            NamespaceValue(d, s, n) => {
                let ns = Namespace::new(d, s, n);
                let (machine, columns) = self.evaluate_as_atoms(columns)?;
                let config = DataFrameConfig::load(&ns)?;
                let mut indices = config.get_indices().to_owned();
                indices.push(HashIndexConfig::new(columns, false));
                let config = DataFrameConfig::new(
                    config.get_columns().to_owned(),
                    indices.to_owned(),
                    config.get_partitions().to_owned(),
                );
                config.save(&ns)?;
                Ok((machine, Ack))
            }
            z =>
                Ok((machine, ErrorValue(format!("Type mismatch: expected an iterable near {}", z))))
        }
    }

    fn evaluate_table_create_table(
        &self,
        table: &Expression,
        columns: &Vec<ColumnJs>,
        from: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, result) = self.evaluate(table)?;
        match result.to_owned() {
            Null | Undefined => Ok((machine, result)),
            TableValue(_mrc) =>
                Ok((machine, ErrorValue("Memory collections do not 'create' keyword".to_string()))),
            NamespaceValue(d, s, n) => {
                let ns = Namespace::new(d, s, n);
                let config = DataFrameConfig::new(columns.to_owned(), Vec::new(), Vec::new());
                DataFrame::create(ns, config)?;
                Ok((machine, Ack))
            }
            x =>
                Ok((machine, ErrorValue(format!("Type mismatch: expected an iterable near {}", x))))
        }
    }

    fn evaluate_table_declare_index(
        &self,
        columns: &Vec<Expression>,
    ) -> std::io::Result<(Self, TypedValue)> {
        // TODO determine how to implement
        Ok((self.to_owned(), ErrorValue("Not yet implemented".into())))
    }

    fn evaluate_table_declare_table(
        &self,
        columns: &Vec<ColumnJs>,
        from: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let columns = TableColumn::from_columns(columns)?;
        Ok((self.to_owned(), TableValue(ModelRowCollection::with_rows(columns, Vec::new()))))
    }

    fn evaluate_table_describe(
        &self,
        expression: &Box<Expression>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, result) = self.evaluate(expression)?;
        let rc = result.to_table()?;
        Ok((machine, rc.describe()))
    }

    fn evaluate_table_drop(&self, table: &Expression) -> std::io::Result<(Self, TypedValue)> {
        let (machine, table) = self.evaluate(table)?;
        match table {
            NamespaceValue(d, s, n) => {
                let ns = Namespace::new(d, s, n);
                let result = fs::remove_file(ns.get_table_file_path());
                Ok((machine, if result.is_ok() { Ack } else { Boolean(false) }))
            }
            _ => Ok((machine, Boolean(false)))
        }
    }

    fn evaluate_table_into(
        &self,
        table: &Expression,
        source: &Expression,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let machine = self.to_owned();
        let (machine, rows) = match source {
            Expression::From(source) =>
                self.extract_rows_from_query(source, table)?,
            Literal(TableValue(mrc)) =>
                (machine, mrc.get_rows()),
            Literal(NamespaceValue(d, s, n)) => {
                let ns = Namespace::new(d, s, n);
                let frc = FileRowCollection::open(&ns)?;
                (machine, frc.read_active_rows()?)
            }
            Perform(Declare(TableEntity { columns, from })) =>
                machine.extract_rows_from_table_declaration(table, from, columns)?,
            source =>
                self.extract_rows_from_query(source, table)?,
        };

        // write the rows to the target
        let mut inserted = 0;
        let mut rc = machine.expect_row_collection(table)?;
        match rc.append_rows(rows) {
            ErrorValue(message) => return Ok((machine, ErrorValue(message))),
            RowsAffected(n) => inserted += n,
            _ => {}
        }
        Ok((machine, RowsAffected(inserted)))
    }

    fn evaluate_table_compact(&self, expr: &Expression) -> std::io::Result<(Self, TypedValue)> {
        let (machine, tv_table) = self.evaluate(expr)?;
        Self::orchestrate_rc(machine, tv_table, |machine, mut rc| {
            Ok((machine, rc.compact()))
        })
    }

    fn evaluate_table_ns(&self, expr: &Expression) -> std::io::Result<(Self, TypedValue)> {
        let (ms, result) = self.evaluate(expr)?;
        match result {
            StringValue(path) =>
                match path.split('.').collect::<Vec<_>>().as_slice() {
                    [d, s, n] => Ok((ms, NamespaceValue(d.to_string(), s.to_string(), n.to_string()))),
                    _ => Ok((ms, ErrorValue(format!("Invalid namespace reference '{}'", path))))
                }
            NamespaceValue(d, s, n) => Ok((ms, NamespaceValue(d, s, n))),
            other => Ok((ms, Self::error_unexpected("Table reference", &other))),
        }
    }

    fn evaluate_table_row_resize(
        &self,
        table: &Expression,
        limit: TypedValue,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, table) = self.evaluate(table)?;
        Self::orchestrate_io(machine, table, &Vec::new(), &Vec::new(), &None, limit, |machine, df, _fields, _values, condition, limit| {
            let limit = limit.assume_usize().unwrap_or(0);
            let new_size = df.resize(limit)?;
            Ok((machine, RowsAffected(new_size)))
        })
    }

    fn evaluate_table_row_append(
        &self,
        table: &Expression,
        source: &Expression,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let machine = self.to_owned();
        if let Expression::From(source) = source {
            // determine the writable target
            let mut writable = self.expect_row_collection(table)?;

            // retrieve rows from the source
            let columns = writable.get_columns();
            let rows = self.expect_rows(source, columns)?;

            // write the rows to the target
            Ok((machine, writable.append_rows(rows)))
        } else {
            Ok((machine, Self::error_expr("A queryable was expected".to_string(), source)))
        }
    }

    fn evaluate_table_row_delete(
        &self,
        from: &Expression,
        condition: &Option<Box<Expression>>,
        limit: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, limit) = Self::evaluate_optional(self.to_owned(), limit)?;
        let (machine, result) = machine.evaluate(from)?;
        Self::orchestrate_io(machine, result, &Vec::new(), &Vec::new(), condition, limit, |machine, df, _fields, _values, condition, limit| {
            let deleted = df.delete_where(&machine, &condition, limit).ok().unwrap_or(0);
            Ok((machine, RowsAffected(deleted)))
        })
    }

    fn evaluate_table_row_overwrite(
        &self,
        table: &Expression,
        source: &Expression,
        condition: &Option<Box<Expression>>,
        limit: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, limit) = Self::evaluate_optional(self.to_owned(), limit)?;
        let (machine, tv_table) = machine.evaluate(table)?;
        let (fields, values) = self.expect_via(&table, &source)?;
        Self::orchestrate_io(machine, tv_table, &fields, &values, condition, limit, |machine, df, fields, values, condition, limit| {
            let overwritten = df.overwrite_where(&machine, fields, values, condition, limit).ok().unwrap_or(0);
            Ok((machine, RowsAffected(overwritten)))
        })
    }

    /// Evaluates the queryable [Expression] (e.g. from, limit and where)
    /// e.g.: from ns("interpreter.select.stocks") where last_sale > 1.0 limit 1
    fn evaluate_table_row_query(
        &self,
        src: &Expression,
        condition: &Expression,
        limit: TypedValue,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, result) = self.evaluate(src)?;
        let limit = match limit {
            Null | Undefined => None,
            value =>
                match value.assume_usize() {
                    None => None,
                    Some(n) if n < 1 => Some(1),
                    Some(n) => Some(n)
                }
        }.unwrap_or(usize::MAX);
        match result {
            TableValue(mrc) => {
                let mut cursor = Cursor::filter(Box::new(mrc), condition.to_owned());
                Ok((machine, TableValue(ModelRowCollection::from_rows(cursor.take(limit)?))))
            }
            NamespaceValue(d, s, n) => {
                let ns = Namespace::new(d, s, n);
                let frc = FileRowCollection::open(&ns)?;
                let mut cursor = Cursor::filter(Box::new(frc), condition.to_owned());
                Ok((machine, TableValue(ModelRowCollection::from_rows(cursor.take(limit)?))))
            }
            value => fail_value("Queryable expected", &value)
        }
    }

    /// Reverse orders a collection
    fn evaluate_table_row_reverse(
        &self,
        table: &Box<Expression>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, table) = self.evaluate(table)?;
        let result = self.assume_dataframe(&table, |df| {
            let rows = df.reverse()?;
            Ok(TableValue(ModelRowCollection::from_rows(rows)))
        })?;
        Ok((machine, result))
    }

    fn evaluate_table_row_selection(
        &self,
        fields: &Vec<Expression>,
        from: &Option<Box<Expression>>,
        condition: &Option<Box<Expression>>,
        _group_by: &Option<Vec<Expression>>,
        _having: &Option<Box<Expression>>,
        _order_by: &Option<Vec<Expression>>,
        limit: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, limit) = Self::evaluate_optional(self.to_owned(), limit)?;
        match from {
            None => machine.evaluate_array(fields),
            Some(source) => {
                let (machine, table) = machine.evaluate(source)?;
                Self::orchestrate_io(machine, table, &Vec::new(), &Vec::new(), condition, limit, |machine, df, _, _, condition, limit| {
                    let rows = df.read_where(&machine, condition, limit)?;
                    Ok((machine, TableValue(ModelRowCollection::from_rows(rows))))
                })
            }
        }
    }

    fn evaluate_table_row_undelete(
        &self,
        from: &Expression,
        condition: &Option<Box<Expression>>,
        limit: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, limit) = Self::evaluate_optional(self.to_owned(), limit)?;
        let (machine, result) = machine.evaluate(from)?;
        Self::orchestrate_io(machine, result, &Vec::new(), &Vec::new(), condition, limit, |machine, df, _fields, _values, condition, limit| {
            match df.undelete_where(&machine, &condition, limit) {
                Ok(restored) => Ok((machine, RowsAffected(restored))),
                Err(err) => Ok((machine, ErrorValue(err.to_string()))),
            }
        })
    }

    fn evaluate_table_row_update(
        &self,
        table: &Expression,
        source: &Expression,
        condition: &Option<Box<Expression>>,
        limit: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, limit) = Self::evaluate_optional(self.to_owned(), limit)?;
        let (machine, tv_table) = machine.evaluate(table)?;
        let (fields, values) = self.expect_via(&table, &source)?;
        Self::orchestrate_io(machine, tv_table, &fields, &values, condition, limit, |machine, df, fields, values, condition, limit| {
            let modified = df.update_where(&machine, fields, values, condition, limit).ok().unwrap_or(0);
            Ok((machine, RowsAffected(modified)))
        })
    }

    fn evaluate_table_scan(&self, expr: &Expression) -> std::io::Result<(Self, TypedValue)> {
        let (machine, tv_table) = self.evaluate(expr)?;
        Self::orchestrate_rc(machine, tv_table, |machine, rc| {
            let mrc = ModelRowCollection::from_rows(rc.examine_rows()?);
            Ok((machine, TableValue(mrc)))
        })
    }

    fn evaluate_to_csv(
        &self,
        rc: Box<dyn RowCollection>,
    ) -> (Self, TypedValue) {
        (self.to_owned(), Array(rc.iter().map(|row| row.to_csv())
            .map(StringValue).collect()))
    }

    fn evaluate_to_json(
        &self,
        rc: Box<dyn RowCollection>,
    ) -> (Self, TypedValue) {
        (self.to_owned(), Array(rc.iter().map(|row| row.to_json())
            .map(StringValue).collect()))
    }

    fn evaluate_while(
        &self,
        condition: &Expression,
        code: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let mut machine = self.to_owned();
        let mut is_looping = true;
        let mut outcome = Undefined;
        while is_looping {
            let (_, result) = machine.evaluate(condition)?;
            is_looping = result.is_true();
            if is_looping {
                let (m, result) = machine.evaluate(code)?;
                machine = m;
                outcome = result;
            }
        }
        Ok((machine, outcome))
    }

    fn expect_namespace(&self, table: &Expression) -> std::io::Result<Namespace> {
        let (_, v_table) = self.evaluate(table)?;
        match v_table {
            NamespaceValue(d, s, n) => Ok(Namespace::new(d, s, n)),
            x => fail_unexpected("Table namespace", &x)
        }
    }

    fn expect_row_collection(
        &self,
        table: &Expression,
    ) -> std::io::Result<Box<dyn RowCollection>> {
        let (_, v_table) = self.evaluate(table)?;
        v_table.to_table()
    }

    fn expect_rows(
        &self,
        source: &Expression,
        columns: &Vec<TableColumn>,
    ) -> std::io::Result<Vec<Row>> {
        let (_, source) = self.evaluate(source)?;
        match source {
            Array(items) => {
                let mut rows = Vec::new();
                for tuples in items {
                    if let JSONValue(tuples) = tuples {
                        rows.push(Row::from_tuples(0, columns, &tuples))
                    }
                }
                Ok(rows)
            }
            JSONValue(tuples) => Ok(vec![Row::from_tuples(0, columns, &tuples)]),
            NamespaceValue(d, s, n) => {
                let ns = Namespace::new(d, s, n);
                let frc = FileRowCollection::open(&ns)?;
                frc.read_active_rows()
            }
            TableValue(mrc) => Ok(mrc.get_rows()),
            tv => fail_value("A queryable was expected".to_string(), &tv)
        }
    }

    fn expect_via(
        &self,
        table: &Expression,
        source: &Expression,
    ) -> std::io::Result<(Vec<Expression>, Vec<Expression>)> {
        let writable = self.expect_row_collection(table)?;
        let (fields, values) = {
            if let Via(my_source) = source {
                if let (_, JSONValue(tuples)) = self.evaluate(&my_source)? {
                    let row = Row::from_tuples(0, writable.get_columns(), &tuples);
                    Self::split(&row)
                } else {
                    return fail_expr("Expected a data object".to_string(), &my_source);
                }
            } else {
                return fail_expr("Expected keyword 'via'".to_string(), &source);
            }
        };
        Ok((fields, values))
    }

    fn extract_rows_from_table_declaration(
        &self,
        table: &Expression,
        from: &Option<Box<Expression>>,
        columns: &Vec<ColumnJs>,
    ) -> std::io::Result<(Machine, Vec<Row>)> {
        let machine = self.to_owned();
        // create the config and an empty data file
        let ns = self.expect_namespace(table)?;
        let cfg = DataFrameConfig::new(columns.to_owned(), Vec::new(), Vec::new());
        cfg.save(&ns)?;
        FileRowCollection::table_file_create(&ns)?;
        // decipher the "from" expression
        let columns = TableColumn::from_columns(columns)?;
        let results = match from {
            Some(expr) => machine.expect_rows(expr.deref(), &columns)?,
            None => Vec::new()
        };
        Ok((machine, results))
    }

    fn extract_rows_from_query(
        &self,
        source: &Expression,
        table: &Expression,
    ) -> std::io::Result<(Machine, Vec<Row>)> {
        // determine the row collection
        let machine = self.to_owned();
        let rc = machine.expect_row_collection(table)?;

        // retrieve rows from the source
        let rows = machine.expect_rows(source, rc.get_columns())?;
        Ok((machine, rows))
    }

    /// returns a variable by name
    pub fn get(&self, name: &str) -> Option<TypedValue> {
        self.variables.get(name).map(|x| x.to_owned())
    }

    /// returns a table describing all variables
    /// ex. __variables()
    pub fn get_variables(&self) -> ModelRowCollection {
        let columns = TableColumn::from_columns(&vec![
            ColumnJs::new("name", "String(80)", None),
            ColumnJs::new("kind", "String(80)", None),
            ColumnJs::new("value", "String(256)", None),
        ]).unwrap_or(vec![]);
        let mut mrc = ModelRowCollection::new(columns.to_owned());
        let variables = self.variables.iter()
            //.filter(|&(_, v)| v.get_type_name() != "BackDoor")
            .collect::<Vec<_>>();
        for (name, value) in variables {
            mrc.append_row(Row::new(0, columns.to_owned(), vec![
                StringValue(name.to_owned()),
                StringValue(value.get_type_name()),
                match value {
                    BackDoor(f) => StringValue(format!("oxide.native.{:?}", f)),
                    TableValue(rc) => StringValue(format!("{} row(s)", rc.len().unwrap_or(0))),
                    v => StringValue(v.get_raw_value())
                },
            ]));
        }
        mrc
    }

    /// returns the option of a value from the stack
    pub fn pop(&self) -> (Self, Option<TypedValue>) {
        let mut stack = self.stack.to_owned();
        let value = stack.pop();
        let variables = self.variables.to_owned();
        (Self::construct(stack, variables), value)
    }

    /// returns a value from the stack or the default value if the stack is empty.
    pub fn pop_or(&self, default_value: TypedValue) -> (Self, TypedValue) {
        let (machine, result) = self.pop();
        (machine, result.unwrap_or(default_value))
    }

    /// pushes a value unto the stack
    pub fn push(&self, value: TypedValue) -> Self {
        let mut stack = self.stack.to_owned();
        stack.push(value);
        Self::construct(stack, self.variables.to_owned())
    }

    /// pushes a collection of values unto the stack
    pub fn push_all(&self, values: Vec<TypedValue>) -> Self {
        values.iter().fold(self.to_owned(), |machine, tv| machine.push(tv.to_owned()))
    }

    pub fn set(&self, name: &str, value: TypedValue) -> Self {
        let mut variables = self.variables.to_owned();
        variables.insert(name.to_string(), value);
        Self::construct(self.stack.to_owned(), variables)
    }

    pub fn transform_numeric(&self, number: TypedValue,
                             fi: fn(i64) -> TypedValue,
                             ff: fn(f64) -> TypedValue) -> std::io::Result<Self> {
        match number {
            Float32Value(n) => Ok(self.push(ff(n as f64))),
            Float64Value(n) => Ok(self.push(ff(n))),
            UInt8Value(n) => Ok(self.push(fi(n as i64))),
            Int16Value(n) => Ok(self.push(fi(n as i64))),
            Int32Value(n) => Ok(self.push(fi(n as i64))),
            Int64Value(n) => Ok(self.push(fi(n))),
            RowsAffected(n) => Ok(self.push(ff(n as f64))),
            unknown => fail(format!("Unsupported type {:?}", unknown))
        }
    }

    pub fn with_row(&self, row: &Row) -> Self {
        row.get_values().iter().zip(row.get_columns().iter())
            .fold(self.to_owned(), |machine, (v, c)| {
                machine.with_variable(c.get_name(), v.to_owned())
            })
    }

    pub fn with_variable(&self, name: &str, value: TypedValue) -> Self {
        let mut variables = self.variables.to_owned();
        variables.insert(name.to_string(), value);
        Self::construct(self.stack.to_owned(), variables)
    }


}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::compiler::Compiler;
    use crate::expression::{FALSE, NULL, TRUE};
    use crate::expression::Expression::*;
    use crate::table_columns::TableColumn;
    use crate::table_renderer::TableRenderer;
    use crate::testdata::{make_dataframe_ns, make_quote, make_quote_columns, make_table_columns};

    use super::*;

    #[test]
    fn test_array_declaration() {
        let models = vec![Literal(Float64Value(3.25)), TRUE, FALSE, NULL, UNDEFINED];
        assert_eq!(models.iter().map(|e| e.to_code()).collect::<Vec<String>>(), vec![
            "3.25", "true", "false", "null", "undefined",
        ]);

        let (_, array) = Machine::new().evaluate_array(&models).unwrap();
        assert_eq!(array, Array(vec![Float64Value(3.25), Boolean(true), Boolean(false), Null, Undefined]));
    }

    #[test]
    fn test_aliases() {
        let model = AsValue(
            "symbol".to_string(),
            Box::new(Literal(StringValue("ABC".into()))),
        );
        assert_eq!(model.to_code(), "symbol: \"ABC\"");

        let machine = Machine::new();
        let (machine, tv) = machine.evaluate(&model).unwrap();
        assert_eq!(tv, StringValue("ABC".into()));
        assert_eq!(machine.get("symbol"), Some(StringValue("ABC".into())));
    }

    #[test]
    fn test_column_set() {
        let model = ColumnSet(make_quote_columns());
        assert_eq!(model.to_code(), "(symbol: String(8), exchange: String(8), last_sale: f64)");

        let machine = Machine::new()
            .with_variable("symbol", StringValue("ABC".into()))
            .with_variable("exchange", StringValue("NYSE".into()))
            .with_variable("last_sale", Float64Value(12.66));
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, Array(vec![
            StringValue("ABC".into()),
            StringValue("NYSE".into()),
            Float64Value(12.66),
        ]));
    }

    #[test]
    fn test_divide() {
        let machine = Machine::new().with_variable("x", Int64Value(50));
        let model = Divide(Box::new(Variable("x".into())), Box::new(Literal(Int64Value(7))));
        assert_eq!(model.to_code(), "x / 7");

        let (machine, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, Int64Value(7));
        assert_eq!(machine.get("x"), Some(Int64Value(50)));
    }

    #[test]
    fn test_factorial() {
        let model = Factorial(Box::new(Literal(Float64Value(6.))));
        assert_eq!(model.to_code(), "¡6");

        let machine = Machine::new();
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, Float64Value(720.))
    }

    #[test]
    fn test_if_else_flow_1() {
        let model = If {
            condition: Box::new(GreaterThan(
                Box::new(Variable("num".into())),
                Box::new(Literal(Int64Value(25))),
            )),
            a: Box::new(Literal(StringValue("Yes".into()))),
            b: Some(Box::new(Literal(StringValue("No".into())))),
        };
        assert_eq!(model.to_code(), r#"if num > 25 "Yes" else "No""#);

        let machine = Machine::new().with_variable("num", Int64Value(5));
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, StringValue("No".into()));

        let machine = Machine::new().with_variable("num", Int64Value(37));
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, StringValue("Yes".into()));
    }

    #[test]
    fn test_if_else_flow_2() {
        let model = If {
            condition: Box::new(LessThan(
                Box::new(Variable("num".into())),
                Box::new(Literal(Int64Value(10))),
            )),
            a: Box::new(Literal(StringValue("Yes".into()))),
            b: Some(Box::new(Literal(StringValue("No".into())))),
        };
        assert_eq!(model.to_code(), r#"if num < 10 "Yes" else "No""#);

        let machine = Machine::new().with_variable("num", Int64Value(5));
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, StringValue("Yes".into()));

        let machine = Machine::new().with_variable("num", Int64Value(99));
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, StringValue("No".into()));
    }

    #[test]
    fn test_index_of_array() {
        // create the instruction model for "[1, 4, 2, 8, 5, 7][5]"
        let model = ElementAt(
            Box::new(ArrayLiteral(vec![
                Literal(Int64Value(1)), Literal(Int64Value(4)), Literal(Int64Value(2)),
                Literal(Int64Value(8)), Literal(Int64Value(5)), Literal(Int64Value(7)),
            ])),
            Box::new(Literal(Int64Value(5))),
        );
        assert_eq!(model.to_code(), "[1, 4, 2, 8, 5, 7][5]");

        let machine = Machine::new();
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, Int64Value(7))
    }

    #[test]
    fn test_index_of_table_in_namespace() {
        // create a table with test data
        let ns = Namespace::new("machine", "element_at", "stocks");
        let phys_columns = make_table_columns();
        let value_tuples = vec![
            ("ABC", "AMEX", 11.77), ("UNO", "OTC", 0.2456),
            ("BIZ", "NYSE", 23.66), ("GOTO", "OTC", 0.1428),
            ("BOOM", "NASDAQ", 56.87),
        ];
        let rows = value_tuples.iter()
            .map(|(symbol, exchange, last_sale)| {
                Row::new(0, phys_columns.to_owned(), vec![
                    StringValue(symbol.to_string()),
                    StringValue(exchange.to_string()),
                    Float64Value(*last_sale),
                ])
            }).collect();
        let mut frc = FileRowCollection::create_table(&ns, phys_columns.to_owned()).unwrap();
        assert_eq!(RowsAffected(5), frc.append_rows(rows));

        // create the instruction model 'ns("machine.element_at.stocks")[2]'
        let model = ElementAt(
            Box::new(Ns(Box::new(Literal(StringValue(ns.into()))))),
            Box::new(Literal(Int64Value(2))),
        );
        assert_eq!(model.to_code(), r#"ns("machine.element_at.stocks")[2]"#);

        // evaluate the instruction
        let (_, result) = Machine::new().evaluate(&model).unwrap();
        assert_eq!(result, StructureValue(
            Structure::from_row(&make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66))
        ))
    }

    #[test]
    fn test_index_of_table_in_variable() {
        let phys_columns = make_table_columns();
        let my_table = ModelRowCollection::from_rows(vec![
            make_quote(0, &phys_columns, "ABC", "AMEX", 12.33),
            make_quote(1, &phys_columns, "GAS", "NYSE", 0.2456),
            make_quote(2, &phys_columns, "BASH", "NASDAQ", 13.11),
            make_quote(3, &phys_columns, "OIL", "NYSE", 0.1442),
            make_quote(4, &phys_columns, "VAPOR", "NYSE", 0.0289),
        ]);

        // create the instruction model "stocks[4]"
        let model = ElementAt(
            Box::new(Variable("stocks".to_string())),
            Box::new(Literal(Int64Value(4))),
        );
        assert_eq!(model.to_code(), "stocks[4]");

        // evaluate the instruction
        let machine = Machine::new()
            .with_variable("stocks", TableValue(my_table));
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, StructureValue(
            Structure::from_row(&make_quote(4, &phys_columns, "VAPOR", "NYSE", 0.0289))
        ))
    }

    #[test]
    fn test_create_table() {
        let model = Perform(Infrastructure::Create {
            path: Box::new(Ns(Box::new(Literal(StringValue("machine.create.stocks".into()))))),
            entity: TableEntity {
                columns: make_quote_columns(),
                from: None,
            },
        });

        // create the table
        let machine = Machine::new();
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, Ack);

        // decompile back to source code
        assert_eq!(
            model.to_code(),
            "create table ns(\"machine.create.stocks\") (symbol: String(8), exchange: String(8), last_sale: f64)"
        );
    }

    #[test]
    fn test_create_table_with_index() {
        let path = "machine.index.stocks";
        let machine = Machine::new();

        // drop table if exists ns("machine.index.stocks")
        let (machine, _) = machine.evaluate(&Perform(Infrastructure::Drop(TableTarget {
            path: Box::new(Ns(Box::new(Literal(StringValue(path.into()))))),
        }))).unwrap();

        // create table ns("machine.index.stocks") (...)
        let (machine, result) = machine.evaluate(&Perform(Infrastructure::Create {
            path: Box::new(Ns(Box::new(Literal(StringValue(path.into()))))),
            entity: TableEntity {
                columns: make_quote_columns(),
                from: None,
            },
        })).unwrap();
        assert_eq!(result, Ack);

        // create index ns("machine.index.stocks") [symbol, exchange]
        let model = Perform(Infrastructure::Create {
            path: Box::new(Ns(Box::new(Literal(StringValue(path.into()))))),
            entity: IndexEntity {
                columns: vec![
                    Variable("symbol".into()),
                    Variable("exchange".into()),
                ],
            },
        });
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, Ack);

        // decompile back to source code
        assert_eq!(
            model.to_code(),
            "create index ns(\"machine.index.stocks\") [symbol, exchange]"
        );
    }

    #[test]
    fn test_declare_table() {
        let model = Perform(Infrastructure::Declare(TableEntity {
            columns: vec![
                ColumnJs::new("symbol", "String(8)", None),
                ColumnJs::new("exchange", "String(8)", None),
                ColumnJs::new("last_sale", "f64", None),
            ],
            from: None,
        }));

        let machine = Machine::new();
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, TypedValue::TableValue(ModelRowCollection::with_rows(
            make_table_columns(), Vec::new(),
        )))
    }

    #[test]
    fn test_drop_table() {
        // create a table with test data
        let ns_path = "machine.drop.stocks";
        let ns = Ns(Box::new(Literal(StringValue(ns_path.into()))));
        create_dataframe(ns_path);

        let model = Perform(Infrastructure::Drop(TableTarget { path: Box::new(ns) }));
        assert_eq!(model.to_code(), "drop table ns(\"machine.drop.stocks\")");

        let machine = Machine::new();
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, Ack)
    }

    #[test]
    fn test_feature_with_a_scenario() {
        let model = Feature {
            title: Box::new(Literal(StringValue("Karate translator".to_string()))),
            scenarios: vec![
                Scenario {
                    title: Box::new(Literal(StringValue("Translate Karate Scenario to Oxide Scenario".to_string()))),
                    verifications: vec![
                        FunctionCall {
                            fx: Box::new(Variable("assert".to_string())),
                            args: vec![Literal(Boolean(true))],
                        }
                    ],
                    inherits: None,
                }
            ],
        };

        let machine = Machine::new();
        let (_, result) = machine.evaluate(&model).unwrap();
        let table = result.to_table().unwrap();
        let columns = table.get_columns();
        let rows = table.read_active_rows().unwrap();
        for s in TableRenderer::from_rows(rows.to_owned()) {
            println!("{}", s)
        }
        let dump = rows.iter().map(|row| row.get_values()).collect::<Vec<_>>();
        assert_eq!(dump, vec![
            vec![
                UInt16Value(0),
                StringValue("Karate translator".into()),
                Boolean(true),
                Ack,
            ],
            vec![
                UInt16Value(1),
                StringValue("Translate Karate Scenario to Oxide Scenario".into()),
                Boolean(true),
                Ack,
            ],
            vec![
                UInt16Value(2),
                StringValue("assert(true)".into()),
                Boolean(true),
                Boolean(true),
            ],
        ]);
    }

    #[test]
    fn test_anonymous_function() {
        // define a function call: (n => n + 5)(3)
        let model = FunctionCall {
            fx: Box::new(
                Literal(Function {
                    params: vec![
                        ColumnJs::new("n", "i64", None)
                    ],
                    code: Box::new(Plus(
                        Box::new(Variable("n".into())),
                        Box::new(Literal(Int64Value(5))))),
                })),
            args: vec![
                Literal(Int64Value(3))
            ],
        };

        // evaluate the function
        let (machine, result) = Machine::new()
            .with_variable("n", Int64Value(3))
            .evaluate(&model)
            .unwrap();
        assert_eq!(result, Int64Value(8));
        assert_eq!(model.to_code(), "((n: i64) => n + 5)(3)")
    }

    #[test]
    fn test_named_function() {
        // define a function: (a, b) => a + b
        let fx = Function {
            params: vec![
                ColumnJs::new("a", "i64", None),
                ColumnJs::new("b", "i64", None),
            ],
            code: Box::new(Plus(Box::new(
                Variable("a".into())
            ), Box::new(
                Variable("b".into())
            ))),
        };

        // publish the function in scope: fn add(a, b) => a + b
        let machine = Machine::new();
        let (machine, result) = machine.evaluate_scope(&vec![
            SetVariable("add".to_string(), Box::new(Literal(fx.to_owned())))
        ]).unwrap();
        assert_eq!(machine.get("add").unwrap(), fx);
        assert_eq!(result, TypedValue::Ack);

        // execute the function via function call in scope: add(2, 3)
        let model = FunctionCall {
            fx: Box::new(Literal(fx)),
            args: vec![
                Literal(Int64Value(2)),
                Literal(Int64Value(3)),
            ],
        };
        let (machine, result) = machine
            .evaluate(&model)
            .unwrap();
        assert_eq!(result, Int64Value(5));
        assert_eq!(model.to_code(), "((a: i64, b: i64) => a + b)(2, 3)")
    }

    #[test]
    fn test_from_where_limit_in_memory() {
        // create a table with test data
        let columns = make_quote_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let machine = Machine::new()
            .with_variable("stocks", TableValue(ModelRowCollection::from_rows(vec![
                make_quote(0, &phys_columns, "ABC", "AMEX", 12.33),
                make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
                make_quote(2, &phys_columns, "BIZ", "NYSE", 9.775),
                make_quote(3, &phys_columns, "GOTO", "OTC", 0.1442),
                make_quote(4, &phys_columns, "XYZ", "NYSE", 0.0289),
            ])));

        let model = Inquire(Queryable::Limit {
            from: Box::new(Inquire(Queryable::Where {
                from: Box::new(From(Box::new(Variable("stocks".into())))),
                condition: Box::new(GreaterOrEqual(
                    Box::new(Variable("last_sale".into())),
                    Box::new(Literal(Float64Value(1.0))),
                )),
            })),
            limit: Box::new(Literal(Int64Value(2))),
        });
        assert_eq!(model.to_code(), "from stocks where last_sale >= 1 limit 2");

        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, TableValue(ModelRowCollection::from_rows(vec![
            make_quote(0, &phys_columns, "ABC", "AMEX", 12.33),
            make_quote(2, &phys_columns, "BIZ", "NYSE", 9.775),
        ])));
    }

    #[test]
    fn test_delete_rows_from_namespace() {
        // create a table with test data
        let ns_path = "machine.delete.stocks";
        let (df, phys_columns) = create_dataframe(ns_path);

        // delete some rows
        let machine = Machine::new();
        let code = Compiler::compile_script(r#"
            delete from ns("machine.delete.stocks")
            where last_sale > 1.0
            "#).unwrap();
        let (_, result) = machine.evaluate(&code).unwrap();
        assert_eq!(result, RowsAffected(3));

        // verify the remaining rows
        let rows = df.read_active_rows().unwrap();
        assert_eq!(rows, vec![
            make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
            make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428),
        ]);
    }

    #[test]
    fn test_undelete_rows_from_namespace() {
        // create a table with test data
        let ns_path = "machine.undelete.stocks";
        let (df, phys_columns) = create_dataframe(ns_path);

        // delete some rows
        let machine = Machine::new();
        let code = Compiler::compile_script(r#"
            stocks := ns("machine.undelete.stocks")
            delete from stocks where last_sale > 1.0
            "#).unwrap();
        let (machine, result) = machine.evaluate(&code).unwrap();
        assert_eq!(result, RowsAffected(3));

        // verify the remaining rows
        let rows = df.read_active_rows().unwrap();
        assert_eq!(rows, vec![
            make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
            make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428),
        ]);

        // undelete the rows
        let code = Compiler::compile_script(r#"
            undelete from stocks where last_sale > 1.0
            "#).unwrap();
        let (_, result) = machine.evaluate(&code).unwrap();
        assert_eq!(result, RowsAffected(3));

        // verify the remaining rows
        let rows = df.read_active_rows().unwrap();
        assert_eq!(rows, vec![
            make_quote(0, &phys_columns, "ABC", "AMEX", 11.77),
            make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
            make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66),
            make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428),
            make_quote(4, &phys_columns, "BOOM", "NASDAQ", 56.87),
        ]);
    }

    #[test]
    fn test_append_namespace() {
        // create a table with test data
        let ns_path = "machine.append.stocks";
        let (df, phys_columns) = create_dataframe(ns_path);

        // insert some rows
        let machine = Machine::new();
        let (_, result) = machine.evaluate(&Mutate(Mutation::Append {
            path: Box::new(Ns(Box::new(Literal(StringValue(ns_path.to_string()))))),
            source: Box::new(From(Box::new(JSONLiteral(vec![
                ("symbol".into(), Literal(StringValue("REX".into()))),
                ("exchange".into(), Literal(StringValue("NASDAQ".into()))),
                ("last_sale".into(), Literal(Float64Value(16.99))),
            ])))),
        })).unwrap();
        assert_eq!(result, RowsAffected(1));

        // verify the remaining rows
        let rows = df.read_active_rows().unwrap();
        assert_eq!(rows, vec![
            make_quote(0, &phys_columns, "ABC", "AMEX", 11.77),
            make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
            make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66),
            make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428),
            make_quote(4, &phys_columns, "BOOM", "NASDAQ", 56.87),
            make_quote(5, &phys_columns, "REX", "NASDAQ", 16.99),
        ]);
    }

    #[test]
    fn test_overwrite_rows_in_namespace() {
        let ns_path = "machine.overwrite.stocks";
        let model = Mutate(Mutation::Overwrite {
            path: Box::new(Ns(Box::new(Literal(StringValue(ns_path.to_string()))))),
            source: Box::new(Via(Box::new(JSONLiteral(vec![
                ("symbol".into(), Literal(StringValue("BOOM".into()))),
                ("exchange".into(), Literal(StringValue("NYSE".into()))),
                ("last_sale".into(), Literal(Float64Value(56.99))),
            ])))),
            condition: Some(Box::new(Equal(
                Box::new(Variable("symbol".into())),
                Box::new(Literal(StringValue("BOOM".into()))),
            ))),
            limit: None,
        });
        assert_eq!(model.to_code(), r#"overwrite ns("machine.overwrite.stocks") via {symbol: "BOOM", exchange: "NYSE", last_sale: 56.99} where symbol == "BOOM""#);

        // create a table with test data
        let (df, phys_columns) = create_dataframe(ns_path);

        // overwrite some rows
        let machine = Machine::new();
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, RowsAffected(1));

        // verify the remaining rows
        let rows = df.read_active_rows().unwrap();
        assert_eq!(rows, vec![
            make_quote(0, &phys_columns, "ABC", "AMEX", 11.77),
            make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
            make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66),
            make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428),
            make_quote(4, &phys_columns, "BOOM", "NYSE", 56.99),
        ])
    }

    #[ignore]
    #[test]
    fn test_precedence() {
        // 2 + 4 * 3
        let opcodes = vec![
            Plus(Box::new(Literal(Int64Value(2))),
                 Box::new(Multiply(Box::new(Literal(Int64Value(4))),
                                   Box::new(Literal(Int64Value(3))))))
        ];

        let (_ms, result) = Machine::new().evaluate_scope(&opcodes).unwrap();
        assert_eq!(result, Float64Value(14.))
    }

    #[test]
    fn test_push_all() {
        let machine = Machine::new().push_all(vec![
            Float32Value(2.), Float64Value(3.),
            Int16Value(4), Int32Value(5),
            Int64Value(6), StringValue("Hello World".into()),
        ]);
        assert_eq!(machine.stack, vec![
            Float32Value(2.), Float64Value(3.),
            Int16Value(4), Int32Value(5),
            Int64Value(6), StringValue("Hello World".into()),
        ])
    }

    #[test]
    fn test_reverse_from_variable() {
        let model = Inquire(Queryable::Reverse(Box::new(From(Box::new(Variable("stocks".to_string()))))));
        let phys_columns = make_table_columns();
        let machine = Machine::new()
            .with_variable("stocks", TableValue(ModelRowCollection::from_rows(vec![
                make_quote(0, &phys_columns, "ABC", "AMEX", 12.33),
                make_quote(1, &phys_columns, "GAS.Q", "OTC", 0.2456),
                make_quote(2, &phys_columns, "BASH", "NYSE", 13.11),
                make_quote(3, &phys_columns, "GOTO", "OTC", 0.1442),
                make_quote(4, &phys_columns, "VAPOR", "NYSE", 0.0289),
            ])));
        let (_, value) = machine.evaluate(&model).unwrap();
        assert_eq!(value, TableValue(ModelRowCollection::from_rows(vec![
            make_quote(4, &phys_columns, "VAPOR", "NYSE", 0.0289),
            make_quote(3, &phys_columns, "GOTO", "OTC", 0.1442),
            make_quote(2, &phys_columns, "BASH", "NYSE", 13.11),
            make_quote(1, &phys_columns, "GAS.Q", "OTC", 0.2456),
            make_quote(0, &phys_columns, "ABC", "AMEX", 12.33),
        ])))
    }

    #[test]
    fn test_select_from_namespace() {
        // create a table with test data
        let (_, phys_columns) =
            create_dataframe("machine.select.stocks");

        // execute the query
        let machine = Machine::new();
        let (_, result) = machine.evaluate(&Inquire(Queryable::Select {
            fields: vec![
                Variable("symbol".into()),
                Variable("exchange".into()),
                Variable("last_sale".into()),
            ],
            from: Some(Box::new(Ns(Box::new(Literal(StringValue("machine.select.stocks".into())))))),
            condition: Some(Box::new(GreaterThan(
                Box::new(Variable("last_sale".into())),
                Box::new(Literal(Float64Value(1.0))),
            ))),
            group_by: None,
            having: None,
            order_by: Some(vec![Variable("symbol".into())]),
            limit: Some(Box::new(Literal(Int64Value(5)))),
        })).unwrap();
        assert_eq!(result, TableValue(ModelRowCollection::from_rows(vec![
            make_quote(0, &phys_columns, "ABC", "AMEX", 11.77),
            make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66),
            make_quote(4, &phys_columns, "BOOM", "NASDAQ", 56.87),
        ])));
    }

    #[test]
    fn test_select_from_variable() {
        let (mrc, phys_columns) = create_memory_table();
        let machine = Machine::new()
            .with_variable("stocks", TableValue(mrc));

        // execute the code
        let (_, result) = machine.evaluate(&Inquire(Queryable::Select {
            fields: vec![
                Variable("symbol".into()),
                Variable("exchange".into()),
                Variable("last_sale".into()),
            ],
            from: Some(Box::new(Variable("stocks".into()))),
            condition: Some(Box::new(LessThan(
                Box::new(Variable("last_sale".into())),
                Box::new(Literal(Float64Value(1.0))),
            ))),
            group_by: None,
            having: None,
            order_by: Some(vec![Variable("symbol".into())]),
            limit: Some(Box::new(Literal(Int64Value(5)))),
        })).unwrap();
        assert_eq!(result, TableValue(ModelRowCollection::from_rows(vec![
            make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
            make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428),
        ])));
    }

    #[test]
    fn test_system_call() {
        let model = FunctionCall {
            fx: Box::new(Variable("syscall".into())),
            args: vec![
                Literal(StringValue("cat".into())),
                Literal(StringValue("LICENSE".into())),
            ],
        };
        let (_, result) = Machine::new().evaluate(&model).unwrap();
        assert_eq!(result, StringValue(r#"MIT License

Copyright (c) 2024 Lawrence Daniels

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"#.into()))
    }

    #[test]
    fn test_type_of() {
        let model = FunctionCall {
            fx: Box::new(Variable("type_of".to_string())),
            args: vec![
                Literal(StringValue("cat".to_string()))
            ],
        };
        let (_, result) = Machine::new().evaluate(&model).unwrap();
        assert_eq!(result, StringValue("String".to_string()));
    }

    #[ignore]
    #[test]
    fn test_update_rows_in_memory() {
        // build the update model
        let model = Mutate(Mutation::Update {
            path: Box::new(Variable("stocks".into())),
            source: Box::new(Via(Box::new(JSONLiteral(vec![
                ("exchange".into(), Literal(StringValue("OTC_BB".into()))),
            ])))),
            condition: Some(
                Box::new(Equal(
                    Box::new(Variable("exchange".into())),
                    Box::new(Literal(StringValue("OTC".into()))),
                ))
            ),
            limit: Some(Box::new(Literal(Int64Value(5)))),
        });
        assert_eq!(model.to_code(), r#"update stocks via {exchange: "OTC_BB"} where exchange == "OTC" limit 5"#);

        // perform the update and verify
        let (mrc, phys_columns) = create_memory_table();
        let machine = Machine::new().with_variable("stocks", TableValue(mrc));
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, RowsAffected(2));

        // retrieve and verify all rows
        let model = From(Box::new(Variable("stocks".into())));
        let (machine, _) = machine.evaluate(&model).unwrap();
        assert_eq!(machine.get("stocks").unwrap(), TableValue(ModelRowCollection::from_rows(vec![
            make_quote(0, &phys_columns, "ABC", "AMEX", 11.77),
            make_quote(1, &phys_columns, "UNO", "OTC_BB", 0.2456),
            make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66),
            make_quote(3, &phys_columns, "GOTO", "OTC_BB", 0.1428),
            make_quote(4, &phys_columns, "BOOM", "NASDAQ", 56.87),
        ])))
    }

    #[test]
    fn test_update_rows_in_namespace() {
        // create a table with test data
        let ns_path = "machine.update.stocks";
        let (df, phys_columns) = create_dataframe(ns_path);

        // create the instruction model
        let model = Mutate(Mutation::Update {
            path: Box::new(Ns(Box::new(Literal(StringValue(ns_path.to_string()))))),
            source: Box::new(Via(Box::new(JSONLiteral(vec![
                ("exchange".into(), Literal(StringValue("OTC_BB".into()))),
            ])))),
            condition: Some(
                Box::new(Equal(
                    Box::new(Variable("exchange".into())),
                    Box::new(Literal(StringValue("OTC".into()))),
                ))
            ),
            limit: Some(Box::new(Literal(Int64Value(5)))),
        });

        // update some rows
        let (_, delta) = Machine::new().evaluate(&model).unwrap();
        assert_eq!(delta, RowsAffected(2));

        // verify the rows
        let rows = df.read_active_rows().unwrap();
        assert_eq!(rows, vec![
            make_quote(0, &phys_columns, "ABC", "AMEX", 11.77),
            make_quote(1, &phys_columns, "UNO", "OTC_BB", 0.2456),
            make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66),
            make_quote(3, &phys_columns, "GOTO", "OTC_BB", 0.1428),
            make_quote(4, &phys_columns, "BOOM", "NASDAQ", 56.87),
        ])
    }

    #[test]
    fn test_truncate_table() {
        let model = Mutate(Mutation::Truncate {
            path: Box::new(Variable("stocks".into())),
            limit: Some(Box::new(Literal(Int64Value(0)))),
        });

        let (mrc, _) = create_memory_table();
        let machine = Machine::new().with_variable("stocks", TableValue(mrc));
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, RowsAffected(1))
    }

    #[test]
    fn test_variables() {
        let machine = Machine::new()
            .with_variable("abc", Int32Value(5))
            .with_variable("xyz", Int32Value(58));
        assert_eq!(machine.get("abc"), Some(Int32Value(5)));
        assert_eq!(machine.get("xyz"), Some(Int32Value(58)));
    }

    #[test]
    fn test_while_loop() {
        let model = While {
            // num < 5
            condition: Box::new(LessThan(
                Box::new(Variable("num".into())),
                Box::new(Literal(Int64Value(5))),
            )),
            // num := num + 1
            code: Box::new(SetVariable("num".into(), Box::new(Plus(
                Box::new(Variable("num".into())),
                Box::new(Literal(Int64Value(1))),
            )))),
        };
        assert_eq!(model.to_code(), "while num < 5 num := num + 1");

        let machine = Machine::new().with_variable("num", Int64Value(0));
        let (machine, _) = machine.evaluate(&model).unwrap();
        assert_eq!(machine.get("num"), Some(Int64Value(5)))
    }

    fn create_memory_table() -> (ModelRowCollection, Vec<TableColumn>) {
        let phys_columns = TableColumn::from_columns(&make_quote_columns()).unwrap();
        let mrc = ModelRowCollection::from_rows(vec![
            make_quote(0, &phys_columns, "ABC", "AMEX", 11.77),
            make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
            make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66),
            make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428),
            make_quote(4, &phys_columns, "BOOM", "NASDAQ", 56.87)]);
        (mrc, phys_columns)
    }

    fn create_dataframe(namespace: &str) -> (DataFrame, Vec<TableColumn>) {
        let columns = make_quote_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let ns = Namespace::parse(namespace).unwrap();
        match fs::remove_file(ns.get_table_file_path()) {
            Ok(_) => {}
            Err(_) => {}
        }

        let mut df = make_dataframe_ns(ns, columns.to_owned()).unwrap();
        assert_eq!(0, df.append(make_quote(0, &phys_columns, "ABC", "AMEX", 11.77)).unwrap());
        assert_eq!(1, df.append(make_quote(1, &phys_columns, "UNO", "OTC", 0.2456)).unwrap());
        assert_eq!(2, df.append(make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66)).unwrap());
        assert_eq!(3, df.append(make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428)).unwrap());
        assert_eq!(4, df.append(make_quote(4, &phys_columns, "BOOM", "NASDAQ", 56.87)).unwrap());
        (df, phys_columns)
    }
}