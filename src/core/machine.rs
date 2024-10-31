////////////////////////////////////////////////////////////////////
//  Machine - state machine module
////////////////////////////////////////////////////////////////////

use std::collections::HashMap;
use std::convert::{From, Into};
use std::fs::File;
use std::io::Read;
use std::ops::{Deref, Neg};
use std::process::Output;
use std::{fs, thread};

use chrono::{Datelike, Local, TimeZone, Timelike};
use log::{error, info, warn};
use reqwest::multipart::{Form, Part};
use reqwest::{Client, RequestBuilder, Response};
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;
use uuid::Uuid;

use shared_lib::fail;

use crate::backdoor::BackDoorKey;
use crate::compiler::Compiler;
use crate::compiler::{fail_expr, fail_unexpected, fail_value};
use crate::cursor::Cursor;
use crate::data_types::DataType::OutcomeType;
use crate::dataframe_config::{DataFrameConfig, HashIndexConfig};
use crate::dataframes::DataFrame;
use crate::errors::Errors;
use crate::errors::Errors::*;
use crate::expression::Conditions::True;
use crate::expression::CreationEntity::{IndexEntity, TableEntity};
use crate::expression::Expression::*;
use crate::expression::Infrastructure::Declare;
use crate::expression::MutateTarget::{IndexTarget, TableTarget};
use crate::expression::{BitwiseOps, Conditions, Expression, UNDEFINED};
use crate::expression::{Directives, Excavation, Infrastructure, Mutation, Queryable};
use crate::file_row_collection::FileRowCollection;
use crate::model_row_collection::ModelRowCollection;
use crate::namespaces::Namespace;
use crate::numbers::NumberValue::*;
use crate::outcomes::Outcomes::Ack;
use crate::outcomes::{OutcomeKind, Outcomes};
use crate::parameter::Parameter;
use crate::row_collection::RowCollection;
use crate::rows::Row;
use crate::structures::*;
use crate::table_columns::Column;
use crate::table_renderer::TableRenderer;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::*;
use crate::web_routes;

pub const MAJOR_VERSION: u8 = 0;
pub const MINOR_VERSION: u8 = 1;

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

    /// (lowest-level constructor) creates a new state machine
    fn construct(
        stack: Vec<TypedValue>,
        variables: HashMap<String, TypedValue>,
    ) -> Self {
        Self { stack, variables }
    }

    /// creates a new completely empty state machine
    pub fn empty() -> Self {
        Self::construct(Vec::new(), HashMap::new())
    }

    /// creates a new state machine
    pub fn new() -> Self {
        Self::empty()
    }

    /// creates a new state machine prepopulated with platform packages
    pub fn new_platform() -> Self {
        use BackDoorKey::*;
        Self::empty()
            //////////////////////// packages ////////////////////////
            .with_package("io", vec![
                ("stderr", BackDoor(BxStdErr)),
                ("stdout", BackDoor(BxStdOut)),
            ])
            .with_package("lang", vec![
                ("assert", BackDoor(BxAssert)),
                ("matches", BackDoor(BxMatches)),
                ("println", BackDoor(BxStdOut)),
                ("type_of", BackDoor(BxTypeOf)),
            ])
            .with_package("os", vec![
                ("call", BackDoor(BxSysCall)),
                ("env", BackDoor(BxEnvVars)),
            ])
            .with_package("str", vec![
                ("format", BackDoor(BxFormat)),
                ("left", BackDoor(BxLeft)),
                ("right", BackDoor(BxRight)),
                ("substring", BackDoor(BxSubstring)),
            ])
            .with_package("util", vec![
                ("day_of", BackDoor(BxTimestampDay)),
                ("hour_of", BackDoor(BxTimestampHour)),
                ("hour12_of", BackDoor(BxTimestampHour12)),
                ("minute_of", BackDoor(BxTimestampMinute)),
                ("month_of", BackDoor(BxTimestampMonth)),
                ("second_of", BackDoor(BxTimestampSecond)),
                ("timestamp", BackDoor(BxTimestamp)),
                ("to_csv", BackDoor(BxToCSV)),
                ("to_json", BackDoor(BxToJSON)),
                ("uuid", BackDoor(BxUUID)),
                ("year_of", BackDoor(BxTimestampYear)),
            ])
            .with_package("vm", vec![
                ("eval", BackDoor(BxEval)),
                ("reset", BackDoor(BxReset)),
                ("serve", BackDoor(BxServe)),
                ("vars", BackDoor(BxVariables)),
                ("version", BackDoor(BxVersion)),
            ])
    }

    /// creates a new state machine prepopulated with platform packages
    /// and default imports
    pub fn new_platform_with_default_imports() -> Self {
        Self::new_platform()
            .import_module_by_name("lang")
            .import_module_by_name("str")
    }

    ////////////////////////////////////////////////////////////////
    //  static methods
    ////////////////////////////////////////////////////////////////

    fn create_env_table() -> TypedValue {
        use std::env;
        let mut mrc = ModelRowCollection::construct(&vec![
            Parameter::new("key", Some("String(256)".into()), None),
            Parameter::new("value", Some("String(8192)".into()), None)
        ]);
        for (key, value) in env::vars() {
            if let ErrorValue(err) = mrc.append_row(Row::new(0, vec![
                StringValue(key), StringValue(value)
            ])) {
                return ErrorValue(err);
            }
        }

        TableValue(mrc)
    }

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

    fn extract_string_tuples(value: TypedValue) -> std::io::Result<Vec<(String, String)>> {
        Self::extract_value_tuples(value)
            .map(|values| values.iter()
                .map(|(k, v)| (k.to_string(), v.unwrap_value()))
                .collect())
    }

    fn extract_value_tuples(value: TypedValue) -> std::io::Result<Vec<(String, TypedValue)>> {
        match value {
            SoftStructureValue(structure) => Ok(structure.get_tuples()),
            z => fail_value(Errors::TypeMismatch("Schemaless".into(), z.to_code()).to_string(), &z),
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
                Ok((machine, ErrorValue(CollectionExpected(z.to_code()))))
        }
    }

    fn orchestrate_io(
        machine: Self,
        table: TypedValue,
        fields: &Vec<Expression>,
        values: &Vec<Expression>,
        condition: &Option<Conditions>,
        limit: TypedValue,
        f: fn(Self, &mut DataFrame, &Vec<Expression>, &Vec<Expression>, &Option<Conditions>, TypedValue) -> std::io::Result<(Self, TypedValue)>,
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
                Ok((machine, ErrorValue(CollectionExpected(z.to_code()))))
        }
    }

    fn split(columns: &Vec<Column>, row: &Row) -> (Vec<Expression>, Vec<Expression>) {
        let my_fields = columns.iter()
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

    fn assume_table_value<A>(
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
                fail_value(format!("{} is not a table", z), z)
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
                fail_value(format!("{} is not a table", z), z)
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
                    Err(err) => (self.to_owned(), ErrorValue(Exact(err.to_string())))
                }
            TableValue(mrc) => {
                let rc = Box::new(mrc.to_owned());
                if is_csv { self.evaluate_to_csv(rc) } else { self.evaluate_to_json(rc) }
            }
            _ => (self.to_owned(), ErrorValue(Exact(format!("Cannot convert to {}", if is_csv { "CSV" } else { "JSON" }))))
        }
    }

    /// converts a [Response] to a [TypedValue]
    pub async fn convert_response(
        &self,
        response: Response,
        is_header_only: bool,
    ) -> TypedValue {
        if response.status().is_success() {
            if is_header_only {
                let header_keys = response.headers().keys()
                    .map(|k| k.to_string())
                    .collect::<Vec<_>>();
                let header_values = response.headers().values()
                    .map(|hv| match hv.to_str() {
                        Ok(s) => TypedValue::wrap_value(s).unwrap_or(Undefined),
                        Err(e) => ErrorValue(Exact(e.to_string()))
                    }).collect::<Vec<_>>();
                SoftStructureValue(SoftStructure::from_tuples(header_keys.iter().zip(header_values.iter())
                    .map(|(k, v)| (k.to_owned(), v.to_owned()))
                    .collect::<Vec<_>>()))
            } else {
                match response.text().await {
                    Ok(body) => {
                        match Compiler::compile_script(body.as_str()) {
                            Ok(expr) => {
                                match self.evaluate(&expr) {
                                    Ok((_, Undefined)) => SoftStructureValue(SoftStructure::empty()),
                                    Ok((_, value)) => value,
                                    Err(_) => StringValue(body)
                                }
                            }
                            _ => StringValue(body)
                        }
                    }
                    Err(err) => ErrorValue(Exact(format!("Error reading response body: {}", err))),
                }
            }
        } else {
            ErrorValue(Exact(format!("Request failed with status: {}", response.status())))
        }
    }

    /// converts a [std::io::Result<(Self, TypedValue)>] to a [(Self, TypedValue)]
    pub fn convert_result(
        &self,
        result: std::io::Result<(Self, TypedValue)>,
    ) -> (Self, TypedValue) {
        match result {
            Ok(value) => value,
            Err(err) => (self.clone(), ErrorValue(Exact(err.to_string())))
        }
    }

    /// evaluates the specified [Expression]; returning a [TypedValue] result.
    pub fn evaluate(
        &self,
        expression: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        use crate::expression::Expression::*;
        match expression {
            ArrayExpression(items) => self.evaluate_array(items),
            AsValue(name, expr) => {
                let (machine, tv) = self.evaluate(expr)?;
                Ok((machine.with_variable(name, tv.to_owned()), tv))
            }
            BitwiseOp(bitwise) => self.evaluate_bitwise(bitwise),
            CodeBlock(ops) => self.evaluate_scope(ops),
            Parameters(columns) => Ok(self.evaluate_column_set(columns)),
            Condition(condition) => self.evaluate_cond(condition),
            Directive(d) => self.evaluate_directive(d),
            Divide(a, b) =>
                self.evaluate_inline_2(a, b, |aa, bb| aa / bb),
            ElementAt(a, b) => self.evaluate_index_of_collection(a, b),
            Extraction(a, b) => self.evaluate_extraction(a, b),
            Factorial(a) => self.evaluate_inline_1(a, |aa| aa.factorial()),
            Feature { title, scenarios } => self.evaluate_feature(title, scenarios),
            From(src) => self.evaluate_table_row_query(src, &True, Undefined),
            FunctionCall { fx, args } =>
                self.evaluate_function_call(fx, args),
            HTTP { .. } =>
                Ok((self.to_owned(), ErrorValue(AsynchronousContextRequired))),
            If { condition, a, b } =>
                self.evaluate_if_then_else(condition, a, b),
            Import(args) => self.evaluate_import(args),
            Include(path) => self.evaluate_include(path),
            JSONExpression(items) => self.evaluate_json(items),
            Literal(value) => Ok((self.to_owned(), value.to_owned())),
            Minus(a, b) =>
                self.evaluate_inline_2(a, b, |aa, bb| aa - bb),
            Module(name, ops) => self.evaluate_module(name, ops),
            Modulo(a, b) =>
                self.evaluate_inline_2(a, b, |aa, bb| aa % bb),
            Multiply(a, b) =>
                self.evaluate_inline_2(a, b, |aa, bb| aa * bb),
            Neg(a) => Ok(self.evaluate_neg(a)),
            Ns(a) => self.evaluate_table_ns(a),
            Plus(a, b) =>
                self.evaluate_inline_2(a, b, |aa, bb| aa + bb),
            Pow(a, b) =>
                self.evaluate_inline_2(a, b, |aa, bb| aa.pow(&bb).unwrap_or(Undefined)),
            Quarry(payload) => match payload {
                Excavation::Construct(i) => self.evaluate_infrastructure(i),
                Excavation::Query(q) => self.evaluate_inquiry(q),
                Excavation::Mutate(m) => self.evaluate_mutation(m),
            },
            Range(a, b) =>
                self.evaluate_inline_2(a, b, |aa, bb| aa.range(&bb).unwrap_or(Undefined)),
            Return(a) => {
                let (machine, result) = self.evaluate_array(a)?;
                Ok((machine, result))
            }
            Scenario { .. } => Ok((self.to_owned(), ErrorValue(Exact("Scenario should not be called directly".to_string())))),
            SetVariable(name, expr) => {
                let (machine, value) = self.evaluate(expr)?;
                Ok((machine.set(name, value), Outcome(Ack)))
            }
            StructureImpl(name, ops) => Ok(self.evaluate_structure_impl(name, ops)),
            Variable(name) => Ok((self.to_owned(), self.get(&name).unwrap_or(Undefined))),
            Via(src) => self.evaluate_table_row_query(src, &True, Undefined),
            While { condition, code } =>
                self.evaluate_while(condition, code),
        }
    }

    /// evaluates the specified [Expression]; returning a [TypedValue] result.
    pub fn evaluate_cond(
        &self,
        condition: &Conditions,
    ) -> std::io::Result<(Self, TypedValue)> {
        use crate::expression::Conditions::*;
        match condition {
            And(a, b) =>
                self.evaluate_inline_2(a, b, |aa, bb| aa.and(&bb).unwrap_or(Undefined)),
            Between(a, b, c) =>
                self.evaluate_inline_3(a, b, c, |aa, bb, cc| Boolean((aa >= bb) && (aa <= cc))),
            Betwixt(a, b, c) =>
                self.evaluate_inline_3(a, b, c, |aa, bb, cc| Boolean((aa >= bb) && (aa < cc))),
            Contains(a, b) => self.evaluate_contains(a, b),
            Equal(a, b) =>
                self.evaluate_inline_2(a, b, |aa, bb| Boolean(aa == bb)),
            False => Ok((self.to_owned(), Boolean(false))),
            GreaterThan(a, b) =>
                self.evaluate_inline_2(a, b, |aa, bb| Boolean(aa > bb)),
            GreaterOrEqual(a, b) =>
                self.evaluate_inline_2(a, b, |aa, bb| Boolean(aa >= bb)),
            LessThan(a, b) =>
                self.evaluate_inline_2(a, b, |aa, bb| Boolean(aa < bb)),
            LessOrEqual(a, b) =>
                self.evaluate_inline_2(a, b, |aa, bb| Boolean(aa <= bb)),
            Not(a) => self.evaluate_inline_1(a, |aa| !aa),
            NotEqual(a, b) =>
                self.evaluate_inline_2(a, b, |aa, bb| Boolean(aa != bb)),
            Or(a, b) =>
                self.evaluate_inline_2(a, b, |aa, bb| aa.or(&bb).unwrap_or(Undefined)),
            True => Ok((self.to_owned(), Boolean(true))),
        }
    }

    /// evaluates the specified [Directives]; returning a [TypedValue] result.
    pub fn evaluate_directive(
        &self,
        directive: &Directives,
    ) -> std::io::Result<(Self, TypedValue)> {
        match directive {
            Directives::MustAck(a) => self.evaluate_directive_ack(a),
            Directives::MustDie(a) => self.evaluate_directive_die(a),
            Directives::MustIgnoreAck(a) => self.evaluate_directive_ignore_ack(a),
            Directives::MustNotAck(a) => self.evaluate_directive_not_ack(a),
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
                          errors.push(ErrorValue(ParameterExpected(expr.to_code())));
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
                Ok((machine.set(name, value), Outcome(Ack)))
            }
            other => self.evaluate(other)
        }
    }

    fn evaluate_back_door(
        &self,
        bdk: &BackDoorKey,
        args: Vec<TypedValue>,
    ) -> (Self, TypedValue) {
        use BackDoorKey::*;
        match bdk {
            BackDoorKey::BxAssert => self.evaluate_back_door_fn1(args, Machine::do_assert),
            BackDoorKey::BxEnvVars => (self.clone(), Self::create_env_table()),
            BackDoorKey::BxEval => self.evaluate_back_door_fn1(args, Machine::do_eval),
            BackDoorKey::BxFormat => self.evaluate_format_string(args),
            BackDoorKey::BxLeft => self.evaluate_back_door_fn2(args, |ms, s, n| ms.evaluate_string_left(s, n)),
            BackDoorKey::BxMatches => self.evaluate_back_door_fn2(args, |ms, a, b| (ms, a.matches(b))),
            BackDoorKey::BxReset => self.evaluate_back_door_fn0(args, |_| (Machine::new(), Outcome(Ack))),
            BackDoorKey::BxRight => self.evaluate_back_door_fn2(args, |ms, s, n| ms.evaluate_string_right(s, n)),
            BackDoorKey::BxServe => self.evaluate_back_door_fn1(args, Machine::do_serve),
            BackDoorKey::BxStdErr => self.evaluate_back_door_fn1(args, Machine::do_stdout),
            BackDoorKey::BxStdOut => self.evaluate_back_door_fn1(args, Machine::do_stdout),
            BackDoorKey::BxSubstring => self.evaluate_back_door_fn3(args, |ms, s, a, b| ms.evaluate_substring(s, a, b)),
            BackDoorKey::BxSysCall => self.convert_result(self.evaluate_syscall(args)),
            BackDoorKey::BxTimestamp => self.convert_result(self.evaluate_timestamp(args)),
            BackDoorKey::BxTimestampDay => self.evaluate_back_door_fn1bdk(args, bdk, Machine::do_date_part),
            BackDoorKey::BxTimestampHour => self.evaluate_back_door_fn1bdk(args, bdk, Machine::do_date_part),
            BackDoorKey::BxTimestampHour12 => self.evaluate_back_door_fn1bdk(args, bdk, Machine::do_date_part),
            BackDoorKey::BxTimestampMinute => self.evaluate_back_door_fn1bdk(args, bdk, Machine::do_date_part),
            BackDoorKey::BxTimestampMonth => self.evaluate_back_door_fn1bdk(args, bdk, Machine::do_date_part),
            BackDoorKey::BxTimestampSecond => self.evaluate_back_door_fn1bdk(args, bdk, Machine::do_date_part),
            BackDoorKey::BxTimestampYear => self.evaluate_back_door_fn1bdk(args, bdk, Machine::do_date_part),
            BackDoorKey::BxToCSV => self.evaluate_back_door_fn1(args, |ms, a| ms.convert_to_csv_or_json(a, true)),
            BackDoorKey::BxToJSON => self.evaluate_back_door_fn1(args, |ms, a| ms.convert_to_csv_or_json(a, false)),
            BackDoorKey::BxTypeOf => self.evaluate_back_door_fn1(args, |ms, a| (ms, StringValue(a.get_type_name()))),
            BackDoorKey::BxUUID => self.convert_result(self.evaluate_uuid(args)),
            BackDoorKey::BxVariables => self.evaluate_back_door_fn0(args, |ms| (ms.clone(), TableValue(ms.get_variables()))),
            BackDoorKey::BxVersion => self.evaluate_back_door_fn0(args, |ms| (ms, StringValue(format!("{MAJOR_VERSION}.{MINOR_VERSION}")))),
        }
    }

    fn do_assert(ms: Machine, value: &TypedValue) -> (Machine, TypedValue) {
        match value {
            ErrorValue(msg) => (ms, ErrorValue(msg.to_owned())),
            Boolean(false) => (ms, ErrorValue(AssertionError("true".to_string(), "false".to_string()))),
            z => (ms, z.to_owned())
        }
    }

    fn do_date_part(
        ms: Machine,
        value: &TypedValue,
        bdk: &BackDoorKey,
    ) -> (Machine, TypedValue) {
        match value {
            DateValue(epoch_millis) => {
                let datetime = {
                    let seconds = epoch_millis / 1000;
                    let millis_part = epoch_millis % 1000;
                    Local.timestamp(seconds, (millis_part * 1_000_000) as u32)
                };
                match bdk {
                    BackDoorKey::BxTimestampDay => (ms, Number(U32Value(datetime.day()))),
                    BackDoorKey::BxTimestampHour => (ms, Number(U32Value(datetime.hour()))),
                    BackDoorKey::BxTimestampHour12 => (ms, Number(U32Value(datetime.hour12().1))),
                    BackDoorKey::BxTimestampMinute => (ms, Number(U32Value(datetime.minute()))),
                    BackDoorKey::BxTimestampMonth => (ms, Number(U32Value(datetime.second()))),
                    BackDoorKey::BxTimestampSecond => (ms, Number(U32Value(datetime.second()))),
                    BackDoorKey::BxTimestampYear => (ms, Number(I32Value(datetime.year()))),
                    other => (ms, ErrorValue(Syntax(other.to_code())))
                }
            }
            other => (ms, ErrorValue(DateExpected(other.to_code())))
        }
    }

    fn do_eval(ms: Machine, value: &TypedValue) -> (Machine, TypedValue) {
        match value {
            StringValue(ql) =>
                match Compiler::compile_script(ql.as_str()) {
                    Ok(opcode) =>
                        match ms.evaluate(&opcode) {
                            Ok((machine, tv)) => (machine, tv),
                            Err(err) => (ms, ErrorValue(Exact(err.to_string())))
                        }
                    Err(err) => (ms, ErrorValue(Exact(err.to_string())))
                }
            x => (ms, ErrorValue(StringExpected(x.get_type_name())))
        }
    }

    fn do_serve(ms: Machine, value: &TypedValue) -> (Machine, TypedValue) {
        match ms.evaluate_serve(value) {
            Ok(mtv) => mtv,
            Err(err) => (ms.to_owned(), ErrorValue(Exact(err.to_string())))
        }
    }

    fn do_stdout(ms: Machine, value: &TypedValue) -> (Machine, TypedValue) {
        println!("{}", value.unwrap_value());
        (ms, Outcome(Ack))
    }

    fn do_stderr(ms: Machine, value: &TypedValue) -> (Machine, TypedValue) {
        println!("{}", value.unwrap_value());
        (ms, Outcome(Ack))
    }

    fn evaluate_back_door_fn0(
        &self,
        args: Vec<TypedValue>,
        f: fn(Self) -> (Self, TypedValue),
    ) -> (Self, TypedValue) {
        let ms = self.to_owned();
        match args.len() {
            0 => f(ms),
            n => (ms, ErrorValue(ArgumentsMismatched(0, n)))
        }
    }

    fn evaluate_back_door_fn1(
        &self,
        args: Vec<TypedValue>,
        f: fn(Self, &TypedValue) -> (Machine, TypedValue),
    ) -> (Self, TypedValue) {
        let ms = self.to_owned();
        match args.as_slice() {
            [value] => f(ms, value),
            _ => (ms, ErrorValue(ArgumentsMismatched(1, args.len())))
        }
    }

    fn evaluate_back_door_fn1bdk(
        &self,
        args: Vec<TypedValue>,
        bdk: &BackDoorKey,
        f: fn(Self, &TypedValue, &BackDoorKey) -> (Self, TypedValue),
    ) -> (Self, TypedValue) {
        let ms = self.to_owned();
        match args.as_slice() {
            [a] => f(ms, a, bdk),
            _ => (ms, ErrorValue(ArgumentsMismatched(1, args.len())))
        }
    }

    fn evaluate_back_door_fn2(
        &self,
        args: Vec<TypedValue>,
        f: fn(Self, &TypedValue, &TypedValue) -> (Self, TypedValue),
    ) -> (Self, TypedValue) {
        let ms = self.to_owned();
        match args.as_slice() {
            [a, b] => f(ms, a, b),
            _ => (ms, ErrorValue(ArgumentsMismatched(2, args.len())))
        }
    }

    fn evaluate_back_door_fn3(
        &self,
        args: Vec<TypedValue>,
        f: fn(Self, &TypedValue, &TypedValue, &TypedValue) -> (Self, TypedValue),
    ) -> (Self, TypedValue) {
        let ms = self.to_owned();
        match args.as_slice() {
            [a, b, c] => f(ms, a, b, c),
            _ => (ms, ErrorValue(ArgumentsMismatched(3, args.len())))
        }
    }

    /// evaluates the specified [Expression]; returning a [TypedValue] result.
    pub fn evaluate_bitwise(
        &self,
        bitwise: &BitwiseOps,
    ) -> std::io::Result<(Self, TypedValue)> {
        match bitwise {
            BitwiseOps::And(a, b) =>
                self.evaluate_inline_2(a, b, |aa, bb| aa & bb),
            BitwiseOps::Or(a, b) =>
                self.evaluate_inline_2(a, b, |aa, bb| aa | bb),
            BitwiseOps::ShiftLeft(a, b) =>
                self.evaluate_inline_2(a, b, |aa, bb| aa << bb),
            BitwiseOps::ShiftRight(a, b) =>
                self.evaluate_inline_2(a, b, |aa, bb| aa >> bb),
            BitwiseOps::Xor(a, b) =>
                self.evaluate_inline_2(a, b, |aa, bb| aa ^ bb),
        }
    }

    fn evaluate_column_set(
        &self,
        columns: &Vec<Parameter>,
    ) -> (Machine, TypedValue) {
        let machine = self.to_owned();
        let values = columns.iter()
            .map(|c| machine.variables.get(c.get_name())
                .map(|c| c.to_owned())
                .unwrap_or(Undefined))
            .collect::<Vec<TypedValue>>();
        (machine, Array(values))
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
                Ok((machine, ErrorValue(OutcomeExpected(v.to_code()))))
        }
    }

    fn evaluate_directive_die(
        &self,
        expr: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        info!("{}", expr.to_code());
        let (machine, value) = self.evaluate(expr)?;
        Ok((machine, ErrorValue(Exact(value.unwrap_value()))))
    }

    fn evaluate_directive_ignore_ack(
        &self,
        expr: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        info!("{}", expr.to_code());
        let (machine, _) = self.evaluate(expr)?;
        Ok((machine, Outcome(Ack)))
    }

    fn evaluate_directive_not_ack(
        &self,
        expr: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        info!("{}", expr.to_code());
        let (machine, value) = self.evaluate(expr)?;
        match value {
            v if v.is_ok() =>
                Ok((machine, ErrorValue(Exact(format!("Expected a non-success value, but got {}", &v))))),
            v => Ok((machine, v))
        }
    }

    fn evaluate_extraction(
        &self,
        object: &Box<Expression>,
        field: &Box<Expression>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, obj) = self.evaluate(object)?;
        match obj {
            ErrorValue(err) => Ok((ms, ErrorValue(err))),
            SoftStructureValue(s) => self.evaluate_extraction_schemaless(field, &s),
            HardStructureValue(s) => self.evaluate_extraction_structure(field, &s),
            z => fail(format!("Illegal object {}", z.to_code()))
        }
    }

    fn evaluate_extraction_schemaless(
        &self,
        field: &Expression,
        schemaless: &SoftStructure,
    ) -> std::io::Result<(Self, TypedValue)> {
        let ms = self.to_owned();
        match field {
            // method call: math::compute(5, 8)
            FunctionCall { fx, args } =>
                schemaless.pollute(ms).evaluate_function_call(fx, args),
            // { symbol:"AAA", price:123.45 }::symbol
            Variable(name) => Ok((ms, schemaless.get(name))),
            z =>
                fail(format!("Illegal field {} for object {}",
                             z.to_code(), schemaless.to_code()))
        }
    }

    fn evaluate_extraction_structure(
        &self,
        field: &Expression,
        structure: &HardStructure,
    ) -> std::io::Result<(Self, TypedValue)> {
        let ms = self.to_owned();
        match field {
            // method call: math::compute(5, 8)
            FunctionCall { fx, args } =>
                structure.pollute(ms).evaluate_function_call(fx, args),
            // stock::symbol
            Variable(name) => Ok((ms, structure.get(name))),
            z => fail(format!("Illegal field {} for structure {}",
                              z.to_code(), HardStructureValue(structure.to_owned()).to_code()))
        }
    }

    fn evaluate_feature(
        &self,
        title: &Box<Expression>,
        scenarios: &Vec<Expression>,
    ) -> std::io::Result<(Self, TypedValue)> {
        // create a table and capture function to store the verification report
        let verification_columns = Column::from_parameters(&vec![
            Parameter::new("level", Some("u16".into()), None),
            Parameter::new("item", Some("String(256)".into()), None),
            Parameter::new("passed", Some("Boolean".into()), None),
            Parameter::new("result", Some("String(256)".into()), None),
        ])?;
        let mut report = ModelRowCollection::new(verification_columns.to_owned());
        let mut capture = |level: u16, text: String, passed: bool, result: TypedValue| {
            let outcome = report.push_row(Row::new(0, vec![
                Number(U16Value(level)), StringValue(text), Boolean(passed), result,
            ]));
            let count = match outcome {
                Number(n) => n.to_usize(),
                Outcome(oc) => oc.to_usize(),
                ErrorValue(err) => return fail(err.to_string()),
                _ => 0
            };
            Ok((self, count))
        };

        // feature processing
        let (mut ms, title) = self.evaluate(title)?;
        capture(0, title.unwrap_value(), true, Outcome(Ack))?;

        // scenario processing
        for scenario in scenarios {
            match &scenario {
                // scenarios require specialized processing
                Scenario { title, verifications, inherits } => {
                    // scenario::title
                    let (msb, subtitle) = ms.evaluate(title)?;
                    ms = msb;

                    // update the report
                    capture(1, subtitle.unwrap_value(), true, Outcome(Ack))?;

                    // verification processing
                    let level: u16 = 2;
                    let mut errors = 0;
                    for verification in verifications {
                        let (msb, result) = ms.evaluate(verification)?;
                        ms = msb;
                        match result {
                            ErrorValue(msg) => {
                                capture(level, verification.to_code(), false, ErrorValue(msg))?;
                                errors += 1;
                            }
                            result => {
                                capture(level, verification.to_code(), true, result)?;
                            }
                        };
                    }
                }
                other => {
                    let (msb, result) = ms.evaluate(other)?;
                    ms = msb;
                    capture(2, other.to_code(), true, result)?;
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
                    (ms, ErrorValue(StringExpected(other.to_code())))
            }
        }
    }

    fn evaluate_function_call(
        &self,
        fx: &Expression,
        args: &Vec<Expression>,
    ) -> std::io::Result<(Self, TypedValue)> {
        // extract the arguments
        if let (ms, Array(args)) = self.evaluate_array(args)? {
            // evaluate the anonymous- or named-function
            match ms.evaluate(fx)? {
                (ms, BackDoor(bdf)) =>
                    Ok(ms.evaluate_back_door(&bdf, args)),
                (ms, Function { params, code }) =>
                    ms.evaluate_function_arguments(params, args).evaluate(&code),
                (_, z) => fail(format!("'{}' is not a function ({})", fx.to_code(), z))
            }
        } else {
            fail(format!("Function arguments expected, but got {}", ArrayExpression(args.to_owned())))
        }
    }

    fn evaluate_function_arguments(
        &self,
        params: Vec<Parameter>,
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
            body.map(|tv| tv.to_json().to_string()),
            match headers {
                Some(v) => Self::extract_string_tuples(v)?,
                None => Vec::new()
            },
            match multipart {
                Some(SoftStructureValue(soft)) =>
                    Some(soft.get_tuples().iter().fold(Form::new(), |form, (name, value)| {
                        form.part(name.to_owned(), Part::text(value.unwrap_value()))
                    })),
                Some(HardStructureValue(hard)) =>
                    Some(hard.get_tuples().iter().fold(Form::new(), |form, (name, value)| {
                        form.part(name.to_owned(), Part::text(value.unwrap_value()))
                    })),
                Some(z) => return Ok((ms, ErrorValue(TypeMismatch("Object".into(), z.to_code())))),
                None => None
            },
        ).await
    }

    async fn evaluate_http_delete(
        &self,
        url: &str,
        headers: Vec<(String, String)>,
        body_opt: Option<String>,
    ) -> std::io::Result<(Self, TypedValue)> {
        Ok((self.to_owned(), self.evaluate_http_rest_call(Client::new().delete(url), headers, body_opt, false).await))
    }

    async fn evaluate_http_get(
        &self,
        url: &str,
        headers: Vec<(String, String)>,
        body_opt: Option<String>,
    ) -> std::io::Result<(Self, TypedValue)> {
        Ok((self.to_owned(), self.evaluate_http_rest_call(Client::new().get(url), headers, body_opt, false).await))
    }

    async fn evaluate_http_head(
        &self,
        url: &str,
        headers: Vec<(String, String)>,
        body_opt: Option<String>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let value = self.evaluate_http_rest_call(Client::new().head(url), headers, body_opt, true).await;
        Ok((self.to_owned(), value))
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
            method => Ok((self.to_owned(), ErrorValue(Exact(format!("Invalid HTTP method '{method}'")))))
        }
    }

    async fn evaluate_http_patch(
        &self,
        url: &str,
        headers: Vec<(String, String)>,
        body_opt: Option<String>,
    ) -> std::io::Result<(Self, TypedValue)> {
        Ok((self.to_owned(), self.evaluate_http_rest_call(Client::new().patch(url), headers, body_opt, false).await))
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
        Ok((self.to_owned(), self.evaluate_http_rest_call(request, headers, body_opt, false).await))
    }

    async fn evaluate_http_put(
        &self,
        url: &str,
        headers: Vec<(String, String)>,
        body_opt: Option<String>,
    ) -> std::io::Result<(Self, TypedValue)> {
        Ok((self.to_owned(), self.evaluate_http_rest_call(Client::new().put(url), headers, body_opt, false).await))
    }

    async fn evaluate_http_rest_call(
        &self,
        request: RequestBuilder,
        headers: Vec<(String, String)>,
        body_opt: Option<String>,
        is_header_only: bool,
    ) -> TypedValue {
        let request = Self::enrich_request(request, body_opt);
        let request = headers.iter().fold(request, |request, (k, v)| {
            request.header(k, v)
        });
        match request.send().await {
            Ok(response) => self.convert_response(response, is_header_only).await,
            Err(err) => ErrorValue(Exact(format!("Error making request: {}", err))),
        }
    }

    /// Evaluates an if-then-else expression
    fn evaluate_if_then_else(
        &self,
        condition: &Box<Expression>,
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

    fn evaluate_import(
        &self,
        args: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        match self.evaluate(args)? {
            (ms, StringValue(pkg)) =>
                ms.evaluate_import_fold(vec![StringValue(pkg)]),
            (ms, Array(args)) =>
                ms.evaluate_import_fold(args),
            (ms, other) =>
                Ok((ms, ErrorValue(StringExpected(other.to_code()))))
        }
    }

    /// Produces an aggregate [Machine] instance containing
    /// the specified imports
    fn evaluate_import_fold(
        &self,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let mut ms = self.to_owned();
        for arg in args {
            match arg {
                StringValue(pkg) =>
                    match ms.get(pkg.as_str()) {
                        None =>
                            return Ok((ms, ErrorValue(PackageNotFound(pkg)))),
                        Some(SoftStructureValue(schemaless)) =>
                            ms = schemaless.get_tuples().iter().fold(ms, |ms, (name, value)| {
                                ms.with_variable(name, value.to_owned())
                            }),
                        Some(HardStructureValue(structure)) =>
                            ms = structure.pollute(ms),
                        Some(other) =>
                            return Ok((ms, ErrorValue(Syntax(other.to_code()))))
                    }
                other =>
                    return Ok((ms, ErrorValue(Syntax(other.to_code()))))
            }
        }
        Ok((ms, Outcome(Ack)))
    }

    fn evaluate_infrastructure(
        &self,
        expression: &Infrastructure,
    ) -> std::io::Result<(Self, TypedValue)> {
        use crate::expression::Infrastructure::*;
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

    fn evaluate_inquiry(
        &self,
        expression: &Queryable,
    ) -> std::io::Result<(Self, TypedValue)> {
        use crate::expression::Queryable::*;
        match expression {
            Describe(table) => self.evaluate_table_describe(table),
            Limit { from, limit } => {
                let (machine, limit) = self.evaluate(limit)?;
                machine.evaluate_table_row_query(from, &True, limit)
            }
            Reverse(a) => self.evaluate_table_row_reverse(a),
            Select { fields, from, condition, group_by, having, order_by, limit } =>
                self.evaluate_table_row_selection(fields, from, condition, group_by, having, order_by, limit),
            Where { from, condition } =>
                self.evaluate_table_row_query(from, condition, Undefined),
        }
    }

    fn evaluate_include(
        &self,
        path: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        // evaluate the script path
        let (machine, path_value) = self.evaluate(path)?;
        if let StringValue(script_path) = path_value {
            // read the script file contents into the string
            let mut file = File::open(script_path)?;
            let mut script_code = String::new();
            file.read_to_string(&mut script_code)?;
            // compile and execute the string (script code)
            let opcode = Compiler::compile_script(script_code.as_str())?;
            machine.evaluate(&opcode)
        } else {
            Ok((machine, ErrorValue(TypeMismatch("String".to_string(), path_value.get_type_name()))))
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
                let idx = index.to_usize();
                if idx < items.len() { items[idx].to_owned() } else {
                    ErrorValue(IndexOutOfRange("Array".to_string(), idx, items.len()))
                }
            }
            NamespaceValue(d, s, n) => {
                let id = index.to_usize();
                let ns = Namespace::new(d, s, n);
                let frc = FileRowCollection::open(&ns)?;
                match frc.read_one(id)? {
                    Some(row) => HardStructureValue(HardStructure::from_row(frc.get_columns().clone(), &row)),
                    None => HardStructureValue(HardStructure::new(frc.get_columns().to_owned(), Vec::new()))
                }
            }
            StringValue(string) => {
                let idx = index.to_usize();
                if idx < string.len() { StringValue(string[idx..idx].to_string()) } else { ErrorValue(IndexOutOfRange("String".to_string(), idx, string.len())) }
            }
            HardStructureValue(structure) => {
                let idx = index.to_usize();
                let values = structure.get_values();
                if idx < values.len() { values[idx].to_owned() } else { ErrorValue(IndexOutOfRange("Structure element".to_string(), idx, values.len())) }
            }
            TableValue(mrc) => {
                let id = index.to_usize();
                match mrc.read_one(id)? {
                    Some(row) => HardStructureValue(HardStructure::from_row(mrc.get_columns().clone(), &row)),
                    None => HardStructureValue(HardStructure::new(mrc.get_columns().to_owned(), Vec::new()))
                }
            }
            other =>
                ErrorValue(TypeMismatch("Table|Array".into(), other.to_code()))
        };
        Ok((machine, value))
    }

    /// evaluates expression `a` then applies function `f`. ex: f(a)
    fn evaluate_inline_1(
        &self,
        a: &Expression,
        f: fn(TypedValue) -> TypedValue,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, aa) = self.evaluate(a)?;
        Ok((machine, f(aa)))
    }

    /// evaluates expressions `a` and `b` then applies function `f`. ex: f(a, b)
    fn evaluate_inline_2(
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
    fn evaluate_inline_3(
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

    fn evaluate_json(
        &self,
        items: &Vec<(String, Expression)>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let mut elems = Vec::new();
        for (name, expr) in items {
            let (_, value) = self.evaluate(expr)?;
            elems.push((name.to_string(), value))
        }
        Ok((self.to_owned(), SoftStructureValue(SoftStructure::from_tuples(elems))))
    }

    fn evaluate_module(
        &self,
        name: &str,
        ops: &Vec<Expression>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let structure = HardStructure::empty();
        let result =
            ops.iter().fold(HardStructureValue(structure), |tv, op| match tv {
                ErrorValue(msg) => ErrorValue(msg),
                HardStructureValue(structure) =>
                    match op {
                        SetVariable(name, expr) =>
                            match self.evaluate(expr) {
                                Ok((_, value)) => HardStructureValue(structure.with_variable(name, value)),
                                Err(err) => ErrorValue(Exact(err.to_string()))
                            },
                        z => ErrorValue(IllegalExpression(z.to_code()))
                    },
                z => ErrorValue(StructExpected(name.to_string(), z.to_code()))
            });
        Ok((self.with_variable(name, result), Outcome(Ack)))
    }

    fn evaluate_mutation(
        &self,
        expression: &Mutation,
    ) -> std::io::Result<(Self, TypedValue)> {
        use crate::expression::Mutation::*;
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

    /// evaluates the specified [Expression]; returning a negative [TypedValue] result.
    fn evaluate_neg(&self, expr: &Expression) -> (Self, TypedValue) {
        match self.evaluate(expr) {
            Ok((machine, result)) => (machine, result.neg()),
            Err(err) => (self.to_owned(), ErrorValue(Exact(err.to_string())))
        }
    }

    /// evaluates the specified [Expression]; returning a [TypedValue] result.
    fn evaluate_optional(&self, expr: &Option<Box<Expression>>) -> std::io::Result<(Self, TypedValue)> {
        match expr {
            Some(item) => self.evaluate(item),
            None => Ok((self.to_owned(), Undefined))
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
                               Err(err) => (m, ErrorValue(Exact(err.to_string())))
                           }))
    }

    fn evaluate_serve(
        &self,
        port: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        use actix_web::web;
        use crate::rest_server::*;
        let port = port.to_usize();
        thread::spawn(move || {
            let server = actix_web::HttpServer::new(move || web_routes!(SharedState::new()))
                .bind(format!("{}:{}", "0.0.0.0", port))
                .expect(format!("Can not bind to port {port}").as_str())
                .run();
            Runtime::new()
                .expect("Failed to create a Runtime instance")
                .block_on(server)
                .expect(format!("Failed while blocking on port {port}").as_str());
        });
        Ok((self.to_owned(), Outcome(Ack)))
    }

    fn evaluate_substring(
        &self,
        string: &TypedValue,
        a: &TypedValue,
        b: &TypedValue,
    ) -> (Machine, TypedValue) {
        (self.to_owned(), match string {
            StringValue(s) =>
                match (a, b) {
                    (Number(na), Number(nb)) =>
                        StringValue(s[na.to_usize()..nb.to_usize()].to_string()),
                    (..) => Undefined
                },
            _ => Undefined
        })
    }

    fn evaluate_string_left(
        &self,
        string: &TypedValue,
        n_chars: &TypedValue,
    ) -> (Machine, TypedValue) {
        let result = match string {
            StringValue(s) =>
                match n_chars {
                    Number(nv) if nv.to_i64() < 0 =>
                        self.evaluate_string_right(string, &Number(I64Value(-nv.to_i64()))).1,
                    Number(nv) =>
                        StringValue(s[0..nv.to_usize()].to_string()),
                    _ => Undefined
                },
            _ => Undefined
        };
        (self.to_owned(), result)
    }

    fn evaluate_string_right(
        &self,
        string: &TypedValue,
        n_chars: &TypedValue,
    ) -> (Machine, TypedValue) {
        let result = match string {
            StringValue(s) =>
                match n_chars {
                    Number(nv) if nv.to_i64() < 0 =>
                        self.evaluate_string_left(string, &Number(I64Value(-nv.to_i64()))).1,
                    Number(nv) => {
                        let strlen = s.len();
                        StringValue(s[(strlen - nv.to_usize())..strlen].to_string())
                    }
                    _ => Undefined
                },
            _ => Undefined
        };
        (self.to_owned(), result)
    }

    fn evaluate_structure_impl(
        &self,
        name: &str,
        ops: &Vec<Expression>,
    ) -> (Self, TypedValue) {
        match self.get(name) {
            Some(HardStructureValue(structure)) =>
                self.evaluate_structure_hard_impl(name, structure, ops),
            Some(SoftStructureValue(structure)) =>
                self.evaluate_structure_soft_impl(name, structure, ops),
            Some(v) => (self.clone(), ErrorValue(StructExpected(name.to_string(), v.to_code()))),
            None => (self.clone(), ErrorValue(StructExpected(name.to_string(), Undefined.to_code()))),
        }
    }

    fn evaluate_structure_hard_impl(
        &self,
        name: &str,
        structure: HardStructure,
        ops: &Vec<Expression>,
    ) -> (Self, TypedValue) {
        let result = ops.iter()
            .fold(HardStructureValue(structure), |tv, op| match tv {
                ErrorValue(msg) => ErrorValue(msg),
                HardStructureValue(structure) =>
                    match op {
                        SetVariable(name, expr) =>
                            match self.evaluate(expr) {
                                Ok((_, value)) => HardStructureValue(structure.with_variable(name, value)),
                                Err(err) => ErrorValue(Exact(err.to_string()))
                            },
                        z => ErrorValue(IllegalExpression(z.to_code()))
                    },
                z => ErrorValue(StructExpected(name.to_string(), z.to_code()))
            });
        (self.with_variable(name, result), Outcome(Ack))
    }

    fn evaluate_structure_soft_impl(
        &self,
        name: &str,
        structure: SoftStructure,
        ops: &Vec<Expression>,
    ) -> (Self, TypedValue) {
        let result = ops.iter()
            .fold(SoftStructureValue(structure), |tv, op| match tv {
                ErrorValue(msg) => ErrorValue(msg),
                SoftStructureValue(structure) =>
                    match op {
                        SetVariable(name, expr) =>
                            match self.evaluate(expr) {
                                Ok((_, value)) => SoftStructureValue(structure.with_variable(name, value)),
                                Err(err) => ErrorValue(Exact(err.to_string()))
                            },
                        z => ErrorValue(IllegalExpression(z.to_code()))
                    },
                z => ErrorValue(StructExpected(name.to_string(), z.to_code()))
            });
        (self.with_variable(name, result), Outcome(Ack))
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
                    ErrorValue(Exact(message.to_string()))
                };
            Ok((ms, result))
        } else {
            Ok((ms, ErrorValue(CollectionExpected(args.iter()
                .map(|e| e.to_code())
                .collect::<Vec<_>>()
                .join(", ")))))
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
                Ok((machine, ErrorValue(Exact("Memory collections do not yet support indexes".to_string())))),
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
                Ok((machine, Outcome(Ack)))
            }
            z =>
                Ok((machine, ErrorValue(CollectionExpected(z.to_code()))))
        }
    }

    fn evaluate_table_create_table(
        &self,
        table: &Expression,
        columns: &Vec<Parameter>,
        from: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, result) = self.evaluate(table)?;
        match result.to_owned() {
            Null | Undefined => Ok((machine, result)),
            TableValue(_mrc) =>
                Ok((machine, ErrorValue(Exact("Memory collections do not 'create' keyword".to_string())))),
            NamespaceValue(d, s, n) => {
                let ns = Namespace::new(d, s, n);
                let config = DataFrameConfig::new(columns.to_owned(), Vec::new(), Vec::new());
                DataFrame::create(ns, config)?;
                Ok((machine, Outcome(Ack)))
            }
            x =>
                Ok((machine, ErrorValue(CollectionExpected(x.to_code()))))
        }
    }

    fn evaluate_table_declare_index(
        &self,
        columns: &Vec<Expression>,
    ) -> std::io::Result<(Self, TypedValue)> {
        // TODO determine how to implement
        Ok((self.to_owned(), ErrorValue(NotImplemented)))
    }

    fn evaluate_table_declare_table(
        &self,
        columns: &Vec<Parameter>,
        from: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let columns = Column::from_parameters(columns)?;
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
                Ok((machine, if result.is_ok() { Outcome(Ack) } else { Boolean(false) }))
            }
            _ => Ok((machine, Boolean(false)))
        }
    }

    fn evaluate_table_into(
        &self,
        table: &Expression,
        source: &Expression,
    ) -> std::io::Result<(Machine, TypedValue)> {
        use Outcomes::*;
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
            Quarry(Excavation::Construct(Declare(TableEntity { columns, from }))) =>
                machine.extract_rows_from_table_declaration(table, from, columns)?,
            source =>
                self.extract_rows_from_query(source, table)?,
        };

        // write the rows to the target
        let mut inserted = 0;
        let mut rc = machine.expect_row_collection(table)?;
        match rc.append_rows(rows) {
            ErrorValue(message) => return Ok((machine, ErrorValue(message))),
            Outcome(oc) => inserted += oc.to_update_count(),
            _ => {}
        }
        Ok((machine, Outcome(RowsAffected(inserted))))
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
                    _ => Ok((ms, ErrorValue(InvalidNamespace(path))))
                }
            NamespaceValue(d, s, n) => Ok((ms, NamespaceValue(d, s, n))),
            other => Ok((ms, ErrorValue(TypeMismatch("Table".to_string(), other.to_code())))),
        }
    }

    fn evaluate_table_row_resize(
        &self,
        table: &Expression,
        limit: TypedValue,
    ) -> std::io::Result<(Self, TypedValue)> {
        use Outcomes::*;
        let (machine, table) = self.evaluate(table)?;
        Self::orchestrate_io(machine, table, &Vec::new(), &Vec::new(), &None, limit, |machine, df, _fields, _values, condition, limit| {
            let limit = limit.to_usize();
            let new_size = df.resize(limit)?;
            Ok((machine, Outcome(RowsAffected(new_size))))
        })
    }

    fn evaluate_table_row_append(
        &self,
        table: &Expression,
        source: &Expression,
    ) -> std::io::Result<(Machine, TypedValue)> {
        use Outcomes::*;
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
            Ok((machine, ErrorValue(QueryableExpected(source.to_string()))))
        }
    }

    fn evaluate_table_row_delete(
        &self,
        from: &Expression,
        condition: &Option<Conditions>,
        limit: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        use Outcomes::*;
        let (machine, limit) = self.evaluate_optional(limit)?;
        let (machine, result) = machine.evaluate(from)?;
        Self::orchestrate_io(machine, result, &Vec::new(), &Vec::new(), condition, limit, |machine, df, _fields, _values, condition, limit| {
            let deleted = df.delete_where(&machine, &condition, limit).ok().unwrap_or(0);
            Ok((machine, Outcome(RowsAffected(deleted))))
        })
    }

    fn evaluate_table_row_overwrite(
        &self,
        table: &Expression,
        source: &Expression,
        condition: &Option<Conditions>,
        limit: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        use Outcomes::*;
        let (machine, limit) = self.evaluate_optional(limit)?;
        let (machine, tv_table) = machine.evaluate(table)?;
        let (fields, values) = self.expect_via(&table, &source)?;
        Self::orchestrate_io(machine, tv_table, &fields, &values, condition, limit, |machine, df, fields, values, condition, limit| {
            let overwritten = df.overwrite_where(&machine, fields, values, condition, limit).ok().unwrap_or(0);
            Ok((machine, Outcome(RowsAffected(overwritten))))
        })
    }

    /// Evaluates the queryable [Expression] (e.g. from, limit and where)
    /// e.g.: from ns("interpreter.select.stocks") where last_sale > 1.0 limit 1
    fn evaluate_table_row_query(
        &self,
        src: &Expression,
        condition: &Conditions,
        limit: TypedValue,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, result) = self.evaluate(src)?;
        let limit = limit.to_usize();
        match result {
            TableValue(mrc) => {
                let phys_columns = mrc.get_columns().clone();
                let mut cursor = Cursor::filter(Box::new(mrc), condition.to_owned());
                Ok((machine, TableValue(ModelRowCollection::from_rows(phys_columns, cursor.take(limit)?))))
            }
            NamespaceValue(d, s, n) => {
                let ns = Namespace::new(d, s, n);
                let frc = FileRowCollection::open(&ns)?;
                let phys_columns = frc.get_columns().clone();
                let mut cursor = Cursor::filter(Box::new(frc), condition.to_owned());
                Ok((machine, TableValue(ModelRowCollection::from_rows(phys_columns, cursor.take(limit)?))))
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
            Ok(TableValue(ModelRowCollection::from_rows(df.get_columns().clone(), rows)))
        })?;
        Ok((machine, result))
    }

    fn evaluate_table_row_selection(
        &self,
        fields: &Vec<Expression>,
        from: &Option<Box<Expression>>,
        condition: &Option<Conditions>,
        _group_by: &Option<Vec<Expression>>,
        _having: &Option<Box<Expression>>,
        _order_by: &Option<Vec<Expression>>,
        limit: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, limit) = self.evaluate_optional(limit)?;
        match from {
            None => machine.evaluate_array(fields),
            Some(source) => {
                let (machine, table) = machine.evaluate(source)?;
                Self::orchestrate_io(machine, table, &Vec::new(), &Vec::new(), condition, limit, |machine, df, _, _, condition, limit| {
                    let phys_columns = df.get_columns().clone();
                    let rows = df.read_where(&machine, condition, limit)?;
                    Ok((machine, TableValue(ModelRowCollection::from_rows(phys_columns, rows))))
                })
            }
        }
    }

    fn evaluate_table_row_undelete(
        &self,
        from: &Expression,
        condition: &Option<Conditions>,
        limit: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        use Outcomes::*;
        let (machine, limit) = self.evaluate_optional(limit)?;
        let (machine, result) = machine.evaluate(from)?;
        Self::orchestrate_io(machine, result, &Vec::new(), &Vec::new(), condition, limit, |machine, df, _fields, _values, condition, limit| {
            match df.undelete_where(&machine, &condition, limit) {
                Ok(restored) => Ok((machine, Outcome(RowsAffected(restored)))),
                Err(err) => Ok((machine, ErrorValue(Exact(err.to_string())))),
            }
        })
    }

    fn evaluate_table_row_update(
        &self,
        table: &Expression,
        source: &Expression,
        condition: &Option<Conditions>,
        limit: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        use Outcomes::*;
        let (machine, limit) = self.evaluate_optional(limit)?;
        let (machine, tv_table) = machine.evaluate(table)?;
        let (fields, values) = self.expect_via(&table, &source)?;
        Self::orchestrate_io(machine, tv_table, &fields, &values, condition, limit, |machine, df, fields, values, condition, limit| {
            let modified = df.update_where(&machine, fields, values, condition, limit).ok().unwrap_or(0);
            Ok((machine, Outcome(RowsAffected(modified))))
        })
    }

    fn evaluate_table_scan(&self, expr: &Expression) -> std::io::Result<(Self, TypedValue)> {
        let (machine, tv_table) = self.evaluate(expr)?;
        Self::orchestrate_rc(machine, tv_table, |machine, rc| {
            let rows = rc.examine_rows()?;
            let columns = rows.first()
                .map(|row| rc.get_columns().to_owned())
                .unwrap_or(Vec::new());
            let mrc = ModelRowCollection::from_rows(columns, rows);
            Ok((machine, TableValue(mrc)))
        })
    }

    fn evaluate_timestamp(
        &self,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        if !args.is_empty() {
            return fail(format!("No arguments expected, but found {}", args.len()));
        }
        Ok((self.to_owned(), DateValue(Local::now().timestamp_millis())))
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
        (self.to_owned(), Array(rc.iter().map(|row| row.to_json(rc.get_columns()))
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

    fn evaluate_uuid(
        &self,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Self, TypedValue)> {
        if !args.is_empty() {
            return fail(format!("No arguments expected, but found {}", args.len()));
        }
        Ok((self.to_owned(), UUIDValue(Uuid::new_v4().as_u128())))
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
        columns: &Vec<Column>,
    ) -> std::io::Result<Vec<Row>> {
        let (_, source) = self.evaluate(source)?;
        match source {
            Array(items) => {
                let mut rows = Vec::new();
                for tuples in items {
                    if let SoftStructureValue(schemaless) = tuples {
                        rows.push(Row::from_tuples(0, columns, &schemaless.get_tuples()))
                    }
                }
                Ok(rows)
            }
            HardStructureValue(s) => Ok(vec![Row::from_tuples(0, columns, &s.get_tuples())]),
            SoftStructureValue(s) => Ok(vec![Row::from_tuples(0, columns, &s.get_tuples())]),
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
                if let (_, SoftStructureValue(schemaless)) = self.evaluate(&my_source)? {
                    let columns = writable.get_columns();
                    let row = Row::from_tuples(0, columns, &schemaless.get_tuples());
                    Self::split(columns, &row)
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
        columns: &Vec<Parameter>,
    ) -> std::io::Result<(Machine, Vec<Row>)> {
        let machine = self.to_owned();
        // create the config and an empty data file
        let ns = self.expect_namespace(table)?;
        let cfg = DataFrameConfig::new(columns.to_owned(), Vec::new(), Vec::new());
        cfg.save(&ns)?;
        FileRowCollection::table_file_create(&ns)?;
        // decipher the "from" expression
        let columns = Column::from_parameters(columns)?;
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
        let columns = Column::from_parameters(&vec![
            Parameter::new("name", Some("String(80)".into()), None),
            Parameter::new("kind", Some("String(80)".into()), None),
            Parameter::new("value", Some("String(256)".into()), None),
        ]).unwrap_or(vec![]);
        let mut mrc = ModelRowCollection::new(columns.to_owned());
        let variables = self.variables.iter()
            .filter(|&(_, v)| v.get_type_name() != "BackDoor")
            .collect::<Vec<_>>();
        for (name, value) in variables {
            mrc.append_row(Row::new(0, vec![
                StringValue(name.to_owned()),
                StringValue(value.get_type_name()),
                match value {
                    BackDoor(key) => StringValue(format!("{}(???)", key.to_code())),
                    TableValue(rc) => StringValue(format!("{} row(s)", rc.len().unwrap_or(0))),
                    v => StringValue(v.to_code())
                },
            ]));
        }
        mrc
    }

    pub fn import_module_by_name(&self, name: &str) -> Self {
        let module = Literal(StringValue(name.into()));
        match self.evaluate_import(&module) {
            Ok((m, _)) => m,
            Err(err) => {
                error!("{}", err);
                self.with_variable("__error__", StringValue(err.to_string()))
            }
        }
    }

    pub fn set(&self, name: &str, value: TypedValue) -> Self {
        let mut variables = self.variables.to_owned();
        variables.insert(name.to_string(), value);
        Self::construct(self.stack.to_owned(), variables)
    }

    pub fn show(columns: Vec<Column>, rows: Vec<Row>) {
        for s in TableRenderer::from_rows(columns, rows) {
            println!("{}", s)
        }
    }

    pub fn with_package(
        &self,
        name: &str,
        variables: Vec<(&str, TypedValue)>,
    ) -> Self {
        let structure = variables.iter().fold(
            HardStructure::empty(),
            |structure, (name, value)| {
                structure.with_variable(name, value.to_owned())
            });
        self.with_variable(name, HardStructureValue(structure))
    }

    pub fn with_row(&self, columns: &Vec<Column>, row: &Row) -> Self {
        row.get_values().iter().zip(columns.iter())
            .fold(self.to_owned(), |machine, (v, c)| {
                machine.with_variable(c.get_name(), v.to_owned())
            })
    }

    pub fn with_tuples(&self, tuples: Vec<(&str, TypedValue)>) -> Self {
        tuples.iter().fold(self.to_owned(), |ms, (name, value)| {
            ms.with_variable(name, value.to_owned())
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
    use super::*;
    use crate::compiler::Compiler;
    use crate::data_types::DataType::NumberType;
    use crate::expression::Conditions::{Equal, GreaterOrEqual, GreaterThan, LessOrEqual, LessThan};
    use crate::expression::CreationEntity::{IndexEntity, TableEntity};
    use crate::expression::Excavation::Query;
    use crate::expression::MutateTarget::TableTarget;
    use crate::expression::{Excavation, Infrastructure, Mutation, Queryable};
    use crate::expression::{FALSE, NULL, TRUE};
    use crate::number_kind::NumberKind::I64Kind;
    use crate::outcomes::Outcomes;
    use crate::table_columns::Column;
    use crate::table_renderer::TableRenderer;
    use crate::testdata::{make_dataframe_ns, make_quote, make_quote_columns, make_quote_parameters};
    use Outcomes::*;

    #[test]
    fn test_new_platform() {
        let m = Machine::new();
        let value = m.get("lang");
        println!("value {:?}", value)
    }

    #[test]
    fn test_array_declaration() {
        let models = vec![Literal(Number(F64Value(3.25))), TRUE, FALSE, NULL, UNDEFINED];
        assert_eq!(models.iter().map(|e| e.to_code()).collect::<Vec<String>>(), vec![
            "3.25", "true", "false", "null", "undefined",
        ]);

        let (_, array) = Machine::new().evaluate_array(&models).unwrap();
        assert_eq!(array, Array(vec![Number(F64Value(3.25)), Boolean(true), Boolean(false), Null, Undefined]));
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
        let model = Parameters(make_quote_parameters());
        assert_eq!(model.to_code(), "symbol: String(8), exchange: String(8), last_sale: f64");

        let machine = Machine::new()
            .with_variable("symbol", StringValue("ABC".into()))
            .with_variable("exchange", StringValue("NYSE".into()))
            .with_variable("last_sale", Number(F64Value(12.66)));
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, Array(vec![
            StringValue("ABC".into()),
            StringValue("NYSE".into()),
            Number(F64Value(12.66)),
        ]));
    }

    #[test]
    fn test_divide() {
        let machine = Machine::new().with_variable("x", Number(I64Value(50)));
        let model = Divide(Box::new(Variable("x".into())), Box::new(Literal(Number(I64Value(7)))));
        assert_eq!(model.to_code(), "x / 7");

        let (machine, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, Number(I64Value(7)));
        assert_eq!(machine.get("x"), Some(Number(I64Value(50))));
        assert_eq!(model.infer_type(), NumberType(I64Kind))
    }

    #[test]
    fn test_factorial() {
        let model = Factorial(Box::new(Literal(Number(U64Value(6)))));
        assert_eq!(model.to_code(), "¡6");

        let machine = Machine::new();
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, Number(U128Value(720)))
    }

    #[test]
    fn test_function_recursion() {
        // f := (fn(n: i64) => if(n <= 1) 1 else n * f(n - 1))
        let model = Function {
            params: vec![
                Parameter::new("n", Some("i64".into()), None)
            ],
            // iff(n <= 1, 1, n * f(n - 1))
            code: Box::new(If {
                // n <= 1
                condition: Box::new(Condition(LessOrEqual(
                    Box::new(Variable("n".into())),
                    Box::new(Literal(Number(I64Value(1)))),
                ))),
                // 1
                a: Box::new(Literal(Number(I64Value(1)))),
                // n * f(n - 1)
                b: Some(Box::from(Multiply(
                    Box::from(Variable("n".into())),
                    Box::from(FunctionCall {
                        fx: Box::new(Variable("f".into())),
                        args: vec![
                            Minus(
                                Box::from(Variable("n".into())),
                                Box::from(Literal(Number(I64Value(1)))),
                            ),
                        ],
                    }),
                ))),
            }),
        };
        println!("f := {}", model.to_code());

        // f(5.0)
        let machine = Machine::new().with_variable("f", model);
        let (machine, result) = machine.evaluate(&FunctionCall {
            fx: Box::new(Variable("f".into())),
            args: vec![
                Literal(Number(I64Value(5)))
            ],
        }).unwrap();

        println!("result: {}", result);
        assert_eq!(result, Number(I64Value(120)));
    }

    #[test]
    fn test_if_else_flow_1() {
        let model = If {
            condition: Box::new(Condition(GreaterThan(
                Box::new(Variable("num".into())),
                Box::new(Literal(Number(I64Value(25)))),
            ))),
            a: Box::new(Literal(StringValue("Yes".into()))),
            b: Some(Box::new(Literal(StringValue("No".into())))),
        };
        assert_eq!(model.to_code(), r#"if num > 25 "Yes" else "No""#);

        let machine = Machine::new().with_variable("num", Number(I64Value(5)));
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, StringValue("No".into()));

        let machine = Machine::new().with_variable("num", Number(I64Value(37)));
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, StringValue("Yes".into()));
    }

    #[test]
    fn test_if_else_flow_2() {
        let model = If {
            condition: Box::new(Condition(LessThan(
                Box::new(Variable("num".into())),
                Box::new(Literal(Number(I64Value(10)))),
            ))),
            a: Box::new(Literal(StringValue("Yes".into()))),
            b: Some(Box::new(Literal(StringValue("No".into())))),
        };
        assert_eq!(model.to_code(), r#"if num < 10 "Yes" else "No""#);

        let machine = Machine::new().with_variable("num", Number(I64Value(5)));
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, StringValue("Yes".into()));

        let machine = Machine::new().with_variable("num", Number(I64Value(99)));
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, StringValue("No".into()));
    }

    #[test]
    fn test_index_of_array() {
        // create the instruction model for "[1, 4, 2, 8, 5, 7][5]"
        let model = ElementAt(
            Box::new(ArrayExpression(vec![
                Literal(Number(I64Value(1))), Literal(Number(I64Value(4))),
                Literal(Number(I64Value(2))), Literal(Number(I64Value(8))),
                Literal(Number(I64Value(5))), Literal(Number(I64Value(7))),
            ])),
            Box::new(Literal(Number(I64Value(5)))),
        );
        assert_eq!(model.to_code(), "[1, 4, 2, 8, 5, 7][5]");

        let machine = Machine::new();
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, Number(I64Value(7)))
    }

    #[test]
    fn test_index_of_table_in_namespace() {
        // create a table with test data
        let ns = Namespace::new("machine", "element_at", "stocks");
        let logical_columns = make_quote_parameters();
        let phys_columns = Column::from_parameters(&logical_columns).unwrap();
        let value_tuples = vec![
            ("ABC", "AMEX", 11.77), ("UNO", "OTC", 0.2456),
            ("BIZ", "NYSE", 23.66), ("GOTO", "OTC", 0.1428),
            ("BOOM", "NASDAQ", 56.87),
        ];
        let rows = value_tuples.iter()
            .map(|(symbol, exchange, last_sale)| {
                Row::new(0, vec![
                    StringValue(symbol.to_string()),
                    StringValue(exchange.to_string()),
                    Number(F64Value(*last_sale)),
                ])
            }).collect();
        let mut dfrc = DataFrame::create(ns.to_owned(), DataFrameConfig::build(logical_columns)).unwrap();
        assert_eq!(Outcome(RowsAffected(5)), dfrc.append_rows(rows));

        // create the instruction model 'ns("machine.element_at.stocks")[2]'
        let model = ElementAt(
            Box::new(Ns(Box::new(Literal(StringValue(ns.into()))))),
            Box::new(Literal(Number(I64Value(2)))),
        );
        assert_eq!(model.to_code(), r#"ns("machine.element_at.stocks")[2]"#);

        // evaluate the instruction
        let (_, result) = Machine::new().evaluate(&model).unwrap();
        assert_eq!(result, HardStructureValue(
            HardStructure::from_row(
                phys_columns.clone(),
                &make_quote(2, "BIZ", "NYSE", 23.66))
        ))
    }

    #[test]
    fn test_index_of_table_in_variable() {
        let phys_columns = make_quote_columns();
        let my_table = ModelRowCollection::from_rows(phys_columns.clone(), vec![
            make_quote(0, "ABC", "AMEX", 12.33),
            make_quote(1, "GAS", "NYSE", 0.2456),
            make_quote(2, "BASH", "NASDAQ", 13.11),
            make_quote(3, "OIL", "NYSE", 0.1442),
            make_quote(4, "VAPOR", "NYSE", 0.0289),
        ]);

        // create the instruction model "stocks[4]"
        let model = ElementAt(
            Box::new(Variable("stocks".to_string())),
            Box::new(Literal(Number(I64Value(4)))),
        );
        assert_eq!(model.to_code(), "stocks[4]");

        // evaluate the instruction
        let machine = Machine::new()
            .with_variable("stocks", TableValue(my_table));
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, HardStructureValue(
            HardStructure::from_row(phys_columns.clone(),
                                    &make_quote(4, "VAPOR", "NYSE", 0.0289))
        ))
    }

    #[test]
    fn test_create_table() {
        let model = Quarry(Excavation::Construct(Infrastructure::Create {
            path: Box::new(Ns(Box::new(Literal(StringValue("machine.create.stocks".into()))))),
            entity: TableEntity {
                columns: make_quote_parameters(),
                from: None,
            },
        }));

        // create the table
        let machine = Machine::new();
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, Outcome(Ack));

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
        let (machine, _) = machine.evaluate(&Quarry(Excavation::Construct(Infrastructure::Drop(TableTarget {
            path: Box::new(Ns(Box::new(Literal(StringValue(path.into()))))),
        })))).unwrap();

        // create table ns("machine.index.stocks") (...)
        let (machine, result) = machine.evaluate(&Quarry(Excavation::Construct(Infrastructure::Create {
            path: Box::new(Ns(Box::new(Literal(StringValue(path.into()))))),
            entity: TableEntity {
                columns: make_quote_parameters(),
                from: None,
            },
        }))).unwrap();
        assert_eq!(result, Outcome(Ack));

        // create index ns("machine.index.stocks") [symbol, exchange]
        let model = Quarry(Excavation::Construct(Infrastructure::Create {
            path: Box::new(Ns(Box::new(Literal(StringValue(path.into()))))),
            entity: IndexEntity {
                columns: vec![
                    Variable("symbol".into()),
                    Variable("exchange".into()),
                ],
            },
        }));
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, Outcome(Ack));

        // decompile back to source code
        assert_eq!(
            model.to_code(),
            "create index ns(\"machine.index.stocks\") [symbol, exchange]"
        );
    }

    #[test]
    fn test_declare_table() {
        let model = Quarry(Excavation::Construct(Infrastructure::Declare(TableEntity {
            columns: vec![
                Parameter::new("symbol", Some("String(8)".into()), None),
                Parameter::new("exchange", Some("String(8)".into()), None),
                Parameter::new("last_sale", Some("f64".into()), None),
            ],
            from: None,
        })));

        let machine = Machine::new();
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, TypedValue::TableValue(ModelRowCollection::with_rows(
            make_quote_columns(), Vec::new(),
        )))
    }

    #[test]
    fn test_drop_table() {
        // create a table with test data
        let ns_path = "machine.drop.stocks";
        let ns = Ns(Box::new(Literal(StringValue(ns_path.into()))));
        create_dataframe(ns_path);

        let model = Quarry(Excavation::Construct(Infrastructure::Drop(TableTarget { path: Box::new(ns) })));
        assert_eq!(model.to_code(), "drop table ns(\"machine.drop.stocks\")");

        let machine = Machine::new();
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, Outcome(Ack))
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

        let machine = Machine::new_platform_with_default_imports();
        let (_, result) = machine.evaluate(&model).unwrap();
        let table = result.to_table().unwrap();
        let columns = table.get_columns();
        let rows = table.read_active_rows().unwrap();
        for s in TableRenderer::from_rows(columns.clone(), rows.to_owned()) {
            println!("{}", s)
        }
        let dump = rows.iter().map(|row| row.get_values()).collect::<Vec<_>>();
        assert_eq!(dump, vec![
            vec![
                Number(U16Value(0)),
                StringValue("Karate translator".into()),
                Boolean(true),
                Outcome(Ack),
            ],
            vec![
                Number(U16Value(1)),
                StringValue("Translate Karate Scenario to Oxide Scenario".into()),
                Boolean(true),
                Outcome(Ack),
            ],
            vec![
                Number(U16Value(2)),
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
                        Parameter::new("n", Some("i64".into()), None)
                    ],
                    code: Box::new(Plus(
                        Box::new(Variable("n".into())),
                        Box::new(Literal(Number(I64Value(5)))))),
                })),
            args: vec![
                Literal(Number(I64Value(3)))
            ],
        };

        // evaluate the function
        let (machine, result) = Machine::new()
            .with_variable("n", Number(I64Value(3)))
            .evaluate(&model)
            .unwrap();
        assert_eq!(result, Number(I64Value(8)));
        assert_eq!(model.to_code(), "(fn(n: i64) => n + 5)(3)")
    }

    #[test]
    fn test_named_function() {
        // define a function: (a, b) => a + b
        let fx = Function {
            params: vec![
                Parameter::new("a", Some("i64".into()), None),
                Parameter::new("b", Some("i64".into()), None),
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
        assert_eq!(result, Outcome(Ack));

        // execute the function via function call in scope: add(2, 3)
        let model = FunctionCall {
            fx: Box::new(Literal(fx)),
            args: vec![
                Literal(Number(I64Value(2))),
                Literal(Number(I64Value(3))),
            ],
        };
        let (machine, result) = machine
            .evaluate(&model)
            .unwrap();
        assert_eq!(result, Number(I64Value(5)));
        assert_eq!(model.to_code(), "(fn(a: i64, b: i64) => a + b)(2, 3)")
    }

    #[test]
    fn test_from_where_limit_in_memory() {
        // create a table with test data
        let columns = make_quote_parameters();
        let phys_columns = Column::from_parameters(&columns).unwrap();
        let machine = Machine::new()
            .with_variable("stocks", TableValue(ModelRowCollection::from_rows(phys_columns.clone(), vec![
                make_quote(0, "ABC", "AMEX", 12.33),
                make_quote(1, "UNO", "OTC", 0.2456),
                make_quote(2, "BIZ", "NYSE", 9.775),
                make_quote(3, "GOTO", "OTC", 0.1442),
                make_quote(4, "XYZ", "NYSE", 0.0289),
            ])));

        let model = Quarry(Query(Queryable::Limit {
            from: Box::new(Quarry(Query(Queryable::Where {
                from: Box::new(From(Box::new(Variable("stocks".into())))),
                condition: GreaterOrEqual(
                    Box::new(Variable("last_sale".into())),
                    Box::new(Literal(Number(F64Value(1.0)))),
                ),
            }))),
            limit: Box::new(Literal(Number(I64Value(2)))),
        }));
        assert_eq!(model.to_code(), "from stocks where last_sale >= 1 limit 2");

        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, TableValue(ModelRowCollection::from_rows(phys_columns.clone(), vec![
            make_quote(0, "ABC", "AMEX", 12.33),
            make_quote(2, "BIZ", "NYSE", 9.775),
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
        assert_eq!(result, Outcome(RowsAffected(3)));

        // verify the remaining rows
        let rows = df.read_active_rows().unwrap();
        assert_eq!(rows, vec![
            make_quote(1, "UNO", "OTC", 0.2456),
            make_quote(3, "GOTO", "OTC", 0.1428),
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
        assert_eq!(result, Outcome(RowsAffected(3)));

        // verify the remaining rows
        let rows = df.read_active_rows().unwrap();
        assert_eq!(rows, vec![
            make_quote(1, "UNO", "OTC", 0.2456),
            make_quote(3, "GOTO", "OTC", 0.1428),
        ]);

        // undelete the rows
        let code = Compiler::compile_script(r#"
            undelete from stocks where last_sale > 1.0
            "#).unwrap();
        let (_, result) = machine.evaluate(&code).unwrap();
        assert_eq!(result, Outcome(RowsAffected(3)));

        // verify the remaining rows
        let rows = df.read_active_rows().unwrap();
        assert_eq!(rows, vec![
            make_quote(0, "ABC", "AMEX", 11.77),
            make_quote(1, "UNO", "OTC", 0.2456),
            make_quote(2, "BIZ", "NYSE", 23.66),
            make_quote(3, "GOTO", "OTC", 0.1428),
            make_quote(4, "BOOM", "NASDAQ", 56.87),
        ]);
    }

    #[test]
    fn test_append_namespace() {
        // create a table with test data
        let ns_path = "machine.append.stocks";
        let (df, phys_columns) = create_dataframe(ns_path);

        // insert some rows
        let machine = Machine::new();
        let (_, result) = machine.evaluate(&Quarry(Excavation::Mutate(Mutation::Append {
            path: Box::new(Ns(Box::new(Literal(StringValue(ns_path.to_string()))))),
            source: Box::new(From(Box::new(JSONExpression(vec![
                ("symbol".into(), Literal(StringValue("REX".into()))),
                ("exchange".into(), Literal(StringValue("NASDAQ".into()))),
                ("last_sale".into(), Literal(Number(F64Value(16.99)))),
            ])))),
        }))).unwrap();
        assert_eq!(result, Outcome(RowsAffected(1)));

        // verify the remaining rows
        let rows = df.read_active_rows().unwrap();
        assert_eq!(rows, vec![
            make_quote(0, "ABC", "AMEX", 11.77),
            make_quote(1, "UNO", "OTC", 0.2456),
            make_quote(2, "BIZ", "NYSE", 23.66),
            make_quote(3, "GOTO", "OTC", 0.1428),
            make_quote(4, "BOOM", "NASDAQ", 56.87),
            make_quote(5, "REX", "NASDAQ", 16.99),
        ]);
    }

    #[test]
    fn test_overwrite_rows_in_namespace() {
        let ns_path = "machine.overwrite.stocks";
        let model = Quarry(Excavation::Mutate(Mutation::Overwrite {
            path: Box::new(Ns(Box::new(Literal(StringValue(ns_path.to_string()))))),
            source: Box::new(Via(Box::new(JSONExpression(vec![
                ("symbol".into(), Literal(StringValue("BOOM".into()))),
                ("exchange".into(), Literal(StringValue("NYSE".into()))),
                ("last_sale".into(), Literal(Number(F64Value(56.99)))),
            ])))),
            condition: Some(Equal(
                Box::new(Variable("symbol".into())),
                Box::new(Literal(StringValue("BOOM".into()))),
            )),
            limit: None,
        }));
        assert_eq!(model.to_code(), r#"overwrite ns("machine.overwrite.stocks") via {symbol: "BOOM", exchange: "NYSE", last_sale: 56.99} where symbol == "BOOM""#);

        // create a table with test data
        let (df, phys_columns) = create_dataframe(ns_path);

        // overwrite some rows
        let machine = Machine::new();
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, Outcome(RowsAffected(1)));

        // verify the remaining rows
        let rows = df.read_active_rows().unwrap();
        assert_eq!(rows, vec![
            make_quote(0, "ABC", "AMEX", 11.77),
            make_quote(1, "UNO", "OTC", 0.2456),
            make_quote(2, "BIZ", "NYSE", 23.66),
            make_quote(3, "GOTO", "OTC", 0.1428),
            make_quote(4, "BOOM", "NYSE", 56.99),
        ])
    }

    #[ignore]
    #[test]
    fn test_precedence() {
        // 2 + 4 * 3
        let opcodes = vec![
            Plus(Box::new(Literal(Number(I64Value(2)))),
                 Box::new(Multiply(Box::new(Literal(Number(I64Value(4)))),
                                   Box::new(Literal(Number(I64Value(3)))))))
        ];

        let (_ms, result) = Machine::new().evaluate_scope(&opcodes).unwrap();
        assert_eq!(result, Number(F64Value(14.)))
    }

    #[test]
    fn test_reverse_from_variable() {
        let model = Quarry(Excavation::Query(Queryable::Reverse(Box::new(From(Box::new(Variable("stocks".to_string())))))));
        let phys_columns = make_quote_columns();
        let machine = Machine::new()
            .with_variable("stocks", TableValue(ModelRowCollection::from_rows(phys_columns.clone(), vec![
                make_quote(0, "ABC", "AMEX", 12.33),
                make_quote(1, "GAS.Q", "OTC", 0.2456),
                make_quote(2, "BASH", "NYSE", 13.11),
                make_quote(3, "GOTO", "OTC", 0.1442),
                make_quote(4, "VAPOR", "NYSE", 0.0289),
            ])));
        let (_, value) = machine.evaluate(&model).unwrap();
        assert_eq!(value, TableValue(ModelRowCollection::from_rows(phys_columns.clone(), vec![
            make_quote(4, "VAPOR", "NYSE", 0.0289),
            make_quote(3, "GOTO", "OTC", 0.1442),
            make_quote(2, "BASH", "NYSE", 13.11),
            make_quote(1, "GAS.Q", "OTC", 0.2456),
            make_quote(0, "ABC", "AMEX", 12.33),
        ])))
    }

    #[test]
    fn test_select_from_namespace() {
        // create a table with test data
        let (_, phys_columns) =
            create_dataframe("machine.select.stocks");

        // execute the query
        let machine = Machine::new();
        let (_, result) = machine.evaluate(&Quarry(Excavation::Query(Queryable::Select {
            fields: vec![
                Variable("symbol".into()),
                Variable("exchange".into()),
                Variable("last_sale".into()),
            ],
            from: Some(Box::new(Ns(Box::new(Literal(StringValue("machine.select.stocks".into())))))),
            condition: Some(GreaterThan(
                Box::new(Variable("last_sale".into())),
                Box::new(Literal(Number(F64Value(1.0)))),
            )),
            group_by: None,
            having: None,
            order_by: Some(vec![Variable("symbol".into())]),
            limit: Some(Box::new(Literal(Number(I64Value(5))))),
        }))).unwrap();
        assert_eq!(result, TableValue(ModelRowCollection::from_rows(phys_columns.clone(), vec![
            make_quote(0, "ABC", "AMEX", 11.77),
            make_quote(2, "BIZ", "NYSE", 23.66),
            make_quote(4, "BOOM", "NASDAQ", 56.87),
        ])));
    }

    #[test]
    fn test_select_from_variable() {
        let (mrc, phys_columns) = create_memory_table();
        let machine = Machine::new()
            .with_variable("stocks", TableValue(mrc));

        // execute the code
        let (_, result) = machine.evaluate(&Quarry(Excavation::Query(Queryable::Select {
            fields: vec![
                Variable("symbol".into()),
                Variable("exchange".into()),
                Variable("last_sale".into()),
            ],
            from: Some(Box::new(Variable("stocks".into()))),
            condition: Some(LessThan(
                Box::new(Variable("last_sale".into())),
                Box::new(Literal(Number(F64Value(1.0)))),
            )),
            group_by: None,
            having: None,
            order_by: Some(vec![Variable("symbol".into())]),
            limit: Some(Box::new(Literal(Number(I64Value(5))))),
        }))).unwrap();
        assert_eq!(result, TableValue(ModelRowCollection::from_rows(phys_columns.clone(), vec![
            make_quote(1, "UNO", "OTC", 0.2456),
            make_quote(3, "GOTO", "OTC", 0.1428),
        ])));
    }

    #[test]
    fn test_type_of() {
        let model = FunctionCall {
            fx: Box::new(Variable("type_of".to_string())),
            args: vec![
                Literal(StringValue("cat".to_string()))
            ],
        };
        let (_, result) = Machine::new_platform_with_default_imports()
            .evaluate(&model).unwrap();
        assert_eq!(result, StringValue("String(3)".to_string()));
    }

    #[ignore]
    #[test]
    fn test_update_rows_in_memory() {
        // build the update model
        let model = Quarry(Excavation::Mutate(Mutation::Update {
            path: Box::new(Variable("stocks".into())),
            source: Box::new(Via(Box::new(JSONExpression(vec![
                ("exchange".into(), Literal(StringValue("OTC_BB".into()))),
            ])))),
            condition: Some(Equal(
                Box::new(Variable("exchange".into())),
                Box::new(Literal(StringValue("OTC".into()))),
            )),
            limit: Some(Box::new(Literal(Number(I64Value(5))))),
        }));
        assert_eq!(model.to_code(), r#"update stocks via {exchange: "OTC_BB"} where exchange == "OTC" limit 5"#);

        // perform the update and verify
        let (mrc, phys_columns) = create_memory_table();
        let machine = Machine::new().with_variable("stocks", TableValue(mrc));
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, Outcome(RowsAffected(2)));

        // retrieve and verify all rows
        let model = From(Box::new(Variable("stocks".into())));
        let (machine, _) = machine.evaluate(&model).unwrap();
        assert_eq!(machine.get("stocks").unwrap(), TableValue(ModelRowCollection::from_rows(phys_columns.clone(), vec![
            make_quote(0, "ABC", "AMEX", 11.77),
            make_quote(1, "UNO", "OTC_BB", 0.2456),
            make_quote(2, "BIZ", "NYSE", 23.66),
            make_quote(3, "GOTO", "OTC_BB", 0.1428),
            make_quote(4, "BOOM", "NASDAQ", 56.87),
        ])))
    }

    #[test]
    fn test_update_rows_in_namespace() {
        // create a table with test data
        let ns_path = "machine.update.stocks";
        let (df, phys_columns) = create_dataframe(ns_path);

        // create the instruction model
        let model = Quarry(Excavation::Mutate(Mutation::Update {
            path: Box::new(Ns(Box::new(Literal(StringValue(ns_path.to_string()))))),
            source: Box::new(Via(Box::new(JSONExpression(vec![
                ("exchange".into(), Literal(StringValue("OTC_BB".into()))),
            ])))),
            condition: Some(Equal(
                Box::new(Variable("exchange".into())),
                Box::new(Literal(StringValue("OTC".into()))),
            )),
            limit: Some(Box::new(Literal(Number(I64Value(5))))),
        }));

        // update some rows
        let (_, delta) = Machine::new().evaluate(&model).unwrap();
        assert_eq!(delta, Outcome(RowsAffected(2)));

        // verify the rows
        let rows = df.read_active_rows().unwrap();
        assert_eq!(rows, vec![
            make_quote(0, "ABC", "AMEX", 11.77),
            make_quote(1, "UNO", "OTC_BB", 0.2456),
            make_quote(2, "BIZ", "NYSE", 23.66),
            make_quote(3, "GOTO", "OTC_BB", 0.1428),
            make_quote(4, "BOOM", "NASDAQ", 56.87),
        ])
    }

    #[test]
    fn test_truncate_table() {
        let model = Quarry(Excavation::Mutate(Mutation::Truncate {
            path: Box::new(Variable("stocks".into())),
            limit: Some(Box::new(Literal(Number(I64Value(0))))),
        }));

        let (mrc, _) = create_memory_table();
        let machine = Machine::new().with_variable("stocks", TableValue(mrc));
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, Outcome(RowsAffected(1)))
    }

    #[test]
    fn test_variables() {
        let machine = Machine::new()
            .with_variable("abc", Number(I32Value(5)))
            .with_variable("xyz", Number(I32Value(58)));
        assert_eq!(machine.get("abc"), Some(Number(I32Value(5))));
        assert_eq!(machine.get("xyz"), Some(Number(I32Value(58))));
    }

    #[test]
    fn test_while_loop() {
        let model = While {
            // num < 5
            condition: Box::new(Condition(LessThan(
                Box::new(Variable("num".into())),
                Box::new(Literal(Number(I64Value(5)))),
            ))),
            // num := num + 1
            code: Box::new(SetVariable("num".into(), Box::new(Plus(
                Box::new(Variable("num".into())),
                Box::new(Literal(Number(I64Value(1)))),
            )))),
        };
        assert_eq!(model.to_code(), "while num < 5 num := num + 1");

        let machine = Machine::new().with_variable("num", Number(I64Value(0)));
        let (machine, _) = machine.evaluate(&model).unwrap();
        assert_eq!(machine.get("num"), Some(Number(I64Value(5))))
    }

    fn create_memory_table() -> (ModelRowCollection, Vec<Column>) {
        let phys_columns = make_quote_columns();
        let mrc = ModelRowCollection::from_rows(phys_columns.clone(), vec![
            make_quote(0, "ABC", "AMEX", 11.77),
            make_quote(1, "UNO", "OTC", 0.2456),
            make_quote(2, "BIZ", "NYSE", 23.66),
            make_quote(3, "GOTO", "OTC", 0.1428),
            make_quote(4, "BOOM", "NASDAQ", 56.87)]);
        (mrc, phys_columns)
    }

    fn create_dataframe(namespace: &str) -> (DataFrame, Vec<Column>) {
        let columns = make_quote_parameters();
        let phys_columns = Column::from_parameters(&columns).unwrap();
        let ns = Namespace::parse(namespace).unwrap();
        match fs::remove_file(ns.get_table_file_path()) {
            Ok(_) => {}
            Err(_) => {}
        }

        let mut df = make_dataframe_ns(ns, columns.to_owned()).unwrap();
        assert_eq!(0, df.append(make_quote(0, "ABC", "AMEX", 11.77)).unwrap());
        assert_eq!(1, df.append(make_quote(1, "UNO", "OTC", 0.2456)).unwrap());
        assert_eq!(2, df.append(make_quote(2, "BIZ", "NYSE", 23.66)).unwrap());
        assert_eq!(3, df.append(make_quote(3, "GOTO", "OTC", 0.1428)).unwrap());
        assert_eq!(4, df.append(make_quote(4, "BOOM", "NASDAQ", 56.87)).unwrap());
        (df, phys_columns)
    }
}