////////////////////////////////////////////////////////////////////
//  Machine - state machine module
////////////////////////////////////////////////////////////////////

use actix_web::web::scope;
use chrono::{Datelike, Local, TimeZone, Timelike};
use crossterm::style::Stylize;
use log::{error, info};
use regex::Error;
use reqwest::multipart::{Form, Part};
use reqwest::{Client, RequestBuilder, Response};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::{From, Into};
use std::fs::File;
use std::io::Read;
use std::ops::{Deref, Neg};
use std::path::Path;
use std::process::Output;
use std::{env, fs, thread};
use tokio::runtime::Runtime;
use uuid::Uuid;

use crate::arrays::Array;
use crate::compiler::Compiler;
use crate::compiler::{fail_expr, fail_unexpected, fail_value};
use crate::cursor::Cursor;
use crate::data_types::DataType;
use crate::data_types::DataType::{ArrayType, StringType, StructureType, TableType, UnionType};
use crate::data_types::StorageTypes::{BLOBSized, FixedSize};
use crate::dataframe::Dataframe;
use crate::dataframe::Dataframe::{Disk, Model};
use crate::dataframe_config::{DataFrameConfig, HashIndexConfig};
use crate::errors::Errors::*;
use crate::errors::{throw, Errors};
use crate::expression::Conditions::{False, True};
use crate::expression::CreationEntity::{IndexEntity, TableEntity};
use crate::expression::Expression::*;
use crate::expression::MutateTarget::{IndexTarget, TableTarget};
use crate::expression::{BitwiseOps, Conditions, Expression, ImportOps, ACK, UNDEFINED};
use crate::expression::{DatabaseOps, Directives, Mutation, Queryable};
use crate::file_row_collection::FileRowCollection;
use crate::inferences::Inferences;
use crate::model_row_collection::ModelRowCollection;
use crate::namespaces::Namespace;
use crate::numbers::Numbers::*;
use crate::parameter::Parameter;
use crate::platform::PlatformOps;
use crate::query_engine;
use crate::query_engine::*;
use crate::row_collection::RowCollection;
use crate::rows::Row;
use crate::structures::*;
use crate::table_columns::Column;
use crate::table_renderer::TableRenderer;
use crate::testdata::verify_exact_table_where;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::*;
use shared_lib::fail;

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
    fn build(
        stack: Vec<TypedValue>,
        variables: HashMap<String, TypedValue>,
    ) -> Self {
        Self { stack, variables }
    }

    /// creates a new completely empty state machine
    pub fn empty() -> Self {
        Self::build(Vec::new(), HashMap::new())
    }

    /// creates a new state machine prepopulated with platform packages
    pub fn new() -> Self {
        PlatformOps::build_packages().iter()
            .fold(Self::empty(), |ms, (name, list)| {
                ms.with_module(name, list.to_owned())
            })
    }

    /// creates a new state machine prepopulated with platform packages
    /// and default imports
    pub fn new_platform() -> Self {
        Self::new()
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

    pub fn oxide_home() -> String {
        env::var("OXIDE_HOME").unwrap_or("./oxide_db".to_string())
    }

    ////////////////////////////////////////////////////////////////
    //  instance methods
    ////////////////////////////////////////////////////////////////

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
            BitwiseOp(bitwise) => self.do_bitwise_ops(bitwise),
            CodeBlock(ops) => Ok(self.evaluate_scope(ops)),
            Condition(condition) => self.evaluate_cond(condition),
            Directive(d) => self.do_directive(d),
            Divide(a, b) =>
                self.do_inline_2(a, b, |aa, bb| aa / bb),
            ElementAt(a, b) => self.do_index_of_collection(a, b),
            Extraction(a, b) => self.do_extraction(a, b),
            ExtractPostfix(a, b) => self.do_function_call_postfix(a, b),
            Factorial(a) => self.do_inline_1(a, |aa| aa.factorial()),
            Feature { title, scenarios } => self.do_feature(title, scenarios),
            ForEach(a, b, c) => Ok(self.do_foreach(a, b, c)),
            From(src) => do_table_row_query(self, src, &True, Undefined),
            FunctionCall { fx, args } =>
                Ok(self.do_function_call(fx, args)),
            HTTP { method, url, body, headers, multipart } => {
                self.do_http(method, url, body, headers, multipart)
            }
            If { condition, a, b } =>
                self.do_if_then_else(condition, a, b),
            Import(ops) => Ok(self.do_imports(ops)),
            Include(path) => self.do_include(path),
            JSONExpression(items) => self.do_structure_soft(items),
            Literal(value) => Ok((self.to_owned(), value.to_owned())),
            Minus(a, b) =>
                self.do_inline_2(a, b, |aa, bb| aa - bb),
            Module(name, ops) => Ok(self.do_structure_module(name, ops)),
            Modulo(a, b) =>
                self.do_inline_2(a, b, |aa, bb| aa % bb),
            Multiply(a, b) =>
                self.do_inline_2(a, b, |aa, bb| aa * bb),
            Neg(a) => Ok(self.do_negate(a)),
            Ns(a) => do_table_ns(self, a),
            Parameters(params) => Ok(self.evaluate_parameters(params)),
            Plus(a, b) =>
                self.do_inline_2(a, b, |aa, bb| aa + bb),
            PlusPlus(a, b) =>
                self.do_inline_2(a, b, Self::do_plus_plus),
            Pow(a, b) =>
                self.do_inline_2(a, b, |aa, bb| aa.pow(&bb).unwrap_or(Undefined)),
            DatabaseOp(payload) => evaluate(self, payload),
            Range(a, b) =>
                self.do_inline_2(a, b, |aa, bb| aa.range(&bb).unwrap_or(Undefined)),
            Return(a) => {
                let (machine, result) = self.evaluate_array(a)?;
                Ok((machine, result))
            }
            Scenario { .. } => Ok((self.to_owned(), ErrorValue(Exact("Scenario should not be called directly".to_string())))),
            SetVariable(name, expr) => {
                let (machine, value) = self.evaluate(expr)?;
                Ok((machine.set(name, value), Number(Ack)))
            }
            Variable(name) => Ok((self.to_owned(), self.get_or_else(&name, || Undefined))),
            Via(src) => do_table_row_query(self, src, &True, Undefined),
            While { condition, code } =>
                self.do_while(condition, code),
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
                      Err(err) => {
                          error!("{}", err.to_string());
                          (machine, array)
                      }
                  });
        Ok((machine, ArrayValue(Array::from(results))))
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

    /// evaluates the specified [Expression]; returning a [TypedValue] result.
    pub fn evaluate_cond(
        &self,
        condition: &Conditions,
    ) -> std::io::Result<(Self, TypedValue)> {
        use crate::expression::Conditions::*;
        match condition {
            And(a, b) =>
                self.do_inline_2(a, b, |aa, bb| aa.and(&bb).unwrap_or(Undefined)),
            Between(a, b, c) =>
                self.do_inline_3(a, b, c, |aa, bb, cc| Boolean((aa >= bb) && (aa <= cc))),
            Betwixt(a, b, c) =>
                self.do_inline_3(a, b, c, |aa, bb, cc| Boolean((aa >= bb) && (aa < cc))),
            Contains(a, b) => self.do_contains(a, b),
            Equal(a, b) =>
                self.do_inline_2(a, b, |aa, bb| Boolean(aa == bb)),
            False => Ok((self.to_owned(), Boolean(false))),
            GreaterThan(a, b) =>
                self.do_inline_2(a, b, |aa, bb| Boolean(aa > bb)),
            GreaterOrEqual(a, b) =>
                self.do_inline_2(a, b, |aa, bb| Boolean(aa >= bb)),
            LessThan(a, b) =>
                self.do_inline_2(a, b, |aa, bb| Boolean(aa < bb)),
            LessOrEqual(a, b) =>
                self.do_inline_2(a, b, |aa, bb| Boolean(aa <= bb)),
            Like(text, pattern) =>
                self.do_like(text, pattern),
            Not(a) => self.do_inline_1(a, |aa| !aa),
            NotEqual(a, b) =>
                self.do_inline_2(a, b, |aa, bb| Boolean(aa != bb)),
            Or(a, b) =>
                self.do_inline_2(a, b, |aa, bb| aa.or(&bb).unwrap_or(Undefined)),
            True => Ok((self.to_owned(), Boolean(true))),
        }
    }

    /// evaluates the specified [Expression]; returning a [TypedValue] result.
    pub fn evaluate_opt(&self, expr: &Option<Box<Expression>>) -> std::io::Result<(Self, TypedValue)> {
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

    fn evaluate_parameters(
        &self,
        columns: &Vec<Parameter>,
    ) -> (Machine, TypedValue) {
        let machine = self.to_owned();
        let values = columns.iter()
            .map(|c| machine.variables.get(c.get_name())
                .map(|c| c.to_owned())
                .unwrap_or(Undefined))
            .collect::<Vec<TypedValue>>();
        (machine, ArrayValue(Array::from(values)))
    }

    /// evaluates the specified [Expression]; returning a [TypedValue] result.
    fn evaluate_scope(&self, ops: &Vec<Expression>) -> (Self, TypedValue) {
        ops.iter().fold((self.to_owned(), Undefined),
                        |(m, _), op| match m.evaluate(op) {
                            Ok((m, ErrorValue(msg))) => (m, ErrorValue(msg)),
                            Ok((m, tv)) => (m, tv),
                            Err(err) => (m, ErrorValue(Exact(err.to_string())))
                        })
    }

    /// evaluates the specified [Expression]; returning a [TypedValue] result.
    fn do_bitwise_ops(
        &self,
        bitwise: &BitwiseOps,
    ) -> std::io::Result<(Self, TypedValue)> {
        match bitwise {
            BitwiseOps::And(a, b) =>
                self.do_inline_2(a, b, |aa, bb| aa & bb),
            BitwiseOps::Or(a, b) =>
                self.do_inline_2(a, b, |aa, bb| aa | bb),
            BitwiseOps::ShiftLeft(a, b) =>
                self.do_inline_2(a, b, |aa, bb| aa << bb),
            BitwiseOps::ShiftRight(a, b) =>
                self.do_inline_2(a, b, |aa, bb| aa >> bb),
            BitwiseOps::Xor(a, b) =>
                self.do_inline_2(a, b, |aa, bb| aa ^ bb),
        }
    }

    fn do_contains(
        &self,
        a: &Expression,
        b: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, a) = self.evaluate(a)?;
        let (machine, b) = machine.evaluate(b)?;
        Ok((machine, a.contains(&b)))
    }

    /// evaluates the specified [Directives]; returning a [TypedValue] result.
    fn do_directive(
        &self,
        directive: &Directives,
    ) -> std::io::Result<(Self, TypedValue)> {
        match directive {
            Directives::MustAck(a) => self.do_directive_ack(a),
            Directives::MustDie(a) => self.do_directive_die(a),
            Directives::MustIgnoreAck(a) => self.do_directive_ignore_ack(a),
            Directives::MustNotAck(a) => self.do_directive_not_ack(a),
        }
    }

    fn do_directive_ack(
        &self,
        expr: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        info!("{}", expr.to_code());
        let (machine, value) = self.evaluate(expr)?;
        match value {
            v if v.is_ok() => Ok((machine, v.to_owned())),
            v => throw(OutcomeExpected(v.to_code()))
        }
    }

    fn do_directive_die(
        &self,
        expr: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        info!("{}", expr.to_code());
        let (_, value) = self.evaluate(expr)?;
        throw(Exact(value.unwrap_value()))
    }

    fn do_directive_ignore_ack(
        &self,
        expr: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        info!("{}", expr.to_code());
        let (machine, _) = self.evaluate(expr)?;
        Ok((machine, Number(Ack)))
    }

    fn do_directive_not_ack(
        &self,
        expr: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        info!("{}", expr.to_code());
        let (machine, value) = self.evaluate(expr)?;
        match value {
            v if v.is_ok() => throw(Exact(format!("Expected a non-success value, but got {}", &v))),
            v => Ok((machine, v))
        }
    }

    fn do_extraction(
        &self,
        object: &Expression,
        field: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let ms = self.clone();
        match object {
            Variable(var_name) =>
                match ms.get(var_name) {
                    None => fail(format!("Variable '{}' was not found", var_name)),
                    Some(object_v) =>
                        match object_v {
                            ErrorValue(err) => Ok((ms, ErrorValue(err))),
                            Null => fail(format!("Cannot evaluate {}::{}", Null, field.to_code())),
                            StructureHard(structure) =>
                                self.do_extraction_structure(var_name, Box::from(structure), field),
                            StructureSoft(structure) =>
                                self.do_extraction_structure(var_name, Box::from(structure), field),
                            Undefined => fail(format!("Cannot evaluate {}::{}", Undefined, field.to_code())),
                            z => fail(format!("Illegal structure {}", z.to_code()))
                        }
                }
            z => fail_expr(Syntax(z.to_code()).to_string(), &z)
        }
    }

    fn do_extraction_structure(
        &self,
        var_name: &str,
        structure: Box<dyn Structure>,
        field: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let ms = self.clone();
        match field {
            // math::compute(5, 8)
            FunctionCall { fx, args } =>
                Ok(structure.pollute(ms).do_function_call(fx, args)),
            // stock::last_sale := 24.11
            SetVariable(name, expr) => {
                let (ms, value) = ms.evaluate(expr)?;
                Ok((ms.with_variable(var_name, structure.update(name, value)), Number(Ack)))
            }
            // stock::symbol
            Variable(name) => Ok((ms, structure.get(name))),
            z => fail(format!("Illegal field '{}' for structure {}",
                              z.to_code(), var_name))
        }
    }

    pub fn do_feature(&self,
                      title: &Box<Expression>,
                      scenarios: &Vec<Expression>,
    ) -> std::io::Result<(Self, TypedValue)> {
        // create a table and capture function to store the verification report
        let verification_columns = Column::from_parameters(&PlatformOps::get_kung_fu_feature_parameters())?;
        let mut report = ModelRowCollection::new(verification_columns.to_owned());
        let mut capture = |level: u16, text: String, passed: bool, result: TypedValue| {
            let outcome = report.push_row(Row::new(0, vec![
                Number(U16Value(level)), StringValue(text), Boolean(passed), result,
            ]));
            let count = match outcome {
                Number(n) => n.to_usize(),
                ErrorValue(err) => return fail(err.to_string()),
                _ => 0
            };
            Ok((self, count))
        };

        // feature processing
        let (mut ms, title) = self.evaluate(title)?;
        capture(0, title.unwrap_value(), true, Number(Ack))?;

        // scenario processing
        for scenario in scenarios {
            match &scenario {
                // scenarios require specialized processing
                Scenario { title, verifications } => {
                    // scenario::title
                    let (msb, subtitle) = ms.evaluate(title)?;
                    ms = msb;

                    // update the report
                    capture(1, subtitle.unwrap_value(), true, Number(Ack))?;

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
        Ok((ms, TableValue(Model(report))))
    }

    /// foreach `name` in `items` { `block` }
    fn do_foreach(&self,
                  name: &str,
                  items: &Box<Expression>,
                  block: &Box<Expression>,
    ) -> (Self, TypedValue) {
        let ms0 = self.clone();
        match ms0.evaluate(items).map(|(ms, tv)| (ms, tv.to_array())) {
            Ok((_, ArrayValue(array))) => {
                let mut index = 0;
                while index < array.len() {
                    match ms0
                        .with_variable(name, array.get(index).unwrap_or(Null))
                        .evaluate(block) {
                        Ok(..) => {}
                        Err(err) => return (ms0, ErrorValue(Exact(err.to_string())))
                    }
                    index += 1
                }
                (ms0, Number(Ack))
            }
            Ok((ms, other)) =>
                (ms, ErrorValue(TypeMismatch(
                    ArrayType(Box::from(UnionType(vec![]))),
                    other.get_type(),
                ))),
            Err(err) => (ms0, ErrorValue(Exact(err.to_string())))
        }
    }

    fn do_function_arguments(&self,
                             params: Vec<Parameter>,
                             args: Vec<TypedValue>,
    ) -> Self {
        assert_eq!(params.len(), args.len());
        params.iter().zip(args.iter())
            .fold(self.to_owned(), |ms, (c, v)|
                ms.with_variable(c.get_name(), v.to_owned()))
    }

    /// Executes a function call
    /// e.g. factorial(5)
    fn do_function_call(&self,
                        fx: &Expression,
                        args: &Vec<Expression>,
    ) -> (Self, TypedValue) {
        let ms = self.to_owned();
        match self.evaluate_array(args) {
            Ok((ms, ArrayValue(args))) =>
                match ms.evaluate(fx) {
                    Ok((ms, Function { params, code })) =>
                        match ms.do_function_arguments(params, args.values().clone()).evaluate(&code) {
                            Ok((ms, result)) => (ms, result),
                            Err(err) => (ms, ErrorValue(Exact(err.to_string())))
                        }
                    Ok((ms, PlatformOp(pf))) => pf.evaluate(ms, args.values().clone()),
                    Ok((_, z)) => (ms, ErrorValue(Exact(format!("'{}' is not a function ({})", fx.to_code(), z)))),
                    Err(err) => (ms, ErrorValue(Exact(err.to_string())))
                }
            Ok((ms, other)) => (ms, ErrorValue(FunctionArgsExpected(other.to_code()))),
            Err(err) => (ms, ErrorValue(Exact(err.to_string())))
        }
    }

    /// Executes a postfix function call
    /// e.g. "hello":::left(5)
    fn do_function_call_postfix(
        &self,
        object: &Expression,
        field: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        match field {
            FunctionCall { fx, args } => {
                // "hello":::left(5) => left("hello", 5)
                let mut enriched_args = Vec::new();
                enriched_args.push(object.to_owned());
                enriched_args.extend(args.to_owned());
                Ok(self.do_function_call(fx, &enriched_args))
            }
            z => fail(format!("{} is not a function call", z.to_code()))
        }
    }

    fn do_http(
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

        fn create_form(structure: Box<dyn Structure>) -> Form {
            structure.get_tuples().iter().fold(Form::new(), |form, (name, value)| {
                form.part(name.to_owned(), Part::text(value.unwrap_value()))
            })
        }

        fn extract_string_tuples(value: TypedValue) -> std::io::Result<Vec<(String, String)>> {
            extract_value_tuples(value)
                .map(|values| values.iter()
                    .map(|(k, v)| (k.to_string(), v.unwrap_value()))
                    .collect())
        }

        fn extract_value_tuples(value: TypedValue) -> std::io::Result<Vec<(String, TypedValue)>> {
            match value {
                StructureSoft(structure) => Ok(structure.get_tuples()),
                z => fail_value(TypeMismatch(StructureType(vec![]), z.get_type()).to_string(), &z),
            }
        }

        ms.do_http_method_call(
            method.unwrap_value(),
            url.unwrap_value(),
            body.map(|tv| tv.to_json().to_string()),
            match headers {
                Some(v) => extract_string_tuples(v)?,
                None => Vec::new()
            },
            match multipart {
                Some(StructureSoft(soft)) => Some(create_form(Box::new(soft))),
                Some(StructureHard(hard)) => Some(create_form(Box::new(hard))),
                Some(z) => return Ok((ms, ErrorValue(TypeMismatch(
                    UnionType(vec![
                        ArrayType(Box::new(StructureType(vec![]))),
                        StringType(BLOBSized),
                        StructureType(vec![]),
                        TableType(vec![], BLOBSized),
                    ]),
                    z.get_type(),
                )))),
                None => None
            },
        )
    }

    async fn do_http_delete(
        &self,
        url: &str,
        headers: Vec<(String, String)>,
        body_opt: Option<String>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let req = Client::new().delete(url);
        Ok((self.to_owned(), self.do_http_rest_call(req, headers, body_opt, false).await))
    }

    async fn do_http_get(
        &self,
        url: &str,
        headers: Vec<(String, String)>,
        body_opt: Option<String>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let req = Client::new().get(url);
        Ok((self.to_owned(), self.do_http_rest_call(req, headers, body_opt, false).await))
    }

    async fn do_http_head(
        &self,
        url: &str,
        headers: Vec<(String, String)>,
        body_opt: Option<String>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let req = Client::new().head(url);
        let value = self.do_http_rest_call(req, headers, body_opt, true).await;
        Ok((self.to_owned(), value))
    }

    fn do_http_method_call(
        &self,
        method: String,
        url: String,
        body: Option<String>,
        headers: Vec<(String, String)>,
        multipart: Option<Form>,
    ) -> std::io::Result<(Self, TypedValue)> {
        async fn www(
            ms: Machine,
            method: String,
            url: String,
            body: Option<String>,
            headers: Vec<(String, String)>,
            multipart: Option<Form>,
        ) -> std::io::Result<(Machine, TypedValue)> {
            match method.to_ascii_uppercase().as_str() {
                "DELETE" => ms.do_http_delete(url.as_str(), headers, body).await,
                "GET" => ms.do_http_get(url.as_str(), headers, body).await,
                "HEAD" => ms.do_http_head(url.as_str(), headers, body).await,
                "PATCH" => ms.do_http_patch(url.as_str(), headers, body).await,
                "POST" => ms.do_http_post(url.as_str(), headers, body, multipart).await,
                "PUT" => ms.do_http_put(url.as_str(), headers, body).await,
                method => Ok((ms.to_owned(), ErrorValue(Exact(format!("Invalid HTTP method '{method}'")))))
            }
        }

        let rt = Runtime::new()?;
        rt.block_on(www(self.to_owned(), method, url, body, headers, multipart))
    }

    async fn do_http_patch(
        &self,
        url: &str,
        headers: Vec<(String, String)>,
        body_opt: Option<String>,
    ) -> std::io::Result<(Self, TypedValue)> {
        Ok((self.to_owned(), self.do_http_rest_call(Client::new().patch(url), headers, body_opt, false).await))
    }

    async fn do_http_post(
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
        Ok((self.to_owned(), self.do_http_rest_call(request, headers, body_opt, false).await))
    }

    async fn do_http_put(
        &self,
        url: &str,
        headers: Vec<(String, String)>,
        body_opt: Option<String>,
    ) -> std::io::Result<(Self, TypedValue)> {
        Ok((self.to_owned(), self.do_http_rest_call(Client::new().put(url), headers, body_opt, false).await))
    }

    async fn do_http_rest_call(
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
            Ok(response) => self.convert_http_response(response, is_header_only).await,
            Err(err) => ErrorValue(Exact(format!("Error making request: {}", err))),
        }
    }

    /// Evaluates an if-then-else expression
    fn do_if_then_else(
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

    /// Produces an aggregate [Machine] instance containing
    /// the specified imports
    fn do_import(
        &self,
        package_name: &str,
        selection: &Vec<String>,
    ) -> (Self, TypedValue) {
        let ms = self.to_owned();
        match ms.get(package_name) {
            None => (ms, ErrorValue(PackageNotFound(package_name.into()))),
            Some(component) => match component {
                StructureHard(structure) =>
                    if selection.is_empty() {
                        (structure.pollute(ms), Number(Ack))
                    } else {
                        let ms = selection.iter().fold(ms, |ms, name| {
                            ms.with_variable(name, structure.get(name))
                        });
                        (ms, Number(Ack))
                    }
                StructureSoft(structure) => {
                    let ms = structure.get_tuples().iter().fold(ms, |ms, (name, value)| {
                        if selection.is_empty() || selection.contains(name) {
                            ms.with_variable(name, value.to_owned())
                        } else { ms }
                    });
                    (ms, Number(Ack))
                }
                other => (ms, ErrorValue(Syntax(other.to_code())))
            }
        }
    }

    fn do_imports(&self, ops: &Vec<ImportOps>) -> (Self, TypedValue) {
        ops.iter()
            .fold((self.to_owned(), Undefined),
                  |(ms, tv), iop| match iop {
                      ImportOps::Everything(pkg) =>
                          ms.do_import(pkg, &Vec::new()),
                      ImportOps::Selection(pkg, selection) =>
                          ms.do_import(pkg, selection),
                  })
    }

    fn do_include(
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
            let opcode = Compiler::build(script_code.as_str())?;
            machine.evaluate(&opcode)
        } else {
            throw(TypeMismatch(StringType(BLOBSized), path_value.get_type()))
        }
    }

    /// Evaluates the index of a collection (array, string or table)
    fn do_index_of_collection(
        &self,
        collection_expr: &Expression,
        index_expr: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, index) = self.evaluate(index_expr)?;
        let (machine, collection) = machine.evaluate(collection_expr)?;
        let value = match collection {
            ArrayValue(items) => {
                let idx = index.to_usize();
                if idx < items.len() { items.get(idx).unwrap_or(Undefined) } else {
                    ErrorValue(IndexOutOfRange("Array".to_string(), idx, items.len()))
                }
            }
            NamespaceValue(ns) => {
                let id = index.to_usize();
                let frc = FileRowCollection::open(&ns)?;
                match frc.read_one(id)? {
                    Some(row) => StructureHard(HardStructure::from_row(frc.get_columns(), &row)),
                    None => StructureHard(HardStructure::new(frc.get_columns().to_owned(), Vec::new()))
                }
            }
            StringValue(string) => {
                let idx = index.to_usize();
                if idx < string.len() { StringValue(string[idx..idx].to_string()) } else { ErrorValue(IndexOutOfRange("String".to_string(), idx, string.len())) }
            }
            StructureHard(structure) => {
                let idx = index.to_usize();
                let values = structure.get_values();
                if idx < values.len() { values[idx].to_owned() } else { ErrorValue(IndexOutOfRange("Structure element".to_string(), idx, values.len())) }
            }
            TableValue(rcv) => {
                let id = index.to_usize();
                match rcv.read_one(id)? {
                    Some(row) => StructureHard(HardStructure::from_row(rcv.get_columns(), &row)),
                    None => StructureHard(HardStructure::new(rcv.get_columns().to_owned(), Vec::new()))
                }
            }
            other =>
                ErrorValue(TypeMismatch(
                    UnionType(vec![
                        ArrayType(Box::from(StructureType(vec![]))),
                        TableType(vec![], BLOBSized)
                    ]),
                    other.get_type(),
                ))
        };
        Ok((machine, value))
    }

    /// evaluates expression `a` then applies function `f`. ex: f(a)
    fn do_inline_1(
        &self,
        a: &Expression,
        f: fn(TypedValue) -> TypedValue,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, aa) = self.evaluate(a)?;
        Ok((machine, f(aa)))
    }

    /// evaluates expressions `a` and `b` then applies function `f`. ex: f(a, b)
    fn do_inline_2(
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
    fn do_inline_3(
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

    fn do_like(
        &self,
        text: &Expression,
        pattern: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        use regex::Regex;
        let (ms, text_v) = self.evaluate(text)?;
        let (ms, pattern_v) = ms.evaluate(pattern)?;
        match (text_v, pattern_v) {
            (StringValue(text), StringValue(pattern)) =>
                match Regex::new(pattern.as_str()) {
                    Ok(pattern) => Ok((ms, Boolean(pattern.is_match(text.as_str())))),
                    Err(err) => Ok((ms, ErrorValue(Exact(err.to_string()))))
                }
            (a, b) =>
                Ok((ms, ErrorValue(Syntax(format!("{} like {}", a.to_code(), b.to_code())))))
        }
    }

    fn do_structure_soft(
        &self,
        items: &Vec<(String, Expression)>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let mut elems = Vec::new();
        for (name, expr) in items {
            let (_, value) = self.evaluate(expr)?;
            elems.push((name.to_string(), value))
        }
        Ok((self.to_owned(), StructureSoft(SoftStructure::from_tuples(elems))))
    }

    fn do_module_alone(
        &self,
        name: &str,
        ops: &Vec<Expression>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let structure = HardStructure::empty();
        let result =
            ops.iter().fold(StructureHard(structure), |tv, op| match tv {
                ErrorValue(msg) => ErrorValue(msg),
                StructureHard(structure) =>
                    match op {
                        SetVariable(name, expr) =>
                            match self.evaluate(expr) {
                                Ok((_, value)) => StructureHard(structure.with_variable(name, value)),
                                Err(err) => ErrorValue(Exact(err.to_string()))
                            },
                        z => ErrorValue(IllegalExpression(z.to_code()))
                    },
                StructureSoft(structure) =>
                    match op {
                        SetVariable(name, expr) =>
                            match self.evaluate(expr) {
                                Ok((_, value)) => StructureSoft(structure.with_variable(name, value)),
                                Err(err) => ErrorValue(Exact(err.to_string()))
                            },
                        z => ErrorValue(IllegalExpression(z.to_code()))
                    },
                z => ErrorValue(StructExpected(name.to_string(), z.to_code()))
            });
        Ok((self.with_variable(name, result), Number(Ack)))
    }

    /// evaluates the specified [Expression]; returning a negative [TypedValue] result.
    fn do_negate(&self, expr: &Expression) -> (Self, TypedValue) {
        match self.evaluate(expr) {
            Ok((machine, result)) => (machine, result.neg()),
            Err(err) => (self.to_owned(), ErrorValue(Exact(err.to_string())))
        }
    }

    fn do_structure_module(
        &self,
        name: &str,
        ops: &Vec<Expression>,
    ) -> (Self, TypedValue) {
        match self.get(name) {
            Some(StructureHard(structure)) =>
                self.do_structure_hard_impl(name, structure, ops),
            Some(StructureSoft(structure)) =>
                self.do_structure_soft_impl(name, structure, ops),
            Some(v) => (self.clone(), ErrorValue(StructExpected(name.into(), v.to_code()))),
            None => match self.do_module_alone(name, ops) {
                Ok(result) => result,
                Err(err) => (self.to_owned(), ErrorValue(Exact(err.to_string())))
            }
        }
    }

    fn do_structure_hard_impl(
        &self,
        name: &str,
        structure: HardStructure,
        ops: &Vec<Expression>,
    ) -> (Self, TypedValue) {
        let result = ops.iter()
            .fold(StructureHard(structure), |tv, op| match tv {
                ErrorValue(msg) => ErrorValue(msg),
                StructureHard(structure) =>
                    match op {
                        SetVariable(name, expr) =>
                            match self.evaluate(expr) {
                                Ok((_, value)) => StructureHard(structure.with_variable(name, value)),
                                Err(err) => ErrorValue(Exact(err.to_string()))
                            },
                        z => ErrorValue(IllegalExpression(z.to_code()))
                    },
                z => ErrorValue(StructExpected(name.to_string(), z.to_code()))
            });
        match result {
            ErrorValue(err) => (self.to_owned(), ErrorValue(err)),
            result => (self.with_variable(name, result), Number(Ack))
        }
    }

    fn do_structure_soft_impl(
        &self,
        name: &str,
        structure: SoftStructure,
        ops: &Vec<Expression>,
    ) -> (Self, TypedValue) {
        let result = ops.iter()
            .fold(StructureSoft(structure), |tv, op| match tv {
                ErrorValue(msg) => ErrorValue(msg),
                StructureSoft(structure) =>
                    match op {
                        SetVariable(name, expr) =>
                            match self.evaluate(expr) {
                                Ok((_, value)) => StructureSoft(structure.with_variable(name, value)),
                                Err(err) => ErrorValue(Exact(err.to_string()))
                            },
                        z => ErrorValue(IllegalExpression(z.to_code()))
                    },
                z => ErrorValue(StructExpected(name.to_string(), z.to_code()))
            });
        match result {
            ErrorValue(err) => (self.to_owned(), ErrorValue(err)),
            result => (self.with_variable(name, result), Number(Ack))
        }
    }

    fn do_while(
        &self,
        condition: &Expression,
        code: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let mut machine = self.to_owned();
        let mut is_looping = true;
        let mut outcome = Undefined;
        while is_looping {
            let (m, result) = machine.evaluate(condition)?;
            machine = m;
            is_looping = result.is_true();
            if is_looping {
                let (m, result) = machine.evaluate(code)?;
                machine = m;
                outcome = result;
            }
        }
        Ok((machine, outcome))
    }

    fn do_plus_plus(
        a: TypedValue,
        b: TypedValue,
    ) -> TypedValue {
        match (a, b) {
            (ArrayValue(a), ArrayValue(b)) => {
                let mut c = a.clone();
                c.push_all(b.values().clone());
                ArrayValue(c)
            }
            (a, b) =>
                ErrorValue(Syntax(format!("{a} ++ {b}")))
        }
    }

    fn assume_table_value<A>(
        &self,
        table: &TypedValue,
        f: fn(Box<dyn RowCollection>) -> std::io::Result<A>,
    ) -> std::io::Result<A> {
        match table {
            NamespaceValue(ns) =>
                f(Box::new(FileRowCollection::open(&ns)?)),
            TableValue(rcv) => f(Box::new(rcv.to_owned())),
            z =>
                fail_value(format!("{} is not a table", z), z)
        }
    }

    fn assume_dataframe<A>(
        &self,
        table: &TypedValue,
        f: fn(Dataframe) -> std::io::Result<A>,
    ) -> std::io::Result<A> {
        match table {
            NamespaceValue(ns) => f(Disk(FileRowCollection::open(&ns)?)),
            TableValue(rc) => f(rc.to_owned()),
            z => throw(TypeMismatch(TableType(vec![], BLOBSized), z.get_type()))
        }
    }

    /// converts a [Response] to a [TypedValue]
    pub async fn convert_http_response(
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
                StructureSoft(SoftStructure::from_tuples(header_keys.iter().zip(header_values.iter())
                    .map(|(k, v)| (k.to_owned(), v.to_owned()))
                    .collect::<Vec<_>>()))
            } else {
                match response.text().await {
                    Ok(body) =>
                        match Compiler::build(body.as_str()) {
                            Ok(expr) => {
                                match self.evaluate(&expr) {
                                    Ok((_, Undefined)) => StructureSoft(SoftStructure::empty()),
                                    Ok((_, value)) => value,
                                    Err(_) => StringValue(body)
                                }
                            }
                            _ => StringValue(body)
                        }
                    Err(err) => ErrorValue(Exact(format!("Error reading response body: {}", err))),
                }
            }
        } else {
            ErrorValue(Exact(format!("Request failed with status: {}", response.status())))
        }
    }

    /// returns a variable by name
    pub fn get(&self, name: &str) -> Option<TypedValue> {
        self.variables.get(name).map(|x| x.to_owned())
    }

    /// returns a variable by name or the default value
    pub fn get_or_else(&self, name: &str, default: fn() -> TypedValue) -> TypedValue {
        self.get(name).unwrap_or(default())
    }

    pub fn get_variables(&self) -> Vec<(&String, &TypedValue)> {
        self.variables.iter().collect::<Vec<_>>()
    }

    pub fn import_module_by_name(&self, name: &str) -> Self {
        let module = vec![
            ImportOps::Everything(name.to_string())
        ];
        match self.do_imports(&module) {
            (m, ErrorValue(err)) => {
                error!("{}", err);
                m.with_variable("__error__", StringValue(err.to_string()))
            }
            (m, _) => m,
        }
    }

    pub fn set(&self, name: &str, value: TypedValue) -> Self {
        let mut variables = self.variables.to_owned();
        variables.insert(name.to_string(), value);
        Self::build(self.stack.to_owned(), variables)
    }

    pub fn show(columns: &Vec<Column>, rows: &Vec<Row>) {
        for s in TableRenderer::from_rows(columns, rows) {
            println!("{}", s)
        }
    }

    pub fn with_module(
        &self,
        name: &str,
        variables: Vec<PlatformOps>,
    ) -> Self {
        let structure = variables.iter().fold(
            HardStructure::empty(),
            |structure, key|
                structure.with_variable(key.get_name().as_str(), PlatformOp(key.clone())));
        self.with_variable(name, StructureHard(structure))
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
        Self::build(self.stack.to_owned(), variables)
    }
}

/// Unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::compiler::Compiler;
    use crate::data_types::DataType::NumberType;
    use crate::expression::Conditions::{Equal, GreaterOrEqual, GreaterThan, LessOrEqual, LessThan};
    use crate::expression::CreationEntity::{IndexEntity, TableEntity};
    use crate::expression::DatabaseOps::Query;
    use crate::expression::MutateTarget::TableTarget;
    use crate::expression::Queryable;
    use crate::expression::{FALSE, NULL, TRUE};
    use crate::number_kind::NumberKind::I64Kind;
    use crate::table_columns::Column;
    use crate::table_renderer::TableRenderer;
    use crate::testdata::*;

    #[test]
    fn test_array_declaration() {
        let models = vec![Literal(Number(F64Value(3.25))), TRUE, FALSE, NULL, UNDEFINED];
        assert_eq!(models.iter().map(|e| e.to_code()).collect::<Vec<_>>(), vec![
            "3.25", "true", "false", "null", "undefined",
        ]);

        let (_, array) = Machine::empty().evaluate_array(&models).unwrap();
        assert_eq!(array, ArrayValue(Array::from(vec![
            Number(F64Value(3.25)), Boolean(true), Boolean(false), Null, Undefined
        ])));
    }

    #[test]
    fn test_aliases() {
        let model = AsValue(
            "symbol".to_string(),
            Box::new(Literal(StringValue("ABC".into()))),
        );
        assert_eq!(model.to_code(), "symbol: \"ABC\"");

        let machine = Machine::empty();
        let (machine, tv) = machine.evaluate(&model).unwrap();
        assert_eq!(tv, StringValue("ABC".into()));
        assert_eq!(machine.get("symbol"), Some(StringValue("ABC".into())));
    }

    #[test]
    fn test_column_set() {
        let model = Parameters(make_quote_parameters());
        assert_eq!(model.to_code(), "symbol: String(8), exchange: String(8), last_sale: f64");

        let machine = Machine::empty()
            .with_variable("symbol", StringValue("ABC".into()))
            .with_variable("exchange", StringValue("NYSE".into()))
            .with_variable("last_sale", Number(F64Value(12.66)));
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, ArrayValue(Array::from(vec![
            StringValue("ABC".into()),
            StringValue("NYSE".into()),
            Number(F64Value(12.66)),
        ])));
    }

    #[test]
    fn test_divide() {
        let machine = Machine::empty().with_variable("x", Number(I64Value(50)));
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

        let machine = Machine::empty();
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, Number(U128Value(720)))
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

        let machine = Machine::empty();
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, Number(I64Value(7)))
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
                    ]
                }
            ],
        };

        let machine = Machine::new_platform()
            .import_module_by_name("kungfu");
        let (_, result) = machine.evaluate(&model).unwrap();
        let table = result.to_table().unwrap();
        let columns = table.get_columns();
        let rows = table.read_active_rows().unwrap();
        for s in TableRenderer::from_table(&table) { println!("{}", s) }
        let dump = rows.iter().map(|row| row.get_values()).collect::<Vec<_>>();
        assert_eq!(dump, vec![
            vec![
                Number(U16Value(0)),
                StringValue("Karate translator".into()),
                Boolean(true),
                Number(Ack),
            ],
            vec![
                Number(U16Value(1)),
                StringValue("Translate Karate Scenario to Oxide Scenario".into()),
                Boolean(true),
                Number(Ack),
            ],
            vec![
                Number(U16Value(2)),
                StringValue("assert(true)".into()),
                Boolean(true),
                Boolean(true),
            ],
        ]);
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

        let (_ms, result) = Machine::empty().evaluate_scope(&opcodes);
        assert_eq!(result, Number(F64Value(14.)))
    }

    #[test]
    fn test_variables() {
        let machine = Machine::empty()
            .with_variable("abc", Number(I32Value(5)))
            .with_variable("xyz", Number(I32Value(58)));
        assert_eq!(machine.get("abc"), Some(Number(I32Value(5))));
        assert_eq!(machine.get("xyz"), Some(Number(I32Value(58))));
    }

    /// Function tests
    #[cfg(test)]
    mod function_tests {
        use super::*;

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
            let (machine, result) = Machine::empty()
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
            let machine = Machine::empty();
            let (machine, result) = machine.evaluate_scope(&vec![
                SetVariable("add".to_string(), Box::new(Literal(fx.to_owned())))
            ]);
            assert_eq!(machine.get("add").unwrap(), fx);
            assert_eq!(result, Number(Ack));

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
            let machine = Machine::empty().with_variable("f", model);
            let (machine, result) = machine.evaluate(&FunctionCall {
                fx: Box::new(Variable("f".into())),
                args: vec![
                    Literal(Number(I64Value(5)))
                ],
            }).unwrap();

            println!("result: {}", result);
            assert_eq!(result, Number(I64Value(120)));
        }
    }

    /// Logical tests
    #[cfg(test)]
    mod logical_tests {
        use super::*;

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

            let machine = Machine::empty().with_variable("num", Number(I64Value(5)));
            let (_, result) = machine.evaluate(&model).unwrap();
            assert_eq!(result, StringValue("No".into()));

            let machine = Machine::empty().with_variable("num", Number(I64Value(37)));
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

            let machine = Machine::empty().with_variable("num", Number(I64Value(5)));
            let (_, result) = machine.evaluate(&model).unwrap();
            assert_eq!(result, StringValue("Yes".into()));

            let machine = Machine::empty().with_variable("num", Number(I64Value(99)));
            let (_, result) = machine.evaluate(&model).unwrap();
            assert_eq!(result, StringValue("No".into()));
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

            let machine = Machine::empty().with_variable("num", Number(I64Value(0)));
            let (machine, _) = machine.evaluate(&model).unwrap();
            assert_eq!(machine.get("num"), Some(Number(I64Value(5))))
        }
    }

    /// SQL tests
    #[cfg(test)]
    mod sql_tests {
        use super::*;
        use crate::compiler::Compiler;
        use crate::expression::Conditions::Equal;
        use crate::expression::CreationEntity::{IndexEntity, TableEntity};
        use crate::expression::DatabaseOps::Mutate;
        use crate::expression::MutateTarget::TableTarget;
        use crate::expression::Mutation::{Append, Create, Declare, Drop, Overwrite, Truncate, Update};
        use crate::expression::{DatabaseOps, Mutation};
        use crate::table_columns::Column;
        use crate::testdata::{make_quote, make_quote_columns, make_quote_parameters};

        #[test]
        fn test_from_where_limit_in_memory() {
            // create a table with test data
            let columns = make_quote_parameters();
            let phys_columns = Column::from_parameters(&columns).unwrap();
            let machine = Machine::empty()
                .with_variable("stocks", TableValue(Model(ModelRowCollection::from_rows(&phys_columns, &vec![
                    make_quote(0, "ABC", "AMEX", 12.33),
                    make_quote(1, "UNO", "OTC", 0.2456),
                    make_quote(2, "BIZ", "NYSE", 9.775),
                    make_quote(3, "GOTO", "OTC", 0.1442),
                    make_quote(4, "XYZ", "NYSE", 0.0289),
                ]))));

            let model = DatabaseOp(Query(Queryable::Limit {
                from: Box::new(DatabaseOp(Query(Queryable::Where {
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
            assert_eq!(result, TableValue(Model(ModelRowCollection::from_rows(&phys_columns, &vec![
                make_quote(0, "ABC", "AMEX", 12.33),
                make_quote(2, "BIZ", "NYSE", 9.775),
            ]))));
        }

        #[test]
        fn test_index_of_table_in_namespace() {
            // create a table with test data
            let ns = Namespace::new("machine", "element_at", "stocks");
            let params = make_quote_parameters();
            let columns = Column::from_parameters(&params).unwrap();
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
            let mut dfrc = FileRowCollection::create_table(&ns, &params).unwrap();
            assert_eq!(Number(RowsAffected(5)), dfrc.append_rows(rows));

            // create the instruction model 'ns("machine.element_at.stocks")[2]'
            let model = ElementAt(
                Box::new(Ns(Box::new(Literal(StringValue(ns.into()))))),
                Box::new(Literal(Number(I64Value(2)))),
            );
            assert_eq!(model.to_code(), r#"ns("machine.element_at.stocks")[2]"#);

            // evaluate the instruction
            let (_, result) = Machine::empty().evaluate(&model).unwrap();
            assert_eq!(result, StructureHard(
                HardStructure::from_row(
                    &columns,
                    &make_quote(2, "BIZ", "NYSE", 23.66))
            ))
        }

        #[test]
        fn test_index_of_table_in_variable() {
            let phys_columns = make_quote_columns();
            let my_table = ModelRowCollection::from_rows(&phys_columns, &vec![
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
            let machine = Machine::empty()
                .with_variable("stocks", TableValue(Model(my_table)));
            let (_, result) = machine.evaluate(&model).unwrap();
            assert_eq!(result, StructureHard(
                HardStructure::from_row(&phys_columns,
                                        &make_quote(4, "VAPOR", "NYSE", 0.0289))
            ))
        }

        #[test]
        fn test_create_table() {
            let model = DatabaseOp(Mutate(Create {
                path: Box::new(Ns(Box::new(Literal(StringValue("machine.create.stocks".into()))))),
                entity: TableEntity {
                    columns: make_quote_parameters(),
                    from: None,
                },
            }));

            // create the table
            let machine = Machine::empty();
            let (_, result) = machine.evaluate(&model).unwrap();
            assert_eq!(result, Number(Ack));

            // decompile back to source code
            assert_eq!(
                model.to_code(),
                "create table ns(\"machine.create.stocks\") (symbol: String(8), exchange: String(8), last_sale: f64)"
            );
        }

        #[test]
        fn test_create_table_with_index() {
            let path = "machine.index.stocks";
            let machine = Machine::empty();

            // drop table if exists ns("machine.index.stocks")
            let (machine, _) = machine.evaluate(&DatabaseOp(Mutate(Drop(TableTarget {
                path: Box::new(Ns(Box::new(Literal(StringValue(path.into()))))),
            })))).unwrap();

            // create table ns("machine.index.stocks") (...)
            let (machine, result) = machine.evaluate(&DatabaseOp(Mutate(Create {
                path: Box::new(Ns(Box::new(Literal(StringValue(path.into()))))),
                entity: TableEntity {
                    columns: make_quote_parameters(),
                    from: None,
                },
            }))).unwrap();
            assert_eq!(result, Number(Ack));

            // create index ns("machine.index.stocks") [symbol, exchange]
            let model = DatabaseOp(Mutate(Create {
                path: Box::new(Ns(Box::new(Literal(StringValue(path.into()))))),
                entity: IndexEntity {
                    columns: vec![
                        Variable("symbol".into()),
                        Variable("exchange".into()),
                    ],
                },
            }));
            let (_, result) = machine.evaluate(&model).unwrap();
            assert_eq!(result, Number(Ack));

            // decompile back to source code
            assert_eq!(
                model.to_code(),
                "create index ns(\"machine.index.stocks\") [symbol, exchange]"
            );
        }

        #[test]
        fn test_declare_table() {
            let model = DatabaseOp(Mutate(Declare(TableEntity {
                columns: vec![
                    Parameter::new("symbol", Some("String(8)".into()), None),
                    Parameter::new("exchange", Some("String(8)".into()), None),
                    Parameter::new("last_sale", Some("f64".into()), None),
                ],
                from: None,
            })));

            let machine = Machine::empty();
            let (_, result) = machine.evaluate(&model).unwrap();
            assert_eq!(result, TypedValue::TableValue(Model(ModelRowCollection::with_rows(
                make_quote_columns(), Vec::new(),
            ))))
        }

        #[test]
        fn test_drop_table() {
            // create a table with test data
            let ns_path = "machine.drop.stocks";
            let ns = Ns(Box::new(Literal(StringValue(ns_path.into()))));
            create_dataframe(ns_path);

            let model = DatabaseOp(Mutate(Drop(TableTarget { path: Box::new(ns) })));
            assert_eq!(model.to_code(), "drop table ns(\"machine.drop.stocks\")");

            let machine = Machine::empty();
            let (_, result) = machine.evaluate(&model).unwrap();
            assert_eq!(result, Number(Ack))
        }

        #[test]
        fn test_delete_rows_from_namespace() {
            // create a table with test data
            let ns_path = "machine.delete.stocks";
            let (df, _) = create_dataframe(ns_path);

            // delete some rows
            let machine = Machine::empty();
            let code = Compiler::build(r#"
            delete from ns("machine.delete.stocks")
            where last_sale > 1.0
            "#).unwrap();
            let (_, result) = machine.evaluate(&code).unwrap();
            assert_eq!(result, Number(RowsAffected(3)));

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
            let (df, _) = create_dataframe(ns_path);

            // delete some rows
            let machine = Machine::empty();
            let code = Compiler::build(r#"
            stocks := ns("machine.undelete.stocks")
            delete from stocks where last_sale > 1.0
            "#).unwrap();
            let (machine, result) = machine.evaluate(&code).unwrap();
            assert_eq!(result, Number(RowsAffected(3)));

            // verify the remaining rows
            let rows = df.read_active_rows().unwrap();
            assert_eq!(rows, vec![
                make_quote(1, "UNO", "OTC", 0.2456),
                make_quote(3, "GOTO", "OTC", 0.1428),
            ]);

            // undelete the rows
            let code = Compiler::build(r#"
            undelete from stocks where last_sale > 1.0
            "#).unwrap();
            let (_, result) = machine.evaluate(&code).unwrap();
            assert_eq!(result, Number(RowsAffected(3)));

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
            let (df, _) = create_dataframe(ns_path);

            // insert some rows
            let machine = Machine::empty();
            let (_, result) = machine.evaluate(&DatabaseOp(Mutate(Append {
                path: Box::new(Ns(Box::new(Literal(StringValue(ns_path.to_string()))))),
                source: Box::new(From(Box::new(JSONExpression(vec![
                    ("symbol".into(), Literal(StringValue("REX".into()))),
                    ("exchange".into(), Literal(StringValue("NASDAQ".into()))),
                    ("last_sale".into(), Literal(Number(F64Value(16.99)))),
                ])))),
            }))).unwrap();
            assert_eq!(result, Number(RowsAffected(1)));

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
            let model = DatabaseOp(Mutate(Overwrite {
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
            let (df, _) = create_dataframe(ns_path);

            // overwrite some rows
            let machine = Machine::empty();
            let (_, result) = machine.evaluate(&model).unwrap();
            assert_eq!(result, Number(RowsAffected(1)));

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
        fn test_select_from_namespace() {
            // create a table with test data
            let (_, phys_columns) =
                create_dataframe("machine.select.stocks");

            // execute the query
            let machine = Machine::empty();
            let (_, result) = machine.evaluate(&DatabaseOp(Query(Queryable::Select {
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
            assert_eq!(result, TableValue(Model(ModelRowCollection::from_rows(&phys_columns, &vec![
                make_quote(0, "ABC", "AMEX", 11.77),
                make_quote(2, "BIZ", "NYSE", 23.66),
                make_quote(4, "BOOM", "NASDAQ", 56.87),
            ]))));
        }

        #[ignore]
        #[test]
        fn test_select_from_variable() {
            let (mrc, phys_columns) = create_memory_table();
            let machine = Machine::empty()
                .with_variable("stocks", TableValue(Model(mrc)));

            // execute the code
            let (_, result) = machine.evaluate(&DatabaseOp(Query(Queryable::Select {
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
            assert_eq!(result, TableValue(Model(ModelRowCollection::from_rows(&phys_columns, &vec![
                make_quote(1, "UNO", "OTC", 0.2456),
                make_quote(3, "GOTO", "OTC", 0.1428),
            ]))));
        }

        #[ignore]
        #[test]
        fn test_update_rows_in_memory() {
            // build the update model
            let model = DatabaseOp(Mutate(Update {
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
            let machine = Machine::empty().with_variable("stocks", TableValue(Model(mrc)));
            let (_, result) = machine.evaluate(&model).unwrap();
            assert_eq!(result, Number(RowsAffected(2)));

            // retrieve and verify all rows
            let model = From(Box::new(Variable("stocks".into())));
            let (machine, _) = machine.evaluate(&model).unwrap();
            assert_eq!(machine.get("stocks").unwrap(), TableValue(Model(ModelRowCollection::from_rows(&phys_columns, &vec![
                make_quote(0, "ABC", "AMEX", 11.77),
                make_quote(1, "UNO", "OTC_BB", 0.2456),
                make_quote(2, "BIZ", "NYSE", 23.66),
                make_quote(3, "GOTO", "OTC_BB", 0.1428),
                make_quote(4, "BOOM", "NASDAQ", 56.87),
            ]))))
        }

        #[test]
        fn test_update_rows_in_namespace() {
            // create a table with test data
            let ns_path = "machine.update.stocks";
            let (df, _) = create_dataframe(ns_path);

            // create the instruction model
            let model = DatabaseOp(Mutate(Update {
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
            let (_, delta) = Machine::empty().evaluate(&model).unwrap();
            assert_eq!(delta, Number(RowsAffected(2)));

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
            let model = DatabaseOp(Mutate(Truncate {
                path: Box::new(Variable("stocks".into())),
                limit: Some(Box::new(Literal(Number(I64Value(0))))),
            }));

            let (mrc, _) = create_memory_table();
            let machine = Machine::empty().with_variable("stocks", TableValue(Model(mrc)));
            let (_, result) = machine.evaluate(&model).unwrap();
            assert_eq!(result, Number(Ack))
        }

        fn create_memory_table() -> (ModelRowCollection, Vec<Column>) {
            let phys_columns = make_quote_columns();
            let mrc = ModelRowCollection::from_rows(&phys_columns, &vec![
                make_quote(0, "ABC", "AMEX", 11.77),
                make_quote(1, "UNO", "OTC", 0.2456),
                make_quote(2, "BIZ", "NYSE", 23.66),
                make_quote(3, "GOTO", "OTC", 0.1428),
                make_quote(4, "BOOM", "NASDAQ", 56.87)]);
            (mrc, phys_columns)
        }

        fn create_dataframe(namespace: &str) -> (Dataframe, Vec<Column>) {
            let params = make_quote_parameters();
            let columns = Column::from_parameters(&params).unwrap();
            let ns = Namespace::parse(namespace).unwrap();
            match fs::remove_file(ns.get_table_file_path()) {
                Ok(..) => {}
                Err(err) => error!("create_dataframe: {}", err)
            }

            let mut df = make_dataframe_ns(ns, params.to_owned()).unwrap();
            assert_eq!(0, df.append_row(make_quote(0, "ABC", "AMEX", 11.77)).to_usize());
            assert_eq!(1, df.append_row(make_quote(1, "UNO", "OTC", 0.2456)).to_usize());
            assert_eq!(2, df.append_row(make_quote(2, "BIZ", "NYSE", 23.66)).to_usize());
            assert_eq!(3, df.append_row(make_quote(3, "GOTO", "OTC", 0.1428)).to_usize());
            assert_eq!(4, df.append_row(make_quote(4, "BOOM", "NASDAQ", 56.87)).to_usize());
            (df, columns)
        }
    }
}