#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
//  Machine - state machine module
////////////////////////////////////////////////////////////////////

use actix::fut::result;
use actix_web::web::{scope, to};
use chrono::{Datelike, Local, TimeZone, Timelike};
use crossterm::style::Stylize;
use futures_util::SinkExt;
use isahc::{Body, ReadResponseExt, Request, RequestExt, Response};
use log::{error, info};
use regex::Error;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::convert::{From, Into};
use std::fmt::format;
use std::fs::File;
use std::io::{stdout, Read, Write};
use std::ops::{Deref, Neg};
use std::path::Path;
use std::process::Output;
use std::{env, fs, thread};
use tokio::runtime::Runtime;
use tokio_tungstenite::tungstenite::http::Method;
use uuid::Uuid;

use crate::columns::Column;
use crate::compiler::Compiler;
use crate::cursor::Cursor;
use crate::data_types::DataType;
use crate::data_types::DataType::*;
use crate::sequences::{is_in_range, Array, Sequence};

use crate::dataframe::Dataframe;
use crate::dataframe::Dataframe::{Disk, Model, TestReport};

use crate::errors::Errors::*;
use crate::errors::SyntaxErrors::IllegalExpression;
use crate::errors::TypeMismatchErrors::*;
use crate::errors::{throw, Errors, SyntaxErrors, TypeMismatchErrors};
use crate::expression::Conditions::{AssumedBoolean, False, In, True, When};
use crate::expression::Expression::*;
use crate::expression::Ranges::{Exclusive, Inclusive};
use crate::expression::{Conditions, Expression, HttpMethodCalls, UseOps, UNDEFINED};
use crate::file_row_collection::FileRowCollection;
use crate::machine::Observations::VariableObservation;
use crate::model_row_collection::ModelRowCollection;
use crate::namespaces::Namespace;
use crate::number_kind::NumberKind::I64Kind;
use crate::numbers::Numbers::*;
use crate::object_config::{HashIndexConfig, ObjectConfig};
use crate::packages::{ToolsPkg, UtilsPkg};
use crate::parameter::Parameter;
use crate::platform::{Package, PackageOps};
use crate::query_engine::QueryEngine;
use crate::row_collection::RowCollection;
use crate::sequences::Sequences::{TheArray, TheDataframe, TheRange, TheTuple};
use crate::structures::Row;
use crate::structures::Structures::{Firm, Hard, Soft};
use crate::structures::*;
use crate::table_renderer::TableRenderer;
use crate::test_engine::TestEngine;
use crate::testdata::verify_exact_table_with;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::*;
use crate::utils::{lift_condition, pull_array, pull_bool, pull_identifier_name, pull_name, pull_number, pull_string, pull_string_lit, pull_vec};
use crate::{compiler, query_engine};
use shared_lib::cnv_error;

pub const ROW_ID: &str = "__row_id__";
pub const FX_SELF: &str = "__self__";

/// Represents the state of the machine.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Machine {
    observations: Vec<Observations>,
    variables: HashMap<String, TypedValue>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Observations {
    VariableObservation { condition: Conditions, code: Expression },
}

impl Ord for Machine {
    fn cmp(&self, other: &Self) -> Ordering {
        self.observations.cmp(&other.observations)
    }
}

impl PartialOrd for Machine {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.observations.partial_cmp(&other.observations)
    }
}

impl Machine {

    ////////////////////////////////////////////////////////////////
    //  constructors
    ////////////////////////////////////////////////////////////////

    /// (lowest-level constructor) creates a new state machine
    fn build(
        observations: Vec<Observations>,
        variables: HashMap<String, TypedValue>,
    ) -> Self {
        Self { observations, variables }
    }

    /// creates a new completely empty state machine
    pub fn empty() -> Self {
        Self::build(Vec::new(), HashMap::new())
    }

    /// creates a new state machine prepopulated with platform packages
    pub fn new() -> Self {
        PackageOps::build_packages().iter()
            .fold(Self::empty(), |ms, (name, list)| {
                ms.with_module(name, list.to_owned())
            })
    }

    /// creates a new state machine prepopulated with platform packages
    /// and default imports and/or constants
    pub fn new_platform() -> Self {
        Self::new()
            .with_variable("𝑒", Number(F64Value(std::f64::consts::E)))
            .with_variable("π", Number(F64Value(std::f64::consts::PI)))
            .with_variable("γ", Number(F64Value(0.577215664901532860606512090082402431_f64)))
            .with_variable("φ", Number(F64Value((1f64 + 5.0f64.sqrt()) / 2f64)))
    }

    ////////////////////////////////////////////////////////////////
    //  static methods
    ////////////////////////////////////////////////////////////////

    pub fn oxide_home() -> String {
        let oxide_db = "oxide_db";
        env::var("OXIDE_HOME")
            .unwrap_or(home::home_dir()
                .map(|dir| format!("{}/{oxide_db}", dir.display().to_string()))
                .unwrap_or(format!("./{oxide_db}")))
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
            ArrayExpression(items) => self.evaluate_as_array(items),
            ArrowCurvyLeft(a, b) => self.exec_arrow_curvy_left(a, b),
            ArrowCurvyLeft2x(a, b) => self.exec_arrow_curvy_x2_left(a, b),
            ArrowCurvyRight(a, b) => self.exec_arrow_curvy_right(a, b),
            ArrowCurvyRight2x(a, b) => self.exec_arrow_curvy_x2_right(a, b),
            ArrowFat(a, b) => self.exec_arrow_fat(a, b),
            ArrowSkinnyLeft(a, b) => self.exec_arrow_skinny_left(a, b),
            ArrowSkinnyRight(a, b) => self.exec_arrow_skinny_right(a, b),
            ArrowVerticalBar(a, b) => self.exec_function_pipeline(a, b, false),
            ArrowVerticalBar2x(a, b) => self.exec_function_pipeline(a, b, true),
            Assert { condition, message } =>  self.exec_assert(condition, message),
            BitwiseAnd(a, b) => self.eval_inline_2(a, b, |aa, bb| aa & bb),
            BitwiseOr(a, b) => self.eval_inline_2(a, b, |aa, bb| aa | bb),
            BitwiseShiftLeft(a, b) => self.eval_inline_2(a, b, |aa, bb| aa << bb),
            BitwiseShiftRight(a, b) => self.eval_inline_2(a, b, |aa, bb| aa >> bb),
            BitwiseXor(a, b) => self.eval_inline_2(a, b, |aa, bb| aa ^ bb),
            Coalesce(a, b) => self.eval_inline_2(a, b, |aa, bb| aa.coalesce(bb)),
            CoalesceErr(a, b) => self.exec_coalesce_error(a, b),
            CodeBlock(ops) => self.evaluate_scope(ops),
            ColonColon(a, b) => self.exec_colon_colon(a, b),
            ColonColonColon(a, b) => self.exec_colon_colon_colon(a, b),
            Condition(condition) => self.evaluate_condition(condition),
            Divide(a, b) => self.eval_inline_2(a, b, |aa, bb| aa / bb),
            DoWhile { condition, code } => self.exec_while(condition, code),
            ElementAt(a, b) => self.exec_index_of_collection(a, b),
            Feature { title, scenarios } => TestEngine::add_feature(self, title, scenarios),
            For { construct, op } => self.exec_for_construct(construct, op),
            FunctionCall { fx, args } => self.exec_function_call(fx, args),
            HTTP(method_call) => self.exec_http(method_call),
            Identifier(name) => self.exec_get_variable(name),
            If { condition, a, b } => self.exec_if_then_else(condition, a, b),
            Include(path) => self.exec_include(path),
            Infix(a, b) => self.exec_infix(a, b),
            IsDefined(a) => self.exec_is_defined(a),
            Literal(value) => Ok((self.to_owned(), value.to_owned())),
            MatchExpression(src, cases) => self.exec_match(src, cases),
            Minus(a, b) => self.eval_inline_2(a, b, |aa, bb| aa - bb),
            Module(name, ops) => self.exec_structure_module(name, ops),
            Modulo(a, b) => self.eval_inline_2(a, b, |aa, bb| aa % bb),
            Multiply(a, b) => self.eval_inline_2(a, b, |aa, bb| aa * bb),
            NamedValue(name, expr) => {
                let (machine, tv) = self.evaluate(expr)?;
                Ok((machine.with_variable(name, tv.clone()), tv))
            }
            Neg(a) => self.exec_negate(a),
            Parameters(params) => self.evaluate_parameters(params),
            Plus(a, b) => self.eval_inline_2(a, b, |aa, bb| aa + bb),
            PlusPlus(a, b) => self.eval_inline_2(a, b, Self::exec_plus_plus),
            Pow(a, b) => self.eval_inline_2(a, b, |aa, bb| aa.pow(&bb).unwrap_or(Undefined)),
            Range(Exclusive(a, b)) => self.eval_inline_2(a, b, |aa, bb| aa.range_exclusive(&bb).unwrap_or(Undefined)),
            Range(Inclusive(a, b)) => self.eval_inline_2(a, b, |aa, bb| aa.range_inclusive(&bb).unwrap_or(Undefined)),
            Referenced(a) => self.exec_referenced(a),
            Return(a) => self.evaluate(a),
            Scenario { .. } => throw(Exact("scenario should not be called directly".to_string())),
            SetVariables(vars, values) => self.exec_set_variables(vars, values),
            SetVariablesExpr(vars, values) => self.exec_set_variables_expr(vars, values),
            StructureExpression(items) => self.exec_structure_soft(items),
            Test(expr) => TestEngine::evaluate_feature(self, expr),
            Throw(expr) => self.exec_throw(expr),
            TupleExpression(args) => self.exec_tuple(args),
            TypeDef(expr) => self.exec_type_def(expr),
            TypeOf(expr) => self.exec_type_of(expr),
            Use(ops) => self.exec_uses(ops),
            WhenEver { condition, code } => self.exec_when_statement(condition, code),
            While { condition, code } => self.exec_while(condition, code),
            Yield(a) => self.exec_yield(a),
            Zip(a, b) => self.exec_zip(a, b),
            ////////////////////////////////////////////////////////////////////
            // SQL models
            ////////////////////////////////////////////////////////////////////
            Delete { .. } | GroupBy { .. } | Having { .. }
            | Limit { .. } | OrderBy { .. } | Select { .. }
            | Undelete { .. } | Where { .. } =>
                QueryEngine::evaluate(self, expression),
        }
    }

    /// evaluates the specified [Expression]; returning an array ([TypedValue]) result.
    pub fn evaluate_as_array(
        &self,
        ops: &Vec<Expression>,
    ) -> std::io::Result<(Self, TypedValue)> {
        self.evaluate_as_vec(ops).map(|(ms, vec)| (ms, ArrayValue(Array::from(vec))))
    }

    /// evaluates the specified [Expression]; returning an array ([String]) result.
    pub fn evaluate_as_atoms(
        &self,
        ops: &Vec<Expression>,
    ) -> std::io::Result<(Self, Vec<String>)> {
        let (machine, results, errors) = ops.iter()
            .fold((self.to_owned(), Vec::new(), Vec::new()),
                  |(ma, mut array, mut errors), op| match op {
                      Identifier(name) => {
                          array.push(name.to_owned());
                          (ma, array, errors)
                      }
                      expr => {
                          errors.push(ErrorValue(TypeMismatch(ParameterExpected(expr.to_code()))));
                          (ma, array, errors)
                      }
                  });

        // if errors occurred ...
        if !errors.is_empty() {
            throw(Exact(errors.iter()
                .map(|v| v.unwrap_value())
                .collect::<Vec<_>>()
                .join("\n")))
        } else {
            Ok((machine, results))
        }
    }

    /// evaluates the specified [Expression]; returning a [Dataframe] result.
    pub fn evaluate_as_dataframe(
        &self,
        expr: &Expression,
    ) -> std::io::Result<(Self, Dataframe)> {
        let (ms, value) = self.evaluate(expr)?;
        Ok((ms, value.to_dataframe()?))
    }

    /// evaluates the specified [Expression]; returning a [String] result.
    pub fn evaluate_as_string(
        &self,
        expression: &Expression,
    ) -> std::io::Result<(Self, String)> {
        let (ms, value) = self.evaluate(expression)?;
        match value {
            StringValue(s) => Ok((ms, s)),
            other => throw(TypeMismatch(StringExpected(other.to_code())))
        }
    }
    
    /// evaluates the specified [Expression]; returning a [Vec] of [TypedValue]s.
    pub fn evaluate_as_vec(
        &self,
        ops: &Vec<Expression>,
    ) -> std::io::Result<(Self, Vec<TypedValue>)> {
        let (ms, results) = ops.iter()
            .fold((self.to_owned(), Vec::new()),
                  |(ms, mut array), op| match ms.evaluate(op) {
                      Ok((ms, tv)) => {
                          array.push(tv);
                          (ms, array)
                      }
                      Err(err) => {
                          error!("{}", err.to_string());
                          (ms, array)
                      }
                  });
        Ok((ms, results))
    }

    /// evaluates the specified [Expression]; returning a [TypedValue] result.
    pub fn evaluate_condition(
        &self,
        condition: &Conditions,
    ) -> std::io::Result<(Self, TypedValue)> {
        use crate::expression::Conditions::*;
        match condition {
            And(a, b) => self.eval_inline_2(a, b, |aa, bb| aa.and(&bb).unwrap_or(Undefined)),
            AssumedBoolean(a) => self.exec_assume_boolean(a),
            Contains(a, b) => self.exec_contains(a, b),
            Equal(a, b) => self.eval_inline_2(a, b, |aa, bb| Boolean(aa == bb)),
            False => Ok((self.to_owned(), Boolean(false))),
            GreaterThan(a, b) => self.eval_inline_2(a, b, |aa, bb| Boolean(aa > bb)),
            GreaterOrEqual(a, b) => self.eval_inline_2(a, b, |aa, bb| Boolean(aa >= bb)),
            In(a, b) => self.exec_in(a, b),
            LessThan(a, b) => self.eval_inline_2(a, b, |aa, bb| Boolean(aa < bb)),
            LessOrEqual(a, b) => self.eval_inline_2(a, b, |aa, bb| Boolean(aa <= bb)),
            Like(text, pattern) => self.exec_like(text, pattern),
            Matches(src, pattern) => self.eval_inline_2(src, pattern, |aa, bb| aa.matches(&bb)),
            Not(a) => self.eval_inline_1(a, |aa| !aa),
            NotEqual(a, b) => self.eval_inline_2(a, b, |aa, bb| Boolean(aa != bb)),
            Or(a, b) => self.eval_inline_2(a, b, |aa, bb| aa.or(&bb).unwrap_or(Undefined)),
            True => Ok((self.to_owned(), Boolean(true))),
            When(..) => Ok((self.clone(), Boolean(false))),
        }
    }

    /// evaluates the specified [Expression]; returning a [TypedValue] result.
    pub fn evaluate_opt(&self, expr: &Option<Box<Expression>>) -> std::io::Result<(Self, Option<TypedValue>)> {
        match expr {
            None => Ok((self.to_owned(), None)),
            Some(item) => self.evaluate(item)
                .map(|(ms, result)| (ms, Some(result)))
        }
    }

    /// evaluates the specified [Expression]; returning a [TypedValue] result.
    pub fn evaluate_or_undef(&self, expr: &Option<Box<Expression>>) -> std::io::Result<(Self, TypedValue)> {
        match expr {
            None => Ok((self.to_owned(), Undefined)),
            Some(item) => self.evaluate(item)
        }
    }

    fn evaluate_parameters(
        &self,
        columns: &Vec<Parameter>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let machine = self.to_owned();
        let values = columns.iter()
            .map(|c| machine.variables.get(c.get_name())
                .map(|c| c.to_owned())
                .unwrap_or(Undefined))
            .collect::<Vec<TypedValue>>();
        Ok((machine, ArrayValue(Array::from(values))))
    }

    /// evaluates the specified [Expression]; returning a [TypedValue] result.
    fn evaluate_scope(&self, ops: &Vec<Expression>) -> std::io::Result<(Self, TypedValue)> {
        let result = ops.iter().fold(
            (self.to_owned(), Undefined),
            |(m, result0), op| match result0 {
                ErrorValue(msg) => (m, ErrorValue(msg)),
                _ => match m.evaluate(op) {
                    Ok((m, tv)) => (m, tv),
                    Err(err) => (m, ErrorValue(Exact(err.to_string())))
                }
            });
        match result {
            (_, ErrorValue(err)) => throw(err),
            result => Ok(result),
        }
    }

    pub fn is_true(
        &self,
        conditions: &Conditions,
    ) -> std::io::Result<(Self, bool)> {
        let (ms, result) = self.evaluate_condition(conditions)?;
        Ok((ms, pull_bool(&result)?))
    }

    pub fn is_true_expr(
        &self,
        conditions: &Expression,
    ) -> std::io::Result<(Self, bool)> {
        let (ms, result) = self.evaluate(&conditions)?;
        Ok((ms, pull_bool(&result)?))
    }

    /// Evaluates a curvy left arrow (<~)
    /// #### fetches a record from a table
    /// ```
    /// { symbol: "XYZ", exchange: "NASDAQ", last_sale: 0.0872 } ~> stocks
    /// stock <~ stocks
    /// // { symbol: "XYZ", exchange: "NASDAQ", last_sale: 0.0872 }
    /// ```
    fn exec_arrow_curvy_left(
        &self,
        dest: &Expression,
        src: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        QueryEngine::eval_pull_row(&self, dest, src)
    }

    /// Evaluates a curvy left x2 arrow (<<~)
    /// #### fetches all remaining records from a table
    /// ```
    /// { symbol: "XYZ", exchange: "NASDAQ", last_sale: 0.0872 } ~>> stocks
    /// stock <<~ stocks
    /// // { symbol: "XYZ", exchange: "NASDAQ", last_sale: 0.0872 }
    /// ```
    fn exec_arrow_curvy_x2_left(
        &self,
        dest: &Expression,
        src: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        QueryEngine::eval_pull_rows(&self, dest, src)
    }

    /// Evaluates a curvy right arrow (~>), which appends record(s) to a table
    /// #### Examples
    /// ##### insert a row into the table
    /// ```
    /// { symbol: "XYZ", exchange: "NASDAQ", last_sale: 0.0872 } 
    ///     ~> stocks
    /// ```
    /// ##### update a row within the table
    /// ```
    /// { symbol: "BKP", last_sale: 0.1421 } 
    ///    ~> (stocks where symbol is "BKPQ")
    /// ```
    fn exec_arrow_curvy_right(
        &self,
        source: &Expression,
        target: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        QueryEngine::eval_push_rows(self, source, target, false)
    }

    /// Evaluates a curvy right x2 arrow (~>>), which overwrites record(s) in a table
    /// #### Examples
    /// ##### overwrites all rows of a table
    /// ```
    /// { symbol: "XYZ", exchange: "NASDAQ", last_sale: 0.0872 }
    ///     ~>> stocks
    /// ```
    /// ##### overwrite a row within the table
    /// ```
    /// { symbol: "BKP", exchange: "OTCBB", last_sale: 0.1421 }
    ///    ~>> (stocks where symbol is "BKPQ")
    /// ```
    fn exec_arrow_curvy_x2_right(
        &self,
        source: &Expression,
        target: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        QueryEngine::eval_push_rows(self, source, target, true)
    }

    /// Evaluates a fat arrow (=>)
    /// #### Examples
    /// ```
    /// x when x == 1 => stocks
    /// ```
    fn exec_arrow_fat(
        &self,
        _src: &Expression,
        _dest: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        Ok((self.to_owned(), Undefined))
    }

    /// Evaluates a skinny arrow left (<-)
    /// #### Examples
    /// ```
    /// x <- stocks
    /// ```
    fn exec_arrow_skinny_left(
        &self,
        _src: &Expression,
        _dest: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        Ok((self.to_owned(), Undefined))
    }

    /// Creates a function definition
    /// #### Examples
    /// ```
    /// fn product(a: i64, b: i64): i64 -> a * b
    /// ```
    /// ```
    /// fn product(a: i64, b: i64) -> a * b
    /// ```
    /// ```
    /// fn product(a, b) -> a * b
    /// ```
    /// ```
    /// product = (a, b) -> a * b
    /// ```
    /// ```
    /// power = (a, b = 1) -> a * b
    /// ```
    fn exec_arrow_skinny_right(
        &self,
        head: &Expression,
        body: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        // resolve the optional name, parameters and return type
        let (name_maybe, params, return_type_maybe) =
            compiler::resolve_name_and_parameters_and_return_type(head)?;
        let fx = Function {
            params,
            returns: return_type_maybe.unwrap_or(body.infer_type()),
            body: body.clone().into(),
        };

        // build the model
        match name_maybe {
            None => Ok((self.to_owned(), fx)),
            Some(name) => Ok((self.with_variable(&name, fx), Boolean(true)))
        }
    }

    /// Evaluates an assertion returning true or an error
    fn exec_assert(
        &self,
        condition: &Expression,
        message: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        match self.exec_assume_boolean(condition)? {
            (machine, Boolean(true)) => Ok((machine, Boolean(true))),
            _ => {
                let message = match message {
                    Some(msg) => msg.to_code(),
                    None => "Assertion failed".to_string()
                };
                throw(Exact(message))
            }
        }
    }

    /// Requires the result to be [Boolean]
    fn exec_assume_boolean(
        &self,
        expr: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        match self.evaluate(expr)? {
            (machine, Boolean(b)) => Ok((machine, Boolean(b))),
            _ => throw(Exact(format!("Expression must be a boolean: {}", expr.to_code())))
        }
    }

    fn exec_coalesce_error(
        &self,
        attempt: &Expression,
        substitute: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        match self.evaluate(attempt) {
            Ok((_, ErrorValue(..))) | Err(_) => self.evaluate(substitute),
            Ok((ms, value)) => Ok((ms, value)),
        }
    }

    /// Executes a method call or field assignment
    /// #### Examples
    /// ```
    /// str::left("hello", 5)
    /// ```
    /// ```
    /// stock::is_this_you('ABC')
    /// ```
    /// ```
    /// stock::last_sale = 24.11
    /// ```
    fn exec_colon_colon(
        &self,
        container: &Expression,
        field: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        // is this an instantiation?
        let ms = self.clone();
        match field {
            // ex: Error::new("Process failed")
            FunctionCall { fx, args } if pull_identifier_name(fx)? == "new" => {
                // `Error::new("Process failed")` becomes `Error("Process failed")`
                let data_type = DataType::decipher_type(&FunctionCall {
                    fx: container.clone().into(),
                    args: args.to_vec()
                })?;
                let inst = data_type.get_default_value();
                Ok((ms, inst))
            }
            // must be a platform function. ex: nsd::replay(stocks)
            _ => {
                let container_name = pull_identifier_name(container)?;
                match ms.get(container_name.as_str()) {
                    Some(Structured(structure)) => 
                        self.exec_colon_colon_structure(&container_name, Box::from(structure), field),
                    Some(TableValue(df)) =>
                        self.exec_colon_colon_table_tools(&container_name, df, field),
                    // error cases
                    None => throw(Exact(format!("Variable '{}' was not found", container_name))),
                    Some(ErrorValue(err)) => throw(err),
                    Some(x) if matches!(x, Null | Undefined) => 
                        throw(Exact(format!("Cannot evaluate {}::{}", x, field.to_code()))),
                    Some(z) => throw(Exact(format!("Illegal structure {}", z.to_code())))
                }
            }
        }
    }

    /// Enables support for structure method calls and field assignments
    /// #### Examples
    /// ```
    /// math::compute(5, 8)
    /// ```
    /// ```
    /// stock::last_sale = 24.11
    /// ```
    fn exec_colon_colon_structure(
        &self,
        container_name: &str,
        structure: Box<dyn Structure>,
        field: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let ms = self.clone();
        match field {
            // math::compute(5, 8)
            FunctionCall { fx, args } => structure.pollute(ms).exec_function_call(fx, args),
            // stock::last_sale = 24.11
            SetVariables(var_expr, value_expr) => {
                match var_expr.deref() {
                    Identifier(name) => {
                        let (ms, value) = ms.evaluate(value_expr)?;
                        Ok((ms.with_variable(container_name, Structured(structure.update_by_name(name, value))), Boolean(true)))
                    }
                    z => throw(Exact(format!("Illegal field '{}' for structure {}",
                                             z.to_code(), container_name)))
                }
            }
            // stock::symbol
            Identifier(name) => Ok((ms, structure.get(name))),
            z => throw(Exact(format!("Illegal field '{}' for structure {}",
                                     z.to_code(), container_name)))
        }
    }

    /// Enables integrated support for the "tools" package for tables
    /// #### Examples
    /// ```
    /// stocks::reverse()
    /// ```
    fn exec_colon_colon_table_tools(
        &self,
        container_name: &str,
        df: Dataframe,
        field: &Expression
    ) -> std::io::Result<(Self, TypedValue)> {
        match field {
            // stocks::describe()
            FunctionCall { fx, args } => {
                match fx.deref() {
                    Identifier(name) => {
                        let (ms, args) = self.evaluate_as_vec(args)?;
                        let mut f_args = vec![TableValue(df)];
                        f_args.extend(args);
                        ToolsPkg::invoke(name, ms, f_args)
                    }
                    z => throw(Exact(format!("Illegal function '{}' for table {}", z.to_code(), container_name)))
                }
            }
            z => throw(Exact(format!("Illegal field '{}' for table {}", z.to_code(), container_name)))
        }
    }

    /// Executes a postfix function call
    /// #### Examples
    /// ```
    /// "hello":::left(5) //=> left("hello", 5)
    /// ```
    /// ```
    /// 3.2:::kb //=> kb(3.2)
    /// ```
    fn exec_colon_colon_colon(
        &self,
        object: &Expression,
        field: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        match field {
            // "hello":::left(5) //=> left("hello", 5)
            FunctionCall { fx, args } => {
                let mut enriched_args = Vec::new();
                enriched_args.push(object.to_owned());
                enriched_args.extend(args.to_owned());
                self.exec_function_call(fx, &enriched_args)
            }
            // 3.2:::kb //=> kb(3.2)
            Identifier(..) => {
                self.exec_function_call(field, &vec![
                    object.to_owned()
                ])
            }
            // unsupported
            z => throw(Exact(format!("{} is not a function call", z.to_code())))
        }
    }

    /// Returns [True] if `a` contains `b`
    fn exec_contains(
        &self,
        a: &Expression,
        b: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, a) = self.evaluate(a)?;
        let (machine, b) = machine.evaluate(b)?;
        Ok((machine, Boolean(a.contains(&b))))
    }

    /// Performs destructure of an [ArrayExpression], [Identifier]
    /// or [TupleExpression] for variable assignment.
    /// #### Parameters
    /// - identifiers: the identifier(s) for which to deconstruct
    /// - values: the value(s) for which to assign to the deconstructed variable(s)
    /// #### Returns
    /// - a [Machine] populated with the deconstructed key-value pairs
    /// #### Examples
    /// ```
    /// let [a, b, c, d] = [1, 5, 6, 11]
    /// ```
    /// ```
    /// let (x, y) = (1, 5)
    /// ```
    /// ##### Future
    /// ```
    /// let { symbol: _, exchange: _, last_sale: _ } =
    ///     {"symbol":"ABC","exchange":"NYSE","last_sale":56.11}
    /// ```
    fn exec_destructure(
        &self,
        identifiers: &Expression,
        values: &TypedValue,
    ) -> std::io::Result<Machine> {
        match identifiers {
            ArrayExpression(elems) | TupleExpression(elems) => {
                let mut ms = self.clone();
                let values = pull_vec(&values)?;
                for (elem, value) in elems.iter().zip(values.iter()) {
                    ms = ms.exec_destructure(elem, value)?;
                }
                Ok(ms)
            }
            Identifier(name) => Ok(self.with_variable(name.as_str(), values.clone())),
            z => throw(Exact(format!("{} could not be deconstructed", z.to_code())))
        }
    }

    /// Evaluates the `do..while` declaration
    fn exec_do_while(
        &self,
        condition: &Expression,
        code: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        // evaluate the code
        let (mut ms, mut result) = self.evaluate(code)?;

        // while the condition is true, execute the code
        let mut is_looping = true;
        while is_looping {
            let (ms1, result1) = ms.evaluate(condition)?;
            is_looping = result1.is_true();
            if is_looping {
                let (ms2, result2) = ms1.evaluate(code)?;
                ms = ms2;
                result = result2;
            }
        }
        Ok((ms, result))
    }

    /// Evaluates a for statement
    /// #### Parameters
    /// - construct: the expression that represents the for loop construct
    /// - block: the block of code to execute for each item
    /// #### Returns
    /// - a [Machine] populated with the deconstructed key-value pairs
    /// - the result of the last statement in the block
    /// #### Examples
    /// ```
    /// for(i = 0, i < 5, i = i + 1) ...
    /// ```
    /// ```
    /// for [x, y, z] in [[1, 5, 3], [6, 11, 17], ...] ...
    /// ```
    /// ```
    /// for (c, n) in [('a', 5), ('c', 11), ...] ...
    /// ```
    /// ```
    /// for item in ['apple', 'berry', ...] ...
    /// ```
    /// ```
    /// for row in tools::to_table([{symbol:'ABC', price: 10.0}, ...]) ...
    /// ```
    fn exec_for_construct(&self,
                          construct: &Box<Expression>,
                          block: &Box<Expression>,
    ) -> std::io::Result<(Self, TypedValue)> {
        match construct.deref() {
            // for item in ['apple', 'berry', ...] ...
            Condition(In(item, items)) => self.exec_for_each(item, items, block),
            // for(i = 0, i < 5, i = i + 1) ...
            TupleExpression(components) => {
                match components.as_slice() {
                    [init, Condition(cond), counter] => {
                        self.exec_for_loop(init, cond, counter, block)
                    },
                    _ => throw(Exact(format!("Invalid for loop construct: {}", construct.to_code())))
                }
            }
            _ => throw(Exact(format!("Invalid for loop construct: {}", construct.to_code())))
        }
    }

    fn exec_for_each(
        &self,
        item: &Expression,
        items: &Expression,
        block: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let mut ms = self.clone();
        let (_, array_value) = ms.evaluate(items)?;
        let array = pull_array(&array_value.to_array())?;
        let mut index = 0;
        let mut result = Undefined;
        while index < array.len() {
            // get the value at `index`
            let value = array.get_or_else(index, Undefined);
            // deconstruct the value (e.g. (a, b) ~> a = ?, b = ?)
            ms = ms.exec_destructure(item, &value)?;
            // evaluate the block - capture the results
            let (ms1, result1) = ms.evaluate(block)?;
            ms = ms1;
            result = result1;
            // advance the `index`
            index += 1
        }
        Ok((ms, result))
    }

    fn exec_for_loop(
        &self,
        init: &Expression,
        cond: &Conditions,
        counter: &Expression,
        block: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        // evaluate the init expression
        let (mut ms, mut result) = self.evaluate(init)?;

        // while the condition is true, evaluate the block
        while let (ms1, true) = ms.is_true(cond)? {
            let (ms2, result1) = ms1.evaluate(block)?;
            result = result1;

            // increment the loop counter
            let (ms3, _) = ms2.evaluate(counter)?;
            ms = ms3;
        }
        Ok((ms, result))
    }

    /// Evaluates a function pipeline
    /// #### Examples
    /// ```
    /// "Hello" |> util::md5 |> util::hex
    /// ```
    /// ```
    /// // arrays, tuples and structures can be deconstructed into arguments
    /// 
    /// fn add(a, b) => a + b
    /// fn inverse(a) => 1.0 / a
    /// (2, 3) |>> add |> inverse
    /// ```
    fn exec_function_pipeline(
        &self,
        expr: &Expression,
        operation: &Expression,
        is_destructure: bool,
    ) -> std::io::Result<(Self, TypedValue)> {
        let args = match is_destructure {
            true => match expr {
                ArrayExpression(args) => args.clone(),
                StructureExpression(items) =>
                    items.iter().map(|(_, v)| v.clone()).collect(),
                TupleExpression(args) => args.clone(),
                item => vec![item.clone()]
            },
            false => vec![expr.clone()]
        };
        self.evaluate(&FunctionCall {
            fx: Box::new(operation.clone()),
            args,
        })
    }

    fn exec_function_arguments(
        &self,
        params: Vec<Parameter>,
        args: Vec<TypedValue>,
    ) -> Self {
        if params.len() > args.len() {
            println!("Argument mismatch (params: {}, args: {:?})", Parameter::render(&params), args);
            assert_eq!(params.len(), args.len());
        }
        params.iter().zip(args.iter())
            .fold(self.to_owned(), |ms, (c, v)|
                ms.with_variable(c.get_name(), v.to_owned()))
    }

    /// Executes a function call
    /// #### Examples
    /// ```
    /// factorial(5)
    /// ```
    fn exec_function_call(&self,
                          fx: &Expression,
                          args: &Vec<Expression>,
    ) -> std::io::Result<(Self, TypedValue)> {
        match self.evaluate_as_array(args)? {
            (ms, ArrayValue(args)) =>
                match ms.evaluate(fx)? {
                    (ms, Function { params, body, .. }) =>
                        ms.exec_function_arguments(params, args.get_values())
                            .evaluate(&body),
                    (ms, PlatformOp(pf)) => pf.evaluate(ms, args.get_values()),
                    (_, z) => throw(TypeMismatch(FunctionExpected(z.to_code()))),
                }
            (_, other) => throw(TypeMismatch(FunctionArgsExpected(other.to_code()))),
        }
    }

    fn exec_get_variable(
        &self,
        name: &str,
    ) -> std::io::Result<(Self, TypedValue)> {
        let result = match self.get(name) {
            Some(value) => value,
            None if DataType::is_type_name(name) =>
                Kind(DataType::decipher_type(&Identifier(name.into()))?),
            None => return throw(Exact(format!("Variable '{}' not found", name))),
        };
        Ok((self.clone(), result))
    }

    fn exec_http(&self, call: &HttpMethodCalls) -> std::io::Result<(Self, TypedValue)> {
        // fn create_form(structure: Box<dyn Structure>) -> Form {
        //     structure.to_name_values().iter().fold(Form::new(), |form, (name, value)| {
        //         form.part(name.to_owned(), Part::text(value.unwrap_value()))
        //     })
        // }

        fn extract_string_tuples(value: TypedValue) -> std::io::Result<Vec<(String, String)>> {
            extract_value_tuples(value)
                .map(|values| values.iter()
                    .map(|(k, v)| (k.to_string(), v.unwrap_value()))
                    .collect())
        }

        fn extract_value_tuples(value: TypedValue) -> std::io::Result<Vec<(String, TypedValue)>> {
            match value {
                Structured(structure) => Ok(structure.to_name_values()),
                z => throw(TypeMismatch(UnsupportedType(StructureType(vec![]), z.get_type()))),
            }
        }

        // evaluate the URL or configuration object
        match self.evaluate(&call.get_url_or_config())? {
            // GET http://localhost:9000/quotes/AAPL/NYSE
            (ms, StringValue(url)) =>
                ms.exec_http_request(call, url.to_string(), None, Vec::new()),
            // POST {
            //     url: http://localhost:8080/machine/append/stocks
            //     body: stocks
            //     headers: { "Content-Type": "application/json" }
            // }
            (ms, Structured(config)) => {
                let url = config.get("url");
                let maybe_body = config.get_opt("body")
                    .map(|body| body.unwrap_value());
                let headers = match config.get_opt("headers") {
                    None => Vec::new(),
                    Some(headers) => extract_string_tuples(headers)?
                };
                ms.exec_http_request(call, url.unwrap_value(), maybe_body, headers)
            }
            // unsupported expression
            (_ms, other) =>
                throw(TypeMismatch(StructExpected(other.to_code(), other.to_code())))
        }
    }

    fn exec_http_request(
        &self,
        method_call: &HttpMethodCalls,
        url: String,
        body: Option<String>,
        headers: Vec<(String, String)>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let mut builder = match method_call {
            HttpMethodCalls::CONNECT(..) => Request::connect(url),
            HttpMethodCalls::DELETE(..) => Request::delete(url),
            HttpMethodCalls::GET(..) => Request::get(url),
            HttpMethodCalls::HEAD(..) => Request::head(url),
            HttpMethodCalls::OPTIONS(..) => Request::options(url),
            HttpMethodCalls::PATCH(..) => Request::patch(url),
            HttpMethodCalls::POST(..) => Request::post(url),
            HttpMethodCalls::PUT(..) => Request::put(url),
            HttpMethodCalls::TRACE(..) => Request::trace(url),
        };

        // enrich and submit the request
        for (key, value) in headers {
            builder = builder.header(&key, &value);
        }
        let response = if let Some(body) = body {
            builder = builder.header("Content-Type", "application/json");
            builder
                .body(body)
                .map_err(|e| cnv_error!(e))?
                .send()
                .map_err(|e| cnv_error!(e))?
        } else {
            builder = builder.header("Content-Type", "application/json");
            builder
                .body(())
                .map_err(|e| cnv_error!(e))?
                .send()
                .map_err(|e| cnv_error!(e))?
        };
        self.exec_http_response(response, method_call.is_header_only())
            .map(|result| (self.clone(), result))
    }

    /// Converts a [Response] to a [TypedValue]
    fn exec_http_response(
        &self,
        mut response: Response<Body>,
        is_header_only: bool,
    ) -> std::io::Result<TypedValue> {
        if response.status().is_success() {
            if is_header_only {
                let mut key_values = vec![];
                for (h_key, h_val) in response.headers().iter() {
                    let value = match h_val.to_str() {
                        Ok(s) => StringValue(s.into()),
                        Err(e) => {
                            eprintln!("exec_http_response: {}", e.to_string());
                            ErrorValue(Exact(e.to_string()))
                        }
                    };
                    key_values.push((h_key.to_string(), value))
                }
                Ok(Structured(Soft(SoftStructure::ordered(key_values))))
            } else {
                let body = response.text().map_err(|e| cnv_error!(e))?;
                match Compiler::build(body.as_str()) {
                    Ok(expr) => {
                        Ok(match self.evaluate(&expr) {
                            Ok((_, Undefined)) => Structured(Soft(SoftStructure::empty())),
                            Ok((_, value)) => value,
                            Err(_) => StringValue(body)
                        })
                    }
                    _ => Ok(StringValue(body))
                }
            }
        } else {
            throw(Exact(format!("Request failed with status: {}", response.status())))
        }
    }

    /// Evaluates an if-then-else expression
    fn exec_if_then_else(
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

    /// Evaluates an `in` expression
    /// #### Examples
    /// ##### arrays
    /// ```
    /// "ABC" in ["123", "XYZ", "ABC", "TZZ"]
    /// ```
    /// ##### dataframes
    /// ```
    /// "ABC" in (select symbol from portfolio where exchange == "NYSE")
    /// ```
    /// ##### ranges
    /// ```
    /// n in 0..100
    /// ```
    /// ##### tuples
    /// ```
    /// "apple" in ("apple", "banana", "blueberry", "mango")
    /// ```
    fn exec_in(
        &self,
        item: &Expression,
        container: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, item_val) = self.evaluate(item)?;
        let (ms, container_val) = ms.evaluate(container)?;
        let result = match container_val.to_sequence()? {
            TheArray(array) => array.contains(&item_val),
            TheDataframe(df) => {
                match item_val {
                    Structured(s) => {
                        df.contains(&Structures::transform_row(
                            &s.get_parameters(),
                            &s.get_values(),
                            &df.get_parameters()
                        ))
                    },
                    _ => false
                }
            },
            TheRange(a, b, incl) => is_in_range(&item_val, &a, &b, incl),
            TheTuple(values) => values.contains(&item_val),
        };
        Ok((ms, Boolean(result)))
    }

    fn exec_include(
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
            throw(TypeMismatch(UnsupportedType(StringType, path_value.get_type())))
        }
    }

    fn exec_infix(
        &self,
        container: &Expression,
        field: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        match field {
            // function call: stocks.lookup("TALLY")
            FunctionCall { .. } => 
                match self.evaluate(container)? {
                    (ms, Structured(structure)) => {
                       let ms1 = structure.pollute(ms.clone());
                        ms1.evaluate(field).map(|(_ms, result)| (ms, result))
                    }
                    (_, z) => throw(Exact(format!("Expected a structure near {}", z)))
                }
            // field assignment: stock.last_sale = 24.11
            SetVariables(var_expr, value_expr) => 
                match self.evaluate(container)? {
                    (ms, Structured(structure)) =>
                        match var_expr.deref() {
                            Identifier(name) => {
                                let (ms, value) = ms.evaluate(value_expr)?;
                                Ok((ms.with_variable(
                                    &pull_identifier_name(container)?,
                                    Structured(structure.update_by_name(name, value))), Boolean(true)))
                            }
                            z => throw(Exact(format!("Illegal field '{}' for structure {}", z, container)))
                        }
                    (_, z) => throw(Exact(format!("Expected a structure near {}", z)))
                }
            // field access: stock.last_sale
            Identifier(field_name) => 
                match self.evaluate(container)? {
                    (ms, Structured(st)) =>
                        match st.get_opt(field_name.as_str()) {
                            None => Ok((ms, Undefined)),
                            Some(value) => Ok((ms, value)),
                        }
                    (_, z) => throw(Exact(format!("Expected a structure near {}", z)))
                }
            z => throw(Exact(format!("Illegal field '{}' for {}", z, container)))
        }
    }
    
    fn exec_is_defined(
        &self,
        expr: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        match expr { 
            Identifier(name) => Ok((self.to_owned(), Boolean(self.get(name).is_some()))),
            z => throw(TypeMismatch(IdentifierExpected(z.to_code())))
        }
    }

    /// Evaluates the index of a collection (array, string or table)
    fn exec_index_of_collection(
        &self,
        collection_expr: &Expression,
        index_expr: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, index) = self.evaluate(index_expr)?;
        let (machine, collection) = machine.evaluate(collection_expr)?;
        let value = match collection {
            ArrayValue(items) => {
                let idx = index.to_usize();
                if idx < items.len() { items.get_or_else(idx, Undefined) } else {
                    ErrorValue(IndexOutOfRange("Array".to_string(), idx, items.len()))
                }
            }
            StringValue(string) => {
                let idx = index.to_usize();
                if idx < string.len() { StringValue(string[idx..idx].to_string()) } else { ErrorValue(IndexOutOfRange("String".to_string(), idx, string.len())) }
            }
            Structured(structure) => {
                let idx = index.to_usize();
                let values = structure.get_values();
                if idx < values.len() { values[idx].to_owned() } else { ErrorValue(IndexOutOfRange("Structure element".to_string(), idx, values.len())) }
            }
            TableValue(df) => {
                let id = index.to_usize();
                match df.read_one(id)? {
                    Some(row) => Structured(Firm(row, df.get_parameters())),
                    None => Structured(Firm(Row::create(id, df.get_columns()), df.get_parameters()))
                }
            }
            TupleValue(values) => {
                let idx = index.to_usize();
                if idx < values.len() { values[idx].to_owned() } else { ErrorValue(IndexOutOfRange("Tuple element".to_string(), idx, values.len())) }
            }
            other =>
                ErrorValue(TypeMismatch(UnsupportedType(
                    UnresolvedType,
                    other.get_type(),
                )))
        };
        Ok((machine, value))
    }

    fn exec_like(
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
                Ok((ms, ErrorValue(SyntaxError(SyntaxErrors::TypeIdentifierExpected(format!("{} like {}", a.to_code(), b.to_code()))))))
        }
    }

    /// Evaluates a match expression
    /// #### Examples
    /// ```
    /// match code {
    ///     100 => "Accepted"
    ///     n when n in 101..104 => "Escalated"
    ///     n when n > 0 && n < 100 => "Pending"
    ///     _ => "Rejected"
    /// }
    /// ```
    fn exec_match(
        &self,
        host: &Expression,
        cases: &Vec<Expression>,
    ) -> std::io::Result<(Self, TypedValue)> {
        // find a matching case
        let (ms, host_value) = self.evaluate(host)?;
        for case in cases {
            match case {
                ArrowFat(cond_expr, op_expr) =>
                    match cond_expr.deref() {
                        // condition case: n when n < 100 => "Accepted"
                        Condition(When(given_expr, given_cond)) => {
                            let name = pull_identifier_name(given_expr)?;
                            let ms = ms.with_variable(name.as_str(), host_value.clone());
                            if let (ms, Boolean(true)) = ms.exec_assume_boolean(given_cond)? {
                                return ms.evaluate(op_expr);
                            }
                        }
                        // literal case: 100 => "Accepted"
                        Literal(lit_value) => {
                            if *lit_value == host_value {
                                return ms.evaluate(op_expr);
                            }
                        }
                        // variable case: n => "Rejected"
                        Identifier(name) => return ms.with_variable(name, host_value.clone()).evaluate(&op_expr),
                        // unsupported cases ...
                        z => return throw(SyntaxError(SyntaxErrors::IllegalExpression(z.to_code())))
                    }
                z => return throw(Exact(format!("Expected a case expression near {}", z.to_code())))
            }
        }
        throw(Exact("Failed to find a matching case".into()))
    }

    fn exec_module_alone(
        &self,
        name: &str,
        ops: &Vec<Expression>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let structure = HardStructure::empty();
        let result =
            ops.iter().fold(Structured(Hard(structure)), |tv, op| match tv {
                ErrorValue(msg) => ErrorValue(msg),
                Structured(Hard(structure)) =>
                    match op {
                        SetVariables(var_expr, value_expr) =>
                            match var_expr.deref() {
                                Identifier(name) =>
                                    match self.evaluate(value_expr) {
                                        Ok((_, value)) => Structured(Hard(structure.with_variable(name, value))),
                                        Err(err) => ErrorValue(Exact(err.to_string()))
                                    }
                                z => ErrorValue(SyntaxError(SyntaxErrors::IllegalExpression(z.to_code())))
                            }
                        z => ErrorValue(SyntaxError(SyntaxErrors::IllegalExpression(z.to_code())))
                    },
                Structured(Soft(structure)) =>
                    match op {
                        SetVariables(var_expr, expr) =>
                            match var_expr.deref() {
                                Identifier(name) =>
                                    match self.evaluate(expr) {
                                        Ok((_, value)) => Structured(Soft(structure.with_variable(name, value))),
                                        Err(err) => ErrorValue(Exact(err.to_string()))
                                    }
                                z => ErrorValue(SyntaxError(SyntaxErrors::IllegalExpression(z.to_code())))
                            }
                        z => ErrorValue(SyntaxError(SyntaxErrors::IllegalExpression(z.to_code())))
                    },
                z => ErrorValue(TypeMismatch(StructExpected(name.to_string(), z.to_code())))
            });
        Ok((self.with_variable(name, result), Boolean(true)))
    }

    /// evaluates the specified [Expression]; returning a negative [TypedValue] result.
    fn exec_negate(&self, expr: &Expression) -> std::io::Result<(Self, TypedValue)> {
        self.evaluate(expr).map(|(ms, result)| (ms, result.neg()))
    }

    /// ++ operator
    /// ex: checks++
    fn exec_plus_plus(
        a: TypedValue,
        b: TypedValue,
    ) -> TypedValue {
        match (a, b) {
            (ArrayValue(a), ArrayValue(b)) => {
                let mut c = a.clone();
                c.push_all(b.get_values().clone());
                ArrayValue(c)
            }
            (a, b) =>
                ErrorValue(SyntaxError(SyntaxErrors::TypeIdentifierExpected(format!("{a} ++ {b}"))))
        }
    }
    
    /// Returns a reference to an embedded table
    /// ```
    /// // create a table with an embedded table
    /// let stocks = nsd::save(
    ///     "machine.examples.stocks",
    ///     Table::new(symbol: String(8), exchange: String(8), history: Table(last_sale: f64, processed_time: Date))
    /// )
    /// { symbol: "BIZ", exchange: "NYSE", history: [] } ~> stocks
    /// 
    /// // get a reference to the embedded table
    /// let history = &stocks(0, 2)
    /// { last_sale: 11.67, processed_time: cal::now() } ~> history
    /// stocks
    /// 
    /// // { symbol: "BIZ", exchange: "NYSE", history: [{ last_sale: 11.67, processed_time: 2025-01-13T03:25:47.350Z }] }
    /// ```
    fn exec_referenced(
        &self,
        expr: &Expression
    ) -> std::io::Result<(Self, TypedValue)> {
        match expr {
            FunctionCall { fx, args } => {
                let (ms, table_val) = self.evaluate(fx)?;
                match table_val.to_table_or_value() {
                    TableValue(df) => {
                        let (ms, coords) = ms.get_referenced_coordinates(args)?;
                        let ref_df = ms.get_referenced_dataframe(&df, coords)?;
                        Ok((ms, TableValue(ref_df)))
                    }
                    x => throw(TypeMismatch(TableExpected(x.to_code())))
                }
            }
            x => throw(TypeMismatch(FunctionExpected(x.to_code())))
        }
    }

    fn get_referenced_coordinates(
        &self, 
        args: &Vec<Expression>
    ) -> std::io::Result<(Self, Vec<usize>)> {
        let (_, coords_tuple) = self.evaluate(&TupleExpression(args.clone()))?;
        match coords_tuple {
            TupleValue(items) => {
                let mut coords = vec![];
                for item in items {
                    coords.push(pull_number(&item).map(|n| n.to_usize())?);
                }
                Ok((self.to_owned(), coords))
            }
            x => throw(TypeMismatch(TupleExpected(x.to_code())))
        }
    }

    fn get_referenced_dataframe(
        &self,
        host: &Dataframe,
        coords: Vec<usize>
    ) -> std::io::Result<Dataframe> {
        
        fn next_id(mut coords: Vec<usize>) -> std::io::Result<(usize, Vec<usize>)> {
            match coords.pop() {
                None => throw(Exact("Table not found".into())),
                Some(id) => Ok((id, coords))
            }
        }

        fn recurse_df(
            df: &Dataframe,
            coords: Vec<usize>,
        ) -> std::io::Result<Dataframe> {
            if coords.is_empty() { Ok(df.to_owned()) }
            else {
                let (row_id, coords) = next_id(coords)?;
                match df.read_one(row_id)? {
                    None => throw(Exact("Table not found".into())),
                    Some(row) => recurse_row(row, coords)
                }
            }
        }
        
        fn recurse_row(
            row: Row,
            coords: Vec<usize>,
        ) -> std::io::Result<Dataframe> {
            let (column_id, coords) = next_id(coords)?;
            match row.get(column_id).to_table_or_value() {
                TableValue(df) => recurse_df(&df, coords),
                other => throw(TypeMismatch(TableExpected(other.to_code())))
            }
        }

        // recursively resolve the embedded dataframe
        recurse_df(host, coords.iter().rev().map(|n| *n).collect::<Vec<_>>()) 
    }

    /// Sets variable(s) in the current [Machine] instance
    /// #### Returns
    /// - `true` if the operation was successful
    /// - `false` if the operation failed
    /// #### Examples
    /// ```
    /// let n = 100
    /// ```
    /// ```
    /// let (x, y, z) = (3, 6, 9)
    /// ```
    /// ```
    /// let [a, b, c, d] = [1, 3, 5, 7]
    /// ```
    fn exec_set_variables(
        &self,
        src: &Expression,
        dest: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        self.exec_set_variables_expr(src, dest).map(|(ms, _)| (ms, Boolean(true)))        
    }

    /// Sets variable(s) in the current [Machine] instance 
    /// #### Returns
    /// - the assigned value
    /// - `undefined` if the operation failed
    /// #### Examples
    /// ```
    /// let n := 100
    /// ```
    /// ```
    /// let (x, y, z) := (3, 6, 9)
    /// ```
    /// ```
    /// let [a, b, c, d] := [1, 3, 5, 7]
    /// ```
    fn exec_set_variables_expr(
        &self,
        src: &Expression,
        dest: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, result) = self.evaluate(dest)?;
        match src.clone() {
            // [a, b, c, d] := [1, 2, 3, 4]
            ArrayExpression(variables) =>
                match result {
                    ArrayValue(array) => {
                        let (ms1, result1) = ms.set_variables(
                            Self::get_variables_names(&variables)?,
                            array.get_values(),
                            |v| ArrayValue(Array::from(v))
                        )?;
                        let (ms2, _) = ms1.exec_whenever_observations()?;
                        Ok((ms2, result1))
                    }
                    z => throw(Exact(format!("Expected an Array near {}", z)))
                }
            // stock::last_sale = 24.11
            ColonColon(container, variable) => {
                let container_name = pull_identifier_name(container.deref())?;
                let variable_name = pull_identifier_name(variable.deref())?;
                match ms.get(container_name.as_str()) {
                    None => throw(Exact(format!("Expected a variable near {}", container.to_code()))),
                    Some(Structured(Hard(hs))) => {
                        let hs = hs.with_variable(&variable_name, result.clone());
                        Ok((ms.with_variable(&container_name, Structured(Hard(hs))), result))
                    }
                    Some(Structured(Soft(ss))) => {
                        let ss = ss.with_variable(&variable_name, result.clone());
                        Ok((ms.with_variable(&container_name, Structured(Soft(ss))), result))
                    }
                    Some(z) => throw(Exact(format!("Expected a structure near {}", z)))
                }
            }
            // (a, b, c) := (3, 6, 9)
            TupleExpression(variables) =>
                match result {
                    TupleValue(values) => {
                        let (ms1, result1) = ms.set_variables(
                            Self::get_variables_names(&variables)?,
                            values,
                            TupleValue
                        )?;
                        let (ms2, _) = ms1.exec_whenever_observations()?;
                        Ok((ms2, result1))
                    }
                    z => throw(Exact(format!("Expected a Tuple near {}", z)))
                }
            // a := 7
            Identifier(name) => {
                let ms1 = ms.set(name.as_str(), result.clone());
                let (ms2, _) = ms1.exec_whenever_observations()?;
                Ok((ms2, result))
            }
            z => throw(Exact(format!("Expected an Array, Tuple or variable near {}", z)))
        }
    }
    
    fn exec_structure_module(
        &self,
        name: &str,
        ops: &Vec<Expression>,
    ) -> std::io::Result<(Self, TypedValue)> {
        match self.get(name) {
            Some(Structured(structure)) => self.exec_structure_module_impl(name, structure, ops),
            Some(v) => throw(TypeMismatch(StructExpected(name.into(), v.to_code()))),
            None => self.exec_module_alone(name, ops)
        }
    }

    fn exec_structure_module_impl(
        &self,
        name: &str,
        structure: Structures,
        ops: &Vec<Expression>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let result = ops.iter()
            .fold(Structured(structure), |tv, op| match tv {
                ErrorValue(msg) => ErrorValue(msg),
                Structured(structure) =>
                    match op {
                        // name = "Hello World"
                        SetVariables(var_expr, value_expr) =>
                            match var_expr.deref() {
                                Identifier(name) =>
                                    match self.evaluate(value_expr) {
                                        Ok((_, value)) => Structured(structure.update_by_name(name, value)),
                                        Err(err) => ErrorValue(Exact(err.to_string()))
                                    }
                                other => ErrorValue(Exact(format!("Decomposition is not allowed near {}", other.to_code())))
                            }
                        z => ErrorValue(SyntaxError(SyntaxErrors::IllegalExpression(z.to_code())))
                    },
                z => ErrorValue(TypeMismatch(StructExpected(name.to_string(), z.to_code())))
            });
        match result {
            ErrorValue(err) => throw(err),
            result => Ok((self.with_variable(name, result), Boolean(true)))
        }
    }

    fn exec_structure_soft(
        &self,
        items: &Vec<(String, Expression)>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let mut elems = Vec::new();
        for (name, expr) in items {
            let (_, value) = self.evaluate(expr)?;
            elems.push((name.to_string(), value))
        }
        Ok((self.to_owned(), Structured(Soft(SoftStructure::from_tuples(elems)))))
    }
    
    fn exec_throw(
        &self,
        expr: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, message) = self.evaluate(expr)?;
        Ok((ms, ErrorValue(Exact(message.unwrap_value()))))
    }
    
    /// Defines a tuple expression
    /// ex: (1, 2, 3)
    fn exec_tuple(
        &self,
        ops: &Vec<Expression>,
    ) -> std::io::Result<(Self, TypedValue)> {
        self.evaluate_as_vec(ops).map(|(ms, values)| (ms, TupleValue(values)))
    }

    /// Declares a type definition expression
    /// #### Example
    /// ```
    /// stock_type = typedef(Table(symbol: String(8), exchange: String(8), last_sale: f64)
    /// ```
    fn exec_type_def(
        &self,
        expr: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        Ok((self.clone(), Kind(DataType::decipher_type(expr)?)))
    }

    /// Returns the type of value
    /// #### Example
    /// ```
    /// type_of stock_type
    /// ```
    fn exec_type_of(
        &self,
        expr: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, value) = self.evaluate(expr)?;
        Ok((ms, Kind(value.get_type())))
    }

    /// Produces an aggregate [Machine] instance containing
    /// the specified imports
    fn exec_use(
        &self,
        package_name: &str,
        selection: &Vec<String>,
    ) -> (Self, TypedValue) {
        let ms = self.to_owned();
        match ms.get(package_name) {
            None => (ms, ErrorValue(PackageNotFound(package_name.into()))),
            Some(component) => match component {
                Structured(structure) =>
                    if selection.is_empty() {
                        (structure.pollute(ms), Boolean(true))
                    } else {
                        let ms = selection.iter().fold(ms, |ms, name| {
                            ms.with_variable(name, structure.get(name))
                        });
                        (ms, Boolean(true))
                    }
                other => (ms, ErrorValue(SyntaxError(SyntaxErrors::TypeIdentifierExpected(other.to_code()))))
            }
        }
    }

    fn exec_uses(&self, ops: &Vec<UseOps>) -> std::io::Result<(Self, TypedValue)> {
        let result = ops.iter().fold(
            (self.to_owned(), Undefined),
            |(ms, tv), iop| match iop {
                UseOps::Everything(pkg) =>
                    ms.exec_use(pkg, &Vec::new()),
                UseOps::Selection(pkg, selection) =>
                    ms.exec_use(pkg, selection),
            });
        Ok(result)
    }

    /// Evaluates the `yield` declaration
    fn exec_yield(
        &self,
        expr: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        const LABEL: &str = "__yield__";
        let full_prop_name = format!("{}{}", LABEL, hex::encode(expr.encode()));
        let prop_name = full_prop_name.as_str();
        let (mut ms, value) = self.evaluate(expr)?;
        let array = match ms.get(prop_name) {
            Some(ArrayValue(mut array)) => {
                array.push(value);
                ms = ms.set(prop_name, ArrayValue(array.clone()));
                array
            },
            _ => {
                let array = Array::from(vec![value]);
                ms = ms.with_variable(prop_name, ArrayValue(array.clone()));
                array
            }
        };
        Ok((ms, ArrayValue(array)))
    }

    fn exec_zip(
        &self,
        a: &Expression,
        b: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, aa) = self.evaluate(a)?;
        let (ms, bb) = ms.evaluate(b)?;
        match (aa, bb) {
            (ArrayValue(aa), ArrayValue(bb)) => {
                let items = aa.iter().zip(bb.iter()).map(|(i, j)| {
                    TupleValue(vec![i.to_owned(), j.to_owned()])
                }).collect();
                Ok((ms, ArrayValue(Array::from(items))))
            }
            (x, y) => throw(Exact(format!("Incompatible types: {} and {}", x.get_type(), y.get_type())))
        }
    }
    
    /// Evaluates the `when` declaration
    fn exec_when_statement(
        &self,
        condition: &Expression,
        code: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let cond = lift_condition(condition)?;
        let ms = self.with_observer_variable(cond, code.clone());
        Ok((ms, Boolean(true)))
    }

    /// Evaluates the `whenever` observations (triggers) 
    fn exec_whenever_observations(&self) -> std::io::Result<(Self, TypedValue)> {
        let mut ms = self.clone(); 
        for observation in &self.observations {
            match observation {
                VariableObservation { condition, code } => {
                    match ms.is_true(condition).map(|(_, yes)| yes) {
                        Ok(true) => {
                            match ms.evaluate(code) {
                                Ok((ms1, _result)) => {
                                    ms = ms1;
                                }
                                Err(_) => {}
                            }
                        }
                        Ok(_) => {}
                        Err(_) => {}
                    }
                }
            }
        }
        Ok((ms, Undefined))       
    }

    /// Evaluates the `while` declaration
    fn exec_while(
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

    /// evaluates expression `a` then applies function `f`. ex: f(a)
    fn eval_inline_1(
        &self,
        a: &Expression,
        f: fn(TypedValue) -> TypedValue,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, aa) = self.evaluate(a)?;
        Ok((machine, f(aa)))
    }

    /// evaluates expressions `a` and `b` then applies function `f`. ex: f(a, b)
    fn eval_inline_2(
        &self,
        a: &Expression,
        b: &Expression,
        f: fn(TypedValue, TypedValue) -> TypedValue,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, aa) = self.evaluate(a)?;
        let (machine, bb) = machine.evaluate(b)?;
        Ok((machine, f(aa, bb)))
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

    fn get_variables_names(items: &Vec<Expression>) -> std::io::Result<Vec<String>> {
        let mut variables = vec![];
        for item in items {
            match item {
                Identifier(name) => variables.push(name.into()),
                other =>
                    return throw(Exact(format!("Expected a variable near {}", other.to_code())))
            }
        }
        Ok(variables)
    }

    fn set_variables(&self, names: Vec<String>, values: Vec<TypedValue>, f: fn(Vec<TypedValue>) -> TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        let ms = names.iter().zip(values.iter())
            .fold(self.clone(), |ms, (name, value)| ms.set(name, value.clone()));
        Ok((ms, f(values)))
    }
    
    pub fn set(&self, name: &str, value: TypedValue) -> Self {
        let mut variables = self.variables.to_owned();
        variables.insert(name.to_string(), value);
        Self::build(self.observations.to_owned(), variables)
    }

    pub fn with_module(
        &self,
        name: &str,
        variables: Vec<PackageOps>,
    ) -> Self {
        let structure = variables.iter().fold(
            HardStructure::empty(),
            |structure, key|
                structure.with_variable(
                    key.get_name().as_str(),
                    PlatformOp(key.clone()),
                ),
        );
        self.with_variable(name, Structured(Hard(structure)))
    }

    pub fn with_observer_variable(
        &self, 
        condition: Conditions, 
        code: Expression
    ) -> Self {
        let mut observations = self.observations.clone();
        observations.push(VariableObservation { condition, code });
        Self::build(observations, self.variables.clone())
    }

    pub fn with_row(&self, columns: &Vec<Column>, row: &Row) -> Self {
        row.get_values().iter().zip(columns.iter())
            .fold(self.clone(), |ms, (v, c)| ms.with_variable(c.get_name(), v.to_owned()))
            .with_variable(ROW_ID, Number(I64Value(row.get_id() as i64)))
    }

    pub fn with_variable(&self, name: &str, value: TypedValue) -> Self {
        let mut variables = self.variables.to_owned();
        variables.insert(name.to_string(), value);
        Self::build(self.observations.to_owned(), variables)
    }
}

/// Unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_types::DataType::NumberType;
    use crate::expression::Conditions::{GreaterThan, LessOrEqual, LessThan};
    use crate::expression::{FALSE, NULL, TRUE};
    use crate::number_kind::NumberKind::I64Kind;
    use crate::testdata::*;

    #[test]
    fn test_array_declaration() {
        let models = vec![Literal(Number(F64Value(3.25))), TRUE, FALSE, NULL, UNDEFINED];
        assert_eq!(models.iter().map(|e| e.to_code()).collect::<Vec<_>>(), vec![
            "3.25", "true", "false", "null", "undefined",
        ]);

        let (_, array) = Machine::empty().evaluate_as_array(&models).unwrap();
        assert_eq!(array, ArrayValue(Array::from(vec![
            Number(F64Value(3.25)), Boolean(true), Boolean(false), Null, Undefined
        ])));
    }

    #[test]
    fn test_aliases() {
        let model = NamedValue(
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
        let model = Divide(Box::new(Identifier("x".into())), Box::new(Literal(Number(I64Value(7)))));
        assert_eq!(model.to_code(), "x / 7");

        let (machine, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, Number(I64Value(7)));
        assert_eq!(machine.get("x"), Some(Number(I64Value(50))));
        assert_eq!(model.infer_type(), NumberType(I64Kind))
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
    fn test_variables() {
        let machine = Machine::empty()
            .with_variable("abc", Number(I64Value(5)))
            .with_variable("xyz", Number(I64Value(58)));
        assert_eq!(machine.get("abc"), Some(Number(I64Value(5))));
        assert_eq!(machine.get("xyz"), Some(Number(I64Value(58))));
    }

    /// Control Flow tests
    #[cfg(test)]
    mod control_flow_tests {
        use super::*;

        #[test]
        fn test_if_else_flow_1() {
            let model = If {
                condition: Box::new(Condition(GreaterThan(
                    Box::new(Identifier("num".into())),
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
                    Box::new(Identifier("num".into())),
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
                    Box::new(Identifier("num".into())),
                    Box::new(Literal(Number(I64Value(5)))),
                ))),
                // num = num + 1
                code: SetVariables(
                    Identifier("num".into()).into(),
                    Plus(
                        Identifier("num".into()).into(),
                        Literal(Number(I64Value(1))).into(),
                    ).into()).into(),
            };
            assert_eq!(model.to_code(), "while num < 5 num = num + 1");

            let machine = Machine::empty().with_variable("num", Number(I64Value(0)));
            let (machine, _) = machine.evaluate(&model).unwrap();
            assert_eq!(machine.get("num"), Some(Number(I64Value(5))))
        }
    }

    /// Function tests
    #[cfg(test)]
    mod function_tests {
        use super::*;
        use crate::data_types::DataType::UnresolvedType;

        #[test]
        fn test_anonymous_function() {
            // define a function call: (n => n + 5)(3)
            let model = FunctionCall {
                fx: Box::new(
                    Literal(Function {
                        params: vec![
                            Parameter::new("n", NumberType(I64Kind))
                        ],
                        body: Box::new(Plus(
                            Box::new(Identifier("n".into())),
                            Box::new(Literal(Number(I64Value(5)))))),
                        returns: NumberType(I64Kind),
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
            assert_eq!(model.to_code(), "(fn(n: i64): i64 => n + 5)(3)")
        }

        #[test]
        fn test_named_function() {
            // define a function: (a, b) => a + b
            let fx = Function {
                params: vec![
                    Parameter::new("a", NumberType(I64Kind)),
                    Parameter::new("b", NumberType(I64Kind)),
                ],
                body: Box::new(Plus(Box::new(
                    Identifier("a".into())
                ), Box::new(
                    Identifier("b".into())
                ))),
                returns: UnresolvedType,
            };

            // publish the function in scope: fn add(a, b) => a + b
            let machine = Machine::empty();
            let (machine, result) = machine.evaluate_scope(&vec![
                SetVariables(
                    Identifier("add".into()).into(),
                    Literal(fx.to_owned()).into()
                )
            ]).unwrap();
            assert_eq!(machine.get("add").unwrap(), fx);
            assert_eq!(result, Boolean(true));

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
        fn test_function_pipeline() {
            //  "Hello" |> util::md5 |> util::hex
            let model = ArrowVerticalBar(
                Box::new(ArrowVerticalBar(
                    Box::new(Literal(StringValue("Hello".to_string()))),
                    Box::new(ColonColon(
                        Box::new(Identifier("util".to_string())),
                        Box::new(Identifier("md5".to_string())),
                    )),
                )),
                Box::new(ColonColon(
                    Box::new(Identifier("util".to_string())),
                    Box::new(Identifier("hex".to_string())),
                )),
            );

            // evaluate the function
            let (machine, result) = Machine::new()
                .evaluate(&model)
                .unwrap();
            assert_eq!(result, StringValue("8b1a9953c4611296a827abf8c47804d7".to_string()));
            assert_eq!(model.to_code(), r#""Hello" |> util::md5 |> util::hex"#)
        }

        #[test]
        fn test_function_recursion() {
            // f = (fn(n: i64) => if(n <= 1) 1 else n * f(n - 1))
            let model = Function {
                params: vec![
                    Parameter::new("n", NumberType(I64Kind))
                ],
                // if(n <= 1, 1, n * f(n - 1))
                body: Box::new(If {
                    // n <= 1
                    condition: Box::new(Condition(LessOrEqual(
                        Box::new(Identifier("n".into())),
                        Box::new(Literal(Number(I64Value(1)))),
                    ))),
                    // 1
                    a: Box::new(Literal(Number(I64Value(1)))),
                    // n * f(n - 1)
                    b: Some(Box::from(Multiply(
                        Box::from(Identifier("n".into())),
                        Box::from(FunctionCall {
                            fx: Box::new(Identifier("f".into())),
                            args: vec![
                                Minus(
                                    Box::from(Identifier("n".into())),
                                    Box::from(Literal(Number(I64Value(1)))),
                                ),
                            ],
                        }),
                    ))),
                }),
                returns: NumberType(I64Kind),
            };
            println!("f = {}", model.to_code());

            // f(5.0)
            let machine = Machine::empty().with_variable("f", model);
            let (machine, result) = machine.evaluate(&FunctionCall {
                fx: Box::new(Identifier("f".into())),
                args: vec![
                    Literal(Number(I64Value(5)))
                ],
            }).unwrap();

            println!("result: {}", result);
            assert_eq!(result, Number(I64Value(120)));
        }
    }
    
}