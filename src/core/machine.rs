#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
//  Machine - state machine module
////////////////////////////////////////////////////////////////////

use crate::builtins::Builtins;
use crate::columns::Column;
use crate::compiler::Compiler;
use crate::data_types::DataType;
use crate::data_types::DataType::*;
use crate::dataframe::Dataframe;
use crate::dataframe::Dataframe::ModelTable;
use crate::errors::Errors::*;
use crate::errors::TypeMismatchErrors::*;
use crate::errors::{throw, SyntaxErrors};
use crate::expression::Conditions::{AssumedBoolean, In, When};
use crate::expression::Expression::*;
use crate::expression::Ranges::{Exclusive, Inclusive};
use crate::expression::{AliasCalls, Conditions, Expression, Observations, UseOps, UNDEFINED};
use crate::extractions::*;
use crate::machine::Observations::VariableObservation;
use crate::model_row_collection::ModelRowCollection;
use crate::numbers::Numbers::*;
use crate::packages::IoPkg;
use crate::packages::{Package, PackageOps};
use crate::parameter::Parameter;
use crate::query_engine::QueryEngine;
use crate::row_collection::RowCollection;
use crate::sequences::Sequences::{TheArray, TheDataframe, TheRange, TheTuple};
use crate::sequences::{is_in_range, Array, Sequence};
use crate::structures::Row;
use crate::structures::Structures::{Firm, Hard, Soft};
use crate::structures::*;
use crate::test_engine::TestEngine;
use crate::type_engine::TypeEngine;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::*;
use crate::utils::*;
use crate::web_engine::WebEngine;
use crate::{compiler, connections};
use serde::{Deserialize, Serialize};
use std::convert::{From, Into};
use std::env;
use std::fs::File;
use std::future::Future;
use std::io::Read;
use std::ops::{Deref, Neg};
use std::pin::Pin;

pub const FX_SELF: &str = "self";
pub const ROW_ID: &str = "__row_id__";
pub const ROW_SELF: &str = "__self__";

/// Represents the state of the machine.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Machine {
    aliases: Vec<AliasCalls>,
    observations: Vec<Observations>,
    variables: im::HashMap<String, TypedValue>,
}

impl Machine {

    ////////////////////////////////////////////////////////////////
    //  constructors
    ////////////////////////////////////////////////////////////////

    /// (lowest-level constructor) creates a new state machine
    fn build(
        aliases: Vec<AliasCalls>,
        observations: Vec<Observations>,
        variables: im::HashMap<String, TypedValue>,
    ) -> Self {
        Machine { aliases, observations, variables }
    }

    /// creates a new completely empty state machine
    pub fn empty() -> Self {
        Self::build(Vec::new(), Vec::new(), im::HashMap::new())
    }

    /// creates a new state machine prepopulated with platform packages
    pub fn new() -> Self {
        Self::empty()
    }

    /// creates a new state machine prepopulated with platform packages
    /// and default imports and/or constants
    pub fn new_platform() -> Self {
        Self::new()
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
        if self.is_debugging() { println!("machine: evaluate {:?}", expression); }
        match expression {
            Alias(a) => self.exec_alias_define(a),
            Aliases => self.exec_aliases(),
            ArrayExpression(items) => self.evaluate_as_array(items),
            ArrowCurvyLeft(dest, src) => QueryEngine::eval_pull_row(self, dest, src),
            ArrowCurvyLeft2x(dest, src) => QueryEngine::eval_pull_rows(&self, dest, src),
            ArrowCurvyRight(src, dest) => QueryEngine::eval_push_rows(self, src, dest, false),
            ArrowCurvyRight2x(src, dest) => QueryEngine::eval_push_rows(self, src, dest, true),
            ArrowFat(..) => Ok((self.to_owned(), Undefined)),
            ArrowSkinnyLeft(..) => Ok((self.to_owned(), Undefined)),
            ArrowSkinnyRight(a, b) => self.exec_arrow_skinny_right(a, b),
            ArrowVerticalBar(a, b) => self.exec_function_pipeline(a, b, false),
            ArrowVerticalBar2x(a, b) => self.exec_function_pipeline(a, b, true),
            As(value, data_type) => self.exec_cast_value_as(value, data_type),
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
            ColonColonCapture(a, b) => self.exec_colon_colon_capture(a, b),
            ColonColonColon(a, b) => self.exec_colon_colon_colon(a, b),
            ColonColonColonCapture(a, b) => self.exec_colon_colon_colon_capture(a, b),
            Condition(condition) => self.evaluate_condition(condition),
            Divide(a, b) => self.eval_inline_2(a, b, |aa, bb| aa / bb),
            DoWhile { condition, code } => self.exec_do_while(condition, code),
            ElementAt(a, b) => self.exec_index_of_collection(a, b),
            Feature { title, scenarios } => TestEngine::add_feature(self, title, scenarios),
            For { construct, code } => self.exec_for_construct(construct, code),
            FunctionCall { fx, args } => self.exec_function_call(fx, args),
            HTTP(..) => WebEngine::evaluate(self, expression),
            Identifier(name) => self.exec_get_variable(name),
            If { condition, a, b } => self.exec_if_then_else(condition, a, b),
            Include(path) => self.exec_include(path),
            Infix(a, b) => self.exec_infix(a, b),
            IsDefined(a) => self.exec_is_defined(a),
            Literal(value) => Ok((self.to_owned(), value.to_owned())),
            Ls(path) => self.exec_ls(path),
            MatchExpression(src, cases) => self.exec_match(src, cases),
            Minus(a, b) => self.eval_inline_2(a, b, |aa, bb| aa - bb),
            Module(name, ops) => self.exec_module_structure(name, ops),
            Modulo(a, b) => self.eval_inline_2(a, b, |aa, bb| aa % bb),
            Multiply(a, b) => self.eval_inline_2(a, b, |aa, bb| aa * bb),
            NamedValue(name, expr) => {
                let (machine, tv) = self.evaluate(expr)?;
                Ok((machine.with_variable(name, tv.clone()), tv))
            }
            Neg(a) => self.exec_negate(a),
            OneTime(a, uid) => self.exec_one_time(a, *uid),
            Parameters(params) => self.build_parameters(params),
            Plus(a, b) => self.eval_inline_2(a, b, |aa, bb| aa + bb),
            PlusPlus(a, b) => self.eval_inline_2(a, b, Self::exec_plus_plus),
            Pow(a, b) => self.eval_inline_2(a, b, |aa, bb| aa.pow(&bb).unwrap_or(Undefined)),
            Range(Exclusive(a, b)) => self.eval_inline_2(a, b, |aa, bb| aa.range_exclusive(&bb).unwrap_or(Undefined)),
            Range(Inclusive(a, b)) => self.eval_inline_2(a, b, |aa, bb| aa.range_inclusive(&bb).unwrap_or(Undefined)),
            Referenced(a) => self.exec_referenced(a),
            Return(a) => self.evaluate(a),
            Scenario { title, inherits, verifications } => self.exec_scenario(title, inherits, verifications),
            SetVariables(vars, values) => self.exec_set_variables(vars, values),
            SetVariablesExpr(vars, values) => self.exec_set_variables_expr(vars, values),
            StructureExpression(items) => self.exec_structure_soft(items),
            Test(expr) => TestEngine::evaluate_feature(self, expr),
            Throw(expr) => self.exec_throw(expr),
            TupleExpression(args) => self.exec_tuple(args),
            Use(ops) => self.exec_uses(ops),
            WhenEver { condition, code } => self.exec_when_statement(condition, code),
            While { condition, code } => self.exec_while(condition, code),
            With { resource, code } => self.exec_with(resource, code),
            Yield(a, uuid) => self.exec_yield(a, *uuid),
            Zip(a, b) => self.exec_zip(a, b),
            ////////////////////////////////////////////////////////////////////
            // SQL models
            ////////////////////////////////////////////////////////////////////
            Delete { .. } | Deselect { .. } | Fetch { .. } | GroupBy { .. }
            | Having { .. } | Limit { .. } | OrderBy { .. } 
            | Select { .. } | Undelete { .. } | Where { .. } =>
                QueryEngine::evaluate(self, expression),
        }
    }

    pub fn evaluate_async<'a>(
        &'a self,
        expression: &'a Expression,
    ) -> Pin<Box<dyn Future<Output=std::io::Result<(Self, TypedValue)>> + Send + 'a>> {
        if self.is_debugging() { println!("machine: evaluate_async {:?}", expression); }
        Box::pin(async move {
            match expression {
                Alias(a) => self.exec_alias_define(a),
                Aliases => self.exec_aliases(),
                ArrayExpression(items) => self.evaluate_as_array_async(items).await,
                ArrowCurvyLeft(dest, src) => QueryEngine::eval_pull_row_async(self, dest, src).await,
                ArrowCurvyLeft2x(dest, src) => QueryEngine::eval_pull_rows_async(&self, dest, src).await,
                ArrowCurvyRight(src, dest) => QueryEngine::eval_push_rows_async(self, src, dest, false).await,
                ArrowCurvyRight2x(src, dest) => QueryEngine::eval_push_rows_async(self, src, dest, true).await,
                ArrowFat(..) => Ok((self.to_owned(), Undefined)),
                ArrowSkinnyLeft(..) => Ok((self.to_owned(), Undefined)),
                ArrowSkinnyRight(a, b) => self.exec_arrow_skinny_right(a, b),
                ArrowVerticalBar(a, b) => self.exec_function_pipeline(a, b, false),
                ArrowVerticalBar2x(a, b) => self.exec_function_pipeline(a, b, true),
                As(value, data_type) => self.exec_cast_value_as_async(value, data_type).await,
                Assert { condition, message } => self.exec_assert_async(condition, message).await,
                BitwiseAnd(a, b) => self.eval_inline_2_async(a, b, |aa, bb| aa & bb).await,
                BitwiseOr(a, b) => self.eval_inline_2_async(a, b, |aa, bb| aa | bb).await,
                BitwiseShiftLeft(a, b) => self.eval_inline_2_async(a, b, |aa, bb| aa << bb).await,
                BitwiseShiftRight(a, b) => self.eval_inline_2_async(a, b, |aa, bb| aa >> bb).await,
                BitwiseXor(a, b) => self.eval_inline_2_async(a, b, |aa, bb| aa ^ bb).await,
                Coalesce(a, b) => self.eval_inline_2_async(a, b, |aa, bb| aa.coalesce(bb)).await,
                CoalesceErr(a, b) => self.exec_coalesce_error_async(a, b).await,
                CodeBlock(ops) => self.evaluate_scope_async(ops).await,
                ColonColon(a, b) => self.exec_colon_colon_async(a, b).await,
                ColonColonCapture(a, b) => self.exec_colon_colon_capture_async(a, b).await,
                ColonColonColon(a, b) => self.exec_colon_colon_colon_async(a, b).await,
                ColonColonColonCapture(a, b) => self.exec_colon_colon_colon_capture_async(a, b).await,
                Condition(condition) => self.evaluate_condition_async(condition).await,
                Divide(a, b) => self.eval_inline_2_async(a, b, |aa, bb| aa / bb).await,
                DoWhile { condition, code } => self.exec_do_while_async(condition, code).await,
                ElementAt(a, b) => self.exec_index_of_collection_async(a, b).await,
                Feature { title, scenarios } => TestEngine::add_feature(self, title, scenarios),
                For { construct, code } => self.exec_for_construct_async(construct, code).await,
                FunctionCall { fx, args } => self.exec_function_call_async(fx, args).await,
                HTTP(..) => WebEngine::evaluate_async(self, expression).await,
                Identifier(name) => self.exec_get_variable_async(name).await,
                If { condition, a, b } => self.exec_if_then_else_async(condition, a, b).await,
                Include(path) => self.exec_include_async(path).await,
                Infix(a, b) => self.exec_infix_async(a, b).await,
                IsDefined(a) => self.exec_is_defined(a),
                Literal(value) => Ok((self.to_owned(), value.to_owned())),
                Ls(path) => self.exec_ls_async(path).await,
                MatchExpression(src, cases) => self.exec_match(src, cases),
                Minus(a, b) => self.eval_inline_2_async(a, b, |aa, bb| aa - bb).await,
                Module(name, ops) => self.exec_module_structure(name, ops),
                Modulo(a, b) => self.eval_inline_2_async(a, b, |aa, bb| aa % bb).await,
                Multiply(a, b) => self.eval_inline_2_async(a, b, |aa, bb| aa * bb).await,
                NamedValue(name, expr) => {
                    let (machine, tv) = self.evaluate_async(expr).await?;
                    Ok((machine.with_variable(name, tv.clone()), tv))
                }
                Neg(a) => self.exec_negate(a),
                OneTime(a, id) => self.exec_one_time_async(a, *id).await,
                Parameters(params) => self.build_parameters(params),
                Plus(a, b) => self.eval_inline_2_async(a, b, |aa, bb| aa + bb).await,
                PlusPlus(a, b) => self.eval_inline_2_async(a, b, Self::exec_plus_plus).await,
                Pow(a, b) => self.eval_inline_2_async(a, b, |aa, bb| aa.pow(&bb).unwrap_or(Undefined)).await,
                Range(Exclusive(a, b)) => self.eval_inline_2_async(a, b, |aa, bb| aa.range_exclusive(&bb).unwrap_or(Undefined)).await,
                Range(Inclusive(a, b)) => self.eval_inline_2_async(a, b, |aa, bb| aa.range_inclusive(&bb).unwrap_or(Undefined)).await,
                Referenced(a) => self.exec_referenced_async(a).await,
                Return(a) => self.evaluate_async(a).await,
                Scenario { title, inherits, verifications } => self.exec_scenario(title, inherits, verifications),
                SetVariables(vars, values) => self.exec_set_variables_async(vars, values).await,
                SetVariablesExpr(vars, values) => self.exec_set_variables_expr_async(vars, values).await,
                StructureExpression(items) => self.exec_structure_soft_async(items).await,
                Test(expr) => TestEngine::evaluate_feature(self, expr),
                Throw(expr) => self.exec_throw_async(expr).await,
                TupleExpression(args) => self.exec_tuple_async(args).await,
                Use(ops) => self.exec_uses(ops),
                WhenEver { condition, code } => self.exec_when_statement(condition, code),
                While { condition, code } => self.exec_while_async(condition, code).await,
                With { resource, code } => self.exec_with_async(resource, code).await,
                Yield(a, uuid) => self.exec_yield_async(a, *uuid).await,
                Zip(a, b) => self.exec_zip_async(a, b).await,
                ////////////////////////////////////////////////////////////////////
                // SQL models
                ////////////////////////////////////////////////////////////////////
                Delete { .. } | Deselect { .. } | Fetch { .. } | GroupBy { .. }
                | Having { .. } | Limit { .. } | OrderBy { .. }
                | Select { .. } | Undelete { .. } | Where { .. } =>
                    QueryEngine::evaluate_async(self, expression).await,
            }
        })
    }

    /// evaluates the specified [Expression]; returning an array ([TypedValue]) result.
    pub fn evaluate_as_array(
        &self,
        ops: &[Expression],
    ) -> std::io::Result<(Self, TypedValue)> {
        self.evaluate_as_vec(ops).map(|(ms, vec)| (ms, ArrayValue(Array::from(vec))))
    }

    /// evaluates the specified [Expression]; returning an array ([TypedValue]) result.
    pub async fn evaluate_as_array_async(
        &self,
        ops: &[Expression],
    ) -> std::io::Result<(Self, TypedValue)> {
        self.evaluate_as_vec_async(ops).await.map(|(ms, vec)| (ms, ArrayValue(Array::from(vec))))
    }

    /// evaluates the specified [Expression]; returning an array ([String]) result.
    pub fn evaluate_as_atoms(
        &self,
        ops: &[Expression],
    ) -> std::io::Result<(Self, Vec<String>)> {
        let mut values = Vec::new();
        for op in ops {
            values.push(pull_identifier_name(op)?);
        }
        Ok((self.to_owned(), values))
    }

    /// evaluates the specified [Expression]; returning a [Dataframe] result.
    pub fn evaluate_as_dataframe(
        &self,
        expression: &Expression,
    ) -> std::io::Result<(Self, Dataframe)> {
        let (ms, value) = self.evaluate(expression)?;
        Ok((ms, value.to_dataframe()?))
    }

    /// evaluates the specified [Expression]; returning a [Dataframe] result.
    pub async fn evaluate_as_dataframe_async(
        &self,
        expression: &Expression,
    ) -> std::io::Result<(Self, Dataframe)> {
        let (ms, value) = self.evaluate_async(expression).await?;
        Ok((ms, value.to_dataframe()?))
    }
    
    /// evaluates the specified [Expression]; returning a [Dataframe] result.
    pub fn evaluate_as_datatype(
        &self,
        expression: &Expression,
    ) -> std::io::Result<(Self, DataType)> {
        let (ms, value) = self.evaluate(expression)?;
        match value {
            Kind(datatype) => Ok((ms, datatype)),
            other => throw(TypeMismatch(TypeIdentifierExpected(other.to_code())))
        }
    }

    /// evaluates the specified [Expression]; returning a [Dataframe] result.
    pub async fn evaluate_as_datatype_async(
        &self,
        expression: &Expression,
    ) -> std::io::Result<(Self, DataType)> {
        let (ms, value) = self.evaluate_async(expression).await?;
        match value {
            Kind(datatype) => Ok((ms, datatype)),
            other => throw(TypeMismatch(TypeIdentifierExpected(other.to_code())))
        }
    }

    /// evaluates the specified [Expression]; returning a [String] result.
    pub fn evaluate_as_string(
        &self,
        expression: &Expression,
    ) -> std::io::Result<(Self, String)> {
        let (ms, value) = self.evaluate(expression)?;
        Ok((ms, pull_string(&value)?))
    }

    /// evaluates the specified [Expression]; returning a [String] result.
    pub async fn evaluate_as_string_async(
        &self,
        expression: &Expression,
    ) -> std::io::Result<(Self, String)> {
        let (ms, value) = self.evaluate_async(expression).await?;
        Ok((ms, pull_string(&value)?))
    }
    
    /// evaluates the specified [Expression]; returning a [Vec] of [TypedValue]s.
    pub fn evaluate_as_vec(
        &self,
        ops: &[Expression],
    ) -> std::io::Result<(Self, Vec<TypedValue>)> {
        let (mut ms, mut results) = (self.to_owned(), Vec::new());
        for op in ops {
            let (ms1, result1) = ms.evaluate(op)?;
            results.push(result1);
            ms = ms1;
        }
        Ok((ms, results))
    }

    /// evaluates the specified [Expression]; returning a [Vec] of [TypedValue]s.
    pub async fn evaluate_as_vec_async(
        &self,
        ops: &[Expression],
    ) -> std::io::Result<(Self, Vec<TypedValue>)> {
        let (mut ms, mut results) = (self.to_owned(), Vec::new());
        for op in ops {
            let (ms1, result1) = ms.evaluate_async(op).await?;
            results.push(result1);
            ms = ms1;
        }
        Ok((ms, results))
    }

    /// evaluates the specified [Expression]; returning a [TypedValue] result.
    pub fn evaluate_condition(
        &self,
        condition: &Conditions,
    ) -> std::io::Result<(Self, TypedValue)> {
        use crate::expression::Conditions::*;
        if self.is_debugging() { println!("machine: evaluate_condition: {:?}", condition) }
        match condition {
            And(a, b) => self.eval_inline_2(a, b, |aa, bb| aa.and(&bb).unwrap_or(Undefined)),
            AssumedBoolean(a) => self.exec_assume_boolean(a),
            Equal(a, b) => self.eval_inline_2(a, b, |aa, bb| Boolean(aa.equals(&bb))),
            False => Ok((self.to_owned(), Boolean(false))),
            GreaterThan(a, b) => self.eval_inline_2(a, b, |aa, bb| Boolean(aa > bb)),
            GreaterOrEqual(a, b) => self.eval_inline_2(a, b, |aa, bb| Boolean(aa >= bb)),
            In(a, b) => self.exec_in(a, b),
            LessThan(a, b) => self.eval_inline_2(a, b, |aa, bb| Boolean(aa < bb)),
            LessOrEqual(a, b) => self.eval_inline_2(a, b, |aa, bb| Boolean(aa <= bb)),
            Like(text, pattern) => self.exec_like(text, pattern),
            Matches(src, pattern) => self.eval_inline_2(src, pattern, |aa, bb| aa.matches(&bb)),
            Not(a) => self.eval_inline_1(a, |aa| !aa),
            NotEqual(a, b) => self.eval_inline_2(a, b, |aa, bb| Boolean(!aa.equals(&bb))),
            Or(a, b) => self.eval_inline_2(a, b, |aa, bb| aa.or(&bb).unwrap_or(Undefined)),
            True => Ok((self.to_owned(), Boolean(true))),
            When(..) => Ok((self.clone(), Boolean(false))),
        }
    }

    /// evaluates the specified [Expression]; returning a [TypedValue] result.
    pub async fn evaluate_condition_async(
        &self,
        condition: &Conditions,
    ) -> std::io::Result<(Self, TypedValue)> {
        use crate::expression::Conditions::*;
        if self.is_debugging() { println!("machine: evaluate_condition_async: {:?}", condition) }
        match condition {
            And(a, b) => self.eval_inline_2_async(a, b, |aa, bb| aa.and(&bb).unwrap_or(Undefined)).await,
            AssumedBoolean(a) => self.exec_assume_boolean_async(a).await,
            Equal(a, b) => self.eval_inline_2_async(a, b, |aa, bb| Boolean(aa == bb)).await,
            False => Ok((self.to_owned(), Boolean(false))),
            GreaterThan(a, b) => self.eval_inline_2_async(a, b, |aa, bb| Boolean(aa > bb)).await,
            GreaterOrEqual(a, b) => self.eval_inline_2_async(a, b, |aa, bb| Boolean(aa >= bb)).await,
            In(a, b) => self.exec_in(a, b),
            LessThan(a, b) => self.eval_inline_2_async(a, b, |aa, bb| Boolean(aa < bb)).await,
            LessOrEqual(a, b) => self.eval_inline_2_async(a, b, |aa, bb| Boolean(aa <= bb)).await,
            Like(text, pattern) => self.exec_like_async(text, pattern).await,
            Matches(src, pattern) => self.eval_inline_2_async(src, pattern, |aa, bb| aa.matches(&bb)).await,
            Not(a) => self.eval_inline_1_async(a, |aa| !aa).await,
            NotEqual(a, b) => self.eval_inline_2_async(a, b, |aa, bb| Boolean(aa != bb)).await,
            Or(a, b) => self.eval_inline_2_async(a, b, |aa, bb| aa.or(&bb).unwrap_or(Undefined)).await,
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
    pub async fn evaluate_opt_async(&self, expr: &Option<Box<Expression>>) -> std::io::Result<(Self, Option<TypedValue>)> {
        match expr {
            None => Ok((self.to_owned(), None)),
            Some(item) => self.evaluate_async(item).await
                .map(|(ms, result)| (ms, Some(result)))
        }
    }

    /// evaluates the specified [Expression]; returning a [TypedValue] result.
    pub fn evaluate_or_else(
        &self,
        maybe_expr: &Option<Box<Expression>>,
        f: fn(&Machine) -> TypedValue
    ) -> std::io::Result<(Self, TypedValue)> {
        match maybe_expr {
            Some(expr) => self.evaluate(expr),
            None => Ok((self.to_owned(), f(self))),
        }
    }

    /// evaluates the specified [Expression]; returning a [TypedValue] result.
    pub async fn evaluate_or_else_async(
        &self,
        maybe_expr: &Option<Box<Expression>>,
        f: fn(&Machine) -> TypedValue
    ) -> std::io::Result<(Self, TypedValue)> {
        match maybe_expr {
            Some(expr) => self.evaluate_async(expr).await,
            None => Ok((self.to_owned(), f(self))),
        }
    }

    /// evaluates the specified [Expression]; returning a [TypedValue] result.
    pub fn evaluate_or_undef(&self, expr: &Option<Box<Expression>>) -> std::io::Result<(Self, TypedValue)> {
        match expr {
            None => Ok((self.to_owned(), Undefined)),
            Some(item) => self.evaluate(item)
        }
    }

    /// evaluates the specified [Expression]; returning a [TypedValue] result.
    pub async fn evaluate_or_undef_async(&self, expr: &Option<Box<Expression>>) -> std::io::Result<(Self, TypedValue)> {
        match expr {
            None => Ok((self.to_owned(), Undefined)),
            Some(item) => self.evaluate_async(item).await,
        }
    }

    fn build_parameters(
        &self,
        columns: &Vec<Parameter>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let machine = self.to_owned();
        let variables = machine.get_variables();
        let mut values = Vec::with_capacity(columns.len());
        for c in columns.iter() {
            if let Some(value) = variables.get(c.get_name()) {
                values.push(value.to_owned());
            }
        }
        Ok((machine, ArrayValue(Array::from(values))))
    }

    pub fn evaluate_pure(
        expression: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        match expression {
            expr if expr.is_pure() => Self::new_platform().evaluate(expr),
            expr => throw(Exact(format!("Pure expression was expected near {}", expr.to_code())))
        }
    }

    /// evaluates the specified [Expression]; returning a [TypedValue] result.
    fn evaluate_scope(&self, ops: &[Expression]) -> std::io::Result<(Self, TypedValue)> {
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

    /// evaluates the specified [Expression]; returning a [TypedValue] result.
    async fn evaluate_scope_async(&self, ops: &[Expression]) -> std::io::Result<(Self, TypedValue)> {
        let (mut ms, mut result) = (self.clone(), Undefined);
        for op in ops {
            let (ms1, result1) = ms.evaluate_async(op).await?;
            ms = ms1;
            result = result1;
        }
        Ok((ms, result))
    }

    pub fn is_debugging(&self) -> bool {
        self.get("__DEBUG__").is_some_and(|v| v == Boolean(true))
    }

    pub fn is_true(
        &self,
        condition: &Conditions,
    ) -> std::io::Result<(Self, bool)> {
        let (ms, result) = self.evaluate_condition(condition)?;
        Ok((ms, pull_bool(&result)?))
    }

    pub async fn is_true_async(
        &self,
        condition: &Conditions,
    ) -> std::io::Result<(Self, bool)> {
        let (ms, result) = self.evaluate_condition_async(condition).await?;
        Ok((ms, pull_bool(&result)?))
    }

    /// Defines an `alias`
    fn exec_alias_define(&self, alias: &AliasCalls) -> std::io::Result<(Self, TypedValue)> {
        Ok((self.with_alias(alias.clone()), Boolean(true)))
    }
    
    pub fn get_alias_parameters() -> Vec<Parameter> {
        vec![
            Parameter::new("value", StringType)
        ]
    }
    
    fn exec_aliases(&self) -> std::io::Result<(Self, TypedValue)> {
        Ok((self.to_owned(), TableValue(ModelTable(ModelRowCollection::from_parameters_and_rows(
            &Self::get_alias_parameters(),
            &self.aliases.iter().enumerate()
                .map(|(id, call)| Row::new(id, vec![
                    StringValue(Alias(call.clone()).to_code())
                ]))
                .collect::<Vec<_>>(),
        )))))
    }

    /// Executes an `alias` function call
    fn exec_alias_function_call(
        &self,
        name: &str,
        args: &[Expression],
    ) -> std::io::Result<(Self, TypedValue)> {
        let alias_maybe = self.aliases.iter().find(|alias| match alias {
            AliasCalls::FunctionCall(my_name, my_params, _) =>
                my_name == name && my_params.len() == args.len(),
            _ => false
        }).map(|alias| (alias.get_parameters(), alias.get_body()));
        match alias_maybe {
            Some((params, body)) => {
                let (ms, vargs) = self.evaluate_as_vec(args)?;
                let new_body = Compiler::build(body.to_code().as_str())?;
                ms.build_function_arguments(params, vargs).evaluate(&new_body)
            },
            _ => throw(IdentifierNotFound(name.into()))
        }
    }

    /// Executes an `alias` function call
    async fn exec_alias_function_call_async(
        &self,
        name: &str,
        args: &[Expression],
    ) -> std::io::Result<(Self, TypedValue)> {
        let alias_maybe = self.aliases.iter().find(|alias| match alias {
            AliasCalls::FunctionCall(my_name, my_params, _) =>
                my_name == name && my_params.len() == args.len(),
            _ => false
        }).map(|alias| (alias.get_parameters(), alias.get_body()));
        match alias_maybe {
            Some((params, body)) => {
                let (ms, vargs) = self.evaluate_as_vec_async(args).await?;
                let new_body = Compiler::build(body.to_code().as_str())?;
                ms.build_function_arguments(params, vargs).evaluate_async(&new_body).await
            },
            _ => throw(IdentifierNotFound(name.into()))
        }
    }

    /// Executes an `alias` identifier call
    fn exec_alias_identifier_call(
        &self,
        name: &str,
    ) -> std::io::Result<(Self, TypedValue)> {
        let alias_maybe = self.aliases.iter().find(|alias| match alias {
            AliasCalls::IdentifierCall(my_name, _) => my_name == name,
            _ => false
        }).map(|alias| alias.get_body());
        match alias_maybe {
            Some(body) => {
                let new_body = Compiler::build(body.to_code().as_str())?;
                self.evaluate(&new_body)
            },
            _ => throw(IdentifierNotFound(name.into()))
        }
    }

    /// Executes an `alias` identifier call (async)
    async fn exec_alias_identifier_call_async(
        &self,
        name: &str,
    ) -> std::io::Result<(Self, TypedValue)> {
        let alias_maybe = self.aliases.iter().find(|alias| match alias {
            AliasCalls::IdentifierCall(my_name, _) => my_name == name,
            _ => false
        }).map(|alias| alias.get_body());
        match alias_maybe {
            Some(body) => {
                let new_body = Compiler::build(body.to_code().as_str())?;
                self.evaluate_async(&new_body).await
            },
            _ => throw(IdentifierNotFound(name.into()))
        }
    }

    /// Creates a function definition
    /// #### Examples
    /// ##### Full signature with parameter and return types.
    /// ```
    /// fn product(a: i64, b: i64): i64 -> a * b
    /// ```
    /// ##### Return type inferred, parameters still typed.
    /// ```
    /// fn product(a: i64, b: i64) -> a * b
    /// ```
    /// ##### Fully inferred types — great for scripting.
    /// ```
    /// fn product(a, b) -> a * b
    /// ```
    /// ##### First-class anonymous function assigned to identifier.
    /// ```
    /// product = (a, b) -> a * b
    /// ```
    /// ##### Default argument value (b = 1) supported
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

    /// Evaluates an assertion returning true or an error
    async fn exec_assert_async(
        &self,
        condition: &Expression,
        message: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        match self.exec_assume_boolean_async(condition).await? {
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

    /// Requires the result to be [Boolean]
    async fn exec_assume_boolean_async(
        &self,
        expr: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        match self.evaluate_async(expr).await? {
            (machine, Boolean(b)) => Ok((machine, Boolean(b))),
            _ => throw(Exact(format!("Expression must be a boolean: {}", expr.to_code())))
        }
    }
    
    fn exec_cast_value_as(
        &self,
        value_expr: &Expression,
        type_expr: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, value) = self.evaluate(value_expr)?;
        let (ms, datatype) = ms.evaluate_as_datatype(type_expr)?;
        Ok((ms, value.convert_to(&datatype)?))
    }

    async fn exec_cast_value_as_async(
        &self,
        value_expr: &Expression,
        type_expr: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, value) = self.evaluate_async(value_expr).await?;
        let (ms, datatype) = ms.evaluate_as_datatype_async(type_expr).await?;
        Ok((ms, value.convert_to(&datatype)?))
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

    async fn exec_coalesce_error_async(
        &self,
        attempt: &Expression,
        substitute: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        match self.evaluate_async(attempt).await {
            Ok((_, ErrorValue(..))) | Err(_) => self.evaluate(substitute),
            Ok((ms, value)) => Ok((ms, value)),
        }
    }

    /// Executes a native method call
    fn exec_colon_colon(
        &self,
        container: &Expression,
        field: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        match field {
            // is this an instantiation?
            // ex: Table(face: String(2), suit: String(2))::new
            FunctionCall { fx, args } if pull_identifier_name(fx)? == "new" => {
                self.exec_colon_colon_new(container, args)
            }
            // ex: DateTime::new
            Identifier(name) if name == "new" => 
                self.exec_colon_colon_new(container, &vec![]),
            // must be a platform function. 
            // ex: stocks::replay()
            _ => self.exec_colon_colon_package(container, field)
        }
    }

    /// Executes a native method call 
    /// #### Examples
    /// #### Method call
    /// ```
    /// "hello"::left("5)
    /// ```
    /// #### Instantiation 
    /// ```
    /// let Stock = Struct(symbol: String(8), exchange: String(8), last_sale: f64)
    /// Stock::new("ABC", "NYSE", 37.37)
    /// ```
    async fn exec_colon_colon_async(
        &self,
        container: &Expression,
        field: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        match field {
            // is this an instantiation?
            // ex: Table(face: String(2), suit: String(2))::new
            FunctionCall { fx, args } if pull_identifier_name(fx)? == "new" => {
                self.exec_colon_colon_new_async(container, args).await
            }
            // ex: DateTime::new
            Identifier(name) if name == "new" =>
                self.exec_colon_colon_new_async(container, &vec![]).await,
            // must be a platform function. 
            // ex: stocks::replay()
            _ => self.exec_colon_colon_package_async(container, field).await
        }
    }

    /// Evaluates a type instantiation
    fn exec_colon_colon_new(
        &self,
        container: &Expression,
        args: &[Expression],
    ) -> std::io::Result<(Self, TypedValue)> {
        // evaluate the "kind"
        let (ms, data_type) = match self.evaluate(container)? {
            (ms, Kind(data_type)) => (ms, data_type),
            (ms, TupleValue(values)) => (ms, TypeEngine::decipher_model_tuple_literal(&values)?),
            (ms, _) => (ms, TypeEngine::decipher_model(container)?)
        };
        // evaluate the arguments
        let (ms, inst_args) = ms.evaluate_as_vec(args)?;
        // construct the instance
        let inst = data_type.instantiate(inst_args)?;
        Ok((ms, inst))
    }

    /// Evaluates a type instantiation
    /// #### Examples
    /// ##### System-defined types
    /// ```
    /// Table(face: String(2), suit: String(2))::new
    /// ```
    /// ##### User-defined types
    /// ```
    /// Cards = Table(face: String(2), suit: String(2))
    /// Cards::new()
    /// ```
    async fn exec_colon_colon_new_async(
        &self,
        container: &Expression,
        args: &[Expression],
    ) -> std::io::Result<(Self, TypedValue)> {
        // evaluate the class
        let (ms, data_type) = match self.evaluate_async(container).await? {
            (ms, Kind(data_type)) => (ms, data_type),
            (ms, _) => (ms, TypeEngine::decipher_model(container)?)
        };
        // evaluate the arguments
        let (ms, inst_args) = ms.evaluate_as_vec_async(args).await?;
        // construct the instance
        let inst = data_type.instantiate(inst_args)?;
        Ok((ms, inst))
    }

    /// Enables integrated support for package functions for specific data types:
    fn exec_colon_colon_builtins(
        &self,
        container: &Expression,
        field: &Expression
    ) -> std::io::Result<(Machine, TypedValue)> {
        // get the function name and arguments
        let (fx_name, args) = match find_name_and_args(field) {
            Some((name, args)) => (name, args),
            None => return throw(Exact(format!("Illegal function '{}' for {}", field, container)))
        };
        // convert the arguments to values
        let (ms, host) = self.evaluate(&container)?;
        let mut fx_args = vec![host.clone()];
        let (ms, v_args) = ms.evaluate_as_vec(&args)?;
        fx_args.extend(v_args);
        // is it a builtin method?
        match Builtins::lookup_by_value(&host, fx_name.as_str()) {
            Some(pkg_op) => pkg_op.evaluate(ms, fx_args),
            None => {
                let container_name = pull_identifier_name(container)?;
                match ms.get(container_name.as_str()) {
                    Some(Structured(structure)) =>
                        ms.exec_structure_method_or_property(&container_name, structure, field, false),
                    _ => throw(Exact(format!("Illegal function '{}' for {}", fx_name, host)))
                }
            }
        }
    }

    /// Enables integrated support for package functions for specific data types:
    /// #### Examples
    /// ##### Many platform functions are treated as built-in/native functions
    /// ```
    /// "Hello World"::position("World")
    /// ```
    /// ##### Parentheses are optional for parameterless native functions
    /// ```
    /// stocks::to_json
    /// ```
    async fn exec_colon_colon_builtins_async(
        &self,
        container: &Expression,
        field: &Expression
    ) -> std::io::Result<(Machine, TypedValue)> {
        // get the function name and arguments
        let (fx_name, args) = match find_name_and_args(field) {
            Some((name, args)) => (name, args),
            None => return throw(Exact(format!("Illegal function '{}' for {}", field, container)))
        };
        // convert the arguments to values
        let (ms, host) = self.evaluate_async(&container).await?;
        let mut fx_args = vec![host.clone()];
        let (ms, v_args) = ms.evaluate_as_vec_async(&args).await?;
        fx_args.extend(v_args);
        // is it a builtin method?
        match Builtins::lookup_by_value(&host, fx_name.as_str()) {
            Some(pkg_op) => pkg_op.evaluate_async(ms, fx_args).await,
            None => {
                let container_name = pull_identifier_name(container)?;
                match ms.get(container_name.as_str()) {
                    Some(Structured(structure)) =>
                        ms.exec_structure_method_or_property_async(&container_name, structure, field, false).await,
                    _ => throw(Exact(format!("Illegal function '{}' for {}", fx_name, host)))
                }
            }
        }
    }

    fn exec_colon_colon_package(
        &self,
        container: &Expression,
        field: &Expression
    ) -> std::io::Result<(Machine, TypedValue)> {
        let pkg_op_maybe = find_name(container)
            .and_then(|pkg_name| find_name(field).map(|name| (pkg_name, name)))
            .and_then(|(pkg_name, name)| PackageOps::lookup_by_name(pkg_name.as_str(), name.as_str()))
            .and_then(|pkg_op| find_name_and_args(field).map(|(_, args)| (pkg_op, args)));
        match pkg_op_maybe {
            None => self.exec_colon_colon_builtins(container, field),
            Some((pkg_op, args)) => {
                let (ms, fx_args) = self.evaluate_as_vec(&args)?;
                pkg_op.evaluate(ms, fx_args)
            }
        }
    }

    async fn exec_colon_colon_package_async(
        &self,
        container: &Expression,
        field: &Expression
    ) -> std::io::Result<(Machine, TypedValue)> {
        let pkg_op_maybe = find_name(container)
            .and_then(|pkg_name| find_name(field).map(|name| (pkg_name, name)))
            .and_then(|(pkg_name, name)| PackageOps::lookup_by_name(pkg_name.as_str(), name.as_str()))
            .and_then(|pkg_op| find_name_and_args(field).map(|(_, args)| (pkg_op, args)));
        match pkg_op_maybe {
            None => self.exec_colon_colon_builtins_async(container, field).await,
            Some((pkg_op, args)) => {
                let (ms, fx_args) = self.evaluate_as_vec_async(&args).await?;
                pkg_op.evaluate_async(ms, fx_args).await
            }
        }
    }

    /// Enables support for structure method calls and field assignments
    fn exec_structure_method_or_property(
        &self,
        container_name: &str,
        structure: Structures,
        field: &Expression,
        allow_assignment: bool,
    ) -> std::io::Result<(Self, TypedValue)> {
        let ms = self.clone();
        match field {
            // "Hello World"::position('W') | user_dao.create(item)
            FunctionCall { fx, args } => structure.pollute(ms).exec_function_call(fx, args),
            // stock.last_sale = 24.11
            SetVariables(var_expr, value_expr) if allow_assignment => {
                match var_expr.deref() {
                    Identifier(name) => {
                        let (ms, value) = ms.evaluate(value_expr)?;
                        Ok((ms.with_variable(container_name, Structured(structure.update_by_name(name, value))), Boolean(true)))
                    }
                    z => throw(Exact(format!("Illegal field '{}' for structure {}", z.to_code(), container_name)))
                }
            }
            // "Hello"::trim | stock.symbol
            Identifier(name) => Ok((ms, structure.get(name))),
            z => throw(Exact(format!("Illegal field '{}' for structure {}", z.to_code(), container_name)))
        }
    }

    /// Enables support for structure method calls and field assignments
    /// #### Examples
    /// ##### Method invocations
    /// ```
    /// math.compute(5, 8)
    /// "Hello World"::position('W')
    /// "Hello"::trim
    /// ```
    /// ##### Property assignments
    /// ```
    /// stock.last_sale = 24.11
    /// ```
    /// ##### Property invocations
    /// ```
    /// stock.last_sale
    /// ```
    async fn exec_structure_method_or_property_async(
        &self,
        container_name: &str,
        structure: Structures,
        field: &Expression,
        allow_assignment: bool,
    ) -> std::io::Result<(Self, TypedValue)> {
        let ms = self.clone();
        match field {
            // "Hello World"::position('W') | user_dao.create(item)
            FunctionCall { fx, args } => structure.pollute(ms)
                .exec_function_call_async(fx, args).await,
            // stock.last_sale = 24.11
            SetVariables(var_expr, value_expr) if allow_assignment => {
                match var_expr.deref() {
                    Identifier(name) => {
                        let (ms, value) = ms.evaluate_async(value_expr).await?;
                        Ok((ms.with_variable(container_name, Structured(structure.update_by_name(name, value))), Boolean(true)))
                    }
                    z => throw(Exact(format!("Illegal field '{}' for structure {}", z.to_code(), container_name)))
                }
            }
            // "Hello"::trim | stock.symbol
            Identifier(name) => Ok((ms, structure.get(name))),
            z => throw(Exact(format!("Illegal field '{}' for structure {}", z.to_code(), container_name)))
        }
    }

    /// Executes a postfix function call
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

    /// Executes a postfix function call
    /// #### Examples
    /// ```
    /// "hello":::left(5) //=> left("hello", 5)
    /// ```
    /// ```
    /// 3.2:::kb //=> kb(3.2)
    /// ```
    async fn exec_colon_colon_colon_async(
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
                self.exec_function_call_async(field, &vec![
                    object.to_owned()
                ]).await
            }
            // unsupported
            z => throw(Exact(format!("{} is not a function call", z.to_code())))
        }
    }

    /// Package operation execute-then-update
    fn exec_colon_colon_capture(
        &self,
        object: &Expression,
        field: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let name = pull_identifier_name(object)?;
        let model = ColonColon(object.clone().into(), field.clone().into());
        let (ms, result) = self.evaluate(&model)?;
        Ok((ms.with_variable(name.as_str(), result.clone()), result))
    }

    /// Package operation execute-then-update
    /// #### Example
    /// ##### Execution with explicit update
    /// ```
    /// stocks = []
    /// stocks = stocks::push({ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 })
    /// stocks = stocks::push({ symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 })
    /// stocks
    /// ```
    /// ##### Execution with implicit update
    /// ```
    /// stocks = []
    /// stocks<::push({ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 })
    /// stocks<::push({ symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 })
    /// stocks
    /// ```
    async fn exec_colon_colon_capture_async(
        &self,
        object: &Expression,
        field: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let name = pull_identifier_name(object)?;
        let model = ColonColon(object.clone().into(), field.clone().into());
        let (ms, result) = self.evaluate_async(&model).await?;
        Ok((ms.with_variable(name.as_str(), result.clone()), result))
    }

    /// Function execute-then-update
    fn exec_colon_colon_colon_capture(
        &self,
        object: &Expression,
        field: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let name = pull_identifier_name(object)?;
        let model = ColonColonColon(object.clone().into(), field.clone().into());
        let (ms, result) = self.evaluate(&model)?;
        Ok((ms.with_variable(name.as_str(), result.clone()), result))
    }

    /// Function execute-then-update
    /// #### Example
    /// ##### Execution with explicit update
    /// ```
    /// fn multiply(n) -> n * 2
    /// value = 8
    /// value = value:::multiply()
    /// ```
    /// ##### Execution with implicit update
    /// ```
    /// fn multiply(n) -> n * 2
    /// value = 8
    /// value<:::multiply()
    /// ```
    async fn exec_colon_colon_colon_capture_async(
        &self,
        object: &Expression,
        field: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let name = pull_identifier_name(object)?;
        let model = ColonColonColon(object.clone().into(), field.clone().into());
        let (ms, result) = self.evaluate_async(&model).await?;
        Ok((ms.with_variable(name.as_str(), result.clone()), result))
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

    /// Evaluates the `do..while` declaration
    async fn exec_do_while_async(
        &self,
        condition: &Expression,
        code: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        // evaluate the code
        let (mut ms, mut result) = self.evaluate_async(code).await?;

        // while the condition is true, execute the code
        let mut is_looping = true;
        while is_looping {
            let (ms1, result1) = ms.evaluate_async(condition).await?;
            is_looping = result1.is_true();
            if is_looping {
                let (ms2, result2) = ms1.evaluate_async(code).await?;
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
    /// for row in collections::to_table([{symbol:'ABC', price: 10.0}, ...]) ...
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
    /// for row in collections::to_table([{symbol:'ABC', price: 10.0}, ...]) ...
    /// ```
    async fn exec_for_construct_async(&self,
                                      construct: &Box<Expression>,
                                      block: &Box<Expression>,
    ) -> std::io::Result<(Self, TypedValue)> {
        match construct.deref() {
            // for item in ['apple', 'berry', ...] ...
            Condition(In(item, items)) => self.exec_for_each_async(item, items, block).await,
            // for(i = 0, i < 5, i = i + 1) ...
            TupleExpression(components) => {
                match components.as_slice() {
                    [init, Condition(cond), counter] => {
                        self.exec_for_loop_async(init, cond, counter, block).await
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
        let array = pull_array(&array_value.to_array()?)?;
        let mut index = 0;
        let mut result = Undefined;
        while index < array.len() {
            ms = ms.with_variable(ROW_ID, Number(U64Value(index as u64)));
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

    async fn exec_for_each_async(
        &self,
        item: &Expression,
        items: &Expression,
        block: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let mut ms = self.clone();
        let (_, array_value) = ms.evaluate_async(items).await?;
        let array = pull_array(&array_value.to_array()?)?;
        let mut index = 0;
        let mut result = Undefined;
        while index < array.len() {
            ms = ms.with_variable(ROW_ID, Number(U64Value(index as u64)));
            // get the value at `index`
            let value = array.get_or_else(index, Undefined);
            // deconstruct the value (e.g. (a, b) ~> a = ?, b = ?)
            ms = ms.exec_destructure(item, &value)?;
            // evaluate the block - capture the results
            let (ms1, result1) = ms.evaluate_async(block).await?;
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

    async fn exec_for_loop_async(
        &self,
        init: &Expression,
        cond: &Conditions,
        counter: &Expression,
        block: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        // evaluate the init expression
        let (mut ms, mut result) = self.evaluate_async(init).await?;

        // while the condition is true, evaluate the block
        while let (ms1, true) = ms.is_true_async(cond).await? {
            let (ms2, result1) = ms1.evaluate_async(block).await?;
            result = result1;

            // increment the loop counter
            let (ms3, _) = ms2.evaluate_async(counter).await?;
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
    /// fn add(a, b) -> a + b
    /// fn inverse(a) -> 1.0 / a
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
                StructureExpression(items) => items.iter().map(|(_, v)| v.clone()).collect(),
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

    fn build_function_arguments(
        &self,
        params: Vec<Parameter>,
        args: Vec<TypedValue>,
    ) -> Self {
        if params.len() > args.len() {
            println!("Argument mismatch (params: {}, args: {:?})", Parameter::render(&params), args);
            //assert_eq!(params.len(), args.len());
        }
        params.iter().zip(args.iter())
            .fold(self.to_owned(), |ms, (c, v)|
                ms.with_variable(c.get_name(), v.to_owned()))
    }

    fn is_alias_function(&self, name: &str, args: &[Expression]) -> bool {
        self.aliases.iter().position(|alias| match alias {
            AliasCalls::FunctionCall(my_name, my_params, _) =>
                my_name == name && my_params.len() == args.len(),
            _ => false,
        }).is_some()
    }

    /// Executes a function call
    /// #### Examples
    /// ```
    /// factorial(5)
    /// ```
    fn exec_function_call(&self,
                          fx: &Expression,
                          args: &[Expression],
    ) -> std::io::Result<(Self, TypedValue)> {
        match find_name(fx) {
            Some(name) if self.is_alias_function(name.as_str(), args) =>
                self.exec_alias_function_call(name.as_str(), args),
            _ => {
                let (ms, args) = self.evaluate_as_vec(args)?;
                match ms.evaluate(fx)? {
                    (ms, Function { params, body, .. }) =>
                        ms.build_function_arguments(params, args)
                            .evaluate(&body),
                    (ms, PackageFunction(pf)) => pf.evaluate(ms, args),
                    (_, z) => throw(TypeMismatch(FunctionExpected(z.to_code()))),
                }
            }
        }
    }

    /// Executes a function call
    /// #### Examples
    /// ```
    /// factorial(5)
    /// ```
    async fn exec_function_call_async(&self,
                                fx: &Expression,
                                args: &[Expression],
    ) -> std::io::Result<(Self, TypedValue)> {
        match find_name(fx) {
            Some(name) if self.is_alias_function(name.as_str(), args) =>
                self.exec_alias_function_call_async(name.as_str(), args).await,
            _ => {
                let (ms, args) = self.evaluate_as_vec(args)?;
                match ms.evaluate_async(fx).await? {
                    (ms, Function { params, body, .. }) =>
                        ms.build_function_arguments(params, args)
                            .evaluate_async(&body).await,
                    (ms, PackageFunction(pf)) => pf.evaluate_async(ms, args).await,
                    (_, z) => throw(TypeMismatch(FunctionExpected(z.to_code()))),
                }
            }
        }
    }

    fn exec_get_variable(
        &self,
        name: &str,
    ) -> std::io::Result<(Self, TypedValue)> {
        let result = match self.get(name) {
            Some(value) => value,
            // is it a type?
            None if TypeEngine::is_type_name(name) =>
                Kind(TypeEngine::decipher_model(&Identifier(name.into()))?),
            // is it an alias?
            None => return self.exec_alias_identifier_call(name),
        };
        Ok((self.clone(), result))
    }

    async fn exec_get_variable_async(
        &self,
        name: &str,
    ) -> std::io::Result<(Self, TypedValue)> {
        let result = match self.get(name) {
            Some(value) => value,
            // is it a type?
            None if TypeEngine::is_type_name(name) =>
                Kind(TypeEngine::decipher_model(&Identifier(name.into()))?),
            // is it an alias?
            None => return self.exec_alias_identifier_call_async(name).await,
        };
        Ok((self.clone(), result))
    }

    /// Evaluates an if-then-else expression
    fn exec_if_then_else(
        &self,
        condition: &Box<Expression>,
        a: &Expression,
        b: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
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

    /// Evaluates an if-then-else expression
    async fn exec_if_then_else_async(
        &self,
        condition: &Box<Expression>,
        a: &Expression,
        b: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let ms0 = self.to_owned();
        let (machine, result) = ms0.evaluate_async(condition).await?;
        if result.is_ok() {
            machine.evaluate_async(a).await
        } else if let Some(b) = b {
            machine.evaluate_async(b).await
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
        let (ms, script_path) = self.evaluate_as_string(path)?;
        // read the script file contents into the string
        let mut file = File::open(script_path)?;
        let mut script_code = String::new();
        file.read_to_string(&mut script_code)?;
        // compile and execute the string (script code)
        let opcode = Compiler::build(script_code.as_str())?;
        ms.evaluate(&opcode)
    }

    async fn exec_include_async(
        &self,
        path: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        // evaluate the script path
        let (ms, script_path) = self.evaluate_as_string_async(path).await?;
        // read the script file contents into the string
        let mut file = File::open(script_path)?;
        let mut script_code = String::new();
        file.read_to_string(&mut script_code)?;
        // compile and execute the string (script code)
        let opcode = Compiler::build(script_code.as_str())?;
        ms.evaluate_async(&opcode).await
    }

    /// Evaluates a user-defined method or property
    /// #### Examples
    /// ##### Evaluate a user-defined method
    /// ```
    /// user_dao.create(item)
    /// ```
    /// ##### Evaluate a user-defined property
    /// ```
    /// user_vo.name
    /// ```
    /// ##### Mutate a user-defined property
    /// ```
    /// user_vo.name = "Fred Jenkins"
    /// ```
    fn exec_infix(
        &self,
        container: &Expression,
        field: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        match container {
            Identifier(container_name) =>
                match self.get(container_name.as_str()) {
                    None => throw(TypeMismatch(IdentifierExpected(container_name.into()))),
                    Some(ErrorValue(err)) => throw(err),
                    Some(Structured(structure)) =>
                        self.exec_structure_method_or_property(&container_name, structure, field, true),
                    Some(z) => throw(TypeMismatch(IdentifierExpected(z.to_string()))),
                }
            z => throw(Exact(format!("Illegal field '{}' for structure {}", z, container)))
        }
    }

    /// Evaluates a user-defined method or property
    /// #### Examples
    /// ##### Evaluate a user-defined method
    /// ```
    /// user_dao.create(item)
    /// ```
    /// ##### Evaluate a user-defined property
    /// ```
    /// user_vo.name
    /// ```
    /// ##### Mutate a user-defined property
    /// ```
    /// user_vo.name = "Fred Jenkins"
    /// ```
    async fn exec_infix_async(
        &self,
        container: &Expression,
        field: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        match container {
            Identifier(container_name) =>
                match self.get(container_name.as_str()) {
                    None => throw(TypeMismatch(IdentifierExpected(container_name.into()))),
                    Some(ErrorValue(err)) => throw(err),
                    Some(Structured(structure)) =>
                        self.exec_structure_method_or_property_async(
                            &container_name, structure, field, true
                        ).await,
                    Some(z) => throw(TypeMismatch(IdentifierExpected(z.to_string()))),
                }
            z => throw(Exact(format!("Illegal field '{}' for structure {}", z, container)))
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

    /// Evaluates the index of a collection (array, string, structure or table)
    fn exec_index_of_collection(
        &self,
        collection_expr: &Expression,
        index_expr: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, index) = self.evaluate(index_expr)?;
        let (ms, collection) = ms.evaluate(collection_expr)?;
        let value = Self::element_at(collection, index)?;
        Ok((ms, value))
    }

    /// Evaluates the index of a collection (array, string, structure or table)
    async fn exec_index_of_collection_async(
        &self,
        collection_expr: &Expression,
        index_expr: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, index) = self.evaluate_async(index_expr).await?;
        let (ms, collection) = ms.evaluate_async(collection_expr).await?;
        let value = Self::element_at(collection, index)?;
        Ok((ms, value))
    }
    
    fn element_at(collection: TypedValue, index: TypedValue) -> std::io::Result<TypedValue> {
        let value = match collection {
            ArrayValue(array) => {
                elem_at("Array", array, index,
                        |a| Ok(a.len()), |a, i| Ok(a[i].clone()))?
            }
            ByteStringValue(bytes) => {
                elem_at("Byte", bytes, index,
                        |bv| Ok(bv.len()), |bv, i| Ok(Number(U8Value(bv[i].clone()))))?
            }
            EnumValue(pos, params) =>
                match pos {
                    None => Undefined,
                    Some(n) => {
                        let value = params[n as usize].get_name().to_string();
                        elem_at("Char", value, index,
                                |s| Ok(s.len()), |s, i| Ok(CharValue(s.chars().nth(i).unwrap())))?
                    }
                }
            StringValue(string) => {
                elem_at("Char", string, index,
                        |s| Ok(s.chars().count()), |s, i| Ok(CharValue(s.chars().nth(i).unwrap())))?
            }
            Structured(structure) => {
                elem_at("Property", structure, index,
                        |s| Ok(s.len()), |s, i| Ok(s.get_values()[i].clone()))?
            }
            TableValue(df) => {
                elem_at("Row", df, index, |df| df.len(), |df, id| df.read_one(id)
                    .map(|row| match row {
                        Some(row) => Structured(Firm(row, df.get_parameters())),
                        None => Structured(Firm(Row::create(id, df.get_columns()), df.get_parameters()))
                    }))?
            }
            TupleValue(values) => {
                elem_at("Value", values, index,
                        |tv| Ok(tv.len()), |tv, i| Ok(tv[i].clone()))?
            }
            other =>
                ErrorValue(TypeMismatch(UnsupportedType(RuntimeResolvedType, other.get_type())))
        };
        Ok(value)
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

    async fn exec_like_async(
        &self,
        text: &Expression,
        pattern: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        use regex::Regex;
        let (ms, text_v) = self.evaluate_async(text).await?;
        let (ms, pattern_v) = ms.evaluate_async(pattern).await?;
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
    
    /// Evaluates a list system files expression
    /// #### Examples
    /// ##### List files in the current directory
    /// ```
    /// ls
    /// ```
    /// ##### List subdirectories in the current directory
    /// ```
    /// ls where is_directory is true
    /// ```
    /// ##### List all files in a specific directory
    /// ```
    /// ls("../Downloads")
    /// ```
    fn exec_ls(
        &self,
        maybe_path: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, path) = self.evaluate_or_else(maybe_path, |_| StringValue(".".into()))?;
        IoPkg::do_io_list_files(ms, &path)
    }

    /// Evaluates a list system files expression
    /// #### Examples
    /// ##### List files in the current directory
    /// ```
    /// ls
    /// ```
    /// ##### List subdirectories in the current directory
    /// ```
    /// ls where is_directory is true
    /// ```
    /// ##### List all files in a specific directory
    /// ```
    /// ls("../Downloads")
    /// ```
    async fn exec_ls_async(
        &self,
        maybe_path: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, path) = self.evaluate_or_else_async(maybe_path, |_| StringValue(".".into())).await?;
        IoPkg::do_io_list_files(ms, &path)
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
        cases: &[Expression],
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
        ops: &[Expression],
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
                z => ErrorValue(TypeMismatch(StructIsnt(name.to_string(), z.to_code())))
            });
        Ok((self.with_variable(name, result), Boolean(true)))
    }

    fn exec_module_structure(
        &self,
        name: &str,
        ops: &[Expression],
    ) -> std::io::Result<(Self, TypedValue)> {
        match self.get(name) {
            Some(Structured(structure)) => self.exec_module_structure_impl(name, structure, ops),
            Some(v) => throw(TypeMismatch(StructIsnt(name.into(), v.to_code()))),
            None => self.exec_module_alone(name, ops)
        }
    }

    fn exec_module_structure_impl(
        &self,
        name: &str,
        structure: Structures,
        ops: &[Expression],
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
                z => ErrorValue(TypeMismatch(StructIsnt(name.to_string(), z.to_code())))
            });
        match result {
            ErrorValue(err) => throw(err),
            result => Ok((self.with_variable(name, result), Boolean(true)))
        }
    }
    
    /// evaluates the specified [Expression]; returning a negative [TypedValue] result.
    fn exec_negate(&self, expr: &Expression) -> std::io::Result<(Self, TypedValue)> {
        self.evaluate(expr).map(|(ms, result)| (ms, result.neg()))
    }

    fn exec_one_time(&self, expr: &Expression, uid: u128) -> std::io::Result<(Self, TypedValue)> {
        if connections::one_time::if_not_triggered(&uid)? {
            self.evaluate(expr).map(|(ms, _)| (ms, Boolean(true)))
        }
        else {
            Ok((self.clone(), Boolean(false)))
        }
    }

    async fn exec_one_time_async(&self, expr: &Expression, uid: u128) -> std::io::Result<(Self, TypedValue)> {
        if connections::one_time::if_not_triggered(&uid)? {
            self.evaluate_async(expr).await.map(|(ms, _)| (ms, Boolean(true)))
        }
        else {
            Ok((self.clone(), Boolean(false)))
        }
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
    /// let stocks = Table(
    ///    symbol: String(8),
    ///    exchange: String(8),
    ///    history: Table(last_sale: f64, processed_time: DateTime)
    /// )::new::save_as("machine.examples.stocks")
    ///
    /// { symbol: "BIZ", exchange: "NYSE", history: [] } ~> stocks
    /// 
    /// // get a reference to the embedded table
    /// let history = &stocks(0, 2)
    ///
    /// { last_sale: 11.67, processed_time: DateTime::new() } ~> history
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
                let (ms, df) = self.evaluate_as_dataframe(fx)?;
                let (ms, coords) = ms.get_referenced_coordinates(args)?;
                let ref_df = ms.get_referenced_dataframe(&df, coords)?;
                Ok((ms, TableValue(ref_df)))
            }
            x => throw(TypeMismatch(FunctionExpected(x.to_code())))
        }
    }

    /// Returns a reference to an embedded table
    /// ```
    /// // create a table with an embedded table
    /// let stocks = Table(
    ///    symbol: String(8),
    ///    exchange: String(8),
    ///    history: Table(last_sale: f64, processed_time: DateTime)
    /// )::new::save_as("machine.examples.stocks")
    ///
    /// { symbol: "BIZ", exchange: "NYSE", history: [] } ~> stocks
    ///
    /// // get a reference to the embedded table
    /// let history = &stocks(0, 2)
    /// 
    /// { last_sale: 11.67, processed_time: DateTime::new() } ~> history
    /// stocks
    ///
    /// // { symbol: "BIZ", exchange: "NYSE", history: [{ last_sale: 11.67, processed_time: 2025-01-13T03:25:47.350Z }] }
    /// ```
    async fn exec_referenced_async(
        &self,
        expr: &Expression
    ) -> std::io::Result<(Self, TypedValue)> {
        match expr {
            FunctionCall { fx, args } => {
                let (ms, df) = self.evaluate_as_dataframe_async(fx).await?;
                let (ms, coords) = ms.get_referenced_coordinates(args)?;
                let ref_df = ms.get_referenced_dataframe(&df, coords)?;
                Ok((ms, TableValue(ref_df)))
            }
            x => throw(TypeMismatch(FunctionExpected(x.to_code())))
        }
    }

    fn get_referenced_coordinates(
        &self, 
        args: &[Expression]
    ) -> std::io::Result<(Self, Vec<usize>)> {
        let (_, coords_tuple) = self.evaluate(&TupleExpression(args.to_vec()))?;
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
            let df = row.get(column_id).to_dataframe()?;
            recurse_df(&df, coords)
        }

        // recursively resolve the embedded dataframe
        recurse_df(host, coords.iter().rev().map(|n| *n).collect::<Vec<_>>()) 
    }

    fn exec_scenario(
        &self,
        title: &Box<Expression>,
        inherits: &Option<String>,
        verifications: &[Expression]
    ) -> std::io::Result<(Self, TypedValue)> {
        TestEngine::add_feature(self, title, &vec![
            Scenario {
                title: title.clone(),
                inherits: inherits.clone(),
                verifications: verifications.to_vec()
            }
        ])
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
    async fn exec_set_variables_async(
        &self,
        src: &Expression,
        dest: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, _) = self.exec_set_variables_expr_async(src, dest).await?;
        Ok((ms, Boolean(true)))
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
            // stock.last_sale = 24.11
            Infix(container, variable) => {
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
    async fn exec_set_variables_expr_async(
        &self,
        src: &Expression,
        dest: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, result) = self.evaluate_async(dest).await?;
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
                        let (ms2, _) = ms1.exec_whenever_observations_async().await?;
                        Ok((ms2, result1))
                    }
                    z => throw(Exact(format!("Expected an Array near {}", z)))
                }
            // stock.last_sale = 24.11
            Infix(container, variable) => {
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
                        let (ms2, _) = ms1.exec_whenever_observations_async().await?;
                        Ok((ms2, result1))
                    }
                    z => throw(Exact(format!("Expected a Tuple near {}", z)))
                }
            // a := 7
            Identifier(name) => {
                let ms1 = ms.set(name.as_str(), result.clone());
                let (ms2, _) = ms1.exec_whenever_observations_async().await?;
                Ok((ms2, result))
            }
            z => throw(Exact(format!("Expected an Array, Tuple or variable near {}", z)))
        }
    }

    fn exec_structure_soft(
        &self,
        items: &Vec<(String, Expression)>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let mut elems = Vec::with_capacity(items.len());
        for (name, expr) in items {
            let (_, value) = self.evaluate(expr)?;
            elems.push((name.to_string(), value))
        }
        Ok((self.to_owned(), Structured(Soft(SoftStructure::from_tuples(elems)))))
    }

    async fn exec_structure_soft_async(
        &self,
        items: &Vec<(String, Expression)>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let mut elems = Vec::with_capacity(items.len());
        for (name, expr) in items {
            let (_, value) = self.evaluate_async(expr).await?;
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

    async fn exec_throw_async(
        &self,
        expr: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, message) = self.evaluate_async(expr).await?;
        Ok((ms, ErrorValue(Exact(message.unwrap_value()))))
    }
    
    /// Defines a tuple expression
    /// ex: (1, 2, 3)
    fn exec_tuple(
        &self,
        ops: &[Expression],
    ) -> std::io::Result<(Self, TypedValue)> {
        self.evaluate_as_vec(ops).map(|(ms, values)| (ms, TupleValue(values)))
    }

    /// Defines a tuple expression
    /// ex: (1, 2, 3)
    async fn exec_tuple_async(
        &self,
        ops: &[Expression],
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, values) = self.evaluate_as_vec_async(ops).await?;
        Ok((ms, TupleValue(values)))
    }

    /// Produces an aggregate [Machine] instance containing
    /// the specified imports
    fn exec_use(
        &self,
        package_name: &str,
        selection: &Vec<String>,
    ) -> (Self, TypedValue) {
        let ms = self.to_owned();
        match PackageOps::lookup_by_package_name(package_name) {
            None =>
                match ms.get(package_name) {
                    None => (ms, ErrorValue(PackageNotFound(package_name.into()))),
                    Some(component) =>
                        match component {
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
            Some(ops) => {
                let ms = ops.iter().fold(self.clone(), |ms, op| {
                    let name = &op.get_name();
                    if selection.is_empty() || selection.contains(name) {
                        ms.with_variable(name, PackageFunction(op.clone()))
                    } else {
                        ms
                    }
                });
                (ms, Boolean(true))
            }
        }
    }

    fn exec_uses(&self, ops: &Vec<UseOps>) -> std::io::Result<(Self, TypedValue)> {
        let result = ops.iter().fold(
            (self.to_owned(), Undefined),
            |(ms, _tv), iop| match iop {
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
        uuid: u128,
    ) -> std::io::Result<(Self, TypedValue)> {
        const LABEL: &str = "__yield__";
        let full_prop_name = format!("{}{}", LABEL, u128_to_uuid(uuid));
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

    /// Evaluates the `yield` declaration
    async fn exec_yield_async(
        &self,
        expr: &Expression,
        uuid: u128,
    ) -> std::io::Result<(Self, TypedValue)> {
        const LABEL: &str = "__yield__";
        let full_prop_name = format!("{}{}", LABEL, u128_to_uuid(uuid));
        let prop_name = full_prop_name.as_str();
        let (mut ms, value) = self.evaluate_async(expr).await?;
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

    async fn exec_zip_async(
        &self,
        a: &Expression,
        b: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, aa) = self.evaluate_async(a).await?;
        let (ms, bb) = ms.evaluate_async(b).await?;
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
                                Ok((ms1, _result)) => ms = ms1,
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

    /// Evaluates the `whenever` observations (triggers) 
    async fn exec_whenever_observations_async(&self) -> std::io::Result<(Self, TypedValue)> {
        let mut ms = self.clone();
        for observation in &self.observations {
            match observation {
                VariableObservation { condition, code } => {
                    match ms.is_true_async(condition).await.map(|(_, yes)| yes) {
                        Ok(true) => {
                            match ms.evaluate_async(code).await {
                                Ok((ms1, _result)) => ms = ms1,
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

    /// Evaluates the `while` declaration
    async fn exec_while_async(
        &self,
        condition: &Expression,
        code: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let mut machine = self.to_owned();
        let mut is_looping = true;
        let mut outcome = Undefined;
        while is_looping {
            let (m, result) = machine.evaluate_async(condition).await?;
            machine = m;
            is_looping = result.is_true();
            if is_looping {
                let (m, result) = machine.evaluate_async(code).await?;
                machine = m;
                outcome = result;
            }
        }
        Ok((machine, outcome))
    }

    /// Evaluates a resource then automatically closes it after use
    /// #### Example
    /// ```
    /// blobs::create("builtins.blob.append_blocking") with bs -> {
    ///     let id = bs::append("Hello World")
    ///     bs::read(id)
    /// }
    /// ```
    fn exec_with(
        &self,
        resource: &Expression,
        code: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, my_resource) = self.evaluate(resource)?;
        let (ms, fx) = ms.evaluate(code)?;
        let (ms, result) =  ms.evaluate(&FunctionCall {
            fx: Literal(fx.clone()).into(),
            args: vec![
                Literal(my_resource.clone()),
            ],
        })?;
        let _closed = Self::resource_close(my_resource).ok();
        Ok((ms, result))
    }

    /// Evaluates a resource then automatically closes it after use
    async fn exec_with_async(
        &self,
        resource: &Expression,
        code: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, my_resource) = self.evaluate_async(resource).await?;
        let (ms, fx) = ms.evaluate_async(code).await?;
        let (ms, result) =  ms.evaluate_async(&FunctionCall {
            fx: Literal(fx.clone()).into(),
            args: vec![
                Literal(my_resource.clone()),
            ],
        }).await?;
        let _closed = Self::resource_close_async(my_resource).await.ok();
        Ok((ms, result))
    }

    fn resource_close(resource: TypedValue) -> std::io::Result<bool> {
        match resource {
            Connection(conn) => conn.close(),
            other => throw(Exact(format!("Connection expected near {}", other)))
        }
    }

    async fn resource_close_async(resource: TypedValue) -> std::io::Result<bool> {
        match resource {
            Connection(conn) => conn.close_async().await,
            other => throw(Exact(format!("Connection expected near {}", other)))
        }
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

    /// evaluates expression `a` then applies function `f`. ex: f(a)
    async fn eval_inline_1_async(
        &self,
        a: &Expression,
        f: fn(TypedValue) -> TypedValue,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, aa) = self.evaluate_async(a).await?;
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

    /// evaluates expressions `a` and `b` then applies function `f`. ex: f(a, b)
    async fn eval_inline_2_async(
        &self,
        a: &Expression,
        b: &Expression,
        f: fn(TypedValue, TypedValue) -> TypedValue,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, aa) = self.evaluate_async(a).await?;
        let (machine, bb) = machine.evaluate_async(b).await?;
        Ok((machine, f(aa, bb)))
    }

    /// returns a variable by name
    pub fn get(&self, name: &str) -> Option<TypedValue> {
        self.variables.get(name).cloned()
    }

    pub fn get_aliases(&self) -> &Vec<AliasCalls> {
        &self.aliases
    }

    pub fn get_observations(&self) -> &Vec<Observations> {
        &self.observations
    }

    /// returns a variable by name or the default value
    pub fn get_or_else(&self, name: &str, default: fn() -> TypedValue) -> TypedValue {
        self.get(name).unwrap_or(default())
    }

    pub fn get_variables(&self) -> &im::HashMap<String, TypedValue> {
        &self.variables
    }

    fn get_variables_names(items: &[Expression]) -> std::io::Result<Vec<String>> {
        let mut variables = vec![];
        for item in items {
            variables.push(pull_identifier_name(item)?);
        }
        Ok(variables)
    }

    fn set_variables(&self, names: Vec<String>, values: Vec<TypedValue>, f: fn(Vec<TypedValue>) -> TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        let ms = names.iter().zip(values.iter())
            .fold(self.clone(), |ms, (name, value)| ms.set(name, value.clone()));
        Ok((ms, f(values)))
    }
    
    pub fn set(&self, name: &str, value: TypedValue) -> Self {
        let mut ms = self.clone();
        ms.variables = ms.variables.update(name.to_owned(), value);
        ms
    }

    pub fn with_alias(&self, alias: AliasCalls) -> Self {
        let mut ms = self.clone();
        let index_maybe = match &alias {
            AliasCalls::IdentifierCall(name, _) =>
                ms.aliases.iter().position(|alias| match alias {
                    AliasCalls::IdentifierCall(my_name, _) if my_name == name => true,
                    _ => false,
                }),
            AliasCalls::FunctionCall(name, params, _) =>
                ms.aliases.iter().position(|alias| match alias {
                    AliasCalls::FunctionCall(my_name, my_params, _) =>
                        my_name == name && my_params.len() == params.len(),
                    _ => false,
                })
        };
        match index_maybe {
            Some(index) => ms.aliases[index] = alias,
            None =>ms.aliases.push(alias)
        }
        ms
    }

    pub fn with_observer_variable(
        &self, 
        condition: Conditions, 
        code: Expression
    ) -> Self {
        let mut ms = self.clone();
        ms.observations.push(VariableObservation { condition, code });
        ms
    }

    pub fn with_row(&self, columns: &Vec<Column>, row: &Row) -> Self {
        row.get_values().iter().zip(columns.iter())
            .fold(self.clone(), |ms, (v, c)| ms.with_variable(c.get_name(), v.to_owned()))
            .with_variable(ROW_ID, Number(I64Value(row.get_id() as i64)))
    }

    pub fn with_variable(&self, name: &str, value: TypedValue) -> Self {
        let mut ms = self.clone();
        ms.variables = ms.variables.update(name.to_owned(), value);
        ms
    }

    pub fn without_temp_variables(&self) -> Self {
        let mut ms = self.clone();
        ms.variables = ms.variables
            .into_iter()
            .filter(|(k, _)| !k.starts_with("__"))
            .collect::<im::HashMap<String, TypedValue>>();
        ms
    }

    pub fn without_variable(&self, name: &str) -> Self {
        let mut ms = self.clone();
        ms.variables.remove(name);
        ms
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
    use crate::test_util::*;

    #[test]
    fn encode_decode() {
        let machine = Machine::new();
        machine.with_variable("x", Number(I64Value(1)));
        machine.with_variable("y", Number(I64Value(2)));
        machine.with_variable("z", Number(I64Value(3)));

        let bytes = bincode::serialize(&machine).unwrap();
        let decoded = bincode::deserialize::<Machine>(&bytes[..]).unwrap();
        assert_eq!(machine.get_variables(), decoded.get_variables());
    }

    #[actix::test]
    async fn test_array_declaration() {
        let models = vec![Literal(Number(F64Value(3.25))), TRUE, FALSE, NULL, UNDEFINED];
        assert_eq!(models.iter().map(|e| e.to_code()).collect::<Vec<_>>(), vec![
            "3.25", "true", "false", "null", "undefined",
        ]);

        let (_, array) = Machine::empty().evaluate_as_array_async(&models).await.unwrap();
        assert_eq!(array, ArrayValue(Array::from(vec![
            Number(F64Value(3.25)), Boolean(true), Boolean(false), Null, Undefined
        ])));
    }

    #[actix::test]
    async fn test_aliases() {
        let model = NamedValue(
            "symbol".to_string(),
            Box::new(Literal(StringValue("ABC".into()))),
        );
        assert_eq!(model.to_code(), "symbol: \"ABC\"");

        let machine = Machine::empty();
        let (machine, tv) = machine.evaluate_async(&model).await.unwrap();
        assert_eq!(tv, StringValue("ABC".into()));
        assert_eq!(machine.get("symbol"), Some(StringValue("ABC".into())));
    }

    #[actix::test]
    async fn test_column_set() {
        let model = Parameters(make_quote_parameters());
        assert_eq!(model.to_code(), "symbol: String(8), exchange: String(8), last_sale: f64");

        let machine = Machine::empty()
            .with_variable("symbol", StringValue("ABC".into()))
            .with_variable("exchange", StringValue("NYSE".into()))
            .with_variable("last_sale", Number(F64Value(12.66)));
        let (_, result) = machine.evaluate_async(&model).await.unwrap();
        assert_eq!(result, ArrayValue(Array::from(vec![
            StringValue("ABC".into()),
            StringValue("NYSE".into()),
            Number(F64Value(12.66)),
        ])));
    }

    #[actix::test]
    async fn test_divide() {
        let machine = Machine::empty().with_variable("x", Number(I64Value(50)));
        let model = Divide(Box::new(Identifier("x".into())), Box::new(Literal(Number(I64Value(7)))));
        assert_eq!(model.to_code(), "x / 7");

        let (machine, result) = machine.evaluate_async(&model).await.unwrap();
        assert_eq!(result, Number(I64Value(7)));
        assert_eq!(machine.get("x"), Some(Number(I64Value(50))));
        assert_eq!(model.infer_type(), NumberType(I64Kind))
    }

    #[actix::test]
    async fn test_index_of_array() {
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
        let (_, result) = machine.evaluate_async(&model).await.unwrap();
        assert_eq!(result, Number(I64Value(7)))
    }

    #[actix::test]
    async fn test_variables() {
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

        #[actix::test]
        async fn test_assumed_boolean_contains() {
            let code = Compiler::build(r#"
                scenario_name::contains("JSON")
            "#).unwrap();

            let machine = Machine::empty()
                .with_variable("scenario_name", StringValue("Compare JSON contents".into()));

            let model = AssumedBoolean(code.into());
            let (_, result) = machine.is_true_async(&model).await.unwrap();
            assert_eq!(result, true);
        }

        #[actix::test]
        async fn test_assumed_boolean_false() {
            let machine = Machine::empty();
            let model = AssumedBoolean(Box::new(FALSE));
            let (_, result) = machine.is_true_async(&model).await.unwrap();
            assert_eq!(result, false);
        }

        #[actix::test]
        async fn test_assumed_boolean_true() {
            let machine = Machine::empty();
            let model = AssumedBoolean(Box::new(TRUE));
            let (_, result) = machine.is_true_async(&model).await.unwrap();
            assert_eq!(result, true);
        }

        #[actix::test]
        async fn test_if_else_flow_1() {
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
            let (_, result) = machine.evaluate_async(&model).await.unwrap();
            assert_eq!(result, StringValue("No".into()));

            let machine = Machine::empty().with_variable("num", Number(I64Value(37)));
            let (_, result) = machine.evaluate_async(&model).await.unwrap();
            assert_eq!(result, StringValue("Yes".into()));
        }

        #[actix::test]
        async fn test_if_else_flow_2() {
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
            let (_, result) = machine.evaluate_async(&model).await.unwrap();
            assert_eq!(result, StringValue("Yes".into()));

            let machine = Machine::empty().with_variable("num", Number(I64Value(99)));
            let (_, result) = machine.evaluate_async(&model).await.unwrap();
            assert_eq!(result, StringValue("No".into()));
        }

        #[actix::test]
        async fn test_while_loop() {
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
            let (machine, _) = machine.evaluate_async(&model).await.unwrap();
            assert_eq!(machine.get("num"), Some(Number(I64Value(5))))
        }
    }

    /// Function tests
    #[cfg(test)]
    mod function_tests {
        use super::*;
        use crate::data_types::DataType::RuntimeResolvedType;

        #[actix::test]
        async fn test_anonymous_function() {
            // define a function call: (n -> n + 5)(3)
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
            let (_machine, result) = Machine::empty()
                .with_variable("n", Number(I64Value(3)))
                .evaluate_async(&model).await
                .unwrap();
            assert_eq!(result, Number(I64Value(8)));
            assert_eq!(model.to_code(), "((n: i64): i64 -> n + 5)(3)")
        }

        #[actix::test]
        async fn test_named_function() {
            // define a function: (a, b) -> a + b
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
                returns: RuntimeResolvedType,
            };

            // publish the function in scope: fn add(a, b) -> a + b
            let machine = Machine::empty();
            let (machine, result) = machine.evaluate_scope_async(&vec![
                SetVariables(
                    Identifier("add".into()).into(),
                    Literal(fx.to_owned()).into()
                )
            ]).await.unwrap();
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
            let (_machine, result) = machine
                .evaluate_async(&model).await
                .unwrap();
            assert_eq!(result, Number(I64Value(5)));
            assert_eq!(model.to_code(), "((a: i64, b: i64) -> a + b)(2, 3)")
        }

        #[ignore]
        #[actix::test]
        async fn test_function_recursion() {
            // f = ((n: i64) -> if(n <= 1) 1 else n * f(n - 1))
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
            let (_machine, result) = machine.evaluate_async(&FunctionCall {
                fx: Box::new(Identifier("f".into())),
                args: vec![
                    Literal(Number(I64Value(5)))
                ],
            }).await.unwrap();

            println!("result: {}", result);
            assert_eq!(result, Number(I64Value(120)));
        }
    }
    
}