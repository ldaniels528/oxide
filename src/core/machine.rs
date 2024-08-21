////////////////////////////////////////////////////////////////////
// state machine module
////////////////////////////////////////////////////////////////////

use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::Read;
use std::ops::Deref;
use std::process::Output;

use log::{error, info};
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
use crate::expression::Expression::{ArrayLiteral, ColumnSet, From, Literal, Ns, Perform, Variable, Via};
use crate::expression::Infrastructure::Declare;
use crate::expression::MutateTarget::{IndexTarget, TableTarget};
use crate::file_row_collection::FileRowCollection;
use crate::model_row_collection::ModelRowCollection;
use crate::namespaces::Namespace;
use crate::row_collection::RowCollection;
use crate::rows::Row;
use crate::server::ColumnJs;
use crate::structure::Structure;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::*;

/// represents an Oxide executable instruction (opcode)
pub type OpCode = fn(Machine) -> std::io::Result<Machine>;

/// Represents the state of the machine.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Machine {
    stack: Vec<TypedValue>,
    variables: HashMap<String, TypedValue>,
}

impl Machine {

    ////////////////////////////////////////////////////////////////
    // static methods
    ////////////////////////////////////////////////////////////////

    /// creates a new state machine
    pub fn construct(stack: Vec<TypedValue>, variables: HashMap<String, TypedValue>) -> Self {
        Self { stack, variables }
    }

    fn expand(machine: Self, expr: &Option<Box<Expression>>) -> std::io::Result<(Self, TypedValue)> {
        match expr {
            Some(item) => machine.evaluate(item),
            None => Ok((machine, TypedValue::Undefined))
        }
    }

    fn expand_vec(machine: Self, expr: &Option<Vec<Expression>>) -> std::io::Result<(Self, TypedValue)> {
        match expr {
            Some(items) => machine.evaluate_array(items),
            None => Ok((machine, TypedValue::Undefined))
        }
    }

    /// creates a new empty state machine
    pub fn new() -> Self {
        Self::construct(Vec::new(), HashMap::new())
    }

    fn orchestrate_rc(
        machine: Self,
        table: TypedValue,
        f: fn(Self, Box<dyn RowCollection>) -> std::io::Result<(Self, TypedValue)>,
    ) -> std::io::Result<(Self, TypedValue)> {
        use TypedValue::*;
        match table {
            TableValue(mrc) => f(machine, Box::new(mrc)),
            TableNs(path) => {
                let ns = Namespace::parse(path.as_str())?;
                let frc = FileRowCollection::open(&ns)?;
                f(machine, Box::new(frc))
            }
            x =>
                Ok((machine, ErrorValue(format!("Type mismatch: expected an iterable near {}", x))))
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
            TableValue(mrc) => {
                let mut df = DataFrame::from_row_collection(Box::new(mrc));
                f(machine, &mut df, fields, values, condition, limit)
            }
            TableNs(path) => {
                let ns = Namespace::parse(path.as_str())?;
                let mut df = DataFrame::load(ns)?;
                f(machine, &mut df, fields, values, condition, limit)
            }
            x =>
                Ok((machine, ErrorValue(format!("Type mismatch: expected an iterable near {}", x))))
        }
    }

    pub fn error_expr(message: impl Into<String>, expr: &Expression) -> TypedValue {
        ErrorValue(format!("{} near {}", message.into(), expr.to_code()))
    }

    pub fn error_unexpected(expected_type: impl Into<String>, value: &TypedValue) -> TypedValue {
        ErrorValue(format!("Expected a(n) {}, but got {:?}", expected_type.into(), value))
    }

    fn split(row: &Row) -> (Vec<Expression>, Vec<Expression>) {
        let my_fields = row.get_columns().iter()
            .map(|tc| Variable(tc.get_name().to_string()))
            .collect::<Vec<Expression>>();
        let my_values = row.get_values().iter()
            .map(|v| Literal(v.clone()))
            .collect::<Vec<Expression>>();
        (my_fields, my_values)
    }

    ////////////////////////////////////////////////////////////////
    // instance methods
    ////////////////////////////////////////////////////////////////

    /// evaluates the specified [Expression]; returning a [TypedValue] result.
    pub fn evaluate(&self, expression: &Expression) -> std::io::Result<(Self, TypedValue)> {
        use crate::expression::Expression::*;
        match expression {
            And(a, b) =>
                self.expand2(a, b, |aa, bb| aa.and(&bb).unwrap_or(Undefined)),
            ArrayLiteral(items) => self.evaluate_array(items),
            AsValue(name, expr) => {
                let (machine, tv) = self.evaluate(expr)?;
                Ok((machine.with_variable(name, tv.clone()), tv))
            }
            Between(a, b, c) =>
                self.expand3(a, b, c, |aa, bb, cc| Boolean((aa >= bb) && (aa <= cc))),
            Betwixt(a, b, c) =>
                self.expand3(a, b, c, |aa, bb, cc| Boolean((aa >= bb) && (aa < cc))),
            BitwiseAnd(a, b) =>
                self.expand2(a, b, |aa, bb| aa & bb),
            BitwiseOr(a, b) =>
                self.expand2(a, b, |aa, bb| aa | bb),
            BitwiseXor(a, b) =>
                self.expand2(a, b, |aa, bb| aa ^ bb),
            CodeBlock(ops) => self.evaluate_scope(ops),
            ColumnSet(columns) => {
                let machine = self.clone();
                let values = columns.iter()
                    .map(|c| machine.variables.clone().get(c.get_name()).map(|c| c.clone())
                        .unwrap_or(Undefined))
                    .collect::<Vec<TypedValue>>();
                Ok((machine, Array(values)))
            }
            Contains(a, b) => self.evaluate_contains(a, b),
            Divide(a, b) =>
                self.expand2(a, b, |aa, bb| aa / bb),
            ElementAt(a, b) => self.evaluate_index_of_collection(a, b),
            Equal(a, b) =>
                self.expand2(a, b, |aa, bb| Boolean(aa == bb)),
            Eval(a) => self.evaluate_eval(&a),
            Factorial(a) => self.expand1(a, |aa| aa.factorial().unwrap_or(Undefined)),
            From(src) => self.perform_table_row_query(src, &UNDEFINED, Undefined),
            FunctionCall { fx, args } =>
                self.evaluate_function_call(fx, args),
            GreaterThan(a, b) =>
                self.expand2(a, b, |aa, bb| Boolean(aa > bb)),
            GreaterOrEqual(a, b) =>
                self.expand2(a, b, |aa, bb| Boolean(aa >= bb)),
            If { condition, a, b } =>
                self.evaluate_if_then_else(condition, a, b),
            Include(path) => self.evaluate_include(path),
            Inquire(q) => self.evaluate_inquiry(q),
            JSONLiteral(items) => self.evaluate_json(items),
            LessThan(a, b) =>
                self.expand2(a, b, |aa, bb| Boolean(aa < bb)),
            LessOrEqual(a, b) =>
                self.expand2(a, b, |aa, bb| Boolean(aa <= bb)),
            Literal(value) => Ok((self.clone(), value.clone())),
            Minus(a, b) =>
                self.expand2(a, b, |aa, bb| aa - bb),
            Modulo(a, b) =>
                self.expand2(a, b, |aa, bb| aa % bb),
            Multiply(a, b) =>
                self.expand2(a, b, |aa, bb| aa * bb),
            MustAck(a) => self.evaluate_directive_ack(a),
            MustDie(a) => self.evaluate_directive_die(a),
            MustIgnoreAck(a) => self.evaluate_directive_ignore_ack(a),
            MustNotAck(a) => self.evaluate_directive_not_ack(a),
            Mutate(m) => self.evaluate_mutation(m),
            Neg(a) => self.evaluate_neg(a),
            Not(a) => self.expand1(a, |aa| !aa),
            NotEqual(a, b) =>
                self.expand2(a, b, |aa, bb| Boolean(aa != bb)),
            Ns(a) => self.perform_table_ns(a),
            Or(a, b) =>
                self.expand2(a, b, |aa, bb| aa.or(&bb).unwrap_or(Undefined)),
            Perform(i) => self.evaluate_infrastructure(i),
            Plus(a, b) =>
                self.expand2(a, b, |aa, bb| aa + bb),
            Pow(a, b) =>
                self.expand2(a, b, |aa, bb| aa.pow(&bb).unwrap_or(Undefined)),
            Range(a, b) =>
                self.expand2(a, b, |aa, bb| aa.range(&bb).unwrap_or(Undefined)),
            Return(a) => {
                let (machine, result) = self.evaluate_array(a)?;
                Ok((machine, result))
            }
            SetVariable(name, expr) => {
                let (machine, value) = self.evaluate(expr)?;
                Ok((machine.set(name, value), Ack))
            }
            ShiftLeft(a, b) =>
                self.expand2(a, b, |aa, bb| aa << bb),
            ShiftRight(a, b) =>
                self.expand2(a, b, |aa, bb| aa >> bb),
            StdErr(a) => self.write_to_console(a),
            StdOut(a) => self.write_to_console(a),
            SystemCall(args) => self.execute_system_call(args),
            TupleLiteral(values) => self.evaluate_array(values),
            TypeOf(a) => self.execute_type_of(a),
            Variable(name) => Ok((self.clone(), self.get(&name).unwrap_or(Undefined))),
            Via(src) => self.perform_table_row_query(src, &UNDEFINED, Undefined),
            While { condition, code } =>
                self.evaluate_while(condition, code),
            Www { method, url, body, headers } =>
                self.evaluate_www(method, url, body, headers),
        }
    }

    /// Evaluates the index of a collection (array, string or table)
    pub fn evaluate_index_of_collection(
        &self,
        collection_expr: &Expression,
        index_expr: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, index) = self.evaluate(index_expr)?;
        let (machine, collection) = machine.evaluate(collection_expr)?;
        let value = match collection {
            Array(items) => {
                let idx = index.assume_usize().unwrap_or(0);
                if idx < items.len() { items[idx].clone() } else { ErrorValue(format!("Array element index is out of range ({} >= {})", idx, items.len())) }
            }
            StringValue(string) => {
                let idx = index.assume_usize().unwrap_or(0);
                if idx < string.len() { StringValue(string[idx..idx].to_string()) } else { ErrorValue(format!("String character index is out of range ({} >= {})", idx, string.len())) }
            }
            StructureValue(structure) => {
                let idx = index.assume_usize().unwrap_or(0);
                let values = structure.get_values();
                if idx < values.len() { values[idx].clone() } else { ErrorValue(format!("Structure element index is out of range ({} >= {})", idx, values.len())) }
            }
            TableNs(path) => {
                let id = index.assume_usize().unwrap_or(0);
                let frc = FileRowCollection::open_path(path.as_str())?;
                match frc.read_one(id)? {
                    Some(row) => StructureValue(Structure::from_row(&row)),
                    None => StructureValue(Structure::new(frc.get_columns().clone()))
                }
            }
            TableValue(mrc) => {
                let id = index.assume_usize().unwrap_or(0);
                match mrc.read_one(id)? {
                    Some(row) => StructureValue(Structure::from_row(&row)),
                    None => StructureValue(Structure::new(mrc.get_columns().clone()))
                }
            }
            other =>
                ErrorValue(format!("Type mismatch: Table or Array expected near {}", other))
        };
        Ok((machine, value))
    }

    pub fn evaluate_infrastructure(
        &self,
        expression: &Infrastructure,
    ) -> std::io::Result<(Self, TypedValue)> {
        use Infrastructure::*;
        match expression {
            Create { path, entity: IndexEntity { columns } } =>
                self.perform_table_create_index(path, columns),
            Create { path, entity: TableEntity { columns, from } } =>
                self.perform_table_create_table(path, columns, from),
            Declare(IndexEntity { columns }) =>
                self.perform_table_declare_index(columns),
            Declare(TableEntity { columns, from }) =>
                self.perform_table_declare_table(columns, from),
            Drop(IndexTarget { path }) => self.perform_table_drop(path),
            Drop(TableTarget { path }) => self.perform_table_drop(path),
        }
    }

    pub fn evaluate_inquiry(&self, expression: &Queryable) -> std::io::Result<(Self, TypedValue)> {
        use Queryable::*;
        match expression {
            Describe(table) => self.perform_table_describe(table),
            Limit { from, limit } => {
                let (machine, limit) = self.evaluate(limit)?;
                machine.perform_table_row_query(from, &UNDEFINED, limit)
            }
            Reverse(a) => self.perform_table_row_reverse(a),
            Select { fields, from, condition, group_by, having, order_by, limit } =>
                self.perform_table_row_selection(fields, from, condition, group_by, having, order_by, limit),
            Where { from, condition } =>
                self.perform_table_row_query(from, condition, Undefined),
        }
    }

    pub fn evaluate_mutation(&self, expression: &Mutation) -> std::io::Result<(Self, TypedValue)> {
        use Mutation::*;
        match expression {
            Append { path, source } =>
                self.perform_table_row_append(path, source),
            Compact { path } =>
                self.perform_table_compact(path),
            Delete { path, condition, limit } =>
                self.perform_table_row_delete(path, condition, limit),
            IntoNs(source, target) =>
                self.perform_table_into(target, source),
            Overwrite { path, source, condition, limit } =>
                self.perform_table_row_overwrite(path, source, condition, limit),
            Scan { path } =>
                self.perform_table_scan(path),
            Truncate { path, limit } =>
                match limit {
                    None => self.perform_table_row_resize(path, Boolean(false)),
                    Some(limit) => {
                        let (machine, limit) = self.evaluate(limit)?;
                        machine.perform_table_row_resize(path, limit)
                    }
                }
            Undelete { path, condition, limit } =>
                self.perform_table_row_undelete(path, condition, limit),
            Update { path, source, condition, limit } =>
                self.perform_table_row_update(path, source, condition, limit),
        }
    }

    fn evaluate_anonymous_function(
        &self,
        params: &Vec<ColumnJs>,
        code: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        Ok((self.clone(), TypedValue::Function {
            params: params.clone(),
            code: Box::new(code.clone()),
        }))
    }

    /// evaluates the specified [Expression]; returning an array ([TypedValue]) result.
    pub fn evaluate_array(
        &self,
        ops: &Vec<Expression>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, results) = ops.iter()
            .fold((self.clone(), vec![]),
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
    pub fn evaluate_atoms(
        &self,
        ops: &Vec<Expression>,
    ) -> std::io::Result<(Self, Vec<String>)> {
        let (machine, results) = ops.iter()
            .fold((self.clone(), vec![]),
                  |(machine, mut array), op| match op {
                      Variable(name) => {
                          array.push(name.to_string());
                          (machine, array)
                      }
                      expr =>
                          panic!("Expected a column, got \"{}\" instead", expr),
                  });
        Ok((machine, results))
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
        let (machine, _) = self.evaluate_eval(expr)?;
        Ok((machine, Ack))
    }

    fn evaluate_directive_ack(
        &self,
        expr: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        info!("{}", expr.to_code());
        let (machine, value) = self.evaluate(expr)?;
        match value {
            v if v.is_ok() => Ok((machine, v.clone())),
            v =>
                Ok((machine, ErrorValue(format!("Expected true, Table(..) or RowsAffected(_ >= 1), but got {}", &v))))
        }
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

    fn evaluate_eval(
        &self,
        string_expr: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, string_value) = self.evaluate(string_expr)?;
        match string_value {
            TypedValue::StringValue(ql) => {
                match Compiler::compile_script(ql.as_str()) {
                    Ok(opcode) => {
                        match machine.evaluate(&opcode) {
                            Ok((machine, tv)) => Ok((machine, tv)),
                            Err(err) => Ok((machine, TypedValue::ErrorValue(err.to_string())))
                        }
                    }
                    Err(err) => Ok((machine, TypedValue::ErrorValue(err.to_string())))
                }
            }
            x =>
                Ok((machine, TypedValue::ErrorValue(format!("Type mismatch - expected String, got {}", x.get_type_name()))))
        }
    }

    fn evaluate_function_call(
        &self,
        fx: &Expression,
        args: &Vec<Expression>,
    ) -> std::io::Result<(Self, TypedValue)> {
        // extract the arguments
        if let (machine, TypedValue::Array(args)) = self.evaluate_array(args)? {
            // evaluate the anonymous- or named-function
            match machine.evaluate(fx)? {
                (machine, Function { params, code }) =>
                    machine.create_function_arguments(params, args).evaluate(&code),
                _ => fail(format!("'{}' is not a function", fx.to_code()))
            }
        } else {
            fail(format!("Function arguments expected, but got {}", ArrayLiteral(args.clone())))
        }
    }

    fn create_function_arguments(
        &self,
        params: Vec<ColumnJs>,
        args: Vec<TypedValue>,
    ) -> Self {
        assert_eq!(params.len(), args.len());
        params.iter().zip(args.iter())
            .fold(self.clone(), |machine, (c, v)|
                machine.with_variable(c.get_name(), v.clone()))
    }

    /// Evaluates an if-then-else expression
    fn evaluate_if_then_else(
        &self,
        condition: &Expression,
        a: &Expression,
        b: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        use TypedValue::*;
        let ms0 = self.clone();
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
            Ok((machine, TypedValue::ErrorValue(format!("Type mismatch - expected String, got {}",
                                                        path_value.get_type_name()))))
        }
    }

    fn evaluate_json(
        &self,
        items: &Vec<(String, Expression)>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let mut elems = vec![];
        for (name, expr) in items {
            let (_, value) = self.evaluate(expr)?;
            elems.push((name.to_string(), value))
        }
        Ok((self.clone(), TypedValue::JSONObjectValue(elems)))
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

    /// evaluates the specified [Expression]; returning an option of a [TypedValue] result.
    pub fn evaluate_opt(
        &self,
        opt_of_expr: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, Option<TypedValue>)> {
        Ok(match opt_of_expr {
            Some(expr) => {
                let (ms, tv) = self.evaluate(&expr)?;
                (ms, Some(tv))
            }
            None => (self.clone(), None)
        })
    }

    fn execute_system_call(
        &self,
        args: &Vec<Expression>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let machine = self.clone();
        if let Ok((machine, Array(items))) = machine.evaluate_array(args) {
            let items: Vec<String> = items.iter().map(|i| i.unwrap_value()).collect();
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
                Ok((machine, result))
            } else {
                Ok((machine, ErrorValue(format!("Expected array but got {:?}", args))))
            }
        } else {
            Ok((machine, ErrorValue(format!("Expected array but got {:?}", args))))
        }
    }

    fn execute_type_of(&self,
                       expression: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, value) = self.evaluate(expression)?;
        Ok((ms, StringValue(value.get_type_name())))
    }

    fn evaluate_while(
        &self,
        condition: &Expression,
        code: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let mut machine = self.clone();
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

    fn evaluate_www(
        &self,
        method: &Expression,
        url: &Expression,
        body: &Option<Box<Expression>>,
        headers: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, method) = self.evaluate(method)?;
        let (ms, url) = ms.evaluate(url)?;
        let (ms, body) = ms.evaluate_opt(body)?;
        let (ms, headers) = ms.evaluate_opt(headers)?;
        let promise = ms.http_call(
            method.unwrap_value(),
            url.unwrap_value(),
            body.map(|e| e.unwrap_value()));
        Runtime::new()?.block_on(promise)
    }

    async fn http_call(
        &self,
        method: String,
        url: String,
        body: Option<String>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let ms = self.clone();
        let result = match reqwest::get(url).await {
            Ok(response) => {
                if response.status().is_success() {
                    match response.text().await {
                        Ok(body) => StringValue(body),
                        Err(err) =>
                            ErrorValue(format!("Error reading response body: {}", err)),
                    }
                } else {
                    ErrorValue(format!("Request failed with status: {}", response.status()))
                }
            }
            Err(err) => ErrorValue(format!("Error making request: {}", err)),
        };

        Ok((ms, result))
    }

    pub fn install_platform_functions(&self) -> Self {
        let mut m = self.clone();
        // m.variables.insert("type_of".to_string(), Function {
        //     params: vec![
        //         ColumnJs::new("item", "", )
        //     ],
        //     code: Box::new(()),
        // });
        m
    }

    fn perform_table_create_index(
        &self,
        index: &Expression,
        columns: &Vec<Expression>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, result) = self.evaluate(index)?;
        match result {
            Null | Undefined => Ok((machine, result)),
            TableValue(_mrc) =>
                Ok((machine, ErrorValue("Memory collections do not yet support indexes".to_string()))),
            TableNs(path) => {
                let ns = Namespace::parse(path.as_str())?;
                let (machine, columns) = self.evaluate_atoms(columns)?;
                let config = DataFrameConfig::load(&ns)?;
                let mut indices = config.get_indices().clone();
                indices.push(HashIndexConfig::new(columns, false));
                let config = DataFrameConfig::new(
                    config.get_columns().clone(),
                    indices.clone(),
                    config.get_partitions().clone(),
                );
                config.save(&ns)?;
                Ok((machine, Ack))
            }
            x =>
                Ok((machine, ErrorValue(format!("Type mismatch: expected an iterable near {}", x))))
        }
    }

    fn perform_table_create_table(
        &self,
        table: &Expression,
        columns: &Vec<ColumnJs>,
        from: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, result) = self.evaluate(table)?;
        match result.clone() {
            Null | Undefined => Ok((machine, result)),
            TableValue(_mrc) =>
                Ok((machine, ErrorValue("Memory collections do not 'create' keyword".to_string()))),
            TableNs(path) => {
                let ns = Namespace::parse(path.as_str())?;
                let config = DataFrameConfig::new(columns.clone(), vec![], vec![]);
                DataFrame::create(ns, config)?;
                Ok((machine, Ack))
            }
            x =>
                Ok((machine, ErrorValue(format!("Type mismatch: expected an iterable near {}", x))))
        }
    }

    fn perform_table_declare_index(
        &self,
        columns: &Vec<Expression>,
    ) -> std::io::Result<(Self, TypedValue)> {
        // TODO determine how to implement
        Ok((self.clone(), ErrorValue("Not yet implemented".into())))
    }

    fn perform_table_declare_table(
        &self,
        columns: &Vec<ColumnJs>,
        from: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let columns = TableColumn::from_columns(columns)?;
        Ok((self.clone(), TableValue(ModelRowCollection::with_rows(columns, vec![]))))
    }

    fn perform_table_describe(
        &self,
        expression: &Box<Expression>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, result) = self.evaluate(expression)?;
        let rc = result.to_table()?;
        Ok((machine, rc.describe()))
    }

    fn perform_table_drop(&self, table: &Expression) -> std::io::Result<(Self, TypedValue)> {
        let (machine, table) = self.evaluate(table)?;
        match table {
            TableNs(path) => {
                let ns = Namespace::parse(path.as_str())?;
                let result = fs::remove_file(ns.get_table_file_path());
                Ok((machine, if result.is_ok() { Ack } else { Boolean(false) }))
            }
            _ => Ok((machine, Boolean(false)))
        }
    }

    fn perform_table_into(
        &self,
        table: &Expression,
        source: &Expression,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let machine = self.clone();
        let (machine, rows) = match source {
            From(source) =>
                self.extract_rows_from_query(source, table)?,
            Literal(TableValue(mrc)) =>
                (machine, mrc.get_rows()),
            Literal(TableNs(name)) => {
                let ns = Namespace::parse(name.as_str())?;
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

    fn extract_rows_from_table_declaration(
        &self,
        table: &Expression,
        from: &Option<Box<Expression>>,
        columns: &Vec<ColumnJs>,
    ) -> std::io::Result<(Machine, Vec<Row>)> {
        let machine = self.clone();
        // create the config and an empty data file
        let ns = self.expect_namespace(table)?;
        let cfg = DataFrameConfig::new(columns.clone(), vec![], vec![]);
        cfg.save(&ns)?;
        FileRowCollection::table_file_create(&ns)?;
        // decipher the "from" expression
        let columns = TableColumn::from_columns(columns)?;
        let results = match from {
            Some(expr) => machine.expect_rows(expr.deref(), &columns)?,
            None => vec![]
        };
        Ok((machine, results))
    }

    fn extract_rows_from_query(
        &self,
        source: &Expression,
        table: &Expression,
    ) -> std::io::Result<(Machine, Vec<Row>)> {
        // determine the row collection
        let machine = self.clone();
        let rc = machine.expect_row_collection(table)?;

        // retrieve rows from the source
        let rows = machine.expect_rows(source, rc.get_columns())?;
        Ok((machine, rows))
    }

    fn perform_table_compact(&self, expr: &Expression) -> std::io::Result<(Self, TypedValue)> {
        let (machine, tv_table) = self.evaluate(expr)?;
        Self::orchestrate_rc(machine, tv_table, |machine, mut rc| {
            Ok((machine, rc.compact()))
        })
    }

    fn perform_table_ns(&self, expr: &Expression) -> std::io::Result<(Self, TypedValue)> {
        let (machine, result) = self.evaluate(expr)?;
        let result = match result {
            StringValue(path) => TableNs(path),
            TableNs(path) => TableNs(path),
            other => Self::error_unexpected("Table reference", &other),
        };
        Ok((machine, result))
    }

    fn perform_table_row_resize(
        &self,
        table: &Expression,
        limit: TypedValue,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, table) = self.evaluate(table)?;
        Self::orchestrate_io(machine, table, &vec![], &vec![], &None, limit, |machine, df, _fields, _values, condition, limit| {
            let limit = limit.assume_usize().unwrap_or(0);
            let new_size = df.resize(limit)?;
            Ok((machine, RowsAffected(new_size)))
        })
    }

    fn perform_table_row_append(
        &self,
        table: &Expression,
        source: &Expression,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let machine = self.clone();
        if let From(source) = source {
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

    fn perform_table_row_delete(
        &self,
        from: &Expression,
        condition: &Option<Box<Expression>>,
        limit: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, limit) = Self::expand(self.clone(), limit)?;
        let (machine, result) = machine.evaluate(from)?;
        Self::orchestrate_io(machine, result, &vec![], &vec![], condition, limit, |machine, df, _fields, _values, condition, limit| {
            let deleted = df.delete_where(&machine, &condition, limit).ok().unwrap_or(0);
            Ok((machine, RowsAffected(deleted)))
        })
    }

    fn perform_table_row_overwrite(
        &self,
        table: &Expression,
        source: &Expression,
        condition: &Option<Box<Expression>>,
        limit: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, limit) = Self::expand(self.clone(), limit)?;
        let (machine, tv_table) = machine.evaluate(table)?;
        let (fields, values) = self.expect_via(&table, &source)?;
        Self::orchestrate_io(machine, tv_table, &fields, &values, condition, limit, |machine, df, fields, values, condition, limit| {
            let overwritten = df.overwrite_where(&machine, fields, values, condition, limit).ok().unwrap_or(0);
            Ok((machine, RowsAffected(overwritten)))
        })
    }

    /// Evaluates the queryable [Expression] (e.g. from, limit and where)
    /// e.g.: from ns("interpreter.select.stocks") where last_sale > 1.0 limit 1
    fn perform_table_row_query(
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
                let mut cursor = Cursor::filter(Box::new(mrc), condition.clone());
                Ok((machine, TableValue(ModelRowCollection::from_rows(cursor.take(limit)?))))
            }
            TableNs(name) => {
                let ns = Namespace::parse(name.as_str())?;
                let frc = FileRowCollection::open(&ns)?;
                let mut cursor = Cursor::filter(Box::new(frc), condition.clone());
                Ok((machine, TableValue(ModelRowCollection::from_rows(cursor.take(limit)?))))
            }
            value => fail_value("Queryable expected", &value)
        }
    }

    /// Reverse orders a collection
    pub fn perform_table_row_reverse(
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

    fn perform_table_row_selection(
        &self,
        fields: &Vec<Expression>,
        from: &Option<Box<Expression>>,
        condition: &Option<Box<Expression>>,
        _group_by: &Option<Vec<Expression>>,
        _having: &Option<Box<Expression>>,
        _order_by: &Option<Vec<Expression>>,
        limit: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, limit) = Self::expand(self.clone(), limit)?;
        match from {
            None => machine.evaluate_array(fields),
            Some(source) => {
                let (machine, table) = machine.evaluate(source)?;
                Self::orchestrate_io(machine, table, &vec![], &vec![], condition, limit, |machine, df, _, _, condition, limit| {
                    let rows = df.read_where(&machine, condition, limit)?;
                    Ok((machine, TableValue(ModelRowCollection::from_rows(rows))))
                })
            }
        }
    }

    fn perform_table_row_undelete(
        &self,
        from: &Expression,
        condition: &Option<Box<Expression>>,
        limit: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, limit) = Self::expand(self.clone(), limit)?;
        let (machine, result) = machine.evaluate(from)?;
        Self::orchestrate_io(machine, result, &vec![], &vec![], condition, limit, |machine, df, _fields, _values, condition, limit| {
            match df.undelete_where(&machine, &condition, limit) {
                Ok(restored) => Ok((machine, RowsAffected(restored))),
                Err(err) => Ok((machine, ErrorValue(err.to_string()))),
            }
        })
    }

    fn perform_table_row_update(
        &self,
        table: &Expression,
        source: &Expression,
        condition: &Option<Box<Expression>>,
        limit: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, limit) = Self::expand(self.clone(), limit)?;
        let (machine, tv_table) = machine.evaluate(table)?;
        let (fields, values) = self.expect_via(&table, &source)?;
        Self::orchestrate_io(machine, tv_table, &fields, &values, condition, limit, |machine, df, fields, values, condition, limit| {
            let modified = df.update_where(&machine, fields, values, condition, limit).ok().unwrap_or(0);
            Ok((machine, RowsAffected(modified)))
        })
    }

    fn perform_table_scan(&self, expr: &Expression) -> std::io::Result<(Self, TypedValue)> {
        let (machine, tv_table) = self.evaluate(expr)?;
        Self::orchestrate_rc(machine, tv_table, |machine, rc| {
            let mrc = ModelRowCollection::from_rows(rc.examine_rows()?);
            Ok((machine, TableValue(mrc)))
        })
    }

    /// evaluates the specified [Expression]; returning a [TypedValue] result.
    pub fn evaluate_scope(&self, ops: &Vec<Expression>) -> std::io::Result<(Self, TypedValue)> {
        Ok(ops.iter().fold((self.clone(), Undefined),
                           |(machine, _), op| match machine.evaluate(op) {
                               Ok((machine, tv)) => (machine, tv),
                               Err(err) => panic!("{}", err.to_string())
                           }))
    }

    fn assume_table_value_or_reference<A>(
        &self,
        table: &TypedValue,
        f: fn(Box<dyn RowCollection>) -> std::io::Result<A>,
    ) -> std::io::Result<A> {
        match table {
            TableNs(namespace) => {
                let ns = Namespace::parse(namespace.as_str())?;
                let frc = FileRowCollection::open(&ns)?;
                f(Box::new(frc))
            }
            TableValue(mrc) => f(Box::new(mrc.clone())),
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
            TableNs(namespace) => {
                let ns = Namespace::parse(namespace.as_str())?;
                f(DataFrame::load(ns)?)
            }
            TableValue(mrc) =>
                f(DataFrame::new(Box::new(mrc.clone()))),
            z =>
                return fail_value(format!("{} is not a table", z), z)
        }
    }

    /// executes the specified instructions on this state machine.
    pub fn execute(&self, ops: &Vec<OpCode>) -> std::io::Result<(Self, TypedValue)> {
        let mut machine = self.clone();
        for op in ops { machine = op(machine)? }
        let (machine, result) = machine.pop_or(Undefined);
        Ok((machine, result))
    }

    /// evaluates the boxed expression and applies the supplied function
    fn expand1(
        &self,
        a: &Expression,
        f: fn(TypedValue) -> TypedValue,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, aa) = self.evaluate(a)?;
        Ok((machine, f(aa)))
    }

    /// evaluates the two boxed expressions and applies the supplied function
    fn expand2(
        &self,
        a: &Expression,
        b: &Expression,
        f: fn(TypedValue, TypedValue) -> TypedValue,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, aa) = self.evaluate(a)?;
        let (machine, bb) = machine.evaluate(b)?;
        Ok((machine, f(aa, bb)))
    }

    /// evaluates the three boxed expressions and applies the supplied function
    fn expand3(
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

    fn expect_namespace(&self, table: &Expression) -> std::io::Result<Namespace> {
        let (_, v_table) = self.evaluate(table)?;
        match v_table {
            TableNs(path) => Namespace::parse(path.as_str()),
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
                let mut rows = vec![];
                for tuples in items {
                    if let JSONObjectValue(tuples) = tuples {
                        rows.push(Row::from_tuples(0, columns, &tuples))
                    }
                }
                Ok(rows)
            }
            JSONObjectValue(tuples) => Ok(vec![Row::from_tuples(0, columns, &tuples)]),
            TableNs(path) => {
                let ns = Namespace::parse(path.as_str())?;
                let frc = FileRowCollection::open(&ns)?;
                frc.read_active_rows()
            }
            TableValue(mrc) => Ok(mrc.get_rows()),
            tv => fail_value("A queryable was expected".to_string(), &tv)
        }
    }

    fn expect_string(&self, expression: &Expression) -> std::io::Result<String> {
        if let (_, StringValue(value)) = self.evaluate(expression)? {
            Ok(value)
        } else {
            fail_expr("A string value was expected".to_string(), expression)
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
                if let (_, JSONObjectValue(tuples)) = self.evaluate(&my_source)? {
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

    fn extract_array_of_strings(&self, columns: &Vec<TypedValue>) -> Vec<String> {
        columns.iter().map(|tv| match tv {
            StringValue(value) => value.clone(),
            other => panic!("Type mismatch: An identifier was expected near \"{}\"", other)
        }).collect()
    }

    /// returns a variable by name
    pub fn get(&self, name: &str) -> Option<TypedValue> {
        self.variables.get(name).map(|x| x.clone())
    }

    pub fn get_variables(&self) -> &HashMap<String, TypedValue> { &self.variables }

    fn is_true(&self, row: &Row, condition: Option<Box<Expression>>) -> bool {
        condition.is_none() || condition.is_some_and(|expr|
            match self.with_row(row).evaluate(&expr) {
                Ok((_, result)) => result == Boolean(true),
                Err(err) => {
                    error!("{}", err);
                    false
                }
            })
    }

    /// returns the option of a value from the stack
    pub fn pop(&self) -> (Self, Option<TypedValue>) {
        let mut stack = self.stack.clone();
        let value = stack.pop();
        let variables = self.variables.clone();
        (Self::construct(stack, variables), value)
    }

    /// returns a value from the stack or the default value if the stack is empty.
    pub fn pop_or(&self, default_value: TypedValue) -> (Self, TypedValue) {
        let (machine, result) = self.pop();
        (machine, result.unwrap_or(default_value))
    }

    /// pushes a value unto the stack
    pub fn push(&self, value: TypedValue) -> Self {
        let mut stack = self.stack.clone();
        stack.push(value);
        Self::construct(stack, self.variables.clone())
    }

    /// pushes a collection of values unto the stack
    pub fn push_all(&self, values: Vec<TypedValue>) -> Self {
        values.iter().fold(self.clone(), |machine, tv| machine.push(tv.clone()))
    }

    pub fn set(&self, name: &str, value: TypedValue) -> Self {
        let mut variables = self.variables.clone();
        variables.insert(name.to_string(), value);
        Self::construct(self.stack.clone(), variables)
    }

    fn split_first<T>(vec: Vec<T>) -> Option<(T, Vec<T>)> {
        let mut iter = vec.into_iter();
        iter.next().map(|first| (first, iter.collect()))
    }

    pub fn stack_len(&self) -> usize {
        self.stack.len()
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
            .fold(self.clone(), |machine, (v, c)| {
                machine.with_variable(c.get_name(), v.clone())
            })
    }

    pub fn with_variable(&self, name: &str, value: TypedValue) -> Self {
        let mut variables = self.variables.clone();
        variables.insert(name.to_string(), value);
        Self::construct(self.stack.clone(), variables)
    }

    fn write_to_console(
        &self,
        expr: &Box<Expression>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (machine, result) = self.evaluate(expr)?;
        print!("{}", result.unwrap_value());
        Ok((machine, TypedValue::Ack))
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::compiler::Compiler;
    use crate::expression::{FALSE, NULL, TRUE};
    use crate::expression::Expression::*;
    use crate::table_columns::TableColumn;
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
    fn test_eval() {
        let model = Eval(Box::new(Literal(StringValue("5 + 7".to_string()))));
        assert_eq!(model.to_code(), "eval \"5 + 7\"");

        let machine = Machine::new();
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, Int64Value(12))
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
        let ns_path = "machine.element_at.stocks";
        let (mut df, phys_columns) = create_file_table(ns_path);
        assert_eq!(1, df.append(make_quote(0, &phys_columns, "ABC", "AMEX", 11.77)).unwrap());
        assert_eq!(1, df.append(make_quote(1, &phys_columns, "UNO", "OTC", 0.2456)).unwrap());
        assert_eq!(1, df.append(make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66)).unwrap());
        assert_eq!(1, df.append(make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428)).unwrap());
        assert_eq!(1, df.append(make_quote(4, &phys_columns, "BOOM", "NASDAQ", 56.87)).unwrap());

        // create the instruction model 'ns("machine.element_at.stocks")[2]'
        let model = ElementAt(
            Box::new(Ns(Box::new(Literal(StringValue(ns_path.into()))))),
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
            make_table_columns(), vec![],
        )))
    }

    #[test]
    fn test_drop_table() {
        // create a table with test data
        let ns_path = "machine.drop.stocks";
        let ns = Ns(Box::new(Literal(StringValue(ns_path.into()))));
        create_file_table(ns_path);

        let model = Perform(Infrastructure::Drop(TableTarget { path: Box::new(ns) }));
        assert_eq!(model.to_code(), "drop table ns(\"machine.drop.stocks\")");

        let machine = Machine::new();
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, Ack)
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
            SetVariable("add".to_string(), Box::new(Literal(fx.clone())))
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
        let (df, phys_columns) = create_file_table(ns_path);

        // delete some rows
        let machine = Machine::new();
        let code = Compiler::compile_script(r#"
            delete from ns("machine.delete.stocks")
            where last_sale > 1.0
            "#).unwrap();
        let (_, result) = machine.evaluate(&code).unwrap();
        assert_eq!(result, RowsAffected(3));

        // verify the remaining rows
        let rows = df.read_all_rows().unwrap();
        assert_eq!(rows, vec![
            make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
            make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428),
        ]);
    }

    #[test]
    fn test_undelete_rows_from_namespace() {
        // create a table with test data
        let ns_path = "machine.undelete.stocks";
        let (df, phys_columns) = create_file_table(ns_path);

        // delete some rows
        let machine = Machine::new();
        let code = Compiler::compile_script(r#"
            stocks := ns("machine.undelete.stocks")
            delete from stocks where last_sale > 1.0
            "#).unwrap();
        let (machine, result) = machine.evaluate(&code).unwrap();
        assert_eq!(result, RowsAffected(3));

        // verify the remaining rows
        let rows = df.read_all_rows().unwrap();
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
        let rows = df.read_all_rows().unwrap();
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
        let (df, phys_columns) = create_file_table(ns_path);

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
        let rows = df.read_all_rows().unwrap();
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
        let (df, phys_columns) = create_file_table(ns_path);

        // overwrite some rows
        let machine = Machine::new();
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, RowsAffected(1));

        // verify the remaining rows
        let rows = df.read_all_rows().unwrap();
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
            create_file_table("machine.select.stocks");

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
        let model = SystemCall(vec![
            Literal(StringValue("cat".into())),
            Literal(StringValue("LICENSE".into())),
        ]);
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
        let model = TypeOf(Box::new(Literal(StringValue("cat".into()))));
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
        let (df, phys_columns) = create_file_table(ns_path);

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
        let rows = df.read_all_rows().unwrap();
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

    #[test]
    fn test_www_get() {
        let model = Www {
            method: Box::new(Literal(StringValue("get".to_string()))),
            url: Box::new(Literal(StringValue("http://www.yahoo.com".to_string()))),
            body: None,
            headers: None,
        };
        let machine = Machine::new();
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, StringValue("OK\r\n".into()));
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

    fn create_file_table(namespace: &str) -> (DataFrame, Vec<TableColumn>) {
        let columns = make_quote_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let ns = Namespace::parse(namespace).unwrap();
        match fs::remove_file(ns.get_table_file_path()) {
            Ok(_) => {}
            Err(_) => {}
        }

        let mut df = make_dataframe_ns(ns, columns.clone()).unwrap();
        assert_eq!(1, df.append(make_quote(0, &phys_columns, "ABC", "AMEX", 11.77)).unwrap());
        assert_eq!(1, df.append(make_quote(1, &phys_columns, "UNO", "OTC", 0.2456)).unwrap());
        assert_eq!(1, df.append(make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66)).unwrap());
        assert_eq!(1, df.append(make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428)).unwrap());
        assert_eq!(1, df.append(make_quote(4, &phys_columns, "BOOM", "NASDAQ", 56.87)).unwrap());
        (df, phys_columns)
    }
}