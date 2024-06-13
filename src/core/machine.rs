////////////////////////////////////////////////////////////////////
// state machine module
////////////////////////////////////////////////////////////////////

use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::Read;
use std::ptr::null;

use actix_web::web::to;
use crossterm::terminal::window_size;
use log::error;
use serde::{Deserialize, Serialize};

use shared_lib::{cnv_error, fail};

use crate::{row, serialization};
use crate::compiler::{CompilerState, fail_expr, fail_near, fail_unexpected, fail_unhandled_expr, fail_value};
use crate::cursor::Cursor;
use crate::dataframe_config::{DataFrameConfig, HashIndexConfig};
use crate::dataframes::DataFrame;
use crate::expression::{Expression, TRUE, UNDEFINED};
use crate::expression::CreationEntity::{IndexEntity, TableEntity};
use crate::expression::Expression::{ArrayLiteral, ColumnSet, From, Literal, Ns, Variable, Via};
use crate::expression::MutateTarget::{IndexTarget, TableTarget};
use crate::fields::Field;
use crate::file_row_collection::FileRowCollection;
use crate::interpreter::Interpreter;
use crate::model_row_collection::ModelRowCollection;
use crate::namespaces::Namespace;
use crate::row_collection::RowCollection;
use crate::rows::Row;
use crate::server::ColumnJs;
use crate::table_columns::TableColumn;
use crate::tokens::Token::Atom;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::*;

/// represents an Oxide executable instruction (opcode)
pub type OpCode = fn(MachineState) -> std::io::Result<MachineState>;

/// Represents the state of the machine.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MachineState {
    stack: Vec<TypedValue>,
    variables: HashMap<String, TypedValue>,
}

impl MachineState {

    ////////////////////////////////////////////////////////////////
    // static methods
    ////////////////////////////////////////////////////////////////

    /// creates a new state machine
    pub fn construct(stack: Vec<TypedValue>, variables: HashMap<String, TypedValue>) -> Self {
        Self { stack, variables }
    }

    fn expand(ms: Self, expr: &Option<Box<Expression>>) -> std::io::Result<(Self, TypedValue)> {
        match expr {
            Some(item) => ms.evaluate(item),
            None => Ok((ms, TypedValue::Undefined))
        }
    }

    fn expand_vec(ms: Self, expr: &Option<Vec<Expression>>) -> std::io::Result<(Self, TypedValue)> {
        match expr {
            Some(items) => ms.evaluate_array(items),
            None => Ok((ms, TypedValue::Undefined))
        }
    }

    fn load_row_collection(path: String) -> std::io::Result<Box<dyn RowCollection>> {
        let ns = Namespace::parse(path.as_str())?;
        let frc = FileRowCollection::open(&ns)?;
        Ok(Box::new(frc))
    }

    /// creates a new empty state machine
    pub fn new() -> Self {
        Self::construct(Vec::new(), HashMap::new())
    }

    fn orchestrate_io(
        ms: Self,
        table: TypedValue,
        fields: &Vec<Expression>,
        values: &Vec<Expression>,
        condition: &Option<Box<Expression>>,
        limit: TypedValue,
        f: fn(Self, &mut DataFrame, &Vec<Expression>, &Vec<Expression>, &Option<Box<Expression>>, TypedValue) -> std::io::Result<(Self, TypedValue)>,
    ) -> std::io::Result<(Self, TypedValue)> {
        match table {
            TypedValue::TableValue(mrc) => {
                let mut df = DataFrame::from_row_collection(Namespace::temp(), Box::new(mrc));
                f(ms, &mut df, fields, values, condition, limit)
            }
            TypedValue::TableRef(path) => {
                let ns = Namespace::parse(path.as_str())?;
                let mut df = DataFrame::load(ns)?;
                f(ms, &mut df, fields, values, condition, limit)
            }
            x =>
                fail(format!("Type mismatch: expected an iterable near {}", x))
        }
    }

    fn split(row: &Row) -> (Vec<Expression>, Vec<Expression>) {
        let my_fields = row.get_columns().iter()
            .map(|tc| Variable(tc.get_name().to_string()))
            .collect::<Vec<Expression>>();
        let my_values = row.get_fields().iter()
            .map(|f| Literal(f.value.clone()))
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
            Append { path, source } =>
                self.evaluate_ql_into(path, source),
            ArrayLiteral(items) => self.evaluate_array(items),
            AsValue(name, expr) => {
                let (ms, tv) = self.evaluate(expr)?;
                Ok((ms.with_variable(name, tv.clone()), tv))
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
                let ms = self.clone();
                let values = columns.iter()
                    .map(|c| ms.variables.clone().get(c.get_name()).map(|c| c.clone())
                        .unwrap_or(Undefined))
                    .collect::<Vec<TypedValue>>();
                Ok((ms, Array(values)))
            }
            Contains(a, b) => self.evaluate_contains(a, b),
            Create { path, entity: IndexEntity { columns } } =>
                self.evaluate_ql_create_index(path, columns),
            Create { path, entity: TableEntity { columns, from } } =>
                self.evaluate_ql_create_table(path, columns, from),
            Declare(IndexEntity { columns }) =>
                self.evaluate_ql_declare_index(columns),
            Declare(TableEntity { columns, from }) =>
                self.evaluate_ql_declare_table(columns, from),
            Delete { path, condition, limit } =>
                self.evaluate_ql_delete(path, condition, limit),
            Divide(a, b) =>
                self.expand2(a, b, |aa, bb| aa / bb),
            Drop(IndexTarget { path, if_exists }) => self.evaluate_ql_drop(path, *if_exists),
            Drop(TableTarget { path, if_exists }) => self.evaluate_ql_drop(path, *if_exists),
            Equal(a, b) =>
                self.expand2(a, b, |aa, bb| Boolean(aa == bb)),
            Eval(a) => self.evaluate_eval(&a),
            Factorial(a) => self.expand1(a, |aa| aa.factorial().unwrap_or(Undefined)),
            From(src) => self.evaluate_queryable(src, &UNDEFINED, Undefined),
            FunctionCall { fx, args } =>
                self.evaluate_function_call(fx, args),
            GreaterThan(a, b) =>
                self.expand2(a, b, |aa, bb| Boolean(aa > bb)),
            GreaterOrEqual(a, b) =>
                self.expand2(a, b, |aa, bb| Boolean(aa >= bb)),
            If { condition, a, b } =>
                self.evaluate_if_then_else(condition, a, b),
            Include(path) => self.evaluate_include(path),
            IntoTable { source, target } => self.evaluate_into_table(source, target),
            JSONLiteral(items) => self.evaluate_json(items),
            LessThan(a, b) =>
                self.expand2(a, b, |aa, bb| Boolean(aa < bb)),
            LessOrEqual(a, b) =>
                self.expand2(a, b, |aa, bb| Boolean(aa <= bb)),
            Limit { from, limit } => {
                let (ms, limit) = self.evaluate(limit)?;
                ms.evaluate_queryable(from, &UNDEFINED, limit)
            }
            Literal(value) => Ok((self.clone(), value.clone())),
            Minus(a, b) =>
                self.expand2(a, b, |aa, bb| aa - bb),
            Modulo(a, b) =>
                self.expand2(a, b, |aa, bb| aa % bb),
            Multiply(a, b) =>
                self.expand2(a, b, |aa, bb| aa * bb),
            Neg(a) => self.evaluate_neg(a),
            Not(a) => self.expand1(a, |aa| !aa),
            NotEqual(a, b) =>
                self.expand2(a, b, |aa, bb| Boolean(aa != bb)),
            Ns(a) => self.evaluate_ql_ns(a),
            Or(a, b) =>
                self.expand2(a, b, |aa, bb| aa.or(&bb).unwrap_or(Undefined)),
            Overwrite { path, source, condition, limit } =>
                self.evaluate_ql_overwrite(path, source, condition, limit),
            Plus(a, b) =>
                self.expand2(a, b, |aa, bb| aa + bb),
            Pow(a, b) =>
                self.expand2(a, b, |aa, bb| aa.pow(&bb).unwrap_or(Undefined)),
            Range(a, b) =>
                self.expand2(a, b, |aa, bb| aa.range(&bb).unwrap_or(Undefined)),
            Return(a) => {
                let (ms, result) = self.evaluate_array(a)?;
                Ok((ms, result))
            }
            Reverse(a) => self.evaluate_reverse(a),
            Select { fields, from, condition, group_by, having, order_by, limit } =>
                self.evaluate_ql_select(fields, from, condition, group_by, having, order_by, limit),
            SetVariable(name, expr) => {
                let (ms, value) = self.evaluate(expr)?;
                Ok((ms.set(name, value), Boolean(true)))
            }
            Shall(a) => self.evaluate_ql_shall(a),
            ShiftLeft(a, b) =>
                self.expand2(a, b, |aa, bb| aa << bb),
            ShiftRight(a, b) =>
                self.expand2(a, b, |aa, bb| aa >> bb),
            Truncate { path, limit } =>
                match limit {
                    None => self.evaluate_ql_truncate(path, Boolean(false)),
                    Some(limit) => {
                        let (ms, limit) = self.evaluate(limit)?;
                        ms.evaluate_ql_truncate(path, limit)
                    }
                }
            TupleLiteral(values) => self.evaluate_array(values),
            Update { path, source, condition, limit } =>
                self.evaluate_ql_update(path, source, condition, limit),
            Variable(name) => Ok((self.clone(), self.get(&name).unwrap_or(Undefined))),
            Via(src) => self.evaluate_queryable(src, &UNDEFINED, Undefined),
            Where { from, condition } =>
                self.evaluate_queryable(from, condition, Undefined),
            While { condition, code } =>
                self.evaluate_while(condition, code),
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
        let (ms, results) = ops.iter()
            .fold((self.clone(), vec![]),
                  |(ms, mut array), op| match ms.evaluate(op) {
                      Ok((ms, tv)) => {
                          array.push(tv);
                          (ms, array)
                      }
                      Err(err) => panic!("{}", err.to_string())
                  });
        Ok((ms, Array(results)))
    }

    /// evaluates the specified [Expression]; returning an array ([String]) result.
    pub fn evaluate_atoms(
        &self,
        ops: &Vec<Expression>,
    ) -> std::io::Result<(Self, Vec<String>)> {
        let (ms, results) = ops.iter()
            .fold((self.clone(), vec![]),
                  |(ms, mut array), op| match op {
                      Variable(name) => {
                          array.push(name.to_string());
                          (ms, array)
                      }
                      expr => panic!("Expected a column, got \"{}\" instead", expr),
                  });
        Ok((ms, results))
    }

    fn evaluate_contains(
        &self,
        a: &Expression,
        b: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, a) = self.evaluate(a)?;
        let (ms, b) = ms.evaluate(b)?;
        Ok((ms, TypedValue::Boolean(a.contains(&b))))
    }

    fn evaluate_eval(
        &self,
        string_expr: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, string_value) = self.evaluate(string_expr)?;
        match string_value {
            TypedValue::StringValue(ql) => {
                match CompilerState::compile_script(ql.as_str()) {
                    Ok(opcode) => {
                        match ms.evaluate(&opcode) {
                            Ok((ms, tv)) => Ok((ms, tv)),
                            Err(err) => Ok((ms, TypedValue::ErrorValue(err.to_string())))
                        }
                    }
                    Err(err) => Ok((ms, TypedValue::ErrorValue(err.to_string())))
                }
            }
            x =>
                Ok((ms, TypedValue::ErrorValue(format!("Type mismatch - expected String, got {}", x.get_type_name()))))
        }
    }

    fn evaluate_function_call(
        &self,
        fx: &Expression,
        args: &Vec<Expression>,
    ) -> std::io::Result<(Self, TypedValue)> {
        // extract the arguments
        if let (ms, TypedValue::Array(args)) = self.evaluate_array(args)? {
            // evaluate the anonymous- or named-function
            match ms.evaluate(fx)? {
                (ms, Function { params, code }) =>
                    ms.create_function_arguments(params, args).evaluate(&code),
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
            .fold(self.clone(), |ms, (c, v)|
                ms.with_variable(c.get_name(), v.clone()))
    }

    /// Evaluates an if-then-else expression
    fn evaluate_if_then_else(
        &self,
        condition: &Expression,
        a: &Expression,
        b: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let ms0 = self.clone();
        let (ms, result) = ms0.evaluate(condition)?;
        match result {
            TypedValue::Null | TypedValue::Undefined => Ok((ms, result)),
            TypedValue::Boolean(is_true) =>
                if is_true {
                    Ok((ms0, ms.evaluate(a)?.1))
                } else if let Some(expr) = b {
                    Ok((ms0, ms.evaluate(expr)?.1))
                } else { Ok((ms0, Undefined)) },
            other => fail_unexpected("Boolean", &other)
        }
    }

    fn evaluate_include(
        &self,
        path: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        // evaluate the script path
        let (ms, path_value) = self.evaluate(path)?;
        if let TypedValue::StringValue(script_path) = path_value {
            // read the script file contents into the string
            let mut file = File::open(script_path)?;
            let mut script_code = String::new();
            file.read_to_string(&mut script_code)?;
            // compile and execute the string (script code)
            let opcode = CompilerState::compile_script(script_code.as_str())?;
            ms.evaluate(&opcode)
        } else {
            Ok((ms, TypedValue::ErrorValue(format!("Type mismatch - expected String, got {}",
                                                   path_value.get_type_name()))))
        }
    }

    fn evaluate_into_table(
        &self,
        source: &Expression,
        target: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, source) = self.evaluate(source)?;
        let (ms, target) = ms.evaluate(target)?;
        match target {
            TableRef(namespace) => {
                let ns = Namespace::parse(namespace.as_str())?;
                match source {
                    TableRef(q_name) => Ok((ms, RowsAffected(5))),
                    TableValue(mrc) => Ok((ms, RowsAffected(5))),
                    z => fail_value(format!("Source {} is not a table", &z), &z)
                }
            }
            z => fail_value(format!("Target {} is not a table", &z), &z)
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
        let (ms, result) = self.evaluate(expr)?;
        let neg_result = match result {
            Boolean(n) => Boolean(!n),
            UInt8Value(n) => Int16Value(-(n as i16)),
            Int16Value(n) => Int16Value(-n),
            Int32Value(n) => Int32Value(-n),
            Int64Value(n) => Int64Value(-n),
            Float32Value(n) => Float32Value(-n),
            Float64Value(n) => Float64Value(-n),
            RowsAffected(n) => Int64Value(-(n as i64)),
            x => x
        };
        Ok((ms, neg_result))
    }

    fn evaluate_ql_create_index(
        &self,
        index: &Expression,
        columns: &Vec<Expression>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, result) = self.evaluate(index)?;
        match result.clone() {
            Null | Undefined => Ok((ms, result)),
            TableValue(mrc) => todo!(),
            TableRef(path) => {
                let ns = Namespace::parse(path.as_str())?;
                let (ms, columns) = self.evaluate_atoms(columns)?;
                let config = DataFrameConfig::load(&ns)?;
                let mut indices = config.get_indices().clone();
                indices.push(HashIndexConfig::new(columns, false));
                let config = DataFrameConfig::new(
                    config.get_columns().clone(),
                    indices.clone(),
                    config.get_partitions().clone(),
                );
                config.save(&ns)?;
                Ok((ms, Boolean(true)))
            }
            x => fail(format!("Type mismatch: expected an iterable near {}", x))
        }
    }

    fn evaluate_ql_create_table(
        &self,
        table: &Expression,
        columns: &Vec<ColumnJs>,
        from: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, result) = self.evaluate(table)?;
        match result.clone() {
            Null | Undefined => Ok((ms, result)),
            TableValue(mrc) => todo!(),
            TableRef(path) => {
                let ns = Namespace::parse(path.as_str())?;
                let config = DataFrameConfig::new(columns.clone(), vec![], vec![]);
                DataFrame::create(ns, config)?;
                Ok((ms, Boolean(true)))
            }
            x => fail(format!("Type mismatch: expected an iterable near {}", x))
        }
    }

    fn evaluate_ql_declare_index(
        &self,
        columns: &Vec<Expression>,
    ) -> std::io::Result<(Self, TypedValue)> {
        // TODO determine how to implement
        Ok((self.clone(), ErrorValue("Not yet implemented".to_string())))
    }

    fn evaluate_ql_declare_table(
        &self,
        columns: &Vec<ColumnJs>,
        from: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let columns = TableColumn::from_columns(columns)?;
        Ok((self.clone(), TableValue(ModelRowCollection::new(columns, vec![]))))
    }

    fn evaluate_ql_delete(
        &self,
        from: &Expression,
        condition: &Option<Box<Expression>>,
        limit: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, limit) = Self::expand(self.clone(), limit)?;
        let (ms, result) = ms.evaluate(from)?;
        Self::orchestrate_io(ms, result, &vec![], &vec![], condition, limit, |ms, df, _fields, _values, condition, limit| {
            let deleted = df.delete_where(&ms, &condition, limit).unwrap();
            Ok((ms, RowsAffected(deleted)))
        })
    }

    fn evaluate_ql_drop(&self, table: &Expression, if_exists: bool) -> std::io::Result<(Self, TypedValue)> {
        let (ms, table) = self.evaluate(table)?;
        match table {
            TableRef(path) => {
                let ns = Namespace::parse(path.as_str())?;
                let result = fs::remove_file(ns.get_table_file_path());
                Ok((ms, Boolean(result.is_ok())))
            }
            _ => Ok((ms, Boolean(false)))
        }
    }

    fn evaluate_ql_append(
        &self,
        table: &Expression,
        fields: &Vec<Expression>,
        values: &Vec<Expression>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, table) = self.clone().evaluate(table)?;
        Self::orchestrate_io(ms, table, &fields, &values, &None, Undefined, |ms, df, fields, values, condition, limit| {
            let (ms, field_names) = ms.evaluate_atoms(fields)?;
            if let (ms, Array(values)) = ms.evaluate_array(values)? {
                let empty_fields = vec![Field::new(Null); df.get_columns().len()];
                let row = Row::new(0, df.get_columns().clone(), empty_fields);
                let new_row = DataFrame::transform_row(row, field_names, values)?;
                let inserted = df.append(&new_row)?;
                Ok((ms, RowsAffected(inserted)))
            } else { fail("Array expression expected") }
        })
    }

    fn evaluate_ql_into(
        &self,
        table: &Expression,
        source: &Expression,
    ) -> std::io::Result<(MachineState, TypedValue)> {
        if let From(source) = source {
            // determine the writable target
            let mut writable = self.expect_row_collection(table)?;

            // retrieve rows from the source
            let columns = writable.get_columns();
            let rows = self.expect_rows(source, columns)?;

            // write the rows to the target
            let inserted = self.write_rows(&mut writable, rows)?;
            Ok((self.clone(), RowsAffected(inserted)))
        } else {
            fail_expr("A queryable was expected".to_string(), source)
        }
    }

    fn evaluate_ql_ns(&self, expr: &Expression) -> std::io::Result<(Self, TypedValue)> {
        let (ms, result) = self.evaluate(expr)?;
        match result {
            StringValue(path) => Ok((ms, TableRef(path))),
            TableRef(path) => Ok((ms, TableRef(path))),
            x => fail_unexpected("TableRef", &x)
        }
    }

    fn evaluate_ql_overwrite(
        &self,
        table: &Expression,
        source: &Expression,
        condition: &Option<Box<Expression>>,
        limit: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, limit) = Self::expand(self.clone(), limit)?;
        let (ms, tv_table) = ms.evaluate(table)?;
        let (fields, values) = self.expect_via(&table, &source)?;
        Self::orchestrate_io(ms, tv_table, &fields, &values, condition, limit, |ms, df, fields, values, condition, limit| {
            let overwritten = df.overwrite_where(&ms, fields, values, condition, limit).unwrap();
            Ok((ms, RowsAffected(overwritten)))
        })
    }

    fn evaluate_ql_select(
        &self,
        fields: &Vec<Expression>,
        from: &Option<Box<Expression>>,
        condition: &Option<Box<Expression>>,
        _group_by: &Option<Vec<Expression>>,
        _having: &Option<Box<Expression>>,
        _order_by: &Option<Vec<Expression>>,
        limit: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, limit) = Self::expand(self.clone(), limit)?;
        match from {
            None => ms.evaluate_array(fields),
            Some(source) => {
                let (ms, table) = ms.evaluate(source)?;
                Self::orchestrate_io(ms, table, &vec![], &vec![], condition, limit, |ms, df, _, _, condition, limit| {
                    let rows = df.read_where(&ms, condition, limit)?;
                    Ok((ms, TableValue(ModelRowCollection::from_rows(rows))))
                })
            }
        }
    }

    fn evaluate_ql_shall(
        &self,
        expr: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, value) = self.evaluate(expr)?;
        match value {
            Boolean(true) => Ok((ms, Boolean(true))),
            RowsAffected(n) if n >= 0 => Ok((ms, RowsAffected(n))),
            TableRef(path) => Ok((ms, TableRef(path))),
            TableValue(mrc) => Ok((ms, TableValue(mrc))),
            z => fail_value("Expected true, Table(..) or RowsAffected(n) where n >= 1", &z)
        }
    }

    fn evaluate_ql_update(
        &self,
        table: &Expression,
        source: &Expression,
        condition: &Option<Box<Expression>>,
        limit: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, limit) = Self::expand(self.clone(), limit)?;
        let (ms, tv_table) = ms.evaluate(table)?;
        let (fields, values) = self.expect_via(&table, &source)?;
        Self::orchestrate_io(ms, tv_table, &fields, &values, condition, limit, |ms, df, fields, values, condition, limit| {
            let modified = df.update_where(&ms, fields, values, condition, limit).unwrap();
            Ok((ms, RowsAffected(modified)))
        })
    }

    fn evaluate_ql_truncate(
        &self,
        table: &Expression,
        limit: TypedValue,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, table) = self.evaluate(table)?;
        Self::orchestrate_io(ms, table, &vec![], &vec![], &None, limit, |ms, df, _fields, _values, condition, limit| {
            let limit = limit.assume_usize().unwrap_or(0);
            let new_size = df.resize(limit)?;
            Ok((ms, RowsAffected(new_size)))
        })
    }

    /// Evaluates the queryable [Expression] (e.g. from, limit and where)
    /// e.g.: from ns("interpreter.select.stocks") where last_sale > 1.0 limit 1
    fn evaluate_queryable(
        &self,
        src: &Expression,
        condition: &Expression,
        limit: TypedValue,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, result) = self.evaluate(src)?;
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
                Ok((ms, TableValue(ModelRowCollection::from_rows(cursor.take(limit)?))))
            }
            TableRef(name) => {
                let ns = Namespace::parse(name.as_str())?;
                let frc = FileRowCollection::open(&ns)?;
                let mut cursor = Cursor::filter(Box::new(frc), condition.clone());
                Ok((ms, TableValue(ModelRowCollection::from_rows(cursor.take(limit)?))))
            }
            value => fail_value("Queryable expected", &value)
        }
    }

    /// Reverse orders a collection
    pub fn evaluate_reverse(
        &self,
        table: &Box<Expression>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, table) = self.evaluate(table)?;
        match table {
            TableValue(mut mrc) => {
                let ns = Namespace::new("_", "_", "test");
                let df = DataFrame::from_row_collection(ns, Box::new(mrc.clone()));
                let rows = df.reverse()?;
                Ok((ms, TableValue(ModelRowCollection::from_rows(rows))))
            }
            TableRef(name) => {
                let ns = Namespace::parse(name.as_str())?;
                let df = DataFrame::load(ns)?;
                let rows = df.reverse()?;
                Ok((ms, TableValue(ModelRowCollection::from_rows(rows))))
            }
            value => fail_value("Queryable expected", &value)
        }
    }

    /// evaluates the specified [Expression]; returning a [TypedValue] result.
    pub fn evaluate_scope(&self, ops: &Vec<Expression>) -> std::io::Result<(Self, TypedValue)> {
        Ok(ops.iter().fold((self.clone(), Undefined),
                           |(ms, _), op| match ms.evaluate(op) {
                               Ok((ms, tv)) => (ms, tv),
                               Err(err) => panic!("{}", err.to_string())
                           }))
    }

    fn evaluate_while(
        &self,
        condition: &Expression,
        code: &Expression,
    ) -> std::io::Result<(Self, TypedValue)> {
        let mut ms = self.clone();
        let mut is_done = false;
        while !is_done {
            if let (ms1, Boolean(true)) = ms.evaluate(condition)? {
                ms = ms1;
                let (ms2, _) = ms.evaluate(code)?;
                ms = ms2;
            } else { is_done = true }
        }
        Ok((ms, Boolean(true)))
    }

    /// executes the specified instructions on this state machine.
    pub fn execute(&self, ops: &Vec<OpCode>) -> std::io::Result<(Self, TypedValue)> {
        let mut ms = self.clone();
        for op in ops { ms = op(ms)? }
        let (ms, result) = ms.pop_or(Undefined);
        Ok((ms, result))
    }

    /// evaluates the boxed expression and applies the supplied function
    fn expand1(
        &self,
        a: &Expression,
        f: fn(TypedValue) -> TypedValue,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, aa) = self.evaluate(a)?;
        Ok((ms, f(aa)))
    }

    /// evaluates the two boxed expressions and applies the supplied function
    fn expand2(
        &self,
        a: &Expression,
        b: &Expression,
        f: fn(TypedValue, TypedValue) -> TypedValue,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, aa) = self.evaluate(a)?;
        let (ms, bb) = ms.evaluate(b)?;
        Ok((ms, f(aa, bb)))
    }

    /// evaluates the three boxed expressions and applies the supplied function
    fn expand3(
        &self,
        a: &Expression,
        b: &Expression,
        c: &Expression,
        f: fn(TypedValue, TypedValue, TypedValue) -> TypedValue,
    ) -> std::io::Result<(Self, TypedValue)> {
        let (ms, aa) = self.evaluate(a)?;
        let (ms, bb) = ms.evaluate(b)?;
        let (ms, cc) = ms.evaluate(c)?;
        Ok((ms, f(aa, bb, cc)))
    }

    fn expect_row_collection(&self, table: &Expression) -> std::io::Result<Box<dyn RowCollection>> {
        match table {
            Ns(path) =>
                Self::load_row_collection(self.expect_string(path)?),
            table => {
                let (_, table) = self.evaluate(table)?;
                match table {
                    TableRef(path) => Self::load_row_collection(path),
                    TableValue(mrc) => Ok(Box::new(mrc)),
                    x => fail_unexpected("TableRef", &x)
                }
            }
            e => fail_expr("A writable was expected".to_string(), e)
        }
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
            TableRef(path) => {
                let ns = Namespace::parse(path.as_str())?;
                let frc = FileRowCollection::open(&ns)?;
                frc.read_range(0..frc.len()?)
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
        let mut writable = self.expect_row_collection(table)?;
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
        let (ms, result) = self.pop();
        (ms, result.unwrap_or(default_value))
    }

    /// pushes a value unto the stack
    pub fn push(&self, value: TypedValue) -> Self {
        let mut stack = self.stack.clone();
        stack.push(value);
        Self::construct(stack, self.variables.clone())
    }

    /// pushes a collection of values unto the stack
    pub fn push_all(&self, values: Vec<TypedValue>) -> Self {
        values.iter().fold(self.clone(), |ms, tv| ms.push(tv.clone()))
    }

    pub fn set(&self, name: &str, value: TypedValue) -> Self {
        let mut variables = self.variables.clone();
        variables.insert(name.to_string(), value);
        Self::construct(self.stack.clone(), variables)
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
        row.get_fields().iter().zip(row.get_columns().iter())
            .fold(self.clone(), |ms, (f, c)| {
                ms.with_variable(c.get_name(), f.value.clone())
            })
    }

    pub fn with_variable(&self, name: &str, value: TypedValue) -> Self {
        let mut variables = self.variables.clone();
        variables.insert(name.to_string(), value);
        Self::construct(self.stack.clone(), variables)
    }

    fn write_rows(&self, writable: &mut Box<dyn RowCollection>, rows: Vec<Row>) -> std::io::Result<usize> {
        let mut written = 0;
        for row in rows {
            let new_id = writable.len()?;
            written += writable.overwrite(new_id, &row.with_row_id(new_id))?
        }
        Ok(written)
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::compiler::CompilerState;
    use crate::expression::{FALSE, NULL, TRUE};
    use crate::expression::Expression::*;
    use crate::table_columns::TableColumn;
    use crate::testdata::{make_dataframe_ns, make_quote, make_quote_columns, make_table_columns};

    use super::*;

    #[test]
    fn test_array() {
        let models = vec![Literal(Float64Value(3.25)), TRUE, FALSE, NULL, UNDEFINED];
        assert_eq!(models.iter().map(|e| e.to_code()).collect::<Vec<String>>(), vec![
            "3.25", "true", "false", "null", "undefined",
        ]);

        let (_, array) = MachineState::new().evaluate_array(&models).unwrap();
        assert_eq!(array, Array(vec![Float64Value(3.25), Boolean(true), Boolean(false), Null, Undefined]));
    }

    #[test]
    fn test_as() {
        let model = AsValue(
            "symbol".to_string(),
            Box::new(Literal(StringValue("ABC".into()))),
        );
        assert_eq!(model.to_code(), "symbol: \"ABC\"");

        let ms = MachineState::new();
        let (ms, tv) = ms.evaluate(&model).unwrap();
        assert_eq!(tv, StringValue("ABC".into()));
        assert_eq!(ms.get("symbol"), Some(StringValue("ABC".into())));
    }

    #[test]
    fn test_column_set() {
        let model = ColumnSet(make_quote_columns());
        assert_eq!(model.to_code(), "(symbol: String(8), exchange: String(8), last_sale: f64)");

        let ms = MachineState::new()
            .with_variable("symbol", StringValue("ABC".into()))
            .with_variable("exchange", StringValue("NYSE".into()))
            .with_variable("last_sale", Float64Value(12.66));
        let (_, result) = ms.evaluate(&model).unwrap();
        assert_eq!(result, Array(vec![
            StringValue("ABC".into()),
            StringValue("NYSE".into()),
            Float64Value(12.66),
        ]));
    }

    #[test]
    fn test_divide() {
        let ms = MachineState::new().with_variable("x", Int64Value(50));
        let model = Divide(Box::new(Variable("x".into())), Box::new(Literal(Int64Value(7))));
        assert_eq!(model.to_code(), "x / 7");

        let (ms, result) = ms.evaluate(&model).unwrap();
        assert_eq!(result, Int64Value(7));
        assert_eq!(ms.get("x"), Some(Int64Value(50)));
    }

    #[test]
    fn test_factorial() {
        let model = Factorial(Box::new(Literal(Float64Value(6.))));
        assert_eq!(model.to_code(), "¡6");

        let ms = MachineState::new();
        let (_, result) = ms.evaluate(&model).unwrap();
        assert_eq!(result, Float64Value(720.))
    }

    #[test]
    fn test_eval() {
        let model = Eval(Box::new(Literal(StringValue("5 + 7".to_string()))));
        assert_eq!(model.to_code(), "eval \"5 + 7\"");

        let ms = MachineState::new();
        let (_, result) = ms.evaluate(&model).unwrap();
        assert_eq!(result, Int64Value(12))
    }

    #[test]
    fn test_if_1() {
        let model = If {
            condition: Box::new(GreaterThan(
                Box::new(Variable("num".into())),
                Box::new(Literal(Int64Value(25))),
            )),
            a: Box::new(Literal(StringValue("Yes".into()))),
            b: Some(Box::new(Literal(StringValue("No".into())))),
        };
        assert_eq!(model.to_code(), r#"if num > 25 "Yes" else "No""#);

        let ms = MachineState::new().with_variable("num", Int64Value(5));
        let (_, result) = ms.evaluate(&model).unwrap();
        assert_eq!(result, StringValue("No".into()));

        let ms = MachineState::new().with_variable("num", Int64Value(37));
        let (_, result) = ms.evaluate(&model).unwrap();
        assert_eq!(result, StringValue("Yes".into()));
    }

    #[test]
    fn test_if_2() {
        let model = If {
            condition: Box::new(LessThan(
                Box::new(Variable("num".into())),
                Box::new(Literal(Int64Value(10))),
            )),
            a: Box::new(Literal(StringValue("Yes".into()))),
            b: Some(Box::new(Literal(StringValue("No".into())))),
        };
        assert_eq!(model.to_code(), r#"if num < 10 "Yes" else "No""#);

        let ms = MachineState::new().with_variable("num", Int64Value(5));
        let (_, result) = ms.evaluate(&model).unwrap();
        assert_eq!(result, StringValue("Yes".into()));

        let ms = MachineState::new().with_variable("num", Int64Value(99));
        let (_, result) = ms.evaluate(&model).unwrap();
        assert_eq!(result, StringValue("No".into()));
    }

    #[test]
    fn test_create_table() {
        let model = Create {
            path: Box::new(Ns(Box::new(Literal(StringValue("machine.create.stocks".into()))))),
            entity: TableEntity {
                columns: make_quote_columns(),
                from: None,
            },
        };

        // create the table
        let ms = MachineState::new();
        let (_, result) = ms.evaluate(&model).unwrap();
        assert_eq!(result, Boolean(true));

        // decompile back to source code
        assert_eq!(
            model.to_code(),
            "create table ns(\"machine.create.stocks\") (symbol: String(8), exchange: String(8), last_sale: f64)"
        );
    }

    #[test]
    fn test_create_table_with_index() {
        let path = "machine.index.stocks";
        let ms = MachineState::new();

        // drop table if exists ns("machine.index.stocks")
        let (ms, result) = ms.evaluate(&Drop(TableTarget {
            path: Box::new(Ns(Box::new(Literal(StringValue(path.into()))))),
            if_exists: true,
        })).unwrap();

        // create table ns("machine.index.stocks") (...)
        let (ms, result) = ms.evaluate(&Create {
            path: Box::new(Ns(Box::new(Literal(StringValue(path.into()))))),
            entity: TableEntity {
                columns: make_quote_columns(),
                from: None,
            },
        }).unwrap();
        assert_eq!(result, Boolean(true));

        // create index ns("machine.index.stocks") [symbol, exchange]
        let model = Create {
            path: Box::new(Ns(Box::new(Literal(StringValue(path.into()))))),
            entity: IndexEntity {
                columns: vec![
                    Variable("symbol".into()),
                    Variable("exchange".into()),
                ],
            },
        };
        let (_, result) = ms.evaluate(&model).unwrap();
        assert_eq!(result, Boolean(true));

        // decompile back to source code
        assert_eq!(
            model.to_code(),
            "create index ns(\"machine.index.stocks\") [symbol, exchange]"
        );
    }

    #[test]
    fn test_declare_table() {
        let model = Declare(TableEntity {
            columns: vec![
                ColumnJs::new("symbol", "String(8)", None),
                ColumnJs::new("exchange", "String(8)", None),
                ColumnJs::new("last_sale", "f64", None),
            ],
            from: None,
        });

        let ms = MachineState::new();
        let (ms, result) = ms.evaluate(&model).unwrap();
        assert_eq!(result, TypedValue::TableValue(ModelRowCollection::new(
            make_table_columns(), vec![],
        )))
    }

    #[test]
    fn test_drop_table() {
        // create a table with test data
        let ns_path = "machine.drop.stocks";
        let ns = Ns(Box::new(Literal(StringValue(ns_path.into()))));
        let (df, phys_columns) = create_file_table(ns_path);

        let model = Drop(TableTarget { path: Box::new(ns), if_exists: false });
        assert_eq!(model.to_code(), "drop table ns(\"machine.drop.stocks\")");

        let ms = MachineState::new();
        let (_, result) = ms.evaluate(&model).unwrap();
        assert_eq!(result, Boolean(true))
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
        let (ms, result) = MachineState::new()
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
        let ms = MachineState::new();
        let (ms, result) = ms.evaluate_scope(&vec![
            SetVariable("add".to_string(), Box::new(Literal(fx.clone())))
        ]).unwrap();
        assert_eq!(ms.get("add").unwrap(), fx);
        assert_eq!(result, TypedValue::Boolean(true));

        // execute the function via function call in scope: add(2, 3)
        let model = FunctionCall {
            fx: Box::new(Literal(fx)),
            args: vec![
                Literal(Int64Value(2)),
                Literal(Int64Value(3)),
            ],
        };
        let (ms, result) = ms
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
        let ms = MachineState::new()
            .with_variable("stocks", TableValue(ModelRowCollection::from_rows(vec![
                make_quote(0, &phys_columns, "ABC", "AMEX", 12.33),
                make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
                make_quote(2, &phys_columns, "BIZ", "NYSE", 9.775),
                make_quote(3, &phys_columns, "GOTO", "OTC", 0.1442),
                make_quote(4, &phys_columns, "XYZ", "NYSE", 0.0289),
            ])));

        let model = Limit {
            from: Box::new(Where {
                from: Box::new(From(Box::new(Variable("stocks".into())))),
                condition: Box::new(GreaterOrEqual(
                    Box::new(Variable("last_sale".into())),
                    Box::new(Literal(Float64Value(1.0))),
                )),
            }),
            limit: Box::new(Literal(Int64Value(2))),
        };
        assert_eq!(model.to_code(), "from stocks where last_sale >= 1 limit 2");

        let (_, result) = ms.evaluate(&model).unwrap();
        assert_eq!(result, TableValue(ModelRowCollection::from_rows(vec![
            make_quote(0, &phys_columns, "ABC", "AMEX", 12.33),
            make_quote(2, &phys_columns, "BIZ", "NYSE", 9.775),
        ])));
    }

    #[test]
    fn test_delete_rows_from_namespace() {
        // create a table with test data
        let ns_path = "machine.delete.stocks";
        let ns = Ns(Box::new(Literal(StringValue(ns_path.into()))));
        let (df, phys_columns) = create_file_table(ns_path);

        // delete some rows
        let ms = MachineState::new();
        let code = CompilerState::compile_script(r#"
            delete from ns("machine.delete.stocks")
            where last_sale > 1.0
            "#).unwrap();
        let (_, result) = ms.evaluate(&code).unwrap();
        assert_eq!(result, RowsAffected(3));

        // verify the remaining rows
        let rows = df.read_fully().unwrap();
        assert_eq!(rows, vec![
            make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
            make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428),
        ]);
    }

    #[test]
    fn test_append_namespace() {
        // create a table with test data
        let ns_path = "machine.into.stocks";
        let ns = Ns(Box::new(Literal(StringValue(ns_path.into()))));
        let (df, phys_columns) = create_file_table(ns_path);

        // insert some rows
        let ms = MachineState::new();
        let (_, result) = ms.evaluate(&Append {
            path: Box::new(Ns(Box::new(Literal(StringValue(ns_path.to_string()))))),
            source: Box::new(From(Box::new(JSONLiteral(vec![
                ("symbol".into(), Literal(StringValue("REX".into()))),
                ("exchange".into(), Literal(StringValue("NASDAQ".into()))),
                ("last_sale".into(), Literal(Float64Value(16.99))),
            ])))),
        }).unwrap();
        assert_eq!(result, RowsAffected(1));

        // verify the remaining rows
        let rows = df.read_fully().unwrap();
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
    fn test_into_ns_from_variable() {
        let model = IntoTable {
            target: Box::new(Ns(Box::new(Literal(StringValue("machine.staging.stocks".to_string()))))),
            source: Box::new(From(Box::new(Variable("stocks".to_string())))),
        };
        let columns = make_quote_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let ms = MachineState::new()
            .with_variable("stocks", TableValue(ModelRowCollection::from_rows(vec![
                make_quote(0, &phys_columns, "ABC", "AMEX", 12.33),
                make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
                make_quote(2, &phys_columns, "BIZ", "NYSE", 9.775),
                make_quote(3, &phys_columns, "GOTO", "OTC", 0.1442),
                make_quote(4, &phys_columns, "XYZ", "NYSE", 0.0289),
            ])));
        let (_, result) = ms.evaluate(&model).unwrap();
        assert_eq!(result, RowsAffected(5))
    }

    #[test]
    fn test_overwrite_rows_in_namespace() {
        let ns_path = "machine.overwrite.stocks";
        let model = Overwrite {
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
        };
        assert_eq!(model.to_code(), r#"overwrite ns("machine.overwrite.stocks") via {symbol: "BOOM", exchange: "NYSE", last_sale: 56.99} where symbol == "BOOM""#);

        // create a table with test data
        let ns = Ns(Box::new(Literal(StringValue(ns_path.into()))));
        let (df, phys_columns) = create_file_table(ns_path);

        // overwrite some rows
        let ms = MachineState::new();
        let (_, result) = ms.evaluate(&model).unwrap();
        assert_eq!(result, RowsAffected(1));

        // verify the remaining rows
        let rows = df.read_fully().unwrap();
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

        let (_ms, result) = MachineState::new().evaluate_scope(&opcodes).unwrap();
        assert_eq!(result, Float64Value(14.))
    }

    #[test]
    fn test_push_all() {
        let ms = MachineState::new().push_all(vec![
            Float32Value(2.), Float64Value(3.),
            Int16Value(4), Int32Value(5),
            Int64Value(6), StringValue("Hello World".into()),
        ]);
        assert_eq!(ms.stack, vec![
            Float32Value(2.), Float64Value(3.),
            Int16Value(4), Int32Value(5),
            Int64Value(6), StringValue("Hello World".into()),
        ])
    }

    #[test]
    fn test_reverse_from_variable() {
        let model = Reverse(Box::new(From(Box::new(Variable("stocks".to_string())))));
        let phys_columns = make_table_columns();
        let ms = MachineState::new()
            .with_variable("stocks", TableValue(ModelRowCollection::from_rows(vec![
                make_quote(0, &phys_columns, "ABC", "AMEX", 12.33),
                make_quote(1, &phys_columns, "GAS.Q", "OTC", 0.2456),
                make_quote(2, &phys_columns, "BASH", "NYSE", 13.11),
                make_quote(3, &phys_columns, "GOTO", "OTC", 0.1442),
                make_quote(4, &phys_columns, "VAPOR", "NYSE", 0.0289),
            ])));
        let (ms, value) = ms.evaluate(&model).unwrap();
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
        let (df, phys_columns) =
            create_file_table("machine.select.stocks");

        // execute the query
        let ms = MachineState::new();
        let (_, result) = ms.evaluate(&Select {
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
        }).unwrap();
        assert_eq!(result, TableValue(ModelRowCollection::from_rows(vec![
            make_quote(0, &phys_columns, "ABC", "AMEX", 11.77),
            make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66),
            make_quote(4, &phys_columns, "BOOM", "NASDAQ", 56.87),
        ])));
    }

    #[test]
    fn test_select_from_variable() {
        let (mrc, phys_columns) = create_memory_table();
        let ms = MachineState::new()
            .with_variable("stocks", TableValue(mrc));

        // execute the code
        let (_, result) = ms.evaluate(&Select {
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
        }).unwrap();
        assert_eq!(result, TableValue(ModelRowCollection::from_rows(vec![
            make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
            make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428),
        ])));
    }

    #[ignore]
    #[test]
    fn test_update_rows_in_memory() {
        let model = Update {
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
        };

        // perform the update and verify
        let (mrc, phys_columns) = create_memory_table();
        let ms = MachineState::new().with_variable("stocks", TableValue(mrc));
        let (_, result) = ms.evaluate(&model).unwrap();
        assert_eq!(result, RowsAffected(2));

        // retrieve and verify all rows
        let model = From(Box::new(Variable("stocks".into())));
        let (ms, result) = ms.evaluate(&model).unwrap();
        assert_eq!(ms.get("stocks").unwrap(), TableValue(ModelRowCollection::from_rows(vec![
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
        let ns = Ns(Box::new(Literal(StringValue(ns_path.into()))));
        let (df, phys_columns) = create_file_table(ns_path);

        // create the instruction model
        let model = Update {
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
        };

        // update some rows
        let (_, delta) = MachineState::new().evaluate(&model).unwrap();
        assert_eq!(delta, RowsAffected(2));

        // verify the rows
        let rows = df.read_fully().unwrap();
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
        let model = Truncate {
            path: Box::new(Variable("stocks".into())),
            limit: Some(Box::new(Literal(Int64Value(0)))),
        };

        let (mrc, phys_columns) = create_memory_table();
        let ms = MachineState::new().with_variable("stocks", TableValue(mrc));
        let (_, result) = ms.evaluate(&model).unwrap();
        assert_eq!(result, RowsAffected(1))
    }

    #[test]
    fn test_variables() {
        let ms = MachineState::new()
            .with_variable("abc", Int32Value(5))
            .with_variable("xyz", Int32Value(58));
        assert_eq!(ms.get("abc"), Some(Int32Value(5)));
        assert_eq!(ms.get("xyz"), Some(Int32Value(58)));
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
        assert_eq!(model.to_code(), "while num < 5 do num := num + 1");

        let ms = MachineState::new().with_variable("num", Int64Value(0));
        let (ms, result) = ms.evaluate(&model).unwrap();
        assert_eq!(ms.get("num"), Some(Int64Value(5)))
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
        assert_eq!(1, df.append(&make_quote(0, &phys_columns, "ABC", "AMEX", 11.77)).unwrap());
        assert_eq!(1, df.append(&make_quote(1, &phys_columns, "UNO", "OTC", 0.2456)).unwrap());
        assert_eq!(1, df.append(&make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66)).unwrap());
        assert_eq!(1, df.append(&make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428)).unwrap());
        assert_eq!(1, df.append(&make_quote(4, &phys_columns, "BOOM", "NASDAQ", 56.87)).unwrap());
        (df, phys_columns)
    }
}