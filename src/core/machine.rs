////////////////////////////////////////////////////////////////////
// state machine module
////////////////////////////////////////////////////////////////////

use std::collections::HashMap;

use log::error;
use serde::{Deserialize, Serialize};

use shared_lib::fail;

use crate::compiler::{fail_expr, fail_value};
use crate::cursor::Cursor;
use crate::dataframes::DataFrame;
use crate::expression::{Expression, UNDEFINED};
use crate::expression::Expression::Variable;
use crate::fields::Field;
use crate::file_row_collection::FileRowCollection;
use crate::model_row_collection::ModelRowCollection;
use crate::namespaces::Namespace;
use crate::rows::Row;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::*;

/// Represents the state of the machine.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MachineState {
    stack: Vec<TypedValue>,
    variables: HashMap<String, TypedValue>,
}

impl MachineState {
    /// creates a new empty state machine
    pub fn new() -> Self {
        Self::construct(Vec::new(), HashMap::new())
    }

    /// creates a new state machine
    pub fn construct(stack: Vec<TypedValue>, variables: HashMap<String, TypedValue>) -> Self {
        Self { stack, variables }
    }

    pub fn eval_if(
        &self,
        condition: &Expression,
        a: &Expression,
        b: &Option<Box<Expression>>,
    ) -> std::io::Result<(Self, TypedValue)> {
        let ms0 = self.clone();
        let (ms, result) = ms0.evaluate(condition)?;
        match result {
            Boolean(is_true) =>
                if is_true {
                    Ok((ms0, ms.evaluate(a)?.1))
                } else if let Some(expr) = b {
                    Ok((ms0, ms.evaluate(expr)?.1))
                } else { Ok((ms0, Undefined)) },
            other => fail(format!("Expected Boolean, but got {}", other.unwrap_value()))
        }
    }

    /// evaluates the specified [Expression]; returning a [TypedValue] result.
    pub fn evaluate(&self, expression: &Expression) -> std::io::Result<(MachineState, TypedValue)> {
        use crate::expression::Expression::*;
        match expression {
            And(a, b) =>
                self.expand2(a, b, |aa, bb| aa.and(&bb).unwrap_or(Undefined)),
            Between(a, b, c) =>
                self.expand3(a, b, c, |aa, bb, cc| Boolean((aa >= bb) && (aa <= cc))),
            CodeBlock(ops) => self.evaluate_all(ops),
            Collection(items) => self.evaluate_array(items),
            Delete { table, condition, limit } =>
                self.sql_delete(table, condition, limit),
            Divide(a, b) =>
                self.expand2(a, b, |aa, bb| aa / bb),
            Equal(a, b) =>
                self.expand2(a, b, |aa, bb| Boolean(aa == bb)),
            Factorial(a) => self.expand1(a, |aa| aa.factorial().unwrap_or(Undefined)),
            From(src) => self.evaluate_from_where_limit(src, &UNDEFINED, Undefined),
            GreaterThan(a, b) =>
                self.expand2(a, b, |aa, bb| Boolean(aa > bb)),
            GreaterOrEqual(a, b) =>
                self.expand2(a, b, |aa, bb| Boolean(aa >= bb)),
            If { condition, a, b } =>
                self.eval_if(condition, a, b),
            InsertInto { table, fields, values } =>
                self.sql_append(table, fields, values),
            LessThan(a, b) =>
                self.expand2(a, b, |aa, bb| Boolean(aa < bb)),
            LessOrEqual(a, b) =>
                self.expand2(a, b, |aa, bb| Boolean(aa <= bb)),
            Limit { from, limit } => {
                let (ms, limit) = self.evaluate(limit)?;
                ms.evaluate_from_where_limit(from, &UNDEFINED, limit)
            }
            Literal(value) => Ok((self.clone(), value.clone())),
            Minus(a, b) =>
                self.expand2(a, b, |aa, bb| aa - bb),
            Modulo(a, b) =>
                self.expand2(a, b, |aa, bb| aa % bb),
            Multiply(a, b) =>
                self.expand2(a, b, |aa, bb| aa * bb),
            Not(a) => self.expand1(a, |aa| !aa),
            NotEqual(a, b) =>
                self.expand2(a, b, |aa, bb| Boolean(aa != bb)),
            Ns(a) => self.sql_ns(a),
            Or(a, b) =>
                self.expand2(a, b, |aa, bb| aa.or(&bb).unwrap_or(Undefined)),
            Overwrite { table, fields, values, condition, limit } =>
                self.sql_overwrite(table, fields, values, condition, limit),
            Plus(a, b) =>
                self.expand2(a, b, |aa, bb| aa + bb),
            Pow(a, b) =>
                self.expand2(a, b, |aa, bb| aa.pow(&bb).unwrap_or(Undefined)),
            Range(a, b) =>
                self.expand2(a, b, |aa, bb| aa.range(&bb).unwrap_or(Undefined)),
            Select { fields, from, condition, group_by, having, order_by, limit } =>
                self.sql_select(fields, from, condition, group_by, having, order_by, limit),
            SetVariable(name, expr) => {
                let (ms, value) = self.evaluate(expr)?;
                Ok((ms.set(name, value), Undefined))
            }
            ShiftLeft(a, b) =>
                self.expand2(a, b, |aa, bb| aa << bb),
            ShiftRight(a, b) =>
                self.expand2(a, b, |aa, bb| aa >> bb),
            Tuple(values) => self.evaluate_array(values),
            Variable(name) => Ok((self.clone(), self.get(&name).unwrap_or(Undefined))),
            Xor(a, b) =>
                self.expand2(a, b, |aa, bb| aa ^ bb),
            Where { from, condition } =>
                self.evaluate_from_where_limit(from, condition, Undefined),
            other => fail_expr("Unhandled expression", other)
        }
    }

    /// evaluates the specified [Expression]; returning a [TypedValue] result.
    pub fn evaluate_all(&self, ops: &Vec<Expression>) -> std::io::Result<(MachineState, TypedValue)> {
        Ok(ops.iter().fold((self.clone(), Undefined),
                           |(ms, _), op| match ms.evaluate(op) {
                               Ok((ms, tv)) => (ms, tv),
                               Err(err) => panic!("{}", err.to_string())
                           }))
    }

    /// evaluates the specified [Expression]; returning an array ([TypedValue]) result.
    pub fn evaluate_array(&self, ops: &Vec<Expression>) -> std::io::Result<(MachineState, TypedValue)> {
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
    pub fn evaluate_atoms(&self, ops: &Vec<Expression>) -> std::io::Result<(MachineState, Vec<String>)> {
        let (ms, results) = ops.iter()
            .fold((self.clone(), vec![]),
                  |(ms, mut array), op| match op {
                      Variable(name) => {
                          array.push(name.to_string());
                          (ms, array)
                      }
                      expr => panic!("Expected a column, got \"{:?}\" instead", expr),
                  });
        Ok((ms, results))
    }

    /// evaluates the queryable [Expression]
    pub fn evaluate_from_where_limit(
        &self,
        src: &Expression,
        condition: &Expression,
        limit: TypedValue,
    ) -> std::io::Result<(MachineState, TypedValue)> {
        let (ms, result) = self.evaluate(src)?;
        let limit = match limit {
            Null | Undefined => None,
            value => value.assume_usize()
        }.unwrap_or(usize::MAX);
        match result {
            MemoryTable(brc) => {
                let mut cursor = Cursor::filter(Box::new(brc), condition.clone());
                Ok((ms, MemoryTable(ModelRowCollection::from_rows(cursor.take(limit)?))))
            }
            TableRef(name) => {
                let ns = Namespace::parse(name.as_str())?;
                let rc = FileRowCollection::open(&ns)?;
                let mut cursor = Cursor::filter(Box::new(rc), condition.clone());
                Ok((ms, MemoryTable(ModelRowCollection::from_rows(cursor.take(limit)?))))
            }
            value => fail_value("Queryable expected", &value)
        }
    }

    /// executes the specified instructions on this state machine.
    pub fn execute(&self, ops: &Vec<OpCode>) -> std::io::Result<(MachineState, TypedValue)> {
        let mut ms = self.clone();
        for op in ops { ms = op(&ms)? }
        let (ms, result) = ms.pop_or(Undefined);
        Ok((ms, result))
    }

    fn expand(ms: MachineState, expr: &Option<Box<Expression>>) -> std::io::Result<(MachineState, TypedValue)> {
        match expr {
            Some(item) => ms.evaluate(item),
            None => Ok((ms, Undefined))
        }
    }

    fn expand_vec(ms: MachineState, expr: &Option<Vec<Expression>>) -> std::io::Result<(MachineState, TypedValue)> {
        match expr {
            Some(items) => ms.evaluate_array(items),
            None => Ok((ms, Undefined))
        }
    }

    /// evaluates the boxed expression and applies the supplied function
    fn expand1(
        &self,
        a: &Box<Expression>,
        f: fn(TypedValue) -> TypedValue,
    ) -> std::io::Result<(MachineState, TypedValue)> {
        let (ms, aa) = self.evaluate(a)?;
        Ok((ms, f(aa)))
    }

    /// evaluates the two boxed expressions and applies the supplied function
    fn expand2(
        &self,
        a: &Box<Expression>,
        b: &Box<Expression>,
        f: fn(TypedValue, TypedValue) -> TypedValue,
    ) -> std::io::Result<(MachineState, TypedValue)> {
        let (ms, aa) = self.evaluate(a)?;
        let (ms, bb) = ms.evaluate(b)?;
        Ok((ms, f(aa, bb)))
    }

    /// evaluates the three boxed expressions and applies the supplied function
    fn expand3(
        &self,
        a: &Box<Expression>,
        b: &Box<Expression>,
        c: &Box<Expression>,
        f: fn(TypedValue, TypedValue, TypedValue) -> TypedValue,
    ) -> std::io::Result<(MachineState, TypedValue)> {
        let (ms, aa) = self.evaluate(a)?;
        let (ms, bb) = ms.evaluate(b)?;
        let (ms, cc) = ms.evaluate(c)?;
        Ok((ms, f(aa, bb, cc)))
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

    fn orchestrate_io(
        ms: MachineState,
        table: TypedValue,
        fields: &Vec<Expression>,
        values: &Vec<Expression>,
        condition: &Option<Box<Expression>>,
        limit: TypedValue,
        f: fn(MachineState, &mut DataFrame, &Vec<Expression>, &Vec<Expression>, &Option<Box<Expression>>, TypedValue) -> std::io::Result<(MachineState, TypedValue)>,
    ) -> std::io::Result<(MachineState, TypedValue)> {
        match table {
            MemoryTable(brc) => {
                let mut df = DataFrame::from_row_collection(Namespace::temp(), Box::new(brc));
                f(ms, &mut df, fields, values, condition, limit)
            }
            Null | Undefined => Ok((ms, Undefined)),
            StringValue(path) | TableRef(path) => {
                let ns = Namespace::parse(path.as_str())?;
                let mut df = DataFrame::load(ns)?;
                f(ms, &mut df, fields, values, condition, limit)
            }
            x => fail(format!("Type mismatch: expected an iterable: {}", x))
        }
    }

    /// returns the option of a value from the stack
    pub fn pop(&self) -> (Self, Option<TypedValue>) {
        let mut stack = self.stack.clone();
        let value = stack.pop();
        let variables = self.variables.clone();
        (MachineState::construct(stack, variables), value)
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
        MachineState::construct(stack, self.variables.clone())
    }

    /// pushes a collection of values unto the stack
    pub fn push_all(&self, values: Vec<TypedValue>) -> Self {
        values.iter().fold(self.clone(), |ms, tv| ms.push(tv.clone()))
    }

    pub fn set(&self, name: &str, value: TypedValue) -> Self {
        let mut variables = self.variables.clone();
        variables.insert(name.to_string(), value);
        MachineState::construct(self.stack.clone(), variables)
    }

    fn sql_append(
        &self,
        table: &Expression,
        fields: &Vec<Expression>,
        values: &Vec<Expression>,
    ) -> std::io::Result<(MachineState, TypedValue)> {
        let (ms, table) = self.clone().evaluate(table)?;
        Self::orchestrate_io(ms, table, fields, values, &None, Undefined, |ms, df, fields, values, condition, limit| {
            let (ms, field_names) = ms.evaluate_atoms(fields)?;
            if let (ms, Array(values)) = ms.evaluate_array(values)? {
                let empty_fields = vec![Field::new(Null); df.get_columns().len()];
                let row = Row::new(0, df.get_columns().clone(), empty_fields);
                let new_row = DataFrame::transform_row(row, field_names, values)?;
                let inserted = df.append(&new_row)?;
                Ok((ms, Int64Value(inserted as i64)))
            } else { fail("Array expression expected") }
        })
    }

    fn sql_delete(
        &self,
        from: &Box<Expression>,
        condition: &Option<Box<Expression>>,
        limit: &Option<Box<Expression>>,
    ) -> std::io::Result<(MachineState, TypedValue)> {
        let (ms, limit) = Self::expand(self.clone(), limit)?;
        let (ms, result) = ms.evaluate(from)?;
        Self::orchestrate_io(ms, result, &vec![], &vec![], condition, limit, |ms, df, _fields, _values, condition, limit| {
            let deleted = df.delete_where(&ms, &condition, limit).unwrap();
            Ok((ms, Int64Value(deleted as i64)))
        })
    }

    fn sql_ns(&self, expr: &Box<Expression>) -> std::io::Result<(MachineState, TypedValue)> {
        let (ms, result) = self.evaluate(expr)?;
        match result {
            StringValue(path) => Ok((ms, TableRef(path))),
            TableRef(path) => Ok((ms, TableRef(path))),
            x => fail(format!("Expected a TableRef but got '{}'", x))
        }
    }

    fn sql_overwrite(
        &self,
        table: &Box<Expression>,
        fields: &Vec<Expression>,
        values: &Vec<Expression>,
        condition: &Option<Box<Expression>>,
        limit: &Option<Box<Expression>>,
    ) -> std::io::Result<(MachineState, TypedValue)> {
        println!("100");
        let (ms, limit) = Self::expand(self.clone(), limit)?;
        println!("200");
        let (ms, table) = ms.evaluate(table)?;
        println!("300");
        Self::orchestrate_io(ms, table, fields, values, condition, limit, |ms, df, fields, values, condition, limit| {
            let overwritten = df.overwrite_where(&ms, fields, values, condition, limit).unwrap();
            Ok((ms, Int64Value(overwritten as i64)))
        })
    }

    fn sql_select(
        &self,
        fields: &Vec<Expression>,
        from: &Option<Box<Expression>>,
        condition: &Option<Box<Expression>>,
        _group_by: &Option<Vec<Expression>>,
        _having: &Option<Box<Expression>>,
        _order_by: &Option<Vec<Expression>>,
        limit: &Option<Box<Expression>>,
    ) -> std::io::Result<(MachineState, TypedValue)> {
        let (ms, limit) = Self::expand(self.clone(), limit)?;
        match from {
            None => ms.evaluate_array(fields),
            Some(source) => {
                let (ms, table) = ms.evaluate(source)?;
                Self::orchestrate_io(ms, table, &vec![], &vec![], condition, limit, |ms, df, _, _, condition, limit| {
                    let rows = df.read_where(&ms, condition, limit)?;
                    Ok((ms, MemoryTable(ModelRowCollection::from_rows(rows))))
                })
            }
        }
    }

    fn sql_update(
        &self,
        table: &Box<Expression>,
        fields: &Vec<Expression>,
        values: &Vec<Expression>,
        condition: &Option<Box<Expression>>,
        limit: &Option<Box<Expression>>,
    ) -> std::io::Result<(MachineState, TypedValue)> {
        println!("100");
        let (ms, limit) = Self::expand(self.clone(), limit)?;
        println!("200");
        let (ms, table) = ms.evaluate(table)?;
        println!("300");
        Self::orchestrate_io(ms, table, fields, values, condition, limit, |ms, df, fields, values, condition, limit| {
            let overwritten = df.overwrite_where(&ms, fields, values, condition, limit).unwrap();
            Ok((ms, Int64Value(overwritten as i64)))
        })
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
            Int8Value(n) => Ok(self.push(fi(n as i64))),
            Int16Value(n) => Ok(self.push(fi(n as i64))),
            Int32Value(n) => Ok(self.push(fi(n as i64))),
            Int64Value(n) => Ok(self.push(fi(n))),
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
        MachineState::construct(self.stack.clone(), variables)
    }
}

/// represents an executable instruction (opcode)
pub type OpCode = fn(&MachineState) -> std::io::Result<MachineState>;

// Unit tests
#[cfg(test)]
mod tests {
    use crate::compiler::Compiler;
    use crate::expression::{FALSE, NULL, TRUE};
    use crate::expression::Expression::{Divide, Equal, Factorial, From, GreaterOrEqual, GreaterThan, If, InsertInto, LessThan, Limit, Literal, Ns, Overwrite, Variable, Where};
    use crate::table_columns::TableColumn;
    use crate::testdata::{make_columns, make_dataframe_ns, make_quote};

    use super::*;

    #[test]
    fn test_evaluate_all_factorial() {
        let ms = MachineState::new();
        let opcodes = vec![
            Factorial(Box::new(Literal(Float64Value(6.))))
        ];
        let (_ms, result) = ms.evaluate_all(&opcodes).unwrap();
        assert_eq!(result, Float64Value(720.))
    }

    #[test]
    fn test_evaluate_array() {
        let ms = MachineState::new();
        let (_, array) =
            ms.evaluate_array(&vec![Literal(Float64Value(3.25)), TRUE, FALSE, NULL]).unwrap();
        assert_eq!(array, Array(vec![Float64Value(3.25), Boolean(true), Boolean(false), Null]));
    }

    #[test]
    fn test_if_1() {
        // if n > 25 { "Yes" } else { "No" }
        let ms = MachineState::new().with_variable("n", Int64Value(5));
        let (_, result) = ms.evaluate(&If {
            condition: Box::new(GreaterThan(
                Box::new(Variable("n".into())),
                Box::new(Literal(Int64Value(25))),
            )),
            a: Box::new(Literal(StringValue("Yes".into()))),
            b: Some(Box::new(Literal(StringValue("No".into())))),
        }).unwrap();
        assert_eq!(result, StringValue("No".into()));
    }

    #[test]
    fn test_if_2() {
        // if n < 10 { "Yes" } else { "No" }
        let ms = MachineState::new().with_variable("n", Int64Value(5));
        let (_, result) = ms.evaluate(&If {
            condition: Box::new(LessThan(
                Box::new(Variable("n".into())),
                Box::new(Literal(Int64Value(10))),
            )),
            a: Box::new(Literal(StringValue("Yes".into()))),
            b: Some(Box::new(Literal(StringValue("No".into())))),
        }).unwrap();
        assert_eq!(result, StringValue("Yes".into()));
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
    fn test_sql_from_where_limit() {
        // create a table with test data
        let columns = make_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let ms = MachineState::new()
            .with_variable("stocks", MemoryTable(ModelRowCollection::from_rows(vec![
                make_quote(0, &phys_columns, "ABC", "AMEX", 12.33),
                make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
                make_quote(2, &phys_columns, "BIZ", "NYSE", 9.775),
                make_quote(3, &phys_columns, "GOTO", "OTC", 0.1442),
                make_quote(4, &phys_columns, "XYZ", "NYSE", 0.0289),
            ])));

        let (_, result) = ms.evaluate(&Limit {
            from: Box::new(Where {
                from: Box::new(From(Box::new(Variable("stocks".into())))),
                condition: Box::new(GreaterOrEqual(
                    Box::new(Variable("last_sale".into())),
                    Box::new(Literal(Float64Value(1.0))),
                )),
            }),
            limit: Box::new(Literal(Int64Value(2))),
        }).unwrap();
        assert_eq!(result, MemoryTable(ModelRowCollection::from_rows(vec![
            make_quote(0, &phys_columns, "ABC", "AMEX", 12.33),
            make_quote(2, &phys_columns, "BIZ", "NYSE", 9.775),
        ])));
    }

    #[test]
    fn test_sql_into_namespace() {
        // create a table with test data
        let columns = make_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let ns = Namespace::parse("machine.append.stocks").unwrap();
        let mut df = make_dataframe_ns(ns, columns.clone()).unwrap();
        assert_eq!(1, df.append(&make_quote(0, &phys_columns, "ABC", "AMEX", 11.77)).unwrap());
        assert_eq!(1, df.append(&make_quote(1, &phys_columns, "UNO", "OTC", 0.2456)).unwrap());
        assert_eq!(1, df.append(&make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66)).unwrap());
        assert_eq!(1, df.append(&make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428)).unwrap());

        // insert some rows
        let ms = MachineState::new();
        let (_, result) = ms.evaluate(&InsertInto {
            table: Box::new(Ns(Box::new(Literal(StringValue("machine.append.stocks".to_string()))))),
            fields: vec![
                Variable("symbol".into()),
                Variable("exchange".into()),
                Variable("last_sale".into()),
            ],
            values: vec![
                Literal(StringValue("BOOM".into())),
                Literal(StringValue("NYSE".into())),
                Literal(Float64Value(56.99)),
            ],
        }).unwrap();
        assert_eq!(result, Int64Value(1));

        // verify the remaining rows
        let rows = df.read_fully().unwrap();
        assert_eq!(rows, vec![
            make_quote(0, &phys_columns, "ABC", "AMEX", 11.77),
            make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
            make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66),
            make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428),
            make_quote(4, &phys_columns, "BOOM", "NYSE", 56.99),
        ]);
    }

    #[test]
    fn test_sql_delete_from_namespace() {
        // create a table with test data
        let columns = make_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let ns = Namespace::parse("machine.delete.stocks").unwrap();
        let mut df = make_dataframe_ns(ns, columns.clone()).unwrap();
        assert_eq!(1, df.append(&make_quote(0, &phys_columns, "ABC", "AMEX", 11.77)).unwrap());
        assert_eq!(1, df.append(&make_quote(1, &phys_columns, "UNO", "OTC", 0.2456)).unwrap());
        assert_eq!(1, df.append(&make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66)).unwrap());
        assert_eq!(1, df.append(&make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428)).unwrap());
        assert_eq!(1, df.append(&make_quote(4, &phys_columns, "BOOM", "NASDAQ", 56.87)).unwrap());

        // delete some rows
        let ms = MachineState::new();
        let (_, result) = ms.evaluate_all(&Compiler::compile(r#"
            delete from ns("machine.delete.stocks")
            where last_sale > 1.0
            "#).unwrap()).unwrap();
        assert_eq!(result, Int64Value(3));

        // verify the remaining rows
        let rows = df.read_fully().unwrap();
        assert_eq!(rows, vec![
            make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
            make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428),
        ]);
    }

    #[test]
    fn test_sql_overwrite_rows_in_namespace() {
        // create a table with test data
        let columns = make_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let ns = Namespace::parse("machine.overwrite.stocks").unwrap();
        let mut df = make_dataframe_ns(ns, columns.clone()).unwrap();
        assert_eq!(1, df.append(&make_quote(0, &phys_columns, "ABC", "AMEX", 11.77)).unwrap());
        assert_eq!(1, df.append(&make_quote(1, &phys_columns, "UNO", "OTC", 0.2456)).unwrap());
        assert_eq!(1, df.append(&make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66)).unwrap());
        assert_eq!(1, df.append(&make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428)).unwrap());
        assert_eq!(1, df.append(&make_quote(4, &phys_columns, "BOOM", "NASDAQ", 56.87)).unwrap());

        // delete some rows
        let ms = MachineState::new();
        let (_, result) = ms.evaluate(&Overwrite {
            table: Box::new(Ns(Box::new(Literal(StringValue("machine.overwrite.stocks".to_string()))))),
            fields: vec![
                Variable("symbol".into()),
                Variable("exchange".into()),
                Variable("last_sale".into()),
            ],
            values: vec![
                Literal(StringValue("BOOM".into())),
                Literal(StringValue("NYSE".into())),
                Literal(Float64Value(56.99)),
            ],
            condition: Some(Box::new(Equal(
                Box::new(Variable("symbol".into())),
                Box::new(Literal(StringValue("BOOM".into()))),
            ))),
            limit: None,
        }).unwrap();
        assert_eq!(result, Int64Value(1));

        // verify the remaining rows
        let rows = df.read_fully().unwrap();
        assert_eq!(rows, vec![
            make_quote(0,
                       &phys_columns,
                       "ABC",
                       "AMEX",
                       11.77),
            make_quote(1,
                       &phys_columns,
                       "UNO",
                       "OTC",
                       0.2456),
            make_quote(2,
                       &phys_columns,
                       "BIZ",
                       "NYSE",
                       23.66),
            make_quote(3,
                       &phys_columns,
                       "GOTO",
                       "OTC",
                       0.1428),
            make_quote(4,
                       &phys_columns,
                       "BOOM",
                       "NYSE",
                       56.99),
        ]);
    }

    #[test]
    fn test_sql_select_from_namespace() {
        // create a table with test data
        let columns = make_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let ns = Namespace::parse("machine.select.stocks").unwrap();
        let mut df = make_dataframe_ns(ns, columns.clone()).unwrap();
        assert_eq!(1, df.append(&make_quote(0, &phys_columns, "ABC", "AMEX", 11.77)).unwrap());
        assert_eq!(1, df.append(&make_quote(1, &phys_columns, "UNO", "OTC", 0.2456)).unwrap());
        assert_eq!(1, df.append(&make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66)).unwrap());
        assert_eq!(1, df.append(&make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428)).unwrap());
        assert_eq!(1, df.append(&make_quote(4, &phys_columns, "BOOM", "NASDAQ", 56.87)).unwrap());

        // compile and execute the code
        let ms = MachineState::new();
        let (_, result) = ms.evaluate_all(&Compiler::compile(r#"
            select symbol, exchange, last_sale
            from ns("machine.select.stocks")
            where last_sale > 1.0
            order by symbol
            limit 5
            "#).unwrap()).unwrap();
        assert_eq!(result, MemoryTable(ModelRowCollection::from_rows(vec![
            make_quote(0, &phys_columns, "ABC", "AMEX", 11.77),
            make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66),
            make_quote(4, &phys_columns, "BOOM", "NASDAQ", 56.87),
        ])));
    }

    #[test]
    fn test_sql_select_from_variable() {
        // build an in-memory table
        let phys_columns = TableColumn::from_columns(&make_columns()).unwrap();
        let brc = ModelRowCollection::from_rows(vec![
            make_quote(0, &phys_columns, "ABC", "AMEX", 11.77),
            make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
            make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66),
            make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428),
            make_quote(4, &phys_columns, "BOOM", "NASDAQ", 56.87)]);

        // build a new MachineState with containing the in-memory table
        let ms = MachineState::new().with_variable("stocks", MemoryTable(brc));

        // execute the code
        let (_, result) = ms.evaluate_all(&Compiler::compile(r#"
            select symbol, exchange, last_sale
            from stocks
            where last_sale < 1.0
            order by symbol
            limit 5
            "#).unwrap()).unwrap();
        assert_eq!(result, MemoryTable(ModelRowCollection::from_rows(vec![
            make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
            make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428),
        ])));
    }

    #[test]
    fn test_variables_directly_1() {
        let ms = MachineState::new()
            .set("abc", Int32Value(5))
            .set("xyz", Int32Value(58));
        assert_eq!(ms.get("abc"), Some(Int32Value(5)));
        assert_eq!(ms.get("xyz"), Some(Int32Value(58)));
    }

    #[test]
    fn test_variables_directly_2() {
        let ms = MachineState::new()
            .set("x", Int64Value(50));
        let (ms, result) = ms.evaluate(
            &Divide(Box::new(Variable("x".into())), Box::new(Literal(Int64Value(7))))
        ).unwrap();
        assert_eq!(result, Int64Value(7));
        assert_eq!(ms.get("x"), Some(Int64Value(50)));
    }

    #[ignore]
    #[test]
    fn test_precedence() {
        let ms = MachineState::new();
        let opcodes = Compiler::compile("2 + 4 * 3").unwrap();
        let (_ms, result) = ms.evaluate_all(&opcodes).unwrap();
        assert_eq!(result, Float64Value(14.))
    }
}