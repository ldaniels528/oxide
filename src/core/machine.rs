////////////////////////////////////////////////////////////////////
// state machine module
////////////////////////////////////////////////////////////////////

use std::collections::HashMap;
use std::fmt::format;
use std::ops::Deref;

use actix_web::http::header::q;
use log::{error, info};
use serde::{Deserialize, Serialize};
use tokio::io;

use crate::error_mgmt::fail;
use crate::expression::Expression;
use crate::rows::Row;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::*;

/// represents the state of the machine.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MachineState {
    current_row: Option<Row>,
    stack: Vec<TypedValue>,
    variables: HashMap<String, TypedValue>,
}

impl MachineState {
    /// creates a new state machine
    pub fn new() -> Self {
        Self {
            current_row: None,
            stack: Vec::new(),
            variables: HashMap::new(),
        }
    }

    /// evaluates the specified [Expression]; returning a [TypedValue] result.
    pub fn evaluate(&self, expression: &Expression) -> io::Result<(MachineState, TypedValue)> {
        use crate::expression::Expression::*;
        match expression {
            And(a, b) =>
                self.expand2(a, b, |aa, bb| aa.and(&bb)),
            Between(a, b, c) =>
                self.expand3(a, b, c, |aa, bb, cc| aa.between(&bb, &cc)),
            CodeBlock(ops) => self.evaluate_all(ops),
            Divide(a, b) =>
                self.expand2(a, b, |aa, bb| Some(aa / bb)),
            Equal(a, b) =>
                self.expand2(a, b, |aa, bb| aa.eq(&bb)),
            Factorial(a) => self.expand1(a, |aa| aa.factorial()),
            Field(name) =>
                Ok((self.clone(), if let Some(row) = &self.current_row {
                    row.find_field_by_name(name).unwrap_or(Undefined)
                } else { Undefined })),
            GreaterThan(a, b) =>
                self.expand2(a, b, |aa, bb| aa.gt(&bb)),
            GreaterOrEqual(a, b) =>
                self.expand2(a, b, |aa, bb| aa.gte(&bb)),
            LessThan(a, b) =>
                self.expand2(a, b, |aa, bb| aa.lt(&bb)),
            LessOrEqual(a, b) =>
                self.expand2(a, b, |aa, bb| aa.lte(&bb)),
            Literal(value) => Ok((self.clone(), value.clone())),
            Minus(a, b) =>
                self.expand2(a, b, |aa, bb| Some(aa - bb)),
            Modulo(a, b) =>
                self.expand2(a, b, |aa, bb| aa.modulo(&bb)),
            NotEqual(a, b) =>
                self.expand2(a, b, |aa, bb| aa.ne(&bb)),
            Or(a, b) =>
                self.expand2(a, b, |aa, bb| aa.or(&bb)),
            Not(a) => self.expand1(a, |aa| aa.not()),
            Plus(a, b) =>
                self.expand2(a, b, |aa, bb| Some(aa + bb)),
            Pow(a, b) =>
                self.expand2(a, b, |aa, bb| Some(aa ^ bb)),
            SetVariable(name, e) => {
                let (vm, result) = self.evaluate(e)?;
                Ok((vm.with_variable(name.into(), result.clone()), result))
            }
            Times(a, b) =>
                self.expand2(a, b, |aa, bb| Some(aa * bb)),
            Tuple(values) => self.evaluate_array(values),
            Variable(name) => Ok((self.clone(), self.get(&name).unwrap_or(Undefined))),
            other => fail(format!("Unhandled expression: {:?}", other))
        }
    }

    /// evaluates the specified [Expression]; returning a [TypedValue] result.
    pub fn evaluate_all(&self, ops: &Vec<Expression>) -> io::Result<(MachineState, TypedValue)> {
        let mut result = Undefined;
        let mut vm = self.clone();
        for op in ops {
            let (vm1, result1) = vm.evaluate(op)?;
            vm = vm1;
            result = result1
        }
        Ok((vm, result))
    }

    /// evaluates the specified [Expression]; returning an array ([TypedValue]) result.
    pub fn evaluate_array(&self, ops: &Vec<Expression>) -> io::Result<(MachineState, TypedValue)> {
        let mut results = vec![];
        let mut vm = self.clone();
        for op in ops {
            let (vm1, result1) = vm.evaluate(op)?;
            vm = vm1;
            results.push(result1);
        }
        Ok((vm, Array(results)))
    }

    /// executes the specified instructions on this state machine.
    pub fn execute(&self, ops: Vec<OpCode>) -> io::Result<MachineState> {
        let mut vm = self.clone();
        for op in ops { vm = op(&vm)? }
        Ok(vm)
    }

    /// evaluates the boxed expression and applies the supplied function
    fn expand1(&self,
               a: &Box<Expression>,
               f: fn(TypedValue) -> Option<TypedValue>) -> io::Result<(MachineState, TypedValue)> {
        let (vm, aa) = self.evaluate(a.deref())?;
        Ok((vm, f(aa).unwrap_or(Undefined)))
    }

    /// evaluates the two boxed expressions and applies the supplied function
    fn expand2(&self,
               a: &Box<Expression>,
               b: &Box<Expression>,
               f: fn(TypedValue, TypedValue) -> Option<TypedValue>) -> io::Result<(MachineState, TypedValue)> {
        let (vm, aa) = self.evaluate(a.deref())?;
        let (vm, bb) = vm.evaluate(b.deref())?;
        Ok((vm, f(aa, bb).unwrap_or(Undefined)))
    }

    /// evaluates the three boxed expressions and applies the supplied function
    fn expand3(&self,
               a: &Box<Expression>,
               b: &Box<Expression>,
               c: &Box<Expression>,
               f: fn(TypedValue, TypedValue, TypedValue) -> Option<TypedValue>) -> io::Result<(MachineState, TypedValue)> {
        let (vm, aa) = self.evaluate(a.deref())?;
        let (vm, bb) = vm.evaluate(b.deref())?;
        let (vm, cc) = vm.evaluate(c.deref())?;
        Ok((vm, f(aa, bb, cc).unwrap_or(Undefined)))
    }

    /// returns a variable by name
    pub fn get(&self, name: &str) -> Option<TypedValue> {
        match self.variables.get(name) {
            Some(v) => Some(v.clone()),
            None => None
        }
    }

    /// sets the value of a variable by name
    pub fn set(&self, name: &str, value: TypedValue) -> Self {
        self.with_variable(name.to_string(), value)
    }

    /// returns the option of a value from the stack
    pub fn pop(&self) -> (Self, Option<TypedValue>) {
        let mut stack = self.stack.clone();
        let value = stack.pop();
        let machine = MachineState {
            current_row: self.current_row.clone(),
            stack,
            variables: self.variables.clone(),
        };
        (machine, value)
    }

    /// returns a value from the stack or the default value if the stack is empty.
    pub fn pop_or(&self, default_value: TypedValue) -> (Self, TypedValue) {
        let (vm, result) = self.pop();
        (vm, result.unwrap_or(default_value))
    }

    /// pushes a value unto the stack
    pub fn push(&self, value: TypedValue) -> Self {
        let mut stack = self.stack.clone();
        stack.push(value);
        MachineState {
            current_row: self.current_row.clone(),
            stack,
            variables: self.variables.clone(),
        }
    }

    /// pushes a collection of values unto the stack
    pub fn push_all(&self, values: Vec<TypedValue>) -> Self {
        values.iter().fold(self.clone(), |vm, tv| vm.push(tv.clone()))
    }

    /// evaluates the collection of [Expression]s; returning a [TypedValue] result.
    pub fn run(&self, opcodes: Vec<Expression>) -> io::Result<(MachineState, TypedValue)> {
        let mut result = Undefined;
        let mut vm = self.clone();
        for op in opcodes {
            info!("{:?}", op);
            let (vm1, result1) = vm.evaluate(&op)?;
            vm = vm1;
            result = result1;
        }
        Ok((vm, result))
    }

    pub fn stack_len(&self) -> usize {
        self.stack.len()
    }

    pub fn transform_numeric(&self, number: TypedValue,
                             fi: fn(i64) -> TypedValue,
                             ff: fn(f64) -> TypedValue) -> io::Result<Self> {
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

    pub fn with_current_row(&self, current_row: Option<Row>) -> Self {
        MachineState {
            current_row,
            stack: self.stack.clone(),
            variables: self.variables.clone(),
        }
    }

    pub fn with_variable(&self, name: String, value: TypedValue) -> Self {
        let mut variables = self.variables.clone();
        variables.insert(name, value);
        MachineState {
            current_row: self.current_row.clone(),
            stack: self.stack.clone(),
            variables,
        }
    }
}

/// represents an executable instruction (opcode)
pub type OpCode = fn(&MachineState) -> io::Result<MachineState>;

// Unit tests
#[cfg(test)]
mod tests {
    use crate::compiler::Compiler;
    use crate::expression::Expression::{Factorial, Literal};

    use super::*;

    #[test]
    fn test_push_all() {
        let vm = MachineState::new()
            .push_all(vec![
                Float32Value(2.), Float64Value(3.),
                Int16Value(4), Int32Value(5),
                Int64Value(6), StringValue("Hello World".into()),
            ]);
        assert_eq!(vm.stack, vec![
            Float32Value(2.), Float64Value(3.),
            Int16Value(4), Int32Value(5),
            Int64Value(6), StringValue("Hello World".into()),
        ])
    }

    #[test]
    fn test_variables() {
        let vm = MachineState::new()
            .set("abc", Int32Value(5))
            .set("xyz", Int32Value(58));
        assert_eq!(vm.get("abc"), Some(Int32Value(5)));
        assert_eq!(vm.get("xyz"), Some(Int32Value(58)));
    }

    #[test]
    fn test_compile_5_x_5_and_run() {
        let vm = MachineState::new();
        let opcodes = Compiler::compile("5 * 5").unwrap();
        let (_, result) = vm.run(opcodes).unwrap();
        assert_eq!(result, Int64Value(25))
    }

    #[test]
    fn test_compile_7_gt_5_and_run() {
        let vm = MachineState::new();
        let opcodes = Compiler::compile("7 > 5").unwrap();
        let (_, result) = vm.run(opcodes).unwrap();
        assert_eq!(result, Boolean(true))
    }

    #[test]
    fn test_model_factorial_and_run() {
        let vm = MachineState::new();
        let opcodes = vec![
            Factorial(Box::new(Literal(Float64Value(6.))))
        ];
        let (_, result) = vm.run(opcodes).unwrap();
        assert_eq!(result, Float64Value(720.))
    }

    #[ignore]
    #[test]
    fn test_precedence() {
        let vm = MachineState::new();
        let opcodes = Compiler::compile("2 + 4 * 3").unwrap();
        let (_, result) = vm.run(opcodes).unwrap();
        assert_eq!(result, Float64Value(14.))
    }
}