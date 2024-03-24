////////////////////////////////////////////////////////////////////
// state machine module
////////////////////////////////////////////////////////////////////

use std::collections::HashMap;
use std::ops::{Add, BitXor, Div};

use log::info;
use serde::{Deserialize, Serialize};
use tokio::io;

use crate::error_mgmt::fail;
use crate::expression::Expression;
use crate::expression::Expression::Xor;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::*;

/// represents the state of the machine.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MachineState {
    stack: Vec<TypedValue>,
    variables: HashMap<String, TypedValue>,
}

impl MachineState {
    /// creates a new state machine
    pub fn new() -> Self {
        Self {
            stack: Vec::new(),
            variables: HashMap::new(),
        }
    }

    pub fn add_variables(&mut self, variables: HashMap<String, TypedValue>) {
        self.variables.extend(variables)
    }

    pub fn get_variables(&self) -> &HashMap<String, TypedValue> {
        &self.variables
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
                self.expand2(a, b, |aa, bb| Some(aa % bb)),
            Multiply(a, b) =>
                self.expand2(a, b, |aa, bb| Some(aa * bb)),
            NotEqual(a, b) =>
                self.expand2(a, b, |aa, bb| aa.ne(&bb)),
            Or(a, b) =>
                self.expand2(a, b, |aa, bb| aa.or(&bb)),
            Not(a) => self.expand1(a, |aa| Some(!aa)),
            Plus(a, b) =>
                self.expand2(a, b, |aa, bb| Some(aa + bb)),
            Pow(a, b) =>
                self.expand2(a, b, |aa, bb| aa.pow(&bb)),
            Range(a, b) =>
                self.expand2(a, b, |aa, bb| aa.range(&bb)),
            ShiftLeft(a, b) =>
                self.expand2(a, b, |aa, bb| Some(aa << bb)),
            ShiftRight(a, b) =>
                self.expand2(a, b, |aa, bb| Some(aa >> bb)),
            SetVariable(name, e) => {
                let (vm, result) = self.evaluate(e)?;
                Ok((vm.set(name, result), Undefined))
            }
            Tuple(values) => self.evaluate_array(values),
            Variable(name) => Ok((self.clone(), self.get(&name).unwrap_or(Undefined))),
            Xor(a, b) =>
                self.expand2(a, b, |aa, bb| Some(aa ^ bb)),
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
    pub fn execute(&self, ops: &Vec<OpCode>) -> io::Result<(MachineState, TypedValue)> {
        let mut vm = self.clone();
        for op in ops { vm = op(&vm)? }
        let (vm, result) = vm.pop_or(Undefined);
        Ok((vm, result))
    }

    /// evaluates the boxed expression and applies the supplied function
    fn expand1(&self,
               a: &Box<Expression>,
               f: fn(TypedValue) -> Option<TypedValue>) -> io::Result<(MachineState, TypedValue)> {
        let (vm, aa) = self.evaluate(a)?;
        Ok((vm, f(aa).unwrap_or(Undefined)))
    }

    /// evaluates the two boxed expressions and applies the supplied function
    fn expand2(&self,
               a: &Box<Expression>,
               b: &Box<Expression>,
               f: fn(TypedValue, TypedValue) -> Option<TypedValue>) -> io::Result<(MachineState, TypedValue)> {
        let (vm, aa) = self.evaluate(a)?;
        let (vm, bb) = vm.evaluate(b)?;
        Ok((vm, f(aa, bb).unwrap_or(Undefined)))
    }

    /// evaluates the three boxed expressions and applies the supplied function
    fn expand3(&self,
               a: &Box<Expression>,
               b: &Box<Expression>,
               c: &Box<Expression>,
               f: fn(TypedValue, TypedValue, TypedValue) -> Option<TypedValue>) -> io::Result<(MachineState, TypedValue)> {
        let (vm, aa) = self.evaluate(a)?;
        let (vm, bb) = vm.evaluate(b)?;
        let (vm, cc) = vm.evaluate(c)?;
        Ok((vm, f(aa, bb, cc).unwrap_or(Undefined)))
    }

    /// returns a variable by name
    pub fn get(&self, name: &str) -> Option<TypedValue> {
        self.variables.get(name).map(|x| x.clone())
    }

    /// returns the option of a value from the stack
    pub fn pop(&self) -> (Self, Option<TypedValue>) {
        let mut stack = self.stack.clone();
        let value = stack.pop();
        let machine = MachineState {
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

    pub fn set(&self, name: &str, value: TypedValue) -> Self {
        let mut variables = self.variables.clone();
        variables.insert(name.to_string(), value);
        MachineState {
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
    fn test_compile_n_x_5_and_run() {
        let vm = MachineState::new()
            .set("n", Int64Value(5));
        let opcodes = Compiler::compile("n ** 2").unwrap();
        let (_, result) = vm.run(opcodes).unwrap();
        assert_eq!(result, Int64Value(25))
    }

    #[test]
    fn test_compile_n_gt_5_and_run() {
        let vm = MachineState::new()
            .set("n", Int64Value(7));
        let opcodes = Compiler::compile("n > 5").unwrap();
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