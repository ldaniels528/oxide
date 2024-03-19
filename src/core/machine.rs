////////////////////////////////////////////////////////////////////
// state machine module
////////////////////////////////////////////////////////////////////

use std::collections::HashMap;
use std::ops::Deref;

use log::info;
use serde::{Deserialize, Serialize};

use crate::expression::Expression;
use crate::machine::ErrorCode::*;
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
    pub fn evaluate(&self, expression: &Expression) -> TypedValue {
        use crate::expression::Expression::*;
        match expression {
            And(a, b) =>
                self.expand2(a, b, |aa, bb| aa.and(&bb)),
            Between(a, b, c) =>
                self.expand3(a, b, c, |aa, bb, cc| aa.between(&bb, &cc)),
            CodeBlock(ops) =>
                ops.iter().fold(Null, |_, op| self.evaluate(op)),
            Divide(a, b) =>
                self.expand2(a, b, |aa, bb| Some(aa / bb)),
            Equal(a, b) =>
                self.expand2(a, b, |aa, bb| aa.eq(&bb)),
            Factorial(a) => self.expand1(a, |aa| aa.factorial()),
            Field(name) =>
                if let Some(row) = &self.current_row {
                    row.find_field_by_name(&name).unwrap_or(Undefined)
                } else { Undefined }
            GreaterThan(a, b) =>
                self.expand2(a, b, |aa, bb| aa.gt(&bb)),
            GreaterOrEqual(a, b) =>
                self.expand2(a, b, |aa, bb| aa.gte(&bb)),
            LessThan(a, b) =>
                self.expand2(a, b, |aa, bb| aa.lt(&bb)),
            LessOrEqual(a, b) =>
                self.expand2(a, b, |aa, bb| aa.lte(&bb)),
            Literal(value) => value.clone(),
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
            Times(a, b) =>
                self.expand2(a, b, |aa, bb| Some(aa * bb)),
            Tuple(values) =>
                Array(values.iter().map(|op| self.evaluate(op)).collect()),
            Variable(name) => self.get(&name).unwrap_or(Undefined),
            other => {
                eprintln!("Unhandled expression: {:?}", other);
                Undefined
            }
        }
    }

    /// executes the specified instructions on this state machine.
    pub fn execute(&mut self, ops: Vec<OpCode>) -> Option<ErrorCode> {
        ops.iter().fold(None, |err, op| {
            if err.is_none() { op(self) } else { err }
        })
    }

    fn expand1(&self,
               a: &Box<Expression>,
               f: fn(TypedValue) -> Option<TypedValue>) -> TypedValue {
        let aa = self.evaluate(a.deref());
        f(aa).unwrap_or(Undefined)
    }

    fn expand2(&self,
               a: &Box<Expression>,
               b: &Box<Expression>,
               f: fn(TypedValue, TypedValue) -> Option<TypedValue>) -> TypedValue {
        let (aa, bb) = (self.evaluate(a.deref()), self.evaluate(b.deref()));
        f(aa, bb).unwrap_or(Undefined)
    }

    fn expand3(&self,
               a: &Box<Expression>,
               b: &Box<Expression>,
               c: &Box<Expression>,
               f: fn(TypedValue, TypedValue, TypedValue) -> Option<TypedValue>) -> TypedValue {
        let (aa, bb, cc) = (self.evaluate(a.deref()), self.evaluate(c.deref()), self.evaluate(b.deref()));
        f(aa, bb, cc).unwrap_or(Undefined)
    }

    /// returns a variable by name
    pub fn get(&self, name: &str) -> Option<TypedValue> {
        match self.variables.get(name) {
            Some(v) => Some(v.clone()),
            None => None
        }
    }

    /// sets the value of a variable by name
    pub fn set(&mut self, name: impl Into<String>, value: TypedValue) {
        self.variables.insert(name.into(), value);
    }

    /// returns the option of a value from the stack
    pub fn pop(&mut self) -> Option<TypedValue> {
        self.stack.pop()
    }

    /// pushes a value unto the stack
    pub fn push(&mut self, value: TypedValue) {
        self.stack.push(value)
    }

    /// pushes a collection of values unto the stack
    pub fn push_all(&mut self, values: Vec<TypedValue>) {
        for value in values { self.stack.push(value); }
    }

    /// evaluates the collection of [Expression]s; returning a [TypedValue] result.
    pub fn run(&self, opcodes: Vec<Expression>) -> TypedValue {
        opcodes.iter().fold(Undefined, |_, op| {
            info!("{:?}", op);
            self.evaluate(op)
        })
    }

    pub fn stack_len(&self) -> usize {
        self.stack.len()
    }

    pub fn transform_numeric(&mut self, number: TypedValue,
                             fi: fn(i64) -> TypedValue,
                             ff: fn(f64) -> TypedValue) -> Option<ErrorCode> {
        match number {
            Float32Value(n) => self.push(ff(n as f64)),
            Float64Value(n) => self.push(ff(n)),
            Int8Value(n) => self.push(fi(n as i64)),
            Int16Value(n) => self.push(fi(n as i64)),
            Int32Value(n) => self.push(fi(n as i64)),
            Int64Value(n) => self.push(fi(n)),
            unknown => return Some(UnsupportedValue(unknown))
        }
        None
    }
}

/// represents the class for all errors.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ErrorCode {
    UnsupportedValue(TypedValue)
}

/// represents an executable instruction (opcode)
pub type OpCode = fn(&mut MachineState) -> Option<ErrorCode>;

/// represents an executable instruction (opcode)
//pub type OpCodeDyn = dyn FnMut(&mut MachineState) -> Option<ErrorCode>;

// Unit tests
#[cfg(test)]
mod tests {
    use crate::compiler::Compiler;
    use crate::expression::Expression::{Factorial, Literal};

    use super::*;

    #[test]
    fn test_get_set_variables() {
        let mut vm = MachineState::new();
        vm.set("abc", Int32Value(5));
        vm.set("xyz", Int32Value(58));
        assert_eq!(vm.get("abc"), Some(Int32Value(5)));
        assert_eq!(vm.get("xyz"), Some(Int32Value(58)));
    }

    #[test]
    fn test_compile_5_x_5_and_run() {
        let vm = MachineState::new();
        let opcodes = Compiler::compile("5 * 5").unwrap();
        let result = vm.run(opcodes);
        assert_eq!(result, Int64Value(25))
    }

    #[test]
    fn test_compile_7_gt_5_and_run() {
        let vm = MachineState::new();
        let opcodes = Compiler::compile("7 > 5").unwrap();
        let result = vm.run(opcodes);
        assert_eq!(result, Boolean(true))
    }

    #[test]
    fn test_model_factorial_and_run() {
        let vm = MachineState::new();
        let opcodes = vec![
            Factorial(Box::new(Literal(Float64Value(6.))))
        ];
        let result = vm.run(opcodes);
        assert_eq!(result, Float64Value(720.))
    }

    #[ignore]
    #[test]
    fn test_precedence() {
        let vm = MachineState::new();
        let opcodes = Compiler::compile("2 + 4 * 3").unwrap();
        let result = vm.run(opcodes);
        assert_eq!(result, Float64Value(14.))
    }
}