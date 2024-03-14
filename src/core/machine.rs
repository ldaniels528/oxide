////////////////////////////////////////////////////////////////////
// state machine module
////////////////////////////////////////////////////////////////////

use std::collections::HashMap;
use std::ops::Deref;

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
            Contains(_a, _b) => todo!(),
            Equal(a, b) =>
                self.expand2(a, b, |aa, bb| aa.eq(&bb)),
            NotEqual(a, b) =>
                self.expand2(a, b, |aa, bb| aa.ne(&bb)),
            GreaterThan(a, b) =>
                self.expand2(a, b, |aa, bb| aa.gt(&bb)),
            GreaterOrEqual(a, b) =>
                self.expand2(a, b, |aa, bb| aa.gte(&bb)),
            LessThan(a, b) =>
                self.expand2(a, b, |aa, bb| aa.lt(&bb)),
            LessOrEqual(a, b) =>
                self.expand2(a, b, |aa, bb| aa.lte(&bb)),
            Or(a, b) =>
                self.expand2(a, b, |aa, bb| aa.or(&bb)),
            Field(name) =>
                if let Some(row) = &self.current_row {
                    row.find_field_by_name(&name).unwrap_or(Undefined)
                } else { Undefined }
            Literal(value) => value.clone(),
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
            v => return Some(UnsupportedValue(v))
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

// Unit tests
#[cfg(test)]
mod tests {
    use crate::expression::Expression::*;
    use crate::opcode::*;
    use crate::typed_values::TypedValue::*;

    use super::*;

    #[test]
    fn test_math_opcodes() {
        // execute the program
        let mut vm = MachineState::new();
        vm.push_all(vec![
            2.0, 3.0, // add
            4.0, // mul
            5.0, // sub
            2.0, // div
        ].iter().map(|n| Float64Value(*n)).collect());
        let err = vm.execute(vec![add, mul, sub, div]);
        assert_eq!(err, None);

        // get the return value
        let ret_val = vm.pop();
        println!("ret_val {:?}", ret_val);
        assert_eq!(ret_val, Some(Float64Value(-0.08)));
        assert_eq!(vm.stack_len(), 0)
    }

    #[test]
    fn test_get_set_variables() {
        let mut vm = MachineState::new();
        vm.set("abc", Int32Value(5));
        vm.set("xyz", Int32Value(58));
        assert_eq!(vm.get("abc"), Some(Int32Value(5)));
        assert_eq!(vm.get("xyz"), Some(Int32Value(58)));
    }
}