////////////////////////////////////////////////////////////////////
// expression module
////////////////////////////////////////////////////////////////////

use std::ops::Add;

use serde::{Deserialize, Serialize};

use crate::expression::Expression::*;
use crate::typed_values::TypedValue;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Expression {
    // conditional
    And(Box<Expression>, Box<Expression>),
    Between(Box<Expression>, Box<Expression>, Box<Expression>),
    Contains(Vec<Expression>, Box<Expression>),
    Equal(Box<Expression>, Box<Expression>),
    GreaterThan(Box<Expression>, Box<Expression>),
    GreaterOrEqual(Box<Expression>, Box<Expression>),
    LessThan(Box<Expression>, Box<Expression>),
    LessOrEqual(Box<Expression>, Box<Expression>),
    NotEqual(Box<Expression>, Box<Expression>),
    Or(Box<Expression>, Box<Expression>),
    // direct values
    Literal(TypedValue),
    // referential
    Field(String),
    Variable(String),
}

impl Expression {
    pub fn is_conditional(&self) -> bool {
        match &self {
            And(..) => true,
            Between(..) => true,
            Contains(..) => true,
            Equal(..) => true,
            NotEqual(..) => true,
            GreaterThan(..) => true,
            GreaterOrEqual(..) => true,
            LessThan(..) => true,
            LessOrEqual(..) => true,
            Or(..) => true,
            _ => false
        }
    }

    pub fn is_referential(&self) -> bool {
        match &self {
            Field(..) => true,
            Variable(..) => true,
            _ => false
        }
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::expression::Expression::{And, Between, Equal, Field, GreaterOrEqual, GreaterThan, LessOrEqual, LessThan, Literal, NotEqual, Or, Variable};
    use crate::machine::MachineState;
    use crate::typed_values::TypedValue::{BooleanValue, Int32Value};

    #[test]
    fn test_and() {
        let mut vm = MachineState::new();
        let result = vm.evaluate(&And(
            Box::from(Literal(BooleanValue(true))),
            Box::from(Literal(BooleanValue(false)))));
        assert_eq!(result, BooleanValue(false));
    }

    #[test]
    fn test_between() {
        let mut vm = MachineState::new();
        let result = vm.evaluate(&Between(
            Box::from(Literal(Int32Value(5))),
            Box::from(Literal(Int32Value(1))),
            Box::from(Literal(Int32Value(10)))));
        assert_eq!(result, BooleanValue(true));
    }

    #[test]
    fn test_eq() {
        let mut vm = MachineState::new();
        let result = vm.evaluate(&Equal(
            Box::from(Literal(Int32Value(5))),
            Box::from(Literal(Int32Value(5)))));
        assert_eq!(result, BooleanValue(true));
    }

    #[test]
    fn test_gt() {
        let mut vm = MachineState::new();
        let result = vm.evaluate(&GreaterThan(
            Box::from(Literal(Int32Value(5))),
            Box::from(Literal(Int32Value(1)))));
        assert_eq!(result, BooleanValue(true));
    }

    #[test]
    fn test_gte() {
        let mut vm = MachineState::new();
        let result = vm.evaluate(&GreaterOrEqual(
            Box::from(Literal(Int32Value(5))),
            Box::from(Literal(Int32Value(1)))));
        assert_eq!(result, BooleanValue(true));
    }

    #[test]
    fn test_lt() {
        let mut vm = MachineState::new();
        let result = vm.evaluate(&LessThan(
            Box::from(Literal(Int32Value(4))),
            Box::from(Literal(Int32Value(5)))));
        assert_eq!(result, BooleanValue(true));
    }

    #[test]
    fn test_lte() {
        let mut vm = MachineState::new();
        let result = vm.evaluate(&LessOrEqual(
            Box::from(Literal(Int32Value(1))),
            Box::from(Literal(Int32Value(5)))));
        assert_eq!(result, BooleanValue(true));
    }

    #[test]
    fn test_ne() {
        let mut vm = MachineState::new();
        let result = vm.evaluate(&NotEqual(
            Box::from(Literal(Int32Value(-5))),
            Box::from(Literal(Int32Value(5)))));
        assert_eq!(result, BooleanValue(true));
    }

    #[test]
    fn test_or() {
        let mut vm = MachineState::new();
        let result = vm.evaluate(&Or(
            Box::from(Literal(BooleanValue(true))),
            Box::from(Literal(BooleanValue(false)))));
        assert_eq!(result, BooleanValue(true));
    }

    #[test]
    fn test_is_conditional() {
        assert!(And(Box::from(Literal(BooleanValue(true))), Box::from(Literal(BooleanValue(false))))
            .is_conditional());
        assert!(Between(Box::from(Literal(Int32Value(5))), Box::from(Literal(Int32Value(1))), Box::from(Literal(Int32Value(10))))
            .is_conditional());
        assert!(Or(Box::from(Literal(BooleanValue(true))), Box::from(Literal(BooleanValue(false))))
            .is_conditional());
    }

    #[test]
    fn test_is_referential() {
        assert!(Field("symbol".into()).is_referential());
        assert!(Variable("symbol".into()).is_referential());
    }
}