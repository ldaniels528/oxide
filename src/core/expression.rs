////////////////////////////////////////////////////////////////////
// expression module
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

use crate::columns::Column;
use crate::expression::Expression::*;
use crate::namespaces::Namespace;
use crate::typed_values::TypedValue;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    Not(Box<Expression>),
    NotEqual(Box<Expression>, Box<Expression>),
    Or(Box<Expression>, Box<Expression>),
    // mathematics
    Divide(Box<Expression>, Box<Expression>),
    Factorial(Box<Expression>),
    Plus(Box<Expression>, Box<Expression>),
    Pow(Box<Expression>, Box<Expression>),
    Minus(Box<Expression>, Box<Expression>),
    Modulo(Box<Expression>, Box<Expression>),
    Times(Box<Expression>, Box<Expression>),
    // direct/reference values
    Field(String),
    Literal(TypedValue),
    Range(Box<Expression>, Box<Expression>),
    Tuple(Vec<Expression>),
    Variable(String),
    // assignment
    SetVariable(String, Box<Expression>),
    // control/flow
    CodeBlock(Vec<Expression>),
    If {
        condition: Box<Expression>,
        a: Box<Expression>,
        b: Option<Box<Expression>>,
    },
    Return(Vec<Expression>),
    While {
        condition: Option<Box<Expression>>,
        code: Box<Expression>,
    },
    // SQL
    CreateIndex {
        index: Box<Expression>,
        columns: Vec<Column>,
        table: Box<Expression>,
    },
    CreateTable {
        table: Box<Expression>,
        columns: Vec<Column>,
        from: Option<Box<Expression>>,
    },
    Delete {
        table: Box<Expression>,
        condition: Option<Box<Expression>>,
        limit: Option<TypedValue>,
    },
    Drop {
        table: Box<Expression>,
    },
    Insert {
        table: Box<Expression>,
        fields: Vec<Expression>,
        values: Vec<Expression>,
    },
    Ns(Namespace),
    Overwrite {
        table: Box<Expression>,
        fields: Vec<Expression>,
        values: Vec<Expression>,
        condition: Option<Box<Expression>>,
        limit: Option<TypedValue>,
    },
    Select {
        fields: Vec<Expression>,
        from: Option<Box<Expression>>,
        condition: Option<Box<Expression>>,
        group_by: Option<Vec<Expression>>,
        having: Option<Vec<Expression>>,
        order_by: Option<Vec<(Expression, bool)>>,
        limit: Option<TypedValue>,
    },
    Truncate {
        table: Box<Expression>,
        limit: Option<TypedValue>,
    },
    Update {
        table: Box<Expression>,
        fields: Vec<Expression>,
        values: Vec<Expression>,
        condition: Option<Box<Expression>>,
        limit: Option<TypedValue>,
    },
}

impl Expression {
    pub fn is_conditional(&self) -> bool {
        matches!(self, And(..) | Between(..) | Contains(..) | Equal(..) | NotEqual(..) |
        GreaterThan(..) | GreaterOrEqual(..) | LessThan(..) | LessOrEqual(..) | Or(..))
    }

    pub fn is_control_flow(&self) -> bool {
        matches!(self, CodeBlock(..) | If { .. } | Return(..) | While { .. })
    }

    pub fn is_referential(&self) -> bool {
        matches!(self, Field(..) | Variable(..))
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::expression::Expression::*;
    use crate::machine::MachineState;
    use crate::typed_values::TypedValue::{Boolean, Int32Value};

    #[test]
    fn test_and() {
        let vm = MachineState::new();
        let (_, result) = vm.evaluate(&And(
            Box::from(Literal(Boolean(true))),
            Box::from(Literal(Boolean(false))))).unwrap();
        assert_eq!(result, Boolean(false));
    }

    #[test]
    fn test_between() {
        let vm = MachineState::new();
        let (_, result) = vm.evaluate(&Between(
            Box::from(Literal(Int32Value(5))),
            Box::from(Literal(Int32Value(1))),
            Box::from(Literal(Int32Value(10))))).unwrap();
        assert_eq!(result, Boolean(true));
    }

    #[test]
    fn test_eq() {
        let vm = MachineState::new();
        let (_, result) = vm.evaluate(&Equal(
            Box::from(Literal(Int32Value(5))),
            Box::from(Literal(Int32Value(5))))).unwrap();
        assert_eq!(result, Boolean(true));
    }

    #[test]
    fn test_gt() {
        let vm = MachineState::new();
        let (_, result) = vm.evaluate(&GreaterThan(
            Box::from(Literal(Int32Value(5))),
            Box::from(Literal(Int32Value(1))))).unwrap();
        assert_eq!(result, Boolean(true));
    }

    #[test]
    fn test_gte() {
        let vm = MachineState::new();
        let (_, result) = vm.evaluate(&GreaterOrEqual(
            Box::from(Literal(Int32Value(5))),
            Box::from(Literal(Int32Value(1))))).unwrap();
        assert_eq!(result, Boolean(true));
    }

    #[test]
    fn test_lt() {
        let vm = MachineState::new();
        let (_, result) = vm.evaluate(&LessThan(
            Box::from(Literal(Int32Value(4))),
            Box::from(Literal(Int32Value(5))))).unwrap();
        assert_eq!(result, Boolean(true));
    }

    #[test]
    fn test_lte() {
        let vm = MachineState::new();
        let (_, result) = vm.evaluate(&LessOrEqual(
            Box::from(Literal(Int32Value(1))),
            Box::from(Literal(Int32Value(5))))).unwrap();
        assert_eq!(result, Boolean(true));
    }

    #[test]
    fn test_ne() {
        let vm = MachineState::new();
        let (_, result) = vm.evaluate(&NotEqual(
            Box::from(Literal(Int32Value(-5))),
            Box::from(Literal(Int32Value(5))))).unwrap();
        assert_eq!(result, Boolean(true));
    }

    #[test]
    fn test_or() {
        let vm = MachineState::new();
        let (_, result) = vm.evaluate(&Or(
            Box::from(Literal(Boolean(true))),
            Box::from(Literal(Boolean(false))))).unwrap();
        assert_eq!(result, Boolean(true));
    }

    #[test]
    fn test_is_conditional() {
        assert!(And(Box::from(Literal(Boolean(true))), Box::from(Literal(Boolean(false))))
            .is_conditional());
        assert!(Between(Box::from(Literal(Int32Value(5))), Box::from(Literal(Int32Value(1))), Box::from(Literal(Int32Value(10))))
            .is_conditional());
        assert!(Or(Box::from(Literal(Boolean(true))), Box::from(Literal(Boolean(false))))
            .is_conditional());
    }

    #[test]
    fn test_if_is_control_flow() {
        let op = If {
            condition: Box::from(LessThan(Box::from(Variable("x".into())), Box::from(Variable("y".into())))),
            a: Box::from(Literal(Int32Value(1))),
            b: Some(Box::from(Literal(Int32Value(10)))),
        };
        println!("op: {:?}", &op);
        assert!(op.is_control_flow());
    }

    #[test]
    fn test_while_is_control_flow() {
        // CodeBlock(..) | If(..) | Return(..) | While { .. }
        let op = While {
            condition: Some(Box::from(LessThan(Box::from(Variable("x".into())), Box::from(Variable("y".into()))))),
            code: Box::from(Literal(Int32Value(1))),
        };
        println!("op: {:?}", &op);
        assert!(op.is_control_flow());
    }

    #[test]
    fn test_is_referential() {
        assert!(Field("symbol".into()).is_referential());
        assert!(Variable("symbol".into()).is_referential());
    }
}