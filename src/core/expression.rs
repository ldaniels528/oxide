////////////////////////////////////////////////////////////////////
// expression module
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

use crate::expression::Expression::*;
use crate::namespaces::Namespace;
use crate::server::ColumnJs;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{Boolean, Null};

pub const FALSE: Expression = Literal(Boolean(false));
pub const TRUE: Expression = Literal(Boolean(true));
pub const NULL: Expression = Literal(Null);

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
    // bitwise operations
    ShiftLeft(Box<Expression>, Box<Expression>),
    ShiftRight(Box<Expression>, Box<Expression>),
    Xor(Box<Expression>, Box<Expression>),
    // mathematics
    Divide(Box<Expression>, Box<Expression>),
    Factorial(Box<Expression>),
    Plus(Box<Expression>, Box<Expression>),
    Pow(Box<Expression>, Box<Expression>),
    Minus(Box<Expression>, Box<Expression>),
    Modulo(Box<Expression>, Box<Expression>),
    Multiply(Box<Expression>, Box<Expression>),
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
        columns: Vec<ColumnJs>,
        table: Box<Expression>,
    },
    CreateTable {
        table: Box<Expression>,
        columns: Vec<ColumnJs>,
        from: Option<Box<Expression>>,
    },
    Delete {
        table: Box<Expression>,
        condition: Option<Box<Expression>>,
        limit: Option<Box<Expression>>,
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
        limit: Option<Box<Expression>>,
    },
    Select {
        fields: Vec<Expression>,
        from: Option<Box<Expression>>,
        condition: Option<Box<Expression>>,
        group_by: Option<Vec<Expression>>,
        having: Option<Box<Expression>>,
        order_by: Option<Vec<Expression>>,
        limit: Option<Box<Expression>>,
    },
    Truncate {
        table: Box<Expression>,
        limit: Option<Box<Expression>>,
    },
    Update {
        table: Box<Expression>,
        fields: Vec<Expression>,
        values: Vec<Expression>,
        condition: Option<Box<Expression>>,
        limit: Option<Box<Expression>>,
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
    use crate::expression::{FALSE, TRUE};
    use crate::expression::Expression::*;
    use crate::machine::MachineState;
    use crate::typed_values::TypedValue::{Boolean, Int32Value};

    #[test]
    fn test_and() {
        let ms = MachineState::new();
        let (_, result) = ms.evaluate(&And(
            Box::from(TRUE),
            Box::from(FALSE))).unwrap();
        assert_eq!(result, Boolean(false));
    }

    #[test]
    fn test_between() {
        let ms = MachineState::new();
        let (_, result) = ms.evaluate(&Between(
            Box::from(Literal(Int32Value(5))),
            Box::from(Literal(Int32Value(1))),
            Box::from(Literal(Int32Value(10))))).unwrap();
        assert_eq!(result, Boolean(true));
    }

    #[test]
    fn test_eq() {
        let ms = MachineState::new();
        let (_, result) = ms.evaluate(&Equal(
            Box::from(Literal(Int32Value(5))),
            Box::from(Literal(Int32Value(5))))).unwrap();
        assert_eq!(result, Boolean(true));
    }

    #[test]
    fn test_gt() {
        let ms = MachineState::new();
        let (_, result) = ms.evaluate(&GreaterThan(
            Box::from(Literal(Int32Value(5))),
            Box::from(Literal(Int32Value(1))))).unwrap();
        assert_eq!(result, Boolean(true));
    }

    #[test]
    fn test_gte() {
        let ms = MachineState::new();
        let (_, result) = ms.evaluate(&GreaterOrEqual(
            Box::from(Literal(Int32Value(5))),
            Box::from(Literal(Int32Value(1))))).unwrap();
        assert_eq!(result, Boolean(true));
    }

    #[test]
    fn test_lt() {
        let ms = MachineState::new();
        let (_, result) = ms.evaluate(&LessThan(
            Box::from(Literal(Int32Value(4))),
            Box::from(Literal(Int32Value(5))))).unwrap();
        assert_eq!(result, Boolean(true));
    }

    #[test]
    fn test_lte() {
        let ms = MachineState::new();
        let (_, result) = ms.evaluate(&LessOrEqual(
            Box::from(Literal(Int32Value(1))),
            Box::from(Literal(Int32Value(5))))).unwrap();
        assert_eq!(result, Boolean(true));
    }

    #[test]
    fn test_ne() {
        let ms = MachineState::new();
        let (_, result) = ms.evaluate(&NotEqual(
            Box::from(Literal(Int32Value(-5))),
            Box::from(Literal(Int32Value(5))))).unwrap();
        assert_eq!(result, Boolean(true));
    }

    #[test]
    fn test_or() {
        let ms = MachineState::new();
        let (_, result) = ms.evaluate(&Or(
            Box::from(TRUE),
            Box::from(FALSE))).unwrap();
        assert_eq!(result, Boolean(true));
    }

    #[test]
    fn test_is_conditional() {
        assert!(And(Box::from(TRUE), Box::from(FALSE))
            .is_conditional());
        assert!(Between(Box::from(Literal(Int32Value(5))), Box::from(Literal(Int32Value(1))), Box::from(Literal(Int32Value(10))))
            .is_conditional());
        assert!(Or(Box::from(TRUE), Box::from(FALSE))
            .is_conditional());
    }

    #[test]
    fn test_if_is_control_flow() {
        let op = If {
            condition: Box::from(LessThan(Box::from(Variable("x".into())), Box::from(Variable("y".into())))),
            a: Box::from(Literal(Int32Value(1))),
            b: Some(Box::from(Literal(Int32Value(10)))),
        };
        assert!(op.is_control_flow());
    }

    #[test]
    fn test_while_is_control_flow() {
        // CodeBlock(..) | If(..) | Return(..) | While { .. }
        let op = While {
            condition: Some(Box::from(LessThan(Box::from(Variable("x".into())), Box::from(Variable("y".into()))))),
            code: Box::from(Literal(Int32Value(1))),
        };
        assert!(op.is_control_flow());
    }

    #[test]
    fn test_is_referential() {
        assert!(Field("symbol".into()).is_referential());
        assert!(Variable("symbol".into()).is_referential());
    }
}