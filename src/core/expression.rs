////////////////////////////////////////////////////////////////////
// expression module
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

use crate::expression::Expression::*;
use crate::namespaces::Namespace;
use crate::server::ColumnJs;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{Boolean, Null, Undefined};

pub const FALSE: Expression = Literal(Boolean(false));
pub const TRUE: Expression = Literal(Boolean(true));
pub const NULL: Expression = Literal(Null);
pub const UNDEFINED: Expression = Literal(Undefined);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
    Neg(Box<Expression>),
    // direct/reference values
    Field(String),
    Literal(TypedValue),
    Ns(Box<Expression>),
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
    OpenTable(Namespace),
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
            Box::new(TRUE),
            Box::new(FALSE))).unwrap();
        assert_eq!(result, Boolean(false));
    }

    #[test]
    fn test_between() {
        let ms = MachineState::new();
        let (_, result) = ms.evaluate(&Between(
            Box::new(Literal(Int32Value(5))),
            Box::new(Literal(Int32Value(1))),
            Box::new(Literal(Int32Value(10))))).unwrap();
        assert_eq!(result, Boolean(true));
    }

    #[test]
    fn test_eq() {
        let ms = MachineState::new();
        let (_, result) = ms.evaluate(&Equal(
            Box::new(Literal(Int32Value(5))),
            Box::new(Literal(Int32Value(5))))).unwrap();
        assert_eq!(result, Boolean(true));
    }

    #[test]
    fn test_gt() {
        let ms = MachineState::new();
        let (_, result) = ms.evaluate(&GreaterThan(
            Box::new(Literal(Int32Value(5))),
            Box::new(Literal(Int32Value(1))))).unwrap();
        assert_eq!(result, Boolean(true));
    }

    #[test]
    fn test_gte() {
        let ms = MachineState::new();
        let (_, result) = ms.evaluate(&GreaterOrEqual(
            Box::new(Literal(Int32Value(5))),
            Box::new(Literal(Int32Value(1))))).unwrap();
        assert_eq!(result, Boolean(true));
    }

    #[test]
    fn test_lt() {
        let ms = MachineState::new();
        let (_, result) = ms.evaluate(&LessThan(
            Box::new(Literal(Int32Value(4))),
            Box::new(Literal(Int32Value(5))))).unwrap();
        assert_eq!(result, Boolean(true));
    }

    #[test]
    fn test_lte() {
        let ms = MachineState::new();
        let (_, result) = ms.evaluate(&LessOrEqual(
            Box::new(Literal(Int32Value(1))),
            Box::new(Literal(Int32Value(5))))).unwrap();
        assert_eq!(result, Boolean(true));
    }

    #[test]
    fn test_ne() {
        let ms = MachineState::new();
        let (_, result) = ms.evaluate(&NotEqual(
            Box::new(Literal(Int32Value(-5))),
            Box::new(Literal(Int32Value(5))))).unwrap();
        assert_eq!(result, Boolean(true));
    }

    #[test]
    fn test_or() {
        let ms = MachineState::new();
        let (_, result) = ms.evaluate(&Or(
            Box::new(TRUE),
            Box::new(FALSE))).unwrap();
        assert_eq!(result, Boolean(true));
    }

    #[test]
    fn test_is_conditional() {
        assert!(And(Box::new(TRUE), Box::new(FALSE))
            .is_conditional());
        assert!(Between(Box::new(Literal(Int32Value(5))), Box::new(Literal(Int32Value(1))), Box::new(Literal(Int32Value(10))))
            .is_conditional());
        assert!(Or(Box::new(TRUE), Box::new(FALSE))
            .is_conditional());
    }

    #[test]
    fn test_if_is_control_flow() {
        let op = If {
            condition: Box::new(LessThan(Box::new(Variable("x".into())), Box::new(Variable("y".into())))),
            a: Box::new(Literal(Int32Value(1))),
            b: Some(Box::new(Literal(Int32Value(10)))),
        };
        assert!(op.is_control_flow());
    }

    #[test]
    fn test_while_is_control_flow() {
        // CodeBlock(..) | If(..) | Return(..) | While { .. }
        let op = While {
            condition: Some(Box::new(LessThan(Box::new(Variable("x".into())), Box::new(Variable("y".into()))))),
            code: Box::new(Literal(Int32Value(1))),
        };
        assert!(op.is_control_flow());
    }

    #[test]
    fn test_is_referential() {
        assert!(Field("symbol".into()).is_referential());
        assert!(Variable("symbol".into()).is_referential());
    }
}