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
    Plus(Box<Expression>, Box<Expression>),
    Minus(Box<Expression>, Box<Expression>),
    Times(Box<Expression>, Box<Expression>),
    // direct/reference values
    Field(String),
    Literal(TypedValue),
    Tuple(Vec<Expression>),
    Variable(String),
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

    pub fn is_control_flow(&self) -> bool {
        match &self {
            CodeBlock(..) | If { .. } | Return(..) | While { .. } => true,
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
    use crate::expression::Expression::*;
    use crate::machine::MachineState;
    use crate::typed_values::TypedValue::{BooleanValue, Int32Value};

    #[test]
    fn test_and() {
        let vm = MachineState::new();
        let result = vm.evaluate(&And(
            Box::from(Literal(BooleanValue(true))),
            Box::from(Literal(BooleanValue(false)))));
        assert_eq!(result, BooleanValue(false));
    }

    #[test]
    fn test_between() {
        let vm = MachineState::new();
        let result = vm.evaluate(&Between(
            Box::from(Literal(Int32Value(5))),
            Box::from(Literal(Int32Value(1))),
            Box::from(Literal(Int32Value(10)))));
        assert_eq!(result, BooleanValue(true));
    }

    #[test]
    fn test_eq() {
        let vm = MachineState::new();
        let result = vm.evaluate(&Equal(
            Box::from(Literal(Int32Value(5))),
            Box::from(Literal(Int32Value(5)))));
        assert_eq!(result, BooleanValue(true));
    }

    #[test]
    fn test_gt() {
        let vm = MachineState::new();
        let result = vm.evaluate(&GreaterThan(
            Box::from(Literal(Int32Value(5))),
            Box::from(Literal(Int32Value(1)))));
        assert_eq!(result, BooleanValue(true));
    }

    #[test]
    fn test_gte() {
        let vm = MachineState::new();
        let result = vm.evaluate(&GreaterOrEqual(
            Box::from(Literal(Int32Value(5))),
            Box::from(Literal(Int32Value(1)))));
        assert_eq!(result, BooleanValue(true));
    }

    #[test]
    fn test_lt() {
        let vm = MachineState::new();
        let result = vm.evaluate(&LessThan(
            Box::from(Literal(Int32Value(4))),
            Box::from(Literal(Int32Value(5)))));
        assert_eq!(result, BooleanValue(true));
    }

    #[test]
    fn test_lte() {
        let vm = MachineState::new();
        let result = vm.evaluate(&LessOrEqual(
            Box::from(Literal(Int32Value(1))),
            Box::from(Literal(Int32Value(5)))));
        assert_eq!(result, BooleanValue(true));
    }

    #[test]
    fn test_ne() {
        let vm = MachineState::new();
        let result = vm.evaluate(&NotEqual(
            Box::from(Literal(Int32Value(-5))),
            Box::from(Literal(Int32Value(5)))));
        assert_eq!(result, BooleanValue(true));
    }

    #[test]
    fn test_or() {
        let vm = MachineState::new();
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
    fn test_if_is_control_flow() {
        let op = If {
            condition: Box::from(LessThan(Box::from(Variable("x".into())), Box::from(Variable("y".into())))),
            a: Box::from(Literal(Int32Value(1))),
            b: Some(Box::from(Literal(Int32Value(10))))
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