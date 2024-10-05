////////////////////////////////////////////////////////////////////
// expression module
////////////////////////////////////////////////////////////////////

use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::byte_code_compiler::ByteCodeCompiler;
use crate::decompiler::Decompiler;
use crate::expression::Expression::*;
use crate::expression_support::{Directives, Excavation, Mutation, Queryable};
use crate::machine::Machine;
use crate::numbers::NumberValue;
use crate::server::ColumnJs;
use crate::tokens::Token;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{ErrorValue, Number, StringValue};

// constants
pub const ACK: Expression = Literal(TypedValue::Ack);
pub const FALSE: Expression = Conditional(Condition::False);
pub const TRUE: Expression = Conditional(Condition::True);
pub const NULL: Expression = Literal(TypedValue::Null);
pub const UNDEFINED: Expression = Literal(TypedValue::Undefined);

/// Represents a Logical Condition
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Condition {
    And(Box<Expression>, Box<Expression>),
    Between(Box<Expression>, Box<Expression>, Box<Expression>),
    Betwixt(Box<Expression>, Box<Expression>, Box<Expression>),
    Contains(Box<Expression>, Box<Expression>),
    Equal(Box<Expression>, Box<Expression>),
    False,
    GreaterOrEqual(Box<Expression>, Box<Expression>),
    GreaterThan(Box<Expression>, Box<Expression>),
    LessOrEqual(Box<Expression>, Box<Expression>),
    LessThan(Box<Expression>, Box<Expression>),
    Not(Box<Expression>),
    NotEqual(Box<Expression>, Box<Expression>),
    Or(Box<Expression>, Box<Expression>),
    True,
}

impl Condition {
    /// Returns a string representation of this object
    pub fn to_code(&self) -> String {
        Decompiler::new().decompile_cond(self)
    }
}

/// Represents an Expression
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Expression {
    ArrayLiteral(Vec<Expression>),
    AsValue(String, Box<Expression>),
    BitwiseAnd(Box<Expression>, Box<Expression>),
    BitwiseOr(Box<Expression>, Box<Expression>),
    BitwiseShiftLeft(Box<Expression>, Box<Expression>),
    BitwiseShiftRight(Box<Expression>, Box<Expression>),
    BitwiseXor(Box<Expression>, Box<Expression>),
    CodeBlock(Vec<Expression>),
    ColumnSet(Vec<ColumnJs>),
    Conditional(Condition),
    Directive(Directives),
    Divide(Box<Expression>, Box<Expression>),
    ElementAt(Box<Expression>, Box<Expression>),
    Extract(Box<Expression>, Box<Expression>),
    Factorial(Box<Expression>),
    Feature { title: Box<Expression>, scenarios: Vec<Expression> },
    From(Box<Expression>),
    FunctionCall { fx: Box<Expression>, args: Vec<Expression> },
    HTTP {
        method: Box<Expression>,
        url: Box<Expression>,
        body: Option<Box<Expression>>,
        headers: Option<Box<Expression>>,
        multipart: Option<Box<Expression>>,
    },
    If {
        condition: Box<Expression>,
        a: Box<Expression>,
        b: Option<Box<Expression>>,
    },
    Include(Box<Expression>),
    JSONLiteral(Vec<(String, Expression)>),
    Literal(TypedValue),
    Minus(Box<Expression>, Box<Expression>),
    Modulo(Box<Expression>, Box<Expression>),
    Multiply(Box<Expression>, Box<Expression>),
    Neg(Box<Expression>),
    Ns(Box<Expression>),
    Plus(Box<Expression>, Box<Expression>),
    Pow(Box<Expression>, Box<Expression>),
    Quarry(Excavation),
    Range(Box<Expression>, Box<Expression>),
    Return(Vec<Expression>),
    Scenario {
        title: Box<Expression>,
        verifications: Vec<Expression>,
        inherits: Option<Box<Expression>>,
    },
    SetVariable(String, Box<Expression>),
    StructureImpl(String, Vec<Expression>),
    TupleLiteral(Vec<Expression>),
    Variable(String),
    Via(Box<Expression>),
    While {
        condition: Box<Expression>,
        code: Box<Expression>,
    },
}

impl Expression {

    ////////////////////////////////////////////////////////////////
    // instance methods
    ////////////////////////////////////////////////////////////////

    pub fn encode(&self) -> Vec<u8> {
        ByteCodeCompiler::assemble(&self)
    }

    pub fn from_token(token: Token) -> Self {
        match token {
            Token::Atom { text, .. } => Variable(text),
            Token::Backticks { text, .. } => Variable(text),
            Token::DoubleQuoted { text, .. } => Literal(StringValue(text)),
            Token::Numeric { text, .. } => Literal(Number(NumberValue::from_string(text))),
            Token::Operator { text, .. } => Literal(ErrorValue(format!("Illegal use of operator '{}'", text))),
            Token::SingleQuoted { text, .. } => Literal(StringValue(text)),
        }
    }

    /// Indicates whether the expression is a conditional expression
    pub fn is_conditional(&self) -> bool {
        matches!(self, Conditional(..))
    }

    /// Indicates whether the expression is a control flow expression
    pub fn is_control_flow(&self) -> bool {
        matches!(self, CodeBlock(..) | If { .. } | Return(..) | While { .. })
    }

    /// Indicates whether the expression is a referential expression
    pub fn is_referential(&self) -> bool {
        matches!(self, Variable(..))
    }

    /// Returns a string representation of this object
    pub fn to_code(&self) -> String {
        Decompiler::new().decompile(self)
    }
}

impl Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_code())
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::expression::*;
    use crate::expression::Condition::*;
    use crate::expression::Excavation::{Mutate, Query};
    use crate::expression_support::{Mutation, Queryable};
    use crate::machine::Machine;
    use crate::numbers::NumberValue::*;
    use crate::tokenizer;
    use crate::typed_values::TypedValue::*;

    #[test]
    fn test_from_token_to_i64() {
        let model = match tokenizer::parse_fully("12345").as_slice() {
            [tok] => Expression::from_token(tok.to_owned()),
            _ => UNDEFINED
        };
        assert_eq!(model, Literal(Number(I64Value(12345))))
    }

    #[test]
    fn test_from_token_to_f64() {
        let model = match tokenizer::parse_fully("123.45").as_slice() {
            [tok] => Expression::from_token(tok.to_owned()),
            _ => UNDEFINED
        };
        assert_eq!(model, Literal(Number(F64Value(123.45))))
    }

    #[test]
    fn test_from_token_to_string_double_quoted() {
        let model = match tokenizer::parse_fully("\"123.45\"").as_slice() {
            [tok] => Expression::from_token(tok.to_owned()),
            _ => UNDEFINED
        };
        assert_eq!(model, Literal(StringValue("123.45".into())))
    }

    #[test]
    fn test_from_token_to_string_single_quoted() {
        let model = match tokenizer::parse_fully("'123.45'").as_slice() {
            [tok] => Expression::from_token(tok.to_owned()),
            _ => UNDEFINED
        };
        assert_eq!(model, Literal(StringValue("123.45".into())))
    }

    #[test]
    fn test_from_token_to_variable() {
        let model = match tokenizer::parse_fully("`symbol`").as_slice() {
            [tok] => Expression::from_token(tok.to_owned()),
            _ => UNDEFINED
        };
        assert_eq!(model, Variable("symbol".into()))
    }

    #[test]
    fn test_conditional_and() {
        let machine = Machine::new();
        let model = And(Box::new(TRUE), Box::new(FALSE));
        let (_, result) = machine.evaluate_cond(&model).unwrap();
        assert_eq!(result, Boolean(false));
        assert_eq!(model.to_code(), "true && false")
    }

    #[test]
    fn test_between_expression() {
        let machine = Machine::new();
        let model = Between(
            Box::new(Literal(Number(I32Value(10)))),
            Box::new(Literal(Number(I32Value(1)))),
            Box::new(Literal(Number(I32Value(10)))),
        );
        let (_, result) = machine.evaluate_cond(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "10 between 1 and 10")
    }

    #[test]
    fn test_betwixt_expression() {
        let machine = Machine::new();
        let model = Betwixt(
            Box::new(Literal(Number(I32Value(10)))),
            Box::new(Literal(Number(I32Value(1)))),
            Box::new(Literal(Number(I32Value(10)))),
        );
        let (_, result) = machine.evaluate_cond(&model).unwrap();
        assert_eq!(result, Boolean(false));
        assert_eq!(model.to_code(), "10 betwixt 1 and 10")
    }

    #[test]
    fn test_equality_integers() {
        let machine = Machine::new();
        let model = Equal(
            Box::new(Literal(Number(I32Value(5)))),
            Box::new(Literal(Number(I32Value(5)))),
        );
        let (_, result) = machine.evaluate_cond(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "5 == 5")
    }

    #[test]
    fn test_equality_floats() {
        let machine = Machine::new();
        let model = Equal(
            Box::new(Literal(Number(F64Value(5.)))),
            Box::new(Literal(Number(F64Value(5.)))),
        );
        let (_, result) = machine.evaluate_cond(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "5 == 5")
    }

    #[test]
    fn test_equality_strings() {
        let machine = Machine::new();
        let model = Equal(
            Box::new(Literal(StringValue("Hello".to_string()))),
            Box::new(Literal(StringValue("Hello".to_string()))),
        );
        let (_, result) = machine.evaluate_cond(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "\"Hello\" == \"Hello\"")
    }

    #[test]
    fn test_inequality_strings() {
        let machine = Machine::new();
        let model = NotEqual(
            Box::new(Literal(StringValue("Hello".to_string()))),
            Box::new(Literal(StringValue("Goodbye".to_string()))),
        );
        let (_, result) = machine.evaluate_cond(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "\"Hello\" != \"Goodbye\"")
    }

    #[test]
    fn test_greater_than() {
        let machine = Machine::new()
            .with_variable("x", Number(I64Value(5)));
        let model = GreaterThan(
            Box::new(Variable("x".into())),
            Box::new(Literal(Number(I64Value(1)))),
        );
        let (_, result) = machine.evaluate_cond(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "x > 1")
    }

    #[test]
    fn test_greater_than_or_equal() {
        let machine = Machine::new();
        let model = GreaterOrEqual(
            Box::new(Literal(Number(I32Value(5)))),
            Box::new(Literal(Number(I32Value(1)))),
        );
        let (_, result) = machine.evaluate_cond(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "5 >= 1")
    }

    #[test]
    fn test_less_than() {
        let machine = Machine::new();
        let model = LessThan(
            Box::new(Literal(Number(I32Value(4)))),
            Box::new(Literal(Number(I32Value(5)))),
        );
        let (_, result) = machine.evaluate_cond(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "4 < 5")
    }

    #[test]
    fn test_less_than_or_equal() {
        let machine = Machine::new();
        let model = LessOrEqual(
            Box::new(Literal(Number(I32Value(1)))),
            Box::new(Literal(Number(I32Value(5)))),
        );
        let (_, result) = machine.evaluate_cond(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "1 <= 5")
    }

    #[test]
    fn test_not_equal() {
        let machine = Machine::new();
        let model = NotEqual(
            Box::new(Literal(Number(I32Value(-5)))),
            Box::new(Literal(Number(I32Value(5)))),
        );
        let (_, result) = machine.evaluate_cond(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "-5 != 5")
    }

    #[test]
    fn test_conditional_or() {
        let machine = Machine::new();
        let model = Or(Box::new(TRUE), Box::new(FALSE));
        let (_, result) = machine.evaluate_cond(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "true || false")
    }

    #[test]
    fn test_is_conditional() {
        let model = Conditional(And(Box::new(TRUE), Box::new(FALSE)));
        assert_eq!(model.to_code(), "true && false");
        assert!(model.is_conditional());

        let model = Conditional(Between(
            Box::new(Variable("x".into())),
            Box::new(Literal(Number(I32Value(1)))),
            Box::new(Literal(Number(I32Value(10)))),
        ));
        assert_eq!(model.to_code(), "x between 1 and 10");
        assert!(model.is_conditional());

        let model = Conditional(Or(Box::new(TRUE), Box::new(FALSE)));
        assert_eq!(model.to_code(), "true || false");
        assert!(model.is_conditional());
    }

    #[test]
    fn test_if_is_control_flow() {
        let op = If {
            condition: Box::new(Conditional(LessThan(
                Box::new(Variable("x".into())),
                Box::new(Variable("y".into())),
            ))),
            a: Box::new(Literal(Number(I32Value(1)))),
            b: Some(Box::new(Literal(Number(I32Value(10))))),
        };
        assert!(op.is_control_flow());
        assert_eq!(op.to_code(), "if x < y 1 else 10");
    }

    #[test]
    fn test_from() {
        let from = From(Box::new(
            Ns(Box::new(Literal(StringValue("machine.overwrite.stocks".into()))))
        ));
        let from = Quarry(Query(Queryable::Where {
            from: Box::new(from),
            condition: GreaterOrEqual(
                Box::new(Variable("last_sale".into())),
                Box::new(Literal(Number(F64Value(1.25)))),
            ),
        }));
        let from = Quarry(Query(Queryable::Limit {
            from: Box::new(from),
            limit: Box::new(Literal(Number(I64Value(5)))),
        }));
        assert_eq!(
            from.to_code(),
            "from ns(\"machine.overwrite.stocks\") where last_sale >= 1.25 limit 5"
        )
    }

    #[test]
    fn test_overwrite() {
        let model = Quarry(Mutate(Mutation::Overwrite {
            path: Box::new(Variable("stocks".into())),
            source: Box::new(Via(Box::new(JSONLiteral(vec![
                ("symbol".into(), Literal(StringValue("BOX".into()))),
                ("exchange".into(), Literal(StringValue("NYSE".into()))),
                ("last_sale".into(), Literal(Number(F64Value(21.77)))),
            ])))),
            condition: Some(Equal(
                Box::new(Variable("symbol".into())),
                Box::new(Literal(StringValue("BOX".into()))),
            )),
            limit: Some(Box::new(Literal(Number(I64Value(1))))),
        }));
        assert_eq!(
            model.to_code(),
            r#"overwrite stocks via {symbol: "BOX", exchange: "NYSE", last_sale: 21.77} where symbol == "BOX" limit 1"#)
    }

    #[test]
    fn test_while_is_control_flow() {
        // CodeBlock(..) | If(..) | Return(..) | While { .. }
        let op = While {
            condition: Box::new(Conditional(LessThan(
                Box::new(Variable("x".into())),
                Box::new(Variable("y".into())))
            )),
            code: Box::new(Literal(Number(I32Value(1)))),
        };
        assert!(op.is_control_flow());
    }

    #[test]
    fn test_is_referential() {
        assert!(Variable("symbol".into()).is_referential());
    }
}