////////////////////////////////////////////////////////////////////
// Expression class
////////////////////////////////////////////////////////////////////

use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::codec::Codec;
use crate::data_types::DataType;
use crate::decompiler::Decompiler;
use crate::errors::Errors::IllegalOperator;
use crate::expression::Expression::*;
use crate::inferences::Inferences;
use crate::numbers::NumberValue;
use crate::outcomes::Outcomes;
use crate::parameter::Parameter;
use crate::tokens::Token;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{ErrorValue, Number, Outcome, StringValue};

// constants
pub const ACK: Expression = Literal(Outcome(Outcomes::Ack));
pub const FALSE: Expression = Condition(Conditions::False);
pub const TRUE: Expression = Condition(Conditions::True);
pub const NULL: Expression = Literal(TypedValue::Null);
pub const UNDEFINED: Expression = Literal(TypedValue::Undefined);

/// Represents Bitwise Operations
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum BitwiseOps {
    And(Box<Expression>, Box<Expression>),
    Or(Box<Expression>, Box<Expression>),
    ShiftLeft(Box<Expression>, Box<Expression>),
    ShiftRight(Box<Expression>, Box<Expression>),
    Xor(Box<Expression>, Box<Expression>),
}

impl BitwiseOps {
    /// Returns a string representation of this object
    pub fn to_code(&self) -> String {
        Decompiler::new().decompile_bitwise(self)
    }
}

/// Represents Logical Conditions
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Conditions {
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
    Like(Box<Expression>, Box<Expression>),
    Not(Box<Expression>),
    NotEqual(Box<Expression>, Box<Expression>),
    Or(Box<Expression>, Box<Expression>),
    True,
}

impl Conditions {
    /// Returns a string representation of this object
    pub fn to_code(&self) -> String {
        Decompiler::new().decompile_cond(self)
    }
}

/// Represents the set of all Directives
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Directives {
    MustAck(Box<Expression>),
    MustDie(Box<Expression>),
    MustIgnoreAck(Box<Expression>),
    MustNotAck(Box<Expression>),
}

/// Represents the set of all Quarry Activities
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Excavation {
    Construct(Infrastructure),
    Query(Queryable),
    Mutate(Mutation),
}

/// Represents a Creation Entity
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CreationEntity {
    IndexEntity {
        columns: Vec<Expression>,
    },
    TableEntity {
        columns: Vec<Parameter>,
        from: Option<Box<Expression>>,
    },
}

/// Represents an import definition
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum ImportOps {
    Everything(String),
    Selection(String, Vec<String>),
}

impl ImportOps {
    pub fn to_code(&self) -> String {
        match self {
            ImportOps::Everything(pkg) => pkg.to_string(),
            ImportOps::Selection(pkg, items) =>
                format!("{pkg}::{}", items.join(", "))
        }
    }
}

impl Display for ImportOps {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_code())
    }
}

/// Represents an infrastructure construction/deconstruction event
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Infrastructure {
    Create { path: Box<Expression>, entity: CreationEntity },
    Declare(CreationEntity),
    Drop(MutateTarget),
}

/// Represents a data modification event
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Mutation {
    Append {
        path: Box<Expression>,
        source: Box<Expression>,
    },
    Delete {
        path: Box<Expression>,
        condition: Option<Conditions>,
        limit: Option<Box<Expression>>,
    },
    IntoNs(Box<Expression>, Box<Expression>),
    Overwrite {
        path: Box<Expression>,
        source: Box<Expression>,
        condition: Option<Conditions>,
        limit: Option<Box<Expression>>,
    },
    Truncate {
        path: Box<Expression>,
        limit: Option<Box<Expression>>,
    },
    Undelete {
        path: Box<Expression>,
        condition: Option<Conditions>,
        limit: Option<Box<Expression>>,
    },
    Update {
        path: Box<Expression>,
        source: Box<Expression>,
        condition: Option<Conditions>,
        limit: Option<Box<Expression>>,
    },
}

/// Represents a Mutation Target
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum MutateTarget {
    IndexTarget {
        path: Box<Expression>,
    },
    TableTarget {
        path: Box<Expression>,
    },
}

/// Represents a queryable
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Queryable {
    Limit { from: Box<Expression>, limit: Box<Expression> },
    Select {
        fields: Vec<Expression>,
        from: Option<Box<Expression>>,
        condition: Option<Conditions>,
        group_by: Option<Vec<Expression>>,
        having: Option<Box<Expression>>,
        order_by: Option<Vec<Expression>>,
        limit: Option<Box<Expression>>,
    },
    Where { from: Box<Expression>, condition: Conditions },
}

/// Represents an Expression
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Expression {
    ArrayExpression(Vec<Expression>),
    AsValue(String, Box<Expression>),
    BitwiseOp(BitwiseOps),
    CodeBlock(Vec<Expression>),
    Condition(Conditions),
    Directive(Directives),
    Divide(Box<Expression>, Box<Expression>),
    ElementAt(Box<Expression>, Box<Expression>),
    Extraction(Box<Expression>, Box<Expression>),
    ExtractPostfix(Box<Expression>, Box<Expression>),
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
    Import(Vec<ImportOps>),
    Include(Box<Expression>),
    JSONExpression(Vec<(String, Expression)>),
    Literal(TypedValue),
    Minus(Box<Expression>, Box<Expression>),
    Module(String, Vec<Expression>),
    Modulo(Box<Expression>, Box<Expression>),
    Multiply(Box<Expression>, Box<Expression>),
    Neg(Box<Expression>),
    Ns(Box<Expression>),
    Parameters(Vec<Parameter>),
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
        Codec::encode(&self).unwrap_or_else(|e| panic!("{}", e))
    }

    pub fn from_token(token: Token) -> Self {
        match token.to_owned() {
            Token::Atom { text, .. } => Variable(text),
            Token::Backticks { text, .. } => Variable(text),
            Token::DoubleQuoted { text, .. } => Literal(StringValue(text)),
            Token::Numeric { text, .. } => Literal(Number(NumberValue::from_string(text))),
            Token::Operator { .. } => Literal(ErrorValue(IllegalOperator(token))),
            Token::SingleQuoted { text, .. } => Literal(StringValue(text)),
        }
    }

    pub fn infer_type(&self) -> DataType {
        Inferences::infer(self)
    }

    /// Indicates whether the expression is a conditional expression
    pub fn is_conditional(&self) -> bool {
        matches!(self, Condition(..))
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

fn to_ns(path: Expression) -> Expression {
    fn fx(name: &str, path: Expression) -> Expression {
        FunctionCall {
            fx: Box::new(Variable(name.into())),
            args: vec![path],
        }
    }
    fx("ns", path)
}

/// Unit tests
#[cfg(test)]
mod tests {
    use crate::expression::Conditions::*;
    use crate::expression::Excavation::{Mutate, Query};
    use crate::expression::*;
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
        let machine = Machine::empty();
        let model = And(Box::new(TRUE), Box::new(FALSE));
        let (_, result) = machine.evaluate_cond(&model).unwrap();
        assert_eq!(result, Boolean(false));
        assert_eq!(model.to_code(), "true && false")
    }

    #[test]
    fn test_between_expression() {
        let machine = Machine::empty();
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
        let machine = Machine::empty();
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
        let machine = Machine::empty();
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
        let machine = Machine::empty();
        let model = Equal(
            Box::new(Literal(Number(F64Value(5.1)))),
            Box::new(Literal(Number(F64Value(5.1)))),
        );
        let (_, result) = machine.evaluate_cond(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "5.1 == 5.1")
    }

    #[test]
    fn test_equality_strings() {
        let machine = Machine::empty();
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
        let machine = Machine::empty();
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
        let machine = Machine::empty()
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
        let machine = Machine::empty();
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
        let machine = Machine::empty();
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
        let machine = Machine::empty();
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
        let machine = Machine::empty();
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
        let machine = Machine::empty();
        let model = Or(Box::new(TRUE), Box::new(FALSE));
        let (_, result) = machine.evaluate_cond(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "true || false")
    }

    #[test]
    fn test_is_conditional() {
        let model = Condition(And(Box::new(TRUE), Box::new(FALSE)));
        assert_eq!(model.to_code(), "true && false");
        assert!(model.is_conditional());

        let model = Condition(Between(
            Box::new(Variable("x".into())),
            Box::new(Literal(Number(I32Value(1)))),
            Box::new(Literal(Number(I32Value(10)))),
        ));
        assert_eq!(model.to_code(), "x between 1 and 10");
        assert!(model.is_conditional());

        let model = Condition(Or(Box::new(TRUE), Box::new(FALSE)));
        assert_eq!(model.to_code(), "true || false");
        assert!(model.is_conditional());
    }

    #[test]
    fn test_if_is_control_flow() {
        let op = If {
            condition: Box::new(Condition(LessThan(
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
            source: Box::new(Via(Box::new(JSONExpression(vec![
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
            condition: Box::new(Condition(LessThan(
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