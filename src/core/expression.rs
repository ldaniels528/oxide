////////////////////////////////////////////////////////////////////
// expression module
////////////////////////////////////////////////////////////////////

use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::expression::CreationEntity::{IndexEntity, TableEntity};
use crate::expression::DropTarget::{IndexTarget, TableTarget};
use crate::expression::Expression::*;
use crate::serialization::assemble;
use crate::server::ColumnJs;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{Boolean, Null, Undefined};

pub const FALSE: Expression = Literal(Boolean(false));
pub const TRUE: Expression = Literal(Boolean(true));
pub const NULL: Expression = Literal(Null);
pub const UNDEFINED: Expression = Literal(Undefined);

/// Represents a Creation Entity
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CreationEntity {
    IndexEntity {
        path: Box<Expression>,
        columns: Vec<Expression>,
    },
    TableEntity {
        path: Box<Expression>,
        columns: Vec<ColumnJs>,
        from: Option<Box<Expression>>,
    },
}

/// Represents a Drop Target
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum DropTarget {
    IndexTarget {
        path: Box<Expression>,
        if_exists: bool,
    },
    TableTarget {
        path: Box<Expression>,
        if_exists: bool,
    },
}

/// Represents an Expression
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Expression {
    // conditional
    And(Box<Expression>, Box<Expression>),
    Between(Box<Expression>, Box<Expression>, Box<Expression>),
    Contains(Box<Expression>, Box<Expression>),
    Equal(Box<Expression>, Box<Expression>),
    GreaterThan(Box<Expression>, Box<Expression>),
    GreaterOrEqual(Box<Expression>, Box<Expression>),
    LessThan(Box<Expression>, Box<Expression>),
    LessOrEqual(Box<Expression>, Box<Expression>),
    Not(Box<Expression>),
    NotEqual(Box<Expression>, Box<Expression>),
    Or(Box<Expression>, Box<Expression>),
    // bitwise operations
    BitwiseAnd(Box<Expression>, Box<Expression>),
    BitwiseOr(Box<Expression>, Box<Expression>),
    BitwiseXor(Box<Expression>, Box<Expression>),
    ShiftLeft(Box<Expression>, Box<Expression>),
    ShiftRight(Box<Expression>, Box<Expression>),
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
    ArrayLiteral(Vec<Expression>),
    AsValue(String, Box<Expression>),
    ColumnSet(Vec<ColumnJs>),
    FunctionCall { fx: Box<Expression>, args: Vec<Expression> },
    JSONLiteral(Vec<(String, Expression)>),
    Literal(TypedValue),
    Ns(Box<Expression>),
    Range(Box<Expression>, Box<Expression>),
    TupleExpr(Vec<Expression>),
    Variable(String),
    // assignment
    SetVariable(String, Box<Expression>),
    // control/flow
    CodeBlock(Vec<Expression>),
    Eval(Box<Expression>),
    If {
        condition: Box<Expression>,
        a: Box<Expression>,
        b: Option<Box<Expression>>,
    },
    Return(Vec<Expression>),
    While {
        condition: Box<Expression>,
        code: Box<Expression>,
    },
    // SQL
    Create(CreationEntity),
    Delete {
        path: Box<Expression>,
        condition: Option<Box<Expression>>,
        limit: Option<Box<Expression>>,
    },
    Drop(DropTarget),
    From(Box<Expression>),
    InsertInto {
        path: Box<Expression>,
        source: Box<Expression>,
    },
    Limit { from: Box<Expression>, limit: Box<Expression> },
    Overwrite {
        path: Box<Expression>,
        source: Box<Expression>,
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
        path: Box<Expression>,
        limit: Option<Box<Expression>>,
    },
    Update {
        path: Box<Expression>,
        source: Box<Expression>,
        condition: Option<Box<Expression>>,
        limit: Option<Box<Expression>>,
    },
    Via(Box<Expression>),
    Where { from: Box<Expression>, condition: Box<Expression> },
}

impl Expression {

    ////////////////////////////////////////////////////////////////
    // instance methods
    ////////////////////////////////////////////////////////////////

    pub fn encode(&self) -> Vec<u8> {
        assemble(&self)
    }

    /// Indicates whether the expression is a conditional expression
    pub fn is_conditional(&self) -> bool {
        matches!(self, And(..) | Between(..) | Contains(..) | Equal(..) |
            GreaterThan(..) | GreaterOrEqual(..) | LessThan(..) | LessOrEqual(..) |
            Not(..) | NotEqual(..) | Or(..))
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
        decompile(self)
    }
}

impl Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_code())
    }
}

pub fn decompile(expr: &Expression) -> String {
    match expr {
        And(a, b) =>
            format!("{} && {}", decompile(a), decompile(b)),
        ArrayLiteral(items) =>
            format!("[{}]", items.iter().map(|i| decompile(i)).collect::<Vec<String>>().join(", ")),
        AsValue(name, expr) =>
            format!("{}: {}", name, decompile(expr)),
        Between(a, b, c) =>
            format!("{} between {} and {}", decompile(a), decompile(b), decompile(c)),
        BitwiseAnd(a, b) =>
            format!("{} & {}", decompile(a), decompile(b)),
        BitwiseOr(a, b) =>
            format!("{} | {}", decompile(a), decompile(b)),
        Contains(a, b) =>
            format!("{} contains {}", decompile(a), decompile(b)),
        ColumnSet(columns) =>
            format!("({})", columns.iter().map(|c|
                format!("{}: {}", c.get_name(), c.get_column_type()))
                .collect::<Vec<String>>().join(", ")),
        Create(entity) => {
            match entity {
                IndexEntity { path, columns } =>
                    format!("create index {} [{}]", decompile(path), decompile_list(columns)),
                TableEntity { path, columns, from } =>
                    format!("create table {} ({})", decompile(path), decompile_columns(columns)),
            }
        }
        Delete { path, condition, limit } =>
            format!("delete from {} where {}{}", decompile(path), decompile_opt(condition), decompile_opt(limit)),
        Drop(target) => {
            let (kind, path, if_exists) = match target {
                IndexTarget { path, if_exists } => ("index", path, if_exists),
                TableTarget { path, if_exists } => ("table", path, if_exists),
            };
            format!("drop {} {}{}", kind, decompile_if_exists(*if_exists), decompile(path))
        }
        From(a) => format!("from {}", decompile(a)),
        FunctionCall { fx, args } =>
            format!("{}({})", decompile(fx), decompile_list(args)),
        JSONLiteral(items) => {
            let dict = items.iter()
                .map(|(k, v)| format!("{}: {}", k, v))
                .collect::<Vec<String>>()
                .join(", ");
            format!("{{{}}}", dict)
        }
        Equal(a, b) =>
            format!("{} == {}", decompile(a), decompile(b)),
        Eval(a) => format!("eval {}", decompile(a)),
        GreaterThan(a, b) =>
            format!("{} > {}", decompile(a), decompile(b)),
        GreaterOrEqual(a, b) =>
            format!("{} >= {}", decompile(a), decompile(b)),
        LessThan(a, b) =>
            format!("{} < {}", decompile(a), decompile(b)),
        LessOrEqual(a, b) =>
            format!("{} <= {}", decompile(a), decompile(b)),
        Not(a) =>
            format!("!{}", decompile(a)),
        NotEqual(a, b) =>
            format!("{} != {}", decompile(a), decompile(b)),
        Or(a, b) =>
            format!("{} || {}", decompile(a), decompile(b)),
        ShiftLeft(a, b) =>
            format!("{} << {}", decompile(a), decompile(b)),
        ShiftRight(a, b) =>
            format!("{} >> {}", decompile(a), decompile(b)),
        BitwiseXor(a, b) =>
            format!("{} ^ {}", decompile(a), decompile(b)),
        Divide(a, b) =>
            format!("{} / {}", decompile(a), decompile(b)),
        Factorial(a) =>
            format!("¡{}", decompile(a)),
        Plus(a, b) =>
            format!("{} + {}", decompile(a), decompile(b)),
        Pow(a, b) =>
            format!("{} ** {}", decompile(a), decompile(b)),
        Minus(a, b) =>
            format!("{} - {}", decompile(a), decompile(b)),
        Modulo(a, b) =>
            format!("{} % {}", decompile(a), decompile(b)),
        Multiply(a, b) =>
            format!("{} * {}", decompile(a), decompile(b)),
        Neg(a) => format!("-({})", decompile(a)),
        Literal(value) => value.to_code(),
        Ns(a) => format!("ns({})", decompile(a)),
        Range(a, b) =>
            format!("{}..{}", decompile(a), decompile(b)),
        TupleExpr(items) =>
            format!("({})", decompile_list(items)),
        Variable(name) => name.to_string(),
        Via(expr) => format!("via {}", decompile(expr)),
        SetVariable(name, value) =>
            format!("{} := {}", name, decompile(value)),
        CodeBlock(items) =>
            format!("{{\n {}\n }}", items.iter().map(|i| decompile(i)).collect::<Vec<String>>().join("\n")),
        If { condition, a, b } =>
            format!("if {} {}{}", decompile(condition), decompile(a), b.clone()
                .map(|x| format!(" else {}", decompile(&x)))
                .unwrap_or("".into())),
        Return(items) =>
            format!("return {}", decompile_list(items)),
        While { condition, code } =>
            format!("while {} do {}", decompile(condition), decompile(code)),

        InsertInto { path, source } =>
            format!("into {} {}", decompile(path), decompile(source)),
        Limit { from: a, limit: b } =>
            format!("{} limit {}", decompile(a), decompile(b)),
        Overwrite { path, source, condition, limit } =>
            format!("overwrite {} {}{}{}", decompile(path), decompile(source),
                    condition.clone().map(|e| format!(" where {}", decompile(&e))).unwrap_or("".into()),
                    limit.clone().map(|e| format!(" limit {}", decompile(&e))).unwrap_or("".into()),
            ),
        Select { fields, from, condition, group_by, having, order_by, limit } =>
            format!("select {}{}{}{}{}{}{}", decompile_list(fields),
                    from.clone().map(|e| format!(" from {}", decompile(&e))).unwrap_or("".into()),
                    condition.clone().map(|e| format!(" where {}", decompile(&e))).unwrap_or("".into()),
                    limit.clone().map(|e| format!(" limit {}", decompile(&e))).unwrap_or("".into()),
                    group_by.clone().map(|items| format!(" group by {}", items.iter().map(|e| decompile(e)).collect::<Vec<String>>().join(", "))).unwrap_or("".into()),
                    having.clone().map(|e| format!(" having {}", decompile(&e))).unwrap_or("".into()),
                    order_by.clone().map(|e| format!(" order by {}", decompile_list(&e))).unwrap_or("".into()),
            ),
        Truncate { path, limit } =>
            format!("truncate {}{}", decompile(path), decompile_limit(limit)),
        Update { path, source, condition, limit } =>
            format!("update {} {} where {}{}", decompile(path), decompile(source), decompile_opt(condition), decompile_opt(limit)),
        Where { from, condition } =>
            format!("{} where {}", decompile(from), decompile(condition)),
    }
}

fn decompile_columns(columns: &Vec<ColumnJs>) -> String {
    columns.iter()
        .map(|c| format!("{}: {}", c.get_name(), c.get_column_type()))
        .collect::<Vec<String>>().join(", ")
}

fn decompile_if_exists(if_exists: bool) -> String {
    (if if_exists { "if exists " } else { "" }).to_string()
}

fn decompile_insert_list(fields: &Vec<Expression>, values: &Vec<Expression>) -> String {
    let field_list = fields.iter().map(|f| decompile(f)).collect::<Vec<String>>().join(", ");
    let value_list = values.iter().map(|v| decompile(v)).collect::<Vec<String>>().join(", ");
    format!("({}) values ({})", field_list, value_list)
}

fn decompile_limit(opt: &Option<Box<Expression>>) -> String {
    opt.clone().map(|x| format!(" limit {}", decompile(&x))).unwrap_or("".into())
}

fn decompile_list(fields: &Vec<Expression>) -> String {
    fields.iter().map(|x| decompile(x)).collect::<Vec<String>>().join(", ".into())
}

fn decompile_opt(opt: &Option<Box<Expression>>) -> String {
    opt.clone().map(|i| decompile(&i)).unwrap_or("".into())
}

fn decompile_update_list(fields: &Vec<Expression>, values: &Vec<Expression>) -> String {
    fields.iter().zip(values.iter()).map(|(f, v)|
        format!("{} = {}", decompile(f), decompile(v))).collect::<Vec<String>>().join(", ")
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::expression::{FALSE, TRUE};
    use crate::expression::Expression::*;
    use crate::machine::MachineState;
    use crate::typed_values::TypedValue::{Boolean, Float64Value, Int32Value, Int64Value, StringValue};

    #[test]
    fn test_decompile_from() {
        let from = From(Box::new(
            Ns(Box::new(Literal(StringValue("machine.overwrite.stocks".into()))))
        ));
        let from = Where {
            from: Box::new(from),
            condition: Box::new(GreaterOrEqual(
                Box::new(Variable("last_sale".into())),
                Box::new(Literal(Float64Value(1.25))),
            )),
        };
        let from = Limit { from: Box::new(from), limit: Box::new(Literal(Int64Value(5))) };
        assert_eq!(
            from.to_code(),
            "from ns(\"machine.overwrite.stocks\") where last_sale >= 1.25 limit 5"
        )
    }

    #[test]
    fn test_and() {
        let ms = MachineState::new();
        let model = And(Box::new(TRUE), Box::new(FALSE));
        let (_, result) = ms.evaluate(&model).unwrap();
        assert_eq!(result, Boolean(false));
        assert_eq!(model.to_code(), "true && false")
    }

    #[test]
    fn test_between() {
        let ms = MachineState::new();
        let model = Between(
            Box::new(Literal(Int32Value(5))),
            Box::new(Literal(Int32Value(1))),
            Box::new(Literal(Int32Value(10))));
        let (_, result) = ms.evaluate(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "5 between 1 and 10")
    }

    #[test]
    fn test_eq() {
        let ms = MachineState::new();
        let model = Equal(
            Box::new(Literal(Int32Value(5))),
            Box::new(Literal(Int32Value(5))));
        let (_, result) = ms.evaluate(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "5 == 5")
    }

    #[test]
    fn test_gt() {
        let ms = MachineState::new();
        let model = GreaterThan(
            Box::new(Literal(Int32Value(5))),
            Box::new(Literal(Int32Value(1))));
        let (_, result) = ms.evaluate(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "5 > 1")
    }

    #[test]
    fn test_gte() {
        let ms = MachineState::new();
        let model = GreaterOrEqual(
            Box::new(Literal(Int32Value(5))),
            Box::new(Literal(Int32Value(1))));
        let (_, result) = ms.evaluate(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "5 >= 1")
    }

    #[test]
    fn test_lt() {
        let ms = MachineState::new();
        let model = LessThan(
            Box::new(Literal(Int32Value(4))),
            Box::new(Literal(Int32Value(5))));
        let (_, result) = ms.evaluate(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "4 < 5")
    }

    #[test]
    fn test_lte() {
        let ms = MachineState::new();
        let model = LessOrEqual(
            Box::new(Literal(Int32Value(1))),
            Box::new(Literal(Int32Value(5))));
        let (_, result) = ms.evaluate(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "1 <= 5")
    }

    #[test]
    fn test_ne() {
        let ms = MachineState::new();
        let model = NotEqual(
            Box::new(Literal(Int32Value(-5))),
            Box::new(Literal(Int32Value(5))));
        let (_, result) = ms.evaluate(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "-5 != 5")
    }

    #[test]
    fn test_or() {
        let ms = MachineState::new();
        let model = Or(Box::new(TRUE), Box::new(FALSE));
        let (_, result) = ms.evaluate(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "true || false")
    }

    #[test]
    fn test_is_conditional() {
        let model = And(Box::new(TRUE), Box::new(FALSE));
        assert_eq!(model.to_code(), "true && false");
        assert!(model.is_conditional());

        let model = Between(
            Box::new(Variable("x".into())),
            Box::new(Literal(Int32Value(1))),
            Box::new(Literal(Int32Value(10))
            ));
        assert_eq!(model.to_code(), "x between 1 and 10");
        assert!(model.is_conditional());

        let model = Or(Box::new(TRUE), Box::new(FALSE));
        assert_eq!(model.to_code(), "true || false");
        assert!(model.is_conditional());
    }

    #[test]
    fn test_if_is_control_flow() {
        let op = If {
            condition: Box::new(LessThan(
                Box::new(Variable("x".into())),
                Box::new(Variable("y".into())),
            )),
            a: Box::new(Literal(Int32Value(1))),
            b: Some(Box::new(Literal(Int32Value(10)))),
        };
        assert!(op.is_control_flow());
        assert_eq!(op.to_code(), "if x < y 1 else 10");
    }

    #[test]
    fn test_overwrite() {
        let model = Overwrite {
            path: Box::new(Variable("stocks".into())),
            source: Box::new(Via(Box::new(JSONLiteral(vec![
                ("symbol".into(), Literal(StringValue("BOX".into()))),
                ("exchange".into(), Literal(StringValue("NYSE".into()))),
                ("last_sale".into(), Literal(Float64Value(21.77))),
            ])))),
            condition: Some(Box::new(Equal(
                Box::new(Variable("symbol".into())),
                Box::new(Literal(StringValue("BOX".into()))),
            ))),
            limit: Some(Box::new(Literal(Int64Value(1)))),
        };
        assert_eq!(
            model.to_code(),
            r#"overwrite stocks via {symbol: "BOX", exchange: "NYSE", last_sale: 21.77} where symbol == "BOX" limit 1"#)
    }

    #[test]
    fn test_while_is_control_flow() {
        // CodeBlock(..) | If(..) | Return(..) | While { .. }
        let op = While {
            condition: Box::new(LessThan(Box::new(Variable("x".into())), Box::new(Variable("y".into())))),
            code: Box::new(Literal(Int32Value(1))),
        };
        assert!(op.is_control_flow());
    }

    #[test]
    fn test_is_referential() {
        assert!(Variable("symbol".into()).is_referential());
    }
}