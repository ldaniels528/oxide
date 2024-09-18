////////////////////////////////////////////////////////////////////
// expression module
////////////////////////////////////////////////////////////////////

use std::fmt::Display;

use serde::{Deserialize, Serialize};
use crate::byte_code_compiler::ByteCodeCompiler;

use crate::expression::CreationEntity::{IndexEntity, TableEntity};
use crate::expression::Expression::*;
use crate::expression::MutateTarget::{IndexTarget, TableTarget};
use crate::server::ColumnJs;
use crate::typed_values::TypedValue;

// constants
pub const ACK: Expression = Literal(TypedValue::Ack);
pub const FALSE: Expression = Literal(TypedValue::Boolean(false));
pub const TRUE: Expression = Literal(TypedValue::Boolean(true));
pub const NULL: Expression = Literal(TypedValue::Null);
pub const UNDEFINED: Expression = Literal(TypedValue::Undefined);

/// Represents a Creation Entity
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CreationEntity {
    IndexEntity {
        columns: Vec<Expression>,
    },
    TableEntity {
        columns: Vec<ColumnJs>,
        from: Option<Box<Expression>>,
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

/// Represents an Expression
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Expression {
    And(Box<Expression>, Box<Expression>),
    ArrayLiteral(Vec<Expression>),
    AsValue(String, Box<Expression>),
    Between(Box<Expression>, Box<Expression>, Box<Expression>),
    Betwixt(Box<Expression>, Box<Expression>, Box<Expression>),
    BitwiseAnd(Box<Expression>, Box<Expression>),
    BitwiseOr(Box<Expression>, Box<Expression>),
    BitwiseXor(Box<Expression>, Box<Expression>),
    CodeBlock(Vec<Expression>),
    ColumnSet(Vec<ColumnJs>),
    Contains(Box<Expression>, Box<Expression>),
    Divide(Box<Expression>, Box<Expression>),
    ElementAt(Box<Expression>, Box<Expression>),
    Equal(Box<Expression>, Box<Expression>),
    Factorial(Box<Expression>),
    Feature { title: Box<Expression>, scenarios: Vec<Expression> },
    From(Box<Expression>),
    FunctionCall { fx: Box<Expression>, args: Vec<Expression> },
    GreaterOrEqual(Box<Expression>, Box<Expression>),
    GreaterThan(Box<Expression>, Box<Expression>),
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
    Inquire(Queryable),
    JSONLiteral(Vec<(String, Expression)>),
    LessOrEqual(Box<Expression>, Box<Expression>),
    LessThan(Box<Expression>, Box<Expression>),
    Literal(TypedValue),
    Minus(Box<Expression>, Box<Expression>),
    Modulo(Box<Expression>, Box<Expression>),
    Multiply(Box<Expression>, Box<Expression>),
    MustAck(Box<Expression>),
    MustDie(Box<Expression>),
    MustIgnoreAck(Box<Expression>),
    MustNotAck(Box<Expression>),
    Mutate(Mutation),
    Neg(Box<Expression>),
    Not(Box<Expression>),
    NotEqual(Box<Expression>, Box<Expression>),
    Ns(Box<Expression>),
    Or(Box<Expression>, Box<Expression>),
    Perform(Infrastructure),
    Plus(Box<Expression>, Box<Expression>),
    Pow(Box<Expression>, Box<Expression>),
    Range(Box<Expression>, Box<Expression>),
    Return(Vec<Expression>),
    Scenario { title: Box<Expression>, verifications: Vec<Expression>, inherits: Option<Box<Expression>> },
    SERVE(Box<Expression>),
    SetVariable(String, Box<Expression>),
    ShiftLeft(Box<Expression>, Box<Expression>),
    ShiftRight(Box<Expression>, Box<Expression>),
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
    Compact {
        path: Box<Expression>,
    },
    Delete {
        path: Box<Expression>,
        condition: Option<Box<Expression>>,
        limit: Option<Box<Expression>>,
    },
    IntoNs(Box<Expression>, Box<Expression>),
    Overwrite {
        path: Box<Expression>,
        source: Box<Expression>,
        condition: Option<Box<Expression>>,
        limit: Option<Box<Expression>>,
    },
    Scan {
        path: Box<Expression>,
    },
    Truncate {
        path: Box<Expression>,
        limit: Option<Box<Expression>>,
    },
    Undelete {
        path: Box<Expression>,
        condition: Option<Box<Expression>>,
        limit: Option<Box<Expression>>,
    },
    Update {
        path: Box<Expression>,
        source: Box<Expression>,
        condition: Option<Box<Expression>>,
        limit: Option<Box<Expression>>,
    },
}

/// Represents a queryable
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Queryable {
    Describe(Box<Expression>),
    Limit { from: Box<Expression>, limit: Box<Expression> },
    Reverse(Box<Expression>),
    Select {
        fields: Vec<Expression>,
        from: Option<Box<Expression>>,
        condition: Option<Box<Expression>>,
        group_by: Option<Vec<Expression>>,
        having: Option<Box<Expression>>,
        order_by: Option<Vec<Expression>>,
        limit: Option<Box<Expression>>,
    },
    Where { from: Box<Expression>, condition: Box<Expression> },
}

pub fn decompile(expr: &Expression) -> String {
    use Expression::*;
    match expr {
        And(a, b) =>
            format!("{} && {}", decompile(a), decompile(b)),
        ArrayLiteral(items) =>
            format!("[{}]", items.iter().map(|i| decompile(i)).collect::<Vec<String>>().join(", ")),
        AsValue(name, expr) =>
            format!("{}: {}", name, decompile(expr)),
        Between(a, b, c) =>
            format!("{} between {} and {}", decompile(a), decompile(b), decompile(c)),
        Betwixt(a, b, c) =>
            format!("{} betwixt {} and {}", decompile(a), decompile(b), decompile(c)),
        BitwiseAnd(a, b) =>
            format!("{} & {}", decompile(a), decompile(b)),
        BitwiseOr(a, b) =>
            format!("{} | {}", decompile(a), decompile(b)),
        BitwiseXor(a, b) =>
            format!("{} ^ {}", decompile(a), decompile(b)),
        CodeBlock(items) =>
            format!("{{\n {}\n }}", items.iter().map(|i| decompile(i)).collect::<Vec<String>>().join("\n")),
        ColumnSet(columns) =>
            format!("({})", columns.iter().map(|c|
                format!("{}: {}", c.get_name(), c.get_column_type()))
                .collect::<Vec<String>>().join(", ")),
        Contains(a, b) =>
            format!("{} contains {}", decompile(a), decompile(b)),
        Divide(a, b) =>
            format!("{} / {}", decompile(a), decompile(b)),
        ElementAt(a, b) =>
            format!("{}[{}]", decompile(a), decompile(b)),
        Factorial(a) => format!("¡{}", decompile(a)),
        Feature { title, scenarios } => {
            let title = title.to_code();
            let scenarios = scenarios.iter()
                .map(|s| s.to_code())
                .collect::<Vec<_>>()
                .join("\n");
            format!("feature {title} {{\n{scenarios}\n}}")
        }
        From(a) => format!("from {}", decompile(a)),
        Equal(a, b) =>
            format!("{} == {}", decompile(a), decompile(b)),
        FunctionCall { fx, args } =>
            format!("{}({})", decompile(fx), decompile_list(args)),
        GreaterThan(a, b) =>
            format!("{} > {}", decompile(a), decompile(b)),
        GreaterOrEqual(a, b) =>
            format!("{} >= {}", decompile(a), decompile(b)),
        If { condition, a, b } =>
            format!("if {} {}{}", decompile(condition), decompile(a), b.to_owned()
                .map(|x| format!(" else {}", decompile(&x)))
                .unwrap_or("".into())),
        Include(path) => format!("include {}", decompile(path)),
        Inquire(q) => decompile_queryable(q),
        JSONLiteral(items) => {
            let dict = items.iter()
                .map(|(k, v)| format!("{}: {}", k, v))
                .collect::<Vec<String>>()
                .join(", ");
            format!("{{{}}}", dict)
        }
        LessThan(a, b) =>
            format!("{} < {}", decompile(a), decompile(b)),
        LessOrEqual(a, b) =>
            format!("{} <= {}", decompile(a), decompile(b)),
        Literal(value) => value.to_code(),
        Minus(a, b) =>
            format!("{} - {}", decompile(a), decompile(b)),
        Modulo(a, b) =>
            format!("{} % {}", decompile(a), decompile(b)),
        Multiply(a, b) =>
            format!("{} * {}", decompile(a), decompile(b)),
        MustAck(a) => format!("[+] {}", decompile(a)),
        MustDie(a) => format!("[!] {}", decompile(a)),
        MustIgnoreAck(a) => format!("[~] {}", decompile(a)),
        MustNotAck(a) => format!("[-] {}", decompile(a)),
        Mutate(m) => decompile_modification(m),
        Neg(a) => format!("-({})", decompile(a)),
        Not(a) => format!("!{}", decompile(a)),
        NotEqual(a, b) =>
            format!("{} != {}", decompile(a), decompile(b)),
        Ns(a) => format!("ns({})", decompile(a)),
        Or(a, b) =>
            format!("{} || {}", decompile(a), decompile(b)),
        Perform(i) => decompile_infrastructure(i),
        Plus(a, b) =>
            format!("{} + {}", decompile(a), decompile(b)),
        Pow(a, b) =>
            format!("{} ** {}", decompile(a), decompile(b)),
        Range(a, b) =>
            format!("{}..{}", decompile(a), decompile(b)),
        Return(items) =>
            format!("return {}", decompile_list(items)),
        Scenario { title, verifications, inherits } => {
            let title = title.to_code();
            let inherits = inherits.to_owned().map(|e| e.to_code()).unwrap_or(String::new());
            let verifications = verifications.iter()
                .map(|s| s.to_code())
                .collect::<Vec<_>>()
                .join("\n");
            format!("scenario {title} {{\n{verifications}\n}}")
        }
        SERVE(a) => format!("SERVE {}", decompile(a)),
        SetVariable(name, value) =>
            format!("{} := {}", name, decompile(value)),
        ShiftLeft(a, b) =>
            format!("{} << {}", decompile(a), decompile(b)),
        ShiftRight(a, b) =>
            format!("{} >> {}", decompile(a), decompile(b)),
        TupleLiteral(items) =>
            format!("({})", decompile_list(items)),
        Variable(name) => name.to_string(),
        Via(expr) => format!("via {}", decompile(expr)),
        While { condition, code } =>
            format!("while {} {}", decompile(condition), decompile(code)),
        HTTP { method, url, body, headers, multipart } =>
            format!("{} {}{}{}{}", method, decompile(url), decompile_opt(body), decompile_opt(headers), decompile_opt(multipart)),
    }
}

pub fn decompile_infrastructure(expr: &Infrastructure) -> String {
    match expr {
        Infrastructure::Create { path, entity } =>
            match entity {
                IndexEntity { columns } =>
                    format!("create index {} [{}]", decompile(path), decompile_list(columns)),
                TableEntity { columns, from } =>
                    format!("create table {} ({})", decompile(path), decompile_columns(columns)),
            }
        Infrastructure::Declare(entity) =>
            match entity {
                IndexEntity { columns } =>
                    format!("index [{}]", decompile_list(columns)),
                TableEntity { columns, from } =>
                    format!("table({})", decompile_columns(columns)),
            }
        Infrastructure::Drop(target) => {
            let (kind, path) = match target {
                IndexTarget { path } => ("index", path),
                TableTarget { path } => ("table", path),
            };
            format!("drop {} {}", kind, decompile(path))
        }
    }
}

pub fn decompile_modification(expr: &Mutation) -> String {
    match expr {
        Mutation::Append { path, source } =>
            format!("append {} {}", decompile(path), decompile(source)),
        Mutation::Compact { path } =>
            format!("compact {}", decompile(path)),
        Mutation::Delete { path, condition, limit } =>
            format!("delete from {} where {}{}", decompile(path), decompile_opt(condition), decompile_opt(limit)),
        Mutation::IntoNs(a, b) =>
            format!("{} ~> {}", decompile(a), decompile(b)),
        Mutation::Overwrite { path, source, condition, limit } =>
            format!("overwrite {} {}{}{}", decompile(path), decompile(source),
                    condition.to_owned().map(|e| format!(" where {}", decompile(&e))).unwrap_or("".into()),
                    limit.to_owned().map(|e| format!(" limit {}", decompile(&e))).unwrap_or("".into()),
            ),
        Mutation::Scan { path } =>
            format!("scan {}", decompile(path)),
        Mutation::Truncate { path, limit } =>
            format!("truncate {}{}", decompile(path), decompile_limit(limit)),
        Mutation::Undelete { path, condition, limit } =>
            format!("undelete from {} where {}{}", decompile(path), decompile_opt(condition), decompile_opt(limit)),
        Mutation::Update { path, source, condition, limit } =>
            format!("update {} {} where {}{}", decompile(path), decompile(source), decompile_opt(condition),
                    limit.to_owned().map(|e| format!(" limit {}", decompile(&e))).unwrap_or("".into()), ),
    }
}

pub fn decompile_queryable(expr: &Queryable) -> String {
    match expr {
        Queryable::Describe(a) =>
            format!("describe {}", decompile(a)),
        Queryable::Limit { from: a, limit: b } =>
            format!("{} limit {}", decompile(a), decompile(b)),
        Queryable::Where { from, condition } =>
            format!("{} where {}", decompile(from), decompile(condition)),
        Queryable::Reverse(a) => format!("reverse {}", decompile(a)),
        Queryable::Select { fields, from, condition, group_by, having, order_by, limit } =>
            format!("select {}{}{}{}{}{}{}", decompile_list(fields),
                    from.to_owned().map(|e| format!(" from {}", decompile(&e))).unwrap_or("".into()),
                    condition.to_owned().map(|e| format!(" where {}", decompile(&e))).unwrap_or("".into()),
                    limit.to_owned().map(|e| format!(" limit {}", decompile(&e))).unwrap_or("".into()),
                    group_by.to_owned().map(|items| format!(" group by {}", items.iter().map(|e| decompile(e)).collect::<Vec<String>>().join(", "))).unwrap_or("".into()),
                    having.to_owned().map(|e| format!(" having {}", decompile(&e))).unwrap_or("".into()),
                    order_by.to_owned().map(|e| format!(" order by {}", decompile_list(&e))).unwrap_or("".into()),
            ),
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
    opt.to_owned().map(|x| format!(" limit {}", decompile(&x))).unwrap_or("".into())
}

fn decompile_list(fields: &Vec<Expression>) -> String {
    fields.iter().map(|x| decompile(x)).collect::<Vec<String>>().join(", ".into())
}

fn decompile_opt(opt: &Option<Box<Expression>>) -> String {
    opt.to_owned().map(|i| decompile(&i)).unwrap_or("".into())
}

fn decompile_update_list(fields: &Vec<Expression>, values: &Vec<Expression>) -> String {
    fields.iter().zip(values.iter()).map(|(f, v)|
        format!("{} = {}", decompile(f), decompile(v))).collect::<Vec<String>>().join(", ")
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::expression::*;
    use crate::expression::Expression::*;
    use crate::machine::Machine;
    use crate::numbers::NumberValue::*;
    use crate::typed_values::TypedValue::*;

    #[test]
    fn test_and() {
        let machine = Machine::new();
        let model = And(Box::new(TRUE), Box::new(FALSE));
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, Boolean(false));
        assert_eq!(model.to_code(), "true && false")
    }

    #[test]
    fn test_between() {
        let machine = Machine::new();
        let model = Between(
            Box::new(Literal(Number(Int32Value(10)))),
            Box::new(Literal(Number(Int32Value(1)))),
            Box::new(Literal(Number(Int32Value(10)))));
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "10 between 1 and 10")
    }

    #[test]
    fn test_betwixt() {
        let machine = Machine::new();
        let model = Betwixt(
            Box::new(Literal(Number(Int32Value(10)))),
            Box::new(Literal(Number(Int32Value(1)))),
            Box::new(Literal(Number(Int32Value(10)))),
        );
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, Boolean(false));
        assert_eq!(model.to_code(), "10 betwixt 1 and 10")
    }

    #[test]
    fn test_equality_integers() {
        let machine = Machine::new();
        let model = Equal(
            Box::new(Literal(Number(Int32Value(5)))),
            Box::new(Literal(Number(Int32Value(5)))),
        );
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "5 == 5")
    }

    #[test]
    fn test_equality_floats() {
        let machine = Machine::new();
        let model = Equal(
            Box::new(Literal(Number(Float64Value(5.)))),
            Box::new(Literal(Number(Float64Value(5.)))),
        );
        let (_, result) = machine.evaluate(&model).unwrap();
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
        let (_, result) = machine.evaluate(&model).unwrap();
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
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "\"Hello\" != \"Goodbye\"")
    }

    #[test]
    fn test_gt() {
        let machine = Machine::new()
            .with_variable("x", Number(Int64Value(5)));
        let model = GreaterThan(
            Box::new(Variable("x".into())),
            Box::new(Literal(Number(Int64Value(1)))),
        );
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "x > 1")
    }

    #[test]
    fn test_gte() {
        let machine = Machine::new();
        let model = GreaterOrEqual(
            Box::new(Literal(Number(Int32Value(5)))),
            Box::new(Literal(Number(Int32Value(1)))),
        );
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "5 >= 1")
    }

    #[test]
    fn test_lt() {
        let machine = Machine::new();
        let model = LessThan(
            Box::new(Literal(Number(Int32Value(4)))),
            Box::new(Literal(Number(Int32Value(5)))),
        );
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "4 < 5")
    }

    #[test]
    fn test_lte() {
        let machine = Machine::new();
        let model = LessOrEqual(
            Box::new(Literal(Number(Int32Value(1)))),
            Box::new(Literal(Number(Int32Value(5)))),
        );
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "1 <= 5")
    }

    #[test]
    fn test_ne() {
        let machine = Machine::new();
        let model = NotEqual(
            Box::new(Literal(Number(Int32Value(-5)))),
            Box::new(Literal(Number(Int32Value(5)))));
        let (_, result) = machine.evaluate(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "-5 != 5")
    }

    #[test]
    fn test_or() {
        let machine = Machine::new();
        let model = Or(Box::new(TRUE), Box::new(FALSE));
        let (_, result) = machine.evaluate(&model).unwrap();
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
            Box::new(Literal(Number(Int32Value(1)))),
            Box::new(Literal(Number(Int32Value(10)))),
        );
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
            a: Box::new(Literal(Number(Int32Value(1)))),
            b: Some(Box::new(Literal(Number(Int32Value(10))))),
        };
        assert!(op.is_control_flow());
        assert_eq!(op.to_code(), "if x < y 1 else 10");
    }

    #[test]
    fn test_from() {
        let from = From(Box::new(
            Ns(Box::new(Literal(StringValue("machine.overwrite.stocks".into()))))
        ));
        let from = Inquire(Queryable::Where {
            from: Box::new(from),
            condition: Box::new(GreaterOrEqual(
                Box::new(Variable("last_sale".into())),
                Box::new(Literal(Number(Float64Value(1.25)))),
            )),
        });
        let from = Inquire(Queryable::Limit {
            from: Box::new(from),
            limit: Box::new(Literal(Number(Int64Value(5)))),
        });
        assert_eq!(
            from.to_code(),
            "from ns(\"machine.overwrite.stocks\") where last_sale >= 1.25 limit 5"
        )
    }

    #[test]
    fn test_overwrite() {
        let model = Mutate(Mutation::Overwrite {
            path: Box::new(Variable("stocks".into())),
            source: Box::new(Via(Box::new(JSONLiteral(vec![
                ("symbol".into(), Literal(StringValue("BOX".into()))),
                ("exchange".into(), Literal(StringValue("NYSE".into()))),
                ("last_sale".into(), Literal(Number(Float64Value(21.77)))),
            ])))),
            condition: Some(Box::new(Equal(
                Box::new(Variable("symbol".into())),
                Box::new(Literal(StringValue("BOX".into()))),
            ))),
            limit: Some(Box::new(Literal(Number(Int64Value(1))))),
        });
        assert_eq!(
            model.to_code(),
            r#"overwrite stocks via {symbol: "BOX", exchange: "NYSE", last_sale: 21.77} where symbol == "BOX" limit 1"#)
    }

    #[test]
    fn test_while_is_control_flow() {
        // CodeBlock(..) | If(..) | Return(..) | While { .. }
        let op = While {
            condition: Box::new(LessThan(Box::new(Variable("x".into())), Box::new(Variable("y".into())))),
            code: Box::new(Literal(Number(Int32Value(1)))),
        };
        assert!(op.is_control_flow());
    }

    #[test]
    fn test_is_referential() {
        assert!(Variable("symbol".into()).is_referential());
    }
}