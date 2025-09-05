#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Expression class
////////////////////////////////////////////////////////////////////

use crate::byte_code_compiler::ByteCodeCompiler;
use crate::data_types::DataType;
use crate::expression::Expression::*;
use crate::expression::Ranges::{Exclusive, Inclusive};
use crate::parameter::Parameter;
use crate::type_engine::{TypeEngine, TypeHints};
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::Boolean;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::ops::Deref;

// constants
pub const FALSE: Expression = Condition(Conditions::False);
pub const TRUE: Expression = Condition(Conditions::True);
pub const NULL: Expression = Literal(TypedValue::Null);
pub const UNDEFINED: Expression = Literal(TypedValue::Undefined);

/// Alias Call Types
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum AliasCalls {
    IdentifierCall(String, Box<Expression>),
    FunctionCall(String, Vec<Parameter>, Box<Expression>),
}

impl AliasCalls {
    pub fn get_body(&self) -> &Expression {
        match self {
            AliasCalls::IdentifierCall(_, b) => b,
            AliasCalls::FunctionCall(_, _, b) => b,
        }
    }

    pub fn get_parameters(&self) -> Vec<Parameter> {
        match self {
            AliasCalls::IdentifierCall(..) => vec![],
            AliasCalls::FunctionCall(_, params, _) => params.to_vec(),
        }
    }
}

/// Represents Logical Conditions
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Conditions {
    And(Box<Expression>, Box<Expression>),
    AssumedBoolean(Box<Expression>),
    Equal(Box<Expression>, Box<Expression>),
    False,
    GreaterOrEqual(Box<Expression>, Box<Expression>),
    GreaterThan(Box<Expression>, Box<Expression>),
    In(Box<Expression>, Box<Expression>),
    LessOrEqual(Box<Expression>, Box<Expression>),
    LessThan(Box<Expression>, Box<Expression>),
    Like(Box<Expression>, Box<Expression>),
    Matches(Box<Expression>, Box<Expression>),
    Not(Box<Expression>),
    NotEqual(Box<Expression>, Box<Expression>),
    Or(Box<Expression>, Box<Expression>),
    True,
    When(Box<Expression>, Box<Expression>),
}

impl Conditions {
    pub fn is_pure(&self) -> bool {
        match self {
            Conditions::And(a, b) => is_pure_a_and_b(a, b),
            Conditions::AssumedBoolean(a) => a.is_pure(),
            Conditions::Equal(a, b) => is_pure_a_and_b(a, b),
            Conditions::False => true,
            Conditions::GreaterOrEqual(a, b) => is_pure_a_and_b(a, b),
            Conditions::GreaterThan(a, b) => is_pure_a_and_b(a, b),
            Conditions::In(a, b) => is_pure_a_and_b(a, b),
            Conditions::LessOrEqual(a, b) => is_pure_a_and_b(a, b),
            Conditions::LessThan(a, b) => is_pure_a_and_b(a, b),
            Conditions::Like(a, b) => is_pure_a_and_b(a, b),
            Conditions::Matches(a, b) => is_pure_a_and_b(a, b),
            Conditions::Not(a) => a.is_pure(),
            Conditions::NotEqual(a, b) => is_pure_a_and_b(a, b),
            Conditions::Or(a, b) => is_pure_a_and_b(a, b),
            Conditions::True => true,
            Conditions::When(a, b) => is_pure_a_and_b(a, b),
        }
    }

    /// Returns a string representation of this object
    pub fn to_code(&self) -> String {
        Expression::decompile_cond(self)
    }
}

/// Represents an HTTP Method Call
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum HttpMethodCalls {
    DELETE(Box<Expression>),
    GET(Box<Expression>),
    HEAD(Box<Expression>),
    OPTIONS(Box<Expression>),
    PATCH(Box<Expression>),
    POST(Box<Expression>),
    PUT(Box<Expression>),
    TRACE(Box<Expression>),
}

impl HttpMethodCalls {
    /// Returns the [Option] of a [HttpMethodCalls] instance
    /// ### Parameters
    /// - method: the HTTP method
    /// - config: a URL or configuration object
    /// ### Returns
    /// - the [Option] of a [HttpMethodCalls] instance
    pub fn new(method: &str, config: Expression) -> Option<Self> {
        Some(match method {
            "DELETE" => HttpMethodCalls::DELETE(config.into()),
            "GET" => HttpMethodCalls::GET(config.into()),
            "HEAD" => HttpMethodCalls::HEAD(config.into()),
            "OPTIONS" => HttpMethodCalls::OPTIONS(config.into()),
            "PATCH" => HttpMethodCalls::PATCH(config.into()),
            "POST" => HttpMethodCalls::POST(config.into()),
            "PUT" => HttpMethodCalls::PUT(config.into()),
            "TRACE" => HttpMethodCalls::TRACE(config.into()),
            _ => return None
        })
    }

    /// Returns the HTTP method (e.g. "POST")
    pub fn get_method(&self) -> String {
        match self {
            HttpMethodCalls::DELETE(_) => "DELETE",
            HttpMethodCalls::GET(_) => "GET",
            HttpMethodCalls::HEAD(_) => "HEAD",
            HttpMethodCalls::OPTIONS(_) => "OPTIONS",
            HttpMethodCalls::PATCH(_) => "PATCH",
            HttpMethodCalls::POST(_) => "POST",
            HttpMethodCalls::PUT(_) => "PUT",
            HttpMethodCalls::TRACE(_) => "TRACE",
        }.into()
    }

    /// Returns the URL or configuration structure/object
    pub fn get_url_or_config(&self) -> Expression {
        match self.clone() {
            HttpMethodCalls::DELETE(url_or_object) |
            HttpMethodCalls::GET(url_or_object) |
            HttpMethodCalls::HEAD(url_or_object) |
            HttpMethodCalls::OPTIONS(url_or_object) |
            HttpMethodCalls::PATCH(url_or_object) |
            HttpMethodCalls::POST(url_or_object) |
            HttpMethodCalls::PUT(url_or_object) |
            HttpMethodCalls::TRACE(url_or_object) => *url_or_object
        }
    }

    pub fn is_header_only(&self) -> bool {
        matches!(self, HttpMethodCalls::HEAD(..))
    }
}

/// Observables
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Observations {
    VariableObservation { condition: Conditions, code: Expression },
}

/// Represents an enumeration of range variations
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Ranges {
    Inclusive(Box<Expression>, Box<Expression>),
    Exclusive(Box<Expression>, Box<Expression>),
}

impl Ranges {
    pub fn is_pure(&self) -> bool {
        match self {
            Inclusive(a, b) => is_pure_a_and_b(a, b),
            Exclusive(a, b) => is_pure_a_and_b(a, b),
        }
    }
}

/// Represents an enumeration of use definition variations
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum UseOps {
    Everything(String),
    Selection(String, Vec<String>),
}

impl UseOps {
    pub fn to_code(&self) -> String {
        match self {
            UseOps::Everything(pkg) => pkg.to_string(),
            UseOps::Selection(pkg, items) =>
                format!("{pkg}::{}", items.join(", "))
        }
    }
}

impl Display for UseOps {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_code())
    }
}

/// Represents an enumeration of Expression variations
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Expression {
    Alias(AliasCalls),
    Aliases,
    ArrayExpression(Vec<Expression>),
    ArrowCurvyLeft(Box<Expression>, Box<Expression>),
    ArrowCurvyLeft2x(Box<Expression>, Box<Expression>),
    ArrowCurvyRight(Box<Expression>, Box<Expression>),
    ArrowCurvyRight2x(Box<Expression>, Box<Expression>),
    ArrowFat(Box<Expression>, Box<Expression>),
    ArrowSkinnyRight(Box<Expression>, Box<Expression>),
    ArrowSkinnyLeft(Box<Expression>, Box<Expression>),
    ArrowVerticalBar(Box<Expression>, Box<Expression>),
    ArrowVerticalBar2x(Box<Expression>, Box<Expression>),
    As(Box<Expression>, Box<Expression>),
    Assert { condition: Box<Expression>, message: Option<Box<Expression>> },
    BitwiseAnd(Box<Expression>, Box<Expression>),
    BitwiseOr(Box<Expression>, Box<Expression>),
    BitwiseShiftLeft(Box<Expression>, Box<Expression>),
    BitwiseShiftRight(Box<Expression>, Box<Expression>),
    BitwiseXor(Box<Expression>, Box<Expression>),
    Coalesce(Box<Expression>, Box<Expression>),
    CoalesceErr(Box<Expression>, Box<Expression>),
    CodeBlock(Vec<Expression>),
    ColonColon(Box<Expression>, Box<Expression>),
    ColonColonColon(Box<Expression>, Box<Expression>),
    ColonColonCapture(Box<Expression>, Box<Expression>),
    ColonColonColonCapture(Box<Expression>, Box<Expression>),
    Condition(Conditions),
    Divide(Box<Expression>, Box<Expression>),
    DoWhile {
        condition: Box<Expression>,
        code: Box<Expression>,
    },
    ElementAt(Box<Expression>, Box<Expression>),
    Feature { title: Box<Expression>, scenarios: Vec<Expression> },
    For { construct: Box<Expression>, code: Box<Expression> },
    FunctionCall { fx: Box<Expression>, args: Vec<Expression> },
    HTTP(HttpMethodCalls),
    Identifier(String),
    If {
        condition: Box<Expression>,
        a: Box<Expression>,
        b: Option<Box<Expression>>,
    },
    Include(Box<Expression>),
    Infix(Box<Expression>, Box<Expression>),
    IsDefined(Box<Expression>),
    Literal(TypedValue),
    Ls(Option<Box<Expression>>),
    MatchExpression(Box<Expression>, Vec<Expression>),
    Minus(Box<Expression>, Box<Expression>),
    Module(String, Vec<Expression>),
    Modulo(Box<Expression>, Box<Expression>),
    Multiply(Box<Expression>, Box<Expression>),
    NamedValue(String, Box<Expression>),
    Neg(Box<Expression>),
    OneTime(Box<Expression>, u128),
    Parameters(Vec<Parameter>),
    Plus(Box<Expression>, Box<Expression>),
    PlusPlus(Box<Expression>, Box<Expression>),
    Pow(Box<Expression>, Box<Expression>),
    Range(Ranges),
    Referenced(Box<Expression>),
    Return(Box<Expression>),
    Scenario {
        title: Box<Expression>,
        inherits: Option<String>,
        verifications: Vec<Expression>,
    },
    SetVariables(Box<Expression>, Box<Expression>),
    SetVariablesExpr(Box<Expression>, Box<Expression>),
    StructureExpression(Vec<(String, Expression)>),
    Test(Box<Expression>),
    Throw(Box<Expression>),
    TupleExpression(Vec<Expression>),
    Use(Vec<UseOps>),
    WhenEver {
        condition: Box<Expression>,
        code: Box<Expression>,
    },
    While {
        condition: Box<Expression>,
        code: Box<Expression>,
    },
    With {
        resource: Box<Expression>,
        code: Box<Expression>,
    },
    Yield(Box<Expression>),
    Zip(Box<Expression>, Box<Expression>),
    ////////////////////////////////////////////////////////////////////
    // SQL models
    ////////////////////////////////////////////////////////////////////
    Delete { from: Box<Expression> },
    Deselect { from: Box<Expression>, fields: Vec<Expression> },
    Fetch { from: Box<Expression>, fields: Vec<Expression> },
    GroupBy { from: Box<Expression>, columns: Vec<Expression> },
    Having { from: Box<Expression>, condition: Conditions },
    Limit { from: Box<Expression>, limit: Box<Expression> },
    OrderBy { from: Box<Expression>, columns: Vec<Expression> },
    Select { from: Box<Expression>, fields: Vec<Expression> },
    Undelete { from: Box<Expression> },
    Where { from: Box<Expression>, condition: Conditions },
}

impl Expression {

    ////////////////////////////////////////////////////////////////
    // instance methods
    ////////////////////////////////////////////////////////////////
    
    pub fn decompile(expr: &Expression) -> String {
        match expr {
            Alias(AliasCalls::FunctionCall(name, params, b)) =>
                format!("alias {}({}) = {}", name, Parameter::render(params), Self::decompile(b)),
            Alias(AliasCalls::IdentifierCall(name, b)) =>
                format!("alias {} = {}", name, Self::decompile(b)),
            Aliases => "aliases".to_string(),
            ArrayExpression(items) =>
                format!("[{}]", items.iter().map(|i| Self::decompile(i)).collect::<Vec<String>>().join(", ")),
            ArrowCurvyLeft(a, b) =>
                format!("{} <~ {}", Self::decompile(a), Self::decompile(b)),
            ArrowCurvyLeft2x(a, b) =>
                format!("{} <<~ {}", Self::decompile(a), Self::decompile(b)),
            ArrowCurvyRight(a, b) =>
                format!("{} ~> {}", Self::decompile(a), Self::decompile(b)),
            ArrowCurvyRight2x(a, b) =>
                format!("{} ~>> {}", Self::decompile(a), Self::decompile(b)),
            ArrowFat(a, b) =>
                format!("{} => {}", Self::decompile(a), Self::decompile(b)),
            ArrowSkinnyLeft(a, b) =>
                format!("{} <- {}", Self::decompile(a), Self::decompile(b)),
            ArrowSkinnyRight(a, b) =>
                format!("{} -> {}", Self::decompile(a), Self::decompile(b)),
            ArrowVerticalBar(a, b) =>
                format!("{} |> {}", Self::decompile(a), Self::decompile(b)),
            ArrowVerticalBar2x(a, b) =>
                format!("{} |>> {}", Self::decompile(a), Self::decompile(b)),
            As(a, b) =>
                format!("{} as {}", Self::decompile(a), Self::decompile(b)),
            Assert { condition, message } =>
                match message {
                    Some(msg) => format!("assert {}, {} ", Self::decompile(condition), Self::decompile(msg)),
                    None => format!("assert {} ", Self::decompile(condition)),
                }
            BitwiseAnd(a, b) =>
                format!("{} & {}", Self::decompile(a), Self::decompile(b)),
            BitwiseOr(a, b) =>
                format!("{} | {}", Self::decompile(a), Self::decompile(b)),
            BitwiseXor(a, b) =>
                format!("{} ^ {}", Self::decompile(a), Self::decompile(b)),
            BitwiseShiftLeft(a, b) =>
                format!("{} << {}", Self::decompile(a), Self::decompile(b)),
            BitwiseShiftRight(a, b) =>
                format!("{} >> {}", Self::decompile(a), Self::decompile(b)),
            Coalesce(a, b) =>
                format!("{} ? {}", Self::decompile(a), Self::decompile(b)),
            CoalesceErr(a, b) =>
                format!("{} !? {}", Self::decompile(a), Self::decompile(b)),
            CodeBlock(items) => Self::decompile_code_blocks(items),
            ColonColon(a, b) =>
                format!("{}::{}", Self::decompile(a), Self::decompile(b)),
            ColonColonCapture(a, b) =>
                format!("{}<::{}", Self::decompile(a), Self::decompile(b)),
            ColonColonColon(a, b) =>
                format!("{}:::{}", Self::decompile(a), Self::decompile(b)),
            ColonColonColonCapture(a, b) =>
                format!("{}<:::{}", Self::decompile(a), Self::decompile(b)),
            Condition(cond) => Self::decompile_cond(cond),
            Divide(a, b) =>
                format!("{} / {}", Self::decompile(a), Self::decompile(b)),
            DoWhile { condition, code } => 
                format!("do {} while {}", Self::decompile(condition), Self::decompile(code)),
            ElementAt(a, b) =>
                format!("{}[{}]", Self::decompile(a), Self::decompile(b)),
            Feature { title, scenarios } =>
                format!("feature {} {{\n{}\n}}", title.to_code(), scenarios.iter()
                    .map(|s| s.to_code())
                    .collect::<Vec<_>>()
                    .join("\n")),
            For { construct, code } =>
                format!("for {} {}", Self::decompile(construct), Self::decompile(code)),
            FunctionCall { fx, args } =>
                format!("{}({})", Self::decompile(fx), Self::decompile_list(args)),
            HTTP(method) =>
                format!("{} {}", method.get_method(), Self::decompile(&method.get_url_or_config())),
            Identifier(name) => name.to_string(),
            If { condition, a, b } =>
                format!("if {} {}{}", Self::decompile(condition), Self::decompile(a), b.to_owned()
                    .map(|x| format!(" else {}", Self::decompile(&x)))
                    .unwrap_or("".into())),
            Include(path) => format!("include {}", Self::decompile(path)),
            Infix(a, b) =>
                format!("{}.{}", Self::decompile(a), Self::decompile(b)),
            IsDefined(a) => format!("is_defined({})", Self::decompile(a)),
            Literal(value) => value.to_code(),
            Ls(maybe_path) => match maybe_path {
                None => "ls".into(),
                Some(path) => format!("ls({})", Self::decompile(path))
            }
            MatchExpression(a, b) =>
                format!("match {} {}", Self::decompile(a), Self::decompile(&ArrayExpression(b.clone()))),
            Minus(a, b) =>
                format!("{} - {}", Self::decompile(a), Self::decompile(b)),
            Module(name, ops) =>
                format!("{} {}", name, Self::decompile_code_blocks(ops)),
            Modulo(a, b) =>
                format!("{} % {}", Self::decompile(a), Self::decompile(b)),
            Multiply(a, b) =>
                format!("{} * {}", Self::decompile(a), Self::decompile(b)),
            NamedValue(name, expr) =>
                format!("{}: {}", name, Self::decompile(expr)),
            Neg(a) => format!("-({})", Self::decompile(a)),
            OneTime(expr, ..) => format!("once {}", Self::decompile(expr)),
            Parameters(parameters) => Self::decompile_parameters(parameters),
            Plus(a, b) =>
                format!("{} + {}", Self::decompile(a), Self::decompile(b)),
            PlusPlus(a, b) =>
                format!("{} ++ {}", Self::decompile(a), Self::decompile(b)),
            Pow(a, b) =>
                format!("{} ** {}", Self::decompile(a), Self::decompile(b)),
            Range(Exclusive(a, b)) =>
                format!("{}..{}", Self::decompile(a), Self::decompile(b)),
            Range(Inclusive(a, b)) =>
                format!("{}..={}", Self::decompile(a), Self::decompile(b)),
            Referenced(a) =>
                format!("&{}", Self::decompile(a)),
            Return(a) =>
                format!("return {}", Self::decompile(a)),
            Scenario { title, inherits, verifications } => {
                let title = title.to_code();
                let inherits = inherits.clone()
                    .map(|name| format!(" inherits \"{name}\""))
                    .unwrap_or("".into());
                let verifications = verifications.iter()
                    .map(|s| s.to_code())
                    .collect::<Vec<_>>()
                    .join("\n");
                format!("scenario {title}{inherits} {{\n{verifications}\n}}")
            }
            SetVariables(vars, values) =>
                format!("{} = {}", Self::decompile(vars), Self::decompile(values)),
            SetVariablesExpr(vars, values) =>
                format!("{} := {}", Self::decompile(vars), Self::decompile(values)),
            StructureExpression(items) =>
                format!("{{{}}}", items.iter()
                    .map(|(k, v)| format!("{}: {}", k, v))
                    .collect::<Vec<String>>()
                    .join(", ")),
            Test(expr) => format!("test{}", match expr.deref() {
                Condition(cond) => format!(" where {}", Self::decompile_cond(cond)),
                Literal(TypedValue::Null | TypedValue::Undefined) => "".to_string(),
                other => format!(" {}", Self::decompile(other)),
            }),
            Throw(message) => format!("throw({})", Self::decompile(message)),
            TupleExpression(args) => format!("({})", Self::decompile_list(args)),
            Use(args) =>
                format!("use {}", args.iter().map(|a| a.to_code())
                    .collect::<Vec<_>>()
                    .join(", ")),
            WhenEver { condition, code } => 
                format!("when {} {}", Self::decompile(condition), Self::decompile(code)),
            While { condition, code } =>
                format!("while {} {}", Self::decompile(condition), Self::decompile(code)),
            With { resource, code } =>
                format!("{} with {}", Self::decompile(resource), Self::decompile(code)),
            Yield(expr) =>
                format!("yield {}", Self::decompile(expr)),
            Zip(a, b) =>
                format!("{} <|> {}", Self::decompile(a), Self::decompile(b)),
            ////////////////////////////////////////////////////////////////////
            // SQL models
            ////////////////////////////////////////////////////////////////////
            Delete { from } =>
                format!("delete {}", Self::decompile(from)),
            Deselect { fields, from } =>
                format!("deselect {} from {}", Self::decompile_list(fields), Self::decompile(from)),
            Fetch { fields, from } =>
                format!("fetch {} from {}", Self::decompile_list(fields), Self::decompile(from)),
            GroupBy { from, columns } =>
                format!("{} group_by {}", Self::decompile(from), Self::decompile_list(columns)),
            Having { from, condition } =>
                format!("{} having {}", Self::decompile(from), Self::decompile_cond(condition)),
            Limit { from, limit } =>
                format!("{} limit {}", Self::decompile(from), Self::decompile(limit)),
            OrderBy { from, columns } =>
                format!("{} order_by {}", Self::decompile(from), Self::decompile_list(columns)),
            Select { fields, from } =>
                format!("select {} from {}", Self::decompile_list(fields), Self::decompile(from)),
            Undelete { from } =>
                format!("undelete {}", Self::decompile(from)),
            Where { from, condition } =>
                format!("{} where {}", Self::decompile(from), Self::decompile_cond(condition)),
        }
    }

    pub fn decompile_code_blocks(ops: &[Expression]) -> String {
        format!("{{\n{}\n}}", ops.iter().map(|i| Self::decompile(i))
            .collect::<Vec<String>>()
            .join("\n"))
    }

    pub fn decompile_cond(cond: &Conditions) -> String {
        match cond {
            Conditions::And(a, b) =>
                format!("{} && {}", Self::decompile(a), Self::decompile(b)),
            Conditions::AssumedBoolean(a) =>
                format!("Boolean({})", Self::decompile(a)),
            Conditions::Equal(a, b) =>
                format!("{} == {}", Self::decompile(a), Self::decompile(b)),
            Conditions::False => "false".to_string(),
            Conditions::When(a, b) =>
                format!("{} when {}", Self::decompile(a), Self::decompile(b)),
            Conditions::GreaterThan(a, b) =>
                format!("{} > {}", Self::decompile(a), Self::decompile(b)),
            Conditions::GreaterOrEqual(a, b) =>
                format!("{} >= {}", Self::decompile(a), Self::decompile(b)),
            Conditions::In(a, b) =>
                format!("{} in {}", Self::decompile(a), Self::decompile(b)),
            Conditions::LessThan(a, b) =>
                format!("{} < {}", Self::decompile(a), Self::decompile(b)),
            Conditions::LessOrEqual(a, b) =>
                format!("{} <= {}", Self::decompile(a), Self::decompile(b)),
            Conditions::Like(a, b) =>
                format!("{} like {}", Self::decompile(a), Self::decompile(b)),
            Conditions::Matches(a, b) =>
                format!("{} matches {}", Self::decompile(a), Self::decompile(b)),
            Conditions::Not(a) => format!("!({})", Self::decompile(a)),
            Conditions::NotEqual(a, b) =>
                format!("{} != {}", Self::decompile(a), Self::decompile(b)),
            Conditions::Or(a, b) =>
                format!("{} || {}", Self::decompile(a), Self::decompile(b)),
            Conditions::True => "true".to_string(),
        }
    }

    pub fn decompile_parameters(params: &Vec<Parameter>) -> String {
        params.iter().map(|p| p.to_code())
            .collect::<Vec<_>>()
            .join(", ")
    }

    pub fn decompile_list(fields: &[Expression]) -> String {
        fields.iter().map(|x| Self::decompile(x)).collect::<Vec<String>>().join(", ".into())
    }
    
    pub fn encode(&self) -> Vec<u8> {
        ByteCodeCompiler::encode(&self).unwrap_or_else(|e| panic!("{}", e))
    }

    pub fn infer_type(&self) -> DataType {
        TypeEngine::infer(TypeHints::new(), self).1
    }

    /// Indicates whether the expression is a conditional expression
    pub fn is_conditional(&self) -> bool {
        matches!(self, Condition(..))
    }

    /// Indicates whether the expression is a control flow expression
    pub fn is_control_flow(&self) -> bool {
        matches!(self, CodeBlock(..) | If { .. } | Return(..) | While { .. })
    }

    pub fn is_pure(&self) -> bool {
        match self {
            Alias(AliasCalls::FunctionCall(_, _, b)) => b.is_pure(),
            Alias(AliasCalls::IdentifierCall(_, b)) => b.is_pure(),
            Aliases => true,
            ArrayExpression(items) => is_pure_all(items),
            ArrowCurvyLeft(_, _) => false,
            ArrowCurvyLeft2x(_, _) => false,
            ArrowCurvyRight(_, _) => false,
            ArrowCurvyRight2x(_, _) => false,
            ArrowFat(_, _) => false,
            ArrowSkinnyRight(_, _) => false,
            ArrowSkinnyLeft(_, _) => false,
            ArrowVerticalBar(_, _) => false,
            ArrowVerticalBar2x(_, _) => false,
            As(a, _) => a.is_pure(),
            Assert { condition, message } => condition.is_pure() && is_pure_opt(message),
            BitwiseAnd(a, b) => is_pure_a_and_b(a, b),
            BitwiseOr(a, b) => is_pure_a_and_b(a, b),
            BitwiseShiftLeft(a, b) => is_pure_a_and_b(a, b),
            BitwiseShiftRight(a, b) => is_pure_a_and_b(a, b),
            BitwiseXor(a, b) => is_pure_a_and_b(a, b),
            Coalesce(a, b) => is_pure_a_and_b(a, b),
            CoalesceErr(a, b) => is_pure_a_and_b(a, b),
            CodeBlock(items) => is_pure_all(items),
            ColonColon(_, _) => false,
            ColonColonCapture(_, _) => false,
            ColonColonColon(_, _) => false,
            ColonColonColonCapture(_, _) => false,
            Condition(c) => c.is_pure(),
            Divide(a, b) => is_pure_a_and_b(a, b),
            DoWhile { condition, code } => is_pure_a_and_b(condition, code),
            ElementAt(a, b) => is_pure_a_and_b(a, b),
            Feature { .. } => false,
            For { construct, code } => is_pure_a_and_b(construct, code),
            FunctionCall { fx, args } => fx.is_pure() && is_pure_all(args),
            HTTP(_) => false,
            Identifier(_) => false,
            If { condition, a, b } =>
                is_pure_a_and_b(condition, a) && is_pure_opt(b),
            Include(_) => false,
            Infix(_, _) => false,
            IsDefined(a) => a.is_pure(),
            Literal(v) => v.is_pure(),
            Ls(_) => false,
            MatchExpression(a, b) => a.is_pure() && is_pure_all(b),
            Minus(a, b) => is_pure_a_and_b(a, b),
            Module(_, items) => is_pure_all(items),
            Modulo(a, b) => is_pure_a_and_b(a, b),
            Multiply(a, b) => is_pure_a_and_b(a, b),
            NamedValue(_, b) => b.is_pure(),
            Neg(a) => a.is_pure(),
            OneTime(a, ..) => a.is_pure(),
            Parameters(p) => p.iter().all(|p| p.get_default_value().is_pure()),
            Plus(a, b) => is_pure_a_and_b(a, b),
            PlusPlus(a, b) => is_pure_a_and_b(a, b),
            Pow(a, b) => is_pure_a_and_b(a, b),
            Range(r) => r.is_pure(),
            Referenced(a) => a.is_pure(),
            Return(a) => a.is_pure(),
            Scenario { .. } => false,
            SetVariables(_, b) => b.is_pure(),
            SetVariablesExpr(_, b) => b.is_pure(),
            StructureExpression(pairs) => pairs.iter().all(|(_, e)| e.is_pure()),
            Test(_) => false,
            Throw(a) => a.is_pure(),
            TupleExpression(items) => is_pure_all(items),
            Use(_) => false,
            WhenEver { .. } => false,
            While { condition, code } => is_pure_a_and_b(condition, code),
            With { resource, code } => resource.is_pure() && code.is_pure(),
            Yield(a) => a.is_pure(),
            Zip(a, b) => is_pure_a_and_b(a, b),
            // SQL models
            Delete { from } => from.is_pure(),
            Deselect { from, fields } => from.is_pure() && is_pure_all(fields),
            Fetch { from, fields } => from.is_pure() && is_pure_all(fields),
            GroupBy { from, .. } => from.is_pure(),
            Having { from, condition, .. } => from.is_pure() && condition.is_pure(),
            Limit { from, limit } => is_pure_a_and_b(from, limit),
            OrderBy { from, .. } => from.is_pure(),
            Select { from, fields } => from.is_pure() && is_pure_all(fields),
            Undelete { from } => from.is_pure(),
            Where { from, condition } => from.is_pure() && condition.is_pure(),
        }
    }

    /// Indicates whether the expression is a referential expression
    pub fn is_referential(&self) -> bool {
        matches!(self, Identifier(..))
    }

    /// Returns a string representation of this object
    pub fn to_code(&self) -> String {
        Self::decompile(self)
    }
    
}

impl Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_code())
    }
}

/// expression unit tests
#[cfg(test)]
mod expression_tests {
    use crate::expression::Conditions::*;
    use crate::machine::Machine;
    use crate::numbers::Numbers::I64Value;
    use crate::numbers::Numbers::*;
    use crate::typed_values::TypedValue::{Number, StringValue};

    use super::*;

    #[test]
    fn test_conditional_and() {
        let machine = Machine::empty();
        let model = And(Box::new(TRUE), Box::new(FALSE));
        let (_, result) = machine.evaluate_condition(&model).unwrap();
        assert_eq!(result, Boolean(false));
        assert_eq!(model.to_code(), "true && false")
    }

    #[test]
    fn test_in_range_exclusive_expression() {
        let machine = Machine::empty();
        let model = In(
            Literal(Number(I64Value(10))).into(),
            Range(Exclusive(
                Literal(Number(I64Value(1))).into(),
                Literal(Number(I64Value(10))).into()
            )).into(),
        );
        let (_, result) = machine.evaluate_condition(&model).unwrap();
        assert_eq!(result, Boolean(false));
        assert_eq!(model.to_code(), "10 in 1..10")
    }

    #[test]
    fn test_in_range_inclusive_expression() {
        let machine = Machine::empty();
        let model = In(
            Literal(Number(I64Value(10))).into(),
            Range(Inclusive(
                Literal(Number(I64Value(1))).into(),
                Literal(Number(I64Value(10))).into()
            )).into(),
        );
        let (_, result) = machine.evaluate_condition(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "10 in 1..=10")
    }

    #[test]
    fn test_equality_integers() {
        let machine = Machine::empty();
        let model = Equal(
            Box::new(Literal(Number(I64Value(5)))),
            Box::new(Literal(Number(I64Value(5)))),
        );
        let (_, result) = machine.evaluate_condition(&model).unwrap();
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
        let (_, result) = machine.evaluate_condition(&model).unwrap();
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
        let (_, result) = machine.evaluate_condition(&model).unwrap();
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
        let (_, result) = machine.evaluate_condition(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "\"Hello\" != \"Goodbye\"")
    }

    #[test]
    fn test_greater_than() {
        let machine = Machine::empty()
            .with_variable("x", Number(I64Value(5)));
        let model = GreaterThan(
            Box::new(Identifier("x".into())),
            Box::new(Literal(Number(I64Value(1)))),
        );
        let (_, result) = machine.evaluate_condition(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "x > 1")
    }

    #[test]
    fn test_greater_than_or_equal() {
        let machine = Machine::empty();
        let model = GreaterOrEqual(
            Box::new(Literal(Number(I64Value(5)))),
            Box::new(Literal(Number(I64Value(1)))),
        );
        let (_, result) = machine.evaluate_condition(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "5 >= 1")
    }

    #[test]
    fn test_less_than() {
        let machine = Machine::empty();
        let model = LessThan(
            Box::new(Literal(Number(I64Value(4)))),
            Box::new(Literal(Number(I64Value(5)))),
        );
        let (_, result) = machine.evaluate_condition(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "4 < 5")
    }

    #[test]
    fn test_less_than_or_equal() {
        let machine = Machine::empty();
        let model = LessOrEqual(
            Box::new(Literal(Number(I64Value(1)))),
            Box::new(Literal(Number(I64Value(5)))),
        );
        let (_, result) = machine.evaluate_condition(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "1 <= 5")
    }

    #[test]
    fn test_not_equal() {
        let machine = Machine::empty();
        let model = NotEqual(
            Box::new(Literal(Number(I64Value(-5)))),
            Box::new(Literal(Number(I64Value(5)))),
        );
        let (_, result) = machine.evaluate_condition(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "-5 != 5")
    }

    #[test]
    fn test_conditional_or() {
        let machine = Machine::empty();
        let model = Conditions::Or(Box::new(TRUE), Box::new(FALSE));
        let (_, result) = machine.evaluate_condition(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "true || false")
    }

    #[test]
    fn test_is_conditional() {
        // x and y
        let model = Condition(Conditions::And(Box::new(TRUE), Box::new(FALSE)));
        assert_eq!(model.to_code(), "true && false");
        assert!(model.is_conditional());
        
        // x in y..=z
        let model = Condition(In(
            Identifier("x".into()).into(),
            Range(Inclusive(
                Literal(Number(I64Value(1))).into(),
                Literal(Number(I64Value(10))).into(),
            )).into()
        ));
        assert_eq!(model.to_code(), "x in 1..=10");
        assert!(model.is_conditional());

        // x or y
        let model = Condition(Conditions::Or(Box::new(TRUE), Box::new(FALSE)));
        assert_eq!(model.to_code(), "true || false");
        assert!(model.is_conditional());
    }

    #[test]
    fn test_if_is_control_flow() {
        let op = If {
            condition: Box::new(Condition(LessThan(
                Box::new(Identifier("x".into())),
                Box::new(Identifier("y".into())),
            ))),
            a: Box::new(Literal(Number(I64Value(1)))),
            b: Some(Box::new(Literal(Number(I64Value(10))))),
        };
        assert!(op.is_control_flow());
        assert_eq!(op.to_code(), "if x < y 1 else 10");
    }

    #[test]
    fn test_while_is_control_flow() {
        // CodeBlock(..) | If(..) | Return(..) | While { .. }
        let op = While {
            condition: Box::new(Condition(LessThan(
                Box::new(Identifier("x".into())),
                Box::new(Identifier("y".into())))
            )),
            code: Box::new(Literal(Number(I64Value(1)))),
        };
        assert!(op.is_control_flow());
    }

    #[test]
    fn test_is_referential() {
        assert!(Identifier("symbol".into()).is_referential());
    }

    #[test]
    fn test_alias() {
        let model = NamedValue("symbol".into(), Box::new(Literal(StringValue("ABC".into()))));
        assert_eq!(Expression::decompile(&model), r#"symbol: "ABC""#);
    }

    #[test]
    fn test_array_declaration() {
        let model = ArrayExpression(vec![
            Literal(Number(I64Value(2))), Literal(Number(I64Value(5))), Literal(Number(I64Value(8))),
            Literal(Number(I64Value(7))), Literal(Number(I64Value(4))), Literal(Number(I64Value(1))),
        ]);
        assert_eq!(Expression::decompile(&model), "[2, 5, 8, 7, 4, 1]")
    }

    #[test]
    fn test_array_indexing() {
        let model = ElementAt(
            Box::new(ArrayExpression(vec![
                Literal(Number(I64Value(7))), Literal(Number(I64Value(5))), Literal(Number(I64Value(8))),
                Literal(Number(I64Value(2))), Literal(Number(I64Value(4))), Literal(Number(I64Value(1))),
            ])),
            Box::new(Literal(Number(I64Value(3)))));
        assert_eq!(Expression::decompile(&model), "[7, 5, 8, 2, 4, 1][3]")
    }

    #[test]
    fn test_bitwise_and() {
        let model = BitwiseAnd(
            Box::new(Literal(Number(I64Value(20)))),
            Box::new(Literal(Number(I64Value(3)))),
        );
        assert_eq!(to_pure(&model), Number(I64Value(0)));
        assert_eq!(Expression::decompile(&model), "20 & 3")
    }

    #[test]
    fn test_bitwise_or() {
        let model = BitwiseOr(
            Box::new(Literal(Number(I64Value(20)))),
            Box::new(Literal(Number(I64Value(3)))),
        );
        assert_eq!(to_pure(&model), Number(I64Value(23)));
        assert_eq!(Expression::decompile(&model), "20 | 3")
    }

    #[test]
    fn test_bitwise_shl() {
        let model = BitwiseShiftLeft(
            Box::new(Literal(Number(I64Value(20)))),
            Box::new(Literal(Number(I64Value(3)))),
        );
        assert_eq!(to_pure(&model), Number(I64Value(160)));
        assert_eq!(Expression::decompile(&model), "20 << 3")
    }

    #[test]
    fn test_bitwise_shr() {
        let model = BitwiseShiftRight(
            Box::new(Literal(Number(I64Value(20)))),
            Box::new(Literal(Number(I64Value(3)))),
        );
        assert_eq!(to_pure(&model), Number(I64Value(2)));
        assert_eq!(Expression::decompile(&model), "20 >> 3")
    }

    #[test]
    fn test_bitwise_xor() {
        let model = BitwiseXor(
            Box::new(Literal(Number(I64Value(20)))),
            Box::new(Literal(Number(I64Value(3)))),
        );
        assert_eq!(to_pure(&model), Number(I64Value(23)));
        assert_eq!(Expression::decompile(&model), "20 ^ 3")
    }

    #[test]
    fn test_function_call() {
        let model = FunctionCall {
            fx: Box::new(Identifier("f".into())),
            args: vec![
                Literal(Number(I64Value(2))),
                Literal(Number(I64Value(3))),
            ],
        };
        assert_eq!(Expression::decompile(&model), "f(2, 3)")
    }

    fn to_pure(expr: &Expression) -> TypedValue {
        Machine::evaluate_pure(expr).unwrap().1
    }
}

/// pure expression unit tests
#[cfg(test)]
mod pure_expression_tests {
    use crate::compiler::Compiler;
    use crate::machine::Machine;
    use crate::numbers::Numbers::{F64Value, I64Value};
    use crate::sequences::Array;
    use crate::typed_values::TypedValue;
    use crate::typed_values::TypedValue::{ArrayValue, Boolean, Number};

    #[test]
    fn test_to_pure_array() {
        verify_pure(
            "[1, 2, 3, 4] * 2",
            ArrayValue(Array::from(vec![
                Number(I64Value(2)), Number(I64Value(4)),
                Number(I64Value(6)), Number(I64Value(8)),
            ])))
    }

    #[test]
    fn test_to_pure_as_value() {
        verify_pure("x: 55", Number(I64Value(55)))
    }

    #[test]
    fn test_to_pure_bitwise_and() {
        verify_pure("0b1011 & 0b1101", Number(I64Value(9)))
    }

    #[test]
    fn test_to_pure_bitwise_or() {
        verify_pure("0b0110 | 0b0011", Number(I64Value(7)))
    }

    #[test]
    fn test_to_pure_bitwise_shl() {
        verify_pure("0b0001 << 0x03", Number(I64Value(8)))
    }

    #[test]
    fn test_to_pure_bitwise_shr() {
        verify_pure("0b1_000_000 >> 0b0010", Number(I64Value(16)))
    }

    #[test]
    fn test_to_pure_bitwise_xor() {
        verify_pure("0b0110 ^ 0b0011", Number(I64Value(5))) // 0b0101
    }

    #[test]
    fn test_to_pure_conditional_false() {
        verify_pure("false", Boolean(false))
    }

    #[test]
    fn test_to_pure_conditional_true() {
        verify_pure("true", Boolean(true))
    }

    #[test]
    fn test_to_pure_conditional_and() {
        verify_pure("true && false", Boolean(false))
    }

    #[test]
    fn test_to_pure_conditional_or() {
        verify_pure("true || false", Boolean(true))
    }

    #[test]
    fn test_to_pure_math_add() {
        verify_pure("237 + 91", Number(I64Value(328)))
    }

    #[test]
    fn test_to_pure_math_divide() {
        verify_pure("16 / 3", Number(I64Value(5)))
    }

    #[test]
    fn test_to_pure_math_multiply() {
        verify_pure("81 * 33", Number(I64Value(2673)))
    }

    #[test]
    fn test_to_pure_math_neg() {
        verify_pure("-(40 + 41)", Number(I64Value(-81)))
    }

    #[test]
    fn test_to_pure_math_power() {
        verify_pure("5 ** 3", Number(F64Value(125.0)))
    }

    #[test]
    fn test_to_pure_math_subtract() {
        verify_pure("237 - 91", Number(I64Value(146)))
    }

    fn verify_pure(code: &str, expected: TypedValue) {
        let expr = Compiler::build(code).unwrap();
        let (_, actual) = Machine::evaluate_pure(&expr).unwrap();
        assert_eq!(actual, expected)
    }
}

fn is_pure_a_and_b(a: &Expression, b: &Expression) -> bool {
    a.is_pure() && b.is_pure()
}

fn is_pure_all(items: &[Expression]) -> bool {
    items.iter().all(|item| item.is_pure())
}

fn is_pure_opt(maybe_expr: &Option<Box<Expression>>) -> bool {
    maybe_expr.is_none() || maybe_expr.iter().all(|e| e.is_pure())
}
