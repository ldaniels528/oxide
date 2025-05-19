#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Expression class
////////////////////////////////////////////////////////////////////

use crate::byte_code_compiler::ByteCodeCompiler;
use crate::data_types::DataType;
use crate::data_types::DataType::*;
use crate::errors::Errors::{SyntaxError, TypeMismatch};
use crate::errors::TypeMismatchErrors::{ConstantValueExpected, UnsupportedType};
use crate::errors::{throw, SyntaxErrors};
use crate::expression::Expression::*;
use crate::expression::Ranges::{Exclusive, Inclusive};
use crate::machine;
use crate::number_kind::NumberKind;
use crate::number_kind::NumberKind::I64Kind;
use crate::numbers::Numbers;
use crate::numbers::Numbers::I64Value;
use crate::parameter::Parameter;
use crate::platform::{Package, PackageOps};
use crate::row_collection::RowCollection;
use crate::sequences::{Array, Sequence};
use crate::structures::Structures::{Firm, Soft};
use crate::structures::{SoftStructure, Structure};
use crate::tokens::Token;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{ArrayValue, Boolean, ErrorValue, Function, Number, PlatformOp, StringValue, Structured, Undefined};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::ops::Deref;

// constants
pub const FALSE: Expression = Condition(Conditions::False);
pub const TRUE: Expression = Condition(Conditions::True);
pub const NULL: Expression = Literal(TypedValue::Null);
pub const UNDEFINED: Expression = Literal(TypedValue::Undefined);

/// Represents Logical Conditions
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Conditions {
    And(Box<Expression>, Box<Expression>),
    Contains(Box<Expression>, Box<Expression>),
    Equal(Box<Expression>, Box<Expression>),
    False,
    GreaterOrEqual(Box<Expression>, Box<Expression>),
    GreaterThan(Box<Expression>, Box<Expression>),
    In(Box<Expression>, Box<Expression>),
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
        Expression::decompile_cond(self)
    }
}

/// Represents the set of all Directives
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Directives {
    MustAck(Box<Expression>),
    MustDie(Box<Expression>),
    MustIgnoreAck(Box<Expression>),
    MustNotAck(Box<Expression>),
}

/// Represents the set of all Database Operations
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum DatabaseOps {
    Queryable(Queryables),
    Mutation(Mutations),
}

/// Represents a Creation Entity
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum CreationEntity {
    IndexEntity {
        columns: Vec<Expression>,
    },
    TableEntity {
        columns: Vec<Parameter>,
        from: Option<Box<Expression>>,
    },
    TableFnEntity {
        fx: Box<Expression>,
    },
}

/// Represents a HTTP Method Call
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum HttpMethodCalls {
    CONNECT(Box<Expression>),
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
            "CONNECT" => HttpMethodCalls::CONNECT(config.into()),
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
            HttpMethodCalls::CONNECT(_) => "CONNECT",
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
            HttpMethodCalls::CONNECT(url_or_object) |
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

/// Represents an enumeration of import definition variations
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

/// Represents an enumeration of data modification event variations
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Mutations {
    Append {
        path: Box<Expression>,
        source: Box<Expression>,
    },
    Create { path: Box<Expression>, entity: CreationEntity },
    Declare(CreationEntity),
    Delete {
        path: Box<Expression>,
        condition: Option<Conditions>,
        limit: Option<Box<Expression>>,
    },
    Drop(MutateTarget),
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

/// Represents an enumeration of Mutation Target variations
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum MutateTarget {
    IndexTarget {
        path: Box<Expression>,
    },
    TableTarget {
        path: Box<Expression>,
    },
}

/// Represents an enumeration of Queryable variations
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Queryables {
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

/// Represents an enumeration of range variations
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Ranges {
    Inclusive(Box<Expression>, Box<Expression>),
    Exclusive(Box<Expression>, Box<Expression>),
}

/// Represents an enumeration of Expression variations
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Expression {
    ArrayExpression(Vec<Expression>),
    AsValue(String, Box<Expression>),
    BitwiseAnd(Box<Expression>, Box<Expression>),
    BitwiseOr(Box<Expression>, Box<Expression>),
    BitwiseShiftLeft(Box<Expression>, Box<Expression>),
    BitwiseShiftRight(Box<Expression>, Box<Expression>),
    BitwiseXor(Box<Expression>, Box<Expression>),
    Coalesce(Box<Expression>, Box<Expression>),
    CodeBlock(Vec<Expression>),
    ColonColon(Box<Expression>, Box<Expression>),
    ColonColonColon(Box<Expression>, Box<Expression>),
    Condition(Conditions),
    CurvyArrowLeft(Box<Expression>, Box<Expression>),
    CurvyArrowRight(Box<Expression>, Box<Expression>),
    DatabaseOp(DatabaseOps),
    Directive(Directives),
    Divide(Box<Expression>, Box<Expression>),
    ElementAt(Box<Expression>, Box<Expression>),
    Feature { title: Box<Expression>, scenarios: Vec<Expression> },
    FnExpression {
        params: Vec<Parameter>,
        body: Option<Box<Expression>>,
        returns: DataType,
    },
    For { item: Box<Expression>, items: Box<Expression>, op: Box<Expression> },
    From(Box<Expression>),
    FunctionCall { fx: Box<Expression>, args: Vec<Expression> },
    HTTP(HttpMethodCalls),
    If {
        condition: Box<Expression>,
        a: Box<Expression>,
        b: Option<Box<Expression>>,
    },
    Import(Vec<ImportOps>),
    Include(Box<Expression>),
    Literal(TypedValue),
    MatchExpression(Box<Expression>, Vec<Expression>),
    Minus(Box<Expression>, Box<Expression>),
    Module(String, Vec<Expression>),
    Modulo(Box<Expression>, Box<Expression>),
    Multiply(Box<Expression>, Box<Expression>),
    Neg(Box<Expression>),
    New(Box<Expression>),
    Ns(Box<Expression>),
    Parameters(Vec<Parameter>),
    Pipeline(Box<Expression>, Box<Expression>),
    Plus(Box<Expression>, Box<Expression>),
    PlusPlus(Box<Expression>, Box<Expression>),
    Pow(Box<Expression>, Box<Expression>),
    Range(Ranges),
    Return(Box<Expression>),
    Scenario {
        title: Box<Expression>,
        verifications: Vec<Expression>,
    },
    SetVariables(Box<Expression>, Box<Expression>),
    StructureExpression(Vec<(String, Expression)>),
    TupleExpression(Vec<Expression>),
    TypeDef(Box<Expression>),
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

    pub fn decompile(expr: &Expression) -> String {
        match expr {
            Expression::ArrayExpression(items) =>
                format!("[{}]", items.iter().map(|i| Self::decompile(i)).collect::<Vec<String>>().join(", ")),
            Expression::AsValue(name, expr) =>
                format!("{}: {}", name, Self::decompile(expr)),
            Expression::BitwiseAnd(a, b) =>
                format!("{} & {}", Self::decompile(a), Self::decompile(b)),
            Expression::BitwiseOr(a, b) =>
                format!("{} | {}", Self::decompile(a), Self::decompile(b)),
            Expression::BitwiseXor(a, b) =>
                format!("{} ^ {}", Self::decompile(a), Self::decompile(b)),
            Expression::BitwiseShiftLeft(a, b) =>
                format!("{} << {}", Self::decompile(a), Self::decompile(b)),
            Expression::BitwiseShiftRight(a, b) =>
                format!("{} >> {}", Self::decompile(a), Self::decompile(b)),
            Expression::Coalesce(a, b) =>
                format!("{} ? {}", Self::decompile(a), Self::decompile(b)),
            Expression::CodeBlock(items) => Self::decompile_code_blocks(items),
            Expression::Condition(cond) => Self::decompile_cond(cond),
            Expression::CurvyArrowLeft(a, b) =>
                format!("{} <~ {}", Self::decompile(a), Self::decompile(b)),
            Expression::CurvyArrowRight(a, b) =>
                format!("{} ~> {}", Self::decompile(a), Self::decompile(b)),
            Expression::DatabaseOp(op) =>
                match op {
                    DatabaseOps::Queryable(q) => Self::decompile_queryables(q),
                    DatabaseOps::Mutation(m) => Self::decompile_modifications(m),
                },
            Expression::Directive(d) => Self::decompile_directives(d),
            Expression::Divide(a, b) =>
                format!("{} / {}", Self::decompile(a), Self::decompile(b)),
            Expression::ElementAt(a, b) =>
                format!("{}[{}]", Self::decompile(a), Self::decompile(b)),
            Expression::ColonColon(a, b) =>
                format!("{}::{}", Self::decompile(a), Self::decompile(b)),
            Expression::ColonColonColon(a, b) =>
                format!("{}:::{}", Self::decompile(a), Self::decompile(b)),
            Expression::Feature { title, scenarios } =>
                format!("feature {} {{\n{}\n}}", title.to_code(), scenarios.iter()
                    .map(|s| s.to_code())
                    .collect::<Vec<_>>()
                    .join("\n")),
            Expression::FnExpression { params, body, returns } =>
                format!("fn({}){}{}", Self::decompile_parameters(params),
                        match returns.to_code() {
                            type_name if !type_name.is_empty() => format!(": {}", type_name),
                            _ => String::new()
                        },
                        match body {
                            Some(my_body) => format!(" => {}", my_body.to_code()),
                            None => String::new()
                        }),
            Expression::For { item, items, op } =>
                format!("for {} in {} {}",
                        Self::decompile(item), Self::decompile(items), Self::decompile(op)),
            Expression::From(a) => format!("from {}", Self::decompile(a)),
            Expression::FunctionCall { fx, args } =>
                format!("{}({})", Self::decompile(fx), Self::decompile_list(args)),
            Expression::HTTP(method) =>
                format!("{} {}", method.get_method(), Self::decompile(&method.get_url_or_config())),
            Expression::If { condition, a, b } =>
                format!("if {} {}{}", Self::decompile(condition), Self::decompile(a), b.to_owned()
                    .map(|x| format!(" else {}", Self::decompile(&x)))
                    .unwrap_or("".into())),
            Expression::Import(args) =>
                format!("import {}", args.iter().map(|a| a.to_code())
                    .collect::<Vec<_>>()
                    .join(", ")),
            Expression::Include(path) => format!("include {}", Self::decompile(path)),
            Expression::Literal(value) => value.to_code(),
            Expression::MatchExpression(a, b) =>
                format!("match {} {}", Self::decompile(a), Self::decompile(&ArrayExpression(b.clone()))),
            Expression::Minus(a, b) =>
                format!("{} - {}", Self::decompile(a), Self::decompile(b)),
            Expression::Module(name, ops) =>
                format!("{} {}", name, Self::decompile_code_blocks(ops)),
            Expression::Modulo(a, b) =>
                format!("{} % {}", Self::decompile(a), Self::decompile(b)),
            Expression::Multiply(a, b) =>
                format!("{} * {}", Self::decompile(a), Self::decompile(b)),
            Expression::Neg(a) => format!("-({})", Self::decompile(a)),
            Expression::New(a) => format!("new {}", Self::decompile(a)),
            Expression::Ns(a) => format!("ns({})", Self::decompile(a)),
            Expression::Parameters(parameters) => Self::decompile_parameters(parameters),
            Expression::Pipeline(a, b) =>
                format!("{} |> {}", Self::decompile(a), Self::decompile(b)),
            Expression::Plus(a, b) =>
                format!("{} + {}", Self::decompile(a), Self::decompile(b)),
            Expression::PlusPlus(a, b) =>
                format!("{} ++ {}", Self::decompile(a), Self::decompile(b)),
            Expression::Pow(a, b) =>
                format!("{} ** {}", Self::decompile(a), Self::decompile(b)),
            Expression::Range(Exclusive(a, b)) =>
                format!("{}..{}", Self::decompile(a), Self::decompile(b)),
            Expression::Range(Inclusive(a, b)) =>
                format!("{}..={}", Self::decompile(a), Self::decompile(b)),
            Expression::Return(a) =>
                format!("return {}", Self::decompile(a)),
            Expression::Scenario { title, verifications } => {
                let title = title.to_code();
                let verifications = verifications.iter()
                    .map(|s| s.to_code())
                    .collect::<Vec<_>>()
                    .join("\n");
                format!("scenario {title} {{\n{verifications}\n}}")
            }
            Expression::SetVariables(vars, values) =>
                format!("{} := {}", Self::decompile(vars), Self::decompile(values)),
            Expression::StructureExpression(items) =>
                format!("{{{}}}", items.iter()
                    .map(|(k, v)| format!("{}: {}", k, v))
                    .collect::<Vec<String>>()
                    .join(", ")),
            Expression::TupleExpression(args) => format!("({})", Self::decompile_list(args)),
            Expression::TypeDef(expr) => format!("typedef({})", expr.to_code()),
            Expression::Variable(name) => name.to_string(),
            Expression::Via(expr) => format!("via {}", Self::decompile(expr)),
            Expression::While { condition, code } =>
                format!("while {} {}", Self::decompile(condition), Self::decompile(code)),
        }
    }

    pub fn decompile_code_blocks(ops: &Vec<Expression>) -> String {
        format!("{{\n{}\n}}", ops.iter().map(|i| Self::decompile(i))
            .collect::<Vec<String>>()
            .join("\n"))
    }

    pub fn decompile_cond(cond: &Conditions) -> String {
        match cond {
            Conditions::And(a, b) =>
                format!("{} && {}", Self::decompile(a), Self::decompile(b)),
            Conditions::Contains(a, b) =>
                format!("{} contains {}", Self::decompile(a), Self::decompile(b)),
            Conditions::Equal(a, b) =>
                format!("{} == {}", Self::decompile(a), Self::decompile(b)),
            Conditions::False => "false".to_string(),
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
            Conditions::Not(a) => format!("!{}", Self::decompile(a)),
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

    pub fn decompile_directives(directive: &Directives) -> String {
        match directive {
            Directives::MustAck(a) => format!("[+] {}", Self::decompile(a)),
            Directives::MustDie(a) => format!("[!] {}", Self::decompile(a)),
            Directives::MustIgnoreAck(a) => format!("[~] {}", Self::decompile(a)),
            Directives::MustNotAck(a) => format!("[-] {}", Self::decompile(a)),
        }
    }

    pub fn decompile_if_exists(if_exists: bool) -> String {
        (if if_exists { "if exists " } else { "" }).to_string()
    }

    pub fn decompile_insert_list(fields: &Vec<Expression>, values: &Vec<Expression>) -> String {
        let field_list = fields.iter().map(|f| Self::decompile(f)).collect::<Vec<String>>().join(", ");
        let value_list = values.iter().map(|v| Self::decompile(v)).collect::<Vec<String>>().join(", ");
        format!("({}) values ({})", field_list, value_list)
    }

    pub fn decompile_limit(opt: &Option<Box<Expression>>) -> String {
        opt.to_owned().map(|x| format!(" limit {}", Self::decompile(&x))).unwrap_or("".into())
    }

    pub fn decompile_list(fields: &Vec<Expression>) -> String {
        fields.iter().map(|x| Self::decompile(x)).collect::<Vec<String>>().join(", ".into())
    }

    pub fn decompile_cond_opt(opt: &Option<Conditions>) -> String {
        opt.to_owned().map(|i| Self::decompile_cond(&i)).unwrap_or("".into())
    }

    pub fn decompile_opt(opt: &Option<Box<Expression>>) -> String {
        opt.to_owned().map(|i| Self::decompile(&i)).unwrap_or("".into())
    }

    pub fn decompile_update_list(fields: &Vec<Expression>, values: &Vec<Expression>) -> String {
        fields.iter().zip(values.iter()).map(|(f, v)|
            format!("{} = {}", Self::decompile(f), Self::decompile(v))).collect::<Vec<String>>().join(", ")
    }

    pub fn decompile_excavations(excavation: &DatabaseOps) -> String {
        match excavation {
            DatabaseOps::Queryable(q) => Self::decompile_queryables(q),
            DatabaseOps::Mutation(m) => Self::decompile_modifications(m),
        }
    }

    pub fn decompile_modifications(expr: &Mutations) -> String {
        match expr {
            Mutations::Append { path, source } =>
                format!("append {} {}", Self::decompile(path), Self::decompile(source)),
            Mutations::Create { path, entity } =>
                match entity {
                    CreationEntity::IndexEntity { columns } =>
                        format!("create index {} [{}]", Self::decompile(path), Self::decompile_list(columns)),
                    CreationEntity::TableEntity { columns, from } =>
                        format!("create table {} ({})", Self::decompile(path), Self::decompile_parameters(columns)),
                    CreationEntity::TableFnEntity { fx } =>
                        format!("create table {} fn({})", Self::decompile(path), Self::decompile(fx)),
                }
            Mutations::Declare(entity) =>
                match entity {
                    CreationEntity::IndexEntity { columns } =>
                        format!("index [{}]", Self::decompile_list(columns)),
                    CreationEntity::TableEntity { columns, from } =>
                        format!("table({})", Self::decompile_parameters(columns)),
                    CreationEntity::TableFnEntity { fx } =>
                        format!("table fn({})", Self::decompile(fx)),
                }
            Mutations::Drop(target) => {
                let (kind, path) = match target {
                    MutateTarget::IndexTarget { path } => ("index", path),
                    MutateTarget::TableTarget { path } => ("table", path),
                };
                format!("drop {} {}", kind, Self::decompile(path))
            }
            Mutations::Delete { path, condition, limit } =>
                format!("delete from {} where {}{}", Self::decompile(path), Self::decompile_cond_opt(condition), Self::decompile_opt(limit)),
            Mutations::IntoNs(a, b) =>
                format!("{} ~> {}", Self::decompile(a), Self::decompile(b)),
            Mutations::Overwrite { path, source, condition, limit } =>
                format!("overwrite {} {}{}{}", Self::decompile(path), Self::decompile(source),
                        condition.to_owned().map(|e| format!(" where {}", Self::decompile_cond(&e))).unwrap_or("".into()),
                        limit.to_owned().map(|e| format!(" limit {}", Self::decompile(&e))).unwrap_or("".into()),
                ),
            Mutations::Truncate { path, limit } =>
                format!("truncate {}{}", Self::decompile(path), Self::decompile_limit(limit)),
            Mutations::Undelete { path, condition, limit } =>
                format!("undelete from {} where {}{}", Self::decompile(path), Self::decompile_cond_opt(condition), Self::decompile_opt(limit)),
            Mutations::Update { path, source, condition, limit } =>
                format!("update {} {} where {}{}", Self::decompile(path), Self::decompile(source), Self::decompile_cond_opt(condition),
                        limit.to_owned().map(|e| format!(" limit {}", Self::decompile(&e))).unwrap_or("".into()), ),
        }
    }

    pub fn decompile_queryables(expr: &Queryables) -> String {
        match expr {
            Queryables::Limit { from: a, limit: b } =>
                format!("{} limit {}", Self::decompile(a), Self::decompile(b)),
            Queryables::Where { from, condition } =>
                format!("{} where {}", Self::decompile(from), Self::decompile_cond(condition)),
            Queryables::Select { fields, from, condition, group_by, having, order_by, limit } =>
                format!("select {}{}{}{}{}{}{}", Self::decompile_list(fields),
                        from.to_owned().map(|e| format!(" from {}", Self::decompile(&e))).unwrap_or("".into()),
                        condition.to_owned().map(|c| format!(" where {}", Self::decompile_cond(&c))).unwrap_or("".into()),
                        limit.to_owned().map(|e| format!(" limit {}", Self::decompile(&e))).unwrap_or("".into()),
                        group_by.to_owned().map(|items| format!(" group by {}", items.iter().map(|e| Self::decompile(e)).collect::<Vec<String>>().join(", "))).unwrap_or("".into()),
                        having.to_owned().map(|e| format!(" having {}", Self::decompile(&e))).unwrap_or("".into()),
                        order_by.to_owned().map(|e| format!(" order by {}", Self::decompile_list(&e))).unwrap_or("".into()),
                ),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        ByteCodeCompiler::encode(&self).unwrap_or_else(|e| panic!("{}", e))
    }

    pub fn from_token(token: Token) -> Self {
        match token.to_owned() {
            Token::Atom { text, .. } => Variable(text),
            Token::Backticks { text, .. } => Variable(text),
            Token::DoubleQuoted { text, .. } => Literal(StringValue(text)),
            Token::Numeric { text, .. } => Literal(Number(Numbers::from_string(text))),
            Token::Operator { .. } => Literal(ErrorValue(SyntaxError(SyntaxErrors::IllegalOperator(token)))),
            Token::SingleQuoted { text, .. } => Literal(StringValue(text)),
            Token::URL { text, .. } => Literal(StringValue(text)),
        }
    }

    pub fn infer_type(&self) -> DataType {
        Expression::infer(self)
    }

    /// provides type inference for the given [Expression]
    pub fn infer(expr: &Expression) -> DataType {
        Self::infer_with_hints(expr, &vec![])
    }

    /// provides type inference for the given [Expression] with hints
    /// to improve the matching performance.
    pub fn infer_with_hints(
        expr: &Expression,
        hints: &Vec<Parameter>,
    ) -> DataType {
        let data_type = Self::do_infer_with_hints(expr, hints);
        //println!("infer_with_hints: [{}] {:?} => '{}'", if hints.is_empty() { "N" } else { "Y" }, expr, data_type);
        data_type
    }

    /// provides type inference for the given [Expression] with hints
    /// to improve the matching performance.
    fn do_infer_with_hints(
        expr: &Expression,
        hints: &Vec<Parameter>,
    ) -> DataType {
        match expr {
            ArrayExpression(items) => ArrayType(items.len()),
            // alias functions: name: get_name()
            AsValue(_, e) => Self::infer_with_hints(e, hints),
            BitwiseAnd(a, b) => Self::infer_a_or_b(a, b, hints),
            BitwiseOr(a, b) => Self::infer_a_or_b(a, b, hints),
            BitwiseShiftLeft(a, b) => Self::infer_a_or_b(a, b, hints),
            BitwiseShiftRight(a, b) => Self::infer_a_or_b(a, b, hints),
            BitwiseXor(a, b) => Self::infer_a_or_b(a, b, hints),
            Coalesce(a, b) => Self::infer_a_or_b(a, b, hints),
            CodeBlock(ops) => ops.last().map(|op| Self::infer_with_hints(op, hints))
                .unwrap_or(UnresolvedType),
            // platform functions: cal::now()
            ColonColon(a, b) | ColonColonColon(a, b) =>
                match (a.deref(), b.deref()) {
                    (Variable(package), FunctionCall { fx, .. }) =>
                        match fx.deref() {
                            Variable(name) =>
                                PackageOps::find_function(package, name)
                                    .map(|pf| pf.get_return_type())
                                    .unwrap_or(UnresolvedType),
                            _ => UnresolvedType
                        }
                    _ => UnresolvedType
                }
            Condition(..) => BooleanType,
            CurvyArrowLeft(..) => StructureType(vec![]),
            CurvyArrowRight(..) => BooleanType,
            DatabaseOp(a) => match a {
                DatabaseOps::Queryable(_) => TableType(vec![], 0),
                DatabaseOps::Mutation(m) => match m {
                    Mutations::Append { .. } => NumberType(NumberKind::RowIdKind),
                    _ => NumberType(NumberKind::I64Kind),
                }
            }
            Directive(..) => BooleanType,
            Divide(a, b) => Self::infer_a_or_b(a, b, hints),
            ElementAt(..) => UnresolvedType,
            Feature { .. } => BooleanType,
            FnExpression { params, returns, .. } =>
                FunctionType(params.clone(), Box::new(returns.clone())),
            Pipeline(_, b) => Self::infer_with_hints(b, hints),
            For { op, .. } => Self::infer_with_hints(op, hints),
            From(..) => TableType(vec![], 0),
            FunctionCall { fx, .. } => Self::infer_with_hints(fx, hints),
            HTTP { .. } => UnresolvedType,
            If { a: true_v, b: Some(false_v), .. } => Self::infer_a_or_b(true_v, false_v, hints),
            If { a: true_v, .. } => Self::infer_with_hints(true_v, hints),
            Import(..) => BooleanType,
            Include(..) => BooleanType,
            Literal(Function { body, .. }) => Self::infer_with_hints(body, hints),
            Literal(PlatformOp(pf)) => pf.get_return_type(),
            Literal(v) => v.get_type(),
            MatchExpression(_, cases) => DataType::best_fit(
                cases.iter()
                    .map(|op| Self::infer_with_hints(op, hints))
                    .collect::<Vec<_>>()
            ),
            Minus(a, b) => Self::infer_a_or_b(a, b, hints),
            Module(..) => BooleanType,
            Modulo(a, b) => Self::infer_a_or_b(a, b, hints),
            Multiply(a, b) => Self::infer_a_or_b(a, b, hints),
            Neg(a) => Self::infer_with_hints(a, hints),
            New(a) => Self::infer_with_hints(a, hints),
            Ns(..) => BooleanType,
            Parameters(params) => ArrayType(params.len()),
            Plus(a, b) => Self::infer_a_or_b(a, b, hints),
            PlusPlus(a, b) => Self::infer_a_or_b(a, b, hints),
            Pow(a, b) => Self::infer_a_or_b(a, b, hints),
            Range(Exclusive(a, b)) => Self::infer_a_or_b(a, b, hints),
            Range(Inclusive(a, b)) => Self::infer_a_or_b(a, b, hints),
            Return(a) => Self::infer_with_hints(a, hints),
            Scenario { .. } => BooleanType,
            SetVariables(..) => BooleanType,
            // structures: { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 }
            StructureExpression(key_values) => {
                let mut params = vec![];
                let mut combined_hints = hints.clone();
                for (name, value) in key_values {
                    let param = Parameter::new(name.clone(), Self::infer_with_hints(value, &combined_hints));
                    params.push(param.clone());
                    combined_hints.push(param);
                }
                StructureType(params)
            }
            // tuples: (100, 23, 36)
            TupleExpression(values) => TupleType(values.iter()
                .map(|p| Self::infer_with_hints(p, hints))
                .collect::<Vec<_>>()),
            TypeDef(expr) => Self::infer_with_hints(expr, hints),
            Variable(name) =>
                match name {
                    s if s == machine::ROW_ID => NumberType(I64Kind),
                    _ =>
                        match hints.iter().find(|hint| hint.get_name() == name) {
                            Some(param) => param.get_data_type(),
                            None => UnresolvedType
                        }
                }
            Via(..) => TableType(vec![], 0),
            While { .. } => UnresolvedType,
        }
    }

    /// provides type inference for the given [Expression]s
    fn infer_a_or_b(
        a: &Expression,
        b: &Expression,
        hints: &Vec<Parameter>,
    ) -> DataType {
        DataType::best_fit(vec![
            Self::infer_with_hints(a, hints),
            Self::infer_with_hints(b, hints)
        ])
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
        Self::decompile(self)
    }

    fn purify(items: &Vec<Expression>) -> std::io::Result<TypedValue> {
        let mut new_items = Vec::new();
        for item in items {
            new_items.push(item.to_pure()?);
        }
        Ok(ArrayValue(Array::from(new_items)))
    }

    /// Attempts to resolve the [Expression] as a [TypedValue]
    pub fn to_pure(&self) -> std::io::Result<TypedValue> {
        match self {
            Expression::AsValue(_, expr) => expr.to_pure(),
            Expression::ArrayExpression(items) => Self::purify(items),
            Expression::BitwiseAnd(a, b) => Ok(a.to_pure()? & b.to_pure()?),
            Expression::BitwiseOr(a, b) => Ok(a.to_pure()? | b.to_pure()?),
            Expression::BitwiseXor(a, b) => Ok(a.to_pure()? ^ b.to_pure()?),
            Expression::BitwiseShiftLeft(a, b) => Ok(a.to_pure()? << b.to_pure()?),
            Expression::BitwiseShiftRight(a, b) => Ok(a.to_pure()? >> b.to_pure()?),
            Expression::Condition(kind) => match kind {
                Conditions::And(a, b) =>
                    Ok(Boolean(a.to_pure()?.is_true() && b.to_pure()?.is_true())),
                Conditions::False => Ok(Boolean(false)),
                Conditions::Or(a, b) =>
                    Ok(Boolean(a.to_pure()?.is_true() || b.to_pure()?.is_true())),
                Conditions::True => Ok(Boolean(true)),
                z => throw(TypeMismatch(ConstantValueExpected(z.to_code())))
            }
            Expression::Divide(a, b) => Ok(a.to_pure()? / b.to_pure()?),
            Expression::ElementAt(a, b) => {
                let index = b.to_pure()?.to_usize();
                Ok(match a.to_pure()? {
                    TypedValue::ArrayValue(arr) => arr.get_or_else(index, Undefined),
                    TypedValue::ErrorValue(err) => ErrorValue(err),
                    TypedValue::Null => TypedValue::Null,
                    TypedValue::Structured(s) => {
                        let items = s.get_values();
                        if index >= items.len() { Undefined } else { items[index].clone() }
                    }
                    TypedValue::TableValue(df) => df.read_one(index)?
                        .map(|row| Structured(Firm(row, df.get_parameters())))
                        .unwrap_or(Undefined),
                    TypedValue::Undefined => Undefined,
                    z => return throw(TypeMismatch(UnsupportedType(UnresolvedType, z.get_type())))
                })
            }
            Expression::StructureExpression(items) => {
                let mut new_items = Vec::new();
                for (name, expr) in items {
                    new_items.push((name.to_string(), expr.to_pure()?))
                }
                Ok(Structured(Soft(SoftStructure::from_tuples(new_items))))
            }
            Expression::Literal(value) => Ok(value.clone()),
            Expression::Minus(a, b) => Ok(a.to_pure()? - b.to_pure()?),
            Expression::Modulo(a, b) => Ok(a.to_pure()? % b.to_pure()?),
            Expression::Multiply(a, b) => Ok(a.to_pure()? * b.to_pure()?),
            Expression::Neg(expr) => expr.to_pure().map(|v| -v),
            Expression::Plus(a, b) => Ok(a.to_pure()? + b.to_pure()?),
            Expression::Pow(a, b) => Ok(a.to_pure()?.pow(&b.to_pure()?)
                .unwrap_or(Undefined)),
            Expression::Range(Exclusive(a, b)) =>
                Ok(ArrayValue(Array::from(TypedValue::exclusive_range(
                    a.to_pure()?,
                    b.to_pure()?,
                    Number(I64Value(1)),
                )))),
            Expression::Range(Inclusive(a, b)) =>
                Ok(ArrayValue(Array::from(TypedValue::inclusive_range(
                    a.to_pure()?,
                    b.to_pure()?,
                    Number(I64Value(1)),
                )))),
            z => throw(TypeMismatch(ConstantValueExpected(z.to_code())))
        }
    }
}

impl Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_code())
    }
}

fn to_ns(path: Expression) -> Expression {
    fn fx(name: &str, path: Expression) -> Expression {
        Expression::FunctionCall {
            fx: Box::new(Variable(name.into())),
            args: vec![path],
        }
    }
    fx("ns", path)
}

/// expression unit tests
#[cfg(test)]
mod expression_tests {
    use crate::data_types::DataType::{NumberType, StringType, UnresolvedType};
    use crate::expression::Conditions::*;
    use crate::expression::CreationEntity::{IndexEntity, TableEntity};
    use crate::expression::DatabaseOps::{Mutation, Queryable};
    use crate::expression::Expression::*;
    use crate::expression::*;
    use crate::machine::Machine;
    use crate::number_kind::NumberKind::F64Kind;
    use crate::numbers::Numbers::I64Value;
    use crate::numbers::Numbers::*;
    use crate::tokenizer;
    use crate::typed_values::TypedValue::*;
    use crate::typed_values::TypedValue::{Number, StringValue};

    use super::*;

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
        let model = Conditions::And(Box::new(TRUE), Box::new(FALSE));
        let (_, result) = machine.do_condition(&model).unwrap();
        assert_eq!(result, Boolean(false));
        assert_eq!(model.to_code(), "true && false")
    }

    #[test]
    fn test_in_range_exclusive_expression() {
        let machine = Machine::empty();
        let model = In(
            Literal(Number(I32Value(10))).into(),
            Range(Exclusive(
                Literal(Number(I32Value(1))).into(),
                Literal(Number(I32Value(10))).into()
            )).into(),
        );
        let (_, result) = machine.do_condition(&model).unwrap();
        assert_eq!(result, Boolean(false));
        assert_eq!(model.to_code(), "10 in 1..10")
    }

    #[test]
    fn test_in_range_inclusive_expression() {
        let machine = Machine::empty();
        let model = In(
            Literal(Number(I32Value(10))).into(),
            Range(Inclusive(
                Literal(Number(I32Value(1))).into(),
                Literal(Number(I32Value(10))).into()
            )).into(),
        );
        let (_, result) = machine.do_condition(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "10 in 1..=10")
    }

    #[test]
    fn test_equality_integers() {
        let machine = Machine::empty();
        let model = Equal(
            Box::new(Literal(Number(I32Value(5)))),
            Box::new(Literal(Number(I32Value(5)))),
        );
        let (_, result) = machine.do_condition(&model).unwrap();
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
        let (_, result) = machine.do_condition(&model).unwrap();
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
        let (_, result) = machine.do_condition(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "\"Hello\" == \"Hello\"")
    }

    #[test]
    fn test_function_expression() {
        let model = FnExpression {
            params: vec![],
            body: None,
            returns: StringType(0),
        };
        assert_eq!(model.to_code(), "fn(): String")
    }

    #[test]
    fn test_inequality_strings() {
        let machine = Machine::empty();
        let model = NotEqual(
            Box::new(Literal(StringValue("Hello".to_string()))),
            Box::new(Literal(StringValue("Goodbye".to_string()))),
        );
        let (_, result) = machine.do_condition(&model).unwrap();
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
        let (_, result) = machine.do_condition(&model).unwrap();
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
        let (_, result) = machine.do_condition(&model).unwrap();
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
        let (_, result) = machine.do_condition(&model).unwrap();
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
        let (_, result) = machine.do_condition(&model).unwrap();
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
        let (_, result) = machine.do_condition(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "-5 != 5")
    }

    #[test]
    fn test_conditional_or() {
        let machine = Machine::empty();
        let model = Conditions::Or(Box::new(TRUE), Box::new(FALSE));
        let (_, result) = machine.do_condition(&model).unwrap();
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
            Variable("x".into()).into(),
            Range(Inclusive(
                Literal(Number(I32Value(1))).into(),
                Literal(Number(I32Value(10))).into(),
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
        let from = DatabaseOp(Queryable(Queryables::Where {
            from: Box::new(from),
            condition: GreaterOrEqual(
                Box::new(Variable("last_sale".into())),
                Box::new(Literal(Number(F64Value(1.25)))),
            ),
        }));
        let from = DatabaseOp(Queryable(Queryables::Limit {
            from: Box::new(from),
            limit: Box::new(Literal(Number(I64Value(5)))),
        }));
        assert_eq!(
            from.to_code(),
            r#"from ns("machine.overwrite.stocks") where last_sale >= 1.25 limit 5"#
        )
    }

    #[test]
    fn test_overwrite() {
        let model = DatabaseOp(Mutation(Mutations::Overwrite {
            path: Box::new(Variable("stocks".into())),
            source: Box::new(Via(Box::new(StructureExpression(vec![
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

    #[test]
    fn test_alias() {
        let model = AsValue("symbol".into(), Box::new(Literal(StringValue("ABC".into()))));
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
        assert_eq!(model.to_pure().unwrap(), Number(I64Value(0)));
        assert_eq!(Expression::decompile(&model), "20 & 3")
    }

    #[test]
    fn test_bitwise_or() {
        let model = BitwiseOr(
            Box::new(Literal(Number(I64Value(20)))),
            Box::new(Literal(Number(I64Value(3)))),
        );
        assert_eq!(model.to_pure().unwrap(), Number(I64Value(23)));
        assert_eq!(Expression::decompile(&model), "20 | 3")
    }

    #[test]
    fn test_bitwise_shl() {
        let model = BitwiseShiftLeft(
            Box::new(Literal(Number(I64Value(20)))),
            Box::new(Literal(Number(I64Value(3)))),
        );
        assert_eq!(model.to_pure().unwrap(), Number(I64Value(160)));
        assert_eq!(Expression::decompile(&model), "20 << 3")
    }

    #[test]
    fn test_bitwise_shr() {
        let model = BitwiseShiftRight(
            Box::new(Literal(Number(I64Value(20)))),
            Box::new(Literal(Number(I64Value(3)))),
        );
        assert_eq!(model.to_pure().unwrap(), Number(I64Value(2)));
        assert_eq!(Expression::decompile(&model), "20 >> 3")
    }

    #[test]
    fn test_bitwise_xor() {
        let model = BitwiseXor(
            Box::new(Literal(Number(I64Value(20)))),
            Box::new(Literal(Number(I64Value(3)))),
        );
        assert_eq!(model.to_pure().unwrap(), Number(I64Value(23)));
        assert_eq!(Expression::decompile(&model), "20 ^ 3")
    }

    #[test]
    fn test_define_anonymous_function() {
        let model = FnExpression {
            params: vec![
                Parameter::add("a"),
                Parameter::add("b"),
            ],
            body: Some(Box::new(Multiply(Box::new(
                Variable("a".into())
            ), Box::new(
                Variable("b".into())
            )))),
            returns: UnresolvedType,
        };
        assert_eq!(Expression::decompile(&model), "fn(a, b) => a * b")
    }

    #[test]
    fn test_define_named_function() {
        let model = SetVariables(
            Variable("add".into()).into(),
            FnExpression {
                params: vec![
                    Parameter::add("a"),
                    Parameter::add("b"),
                ],
                body: Some(Box::new(Plus(Box::new(
                    Variable("a".into())
                ), Box::new(
                    Variable("b".into())
                )))),
                returns: UnresolvedType,
            }.into(),
        );
        assert_eq!(Expression::decompile(&model), "add := fn(a, b) => a + b")
    }

    #[test]
    fn test_function_call() {
        let model = FunctionCall {
            fx: Box::new(Variable("f".into())),
            args: vec![
                Literal(Number(I64Value(2))),
                Literal(Number(I64Value(3))),
            ],
        };
        assert_eq!(Expression::decompile(&model), "f(2, 3)")
    }

    #[test]
    fn test_create_index_in_namespace() {
        let model = DatabaseOp(Mutation(Mutations::Create {
            path: Box::new(Ns(Box::new(Literal(StringValue("compiler.create.stocks".into()))))),
            entity: IndexEntity {
                columns: vec![
                    Variable("symbol".into()),
                    Variable("exchange".into()),
                ],
            },
        }));
        assert_eq!(
            Expression::decompile(&model),
            r#"create index ns("compiler.create.stocks") [symbol, exchange]"#)
    }

    #[test]
    fn test_create_table_in_namespace() {
        let ns_path = "compiler.create.stocks";
        let model = DatabaseOp(Mutation(Mutations::Create {
            path: Box::new(Ns(Box::new(Literal(StringValue(ns_path.into()))))),
            entity: TableEntity {
                columns: vec![
                    Parameter::with_default("symbol", StringType(8), StringValue("ABC".into())),
                    Parameter::with_default("exchange", StringType(8), StringValue("NYSE".into())),
                    Parameter::with_default("last_sale", NumberType(F64Kind), Number(F64Value(0.))),
                ],
                from: None,
            },
        }));
        assert_eq!(
            Expression::decompile(&model),
            r#"create table ns("compiler.create.stocks") (symbol: String(8) := "ABC", exchange: String(8) := "NYSE", last_sale: f64 := 0.0)"#)
    }

    #[test]
    fn test_declare_table() {
        let model = DatabaseOp(Mutation(Mutations::Declare(TableEntity {
            columns: vec![
                Parameter::new("symbol", StringType(8)),
                Parameter::new("exchange", StringType(8)),
                Parameter::new("last_sale", NumberType(F64Kind)),
            ],
            from: None,
        })));
        assert_eq!(
            Expression::decompile(&model),
            r#"table(symbol: String(8), exchange: String(8), last_sale: f64)"#)
    }
}

/// pure expression unit tests
#[cfg(test)]
mod pure_expression_tests {
    use crate::compiler::Compiler;
    use crate::numbers::Numbers::{F64Value, I64Value, U64Value};
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
        verify_pure("0b1011 & 0b1101", Number(U64Value(9)))
    }

    #[test]
    fn test_to_pure_bitwise_or() {
        verify_pure("0b0110 | 0b0011", Number(U64Value(7)))
    }

    #[test]
    fn test_to_pure_bitwise_shl() {
        verify_pure("0b0001 << 0x03", Number(U64Value(8)))
    }

    #[test]
    fn test_to_pure_bitwise_shr() {
        verify_pure("0b1_000_000 >> 0b0010", Number(U64Value(16)))
    }

    #[test]
    fn test_to_pure_bitwise_xor() {
        verify_pure("0b0110 ^ 0b0011", Number(U64Value(5))) // 0b0101
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
        assert_eq!(expr.to_pure().unwrap(), expected)
    }
}

/// inference unit tests
#[cfg(test)]
mod inference_tests {
    use super::*;
    use crate::number_kind::NumberKind::{F64Kind, I64Kind};
    use crate::numbers::Numbers::{F64Value, I64Value};
    use crate::testdata::{verify_bit_operator, verify_data_type, verify_math_operator};
    use crate::typed_values::TypedValue::{Number, StringValue};

    #[test]
    fn test_infer() {
        let kind = Expression::infer(
            &Literal(StringValue("hello world".into()))
        );
        assert_eq!(kind, StringType(11))
    }

    #[test]
    fn test_infer_a_or_b_strings() {
        let kind = Expression::infer_a_or_b(
            &Literal(StringValue("yes".into())),
            &Literal(StringValue("hello".into())),
            &vec![],
        );
        assert_eq!(kind, StringType(5))
    }

    #[test]
    fn test_infer_a_or_b_numbers() {
        let kind = Expression::infer_a_or_b(
            &Literal(Number(I64Value(76))),
            &Literal(Number(F64Value(76.0))),
            &vec![],
        );
        assert_eq!(kind, NumberType(F64Kind))
    }

    #[test]
    fn test_infer_conditionals_and() {
        verify_data_type("true && false", BooleanType);
    }

    #[test]
    fn test_infer_conditionals_in_range_exclusive() {
        verify_data_type("20 in 1..20", BooleanType);
    }

    #[test]
    fn test_infer_conditionals_in_range_inclusive() {
        verify_data_type("20 in 1..=20", BooleanType);
    }

    #[test]
    fn test_infer_conditionals_eq() {
        verify_data_type("x == y", BooleanType);
    }

    #[test]
    fn test_infer_conditionals_gt() {
        verify_data_type("x > y", BooleanType);
    }

    #[test]
    fn test_infer_conditionals_gte() {
        verify_data_type("x >= y", BooleanType);
    }

    #[test]
    fn test_infer_conditionals_is() {
        verify_data_type("a is b", BooleanType);
    }

    #[test]
    fn test_infer_conditionals_isnt() {
        verify_data_type("a isnt b", BooleanType);
    }

    #[test]
    fn test_infer_conditionals_lt() {
        verify_data_type("x < y", BooleanType);
    }

    #[test]
    fn test_infer_conditionals_lte() {
        verify_data_type("x <= y", BooleanType);
    }

    #[test]
    fn test_infer_conditionals_neq() {
        verify_data_type("x != y", BooleanType);
    }

    #[test]
    fn test_infer_conditionals_or() {
        verify_data_type("true || false", BooleanType);
    }

    #[test]
    fn test_infer_mathematics_divide() {
        verify_math_operator("/");
    }

    #[test]
    fn test_infer_mathematics_minus() {
        verify_math_operator("-");
    }

    #[test]
    fn test_infer_mathematics_plus() {
        verify_math_operator("+");
    }

    #[test]
    fn test_infer_mathematics_plus_plus() {
        verify_math_operator("++");
    }

    #[test]
    fn test_infer_mathematics_power() {
        verify_math_operator("**");
    }

    #[test]
    fn test_infer_mathematics_shl() {
        verify_bit_operator("<<");
    }

    #[test]
    fn test_infer_mathematics_shr() {
        verify_bit_operator(">>");
    }

    #[test]
    fn test_infer_mathematics_times() {
        verify_math_operator("*");
    }

    #[test]
    fn test_infer_return() {
        verify_data_type("return 5", NumberType(I64Kind));
    }

    #[test]
    fn test_infer_row_id() {
        verify_data_type("__row_id__", NumberType(I64Kind));
    }

    #[test]
    fn test_infer_tools_row_id() {
        verify_data_type("tools::row_id()", NumberType(I64Kind));
    }
}