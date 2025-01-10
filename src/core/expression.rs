#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Expression class
////////////////////////////////////////////////////////////////////

use crate::arrays::Array;
use crate::byte_code_compiler::ByteCodeCompiler;
use crate::data_types::DataType;
use crate::data_types::DataType::VaryingType;
use crate::descriptor::Descriptor;
use crate::errors::throw;
use crate::errors::Errors::{Exact, IllegalOperator, TypeMismatch};
use crate::errors::TypeMismatchErrors::UnsupportedType;
use crate::expression::Expression::*;
use crate::inferences::Inferences;
use crate::numbers::Numbers;
use crate::numbers::Numbers::I64Value;
use crate::parameter::Parameter;
use crate::row_collection::RowCollection;
use crate::structures::Structures::{Firm, Soft};
use crate::structures::{SoftStructure, Structure};
use crate::tokens::Token;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{ArrayValue, Boolean, ErrorValue, Number, StringValue, Structured, Undefined};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

// constants
pub const ACK: Expression = Literal(Number(Numbers::Ack));
pub const FALSE: Expression = Condition(Conditions::False);
pub const TRUE: Expression = Condition(Conditions::True);
pub const NULL: Expression = Literal(TypedValue::Null);
pub const UNDEFINED: Expression = Literal(TypedValue::Undefined);

/// Represents Logical Conditions
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
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

/// Represents a data modification event
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

/// Represents a Mutation Target
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum MutateTarget {
    IndexTarget {
        path: Box<Expression>,
    },
    TableTarget {
        path: Box<Expression>,
    },
}

/// Represents an enumeration of queryables
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

/// Represents an Expression
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Expression {
    ArrayExpression(Vec<Expression>),
    AsValue(String, Box<Expression>),
    // bitwise
    BitwiseAnd(Box<Expression>, Box<Expression>),
    BitwiseOr(Box<Expression>, Box<Expression>),
    BitwiseShiftLeft(Box<Expression>, Box<Expression>),
    BitwiseShiftRight(Box<Expression>, Box<Expression>),
    BitwiseXor(Box<Expression>, Box<Expression>),
    //
    CodeBlock(Vec<Expression>),
    Condition(Conditions),
    DatabaseOp(DatabaseOps),
    Directive(Directives),
    Divide(Box<Expression>, Box<Expression>),
    ElementAt(Box<Expression>, Box<Expression>),
    Extraction(Box<Expression>, Box<Expression>),
    ExtractPostfix(Box<Expression>, Box<Expression>),
    Factorial(Box<Expression>),
    Feature { title: Box<Expression>, scenarios: Vec<Expression> },
    ForEach(String, Box<Expression>, Box<Expression>),
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
    PlusPlus(Box<Expression>, Box<Expression>),
    Pow(Box<Expression>, Box<Expression>),
    Range(Box<Expression>, Box<Expression>),
    Return(Vec<Expression>),
    Scenario {
        title: Box<Expression>,
        verifications: Vec<Expression>,
    },
    SetVariable(String, Box<Expression>),
    Tuple(Vec<Expression>),
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
        use crate::expression::Expression::*;
        match expr {
            ArrayExpression(items) =>
                format!("[{}]", items.iter().map(|i| Self::decompile(i)).collect::<Vec<String>>().join(", ")),
            AsValue(name, expr) =>
                format!("{}: {}", name, Self::decompile(expr)),
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
            CodeBlock(items) => Self::decompile_code_blocks(items),
            Condition(cond) => Self::decompile_cond(cond),
            Directive(d) => Self::decompile_directives(d),
            Divide(a, b) =>
                format!("{} / {}", Self::decompile(a), Self::decompile(b)),
            ElementAt(a, b) =>
                format!("{}[{}]", Self::decompile(a), Self::decompile(b)),
            Extraction(a, b) =>
                format!("{}::{}", Self::decompile(a), Self::decompile(b)),
            ExtractPostfix(a, b) =>
                format!("{}:::{}", Self::decompile(a), Self::decompile(b)),
            Factorial(a) => format!("{}¡", Self::decompile(a)),
            Feature { title, scenarios } =>
                format!("feature {} {{\n{}\n}}", title.to_code(), scenarios.iter()
                    .map(|s| s.to_code())
                    .collect::<Vec<_>>()
                    .join("\n")),
            ForEach(a, b, c) =>
                format!("foreach {} in {} {}", a, Self::decompile(b), Self::decompile(c)),
            From(a) => format!("from {}", Self::decompile(a)),
            Tuple(args) => format!("({})", Self::decompile_list(args)),
            FunctionCall { fx, args } =>
                format!("{}({})", Self::decompile(fx), Self::decompile_list(args)),
            If { condition, a, b } =>
                format!("if {} {}{}", Self::decompile(condition), Self::decompile(a), b.to_owned()
                    .map(|x| format!(" else {}", Self::decompile(&x)))
                    .unwrap_or("".into())),
            Import(args) =>
                format!("import {}", args.iter().map(|a| a.to_code())
                    .collect::<Vec<_>>()
                    .join(", ")),
            Include(path) => format!("include {}", Self::decompile(path)),
            JSONExpression(items) =>
                format!("{{{}}}", items.iter()
                    .map(|(k, v)| format!("{}: {}", k, v))
                    .collect::<Vec<String>>()
                    .join(", ")),
            Literal(value) => value.to_code(),
            Minus(a, b) =>
                format!("{} - {}", Self::decompile(a), Self::decompile(b)),
            Module(name, ops) =>
                format!("{} {}", name, Self::decompile_code_blocks(ops)),
            Modulo(a, b) =>
                format!("{} % {}", Self::decompile(a), Self::decompile(b)),
            Multiply(a, b) =>
                format!("{} * {}", Self::decompile(a), Self::decompile(b)),
            Neg(a) => format!("-({})", Self::decompile(a)),
            Ns(a) => format!("ns({})", Self::decompile(a)),
            Parameters(parameters) => Self::decompile_parameters(parameters),
            Plus(a, b) =>
                format!("{} + {}", Self::decompile(a), Self::decompile(b)),
            PlusPlus(a, b) =>
                format!("{} ++ {}", Self::decompile(a), Self::decompile(b)),
            Pow(a, b) =>
                format!("{} ** {}", Self::decompile(a), Self::decompile(b)),
            DatabaseOp(job) =>
                match job {
                    DatabaseOps::Queryable(q) => Self::decompile_queryables(q),
                    DatabaseOps::Mutation(m) => Self::decompile_modifications(m),
                },
            Range(a, b) =>
                format!("{}..{}", Self::decompile(a), Self::decompile(b)),
            Return(items) =>
                format!("return {}", Self::decompile_list(items)),
            Scenario { title, verifications } => {
                let title = title.to_code();
                let verifications = verifications.iter()
                    .map(|s| s.to_code())
                    .collect::<Vec<_>>()
                    .join("\n");
                format!("scenario {title} {{\n{verifications}\n}}")
            }
            SetVariable(name, value) =>
                format!("{} := {}", name, Self::decompile(value)),
            Variable(name) => name.to_string(),
            Via(expr) => format!("via {}", Self::decompile(expr)),
            While { condition, code } =>
                format!("while {} {}", Self::decompile(condition), Self::decompile(code)),
            HTTP { method, url, body, headers, multipart } =>
                format!("{} {}{}{}{}", method, Self::decompile(url), Self::decompile_opt(body), Self::decompile_opt(headers), Self::decompile_opt(multipart)),
        }
    }

    pub fn decompile_code_blocks(ops: &Vec<Expression>) -> String {
        format!("{{\n{}\n}}", ops.iter().map(|i| Self::decompile(i))
            .collect::<Vec<String>>()
            .join("\n"))
    }

    pub fn decompile_cond(cond: &Conditions) -> String {
        use crate::expression::Conditions::*;
        match cond {
            And(a, b) =>
                format!("{} && {}", Self::decompile(a), Self::decompile(b)),
            Between(a, b, c) =>
                format!("{} between {} and {}", Self::decompile(a), Self::decompile(b), Self::decompile(c)),
            Betwixt(a, b, c) =>
                format!("{} betwixt {} and {}", Self::decompile(a), Self::decompile(b), Self::decompile(c)),
            Contains(a, b) =>
                format!("{} contains {}", Self::decompile(a), Self::decompile(b)),
            Equal(a, b) =>
                format!("{} == {}", Self::decompile(a), Self::decompile(b)),
            False => "false".to_string(),
            GreaterThan(a, b) =>
                format!("{} > {}", Self::decompile(a), Self::decompile(b)),
            GreaterOrEqual(a, b) =>
                format!("{} >= {}", Self::decompile(a), Self::decompile(b)),
            LessThan(a, b) =>
                format!("{} < {}", Self::decompile(a), Self::decompile(b)),
            LessOrEqual(a, b) =>
                format!("{} <= {}", Self::decompile(a), Self::decompile(b)),
            Like(a, b) =>
                format!("{} like {}", Self::decompile(a), Self::decompile(b)),
            Not(a) => format!("!{}", Self::decompile(a)),
            NotEqual(a, b) =>
                format!("{} != {}", Self::decompile(a), Self::decompile(b)),
            Or(a, b) =>
                format!("{} || {}", Self::decompile(a), Self::decompile(b)),
            True => "true".to_string(),
        }
    }

    pub fn decompile_descriptors(params: &Vec<Descriptor>) -> String {
        params.iter().map(|d| d.to_code())
            .collect::<Vec<_>>()
            .join(", ")
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
                }
            Mutations::Declare(entity) =>
                match entity {
                    CreationEntity::IndexEntity { columns } =>
                        format!("index [{}]", Self::decompile_list(columns)),
                    CreationEntity::TableEntity { columns, from } =>
                        format!("table({})", Self::decompile_parameters(columns)),
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
        Self::decompile(self)
    }

    /// Attempts to resolve the [Expression] as a [TypedValue]
    pub fn to_pure(&self) -> std::io::Result<TypedValue> {
        match self {
            AsValue(_, expr) => expr.to_pure(),
            ArrayExpression(items) => {
                let mut new_items = Vec::new();
                for item in items {
                    new_items.push(item.to_pure()?);
                }
                Ok(ArrayValue(Array::from(new_items)))
            }
            BitwiseAnd(a, b) => Ok(a.to_pure()? & b.to_pure()?),
            BitwiseOr(a, b) => Ok(a.to_pure()? | b.to_pure()?),
            BitwiseXor(a, b) => Ok(a.to_pure()? ^ b.to_pure()?),
            BitwiseShiftLeft(a, b) => Ok(a.to_pure()? << b.to_pure()?),
            BitwiseShiftRight(a, b) => Ok(a.to_pure()? >> b.to_pure()?),
            Condition(kind) => match kind {
                Conditions::And(a, b) =>
                    Ok(Boolean(a.to_pure()?.is_true() && b.to_pure()?.is_true())),
                Conditions::False => Ok(Boolean(false)),
                Conditions::Or(a, b) =>
                    Ok(Boolean(a.to_pure()?.is_true() || b.to_pure()?.is_true())),
                Conditions::True => Ok(Boolean(true)),
                z => throw(Exact(format!("Constant value required near {}", z.to_code())))
            }
            Divide(a, b) => Ok(a.to_pure()? / b.to_pure()?),
            ElementAt(a, b) => {
                let index = b.to_pure()?.to_usize();
                Ok(match a.to_pure()? {
                    ArrayValue(arr) => arr.get_or_else(index, Undefined),
                    ErrorValue(err) => ErrorValue(err),
                    TypedValue::Null => TypedValue::Null,
                    Structured(s) => {
                        let items = s.get_values();
                        if index >= items.len() { Undefined } else { items[index].clone() }
                    }
                    TypedValue::TableValue(df) => df.read_one(index)?
                        .map(|row| Structured(Firm(row, df.get_columns().clone())))
                        .unwrap_or(Undefined),
                    Undefined => Undefined,
                    z => ErrorValue(TypeMismatch(UnsupportedType(VaryingType(vec![]), z.get_type())))
                })
            }
            Factorial(expr) => expr.to_pure().map(|v| v.factorial()),
            JSONExpression(items) => {
                let mut new_items = Vec::new();
                for (name, expr) in items {
                    new_items.push((name.to_string(), expr.to_pure()?))
                }
                Ok(Structured(Soft(SoftStructure::from_tuples(new_items))))
            }
            Literal(value) => Ok(value.clone()),
            Minus(a, b) => Ok(a.to_pure()? - b.to_pure()?),
            Modulo(a, b) => Ok(a.to_pure()? % b.to_pure()?),
            Multiply(a, b) => Ok(a.to_pure()? * b.to_pure()?),
            Neg(expr) => expr.to_pure().map(|v| -v),
            Plus(a, b) => Ok(a.to_pure()? + b.to_pure()?),
            Pow(a, b) => Ok(a.to_pure()?.pow(&b.to_pure()?)
                .unwrap_or(Undefined)),
            Range(a, b) =>
                Ok(ArrayValue(Array::from(TypedValue::express_range(
                    a.to_pure()?,
                    b.to_pure()?,
                    Number(I64Value(1)),
                )))),
            z => throw(Exact(format!("Constant value required near {}", z.to_code())))
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
    use crate::data_types::DataType::{NumberType, StringType};
    use crate::expression::Conditions::*;
    use crate::expression::CreationEntity::{IndexEntity, TableEntity};
    use crate::expression::DatabaseOps::{Mutation, Queryable};
    use crate::expression::Expression::{AsValue, Literal};
    use crate::expression::*;
    use crate::machine::Machine;
    use crate::number_kind::NumberKind::F64Kind;
    use crate::numbers::Numbers::I64Value;
    use crate::numbers::Numbers::*;
    use crate::tokenizer;
    use crate::typed_values::TypedValue::*;
    use crate::typed_values::TypedValue::{Function, Number, StringValue};

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
        let model = Conditions::Or(Box::new(TRUE), Box::new(FALSE));
        let (_, result) = machine.evaluate_cond(&model).unwrap();
        assert_eq!(result, Boolean(true));
        assert_eq!(model.to_code(), "true || false")
    }

    #[test]
    fn test_is_conditional() {
        let model = Condition(Conditions::And(Box::new(TRUE), Box::new(FALSE)));
        assert_eq!(model.to_code(), "true && false");
        assert!(model.is_conditional());

        let model = Condition(Between(
            Box::new(Variable("x".into())),
            Box::new(Literal(Number(I32Value(1)))),
            Box::new(Literal(Number(I32Value(10)))),
        ));
        assert_eq!(model.to_code(), "x between 1 and 10");
        assert!(model.is_conditional());

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
            "from ns(\"machine.overwrite.stocks\") where last_sale >= 1.25 limit 5"
        )
    }

    #[test]
    fn test_overwrite() {
        let model = DatabaseOp(Mutation(Mutations::Overwrite {
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
        let model = Literal(Function {
            params: vec![
                Parameter::build("a"),
                Parameter::build("b"),
            ],
            code: Box::new(Multiply(Box::new(
                Variable("a".into())
            ), Box::new(
                Variable("b".into())
            ))),
        });
        assert_eq!(Expression::decompile(&model), "(fn(a, b) => a * b)")
    }

    #[test]
    fn test_define_named_function() {
        let model = SetVariable("add".into(), Box::new(
            Literal(Function {
                params: vec![
                    Parameter::build("a"),
                    Parameter::build("b"),
                ],
                code: Box::new(Plus(Box::new(
                    Variable("a".into())
                ), Box::new(
                    Variable("b".into())
                ))),
            })
        ));
        assert_eq!(Expression::decompile(&model), "add := (fn(a, b) => a + b)")
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

    /// Unit tests
    #[cfg(test)]
    mod pure_tests {
        use crate::arrays::Array;
        use crate::compiler::Compiler;
        use crate::numbers::Numbers::{F64Value, I64Value, U128Value, U64Value};
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
        fn test_to_pure_math_factorial() {
            verify_pure("6¡", Number(U128Value(720)))
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
}