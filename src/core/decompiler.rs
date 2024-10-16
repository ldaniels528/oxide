////////////////////////////////////////////////////////////////////
// Decompiler module
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

use crate::expression::Expression::*;
use crate::expression::*;
use crate::expression::{BitwiseOps, Conditions, Expression};
use crate::parameter::Parameter;

/// Oxide language decompiler - converts an [Expression] to source code.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Decompiler;

impl Decompiler {
    /// Default constructor
    pub fn new() -> Self {
        Self
    }

    pub fn decompile(&self, expr: &Expression) -> String {
        use crate::expression::Expression::*;
        match expr {
            ArrayExpression(items) =>
                format!("[{}]", items.iter().map(|i| self.decompile(i)).collect::<Vec<String>>().join(", ")),
            AsValue(name, expr) =>
                format!("{}: {}", name, self.decompile(expr)),
            BitwiseOp(bitwise) => self.decompile_bitwise(bitwise),
            CodeBlock(items) => self.decompile_code_blocks(items),
            Parameters(parameters) => self.decompile_parameters(parameters),
            Condition(cond) => self.decompile_cond(cond),
            Directive(d) => self.decompile_directives(d),
            Divide(a, b) =>
                format!("{} / {}", self.decompile(a), self.decompile(b)),
            ElementAt(a, b) =>
                format!("{}[{}]", self.decompile(a), self.decompile(b)),
            Extract(a, b) =>
                format!("{}::{}", self.decompile(a), self.decompile(b)),
            Factorial(a) => format!("¡{}", self.decompile(a)),
            Feature { title, scenarios } =>
                format!("feature {} {{\n{}\n}}", title.to_code(), scenarios.iter()
                    .map(|s| s.to_code())
                    .collect::<Vec<_>>()
                    .join("\n")),
            From(a) => format!("from {}", self.decompile(a)),
            FunctionCall { fx, args } =>
                format!("{}({})", self.decompile(fx), self.decompile_list(args)),
            If { condition, a, b } =>
                format!("if {} {}{}", self.decompile(condition), self.decompile(a), b.to_owned()
                    .map(|x| format!(" else {}", self.decompile(&x)))
                    .unwrap_or("".into())),
            Include(path) => format!("include {}", self.decompile(path)),
            JSONExpression(items) =>
                format!("{{{}}}", items.iter()
                    .map(|(k, v)| format!("{}: {}", k, v))
                    .collect::<Vec<String>>()
                    .join(", ")),
            Literal(value) => value.to_code(),
            Minus(a, b) =>
                format!("{} - {}", self.decompile(a), self.decompile(b)),
            Modulo(a, b) =>
                format!("{} % {}", self.decompile(a), self.decompile(b)),
            Multiply(a, b) =>
                format!("{} * {}", self.decompile(a), self.decompile(b)),
            Neg(a) => format!("-({})", self.decompile(a)),
            Ns(a) => format!("ns({})", self.decompile(a)),
            Plus(a, b) =>
                format!("{} + {}", self.decompile(a), self.decompile(b)),
            Pow(a, b) =>
                format!("{} ** {}", self.decompile(a), self.decompile(b)),
            Quarry(job) =>
                match job {
                    Excavation::Construct(i) => self.decompile_infrastructures(i),
                    Excavation::Query(q) => self.decompile_queryables(q),
                    Excavation::Mutate(m) => self.decompile_modifications(m),
                },
            Range(a, b) =>
                format!("{}..{}", self.decompile(a), self.decompile(b)),
            Return(items) =>
                format!("return {}", self.decompile_list(items)),
            Scenario { title, verifications, inherits } => {
                let title = title.to_code();
                let inherits = inherits.to_owned().map(|e| e.to_code()).unwrap_or(String::new());
                let verifications = verifications.iter()
                    .map(|s| s.to_code())
                    .collect::<Vec<_>>()
                    .join("\n");
                format!("scenario {title} {{\n{verifications}\n}}")
            }
            SetVariable(name, value) =>
                format!("{} := {}", name, self.decompile(value)),
            StructureImpl(name, ops) =>
                format!("{} {}", name, self.decompile_code_blocks(ops)),
            Variable(name) => name.to_string(),
            Via(expr) => format!("via {}", self.decompile(expr)),
            While { condition, code } =>
                format!("while {} {}", self.decompile(condition), self.decompile(code)),
            HTTP { method, url, body, headers, multipart } =>
                format!("{} {}{}{}{}", method, self.decompile(url), self.decompile_opt(body), self.decompile_opt(headers), self.decompile_opt(multipart)),
        }
    }

    pub fn decompile_code_blocks(&self, ops: &Vec<Expression>) -> String {
        format!("{{\n{}\n}}", ops.iter().map(|i| self.decompile(i))
            .collect::<Vec<String>>()
            .join("\n"))
    }

    pub fn decompile_bitwise(&self, bitwise: &BitwiseOps) -> String {
        match bitwise {
            BitwiseOps::And(a, b) =>
                format!("{} & {}", self.decompile(a), self.decompile(b)),
            BitwiseOps::Or(a, b) =>
                format!("{} | {}", self.decompile(a), self.decompile(b)),
            BitwiseOps::Xor(a, b) =>
                format!("{} ^ {}", self.decompile(a), self.decompile(b)),
            BitwiseOps::ShiftLeft(a, b) =>
                format!("{} << {}", self.decompile(a), self.decompile(b)),
            BitwiseOps::ShiftRight(a, b) =>
                format!("{} >> {}", self.decompile(a), self.decompile(b)),
        }
    }

    pub fn decompile_cond(&self, cond: &Conditions) -> String {
        use crate::expression::Conditions::*;
        match cond {
            And(a, b) =>
                format!("{} && {}", self.decompile(a), self.decompile(b)),
            Between(a, b, c) =>
                format!("{} between {} and {}", self.decompile(a), self.decompile(b), self.decompile(c)),
            Betwixt(a, b, c) =>
                format!("{} betwixt {} and {}", self.decompile(a), self.decompile(b), self.decompile(c)),
            Contains(a, b) =>
                format!("{} contains {}", self.decompile(a), self.decompile(b)),
            Equal(a, b) =>
                format!("{} == {}", self.decompile(a), self.decompile(b)),
            False => "false".to_string(),
            GreaterThan(a, b) =>
                format!("{} > {}", self.decompile(a), self.decompile(b)),
            GreaterOrEqual(a, b) =>
                format!("{} >= {}", self.decompile(a), self.decompile(b)),
            LessThan(a, b) =>
                format!("{} < {}", self.decompile(a), self.decompile(b)),
            LessOrEqual(a, b) =>
                format!("{} <= {}", self.decompile(a), self.decompile(b)),
            Not(a) => format!("!{}", self.decompile(a)),
            NotEqual(a, b) =>
                format!("{} != {}", self.decompile(a), self.decompile(b)),
            Or(a, b) =>
                format!("{} || {}", self.decompile(a), self.decompile(b)),
            True => "true".to_string(),
        }
    }

    pub fn decompile_column(&self, column: &Parameter) -> String {
        match column.get_param_type() {
            Some(type_decl) if type_decl.trim().is_empty() => column.get_name().to_owned(),
            Some(type_decl) => format!("{}: {}", column.get_name(), type_decl),
            None => column.get_name().to_string()
        }
    }

    pub fn decompile_parameters(&self, columns: &Vec<Parameter>) -> String {
        columns.iter().map(|c| self.decompile_column(c))
            .collect::<Vec<String>>().join(", ")
    }

    pub fn decompile_directives(&self, directive: &Directives) -> String {
        match directive {
            Directives::MustAck(a) => format!("[+] {}", self.decompile(a)),
            Directives::MustDie(a) => format!("[!] {}", self.decompile(a)),
            Directives::MustIgnoreAck(a) => format!("[~] {}", self.decompile(a)),
            Directives::MustNotAck(a) => format!("[-] {}", self.decompile(a)),
        }
    }

    pub fn decompile_if_exists(&self, if_exists: bool) -> String {
        (if if_exists { "if exists " } else { "" }).to_string()
    }

    pub fn decompile_insert_list(&self, fields: &Vec<Expression>, values: &Vec<Expression>) -> String {
        let field_list = fields.iter().map(|f| self.decompile(f)).collect::<Vec<String>>().join(", ");
        let value_list = values.iter().map(|v| self.decompile(v)).collect::<Vec<String>>().join(", ");
        format!("({}) values ({})", field_list, value_list)
    }

    pub fn decompile_limit(&self, opt: &Option<Box<Expression>>) -> String {
        opt.to_owned().map(|x| format!(" limit {}", self.decompile(&x))).unwrap_or("".into())
    }

    pub fn decompile_list(&self, fields: &Vec<Expression>) -> String {
        fields.iter().map(|x| self.decompile(x)).collect::<Vec<String>>().join(", ".into())
    }

    pub fn decompile_cond_opt(&self, opt: &Option<Conditions>) -> String {
        opt.to_owned().map(|i| self.decompile_cond(&i)).unwrap_or("".into())
    }

    pub fn decompile_opt(&self, opt: &Option<Box<Expression>>) -> String {
        opt.to_owned().map(|i| self.decompile(&i)).unwrap_or("".into())
    }

    pub fn decompile_update_list(&self, fields: &Vec<Expression>, values: &Vec<Expression>) -> String {
        fields.iter().zip(values.iter()).map(|(f, v)|
            format!("{} = {}", self.decompile(f), self.decompile(v))).collect::<Vec<String>>().join(", ")
    }

    pub fn decompile_excavations(&self, excavation: &Excavation) -> String {
        match excavation {
            Excavation::Construct(i) => self.decompile_infrastructures(i),
            Excavation::Query(q) => self.decompile_queryables(q),
            Excavation::Mutate(m) => self.decompile_modifications(m),
        }
    }

    pub fn decompile_infrastructures(&self, infra: &Infrastructure) -> String {
        match infra {
            Infrastructure::Create { path, entity } =>
                match entity {
                    CreationEntity::IndexEntity { columns } =>
                        format!("create index {} [{}]", self.decompile(path), self.decompile_list(columns)),
                    CreationEntity::TableEntity { columns, from } =>
                        format!("create table {} ({})", self.decompile(path), self.decompile_parameters(columns)),
                }
            Infrastructure::Declare(entity) =>
                match entity {
                    CreationEntity::IndexEntity { columns } =>
                        format!("index [{}]", self.decompile_list(columns)),
                    CreationEntity::TableEntity { columns, from } =>
                        format!("table({})", self.decompile_parameters(columns)),
                }
            Infrastructure::Drop(target) => {
                let (kind, path) = match target {
                    MutateTarget::IndexTarget { path } => ("index", path),
                    MutateTarget::TableTarget { path } => ("table", path),
                };
                format!("drop {} {}", kind, self.decompile(path))
            }
        }
    }

    pub fn decompile_modifications(&self, expr: &Mutation) -> String {
        match expr {
            Mutation::Append { path, source } =>
                format!("append {} {}", self.decompile(path), self.decompile(source)),
            Mutation::Compact { path } =>
                format!("compact {}", self.decompile(path)),
            Mutation::Delete { path, condition, limit } =>
                format!("delete from {} where {}{}", self.decompile(path), self.decompile_cond_opt(condition), self.decompile_opt(limit)),
            Mutation::IntoNs(a, b) =>
                format!("{} ~> {}", self.decompile(a), self.decompile(b)),
            Mutation::Overwrite { path, source, condition, limit } =>
                format!("overwrite {} {}{}{}", self.decompile(path), self.decompile(source),
                        condition.to_owned().map(|e| format!(" where {}", self.decompile_cond(&e))).unwrap_or("".into()),
                        limit.to_owned().map(|e| format!(" limit {}", self.decompile(&e))).unwrap_or("".into()),
                ),
            Mutation::Scan { path } =>
                format!("scan {}", self.decompile(path)),
            Mutation::Truncate { path, limit } =>
                format!("truncate {}{}", self.decompile(path), self.decompile_limit(limit)),
            Mutation::Undelete { path, condition, limit } =>
                format!("undelete from {} where {}{}", self.decompile(path), self.decompile_cond_opt(condition), self.decompile_opt(limit)),
            Mutation::Update { path, source, condition, limit } =>
                format!("update {} {} where {}{}", self.decompile(path), self.decompile(source), self.decompile_cond_opt(condition),
                        limit.to_owned().map(|e| format!(" limit {}", self.decompile(&e))).unwrap_or("".into()), ),
        }
    }

    pub fn decompile_queryables(&self, expr: &Queryable) -> String {
        match expr {
            Queryable::Describe(a) =>
                format!("describe {}", self.decompile(a)),
            Queryable::Limit { from: a, limit: b } =>
                format!("{} limit {}", self.decompile(a), self.decompile(b)),
            Queryable::Where { from, condition } =>
                format!("{} where {}", self.decompile(from), self.decompile_cond(condition)),
            Queryable::Reverse(a) => format!("reverse {}", self.decompile(a)),
            Queryable::Select { fields, from, condition, group_by, having, order_by, limit } =>
                format!("select {}{}{}{}{}{}{}", self.decompile_list(fields),
                        from.to_owned().map(|e| format!(" from {}", self.decompile(&e))).unwrap_or("".into()),
                        condition.to_owned().map(|c| format!(" where {}", self.decompile_cond(&c))).unwrap_or("".into()),
                        limit.to_owned().map(|e| format!(" limit {}", self.decompile(&e))).unwrap_or("".into()),
                        group_by.to_owned().map(|items| format!(" group by {}", items.iter().map(|e| self.decompile(e)).collect::<Vec<String>>().join(", "))).unwrap_or("".into()),
                        having.to_owned().map(|e| format!(" having {}", self.decompile(&e))).unwrap_or("".into()),
                        order_by.to_owned().map(|e| format!(" order by {}", self.decompile_list(&e))).unwrap_or("".into()),
                ),
        }
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::expression::CreationEntity::{IndexEntity, TableEntity};
    use crate::expression::Expression::{AsValue, Literal};
    use crate::numbers::NumberValue::I64Value;
    use crate::typed_values::TypedValue::{Function, Number, StringValue};

    use super::*;

    #[test]
    fn test_alias() {
        let model = AsValue("symbol".to_string(), Box::new(Literal(StringValue("ABC".into()))));
        assert_eq!(Decompiler::new().decompile(&model), r#"symbol: "ABC""#);
    }

    #[test]
    fn test_array_declaration() {
        let model = ArrayExpression(vec![
            Literal(Number(I64Value(2))), Literal(Number(I64Value(5))), Literal(Number(I64Value(8))),
            Literal(Number(I64Value(7))), Literal(Number(I64Value(4))), Literal(Number(I64Value(1))),
        ]);
        assert_eq!(Decompiler::new().decompile(&model), "[2, 5, 8, 7, 4, 1]")
    }

    #[test]
    fn test_array_indexing() {
        let model = ElementAt(
            Box::new(ArrayExpression(vec![
                Literal(Number(I64Value(7))), Literal(Number(I64Value(5))), Literal(Number(I64Value(8))),
                Literal(Number(I64Value(2))), Literal(Number(I64Value(4))), Literal(Number(I64Value(1))),
            ])),
            Box::new(Literal(Number(I64Value(3)))));
        assert_eq!(Decompiler::new().decompile(&model), "[7, 5, 8, 2, 4, 1][3]")
    }

    #[test]
    fn test_bitwise_and() {
        let model = BitwiseOp(BitwiseOps::And(
            Box::new(Literal(Number(I64Value(20)))),
            Box::new(Literal(Number(I64Value(3))))
        ));
        assert_eq!(Decompiler::new().decompile(&model), "20 & 3")
    }

    #[test]
    fn test_bitwise_or() {
        let model = BitwiseOp(BitwiseOps::Or(
            Box::new(Literal(Number(I64Value(20)))),
            Box::new(Literal(Number(I64Value(3))))
        ));
        assert_eq!(Decompiler::new().decompile(&model), "20 | 3")
    }

    #[test]
    fn test_bitwise_shl() {
        let model = BitwiseOp(BitwiseOps::ShiftLeft(
            Box::new(Literal(Number(I64Value(20)))),
            Box::new(Literal(Number(I64Value(3))))
        ));
        assert_eq!(Decompiler::new().decompile(&model), "20 << 3")
    }

    #[test]
    fn test_bitwise_shr() {
        let model = BitwiseOp(BitwiseOps::ShiftRight(
            Box::new(Literal(Number(I64Value(20)))),
            Box::new(Literal(Number(I64Value(3))))
        ));
        assert_eq!(Decompiler::new().decompile(&model), "20 >> 3")
    }

    #[test]
    fn test_bitwise_xor() {
        let model = BitwiseOp(BitwiseOps::Xor(
            Box::new(Literal(Number(I64Value(20)))),
            Box::new(Literal(Number(I64Value(3))))
        ));
        assert_eq!(Decompiler::new().decompile(&model), "20 ^ 3")
    }

    #[test]
    fn test_define_anonymous_function() {
        let model = Literal(Function {
            params: vec![
                Parameter::new("a", None, None),
                Parameter::new("b", None, None),
            ],
            code: Box::new(Multiply(Box::new(
                Variable("a".into())
            ), Box::new(
                Variable("b".into())
            ))),
        });
        assert_eq!(Decompiler::new().decompile(&model), "(fn(a, b) => a * b)")
    }

    #[test]
    fn test_define_named_function() {
        let model = SetVariable("add".to_string(), Box::new(
            Literal(Function {
                params: vec![
                    Parameter::new("a", None, None),
                    Parameter::new("b", None, None),
                ],
                code: Box::new(Plus(Box::new(
                    Variable("a".into())
                ), Box::new(
                    Variable("b".into())
                ))),
            })
        ));
        assert_eq!(Decompiler::new().decompile(&model), "add := (fn(a, b) => a + b)")
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
        assert_eq!(Decompiler::new().decompile(&model), "f(2, 3)")
    }

    #[test]
    fn test_create_index_in_namespace() {
        let model = Quarry(Excavation::Construct(Infrastructure::Create {
            path: Box::new(Ns(Box::new(Literal(StringValue("compiler.create.stocks".into()))))),
            entity: IndexEntity {
                columns: vec![
                    Variable("symbol".into()),
                    Variable("exchange".into()),
                ],
            },
        }));
        assert_eq!(
            Decompiler::new().decompile(&model),
            r#"create index ns("compiler.create.stocks") [symbol, exchange]"#)
    }

    #[test]
    fn test_create_table_in_namespace() {
        let ns_path = "compiler.create.stocks";
        let model = Quarry(Excavation::Construct(Infrastructure::Create {
            path: Box::new(Ns(Box::new(Literal(StringValue(ns_path.into()))))),
            entity: TableEntity {
                columns: vec![
                    Parameter::new("symbol", Some("String(8)".into()), Some("ABC".into())),
                    Parameter::new("exchange", Some("String(8)".into()), Some("NYSE".into())),
                    Parameter::new("last_sale", Some("f64".into()), Some("23.54".into())),
                ],
                from: None,
            },
        }));
        assert_eq!(
            Decompiler::new().decompile(&model),
            r#"create table ns("compiler.create.stocks") (symbol: String(8), exchange: String(8), last_sale: f64)"#)
    }

    #[test]
    fn test_declare_table() {
        let model = Quarry(Excavation::Construct(Infrastructure::Declare(TableEntity {
            columns: vec![
                Parameter::new("symbol", Some("String(8)".into()), None),
                Parameter::new("exchange", Some("String(8)".into()), None),
                Parameter::new("last_sale", Some("f64".into()), None),
            ],
            from: None,
        })));
        assert_eq!(
            Decompiler::new().decompile(&model),
            r#"table(symbol: String(8), exchange: String(8), last_sale: f64)"#)
    }
}