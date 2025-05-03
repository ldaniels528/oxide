#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Compiler class
////////////////////////////////////////////////////////////////////

use crate::byte_code_compiler::ByteCodeCompiler;
use crate::data_types::DataType;
use crate::data_types::DataType::DynamicType;
use crate::errors::Errors::{ExactNear, SyntaxError, TypeMismatch};
use crate::errors::TypeMismatchErrors::{CodeBlockExpected, ParameterExpected, VariableExpected};
use crate::errors::{throw, SyntaxErrors};
use crate::expression::Conditions::*;
use crate::expression::CreationEntity::{IndexEntity, TableEntity, TableFnEntity};
use crate::expression::DatabaseOps::{Mutation, Queryable};
use crate::expression::Expression::*;
use crate::expression::MutateTarget::TableTarget;
use crate::expression::Mutations::{Create, Declare, Drop, Undelete};
use crate::expression::Queryables::Select;
use crate::expression::*;
use crate::inferences::Inferences;
use crate::numbers::Numbers::*;
use crate::parameter::Parameter;
use crate::structures::HardStructure;
use crate::structures::Structures::Hard;
use crate::token_slice::TokenSlice;
use crate::tokens::Token::*;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{Null, Number, StringValue, Structured};
use serde::{Deserialize, Serialize};
use shared_lib::fail;
use std::convert::From;
use std::fs;
use std::io::Read;

/// Oxide language compiler - converts source code into [Expression]s.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Compiler {
    stack: Vec<Expression>,
}

impl Compiler {

    ////////////////////////////////////////////////////////////////
    // static methods
    ////////////////////////////////////////////////////////////////

    /// compiles the source code into a [Vec<Expression>]; a graph representing
    /// the program's executable code
    pub fn build(source_code: &str) -> std::io::Result<Expression> {
        let (code, _) = Self::new().compile(TokenSlice::from_string(source_code))?;
        Ok(code)
    }

    /// Compiles the provided source code and saves it as binary to the provided path
    pub fn build_and_save(path: &str, source_code: &str) -> std::io::Result<Expression> {
        let expr = Self::build(source_code)?;
        Self::save(path, &expr)?;
        Ok(expr)
    }

    /// Loads an Oxide binary from disk
    pub fn load(path: &str) -> std::io::Result<Expression> {
        let bytes = fs::read(path)?;
        let expr = ByteCodeCompiler::decode(&bytes);
        Ok(expr)
    }

    /// creates a new [Compiler] instance
    pub fn new() -> Self {
        Compiler {
            stack: Vec::new(),
        }
    }

    /// Compiles the provided source code and saves it as binary to the provided path
    pub fn save(path: &str, expr: &Expression) -> std::io::Result<()> {
        let bytes = ByteCodeCompiler::encode(&expr)?;
        fs::write(path, bytes)?;
        Ok(())
    }

    ////////////////////////////////////////////////////////////////
    // instance methods
    ////////////////////////////////////////////////////////////////

    /// compiles the entire [TokenSlice] into an [Expression]
    pub fn compile(&mut self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        // consume the entire iterator
        let mut models = Vec::new();
        let mut ts = ts;
        while ts.has_more() {
            let (expr, tts) = self.compile_next(ts)?;
            models.push(expr);
            ts = tts;
        }

        // return the instruction
        match models {
            ops if ops.len() == 1 => Ok((ops[0].to_owned(), ts)),
            ops => Ok((CodeBlock(ops), ts))
        }
    }

    /// compiles the next [TokenSlice] into an [Expression]
    pub fn compile_next(&mut self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        let (expr, ts) = match ts.next() {
            (Some(Operator { text, precedence, .. }), ts) =>
                self.parse_operator(ts, text, precedence),
            (Some(Backticks { text, .. }), ts) =>
                Ok((Variable(text.into()), ts)),
            (Some(Atom { .. }), _) => self.parse_keyword(ts),
            (Some(DoubleQuoted { text, .. } |
                  SingleQuoted { text, .. }), ts) =>
                Ok((Literal(StringValue(text.into())), ts)),
            (Some(Numeric { text, .. }), ts) =>
                Ok((Literal(TypedValue::from_numeric(text.as_str())?), ts)),
            (Some(URL { text, .. }), ts) =>
                Ok((Literal(StringValue(text.into())), ts)),
            (None, ts) => throw(ExactNear("Unexpected end of input".into(), ts.current()))
        }?;

        // handle postfix operator?
        match ts.next() {
            // keyword operator "between"
            (Some(Atom { text: kw, .. }), ts) if kw == "between" => {
                let (expr1, ts) = self.compile_next(ts)?;
                let ts = ts.expect("and")?;
                let (expr2, ts) = self.compile_next(ts)?;
                Ok((Condition(Between(Box::new(expr), Box::new(expr1), Box::new(expr2))), ts))
            }
            // keyword operator "betwixt"
            (Some(Atom { text: kw, .. }), ts) if kw == "betwixt" => {
                let (expr1, ts) = self.compile_next(ts)?;
                let ts = ts.expect("and")?;
                let (expr2, ts) = self.compile_next(ts)?;
                Ok((Condition(Betwixt(Box::new(expr), Box::new(expr1), Box::new(expr2))), ts))
            }
            // keyword operator "is"
            (Some(Atom { text: kw, .. }), ts) if kw == "is" => {
                let (expr1, ts) = self.compile_next(ts)?;
                Ok((Condition(Equal(Box::new(expr), Box::new(expr1))), ts))
            }
            // keyword operator "isnt"
            (Some(Atom { text: kw, .. }), ts) if kw == "isnt" => {
                let (expr1, ts) = self.compile_next(ts)?;
                Ok((Condition(NotEqual(Box::new(expr), Box::new(expr1))), ts))
            }
            // keyword operator "like"
            (Some(Atom { text: kw, .. }), ts) if kw == "like" => {
                let (expr1, ts) = self.compile_next(ts)?;
                Ok((Condition(Like(Box::new(expr), Box::new(expr1))), ts))
            }
            // non-barrier operator: "," | ";"
            (Some(Operator { is_barrier, .. }), _) if !is_barrier => {
                self.push(expr);
                self.compile_next(ts)
            }
            // array index: "items[n]"
            (Some(Operator { text, .. }), ts) if text == "[" && ts.is_previous_adjacent() => {
                let (index, ts) = self.compile_next(ts)?;
                let ts = ts.expect("]")?;
                let model = ElementAt(Box::new(expr), Box::new(index));
                if ts.has_more() {
                    self.push(model);
                    self.compile_next(ts)
                } else { Ok((model, ts)) }
            }
            // anything else is passed through...
            _ => Ok((expr, ts))
        }
    }

    /// compiles the [TokenSlice] into a leading single-parameter [Conditions] (e.g. !true)
    fn parse_condition_1a(
        &mut self,
        ts: TokenSlice,
        f: fn(Box<Expression>) -> Conditions,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        match self.compile_next(ts)? {
            (expr, ts) => Ok((Condition(f(Box::new(expr))), ts)),
            //(_, ts) => throw("Boolean expression expected", &ts)
        }
    }

    /// compiles the [TokenSlice] into a leading single-parameter [Expression] (e.g. !true)
    fn parse_expression_1a(
        &mut self,
        ts: TokenSlice,
        f: fn(Box<Expression>) -> Expression,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let (expr, ts) = self.compile_next(ts)?;
        Ok((f(Box::new(expr)), ts))
    }

    /// compiles the [TokenSlice] into a two-parameter [Expression]
    fn parse_expression_1b(
        &mut self,
        ts: TokenSlice,
        f: fn(Box<Expression>) -> Expression,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        match self.pop() {
            None => throw(ExactNear("Expected an expression".into(), ts.current())),
            Some(expr0) => Ok((f(Box::new(expr0)), ts))
        }
    }

    /// compiles the [TokenSlice] into a two-parameter [Expression]
    fn parse_conditional_2a(
        &mut self,
        ts: TokenSlice,
        expr0: Expression,
        f: fn(Box<Expression>, Box<Expression>) -> Conditions,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let (expr1, ts) = self.compile_next(ts)?;
        Ok((Condition(f(Box::new(expr0), Box::new(expr1))), ts))
    }

    /// compiles the [TokenSlice] into a two-parameter [Expression]
    fn parse_expression_2a(
        &mut self,
        ts: TokenSlice,
        expr0: Expression,
        f: fn(Box<Expression>, Box<Expression>) -> Expression,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let (expr1, ts) = self.compile_next(ts)?;
        Ok((f(Box::new(expr0), Box::new(expr1)), ts))
    }

    /// compiles the [TokenSlice] into a two-parameter [Expression]
    fn parse_expression_2b(
        &mut self, ts: TokenSlice,
        expr1: Expression,
        f: fn(Box<Expression>, Box<Expression>) -> Expression,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        match self.pop() {
            None => fail(ExactNear("expected an expression".into(), ts.current()).to_string()),
            Some(expr0) => Ok((f(Box::new(expr0), Box::new(expr1)), ts))
        }
    }

    /// compiles the [TokenSlice] into a name-value parameter [Expression]
    fn parse_expression_2nv(
        &mut self,
        ts: TokenSlice,
        expr0: Expression,
        f: fn(String, Box<Expression>) -> Expression,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        match expr0 {
            Literal(StringValue(name)) => {
                let (expr1, ts) = self.compile_next(ts)?;
                Ok((f(name, Box::new(expr1)), ts))
            }
            Variable(name) => {
                let (expr1, ts) = self.compile_next(ts)?;
                Ok((f(name, Box::new(expr1)), ts))
            }
            _ => throw(ExactNear("an identifier name was expected".into(), ts.current()))
        }
    }

    /// compiles the [TokenSlice] into a name-value parameter [Expression]
    fn parse_expression_set(
        &mut self,
        ts: TokenSlice,
        expr0: Expression,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let (expr1, ts) = self.compile_next(ts)?;
        match expr0 {
            ArrayExpression(items) =>
                Ok((SetVariables(Box::new(ArrayExpression(items)), Box::new(expr1)), ts)),
            TupleExpression(items) =>
                Ok((SetVariables(Box::new(TupleExpression(items)), Box::new(expr1)), ts)),
            Variable(name) =>
                Ok((SetVariable(name, Box::new(expr1)), ts)),
            _ => throw(ExactNear("an identifier name was expected".into(), ts.current()))
        }
    }

    /// Parses an identifier.
    fn parse_identifier(
        &mut self,
        ts: TokenSlice,
        f: fn(String) -> Expression,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        match ts.next() {
            (Some(
                Atom { text: name, .. } |
                Backticks { text: name, .. }), ts) => Ok((f(name), ts)),
            (_, ts) => throw(ExactNear("Invalid identifier".into(), ts.current()))
        }
    }

    /// Parses reserved words (i.e. keyword)
    fn parse_keyword(&mut self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        if let (Some(Atom { text, .. }), nts) = ts.next() {
            match text.as_str() {
                "[!]" => self.parse_expression_1a(nts, |e| Directive(Directives::MustDie(e))),
                "[+]" => self.parse_expression_1a(nts, |e| Directive(Directives::MustAck(e))),
                "[-]" => self.parse_expression_1a(nts, |e| Directive(Directives::MustNotAck(e))),
                "[~]" => self.parse_expression_1a(nts, |e| Directive(Directives::MustIgnoreAck(e))),
                "append" => self.parse_keyword_append(nts),
                "create" => self.parse_keyword_create(nts),
                "delete" => self.parse_keyword_delete(nts),
                "DELETE" => self.parse_keyword_http(ts),
                "drop" => self.parse_mutate_target(nts, |m| DatabaseOp(Mutation(Drop(m)))),
                "false" => Ok((FALSE, nts)),
                "Feature" => self.parse_keyword_feature(nts),
                "fn" => self.parse_keyword_fn(nts),
                "foreach" => self.parse_keyword_foreach(nts),
                "from" => {
                    let (from, ts) = self.parse_expression_1a(nts, Expression::From)?;
                    self.parse_queryable(from, ts)
                }
                "GET" => self.parse_keyword_http(ts),
                "HEAD" => self.parse_keyword_http(ts),
                "HTTP" => self.parse_keyword_http(nts),
                "if" => self.parse_keyword_if(nts),
                "import" => self.parse_keyword_import(nts),
                "include" => self.parse_expression_1a(nts, Expression::Include),
                "limit" => throw(ExactNear("`from` is expected before `limit`: from stocks limit 5".into(), nts.current())),
                "mod" => self.parse_keyword_mod(nts),
                "NaN" => Ok((Literal(Number(NaNValue)), nts)),
                "new" => self.parse_expression_1a(nts, New),
                "ns" => self.parse_expression_1a(nts, Ns),
                "null" => Ok((NULL, nts)),
                "overwrite" => self.parse_keyword_overwrite(nts),
                "PATCH" => self.parse_keyword_http(ts),
                "POST" => self.parse_keyword_http(ts),
                "PUT" => self.parse_keyword_http(ts),
                "Scenario" => self.parse_keyword_scenario(nts),
                "select" => self.parse_keyword_select(nts),
                "Struct" => self.parse_keyword_struct(nts),
                "table" => self.parse_keyword_table(nts),
                "true" => Ok((TRUE, nts)),
                "typedef" => self.parse_keyword_type_decl(nts),
                "undefined" => Ok((UNDEFINED, nts)),
                "undelete" => self.parse_keyword_undelete(nts),
                "update" => self.parse_keyword_update(nts),
                "via" => self.parse_expression_1a(nts, Via),
                "where" => throw(ExactNear("`from` is expected before `where`: from stocks where last_sale < 1.0".into(), nts.current())),
                "while" => self.parse_keyword_while(nts),
                name => self.expect_function_call_or_variable(name, nts)
            }
        } else { fail("Unexpected end of input") }
    }

    /// Appends a new row to a table
    /// ex: append stocks select symbol: "ABC", exchange: "NYSE", last_sale: 0.1008
    fn parse_keyword_append(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let (table, ts) = self.compile_next(ts)?;
        let (source, ts) = self.compile_next(ts)?;
        Ok((DatabaseOp(Mutation(Mutations::Append { path: Box::new(table), source: Box::new(source) })), ts))
    }

    /// Creates a database object (e.g., table or index)
    fn parse_keyword_create(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        if let (Some(t), ts) = ts.next() {
            match t.get_raw_value().as_str() {
                "index" => self.parse_keyword_create_index(ts),
                "table" => self.parse_keyword_create_table(ts),
                name => throw(SyntaxError(SyntaxErrors::TypeIdentifierExpected(name.to_string())))
            }
        } else { fail("Unexpected end of input") }
    }

    fn parse_keyword_create_index(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let (index, ts) = self.compile_next(ts)?;
        let ts = ts.expect("on")?;
        if let (ArrayExpression(columns), ts) = self.compile_next(ts.to_owned())? {
            Ok((DatabaseOp(Mutation(Create {
                path: Box::new(index),
                entity: IndexEntity { columns },
            })), ts))
        } else {
            throw(ExactNear("Columns expected".into(), ts.current()))
        }
    }

    /// Parses a table creation expression
    fn parse_keyword_create_table(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        // create table `stocks` (name: String, ..)
        let (table, ts) = self.compile_next(ts)?;
        if ts.is("fn") {
            let ts = ts.expect("fn")?;
            let (fx, ts) = self.expect_function_parameters_and_body(None, ts)?;
            Ok((DatabaseOp(Mutation(Create {
                path: Box::new(table),
                entity: TableFnEntity { fx: Box::new(fx) },
            })), ts))
        } else if let (Parameters(columns), ts) = self.expect_parameters(ts.to_owned())? {
            // from { symbol: "ABC", exchange: "NYSE", last_sale: 67.89 }
            let (from, ts) =
                if ts.is("from") {
                    let ts = ts.expect("from")?;
                    let (from, ts) = self.compile_next(ts)?;
                    (Some(Box::new(from)), ts)
                } else {
                    (None, ts)
                };
            Ok((DatabaseOp(Mutation(Create {
                path: Box::new(table),
                entity: TableEntity { columns, from },
            })), ts))
        } else {
            throw(ExactNear("Expected column definitions".into(), ts.current()))
        }
    }

    /// SQL Delete statement.
    /// ex: delete from stocks where last_sale > 1.00
    fn parse_keyword_delete(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let (from, ts) = self.next_keyword_expr("from", ts)?;
        let from = from.expect("Expected keyword 'from'");
        let (condition, ts) = self.next_keyword_cond("where", ts)?;
        let (limit, ts) = self.next_keyword_expr("limit", ts)?;
        Ok((DatabaseOp(Mutation(Mutations::Delete { path: Box::new(from), condition, limit: limit.map(Box::new) })), ts))
    }

    fn parse_keyword_feature(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let (title, ts) = self.compile_next(ts)?;
        let ts = ts.expect("{")?;
        let (code, ts) = self.expect_curly_brackets(ts)?;
        let scenarios = match code {
            CodeBlock(scenarios) => scenarios,
            StructureExpression(tuples) => tuples.iter()
                .map(|(name, expression)| Scenario {
                    title: Box::new(Literal(StringValue(name.to_owned()))),
                    verifications: match expression.to_owned() {
                        CodeBlock(ops) => ops,
                        z => vec![z]
                    },
                })
                .collect::<Vec<_>>(),
            other => {
                println!("parse_keyword_feature: other {:?}", other);
                return throw(TypeMismatch(CodeBlockExpected(other.to_code())));
            }
        };
        Ok((Feature {
            title: Box::new(title),
            scenarios,
        }, ts))
    }

    fn parse_keyword_scenario(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let (title, ts) = self.compile_next(ts)?;
        let ts = ts.expect("{")?;
        let (code, ts) = self.expect_curly_brackets(ts)?;
        match code {
            CodeBlock(verifications) =>
                Ok((Scenario { title: Box::new(title), verifications }, ts)),
            other => {
                println!("parse_keyword_scenario: other {:?}", other);
                throw(TypeMismatch(CodeBlockExpected(other.to_code())))
            }
        }
    }

    /// Builds a language model from a function variant
    /// ex: f := fn (x, y) => x + y
    fn parse_keyword_fn(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        // parse the function
        match ts.next() {
            // anonymous function? e.g., fn (x, y) => x + y
            (Some(Operator { text: symbol, .. }), _) if symbol == "(" =>
                self.expect_function_parameters_and_body(None, ts),
            // named function? e.g., fn add(x, y) => x + y
            (Some(Atom { text: name, .. }), ts) =>
                self.expect_function_parameters_and_body(Some(name), ts),
            // unrecognized
            _ => throw(ExactNear("Function name expected".into(), ts.current()))
        }
    }

    /// "foreach" expression
    /// ex: foreach item in items { ... }
    fn parse_keyword_foreach(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        // foreach [item] in [items] [block]
        match self.compile_next(ts)? {
            (Variable(name), ts) => {
                let ts = ts.expect("in")?;
                let (items, ts) = self.compile_next(ts)?;
                let (block, ts) = self.compile_next(ts)?;
                Ok((ForEach(name, Box::new(items), Box::new(block)), ts))
            }
            (_, ts) => throw(TypeMismatch(VariableExpected(ts.current())))
        }
    }

    /// Parses an HTTP expression.
    /// ## Examples
    /// ### URL-Based Request
    /// ```http
    /// GET http://localhost:9000/quotes/AAPL/NYSE
    /// ```
    /// ### Structured Request
    /// ```http
    /// POST {
    ///     url: http://localhost:8080/machine/append/stocks
    ///     body: stocks
    ///     headers: { "Content-Type": "application/json" }
    /// }
    /// ```
    /// ### Hybrid Request
    /// ```http
    /// POST http://localhost:8080/machine/append/stocks
    ///   <~ { body: stocks, headers: {"Content-Type": "application/json"}}
    /// ```
    /// # Errors
    /// Returns an `std::io::Result` in case of parsing failures.
    /// # Parameters
    /// - `ts`: A slice of tokens representing the HTTP expression.
    /// # Returns
    /// - A tuple containing the parsed `Expression` and the remaining `TokenSlice`.
    fn parse_keyword_http(&mut self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        if let (Some(Atom { text: method, .. }), ts) = ts.next() {
            let (expr, ts) = self.compile_next(ts.clone())?;
            Ok((HTTP {
                method,
                url_or_object: Box::new(expr),
            }, ts))
        } else {
            throw(ExactNear("HTTP method expected: DELETE, GET, HEAD, PATCH, POST or PUT".into(), ts.current()))
        }
    }

    /// Builds a language model from an if expression
    /// ex: if (x > 5) x else 5
    fn parse_keyword_if(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let (condition, ts) = self.compile_next(ts)?;
        let (a, ts) = self.compile_next(ts)?;
        let (b, ts) = self.next_keyword_expr("else", ts)?;
        Ok((If {
            condition: Box::new(condition),
            a: Box::new(a),
            b: b.map(Box::new),
        }, ts))
    }

    /// Builds an import statement:
    ///   ex: import "os"
    ///   ex: import str::format
    ///   ex: import oxide::[eval, serve, version]
    ///   ex: import "os", str::format, oxide::[eval, serve, version]
    fn parse_keyword_import(
        &mut self,
        mut ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let mut ops = Vec::new();
        let mut is_done = false;
        while !is_done {
            let (expr, ts1) = self.compile_next(ts)?;
            ts = ts1;
            match expr {
                // import 'cnv'
                Literal(StringValue(pkg)) => ops.push(ImportOps::Everything(pkg)),
                // import vm
                Variable(pkg) => ops.push(ImportOps::Everything(pkg)),
                // import www::serve | oxide::[eval, serve, version]
                ColonColon(a, b) => {
                    match (*a, *b) {
                        // import www::serve
                        (Variable(pkg), Variable(func)) =>
                            ops.push(ImportOps::Selection(pkg, vec![func])),
                        // import oxide::[eval, serve, version]
                        (Variable(pkg), ArrayExpression(items)) => {
                            let mut func_list = Vec::new();
                            for item in items {
                                match item {
                                    Literal(StringValue(name)) => func_list.push(name),
                                    Variable(name) => func_list.push(name),
                                    other => return throw(SyntaxError(SyntaxErrors::TypeIdentifierExpected(other.to_code())))
                                }
                            }
                            ops.push(ImportOps::Selection(pkg, func_list))
                        }
                        (a, ..) => return throw(SyntaxError(SyntaxErrors::TypeIdentifierExpected(a.to_code())))
                    }
                }
                // syntax error
                other => return throw(SyntaxError(SyntaxErrors::TypeIdentifierExpected(other.to_code())))
            }

            // if there's a comma, keep going
            is_done = !ts.is(",");
            if !is_done {
                ts = ts.skip()
            }
        }
        Ok((Import(ops), ts))
    }

    /// Translates a `mod` expression into an [Expression]
    /// ex:
    /// mod abc {
    ///     fn hello() => "hello"
    /// }
    /// abc::hello()
    fn parse_keyword_mod(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let (name, ts) = match self.compile_next(ts)? {
            (Variable(name), ts) => (name, ts),
            (_, ts) => return throw(TypeMismatch(VariableExpected(ts.current())))
        };
        match self.compile_next(ts)? {
            (CodeBlock(ops), ts) => Ok((Module(name, ops), ts)),
            (_, ts) => throw(TypeMismatch(CodeBlockExpected(ts.current().get_raw_value())))
        }
    }

    /// Builds a language model from an OVERWRITE statement:
    /// ex: overwrite stocks
    ///         via {symbol: "ABC", exchange: "NYSE", last_sale: 0.2308}
    ///         where symbol == "ABC"
    ///         limit 5
    fn parse_keyword_overwrite(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let (table, ts) = self.compile_next(ts)?;
        let (source, ts) = self.compile_next(ts)?;
        let (condition, ts) = match self.next_keyword_expr("where", ts)? {
            (Some(Condition(cond)), ts) => (cond, ts),
            (.., ts) => return throw(ExactNear("Expected a boolean expression".into(), ts.current()))
        };
        let (limit, ts) = self.next_keyword_expr("limit", ts)?;
        Ok((DatabaseOp(Mutation(Mutations::Overwrite {
            path: Box::new(table),
            source: Box::new(source),
            condition: Some(condition),
            limit: limit.map(Box::new),
        })), ts))
    }

    /// Builds a language model from a SELECT statement:
    /// ex: select sum(last_sale) from stocks group by exchange
    fn parse_keyword_select(&mut self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        let (fields, ts) = self.next_expression_list(ts)?;
        let fields = fields.expect("At least one field is required");
        let (from, ts) = self.next_keyword_expr("from", ts)?;
        let (condition, ts) = self.next_keyword_cond("where", ts)?;
        let (group_by, ts) = self.next_keyword_expression_list("group", "by", ts)?;
        let (having, ts) = self.next_keyword_expr("having", ts)?;
        let (order_by, ts) = self.next_keyword_expression_list("order", "by", ts)?;
        let (limit, ts) = self.next_keyword_expr("limit", ts)?;
        Ok((DatabaseOp(Queryable(Select {
            fields,
            from: from.map(Box::new),
            condition,
            group_by,
            having: having.map(Box::new),
            order_by,
            limit: limit.map(Box::new),
        })), ts))
    }

    /// Builds a language model from a 'struct' statement:
    /// ex: Struct(symbol: String(8), exchange: String(8), last_sale: f64)
    /// ex: Struct(symbol: String(8) = "TRX", exchange: String(8) = "AMEX", last_sale: f64 = 17.69)
    fn parse_keyword_struct(&mut self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        if let (Parameters(fields), ts) = self.expect_parameters(ts.to_owned())? {
            Ok((Literal(Structured(Hard(HardStructure::from_parameters(fields)))), ts))
        } else {
            throw(ExactNear("Expected column definitions".into(), ts.current()))
        }
    }

    /// Builds a language model from a table expression.
    /// ex: stocks := table (symbol: String(8), exchange: String(8), last_sale: f64)
    fn parse_keyword_table(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        // detect whether this is a table function
        let mut ts = ts;
        let is_function = ts.is("fn");
        if is_function {
            let ats = ts.expect("fn")?;
            ts = ats;
        }

        if let (Parameters(params), ts) = self.expect_parameters(ts.to_owned())? {
            // from { symbol: "ABC", exchange: "NYSE", last_sale: 67.89 }
            let (from, ts) = if ts.is("from") {
                let ts = ts.expect("from")?;
                let (from, ts) = self.compile_next(ts)?;
                (Some(Box::new(from)), ts)
            } else {
                (None, ts)
            };
            if is_function {
                Ok((DatabaseOp(Mutation(Declare(TableEntity { columns: params, from }))), ts))
            } else {
                Ok((DatabaseOp(Mutation(Declare(TableEntity { columns: params, from }))), ts))
            }
        } else {
            throw(ExactNear("Expected column definitions".into(), ts.current()))
        }
    }

    /// Builds a type declaration
    /// ex: typedef(String(80))
    fn parse_keyword_type_decl(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let (expr, ts) = self.compile_next(ts)?;
        Ok((TypeDef(Box::new(expr)), ts))
    }

    /// Builds a language model from an UNDELETE statement:
    /// ex: undelete from stocks where symbol == "BANG"
    fn parse_keyword_undelete(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let (from, ts) = self.next_keyword_expr("from", ts)?;
        let from = from.expect("Expected keyword 'from'");
        let (condition, ts) = self.next_keyword_cond("where", ts)?;
        let (limit, ts) = self.next_keyword_expr("limit", ts)?;
        Ok((DatabaseOp(Mutation(Undelete { path: Box::new(from), condition, limit: limit.map(Box::new) })), ts))
    }

    /// Builds a language model from an UPDATE statement:
    /// ex: update stocks set last_sale = 0.45 where symbol == "BANG"
    fn parse_keyword_update(&mut self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        let (table, ts) = self.compile_next(ts)?;
        let (source, ts) = self.compile_next(ts)?;
        let (condition, ts) = self.next_keyword_cond("where", ts)?;
        let (limit, ts) = self.next_keyword_expr("limit", ts)?;
        Ok((DatabaseOp(Mutation(Mutations::Update {
            path: Box::new(table),
            source: Box::new(source),
            condition,
            limit: limit.map(Box::new),
        })), ts))
    }

    /// Builds a language model from a while expression
    /// ex: x := 0 while (x < 5) { x := x + 1 }
    fn parse_keyword_while(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let (condition, ts) = self.compile_next(ts)?;
        let (code, ts) = self.compile_next(ts)?;
        Ok((While {
            condition: Box::new(condition),
            code: Box::new(code),
        }, ts))
    }

    /// Parses a mutate-target expression.
    /// ex: drop table ns('finance.securities.stocks')
    /// ex: drop index ns('finance.securities.stocks')
    fn parse_mutate_target(
        &mut self,
        ts: TokenSlice,
        f: fn(MutateTarget) -> Expression,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        match ts.next() {
            (Some(Atom { text: keyword, .. }), ts) => {
                match keyword.as_str() {
                    "table" => {
                        let (expr, ts) = self.compile_next(ts)?;
                        Ok((f(TableTarget { path: Box::new(expr) }), ts))
                    }
                    z => throw(ExactNear(format!("Invalid type `{}`, try `table` instead", z), ts.current()))
                }
            }
            (_, ts) => throw(ExactNear("Syntax error".into(), ts.current()))
        }
    }

    /// compiles the [TokenSlice] into an operator-based [Expression]
    fn parse_operator(
        &mut self,
        ts: TokenSlice,
        symbol: String,
        _precedence: usize,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        fn pow(compiler: &mut Compiler, ts: TokenSlice, n: i64) -> std::io::Result<(Expression, TokenSlice)> {
            compiler.parse_expression_2b(ts, Literal(Number(I64Value(n))), Pow)
        }
        match symbol.as_str() {
            ";" => Ok((self.pop().unwrap_or(UNDEFINED), ts.skip())),
            "⁰" => pow(self, ts, 0),
            "¹" => pow(self, ts, 1),
            "²" => pow(self, ts, 2),
            "³" => pow(self, ts, 3),
            "⁴" => pow(self, ts, 4),
            "⁵" => pow(self, ts, 5),
            "⁶" => pow(self, ts, 6),
            "⁷" => pow(self, ts, 7),
            "⁸" => pow(self, ts, 8),
            "⁹" => pow(self, ts, 9),
            "¡" => self.parse_expression_1b(ts, Factorial),
            "!" => self.parse_condition_1a(ts, Not),
            "$" => self.parse_identifier(ts, Variable),
            "(" => self.expect_parentheses(ts),
            "[" => self.expect_square_brackets(ts),
            "{" => self.expect_curly_brackets(ts),
            "-" if self.stack.is_empty() => {
                let (expr, ts) = self.compile_next(ts)?;
                Ok((Neg(Box::new(expr)), ts))
            }
            sym =>
                if let Some(op0) = self.pop() {
                    match sym {
                        "&&" => self.parse_conditional_2a(ts, op0, Conditions::And),
                        ":" => self.parse_expression_2nv(ts, op0, AsValue),
                        "&" => self.parse_expression_2a(ts, op0, BitwiseAnd),
                        "|" => self.parse_expression_2a(ts, op0, BitwiseOr),
                        "^" => self.parse_expression_2a(ts, op0, BitwiseXor),
                        "÷" | "/" => self.parse_expression_2a(ts, op0, Divide),
                        "==" => self.parse_conditional_2a(ts, op0, Equal),
                        "::" => self.parse_expression_2a(ts, op0, ColonColon),
                        ":::" => self.parse_expression_2a(ts, op0, ColonColonColon),
                        ">" => self.parse_conditional_2a(ts, op0, GreaterThan),
                        ">=" => self.parse_conditional_2a(ts, op0, GreaterOrEqual),
                        "<" => self.parse_conditional_2a(ts, op0, LessThan),
                        "<=" => self.parse_conditional_2a(ts, op0, LessOrEqual),
                        "-" => self.parse_expression_2a(ts, op0, Minus),
                        "%" => self.parse_expression_2a(ts, op0, Modulo),
                        "×" | "*" => self.parse_expression_2a(ts, op0, Multiply),
                        "<~" => self.parse_expression_2a(ts, op0, CurvyArrowLeft),
                        "~>" => self.parse_expression_2a(ts, op0, CurvyArrowRight),
                        "!=" => self.parse_conditional_2a(ts, op0, NotEqual),
                        "||" => self.parse_conditional_2a(ts, op0, Conditions::Or),
                        "+" => self.parse_expression_2a(ts, op0, Plus),
                        "++" => self.parse_expression_2a(ts, op0, PlusPlus),
                        "**" => self.parse_expression_2a(ts, op0, Pow),
                        ".." => self.parse_expression_2a(ts, op0, Range),
                        ":=" => self.parse_expression_set(ts, op0),
                        "<<" => self.parse_expression_2a(ts, op0, BitwiseShiftLeft),
                        ">>" => self.parse_expression_2a(ts, op0, BitwiseShiftRight),
                        unknown => fail(format!("Invalid operator '{}'", unknown))
                    }
                } else { fail(format!("Illegal start of expression '{}'", symbol)) }
        }
    }

    fn parse_queryable(
        &mut self,
        host: Expression,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        match ts.to_owned() {
            t if t.is("limit") => {
                let (expr, ts) = self.compile_next(ts.skip())?;
                self.parse_queryable(DatabaseOp(Queryable(Queryables::Limit { from: Box::new(host), limit: Box::new(expr) })), ts)
            }
            t if t.is("where") => {
                match self.compile_next(ts.skip())? {
                    (Condition(condition), ts) =>
                        self.parse_queryable(DatabaseOp(Queryable(Queryables::Where { from: Box::new(host), condition })), ts),
                    (_, ts) =>
                        throw(ExactNear("Boolean expression expected".into(), ts.current()))
                }
            }
            _ => Ok((host, ts))
        }
    }

    fn error_expected_operator<A>(&self, symbol: &str) -> std::io::Result<(A, TokenSlice)> {
        fail(format!(r#"operator '{}' was expected; e.g.: ns("securities", "etf", "stocks")"#, symbol))
    }

    /// parse an argument list from the [TokenSlice] (e.g. "(symbol, exchange, last_sale)")
    pub fn expect_arguments(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Vec<Expression>, TokenSlice)> {
        // parse: ("abc", "123", ..)
        let mut args = Vec::new();
        let mut ts = ts.expect("(")?;
        while ts.isnt(")") {
            let (expr, ats) = self.compile_next(ts)?;
            args.push(expr);
            ts = if ats.is(")") { ats } else { ats.expect(",")? }
        }
        Ok((args, ts.expect(")")?))
    }

    /// parse an atom-based argument list from the [TokenSlice] (e.g. "(symbol, exchange, last_sale)")
    pub fn expect_atom_arguments(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Vec<String>, TokenSlice)> {
        // parse: ("abc", "123", ..)
        let mut args = Vec::new();
        let mut ts = ts.expect("(")?;
        while ts.isnt(")") {
            if let (Variable(name), ats) = self.compile_next(ts.to_owned())? {
                args.push(name);
                ts = if ats.is(")") { ats } else { ats.expect(",")? }
            } else {
                return throw(ExactNear("an atom was expected".into(), ts.current()));
            }
        }
        Ok((args, ts.expect(")")?))
    }

    fn expect_curly_brackets(&mut self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        // collect all expressions and/or key-value pairs
        let (mut expressions, mut key_values) = (Vec::new(), Vec::new());
        let mut ts = ts;
        while ts.isnt("}") {
            // process the next expression
            let (expr, tsb) = self.compile_next(ts)?;
            ts = tsb;

            // capture key-value pairs (JSON)
            if let AsValue(name, value) = expr.to_owned() {
                key_values.push((name, *value))
            }

            // add the expression (code block)
            expressions.push(expr);

            // comma separator?
            if ts.is(",") { ts = ts.next().1 }
        }
        let ts = ts.expect("}")?;

        // if all expressions are key-value pairs, it's a JSON literal,
        // otherwise it's a code block
        if !key_values.is_empty() && key_values.len() == expressions.len() {
            Ok((StructureExpression(key_values), ts))
        } else {
            Ok((CodeBlock(expressions), ts))
        }
    }

    /// Expects a function call or variable
    /// ex: String(32) | f(2, 3) | abc
    fn expect_function_call_or_variable(
        &mut self,
        name: &str,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        // is it a function call? e.g., f(2, 3)
        if ts.is("(") {
            let (args, ts) = self.expect_arguments(ts)?;
            match name {
                "iff" => self.expect_function_call_iff(args, ts),
                name => Ok((FunctionCall { fx: Box::new(Variable(name.to_string())), args }, ts))
            }
        }
        // must be a variable. e.g., abc
        else {
            Ok((Variable(name.to_string()), ts))
        }
    }

    /// Expects an "if" function
    /// ex: iff(n < 0, 1, n)
    fn expect_function_call_iff(
        &mut self,
        args: Vec<Expression>,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        match args.as_slice() {
            [condition, a, b] =>
                Ok((If {
                    condition: Box::new(condition.to_owned()),
                    a: Box::new(a.to_owned()),
                    b: Some(Box::new(b.to_owned())),
                }, ts)),
            _ => throw(ExactNear("Syntax error; usage: iff(condition, if_true, if_false)".into(), ts.current()))
        }
    }

    /// Expects function parameters and a body
    /// ex: (a, b) => a + b
    /// ex: (a: i32, b: i32) => a + b
    /// ex: (a: String, b: String): String => a + b
    fn expect_function_parameters_and_body(
        &mut self,
        name: Option<String>,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        // extract the function parameters
        if let (Parameters(params), ts) = self.expect_parameters(ts.to_owned())? {
            // extract or infer the function return type
            let (returns, ts) = match ts.next() {
                (Some(Operator { text, .. }), ts) if text == ":" => {
                    let (expr, ts) = self.compile_next(ts)?;
                    let data_type = DataType::decipher_type(&expr)?;
                    (Some(data_type), ts)
                }
                _ => (None, ts)
            };
            // extract the function body
            match ts.next() {
                (Some(Operator { text, .. }), ts) if text == "=>" => {
                    let (body, ts) = self.compile_next(ts)?;
                    let returns = returns.unwrap_or(Inferences::infer_with_hints(&body, &params));
                    // build the model
                    let func = FnExpression {
                        params,
                        body: Some(Box::new(body)),
                        returns,
                    };
                    match name {
                        Some(name) => Ok((SetVariable(name, Box::new(func)), ts)),
                        None => Ok((func, ts))
                    }
                }
                // if there's not a body, must be an anonymous function head
                // ex: fn(symbol: String(8), exchange: String(8), last_sale: f64)
                _ => match name {
                    Some(..) => throw(ExactNear("Symbol expected '=>'".into(), ts.current())),
                    None => Ok((FnExpression { params, body: None, returns: DynamicType }, ts))
                }
            }
        } else {
            throw(ExactNear("Function parameters expected".into(), ts.current()))
        }
    }

    /// Expects function parameters
    /// ex: (a: i32, b: i32)
    pub fn expect_parameters(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let mut parameters = Vec::new();
        let mut ts = ts.expect("(")?;
        let mut is_done = ts.is(")");
        while !is_done {
            // get the next parameter
            let (param, ats) = self.expect_parameter(ts.to_owned())?;
            parameters.push(param);

            // are we done yet?
            is_done = ats.is(")");
            ts = if !is_done { ats.expect(",")? } else { ats };
        }
        Ok((Parameters(parameters), ts.expect(")")?))
    }

    /// Expects a single function parameter
    /// ex: a: i32
    fn expect_parameter(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Parameter, TokenSlice)> {
        // attempt to match the parameter name
        // name: String(8) | cost: Float = 0.0
        match ts.next() {
            (Some(Atom { text: name, .. }), ts) => {
                // next, check for type constraint
                let (typedef, ts) = if ts.is(":") {
                    self.expect_parameter_type_decl(ts.skip())?
                } else { (None, ts) };

                // finally, check for a default value
                let (default_value, ts) = if ts.is("=") {
                    if let (Some(tok), ts) = ts.skip().next() {
                        (TypedValue::from_token(&tok)?, ts)
                    } else {
                        return throw(ExactNear("An expression was expected".into(), ts.current()));
                    }
                } else { (Null, ts) };

                Ok((Parameter::with_default(name, typedef.unwrap_or(DynamicType), default_value), ts))
            }
            (_, ats) => throw(ExactNear("Function name expected".into(), ats.current()))
        }
    }

    /// Expects a parameter type declaration
    /// ex: String(8)
    pub fn expect_parameter_type_decl(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Option<DataType>, TokenSlice)> {
        if let (Some(Atom { text: type_name, .. }), ts) = ts.next() {
            // e.g. String(8)
            if ts.is("(") {
                let (args, ts) = self.expect_arguments(ts)?;
                let kind = DataType::from_str(format!("{}({})", type_name, args.iter()
                    .map(|expr| expr.to_code())
                    .collect::<Vec<_>>()
                    .join(", ")).as_str())?;
                Ok((Some(kind), ts))
            } else { Ok((Some(DataType::from_str(type_name.as_str())?), ts)) }
        } else { Ok((None, ts)) }
    }

    /// Expects an expression starting with an opening parenthesis
    /// ex: (8 * n)
    fn expect_parentheses(&mut self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        let mut items = vec![];
        let mut ts = ts;
        while ts.isnt(")") {
            // compile the next expression
            let (expr, tts) = self.compile_next(ts)?;
            items.push(expr);
            ts = tts;

            // commas are optional
            if ts.is(",") { ts = ts.skip(); }
        }

        // expect the closing parenthesis
        ts = ts.expect(")")?;

        // what did we get?
        match items.as_slice() {
            // are they parameters?
            args if args.iter().find(|arg| matches!(arg, AsValue(..))).is_some() =>
                Ok((Self::convert_to_parameters(args)?, ts)),
            [expr] => Ok((expr.clone(), ts)),
            args => Ok((TupleExpression(args.to_vec()), ts))
        }
    }

    fn convert_to_parameters(args: &[Expression]) -> std::io::Result<Expression> {
        let mut params = vec![];
        for arg in args.iter() {
            match arg {
                AsValue(name, expr) =>
                    params.push(Parameter::new(name, DataType::decipher_type(expr)?)),
                Variable(name) =>
                    params.push(Parameter::new(name, DynamicType)),
                expr =>
                    return throw(TypeMismatch(ParameterExpected(expr.to_code())))
            }
        }
        Ok(Parameters(params))
    }

    /// Parses a square bracket expression
    /// ex: ["Hello", 123, cal::now()]
    fn expect_square_brackets(&mut self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        let mut items = vec![];
        let mut ts = ts;
        while ts.isnt("]") {
            // compile the next expression
            let (expr, tts) = self.compile_next(ts)?;
            items.push(expr);
            ts = tts;

            // commas are optional
            if ts.is(",") { ts = ts.skip(); }
        }
        Ok((ArrayExpression(items), ts.expect("]")?))
    }

    fn maybe_curly_brackets(
        &mut self,
        ts: TokenSlice,
    ) -> Option<(std::io::Result<Expression>, TokenSlice)> {
        match ts.next() {
            (Some(t), ts) if t.contains("{") =>
                match self.expect_curly_brackets(ts.to_owned()) {
                    Ok((expr, ts)) => Some((Ok(expr), ts)),
                    Err(err) => Some((fail(format!("{}", err)), ts))
                }
            _ => None
        }
    }

    /// parse an expression list from the [TokenSlice] (e.g. ['x', ',', 'y', ',', 'z'])
    fn next_expression_list(&mut self, ts: TokenSlice) -> std::io::Result<(Option<Vec<Expression>>, TokenSlice)> {
        let mut items = Vec::new();
        // get the first item
        let (item, mut ts) = self.compile_next(ts)?;
        items.push(item);
        // get the others
        while ts.is(",") {
            let (item, _ts) = self.compile_next(ts.skip())?;
            ts = _ts;
            items.push(item);
        }
        Ok((Some(items), ts))
    }

    /// Returns the option of an [Expression] based the next token matching the specified keyword
    fn next_keyword_expr(&mut self, keyword: &str, ts: TokenSlice) -> std::io::Result<(Option<Expression>, TokenSlice)> {
        if ts.isnt(keyword) { Ok((None, ts)) } else {
            let (expr, ts) = self.compile_next(ts.skip())?;
            Ok((Some(expr), ts))
        }
    }

    /// Returns the option of a [Conditions] based the next token matching the specified keyword
    fn next_keyword_cond(&mut self, keyword: &str, ts: TokenSlice) -> std::io::Result<(Option<Conditions>, TokenSlice)> {
        match self.next_keyword_expr(keyword, ts)? {
            (Some(Condition(cond)), ts) => Ok((Some(cond), ts)),
            (Some(..), ts) => return throw(ExactNear("Expected a boolean expression".into(), ts.current())),
            (None, ts) => Ok((None, ts)),
        }
    }

    /// Returns the [Option] of a list of expressions based the next token matching the specified keywords
    fn next_keyword_expression_list(&mut self, keyword0: &str, keyword1: &str, ts: TokenSlice) -> std::io::Result<(Option<Vec<Expression>>, TokenSlice)> {
        if ts.isnt(keyword0) || ts.skip().isnt(keyword1) { Ok((None, ts)) } else {
            self.next_expression_list(ts.skip().skip())
        }
    }

    pub fn push(&mut self, expression: Expression) {
        //println!("push -> {:?}", expression);
        self.stack.push(expression)
    }

    pub fn pop(&mut self) -> Option<Expression> {
        let result = self.stack.pop();
        //println!("pop <- {:?}", result);
        result
    }
}

/// Unit tests
#[cfg(test)]
mod tests {
    /// Build tests
    #[cfg(test)]
    mod build_tests {
        use crate::compiler::Compiler;
        use crate::machine::Machine;
        use crate::numbers::Numbers::I64Value;
        use crate::typed_values::TypedValue::Number;

        #[test]
        fn test_build_save_load() {
            let src_path = "stocks.oxc";

            // compile and save as a binary
            Compiler::build_and_save(src_path, r#"
                stocks := ns("compiler.build_save_load.stocks")
                table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                append stocks from [
                    { symbol: "ABC", exchange: "AMEX", last_sale: 11.77 },
                    { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                    { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                    { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                    { symbol: "XYZ", exchange: "NASDAQ", last_sale: 0.0872 }
                ]
            "#).unwrap();

            // load the binary
            let expr = Compiler::load(src_path).unwrap();
            let (_, result) = Machine::new_platform().evaluate(&expr).unwrap();
            assert_eq!(result, Number(I64Value(5)))
        }
    }

    /// Core tests
    #[cfg(test)]
    mod core_tests {
        use crate::compiler::Compiler;
        use crate::expression::Conditions::True;
        use crate::expression::Expression::{Condition, Feature, FunctionCall, Literal, Scenario, SetVariables, TupleExpression, Variable};
        use crate::numbers::Numbers::{F64Value, I64Value};
        use crate::token_slice::TokenSlice;
        use crate::typed_values::TypedValue::{Number, StringValue};

        #[test]
        fn test_feature_with_a_scenario() {
            let code = Compiler::build(r#"
            Feature "Karate translator" {
                Scenario "Translate Karate Scenario to Oxide Scenario" {
                    assert(true)
                }
            } "#).unwrap();
            assert_eq!(code, Feature {
                title: Box::new(Literal(StringValue("Karate translator".to_string()))),
                scenarios: vec![
                    Scenario {
                        title: Box::new(Literal(StringValue("Translate Karate Scenario to Oxide Scenario".to_string()))),
                        verifications: vec![
                            FunctionCall {
                                fx: Box::new(Variable("assert".to_string())),
                                args: vec![Condition(True)],
                            }
                        ]
                    }
                ],
            })
        }

        #[test]
        fn test_next_argument_list() {
            let mut compiler = Compiler::new();
            let ts = TokenSlice::from_string("(abc, 123, 'Hello')");
            let (items, _) = compiler.expect_arguments(ts).unwrap();
            assert_eq!(items, vec![
                Variable("abc".into()),
                Literal(Number(I64Value(123))),
                Literal(StringValue("Hello".into())),
            ])
        }

        #[test]
        fn test_next_expression() {
            let mut compiler = Compiler::new();
            let ts = TokenSlice::from_string("abc");
            let (expr, _) = compiler.compile_next(ts).unwrap();
            assert_eq!(expr, Variable("abc".into()))
        }

        #[test]
        fn test_next_expression_list() {
            let mut compiler = Compiler::new();
            let ts = TokenSlice::from_string("abc, 123.0, 'Hello'");
            let (items, _) = compiler.next_expression_list(ts).unwrap();
            assert_eq!(items, Some(vec![
                Variable("abc".into()),
                Literal(Number(F64Value(123.))),
                Literal(StringValue("Hello".into())),
            ]))
        }

        #[test]
        fn test_system_call() {
            let model = Compiler::build(r#"
                syscall("cat", "LICENSE")
            "#).unwrap();
            assert_eq!(
                model,
                FunctionCall {
                    fx: Box::new(Variable("syscall".into())),
                    args: vec![
                        Literal(StringValue("cat".into())),
                        Literal(StringValue("LICENSE".into())),
                    ],
                });
        }

        #[test]
        fn test_tuple_assignment() {
            let input = "(a, b, c) := (3, 5, 7)";
            let model = Compiler::build(input).unwrap();
            assert_eq!(model, SetVariables(
                Box::new(TupleExpression(vec![
                    Variable("a".into()),
                    Variable("b".into()),
                    Variable("c".into()),
                ])),
                Box::new(TupleExpression(vec![
                    Literal(Number(I64Value(3))),
                    Literal(Number(I64Value(5))),
                    Literal(Number(I64Value(7))),
                ])),
            ));
            assert_eq!(model.to_code(), input)
        }

        #[test]
        fn test_type_of() {
            let model = Compiler::build(r#"
                type_of("cat")
            "#).unwrap();
            assert_eq!(
                model,
                FunctionCall {
                    fx: Box::new(Variable("type_of".to_string())),
                    args: vec![
                        Literal(StringValue("cat".to_string()))
                    ],
                });
        }
    }

    /// Bitwise tests
    #[cfg(test)]
    mod bitwise_tests {
        use crate::compiler::Compiler;
        use crate::expression::Expression;
        use crate::expression::Expression::Literal;
        use crate::numbers::Numbers::I64Value;
        use crate::typed_values::TypedValue::Number;

        #[test]
        fn test_bitwise_and() {
            assert_eq!(
                Compiler::build("20 & 3").unwrap(),
                Expression::BitwiseAnd(
                    Box::new(Literal(Number(I64Value(20)))),
                    Box::new(Literal(Number(I64Value(3)))),
                ));
        }

        #[test]
        fn test_bitwise_or() {
            assert_eq!(
                Compiler::build("20 | 3").unwrap(),
                Expression::BitwiseOr(
                    Box::new(Literal(Number(I64Value(20)))),
                    Box::new(Literal(Number(I64Value(3)))),
                ));
        }

        #[test]
        fn test_bitwise_shl() {
            assert_eq!(
                Compiler::build("20 << 3").unwrap(),
                Expression::BitwiseShiftLeft(
                    Box::new(Literal(Number(I64Value(20)))),
                    Box::new(Literal(Number(I64Value(3)))),
                ));
        }

        #[test]
        fn test_bitwise_shr() {
            assert_eq!(
                Compiler::build("20 >> 3").unwrap(),
                Expression::BitwiseShiftRight(
                    Box::new(Literal(Number(I64Value(20)))),
                    Box::new(Literal(Number(I64Value(3)))),
                ));
        }

        #[test]
        fn test_bitwise_xor() {
            assert_eq!(
                Compiler::build("19 ^ 13").unwrap(),
                Expression::BitwiseXor(
                    Box::new(Literal(Number(I64Value(19)))),
                    Box::new(Literal(Number(I64Value(13)))),
                ));
        }
    }

    /// Function tests
    #[cfg(test)]
    mod function_tests {
        use crate::compiler::Compiler;
        use crate::data_types::DataType::{DynamicType, NumberType, StringType, StructureType};
        use crate::expression::Expression::{FnExpression, FunctionCall, Literal, Multiply, Plus, SetVariable, StructureExpression, Variable};
        use crate::number_kind::NumberKind::{F64Kind, I32Kind, I64Kind, U64Kind};
        use crate::numbers::Numbers::{F64Value, I64Value};
        use crate::parameter::Parameter;
        use crate::typed_values::TypedValue::Number;

        #[test]
        fn test_define_anonymous_function_with_explicit_types() {
            let code = Compiler::build(r#"
                fn (a: i64, b: i64) : i64 => a * b
            "#).unwrap();
            assert_eq!(code, FnExpression {
                params: vec![
                    Parameter::new("a", NumberType(I64Kind)),
                    Parameter::new("b", NumberType(I64Kind)),
                ],
                body: Some(Box::new(Multiply(Box::new(
                    Variable("a".into())
                ), Box::new(
                    Variable("b".into())
                )))),
                returns: NumberType(I64Kind)
            })
        }

        #[test]
        fn test_define_anonymous_function_with_inferred_types() {
            let code = Compiler::build(r#"
                fn (a, b) => a * b
            "#).unwrap();
            assert_eq!(code, FnExpression {
                params: vec![
                    Parameter::add("a"),
                    Parameter::add("b"),
                ],
                body: Some(Box::new(Multiply(Box::new(
                    Variable("a".into())
                ), Box::new(
                    Variable("b".into())
                )))),
                returns: DynamicType
            })
        }

        #[test]
        fn test_define_named_function_with_explicit_types() {
            let code = Compiler::build(r#"
                fn add(a: i32, b: i32): i32 => a + b
            "#).unwrap();
            assert_eq!(code, SetVariable("add".to_string(), Box::new(
                FnExpression {
                    params: vec![
                        Parameter::new("a", NumberType(I32Kind)),
                        Parameter::new("b", NumberType(I32Kind)),
                    ],
                    body: Some(Box::new(Plus(Box::new(
                        Variable("a".into())
                    ), Box::new(
                        Variable("b".into())
                    )))),
                    returns: NumberType(I32Kind)
                }
            )))
        }

        #[test]
        fn test_define_named_function_with_explicit_and_inferred_types() {
            let code = Compiler::build(r#"
                fn add(a: i32, b: i32) => a + b
            "#).unwrap();
            assert_eq!(code, SetVariable("add".to_string(), Box::new(
                FnExpression {
                    params: vec![
                        Parameter::new("a", NumberType(I32Kind)),
                        Parameter::new("b", NumberType(I32Kind)),
                    ],
                    body: Some(Box::new(Plus(Box::new(
                        Variable("a".into())
                    ), Box::new(
                        Variable("b".into())
                    )))),
                    returns: NumberType(I32Kind)
                }
            )))
        }

        #[test]
        fn test_define_named_function_with_inferred_types() {
            let code = Compiler::build(r#"
                fn add(a, b) => a + b
            "#).unwrap();
            assert_eq!(code, SetVariable("add".to_string(), Box::new(
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
                    returns: DynamicType
                }
            )))
        }

        #[test]
        fn test_define_named_function_with_explicit_input_and_return_types() {
            let code = Compiler::build(r#"
                fn(
                    symbol: String(8),
                    exchange: String(8),
                    last_sale: f64
                ): Struct(symbol: String(8), exchange: String(8), last_sale: f64, uid: u64) =>
                    {
                        symbol: symbol,
                        market: exchange,
                        last_sale: last_sale * 2.0,
                        uid: __row_id__
                    }
            "#).unwrap();
            assert_eq!(code, FnExpression {
                params: vec![
                    Parameter::new("symbol", StringType(8)),
                    Parameter::new("exchange", StringType(8)),
                    Parameter::new("last_sale", NumberType(F64Kind)),
                ],
                body: Some(Box::new(StructureExpression(vec![
                    ("symbol".to_string(), Variable("symbol".into())),
                    ("market".to_string(), Variable("exchange".into())),
                    ("last_sale".to_string(), Multiply(
                        Box::new(Variable("last_sale".into())),
                        Box::new(Literal(Number(F64Value(2.0))))
                    )),
                    ("uid".to_string(), Variable("__row_id__".into()))
                ]))),
                returns: StructureType(vec![
                    Parameter::new("symbol", StringType(8)),
                    Parameter::new("exchange", StringType(8)),
                    Parameter::new("last_sale", NumberType(F64Kind)),
                    Parameter::new("uid", NumberType(U64Kind)),
                ])
            })
        }

        #[test]
        fn test_function_call() {
            let code = Compiler::build(r#"
                f(2, 3)
            "#).unwrap();
            assert_eq!(code, FunctionCall {
                fx: Box::new(Variable("f".into())),
                args: vec![
                    Literal(Number(I64Value(2))),
                    Literal(Number(I64Value(3))),
                ],
            })
        }
    }

    /// HTTP tests
    #[cfg(test)]
    mod http_tests {
        use crate::compiler::Compiler;
        use crate::expression::Expression::{Literal, StructureExpression, HTTP};
        use crate::typed_values::TypedValue::StringValue;

        #[test]
        fn test_http_delete() {
            let model = Compiler::build(r#"
                DELETE "http://localhost:9000/comments?id=675af"
            "#).unwrap();
            assert_eq!(model, HTTP {
                method: "DELETE".to_string(),
                url_or_object: Box::new(Literal(StringValue("http://localhost:9000/comments?id=675af".to_string()))),
            });
        }

        #[test]
        fn test_http_get() {
            let model = Compiler::build(r#"
                GET "http://localhost:9000/comments?id=675af"
            "#).unwrap();
            assert_eq!(model, HTTP {
                method: "GET".to_string(),
                url_or_object: Box::new(Literal(StringValue("http://localhost:9000/comments?id=675af".to_string()))),
            });
        }

        #[test]
        fn test_http_head() {
            let model = Compiler::build(r#"
                HEAD "http://localhost:9000/quotes/AMD/NYSE"
            "#).unwrap();
            assert_eq!(model, HTTP {
                method: "HEAD".to_string(),
                url_or_object: Box::new(Literal(StringValue("http://localhost:9000/quotes/AMD/NYSE".to_string()))),
            });
        }

        #[test]
        fn test_http_patch() {
            let model = Compiler::build(r#"
                PATCH "http://localhost:9000/quotes/AMD/NASDAQ?exchange=NYSE"
            "#).unwrap();
            assert_eq!(model, HTTP {
                method: "PATCH".to_string(),
                url_or_object: Box::new(Literal(StringValue("http://localhost:9000/quotes/AMD/NASDAQ?exchange=NYSE".to_string()))),
            });
        }

        #[test]
        fn test_http_post() {
            let model = Compiler::build(r#"
                POST "http://localhost:9000/quotes/AMD/NASDAQ"
            "#).unwrap();
            assert_eq!(model, HTTP {
                method: "POST".to_string(),
                url_or_object: Box::new(Literal(StringValue("http://localhost:9000/quotes/AMD/NASDAQ".to_string()))),
            });
        }

        #[test]
        fn test_http_post_with_body() {
            let model = Compiler::build(r#"
                POST {
                    url: http://localhost:8080/machine/www/stocks
                    body: "Hello World"
                }
            "#).unwrap();
            assert_eq!(model, HTTP {
                method: "POST".to_string(),
                url_or_object: Box::new(StructureExpression(vec![
                    ("url".to_string(), Literal(StringValue("http://localhost:8080/machine/www/stocks".to_string()))),
                    ("body".to_string(), Literal(StringValue("Hello World".to_string()))),
                ]))
            });
        }

        #[test]
        fn test_http_post_with_multipart() {
            let model = Compiler::build(r#"
                POST {
                    url: http://localhost:8080/machine/www/stocks
                    body: file://./demoes/language/include_file.ox
                }
            "#).unwrap();
            assert_eq!(model, HTTP {
                method: "POST".to_string(),
                url_or_object: Box::new(StructureExpression(vec![
                    ("url".to_string(), Literal(StringValue("http://localhost:8080/machine/www/stocks".to_string()))),
                    ("body".to_string(), Literal(StringValue("file://./demoes/language/include_file.ox".to_string()))),
                ]))
            });
        }

        #[test]
        fn test_http_put() {
            let model = Compiler::build(r#"
            PUT "http://localhost:9000/quotes/AMD/NASDAQ"
        "#).unwrap();
            assert_eq!(model, HTTP {
                method: "PUT".to_string(),
                url_or_object: Box::new(Literal(
                    StringValue("http://localhost:9000/quotes/AMD/NASDAQ".to_string())
                )),
            });
        }
    }

    /// Import tests
    #[cfg(test)]
    mod import_tests {
        use crate::compiler::Compiler;
        use crate::expression::Expression::{ColonColon, Import, Variable};
        use crate::expression::ImportOps;

        #[test]
        fn test_extraction() {
            let code = Compiler::build(r#"
                oxide::tools::compact
            "#).unwrap();
            assert_eq!(
                code,
                ColonColon(
                    Box::new(Variable("oxide".to_string())),
                    Box::new(ColonColon(Box::new(Variable("tools".to_string())),
                                        Box::new(Variable("compact".to_string()))
                    ))
                ));
            assert_eq!(code.to_code(), "oxide::tools::compact");
        }

        #[test]
        fn test_import_one() {
            // single import
            let code = Compiler::build(r#"
                import "os"
            "#).unwrap();
            assert_eq!(code, Import(vec![
                ImportOps::Everything("os".into())
            ]));
            assert_eq!(code.to_code(), "import os");
        }

        #[test]
        fn test_import_multiple() {
            // multiple imports
            let code = Compiler::build(r#"
                import os, util
            "#).unwrap();
            assert_eq!(code, Import(vec![
                ImportOps::Everything("os".into()),
                ImportOps::Everything("util".into()),
            ]));
            assert_eq!(code.to_code(), "import os, util");
        }
    }

    /// Array tests
    #[cfg(test)]
    mod literal_tests {
        use crate::compiler::Compiler;
        use crate::expression::Expression::{ArrayExpression, AsValue, ElementAt, Literal, StructureExpression};
        use crate::numbers::Numbers::{F64Value, I64Value};
        use crate::token_slice::TokenSlice;
        use crate::typed_values::TypedValue::{Number, StringValue};

        #[test]
        fn test_aliasing() {
            assert_eq!(
                Compiler::build(r#"symbol: "ABC""#).unwrap(),
                AsValue("symbol".into(), Box::new(Literal(StringValue("ABC".into())))));

            assert_eq!(
                Compiler::build(r#""name": "Bill Bass""#).unwrap(),
                AsValue("name".into(), Box::new(Literal(StringValue("Bill Bass".into())))));
        }

        #[test]
        fn test_array_declaration() {
            assert_eq!(
                Compiler::build("[7, 5, 8, 2, 4, 1]").unwrap(),
                ArrayExpression(vec![
                    Literal(Number(I64Value(7))), Literal(Number(I64Value(5))), Literal(Number(I64Value(8))),
                    Literal(Number(I64Value(2))), Literal(Number(I64Value(4))), Literal(Number(I64Value(1))),
                ]))
        }

        #[test]
        fn test_array_indexing() {
            assert_eq!(
                Compiler::build("[7, 5, 8, 2, 4, 1][3]").unwrap(),
                ElementAt(
                    Box::new(ArrayExpression(vec![
                        Literal(Number(I64Value(7))), Literal(Number(I64Value(5))), Literal(Number(I64Value(8))),
                        Literal(Number(I64Value(2))), Literal(Number(I64Value(4))), Literal(Number(I64Value(1))),
                    ])),
                    Box::new(Literal(Number(I64Value(3))))));
        }

        #[test]
        fn test_json_literal_value() {
            let code = Compiler::build(r#"
                {symbol: "ABC", exchange: "NYSE", last_sale: 16.79}
            "#).unwrap();
            assert_eq!(
                code,
                StructureExpression(vec![
                    ("symbol".to_string(), Literal(StringValue("ABC".into()))),
                    ("exchange".to_string(), Literal(StringValue("NYSE".into()))),
                    ("last_sale".to_string(), Literal(Number(F64Value(16.79)))),
                ])
            );
        }

        #[test]
        fn test_maybe_curly_brackets() {
            let ts = TokenSlice::from_string(r#"{w:'abc', x:1.0, y:2, z:[1, 2, 3]}"#);
            let mut compiler = Compiler::new();
            let (result, _) = compiler.maybe_curly_brackets(ts).unwrap();
            let result = result.unwrap();
            assert_eq!(result, StructureExpression(vec![
                ("w".to_string(), Literal(StringValue("abc".into()))),
                ("x".to_string(), Literal(Number(F64Value(1.)))),
                ("y".to_string(), Literal(Number(I64Value(2)))),
                ("z".to_string(), ArrayExpression(vec![
                    Literal(Number(I64Value(1))),
                    Literal(Number(I64Value(2))),
                    Literal(Number(I64Value(3))),
                ])),
            ]));
        }

        #[test]
        fn test_numeric_literal_value_float() {
            assert_eq!(Compiler::build("1_234_567.890").unwrap(),
                       Literal(Number(F64Value(1_234_567.890))));
        }

        #[test]
        fn test_numeric_literal_value_integer() {
            assert_eq!(Compiler::build("1_234_567_890").unwrap(),
                       Literal(Number(I64Value(1_234_567_890))));
        }
    }

    /// Logical tests
    #[cfg(test)]
    mod logical_tests {
        use crate::compiler::Compiler;
        use crate::expression::Conditions::{GreaterThan, LessThan, Not};
        use crate::expression::Expression::{CodeBlock, Condition, If, Literal, Plus, SetVariable, Variable, While};
        use crate::expression::{FALSE, TRUE};
        use crate::numbers::Numbers::I64Value;
        use crate::typed_values::TypedValue::{Number, StringValue};

        #[test]
        fn test_if() {
            let code = Compiler::build(r#"
                if(n > 100) "Yes"
            "#).unwrap();
            assert_eq!(code, If {
                condition: Box::new(Condition(GreaterThan(
                    Box::new(Variable("n".to_string())),
                    Box::new(Literal(Number(I64Value(100)))),
                ))),
                a: Box::new(Literal(StringValue("Yes".to_string()))),
                b: None,
            });
        }

        #[test]
        fn test_if_else() {
            let code = Compiler::build(r#"
                if(n > 100) n else m
            "#).unwrap();
            assert_eq!(code, If {
                condition: Box::new(Condition(GreaterThan(
                    Box::new(Variable("n".to_string())),
                    Box::new(Literal(Number(I64Value(100)))),
                ))),
                a: Box::new(Variable("n".to_string())),
                b: Some(Box::new(Variable("m".to_string()))),
            });
        }

        #[test]
        fn test_iff() {
            let code = Compiler::build(r#"
                iff(n > 5, a, b)
            "#).unwrap();
            assert_eq!(code, If {
                condition: Box::new(Condition(GreaterThan(
                    Box::new(Variable("n".to_string())),
                    Box::new(Literal(Number(I64Value(5)))),
                ))),
                a: Box::new(Variable("a".to_string())),
                b: Some(Box::new(Variable("b".to_string()))),
            });
        }

        #[test]
        fn test_not_expression() {
            assert_eq!(Compiler::build("!false").unwrap(), Condition(Not(Box::new(FALSE))));
            assert_eq!(Compiler::build("!true").unwrap(), Condition(Not(Box::new(TRUE))));
        }

        #[test]
        fn test_while_loop() {
            let model = Compiler::build(r#"
                while (x < 5) x := x + 1
            "#).unwrap();
            assert_eq!(model, While {
                condition: Box::new(Condition(LessThan(
                    Box::new(Variable("x".into())),
                    Box::new(Literal(Number(I64Value(5)))),
                ))),
                code: Box::new(SetVariable("x".into(), Box::new(Plus(
                    Box::new(Variable("x".into())),
                    Box::new(Literal(Number(I64Value(1))))),
                ))),
            });
        }

        #[test]
        fn test_while_loop_fix() {
            let model = Compiler::build(r#"
                x := 0
                while x < 7 x := x + 1
                x
            "#).unwrap();
            assert_eq!(model, CodeBlock(vec![
                SetVariable("x".into(), Box::new(Literal(Number(I64Value(0))))),
                While {
                    condition: Box::new(Condition(LessThan(
                        Box::new(Variable("x".into())),
                        Box::new(Literal(Number(I64Value(7)))),
                    ))),
                    code: Box::new(SetVariable("x".into(), Box::new(Plus(
                        Box::new(Variable("x".into())),
                        Box::new(Literal(Number(I64Value(1))))),
                    ))),
                },
                Variable("x".into()),
            ]));
        }
    }

    /// Mathematics tests
    #[cfg(test)]
    mod math_tests {
        use crate::compiler::Compiler;
        use crate::expression::Expression::{Divide, Factorial, Literal, Minus, Modulo, Multiply, Plus, Pow, Variable};
        use crate::numbers::Numbers::I64Value;
        use crate::typed_values::TypedValue::Number;

        #[test]
        fn test_mathematical_addition() {
            let model = Compiler::build("n + 3").unwrap();
            assert_eq!(model, Plus(
                Box::new(Variable("n".into())),
                Box::new(Literal(Number(I64Value(3)))),
            ));
        }

        #[test]
        fn test_mathematical_division() {
            let model = Compiler::build("n / 3").unwrap();
            assert_eq!(model, Divide(
                Box::new(Variable("n".into())),
                Box::new(Literal(Number(I64Value(3)))),
            ));
        }

        #[test]
        fn test_mathematical_exponent() {
            let model = Compiler::build("5 ** 2").unwrap();
            assert_eq!(model, Pow(
                Box::new(Literal(Number(I64Value(5)))),
                Box::new(Literal(Number(I64Value(2)))),
            ));
        }

        #[test]
        fn test_mathematical_exponent_via_symbol() {
            let symbols = vec!["⁰", "¹", "²", "³", "⁴", "⁵", "⁶", "⁷", "⁸", "⁹"];
            let mut num = 0;
            for symbol in symbols {
                let model = Compiler::build(format!("5{}", symbol).as_str()).unwrap();
                assert_eq!(model, Pow(
                    Box::new(Literal(Number(I64Value(5)))),
                    Box::new(Literal(Number(I64Value(num)))),
                ));
                num += 1
            }
        }

        #[test]
        fn test_mathematical_factorial() {
            let model = Compiler::build("5¡").unwrap();
            assert_eq!(model, Factorial(Box::new(Literal(Number(I64Value(5))))));
        }

        #[test]
        fn test_mathematical_modulus() {
            let model = Compiler::build("n % 4").unwrap();
            assert_eq!(
                model,
                Modulo(Box::new(Variable("n".into())), Box::new(Literal(Number(I64Value(4))))));
        }

        #[test]
        fn test_mathematical_multiplication() {
            let model = Compiler::build("n * 10").unwrap();
            assert_eq!(model, Multiply(
                Box::new(Variable("n".into())),
                Box::new(Literal(Number(I64Value(10)))),
            ));
        }

        #[test]
        fn test_mathematical_subtraction() {
            let model = Compiler::build("_ - 7").unwrap();
            assert_eq!(model,
                       Minus(Box::new(Variable("_".into())), Box::new(Literal(Number(I64Value(7))))));
        }
    }

    /// SQL tests
    #[cfg(test)]
    mod order_of_operations_tests {
        use crate::compiler::Compiler;
        use crate::expression::Expression::{Divide, Literal, Minus, Multiply, Plus};
        use crate::numbers::Numbers::{F64Value, I64Value};
        use crate::typed_values::TypedValue::Number;

        #[test]
        fn test_order_of_operations_1() {
            let model = Compiler::build("2 + (4 * 3)").unwrap();
            assert_eq!(model, Plus(
                Box::new(Literal(Number(I64Value(2)))),
                Box::new(Multiply(
                    Box::new(Literal(Number(I64Value(4)))),
                    Box::new(Literal(Number(I64Value(3)))),
                )),
            ));
        }

        #[test]
        fn test_order_of_operations_2() {
            let model = Compiler::build("(4.0 / 3.0) + (4 * 3)").unwrap();
            assert_eq!(model, Plus(
                Box::new(Divide(
                    Box::new(Literal(Number(F64Value(4.0)))),
                    Box::new(Literal(Number(F64Value(3.0)))),
                )),
                Box::new(Multiply(
                    Box::new(Literal(Number(I64Value(4)))),
                    Box::new(Literal(Number(I64Value(3)))),
                )),
            ));
        }

        #[test]
        fn test_order_of_operations_3() {
            let model = Compiler::build("2 - 4 * 3").unwrap();
            assert_eq!(model, Minus(
                Box::new(Literal(Number(I64Value(2)))),
                Box::new(Multiply(
                    Box::new(Literal(Number(I64Value(4)))),
                    Box::new(Literal(Number(I64Value(3)))),
                )),
            ));
        }
    }

    /// Parameter and Tuple tests
    #[cfg(test)]
    mod parameter_and_tuple_tests {
        use crate::compiler::Compiler;
        use crate::data_types::DataType::{DynamicType, NumberType};
        use crate::expression::Expression::{Parameters, TupleExpression, Variable};
        use crate::number_kind::NumberKind::I64Kind;
        use crate::parameter::Parameter;

        #[test]
        fn test_parentheses_for_qualified_parameters() {
            let expr = Compiler::build(r#"
                (a: i64, b: i64, c: i64)
            "#).unwrap();
            assert_eq!(expr, Parameters(vec![
                Parameter::new("a", NumberType(I64Kind)),
                Parameter::new("b", NumberType(I64Kind)),
                Parameter::new("c", NumberType(I64Kind)),
            ]))
        }

        #[test]
        fn test_parentheses_for_parameters() {
            let expr = Compiler::build(r#"
                (a, b, c: i64)
            "#).unwrap();
            assert_eq!(expr, Parameters(vec![
                Parameter::new("a", DynamicType),
                Parameter::new("b", DynamicType),
                Parameter::new("c", NumberType(I64Kind)),
            ]))
        }

        #[test]
        fn test_parentheses_for_tuples() {
            let expr = Compiler::build(r#"
                (a, b, c)
            "#).unwrap();
            assert_eq!(expr, TupleExpression(vec![
                Variable("a".into()),
                Variable("b".into()),
                Variable("c".into()),
            ]))
        }
    }

    /// SQL tests
    #[cfg(test)]
    mod sql_tests {
        use crate::compiler::Compiler;
        use crate::data_types::DataType::{NumberType, StringType, StructureType};
        use crate::expression::Conditions::{Between, Betwixt, Equal, GreaterOrEqual, LessOrEqual, LessThan, Like, True};
        use crate::expression::CreationEntity::{IndexEntity, TableEntity};
        use crate::expression::DatabaseOps::{Mutation, Queryable};
        use crate::expression::Expression::{ArrayExpression, ColonColon, ColonColonColon, Condition, CurvyArrowRight, DatabaseOp, FnExpression, From, FunctionCall, Literal, Ns, StructureExpression, Variable, Via};
        use crate::expression::MutateTarget::TableTarget;
        use crate::expression::Mutations::{Create, Declare, Drop};
        use crate::expression::{CreationEntity, Mutations, Queryables};
        use crate::number_kind::NumberKind::{DateKind, F64Kind};
        use crate::numbers::Numbers::{F64Value, I64Value};
        use crate::parameter::Parameter;
        use crate::typed_values::TypedValue::{Number, StringValue};

        #[test]
        fn test_append_from_json_literal() {
            let model = Compiler::build(r#"
                append ns("compiler.append2.stocks")
                from { symbol: "ABC", exchange: "NYSE", last_sale: 0.1008 }
            "#).unwrap();
            assert_eq!(model, DatabaseOp(Mutation(Mutations::Append {
                path: Box::new(Ns(Box::new(Literal(StringValue("compiler.append2.stocks".to_string()))))),
                source: Box::new(From(Box::new(StructureExpression(vec![
                    ("symbol".into(), Literal(StringValue("ABC".into()))),
                    ("exchange".into(), Literal(StringValue("NYSE".into()))),
                    ("last_sale".into(), Literal(Number(F64Value(0.1008)))),
                ])))),
            })))
        }

        #[test]
        fn test_append_from_json_array() {
            let model = Compiler::build(r#"
                append ns("compiler.into.stocks")
                from [
                    { symbol: "CAT", exchange: "NYSE", last_sale: 11.1234 },
                    { symbol: "DOG", exchange: "NASDAQ", last_sale: 0.1008 },
                    { symbol: "SHARK", exchange: "AMEX", last_sale: 52.08 }
                ]
            "#).unwrap();
            assert_eq!(model, DatabaseOp(Mutation(Mutations::Append {
                path: Box::new(Ns(Box::new(Literal(StringValue("compiler.into.stocks".to_string()))))),
                source: Box::new(From(Box::new(
                    ArrayExpression(vec![
                        StructureExpression(vec![
                            ("symbol".into(), Literal(StringValue("CAT".into()))),
                            ("exchange".into(), Literal(StringValue("NYSE".into()))),
                            ("last_sale".into(), Literal(Number(F64Value(11.1234)))),
                        ]),
                        StructureExpression(vec![
                            ("symbol".into(), Literal(StringValue("DOG".into()))),
                            ("exchange".into(), Literal(StringValue("NASDAQ".into()))),
                            ("last_sale".into(), Literal(Number(F64Value(0.1008)))),
                        ]),
                        StructureExpression(vec![
                            ("symbol".into(), Literal(StringValue("SHARK".into()))),
                            ("exchange".into(), Literal(StringValue("AMEX".into()))),
                            ("last_sale".into(), Literal(Number(F64Value(52.08)))),
                        ]),
                    ]))
                )),
            })))
        }

        #[test]
        fn test_append_from_variable() {
            let model = Compiler::build(r#"
                append ns("compiler.append.stocks") from stocks
            "#).unwrap();
            assert_eq!(model, DatabaseOp(Mutation(Mutations::Append {
                path: Box::new(Ns(Box::new(Literal(StringValue("compiler.append.stocks".to_string()))))),
                source: Box::new(From(Box::new(Variable("stocks".into())))),
            })))
        }

        #[test]
        fn test_between() {
            assert_eq!(
                Compiler::build("20 between 1 and 20").unwrap(),
                Condition(Between(
                    Box::new(Literal(Number(I64Value(20)))),
                    Box::new(Literal(Number(I64Value(1)))),
                    Box::new(Literal(Number(I64Value(20)))),
                )));
        }

        #[test]
        fn test_betwixt() {
            assert_eq!(
                Compiler::build("20 betwixt 1 and 21").unwrap(),
                Condition(Betwixt(
                    Box::new(Literal(Number(I64Value(20)))),
                    Box::new(Literal(Number(I64Value(1)))),
                    Box::new(Literal(Number(I64Value(21)))),
                )));
        }

        #[test]
        fn test_create_index_in_namespace() {
            let code = Compiler::build(r#"
                create index ns("compiler.create.stocks") on [symbol, exchange]
            "#).unwrap();
            assert_eq!(code, DatabaseOp(Mutation(Create {
                path: Box::new(Ns(Box::new(Literal(StringValue("compiler.create.stocks".into()))))),
                entity: IndexEntity {
                    columns: vec![
                        Variable("symbol".into()),
                        Variable("exchange".into()),
                    ],
                },
            })));
        }

        #[test]
        fn test_create_table_in_namespace() {
            let ns_path = "compiler.create.stocks";
            let code = Compiler::build(r#"
                create table ns("compiler.create.stocks") (
                    symbol: String(8) = "ABC",
                    exchange: String(8) = "NYSE",
                    last_sale: f64 = 23.54)
                "#).unwrap();
            assert_eq!(code, DatabaseOp(Mutation(Create {
                path: Box::new(Ns(Box::new(Literal(StringValue(ns_path.into()))))),
                entity: TableEntity {
                    columns: vec![
                        Parameter::with_default("symbol", StringType(8), StringValue("ABC".into())),
                        Parameter::with_default("exchange", StringType(8), StringValue("NYSE".into())),
                        Parameter::with_default("last_sale", NumberType(F64Kind), Number(F64Value(23.54))),
                    ],
                    from: None,
                },
            })));
        }

        #[test]
        fn test_create_table_fn_in_namespace() {
            let ns_path = "compiler.journal.stocks";
            let model = Compiler::build(format!(r#"
                create table ns("{ns_path}") fn(
                   symbol: String(8), exchange: String(8), last_sale: f64
                ) => {{ symbol: symbol, market: exchange, last_sale: last_sale, process_time: cal::now() }}
            "#).as_str()).unwrap();
            assert_eq!(model, DatabaseOp(Mutation(Mutations::Create {
                path: Box::new(Ns(Box::new(Literal(StringValue(ns_path.into()))))),
                entity: CreationEntity::TableFnEntity {
                    fx: Box::new(FnExpression {
                        params: vec![
                            Parameter::new("symbol", StringType(8)),
                            Parameter::new("exchange", StringType(8)),
                            Parameter::new("last_sale", NumberType(F64Kind)),
                        ],
                        body: Some(Box::new(StructureExpression(vec![
                            ("symbol".to_string(), Variable("symbol".into())),
                            ("market".to_string(), Variable("exchange".into())),
                            ("last_sale".to_string(), Variable("last_sale".into())),
                            ("process_time".to_string(), ColonColon(
                                Box::new(Variable("cal".into())),
                                Box::new(FunctionCall {
                                    fx: Box::new(Variable("now".into())),
                                    args: vec![]
                                })))
                        ]))),
                        returns: StructureType(vec![
                            Parameter::new("symbol", StringType(8)),
                            Parameter::new("market", StringType(8)),
                            Parameter::new("last_sale", NumberType(F64Kind)),
                            Parameter::new("process_time", NumberType(DateKind)),
                        ])
                    })
                }
            })))
        }

        #[test]
        fn test_create_table_with_journaling() {
            let ns_path = "compiler.journal.stocks";
            let model = Compiler::build(format!(r#"
                create table ns("{ns_path}") (
                   symbol: String(8), exchange: String(8), last_sale: f64
                ):::{{ journaling: true }}
            "#).as_str()).unwrap();
            assert_eq!(model, ColonColonColon(
                Box::new(DatabaseOp(Mutation(Create {
                    path: Box::new(Ns(Box::new(Literal(StringValue(ns_path.into()))))),
                    entity: TableEntity {
                        columns: vec![
                            Parameter::new("symbol", StringType(8)),
                            Parameter::new("exchange", StringType(8)),
                            Parameter::new("last_sale", NumberType(F64Kind)),
                        ],
                        from: None,
                    }
                }))),
                Box::new(StructureExpression(vec![
                    ("journaling".to_string(), Condition(True))
                ]))
            ));
        }

        #[test]
        fn test_declare_table() {
            let model = Compiler::build(r#"
                table(
                    symbol: String(8),
                    exchange: String(8),
                    last_sale: f64)
            "#).unwrap();
            assert_eq!(model, DatabaseOp(Mutation(Declare(TableEntity {
                columns: vec![
                    Parameter::new("symbol", StringType(8)),
                    Parameter::new("exchange", StringType(8)),
                    Parameter::new("last_sale", NumberType(F64Kind)),
                ],
                from: None,
            }))));
        }

        #[test]
        fn test_declare_table_with_ttl() {
            let model = Compiler::build(r#"
              table(symbol: String(8), exchange: String(8), last_sale: f64):::{
                 ttl: 3:::days()
              }
            "#).unwrap();
            assert_eq!(model, ColonColonColon(
                Box::new(DatabaseOp(Mutation(Declare(TableEntity {
                    columns: vec![
                        Parameter::new("symbol", StringType(8)),
                        Parameter::new("exchange", StringType(8)),
                        Parameter::new("last_sale", NumberType(F64Kind)),
                    ],
                    from: None,
                })))),
                Box::new(StructureExpression(vec![
                    ("ttl".to_string(), ColonColonColon(
                        Box::new(Literal(Number(I64Value(3)))),
                        Box::new(FunctionCall { fx: Box::new(Variable("days".into())), args: vec![] })
                    ))
                ]))
            ));
        }

        #[test]
        fn test_delete() {
            let model = Compiler::build(r#"
                delete from stocks
            "#).unwrap();
            assert_eq!(model, DatabaseOp(Mutation(
                Mutations::Delete {
                    path: Box::new(Variable("stocks".into())),
                    condition: None,
                    limit: None,
                })))
        }

        #[test]
        fn test_delete_where_limit() {
            let model = Compiler::build(r#"
                delete from stocks
                where last_sale >= 1.0
                limit 100
            "#).unwrap();
            assert_eq!(model, DatabaseOp(Mutation(Mutations::Delete {
                path: Box::new(Variable("stocks".into())),
                condition: Some(GreaterOrEqual(
                    Box::new(Variable("last_sale".into())),
                    Box::new(Literal(Number(F64Value(1.0)))),
                )),
                limit: Some(Box::new(Literal(Number(I64Value(100))))),
            })))
        }

        #[test]
        fn test_drop_table() {
            let code = Compiler::build(r#"
                drop table ns('finance.securities.stocks')
            "#).unwrap();
            assert_eq!(
                code,
                DatabaseOp(Mutation(Drop(TableTarget {
                    path: Box::new(Ns(Box::new(Literal(StringValue("finance.securities.stocks".into()))))),
                })))
            );
        }

        #[test]
        fn test_from() {
            let model = Compiler::build("from stocks").unwrap();
            assert_eq!(model, From(Box::new(Variable("stocks".into()))));
        }

        #[test]
        fn test_from_where_limit() {
            let model = Compiler::build(r#"
                from stocks where last_sale >= 1.0 limit 20
            "#).unwrap();
            assert_eq!(model, DatabaseOp(Queryable(Queryables::Limit {
                from: Box::new(
                    DatabaseOp(Queryable(Queryables::Where {
                        from: Box::new(From(Box::new(Variable("stocks".into())))),
                        condition: GreaterOrEqual(
                            Box::new(Variable("last_sale".into())),
                            Box::new(Literal(Number(F64Value(1.0)))),
                        ),
                    }))),
                limit: Box::new(Literal(Number(I64Value(20)))),
            })
            ));
        }

        #[test]
        fn test_like() {
            assert_eq!(
                Compiler::build("'Hello' like 'H.ll.'").unwrap(),
                Condition(Like(
                    Box::new(Literal(StringValue("Hello".into()))),
                    Box::new(Literal(StringValue("H.ll.".into()))),
                )));
        }

        #[test]
        fn test_ns() {
            let code = Compiler::build(r#"
                ns("securities.etf.stocks")
            "#).unwrap();
            assert_eq!(code, Ns(Box::new(Literal(StringValue("securities.etf.stocks".to_string())))))
        }

        #[test]
        fn test_overwrite() {
            let model = Compiler::build(r#"
                overwrite stocks
                via {symbol: "ABC", exchange: "NYSE", last_sale: 0.2308}
                where symbol == "ABCQ"
                limit 5
            "#).unwrap();
            assert_eq!(model, DatabaseOp(Mutation(Mutations::Overwrite {
                path: Box::new(Variable("stocks".into())),
                source: Box::new(Via(Box::new(StructureExpression(vec![
                    ("symbol".into(), Literal(StringValue("ABC".into()))),
                    ("exchange".into(), Literal(StringValue("NYSE".into()))),
                    ("last_sale".into(), Literal(Number(F64Value(0.2308)))),
                ])))),
                condition: Some(Equal(
                    Box::new(Variable("symbol".into())),
                    Box::new(Literal(StringValue("ABCQ".into()))),
                )),
                limit: Some(Box::new(Literal(Number(I64Value(5))))),
            })))
        }

        #[test]
        fn test_select_from_variable() {
            let model = Compiler::build(r#"
                select symbol, exchange, last_sale from stocks
                "#).unwrap();
            assert_eq!(model, DatabaseOp(Queryable(Queryables::Select {
                fields: vec![Variable("symbol".into()), Variable("exchange".into()), Variable("last_sale".into())],
                from: Some(Box::new(Variable("stocks".into()))),
                condition: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
            })))
        }

        #[test]
        fn test_select_from_where() {
            let model = Compiler::build(r#"
                select symbol, exchange, last_sale from stocks
                where last_sale >= 1.0
                "#).unwrap();
            assert_eq!(model, DatabaseOp(Queryable(
                Queryables::Select {
                    fields: vec![Variable("symbol".into()), Variable("exchange".into()), Variable("last_sale".into())],
                    from: Some(Box::new(Variable("stocks".into()))),
                    condition: Some(GreaterOrEqual(
                        Box::new(Variable("last_sale".into())),
                        Box::new(Literal(Number(F64Value(1.0)))),
                    )),
                    group_by: None,
                    having: None,
                    order_by: None,
                    limit: None,
                })))
        }

        #[test]
        fn test_select_from_where_limit() {
            let model = Compiler::build(r#"
                select symbol, exchange, last_sale from stocks
                where last_sale <= 1.0
                limit 5
                "#).unwrap();
            assert_eq!(model, DatabaseOp(Queryable(Queryables::Select {
                fields: vec![Variable("symbol".into()), Variable("exchange".into()), Variable("last_sale".into())],
                from: Some(Box::new(Variable("stocks".into()))),
                condition: Some(LessOrEqual(
                    Box::new(Variable("last_sale".into())),
                    Box::new(Literal(Number(F64Value(1.0)))),
                )),
                group_by: None,
                having: None,
                order_by: None,
                limit: Some(Box::new(Literal(Number(I64Value(5))))),
            })))
        }

        #[test]
        fn test_select_from_where_order_by_limit() {
            let opcode = Compiler::build(r#"
                select symbol, exchange, last_sale from stocks
                where last_sale < 1.0
                order by symbol
                limit 5
                "#).unwrap();
            assert_eq!(opcode, DatabaseOp(Queryable(Queryables::Select {
                fields: vec![Variable("symbol".into()), Variable("exchange".into()), Variable("last_sale".into())],
                from: Some(Box::new(Variable("stocks".into()))),
                condition: Some(LessThan(
                    Box::new(Variable("last_sale".into())),
                    Box::new(Literal(Number(F64Value(1.0)))),
                )),
                group_by: None,
                having: None,
                order_by: Some(vec![Variable("symbol".into())]),
                limit: Some(Box::new(Literal(Number(I64Value(5))))),
            })))
        }

        #[test]
        fn test_undelete() {
            let model = Compiler::build(r#"
                undelete from stocks
                "#).unwrap();
            assert_eq!(model, DatabaseOp(Mutation(Mutations::Undelete {
                path: Box::new(Variable("stocks".into())),
                condition: None,
                limit: None,
            })))
        }

        #[test]
        fn test_undelete_where_limit() {
            let model = Compiler::build(r#"
                undelete from stocks
                where last_sale >= 1.0
                limit 100
                "#).unwrap();
            assert_eq!(model, DatabaseOp(Mutation(Mutations::Undelete {
                path: Box::new(Variable("stocks".into())),
                condition: Some(GreaterOrEqual(
                    Box::new(Variable("last_sale".into())),
                    Box::new(Literal(Number(F64Value(1.0)))),
                )),
                limit: Some(Box::new(Literal(Number(I64Value(100))))),
            })))
        }

        #[test]
        fn test_update() {
            let model = Compiler::build(r#"
                update stocks
                via { last_sale: 0.1111 }
                where symbol == "ABC"
                limit 10
                "#).unwrap();
            assert_eq!(model, DatabaseOp(Mutation(Mutations::Update {
                path: Box::new(Variable("stocks".into())),
                source: Box::new(Via(Box::new(StructureExpression(vec![
                    ("last_sale".into(), Literal(Number(F64Value(0.1111)))),
                ])))),
                condition: Some(Equal(
                    Box::new(Variable("symbol".into())),
                    Box::new(Literal(StringValue("ABC".into()))),
                )),
                limit: Some(Box::new(Literal(Number(I64Value(10))))),
            })))
        }

        #[test]
        fn test_write_json_into_namespace() {
            let model = Compiler::build(r#"
                [{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                 { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }]
                        ~> ns("interpreter.into.stocks")
                "#).unwrap();
            assert_eq!(model, CurvyArrowRight(
                Box::new(ArrayExpression(vec![
                    StructureExpression(vec![
                        ("symbol".into(), Literal(StringValue("ABC".into()))),
                        ("exchange".into(), Literal(StringValue("AMEX".into()))),
                        ("last_sale".into(), Literal(Number(F64Value(12.49)))),
                    ]),
                    StructureExpression(vec![
                        ("symbol".into(), Literal(StringValue("BOOM".into()))),
                        ("exchange".into(), Literal(StringValue("NYSE".into()))),
                        ("last_sale".into(), Literal(Number(F64Value(56.88)))),
                    ]),
                    StructureExpression(vec![
                        ("symbol".into(), Literal(StringValue("JET".into()))),
                        ("exchange".into(), Literal(StringValue("NASDAQ".into()))),
                        ("last_sale".into(), Literal(Number(F64Value(32.12)))),
                    ]),
                ])),
                Box::new(Ns(Box::new(Literal(StringValue("interpreter.into.stocks".into()))))),
            ));
        }
    }

    /// Type tests
    #[cfg(test)]
    mod type_tests {
        use crate::compiler::Compiler;
        use crate::expression::Expression::{FunctionCall, Literal, TypeDef, Variable};
        use crate::numbers::Numbers::I64Value;
        use crate::typed_values::TypedValue::Number;

        #[test]
        fn test_simple_type_declaration() {
            let model = Compiler::build(r#"
                typedef(String(32))
            "#).unwrap();
            assert_eq!(model, TypeDef(
                Box::new(FunctionCall {
                    fx: Box::new(Variable("String".into())),
                    args: vec![
                        Literal(Number(I64Value(32)))
                    ]
                })))
        }
    }
}