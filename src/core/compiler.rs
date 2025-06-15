#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Compiler class
////////////////////////////////////////////////////////////////////

use crate::byte_code_compiler::ByteCodeCompiler;
use crate::data_types::DataType;
use crate::data_types::DataType::{FunctionType, UnresolvedType};
use crate::dataframe::Dataframe;
use crate::errors::Errors::{CompilerError, Exact, ExactNear, SyntaxError};
use crate::errors::SyntaxErrors::IllegalExpression;
use crate::errors::{throw, CompileErrors, SyntaxErrors};
use crate::expression::Expression::*;
use crate::expression::Ranges::{Exclusive, Inclusive};
use crate::expression::*;
use crate::numbers::Numbers::*;
use crate::parameter::Parameter;
use crate::token_slice::TokenSlice;
use crate::tokens::Token;
use crate::tokens::Token::*;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{BinaryValue, DateValue, Kind, Null, Number, StringValue, TableValue};
use crate::utils::{pull_name, pull_variable_name};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use shared_lib::cnv_error;
use std::fs;
use std::ops::Deref;

/// Oxide language compiler - converts source code into [Expression]s.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Compiler;

impl Compiler {
    ////////////////////////////////////////////////////////////////
    // static methods
    ////////////////////////////////////////////////////////////////

    /// compiles the source code into a [Vec<Expression>]; a graph representing
    /// the program's executable code
    pub fn build(source_code: &str) -> std::io::Result<Expression> {
        let (code, _) = Self::new().compile_fully(TokenSlice::from_string(source_code))?;
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
        Compiler {}
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
    pub fn compile_fully(&self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        // consume the entire iterator
        let mut models = Vec::new();
        let mut ts = ts;
        while ts.has_more() {
            let (expr, ts1) = self.compile_next(ts)?;
            models.push(expr);
            ts = ts1;
        }

        // return the instruction
        match models {
            ops if ops.len() == 1 => Ok((ops[0].to_owned(), ts)),
            ops => Ok((CodeBlock(ops), ts)),
        }
    }

    ////////////////////////////////////////////////////////////////////
    // precedence logic
    ////////////////////////////////////////////////////////////////////

    pub fn compile_next(
        &self,
        mut ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let mut output: Vec<Expression> = Vec::new();
        let mut operators: Vec<Token> = Vec::new();
        let mut pos = 0;
        let mut is_done = false;
        while !is_done && ts.has_more() {
            let is_even = pos % 2 == 0;
            if let (Some(token), ts0) = &ts.next() {
                match token {
                    // break on a semicolon
                    t if t.get().as_str() == ";" => {
                        ts = ts0.clone();
                        is_done = true
                    }
                    // is it a unary operator?
                    t if is_unary_operator(t, is_even) => {
                        if get_exponent_symbols().contains(&t.get().as_str()) {
                            let expr1 = output.pop().unwrap();
                            output.push(apply_operator_unary(&t, expr1)?);
                            ts = ts0.clone();
                        } else {
                            let (expr1, ts1) = self.compile_next(ts0.clone())?;
                            output.push(apply_operator_unary(&t, expr1)?);
                            ts = ts1;
                        }
                    }
                    // is it an array or element-at-index expression?
                    t if t.is("[") => {
                        match is_even {
                            // arrays: "[1, 4, 2, 8, 5, 7]"
                            true =>
                                match self.next_list_of_items(&ts0, "]", |items| Ok(Some(ArrayExpression(items))))? {
                                    (Some(expr1), ts1) => {
                                        output.push(expr1);
                                        ts = ts1;
                                    }
                                    (None, _) => is_done = true
                                }
                            // element-at-index: "arr[1]"
                            false => {
                                if ts.is_previous_on_same_line() {
                                    let expr1 = output.pop().unwrap();
                                    let (expr2, ts1) = self.compile_next(ts0.clone())?;
                                    ts = ts1.expect("]")?;
                                    output.push(ElementAt(expr1.into(), expr2.into()));
                                } else {
                                    is_done = true;
                                }
                            }
                        }
                    }
                    // is it a quantity or tuple expression?
                    t if t.is("(") => {
                        match self.next_list_of_items(&ts0, ")", |items| {
                            // if at least one item is a parameter, convert the others
                            let result = if items.iter().any(|item| matches!(item, Parameters(..))) {
                               Parameters(convert_to_parameters(items.clone())?)
                            } else {
                                match items.as_slice() {
                                    [item] => item.clone(),
                                    items => TupleExpression(items.to_vec())
                                }
                            };
                            Ok(Some(result))
                        })? {
                            (Some(expr1), ts1) => {
                                output.push(expr1);
                                ts = ts1;
                            }
                            (None, _) => is_done = true
                        }
                    }
                    // skip barrier operators
                    t if matches!(t.get().as_str(), ")" | "]" | "}" | "," | ";") => is_done = true,
                    // handle other operators
                    t if is_operator(t, is_even) => {
                        while let Some(top_op) = operators.last() {
                            if get_precedence(top_op, is_even) >= get_precedence(&t, is_even) {
                                let b = output.pop().unwrap();
                                let a = output.pop().unwrap();
                                let result = apply_operator_binary(top_op, b, a)?;
                                output.push(result);
                                operators.pop();
                            } else {
                                break;
                            }
                        }
                        operators.push(t.clone());
                        ts = ts0.clone();
                    }
                    // on is_even, it should be an expression
                    t if is_even => {
                        // codeblock | structure
                        if t.is("{") {
                            match self.next_operator_brackets_curly(ts0.clone())? {
                                (Some(expr1), ts1) => {
                                    output.push(expr1);
                                    ts = ts1;
                                }
                                (None, _) => is_done = true
                            }
                        } else {
                            let (maybe_expr, ts1) = self.next_expression_token(&ts)?;
                            match maybe_expr {
                                None => is_done = true,
                                Some(expr) => {
                                    output.push(expr);
                                    ts = ts1;
                                }
                            }
                        }
                    }
                    _t => is_done = true,
                }
                pos += 1;
            }
        }

        while let Some(op) = operators.pop() {
            match (output.pop(), output.pop()) {
                (Some(b), Some(a)) => {
                    let result = apply_operator_binary(&op, b, a)?;
                    output.push(result);
                }
                (Some(a), _) => return throw(SyntaxError(IllegalExpression(a.to_code()))),
                (_, Some(b)) => return throw(SyntaxError(IllegalExpression(b.to_code()))),
                _ => return throw(ExactNear("Syntax error".into(), ts.current())),
            }
        }

        match output.pop() {
            Some(expr) => Ok((expr, ts)),
            None => throw(ExactNear("Unexpected end of input".into(), ts.current())),
        }
    }

    ////////////////////////////////////////////////////////////////////
    // internal logic
    ////////////////////////////////////////////////////////////////////

    /// compiles the next [TokenSlice] into an [Expression]
    fn next_expression_token(
        &self,
        ts: &TokenSlice,
    ) -> std::io::Result<(Option<Expression>, TokenSlice)> {
        match ts.next() {
            // function, keyword or variable
            (Some(Atom { .. }), _) => self.next_keyword(ts.clone()),
            // binary literals
            (Some(BinaryLiteral { text, .. }), ts) => {
                let bytes = convert_binary_literal_to_bytes(&text)?;
                Ok((Some(Literal(BinaryValue(bytes))), ts))
            },
            // variables
            (Some(Backticks { text, .. }), ts) => Ok((Some(Variable(text)), ts)),
            // dataframe literals
            (Some(DataframeLiteral { cells, .. }), ts) => 
                Ok((Some(Literal(TableValue(Dataframe::from_cells(&cells)))), ts)),
            // date literals
            (Some(DateLiteral { text, .. }), ts) => {
                let date = text.parse::<DateTime<Utc>>()
                    .map(|date| DateValue(date.timestamp_millis()))
                    .map_err(|e| cnv_error!(e))?;
                Ok((Some(Literal(date)), ts))
            }
            // double- or single-quoted or URL strings
            (Some(DoubleQuoted { text, .. }
                  | SingleQuoted { text, .. }
                  | URL { text, .. }), ts) =>
                Ok((Some(Literal(StringValue(text))), ts)),
            // numeric values
            (Some(Numeric { text, .. }), ts) => {
                Ok((Some(Literal(TypedValue::from_numeric(text.as_str())?)), ts))
            }
            // unrecognized token
            _ => Ok((None, ts.clone())),
        }
    }
    
    /// Parses reserved words (i.e. keyword)
    fn next_keyword(&self, ts: TokenSlice) -> std::io::Result<(Option<Expression>, TokenSlice)> {
        if let (Some(Atom { text, .. }), nts) = ts.next() {
            let (expr, ts) = match text.as_str() {
                "assert" => self.parse_keyword_assert(nts),
                "delete" => self.parse_keyword_delete(nts),
                "DELETE" => self.parse_keyword_http(ts),
                "do" => self.parse_keyword_do_while(nts),
                "false" => Ok((FALSE, nts)),
                "Feature" => self.parse_keyword_feature(nts),
                "fn" => self.parse_keyword_fn(nts),
                "for" => self.parse_keyword_for(nts),
                "GET" => self.parse_keyword_http(ts),
                "HEAD" => self.parse_keyword_http(ts),
                "HTTP" => self.parse_keyword_http(nts),
                "if" => self.parse_keyword_if(nts),
                "include" => self.parse_expression_1a(nts, Include),
                "let" => self.parse_keyword_let(nts),
                "limit" => throw(ExactNear(
                    "`from` is expected before `limit`: from stocks limit 5".into(),
                    nts.current(),
                )),
                "match" => self.parse_keyword_match(nts),
                "mod" => self.parse_keyword_mod(nts),
                "NaN" => Ok((Literal(Number(NaNValue)), nts)),
                "null" => Ok((NULL, nts)),
                "PATCH" => self.parse_keyword_http(ts),
                "POST" => self.parse_keyword_http(ts),
                "PUT" => self.parse_keyword_http(ts),
                "Scenario" => return self.parse_keyword_scenario(nts),
                "select" => self.parse_keyword_select(nts),
                "throw" => self.parse_keyword_throw(nts),
                "true" => Ok((TRUE, nts)),
                "typedef" => self.parse_keyword_type_decl(nts),
                "type_of" => self.parse_keyword_type_of(nts),
                "undefined" => Ok((UNDEFINED, nts)),
                "undelete" => self.parse_keyword_undelete(nts),
                "use" => self.parse_keyword_use(nts),
                "whenever" => self.parse_keyword_whenever(nts),
                "while" => self.parse_keyword_while(nts),
                "yield" => self.parse_expression_1a(nts, Yield),
                name => self.expect_function_call_or_variable(name, nts),
            }?;
            Ok((Some(expr), ts))
        } else {
            throw(Exact("Unexpected end of input".into()))
        }
    }

    /// Parses the [TokenSlice] producing a list-of-items;
    /// including [ArrayExpression], [Parameters] and [TupleExpression]
    fn next_list_of_items<F>(
        &self,
        ts: &TokenSlice,
        terminator: &str,
        collect: F,
    ) -> std::io::Result<(Option<Expression>, TokenSlice)>
    where
        F: Fn(Vec<Expression>) -> std::io::Result<Option<Expression>>,
    {
        self.next_list_of_transformed_items(ts, terminator, collect, |expr| Ok(expr))
    }

    /// Parses the [TokenSlice] producing a list-of-transformed-items;
    /// including [ArrayExpression] and [TupleExpression]
    fn next_list_of_transformed_items<F, G>(
        &self,
        ts: &TokenSlice,
        terminator: &str,
        collect: F,
        transform: G,
    ) -> std::io::Result<(Option<Expression>, TokenSlice)>
    where
        F: Fn(Vec<Expression>) -> std::io::Result<Option<Expression>>,
        G: Fn(Expression) -> std::io::Result<Expression>,
    {
        let (mut items, mut ts) = (vec![], ts.clone());
        while ts.isnt(terminator) {
            if ts.is(",") { ts = ts.skip() };
            let (expr, tsn) = self.compile_next(ts)?;
            items.push(transform(expr)?);
            ts = tsn;
        }
        // collect and skip the terminator (e.g. ")")
        collect(items).map(|expr_maybe| (expr_maybe, ts.skip()))
    }

    fn next_operator_brackets_curly(
        &self,
        ts: TokenSlice,
    ) -> std::io::Result<(Option<Expression>, TokenSlice)> {
        // collect all items (expressions)
        self.next_list_of_items(&ts, "}", |items| {
            // if all items are key-value pairs, it's a JSON literal,
            // otherwise it's a code block
            if !items.is_empty() && items.iter().all(|expr| matches!(expr, NamedValue(..))) {
                let mut key_values = vec![];
                for item in items.iter() {
                    if let NamedValue(name, value) = item {
                        key_values.push((name.clone(), value.deref().clone()));
                    }
                }
                Ok(Some(StructureExpression(key_values)))
            } else {
                match items.as_slice() {
                    [expr] => Ok(Some(expr.clone())),
                    _ => Ok(Some(CodeBlock(items))),
                }
            }
        })
    }

    /// compiles the [TokenSlice] into a leading single-parameter [Expression] (e.g. -x)
    fn parse_expression_1a(
        &self,
        ts: TokenSlice,
        f: fn(Box<Expression>) -> Expression,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let (expr, ts) = self.compile_next(ts.clone())?;
        Ok((f(expr.into()), ts))
    }
    
    /// Creates an `assert` expression
    /// #### Examples
    /// ```
    /// assert last_sale > 0.00
    /// ```
    /// ```
    /// assert last_sale > 0.00, "last_sale must be greater than 0.00"
    /// ```
    fn parse_keyword_assert(&self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        let (condition, ts) = self.compile_next(ts.clone())?;
        let (message, ts) = match ts.next() {
            (Some(Operator { text, .. }), ts) if text == "," => {
                let (message, ts) = self.compile_next(ts.clone())?;
                (Some(message.into()), ts)
            }
            _ => (None, ts)
        };
        Ok((Assert { condition: condition.into(), message }, ts))
    }

    /// SQL Delete statement.
    /// #### Examples
    /// ```
    /// delete stocks where last_sale > 1.00
    /// ```
    fn parse_keyword_delete(&self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        let (model, ts) = self.compile_next(ts)?;
        unwind_sql_update_graph(model, |from, condition, limit| {
            Delete {
                from: from.into(),
                condition,
                limit,
            }
        }).map(|expr| (expr, ts))
    }

    /// Parses do-while statement
    /// #### Examples
    /// ```
    /// do {
    ///     let x = 10;
    ///     let y = 20;
    ///     yield x + y
    /// } while x < 100;
    /// ```
    fn parse_keyword_do_while(
        &self,
        ts: TokenSlice
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let (code, mut ts) = self.compile_next(ts.clone())?;
        ts = ts.expect("while")?;
        let (condition, ts) = self.compile_next(ts.clone())?;
        Ok((DoWhile { condition: condition.into(), code: code.into() }, ts))
    }

    fn parse_keyword_feature(&self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        let (title, ts) = self.compile_next(ts.clone())?;
        let ts = ts.expect("{")?;
        match self.next_operator_brackets_curly(ts)? {
            (None, ts) => throw(SyntaxError(SyntaxErrors::IllegalOperator(ts.current()))),
            (Some(code), ts) => {
                let scenarios = match code {
                    CodeBlock(scenarios) => scenarios,
                    StructureExpression(tuples) => tuples
                        .iter()
                        .map(|(name, expression)| Scenario {
                            title: Box::new(Literal(StringValue(name.to_owned()))),
                            verifications: match expression.to_owned() {
                                CodeBlock(ops) => ops,
                                z => vec![z],
                            },
                        })
                        .collect::<Vec<_>>(),
                    other => vec![other],
                };
                Ok((
                    Feature {
                        title: Box::new(title),
                        scenarios,
                    },
                    ts,
                ))
            }
        }
    }

    fn parse_keyword_scenario(
        &self,
        ts: TokenSlice,
    ) -> std::io::Result<(Option<Expression>, TokenSlice)> {
        let (title, ts) = self.compile_next(ts.clone())?;
        let ts = ts.expect("{")?;
        match self.next_operator_brackets_curly(ts)? {
            (None, ts) => Ok((None, ts)),
            (Some(code), ts) => match code {
                CodeBlock(verifications) => Ok((
                    Some(Scenario {
                        title: Box::new(title),
                        verifications,
                    }),
                    ts,
                )),
                other => Ok((
                    Some(Scenario {
                        title: Box::new(title),
                        verifications: vec![other],
                    }),
                    ts,
                )),
            },
        }
    }

    /// Builds a language model from a function variant
    /// #### Examples
    /// ```
    /// fn add(x, y) -> x + y
    /// ```
    /// ```
    /// fn add(x: i64, y: i64) -> x + y
    /// ```
    /// ```
    /// fn add(x: i64, y: i64): i64 -> x + y
    /// ```
    fn parse_keyword_fn(
        &self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        match ts.next() {
            // get a function name - it's a function declaration
            (Some(Atom { text: name, .. }), ts) => {
                let (expr, ts) = self.compile_next(ts.clone())?;
                Ok((SetVariables(
                    Variable(name.into()).into(),
                    expr.into()
                ), ts))
            }
            _ => {
                let (expr, ts) = self.compile_next(ts.clone())?;
                let params = match expr {
                    Variable(name) => vec![
                        Parameter::add(name)
                    ],
                    Parameters(params) => params,
                    TupleExpression(items) => convert_to_parameters(items)?,
                    _ => return throw(ExactNear("Parameters expected".into(), ts.current()))
                };
                Ok((Literal(Kind(FunctionType(params, UnresolvedType.into()))), ts))
            }
        }
    }

    /// Parses a for statement
    /// #### Examples
    /// ```
    /// for(i = 0, i < 5, i = i + 1) ...
    /// ```
    /// ```
    /// for [x, y, z] in [[1, 5, 3], [6, 11, 17], ...] ...
    /// ```
    /// ```
    /// for (c, n) in [('a', 5), ('c', 11), ...] ...
    /// ```
    /// ```
    /// for item in ['apple', 'berry', ...] ...
    /// ```
    /// ```
    /// for row in tools::to_table([{symbol:'ABC', price: 10.0}, ...]) ...
    /// ```
    fn parse_keyword_for(&self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        // for [item in items] [block]
        let (construct, ts) = self.compile_next(ts.clone())?;
        match construct {
            Condition(Conditions::In(item, items)) => {
                let (block, ts) = self.compile_next(ts.clone())?;
                Ok((For {
                    construct: Condition(Conditions::In(item, items)).into(),
                    op: block.into(),
                }, ts))
            }
            TupleExpression(components) if components.len() == 3 => {
                let (block, ts) = self.compile_next(ts.clone())?;
                Ok((For {
                    construct: TupleExpression(components).into(),
                    op: block.into(),
                }, ts))
            }
            _ => throw(ExactNear(
                format!("Expected `in` keyword: for item in items {{ ... }}"),
                ts.current(),
            )),
        }
    }

    /// Parses an HTTP expression.
    /// #### Examples
    /// ##### URL-Based Request
    /// ```http
    /// GET http://localhost:9000/quotes/AAPL/NYSE
    /// ```
    /// ##### Structured Request
    /// ```http
    /// POST {
    ///     url: http://localhost:8080/machine/append/stocks
    ///     body: stocks
    ///     headers: { "Content-Type": "application/json" }
    /// }
    /// ```
    /// #### Errors
    /// Returns an `std::io::Result` in case of parsing failures.
    /// #### Parameters
    /// - `ts`: A slice of tokens representing the HTTP expression.
    /// #### Returns
    /// - A tuple containing the parsed `Expression` and the remaining `TokenSlice`.
    fn parse_keyword_http(&self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        if let (Some(Atom { text: method, .. }), ts) = ts.next() {
            let (expr, ts) = self.compile_next(ts)?;
            match HttpMethodCalls::new(method.as_str(), expr) {
                None => throw(CompilerError(
                    CompileErrors::IllegalHttpMethod(method),
                    ts.current(),
                )),
                Some(call) => Ok((HTTP(call), ts)),
            }
        } else {
            throw(CompilerError(
                CompileErrors::ExpectedHttpMethod,
                ts.current(),
            ))
        }
    }

    /// Builds a language model from an `if` expression
    /// #### Example
    /// ```
    /// if (x > 5) x else 5
    /// ```
    /// ```
    /// if(x > 5, x, 5)
    /// ```
    fn parse_keyword_if(&self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        match self.compile_next(ts.clone())? {
            // if(x > 5, x, 5)
            (TupleExpression(items), ts) if items.len() == 3 => {
                Ok((If {
                    condition: items[0].clone().into(),
                    a: items[1].clone().into(),
                    b: Some(items[2].clone().into()),
                }, ts))
            }
            // invalid if(...)
            (TupleExpression(..), ts) =>
                throw(ExactNear(
                    "Expected 3 items in if function: if(x > 5, x, 5)".into(),
                    ts.current(),
                )),
            // if (x > 5) x else 5
            (condition, ts) => {
                let (a, ts) = self.compile_next(ts.clone())?;
                let (b, ts) = self.next_keyword_expr("else", ts)?;
                Ok((If {
                    condition: Box::new(condition),
                    a: Box::new(a),
                    b: b.map(Box::new),
                }, ts,))
            }
        }
    }

    /// Builds a variable assignment model
    /// #### Returns
    /// - an [Expression]
    /// #### Examples
    /// ```
    /// let n = 100
    /// ```
    /// ```
    /// let (x, y, z) = (3, 6, 9)
    /// ```
    /// ```
    /// let [a, b, c, d] = [1, 3, 5, 7]
    /// ```
    fn parse_keyword_let(&self, ts0: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        match self.compile_next(ts0.clone())? {
            (SetVariables(name, value), ts) => Ok((SetVariables(name, value), ts)),
            (SetVariablesExpr(name, value), ts) => Ok((SetVariablesExpr(name, value), ts)),
            _ => throw(ExactNear("Expected assignment: let x = y".into(), ts0.current())),
        }
    }

    /// Parses an use statement:
    /// #### Examples
    /// ```
    /// use "os"
    /// ```
    /// ```
    /// use str::format
    /// ```
    /// ```
    /// use oxide::[eval, serve, version]
    /// ```
    /// ```
    /// use "os", str::format, oxide::[eval, serve, version]
    /// ```
    fn parse_keyword_use(
        &self,
        mut ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let mut ops = Vec::new();
        let mut is_done = false;
        while !is_done {
            let (expr, ts1) = self.compile_next(ts.clone())?;
            ts = ts1;
            match expr {
                // use 'cnv'
                Literal(StringValue(pkg)) => ops.push(UseOps::Everything(pkg)),
                // use vm
                Variable(pkg) => ops.push(UseOps::Everything(pkg)),
                // use www::serve | oxide::[eval, serve, version]
                ColonColon(a, b) => {
                    match (*a, *b) {
                        // use www::serve
                        (Variable(pkg), Variable(func)) => {
                            ops.push(UseOps::Selection(pkg, vec![func]))
                        }
                        // use oxide::[eval, serve, version]
                        (Variable(pkg), ArrayExpression(items)) => {
                            let mut func_list = Vec::new();
                            for item in items {
                                match item {
                                    Literal(StringValue(name)) => func_list.push(name),
                                    Variable(name) => func_list.push(name),
                                    other => {
                                        return throw(SyntaxError(
                                            SyntaxErrors::TypeIdentifierExpected(other.to_code()),
                                        ))
                                    }
                                }
                            }
                            ops.push(UseOps::Selection(pkg, func_list))
                        }
                        (a, ..) => {
                            return throw(SyntaxError(SyntaxErrors::TypeIdentifierExpected(
                                a.to_code(),
                            )))
                        }
                    }
                }
                // syntax error
                other => {
                    return throw(SyntaxError(SyntaxErrors::TypeIdentifierExpected(
                        other.to_code(),
                    )))
                }
            }

            // if there's a comma, keep going
            is_done = !ts.is(",");
            if !is_done {
                ts = ts.skip()
            }
        }
        Ok((Use(ops), ts))
    }

    /// Translates a `match` declaration into an [Expression]
    /// #### Examples
    /// ```
    /// match code {
    ///     100 => "Accepted"
    ///     n when n in 101..104 => "Escalated"
    ///     n when n > 0 && n < 100 => "Pending"
    ///     _ => "Rejected"
    /// }
    /// ```
    fn parse_keyword_match(&self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        let (host, ts) = self.compile_next(ts.clone())?;
        let (block, ts) = self.compile_next(ts.clone())?;
        match block.clone() {
            CodeBlock(cases) => Ok((MatchExpression(host.into(), cases), ts)),
            _ => throw(ExactNear("Expected a sequence of match cases".into(), ts.current())),
        }
    }

    /// Translates a `mod` declaration into an [Expression]
    /// #### Examples
    /// ```
    /// mod "abc" {
    ///     fn hello() => "hello"
    /// }
    /// ```
    /// ```
    /// mod abc {
    ///     fn hello() => "hello"
    /// }
    /// abc::hello()
    /// ```
    fn parse_keyword_mod(&self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        // get the model name: mod "abc"
        if let (Some(Atom { text: name, ..}), ts1) = ts.next() {
            // get the codeblock: { fn hello() => "hello" }
            match self.compile_next(ts1.clone())? {
                (CodeBlock(ops), ts) => Ok((Module(name, ops), ts)),
                (expr, ts) => Ok((Module(name, vec![expr]), ts)),
            }
        } else { 
            throw(ExactNear("Expected module name".into(), ts.current()))
        }
    }

    /// Builds a language model from a SELECT statement:
    /// #### Examples
    /// ```
    /// select sum(last_sale) from stocks 
    /// group_by exchange
    /// ```
    fn parse_keyword_select(&self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        let (fields, ts) = self.next_expression_list(ts)?;
        let fields = fields.expect("At least one field is required");
        let (from, ts) = self.next_keyword_expr("from", ts)?;
        Ok((Select {
            fields,
            from: from.map(Box::new),
        }, ts))
    }

    /// Builds a `throw` declaration
    /// #### Examples
    /// ```
    /// throw("Expected a string value")
    /// ```
    fn parse_keyword_throw(&self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        let (expr, ts) = self.compile_next(ts.clone())?;
        Ok((Throw(expr.into()), ts))
    }
    
    /// Builds a type declaration
    /// #### Examples
    /// ```
    /// typedef(String(80))
    /// ```
    fn parse_keyword_type_decl(&self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        let (expr, ts) = self.compile_next(ts.clone())?;
        Ok((TypeDef(expr.into()), ts))
    }

    /// Builds a `type_of` declaration
    /// #### Examples
    /// ```
    /// type_of(name)
    /// ```
    /// ```
    /// type_of num
    /// ``
    fn parse_keyword_type_of(&self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        let (expr, ts) = self.compile_next(ts.clone())?;
        Ok((TypeOf(expr.into()), ts))
    }

    /// Builds a language model from an UNDELETE statement:
    /// ex: undelete stocks where symbol == "BANG"
    fn parse_keyword_undelete(&self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        let (model, ts) = self.compile_next(ts)?;
        unwind_sql_update_graph(model, |from, condition, limit| {
            Undelete {
                from: from.into(),
                condition,
                limit,
            }
        }).map(|expr| (expr, ts))
    }
    
    /// Builds a language model for a `when` expression
    /// #### Examples
    /// ```
    /// let x = 1
    /// whenever x <= 0 {
    ///     x_boundary_hit()
    /// }
    /// x = x - 1
    /// ```
    fn parse_keyword_whenever(&self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        let (condition, ts) = self.compile_next(ts.clone())?;
        let (code, ts) = self.compile_next(ts.clone())?;
        Ok((WhenEver { condition: condition.into(), code: code.into() }, ts))
    }

    /// Builds a language model for a `while` expression
    /// #### Examples
    /// ```
    /// let x = 0
    /// while (x < 5) {
    ///     yield x = x + 1
    /// }
    /// ```
    fn parse_keyword_while(&self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        let (condition, ts) = self.compile_next(ts.clone())?;
        let (code, ts) = self.compile_next(ts.clone())?;
        Ok((While { condition: condition.into(), code: code.into() }, ts))
    }

    /// parse an argument list from the [TokenSlice] (e.g. "(symbol, exchange, last_sale)")
    fn expect_arguments(
        &self,
        ts: TokenSlice,
    ) -> std::io::Result<(Vec<Expression>, TokenSlice)> {
        // parse: ("abc", "123", ..)
        let mut args = Vec::new();
        let mut ts = ts.expect("(")?;
        while ts.isnt(")") {
            let (expr, ats) = self.compile_next(ts.clone())?;
            args.push(expr);
            ts = if ats.is(")") { ats } else { ats.expect(",")? }
        }
        Ok((args, ts.expect(")")?))
    }

    /// Expects a function call or variable
    /// ex: String(32) | f(2, 3) | abc
    fn expect_function_call_or_variable(
        &self,
        name: &str,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        // is it a function call? e.g., f(2, 3)
        if ts.is("(") {
            let (args, ts) = self.expect_arguments(ts)?;
            let fx = FunctionCall {
                fx: Box::new(Variable(name.to_string())),
                args,
            };
            // is it a type declaration? e.g., String(32)
            match DataType::decipher_type(&fx) {
                Ok(data_type) => Ok((Literal(Kind(data_type)), ts)),
                Err(_) => Ok((fx, ts))
            }
        }
        // must be a variable. e.g., abc
        else {
            Ok((Variable(name.to_string()), ts))
        }
    }

    /// Expects a single function parameter
    /// #### Examples
    /// ```
    /// a: i64
    /// ```
    fn expect_parameter(&self, ts: TokenSlice) -> std::io::Result<(Parameter, TokenSlice)> {
        // attempt to match the parameter name
        // name: String(8) | cost: Float = 0.0
        match ts.next() {
            (Some(Atom { text: name, .. }), ts) => {
                // next, check for type constraint
                let (typedef, ts) = if ts.is(":") {
                    self.expect_parameter_type_decl(ts.skip())?
                } else {
                    (None, ts)
                };

                // finally, check for a default value
                let (default_value, ts) = if ts.is("=") {
                    let ts = ts.skip();
                    let (expr, ts) = self.compile_next(ts)?;
                    (expr.to_pure()?, ts)
                } else {
                    (Null, ts)
                };

                Ok((
                    Parameter::new_with_default(name, typedef.unwrap_or(UnresolvedType), default_value),
                    ts,
                ))
            }
            (_, tsn) => throw(ExactNear("Function name expected".into(), tsn.current())),
        }
    }

    /// Expects a parameter type declaration
    /// ex: String(8)
    fn expect_parameter_type_decl(
        &self,
        ts: TokenSlice,
    ) -> std::io::Result<(Option<DataType>, TokenSlice)> {
        if let (
            Some(Atom {
                text: type_name, ..
            }),
            ts,
        ) = ts.next()
        {
            // e.g. String(8)
            if ts.is("(") {
                let (args, ts) = self.expect_arguments(ts)?;
                let kind = DataType::from_str(
                    format!(
                        "{}({})",
                        type_name,
                        args.iter()
                            .map(|expr| expr.to_code())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                    .as_str(),
                )?;
                Ok((Some(kind), ts))
            } else {
                Ok((Some(DataType::from_str(type_name.as_str())?), ts))
            }
        } else {
            Ok((None, ts))
        }
    }

    /// parse an expression list from the [TokenSlice] (e.g. ['x', ',', 'y', ',', 'z'])
    fn next_expression_list(
        &self,
        ts: TokenSlice,
    ) -> std::io::Result<(Option<Vec<Expression>>, TokenSlice)> {
        let mut items = Vec::new();
        // get the first item
        let (item, mut ts) = self.compile_next(ts.clone())?;
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
    fn next_keyword_expr(
        &self,
        keyword: &str,
        ts: TokenSlice,
    ) -> std::io::Result<(Option<Expression>, TokenSlice)> {
        if ts.isnt(keyword) {
            Ok((None, ts))
        } else {
            let (expr, ts) = self.compile_next(ts.skip())?;
            Ok((Some(expr), ts))
        }
    }
    
}

fn apply_colon(
    left: Expression, 
    right: Expression
) -> std::io::Result<Expression> {
    let b_as_type = DataType::decipher_type(&right);
    let b_is_type = b_as_type.is_ok();
    match (left, right) {
        (Literal(Kind(FunctionType(params, _))), _b) if b_is_type => 
            Ok(Literal(Kind(FunctionType(params, b_as_type?.into())))),
        (Parameters(params), _b) if b_is_type =>
            Ok(Literal(Kind(FunctionType(params, b_as_type?.into())))),
        (TupleExpression(args), _b) if b_is_type =>
            Ok(Literal(Kind(FunctionType(convert_to_parameters(args)?, b_as_type?.into())))),
        (Variable(name), _b) if b_is_type =>
            Ok(Parameters(vec![Parameter::new(name, b_as_type?)])),
        (a, b) => 
            Ok(NamedValue(pull_name(&a)?, b.into())),
    }
}

fn apply_operator_binary(
    op: &Token,
    b: Expression,
    a: Expression
) -> std::io::Result<Expression> {
    let (aa, bb) = (a.clone().into(), b.clone().into());
    match op.get().as_str() {
        "<~" => Ok(ArrowCurvyLeft(aa, bb)),
        "<<~" => Ok(ArrowCurvyLeft2x(aa, bb)),
        "~>" => Ok(ArrowCurvyRight(aa, bb)),
        "~>>" => Ok(ArrowCurvyRight2x(aa, bb)),
        "=>" => Ok(ArrowFat(aa, bb)),
        "<-" => Ok(ArrowSkinnyLeft(aa, bb)),
        "->" => Ok(ArrowSkinnyRight(aa, bb)),
        "|>" => Ok(ArrowVerticalBar(aa, bb)),
        "|>>" => Ok(ArrowVerticalBar2x(aa, bb)),
        "&" => Ok(BitwiseAnd(aa, bb)),
        "|" => Ok(BitwiseOr(aa, bb)),
        "<<" => Ok(BitwiseShiftLeft(aa, bb)),
        ">>" => Ok(BitwiseShiftRight(aa, bb)),
        "^" => Ok(BitwiseXor(aa, bb)),
        "?" => Ok(Coalesce(aa, bb)),
        "!?" => Ok(CoalesceErr(aa, bb)),
        ":" => apply_colon(a, b),
        "::" => Ok(ColonColon(aa, bb)),
        ":::" => Ok(ColonColonColon(aa, bb)),
        "&&" => Ok(Condition(Conditions::And(aa, bb))),
        "contains" => Ok(Condition(Conditions::Contains(aa, bb))),
        "==" | "is" => Ok(Condition(Conditions::Equal(aa, bb))),
        "when" => Ok(Condition(Conditions::When(aa, bb))),
        ">=" => Ok(Condition(Conditions::GreaterOrEqual(aa, bb))),
        ">" => Ok(Condition(Conditions::GreaterThan(aa, bb))),
        "in" => Ok(Condition(Conditions::In(aa, bb))),
        "<=" => Ok(Condition(Conditions::LessOrEqual(aa, bb))),
        "<" => Ok(Condition(Conditions::LessThan(aa, bb))),
        "like" => Ok(Condition(Conditions::Like(aa, bb))),
        "matches" => Ok(Condition(Conditions::Matches(aa, bb))),
        "!=" | "isnt" => Ok(Condition(Conditions::NotEqual(aa, bb))),
        "||" => Ok(Condition(Conditions::Or(aa, bb))),
        "÷" | "/" => Ok(Divide(aa, bb)),
        "." => Ok(Infix(aa, bb)),
        "-" => Ok(Minus(aa, bb)),
        "%" => Ok(Modulo(aa, bb)),
        "×" | "*" => Ok(Multiply(aa, bb)),
        "+" => Ok(Plus(aa, bb)),
        "++" => Ok(PlusPlus(aa, bb)),
        "**" => Ok(Pow(aa, bb)),
        ".." => Ok(Range(Exclusive(aa, bb))),
        "..=" => Ok(Range(Inclusive(aa, bb))),
        "=" => Ok(SetVariables(aa, bb)),
        "&=" => Ok(SetVariables(aa.clone(), BitwiseAnd(aa, bb).into())),
        "|=" => Ok(SetVariables(aa.clone(), BitwiseOr(aa, bb).into())),
        "<<=" => Ok(SetVariables(aa.clone(), BitwiseShiftLeft(aa, bb).into())),
        ">>=" => Ok(SetVariables(aa.clone(), BitwiseShiftRight(aa, bb).into())),
        "^=" => Ok(SetVariables(aa.clone(), BitwiseXor(aa, bb).into())),
        "?=" => Ok(SetVariables(aa.clone(), Coalesce(aa, bb).into())),
        "&&=" => Ok(SetVariables(aa.clone(), Condition(Conditions::And(aa, bb)).into())),
        "||=" => Ok(SetVariables(aa.clone(), Condition(Conditions::Or(aa, bb)).into())),
        "/=" => Ok(SetVariables(aa.clone(), Divide(aa, bb).into())),
        "-=" => Ok(SetVariables(aa.clone(), Minus(aa, bb).into())),
        "%=" => Ok(SetVariables(aa.clone(), Modulo(aa, bb).into())),
        "*=" => Ok(SetVariables(aa.clone(), Multiply(aa, bb).into())),
        "+=" => Ok(SetVariables(aa.clone(), Plus(aa, bb).into())),
        ":=" => Ok(SetVariablesExpr(aa, bb)),
        "group_by" => Ok(GroupBy { from: aa, columns: vec![b] }),
        "having" => Ok(Having { from: aa, condition: must_be_condition(bb.deref())? }),
        "limit" => Ok(Limit { from: aa, limit: bb }),
        "order_by" => Ok(OrderBy { from: aa, columns: vec![b] }),
        "where" => Ok(Where { from: aa, condition: must_be_condition(bb.deref())? }),
        sym => throw(CompilerError(CompileErrors::IllegalOperator(sym.into()), op.clone())),
    }
}

fn apply_operator_unary(
    op: &Token,
    a: Expression
) -> std::io::Result<Expression> {
    let exponents = get_exponent_symbols();
    match op.get().as_str() {
        "+" => Ok(a),
        "-" => Ok(Neg(a.into())),
        "!" => Ok(Condition(Conditions::Not(a.into()))),
        "&" => Ok(Referenced(a.into())),
        sym if exponents.contains(&sym) => {
            let expo = exponents.iter().position(|s| *s == sym ).unwrap();
            Ok(Pow(a.into(), Literal(Number(I64Value(expo as i64))).into()))
        }
        sym => throw(CompilerError(CompileErrors::IllegalOperator(sym.into()), op.clone())),
    }
}

pub fn unwind_sql_query_graph<F, E>(
    expr: Expression,
    f: F
) -> std::io::Result<E>
where
    F: Fn(Expression, Vec<Expression>, Option<Expression>, Option<Vec<Expression>>, Option<Box<Expression>>, Option<Vec<Expression>>, Option<Box<Expression>>) -> std::io::Result<E>,
{
    fn unwind<T, S>(
        from: Expression,
        fields: Vec<Expression>,
        condition: Option<Expression>,
        group_by: Option<Vec<Expression>>,
        having: Option<Box<Expression>>,
        order_by: Option<Vec<Expression>>,
        limit: Option<Box<Expression>>,
        f: T,
    ) -> std::io::Result<S>
    where
        T: Fn(Expression, Vec<Expression>, Option<Expression>, Option<Vec<Expression>>, Option<Box<Expression>>, Option<Vec<Expression>>, Option<Box<Expression>>) -> std::io::Result<S>,
    {
        match from {
            GroupBy { from, columns: group_by } =>
                unwind(from.deref().clone(), fields, condition, Some(group_by), having, order_by, limit, f),
            Having { from, condition } =>
                unwind(from.deref().clone(), fields, Some(Condition(condition)), group_by, having, order_by, limit, f),
            Limit { from, limit } =>
                unwind(from.deref().clone(), fields, condition, group_by, having, order_by, Some(limit), f),
            OrderBy { from, columns: order_by } =>
                unwind(from.deref().clone(), fields, condition, group_by, having, Some(order_by), limit, f),
            Select { fields, from, .. } => {
                let from = match from {
                    None => UNDEFINED,
                    Some(expr) => expr.deref().clone()
                };
                unwind(from, fields, condition, group_by, having, order_by, limit, f)
            }
            Where { from, condition } =>
                unwind(from.deref().clone(), fields, Some(Condition(condition)), group_by, having, order_by, limit, f),
            _ => f(from, fields, condition, group_by, having, order_by, limit)
        }
    }
    unwind(expr, vec![], None, None, None, None, None, f)
}

pub fn unwind_sql_update_graph<F, E>(
    expr: Expression, 
    f: F
) -> std::io::Result<E>
where
    F: Fn(Expression, Option<Conditions>, Option<Box<Expression>>) -> E,
{
    fn unwind<T, S>(
        from: Expression,
        condition: Option<Conditions>,
        limit: Option<Box<Expression>>,
        f: T,
    ) -> std::io::Result<S>
    where
        T: Fn(Expression, Option<Conditions>, Option<Box<Expression>>) -> S,
    {
        match from {
            GroupBy { .. } =>
                throw(Exact("group_by is not supported in this context".into())),
            Having { .. } =>
                throw(Exact("having is not supported in this context".into())),
            Limit { from, limit } =>
                unwind(from.deref().clone(), condition, Some(limit.clone()), f),
            OrderBy { .. } =>
                throw(Exact("order_by is not supported in this context".into())),
            Select { .. } =>
                throw(Exact("select is not supported in this context".into())),
            Where { from, condition } =>
                unwind(from.deref().clone(), Some(condition.clone()), limit, f),
            _ => Ok(f(from, condition, limit))
        }
    }
    unwind(expr, None, None, f)
}

fn convert_binary_literal_to_bytes(text: &str) -> std::io::Result<Vec<u8>> {
    use itertools::Itertools;
    let mut bytes = vec![];
    for chunk in text[2..].chars().chunks(2).into_iter() {
        let pair = chunk.collect::<String>();
        let byte = u8::from_str_radix(&pair, 16)
            .map_err(|e| cnv_error!(e))?;
        bytes.push(byte);
    }
    Ok(bytes)
}

pub fn convert_to_parameters(items: Vec<Expression>) -> std::io::Result<Vec<Parameter>> {
    let mut my_params = vec![];
    for item in items {
        match item.clone() {
            // (a: i64, b: i64)
            Parameters(new_params) => my_params.extend(new_params),
            // (x = 5, y)
            SetVariables(var_names, default_value) => {
                match (var_names.deref(), default_value.deref()) {
                    (Parameters(params), Literal(value)) => {
                        my_params.extend(params.iter()
                            .map(|param| param.with_default(value.clone()))
                            .collect::<Vec<_>>());
                    }
                    (Variable(name), Literal(value)) => {
                        my_params.push(Parameter::new_with_default(name, value.get_type(), value.clone()))
                    }
                    _ => return throw(SyntaxError(IllegalExpression(item.to_code())))
                }
            }
            // x
            Variable(name) => my_params.push(Parameter::new(name, UnresolvedType)),
            other => return throw(SyntaxError(IllegalExpression(other.to_code())))
        }
    }
    Ok(my_params)
}

fn get_exponent_symbols() -> Vec<&'static str> {
    vec!["⁰", "¹", "²", "³", "⁴", "⁵", "⁶", "⁷", "⁸", "⁹"]
}

fn get_precedence(op: &Token, is_even: bool) -> i8 {
    match op.get().as_str() {
        s if get_exponent_symbols().contains(&s) => 12,
        "**" => 11,             // Exponentiation
        "*" | "/" => 10,        // Multiplication / Division
        "+" | "-" => 9,         // Addition / Subtraction
        "<<" | ">>" => 8,       // Bitwise Shift Left/Right
        "&" if is_even => 0,
        "&" | "&&" => 7,        // Bitwise/Logical AND
        "^" | "|" | "||" => 6,  // Bitwise XOR, Bitwise/Logical OR
        ".." | "..=" => 5,
        "::" | ":::" => 4,
        ":" => 3,
        "==" | "!=" | "<" | "<=" | ">" | ">=" => 2,
        "contains" | "in" | "is" | "isnt" | "like" | "matches" => 2,
        "limit" | "order_by" | "when" | "where" => 1,
        "->" | "=>" | "~>" | "<~" | "~>>" | "<<~" | "|>" | "|>>" => 1,
        "=" | ":=" | "+=" | "-=" | "*=" | "/=" | "%=" | "&=" | "|=" | "^=" => 0,
        "?" | "!?" => 0,
        _ => -1, // Unknown or lowest
    }
}

/// Indicates whether the token is an operator.
fn is_operator(token: &Token, is_even: bool) -> bool {
    match token {
        _ if is_unary_operator(token, is_even) => true,
        Atom { .. } if is_mnemonic_operator(token, is_even) => true,
        Operator { text, .. } => !matches!(text.as_str(), "{" | "}" | "(" | ")" | "[" | "]"),
        tok => get_exponent_symbols().contains(&tok.get().as_str())
    }
}

/// Indicates whether the token is a mnemonic operator.
fn is_mnemonic_operator(token: &Token, _is_even: bool) -> bool {
    matches!(token.get().as_str(), 
        "contains" | "in" | "is" | "isnt" | "like" |
        /* "from" |*/ "group_by" | "having" | "limit" | "matches" | "order_by" | "when" | "where"
    )
}

/// Indicates whether the token is a unary operator.
fn is_unary_operator(token: &Token, is_even: bool) -> bool {
    (token.is("-") && is_even) 
        | (token.is("+") && is_even)
        | (token.is("&") && is_even)
        | token.is("!")
        | get_exponent_symbols().contains(&token.get().as_str())
}

fn must_be_condition(expression: &Expression) -> std::io::Result<Conditions> {
    match expression {
        Condition(cond) => Ok(cond.to_owned()),
        z => throw(Exact(format!("Boolean expression expected near {}", z)))
    }
}

pub fn resolve_name_and_parameters_and_return_type(
    expr: &Expression
) -> std::io::Result<(Option<String>, Vec<Parameter>, Option<DataType>)> {
    match expr.to_owned() {
        // product(a: i64, b: i64)
        FunctionCall { fx, args } => {
            let fx_name = pull_variable_name(fx.deref())?;
            convert_to_parameters(args).map(|params| (Some(fx_name), params, None))
        }
        Literal(Kind(FunctionType(params, returns))) =>
            Ok((None, params, Some(returns.deref().clone()))),
        // (a: i64, b: i64)
        Parameters(params) => Ok((None, params, None)),
        // (a, b)
        TupleExpression(items) =>
            convert_to_parameters(items).map(|params| (None, params, None)),
        // x -> x + 1
        Variable(name) =>
            Ok((None, vec![Parameter::new(name, UnresolvedType)], None)),
        other => {
            println!("resolve_name_and_parameters_and_return_type: {:?}", other);
            throw(SyntaxError(IllegalExpression(other.to_code())))
        }
    }
}

/// Unit tests
#[cfg(test)]
mod tests {
    use crate::compiler::Compiler;
    use crate::expression::Expression;
    use crate::numbers::Numbers::I64Value;
    use crate::typed_values::TypedValue::Number;

    #[cfg(test)]
    mod unit_tests {
        use crate::compiler::Compiler;
        use crate::data_types::DataType::{DateTimeType, NumberType, StringType};
        use crate::number_kind::NumberKind::F64Kind;
        use crate::numbers::Numbers::F64Value;
        use crate::parameter::Parameter;
        use crate::token_slice::TokenSlice;
        use crate::typed_values::TypedValue::{DateValue, Number};

        #[test]
        fn test_expected_parameter() {
            let ts = TokenSlice::from_string("symbol: String");
            let compiler = Compiler::new();
            let (param, _) = compiler.expect_parameter(ts).unwrap();
            assert_eq!(
                param,
                Parameter::new("symbol", StringType),
            )
        }

        #[test]
        fn test_expected_parameter_with_default_literal() {
            let ts = TokenSlice::from_string("processed_time: Date = 2025-01-13T03:25:47.350Z");
            let compiler = Compiler::new();
            let (param, _) = compiler.expect_parameter(ts).unwrap();
            assert_eq!(
                param,
                Parameter::new_with_default("processed_time", DateTimeType, DateValue(1736738747350)),
            )
        }

        #[test]
        fn test_expected_parameter_with_default_expression() {
            let ts = TokenSlice::from_string("last_sale: f64 = 2.0 + 0.98");
            let compiler = Compiler::new();
            let (param, _) = compiler.expect_parameter(ts).unwrap();
            assert_eq!(
                param,
                Parameter::new_with_default("last_sale", NumberType(F64Kind), Number(F64Value(2.98))),
            )
        }
    }

    /// unit tests
    #[cfg(test)]
    mod integration_tests {
        use super::*;
        use crate::compiler::{get_exponent_symbols, Compiler};
        use crate::data_types::DataType::{FixedSizeType, FunctionType, NumberType, StringType, StructureType, UnresolvedType};
        use crate::expression::Conditions::*;
        use crate::expression::Expression::*;
        use crate::expression::Ranges::{Exclusive, Inclusive};
        use crate::expression::{HttpMethodCalls, UseOps, FALSE, TRUE};
        use crate::machine::Machine;
        use crate::number_kind::NumberKind::{F64Kind, I64Kind};
        use crate::numbers::Numbers::{F64Value, I64Value};
        use crate::parameter::Parameter;
        use crate::token_slice::TokenSlice;
        use crate::typed_values::TypedValue::{Kind, Number, StringValue};
        use crate::utils::strip_margin;

        #[test]
        fn test_as_value_quoted() {
            verify_build_with_decompile(
                r#""name": "Bill Bass""#,
                r#"name: "Bill Bass""#,
                NamedValue(
                    "name".into(),
                    Literal(StringValue("Bill Bass".into())).into(),
                ),
            );
        }

        #[test]
        fn test_as_value_unquoted() {
            verify_build(
                r#"symbol: "ABC""#,
                NamedValue("symbol".into(), Literal(StringValue("ABC".into())).into()),
            );
        }

        #[test]
        fn test_array_expression() {
            verify_build(
                "[8, 5, 7]",
                ArrayExpression(vec![
                    Literal(Number(I64Value(8))),
                    Literal(Number(I64Value(5))),
                    Literal(Number(I64Value(7))),
                ]),
            )
        }

        #[test]
        fn test_bitwise_and() {
            verify_build(
                "20 & 3",
                BitwiseAnd(
                    Literal(Number(I64Value(20))).into(),
                    Literal(Number(I64Value(3))).into(),
                ),
            );
        }

        #[test]
        fn test_bitwise_or() {
            verify_build(
                "20 | 3",
                BitwiseOr(
                    Literal(Number(I64Value(20))).into(),
                    Literal(Number(I64Value(3))).into(),
                ),
            );
        }

        #[test]
        fn test_bitwise_shl() {
            verify_build(
                "20 << 3",
                BitwiseShiftLeft(
                    Literal(Number(I64Value(20))).into(),
                    Literal(Number(I64Value(3))).into(),
                ),
            );
        }

        #[test]
        fn test_bitwise_shr() {
            verify_build(
                "20 >> 3",
                BitwiseShiftRight(
                    Literal(Number(I64Value(20))).into(),
                    Literal(Number(I64Value(3))).into(),
                ),
            );
        }

        #[test]
        fn test_bitwise_xor() {
            verify_build(
                "19 ^ 13",
                BitwiseXor(
                    Literal(Number(I64Value(19))).into(),
                    Literal(Number(I64Value(13))).into(),
                ),
            );
        }

        #[test]
        fn test_build_save_load() {
            // compile and save as a binary
            let src_path = "test.oxc";
            Compiler::build_and_save(src_path, "8 + 7").unwrap();

            // load the binary
            let expr = Compiler::load(src_path).unwrap();
            let (_, result) = Machine::new_platform().evaluate(&expr).unwrap();
            assert_eq!(result, Number(I64Value(15)))
        }

        #[test]
        fn test_code_block() {
            verify_build_with_decompile(
                "{ x = 19 ^ 13 x }",
                "{\nx = 19 ^ 13\nx\n}",
                CodeBlock(vec![
                    SetVariables(
                        Variable("x".into()).into(),
                        BitwiseXor(
                            Literal(Number(I64Value(19))).into(),
                            Literal(Number(I64Value(13))).into(),
                        )
                        .into(),
                    ),
                    Variable("x".into()),
                ]),
            );
        }

        #[test]
        fn test_colon_colon_function() {
            assert_eq!(
                Compiler::build("cal::now()").unwrap(),
                ColonColon(
                    Variable("cal".into()).into(),
                    FunctionCall {
                        fx: Variable("now".into()).into(),
                        args: vec![]
                    }
                    .into()
                )
            )
        }

        #[test]
        fn test_colon_colon_variable() {
            verify_build(
                "time::seconds",
                ColonColon(
                    Variable("time".into()).into(),
                    Variable("seconds".into()).into(),
                ),
            )
        }

        #[test]
        fn test_colon_colon_sequence() {
            let code = Compiler::build("oxide::tools::compact").unwrap();
            assert_eq!(
                code,
                ColonColon(
                    ColonColon(
                        Variable("oxide".into()).into(),
                        Variable("tools".into()).into(),
                        
                    ).into(),
                    Variable("compact".into()).into()
                )
            );
            assert_eq!(code.to_code(), "oxide::tools::compact");
        }

        #[test]
        fn test_colon_colon_colon() {
            assert_eq!(
                Compiler::build("clock:::seconds()").unwrap(),
                ColonColonColon(
                    Variable("clock".into()).into(),
                    FunctionCall {
                        fx: Variable("seconds".into()).into(),
                        args: vec![]
                    }
                    .into()
                )
            )
        }

        #[test]
        fn test_condition_equal() {
            verify_build(
                "n == 0",
                Condition(Equal(
                    Variable("n".into()).into(),
                    Literal(Number(I64Value(0))).into(),
                )),
            );
        }

        #[test]
        fn test_condition_greater_than() {
            verify_build(
                "n > 0",
                Condition(GreaterThan(
                    Variable("n".into()).into(),
                    Literal(Number(I64Value(0))).into(),
                )),
            );
        }

        #[test]
        fn test_condition_greater_than_or_equal() {
            verify_build(
                "n >= 0",
                Condition(GreaterOrEqual(
                    Variable("n".into()).into(),
                    Literal(Number(I64Value(0))).into(),
                )),
            );
        }

        #[test]
        fn test_condition_less_than() {
            verify_build(
                "n < 0",
                Condition(LessThan(
                    Variable("n".into()).into(),
                    Literal(Number(I64Value(0))).into(),
                )),
            );
        }

        #[test]
        fn test_condition_less_than_or_equal() {
            verify_build(
                "n <= 0",
                Condition(LessOrEqual(
                    Variable("n".into()).into(),
                    Literal(Number(I64Value(0))).into(),
                )),
            );
        }

        #[test]
        fn test_condition_not_equal() {
            verify_build(
                "a != 0",
                Condition(NotEqual(
                    Variable("a".into()).into(),
                    Literal(Number(I64Value(0))).into(),
                )),
            );
        }

        #[test]
        fn test_curvy_arrow_left() {
            let compiler = Compiler::new();
            let ts = TokenSlice::from_string(r#"n: 200 <~ "Rejected""#);
            let (model, _) = compiler.compile_next(ts).unwrap();
            assert_eq!(
                model,
                ArrowCurvyLeft(
                    NamedValue("n".into(), Literal(Number(I64Value(200))).into()).into(),
                    Literal(StringValue("Rejected".into())).into()
                )
            )
        }

        #[test]
        fn test_curvy_arrow_right() {
            let compiler = Compiler::new();
            let ts = TokenSlice::from_string(r#"n: 100 ~> "Accepted""#);
            let (model, _) = compiler.compile_next(ts).unwrap();
            assert_eq!(
                model,
                ArrowCurvyRight(
                    NamedValue("n".into(), Literal(Number(I64Value(100))).into()).into(),
                    Literal(StringValue("Accepted".into())).into()
                )
            )
        }

        #[test]
        fn test_do_while_loop() {
            let model = Compiler::build(r#"do x = x + 1 while (x < 5)"#)
                .unwrap();
            assert_eq!(
                model,
                DoWhile {
                    condition: Condition(LessThan(
                        Box::new(Variable("x".into())),
                        Box::new(Literal(Number(I64Value(5)))),
                    )).into(),
                    code: SetVariables(
                        Variable("x".into()).into(),
                        Plus(
                            Variable("x".into()).into(),
                            Literal(Number(I64Value(1))).into()
                        ).into(),
                    ).into(),
                });
        }

        #[test]
        fn test_element_at_array_literal() {
            verify_build(
                "[7, 5, 8, 2, 4, 1][3]",
                ElementAt(
                    ArrayExpression(vec![
                        Literal(Number(I64Value(7))),
                        Literal(Number(I64Value(5))),
                        Literal(Number(I64Value(8))),
                        Literal(Number(I64Value(2))),
                        Literal(Number(I64Value(4))),
                        Literal(Number(I64Value(1))),
                    ])
                    .into(),
                    Literal(Number(I64Value(3))).into(),
                ),
            );
        }

        #[test]
        fn test_element_at_variable() {
            assert_eq!(
                Compiler::build("stocks[1]").unwrap(),
                ElementAt(
                    Variable("stocks".into()).into(),
                    Literal(Number(I64Value(1))).into()
                )
            )
        }

        #[test]
        fn test_feature_with_a_scenario() {
            let code = Compiler::build(r#"
                Feature "Karate translator" {
                    Scenario "Translate Karate Scenario to Oxide Scenario" {
                        assert true, "Should be true"
                    }
                }
            "#).unwrap();
            assert_eq!(
                code,
                Feature {
                    title: Box::new(Literal(StringValue("Karate translator".to_string()))),
                    scenarios: vec![Scenario {
                        title: Box::new(Literal(StringValue(
                            "Translate Karate Scenario to Oxide Scenario".to_string()
                        ))),
                        verifications: vec![
                            Assert { 
                                condition: TRUE.into(), 
                                message: Some(Literal(StringValue("Should be true".into())).into()) 
                            }
                        ]
                    }],
                }
            )
        }

        #[test]
        fn test_function_anonymous_with_inferred_types() {
            let code = Compiler::build("(a, b) -> a * b").unwrap();
            assert_eq!(
                code,
                ArrowSkinnyRight(
                    TupleExpression(vec![
                        Variable("a".into()), Variable("b".into())
                    ]).into(),
                    Multiply(
                        Variable("a".into()).into(),
                        Variable("b".into()).into()
                    ).into()
                )
            )
        }

        #[test]
        fn test_function_anonymous_with_explicit_types() {
            let code = Compiler::build("(a: i64, b: i64) -> a * b").unwrap();
            assert_eq!(
                code,
                ArrowSkinnyRight(
                    Parameters(vec![
                        Parameter::new("a", NumberType(I64Kind)),
                        Parameter::new("b", NumberType(I64Kind)),
                    ]).into(),
                    Multiply(
                        Variable("a".into()).into(),
                        Variable("b".into()).into()
                    ).into()
                )
            )
        }

        #[test]
        fn test_function_named_with_explicit_types() {
            let code = Compiler::build(r#"
                fn add(a: i64, b: i64): i64 -> a + b
            "#).unwrap();
            assert_eq!(
                code,
                SetVariables(
                    Variable("add".into()).into(),
                    ArrowSkinnyRight(
                        Literal(Kind(FunctionType(
                            vec![
                                Parameter::new("a", NumberType(I64Kind)),
                                Parameter::new("b", NumberType(I64Kind)),
                            ],
                            NumberType(I64Kind).into()
                        ))).into(),
                        Plus(
                            Variable("a".into()).into(),
                            Variable("b".into()).into()
                        ).into()
                    ).into()
                ))
        }

        #[test]
        fn test_function_named_with_explicit_and_inferred_types() {
            let code = Compiler::build(r#"
               fn add(a: i64, b: i64) -> a + b
            "#).unwrap();
            assert_eq!(
                code,
                SetVariables(
                    Variable("add".into()).into(),
                    ArrowSkinnyRight(
                        Parameters(vec![
                            Parameter::new("a", NumberType(I64Kind)),
                            Parameter::new("b", NumberType(I64Kind))
                        ]).into(),
                        Plus(
                            Variable("a".into()).into(),
                            Variable("b".into()).into()
                        ).into()
                    ).into()
                ))
        }

        #[test]
        fn test_function_named_with_inferred_types() {
            let code = Compiler::build(r#"
                fn add(a, b) -> a + b
            "#).unwrap();
            assert_eq!(
                code,
                SetVariables(
                    Variable("add".into()).into(),
                    ArrowSkinnyRight(
                        TupleExpression(vec![
                            Variable("a".into()),
                            Variable("b".into()),
                        ]).into(),
                        Plus(
                            Box::new(Variable("a".into())),
                            Box::new(Variable("b".into()))
                        ).into(),
                    ).into()
                )
            )
        }

        #[test]
        fn test_function_named_with_explicit_input_and_return_types() {
            let code = Compiler::build(r#"
                fn make_quote(symbol: String(8), exchange: String(8), last_sale: f64):
                    Struct(symbol: String(8), exchange: String(8), last_sale: f64, uid: i64) ->
                    {
                        symbol: symbol,
                        market: exchange,
                        last_sale: last_sale * 2.0,
                        uid: __row_id__
                    }
            "#).unwrap();
            assert_eq!(
                code,
                SetVariables(
                    Variable("make_quote".into()).into(),
                    ArrowSkinnyRight(
                        Literal(Kind(FunctionType(
                            vec![
                                Parameter::new("symbol", FixedSizeType(StringType.into(), 8)),
                                Parameter::new("exchange", FixedSizeType(StringType.into(), 8)),
                                Parameter::new("last_sale", NumberType(F64Kind)),
                            ],
                            StructureType(vec![
                                Parameter::new("symbol", FixedSizeType(StringType.into(), 8)),
                                Parameter::new("exchange", FixedSizeType(StringType.into(), 8)),
                                Parameter::new("last_sale", NumberType(F64Kind)),
                                Parameter::new("uid", NumberType(I64Kind)),
                            ]).into()
                        ))).into(),
                        StructureExpression(vec![
                            ("symbol".to_string(), Variable("symbol".into())),
                            ("market".to_string(), Variable("exchange".into())),
                            ("last_sale".to_string(), Multiply(
                                Variable("last_sale".into()).into(),
                                Literal(Number(F64Value(2.0))).into()
                            )),
                            ("uid".to_string(), Variable("__row_id__".into()))
                        ]).into()
                    ).into())
            )
        }

        #[test]
        fn test_for_each_item_in_array() {
            verify_build(
                "for item in [1, 5, 6, 11, 17] println(item)",
                For {
                    construct: Condition(In(
                        Variable("item".into()).into(),
                        ArrayExpression(vec![
                            Literal(Number(I64Value(1))),
                            Literal(Number(I64Value(5))),
                            Literal(Number(I64Value(6))),
                            Literal(Number(I64Value(11))),
                            Literal(Number(I64Value(17))),
                        ]).into()
                    )).into(),
                    op: FunctionCall {
                        fx: Variable("println".into()).into(),
                        args: vec![Variable("item".into())],
                    }
                    .into(),
                },
            );
        }

        #[test]
        fn test_for_iteration() {
            verify_build_with_decompile(
                "for(i = 0, i < 5, i = i + 1) println(i)",
                "for (i = 0, i < 5, i = i + 1) println(i)",
                For {
                    construct:
                    TupleExpression(vec![
                        // i = 0
                        SetVariables(
                            Variable("i".into()).into(),
                            Literal(Number(I64Value(0))).into()
                        ).into(),
                        // i < 5
                        Condition(LessThan(
                            Variable("i".into()).into(),
                            Literal(Number(I64Value(5))).into()
                        )).into(),
                        // i = i + 1
                        SetVariables(
                            Variable("i".into()).into(),
                            Plus(
                                Variable("i".into()).into(),
                                Literal(Number(I64Value(1))).into()
                            ).into()
                        ).into(),
                    ]).into(),
                    op: FunctionCall {
                        fx: Variable("println".into()).into(),
                        args: vec![Variable("i".into())],
                    }
                        .into(),
                },
            );
        }

        #[test]
        fn test_function_call() {
            verify_build(
                "plot(2, 3)",
                FunctionCall {
                    fx: Box::new(Variable("plot".into())),
                    args: vec![Literal(Number(I64Value(2))), Literal(Number(I64Value(3)))],
                },
            )
        }

        #[test]
        fn test_function_pipeline() {
            let compiler = Compiler::new();
            let ts = TokenSlice::from_string(
                r#"
                "Hello" |> util::md5 |> util::hex
            "#);
            let (model, _) = compiler.compile_next(ts).unwrap();
            assert_eq!(
                model,
                ArrowVerticalBar(
                    ArrowVerticalBar(
                        Literal(StringValue("Hello".into())).into(),
                        ColonColon(
                            Variable("util".into()).into(),
                            Variable("md5".into()).into(),
                        )
                        .into(),
                    )
                    .into(),
                    ColonColon(
                        Variable("util".into()).into(),
                        Variable("hex".into()).into(),
                    )
                    .into(),
                ),
            );
        }

        #[test]
        fn test_http_delete() {
            let model = Compiler::build(
                r#"
                DELETE "http://localhost:9000/comments?id=675af"
            "#,
            )
            .unwrap();
            assert_eq!(
                model,
                HTTP(HttpMethodCalls::DELETE(
                    Literal(StringValue(
                        "http://localhost:9000/comments?id=675af".into()
                    ))
                    .into()
                ))
            );
        }

        #[test]
        fn test_http_get() {
            let model = Compiler::build(
                r#"
                GET "http://localhost:9000/comments?id=675af"
            "#,
            )
            .unwrap();
            assert_eq!(
                model,
                HTTP(HttpMethodCalls::GET(
                    Literal(StringValue(
                        "http://localhost:9000/comments?id=675af".into()
                    ))
                    .into()
                ))
            );
        }

        #[test]
        fn test_http_head() {
            let model = Compiler::build(
                r#"
                HEAD "http://localhost:9000/quotes/AMD/NYSE"
            "#,
            )
            .unwrap();
            assert_eq!(
                model,
                HTTP(HttpMethodCalls::HEAD(
                    Literal(StringValue("http://localhost:9000/quotes/AMD/NYSE".into())).into()
                ))
            );
        }

        #[test]
        fn test_http_patch() {
            let model = Compiler::build(
                r#"
                PATCH "http://localhost:9000/quotes/AMD/NASDAQ?exchange=NYSE"
            "#,
            )
            .unwrap();
            assert_eq!(
                model,
                HTTP(HttpMethodCalls::PATCH(
                    Literal(StringValue(
                        "http://localhost:9000/quotes/AMD/NASDAQ?exchange=NYSE".into()
                    ))
                    .into()
                ))
            );
        }

        #[test]
        fn test_http_post() {
            let model = Compiler::build(r#"
                POST "http://localhost:9000/quotes/AMD/NASDAQ"
            "#).unwrap();
            assert_eq!(
                model,
                HTTP(HttpMethodCalls::POST(
                    Literal(StringValue(
                        "http://localhost:9000/quotes/AMD/NASDAQ".into()
                    ))
                    .into()
                ))
            );
        }

        #[test]
        fn test_http_post_with_body() {
            let model = Compiler::build(
                r#"
                POST {
                    url: "http://localhost:8080/machine/www/stocks",
                    body: "Hello World"
                }
            "#).unwrap();
            assert_eq!(
                model,
                HTTP(HttpMethodCalls::POST(
                    StructureExpression(vec![
                        (
                            "url".to_string(),
                            Literal(StringValue(
                                "http://localhost:8080/machine/www/stocks".to_string()
                            ))
                        ),
                        (
                            "body".to_string(),
                            Literal(StringValue("Hello World".to_string()))
                        ),
                    ])
                    .into()
                ))
            );
        }

        #[test]
        fn test_http_post_with_multipart() {
            let model = Compiler::build(r#"
                POST {
                    url: "http://localhost:8080/machine/www/stocks",
                    body: "./demoes/language/include_file.ox"
                }
            "#).unwrap();
            assert_eq!(
                model,
                HTTP(HttpMethodCalls::POST(
                    StructureExpression(vec![
                        (
                            "url".to_string(),
                            Literal(StringValue(
                                "http://localhost:8080/machine/www/stocks".to_string()
                            ))
                        ),
                        (
                            "body".to_string(),
                            Literal(StringValue("./demoes/language/include_file.ox".to_string()))
                        ),
                    ])
                    .into()
                ))
            );
        }

        #[test]
        fn test_http_put() {
            let model = Compiler::build(r#"
                PUT "http://localhost:9000/quotes/AMD/NASDAQ"
            "#).unwrap();
            assert_eq!(
                model,
                HTTP(HttpMethodCalls::PUT(
                    Literal(StringValue(
                        "http://localhost:9000/quotes/AMD/NASDAQ".to_string()
                    ))
                    .into()
                ))
            );
        }

        #[test]
        fn test_if_statement() {
            let model = Compiler::build(r#"
                if n > 100 "Yes"
            "#).unwrap();
            assert_eq!(
                model,
                If {
                    condition: Box::new(Condition(GreaterThan(
                        Box::new(Variable("n".to_string())),
                        Box::new(Literal(Number(I64Value(100)))),
                    ))),
                    a: Box::new(Literal(StringValue("Yes".to_string()))),
                    b: None,
                }
            );
        }

        #[test]
        fn test_if_else_statement() {
            let code = Compiler::build(r#"
                if n > 100 n else m
            "#).unwrap();
            assert_eq!(
                code,
                If {
                    condition: Box::new(Condition(GreaterThan(
                        Box::new(Variable("n".to_string())),
                        Box::new(Literal(Number(I64Value(100)))),
                    ))),
                    a: Box::new(Variable("n".to_string())),
                    b: Some(Box::new(Variable("m".to_string()))),
                }
            );
        }

        #[test]
        fn test_iff_expression() {
            let model = Compiler::build(r#"
                if(n > 5, a, b)
            "#).unwrap();
            assert_eq!(
                model,
                If {
                    condition: Box::new(Condition(GreaterThan(
                        Box::new(Variable("n".to_string())),
                        Box::new(Literal(Number(I64Value(5)))),
                    ))),
                    a: Box::new(Variable("a".to_string())),
                    b: Some(Box::new(Variable("b".to_string()))),
                }
            );
        }

        #[test]
        fn test_in_range_exclusive() {
            verify_build(
                "n in a..z",
                Condition(In(
                    Variable("n".into()).into(),
                    Range(Exclusive(
                        Variable("a".into()).into(),
                        Variable("z".into()).into(),
                    ))
                        .into(),
                )),
            )
        }

        #[test]
        fn test_in_range_inclusive() {
            verify_build(
                "n in a..=z",
                Condition(In(
                    Variable("n".into()).into(),
                    Range(Inclusive(
                        Variable("a".into()).into(),
                        Variable("z".into()).into(),
                    ))
                        .into(),
                )),
            )
        }

        #[test]
        fn test_let_statement() {
            assert_eq!(
                Compiler::build("let x = 5").unwrap(),
                SetVariables(Variable("x".into()).into(), Literal(Number(I64Value(5))).into())
            );
        }

        #[test]
        fn test_like_statement() {
            assert_eq!(
                Compiler::build("'Hello' like 'H.ll.'").unwrap(),
                Condition(Like(
                    Literal(StringValue("Hello".into())).into(),
                    Literal(StringValue("H.ll.".into())).into(),
                ))
            );
        }

        #[ignore]
        #[test]
        fn test_match_statement() {
            let model = Compiler::build(r#"
                match code {
                   100 => "Accepted"
                   n => "Rejected"
                }
            "#).unwrap();
            assert_eq!(
                model,
                MatchExpression(
                    Variable("code".into()).into(),
                    vec![
                        // 100 => "Accepted"
                        ArrowFat(
                            Condition(When(
                                Variable("n".into()).into(), 
                                Condition(Equal(
                                    Variable("n".into()).into(),
                                    Literal(Number(I64Value(100))).into()
                                )).into()
                            )).into(),
                            Literal(StringValue("Accepted".into())).into()
                        ),
                        // n => "Rejected"
                        ArrowFat(
                            Variable("n".into()).into(),
                            Literal(StringValue("Rejected".into())).into()
                        ).into()
                    ]
                )
            )
        }

        #[test]
        fn test_mathematical_addition() {
            let model = Compiler::build("n + 3").unwrap();
            assert_eq!(
                model,
                Plus(
                    Box::new(Variable("n".into())),
                    Box::new(Literal(Number(I64Value(3)))),
                )
            );
        }

        #[test]
        fn test_mathematical_division() {
            let model = Compiler::build("n / 3").unwrap();
            assert_eq!(
                model,
                Divide(
                    Box::new(Variable("n".into())),
                    Box::new(Literal(Number(I64Value(3)))),
                )
            );
        }

        #[test]
        fn test_mathematical_exponent() {
            let model = Compiler::build("5 ** 2").unwrap();
            assert_eq!(
                model,
                Pow(
                    Box::new(Literal(Number(I64Value(5)))),
                    Box::new(Literal(Number(I64Value(2)))),
                )
            );
        }

        #[test]
        fn test_mathematical_exponent_via_symbol() {
            let mut num = 0;
            for symbol in get_exponent_symbols() {
                let model = Compiler::build(format!("5{}", symbol).as_str()).unwrap();
                assert_eq!(
                    model,
                    Pow(
                        Box::new(Literal(Number(I64Value(5)))),
                        Box::new(Literal(Number(I64Value(num)))),
                    )
                );
                num += 1
            }
        }

        #[test]
        fn test_mathematical_modulus() {
            let model = Compiler::build("n % 4").unwrap();
            assert_eq!(
                model,
                Modulo(
                    Box::new(Variable("n".into())),
                    Box::new(Literal(Number(I64Value(4))))
                )
            );
        }

        #[test]
        fn test_mathematical_multiplication() {
            let model = Compiler::build("n * 10").unwrap();
            assert_eq!(
                model,
                Multiply(
                    Box::new(Variable("n".into())),
                    Box::new(Literal(Number(I64Value(10)))),
                )
            );
        }

        #[test]
        fn test_mathematical_subtraction() {
            verify_build(
                "_ - 7",
                Minus(
                    Variable("_".into()).into(),
                    Literal(Number(I64Value(7))).into(),
                ),
            );
        }

        #[test]
        fn test_negative_array() {
            verify_build_with_decompile(
                "-[w, x, y, z]",
                "-([w, x, y, z])",
                Neg(ArrayExpression(vec![
                    Variable("w".into()).into(),
                    Variable("x".into()).into(),
                    Variable("y".into()).into(),
                    Variable("z".into()).into(),
                ])
                .into()),
            );
        }

        #[test]
        fn test_negative_number() {
            verify_build_with_decompile("-5", "-(5)", Neg(Literal(Number(I64Value(5))).into()));
        }

        #[test]
        fn test_negative_tuple() {
            verify_build_with_decompile(
                "-(x, y, z)",
                "-((x, y, z))",
                Neg(TupleExpression(vec![
                    Variable("x".into()).into(),
                    Variable("y".into()).into(),
                    Variable("z".into()).into(),
                ])
                .into()),
            );
        }

        #[test]
        fn test_negative_variable() {
            verify_build_with_decompile("-x", "-(x)", Neg(Variable("x".into()).into()));
        }

        #[test]
        fn test_not_false() {
            verify_build_with_decompile("!false", "!(false)", Condition(Not(FALSE.into())));
        }

        #[test]
        fn test_not_true() {
            verify_build_with_decompile("!true", "!(true)", Condition(Not(TRUE.into())));
        }

        #[test]
        fn test_not_variable() {
            verify_build_with_decompile("!x", "!(x)", Condition(Not(Variable("x".into()).into())));
        }

        #[test]
        fn test_numeric_literal_value_float() {
            verify_build_with_decompile(
                "1_234_567.890",
                "1234567.89",
                Literal(Number(F64Value(1_234_567.890))),
            );
        }

        #[test]
        fn test_numeric_literal_value_integer() {
            verify_build_with_decompile(
                "1_234_567_890",
                "1234567890",
                Literal(Number(I64Value(1_234_567_890))),
            );
        }

        #[test]
        fn test_parameters() {
            let expr = Compiler::build(r#"
                (a, b, c: i64)
            "#).unwrap();
            assert_eq!(
                expr,
                Parameters(vec![
                    Parameter::new("a", UnresolvedType),
                    Parameter::new("b", UnresolvedType),
                    Parameter::new("c", NumberType(I64Kind)),
                ])
            )
        }

        #[test]
        fn test_parameters_all_qualified() {
            let expr = Compiler::build(
                r#"
                (a: i64, b: i64, c: i64)
            "#,
            )
            .unwrap();
            assert_eq!(
                expr,
                Parameters(vec![
                    Parameter::new("a", NumberType(I64Kind)),
                    Parameter::new("b", NumberType(I64Kind)),
                    Parameter::new("c", NumberType(I64Kind)),
                ])
            )
        }

        #[test]
        fn test_positive_number() {
            verify_build_with_decompile("+5", "5", Literal(Number(I64Value(5))));
        }

        #[test]
        fn test_positive_variable() {
            verify_build_with_decompile("+x", "x", Variable("x".into()).into());
        }

        #[test]
        fn test_quantity() {
            verify_build_with_decompile(
                "(n + 5)",
                "n + 5",
                Plus(
                    Variable("n".into()).into(),
                    Literal(Number(I64Value(5))).into(),
                ),
            )
        }

        #[test]
        fn test_range_exclusive() {
            verify_build(
                "n in 1..100",
                Condition(In(
                    Variable("n".into()).into(),
                    Range(
                        Exclusive(
                            Literal(Number(I64Value(1))).into(),
                            Literal(Number(I64Value(100))).into(),
                        )
                        .into(),
                    ).into()
                )),
            )
        }

        #[test]
        fn test_range_inclusive() {
            verify_build(
                "n in 1..=100",
                Condition(In(
                    Variable("n".into()).into(),
                    Range(
                        Inclusive(
                            Literal(Number(I64Value(1))).into(),
                            Literal(Number(I64Value(100))).into(),
                        )
                            .into(),
                    ).into()
                )),
            )
        }

        #[test]
        fn test_embedded_table_reference() {
            verify_build(
                "&stocks(1, 0)",
                Referenced(
                    FunctionCall {
                        fx: Variable("stocks".into()).into(),
                        args: vec![
                            Literal(Number(I64Value(1))).into(),
                            Literal(Number(I64Value(0))).into(),
                        ],
                    }.into()
                ))
        }

        #[test]
        fn test_semi_colon_expression() {
            verify_build_with_decompile(
                "x = 6; x",
                "{\nx = 6\nx\n}",
                CodeBlock(vec![
                    SetVariables(
                        Variable("x".into()).into(), 
                        Literal(Number(I64Value(6))).into()
                    ), 
                    Variable("x".into()).into()
                ])
            )
        }

        #[test]
        fn test_set_variables_tuple_destructure() {
            verify_build(
                "(a, b, c) = (3, 5, 7)",
                SetVariables(
                    TupleExpression(vec![
                        Variable("a".into()),
                        Variable("b".into()),
                        Variable("c".into()),
                    ])
                    .into(),
                    TupleExpression(vec![
                        Literal(Number(I64Value(3))),
                        Literal(Number(I64Value(5))),
                        Literal(Number(I64Value(7))),
                    ])
                    .into(),
                ),
            )
        }

        #[test]
        fn test_simple_soft_structure() {
            let code = Compiler::build(r#"
                {w:'abc', x:1.0, y:2, z:[1, 2, 3]}
            "#).unwrap();
            assert_eq!(
                code,
                StructureExpression(vec![
                    ("w".to_string(), Literal(StringValue("abc".into()))),
                    ("x".to_string(), Literal(Number(F64Value(1.)))),
                    ("y".to_string(), Literal(Number(I64Value(2)))),
                    (
                        "z".to_string(),
                        ArrayExpression(vec![
                            Literal(Number(I64Value(1))),
                            Literal(Number(I64Value(2))),
                            Literal(Number(I64Value(3))),
                        ])
                    ),
                ])
            );
        }
        

        #[test]
        fn test_structure_expression() {
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
        fn test_structure_expression_complex() {
            verify_build_with_decompile(
                strip_margin(
                    r#"
                          |{
                          |    symbol: symbol,
                          |    exchange: exchange,
                          |    last_sale: last_sale * 2.0,
                          |    event_time: cal::now()
                          |}"#, '|').trim(),
                "{symbol: symbol, exchange: exchange, last_sale: last_sale * 2, event_time: cal::now()}",
                StructureExpression(vec![
                    ("symbol".into(), Variable("symbol".into())),
                    ("exchange".into(), Variable("exchange".into())),
                    ("last_sale".into(), Multiply(
                        Variable("last_sale".into()).into(),
                        Literal(Number(F64Value(2.0))).into()
                    )),
                    ("event_time".into(), ColonColon(
                        Variable("cal".into()).into(),
                        FunctionCall {
                            fx: Variable("now".into()).into(),
                            args: vec![]
                        }.into()))
                ]))
        }

        #[test]
        fn test_tuple_literals() {
            verify_build(
                r#"("a", "b", "c")"#,
                TupleExpression(vec![
                    Literal(StringValue("a".into())),
                    Literal(StringValue("b".into())),
                    Literal(StringValue("c".into())),
                ]),
            )
        }

        #[test]
        fn test_tuple_mixed() {
            verify_build(
                r#"(abc, 123, "Hello")"#,
                TupleExpression(vec![
                    Variable("abc".into()),
                    Literal(Number(I64Value(123))),
                    Literal(StringValue("Hello".into())),
                ]))
        }

        #[test]
        fn test_tuple_variables() {
            verify_build(
                r#"(a, b, c)"#,
                TupleExpression(vec![
                    Variable("a".into()),
                    Variable("b".into()),
                    Variable("c".into()),
                ])
            )
        }

        #[test]
        fn test_typedef() {
            let model = Compiler::build(
                r#"
                typedef(String(32))
            "#,
            )
            .unwrap();
            assert_eq!(
                model,
                TypeDef(Literal(Kind(FixedSizeType(StringType.into(), 32))).into())
            )
        }

        #[test]
        fn test_type_of() {
            let model = Compiler::build(
                r#"
                type_of("cat")
            "#,
            )
            .unwrap();
            assert_eq!(
                model,
                TypeOf(Literal(StringValue("cat".into())).into())
            );
        }

        #[test]
        fn test_use_single() {
            // single use
            let code = Compiler::build(
                r#"
                use "os"
            "#,
            )
                .unwrap();
            assert_eq!(code, Use(vec![UseOps::Everything("os".into())]));
            assert_eq!(code.to_code(), "use os");
        }

        #[test]
        fn test_use_multiple() {
            // multiple uses
            let code = Compiler::build(
                r#"
                use os, utils, "tools"
            "#,
            )
                .unwrap();
            assert_eq!(
                code,
                Use(vec![
                    UseOps::Everything("os".into()),
                    UseOps::Everything("utils".into()),
                    UseOps::Everything("tools".into()),
                ])
            );
            assert_eq!(code.to_code(), "use os, utils, tools");
        }

        #[test]
        fn test_variable_expression() {
            verify_build("abc", Variable("abc".into()))
        }

        #[test]
        fn test_whenever_statement() {
            let model = Compiler::build(r#"whenever x == 0 hit_boundary()"#)
                .unwrap();
            assert_eq!(
                model,
                WhenEver {
                    condition: Condition(Equal(
                        Variable("x".into()).into(),
                        Literal(Number(I64Value(0))).into()
                    )).into(),
                    code: FunctionCall {
                        fx: Variable("hit_boundary".into()).into(),
                        args: vec![]
                    }.into(),
                })
        }

        #[test]
        fn test_whenever_statement_2() {
            let model = Compiler::build(r#"whenever x == 0 { hit_boundary() }"#)
                .unwrap();
            assert_eq!(
                model,
                WhenEver {
                    condition: Condition(Equal(
                        Variable("x".into()).into(),
                        Literal(Number(I64Value(0))).into()
                    )).into(),
                    code: FunctionCall {
                        fx: Variable("hit_boundary".into()).into(),
                        args: vec![]
                    }.into(),
                })
        }

        #[test]
        fn test_while_loop() {
            let model = Compiler::build(r#"while (x < 5) x = x + 1"#)
                .unwrap();
            assert_eq!(
                model,
                While {
                    condition: Condition(LessThan(
                        Variable("x".into()).into(),
                        Literal(Number(I64Value(5))).into(),
                    )).into(),
                    code: SetVariables(
                        Variable("x".into()).into(),
                        Plus(
                            Variable("x".into()).into(),
                            Literal(Number(I64Value(1))).into()
                        ).into(),
                    ).into(),
                });
        }

        #[test]
        fn test_while_loop_fix() {
            let model = Compiler::build(r#"
                x = 0
                while x < 7 x = x + 1
                x
            "#).unwrap();
            assert_eq!(
                model,
                CodeBlock(vec![
                    SetVariables(
                        Variable("x".into()).into(),
                        Literal(Number(I64Value(0))).into()
                    ),
                    While {
                        condition: Box::new(Condition(LessThan(
                            Box::new(Variable("x".into())),
                            Box::new(Literal(Number(I64Value(7)))),
                        ))),
                        code: SetVariables(
                            Variable("x".into()).into(),
                            Plus(
                                Variable("x".into()).into(),
                                Literal(Number(I64Value(1))).into(),
                            )
                            .into()
                        )
                        .into()
                    },
                    Variable("x".into()),
                ])
            );
        }
    }

    /// SQL tests
    #[cfg(test)]
    mod precedence_tests {
        use crate::compiler::Compiler;
        use crate::expression::Expression::{ArrowVerticalBar, ColonColon, Divide, Literal, Minus, Multiply, Plus, Variable};
        use crate::machine::Machine;
        use crate::numbers::Numbers::{F64Value, I64Value};
        use crate::token_slice::TokenSlice;
        use crate::tokenizer;
        use crate::typed_values::TypedValue::{Number, StringValue};

        #[test]
        fn test_order_of_operations_1() {
            let model = Compiler::build("2 + (4 * 3)").unwrap();
            assert_eq!(
                model,
                Plus(
                    Box::new(Literal(Number(I64Value(2)))),
                    Box::new(Multiply(
                        Box::new(Literal(Number(I64Value(4)))),
                        Box::new(Literal(Number(I64Value(3)))),
                    )),
                )
            );
        }

        #[test]
        fn test_order_of_operations_2() {
            let model = Compiler::build("(4.0 / 3.0) + (4 * 3)").unwrap();
            assert_eq!(
                model,
                Plus(
                    Box::new(Divide(
                        Box::new(Literal(Number(F64Value(4.0)))),
                        Box::new(Literal(Number(F64Value(3.0)))),
                    )),
                    Box::new(Multiply(
                        Box::new(Literal(Number(I64Value(4)))),
                        Box::new(Literal(Number(I64Value(3)))),
                    )),
                )
            );
        }

        #[test]
        fn test_order_of_operations_3() {
            let model = Compiler::build("2 - 4 * 3").unwrap();
            assert_eq!(
                model,
                Minus(
                    Box::new(Literal(Number(I64Value(2)))),
                    Box::new(Multiply(
                        Box::new(Literal(Number(I64Value(4)))),
                        Box::new(Literal(Number(I64Value(3)))),
                    )),
                )
            );
        }

        #[test]
        fn test_function_pipelining() {
            let ts = TokenSlice::from_string("'Hello' |> str::reverse |> util::md5");
            let compiler = Compiler::new();
            let (model, _) = compiler.compile_next(ts).unwrap();
            assert_eq!(
                model,
                ArrowVerticalBar(
                    ArrowVerticalBar(
                        Literal(StringValue("Hello".into())).into(), 
                        ColonColon(Variable("str".into()).into(), Variable("reverse".into()).into()).into()
                    ).into(),
                    ColonColon(Variable("util".into()).into(), Variable("md5".into()).into()).into()
                )
            )
        }

        #[test]
        fn test_math_precedence() {
            // parse the expression into tokens
            let tokens = tokenizer::parse_fully("4 * (2 - 1) ** 2 + 3");
            // tokens ["4", "*", "(", "2", "-", "1", ")", "**", "2", "+", "3"]

            // set up the compiler
            let ts = TokenSlice::new(tokens);
            let compiler = Compiler::new();

            // build the model
            let (model, _) = compiler.compile_next(ts).unwrap();
            // model: 4 * (2 - 1) ** 2 + 3

            // compute the result
            let machine = Machine::new();
            let (_, result) = machine.evaluate(&model).unwrap();
            assert_eq!(result, Number(F64Value(7.0)))
            // result: 7.0
        }
    }

    /// SQL tests
    #[cfg(test)]
    mod sql_tests {
        use crate::compiler::Compiler;
        use crate::expression::Conditions::{Equal, GreaterOrEqual};
        use crate::expression::Expression::*;
        use crate::numbers::Numbers::{F64Value, I64Value};
        use crate::typed_values::TypedValue::{Number, StringValue};

        #[test]
        fn test_delete_from() {
            let model = Compiler::build(r#"
                delete stocks
            "#).unwrap();
            assert_eq!(
                model,
                Delete {
                    from: Box::new(Variable("stocks".into())),
                    condition: None,
                    limit: None,
                }
            )
        }

        #[test]
        fn test_delete_from_where_limit() {
            let model = Compiler::build(r#"
                delete stocks
                where last_sale >= 1.0
                limit 100
            "#).unwrap();
            assert_eq!(
                model,
                Delete {
                    from: Box::new(Variable("stocks".into())),
                    condition: Some(GreaterOrEqual(
                        Box::new(Variable("last_sale".into())),
                        Box::new(Literal(Number(F64Value(1.0)))),
                    )),
                    limit: Some(Box::new(Literal(Number(I64Value(100))))),
                }
            )
        }

        #[test]
        fn test_identifier_where_limit() {
            let model = Compiler::build(r#"
                stocks where last_sale >= 1.0 limit 20
            "#).unwrap();
            assert_eq!(
                model,
                Limit {
                    from: Box::new(Where {
                        from: Variable("stocks".into()).into(),
                        condition: GreaterOrEqual(
                            Box::new(Variable("last_sale".into())),
                            Box::new(Literal(Number(F64Value(1.0)))),
                        ),
                    }),
                    limit: Box::new(Literal(Number(I64Value(20)))),
                }
            );
        }

        #[test]
        fn test_write_where() {
            let model = Compiler::build(r#"
                {symbol: "BKP", exchange: "OTCBB", last_sale: 0.1421} 
                     ~> (stocks where symbol is "BKPQ")
            "#).unwrap();
            assert_eq!(
                model,
                ArrowCurvyRight(
                    StructureExpression(vec![
                        ("symbol".into(), Literal(StringValue("BKP".into()))),
                        ("exchange".into(), Literal(StringValue("OTCBB".into()))),
                        ("last_sale".into(), Literal(Number(F64Value(0.1421))))
                    ]).into(),
                    Where {
                        from: Variable("stocks".into()).into(),
                        condition: Equal(
                            Variable("symbol".into()).into(),
                            Literal(StringValue("BKPQ".into())).into()
                        )
                    }.into())
            );
        }

        #[test]
        fn test_select_from_variable() {
            let model = Compiler::build(r#"
                select symbol, exchange, last_sale from stocks
            "#).unwrap();
            assert_eq!(
                model,
                Select {
                    fields: vec![
                        Variable("symbol".into()),
                        Variable("exchange".into()),
                        Variable("last_sale".into())
                    ],
                    from: Some(Box::new(Variable("stocks".into()))),
                }
            )
        }

        #[test]
        fn test_undelete_from() {
            let model = Compiler::build(r#"
                undelete stocks
            "#).unwrap();
            assert_eq!(
                model,
                Undelete {
                    from: Box::new(Variable("stocks".into())),
                    condition: None,
                    limit: None,
                }
            )
        }

        #[test]
        fn test_undelete_from_where_limit() {
            let model = Compiler::build(r#"
                undelete stocks
                where last_sale >= 1.0
                limit 100
            "#).unwrap();
            assert_eq!(
                model,
                Undelete {
                    from: Box::new(Variable("stocks".into())),
                    condition: Some(GreaterOrEqual(
                        Box::new(Variable("last_sale".into())),
                        Box::new(Literal(Number(F64Value(1.0)))),
                    )),
                    limit: Some(Box::new(Literal(Number(I64Value(100))))),
                }
            )
        }

        #[test]
        fn test_write_json_into_table() {
            let model = Compiler::build(r#"
                [{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                 { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }]
                        ~> stocks
            "#).unwrap();
            assert_eq!(
                model,
                ArrowCurvyRight(
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
                    Variable("stocks".into()).into(),
                )
            );
        }
    }

    fn verify_build(code: &str, model: Expression) {
        assert_eq!(code, model.to_code());
        assert_eq!(Compiler::build(code).unwrap(), model);
    }

    fn verify_build_with_decompile(code: &str, decompiled: &str, model: Expression) {
        assert_eq!(Compiler::build(code).unwrap(), model);
        assert_eq!(decompiled, model.to_code())
    }
}
