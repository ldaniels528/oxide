////////////////////////////////////////////////////////////////////
// compiler module
////////////////////////////////////////////////////////////////////

use log::info;
use serde::{Deserialize, Serialize};

use shared_lib::{cnv_error, fail, FieldJs, RowJs};

use crate::expression::{Expression, FALSE, NULL, TRUE, UNDEFINED};
use crate::expression::CreationEntity::{IndexEntity, TableEntity};
use crate::expression::DropTarget::TableTarget;
use crate::expression::Expression::*;
use crate::serialization::{assemble, assemble_fully, disassemble_fully};
use crate::server::ColumnJs;
use crate::token_slice::TokenSlice;
use crate::tokens::Token;
use crate::tokens::Token::{Atom, Backticks, DoubleQuoted, Numeric, Operator, SingleQuoted};
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{Function, Int64Value, StringValue};

/// Represents the Oxide compiler state
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompilerState {
    stack: Vec<Expression>,
}

impl CompilerState {

    ////////////////////////////////////////////////////////////////
    // static methods
    ////////////////////////////////////////////////////////////////

    /// compiles the source code into a [Vec<Expression>]; a graph representing
    /// the program's executable code
    pub fn compile_script(source_code: &str) -> std::io::Result<Expression> {
        let mut compiler = CompilerState::new();
        let (code, _) = compiler.compile(TokenSlice::from_string(source_code))?;
        Ok(code)
    }

    /// compiles the source code into byte code
    pub fn compile_to_byte_code(source_code: &str) -> std::io::Result<Vec<u8>> {
        let code = CompilerState::compile_script(source_code)?;
        let byte_code = assemble(&code);
        Ok(byte_code)
    }

    /// decompiles the byte code into model expressions
    pub fn decompile_from_byte_code(byte_code: &Vec<u8>) -> std::io::Result<Expression> {
        Ok(disassemble_fully(&byte_code)?)
    }

    /// creates a new [CompilerState] instance
    pub fn new() -> Self {
        CompilerState {
            stack: vec![],
        }
    }

    ////////////////////////////////////////////////////////////////
    // instance methods
    ////////////////////////////////////////////////////////////////

    /// compiles the entire [TokenSlice] into an [Expression]
    pub fn compile(&mut self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        fn push(compiler: &mut CompilerState, ts: TokenSlice) -> std::io::Result<TokenSlice> {
            let start = ts.get_position();
            let (expr, ts) = compiler.compile_next(ts)?;
            // ensure we actually moved the cursor
            assert!(ts.get_position() > start);
            compiler.push(expr);
            Ok(ts)
        }

        // consume the entire iterator
        let mut ts = ts;
        while ts.has_more() { ts = push(self, ts)?; }
        let items = self.get_stack();
        let code = if items.len() == 1 { items[0].clone() } else { CodeBlock(items) };
        Ok((code, ts))
    }

    /// compiles the next [TokenSlice] into an [Expression]
    pub fn compile_next(&mut self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        let (expr, ts) = match ts.next() {
            (Some(Operator { text, precedence, .. }), ts) =>
                self.compile_operator(ts, text, precedence),
            (Some(Backticks { text, .. }), ts) =>
                Ok((Variable(text.into()), ts)),
            (Some(Atom { .. }), _) => self.compile_keyword(ts),
            (Some(DoubleQuoted { text, .. } |
                  SingleQuoted { text, .. }), ts) =>
                Ok((Literal(StringValue(text.into())), ts)),
            (Some(Numeric { text, .. }), ts) =>
                Ok((Literal(TypedValue::from_numeric(text.as_str())?), ts)),
            (None, ts) => fail_near("Unexpected end of input", &ts)
        }?;

        // handle postfix operator?
        match ts.next() {
            (Some(Operator { is_barrier, .. }), _) if !is_barrier => {
                self.push(expr);
                self.compile_next(ts)
            }
            _ => Ok((expr, ts))
        }
    }

    /// compiles the [TokenSlice] into a leading single-parameter [Expression] (e.g. !true)
    fn compile_expr_1a(&mut self,
                       ts: TokenSlice,
                       f: fn(Box<Expression>) -> Expression) -> std::io::Result<(Expression, TokenSlice)> {
        let (expr, ts) = self.compile_next(ts)?;
        Ok((f(Box::new(expr)), ts))
    }

    /// compiles the [TokenSlice] into a two-parameter [Expression]
    fn compile_expr_1b(&mut self, ts: TokenSlice,
                       f: fn(Box<Expression>) -> Expression) -> std::io::Result<(Expression, TokenSlice)> {
        match self.pop() {
            None => fail_near("expected an expression", &ts),
            Some(expr0) => Ok((f(Box::new(expr0)), ts))
        }
    }

    /// compiles the [TokenSlice] into a two-parameter [Expression]
    fn compile_expr_2a(&mut self,
                       ts: TokenSlice,
                       expr0: Expression,
                       f: fn(Box<Expression>, Box<Expression>) -> Expression) -> std::io::Result<(Expression, TokenSlice)> {
        let (expr1, ts) = self.compile_next(ts)?;
        Ok((f(Box::new(expr0), Box::new(expr1)), ts))
    }

    /// compiles the [TokenSlice] into a two-parameter [Expression]
    fn compile_expr_2b(&mut self, ts: TokenSlice,
                       expr1: Expression,
                       f: fn(Box<Expression>, Box<Expression>) -> Expression) -> std::io::Result<(Expression, TokenSlice)> {
        match self.pop() {
            None => fail_near("expected an expression", &ts),
            Some(expr0) => Ok((f(Box::new(expr0), Box::new(expr1)), ts))
        }
    }

    /// compiles the [TokenSlice] into a name-value parameter [Expression]
    fn compile_expr_2nv(&mut self,
                        ts: TokenSlice,
                        expr0: Expression,
                        f: fn(String, Box<Expression>) -> Expression) -> std::io::Result<(Expression, TokenSlice)> {
        if let Variable(name) = expr0 {
            let (expr1, ts) = self.compile_next(ts)?;
            Ok((f(name, Box::new(expr1)), ts))
        } else { fail_near("an identifier name was expected", &ts) }
    }

    /// compiles the [TokenSlice] into an operator-based [Expression]
    fn compile_operator(&mut self,
                        ts: TokenSlice,
                        symbol: String,
                        _precedence: usize) -> std::io::Result<(Expression, TokenSlice)> {
        fn pow(compiler: &mut CompilerState, ts: TokenSlice, n: i64) -> std::io::Result<(Expression, TokenSlice)> {
            compiler.compile_expr_2b(ts, Literal(Int64Value(n)), Pow)
        }
        match symbol.as_str() {
            ";" => Ok((self.pop().unwrap_or(NULL), ts.skip())),
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
            "!" => self.compile_expr_1a(ts, Not),
            "¡" => self.compile_expr_1b(ts, Factorial),
            "(" => self.expect_parentheses(ts),
            "[" => self.expect_square_brackets(ts),
            "{" => self.expect_curly_brackets(ts),
            sym =>
                if let Some(op0) = self.pop() {
                    match sym {
                        "&&" => self.compile_expr_2a(ts, op0, And),
                        "&" => self.compile_expr_2a(ts, op0, BitwiseAnd),
                        "|" => self.compile_expr_2a(ts, op0, BitwiseOr),
                        "÷" | "/" => self.compile_expr_2a(ts, op0, Divide),
                        "==" => self.compile_expr_2a(ts, op0, Equal),
                        ">" => self.compile_expr_2a(ts, op0, GreaterThan),
                        ">=" => self.compile_expr_2a(ts, op0, GreaterOrEqual),
                        "<" => self.compile_expr_2a(ts, op0, LessThan),
                        "<=" => self.compile_expr_2a(ts, op0, LessOrEqual),
                        "-" => self.compile_expr_2a(ts, op0, Minus),
                        "%" => self.compile_expr_2a(ts, op0, Modulo),
                        "×" | "*" => self.compile_expr_2a(ts, op0, Multiply),
                        "!=" => self.compile_expr_2a(ts, op0, NotEqual),
                        "||" => self.compile_expr_2a(ts, op0, Or),
                        "+" => self.compile_expr_2a(ts, op0, Plus),
                        "**" => self.compile_expr_2a(ts, op0, Pow),
                        ".." => self.compile_expr_2a(ts, op0, Range),
                        ":" => self.compile_expr_2nv(ts, op0, AsValue),
                        ":=" => self.compile_expr_2nv(ts, op0, SetVariable),
                        "<<" => self.compile_expr_2a(ts, op0, ShiftLeft),
                        ">>" => self.compile_expr_2a(ts, op0, ShiftRight),
                        "^" => self.compile_expr_2a(ts, op0, BitwiseXor),
                        unknown => fail(format!("Invalid operator '{}'", unknown))
                    }
                } else { fail(format!("Illegal start of expression '{}'", symbol)) }
        }
    }

    fn compile_keyword(&mut self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        if let (Some(Atom { text, .. }), ts) = ts.next() {
            match text.as_str() {
                "create" => self.compile_keyword_create(ts),
                "delete" => self.compile_keyword_delete(ts),
                "drop" => self.compile_keyword_drop(ts),
                "false" => Ok((FALSE, ts)),
                "fn" => self.compile_keyword_fn(ts),
                "from" => {
                    let (from, ts) = self.compile_keyword_from(ts)?;
                    self.compile_keyword_queryables(from, ts)
                }
                "into" => self.compile_keyword_into(ts),
                "lambda" => self.compile_keyword_lambda(ts),
                "limit" => fail_near("`from` is expected before `limit`: from stocks limit 5", &ts),
                "ns" => self.compile_keyword_ns(ts),
                "null" => Ok((NULL, ts)),
                "overwrite" => self.compile_keyword_overwrite(ts),
                "select" => self.compile_keyword_select(ts),
                "true" => Ok((TRUE, ts)),
                "undefined" => Ok((UNDEFINED, ts)),
                "update" => self.compile_keyword_update(ts),
                "via" => self.compile_keyword_via(ts),
                "where" => fail_near("`from` is expected before `where`: from stocks where last_sale < 1.0", &ts),
                name => self.expect_function_call_or_variable(name, ts)
            }
        } else { fail("Unexpected end of input") }
    }

    fn compile_keyword_create(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        if let (Some(t), ts) = ts.next() {
            match t.get_raw_value().as_str() {
                "index" => self.compile_keyword_create_index(ts),
                "table" => self.compile_keyword_create_table(ts),
                name => fail_near(format!("Syntax error: expect type identifier, got '{}'", name), &ts)
            }
        } else { fail("Unexpected end of input") }
    }

    fn compile_keyword_create_index(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let (index, ts) = self.compile_next(ts)?;
        if let (ArrayLiteral(columns), ts) = self.compile_next(ts.clone())? {
            Ok((Create(IndexEntity { path: Box::new(index), columns }), ts))
        } else {
            fail_near("Columns expected", &ts)
        }
    }

    fn compile_keyword_create_table(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        // create table ns("..") (name: String, ..)
        let (table, ts) = self.compile_next(ts)?;
        if let (ColumnSet(columns), ts) = self.expect_parameters(ts.clone())? {
            // from { symbol: "ABC", exchange: "NYSE", last_sale: 67.89 }
            if ts.is("from") {
                let ts = ts.expect("from")?;
                let (from, ts) = self.compile_next(ts)?;
                Ok((Create(TableEntity { path: Box::new(table), columns, from: Some(Box::new(from)) }), ts))
            } else {
                Ok((Create(TableEntity { path: Box::new(table), columns, from: None }), ts))
            }
        } else {
            fail_near("Expected column definitions", &ts)
        }
    }

    /// SQL Delete statement. ex: delete from stocks where last_sale > 1.00
    fn compile_keyword_delete(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let (from, ts) = self.next_keyword_expr("from", ts)?;
        let from = from.expect("Expected keyword 'from'");
        let (condition, ts) = self.next_keyword_expr("where", ts)?;
        let (limit, ts) = self.next_keyword_expr("limit", ts)?;
        Ok((Delete { path: Box::new(from), condition: condition.map(Box::new), limit: limit.map(Box::new) }, ts))
    }

    /// SQL Drop statement. ex: drop table ns('finance.securities.stocks')
    fn compile_keyword_drop(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        match ts.next() {
            (Some(Atom { text: keyword, .. }), ts) => {
                match keyword.as_str() {
                    "table" => {
                        let (expr, ts) = self.compile_next(ts)?;
                        Ok((Drop(TableTarget { path: Box::new(expr), if_exists: false }), ts))
                    }
                    x => fail_near(format!("Invalid type `{}`, try `table` instead", x), &ts)
                }
            }
            (_, ts) => fail_near("Syntax error".to_string(), &ts)
        }
    }

    fn compile_keyword_fn(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        // first, extract the function name
        if let (Some(Atom { text: name, .. }), ts) = ts.next() {
            // next, extract the function parameters
            if let (ColumnSet(params), ts) = self.expect_parameters(ts.clone())? {
                // finally, extract the function code
                let ts = ts.expect("=>")?;
                let (code, ts) = self.compile_next(ts)?;
                Ok((SetVariable(name, Box::new(Literal(Function { params, code: Box::new(code) }))), ts))
            } else {
                fail_near("Function parameters expected", &ts)
            }
        } else {
            fail_near("Function name expected", &ts)
        }
    }

    fn compile_keyword_lambda(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        // next, extract the function parameters
        if let (ColumnSet(params), ts) = self.expect_parameters(ts.clone())? {
            // finally, extract the function code
            let ts = ts.expect("=>")?;
            let (code, ts) = self.compile_next(ts)?;
            Ok((Literal(Function { params, code: Box::new(code) }), ts))
        } else {
            fail_near("Function parameters expected", &ts)
        }
    }

    /// SQL From clause. ex: from stocks
    fn compile_keyword_from(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let (table, ts) = self.compile_next(ts)?;
        Ok((From(Box::new(table)), ts))
    }

    /// Appends a new row to a table
    /// ex: into stocks select symbol: "ABC", exchange: "NYSE", last_sale: 0.1008
    fn compile_keyword_into(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let (table, ts) = self.compile_next(ts)?;
        let (source, ts) = self.compile_next(ts)?;
        Ok((InsertInto { path: Box::new(table), source: Box::new(source) }, ts))
    }

    fn compile_keyword_ns(&mut self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        let (path, ts) = self.compile_next(ts)?;
        Ok((Ns(Box::new(path)), ts))
    }

    /// compiles an OVERWRITE statement:
    /// ex: overwrite stocks
    ///         via {symbol: "ABC", exchange: "NYSE", last_sale: 0.2308}
    ///         where symbol == "ABC"
    ///         limit 5
    fn compile_keyword_overwrite(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let (table, ts) = self.compile_next(ts)?;
        let (source, ts) = self.compile_next(ts)?;
        let (condition, ts) = self.next_keyword_expr("where", ts)?;
        let (limit, ts) = self.next_keyword_expr("limit", ts)?;
        Ok((Overwrite {
            path: Box::new(table),
            source: Box::new(source),
            condition: condition.map(Box::new),
            limit: limit.map(Box::new),
        }, ts))
    }

    fn compile_keyword_queryables(
        &mut self,
        host: Expression,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        match ts.clone() {
            t if t.is("limit") => {
                let (expr, ts) = self.compile_next(ts.skip())?;
                self.compile_keyword_queryables(Limit { from: Box::new(host), limit: Box::new(expr) }, ts)
            }
            t if t.is("where") => {
                let (expr, ts) = self.compile_next(ts.skip())?;
                self.compile_keyword_queryables(Where { from: Box::new(host), condition: Box::new(expr) }, ts)
            }
            _ => Ok((host, ts))
        }
    }

    /// compiles a SELECT statement:
    /// ex: select sum(last_sale) from stocks group by exchange
    fn compile_keyword_select(&mut self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        let (fields, ts) = self.next_expression_list(ts)?;
        let fields = fields.expect("At least one field is required");
        let (from, ts) = self.next_keyword_expr("from", ts)?;
        let (condition, ts) = self.next_keyword_expr("where", ts)?;
        let (group_by, ts) = self.next_keyword_expr_list("group", "by", ts)?;
        let (having, ts) = self.next_keyword_expr("having", ts)?;
        let (order_by, ts) = self.next_keyword_expr_list("order", "by", ts)?;
        let (limit, ts) = self.next_keyword_expr("limit", ts)?;
        Ok((Select {
            fields,
            from: from.map(Box::new),
            condition: condition.map(Box::new),
            group_by,
            having: having.map(Box::new),
            order_by,
            limit: limit.map(Box::new),
        }, ts))
    }

    /// compiles an UPDATE statement:
    /// ex: update stocks set last_sale = 0.45 where symbol == "BANG"
    fn compile_keyword_update(&mut self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        let (table, ts) = self.compile_next(ts)?;
        let (source, ts) = self.compile_next(ts)?;
        let (condition, ts) = self.next_keyword_expr("where", ts)?;
        let (limit, ts) = self.next_keyword_expr("limit", ts)?;
        Ok((Update {
            path: Box::new(table),
            source: Box::new(source),
            condition: condition.map(Box::new),
            limit: limit.map(Box::new),
        }, ts))
    }

    /// SQL From clause. ex: via stocks
    fn compile_keyword_via(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let (table, ts) = self.compile_next(ts)?;
        Ok((Via(Box::new(table)), ts))
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
        let mut args = vec![];
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
        let mut args = vec![];
        let mut ts = ts.expect("(")?;
        while ts.isnt(")") {
            if let (Variable(name), ats) = self.compile_next(ts.clone())? {
                args.push(name);
                ts = if ats.is(")") { ats } else { ats.expect(",")? }
            } else {
                return fail_near("an atom was expected", &ts)
            }
        }
        Ok((args, ts.expect(")")?))
    }

    fn expect_curly_brackets(&mut self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        let mut ts = ts;
        let mut kvps = vec![];
        while ts.isnt("}") {
            match self.compile_next(ts)? {
                (AsValue(name, expr), ats) => {
                    kvps.push((name, *expr));
                    ts = ats;
                }
                (expr, ats) =>
                    return fail_near(format!("Illegal expression {}", expr), &ats),
            }
            if ts.isnt("}") { ts = ts.expect(",")? }
        }
        Ok((JSONLiteral(kvps), ts.expect("}")?))
    }

    fn expect_function_call_or_variable(
        &mut self,
        name: &str,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        // is it a function call? e.g. f(2, 3)
        if ts.is("(") {
            let (args, ts) = self.expect_arguments(ts)?;
            Ok((FunctionCall { fx: Box::new(Variable(name.to_string())), args }, ts))
        } else {
            Ok((Variable(name.to_string()), ts))
        }
    }

    pub fn expect_parameters(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let mut columns = vec![];
        let mut ts = ts.expect("(")?;
        let mut is_done = ts.is(")");
        while !is_done {
            // get the next parameter
            let (param, ats) = self.expect_parameter(ts.clone())?;
            columns.push(param);

            // are we done yet?
            is_done = ats.is(")");
            ts = if !is_done { ats.expect(",")? } else { ats };
        }
        Ok((ColumnSet(columns), ts.expect(")")?))
    }

    fn expect_parameter(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(ColumnJs, TokenSlice)> {
        // attempt to match the parameter name
        // name: String(8) | cost: Float = 0.0
        match ts.next() {
            (Some(Atom { text: name, .. }), ts) => {

                // next, check for type constraint
                let (type_name, ts) = if ts.is(":") {
                    let ts = ts.skip();
                    if let (Some(Atom { text: type_name, .. }), ts) = ts.next() {
                        // e.g. String(8)
                        if ts.is("(") {
                            let (args, ts) = self.expect_arguments(ts)?;
                            (Some(format!("{}({})", type_name,
                                          args.iter().map(|e| e.to_code()).collect::<Vec<String>>().join(", "))), ts)
                        } else { (Some(type_name), ts) }
                    } else { (None, ts) }
                } else { (None, ts) };

                // finally, check for a default value
                let default_value = None;

                Ok((ColumnJs::new(name, type_name.unwrap_or("".to_string()), default_value), ts))
            }
            (_, ats) => fail_near("Function name expected", &ats)
        }
    }

    fn expect_parentheses(&mut self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        let (innards, ts) = ts.scan_to(|t| t.get_raw_value() == ")");
        let innards = TokenSlice::new(innards.to_vec());
        let (opcode, _) = self.compile(innards)?;
        self.pop();
        Ok((opcode, ts.expect(")")?))
    }

    fn expect_square_brackets(&mut self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        let (innards, ts) = ts.scan_to(|t| t.get_raw_value() == "]");
        let mut innards = TokenSlice::new(innards.to_vec());
        let mut items = vec![];
        while innards.has_more() {
            let (expr, its) = self.compile_next(innards)?;
            items.push(expr);
            innards = if its.has_more() { its.expect(",")? } else { its };
        }
        Ok((ArrayLiteral(items), ts.expect("]")?))
    }

    pub fn get_stack(&self) -> Vec<Expression> {
        self.stack.iter().map(|x| x.clone()).collect()
    }

    fn maybe_curly_brackets(
        &mut self,
        ts: TokenSlice,
    ) -> Option<(std::io::Result<Expression>, TokenSlice)> {
        match ts.next() {
            (Some(t), ts) if t.contains("{") =>
                match self.expect_curly_brackets(ts.clone()) {
                    Ok((expr, ts)) => Some((Ok(expr), ts)),
                    Err(err) => Some((fail(format!("{}", err)), ts))
                }
            _ => None
        }
    }

    fn maybe_parentheses(
        &mut self,
        ts: TokenSlice,
    ) -> Option<(std::io::Result<Expression>, TokenSlice)> {
        match ts.next() {
            (Some(t), ts) if t.contains("(") =>
                match self.expect_parentheses(ts.clone()) {
                    Ok((expr, ts)) => Some((Ok(expr), ts)),
                    Err(err) => Some((fail(format!("{}", err)), ts))
                }
            _ => None
        }
    }

    fn maybe_square_brackets(
        &mut self,
        ts: TokenSlice,
    ) -> Option<(std::io::Result<Expression>, TokenSlice)> {
        match ts.next() {
            (Some(t), ts) if t.contains("[") =>
                match self.expect_square_brackets(ts.clone()) {
                    Ok((expr, ts)) => Some((Ok(expr), ts)),
                    Err(err) => Some((fail(format!("{}", err)), ts))
                }
            _ => None
        }
    }

    /// parse an expression list from the [TokenSlice] (e.g. ['x', ',', 'y', ',', 'z'])
    fn next_expression_list(&mut self, ts: TokenSlice) -> std::io::Result<(Option<Vec<Expression>>, TokenSlice)> {
        let mut items = vec![];
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

    /// Returns the option of an expression based the next token matching the specified keyword
    fn next_keyword_expr(&mut self, keyword: &str, ts: TokenSlice) -> std::io::Result<(Option<Expression>, TokenSlice)> {
        if ts.isnt(keyword) { Ok((None, ts)) } else {
            let (expr, ts) = self.compile_next(ts.skip())?;
            Ok((Some(expr), ts))
        }
    }

    /// Returns the [Option] of a list of expressions based the next token matching the specified keywords
    fn next_keyword_expr_list(&mut self, keyword0: &str, keyword1: &str, ts: TokenSlice) -> std::io::Result<(Option<Vec<Expression>>, TokenSlice)> {
        if ts.isnt(keyword0) || ts.skip().isnt(keyword1) { Ok((None, ts)) } else {
            self.next_expression_list(ts.skip().skip())
        }
    }

    /// Returns the [Option] of a list of expressions based the next token matching the specified keyword
    fn next_keyword_value_list(&mut self, keyword: &str, ts: TokenSlice) -> std::io::Result<(Option<Vec<Expression>>, TokenSlice)> {
        if ts.isnt(keyword) { Ok((None, ts)) } else {
            self.next_expression_list(ts.skip())
        }
    }

    pub fn push(&mut self, expression: Expression) {
        info!("push -> {:?}", expression);
        self.stack.push(expression)
    }

    pub fn pop(&mut self) -> Option<Expression> {
        let result = self.stack.pop();
        info!("pop <- {:?}", result);
        result
    }
}

pub fn fail_expr<A>(message: impl Into<String>, expr: &Expression) -> std::io::Result<A> {
    Err(std::io::Error::new(std::io::ErrorKind::Other, format!("{} near {}", message.into(), expr.to_code())))
}

pub fn fail_near<A>(message: impl Into<String>, ts: &TokenSlice) -> std::io::Result<A> {
    Err(std::io::Error::new(std::io::ErrorKind::Other, format!("{} near {}", message.into(), ts)))
}

pub fn fail_token<A>(message: impl Into<String>, t: &Token) -> std::io::Result<A> {
    Err(std::io::Error::new(std::io::ErrorKind::Other, format!("{} near {}", message.into(), t)))
}

pub fn fail_unexpected<A>(expected_type: impl Into<String>, value: &TypedValue) -> std::io::Result<A> {
    fail(format!("Expected a(n) {}, but got {:?}", expected_type.into(), value))
}

pub fn fail_unhandled_expr<A>(expr: &Expression) -> std::io::Result<A> {
    fail_expr("Unhandled expression", expr)
}

pub fn fail_value<A>(message: impl Into<String>, value: &TypedValue) -> std::io::Result<A> {
    Err(std::io::Error::new(std::io::ErrorKind::Other, format!("{} near {}", message.into(), value.unwrap_value())))
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::data_types::*;
    use crate::serialization::{A_ARRAY_LIT, A_JSON_LITERAL, A_LITERAL};
    use crate::server::ColumnJs;
    use crate::typed_values::TypedValue::{Float64Value, Int64Value};

    use super::*;

    #[test]
    fn test_compile_array() {
        assert_eq!(CompilerState::compile_script("[1, 4, 2, 8, 5, 7]").unwrap(),
                   ArrayLiteral(vec![
                       Literal(Int64Value(1)), Literal(Int64Value(4)), Literal(Int64Value(2)),
                       Literal(Int64Value(8)), Literal(Int64Value(5)), Literal(Int64Value(7)),
                   ]))
    }

    #[test]
    fn test_compile_as_expression() {
        let code = CompilerState::compile_script(r#"symbol: "ABC""#).unwrap();
        assert_eq!(code, AsValue("symbol".to_string(), Box::new(Literal(StringValue("ABC".into())))));
    }

    #[test]
    fn test_compile_bitwise_and() {
        assert_eq!(CompilerState::compile_script("20 & 3").unwrap(),
                   BitwiseAnd(
                       Box::new(Literal(Int64Value(20))),
                       Box::new(Literal(Int64Value(3))),
                   ));
    }

    #[test]
    fn test_compile_bitwise_or() {
        assert_eq!(CompilerState::compile_script("20 | 3").unwrap(),
                   BitwiseOr(
                       Box::new(Literal(Int64Value(20))),
                       Box::new(Literal(Int64Value(3))),
                   ));
    }

    #[test]
    fn test_define_function() {
        // examples:
        // fn add(a: u64, b: u64) => a + b
        // fn add(a: u64, b: u64) : u64 => a + b
        let code = CompilerState::compile_script(r#"
        fn add(a, b) => a + b
        "#).unwrap();
        assert_eq!(code,
                   SetVariable("add".to_string(), Box::new(
                       Literal(Function {
                           params: vec![
                               ColumnJs::new("a", "", None),
                               ColumnJs::new("b", "", None),
                           ],
                           code: Box::new(Plus(Box::new(
                               Variable("a".into())
                           ), Box::new(
                               Variable("b".into())
                           ))),
                       })
                   ))
        )
    }

    #[test]
    fn test_lambda_function() {
        // examples:
        // lambda (a: u64, b: u64) : u64 => a + b
        // lambda (a, b) => a + b
        let code = CompilerState::compile_script(r#"
        lambda (a, b) => a * b
        "#).unwrap();
        assert_eq!(code,
                   Literal(Function {
                       params: vec![
                           ColumnJs::new("a", "", None),
                           ColumnJs::new("b", "", None),
                       ],
                       code: Box::new(Multiply(Box::new(
                           Variable("a".into())
                       ), Box::new(
                           Variable("b".into())
                       ))),
                   })
        )
    }

    #[test]
    fn test_function_call() {
        let code = CompilerState::compile_script(r#"
        f(2, 3)
        "#).unwrap();
        assert_eq!(code, FunctionCall {
            fx: Box::new(Variable("f".into())),
            args: vec![
                Literal(Int64Value(2)),
                Literal(Int64Value(3)),
            ],
        })
    }

    #[test]
    fn test_compile_create_index() {
        let code = CompilerState::compile_script(r#"
        create index ns("compiler.create.stocks") [symbol, exchange]
        "#).unwrap();
        assert_eq!(code,
                   Create(IndexEntity {
                       path: Box::new(Ns(Box::new(Literal(StringValue("compiler.create.stocks".into()))))),
                       columns: vec![
                           Variable("symbol".into()),
                           Variable("exchange".into()),
                       ],
                   })
        );
    }

    #[test]
    fn test_compile_create_table() {
        let ns_path = "compiler.create.stocks";
        let code = CompilerState::compile_script(r#"
        create table ns("compiler.create.stocks") (
            symbol: String(8),
            exchange: String(8),
            last_sale: f64)
        "#).unwrap();
        assert_eq!(code,
                   Create(TableEntity {
                       path: Box::new(Ns(Box::new(Literal(StringValue(ns_path.into()))))),
                       columns: vec![
                           ColumnJs::new("symbol", "String(8)", None),
                           ColumnJs::new("exchange", "String(8)", None),
                           ColumnJs::new("last_sale", "f64", None),
                       ],
                       from: None,
                   })
        );
    }

    #[test]
    fn test_compile_drop_table() {
        let code = CompilerState::compile_script(r#"
        drop table ns('finance.securities.stocks')
        "#).unwrap();
        assert_eq!(code,
                   Drop(TableTarget {
                       path: Box::new(Ns(Box::new(Literal(StringValue("finance.securities.stocks".into()))))),
                       if_exists: false,
                   })
        );
    }

    #[test]
    fn test_compile_json_literal_value() {
        let code = CompilerState::compile_script(r#"
        {symbol: "ABC", exchange: "NYSE", last_sale: 16.79}
        "#).unwrap();
        assert_eq!(code,
                   JSONLiteral(vec![
                       ("symbol".to_string(), Literal(StringValue("ABC".into()))),
                       ("exchange".to_string(), Literal(StringValue("NYSE".into()))),
                       ("last_sale".to_string(), Literal(Float64Value(16.79))),
                   ])
        );
    }

    #[test]
    fn test_compile_numeric_literal_value() {
        assert_eq!(CompilerState::compile_script("1_234_567_890").unwrap(),
                   Literal(Int64Value(1_234_567_890)));

        assert_eq!(CompilerState::compile_script("1_234_567.890").unwrap(),
                   Literal(Float64Value(1_234_567.890)));
    }

    #[test]
    fn test_compile_not_expression() {
        assert_eq!(CompilerState::compile_script("!false").unwrap(), Not(Box::new(FALSE)));
        assert_eq!(CompilerState::compile_script("!true").unwrap(), Not(Box::new(TRUE)));
    }

    #[test]
    fn test_compile_mathematical_addition() {
        let opcodes = CompilerState::compile_script("n + 3").unwrap();
        assert_eq!(opcodes,
                   Plus(Box::new(Variable("n".into())), Box::new(Literal(Int64Value(3)))));
    }

    #[test]
    fn test_compile_mathematical_division() {
        let opcodes = CompilerState::compile_script("n / 3").unwrap();
        assert_eq!(opcodes,
                   Divide(Box::new(Variable("n".into())), Box::new(Literal(Int64Value(3)))));
    }

    #[test]
    fn test_compile_mathematical_exponent() {
        let opcodes = CompilerState::compile_script("5 ** 2").unwrap();
        assert_eq!(opcodes,
                   Pow(Box::new(Literal(Int64Value(5))), Box::new(Literal(Int64Value(2)))));
    }

    #[test]
    fn test_compile_mathematical_exponent_via_symbol() {
        let symbols = vec!["⁰", "¹", "²", "³", "⁴", "⁵", "⁶", "⁷", "⁸", "⁹"];
        let mut num = 0;
        for symbol in symbols {
            let opcodes = CompilerState::compile_script(format!("5{}", symbol).as_str()).unwrap();
            assert_eq!(opcodes,
                       Pow(Box::new(Literal(Int64Value(5))), Box::new(Literal(Int64Value(num)))));
            num += 1
        }
    }

    #[test]
    fn test_compile_mathematical_factorial() {
        let opcodes = CompilerState::compile_script("5¡").unwrap();
        assert_eq!(opcodes, Factorial(Box::new(Literal(Int64Value(5)))));
    }

    #[test]
    fn test_compile_mathematical_modulus() {
        let opcodes = CompilerState::compile_script("n % 4").unwrap();
        assert_eq!(opcodes,
                   Modulo(Box::new(Variable("n".into())), Box::new(Literal(Int64Value(4)))));
    }

    #[test]
    fn test_compile_mathematical_multiplication() {
        let opcodes = CompilerState::compile_script("n * 10").unwrap();
        assert_eq!(opcodes,
                   Multiply(Box::new(Variable("n".into())), Box::new(Literal(Int64Value(10)))));
    }

    #[test]
    fn test_compile_mathematical_subtraction() {
        let opcodes = CompilerState::compile_script("_ - 7").unwrap();
        assert_eq!(opcodes,
                   Minus(Box::new(Variable("_".into())), Box::new(Literal(Int64Value(7)))));
    }

    #[test]
    fn test_compile_to_byte_code() {
        // compile script to byte code
        let byte_code = CompilerState::compile_to_byte_code(r#"
        {w:'abc', x:1.0, y:2, z:[1, 2, 3]}
        "#).unwrap();
        assert_eq!(byte_code, vec![
            A_JSON_LITERAL, 0, 0, 0, 0, 0, 0, 0, 8,
            A_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 1, b'w',
            A_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 3, b'a', b'b', b'c',
            A_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 1, b'x',
            A_LITERAL, T_FLOAT64, 63, 240, 0, 0, 0, 0, 0, 0,
            A_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 1, b'y',
            A_LITERAL, T_INT64, 0, 0, 0, 0, 0, 0, 0, 2,
            A_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 1, b'z',
            A_ARRAY_LIT, 0, 0, 0, 0, 0, 0, 0, 3,
            A_LITERAL, T_INT64, 0, 0, 0, 0, 0, 0, 0, 1,
            A_LITERAL, T_INT64, 0, 0, 0, 0, 0, 0, 0, 2,
            A_LITERAL, T_INT64, 0, 0, 0, 0, 0, 0, 0, 3,
        ]);
    }

    #[test]
    fn test_decompile_from_byte_code() {
        let byte_code = vec![
            A_JSON_LITERAL, 0, 0, 0, 0, 0, 0, 0, 8,
            A_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 1, b'w',
            A_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 3, b'a', b'b', b'c',
            A_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 1, b'x',
            A_LITERAL, T_FLOAT64, 63, 240, 0, 0, 0, 0, 0, 0,
            A_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 1, b'y',
            A_LITERAL, T_INT64, 0, 0, 0, 0, 0, 0, 0, 2,
            A_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 1, b'z',
            A_ARRAY_LIT, 0, 0, 0, 0, 0, 0, 0, 3,
            A_LITERAL, T_INT64, 0, 0, 0, 0, 0, 0, 0, 1,
            A_LITERAL, T_INT64, 0, 0, 0, 0, 0, 0, 0, 2,
            A_LITERAL, T_INT64, 0, 0, 0, 0, 0, 0, 0, 3,
        ];
        // decompile byte code to model code
        let code = CompilerState::decompile_from_byte_code(&byte_code).unwrap();
        assert_eq!(code,
                   JSONLiteral(vec![
                       ("w".into(), Literal(StringValue("abc".into()))),
                       ("x".into(), Literal(Float64Value(1.0))),
                       ("y".into(), Literal(Int64Value(2))),
                       ("z".into(), ArrayLiteral(vec![
                           Literal(Int64Value(1)),
                           Literal(Int64Value(2)),
                           Literal(Int64Value(3)),
                       ])),
                   ])
        );
        // decompile model code to source code
        assert_eq!(code.to_code(), r#"{w: "abc", x: 1, y: 2, z: [1, 2, 3]}"#)
    }

    #[test]
    fn test_maybe_curly_brackets() {
        let ts = TokenSlice::from_string(r#"{w:'abc', x:1.0, y:2, z:[1, 2, 3]}"#);
        let mut compiler = CompilerState::new();
        let (result, _) = compiler.maybe_curly_brackets(ts).unwrap();
        let result = result.unwrap();
        assert_eq!(result, JSONLiteral(vec![
            ("w".to_string(), Literal(StringValue("abc".to_string()))),
            ("x".to_string(), Literal(Float64Value(1.))),
            ("y".to_string(), Literal(Int64Value(2))),
            ("z".to_string(), ArrayLiteral(vec![
                Literal(Int64Value(1)),
                Literal(Int64Value(2)),
                Literal(Int64Value(3)),
            ])),
        ]));
    }

    #[test]
    fn test_next_argument_list() {
        let mut compiler = CompilerState::new();
        let ts = TokenSlice::from_string("(abc, 123, 'Hello')");
        let (items, _) = compiler.expect_arguments(ts).unwrap();
        assert_eq!(items, vec![Variable("abc".into()), Literal(Int64Value(123)), Literal(StringValue("Hello".into()))])
    }

    #[test]
    fn test_next_expression() {
        let mut compiler = CompilerState::new();
        let ts = TokenSlice::from_string("abc");
        let (expr, _) = compiler.compile_next(ts).unwrap();
        assert_eq!(expr, Variable("abc".into()))
    }

    #[test]
    fn test_next_expression_list() {
        let mut compiler = CompilerState::new();
        let ts = TokenSlice::from_string("abc, 123.0, 'Hello'");
        let (items, _) = compiler.next_expression_list(ts).unwrap();
        assert_eq!(items, Some(vec![Variable("abc".into()), Literal(Float64Value(123.)), Literal(StringValue("Hello".into()))]))
    }

    #[test]
    fn test_order_of_operations_1() {
        let opcodes = CompilerState::compile_script("2 + (4 * 3)").unwrap();
        assert_eq!(opcodes,
                   Plus(Box::new(Literal(Int64Value(2))),
                        Box::new(Multiply(Box::new(Literal(Int64Value(4))),
                                          Box::new(Literal(Int64Value(3))))),
                   ));
    }

    #[test]
    fn test_order_of_operations_2() {
        let opcodes = CompilerState::compile_script("(4.0 / 3.0) + (4 * 3)").unwrap();
        assert_eq!(opcodes,
                   Plus(
                       Box::new(Divide(Box::new(Literal(Float64Value(4.0))), Box::new(Literal(Float64Value(3.0))))),
                       Box::new(Multiply(Box::new(Literal(Int64Value(4))), Box::new(Literal(Int64Value(3))))),
                   ));
    }

    #[ignore]
    #[test]
    fn test_order_of_operations_3() {
        let opcodes = CompilerState::compile_script("2 - 4 * 3").unwrap();
        assert_eq!(opcodes,
                   Minus(Box::new(Literal(Float64Value(2.))),
                         Box::new(Multiply(Box::new(Literal(Float64Value(4.))),
                                           Box::new(Literal(Float64Value(3.))))),
                   ));
    }

    #[test]
    fn test_ql_delete() {
        let opcodes = CompilerState::compile_script(r#"
        delete from stocks
        "#).unwrap();
        assert_eq!(opcodes, Delete {
            path: Box::new(Variable("stocks".into())),
            condition: None,
            limit: None,
        })
    }

    #[test]
    fn test_ql_delete_where_limit() {
        let opcodes = CompilerState::compile_script(r#"
        delete from stocks
        where last_sale >= 1.0
        limit 100
        "#).unwrap();
        assert_eq!(opcodes, Delete {
            path: Box::new(Variable("stocks".into())),
            condition: Some(
                Box::new(GreaterOrEqual(
                    Box::new(Variable("last_sale".into())),
                    Box::new(Literal(Float64Value(1.0))),
                ))
            ),
            limit: Some(Box::new(Literal(Int64Value(100)))),
        })
    }


    #[test]
    fn test_ql_from() {
        let opcodes = CompilerState::compile_script("from stocks").unwrap();
        assert_eq!(opcodes, From(Box::new(Variable("stocks".into()))));
    }

    #[test]
    fn test_ql_from_where_limit() {
        let opcodes = CompilerState::compile_script(r#"
        from stocks where last_sale >= 1.0 limit 20
        "#).unwrap();
        assert_eq!(opcodes,
                   Limit {
                       from: Box::new(
                           Where {
                               from: Box::new(From(Box::new(Variable("stocks".into())))),
                               condition: Box::new(GreaterOrEqual(
                                   Box::new(Variable("last_sale".into())),
                                   Box::new(Literal(Float64Value(1.0))),
                               )),
                           }),
                       limit: Box::new(Literal(Int64Value(20))),
                   }
        );
    }

    #[test]
    fn test_ql_into_from_variable() {
        let opcodes = CompilerState::compile_script(r#"
        into ns("compiler.into.stocks") from stocks
        "#).unwrap();
        assert_eq!(opcodes, InsertInto {
            path: Box::new(Ns(Box::new(Literal(StringValue("compiler.into.stocks".to_string()))))),
            source: Box::new(From(Box::new(Variable("stocks".into())))),
        })
    }

    #[test]
    fn test_ql_into_from_json_literal() {
        let opcodes = CompilerState::compile_script(r#"
        into ns("compiler.into.stocks")
        from { symbol: "ABC", exchange: "NYSE", last_sale: 0.1008 }
        "#).unwrap();
        assert_eq!(opcodes, InsertInto {
            path: Box::new(Ns(Box::new(Literal(StringValue("compiler.into.stocks".to_string()))))),
            source: Box::new(From(Box::new(JSONLiteral(vec![
                ("symbol".into(), Literal(StringValue("ABC".into()))),
                ("exchange".into(), Literal(StringValue("NYSE".into()))),
                ("last_sale".into(), Literal(Float64Value(0.1008))),
            ])))),
        })
    }

    #[test]
    fn test_ql_into_from_json_array() {
        let opcodes = CompilerState::compile_script(r#"
        into ns("compiler.into.stocks")
        from [
            { symbol: "ABC", exchange: "NYSE", last_sale: 11.1234 },
            { symbol: "DOG", exchange: "NASDAQ", last_sale: 0.1008 },
            { symbol: "SHARK", exchange: "AMEX", last_sale: 52.08 }
        ]
        "#).unwrap();
        assert_eq!(opcodes, InsertInto {
            path: Box::new(Ns(Box::new(Literal(StringValue("compiler.into.stocks".to_string()))))),
            source: Box::new(From(Box::new(
                ArrayLiteral(vec![
                    JSONLiteral(vec![
                        ("symbol".into(), Literal(StringValue("ABC".into()))),
                        ("exchange".into(), Literal(StringValue("NYSE".into()))),
                        ("last_sale".into(), Literal(Float64Value(11.1234))),
                    ]),
                    JSONLiteral(vec![
                        ("symbol".into(), Literal(StringValue("DOG".into()))),
                        ("exchange".into(), Literal(StringValue("NASDAQ".into()))),
                        ("last_sale".into(), Literal(Float64Value(0.1008))),
                    ]),
                    JSONLiteral(vec![
                        ("symbol".into(), Literal(StringValue("SHARK".into()))),
                        ("exchange".into(), Literal(StringValue("AMEX".into()))),
                        ("last_sale".into(), Literal(Float64Value(52.08))),
                    ]),
                ]))
            )),
        })
    }

    #[test]
    fn test_ql_ns() {
        let code = CompilerState::compile_script(r#"
        ns("securities.etf.stocks")
        "#).unwrap();
        assert_eq!(code, Ns(Box::new(Literal(StringValue("securities.etf.stocks".to_string())))))
    }

    #[test]
    fn test_ql_overwrite() {
        let opcodes = CompilerState::compile_script(r#"
        overwrite stocks
        via {symbol: "ABC", exchange: "NYSE", last_sale: 0.2308}
        where symbol == "ABCQ"
        limit 5
        "#).unwrap();
        assert_eq!(opcodes, Overwrite {
            path: Box::new(Variable("stocks".into())),
            source: Box::new(Via(Box::new(JSONLiteral(vec![
                ("symbol".into(), Literal(StringValue("ABC".into()))),
                ("exchange".into(), Literal(StringValue("NYSE".into()))),
                ("last_sale".into(), Literal(Float64Value(0.2308))),
            ])))),
            condition: Some(
                Box::new(Equal(
                    Box::new(Variable("symbol".into())),
                    Box::new(Literal(StringValue("ABCQ".into()))),
                ))),
            limit: Some(Box::new(Literal(Int64Value(5)))),
        })
    }

    #[test]
    fn test_ql_select_from() {
        let opcodes = CompilerState::compile_script(r#"
        select symbol, exchange, last_sale from stocks
        "#).unwrap();
        assert_eq!(opcodes, Select {
            fields: vec![Variable("symbol".into()), Variable("exchange".into()), Variable("last_sale".into())],
            from: Some(Box::new(Variable("stocks".into()))),
            condition: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
        })
    }

    #[test]
    fn test_ql_select_from_where() {
        let opcodes = CompilerState::compile_script(r#"
        select symbol, exchange, last_sale from stocks
        where last_sale >= 1.0
        "#).unwrap();
        assert_eq!(opcodes, Select {
            fields: vec![Variable("symbol".into()), Variable("exchange".into()), Variable("last_sale".into())],
            from: Some(Box::new(Variable("stocks".into()))),
            condition: Some(
                Box::new(GreaterOrEqual(
                    Box::new(Variable("last_sale".into())),
                    Box::new(Literal(Float64Value(1.0))),
                ))
            ),
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
        })
    }

    #[test]
    fn test_ql_select_from_where_limit() {
        let opcodes = CompilerState::compile_script(r#"
        select symbol, exchange, last_sale from stocks
        where last_sale <= 1.0
        limit 5
        "#).unwrap();
        assert_eq!(opcodes, Select {
            fields: vec![Variable("symbol".into()), Variable("exchange".into()), Variable("last_sale".into())],
            from: Some(Box::new(Variable("stocks".into()))),
            condition: Some(
                Box::new(LessOrEqual(
                    Box::new(Variable("last_sale".into())),
                    Box::new(Literal(Float64Value(1.0))),
                ))
            ),
            group_by: None,
            having: None,
            order_by: None,
            limit: Some(Box::new(Literal(Int64Value(5)))),
        })
    }

    #[test]
    fn test_ql_select_from_where_order_by_limit() {
        let opcodes = CompilerState::compile_script(r#"
        select symbol, exchange, last_sale from stocks
        where last_sale < 1.0
        order by symbol
        limit 5
        "#).unwrap();
        assert_eq!(opcodes, Select {
            fields: vec![Variable("symbol".into()), Variable("exchange".into()), Variable("last_sale".into())],
            from: Some(Box::new(Variable("stocks".into()))),
            condition: Some(
                Box::new(LessThan(
                    Box::new(Variable("last_sale".into())),
                    Box::new(Literal(Float64Value(1.0))),
                ))
            ),
            group_by: None,
            having: None,
            order_by: Some(vec![Variable("symbol".into())]),
            limit: Some(Box::new(Literal(Int64Value(5)))),
        })
    }

    #[test]
    fn test_ql_update() {
        let opcodes = CompilerState::compile_script(r#"
        update stocks
        via { last_sale: 0.1111 }
        where symbol == "ABC"
        limit 10
        "#).unwrap();
        assert_eq!(opcodes, Update {
            path: Box::new(Variable("stocks".into())),
            source: Box::new(Via(Box::new(JSONLiteral(vec![
                ("last_sale".into(), Literal(Float64Value(0.1111))),
            ])))),
            condition: Some(
                Box::new(Equal(
                    Box::new(Variable("symbol".into())),
                    Box::new(Literal(StringValue("ABC".into()))),
                ))
            ),
            limit: Some(Box::new(Literal(Int64Value(10)))),
        })
    }
}