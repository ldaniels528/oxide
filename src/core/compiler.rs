////////////////////////////////////////////////////////////////////
// compiler module
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

use shared_lib::fail;

use crate::expression::{ACK, Expression, FALSE, Infrastructure, MutateTarget, Mutation, NULL, Queryable, TRUE, UNDEFINED};
use crate::expression::CreationEntity::{IndexEntity, TableEntity};
use crate::expression::Expression::*;
use crate::expression::MutateTarget::TableTarget;
use crate::expression::Mutation::IntoNs;
use crate::serialization::{assemble, disassemble_fully};
use crate::server::ColumnJs;
use crate::token_slice::TokenSlice;
use crate::tokens::Token;
use crate::tokens::Token::{Atom, Backticks, DoubleQuoted, Numeric, Operator, SingleQuoted};
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{Function, Int64Value, StringValue};

/// Represents the Oxide compiler
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
    pub fn compile_script(source_code: &str) -> std::io::Result<Expression> {
        let mut compiler = Compiler::new();
        let (code, _) = compiler.compile(TokenSlice::from_string(source_code))?;
        Ok(code)
    }

    /// compiles the source code into byte code
    pub fn compile_to_byte_code(source_code: &str) -> std::io::Result<Vec<u8>> {
        let code = Compiler::compile_script(source_code)?;
        let byte_code = assemble(&code);
        Ok(byte_code)
    }

    /// decompiles the byte code into model expressions
    pub fn decompile_from_byte_code(byte_code: &Vec<u8>) -> std::io::Result<Expression> {
        Ok(disassemble_fully(&byte_code)?)
    }

    /// creates a new [Compiler] instance
    pub fn new() -> Self {
        Compiler {
            stack: vec![],
        }
    }

    ////////////////////////////////////////////////////////////////
    // instance methods
    ////////////////////////////////////////////////////////////////

    /// compiles the entire [TokenSlice] into an [Expression]
    pub fn compile(&mut self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        // consume the entire iterator
        let mut opcodes = vec![];
        let mut ts = ts;
        while ts.has_more() {
            let (expr, tts) = self.compile_next(ts)?;
            //println!("compile: expr -> {:?}", expr);
            opcodes.push(expr);
            ts = tts;
        }

        // return the instruction
        match opcodes {
            ops if ops.len() == 1 => Ok((ops[0].clone(), ts)),
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
            (Some(Atom { .. }), _) => self.parse_atom(ts),
            (Some(DoubleQuoted { text, .. } |
                  SingleQuoted { text, .. }), ts) =>
                Ok((Literal(StringValue(text.into())), ts)),
            (Some(Numeric { text, .. }), ts) =>
                Ok((Literal(TypedValue::from_numeric(text.as_str())?), ts)),
            (None, ts) => fail_near("Unexpected end of input", &ts)
        }?;

        // handle postfix operator?
        match ts.next() {
            // non-barrier operator: "," | ";"
            (Some(Operator { is_barrier, .. }), _) if !is_barrier => {
                self.push(expr);
                self.compile_next(ts)
            }
            // array index: "items[n]"
            (Some(Operator { text, .. }), ts) if text == "[" && ts.is_previous_adjacent() => {
                let (index, ts) = self.compile_next(ts)?;
                let ts = ts.expect("]")?;
                let model = ArrayIndex(Box::new(expr), Box::new(index));
                if ts.has_more() {
                    self.push(model);
                    self.compile_next(ts)
                } else { Ok((model, ts)) }
            }
            // anything else is passed through...
            _ => Ok((expr, ts))
        }
    }

    fn parse_atom(&mut self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        if let (Some(Atom { text, .. }), ts) = ts.next() {
            match text.as_str() {
                "[!]" => self.parse_expression_1a(ts, MustDie),
                "[+]" => self.parse_expression_1a(ts, MustAck),
                "[-]" => self.parse_expression_1a(ts, MustNotAck),
                "[~]" => self.parse_expression_1a(ts, MustIgnoreAck),
                "ack" => Ok((ACK, ts)),
                "append" => self.parse_keyword_append(ts),
                "create" => self.parse_keyword_create(ts),
                "delete" => self.parse_keyword_delete(ts),
                "drop" => self.parse_mutate_target(ts, |m| Perform(Infrastructure::Drop(m))),
                "eval" => self.parse_expression_1a(ts, Eval),
                "false" => Ok((FALSE, ts)),
                "fn" => self.parse_keyword_fn(ts),
                "from" => {
                    let (from, ts) = self.parse_expression_1a(ts, From)?;
                    self.parse_queryable(from, ts)
                }
                "include" => self.parse_expression_1a(ts, Include),
                "if" => self.parse_keyword_if(ts),
                "iff" => self.parse_keyword_iff(ts),
                "limit" => fail_near("`from` is expected before `limit`: from stocks limit 5", &ts),
                "ns" => self.parse_expression_1a(ts, Ns),
                "null" => Ok((NULL, ts)),
                "overwrite" => self.parse_keyword_overwrite(ts),
                "reverse" => self.parse_expression_1a(ts, |q| Inquire(Queryable::Reverse(q))),
                "select" => self.parse_keyword_select(ts),
                "stderr" => self.parse_expression_1a(ts, StdErr),
                "stdout" => self.parse_expression_1a(ts, StdOut),
                "syscall" => self.parse_keyword_syscall(ts),
                "table" => self.parse_keyword_table(ts),
                "true" => Ok((TRUE, ts)),
                "undefined" => Ok((UNDEFINED, ts)),
                "undelete" => self.parse_keyword_undelete(ts),
                "update" => self.parse_keyword_update(ts),
                "via" => self.parse_expression_1a(ts, Via),
                "where" => fail_near("`from` is expected before `where`: from stocks where last_sale < 1.0", &ts),
                "while" => self.parse_keyword_while(ts),
                name => self.expect_function_call_or_variable(name, ts)
            }
        } else { fail("Unexpected end of input") }
    }

    /// compiles the [TokenSlice] into a leading single-parameter [Expression] (e.g. !true)
    fn parse_expression_1a(
        &mut self,
        ts: TokenSlice,
        f: fn(Box<Expression>,
        ) -> Expression) -> std::io::Result<(Expression, TokenSlice)> {
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
            None => fail_near("expected an expression", &ts),
            Some(expr0) => Ok((f(Box::new(expr0)), ts))
        }
    }

    /// compiles the [TokenSlice] into a two-parameter [Expression]
    fn parse_expression_2a(
        &mut self,
        ts: TokenSlice,
        expr0: Expression,
        f: fn(Box<Expression>, Box<Expression>,
        ) -> Expression) -> std::io::Result<(Expression, TokenSlice)> {
        let (expr1, ts) = self.compile_next(ts)?;
        Ok((f(Box::new(expr0), Box::new(expr1)), ts))
    }

    /// compiles the [TokenSlice] into a two-parameter [Expression]
    fn parse_expression_2b(
        &mut self, ts: TokenSlice,
        expr1: Expression,
        f: fn(Box<Expression>, Box<Expression>,
        ) -> Expression) -> std::io::Result<(Expression, TokenSlice)> {
        match self.pop() {
            None => fail_near("expected an expression", &ts),
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
        if let Variable(name) = expr0 {
            let (expr1, ts) = self.compile_next(ts)?;
            Ok((f(name, Box::new(expr1)), ts))
        } else { fail_near("an identifier name was expected", &ts) }
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
            (_, ts) => fail_near("Invalid identifier".to_string(), &ts)
        }
    }

    /// Appends a new row to a table
    /// ex: append stocks select symbol: "ABC", exchange: "NYSE", last_sale: 0.1008
    fn parse_keyword_append(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let (table, ts) = self.compile_next(ts)?;
        let (source, ts) = self.compile_next(ts)?;
        Ok((Mutate(Mutation::Append { path: Box::new(table), source: Box::new(source) }), ts))
    }

    fn parse_keyword_create(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        if let (Some(t), ts) = ts.next() {
            match t.get_raw_value().as_str() {
                "index" => self.parse_keyword_create_index(ts),
                "table" => self.parse_keyword_create_table(ts),
                name => fail_near(format!("Syntax error: expect type identifier, got '{}'", name), &ts)
            }
        } else { fail("Unexpected end of input") }
    }

    fn parse_keyword_create_index(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let (index, ts) = self.compile_next(ts)?;
        let ts = ts.expect("on")?;
        if let (ArrayLiteral(columns), ts) = self.compile_next(ts.clone())? {
            Ok((Perform(Infrastructure::Create {
                path: Box::new(index),
                entity: IndexEntity { columns },
            }), ts))
        } else {
            fail_near("Columns expected", &ts)
        }
    }

    fn parse_keyword_create_table(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        // create table `stocks` (name: String, ..)
        let (table, ts) = self.compile_next(ts)?;
        if let (ColumnSet(columns), ts) = self.expect_parameters(ts.clone())? {
            // from { symbol: "ABC", exchange: "NYSE", last_sale: 67.89 }
            let (from, ts) =
                if ts.is("from") {
                    let ts = ts.expect("from")?;
                    let (from, ts) = self.compile_next(ts)?;
                    (Some(Box::new(from)), ts)
                } else {
                    (None, ts)
                };
            Ok((Perform(Infrastructure::Create {
                path: Box::new(table),
                entity: TableEntity { columns, from },
            }), ts))
        } else {
            fail_near("Expected column definitions", &ts)
        }
    }

    /// SQL Delete statement. ex: delete from stocks where last_sale > 1.00
    fn parse_keyword_delete(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let (from, ts) = self.next_keyword_expr("from", ts)?;
        let from = from.expect("Expected keyword 'from'");
        let (condition, ts) = self.next_keyword_expr("where", ts)?;
        let (limit, ts) = self.next_keyword_expr("limit", ts)?;
        Ok((Mutate(Mutation::Delete { path: Box::new(from), condition: condition.map(Box::new), limit: limit.map(Box::new) }), ts))
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
            _ => fail_near("Function name expected", &ts)
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

    /// Builds a language model from an if function
    /// ex: iff(x > 5, x, 5)
    fn parse_keyword_iff(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let (args, ts) = self.expect_arguments(ts)?;
        match args.as_slice() {
            [c, a, b] =>
                Ok((If {
                    condition: Box::new(c.clone()),
                    a: Box::new(a.clone()),
                    b: Some(Box::new(b.clone())),
                }, ts)),
            _ => fail_near("Syntax error. Usage: iff(cond, a, b)", &ts)
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
        let (condition, ts) = self.next_keyword_expr("where", ts)?;
        let (limit, ts) = self.next_keyword_expr("limit", ts)?;
        Ok((Mutate(Mutation::Overwrite {
            path: Box::new(table),
            source: Box::new(source),
            condition: condition.map(Box::new),
            limit: limit.map(Box::new),
        }), ts))
    }

    /// Builds a language model from a SELECT statement:
    /// ex: select sum(last_sale) from stocks group by exchange
    fn parse_keyword_select(&mut self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        let (fields, ts) = self.next_expression_list(ts)?;
        let fields = fields.expect("At least one field is required");
        let (from, ts) = self.next_keyword_expr("from", ts)?;
        let (condition, ts) = self.next_keyword_expr("where", ts)?;
        let (group_by, ts) = self.next_keyword_expression_list("group", "by", ts)?;
        let (having, ts) = self.next_keyword_expr("having", ts)?;
        let (order_by, ts) = self.next_keyword_expression_list("order", "by", ts)?;
        let (limit, ts) = self.next_keyword_expr("limit", ts)?;
        Ok((Inquire(Queryable::Select {
            fields,
            from: from.map(Box::new),
            condition: condition.map(Box::new),
            group_by,
            having: having.map(Box::new),
            order_by,
            limit: limit.map(Box::new),
        }), ts))
    }

    /// Builds a language model from a SYSCALL statement:
    /// ex: syscall("ps", "aux")
    fn parse_keyword_syscall(&mut self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        let (args, ts) = self.expect_arguments(ts)?;
        Ok((SystemCall(args), ts))
    }

    /// Builds a language model from a table expression.
    /// ex: stocks := table (symbol: String(8), exchange: String(8), last_sale: f64)
    fn parse_keyword_table(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        if let (ColumnSet(columns), ts) = self.expect_parameters(ts.clone())? {
            // from { symbol: "ABC", exchange: "NYSE", last_sale: 67.89 }
            let (from, ts) = if ts.is("from") {
                let ts = ts.expect("from")?;
                let (from, ts) = self.compile_next(ts)?;
                (Some(Box::new(from)), ts)
            } else {
                (None, ts)
            };
            Ok((Perform(Infrastructure::Declare(TableEntity { columns, from })), ts))
        } else {
            fail_near("Expected column definitions", &ts)
        }
    }

    /// Builds a language model from an UNDELETE statement:
    /// ex: undelete from stocks where symbol == "BANG"
    fn parse_keyword_undelete(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let (from, ts) = self.next_keyword_expr("from", ts)?;
        let from = from.expect("Expected keyword 'from'");
        let (condition, ts) = self.next_keyword_expr("where", ts)?;
        let (limit, ts) = self.next_keyword_expr("limit", ts)?;
        Ok((Mutate(Mutation::Undelete { path: Box::new(from), condition: condition.map(Box::new), limit: limit.map(Box::new) }), ts))
    }

    /// Builds a language model from an UPDATE statement:
    /// ex: update stocks set last_sale = 0.45 where symbol == "BANG"
    fn parse_keyword_update(&mut self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        let (table, ts) = self.compile_next(ts)?;
        let (source, ts) = self.compile_next(ts)?;
        let (condition, ts) = self.next_keyword_expr("where", ts)?;
        let (limit, ts) = self.next_keyword_expr("limit", ts)?;
        Ok((Mutate(Mutation::Update {
            path: Box::new(table),
            source: Box::new(source),
            condition: condition.map(Box::new),
            limit: limit.map(Box::new),
        }), ts))
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
                    z => fail_near(format!("Invalid type `{}`, try `table` instead", z), &ts)
                }
            }
            (_, ts) => fail_near("Syntax error".to_string(), &ts)
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
            compiler.parse_expression_2b(ts, Literal(Int64Value(n)), Pow)
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
            "!" => self.parse_expression_1a(ts, Not),
            "$" => self.parse_identifier(ts, Variable),
            "(" => self.expect_parentheses(ts),
            "[" => self.expect_square_brackets(ts),
            "{" => self.expect_curly_brackets(ts),
            sym =>
                if let Some(op0) = self.pop() {
                    match sym {
                        "&&" => self.parse_expression_2a(ts, op0, And),
                        ":" => self.parse_expression_2nv(ts, op0, AsValue),
                        "&" => self.parse_expression_2a(ts, op0, BitwiseAnd),
                        "|" => self.parse_expression_2a(ts, op0, BitwiseOr),
                        "^" => self.parse_expression_2a(ts, op0, BitwiseXor),
                        "÷" | "/" => self.parse_expression_2a(ts, op0, Divide),
                        "==" => self.parse_expression_2a(ts, op0, Equal),
                        ">" => self.parse_expression_2a(ts, op0, GreaterThan),
                        ">=" => self.parse_expression_2a(ts, op0, GreaterOrEqual),
                        "<" => self.parse_expression_2a(ts, op0, LessThan),
                        "<=" => self.parse_expression_2a(ts, op0, LessOrEqual),
                        "-" => self.parse_expression_2a(ts, op0, Minus),
                        "%" => self.parse_expression_2a(ts, op0, Modulo),
                        "×" | "*" => self.parse_expression_2a(ts, op0, Multiply),
                        "|>" => self.parse_expression_2a(ts, op0, |a, b| Mutate(IntoNs(a, b))),
                        "!=" => self.parse_expression_2a(ts, op0, NotEqual),
                        "||" => self.parse_expression_2a(ts, op0, Or),
                        "+" => self.parse_expression_2a(ts, op0, Plus),
                        "**" => self.parse_expression_2a(ts, op0, Pow),
                        ".." => self.parse_expression_2a(ts, op0, Range),
                        ":=" => self.parse_expression_2nv(ts, op0, SetVariable),
                        "<<" => self.parse_expression_2a(ts, op0, ShiftLeft),
                        ">>" => self.parse_expression_2a(ts, op0, ShiftRight),
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
        match ts.clone() {
            t if t.is("limit") => {
                let (expr, ts) = self.compile_next(ts.skip())?;
                self.parse_queryable(Inquire(Queryable::Limit { from: Box::new(host), limit: Box::new(expr) }), ts)
            }
            t if t.is("where") => {
                let (expr, ts) = self.compile_next(ts.skip())?;
                self.parse_queryable(Inquire(Queryable::Where { from: Box::new(host), condition: Box::new(expr) }), ts)
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
                return fail_near("an atom was expected", &ts);
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
        // is it a function call? e.g., f(2, 3)
        if ts.is("(") {
            let (args, ts) = self.expect_arguments(ts)?;
            Ok((FunctionCall { fx: Box::new(Variable(name.to_string())), args }, ts))
        }
        // must be a variable. e.g., abc
        else {
            Ok((Variable(name.to_string()), ts))
        }
    }

    fn expect_function_parameters_and_body(
        &mut self,
        name: Option<String>,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        // extract the function parameters
        if let (ColumnSet(params), ts) = self.expect_parameters(ts.clone())? {
            // extract the function body
            let ts = ts.expect("=>")?;
            let (body, ts) = self.compile_next(ts)?;
            // build the model
            let func = Literal(Function { params, code: Box::new(body) });
            match name {
                Some(name) => Ok((SetVariable(name, Box::new(func)), ts)),
                None => Ok((func, ts))
            }
        } else {
            fail_near("Function parameters expected", &ts)
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
    fn next_keyword_expression_list(&mut self, keyword0: &str, keyword1: &str, ts: TokenSlice) -> std::io::Result<(Option<Vec<Expression>>, TokenSlice)> {
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
        //println!("push -> {:?}", expression);
        self.stack.push(expression)
    }

    pub fn pop(&mut self) -> Option<Expression> {
        let result = self.stack.pop();
        //println!("pop <- {:?}", result);
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
    use crate::expression::{E_ARRAY_LIT, E_JSON_LITERAL, E_LITERAL, Infrastructure, Mutation, Queryable};
    use crate::server::ColumnJs;
    use crate::typed_values::TypedValue::{Float64Value, Int64Value};

    use super::*;

    #[test]
    fn test_aliases() {
        let script = r#"symbol: "ABC""#;
        let code = Compiler::compile_script(script).unwrap();
        assert_eq!(code, AsValue("symbol".to_string(), Box::new(Literal(StringValue("ABC".into())))));
    }

    #[test]
    fn test_array_declaration() {
        let script = "[7, 5, 8, 2, 4, 1]";
        let model = ArrayLiteral(vec![
            Literal(Int64Value(7)), Literal(Int64Value(5)), Literal(Int64Value(8)),
            Literal(Int64Value(2)), Literal(Int64Value(4)), Literal(Int64Value(1)),
        ]);
        assert_eq!(model.to_code(), script);
        assert_eq!(Compiler::compile_script(script).unwrap(), model)
    }

    #[test]
    fn test_array_indexing() {
        let script = "[7, 5, 8, 2, 4, 1][3]";
        let model = ArrayIndex(
            Box::new(ArrayLiteral(vec![
                Literal(Int64Value(7)), Literal(Int64Value(5)), Literal(Int64Value(8)),
                Literal(Int64Value(2)), Literal(Int64Value(4)), Literal(Int64Value(1)),
            ])),
            Box::new(Literal(Int64Value(3))),
        );
        assert_eq!(model.to_code(), script);
        assert_eq!(Compiler::compile_script(script).unwrap(), model);
    }

    #[test]
    fn test_bitwise_and() {
        assert_eq!(Compiler::compile_script("20 & 3").unwrap(),
                   BitwiseAnd(
                       Box::new(Literal(Int64Value(20))),
                       Box::new(Literal(Int64Value(3))),
                   ));
    }

    #[test]
    fn test_bitwise_or() {
        assert_eq!(Compiler::compile_script("20 | 3").unwrap(),
                   BitwiseOr(
                       Box::new(Literal(Int64Value(20))),
                       Box::new(Literal(Int64Value(3))),
                   ));
    }

    #[test]
    fn test_define_named_function() {
        // examples:
        // fn add(a: u64, b: u64) => a + b
        // fn add(a: u64, b: u64) : u64 => a + b
        let code = Compiler::compile_script(r#"
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
    fn test_define_anonymous_function() {
        // examples:
        // fn (a: u64, b: u64) : u64 => a + b
        // fn (a, b) => a + b
        let code = Compiler::compile_script(r#"
        fn (a, b) => a * b
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
    fn test_eval() {
        let code = Compiler::compile_script(r#"
        eval "5 + 7"
        "#).unwrap();
        assert_eq!(code, Eval(Box::new(Literal(StringValue("5 + 7".to_string())))))
    }

    #[test]
    fn test_function_call() {
        let code = Compiler::compile_script(r#"
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
    fn create_index_in_namespace() {
        let code = Compiler::compile_script(r#"
        create index ns("compiler.create.stocks") on [symbol, exchange]
        "#).unwrap();
        assert_eq!(code, Perform(Infrastructure::Create {
            path: Box::new(Ns(Box::new(Literal(StringValue("compiler.create.stocks".into()))))),
            entity: IndexEntity {
                columns: vec![
                    Variable("symbol".into()),
                    Variable("exchange".into()),
                ],
            },
        }));
    }

    #[test]
    fn create_table_in_namespace() {
        let ns_path = "compiler.create.stocks";
        let code = Compiler::compile_script(r#"
        create table ns("compiler.create.stocks") (
            symbol: String(8),
            exchange: String(8),
            last_sale: f64)
        "#).unwrap();
        assert_eq!(code, Perform(Infrastructure::Create {
            path: Box::new(Ns(Box::new(Literal(StringValue(ns_path.into()))))),
            entity: TableEntity {
                columns: vec![
                    ColumnJs::new("symbol", "String(8)", None),
                    ColumnJs::new("exchange", "String(8)", None),
                    ColumnJs::new("last_sale", "f64", None),
                ],
                from: None,
            },
        }));
    }

    #[test]
    fn test_declare_table() {
        let model = Compiler::compile_script(r#"
        table(
            symbol: String(8),
            exchange: String(8),
            last_sale: f64
        )"#).unwrap();
        assert_eq!(model, Perform(Infrastructure::Declare(TableEntity {
            columns: vec![
                ColumnJs::new("symbol", "String(8)", None),
                ColumnJs::new("exchange", "String(8)", None),
                ColumnJs::new("last_sale", "f64", None),
            ],
            from: None,
        })));
    }

    #[test]
    fn test_drop_table() {
        let code = Compiler::compile_script(r#"
        drop table ns('finance.securities.stocks')
        "#).unwrap();
        assert_eq!(code,
                   Perform(Infrastructure::Drop(TableTarget {
                       path: Box::new(Ns(Box::new(Literal(StringValue("finance.securities.stocks".into()))))),
                   }))
        );
    }

    #[test]
    fn test_if() {
        let code = Compiler::compile_script(r#"
        if(n > 100) "Yes"
        "#).unwrap();
        assert_eq!(code, If {
            condition: Box::new(GreaterThan(
                Box::new(Variable("n".to_string())),
                Box::new(Literal(Int64Value(100))),
            )),
            a: Box::new(Literal(StringValue("Yes".to_string()))),
            b: None,
        });
    }

    #[test]
    fn test_if_else() {
        let code = Compiler::compile_script(r#"
        if(n > 100) n else m
        "#).unwrap();
        assert_eq!(code, If {
            condition: Box::new(GreaterThan(
                Box::new(Variable("n".to_string())),
                Box::new(Literal(Int64Value(100))),
            )),
            a: Box::new(Variable("n".to_string())),
            b: Some(Box::new(Variable("m".to_string()))),
        });
    }

    #[test]
    fn test_iff() {
        let code = Compiler::compile_script(r#"
        iff(x > 5, a, b)
        "#).unwrap();
        assert_eq!(code, If {
            condition: Box::new(GreaterThan(
                Box::new(Variable("x".to_string())),
                Box::new(Literal(Int64Value(5))),
            )),
            a: Box::new(Variable("a".to_string())),
            b: Some(Box::new(Variable("b".to_string()))),
        });
    }

    #[test]
    fn test_json_literal_value() {
        let code = Compiler::compile_script(r#"
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
    fn test_numeric_literal_value() {
        assert_eq!(Compiler::compile_script("1_234_567_890").unwrap(),
                   Literal(Int64Value(1_234_567_890)));

        assert_eq!(Compiler::compile_script("1_234_567.890").unwrap(),
                   Literal(Float64Value(1_234_567.890)));
    }

    #[test]
    fn test_not_expression() {
        assert_eq!(Compiler::compile_script("!false").unwrap(), Not(Box::new(FALSE)));
        assert_eq!(Compiler::compile_script("!true").unwrap(), Not(Box::new(TRUE)));
    }

    #[test]
    fn test_mathematical_addition() {
        let opcodes = Compiler::compile_script("n + 3").unwrap();
        assert_eq!(opcodes,
                   Plus(Box::new(Variable("n".into())), Box::new(Literal(Int64Value(3)))));
    }

    #[test]
    fn test_mathematical_division() {
        let opcodes = Compiler::compile_script("n / 3").unwrap();
        assert_eq!(opcodes,
                   Divide(Box::new(Variable("n".into())), Box::new(Literal(Int64Value(3)))));
    }

    #[test]
    fn test_mathematical_exponent() {
        let opcodes = Compiler::compile_script("5 ** 2").unwrap();
        assert_eq!(opcodes,
                   Pow(Box::new(Literal(Int64Value(5))), Box::new(Literal(Int64Value(2)))));
    }

    #[test]
    fn test_mathematical_exponent_via_symbol() {
        let symbols = vec!["⁰", "¹", "²", "³", "⁴", "⁵", "⁶", "⁷", "⁸", "⁹"];
        let mut num = 0;
        for symbol in symbols {
            let opcodes = Compiler::compile_script(format!("5{}", symbol).as_str()).unwrap();
            assert_eq!(opcodes,
                       Pow(Box::new(Literal(Int64Value(5))), Box::new(Literal(Int64Value(num)))));
            num += 1
        }
    }

    #[test]
    fn test_mathematical_factorial() {
        let opcodes = Compiler::compile_script("5¡").unwrap();
        assert_eq!(opcodes, Factorial(Box::new(Literal(Int64Value(5)))));
    }

    #[test]
    fn test_mathematical_modulus() {
        let opcodes = Compiler::compile_script("n % 4").unwrap();
        assert_eq!(opcodes,
                   Modulo(Box::new(Variable("n".into())), Box::new(Literal(Int64Value(4)))));
    }

    #[test]
    fn test_mathematical_multiplication() {
        let opcodes = Compiler::compile_script("n * 10").unwrap();
        assert_eq!(opcodes,
                   Multiply(Box::new(Variable("n".into())), Box::new(Literal(Int64Value(10)))));
    }

    #[test]
    fn test_mathematical_subtraction() {
        let opcodes = Compiler::compile_script("_ - 7").unwrap();
        assert_eq!(opcodes,
                   Minus(Box::new(Variable("_".into())), Box::new(Literal(Int64Value(7)))));
    }

    #[test]
    fn test_must_succeed() {
        let code = Compiler::compile_script(r#"
        [+] eval "5 + 7"
        "#).unwrap();
        assert_eq!(code, MustAck(Box::new(
            Eval(Box::new(Literal(StringValue("5 + 7".to_string()))))
        )));
    }

    #[test]
    fn test_must_not_succeed() {
        let code = Compiler::compile_script(r#"
        [-] eval "7 / 0"
        "#).unwrap();
        assert_eq!(code, MustNotAck(Box::new(
            Eval(Box::new(Literal(StringValue("7 / 0".to_string()))))
        )))
    }

    #[test]
    fn test_must_ignore_failure() {
        let code = Compiler::compile_script(r#"
        [~] eval "7 / 0"
        "#).unwrap();
        assert_eq!(code, MustIgnoreAck(Box::new(
            Eval(Box::new(Literal(StringValue("7 / 0".to_string()))))
        )))
    }

    #[test]
    fn test_to_byte_code() {
        // compile script to byte code
        let byte_code = Compiler::compile_to_byte_code(r#"
        {w:'abc', x:1.0, y:2, z:[1, 2, 3]}
        "#).unwrap();
        assert_eq!(byte_code, vec![
            E_JSON_LITERAL, 0, 0, 0, 0, 0, 0, 0, 8,
            E_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 1, b'w',
            E_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 3, b'a', b'b', b'c',
            E_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 1, b'x',
            E_LITERAL, T_FLOAT64, 63, 240, 0, 0, 0, 0, 0, 0,
            E_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 1, b'y',
            E_LITERAL, T_INT64, 0, 0, 0, 0, 0, 0, 0, 2,
            E_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 1, b'z',
            E_ARRAY_LIT, 0, 0, 0, 0, 0, 0, 0, 3,
            E_LITERAL, T_INT64, 0, 0, 0, 0, 0, 0, 0, 1,
            E_LITERAL, T_INT64, 0, 0, 0, 0, 0, 0, 0, 2,
            E_LITERAL, T_INT64, 0, 0, 0, 0, 0, 0, 0, 3,
        ]);
    }

    #[test]
    fn test_decompile_from_byte_code() {
        let byte_code = vec![
            E_JSON_LITERAL, 0, 0, 0, 0, 0, 0, 0, 8,
            E_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 1, b'w',
            E_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 3, b'a', b'b', b'c',
            E_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 1, b'x',
            E_LITERAL, T_FLOAT64, 63, 240, 0, 0, 0, 0, 0, 0,
            E_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 1, b'y',
            E_LITERAL, T_INT64, 0, 0, 0, 0, 0, 0, 0, 2,
            E_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 1, b'z',
            E_ARRAY_LIT, 0, 0, 0, 0, 0, 0, 0, 3,
            E_LITERAL, T_INT64, 0, 0, 0, 0, 0, 0, 0, 1,
            E_LITERAL, T_INT64, 0, 0, 0, 0, 0, 0, 0, 2,
            E_LITERAL, T_INT64, 0, 0, 0, 0, 0, 0, 0, 3,
        ];
        // decompile byte code to model code
        let code = Compiler::decompile_from_byte_code(&byte_code).unwrap();
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
        let mut compiler = Compiler::new();
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
        let mut compiler = Compiler::new();
        let ts = TokenSlice::from_string("(abc, 123, 'Hello')");
        let (items, _) = compiler.expect_arguments(ts).unwrap();
        assert_eq!(items, vec![Variable("abc".into()), Literal(Int64Value(123)), Literal(StringValue("Hello".into()))])
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
        assert_eq!(items, Some(vec![Variable("abc".into()), Literal(Float64Value(123.)), Literal(StringValue("Hello".into()))]))
    }

    #[test]
    fn test_order_of_operations_1() {
        let opcodes = Compiler::compile_script("2 + (4 * 3)").unwrap();
        assert_eq!(opcodes,
                   Plus(Box::new(Literal(Int64Value(2))),
                        Box::new(Multiply(Box::new(Literal(Int64Value(4))),
                                          Box::new(Literal(Int64Value(3))))),
                   ));
    }

    #[test]
    fn test_order_of_operations_2() {
        let opcodes = Compiler::compile_script("(4.0 / 3.0) + (4 * 3)").unwrap();
        assert_eq!(opcodes,
                   Plus(
                       Box::new(Divide(Box::new(Literal(Float64Value(4.0))), Box::new(Literal(Float64Value(3.0))))),
                       Box::new(Multiply(Box::new(Literal(Int64Value(4))), Box::new(Literal(Int64Value(3))))),
                   ));
    }

    #[ignore]
    #[test]
    fn test_order_of_operations_3() {
        let opcodes = Compiler::compile_script("2 - 4 * 3").unwrap();
        assert_eq!(opcodes,
                   Minus(Box::new(Literal(Float64Value(2.))),
                         Box::new(Multiply(Box::new(Literal(Float64Value(4.))),
                                           Box::new(Literal(Float64Value(3.))))),
                   ));
    }

    #[test]
    fn test_delete() {
        let opcodes = Compiler::compile_script(r#"
        delete from stocks
        "#).unwrap();
        assert_eq!(opcodes, Mutate(Mutation::Delete {
            path: Box::new(Variable("stocks".into())),
            condition: None,
            limit: None,
        }))
    }

    #[test]
    fn test_delete_where_limit() {
        let opcodes = Compiler::compile_script(r#"
        delete from stocks
        where last_sale >= 1.0
        limit 100
        "#).unwrap();
        assert_eq!(opcodes, Mutate(Mutation::Delete {
            path: Box::new(Variable("stocks".into())),
            condition: Some(
                Box::new(GreaterOrEqual(
                    Box::new(Variable("last_sale".into())),
                    Box::new(Literal(Float64Value(1.0))),
                ))
            ),
            limit: Some(Box::new(Literal(Int64Value(100)))),
        }))
    }

    #[test]
    fn test_undelete() {
        let opcodes = Compiler::compile_script(r#"
        undelete from stocks
        "#).unwrap();
        assert_eq!(opcodes, Mutate(Mutation::Undelete {
            path: Box::new(Variable("stocks".into())),
            condition: None,
            limit: None,
        }))
    }

    #[test]
    fn test_undelete_where_limit() {
        let opcodes = Compiler::compile_script(r#"
        undelete from stocks
        where last_sale >= 1.0
        limit 100
        "#).unwrap();
        assert_eq!(opcodes, Mutate(Mutation::Undelete {
            path: Box::new(Variable("stocks".into())),
            condition: Some(
                Box::new(GreaterOrEqual(
                    Box::new(Variable("last_sale".into())),
                    Box::new(Literal(Float64Value(1.0))),
                ))
            ),
            limit: Some(Box::new(Literal(Int64Value(100)))),
        }))
    }

    #[test]
    fn test_from() {
        let opcodes = Compiler::compile_script("from stocks").unwrap();
        assert_eq!(opcodes, From(Box::new(Variable("stocks".into()))));
    }

    #[test]
    fn test_from_where_limit() {
        let opcodes = Compiler::compile_script(r#"
        from stocks where last_sale >= 1.0 limit 20
        "#).unwrap();
        assert_eq!(opcodes,
                   Inquire(Queryable::Limit {
                       from: Box::new(
                           Inquire(Queryable::Where {
                               from: Box::new(From(Box::new(Variable("stocks".into())))),
                               condition: Box::new(GreaterOrEqual(
                                   Box::new(Variable("last_sale".into())),
                                   Box::new(Literal(Float64Value(1.0))),
                               )),
                           })),
                       limit: Box::new(Literal(Int64Value(20))),
                   })
        );
    }

    #[test]
    fn test_write_json_into_namespace() {
        let opcodes = Compiler::compile_script(r#"
        [{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
         { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
         { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] |> ns("interpreter.into.stocks")
        "#).unwrap();
        assert_eq!(opcodes,
                   Mutate(IntoNs(
                       Box::new(ArrayLiteral(vec![
                           JSONLiteral(vec![
                               ("symbol".into(), Literal(StringValue("ABC".into()))),
                               ("exchange".into(), Literal(StringValue("AMEX".into()))),
                               ("last_sale".into(), Literal(Float64Value(12.49))),
                           ]),
                           JSONLiteral(vec![
                               ("symbol".into(), Literal(StringValue("BOOM".into()))),
                               ("exchange".into(), Literal(StringValue("NYSE".into()))),
                               ("last_sale".into(), Literal(Float64Value(56.88))),
                           ]),
                           JSONLiteral(vec![
                               ("symbol".into(), Literal(StringValue("JET".into()))),
                               ("exchange".into(), Literal(StringValue("NASDAQ".into()))),
                               ("last_sale".into(), Literal(Float64Value(32.12))),
                           ]),
                       ])),
                       Box::new(Ns(Box::new(Literal(StringValue("interpreter.into.stocks".into()))))),
                   ))
        );
    }

    #[test]
    fn test_append_from_variable() {
        let opcodes = Compiler::compile_script(r#"
        append ns("compiler.append.stocks") from stocks
        "#).unwrap();
        assert_eq!(opcodes, Mutate(Mutation::Append {
            path: Box::new(Ns(Box::new(Literal(StringValue("compiler.append.stocks".to_string()))))),
            source: Box::new(From(Box::new(Variable("stocks".into())))),
        }))
    }

    #[test]
    fn test_append_from_json_literal() {
        let opcodes = Compiler::compile_script(r#"
        append ns("compiler.append2.stocks")
        from { symbol: "ABC", exchange: "NYSE", last_sale: 0.1008 }
        "#).unwrap();
        assert_eq!(opcodes, Mutate(Mutation::Append {
            path: Box::new(Ns(Box::new(Literal(StringValue("compiler.append2.stocks".to_string()))))),
            source: Box::new(From(Box::new(JSONLiteral(vec![
                ("symbol".into(), Literal(StringValue("ABC".into()))),
                ("exchange".into(), Literal(StringValue("NYSE".into()))),
                ("last_sale".into(), Literal(Float64Value(0.1008))),
            ])))),
        }))
    }

    #[test]
    fn test_append_from_json_array() {
        let opcodes = Compiler::compile_script(r#"
        append ns("compiler.into.stocks")
        from [
            { symbol: "ABC", exchange: "NYSE", last_sale: 11.1234 },
            { symbol: "DOG", exchange: "NASDAQ", last_sale: 0.1008 },
            { symbol: "SHARK", exchange: "AMEX", last_sale: 52.08 }
        ]
        "#).unwrap();
        assert_eq!(opcodes, Mutate(Mutation::Append {
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
        }))
    }

    #[test]
    fn test_ns() {
        let code = Compiler::compile_script(r#"
        ns("securities.etf.stocks")
        "#).unwrap();
        assert_eq!(code, Ns(Box::new(Literal(StringValue("securities.etf.stocks".to_string())))))
    }

    #[test]
    fn test_overwrite() {
        let opcodes = Compiler::compile_script(r#"
        overwrite stocks
        via {symbol: "ABC", exchange: "NYSE", last_sale: 0.2308}
        where symbol == "ABCQ"
        limit 5
        "#).unwrap();
        assert_eq!(opcodes, Mutate(Mutation::Overwrite {
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
        }))
    }

    #[test]
    fn test_reverse_from_variable() {
        let code = Compiler::compile_script(r#"
        reverse from stocks
        "#).unwrap();
        assert_eq!(code, Inquire(Queryable::Reverse(Box::new(From(Box::new(Variable("stocks".to_string())))))))
    }

    #[test]
    fn test_select_from_variable() {
        let opcodes = Compiler::compile_script(r#"
        select symbol, exchange, last_sale from stocks
        "#).unwrap();
        assert_eq!(opcodes, Inquire(Queryable::Select {
            fields: vec![Variable("symbol".into()), Variable("exchange".into()), Variable("last_sale".into())],
            from: Some(Box::new(Variable("stocks".into()))),
            condition: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
        }))
    }

    #[test]
    fn test_select_from_where() {
        let opcodes = Compiler::compile_script(r#"
        select symbol, exchange, last_sale from stocks
        where last_sale >= 1.0
        "#).unwrap();
        assert_eq!(opcodes, Inquire(Queryable::Select {
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
        }))
    }

    #[test]
    fn test_select_from_where_limit() {
        let opcodes = Compiler::compile_script(r#"
        select symbol, exchange, last_sale from stocks
        where last_sale <= 1.0
        limit 5
        "#).unwrap();
        assert_eq!(opcodes, Inquire(Queryable::Select {
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
        }))
    }

    #[test]
    fn test_select_from_where_order_by_limit() {
        let opcode = Compiler::compile_script(r#"
        select symbol, exchange, last_sale from stocks
        where last_sale < 1.0
        order by symbol
        limit 5
        "#).unwrap();
        assert_eq!(opcode, Inquire(Queryable::Select {
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
        }))
    }

    #[test]
    fn test_system_call() {
        let model = Compiler::compile_script(r#"
        syscall("cat", "LICENSE")
        "#).unwrap();
        assert_eq!(model, SystemCall(vec![
            Literal(StringValue("cat".into())),
            Literal(StringValue("LICENSE".into())),
        ]));
    }

    #[test]
    fn test_update() {
        let opcodes = Compiler::compile_script(r#"
        update stocks
        via { last_sale: 0.1111 }
        where symbol == "ABC"
        limit 10
        "#).unwrap();
        assert_eq!(opcodes, Mutate(Mutation::Update {
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
        }))
    }

    #[test]
    fn test_while_loop() {
        let opcodes = Compiler::compile_script(r#"
            while (x < 5) x := x + 1
        "#).unwrap();
        assert_eq!(opcodes, While {
            condition: Box::new(LessThan(
                Box::new(Variable("x".into())),
                Box::new(Literal(Int64Value(5))),
            )),
            code: Box::new(SetVariable("x".into(), Box::new(Plus(
                Box::new(Variable("x".into())),
                Box::new(Literal(Int64Value(1)))),
            ))),
        });
    }

    #[test]
    fn test_while_loop_fix() {
        let opcodes = Compiler::compile_script(r#"
            x := 0
            while x < 7 x := x + 1
            x
        "#).unwrap();
        assert_eq!(opcodes, CodeBlock(vec![
            SetVariable("x".into(), Box::new(Literal(Int64Value(0)))),
            While {
                condition: Box::new(LessThan(
                    Box::new(Variable("x".into())),
                    Box::new(Literal(Int64Value(7))),
                )),
                code: Box::new(SetVariable("x".into(), Box::new(Plus(
                    Box::new(Variable("x".into())),
                    Box::new(Literal(Int64Value(1)))),
                ))),
            },
            Variable("x".into()),
        ]));
    }
}