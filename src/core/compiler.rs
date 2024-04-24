////////////////////////////////////////////////////////////////////
// compiler module
////////////////////////////////////////////////////////////////////

use log::info;
use serde::{Deserialize, Serialize};

use shared_lib::fail;

use crate::expression::{Expression, FALSE, NULL, TRUE, UNDEFINED};
use crate::expression::Expression::*;
use crate::token_slice::TokenSlice;
use crate::tokens::Token::{Atom, Backticks, DoubleQuoted, Numeric, Operator, SingleQuoted};
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{Int64Value, StringValue};

/// Represents the compiler state
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
    pub fn compile(source_code: &str) -> std::io::Result<Vec<Expression>> {
        let mut compiler = Compiler::new();
        let (code, _) = compiler.compile_all(TokenSlice::from_string(source_code))?;
        Ok(code)
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

    /// compiles the [TokenSlice] into a [Vec<Expression>]
    pub fn compile_all(&mut self, ts: TokenSlice) -> std::io::Result<(Vec<Expression>, TokenSlice)> {
        fn push(compiler: &mut Compiler, ts: TokenSlice) -> std::io::Result<TokenSlice> {
            let start = ts.get_position();
            let (expr, ts) = compiler.next_expression(ts)?;
            // ensure we actually moved the cursor
            assert!(ts.get_position() > start);
            compiler.push(expr);
            Ok(ts)
        }

        // consume the entire iterator
        let mut ts = ts;
        while ts.has_more() { ts = push(self, ts)?; }
        Ok((self.get_stack(), ts))
    }

    /// compiles the [TokenSlice] into an [Expression]
    pub fn next_expression(&mut self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        let (expr, ts) = match ts.next() {
            (Some(Operator { text, precedence, .. }), ts) =>
                self.compile_expr_operator(ts, text, precedence),
            (Some(Backticks { text, .. }), ts) =>
                Ok((Variable(text.into()), ts)),
            (Some(Atom { .. }), _) => self.compile_keyword(ts),
            (Some(DoubleQuoted { text, .. } |
                  SingleQuoted { text, .. }), ts) =>
                Ok((Literal(StringValue(text.into())), ts)),
            (Some(Numeric { text, .. }), ts) =>
                Ok((Literal(TypedValue::from_numeric(text.as_str())?), ts)),
            (None, ts) => fail_near("Unexpected end of input", ts)
        }?;

        // handle postfix operator?
        match ts.next() {
            (Some(Operator { is_barrier, .. }), _) if !is_barrier => {
                self.push(expr);
                self.next_expression(ts)
            }
            _ => Ok((expr, ts))
        }
    }

    /// compiles the [TokenSlice] into a leading single-parameter [Expression] (e.g. !true)
    pub fn compile_expr_1a(&mut self,
                           ts: TokenSlice,
                           f: fn(Box<Expression>) -> Expression) -> std::io::Result<(Expression, TokenSlice)> {
        let (expr, ts) = self.next_expression(ts)?;
        Ok((f(Box::new(expr)), ts))
    }

    /// compiles the [TokenSlice] into a two-parameter [Expression]
    pub fn compile_expr_1b(&mut self, ts: TokenSlice,
                           f: fn(Box<Expression>) -> Expression) -> std::io::Result<(Expression, TokenSlice)> {
        match self.pop() {
            None => fail_near("expected an expression", ts),
            Some(expr0) => Ok((f(Box::new(expr0)), ts))
        }
    }

    /// compiles the [TokenSlice] into a two-parameter [Expression]
    pub fn compile_expr_2a(&mut self,
                           ts: TokenSlice,
                           expr0: Expression,
                           f: fn(Box<Expression>, Box<Expression>) -> Expression) -> std::io::Result<(Expression, TokenSlice)> {
        let (expr1, ts) = self.next_expression(ts)?;
        Ok((f(Box::new(expr0), Box::new(expr1)), ts))
    }

    /// compiles the [TokenSlice] into a two-parameter [Expression]
    pub fn compile_expr_2b(&mut self, ts: TokenSlice,
                           expr1: Expression,
                           f: fn(Box<Expression>, Box<Expression>) -> Expression) -> std::io::Result<(Expression, TokenSlice)> {
        match self.pop() {
            None => fail_near("expected an expression", ts),
            Some(expr0) => Ok((f(Box::new(expr0), Box::new(expr1)), ts))
        }
    }

    /// compiles the [TokenSlice] into a name-value parameter [Expression]
    pub fn compile_expr_2nv(&mut self,
                            ts: TokenSlice,
                            expr0: Expression,
                            f: fn(String, Box<Expression>) -> Expression) -> std::io::Result<(Expression, TokenSlice)> {
        if let Variable(name) = expr0 {
            let (expr1, ts) = self.next_expression(ts)?;
            Ok((f(name, Box::new(expr1)), ts))
        } else { fail_near("an identifier name was expected", ts) }
    }

    /// compiles the [TokenSlice] into an operator-based [Expression]
    pub fn compile_expr_operator(&mut self,
                                 ts: TokenSlice,
                                 symbol: String,
                                 _precedence: usize) -> std::io::Result<(Expression, TokenSlice)> {
        fn pow(compiler: &mut Compiler, ts: TokenSlice, n: i64) -> std::io::Result<(Expression, TokenSlice)> {
            compiler.compile_expr_2b(ts, Literal(Int64Value(n)), Pow)
        }
        match symbol.as_str() {
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
            sym =>
                if let Some(op0) = self.pop() {
                    match sym {
                        "&&" => self.compile_expr_2a(ts, op0, And),
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
                        ":=" => self.compile_expr_2nv(ts, op0, SetVariable),
                        "<<" => self.compile_expr_2a(ts, op0, ShiftLeft),
                        ">>" => self.compile_expr_2a(ts, op0, ShiftRight),
                        "^" => self.compile_expr_2a(ts, op0, Xor),
                        unknown => fail(format!("Invalid operator '{}'", unknown))
                    }
                } else { fail(format!("Illegal start of expression '{}'", symbol)) }
        }
    }

    pub fn compile_keyword(&mut self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        if let (Some(t), ts) = ts.next() {
            match t.get_raw_value().as_str() {
                "delete" => self.compile_keyword_delete(ts),
                "false" => Ok((FALSE, ts)),
                "from" => {
                    let (from, ts) = self.compile_keyword_from(ts)?;
                    self.compile_keyword_queryables(from, ts)
                }
                "into" => self.compile_keyword_into(ts),
                "limit" => fail_near("`from` is expected before `limit`: from stocks limit 5", ts),
                "ns" => self.compile_keyword_ns(ts),
                "null" => Ok((NULL, ts)),
                "overwrite" => self.compile_keyword_overwrite(ts),
                "select" => self.compile_keyword_select(ts),
                "update" => self.compile_keyword_update(ts),
                "true" => Ok((TRUE, ts)),
                "undefined" => Ok((UNDEFINED, ts)),
                "where" => fail_near("`from` is expected before `where`: from stocks where last_sale < 1.0", ts),
                name => Ok((Variable(name.to_string()), ts))
            }
        } else { fail("Unexpected end of input") }
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
        Ok((Delete { table: Box::new(from), condition: condition.map(Box::new), limit: limit.map(Box::new) }, ts))
    }

    /// SQL From clause. ex: from stocks
    fn compile_keyword_from(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let (table, ts) = self.next_expression(ts)?;
        Ok((From(Box::new(table)), ts))
    }

    /// Appends a new row to a table
    /// ex: into stocks (symbol, exchange, last_sale) \[("ABC", "NYSE", 0.1008)\]
    fn compile_keyword_into(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let (table, ts) = self.next_expression(ts)?;
        println!("table {:?}", table);
        let (fields, ts) = self.expect_argument_list(ts)?;
        println!("fields {:?}", fields);
        let ts = ts.expect("values")?;
        let (values, ts) = self.expect_argument_list(ts)?;
        println!("values {:?}", values);
        Ok((InsertInto { table: Box::new(table), fields, values }, ts))
    }

    /// SQL Limit clause. ex: from stocks where last_sale > 1.00 limit 5
    fn compile_keyword_limit(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let table = self.pop().unwrap_or(fail("A queryable host was expected")?);
        let (condition, ts) = self.next_expression(ts)?;
        Ok((Where { from: Box::new(table), condition: Box::new(condition) }, ts))
    }

    fn compile_keyword_ns(&mut self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        let (path, ts) = self.next_expression(ts)?;
        Ok((Ns(Box::new(path)), ts))
    }

    /// compiles an OVERWRITE statement:
    /// ex: overwrite stocks (symbol, exchange, last_sale) \[("ABC", "NYSE", 6.54)\]
    ///     where symbol == "ABC"
    fn compile_keyword_overwrite(
        &mut self,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        let (table, ts) = self.next_expression(ts)?;
        let (fields, ts) = self.expect_argument_list(ts)?;
        let (values, ts) = self.next_keyword_value_list("values", ts)?;
        let values = values.expect("keyword 'values' expected");
        let (condition, ts) = self.next_keyword_expr("where", ts)?;
        let (limit, ts) = self.next_keyword_expr("limit", ts)?;
        Ok((Overwrite { table: Box::new(table), fields, values, condition: condition.map(Box::new), limit: limit.map(Box::new) }, ts))
    }

    fn compile_keyword_queryables(
        &mut self,
        host: Expression,
        ts: TokenSlice,
    ) -> std::io::Result<(Expression, TokenSlice)> {
        match ts.clone() {
            t if t.is("limit") => {
                let (expr, ts) = self.next_expression(ts.skip())?;
                self.compile_keyword_queryables(Limit { from: Box::new(host), limit: Box::new(expr) }, ts)
            }
            t if t.is("where") => {
                let (expr, ts) = self.next_expression(ts.skip())?;
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
        let (table, ts) = self.next_expression(ts)?;
        let (fields, ts) = self.expect_argument_list(ts)?;
        let (values, ts) = self.next_keyword_value_list("values", ts)?;
        let values = values.expect("keyword 'values' expected");
        let (condition, ts) = self.next_keyword_expr("where", ts)?;
        let (limit, ts) = self.next_keyword_expr("limit", ts)?;
        Ok((Update { table: Box::new(table), fields, values, condition: condition.map(Box::new), limit: limit.map(Box::new) }, ts))
    }

    /// SQL Where clause. ex: from stocks where last_sale > 1.00
    fn compile_keyword_where(&mut self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        let table = self.pop().unwrap_or(fail("A queryable host was expected")?);
        let (condition, ts) = self.next_expression(ts)?;
        Ok((Where { from: Box::new(table), condition: Box::new(condition) }, ts))
    }

    fn error_expected_operator<A>(&self, symbol: &str) -> std::io::Result<(A, TokenSlice)> {
        fail(format!(r#"operator '{}' was expected; e.g.: ns("securities", "etf", "stocks")"#, symbol))
    }

    /// parse an argument list from tge [TokenSlice] (e.g. Vec('(', 'x', ',', 'y', ',', 'z', ')')
    fn expect_argument_list(&mut self, ts: TokenSlice) -> std::io::Result<(Vec<Expression>, TokenSlice)> {
        // parse: ("abc", "123", ..)
        let ts = ts.expect("(")?;
        let (tokens, ts) = ts.scan_until(|t| t.contains(")"));
        let mut items = vec![];
        let mut innards = TokenSlice::new(tokens[0..(tokens.len() - 1)].to_vec());
        while innards.has_more() {
            println!("innards {:?}", innards.next().0);
            let (expr, its) = self.next_expression(innards)?;
            items.push(expr);
            innards = if its.has_more() { its.expect(",")? } else { its };
        }

        //self.pop();
        Ok((items, ts.skip()))
    }

    fn expect_parentheses(&mut self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        let (innards, ts) = ts.scan_to(|t| t.get_raw_value() == ")");
        let innards = TokenSlice::new(innards.to_vec());
        let (expression, _) = self.compile_all(innards)?;
        let result = match expression.as_slice() {
            [] => UNDEFINED,
            [one] => one.clone(),
            many => Tuple(many.to_vec())
        };
        self.pop();
        Ok((result, ts.expect(")")?))
    }

    fn expect_square_brackets(&mut self, ts: TokenSlice) -> std::io::Result<(Expression, TokenSlice)> {
        let (innards, ts) = ts.scan_to(|t| t.get_raw_value() == "]");
        let mut innards = TokenSlice::new(innards.to_vec());
        let mut items = vec![];
        while innards.has_more() {
            let (expr, its) = self.next_expression(innards)?;
            items.push(expr);
            innards = if its.has_more() { its.expect(",")? } else { its };
        }
        Ok((Collection(items), ts.expect("]")?))
    }

    pub fn get_stack(&self) -> Vec<Expression> {
        self.stack.iter().map(|x| x.clone()).collect()
    }

    /// parse an expression list from the [TokenSlice] (e.g. ['x', ',', 'y', ',', 'z'])
    fn next_expression_list(&mut self, ts: TokenSlice) -> std::io::Result<(Option<Vec<Expression>>, TokenSlice)> {
        let mut items = vec![];
        // get the first item
        let (item, mut ts) = self.next_expression(ts)?;
        items.push(item);
        // get the others
        while ts.is(",") {
            let (item, _ts) = self.next_expression(ts.skip())?;
            ts = _ts;
            items.push(item);
        }
        Ok((Some(items), ts))
    }

    /// Returns the option of an expression based the next token matching the specified keyword
    fn next_keyword_expr(&mut self, keyword: &str, ts: TokenSlice) -> std::io::Result<(Option<Expression>, TokenSlice)> {
        if ts.isnt(keyword) { Ok((None, ts)) } else {
            let (expr, ts) = self.next_expression(ts.skip())?;
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

pub fn fail_near<A>(message: impl Into<String>, ts: TokenSlice) -> std::io::Result<A> {
    Err(std::io::Error::new(std::io::ErrorKind::Other, format!("{} near {}", message.into(), ts)))
}

pub fn fail_value<A>(message: impl Into<String>, value: &TypedValue) -> std::io::Result<A> {
    Err(std::io::Error::new(std::io::ErrorKind::Other, format!("{} near {}", message.into(), value.unwrap_value())))
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::typed_values::TypedValue::{Float64Value, Int64Value};

    use super::*;

    #[test]
    fn test_array() {
        assert_eq!(Compiler::compile("[1, 4, 2, 8, 5, 7]").unwrap(),
                   vec![Collection(vec![
                       Literal(Int64Value(1)), Literal(Int64Value(4)), Literal(Int64Value(2)),
                       Literal(Int64Value(8)), Literal(Int64Value(5)), Literal(Int64Value(7)),
                   ])])
    }

    #[test]
    fn test_literal_value() {
        assert_eq!(Compiler::compile("1_234_567_890").unwrap(),
                   vec![Literal(Int64Value(1_234_567_890))]);

        assert_eq!(Compiler::compile("1_234_567.890").unwrap(),
                   vec![Literal(Float64Value(1_234_567.890))]);
    }

    #[test]
    fn test_logical_not() {
        assert_eq!(Compiler::compile("!false").unwrap(), vec![Not(Box::new(FALSE))]);
        assert_eq!(Compiler::compile("!true").unwrap(), vec![Not(Box::new(TRUE))]);
    }

    #[test]
    fn test_mathematical_addition() {
        let opcodes = Compiler::compile("n + 3").unwrap();
        assert_eq!(opcodes, vec![
            Plus(Box::new(Variable("n".into())), Box::new(Literal(Int64Value(3))))
        ]);
    }

    #[test]
    fn test_mathematical_division() {
        let opcodes = Compiler::compile("n / 3").unwrap();
        assert_eq!(opcodes, vec![
            Divide(Box::new(Variable("n".into())), Box::new(Literal(Int64Value(3))))
        ]);
    }

    #[test]
    fn test_mathematical_exponent() {
        let opcodes = Compiler::compile("5 ** 2").unwrap();
        assert_eq!(opcodes, vec![
            Pow(Box::new(Literal(Int64Value(5))), Box::new(Literal(Int64Value(2))))
        ]);
    }

    #[test]
    fn test_mathematical_exponent_via_symbol() {
        let symbols = vec!["⁰", "¹", "²", "³", "⁴", "⁵", "⁶", "⁷", "⁸", "⁹"];
        let mut num = 0;
        for symbol in symbols {
            let opcodes = Compiler::compile(format!("5{}", symbol).as_str()).unwrap();
            assert_eq!(opcodes, vec![
                Pow(Box::new(Literal(Int64Value(5))), Box::new(Literal(Int64Value(num))))
            ]);
            num += 1
        }
    }

    #[test]
    fn test_mathematical_factorial() {
        let opcodes = Compiler::compile("5¡").unwrap();
        assert_eq!(opcodes, vec![
            Factorial(Box::new(Literal(Int64Value(5))))
        ]);
    }

    #[test]
    fn test_mathematical_modulus() {
        let opcodes = Compiler::compile("n % 4").unwrap();
        assert_eq!(opcodes, vec![
            Modulo(Box::new(Variable("n".into())), Box::new(Literal(Int64Value(4))))
        ]);
    }

    #[test]
    fn test_mathematical_multiplication() {
        let opcodes = Compiler::compile("n * 10").unwrap();
        assert_eq!(opcodes, vec![
            Multiply(Box::new(Variable("n".into())), Box::new(Literal(Int64Value(10))))
        ]);
    }

    #[test]
    fn test_mathematical_subtraction() {
        let opcodes = Compiler::compile("_ - 7").unwrap();
        assert_eq!(opcodes, vec![
            Minus(Box::new(Variable("_".into())), Box::new(Literal(Int64Value(7))))
        ]);
    }

    #[test]
    fn test_next_argument_list() {
        let mut compiler = Compiler::new();
        let ts = TokenSlice::from_string("(abc, 123, 'Hello')");
        let (items, _) = compiler.expect_argument_list(ts).unwrap();
        assert_eq!(items, vec![Variable("abc".into()), Literal(Int64Value(123)), Literal(StringValue("Hello".into()))])
    }

    #[test]
    fn test_next_expression() {
        let mut compiler = Compiler::new();
        let ts = TokenSlice::from_string("abc");
        let (expr, _) = compiler.next_expression(ts).unwrap();
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
        let opcodes = Compiler::compile("2 + (4 * 3)").unwrap();
        assert_eq!(opcodes, vec![
            Plus(Box::new(Literal(Int64Value(2))),
                 Box::new(Multiply(Box::new(Literal(Int64Value(4))),
                                   Box::new(Literal(Int64Value(3))))))
        ]);
    }

    #[test]
    fn test_order_of_operations_2() {
        let opcodes = Compiler::compile("(4.0 / 3.0) + (4 * 3)").unwrap();
        assert_eq!(opcodes, vec![
            Plus(
                Box::new(Divide(Box::new(Literal(Float64Value(4.0))), Box::new(Literal(Float64Value(3.0))))),
                Box::new(Multiply(Box::new(Literal(Int64Value(4))), Box::new(Literal(Int64Value(3))))),
            )
        ]);
    }

    #[ignore]
    #[test]
    fn test_order_of_operations_3() {
        let opcodes = Compiler::compile("2 - 4 * 3").unwrap();
        assert_eq!(opcodes, vec![
            Minus(Box::new(Literal(Float64Value(2.))),
                  Box::new(Multiply(Box::new(Literal(Float64Value(4.))),
                                    Box::new(Literal(Float64Value(3.))))))
        ]);
    }

    #[test]
    fn test_sql_delete() {
        let opcodes = Compiler::compile(r#"
        delete from stocks
        where last_sale >= 1.0
        limit 100
        "#).unwrap();
        assert_eq!(opcodes, vec![Delete {
            table: Box::new(Variable("stocks".into())),
            condition: Some(
                Box::new(GreaterOrEqual(
                    Box::new(Variable("last_sale".into())),
                    Box::new(Literal(Float64Value(1.0))),
                ))
            ),
            limit: Some(Box::new(Literal(Int64Value(100)))),
        }])
    }

    #[test]
    fn test_sql_from() {
        let opcodes = Compiler::compile("from stocks").unwrap();
        assert_eq!(opcodes, vec![
            From(Box::new(Variable("stocks".into())))
        ]);
    }

    #[test]
    fn test_sql_from_where_limit() {
        let opcodes = Compiler::compile(r#"
        from stocks where last_sale >= 1.0 limit 20
        "#).unwrap();
        assert_eq!(opcodes, vec![
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
        ]);
    }

    #[ignore]
    #[test]
    fn test_sql_into() {
        let opcodes = Compiler::compile(r#"
        select symbol, exchange, last_sale
        from [("ABC", "NYSE", 0.1008)]
        into stocks
        "#).unwrap();
        assert_eq!(opcodes, vec![InsertInto {
            table: Box::new(Variable("stocks".into())),
            fields: vec![Variable("symbol".into()), Variable("exchange".into()), Variable("last_sale".into())],
            values: vec![Literal(StringValue("ABC".into())), Literal(StringValue("NYSE".into())), Literal(Float64Value(0.1008))],
        }])
    }

    #[test]
    fn test_sql_ns() {
        let opcodes = Compiler::compile(r#"ns("securities.etf.stocks")"#).unwrap();
        assert_eq!(opcodes, vec![
            Ns(Box::new(Literal(StringValue("securities.etf.stocks".to_string()))))
        ])
    }

    #[ignore]
    #[test]
    fn test_sql_overwrite() {
        let opcodes = Compiler::compile(r#"
        overwrite into stocks (symbol, exchange, last_sale) [("ABC", "NYSE", 0.1008)]
        where symbol == "ABC"
        limit 5
        "#).unwrap();
        assert_eq!(opcodes, vec![Overwrite {
            table: Box::new(Variable("stocks".into())),
            fields: vec![Variable("symbol".into()), Variable("exchange".into()), Variable("last_sale".into())],
            values: vec![Literal(StringValue("ABC".into())), Literal(StringValue("NYSE".into())), Literal(Float64Value(0.1008))],
            condition: Some(
                Box::new(Equal(
                    Box::new(Variable("symbol".into())),
                    Box::new(Literal(StringValue("ABC".into()))),
                ))),
            limit: Some(Box::new(Literal(Int64Value(5)))),
        }])
    }

    #[test]
    fn test_sql_select() {
        let opcodes = Compiler::compile(r#"
        select symbol, exchange, last_sale from stocks
        where last_sale < 1.0
        order by symbol
        limit 5
        "#).unwrap();
        assert_eq!(opcodes, vec![Select {
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
        }])
    }

    #[ignore]
    #[test]
    fn test_sql_update() {
        let opcodes = Compiler::compile(r#"
        update stocks
        set last_sale = 0.1111
        where symbol == "ABC"
        limit 10
        "#).unwrap();
        assert_eq!(opcodes, vec![Update {
            table: Box::new(Variable("stocks".into())),
            fields: vec![Variable("symbol".into())],
            values: vec![Literal(StringValue("ABC".into()))],
            condition: Some(
                Box::new(LessThan(
                    Box::new(Variable("last_sale".into())),
                    Box::new(Literal(Float64Value(0.1111))),
                ))
            ),
            limit: Some(Box::new(Literal(Int64Value(10)))),
        }])
    }
}