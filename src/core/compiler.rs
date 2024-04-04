////////////////////////////////////////////////////////////////////
// compiler module
////////////////////////////////////////////////////////////////////

use std::io;

use log::info;
use serde::{Deserialize, Serialize};

use shared_lib::fail;

use crate::expression::{Expression, FALSE, NULL, TRUE};
use crate::expression::Expression::*;
use crate::token_slice::TokenSlice;
use crate::tokens::Token::{Atom, Backticks, DoubleQuoted, Numeric, Operator, SingleQuoted};
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{Int64Value, StringValue, Undefined};

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
    pub fn compile(source_code: &str) -> io::Result<Vec<Expression>> {
        let mut ctx = Compiler {
            stack: vec![],
        };
        let (code, _) = ctx.compile_all(TokenSlice::from_string(source_code))?;
        Ok(code)
    }

    ////////////////////////////////////////////////////////////////
    // instance methods
    ////////////////////////////////////////////////////////////////

    /// compiles the [TokenSlice] into a [Vec<Expression>]
    pub fn compile_all(&mut self, ts: TokenSlice) -> io::Result<(Vec<Expression>, TokenSlice)> {
        fn push(compiler: &mut Compiler, ts0: TokenSlice) -> io::Result<TokenSlice> {
            let start = ts0.get_position();
            let (expr, ts1) = compiler.compile_expr(ts0)?;
            // ensure we actually moved the cursor
            assert!(ts1.get_position() > start);
            compiler.push(expr);
            Ok(ts1)
        }

        // consume the entire iterator
        let mut a_ts = ts;
        while a_ts.has_more() {
            a_ts = push(self, a_ts)?;
        }
        Ok((self.get_stack(), a_ts))
    }

    /// compiles the [TokenSlice] into an [Expression]
    pub fn compile_expr(&mut self, ts: TokenSlice) -> io::Result<(Expression, TokenSlice)> {
        let (expr, ts) = match ts.next() {
            (Some(Operator { text, precedence, .. }), ts) =>
                self.compile_operator(ts, text, precedence),
            (Some(Backticks { text, .. }), ts) =>
                Ok((Variable(text.into()), ts)),
            (Some(Atom { text, .. }), ts) =>
                self.compile_atom(ts, text),
            (Some(DoubleQuoted { text, .. } |
                  SingleQuoted { text, .. }), ts) =>
                Ok((Literal(StringValue(text.into())), ts)),
            (Some(Numeric { text, .. }), ts) =>
                Ok((Literal(TypedValue::from_numeric(text.as_str())?), ts)),
            (None, _) => fail("Unexpected end of input")
        }?;

        // handle postfix operator?
        match ts.next() {
            (Some(Operator { is_barrier, .. }), _) if !is_barrier => {
                self.push(expr);
                self.compile_expr(ts)
            }
            _ => Ok((expr, ts))
        }
    }

    /// compiles the [TokenSlice] into a leading single-parameter [Expression] (e.g. !true)
    pub fn compile_expr_1a(&mut self,
                           ts: TokenSlice,
                           f: fn(Box<Expression>) -> Expression) -> io::Result<(Expression, TokenSlice)> {
        let (expr, ts_z) = self.compile_expr(ts)?;
        Ok((f(Box::new(expr)), ts_z))
    }

    /// compiles the [TokenSlice] into a two-parameter [Expression]
    pub fn compile_expr_1b(&mut self, ts: TokenSlice,
                           f: fn(Box<Expression>) -> Expression) -> io::Result<(Expression, TokenSlice)> {
        match self.pop() {
            None => fail(format!("expected an expression near {:?}", ts)),
            Some(expr0) => Ok((f(Box::new(expr0)), ts))
        }
    }

    /// compiles the [TokenSlice] into a two-parameter [Expression]
    pub fn compile_expr_2a(&mut self,
                           ts: TokenSlice,
                           expr0: Expression,
                           f: fn(Box<Expression>, Box<Expression>) -> Expression) -> io::Result<(Expression, TokenSlice)> {
        let (expr1, ts_z) = self.compile_expr(ts)?;
        Ok((f(Box::new(expr0), Box::new(expr1)), ts_z))
    }

    /// compiles the [TokenSlice] into a two-parameter [Expression]
    pub fn compile_expr_2b(&mut self, ts: TokenSlice,
                           expr1: Expression,
                           f: fn(Box<Expression>, Box<Expression>) -> Expression) -> io::Result<(Expression, TokenSlice)> {
        match self.pop() {
            None => fail(format!("expected an expression near {:?}", ts)),
            Some(expr0) => Ok((f(Box::new(expr0), Box::new(expr1)), ts))
        }
    }

    /// compiles the [TokenSlice] into a name-value parameter [Expression]
    pub fn compile_expr_2nv(&mut self,
                            ts: TokenSlice,
                            expr0: Expression,
                            f: fn(String, Box<Expression>) -> Expression) -> io::Result<(Expression, TokenSlice)> {
        if let Field(name) | Variable(name) = expr0 {
            let (expr1, ts_z) = self.compile_expr(ts)?;
            Ok((f(name, Box::new(expr1)), ts_z))
        } else { fail(format!("an identifier name was expected near {:?}", ts)) }
    }

    /// compiles the [TokenSlice] into an operator-based [Expression]
    pub fn compile_atom(&mut self, ts: TokenSlice, text: String) -> io::Result<(Expression, TokenSlice)> {
        match text.as_str() {
            "false" => Ok((FALSE, ts)),
            "null" => Ok((NULL, ts)),
            "true" => Ok((TRUE, ts)),
            "undefined" => Ok((Literal(Undefined), ts)),
            name => self.compile_statement(name, ts.clone()),
        }
    }

    /// compiles the [TokenSlice] into an operator-based [Expression]
    pub fn compile_operator(&mut self,
                            ts: TokenSlice,
                            symbol: String,
                            _precedence: usize) -> io::Result<(Expression, TokenSlice)> {
        info!("compile_operator:symbol {:?}", symbol);
        match symbol.as_str() {
            "²" => self.compile_expr_2b(ts, Literal(Int64Value(2)), Pow),
            "³" => self.compile_expr_2b(ts, Literal(Int64Value(3)), Pow),
            "!" => self.compile_expr_1a(ts, Not),
            "¡" => self.compile_expr_1b(ts, Factorial),
            "(" => self.handle_parentheses(ts),
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

    pub fn compile_statement(&mut self, command: &str, ts: TokenSlice) -> io::Result<(Expression, TokenSlice)> {
        match command {
            "delete" => self.compile_delete(ts),
            "select" => self.compile_select(ts),
            name => Ok((Variable(name.into()), ts))
        }
    }

    fn compile_delete(&mut self, ts: TokenSlice) -> io::Result<(Expression, TokenSlice)> {
        let (_from, ts) = self.next_keyword_expr("from", ts)?;
        let from = _from.expect("Expected keyword 'from'");
        let (condition, ts) = self.next_keyword_expr("where", ts)?;
        let (limit, ts) = self.next_keyword_expr("limit", ts)?;
        Ok((Delete {
            table: Box::new(from),
            condition: condition.map(Box::new),
            limit: limit.map(Box::new),
        }, ts))
    }

    fn compile_select(&mut self, ts: TokenSlice) -> io::Result<(Expression, TokenSlice)> {
        let (_fields, ts) = self.next_expression_list(ts)?;
        let fields = _fields.expect("At least one field is required");
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

    pub fn get_stack(&self) -> Vec<Expression> {
        self.stack.iter().map(|x| x.clone()).collect()
    }

    fn handle_parentheses(&mut self, ts: TokenSlice) -> io::Result<(Expression, TokenSlice)> {
        let (tokens, ts_z) = ts.scan_until(|t| t.contains(")"));
        let innards = TokenSlice::new(tokens[0..(tokens.len() - 1)].to_vec());
        let (expression, _) = self.compile_all(innards)?;
        let result = match expression.as_slice() {
            [] => Literal(Undefined),
            [one] => one.clone(),
            many => Tuple(many.to_vec())
        };
        self.pop();
        Ok((result, ts_z.skip())) // skip the trailing parenthesis
    }

    fn next_expression_list(&mut self, ts: TokenSlice) -> io::Result<(Option<Vec<Expression>>, TokenSlice)> {
        let mut items = vec![];
        // get the first item
        let (item, mut ts) = self.compile_expr(ts)?;
        items.push(item);
        // get the others
        while ts.is(",") {
            let (item, _ts) = self.compile_expr(ts.skip())?;
            ts = _ts;
            items.push(item);
        }
        Ok((Some(items), ts))
    }

    // Returns the option of an expression based the next token matching the specified keyword
    fn next_keyword_expr(&mut self, keyword: &str, ts: TokenSlice) -> io::Result<(Option<Expression>, TokenSlice)> {
        if ts.isnt(keyword) { Ok((None, ts)) } else {
            let (expr, ts) = self.compile_expr(ts.skip())?;
            Ok((Some(expr), ts))
        }
    }

    // Returns the option of a list of expressions based the next token matching the specified keywords
    fn next_keyword_expr_list(&mut self, keyword0: &str, keyword1: &str, ts: TokenSlice) -> io::Result<(Option<Vec<Expression>>, TokenSlice)> {
        if ts.isnt(keyword0) || ts.skip().isnt(keyword1) { Ok((None, ts)) } else {
            self.next_expression_list(ts.skip().skip())
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

// Unit tests
#[cfg(test)]
mod tests {
    use crate::typed_values::TypedValue::{Float64Value, Int64Value};

    use super::*;

    #[test]
    fn test_literal_value() {
        assert_eq!(Compiler::compile("1_234_567_890").unwrap(),
                   vec![Literal(Int64Value(1_234_567_890))])
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

    #[ignore]
    #[test]
    fn test_sql_insert() {
        let opcodes = Compiler::compile(r#"
        insert into stocks (symbol, exchange, last_sale)
        values ("ABC", "NYSE", 0.1008)
        "#).unwrap();
        assert_eq!(opcodes, vec![Insert {
            table: Box::new(Variable("stocks".into())),
            fields: vec![Variable("symbol".into()), Variable("exchange".into()), Variable("last_sale".into())],
            values: vec![Literal(StringValue("ABC".into())), Literal(StringValue("NYSE".into())), Literal(Float64Value(0.1008))],
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