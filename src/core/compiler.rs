////////////////////////////////////////////////////////////////////
// compiler module
////////////////////////////////////////////////////////////////////

use std::fmt::format;
use std::io;

use log::info;
use serde::{Deserialize, Serialize};

use crate::cnv_error;
use crate::error_mgmt::fail;
use crate::expression::Expression;
use crate::expression::Expression::*;
use crate::token_slice::TokenSlice;
use crate::tokens::Token::{Atom, Backticks, DoubleQuoted, Numeric, Operator, SingleQuoted, Symbol};
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{Boolean, Float64Value, Null, StringValue, Undefined};

/// Represents the compiler state
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
        match ts.next() {
            (Some(Operator { text, precedence, .. }), ts) =>
                self.compile_operator(ts, text, precedence),
            (Some(Backticks { text, .. }), ts) =>
                Ok((Variable(text.into()), ts)),
            (Some(Atom { text, .. }), ts) =>
                self.compile_alpha(ts, text),
            (Some(DoubleQuoted { text, .. } |
                  SingleQuoted { text, .. }), ts) =>
                Ok((Literal(StringValue(text.into())), ts)),
            (Some(Numeric { text, .. }), ts) =>
                Ok((Literal(TypedValue::from_numeric(text.as_str())?), ts)),
            (Some(Symbol { text, .. }), ts) =>
                Ok((Variable(text.into()), ts)),
            (None, _) => fail("Unexpected end of input")
        }
    }

    /// compiles the [TokenSlice] into a leading single-parameter [Expression] (e.g. !true)
    pub fn compile_expr_1(&mut self,
                          ts: TokenSlice,
                          f: fn(Box<Expression>) -> Expression) -> io::Result<(Expression, TokenSlice)> {
        let (expr, ts_z) = self.compile_expr(ts)?;
        Ok((f(Box::new(expr)), ts_z))
    }

    /// compiles the [TokenSlice] into a two-parameter [Expression]
    pub fn compile_expr_2(&mut self,
                          ts: TokenSlice,
                          expr0: Expression,
                          f: fn(Box<Expression>, Box<Expression>) -> Expression) -> io::Result<(Expression, TokenSlice)> {
        let (expr1, ts_z) = self.compile_expr(ts)?;
        Ok((f(Box::new(expr0), Box::new(expr1)), ts_z))
    }

    /// compiles the [TokenSlice] into a name-value parameter [Expression]
    pub fn compile_expr_nv(&mut self,
                           ts: TokenSlice,
                           expr0: Expression,
                           f: fn(String, Box<Expression>) -> Expression) -> io::Result<(Expression, TokenSlice)> {
        if let (Field(name) | Variable(name)) = expr0 {
            let (expr1, ts_z) = self.compile_expr(ts)?;
            Ok((f(name, Box::new(expr1)), ts_z))
        } else { fail(format!("an identifier name was expected near {:?}", ts)) }
    }

    /// compiles the [TokenSlice] into an operator-based [Expression]
    pub fn compile_alpha(&mut self, ts: TokenSlice, text: String) -> io::Result<(Expression, TokenSlice)> {
        Ok((match text.as_str() {
            "false" => Literal(Boolean(false)),
            "null" => Literal(Null),
            "true" => Literal(Boolean(true)),
            "undefined" => Literal(Undefined),
            name => Variable(name.into()),
        }, ts))
    }

    /// compiles the [TokenSlice] into an operator-based [Expression]
    pub fn compile_operator(&mut self,
                            ts: TokenSlice,
                            symbol: String,
                            _precedence: usize) -> io::Result<(Expression, TokenSlice)> {
        info!("compile_operator:symbol {:?}", symbol);
        match symbol.as_str() {
            "!" => self.compile_expr_1(ts, Not),
            "!!" => self.compile_expr_1(ts, Factorial),
            "(" => self.handle_parentheses(ts),
            sym =>
                if let Some(op0) = self.pop() {
                    match sym {
                        "&&" => self.compile_expr_2(ts, op0, And),
                        "/" => self.compile_expr_2(ts, op0, Divide),
                        "==" => self.compile_expr_2(ts, op0, Equal),
                        ">" => self.compile_expr_2(ts, op0, GreaterThan),
                        ">=" => self.compile_expr_2(ts, op0, GreaterOrEqual),
                        "<" => self.compile_expr_2(ts, op0, LessThan),
                        "<=" => self.compile_expr_2(ts, op0, LessOrEqual),
                        "-" => self.compile_expr_2(ts, op0, Minus),
                        "%" => self.compile_expr_2(ts, op0, Modulo),
                        "!=" => self.compile_expr_2(ts, op0, NotEqual),
                        "||" => self.compile_expr_2(ts, op0, Or),
                        "+" => self.compile_expr_2(ts, op0, Plus),
                        "**" => self.compile_expr_2(ts, op0, Pow),
                        ".." => self.compile_expr_2(ts, op0, Range),
                        ":=" => self.compile_expr_nv(ts, op0, SetVariable),
                        "<<" => self.compile_expr_2(ts, op0, ShiftLeft),
                        ">>" => self.compile_expr_2(ts, op0, ShiftRight),
                        "*" => self.compile_expr_2(ts, op0, Times),
                        "^" => self.compile_expr_2(ts, op0, Xor),
                        unknown => fail(format!("Invalid operator '{}'", unknown))
                    }
                } else { fail(format!("Illegal start of expression '{}'", symbol)) }
        }
    }

    pub fn handle_parentheses(&mut self, ts: TokenSlice) -> io::Result<(Expression, TokenSlice)> {
        let (tokens, ts_z) = ts.scan_until(|t| t.contains(")"));
        let innards = TokenSlice::new(tokens[0..tokens.len()].to_vec());
        let (expression, _) = self.compile_all(innards)?;
        let result = match expression.as_slice() {
            [] => Ok((Literal(Undefined), ts_z)),
            [one] => Ok((one.clone(), ts_z)),
            many => Ok((Tuple(many.to_vec()), ts_z))
        };
        self.pop();
        result
    }

    pub fn get_stack(&self) -> Vec<Expression> {
        self.stack.iter().map(|x| x.clone()).collect()
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
    use crate::typed_values::TypedValue::{Boolean, Int64Value};

    use super::*;

    #[test]
    fn test_literal_value() {
        let opcodes = Compiler::compile("1_234_567_890").unwrap();
        assert_eq!(opcodes, vec![Literal(Int64Value(1_234_567_890))]);
    }

    #[test]
    fn test_not_false() {
        let opcodes = Compiler::compile("!false").unwrap();
        assert_eq!(opcodes, vec![
            Not(Box::new(Literal(Boolean(false))))
        ]);
    }

    #[test]
    fn test_not_true() {
        let opcodes = Compiler::compile("!true").unwrap();
        assert_eq!(opcodes, vec![
            Not(Box::new(Literal(Boolean(true))))
        ]);
    }

    #[test]
    fn test_compile_math_subtraction() {
        let opcodes = Compiler::compile("_ - 7").unwrap();
        assert_eq!(opcodes, vec![
            Minus(Box::new(Variable("_".into())), Box::new(Literal(Int64Value(7))))
        ]);
    }

    #[test]
    fn test_compile_math_division() {
        let opcodes = Compiler::compile("n / 3").unwrap();
        assert_eq!(opcodes, vec![
            Divide(Box::new(Variable("n".into())), Box::new(Literal(Int64Value(3))))
        ]);
    }

    #[test]
    fn test_order_of_operations_1() {
        let opcodes = Compiler::compile("2 + (4 * 3)").unwrap();
        assert_eq!(opcodes, vec![
            Plus(Box::new(Literal(Int64Value(2))),
                 Box::new(Times(Box::new(Literal(Int64Value(4))),
                                Box::new(Literal(Int64Value(3))))))
        ]);
    }

    #[test]
    fn test_order_of_operations_2() {
        let opcodes = Compiler::compile("(4. / 3.) + (4 * 3)").unwrap();
        assert_eq!(opcodes, vec![
            Plus(Box::new(Literal(Int64Value(2))),
                 Box::new(Times(Box::new(Literal(Int64Value(4))),
                                Box::new(Literal(Int64Value(3))))))
        ]);
    }

    #[ignore]
    #[test]
    fn test_order_of_operations_3() {
        let opcodes = Compiler::compile("2 - 4 * 3").unwrap();
        assert_eq!(opcodes, vec![
            Minus(Box::new(Literal(Float64Value(2.))),
                  Box::new(Times(Box::new(Literal(Float64Value(4.))),
                                 Box::new(Literal(Float64Value(3.))))))
        ]);
    }
}