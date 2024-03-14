////////////////////////////////////////////////////////////////////
// compiler module
////////////////////////////////////////////////////////////////////

use std::io;

use crate::expression::Expression;
use crate::expression::Expression::*;
use crate::token_slice::TokenSlice;
use crate::tokenizer::parse_fully;
use crate::tokens::Token::{Numeric, Operator, Symbol};
use crate::typed_values::TypedValue::{Float64Value, Undefined};

/// Represents the compiler state
#[derive(Debug, Clone)]
pub struct Compiler {
    stack: Vec<Expression>,
}

impl Compiler {
    ////////////////////////////////////////////////////////////////
    // static methods
    ////////////////////////////////////////////////////////////////

    /// compiles the source code into opcodes
    pub fn compile(source_code: &str) -> io::Result<Vec<Expression>> {
        let mut ctx = Compiler {
            stack: vec![],
        };
        let mut ts = TokenSlice::new(parse_fully(source_code));
        while ts.has_next() {
            let start = ts.get_position();
            let op = ctx.compile_expr(&mut ts)?;
            // ensure we actually moved the cursor
            assert!(ts.get_position() > start);
            ctx.push(op);
        }
        Ok(ctx.stack)
    }

    pub fn compile_expr(&mut self, ts: &mut TokenSlice) -> io::Result<Expression> {
        match ts.take() {
            Some(Operator { text: symbol, .. }) => {
                match symbol.trim() {
                    "!" => self.compile_expr_1p(ts, Not),
                    sym =>
                        if let Some(op0) = self.pop() {
                            match sym {
                                "+" => self.compile_expr_2p(ts, op0, Plus),
                                "-" => self.compile_expr_2p(ts, op0, Minus),
                                "*" => self.compile_expr_2p(ts, op0, Times),
                                "/" => self.compile_expr_2p(ts, op0, Divide),
                                "==" => self.compile_expr_2p(ts, op0, Equal),
                                "!=" => self.compile_expr_2p(ts, op0, NotEqual),
                                ">" => self.compile_expr_2p(ts, op0, GreaterThan),
                                ">=" => self.compile_expr_2p(ts, op0, GreaterOrEqual),
                                "<" => self.compile_expr_2p(ts, op0, LessThan),
                                "<=" => self.compile_expr_2p(ts, op0, LessOrEqual),
                                x => fail(format!("Invalid operator '{}'", x))
                            }
                        } else { fail("Illegal start of expression") }
                }
            }
            Some(Numeric { text: number, .. }) => {
                Ok(Literal(Float64Value(number.parse()
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?)))
            }
            Some(Symbol { text: s, .. }) if s == "(" => self.compile_parens(ts),
            Some(t) => fail(format!("Syntax error near {:?}", t)),
            None => fail("Unexpected end of input")
        }
    }

    pub fn compile_expr_1p(&mut self,
                           ts: &mut TokenSlice,
                           f: fn(Box<Expression>) -> Expression) -> io::Result<Expression> {
        Ok(f(Box::new(self.compile_expr(ts)?)))
    }

    pub fn compile_expr_2p(&mut self,
                           ts: &mut TokenSlice,
                           op0: Expression,
                           f: fn(Box<Expression>, Box<Expression>) -> Expression) -> io::Result<Expression> {
        let op1 = self.compile_expr(ts)?;
        Ok(f(Box::new(op0), Box::new(op1)))
    }

    fn compile_parens(&mut self, ts: &mut TokenSlice) -> io::Result<Expression> {
        let mut innards = ts.capture_slice("(", ")", Some(","));
        if innards.is_empty() {
            return Ok(Literal(Undefined));
        }
        println!("compile_parens: innards {:?}", innards);
        let mut values = vec![];
        while innards.has_next() {
            let start = ts.get_position();
            println!("compile_parens: innards {:?}", &innards.get());
            values.push(self.compile_expr(&mut innards)?);
            assert!(ts.get_position() > start);
        }
        Ok(Tuple(values))
    }

    pub fn push(&mut self, expression: Expression) {
        self.stack.push(expression)
    }

    pub fn pop(&mut self) -> Option<Expression> {
        self.stack.pop()
    }
}

fn fail(message: impl Into<String>) -> io::Result<Expression> {
    Err(io::Error::new(io::ErrorKind::Other, message.into()))
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compile_math_negate() {
        let opcodes = Compiler::compile("!5").unwrap();
        assert_eq!(opcodes, vec![
            Not(Box::new(Literal(Float64Value(5.0))))
        ]);
    }

    #[test]
    fn test_compile_math_addition() {
        let opcodes = Compiler::compile("5 + 5").unwrap();
        assert_eq!(opcodes, vec![
            Plus(Box::new(Literal(Float64Value(5.0))), Box::new(Literal(Float64Value(5.0))))
        ]);
    }
}