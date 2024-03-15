////////////////////////////////////////////////////////////////////
// compiler module
////////////////////////////////////////////////////////////////////

use std::io;

use crate::expression::Expression;
use crate::expression::Expression::*;
use crate::token_slice::TokenSlice;
use crate::tokenizer::parse_fully;
use crate::tokens::Token::{AlphaNumeric, BackticksQuoted, DoubleQuoted, Numeric, Operator, SingleQuoted, Symbol};
use crate::typed_values::TypedValue::{Float64Value, StringValue, Undefined};

/// Represents the compiler state
#[derive(Debug, Clone)]
pub struct Compiler {
    stack: Vec<Expression>,
}

impl Compiler {
    ////////////////////////////////////////////////////////////////
    // static methods
    ////////////////////////////////////////////////////////////////

    /// compiles the source code into [Vec<Expression>]
    pub fn compile(source_code: &str) -> io::Result<Vec<Expression>> {
        let mut ctx = Compiler {
            stack: vec![],
        };
        let ts = TokenSlice::new(parse_fully(source_code));
        ctx.compile_thru(ts)
    }

    ////////////////////////////////////////////////////////////////
    // instance methods
    ////////////////////////////////////////////////////////////////

    /// compiles the [TokenSlice] into a [Vec<Expression>]
    pub fn compile_thru(&mut self, mut ts: TokenSlice) -> io::Result<Vec<Expression>> {
        while ts.has_next() {
            let start = ts.get_position();
            let op = self.compile_expr(&mut ts)?;
            // ensure we actually moved the cursor
            assert!(ts.get_position() > start);
            self.push(op);
        }
        Ok(self.get_stack())
    }

    /// compiles the [TokenSlice] into an [Expression]
    pub fn compile_expr(&mut self, ts: &mut TokenSlice) -> io::Result<Expression> {
        match ts.take() {
            Some(Operator { text, precedence, .. }) => {
                println!("compile_expr:Operator: symbol {:?}", text);
                match text.as_str() {
                    "!!" => self.compile_expr_1p(ts, Factorial),
                    "!" => self.compile_expr_1p(ts, Not),
                    "(" => self.finish_parens(ts),
                    sym =>
                        if let Some(op0) = self.pop() {
                            match sym {
                                "/" => self.compile_expr_2p(ts, op0, Divide),
                                "==" => self.compile_expr_2p(ts, op0, Equal),
                                ">" => self.compile_expr_2p(ts, op0, GreaterThan),
                                ">=" => self.compile_expr_2p(ts, op0, GreaterOrEqual),
                                "<" => self.compile_expr_2p(ts, op0, LessThan),
                                "<=" => self.compile_expr_2p(ts, op0, LessOrEqual),
                                "-" => self.compile_expr_2p(ts, op0, Minus),
                                "%" => self.compile_expr_2p(ts, op0, Modulo),
                                "!=" => self.compile_expr_2p(ts, op0, NotEqual),
                                "+" => self.compile_expr_2p(ts, op0, Plus),
                                "^" => self.compile_expr_2p(ts, op0, Pow),
                                ".." => self.compile_expr_2p(ts, op0, Range),
                                "*" => self.compile_expr_2p(ts, op0, Times),
                                unk => fail(format!("Invalid operator '{}'", unk))
                            }
                        } else { fail(format!("Illegal start of expression '{}'", text)) }
                }
            }
            Some(AlphaNumeric { text, .. } |
                 BackticksQuoted { text, .. }) => Ok(Field(text.into())),
            Some(DoubleQuoted { text, .. } |
                 SingleQuoted { text, .. }) => Ok(Literal(StringValue(text.into()))),
            Some(Numeric { text, .. }) => {
                println!("compile_expr:Numeric: number {:?}", text);
                Ok(Literal(Float64Value(text.parse()
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?)))
            }
            Some(Symbol { text, .. }) => Ok(Variable(text.into())),
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

    pub fn finish_parens(&mut self, ts: &mut TokenSlice) -> io::Result<Expression> {
        let tokens = ts.scan_until(|t| t.get_raw_value() == ")").to_vec();
        let innards = TokenSlice::new(tokens[0..tokens.len()].to_vec());
        let result = match self.compile_thru(innards)?.as_slice() {
            [] => Ok(Literal(Undefined)),
            [one] => Ok(one.clone()),
            v => Ok(Tuple(v.to_vec()))
        };
        println!("finish_parens: {:?}", result);
        self.pop();
        result
    }

    pub fn get_stack(&self) -> Vec<Expression> {
        self.stack.iter().map(|x| x.clone()).collect()
    }

    pub fn push(&mut self, expression: Expression) {
        println!("push -> {:?}", expression);
        self.stack.push(expression)
    }

    pub fn pop(&mut self) -> Option<Expression> {
        let result = self.stack.pop();
        println!("pop <- {:?}", result);
        result
    }
}

pub fn fail(message: impl Into<String>) -> io::Result<Expression> {
    Err(io::Error::new(io::ErrorKind::Other, message.into()))
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::machine::MachineState;

    use super::*;

    #[test]
    fn test_compile_math_negate() {
        let opcodes = Compiler::compile("!5").unwrap();
        assert_eq!(opcodes, vec![
            Not(Box::new(Literal(Float64Value(5.))))
        ]);
    }

    #[test]
    fn test_compile_math_subtraction() {
        let opcodes = Compiler::compile("12 - 7").unwrap();
        assert_eq!(opcodes, vec![
            Minus(Box::new(Literal(Float64Value(12.))), Box::new(Literal(Float64Value(7.))))
        ]);
    }

    #[test]
    fn test_compile_math_division() {
        let opcodes = Compiler::compile("n / 3").unwrap();
        assert_eq!(opcodes, vec![
            Divide(Box::new(Field("n".into())), Box::new(Literal(Float64Value(3.))))
        ]);
    }

    #[test]
    fn test_order_of_operations_1() {
        let vm = MachineState::new();
        let opcodes = Compiler::compile("2 + (4 * 3)").unwrap();
        assert_eq!(opcodes, vec![
            Plus(Box::new(Literal(Float64Value(2.))),
                 Box::new(Times(Box::new(Literal(Float64Value(4.))),
                                Box::new(Literal(Float64Value(3.))))))
        ]);
    }

    #[ignore]
    #[test]
    fn test_order_of_operations_2() {
        let vm = MachineState::new();
        let opcodes = Compiler::compile("2 + 4 * 3").unwrap();
        assert_eq!(opcodes, vec![
            Plus(Box::new(Literal(Float64Value(2.))),
                 Box::new(Times(Box::new(Literal(Float64Value(4.))),
                                Box::new(Literal(Float64Value(3.))))))
        ]);
    }
}