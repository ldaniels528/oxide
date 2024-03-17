////////////////////////////////////////////////////////////////////
// compiler module
////////////////////////////////////////////////////////////////////

use std::io;

use crate::expression::Expression;
use crate::expression::Expression::*;
use crate::token_slice::TokenSlice;
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
        let (code, _) = ctx.compile_all(TokenSlice::from_string(source_code))?;
        Ok(code)
    }

    ////////////////////////////////////////////////////////////////
    // instance methods
    ////////////////////////////////////////////////////////////////

    /// compiles the [TokenSlice] into a [Vec<Expression>]
    pub fn compile_all(&mut self, ts: TokenSlice) -> io::Result<(Vec<Expression>, TokenSlice)> {
        let mut ts_z = ts;
        while ts_z.has_next() {
            let start = ts_z.get_position();
            let (op, ts1) = self.compile_expr(ts_z)?;
            ts_z = ts1;
            // ensure we actually moved the cursor
            assert!(ts_z.get_position() > start);
            self.push(op);
        }
        Ok((self.get_stack(), ts_z))
    }

    /// compiles the [TokenSlice] into an [Expression]
    pub fn compile_expr(&mut self, ts: TokenSlice) -> io::Result<(Expression, TokenSlice)> {
        match ts.next() {
            (Some(Operator { text, precedence, .. }), ts) =>
                self.compile_operator(ts, text),
            (Some(AlphaNumeric { text, .. } |
                  BackticksQuoted { text, .. }), ts) => Ok((Field(text.into()), ts)),
            (Some(DoubleQuoted { text, .. } |
                  SingleQuoted { text, .. }), ts) => Ok((Literal(StringValue(text.into())), ts)),
            (Some(Numeric { text, .. }), ts) =>
                Ok((Literal(Float64Value(text.parse()
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?)), ts)),
            (Some(Symbol { text, .. }), ts) => Ok((Variable(text.into()), ts)),
            (Some(t), _) => fail(format!("Syntax error near {:?}", t)),
            (None, _) => fail("Unexpected end of input")
        }
    }

    /// compiles the [TokenSlice] into a single-parameter [Expression]
    pub fn compile_expr_1p(&mut self,
                           ts: TokenSlice,
                           f: fn(Box<Expression>) -> Expression) -> io::Result<(Expression, TokenSlice)> {
        let (expr, ts_z) = self.compile_expr(ts)?;
        Ok((f(Box::new(expr)), ts_z))
    }

    /// compiles the [TokenSlice] into a two-parameter [Expression]
    pub fn compile_expr_2p(&mut self,
                           ts: TokenSlice,
                           expr0: Expression,
                           f: fn(Box<Expression>, Box<Expression>) -> Expression) -> io::Result<(Expression, TokenSlice)> {
        let (expr1, ts_z) = self.compile_expr(ts)?;
        Ok((f(Box::new(expr0), Box::new(expr1)), ts_z))
    }

    /// compiles the [TokenSlice] into an operator-based [Expression]
    pub fn compile_operator(&mut self, ts: TokenSlice, symbol: String) -> io::Result<(Expression, TokenSlice)> {
        println!("compile_expr:Operator: symbol {:?}", symbol);
        match symbol.as_str() {
            "!!" => self.compile_expr_1p(ts, Factorial),
            "!" => self.compile_expr_1p(ts, Not),
            "(" => self.handle_parentheses(&ts),
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
                } else { fail(format!("Illegal start of expression '{}'", symbol)) }
        }
    }

    pub fn handle_parentheses(&mut self, ts: &TokenSlice) -> io::Result<(Expression, TokenSlice)> {
        let (tokens, ts_z) = ts.scan_until(|t| t.get_raw_value() == ")");
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
        println!("push -> {:?}", expression);
        self.stack.push(expression)
    }

    pub fn pop(&mut self) -> Option<Expression> {
        let result = self.stack.pop();
        println!("pop <- {:?}", result);
        result
    }
}

pub fn fail<A>(message: impl Into<String>) -> io::Result<A> {
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