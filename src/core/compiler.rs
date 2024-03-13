////////////////////////////////////////////////////////////////////
// compiler module
////////////////////////////////////////////////////////////////////

use crate::compiler::CompiledCode::CodeBlock;
use crate::machine::{ErrorCode, MachineState};
use crate::token_slice::TokenSlice;
use crate::tokenizer::parse_fully;
use crate::tokens::Token::Symbol;

struct Compiler;

impl Compiler {
    /// compiles the source code into opcodes
    pub fn compile(source_code: &str) -> Result<CompiledCode, ErrorCode> {
        let mut ts = TokenSlice::new(parse_fully(source_code));
        let mut ops = Vec::new();
        while ts.has_next() {
            let opcode = Self::process_slice(&mut ts);
            if opcode.is_some() {
                ops.push(opcode.unwrap());
            } else {
                panic!("Syntax error near {:?}", ts.current().unwrap())
            }
        }
        Ok(CodeBlock(ops))
    }

    fn process_slice(ts: &mut TokenSlice) -> Option<CompiledCode> {
        match ts.current() {
            Some(Symbol { text: s, .. }) if s == "(" => Self::process_parens(ts),
            _ => None
        }
    }

    fn process_parens(ts: &mut TokenSlice) -> Option<CompiledCode> {
        let inner = ts.capture("(", ")", None);
        println!("inner {:?}", inner);
        None
    }
}

/// represents an executable instruction (opcode)
pub type OpCode = fn(&mut MachineState) -> Option<ErrorCode>;

#[derive(Debug)]
pub enum CompiledCode {
    CodeBlock(Vec<CompiledCode>),
    Standard(Box<OpCode>),
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compile() {
        let opcodes = Compiler::compile("return").unwrap();
        println!("{:?}", opcodes)
        //assert_eq!(opcodes, vec![]);
    }
}