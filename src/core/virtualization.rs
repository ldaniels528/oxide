////////////////////////////////////////////////////////////////////
// virtualization module
////////////////////////////////////////////////////////////////////

use std::ops::Deref;

use crate::expression::{Expression, UNDEFINED};
use crate::machine::MachineState;
use crate::server::ColumnJs;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{Float64Value, Int64Value, RecordNumber, StringValue, Undefined};

/// represents an executable instruction (opcode)
pub type OpCode = fn(MachineState) -> std::io::Result<MachineState>;

// constants
pub const A_AND: u8 = 0;
pub const A_ARRAY_LIT: u8 = 1;
pub const A_AS_VALUE: u8 = 2;
pub const A_BETWEEN: u8 = 3;
pub const A_BITWISE_AND: u8 = 4;
pub const A_BITWISE_OR: u8 = 5;
pub const A_CODE_BLOCK: u8 = 6;
pub const A_CONTAINS: u8 = 7;
pub const A_CREATE_INDEX: u8 = 8;
pub const A_CREATE_TABLE: u8 = 9;
pub const A_DELETE: u8 = 10;
pub const A_JSON_LITERAL: u8 = 11;
pub const A_DIVIDE: u8 = 12;
pub const A_DROP: u8 = 13;
pub const A_EQUAL: u8 = 14;
pub const A_FACTORIAL: u8 = 15;
pub const A_FROM: u8 = 16;
pub const A_GREATER_THAN: u8 = 17;
pub const A_GREATER_OR_EQUAL: u8 = 18;
pub const A_IF: u8 = 19;
pub const A_INTO: u8 = 20;
pub const A_LESS_THAN: u8 = 21;
pub const A_LESS_OR_EQUAL: u8 = 22;
pub const A_LIMIT: u8 = 23;
pub const A_LITERAL: u8 = 24;
pub const A_MINUS: u8 = 25;
pub const A_MODULO: u8 = 26;
pub const A_MULTIPLY: u8 = 27;
pub const A_NEG: u8 = 28;
pub const A_NOT: u8 = 29;
pub const A_NOT_EQUAL: u8 = 30;
pub const A_NS: u8 = 31;
pub const A_OR: u8 = 32;
pub const A_OVERWRITE: u8 = 33;
pub const A_PLUS: u8 = 34;
pub const A_POW: u8 = 35;
pub const A_RANGE: u8 = 36;
pub const A_RETURN: u8 = 37;
pub const A_SELECT: u8 = 38;
pub const A_SET_VAR: u8 = 39;
pub const A_SHIFT_LEFT: u8 = 40;
pub const A_SHIFT_RIGHT: u8 = 41;
pub const A_TRUNCATE: u8 = 42;
pub const A_TUPLE: u8 = 43;
pub const A_UPDATE: u8 = 44;
pub const A_GET_VAR: u8 = 45;
pub const A_VIA: u8 = 46;
pub const A_XOR: u8 = 47;
pub const A_WHERE: u8 = 48;
pub const A_WHILE: u8 = 49;

/// compiles the expression into binary code
pub fn assemble(expression: &Expression) -> Vec<u8> {
    use crate::expression::Expression::*;
    match expression {
        And(a, b) => encode2(A_AND, a, b),
        ArrayLiteral(items) => encode_vec(A_ARRAY_LIT, items),
        AsValue(name, expr) =>
            encode2(A_AS_VALUE, &Literal(StringValue(name.to_string())), expr),
        Between(a, b, c) =>
            encode3(A_BETWEEN, a, b, c),
        BitwiseAnd(a, b) =>
            encode2(A_BITWISE_AND, a, b),
        BitwiseOr(a, b) =>
            encode2(A_BITWISE_OR, a, b),
        CodeBlock(ops) => encode_vec(A_CODE_BLOCK, ops),
        ColumnSet(columns) =>
            columns.iter().flat_map(|c| c.encode()).collect::<Vec<u8>>(),
        Contains(a, b) => encode2(A_CONTAINS, a, b),
        CreateIndex { index, columns, table } =>
            encode2c(A_CREATE_INDEX, table, columns),
        CreateTable { table, columns, from } =>
            encode3(A_CREATE_TABLE, table, todo!(), &(from.clone().unwrap_or(Box::new(UNDEFINED)))),
        Delete { table, condition, limit } =>
            encode3(A_DELETE, table, &(condition.clone().unwrap_or(Box::new(UNDEFINED))), &(limit.clone().unwrap_or(Box::new(UNDEFINED)))),
        JSONLiteral(tuples) => {
            let expressions = tuples.iter().flat_map(|(k, v)| {
                vec![Literal(StringValue(k.into())), v.clone()]
            }).collect::<Vec<Expression>>();
            encode_vec(A_JSON_LITERAL, &expressions)
        }
        Divide(a, b) => encode2(A_DIVIDE, a, b),
        Drop { table } => encode1(A_DROP, table),
        Equal(a, b) => encode2(A_EQUAL, a, b),
        Factorial(a) => encode1(A_FACTORIAL, a),
        From(src) => encode1(A_FROM, src),
        GreaterThan(a, b) => encode2(A_GREATER_THAN, a, b),
        GreaterOrEqual(a, b) => encode2(A_GREATER_OR_EQUAL, a, b),
        If { condition, a, b } =>
            encode3(A_IF, condition, a, &(b.clone().unwrap_or(Box::new(UNDEFINED)))),
        InsertInto { table, source } => encode2(A_INTO, table, source),
        LessThan(a, b) => encode2(A_LESS_THAN, a, b),
        LessOrEqual(a, b) => encode2(A_LESS_OR_EQUAL, a, b),
        Limit { from, limit } => encode2(A_LIMIT, from, limit),
        Literal(value) => {
            let mut byte_code = vec![A_LITERAL];
            byte_code.push(value.ordinal());
            byte_code.extend(value.encode());
            byte_code
        }
        Minus(a, b) => encode2(A_MINUS, a, b),
        Modulo(a, b) => encode2(A_MODULO, a, b),
        Multiply(a, b) => encode2(A_MULTIPLY, a, b),
        Neg(a) => encode1(A_NEG, a),
        Not(a) => encode1(A_NOT, a),
        NotEqual(a, b) => encode2(A_NOT_EQUAL, a, b),
        Ns(a) => encode1(A_NS, a),
        Or(a, b) => encode2(A_OR, a, b),
        Overwrite { table, source, condition, limit } => {
            let mut args = vec![];
            args.push(table.deref().clone());
            args.push(source.deref().clone());
            args.push(condition.clone().unwrap_or(Box::new(UNDEFINED)).deref().clone());
            args.push(limit.clone().unwrap_or(Box::new(UNDEFINED)).deref().clone());
            encode_vec(A_OVERWRITE, &args)
        }
        Plus(a, b) => encode2(A_PLUS, a, b),
        Pow(a, b) => encode2(A_POW, a, b),
        Range(a, b) => encode2(A_RANGE, a, b),
        Return(a) => encode_vec(A_RETURN, a),
        Select {
            fields, from, condition,
            group_by, having,
            order_by, limit
        } => {
            let mut args = vec![];
            args.push(from.clone().unwrap_or(Box::new(UNDEFINED)).deref().clone());
            args.push(condition.clone().unwrap_or(Box::new(UNDEFINED)).deref().clone());
            args.push(limit.clone().unwrap_or(Box::new(UNDEFINED)).deref().clone());
            args.push(Literal(RecordNumber(fields.len())));
            args.extend(fields.clone());
            encode_vec(A_SELECT, &args)
        }
        SetVariable(name, expr) =>
            encode2(A_SET_VAR, &Literal(StringValue(name.into())), expr),
        ShiftLeft(a, b) => encode2(A_SHIFT_LEFT, a, b),
        ShiftRight(a, b) => encode2(A_SHIFT_RIGHT, a, b),
        Truncate { table, new_size } =>
            encode2(A_TRUNCATE, table, &(new_size.clone().unwrap_or(Box::new(UNDEFINED)))),
        TupleExpr(values) => encode_vec(A_TUPLE, values),
        Update { table, source, condition, limit } => {
            let mut args: Vec<Expression> = vec![];
            args.push(table.deref().clone());
            args.push(source.deref().clone());
            args.push(condition.clone().unwrap_or(Box::new(UNDEFINED)).deref().clone());
            args.push(limit.clone().unwrap_or(Box::new(UNDEFINED)).deref().clone());
            encode_vec(A_UPDATE, &args)
        }
        Variable(name) => encode1(A_GET_VAR, &Literal(StringValue(name.into()))),
        Via(src) => encode1(A_VIA, src),
        Xor(a, b) => encode2(A_XOR, a, b),
        Where { from, condition } =>
            encode2(A_WHERE, from, condition),
        While { condition, code } =>
            encode2(A_WHILE, condition, code),
    }
}

/// compiles the collection of expressions into binary code
pub fn assemble_all(expressions: &Vec<Expression>) -> Vec<u8> {
    expressions.iter().flat_map(|expr| assemble(expr)).collect::<Vec<u8>>()
}

/// arithmetic addition
pub fn add(ms: MachineState) -> std::io::Result<MachineState> {
    let (ms, y) = ms.pop_or(Undefined);
    let (ms, x) = ms.pop_or(Undefined);
    Ok(ms.push(x + y))
}

/// arithmetic decrement
pub fn dec(ms: MachineState) -> std::io::Result<MachineState> {
    let (ms, x) = ms.pop_or(Undefined);
    ms.transform_numeric(x, |n| Int64Value(n - 1), |n| Float64Value(n - 1.))
}

/// arithmetic division
pub fn div(ms: MachineState) -> std::io::Result<MachineState> {
    let (ms, y) = ms.pop_or(Undefined);
    let (ms, x) = ms.pop_or(Undefined);
    Ok(ms.push(x / y))
}

fn encode1(id: u8, a: &Expression) -> Vec<u8> {
    let mut opc = vec![id];
    opc.extend(assemble(a));
    opc
}

fn encode2(id: u8, a: &Expression, b: &Expression) -> Vec<u8> {
    let mut opc = vec![id];
    opc.extend(assemble(a));
    opc.extend(assemble(b));
    opc
}

fn encode2c(id: u8, a: &Expression, b: &Vec<ColumnJs>) -> Vec<u8> {
    let mut opc = vec![id];
    opc.extend(assemble(a));
    opc.push(b.len() as u8);
    opc.extend(b.iter().flat_map(|c| c.encode()).collect::<Vec<u8>>());
    opc
}

fn encode3(id: u8, a: &Expression, b: &Expression, c: &Expression) -> Vec<u8> {
    let mut opc = vec![id];
    opc.extend(assemble(a));
    opc.extend(assemble(b));
    opc.extend(assemble(c));
    opc
}

fn encode_vec(id: u8, ops: &Vec<Expression>) -> Vec<u8> {
    let mut byte_code = vec![id];
    byte_code.extend(ops.iter().flat_map(|op| assemble(op)).collect::<Vec<u8>>());
    byte_code
}

/// evaluates the specified instructions on this state machine.
pub fn evaluate(ms: MachineState, ops: &Vec<OpCode>) -> std::io::Result<(MachineState, TypedValue)> {
    let mut ms = ms;
    for op in ops { ms = op(ms)? }
    let (ms, result) = ms.pop_or(Undefined);
    Ok((ms, result))
}

/// arithmetic factorial
pub fn fact(ms: MachineState) -> std::io::Result<MachineState> {
    fn fact_i64(n: i64) -> TypedValue {
        fn fact_i(n: i64) -> i64 { if n <= 1 { 1 } else { n * fact_i(n - 1) } }
        Int64Value(fact_i(n))
    }

    fn fact_f64(n: f64) -> TypedValue {
        fn fact_f(n: f64) -> f64 { if n <= 1. { 1. } else { n * fact_f(n - 1.) } }
        Float64Value(fact_f(n))
    }

    let (ms, number) = ms.pop_or(Undefined);
    ms.transform_numeric(number, |n| fact_i64(n), |n| fact_f64(n))
}

/// arithmetic increment
pub fn inc(ms: MachineState) -> std::io::Result<MachineState> {
    let (ms, x) = ms.pop_or(Undefined);
    ms.transform_numeric(x, |n| Int64Value(n + 1), |n| Float64Value(n + 1.))
}

/// arithmetic multiplication
pub fn mul(ms: MachineState) -> std::io::Result<MachineState> {
    let (ms, y) = ms.pop_or(Undefined);
    let (ms, x) = ms.pop_or(Undefined);
    Ok(ms.push(x * y))
}

/// arithmetic subtraction
pub fn sub(ms: MachineState) -> std::io::Result<MachineState> {
    let (ms, y) = ms.pop_or(Undefined);
    let (ms, x) = ms.pop_or(Undefined);
    Ok(ms.push(x - y))
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::expression::Expression::*;
    use crate::machine::MachineState;
    use crate::typed_values::TypedValue::{Float64Value, Int32Value, Int64Value};
    use crate::typed_values::TypedValue::*;
    use crate::virtualization;
    use crate::virtualization::{add, dec, div, fact, inc, mul, sub};

    #[test]
    fn test_assemble_all() {
        let ops = vec![
            Plus(Box::new(Literal(Int64Value(2))),
                 Box::new(Multiply(Box::new(Literal(Int64Value(4))),
                                   Box::new(Literal(Int64Value(3))))))
        ];
        let byte_code = virtualization::assemble_all(&ops);
        assert_eq!(byte_code, vec![
            // PLUS
            34,
            // ___ LIT 2
            24, 13, 0, 0, 0, 0, 0, 0, 0, 2,
            // MUL
            27,
            // ___ LIT 4
            24, 13, 0, 0, 0, 0, 0, 0, 0, 4,
            // ___ LIT 3
            24, 13, 0, 0, 0, 0, 0, 0, 0, 3,
        ])
    }

    #[test]
    fn test_dec() {
        let ms = MachineState::new().push(Int64Value(6));
        let ms = dec(ms).unwrap();
        let (_, result) = ms.pop();
        assert_eq!(result, Some(Int64Value(5)));
    }

    #[test]
    fn test_inc() {
        let ms = MachineState::new().push(Int64Value(6));
        let ms = inc(ms).unwrap();
        let (_, result) = ms.pop();
        assert_eq!(result, Some(Int64Value(7)));
    }

    #[test]
    fn test_fact_i32() {
        let ms = MachineState::new().push(Int32Value(5));
        let ms = fact(ms).unwrap();
        let (_, result) = ms.pop();
        assert_eq!(result, Some(Int64Value(120)));
    }

    #[test]
    fn test_fact_f64() {
        let ms = MachineState::new().push(Float64Value(6.));
        let ms = fact(ms).unwrap();
        let (_, result) = ms.pop();
        assert_eq!(result, Some(Float64Value(720.)));
    }

    #[test]
    fn test_multiple_opcodes() {
        // execute the program
        let (ms, value) = MachineState::new()
            .push_all(vec![
                2., 3., // add
                4., // mul
                5., // sub
                2., // div
            ].iter().map(|n| Float64Value(*n)).collect())
            .execute(&vec![add, mul, sub, div])
            .unwrap();

        // verify the result
        assert_eq!(value, Float64Value(-0.08));
        assert_eq!(ms.stack_len(), 0)
    }
}