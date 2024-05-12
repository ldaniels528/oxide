////////////////////////////////////////////////////////////////////
// virtualization module
////////////////////////////////////////////////////////////////////

use std::ops::Deref;

use bytes::BufMut;

use shared_lib::fail;

use crate::byte_buffer::ByteBuffer;
use crate::compiler::fail_expr;
use crate::data_types::{DataType, T_BLOB, T_BOOLEAN, T_CLOB, T_DATE, T_ENUM, T_FLOAT32, T_FLOAT64, T_INT16, T_INT32, T_INT64, T_INT8, T_RECORD_NUMBER, T_STRING, T_STRUCTURE, T_TABLE, T_UUID};
use crate::data_types::DataType::{BLOBType, BooleanType, CLOBType, DateType, EnumType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, RecordNumberType, StringType, StructureType, TableType, UUIDType};
use crate::expression::{Expression, UNDEFINED};
use crate::expression::Expression::{And, ArrayLiteral, AsValue, Between, BitwiseAnd, BitwiseOr, CodeBlock, Contains, CreateIndex, CreateTable, Delete, Divide, Drop, Equal, Factorial, From, LessOrEqual, LessThan, Limit, Literal, Minus, Multiply, Not, NotEqual, Or, Plus, Pow, Range, Variable, Where, While};
use crate::machine::MachineState;
use crate::typed_values::*;
use crate::typed_values::TypedValue::*;

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
pub const A_DIVIDE: u8 = 12;
pub const A_DROP: u8 = 13;
pub const A_EQUAL: u8 = 14;
pub const A_FACTORIAL: u8 = 15;
pub const A_FROM: u8 = 16;
pub const A_GREATER_THAN: u8 = 17;
pub const A_GREATER_OR_EQUAL: u8 = 18;
pub const A_IF: u8 = 19;
pub const A_INTO: u8 = 20;
pub const A_JSON_LITERAL: u8 = 11;
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
            encode3(A_CREATE_INDEX, index, table, &ColumnSet(columns.clone())),
        CreateTable { table, columns, from } =>
            encode3(A_CREATE_TABLE, table, &ColumnSet(columns.clone()), &(from.clone().unwrap_or(Box::new(UNDEFINED)))),
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
            let mut buf = vec![A_LITERAL];
            buf.extend(assemble_value(value));
            buf
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
pub fn assemble_fully(expressions: &Vec<Expression>) -> Vec<u8> {
    expressions.iter().flat_map(|expr| assemble(expr)).collect::<Vec<u8>>()
}

/// decodes the typed value based on the supplied data type and buffer
pub fn assemble_type(data_type: &DataType) -> Vec<u8> {
    match data_type {
        BLOBType(size) => encode_bytes(T_BLOB, &assemble_usize(*size)),
        BooleanType => encode_bytes(T_BOOLEAN, &vec![]),
        CLOBType(size) => encode_bytes(T_CLOB, &assemble_usize(*size)),
        DateType => encode_bytes(T_DATE, &vec![]),
        EnumType(labels) => encode_bytes(T_ENUM, &vec![]),
        Float32Type => encode_bytes(T_FLOAT32, &vec![]),
        Float64Type => encode_bytes(T_FLOAT64, &vec![]),
        Int8Type => encode_bytes(T_INT8, &vec![]),
        Int16Type => encode_bytes(T_INT16, &vec![]),
        Int32Type => encode_bytes(T_INT32, &vec![]),
        Int64Type => encode_bytes(T_INT64, &vec![]),
        RecordNumberType => encode_bytes(T_RECORD_NUMBER, &vec![]),
        StringType(size) => encode_bytes(T_STRING, &assemble_usize(*size)),
        StructureType(columns) => encode_bytes(T_STRUCTURE, &vec![]),
        TableType(columns) => encode_bytes(T_TABLE, &vec![]),
        UUIDType => encode_bytes(T_UUID, &vec![]),
    }
}

/// decodes the typed value based on the supplied data type and buffer
pub fn assemble_value(value: &TypedValue) -> Vec<u8> {
    let mut byte_code = vec![value.ordinal()];
    byte_code.extend(TypedValue::encode_value(value));
    byte_code
}

fn assemble_usize(value: usize) -> Vec<u8> {
    usize::to_be_bytes(value).to_vec()
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

pub fn disassemble(buf: &mut ByteBuffer) -> std::io::Result<Expression> {
    match buf.next_u8() {
        A_AND => Ok(And(disassemble_box(buf)?, disassemble_box(buf)?)),
        A_ARRAY_LIT => Ok(ArrayLiteral(disassemble_array(buf)?)),
        A_AS_VALUE => Ok(AsValue(buf.next_string(), disassemble_box(buf)?)),
        A_BETWEEN => Ok(Between(disassemble_box(buf)?, disassemble_box(buf)?, disassemble_box(buf)?)),
        A_BITWISE_AND => Ok(BitwiseAnd(disassemble_box(buf)?, disassemble_box(buf)?)),
        A_BITWISE_OR => Ok(BitwiseOr(disassemble_box(buf)?, disassemble_box(buf)?)),
        A_CODE_BLOCK => Ok(CodeBlock(disassemble_array(buf)?)),
        A_CONTAINS => Ok(Contains(disassemble_box(buf)?, disassemble_box(buf)?)),
        A_CREATE_INDEX =>
            Ok(CreateIndex {
                index: disassemble_box(buf)?,
                columns: buf.next_columns(),
                table: disassemble_box(buf)?,
            }),
        A_CREATE_TABLE =>
            Ok(CreateTable {
                table: disassemble_box(buf)?,
                columns: buf.next_columns(),
                from: disassemble_opt(buf)?,
            }),
        A_DELETE =>
            Ok(Delete {
                table: disassemble_box(buf)?,
                condition: disassemble_opt(buf)?,
                limit: disassemble_opt(buf)?,
            }),
        A_DIVIDE => Ok(Divide(disassemble_box(buf)?, disassemble_box(buf)?)),
        A_DROP => Ok(Drop { table: disassemble_box(buf)? }),
        A_EQUAL => Ok(Equal(disassemble_box(buf)?, disassemble_box(buf)?)),
        A_FACTORIAL => Ok(Factorial(disassemble_box(buf)?)),
        A_FROM => Ok(From(disassemble_box(buf)?)),
        A_GET_VAR =>
            match disassemble(buf)? {
                Literal(StringValue(name)) => Ok(Variable(name)),
                z => fail_expr("Expected identifier", &z)
            }
        A_GREATER_OR_EQUAL => Ok(LessOrEqual(disassemble_box(buf)?, disassemble_box(buf)?)),
        A_GREATER_THAN => Ok(LessThan(disassemble_box(buf)?, disassemble_box(buf)?)),
        A_LESS_OR_EQUAL => Ok(LessOrEqual(disassemble_box(buf)?, disassemble_box(buf)?)),
        A_LESS_THAN => Ok(LessThan(disassemble_box(buf)?, disassemble_box(buf)?)),
        A_LIMIT => Ok(Limit { from: disassemble_box(buf)?, limit: disassemble_box(buf)? }),
        A_LITERAL => Ok(Literal(buf.next_value())),
        A_MINUS => Ok(Minus(disassemble_box(buf)?, disassemble_box(buf)?)),
        A_MULTIPLY => Ok(Multiply(disassemble_box(buf)?, disassemble_box(buf)?)),
        A_NOT => Ok(Not(disassemble_box(buf)?)),
        A_NOT_EQUAL => Ok(NotEqual(disassemble_box(buf)?, disassemble_box(buf)?)),
        A_OR => Ok(Or(disassemble_box(buf)?, disassemble_box(buf)?)),
        A_PLUS => Ok(Plus(disassemble_box(buf)?, disassemble_box(buf)?)),
        A_POW => Ok(Pow(disassemble_box(buf)?, disassemble_box(buf)?)),
        A_RANGE => Ok(Range(disassemble_box(buf)?, disassemble_box(buf)?)),
        A_WHERE => Ok(Where { from: disassemble_box(buf)?, condition: disassemble_box(buf)? }),
        A_WHILE => Ok(While { condition: disassemble_box(buf)?, code: disassemble_box(buf)? }),
        x => fail(format!("Invalid expression code {}", x))
    }
}

fn disassemble_array(buf: &mut ByteBuffer) -> std::io::Result<Vec<Expression>> {
    let count = buf.next_u32();
    let mut array = vec![];
    for _ in 0..count { array.push(disassemble(buf)?); }
    Ok(array)
}

fn disassemble_box(buf: &mut ByteBuffer) -> std::io::Result<Box<Expression>> {
    Ok(Box::new(disassemble(buf)?))
}

pub fn disassemble_fully(byte_code: &Vec<u8>) -> std::io::Result<Vec<Expression>> {
    let mut models = vec![];
    let mut buf = ByteBuffer::wrap(byte_code.clone());
    while buf.has_next() {
        models.push(disassemble(&mut buf)?);
    }
    Ok(models)
}

fn disassemble_next_value(buf: &mut ByteBuffer) -> std::io::Result<TypedValue> {
    if buf.has_more(8) {
        match buf.next_u8() {
            V_UNDEFINED => Ok(Undefined),
            V_NULL => Ok(Null),
            V_BLOB => Ok(BLOB(buf.next_blob())),
            V_CLOB => Ok(CLOB(buf.next_clob())),
            V_DATE => Ok(DateValue(buf.next_i64())),
            V_FLOAT32 => Ok(Float32Value(buf.next_f32())),
            V_FLOAT64 => Ok(Float64Value(buf.next_f64())),
            V_INT8 => Ok(Int8Value(buf.next_u8())),
            V_INT16 => Ok(Int16Value(buf.next_i16())),
            V_INT32 => Ok(Int32Value(buf.next_i32())),
            V_INT64 => Ok(Int64Value(buf.next_i64())),
            V_STRING => Ok(StringValue(buf.next_string())),
            V_UUID => Ok(UUIDValue(buf.next_uuid())),
            x => fail(format!("Unhandled value code {}", x))
        }
    } else {
        fail(format!("Buffer underflow: required {}, available {}", 8, buf.position()))
    }
}

fn disassemble_next_type(buf: &mut ByteBuffer) -> std::io::Result<DataType> {
    use crate::data_types::*;
    match buf.next_u8() {
        T_BLOB => Ok(BLOBType(buf.next_u32() as usize)),
        T_BOOLEAN => Ok(BooleanType),
        T_CLOB => Ok(CLOBType(buf.next_u32() as usize)),
        T_DATE => Ok(DateType),
        T_ENUM => Ok(EnumType(buf.next_string_array())),
        T_FLOAT32 => Ok(Float32Type),
        T_FLOAT64 => Ok(Float64Type),
        T_INT8 => Ok(Int8Type),
        T_INT16 => Ok(Int16Type),
        T_INT32 => Ok(Int32Type),
        T_INT64 => Ok(Int64Type),
        T_RECORD_NUMBER => Ok(RecordNumberType),
        T_STRING => Ok(StringType(buf.next_u32() as usize)),
        T_STRUCTURE => Ok(StructureType(buf.next_columns())),
        T_TABLE => Ok(TableType(buf.next_columns())),
        T_UUID => Ok(UUIDType),
        x => fail(format!("Unhandled type code {}", x))
    }
}

fn disassemble_opt(buf: &mut ByteBuffer) -> std::io::Result<Option<Box<Expression>>> {
    match buf.next_u8() {
        V_UNDEFINED => Ok(None),
        _ => {
            buf.move_rel(-1);
            Ok(Some(disassemble_box(buf)?))
        }
    }
}

fn encode_bytes(id: u8, code: &Vec<u8>) -> Vec<u8> {
    let mut byte_code = vec![id];
    byte_code.extend(code);
    byte_code
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

fn encode3(id: u8, a: &Expression, b: &Expression, c: &Expression) -> Vec<u8> {
    let mut opc = vec![id];
    opc.extend(assemble(a));
    opc.extend(assemble(b));
    opc.extend(assemble(c));
    opc
}

fn encode_value(id: u8, value: &TypedValue) -> Vec<u8> {
    let mut byte_code = vec![id];
    byte_code.push(value.ordinal());
    byte_code.extend(value.encode());
    byte_code
}

fn encode_vec(id: u8, ops: &Vec<Expression>) -> Vec<u8> {
    let mut byte_code = vec![id];
    byte_code.extend((ops.len() as u64).to_be_bytes());
    byte_code.extend(ops.iter().flat_map(assemble).collect::<Vec<u8>>());
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
    ms.transform_numeric(number, fact_i64, fact_f64)
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
    use crate::typed_values::V_INT64;

    use super::*;

    #[test]
    fn test_literal() {
        let model = Literal(StringValue("hello".into()));
        let byte_code = vec![
            A_LITERAL, V_STRING, 0, 0, 0, 0, 0, 0, 0, 5, b'h', b'e', b'l', b'l', b'o',
        ];
        assert_eq!(assemble(&model), byte_code);
        assert_eq!(disassemble(&mut ByteBuffer::wrap(byte_code)).unwrap(), model);
    }

    #[test]
    fn test_variable() {
        let model = Variable("name".into());
        let byte_code = vec![
            A_GET_VAR, A_LITERAL, V_STRING, 0, 0, 0, 0, 0, 0, 0, 4, b'n', b'a', b'm', b'e',
        ];
        assert_eq!(assemble(&model), byte_code);
        assert_eq!(disassemble(&mut ByteBuffer::wrap(byte_code)).unwrap(), model);
    }

    #[test]
    fn test_math_ops() {
        let model = Plus(Box::new(Literal(Int64Value(2))),
                         Box::new(Multiply(Box::new(Literal(Int64Value(4))),
                                           Box::new(Literal(Int64Value(3))))));
        let byte_code = vec![
            A_PLUS,
            A_LITERAL, V_INT64, 0, 0, 0, 0, 0, 0, 0, 2,
            A_MULTIPLY,
            A_LITERAL, V_INT64, 0, 0, 0, 0, 0, 0, 0, 4,
            A_LITERAL, V_INT64, 0, 0, 0, 0, 0, 0, 0, 3,
        ];
        let mut buf = ByteBuffer::wrap(byte_code.clone());
        assert_eq!(assemble(&model), byte_code);
        assert_eq!(disassemble(&mut buf).unwrap(), model);
    }

    #[test]
    fn test_delete() {
        let model = Delete {
            table: Box::new(Variable("stocks".into())),
            condition: Some(
                Box::new(LessOrEqual(
                    Box::new(Variable("last_sale".into())),
                    Box::new(Literal(Float64Value(1.0))),
                ))
            ),
            limit: Some(Box::new(Literal(Int64Value(100)))),
        };
        let byte_code = vec![
            A_DELETE,
            A_GET_VAR, A_LITERAL, V_STRING, 0, 0, 0, 0, 0, 0, 0, 6,
            b's', b't', b'o', b'c', b'k', b's',
            A_LESS_OR_EQUAL,
            A_GET_VAR, A_LITERAL, V_STRING, 0, 0, 0, 0, 0, 0, 0, 9,
            b'l', b'a', b's', b't', b'_', b's', b'a', b'l', b'e',
            A_LITERAL, V_FLOAT64, 63, 240, 0, 0, 0, 0, 0, 0,
            A_LITERAL, V_INT64, 0, 0, 0, 0, 0, 0, 0, 100,
        ];
        assert_eq!(assemble(&model), byte_code);
        assert_eq!(disassemble(&mut ByteBuffer::wrap(byte_code)).unwrap(), model)
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