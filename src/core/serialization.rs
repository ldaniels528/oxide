////////////////////////////////////////////////////////////////////
// serialization module
////////////////////////////////////////////////////////////////////

use std::ops::Deref;

use shared_lib::fail;

use crate::byte_buffer::ByteBuffer;
use crate::compiler::fail_expr;
use crate::data_types::*;
use crate::data_types::DataType::*;
use crate::expression::{Expression, UNDEFINED};
use crate::expression::Expression::*;
use crate::machine::MachineState;
use crate::server::ColumnJs;
use crate::typed_values::*;
use crate::typed_values::TypedValue::*;

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
pub const A_VAR_GET: u8 = 45;
pub const A_VIA: u8 = 46;
pub const A_XOR: u8 = 47;
pub const A_WHERE: u8 = 48;
pub const A_WHILE: u8 = 49;
pub const A_ANON_FX: u8 = 50;
pub const A_NAMED_FX: u8 = 51;

/// compiles the expression into binary code
pub fn assemble(expression: &Expression) -> Vec<u8> {
    use crate::expression::Expression::*;
    match expression {
        And(a, b) => encode(A_AND, vec![a, b]),
        AnonymousFx { params, code } =>
            encode(A_ANON_FX, vec![&ColumnSet(params.clone()), code]),
        ArrayLiteral(items) => encode_vec(A_ARRAY_LIT, items),
        AsValue(name, expr) =>
            encode(A_AS_VALUE, vec![&Literal(StringValue(name.to_string())), expr]),
        Between(a, b, c) =>
            encode(A_BETWEEN, vec![a, b, c]),
        BitwiseAnd(a, b) =>
            encode(A_BITWISE_AND, vec![a, b]),
        BitwiseOr(a, b) =>
            encode(A_BITWISE_OR, vec![a, b]),
        CodeBlock(ops) => encode_vec(A_CODE_BLOCK, ops),
        ColumnSet(columns) => encode_columns(columns),
        Contains(a, b) => encode(A_CONTAINS, vec![a, b]),
        CreateIndex { index, columns } =>
            encode(A_CREATE_INDEX, vec![index, columns]),
        CreateTable { table, columns, from } =>
            encode(A_CREATE_TABLE, vec![table, &ColumnSet(columns.clone()), &get_or_undef(from)]),
        Delete { table, condition, limit } =>
            encode(A_DELETE, vec![table, &get_or_undef(condition), &get_or_undef(limit)]),
        JSONLiteral(tuples) => assemble_json_object(tuples),
        Divide(a, b) => encode(A_DIVIDE, vec![a, b]),
        Drop { table } => encode(A_DROP, vec![table]),
        Equal(a, b) => encode(A_EQUAL, vec![a, b]),
        Factorial(a) => encode(A_FACTORIAL, vec![a]),
        From(src) => encode(A_FROM, vec![src]),
        GreaterThan(a, b) => encode(A_GREATER_THAN, vec![a, b]),
        GreaterOrEqual(a, b) => encode(A_GREATER_OR_EQUAL, vec![a, b]),
        If { condition, a, b } =>
            encode(A_IF, vec![condition, a, &get_or_undef(b)]),
        InsertInto { table, source } => encode(A_INTO, vec![table, source]),
        LessThan(a, b) => encode(A_LESS_THAN, vec![a, b]),
        LessOrEqual(a, b) => encode(A_LESS_OR_EQUAL, vec![a, b]),
        Limit { from, limit } => encode(A_LIMIT, vec![from, limit]),
        Literal(value) => encode_value(A_LITERAL, value),
        Minus(a, b) => encode(A_MINUS, vec![a, b]),
        Modulo(a, b) => encode(A_MODULO, vec![a, b]),
        Multiply(a, b) => encode(A_MULTIPLY, vec![a, b]),
        NamedFx { name, params, code } =>
            encode(A_NAMED_FX, vec![&Literal(StringValue(name.into())), &ColumnSet(params.clone()), code]),
        Neg(a) => encode(A_NEG, vec![a]),
        Not(a) => encode(A_NOT, vec![a]),
        NotEqual(a, b) => encode(A_NOT_EQUAL, vec![a, b]),
        Ns(a) => encode(A_NS, vec![a]),
        Or(a, b) => encode(A_OR, vec![a, b]),
        Overwrite { table, source, condition, limit } => {
            let mut args = vec![];
            args.push(table.deref().clone());
            args.push(source.deref().clone());
            args.push(get_or_undef(condition));
            args.push(get_or_undef(limit));
            encode_vec(A_OVERWRITE, &args)
        }
        Plus(a, b) => encode(A_PLUS, vec![a, b]),
        Pow(a, b) => encode(A_POW, vec![a, b]),
        Range(a, b) => encode(A_RANGE, vec![a, b]),
        Return(a) => encode_vec(A_RETURN, a),
        Select {
            fields, from, condition,
            group_by, having,
            order_by, limit
        } => assemble_select(fields, from, condition, group_by, having, order_by, limit),
        SetVariable(name, expr) =>
            encode(A_SET_VAR, vec![&Literal(StringValue(name.into())), expr]),
        ShiftLeft(a, b) => encode(A_SHIFT_LEFT, vec![a, b]),
        ShiftRight(a, b) => encode(A_SHIFT_RIGHT, vec![a, b]),
        Truncate { table, new_size } =>
            encode(A_TRUNCATE, vec![table, &get_or_undef(new_size)]),
        TupleExpr(values) => encode_vec(A_TUPLE, values),
        Update { table, source, condition, limit } => {
            let mut args: Vec<Expression> = vec![];
            args.push(table.deref().clone());
            args.push(source.deref().clone());
            args.push(get_or_undef(condition));
            args.push(get_or_undef(limit));
            encode_vec(A_UPDATE, &args)
        }
        Variable(name) => encode(A_VAR_GET, vec![&Literal(StringValue(name.into()))]),
        Via(src) => encode(A_VIA, vec![src]),
        Xor(a, b) => encode(A_XOR, vec![a, b]),
        Where { from, condition } =>
            encode(A_WHERE, vec![from, condition]),
        While { condition, code } =>
            encode(A_WHILE, vec![condition, code]),
    }
}

/// compiles the collection of expressions into binary code
pub fn assemble_fully(expressions: &Vec<Expression>) -> Vec<u8> {
    expressions.iter().flat_map(|expr| assemble(expr)).collect::<Vec<u8>>()
}

/// compiles a JSON-like collection of expressions into binary code
fn assemble_json_object(tuples: &Vec<(String, Expression)>) -> Vec<u8> {
    let expressions = tuples.iter().flat_map(|(k, v)| {
        vec![Literal(StringValue(k.into())), v.clone()]
    }).collect::<Vec<Expression>>();
    encode_vec(A_JSON_LITERAL, &expressions)
}

/// compiles a SELECT query into binary code
fn assemble_select(
    fields: &Vec<Expression>,
    from: &Option<Box<Expression>>,
    condition: &Option<Box<Expression>>,
    group_by: &Option<Vec<Expression>>,
    having: &Option<Box<Expression>>,
    order_by: &Option<Vec<Expression>>,
    limit: &Option<Box<Expression>>,
) -> Vec<u8> {
    let mut byte_code = vec![A_SELECT];
    byte_code.extend(encode_array(fields));
    byte_code.extend(encode_opt_as_array(from));
    byte_code.extend(encode_opt_as_array(condition));
    byte_code.extend(encode_array_opt(group_by));
    byte_code.extend(encode_opt_as_array(having));
    byte_code.extend(encode_array_opt(order_by));
    byte_code.extend(encode_opt_as_array(limit));
    byte_code
}

/// decodes the typed value based on the supplied data type and buffer
pub fn assemble_type(data_type: &DataType) -> Vec<u8> {
    match data_type {
        BLOBType(size) => assemble_bytes(T_BLOB, &assemble_usize(*size)),
        BooleanType => assemble_bytes(T_BOOLEAN, &vec![]),
        CLOBType(size) => assemble_bytes(T_CLOB, &assemble_usize(*size)),
        DateType => assemble_bytes(T_DATE, &vec![]),
        EnumType(labels) => assemble_strings(T_ENUM, &labels),
        Float32Type => assemble_bytes(T_FLOAT32, &vec![]),
        Float64Type => assemble_bytes(T_FLOAT64, &vec![]),
        Int8Type => assemble_bytes(T_INT8, &vec![]),
        Int16Type => assemble_bytes(T_INT16, &vec![]),
        Int32Type => assemble_bytes(T_INT32, &vec![]),
        Int64Type => assemble_bytes(T_INT64, &vec![]),
        RecordNumberType => assemble_bytes(T_RECORD_NUMBER, &vec![]),
        StringType(size) => assemble_bytes(T_STRING, &assemble_usize(*size)),
        StructureType(columns) => assemble_bytes(T_STRUCTURE, &encode_columns(columns)),
        TableType(columns) => assemble_bytes(T_TABLE, &encode_columns(columns)),
        UUIDType => assemble_bytes(T_UUID, &vec![]),
    }
}

fn assemble_usize(value: usize) -> Vec<u8> {
    usize::to_be_bytes(value).to_vec()
}

/// decodes the typed value based on the supplied data type and buffer
pub fn assemble_value(value: &TypedValue) -> Vec<u8> {
    let mut byte_code = vec![value.ordinal()];
    byte_code.extend(TypedValue::encode_value(value));
    byte_code
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
        A_AND => Ok(And(decode_box(buf)?, decode_box(buf)?)),
        A_ARRAY_LIT => Ok(ArrayLiteral(decode_array(buf)?)),
        A_AS_VALUE => Ok(AsValue(buf.next_string(), decode_box(buf)?)),
        A_BETWEEN => Ok(Between(decode_box(buf)?, decode_box(buf)?, decode_box(buf)?)),
        A_BITWISE_AND => Ok(BitwiseAnd(decode_box(buf)?, decode_box(buf)?)),
        A_BITWISE_OR => Ok(BitwiseOr(decode_box(buf)?, decode_box(buf)?)),
        A_CODE_BLOCK => Ok(CodeBlock(decode_array(buf)?)),
        A_CONTAINS => Ok(Contains(decode_box(buf)?, decode_box(buf)?)),
        A_CREATE_INDEX =>
            Ok(CreateIndex {
                index: decode_box(buf)?,
                columns: decode_box(buf)?,
            }),
        A_CREATE_TABLE =>
            Ok(CreateTable {
                table: decode_box(buf)?,
                columns: buf.next_columns(),
                from: decode_opt(buf)?,
            }),
        A_DELETE =>
            Ok(Delete {
                table: decode_box(buf)?,
                condition: decode_opt(buf)?,
                limit: decode_opt(buf)?,
            }),
        A_DIVIDE => Ok(Divide(decode_box(buf)?, decode_box(buf)?)),
        A_DROP => Ok(Drop { table: decode_box(buf)? }),
        A_EQUAL => Ok(Equal(decode_box(buf)?, decode_box(buf)?)),
        A_FACTORIAL => Ok(Factorial(decode_box(buf)?)),
        A_FROM => Ok(From(decode_box(buf)?)),
        A_GREATER_OR_EQUAL => Ok(GreaterOrEqual(decode_box(buf)?, decode_box(buf)?)),
        A_GREATER_THAN => Ok(GreaterThan(decode_box(buf)?, decode_box(buf)?)),
        A_IF => Ok(If { condition: decode_box(buf)?, a: decode_box(buf)?, b: decode_opt(buf)? }),
        A_INTO => Ok(InsertInto { table: decode_box(buf)?, source: decode_box(buf)? }),
        A_JSON_LITERAL => Ok(JSONLiteral(decode_json_object(buf)?)),
        A_LESS_OR_EQUAL => Ok(LessOrEqual(decode_box(buf)?, decode_box(buf)?)),
        A_LESS_THAN => Ok(LessThan(decode_box(buf)?, decode_box(buf)?)),
        A_LIMIT => Ok(Limit { from: decode_box(buf)?, limit: decode_box(buf)? }),
        A_LITERAL => Ok(Literal(buf.next_value())),
        A_MINUS => Ok(Minus(decode_box(buf)?, decode_box(buf)?)),
        A_MODULO => Ok(Modulo(decode_box(buf)?, decode_box(buf)?)),
        A_MULTIPLY => Ok(Multiply(decode_box(buf)?, decode_box(buf)?)),
        A_NEG => Ok(Neg(decode_box(buf)?)),
        A_NOT => Ok(Not(decode_box(buf)?)),
        A_NOT_EQUAL => Ok(NotEqual(decode_box(buf)?, decode_box(buf)?)),
        A_OR => Ok(Or(decode_box(buf)?, decode_box(buf)?)),
        A_OVERWRITE => Ok(Overwrite {
            table: decode_box(buf)?,
            source: decode_box(buf)?,
            condition: decode_opt(buf)?,
            limit: decode_opt(buf)?,
        }),
        A_PLUS => Ok(Plus(decode_box(buf)?, decode_box(buf)?)),
        A_POW => Ok(Pow(decode_box(buf)?, decode_box(buf)?)),
        A_RANGE => Ok(Range(decode_box(buf)?, decode_box(buf)?)),
        A_RETURN => Ok(Return(decode_array(buf)?)),
        A_SELECT => disassemble_select(buf),
        A_SET_VAR =>
            match disassemble(buf)? {
                Literal(StringValue(name)) => Ok(SetVariable(name, decode_box(buf)?)),
                z => fail_expr("Expected String", &z)
            }
        A_SHIFT_LEFT => Ok(ShiftLeft(decode_box(buf)?, decode_box(buf)?)),
        A_SHIFT_RIGHT => Ok(ShiftRight(decode_box(buf)?, decode_box(buf)?)),
        A_TRUNCATE => Ok(Truncate { table: decode_box(buf)?, new_size: decode_opt(buf)? }),
        A_TUPLE => Ok(TupleExpr(decode_array(buf)?)),
        A_UPDATE => disassemble_update(buf),
        A_VAR_GET =>
            match disassemble(buf)? {
                Literal(StringValue(name)) => Ok(Variable(name)),
                z => fail_expr("Expected identifier", &z)
            }
        A_VIA => Ok(Via(decode_box(buf)?)),
        A_XOR => Ok(Xor(decode_box(buf)?, decode_box(buf)?)),
        A_WHERE => Ok(Where { from: decode_box(buf)?, condition: decode_box(buf)? }),
        A_WHILE => Ok(While { condition: decode_box(buf)?, code: decode_box(buf)? }),
        x => fail(format!("Invalid expression code {}", x))
    }
}

pub fn disassemble_fully(byte_code: &Vec<u8>) -> std::io::Result<Vec<Expression>> {
    let mut models = vec![];
    let mut buf = ByteBuffer::wrap(byte_code.clone());
    while buf.has_next() {
        models.push(disassemble(&mut buf)?);
    }
    Ok(models)
}

fn disassemble_select(buf: &mut ByteBuffer) -> std::io::Result<Expression> {
    let fields = decode_array(buf)?;
    let from = decode_array_as_opt(buf)?;
    let condition = decode_array_as_opt(buf)?;
    let group_by = decode_array_opt(buf)?;
    let having = decode_array_as_opt(buf)?;
    let order_by = decode_array_opt(buf)?;
    let limit = decode_array_as_opt(buf)?;
    Ok(Select { fields, from, condition, group_by, having, order_by, limit })
}

fn disassemble_update(buf: &mut ByteBuffer) -> std::io::Result<Expression> {
    let args = decode_array(buf)?;
    if args.len() < 2 {
        return fail(format!("Invalid args: expected 4, got {}", args.len()));
    }
    let table = Box::new(args[0].clone());
    let source = Box::new(args[1].clone());
    let condition = if args.len() > 2 { Some(Box::new(args[2].clone())) } else { None };
    let limit = if args.len() > 3 { Some(Box::new(args[3].clone())) } else { None };
    Ok(Update { table, source, condition, limit })
}

fn decode_array(buf: &mut ByteBuffer) -> std::io::Result<Vec<Expression>> {
    let count = buf.next_u64();
    let mut array = vec![];
    for _ in 0..count {
        let expr = disassemble(buf)?;
        array.push(expr);
    }
    Ok(array)
}

fn decode_array_as_opt(buf: &mut ByteBuffer) -> std::io::Result<Option<Box<Expression>>> {
    let ops = decode_array(buf)?;
    if ops.is_empty() { Ok(None) } else { Ok(Some(Box::new(ops[0].clone()))) }
}

fn decode_array_opt(buf: &mut ByteBuffer) -> std::io::Result<Option<Vec<Expression>>> {
    let array = decode_array(buf)?;
    Ok(if array.is_empty() { None } else { Some(array) })
}

fn decode_box(buf: &mut ByteBuffer) -> std::io::Result<Box<Expression>> {
    Ok(Box::new(disassemble(buf)?))
}

fn decode_json_object(buf: &mut ByteBuffer) -> std::io::Result<Vec<(String, Expression)>> {
    let count = buf.next_u64() / 2;
    let mut tuples = vec![];
    for _ in 0..count {
        match disassemble(buf)? {
            Literal(StringValue(k)) => {
                let v = disassemble(buf)?;
                tuples.push((k, v))
            }
            z => panic!("Expected String, but got {}", z)
        }
    }
    Ok(tuples)
}

fn decode_value(buf: &mut ByteBuffer) -> std::io::Result<TypedValue> {
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

fn decode_data_type(buf: &mut ByteBuffer) -> std::io::Result<DataType> {
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

fn decode_opt(buf: &mut ByteBuffer) -> std::io::Result<Option<Box<Expression>>> {
    match buf.next_u8() {
        V_UNDEFINED => Ok(None),
        _ => {
            buf.move_rel(-1);
            Ok(Some(decode_box(buf)?))
        }
    }
}

fn encode(id: u8, args: Vec<&Expression>) -> Vec<u8> {
    let mut byte_code = vec![id];
    for expr in args {
        let code = assemble(expr);
        byte_code.extend(code);
    }
    byte_code
}

fn encode_array(ops: &Vec<Expression>) -> Vec<u8> {
    let mut byte_code = vec![];
    byte_code.extend((ops.len() as u64).to_be_bytes());
    byte_code.extend(ops.iter().flat_map(assemble).collect::<Vec<u8>>());
    byte_code
}

fn encode_opt_as_array(opt: &Option<Box<Expression>>) -> Vec<u8> {
    let ops = if opt.is_none() { vec![] } else {
        vec![opt.clone().unwrap().deref().clone()]
    };
    encode_array(&ops)
}

fn encode_array_opt(ops_opt: &Option<Vec<Expression>>) -> Vec<u8> {
    let ops = ops_opt.clone().unwrap_or(vec![]);
    encode_array(&ops)
}

fn assemble_bytes(id: u8, code: &Vec<u8>) -> Vec<u8> {
    let mut byte_code = vec![id];
    byte_code.extend(code);
    byte_code
}

fn encode_columns(columns: &Vec<ColumnJs>) -> Vec<u8> {
    columns.iter().flat_map(|c| c.encode()).collect::<Vec<u8>>()
}

fn assemble_strings(id: u8, strings: &Vec<String>) -> Vec<u8> {
    let mut byte_code = vec![id];
    byte_code.extend((strings.len() as u64).to_be_bytes());
    for string in strings {
        byte_code.extend(string.len().to_be_bytes());
        byte_code.extend(string.bytes())
    }
    byte_code
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

fn get_or_undef(opt: &Option<Box<Expression>>) -> Expression {
    opt.clone().unwrap_or(Box::new(UNDEFINED)).deref().clone()
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
            A_VAR_GET, A_LITERAL, V_STRING, 0, 0, 0, 0, 0, 0, 0, 4, b'n', b'a', b'm', b'e',
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
            A_VAR_GET, A_LITERAL, V_STRING, 0, 0, 0, 0, 0, 0, 0, 6,
            b's', b't', b'o', b'c', b'k', b's',
            A_LESS_OR_EQUAL,
            A_VAR_GET, A_LITERAL, V_STRING, 0, 0, 0, 0, 0, 0, 0, 9,
            b'l', b'a', b's', b't', b'_', b's', b'a', b'l', b'e',
            A_LITERAL, V_FLOAT64, 63, 240, 0, 0, 0, 0, 0, 0,
            A_LITERAL, V_INT64, 0, 0, 0, 0, 0, 0, 0, 100,
        ];
        assert_eq!(assemble(&model), byte_code);
        assert_eq!(disassemble(&mut ByteBuffer::wrap(byte_code)).unwrap(), model)
    }

    #[test]
    fn test_select() {
        let model = Select {
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
            order_by: Some(vec![Variable("symbol".into())]),
            limit: Some(Box::new(Literal(Int64Value(5)))),
        };
        let byte_code = vec![
            // select symbol, exchange, last_sale
            A_SELECT, 0, 0, 0, 0, 0, 0, 0, 3,
            A_VAR_GET, A_LITERAL, V_STRING, 0, 0, 0, 0, 0, 0, 0, 6, b's', b'y', b'm', b'b', b'o', b'l',
            A_VAR_GET, A_LITERAL, V_STRING, 0, 0, 0, 0, 0, 0, 0, 8, b'e', b'x', b'c', b'h', b'a', b'n', b'g', b'e',
            A_VAR_GET, A_LITERAL, V_STRING, 0, 0, 0, 0, 0, 0, 0, 9, b'l', b'a', b's', b't', b'_', b's', b'a', b'l', b'e',
            // from stocks
            0, 0, 0, 0, 0, 0, 0, 1,
            A_VAR_GET, A_LITERAL, V_STRING, 0, 0, 0, 0, 0, 0, 0, 6, b's', b't', b'o', b'c', b'k', b's',
            // where last_sale <= 1.0
            0, 0, 0, 0, 0, 0, 0, 1,
            A_LESS_OR_EQUAL,
            A_VAR_GET, A_LITERAL, V_STRING, 0, 0, 0, 0, 0, 0, 0, 9, b'l', b'a', b's', b't', b'_', b's', b'a', b'l', b'e',
            A_LITERAL, V_FLOAT64, 63, 240, 0, 0, 0, 0, 0, 0, // 1.0
            // group by ...
            0, 0, 0, 0, 0, 0, 0, 0,
            // having ...
            0, 0, 0, 0, 0, 0, 0, 0,
            // order by symbol
            0, 0, 0, 0, 0, 0, 0, 1,
            A_VAR_GET, A_LITERAL, V_STRING, 0, 0, 0, 0, 0, 0, 0, 6, b's', b'y', b'm', b'b', b'o', b'l',
            // limit 5
            0, 0, 0, 0, 0, 0, 0, 1,
            A_LITERAL, V_INT64, 0, 0, 0, 0, 0, 0, 0, 5,
        ];
        assert_eq!(assemble(&model), byte_code);
        assert_eq!(disassemble(&mut ByteBuffer::wrap(byte_code)).unwrap(), model)
    }

    #[test]
    fn test_update() {
        let model = Update {
            table: Box::new(Variable("stocks".into())),
            source: Box::new(Via(Box::new(JSONLiteral(vec![
                ("last_sale".into(), Literal(Float64Value(0.1111))),
            ])))),
            condition: Some(
                Box::new(Equal(
                    Box::new(Variable("exchange".into())),
                    Box::new(Literal(StringValue("NYSE".into()))),
                ))
            ),
            limit: Some(Box::new(Literal(Int64Value(10)))),
        };
        let byte_code = vec![
            // update stocks
            A_UPDATE, 0, 0, 0, 0, 0, 0, 0, 4,
            A_VAR_GET, A_LITERAL, V_STRING, 0, 0, 0, 0, 0, 0, 0, 6, b's', b't', b'o', b'c', b'k', b's',
            // via { last_sale: 0.1111 }
            A_VIA, V_INT64, 0, 0, 0, 0, 0, 0, 0, 2,
            A_LITERAL, V_STRING, 0, 0, 0, 0, 0, 0, 0, 9, b'l', b'a', b's', b't', b'_', b's', b'a', b'l', b'e',
            A_LITERAL, V_FLOAT64, 63, 188, 113, 12, 178, 149, 233, 226, // 0.1111
            // where symbol == 'ABC'
            A_EQUAL,
            A_VAR_GET, A_LITERAL, V_STRING, 0, 0, 0, 0, 0, 0, 0, 8, b'e', b'x', b'c', b'h', b'a', b'n', b'g', b'e',
            A_LITERAL, V_STRING, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E',
            // limit 10
            A_LITERAL, V_INT64, 0, 0, 0, 0, 0, 0, 0, 10,
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
    fn test_json_literal() {
        let model = JSONLiteral(vec![
            ("symbol".to_string(), Literal(StringValue("TRX".into()))),
            ("exchange".to_string(), Literal(StringValue("NYSE".into()))),
            ("last_sale".to_string(), Literal(Float64Value(11.1111))),
        ]);
        let byte_code = vec![
            A_JSON_LITERAL, 0, 0, 0, 0, 0, 0, 0, 6,
            A_LITERAL, V_STRING, 0, 0, 0, 0, 0, 0, 0, 6, b's', b'y', b'm', b'b', b'o', b'l',
            A_LITERAL, V_STRING, 0, 0, 0, 0, 0, 0, 0, 3, b'T', b'R', b'X',
            A_LITERAL, V_STRING, 0, 0, 0, 0, 0, 0, 0, 8, b'e', b'x', b'c', b'h', b'a', b'n', b'g', b'e',
            A_LITERAL, V_STRING, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E',
            A_LITERAL, V_STRING, 0, 0, 0, 0, 0, 0, 0, 9, b'l', b'a', b's', b't', b'_', b's', b'a', b'l', b'e',
            A_LITERAL, V_FLOAT64, 64, 38, 56, 226, 25, 101, 43, 212,
        ];
        assert_eq!(assemble(&model), byte_code);
        assert_eq!(disassemble(&mut ByteBuffer::wrap(byte_code)).unwrap(), model);
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