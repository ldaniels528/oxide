////////////////////////////////////////////////////////////////////
// serialization module
////////////////////////////////////////////////////////////////////

use std::ops::Deref;

use shared_lib::fail;

use crate::byte_buffer::ByteBuffer;
use crate::compiler::fail_expr;
use crate::data_types::*;
use crate::data_types::DataType::*;
use crate::expression::*;
use crate::expression::CreationEntity::{IndexEntity, TableEntity};
use crate::expression::Expression::*;
use crate::expression::MutateTarget::{IndexTarget, TableTarget};
use crate::expression::Mutation::IntoTable;
use crate::machine::Machine;
use crate::server::ColumnJs;
use crate::typed_values::*;
use crate::typed_values::TypedValue::*;

/// compiles the expression into binary code
pub fn assemble(expression: &Expression) -> Vec<u8> {
    use crate::expression::Expression::*;
    match expression {
        And(a, b) => encode(E_AND, vec![a, b]),
        ArrayLiteral(items) => encode_vec(E_ARRAY_LIT, items),
        AsValue(name, expr) =>
            encode(E_AS_VALUE, vec![&Literal(StringValue(name.to_string())), expr]),
        Between(a, b, c) =>
            encode(E_BETWEEN, vec![a, b, c]),
        Betwixt(a, b, c) =>
            encode(E_BETWIXT, vec![a, b, c]),
        BitwiseAnd(a, b) =>
            encode(E_BITWISE_AND, vec![a, b]),
        BitwiseOr(a, b) =>
            encode(E_BITWISE_OR, vec![a, b]),
        BitwiseXor(a, b) =>
            encode(E_BITWISE_XOR, vec![a, b]),
        CodeBlock(ops) => encode_vec(E_CODE_BLOCK, ops),
        ColumnSet(columns) => encode_columns(columns),
        Contains(a, b) => encode(E_CONTAINS, vec![a, b]),
        Divide(a, b) => encode(E_DIVIDE, vec![a, b]),
        Equal(a, b) => encode(E_EQUAL, vec![a, b]),
        Eval(a) => encode(E_EVAL, vec![a]),
        Factorial(a) => encode(E_FACTORIAL, vec![a]),
        From(src) => encode(E_FROM, vec![src]),
        FunctionCall { fx, args } => {
            let mut values = vec![fx.deref()];
            values.extend(args);
            encode(E_WHILE, values)
        }
        GreaterThan(a, b) => encode(E_GREATER_THAN, vec![a, b]),
        GreaterOrEqual(a, b) => encode(E_GREATER_OR_EQUAL, vec![a, b]),
        If { condition, a, b } =>
            encode(E_IF, vec![condition, a, &get_or_undef(b)]),
        Include(path) => encode(E_INCLUDE, vec![path]),
        Inquire(q) => assemble_inquiry(q),
        JSONLiteral(tuples) => assemble_json_object(tuples),
        LessThan(a, b) => encode(E_LESS_THAN, vec![a, b]),
        LessOrEqual(a, b) => encode(E_LESS_OR_EQUAL, vec![a, b]),
        Literal(value) => encode_value(E_LITERAL, value),
        Minus(a, b) => encode(E_MINUS, vec![a, b]),
        Modulo(a, b) => encode(E_MODULO, vec![a, b]),
        Multiply(a, b) => encode(E_MULTIPLY, vec![a, b]),
        MustAck(a) => encode(E_MUST_ACK, vec![a]),
        MustDie(a) => encode(E_MUST_DIE, vec![a]),
        MustIgnoreAck(a) => encode(E_MUST_IGNORE_ACK, vec![a]),
        MustNotAck(a) => encode(E_MUST_NOT_ACK, vec![a]),
        Mutate(m) => assemble_modification(m),
        Neg(a) => encode(E_NEG, vec![a]),
        Not(a) => encode(E_NOT, vec![a]),
        NotEqual(a, b) => encode(E_NOT_EQUAL, vec![a, b]),
        Ns(a) => encode(E_NS, vec![a]),
        Or(a, b) => encode(E_OR, vec![a, b]),
        Perform(i) => assemble_infrastructure(i),
        Plus(a, b) => encode(E_PLUS, vec![a, b]),
        Pow(a, b) => encode(E_POW, vec![a, b]),
        Range(a, b) => encode(E_RANGE, vec![a, b]),
        Return(a) => encode_vec(E_RETURN, a),
        SetVariable(name, expr) =>
            encode(E_VAR_SET, vec![&Literal(StringValue(name.into())), expr]),
        ShiftLeft(a, b) => encode(E_SHIFT_LEFT, vec![a, b]),
        ShiftRight(a, b) => encode(E_SHIFT_RIGHT, vec![a, b]),
        StdErr(a) => encode(E_STDERR, vec![a]),
        StdOut(a) => encode(E_STDOUT, vec![a]),
        SystemCall(args) => {
            let mut my_args = vec![];
            for arg in args { my_args.push(arg) }
            encode(E_SYSTEM_CALL, my_args)
        }
        TupleLiteral(values) => encode_vec(E_TUPLE, values),
        Variable(name) => encode(E_VAR_GET, vec![&Literal(StringValue(name.into()))]),
        Via(src) => encode(E_VIA, vec![src]),
        While { condition, code } =>
            encode(E_WHILE, vec![condition, code]),
    }
}


/// compiles the [Infrastructure] into binary code
pub fn assemble_infrastructure(expression: &Infrastructure) -> Vec<u8> {
    use Infrastructure::*;
    match expression {
        Create { path, entity: IndexEntity { columns } } => {
            let mut args = vec![path.deref()];
            args.extend(columns);
            encode(E_CREATE_INDEX, args)
        }
        Create { path, entity: TableEntity { columns, from } } =>
            encode(E_CREATE_TABLE, vec![path, &ColumnSet(columns.clone()), &get_or_undef(from)]),
        Declare(IndexEntity { columns }) => {
            let mut args = vec![];
            args.extend(columns);
            encode(E_DECLARE_INDEX, args)
        }
        Declare(TableEntity { columns, from }) =>
            encode(E_DECLARE_TABLE, vec![&ColumnSet(columns.clone()), &get_or_undef(from)]),
        Drop(IndexTarget { path }) => encode(E_DROP, vec![path]),
        Drop(TableTarget { path }) => encode(E_DROP, vec![path]),
    }
}

/// compiles the [Mutation] into binary code
pub fn assemble_modification(expression: &Mutation) -> Vec<u8> {
    use Mutation::*;
    match expression {
        Append { path, source } => encode(E_APPEND, vec![path, source]),
        Delete { path, condition, limit } =>
            encode(E_DELETE, vec![path, &get_or_undef(condition), &get_or_undef(limit)]),
        IntoTable(a, b) => encode(E_INTO_TABLE, vec![a, b]),
        Overwrite { path, source, condition, limit } => {
            let mut args = vec![];
            args.push(path.deref().clone());
            args.push(source.deref().clone());
            args.push(get_or_undef(condition));
            args.push(get_or_undef(limit));
            encode_vec(E_OVERWRITE, &args)
        }
        Truncate { path, limit: new_size } =>
            encode(E_TRUNCATE, vec![path, &get_or_undef(new_size)]),
        Update { path, source, condition, limit } => {
            let mut args: Vec<Expression> = vec![];
            args.push(path.deref().clone());
            args.push(source.deref().clone());
            args.push(get_or_undef(condition));
            args.push(get_or_undef(limit));
            encode_vec(E_UPDATE, &args)
        }
    }
}

/// compiles the [Queryable] into binary code
pub fn assemble_inquiry(expression: &Queryable) -> Vec<u8> {
    use Queryable::*;
    match expression {
        Limit { from, limit } => encode(E_LIMIT, vec![from, limit]),
        Reverse(a) => encode(E_REVERSE, vec![a]),
        Select {
            fields, from, condition,
            group_by, having,
            order_by, limit
        } => assemble_select(fields, from, condition, group_by, having, order_by, limit),
        Where { from, condition } =>
            encode(E_WHERE, vec![from, condition]),
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
    encode_vec(E_JSON_LITERAL, &expressions)
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
    let mut byte_code = vec![E_SELECT];
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
        AckType => assemble_bytes(T_ACK, &vec![]),
        BLOBType(size) => assemble_bytes(T_BLOB, &assemble_usize(*size)),
        BooleanType => assemble_bytes(T_BOOLEAN, &vec![]),
        CLOBType(size) => assemble_bytes(T_CLOB, &assemble_usize(*size)),
        DateType => assemble_bytes(T_DATE, &vec![]),
        EnumType(labels) => assemble_strings(T_ENUM, &labels),
        ErrorType => assemble_bytes(T_STRING, &assemble_usize(256)),
        Float32Type => assemble_bytes(T_FLOAT32, &vec![]),
        Float64Type => assemble_bytes(T_FLOAT64, &vec![]),
        FuncType(columns) => assemble_bytes(T_FUNCTION, &encode_columns(columns)),
        Int8Type => assemble_bytes(T_INT8, &vec![]),
        Int16Type => assemble_bytes(T_INT16, &vec![]),
        Int32Type => assemble_bytes(T_INT32, &vec![]),
        Int64Type => assemble_bytes(T_INT64, &vec![]),
        Int128Type => assemble_bytes(T_INT128, &vec![]),
        JSONObjectType => assemble_bytes(T_JSON_OBJECT, &vec![]),
        RowsAffectedType => assemble_bytes(T_ROWS_AFFECTED, &vec![]),
        StringType(size) => assemble_bytes(T_STRING, &assemble_usize(*size)),
        StructureType(columns) => assemble_bytes(T_STRUCT, &encode_columns(columns)),
        TableType(columns) => assemble_bytes(T_TABLE, &encode_columns(columns)),
        UInt8Type => assemble_bytes(T_UINT8, &vec![]),
        UInt16Type => assemble_bytes(T_UINT16, &vec![]),
        UInt32Type => assemble_bytes(T_UINT32, &vec![]),
        UInt64Type => assemble_bytes(T_UINT64, &vec![]),
        UInt128Type => assemble_bytes(T_UINT128, &vec![]),
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
pub fn add(machine: Machine) -> std::io::Result<Machine> {
    let (machine, y) = machine.pop_or(Undefined);
    let (machine, x) = machine.pop_or(Undefined);
    Ok(machine.push(x + y))
}

/// arithmetic decrement
pub fn dec(machine: Machine) -> std::io::Result<Machine> {
    let (machine, x) = machine.pop_or(Undefined);
    machine.transform_numeric(x, |n| Int64Value(n - 1), |n| Float64Value(n - 1.))
}

/// arithmetic division
pub fn div(machine: Machine) -> std::io::Result<Machine> {
    let (machine, y) = machine.pop_or(Undefined);
    let (machine, x) = machine.pop_or(Undefined);
    Ok(machine.push(x / y))
}

pub fn disassemble(buf: &mut ByteBuffer) -> std::io::Result<Expression> {
    use Expression::*;
    match buf.next_u8() {
        E_AND => Ok(And(decode_box(buf)?, decode_box(buf)?)),
        E_APPEND => Ok(Mutate(Mutation::Append { path: decode_box(buf)?, source: decode_box(buf)? })),
        E_ARRAY_LIT => Ok(ArrayLiteral(decode_array(buf)?)),
        E_AS_VALUE => Ok(AsValue(buf.next_string(), decode_box(buf)?)),
        E_BETWEEN => Ok(Between(decode_box(buf)?, decode_box(buf)?, decode_box(buf)?)),
        E_BETWIXT => Ok(Betwixt(decode_box(buf)?, decode_box(buf)?, decode_box(buf)?)),
        E_BITWISE_AND => Ok(BitwiseAnd(decode_box(buf)?, decode_box(buf)?)),
        E_BITWISE_OR => Ok(BitwiseOr(decode_box(buf)?, decode_box(buf)?)),
        E_CODE_BLOCK => Ok(CodeBlock(decode_array(buf)?)),
        E_CONTAINS => Ok(Contains(decode_box(buf)?, decode_box(buf)?)),
        E_CREATE_INDEX => {
            let mut args = decode_array(buf)?;
            assert!(args.len() >= 2);
            Ok(Perform(Infrastructure::Create {
                path: Box::new(args.pop().unwrap().clone()),
                entity: IndexEntity {
                    columns: args,
                },
            }))
        }
        E_CREATE_TABLE =>
            Ok(Perform(Infrastructure::Create {
                path: decode_box(buf)?,
                entity: TableEntity {
                    columns: buf.next_columns(),
                    from: decode_opt(buf)?,
                },
            })),
        E_DECLARE_INDEX =>
            Ok(Perform(Infrastructure::Declare(IndexEntity { columns: decode_array(buf)? }))),
        E_DECLARE_TABLE =>
            Ok(Perform(Infrastructure::Declare(TableEntity { columns: buf.next_columns(), from: decode_opt(buf)? }))),
        E_DELETE =>
            Ok(Mutate(Mutation::Delete {
                path: decode_box(buf)?,
                condition: decode_opt(buf)?,
                limit: decode_opt(buf)?,
            })),
        E_DIVIDE => Ok(Divide(decode_box(buf)?, decode_box(buf)?)),
        E_DROP => Ok(Perform(Infrastructure::Drop(TableTarget { path: decode_box(buf)? }))),
        E_EQUAL => Ok(Equal(decode_box(buf)?, decode_box(buf)?)),
        E_EVAL => Ok(Eval(decode_box(buf)?)),
        E_FACTORIAL => Ok(Factorial(decode_box(buf)?)),
        E_FROM => Ok(From(decode_box(buf)?)),
        E_GREATER_OR_EQUAL => Ok(GreaterOrEqual(decode_box(buf)?, decode_box(buf)?)),
        E_GREATER_THAN => Ok(GreaterThan(decode_box(buf)?, decode_box(buf)?)),
        E_IF => Ok(If { condition: decode_box(buf)?, a: decode_box(buf)?, b: decode_opt(buf)? }),
        E_INCLUDE => Ok(Include(decode_box(buf)?)),
        E_INTO_TABLE => Ok(Mutate(Mutation::IntoTable(decode_box(buf)?, decode_box(buf)?))),
        E_JSON_LITERAL => Ok(JSONLiteral(decode_json_object(buf)?)),
        E_LESS_OR_EQUAL => Ok(LessOrEqual(decode_box(buf)?, decode_box(buf)?)),
        E_LESS_THAN => Ok(LessThan(decode_box(buf)?, decode_box(buf)?)),
        E_LIMIT => Ok(Inquire(Queryable::Limit { from: decode_box(buf)?, limit: decode_box(buf)? })),
        E_LITERAL => Ok(Literal(buf.next_value()?)),
        E_MINUS => Ok(Minus(decode_box(buf)?, decode_box(buf)?)),
        E_MODULO => Ok(Modulo(decode_box(buf)?, decode_box(buf)?)),
        E_MULTIPLY => Ok(Multiply(decode_box(buf)?, decode_box(buf)?)),
        E_MUST_ACK => Ok(MustAck(decode_box(buf)?)),
        E_MUST_DIE => Ok(MustDie(decode_box(buf)?)),
        E_MUST_IGNORE_ACK => Ok(MustIgnoreAck(decode_box(buf)?)),
        E_MUST_NOT_ACK => Ok(MustNotAck(decode_box(buf)?)),
        E_NEG => Ok(Neg(decode_box(buf)?)),
        E_NOT => Ok(Not(decode_box(buf)?)),
        E_NOT_EQUAL => Ok(NotEqual(decode_box(buf)?, decode_box(buf)?)),
        E_NS => Ok(Ns(decode_box(buf)?)),
        E_OR => Ok(Or(decode_box(buf)?, decode_box(buf)?)),
        E_OVERWRITE => Ok(Mutate(Mutation::Overwrite {
            path: decode_box(buf)?,
            source: decode_box(buf)?,
            condition: decode_opt(buf)?,
            limit: decode_opt(buf)?,
        })),
        E_PLUS => Ok(Plus(decode_box(buf)?, decode_box(buf)?)),
        E_POW => Ok(Pow(decode_box(buf)?, decode_box(buf)?)),
        E_RANGE => Ok(Range(decode_box(buf)?, decode_box(buf)?)),
        E_RETURN => Ok(Return(decode_array(buf)?)),
        E_REVERSE => Ok(Inquire(Queryable::Reverse(decode_box(buf)?))),
        E_SELECT => disassemble_select(buf),
        E_STDERR => Ok(StdErr(decode_box(buf)?)),
        E_STDOUT => Ok(StdErr(decode_box(buf)?)),
        E_SYSTEM_CALL => Ok(SystemCall(decode_array(buf)?)),
        E_VAR_SET =>
            match disassemble(buf)? {
                Literal(StringValue(name)) => Ok(SetVariable(name, decode_box(buf)?)),
                z => fail_expr("Expected String", &z)
            }
        E_SHIFT_LEFT => Ok(ShiftLeft(decode_box(buf)?, decode_box(buf)?)),
        E_SHIFT_RIGHT => Ok(ShiftRight(decode_box(buf)?, decode_box(buf)?)),
        E_TRUNCATE => Ok(Mutate(Mutation::Truncate { path: decode_box(buf)?, limit: decode_opt(buf)? })),
        E_TUPLE => Ok(TupleLiteral(decode_array(buf)?)),
        E_UPDATE => disassemble_update(buf),
        E_VAR_GET =>
            match disassemble(buf)? {
                Literal(StringValue(name)) => Ok(Variable(name)),
                z => fail_expr("Expected identifier", &z)
            }
        E_VIA => Ok(Via(decode_box(buf)?)),
        E_BITWISE_XOR => Ok(BitwiseXor(decode_box(buf)?, decode_box(buf)?)),
        E_WHERE => Ok(Inquire(Queryable::Where { from: decode_box(buf)?, condition: decode_box(buf)? })),
        E_WHILE => Ok(While { condition: decode_box(buf)?, code: decode_box(buf)? }),
        z => fail(format!("Invalid expression code {}", z))
    }
}

pub fn disassemble_fully(byte_code: &Vec<u8>) -> std::io::Result<Expression> {
    let mut models = vec![];
    let mut buf = ByteBuffer::wrap(byte_code.clone());
    while buf.has_next() {
        models.push(disassemble(&mut buf)?);
    }
    let opcode = if models.len() == 1 { models[0].clone() } else { CodeBlock(models) };
    Ok(opcode)
}

fn disassemble_select(buf: &mut ByteBuffer) -> std::io::Result<Expression> {
    let fields = decode_array(buf)?;
    let from = decode_array_as_opt(buf)?;
    let condition = decode_array_as_opt(buf)?;
    let group_by = decode_array_opt(buf)?;
    let having = decode_array_as_opt(buf)?;
    let order_by = decode_array_opt(buf)?;
    let limit = decode_array_as_opt(buf)?;
    Ok(Inquire(Queryable::Select { fields, from, condition, group_by, having, order_by, limit }))
}

fn disassemble_update(buf: &mut ByteBuffer) -> std::io::Result<Expression> {
    let args = decode_array(buf)?;
    if args.len() < 2 {
        return fail(format!("Invalid args: expected 4, got {}", args.len()));
    }
    let path = Box::new(args[0].clone());
    let source = Box::new(args[1].clone());
    let condition = if args.len() > 2 { Some(Box::new(args[2].clone())) } else { None };
    let limit = if args.len() > 3 { Some(Box::new(args[3].clone())) } else { None };
    Ok(Mutate(Mutation::Update { path, source, condition, limit }))
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
            T_UNDEFINED => Ok(Undefined),
            T_NULL => Ok(Null),
            T_BLOB => Ok(BLOB(buf.next_blob())),
            T_BOOLEAN => Ok(Boolean(buf.next_bool())),
            T_CLOB => Ok(CLOB(buf.next_clob())),
            T_DATE => Ok(DateValue(buf.next_i64())),
            T_FLOAT32 => Ok(Float32Value(buf.next_f32())),
            T_FLOAT64 => Ok(Float64Value(buf.next_f64())),
            T_INT8 => Ok(Int8Value(buf.next_i8())),
            T_INT16 => Ok(Int16Value(buf.next_i16())),
            T_INT32 => Ok(Int32Value(buf.next_i32())),
            T_INT64 => Ok(Int64Value(buf.next_i64())),
            T_INT128 => Ok(Int128Value(buf.next_i128())),
            T_STRING => Ok(StringValue(buf.next_string())),
            T_UINT8 => Ok(UInt8Value(buf.next_u8())),
            T_UINT16 => Ok(UInt16Value(buf.next_u16())),
            T_UINT32 => Ok(UInt32Value(buf.next_u32())),
            T_UINT64 => Ok(UInt64Value(buf.next_u64())),
            T_UINT128 => Ok(UInt128Value(buf.next_u128())),
            T_UUID => Ok(UUIDValue(buf.next_uuid())),
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
        T_INT128 => Ok(Int128Type),
        T_JSON_OBJECT => Ok(JSONObjectType),
        T_ROWS_AFFECTED => Ok(RowsAffectedType),
        T_STRING => Ok(StringType(buf.next_u32() as usize)),
        T_TABLE => Ok(TableType(buf.next_columns())),
        T_UINT8 => Ok(UInt8Type),
        T_UINT16 => Ok(UInt16Type),
        T_UINT32 => Ok(UInt32Type),
        T_UINT64 => Ok(UInt64Type),
        T_UINT128 => Ok(UInt128Type),
        T_UUID => Ok(UUIDType),
        x => fail(format!("Unhandled type code {}", x))
    }
}

fn decode_opt(buf: &mut ByteBuffer) -> std::io::Result<Option<Box<Expression>>> {
    match buf.next_u8() {
        T_UNDEFINED => Ok(None),
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
pub fn fact(machine: Machine) -> std::io::Result<Machine> {
    fn fact_i64(n: i64) -> TypedValue {
        fn fact_i(n: i64) -> i64 { if n <= 1 { 1 } else { n * fact_i(n - 1) } }
        Int64Value(fact_i(n))
    }

    fn fact_f64(n: f64) -> TypedValue {
        fn fact_f(n: f64) -> f64 { if n <= 1. { 1. } else { n * fact_f(n - 1.) } }
        Float64Value(fact_f(n))
    }

    let (machine, number) = machine.pop_or(Undefined);
    machine.transform_numeric(number, fact_i64, fact_f64)
}

fn get_or_undef(opt: &Option<Box<Expression>>) -> Expression {
    opt.clone().unwrap_or(Box::new(UNDEFINED)).deref().clone()
}

/// arithmetic increment
pub fn inc(machine: Machine) -> std::io::Result<Machine> {
    let (machine, x) = machine.pop_or(Undefined);
    machine.transform_numeric(x, |n| Int64Value(n + 1), |n| Float64Value(n + 1.))
}

/// arithmetic multiplication
pub fn mul(machine: Machine) -> std::io::Result<Machine> {
    let (machine, y) = machine.pop_or(Undefined);
    let (machine, x) = machine.pop_or(Undefined);
    Ok(machine.push(x * y))
}

/// arithmetic subtraction
pub fn sub(machine: Machine) -> std::io::Result<Machine> {
    let (machine, y) = machine.pop_or(Undefined);
    let (machine, x) = machine.pop_or(Undefined);
    Ok(machine.push(x - y))
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::expression::Expression::*;
    use crate::machine::Machine;
    use crate::typed_values::TypedValue::{Float64Value, Int32Value, Int64Value};
    use crate::typed_values::TypedValue::*;

    use super::*;

    #[test]
    fn test_literal() {
        let model = Literal(StringValue("hello".into()));
        let byte_code = vec![
            E_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 5, b'h', b'e', b'l', b'l', b'o',
        ];
        assert_eq!(assemble(&model), byte_code);
        assert_eq!(disassemble(&mut ByteBuffer::wrap(byte_code)).unwrap(), model);
    }

    #[test]
    fn test_variable() {
        let model = Variable("name".into());
        let byte_code = vec![
            E_VAR_GET, E_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 4, b'n', b'a', b'm', b'e',
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
            E_PLUS,
            E_LITERAL, T_INT64, 0, 0, 0, 0, 0, 0, 0, 2,
            E_MULTIPLY,
            E_LITERAL, T_INT64, 0, 0, 0, 0, 0, 0, 0, 4,
            E_LITERAL, T_INT64, 0, 0, 0, 0, 0, 0, 0, 3,
        ];
        let mut buf = ByteBuffer::wrap(byte_code.clone());
        assert_eq!(assemble(&model), byte_code);
        assert_eq!(disassemble(&mut buf).unwrap(), model);
    }

    #[test]
    fn test_delete() {
        let model = Mutate(Mutation::Delete {
            path: Box::new(Variable("stocks".into())),
            condition: Some(
                Box::new(LessOrEqual(
                    Box::new(Variable("last_sale".into())),
                    Box::new(Literal(Float64Value(1.0))),
                ))
            ),
            limit: Some(Box::new(Literal(Int64Value(100)))),
        });
        let byte_code = vec![
            E_DELETE,
            E_VAR_GET, E_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 6,
            b's', b't', b'o', b'c', b'k', b's',
            E_LESS_OR_EQUAL,
            E_VAR_GET, E_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 9,
            b'l', b'a', b's', b't', b'_', b's', b'a', b'l', b'e',
            E_LITERAL, T_FLOAT64, 63, 240, 0, 0, 0, 0, 0, 0,
            E_LITERAL, T_INT64, 0, 0, 0, 0, 0, 0, 0, 100,
        ];
        assert_eq!(assemble(&model), byte_code);
        assert_eq!(disassemble(&mut ByteBuffer::wrap(byte_code)).unwrap(), model)
    }

    #[test]
    fn test_select() {
        let model = Inquire(Queryable::Select {
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
        });
        let byte_code = vec![
            // select symbol, exchange, last_sale
            E_SELECT, 0, 0, 0, 0, 0, 0, 0, 3,
            E_VAR_GET, E_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 6, b's', b'y', b'm', b'b', b'o', b'l',
            E_VAR_GET, E_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 8, b'e', b'x', b'c', b'h', b'a', b'n', b'g', b'e',
            E_VAR_GET, E_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 9, b'l', b'a', b's', b't', b'_', b's', b'a', b'l', b'e',
            // from stocks
            0, 0, 0, 0, 0, 0, 0, 1,
            E_VAR_GET, E_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 6, b's', b't', b'o', b'c', b'k', b's',
            // where last_sale <= 1.0
            0, 0, 0, 0, 0, 0, 0, 1,
            E_LESS_OR_EQUAL,
            E_VAR_GET, E_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 9, b'l', b'a', b's', b't', b'_', b's', b'a', b'l', b'e',
            E_LITERAL, T_FLOAT64, 63, 240, 0, 0, 0, 0, 0, 0, // 1.0
            // group by ...
            0, 0, 0, 0, 0, 0, 0, 0,
            // having ...
            0, 0, 0, 0, 0, 0, 0, 0,
            // order by symbol
            0, 0, 0, 0, 0, 0, 0, 1,
            E_VAR_GET, E_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 6, b's', b'y', b'm', b'b', b'o', b'l',
            // limit 5
            0, 0, 0, 0, 0, 0, 0, 1,
            E_LITERAL, T_INT64, 0, 0, 0, 0, 0, 0, 0, 5,
        ];
        assert_eq!(assemble(&model), byte_code);
        assert_eq!(disassemble(&mut ByteBuffer::wrap(byte_code)).unwrap(), model)
    }

    #[test]
    fn test_update() {
        let model = Mutate(Mutation::Update {
            path: Box::new(Variable("stocks".into())),
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
        });
        let byte_code = vec![
            // update stocks
            E_UPDATE, 0, 0, 0, 0, 0, 0, 0, 4,
            E_VAR_GET, E_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 6, b's', b't', b'o', b'c', b'k', b's',
            // via { last_sale: 0.1111 }
            E_VIA, E_JSON_LITERAL, 0, 0, 0, 0, 0, 0, 0, 2,
            E_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 9, b'l', b'a', b's', b't', b'_', b's', b'a', b'l', b'e',
            E_LITERAL, T_FLOAT64, 63, 188, 113, 12, 178, 149, 233, 226, // 0.1111
            // where symbol == 'ABC'
            E_EQUAL,
            E_VAR_GET, E_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 8, b'e', b'x', b'c', b'h', b'a', b'n', b'g', b'e',
            E_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E',
            // limit 10
            E_LITERAL, T_INT64, 0, 0, 0, 0, 0, 0, 0, 10,
        ];
        assert_eq!(assemble(&model), byte_code);
        assert_eq!(disassemble(&mut ByteBuffer::wrap(byte_code)).unwrap(), model)
    }

    #[test]
    fn test_dec() {
        let machine = Machine::new().push(Int64Value(6));
        let machine = dec(machine).unwrap();
        let (_, result) = machine.pop();
        assert_eq!(result, Some(Int64Value(5)));
    }

    #[test]
    fn test_inc() {
        let machine = Machine::new().push(Int64Value(6));
        let machine = inc(machine).unwrap();
        let (_, result) = machine.pop();
        assert_eq!(result, Some(Int64Value(7)));
    }

    #[test]
    fn test_fact_i32() {
        let machine = Machine::new().push(Int32Value(5));
        let machine = fact(machine).unwrap();
        let (_, result) = machine.pop();
        assert_eq!(result, Some(Int64Value(120)));
    }

    #[test]
    fn test_fact_f64() {
        let machine = Machine::new().push(Float64Value(6.));
        let machine = fact(machine).unwrap();
        let (_, result) = machine.pop();
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
            E_JSON_LITERAL, 0, 0, 0, 0, 0, 0, 0, 6,
            E_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 6, b's', b'y', b'm', b'b', b'o', b'l',
            E_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 3, b'T', b'R', b'X',
            E_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 8, b'e', b'x', b'c', b'h', b'a', b'n', b'g', b'e',
            E_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E',
            E_LITERAL, T_STRING, 0, 0, 0, 0, 0, 0, 0, 9, b'l', b'a', b's', b't', b'_', b's', b'a', b'l', b'e',
            E_LITERAL, T_FLOAT64, 64, 38, 56, 226, 25, 101, 43, 212,
        ];
        assert_eq!(assemble(&model), byte_code);
        assert_eq!(disassemble(&mut ByteBuffer::wrap(byte_code)).unwrap(), model);
    }

    #[test]
    fn test_multiple_opcodes() {
        // execute the program
        let (machine, value) = Machine::new()
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
        assert_eq!(machine.stack_len(), 0)
    }
}