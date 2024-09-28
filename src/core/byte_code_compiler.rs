////////////////////////////////////////////////////////////////////
// ByteCodeCompiler - responsible for assembling and disassembling
//                    expressions to and from byte code.
////////////////////////////////////////////////////////////////////

use std::ops::{Deref, Index};

use shared_lib::fail;

use crate::backdoor::BackDoorFunction;
use crate::codec;
use crate::compiler::fail_expr;
use crate::data_type_kind::DataTypeKind;
use crate::data_type_kind::DataTypeKind::*;
use crate::data_types::DataType;
use crate::expression::*;
use crate::expression::CreationEntity::{IndexEntity, TableEntity};
use crate::expression::Expression::*;
use crate::expression::MutateTarget::{IndexTarget, TableTarget};
use crate::expression::Queryable::Describe;
use crate::mnemonic::Mnemonic;
use crate::mnemonic::Mnemonic::*;
use crate::model_row_collection::ModelRowCollection;
use crate::numbers::NumberValue::*;
use crate::row_collection::RowCollection;
use crate::rows::Row;
use crate::server::ColumnJs;
use crate::structure::Structure;
use crate::table_columns::TableColumn;
use crate::typed_values::*;
use crate::typed_values::TypedValue::{BackDoor, Function, StringValue};

/// A JVM-inspired Byte Buffer utility (Big Endian)
pub struct ByteCodeCompiler {
    buf: Vec<u8>,
    limit: usize,
    offset: usize,
}

impl ByteCodeCompiler {

    ////////////////////////////////////////////////////////////////
    //      Static Methods
    ////////////////////////////////////////////////////////////////

    /// creates a new [ByteCodeCompiler]
    pub fn new(size: usize) -> Self {
        ByteCodeCompiler { offset: 0, limit: 0, buf: vec![0u8; size] }
    }

    /// creates a new ready-to-be-written [ByteCodeCompiler] by wrapping an existing vector
    pub fn from_bytes(buf: &Vec<u8>, capacity: usize) -> Self {
        let mut new_buf = Vec::with_capacity(capacity);
        new_buf.extend(buf);
        ByteCodeCompiler { offset: new_buf.len(), limit: 0, buf: new_buf }
    }

    /// creates a new ready-to-be-read [ByteCodeCompiler] by wrapping an existing vector
    pub fn wrap(buf: Vec<u8>) -> Self {
        ByteCodeCompiler { offset: 0, limit: buf.len(), buf }
    }

    ////////////////////////////////////////////////////////////////
    //      Instance Methods
    ////////////////////////////////////////////////////////////////

    pub fn flip(&mut self) {
        self.limit = self.offset;
        self.offset = 0;
    }

    pub fn has_more(&self, number: usize) -> bool { self.offset + number < self.limit }

    pub fn has_next(&self) -> bool { self.offset < self.limit }

    pub fn len(&self) -> usize { self.buf.len() }

    pub fn limit(&self) -> usize { self.limit }

    pub fn move_rel(&mut self, delta: isize) {
        match self.offset as isize + delta {
            n if n >= 0 => self.offset = n as usize,
            _ => self.offset = 0
        }
    }

    /// returns a 64-bit typed-value array
    pub fn next_array(&mut self) -> std::io::Result<Vec<TypedValue>> {
        let length = self.next_u32();
        let mut array = Vec::new();
        for _ in 0..length {
            array.push(self.next_value()?);
        }
        Ok(array)
    }

    pub fn next_backdoor_fn(&mut self) -> BackDoorFunction {
        BackDoorFunction::from_u8(self.next_u8())
    }

    /// returns a 64-bit byte array
    pub fn next_blob(&mut self) -> Vec<u8> {
        let length = self.next_u64() as usize;
        self.next_bytes(length)
    }

    pub fn next_bool(&mut self) -> bool {
        let size = self.validate(1);
        let result = codec::decode_u8(&self.buf, self.offset, |b| b);
        self.offset += size;
        result != 0
    }

    pub fn next_bytes(&mut self, length: usize) -> Vec<u8> {
        let start = self.offset;
        let end = self.offset + self.validate(length);
        let bytes = self.buf[start..end].to_vec();
        self.offset = end;
        bytes
    }

    /// returns a 64-bit character array
    pub fn next_clob(&mut self) -> Vec<char> {
        let length = self.next_u64() as usize;
        let bytes = self.next_bytes(length);
        String::from_utf8(bytes).unwrap().chars().collect()
    }

    pub fn next_column(&mut self) -> ColumnJs {
        let name = self.next_string();
        let column_type = self.next_string();
        let default_value = self.next_string_opt();
        ColumnJs::new(name, column_type, default_value)
    }

    pub fn next_columns(&mut self) -> Vec<ColumnJs> {
        let length = self.next_u16();
        let mut columns = Vec::new();
        for _ in 0..length {
            columns.push(self.next_column());
        }
        columns
    }

    pub fn next_f32(&mut self) -> f32 {
        let size = self.validate(4);
        let result = codec::decode_u8x4(&self.buf, self.offset, |buf| f32::from_be_bytes(buf));
        self.offset += size;
        result
    }

    pub fn next_f64(&mut self) -> f64 {
        let size = self.validate(8);
        let result = codec::decode_u8x8(&self.buf, self.offset, |buf| f64::from_be_bytes(buf));
        self.offset += size;
        result
    }

    pub fn next_i8(&mut self) -> i8 {
        let size = self.validate(1);
        let result = codec::decode_u8(&self.buf, self.offset, |b| b as i8);
        self.offset += size;
        result
    }

    pub fn next_i16(&mut self) -> i16 {
        let size = self.validate(2);
        let result = codec::decode_u8x2(&self.buf, self.offset, |buf| i16::from_be_bytes(buf));
        self.offset += size;
        result
    }

    pub fn next_i32(&mut self) -> i32 {
        let size = self.validate(4);
        let result = codec::decode_u8x4(&self.buf, self.offset, |buf| i32::from_be_bytes(buf));
        self.offset += size;
        result
    }

    pub fn next_i64(&mut self) -> i64 {
        let size = self.validate(8);
        let result = codec::decode_u8x8(&self.buf, self.offset, |buf| i64::from_be_bytes(buf));
        self.offset += size;
        result
    }

    pub fn next_i128(&mut self) -> i128 {
        let size = self.validate(16);
        let result = codec::decode_u8x16(&self.buf, self.offset, |buf| i128::from_be_bytes(buf));
        self.offset += size;
        result
    }

    /// compiles the expression into binary code
    pub fn assemble(expression: &Expression) -> Vec<u8> {
        use crate::expression::Expression::*;
        use crate::mnemonic::Mnemonic::*;
        match expression {
            And(a, b) => Self::encode(ExAnd.to_u8(), vec![a, b]),
            ArrayLiteral(items) => Self::encode_vec(ExArrayLit.to_u8(), items),
            AsValue(name, expr) =>
                Self::encode(ExAsValue.to_u8(), vec![&Literal(StringValue(name.to_string())), expr]),
            Between(a, b, c) =>
                Self::encode(ExBetween.to_u8(), vec![a, b, c]),
            Betwixt(a, b, c) =>
                Self::encode(ExBetwixt.to_u8(), vec![a, b, c]),
            BitwiseAnd(a, b) =>
                Self::encode(ExBitwiseAnd.to_u8(), vec![a, b]),
            BitwiseOr(a, b) =>
                Self::encode(ExBitwiseOr.to_u8(), vec![a, b]),
            BitwiseXor(a, b) =>
                Self::encode(ExBitwiseXor.to_u8(), vec![a, b]),
            CodeBlock(ops) => Self::encode_vec(ExCodeBlock.to_u8(), ops),
            ColumnSet(columns) => Self::encode_columns(columns),
            Contains(a, b) => Self::encode(ExContains.to_u8(), vec![a, b]),
            Divide(a, b) => Self::encode(ExDivide.to_u8(), vec![a, b]),
            ElementAt(a, b) => Self::encode(ExElemIndex.to_u8(), vec![a, b]),
            Equal(a, b) => Self::encode(ExEqual.to_u8(), vec![a, b]),
            Factorial(a) => Self::encode(ExFactorial.to_u8(), vec![a]),
            Feature { title, scenarios } => {
                let mut values = Vec::new();
                values.push(title.deref());
                values.extend(scenarios);
                Self::encode(ExFeature.to_u8(), values)
            }
            From(src) => Self::encode(ExFrom.to_u8(), vec![src]),
            FunctionCall { fx, args } => {
                let mut values = vec![fx.deref()];
                values.extend(args);
                Self::encode(ExWhile.to_u8(), values)
            }
            GreaterThan(a, b) => Self::encode(ExGreaterThan.to_u8(), vec![a, b]),
            GreaterOrEqual(a, b) => Self::encode(ExGreaterOrEqual.to_u8(), vec![a, b]),
            HTTP { method, url, body, headers, multipart } =>
                Self::encode(ExHTTP.to_u8(), vec![method, url, &Self::get_or_undef(body), &Self::get_or_undef(headers), &Self::get_or_undef(multipart)]),
            If { condition, a, b } =>
                Self::encode(ExIf.to_u8(), vec![condition, a, &Self::get_or_undef(b)]),
            Include(path) => Self::encode(ExInclude.to_u8(), vec![path]),
            Inquire(q) => Self::assemble_inquiry(q),
            JSONLiteral(tuples) => Self::assemble_json_object(tuples),
            LessThan(a, b) => Self::encode(ExLessThan.to_u8(), vec![a, b]),
            LessOrEqual(a, b) => Self::encode(ExLessOrEqual.to_u8(), vec![a, b]),
            Literal(value) => Self::encode_value(ExLiteral.to_u8(), value),
            Minus(a, b) => Self::encode(ExMinus.to_u8(), vec![a, b]),
            Modulo(a, b) => Self::encode(ExModulo.to_u8(), vec![a, b]),
            Multiply(a, b) => Self::encode(ExMultiply.to_u8(), vec![a, b]),
            MustAck(a) => Self::encode(ExMustAck.to_u8(), vec![a]),
            MustDie(a) => Self::encode(ExMustDie.to_u8(), vec![a]),
            MustIgnoreAck(a) => Self::encode(ExMustIgnoreAck.to_u8(), vec![a]),
            MustNotAck(a) => Self::encode(ExMustNotAck.to_u8(), vec![a]),
            Mutate(m) => Self::assemble_modification(m),
            Neg(a) => Self::encode(ExNeg.to_u8(), vec![a]),
            Not(a) => Self::encode(ExNot.to_u8(), vec![a]),
            NotEqual(a, b) => Self::encode(ExNotEqual.to_u8(), vec![a, b]),
            Ns(a) => Self::encode(ExNS.to_u8(), vec![a]),
            Or(a, b) => Self::encode(ExOr.to_u8(), vec![a, b]),
            Perform(i) => Self::assemble_infrastructure(i),
            Plus(a, b) => Self::encode(ExPlus.to_u8(), vec![a, b]),
            Pow(a, b) => Self::encode(ExPow.to_u8(), vec![a, b]),
            Range(a, b) => Self::encode(ExRange.to_u8(), vec![a, b]),
            Return(a) => Self::encode_vec(ExReturn.to_u8(), a),
            Scenario { title, verifications, inherits } => {
                let mut values = Vec::new();
                values.push(title.deref());
                values.push(match inherits {
                    Some(value) => value,
                    None => &UNDEFINED,
                });
                values.extend(verifications);
                Self::encode(ExFeature.to_u8(), values)
            }
            SERVE(a) => Self::encode(ExSERVE.to_u8(), vec![a]),
            SetVariable(name, expr) =>
                Self::encode(ExVarSet.to_u8(), vec![&Literal(StringValue(name.into())), expr]),
            BitwiseShiftLeft(a, b) => Self::encode(ExShiftLeft.to_u8(), vec![a, b]),
            BitwiseShiftRight(a, b) => Self::encode(ExShiftRight.to_u8(), vec![a, b]),
            TupleLiteral(values) => Self::encode_vec(ExTuple.to_u8(), values),
            Variable(name) => Self::encode(ExVarGet.to_u8(), vec![&Literal(StringValue(name.into()))]),
            Via(src) => Self::encode(ExVia.to_u8(), vec![src]),
            While { condition, code } =>
                Self::encode(ExWhile.to_u8(), vec![condition, code]),
        }
    }

    /// compiles the [Infrastructure] into binary code
    fn assemble_infrastructure(expression: &Infrastructure) -> Vec<u8> {
        use Infrastructure::*;
        use Mnemonic::*;
        match expression {
            Create { path, entity: IndexEntity { columns } } => {
                let mut args = vec![path.deref()];
                args.extend(columns);
                Self::encode(ExCreateIndex.to_u8(), args)
            }
            Create { path, entity: TableEntity { columns, from } } =>
                Self::encode(ExCreateTable.to_u8(), vec![path, &ColumnSet(columns.to_owned()), &Self::get_or_undef(from)]),
            Declare(IndexEntity { columns }) => {
                let mut args = Vec::new();
                args.extend(columns);
                Self::encode(ExDeclareIndex.to_u8(), args)
            }
            Declare(TableEntity { columns, from }) =>
                Self::encode(ExDeclareTable.to_u8(), vec![&ColumnSet(columns.to_owned()), &Self::get_or_undef(from)]),
            Drop(IndexTarget { path }) => Self::encode(ExDrop.to_u8(), vec![path]),
            Drop(TableTarget { path }) => Self::encode(ExDrop.to_u8(), vec![path]),
        }
    }

    /// compiles the [Mutation] into binary code
    fn assemble_modification(expression: &Mutation) -> Vec<u8> {
        use Mutation::*;
        use Mnemonic::*;
        match expression {
            Append { path, source } => Self::encode(ExAppend.to_u8(), vec![path, source]),
            Compact { path } => Self::encode(ExCompact.to_u8(), vec![path]),
            Delete { path, condition, limit } =>
                Self::encode(ExDelete.to_u8(), vec![path, &Self::get_or_undef(condition), &Self::get_or_undef(limit)]),
            IntoNs(a, b) => Self::encode(ExIntoNS.to_u8(), vec![a, b]),
            Overwrite { path, source, condition, limit } => {
                let mut args = Vec::new();
                args.push(path.deref().to_owned());
                args.push(source.deref().to_owned());
                args.push(Self::get_or_undef(condition));
                args.push(Self::get_or_undef(limit));
                Self::encode_vec(ExOverwrite.to_u8(), &args)
            }
            Scan { path } => Self::encode(ExScan.to_u8(), vec![path]),
            Truncate { path, limit: new_size } =>
                Self::encode(ExTruncate.to_u8(), vec![path, &Self::get_or_undef(new_size)]),
            Undelete { path, condition, limit } =>
                Self::encode(ExUndelete.to_u8(), vec![path, &Self::get_or_undef(condition), &Self::get_or_undef(limit)]),
            Update { path, source, condition, limit } => {
                let mut args: Vec<Expression> = Vec::new();
                args.push(path.deref().to_owned());
                args.push(source.deref().to_owned());
                args.push(Self::get_or_undef(condition));
                args.push(Self::get_or_undef(limit));
                Self::encode_vec(ExUpdate.to_u8(), &args)
            }
        }
    }

    /// compiles the [Queryable] into binary code
    fn assemble_inquiry(expression: &Queryable) -> Vec<u8> {
        use Queryable::*;
        use Mnemonic::*;
        match expression {
            Describe(a) => Self::encode(ExDescribe.to_u8(), vec![a]),
            Limit { from, limit } => Self::encode(ExLimit.to_u8(), vec![from, limit]),
            Reverse(a) => Self::encode(ExReverse.to_u8(), vec![a]),
            Select {
                fields, from, condition,
                group_by, having,
                order_by, limit
            } => Self::assemble_select(fields, from, condition, group_by, having, order_by, limit),
            Where { from, condition } =>
                Self::encode(ExWhere.to_u8(), vec![from, condition]),
        }
    }

    /// compiles the collection of expressions into binary code
    fn assemble_fully(expressions: &Vec<Expression>) -> Vec<u8> {
        expressions.iter().flat_map(|expr| Self::assemble(expr)).collect::<Vec<u8>>()
    }

    /// compiles a JSON-like collection of expressions into binary code
    fn assemble_json_object(tuples: &Vec<(String, Expression)>) -> Vec<u8> {
        use Mnemonic::ExJsonLiteral;
        let expressions = tuples.iter().flat_map(|(k, v)| {
            vec![Literal(StringValue(k.into())), v.to_owned()]
        }).collect::<Vec<Expression>>();
        Self::encode_vec(ExJsonLiteral.to_u8(), &expressions)
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
        use Mnemonic::ExSelect;
        let mut byte_code = vec![ExSelect.to_u8()];
        byte_code.extend(Self::encode_array(fields));
        byte_code.extend(Self::encode_opt_as_array(from));
        byte_code.extend(Self::encode_opt_as_array(condition));
        byte_code.extend(Self::encode_array_opt(group_by));
        byte_code.extend(Self::encode_opt_as_array(having));
        byte_code.extend(Self::encode_array_opt(order_by));
        byte_code.extend(Self::encode_opt_as_array(limit));
        byte_code
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

    /// decodes the typed value based on the supplied data type and buffer
    fn assemble_type(data_type: &DataType) -> Vec<u8> {
        use DataType::*;
        match data_type {
            AckType => Self::assemble_bytes(TxAck.to_u8(), &Vec::new()),
            BackDoorType => Self::assemble_bytes(TxBackDoor.to_u8(), &vec![0]),
            BLOBType(size) => Self::assemble_bytes(TxBlob.to_u8(), &Self::assemble_usize(*size)),
            BooleanType => Self::assemble_bytes(TxBoolean.to_u8(), &Vec::new()),
            CLOBType(size) => Self::assemble_bytes(TxClob.to_u8(), &Self::assemble_usize(*size)),
            DateType => Self::assemble_bytes(TxDate.to_u8(), &Vec::new()),
            EnumType(labels) => Self::assemble_strings(TxEnum.to_u8(), &labels),
            ErrorType => Self::assemble_bytes(TxString.to_u8(), &Self::assemble_usize(256)),
            FunctionType(columns) => Self::assemble_bytes(TxFunction.to_u8(), &Self::encode_columns(columns)),
            JSONType => Self::assemble_bytes(TxJsonObject.to_u8(), &Vec::new()),
            NumberType(kind) => {
                use crate::number_kind::NumberKind::*;

                match *kind {
                    F32Kind => Self::assemble_bytes(F32Kind.to_u8(), &Vec::new()),
                    F64Kind => Self::assemble_bytes(F64Kind.to_u8(), &Vec::new()),
                    I8Kind => Self::assemble_bytes(I8Kind.to_u8(), &Vec::new()),
                    I16Kind => Self::assemble_bytes(I16Kind.to_u8(), &Vec::new()),
                    I32Kind => Self::assemble_bytes(I32Kind.to_u8(), &Vec::new()),
                    I64Kind => Self::assemble_bytes(I64Kind.to_u8(), &Vec::new()),
                    I128Kind => Self::assemble_bytes(I128Kind.to_u8(), &Vec::new()),
                    U8Kind => Self::assemble_bytes(U8Kind.to_u8(), &Vec::new()),
                    U16Kind => Self::assemble_bytes(U16Kind.to_u8(), &Vec::new()),
                    U32Kind => Self::assemble_bytes(U32Kind.to_u8(), &Vec::new()),
                    U64Kind => Self::assemble_bytes(U64Kind.to_u8(), &Vec::new()),
                    U128Kind => Self::assemble_bytes(U128Kind.to_u8(), &Vec::new()),
                    NaNKind => Vec::new()
                }
            }
            RowsAffectedType => Self::assemble_bytes(TxRowsAffected.to_u8(), &Vec::new()),
            StringType(size) => Self::assemble_bytes(TxString.to_u8(), &Self::assemble_usize(*size)),
            StructureType(columns) => Self::assemble_bytes(TxStructure.to_u8(), &Self::encode_columns(columns)),
            TableType(columns) => Self::assemble_bytes(TxTableValue.to_u8(), &Self::encode_columns(columns)),
            UUIDType => Self::assemble_bytes(TxUUID.to_u8(), &Vec::new()),
        }
    }

    fn assemble_usize(value: usize) -> Vec<u8> {
        usize::to_be_bytes(value).to_vec()
    }

    pub fn disassemble(buf: &mut ByteCodeCompiler) -> std::io::Result<Expression> {
        buf.next_expression()
    }

    pub fn disassemble_fully(byte_code: &Vec<u8>) -> std::io::Result<Expression> {
        let mut models = Vec::new();
        let mut buf = Self::wrap(byte_code.to_owned());
        while buf.has_next() {
            models.push(Self::disassemble(&mut buf)?);
        }
        let opcode = if models.len() == 1 { models[0].to_owned() } else { CodeBlock(models) };
        Ok(opcode)
    }

    fn encode(id: u8, args: Vec<&Expression>) -> Vec<u8> {
        let mut byte_code = vec![id];
        for expr in args {
            let code = Self::assemble(expr);
            byte_code.extend(code);
        }
        byte_code
    }

    fn encode_array(ops: &Vec<Expression>) -> Vec<u8> {
        let mut byte_code = Vec::new();
        byte_code.extend((ops.len() as u64).to_be_bytes());
        byte_code.extend(ops.iter().flat_map(Self::assemble).collect::<Vec<u8>>());
        byte_code
    }

    fn encode_opt_as_array(opt: &Option<Box<Expression>>) -> Vec<u8> {
        let ops = if opt.is_none() { Vec::new() } else {
            vec![opt.to_owned().unwrap().deref().to_owned()]
        };
        Self::encode_array(&ops)
    }

    fn encode_array_opt(ops_opt: &Option<Vec<Expression>>) -> Vec<u8> {
        let ops = ops_opt.to_owned().unwrap_or(Vec::new());
        Self::encode_array(&ops)
    }

    fn assemble_bytes(id: u8, code: &Vec<u8>) -> Vec<u8> {
        let mut byte_code = vec![id];
        byte_code.extend(code);
        byte_code
    }

    fn encode_columns(columns: &Vec<ColumnJs>) -> Vec<u8> {
        columns.iter().flat_map(|c| c.encode()).collect::<Vec<u8>>()
    }

    fn encode_value(id: u8, value: &TypedValue) -> Vec<u8> {
        let mut byte_code = vec![id];
        byte_code.push(value.to_kind().to_u8());
        byte_code.extend(value.encode());
        byte_code
    }

    fn encode_vec(id: u8, ops: &Vec<Expression>) -> Vec<u8> {
        let mut byte_code = vec![id];
        byte_code.extend((ops.len() as u64).to_be_bytes());
        byte_code.extend(ops.iter().flat_map(Self::assemble).collect::<Vec<u8>>());
        byte_code
    }

    fn get_or_undef(opt: &Option<Box<Expression>>) -> Expression {
        opt.to_owned().unwrap_or(Box::new(UNDEFINED)).deref().to_owned()
    }

    pub fn next_expression(&mut self) -> std::io::Result<Expression> {
        use Expression::*;
        use Mnemonic::*;

        // get the next mnemonic
        match Mnemonic::from_u8(self.next_u8()) {
            ExAnd => Ok(And(self.decode_box()?, self.decode_box()?)),
            ExAppend => Ok(Mutate(Mutation::Append { path: self.decode_box()?, source: self.decode_box()? })),
            ExArrayLit => Ok(ArrayLiteral(self.decode_array()?)),
            ExAsValue => Ok(AsValue(self.next_string(), self.decode_box()?)),
            ExCompact => Ok(Mutate(Mutation::Compact { path: self.decode_box()? })),
            ExElemIndex => Ok(ElementAt(self.decode_box()?, self.decode_box()?)),
            ExFeature => Ok(Feature { title: self.decode_box()?, scenarios: self.decode_array()? }),
            ExBetween => Ok(Between(self.decode_box()?, self.decode_box()?, self.decode_box()?)),
            ExBetwixt => Ok(Betwixt(self.decode_box()?, self.decode_box()?, self.decode_box()?)),
            ExBitwiseAnd => Ok(BitwiseAnd(self.decode_box()?, self.decode_box()?)),
            ExBitwiseOr => Ok(BitwiseOr(self.decode_box()?, self.decode_box()?)),
            ExBitwiseXor => Ok(BitwiseXor(self.decode_box()?, self.decode_box()?)),
            ExCodeBlock => Ok(CodeBlock(self.decode_array()?)),
            ExContains => Ok(Contains(self.decode_box()?, self.decode_box()?)),
            ExCreateIndex => {
                let mut args = self.decode_array()?;
                assert!(args.len() >= 2);
                Ok(Perform(Infrastructure::Create {
                    path: Box::new(args.pop().unwrap().to_owned()),
                    entity: IndexEntity {
                        columns: args,
                    },
                }))
            }
            ExCreateTable =>
                Ok(Perform(Infrastructure::Create {
                    path: self.decode_box()?,
                    entity: TableEntity {
                        columns: self.next_columns(),
                        from: self.decode_opt()?,
                    },
                })),
            ExCSV => Ok(Literal(BackDoor(BackDoorFunction::from_u8(self.next_u8())))),
            ExDeclareIndex =>
                Ok(Perform(Infrastructure::Declare(IndexEntity { columns: self.decode_array()? }))),
            ExDeclareTable =>
                Ok(Perform(Infrastructure::Declare(TableEntity { columns: self.next_columns(), from: self.decode_opt()? }))),
            ExDelete => Ok(Mutate(Mutation::Delete {
                path: self.decode_box()?,
                condition: self.decode_opt()?,
                limit: self.decode_opt()?,
            })),
            ExDescribe => Ok(Inquire(Describe(self.decode_box()?))),
            ExDivide => Ok(Divide(self.decode_box()?, self.decode_box()?)),
            ExDrop => Ok(Perform(Infrastructure::Drop(TableTarget { path: self.decode_box()? }))),
            ExEqual => Ok(Equal(self.decode_box()?, self.decode_box()?)),
            ExFactorial => Ok(Factorial(self.decode_box()?)),
            ExFrom => Ok(From(self.decode_box()?)),
            ExFunction => Ok(Literal(Function {
                params: self.next_columns(),
                code: Self::decode_box(self)?,
            })),
            ExGreaterOrEqual => Ok(GreaterOrEqual(self.decode_box()?, self.decode_box()?)),
            ExGreaterThan => Ok(GreaterThan(self.decode_box()?, self.decode_box()?)),
            ExHTTP => Ok(HTTP {
                method: self.decode_box()?,
                url: self.decode_box()?,
                body: self.decode_opt()?,
                headers: self.decode_opt()?,
                multipart: self.decode_opt()?,
            }),
            ExIf => Ok(If { condition: self.decode_box()?, a: self.decode_box()?, b: self.decode_opt()? }),
            ExInclude => Ok(Include(self.decode_box()?)),
            ExIntoNS => Ok(Mutate(Mutation::IntoNs(self.decode_box()?, self.decode_box()?))),
            ExJsonLiteral => Ok(JSONLiteral(self.decode_json_object()?)),
            ExLessOrEqual => Ok(LessOrEqual(self.decode_box()?, self.decode_box()?)),
            ExLessThan => Ok(LessThan(self.decode_box()?, self.decode_box()?)),
            ExLimit => Ok(Inquire(Queryable::Limit { from: self.decode_box()?, limit: self.decode_box()? })),
            ExLiteral => Ok(Literal(self.next_value()?)),
            ExMinus => Ok(Minus(self.decode_box()?, self.decode_box()?)),
            ExModulo => Ok(Modulo(self.decode_box()?, self.decode_box()?)),
            ExMultiply => Ok(Multiply(self.decode_box()?, self.decode_box()?)),
            ExMustAck => Ok(MustAck(self.decode_box()?)),
            ExMustDie => Ok(MustDie(self.decode_box()?)),
            ExMustIgnoreAck => Ok(MustIgnoreAck(self.decode_box()?)),
            ExMustNotAck => Ok(MustNotAck(self.decode_box()?)),
            ExNeg => Ok(Neg(self.decode_box()?)),
            ExNot => Ok(Not(self.decode_box()?)),
            ExNotEqual => Ok(NotEqual(self.decode_box()?, self.decode_box()?)),
            ExNS => Ok(Ns(self.decode_box()?)),
            ExOr => Ok(Or(self.decode_box()?, self.decode_box()?)),
            ExOverwrite => Ok(Mutate(Mutation::Overwrite {
                path: self.decode_box()?,
                source: self.decode_box()?,
                condition: self.decode_opt()?,
                limit: self.decode_opt()?,
            })),
            ExPlus => Ok(Plus(self.decode_box()?, self.decode_box()?)),
            ExPow => Ok(Pow(self.decode_box()?, self.decode_box()?)),
            ExRange => Ok(Range(self.decode_box()?, self.decode_box()?)),
            ExReturn => Ok(Return(self.decode_array()?)),
            ExReverse => Ok(Inquire(Queryable::Reverse(self.decode_box()?))),
            ExScan => Ok(Mutate(Mutation::Scan { path: self.decode_box()? })),
            ExScenario => Ok(Scenario { title: self.decode_box()?, verifications: self.decode_array()?, inherits: self.decode_opt()? }),
            ExSelect => self.disassemble_select(),
            ExSERVE => Ok(SERVE(self.decode_box()?)),
            ExShiftLeft => Ok(BitwiseShiftLeft(self.decode_box()?, self.decode_box()?)),
            ExShiftRight => Ok(BitwiseShiftRight(self.decode_box()?, self.decode_box()?)),
            ExStderr => Ok(Literal(BackDoor(BackDoorFunction::BxStdErr))),
            ExStdout => Ok(Literal(BackDoor(BackDoorFunction::BxStdOut))),
            ExTruncate => Ok(Mutate(Mutation::Truncate { path: self.decode_box()?, limit: self.decode_opt()? })),
            ExTuple => Ok(TupleLiteral(self.decode_array()?)),
            ExUndelete =>
                Ok(Mutate(Mutation::Undelete {
                    path: self.decode_box()?,
                    condition: self.decode_opt()?,
                    limit: self.decode_opt()?,
                })),
            ExUpdate => self.disassemble_update(),
            ExVarGet =>
                match self.next_expression()? {
                    Literal(StringValue(name)) => Ok(Variable(name)),
                    z => fail_expr("Expected identifier", &z)
                }
            ExVarSet =>
                match self.next_expression()? {
                    Literal(StringValue(name)) => Ok(SetVariable(name, self.decode_box()?)),
                    z => fail_expr("Expected String", &z)
                }
            ExVia => Ok(Via(self.decode_box()?)),
            ExWhere => Ok(Inquire(Queryable::Where { from: self.decode_box()?, condition: self.decode_box()? })),
            ExWhile => Ok(While { condition: self.decode_box()?, code: self.decode_box()? }),
        }
    }

    fn decode_array(&mut self) -> std::io::Result<Vec<Expression>> {
        let count = self.next_u64();
        let mut array = Vec::new();
        for _ in 0..count { array.push(self.next_expression()?); }
        Ok(array)
    }

    fn decode_array_as_opt(&mut self) -> std::io::Result<Option<Box<Expression>>> {
        let ops = self.decode_array()?;
        if ops.is_empty() { Ok(None) } else { Ok(Some(Box::new(ops[0].to_owned()))) }
    }

    fn decode_array_opt(&mut self) -> std::io::Result<Option<Vec<Expression>>> {
        let array = self.decode_array()?;
        Ok(if array.is_empty() { None } else { Some(array) })
    }

    fn decode_box(&mut self) -> std::io::Result<Box<Expression>> {
        Ok(Box::new(self.next_expression()?))
    }

    fn decode_json_object(&mut self) -> std::io::Result<Vec<(String, Expression)>> {
        let count = self.next_u64() / 2;
        let mut tuples = Vec::new();
        for _ in 0..count {
            match self.next_expression()? {
                Literal(StringValue(k)) => {
                    let v = self.next_expression()?;
                    tuples.push((k, v))
                }
                z => panic!("Expected String, but got {}", z)
            }
        }
        Ok(tuples)
    }

    fn decode_opt(&mut self) -> std::io::Result<Option<Box<Expression>>> {
        if self.remaining() == 0 { return Ok(None); } else {
            let code = self.next_u8();
            match Mnemonic::from_u8(code) {
                //TUndefined => Ok(None),
                _ => {
                    self.move_rel(-1);
                    let expr = self.decode_box()?;
                    Ok(Some(expr))
                }
            }
        }
    }

    fn disassemble_select(&mut self) -> std::io::Result<Expression> {
        let fields = self.decode_array()?;
        let from = self.decode_array_as_opt()?;
        let condition = self.decode_array_as_opt()?;
        let group_by = self.decode_array_opt()?;
        let having = self.decode_array_as_opt()?;
        let order_by = self.decode_array_opt()?;
        let limit = self.decode_array_as_opt()?;
        Ok(Inquire(Queryable::Select { fields, from, condition, group_by, having, order_by, limit }))
    }

    fn disassemble_update(&mut self) -> std::io::Result<Expression> {
        let args = self.decode_array()?;
        if args.len() < 2 {
            return fail(format!("Invalid args: expected 4, got {}", args.len()));
        }
        let path = Box::new(args[0].to_owned());
        let source = Box::new(args[1].to_owned());
        let condition = if args.len() > 2 { Some(Box::new(args[2].to_owned())) } else { None };
        let limit = if args.len() > 3 { Some(Box::new(args[3].to_owned())) } else { None };
        Ok(Mutate(Mutation::Update { path, source, condition, limit }))
    }

    pub fn next_json(&mut self) -> std::io::Result<Vec<(String, TypedValue)>> {
        let length = self.next_u64();
        let mut list = Vec::new();
        for _ in 0..length {
            let name = self.next_string();
            let value = self.next_value()?;
            list.push((name, value));
        }
        Ok(list)
    }

    pub fn next_row_id(&mut self) -> usize {
        let size = self.validate(4);
        let result = codec::decode_u8x4(&self.buf, self.offset, |buf| u32::from_be_bytes(buf));
        self.offset += size;
        result as usize
    }

    pub fn next_rows(&mut self) -> std::io::Result<Vec<Row>> {
        let columns = self.next_columns();
        self.next_rows_with_columns(&columns)
    }

    pub fn next_rows_with_columns(
        &mut self,
        columns: &Vec<ColumnJs>,
    ) -> std::io::Result<Vec<Row>> {
        let t_columns = TableColumn::from_columns(columns)?;
        let n_rows = self.next_u64();
        let mut rows = Vec::new();
        for _ in 0..n_rows {
            let (row, rmd) = Row::from_buffer(&t_columns, self)?;
            if rmd.is_allocated {
                rows.push(row)
            }
        }
        Ok(rows)
    }

    /// returns a 64-bit character string
    pub fn next_string(&mut self) -> String {
        let length = self.next_u64();
        let bytes = self.next_bytes(length as usize);
        String::from_utf8(bytes).unwrap()
    }

    /// returns a 32-bit string array
    pub fn next_string_array(&mut self) -> Vec<String> {
        let length = self.next_u64();
        let mut array = Vec::new();
        for _ in 0..length {
            array.push(self.next_string());
        }
        array
    }

    pub fn next_string_opt(&mut self) -> Option<String> {
        match self.next_u64() as usize {
            0 => None,
            n => String::from_utf8(self.next_bytes(n)).ok()
        }
    }

    pub fn next_struct(&mut self) -> std::io::Result<Structure> {
        let columns = self.next_columns();
        self.next_struct_with_columns(&columns)
    }

    pub fn next_struct_with_columns(
        &mut self,
        columns: &Vec<ColumnJs>,
    ) -> std::io::Result<Structure> {
        let fields = TableColumn::from_columns(columns)?;
        Ok(Structure::new(fields, self.next_array()?))
    }

    pub fn next_table(&mut self) -> std::io::Result<ModelRowCollection> {
        let columns = self.next_columns();
        let phys_columns = TableColumn::from_columns(&columns)?;
        let rows = self.next_rows()?;
        Ok(ModelRowCollection::from_rows(phys_columns, rows))
    }

    pub fn next_table_with_columns(
        &mut self,
        columns: &Vec<ColumnJs>,
    ) -> std::io::Result<ModelRowCollection> {
        let phys_columns = TableColumn::from_columns(&columns)?;
        let rows = self.next_rows_with_columns(columns)?;
        Ok(ModelRowCollection::from_rows(phys_columns, rows))
    }

    pub fn next_u8(&mut self) -> u8 {
        let size = self.validate(1);
        let result = codec::decode_u8(&self.buf, self.offset, |b| b);
        self.offset += size;
        result
    }

    pub fn next_u16(&mut self) -> u16 {
        let size = self.validate(2);
        let result = codec::decode_u8x2(&self.buf, self.offset, |buf| u16::from_be_bytes(buf));
        self.offset += size;
        result
    }

    pub fn next_u32(&mut self) -> u32 {
        let size = self.validate(4);
        let result = codec::decode_u8x4(&self.buf, self.offset, |buf| u32::from_be_bytes(buf));
        self.offset += size;
        result
    }

    pub fn next_u64(&mut self) -> u64 {
        let size = self.validate(8);
        let result = codec::decode_u8x8(&self.buf, self.offset, |buf| u64::from_be_bytes(buf));
        self.offset += size;
        result
    }

    pub fn next_u128(&mut self) -> u128 {
        let size = self.validate(16);
        let result = codec::decode_u8x16(&self.buf, self.offset, |buf| u128::from_be_bytes(buf));
        self.offset += size;
        result
    }

    pub fn next_uuid(&mut self) -> [u8; 16] {
        let mut uuid = [0u8; 16];
        let bytes = self.next_bytes(uuid.len());
        for i in 0..uuid.len() { uuid[i] = bytes[i] }
        uuid
    }

    pub fn next_value(&mut self) -> std::io::Result<TypedValue> {
        use TypedValue::*;
        use DataTypeKind::*;
        match DataTypeKind::from_u8(self.next_u8()) {
            TxAck => Ok(Ack),
            TxArray => Ok(Array(self.next_array()?)),
            TxBackDoor => Ok(ErrorValue("BackDoors cannot be decoded".into())),
            TxBlob => Ok(BLOB(self.next_blob())),
            TxBoolean => Ok(Boolean(self.next_bool())),
            TxClob => Ok(CLOB(self.next_clob())),
            TxDate => Ok(DateValue(self.next_i64())),
            TxEnum => Ok(Array(self.next_array()?)),
            TxError => Ok(ErrorValue(self.next_string())),
            TxFunction => Ok(Function {
                params: self.next_columns(),
                code: self.decode_box()?,
            }),
            TxJsonObject => Ok(JSONValue(self.next_json()?)),
            TxNamespace =>
                match self.next_string().split('.').collect::<Vec<_>>().as_slice() {
                    [d, s, n] => Ok(NamespaceValue(d.to_string(), s.to_string(), n.to_string())),
                    _ => fail("Invalid namespace reference (ex. 'a.b.stocks')")
                }
            TxNull => Ok(Null),
            TxNumberF32 => Ok(Number(F32Value(self.next_f32()))),
            TxNumberF64 => Ok(Number(F64Value(self.next_f64()))),
            TxNumberI8 => Ok(Number(I8Value(self.next_i8()))),
            TxNumberI16 => Ok(Number(I16Value(self.next_i16()))),
            TxNumberI32 => Ok(Number(I32Value(self.next_i32()))),
            TxNumberI64 => Ok(Number(I64Value(self.next_i64()))),
            TxNumberI128 => Ok(Number(I128Value(self.next_i128()))),
            TxNumberU8 => Ok(Number(U8Value(self.next_u8()))),
            TxNumberU16 => Ok(Number(U16Value(self.next_u16()))),
            TxNumberU32 => Ok(Number(U32Value(self.next_u32()))),
            TxNumberU64 => Ok(Number(U64Value(self.next_u64()))),
            TxNumberU128 => Ok(Number(U128Value(self.next_u128()))),
            TxNumberNaN => Ok(Number(NaNValue)),
            TxRowsAffected => Ok(RowsAffected(self.next_u32() as usize)),
            TxString => Ok(StringValue(self.next_string())),
            TxStructure => Ok(StructureValue(self.next_struct()?)),
            TxTableValue => Ok(TableValue(self.next_table()?)),
            TxTuple => Ok(TupleValue(self.next_array()?)),
            TxUndefined => Ok(Undefined),
            TxUUID => Ok(UUIDValue(self.next_uuid())),
        }
    }

    pub fn position(&self) -> usize { self.offset }

    pub fn put_bytes(&mut self, bytes: &Vec<u8>) -> &Self {
        let required = self.offset + bytes.len();
        assert!(required <= self.buf.capacity());
        let mut pos = self.offset;
        if required > self.buf.len() { self.buf.resize(self.offset + bytes.len(), 0u8); }
        for byte in bytes {
            self.buf[pos] = *byte;
            pos += 1;
        }
        self.offset = pos;
        self
    }

    pub fn put_column(&mut self, column: &ColumnJs) -> &Self {
        self.put_string(column.get_name());
        self.put_string(column.get_column_type());
        self.put_string_opt(column.get_default_value());
        self
    }

    pub fn put_columns(&mut self, columns: &Vec<ColumnJs>) -> &Self {
        self.put_u16(columns.len() as u16);
        for column in columns {
            self.put_column(&column);
        }
        self
    }

    pub fn put_f32(&mut self, value: f32) -> &Self {
        let bytes = value.to_be_bytes();
        self.put_bytes(&bytes.to_vec())
    }

    pub fn put_f64(&mut self, value: f64) -> &Self {
        let bytes = value.to_be_bytes();
        self.put_bytes(&bytes.to_vec())
    }

    pub fn put_i8(&mut self, value: i8) -> &Self {
        self.buf[self.offset] = value as u8;
        self.offset += 1;
        self
    }

    pub fn put_i16(&mut self, value: i16) -> &Self {
        let bytes = value.to_be_bytes();
        self.put_bytes(&bytes.to_vec())
    }

    pub fn put_i32(&mut self, value: i32) -> &Self {
        let bytes = value.to_be_bytes();
        self.put_bytes(&bytes.to_vec())
    }

    pub fn put_i64(&mut self, value: i64) -> &Self {
        let bytes = value.to_be_bytes();
        self.put_bytes(&bytes.to_vec())
    }

    pub fn put_i128(&mut self, value: i128) -> &Self {
        let bytes = value.to_be_bytes();
        self.put_bytes(&bytes.to_vec())
    }

    pub fn put_rows(&mut self, phys_columns: Vec<TableColumn>, rows: Vec<Row>) -> &Self {
        let columns = ColumnJs::from_physical_columns(&phys_columns);
        self.put_columns(&columns);
        self.put_u64(rows.len() as u64);
        for row in rows {
            self.put_bytes(&row.encode(&phys_columns));
        }
        self
    }

    pub fn put_string(&mut self, string: &str) -> &Self {
        let bytes: Vec<u8> = string.bytes().collect();
        self.put_u64(bytes.len() as u64);
        self.put_bytes(&bytes);
        self
    }

    pub fn put_string_opt(&mut self, string: &Option<String>) -> &Self {
        let bytes: Vec<u8> = string.to_owned().map(|s| s.bytes().collect()).unwrap_or(Vec::new());
        self.put_u64(bytes.len() as u64);
        self.put_bytes(&bytes);
        self
    }

    pub fn put_struct(&mut self, structure: Structure) -> &Self {
        let columns = ColumnJs::from_physical_columns(&structure.get_columns());
        self.put_columns(&columns);
        self.put_bytes(&structure.encode());
        self
    }

    pub fn put_table(&mut self, mrc: ModelRowCollection) -> &Self {
        let columns = ColumnJs::from_physical_columns(mrc.get_columns());
        self.put_columns(&columns);
        self.put_bytes(&mrc.encode());
        self
    }

    pub fn put_u8(&mut self, value: u8) -> &Self {
        self.buf[self.offset] = value;
        self.offset += 1;
        self
    }

    pub fn put_u16(&mut self, value: u16) -> &Self {
        let bytes = value.to_be_bytes();
        self.put_bytes(&bytes.to_vec())
    }

    pub fn put_u32(&mut self, value: u32) -> &Self {
        let bytes = value.to_be_bytes();
        self.put_bytes(&bytes.to_vec())
    }

    pub fn put_u64(&mut self, value: u64) -> &Self {
        let bytes = value.to_be_bytes();
        self.put_bytes(&bytes.to_vec())
    }

    pub fn put_u128(&mut self, value: u128) -> &Self {
        let bytes = value.to_be_bytes();
        self.put_bytes(&bytes.to_vec())
    }

    pub fn put_value(&mut self, value: &TypedValue) -> &Self {
        self.put_u8(value.to_kind().to_u8());
        self.put_bytes(&value.encode())
    }

    pub fn remaining(&self) -> usize {
        self.limit - self.offset
    }

    /// changes the capacity to the buffer
    pub fn resize(&mut self, new_size: usize) {
        self.buf.resize(new_size, 0u8);
    }

    /// returns a vector contains all bytes from the current position until the end of the buffer
    pub fn to_array(&self) -> Vec<u8> {
        self.buf[self.offset..self.limit].to_vec()
    }

    fn validate(&self, delta: usize) -> usize {
        let new_offset = self.offset + delta;
        if new_offset > self.limit {
            panic!("Buffer underflow: {} + {} ({}) > {}", self.offset, delta, new_offset, self.limit)
        }
        delta
    }
}

impl Index<usize> for ByteCodeCompiler {
    type Output = u8;

    fn index(&self, index: usize) -> &Self::Output {
        &self.buf[index]
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::testdata::make_quote_columns;
    use crate::typed_values::TypedValue::Number;

    use super::*;

    #[test]
    fn test_columns() {
        let mut buffer = ByteCodeCompiler::new(512);
        buffer.put_columns(&make_quote_columns());
        assert_eq!(buffer.position(), 118);

        buffer.flip();
        assert_eq!(buffer.next_columns(), vec![
            ColumnJs::new("symbol", "String(8)", None),
            ColumnJs::new("exchange", "String(8)", None),
            ColumnJs::new("last_sale", "f64", None),
        ])
    }

    #[test]
    fn test_len() {
        let buffer = ByteCodeCompiler::wrap(vec![
            0xBA, 0xBE, 0xFA, 0xCE, 0xD0, 0xCA, 0xFE, 0x00,
        ]);
        assert_eq!(buffer.len(), 8)
    }

    #[test]
    fn test_next_f32() {
        let mut buffer = ByteCodeCompiler::wrap(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(buffer.limit(), 4);
        assert_eq!(buffer.next_f32(), -6.2598534e18)
    }

    #[test]
    fn test_next_f64() {
        let mut buffer = ByteCodeCompiler::wrap(vec![
            0xBA, 0xBE, 0xFA, 0xCE, 0xD0, 0xCA, 0xFE, 0x00,
        ]);
        assert_eq!(buffer.next_f64(), -1.0010086845065389e-25)
    }

    #[test]
    fn test_next_i8() {
        let mut buffer = ByteCodeCompiler::wrap(vec![0xAA]);
        assert_eq!(buffer.next_i8(), -86)
    }

    #[test]
    fn test_next_i16() {
        let mut buffer = ByteCodeCompiler::wrap(vec![0xBE, 0xEF]);
        assert_eq!(buffer.next_i16(), -16657)
    }

    #[test]
    fn test_next_i32() {
        let mut buffer = ByteCodeCompiler::wrap(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(buffer.next_i32(), -559038737)
    }

    #[test]
    fn test_next_i64() {
        let mut buffer = ByteCodeCompiler::wrap(vec![
            0xBA, 0xBE, 0xFA, 0xCE, 0xD0, 0xCA, 0xFE, 0x00,
        ]);
        assert_eq!(buffer.next_i64(), -4990275570906759680)
    }

    #[test]
    fn test_next_i128() {
        let mut buffer = ByteCodeCompiler::wrap(vec![
            0xBA, 0xBE, 0xFA, 0xCE, 0xBA, 0xDD, 0xEE, 0x00,
            0xCA, 0xFE, 0xDE, 0xAD, 0xBE, 0xEF, 0xBE, 0xAD,
        ]);
        assert_eq!(buffer.next_i128(), -92054336320587594627030856550656786771i128)
    }

    #[test]
    fn test_next_row_id() {
        let mut buffer = ByteCodeCompiler::wrap(vec![0x00, 0x00, 0x00, 0x07]);
        assert_eq!(buffer.next_row_id(), 7)
    }

    #[test]
    fn test_next_u8() {
        let mut buffer = ByteCodeCompiler::wrap(vec![0xAA]);
        assert_eq!(buffer.next_u8(), 0xAA)
    }

    #[test]
    fn test_next_u16() {
        let mut buffer = ByteCodeCompiler::wrap(vec![0xBE, 0xEF]);
        assert_eq!(buffer.next_u16(), 0xBEEF)
    }

    #[test]
    fn test_next_u32() {
        let mut buffer = ByteCodeCompiler::wrap(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(buffer.next_u32(), 0xDEADBEEF)
    }

    #[test]
    fn test_next_u64() {
        let mut buffer = ByteCodeCompiler::wrap(vec![
            0xBA, 0xBE, 0xFA, 0xCE, 0xD0, 0xCA, 0xFE, 0x00,
        ]);
        assert_eq!(buffer.next_u64(), 0xBABE_FACED_0_CAFE_00)
    }

    #[test]
    fn test_next_u128() {
        let bytes = vec![
            0xBA, 0xBE, 0xFA, 0xCE, 0xBA, 0xDD, 0xEE, 0x00,
            0xCA, 0xFE, 0xDE, 0xAD, 0xBE, 0xEF, 0xBE, 0xAD,
        ];
        let mut buffer = ByteCodeCompiler::from_bytes(&bytes, bytes.len());
        buffer.flip();
        assert_eq!(buffer.next_u128(), 0xBABE_FACE_BADDEE_00_CAFE_DEAD_BEEF_BEAD)
    }

    #[test]
    fn test_put_f32() {
        let mut buffer = ByteCodeCompiler::new(4);
        buffer.put_f32(125.0);
        buffer.flip();
        assert_eq!(buffer.next_f32(), 125.0);
    }

    #[test]
    fn test_put_f64() {
        let mut buffer = ByteCodeCompiler::new(8);
        buffer.put_f64(4_000_000.0);
        buffer.flip();
        assert_eq!(buffer.next_f64(), 4_000_000.0);
    }

    #[test]
    fn test_put_i8() {
        let mut buffer = ByteCodeCompiler::new(1);
        buffer.put_i8(99);
        buffer.flip();
        assert_eq!(buffer.next_i8(), 99);
    }

    #[test]
    fn test_put_i16() {
        let mut buffer = ByteCodeCompiler::new(2);
        buffer.put_i16(16235);
        buffer.flip();
        assert_eq!(buffer.next_i16(), 16235);
    }

    #[test]
    fn test_put_i32() {
        let mut buffer = ByteCodeCompiler::new(4);
        buffer.put_i32(125_000);
        buffer.flip();
        assert_eq!(buffer.next_i32(), 125_000);
    }

    #[test]
    fn test_put_i64() {
        let mut buffer = ByteCodeCompiler::new(8);
        buffer.put_i64(4_000_000);
        buffer.flip();
        assert_eq!(buffer.next_i64(), 4_000_000);
    }

    #[test]
    fn test_put_i128() {
        let mut buffer = ByteCodeCompiler::new(16);
        buffer.put_i128(4_000_000_000_000);
        buffer.flip();
        assert_eq!(buffer.next_i128(), 4_000_000_000_000);
    }

    #[test]
    fn test_put_u8() {
        let mut buffer = ByteCodeCompiler::new(8);
        buffer.put_u8(0x88);
        buffer.flip();
        assert_eq!(buffer.next_u8(), 0x88);
    }

    #[test]
    fn test_put_u16() {
        let mut buffer = ByteCodeCompiler::new(8);
        buffer.put_u16(0x8877);
        buffer.flip();
        assert_eq!(buffer.next_u16(), 0x8877);
    }

    #[test]
    fn test_put_u32() {
        let mut buffer = ByteCodeCompiler::new(8);
        buffer.put_u32(0x88776655);
        buffer.flip();
        assert_eq!(buffer.next_u32(), 0x88776655);
    }

    #[test]
    fn test_put_u64() {
        let mut buffer = ByteCodeCompiler::new(8);
        buffer.put_u64(0x88776655_44332211);
        buffer.flip();
        assert_eq!(buffer.next_u64(), 0x88776655_44332211);
    }

    #[test]
    fn test_put_u128() {
        let mut buffer = ByteCodeCompiler::new(16);
        buffer.put_u128(0xBABE_FACE_BADDEE_CAFE_DEAD_BEEF_BEAD_00);
        buffer.flip();
        assert_eq!(buffer.next_u128(), 0xBABE_FACE_BADDEE_CAFE_DEAD_BEEF_BEAD_00);
    }

    #[test]
    fn test_next_string() {
        let mut buffer = ByteCodeCompiler::new(20);
        buffer.put_string("Hello World!");
        buffer.flip();
        assert_eq!(buffer.to_array(), vec![
            0, 0, 0, 0, 0, 0, 0, 12,
            b'H', b'e', b'l', b'l', b'o', b' ', b'W', b'o', b'r', b'l', b'd', b'!',
        ]);
        assert_eq!(buffer.next_string(), "Hello World!");
    }

    #[test]
    fn test_next_string_opt() {
        let mut buffer = ByteCodeCompiler::wrap(vec![
            0, 0, 0, 0, 0, 0, 0, 12,
            b'H', b'e', b'l', b'l', b'o', b' ', b'W', b'o', b'r', b'l', b'd', b'!',
        ]);
        assert_eq!(buffer.next_string_opt(), Some("Hello World!".into()));
    }

    #[test]
    fn test_next_string_opt_is_empty() {
        let mut buffer = ByteCodeCompiler::wrap(vec![
            0, 0, 0, 0, 0, 0, 0, 0,
        ]);
        assert_eq!(buffer.next_string_opt(), None);
    }

    #[test]
    fn test_reading() {
        let mut buffer = ByteCodeCompiler::wrap(vec![
            0xBA, 0xBE, 0xFA, 0xCE, 0xCA, 0xFE, 0xDE, 0xAD,
        ]);
        assert_eq!(buffer.position(), 0);
        assert_eq!(buffer.next_u32(), 0xBABE_FACE);
        assert_eq!(buffer.position(), 4);
        assert_eq!(buffer.next_u16(), 0xCAFE);
        assert_eq!(buffer.position(), 6);
        assert_eq!(buffer.next_u16(), 0xDEAD);
        assert_eq!(buffer.position(), 8);
    }

    #[test]
    fn test_resize() {
        let mut buffer = ByteCodeCompiler::wrap(vec![
            0xBA, 0xBE, 0xFA, 0xCE, 0xD0, 0xCA, 0xFE, 0x00,
        ]);
        buffer.resize(13);
        assert_eq!(buffer.len(), 13)
    }

    #[test]
    fn test_to_array() {
        let mut buffer = ByteCodeCompiler::new(8);
        buffer.put_u64(0xBABE_FACE_CAFE_DEAD);
        buffer.flip();
        buffer.next_u32(); // skip 4 bytes
        assert_eq!(buffer.to_array(), vec![0xCA, 0xFE, 0xDE, 0xAD])
    }

    #[test]
    fn test_writing() {
        let mut buffer = ByteCodeCompiler::from_bytes(&vec![0xBA, 0xBE], 8);
        assert_eq!(buffer.position(), 2);
        buffer.put_u16(0xFACE);
        assert_eq!(buffer.position(), 4);
        buffer.put_u16(0xCAFE);
        assert_eq!(buffer.position(), 6);
        buffer.put_u16(0xDEAD);
        assert_eq!(buffer.position(), 8);
        buffer.flip();
        assert_eq!(buffer.next_u64(), 0xBABE_FACE_CAFE_DEAD)
    }

    #[test]
    fn test_expression_delete() {
        use crate::mnemonic::Mnemonic::*;
        let model = Mutate(Mutation::Delete {
            path: Box::new(Variable("stocks".into())),
            condition: Some(
                Box::new(LessOrEqual(
                    Box::new(Variable("last_sale".into())),
                    Box::new(Literal(Number(F64Value(1.0)))),
                ))
            ),
            limit: Some(Box::new(Literal(Number(I64Value(100))))),
        });
        let byte_code = vec![
            ExDelete.to_u8(),
            ExVarGet.to_u8(), ExLiteral.to_u8(), TxString.to_u8(),
            0, 0, 0, 0, 0, 0, 0, 6, b's', b't', b'o', b'c', b'k', b's',
            ExLessOrEqual.to_u8(),
            ExVarGet.to_u8(), ExLiteral.to_u8(), TxString.to_u8(),
            0, 0, 0, 0, 0, 0, 0, 9, b'l', b'a', b's', b't', b'_', b's', b'a', b'l', b'e',
            ExLiteral.to_u8(), TxNumberF64.to_u8(), 63, 240, 0, 0, 0, 0, 0, 0,
            ExLiteral.to_u8(), TxNumberI64.to_u8(), 0, 0, 0, 0, 0, 0, 0, 100,
        ];
        assert_eq!(ByteCodeCompiler::assemble(&model), byte_code);
        assert_eq!(ByteCodeCompiler::wrap(byte_code).next_expression().unwrap(), model)
    }

    #[test]
    fn test_literal() {
        let model = Literal(StringValue("hello".into()));
        let byte_code = vec![
            ExLiteral.to_u8(), TxString.to_u8(),
            0, 0, 0, 0, 0, 0, 0, 5, b'h', b'e', b'l', b'l', b'o',
        ];
        assert_eq!(ByteCodeCompiler::assemble(&model), byte_code);
        assert_eq!(ByteCodeCompiler::disassemble(&mut ByteCodeCompiler::wrap(byte_code)).unwrap(), model);
    }

    #[test]
    fn test_variable() {
        let model = Variable("name".into());
        let byte_code = vec![
            ExVarGet.to_u8(), ExLiteral.to_u8(), TxString.to_u8(),
            0, 0, 0, 0, 0, 0, 0, 4, b'n', b'a', b'm', b'e',
        ];
        assert_eq!(ByteCodeCompiler::assemble(&model), byte_code);
        assert_eq!(ByteCodeCompiler::disassemble(&mut ByteCodeCompiler::wrap(byte_code)).unwrap(), model);
    }

    #[test]
    fn test_math_ops() {
        let model = Plus(Box::new(Literal(Number(I64Value(2)))),
                         Box::new(Multiply(Box::new(Literal(Number(I64Value(4)))),
                                           Box::new(Literal(Number(I64Value(3)))))));
        let byte_code = vec![
            ExPlus.to_u8(),
            ExLiteral.to_u8(), TxNumberI64.to_u8(), 0, 0, 0, 0, 0, 0, 0, 2,
            ExMultiply.to_u8(),
            ExLiteral.to_u8(), TxNumberI64.to_u8(), 0, 0, 0, 0, 0, 0, 0, 4,
            ExLiteral.to_u8(), TxNumberI64.to_u8(), 0, 0, 0, 0, 0, 0, 0, 3,
        ];
        let mut buf = ByteCodeCompiler::wrap(byte_code.to_owned());
        assert_eq!(ByteCodeCompiler::assemble(&model), byte_code);
        assert_eq!(ByteCodeCompiler::disassemble(&mut buf).unwrap(), model);
    }

    #[test]
    fn test_select() {
        use Mnemonic::{ExLessOrEqual, ExSelect};
        let model = Inquire(Queryable::Select {
            fields: vec![Variable("symbol".into()), Variable("exchange".into()), Variable("last_sale".into())],
            from: Some(Box::new(Variable("stocks".into()))),
            condition: Some(
                Box::new(LessOrEqual(
                    Box::new(Variable("last_sale".into())),
                    Box::new(Literal(Number(F64Value(1.0)))),
                ))
            ),
            group_by: None,
            having: None,
            order_by: Some(vec![Variable("symbol".into())]),
            limit: Some(Box::new(Literal(Number(I64Value(5))))),
        });
        let byte_code = vec![
            // select symbol, exchange, last_sale
            ExSelect.to_u8(), 0, 0, 0, 0, 0, 0, 0, 3,
            ExVarGet.to_u8(), ExLiteral.to_u8(), TxString.to_u8(),
            0, 0, 0, 0, 0, 0, 0, 6, b's', b'y', b'm', b'b', b'o', b'l',
            ExVarGet.to_u8(), ExLiteral.to_u8(), TxString.to_u8(),
            0, 0, 0, 0, 0, 0, 0, 8, b'e', b'x', b'c', b'h', b'a', b'n', b'g', b'e',
            ExVarGet.to_u8(), ExLiteral.to_u8(), TxString.to_u8(),
            0, 0, 0, 0, 0, 0, 0, 9, b'l', b'a', b's', b't', b'_', b's', b'a', b'l', b'e',
            // from stocks
            0, 0, 0, 0, 0, 0, 0, 1,
            ExVarGet.to_u8(), ExLiteral.to_u8(), TxString.to_u8(),
            0, 0, 0, 0, 0, 0, 0, 6, b's', b't', b'o', b'c', b'k', b's',
            // where last_sale <= 1.0
            0, 0, 0, 0, 0, 0, 0, 1,
            ExLessOrEqual.to_u8(),
            ExVarGet.to_u8(), ExLiteral.to_u8(), TxString.to_u8(),
            0, 0, 0, 0, 0, 0, 0, 9, b'l', b'a', b's', b't', b'_', b's', b'a', b'l', b'e',
            ExLiteral.to_u8(), TxNumberF64.to_u8(),
            63, 240, 0, 0, 0, 0, 0, 0, // 1.0
            // group by ...
            0, 0, 0, 0, 0, 0, 0, 0,
            // having ...
            0, 0, 0, 0, 0, 0, 0, 0,
            // order by symbol
            0, 0, 0, 0, 0, 0, 0, 1,
            ExVarGet.to_u8(), ExLiteral.to_u8(), TxString.to_u8(),
            0, 0, 0, 0, 0, 0, 0, 6, b's', b'y', b'm', b'b', b'o', b'l',
            // limit 5
            0, 0, 0, 0, 0, 0, 0, 1,
            ExLiteral.to_u8(), TxNumberI64.to_u8(), 0, 0, 0, 0, 0, 0, 0, 5,
        ];
        assert_eq!(ByteCodeCompiler::assemble(&model), byte_code);
        assert_eq!(ByteCodeCompiler::disassemble(&mut ByteCodeCompiler::wrap(byte_code)).unwrap(), model)
    }

    #[test]
    fn test_update() {
        use Mnemonic::{ExEqual, ExJsonLiteral, ExUpdate, ExVia};
        let model = Mutate(Mutation::Update {
            path: Box::new(Variable("stocks".into())),
            source: Box::new(Via(Box::new(JSONLiteral(vec![
                ("last_sale".into(), Literal(Number(F64Value(0.1111)))),
            ])))),
            condition: Some(
                Box::new(Equal(
                    Box::new(Variable("exchange".into())),
                    Box::new(Literal(StringValue("NYSE".into()))),
                ))
            ),
            limit: Some(Box::new(Literal(Number(I64Value(10))))),
        });
        let byte_code = vec![
            // update stocks
            ExUpdate.to_u8(), 0, 0, 0, 0, 0, 0, 0, 4,
            ExVarGet.to_u8(), ExLiteral.to_u8(), TxString.to_u8(),
            0, 0, 0, 0, 0, 0, 0, 6, b's', b't', b'o', b'c', b'k', b's',
            // via { last_sale: 0.1111 }
            ExVia.to_u8(), ExJsonLiteral.to_u8(), 0, 0, 0, 0, 0, 0, 0, 2,
            ExLiteral.to_u8(), TxString.to_u8(),
            0, 0, 0, 0, 0, 0, 0, 9, b'l', b'a', b's', b't', b'_', b's', b'a', b'l', b'e',
            ExLiteral.to_u8(), TxNumberF64.to_u8(),
            63, 188, 113, 12, 178, 149, 233, 226, // 0.1111
            // where symbol == 'ABC'
            ExEqual.to_u8(),
            ExVarGet.to_u8(), ExLiteral.to_u8(), TxString.to_u8(),
            0, 0, 0, 0, 0, 0, 0, 8, b'e', b'x', b'c', b'h', b'a', b'n', b'g', b'e',
            ExLiteral.to_u8(), TxString.to_u8(),
            0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E',
            // limit 10
            ExLiteral.to_u8(), TxNumberI64.to_u8(), 0, 0, 0, 0, 0, 0, 0, 10,
        ];
        assert_eq!(ByteCodeCompiler::assemble(&model), byte_code);
        assert_eq!(ByteCodeCompiler::disassemble(&mut ByteCodeCompiler::wrap(byte_code)).unwrap(), model)
    }

    #[test]
    fn test_json_literal() {
        use Mnemonic::ExJsonLiteral;
        let model = JSONLiteral(vec![
            ("symbol".to_string(), Literal(StringValue("TRX".into()))),
            ("exchange".to_string(), Literal(StringValue("NYSE".into()))),
            ("last_sale".to_string(), Literal(Number(F64Value(11.1111)))),
        ]);
        let byte_code = vec![
            ExJsonLiteral.to_u8(), 0, 0, 0, 0, 0, 0, 0, 6,
            ExLiteral.to_u8(), TxString.to_u8(), 0, 0, 0, 0, 0, 0, 0, 6, b's', b'y', b'm', b'b', b'o', b'l',
            ExLiteral.to_u8(), TxString.to_u8(), 0, 0, 0, 0, 0, 0, 0, 3, b'T', b'R', b'X',
            ExLiteral.to_u8(), TxString.to_u8(), 0, 0, 0, 0, 0, 0, 0, 8, b'e', b'x', b'c', b'h', b'a', b'n', b'g', b'e',
            ExLiteral.to_u8(), TxString.to_u8(), 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E',
            ExLiteral.to_u8(), TxString.to_u8(), 0, 0, 0, 0, 0, 0, 0, 9, b'l', b'a', b's', b't', b'_', b's', b'a', b'l', b'e',
            ExLiteral.to_u8(), TxNumberF64.to_u8(), 64, 38, 56, 226, 25, 101, 43, 212,
        ];
        assert_eq!(ByteCodeCompiler::assemble(&model), byte_code);
        assert_eq!(ByteCodeCompiler::disassemble(&mut ByteCodeCompiler::wrap(byte_code)).unwrap(), model);
    }
}