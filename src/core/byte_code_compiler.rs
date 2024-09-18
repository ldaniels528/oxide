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
            And(a, b) => Self::encode(EAnd.to_u8(), vec![a, b]),
            ArrayLiteral(items) => Self::encode_vec(EArrayLit.to_u8(), items),
            AsValue(name, expr) =>
                Self::encode(EAsValue.to_u8(), vec![&Literal(StringValue(name.to_string())), expr]),
            Between(a, b, c) =>
                Self::encode(EBetween.to_u8(), vec![a, b, c]),
            Betwixt(a, b, c) =>
                Self::encode(EBetwixt.to_u8(), vec![a, b, c]),
            BitwiseAnd(a, b) =>
                Self::encode(EBitwiseAnd.to_u8(), vec![a, b]),
            BitwiseOr(a, b) =>
                Self::encode(EBitwiseOr.to_u8(), vec![a, b]),
            BitwiseXor(a, b) =>
                Self::encode(EBitwiseXor.to_u8(), vec![a, b]),
            CodeBlock(ops) => Self::encode_vec(ECodeBlock.to_u8(), ops),
            ColumnSet(columns) => Self::encode_columns(columns),
            Contains(a, b) => Self::encode(EContains.to_u8(), vec![a, b]),
            Divide(a, b) => Self::encode(EDivide.to_u8(), vec![a, b]),
            ElementAt(a, b) => Self::encode(EElemIndex.to_u8(), vec![a, b]),
            Equal(a, b) => Self::encode(EEqual.to_u8(), vec![a, b]),
            Factorial(a) => Self::encode(EFactorial.to_u8(), vec![a]),
            Feature { title, scenarios } => {
                let mut values = Vec::new();
                values.push(title.deref());
                values.extend(scenarios);
                Self::encode(EFeature.to_u8(), values)
            }
            From(src) => Self::encode(EFrom.to_u8(), vec![src]),
            FunctionCall { fx, args } => {
                let mut values = vec![fx.deref()];
                values.extend(args);
                Self::encode(EWhile.to_u8(), values)
            }
            GreaterThan(a, b) => Self::encode(EGreaterThan.to_u8(), vec![a, b]),
            GreaterOrEqual(a, b) => Self::encode(EGreaterOrEqual.to_u8(), vec![a, b]),
            HTTP { method, url, body, headers, multipart } =>
                Self::encode(EHttp.to_u8(), vec![method, url, &Self::get_or_undef(body), &Self::get_or_undef(headers), &Self::get_or_undef(multipart)]),
            If { condition, a, b } =>
                Self::encode(EIf.to_u8(), vec![condition, a, &Self::get_or_undef(b)]),
            Include(path) => Self::encode(EInclude.to_u8(), vec![path]),
            Inquire(q) => Self::assemble_inquiry(q),
            JSONLiteral(tuples) => Self::assemble_json_object(tuples),
            LessThan(a, b) => Self::encode(ELessThan.to_u8(), vec![a, b]),
            LessOrEqual(a, b) => Self::encode(ELessOrEqual.to_u8(), vec![a, b]),
            Literal(value) => Self::encode_value(ELiteral.to_u8(), value),
            Minus(a, b) => Self::encode(EMinus.to_u8(), vec![a, b]),
            Modulo(a, b) => Self::encode(EModulo.to_u8(), vec![a, b]),
            Multiply(a, b) => Self::encode(EMultiply.to_u8(), vec![a, b]),
            MustAck(a) => Self::encode(EMustAck.to_u8(), vec![a]),
            MustDie(a) => Self::encode(EMustDie.to_u8(), vec![a]),
            MustIgnoreAck(a) => Self::encode(EMustIgnoreAck.to_u8(), vec![a]),
            MustNotAck(a) => Self::encode(EMustNotAck.to_u8(), vec![a]),
            Mutate(m) => Self::assemble_modification(m),
            Neg(a) => Self::encode(ENeg.to_u8(), vec![a]),
            Not(a) => Self::encode(ENot.to_u8(), vec![a]),
            NotEqual(a, b) => Self::encode(ENotEqual.to_u8(), vec![a, b]),
            Ns(a) => Self::encode(ENs.to_u8(), vec![a]),
            Or(a, b) => Self::encode(EOr.to_u8(), vec![a, b]),
            Perform(i) => Self::assemble_infrastructure(i),
            Plus(a, b) => Self::encode(EPlus.to_u8(), vec![a, b]),
            Pow(a, b) => Self::encode(EPow.to_u8(), vec![a, b]),
            Range(a, b) => Self::encode(ERange.to_u8(), vec![a, b]),
            Return(a) => Self::encode_vec(EReturn.to_u8(), a),
            Scenario { title, verifications, inherits } => {
                let mut values = Vec::new();
                values.push(title.deref());
                values.push(match inherits {
                    Some(value) => value,
                    None => &UNDEFINED,
                });
                values.extend(verifications);
                Self::encode(EFeature.to_u8(), values)
            }
            SERVE(a) => Self::encode(EServe.to_u8(), vec![a]),
            SetVariable(name, expr) =>
                Self::encode(EVarSet.to_u8(), vec![&Literal(StringValue(name.into())), expr]),
            ShiftLeft(a, b) => Self::encode(EShiftLeft.to_u8(), vec![a, b]),
            ShiftRight(a, b) => Self::encode(EShiftRight.to_u8(), vec![a, b]),
            TupleLiteral(values) => Self::encode_vec(ETuple.to_u8(), values),
            Variable(name) => Self::encode(EVarGet.to_u8(), vec![&Literal(StringValue(name.into()))]),
            Via(src) => Self::encode(EVia.to_u8(), vec![src]),
            While { condition, code } =>
                Self::encode(EWhile.to_u8(), vec![condition, code]),
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
                Self::encode(ECreateIndex.to_u8(), args)
            }
            Create { path, entity: TableEntity { columns, from } } =>
                Self::encode(ECreateTable.to_u8(), vec![path, &ColumnSet(columns.to_owned()), &Self::get_or_undef(from)]),
            Declare(IndexEntity { columns }) => {
                let mut args = Vec::new();
                args.extend(columns);
                Self::encode(EDeclareIndex.to_u8(), args)
            }
            Declare(TableEntity { columns, from }) =>
                Self::encode(EDeclareTable.to_u8(), vec![&ColumnSet(columns.to_owned()), &Self::get_or_undef(from)]),
            Drop(IndexTarget { path }) => Self::encode(EDrop.to_u8(), vec![path]),
            Drop(TableTarget { path }) => Self::encode(EDrop.to_u8(), vec![path]),
        }
    }

    /// compiles the [Mutation] into binary code
    fn assemble_modification(expression: &Mutation) -> Vec<u8> {
        use Mutation::*;
        use Mnemonic::*;
        match expression {
            Append { path, source } => Self::encode(EAppend.to_u8(), vec![path, source]),
            Compact { path } => Self::encode(ECompact.to_u8(), vec![path]),
            Delete { path, condition, limit } =>
                Self::encode(EDelete.to_u8(), vec![path, &Self::get_or_undef(condition), &Self::get_or_undef(limit)]),
            IntoNs(a, b) => Self::encode(EIntoNs.to_u8(), vec![a, b]),
            Overwrite { path, source, condition, limit } => {
                let mut args = Vec::new();
                args.push(path.deref().to_owned());
                args.push(source.deref().to_owned());
                args.push(Self::get_or_undef(condition));
                args.push(Self::get_or_undef(limit));
                Self::encode_vec(EOverwrite.to_u8(), &args)
            }
            Scan { path } => Self::encode(EScan.to_u8(), vec![path]),
            Truncate { path, limit: new_size } =>
                Self::encode(ETruncate.to_u8(), vec![path, &Self::get_or_undef(new_size)]),
            Undelete { path, condition, limit } =>
                Self::encode(EUndelete.to_u8(), vec![path, &Self::get_or_undef(condition), &Self::get_or_undef(limit)]),
            Update { path, source, condition, limit } => {
                let mut args: Vec<Expression> = Vec::new();
                args.push(path.deref().to_owned());
                args.push(source.deref().to_owned());
                args.push(Self::get_or_undef(condition));
                args.push(Self::get_or_undef(limit));
                Self::encode_vec(EUpdate.to_u8(), &args)
            }
        }
    }

    /// compiles the [Queryable] into binary code
    fn assemble_inquiry(expression: &Queryable) -> Vec<u8> {
        use Queryable::*;
        use Mnemonic::*;
        match expression {
            Describe(a) => Self::encode(EDescribe.to_u8(), vec![a]),
            Limit { from, limit } => Self::encode(ELimit.to_u8(), vec![from, limit]),
            Reverse(a) => Self::encode(EReverse.to_u8(), vec![a]),
            Select {
                fields, from, condition,
                group_by, having,
                order_by, limit
            } => Self::assemble_select(fields, from, condition, group_by, having, order_by, limit),
            Where { from, condition } =>
                Self::encode(EWhere.to_u8(), vec![from, condition]),
        }
    }

    /// compiles the collection of expressions into binary code
    fn assemble_fully(expressions: &Vec<Expression>) -> Vec<u8> {
        expressions.iter().flat_map(|expr| Self::assemble(expr)).collect::<Vec<u8>>()
    }

    /// compiles a JSON-like collection of expressions into binary code
    fn assemble_json_object(tuples: &Vec<(String, Expression)>) -> Vec<u8> {
        use Mnemonic::EJsonLiteral;
        let expressions = tuples.iter().flat_map(|(k, v)| {
            vec![Literal(StringValue(k.into())), v.to_owned()]
        }).collect::<Vec<Expression>>();
        Self::encode_vec(EJsonLiteral.to_u8(), &expressions)
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
        use Mnemonic::ESelect;
        let mut byte_code = vec![ESelect.to_u8()];
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
            AckType => Self::assemble_bytes(TAck.to_u8(), &Vec::new()),
            BackDoorType => Self::assemble_bytes(TBackDoor.to_u8(), &vec![0]),
            BLOBType(size) => Self::assemble_bytes(TBlob.to_u8(), &Self::assemble_usize(*size)),
            BooleanType => Self::assemble_bytes(TBoolean.to_u8(), &Vec::new()),
            CLOBType(size) => Self::assemble_bytes(TClob.to_u8(), &Self::assemble_usize(*size)),
            DateType => Self::assemble_bytes(TDate.to_u8(), &Vec::new()),
            EnumType(labels) => Self::assemble_strings(TEnum.to_u8(), &labels),
            ErrorType => Self::assemble_bytes(TString.to_u8(), &Self::assemble_usize(256)),
            FunctionType(columns) => Self::assemble_bytes(TFunction.to_u8(), &Self::encode_columns(columns)),
            JSONType => Self::assemble_bytes(TJsonObject.to_u8(), &Vec::new()),
            NumberType(kind) => {
                use crate::numbers::NumberKind::*;
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
            RowsAffectedType => Self::assemble_bytes(TRowsAffected.to_u8(), &Vec::new()),
            StringType(size) => Self::assemble_bytes(TString.to_u8(), &Self::assemble_usize(*size)),
            StructureType(columns) => Self::assemble_bytes(TStructure.to_u8(), &Self::encode_columns(columns)),
            TableType(columns) => Self::assemble_bytes(TTableValue.to_u8(), &Self::encode_columns(columns)),
            UUIDType => Self::assemble_bytes(TUUID.to_u8(), &Vec::new()),
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
            EAnd => Ok(And(self.decode_box()?, self.decode_box()?)),
            EAppend => Ok(Mutate(Mutation::Append { path: self.decode_box()?, source: self.decode_box()? })),
            EArrayLit => Ok(ArrayLiteral(self.decode_array()?)),
            EAsValue => Ok(AsValue(self.next_string(), self.decode_box()?)),
            ECompact => Ok(Mutate(Mutation::Compact { path: self.decode_box()? })),
            EElemIndex => Ok(ElementAt(self.decode_box()?, self.decode_box()?)),
            EFeature => Ok(Feature { title: self.decode_box()?, scenarios: self.decode_array()? }),
            EBetween => Ok(Between(self.decode_box()?, self.decode_box()?, self.decode_box()?)),
            EBetwixt => Ok(Betwixt(self.decode_box()?, self.decode_box()?, self.decode_box()?)),
            EBitwiseAnd => Ok(BitwiseAnd(self.decode_box()?, self.decode_box()?)),
            EBitwiseOr => Ok(BitwiseOr(self.decode_box()?, self.decode_box()?)),
            EBitwiseXor => Ok(BitwiseXor(self.decode_box()?, self.decode_box()?)),
            ECodeBlock => Ok(CodeBlock(self.decode_array()?)),
            EContains => Ok(Contains(self.decode_box()?, self.decode_box()?)),
            ECreateIndex => {
                let mut args = self.decode_array()?;
                assert!(args.len() >= 2);
                Ok(Perform(Infrastructure::Create {
                    path: Box::new(args.pop().unwrap().to_owned()),
                    entity: IndexEntity {
                        columns: args,
                    },
                }))
            }
            ECreateTable =>
                Ok(Perform(Infrastructure::Create {
                    path: self.decode_box()?,
                    entity: TableEntity {
                        columns: self.next_columns(),
                        from: self.decode_opt()?,
                    },
                })),
            ECsv => Ok(Literal(BackDoor(BackDoorFunction::from_u8(self.next_u8())))),
            EDeclareIndex =>
                Ok(Perform(Infrastructure::Declare(IndexEntity { columns: self.decode_array()? }))),
            EDeclareTable =>
                Ok(Perform(Infrastructure::Declare(TableEntity { columns: self.next_columns(), from: self.decode_opt()? }))),
            EDelete => Ok(Mutate(Mutation::Delete {
                path: self.decode_box()?,
                condition: self.decode_opt()?,
                limit: self.decode_opt()?,
            })),
            EDescribe => Ok(Inquire(Describe(self.decode_box()?))),
            EDivide => Ok(Divide(self.decode_box()?, self.decode_box()?)),
            EDrop => Ok(Perform(Infrastructure::Drop(TableTarget { path: self.decode_box()? }))),
            EEqual => Ok(Equal(self.decode_box()?, self.decode_box()?)),
            EFactorial => Ok(Factorial(self.decode_box()?)),
            EFrom => Ok(From(self.decode_box()?)),
            EFunction => Ok(Literal(Function {
                params: self.next_columns(),
                code: Self::decode_box(self)?,
            })),
            EGreaterOrEqual => Ok(GreaterOrEqual(self.decode_box()?, self.decode_box()?)),
            EGreaterThan => Ok(GreaterThan(self.decode_box()?, self.decode_box()?)),
            EHttp => Ok(HTTP {
                method: self.decode_box()?,
                url: self.decode_box()?,
                body: self.decode_opt()?,
                headers: self.decode_opt()?,
                multipart: self.decode_opt()?,
            }),
            EIf => Ok(If { condition: self.decode_box()?, a: self.decode_box()?, b: self.decode_opt()? }),
            EInclude => Ok(Include(self.decode_box()?)),
            EIntoNs => Ok(Mutate(Mutation::IntoNs(self.decode_box()?, self.decode_box()?))),
            EJsonLiteral => Ok(JSONLiteral(self.decode_json_object()?)),
            ELessOrEqual => Ok(LessOrEqual(self.decode_box()?, self.decode_box()?)),
            ELessThan => Ok(LessThan(self.decode_box()?, self.decode_box()?)),
            ELimit => Ok(Inquire(Queryable::Limit { from: self.decode_box()?, limit: self.decode_box()? })),
            ELiteral => Ok(Literal(self.next_value()?)),
            EMinus => Ok(Minus(self.decode_box()?, self.decode_box()?)),
            EModulo => Ok(Modulo(self.decode_box()?, self.decode_box()?)),
            EMultiply => Ok(Multiply(self.decode_box()?, self.decode_box()?)),
            EMustAck => Ok(MustAck(self.decode_box()?)),
            EMustDie => Ok(MustDie(self.decode_box()?)),
            EMustIgnoreAck => Ok(MustIgnoreAck(self.decode_box()?)),
            EMustNotAck => Ok(MustNotAck(self.decode_box()?)),
            ENeg => Ok(Neg(self.decode_box()?)),
            ENot => Ok(Not(self.decode_box()?)),
            ENotEqual => Ok(NotEqual(self.decode_box()?, self.decode_box()?)),
            ENs => Ok(Ns(self.decode_box()?)),
            EOr => Ok(Or(self.decode_box()?, self.decode_box()?)),
            EOverwrite => Ok(Mutate(Mutation::Overwrite {
                path: self.decode_box()?,
                source: self.decode_box()?,
                condition: self.decode_opt()?,
                limit: self.decode_opt()?,
            })),
            EPlus => Ok(Plus(self.decode_box()?, self.decode_box()?)),
            EPow => Ok(Pow(self.decode_box()?, self.decode_box()?)),
            ERange => Ok(Range(self.decode_box()?, self.decode_box()?)),
            EReturn => Ok(Return(self.decode_array()?)),
            EReverse => Ok(Inquire(Queryable::Reverse(self.decode_box()?))),
            EScan => Ok(Mutate(Mutation::Scan { path: self.decode_box()? })),
            EScenario => Ok(Scenario { title: self.decode_box()?, verifications: self.decode_array()?, inherits: self.decode_opt()? }),
            ESelect => self.disassemble_select(),
            EServe => Ok(SERVE(self.decode_box()?)),
            EShiftLeft => Ok(ShiftLeft(self.decode_box()?, self.decode_box()?)),
            EShiftRight => Ok(ShiftRight(self.decode_box()?, self.decode_box()?)),
            EStderr => Ok(Literal(BackDoor(BackDoorFunction::StdErr))),
            EStdout => Ok(Literal(BackDoor(BackDoorFunction::StdOut))),
            ETruncate => Ok(Mutate(Mutation::Truncate { path: self.decode_box()?, limit: self.decode_opt()? })),
            ETuple => Ok(TupleLiteral(self.decode_array()?)),
            EUndelete =>
                Ok(Mutate(Mutation::Undelete {
                    path: self.decode_box()?,
                    condition: self.decode_opt()?,
                    limit: self.decode_opt()?,
                })),
            EUpdate => self.disassemble_update(),
            EVarGet =>
                match self.next_expression()? {
                    Literal(StringValue(name)) => Ok(Variable(name)),
                    z => fail_expr("Expected identifier", &z)
                }
            EVarSet =>
                match self.next_expression()? {
                    Literal(StringValue(name)) => Ok(SetVariable(name, self.decode_box()?)),
                    z => fail_expr("Expected String", &z)
                }
            EVia => Ok(Via(self.decode_box()?)),
            EWhere => Ok(Inquire(Queryable::Where { from: self.decode_box()?, condition: self.decode_box()? })),
            EWhile => Ok(While { condition: self.decode_box()?, code: self.decode_box()? }),
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
            println!("decode_opt[1]: {:?} | {}", Mnemonic::from_u8(code), code);
            match Mnemonic::from_u8(code) {
                //TUndefined => Ok(None),
                _ => {
                    self.move_rel(-1);
                    let expr = self.decode_box()?;
                    println!("decode_opt[2]: {:?} <{}>", expr, code);
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
        Structure::from_logical_columns_and_values(columns, self.next_array()?)
    }

    pub fn next_table(&mut self) -> std::io::Result<ModelRowCollection> {
        let rows = self.next_rows()?;
        Ok(ModelRowCollection::from_rows(rows))
    }

    pub fn next_table_with_columns(
        &mut self,
        columns: &Vec<ColumnJs>,
    ) -> std::io::Result<ModelRowCollection> {
        let rows = self.next_rows_with_columns(columns)?;
        Ok(ModelRowCollection::from_rows(rows))
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
        println!("next_value[0]");
        let code = self.next_u8();
        println!("next_value[1]: {:?} | {} [pos {}, left {}]",
                 DataTypeKind::from_u8(code), code, self.position(), self.remaining());
        let x = match DataTypeKind::from_u8(code) {
            TAck => Ok(Ack),
            TArray => Ok(Array(self.next_array()?)),
            TBackDoor => Ok(ErrorValue("BackDoors cannot be decoded".into())),
            TBlob => Ok(BLOB(self.next_blob())),
            TBoolean => Ok(Boolean(self.next_bool())),
            TClob => Ok(CLOB(self.next_clob())),
            TDate => Ok(DateValue(self.next_i64())),
            TEnum => Ok(Array(self.next_array()?)),
            TError => Ok(ErrorValue(self.next_string())),
            TFunction => Ok(Function {
                params: self.next_columns(),
                code: self.decode_box()?,
            }),
            TJsonObject => Ok(JSONValue(self.next_json()?)),
            TNamespace =>
                match self.next_string().split('.').collect::<Vec<_>>().as_slice() {
                    [d, s, n] => Ok(NamespaceValue(d.to_string(), s.to_string(), n.to_string())),
                    _ => fail("Invalid namespace reference (ex. 'a.b.stocks')")
                }
            TNull => Ok(Null),
            TNumberF32 => Ok(Number(Float32Value(self.next_f32()))),
            TNumberF64 => Ok(Number(Float64Value(self.next_f64()))),
            TNumberI8 => Ok(Number(Int8Value(self.next_i8()))),
            TNumberI16 => Ok(Number(Int16Value(self.next_i16()))),
            TNumberI32 => Ok(Number(Int32Value(self.next_i32()))),
            TNumberI64 => Ok(Number(Int64Value(self.next_i64()))),
            TNumberI128 => Ok(Number(Int128Value(self.next_i128()))),
            TNumberU8 => Ok(Number(UInt8Value(self.next_u8()))),
            TNumberU16 => Ok(Number(UInt16Value(self.next_u16()))),
            TNumberU32 => Ok(Number(UInt32Value(self.next_u32()))),
            TNumberU64 => Ok(Number(UInt64Value(self.next_u64()))),
            TNumberU128 => Ok(Number(UInt128Value(self.next_u128()))),
            TNumberNan => Ok(Number(NotANumber)),
            TRowsAffected => Ok(RowsAffected(self.next_u32() as usize)),
            TString => Ok(StringValue(self.next_string())),
            TStructure => Ok(StructureValue(self.next_struct()?)),
            TTableValue => Ok(TableValue(self.next_table()?)),
            TTuple => Ok(TupleValue(self.next_array()?)),
            TUndefined => Ok(Undefined),
            TUUID => Ok(UUIDValue(self.next_uuid())),
        };
        println!("next_value[2]: {:?}", x);
        x
    }

    pub fn peek(&self) -> u8 {
        if self.remaining() > 0 {
            codec::decode_u8(&self.buf, self.offset, |b| b)
        } else { 0 }
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

    pub fn put_rows(&mut self, rows: Vec<Row>) -> &Self {
        let t_columns: Vec<TableColumn> = if rows.len() == 0 { Vec::new() } else {
            rows[0].get_columns().to_owned()
        };
        let columns = ColumnJs::from_physical_columns(&t_columns);
        self.put_columns(&columns);
        self.put_u64(rows.len() as u64);
        for row in rows {
            self.put_bytes(&row.encode());
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
                    Box::new(Literal(Number(Float64Value(1.0)))),
                ))
            ),
            limit: Some(Box::new(Literal(Number(Int64Value(100))))),
        });
        let byte_code = vec![
            EDelete.to_u8(),
            EVarGet.to_u8(), ELiteral.to_u8(), TString.to_u8(),
            0, 0, 0, 0, 0, 0, 0, 6, b's', b't', b'o', b'c', b'k', b's',
            ELessOrEqual.to_u8(),
            EVarGet.to_u8(), ELiteral.to_u8(), TString.to_u8(),
            0, 0, 0, 0, 0, 0, 0, 9, b'l', b'a', b's', b't', b'_', b's', b'a', b'l', b'e',
            ELiteral.to_u8(), TNumberF64.to_u8(), 63, 240, 0, 0, 0, 0, 0, 0,
            ELiteral.to_u8(), TNumberI64.to_u8(), 0, 0, 0, 0, 0, 0, 0, 100,
        ];
        assert_eq!(ByteCodeCompiler::assemble(&model), byte_code);
        assert_eq!(ByteCodeCompiler::wrap(byte_code).next_expression().unwrap(), model)
    }

    #[test]
    fn test_literal() {
        let model = Literal(StringValue("hello".into()));
        let byte_code = vec![
            ELiteral.to_u8(), TString.to_u8(),
            0, 0, 0, 0, 0, 0, 0, 5, b'h', b'e', b'l', b'l', b'o',
        ];
        assert_eq!(ByteCodeCompiler::assemble(&model), byte_code);
        assert_eq!(ByteCodeCompiler::disassemble(&mut ByteCodeCompiler::wrap(byte_code)).unwrap(), model);
    }

    #[test]
    fn test_variable() {
        let model = Variable("name".into());
        let byte_code = vec![
            EVarGet.to_u8(), ELiteral.to_u8(), TString.to_u8(),
            0, 0, 0, 0, 0, 0, 0, 4, b'n', b'a', b'm', b'e',
        ];
        assert_eq!(ByteCodeCompiler::assemble(&model), byte_code);
        assert_eq!(ByteCodeCompiler::disassemble(&mut ByteCodeCompiler::wrap(byte_code)).unwrap(), model);
    }

    #[test]
    fn test_math_ops() {
        let model = Plus(Box::new(Literal(Number(Int64Value(2)))),
                         Box::new(Multiply(Box::new(Literal(Number(Int64Value(4)))),
                                           Box::new(Literal(Number(Int64Value(3)))))));
        let byte_code = vec![
            EPlus.to_u8(),
            ELiteral.to_u8(), TNumberI64.to_u8(), 0, 0, 0, 0, 0, 0, 0, 2,
            EMultiply.to_u8(),
            ELiteral.to_u8(), TNumberI64.to_u8(), 0, 0, 0, 0, 0, 0, 0, 4,
            ELiteral.to_u8(), TNumberI64.to_u8(), 0, 0, 0, 0, 0, 0, 0, 3,
        ];
        let mut buf = ByteCodeCompiler::wrap(byte_code.to_owned());
        assert_eq!(ByteCodeCompiler::assemble(&model), byte_code);
        assert_eq!(ByteCodeCompiler::disassemble(&mut buf).unwrap(), model);
    }

    #[test]
    fn test_select() {
        use Mnemonic::{ELessOrEqual, ESelect};
        let model = Inquire(Queryable::Select {
            fields: vec![Variable("symbol".into()), Variable("exchange".into()), Variable("last_sale".into())],
            from: Some(Box::new(Variable("stocks".into()))),
            condition: Some(
                Box::new(LessOrEqual(
                    Box::new(Variable("last_sale".into())),
                    Box::new(Literal(Number(Float64Value(1.0)))),
                ))
            ),
            group_by: None,
            having: None,
            order_by: Some(vec![Variable("symbol".into())]),
            limit: Some(Box::new(Literal(Number(Int64Value(5))))),
        });
        let byte_code = vec![
            // select symbol, exchange, last_sale
            ESelect.to_u8(), 0, 0, 0, 0, 0, 0, 0, 3,
            EVarGet.to_u8(), ELiteral.to_u8(), TString.to_u8(),
            0, 0, 0, 0, 0, 0, 0, 6, b's', b'y', b'm', b'b', b'o', b'l',
            EVarGet.to_u8(), ELiteral.to_u8(), TString.to_u8(),
            0, 0, 0, 0, 0, 0, 0, 8, b'e', b'x', b'c', b'h', b'a', b'n', b'g', b'e',
            EVarGet.to_u8(), ELiteral.to_u8(), TString.to_u8(),
            0, 0, 0, 0, 0, 0, 0, 9, b'l', b'a', b's', b't', b'_', b's', b'a', b'l', b'e',
            // from stocks
            0, 0, 0, 0, 0, 0, 0, 1,
            EVarGet.to_u8(), ELiteral.to_u8(), TString.to_u8(),
            0, 0, 0, 0, 0, 0, 0, 6, b's', b't', b'o', b'c', b'k', b's',
            // where last_sale <= 1.0
            0, 0, 0, 0, 0, 0, 0, 1,
            ELessOrEqual.to_u8(),
            EVarGet.to_u8(), ELiteral.to_u8(), TString.to_u8(),
            0, 0, 0, 0, 0, 0, 0, 9, b'l', b'a', b's', b't', b'_', b's', b'a', b'l', b'e',
            ELiteral.to_u8(), TNumberF64.to_u8(),
            63, 240, 0, 0, 0, 0, 0, 0, // 1.0
            // group by ...
            0, 0, 0, 0, 0, 0, 0, 0,
            // having ...
            0, 0, 0, 0, 0, 0, 0, 0,
            // order by symbol
            0, 0, 0, 0, 0, 0, 0, 1,
            EVarGet.to_u8(), ELiteral.to_u8(), TString.to_u8(),
            0, 0, 0, 0, 0, 0, 0, 6, b's', b'y', b'm', b'b', b'o', b'l',
            // limit 5
            0, 0, 0, 0, 0, 0, 0, 1,
            ELiteral.to_u8(), TNumberI64.to_u8(), 0, 0, 0, 0, 0, 0, 0, 5,
        ];
        assert_eq!(ByteCodeCompiler::assemble(&model), byte_code);
        assert_eq!(ByteCodeCompiler::disassemble(&mut ByteCodeCompiler::wrap(byte_code)).unwrap(), model)
    }

    #[test]
    fn test_update() {
        use Mnemonic::{EEqual, EJsonLiteral, EUpdate, EVia};
        let model = Mutate(Mutation::Update {
            path: Box::new(Variable("stocks".into())),
            source: Box::new(Via(Box::new(JSONLiteral(vec![
                ("last_sale".into(), Literal(Number(Float64Value(0.1111)))),
            ])))),
            condition: Some(
                Box::new(Equal(
                    Box::new(Variable("exchange".into())),
                    Box::new(Literal(StringValue("NYSE".into()))),
                ))
            ),
            limit: Some(Box::new(Literal(Number(Int64Value(10))))),
        });
        let byte_code = vec![
            // update stocks
            EUpdate.to_u8(), 0, 0, 0, 0, 0, 0, 0, 4,
            EVarGet.to_u8(), ELiteral.to_u8(), TString.to_u8(),
            0, 0, 0, 0, 0, 0, 0, 6, b's', b't', b'o', b'c', b'k', b's',
            // via { last_sale: 0.1111 }
            EVia.to_u8(), EJsonLiteral.to_u8(), 0, 0, 0, 0, 0, 0, 0, 2,
            ELiteral.to_u8(), TString.to_u8(),
            0, 0, 0, 0, 0, 0, 0, 9, b'l', b'a', b's', b't', b'_', b's', b'a', b'l', b'e',
            ELiteral.to_u8(), TNumberF64.to_u8(),
            63, 188, 113, 12, 178, 149, 233, 226, // 0.1111
            // where symbol == 'ABC'
            EEqual.to_u8(),
            EVarGet.to_u8(), ELiteral.to_u8(), TString.to_u8(),
            0, 0, 0, 0, 0, 0, 0, 8, b'e', b'x', b'c', b'h', b'a', b'n', b'g', b'e',
            ELiteral.to_u8(), TString.to_u8(),
            0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E',
            // limit 10
            ELiteral.to_u8(), TNumberI64.to_u8(), 0, 0, 0, 0, 0, 0, 0, 10,
        ];
        assert_eq!(ByteCodeCompiler::assemble(&model), byte_code);
        assert_eq!(ByteCodeCompiler::disassemble(&mut ByteCodeCompiler::wrap(byte_code)).unwrap(), model)
    }

    #[test]
    fn test_json_literal() {
        use Mnemonic::EJsonLiteral;
        let model = JSONLiteral(vec![
            ("symbol".to_string(), Literal(StringValue("TRX".into()))),
            ("exchange".to_string(), Literal(StringValue("NYSE".into()))),
            ("last_sale".to_string(), Literal(Number(Float64Value(11.1111)))),
        ]);
        let byte_code = vec![
            EJsonLiteral.to_u8(), 0, 0, 0, 0, 0, 0, 0, 6,
            ELiteral.to_u8(), TString.to_u8(), 0, 0, 0, 0, 0, 0, 0, 6, b's', b'y', b'm', b'b', b'o', b'l',
            ELiteral.to_u8(), TString.to_u8(), 0, 0, 0, 0, 0, 0, 0, 3, b'T', b'R', b'X',
            ELiteral.to_u8(), TString.to_u8(), 0, 0, 0, 0, 0, 0, 0, 8, b'e', b'x', b'c', b'h', b'a', b'n', b'g', b'e',
            ELiteral.to_u8(), TString.to_u8(), 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E',
            ELiteral.to_u8(), TString.to_u8(), 0, 0, 0, 0, 0, 0, 0, 9, b'l', b'a', b's', b't', b'_', b's', b'a', b'l', b'e',
            ELiteral.to_u8(), TNumberF64.to_u8(), 64, 38, 56, 226, 25, 101, 43, 212,
        ];
        assert_eq!(ByteCodeCompiler::assemble(&model), byte_code);
        assert_eq!(ByteCodeCompiler::disassemble(&mut ByteCodeCompiler::wrap(byte_code)).unwrap(), model);
    }
}