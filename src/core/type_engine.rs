#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// TypeEngine - type inference, instantiation and detection services
////////////////////////////////////////////////////////////////////

use crate::builtins::Builtins;
use crate::compiler;
use crate::connections::ConnectionTypes;
use crate::connections::Connections::{BLOBStoreHandle, WebServerHandle, WebSocketHandle};
use crate::conversions::Conversions;
use crate::data_types::DataType;
use crate::data_types::DataType::*;
use crate::dataframe::Dataframe::ModelTable;
use crate::errors::Errors::{Exact, SyntaxError, TypeMismatch};
use crate::errors::TypeMismatchErrors::*;
use crate::errors::{throw, Errors, SyntaxErrors};
use crate::expression::Expression::*;
use crate::expression::Ranges::{Exclusive, Inclusive};
use crate::expression::{Expression, HttpMethodCalls};
use crate::extractions::*;
use crate::machine::{Machine, ROW_ID};
use crate::model_row_collection::ModelRowCollection;
use crate::number_kind::NumberKind;
use crate::number_kind::NumberKind::*;
use crate::numbers::Numbers::U16Value;
use crate::packages::{IoPkg, Package, PackageOps};
use crate::parameter::Parameter;
use crate::query_engine::QueryEngine;
use crate::row_collection::RowCollection;
use crate::sequences::{Array, Sequence};
use crate::structures::Structures::Hard;
use crate::structures::{HardStructure, Structure};
use crate::test_engine::TestEngine;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::*;
use chrono::Local;
use num_traits::ToPrimitive;
use std::collections::HashSet;
use std::ops::Deref;
use uuid::Uuid;

/// TypeEngine - type inference, instantiation and detection services
pub struct TypeEngine;

impl TypeEngine {

    /// provides type resolution for the given [Vec<DataType>]
    pub fn best_fit(types: Vec<DataType>) -> DataType {
        match types.len() {
            0 => RuntimeResolvedType,
            1 => types[0].to_owned(),
            _ => types[1..].iter().fold(types[0].to_owned(), |agg, t|
                match (agg, t) {
                    (FixedSizeType(a, size_a), FixedSizeType(b, size_b)) if a == *b =>
                        FixedSizeType(a.to_owned(), size_a.max(*size_b)),
                    (_, t) => t.to_owned()
                })
        }
    }

    /// deciphers a datatype from an expression (e.g. "String" | "String(20)")
    pub fn decipher_model(model: &Expression) -> std::io::Result<DataType> {
        use crate::typed_values::TypedValue::Structured;
        match model {
            // e.g. [String, String, f64]
            ArrayExpression(params) => Self::decipher_model_array(params),
            // e.g. String(80)
            FunctionCall { fx, args } => Self::decipher_model_function_call(fx, args),
            // e.g. Struct(symbol: String, exchange: String, last_sale: f64)
            Literal(Structured(s)) => Ok(StructureType(s.get_parameters())),
            // e.g. fn(a, b, c)
            Literal(Kind(data_type)) => Ok(data_type.clone()),
            // e.g. (f64, f64, f64)
            TupleExpression(params) => Self::decipher_model_tuple(params),
            Literal(TupleValue(values)) => Self::decipher_model_tuple_literal(values),
            // e.g. i64
            Identifier(name) => Self::decipher_model_identifier(name),
            other => throw(SyntaxError(SyntaxErrors::TypeIdentifierExpected(other.to_code())))
        }
    }

    fn decipher_model_array(items: &[Expression]) -> std::io::Result<DataType> {
        let mut kinds = vec![];
        for item in items {
            kinds.push(Self::decipher_model(item)?);
        }
        match kinds.as_slice() {
            [kind] => Ok(ArrayType(kind.clone().into())),
            other => throw(TypeMismatch(ArgumentsMismatched(1, other.len())))
        }
    }

    fn decipher_model_function_call(
        fx: &Expression,
        args: &[Expression],
    ) -> std::io::Result<DataType> {
        use crate::typed_values::TypedValue::Number;
        fn expect_enums(args: &[Expression], f: fn(Vec<Parameter>) -> DataType) -> std::io::Result<DataType> {
            let params = compiler::convert_to_parameters(args.to_vec())?;
            let mut enum_params = Vec::with_capacity(params.len());
            // TODO future support for custom enums
            // fn override(p: &Parameter, b: TypedValue) -> TypedValue {
            //     let a = p.get_default_value();
            //     if matches!(a, Undefined | Null) { b } else { a }
            // }
            for (n, param) in params.iter().enumerate() {
                //let value = override(param, Number(U16Value(n as u16)));
                let value = Number(U16Value(n as u16));
                enum_params.push(Parameter::new_with_default(
                    param.get_name(), value.get_type(), value
                ));
            }
            Ok(f(enum_params))
        }
        fn expect_params(args: &[Expression], f: fn(Vec<Parameter>) -> DataType) -> std::io::Result<DataType> {
            let params = compiler::convert_to_parameters(args.to_vec())?;
            Ok(f(params))
        }
        fn expect_size(args: &[Expression], data_type: DataType) -> std::io::Result<DataType> {
            match args {
                [] => Ok(data_type),
                [Literal(Number(n))] => Ok(FixedSizeType(data_type.into(), n.to_usize())),
                [other] => throw(SyntaxError(SyntaxErrors::TypeIdentifierExpected(other.to_code()))),
                other => throw(TypeMismatch(ArgumentsMismatched(1, other.len())))
            }
        }
        fn expect_type_and_size(args: &[Expression], f: fn(DataType) -> DataType) -> std::io::Result<DataType> {
            let (data_type, number_maybe) = match args {
                [] => (RuntimeResolvedType, None),
                [arg] => (TypeEngine::decipher_model(arg)?, None),
                [arg, num] => (TypeEngine::decipher_model(arg)?, Some(pull_number_lit(num)?)),
                other => return throw(TypeMismatch(ArgumentsMismatched(2, other.len())))
            };
            match number_maybe {
                None => Ok(f(data_type)),
                Some(size) => Ok(FixedSizeType(f(data_type).into(), size.to_usize()))
            }
        }
        fn expect_types(args: &[Expression], f: fn(Vec<DataType>) -> DataType) -> std::io::Result<DataType> {
            let mut data_types = vec![];
            for arg in args {
                data_types.push(TypeEngine::decipher_model(arg)?);
            }
            Ok(f(data_types))
        }

        // decode the type
        match fx {
            Identifier(name) =>
                match name.as_str() {
                    "Array" => expect_type_and_size(args, |data_type| ArrayType(data_type.into())),
                    "Bytes" => expect_size(args, ByteStringType),
                    "Enum" => expect_enums(args, |params| EnumType(params)),
                    "fn" => expect_params(args, |params| FunctionType(params, RuntimeResolvedType.into())),
                    "String" => expect_size(args, StringType),
                    "Struct" => expect_params(args, |params| StructureType(params)),
                    "Table" => expect_params(args, |params| TableType(params)),
                    "Tuple" => expect_types(args, |types| TupleType(types)),
                    type_name if args.is_empty() => Self::decipher_model_identifier(type_name),
                    type_name => throw(SyntaxError(SyntaxErrors::TypeIdentifierExpected(type_name.into())))
                }
            other => throw(SyntaxError(SyntaxErrors::TypeIdentifierExpected(other.to_code())))
        }
    }

    fn decipher_model_tuple(items: &[Expression]) -> std::io::Result<DataType> {
        let mut kinds = vec![];
        for item in items {
            kinds.push(Self::decipher_model(item)?);
        }
        Ok(TupleType(kinds))
    }

    pub fn decipher_model_tuple_literal(items: &[TypedValue]) -> std::io::Result<DataType> {
        let mut kinds = Vec::with_capacity(items.len());
        for tv in items {
            match tv {
                Kind(k) => kinds.push(k.clone()),
                _ => {
                    // Build the pretty string only on error:
                    let code = TupleValue(items.to_vec()).to_code();
                    return throw(SyntaxError(SyntaxErrors::TypeIdentifierExpected(code)));
                }
            }
        }
        Ok(TupleType(kinds))
    }

    pub fn decipher_model_identifier(name: &str) -> std::io::Result<DataType> {
        match name {
            "Array" => Ok(ArrayType(RuntimeResolvedType.into())),
            "BLOBStore" => Ok(ConnectionType(ConnectionTypes::BLOBStoreHandleType)),
            "Boolean" => Ok(BooleanType),
            "Bytes" => Ok(ByteStringType),
            "Char" => Ok(CharType),
            "DateTime" => Ok(DateTimeType),
            "Enum" => Ok(EnumType(vec![])),
            "Error" => Ok(ErrorType),
            "f64" => Ok(NumberType(F64Kind)),
            "Fn" => Ok(FunctionType(vec![], RuntimeResolvedType.into())),
            "i8" => Ok(NumberType(I8Kind)),
            "i16" => Ok(NumberType(I16Kind)),
            "i32" => Ok(NumberType(I32Kind)),
            "i64" => Ok(NumberType(I64Kind)),
            "i128" => Ok(NumberType(I128Kind)),
            "Number" => Ok(NumberType(NumberKind::AnyKind)),
            "String" => Ok(StringType),
            "Struct" => Ok(StructureType(vec![])),
            "Table" => Ok(TableType(vec![])),
            "Tuple" => Ok(TupleType(vec![])),
            "UUID" => Ok(UUIDType),
            "u8" => Ok(NumberType(U8Kind)),
            "u16" => Ok(NumberType(U16Kind)),
            "u32" => Ok(NumberType(U32Kind)),
            "u64" => Ok(NumberType(U64Kind)),
            "u128" => Ok(NumberType(U128Kind)),
            "WebServer" => Ok(ConnectionType(ConnectionTypes::WebServerHandleType)),
            "WebSocket" => Ok(ConnectionType(ConnectionTypes::WebSocketHandleType)),
            type_name => throw(TypeMismatch(UnrecognizedTypeName(type_name.to_string())))
        }
    }

    pub fn get_type_name(datatype: &DataType) -> String {
        match datatype {
            DataType::ArrayType(..) => "Array".to_string(),
            DataType::ConnectionType(ConnectionTypes::BLOBStoreHandleType) => "BLOBStore".to_string(),
            DataType::ConnectionType(ConnectionTypes::WebServerHandleType) => "WebServer".to_string(),
            DataType::ConnectionType(ConnectionTypes::WebSocketHandleType) => "WebSocket".to_string(),
            DataType::BooleanType => "Boolean".to_string(),
            DataType::ByteStringType => "Bytes".to_string(),
            DataType::CharType => "Char".to_string(),
            DataType::DateTimeType => "DateTime".to_string(),
            DataType::EnumType(..) => "Enum".to_string(),
            DataType::ErrorType => "Error".to_string(),
            DataType::FixedSizeType(dt, ..) => dt.get_name(),
            DataType::FunctionType(..) => "Function".to_string(),
            DataType::NumberType(..) => "Number".to_string(),
            DataType::PackageFunctionType(..) => "PlatformFunction".to_string(),
            DataType::RuntimeResolvedType => "Runtime".to_string(),
            DataType::StringType => "String".to_string(),
            DataType::StructureType(..) => "Struct".to_string(),
            DataType::TableType(..) => "Table".to_string(),
            DataType::TupleType(..) => "Tuple".to_string(),
            DataType::UUIDType => "UUID".to_string(),
        }
    }

    /// Provides type inference for the given [Expression] with hints
    /// to improve the matching performance.
    pub fn infer(
        hints: TypeHints,
        expr: &Expression,
    ) -> (TypeHints, DataType) {
        //println!("infer: expr {:?}", expr);
        match expr {
            Alias(..) => (hints, BooleanType),
            Aliases => (hints, TableType(Machine::get_alias_parameters())),
            ArrayExpression(items) => Self::infer_array(hints, items),
            ArrowCurvyLeft(..) => (hints, StructureType(vec![])),
            ArrowCurvyLeft2x(..) => (hints, TableType(vec![])),
            ArrowCurvyRight(..) => (hints, NumberType(I64Kind)),
            ArrowCurvyRight2x(..) => (hints, NumberType(I64Kind)),
            ArrowFat(_, b) => Self::infer(hints, b),
            ArrowSkinnyLeft(..) => (hints, RuntimeResolvedType),
            ArrowSkinnyRight(a, b) => Self::infer_function(hints, a, b),
            ArrowVerticalBar(_, b) => Self::infer(hints, b),
            ArrowVerticalBar2x(_, b) => Self::infer(hints, b),
            As(_, datatype) => Self::infer(hints, datatype),
            Assert { .. } => (hints, BooleanType),
            Coalesce(a, b) => Self::infer_a_or_b(hints, a, b),
            CoalesceErr(a, b) => Self::infer_a_or_b(hints, a, b),
            CodeBlock(ops) => Self::infer_codeblock(hints, ops),
            ColonColon(a, b) | ColonColonCapture(a, b) => Self::infer_colon_colon(hints, a, b),
            ColonColonColon(a, b) | ColonColonColonCapture(a, b) => Self::infer_colon_colon_colon(hints, a, b),
            Zip(a, b) => Self::infer_zip(hints, a, b),
            Condition(..) => (hints, BooleanType),
            ElementAt(a, b) => Self::infer_element_at(hints, a, b),
            Feature { .. } | Scenario { .. } => (hints, BooleanType),
            FunctionCall { fx, args } => Self::infer_function_call(hints, fx, args),
            HTTP(hmc) => Self::infer_http(hints, hmc),
            Identifier(name) => {
                let dt = hints.get(name).unwrap_or_else(|| RuntimeResolvedType);
                (hints, dt)
            }
            If { a: true_v, b: Some(false_v), .. } => Self::infer_a_or_b(hints, true_v, false_v),
            If { a: true_v, .. } => Self::infer(hints, true_v),
            Include(..) => (hints, BooleanType),
            Infix(container, field) => Self::infer_infix(hints, container, field),
            IsDefined(..) => (hints, BooleanType),
            Literal(Function { body, .. }) => Self::infer(hints, body),
            Literal(PackageFunction(pf)) => (hints, pf.get_return_type()),
            Literal(v) => (hints, v.get_type()),
            Ls(..) => (hints, TableType(IoPkg::get_io_files_parameters())),
            MatchExpression(ident, cases) => Self::infer_match(hints, ident, cases),
            Module(..) => (hints, BooleanType),
            NamedValue(n, v) => Self::infer_named_value(hints, n, v),
            Neg(a) => Self::infer(hints, a),
            OneTime(..) => (hints, BooleanType),
            Parameters(params) => {
                let types = params.iter().map(|p| p.get_data_type()).collect();
                (hints, TupleType(types))
            }
            PlusPlus(a, b) => Self::infer_a_or_b(hints, a, b),
            Range(Exclusive(a, b)) | Range(Inclusive(a, b)) => Self::infer_a_or_b(hints, a, b),
            Referenced(a) => Self::infer_referenced(hints, a),
            Return(a) => Self::infer(hints, a),
            SetVariables(a, b) => {
                let (hints, _) = Self::infer_set_variable(hints, a, b);
                (hints, BooleanType)
            }
            SetVariablesExpr(a, b) => Self::infer_set_variable(hints, a, b),
            StructureExpression(key_values) => Self::infer_structure_expression(hints, key_values),
            Test(..) => (hints, TableType(TestEngine::get_testing_feature_parameters())),
            Throw(..) => (hints, ErrorType),
            TupleExpression(values) => Self::infer_tuple_expression(hints, values),
            Use(..) => (hints, BooleanType),
            WhenEver { code, .. } => Self::infer(hints, code),
            With { resource, code } => Self::infer_with(hints, resource, code),
            Yield(a, ..) => Self::infer_yield(hints, a),
            ////////////////////////////////////////////////////////////////////
            // Logic models
            ////////////////////////////////////////////////////////////////////
            DoWhile { code, .. }
            | For { code, .. }
            | While { code, .. } => Self::infer(hints, code),
            ////////////////////////////////////////////////////////////////////
            // Math models
            ////////////////////////////////////////////////////////////////////
            BitwiseAnd(a, b)
            | BitwiseOr(a, b)
            | BitwiseShiftLeft(a, b)
            | BitwiseShiftRight(a, b)
            | BitwiseXor(a, b)
            | Divide(a, b)
            | Minus(a, b)
            | Modulo(a, b)
            | Multiply(a, b)
            | Plus(a, b)
            | Pow(a, b) => Self::infer_a_or_b(hints, a, b),
            ////////////////////////////////////////////////////////////////////
            // SQL models
            ////////////////////////////////////////////////////////////////////
            Delete { .. } | Undelete { .. } => (hints, NumberType(I64Kind)),
            Fetch { .. }  => Self::infer_query_fetch(hints, expr),
            Deselect { .. }
            | GroupBy { .. }
            | Having { .. }
            | Limit { .. }
            | OrderBy { .. }
            | Select { .. }
            | Where { .. } => Self::infer_query(hints, expr)
        }
    }

    /// Provides type inference for the given [Expression]s
    fn infer_a_or_b(
        hints: TypeHints,
        a: &Expression,
        b: &Expression,
    ) -> (TypeHints, DataType) {
        let (hints, dt_a) = Self::infer(hints, a);
        let (hints, dt_b) = Self::infer(hints, b);
        (hints, Self::best_fit(vec![dt_a, dt_b]))
    }

    fn infer_array(mut hints: TypeHints, items: &[Expression]) -> (TypeHints, DataType) {
        let mut data_types = vec![];
        for item in items {
            let (hints1, dt) = Self::infer(hints, item);
            hints = hints1;
            data_types.push(dt);
        }
        let best_fit = Self::best_fit(data_types);
        (hints, FixedSizeType(ArrayType(best_fit.into()).into(), items.len()))
    }

    fn infer_codeblock(
        hints: TypeHints,
        ops: &[Expression],
    ) -> (TypeHints, DataType) {
        let (mut hints, mut datatype) = (hints, RuntimeResolvedType);
        for op in ops {
            let (hints1, datatype1) = Self::infer(hints, op);
            hints = hints1;
            datatype = datatype1;
            println!("infer_codeblock: {:?} -> {:?} | {:?}", datatype, op, hints);
        }
        (hints, datatype)
    }

    fn infer_colon_colon(
        hints: TypeHints,
        ident: &Expression,
        func: &Expression
    ) -> (TypeHints, DataType) {
        fn resolve(hints: TypeHints, ident: &Expression, func: &Expression, pkg_name: &str, name: &str, is_lib: bool) -> (TypeHints, DataType) {
            // get the function arguments
            let args = match func {
                FunctionCall { args, .. } => args.clone(),
                _ => vec![]
            };
            let pops_maybe = match is_lib {
                true => PackageOps::find_function(pkg_name, name),
                _ => Builtins::lookup_by_name(pkg_name, name)
            };
            match pops_maybe {
                None => (hints, RuntimeResolvedType),
                Some(pops) => {
                    let mut fn_args = vec![];
                    fn_args.push(ident.clone());
                    fn_args.extend(args);
                    pops.infer_type(hints, &fn_args)
                }
            }
        }

        let (hints, ident_t) = Self::infer(hints, ident);
        let (pkg_name, is_lib) = match ident_t {
            // e.g., os::env
            RuntimeResolvedType =>
                match find_name(ident) {
                    None => return (hints, RuntimeResolvedType),
                    Some(name) => (name, true)
                }
            // e.g., DateTime::new
            dt if find_name(func).is_some_and(|name| name == "new") => return (hints, dt),
            // e.g., String::reverse
            dt => (dt.get_name(), false)
        };
        find_name(func)
            .map(|name| resolve(hints.clone(), ident, func, pkg_name.as_str(), name.as_str(), is_lib))
            .unwrap_or((hints, RuntimeResolvedType))
    }

    fn infer_colon_colon_colon(
        hints: TypeHints,
        _arg: &Expression,
        fx: &Expression
    ) -> (TypeHints, DataType) {
        match Self::infer(hints, fx) {
            (hints, FunctionType(.., rt)) => (hints, unbox(rt)),
            (hints, dt) => (hints, dt),
        }
    }

    fn infer_element_at(
        hints: TypeHints,
        container: &Expression,
        index: &Expression,
    ) -> (TypeHints, DataType) {
        let (hints, container_t) = Self::infer(hints, container);
        let (hints, index_t) = Self::infer(hints, index);
        fn resolve(
            hints: TypeHints,
            container_t: DataType,
            index_t: DataType
        ) -> (TypeHints, DataType) {
            match container_t {
                ArrayType(sub_t) => (hints, unbox(sub_t)),
                ByteStringType => (hints, NumberType(U8Kind)),
                EnumType(..) => (hints, CharType),
                FixedSizeType(dt, _) => resolve(hints, unbox(dt), index_t),
                StringType => (hints, CharType),
                TableType(params) => (hints, StructureType(params)),
                _ => (hints, RuntimeResolvedType)
            }
        }
        resolve(hints, container_t, index_t)
    }

    fn infer_function(
        hints: TypeHints,
        a: &Expression,
        b: &Expression
    ) -> (TypeHints, DataType) {
        let params = resolve_as_parameters(a);
        let (hints, dt_b) = Self::infer(hints.with_params(&params), b);
        (hints, FunctionType(params, dt_b.into()))
    }

    fn infer_function_call(
        hints: TypeHints,
        fx: &Expression,
        _args: &[Expression]
    ) -> (TypeHints, DataType) {
        match Self::infer(hints, fx) {
            (hints, FunctionType(.., rt)) => (hints, unbox(rt)),
            (hints, dt) => (hints, dt),
        }
    }

    fn infer_http(
        hints: TypeHints,
        _hmc: &HttpMethodCalls
    ) -> (TypeHints, DataType) {
        (hints, RuntimeResolvedType)
    }

    fn infer_infix(
        hints: TypeHints,
        container: &Expression,
        field: &Expression,
    ) -> (TypeHints, DataType) {
        let (hints, container_t) = Self::infer(hints, container);
        match container_t {
            StructureType(params) | TableType(params) => {
                let hints = hints.with_params(&params);
                Self::infer(hints, field)
            }
            _ => (hints, RuntimeResolvedType)
        }
    }

    fn infer_match(
        hints: TypeHints,
        _ident: &Expression,
        cases: &[Expression],
    ) -> (TypeHints, DataType) {
        let (mut datatype, mut my_hints) = (RuntimeResolvedType, hints.clone());
        for case in cases {
            let (hints1, datatype1) = Self::infer(my_hints, case);
            my_hints = hints1;
            datatype = datatype1;
        }
        (my_hints, datatype)
    }

    fn infer_named_value(
        hints: TypeHints,
        name: &str,
        value: &Expression,
    ) -> (TypeHints, DataType) {
        let (hints, datatype) = Self::infer(hints, value);
        let hints = hints.with_variable(name, datatype.clone());
        (hints, datatype)
    }

    fn infer_query(hints: TypeHints, expr: &Expression) -> (TypeHints, DataType) {
        // unwind the query
        let Ok((from, fields, _condition, _group_by, _having, _order_by, _limit)) =
            QueryEngine::unwind_query_graph(expr.clone())
        else { return (hints, TableType(vec![])); };

        // infer the source
        match Self::infer(hints, &from) {
            (hints, TableType(params)) if !params.is_empty() => {
                // is the query a "deselect"?
                let is_deselect = matches!(expr, Deselect { .. });
                // get the set of field names
                let field_names: HashSet<_> = fields.iter()
                    .filter_map(|f| pull_identifier_name(f).ok())
                    .filter(|s| !s.is_empty())
                    .collect();
                // build the type
                let dt = TableType(params.iter()
                    .filter(|p| is_deselect ^ field_names.contains(p.get_name()))
                    .cloned()
                    .collect());
                (hints, dt)
            }
            (hints, _) => (hints, TableType(vec![]))
        }
    }

    fn infer_query_fetch(hints: TypeHints, expr: &Expression) -> (TypeHints, DataType) {
        match Self::infer_query(hints, expr) {
            (hints, TableType(params)) =>
                match params.len() {
                    1 => (hints, params[0].get_data_type()),
                    _ => (hints, TupleType(params.iter().map(|p| p.get_data_type()).collect())),
                }
            (hints, dt) => (hints, dt),
        }
    }

    fn infer_referenced(
        hints: TypeHints,
        expr: &Expression,
    ) -> (TypeHints, DataType) {
        fn resolve_column(args: &[Expression]) -> Option<usize> {
            match args {
                [_, Literal(Number(n))] => Some(n.to_usize()),
                _ => None
            }
        }
        match expr {
            FunctionCall { fx, args } => {
                match find_name(fx).and_then(|name| hints.get(&name)) {
                    Some(TableType(params)) =>
                        match resolve_column(args) {
                            None => (hints, TableType(vec![])),
                            Some(col) => (hints, if col < params.len() { params[col].get_data_type() } else { RuntimeResolvedType })
                        }
                    _ => (hints, RuntimeResolvedType),
                }
            }
            _ => (hints, RuntimeResolvedType),
        }
    }

    fn infer_set_variable(
        hints: TypeHints,
        a: &Expression,
        b: &Expression,
    ) -> (TypeHints, DataType) {
        match a {
            Identifier(name) => {
                let (hints, datatype) = Self::infer(hints, b);
                let hints = hints.with_variable(name, datatype.clone());
                (hints, datatype)
            }
            _ => Self::infer(hints, b)
        }
    }

    fn infer_structure_expression(
        mut hints: TypeHints,
        key_values: &Vec<(String, Expression)>,
    ) -> (TypeHints, DataType) {
        let mut params = vec![];
        for (name, value) in key_values {
            let (hints1, datatype1) = Self::infer(hints.clone(), value);
            hints = hints1;
            params.push(Parameter::new(name, datatype1));
        }
        (hints, StructureType(params))
    }

    fn infer_tuple_expression(
        mut hints: TypeHints,
        values: &[Expression],
    ) -> (TypeHints, DataType) {
        let mut datatypes = vec![];
        for value in values {
            let (hints1, datatype1) = Self::infer(hints, value);
            datatypes.push(datatype1);
            hints = hints1;
        }
        (hints, TupleType(datatypes))
    }

    fn infer_with(
        hints: TypeHints,
        resource: &Expression,
        fx_code: &Expression,
    ) -> (TypeHints, DataType) {
        match fx_code {
            // get the function head and body
            ArrowSkinnyRight(head, body) => {
                // get the type of the resource
                let (hints, resource_t) = Self::infer(hints, resource);
                // get the parameters of the function head
                let mut params = resolve_as_parameters(head);
                // reconstruct the first parameter with the type of the resource
                if !params.is_empty() { params[0] = Parameter::new(params[0].get_name(), resource_t) }
                // infer the type of the function body
                Self::infer(hints.with_params(&params), body)
            }
            _ => (hints, RuntimeResolvedType)
        }
    }

    fn infer_yield(
        hints: TypeHints,
        a: &Expression,
    ) -> (TypeHints, DataType) {
        let (hints, dt) = Self::infer(hints, a);
        (hints, ArrayType(dt.into()))
    }

    fn infer_zip(
        hints: TypeHints,
        a: &Expression,
        b: &Expression,
    ) -> (TypeHints, DataType) {
        let (hints, dt_a) = Self::infer(hints, a);
        let (hints, dt_b) = Self::infer(hints, b);
        fn resolve(hints: TypeHints, a: DataType, b: DataType) -> (TypeHints, DataType) {
            match (a, b) {
                (FixedSizeType(aa, _), bb) => resolve(hints, unbox(aa), bb),
                (aa, FixedSizeType(bb, _)) => resolve(hints, aa, unbox(bb)),
                (ArrayType(aa), ArrayType(bb)) =>
                    (hints, ArrayType(TupleType(vec![unbox(aa), unbox(bb)]).into())),
                (_, _) => (hints, RuntimeResolvedType),
            }
        }
        resolve(hints, dt_a, dt_b)
    }

    /// Instantiates the data type with the arguments
    pub fn instantiate(
        datatype: &DataType,
        args: Vec<TypedValue>,
    ) -> std::io::Result<TypedValue> {
        match datatype {
            DataType::ArrayType(datatype) =>
                Ok(match args.as_slice() {
                    [] => ArrayValue(Array::new()),
                    items => ArrayValue(Array::from(Conversions::convert_to(&items.to_vec(), datatype)?)),
                }),
            DataType::ConnectionType(ConnectionTypes::BLOBStoreHandleType) =>
                construct_1(args, |item| Ok(Connection(BLOBStoreHandle(pull_uuid(item)?)))),
            DataType::ConnectionType(ConnectionTypes::WebServerHandleType) =>
                construct_1(args, |item| Ok(Connection(WebServerHandle(pull_number(item)?.to_u16())))),
            DataType::ConnectionType(ConnectionTypes::WebSocketHandleType) =>
                construct_1(args, |item| Ok(Connection(WebSocketHandle(pull_uuid(item)?)))),
            DataType::BooleanType =>
                construct_0_or_1(
                    args,
                    || Boolean(false),
                    |item| Boolean(item.to_bool())),
            DataType::ByteStringType =>
                construct_0_or_1(
                    args,
                    || ByteStringValue(vec![]),
                    |bytes| ByteStringValue(bytes.to_bytes())),
            DataType::CharType =>
                construct_0_or_1(
                    args,
                    || CharValue('\0'),
                    |item| item.to_char().map(CharValue).unwrap_or(CharValue('\0'))),
            DataType::DateTimeType =>
                construct_0_or_1(
                    args,
                    || DateTimeValue(Local::now().timestamp_millis()),
                    |item| DateTimeValue(item.to_i64())),
            DataType::EnumType(params) =>
                construct_0_or_1(
                    args,
                    || EnumValue(None, params.to_vec()),
                    |item| match item {
                        Number(n) => EnumValue(Some(n.to_u16()), params.to_vec()),
                        StringValue(s) => EnumValue(
                            params.iter().position(|p| p.get_name() == s).and_then(|n| n.to_u16()),
                            params.to_vec()),
                        _ => EnumValue(None, params.to_vec()),
                    }),
            DataType::ErrorType =>
                construct_0_or_1(
                    args,
                    || ErrorValue(Errors::Empty),
                    |item| ErrorValue(Exact(item.to_string()))),
            DataType::FixedSizeType(data_type, ..) => data_type.instantiate(args),
            DataType::FunctionType(params, returns) => Ok(Function {
                params: params.to_vec(),
                body: Literal(returns.instantiate(vec![])?).into(),
                returns: unbox_r(returns),
            }),
            DataType::NumberType(kind) =>
                construct_0_or_1_ok(
                    args,
                    || Number(kind.get_default_value()),
                    |item| Conversions::convert_to_number(item, kind)),
            DataType::PackageFunctionType(kind) => Ok(PackageFunction(kind.clone())),
            DataType::StringType =>
                construct_0_or_1(
                    args,
                    || StringValue(String::new()),
                    |item| StringValue(item.unwrap_value())),
            DataType::StructureType(params) => Self::instantiate_struct(params, args),
            DataType::TableType(params) => Ok(TableValue(ModelTable(ModelRowCollection::from_parameters(params)))),
            DataType::TupleType(types) => Self::instantiate_tuple(types, args),
            DataType::RuntimeResolvedType => throw(Exact("Type cannot be instantiated".into())),
            DataType::UUIDType =>
                construct_0_or_1(
                    args,
                    || UUIDValue(Uuid::new_v4().as_u128()),
                    |item| UUIDValue(item.to_u128())),
        }
    }

    fn instantiate_struct(
        params: &Vec<Parameter>,
        args: Vec<TypedValue>,
    ) -> std::io::Result<TypedValue> {
        if args.len() > params.len() {
            return throw(TypeMismatch(ArgumentsMismatched(params.len(), args.len())))
        }
        let mut values = vec![];
        for (n, param) in params.iter().enumerate() {
            let arg = if n < args.len() { args[n].clone() } else { param.get_default_value() };
            values.push(arg);
        }
        Ok(Structured(Hard(HardStructure::from_parameters_and_values(params.to_vec(), values))))
    }

    fn instantiate_tuple(
        data_types: &Vec<DataType>,
        args: Vec<TypedValue>,
    ) -> std::io::Result<TypedValue> {
        if args.len() > data_types.len() {
            return throw(TypeMismatch(ArgumentsMismatched(data_types.len(), args.len())))
        }
        let mut values = vec![];
        for (n, data_type) in data_types.iter().enumerate() {
            let arg = if n < args.len() { args[n].clone() } else { data_type.instantiate(vec![])? };
            values.push(data_type.instantiate(vec![arg])?);
        }
        Ok(TupleValue(values))
    }

    pub fn is_a(aa: &DataType, bb: &DataType) -> bool {
        match (aa, bb) {
            (NumberType(a), NumberType(b)) => match (a, b) {
                (NumberKind::AnyKind, _) | (_, NumberKind::AnyKind) => true,
                (a, b) => a == b,
            }
            (aa, bb) => Self::is_compatible(aa, bb)
        }
    }

    pub fn is_compatible(aa: &DataType, bb: &DataType) -> bool {
        match (aa, bb) {
            (ArrayType(..), ArrayType(..)) |
            (ByteStringType, ByteStringType) |
            (StringType, StringType) |
            (RuntimeResolvedType, _) | (_, RuntimeResolvedType) => true,
            (EnumType(a), EnumType(b)) => Parameter::are_compatible(a, b),
            (FixedSizeType(a, _), b) => a.is_compatible(b),
            (a, FixedSizeType(b, _)) => a.is_compatible(b),
            (FunctionType(a, dta), FunctionType(b, dtb)) =>
                Parameter::are_compatible(a, b) && dta.is_compatible(dtb),
            (NumberType(..), NumberType(..)) => true,
            // TODO add a case for when the source struct|table has a subset of the target fields?
            (StructureType(a) | TableType(a), StructureType(b) | TableType(b)) =>
                Parameter::are_compatible(a, b),
            (TupleType(a), TupleType(b)) => Self::is_compatible_seq(a, b),
            (a, b) => a == b,
        }
    }

    pub fn is_compatible_seq(
        types_a: &Vec<DataType>,
        types_b: &Vec<DataType>,
    ) -> bool {
        types_a.iter().zip(types_b.iter()).all(|(a, b)| a.is_compatible(b))
    }

    /// Indicates whether the given name represents a known type
    pub fn is_type_name(name: &str) -> bool {
        Self::decipher_model_identifier(name).is_ok()
    }

}

/// Represents type state information
#[derive(Clone, Debug)]
pub struct TypeHints {
    pub params: Vec<Parameter>,
    pub variables: im::HashMap<String, DataType>,
}

impl TypeHints {
    pub fn new() -> TypeHints {
        TypeHints {
            params: Vec::new(),
            variables: im::HashMap::new(),
        }
    }

    pub fn get(&self, name: &str) -> Option<DataType> {
        if name == ROW_ID {
            Some(NumberType(I64Kind))
        } else {
            match self.params.iter().find(|p| p.get_name() == name) {
                Some(param) => Some(param.get_data_type()),
                None => self.variables.get(name).cloned()
            }
        }
    }

    // pub fn with_key_values(&self, key_values: &Vec<(String, Expression)>) -> TypeHints {
    //     let mut hints = self.clone();
    //     for (name, value) in key_values {
    //         let (hints1, datatype1) = TypeEngine::infer(hints, value);
    //         hints = hints1;
    //         let param = Parameter::new(name, datatype1);
    //         hints.params.push(param);
    //     }
    //     hints
    // }

    pub fn with_params(&self, params: &Vec<Parameter>) -> TypeHints {
        let mut hints = self.clone();
        let mut combined_params = hints.params.clone();
        for param in params {
            match combined_params.iter().position(|p| p.get_name() == param.get_name()) {
                None => combined_params.push(param.clone()),
                Some(n) =>
                    if param.get_data_type() != RuntimeResolvedType {
                        combined_params[n] = param.clone();
                    }
            }
        }

        hints.params = combined_params;
        hints
    }

    pub fn with_variable(&self, name: &str, datatype: DataType) -> TypeHints {
        let mut hints = self.clone();
        hints.variables = self.variables
            .update(name.to_string(), datatype);
        hints
    }
}

fn construct_0_or_1<F, G>(
    args: Vec<TypedValue>,
    f0: F,
    f1: G,
) -> std::io::Result<TypedValue>
where
    F: Fn() -> TypedValue,
    G: Fn(&TypedValue) -> TypedValue,
{
    match args.as_slice() {
        [] => Ok(f0()),
        [item] => Ok(f1(item)),
        items => throw(TypeMismatch(ArgumentsMismatched(1, items.len()))),
    }
}

fn construct_0_or_1_ok<F, G>(
    args: Vec<TypedValue>,
    f0: F,
    f1: G,
) -> std::io::Result<TypedValue>
where
    F: Fn() -> TypedValue,
    G: Fn(&TypedValue) -> std::io::Result<TypedValue>,
{
    match args.as_slice() {
        [] => Ok(f0()),
        [item] => f1(item),
        items => throw(TypeMismatch(ArgumentsMismatched(1, items.len()))),
    }
}

fn construct_1<F>(
    args: Vec<TypedValue>,
    f1: F,
) -> std::io::Result<TypedValue>
where
    F: Fn(&TypedValue) -> std::io::Result<TypedValue>,
{
    match args.as_slice() {
        [item] => f1(item),
        items => throw(TypeMismatch(ArgumentsMismatched(1, items.len()))),
    }
}

fn resolve_as_parameters(expr: &Expression) -> Vec<Parameter> {
    match expr {
        Identifier(name) => vec![Parameter::add(name)],
        Parameters(params) => params.clone(),
        TupleExpression(args) => {
            let mut params = Vec::with_capacity(args.len());
            for arg in args { params.extend(resolve_as_parameters(arg)); }
            params
        }
        _ => vec![]
    }
}

pub fn unbox(t: Box<DataType>) -> DataType {
    t.deref().clone()
}

pub fn unbox_r(t: &Box<DataType>) -> DataType {
    t.deref().clone()
}

/// Unit tests
#[cfg(test)]
mod tests {
    use crate::data_types::DataType::{BooleanType, FixedSizeType, NumberType, TableType};
    use crate::expression::Expression::Literal;
    use crate::parameter::Parameter;

    /// inference unit tests
    #[cfg(test)]
    mod inference_tests {
        use super::*;
        use crate::compiler::Compiler;
        use crate::data_types::DataType::*;
        use crate::machine::ROW_ID;
        use crate::number_kind::NumberKind::{F64Kind, I64Kind, U64Kind, U8Kind};
        use crate::numbers::Numbers::{F64Value, I64Value};
        use crate::test_util::{verify_bit_operator, verify_data_type, verify_math_operator};
        use crate::type_engine::{TypeEngine, TypeHints};
        use crate::typed_values::TypedValue::{Number, StringValue};

        #[test]
        fn test_best_fit() {
            let kind = TypeEngine::best_fit(vec![
                FixedSizeType(StringType.into(), 11),
                FixedSizeType(StringType.into(), 110),
                FixedSizeType(StringType.into(), 55)
            ]);
            assert_eq!(kind, FixedSizeType(StringType.into(), 110))
        }

        #[test]
        fn test_infer_a_or_b_strings() {
            let (_, kind) = TypeEngine::infer_a_or_b(
                TypeHints::new(),
                &Literal(StringValue("yes".into())),
                &Literal(StringValue("hello".into())),
            );
            assert_eq!(kind, FixedSizeType(StringType.into(), 5))
        }

        #[test]
        fn test_infer_a_or_b_numbers() {
            let (_, kind) = TypeEngine::infer_a_or_b(
                TypeHints::new(),
                &Literal(Number(I64Value(76))),
                &Literal(Number(F64Value(76.0))),
            );
            assert_eq!(kind, NumberType(F64Kind))
        }

        #[test]
        fn test_infer_array() {
            verify_data_type(r#"
                ['A', 'B', "C"]
            "#, FixedSizeType(ArrayType(FixedSizeType(StringType.into(), 1).into()).into(), 3));
        }

        #[test]
        fn test_infer_colon_colon_function() {
            verify_data_type("'Hello World'::len()", NumberType(I64Kind));
        }

        #[test]
        fn test_infer_colon_colon_identifier() {
            verify_data_type("'Hello World'::len", NumberType(I64Kind));
        }

        #[test]
        fn test_infer_colon_colon_colon() {
            verify_data_type(r#"
                let kb = n -> n * 1024
                3.2:::kb
            "#, NumberType(I64Kind));
        }

        #[test]
        fn test_infer_conditionals_and() {
            verify_data_type("true && false", BooleanType);
        }

        #[test]
        fn test_infer_conditionals_in_range_exclusive() {
            verify_data_type("20 in 1..20", BooleanType);
        }

        #[test]
        fn test_infer_conditionals_in_range_inclusive() {
            verify_data_type("20 in 1..=20", BooleanType);
        }

        #[test]
        fn test_infer_conditionals_eq() {
            verify_data_type("x == y", BooleanType);
        }

        #[test]
        fn test_infer_conditionals_gt() {
            verify_data_type("x > y", BooleanType);
        }

        #[test]
        fn test_infer_conditionals_gte() {
            verify_data_type("x >= y", BooleanType);
        }

        #[test]
        fn test_infer_conditionals_is() {
            verify_data_type("a is b", BooleanType);
        }

        #[test]
        fn test_infer_conditionals_isnt() {
            verify_data_type("a isnt b", BooleanType);
        }

        #[test]
        fn test_infer_conditionals_lt() {
            verify_data_type("x < y", BooleanType);
        }

        #[test]
        fn test_infer_conditionals_lte() {
            verify_data_type("x <= y", BooleanType);
        }

        #[test]
        fn test_infer_conditionals_neq() {
            verify_data_type("x != y", BooleanType);
        }

        #[test]
        fn test_infer_conditionals_or() {
            verify_data_type("true || false", BooleanType);
        }

        #[test]
        fn test_infer_element_at_array() {
            verify_data_type(r#"
                let arr = [1, 4, 2, 8, 5, 7]
                arr[0]
            "#, NumberType(I64Kind));
        }

        #[test]
        fn test_infer_element_at_bytes() {
            verify_data_type(r#"
                let bytes = 0B576879206469642074686520636869636b656e202e2e2e
                bytes[0]
            "#, NumberType(U8Kind));
        }

        #[test]
        fn test_infer_element_at_enum() {
            verify_data_type(r#"
                let exchange = Enum(AMEX, NASDAQ, NYSE, OTCBB)::new(1)
                exchange[0]
            "#, CharType);
        }

        #[test]
        fn test_infer_element_at_string() {
            verify_data_type(r#"
                let s = "Why did the chicken ..."
                s[0]
            "#, CharType);
        }

        #[test]
        fn test_infer_element_at_table() {
            verify_data_type(r#"
                let stocks =
                    |--------------------------------|
                    | symbol | exchange  | last_sale |
                    |--------------------------------|
                    | TRX    | NASDAQ    | 32.96     |
                    |--------------------------------|
                stocks[0]
            "#, StructureType(vec![
                Parameter::new("symbol", FixedSizeType(StringType.into(), 3)),
                Parameter::new("exchange", FixedSizeType(StringType.into(), 6)),
                Parameter::new("last_sale", NumberType(F64Kind)),
            ]));
        }

        #[test]
        fn test_infer_function_call() {
            verify_data_type(r#"
                let kb = n -> n * 1024
                kb(128)
            "#, NumberType(I64Kind));
        }

        #[test]
        fn test_infer_function_single() {
            verify_data_type(r#"
                n -> n ** 2
            "#, FunctionType(vec![
                Parameter::new("n", RuntimeResolvedType),
            ], NumberType(I64Kind).into()));
        }

        #[test]
        fn test_infer_function_typed() {
            verify_data_type(r#"
                (a: i64, b: i64) -> a + b
            "#, FunctionType(vec![
                Parameter::new("a", NumberType(I64Kind)),
                Parameter::new("b", NumberType(I64Kind)),
            ], NumberType(I64Kind).into()));
        }

        #[test]
        fn test_infer_function_untyped() {
            verify_data_type(r#"
                (a, b) -> a + b
            "#, FunctionType(vec![
                Parameter::new("a", RuntimeResolvedType),
                Parameter::new("b", RuntimeResolvedType),
            ], RuntimeResolvedType.into()));
        }

        #[test]
        fn test_infer_http_get() {
            verify_data_type(r#"
                GET https://www.example.com
            "#, RuntimeResolvedType);
        }

        #[test]
        fn test_infer_infix() {
            verify_data_type(r#"
                let stock = { symbol: "TED", exchange: "AMEX", last_sale: 13.37 }
                stock.last_sale
            "#, NumberType(F64Kind));
        }

        #[test]
        fn test_infer_match() {
            verify_data_type(r#"
                let code = 100
                match code {
                   100 => "Accepted"
                   n when n in 101..=104 => "Escalated"
                   n when n < 100 => "Pending"
                   n => "Rejected"
                }
            "#, FixedSizeType(StringType.into(), 8));
        }

        #[test]
        fn test_infer_mathematics_divide() {
            verify_math_operator("/");
        }

        #[test]
        fn test_infer_mathematics_minus() {
            verify_math_operator("-");
        }

        #[test]
        fn test_infer_mathematics_plus() {
            verify_math_operator("+");
        }

        #[test]
        fn test_infer_mathematics_plus_plus() {
            verify_math_operator("++");
        }

        #[test]
        fn test_infer_mathematics_power() {
            verify_math_operator("**");
        }

        #[test]
        fn test_infer_mathematics_shl() {
            verify_bit_operator("<<");
        }

        #[test]
        fn test_infer_mathematics_shr() {
            verify_bit_operator(">>");
        }

        #[test]
        fn test_infer_mathematics_times() {
            verify_math_operator("*");
        }

        #[test]
        fn test_infer_named_value() {
            verify_data_type(r#"
                symbol: "SCRPTD"
            "#, FixedSizeType(StringType.into(), 6));
        }

        #[test]
        fn test_infer_package_util_md5() {
            verify_data_type("util::md5(a)", UUIDType);
        }

        #[test]
        fn test_infer_query_deselect() {
            verify_data_type(r#"
                deselect accessed_time, mode, size, created_time from ls
            "#, TableType(vec![
                    Parameter::new("name", StringType),
                    Parameter::new("is_directory", BooleanType),
                    Parameter::new("is_file", BooleanType),
                    Parameter::new("is_symlink", BooleanType),
                    Parameter::new("length", NumberType(U64Kind)),
                    Parameter::new("modified_time", DateTimeType),
                ]));
        }

        #[test]
        fn test_infer_query_fetch_one() {
            verify_data_type(r#"
                let stocks =
                    |--------------------------------------|
                    | symbol | exchange | last_sale | rank |
                    |--------------------------------------|
                    | BOOM   | NYSE     | 113.76    | 1    |
                    | ABC    | AMEX     | 24.98     | 2    |
                    | JET    | NASDAQ   | 64.24     | 3    |
                    |--------------------------------------|
                fetch last_sale from stocks
            "#, NumberType(F64Kind));
        }

        #[test]
        fn test_infer_query_fetch_tuple() {
            verify_data_type(r#"
                let stocks =
                    |--------------------------------------|
                    | symbol | exchange | last_sale | rank |
                    |--------------------------------------|
                    | BOOM   | NYSE     | 113.76    | 1    |
                    | ABC    | AMEX     | 24.98     | 2    |
                    | JET    | NASDAQ   | 64.24     | 3    |
                    |--------------------------------------|
                fetch symbol, exchange, last_sale from stocks
            "#, TupleType(vec![
                FixedSizeType(StringType.into(), 4),
                FixedSizeType(StringType.into(), 6),
                NumberType(F64Kind)
            ]));
        }

        #[test]
        fn test_infer_query_fetch_table() {
            verify_data_type(r#"
                let stocks = Table(
                    symbol: String(8),
                    exchange: Enum(AMEX, NYSE, NASDAQ, OTCBB, OTHER_OTC),
                    history: Table(last_sale: f64, processed_time: DateTime)
                )::new

                fetch history from stocks
            "#, TableType(vec![
                Parameter::new("last_sale", NumberType(F64Kind)),
                Parameter::new("processed_time", DateTimeType),
            ]))
        }

        #[test]
        fn test_infer_query_select() {
            verify_data_type(r#"
                select name, is_directory, is_file, length, modified_time from ls
            "#, TableType(vec![
                    Parameter::new("name", StringType),
                    Parameter::new("is_directory", BooleanType),
                    Parameter::new("is_file", BooleanType),
                    Parameter::new("length", NumberType(U64Kind)),
                    Parameter::new("modified_time", DateTimeType),
                ]));
            }

        #[test]
        fn test_infer_query_test() {
            verify_data_type(r#"
                deselect seq, level from test
            "#, TableType(vec![
                    Parameter::new("item", FixedSizeType(StringType.into(), 256)),
                    Parameter::new("passed", NumberType(I64Kind)),
                    Parameter::new("failed", NumberType(I64Kind)),
                ]));
        }

        #[test]
        fn test_infer_referenced() {
            verify_data_type(r#"
            let stocks = Table(
                symbol: String(8),
                exchange: Enum(AMEX, NYSE, NASDAQ, OTCBB, OTHER_OTC),
                history: Table(last_sale: f64, processed_time: DateTime)
            )::new

            let history = &stocks(0, 2)
            history
            "#, TableType(vec![
                Parameter::new("last_sale", NumberType(F64Kind)),
                Parameter::new("processed_time", DateTimeType),
            ]))
        }

        #[test]
        fn test_infer_return() {
            verify_data_type("return 5", NumberType(I64Kind));
        }

        #[test]
        fn test_infer_row_id() {
            verify_data_type(ROW_ID, NumberType(I64Kind));
        }

        #[test]
        fn test_infer_set_variable() {
            let expr = Compiler::build(r#"
                let x = 2 * 5
                x
            "#).unwrap();
            let (hints, datatype) = TypeEngine::infer(TypeHints::new(), &expr);
            println!("hints {:?}", hints);
            assert_eq!(datatype, NumberType(I64Kind));
        }

        #[test]
        fn test_infer_set_variable_expr() {
            let expr = Compiler::build(r#"
                z = (x := 2 * 5)
                z
            "#).unwrap();
            let (hints, datatype) = TypeEngine::infer(TypeHints::new(), &expr);
            println!("hints {:?}", hints);
            assert_eq!(datatype, NumberType(I64Kind));
        }

        #[test]
        fn test_infer_tuple() {
            verify_data_type(r#"
                (1, 'A', "A")
            "#, TupleType(vec![
                NumberType(I64Kind),
                CharType,
                FixedSizeType(StringType.into(), 1)
            ]));
        }

        #[test]
        fn test_infer_yield() {
            verify_data_type(r#"
                do {
                    yield (i := i + 1) * 5
                } while (i < 5)
            "#, ArrayType(NumberType(I64Kind).into()));
        }

        #[test]
        fn test_infer_zip_array() {
            verify_data_type(r#"
                [1, 2, 3] <|> ['A', 'B', 'C']
            "#, ArrayType(TupleType(vec![
                NumberType(I64Kind),
                CharType
            ]).into()));
        }
    }

    /// Compatible Types Unit tests
    mod is_compatible_type_tests {
        use crate::data_types::DataType;
        use crate::data_types::DataType::*;
        use crate::number_kind::NumberKind;
        use crate::number_kind::NumberKind::*;

        #[test]
        fn test_arrays() {
            verify_compatibility(
                FixedSizeType(ArrayType(DateTimeType.into()).into(), 800),
                ArrayType(DateTimeType.into())
            )
        }

        #[test]
        fn test_ascii_and_strings() {
            verify_compatibility(FixedSizeType(StringType.into(), 80), StringType);
        }

        #[test]
        fn test_numbers() {
            verify_compatibility(NumberType(NumberKind::AnyKind), NumberType(I64Kind));
            verify_compatibility(NumberType(I64Kind), NumberType(I64Kind));
            verify_incompatibility(NumberType(I64Kind), BooleanType);
        }

        fn verify_compatibility(a: DataType, b: DataType) {
            assert!(a.is_compatible(&b), "{} is not compatible with {}", a, b);
        }

        fn verify_incompatibility(a: DataType, b: DataType) {
            assert!(!a.is_compatible(&b), "{} is compatible with {}", a, b);
        }
    }

}