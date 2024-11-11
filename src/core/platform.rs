////////////////////////////////////////////////////////////////////
// Platform class
////////////////////////////////////////////////////////////////////

use crate::codec::Codec;
use crate::compiler::Compiler;
use crate::data_types::DataType::{LazyEvalType, PlatformFunctionType, StringType};
use crate::data_types::{DataType, StorageTypes};
use crate::errors::Errors::{ArgumentsMismatched, AssertionError, CollectionExpected, ConversionError, DateExpected, Exact, StringExpected, TableExpected, TypeMismatch};
use crate::file_row_collection::FileRowCollection;
use crate::machine::Machine;
use crate::model_row_collection::ModelRowCollection;
use crate::namespaces::Namespace;
use crate::numbers::NumberValue::{F32Value, F64Value, I128Value, I16Value, I32Value, I64Value, I8Value, U128Value, U16Value, U32Value, U64Value, U8Value};
use crate::outcomes::Outcomes::Ack;
use crate::parameter::Parameter;
use crate::row_collection::RowCollection;
use crate::rows::Row;
use crate::structures::Structure;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{Array, Boolean, DateValue, ErrorValue, NamespaceValue, Number, Outcome, PlatformFunction, StringValue, StructureHard, StructureSoft, TableValue, Undefined};
use chrono::{Datelike, Local, TimeZone, Timelike};
use crossterm::style::Stylize;
use serde::{Deserialize, Serialize};
use shared_lib::fail;
use std::collections::HashMap;
use std::path::Path;
use std::process::Output;
use std::{env, thread};
use tokio::runtime::Runtime;
use uuid::Uuid;
use PlatformFunctions::*;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
struct FunctionInfo {
    name: String,
    package: String,
    parameters: Vec<Parameter>,
    opcode: PlatformFunctions,
}

/// Represents an enumeration of Oxide platform functions
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum PlatformFunctions {
    // io package
    IoCurrentDir,
    IoExists,
    IoStdErr,
    IoStdOut,
    // lang package
    LangAssert,
    LangMatches,
    LangPrintln,
    LangTypeOf,
    // os package
    OsEnvVars,
    OsSysCall,
    // oxide package
    OxideEval,
    OxideHome,
    OxideReset,
    OxideServe,
    OxideVariables,
    OxideVersion,
    // str package
    StrEndsWith,
    StrFormat,
    StrLeft,
    StrRight,
    StrStartsWith,
    StrSubstring,
    StrToString,
    // util package
    UtilDatetimeDay,
    UtilDatetimeHour12,
    UtilDatetimeHour24,
    UtilDatetimeMinute,
    UtilDatetimeMonth,
    UtilDatetimeSecond,
    UtilDatetimeYear,
    UtilDescribe,
    UtilNow,
    UtilReverse,
    UtilScan,
    UtilToArray,
    UtilToCSV,
    UtilToF32,
    UtilToF64,
    UtilToJSON,
    UtilToI8,
    UtilToI16,
    UtilToI32,
    UtilToI64,
    UtilToI128,
    UtilToTable,
    UtilToU8,
    UtilToU16,
    UtilToU32,
    UtilToU64,
    UtilToU128,
    UtilUUID,
}

pub const PLATFORM_OPCODES: [PlatformFunctions; 51] = [
    // io
    IoCurrentDir, IoExists, IoStdErr, IoStdOut,
    // lang
    LangAssert, LangMatches, LangPrintln, LangTypeOf,
    // os
    OsEnvVars, OsSysCall,
    // oxide
    OxideEval, OxideHome, OxideReset, OxideServe, OxideVariables, OxideVersion,
    // str
    StrEndsWith, StrFormat, StrLeft, StrRight, StrStartsWith, StrSubstring, StrToString,
    // util
    UtilDatetimeDay, UtilDatetimeHour12, UtilDatetimeHour24,
    UtilDatetimeMinute, UtilDatetimeMonth, UtilDatetimeSecond, UtilDatetimeYear,
    UtilDescribe, UtilNow, UtilReverse, UtilScan, UtilToArray, UtilToCSV, UtilToF32, UtilToF64,
    UtilToI8, UtilToI16, UtilToI32, UtilToI64, UtilToI128, UtilToJSON,
    UtilToU8, UtilToU16, UtilToU32, UtilToU64, UtilToU128, UtilToTable, UtilUUID,
];

impl PlatformFunctions {
    pub fn decode(bytes: Vec<u8>) -> std::io::Result<PlatformFunctions> {
        Codec::unwrap_as_result(bincode::deserialize(bytes.as_slice()))
    }

    pub fn encode(&self) -> std::io::Result<Vec<u8>> {
        Codec::unwrap_as_result(bincode::serialize(self))
    }

    /// Evaluates the platform function
    pub fn evaluate(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> (Machine, TypedValue) {
        use crate::machine::{MAJOR_VERSION, MINOR_VERSION};
        match self {
            PlatformFunctions::IoCurrentDir => self.evaluate_fn0(ms, args, Self::do_current_dir),
            PlatformFunctions::IoExists => self.evaluate_fn1(ms, args, Self::do_exists),
            PlatformFunctions::IoStdErr => self.evaluate_fn1(ms, args, Self::do_stderr),
            PlatformFunctions::IoStdOut => self.evaluate_fn1(ms, args, Self::do_stdout),
            PlatformFunctions::LangAssert => self.evaluate_fn1(ms, args, Self::do_assert),
            PlatformFunctions::LangMatches => self.evaluate_fn2(ms, args, |ms, a, b| (ms, a.matches(b))),
            PlatformFunctions::LangPrintln => self.evaluate_fn1(ms, args, Self::do_stdout),
            PlatformFunctions::LangTypeOf => self.evaluate_fn1(ms, args, |ms, a| (ms, StringValue(a.get_type_name()))),
            PlatformFunctions::OsEnvVars => (ms, Self::create_env_table()),
            PlatformFunctions::OsSysCall => ms.to_owned().ok_to_tv(Self::evaluate_syscall(ms, args)),
            PlatformFunctions::OxideEval => self.evaluate_fn1(ms, args, Self::do_eval),
            PlatformFunctions::OxideHome => self.evaluate_fn0(ms, args, |ms| (ms, StringValue(Machine::oxide_home()))),
            PlatformFunctions::OxideReset => self.evaluate_fn0(ms, args, |_| (Machine::empty(), Outcome(Ack))),
            PlatformFunctions::OxideServe => self.evaluate_fn1(ms, args, Self::do_serve),
            PlatformFunctions::OxideVariables => self.evaluate_fn0(ms, args, |ms| (ms.clone(), TableValue(ms.get_variables()))),
            PlatformFunctions::OxideVersion => self.evaluate_fn0(ms, args, |ms| (ms, StringValue(format!("{MAJOR_VERSION}.{MINOR_VERSION}")))),
            PlatformFunctions::StrEndsWith => self.evaluate_fn2(ms, args, Self::do_string_ends_with),
            PlatformFunctions::StrFormat => Self::do_format_string(args),
            PlatformFunctions::StrLeft => self.evaluate_fn2(ms, args, Self::evaluate_string_left),
            PlatformFunctions::StrRight => self.evaluate_fn2(ms, args, Self::evaluate_string_right),
            PlatformFunctions::StrStartsWith => self.evaluate_fn2(ms, args, Self::do_string_start_with),
            PlatformFunctions::StrSubstring => self.evaluate_fn3(ms, args, Self::evaluate_substring),
            PlatformFunctions::StrToString => self.evaluate_fn1(ms, args, |ms, a| (ms, StringValue(a.to_string()))),
            PlatformFunctions::UtilDatetimeDay => self.evaluate_fn1pf(ms, args, Self::do_date_part),
            PlatformFunctions::UtilDatetimeHour24 => self.evaluate_fn1pf(ms, args, Self::do_date_part),
            PlatformFunctions::UtilDatetimeHour12 => self.evaluate_fn1pf(ms, args, Self::do_date_part),
            PlatformFunctions::UtilDatetimeMinute => self.evaluate_fn1pf(ms, args, Self::do_date_part),
            PlatformFunctions::UtilDatetimeMonth => self.evaluate_fn1pf(ms, args, Self::do_date_part),
            PlatformFunctions::UtilDatetimeSecond => self.evaluate_fn1pf(ms, args, Self::do_date_part),
            PlatformFunctions::UtilDatetimeYear => self.evaluate_fn1pf(ms, args, Self::do_date_part),
            PlatformFunctions::UtilDescribe => self.evaluate_fn1(ms, args, Self::do_table_describe),
            PlatformFunctions::UtilNow => ms.to_owned().ok_to_tv(Self::evaluate_timestamp(ms, args)),
            PlatformFunctions::UtilReverse => self.evaluate_fn1(ms, args, Self::do_reverse),
            PlatformFunctions::UtilScan => self.evaluate_fn1(ms, args, Self::do_table_scan),
            PlatformFunctions::UtilToArray => self.evaluate_fn1(ms, args, Self::convert_to_array),
            PlatformFunctions::UtilToCSV => self.evaluate_fn1(ms, args, Self::convert_to_csv),
            PlatformFunctions::UtilToF32 => self.evaluate_fn1pf(ms, args, Self::do_conversion),
            PlatformFunctions::UtilToF64 => self.evaluate_fn1pf(ms, args, Self::do_conversion),
            PlatformFunctions::UtilToI8 => self.evaluate_fn1pf(ms, args, Self::do_conversion),
            PlatformFunctions::UtilToI16 => self.evaluate_fn1pf(ms, args, Self::do_conversion),
            PlatformFunctions::UtilToI32 => self.evaluate_fn1pf(ms, args, Self::do_conversion),
            PlatformFunctions::UtilToI64 => self.evaluate_fn1pf(ms, args, Self::do_conversion),
            PlatformFunctions::UtilToI128 => self.evaluate_fn1pf(ms, args, Self::do_conversion),
            PlatformFunctions::UtilToJSON => self.evaluate_fn1(ms, args, Self::convert_to_json),
            PlatformFunctions::UtilToTable => self.evaluate_fn1(ms, args, |ms, a| (ms, a.to_table_value())),
            PlatformFunctions::UtilToU8 => self.evaluate_fn1pf(ms, args, Self::do_conversion),
            PlatformFunctions::UtilToU16 => self.evaluate_fn1pf(ms, args, Self::do_conversion),
            PlatformFunctions::UtilToU32 => self.evaluate_fn1pf(ms, args, Self::do_conversion),
            PlatformFunctions::UtilToU64 => self.evaluate_fn1pf(ms, args, Self::do_conversion),
            PlatformFunctions::UtilToU128 => self.evaluate_fn1pf(ms, args, Self::do_conversion),
            PlatformFunctions::UtilUUID => ms.to_owned().ok_to_tv(self.evaluate_uuid(ms, args)),
        }
    }

    fn create_env_table() -> TypedValue {
        use std::env;
        let mut mrc = ModelRowCollection::construct(&vec![
            Parameter::new("key", Some("String(256)".into()), None),
            Parameter::new("value", Some("String(8192)".into()), None)
        ]);
        for (key, value) in env::vars() {
            if let ErrorValue(err) = mrc.append_row(Row::new(0, vec![
                StringValue(key), StringValue(value)
            ])) {
                return ErrorValue(err);
            }
        }

        TableValue(mrc)
    }

    /// Builds a mapping of package name to function vector
    pub fn create_packages() -> HashMap<String, Vec<PlatformFunctions>> {
        PLATFORM_OPCODES.iter()
            .fold(HashMap::new(), |mut hm, key| {
                hm.entry(key.get_package_name())
                    .or_insert_with(Vec::new)
                    .push(key.to_owned());
                hm
            })
    }

    fn do_assert(ms: Machine, value: &TypedValue) -> (Machine, TypedValue) {
        match value {
            ErrorValue(msg) => (ms, ErrorValue(msg.to_owned())),
            Boolean(false) => (ms, ErrorValue(AssertionError("true".to_string(), "false".to_string()))),
            z => (ms, z.to_owned())
        }
    }

    fn do_conversion(
        ms: Machine,
        value: &TypedValue,
        plat: &PlatformFunctions,
    ) -> (Machine, TypedValue) {
        let result = match plat {
            PlatformFunctions::UtilToF32 => Number(F32Value(value.to_f32())),
            PlatformFunctions::UtilToF64 => Number(F64Value(value.to_f64())),
            PlatformFunctions::UtilToI8 => Number(I8Value(value.to_i8())),
            PlatformFunctions::UtilToI16 => Number(I16Value(value.to_i16())),
            PlatformFunctions::UtilToI32 => Number(I32Value(value.to_i32())),
            PlatformFunctions::UtilToI64 => Number(I64Value(value.to_i64())),
            PlatformFunctions::UtilToI128 => Number(I128Value(value.to_i128())),
            PlatformFunctions::UtilToU8 => Number(U8Value(value.to_u8())),
            PlatformFunctions::UtilToU16 => Number(U16Value(value.to_u16())),
            PlatformFunctions::UtilToU32 => Number(U32Value(value.to_u32())),
            PlatformFunctions::UtilToU64 => Number(U64Value(value.to_u64())),
            PlatformFunctions::UtilToU128 => Number(U128Value(value.to_u128())),
            plat => ErrorValue(ConversionError(plat.to_code()))
        };
        (ms, result)
    }

    fn do_current_dir(ms: Machine) -> (Machine, TypedValue) {
        match env::current_dir() {
            Ok(dir) => (ms, StringValue(dir.display().to_string())),
            Err(err) => (ms, ErrorValue(Exact(err.to_string())))
        }
    }

    fn do_date_part(
        ms: Machine,
        value: &TypedValue,
        plat: &PlatformFunctions,
    ) -> (Machine, TypedValue) {
        match value {
            DateValue(epoch_millis) => {
                let datetime = {
                    let seconds = epoch_millis / 1000;
                    let millis_part = epoch_millis % 1000;
                    Local.timestamp(seconds, (millis_part * 1_000_000) as u32)
                };
                match plat {
                    PlatformFunctions::UtilDatetimeDay => (ms, Number(U32Value(datetime.day()))),
                    PlatformFunctions::UtilDatetimeHour24 => (ms, Number(U32Value(datetime.hour()))),
                    PlatformFunctions::UtilDatetimeHour12 => (ms, Number(U32Value(datetime.hour12().1))),
                    PlatformFunctions::UtilDatetimeMinute => (ms, Number(U32Value(datetime.minute()))),
                    PlatformFunctions::UtilDatetimeMonth => (ms, Number(U32Value(datetime.second()))),
                    PlatformFunctions::UtilDatetimeSecond => (ms, Number(U32Value(datetime.second()))),
                    PlatformFunctions::UtilDatetimeYear => (ms, Number(I32Value(datetime.year()))),
                    plat => (ms, ErrorValue(ConversionError(plat.to_code())))
                }
            }
            other => (ms, ErrorValue(DateExpected(other.to_code())))
        }
    }

    fn do_eval(ms: Machine, query_value: &TypedValue) -> (Machine, TypedValue) {
        match query_value {
            StringValue(ql) =>
                match Compiler::compile_script(ql.as_str()) {
                    Ok(opcode) =>
                        match ms.evaluate(&opcode) {
                            Ok((machine, tv)) => (machine, tv),
                            Err(err) => (ms, ErrorValue(Exact(err.to_string())))
                        }
                    Err(err) => (ms, ErrorValue(Exact(err.to_string())))
                }
            x => (ms, ErrorValue(StringExpected(x.get_type_name())))
        }
    }

    fn do_exists(ms: Machine, path_value: &TypedValue) -> (Machine, TypedValue) {
        match path_value {
            StringValue(path) => (ms, Boolean(Path::new(path).exists())),
            other => (ms, ErrorValue(StringExpected(other.to_string())))
        }
    }

    /// Formats a string based on a template
    /// Ex: format("This {} the {}", "is", "way") => "This is the way"
    fn do_format_string(
        args: Vec<TypedValue>
    ) -> (Machine, TypedValue) {
        // internal parsing function
        fn format_text(
            ms: Machine,
            template: String,
            replacements: Vec<TypedValue>,
        ) -> (Machine, TypedValue) {
            let mut result = String::from(template);
            let mut replacement_iter = replacements.iter();

            // replace each placeholder "{}" with the next element from the vector
            while let Some(pos) = result.find("{}") {
                // get the next replacement, if available
                if let Some(replacement) = replacement_iter.next() {
                    result.replace_range(pos..pos + 2, replacement.unwrap_value().as_str()); // Replace the "{}" with the replacement
                } else {
                    break; // no more replacements available, break out of the loop
                }
            }

            (ms, StringValue(result))
        }

        // parse the arguments
        let ms = Machine::empty();
        if args.is_empty() { (ms, StringValue("".to_string())) } else {
            match (args[0].to_owned(), args[1..].to_owned()) {
                (StringValue(format_str), format_args) =>
                    format_text(ms, format_str, format_args),
                (other, ..) =>
                    (ms, ErrorValue(StringExpected(other.to_code())))
            }
        }
    }

    fn do_reverse(ms: Machine, value: &TypedValue) -> (Machine, TypedValue) {
        match value {
            Array(a) => (ms, Array(a.iter().rev()
                .map(|v| v.to_owned())
                .collect::<Vec<_>>())),
            NamespaceValue(a, b, c) =>
                (ms.clone(), match Self::convert_to_table_value(&Namespace::new(a, b, c)) {
                    TableValue(mrc) => mrc.reverse_table_value(),
                    other => ErrorValue(TypeMismatch("Table".to_string(), other.to_code()))
                }),
            StringValue(s) => (ms, StringValue(s.to_owned().reverse().to_string())),
            TableValue(mrc) => (ms, mrc.reverse_table_value()),
            other => (ms, ErrorValue(TypeMismatch("Array, String or Table".into(), other.to_string())))
        }
    }

    fn do_stdout(ms: Machine, value: &TypedValue) -> (Machine, TypedValue) {
        println!("{}", value.unwrap_value());
        (ms, Outcome(Ack))
    }

    fn do_stderr(ms: Machine, value: &TypedValue) -> (Machine, TypedValue) {
        eprintln!("{}", value.unwrap_value());
        (ms, Outcome(Ack))
    }

    fn do_string_ends_with(
        ms: Machine,
        string_value: &TypedValue,
        slice_value: &TypedValue,
    ) -> (Machine, TypedValue) {
        match string_value {
            StringValue(src) =>
                match slice_value {
                    StringValue(slice) => (ms, Boolean(src.ends_with(slice))),
                    z => (ms, ErrorValue(StringExpected(z.to_code())))
                }
            z => (ms, ErrorValue(StringExpected(z.to_code())))
        }
    }

    fn do_string_start_with(
        ms: Machine,
        string_value: &TypedValue,
        slice_value: &TypedValue,
    ) -> (Machine, TypedValue) {
        match string_value {
            StringValue(src) => {
                match slice_value {
                    StringValue(slice) => (ms, Boolean(src.starts_with(slice))),
                    z => (ms, ErrorValue(StringExpected(z.to_code())))
                }
            }
            z => (ms, ErrorValue(StringExpected(z.to_code())))
        }
    }

    fn convert_to_array(ms: Machine, value: &TypedValue) -> (Machine, TypedValue) {
        match value {
            Array(items) => (ms, Array(items.to_owned())),
            Number(nv) => (ms, Array(nv.encode().iter()
                .map(|n| Number(U8Value(*n)))
                .collect::<Vec<_>>())),
            StringValue(text) => (ms, Array(text.chars()
                .map(|c| StringValue(c.to_string()))
                .collect::<Vec<_>>())),
            StructureHard(hs) => (ms, Array(hs.get_values())),
            StructureSoft(ss) => (ms, Array(ss.get_values())),
            TableValue(mrc) => {
                let columns = mrc.get_columns();
                (ms, Array(mrc.get_rows().iter()
                    .map(|r| StructureHard(r.to_struct(columns)))
                    .collect::<Vec<_>>()))
            }
            _ => (ms, Undefined)
        }
    }

    fn convert_to_csv(ms: Machine, value: &TypedValue) -> (Machine, TypedValue) {
        Self::convert_to_csv_or_json(ms, value, true)
    }

    fn convert_to_csv_or_json(
        ms: Machine,
        value: &TypedValue,
        is_csv: bool,
    ) -> (Machine, TypedValue) {
        match value {
            NamespaceValue(d, s, n) =>
                match FileRowCollection::open(&Namespace::new(d, s, n)) {
                    Ok(frc) => {
                        let rc = Box::new(frc);
                        if is_csv { Self::evaluate_to_csv(ms, rc) } else { Self::evaluate_to_json(ms, rc) }
                    }
                    Err(err) => (ms.to_owned(), ErrorValue(Exact(err.to_string())))
                }
            TableValue(mrc) => {
                let rc = Box::new(mrc.to_owned());
                if is_csv { Self::evaluate_to_csv(ms, rc) } else { Self::evaluate_to_json(ms, rc) }
            }
            _ => (ms.to_owned(), ErrorValue(Exact(format!("Cannot convert to {}", if is_csv { "CSV" } else { "JSON" }))))
        }
    }

    fn convert_to_json(ms: Machine, value: &TypedValue) -> (Machine, TypedValue) {
        Self::convert_to_csv_or_json(ms, value, false)
    }

    fn convert_to_table_value(ns: &Namespace) -> TypedValue {
        match FileRowCollection::open(ns) {
            Err(err) => ErrorValue(Exact(err.to_string())),
            Ok(frc) => {
                let columns = frc.get_columns().to_owned();
                match frc.read_active_rows() {
                    Err(err) => ErrorValue(Exact(err.to_string())),
                    Ok(rows) => TableValue(ModelRowCollection::from_rows(columns, rows))
                }
            }
        }
    }

    fn evaluate_to_csv(
        ms: Machine,
        rc: Box<dyn RowCollection>,
    ) -> (Machine, TypedValue) {
        (ms, Array(rc.iter().map(|row| row.to_csv())
            .map(StringValue).collect()))
    }

    fn evaluate_to_json(
        ms: Machine,
        rc: Box<dyn RowCollection>,
    ) -> (Machine, TypedValue) {
        (ms, Array(rc.iter().map(|row| row.to_json(rc.get_columns()))
            .map(StringValue).collect()))
    }

    fn evaluate_fn0(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
        f: fn(Machine) -> (Machine, TypedValue),
    ) -> (Machine, TypedValue) {
        match args.len() {
            0 => f(ms),
            n => (ms, ErrorValue(ArgumentsMismatched(0, n)))
        }
    }

    fn evaluate_fn1(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
        f: fn(Machine, &TypedValue) -> (Machine, TypedValue),
    ) -> (Machine, TypedValue) {
        match args.as_slice() {
            [value] => f(ms, value),
            _ => (ms, ErrorValue(ArgumentsMismatched(1, args.len())))
        }
    }

    fn evaluate_fn1pf(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
        f: fn(Machine, &TypedValue, &PlatformFunctions) -> (Machine, TypedValue),
    ) -> (Machine, TypedValue) {
        match args.as_slice() {
            [a] => f(ms, a, self),
            _ => (ms, ErrorValue(ArgumentsMismatched(1, args.len())))
        }
    }

    fn evaluate_fn2(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
        f: fn(Machine, &TypedValue, &TypedValue) -> (Machine, TypedValue),
    ) -> (Machine, TypedValue) {
        match args.as_slice() {
            [a, b] => f(ms, a, b),
            _ => (ms, ErrorValue(ArgumentsMismatched(2, args.len())))
        }
    }

    fn evaluate_fn3(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
        f: fn(Machine, &TypedValue, &TypedValue, &TypedValue) -> (Machine, TypedValue),
    ) -> (Machine, TypedValue) {
        match args.as_slice() {
            [a, b, c] => f(ms, a, b, c),
            _ => (ms, ErrorValue(ArgumentsMismatched(3, args.len())))
        }
    }

    fn do_table_describe(
        ms: Machine,
        item: &TypedValue,
    ) -> (Machine, TypedValue) {
        match item {
            ErrorValue(message) => (ms, ErrorValue(message.to_owned())),
            NamespaceValue(d, s, n) =>
                match FileRowCollection::open(&Namespace::new(d, s, n)) {
                    Ok(frc) => (ms, frc.describe()),
                    Err(err) => (ms, ErrorValue(Exact(err.to_string())))
                }
            StructureHard(sh) => (ms, sh.to_table().describe()),
            StructureSoft(ss) => (ms, ss.to_table().describe()),
            TableValue(mrc) => (ms, mrc.describe()),
            other =>
                (ms, ErrorValue(TableExpected("table or struct".to_string(), other.to_code())))
        }
    }

    fn do_table_scan(machine: Machine, tv_table: &TypedValue) -> (Machine, TypedValue) {
        Machine::orchestrate_rc(machine, tv_table.to_owned(), |machine, rc| {
            match rc.examine_rows() {
                Ok(rows) => {
                    let columns = rows.first()
                        .map(|row| rc.get_columns().to_owned())
                        .unwrap_or(Vec::new());
                    let mrc = ModelRowCollection::from_rows(columns, rows);
                    (machine, TableValue(mrc))
                }
                Err(err) => (machine, ErrorValue(Exact(err.to_string())))
            }
        })
    }

    fn do_serve(
        ms: Machine,
        port: &TypedValue,
    ) -> (Machine, TypedValue) {
        use crate::web_routes;
        use actix_web::web;
        use crate::rest_server::*;
        let port = port.to_usize();
        thread::spawn(move || {
            let server = actix_web::HttpServer::new(move || web_routes!(SharedState::new()))
                .bind(format!("{}:{}", "0.0.0.0", port))
                .expect(format!("Can not bind to port {port}").as_str())
                .run();
            Runtime::new()
                .expect("Failed to create a Runtime instance")
                .block_on(server)
                .expect(format!("Failed while blocking on port {port}").as_str());
        });
        (ms, Outcome(Ack))
    }

    fn evaluate_substring(
        ms: Machine,
        string: &TypedValue,
        a: &TypedValue,
        b: &TypedValue,
    ) -> (Machine, TypedValue) {
        (ms.to_owned(), match string {
            StringValue(s) =>
                match (a, b) {
                    (Number(na), Number(nb)) =>
                        StringValue(s[na.to_usize()..nb.to_usize()].to_string()),
                    (..) => Undefined
                },
            _ => Undefined
        })
    }

    fn evaluate_string_left(
        ms: Machine,
        string: &TypedValue,
        n_chars: &TypedValue,
    ) -> (Machine, TypedValue) {
        let result = match string {
            StringValue(s) =>
                match n_chars {
                    Number(nv) if nv.to_i64() < 0 =>
                        Self::evaluate_string_right(ms.to_owned(), string, &Number(I64Value(-nv.to_i64()))).1,
                    Number(nv) =>
                        StringValue(s[0..nv.to_usize()].to_string()),
                    _ => Undefined
                },
            _ => Undefined
        };
        (ms, result)
    }

    fn evaluate_string_right(
        ms: Machine,
        string: &TypedValue,
        n_chars: &TypedValue,
    ) -> (Machine, TypedValue) {
        let result = match string {
            StringValue(s) =>
                match n_chars {
                    Number(nv) if nv.to_i64() < 0 =>
                        Self::evaluate_string_left(ms.to_owned(), string, &Number(I64Value(-nv.to_i64()))).1,
                    Number(nv) => {
                        let strlen = s.len();
                        StringValue(s[(strlen - nv.to_usize())..strlen].to_string())
                    }
                    _ => Undefined
                },
            _ => Undefined
        };
        (ms, result)
    }

    fn evaluate_syscall(
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let items: Vec<_> = args.iter().map(|i| i.unwrap_value()).collect();
        if let Some((command, cmd_args)) = Self::split_first(items) {
            let output: Output = std::process::Command::new(command).args(cmd_args).output()?;
            let result: TypedValue =
                if output.status.success() {
                    let raw_text = String::from_utf8_lossy(&output.stdout);
                    StringValue(raw_text.to_string())
                } else {
                    let message = String::from_utf8_lossy(&output.stderr);
                    ErrorValue(Exact(message.to_string()))
                };
            Ok((ms, result))
        } else {
            Ok((ms, ErrorValue(CollectionExpected(args.iter()
                .map(|e| e.to_code())
                .collect::<Vec<_>>()
                .join(", ")))))
        }
    }

    fn evaluate_timestamp(
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        if !args.is_empty() {
            return fail(format!("No arguments expected, but found {}", args.len()));
        }
        Ok((ms, DateValue(Local::now().timestamp_millis())))
    }

    fn evaluate_uuid(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        if !args.is_empty() {
            return fail(format!("No arguments expected, but found {}", args.len()));
        }
        Ok((ms, Number(U128Value(Uuid::new_v4().as_u128()))))
    }

    pub fn get_info(&self) -> FunctionInfo {
        FunctionInfo {
            name: self.get_name(),
            package: self.get_package_name(),
            parameters: self.get_parameters(),
            opcode: self.to_owned(),
        }
    }

    pub fn get_name(&self) -> String {
        let result = match self {
            PlatformFunctions::IoCurrentDir => "current_dir",
            PlatformFunctions::IoExists => "exists",
            PlatformFunctions::IoStdErr => "stderr",
            PlatformFunctions::IoStdOut => "stdout",
            PlatformFunctions::LangAssert => "assert",
            PlatformFunctions::LangMatches => "matches",
            PlatformFunctions::LangPrintln => "println",
            PlatformFunctions::LangTypeOf => "type_of",
            PlatformFunctions::OsEnvVars => "env",
            PlatformFunctions::OsSysCall => "call",
            PlatformFunctions::OxideEval => "eval",
            PlatformFunctions::OxideHome => "home",
            PlatformFunctions::OxideReset => "reset",
            PlatformFunctions::OxideServe => "serve",
            PlatformFunctions::OxideVariables => "env",
            PlatformFunctions::OxideVersion => "version",
            PlatformFunctions::StrEndsWith => "ends_with",
            PlatformFunctions::StrFormat => "format",
            PlatformFunctions::StrLeft => "left",
            PlatformFunctions::StrRight => "right",
            PlatformFunctions::StrStartsWith => "starts_with",
            PlatformFunctions::StrSubstring => "substring",
            PlatformFunctions::StrToString => "to_string",
            PlatformFunctions::UtilDatetimeDay => "day_of",
            PlatformFunctions::UtilDatetimeHour24 => "hour24",
            PlatformFunctions::UtilDatetimeHour12 => "hour12",
            PlatformFunctions::UtilDatetimeMinute => "minute_of",
            PlatformFunctions::UtilDatetimeMonth => "month_of",
            PlatformFunctions::UtilDatetimeSecond => "second_of",
            PlatformFunctions::UtilDatetimeYear => "year_of",
            PlatformFunctions::UtilDescribe => "describe",
            PlatformFunctions::UtilNow => "now",
            PlatformFunctions::UtilReverse => "reverse",
            PlatformFunctions::UtilScan => "scan",
            PlatformFunctions::UtilToArray => "to_array",
            PlatformFunctions::UtilToCSV => "to_csv",
            PlatformFunctions::UtilToJSON => "to_json",
            PlatformFunctions::UtilToF32 => "to_f32",
            PlatformFunctions::UtilToF64 => "to_f64",
            PlatformFunctions::UtilToI8 => "to_i8",
            PlatformFunctions::UtilToI16 => "to_i16",
            PlatformFunctions::UtilToI32 => "to_i32",
            PlatformFunctions::UtilToI64 => "to_i64",
            PlatformFunctions::UtilToI128 => "to_i128",
            PlatformFunctions::UtilToTable => "to_table",
            PlatformFunctions::UtilToU8 => "to_u8",
            PlatformFunctions::UtilToU16 => "to_u16",
            PlatformFunctions::UtilToU32 => "to_u32",
            PlatformFunctions::UtilToU64 => "to_u64",
            PlatformFunctions::UtilToU128 => "to_u128",
            PlatformFunctions::UtilUUID => "uuid",
        };
        result.to_string()
    }

    pub fn get_package_name(&self) -> String {
        use PlatformFunctions::*;
        let result = match self {
            // io
            IoCurrentDir | IoExists | IoStdErr | IoStdOut => "io",
            // lang
            LangAssert | LangMatches | LangPrintln | LangTypeOf => "lang",
            // os
            OsEnvVars | OsSysCall => "os",
            // oxide
            OxideEval | OxideHome | OxideReset | OxideServe |
            OxideVariables | OxideVersion => "oxide",
            // str
            StrEndsWith | StrFormat | StrLeft | StrRight |
            StrStartsWith | StrSubstring | StrToString => "str",
            // util
            UtilDatetimeDay | UtilDatetimeHour12 | UtilDatetimeHour24 |
            UtilDatetimeMinute | UtilDatetimeMonth | UtilDatetimeSecond | UtilDatetimeYear |
            UtilDescribe | UtilNow | UtilReverse | UtilScan | UtilToArray |
            UtilToCSV | UtilToJSON | UtilToF32 | UtilToF64 |
            UtilToI8 | UtilToI16 | UtilToI32 | UtilToI64 | UtilToI128 |
            UtilToTable |
            UtilToU8 | UtilToU16 | UtilToU32 | UtilToU64 | UtilToU128 | UtilUUID => "util"
        };
        result.to_string()
    }

    pub fn get_parameter_types(&self) -> Vec<DataType> {
        use PlatformFunctions::*;
        match self {
            IoCurrentDir | OsEnvVars | OxideHome | OxideReset |
            OxideVariables | OxideVersion | UtilNow | UtilUUID => Vec::new(),
            UtilDescribe => vec![
                LazyEvalType
            ],
            IoExists | IoStdErr | IoStdOut | LangAssert | LangPrintln | LangTypeOf |
            OsSysCall | OxideEval | OxideServe |
            StrToString | UtilDatetimeDay | UtilDatetimeHour12 | UtilDatetimeHour24 | UtilDatetimeMinute |
            UtilDatetimeMonth | UtilDatetimeSecond | UtilDatetimeYear | UtilReverse | UtilScan | UtilToArray |
            UtilToCSV | UtilToF32 | UtilToF64 | UtilToI8 | UtilToI16 | UtilToI32 | UtilToI64 | UtilToI128 | UtilToJSON |
            UtilToTable | UtilToU8 | UtilToU16 | UtilToU32 | UtilToU64 | UtilToU128 => vec![
                StringType(StorageTypes::BLOB)
            ],
            LangMatches | StrEndsWith | StrFormat | StrLeft | StrRight | StrStartsWith => vec![
                StringType(StorageTypes::BLOB), StringType(StorageTypes::BLOB)
            ],
            StrSubstring => vec![
                StringType(StorageTypes::BLOB), StringType(StorageTypes::BLOB), StringType(StorageTypes::BLOB)
            ],
        }
    }

    fn get_parameter_count(&self) -> u8 {
        use PlatformFunctions::*;
        match self {
            IoCurrentDir | OsEnvVars | OxideHome | OxideReset |
            OxideVariables | OxideVersion | UtilNow | UtilUUID => 0,
            UtilDescribe => 1,
            IoExists | IoStdErr | IoStdOut | LangAssert | LangPrintln | LangTypeOf | OsSysCall | OxideEval | OxideServe |
            StrToString | UtilDatetimeDay | UtilDatetimeHour12 | UtilDatetimeHour24 | UtilDatetimeMinute |
            UtilDatetimeMonth | UtilDatetimeSecond | UtilDatetimeYear | UtilReverse | UtilScan | UtilToArray |
            UtilToCSV | UtilToF32 | UtilToF64 | UtilToI8 | UtilToI16 | UtilToI32 | UtilToI64 | UtilToI128 | UtilToJSON |
            UtilToTable | UtilToU8 | UtilToU16 | UtilToU32 | UtilToU64 | UtilToU128 => 1,
            LangMatches | StrEndsWith | StrFormat | StrLeft | StrRight | StrStartsWith => 2,
            StrSubstring => 3,
        }
    }

    pub fn get_parameters(&self) -> Vec<Parameter> {
        self.get_parameter_types().iter().enumerate()
            .map(|(n, dt)| Parameter::new(
                format!("{}", ((n + 97) as u8) as char),
                dt.to_type_declaration(),
                None,
            ))
            .collect()
    }

    pub fn get_type(&self) -> DataType {
        PlatformFunctionType(self.clone())
    }

    fn split_first<T>(vec: Vec<T>) -> Option<(T, Vec<T>)> {
        let mut iter = vec.into_iter();
        iter.next().map(|first| (first, iter.collect()))
    }

    pub fn to_code(&self) -> String {
        let pkg = self.get_package_name();
        let name = self.get_name();
        let params = self.get_parameters().iter()
            .map(|p| p.to_code())
            .collect::<Vec<_>>()
            .join(", ");
        format!("{pkg}::{name}({params})")
    }
}

/// Unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::interpreter::Interpreter;
    use crate::platform::{PlatformFunctions, PLATFORM_OPCODES};
    use crate::structures::{HardStructure, SoftStructure};
    use crate::table_columns::Column;
    use crate::table_renderer::TableRenderer;
    use crate::testdata::{make_quote, make_quote_columns, make_scan_quote, verify_exact, verify_when, verify_where};
    use crate::typed_values::TypedValue::{PlatformFunction, StructureSoft, TableValue, Undefined};

    #[test]
    fn test_create_env_table() {
        match PlatformFunctions::create_env_table() {
            TableValue(mrc) => {
                let rc: Box<dyn RowCollection> = Box::from(mrc);
                for s in TableRenderer::from_table(&rc) {
                    println!("{}", s)
                }
            }
            rc => assert!(matches!(rc, TableValue(..)))
        }
    }

    #[test]
    fn test_encode_decode() {
        for expected in PLATFORM_OPCODES {
            let bytes = expected.encode().unwrap();
            assert_eq!(bytes.len(), 4);

            let actual = PlatformFunctions::decode(bytes).unwrap();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn test_get_info() {
        assert_eq!(StrSubstring.get_info(), FunctionInfo {
            name: "substring".to_string(),
            package: "str".to_string(),
            parameters: vec![
                Parameter::new("a", Some("String()".into()), None),
                Parameter::new("b", Some("String()".into()), None),
                Parameter::new("c", Some("String()".into()), None)
            ],
            opcode: StrSubstring,
        });
    }

    #[test]
    fn test_get_type() {
        assert_eq!(OxideHome.get_type(), PlatformFunctionType(OxideHome));
    }

    #[test]
    fn test_io_current_dir() {
        let phys_columns = make_quote_columns();
        verify_exact(r#"
            import io, str
            cur_dir := io::current_dir()
            prefix := iff(cur_dir:::ends_with("core"), "../..", ".")
            path_str := prefix + "/demoes/language/include_file.oxide"
            include path_str
        "#, TableValue(ModelRowCollection::from_rows(phys_columns.clone(), vec![
            make_quote(0, "ABC", "AMEX", 12.49),
            make_quote(1, "BOOM", "NYSE", 56.88),
            make_quote(2, "JET", "NASDAQ", 32.12),
        ])));

        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
            include 123
        "#).unwrap();
        assert_eq!(result, ErrorValue(TypeMismatch("String".into(), "i64".into())))
    }

    #[test]
    fn test_io_exists() {
        verify_exact(r#"io::stderr("Goodbye Cruel World")"#, Outcome(Ack));
        verify_exact(r#"io::stdout("Hello World")"#, Outcome(Ack));
        verify_exact(r#"
            import io
            path_str := oxide::home()
            path_str:::exists()
        "#, Boolean(true))
    }

    #[test]
    fn test_os_call_qualified() {
        verify_exact(r#"
            create table ns("platform.os.call") (
                symbol: String(8),
                exchange: String(8),
                last_sale: f64
            )
            os::call("chmod", "777", oxide::home())
        "#, StringValue(String::new()))
    }

    #[test]
    fn test_oxide_eval_qualified() {
        verify_exact("oxide::eval('2 ** 4')", Number(F64Value(16.)));
    }

    #[test]
    fn test_oxide_eval_qualified_negative_case() {
        verify_exact("oxide::eval(123)", ErrorValue(StringExpected("i64".into())))
    }

    #[test]
    fn test_oxide_home_qualified() {
        verify_exact("oxide::home()", StringValue(Machine::oxide_home()));
    }

    #[actix::test]
    async fn test_oxide_serve_qualified_http() {
        let mut interpreter = Interpreter::new();

        // set up a listener on port 8833
        let result = interpreter.evaluate_async(r#"
            oxide::serve(8833)
        "#).await.unwrap();
        assert_eq!(result, Outcome(Ack));

        // create the table
        let result = interpreter.evaluate(r#"
            create table ns("platform.www.stocks") (
                symbol: String(8),
                exchange: String(8),
                last_sale: f64
            )"#).unwrap();
        assert_eq!(result, Outcome(Ack));

        // append a new row
        let row_id = interpreter.evaluate_async(r#"
            POST "http://localhost:8833/platform/www/stocks/0" FROM {
                fields:[
                    { name: "symbol", value: "ABC" },
                    { name: "exchange", value: "AMEX" },
                    { name: "last_sale", value: 11.77 }
                ]
            }"#).await.unwrap();
        assert!(matches!(row_id, Number(I64Value(..))));

        // fetch the previously created row
        let row = interpreter.evaluate_async(format!(r#"
            GET "http://localhost:8833/platform/www/stocks/{row_id}"
        "#).as_str()).await.unwrap();
        assert_eq!(row, StructureSoft(SoftStructure::new(&vec![
            ("id".into(), Number(I64Value(0))),
            ("fields".into(), Array(vec![
                StructureSoft(SoftStructure::new(&vec![
                    ("name", StringValue("symbol".into())),
                    ("value", StringValue("ABC".into())),
                ])),
                StructureSoft(SoftStructure::new(&vec![
                    ("name", StringValue("exchange".into())),
                    ("value", StringValue("AMEX".into())),
                ])),
                StructureSoft(SoftStructure::new(&vec![
                    ("name", StringValue("last_sale".into())),
                    ("value", Number(F64Value(11.77))),
                ])),
            ])),
        ])));

        // replace the previously created row
        let result = interpreter.evaluate_async(r#"
            PUT "http://localhost:8833/platform/www/stocks/:id" FROM {
                fields:[
                    { name: "symbol", value: "ABC" },
                    { name: "exchange", value: "AMEX" },
                    { name: "last_sale", value: 11.79 }
                ]
            }"#
            .replace("/:id", format!("/{}", row_id.unwrap_value()).as_str())
            .as_str()).await.unwrap();
        assert_eq!(result, Number(I64Value(1)));

        // re-fetch the previously updated row
        let row = interpreter.evaluate_async(format!(r#"
            GET "http://localhost:8833/platform/www/stocks/{row_id}"
        "#).as_str()).await.unwrap();
        assert_eq!(row, StructureSoft(SoftStructure::new(&vec![
            ("id".into(), Number(I64Value(0))),
            ("fields".into(), Array(vec![
                StructureSoft(SoftStructure::new(&vec![
                    ("name", StringValue("symbol".into())),
                    ("value", StringValue("ABC".into())),
                ])),
                StructureSoft(SoftStructure::new(&vec![
                    ("name", StringValue("exchange".into())),
                    ("value", StringValue("AMEX".into())),
                ])),
                StructureSoft(SoftStructure::new(&vec![
                    ("name", StringValue("last_sale".into())),
                    ("value", Number(F64Value(11.79))),
                ])),
            ])),
        ])));

        // update the previously created row
        let result = interpreter.evaluate_async(r#"
            PATCH "http://localhost:8833/platform/www/stocks/:id" FROM {
                fields:[
                    { name: "last_sale", value: 11.81 }
                ]
            }"#
            .replace("/:id", format!("/{}", row_id.unwrap_value()).as_str())
            .as_str()).await.unwrap();
        assert_eq!(result, Number(I64Value(1)));

        // re-fetch the previously updated row
        let row = interpreter.evaluate_async(format!(r#"
            GET "http://localhost:8833/platform/www/stocks/{row_id}"
        "#).as_str()).await.unwrap();
        assert_eq!(row, StructureSoft(SoftStructure::new(&vec![
            ("id".into(), Number(I64Value(0))),
            ("fields".into(), Array(vec![
                StructureSoft(SoftStructure::new(&vec![
                    ("name".into(), StringValue("symbol".into())),
                    ("value".into(), StringValue("ABC".into())),
                ])),
                StructureSoft(SoftStructure::new(&vec![
                    ("name".into(), StringValue("exchange".into())),
                    ("value".into(), StringValue("AMEX".into())),
                ])),
                StructureSoft(SoftStructure::new(&vec![
                    ("name".into(), StringValue("last_sale".into())),
                    ("value".into(), Number(F64Value(11.81))),
                ])),
            ])),
        ])));

        // fetch the headers for the previously updated row
        let result = interpreter.evaluate_async(format!(r#"
            HEAD "http://localhost:8833/platform/www/stocks/{row_id}"
        "#).as_str()).await.unwrap();
        println!("HEAD: {}", result.to_string());
        assert!(matches!(result, StructureSoft(..)));

        // delete the previously updated row
        let result = interpreter.evaluate_async(format!(r#"
            DELETE "http://localhost:8833/platform/www/stocks/{row_id}"
        "#).as_str()).await.unwrap();
        assert_eq!(result, Number(I64Value(1)));

        // verify the deleted row is empty
        let row = interpreter.evaluate_async(format!(r#"
            GET "http://localhost:8833/platform/www/stocks/{row_id}"
        "#).as_str()).await.unwrap();
        assert_eq!(row, StructureSoft(SoftStructure::empty()));
    }

    #[test]
    fn test_oxide_version_qualified() {
        use crate::machine::{MAJOR_VERSION, MINOR_VERSION};
        verify_exact(
            "oxide::version()",
            StringValue(format!("{MAJOR_VERSION}.{MINOR_VERSION}")))
    }

    #[test]
    fn test_str_format_qualified() {
        verify_exact(r#"
            str::format("This {} the {}", "is", "way")
        "#, StringValue("This is the way".into()));
    }

    #[test]
    fn test_str_format_scoped() {
        verify_exact(r#"
            import str::format
            format("This {} the {}", "is", "way")
        "#, StringValue("This is the way".into()));
    }

    #[test]
    fn test_str_format_postfix() {
        verify_exact(r#"
            import str::format
            "This {} the {}":::format("is", "way")
        "#, StringValue("This is the way".into()));
    }

    #[test]
    fn test_str_left_postfix() {
        verify_exact(r#"
            import str, util
            'Hello World':::left(5)
        "#, StringValue("Hello".into()));
    }

    #[test]
    fn test_str_left_qualified_inverted() {
        // test valid case 2 (negative)
        verify_exact(r#"
            str::left('Hello World', -5)
        "#, StringValue("World".into()));
    }

    #[test]
    fn test_str_left_postfix_negative_case() {
        // test the invalid case
        verify_exact(r#"
            import str, util
            12345:::left(5)
        "#, Undefined);
    }

    #[test]
    fn test_str_left_scoped_inverted() {
        // attempt a non-import function from the same package
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
            left('Hello World', -5)
        "#);
        assert!(matches!(result, Err(..)));
    }

    #[test]
    fn test_str_right_qualified() {
        let mut interpreter = Interpreter::new();

        // test valid case 1 (positive)
        let result = interpreter.evaluate(r#"
            str::right('Hello World', 5)
        "#).unwrap();
        assert_eq!(result, StringValue("World".into()));
    }

    #[test]
    fn test_str_right_qualified_inverted() {
        let mut interpreter = Interpreter::new();

        // test valid case 2 (negative)
        let result = interpreter.evaluate(r#"
            str::right('Hello World', -5)
        "#).unwrap();
        assert_eq!(result, StringValue("Hello".into()));
    }

    #[test]
    fn test_str_right_qualified_negative_case() {
        let mut interpreter = Interpreter::new();
        // test the invalid case
        let result = interpreter.evaluate(r#"
            str::right(7779311, 5)
        "#).unwrap();
        assert_eq!(result, Undefined)
    }

    #[test]
    fn test_str_right_postfix_inverted() {
        // attempt a non-import function from the same package
        verify_exact(r#"
            import str, util
            'Hello World':::right(-5)
        "#, StringValue("Hello".into()));
    }

    #[test]
    fn test_str_substring_qualified() {
        // test the valid case
        verify_exact(r#"
            str::substring('Hello World', 0, 5)
        "#, StringValue("Hello".into()));
    }

    #[test]
    fn test_str_substring_qualified_negative_case() {
        // test the invalid case
        verify_exact(r#"
            str::substring(8888, 0, 5)
        "#, Undefined)
    }

    #[test]
    fn test_str_to_string_qualified() {
        verify_exact(r#"
            str::to_string(125.75)
        "#, StringValue("125.75".into()));
    }

    #[test]
    fn test_str_to_string_postfix() {
        verify_exact(r#"
            import str::to_string
            123:::to_string()
        "#, StringValue("123".into()));
    }

    #[test]
    fn test_util_describe_qualified() {
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
            util::describe({ symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 })
        "#).unwrap().to_table().unwrap();
        assert_eq!(TableRenderer::from_table(&result), vec![
            "|-----------------------------------------------------|",
            "| name      | type      | default_value | is_nullable |",
            "|-----------------------------------------------------|",
            "| symbol    | String(3) | \"BIZ\"         | true        |",
            "| exchange  | String(4) | \"NYSE\"        | true        |",
            "| last_sale | f64       | 23.66         | true        |",
            "|-----------------------------------------------------|"
        ])
    }

    #[test]
    fn test_util_describe_postfix() {
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
            [+] import util
            [+] stocks := ns("interpreter.struct.stocks")
            [+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
            stocks:::describe()
        "#).unwrap().to_table().unwrap();
        assert_eq!(TableRenderer::from_table(&result), vec![
            "|-----------------------------------------------------|",
            "| name      | type      | default_value | is_nullable |",
            "|-----------------------------------------------------|",
            "| symbol    | String(8) | null          | true        |",
            "| exchange  | String(8) | null          | true        |",
            "| last_sale | f64       | null          | true        |",
            "|-----------------------------------------------------|",
        ]);
    }

    #[test]
    fn test_util_reverse_postfix_ns() {
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
            [+] import util
            [+] stocks := ns("platform.reverse.stocks")
            [+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
            [+] [{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                 { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
            [+] stocks:::reverse()
        "#).unwrap();
        let phys_columns = make_quote_columns();
        assert_eq!(result, TableValue(ModelRowCollection::from_rows(phys_columns.clone(), vec![
            make_quote(0, "JET", "NASDAQ", 32.12),
            make_quote(1, "BOOM", "NYSE", 56.88),
            make_quote(2, "ABC", "AMEX", 12.49),
        ])));
    }

    #[test]
    fn test_util_reverse_postfix_mrc() {
        let phys_columns = make_quote_columns();
        let mut interpreter = Interpreter::new();
        interpreter.with_variable("stocks", TableValue(
            ModelRowCollection::from_rows(phys_columns.clone(), vec![
                make_quote(0, "ABC", "AMEX", 11.88),
                make_quote(1, "UNO", "OTC", 0.2456),
                make_quote(2, "BIZ", "NYSE", 23.66),
                make_quote(3, "GOTO", "OTC", 0.1428),
                make_quote(4, "BOOM", "NASDAQ", 56.87),
            ])));

        let result = interpreter.evaluate(r#"
            import util
            stocks:::reverse()
        "#).unwrap();
        assert_eq!(result, TableValue(ModelRowCollection::from_rows(phys_columns.clone(), vec![
            make_quote(0, "BOOM", "NASDAQ", 56.87),
            make_quote(1, "GOTO", "OTC", 0.1428),
            make_quote(2, "BIZ", "NYSE", 23.66),
            make_quote(3, "UNO", "OTC", 0.2456),
            make_quote(4, "ABC", "AMEX", 11.88),
        ])));
    }

    #[test]
    fn test_util_reverse_postfix_ns_into() {
        verify_exact(r#"
            import util
            [+] stocks := ns("platform.util.reverse")
            [+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
            [+] [{ symbol: "ABC", exchange: "AMEX", last_sale: 12.33 },
                 { symbol: "BIZ", exchange: "NYSE", last_sale: 9.775 },
                 { symbol: "XYZ", exchange: "NASDAQ", last_sale: 89.11 }] ~> stocks
            stocks:::reverse()
        "#, TableValue(ModelRowCollection::from_rows(make_quote_columns(), vec![
            make_quote(0, "XYZ", "NASDAQ", 89.11),
            make_quote(1, "BIZ", "NYSE", 9.775),
            make_quote(2, "ABC", "AMEX", 12.33),
        ])));
    }

    #[test]
    fn test_util_reverse_postfix_to_table_array_of_struct() {
        let mut interpreter = Interpreter::new();
        let rc = interpreter.evaluate(r#"
            import util
            stocks := to_table([
                { symbol: "ABC", exchange: "AMEX", last_sale: 12.33 },
                { symbol: "BIZ", exchange: "NYSE", last_sale: 9.775 },
                { symbol: "XYZ", exchange: "NASDAQ", last_sale: 89.11 }
            ])
            stocks:::reverse()
        "#).unwrap().to_table().unwrap();
        assert_eq!(rc.read_active_rows().unwrap(), vec![
            make_quote(0, "XYZ", "NASDAQ", 89.11),
            make_quote(1, "BIZ", "NYSE", 9.775),
            make_quote(2, "ABC", "AMEX", 12.33),
        ]);
    }

    #[test]
    fn test_util_scan_postfix_ns() {
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
            [+] import util
            [+] stocks := ns("platform.scan.stocks")
            [+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
            [+] [{ symbol: "ABC", exchange: "AMEX", last_sale: 12.33 },
                 { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                 { symbol: "BIZ", exchange: "NYSE", last_sale: 9.775 },
                 { symbol: "GOTO", exchange: "OTC", last_sale: 0.1442 },
                 { symbol: "XYZ", exchange: "NYSE", last_sale: 0.0289 }] ~> stocks
            [+] delete from stocks where last_sale > 1.0
            [+] stocks:::scan()
        "#).unwrap();

        // |-----------------------------------------------|
        // | symbol | exchange | last_sale | _id | _active |
        // |-----------------------------------------------|
        // | ABC    | AMEX     | 12.33     | 0   | false   |
        // | UNO    | OTC      | 0.2456    | 1   | true    |
        // | BIZ    | NYSE     | 9.775     | 2   | false   |
        // | GOTO   | OTC      | 0.1442    | 3   | true    |
        // | XYZ    | NYSE     | 0.0289    | 4   | true    |
        // |-----------------------------------------------|
        let mrc = result.to_table().unwrap();
        let mrc_rows = mrc.read_active_rows().unwrap();
        for s in TableRenderer::from_table(&mrc) { println!("{}", s); }
        assert_eq!(mrc_rows, vec![
            make_scan_quote(0, "ABC", "AMEX", 12.33, false),
            make_scan_quote(1, "UNO", "OTC", 0.2456, true),
            make_scan_quote(2, "BIZ", "NYSE", 9.775, false),
            make_scan_quote(3, "GOTO", "OTC", 0.1442, true),
            make_scan_quote(4, "XYZ", "NYSE", 0.0289, true),
        ])
    }

    #[test]
    fn test_util_to_array_qualified_string() {
        verify_exact(r#"
             util::to_array("Hello")
        "#, Array(vec![
            StringValue("H".into()), StringValue("e".into()),
            StringValue("l".into()), StringValue("l".into()),
            StringValue("o".into())
        ]))
    }

    #[test]
    fn test_util_to_array_postfix_string() {
        verify_exact(r#"
            import util
            "World":::to_array()
        "#, Array(vec![
            StringValue("W".into()), StringValue("o".into()),
            StringValue("r".into()), StringValue("l".into()),
            StringValue("d".into())
        ]))
    }

    #[test]
    fn test_util_to_array_qualified_table() {
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
             util::to_array(util::to_table([
                 { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                 { symbol: "DMX", exchange: "OTC_BB", last_sale: 1.17 }
             ]))
        "#).unwrap();
        let columns = Column::from_parameters(&vec![
            Parameter::new("symbol", Some("String(3)".into()), Some("\"BIZ\"".into())),
            Parameter::new("exchange", Some("String(4)".into()), Some("\"NYSE\"".into())),
            Parameter::new("last_sale", Some("f64".into()), Some("23.66".into())),
        ]).unwrap();
        assert_eq!(result, Array(vec![
            StructureHard(HardStructure::new(columns.clone(), vec![
                StringValue("BIZ".into()), StringValue("NYSE".into()), Number(F64Value(23.66))
            ])),
            StructureHard(HardStructure::new(columns.clone(), vec![
                StringValue("DMX".into()), StringValue("OTC_BB".into()), Number(F64Value(1.17))
            ]))
        ]))
    }

    #[test]
    fn test_util_to_csv_postfix_ns() {
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
            import util::to_csv
            [+] stocks := ns("platform.csv.stocks")
            [+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
            [+] [{ symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
                 { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                 { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                 { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                 { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }] ~> stocks
            stocks:::to_csv()
        "#).unwrap();
        assert_eq!(result, Array(vec![
            StringValue(r#""ABC","AMEX",11.11"#.into()),
            StringValue(r#""UNO","OTC",0.2456"#.into()),
            StringValue(r#""BIZ","NYSE",23.66"#.into()),
            StringValue(r#""GOTO","OTC",0.1428"#.into()),
            StringValue(r#""BOOM","NASDAQ",0.0872"#.into()),
        ]));
    }

    #[test]
    fn test_util_to_json_postfix_ns() {
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
            import util::to_json
            [+] stocks := ns("platform.json.stocks")
            [+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
            [+] [{ symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
                 { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                 { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                 { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                 { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }] ~> stocks
            stocks:::to_json()
        "#).unwrap();
        assert_eq!(result, Array(vec![
            StringValue(r#"{"symbol":"ABC","exchange":"AMEX","last_sale":11.11}"#.into()),
            StringValue(r#"{"symbol":"UNO","exchange":"OTC","last_sale":0.2456}"#.into()),
            StringValue(r#"{"symbol":"BIZ","exchange":"NYSE","last_sale":23.66}"#.into()),
            StringValue(r#"{"symbol":"GOTO","exchange":"OTC","last_sale":0.1428}"#.into()),
            StringValue(r#"{"symbol":"BOOM","exchange":"NASDAQ","last_sale":0.0872}"#.into()),
        ]));
    }

    #[test]
    fn test_util_to_table_qualified_struct() {
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
            util::to_table(struct(
                symbol: String(8) = "ABC",
                exchange: String(8) = "NYSE",
                last_sale: f64 = 45.67
            ))
        "#).unwrap();
        let rows = result.to_table().unwrap()
            .read_active_rows().unwrap();
        assert_eq!(rows, vec![
            make_quote(0, "ABC", "NYSE", 45.67),
        ])
    }

    #[test]
    fn test_util_to_table_qualified_mixed() {
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
             stocks := util::to_table([
                 { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                 { symbol: "DMX", exchange: "OTC_BB", last_sale: 1.17 }
             ])

            util::to_table([
                stocks,
                struct(
                    symbol: String(8) = "ABC",
                    exchange: String(8) = "OTHER_OTC",
                    last_sale: f64 = 0.67),
                { symbol: "TRX", exchange: "AMEX", last_sale: 29.88 },
                { symbol: "BMX", exchange: "NASDAQ", last_sale: 46.11 }
            ])
        "#).unwrap();
        let rows = result.to_table().unwrap()
            .read_active_rows().unwrap();
        assert_eq!(rows, vec![
            make_quote(0, "BIZ", "NYSE", 23.66),
            make_quote(1, "DMX", "OTC_BB", 1.17),
            make_quote(2, "ABC", "OTHER_OTC", 0.67),
            make_quote(3, "TRX", "AMEX", 29.88),
            make_quote(4, "BMX", "NASDAQ", 46.11),
        ])
    }

    #[test]
    fn test_util_to_n_xx_postfix() {
        use crate::numbers::NumberValue::*;
        let mut interpreter = Interpreter::new();
        interpreter.evaluate("import util").unwrap();

        // floating-point kinds
        interpreter = verify_where(interpreter, "1015:::to_f32()", |n| n == Number(F32Value(1015.)));
        interpreter = verify_where(interpreter, "7779311:::to_f64()", |n| n == Number(F64Value(7779311.)));

        // signed-integer kinds
        interpreter = verify_where(interpreter, "12345678987.43:::to_i128()", |n| n == Number(I128Value(12345678987)));
        interpreter = verify_where(interpreter, "123456789.42:::to_i64()", |n| n == Number(I64Value(123456789)));
        interpreter = verify_where(interpreter, "-765.65:::to_i32()", |n| n == Number(I32Value(-765)));
        interpreter = verify_where(interpreter, "567.311:::to_i16()", |n| n == Number(I16Value(567)));
        interpreter = verify_where(interpreter, "-125.089:::to_i8()", |n| n == Number(I8Value(-125)));

        // unsigned-integer kinds
        interpreter = verify_where(interpreter, "12789.43:::to_u128()", |n| n == Number(U128Value(12789)));
        interpreter = verify_where(interpreter, "12.3:::to_u64()", |n| n == Number(U64Value(12)));
        interpreter = verify_where(interpreter, "765.65:::to_u32()", |n| n == Number(U32Value(765)));
        interpreter = verify_where(interpreter, "567.311:::to_u16()", |n| n == Number(U16Value(567)));
        interpreter = verify_where(interpreter, "125.089:::to_u8()", |n| n == Number(U8Value(125)));
    }

    #[test]
    fn test_util_to_u_xx_scoped() {
        let mut interpreter = Interpreter::new();

        // initially 'to_u8' should not be in scope
        assert_eq!(interpreter.machine.get("to_u8"), None);
        assert_eq!(interpreter.machine.get("to_u16"), None);
        assert_eq!(interpreter.machine.get("to_u32"), None);
        assert_eq!(interpreter.machine.get("to_u64"), None);
        assert_eq!(interpreter.machine.get("to_u128"), None);

        // import all conversion members
        interpreter.evaluate("import util").unwrap();

        // after the import, 'to_u8' should be in scope
        assert_eq!(interpreter.machine.get("to_u8"), Some(PlatformFunction(PlatformFunctions::UtilToU8)));
        assert_eq!(interpreter.machine.get("to_u16"), Some(PlatformFunction(PlatformFunctions::UtilToU16)));
        assert_eq!(interpreter.machine.get("to_u32"), Some(PlatformFunction(PlatformFunctions::UtilToU32)));
        assert_eq!(interpreter.machine.get("to_u64"), Some(PlatformFunction(PlatformFunctions::UtilToU64)));
        assert_eq!(interpreter.machine.get("to_u128"), Some(PlatformFunction(PlatformFunctions::UtilToU128)));
    }

    #[test]
    fn test_util_datetime_postfix() {
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
            ts := util::now()
        "#).unwrap();
        assert_eq!(result, Outcome(Ack));
        interpreter.evaluate("import util").unwrap();
        interpreter = verify_where(interpreter, "ts:::day_of()", |n| matches!(n, Number(..)));
        interpreter = verify_where(interpreter, "ts:::hour24()", |n| matches!(n, Number(..)));
        interpreter = verify_where(interpreter, "ts:::hour12()", |n| matches!(n, Number(..)));
        interpreter = verify_where(interpreter, "ts:::minute_of()", |n| matches!(n, Number(..)));
        interpreter = verify_where(interpreter, "ts:::month_of()", |n| matches!(n, Number(..)));
        interpreter = verify_where(interpreter, "ts:::second_of()", |n| matches!(n, Number(..)));
        interpreter = verify_where(interpreter, "ts:::year_of()", |n| matches!(n, Number(..)));
    }

    #[test]
    fn test_util_uuid() {
        verify_when("util::uuid()", |r| matches!(r, Number(U128Value(..))));
    }

    #[test]
    fn test_to_code() {
        for pf in PLATFORM_OPCODES {
            println!("assert_eq!({:?}.to_code(), \"{}\".to_string());",
                     pf, pf.to_code())
        }
        assert_eq!(IoCurrentDir.to_code(), "io::current_dir()".to_string());
        assert_eq!(IoExists.to_code(), "io::exists(a: String())".to_string());
        assert_eq!(IoStdErr.to_code(), "io::stderr(a: String())".to_string());
        assert_eq!(IoStdOut.to_code(), "io::stdout(a: String())".to_string());
        assert_eq!(LangAssert.to_code(), "lang::assert(a: String())".to_string());
        assert_eq!(LangMatches.to_code(), "lang::matches(a: String(), b: String())".to_string());
        assert_eq!(LangPrintln.to_code(), "lang::println(a: String())".to_string());
        assert_eq!(LangTypeOf.to_code(), "lang::type_of(a: String())".to_string());
        assert_eq!(OsEnvVars.to_code(), "os::env()".to_string());
        assert_eq!(OsSysCall.to_code(), "os::call(a: String())".to_string());
        assert_eq!(OxideEval.to_code(), "oxide::eval(a: String())".to_string());
        assert_eq!(OxideHome.to_code(), "oxide::home()".to_string());
        assert_eq!(OxideReset.to_code(), "oxide::reset()".to_string());
        assert_eq!(OxideServe.to_code(), "oxide::serve(a: String())".to_string());
        assert_eq!(OxideVariables.to_code(), "oxide::env()".to_string());
        assert_eq!(OxideVersion.to_code(), "oxide::version()".to_string());
        assert_eq!(StrEndsWith.to_code(), "str::ends_with(a: String(), b: String())".to_string());
        assert_eq!(StrFormat.to_code(), "str::format(a: String(), b: String())".to_string());
        assert_eq!(StrLeft.to_code(), "str::left(a: String(), b: String())".to_string());
        assert_eq!(StrRight.to_code(), "str::right(a: String(), b: String())".to_string());
        assert_eq!(StrStartsWith.to_code(), "str::starts_with(a: String(), b: String())".to_string());
        assert_eq!(StrSubstring.to_code(), "str::substring(a: String(), b: String(), c: String())".to_string());
        assert_eq!(StrToString.to_code(), "str::to_string(a: String())".to_string());
        assert_eq!(UtilDatetimeDay.to_code(), "util::day_of(a: String())".to_string());
        assert_eq!(UtilDatetimeHour12.to_code(), "util::hour12(a: String())".to_string());
        assert_eq!(UtilDatetimeHour24.to_code(), "util::hour24(a: String())".to_string());
        assert_eq!(UtilDatetimeMinute.to_code(), "util::minute_of(a: String())".to_string());
        assert_eq!(UtilDatetimeMonth.to_code(), "util::month_of(a: String())".to_string());
        assert_eq!(UtilDatetimeSecond.to_code(), "util::second_of(a: String())".to_string());
        assert_eq!(UtilDatetimeYear.to_code(), "util::year_of(a: String())".to_string());
        assert_eq!(UtilNow.to_code(), "util::now()".to_string());
        assert_eq!(UtilReverse.to_code(), "util::reverse(a: String())".to_string());
        assert_eq!(UtilScan.to_code(), "util::scan(a: String())".to_string());
        assert_eq!(UtilToArray.to_code(), "util::to_array(a: String())".to_string());
        assert_eq!(UtilToCSV.to_code(), "util::to_csv(a: String())".to_string());
        assert_eq!(UtilToF32.to_code(), "util::to_f32(a: String())".to_string());
        assert_eq!(UtilToF64.to_code(), "util::to_f64(a: String())".to_string());
        assert_eq!(UtilToI8.to_code(), "util::to_i8(a: String())".to_string());
        assert_eq!(UtilToI16.to_code(), "util::to_i16(a: String())".to_string());
        assert_eq!(UtilToI32.to_code(), "util::to_i32(a: String())".to_string());
        assert_eq!(UtilToI64.to_code(), "util::to_i64(a: String())".to_string());
        assert_eq!(UtilToI128.to_code(), "util::to_i128(a: String())".to_string());
        assert_eq!(UtilToJSON.to_code(), "util::to_json(a: String())".to_string());
        assert_eq!(UtilToU8.to_code(), "util::to_u8(a: String())".to_string());
        assert_eq!(UtilToU16.to_code(), "util::to_u16(a: String())".to_string());
        assert_eq!(UtilToU32.to_code(), "util::to_u32(a: String())".to_string());
        assert_eq!(UtilToU64.to_code(), "util::to_u64(a: String())".to_string());
        assert_eq!(UtilToU128.to_code(), "util::to_u128(a: String())".to_string());
        assert_eq!(UtilToTable.to_code(), "util::to_table(a: String())".to_string());
        assert_eq!(UtilUUID.to_code(), "util::uuid()".to_string());
    }
}