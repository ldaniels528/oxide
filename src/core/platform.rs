////////////////////////////////////////////////////////////////////
// Platform class
////////////////////////////////////////////////////////////////////

use crate::codec::Codec;
use crate::compiler::Compiler;
use crate::data_types::DataType::*;
use crate::data_types::StorageTypes::BLOB;
use crate::data_types::{DataType, StorageTypes};
use crate::errors::Errors::*;
use crate::file_row_collection::FileRowCollection;
use crate::machine::Machine;
use crate::model_row_collection::ModelRowCollection;
use crate::namespaces::Namespace;
use crate::number_kind::NumberKind::*;
use crate::numbers::NumberValue::*;
use crate::outcomes::OutcomeKind::Acked;
use crate::outcomes::Outcomes::Ack;
use crate::parameter::Parameter;
use crate::row_collection::RowCollection;
use crate::rows::Row;
use crate::structures::Structure;
use crate::table_columns::Column;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::*;
use chrono::{Datelike, Local, TimeZone, Timelike};
use crossterm::style::Stylize;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::{env, thread};
use tokio::runtime::Runtime;
use uuid::Uuid;
use crate::platform::PlatformFunctions::OsEnv;

pub const MAJOR_VERSION: u8 = 0;
pub const MINOR_VERSION: u8 = 1;

/// Represents Platform Function Information
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct PlatformFunctionInfo {
    pub name: String,
    pub description: String,
    pub package: String,
    pub parameters: Vec<Parameter>,
    pub return_type: DataType,
    pub opcode: PlatformFunctions,
}

/// Represents an enumeration of Oxide Platform Functions
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
    OsCall,
    OsClearScreen,
    OsEnv,
    // oxide package
    OxideEval,
    OxideHelp,
    OxideHome,
    OxideReset,
    OxideServe,
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
    UtilCompact,
    UtilDateDay,
    UtilDateHour12,
    UtilDateHour24,
    UtilDateMinute,
    UtilDateMonth,
    UtilDateSecond,
    UtilDateYear,
    UtilDescribe,
    UtilFetch,
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

pub const PLATFORM_OPCODES: [PlatformFunctions; 54] = {
    use PlatformFunctions::*;
    [
        // io
        IoCurrentDir, IoExists, IoStdErr, IoStdOut,
        // lang
        LangAssert, LangMatches, LangPrintln, LangTypeOf,
        // os
        OsCall, OsClearScreen, OsEnv,
        // oxide
        OxideEval, OxideHome, OxideReset, OxideServe, OxideHelp, OxideVersion,
        // str
        StrEndsWith, StrFormat, StrLeft, StrRight, StrStartsWith, StrSubstring, StrToString,
        // util
        UtilCompact, UtilDateDay, UtilDateHour12, UtilDateHour24, UtilDateMinute,
        UtilDateMonth, UtilDateSecond, UtilDateYear, UtilDescribe, UtilFetch, UtilNow,
        UtilReverse, UtilScan, UtilToArray, UtilToCSV, UtilToF32, UtilToF64,
        UtilToI8, UtilToI16, UtilToI32, UtilToI64, UtilToI128, UtilToJSON,
        UtilToU8, UtilToU16, UtilToU32, UtilToU64, UtilToU128, UtilToTable, UtilUUID,
    ]
};

impl PlatformFunctions {
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
        match self {
            PlatformFunctions::IoCurrentDir => self.adapter_fn0(ms, args, Self::do_io_current_dir),
            PlatformFunctions::IoExists => self.adapter_fn1(ms, args, Self::do_io_exists),
            PlatformFunctions::IoStdErr => self.adapter_fn1(ms, args, Self::do_io_stderr),
            PlatformFunctions::IoStdOut => self.adapter_fn1(ms, args, Self::do_io_stdout),
            PlatformFunctions::LangAssert => self.adapter_fn1(ms, args, Self::do_lang_assert),
            PlatformFunctions::LangMatches => self.adapter_fn2(ms, args, Self::do_lang_matches),
            PlatformFunctions::LangPrintln => self.adapter_fn1(ms, args, Self::do_io_stdout),
            PlatformFunctions::LangTypeOf => self.adapter_fn1(ms, args, Self::do_lang_type_of),
            PlatformFunctions::OsCall => Self::do_os_call(ms, args),
            PlatformFunctions::OsClearScreen => self.adapter_fn0(ms, args, Self::do_os_clear_screen),
            PlatformFunctions::OsEnv => self.adapter_fn0(ms, args, Self::do_os_env),
            PlatformFunctions::OxideEval => self.adapter_fn1(ms, args, Self::do_oxide_eval),
            PlatformFunctions::OxideHelp => self.adapter_fn0(ms, args, Self::do_oxide_help),
            PlatformFunctions::OxideHome => self.adapter_fn0(ms, args, Self::do_oxide_home),
            PlatformFunctions::OxideReset => self.adapter_fn0(ms, args, Self::do_oxide_reset),
            PlatformFunctions::OxideServe => self.adapter_fn1(ms, args, Self::do_oxide_serve),
            PlatformFunctions::OxideVersion => self.adapter_fn0(ms, args, Self::do_oxide_version),
            PlatformFunctions::StrEndsWith => self.adapter_fn2(ms, args, Self::do_str_ends_with),
            PlatformFunctions::StrFormat => Self::do_str_format(ms, args),
            PlatformFunctions::StrLeft => self.adapter_fn2(ms, args, Self::do_str_left),
            PlatformFunctions::StrRight => self.adapter_fn2(ms, args, Self::do_str_right),
            PlatformFunctions::StrStartsWith => self.adapter_fn2(ms, args, Self::do_str_start_with),
            PlatformFunctions::StrSubstring => self.adapter_fn3(ms, args, Self::do_str_substring),
            PlatformFunctions::StrToString => self.adapter_fn1(ms, args, Self::do_str_to_string),
            PlatformFunctions::UtilCompact => self.adapter_fn1(ms, args, Self::do_util_compact),
            PlatformFunctions::UtilDateDay => self.adapter_fn1_pf(ms, args, Self::do_util_date_part),
            PlatformFunctions::UtilDateHour24 => self.adapter_fn1_pf(ms, args, Self::do_util_date_part),
            PlatformFunctions::UtilDateHour12 => self.adapter_fn1_pf(ms, args, Self::do_util_date_part),
            PlatformFunctions::UtilDateMinute => self.adapter_fn1_pf(ms, args, Self::do_util_date_part),
            PlatformFunctions::UtilDateMonth => self.adapter_fn1_pf(ms, args, Self::do_util_date_part),
            PlatformFunctions::UtilDateSecond => self.adapter_fn1_pf(ms, args, Self::do_util_date_part),
            PlatformFunctions::UtilDateYear => self.adapter_fn1_pf(ms, args, Self::do_util_date_part),
            PlatformFunctions::UtilDescribe => self.adapter_fn1(ms, args, Self::do_util_describe),
            PlatformFunctions::UtilFetch => self.adapter_fn2(ms, args, Self::do_util_fetch),
            PlatformFunctions::UtilNow => Self::do_util_now(ms, args),
            PlatformFunctions::UtilReverse => self.adapter_fn1(ms, args, Self::do_util_reverse),
            PlatformFunctions::UtilScan => self.adapter_fn1(ms, args, Self::do_util_scan),
            PlatformFunctions::UtilToArray => self.adapter_fn1(ms, args, Self::do_util_to_array),
            PlatformFunctions::UtilToCSV => self.adapter_fn1(ms, args, Self::do_util_to_csv),
            PlatformFunctions::UtilToF32 => self.adapter_fn1_pf(ms, args, Self::do_util_math_conv),
            PlatformFunctions::UtilToF64 => self.adapter_fn1_pf(ms, args, Self::do_util_math_conv),
            PlatformFunctions::UtilToI8 => self.adapter_fn1_pf(ms, args, Self::do_util_math_conv),
            PlatformFunctions::UtilToI16 => self.adapter_fn1_pf(ms, args, Self::do_util_math_conv),
            PlatformFunctions::UtilToI32 => self.adapter_fn1_pf(ms, args, Self::do_util_math_conv),
            PlatformFunctions::UtilToI64 => self.adapter_fn1_pf(ms, args, Self::do_util_math_conv),
            PlatformFunctions::UtilToI128 => self.adapter_fn1_pf(ms, args, Self::do_util_math_conv),
            PlatformFunctions::UtilToJSON => self.adapter_fn1(ms, args, Self::do_util_to_json),
            PlatformFunctions::UtilToTable => self.adapter_fn1(ms, args, Self::do_util_to_table),
            PlatformFunctions::UtilToU8 => self.adapter_fn1_pf(ms, args, Self::do_util_math_conv),
            PlatformFunctions::UtilToU16 => self.adapter_fn1_pf(ms, args, Self::do_util_math_conv),
            PlatformFunctions::UtilToU32 => self.adapter_fn1_pf(ms, args, Self::do_util_math_conv),
            PlatformFunctions::UtilToU64 => self.adapter_fn1_pf(ms, args, Self::do_util_math_conv),
            PlatformFunctions::UtilToU128 => self.adapter_fn1_pf(ms, args, Self::do_util_math_conv),
            PlatformFunctions::UtilUUID => Self::do_util_uuid(ms, args),
        }
    }

    pub fn find_function(package: &str, name: &str) -> Option<PlatformFunctionInfo> {
        PLATFORM_OPCODES.iter()
            .map(|p| p.get_info())
            .find(|pfi| pfi.package == package && pfi.name == name)
    }

    pub fn get_description(&self) -> String {
        let result = match self {
            PlatformFunctions::IoCurrentDir => "Returns the current directory",
            PlatformFunctions::IoExists => "Returns true if the source path exists",
            PlatformFunctions::IoStdErr => "Writes a string to STDERR",
            PlatformFunctions::IoStdOut => "Writes a string to STDOUT",
            PlatformFunctions::LangAssert => "Evaluates an assertion returning Ack or an error",
            PlatformFunctions::LangMatches => "Compares two values",
            PlatformFunctions::LangPrintln => "Print line function",
            PlatformFunctions::LangTypeOf => "Returns the type of a value",
            PlatformFunctions::OsCall => "Invokes an operating system application",
            PlatformFunctions::OsClearScreen => "Clears the terminal/screen",
            PlatformFunctions::OsEnv => "Returns a table of the OS environment variables",
            PlatformFunctions::OxideEval => "Evaluates a string containing Oxide code",
            PlatformFunctions::OxideHome => "Returns the Oxide home directory path",
            PlatformFunctions::OxideReset => "Clears the scope of all user-defined objects",
            PlatformFunctions::OxideServe => "Starts a local HTTP service",
            PlatformFunctions::OxideHelp => "Integrated help function",
            PlatformFunctions::OxideVersion => "Returns the Oxide version",
            PlatformFunctions::StrEndsWith => "Returns true if string A ends with string B",
            PlatformFunctions::StrFormat => "Returns an argument-formatted string",
            PlatformFunctions::StrLeft => "Returns n-characters from left-to-right",
            PlatformFunctions::StrRight => "Returns n-characters from right-to-left",
            PlatformFunctions::StrStartsWith => "Returns true if string A starts with string B",
            PlatformFunctions::StrSubstring => "Returns a substring of string A from B to C",
            PlatformFunctions::StrToString => "Converts a value to its text-based representation",
            PlatformFunctions::UtilCompact => "Shrinks a table by removing deleted rows",
            PlatformFunctions::UtilDateDay => "Returns the day of the month of a Date",
            PlatformFunctions::UtilDateHour12 => "Returns the hour of the day of a Date",
            PlatformFunctions::UtilDateHour24 => "Returns the hour (military time) of the day of a Date",
            PlatformFunctions::UtilDateMinute => "Returns the minute of the hour of a Date",
            PlatformFunctions::UtilDateMonth => "Returns the month of the year of a Date",
            PlatformFunctions::UtilDateSecond => "Returns the seconds of the minute of a Date",
            PlatformFunctions::UtilDateYear => "Returns the year of a Date",
            PlatformFunctions::UtilDescribe => "Describes a table or structure",
            PlatformFunctions::UtilFetch => "Retrieves a raw structure from a table",
            PlatformFunctions::UtilNow => "Returns the current local date and time",
            PlatformFunctions::UtilReverse => "Returns a reverse copy of a table, string or array",
            PlatformFunctions::UtilScan => "Returns existence metadata for a table",
            PlatformFunctions::UtilToArray => "Converts a collection into an array",
            PlatformFunctions::UtilToCSV => "Converts a collection to CSV format",
            PlatformFunctions::UtilToJSON => "Converts a collection to JSON format",
            PlatformFunctions::UtilToF32 => "Converts a value to f32",
            PlatformFunctions::UtilToF64 => "Converts a value to f64",
            PlatformFunctions::UtilToI8 => "Converts a value to i8",
            PlatformFunctions::UtilToI16 => "Converts a value to i16",
            PlatformFunctions::UtilToI32 => "Converts a value to i32",
            PlatformFunctions::UtilToI64 => "Converts a value to i64",
            PlatformFunctions::UtilToI128 => "Converts a value to i128",
            PlatformFunctions::UtilToTable => "Converts an object into a to_table",
            PlatformFunctions::UtilToU8 => "Converts a value to u8",
            PlatformFunctions::UtilToU16 => "Converts a value to u16",
            PlatformFunctions::UtilToU32 => "Converts a value to u32",
            PlatformFunctions::UtilToU64 => "Converts a value to u64",
            PlatformFunctions::UtilToU128 => "Converts a value to u128",
            PlatformFunctions::UtilUUID => "Returns a random 128-bit UUID",
        };
        result.to_string()
    }

    pub fn get_info(&self) -> PlatformFunctionInfo {
        PlatformFunctionInfo {
            name: self.get_name(),
            description: self.get_description(),
            package: self.get_package_name(),
            parameters: self.get_parameters(),
            return_type: self.get_return_type(),
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
            PlatformFunctions::OsCall => "call",
            PlatformFunctions::OsClearScreen => "clear",
            PlatformFunctions::OsEnv => "env",
            PlatformFunctions::OxideEval => "eval",
            PlatformFunctions::OxideHome => "home",
            PlatformFunctions::OxideReset => "reset",
            PlatformFunctions::OxideServe => "serve",
            PlatformFunctions::OxideHelp => "help",
            PlatformFunctions::OxideVersion => "version",
            PlatformFunctions::StrEndsWith => "ends_with",
            PlatformFunctions::StrFormat => "format",
            PlatformFunctions::StrLeft => "left",
            PlatformFunctions::StrRight => "right",
            PlatformFunctions::StrStartsWith => "starts_with",
            PlatformFunctions::StrSubstring => "substring",
            PlatformFunctions::StrToString => "to_string",
            PlatformFunctions::UtilCompact => "compact",
            PlatformFunctions::UtilDateDay => "day_of",
            PlatformFunctions::UtilDateHour12 => "hour12",
            PlatformFunctions::UtilDateHour24 => "hour24",
            PlatformFunctions::UtilDateMinute => "minute_of",
            PlatformFunctions::UtilDateMonth => "month_of",
            PlatformFunctions::UtilDateSecond => "second_of",
            PlatformFunctions::UtilDateYear => "year_of",
            PlatformFunctions::UtilDescribe => "describe",
            PlatformFunctions::UtilFetch => "fetch",
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
            OsCall | OsClearScreen | OsEnv => "os",
            // oxide
            OxideEval | OxideHome | OxideReset | OxideServe |
            OxideHelp | OxideVersion => "oxide",
            // str
            StrEndsWith | StrFormat | StrLeft | StrRight |
            StrStartsWith | StrSubstring | StrToString => "str",
            // util
            UtilCompact | UtilDateDay | UtilDateHour12 | UtilDateHour24 |
            UtilDateMinute | UtilDateMonth | UtilDateSecond | UtilDateYear |
            UtilDescribe | UtilFetch | UtilNow | UtilReverse | UtilScan |
            UtilToArray | UtilToCSV | UtilToJSON | UtilToF32 | UtilToF64 |
            UtilToI8 | UtilToI16 | UtilToI32 | UtilToI64 | UtilToI128 |
            UtilToTable | UtilToU8 | UtilToU16 | UtilToU32 | UtilToU64 |
            UtilToU128 | UtilUUID => "util"
        };
        result.to_string()
    }

    pub fn get_parameter_types(&self) -> Vec<DataType> {
        use PlatformFunctions::*;
        match self {
            // zero-parameter
            IoCurrentDir | OsEnv | OxideHome | OxideReset |
            OsClearScreen | OxideHelp | OxideVersion | UtilNow | UtilUUID => Vec::new(),
            // single-parameter (boolean)
            LangAssert => vec![BooleanType],
            // single-parameter (date)
            UtilDateDay | UtilDateHour12 | UtilDateHour24 | UtilDateMinute | UtilDateMonth |
            UtilDateSecond | UtilDateYear => vec![DateType],
            // single-parameter (lazy-eval)
            LangTypeOf | OxideServe | StrToString | UtilCompact | UtilDescribe | UtilFetch | UtilScan | UtilToArray |
            UtilToCSV | UtilToF32 | UtilToF64 | UtilToI8 | UtilToI16 | UtilToI32 | UtilToI64 | UtilToI128 |
            UtilToJSON | UtilToTable | UtilToU8 | UtilToU16 | UtilToU32 | UtilToU64 | UtilToU128 |
            UtilReverse => vec![
                LazyEvalType
            ],
            // single-parameter (string)
            IoExists | IoStdErr | IoStdOut | LangPrintln | OsCall | OxideEval => vec![
                StringType(StorageTypes::BLOB)
            ],
            // two-parameter (lazy-eval, lazy-eval)
            LangMatches => vec![
                LazyEvalType, LazyEvalType
            ],
            // two-parameter (string, string)
            StrEndsWith | StrFormat | StrLeft | StrRight | StrStartsWith => vec![
                StringType(StorageTypes::BLOB), StringType(StorageTypes::BLOB)
            ],
            // three-parameter (string, string, string)
            StrSubstring => vec![
                StringType(StorageTypes::BLOB), StringType(StorageTypes::BLOB), StringType(StorageTypes::BLOB)
            ],
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

    pub fn get_return_type(&self) -> DataType {
        use PlatformFunctions::*;
        match self {
            // array
            UtilToArray => ArrayType(Box::from(LazyEvalType)),
            UtilToCSV | UtilToJSON => ArrayType(Box::from(StringType(BLOB))),
            // boolean
            IoExists | LangMatches | StrEndsWith | StrStartsWith => BooleanType,
            // date
            UtilNow => DateType,
            // number
            UtilDateDay | UtilDateHour12 | UtilDateHour24 | UtilDateMinute |
            UtilDateMonth | UtilDateSecond => NumberType(U32Kind),
            UtilToF32 => NumberType(F32Kind),
            UtilToF64 => NumberType(F64Kind),
            UtilToI8 => NumberType(I8Kind),
            UtilToI16 => NumberType(I16Kind),
            UtilToI32 | UtilDateYear => NumberType(I32Kind),
            UtilToI64 => NumberType(I64Kind),
            UtilToI128 => NumberType(I128Kind),
            UtilToU8 => NumberType(U8Kind),
            UtilToU16 => NumberType(U16Kind),
            UtilToU32 => NumberType(U32Kind),
            UtilToU64 => NumberType(U64Kind),
            UtilToU128 | UtilUUID => NumberType(U128Kind),
            // outcome
            LangAssert | LangPrintln | OsClearScreen | OxideReset | OxideServe => OutcomeType(Acked),
            // string
            IoCurrentDir | IoStdErr | IoStdOut | LangTypeOf | OsCall | OxideEval | OxideHome | OxideVersion |
            StrFormat | StrLeft | StrRight | StrSubstring | StrToString => StringType(BLOB),
            // table
            OsEnv => TableType(Self::get_os_env_parameters(), BLOB),
            OxideHelp => TableType(Self::get_oxide_help_parameters(), BLOB),
            UtilCompact | UtilFetch | UtilReverse | UtilScan |
            UtilToTable => TableType(Vec::new(), BLOB),
            UtilDescribe => TableType(Self::get_util_describe_parameters(), BLOB),
        }
    }

    pub fn get_type(&self) -> DataType {
        PlatformFunctionType(self.clone())
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

    ////////////////////////////////////////////////////////////////////
    //      Utility Functions
    ////////////////////////////////////////////////////////////////////

    fn do_io_current_dir(ms: Machine) -> (Machine, TypedValue) {
        match env::current_dir() {
            Ok(dir) => (ms, StringValue(dir.display().to_string())),
            Err(err) => (ms, ErrorValue(Exact(err.to_string())))
        }
    }

    fn do_io_exists(ms: Machine, path_value: &TypedValue) -> (Machine, TypedValue) {
        match path_value {
            StringValue(path) => (ms, Boolean(Path::new(path).exists())),
            other => (ms, ErrorValue(StringExpected(other.to_string())))
        }
    }

    fn do_io_stdout(ms: Machine, value: &TypedValue) -> (Machine, TypedValue) {
        println!("{}", value.unwrap_value());
        (ms, Outcome(Ack))
    }

    fn do_io_stderr(ms: Machine, value: &TypedValue) -> (Machine, TypedValue) {
        eprintln!("{}", value.unwrap_value());
        (ms, Outcome(Ack))
    }

    fn do_lang_assert(ms: Machine, value: &TypedValue) -> (Machine, TypedValue) {
        match value {
            ErrorValue(msg) => (ms, ErrorValue(msg.to_owned())),
            Boolean(false) => (ms, ErrorValue(AssertionError("true".to_string(), "false".to_string()))),
            z => (ms, z.to_owned())
        }
    }

    fn do_lang_matches(ms: Machine, a: &TypedValue, b: &TypedValue) -> (Machine, TypedValue) {
        (ms, a.matches(b))
    }

    fn do_lang_type_of(ms: Machine, a: &TypedValue) -> (Machine, TypedValue) {
        (ms, StringValue(a.get_type_name()))
    }

    fn do_os_call(ms: Machine, args: Vec<TypedValue>) -> (Machine, TypedValue) {
        fn split_first<T>(vec: Vec<T>) -> Option<(T, Vec<T>)> {
            let mut iter = vec.into_iter();
            iter.next().map(|first| (first, iter.collect()))
        }

        let items: Vec<_> = args.iter().map(|i| i.unwrap_value()).collect();
        if let Some((command, cmd_args)) = split_first(items) {
            match std::process::Command::new(command).args(cmd_args).output() {
                Ok(output) =>
                    if output.status.success() {
                        let raw_text = String::from_utf8_lossy(&output.stdout);
                        (ms, StringValue(raw_text.to_string()))
                    } else {
                        let message = String::from_utf8_lossy(&output.stderr);
                        (ms, ErrorValue(Exact(message.to_string())))
                    }
                Err(err) => (ms, ErrorValue(Exact(err.to_string())))
            }
        } else {
            (ms, ErrorValue(CollectionExpected(args.iter()
                .map(|e| e.to_code())
                .collect::<Vec<_>>()
                .join(", "))))
        }
    }

    fn do_os_clear_screen(ms: Machine) -> (Machine, TypedValue) {
        print!("\x1B[2J\x1B[H");
        match std::io::stdout().flush() {
            Ok(_) => (ms, Outcome(Ack)),
            Err(err) => (ms, ErrorValue(Exact(err.to_string())))
        }
    }

    fn do_os_env(ms: Machine) -> (Machine, TypedValue) {
        use std::env;
        let mut mrc = ModelRowCollection::construct(&vec![
            Parameter::new("key", Some("String(256)".into()), None),
            Parameter::new("value", Some("String(8192)".into()), None)
        ]);
        for (key, value) in env::vars() {
            if let ErrorValue(err) = mrc.append_row(Row::new(0, vec![
                StringValue(key), StringValue(value)
            ])) {
                return (ms, ErrorValue(err));
            }
        }

        (ms, TableValue(mrc))
    }

    /// returns a table describing all modules
    fn do_oxide_help(ms: Machine) -> (Machine, TypedValue) {
        let mut mrc = ModelRowCollection::construct(&Self::get_oxide_help_parameters());
        for (module_name, module) in ms.get_variables().iter() {
            match module {
                StructureHard(mod_struct) =>
                    for (name, func) in mod_struct.get_tuples() {
                        mrc.append_row(Row::new(0, vec![
                            // name
                            StringValue(name),
                            // module
                            StringValue(module_name.to_string()),
                            // signature
                            StringValue(func.to_code()),
                            // description
                            match func {
                                PlatformFunction(pf) => StringValue(pf.get_description()),
                                _ => Null,
                            },
                            // returns
                            match func {
                                PlatformFunction(pf) => StringValue(pf.get_return_type().to_type_declaration().unwrap_or("".into())),
                                _ => Null,
                            }
                        ]));
                    }
                _ => {}
            }
        }
        (ms, TableValue(mrc))
    }

    fn do_oxide_version(ms: Machine) -> (Machine, TypedValue) {
        (ms, StringValue(format!("{MAJOR_VERSION}.{MINOR_VERSION}")))
    }

    fn do_oxide_eval(ms: Machine, query_value: &TypedValue) -> (Machine, TypedValue) {
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

    fn do_oxide_home(ms: Machine) -> (Machine, TypedValue) {
        (ms, StringValue(Machine::oxide_home()))
    }

    fn do_oxide_reset(_ms: Machine) -> (Machine, TypedValue) {
        (Machine::new_platform(), Outcome(Ack))
    }

    fn do_oxide_serve(
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

    fn do_str_ends_with(
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

    /// Formats a string based on a template
    /// Ex: format("This {} the {}", "is", "way") => "This is the way"
    fn do_str_format(
        ms: Machine,
        args: Vec<TypedValue>,
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
        if args.is_empty() { (ms, StringValue("".to_string())) } else {
            match (args[0].to_owned(), args[1..].to_owned()) {
                (StringValue(format_str), format_args) =>
                    format_text(ms, format_str, format_args),
                (other, ..) =>
                    (ms, ErrorValue(StringExpected(other.to_code())))
            }
        }
    }

    fn do_str_left(
        ms: Machine,
        string: &TypedValue,
        n_chars: &TypedValue,
    ) -> (Machine, TypedValue) {
        let result = match string {
            StringValue(s) =>
                match n_chars {
                    Number(nv) if nv.to_i64() < 0 =>
                        Self::do_str_right(ms.to_owned(), string, &Number(I64Value(-nv.to_i64()))).1,
                    Number(nv) =>
                        StringValue(s[0..nv.to_usize()].to_string()),
                    _ => Undefined
                },
            _ => Undefined
        };
        (ms, result)
    }

    fn do_str_right(
        ms: Machine,
        string: &TypedValue,
        n_chars: &TypedValue,
    ) -> (Machine, TypedValue) {
        let result = match string {
            StringValue(s) =>
                match n_chars {
                    Number(nv) if nv.to_i64() < 0 =>
                        Self::do_str_left(ms.to_owned(), string, &Number(I64Value(-nv.to_i64()))).1,
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

    fn do_str_start_with(
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

    fn do_str_substring(
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

    fn do_str_to_string(
        ms: Machine,
        a: &TypedValue,
    ) -> (Machine, TypedValue) {
        (ms, StringValue(a.to_string()))
    }

    fn do_util_compact(ms: Machine, table: &TypedValue) -> (Machine, TypedValue) {
        match table {
            ErrorValue(err) => (ms, ErrorValue(err.to_owned())),
            NamespaceValue(d, s, n) => {
                match FileRowCollection::open(&Namespace::new(d, s, n)) {
                    Ok(mut frc) => (ms, frc.compact()),
                    Err(err) => (ms, ErrorValue(Exact(err.to_string())))
                }
            }
            TableValue(mrc) => (ms, mrc.to_owned().compact()),
            z => (ms, ErrorValue(CollectionExpected(z.to_code())))
        }
    }

    fn do_util_date_part(
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
                    PlatformFunctions::UtilDateDay => (ms, Number(U32Value(datetime.day()))),
                    PlatformFunctions::UtilDateHour12 => (ms, Number(U32Value(datetime.hour12().1))),
                    PlatformFunctions::UtilDateHour24 => (ms, Number(U32Value(datetime.hour()))),
                    PlatformFunctions::UtilDateMinute => (ms, Number(U32Value(datetime.minute()))),
                    PlatformFunctions::UtilDateMonth => (ms, Number(U32Value(datetime.second()))),
                    PlatformFunctions::UtilDateSecond => (ms, Number(U32Value(datetime.second()))),
                    PlatformFunctions::UtilDateYear => (ms, Number(I32Value(datetime.year()))),
                    pf => (ms, ErrorValue(ConversionError(pf.to_code())))
                }
            }
            other => (ms, ErrorValue(DateExpected(other.to_code())))
        }
    }

    fn do_util_describe(
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

    /// Retrieves a raw structure from a table
    /// ex: util::fetch(stocks, 5)
    /// ex: stocks:::fetch(5)
    fn do_util_fetch(
        ms: Machine,
        table: &TypedValue,
        row_offset: &TypedValue,
    ) -> (Machine, TypedValue) {
        let offset = row_offset.to_usize();
        match table {
            NamespaceValue(d, s, n) =>
                match FileRowCollection::open(&Namespace::new(d, s, n)) {
                    Ok(frc) => {
                        let columns = frc.get_columns();
                        match frc.read_row(offset) {
                            Ok((row, _)) =>
                                (ms, TableValue(ModelRowCollection::from_rows(columns.clone(), vec![row]))),
                            Err(err) => (ms, ErrorValue(Exact(err.to_string())))
                        }
                    }
                    Err(err) => (ms, ErrorValue(Exact(err.to_string())))
                }
            TableValue(mrc) => {
                let columns = mrc.get_columns();
                match mrc.read_row(offset) {
                    Ok((row, _)) =>
                        (ms, TableValue(ModelRowCollection::from_rows(columns.clone(), vec![row]))),
                    Err(err) => (ms, ErrorValue(Exact(err.to_string())))
                }
            }
            other =>
                (ms, ErrorValue(TableExpected("table or struct".to_string(), other.to_code())))
        }
    }

    fn do_util_math_conv(
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

    fn do_util_now(ms: Machine, args: Vec<TypedValue>) -> (Machine, TypedValue) {
        if !args.is_empty() {
            return (ms, ErrorValue(Exact(format!("No arguments expected, but found {}", args.len()))));
        }
        (ms, DateValue(Local::now().timestamp_millis()))
    }

    fn do_util_reverse(ms: Machine, value: &TypedValue) -> (Machine, TypedValue) {
        match value {
            Array(a) => (ms, Array(a.iter().rev()
                .map(|v| v.to_owned())
                .collect::<Vec<_>>())),
            NamespaceValue(a, b, c) =>
                (ms.clone(), match Self::open_namespace(&Namespace::new(a, b, c)) {
                    TableValue(mrc) => mrc.reverse_table_value(),
                    other => ErrorValue(TypeMismatch("Table".to_string(), other.to_code()))
                }),
            StringValue(s) => (ms, StringValue(s.to_owned().reverse().to_string())),
            TableValue(mrc) => (ms, mrc.reverse_table_value()),
            other => (ms, ErrorValue(TypeMismatch("Array, String or Table".into(), other.to_string())))
        }
    }

    fn do_util_scan(machine: Machine, tv_table: &TypedValue) -> (Machine, TypedValue) {
        Self::orchestrate_rc(machine, tv_table.to_owned(), |machine, rc| {
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

    fn do_util_to_array(ms: Machine, value: &TypedValue) -> (Machine, TypedValue) {
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

    fn do_util_to_csv(ms: Machine, value: &TypedValue) -> (Machine, TypedValue) {
        Self::do_util_to_csv_or_json(ms, value, true)
    }

    fn do_util_to_csv_or_json(
        ms: Machine,
        value: &TypedValue,
        is_csv: bool,
    ) -> (Machine, TypedValue) {
        fn convert_to_csv(
            ms: Machine,
            rc: Box<dyn RowCollection>,
        ) -> (Machine, TypedValue) {
            (ms, Array(rc.iter().map(|row| row.to_csv())
                .map(StringValue).collect()))
        }

        fn convert_to_json(
            ms: Machine,
            rc: Box<dyn RowCollection>,
        ) -> (Machine, TypedValue) {
            (ms, Array(rc.iter().map(|row| row.to_json(rc.get_columns()))
                .map(StringValue).collect()))
        }

        match value {
            NamespaceValue(d, s, n) =>
                match FileRowCollection::open(&Namespace::new(d, s, n)) {
                    Ok(frc) => {
                        let rc = Box::new(frc);
                        if is_csv { convert_to_csv(ms, rc) } else { convert_to_json(ms, rc) }
                    }
                    Err(err) => (ms.to_owned(), ErrorValue(Exact(err.to_string())))
                }
            TableValue(mrc) => {
                let rc = Box::new(mrc.to_owned());
                if is_csv { convert_to_csv(ms, rc) } else { convert_to_json(ms, rc) }
            }
            _ => (ms.to_owned(), ErrorValue(Exact(format!("Cannot convert to {}", if is_csv { "CSV" } else { "JSON" }))))
        }
    }

    fn do_util_to_json(ms: Machine, value: &TypedValue) -> (Machine, TypedValue) {
        Self::do_util_to_csv_or_json(ms, value, false)
    }

    fn do_util_to_table(ms: Machine, value: &TypedValue) -> (Machine, TypedValue) {
        (ms, value.to_table_value())
    }

    fn do_util_uuid(
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> (Machine, TypedValue) {
        if !args.is_empty() {
            return (ms, ErrorValue(Exact(format!("No arguments expected, but found {}", args.len()))));
        }
        (ms, Number(U128Value(Uuid::new_v4().as_u128())))
    }

    fn adapter_fn0(
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

    fn adapter_fn1(
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

    fn adapter_fn1_pf(
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

    fn adapter_fn2(
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

    fn adapter_fn3(
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

    pub fn get_os_env_parameters() -> Vec<Parameter> {
        vec![
            Parameter::new("key", Some("String(256)".into()), None),
            Parameter::new("value", Some("String(8192)".into()), None)
        ]
    }

    pub fn get_oxide_help_parameters() -> Vec<Parameter> {
        vec![
            Parameter::new("name", Some("String(20)".into()), None),
            Parameter::new("module", Some("String(20)".into()), None),
            Parameter::new("signature", Some("String(32)".into()), None),
            Parameter::new("description", Some("String(60)".into()), None),
            Parameter::new("returns", Some("String(32)".into()), None),
        ]
    }

    pub fn get_util_describe_parameters() -> Vec<Parameter> {
        vec![
            Parameter::new("name", Some("String(128)".into()), None),
            Parameter::new("type", Some("String(128)".into()), None),
            Parameter::new("default_value", Some("String(128)".into()), None),
            Parameter::new("is_nullable", Some("Boolean".into()), None),
        ]
    }

    fn open_namespace(ns: &Namespace) -> TypedValue {
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

    pub fn orchestrate_rc(
        ms: Machine,
        table: TypedValue,
        f: fn(Machine, Box<dyn RowCollection>) -> (Machine, TypedValue),
    ) -> (Machine, TypedValue) {
        use TypedValue::*;
        match table {
            ErrorValue(msg) => (ms, ErrorValue(msg)),
            NamespaceValue(d, s, n) => {
                let ns = Namespace::new(d, s, n);
                match FileRowCollection::open(&ns) {
                    Ok(frc) => f(ms, Box::new(frc)),
                    Err(err) => (ms, ErrorValue(Exact(err.to_string())))
                }
            }
            TableValue(mrc) => f(ms, Box::new(mrc)),
            z => (ms, ErrorValue(CollectionExpected(z.to_code())))
        }
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
    use crate::testdata::*;
    use crate::typed_values::TypedValue::*;
    use PlatformFunctions::*;

    #[test]
    fn test_create_env_table() {
        match PlatformFunctions::do_os_env(Machine::new_platform()) {
            (_, TableValue(mrc)) => {
                let rc: Box<dyn RowCollection> = Box::from(mrc);
                for s in TableRenderer::from_table(&rc) {
                    println!("{}", s)
                }
            }
            (_, rc) => assert!(matches!(rc, TableValue(..)))
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
    fn test_get_info_lang_matches() {
        assert_eq!(LangMatches.get_info(), PlatformFunctionInfo {
            name: "matches".into(),
            description: "Compares two values".into(),
            package: "lang".into(),
            parameters: vec![
                Parameter::new("a", None, None),
                Parameter::new("b", None, None),
            ],
            return_type: BooleanType,
            opcode: LangMatches,
        });
    }

    #[test]
    fn test_get_info_str_left() {
        assert_eq!(StrLeft.get_info(), PlatformFunctionInfo {
            name: "left".into(),
            description: "Returns n-characters from left-to-right".into(),
            package: "str".into(),
            parameters: vec![
                Parameter::new("a", Some("String()".into()), None),
                Parameter::new("b", Some("String()".into()), None),
            ],
            return_type: StringType(BLOB),
            opcode: StrLeft,
        });
    }

    #[test]
    fn test_get_info_str_substring() {
        assert_eq!(StrSubstring.get_info(), PlatformFunctionInfo {
            name: "substring".into(),
            description: "Returns a substring of string A from B to C".into(),
            package: "str".into(),
            parameters: vec![
                Parameter::new("a", Some("String()".into()), None),
                Parameter::new("b", Some("String()".into()), None),
                Parameter::new("c", Some("String()".into()), None)
            ],
            return_type: StringType(BLOB),
            opcode: StrSubstring,
        });
    }

    #[test]
    fn test_get_info_util_now() {
        assert_eq!(UtilNow.get_info(), PlatformFunctionInfo {
            name: "now".into(),
            description: "Returns the current local date and time".into(),
            package: "util".into(),
            parameters: Vec::new(),
            return_type: DateType,
            opcode: UtilNow,
        });
    }

    #[test]
    fn test_io_current_dir() {
        let phys_columns = make_quote_columns();
        verify_table_exact_with_ids(r#"
            import io, str
            cur_dir := io::current_dir()
            prefix := iff(cur_dir:::ends_with("core"), "../..", ".")
            path_str := prefix + "/demoes/language/include_file.oxide"
            include path_str
        "#, vec![
            "|------------------------------------|",
            "| id | symbol | exchange | last_sale |",
            "|------------------------------------|",
            "| 0  | ABC    | AMEX     | 12.49     |",
            "| 1  | BOOM   | NYSE     | 56.88     |",
            "| 2  | JET    | NASDAQ   | 32.12     |",
            "|------------------------------------|"
        ]);

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
        "#).unwrap();
        assert!(matches!(result, ErrorValue(..)));
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
    fn test_util_compact_postfix() {
        let phys_columns = make_quote_columns();
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
            [+] stocks := ns("platform.compact.stocks")
            [+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
            [+] [{ symbol: "DMX", exchange: "NYSE", last_sale: 99.99 },
                 { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                 { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                 { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                 { symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
                 { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 },
                 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
            [+] delete from stocks where last_sale > 1.0
            [+] from stocks
        "#).unwrap();

        let rc0 = result.to_table().unwrap();
        assert_eq!(TableRenderer::from_table_with_ids(&rc0).unwrap(), vec![
            "|------------------------------------|",
            "| id | symbol | exchange | last_sale |",
            "|------------------------------------|",
            "| 1  | UNO    | OTC      | 0.2456    |",
            "| 3  | GOTO   | OTC      | 0.1428    |",
            "| 5  | BOOM   | NASDAQ   | 0.0872    |",
            "|------------------------------------|"
        ]);

        let result = interpreter.evaluate(r#"
            [+] import util
            [+] stocks:::compact()
            [+] from stocks
        "#).unwrap();
        let rc1 = result.to_table().unwrap();
        assert_eq!(TableRenderer::from_table_with_ids(&rc1).unwrap(), vec![
            "|------------------------------------|",
            "| id | symbol | exchange | last_sale |",
            "|------------------------------------|",
            "| 0  | BOOM   | NASDAQ   | 0.0872    |",
            "| 1  | UNO    | OTC      | 0.2456    |",
            "| 2  | GOTO   | OTC      | 0.1428    |",
            "|------------------------------------|"
        ]);
    }

    #[test]
    fn test_util_date_postfix() {
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
            ts := util::now()
        "#).unwrap();
        assert_eq!(result, Outcome(Ack));
        interpreter.evaluate("import util").unwrap();
        interpreter = verify_where(interpreter, "ts:::day_of()", |n| matches!(n, Number(U32Value(..))));
        interpreter = verify_where(interpreter, "ts:::hour24()", |n| matches!(n, Number(U32Value(..))));
        interpreter = verify_where(interpreter, "ts:::hour12()", |n| matches!(n, Number(U32Value(..))));
        interpreter = verify_where(interpreter, "ts:::minute_of()", |n| matches!(n, Number(U32Value(..))));
        interpreter = verify_where(interpreter, "ts:::month_of()", |n| matches!(n, Number(U32Value(..))));
        interpreter = verify_where(interpreter, "ts:::second_of()", |n| matches!(n, Number(U32Value(..))));
        interpreter = verify_where(interpreter, "ts:::year_of()", |n| matches!(n, Number(I32Value(..))));
    }

    #[test]
    fn test_util_describe_qualified() {
        verify_table_exact_with_ids(r#"
            util::describe({ symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 })
        "#, vec![
            "|----------------------------------------------------------|",
            "| id | name      | type      | default_value | is_nullable |",
            "|----------------------------------------------------------|",
            "| 0  | symbol    | String(3) | \"BIZ\"         | true        |",
            "| 1  | exchange  | String(4) | \"NYSE\"        | true        |",
            "| 2  | last_sale | f64       | 23.66         | true        |",
            "|----------------------------------------------------------|"
        ])
    }

    #[test]
    fn test_util_describe_postfix() {
        verify_table_exact_with_ids(r#"
            [+] import util
            [+] stocks := ns("platform.describe.stocks")
            [+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
            stocks:::describe()
        "#, vec![
            "|----------------------------------------------------------|",
            "| id | name      | type      | default_value | is_nullable |",
            "|----------------------------------------------------------|",
            "| 0  | symbol    | String(8) | null          | true        |",
            "| 1  | exchange  | String(8) | null          | true        |",
            "| 2  | last_sale | f64       | null          | true        |",
            "|----------------------------------------------------------|"
        ]);
    }

    #[test]
    fn test_util_fetch_postfix_ns() {
        verify_table_exact_with_ids(r#"
            [+] import util
            [+] stocks := ns("platform.fetch.stocks_p")
            [+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
            [+] [{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                 { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
            [+] stocks:::fetch(1)
        "#, vec![
            "|------------------------------------|",
            "| id | symbol | exchange | last_sale |",
            "|------------------------------------|",
            "| 1  | BOOM   | NYSE     | 56.88     |",
            "|------------------------------------|"
        ]);
    }

    #[test]
    fn test_util_fetch_qualified_ns() {
        verify_table_exact_with_ids(r#"
            [+] stocks := ns("platform.fetch.stocks_q")
            [+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
            [+] [{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                 { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
            [+] util::fetch(stocks, 2)
        "#, vec![
            "|------------------------------------|",
            "| id | symbol | exchange | last_sale |",
            "|------------------------------------|",
            "| 2  | JET    | NASDAQ   | 32.12     |",
            "|------------------------------------|"
        ]);
    }

    #[test]
    fn test_util_reverse_postfix_ns() {
        verify_table_exact_with_ids(r#"
            [+] import util
            [+] stocks := ns("platform.reverse.stocks")
            [+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
            [+] [{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                 { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
            [+] stocks:::reverse()
        "#, vec![
            "|------------------------------------|",
            "| id | symbol | exchange | last_sale |",
            "|------------------------------------|",
            "| 0  | JET    | NASDAQ   | 32.12     |",
            "| 1  | BOOM   | NYSE     | 56.88     |",
            "| 2  | ABC    | AMEX     | 12.49     |",
            "|------------------------------------|"
        ]);
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
        "#).unwrap().to_table().unwrap();
        assert_eq!(TableRenderer::from_table_with_ids(&result).unwrap(), vec![
            "|------------------------------------|",
            "| id | symbol | exchange | last_sale |",
            "|------------------------------------|",
            "| 0  | BOOM   | NASDAQ   | 56.87     |",
            "| 1  | GOTO   | OTC      | 0.1428    |",
            "| 2  | BIZ    | NYSE     | 23.66     |",
            "| 3  | UNO    | OTC      | 0.2456    |",
            "| 4  | ABC    | AMEX     | 11.88     |",
            "|------------------------------------|"
        ]);
    }

    #[test]
    fn test_util_reverse_postfix_ns_into() {
        verify_table_exact_with_ids(r#"
            import util
            [+] stocks := ns("platform.util.reverse")
            [+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
            [+] [{ symbol: "ABC", exchange: "AMEX", last_sale: 12.33 },
                 { symbol: "BIZ", exchange: "NYSE", last_sale: 9.775 },
                 { symbol: "XYZ", exchange: "NASDAQ", last_sale: 89.11 }] ~> stocks
            stocks:::reverse()
        "#, vec![
            "|------------------------------------|",
            "| id | symbol | exchange | last_sale |",
            "|------------------------------------|",
            "| 0  | XYZ    | NASDAQ   | 89.11     |",
            "| 1  | BIZ    | NYSE     | 9.775     |",
            "| 2  | ABC    | AMEX     | 12.33     |",
            "|------------------------------------|"
        ]);
    }

    #[test]
    fn test_util_reverse_postfix_to_table_array_of_struct() {
        verify_table_exact_with_ids(r#"
            import util
            stocks := to_table([
                { symbol: "ABC", exchange: "AMEX", last_sale: 12.33 },
                { symbol: "BIZ", exchange: "NYSE", last_sale: 9.775 },
                { symbol: "XYZ", exchange: "NASDAQ", last_sale: 89.11 }
            ])
            stocks:::reverse()
        "#, vec![
            "|------------------------------------|",
            "| id | symbol | exchange | last_sale |",
            "|------------------------------------|",
            "| 0  | XYZ    | NASDAQ   | 89.11     |",
            "| 1  | BIZ    | NYSE     | 9.775     |",
            "| 2  | ABC    | AMEX     | 12.33     |",
            "|------------------------------------|"
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
        assert_eq!(result.to_table().unwrap().read_active_rows().unwrap(), vec![
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
        verify_exact(r#"
            import util::to_csv
            [+] stocks := ns("platform.csv.stocks")
            [+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
            [+] [{ symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
                 { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                 { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                 { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                 { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }] ~> stocks
            stocks:::to_csv()
        "#, Array(vec![
            StringValue(r#""ABC","AMEX",11.11"#.into()),
            StringValue(r#""UNO","OTC",0.2456"#.into()),
            StringValue(r#""BIZ","NYSE",23.66"#.into()),
            StringValue(r#""GOTO","OTC",0.1428"#.into()),
            StringValue(r#""BOOM","NASDAQ",0.0872"#.into()),
        ]));
    }

    #[test]
    fn test_util_to_json_postfix_ns() {
        verify_exact(r#"
            import util::to_json
            [+] stocks := ns("platform.json.stocks")
            [+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
            [+] [{ symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
                 { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                 { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                 { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                 { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }] ~> stocks
            stocks:::to_json()
        "#, Array(vec![
            StringValue(r#"{"symbol":"ABC","exchange":"AMEX","last_sale":11.11}"#.into()),
            StringValue(r#"{"symbol":"UNO","exchange":"OTC","last_sale":0.2456}"#.into()),
            StringValue(r#"{"symbol":"BIZ","exchange":"NYSE","last_sale":23.66}"#.into()),
            StringValue(r#"{"symbol":"GOTO","exchange":"OTC","last_sale":0.1428}"#.into()),
            StringValue(r#"{"symbol":"BOOM","exchange":"NASDAQ","last_sale":0.0872}"#.into()),
        ]));
    }

    #[test]
    fn test_util_to_table_qualified_struct() {
        verify_table_exact_with_ids(r#"
            util::to_table(struct(
                symbol: String(8) = "ABC",
                exchange: String(8) = "NYSE",
                last_sale: f64 = 45.67
            ))
        "#, vec![
            "|------------------------------------|",
            "| id | symbol | exchange | last_sale |",
            "|------------------------------------|",
            "| 0  | ABC    | NYSE     | 45.67     |",
            "|------------------------------------|"
        ])
    }

    #[test]
    fn test_util_to_table_qualified_mixed() {
        verify_table_exact_with_ids(r#"
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
        "#, vec![
            "|-------------------------------------|",
            "| id | symbol | exchange  | last_sale |",
            "|-------------------------------------|",
            "| 0  | BIZ    | NYSE      | 23.66     |",
            "| 1  | DMX    | OTC_BB    | 1.17      |",
            "| 2  | ABC    | OTHER_OTC | 0.67      |",
            "| 3  | TRX    | AMEX      | 29.88     |",
            "| 4  | BMX    | NASDAQ    | 46.11     |",
            "|-------------------------------------|"
        ])
    }

    #[test]
    fn test_util_to_nxx_postfix() {
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
        interpreter = verify_where(interpreter, "-567.311:::to_i16()", |n| n == Number(I16Value(-567)));
        interpreter = verify_where(interpreter, "-125.089:::to_i8()", |n| n == Number(I8Value(-125)));

        // unsigned-integer kinds
        interpreter = verify_where(interpreter, "12789.43:::to_u128()", |n| n == Number(U128Value(12789)));
        interpreter = verify_where(interpreter, "12.3:::to_u64()", |n| n == Number(U64Value(12)));
        interpreter = verify_where(interpreter, "765.65:::to_u32()", |n| n == Number(U32Value(765)));
        interpreter = verify_where(interpreter, "567.311:::to_u16()", |n| n == Number(U16Value(567)));
        interpreter = verify_where(interpreter, "125.089:::to_u8()", |n| n == Number(U8Value(125)));
    }

    #[test]
    fn test_util_to_nxx_scoped() {
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
    fn test_util_uuid() {
        verify_when("util::uuid()", |r| matches!(r, Number(U128Value(..))));
    }

    #[test]
    fn test_generate_asserts_for_to_code() {
        // NOTE: this test generates the test cases for `test_to_code`
        for pf in PLATFORM_OPCODES {
            println!("assert_eq!({:?}.to_code(), \"{}\".to_string());",
                     pf, pf.to_code())
        }
    }

    #[test]
    fn test_to_code() {
        assert_eq!(IoCurrentDir.to_code(), "io::current_dir()".to_string());
        assert_eq!(IoExists.to_code(), "io::exists(a: String())".to_string());
        assert_eq!(IoStdErr.to_code(), "io::stderr(a: String())".to_string());
        assert_eq!(IoStdOut.to_code(), "io::stdout(a: String())".to_string());
        assert_eq!(LangAssert.to_code(), "lang::assert(a: Boolean)".to_string());
        assert_eq!(LangMatches.to_code(), "lang::matches(a, b)".to_string());
        assert_eq!(LangPrintln.to_code(), "lang::println(a: String())".to_string());
        assert_eq!(LangTypeOf.to_code(), "lang::type_of(a)".to_string());
        assert_eq!(OsEnv.to_code(), "os::env()".to_string());
        assert_eq!(OsCall.to_code(), "os::call(a: String())".to_string());
        assert_eq!(OxideEval.to_code(), "oxide::eval(a: String())".to_string());
        assert_eq!(OxideHome.to_code(), "oxide::home()".to_string());
        assert_eq!(OxideReset.to_code(), "oxide::reset()".to_string());
        assert_eq!(OxideServe.to_code(), "oxide::serve(a)".to_string());
        assert_eq!(OxideHelp.to_code(), "oxide::help()".to_string());
        assert_eq!(OxideVersion.to_code(), "oxide::version()".to_string());
        assert_eq!(StrEndsWith.to_code(), "str::ends_with(a: String(), b: String())".to_string());
        assert_eq!(StrFormat.to_code(), "str::format(a: String(), b: String())".to_string());
        assert_eq!(StrLeft.to_code(), "str::left(a: String(), b: String())".to_string());
        assert_eq!(StrRight.to_code(), "str::right(a: String(), b: String())".to_string());
        assert_eq!(StrStartsWith.to_code(), "str::starts_with(a: String(), b: String())".to_string());
        assert_eq!(StrSubstring.to_code(), "str::substring(a: String(), b: String(), c: String())".to_string());
        assert_eq!(StrToString.to_code(), "str::to_string(a)".to_string());
        assert_eq!(UtilCompact.to_code(), "util::compact(a)".to_string());
        assert_eq!(UtilDateDay.to_code(), "util::day_of(a: Date)".to_string());
        assert_eq!(UtilDateHour12.to_code(), "util::hour12(a: Date)".to_string());
        assert_eq!(UtilDateHour24.to_code(), "util::hour24(a: Date)".to_string());
        assert_eq!(UtilDateMinute.to_code(), "util::minute_of(a: Date)".to_string());
        assert_eq!(UtilDateMonth.to_code(), "util::month_of(a: Date)".to_string());
        assert_eq!(UtilDateSecond.to_code(), "util::second_of(a: Date)".to_string());
        assert_eq!(UtilDateYear.to_code(), "util::year_of(a: Date)".to_string());
        assert_eq!(UtilDescribe.to_code(), "util::describe(a)".to_string());
        assert_eq!(UtilFetch.to_code(), "util::fetch(a)".to_string());
        assert_eq!(UtilNow.to_code(), "util::now()".to_string());
        assert_eq!(UtilReverse.to_code(), "util::reverse(a)".to_string());
        assert_eq!(UtilScan.to_code(), "util::scan(a)".to_string());
        assert_eq!(UtilToArray.to_code(), "util::to_array(a)".to_string());
        assert_eq!(UtilToCSV.to_code(), "util::to_csv(a)".to_string());
        assert_eq!(UtilToF32.to_code(), "util::to_f32(a)".to_string());
        assert_eq!(UtilToF64.to_code(), "util::to_f64(a)".to_string());
        assert_eq!(UtilToI8.to_code(), "util::to_i8(a)".to_string());
        assert_eq!(UtilToI16.to_code(), "util::to_i16(a)".to_string());
        assert_eq!(UtilToI32.to_code(), "util::to_i32(a)".to_string());
        assert_eq!(UtilToI64.to_code(), "util::to_i64(a)".to_string());
        assert_eq!(UtilToI128.to_code(), "util::to_i128(a)".to_string());
        assert_eq!(UtilToJSON.to_code(), "util::to_json(a)".to_string());
        assert_eq!(UtilToU8.to_code(), "util::to_u8(a)".to_string());
        assert_eq!(UtilToU16.to_code(), "util::to_u16(a)".to_string());
        assert_eq!(UtilToU32.to_code(), "util::to_u32(a)".to_string());
        assert_eq!(UtilToU64.to_code(), "util::to_u64(a)".to_string());
        assert_eq!(UtilToU128.to_code(), "util::to_u128(a)".to_string());
        assert_eq!(UtilToTable.to_code(), "util::to_table(a)".to_string());
        assert_eq!(UtilUUID.to_code(), "util::uuid()".to_string());
    }
}