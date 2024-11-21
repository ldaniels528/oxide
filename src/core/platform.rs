////////////////////////////////////////////////////////////////////
// Platform class
////////////////////////////////////////////////////////////////////

use crate::codec::Codec;
use crate::compiler::Compiler;
use crate::data_types::DataType;
use crate::data_types::DataType::*;
use crate::data_types::StorageTypes::{FixedSize, BLOB};
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
use crate::platform::PlatformFunctions::*;
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
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use std::{env, thread};
use tokio::runtime::Runtime;
use uuid::Uuid;

pub const MAJOR_VERSION: u8 = 0;
pub const MINOR_VERSION: u8 = 2;

/// Represents Platform Function Information
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct PlatformFunctionInfo {
    pub name: String,
    pub description: String,
    pub package_name: String,
    pub parameters: Vec<Parameter>,
    pub return_type: DataType,
    pub opcode: PlatformFunctions,
}

/// Represents an enumeration of Oxide Platform Functions
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum PlatformFunctions {
    // io package
    IoFileCreate,
    IoFileExists,
    IoFileReadText,
    IoStdErr,
    IoStdOut,
    // os package
    OsCall,
    OsClear,
    OsCurrentDir,
    OsEnv,
    // oxide package
    OxideAssert,
    OxideEval,
    OxideHelp,
    OxideHome,
    OxideMatches,
    OxidePrintln,
    OxideReset,
    OxideTypeOf,
    OxideUUID,
    OxideVersion,
    // str package
    StrEndsWith,
    StrFormat,
    StrJoin,
    StrIndexOf,
    StrLeft,
    StrLen,
    StrRight,
    StrSplit,
    StrStartsWith,
    StrSubstring,
    StrToString,
    // util package
    UtilBase64,
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
    UtilMD5,
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
    // www package
    WwwServe,
    WwwURLDecode,
    WwwURLEncode,
}

pub const PLATFORM_OPCODES: [PlatformFunctions; 64] = {
    use PlatformFunctions::*;
    [
        // io
        IoFileCreate, IoFileExists, IoFileReadText, IoStdErr, IoStdOut,
        // os
        OsCall, OsClear, OsCurrentDir, OsEnv,
        // oxide
        OxideAssert, OxideEval, OxideHelp, OxideHome, OxideMatches,
        OxidePrintln, OxideReset, OxideTypeOf, OxideUUID, OxideVersion,
        // str
        StrEndsWith, StrFormat, StrIndexOf, StrJoin, StrLeft, StrLen,
        StrRight, StrSplit, StrStartsWith, StrSubstring, StrToString,
        // tbl
        UtilCompact, UtilDescribe, UtilFetch, UtilReverse, UtilScan,
        UtilToArray, UtilToCSV, UtilToJSON, UtilToTable,
        // util
        UtilBase64, UtilDateDay, UtilDateHour12, UtilDateHour24,
        UtilDateMinute, UtilDateMonth, UtilDateSecond, UtilDateYear,
        UtilMD5, UtilNow, UtilToF32, UtilToF64,
        UtilToI8, UtilToI16, UtilToI32, UtilToI64, UtilToI128,
        UtilToU8, UtilToU16, UtilToU32, UtilToU64, UtilToU128,
        // www
        WwwURLDecode, WwwURLEncode, WwwServe,
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
            PlatformFunctions::IoFileCreate => self.adapter_fn2(ms, args, Self::do_io_create_file),
            PlatformFunctions::IoFileExists => self.adapter_fn1(ms, args, Self::do_io_exists),
            PlatformFunctions::IoFileReadText => self.adapter_fn1(ms, args, Self::do_io_read_text_file),
            PlatformFunctions::IoStdErr => self.adapter_fn1(ms, args, Self::do_io_stderr),
            PlatformFunctions::IoStdOut => self.adapter_fn1(ms, args, Self::do_io_stdout),
            PlatformFunctions::OsCall => Self::do_os_call(ms, args),
            PlatformFunctions::OsCurrentDir => self.adapter_fn0(ms, args, Self::do_os_current_dir),
            PlatformFunctions::OsClear => self.adapter_fn0(ms, args, Self::do_os_clear_screen),
            PlatformFunctions::OsEnv => self.adapter_fn0(ms, args, Self::do_os_env),
            PlatformFunctions::OxideAssert => self.adapter_fn1(ms, args, Self::do_oxide_assert),
            PlatformFunctions::OxideEval => self.adapter_fn1(ms, args, Self::do_oxide_eval),
            PlatformFunctions::OxideHelp => self.adapter_fn0(ms, args, Self::do_oxide_help),
            PlatformFunctions::OxideHome => self.adapter_fn0(ms, args, Self::do_oxide_home),
            PlatformFunctions::OxideMatches => self.adapter_fn2(ms, args, Self::do_oxide_matches),
            PlatformFunctions::OxidePrintln => self.adapter_fn1(ms, args, Self::do_io_stdout),
            PlatformFunctions::OxideReset => self.adapter_fn0(ms, args, Self::do_oxide_reset),
            PlatformFunctions::OxideTypeOf => self.adapter_fn1(ms, args, Self::do_oxide_type_of),
            PlatformFunctions::OxideUUID => Self::do_util_uuid(ms, args),
            PlatformFunctions::OxideVersion => self.adapter_fn0(ms, args, Self::do_oxide_version),
            PlatformFunctions::StrEndsWith => self.adapter_fn2(ms, args, Self::do_str_ends_with),
            PlatformFunctions::StrFormat => Self::do_str_format(ms, args),
            PlatformFunctions::StrIndexOf => self.adapter_fn2(ms, args, Self::do_str_index_of),
            PlatformFunctions::StrJoin => self.adapter_fn2(ms, args, Self::do_str_join),
            PlatformFunctions::StrLeft => self.adapter_fn2(ms, args, Self::do_str_left),
            PlatformFunctions::StrLen => self.adapter_fn1(ms, args, Self::do_str_len),
            PlatformFunctions::StrRight => self.adapter_fn2(ms, args, Self::do_str_right),
            PlatformFunctions::StrSplit => self.adapter_fn2(ms, args, Self::do_str_split),
            PlatformFunctions::StrStartsWith => self.adapter_fn2(ms, args, Self::do_str_start_with),
            PlatformFunctions::StrSubstring => self.adapter_fn3(ms, args, Self::do_str_substring),
            PlatformFunctions::StrToString => self.adapter_fn1(ms, args, Self::do_str_to_string),
            PlatformFunctions::UtilBase64 => self.adapter_fn1(ms, args, Self::do_util_base64),
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
            PlatformFunctions::UtilMD5 => self.adapter_fn1(ms, args, Self::do_util_md5),
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
            PlatformFunctions::WwwURLDecode => self.adapter_fn1(ms, args, Self::do_www_url_decode),
            PlatformFunctions::WwwURLEncode => self.adapter_fn1(ms, args, Self::do_www_url_encode),
            PlatformFunctions::WwwServe => self.adapter_fn1(ms, args, Self::do_www_serve),
        }
    }

    pub fn find_function(package: &str, name: &str) -> Option<PlatformFunctionInfo> {
        PLATFORM_OPCODES.iter()
            .map(|p| p.get_info())
            .find(|pfi| pfi.package_name == package && pfi.name == name)
    }

    pub fn get_description(&self) -> String {
        let result = match self {
            PlatformFunctions::IoFileCreate => "Creates a new file",
            PlatformFunctions::OsCurrentDir => "Returns the current directory",
            PlatformFunctions::IoFileExists => "Returns true if the source path exists",
            PlatformFunctions::IoFileReadText => "Reads the contents of a text file into memory",
            PlatformFunctions::IoStdErr => "Writes a string to STDERR",
            PlatformFunctions::IoStdOut => "Writes a string to STDOUT",
            PlatformFunctions::OxideAssert => "Evaluates an assertion returning Ack or an error",
            PlatformFunctions::OxideMatches => "Compares two values",
            PlatformFunctions::OxidePrintln => "Print line function",
            PlatformFunctions::OxideTypeOf => "Returns the type of a value",
            PlatformFunctions::OsCall => "Invokes an operating system application",
            PlatformFunctions::OsClear => "Clears the terminal/screen",
            PlatformFunctions::OsEnv => "Returns a table of the OS environment variables",
            PlatformFunctions::OxideEval => "Evaluates a string containing Oxide code",
            PlatformFunctions::OxideHome => "Returns the Oxide home directory path",
            PlatformFunctions::OxideReset => "Clears the scope of all user-defined objects",
            PlatformFunctions::WwwServe => "Starts a local HTTP service",
            PlatformFunctions::OxideHelp => "Integrated help function",
            PlatformFunctions::OxideVersion => "Returns the Oxide version",
            PlatformFunctions::StrEndsWith => "Returns true if string A ends with string B",
            PlatformFunctions::StrFormat => "Returns an argument-formatted string",
            PlatformFunctions::StrIndexOf => "Returns the index of string 'b' in string 'a'",
            PlatformFunctions::StrJoin => "Combines an array into a string",
            PlatformFunctions::StrLeft => "Returns n-characters from left-to-right",
            PlatformFunctions::StrLen => "Returns the number of characters contained in the string",
            PlatformFunctions::StrRight => "Returns n-characters from right-to-left",
            PlatformFunctions::StrSplit => "Splits string A by delimiter string B",
            PlatformFunctions::StrStartsWith => "Returns true if string A starts with string B",
            PlatformFunctions::StrSubstring => "Returns a substring of string A from B to C",
            PlatformFunctions::StrToString => "Converts a value to its text-based representation",
            PlatformFunctions::UtilBase64 => "Translates bytes into Base 64",
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
            PlatformFunctions::UtilMD5 => "Creates a MD5 digest",
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
            PlatformFunctions::OxideUUID => "Returns a random 128-bit UUID",
            PlatformFunctions::WwwURLDecode => "Decodes a URL-encoded string",
            PlatformFunctions::WwwURLEncode => "Encodes a URL string",
        };
        result.to_string()
    }

    pub fn get_info(&self) -> PlatformFunctionInfo {
        PlatformFunctionInfo {
            name: self.get_name(),
            description: self.get_description(),
            package_name: self.get_package_name(),
            parameters: self.get_parameters(),
            return_type: self.get_return_type(),
            opcode: self.to_owned(),
        }
    }

    pub fn get_name(&self) -> String {
        let result = match self {
            PlatformFunctions::IoFileCreate => "create_file",
            PlatformFunctions::OsCurrentDir => "current_dir",
            PlatformFunctions::IoFileExists => "exists",
            PlatformFunctions::IoFileReadText => "read_text_file",
            PlatformFunctions::IoStdErr => "stderr",
            PlatformFunctions::IoStdOut => "stdout",
            PlatformFunctions::OxideAssert => "assert",
            PlatformFunctions::OxideMatches => "matches",
            PlatformFunctions::OxidePrintln => "println",
            PlatformFunctions::OxideTypeOf => "type_of",
            PlatformFunctions::OsCall => "call",
            PlatformFunctions::OsClear => "clear",
            PlatformFunctions::OsEnv => "env",
            PlatformFunctions::OxideEval => "eval",
            PlatformFunctions::OxideHome => "home",
            PlatformFunctions::OxideReset => "reset",
            PlatformFunctions::WwwServe => "serve",
            PlatformFunctions::OxideHelp => "help",
            PlatformFunctions::OxideVersion => "version",
            PlatformFunctions::StrEndsWith => "ends_with",
            PlatformFunctions::StrFormat => "format",
            PlatformFunctions::StrIndexOf => "index_of",
            PlatformFunctions::StrJoin => "join",
            PlatformFunctions::StrLeft => "left",
            PlatformFunctions::StrLen => "len",
            PlatformFunctions::StrRight => "right",
            PlatformFunctions::StrSplit => "split",
            PlatformFunctions::StrStartsWith => "starts_with",
            PlatformFunctions::StrSubstring => "substring",
            PlatformFunctions::StrToString => "to_string",
            PlatformFunctions::UtilBase64 => "base64",
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
            PlatformFunctions::UtilMD5 => "md5",
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
            PlatformFunctions::OxideUUID => "uuid",
            PlatformFunctions::WwwURLDecode => "url_decode",
            PlatformFunctions::WwwURLEncode => "url_encode",
        };
        result.to_string()
    }

    pub fn get_package_name(&self) -> String {
        use PlatformFunctions::*;
        let result = match self {
            // io
            IoFileCreate | IoFileExists | IoFileReadText | IoStdErr | IoStdOut => "io",
            // oxide
            OxideAssert | OxideMatches | OxidePrintln | OxideTypeOf => "oxide",
            // os
            OsCall | OsClear | OsCurrentDir | OsEnv => "os",
            // oxide
            OxideEval | OxideHome | OxideReset | OxideHelp | OxideUUID | OxideVersion => "oxide",
            // str
            StrEndsWith | StrFormat | StrIndexOf | StrJoin | StrLeft | StrLen | StrRight |
            StrSplit | StrStartsWith | StrSubstring | StrToString => "str",
            // util
            UtilBase64 | UtilCompact | UtilDateDay | UtilDateHour12 | UtilDateHour24 |
            UtilDateMinute | UtilDateMonth | UtilDateSecond | UtilDateYear |
            UtilDescribe | UtilFetch | UtilMD5 | UtilNow | UtilReverse | UtilScan |
            UtilToArray | UtilToCSV | UtilToJSON | UtilToF32 | UtilToF64 |
            UtilToI8 | UtilToI16 | UtilToI32 | UtilToI64 | UtilToI128 |
            UtilToTable | UtilToU8 | UtilToU16 | UtilToU32 | UtilToU64 |
            UtilToU128 => "util",
            // www
            WwwURLDecode | WwwURLEncode | WwwServe => "www",
        };
        result.to_string()
    }

    pub fn get_parameter_types(&self) -> Vec<DataType> {
        use PlatformFunctions::*;
        match self {
            // zero-parameter
            OsCurrentDir | OsEnv | OxideHome | OxideReset | OsClear |
            OxideHelp | OxideVersion | UtilNow | OxideUUID
            => vec![],
            // single-parameter (boolean)
            OxideAssert
            => vec![BooleanType],
            // single-parameter (date)
            UtilDateDay | UtilDateHour12 | UtilDateHour24 | UtilDateMinute |
            UtilDateMonth | UtilDateSecond | UtilDateYear
            => vec![DateType],
            // single-parameter (int)
            StrLen | WwwServe
            => vec![NumberType(U32Kind)],
            // single-parameter (lazy)
            OxideTypeOf | StrToString | UtilBase64 | UtilMD5 | UtilToF32 | UtilToF64 |
            UtilToI8 | UtilToI16 | UtilToI32 | UtilToI64 | UtilToI128 | UtilToTable |
            UtilToU8 | UtilToU16 | UtilToU32 | UtilToU64 | UtilToU128
            => vec![LazyType],
            // single-parameter (string)
            IoFileExists | IoFileReadText | IoStdErr | IoStdOut |
            OxidePrintln | OsCall | OxideEval | WwwURLDecode | WwwURLEncode
            => vec![StringType(BLOB)],
            // single-parameter (table)
            UtilCompact | UtilDescribe | UtilReverse | UtilScan |
            UtilToArray | UtilToCSV | UtilToJSON
            => vec![TableType(Vec::new(), BLOB)],
            // two-parameter (lazy, lazy)
            OxideMatches
            => vec![LazyType, LazyType],
            // two-parameter (string, string)
            IoFileCreate | StrEndsWith | StrFormat | StrSplit | StrStartsWith
            => vec![StringType(BLOB), StringType(BLOB)],
            // two-parameter (string, i64)
            StrIndexOf | StrLeft | StrRight
            => vec![StringType(BLOB), NumberType(I64Kind)],
            // two-parameter (table, u64)
            UtilFetch
            => vec![TableType(vec![], BLOB), NumberType(U64Kind)],
            // two-parameter (array, string)
            StrJoin
            => vec![ArrayType(Box::from(LazyType)), StringType(BLOB)],
            // three-parameter (string, i64, i64)
            StrSubstring
            => vec![StringType(BLOB), NumberType(I64Kind), NumberType(I64Kind)],
        }
    }

    pub fn get_parameters(&self) -> Vec<Parameter> {
        let names = match self.get_parameter_types().as_slice() {
            [BooleanType] => vec!["b".to_string()],
            [DateType] => vec!["d".to_string()],
            [LazyType] => vec!["x".to_string()],
            [NumberType(..)] => vec!["n".to_string()],
            [StringType(..)] => vec!["s".to_string()],
            [TableType(..)] => vec!["t".to_string()],
            params => params.iter().enumerate()
                .map(|(n, dt)| format!("{}", ((n + 97) as u8) as char))
                .collect()
        };

        names.iter().zip(self.get_parameter_types().iter()).enumerate()
            .map(|(n, (name, dt))|
                Parameter::new(
                    name,
                    dt.to_type_declaration(),
                    None,
                ))
            .collect()
    }

    pub fn get_return_type(&self) -> DataType {
        use PlatformFunctions::*;
        match self {
            // array
            UtilToArray => ArrayType(Box::from(LazyType)),
            IoFileReadText | StrSplit | UtilToCSV | UtilToJSON => ArrayType(Box::from(StringType(BLOB))),
            // boolean
            IoFileExists | OxideMatches | StrEndsWith | StrStartsWith => BooleanType,
            // bytes
            UtilMD5 => ByteArrayType(FixedSize(16)),
            // date
            UtilNow => DateType,
            // number
            StrIndexOf | StrLen => NumberType(I64Kind),
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
            UtilToU128 | OxideUUID => NumberType(U128Kind),
            // outcome
            IoFileCreate | OxideAssert | OxidePrintln |
            OsClear | OxideReset | WwwServe => OutcomeType(Acked),
            // string
            IoStdErr | IoStdOut | OxideTypeOf |
            OsCall | OsCurrentDir | OxideEval | OxideHome | OxideVersion |
            StrFormat | StrJoin | StrLeft | StrRight | StrSubstring | StrToString |
            UtilBase64 | WwwURLDecode | WwwURLEncode => StringType(BLOB),
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
        self.to_code_with_params(&self.get_parameters())
    }

    pub fn to_code_with_params(&self, parameters: &Vec<Parameter>) -> String {
        let pkg = self.get_package_name();
        let name = self.get_name();
        let params = parameters.iter()
            .map(|p| p.to_code())
            .collect::<Vec<_>>()
            .join(", ");
        format!("{pkg}::{name}({params})")
    }

    ////////////////////////////////////////////////////////////////////
    //      Utility Functions
    ////////////////////////////////////////////////////////////////////

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
            args => (ms, ErrorValue(ArgumentsMismatched(1, args.len())))
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
            args => (ms, ErrorValue(ArgumentsMismatched(1, args.len())))
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
            args => (ms, ErrorValue(ArgumentsMismatched(2, args.len())))
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
            args => (ms, ErrorValue(ArgumentsMismatched(3, args.len())))
        }
    }

    fn do_io_create_file(
        ms: Machine,
        path_v: &TypedValue,
        contents_v: &TypedValue,
    ) -> (Machine, TypedValue) {
        // creates or overwrites a file
        fn create_file(
            ms: Machine,
            path_v: &TypedValue,
            contents_v: &TypedValue,
        ) -> std::io::Result<(Machine, TypedValue)> {
            match path_v {
                StringValue(path) => {
                    let mut file = File::create(path)?;
                    let result = file.write(contents_v.unwrap_value().as_bytes())?;
                    Ok((ms, Number(U64Value(result as u64))))
                }
                other => Ok((ms, ErrorValue(StringExpected(other.to_code()))))
            }
        }

        // create or overwrite the file
        let ms0 = ms.clone();
        match create_file(ms, path_v, contents_v) {
            Ok(result) => result,
            Err(err) => (ms0, ErrorValue(Exact(err.to_string())))
        }
    }

    fn do_io_exists(ms: Machine, path_value: &TypedValue) -> (Machine, TypedValue) {
        match path_value {
            StringValue(path) => (ms, Boolean(Path::new(path).exists())),
            other => (ms, ErrorValue(StringExpected(other.to_string())))
        }
    }

    fn do_io_read_text_file(
        ms: Machine,
        path_v: &TypedValue,
    ) -> (Machine, TypedValue) {
        // reads the contents of a file
        fn read_file(
            ms: Machine,
            path_v: &TypedValue,
        ) -> std::io::Result<(Machine, TypedValue)> {
            match path_v {
                StringValue(path) => {
                    let mut buffer = String::new();
                    let mut file = File::open(path)?;
                    let _count = file.read_to_string(&mut buffer)?;
                    Ok((ms, StringValue(buffer)))
                }
                other => Ok((ms, ErrorValue(StringExpected(other.to_code()))))
            }
        }

        // create or overwrite the file
        let ms0 = ms.clone();
        match read_file(ms, path_v) {
            Ok(result) => result,
            Err(err) => (ms0, ErrorValue(Exact(err.to_string())))
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

    fn do_oxide_assert(ms: Machine, value: &TypedValue) -> (Machine, TypedValue) {
        match value {
            ErrorValue(msg) => (ms, ErrorValue(msg.to_owned())),
            Boolean(false) => (ms, ErrorValue(AssertionError("true".to_string(), "false".to_string()))),
            z => (ms, z.to_owned())
        }
    }

    fn do_oxide_matches(ms: Machine, a: &TypedValue, b: &TypedValue) -> (Machine, TypedValue) {
        (ms, a.matches(b))
    }

    fn do_oxide_type_of(ms: Machine, a: &TypedValue) -> (Machine, TypedValue) {
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

    fn do_os_current_dir(ms: Machine) -> (Machine, TypedValue) {
        match env::current_dir() {
            Ok(dir) => (ms, StringValue(dir.display().to_string())),
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

    /// str::index_of("Hello World", "World")
    fn do_str_index_of(
        ms: Machine,
        host_str: &TypedValue,
        search_str: &TypedValue,
    ) -> (Machine, TypedValue) {
        match host_str {
            StringValue(host) => {
                match search_str {
                    StringValue(search) => {
                        match host.find(search) {
                            None => (ms, Undefined),
                            Some(index) => (ms, Number(I64Value(index as i64))),
                        }
                    }
                    z => (ms, ErrorValue(TypeMismatch("String".into(), z.get_type_name())))
                }
            }
            z => (ms, ErrorValue(TypeMismatch("String".into(), z.get_type_name())))
        }
    }

    /// str::join(\["a", "b", "c"], ", ") => "a, b, c"
    fn do_str_join(
        ms: Machine,
        array: &TypedValue,
        delim: &TypedValue,
    ) -> (Machine, TypedValue) {
        match array {
            Array(items) => {
                let mut buf = String::new();
                for item in items {
                    if !buf.is_empty() { buf.extend(delim.unwrap_value().chars()) }
                    buf.extend(item.unwrap_value().chars());
                }
                (ms, StringValue(buf))
            }
            z => (ms, ErrorValue(TypeMismatch("Array".into(), z.get_type_name())))
        }
    }

    fn do_str_left(
        ms: Machine,
        string: &TypedValue,
        n_chars: &TypedValue,
    ) -> (Machine, TypedValue) {
        match string {
            StringValue(s) =>
                match n_chars {
                    Number(nv) if nv.to_i64() < 0 =>
                        Self::do_str_right(ms.to_owned(), string, &Number(I64Value(-nv.to_i64()))),
                    Number(nv) =>
                        (ms, StringValue(s[0..nv.to_usize()].to_string())),
                    _ => (ms, Undefined)
                },
            _ => (ms, Undefined)
        }
    }

    fn do_str_len(
        ms: Machine,
        string: &TypedValue,
    ) -> (Machine, TypedValue) {
        match string {
            StringValue(s) => (ms, Number(I64Value(s.len() as i64))),
            _ => (ms, Undefined)
        }
    }

    fn do_str_right(
        ms: Machine,
        string: &TypedValue,
        n_chars: &TypedValue,
    ) -> (Machine, TypedValue) {
        match string {
            StringValue(s) =>
                match n_chars {
                    Number(nv) if nv.to_i64() < 0 =>
                        Self::do_str_left(ms.to_owned(), string, &Number(I64Value(-nv.to_i64()))),
                    Number(nv) => {
                        let strlen = s.len();
                        (ms, StringValue(s[(strlen - nv.to_usize())..strlen].to_string()))
                    }
                    _ => (ms, Undefined)
                },
            _ => (ms, Undefined)
        }
    }

    fn do_str_split(
        ms: Machine,
        string_v: &TypedValue,
        delimiter_v: &TypedValue,
    ) -> (Machine, TypedValue) {
        match string_v {
            StringValue(src) => {
                match delimiter_v {
                    StringValue(delimiters) => (ms, Array(src.split(|c| delimiters.contains(c))
                        .map(|s| StringValue(s.to_string()))
                        .collect::<Vec<_>>())),
                    z => (ms, ErrorValue(StringExpected(z.to_code())))
                }
            }
            z => (ms, ErrorValue(StringExpected(z.to_code())))
        }
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
        (ms, StringValue(a.unwrap_value()))
    }

    fn do_util_base64(
        ms: Machine,
        a: &TypedValue,
    ) -> (Machine, TypedValue) {
        (ms, StringValue(base64::encode(a.to_bytes())))
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

    fn do_util_md5(ms: Machine, value: &TypedValue) -> (Machine, TypedValue) {
        match md5::compute(value.to_bytes()) {
            md5::Digest(bytes) => (ms, ByteArray(bytes.to_vec()))
        }
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

    fn do_www_url_decode(ms: Machine, url: &TypedValue) -> (Machine, TypedValue) {
        match url {
            StringValue(uri) => match urlencoding::decode(uri) {
                Ok(decoded) => (ms, StringValue(decoded.to_string())),
                Err(err) => (ms, ErrorValue(Exact(err.to_string())))
            }
            other => (ms, ErrorValue(StringExpected(other.to_code())))
        }
    }

    fn do_www_url_encode(ms: Machine, url: &TypedValue) -> (Machine, TypedValue) {
        match url {
            StringValue(uri) => (ms, StringValue(urlencoding::encode(uri).to_string())),
            other => (ms, ErrorValue(StringExpected(other.to_code())))
        }
    }

    fn do_www_serve(
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
    use crate::testdata::*;
    use crate::typed_values::TypedValue::*;
    use PlatformFunctions::*;

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
    fn test_get_info_oxide_matches() {
        assert_eq!(OxideMatches.get_info(), PlatformFunctionInfo {
            name: "matches".into(),
            description: "Compares two values".into(),
            package_name: "oxide".into(),
            parameters: vec![
                Parameter::new("a", None, None),
                Parameter::new("b", None, None),
            ],
            return_type: BooleanType,
            opcode: OxideMatches,
        });
    }

    #[test]
    fn test_get_info_str_left() {
        assert_eq!(StrLeft.get_info(), PlatformFunctionInfo {
            name: "left".into(),
            description: "Returns n-characters from left-to-right".into(),
            package_name: "str".into(),
            parameters: vec![
                Parameter::new("a", Some("String()".into()), None),
                Parameter::new("b", Some("i64".into()), None),
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
            package_name: "str".into(),
            parameters: vec![
                Parameter::new("a", Some("String()".into()), None),
                Parameter::new("b", Some("i64".into()), None),
                Parameter::new("c", Some("i64".into()), None)
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
            package_name: "util".into(),
            parameters: Vec::new(),
            return_type: DateType,
            opcode: UtilNow,
        });
    }

    #[test]
    fn test_io_create_file() {
        // fully-qualified
        verify_exact(r#"
            io::create_file("quote.json", { symbol: "TRX", exchange: "NYSE", last_sale: 45.32 })
        "#, Number(U64Value(51)));

        verify_exact(r#"
            io::exists("quote.json")
        "#, Boolean(true));

        // postfix
        verify_exact(r#"
            import io
            "quote.json":::create_file({ symbol: "TRX", exchange: "NYSE", last_sale: 45.32 })
        "#, Number(U64Value(51)));

        verify_exact(r#"
            import io
            "quote.json":::exists()
        "#, Boolean(true));
    }

    #[test]
    fn test_io_file_exists() {
        verify_exact(r#"
            import io
            path_str := oxide::home()
            path_str:::exists()
        "#, Boolean(true))
    }

    #[test]
    fn test_io_create_and_read_text_file() {
        verify_exact(r#"
            import io
            file := "temp_secret.txt"
            file:::create_file("**keep**this**secret**")
            file:::read_text_file()
        "#, StringValue("**keep**this**secret**".to_string()))
    }

    #[test]
    fn test_io_stderr() {
        verify_exact(r#"io::stderr("Goodbye Cruel World")"#, Outcome(Ack));
    }

    #[test]
    fn test_io_stdout() {
        verify_exact(r#"io::stdout("Hello World")"#, Outcome(Ack));
    }

    #[test]
    fn test_os_call() {
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
    fn test_os_clear() {
        verify_exact("os::clear()", Outcome(Ack))
    }

    #[test]
    fn test_os_current_dir() {
        let phys_columns = make_quote_columns();
        verify_exact_table_with_ids(r#"
            import str
            cur_dir := os::current_dir()
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
    fn test_os_env() {
        verify_when("os::env()", |v| matches!(v, TableValue(..)))
    }

    #[test]
    fn test_oxide_eval() {
        // fully-qualified
        verify_exact("oxide::eval('2 ** 4')", Number(F64Value(16.)));
        verify_exact("oxide::eval(123)", ErrorValue(StringExpected("i64".into())));

        // postfix
        let mut interpreter = Interpreter::new();
        interpreter.evaluate("import oxide").unwrap();
        interpreter = verify_where(interpreter, "'2 ** 4':::eval()", Number(F64Value(16.)));
        interpreter = verify_where(interpreter, "123:::eval()", ErrorValue(StringExpected("i64".into())));
    }

    #[test]
    fn test_oxide_help() {
        // fully-qualified
        verify_when("oxide::help()", |v| matches!(v, TableValue(..)));

        // postfix
        verify_when(r#"
            import oxide
            help()
        "#, |v| matches!(v, TableValue(..)));
    }

    #[test]
    fn test_oxide_home() {
        verify_exact("oxide::home()", StringValue(Machine::oxide_home()));
    }

    #[test]
    fn test_oxide_matches() {
        // test a perfect match
        verify_exact(r#"
            a := { first: "Tom", last: "Lane", scores: [82, 78, 99] }
            b := { first: "Tom", last: "Lane", scores: [82, 78, 99] }
            oxide::matches(a, b)
        "#, Boolean(true));

        // test an unordered match
        verify_exact(r#"
            import oxide::matches
            a := { scores: [82, 78, 99], first: "Tom", last: "Lane" }
            b := { last: "Lane", first: "Tom", scores: [82, 78, 99] }
            matches(a, b)
        "#, Boolean(true));

        // test when things do not match 1
        verify_exact(r#"
            import oxide
            a := { first: "Tom", last: "Lane" }
            b := { first: "Jerry", last: "Lane" }
            a:::matches(b)
        "#, Boolean(false));

        // test when things do not match 2
        verify_exact(r#"
            a := { key: "123", values: [1, 74, 88] }
            b := { key: "123", values: [1, 74, 88, 0] }
            oxide::matches(a, b)
        "#, Boolean(false));
    }

    #[test]
    fn test_oxide_println() {
        verify_exact(r#"oxide::println("Hello World")"#, Outcome(Ack));
    }

    #[test]
    fn test_oxide_type_of() {
        let mut interpreter = Interpreter::new();
        interpreter.evaluate("import oxide").unwrap();
        interpreter = verify_where(interpreter, "type_of([true, false])", StringValue("Array<Boolean>".into()));
        interpreter = verify_where(interpreter, "type_of([12, 76, 444])", StringValue("Array<i64>".into()));
        interpreter = verify_where(interpreter, "type_of(['ciao', 'hello', 'world'])", StringValue("Array<String(5)>".into()));
        interpreter = verify_where(interpreter, "type_of([12, 'hello', 76.78])", StringValue("Array<f64>".into()));
        interpreter = verify_where(interpreter, "type_of(false)", StringValue("Boolean".into()));
        interpreter = verify_where(interpreter, "type_of(true)", StringValue("Boolean".into()));
        interpreter = verify_where(interpreter, "type_of(util::now())", StringValue("Date".into()));
        interpreter = verify_where(interpreter, "type_of(fn(a, b) => a + b)", StringValue("fn(a, b)".into()));
        interpreter = verify_where(interpreter, "type_of(1234)", StringValue("i64".into()));
        interpreter = verify_where(interpreter, "type_of(12.394)", StringValue("f64".into()));
        interpreter = verify_where(interpreter, "type_of('1234')", StringValue("String(4)".into()));
        interpreter = verify_where(interpreter, r#"type_of("abcde")"#, StringValue("String(5)".into()));
        interpreter = verify_where(interpreter, r#"type_of({symbol:"ABC"})"#,
                                   StringValue(r#"struct(symbol: String(3) = "ABC")"#.into()));
        interpreter = verify_where(interpreter, r#"type_of(ns("a.b.c"))"#, StringValue("Table()".into()));
        interpreter = verify_where(interpreter, r#"type_of(table(
            symbol: String(8),
            exchange: String(8),
            last_sale: f64
        ))"#, StringValue("Table(symbol: String(8), exchange: String(8), last_sale: f64)".into()));
        interpreter = verify_where(interpreter, "type_of(util::uuid())", StringValue("u128".into()));
        interpreter = verify_where(interpreter, "type_of(my_var)", StringValue("".into()));
        interpreter = verify_where(interpreter, "type_of(null)", StringValue("".into()));
        interpreter = verify_where(interpreter, "type_of(undefined)", StringValue("".into()));
    }

    #[test]
    fn test_oxide_version() {
        verify_exact(
            "oxide::version()",
            StringValue(format!("{MAJOR_VERSION}.{MINOR_VERSION}")))
    }

    #[test]
    fn test_str_ends_with() {
        // postfix
        verify_exact(r#"
            import str
            'Hello World':::ends_with('World')
        "#, Boolean(true));

        // fully-qualified
        verify_exact(r#"
            str::ends_with('Hello World', 'World')
        "#, Boolean(true));

        // fully-qualified (negative case)
        verify_exact(r#"
            str::ends_with('Hello World', 'Hello')
        "#, Boolean(false))
    }

    #[test]
    fn test_str_format() {
        // fully qualified
        verify_exact(r#"
            str::format("This {} the {}", "is", "way")
        "#, StringValue("This is the way".into()));

        // postfix
        verify_exact(r#"
            import str::format
            "This {} the {}":::format("is", "way")
        "#, StringValue("This is the way".into()));

        // in-scope
        verify_exact(r#"
            import str::format
            format("This {} the {}", "is", "way")
        "#, StringValue("This is the way".into()));
    }

    #[test]
    fn test_str_index_of() {
        verify_exact(r#"
            str::index_of('The little brown fox', 'brown')
        "#, Number(I64Value(11)));

        verify_exact(r#"
            import str
            'The little brown fox':::index_of('brown')
        "#, Number(I64Value(11)));
    }

    #[test]
    fn test_str_join() {
        verify_exact(r#"
            str::join(['1', 5, 9, '13'], ', ')
        "#, StringValue("1, 5, 9, 13".into()));
    }

    #[test]
    fn test_str_left() {
        // fully-qualified
        verify_exact(r#"
            str::left('Hello World', 5)
        "#, StringValue("Hello".into()));

        // fully-qualified (inverse)
        verify_exact(r#"
            str::left('Hello World', -5)
        "#, StringValue("World".into()));

        // postfix
        verify_exact(r#"
            import str, util
            'Hello World':::left(5)
        "#, StringValue("Hello".into()));

        // postfix - invalid case
        verify_exact(r#"
            import str, util
            12345:::left(5)
        "#, Undefined);

        // attempt a non-import function from the same package
        verify_when(r#"
            left('Hello World', 5)
        "#, |v| matches!(v, ErrorValue(..)));
    }

    #[test]
    fn test_str_len() {
        // fully-qualified
        verify_exact(r#"
            str::len('The little brown fox')
        "#, Number(I64Value(20)));

        // postfix
        verify_exact(r#"
            import str
            'The little brown fox':::len()
        "#, Number(I64Value(20)));
    }

    #[test]
    fn test_str_right() {
        // fully-qualified
        verify_exact(r#"
            str::right('Hello World', 5)
        "#, StringValue("World".into()));

        // fully-qualified (inverse)
        verify_exact(r#"
            str::right('Hello World', -5)
        "#, StringValue("Hello".into()));

        // fully-qualified (negative case)
        verify_exact(r#"
            str::right(7779311, 5)
        "#, Undefined);

        // postfix (inverse)
        verify_exact(r#"
            import str, util
            'Hello World':::right(-5)
        "#, StringValue("Hello".into()));
    }

    #[test]
    fn test_str_split() {
        // fully-qualified
        verify_exact(r#"
            str::split('Hello,there World', ' ,')
        "#, Array(vec![
            StringValue("Hello".into()),
            StringValue("there".into()),
            StringValue("World".into())
        ]));

        // postfix
        verify_exact(r#"
            import str
            'Hello World':::split(' ')
        "#, Array(vec![
            StringValue("Hello".into()),
            StringValue("World".into())
        ]));

        // in-scope
        verify_exact(r#"
            import str
            split('Hello,there World;Yeah!', ' ,;')
        "#, Array(vec![
            StringValue("Hello".into()),
            StringValue("there".into()),
            StringValue("World".into()),
            StringValue("Yeah!".into())
        ]));
    }

    #[test]
    fn test_str_starts_with() {
        // postfix
        verify_exact(r#"
            import str
            'Hello World':::starts_with('Hello')
        "#, Boolean(true));

        // fully-qualified
        verify_exact(r#"
            str::starts_with('Hello World', 'Hello')
        "#, Boolean(true));

        // fully-qualified (negative case)
        verify_exact(r#"
            str::starts_with('Hello World', 'World')
        "#, Boolean(false))
    }

    #[test]
    fn test_str_substring() {
        // fully-qualified
        verify_exact(r#"
            str::substring('Hello World', 0, 5)
        "#, StringValue("Hello".into()));

        // fully-qualified (negative case)
        verify_exact(r#"
            str::substring(8888, 0, 5)
        "#, Undefined)
    }

    #[test]
    fn test_str_to_string() {
        // fully-qualified
        verify_exact(r#"
            str::to_string(125.75)
        "#, StringValue("125.75".into()));

        // postfix
        verify_exact(r#"
            import str::to_string
            123:::to_string()
        "#, StringValue("123".into()));
    }

    #[test]
    fn test_util_base64() {
        verify_exact(
            "util::base64('Hello World')",
            StringValue("SGVsbG8gV29ybGQ=".into()))
    }

    #[test]
    fn test_util_compact() {
        let mut interpreter = Interpreter::new();
        interpreter = verify_exact_table_where(interpreter, r#"
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
        "#, vec![
            "|------------------------------------|",
            "| id | symbol | exchange | last_sale |",
            "|------------------------------------|",
            "| 1  | UNO    | OTC      | 0.2456    |",
            "| 3  | GOTO   | OTC      | 0.1428    |",
            "| 5  | BOOM   | NASDAQ   | 0.0872    |",
            "|------------------------------------|"
        ]);

        verify_exact_table_where(interpreter, r#"
            [+] import util
            [+] stocks:::compact()
            [+] from stocks
        "#, vec![
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
    fn test_util_describe() {
        // fully-qualified
        verify_exact_table_with_ids(r#"
            util::describe({ symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 })
        "#, vec![
            "|----------------------------------------------------------|",
            "| id | name      | type      | default_value | is_nullable |",
            "|----------------------------------------------------------|",
            "| 0  | symbol    | String(3) | \"BIZ\"         | true        |",
            "| 1  | exchange  | String(4) | \"NYSE\"        | true        |",
            "| 2  | last_sale | f64       | 23.66         | true        |",
            "|----------------------------------------------------------|"
        ]);

        // postfix
        verify_exact_table_with_ids(r#"
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
    fn test_util_fetch() {
        // fully-qualified
        verify_exact_table_with_ids(r#"
            [+] stocks := ns("platform.fetch.stocks")
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

        // postfix
        verify_exact_table_with_ids(r#"
            [+] import util
            [+] stocks := to_table([
                    { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                    { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                    { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }
                ])
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
    fn test_util_md5() {
        verify_exact(
            "util::md5('Hello World')",
            ByteArray(vec![177, 10, 141, 177, 100, 224, 117, 65, 5, 183, 169, 155, 231, 46, 63, 229]))
    }

    #[test]
    fn test_util_now_and_date_functions() {
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
            ts := util::now()
        "#).unwrap();
        assert_eq!(result, Outcome(Ack));
        interpreter.evaluate("import util").unwrap();
        interpreter = verify_whence(interpreter, "ts:::day_of()", |n| matches!(n, Number(U32Value(..))));
        interpreter = verify_whence(interpreter, "ts:::hour24()", |n| matches!(n, Number(U32Value(..))));
        interpreter = verify_whence(interpreter, "ts:::hour12()", |n| matches!(n, Number(U32Value(..))));
        interpreter = verify_whence(interpreter, "ts:::minute_of()", |n| matches!(n, Number(U32Value(..))));
        interpreter = verify_whence(interpreter, "ts:::month_of()", |n| matches!(n, Number(U32Value(..))));
        interpreter = verify_whence(interpreter, "ts:::second_of()", |n| matches!(n, Number(U32Value(..))));
        interpreter = verify_whence(interpreter, "ts:::year_of()", |n| matches!(n, Number(I32Value(..))));
    }

    #[test]
    fn test_util_reverse_arrays() {
        verify_exact_table_with_ids(r#"
            import util
            to_table(reverse(['cat', 'dog', 'ferret', 'mouse']))
        "#, vec![
            "|-------------|",
            "| id | value  |",
            "|-------------|",
            "| 0  | mouse  |",
            "| 1  | ferret |",
            "| 2  | dog    |",
            "| 3  | cat    |",
            "|-------------|"
        ])
    }

    #[test]
    fn test_util_reverse_tables() {
        // fully-qualified (ephemeral)
        verify_exact_table_with_ids(r#"
            import util
            stocks := util::to_table([
                { symbol: "ABC", exchange: "AMEX", last_sale: 12.33 },
                { symbol: "BIZ", exchange: "NYSE", last_sale: 9.775 },
                { symbol: "XYZ", exchange: "NASDAQ", last_sale: 89.11 }
            ])
            util::reverse(stocks)
        "#, vec![
            "|------------------------------------|",
            "| id | symbol | exchange | last_sale |",
            "|------------------------------------|",
            "| 0  | XYZ    | NASDAQ   | 89.11     |",
            "| 1  | BIZ    | NYSE     | 9.775     |",
            "| 2  | ABC    | AMEX     | 12.33     |",
            "|------------------------------------|"
        ]);

        // postfix (durable)
        verify_exact_table_with_ids(r#"
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
    fn test_util_scan() {
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
    fn test_util_to_array_with_strings() {
        // fully-qualified
        verify_exact(r#"
             util::to_array("Hello")
        "#, Array(vec![
            StringValue("H".into()), StringValue("e".into()),
            StringValue("l".into()), StringValue("l".into()),
            StringValue("o".into())
        ]));

        // postfix
        verify_exact(r#"
            import util
            "World":::to_array()
        "#, Array(vec![
            StringValue("W".into()), StringValue("o".into()),
            StringValue("r".into()), StringValue("l".into()),
            StringValue("d".into())
        ]));
    }

    #[test]
    fn test_util_to_array_with_tables() {
        // fully qualified
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
        ]));
    }

    #[test]
    fn test_util_to_csv() {
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
    fn test_util_to_json() {
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
    fn test_util_to_table_with_arrays() {
        verify_exact_table_with_ids(r#"
            util::to_table(['cat', 'dog', 'ferret', 'mouse'])
        "#, vec![
            "|-------------|",
            "| id | value  |",
            "|-------------|",
            "| 0  | cat    |",
            "| 1  | dog    |",
            "| 2  | ferret |",
            "| 3  | mouse  |",
            "|-------------|"
        ])
    }

    #[test]
    fn test_util_to_table_with_hard_structures() {
        verify_exact_table_with_ids(r#"
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
    fn test_util_to_table_with_soft_and_hard_structures() {
        verify_exact_table_with_ids(r#"
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
    fn test_util_to_f32_to_u128() {
        use crate::numbers::NumberValue::*;
        let mut interpreter = Interpreter::new();
        interpreter.evaluate("import util").unwrap();

        // floating-point kinds
        interpreter = verify_whence(interpreter, "1015:::to_f32()", |n| n == Number(F32Value(1015.)));
        interpreter = verify_whence(interpreter, "7779311:::to_f64()", |n| n == Number(F64Value(7779311.)));

        // signed-integer kinds
        interpreter = verify_whence(interpreter, "12345678987.43:::to_i128()", |n| n == Number(I128Value(12345678987)));
        interpreter = verify_whence(interpreter, "123456789.42:::to_i64()", |n| n == Number(I64Value(123456789)));
        interpreter = verify_whence(interpreter, "-765.65:::to_i32()", |n| n == Number(I32Value(-765)));
        interpreter = verify_whence(interpreter, "-567.311:::to_i16()", |n| n == Number(I16Value(-567)));
        interpreter = verify_whence(interpreter, "-125.089:::to_i8()", |n| n == Number(I8Value(-125)));

        // unsigned-integer kinds
        interpreter = verify_whence(interpreter, "12789.43:::to_u128()", |n| n == Number(U128Value(12789)));
        interpreter = verify_whence(interpreter, "12.3:::to_u64()", |n| n == Number(U64Value(12)));
        interpreter = verify_whence(interpreter, "765.65:::to_u32()", |n| n == Number(U32Value(765)));
        interpreter = verify_whence(interpreter, "567.311:::to_u16()", |n| n == Number(U16Value(567)));
        interpreter = verify_whence(interpreter, "125.089:::to_u8()", |n| n == Number(U8Value(125)));

        // scope checks
        let mut interpreter = Interpreter::new();

        // initially 'to_u8' should not be in scope
        assert_eq!(interpreter.get("to_u8"), None);
        assert_eq!(interpreter.get("to_u16"), None);
        assert_eq!(interpreter.get("to_u32"), None);
        assert_eq!(interpreter.get("to_u64"), None);
        assert_eq!(interpreter.get("to_u128"), None);

        // import all conversion members
        interpreter.evaluate("import util").unwrap();

        // after the import, 'to_u8' should be in scope
        assert_eq!(interpreter.get("to_u8"), Some(PlatformFunction(PlatformFunctions::UtilToU8)));
        assert_eq!(interpreter.get("to_u16"), Some(PlatformFunction(PlatformFunctions::UtilToU16)));
        assert_eq!(interpreter.get("to_u32"), Some(PlatformFunction(PlatformFunctions::UtilToU32)));
        assert_eq!(interpreter.get("to_u64"), Some(PlatformFunction(PlatformFunctions::UtilToU64)));
        assert_eq!(interpreter.get("to_u128"), Some(PlatformFunction(PlatformFunctions::UtilToU128)));
    }

    #[test]
    fn test_util_uuid() {
        verify_when("oxide::uuid()", |r| matches!(r, Number(U128Value(..))));
    }

    #[test]
    fn test_www_url_decode() {
        verify_exact(
            "www::url_decode('http%3A%2F%2Fshocktrade.com%3Fname%3Dthe%20hero%26t%3D9998')",
            StringValue("http://shocktrade.com?name=the hero&t=9998".to_string()))
    }

    #[test]
    fn test_www_url_encode() {
        verify_exact(
            "www::url_encode('http://shocktrade.com?name=the hero&t=9998')",
            StringValue("http%3A%2F%2Fshocktrade.com%3Fname%3Dthe%20hero%26t%3D9998".to_string()))
    }

    #[actix::test]
    async fn test_www_serve() {
        let mut interpreter = Interpreter::new();

        // set up a listener on port 8833
        let result = interpreter.evaluate_async(r#"
            www::serve(8833)
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
    fn generate_test_to_code() {
        // NOTE: this test generates the test cases for `test_to_code`
        let mut last_module: String = String::new();
        for pf in PLATFORM_OPCODES {
            if last_module != pf.get_package_name() {
                last_module = pf.get_package_name();
                println!("// {}", last_module)
            }
            println!("assert_eq!({:?}.to_code(), \"{}\");", pf, pf.to_code())
        }
    }

    #[test]
    fn test_to_code() {
        // io
        assert_eq!(IoFileCreate.to_code(), "io::create_file(a: String(), b: String())");
        assert_eq!(IoFileExists.to_code(), "io::exists(s: String())");
        assert_eq!(IoFileReadText.to_code(), "io::read_text_file(s: String())");
        assert_eq!(IoStdErr.to_code(), "io::stderr(s: String())");
        assert_eq!(IoStdOut.to_code(), "io::stdout(s: String())");
        // os
        assert_eq!(OsCall.to_code(), "os::call(s: String())");
        assert_eq!(OsClear.to_code(), "os::clear()");
        assert_eq!(OsCurrentDir.to_code(), "os::current_dir()");
        assert_eq!(OsEnv.to_code(), "os::env()");
        // oxide
        assert_eq!(OxideAssert.to_code(), "oxide::assert(b: Boolean)");
        assert_eq!(OxideEval.to_code(), "oxide::eval(s: String())");
        assert_eq!(OxideHelp.to_code(), "oxide::help()");
        assert_eq!(OxideHome.to_code(), "oxide::home()");
        assert_eq!(OxideMatches.to_code(), "oxide::matches(a, b)");
        assert_eq!(OxidePrintln.to_code(), "oxide::println(s: String())");
        assert_eq!(OxideReset.to_code(), "oxide::reset()");
        assert_eq!(OxideTypeOf.to_code(), "oxide::type_of(x)");
        assert_eq!(OxideUUID.to_code(), "oxide::uuid()");
        assert_eq!(OxideVersion.to_code(), "oxide::version()");
        // str
        assert_eq!(StrEndsWith.to_code(), "str::ends_with(a: String(), b: String())");
        assert_eq!(StrFormat.to_code(), "str::format(a: String(), b: String())");
        assert_eq!(StrIndexOf.to_code(), "str::index_of(a: String(), b: i64)");
        assert_eq!(StrJoin.to_code(), "str::join(a: Array<>, b: String())");
        assert_eq!(StrLeft.to_code(), "str::left(a: String(), b: i64)");
        assert_eq!(StrLen.to_code(), "str::len(n: u32)");
        assert_eq!(StrRight.to_code(), "str::right(a: String(), b: i64)");
        assert_eq!(StrSplit.to_code(), "str::split(a: String(), b: String())");
        assert_eq!(StrStartsWith.to_code(), "str::starts_with(a: String(), b: String())");
        assert_eq!(StrSubstring.to_code(), "str::substring(a: String(), b: i64, c: i64)");
        assert_eq!(StrToString.to_code(), "str::to_string(x)");
        // util
        assert_eq!(UtilCompact.to_code(), "util::compact(t: Table())");
        assert_eq!(UtilDescribe.to_code(), "util::describe(t: Table())");
        assert_eq!(UtilFetch.to_code(), "util::fetch(a: Table(), b: u64)");
        assert_eq!(UtilReverse.to_code(), "util::reverse(t: Table())");
        assert_eq!(UtilScan.to_code(), "util::scan(t: Table())");
        assert_eq!(UtilToArray.to_code(), "util::to_array(t: Table())");
        assert_eq!(UtilToCSV.to_code(), "util::to_csv(t: Table())");
        assert_eq!(UtilToJSON.to_code(), "util::to_json(t: Table())");
        assert_eq!(UtilToTable.to_code(), "util::to_table(x)");
        assert_eq!(UtilBase64.to_code(), "util::base64(x)");
        assert_eq!(UtilDateDay.to_code(), "util::day_of(d: Date)");
        assert_eq!(UtilDateHour12.to_code(), "util::hour12(d: Date)");
        assert_eq!(UtilDateHour24.to_code(), "util::hour24(d: Date)");
        assert_eq!(UtilDateMinute.to_code(), "util::minute_of(d: Date)");
        assert_eq!(UtilDateMonth.to_code(), "util::month_of(d: Date)");
        assert_eq!(UtilDateSecond.to_code(), "util::second_of(d: Date)");
        assert_eq!(UtilDateYear.to_code(), "util::year_of(d: Date)");
        assert_eq!(UtilMD5.to_code(), "util::md5(x)");
        assert_eq!(UtilNow.to_code(), "util::now()");
        assert_eq!(UtilToF32.to_code(), "util::to_f32(x)");
        assert_eq!(UtilToF64.to_code(), "util::to_f64(x)");
        assert_eq!(UtilToI8.to_code(), "util::to_i8(x)");
        assert_eq!(UtilToI16.to_code(), "util::to_i16(x)");
        assert_eq!(UtilToI32.to_code(), "util::to_i32(x)");
        assert_eq!(UtilToI64.to_code(), "util::to_i64(x)");
        assert_eq!(UtilToI128.to_code(), "util::to_i128(x)");
        assert_eq!(UtilToU8.to_code(), "util::to_u8(x)");
        assert_eq!(UtilToU16.to_code(), "util::to_u16(x)");
        assert_eq!(UtilToU32.to_code(), "util::to_u32(x)");
        assert_eq!(UtilToU64.to_code(), "util::to_u64(x)");
        assert_eq!(UtilToU128.to_code(), "util::to_u128(x)");
        // www
        assert_eq!(WwwURLDecode.to_code(), "www::url_decode(s: String())");
        assert_eq!(WwwURLEncode.to_code(), "www::url_encode(s: String())");
        assert_eq!(WwwServe.to_code(), "www::serve(n: u32)");
    }
}