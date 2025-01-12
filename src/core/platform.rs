#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Platform class
////////////////////////////////////////////////////////////////////

use crate::arrays::Array;
use crate::byte_code_compiler::ByteCodeCompiler;
use crate::columns::Column;
use crate::compiler::Compiler;
use crate::data_types::DataType;
use crate::data_types::DataType::*;

use crate::dataframe::Dataframe::{Disk, Model};
use crate::descriptor::Descriptor;
use crate::errors::Errors::*;
use crate::errors::TypeMismatchErrors::{ArgumentsMismatched, CollectionExpected, DateExpected, StringExpected, TableExpected, UnsupportedType};
use crate::expression::Expression::{CodeBlock, Literal, Scenario};
use crate::file_row_collection::FileRowCollection;
use crate::machine::Machine;
use crate::model_row_collection::ModelRowCollection;
use crate::namespaces::Namespace;
use crate::number_kind::NumberKind::*;
use crate::numbers::Numbers::*;
use crate::oxide_server;
use crate::parameter::Parameter;
use crate::platform::PlatformOps::*;
use crate::row_collection::RowCollection;
use crate::structures::Row;
use crate::structures::Structure;
use crate::structures::Structures::{Hard, Soft};
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::*;
use chrono::{Datelike, Local, TimeZone, Timelike};
use crossterm::style::Stylize;
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{Read, Write};
use std::ops::Deref;
use std::path::Path;
use uuid::Uuid;

pub const MAJOR_VERSION: u8 = 0;
pub const MINOR_VERSION: u8 = 3;
pub const VERSION: &str = "0.3";

/// Represents a Platform Function
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct PlatformFunction {
    pub package_name: String,
    pub name: String,
    pub description: String,
    pub example: String,
    pub parameters: Vec<Descriptor>,
    pub return_type: DataType,
    pub opcode: PlatformOps,
}

/// Represents an enumeration of Oxide Platform Functions
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum PlatformOps {
    // cal package
    CalDate,
    CalDateDay,
    CalDateHour12,
    CalDateHour24,
    CalDateMinute,
    CalDateMonth,
    CalDateSecond,
    CalDateYear,
    // io package
    IoFileCreate,
    IoFileExists,
    IoFileReadText,
    IoStdErr,
    IoStdOut,
    // kung-fu package
    KungFuAssert,
    KungFuFeature,
    KungFuMatches,
    KungFuTypeOf,
    // os package
    OsCall,
    OsClear,
    OsCurrentDir,
    OsEnv,
    // oxide package
    OxideCompile,
    OxideEval,
    OxideHelp,
    OxideHistory,
    OxideHome,
    OxidePrintln,
    OxideReset,
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
    // tools package
    ToolsCompact,
    ToolsDescribe,
    ToolsFetch,
    ToolsReverse,
    ToolsScan,
    ToolsToArray,
    ToolsToCSV,
    ToolsToJSON,
    ToolsToTable,
    // util package
    UtilBase64,
    UtilBinary,
    UtilHex,
    UtilMD5,
    UtilToASCII,
    UtilToDate,
    UtilToF32,
    UtilToF64,
    UtilToI8,
    UtilToI16,
    UtilToI32,
    UtilToI64,
    UtilToI128,
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

pub const PLATFORM_OPCODES: [PlatformOps; 71] = {
    use PlatformOps::*;
    [
        // cal
        CalDate, CalDateDay, CalDateHour12, CalDateHour24, CalDateMinute,
        CalDateMonth, CalDateSecond, CalDateYear,
        // io
        IoFileCreate, IoFileExists, IoFileReadText, IoStdErr, IoStdOut,
        // kung fu
        KungFuAssert, KungFuFeature, KungFuMatches, KungFuTypeOf,
        // os
        OsCall, OsClear, OsCurrentDir, OsEnv,
        // oxide
        OxideCompile, OxideEval, OxideHelp, OxideHistory, OxideHome,
        OxidePrintln, OxideReset, OxideUUID, OxideVersion,
        // str
        StrEndsWith, StrFormat, StrIndexOf, StrJoin, StrLeft, StrLen,
        StrRight, StrSplit, StrStartsWith, StrSubstring, StrToString,
        // tools
        ToolsCompact, ToolsDescribe, ToolsFetch, ToolsReverse, ToolsScan,
        ToolsToArray, ToolsToCSV, ToolsToJSON, ToolsToTable,
        // util
        UtilBase64, UtilBinary, UtilHex, UtilMD5, UtilToASCII, UtilToDate,
        UtilToF32, UtilToF64,
        UtilToI8, UtilToI16, UtilToI32, UtilToI64, UtilToI128,
        UtilToU8, UtilToU16, UtilToU32, UtilToU64, UtilToU128,
        // www
        WwwURLDecode, WwwURLEncode, WwwServe,
    ]
};

impl PlatformOps {
    /// Builds a mapping of package name to function vector
    pub fn build_packages() -> HashMap<String, Vec<PlatformOps>> {
        PLATFORM_OPCODES.iter()
            .fold(HashMap::new(), |mut hm, key| {
                hm.entry(key.get_package_name())
                    .or_insert_with(Vec::new)
                    .push(key.to_owned());
                hm
            })
    }

    pub fn decode(bytes: Vec<u8>) -> std::io::Result<PlatformOps> {
        ByteCodeCompiler::unwrap_as_result(bincode::deserialize(bytes.as_slice()))
    }

    pub fn encode(&self) -> std::io::Result<Vec<u8>> {
        ByteCodeCompiler::unwrap_as_result(bincode::serialize(self))
    }

    /// Evaluates the platform function
    pub fn evaluate(&self, ms: Machine, args: Vec<TypedValue>) -> (Machine, TypedValue) {
        match self {
            PlatformOps::CalDate => Self::do_cal_date(ms, args),
            PlatformOps::CalDateDay => self.adapter_fn1_pf(ms, args, Self::do_cal_date_part),
            PlatformOps::CalDateHour24 => self.adapter_fn1_pf(ms, args, Self::do_cal_date_part),
            PlatformOps::CalDateHour12 => self.adapter_fn1_pf(ms, args, Self::do_cal_date_part),
            PlatformOps::CalDateMinute => self.adapter_fn1_pf(ms, args, Self::do_cal_date_part),
            PlatformOps::CalDateMonth => self.adapter_fn1_pf(ms, args, Self::do_cal_date_part),
            PlatformOps::CalDateSecond => self.adapter_fn1_pf(ms, args, Self::do_cal_date_part),
            PlatformOps::CalDateYear => self.adapter_fn1_pf(ms, args, Self::do_cal_date_part),
            PlatformOps::IoFileCreate => self.adapter_fn2(ms, args, Self::do_io_create_file),
            PlatformOps::IoFileExists => self.adapter_fn1(ms, args, Self::do_io_exists),
            PlatformOps::IoFileReadText => self.adapter_fn1(ms, args, Self::do_io_read_text_file),
            PlatformOps::IoStdErr => self.adapter_fn1(ms, args, Self::do_io_stderr),
            PlatformOps::IoStdOut => self.adapter_fn1(ms, args, Self::do_io_stdout),
            PlatformOps::KungFuAssert => self.adapter_fn1(ms, args, Self::do_kung_fu_assert),
            PlatformOps::KungFuFeature => self.adapter_fn2(ms, args, Self::do_kung_fu_feature),
            PlatformOps::KungFuMatches => self.adapter_fn2(ms, args, Self::do_kung_fu_matches),
            PlatformOps::KungFuTypeOf => self.adapter_fn1(ms, args, Self::do_kung_fu_type_of),
            PlatformOps::OsCall => Self::do_os_call(ms, args),
            PlatformOps::OsCurrentDir => self.adapter_fn0(ms, args, Self::do_os_current_dir),
            PlatformOps::OsClear => self.adapter_fn0(ms, args, Self::do_os_clear_screen),
            PlatformOps::OsEnv => self.adapter_fn0(ms, args, Self::do_os_env),
            PlatformOps::OxideCompile => self.adapter_fn1(ms, args, Self::do_oxide_compile),
            PlatformOps::OxideEval => self.adapter_fn1(ms, args, Self::do_oxide_eval),
            PlatformOps::OxideHelp => self.adapter_fn0(ms, args, Self::do_oxide_help),
            PlatformOps::OxideHistory => Self::do_oxide_history(ms, args),
            PlatformOps::OxideHome => self.adapter_fn0(ms, args, Self::do_oxide_home),
            PlatformOps::OxidePrintln => self.adapter_fn1(ms, args, Self::do_io_stdout),
            PlatformOps::OxideReset => self.adapter_fn0(ms, args, Self::do_oxide_reset),
            PlatformOps::OxideUUID => Self::do_util_uuid(ms, args),
            PlatformOps::OxideVersion => self.adapter_fn0(ms, args, Self::do_oxide_version),
            PlatformOps::StrEndsWith => self.adapter_fn2(ms, args, Self::do_str_ends_with),
            PlatformOps::StrFormat => Self::do_str_format(ms, args),
            PlatformOps::StrIndexOf => self.adapter_fn2(ms, args, Self::do_str_index_of),
            PlatformOps::StrJoin => self.adapter_fn2(ms, args, Self::do_str_join),
            PlatformOps::StrLeft => self.adapter_fn2(ms, args, Self::do_str_left),
            PlatformOps::StrLen => self.adapter_fn1(ms, args, Self::do_str_len),
            PlatformOps::StrRight => self.adapter_fn2(ms, args, Self::do_str_right),
            PlatformOps::StrSplit => self.adapter_fn2(ms, args, Self::do_str_split),
            PlatformOps::StrStartsWith => self.adapter_fn2(ms, args, Self::do_str_start_with),
            PlatformOps::StrSubstring => self.adapter_fn3(ms, args, Self::do_str_substring),
            PlatformOps::StrToString => self.adapter_fn1(ms, args, Self::do_str_to_string),
            PlatformOps::ToolsCompact => self.adapter_fn1(ms, args, Self::do_tools_compact),
            PlatformOps::ToolsDescribe => self.adapter_fn1(ms, args, Self::do_tools_describe),
            PlatformOps::ToolsFetch => self.adapter_fn2(ms, args, Self::do_tools_fetch),
            PlatformOps::ToolsReverse => self.adapter_fn1(ms, args, Self::do_tools_reverse),
            PlatformOps::ToolsScan => self.adapter_fn1(ms, args, Self::do_tools_scan),
            PlatformOps::ToolsToArray => self.adapter_fn1(ms, args, Self::do_tools_to_array),
            PlatformOps::ToolsToCSV => self.adapter_fn1(ms, args, Self::do_tools_to_csv),
            PlatformOps::ToolsToJSON => self.adapter_fn1(ms, args, Self::do_tools_to_json),
            PlatformOps::ToolsToTable => self.adapter_fn1(ms, args, Self::do_tools_to_table),
            PlatformOps::UtilBase64 => self.adapter_fn1(ms, args, Self::do_util_base64),
            PlatformOps::UtilBinary => self.adapter_fn1(ms, args, Self::do_util_binary),
            PlatformOps::UtilMD5 => self.adapter_fn1(ms, args, Self::do_util_md5),
            PlatformOps::UtilToASCII => self.adapter_fn1(ms, args, Self::do_util_to_ascii),
            PlatformOps::UtilHex => self.adapter_fn1(ms, args, Self::do_util_to_hex),
            PlatformOps::UtilToDate => self.adapter_fn1_pf(ms, args, Self::do_util_numeric_conv),
            PlatformOps::UtilToF32 => self.adapter_fn1_pf(ms, args, Self::do_util_numeric_conv),
            PlatformOps::UtilToF64 => self.adapter_fn1_pf(ms, args, Self::do_util_numeric_conv),
            PlatformOps::UtilToI8 => self.adapter_fn1_pf(ms, args, Self::do_util_numeric_conv),
            PlatformOps::UtilToI16 => self.adapter_fn1_pf(ms, args, Self::do_util_numeric_conv),
            PlatformOps::UtilToI32 => self.adapter_fn1_pf(ms, args, Self::do_util_numeric_conv),
            PlatformOps::UtilToI64 => self.adapter_fn1_pf(ms, args, Self::do_util_numeric_conv),
            PlatformOps::UtilToI128 => self.adapter_fn1_pf(ms, args, Self::do_util_numeric_conv),
            PlatformOps::UtilToU8 => self.adapter_fn1_pf(ms, args, Self::do_util_numeric_conv),
            PlatformOps::UtilToU16 => self.adapter_fn1_pf(ms, args, Self::do_util_numeric_conv),
            PlatformOps::UtilToU32 => self.adapter_fn1_pf(ms, args, Self::do_util_numeric_conv),
            PlatformOps::UtilToU64 => self.adapter_fn1_pf(ms, args, Self::do_util_numeric_conv),
            PlatformOps::UtilToU128 => self.adapter_fn1_pf(ms, args, Self::do_util_numeric_conv),
            PlatformOps::WwwURLDecode => self.adapter_fn1(ms, args, Self::do_www_url_decode),
            PlatformOps::WwwURLEncode => self.adapter_fn1(ms, args, Self::do_www_url_encode),
            PlatformOps::WwwServe => self.adapter_fn1(ms, args, Self::do_www_serve),
        }
    }

    pub fn find_function(package: &str, name: &str) -> Option<PlatformFunction> {
        PLATFORM_OPCODES.iter()
            .map(|p| p.get_info())
            .find(|pfi| pfi.package_name == package && pfi.name == name)
    }

    pub fn get_description(&self) -> String {
        let result = match self {
            PlatformOps::CalDate => "Returns the current local date and time",
            PlatformOps::CalDateDay => "Returns the day of the month of a Date",
            PlatformOps::CalDateHour12 => "Returns the hour of the day of a Date",
            PlatformOps::CalDateHour24 => "Returns the hour (military time) of the day of a Date",
            PlatformOps::CalDateMinute => "Returns the minute of the hour of a Date",
            PlatformOps::CalDateMonth => "Returns the month of the year of a Date",
            PlatformOps::CalDateSecond => "Returns the seconds of the minute of a Date",
            PlatformOps::CalDateYear => "Returns the year of a Date",
            PlatformOps::IoFileCreate => "Creates a new file",
            PlatformOps::IoFileExists => "Returns true if the source path exists",
            PlatformOps::IoFileReadText => "Reads the contents of a text file into memory",
            PlatformOps::IoStdErr => "Writes a string to STDERR",
            PlatformOps::IoStdOut => "Writes a string to STDOUT",
            PlatformOps::KungFuAssert => "Evaluates an assertion returning Ack or an error",
            PlatformOps::KungFuFeature => "Creates a new test feature",
            PlatformOps::KungFuMatches => "Compares two values",
            PlatformOps::KungFuTypeOf => "Returns the type of a value",
            PlatformOps::OsCall => "Invokes an operating system application",
            PlatformOps::OsClear => "Clears the terminal/screen",
            PlatformOps::OsCurrentDir => "Returns the current directory",
            PlatformOps::OsEnv => "Returns a table of the OS environment variables",
            PlatformOps::OxideCompile => "Compiles source code from a string input",
            PlatformOps::OxidePrintln => "Print line function",
            PlatformOps::OxideEval => "Evaluates a string containing Oxide code",
            PlatformOps::OxideHelp => "Integrated help function",
            PlatformOps::OxideHistory => "Returns all commands successfully executed during the session",
            PlatformOps::OxideHome => "Returns the Oxide home directory path",
            PlatformOps::OxideReset => "Clears the scope of all user-defined objects",
            PlatformOps::OxideUUID => "Returns a random 128-bit UUID",
            PlatformOps::OxideVersion => "Returns the Oxide version",
            PlatformOps::StrEndsWith => "Returns true if string `a` ends with string `b`",
            PlatformOps::StrFormat => "Returns an argument-formatted string",
            PlatformOps::StrIndexOf => "Returns the index of string `b` in string `a`",
            PlatformOps::StrJoin => "Combines an array into a string",
            PlatformOps::StrLeft => "Returns n-characters from left-to-right",
            PlatformOps::StrLen => "Returns the number of characters contained in the string",
            PlatformOps::StrRight => "Returns n-characters from right-to-left",
            PlatformOps::StrSplit => "Splits string `a` by delimiter string `b`",
            PlatformOps::StrStartsWith => "Returns true if string `a` starts with string `b`",
            PlatformOps::StrSubstring => "Returns a substring of string `s` from `m` to `n`",
            PlatformOps::StrToString => "Converts a value to its text-based representation",
            PlatformOps::ToolsCompact => "Shrinks a table by removing deleted rows",
            PlatformOps::ToolsDescribe => "Describes a table or structure",
            PlatformOps::ToolsFetch => "Retrieves a raw structure from a table",
            PlatformOps::ToolsReverse => "Returns a reverse copy of a table, string or array",
            PlatformOps::ToolsScan => "Returns existence metadata for a table",
            PlatformOps::ToolsToArray => "Converts a collection into an array",
            PlatformOps::ToolsToCSV => "Converts a collection to CSV format",
            PlatformOps::ToolsToJSON => "Converts a collection to JSON format",
            PlatformOps::ToolsToTable => "Converts an object into a to_table",
            PlatformOps::UtilBase64 => "Translates bytes into Base 64",
            PlatformOps::UtilBinary => "Translates a numeric value into binary",
            PlatformOps::UtilHex => "Translates bytes into hexadecimal",
            PlatformOps::UtilMD5 => "Creates a MD5 digest",
            PlatformOps::UtilToASCII => "Converts an integer to ASCII",
            PlatformOps::UtilToDate => "Converts a value to Date",
            PlatformOps::UtilToF32 => "Converts a value to f32",
            PlatformOps::UtilToF64 => "Converts a value to f64",
            PlatformOps::UtilToI8 => "Converts a value to i8",
            PlatformOps::UtilToI16 => "Converts a value to i16",
            PlatformOps::UtilToI32 => "Converts a value to i32",
            PlatformOps::UtilToI64 => "Converts a value to i64",
            PlatformOps::UtilToI128 => "Converts a value to i128",
            PlatformOps::UtilToU8 => "Converts a value to u8",
            PlatformOps::UtilToU16 => "Converts a value to u16",
            PlatformOps::UtilToU32 => "Converts a value to u32",
            PlatformOps::UtilToU64 => "Converts a value to u64",
            PlatformOps::UtilToU128 => "Converts a value to u128",
            PlatformOps::WwwServe => "Starts a local HTTP service",
            PlatformOps::WwwURLDecode => "Decodes a URL-encoded string",
            PlatformOps::WwwURLEncode => "Encodes a URL string",
        };
        result.to_string()
    }

    pub fn get_example(&self) -> String {
        let result = match self {
            PlatformOps::CalDate => "cal::now()",
            PlatformOps::CalDateDay => r#"
                import cal
                now():::day_of()
            "#,
            PlatformOps::CalDateHour12 => r#"
                import cal
                now():::hour12()
            "#,
            PlatformOps::CalDateHour24 => r#"
                import cal
                now():::hour24()
            "#,
            PlatformOps::CalDateMinute => r#"
                import cal
                now():::minute_of()
            "#,
            PlatformOps::CalDateMonth => r#"
                import cal
                now():::month_of()
            "#,
            PlatformOps::CalDateSecond => r#"
                import cal
                now():::second_of()
            "#,
            PlatformOps::CalDateYear => r#"
                import cal
                now():::year_of()
            "#,
            PlatformOps::IoFileCreate => r#"
                io::create_file("quote.json", { symbol: "TRX", exchange: "NYSE", last_sale: 45.32 })
            "#,
            PlatformOps::IoFileExists => r#"
                io::exists("quote.json")
            "#,
            PlatformOps::IoFileReadText => r#"
                import io, util
                file := "temp_secret.txt"
                file:::create_file(md5("**keep**this**secret**"))
                file:::read_text_file()
            "#,
            PlatformOps::IoStdErr => r#"io::stderr("Goodbye Cruel World")"#,
            PlatformOps::IoStdOut => r#"io::stdout("Hello World")"#,
            PlatformOps::KungFuAssert => r#"
                import kungfu
                assert(matches([ 1 "a" "b" "c" ], [ 1 "a" "b" "c" ]))
            "#,
            PlatformOps::KungFuFeature => r#"
            import kungfu
            feature("Matches function", {
                "Compare Array contents: Equal": fn(ctx) => {
                    assert(matches(
                        [ 1 "a" "b" "c" ],
                        [ 1 "a" "b" "c" ]))
                },
                "Compare Array contents: Not Equal": fn(ctx) => {
                    assert(!matches(
                        [ 1 "a" "b" "c" ],
                        [ 0 "x" "y" "z" ]))
                },
                "Compare JSON contents (in sequence)": fn(ctx) => {
                    assert(matches(
                        { first: "Tom" last: "Lane" },
                        { first: "Tom" last: "Lane" }))
                },
                "Compare JSON contents (out of sequence)": fn(ctx) => {
                    assert(matches(
                        { scores: [82 78 99], id: "A1537" },
                        { id: "A1537", scores: [82 78 99] }))
                }
            })"#,
            PlatformOps::KungFuMatches => r#"
                import kungfu::matches
                a := { scores: [82, 78, 99], first: "Tom", last: "Lane" }
                b := { last: "Lane", first: "Tom", scores: [82, 78, 99] }
                matches(a, b)
            "#,
            PlatformOps::KungFuTypeOf => "kungfu::type_of([12, 76, 444])",
            PlatformOps::OsCall => r#"
                create table ns("platform.os.call") (
                    symbol: String(8),
                    exchange: String(8),
                    last_sale: f64
                )
                os::call("chmod", "777", oxide::home())
            "#,
            PlatformOps::OsClear => "os::clear()",
            PlatformOps::OsCurrentDir => r#"
                import str
                cur_dir := os::current_dir()
                prefix := iff(cur_dir:::ends_with("core"), "../..", ".")
                path_str := prefix + "/demoes/language/include_file.oxide"
                include path_str
            "#,
            PlatformOps::OsEnv => "os::env()",
            PlatformOps::OxideCompile => r#"
                code := oxide::compile("2 ** 4")
                code()
            "#,
            PlatformOps::OxidePrintln => r#"oxide::println("Hello World")"#,
            PlatformOps::OxideEval => r#"
                a := 'Hello '
                b := 'World'
                oxide::eval("a + b")
            "#,
            PlatformOps::OxideHelp => r#"from oxide::help() limit 3"#,
            PlatformOps::OxideHistory => "from oxide::history() limit 3",
            PlatformOps::OxideHome => "oxide::home()",
            PlatformOps::OxideReset => "oxide::reset()",
            PlatformOps::OxideUUID => "oxide::uuid()",
            PlatformOps::OxideVersion => "oxide::version()",
            PlatformOps::StrEndsWith => r#"str::ends_with('Hello World', 'World')"#,
            PlatformOps::StrFormat => r#"str::format("This {} the {}", "is", "way")"#,
            PlatformOps::StrIndexOf => r#"str::index_of('The little brown fox', 'brown')"#,
            PlatformOps::StrJoin => r#"str::join(['1', 5, 9, '13'], ', ')"#,
            PlatformOps::StrLeft => r#"str::left('Hello World', 5)"#,
            PlatformOps::StrLen => r#"str::len('The little brown fox')"#,
            PlatformOps::StrRight => "str::right('Hello World', 5)",
            PlatformOps::StrSplit => r#"str::split('Hello,there World', ' ,')"#,
            PlatformOps::StrStartsWith => "str::starts_with('Hello World', 'World')",
            PlatformOps::StrSubstring => "str::substring('Hello World', 0, 5)",
            PlatformOps::StrToString => "str::to_string(125.75)",
            PlatformOps::ToolsCompact => r#"
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
            "#,
            PlatformOps::ToolsDescribe => r#"
                tools::describe({ symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 })
            "#,
            PlatformOps::ToolsFetch => r#"
                [+] stocks := ns("platform.fetch.stocks")
                [+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                [+] [{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                     { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                     { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
                [+] tools::fetch(stocks, 2)
            "#,
            PlatformOps::ToolsReverse => r#"
                import tools
                to_table(reverse(['cat', 'dog', 'ferret', 'mouse']))
            "#,
            PlatformOps::ToolsScan => r#"
                [+] import tools
                [+] stocks := ns("platform.scan.stocks")
                [+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                [+] [{ symbol: "ABC", exchange: "AMEX", last_sale: 12.33 },
                     { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                     { symbol: "BIZ", exchange: "NYSE", last_sale: 9.775 },
                     { symbol: "GOTO", exchange: "OTC", last_sale: 0.1442 },
                     { symbol: "XYZ", exchange: "NYSE", last_sale: 0.0289 }] ~> stocks
                [+] delete from stocks where last_sale > 1.0
                [+] stocks:::scan()
            "#,
            PlatformOps::ToolsToArray => r#"tools::to_array("Hello")"#,
            PlatformOps::ToolsToCSV => r#"
                import tools::to_csv
                [+] stocks := ns("platform.csv.stocks")
                [+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                [+] [{ symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
                     { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                     { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                     { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                     { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }] ~> stocks
                stocks:::to_csv()
            "#,
            PlatformOps::ToolsToJSON => r#"
                import tools::to_json
                [+] stocks := ns("platform.json.stocks")
                [+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                [+] [{ symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
                     { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                     { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                     { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                     { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }] ~> stocks
                stocks:::to_json()
            "#,
            PlatformOps::ToolsToTable => r#"
                tools::to_table(['cat', 'dog', 'ferret', 'mouse'])
            "#,
            PlatformOps::UtilBase64 => "util::base64('Hello World')",
            PlatformOps::UtilBinary => "(0b1011 & 0b1101):::to_binary()",
            PlatformOps::UtilHex => "util::hex('Hello World')",
            PlatformOps::UtilMD5 => "util::md5('Hello World')",
            PlatformOps::UtilToASCII => "util::to_ascii(177)",
            PlatformOps::UtilToDate => "util::to_date(177)",
            PlatformOps::UtilToF32 => "util::to_f32(4321)",
            PlatformOps::UtilToF64 => "util::to_f64(4321)",
            PlatformOps::UtilToI8 => "util::to_i8(88)",
            PlatformOps::UtilToI16 => "util::to_i16(88)",
            PlatformOps::UtilToI32 => "util::to_i32(88)",
            PlatformOps::UtilToI64 => "util::to_i64(88)",
            PlatformOps::UtilToI128 => "util::to_i128(88)",
            PlatformOps::UtilToU8 => "util::to_u8(88)",
            PlatformOps::UtilToU16 => "util::to_u16(88)",
            PlatformOps::UtilToU32 => "util::to_u32(88)",
            PlatformOps::UtilToU64 => "util::to_u64(88)",
            PlatformOps::UtilToU128 => "util::to_u128(88)",
            PlatformOps::WwwServe => r#"
                [+] www::serve(8822)
                [+] stocks := ns("platform.www.quotes")
                [+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                [+] [{ symbol: "XINU", exchange: "NYSE", last_sale: 8.11 },
                     { symbol: "BOX", exchange: "NYSE", last_sale: 56.88 },
                     { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 },
                     { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                     { symbol: "MIU", exchange: "OTCBB", last_sale: 2.24 }] ~> stocks
                GET "http://localhost:8822/platform/www/quotes/1/4"
            "#,
            PlatformOps::WwwURLDecode => "www::url_decode('http%3A%2F%2Fshocktrade.com%3Fname%3Dthe%20hero%26t%3D9998')",
            PlatformOps::WwwURLEncode => "www::url_encode('http://shocktrade.com?name=the hero&t=9998')",
        };
        result.to_string()
    }

    pub fn get_info(&self) -> PlatformFunction {
        PlatformFunction {
            name: self.get_name(),
            description: self.get_description(),
            example: self.get_example(),
            package_name: self.get_package_name(),
            parameters: self.get_parameters(),
            return_type: self.get_return_type(),
            opcode: self.to_owned(),
        }
    }

    pub fn get_name(&self) -> String {
        let result = match self {
            PlatformOps::CalDate => "now",
            PlatformOps::CalDateDay => "day_of",
            PlatformOps::CalDateHour12 => "hour12",
            PlatformOps::CalDateHour24 => "hour24",
            PlatformOps::CalDateMinute => "minute_of",
            PlatformOps::CalDateMonth => "month_of",
            PlatformOps::CalDateSecond => "second_of",
            PlatformOps::CalDateYear => "year_of",
            PlatformOps::IoFileCreate => "create_file",
            PlatformOps::IoFileExists => "exists",
            PlatformOps::IoFileReadText => "read_text_file",
            PlatformOps::IoStdErr => "stderr",
            PlatformOps::IoStdOut => "stdout",
            PlatformOps::KungFuAssert => "assert",
            PlatformOps::KungFuFeature => "feature",
            PlatformOps::KungFuMatches => "matches",
            PlatformOps::OsCall => "call",
            PlatformOps::OsClear => "clear",
            PlatformOps::OsCurrentDir => "current_dir",
            PlatformOps::OsEnv => "env",
            PlatformOps::OxideCompile => "compile",
            PlatformOps::OxideEval => "eval",
            PlatformOps::OxideHelp => "help",
            PlatformOps::OxideHistory => "history",
            PlatformOps::OxideHome => "home",
            PlatformOps::OxidePrintln => "println",
            PlatformOps::OxideReset => "reset",
            PlatformOps::KungFuTypeOf => "type_of",
            PlatformOps::OxideUUID => "uuid",
            PlatformOps::OxideVersion => "version",
            PlatformOps::StrEndsWith => "ends_with",
            PlatformOps::StrFormat => "format",
            PlatformOps::StrIndexOf => "index_of",
            PlatformOps::StrJoin => "join",
            PlatformOps::StrLeft => "left",
            PlatformOps::StrLen => "len",
            PlatformOps::StrRight => "right",
            PlatformOps::StrSplit => "split",
            PlatformOps::StrStartsWith => "starts_with",
            PlatformOps::StrSubstring => "substring",
            PlatformOps::StrToString => "to_string",
            PlatformOps::ToolsCompact => "compact",
            PlatformOps::ToolsDescribe => "describe",
            PlatformOps::ToolsFetch => "fetch",
            PlatformOps::ToolsReverse => "reverse",
            PlatformOps::ToolsScan => "scan",
            PlatformOps::ToolsToArray => "to_array",
            PlatformOps::ToolsToCSV => "to_csv",
            PlatformOps::ToolsToJSON => "to_json",
            PlatformOps::ToolsToTable => "to_table",
            PlatformOps::UtilBase64 => "base64",
            PlatformOps::UtilBinary => "to_binary",
            PlatformOps::UtilHex => "hex",
            PlatformOps::UtilMD5 => "md5",
            PlatformOps::UtilToASCII => "to_ascii",
            PlatformOps::UtilToDate => "to_date",
            PlatformOps::UtilToF32 => "to_f32",
            PlatformOps::UtilToF64 => "to_f64",
            PlatformOps::UtilToI8 => "to_i8",
            PlatformOps::UtilToI16 => "to_i16",
            PlatformOps::UtilToI32 => "to_i32",
            PlatformOps::UtilToI64 => "to_i64",
            PlatformOps::UtilToI128 => "to_i128",
            PlatformOps::UtilToU8 => "to_u8",
            PlatformOps::UtilToU16 => "to_u16",
            PlatformOps::UtilToU32 => "to_u32",
            PlatformOps::UtilToU64 => "to_u64",
            PlatformOps::UtilToU128 => "to_u128",
            PlatformOps::WwwServe => "serve",
            PlatformOps::WwwURLDecode => "url_decode",
            PlatformOps::WwwURLEncode => "url_encode",
        };
        result.to_string()
    }

    pub fn get_package_name(&self) -> String {
        use PlatformOps::*;
        let result = match self {
            // cal
            CalDateDay | CalDateHour12 | CalDateHour24 | CalDateMinute |
            CalDateMonth | CalDateSecond | CalDateYear | CalDate => "cal",
            // io
            IoFileCreate | IoFileExists | IoFileReadText | IoStdErr | IoStdOut => "io",
            // kung fu
            KungFuAssert | KungFuFeature | KungFuMatches | KungFuTypeOf => "kungfu",
            // oxide
            OxideCompile | OxideEval | OxideHelp | OxideHistory |
            OxideHome | OxidePrintln | OxideReset | OxideUUID | OxideVersion => "oxide",
            // os
            OsCall | OsClear | OsCurrentDir | OsEnv => "os",
            // str
            StrEndsWith | StrFormat | StrIndexOf | StrJoin |
            StrLeft | StrLen | StrRight | StrSplit |
            StrStartsWith | StrSubstring | StrToString => "str",
            // tools
            ToolsCompact | ToolsDescribe | ToolsFetch | ToolsReverse | ToolsScan |
            ToolsToArray | ToolsToCSV | ToolsToJSON | ToolsToTable => "tools",
            // util
            UtilBase64 | UtilBinary | UtilHex | UtilMD5 | UtilToASCII | UtilToDate |
            UtilToF32 | UtilToF64 |
            UtilToI8 | UtilToI16 | UtilToI32 | UtilToI64 | UtilToI128 |
            UtilToU8 | UtilToU16 | UtilToU32 | UtilToU64 | UtilToU128 => "util",
            // www
            WwwURLDecode | WwwURLEncode | WwwServe => "www",
        };
        result.to_string()
    }

    pub fn get_parameter_types(&self) -> Vec<DataType> {
        use PlatformOps::*;
        match self {
            // zero-parameter
            OsCurrentDir | OsEnv | OxideHome | OxideReset | OsClear |
            OxideHelp | OxideHistory | OxideVersion | CalDate | OxideUUID
            => vec![],
            // single-parameter (boolean)
            KungFuAssert
            => vec![BooleanType],
            // single-parameter (date)
            CalDateDay | CalDateHour12 | CalDateHour24 | CalDateMinute |
            CalDateMonth | CalDateSecond | CalDateYear
            => vec![NumberType(DateKind)],
            // single-parameter (int)
            UtilToASCII | WwwServe
            => vec![NumberType(U32Kind)],
            // single-parameter (lazy)
            KungFuTypeOf | StrToString | UtilBase64 | UtilBinary | UtilHex | UtilMD5 | UtilToDate |
            UtilToF32 | UtilToF64 | UtilToI8 | UtilToI16 | UtilToI32 | UtilToI64 | UtilToI128 |
            ToolsToTable | UtilToU8 | UtilToU16 | UtilToU32 | UtilToU64 | UtilToU128
            => vec![VaryingType(vec![])],
            // single-parameter (string)
            IoFileExists | IoFileReadText | IoStdErr | IoStdOut | OxidePrintln | OsCall |
            OxideCompile | OxideEval | StrLen | WwwURLDecode | WwwURLEncode
            => vec![StringType(0)],
            // single-parameter (table)
            ToolsCompact | ToolsDescribe | ToolsReverse | ToolsScan |
            ToolsToArray | ToolsToCSV | ToolsToJSON
            => vec![TableType(Vec::new(), 0)],
            // two-parameter (lazy, lazy)
            KungFuMatches
            => vec![VaryingType(vec![]), VaryingType(vec![])],
            // two-parameter (string, string)
            IoFileCreate | StrEndsWith | StrFormat | StrSplit | StrStartsWith
            => vec![StringType(0), StringType(0)],
            // two-parameter (string, i64)
            StrIndexOf | StrLeft | StrRight
            => vec![StringType(0), NumberType(I64Kind)],
            // two-parameter (string, struct)
            KungFuFeature
            => vec![StringType(0), StructureType(vec![])],
            // two-parameter (table, u64)
            ToolsFetch
            => vec![TableType(vec![], 0), NumberType(U64Kind)],
            // two-parameter (array, string)
            StrJoin
            => vec![ArrayType(0), StringType(0)],
            // three-parameter (string, i64, i64)
            StrSubstring
            => vec![StringType(0), NumberType(I64Kind), NumberType(I64Kind)],
        }
    }

    pub fn get_parameters(&self) -> Vec<Descriptor> {
        let names = match self.get_parameter_types().as_slice() {
            [BooleanType] => vec!['b'],
            [VaryingType(..)] => vec!['x'],
            [NumberType(..)] => vec!['n'],
            [StringType(..)] => vec!['s'],
            [StringType(..), NumberType(..)] => vec!['s', 'n'],
            [TableType(..)] => vec!['t'],
            [TableType(..), NumberType(..)] => vec!['t', 'n'],
            [StringType(..), NumberType(..), NumberType(..)] => vec!['s', 'm', 'n'],
            params => params.iter().enumerate()
                .map(|(n, _)| (n as u8 + b'a') as char)
                .collect()
        };

        names.iter().zip(self.get_parameter_types().iter()).enumerate()
            .map(|(n, (name, dt))|
                Descriptor::new(
                    name.to_string(),
                    dt.to_type_declaration(),
                    None,
                ))
            .collect()
    }

    pub fn get_return_type(&self) -> DataType {
        use PlatformOps::*;
        match self {
            // array
            ToolsToArray => ArrayType(0),
            IoFileReadText | StrSplit | ToolsToCSV | ToolsToJSON => ArrayType(0),
            // boolean
            IoFileExists | KungFuMatches | StrEndsWith | StrStartsWith => BooleanType,
            // bytes
            UtilMD5 => BinaryType(16),
            // date
            CalDate | UtilToDate => NumberType(DateKind),
            // f64
            OxideVersion => NumberType(F64Kind),
            // function
            OxideCompile => FunctionType(vec![]),
            // number
            StrIndexOf | StrLen => NumberType(I64Kind),
            CalDateDay | CalDateHour12 | CalDateHour24 | CalDateMinute |
            CalDateMonth | CalDateSecond => NumberType(U32Kind),
            UtilToF32 => NumberType(F32Kind),
            UtilToF64 => NumberType(F64Kind),
            UtilToI8 => NumberType(I8Kind),
            UtilToI16 => NumberType(I16Kind),
            UtilToI32 | CalDateYear => NumberType(I32Kind),
            UtilToI64 => NumberType(I64Kind),
            UtilToI128 => NumberType(I128Kind),
            UtilToU8 => NumberType(U8Kind),
            UtilToU16 => NumberType(U16Kind),
            UtilToU32 => NumberType(U32Kind),
            UtilToU64 => NumberType(U64Kind),
            UtilToU128 | OxideUUID => NumberType(U128Kind),
            // outcome
            IoFileCreate | KungFuAssert | OxidePrintln |
            OsClear | OxideReset | WwwServe => NumberType(AckKind),
            // string
            IoStdErr | IoStdOut | KungFuTypeOf | OsCall | OsCurrentDir |
            OxideEval | OxideHome | StrFormat | StrJoin | StrLeft | StrRight |
            StrSubstring | StrToString | UtilBase64 | UtilBinary | UtilToASCII | UtilHex |
            WwwURLDecode | WwwURLEncode => StringType(0),
            // table
            KungFuFeature => TableType(Self::get_kung_fu_feature_parameters(), 0),
            OsEnv => TableType(Self::get_os_env_parameters(), 0),
            OxideHelp => TableType(Self::get_oxide_help_parameters(), 0),
            OxideHistory => TableType(Self::get_oxide_history_parameters(), 0),
            ToolsCompact | ToolsFetch | ToolsReverse | ToolsScan |
            ToolsToTable => TableType(Vec::new(), 0),
            ToolsDescribe => TableType(Self::get_tools_describe_parameters(), 0),
        }
    }

    pub fn get_type(&self) -> DataType {
        PlatformOpsType(self.clone())
    }

    pub fn to_code(&self) -> String {
        self.to_code_with_params(&self.get_parameters())
    }

    pub fn to_code_with_params(&self, parameters: &Vec<Descriptor>) -> String {
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
            n => (ms, ErrorValue(TypeMismatch(ArgumentsMismatched(0, n))))
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
            args => (ms, ErrorValue(TypeMismatch(ArgumentsMismatched(1, args.len()))))
        }
    }

    fn adapter_fn1_pf(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
        f: fn(Machine, &TypedValue, &PlatformOps) -> (Machine, TypedValue),
    ) -> (Machine, TypedValue) {
        match args.as_slice() {
            [a] => f(ms, a, self),
            args => (ms, ErrorValue(TypeMismatch(ArgumentsMismatched(1, args.len()))))
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
            args => (ms, ErrorValue(TypeMismatch(ArgumentsMismatched(2, args.len()))))
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
            args => (ms, ErrorValue(TypeMismatch(ArgumentsMismatched(3, args.len()))))
        }
    }

    fn do_cal_date(ms: Machine, args: Vec<TypedValue>) -> (Machine, TypedValue) {
        if !args.is_empty() {
            return (ms, ErrorValue(Exact(format!("No arguments expected, but found {}", args.len()))));
        }
        (ms, Number(DateValue(Local::now().timestamp_millis())))
    }

    fn do_cal_date_part(
        ms: Machine,
        value: &TypedValue,
        plat: &PlatformOps,
    ) -> (Machine, TypedValue) {
        match value {
            Number(DateValue(epoch_millis)) => {
                let datetime = {
                    let seconds = epoch_millis / 1000;
                    let millis_part = epoch_millis % 1000;
                    Local.timestamp(seconds, (millis_part * 1_000_000) as u32)
                };
                match plat {
                    PlatformOps::CalDateDay => (ms, Number(U32Value(datetime.day()))),
                    PlatformOps::CalDateHour12 => (ms, Number(U32Value(datetime.hour12().1))),
                    PlatformOps::CalDateHour24 => (ms, Number(U32Value(datetime.hour()))),
                    PlatformOps::CalDateMinute => (ms, Number(U32Value(datetime.minute()))),
                    PlatformOps::CalDateMonth => (ms, Number(U32Value(datetime.second()))),
                    PlatformOps::CalDateSecond => (ms, Number(U32Value(datetime.second()))),
                    PlatformOps::CalDateYear => (ms, Number(I32Value(datetime.year()))),
                    pf => (ms, ErrorValue(PlatformOpError(pf.to_owned())))
                }
            }
            other => (ms, ErrorValue(TypeMismatch(DateExpected(other.to_code()))))
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
                    let n_bytes = file.write(contents_v.unwrap_value().as_bytes())? as u64;
                    Ok((ms, Number(U64Value(n_bytes))))
                }
                other => Ok((ms, ErrorValue(TypeMismatch(StringExpected(other.to_code())))))
            }
        }

        // create or overwrite the file
        TypedValue::from_results(create_file(ms, path_v, contents_v))
    }

    fn do_io_exists(ms: Machine, path_value: &TypedValue) -> (Machine, TypedValue) {
        match path_value {
            StringValue(path) => (ms, Boolean(Path::new(path).exists())),
            other => (ms, ErrorValue(TypeMismatch(StringExpected(other.to_string()))))
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
                other => Ok((ms, ErrorValue(TypeMismatch(StringExpected(other.to_code())))))
            }
        }

        // create or overwrite the file
        TypedValue::from_results(read_file(ms, path_v))
    }

    fn do_io_stderr(ms: Machine, value: &TypedValue) -> (Machine, TypedValue) {
        eprintln!("{}", value.unwrap_value());
        (ms, Number(Ack))
    }

    fn do_io_stdout(ms: Machine, value: &TypedValue) -> (Machine, TypedValue) {
        println!("{}", value.unwrap_value());
        (ms, Number(Ack))
    }

    fn do_kung_fu_assert(ms: Machine, value: &TypedValue) -> (Machine, TypedValue) {
        match value {
            ErrorValue(msg) => (ms, ErrorValue(msg.to_owned())),
            Boolean(false) => (ms, ErrorValue(AssertionError("true".to_string(), "false".to_string()))),
            z => (ms, z.to_owned())
        }
    }

    fn do_kung_fu_feature(
        ms: Machine,
        title: &TypedValue,
        body: &TypedValue,
    ) -> (Machine, TypedValue) {
        // get the feature title
        let title = match title {
            StringValue(s) => Literal(StringValue(s.to_string())),
            other => return (ms, ErrorValue(TypeMismatch(UnsupportedType(StringType(0), other.get_type()))))
        };

        // get the feature scenarios
        let scenarios = match body {
            Structured(Soft(ss)) => {
                ss.get_tuples().iter().map(|(title, tv)| match tv {
                    Function { code, .. } =>
                        Scenario {
                            title: Box::new(Literal(StringValue(title.to_string()))),
                            verifications: match code.deref() {
                                CodeBlock(ops) => ops.clone(),
                                other => vec![other.clone()]
                            },
                        },
                    other => Literal(other.clone())
                }).collect::<Vec<_>>()
            }
            other => return (ms, ErrorValue(TypeMismatch(UnsupportedType(
                ArrayType(0), other.get_type()))))
        };

        // execute the feature
        TypedValue::from_results(ms.do_feature(&Box::from(title), &scenarios))
    }

    fn do_kung_fu_matches(ms: Machine, a: &TypedValue, b: &TypedValue) -> (Machine, TypedValue) {
        (ms, a.matches(b))
    }

    fn do_kung_fu_type_of(ms: Machine, a: &TypedValue) -> (Machine, TypedValue) {
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
            (ms, ErrorValue(TypeMismatch(CollectionExpected(args.iter()
                .map(|e| e.to_code())
                .collect::<Vec<_>>()
                .join(", ")))))
        }
    }

    fn do_os_clear_screen(ms: Machine) -> (Machine, TypedValue) {
        print!("\x1B[2J\x1B[H");
        match std::io::stdout().flush() {
            Ok(_) => (ms, Number(Ack)),
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
        let mut mrc = ModelRowCollection::from_descriptors(&vec![
            Descriptor::new("key", Some("String(256)".into()), None),
            Descriptor::new("value", Some("String(8192)".into()), None)
        ]);
        for (key, value) in env::vars() {
            if let ErrorValue(err) = mrc.append_row(Row::new(0, vec![
                StringValue(key), StringValue(value)
            ])) {
                return (ms, ErrorValue(err));
            }
        }

        (ms, TableValue(Model(mrc)))
    }

    fn do_oxide_compile(ms: Machine, value: &TypedValue) -> (Machine, TypedValue) {
        match value {
            StringValue(source) => {
                match Compiler::build(source) {
                    Ok(code) => (ms, Function {
                        params: vec![],
                        code: Box::new(code),
                    }),
                    Err(err) => (ms, ErrorValue(Exact(err.to_string()))),
                }
            }
            z => (ms, ErrorValue(TypeMismatch(UnsupportedType(StringType(0), z.get_type()))))
        }
    }

    fn do_oxide_eval(ms: Machine, query_value: &TypedValue) -> (Machine, TypedValue) {
        match query_value {
            StringValue(ql) =>
                match Compiler::build(ql.as_str()) {
                    Ok(opcode) =>
                        match ms.evaluate(&opcode) {
                            Ok((machine, tv)) => (machine, tv),
                            Err(err) => (ms, ErrorValue(Exact(err.to_string())))
                        }
                    Err(err) => (ms, ErrorValue(Exact(err.to_string())))
                }
            x => (ms, ErrorValue(TypeMismatch(StringExpected(x.get_type_name()))))
        }
    }

    /// returns a table describing all modules
    fn do_oxide_help(ms: Machine) -> (Machine, TypedValue) {
        let mut mrc = ModelRowCollection::from_parameters(&Self::get_oxide_help_parameters());
        for (module_name, module) in ms.get_variables().iter() {
            match module {
                Structured(Hard(mod_struct)) =>
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
                                PlatformOp(pf) => StringValue(pf.get_description()),
                                _ => Null,
                            },
                            // returns
                            match func {
                                PlatformOp(pf) => StringValue(pf.get_return_type().to_type_declaration().unwrap_or("".into())),
                                _ => Null,
                            }
                        ]));
                    }
                _ => {}
            }
        }
        (ms, TableValue(Model(mrc)))
    }

    fn do_oxide_history(ms: Machine, args: Vec<TypedValue>) -> (Machine, TypedValue) {
        // re-executes a saved command
        fn re_run(ms: Machine, pid: usize) -> std::io::Result<(Machine, TypedValue)> {
            let frc = FileRowCollection::open_or_create(&PlatformOps::get_oxide_history_ns())?;
            let row_maybe = frc.read_one(pid)?;
            let code = row_maybe.map(|r| r.get_values().last()
                .map(|v| v.unwrap_value()).unwrap_or(String::new())
            ).unwrap_or(String::new());
            for line in code.split(|c| c == ';').collect::<Vec<_>>() {
                println!(">>> {}", line);
            }
            let model = Compiler::build(code.as_str())?;
            ms.evaluate(&model)
        }

        // evaluate based on the arguments
        match args.as_slice() {
            // history()
            [] =>
                match FileRowCollection::open_or_create(&Self::get_oxide_history_ns()) {
                    Ok(frc) => (ms, TableValue(Disk(frc))),
                    Err(err) => (ms, ErrorValue(Exact(err.to_string())))
                }
            // history(11)
            [Number(pid)] =>
                match re_run(ms.to_owned(), pid.to_usize()) {
                    Ok(result) => result,
                    Err(err) => (ms, ErrorValue(Exact(err.to_string())))
                }
            // history(..)
            other => (ms, ErrorValue(TypeMismatch(ArgumentsMismatched(other.len(), 1))))
        }
    }

    fn do_oxide_home(ms: Machine) -> (Machine, TypedValue) {
        (ms, StringValue(Machine::oxide_home()))
    }

    fn do_oxide_reset(_ms: Machine) -> (Machine, TypedValue) {
        (Machine::new_platform(), Number(Ack))
    }

    fn do_oxide_version(ms: Machine) -> (Machine, TypedValue) {
        (ms, StringValue(format!("{MAJOR_VERSION}.{MINOR_VERSION}")))
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
                    z => (ms, ErrorValue(TypeMismatch(StringExpected(z.to_code()))))
                }
            z => (ms, ErrorValue(TypeMismatch(StringExpected(z.to_code()))))
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
        if args.is_empty() { (ms, StringValue(String::new())) } else {
            match (args[0].to_owned(), args[1..].to_owned()) {
                (StringValue(format_str), format_args) =>
                    format_text(ms, format_str, format_args),
                (other, ..) =>
                    (ms, ErrorValue(TypeMismatch(StringExpected(other.to_code()))))
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
                    z => (ms, ErrorValue(TypeMismatch(UnsupportedType(StringType(0), z.get_type()))))
                }
            }
            z => (ms, ErrorValue(TypeMismatch(UnsupportedType(StringType(0), z.get_type()))))
        }
    }

    /// str::join(\["a", "b", "c"], ", ") => "a, b, c"
    fn do_str_join(
        ms: Machine,
        array: &TypedValue,
        delim: &TypedValue,
    ) -> (Machine, TypedValue) {
        match array {
            ArrayValue(items) => {
                let mut buf = String::new();
                for item in items.iter() {
                    if !buf.is_empty() { buf.extend(delim.unwrap_value().chars()) }
                    buf.extend(item.unwrap_value().chars());
                }
                (ms, StringValue(buf))
            }
            z =>
                (ms, ErrorValue(TypeMismatch(UnsupportedType(ArrayType(0), z.get_type()))))
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
                    StringValue(delimiters) => {
                        let pcs = src.split(|c| delimiters.contains(c))
                            .map(|s| StringValue(s.to_string()))
                            .collect::<Vec<_>>();
                        (ms, ArrayValue(Array::from(pcs)))
                    }
                    z => (ms, ErrorValue(TypeMismatch(StringExpected(z.to_code()))))
                }
            }
            z => (ms, ErrorValue(TypeMismatch(StringExpected(z.to_code()))))
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
                    z => (ms, ErrorValue(TypeMismatch(StringExpected(z.to_code()))))
                }
            }
            z => (ms, ErrorValue(TypeMismatch(StringExpected(z.to_code()))))
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

    fn do_tools_compact(ms: Machine, table: &TypedValue) -> (Machine, TypedValue) {
        match table {
            ErrorValue(err) => (ms, ErrorValue(err.to_owned())),
            NamespaceValue(ns) => {
                match FileRowCollection::open(&ns) {
                    Ok(mut frc) => (ms, frc.compact()),
                    Err(err) => (ms, ErrorValue(Exact(err.to_string())))
                }
            }
            TableValue(rcv) => (ms, rcv.to_owned().compact()),
            z => (ms, ErrorValue(TypeMismatch(CollectionExpected(z.to_code()))))
        }
    }

    fn do_tools_describe(
        ms: Machine,
        item: &TypedValue,
    ) -> (Machine, TypedValue) {
        match item {
            ErrorValue(message) => (ms, ErrorValue(message.to_owned())),
            NamespaceValue(ns) =>
                match FileRowCollection::open(&ns) {
                    Ok(frc) => (ms, frc.describe()),
                    Err(err) => (ms, ErrorValue(Exact(err.to_string())))
                }
            Structured(Hard(sh)) => (ms, sh.to_table().describe()),
            Structured(Soft(ss)) => (ms, ss.to_table().describe()),
            TableValue(rcv) => (ms, rcv.describe()),
            other =>
                (ms, ErrorValue(TypeMismatch(TableExpected("table or struct".to_string(), other.to_code()))))
        }
    }

    /// Retrieves a raw structure from a table
    /// ex: util::fetch(stocks, 5)
    /// ex: stocks:::fetch(5)
    fn do_tools_fetch(
        ms: Machine,
        table: &TypedValue,
        row_offset: &TypedValue,
    ) -> (Machine, TypedValue) {
        let offset = row_offset.to_usize();
        match table {
            NamespaceValue(ns) =>
                match FileRowCollection::open(&ns) {
                    Ok(frc) =>
                        match frc.read_row(offset) {
                            Ok((row, _)) => (ms, TableValue(Model(
                                ModelRowCollection::from_columns_and_rows(frc.get_columns(), &vec![row])
                            ))),
                            Err(err) => (ms, ErrorValue(Exact(err.to_string())))
                        }
                    Err(err) => (ms, ErrorValue(Exact(err.to_string())))
                }
            TableValue(rcv) => {
                let columns = rcv.get_columns();
                match rcv.read_row(offset) {
                    Ok((row, _)) =>
                        (ms, TableValue(Model(ModelRowCollection::from_columns_and_rows(columns, &vec![row])))),
                    Err(err) => (ms, ErrorValue(Exact(err.to_string())))
                }
            }
            other =>
                (ms, ErrorValue(TypeMismatch(TableExpected("[Table(), Struct()]".to_string(), other.to_code()))))
        }
    }

    fn do_tools_reverse(ms: Machine, value: &TypedValue) -> (Machine, TypedValue) {
        match value {
            ArrayValue(a) => (ms, ArrayValue(a.rev())),
            NamespaceValue(ns) =>
                (ms.clone(), match Self::open_namespace(&ns) {
                    TableValue(rcv) => rcv.reverse_table_value(),
                    other => ErrorValue(TypeMismatch(UnsupportedType(TableType(vec![], 0), other.get_type())))
                }),
            StringValue(s) => (ms, StringValue(s.chars().rev().collect())),
            TableValue(rcv) => (ms, rcv.reverse_table_value()),
            other => (ms, ErrorValue(TypeMismatch(UnsupportedType(
                VaryingType(vec![
                    ArrayType(0),
                    StringType(0),
                    TableType(vec![], 0)
                ]),
                other.get_type(),
            ))))
        }
    }

    fn do_tools_scan(ms: Machine, tv_table: &TypedValue) -> (Machine, TypedValue) {
        match tv_table.to_table_value() {
            ErrorValue(err) => (ms, ErrorValue(err)),
            TableValue(df) => match df.examine_rows() {
                Ok(rows) => {
                    let columns = rows.first()
                        .map(|row| df.get_columns().to_owned())
                        .unwrap_or(Vec::new());
                    let mrc = ModelRowCollection::from_columns_and_rows(&columns, &rows);
                    (ms, TableValue(Model(mrc)))
                }
                Err(err) => (ms, ErrorValue(Exact(err.to_string())))
            }
            other => (ms, ErrorValue(TypeMismatch(UnsupportedType(TableType(vec![], 0), other.get_type()))))
        }
    }

    fn do_tools_to_array(ms: Machine, value: &TypedValue) -> (Machine, TypedValue) {
        (ms, value.to_array())
    }

    fn do_tools_to_csv(ms: Machine, value: &TypedValue) -> (Machine, TypedValue) {
        Self::do_tools_to_csv_or_json(ms, value, true)
    }

    fn do_tools_to_csv_or_json(
        ms: Machine,
        value: &TypedValue,
        is_csv: bool,
    ) -> (Machine, TypedValue) {
        fn convert_to_csv(
            ms: Machine,
            rc: Box<dyn RowCollection>,
        ) -> (Machine, TypedValue) {
            (ms, ArrayValue(Array::from(rc.iter()
                .map(|row| StringValue(row.to_csv()))
                .collect::<Vec<_>>())))
        }

        fn convert_to_json(
            ms: Machine,
            rc: Box<dyn RowCollection>,
        ) -> (Machine, TypedValue) {
            (ms, ArrayValue(Array::from(rc.iter()
                .map(|row| row.to_json_string(rc.get_columns()))
                .map(StringValue).collect::<Vec<_>>())))
        }

        match value {
            NamespaceValue(ns) =>
                match FileRowCollection::open(&ns) {
                    Ok(frc) => {
                        let rc = Box::new(frc);
                        if is_csv { convert_to_csv(ms, rc) } else { convert_to_json(ms, rc) }
                    }
                    Err(err) => (ms.to_owned(), ErrorValue(Exact(err.to_string())))
                }
            TableValue(rcv) => {
                let rc = Box::new(rcv.to_owned());
                if is_csv { convert_to_csv(ms, rc) } else { convert_to_json(ms, rc) }
            }
            _ => (ms.to_owned(), ErrorValue(Exact(format!("Cannot convert to {}", if is_csv { "CSV" } else { "JSON" }))))
        }
    }

    fn do_tools_to_json(ms: Machine, value: &TypedValue) -> (Machine, TypedValue) {
        Self::do_tools_to_csv_or_json(ms, value, false)
    }

    fn do_tools_to_table(ms: Machine, value: &TypedValue) -> (Machine, TypedValue) {
        fn convert(ms: Machine, value: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
            let rc = value.to_table()?;
            let columns = rc.get_columns();
            let rows = rc.read_active_rows()?;
            let mrc = ModelRowCollection::from_columns_and_rows(columns, &rows);
            Ok((ms, TableValue(Model(mrc))))
        }
        // convert the value to a RowCollection
        match convert(ms.clone(), value) {
            Ok(result) => result,
            Err(err) => (ms, ErrorValue(Exact(err.to_string())))
        }
    }

    fn do_util_base64(
        ms: Machine,
        a: &TypedValue,
    ) -> (Machine, TypedValue) {
        (ms, StringValue(base64::encode(a.to_bytes())))
    }

    fn do_util_binary(
        ms: Machine,
        a: &TypedValue,
    ) -> (Machine, TypedValue) {
        (ms, StringValue(format!("{:b}", a.to_u64())))
    }

    fn do_util_numeric_conv(
        ms: Machine,
        value: &TypedValue,
        plat: &PlatformOps,
    ) -> (Machine, TypedValue) {
        let result = match plat {
            PlatformOps::UtilToDate => Number(DateValue(value.to_i64())),
            PlatformOps::UtilToF32 => Number(F32Value(value.to_f32())),
            PlatformOps::UtilToF32 => Number(F32Value(value.to_f32())),
            PlatformOps::UtilToF64 => Number(F64Value(value.to_f64())),
            PlatformOps::UtilToI8 => Number(I8Value(value.to_i8())),
            PlatformOps::UtilToI16 => Number(I16Value(value.to_i16())),
            PlatformOps::UtilToI32 => Number(I32Value(value.to_i32())),
            PlatformOps::UtilToI64 => Number(I64Value(value.to_i64())),
            PlatformOps::UtilToI128 => Number(I128Value(value.to_i128())),
            PlatformOps::UtilToU8 => Number(U8Value(value.to_u8())),
            PlatformOps::UtilToU16 => Number(U16Value(value.to_u16())),
            PlatformOps::UtilToU32 => Number(U32Value(value.to_u32())),
            PlatformOps::UtilToU64 => Number(U64Value(value.to_u64())),
            PlatformOps::UtilToU128 => Number(U128Value(value.to_u128())),
            plat => ErrorValue(UnsupportedPlatformOps(plat.to_owned()))
        };
        (ms, result)
    }

    fn do_util_md5(ms: Machine, value: &TypedValue) -> (Machine, TypedValue) {
        match md5::compute(value.to_bytes()) {
            md5::Digest(bytes) => (ms, Binary(bytes.to_vec()))
        }
    }

    fn do_util_to_ascii(ms: Machine, value: &TypedValue) -> (Machine, TypedValue) {
        (ms, StringValue(format!("{}", value.to_u8() as char)))
    }

    fn do_util_to_hex(ms: Machine, value: &TypedValue) -> (Machine, TypedValue) {
        (ms, StringValue(format!("{}", StringValue(hex::encode(value.to_bytes())))))
    }

    fn do_util_uuid(
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> (Machine, TypedValue) {
        if !args.is_empty() {
            return (ms, ErrorValue(TypeMismatch(ArgumentsMismatched(0, args.len()))));
        }
        (ms, Number(UUIDValue(Uuid::new_v4().as_u128())))
    }

    fn do_www_serve(
        ms: Machine,
        port: &TypedValue,
    ) -> (Machine, TypedValue) {
        oxide_server::start_http_server(port.to_u16());
        (ms, Number(Ack))
    }

    fn do_www_url_decode(ms: Machine, url: &TypedValue) -> (Machine, TypedValue) {
        match url {
            StringValue(uri) => match urlencoding::decode(uri) {
                Ok(decoded) => (ms, StringValue(decoded.to_string())),
                Err(err) => (ms, ErrorValue(Exact(err.to_string())))
            }
            other => (ms, ErrorValue(TypeMismatch(StringExpected(other.to_code()))))
        }
    }

    fn do_www_url_encode(ms: Machine, url: &TypedValue) -> (Machine, TypedValue) {
        match url {
            StringValue(uri) => (ms, StringValue(urlencoding::encode(uri).to_string())),
            other => (ms, ErrorValue(TypeMismatch(StringExpected(other.to_code()))))
        }
    }

    pub fn get_kung_fu_feature_parameters() -> Vec<Parameter> {
        vec![
            Parameter::new("level", NumberType(U16Kind)),
            Parameter::new("item", StringType(256)),
            Parameter::new("passed", BooleanType),
            Parameter::new("result", StringType(256)),
        ]
    }

    pub fn get_os_env_parameters() -> Vec<Parameter> {
        vec![
            Parameter::new("key", StringType(256)),
            Parameter::new("value", StringType(8192)),
        ]
    }

    pub fn get_oxide_help_parameters() -> Vec<Parameter> {
        vec![
            Parameter::new("name", StringType(20)),
            Parameter::new("module", StringType(20)),
            Parameter::new("signature", StringType(32)),
            Parameter::new("description", StringType(60)),
            Parameter::new("returns", StringType(32)),
        ]
    }

    pub fn get_oxide_history_ns() -> Namespace {
        Namespace::new("oxide", "public", "history")
    }

    pub fn get_oxide_history_parameters() -> Vec<Parameter> {
        vec![
            Parameter::new("session_id", NumberType(I64Kind)),
            Parameter::new("user_id", NumberType(I64Kind)),
            Parameter::new("cpu_time_ms", NumberType(F64Kind)),
            Parameter::new("input", StringType(65536)),
        ]
    }

    pub fn get_tools_describe_parameters() -> Vec<Parameter> {
        vec![
            Parameter::new("name", StringType(128)),
            Parameter::new("type", StringType(128)),
            Parameter::new("default_value", StringType(128)),
            Parameter::new("is_nullable", BooleanType),
        ]
    }

    fn open_namespace(ns: &Namespace) -> TypedValue {
        match FileRowCollection::open(ns) {
            Err(err) => ErrorValue(Exact(err.to_string())),
            Ok(frc) => {
                let columns = frc.get_columns();
                match frc.read_active_rows() {
                    Err(err) => ErrorValue(Exact(err.to_string())),
                    Ok(rows) => TableValue(Model(ModelRowCollection::from_columns_and_rows(columns, &rows)))
                }
            }
        }
    }
}

/// Unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use super::*;
    use crate::columns::Column;
    use crate::interpreter::Interpreter;
    use crate::platform::{PlatformOps, PLATFORM_OPCODES};
    use crate::structures::{HardStructure, SoftStructure};
    use crate::testdata::*;
    use crate::typed_values::TypedValue::*;
    use serde_json::json;

    #[test]
    fn test_encode_decode() {
        for expected in PLATFORM_OPCODES {
            let bytes = expected.encode().unwrap();
            assert_eq!(bytes.len(), 4);

            let actual = PlatformOps::decode(bytes).unwrap();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn test_include_expect_failure() {
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"include 123"#);
        println!("result => {:?}", result);
        assert!(matches!(result, Err(..)))
    }

    #[test]
    fn test_get_info_cal_now() {
        assert_eq!(CalDate.get_info(), PlatformFunction {
            name: "now".into(),
            description: "Returns the current local date and time".into(),
            example: "cal::now()".into(),
            package_name: "cal".into(),
            parameters: Vec::new(),
            return_type: NumberType(DateKind),
            opcode: CalDate,
        });
    }

    #[test]
    fn test_get_info_str_left() {
        assert_eq!(StrLeft.get_info(), PlatformFunction {
            name: "left".into(),
            description: "Returns n-characters from left-to-right".into(),
            example: "str::left('Hello World', 5)".into(),
            package_name: "str".into(),
            parameters: vec![
                Descriptor::new("s", Some("String(0)".into()), None),
                Descriptor::new("n", Some("i64".into()), None),
            ],
            return_type: StringType(0),
            opcode: StrLeft,
        });
    }

    #[test]
    fn test_get_info_str_substring() {
        assert_eq!(StrSubstring.get_info(), PlatformFunction {
            name: "substring".into(),
            description: "Returns a substring of string `s` from `m` to `n`".into(),
            example: "str::substring('Hello World', 0, 5)".into(),
            package_name: "str".into(),
            parameters: vec![
                Descriptor::new("s", Some("String(0)".into()), None),
                Descriptor::new("m", Some("i64".into()), None),
                Descriptor::new("n", Some("i64".into()), None)
            ],
            return_type: StringType(0),
            opcode: StrSubstring,
        });
    }

    #[ignore]
    #[test]
    fn test_examples() {
        let mut errors = 0;
        let mut interpreter = Interpreter::new();
        for op in PLATFORM_OPCODES {
            match interpreter.evaluate(op.get_example().as_str()) {
                Ok(response) => assert_ne!(response, Undefined),
                Err(err) => {
                    println!("{}", "*".repeat(60));
                    println!("{}", op.get_example());
                    eprintln!("ERROR: {}", err);
                    errors += 1
                }
            }
        }
        assert_eq!(errors, 0)
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
        // cal
        assert_eq!(CalDate.to_code(), "cal::now()");
        assert_eq!(CalDateDay.to_code(), "cal::day_of(n: Date)");
        assert_eq!(CalDateHour12.to_code(), "cal::hour12(n: Date)");
        assert_eq!(CalDateHour24.to_code(), "cal::hour24(n: Date)");
        assert_eq!(CalDateMinute.to_code(), "cal::minute_of(n: Date)");
        assert_eq!(CalDateMonth.to_code(), "cal::month_of(n: Date)");
        assert_eq!(CalDateSecond.to_code(), "cal::second_of(n: Date)");
        assert_eq!(CalDateYear.to_code(), "cal::year_of(n: Date)");
        // io
        assert_eq!(IoFileCreate.to_code(), "io::create_file(a: String(0), b: String(0))");
        assert_eq!(IoFileExists.to_code(), "io::exists(s: String(0))");
        assert_eq!(IoFileReadText.to_code(), "io::read_text_file(s: String(0))");
        assert_eq!(IoStdErr.to_code(), "io::stderr(s: String(0))");
        assert_eq!(IoStdOut.to_code(), "io::stdout(s: String(0))");
        // kungfu
        assert_eq!(KungFuAssert.to_code(), "kungfu::assert(b: Boolean)");
        assert_eq!(KungFuFeature.to_code(), "kungfu::feature(a: String(0), b: Struct())");
        assert_eq!(KungFuMatches.to_code(), "kungfu::matches(a, b)");
        assert_eq!(KungFuTypeOf.to_code(), "kungfu::type_of(x)");
        // os
        assert_eq!(OsCall.to_code(), "os::call(s: String(0))");
        assert_eq!(OsClear.to_code(), "os::clear()");
        assert_eq!(OsCurrentDir.to_code(), "os::current_dir()");
        assert_eq!(OsEnv.to_code(), "os::env()");
        // oxide
        assert_eq!(OxideCompile.to_code(), "oxide::compile(s: String(0))");
        assert_eq!(OxideEval.to_code(), "oxide::eval(s: String(0))");
        assert_eq!(OxideHelp.to_code(), "oxide::help()");
        assert_eq!(OxideHistory.to_code(), "oxide::history()");
        assert_eq!(OxideHome.to_code(), "oxide::home()");
        assert_eq!(OxidePrintln.to_code(), "oxide::println(s: String(0))");
        assert_eq!(OxideReset.to_code(), "oxide::reset()");
        assert_eq!(OxideUUID.to_code(), "oxide::uuid()");
        assert_eq!(OxideVersion.to_code(), "oxide::version()");
        // str
        assert_eq!(StrEndsWith.to_code(), "str::ends_with(a: String(0), b: String(0))");
        assert_eq!(StrFormat.to_code(), "str::format(a: String(0), b: String(0))");
        assert_eq!(StrIndexOf.to_code(), "str::index_of(s: String(0), n: i64)");
        assert_eq!(StrJoin.to_code(), "str::join(a: Array(0), b: String(0))");
        assert_eq!(StrLeft.to_code(), "str::left(s: String(0), n: i64)");
        assert_eq!(StrLen.to_code(), "str::len(s: String(0))");
        assert_eq!(StrRight.to_code(), "str::right(s: String(0), n: i64)");
        assert_eq!(StrSplit.to_code(), "str::split(a: String(0), b: String(0))");
        assert_eq!(StrStartsWith.to_code(), "str::starts_with(a: String(0), b: String(0))");
        assert_eq!(StrSubstring.to_code(), "str::substring(s: String(0), m: i64, n: i64)");
        assert_eq!(StrToString.to_code(), "str::to_string(x)");
        // tools
        assert_eq!(ToolsCompact.to_code(), "tools::compact(t: Table())");
        assert_eq!(ToolsDescribe.to_code(), "tools::describe(t: Table())");
        assert_eq!(ToolsFetch.to_code(), "tools::fetch(t: Table(), n: u64)");
        assert_eq!(ToolsReverse.to_code(), "tools::reverse(t: Table())");
        assert_eq!(ToolsScan.to_code(), "tools::scan(t: Table())");
        assert_eq!(ToolsToArray.to_code(), "tools::to_array(t: Table())");
        assert_eq!(ToolsToCSV.to_code(), "tools::to_csv(t: Table())");
        assert_eq!(ToolsToJSON.to_code(), "tools::to_json(t: Table())");
        assert_eq!(ToolsToTable.to_code(), "tools::to_table(x)");
        // util
        assert_eq!(UtilBase64.to_code(), "util::base64(x)");
        assert_eq!(UtilHex.to_code(), "util::hex(x)");
        assert_eq!(UtilMD5.to_code(), "util::md5(x)");
        assert_eq!(UtilToASCII.to_code(), "util::to_ascii(n: u32)");
        assert_eq!(UtilToDate.to_code(), "util::to_date(x)");
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
        assert_eq!(WwwURLDecode.to_code(), "www::url_decode(s: String(0))");
        assert_eq!(WwwURLEncode.to_code(), "www::url_encode(s: String(0))");
        assert_eq!(WwwServe.to_code(), "www::serve(n: u32)");
    }

    /// Package "cal" tests
    #[cfg(test)]
    mod cal_tests {
        use crate::interpreter::Interpreter;
        use crate::numbers::Numbers::*;
        use crate::testdata::verify_whence;
        use crate::typed_values::TypedValue::Number;

        #[test]
        fn test_cal_now() {
            verify_whence(Interpreter::new(), r#"
                cal::now()
            "#, |n| matches!(n, Number(DateValue(..))));
        }

        #[test]
        fn test_cal_day_of() {
            verify_whence(Interpreter::new(), r#"
                import cal
                now():::day_of()
            "#, |n| matches!(n, Number(U32Value(..))));
        }

        #[test]
        fn test_cal_hour24() {
            verify_whence(Interpreter::new(), r#"
                import cal
                now():::hour24()
            "#, |n| matches!(n, Number(U32Value(..))));
        }

        #[test]
        fn test_cal_hour12() {
            verify_whence(Interpreter::new(), r#"
                import cal
                now():::hour12()
            "#, |n| matches!(n, Number(U32Value(..))));
        }

        #[test]
        fn test_cal_minute_of() {
            verify_whence(Interpreter::new(), r#"
                import cal
                now():::minute_of()
            "#, |n| matches!(n, Number(U32Value(..))));
        }

        #[test]
        fn test_cal_month_of() {
            verify_whence(Interpreter::new(), r#"
                import cal
                now():::month_of()
            "#, |n| matches!(n, Number(U32Value(..))));
        }

        #[test]
        fn test_cal_second_of() {
            verify_whence(Interpreter::new(), r#"
                import cal
                now():::second_of()
            "#, |n| matches!(n, Number(U32Value(..))));
        }

        #[test]
        fn test_cal_year_of() {
            verify_whence(Interpreter::new(), r#"
                import cal
                now():::year_of()
            "#, |n| matches!(n, Number(I32Value(..))));
        }
    }

    /// Package "io" tests
    #[cfg(test)]
    mod io_tests {
        use super::*;
        use crate::platform::PlatformOps;
        use crate::typed_values::TypedValue::*;
        use PlatformOps::*;

        #[test]
        fn test_io_create_file_qualified() {
            verify_exact(r#"
                io::create_file("quote.json", { symbol: "TRX", exchange: "NYSE", last_sale: 45.32 })
            "#, Number(U64Value(52)));

            verify_exact(r#"
                io::exists("quote.json")
            "#, Boolean(true));
        }

        #[test]
        fn test_io_create_file_postfix() {
            verify_exact(r#"
                import io
                "quote.json":::create_file({
                    symbol: "TRX",
                    exchange: "NYSE",
                    last_sale: 45.32
                })
            "#, Number(U64Value(52)));

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
                import io, util
                file := "temp_secret.txt"
                file:::create_file(md5("**keep**this**secret**"))
                file:::read_text_file()
            "#, StringValue("47338bd5f35bbb239092c36e30775b4a".into()))
        }

        #[test]
        fn test_io_stderr() {
            verify_exact(r#"io::stderr("Goodbye Cruel World")"#, Number(Ack));
        }

        #[test]
        fn test_io_stdout() {
            verify_exact(r#"io::stdout("Hello World")"#, Number(Ack));
        }
    }

    /// Package "kung_fu" tests
    #[cfg(test)]
    mod kung_fu_tests {
        use crate::testdata::{verify_exact, verify_exact_table_with_ids, verify_exact_text};
        use crate::typed_values::TypedValue::Boolean;

        #[test]
        fn test_kung_fu_feature() {
            verify_exact_table_with_ids(r#"
            import kungfu
            feature("Matches function", {
                "Compare Array contents: Equal": fn(ctx) => {
                    assert(matches(
                        [ 1 "a" "b" "c" ],
                        [ 1 "a" "b" "c" ]))
                },
                "Compare Array contents: Not Equal": fn(ctx) => {
                    assert(!matches(
                        [ 1 "a" "b" "c" ],
                        [ 0 "x" "y" "z" ]))
                },
                "Compare JSON contents (in sequence)": fn(ctx) => {
                    assert(matches(
                        { first: "Tom" last: "Lane" },
                        { first: "Tom" last: "Lane" }))
                },
                "Compare JSON contents (out of sequence)": fn(ctx) => {
                    assert(matches(
                        { scores: [82 78 99], id: "A1537" },
                        { id: "A1537", scores: [82 78 99] }))
                }
            })"#, vec![
                "|--------------------------------------------------------------------------------------------------------------------------|",
                "| id | level | item                                                                                      | passed | result |",
                "|--------------------------------------------------------------------------------------------------------------------------|",
                "| 0  | 0     | Matches function                                                                          | true   | Ack    |",
                "| 1  | 1     | Compare Array contents: Equal                                                             | true   | Ack    |",
                r#"| 2  | 2     | assert(matches([1, "a", "b", "c"], [1, "a", "b", "c"]))                                   | true   | true   |"#,
                "| 3  | 1     | Compare Array contents: Not Equal                                                         | true   | Ack    |",
                r#"| 4  | 2     | assert(!matches([1, "a", "b", "c"], [0, "x", "y", "z"]))                                  | true   | true   |"#,
                "| 5  | 1     | Compare JSON contents (in sequence)                                                       | true   | Ack    |",
                r#"| 6  | 2     | assert(matches({first: "Tom", last: "Lane"}, {first: "Tom", last: "Lane"}))               | true   | true   |"#,
                "| 7  | 1     | Compare JSON contents (out of sequence)                                                   | true   | Ack    |",
                r#"| 8  | 2     | assert(matches({scores: [82, 78, 99], id: "A1537"}, {id: "A1537", scores: [82, 78, 99]})) | true   | true   |"#,
                "|--------------------------------------------------------------------------------------------------------------------------|"
            ]);
        }

        #[test]
        fn test_kung_fu_matches_exact() {
            // test a perfect match
            verify_exact(r#"
                a := { first: "Tom", last: "Lane", scores: [82, 78, 99] }
                b := { first: "Tom", last: "Lane", scores: [82, 78, 99] }
                kungfu::matches(a, b)
            "#, Boolean(true));
        }

        #[test]
        fn test_kung_fu_matches_unordered() {
            // test an unordered match
            verify_exact(r#"
                import kungfu::matches
                a := { scores: [82, 78, 99], first: "Tom", last: "Lane" }
                b := { last: "Lane", first: "Tom", scores: [82, 78, 99] }
                matches(a, b)
            "#, Boolean(true));
        }

        #[test]
        fn test_kung_fu_matches_not_match_1() {
            // test when things do not match 1
            verify_exact(r#"
                import kungfu
                a := { first: "Tom", last: "Lane" }
                b := { first: "Jerry", last: "Lane" }
                a:::matches(b)
            "#, Boolean(false));
        }

        #[test]
        fn test_kung_fu_matches_not_match_2() {
            // test when things do not match 2
            verify_exact(r#"
                a := { key: "123", values: [1, 74, 88] }
                b := { key: "123", values: [1, 74, 88, 0] }
                kungfu::matches(a, b)
            "#, Boolean(false));
        }

        #[test]
        fn test_kung_fu_type_of_array_bool() {
            verify_exact_text_q("kungfu::type_of([true, false])", "Array(2)");
        }

        #[test]
        fn test_kung_fu_type_of_array_i64() {
            verify_exact_text_q("kungfu::type_of([12, 76, 444])", "Array(3)");
        }

        #[test]
        fn test_kung_fu_type_of_array_str() {
            verify_exact_text_q("kungfu::type_of(['ciao', 'hello', 'world'])", "Array(3)");
        }

        #[test]
        fn test_kung_fu_type_of_array_f64() {
            verify_exact_text_q("kungfu::type_of([12, 'hello', 76.78])", "Array(3)");
        }

        #[test]
        fn test_kung_fu_type_of_bool() {
            verify_exact_text_q("kungfu::type_of(false)", "Boolean");
            verify_exact_text_q("kungfu::type_of(true)", "Boolean");
        }

        #[test]
        fn test_kung_fu_type_of_date() {
            verify_exact_text_q("kungfu::type_of(cal::now())", "Date");
        }

        #[test]
        fn test_kung_fu_type_of_fn() {
            verify_exact_text_q("kungfu::type_of(fn(a, b) => a + b)", "fn(a, b)");
        }

        #[test]
        fn test_kung_fu_type_of_i64() {
            verify_exact_text_q("kungfu::type_of(1234)", "i64");
        }

        #[test]
        fn test_kung_fu_type_of_f64() {
            verify_exact_text_q("kungfu::type_of(12.394)", "f64");
        }

        #[test]
        fn test_kung_fu_type_of_string() {
            verify_exact_text_q("kungfu::type_of('1234')", "String(4)");
            verify_exact_text_q(r#"kungfu::type_of("abcde")"#, "String(5)");
        }

        #[test]
        fn test_kung_fu_type_of_structure_hard() {
            verify_exact_text_q(r#"kungfu::type_of(Struct(symbol: String(3) = "ABC"))"#,
                                r#"Struct(symbol: String(3) := "ABC")"#);
        }

        #[test]
        fn test_kung_fu_type_of_structure_soft() {
            verify_exact_text_q(r#"kungfu::type_of({symbol:"ABC"})"#,
                                r#"Struct(symbol: String(3) := "ABC")"#);
        }

        #[test]
        fn test_kung_fu_type_of_namespace() {
            verify_exact_text_q(r#"kungfu::type_of(ns("a.b.c"))"#, "Table()");
        }

        #[test]
        fn test_kung_fu_type_of_table() {
            verify_exact_text_q(r#"kungfu::type_of(table(symbol: String(8), exchange: String(8), last_sale: f64))"#,
                                "Table(symbol: String(8), exchange: String(8), last_sale: f64)");
        }

        #[test]
        fn test_kung_fu_type_of_uuid() {
            verify_exact_text_q("kungfu::type_of(oxide::uuid())", "UUID");
        }

        #[test]
        fn test_kung_fu_type_of_variable() {
            verify_exact_text_q("kungfu::type_of(my_var)", "");
        }

        #[test]
        fn test_kung_fu_type_of_null() {
            verify_exact_text_q("kungfu::type_of(null)", "");
        }

        #[test]
        fn test_kung_fu_type_of_undefined() {
            verify_exact_text_q("kungfu::type_of(undefined)", "");
        }

        fn verify_exact_text_q(code: &str, expected: &str) {
            verify_exact_text(code, format!("\"{}\"", expected).as_str())
        }
    }

    /// Package "os" tests
    #[cfg(test)]
    mod os_tests {
        use super::*;
        use crate::platform::PlatformOps;
        use crate::typed_values::TypedValue::*;
        use PlatformOps::*;

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
            verify_exact("os::clear()", Number(Ack))
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
        }

        #[test]
        fn test_os_env() {
            verify_when("os::env()", |v| matches!(v, TableValue(..)))
        }
    }

    /// Package "oxide" tests
    #[cfg(test)]
    mod oxide_tests {
        use super::*;
        use crate::errors::TypeMismatchErrors::StringExpected;
        use crate::platform::PlatformOps;
        use crate::typed_values::TypedValue::*;
        use PlatformOps::*;

        #[test]
        fn test_oxide_compile() {
            verify_exact(r#"
                code := oxide::compile("2 ** 4")
                code()
            "#, Number(F64Value(16.)));
        }

        #[test]
        fn test_oxide_compile_closure() {
            verify_exact(r#"
                n := 5
                code := oxide::compile("n¡")
                code()
            "#, Number(U128Value(120)));
        }

        #[test]
        fn test_oxide_eval_closure() {
            verify_exact(r#"
                a := 'Hello '
                b := 'World'
                oxide::eval("a + b")
            "#, StringValue("Hello World".to_string()));
        }

        #[test]
        fn test_oxide_eval_qualified() {
            verify_exact("oxide::eval('2 ** 4')", Number(F64Value(16.)));
            verify_exact("oxide::eval(123)", ErrorValue(TypeMismatch(StringExpected("i64".into()))));
        }

        #[test]
        fn test_oxide_eval_postfix() {
            let mut interpreter = Interpreter::new();
            interpreter.evaluate("import oxide").unwrap();
            interpreter = verify_where(interpreter, "'2 ** 4':::eval()", Number(F64Value(16.)));
            interpreter = verify_where(interpreter, "123:::eval()", ErrorValue(TypeMismatch(StringExpected("i64".into()))));
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
        fn test_oxide_history() {
            let mut interpreter = Interpreter::new();
            let result = interpreter.evaluate("oxide::history()").unwrap();
            assert!(matches!(result, TableValue(..)))
        }

        #[test]
        fn test_oxide_home() {
            verify_exact("oxide::home()", StringValue(Machine::oxide_home()));
        }

        #[test]
        fn test_oxide_println() {
            verify_exact(r#"oxide::println("Hello World")"#, Number(Ack));
        }

        #[test]
        fn test_oxide_version() {
            verify_exact(
                "oxide::version()",
                StringValue(format!("{MAJOR_VERSION}.{MINOR_VERSION}")))
        }
    }

    /// Package "str" tests
    #[cfg(test)]
    mod str_tests {
        use super::*;
        use crate::platform::PlatformOps;
        use crate::testdata::*;
        use crate::typed_values::TypedValue::*;
        use PlatformOps::*;

        #[test]
        fn test_str_ends_with_postfix() {
            verify_exact(r#"
                import str
                'Hello World':::ends_with('World')
            "#, Boolean(true));
        }

        #[test]
        fn test_str_ends_with_qualified() {
            verify_exact(r#"
                str::ends_with('Hello World', 'World')
            "#, Boolean(true));

            verify_exact(r#"
                str::ends_with('Hello World', 'Hello')
            "#, Boolean(false))
        }

        #[test]
        fn test_str_format_qualified() {
            verify_exact(r#"
                str::format("This {} the {}", "is", "way")
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
        fn test_str_format_in_scope() {
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
        fn test_str_left_qualified() {
            verify_exact(r#"
                str::left('Hello World', 5)
            "#, StringValue("Hello".into()));

            verify_exact(r#"
                str::left('Hello World', -5)
            "#, StringValue("World".into()));
        }

        #[test]
        fn test_str_left_postfix() {
            verify_exact(r#"
                import str, util
                'Hello World':::left(5)
            "#, StringValue("Hello".into()));

            // postfix - invalid case
            verify_exact(r#"
                import str, util
                12345:::left(5)
            "#, Undefined);
        }

        #[test]
        fn test_str_left_in_scope() {
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
        fn test_str_right_qualified() {
            verify_exact(r#"
                str::right('Hello World', 5)
            "#, StringValue("World".into()));

            // fully-qualified (inverse)
            verify_exact(r#"
                str::right('Hello World', -5)
            "#, StringValue("Hello".into()));
        }

        #[test]
        fn test_str_right_qualified_negative_cases() {
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
        fn test_str_split_qualified() {
            verify_exact(r#"
                str::split('Hello,there World', ' ,')
            "#, ArrayValue(Array::from(vec![
                StringValue("Hello".into()),
                StringValue("there".into()),
                StringValue("World".into())
            ])));
        }

        #[test]
        fn test_str_split_postfix() {
            verify_exact(r#"
                import str
                'Hello World':::split(' ')
            "#, ArrayValue(Array::from(vec![
                StringValue("Hello".into()),
                StringValue("World".into())
            ])));
        }

        #[test]
        fn test_str_split_in_scope() {
            // in-scope
            verify_exact(r#"
                import str
                split('Hello,there World;Yeah!', ' ,;')
            "#, ArrayValue(Array::from(vec![
                StringValue("Hello".into()),
                StringValue("there".into()),
                StringValue("World".into()),
                StringValue("Yeah!".into())
            ])));
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
    }

    /// Package "tools" tests
    #[cfg(test)]
    mod tools_tests {
        use super::*;
        use crate::columns::Column;
        use crate::interpreter::Interpreter;
        use crate::platform::PlatformOps;
        use crate::structures::HardStructure;
        use crate::testdata::*;
        use crate::typed_values::TypedValue::*;
        use PlatformOps::*;

        #[test]
        fn test_tools_compact() {
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
                [+] import tools
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
        fn test_tools_describe() {
            // fully-qualified
            verify_exact_table_with_ids(r#"
                tools::describe({ symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 })
            "#, vec![
                "|----------------------------------------------------------|",
                "| id | name      | type      | default_value | is_nullable |",
                "|----------------------------------------------------------|",
                "| 0  | symbol    | String(3) | BIZ           | true        |",
                "| 1  | exchange  | String(4) | NYSE          | true        |",
                "| 2  | last_sale | f64       | 23.66         | true        |",
                "|----------------------------------------------------------|"
            ]);

            // postfix
            verify_exact_table_with_ids(r#"
                [+] import tools
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
        fn test_tools_fetch() {
            // fully-qualified
            verify_exact_table_with_ids(r#"
                [+] stocks := ns("platform.fetch.stocks")
                [+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                [+] [{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                     { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                     { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
                [+] tools::fetch(stocks, 2)
            "#, vec![
                "|------------------------------------|",
                "| id | symbol | exchange | last_sale |",
                "|------------------------------------|",
                "| 2  | JET    | NASDAQ   | 32.12     |",
                "|------------------------------------|"
            ]);

            // postfix
            verify_exact_table_with_ids(r#"
                [+] import tools
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
        fn test_tools_reverse_arrays() {
            verify_exact_table_with_ids(r#"
                import tools
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
        fn test_tools_reverse_strings() {
            verify_exact(r#"
                fn backwards(a) => tools::reverse(a)
                "Hello World":::backwards()
            "#, StringValue("dlroW olleH".into()));
        }

        #[test]
        fn test_tools_reverse_tables_function() {
            // fully-qualified (ephemeral)
            verify_exact_table_with_ids(r#"
                import tools
                stocks := to_table([
                    { symbol: "ABC", exchange: "AMEX", last_sale: 12.33 },
                    { symbol: "BIZ", exchange: "NYSE", last_sale: 9.775 },
                    { symbol: "XYZ", exchange: "NASDAQ", last_sale: 89.11 }
                ])
                reverse(stocks)
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
        fn test_tools_reverse_tables_method() {
            // postfix (durable)
            verify_exact_table_with_ids(r#"
                [+] import tools
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
        fn test_tools_scan() {
            let mut interpreter = Interpreter::new();
            let result = interpreter.evaluate(r#"
                [+] import tools
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
        fn test_tools_to_array_with_strings_qualified() {
            verify_exact(r#"
                 tools::to_array("Hello")
            "#, ArrayValue(Array::from(vec![
                StringValue("H".into()), StringValue("e".into()),
                StringValue("l".into()), StringValue("l".into()),
                StringValue("o".into())
            ])));
        }

        #[test]
        fn test_tools_to_array_with_strings_postfix() {
            verify_exact(r#"
                import tools
                "World":::to_array()
            "#, ArrayValue(Array::from(vec![
                StringValue("W".into()), StringValue("o".into()),
                StringValue("r".into()), StringValue("l".into()),
                StringValue("d".into())
            ])));
        }

        #[test]
        fn test_tools_to_array_with_tables() {
            // fully qualified
            let mut interpreter = Interpreter::new();
            let result = interpreter.evaluate(r#"
                 tools::to_array(tools::to_table([
                     { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                     { symbol: "DMX", exchange: "OTC_BB", last_sale: 1.17 }
                 ]))
            "#).unwrap();
            let columns = Column::from_descriptors(&vec![
                Descriptor::new("symbol", Some("String(3)".into()), Some("\"BIZ\"".into())),
                Descriptor::new("exchange", Some("String(4)".into()), Some("\"NYSE\"".into())),
                Descriptor::new("last_sale", Some("f64".into()), Some("23.66".into())),
            ]).unwrap();
            assert_eq!(result, ArrayValue(Array::from(vec![
                Structured(Hard(HardStructure::new(columns.clone(), vec![
                    StringValue("BIZ".into()), StringValue("NYSE".into()), Number(F64Value(23.66))
                ]))),
                Structured(Hard(HardStructure::new(columns.clone(), vec![
                    StringValue("DMX".into()), StringValue("OTC_BB".into()), Number(F64Value(1.17))
                ])))
            ])));
        }

        #[test]
        fn test_tools_to_csv() {
            verify_exact(r#"
                import tools::to_csv
                [+] stocks := ns("platform.csv.stocks")
                [+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                [+] [{ symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
                     { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                     { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                     { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                     { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }] ~> stocks
                stocks:::to_csv()
            "#, ArrayValue(Array::from(vec![
                StringValue(r#""ABC","AMEX",11.11"#.into()),
                StringValue(r#""UNO","OTC",0.2456"#.into()),
                StringValue(r#""BIZ","NYSE",23.66"#.into()),
                StringValue(r#""GOTO","OTC",0.1428"#.into()),
                StringValue(r#""BOOM","NASDAQ",0.0872"#.into()),
            ])));
        }

        #[test]
        fn test_tools_to_json() {
            verify_exact(r#"
                import tools::to_json
                [+] stocks := ns("platform.json.stocks")
                [+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                [+] [{ symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
                     { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                     { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                     { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                     { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }] ~> stocks
                stocks:::to_json()
            "#, ArrayValue(Array::from(vec![
                StringValue(r#"{"symbol":"ABC","exchange":"AMEX","last_sale":11.11}"#.into()),
                StringValue(r#"{"symbol":"UNO","exchange":"OTC","last_sale":0.2456}"#.into()),
                StringValue(r#"{"symbol":"BIZ","exchange":"NYSE","last_sale":23.66}"#.into()),
                StringValue(r#"{"symbol":"GOTO","exchange":"OTC","last_sale":0.1428}"#.into()),
                StringValue(r#"{"symbol":"BOOM","exchange":"NASDAQ","last_sale":0.0872}"#.into()),
            ])));
        }

        #[test]
        fn test_tools_to_table_with_arrays() {
            verify_exact_table_with_ids(r#"
                tools::to_table(['cat', 'dog', 'ferret', 'mouse'])
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
        fn test_tools_to_table_with_hard_structures() {
            verify_exact_table_with_ids(r#"
                tools::to_table(Struct(
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
        fn test_tools_to_table_with_soft_and_hard_structures() {
            verify_exact_table_with_ids(r#"
                stocks := tools::to_table([
                    { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                    { symbol: "DMX", exchange: "OTC_BB", last_sale: 1.17 }
                ])

                tools::to_table([
                    stocks,
                    Struct(
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
    }

    /// Package "util" tests
    #[cfg(test)]
    mod util_tests {
        use super::*;
        use crate::interpreter::Interpreter;
        use crate::platform::PlatformOps;
        use crate::testdata::*;
        use crate::typed_values::TypedValue::*;
        use PlatformOps::*;

        #[test]
        fn test_util_base64() {
            verify_exact(
                "util::base64('Hello World')",
                StringValue("SGVsbG8gV29ybGQ=".into()))
        }

        #[test]
        fn test_util_hex() {
            verify_exact(
                "util::hex('Hello World')",
                StringValue("48656c6c6f20576f726c64".into()))
        }

        #[test]
        fn test_util_md5() {
            verify_exact_text(
                "util::md5('Hello World')",
                "b10a8db164e0754105b7a99be72e3fe5")
        }

        #[test]
        fn test_util_md5_type() {
            verify_data_type("util::md5(x)", BinaryType(16));
        }

        #[test]
        fn test_util_to_ascii() {
            verify_exact_text(
                "util::to_ascii(177)",
                "\"±\"")
        }

        #[test]
        fn test_util_to_hex() {
            verify_exact_text(
                "util::hex('Hello World')",
                "\"48656c6c6f20576f726c64\"")
        }

        #[test]
        fn test_util_to_f32_to_u128() {
            use crate::numbers::Numbers::*;
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
            assert_eq!(interpreter.get("to_u8"), Some(PlatformOp(PlatformOps::UtilToU8)));
            assert_eq!(interpreter.get("to_u16"), Some(PlatformOp(PlatformOps::UtilToU16)));
            assert_eq!(interpreter.get("to_u32"), Some(PlatformOp(PlatformOps::UtilToU32)));
            assert_eq!(interpreter.get("to_u64"), Some(PlatformOp(PlatformOps::UtilToU64)));
            assert_eq!(interpreter.get("to_u128"), Some(PlatformOp(PlatformOps::UtilToU128)));
        }
    }

    /// Package "www" tests
    #[cfg(test)]
    mod www_tests {
        use super::*;
        use crate::columns::Column;
        use crate::interpreter::Interpreter;
        use crate::platform::PlatformOps;
        use crate::structures::HardStructure;
        use crate::testdata::*;
        use crate::typed_values::TypedValue::*;
        use PlatformOps::*;

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

        #[test]
        fn test_www_serve() {
            let mut interpreter = Interpreter::new();
            let result = interpreter.evaluate(r#"
                [+] www::serve(8822)
                [+] stocks := ns("platform.www.quotes")
                [+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                [+] [{ symbol: "XINU", exchange: "NYSE", last_sale: 8.11 },
                     { symbol: "BOX", exchange: "NYSE", last_sale: 56.88 },
                     { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 },
                     { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                     { symbol: "MIU", exchange: "OTCBB", last_sale: 2.24 }] ~> stocks
                GET "http://localhost:8822/platform/www/quotes/1/4"
            "#).unwrap();
            assert_eq!(result.to_json(), json!([
            {"symbol": "BOX", "exchange": "NYSE", "last_sale": 56.88},
            {"symbol": "JET", "exchange": "NASDAQ", "last_sale": 32.12},
            {"symbol": "ABC", "exchange": "AMEX", "last_sale": 12.49}
        ]));
        }

        #[test]
        fn test_www_serve_workflow() {
            let mut interpreter = Interpreter::new();

            // set up a listener on port 8833
            let result = interpreter.evaluate(r#"
                www::serve(8833)
            "#).unwrap();
            assert_eq!(result, Number(Ack));

            // create the table
            let result = interpreter.evaluate(r#"
                create table ns("platform.www.stocks") (
                    symbol: String(8),
                    exchange: String(8),
                    last_sale: f64
                )
            "#).unwrap();
            assert_eq!(result, Number(Ack));

            // append a new row
            let row_id = interpreter.evaluate(r#"
                POST "http://localhost:8833/platform/www/stocks/0" FROM {
                    symbol: "ABC", exchange: "AMEX", last_sale: 11.77
                }
            "#).unwrap();
            assert!(matches!(row_id, Number(I64Value(..))));

            // fetch the previously created row
            let row = interpreter.evaluate(format!(r#"
                GET "http://localhost:8833/platform/www/stocks/{row_id}"
            "#).as_str()).unwrap();
            assert_eq!(row.to_json(), json!({"exchange":"AMEX","symbol":"ABC","last_sale":11.77}));

            // replace the previously created row
            let result = interpreter.evaluate(r#"
                PUT "http://localhost:8833/platform/www/stocks/:id" FROM {
                    symbol: "ABC", exchange: "AMEX", last_sale: 11.79
                }
            "#
                .replace("/:id", format!("/{}", row_id.unwrap_value()).as_str())
                .as_str()).unwrap();
            assert_eq!(result, Number(I64Value(1)));

            // re-fetch the previously updated row
            let row = interpreter.evaluate(format!(r#"
                GET "http://localhost:8833/platform/www/stocks/{row_id}"
            "#).as_str()).unwrap();
            assert_eq!(row.to_json(), json!({"symbol":"ABC","exchange":"AMEX","last_sale":11.79}));

            // update the previously created row
            let result = interpreter.evaluate(r#"
                PATCH "http://localhost:8833/platform/www/stocks/:id" FROM {
                    last_sale: 11.81
                }
            "#
                .replace("/:id", format!("/{}", row_id.unwrap_value()).as_str())
                .as_str()).unwrap();
            assert_eq!(result, Number(I64Value(1)));

            // re-fetch the previously updated row
            let row = interpreter.evaluate(format!(r#"
                GET "http://localhost:8833/platform/www/stocks/{row_id}"
            "#).as_str()).unwrap();
            assert_eq!(row.to_json(), json!({"last_sale":11.81,"symbol":"ABC","exchange":"AMEX"}));

            // fetch the headers for the previously updated row
            let result = interpreter.evaluate(format!(r#"
                HEAD "http://localhost:8833/platform/www/stocks/{row_id}"
            "#).as_str()).unwrap();
            println!("HEAD: {}", result.to_string());
            assert!(matches!(result, Structured(Soft(..))));

            // delete the previously updated row
            let result = interpreter.evaluate(format!(r#"
                DELETE "http://localhost:8833/platform/www/stocks/{row_id}"
            "#).as_str()).unwrap();
            assert_eq!(result, Number(I64Value(1)));

            // verify the deleted row is empty
            let row = interpreter.evaluate(format!(r#"
                GET "http://localhost:8833/platform/www/stocks/{row_id}"
            "#).as_str()).unwrap();
            assert_eq!(row, Structured(Soft(SoftStructure::empty())));
        }
    }
}