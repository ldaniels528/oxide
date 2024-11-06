////////////////////////////////////////////////////////////////////
// BackDoorKey enum
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

use crate::data_types::DataType;
use crate::data_types::DataType::FunctionType;
use crate::parameter::Parameter;
use BackDoorKey::*;

/// Represents a backdoor (hook) for calling native functions
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum BackDoorKey {
    // io package
    IoStdErr = 8,
    IoStdOut = 9,
    // lang package
    LangAssert = 0,
    LangMatches = 4,
    LangTypeOf = 22,
    // os package
    OsEnvVars = 25,
    OsSysCall = 11,
    // str package
    StrFormat = 2,
    StrLeft = 3,
    StrRight = 6,
    StrSubstring = 10,
    StrToString = 40,
    // util package
    UtilTimestamp = 12,
    UtilTimestampDay = 13,
    UtilTimestampHour = 14,
    UtilTimestampHour12 = 15,
    UtilTimestampMinute = 16,
    UtilTimestampMonth = 17,
    UtilTimestampSecond = 18,
    UtilTimestampYear = 19,
    UtilToCSV = 20,
    UtilToF32 = 27,
    UtilToF64 = 28,
    UtilToJSON = 21,
    UtilToI8 = 29,
    UtilToI16 = 30,
    UtilToI32 = 31,
    UtilToI64 = 32,
    UtilToI128 = 33,
    UtilToTable = 39,
    UtilToU8 = 34,
    UtilToU16 = 35,
    UtilToU32 = 36,
    UtilToU64 = 37,
    UtilToU128 = 38,
    UtilUUID = 23,
    // vm package
    VmEval = 1,
    VmReset = 5,
    VmServe = 7,
    VmVariables = 24,
    VmVersion = 26,
}

const BACK_DOOR_KEYS: [BackDoorKey; 41] = [
    // io
    IoStdErr, IoStdOut,
    // lang
    LangAssert, LangMatches, LangTypeOf,
    // os
    OsEnvVars, OsSysCall,
    // str
    StrFormat, StrLeft, StrRight, StrSubstring, StrToString,
    // util
    UtilToCSV, UtilToF32, UtilToF64,
    UtilToI8, UtilToI16, UtilToI32, UtilToI64, UtilToI128,
    UtilToJSON,
    UtilToU8, UtilToU16, UtilToU32, UtilToU64, UtilToU128,
    UtilTimestamp, UtilTimestampDay, UtilTimestampHour, UtilTimestampHour12,
    UtilTimestampMinute, UtilTimestampMonth, UtilTimestampSecond, UtilTimestampYear,
    UtilToTable, UtilUUID,
    // vm
    VmEval, VmReset, VmServe, VmVariables, VmVersion,
];

impl BackDoorKey {
    pub fn from_u8(code: u8) -> Self {
        for bdk in BACK_DOOR_KEYS {
            if bdk.to_u8() == code {
                return bdk;
            }
        }
        panic!("No key for {code} was found")
    }

    fn get_name(&self) -> String {
        let result = match self {
            BackDoorKey::IoStdErr => "stderr",
            BackDoorKey::IoStdOut => "stdout",
            BackDoorKey::LangAssert => "assert",
            BackDoorKey::LangMatches => "matches",
            BackDoorKey::LangTypeOf => "type_of",
            BackDoorKey::OsEnvVars => "env",
            BackDoorKey::OsSysCall => "call",
            BackDoorKey::StrFormat => "format",
            BackDoorKey::StrLeft => "left",
            BackDoorKey::StrRight => "right",
            BackDoorKey::StrSubstring => "substring",
            BackDoorKey::StrToString => "to_string",
            BackDoorKey::UtilTimestamp => "timestamp",
            BackDoorKey::UtilTimestampDay => "day_of",
            BackDoorKey::UtilTimestampHour => "hour_of",
            BackDoorKey::UtilTimestampHour12 => "hour_12_of",
            BackDoorKey::UtilTimestampMinute => "minute_of",
            BackDoorKey::UtilTimestampMonth => "month_of",
            BackDoorKey::UtilTimestampSecond => "second_of",
            BackDoorKey::UtilTimestampYear => "year_of",
            BackDoorKey::UtilToCSV => "to_csv",
            BackDoorKey::UtilUUID => "uuid",
            BackDoorKey::VmEval => "eval",
            BackDoorKey::VmReset => "reset",
            BackDoorKey::VmServe => "serve",
            BackDoorKey::VmVariables => "vars",
            BackDoorKey::VmVersion => "version",
            BackDoorKey::UtilToF32 => "to_f32",
            BackDoorKey::UtilToF64 => "to_f64",
            BackDoorKey::UtilToI8 => "to_i8",
            BackDoorKey::UtilToI16 => "to_i16",
            BackDoorKey::UtilToI32 => "to_i32",
            BackDoorKey::UtilToI64 => "to_i64",
            BackDoorKey::UtilToI128 => "to_i128",
            BackDoorKey::UtilToJSON => "to_json",
            BackDoorKey::UtilToTable => "to_table",
            BackDoorKey::UtilToU8 => "to_u8",
            BackDoorKey::UtilToU16 => "to_u16",
            BackDoorKey::UtilToU32 => "to_u32",
            BackDoorKey::UtilToU64 => "to_u64",
            BackDoorKey::UtilToU128 => "to_u128",
        };
        result.to_string()
    }

    fn get_parameters(&self) -> Vec<Parameter> {
        let param_count: u8 = match self {
            BackDoorKey::IoStdErr => 1,
            BackDoorKey::IoStdOut => 1,
            BackDoorKey::LangAssert => 1,
            BackDoorKey::LangMatches => 2,
            BackDoorKey::LangTypeOf => 1,
            BackDoorKey::OsEnvVars => 0,
            BackDoorKey::OsSysCall => 1,
            BackDoorKey::StrFormat => 2,
            BackDoorKey::StrLeft => 2,
            BackDoorKey::StrRight => 2,
            BackDoorKey::StrSubstring => 3,
            BackDoorKey::StrToString => 1,
            BackDoorKey::UtilTimestamp => 0,
            BackDoorKey::UtilTimestampDay => 1,
            BackDoorKey::UtilTimestampHour => 1,
            BackDoorKey::UtilTimestampHour12 => 1,
            BackDoorKey::UtilTimestampMinute => 1,
            BackDoorKey::UtilTimestampMonth => 1,
            BackDoorKey::UtilTimestampSecond => 1,
            BackDoorKey::UtilTimestampYear => 1,
            BackDoorKey::UtilToCSV => 1,
            BackDoorKey::UtilToF32 => 1,
            BackDoorKey::UtilToF64 => 1,
            BackDoorKey::UtilToI8 => 1,
            BackDoorKey::UtilToI16 => 1,
            BackDoorKey::UtilToI32 => 1,
            BackDoorKey::UtilToI64 => 1,
            BackDoorKey::UtilToI128 => 1,
            BackDoorKey::UtilToJSON => 1,
            BackDoorKey::UtilToTable => 1,
            BackDoorKey::UtilToU8 => 1,
            BackDoorKey::UtilToU16 => 1,
            BackDoorKey::UtilToU32 => 1,
            BackDoorKey::UtilToU64 => 1,
            BackDoorKey::UtilToU128 => 1,
            BackDoorKey::UtilUUID => 0,
            BackDoorKey::VmEval => 1,
            BackDoorKey::VmReset => 0,
            BackDoorKey::VmServe => 1,
            BackDoorKey::VmVariables => 0,
            BackDoorKey::VmVersion => 0,
        };
        (0..param_count)
            .map(|n| Parameter::new(format!("{}", (n + 97) as char), None, None))
            .collect()
    }

    pub fn get_type(&self) -> DataType {
        FunctionType(self.get_parameters())
    }

    pub fn to_code(&self) -> String {
        let name = self.get_name();
        let params = self.get_parameters().iter()
            .map(|p| p.to_code())
            .collect::<Vec<_>>()
            .join(", ");
        format!("{name}({params})")
    }

    pub fn to_u8(&self) -> u8 {
        *self as u8
    }
}

/// Unit tests
#[cfg(test)]
mod tests {
    use crate::backdoor::{BackDoorKey, BACK_DOOR_KEYS};

    #[test]
    fn test_to_code() {
        for key in BACK_DOOR_KEYS {
            println!("assert_eq!(BackDoorKey::{:?}.to_code(), \"{}\".to_string());", key, key.to_code())
        }
        assert_eq!(BackDoorKey::IoStdErr.to_code(), "stderr(a)".to_string());
        assert_eq!(BackDoorKey::IoStdOut.to_code(), "stdout(a)".to_string());
        assert_eq!(BackDoorKey::LangAssert.to_code(), "assert(a)".to_string());
        assert_eq!(BackDoorKey::LangMatches.to_code(), "matches(a, b)".to_string());
        assert_eq!(BackDoorKey::LangTypeOf.to_code(), "type_of(a)".to_string());
        assert_eq!(BackDoorKey::OsEnvVars.to_code(), "env()".to_string());
        assert_eq!(BackDoorKey::OsSysCall.to_code(), "call(a)".to_string());
        assert_eq!(BackDoorKey::StrFormat.to_code(), "format(a, b)".to_string());
        assert_eq!(BackDoorKey::StrLeft.to_code(), "left(a, b)".to_string());
        assert_eq!(BackDoorKey::StrRight.to_code(), "right(a, b)".to_string());
        assert_eq!(BackDoorKey::StrSubstring.to_code(), "substring(a, b, c)".to_string());
        assert_eq!(BackDoorKey::StrToString.to_code(), "to_string(a)".to_string());
        assert_eq!(BackDoorKey::UtilToCSV.to_code(), "to_csv(a)".to_string());
        assert_eq!(BackDoorKey::UtilToF32.to_code(), "to_f32(a)".to_string());
        assert_eq!(BackDoorKey::UtilToF64.to_code(), "to_f64(a)".to_string());
        assert_eq!(BackDoorKey::UtilToI8.to_code(), "to_i8(a)".to_string());
        assert_eq!(BackDoorKey::UtilToI16.to_code(), "to_i16(a)".to_string());
        assert_eq!(BackDoorKey::UtilToI32.to_code(), "to_i32(a)".to_string());
        assert_eq!(BackDoorKey::UtilToI64.to_code(), "to_i64(a)".to_string());
        assert_eq!(BackDoorKey::UtilToI128.to_code(), "to_i128(a)".to_string());
        assert_eq!(BackDoorKey::UtilToJSON.to_code(), "to_json(a)".to_string());
        assert_eq!(BackDoorKey::UtilToU8.to_code(), "to_u8(a)".to_string());
        assert_eq!(BackDoorKey::UtilToU16.to_code(), "to_u16(a)".to_string());
        assert_eq!(BackDoorKey::UtilToU32.to_code(), "to_u32(a)".to_string());
        assert_eq!(BackDoorKey::UtilToU64.to_code(), "to_u64(a)".to_string());
        assert_eq!(BackDoorKey::UtilToU128.to_code(), "to_u128(a)".to_string());
        assert_eq!(BackDoorKey::UtilTimestamp.to_code(), "timestamp()".to_string());
        assert_eq!(BackDoorKey::UtilTimestampDay.to_code(), "day_of(a)".to_string());
        assert_eq!(BackDoorKey::UtilTimestampHour.to_code(), "hour_of(a)".to_string());
        assert_eq!(BackDoorKey::UtilTimestampHour12.to_code(), "hour_12_of(a)".to_string());
        assert_eq!(BackDoorKey::UtilTimestampMinute.to_code(), "minute_of(a)".to_string());
        assert_eq!(BackDoorKey::UtilTimestampMonth.to_code(), "month_of(a)".to_string());
        assert_eq!(BackDoorKey::UtilTimestampSecond.to_code(), "second_of(a)".to_string());
        assert_eq!(BackDoorKey::UtilTimestampYear.to_code(), "year_of(a)".to_string());
        assert_eq!(BackDoorKey::UtilToTable.to_code(), "to_table(a)".to_string());
        assert_eq!(BackDoorKey::UtilUUID.to_code(), "uuid()".to_string());
        assert_eq!(BackDoorKey::VmEval.to_code(), "eval(a)".to_string());
        assert_eq!(BackDoorKey::VmReset.to_code(), "reset()".to_string());
        assert_eq!(BackDoorKey::VmServe.to_code(), "serve(a)".to_string());
        assert_eq!(BackDoorKey::VmVariables.to_code(), "vars()".to_string());
        assert_eq!(BackDoorKey::VmVersion.to_code(), "version()".to_string());
    }

    #[test]
    fn test_to_u8() {
        for model in BACK_DOOR_KEYS {
            assert_eq!(model, BackDoorKey::from_u8(model.to_u8()));
        }
    }
}