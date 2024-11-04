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
    // cnv package
    CnvToF32 = 27,
    CnvToF64 = 28,
    CnvToI8 = 29,
    CnvToI16 = 30,
    CnvToI32 = 31,
    CnvToI64 = 32,
    CnvToI128 = 33,
    CnvToU8 = 34,
    CnvToU16 = 35,
    CnvToU32 = 36,
    CnvToU64 = 37,
    CnvToU128 = 38,
    // lang package
    LangAssert = 0,
    LangMatches = 4,
    LangTypeOf = 22,
    // io package
    IoStdErr = 8,
    IoStdOut = 9,
    // os package
    OsEnvVars = 25,
    OsSysCall = 11,
    // str package
    StrFormat = 2,
    StrLeft = 3,
    StrRight = 6,
    StrSubstring = 10,
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
    UtilToJSON = 21,
    UtilUUID = 23,
    // vm package
    VmEval = 1,
    VmReset = 5,
    VmServe = 7,
    VmVariables = 24,
    VmVersion = 26,
}

const BACK_DOOR_KEYS: [BackDoorKey; 39] = [
    // cnv
    CnvToF32, CnvToF64,
    CnvToI8, CnvToI16, CnvToI32, CnvToI64, CnvToI128,
    CnvToU8, CnvToU16, CnvToU32, CnvToU64, CnvToU128,
    // io
    IoStdErr, IoStdOut,
    // lang
    LangAssert, LangMatches, LangTypeOf,
    // os
    OsEnvVars, OsSysCall,
    // str
    StrFormat, StrLeft, StrRight, StrSubstring,
    // util
    UtilTimestamp, UtilTimestampDay, UtilTimestampHour, UtilTimestampHour12,
    UtilTimestampMinute, UtilTimestampMonth, UtilTimestampSecond, UtilTimestampYear,
    UtilToCSV, UtilToJSON, UtilUUID,
    // vm
    VmEval, VmReset, VmServe, VmVariables, VmVersion,
];

impl BackDoorKey {
    pub fn from_u8(code: u8) -> Self {
        for bdk in BACK_DOOR_KEYS {
            if bdk.to_u8() == code {
                return bdk
            }
        }
        panic!("No key for {code} was found")
    }

    fn get_name(&self) -> String {
        let result = match self {
            BackDoorKey::LangAssert => "assert",
            BackDoorKey::OsEnvVars => "env",
            BackDoorKey::VmEval => "eval",
            BackDoorKey::StrFormat => "format",
            BackDoorKey::StrLeft => "left",
            BackDoorKey::LangMatches => "matches",
            BackDoorKey::VmReset => "reset",
            BackDoorKey::StrRight => "right",
            BackDoorKey::VmServe => "serve",
            BackDoorKey::IoStdErr => "stderr",
            BackDoorKey::IoStdOut => "stdout",
            BackDoorKey::StrSubstring => "substring",
            BackDoorKey::OsSysCall => "call",
            BackDoorKey::UtilTimestamp => "timestamp",
            BackDoorKey::UtilTimestampDay => "day_of",
            BackDoorKey::UtilTimestampHour => "hour_of",
            BackDoorKey::UtilTimestampHour12 => "hour_12_of",
            BackDoorKey::UtilTimestampMinute => "minute_of",
            BackDoorKey::UtilTimestampMonth => "month_of",
            BackDoorKey::UtilTimestampSecond => "second_of",
            BackDoorKey::UtilTimestampYear => "year_of",
            BackDoorKey::UtilToCSV => "to_csv",
            BackDoorKey::UtilToJSON => "to_json",
            BackDoorKey::LangTypeOf => "type_of",
            BackDoorKey::UtilUUID => "uuid",
            BackDoorKey::VmVariables => "vars",
            BackDoorKey::VmVersion => "version",
            BackDoorKey::CnvToF32 => "to_f32",
            BackDoorKey::CnvToF64 => "to_f64",
            BackDoorKey::CnvToI8 => "to_i8",
            BackDoorKey::CnvToI16 => "to_i16",
            BackDoorKey::CnvToI32 => "to_i32",
            BackDoorKey::CnvToI64 => "to_i64",
            BackDoorKey::CnvToI128 => "to_i128",
            BackDoorKey::CnvToU8 => "to_u8",
            BackDoorKey::CnvToU16 => "to_u16",
            BackDoorKey::CnvToU32 => "to_u32",
            BackDoorKey::CnvToU64 => "to_u64",
            BackDoorKey::CnvToU128 => "to_u128",
        };
        result.to_string()
    }

    fn get_parameters(&self) -> Vec<Parameter> {
        let param_count: u8 = match self {
            BackDoorKey::LangAssert => 1,
            BackDoorKey::OsEnvVars => 0,
            BackDoorKey::VmEval => 1,
            BackDoorKey::StrFormat => 2,
            BackDoorKey::StrLeft => 2,
            BackDoorKey::LangMatches => 2,
            BackDoorKey::VmReset => 0,
            BackDoorKey::StrRight => 2,
            BackDoorKey::VmServe => 1,
            BackDoorKey::IoStdErr => 1,
            BackDoorKey::IoStdOut => 1,
            BackDoorKey::StrSubstring => 3,
            BackDoorKey::OsSysCall => 1,
            BackDoorKey::UtilTimestamp => 0,
            BackDoorKey::UtilTimestampDay => 1,
            BackDoorKey::UtilTimestampHour => 1,
            BackDoorKey::UtilTimestampHour12 => 1,
            BackDoorKey::UtilTimestampMinute => 1,
            BackDoorKey::UtilTimestampMonth => 1,
            BackDoorKey::UtilTimestampSecond => 1,
            BackDoorKey::UtilTimestampYear => 1,
            BackDoorKey::UtilToCSV => 1,
            BackDoorKey::UtilToJSON => 1,
            BackDoorKey::LangTypeOf => 1,
            BackDoorKey::UtilUUID => 0,
            BackDoorKey::VmVariables => 0,
            BackDoorKey::VmVersion => 0,
            BackDoorKey::CnvToF32 => 1,
            BackDoorKey::CnvToF64 => 1,
            BackDoorKey::CnvToI8 => 1,
            BackDoorKey::CnvToI16 => 1,
            BackDoorKey::CnvToI32 => 1,
            BackDoorKey::CnvToI64 => 1,
            BackDoorKey::CnvToI128 => 1,
            BackDoorKey::CnvToU8 => 1,
            BackDoorKey::CnvToU16 => 1,
            BackDoorKey::CnvToU32 => 1,
            BackDoorKey::CnvToU64 => 1,
            BackDoorKey::CnvToU128 => 1,
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
        assert_eq!(BackDoorKey::LangAssert.to_code(), "assert(a)".to_string());
        assert_eq!(BackDoorKey::VmEval.to_code(), "eval(a)".to_string());
        assert_eq!(BackDoorKey::StrFormat.to_code(), "format(a, b)".to_string());
        assert_eq!(BackDoorKey::StrLeft.to_code(), "left(a, b)".to_string());
        assert_eq!(BackDoorKey::LangMatches.to_code(), "matches(a, b)".to_string());
        assert_eq!(BackDoorKey::VmReset.to_code(), "reset()".to_string());
        assert_eq!(BackDoorKey::StrRight.to_code(), "right(a, b)".to_string());
        assert_eq!(BackDoorKey::VmServe.to_code(), "serve(a)".to_string());
        assert_eq!(BackDoorKey::IoStdErr.to_code(), "stderr(a)".to_string());
        assert_eq!(BackDoorKey::IoStdOut.to_code(), "stdout(a)".to_string());
        assert_eq!(BackDoorKey::StrSubstring.to_code(), "substring(a, b, c)".to_string());
        assert_eq!(BackDoorKey::OsSysCall.to_code(), "call(a)".to_string());
        assert_eq!(BackDoorKey::UtilTimestamp.to_code(), "timestamp()".to_string());
        assert_eq!(BackDoorKey::UtilTimestampDay.to_code(), "day_of(a)".to_string());
        assert_eq!(BackDoorKey::UtilTimestampHour.to_code(), "hour_of(a)".to_string());
        assert_eq!(BackDoorKey::UtilTimestampHour12.to_code(), "hour_12_of(a)".to_string());
        assert_eq!(BackDoorKey::UtilTimestampMinute.to_code(), "minute_of(a)".to_string());
        assert_eq!(BackDoorKey::UtilTimestampMonth.to_code(), "month_of(a)".to_string());
        assert_eq!(BackDoorKey::UtilTimestampSecond.to_code(), "second_of(a)".to_string());
        assert_eq!(BackDoorKey::UtilTimestampYear.to_code(), "year_of(a)".to_string());
        assert_eq!(BackDoorKey::UtilToCSV.to_code(), "to_csv(a)".to_string());
        assert_eq!(BackDoorKey::UtilToJSON.to_code(), "to_json(a)".to_string());
        assert_eq!(BackDoorKey::LangTypeOf.to_code(), "type_of(a)".to_string());
        assert_eq!(BackDoorKey::UtilUUID.to_code(), "uuid()".to_string());
        assert_eq!(BackDoorKey::VmVariables.to_code(), "vars()".to_string());
        assert_eq!(BackDoorKey::OsEnvVars.to_code(), "env()".to_string());
        assert_eq!(BackDoorKey::VmVersion.to_code(), "version()".to_string());
        assert_eq!(BackDoorKey::CnvToF32.to_code(), "to_f32(a)".to_string());
        assert_eq!(BackDoorKey::CnvToF64.to_code(), "to_f64(a)".to_string());
        assert_eq!(BackDoorKey::CnvToI8.to_code(), "to_i8(a)".to_string());
        assert_eq!(BackDoorKey::CnvToI16.to_code(), "to_i16(a)".to_string());
        assert_eq!(BackDoorKey::CnvToI32.to_code(), "to_i32(a)".to_string());
        assert_eq!(BackDoorKey::CnvToI64.to_code(), "to_i64(a)".to_string());
        assert_eq!(BackDoorKey::CnvToI128.to_code(), "to_i128(a)".to_string());
        assert_eq!(BackDoorKey::CnvToU8.to_code(), "to_u8(a)".to_string());
        assert_eq!(BackDoorKey::CnvToU16.to_code(), "to_u16(a)".to_string());
        assert_eq!(BackDoorKey::CnvToU32.to_code(), "to_u32(a)".to_string());
        assert_eq!(BackDoorKey::CnvToU64.to_code(), "to_u64(a)".to_string());
        assert_eq!(BackDoorKey::CnvToU128.to_code(), "to_u128(a)".to_string());
    }

    #[test]
    fn test_to_u8() {
        let model = BackDoorKey::VmVersion;
        assert_eq!(model, BackDoorKey::from_u8(model.to_u8()));
    }
}