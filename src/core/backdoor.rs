////////////////////////////////////////////////////////////////////
// BackDoor module
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
    BxAssert = 0,
    BxEnvVars = 25,
    BxEval = 1,
    BxFormat = 2,
    BxLeft = 3,
    BxMatches = 4,
    BxReset = 5,
    BxRight = 6,
    BxServe = 7,
    BxStdErr = 8,
    BxStdOut = 9,
    BxSubstring = 10,
    BxSysCall = 11,
    BxTimestamp = 12,
    BxTimestampDay = 13,
    BxTimestampHour = 14,
    BxTimestampHour12 = 15,
    BxTimestampMinute = 16,
    BxTimestampMonth = 17,
    BxTimestampSecond = 18,
    BxTimestampYear = 19,
    BxToCSV = 20,
    BxToJSON = 21,
    BxTypeOf = 22,
    BxUUID = 23,
    BxVariables = 24,
    BxVersion = 26,
}

const BACK_DOOR_KEYS: [BackDoorKey; 27] = [
    BxAssert, BxEval, BxFormat, BxLeft, BxMatches, BxReset, BxRight,
    BxServe, BxStdErr, BxStdOut, BxSubstring, BxSysCall,
    BxTimestamp, BxTimestampDay, BxTimestampHour, BxTimestampHour12,
    BxTimestampMinute, BxTimestampMonth, BxTimestampSecond, BxTimestampYear,
    BxToCSV, BxToJSON, BxTypeOf, BxUUID, BxVariables, BxEnvVars, BxVersion
];

impl BackDoorKey {
    pub fn from_u8(code: u8) -> Self {
        Self::from(BACK_DOOR_KEYS[code as usize].to_owned())
    }

    fn get_name(&self) -> String {
        let result = match self {
            BxAssert => "assert",
            BxEnvVars => "env",
            BxEval => "eval",
            BxFormat => "format",
            BxLeft => "left",
            BxMatches => "matches",
            BxReset => "reset",
            BxRight => "right",
            BxServe => "serve",
            BxStdErr => "stderr",
            BxStdOut => "stdout",
            BxSubstring => "substring",
            BxSysCall => "call",
            BxTimestamp => "timestamp",
            BxTimestampDay => "day_of",
            BxTimestampHour => "hour_of",
            BxTimestampHour12 => "hour_12_of",
            BxTimestampMinute => "minute_of",
            BxTimestampMonth => "month_of",
            BxTimestampSecond => "second_of",
            BxTimestampYear => "year_of",
            BxToCSV => "to_csv",
            BxToJSON => "to_json",
            BxTypeOf => "type_of",
            BxUUID => "uuid",
            BxVariables => "vars",
            BxVersion => "version",
        };
        result.to_string()
    }

    fn get_parameters(&self) -> Vec<Parameter> {
        let param_count = match self {
            BxAssert => 1,
            BxEnvVars => 0,
            BxEval => 1,
            BxFormat => 2,
            BxLeft => 2,
            BxMatches => 2,
            BxReset => 0,
            BxRight => 2,
            BxServe => 1,
            BxStdErr => 1,
            BxStdOut => 1,
            BxSubstring => 3,
            BxSysCall => 1,
            BxTimestamp => 0,
            BxTimestampDay => 1,
            BxTimestampHour => 1,
            BxTimestampHour12 => 1,
            BxTimestampMinute => 1,
            BxTimestampMonth => 1,
            BxTimestampSecond => 1,
            BxTimestampYear => 1,
            BxToCSV => 1,
            BxToJSON => 1,
            BxTypeOf => 1,
            BxUUID => 0,
            BxVariables => 0,
            BxVersion => 0,
        };
        (0..param_count)
            .map(|n| Parameter::new(format!("arg{n}"), None, None))
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
        assert_eq!(BackDoorKey::BxAssert.to_code(), "assert(arg0)".to_string());
        assert_eq!(BackDoorKey::BxEval.to_code(), "eval(arg0)".to_string());
        assert_eq!(BackDoorKey::BxFormat.to_code(), "format(arg0, arg1)".to_string());
        assert_eq!(BackDoorKey::BxLeft.to_code(), "left(arg0, arg1)".to_string());
        assert_eq!(BackDoorKey::BxMatches.to_code(), "matches(arg0, arg1)".to_string());
        assert_eq!(BackDoorKey::BxReset.to_code(), "reset()".to_string());
        assert_eq!(BackDoorKey::BxRight.to_code(), "right(arg0, arg1)".to_string());
        assert_eq!(BackDoorKey::BxServe.to_code(), "serve(arg0)".to_string());
        assert_eq!(BackDoorKey::BxStdErr.to_code(), "stderr(arg0)".to_string());
        assert_eq!(BackDoorKey::BxStdOut.to_code(), "stdout(arg0)".to_string());
        assert_eq!(BackDoorKey::BxSubstring.to_code(), "substring(arg0, arg1, arg2)".to_string());
        assert_eq!(BackDoorKey::BxSysCall.to_code(), "call(arg0)".to_string());
        assert_eq!(BackDoorKey::BxTimestamp.to_code(), "timestamp()".to_string());
        assert_eq!(BackDoorKey::BxTimestampDay.to_code(), "day_of(arg0)".to_string());
        assert_eq!(BackDoorKey::BxTimestampHour.to_code(), "hour_of(arg0)".to_string());
        assert_eq!(BackDoorKey::BxTimestampHour12.to_code(), "hour_12_of(arg0)".to_string());
        assert_eq!(BackDoorKey::BxTimestampMinute.to_code(), "minute_of(arg0)".to_string());
        assert_eq!(BackDoorKey::BxTimestampMonth.to_code(), "month_of(arg0)".to_string());
        assert_eq!(BackDoorKey::BxTimestampSecond.to_code(), "second_of(arg0)".to_string());
        assert_eq!(BackDoorKey::BxTimestampYear.to_code(), "year_of(arg0)".to_string());
        assert_eq!(BackDoorKey::BxToCSV.to_code(), "to_csv(arg0)".to_string());
        assert_eq!(BackDoorKey::BxToJSON.to_code(), "to_json(arg0)".to_string());
        assert_eq!(BackDoorKey::BxTypeOf.to_code(), "type_of(arg0)".to_string());
        assert_eq!(BackDoorKey::BxUUID.to_code(), "uuid()".to_string());
        assert_eq!(BackDoorKey::BxVariables.to_code(), "vars()".to_string());
        assert_eq!(BackDoorKey::BxEnvVars.to_code(), "env()".to_string());
        assert_eq!(BackDoorKey::BxVersion.to_code(), "version()".to_string());
    }

    #[test]
    fn test_to_u8() {
        let model = BackDoorKey::BxVersion;
        assert_eq!(model, BackDoorKey::from_u8(model.to_u8()));
    }
}