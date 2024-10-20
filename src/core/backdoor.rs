////////////////////////////////////////////////////////////////////
// BackDoor module
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

use BackDoorKey::*;

/// Represents a backdoor (hook) for calling native functions
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum BackDoorKey {
    BxAssert = 0,
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
    BxEnvVars = 25,
}

pub const BACK_DOOR_KEYS: [BackDoorKey; 26] = [
    BxAssert, BxEval, BxFormat, BxLeft, BxMatches, BxReset, BxRight,
    BxServe, BxStdErr, BxStdOut, BxSubstring, BxSysCall,
    BxTimestamp, BxTimestampDay, BxTimestampHour, BxTimestampHour12,
    BxTimestampMinute, BxTimestampMonth, BxTimestampSecond, BxTimestampYear,
    BxToCSV, BxToJSON, BxTypeOf, BxUUID, BxVariables, BxEnvVars,
];

impl BackDoorKey {
    pub fn from_u8(code: u8) -> Self {
        Self::from(BACK_DOOR_KEYS[code as usize].to_owned())
    }

    pub fn to_code(&self) -> String {
        let result = match self {
            BxAssert => "assert",
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
            BxEnvVars => "env",
        };
        result.to_string()
    }

    pub fn to_u8(&self) -> u8 {
        *self as u8
    }
}