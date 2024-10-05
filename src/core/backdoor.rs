////////////////////////////////////////////////////////////////////
// BackDoor module
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

/// Represents a backdoor (hook) for calling native functions
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BackDoorFunction {
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
    BxToCSV = 13,
    BxToJSON = 14,
    BxTypeOf = 15,
    BxUUID = 16,
    BxVariables = 17,
}

impl BackDoorFunction {
    pub fn from_u8(code: u8) -> Self {
        Self::from(Self::values()[code as usize].to_owned())
    }

    pub fn to_u8(&self) -> u8 {
        *self as u8
    }

    pub fn values() -> Vec<BackDoorFunction> {
        use BackDoorFunction::*;
        vec![
            BxAssert, BxEval, BxFormat, BxLeft, BxMatches, BxReset, BxRight,
            BxServe, BxStdErr, BxStdOut, BxSubstring, BxSysCall,
            BxTimestamp, BxToCSV, BxToJSON, BxTypeOf, BxUUID, BxVariables,
        ]
    }
}