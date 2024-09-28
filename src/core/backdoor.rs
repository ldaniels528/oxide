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
    BxStdErr = 7,
    BxStdOut = 8,
    BxSubstring = 9,
    BxSysCall = 10,
    BxToCSV = 11,
    BxToJSON = 12,
    BxTypeOf = 13,
    BxVariables = 14,
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
            BxStdErr, BxStdOut, BxSysCall, BxToCSV, BxToJSON,
            BxSubstring, BxTypeOf, BxVariables,
        ]
    }
}