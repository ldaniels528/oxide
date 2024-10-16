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
    BxToCSV = 13,
    BxToJSON = 14,
    BxTypeOf = 15,
    BxUUID = 16,
    BxVariables = 17,
}

pub const BACK_DOOR_KEYS: [BackDoorKey; 18] = [
    BxAssert, BxEval, BxFormat, BxLeft, BxMatches, BxReset, BxRight,
    BxServe, BxStdErr, BxStdOut, BxSubstring, BxSysCall,
    BxTimestamp, BxToCSV, BxToJSON, BxTypeOf, BxUUID, BxVariables,
];

impl BackDoorKey {
    pub fn from_u8(code: u8) -> Self {
        Self::from(BACK_DOOR_KEYS[code as usize].to_owned())
    }

    pub fn to_u8(&self) -> u8 {
        *self as u8
    }
}