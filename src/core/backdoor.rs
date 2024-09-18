////////////////////////////////////////////////////////////////////
// BackDoor module
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

/// Represents a backdoor (hook) for calling native functions
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BackDoorFunction {
    Assert = 0,
    Eval = 1,
    Format = 2,
    If = 3,
    Matches = 4,
    Reset = 5,
    StdErr = 6,
    StdOut = 7,
    SysCall = 8,
    ToCSV = 9,
    ToJSON = 10,
    TypeOf = 11,
    Variables = 12,
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
            Assert, Eval, Format, If, Matches, Reset, StdErr, StdOut, SysCall,
            ToCSV, ToJSON, TypeOf, Variables,
        ]
    }
}