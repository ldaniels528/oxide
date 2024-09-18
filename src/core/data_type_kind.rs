////////////////////////////////////////////////////////////////////
// DataTypeKind enumeration
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

pub const T_NUMBER_START: u8 = 72;

// Represents a data type kind or class
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DataTypeKind {
    TAck = 0,
    TUndefined = 1,
    TNull = 4,
    TArray = 8,
    TBackDoor = 10,
    TBlob = 12,
    TBoolean = 16,
    TClob = 20,
    TDate = 24,
    TError = 28,
    TEnum = 32,
    TFunction = 44,
    TJsonObject = 68,
    TNamespace = 70,
    TNumberF32 = T_NUMBER_START + 0,
    TNumberF64 = T_NUMBER_START + 1,
    TNumberI8 = T_NUMBER_START + 2,
    TNumberI16 = T_NUMBER_START + 3,
    TNumberI32 = T_NUMBER_START + 4,
    TNumberI64 = T_NUMBER_START + 5,
    TNumberI128 = T_NUMBER_START + 6,
    TNumberU8 = T_NUMBER_START + 7,
    TNumberU16 = T_NUMBER_START + 8,
    TNumberU32 = T_NUMBER_START + 9,
    TNumberU64 = T_NUMBER_START + 10,
    TNumberU128 = T_NUMBER_START + 11,
    TNumberNan = T_NUMBER_START + 12,
    TRowsAffected = 96,
    TString = 100,
    TStructure = 104,
    TTableValue = 108,
    TTuple = 112,
    TUUID = 116,
}

impl DataTypeKind {
    pub fn from_u8(value: u8) -> DataTypeKind {
        for dtk in Self::values() {
            if dtk.to_u8() == value { return  dtk }
        }
        panic!("missing DataTypeKind::from_u8({})", value);
    }

    pub fn to_u8(&self) -> u8 {
        *self as u8
    }

    pub fn values() -> Vec<DataTypeKind> {
        use DataTypeKind::*;
        vec![
            TUndefined, TAck, TNull, TArray, TBackDoor, TBlob, TBoolean,
            TClob, TDate, TError, TEnum, TFunction, TJsonObject, TNamespace,
            TNumberF32, TNumberF64,
            TNumberI8, TNumberI16, TNumberI32, TNumberI64, TNumberI128,
            TNumberU8, TNumberU16, TNumberU32, TNumberU64, TNumberU128,
            TNumberNan, TRowsAffected, TString, TStructure, TTableValue, TTuple, TUUID,
        ]
    }
}