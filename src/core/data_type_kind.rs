////////////////////////////////////////////////////////////////////
// DataTypeKind enumeration
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

use crate::data_type_kind::DataTypeKind::*;

pub const DATA_TYPE_KINDS: [DataTypeKind; 33] = [
    TxAck, TxArray, TxBackDoor, TxBlob, TxBoolean,
    TxClob, TxDate, TxError, TxEnum, TxFunction, TxJsonObject,
    TxNamespace, TxNull,
    TxNumberF32, TxNumberF64,
    TxNumberI8, TxNumberI16, TxNumberI32, TxNumberI64, TxNumberI128,
    TxNumberU8, TxNumberU16, TxNumberU32, TxNumberU64, TxNumberU128, TxNumberNaN,
    TxRowId, TxRowsAffected, TxString, TxStructure, TxTableValue,
    TxUndefined, TxUUID,
];

pub const T_NUMBER_START: u8 = 56;

// Represents a data type kind or class
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DataTypeKind {
    TxAck = 0,
    TxArray = 8,
    TxBackDoor = 12,
    TxBlob = 16,
    TxBoolean = 20,
    TxClob = 24,
    TxDate = 28,
    TxError = 32,
    TxEnum = 36,
    TxFunction = 40,
    TxJsonObject = 44,
    TxNamespace = 48,
    TxNull = 52,
    TxNumberF32 = T_NUMBER_START + 0,
    TxNumberF64 = T_NUMBER_START + 1,
    TxNumberI8 = T_NUMBER_START + 2,
    TxNumberI16 = T_NUMBER_START + 3,
    TxNumberI32 = T_NUMBER_START + 4,
    TxNumberI64 = T_NUMBER_START + 5,
    TxNumberI128 = T_NUMBER_START + 6,
    TxNumberU8 = T_NUMBER_START + 7,
    TxNumberU16 = T_NUMBER_START + 8,
    TxNumberU32 = T_NUMBER_START + 9,
    TxNumberU64 = T_NUMBER_START + 10,
    TxNumberU128 = T_NUMBER_START + 11,
    TxNumberNaN = T_NUMBER_START + 12,
    TxRowId = 70,
    TxRowsAffected = 72,
    TxString = 74,
    TxStructure = 78,
    TxTableValue = 82,
    TxUndefined = 90,
    TxUUID = 94,
}

impl DataTypeKind {
    pub fn from_u8(value: u8) -> DataTypeKind {
        for dtk in DATA_TYPE_KINDS {
            if dtk.to_u8() == value { return  dtk }
        }
        panic!("missing DataTypeKind::from_u8({})", value);
    }

    pub fn to_u8(&self) -> u8 {
        *self as u8
    }
}