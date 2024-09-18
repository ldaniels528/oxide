////////////////////////////////////////////////////////////////////
// Mnemonic enumerations
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

/// Represents a mnemonic enumeration
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Mnemonic {
    EAnd = 0,
    EAppend = 4,
    EArrayLit = 8,
    EAsValue = 12,
    EBetween = 16,
    EBetwixt = 20,
    EBitwiseAnd = 24,
    EBitwiseOr = 28,
    EBitwiseXor = 32,
    ECodeBlock = 36,
    ECompact = 40,
    EContains = 44,
    ECreateIndex = 48,
    ECreateTable = 52,
    EDeclareIndex = 56,
    EDeclareTable = 60,
    EDelete = 64,
    EDescribe = 68,
    ECsv = 72,
    EDivide = 76,
    EDrop = 80,
    EElemIndex = 82,
    EEqual = 84,
    EFactorial = 86,
    EFeature = 88,
    EFrom = 90,
    EFunction = 92,
    EGreaterThan = 94,
    EGreaterOrEqual = 96,
    EHttp = 98,
    EIf = 100,
    EInclude = 105,
    EIntoNs = 110,
    EJsonLiteral = 115,
    ELessThan = 120,
    ELessOrEqual = 125,
    ELimit = 130,
    ELiteral = 135,
    EMinus = 140,
    EModulo = 145,
    EMultiply = 147,
    EMustAck = 149,
    EMustDie = 151,
    EMustIgnoreAck = 153,
    EMustNotAck = 155,
    ENeg = 157,
    ENot = 160,
    ENotEqual = 165,
    ENs = 170,
    EOr = 175,
    EOverwrite = 180,
    EPlus = 185,
    EPow = 190,
    ERange = 195,
    EReturn = 200,
    EReverse = 205,
    EScan = 207,
    EScenario = 209,
    ESelect = 210,
    EServe = 215,
    EShiftLeft = 225,
    EShiftRight = 230,
    EStderr = 233,
    EStdout = 235,
    ETruncate = 237,
    ETuple = 239,
    EUndelete = 243,
    EUpdate = 245,
    EVarGet = 247,
    EVarSet = 249,
    EVia = 250,
    EWhere = 251,
    EWhile = 255,
}

impl Mnemonic {
    pub fn from_u8(value: u8) -> Mnemonic {
        for op in Self::values() {
            if op.to_u8() == value { return op }
        }
        panic!("missing Mnemonic::from_u8({})", value);
    }

    pub fn to_u8(&self) -> u8 {
        *self as u8
    }

    pub fn values() -> Vec<Mnemonic> {
        use Mnemonic::*;
        vec![
            EAnd, EAppend, EArrayLit, EAsValue,
            EBetween, EBetwixt, EBitwiseAnd, EBitwiseOr, EBitwiseXor,
            ECodeBlock, ECompact, EContains, ECsv,
            ECreateIndex, ECreateTable,
            EDeclareIndex, EDeclareTable, EDelete, EDescribe,
            EDivide, EDrop,
            EElemIndex, EEqual,
            EFactorial, EFeature, EFrom, EFunction,
            EGreaterThan, EGreaterOrEqual,
            EHttp,
            EIf, EInclude, EIntoNs,
            EJsonLiteral,
            ELessThan, ELessOrEqual, ELimit, ELiteral,
            EMinus, EModulo, EMultiply, EMustAck, EMustDie, EMustIgnoreAck, EMustNotAck,
            ENeg, ENot, ENotEqual, ENs,
            EOr, EOverwrite,
            EPlus, EPow,
            ERange, EReturn, EReverse,
            EScan, EScenario, ESelect, EServe, EShiftLeft, EShiftRight, EStderr, EStdout,
            ETruncate, ETuple,
            EUndelete, EUpdate,
            EVarGet, EVarSet, EVia,
            EWhere, EWhile,
        ]
    }
}