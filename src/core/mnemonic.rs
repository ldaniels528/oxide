////////////////////////////////////////////////////////////////////
// Mnemonic enumerations
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

/// Represents a mnemonic enumeration
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Mnemonic {
    ExAnd = 0,
    ExAppend = 4,
    ExArrayLit = 8,
    ExAsValue = 12,
    ExBetween = 16,
    ExBetwixt = 20,
    ExBitwiseAnd = 24,
    ExBitwiseOr = 28,
    ExBitwiseXor = 32,
    ExBoolean = 34,
    ExCodeBlock = 36,
    ExCompact = 40,
    ExContains = 44,
    ExCreateIndex = 48,
    ExCreateTable = 52,
    ExDeclareIndex = 56,
    ExDeclareTable = 60,
    ExDelete = 64,
    ExDescribe = 68,
    ExCSV = 72,
    ExDivide = 76,
    ExDrop = 80,
    ExElemIndex = 82,
    ExEqual = 84,
    ExExtract = 85,
    ExFactorial = 86,
    ExFeature = 88,
    ExFrom = 90,
    ExFunctionCall = 92,
    ExGreaterThan = 94,
    ExGreaterOrEqual = 96,
    ExHTTP = 98,
    ExIf = 100,
    ExInclude = 105,
    ExIntoNS = 110,
    ExJsonLiteral = 115,
    ExLessThan = 120,
    ExLessOrEqual = 125,
    ExLimit = 130,
    ExLiteral = 135,
    ExMinus = 140,
    ExModulo = 145,
    ExMultiply = 147,
    ExMustAck = 149,
    ExMustDie = 151,
    ExMustIgnoreAck = 153,
    ExMustNotAck = 155,
    ExNeg = 157,
    ExNot = 160,
    ExNotEqual = 165,
    ExNS = 170,
    ExOr = 175,
    ExOverwrite = 180,
    ExPlus = 185,
    ExPow = 190,
    ExRange = 195,
    ExReturn = 200,
    ExReverse = 205,
    ExScan = 207,
    ExScenario = 209,
    ExSelect = 210,
    ExBitwiseShiftLeft = 225,
    ExBitwiseShiftRight = 230,
    ExStderr = 233,
    ExStdout = 235,
    ExStructureImpl = 236,
    ExTruncate = 237,
    ExTuple = 239,
    ExUndelete = 243,
    ExUpdate = 245,
    ExVarGet = 247,
    ExVarSet = 249,
    ExVia = 250,
    ExWhere = 251,
    ExWhile = 255,
}

impl Mnemonic {
    pub fn from_u8(value: u8) -> Mnemonic {
        for op in Self::values() {
            if op.to_u8() == value { return op.to_owned(); }
        }
        panic!("missing Mnemonic::from_u8({})", value);
    }

    pub fn to_u8(&self) -> u8 {
        *self as u8
    }

    /// Returns all values of Mnemonic as an array.
    pub fn values() -> &'static [Mnemonic] {
        use Mnemonic::*;
        &[
            ExAnd, ExAppend, ExArrayLit, ExAsValue,
            ExBetween, ExBetwixt, ExBitwiseAnd, ExBitwiseOr, ExBitwiseXor, ExBoolean,
            ExCodeBlock, ExCompact, ExContains, ExCSV,
            ExCreateIndex, ExCreateTable,
            ExDeclareIndex, ExDeclareTable, ExDelete, ExDescribe,
            ExDivide, ExDrop,
            ExElemIndex, ExEqual, ExExtract,
            ExFactorial, ExFeature, ExFrom, ExFunctionCall,
            ExGreaterThan, ExGreaterOrEqual,
            ExHTTP,
            ExIf, ExInclude, ExIntoNS,
            ExJsonLiteral,
            ExLessThan, ExLessOrEqual, ExLimit, ExLiteral,
            ExMinus, ExModulo, ExMultiply, ExMustAck, ExMustDie, ExMustIgnoreAck, ExMustNotAck,
            ExNeg, ExNot, ExNotEqual, ExNS,
            ExOr, ExOverwrite,
            ExPlus, ExPow,
            ExRange, ExReturn, ExReverse,
            ExScan, ExScenario, ExSelect, ExBitwiseShiftLeft, ExBitwiseShiftRight, ExStderr, ExStdout, ExStructureImpl,
            ExTruncate, ExTuple,
            ExUndelete, ExUpdate,
            ExVarGet, ExVarSet, ExVia,
            ExWhere, ExWhile,
        ]
    }
}