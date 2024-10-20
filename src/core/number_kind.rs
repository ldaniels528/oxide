////////////////////////////////////////////////////////////////////
//  NumberKind enumeration
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

// Represents a numeric type or kind of value
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum NumberKind {
    F32Kind = 0,
    F64Kind = 1,
    I8Kind = 2,
    I16Kind = 3,
    I32Kind = 4,
    I64Kind = 5,
    I128Kind = 6,
    U8Kind = 7,
    U16Kind = 8,
    U32Kind = 9,
    U64Kind = 10,
    U128Kind = 11,
    NaNKind = 12,
}
