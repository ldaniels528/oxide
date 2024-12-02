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

impl NumberKind {
    pub fn compute_max_physical_size(&self) -> usize {
        use NumberKind::*;
        match self {
            I8Kind | U8Kind => 1,
            I16Kind | U16Kind => 2,
            F32Kind | I32Kind | U32Kind => 4,
            F64Kind | I64Kind | U64Kind => 8,
            I128Kind | U128Kind => 16,
            NaNKind => 0,
        }
    }

    pub fn get_type_name(&self) -> String {
        use NumberKind::*;
        match self {
            F32Kind => "f32".into(),
            F64Kind => "f64".into(),
            I8Kind => "i8".into(),
            I16Kind => "i16".into(),
            I32Kind => "i32".into(),
            I64Kind => "i64".into(),
            I128Kind => "i128".into(),
            U8Kind => "u8".into(),
            U16Kind => "u16".into(),
            U32Kind => "u32".into(),
            U64Kind => "u64".into(),
            U128Kind => "u128".into(),
            NaNKind => "NaN".into()
        }
    }
}