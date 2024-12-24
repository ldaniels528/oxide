#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// FieldMetadata class
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

/// Represents the metadata of a field
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct FieldMetadata {
    pub(crate) is_active: bool,
    pub(crate) is_compressed: bool,
}

impl FieldMetadata {
    pub fn new(is_active: bool) -> Self {
        Self { is_active, is_compressed: false }
    }

    pub fn decode(metadata: u8) -> Self {
        Self {
            is_active: metadata & 0b1000_0000u8 > 0,
            is_compressed: metadata & 0b0100_0000u8 > 0,
        }
    }

    pub fn encode(&self) -> u8 {
        let b2n: fn(bool) -> usize = |b: bool| if b { 1 } else { 0 };
        ((b2n(self.is_active) << 7) |
            (b2n(self.is_compressed) << 6)) as u8
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_decode() {
        let aa = FieldMetadata::decode(0b0000_0000u8);
        assert_eq!(aa.is_active, false);
        assert_eq!(aa.is_compressed, false);

        let bb = FieldMetadata::decode(0b1100_0000u8);
        assert_eq!(bb.is_active, true);
        assert_eq!(bb.is_compressed, true);

        let cc = FieldMetadata::decode(0b1000_0000u8);
        assert_eq!(cc.is_active, true);
        assert_eq!(cc.is_compressed, false);
    }

    #[test]
    fn test_metadata_encode() {
        let aa = FieldMetadata { is_active: true, is_compressed: true };
        assert_eq!(aa.encode(), 0b1100_0000u8);

        let bb = FieldMetadata { is_active: true, is_compressed: false };
        assert_eq!(bb.encode(), 0b1000_0000u8);

        let cc = FieldMetadata { is_active: false, is_compressed: true };
        assert_eq!(cc.encode(), 0b0100_0000u8);
    }
}