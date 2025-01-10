#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// FieldMetadata class
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

pub const ACTIVE_MASK: u8 = 0b1000_0000u8;
pub const COMPRESSED_MASK: u8 = 0b0100_0000u8;
pub const EXTERNAL_MASK: u8 = 0b0010_0000u8;

/// Represents the metadata of a field
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct FieldMetadata {
    pub is_active: bool,
    pub is_compressed: bool,
    pub is_external: bool,
}

impl FieldMetadata {
    pub fn build(
        is_active: bool,
        is_compressed: bool,
        is_external: bool,
    ) -> Self {
        Self { is_active, is_compressed, is_external }
    }

    pub fn new(is_active: bool) -> Self {
        Self::build(is_active, false, false)
    }

    pub fn decode(metadata: u8) -> Self {
        Self::build(
            metadata & ACTIVE_MASK > 0,
            metadata & COMPRESSED_MASK > 0,
            metadata & EXTERNAL_MASK > 0,
        )
    }

    pub fn encode(&self) -> u8 {
        let b2n: fn(bool) -> usize = |b: bool| if b { 1 } else { 0 };
        ((b2n(self.is_active) << 7) |
            (b2n(self.is_compressed) << 6) |
            (b2n(self.is_external) << 5)) as u8
    }
}

/// Unit tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_decode_xxx() {
        let fmd = FieldMetadata::decode(0);
        assert_eq!(fmd.is_active, false);
        assert_eq!(fmd.is_compressed, false);
        assert_eq!(fmd.is_external, false);
    }

    #[test]
    fn test_metadata_decode_ace() {
        let fmd = FieldMetadata::decode(ACTIVE_MASK | COMPRESSED_MASK | EXTERNAL_MASK);
        assert_eq!(fmd.is_active, true);
        assert_eq!(fmd.is_compressed, true);
        assert_eq!(fmd.is_external, true);
    }

    #[test]
    fn test_metadata_decode_axe() {
        let fmd = FieldMetadata::decode(ACTIVE_MASK | EXTERNAL_MASK);
        assert_eq!(fmd.is_active, true);
        assert_eq!(fmd.is_compressed, false);
        assert_eq!(fmd.is_external, true);
    }

    #[test]
    fn test_metadata_encode() {
        let aa = FieldMetadata { is_active: true, is_compressed: true, is_external: true };
        assert_eq!(aa.encode(), 0b1110_0000u8);

        let bb = FieldMetadata { is_active: true, is_compressed: false, is_external: true };
        assert_eq!(bb.encode(), 0b1010_0000u8);

        let cc = FieldMetadata { is_active: false, is_compressed: true, is_external: false };
        assert_eq!(cc.encode(), 0b0100_0000u8);
    }
}