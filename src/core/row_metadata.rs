////////////////////////////////////////////////////////////////////
// row metadata module
////////////////////////////////////////////////////////////////////

use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct RowMetadata {
    pub(crate) is_allocated: bool,
    pub(crate) is_blob: bool,
    pub(crate) is_encrypted: bool,
    pub(crate) is_replicated: bool,
}

impl RowMetadata {
    pub fn from_bytes(buf: &Vec<u8>, offset: usize) -> Self {
        if offset >= buf.len() {
            return Self::new(false);
        }
        let metadata = buf[offset];
        Self {
            is_allocated: metadata & 0b1000_0000u8 != 0,
            is_blob: metadata & 0b0100_0000u8 != 0,
            is_encrypted: metadata & 0b0010_0000u8 != 0,
            is_replicated: metadata & 0b0001_0000u8 != 0,
        }
    }

    pub fn new(is_allocated: bool) -> Self {
        Self {
            is_allocated,
            is_blob: false,
            is_encrypted: false,
            is_replicated: false,
        }
    }

    pub fn decode(metadata: u8) -> Self {
        Self {
            is_allocated: metadata & 0b1000_0000u8 != 0,
            is_blob: metadata & 0b0100_0000u8 != 0,
            is_encrypted: metadata & 0b0010_0000u8 != 0,
            is_replicated: metadata & 0b0001_0000u8 != 0,
        }
    }

    pub fn encode(&self) -> u8 {
        let b2n: fn(bool) -> usize = |b: bool| if b { 1 } else { 0 };
        ((b2n(self.is_allocated) << 7) |
            (b2n(self.is_blob) << 6) |
            (b2n(self.is_encrypted) << 5) |
            (b2n(self.is_replicated) << 4)) as u8
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_decode() {
        let aa = RowMetadata::decode(0b0000_0000);
        assert_eq!(aa.is_allocated, false);
        assert_eq!(aa.is_blob, false);
        assert_eq!(aa.is_encrypted, false);
        assert_eq!(aa.is_replicated, false);

        let bb = RowMetadata::decode(0b1111_0000);
        assert_eq!(bb.is_allocated, true);
        assert_eq!(bb.is_blob, true);
        assert_eq!(bb.is_encrypted, true);
        assert_eq!(bb.is_replicated, true);

        let cc = RowMetadata::decode(0b1010_0000);
        assert_eq!(cc.is_allocated, true);
        assert_eq!(cc.is_blob, false);
        assert_eq!(cc.is_encrypted, true);
        assert_eq!(cc.is_replicated, false);
    }

    #[test]
    fn test_metadata_encode() {
        let aa = RowMetadata {
            is_allocated: true,
            is_blob: true,
            is_encrypted: true,
            is_replicated: true,
        };
        assert_eq!(aa.encode(), 0b1111_0000u8);

        let bb = RowMetadata {
            is_allocated: false,
            is_blob: false,
            is_encrypted: false,
            is_replicated: false,
        };
        assert_eq!(bb.encode(), 0b0000_0000u8);

        let cc = RowMetadata {
            is_allocated: true,
            is_blob: false,
            is_encrypted: true,
            is_replicated: false,
        };
        assert_eq!(cc.encode(), 0b1010_0000u8);
    }
}