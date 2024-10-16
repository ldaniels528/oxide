////////////////////////////////////////////////////////////////////
// Outcomes class
////////////////////////////////////////////////////////////////////

use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};
use crate::neocodec::Codec;

/// Represents an Outcome type/kind
#[repr(u8)]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum OutcomeKind {
    Acked = 0,
    RowInserted = 1,
    RowsUpdated = 2,
}

/// Represents a non-error Operation Outcome
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Outcomes {
    Ack,
    RowId(usize),
    RowsAffected(usize),
}

impl Outcomes {
    pub fn decode(bytes: &Vec<u8>) -> std::io::Result<Outcomes> {
        Codec::unwrap_as_result(bincode::deserialize(bytes))
    }

    pub fn encode(&self) -> std::io::Result<Vec<u8>> {
        Codec::unwrap_as_result(bincode::serialize(self))
    }

    pub fn to_bool(&self) -> bool {
        match self {
            Outcomes::Ack | Outcomes::RowId(..) => true,
            Outcomes::RowsAffected(n) => *n > 0,
        }
    }

    pub fn to_f64(&self) -> f64 {
        match self {
            Outcomes::Ack => 0.,
            Outcomes::RowId(id) => *id as f64,
            Outcomes::RowsAffected(n) => *n as f64,
        }
    }

    pub fn to_i64(&self) -> i64 {
        match self {
            Outcomes::Ack => 0,
            Outcomes::RowId(id) => *id as i64,
            Outcomes::RowsAffected(n) => *n as i64,
        }
    }

    pub fn to_u64(&self) -> u64 {
        match self {
            Outcomes::Ack => 0,
            Outcomes::RowId(id) => *id as u64,
            Outcomes::RowsAffected(n) => *n as u64,
        }
    }

    pub fn to_update_count(&self) -> usize {
        match self {
            Outcomes::Ack | Outcomes::RowId(..) => 1,
            Outcomes::RowsAffected(n) => *n,
        }
    }

    pub fn to_usize(&self) -> usize {
        match self {
            Outcomes::Ack => 0,
            Outcomes::RowId(n) | Outcomes::RowsAffected(n) => *n,
        }
    }
}

impl Display for Outcomes {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self {
            Outcomes::Ack => write!(f, "Ack"),
            Outcomes::RowId(id) => write!(f, "RowId({})", id),
            Outcomes::RowsAffected(n) => write!(f, "RowsAffected({})", n),
        }
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::outcomes::Outcomes;

    #[test]
    fn test_encode() {
        fn enc_dec(expected: Outcomes) {
            let bytes = expected.encode().unwrap();
            let actual = Outcomes::decode(&bytes).unwrap();
            println!("Outcomes::{expected}.encode() => {:?} ({})", bytes, bytes.len());
            assert_eq!(actual, expected)
        }

        enc_dec(Outcomes::Ack);
        enc_dec(Outcomes::RowId(5));
        enc_dec(Outcomes::RowsAffected(7));
    }

    #[test]
    fn test_to_bool() {
        assert_eq!(Outcomes::Ack.to_bool(), true);
        assert_eq!(Outcomes::RowId(5).to_bool(), true);
        assert_eq!(Outcomes::RowsAffected(7).to_bool(), true);
        assert_eq!(Outcomes::RowsAffected(0).to_bool(), false);
    }

    #[test]
    fn test_to_f64() {
        assert_eq!(Outcomes::Ack.to_f64(), 0.);
        assert_eq!(Outcomes::RowId(5).to_f64(), 5.);
        assert_eq!(Outcomes::RowsAffected(7).to_f64(), 7.);
    }

    #[test]
    fn test_to_i64() {
        assert_eq!(Outcomes::Ack.to_i64(), 0);
        assert_eq!(Outcomes::RowId(5).to_i64(), 5);
        assert_eq!(Outcomes::RowsAffected(7).to_i64(), 7);
    }

    #[test]
    fn test_to_u64() {
        assert_eq!(Outcomes::Ack.to_u64(), 0);
        assert_eq!(Outcomes::RowId(5).to_u64(), 5);
        assert_eq!(Outcomes::RowsAffected(7).to_u64(), 7);
    }

    #[test]
    fn test_to_string() {
        assert_eq!(Outcomes::Ack.to_string(), "Ack".to_string());
        assert_eq!(Outcomes::RowId(5).to_string(), "RowId(5)".to_string());
        assert_eq!(Outcomes::RowsAffected(7).to_string(), "RowsAffected(7)".to_string());
    }
}