////////////////////////////////////////////////////////////////////
// I/O cost
////////////////////////////////////////////////////////////////////

use serde::Serialize;

#[derive(Clone, Copy, Debug, Serialize)]
pub struct IOCost {
    pub(crate) altered: usize,
    pub(crate) created: usize,
    pub(crate) destroyed: usize,
    pub(crate) deleted: usize,
    pub(crate) inserted: usize,
    pub(crate) matched: usize,
    pub(crate) scanned: usize,
    pub(crate) shuffled: usize,
    pub(crate) updated: usize,
    pub(crate) bytes_read: usize,
    pub(crate) bytes_written: usize,
}

impl IOCost {
    pub fn read(scanned: usize, bytes_read: usize) -> IOCost {
        IOCost {
            altered: 0,
            created: 0,
            destroyed: 0,
            deleted: 0,
            inserted: 0,
            matched: 0,
            scanned,
            shuffled: 0,
            updated: 0,
            bytes_read,
            bytes_written : 0,
        }
    }

    pub fn insert(inserted: usize, bytes_read: usize, bytes_written: usize) -> IOCost {
        IOCost {
            altered: 0,
            created: 0,
            destroyed: 0,
            deleted: 0,
            inserted,
            matched: 0,
            scanned: 0,
            shuffled: 0,
            updated: 0,
            bytes_read,
            bytes_written,
        }
    }

    pub fn update(updated: usize, bytes_read: usize, bytes_written: usize) -> IOCost {
        IOCost {
            altered: 0,
            created: 0,
            destroyed: 0,
            deleted: 0,
            inserted: 0,
            matched: 0,
            scanned: 0,
            shuffled: 0,
            updated,
            bytes_read,
            bytes_written,
        }
    }
}

// Unit tests
#[cfg(test)]
mod tests {}