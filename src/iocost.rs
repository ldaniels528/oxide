////////////////////////////////////////////////////////////////////
// I/O cost
////////////////////////////////////////////////////////////////////

use std::ops::Add;
use std::ops::AddAssign;

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

impl Add for IOCost {
    type Output = IOCost;

    fn add(self, other: IOCost) -> IOCost {
        IOCost {
            altered: self.altered + other.altered,
            created: self.created + other.created,
            destroyed: self.destroyed + other.destroyed,
            deleted: self.deleted + other.deleted,
            inserted: self.inserted + other.inserted,
            matched: self.matched + other.matched,
            scanned: self.scanned + other.scanned,
            shuffled: self.shuffled + other.shuffled,
            updated: self.updated + other.updated,
            bytes_read: self.bytes_read + other.bytes_read,
            bytes_written: self.bytes_written + other.bytes_written,
        }
    }
}

impl AddAssign for IOCost {
    fn add_assign(&mut self, other: IOCost) {
        self.altered += other.altered;
        self.created += other.created;
        self.destroyed += other.destroyed;
        self.deleted += other.deleted;
        self.inserted += other.inserted;
        self.matched += other.matched;
        self.scanned += other.scanned;
        self.shuffled += other.shuffled;
        self.updated += other.updated;
        self.bytes_read += other.bytes_read;
        self.bytes_written += other.bytes_written;
    }
}

impl IOCost {
    pub fn append(inserted: usize, bytes_written: usize) -> IOCost {
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
            bytes_read: 0,
            bytes_written,
        }
    }

    pub fn new() -> IOCost {
        IOCost {
            altered: 0,
            created: 0,
            destroyed: 0,
            deleted: 0,
            inserted: 0,
            matched: 0,
            scanned: 0,
            shuffled: 0,
            updated: 0,
            bytes_read: 0,
            bytes_written: 0,
        }
    }

    pub fn overwrite(updated: usize, bytes_written: usize) -> IOCost {
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
            bytes_read: 0,
            bytes_written,
        }
    }

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
            bytes_written: 0,
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