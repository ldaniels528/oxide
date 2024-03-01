////////////////////////////////////////////////////////////////////
// I/O cost
////////////////////////////////////////////////////////////////////

use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub struct IOCost {
    pub(crate) altered: u32,
    pub(crate) created: u32,
    pub(crate) destroyed: u32,
    pub(crate) deleted: u32,
    pub(crate) inserted: u32,
    pub(crate) matched: u32,
    pub(crate) scanned: u32,
    pub(crate) shuffled: u32,
    pub(crate) updated: u32,
    pub(crate) bytes_read: u32,
    pub(crate) bytes_written: u32,
}

impl IOCost {}

// Unit tests
#[cfg(test)]
mod tests {}