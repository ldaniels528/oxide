#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// DataFormats class
////////////////////////////////////////////////////////////////////

use crate::row_collection::RowCollection;

/// Represents a Data Format
pub enum DataFormats {
    CSV,
    JSON,
}

impl DataFormats {
    pub fn get_name(&self) -> String {
        (match self {
            DataFormats::CSV => "CSV",
            DataFormats::JSON => "JSON",
        }).to_string()
    }
}

/// Unit tests
#[cfg(test)]
mod tests {}