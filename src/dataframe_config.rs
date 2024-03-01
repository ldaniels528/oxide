////////////////////////////////////////////////////////////////////
// dataframe configuration
////////////////////////////////////////////////////////////////////

use serde::Serialize;

use crate::columns::Column;

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct DataFrameConfig {
    pub columns: Vec<Column>,
    pub indices: Vec<HashIndexConfig>,
    pub partitions: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct HashIndexConfig {
    pub indexed_column_name: String,
    pub is_unique: bool,
}

impl DataFrameConfig {
    pub fn new(columns: Vec<Column>,
               indices: Vec<HashIndexConfig>,
               partitions: Vec<String>) -> Self {
        Self { columns, indices, partitions }
    }
}

// Unit tests
#[cfg(test)]
mod tests {}