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
mod tests {
    use crate::dataframe_config::DataFrameConfig;
    use crate::testdata::create_test_dataframe_config;

    #[test]
    fn test_create_dataframe() {
        let cfg: DataFrameConfig = create_test_dataframe_config();
        assert_eq!(cfg.columns[0].name, "symbol");
        assert_eq!(cfg.columns[0].column_type, "String(4)");
        assert_eq!(cfg.columns[0].default_value, "");
        assert_eq!(cfg.columns[1].name, "exchange");
        assert_eq!(cfg.columns[1].column_type, "String(4)");
        assert_eq!(cfg.columns[1].default_value, "");
        assert_eq!(cfg.columns[2].name, "lastSale");
        assert_eq!(cfg.columns[2].column_type, "Double");
        assert_eq!(cfg.columns[2].default_value, "");
    }
}