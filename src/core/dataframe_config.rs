////////////////////////////////////////////////////////////////////
// dataframe configuration module
////////////////////////////////////////////////////////////////////

use std::fs;

use serde::{Deserialize, Serialize};

use crate::columns::Column;
use crate::namespaces::Namespace;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DataFrameConfig {
    pub(crate) columns: Vec<Column>,
    pub(crate) indices: Vec<HashIndexConfig>,
    pub(crate) partitions: Vec<String>,
}

impl DataFrameConfig {
    /// instantiates a new dataframe configuration.
    pub fn new(columns: Vec<Column>,
               indices: Vec<HashIndexConfig>,
               partitions: Vec<String>) -> Self {
        Self { columns, indices, partitions }
    }

    /// loads a dataframe configuration from disk.
    pub fn load(ns: &Namespace) -> std::io::Result<DataFrameConfig> {
        let cfg_path = ns.get_config_file_path();
        let config_string = fs::read_to_string(cfg_path)?;
        let parsed: serde_json::Result<DataFrameConfig> = serde_json::from_str(&config_string);
        match parsed {
            Ok(cfg) => Ok(cfg),
            Err(err) => Err(err.into())
        }
    }

    /// saves a dataframe configuration to disk.
    pub fn save(&self, ns: &Namespace) -> std::io::Result<()> {
        let json_string = serde_json::to_string(&self)?;
        fs::create_dir_all(&ns.get_root_path())?;
        fs::write(&ns.get_config_file_path(), json_string)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HashIndexConfig {
    pub(crate) indexed_column_name: String,
    pub(crate) is_unique: bool,
}

// Unit tests
#[cfg(test)]
mod tests {
    use tokio::io;

    use crate::columns::Column;
    use crate::dataframe_config::{DataFrameConfig, HashIndexConfig};
    use crate::namespaces::Namespace;

    #[test]
    fn test_load_and_save_config() -> io::Result<()> {
        let columns: Vec<Column> = vec![
            Column::new("symbol", "String(10)", ""),
            Column::new("exchange", "String(10)", ""),
            Column::new("lastSale", "Double", "0.00"),
        ];
        let indices: Vec<HashIndexConfig> = Vec::with_capacity(0);
        let partitions: Vec<String> = Vec::with_capacity(0);
        let cfg = DataFrameConfig::new(columns, indices, partitions);
        let ns = Namespace::new("securities", "other_otc", "Stocks");
        cfg.save(&ns)?;

        // retrieve and verify
        let cfg = DataFrameConfig::load(&ns)?;
        assert_eq!(cfg, DataFrameConfig {
            columns: vec![
                Column {
                    name: "symbol".into(),
                    column_type: "String(10)".into(),
                    default_value: "".into(),
                },
                Column {
                    name: "exchange".into(),
                    column_type: "String(10)".into(),
                    default_value: "".into(),
                },
                Column {
                    name: "lastSale".into(),
                    column_type: "Double".into(),
                    default_value: "0.00".into(),
                },
            ],
            indices: vec![],
            partitions: vec![],
        });
        Ok(())
    }
}