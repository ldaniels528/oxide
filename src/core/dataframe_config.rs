////////////////////////////////////////////////////////////////////
// dataframe configuration module
////////////////////////////////////////////////////////////////////

use std::fs;

use serde::{Deserialize, Serialize};

use crate::cnv_error;
use crate::namespaces::Namespace;
use crate::server::ColumnJs;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DataFrameConfig {
    columns: Vec<ColumnJs>,
    indices: Vec<HashIndexConfig>,
    partitions: Vec<String>,
}

impl DataFrameConfig {
    /// instantiates a new dataframe configuration.
    pub fn build(columns: Vec<ColumnJs>) -> Self {
        DataFrameConfig { columns, indices: Vec::new(), partitions: Vec::new() }
    }

    /// instantiates a new dataframe configuration.
    pub fn new(columns: Vec<ColumnJs>,
               indices: Vec<HashIndexConfig>,
               partitions: Vec<String>) -> Self {
        DataFrameConfig { columns, indices, partitions }
    }

    /// deletes a dataframe configuration from disk.
    pub fn delete(ns: &Namespace) -> std::io::Result<()> {
        fs::remove_file(ns.get_config_file_path())
    }

    pub fn get_columns(&self) -> &Vec<ColumnJs> { &self.columns }

    pub fn get_indices(&self) -> &Vec<HashIndexConfig> { &self.indices }

    pub fn get_partitions(&self) -> &Vec<String> { &self.partitions }

    /// loads a dataframe configuration from disk.
    pub fn load(ns: &Namespace) -> std::io::Result<Self> {
        let config_string = fs::read_to_string(ns.get_config_file_path())?;
        serde_json::from_str::<Self>(&config_string).map_err(|e| cnv_error!(e))
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
    indexed_column_names: Vec<String>,
    is_unique: bool,
}

impl HashIndexConfig {
    /// Creates a new Hash-Index configuration
    pub fn new(indexed_column_names: Vec<String>, is_unique: bool) -> Self {
        HashIndexConfig { indexed_column_names, is_unique }
    }

    pub fn get_indexed_column_name(&self) -> Vec<String> { self.indexed_column_names.clone() }

    pub fn is_unique(&self) -> bool { self.is_unique }
}

// Unit tests
#[cfg(test)]
mod tests {
    use tokio::io;

    use crate::dataframe_config::DataFrameConfig;
    use crate::namespaces::Namespace;
    use crate::server::ColumnJs;

    #[test]
    fn test_load_and_save_config() -> io::Result<()> {
        let columns: Vec<ColumnJs> = vec![
            ColumnJs::new("symbol", "String(10)", None),
            ColumnJs::new("exchange", "String(10)", None),
            ColumnJs::new("last_sale", "f64", Some("0.00".into())),
        ];
        let indices = Vec::with_capacity(0);
        let partitions = Vec::with_capacity(0);
        let cfg = DataFrameConfig::new(columns, indices, partitions);
        let ns = Namespace::parse("securities.other_otc.stocks")?;
        cfg.save(&ns)?;

        // retrieve and verify
        let cfg = DataFrameConfig::load(&ns)?;
        assert_eq!(cfg, DataFrameConfig {
            columns: vec![
                ColumnJs::new("symbol", "String(10)", None),
                ColumnJs::new("exchange", "String(10)", None),
                ColumnJs::new("last_sale", "f64", Some("0.00".into())),
            ],
            indices: Vec::new(),
            partitions: Vec::new(),
        });
        Ok(())
    }
}