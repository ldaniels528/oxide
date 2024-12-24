#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// dataframe configuration module
////////////////////////////////////////////////////////////////////

use std::fs;

use serde::{Deserialize, Serialize};

use crate::cnv_error;
use crate::descriptor::Descriptor;
use crate::namespaces::Namespace;
use crate::parameter::Parameter;

#[derive(Clone, Debug, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct DataFrameConfig {
    columns: Vec<Descriptor>,
    indices: Vec<HashIndexConfig>,
    partitions: Vec<String>,
}

impl DataFrameConfig {
    /// instantiates a new dataframe configuration.
    pub fn build(parameters: &Vec<Parameter>) -> Self {
        DataFrameConfig::new(parameters, Vec::new(), Vec::new())
    }

    /// instantiates a new dataframe configuration.
    pub fn from_descriptors(descriptors: Vec<Descriptor>,
                            indices: Vec<HashIndexConfig>,
                            partitions: Vec<String>) -> Self {
        DataFrameConfig {
            columns: descriptors,
            indices,
            partitions,
        }
    }

    /// instantiates a new dataframe configuration.
    pub fn new(parameters: &Vec<Parameter>,
               indices: Vec<HashIndexConfig>,
               partitions: Vec<String>) -> Self {
        Self::from_descriptors(
            Descriptor::from_parameters(&parameters),
            indices,
            partitions,
        )
    }

    /// deletes a dataframe configuration from disk.
    pub fn delete(ns: &Namespace) -> std::io::Result<()> {
        fs::remove_file(ns.get_config_file_path())
    }

    pub fn get_columns(&self) -> &Vec<Descriptor> { &self.columns }

    pub fn get_indices(&self) -> &Vec<HashIndexConfig> { &self.indices }

    pub fn get_parameters(&self) -> std::io::Result<Vec<Parameter>> {
        Parameter::from_descriptors(&self.columns)
    }

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

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct HashIndexConfig {
    indexed_column_names: Vec<String>,
    is_unique: bool,
}

impl HashIndexConfig {
    /// Creates a new Hash-Index configuration
    pub fn new(indexed_column_names: Vec<String>, is_unique: bool) -> Self {
        HashIndexConfig { indexed_column_names, is_unique }
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::data_types::DataType::{NumberType, StringType};
    use crate::data_types::StorageTypes::FixedSize;
    use crate::dataframe_config::DataFrameConfig;
    use crate::descriptor::Descriptor;
    use crate::namespaces::Namespace;
    use crate::number_kind::NumberKind::F64Kind;
    use crate::numbers::Numbers::F64Value;
    use crate::parameter::Parameter;
    use crate::typed_values::TypedValue::Number;
    use tokio::io;

    #[test]
    fn test_load_and_save_config() -> io::Result<()> {
        let parameters = vec![
            Parameter::new("symbol", StringType(FixedSize(8))),
            Parameter::new("exchange", StringType(FixedSize(8))),
            Parameter::with_default("last_sale", NumberType(F64Kind), Number(F64Value(0.0))),
        ];
        let cfg = DataFrameConfig::build(&parameters);
        let ns = Namespace::parse("securities.other_otc.stocks")?;
        cfg.save(&ns)?;

        // retrieve and verify
        let cfg = DataFrameConfig::load(&ns)?;
        assert_eq!(cfg, DataFrameConfig {
            columns: vec![
                Descriptor::new("symbol", Some("String(8)".into()), None),
                Descriptor::new("exchange", Some("String(8)".into()), None),
                Descriptor::new("last_sale", Some("f64".into()), Some("0.0".into())),
            ],
            indices: Vec::new(),
            partitions: Vec::new(),
        });
        Ok(())
    }
}