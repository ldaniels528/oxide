#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// ObjectConfig enumeration
////////////////////////////////////////////////////////////////////

use std::fs;

use serde::{Deserialize, Serialize};

use crate::cnv_error;
use crate::namespaces::Namespace;
use crate::object_config::ObjectConfig::TableConfig;
use crate::parameter::Parameter;

/// Oxide Object Configuration
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum ObjectConfig {
    TableConfig {
        columns: Vec<Parameter>,
        indices: Vec<HashIndexConfig>,
        partitions: Vec<String>,
    },
}

impl ObjectConfig {
    /// instantiates a new dataframe configuration.
    pub fn build_table(columns: Vec<Parameter>) -> Self {
        ObjectConfig::TableConfig {
            columns,
            indices: vec![],
            partitions: vec![],
        }
    }

    /// Deletes a dataframe configuration from disk.
    pub fn delete(ns: &Namespace) -> std::io::Result<()> {
        fs::remove_file(ns.get_config_file_path())
    }

    pub fn get_columns(&self) -> Vec<Parameter> {
        match self {
            ObjectConfig::TableConfig { columns, .. } => columns.clone(),
            _ => vec![]
        }
    }

    pub fn get_indices(&self) -> Vec<HashIndexConfig> {
        match self {
            ObjectConfig::TableConfig { indices, .. } => indices.clone(),
            _ => vec![]
        }
    }

    pub fn get_partitions(&self) -> Option<&Vec<String>> {
        match self {
            ObjectConfig::TableConfig { partitions, .. } => Some(partitions),
            _ => None
        }
    }

    /// Loads a dataframe configuration from disk.
    pub fn load(ns: &Namespace) -> std::io::Result<Self> {
        let config_string = fs::read_to_string(ns.get_config_file_path())?;
        serde_json::from_str::<Self>(&config_string).map_err(|e| cnv_error!(e))
    }

    /// Saves an Oxide object configuration to disk.
    pub fn save(&self, ns: &Namespace) -> std::io::Result<()> {
        let json_string = serde_json::to_string(&self)?;
        fs::create_dir_all(&ns.get_root_path())?;
        fs::write(&ns.get_config_file_path(), json_string)
    }

    pub fn with_indices(self, indices: Vec<HashIndexConfig>) -> Self {
        match self {
            ObjectConfig::TableConfig { columns, partitions, .. } => {
                TableConfig {
                    columns,
                    indices,
                    partitions,
                }
            }
        }
    }

    pub fn with_partitions(self, partitions: Vec<String>) -> Self {
        match self {
            ObjectConfig::TableConfig { columns, indices, .. } => {
                TableConfig {
                    columns,
                    indices,
                    partitions,
                }
            }
        }
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

/// Unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_types::DataType::{NumberType, StringType};
    use crate::namespaces::Namespace;
    use crate::number_kind::NumberKind::F64Kind;
    use crate::numbers::Numbers::F64Value;
    use crate::parameter::Parameter;
    use crate::typed_values::TypedValue::Number;
    use tokio::io;

    #[test]
    fn test_config_load_and_save() -> io::Result<()> {
        let cfg = ObjectConfig::build_table(vec![
            Parameter::new("symbol", StringType(8)),
            Parameter::new("exchange", StringType(8)),
            Parameter::with_default("last_sale", NumberType(F64Kind), Number(F64Value(0.0))),
        ]);
        let ns = Namespace::parse("securities.other_otc.stocks")?;
        cfg.save(&ns)?;

        // retrieve and verify
        let cfg = ObjectConfig::load(&ns)?;
        assert_eq!(cfg, TableConfig {
            columns: vec![
                Parameter::new("symbol", StringType(8)),
                Parameter::new("exchange", StringType(8)),
                Parameter::with_default("last_sale", NumberType(F64Kind), Number(F64Value(0.0))),
            ],
            indices: Vec::new(),
            partitions: Vec::new(),
        });
        Ok(())
    }
}