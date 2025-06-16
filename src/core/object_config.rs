#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Object Configuration enumerations
////////////////////////////////////////////////////////////////////

use std::fs;

use serde::{Deserialize, Serialize};

use crate::cnv_error;
use crate::expression::Expression;
use crate::namespaces::Namespace;
use crate::parameter::Parameter;

/// Oxide Object Configuration
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum ObjectConfig {
    EventSourceConfig {
        columns: Vec<Parameter>,
        indices: Vec<HashIndexConfig>,
        partitions: Vec<String>,
    },
    TableConfig {
        columns: Vec<Parameter>,
        indices: Vec<HashIndexConfig>,
        partitions: Vec<String>,
    },
    TableFnConfig {
        columns: Vec<Parameter>,
        code: Expression,
        indices: Vec<HashIndexConfig>,
        partitions: Vec<String>,
    },
}

impl ObjectConfig {
    /// instantiates a new table configuration.
    pub fn build_table(columns: Vec<Parameter>) -> Self {
        ObjectConfig::TableConfig {
            columns,
            indices: vec![],
            partitions: vec![],
        }
    }

    /// instantiates a new table function configuration.
    pub fn build_table_fn(
        columns: Vec<Parameter>,
        code: Expression,
    ) -> Self {
        ObjectConfig::TableFnConfig {
            columns,
            code,
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
            ObjectConfig::EventSourceConfig { columns, .. } |
            ObjectConfig::TableConfig { columns, .. } |
            ObjectConfig::TableFnConfig { columns, .. } => columns.clone(),
        }
    }

    pub fn get_indices(&self) -> Vec<HashIndexConfig> {
        match self {
            ObjectConfig::EventSourceConfig { indices, .. } |
            ObjectConfig::TableConfig { indices, .. } |
            ObjectConfig::TableFnConfig { indices, .. } => indices.clone(),
        }
    }

    pub fn get_partitions(&self) -> Option<&Vec<String>> {
        match self {
            ObjectConfig::EventSourceConfig { partitions, .. } |
            ObjectConfig::TableConfig { partitions, .. } |
            ObjectConfig::TableFnConfig { partitions, .. } => Some(partitions),
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
            ObjectConfig::EventSourceConfig { columns, partitions, .. } =>
                ObjectConfig::EventSourceConfig {
                    columns,
                    indices,
                    partitions,
                },
            ObjectConfig::TableConfig { columns, partitions, .. } =>
                ObjectConfig::TableConfig {
                    columns,
                    indices,
                    partitions,
                },
            ObjectConfig::TableFnConfig { columns, code, partitions, .. } =>
                ObjectConfig::TableFnConfig {
                    columns,
                    code,
                    indices,
                    partitions,
                }
        }
    }

    pub fn with_partitions(self, partitions: Vec<String>) -> Self {
        match self {
            ObjectConfig::EventSourceConfig { columns, indices, .. } =>
                ObjectConfig::EventSourceConfig {
                    columns,
                    indices,
                    partitions,
                },
            ObjectConfig::TableConfig { columns, indices, .. } =>
                ObjectConfig::TableConfig {
                    columns,
                    indices,
                    partitions,
                },
            ObjectConfig::TableFnConfig { columns, code, indices, .. } =>
                ObjectConfig::TableFnConfig {
                    columns,
                    code,
                    indices,
                    partitions,
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
    use crate::data_types::DataType::{FixedSizeType, NumberType, StringType};
    use crate::expression::Expression::{ColonColon, FunctionCall, Identifier, StructureExpression};
    use crate::namespaces::Namespace;
    use crate::number_kind::NumberKind::F64Kind;
    use crate::numbers::Numbers::F64Value;
    use crate::parameter::Parameter;
    use crate::typed_values::TypedValue::Number;
    use tokio::io;

    #[test]
    fn test_table_config_load_and_save() -> io::Result<()> {
        let cfg = ObjectConfig::build_table(vec![
            Parameter::new("symbol", FixedSizeType(StringType.into(), 8)),
            Parameter::new("exchange", FixedSizeType(StringType.into(), 8)),
            Parameter::new_with_default("last_sale", NumberType(F64Kind), Number(F64Value(0.0))),
        ]);
        let ns = Namespace::parse("object_config.table_cfg.stocks")?;
        cfg.save(&ns)?;

        // retrieve and verify
        let cfg = ObjectConfig::load(&ns)?;
        assert_eq!(cfg, ObjectConfig::TableConfig {
            columns: vec![
                Parameter::new("symbol", FixedSizeType(StringType.into(), 8)),
                Parameter::new("exchange", FixedSizeType(StringType.into(), 8)),
                Parameter::new_with_default("last_sale", NumberType(F64Kind), Number(F64Value(0.0))),
            ],
            indices: Vec::new(),
            partitions: Vec::new(),
        });
        Ok(())
    }

    #[test]
    fn test_table_fn_config_load_and_save() -> io::Result<()> {
        let cfg = ObjectConfig::build_table_fn(
            vec![
                Parameter::new("symbol", FixedSizeType(StringType.into(), 8)),
                Parameter::new("exchange", FixedSizeType(StringType.into(), 8)),
                Parameter::new_with_default("last_sale", NumberType(F64Kind), Number(F64Value(0.0))),
            ],
            StructureExpression(vec![
                ("symbol".to_string(), Identifier("symbol".into())),
                ("market".to_string(), Identifier("exchange".into())),
                ("last_sale".to_string(), Identifier("last_sale".into())),
                ("process_time".to_string(), ColonColon(
                    Box::new(Identifier("cal".into())),
                    Box::new(FunctionCall {
                        fx: Box::new(Identifier("now".into())),
                        args: vec![]
                    })))
            ]),
        );
        let ns = Namespace::parse("object_config.table_fn.stocks")?;
        cfg.save(&ns)?;

        // retrieve and verify
        let cfg = ObjectConfig::load(&ns)?;
        assert_eq!(cfg, ObjectConfig::TableFnConfig {
            columns: vec![
                Parameter::new("symbol", FixedSizeType(StringType.into(), 8)),
                Parameter::new("exchange", FixedSizeType(StringType.into(), 8)),
                Parameter::new_with_default("last_sale", NumberType(F64Kind), Number(F64Value(0.0))),
            ],
            code: StructureExpression(vec![
                ("symbol".to_string(), Identifier("symbol".into())),
                ("market".to_string(), Identifier("exchange".into())),
                ("last_sale".to_string(), Identifier("last_sale".into())),
                ("process_time".to_string(), ColonColon(
                    Box::new(Identifier("cal".into())),
                    Box::new(FunctionCall {
                        fx: Box::new(Identifier("now".into())),
                        args: vec![]
                    })))
            ]),
            indices: Vec::new(),
            partitions: Vec::new(),
        });
        Ok(())
    }
}