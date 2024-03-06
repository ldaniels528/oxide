////////////////////////////////////////////////////////////////////
// namespaces module
////////////////////////////////////////////////////////////////////

use std::fs;

use serde::Serialize;

use crate::dataframe_config::DataFrameConfig;

// Namespace is a logical representation of a Lollypop object namespace or path
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Namespace {
    pub database: String,
    pub schema: String,
    pub name: String,
}

impl Namespace {
    pub fn new(database: &str, schema: &str, name: &str) -> Self {
        Self {
            database: database.to_string(),
            schema: schema.to_string(),
            name: name.to_string(),
        }
    }

    pub fn get_blob_file_path(&self) -> String {
        self.get_file_path("blob")
    }

    pub fn get_config_file_path(&self) -> String {
        self.get_file_path("json")
    }

    pub fn get_file_path(&self, extension: &str) -> String {
        // ex: "lollypop_db/ns/database/schema/name/name.extension"
        let mut builder = String::new();
        builder.push_str(&*self.get_root_path());
        builder.push('/');
        builder.push_str(&*self.name);
        builder.push('.');
        builder.push_str(&extension);
        builder
    }

    pub fn get_full_name(&self) -> String {
        // ex: "database.schema.name"
        let mut builder = String::new();
        builder.push_str(&*self.database);
        builder.push('.');
        builder.push_str(&*self.schema);
        builder.push('.');
        builder.push_str(&*self.name);
        builder
    }

    pub fn get_root_path(&self) -> String {
        // ex: "lollypop_db/ns/database/schema/name/"
        let mut builder = String::new();
        builder.push_str("lollypop_db/ns/");
        builder.push_str(&self.database);
        builder.push('/');
        builder.push_str(&*self.schema);
        builder.push('/');
        builder.push_str(&*self.name);
        builder
    }

    pub fn get_table_file_path(&self) -> String {
        self.get_file_path("table")
    }

    pub fn write_config(&self, config: &DataFrameConfig) -> std::io::Result<()> {
        // convert the config to a JSON string
        let json_string = serde_json::to_string(&config)?;
        // ensure the parent directory exists
        fs::create_dir_all(&self.get_root_path())?;
        // write the JSON config to disk
        fs::write(&self.get_config_file_path(), json_string)
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::columns::Column;
    use crate::dataframe_config::{DataFrameConfig, HashIndexConfig};

    use super::*;

    #[test]
    fn test_get_blob_file_path() {
        let ns = Namespace::new("securities", "amex", "Stocks");
        let filename = ns.get_blob_file_path();
        assert_eq!(filename, "lollypop_db/ns/securities/amex/Stocks/Stocks.blob")
    }

    #[test]
    fn test_get_config_file_path() {
        let ns = Namespace::new("securities", "otc", "Stocks");
        let filename = ns.get_config_file_path();
        assert_eq!(filename, "lollypop_db/ns/securities/otc/Stocks/Stocks.json")
    }

    #[test]
    fn test_get_full_name() {
        let ns = Namespace::new("securities", "otc", "Stocks");
        assert_eq!(ns.get_full_name(), "securities.otc.Stocks")
    }

    #[test]
    fn test_get_root_path() {
        let ns = Namespace::new("securities", "nasdaq", "Stocks");
        assert_eq!(ns.get_root_path(), "lollypop_db/ns/securities/nasdaq/Stocks")
    }

    #[test]
    fn test_get_table_file_path() {
        let ns = Namespace::new("securities", "nyse", "Stocks");
        let filename = ns.get_table_file_path();
        assert_eq!(filename, "lollypop_db/ns/securities/nyse/Stocks/Stocks.table")
    }

    #[test]
    fn test_write_config() {
        let columns: Vec<Column> = vec![
            Column::new("symbol", "String(10)", ""),
            Column::new("exchange", "String(10)", ""),
            Column::new("lastSale", "Double", ""),
        ];
        let indices: Vec<HashIndexConfig> = Vec::with_capacity(0);
        let partitions: Vec<String> = Vec::with_capacity(0);
        let df: DataFrameConfig = DataFrameConfig::new(columns, indices, partitions);
        let ns: Namespace = Namespace::new("securities", "other_otc", "Stocks");

        ns.write_config(&df)
            .expect("failed to write config");
        assert_eq!(&df.columns[0].name, "symbol");
        assert_eq!(&df.columns[0].column_type, "String(10)");
        assert_eq!(&df.columns[0].default_value, "");
        assert_eq!(&df.columns[1].name, "exchange");
        assert_eq!(&df.columns[1].column_type, "String(10)");
        assert_eq!(&df.columns[1].default_value, "");
        assert_eq!(&df.columns[2].name, "lastSale");
        assert_eq!(&df.columns[2].column_type, "Double");
        assert_eq!(&df.columns[2].default_value, "");
    }
}