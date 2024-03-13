////////////////////////////////////////////////////////////////////
// namespaces module
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

// Namespace is a logical representation of a Lollypop object namespace or path
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Namespace {
    pub database: String,
    pub schema: String,
    pub name: String,
}

impl Namespace {
    /// constructs a new [Namespace]
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
}

// Unit tests
#[cfg(test)]
mod tests {
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
}