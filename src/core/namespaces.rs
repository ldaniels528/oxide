////////////////////////////////////////////////////////////////////
// namespaces module
////////////////////////////////////////////////////////////////////

use std::env;
use std::fmt::Display;

use serde::{Deserialize, Serialize};

use shared_lib::fail;

// Namespace is a logical representation of a Lollypop object namespace or path
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
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

    pub fn parse(text: &str) -> std::io::Result<Namespace> {
        let result: Vec<&str> = text.split(".").collect();
        match result.as_slice() {
            [a, b, c] => Ok(Namespace::new(a, b, c)),
            _ => fail(format!("Failed to parse namespace '{}'", text))
        }
    }

    pub(crate) fn oxide_home() -> String {
        env::var("OXIDE_HOME").unwrap_or("./oxide_db".to_string())
    }

    pub fn id(&self) -> String {
        format!("{}.{}.{}", self.database, self.schema, self.name)
    }

    pub fn get_blob_file_path(&self) -> String {
        self.get_file_path("blob")
    }

    pub fn get_config_file_path(&self) -> String {
        self.get_file_path("json")
    }

    pub fn get_file_path(&self, extension: &str) -> String {
        // ex:  "$OXIDE_HOME/ns/database/schema/name/name.extension"
        let mut builder = String::new();
        builder.push_str(&*self.get_root_path());
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
        // ex:  "$OXIDE_HOME/ns/database/schema/name/"
        let mut builder = String::new();
        builder.push_str(&*Self::oxide_home());
        builder.push_str("/ns/");
        builder.push_str(&self.database);
        builder.push('/');
        builder.push_str(&*self.schema);
        builder.push('/');
        builder.push_str(&*self.name);
        builder.push('/');
        builder
    }

    pub fn get_hash_index_file_path(&self, column_index: usize) -> String {
        self.get_file_path(format!("{}", column_index).as_str())
    }

    pub fn get_table_file_path(&self) -> String {
        self.get_file_path("table")
    }
}

impl Display for Namespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.get_full_name())
    }
}

impl Into<String> for Namespace {
    fn into(self) -> String {
        self.get_full_name()
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_into_string() {
        let ns = Namespace::parse("securities.nyse.stocks").unwrap();
        assert_eq!(Into::<String>::into(ns), "securities.nyse.stocks");
    }

    #[test]
    fn test_to_string() {
        let ns = Namespace::parse("securities.nasdaq.stocks").unwrap();
        assert_eq!(ns.to_string(), "securities.nasdaq.stocks");
    }

    #[test]
    fn test_parse() {
        let ns = Namespace::parse("securities.nyse.stocks").unwrap();
        assert_eq!(ns, Namespace::new("securities", "nyse", "stocks"));
    }

    #[test]
    fn test_get_blob_file_path() {
        let oxide_home = Namespace::oxide_home();
        let ns = Namespace::parse("securities.amex.stocks").unwrap();
        let filename = ns.get_blob_file_path();
        assert_eq!(filename, format!("{}/ns/securities/amex/stocks/stocks.blob", oxide_home))
    }

    #[test]
    fn test_get_config_file_path() {
        let oxide_home = Namespace::oxide_home();
        let ns = Namespace::parse("securities.otc.stocks").unwrap();
        let filename = ns.get_config_file_path();
        assert_eq!(filename, format!("{}/ns/securities/otc/stocks/stocks.json", oxide_home))
    }

    #[test]
    fn test_get_full_name() {
        let ns = Namespace::parse("securities.otc.stocks").unwrap();
        assert_eq!(ns.get_full_name(), "securities.otc.stocks")
    }

    #[test]
    fn test_get_root_path() {
        let oxide_home = Namespace::oxide_home();
        let ns = Namespace::parse("securities.nasdaq.stocks").unwrap();
        assert_eq!(ns.get_root_path(), format!("{}/ns/securities/nasdaq/stocks/", oxide_home))
    }

    #[test]
    fn test_get_table_file_path() {
        let oxide_home = Namespace::oxide_home();
        let ns = Namespace::parse("securities.nyse.stocks").unwrap();
        let filename = ns.get_table_file_path();
        assert_eq!(filename, format!("{}/ns/securities/nyse/stocks/stocks.table", oxide_home))
    }
}