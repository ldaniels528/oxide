#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// namespaces module
////////////////////////////////////////////////////////////////////

use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::dataframe::Dataframe;
use crate::dataframe::Dataframe::EventSource;
use crate::file_row_collection::FileRowCollection;
use crate::journaling::{EventSourceRowCollection, TableFunction};
use crate::machine::Machine;
use crate::object_config::ObjectConfig;
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
    pub fn new<S: AsRef<str>>(database: S, schema: S, name: S) -> Self {
        Self {
            database: database.as_ref().to_string(),
            schema: schema.as_ref().to_string(),
            name: name.as_ref().to_string(),
        }
    }

    pub fn parse(text: &str) -> std::io::Result<Namespace> {
        let result: Vec<&str> = text.split(".").collect();
        match result.as_slice() {
            [d, s, n] => Ok(Namespace::new(d, s, n)),
            _ => fail(format!("Failed to parse namespace '{}'", text))
        }
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
        builder.push_str(&*Machine::oxide_home());
        builder.push_str("/ns/");
        builder.push_str(&self.database);
        builder.push('/');
        builder.push_str(&*self.schema);
        builder.push('/');
        builder.push_str(&*self.name);
        builder.push('/');
        builder
    }

    pub fn get_table_file_path(&self) -> String {
        self.get_file_path("table")
    }

    pub fn get_table_fn_journal_file_path(&self) -> String {
        self.get_file_path("journal")
    }

    pub fn get_table_fn_state_file_path(&self) -> String {
        self.get_file_path("state")
    }

    pub fn id(&self) -> String {
        format!("{}.{}.{}", self.database, self.schema, self.name)
    }

    pub fn load_table(&self) -> std::io::Result<Dataframe> {
        match ObjectConfig::load(self)? {
            ObjectConfig::EventSourceConfig { columns, .. } =>
                EventSourceRowCollection::new(self, &columns).map(|es| EventSource(es)),
            ObjectConfig::TableConfig { .. } =>
                FileRowCollection::open(self).map(|frc| Dataframe::Disk(frc)),
            ObjectConfig::TableFnConfig { .. } =>
                TableFunction::from_namespace(self, Machine::new_platform())
                    .map(|tf| Dataframe::TableFn(Box::new(tf)))
        }
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
    use crate::machine::Machine;

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
        let oxide_home = Machine::oxide_home();
        let ns = Namespace::parse("securities.amex.stocks").unwrap();
        let filename = ns.get_blob_file_path();
        assert_eq!(filename, format!("{}/ns/securities/amex/stocks/stocks.blob", oxide_home))
    }

    #[test]
    fn test_get_config_file_path() {
        let oxide_home = Machine::oxide_home();
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
        let oxide_home = Machine::oxide_home();
        let ns = Namespace::parse("securities.nasdaq.stocks").unwrap();
        assert_eq!(ns.get_root_path(), format!("{}/ns/securities/nasdaq/stocks/", oxide_home))
    }

    #[test]
    fn test_get_table_file_path() {
        let oxide_home = Machine::oxide_home();
        let ns = Namespace::parse("securities.nyse.stocks").unwrap();
        let filename = ns.get_table_file_path();
        assert_eq!(filename, format!("{}/ns/securities/nyse/stocks/stocks.table", oxide_home))
    }
}