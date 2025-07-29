#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
//      Oxide REST Server
////////////////////////////////////////////////////////////////////

extern crate core;

use crate::errors::throw;
use crate::errors::Errors::Exact;
use crate::packages::webservers;
use crate::server_engine::start_http_server_async;
use crate::terminal::TerminalState;
use crate::utils::{is_u16, parse_u16};
use log::LevelFilter;
use serde::{Deserialize, Serialize};
use shared_lib::cnv_error;
use std::env;
use std::string::ToString;

mod bit_array;
mod blob_file_row_collection;
mod blobs;
mod builtins;
mod byte_code_compiler;
mod byte_row_collection;
mod columns;
mod compiler;
mod dataframe;
mod dataframe_actor;
mod data_types;
mod errors;
mod expression;
mod field;
mod file_row_collection;
mod hybrid_row_collection;
mod interpreter;
mod journaling;
mod machine;
mod model_row_collection;
mod namespaces;
mod number_kind;
mod numbers;
mod object_config;
mod packages;
mod parameter;
mod query_engine;
mod readme;
mod row_collection;
mod row_metadata;
mod sequences;
mod server_engine;
mod sprintf;
mod structures;
mod table_renderer;
mod template;
mod terminal;
mod test_util;
mod test_engine;
mod token_slice;
mod tokenizer;
mod tokens;
mod typed_values;
mod utils;
mod web_engine;

const LOCAL_HOST: &str = "0.0.0.0";

/// Represents an enumeration of Application Modes
#[derive(Debug, Eq, PartialEq)]
enum ApplicationModes {
    EmbeddedSession(u16),
    OfflineSession(Vec<String>),
    RemoteSession(String, u16),
}

impl ApplicationModes {
    /// Parses the commandline arguments
    pub fn parse(args: Vec<String>) -> std::io::Result<ApplicationModes> {
        match args.as_slice() {
            [port] if is_u16(port) =>
                Ok(ApplicationModes::RemoteSession(LOCAL_HOST.into(), parse_u16(port)?)),
            [action, port] if is_u16(port) && action == "--embedded" =>
                Ok(ApplicationModes::EmbeddedSession(parse_u16(port)?)),
            [host, port] if is_u16(port) =>
                Ok(ApplicationModes::RemoteSession(host.into(), parse_u16(port)?)),
            args =>
                Ok(ApplicationModes::OfflineSession(args.to_vec())),
        }
    }
}

/// Starts the Oxide server
#[actix::main] 
async fn main() -> std::io::Result<()> {
    // set up the logger
    env_logger::builder()
        .filter_level(LevelFilter::Info)
        .init();

    // start the REPL based on the commandline arguments
    let args = env::args().skip(1).collect();
    start_terminal(ApplicationModes::parse(args)?).await
}

// Start the Oxide terminal (embedded or remote server)
async fn start_terminal(mode: ApplicationModes) -> std::io::Result<()> {
    let (state, args) = match mode {
        ApplicationModes::EmbeddedSession(port) => {
            webservers::start_server(port).await?;
            (TerminalState::connect(LOCAL_HOST, port, "/ws").await?, vec![])
        }
        ApplicationModes::RemoteSession(host, port) =>
            (TerminalState::connect(host.as_str(), port, "/ws").await?, vec![]),
        ApplicationModes::OfflineSession(args) =>
            (TerminalState::offline()?, args),
    };
    terminal::do_terminal(state, args).await
}

/// Unit tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_embedded_session() {
        let mode = ApplicationModes::parse(vec![
            "--embedded".into(), "8754".into()
        ]).unwrap();
        assert_eq!(mode, ApplicationModes::EmbeddedSession(8754));
    }

    #[test]
    fn test_local_session() {
        let mode = ApplicationModes::parse(vec![
            "8888".into()
        ]).unwrap();
        assert_eq!(mode, ApplicationModes::RemoteSession(LOCAL_HOST.into(), 8888));
    }

    #[test]
    fn test_offline_session() {
        let mode = ApplicationModes::parse(vec![
            "1".into(), "2".into(), "3".into()
        ]).unwrap();
        assert_eq!(mode, ApplicationModes::OfflineSession(vec![
            "1".into(), "2".into(), "3".into()
        ]));
    }

    #[test]
    fn test_remote_session() {
        let mode = ApplicationModes::parse(vec![
            "roadrunner.acme.com".into(), "9090".into()
        ]).unwrap();
        assert_eq!(mode, ApplicationModes::RemoteSession(
            "roadrunner.acme.com".into(), 9090
        ));
    }

}