#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
//      Oxide REST Server
////////////////////////////////////////////////////////////////////

use crate::oxide_server::start_http_server;
use crate::repl::{read_line_from_stdin, REPLState};
use crate::terminal::TerminalState;
use log::LevelFilter;
use serde::{Deserialize, Serialize};
use shared_lib::cnv_error;
use std::env;
use std::string::ToString;

mod blobs;
mod byte_code_compiler;
mod byte_row_collection;
mod columns;
mod compiler;
mod cursor;
mod dataframe;
mod dataframe_actor;
mod data_types;
mod errors;
mod expression;
mod field;
mod file_row_collection;
mod hash_table_row_collection;
mod hybrid_row_collection;
mod inferences;
mod interpreter;
mod journaling;
mod machine;
mod model_row_collection;
mod namespaces;
mod number_kind;
mod numbers;
mod object_config;
mod oxide_server;
mod parameter;
mod platform;
mod query_engine;
mod readme;
mod repl;
mod row_collection;
mod row_metadata;
mod sequences;
mod server;
mod structures;
mod table_renderer;
mod template;
mod terminal;
mod testdata;
mod token_slice;
mod tokenizer;
mod tokens;
mod typed_values;
mod websockets;

const LOCAL_HOST: &str = "0.0.0.0";

/// Represents an enumeration of Application Modes
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
enum ApplicationModes {
    EmbeddedSession(u16),
    OfflineSession,
    RemoteSession(String, u16),
    StartupFailure(String),
}

impl ApplicationModes {
    /// Parses the commandline arguments
    pub fn parse(args: Vec<String>) -> ApplicationModes {
        match args.as_slice() {
            [_, port] if is_u16(port) =>
                match parse_u16(port) {
                    Ok(port) => ApplicationModes::RemoteSession(LOCAL_HOST.into(), port),
                    Err(err) => ApplicationModes::StartupFailure(err.to_string())
                }
            [_, action, port] if action == "embedded" && is_u16(port) =>
                match parse_u16(port) {
                    Ok(port) => ApplicationModes::EmbeddedSession(port),
                    Err(err) => ApplicationModes::StartupFailure(err.to_string())
                }
            [_, host, port] if is_u16(port) =>
                match parse_u16(port) {
                    Ok(port) => ApplicationModes::RemoteSession(host.into(), port),
                    Err(err) => ApplicationModes::StartupFailure(err.to_string())
                }
            _ => ApplicationModes::OfflineSession,
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
    match ApplicationModes::parse(env::args().collect()) {
        ApplicationModes::EmbeddedSession(port) => {
            println!("Starting embedded Oxide service on port {port}...");
            start_http_server(port);
            start_online_session(LOCAL_HOST, port).await?
        }
        ApplicationModes::RemoteSession(host, port) => {
            println!("Connecting to remote Oxide service at {host}:{port}...");
            start_online_session(host.as_str(), port).await?
        }
        ApplicationModes::OfflineSession => {
            println!("Starting offline Oxide service...");
            start_offline_session()?
        }
        ApplicationModes::StartupFailure(message) => {
            eprintln!("{}", message)
        }
    }
    Ok(())
}

/// Tests whether as string could be converted into an u16
fn is_u16(s: &str) -> bool { s.parse::<u16>().is_ok() }

/// Converts the contents of a string to u16
fn parse_u16(s: &str) -> std::io::Result<u16> {
    s.parse::<u16>().map_err(|e| cnv_error!(e))
}

/// Starts a standalone/offline Oxide REPL
fn start_offline_session() -> std::io::Result<()> {
    repl::do_terminal(
        REPLState::new(),
        env::args().collect(),
        || read_line_from_stdin(),
    )
}

/// Starts a websockets-based Oxide Server
async fn start_online_session(host: &str, port: u16) -> std::io::Result<()> {
    terminal::do_terminal(
        TerminalState::connect(host, port, "/ws").await?,
        env::args().collect(),
        || read_line_from_stdin(),
    ).await?;
    Ok(())
}

/// Unit tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_u16() {
        assert!(is_u16("456"))
    }

    #[test]
    fn test_is_u16_vs_float() {
        assert!(!is_u16("113.76"))
    }

    #[test]
    fn test_parse_u16() {
        assert_eq!(parse_u16("8766").unwrap(), 8766)
    }

    #[test]
    fn test_parse_args_embedded_session() {
        let args = ApplicationModes::parse(vec![
            "oxide".into(), "embedded".into(), "8754".into()
        ]);
        assert_eq!(args, ApplicationModes::EmbeddedSession(8754));
    }

    #[test]
    fn test_parse_args_offline_session() {
        let args = ApplicationModes::parse(vec![
            "oxide".into()
        ]);
        assert_eq!(args, ApplicationModes::OfflineSession);
    }


    #[test]
    fn test_parse_args_local_session() {
        let args = ApplicationModes::parse(vec![
            "oxide".into(), "8754".into()
        ]);
        assert_eq!(args, ApplicationModes::RemoteSession(LOCAL_HOST.into(), 8754));
    }

    #[test]
    fn test_parse_args_remote_session() {
        let args = ApplicationModes::parse(vec![
            "oxide".into(), "roadrunner.acme.com".into(), "8754".into()
        ]);
        assert_eq!(args, ApplicationModes::RemoteSession("roadrunner.acme.com".into(), 8754));
    }
}