#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
//      Oxide REST Server
////////////////////////////////////////////////////////////////////

extern crate core;

use crate::connections::webservers;
use crate::terminal::TerminalState;
use crate::utils::{is_u16, parse_u16};
use log::LevelFilter;
use serde::{Deserialize, Serialize};
use shared_lib::cnv_error;
use std::env;
use std::string::ToString;

mod blob_file_row_collection;
mod blobs;
mod builtins;
mod byte_code_compiler;
mod byte_row_collection;
mod columns;
mod compiler;
mod connections;
mod conversions;
mod dataframe;
mod dataframe_actor;
mod data_types;
mod errors;
mod expression;
mod extractions;
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
mod type_engine;
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
    let mode = ApplicationModes::parse(args)?;
    let (state, args) = get_terminal_state_and_args(mode).await?;
    terminal::do_terminal(state, args).await
}

async fn get_terminal_state_and_args(mode: ApplicationModes) -> std::io::Result<(TerminalState, Vec<String>)> {
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
    Ok((state, args))
}

/// Unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::connections::webservers::{get_random_port, stop_server};
    use crate::test_util::start_test_server_async;

    #[test]
    fn test_embedded_session() {
        let mode = ApplicationModes::parse(vec![
            "--embedded".into(), "8754".into()
        ]).unwrap();
        assert_eq!(mode, ApplicationModes::EmbeddedSession(8754));
    }

    #[actix::test]
    async fn test_get_terminal_state_and_args_embedded() {
        let port = get_random_port().unwrap();
        let (_state, args) = get_terminal_state_and_args(ApplicationModes::EmbeddedSession(port)).await.unwrap();
        assert!(args.is_empty());
        stop_server(port).await.unwrap();
    }

    #[actix::test]
    async fn test_get_terminal_state_and_args_offline() {
        let (_state, args) = get_terminal_state_and_args(ApplicationModes::OfflineSession(vec![
            "uno".into(),
            "dos".into(),
            "tres".into(),
        ])).await.unwrap();
        assert_eq!(args, vec!["uno".to_string(), "dos".into(), "tres".into()]);
    }

    #[actix::test]
    async fn test_get_terminal_state_and_args_remote() {
        let port = start_test_server_async().await.unwrap();
        let (_state, args) = get_terminal_state_and_args(ApplicationModes::RemoteSession(LOCAL_HOST.into(), port)).await.unwrap();
        assert!(args.is_empty());
        stop_server(port).await.unwrap();
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