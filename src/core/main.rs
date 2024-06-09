////////////////////////////////////////////////////////////////////
//      Oxide Server v0.1.0
////////////////////////////////////////////////////////////////////

use std::{env, thread};
use std::convert::Into;
use std::error::Error;
use std::fs::File;
use std::io::{Read, stdout, Write};
use std::string::ToString;
use std::sync::mpsc;
use std::time::Duration;

use actix::{Actor, Addr};
use actix_session::Session;
use actix_session::storage::CookieSessionStore;
use actix_web::{HttpRequest, HttpResponse, Responder, web};
use actix_web::cookie::Key;
use actix_web_actors::ws;
use crossterm::event::{Event, KeyEvent, poll, read};
use crossterm::terminal::enable_raw_mode;
use log::{error, info, LevelFilter};
use serde_json::Value;

use rest_server::*;
use shared_lib::{cnv_error, fail, get_host_and_port, RowJs};
use shared_lib::{RemoteCallRequest, RemoteCallResponse};

use crate::dataframe_actor::DataframeActor;
use crate::dataframe_config::DataFrameConfig;
use crate::interpreter::Interpreter;
use crate::namespaces::Namespace;
use crate::repl::REPLState;
use crate::rest_server::SharedState;
use crate::rows::Row;
use crate::server::SystemInfoJs;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue;
use crate::websockets::OxideWebSocket;

mod byte_buffer;
mod byte_row_collection;
mod codec;
mod compiler;
mod cursor;
mod dataframe_actor;
mod dataframe_config;
mod dataframes;
mod data_types;
mod expression;
mod field_metadata;
mod fields;
mod file_row_collection;
mod interpreter;
mod machine;
mod model_row_collection;
mod namespaces;
mod readme;
mod repl;
mod rest_server;
mod row_collection;
mod row_metadata;
mod rows;
mod serialization;
mod server;
mod table_columns;
mod table_view;
mod table_renderer;
mod table_writer;
mod template;
mod testdata;
mod token_slice;
mod tokenizer;
mod tokens;
mod typed_values;
mod websockets;

/// Starts the Oxide server
#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // set up the logger
    env_logger::builder()
        .filter_level(LevelFilter::Info)
        .init();

    // process the commandline arguments
    let args: Vec<String> = env::args().collect();
    match args.as_slice() {
        // listen: start a dedicated server?
        // ex: ./target/debug/oxide -listen 0.0.0.0 8080
        [_, cmd, ..] if cmd == "-l" || cmd == "-listen" =>
            start_server(args[1..args.len()].to_vec()).await,
        // peer: connect to a remote peer?
        // ex: ./target/debug/oxide -peer 0.0.0.0 8080
        [_, cmd, host, port] if cmd == "-p" || cmd == "-peer" => {
            let port = match port.parse::<u32>() {
                Ok(value) => value,
                Err(err) => panic!("{}", err.to_string())
            };
            repl::run(REPLState::connect(host.clone(), port)).await
        }
        // script: execute a script file?
        // ex: ./target/debug/oxide -script ./test.ox
        [_, cmd, path] if cmd == "-s" || cmd == "-script" => {
            run_script(path)?;
            Ok(())
        }
        // default: start the REPL
        // ex: ./target/debug/oxide
        _ => repl::run(REPLState::new()).await
    }
}

fn run_script(script_path: &str) -> std::io::Result<TypedValue> {
    // read the script file contents into the string
    let mut file = File::open(script_path)?;
    let mut script_code = String::new();
    file.read_to_string(&mut script_code)?;

    // execute the script code
    let mut interpreter = Interpreter::new();
    interpreter.evaluate(script_code.as_str())
}

async fn start_server(args: Vec<String>) -> std::io::Result<()> {
    // get the commandline arguments
    let (host, port) = get_host_and_port(args)?;

    // start the server
    info!("Welcome to Oxide Server.\n");
    info!("Starting server on port {}:{}.", host, port);
    let server = actix_web::HttpServer::new(move || web_routes!(SharedState::new()))
        .bind(format!("{}:{}", host, port))?
        .run();
    server.await?;
    Ok(())
}