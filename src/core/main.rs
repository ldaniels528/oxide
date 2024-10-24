////////////////////////////////////////////////////////////////////
//      Oxide Server v0.1.0
////////////////////////////////////////////////////////////////////

use std::env;
use std::fs::File;
use std::io::Read;
use std::string::ToString;

use actix_web::web;
use log::{info, LevelFilter};

use rest_server::*;
use shared_lib::{cnv_error, get_host_and_port};

use crate::interpreter::Interpreter;
use crate::machine::Machine;
use crate::repl::REPLState;
use crate::rest_server::SharedState;
use crate::table_renderer::TableRenderer;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::TableValue;

mod backdoor;
mod byte_code_compiler;
mod byte_row_collection;
mod codec;
mod compiler;
mod cursor;
mod dataframe_actor;
mod dataframe_config;
mod dataframes;
mod data_types;
mod decompiler;
mod errors;
mod expression;
mod field_metadata;
mod file_embedded_row_collection;
mod file_row_collection;
mod hash_table_row_collection;
mod interpreter;
mod machine;
mod model_row_collection;
mod namespaces;
mod native;
mod number_kind;
mod numbers;
mod outcomes;
mod parameter;
mod readme;
mod repl;
mod rest_server;
mod row_collection;
mod row_metadata;
mod rows;
mod server;
mod structure;
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
mod view_row_collection;
mod websockets;
mod neocodec;

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
        // no arguments: start the REPL
        // ex: ./target/debug/oxide
        [_] => repl::run(REPLState::new()).await,
        // listen: start a dedicated server?
        // ex: ./target/debug/oxide -listen 0.0.0.0 8080
        [_, cmd, ..] if cmd == "-l" || cmd == "-listen" =>
            start_server(args[1..args.len()].to_vec()).await,
        // peer: connect to a remote peer?
        // ex: ./target/debug/oxide -peer 0.0.0.0 8080
        [_, cmd, host, port] if cmd == "-p" || cmd == "-peer" => {
            let port = match port.parse::<u32>() {
                Ok(value) => value,
                Err(err) => panic!("port: {}", err.to_string())
            };
            repl::run(REPLState::connect(host.to_owned(), port)).await
        }
        // script: execute a script file?
        // ex: ./target/debug/oxide -script ./test.ox
        [_, cmd, path] if cmd == "-s" || cmd == "-script" => {
            run_script(path).await?;
            Ok(())
        }
        // execute the command
        // ex: ./target/debug/oxide 5 + 7
        args => run_command(args[1..].join(" ").as_str())
    }
}

/// Executes a single command
fn run_command(command: &str) -> std::io::Result<()> {
    let mut interpreter = Interpreter::new();
    let result = interpreter.evaluate(command)?;
    match result {
        TableValue(mrc) => {
            let lines =
                TableRenderer::from_collection(Box::new(mrc.to_owned()));
            for line in lines { println!("{}", line); }
        }
        z =>
            println!("{}", z.unwrap_value())
    }
    Ok(())
}

/// Executes a script
async fn run_script(script_path: &str) -> std::io::Result<TypedValue> {
    // read the script file contents into the string
    let mut file = File::open(script_path)?;
    let mut script_code = String::new();
    file.read_to_string(&mut script_code)?;

    // execute the script code
    let mut interpreter = Interpreter::new();
    interpreter.evaluate_async(script_code.as_str()).await
}

/// Starts the listener server
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