#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
//      Oxide Server v0.1.0
////////////////////////////////////////////////////////////////////

use shared_lib::cnv_error;
use std::env;

use crate::repl::{read_line_from_stdin, REPLState};
use log::LevelFilter;

mod arrays;
mod byte_code_compiler;
mod byte_row_collection;
mod compiler;
mod cursor;
mod dataframe_actor;
mod dataframe_config;
mod data_types;
mod errors;
mod expression;
mod field_metadata;
mod file_embedded_row_collection;
mod file_row_collection;
mod hash_table_row_collection;
mod inferences;
mod interpreter;
mod machine;
mod model_row_collection;
mod namespaces;
mod number_kind;
mod numbers;
mod descriptor;
mod platform;
mod readme;
mod repl;
mod rest_server;
mod row_collection;
mod dataframe;
mod row_metadata;
mod server;
mod structures;
mod columns;
mod table_renderer;
mod template;
mod testdata;
mod token_slice;
mod tokenizer;
mod tokens;
mod typed_values;
mod view_row_collection;
mod websockets;
mod query_engine;
mod parameter;

/// Starts the Oxide server
fn main() -> std::io::Result<()> {
    // set up the logger
    env_logger::builder()
        .filter_level(LevelFilter::Info)
        .init();

    // start the REPL
    repl::do_terminal(
        REPLState::new(),
        env::args().collect(),
        || read_line_from_stdin(),
    )
}

