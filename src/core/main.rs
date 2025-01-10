#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
//      Oxide Server v0.1.0
////////////////////////////////////////////////////////////////////

use shared_lib::cnv_error;
use std::env;

use crate::repl::{read_line_from_stdin, REPLState};
use log::LevelFilter;

mod arrays;
mod blobs;
mod byte_code_compiler;
mod byte_row_collection;
mod columns;
mod compiler;
mod cursor;
mod dataframe;
mod dataframe_actor;
mod data_types;
mod descriptor;
mod errors;
mod expression;
mod field;
mod file_row_collection;
mod hash_table_row_collection;
mod hybrid_row_collection;
mod inferences;
mod interpreter;
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
mod server;
mod structures;
mod table_renderer;
mod template;
mod testdata;
mod token_slice;
mod tokenizer;
mod tokens;
mod typed_values;

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

