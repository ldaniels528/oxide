#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Oxide REPL module
////////////////////////////////////////////////////////////////////

use crate::columns::Column;
use crate::dataframe::Dataframe;
use crate::dataframe::Dataframe::Model;
use crate::file_row_collection::FileRowCollection;
use crate::interpreter::Interpreter;
use crate::model_row_collection::ModelRowCollection;
use crate::numbers::Numbers::{F64Value, I64Value};
use crate::oxide_server::SharedState;
use crate::packages::OxidePkg;
use crate::parameter::Parameter;
use crate::platform::PackageOps;
use crate::repl;
use crate::row_collection::RowCollection;
use crate::sequences::Array;
use crate::structures::Row;
use crate::structures::Structures::{Hard, Soft};
use crate::structures::{HardStructure, SoftStructure, Structure};
use crate::table_renderer::TableRenderer;
use crate::terminal::{build_output, build_output_header, cleanup, compile_only, get_hard_type, get_host_and_port, get_soft_type, get_table_type, limit_width, read_until_blank, show_title};
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::*;
use crate::utils::compute_time_millis;
use chrono::{DateTime, Local, TimeDelta};
use crossterm::terminal;
use log::info;
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};
use shared_lib::cnv_error;
use std::fs::File;
use std::io;
use std::io::{stdout, Read, Write};

/// REPL application state
pub struct REPLState {
    database: String,
    schema: String,
    session_id: i64,
    user_id: i64,
    user_name: String,
    counter: usize,
    is_alive: bool,
    interpreter: Interpreter,
}

impl REPLState {
    /// default constructor
    pub fn new() -> REPLState {
        REPLState {
            database: "oxide".into(),
            schema: "public".into(),
            interpreter: Interpreter::new(),
            session_id: Local::now().timestamp_millis(),
            user_id: users::get_current_uid().to_i64().unwrap_or(-1),
            user_name: users::get_current_username()
                .iter()
                .flat_map(|oss| oss.as_os_str().to_str())
                .collect(),
            counter: 0,
            is_alive: true,
        }
    }

    /// instructs the REPL to quit after the current statement has been processed
    pub fn die(&mut self) {
        self.is_alive = false
    }

    /// return the REPL prompt string (e.g. "oxide.public[4]>")
    pub fn get_prompt(&self) -> String {
        format!("{}@{}[{}]> ", self.user_name, self.database, self.counter)
    }

    pub fn is_alive(&self) -> bool {
        self.is_alive
    }
}

pub fn do_terminal(
    mut state: REPLState,
    args: Vec<String>,
    reader: fn() -> Box<dyn FnMut() -> std::io::Result<Option<String>>>,
) -> std::io::Result<()> {
    // show title
    let mut stdout = stdout();
    show_title();
    stdout.flush()?;

    // setup system variables
    state = setup_system_variables(state, args);

    // handle user input
    while state.is_alive {
        (stdout, state) = do_terminal_input(state, stdout, reader)?
    }
    Ok(())
}

fn do_terminal_input(
    mut state: REPLState,
    mut stdout: std::io::Stdout,
    mut reader: fn() -> Box<dyn FnMut() -> std::io::Result<Option<String>>>,
) -> std::io::Result<(std::io::Stdout, REPLState)> {
    // display the prompt
    print!("{}", state.get_prompt());
    stdout.flush()?;

    // get and process the input
    let raw_input = read_until_blank(reader())?;
    match raw_input.trim() {
        "@compile" => stdout = compile_only(stdout, reader)?,
        "q!" => state.die(),
        input if input.is_empty() => {}
        input => {
            let t0 = Local::now();
            match state.interpreter.evaluate(input) {
                Ok(result) => {
                    // compute the execution-time
                    let execution_time = compute_time_millis(Local::now() - t0);
                    // record the request in the history table
                    update_history(&state, input, execution_time).ok();
                    // process the result
                    let limit = state.interpreter.get("__COLUMNS__").map(|v| v.to_usize());
                    let raw_lines = build_output(state.counter, result, execution_time)?;
                    let lines = limit
                        .map(|n| limit_width(raw_lines.clone(), n))
                        .unwrap_or(raw_lines);
                    for line in lines {
                        println!("{}", line)
                    }
                }
                Err(err) => eprintln!("{}", err),
            }
            state.counter += 1;
            stdout.flush()? 
        }
    }
    Ok((stdout, state))
}

fn setup_system_variables(mut state: REPLState, args: Vec<String>) -> REPLState {
    // capture the commandline arguments
    state.interpreter.with_variable(
        "__ARGS__",
        ArrayValue(Array::from(
            args.iter()
                .map(|s| StringValue(s.to_string()))
                .collect::<Vec<_>>(),
        )),
    );

    // capture the session ID
    state
        .interpreter
        .with_variable("__SESSION_ID__", Number(I64Value(state.session_id)));

    // capture the user ID
    state
        .interpreter
        .with_variable("__USER_ID__", Number(I64Value(state.user_id)));

    // capture the terminal width and height
    if let Ok((width, height)) = terminal::size() {
        state
            .interpreter
            .with_variable("__COLUMNS__", Number(I64Value(width as i64)));
        state
            .interpreter
            .with_variable("__HEIGHT__", Number(I64Value(height as i64)));
    }
    state
}

/// Starts the listener server
async fn start_server(args: Vec<String>) -> std::io::Result<()> {
    use crate::oxide_server::*;
    use crate::platform::{MAJOR_VERSION, MINOR_VERSION};
    use crate::web_routes;
    use actix_web::web;
    use actix_web::web::Data;
    use log::info;
    use shared_lib::cnv_error;

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

fn update_history(
    state: &REPLState,
    input: &str,
    processing_time: f64,
) -> std::io::Result<TypedValue> {
    let mut frc = FileRowCollection::open_or_create(
        &OxidePkg::get_oxide_history_ns(),
        OxidePkg::get_oxide_history_parameters(),
    )?;
    let result = frc.append_row(Row::new(
        0,
        vec![
            Number(I64Value(state.session_id)),
            Number(I64Value(state.user_id)),
            Number(F64Value(processing_time)),
            StringValue(cleanup(input)),
        ],
    ));
    Ok(result)
}

/// Unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::repl::REPLState;
    use crate::terminal::{read_line_from, run_script};
    use std::cmp::max;
    use std::fs;

    #[test]
    fn test_do_terminal_input() {
        let state = REPLState::new();
        let stdout = stdout();
        let reader = || read_line_from(vec!["use oxide".into(), "help()".into(), "\n".into()]);
        let (_, new_state) = do_terminal_input(state, stdout, reader).unwrap();
        assert_eq!(new_state.database, "oxide");
        assert_eq!(new_state.schema, "public");
        assert_eq!(new_state.counter, 1);
        assert_eq!(new_state.is_alive, true);
    }

    #[test]
    fn test_get_prompt() {
        let mut state: REPLState = REPLState::new();
        let prompt = state.get_prompt();
        // prompt: "teddy.bear@oxide[0]> "
        assert!(prompt.contains("@oxide") && prompt.contains("[0]> "));
    }

    #[test]
    fn test_is_alive() {
        let mut state: REPLState = REPLState::new();
        assert_eq!(state.is_alive(), true);

        state.die();
        assert_eq!(state.is_alive(), false);
    }

    #[test]
    fn test_build_output_header_string() {
        let mut interpreter = Interpreter::new();
        let result = interpreter
            .evaluate(
                r#"
            "Hello World"
        "#,
            )
            .unwrap();

        let lines = build_output_header(12, &result, 0.1).unwrap();
        assert_eq!(lines, "12: returned type `String(11)` in 0.1 ms")
    }

    #[test]
    fn test_build_output_header_table() {
        let mut interpreter = Interpreter::new();
        let result = interpreter
            .evaluate(
                r#"
            tools::describe(oxide::help())
        "#,
            )
            .unwrap();

        let lines = build_output_header(12, &result, 13.2).unwrap();
        assert_eq!(
            lines,
            "12: 5 row(s) in 13.2 ms ~ Table(String(128), String(128), String(128), Boolean)"
        )
    }

    #[test]
    fn test_build_output() {
        let mut interpreter = Interpreter::new();
        let result = interpreter
            .evaluate(
                r#"
            tools::describe(oxide::help())
        "#,
            )
            .unwrap();

        let lines = build_output(12, result, 13.2).unwrap();
        assert_eq!(
            lines,
            vec![
                "12: 5 row(s) in 13.2 ms ~ Table(String(128), String(128), String(128), Boolean)",
                "|-------------------------------------------------------------|",
                "| id | name        | type       | default_value | is_nullable |",
                "|-------------------------------------------------------------|",
                "| 0  | name        | String(20) | null          | true        |",
                "| 1  | module      | String(20) | null          | true        |",
                "| 2  | signature   | String(32) | null          | true        |",
                "| 3  | description | String(60) | null          | true        |",
                "| 4  | returns     | String(32) | null          | true        |",
                "|-------------------------------------------------------------|"
            ]
        )
    }

    #[test]
    fn test_limit_width() {
        let lines0 = vec![
            "|-------------------------------------------------------------|".into(),
            "| id | name        | type       | default_value | is_nullable |".into(),
            "|-------------------------------------------------------------|".into(),
            "| 0  | name        | String(20) | null          | true        |".into(),
            "| 1  | module      | String(20) | null          | true        |".into(),
            "| 2  | signature   | String(32) | null          | true        |".into(),
            "| 3  | description | String(60) | null          | true        |".into(),
            "| 4  | returns     | String(32) | null          | true        |".into(),
            "|-------------------------------------------------------------|".into(),
        ];
        let lines1 = limit_width(lines0, 50);
        assert_eq!(
            lines1,
            vec![
                "|-------------------------------------------------",
                "| id | name        | type       | default_value | ",
                "|-------------------------------------------------",
                "| 0  | name        | String(20) | null          | ",
                "| 1  | module      | String(20) | null          | ",
                "| 2  | signature   | String(32) | null          | ",
                "| 3  | description | String(60) | null          | ",
                "| 4  | returns     | String(32) | null          | ",
                "|-------------------------------------------------"
            ]
        )
    }

    #[test]
    fn test_read_line_from() {
        let mut reader = read_line_from(vec!["abc".into(), "def".into(), "ghi".into()]);
        assert_eq!(reader().unwrap(), Some("abc\n".into()));
        assert_eq!(reader().unwrap(), Some("def\n".into()));
        assert_eq!(reader().unwrap(), Some("ghi\n".into()));
    }

    #[test]
    fn test_read_until_blank() {
        let reader = read_line_from(vec!["use oxide".into(), "help()".into()]);
        let code = read_until_blank(reader).unwrap();
        assert_eq!(code, "use oxide\nhelp()\n")
    }

    #[test]
    fn test_run_script() {
        let file_path = "dummy.oxide";
        fs::remove_file(file_path).ok();
        let mut file = File::create_new(file_path).unwrap();
        file.write(b"5 + 5").unwrap();
        let result = run_script(file_path).unwrap();
        assert_eq!(result, Number(I64Value(10)));
        fs::remove_file(file_path).ok();
    }

    #[test]
    fn test_setup_system_variables() {
        let state = REPLState::new();
        let state = setup_system_variables(state, vec![]);
        assert!(matches!(
            state.interpreter.get("__ARGS__"),
            Some(ArrayValue(..))
        ));
        assert!(matches!(
            state.interpreter.get("__COLUMNS__"),
            Some(Number(..))
        ));
        assert!(matches!(
            state.interpreter.get("__HEIGHT__"),
            Some(Number(..))
        ));
    }

    #[test]
    fn test_show_title() {
        show_title()
    }

    #[test]
    fn test_update_history() {
        let state = REPLState::new();
        let _ = update_history(&state, "oxide::help()", 3.5).unwrap();
        let mut frc = FileRowCollection::open_or_create(
            &OxidePkg::get_oxide_history_ns(),
            OxidePkg::get_oxide_history_parameters(),
        )
        .unwrap();
        let count = frc.len().unwrap() as i64;
        let last = max(count - 1, -1);
        let row_id = last as usize;
        let row = frc.read_one(row_id).unwrap();
        assert!(row.is_some());
        frc.resize(row_id).unwrap();
    }
}
