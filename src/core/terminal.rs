#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Oxide Terminal module
////////////////////////////////////////////////////////////////////

use crate::file_row_collection::FileRowCollection;
use crate::numbers::Numbers::{F64Value, I64Value, U16Value};
use crate::platform::PlatformOps;
use crate::repl::{build_output, cleanup, limit_width, read_until_blank, show_title};
use crate::row_collection::RowCollection;
use crate::sequences::Array;
use crate::structures::Row;
use crate::structures::Structure;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::*;
use crate::websockets::OxideWebSocketClient;
use chrono::Local;
use crossterm::terminal;
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};
use shared_lib::compute_time_millis;
use std::io::{stdout, Read, Write};

/// Terminal application state
pub struct TerminalState {
    database: String,
    schema: String,
    session_id: i64,
    user_id: i64,
    user_name: String,
    counter: usize,
    is_alive: bool,
    interpreter: OxideWebSocketClient,
}

impl TerminalState {
    /// default constructor
    pub async fn connect(host: &str, port: u16, path: &str) -> std::io::Result<TerminalState> {
        Ok(TerminalState {
            database: "oxide".into(),
            schema: "public".into(),
            interpreter: OxideWebSocketClient::connect(host, port, path).await?,
            session_id: Local::now().timestamp_millis(),
            user_id: users::get_current_uid().to_i64().unwrap_or(-1),
            user_name: users::get_current_username().iter()
                .flat_map(|oss| oss.as_os_str().to_str())
                .collect(),
            counter: 0,
            is_alive: true,
        })
    }

    /// instructs the REPL to quit after the current statement has been processed
    pub fn die(&mut self) {
        self.is_alive = false
    }

    /// return the REPL prompt string (e.g. "oxide.public[4]>")
    pub fn get_prompt(&self) -> String {
        format!("{}@{}[{}]> ", self.user_name, self.database, self.counter)
    }

    /// returns true, if the application is running
    pub fn is_alive(&self) -> bool {
        self.is_alive
    }
}

pub async fn do_terminal(
    mut state: TerminalState,
    args: Vec<String>,
    reader: fn() -> Box<dyn FnMut() -> std::io::Result<Option<String>>>,
) -> std::io::Result<()> {
    // show title
    let mut stdout = stdout();
    show_title();
    stdout.flush()?;

    // setup system variables
    state = setup_system_variables(state, args).await;

    // handle user input
    while state.is_alive {
        (stdout, state) = do_terminal_input(state, stdout, reader).await?
    }
    Ok(())
}

async fn do_terminal_input(
    mut state: TerminalState,
    mut stdout: std::io::Stdout,
    mut reader: fn() -> Box<dyn FnMut() -> std::io::Result<Option<String>>>,
) -> std::io::Result<(std::io::Stdout, TerminalState)> {
    // display the prompt
    print!("{}", state.get_prompt());
    stdout.flush()?;

    // get and process the input
    let raw_input = read_until_blank(reader())?;
    let input = raw_input.trim();
    if input == "q!" { state.die() } else {
        if !input.is_empty() {
            let t0 = Local::now();
            match state.interpreter.evaluate(input).await {
                Ok(result) => {
                    // compute the execution-time
                    let execution_time = compute_time_millis(Local::now() - t0);
                    // record the request in the history table
                    update_history(&state, input, execution_time).ok();
                    // process the result
                    let limit = state.interpreter.evaluate("__COLUMNS__").await?.to_usize();
                    let raw_lines = build_output(state.counter, result, execution_time)?;
                    let lines = limit_width(raw_lines.clone(), limit);
                    for line in lines { println!("{}", line) }
                }
                Err(err) => eprintln!("{}", err)
            }
            state.counter += 1;
            stdout.flush()?
        }
    }
    Ok((stdout, state))
}

async fn setup_system_variables(mut state: TerminalState, args: Vec<String>) -> TerminalState {
    // capture the commandline arguments
    state.interpreter
        .with_variable("__ARGS__", ArrayValue(Array::from(args.iter()
            .map(|s| StringValue(s.to_string()))
            .collect::<Vec<_>>()
        ))).await.unwrap();

    // capture the session ID
    state.interpreter
        .with_variable("__SESSION_ID__", Number(I64Value(state.session_id)))
        .await.unwrap();

    // capture the user ID
    state.interpreter
        .with_variable("__USER_ID__", Number(I64Value(state.user_id)))
        .await.unwrap();

    // capture the terminal width and height
    if let Ok((width, height)) = terminal::size() {
        state.interpreter
            .with_variable("__COLUMNS__", Number(U16Value(width))).await.unwrap();
        state.interpreter
            .with_variable("__HEIGHT__", Number(U16Value(height))).await.unwrap();
    }
    state
}

fn update_history(
    state: &TerminalState,
    input: &str,
    processing_time: f64,
) -> std::io::Result<TypedValue> {
    let mut frc = FileRowCollection::open_or_create(
        &PlatformOps::get_oxide_history_ns(),
        PlatformOps::get_oxide_history_parameters())?;
    let result = frc.append_row(Row::new(0, vec![
        Number(I64Value(state.session_id)),
        Number(I64Value(state.user_id)),
        Number(F64Value(processing_time)),
        StringValue(cleanup(input))
    ]));
    Ok(result)
}

/// Unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::oxide_server::start_http_server;
    use crate::repl::read_line_from;
    use crate::testdata::start_test_server;

    #[actix::test]
    async fn test_do_terminal_input() {
        let port = 7701;
        start_test_server(port);
        let state = TerminalState::connect("localhost", port, "/ws").await.unwrap();
        let stdout = stdout();
        let reader = || read_line_from(vec![
            "import oxide".into(),
            "help()".into(),
            "\n".into(),
        ]);
        let (_, new_state) = do_terminal_input(state, stdout, reader).await.unwrap();
        assert_eq!(new_state.database, "oxide".to_string());
        assert_eq!(new_state.schema, "public".to_string());
        assert_eq!(new_state.counter, 1);
        assert_eq!(new_state.is_alive, true);
    }

    #[actix::test]
    async fn test_build_output() {
        let port = 7703;
        start_http_server(port);
        let mut state = TerminalState::connect("localhost", port, "/ws").await.unwrap();
        let result = state.interpreter.evaluate(r#"
            tools::describe(oxide::help())
        "#).await.unwrap();

        let lines = build_output(12, result, 13.2).unwrap();
        assert_eq!(lines, vec![
            "12: 5 row(s) in 13.2 ms ~ Table(String(128), String(128), String(128), Boolean)",
            "|-------------------------------------------------------------|",
            "| id | name        | type       | default_value | is_nullable |",
            "|-------------------------------------------------------------|",
            "| 0  | name        | String(20) | null          | true        |",
            "| 1  | module      | String(20) | null          | true        |",
            "| 2  | signature   | String(32) | null          | true        |",
            "| 3  | description | String(60) | null          | true        |",
            "| 4  | returns     | String(32) | null          | true        |",
            "|-------------------------------------------------------------|"])
    }
}