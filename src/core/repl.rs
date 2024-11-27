////////////////////////////////////////////////////////////////////
// REPL module
////////////////////////////////////////////////////////////////////

use crate::expression::ACK;
use crate::file_row_collection::FileRowCollection;
use crate::interpreter::Interpreter;
use crate::model_row_collection::ModelRowCollection;
use crate::numbers::Numbers::{F64Value, I64Value, U16Value};
use crate::parameter::Parameter;
use crate::platform::PlatformOps;
use crate::repl;
use crate::rest_server::SharedState;
use crate::row_collection::RowCollection;
use crate::rows::Row;
use crate::table_columns::Column;
use crate::table_renderer::TableRenderer;
use crate::table_values::TableValues::Model;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::*;
use chrono::Local;
use crossterm::terminal;
use log::info;
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};
use shared_lib::get_host_and_port;
use std::fs::File;
use std::io;
use std::io::{stdout, Read, Write};

/// REPL application state
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct REPLState {
    database: String,
    schema: String,
    interpreter: Interpreter,
    session_id: i64,
    user_id: i64,
    counter: usize,
    is_alive: bool,
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
        format!("{}.{}[{}]> ", self.database, self.schema, self.counter)
    }

    pub fn is_alive(&self) -> bool {
        self.is_alive
    }
}

pub fn do_terminal(
    mut repl_state: REPLState,
    args: Vec<String>,
    mut reader: fn() -> Box<dyn FnMut() -> std::io::Result<Option<String>>>
) -> std::io::Result<()> {
    use crate::platform::{MAJOR_VERSION, MINOR_VERSION};
    let mut stdout = stdout();

    // show title
    println!("Welcome to Oxide v{MAJOR_VERSION}.{MINOR_VERSION}\n");
    stdout.flush()?;

    // setup system variables
    repl_state = setup_system_variables(repl_state, args);

    // handle user input
    loop {
        // display the prompt
        print!("{}", repl_state.get_prompt());
        stdout.flush()?;

        // get and process the input
        let raw_input = read_until_blank(reader())?;
        let input = raw_input.trim();
        if !input.is_empty() {
            let pid = repl_state.counter;
            let t0 = Local::now();
            match repl_state.interpreter.evaluate(input) {
                Ok(result) => {
                    // compute the execution-time
                    let dt = Local::now() - t0;
                    let execution_time = match dt.num_nanoseconds() {
                        Some(nano) => nano.to_f64().map(|t| t / 1e+6).unwrap_or(0.),
                        None => dt.num_milliseconds().to_f64().unwrap_or(0.)
                    };
                    // record the request
                    update_history(&repl_state, input, execution_time).ok();
                    // gather the value info
                    let type_name = result.get_type_name();
                    let extras = match &result {
                        TableValue(rcv) => format!(" ~ {} row(s)", &rcv.len()?),
                        _ => "".to_string()
                    };
                    // display the output
                    stdout.write(format!("[{pid}] {type_name}{extras} in {execution_time:.1} millis\n").as_bytes())?;
                    repl_state = print_result(repl_state, result)?;
                }
                Err(err) => eprintln!("{}", err)
            }
            repl_state.counter += 1;
        }
    }
}

fn print_result(state: REPLState, result: TypedValue) -> std::io::Result<REPLState> {
    match result {
        TableValue(rcv) => {
            let rc: Box<dyn RowCollection> = Box::from(rcv);
            let raw_lines = TableRenderer::from_table_with_ids(&rc)?;
            let lines = state.interpreter.get("__COLUMNS__")
                .map(|limit_v| {
                    let limit = limit_v.to_usize();
                    raw_lines.iter()
                        .map(|s| if s.len() > limit { s[0..limit].to_string() } else { s.to_string() })
                        .collect::<Vec<_>>()
                })
                .unwrap_or(raw_lines);
            lines.iter().for_each(|s| println!("{}", s))
        }
        z =>
            println!("{}", z.unwrap_value())
    }
    Ok(state)
}

fn read_line_from(lines: Vec<String>) -> Box<dyn FnMut() -> std::io::Result<Option<String>>> {
    let mut index = 0;
    Box::new(move || {
        Ok(if index < lines.len() {
            let result = Some(format!("{}\n", lines[index]));
            index += 1;
            result
        } else {
            None
        })
    })
}

pub fn read_line_from_stdin() -> Box<dyn FnMut() -> std::io::Result<Option<String>>> {
    Box::new(move || {
        let mut line = String::new();
        io::stdin().read_line(&mut line)?;
        Ok(Some(line))
    })
}

/// Reads lines of input until a blank line is entered.
/// Returns the accumulated input as a single `String`.
fn read_until_blank(
    mut reader: Box<dyn FnMut() -> std::io::Result<Option<String>>>
) -> std::io::Result<String> {
    let mut input_buffer = String::new();
    let mut done = false;
    while !done {
        // read a line of input
        match reader()? {
            Some(line) => {
                // check for blank line (empty or only whitespace)
                if line.trim().is_empty() { break; }

                // append line to buffer
                input_buffer.push_str(&line);
            }
            None => done = true
        }
    }

    Ok(input_buffer)
}

/// Executes a script
fn run_script(script_path: &str) -> std::io::Result<TypedValue> {
    // read the script file contents into the string
    let mut file = File::open(script_path)?;
    let mut script_code = String::new();
    file.read_to_string(&mut script_code)?;

    // execute the script code
    let mut interpreter = Interpreter::new();
    interpreter.evaluate(script_code.as_str())
}

fn setup_system_variables(mut repl_state: REPLState, args: Vec<String>) -> REPLState {
    // capture the commandline arguments
    repl_state.interpreter.with_variable("__ARGS__", Array(args.iter()
        .map(|s| StringValue(s.to_string()))
        .collect::<Vec<_>>()
    ));

    // capture the terminal width and height
    if let Ok((width, height)) = terminal::size() {
        repl_state.interpreter
            .with_variable("__COLUMNS__", Number(U16Value(width)));
        repl_state.interpreter
            .with_variable("__HEIGHT__", Number(U16Value(height)));
    }
    repl_state
}

/// Starts the listener server
async fn start_server(args: Vec<String>) -> std::io::Result<()> {
    use crate::rest_server::*;
    use crate::web_routes;
    use actix_web::web::Data;
    use crate::platform::{MAJOR_VERSION, MINOR_VERSION};
    use actix_web::web;
    use log::info;
    use shared_lib::{cnv_error, get_host_and_port};

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
    fn cleanup(raw: &str) -> String {
        raw.split(|c| "\n\r\t".contains(c))
            .map(|s| s.trim())
            .collect::<Vec<_>>()
            .join("; ")
    }
    let ns = PlatformOps::get_oxide_history_ns();
    let mut frc = FileRowCollection::open_or_create(&ns)?;
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
    use crate::compiler::Compiler;
    use crate::machine::Machine;
    use crate::repl::REPLState;
    use std::cmp::max;
    use std::fs;

    #[ignore]
    #[test]
    fn test_do_terminal() {
        do_terminal(REPLState::new(), vec![], || read_line_from(vec![
            "import oxide".into(),
            "help()".into(),
        ])).unwrap();
    }

    #[test]
    fn test_get_prompt() {
        let mut state: REPLState = REPLState::new();
        let prompt = state.get_prompt();
        assert_eq!(prompt, "oxide.public[0]> ".to_string());
    }

    #[test]
    fn test_is_alive() {
        let mut state: REPLState = REPLState::new();
        assert_eq!(state.is_alive(), true);

        state.die();
        assert_eq!(state.is_alive(), false);
    }

    #[test]
    fn test_print_result() {
        let state = REPLState::new();
        let state = print_result(state, Number(I64Value(100))).unwrap();
        let model = Compiler::build(r#"
            tools::to_table(["abc", "123"])
        "#).unwrap();
        let (_, table) = Machine::new().evaluate(&model).unwrap();
        print_result(state, table).unwrap();
    }

    #[test]
    fn test_read_line_from() {
        let mut reader = read_line_from(vec![
            "abc".into(),
            "def".into(),
            "ghi".into()
        ]);
        assert_eq!(reader().unwrap(), Some("abc\n".into()));
        assert_eq!(reader().unwrap(), Some("def\n".into()));
        assert_eq!(reader().unwrap(), Some("ghi\n".into()));
    }

    #[test]
    fn test_read_until_blank() {
        let reader = read_line_from(vec![
            "import oxide".into(),
            "help()".into(),
        ]);
        let code = read_until_blank(reader).unwrap();
        assert_eq!(code, "import oxide\nhelp()\n")
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
        assert!(matches!(state.interpreter.get("__ARGS__"), Some(Array(..))));
        assert!(matches!(state.interpreter.get("__COLUMNS__"), Some(Number(..))));
        assert!(matches!(state.interpreter.get("__HEIGHT__"), Some(Number(..))));
    }

    #[test]
    fn test_update_history() {
        let state = REPLState::new();
        let _ = update_history(&state, "oxide::help()", 3.5).unwrap();

        let ns = PlatformOps::get_oxide_history_ns();
        let mut frc = FileRowCollection::open_or_create(&ns).unwrap();
        let count = frc.len().unwrap() as i64;
        let last = max(count - 1, -1);
        let row_id = last as usize;
        let row = frc.read_one(row_id).unwrap();
        assert!(row.is_some());
        frc.resize(row_id);
    }
}