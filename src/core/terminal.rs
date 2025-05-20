#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Oxide Terminal module
////////////////////////////////////////////////////////////////////

use crate::compiler::Compiler;
use crate::dataframe::Dataframe;
use crate::file_row_collection::FileRowCollection;
use crate::interpreter::Interpreter;
use crate::numbers::Numbers::{F64Value, I64Value, U16Value};
use crate::parameter::Parameter;
use crate::platform::PackageOps;
use crate::row_collection::RowCollection;
use crate::sequences::Array;
use crate::structures::Structure;
use crate::structures::Structures::{Hard, Soft};
use crate::structures::{HardStructure, Row, SoftStructure};
use crate::table_renderer::TableRenderer;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::*;
use crate::utils::compute_time_millis;
use crate::websockets::OxideWebSocketClient;
use chrono::Local;
use crossterm::terminal;
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};
use shared_lib::cnv_error;
use std::fs::File;
use std::io::{stdin, stdout, Read, Write};

// Represents an enumeration of Terminal Consoles
pub enum TerminalConsoles {
    Local(Interpreter),
    Remote(OxideWebSocketClient),
}

impl TerminalConsoles {
    pub async fn evaluate(&mut self, input: &str) -> std::io::Result<TypedValue> {
        match self {
            TerminalConsoles::Local(interpreter) => interpreter.evaluate(input),
            TerminalConsoles::Remote(client) => client.evaluate(input).await,       
        }
    }

    pub async fn get(&mut self, name: &str) -> std::io::Result<Option<TypedValue>> {
        match self {
            TerminalConsoles::Local(interpreter) => {
                Ok(interpreter.get(name))
            }
            TerminalConsoles::Remote(client) => {
                Ok(Some(client.evaluate("__COLUMNS__").await?))
            }
        }
    }
    
    pub async fn with_variable(&mut self, name: &str, value: TypedValue) -> std::io::Result<TypedValue> {
        match self {
            TerminalConsoles::Local(interpreter) => {
                interpreter.with_variable(name, value);
                Ok(Boolean(true))
            }
            TerminalConsoles::Remote(client) => {
                client.with_variable(name, value).await
            }
        }
    }
}

/// Terminal application state
pub struct TerminalState {
    database: String,
    schema: String,
    session_id: i64,
    user_id: i64,
    user_name: String,
    counter: usize,
    is_alive: bool,
    interpreter: TerminalConsoles,
}

impl TerminalState {
    /// default constructor
    pub async fn connect(host: &str, port: u16, path: &str) -> std::io::Result<TerminalState> {
        Ok(TerminalState {
            database: "oxide".into(),
            schema: "public".into(),
            interpreter: TerminalConsoles::Remote(OxideWebSocketClient::connect(host, port, path).await?),
            session_id: Local::now().timestamp_millis(),
            user_id: users::get_current_uid().to_i64().unwrap_or(-1),
            user_name: users::get_current_username().iter()
                .flat_map(|oss| oss.as_os_str().to_str())
                .collect(),
            counter: 0,
            is_alive: true,
        })
    }

    /// default constructor
    pub fn offline() -> std::io::Result<TerminalState> {
        Ok(TerminalState {
            database: "oxide".into(),
            schema: "public".into(),
            interpreter: TerminalConsoles::Local(Interpreter::new()),
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

    /// returns true if the application is running
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
    match raw_input.trim() {
        "@compile" => stdout = compile_only(stdout, reader)?,
        "q!" => state.die(),
        input if input.is_empty() => {}
        input => {
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

/// Builds the execution result output
pub fn build_output(
    pid: usize,
    result: TypedValue,
    execution_time: f64,
) -> std::io::Result<Vec<String>> {
    let mut out: Vec<String> = vec![];
    out.push(build_output_header(pid, &result, execution_time)?);
    match result {
        TableValue(df) => {
            let rc: Box<dyn RowCollection> = Box::from(df);
            let lines = TableRenderer::from_table_with_ids(&rc)?;
            out.extend(lines)
        }
        Structured(s) => {
            out.extend(s.to_pretty_json()?
                .split("\n")
                .map(|s| s.to_string())
                .collect::<Vec<_>>());
        }
        z => out.push(z.unwrap_value())
    }
    Ok(out)
}

/// Builds the execution result output header
/// ex: "12: 5 row(s) in 13.2 ms ~ Table(String(128), String(128), String(128), Boolean)"
pub fn build_output_header(
    pid: usize,
    result: &TypedValue,
    execution_time: f64,
) -> std::io::Result<String> {
    let label = match &result {
        TableValue(tv) =>
            format!("{} row(s) in {execution_time:.1} ms ~ {}", tv.len()?, get_table_type(tv)),
        other => {
            let kind = match other {
                Structured(Hard(hs)) => get_hard_type(hs),
                Structured(Soft(ss)) => get_soft_type(ss),
                v => v.get_type_name()
            };
            format!("returned type `{}` in {execution_time:.1} ms", kind)
        }
    };
    Ok(format!("{pid}: {label}"))
}

pub fn cleanup(raw: &str) -> String {
    raw.split(|c| "\n\r\t".contains(c))
        .map(|s| s.trim())
        .collect::<Vec<_>>()
        .join("; ")
}

pub fn compile_only(
    mut stdout: std::io::Stdout,
    mut reader: fn() -> Box<dyn FnMut() -> std::io::Result<Option<String>>>,
) -> std::io::Result<std::io::Stdout> {
    let mut is_alive = true;
    while is_alive {
        // display the prompt
        stdout.write(b"compile> ")?;
        stdout.flush()?;

        // compile or quit
        match read_until_blank(reader())?.trim() {
            "q!" => is_alive = false,
            input => {
                let model = Compiler::build(input)?;
                println!("{:?}", model)
            }
        }
    }

    Ok(stdout)
}

pub fn fail<A>(message: impl Into<String>) -> std::io::Result<A> {
    Err(std::io::Error::new(std::io::ErrorKind::Other, message.into()))
}

/// Generates a less verbose hard structure signature
/// ex: Table(String(128), String(128), String(128), Boolean)
pub fn get_hard_type(hs: &HardStructure) -> String {
    format!("Struct({})", get_parameter_string(&hs.get_parameters()))
}

/// Extracts a tuple consisting of the first two arguments from the supplied commandline arguments
pub fn get_host_and_port(args: Vec<String>) -> std::io::Result<(String, String)> {
    // args: ['./myapp', 'arg1', 'arg2', ..]
    let (host, port) = match args.as_slice() {
        [_, port] => (String::from("127.0.0.1"), port.to_string()),
        [_, host, port] => (host.to_string(), port.to_string()),
        [_, host, port, ..] => (host.to_string(), port.to_string()),
        _ => ("127.0.0.1".to_string(), "8080".to_string())
    };

    // validate the port number
    let port_regex = regex::Regex::new(r"^\d+$").map_err(|e| cnv_error!(e))?;
    if !port_regex.is_match(&port) {
        return fail(format!("Port number '{}' is invalid", port));
    }
    Ok((host, port))
}

pub fn get_parameter_string(params: &Vec<Parameter>) -> String {
    params.iter()
        .map(|p| p.get_param_type().unwrap_or("Any".into()))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Generates a less verbose hard structure signature
/// ex: Table(String(128), String(128), String(128), Boolean)
pub fn get_soft_type(ss: &SoftStructure) -> String {
    format!("Struct({})", get_parameter_string(&ss.get_parameters()))
}

/// Generates a less verbose table signature
/// ex: Table(String(128), String(128), String(128), Boolean)
pub fn get_table_type(rc: &Dataframe) -> String {
    let param_types = rc.get_columns().iter()
        .map(|c| c.get_data_type().to_code())
        .collect::<Vec<_>>()
        .join(", ");
    format!("Table({})", param_types)
}

pub fn limit_width(lines: Vec<String>, limit: usize) -> Vec<String> {
    lines.iter()
        .map(|s| if s.len() > limit { s[0..limit].to_string() } else { s.to_string() })
        .collect::<Vec<_>>()
}

pub fn read_line_from(lines: Vec<String>) -> Box<dyn FnMut() -> std::io::Result<Option<String>>> {
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
        stdin().read_line(&mut line)?;
        Ok(Some(line))
    })
}

/// Reads lines of input until a blank line is entered.
/// Returns the accumulated input as a single `String`.
pub fn read_until_blank(
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
pub fn run_script(script_path: &str) -> std::io::Result<TypedValue> {
    // read the script file contents into the string
    let mut file = File::open(script_path)?;
    let mut script_code = String::new();
    file.read_to_string(&mut script_code)?;

    // execute the script code
    let mut interpreter = Interpreter::new();
    interpreter.evaluate(script_code.as_str())
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

pub fn show_title() {
    use crate::platform::VERSION;
    println!("Welcome to Oxide v{VERSION}\n");
}

fn update_history(
    state: &TerminalState,
    input: &str,
    processing_time: f64,
) -> std::io::Result<TypedValue> {
    let mut frc = FileRowCollection::open_or_create(
        &PackageOps::get_oxide_history_ns(),
        PackageOps::get_oxide_history_parameters())?;
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
    use crate::testdata::start_test_server;
    use std::cmp::max;
    use std::fs;
    use std::fs::File;

    #[actix::test]
    async fn test_do_terminal_input() {
        let port = 7701;
        start_test_server(port);
        let state = TerminalState::connect("localhost", port, "/ws").await.unwrap();
        let stdout = stdout();
        let reader = || read_line_from(vec![
            "use oxide".into(),
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

    #[test]
    fn test_commandline_arguments() {
        assert_eq!(get_host_and_port(Vec::new()).unwrap(),
                   ("127.0.0.1".to_string(), "8080".to_string()));

        assert_eq!(get_host_and_port(vec!["my_app".into(), "3333".into()]).unwrap(),
                   ("127.0.0.1".to_string(), "3333".to_string()));

        assert_eq!(get_host_and_port(vec!["my_app".into(), "0.0.0.0".into(), "9000".into()]).unwrap(),
                   ("0.0.0.0".to_string(), "9000".to_string()));

        assert_eq!(get_host_and_port(vec!["my_app".into(), "127.0.0.1".into(), "3333".into(), "zzz".into()]).unwrap(),
                   ("127.0.0.1".to_string(), "3333".to_string()));
    }

    #[test]
    fn test_get_prompt() {
        let mut state: TerminalState = TerminalState::offline().unwrap();
        let prompt = state.get_prompt();
        // prompt: "teddy.bear@oxide[0]> "
        assert!(prompt.contains("@oxide") && prompt.contains("[0]> "));
    }

    #[test]
    fn test_is_alive() {
        let mut state: TerminalState = TerminalState::offline().unwrap();
        assert_eq!(state.is_alive(), true);

        state.die();
        assert_eq!(state.is_alive(), false);
    }

    #[test]
    fn test_build_output_header_string() {
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
            "Hello World"
        "#).unwrap();

        let lines = build_output_header(12, &result, 0.1).unwrap();
        assert_eq!(
            lines,
            "12: returned type `String(11)` in 0.1 ms")
    }

    #[test]
    fn test_build_output_header_table() {
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
            tools::describe(oxide::help())
        "#).unwrap();

        let lines = build_output_header(12, &result, 13.2).unwrap();
        assert_eq!(
            lines,
            "12: 5 row(s) in 13.2 ms ~ Table(String(128), String(128), String(128), Boolean)")
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
        assert_eq!(lines1, vec![
            "|-------------------------------------------------",
            "| id | name        | type       | default_value | ",
            "|-------------------------------------------------",
            "| 0  | name        | String(20) | null          | ",
            "| 1  | module      | String(20) | null          | ",
            "| 2  | signature   | String(32) | null          | ",
            "| 3  | description | String(60) | null          | ",
            "| 4  | returns     | String(32) | null          | ",
            "|-------------------------------------------------"])
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
            "use oxide".into(),
            "help()".into(),
        ]);
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

    // #[actix::test]
    // async fn test_setup_system_variables() {
    //     let state = TerminalState::offline().unwrap();
    //     let state = setup_system_variables(state, vec![]);
    //     assert!(matches!(state.get("__ARGS__").await, Some(ArrayValue(..))));
    //     assert!(matches!(state.get("__COLUMNS__"), Some(Number(..))));
    //     assert!(matches!(state.get("__HEIGHT__"), Some(Number(..))));
    // }

    #[test]
    fn test_show_title() {
        show_title()
    }

    #[test]
    fn test_update_history() {
        let state = TerminalState::offline().unwrap();
        let _ = update_history(&state, "oxide::help()", 3.5).unwrap();
        let mut frc = FileRowCollection::open_or_create(
            &PackageOps::get_oxide_history_ns(),
            PackageOps::get_oxide_history_parameters()).unwrap();
        let count = frc.len().unwrap() as i64;
        let last = max(count - 1, -1);
        let row_id = last as usize;
        let row = frc.read_one(row_id).unwrap();
        assert!(row.is_some());
        frc.resize(row_id).unwrap();
    }
}