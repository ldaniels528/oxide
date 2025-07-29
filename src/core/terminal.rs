#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Oxide Terminal module
////////////////////////////////////////////////////////////////////

use crate::compiler::Compiler;
use crate::dataframe::Dataframe;
use crate::dataframe::Dataframe::{ModelTable, TestReport};
use crate::errors::throw;
use crate::errors::Errors::Exact;
use crate::interpreter::Interpreter;
use crate::numbers::Numbers::I64Value;
use crate::parameter::Parameter;
use crate::row_collection::RowCollection;
use crate::sequences::Array;
use crate::structures::Structure;
use crate::structures::Structures::{Hard, Soft};
use crate::structures::{HardStructure, SoftStructure};
use crate::table_renderer::TableRenderer;
use crate::test_engine::TestEngine;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::*;
use crate::utils::compute_time_millis;
use crate::web_engine::WebSocketClient;
use chrono::Local;
use crossterm::style::Stylize;
use crossterm::terminal;
use num_traits::ToPrimitive;
use rustyline::completion::{Completer, Pair};
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::history::FileHistory;
use rustyline::validate::Validator;
use rustyline::{Context, Helper};
use serde::{Deserialize, Serialize};
use shared_lib::cnv_error;
use std::fs::File;
use std::io::{stdout, Read, Write};

/// Oxide REPL auto-completion config
pub struct OxideCompleter {
    keywords: Vec<String>,
}

impl OxideCompleter {
    pub fn new() -> Self {
        Self {
            keywords: Compiler::get_keywords()
        }
    }
}

impl Completer for OxideCompleter {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let word_start = line[..pos]
            .rfind(|c: char| !(c.is_alphanumeric() || c == '_'))
            .map_or(0, |i| i + 1);

        let prefix = &line[word_start..pos];

        let candidates = self
            .keywords
            .iter()
            .filter(|kw| kw.starts_with(prefix))
            .map(|kw| Pair {
                display: kw.clone(),
                replacement: kw.clone(),
            })
            .collect();

        Ok((word_start, candidates))
    }
}

impl Helper for OxideCompleter {}
impl Hinter for OxideCompleter {
    type Hint = String;
    fn hint(&self, _line: &str, _pos: usize, _ctx: &rustyline::Context<'_>) -> Option<String> {
        None
    }
}
impl Highlighter for OxideCompleter {}
impl Validator for OxideCompleter {}

// Represents an enumeration of Terminal Consoles
pub enum TerminalConsoles {
    Local(Interpreter),
    Remote(WebSocketClient),
}

impl TerminalConsoles {
    pub async fn evaluate(&mut self, input: &str) -> std::io::Result<TypedValue> {
        match self {
            TerminalConsoles::Local(interpreter) => interpreter.evaluate_async(input).await,
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
            interpreter: TerminalConsoles::Remote(WebSocketClient::connect(host, port, path).await?),
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


/// Builds the execution result output
pub fn build_output(
    pid: usize,
    result: TypedValue,
    execution_time: f64,
) -> std::io::Result<Vec<String>> {
    let mut out: Vec<String> = vec![];
    out.push(build_output_header(pid, &result, execution_time)?);
    match result {
        TableValue(TestReport(mrc, state)) => {
            let mut report = TestEngine::generate_summary(&state);
            report.push("".to_string());
            report.extend(TestEngine::generate_report(ModelTable(mrc)));
            out.extend(report)
        }
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
        StringValue(s) => {
            let lines = s.split('\n')
                .map(|s| s.to_string())
                .collect::<Vec<_>>();
            out.extend(lines)
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
        TableValue(tv) => {
            let outcome = format!("{} row(s) in {execution_time:.1} ms", tv.len()?).reverse();
            format!("{} ~ {}", outcome, get_table_type(tv))
                .reverse().to_string()
        }
        other => {
            let kind = match other {
                Structured(Hard(hs)) => get_hard_type(hs),
                Structured(Soft(ss)) => get_soft_type(ss),
                v => v.get_type_decl()
            };
            let outcome = format!("`{}` in {execution_time:.1} ms", kind).reverse();
            format!("returned type {}", outcome)
        }
    };
    Ok(format!("{pid}: {label}"))
}

pub async fn do_terminal(
    mut state: TerminalState,
    args: Vec<String>,
) -> std::io::Result<()> {
    use rustyline::error::ReadlineError;
    use rustyline::{Config, Editor};

    // show title
    let mut stdout = stdout();
    show_title();
    stdout.flush()?;

    // setup system variables
    state = setup_system_variables(state, args).await;

    // create the editor configuration
    let config = Config::builder()
        .completion_type(rustyline::CompletionType::List)
        .build();
    let completer = OxideCompleter::new();
    let mut rl = Editor::<OxideCompleter, FileHistory>::with_config(config)
        .map_err(|e| cnv_error!(e))?;
    rl.set_helper(Some(completer));

    let mut buffer = String::new();
    let mut prompt = state.get_prompt();

    loop {
        let readline = rl.readline(prompt.as_str());
        match readline {
            Ok(raw_line) => {
                let line = raw_line.trim();
                if buffer.is_empty() && line == "q!" {
                    break;
                }

                // update the buffer
                buffer.push_str(line);
                buffer.push('\n');

                // if the statement is incomplete, keep buffering
                if is_incomplete(&buffer) {
                    prompt = "...> ".into();
                    continue;
                }

                let trimmed = buffer.trim();
                if !trimmed.is_empty() {
                    // add the line to history
                    rl.add_history_entry(trimmed).map_err(|e| cnv_error!(e))?;

                    // evaluate the input
                    state = handle_input(state, trimmed).await?;

                    // Reset for next input
                    buffer.clear();
                    prompt = state.get_prompt();
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("^C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("^D");
                break;
            }
            Err(err) => {
                eprintln!("REPL error: {:?}", err);
                break;
            }
        }
    }

    println!("👋 Goodbye!");
    Ok(())
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
        return throw(Exact(format!("Port number '{}' is invalid", port)));
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

async fn handle_input(mut state: TerminalState, input: &str) -> std::io::Result<TerminalState> {
    let t0 = Local::now();
    match state.interpreter.evaluate(input).await {
        Ok(result) => {
            // compute the execution-time
            let execution_time = compute_time_millis(Local::now() - t0);
            // process the result
            let limit = state.interpreter.get("__COLUMNS__").await.map(|v| v.unwrap_or(Undefined).to_usize());
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
    Ok(state)
}

pub fn is_incomplete(code: &str) -> bool {
    let mut parens = 0;
    let mut braces = 0;
    let mut in_string = false;
    let mut prev_char = '\0';
    for c in code.chars() {
        match c {
            '"' if prev_char != '\\' => in_string = !in_string,
            '(' if !in_string => parens += 1,
            ')' if !in_string => parens -= 1,
            '{' if !in_string => braces += 1,
            '}' if !in_string => braces -= 1,
            _ => {}
        }
        prev_char = c;
    }

    parens > 0 || braces > 0 || in_string
}

pub fn limit_width(lines: Vec<String>, limit: usize) -> Vec<String> {
    lines
        .into_iter()
        .map(|s| {
            if s.chars().count() > limit {
                s.chars().take(limit).collect()
            } else {
                s
            }
        })
        .collect()
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
            .with_variable("__COLUMNS__", Number(I64Value(width as i64))).await.unwrap();
        state.interpreter
            .with_variable("__HEIGHT__", Number(I64Value(height as i64))).await.unwrap();
    }
    state
}

pub fn show_title() {
    use crate::packages::VERSION;
    println!("Welcome to Oxide v{VERSION}\n");
}

/// Unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::numbers::Numbers::F64Value;
    use crate::packages::webservers;
    use crate::test_util::{make_quote_parameters, start_test_server_async};
    use std::fs;
    use std::fs::File;

    #[actix::test]
    async fn test_build_output_struct() {
        let port = start_test_server_async().await.unwrap();
        let mut state = TerminalState::connect("localhost", port, "/ws").await.unwrap();
        let result = state.interpreter.evaluate(r#"
            { "symbol": "LEET", "exchange": "GAME", "last_sale": 59.99 }
        "#).await.unwrap();

        let lines = build_output(12, result, 13.2).unwrap();
        assert_eq!(lines, vec![
            "12: returned type \u{1b}[7m`Struct(String(4), String(4), f64)` in 13.2 ms\u{1b}[0m",
            "{",
            "  \"exchange\": \"GAME\",",
            "  \"last_sale\": 59.99,",
            "  \"symbol\": \"LEET\"",
            "}"
        ]);
        webservers::stop_server(port).await.unwrap();
    }

    #[actix::test]
    async fn test_build_output_table() {
        let port = start_test_server_async().await.unwrap();
        let mut state = TerminalState::connect("localhost", port, "/ws").await.unwrap();
        let result = state.interpreter.evaluate(r#"
            oxide::help()::describe()
        "#).await.unwrap();

        let lines = build_output(12, result, 13.2).unwrap();
        assert_eq!(lines, vec![
            "12: \u{1b}[7m\u{1b}[7m5 row(s) in 13.2 ms\u{1b}[0m ~ Table(String(128), String(128), String(128), Boolean)\u{1b}[0m",
            "|-------------------------------------------------------------|",
            "| id | name        | type       | default_value | is_nullable |",
            "|-------------------------------------------------------------|",
            "| 0  | name        | String(20) | null          | true        |",
            "| 1  | module      | String(20) | null          | true        |",
            "| 2  | signature   | String(32) | null          | true        |",
            "| 3  | description | String(60) | null          | true        |",
            "| 4  | returns     | String(32) | null          | true        |",
            "|-------------------------------------------------------------|"]);
        webservers::stop_server(port).await.unwrap();
    }

    #[actix::test]
    async fn test_build_output_test() {
        let port = start_test_server_async().await.unwrap();
        let mut state = TerminalState::connect("localhost", port, "/ws").await.unwrap();
        let result = state.interpreter.evaluate(r#"
            feature "JSON tests" {
                scenario "Compare JSON contents (in sequence)" {
                    assert { first: "Tom" last: "Lane" } matches { first: "Tom" last: "Lane" }
                }
                scenario "Compare JSON contents (out of sequence)" {
                    assert { scores: [82 78 99], id: "A1537" } matches { id: "A1537", scores: [82 78 99] }
                }
            }
            test
        "#).await.unwrap();

        let lines = build_output(12, result, 13.2).unwrap();
        assert_eq!(lines, vec![
            "12: \u{1b}[7m\u{1b}[7m5 row(s) in 13.2 ms\u{1b}[0m ~ Table(i64, i64, String(256), i64, i64)\u{1b}[0m",
            "📊 Test Suite summary:",
            "────────────────────────────────────────────────────────────────────────────────────────",
            "✅ 2 passed | ❌ 0 failed",
            "────────────────────────────────────────────────────────────────────────────────────────",
            "👍 All tests passed. No issues found.",
            "",
            r#"┌───────────────────────────────────────────────────────────────────────────────────────"#,
            r#"│🟩 JSON tests"#,
            r#"├───────────────────────────────────────────────────────────────────────────────────────"#,
            r#"│	🟢 Compare JSON contents (in sequence)"#,
            r#"│		✅ assert {first: "Tom", last: "Lane"} matches {first: "Tom", last: "Lane"}"#,
            r#"│	🟢 Compare JSON contents (out of sequence)"#,
            r#"│		✅ assert {scores: [82, 78, 99], id: "A1537"} matches {id: "A1537", scores: [82, 78, 99]}"#,
            r#"└───────────────────────────────────────────────────────────────────────────────────────"#]);
        webservers::stop_server(port).await.unwrap();
    }

    #[actix::test]
    async fn test_build_output_testreport() {
        let port = start_test_server_async().await.unwrap();
        let mut state = TerminalState::connect("localhost", port, "/ws").await.unwrap();
        let result = state.interpreter.evaluate(r#"
            feature "JSON tests" {
                scenario "Compare JSON contents (in sequence)" {
                    assert { first: "Tom" last: "Lane" } matches { first: "Tom" last: "Lane" }
                }
                scenario "Compare JSON contents (out of sequence)" {
                    assert { scores: [82 78 99], id: "A1537" } matches { id: "A1537", scores: [82 78 99] }
                }
            }
            test report
        "#).await.unwrap();

        let lines = build_output(12, result, 13.2).unwrap();
        assert_eq!(lines, vec![
            "12: returned type \u{1b}[7m`String(534)` in 13.2 ms\u{1b}[0m",
            r#"┌───────────────────────────────────────────────────────────────────────────────────────"#,
            r#"│🟩 JSON tests"#,
            r#"├───────────────────────────────────────────────────────────────────────────────────────"#,
            r#"│	🟢 Compare JSON contents (in sequence)"#,
            r#"│		✅ assert {first: "Tom", last: "Lane"} matches {first: "Tom", last: "Lane"}"#,
            r#"│	🟢 Compare JSON contents (out of sequence)"#,
            r#"│		✅ assert {scores: [82, 78, 99], id: "A1537"} matches {id: "A1537", scores: [82, 78, 99]}"#,
            r#"└───────────────────────────────────────────────────────────────────────────────────────"#]);
        webservers::stop_server(port).await.unwrap();
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
    fn test_build_output_header_string() {
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
            "Hello World"
        "#).unwrap();

        let lines = build_output_header(12, &result, 0.1).unwrap();
        assert_eq!(
            lines,
            "12: returned type \u{1b}[7m`String(11)` in 0.1 ms\u{1b}[0m")
    }

    #[test]
    fn test_build_output_header_table() {
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
            oxide::help()::describe()
        "#).unwrap();

        let lines = build_output_header(12, &result, 13.2).unwrap();
        assert_eq!(
            lines,
            "12: \u{1b}[7m\u{1b}[7m5 row(s) in 13.2 ms\u{1b}[0m ~ Table(String(128), String(128), String(128), Boolean)\u{1b}[0m")
    }

    #[test]
    fn test_get_hard_type() {
        let text = get_hard_type(&HardStructure::new(
            make_quote_parameters(),
            vec![
                StringValue("ABC".into()),
                StringValue("NYSE".into()),
                Number(F64Value(11.77))
            ]));
        assert_eq!(text, "Struct(String(8), String(8), f64)");
    }

    #[test]
    fn test_get_parameter_string() {
        let text = get_parameter_string(&make_quote_parameters());
        assert_eq!(text, "String(8), String(8), f64");
    }

    #[test]
    fn test_get_soft_type() {
        let text = get_soft_type(&SoftStructure::new(&vec![
            ("symbol", StringValue("ABC".into())),
            ("exchange", StringValue("NYSE".into())),
            ("last_sale", Number(F64Value(11.77)))
        ]));
        assert_eq!(text, "Struct(String(3), String(4), f64)");
    }

    #[actix::test]
    async fn test_handle_input() {
        let state: TerminalState = TerminalState::offline().unwrap();
        let new_state = handle_input(state, "2 * 5").await.unwrap();
        assert_eq!(new_state.counter, 1);
    }

    #[test]
    fn test_is_alive() {
        let mut state: TerminalState = TerminalState::offline().unwrap();
        assert_eq!(state.is_alive(), true);

        state.die();
        assert_eq!(state.is_alive(), false);
    }

    #[test]
    fn test_is_incomplete_false() {
        assert_eq!(is_incomplete("x = (4 + 7) * 3"), false);
    }

    #[test]
    fn test_is_incomplete_true() {
        assert_eq!(is_incomplete("x = (4 + 7"), true);
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

    #[actix::test]
    async fn test_setup_system_variables() {
        let state = TerminalState::offline().unwrap();
        let mut state = setup_system_variables(state, vec![]).await;
        assert!(matches!(state.interpreter.get("__ARGS__").await.unwrap(), Some(ArrayValue(..))));
        assert!(matches!(state.interpreter.get("__COLUMNS__").await.unwrap(), Some(Number(..))));
        assert!(matches!(state.interpreter.get("__HEIGHT__").await.unwrap(), Some(Number(..))));
    }

    #[test]
    fn test_show_title() {
        show_title()
    }
}