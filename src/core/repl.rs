////////////////////////////////////////////////////////////////////
// REPL module
////////////////////////////////////////////////////////////////////

use std::io::{stdout, Write};

use chrono::Local;
use crossterm::style::{Print, ResetColor};
use crossterm::terminal::{Clear, ClearType};
use crossterm::execute;
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};

use crate::errors::Errors::Exact;
use shared_lib::{cnv_error, RemoteCallRequest, RemoteCallResponse};

use crate::expression::ACK;
use crate::interpreter::Interpreter;
use crate::model_row_collection::ModelRowCollection;
use crate::outcomes::Outcomes;
use crate::parameter::Parameter;
use crate::row_collection::RowCollection;
use crate::rows::{Row, RowJs};
use crate::table_columns::Column;
use crate::table_renderer::TableRenderer;
use crate::table_writer::TableWriter;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{ErrorValue, Function, TableValue};

pub const HISTORY_TABLE_NAME: &str = "history";

/// REPL connection type
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum REPLConnection {
    LocalConnection,
    RemoteConnection {
        host: String,
        port: u32,
    },
}

/// REPL application state
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct REPLState {
    database: String,
    schema: String,
    pub interpreter: Interpreter,
    counter: usize,
    is_alive: bool,
    connection: REPLConnection,
}

impl REPLState {
    /// Connect to remote peer
    pub fn connect(host: String, port: u32) -> REPLState {
        REPLState {
            database: "oxide".into(),
            schema: "public".into(),
            interpreter: Interpreter::new(),
            counter: 0,
            is_alive: true,
            connection: REPLConnection::RemoteConnection { host, port },
        }
    }

    /// default constructor
    pub fn new() -> REPLState {
        REPLState {
            database: "oxide".into(),
            schema: "public".into(),
            interpreter: Self::attach_builtin_functions(Interpreter::new()),
            counter: 0,
            is_alive: true,
            connection: REPLConnection::LocalConnection,
        }
    }

    pub fn attach_builtin_functions(mut interpreter: Interpreter) -> Interpreter {
        interpreter.with_variable("assert", Function {
            params: vec![
                Parameter::new("condition", Some("Boolean".into()), None)
            ],
            code: Box::new(ACK),
        });
        interpreter
    }

    /// instructs the REPL to quit after the current statement has been processed
    pub fn die(&mut self) {
        self.is_alive = false
    }

    /// creates a new history table
    fn create_history_table() -> std::io::Result<ModelRowCollection> {
        Ok(ModelRowCollection::with_rows(
            Column::from_parameters(&vec![
                Parameter::new("pid", Some("i64".into()), None),
                Parameter::new("input", Some("String(65536)".into()), None),
            ])?, Vec::new(),
        ))
    }

    /// return the REPL input history
    pub async fn get_history(&mut self) -> Vec<String> {
        let mut listing = Vec::new();
        let outcome = self.interpreter
            .evaluate_async(HISTORY_TABLE_NAME).await;
        if let Ok(TypedValue::TableValue(mrc)) = outcome {
            for row in mrc.get_rows() {
                listing.push(format!("[{}] {}",
                                     &row.get_id(), row.get(1).unwrap_value()));
            }
        }
        listing
    }

    /// stores the user input to history
    pub async fn put_history(&mut self, input: &str) -> std::io::Result<()> {
        // get or create the history table
        let mut mrc = match self.interpreter.evaluate_async(HISTORY_TABLE_NAME).await {
            Ok(TypedValue::TableValue(mrc)) => mrc,
            _ => Self::create_history_table()?
        };
        // cleanup the user input
        let clean_input = input.trim().split('\n').map(|s| s.trim())
            .collect::<Vec<&str>>().join("; ");
        // create a new row
        let id = mrc.len()?;
        let row = Row::new(id, vec![
            TypedValue::Outcome(Outcomes::RowId(id)),
            TypedValue::StringValue(clean_input),
        ]);
        // write the row
        let _ = mrc.overwrite_row(id, row);
        // replace the history table in memory
        self.interpreter.with_variable(HISTORY_TABLE_NAME, TypedValue::TableValue(mrc));
        self.counter += 1;
        Ok(())
    }

    /// return the REPL prompt string (e.g. "oxide.public[4]>")
    pub fn get_prompt(&self) -> String {
        format!("{}.{}[{}]> ", self.database, self.schema, self.counter)
    }

    pub fn is_alive(&self) -> bool {
        self.is_alive
    }
}

/// Starts the interactive shell
pub async fn run(mut state: REPLState) -> std::io::Result<()> {
    println!("Welcome to Oxide REPL. Enter \"q!\" to quit.\n");
    let mut stdout = stdout();
    while state.is_alive {
        // show the prompt
        stdout.write(state.get_prompt().as_bytes())?;
        stdout.flush()?;

        // read and process the input - capturing the processing time
        if let Some(input) = read_line()? {
            match input.trim() {
                "" => {}
                "q!" => return Ok(()),
                input => {
                    let pid = state.counter;
                    let t0 = Local::now();
                    let result = process_statement(&mut state, input)
                        .await
                        .unwrap_or_else(|err| ErrorValue(Exact(err.to_string())));
                    let t1 = Local::now();
                    let t2 = t1 - t0;

                    // write the outcome result
                    let execution_time = match t2.num_nanoseconds() {
                        Some(nano) => nano.to_f64().map(|t| t / 1e+6).unwrap_or(0.),
                        None => t2.num_milliseconds().to_f64().unwrap_or(0.)
                    };
                    let type_name = result.get_type_name();
                    let extras = match &result {
                        TableValue(mrc) => format!(" ~ {} row(s)", &mrc.len()?),
                        _ => "".to_string()
                    };
                    stdout.write(format!("[{}] {}{} in {:.1} millis\n", pid, type_name, extras, execution_time).as_bytes())?;

                    // show the output
                    match result {
                        TableValue(mrc) => {
                            let rc: Box<dyn RowCollection> = Box::from(mrc);
                            let raw_lines = TableRenderer::from_table_with_ids(&rc)?;
                            let lines = state.interpreter.machine
                                .get("__COLUMNS__").map(|limit_v| {
                                let limit = limit_v.to_usize();
                                raw_lines.iter()
                                    .map(|s| s[0..limit].to_string())
                                    .collect::<Vec<_>>()
                            })
                                .unwrap_or(raw_lines);
                            for line in lines { stdout.write((line + "\n").as_bytes())?; }
                        }
                        z => {
                            stdout.write((z.unwrap_value() + "\n").as_bytes())?;
                        }
                    };
                    stdout.flush()?;
                }
            }
        }
    }
    Ok(())
}

fn read_line() -> std::io::Result<Option<String>> {
    use crossterm::event::{self, Event, KeyCode};
    use std::io::Write;
    let mut buffer = String::new();
    let mut lines = Vec::new();
    let mut last_char = '\n';

    fn chars_available() -> bool {
        event::poll(std::time::Duration::from_millis(250)).unwrap_or_else(|_| false)
    }

    loop {
        stdout().flush()?;
        if chars_available() {
            if let Event::Key(key_event) = event::read()? {
                match key_event.code {
                    KeyCode::Enter =>
                        match last_char {
                            '\\' => {
                                buffer.pop();
                                lines.push(buffer.to_string());
                                buffer.clear();
                                last_char = '\n';
                            }
                            _ => {
                                lines.push(buffer.to_string());
                                break;
                            }
                        }
                    KeyCode::Char(c) => {
                        buffer.push(c);
                        last_char = c;
                    }
                    KeyCode::Esc => break,
                    _ => {}
                }
            }
        } else if !buffer.is_empty() { break; }
    }
    Ok(if lines.is_empty() { None } else { Some(lines.join("\n")) })
}

/// Processes user input against a local Oxide instance or a remote Oxide peer
pub async fn process_statement(state: &mut REPLState, user_input: &str) -> std::io::Result<TypedValue> {
    state.put_history(user_input).await?;
    match &state.connection {
        REPLConnection::LocalConnection =>
            state.interpreter.evaluate_async(user_input).await,
        REPLConnection::RemoteConnection { host, port } => {
            let body = serde_json::to_string(&RemoteCallRequest::new(user_input.to_string()))?;
            let response = reqwest::Client::new()
                .post(format!("http://{}:{}/rpc", host, port))
                .body(body)
                .header("Content-Type", "application/json")
                .send()
                .await.map_err(|e| cnv_error!(e))?;
            let response_body = response.text().await.map_err(|e| cnv_error!(e))?;
            let outcome = RemoteCallResponse::from_string(response_body.as_str())?;
            if let Some(message) = outcome.get_message() {
                Ok(ErrorValue(Exact(message)))
            } else {
                Ok(TypedValue::from_json(outcome.get_result()))
            }
        }
    }
}

// prints messages to STDOUT
fn say(message: &str) -> std::io::Result<()> {
    let lines = match message {
        // is it JSON array?
        s if s.starts_with("[") => {
            let rows = RowJs::vec_from_string(s)?;
            TableWriter::from_rows(&rows).join("\n")
        }
        // is it JSON object?
        s if s.starts_with("{") => RowJs::from_string(s)?.to_json_string(),
        s => String::from(s)
    };
    execute!(
        stdout(),
        Clear(ClearType::CurrentLine),
        Print(format!("{}\n", lines)),
        ResetColor
    )?;
    Ok(())
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::repl::REPLState;

    use super::*;

    #[test]
    fn test_say() {
        say("Hello").unwrap()
    }

    #[actix::test]
    async fn test_get_put_history() {
        let mut r: REPLState = REPLState::new();
        r.put_history("abc".into()).await.unwrap();
        r.put_history("123".into()).await.unwrap();
        r.put_history("iii".into()).await.unwrap();

        let h = r.get_history().await;
        assert_eq!(h, vec!["[0] abc", "[1] 123", "[2] iii"])
    }

    #[test]
    fn test_lifecycle() {
        let mut r: REPLState = REPLState::new();
        assert_eq!(r.is_alive(), true);

        r.die();
        assert_eq!(r.is_alive(), false);
    }
}