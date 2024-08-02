////////////////////////////////////////////////////////////////////
// REPL module
////////////////////////////////////////////////////////////////////

use std::io::{BufRead, BufReader, Read, stdout, Write};
use std::sync::{Arc, Mutex};

use chrono::Local;
use crossterm::execute;
use crossterm::style::{Print, ResetColor};
use crossterm::terminal::{Clear, ClearType};
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};

use shared_lib::{cnv_error, RemoteCallRequest, RemoteCallResponse, RowJs};

use crate::fields::Field;
use crate::interpreter::Interpreter;
use crate::model_row_collection::ModelRowCollection;
use crate::row_collection::RowCollection;
use crate::rows::Row;
use crate::server::ColumnJs;
use crate::table_columns::TableColumn;
use crate::table_renderer::TableRenderer;
use crate::table_writer::TableWriter;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::ErrorValue;

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
    interpreter: Interpreter,
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
            interpreter: Interpreter::new(),
            counter: 0,
            is_alive: true,
            connection: REPLConnection::LocalConnection,
        }
    }

    /// instructs the REPL to quit after the current statement has been processed
    pub fn die(&mut self) {
        self.is_alive = false
    }

    /// creates a new history table
    fn create_history_table() -> std::io::Result<ModelRowCollection> {
        Ok(ModelRowCollection::new(
            TableColumn::from_columns(&vec![
                ColumnJs::new("pid", "i64", None),
                ColumnJs::new("input", "String(65536)", None),
            ])?, vec![],
        ))
    }

    /// return the REPL input history
    pub fn get_history(&mut self) -> Vec<String> {
        let mut listing = Vec::new();
        if let Ok(TypedValue::TableValue(mut mrc)) = self.interpreter.evaluate(HISTORY_TABLE_NAME) {
            for row in &mrc.get_rows() {
                let input = row.get("input");
                listing.push(format!("[{}] {}", &row.get_id(), input.unwrap_value()));
            }
        }
        listing
    }

    /// stores the user input to history
    pub fn put_history(&mut self, input: &str) -> std::io::Result<()> {
        // get or create the history table
        let mut mrc = match self.interpreter.evaluate(HISTORY_TABLE_NAME) {
            Ok(TypedValue::TableValue(mrc)) => mrc,
            _ => Self::create_history_table()?
        };
        // capture the row ID and columns
        let id = mrc.len()?;
        let columns = mrc.get_columns().clone();
        // cleanup the user input
        let clean_input = input.trim().split('\n').map(|s| s.trim())
            .collect::<Vec<&str>>().join("; ");
        // create a new row
        let row = Row::new(id, columns, vec![
            Field::new(TypedValue::RowsAffected(id)),
            Field::new(TypedValue::StringValue(clean_input)),
        ]);
        // write the row
        mrc.overwrite_row(id, row)?;
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
        let input = read_lines()?;
        if input.trim() == "q!" {
            return Ok(());
        }
        let pid = state.counter;
        let t0 = Local::now();
        let result = process_statement(&mut state, input.as_str())
            .await
            .unwrap_or_else(|err| TypedValue::StringValue(err.to_string()));
        let t1 = Local::now();
        let t2 = t1 - t0;

        // write the outcome result
        let execution_time = match t2.num_nanoseconds() {
            Some(nano) => nano.to_f64().map(|t| t / 1e+6).unwrap_or(0.),
            None => t2.num_milliseconds().to_f64().unwrap_or(0.)
        };
        let type_name = result.get_type_name();
        let extras = match &result {
            TypedValue::TableValue(mrc) => format!(" ~ {} row(s)", &mrc.len()?),
            _ => "".to_string()
        };
        stdout.write(format!("[{}] {}{} in {:.1} millis\n", pid, type_name, extras, execution_time).as_bytes())?;

        // show the output
        match result {
            TypedValue::TableValue(mrc) => {
                let lines =
                    TableRenderer::from_collection(Box::new(mrc.clone()));
                for line in lines { stdout.write((line + "\n").as_bytes())?; }
            }
            z => {
                stdout.write((z.unwrap_value() + "\n").as_bytes())?;
            }
        };
        stdout.flush()?;
    }
    Ok(())
}

fn read_lines() -> std::io::Result<String> {
    let reader = Arc::new(Mutex::new(BufReader::new(std::io::stdin())));
    let mut reader = reader.lock().unwrap();
    let mut sb = String::new();
    let mut done = false;
    while !done {
        let mut line = String::new();
        match reader.read_line(&mut line)? {
            n if n <= 1 => done = true, // EOF reached
            _ => if !line.trim().is_empty() { sb.push_str(line.as_str()) }
        }
    }
    Ok(sb.to_string())
}

/// Processes user input against a local Oxide instance or a remote Oxide peer
pub async fn process_statement(state: &mut REPLState, user_input: &str) -> std::io::Result<TypedValue> {
    state.put_history(user_input)?;
    match &state.connection {
        REPLConnection::LocalConnection =>
            state.interpreter.evaluate(user_input),
        REPLConnection::RemoteConnection { host, port } => {
            let body = serde_json::to_string(&RemoteCallRequest::new(user_input.to_string()))?;
            let response = reqwest::Client::new()
                .post(format!("http://{}:{}/rpc", host, port))
                .body(body)
                .header("Content-type", "application/json")
                .send()
                .await.map_err(|e| cnv_error!(e))?;
            let response_body = response.text().await.map_err(|e| cnv_error!(e))?;
            let outcome = RemoteCallResponse::from_string(response_body.as_str())?;
            if let Some(message) = outcome.get_message() {
                Ok(ErrorValue(message))
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
        s => s.to_string()
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

    #[test]
    fn test_get_prompt() {
        let r: REPLState = REPLState::new();
        assert_eq!(r.get_prompt(), "oxide.public[0]> ");
        assert_eq!(r, REPLState {
            interpreter: Interpreter::new(),
            database: "oxide".into(),
            schema: "public".into(),
            counter: 0,
            is_alive: true,
            connection: REPLConnection::LocalConnection,
        })
    }

    #[test]
    fn test_get_put_history() {
        let mut r: REPLState = REPLState::new();
        r.put_history("abc".into()).unwrap();
        r.put_history("123".into()).unwrap();
        r.put_history("iii".into()).unwrap();

        let h = r.get_history();
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