////////////////////////////////////////////////////////////////////
//      Oxide REPL v0.1.0
////////////////////////////////////////////////////////////////////

use std::{io, thread};
use std::io::{stdout, Write};
use std::sync::mpsc;
use std::time::Duration;

use crossterm::{
    execute,
    style::{Print, ResetColor},
    terminal::{Clear, ClearType},
};
use crossterm::event::{Event, KeyCode, KeyEvent, KeyEventKind, KeyEventState, KeyModifiers, poll, read};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
use tokio::runtime::Builder;

use crate::peers::{RemoteCallRequest, RemoteCallResponse};
use crate::repl::REPLState;

mod peers;
mod repl;

fn main() -> io::Result<()> {
    let mut state: REPLState = REPLState::new();
    println!("Welcome to Oxide REPL. Enter \"q!\" to quit.\n");
    print!("{}", state.get_prompt());

    enable_raw_mode().unwrap();
    let mut stdout = stdout();
    stdout.flush().unwrap();

    // channel for communication between input handling thread and main thread
    let (tx, rx) = mpsc::channel();

    // spawn a separate thread to handle input events
    thread::spawn(move || loop {
        if poll(Duration::from_millis(50)).unwrap() {
            if let Event::Key(KeyEvent { code, modifiers, kind, state }) = read().unwrap() {
                tx.send((code, modifiers, kind, state)).unwrap();
            }
        }
    });

    // main event loop
    while state.is_alive() {
        if let Ok((code, _modifiers, _kind, _state)) = rx.recv() {
            process_keyboard_event(&mut state, code, _modifiers, _kind, _state)?;
            stdout.flush().unwrap();
        }
    }
    Ok(())
}

fn process_keyboard_event(state: &mut REPLState,
                          code: KeyCode,
                          _modifiers: KeyModifiers,
                          _kind: KeyEventKind,
                          _state: KeyEventState) -> io::Result<()> {
    match code {
        KeyCode::Backspace => {
            if !state.chars.is_empty() {
                state.chars.pop();
                print!("{}", "\u{0008}")
            }
        }
        KeyCode::Char(c) => {
            state.chars.push(c);
            print!("{}", c)
        }
        KeyCode::Enter if _modifiers.contains(KeyModifiers::SHIFT) => {
            state.chars.push('\n');
            print!("\r\n")
        }
        KeyCode::Enter => {
            print!("\r\n");
            disable_raw_mode().unwrap();
            let input: String = state.chars.iter().collect();
            let response = process_user_input(state, input).unwrap_or_else(|err| Some(err.to_string()));
            if let Some(message) = response { say(message.as_str())?; }
            print!("{}", state.get_prompt());
            enable_raw_mode().unwrap();
            state.chars.clear();
        }
        KeyCode::Down => {
            print!("\r<D>")
        }
        KeyCode::Left => {
            print!("\r<L>")
        }
        KeyCode::Right => {
            print!("\r<R>")
        }
        KeyCode::Up => {
            print!("\r<U>")
        }
        _ => {}
    }
    Ok(())
}

fn process_user_input(state: &mut REPLState, input: String) -> io::Result<Option<String>> {
    match input.trim() {
        "" => Ok(None),
        "history" => Ok(Some(state.get_history().join("\n"))),
        "q!" => {
            state.die();
            Ok(None)
        }
        cmd => {
            state.put_history(cmd);
            let response = process_statement(cmd.into())?;
            let outcome = response.get_message().unwrap_or(response.get_result().to_string());
            Ok(Some(outcome))
        }
    }
}

fn process_statement(user_input: String) -> io::Result<RemoteCallResponse> {
    let body = serde_json::to_string(&RemoteCallRequest::new(user_input))?;
    let response = reqwest::Client::new()
        .post(format!("http://{}:{}/rpc", "0.0.0.0", 8080))
        .body(body)
        .header("Content-type", "application/json")
        .send();
    let rt = Builder::new_current_thread().enable_time().enable_io().build()?;
    let response = rt.block_on(response).map_err(|e| cnv_error!(e))?;
    let response_body = rt.block_on(response.text()).map_err(|e| cnv_error!(e))?;
    RemoteCallResponse::from_string(response_body.as_str())
}

// prints messages to STDOUT
fn say(message: &str) -> io::Result<()> {
    execute!(
        stdout(),
        Clear(ClearType::CurrentLine),
        Print(format!("{}\n", message)),
        ResetColor
    );
    Ok(())
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_input() {
        let response = process_statement("history".into()).unwrap();
        println!("{:?}", response)
    }

    #[test]
    fn test_say() {
        say("Hello").unwrap()
    }
}