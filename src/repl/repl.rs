////////////////////////////////////////////////////////////////////
// REPL module
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

#[macro_export]
macro_rules! cnv_error {
    ($e:expr) => {
        io::Error::new(io::ErrorKind::Other, $e)
    };
}

/// REPL application state
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct REPLState {
    database: String,
    schema: String,
    history: Vec<String>,
    pub(crate) chars: Vec<char>,
    counter: usize,
    is_alive: bool,
}

impl REPLState {
    /// default constructor
    pub fn new() -> REPLState {
        REPLState {
            database: "oxide".into(),
            schema: "public".into(),
            history: vec![],
            chars: vec![],
            counter: 1,
            is_alive: true,
        }
    }

    /// instructs the REPL to quit after the current statement has been processed
    pub fn die(&mut self) {
        self.is_alive = false
    }

    /// return the REPL input history
    pub fn get_history(&self) -> Vec<String> {
        let mut listing = Vec::new();
        let mut n: usize = 1;
        for hist in &self.history {
            listing.push(format!("{n} {hist}"));
            n += 1;
        }
        listing
    }

    /// return the REPL prompt string (e.g. "oxide.public[4]>")
    pub fn get_prompt(&self) -> String {
        format!("{}.{}[{}]> ", self.database, self.schema, self.counter)
    }

    pub fn is_alive(&self) -> bool {
        self.is_alive
    }

    /// stores the user input to history
    pub fn put_history(&mut self, input: &str) {
        self.history.push(input.into());
        self.counter += 1;
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::repl::REPLState;

    #[test]
    fn test_get_prompt() {
        let r: REPLState = REPLState::new();
        assert_eq!(r.get_prompt(), "oxide.public[1]> ")
    }

    #[test]
    fn test_get_put_history() {
        let mut r: REPLState = REPLState::new();
        r.put_history("abc".into());
        r.put_history("123".into());
        r.put_history("iii".into());

        let h = r.get_history();
        assert_eq!(h, vec!["1 abc", "2 123", "3 iii"])
    }

    #[test]
    fn test_lifecycle() {
        let mut r: REPLState = REPLState::new();
        assert_eq!(r.is_alive(), true);

        r.die();
        assert_eq!(r.is_alive(), false);
    }
}