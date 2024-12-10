////////////////////////////////////////////////////////////////////
// shared libraries
////////////////////////////////////////////////////////////////////

use std::cmp::max;

use chrono::TimeDelta;
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[macro_export]
macro_rules! cnv_error {
    ($e:expr) => {
        std::io::Error::new(std::io::ErrorKind::Other, $e)
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct RemoteCallRequest {
    code: String,
}

impl RemoteCallRequest {
    pub fn new(code: String) -> Self {
        RemoteCallRequest { code }
    }

    pub fn get_code(&self) -> &String { &self.code }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemoteCallResponse {
    result: Value,
    message: Option<String>,
}

impl RemoteCallResponse {
    pub fn from_string(json_string: &str) -> std::io::Result<RemoteCallResponse> {
        serde_json::from_str(json_string).map_err(|e| cnv_error!(e))
    }

    pub fn fail(message: String) -> Self {
        RemoteCallResponse {
            result: Value::Null,
            message: Some(message),
        }
    }

    pub fn success(value: Value) -> Self {
        RemoteCallResponse {
            result: value,
            message: None,
        }
    }

    pub fn get_message(&self) -> Option<String> { self.message.to_owned() }

    pub fn get_result(&self) -> Value { self.result.to_owned() }
}

pub fn compute_time_millis(dt: TimeDelta) -> f64 {
    match dt.num_nanoseconds() {
        Some(nano) => nano.to_f64().map(|t| t / 1e+6).unwrap_or(0.),
        None => dt.num_milliseconds().to_f64().unwrap_or(0.)
    }
}

/// Transforms the cells into a textual table
pub fn tabulate_cells(
    header_cells: Vec<Vec<String>>,
    body_cells: Vec<Vec<String>>,
) -> Vec<String> {
    // create a combined vector of cells (header and body)
    let mut cells = Vec::new();
    cells.extend(header_cells.to_owned());
    cells.extend(body_cells.to_owned());

    // compute the width for each cell
    let cell_widths = cells.iter().map(|cell| {
        cell.iter().map(|s| s.len()).collect::<Vec<usize>>()
    }).collect::<Vec<Vec<usize>>>();

    // compute the width for each column
    let column_widths = cell_widths.iter()
        .fold(cell_widths[0].to_owned(), |a, b| tabulate_maximums(&a, b));

    // produce formatted lines
    tabulate_table(header_cells, body_cells, column_widths)
}

pub fn tabulate_table(
    header_cells: Vec<Vec<String>>,
    body_cells: Vec<Vec<String>>,
    column_widths: Vec<usize>,
) -> Vec<String> {
    // create the separator
    let total_width = match column_widths.iter().sum::<usize>() + column_widths.len() {
        n if n > 0 => n - 1,
        n => n
    };
    let separator = format!("|{}|", "-".repeat(total_width));

    // produce formatted lines
    let mut lines = Vec::new();
    lines.push(separator.to_owned());
    lines.extend(tabulate_lines(&header_cells, &column_widths));
    lines.push(separator.to_owned());
    lines.extend(tabulate_lines(&body_cells, &column_widths));
    lines.push(separator);
    lines
}

pub fn tabulate_cell(s: &String, width: usize) -> String {
    let mut t = s.to_string();
    while t.len() < width { t += " " }
    t
}

pub fn tabulate_lines(cells: &Vec<Vec<String>>, column_widths: &Vec<usize>) -> Vec<String> {
    let mut lines = Vec::new();
    for cell in cells {
        let sheet = cell.iter().zip(column_widths.iter())
            .map(|(s, u)| tabulate_cell(s, *u))
            .collect::<Vec<String>>();
        lines.push(format!("|{}|", sheet.join("|")));
    }
    lines
}

pub fn tabulate_maximums(a: &Vec<usize>, b: &Vec<usize>) -> Vec<usize> {
    let mut c = Vec::new();
    for i in 0..a.len() { c.push(max(a[i], b[i])) }
    c
}

pub fn fail<A>(message: impl Into<String>) -> std::io::Result<A> {
    Err(std::io::Error::new(std::io::ErrorKind::Other, message.into()))
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

pub fn ns_uri(database: &str, schema: &str, name: &str) -> String {
    format!("/{}/{}/{}", database, schema, name)
}

pub fn range_uri(database: &str, schema: &str, name: &str, a: usize, b: usize) -> String {
    format!("/{}/{}/{}/{}/{}", database, schema, name, a, b)
}

pub fn row_uri(database: &str, schema: &str, name: &str, id: usize) -> String {
    format!("/{}/{}/{}/{}", database, schema, name, id)
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;

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
}