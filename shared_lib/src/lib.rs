////////////////////////////////////////////////////////////////////
// shared libraries
////////////////////////////////////////////////////////////////////

use std::cmp::max;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[macro_export]
macro_rules! cnv_error {
    ($e:expr) => {
        std::io::Error::new(std::io::ErrorKind::Other, $e)
    }
}

// JSON representation of a field
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FieldJs {
    name: String,
    value: Value,
}

impl FieldJs {
    pub fn new(name: &str, value: Value) -> Self {
        Self { name: name.into(), value }
    }

    pub fn get_name(&self) -> &String {
        &self.name
    }

    pub fn get_value(&self) -> Value {
        self.value.clone()
    }
}

// JSON representation of a row
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RowJs {
    pub id: Option<usize>,
    pub fields: Vec<FieldJs>,
}

impl RowJs {
    pub fn from_string(json_string: &str) -> std::io::Result<Self> {
        serde_json::from_str(json_string).map_err(|e| cnv_error!(e))
    }

    pub fn vec_from_string(json_string: &str) -> std::io::Result<Vec<Self>> {
        serde_json::from_str(json_string).map_err(|e| cnv_error!(e))
    }

    pub fn new(id: Option<usize>, fields: Vec<FieldJs>) -> Self { Self { id, fields } }

    pub fn get_id(&self) -> Option<usize> { self.id }

    pub fn get_fields(&self) -> &Vec<FieldJs> { &self.fields }

    pub fn to_json_string(&self) -> String { serde_json::to_string(self).unwrap() }
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

    pub fn get_message(&self) -> Option<String> { self.message.clone() }

    pub fn get_result(&self) -> Value { self.result.clone() }
}

/// Transforms the cells into a textual table
pub fn tabulate_cells(
    header_cells: Vec<Vec<String>>,
    body_cells: Vec<Vec<String>>,
) -> Vec<String> {
    // create a combined vector of cells (header and body)
    let mut cells = vec![];
    cells.extend(header_cells.clone());
    cells.extend(body_cells.clone());

    // compute the width for each cell
    let cell_widths = cells.iter().map(|cell| {
        cell.iter().map(|s| s.len()).collect::<Vec<usize>>()
    }).collect::<Vec<Vec<usize>>>();

    // compute the width for each column
    let column_widths = cell_widths.iter()
        .fold(cell_widths[0].clone(), |a, b| tabulate_maximums(&a, b));

    // produce formatted lines
    tabulate_table(header_cells, body_cells, column_widths)
}

pub fn tabulate_body_cells_from_rows(rows: &Vec<RowJs>) -> Vec<Vec<String>> {
    let mut body_cells = vec![];
    for row in rows {
        let mut row_vec = vec![];
        for field in row.get_fields() {
            row_vec.push(format!(" {} ", field.get_value()))
        }
        body_cells.push(row_vec)
    }
    body_cells
}

pub fn tabulate_header_cells(rows: &Vec<RowJs>) -> Vec<Vec<String>> {
    let mut headers = vec![];
    for field in rows[0].get_fields() {
        headers.push(format!(" {} ", field.get_name()))
    }

    let mut header_cells = vec![];
    header_cells.push(headers);
    header_cells
}

pub fn tabulate_table(
    header_cells: Vec<Vec<String>>,
    body_cells: Vec<Vec<String>>,
    column_widths: Vec<usize>,
) -> Vec<String> {
    // create the separator
    let total_width = column_widths.iter().sum::<usize>() + column_widths.len() - 1;
    let separator = format!("|{}|", "-".to_string().repeat(total_width));

    // produce formatted lines
    let mut lines = vec![];
    lines.push(separator.clone());
    lines.extend(tabulate_lines(&header_cells, &column_widths));
    lines.push(separator.clone());
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
    let mut lines = vec![];
    for cell in cells {
        let sheet = cell.iter().zip(column_widths.iter())
            .map(|(s, u)| tabulate_cell(s, *u))
            .collect::<Vec<String>>();
        lines.push(format!("|{}|", sheet.join("|")));
    }
    lines
}

pub fn tabulate_maximums(a: &Vec<usize>, b: &Vec<usize>) -> Vec<usize> {
    let mut c = vec![];
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
        [_, port] => ("127.0.0.1".to_string(), port.to_string()),
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
        assert_eq!(get_host_and_port(vec![]).unwrap(),
                   ("127.0.0.1".to_string(), "8080".to_string()));

        assert_eq!(get_host_and_port(vec!["my_app".into(), "3333".into()]).unwrap(),
                   ("127.0.0.1".to_string(), "3333".to_string()));

        assert_eq!(get_host_and_port(vec!["my_app".into(), "0.0.0.0".into(), "9000".into()]).unwrap(),
                   ("0.0.0.0".to_string(), "9000".to_string()));

        assert_eq!(get_host_and_port(vec!["my_app".into(), "127.0.0.1".into(), "3333".into(), "zzz".into()]).unwrap(),
                   ("127.0.0.1".to_string(), "3333".to_string()));
    }

    #[test]
    fn test_tabulate_cells() {
        let rows = RowJs::vec_from_string(r#"
            [{"fields":[{"name":"symbol","value":"ABC"},{"name":"exchange","value":"AMEX"},
            {"name":"last_sale","value":11.77}],"id":0},{"fields":[{"name":"symbol","value":"BIZ"},
            {"name":"exchange","value":"NYSE"},{"name":"last_sale","value":23.66}],"id":2},
            {"fields":[{"name":"symbol","value":"BOOM"},{"name":"exchange","value":"NASDAQ"},
            {"name":"last_sale","value":56.87}],"id":4}]
        "#).unwrap();
        let header_cells = tabulate_header_cells(&rows);
        let body_cells = tabulate_body_cells_from_rows(&rows);
        let lines = tabulate_cells(header_cells, body_cells);
        assert_eq!(lines, vec![
            "|-------------------------------|".to_string(),
            "| symbol | exchange | last_sale |".to_string(),
            "|-------------------------------|".to_string(),
            "|\"ABC\"   |\"AMEX\"    |11.77      |".to_string(),
            "|\"BIZ\"   |\"NYSE\"    |23.66      |".to_string(),
            "|\"BOOM\"  |\"NASDAQ\"  |56.87      |".to_string(),
            "|-------------------------------|".to_string(),
        ])
    }
}