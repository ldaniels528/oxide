////////////////////////////////////////////////////////////////////
// table writer module
////////////////////////////////////////////////////////////////////

use shared_lib::{RowJs, tabulate_body_cells_from_rows, tabulate_cells, tabulate_header_cells};

/// Table Writer
pub struct TableWriter;

impl TableWriter {
    /// Transforms the vector of [RowJs] into a textual table
    pub fn from_rows(rows: &Vec<RowJs>) -> Vec<String> {
        let header_cells = tabulate_header_cells(rows);
        let body_cells = tabulate_body_cells_from_rows(rows);
        tabulate_cells(header_cells, body_cells)
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use shared_lib::RowJs;

    use super::*;

    #[test]
    fn test_table_writer() {
        let rows = RowJs::vec_from_string(r#"
            [{"fields":[{"name":"symbol","value":"ABC"},{"name":"exchange","value":"AMEX"},
            {"name":"last_sale","value":11.77}],"id":0},{"fields":[{"name":"symbol","value":"BIZ"},
            {"name":"exchange","value":"NYSE"},{"name":"last_sale","value":23.66}],"id":2},
            {"fields":[{"name":"symbol","value":"BOOM"},{"name":"exchange","value":"NASDAQ"},
            {"name":"last_sale","value":56.87}],"id":4}]
        "#).unwrap();
        let lines = TableWriter::from_rows(&rows);
        assert_eq!(lines, vec![
            "|-------------------------------|".to_string(),
            "| symbol | exchange | last_sale |".to_string(),
            "|-------------------------------|".to_string(),
            r#"| "ABC"  | "AMEX"   | 11.77     |"#.to_string(),
            r#"| "BIZ"  | "NYSE"   | 23.66     |"#.to_string(),
            r#"| "BOOM" | "NASDAQ" | 56.87     |"#.to_string(),
            "|-------------------------------|".to_string()
        ])
    }
}