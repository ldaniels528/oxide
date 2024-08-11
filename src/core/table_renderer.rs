////////////////////////////////////////////////////////////////////
// table renderer module
////////////////////////////////////////////////////////////////////

use shared_lib::tabulate_cells;

use crate::cursor::Cursor;
use crate::model_row_collection::ModelRowCollection;
use crate::row_collection::RowCollection;
use crate::rows::Row;
use crate::table_columns::TableColumn;

/// Table renderer
pub struct TableRenderer;

impl TableRenderer {
    /// Transforms the [RowCollection] into a textual table
    pub fn from_collection(rc: Box<dyn RowCollection>) -> Vec<String> {
        let columns = rc.get_columns();
        let header_cells = Self::tabulate_header_cells(columns);
        let body_cells = Self::tabulate_body_cells_from_collection(rc);
        tabulate_cells(header_cells, body_cells)
    }

    /// Transforms the [Cursor] into a textual table
    pub fn from_cursor(cursor: &mut Cursor, columns: &Vec<TableColumn>) -> Vec<String> {
        let header_cells = Self::tabulate_header_cells(columns);
        let body_cells = Self::tabulate_body_cells_from_cursor(cursor);
        tabulate_cells(header_cells, body_cells)
    }

    /// Transforms the [Vec<Row>] into a textual table
    pub fn from_rows(rows: Vec<Row>) -> Vec<String> {
        Self::from_collection(Box::new(ModelRowCollection::from_rows(rows)))
    }

    fn tabulate_body_cells_from_collection(rc: Box<dyn RowCollection>) -> Vec<Vec<String>> {
        let mut cursor = Cursor::new(rc);
        Self::tabulate_body_cells_from_cursor(&mut cursor)
    }

    fn tabulate_body_cells_from_cursor(cursor: &mut Cursor) -> Vec<Vec<String>> {
        let mut body_cells = vec![];
        while let Ok(Some(row)) = cursor.next() {
            let column_text = row.get_values().iter()
                .map(|v| format!(" {} ", v.unwrap_value()))
                .collect::<Vec<String>>();
            body_cells.push(column_text);
        }
        body_cells
    }

    fn tabulate_header_cells(columns: &Vec<TableColumn>) -> Vec<Vec<String>> {
        let mut header_cells = vec![];
        let headers = columns.iter()
            .map(|c| format!(" {} ", c.get_name()))
            .collect::<Vec<String>>();
        header_cells.push(headers);
        header_cells
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::byte_row_collection::ByteRowCollection;
    use crate::cursor::Cursor;
    use crate::table_columns::TableColumn;
    use crate::table_renderer::TableRenderer;
    use crate::testdata::{make_quote, make_quote_columns};

    #[test]
    fn test_from_collection() {
        let (brc, columns) = create_collection();
        let lines = TableRenderer::from_collection(Box::new(brc));
        for line in &lines { println!("{}", line) }
        assert_eq!(lines, vec![
            "|-------------------------------|",
            "| symbol | exchange | last_sale |",
            "|-------------------------------|",
            "| ABC    | AMEX     | 11.77     |",
            "| UNO    | NASDAQ   | 0.2456    |",
            "| BIZ    | NYSE     | 23.66     |",
            "| GOTO   | OTC      | 0.1428    |",
            "| BOOM   | NASDAQ   | 56.87     |",
            "|-------------------------------|",
        ])
    }

    #[test]
    fn test_from_cursor() {
        let (brc, columns) = create_collection();
        let mut cursor = Cursor::new(Box::new(brc));
        let lines = TableRenderer::from_cursor(&mut cursor, &columns);
        assert_eq!(lines, vec![
            "|-------------------------------|",
            "| symbol | exchange | last_sale |",
            "|-------------------------------|",
            "| ABC    | AMEX     | 11.77     |",
            "| UNO    | NASDAQ   | 0.2456    |",
            "| BIZ    | NYSE     | 23.66     |",
            "| GOTO   | OTC      | 0.1428    |",
            "| BOOM   | NASDAQ   | 56.87     |",
            "|-------------------------------|",
        ])
    }

    fn create_collection() -> (ByteRowCollection, Vec<TableColumn>) {
        let phys_columns = TableColumn::from_columns(&make_quote_columns()).unwrap();
        let brc = ByteRowCollection::from_rows(vec![
            make_quote(0, &phys_columns, "ABC", "AMEX", 11.77),
            make_quote(1, &phys_columns, "UNO", "NASDAQ", 0.2456),
            make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66),
            make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428),
            make_quote(4, &phys_columns, "BOOM", "NASDAQ", 56.87),
        ]);
        (brc, phys_columns)
    }
}