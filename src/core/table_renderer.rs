#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// table renderer module
////////////////////////////////////////////////////////////////////

use shared_lib::tabulate_cells;

use crate::columns::Column;
use crate::cursor::Cursor;
use crate::data_types::DataType::NumberType;
use crate::dataframe::Dataframe;
use crate::model_row_collection::ModelRowCollection;
use crate::number_kind::NumberKind::U64Kind;
use crate::numbers::Numbers::U64Value;
use crate::parameter::Parameter;
use crate::row_collection::RowCollection;
use crate::structures::Row;
use crate::typed_values::TypedValue::Number;

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

    /// Transforms the [Dataframe] into a textual table
    pub fn from_dataframe(df: &Dataframe) -> Vec<String> {
        let columns = df.get_columns();
        let rows = df.read_active_rows().unwrap_or(vec![]);
        Self::from_rows(columns, &rows)
    }

    /// Transforms the [Cursor] into a textual table
    pub fn from_cursor(cursor: &mut Cursor, columns: &Vec<Column>) -> Vec<String> {
        let header_cells = Self::tabulate_header_cells(columns);
        let body_cells = Self::tabulate_body_cells_from_cursor(cursor);
        tabulate_cells(header_cells, body_cells)
    }

    /// Transforms the [Vec<Row>] into a textual table
    pub fn from_rows(columns: &Vec<Column>, rows: &Vec<Row>) -> Vec<String> {
        Self::from_collection(Box::new(ModelRowCollection::from_columns_and_rows(columns, rows)))
    }

    /// Transforms the [Vec<Row>] into a textual table
    pub fn from_rows_with_ids(columns: &Vec<Column>, rows: &Vec<Row>) -> std::io::Result<Vec<String>> {
        let rc: Box<dyn RowCollection> = Box::new(ModelRowCollection::from_columns_and_rows(columns, rows));
        Self::from_table_with_ids(&rc)
    }

    /// Transforms the [RowCollection] into a textual table
    pub fn from_table(rc: &Box<dyn RowCollection>) -> Vec<String> {
        let columns = rc.get_columns();
        let rows = rc.read_active_rows().unwrap_or(vec![]);
        Self::from_rows(columns, &rows)
    }

    /// Transforms the [RowCollection] into a textual table
    pub fn from_table_with_ids(rc: &Box<dyn RowCollection>) -> std::io::Result<Vec<String>> {
        // define columns by combining "id" column with those from the RowCollection
        let columns: Vec<Column> = {
            let mut params = vec![Parameter::new("id", NumberType(U64Kind))];
            params.extend(rc.get_columns().iter().map(|c| c.to_parameter()));
            Column::from_parameters(&params)
        };

        // define rows by prepending ID values to each row's data
        let rows: Vec<Row> = rc.read_active_rows()?.into_iter().map(|row| {
            let mut values = vec![Number(U64Value(row.get_id() as u64))];
            values.extend(row.get_values());
            Row::new(row.get_id(), values)
        })
            .collect();

        Ok(Self::from_rows(&columns, &rows))
    }

    fn tabulate_body_cells_from_collection(rc: Box<dyn RowCollection>) -> Vec<Vec<String>> {
        let mut cursor = Cursor::new(rc);
        Self::tabulate_body_cells_from_cursor(&mut cursor)
    }

    fn tabulate_body_cells_from_cursor(cursor: &mut Cursor) -> Vec<Vec<String>> {
        let mut body_cells = Vec::new();
        while let Ok(Some(row)) = cursor.next() {
            let column_text = row.get_values().iter()
                .map(|v| format!(" {} ", v.unwrap_value()))
                .collect::<Vec<String>>();
            body_cells.push(column_text);
        }
        body_cells
    }

    fn tabulate_header_cells(columns: &Vec<Column>) -> Vec<Vec<String>> {
        let mut header_cells = Vec::new();
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
    use crate::columns::Column;
    use crate::cursor::Cursor;
    use crate::table_renderer::TableRenderer;
    use crate::testdata::{make_quote, make_quote_columns};

    #[test]
    fn test_from_collection() {
        let (brc, _) = create_collection();
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

    fn create_collection() -> (ByteRowCollection, Vec<Column>) {
        let phys_columns = make_quote_columns();
        let brc = ByteRowCollection::from_rows(phys_columns.clone(), vec![
            make_quote(0, "ABC", "AMEX", 11.77),
            make_quote(1, "UNO", "NASDAQ", 0.2456),
            make_quote(2, "BIZ", "NYSE", 23.66),
            make_quote(3, "GOTO", "OTC", 0.1428),
            make_quote(4, "BOOM", "NASDAQ", 56.87),
        ]);
        (brc, phys_columns)
    }
}