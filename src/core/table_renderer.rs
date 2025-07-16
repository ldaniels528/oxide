#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// table renderer module
////////////////////////////////////////////////////////////////////

use std::cmp::max;

use crate::columns::Column;
use crate::data_types::DataType::NumberType;
use crate::dataframe::Dataframe;
use crate::model_row_collection::ModelRowCollection;
use crate::number_kind::NumberKind::I64Kind;
use crate::numbers::Numbers::I64Value;
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
            let mut params = vec![Parameter::new("id", NumberType(I64Kind))];
            params.extend(rc.get_columns().iter().map(|c| c.to_parameter()));
            Column::from_parameters(&params)
        };

        // define rows by prepending ID values to each row's data
        let rows: Vec<Row> = rc.read_active_rows()?.into_iter().map(|row| {
            let mut values = vec![Number(I64Value(row.get_id() as i64))];
            values.extend(row.get_values());
            Row::new(row.get_id(), values)
        })
            .collect();

        Ok(Self::from_rows(&columns, &rows))
    }

    fn tabulate_body_cells_from_collection(rc: Box<dyn RowCollection>) -> Vec<Vec<String>> {
        let mut body_cells = Vec::new();
        for row in rc.iter() {
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
        cell.iter().map(|s| s.chars().count()).collect::<Vec<usize>>()
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
    while t.chars().count() < width { t += " " }
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

// Unit tests
#[cfg(test)]
mod tests {
    use crate::byte_row_collection::ByteRowCollection;
    use crate::columns::Column;
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
    
    fn create_collection() -> (ByteRowCollection, Vec<Column>) {
        let phys_columns = make_quote_columns();
        let brc = ByteRowCollection::from_columns_and_rows(phys_columns.clone(), vec![
            make_quote(0, "ABC", "AMEX", 11.77),
            make_quote(1, "UNO", "NASDAQ", 0.2456),
            make_quote(2, "BIZ", "NYSE", 23.66),
            make_quote(3, "GOTO", "OTC", 0.1428),
            make_quote(4, "BOOM", "NASDAQ", 56.87),
        ]);
        (brc, phys_columns)
    }
}