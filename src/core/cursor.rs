////////////////////////////////////////////////////////////////////
// cursor module
////////////////////////////////////////////////////////////////////

use std::fmt::Debug;

use crate::expression::Condition;
use crate::machine::Machine;
use crate::row_collection::RowCollection;
use crate::rows::Row;

/// Represents a cursor that matches rows based on a condition
#[derive(Debug)]
pub struct Cursor {
    rc: Box<dyn RowCollection>,
    condition: Option<Condition>,
    is_forward: bool,
    position: usize,
}

impl Cursor {

    ////////////////////////////////////////////////////////////////
    //  Constructors
    ////////////////////////////////////////////////////////////////

    /// Full constructor
    pub fn construct(
        rc: Box<dyn RowCollection>,
        condition: Option<Condition>,
        is_forward: bool,
        position: usize,
    ) -> Self {
        Self { rc, condition, is_forward, position }
    }

    /// Creates a new cursor that returns rows which satisfy a given condition
    pub fn filter(rc: Box<dyn RowCollection>, condition: Condition) -> Self {
        Self::construct(rc, Some(condition), true, 0)
    }

    /// Creates a new cursor that returns all rows
    pub fn new(rc: Box<dyn RowCollection>) -> Self {
        Self::construct(rc, None, true, 0)
    }

    ////////////////////////////////////////////////////////////////
    //  Functions and Methods
    ////////////////////////////////////////////////////////////////

    /// Moves the current position to the end-of-file (EOF)
    pub fn bottom(&mut self) {
        self.position = self.rc.len().unwrap_or(0)
    }

    /// Folds all rows from the current position to the end-of-file (EOF)
    pub fn fold_left<A>(&mut self, initial: A, f: fn(A, Row) -> A) -> A {
        let mut result = initial;
        while let Ok(Some(row)) = self.next() {
            result = f(result, row)
        }
        result
    }

    /// Folds all rows from the end-of-file (EOF) to the current position
    pub fn fold_right<A>(&mut self, initial: A, f: fn(Row, A) -> A) -> A {
        let mut result = initial;
        while let Ok(Some(row)) = self.previous() {
            result = f(row, result)
        }
        result
    }

    /// Attempts to retrieve the row that corresponds to the given position
    pub fn get(&mut self, pos: usize) -> std::io::Result<Option<Row>> {
        let (row, rmd) = self.rc.read_row(pos)?;
        if rmd.is_allocated {
            let machine = Machine::new().with_row(self.rc.get_columns(), &row);
            if row.matches(&machine, &self.condition, self.rc.get_columns()) {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    pub fn get_collection(&self) -> &Box<dyn RowCollection> {
        &self.rc
    }

    /// Transforms all rows from the current position to the end-of-file (EOF)
    pub fn map<A>(&mut self, f: fn(Row) -> A) -> Vec<A> {
        let mut values = Vec::new();
        while let Ok(Some(row)) = self.next() { values.push(f(row)) }
        values
    }

    /// Moves the current position to the middle-of-file
    pub fn middle(&mut self) {
        self.position = self.rc.len().unwrap_or(0) / 2
    }

    /// Returns the next qualifying row or [None] if not found before the end-of-file (EOF)
    fn move_next(&mut self) -> std::io::Result<Option<Row>> {
        let mut result: Option<Row> = None;
        let mut pos = self.position;
        let eof = self.rc.len()?;
        while result.is_none() && pos < eof {
            result = self.get(pos)?;
            pos += 1;
        }
        self.position = pos;
        Ok(result)
    }

    /// Returns the previous qualifying row or [None] if not found before the top-of-file
    fn move_previous(&mut self) -> std::io::Result<Option<Row>> {
        let mut result = None;
        let mut pos = self.position;
        while result.is_none() && pos > 0 {
            pos -= 1;
            result = self.get(pos)?;
        }
        self.position = pos;
        Ok(result)
    }

    /// Returns the next qualifying row or [None] if not found before the end-of-file (EOF)
    pub fn next(&mut self) -> std::io::Result<Option<Row>> {
        if self.is_forward { self.move_next() } else { self.move_previous() }
    }

    /// Returns the previous qualifying row or [None] if not found before the top-of-file
    pub fn previous(&mut self) -> std::io::Result<Option<Row>> {
        if self.is_forward { self.move_previous() } else { self.move_next() }
    }

    /// Inverts the cursor direction
    pub fn reverse(&mut self) {
        self.is_forward = !self.is_forward;
        if self.is_forward {
            self.position += 1
        } else if self.position > 0 {
            self.position -= 1
        }
    }

    /// Returns the previous qualifying row or [None] if not found before the top-of-file
    pub fn take(&mut self, limit: usize) -> std::io::Result<Vec<Row>> {
        let mut rows = Vec::new();
        let mut done = false;
        while !done && (limit == 0 || rows.len() < limit) {
            if let Ok(Some(row)) = self.next() {
                rows.push(row)
            } else {
                done = true
            }
        }
        Ok(rows)
    }

    /// Moves the current position to the initial position
    pub fn top(&mut self) {
        self.position = 0
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::byte_row_collection::ByteRowCollection;
    use crate::cursor::Cursor;
    use crate::expression::Condition::Equal;
    use crate::expression::Expression::{Literal, Variable};
    use crate::table_columns::TableColumn;
    use crate::testdata::{make_quote, make_quote_columns};
    use crate::typed_values::TypedValue::StringValue;

    #[test]
    fn test_cursor_navigation_top_middle_and_bottom() {
        let (brc, phys_columns) = create_sample_data_1();
        let mut cursor = Cursor::new(Box::new(brc));

        cursor.middle();
        assert_eq!(cursor.next().unwrap(), Some(make_quote(2, "BIZ", "NYSE", 23.66)));

        cursor.bottom();
        assert_eq!(cursor.previous().unwrap(), Some(make_quote(4, "BOOM", "NASDAQ", 56.87)));

        cursor.top();
        assert_eq!(cursor.next().unwrap(), Some(make_quote(0, "ABC", "AMEX", 11.77)));
    }

    #[test]
    fn test_cursor_with_all_rows() {
        let (brc, phys_columns) = create_sample_data_1();
        let mut cursor = Cursor::new(Box::new(brc));
        assert_eq!(cursor.next().unwrap(), Some(make_quote(0, "ABC", "AMEX", 11.77)));
        assert_eq!(cursor.next().unwrap(), Some(make_quote(1, "UNO", "NASDAQ", 0.2456)));
        assert_eq!(cursor.next().unwrap(), Some(make_quote(2, "BIZ", "NYSE", 23.66)));
        assert_eq!(cursor.next().unwrap(), Some(make_quote(3, "GOTO", "OTC", 0.1428)));
        assert_eq!(cursor.next().unwrap(), Some(make_quote(4, "BOOM", "NASDAQ", 56.87)));
        assert_eq!(cursor.next().unwrap(), None);
        assert_eq!(cursor.previous().unwrap(), Some(make_quote(4, "BOOM", "NASDAQ", 56.87)));
        assert_eq!(cursor.previous().unwrap(), Some(make_quote(3, "GOTO", "OTC", 0.1428)));
        assert_eq!(cursor.previous().unwrap(), Some(make_quote(2, "BIZ", "NYSE", 23.66)));
        assert_eq!(cursor.previous().unwrap(), Some(make_quote(1, "UNO", "NASDAQ", 0.2456)));
        assert_eq!(cursor.previous().unwrap(), Some(make_quote(0, "ABC", "AMEX", 11.77)));
        assert_eq!(cursor.previous().unwrap(), None);
    }

    #[test]
    fn test_cursor_with_filtered_rows() {
        let (brc, phys_columns) = create_sample_data_2();
        let mut cursor = Cursor::filter(Box::new(brc), Equal(
            Box::new(Variable("exchange".to_string())),
            Box::new(Literal(StringValue("NYSE".to_string()))),
        ));
        assert_eq!(cursor.next().unwrap(), Some(make_quote(0, "ABC", "NYSE", 11.77)));
        assert_eq!(cursor.next().unwrap(), Some(make_quote(2, "BIZ", "NYSE", 23.66)));
        assert_eq!(cursor.next().unwrap(), Some(make_quote(4, "BOOM", "NYSE", 56.87)));
        assert_eq!(cursor.next().unwrap(), None);
        assert_eq!(cursor.previous().unwrap(), Some(make_quote(4, "BOOM", "NYSE", 56.87)));
        assert_eq!(cursor.previous().unwrap(), Some(make_quote(2, "BIZ", "NYSE", 23.66)));
        assert_eq!(cursor.previous().unwrap(), Some(make_quote(0, "ABC", "NYSE", 11.77)));
        assert_eq!(cursor.previous().unwrap(), None);
    }

    #[test]
    fn test_fold_left() {
        let (brc, _) = create_sample_data_1();
        let mut cursor = Cursor::new(Box::new(brc));
        let results = cursor.fold_left(Vec::new(), |mut agg, r| {
            agg.push(r.get(1));
            agg
        });
        assert_eq!(results, vec![
            StringValue("AMEX".to_string()), StringValue("NASDAQ".to_string()),
            StringValue("NYSE".to_string()), StringValue("OTC".to_string()),
            StringValue("NASDAQ".to_string()),
        ])
    }

    #[test]
    fn test_fold_right() {
        let (brc, _) = create_sample_data_1();
        let mut cursor = Cursor::new(Box::new(brc));
        cursor.bottom();
        let results = cursor.fold_right(Vec::new(), |r, mut agg| {
            agg.push(r.get(1));
            agg
        });
        assert_eq!(results, vec![
            StringValue("NASDAQ".to_string()), StringValue("OTC".to_string()),
            StringValue("NYSE".to_string()), StringValue("NASDAQ".to_string()),
            StringValue("AMEX".to_string()),
        ])
    }

    #[test]
    fn test_map_rows_in_cursor() {
        let (brc, _) = create_sample_data_1();
        let mut cursor = Cursor::new(Box::new(brc));
        let result = cursor.map(|r| r.get(0));
        assert_eq!(result, vec![
            StringValue("ABC".to_string()), StringValue("UNO".to_string()),
            StringValue("BIZ".to_string()), StringValue("GOTO".to_string()),
            StringValue("BOOM".to_string()),
        ])
    }

    #[test]
    fn test_reverse() {
        let (brc, phys_columns) = create_sample_data_1();
        let mut cursor = Cursor::new(Box::new(brc));
        assert_eq!(cursor.next().unwrap(), Some(make_quote(0, "ABC", "AMEX", 11.77)));
        assert_eq!(cursor.next().unwrap(), Some(make_quote(1, "UNO", "NASDAQ", 0.2456)));
        assert_eq!(cursor.next().unwrap(), Some(make_quote(2, "BIZ", "NYSE", 23.66)));
        cursor.reverse();
        assert_eq!(cursor.next().unwrap(), Some(make_quote(1, "UNO", "NASDAQ", 0.2456)));
        assert_eq!(cursor.next().unwrap(), Some(make_quote(0, "ABC", "AMEX", 11.77)));
        cursor.reverse();
        assert_eq!(cursor.next().unwrap(), Some(make_quote(1, "UNO", "NASDAQ", 0.2456)));
    }

    #[test]
    fn test_take() {
        let (brc, phys_columns) = create_sample_data_1();
        let mut cursor = Cursor::new(Box::new(brc));
        assert_eq!(cursor.take(3).unwrap(), vec![
            make_quote(0, "ABC", "AMEX", 11.77),
            make_quote(1, "UNO", "NASDAQ", 0.2456),
            make_quote(2, "BIZ", "NYSE", 23.66),
        ])
    }

    fn create_sample_data_1() -> (ByteRowCollection, Vec<TableColumn>) {
        let phys_columns = TableColumn::from_columns(&make_quote_columns()).unwrap();
        let brc = ByteRowCollection::from_rows(phys_columns.clone(), vec![
            make_quote(0, "ABC", "AMEX", 11.77),
            make_quote(1, "UNO", "NASDAQ", 0.2456),
            make_quote(2, "BIZ", "NYSE", 23.66),
            make_quote(3, "GOTO", "OTC", 0.1428),
            make_quote(4, "BOOM", "NASDAQ", 56.87),
        ]);
        (brc, phys_columns)
    }

    fn create_sample_data_2() -> (ByteRowCollection, Vec<TableColumn>) {
        let phys_columns = TableColumn::from_columns(&make_quote_columns()).unwrap();
        let brc = ByteRowCollection::from_rows(phys_columns.clone(), vec![
            make_quote(0, "ABC", "NYSE", 11.77),
            make_quote(1, "UNO", "NASDAQ", 0.2456),
            make_quote(2, "BIZ", "NYSE", 23.66),
            make_quote(3, "GOTO", "NASDAQ", 0.1428),
            make_quote(4, "BOOM", "NYSE", 56.87),
            make_quote(5, "YIKES", "OTC", 0.00007),
        ]);
        (brc, phys_columns)
    }
}