////////////////////////////////////////////////////////////////////
// table_view trait
////////////////////////////////////////////////////////////////////

use std::io;

use crate::dataframes::DataFrame;
use crate::rows::Row;
use crate::table_view::Direction::{Backward, Forward};

/// represents a table view or table query
pub trait TableView {
    fn next(&mut self) -> io::Result<Option<Row>>;
}

/// represents a forward/reverse dataframe iterator
pub struct TableIterator {
    data_frame: DataFrame,
    direction: Direction,
    position: usize,
}

impl TableView for TableIterator {
    fn next(&mut self) -> io::Result<Option<Row>> {
        let result: Option<Row> = match &self.direction {
            Backward => {
                if self.position > 0 {
                    let new_position = self.position - 1;
                    self.position = new_position;
                    let (row, metadata) = self.data_frame.read_row(self.position)?;
                    if metadata.is_allocated { Some(row) } else { None }
                } else { None }
            }
            Forward => {
                if self.position < self.data_frame.len()? {
                    let (row, metadata) = self.data_frame.read_row(self.position)?;
                    let new_position = self.position + 1;
                    self.position = new_position;
                    if metadata.is_allocated { Some(row) } else { None }
                } else { None }
            }
        };
        Ok(result)
    }
}

impl TableIterator {
    /// constructs a new [TableIterator]
    pub fn new(data_frame: DataFrame) -> TableIterator {
        TableIterator {
            data_frame,
            direction: Forward,
            position: 0,
        }
    }

    pub fn reverse(data_frame: DataFrame) -> TableIterator {
        match data_frame.len() {
            Ok(size) => TableIterator {
                data_frame,
                direction: Backward,
                position: size,
            },
            Err(err) => panic!("{}", err.to_string())
        }
    }
}

/// represents the iterator movement direction
pub enum Direction {
    Forward = 0,
    Backward = 1,
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::rows::Row;
    use crate::table_columns::TableColumn;
    use crate::table_view::{TableIterator, TableView};
    use crate::testdata::{make_dataframe, make_quote, make_quote_columns};

    #[test]
    fn test_forward_iterator() {
        // create a dataframe with 3 rows, 3 columns
        let columns = make_quote_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let mut df = make_dataframe("dataframes", "forward_iterator", "quotes", columns).unwrap();
        df.resize(0).unwrap();
        df.append(make_quote(0, &phys_columns, "AAPL", "NYSE", 118.77)).unwrap();
        df.append(make_quote(1, &phys_columns, "AMD", "NYSE", 93.22)).unwrap();
        df.append(make_quote(2, &phys_columns, "INTC", "NYSE", 67.44)).unwrap();

        // create the iterator and verify the results
        let mut iter = TableIterator::new(df);
        assert_eq!(iter.next().unwrap(), inline(0, &phys_columns, "AAPL", "NYSE", 118.77));
        assert_eq!(iter.next().unwrap(), inline(1, &phys_columns, "AMD", "NYSE", 93.22));
        assert_eq!(iter.next().unwrap(), inline(2, &phys_columns, "INTC", "NYSE", 67.44));
    }

    #[test]
    fn test_reverse_iterator() {
        // create a dataframe with 3 rows, 3 columns
        let columns = make_quote_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let mut df = make_dataframe("dataframes", "reverse_iterator", "quotes", columns).unwrap();
        df.append(make_quote(0, &phys_columns, "AAPL", "NYSE", 118.77)).unwrap();
        df.append(make_quote(1, &phys_columns, "AMD", "NYSE", 93.22)).unwrap();
        df.append(make_quote(2, &phys_columns, "INTC", "NYSE", 67.44)).unwrap();

        // test the reverse flow
        let mut iter = TableIterator::reverse(df);
        assert_eq!(iter.next().unwrap(), inline(2, &phys_columns, "INTC", "NYSE", 67.44));
        assert_eq!(iter.next().unwrap(), inline(1, &phys_columns, "AMD", "NYSE", 93.22));
        assert_eq!(iter.next().unwrap(), inline(0, &phys_columns, "AAPL", "NYSE", 118.77));
    }

    fn inline(id: usize,
              phys_columns: &Vec<TableColumn>,
              symbol: &str,
              exchange: &str,
              last_sale: f64) -> Option<Row> {
        Some(make_quote(id, phys_columns, symbol, exchange, last_sale))
    }
}