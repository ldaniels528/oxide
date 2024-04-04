////////////////////////////////////////////////////////////////////
// row-collection module
////////////////////////////////////////////////////////////////////

use std::fmt::Debug;
use std::fs::File;
use std::sync::Arc;

use crate::byte_row_collection::ByteRowCollection;
use crate::file_row_collection::FileRowCollection;
use crate::row_metadata::RowMetadata;
use crate::rows::Row;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue;

/// represents the underlying storage resource for the dataframe
pub trait RowCollection: Debug {
    fn get_record_size(&self) -> usize;

    /// returns the number of active rows in the table
    fn len(&self) -> std::io::Result<usize>;

    /// overwrites the specified row by ID
    fn overwrite(&mut self, id: usize, row: &Row) -> std::io::Result<usize>;

    /// overwrites the metadata of a specified row by ID
    fn overwrite_row_metadata(&mut self, id: usize, metadata: &RowMetadata) -> std::io::Result<usize>;

    fn read(&self, id: usize) -> std::io::Result<(Row, RowMetadata)>;

    fn read_field(&self, id: usize, column_id: usize) -> std::io::Result<TypedValue>;

    fn read_range(&self, index: std::ops::Range<usize>) -> std::io::Result<Vec<Row>>;

    /// resizes (shrink or grow) the table
    fn resize(&mut self, new_size: usize) -> std::io::Result<()>;

    fn to_row_offset(&self, id: usize) -> u64 { (id * self.get_record_size()) as u64 }
}

impl dyn RowCollection {
    /// creates a new in-memory [RowCollection] from a byte vector.
    pub fn from_bytes(columns: Vec<TableColumn>, rows: Vec<Vec<u8>>) -> impl RowCollection {
        ByteRowCollection::new(columns, rows)
    }

    /// creates a new [RowCollection] from a file.
    pub fn from_file(columns: Vec<TableColumn>, file: File) -> impl RowCollection {
        FileRowCollection::new(columns, Arc::new(file))
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::row;
    use crate::testdata::{make_columns, make_dataframe, make_quote, make_table_columns, make_table_file, make_table_file_from_bytes};
    use crate::typed_values::TypedValue::{Float64Value, StringValue};

    use super::*;

    #[test]
    fn test_from_bytes() {
        // determine the record size of the row
        let columns = make_table_columns();
        let row = make_quote(0, &columns, "RICE", "NYSE", 78.78);
        let mut rc = <dyn RowCollection>::from_bytes(columns.clone(), vec![]);

        // create a new row
        assert_eq!(rc.overwrite(row.id, &row).unwrap(), 1);

        // read and verify the row
        let (row, rmd) = rc.read(0).unwrap();
        assert!(rmd.is_allocated);
        assert_eq!(row, row!(0, make_table_columns(), vec![
            StringValue("RICE".into()), StringValue("NYSE".into()), Float64Value(78.78)
        ]))
    }

    #[test]
    fn test_from_file() {
        let (file, columns, _) =
            make_table_file("rows", "append_row", "quotes", make_columns());
        let mut rc = <dyn RowCollection>::from_file(columns.clone(), file);
        rc.overwrite(0, &make_quote(0, &columns, "BEAM", "NYSE", 78.35)).unwrap();

        // read and verify the row
        let (row, rmd) = rc.read(0).unwrap();
        assert!(rmd.is_allocated);
        assert_eq!(row, row!(0, make_table_columns(), vec![
            StringValue("BEAM".into()), StringValue("NYSE".into()), Float64Value(78.35)
        ]))
    }
}
