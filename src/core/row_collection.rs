////////////////////////////////////////////////////////////////////
// row-collection module
////////////////////////////////////////////////////////////////////

use std::fmt::Debug;
use std::fs::File;

use crate::byte_row_collection::ByteRowCollection;
use crate::file_row_collection::FileRowCollection;

/// represents the underlying storage resource for the dataframe
pub trait RowCollection: Debug {
    fn get_record_size(&self) -> usize;

    /// returns the number of active rows in the table
    fn len(&self) -> std::io::Result<usize>;

    fn overwrite(&mut self, id: usize, block: Vec<u8>) -> std::io::Result<usize>;

    /// overwrites the metadata of a specified row by ID
    fn overwrite_row_metadata(&mut self, id: usize, metadata: u8) -> std::io::Result<usize>;

    fn read(&self, id: usize) -> std::io::Result<Vec<u8>>;

    fn read_field(&self, id: usize, column_offset: usize, max_physical_size: usize) -> std::io::Result<Vec<u8>>;

    fn read_range(&self, from: usize, to: usize) -> std::io::Result<Vec<Vec<u8>>>;

    /// resizes the table
    fn resize(&mut self, new_size: usize) -> std::io::Result<()>;

    fn to_row_offset(&self, id: usize) -> u64 { (id * self.get_record_size()) as u64 }
}

impl dyn RowCollection {
    /// creates a new in-memory [RowCollection] from a byte vector.
    pub fn from_bytes(rows: Vec<Vec<u8>>, record_size: usize) -> impl RowCollection {
        ByteRowCollection::new(rows, record_size)
    }

    /// creates a new [RowCollection] from a file.
    pub fn from_file(file: File, record_size: usize) -> impl RowCollection {
        FileRowCollection::new(file, record_size)
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::row;
    use crate::rows::Row;
    use crate::testdata::{make_columns, make_quote, make_table_columns, make_table_file_from_bytes};
    use crate::typed_values::TypedValue::{Float64Value, StringValue};

    use super::*;

    #[test]
    fn test_from_bytes() {
        // determine the record size of the row
        let columns = make_table_columns();
        let row = make_quote(0, &columns, "RICE", "NYSE", 78.78);
        let record_size = row.get_record_size();
        let mut rc = <dyn RowCollection>::from_bytes(vec![], record_size);

        // create a new row
        assert_eq!(rc.overwrite(row.id, row.encode()).unwrap(), 1);

        // read and verify the row
        let row_bytes = rc.read(0).unwrap();
        let (row, rmd) = Row::decode(&row_bytes, &columns);
        assert!(rmd.is_allocated);
        assert_eq!(row, row!(0, make_table_columns(), vec![
            StringValue("RICE".into()), StringValue("NYSE".into()), Float64Value(78.78)
        ]))
    }

    #[test]
    fn test_from_file() {
        let (file, columns, record_size) =
            make_table_file_from_bytes("rows", "append_row", "quotes", make_columns(), vec![
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 0,
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'B', b'E', b'A', b'M',
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E',
                0b1000_0000, 64, 83, 150, 102, 102, 102, 102, 102,
            ].as_slice());
        let rc = <dyn RowCollection>::from_file(file, record_size);

        // read and verify the row
        let row_bytes = rc.read(0).unwrap();
        let (row, rmd) = Row::decode(&row_bytes, &columns);
        assert!(rmd.is_allocated);
        assert_eq!(row, row!(0, make_table_columns(), vec![
            StringValue("BEAM".into()), StringValue("NYSE".into()), Float64Value(78.35)
        ]))
    }
}
