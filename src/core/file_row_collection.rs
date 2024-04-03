////////////////////////////////////////////////////////////////////
// file row-collection module
////////////////////////////////////////////////////////////////////

use std::fmt::{Debug, Formatter};
use std::fs;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::sync::Arc;

use crate::dataframe_config::DataFrameConfig;
use crate::fields::Field;
use crate::namespaces::Namespace;
use crate::row_collection::RowCollection;
use crate::row_metadata::RowMetadata;
use crate::rows::Row;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue;

/// File-based RowCollection implementation
#[derive(Clone)]
pub struct FileRowCollection {
    columns: Vec<TableColumn>,
    file: Arc<File>,
    record_size: usize,
}

impl FileRowCollection {
    pub fn create(ns: Namespace, columns: Vec<TableColumn>) -> std::io::Result<Self> {
        let file = Arc::new(Self::open_crw(&ns)?);
        Ok(Self::new(columns, file))
    }

    pub fn new(columns: Vec<TableColumn>, file: Arc<File>) -> Self {
        let record_size = Row::compute_record_size(&columns);
        Self { columns, file, record_size }
    }

    pub fn open(ns: &Namespace) -> std::io::Result<Self> {
        let cfg = DataFrameConfig::load(&ns)?;
        let columns = TableColumn::from_columns(&cfg.columns)?;
        let file = Arc::new(Self::open_rw(&ns)?);
        Ok(Self::new(columns, file))
    }

    /// convenience function to create, read or write a table file
    pub(crate) fn open_crw(ns: &Namespace) -> std::io::Result<File> {
        let root_path = ns.get_root_path();
        fs::create_dir_all(root_path.clone())?;
        OpenOptions::new().truncate(true).create(true).read(true).write(true).open(root_path)
    }

    /// convenience function to read or write a table file
    pub(crate) fn open_rw(ns: &Namespace) -> std::io::Result<File> {
        OpenOptions::new().read(true).write(true).open(ns.get_table_file_path())
    }
}

impl Debug for FileRowCollection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FileRowCollection({})", self.record_size)
    }
}

impl RowCollection for FileRowCollection {
    fn get_record_size(&self) -> usize { self.record_size }

    fn len(&self) -> std::io::Result<usize> {
        Ok((self.file.metadata()?.len() as usize) / self.record_size)
    }

    fn overwrite(&mut self, id: usize, row: &Row) -> std::io::Result<usize> {
        let offset = self.to_row_offset(id);
        let _ = &self.file.write_at(&row.encode(), offset)?;
        Ok(1)
    }

    fn overwrite_row_metadata(&mut self, id: usize, metadata: &RowMetadata) -> std::io::Result<usize> {
        let offset = self.to_row_offset(id);
        let _ = &self.file.write_at(&[metadata.encode()], offset)?;
        Ok(1)
    }

    fn read(&self, id: usize) -> std::io::Result<(Row, RowMetadata)> {
        let offset = (id * self.record_size) as u64;
        let mut buffer: Vec<u8> = vec![0; self.record_size];
        let _ = &self.file.read_at(&mut buffer, offset)?;
        Ok(Row::decode(&buffer, &self.columns))
    }

    fn read_field(&self, id: usize, column_id: usize) -> std::io::Result<TypedValue> {
        let column = &self.columns[column_id];
        let row_offset = self.to_row_offset(id);
        let mut buffer: Vec<u8> = vec![0; column.max_physical_size];
        let _ = &self.file.read_at(&mut buffer, row_offset + column.offset as u64)?;
        let field = Field::decode(&column.data_type, &buffer, 0);
        Ok(field.value)
    }

    fn read_range(&self, index: std::ops::Range<usize>) -> std::io::Result<Vec<Row>> {
        let mut rows: Vec<Row> = Vec::with_capacity(index.len());
        for id in index {
            let (row, metadata) = self.read(id)?;
            if metadata.is_allocated {
                rows.push(row);
            }
        }
        Ok(rows)
    }

    fn resize(&mut self, new_size: usize) -> std::io::Result<()> {
        let new_length = new_size as u64 * self.record_size as u64;
        // modify the file
        self.file.set_len(new_length)?;
        Ok(())
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::data_types::DataType::{Float64Type, StringType};
    use crate::fields::Field;
    use crate::file_row_collection::FileRowCollection;
    use crate::row_collection::RowCollection;
    use crate::rows::Row;
    use crate::table_columns::TableColumn;
    use crate::testdata::{make_columns, make_quote, make_table_file, make_table_file_from_bytes};
    use crate::typed_values::TypedValue::{Float64Value, Null, StringValue};

    #[test]
    fn test_append_row_then_read_rows() {
        // create a dataframe with a single (encoded) row
        let (file, columns, _) =
            make_table_file_from_bytes("rows", "append_row", "quotes", make_columns(), &mut vec![
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 0,
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'R', b'I', b'C', b'E',
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E',
                0b1000_0000, 64, 83, 150, 102, 102, 102, 102, 102,
            ]);

        let mut frc = <dyn RowCollection>::from_file(columns.clone(), file);

        // insert a row
        let row = make_quote(0, &columns, "RICE", "NYSE", 78.35);
        assert_eq!(frc.overwrite(0, &row).unwrap(), 1);

        // insert a second row
        let row = make_quote(1, &columns, "BEEF", "AMEX", 31.13);
        assert_eq!(frc.overwrite(1, &row).unwrap(), 1);
        assert_eq!(frc.len().unwrap(), 2);

        // retrieve the rows (as binary)
        let rows = frc.read_range(0..2).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], Row {
            id: 0,
            columns: vec![
                TableColumn::new("symbol", StringType(4), Null, 9),
                TableColumn::new("exchange", StringType(4), Null, 22),
                TableColumn::new("lastSale", Float64Type, Null, 35),
            ],
            fields: vec![
                Field::new(StringValue("RICE".into())),
                Field::new(StringValue("NYSE".into())),
                Field::new(Float64Value(78.35)),
            ],
        });
        assert_eq!(rows[1], Row {
            id: 1,
            columns: vec![
                TableColumn::new("symbol", StringType(4), Null, 9),
                TableColumn::new("exchange", StringType(4), Null, 22),
                TableColumn::new("lastSale", Float64Type, Null, 35),
            ],
            fields: vec![
                Field::new(StringValue("BEEF".into())),
                Field::new(StringValue("AMEX".into())),
                Field::new(Float64Value(31.13)),
            ],
        });
    }

    #[test]
    fn test_overwrite_row() {
        // create a new empty table file
        let (file, columns, _) =
            make_table_file("finance", "stocks", "quotes", make_columns());
        let mut frc = FileRowCollection::new(columns.clone(), Arc::new(file));
        frc.resize(0).unwrap();

        // create a new row

        let row = make_quote(2, &columns, "AMD", "NYSE", 88.78);
        assert_eq!(frc.overwrite(row.id, &row).unwrap(), 1);

        // read and verify the row
        let (new_row, rmd) = frc.read(row.id).unwrap();
        assert!(rmd.is_allocated);
        assert_eq!(new_row, Row {
            id: 2,
            columns: vec![
                TableColumn::new("symbol", StringType(4), Null, 9),
                TableColumn::new("exchange", StringType(4), Null, 22),
                TableColumn::new("lastSale", Float64Type, Null, 35),
            ],
            fields: vec![
                Field::new(StringValue("AMD".into())),
                Field::new(StringValue("NYSE".into())),
                Field::new(Float64Value(88.78)),
            ],
        });
    }

    #[test]
    fn test_read_field() {
        // create a dataframe with a single (encoded) row
        let (file, columns, _) =
            make_table_file_from_bytes("rows", "read_field", "quotes", make_columns(), &mut vec![
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 0,
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'F', b'A', b'C', b'T',
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E',
                0b1000_0000, 64, 83, 150, 102, 102, 102, 102, 102,
            ]);

        // read the first column
        let frc = FileRowCollection::new(columns.clone(), Arc::new(file));
        let value = frc.read_field(0, 0).unwrap();
        assert_eq!(value, StringValue("FACT".into()));
    }

    #[test]
    fn test_read_row() {
        // create a dataframe with a single (encoded) row
        let (file, columns, _) =
            make_table_file_from_bytes(
                "dataframes", "read_row", "quotes", make_columns(), &mut vec![
                    0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 5,
                    0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'R', b'O', b'O', b'M',
                    0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'A', b'M', b'E', b'X',
                    0b1000_0000, 64, 83, 150, 102, 102, 102, 102, 102,
                ]);

        // read the row
        let frc = FileRowCollection::new(columns.clone(), Arc::new(file));
        let (row, metadata) = frc.read(0).unwrap();
        assert!(metadata.is_allocated);
        assert_eq!(row, Row {
            id: 5,
            columns: vec![
                TableColumn::new("symbol", StringType(4), Null, 9),
                TableColumn::new("exchange", StringType(4), Null, 22),
                TableColumn::new("lastSale", Float64Type, Null, 35),
            ],
            fields: vec![
                Field::new(StringValue("ROOM".into())),
                Field::new(StringValue("AMEX".into())),
                Field::new(Float64Value(78.35)),
            ],
        });
    }

    #[test]
    fn test_resize_shrink() {
        // create a dataframe with a single (encoded) row
        let (file, columns, _) =
            make_table_file_from_bytes("dataframes", "resize_shrink", "quotes", make_columns(), &mut vec![
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 0,
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'R', b'I', b'C', b'E',
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E',
                0b1000_0000, 64, 83, 150, 102, 102, 102, 102, 102,
            ]);

        let mut frc = FileRowCollection::new(columns.clone(), Arc::new(file));
        let _ = frc.resize(0).unwrap();
        assert_eq!(frc.len().unwrap(), 0);
    }

    #[test]
    fn test_resize_grow() {
        // create a dataframe with a single (encoded) row
        let (file, columns, _) =
            make_table_file_from_bytes("dataframes", "resize_grow", "quotes", make_columns(), &mut vec![
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 0,
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'R', b'I', b'C', b'E',
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E',
                0b1000_0000, 64, 96, 99, 102, 102, 102, 102, 102,
            ]);

        let mut frc = FileRowCollection::new(columns.clone(), Arc::new(file));
        let _ = frc.resize(5).unwrap();
        assert_eq!(frc.len().unwrap(), 5);
    }
}
