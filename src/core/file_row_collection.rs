////////////////////////////////////////////////////////////////////
// file row-collection module
////////////////////////////////////////////////////////////////////

use std::fmt::{Debug, Formatter};
use std::fs::{File, OpenOptions};
use std::io::{Seek, Write};
use std::os::unix::fs::FileExt;
use std::sync::Arc;

use crate::dataframe_config::DataFrameConfig;
use crate::namespaces::Namespace;
use crate::row_collection::RowCollection;
use crate::row_metadata::RowMetadata;
use crate::rows::Row;
use crate::table_columns::TableColumn;

/// File-based RowCollection implementation
#[derive(Clone)]
pub struct FileRowCollection {
    file: Arc<File>,
    record_size: usize,
}

impl FileRowCollection {
    pub fn create(ns: Namespace, record_size: usize) -> std::io::Result<FileRowCollection> {
        let file = Arc::new(Self::open_crw(&ns)?);
        Ok(Self::new(file, record_size))
    }

    pub fn new(file: Arc<File>, record_size: usize) -> FileRowCollection {
        FileRowCollection { file, record_size }
    }

    pub fn open(ns: &Namespace) -> std::io::Result<Self> {
        let cfg = DataFrameConfig::load(&ns)?;
        let columns = TableColumn::from_columns(&cfg.columns)?;
        let record_size = Row::compute_record_size(&columns);
        let file = Arc::new(Self::open_rw(&ns)?);
        Ok(Self::new(file, record_size))
    }

    /// convenience function to create, read or write a table file
    pub(crate) fn open_crw(ns: &Namespace) -> std::io::Result<File> {
        OpenOptions::new().create(true).read(true).write(true).open(ns.get_table_file_path())
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

    fn overwrite(&mut self, id: usize, block: Vec<u8>) -> std::io::Result<usize> {
        let offset = self.to_row_offset(id);
        let _ = &self.file.write_at(&block, offset)?;
        Ok(1)
    }

    fn overwrite_row_metadata(&mut self, id: usize, metadata: u8) -> std::io::Result<usize> {
        let offset = self.to_row_offset(id);
        let _ = &self.file.write_at(&[metadata], offset)?;
        Ok(1)
    }

    fn read(&self, id: usize) -> std::io::Result<Vec<u8>> {
        let offset = (id * self.record_size) as u64;
        let mut buffer: Vec<u8> = vec![0; self.record_size];
        let _ = &self.file.read_at(&mut buffer, offset)?;
        Ok(buffer)
    }

    fn read_field(&self, id: usize, column_offset: usize, column_max_size: usize) -> std::io::Result<Vec<u8>> {
        let row_offset = self.to_row_offset(id);
        let mut buffer: Vec<u8> = vec![0; column_max_size];
        let _ = &self.file.read_at(&mut buffer, row_offset + column_offset as u64)?;
        Ok(buffer)
    }

    fn read_range(&self, from: usize, to: usize) -> std::io::Result<Vec<Vec<u8>>> {
        let mut rows: Vec<Vec<u8>> = Vec::with_capacity(to - from);
        for id in from..to {
            let bytes = self.read(id)?;
            let metadata: RowMetadata = RowMetadata::decode(bytes[0]);
            if metadata.is_allocated {
                rows.push(bytes);
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

    use crate::codec;
    use crate::data_types::DataType::{Float64Type, StringType};
    use crate::fields::Field;
    use crate::file_row_collection::FileRowCollection;
    use crate::row_collection::RowCollection;
    use crate::rows::Row;
    use crate::table_columns::TableColumn;
    use crate::testdata::{make_columns, make_quote, make_table_file, make_table_file_from_bytes};
    use crate::typed_values::TypedValue;
    use crate::typed_values::TypedValue::{Float64Value, Null, StringValue};

    #[test]
    fn test_append_row_then_read_rows() {
        // create a dataframe with a single (encoded) row
        let (file, columns, record_size) =
            make_table_file_from_bytes("rows", "append_row", "quotes", make_columns(), &mut vec![
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 0,
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'R', b'I', b'C', b'E',
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E',
                0b1000_0000, 64, 83, 150, 102, 102, 102, 102, 102,
            ]);

        // insert a second row
        let row_data: Vec<u8> = vec![
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 1,
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'B', b'E', b'E', b'F',
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'A', b'M', b'E', b'X',
            0b1000_0000, 64, 89, 0, 0, 0, 0, 0, 0,
        ];
        let target_row_id: usize = codec::decode_row_id(&row_data, 1);
        let mut frc = <dyn RowCollection>::from_file(file, record_size);
        assert_eq!(frc.overwrite(target_row_id, row_data).unwrap(), 1);
        assert_eq!(frc.len().unwrap(), 2);

        // retrieve the rows (as binary)
        let bytes = frc.read_range(0, 2).unwrap();
        assert_eq!(bytes.len(), 2);
        assert_eq!(frc.len().unwrap(), 2);

        // decode and verify the rows
        let rows: Vec<Row> = Row::decode_rows(&columns, bytes);
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
                Field::new(Float64Value(100.0)),
            ],
        });
    }

    #[test]
    fn test_overwrite_row() {
        // create a new empty table file
        let (file, columns, record_size) =
            make_table_file("finance", "stocks", "quotes", make_columns());
        let mut frc = FileRowCollection::new(Arc::new(file), record_size);
        frc.resize(0).unwrap();

        // create a new row

        let row = make_quote(2, &columns, "AMD", "NYSE", 88.78);
        assert_eq!(frc.overwrite(row.id, row.encode()).unwrap(), 1);

        // read and verify the row
        let bytes = frc.read(row.id).unwrap();
        let (new_row, _) = Row::decode(&bytes, &columns);
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
        let (file, columns, record_size) =
            make_table_file_from_bytes("rows", "read_field", "quotes", make_columns(), &mut vec![
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 0,
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'F', b'A', b'C', b'T',
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E',
                0b1000_0000, 64, 83, 150, 102, 102, 102, 102, 102,
            ]);

        // read the first column
        let column: &TableColumn = &columns[0];
        let frc = FileRowCollection::new(Arc::new(file), record_size);
        let buf = frc.read_field(0, column.offset, column.max_physical_size).unwrap();
        let value: TypedValue = TypedValue::decode(&column.data_type, &buf, 1);
        assert_eq!(value, StringValue("FACT".into()));
    }

    #[test]
    fn test_read_row() {
        // create a dataframe with a single (encoded) row
        let (file, columns, record_size) =
            make_table_file_from_bytes(
                "dataframes", "read_row", "quotes", make_columns(), &mut vec![
                    0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 5,
                    0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'R', b'O', b'O', b'M',
                    0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'A', b'M', b'E', b'X',
                    0b1000_0000, 64, 83, 150, 102, 102, 102, 102, 102,
                ]);

        // read the row
        let frc = FileRowCollection::new(Arc::new(file), record_size);
        let bytes = frc.read(0).unwrap();
        let (row, metadata) = Row::decode(&bytes, &columns);

        // verify the row
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
        let (file, _, record_size) =
            make_table_file_from_bytes("dataframes", "resize_shrink", "quotes", make_columns(), &mut vec![
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 0,
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'R', b'I', b'C', b'E',
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E',
                0b1000_0000, 64, 83, 150, 102, 102, 102, 102, 102,
            ]);

        let mut frc = FileRowCollection::new(Arc::new(file), record_size);
        let _ = frc.resize(0).unwrap();
        assert_eq!(frc.len().unwrap(), 0);
    }

    #[test]
    fn test_resize_grow() {
        // create a dataframe with a single (encoded) row
        let (file, _, record_size) =
            make_table_file_from_bytes("dataframes", "resize_grow", "quotes", make_columns(), &mut vec![
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 0,
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'R', b'I', b'C', b'E',
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E',
                0b1000_0000, 64, 96, 99, 102, 102, 102, 102, 102,
            ]);

        let mut frc = FileRowCollection::new(Arc::new(file), record_size);
        let _ = frc.resize(5).unwrap();
        assert_eq!(frc.len().unwrap(), 5);
    }
}
