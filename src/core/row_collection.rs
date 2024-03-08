////////////////////////////////////////////////////////////////////
// row-collection module
////////////////////////////////////////////////////////////////////

use std::error::Error;
use std::fs::File;
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;

use crate::row_metadata::RowMetadata;

/// represents the underlying storage resource for the dataframe
pub trait RowCollection {
    /// returns the size of the underlying physical table
    fn length(&self) -> io::Result<u64>;

    fn next_row_id(&self) -> io::Result<usize>;

    fn overwrite(&mut self, id: usize, block: Vec<u8>) -> io::Result<usize>;

    fn read(&self, id: usize) -> io::Result<Vec<u8>>;

    fn read_field(&self, id: usize, column_offset: usize, max_physical_size: usize) -> io::Result<Vec<u8>>;

    fn read_rows(&self, from: usize, to: usize) -> io::Result<Vec<Vec<u8>>>;

    fn record_size(&self) -> usize;

    /// resizes the table
    fn resize(&mut self, new_size: usize) -> io::Result<()>;

    /// returns the allocated sizes the table (in numbers of rows)
    fn size(&self) -> io::Result<usize>;
}

impl dyn RowCollection {
    pub fn from_file(record_size: usize, file: File) -> impl RowCollection {
        FileRowCollection::new(record_size, file)
    }
}

#[derive(Debug)]
pub struct FileRowCollection {
    pub(crate) record_size: usize,
    pub(crate) file: File,
}

impl FileRowCollection {
    pub fn new(record_size: usize, file: File) -> FileRowCollection {
        FileRowCollection { record_size, file }
    }

    pub fn to_row_offset(&self, id: usize) -> u64 {
        (id as u64) * (self.record_size as u64)
    }
}

impl RowCollection for FileRowCollection {
    fn length(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn next_row_id(&self) -> io::Result<usize> {
        let file_len: u64 = (&self.file.metadata()?).len();
        let id: usize = (file_len / self.record_size as u64) as usize;
        Ok(id)
    }

    fn overwrite(&mut self, id: usize, block: Vec<u8>) -> io::Result<usize> {
        let offset: u64 = self.to_row_offset(id);
        let _ = &self.file.seek(SeekFrom::Start(offset))?;
        let _ = &self.file.write_all(&block)?;
        self.file.flush()?;
        Ok(1)
    }

    fn read(&self, id: usize) -> io::Result<Vec<u8>> {
        let offset: u64 = (id * self.record_size) as u64;
        let mut buffer: Vec<u8> = vec![0; self.record_size];
        let _ = &self.file.read_at(&mut buffer, offset)?;
        Ok(buffer)
    }

    fn read_field(&self, id: usize, column_offset: usize, column_max_size: usize) -> io::Result<Vec<u8>> {
        let row_offset: u64 = self.to_row_offset(id);
        let mut buffer: Vec<u8> = vec![0; column_max_size];
        let _ = &self.file.read_at(&mut buffer, row_offset + column_offset as u64)?;
        Ok(buffer)
    }

    fn read_rows(&self, from: usize, to: usize) -> io::Result<Vec<Vec<u8>>> {
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

    fn record_size(&self) -> usize {
        self.record_size
    }

    fn resize(&mut self, new_size: usize) -> io::Result<()> {
        let new_length = new_size as u64 * self.record_size as u64;
        // modify the file
        self.file.set_len(new_length)?;
        self.file.flush()?;
        Ok(())
    }

    /// returns the allocated sizes the table (in numbers of rows)
    fn size(&self) -> io::Result<usize> {
        Ok((self.file.metadata()?.len() as usize) / self.record_size)
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::codec;
    use crate::data_types::DataType::{Float64Type, StringType};
    use crate::fields::Field;
    use crate::row_collection::{FileRowCollection, RowCollection};
    use crate::rows::Row;
    use crate::table_columns::TableColumn;
    use crate::testdata::{make_columns, make_row_from_fields, make_table_file, make_table_file_from_bytes};
    use crate::typed_values::TypedValue;
    use crate::typed_values::TypedValue::{Float64Value, NullValue, StringValue};

    #[test]
    fn test_append_row_then_read_rows() {
        // create a dataframe with a single (encoded) row
        let (file, columns, record_size) =
            make_table_file_from_bytes("finance", "stocks", "quotes", make_columns(), &mut vec![
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
        let mut frc = <dyn RowCollection>::from_file(record_size, file);
        assert_eq!(frc.overwrite(target_row_id, row_data).unwrap(), 1);
        assert_eq!(frc.next_row_id().unwrap(), 2);

        // retrieve the rows (as binary)
        let bytes = frc.read_rows(0, 2).unwrap();
        assert_eq!(bytes.len(), 2);
        assert_eq!(frc.size().unwrap(), 2);

        // decode and verify the rows
        let rows: Vec<Row> = Row::decode_rows(&columns, bytes);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], Row {
            id: 0,
            columns: vec![
                TableColumn::new("symbol", StringType(4), NullValue, 9),
                TableColumn::new("exchange", StringType(4), NullValue, 22),
                TableColumn::new("lastSale", Float64Type, NullValue, 35),
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
                TableColumn::new("symbol", StringType(4), NullValue, 9),
                TableColumn::new("exchange", StringType(4), NullValue, 22),
                TableColumn::new("lastSale", Float64Type, NullValue, 35),
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
        file.set_len(0).unwrap();

        // create a new row
        let mut frc: FileRowCollection = FileRowCollection::new(record_size, file);
        let row: Row = make_row_from_fields(2, vec![
            Field::with_value(StringValue("AMD".into())),
            Field::with_value(StringValue("NYSE".into())),
            Field::with_value(Float64Value(88.78)),
        ]);
        assert_eq!(frc.overwrite(row.id, row.encode()).unwrap(), 1);

        // read and verify the row
        let bytes = frc.read(row.id).unwrap();
        let (new_row, _) = Row::decode(&bytes, &columns);
        assert_eq!(row, Row {
            id: 2,
            columns: vec![
                TableColumn::new("symbol", StringType(4), NullValue, 9),
                TableColumn::new("exchange", StringType(4), NullValue, 22),
                TableColumn::new("lastSale", Float64Type, NullValue, 35),
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
            make_table_file_from_bytes("finance", "stocks", "quotes", make_columns(), &mut vec![
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 0,
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'F', b'A', b'C', b'T',
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E',
                0b1000_0000, 64, 83, 150, 102, 102, 102, 102, 102,
            ]);

        // read the first column
        let column: &TableColumn = &columns[0];
        let mut frc: FileRowCollection = FileRowCollection::new(record_size, file);
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
        let mut frc: FileRowCollection = FileRowCollection::new(record_size, file);
        let bytes = frc.read(0).unwrap();
        let (row, metadata) = Row::decode(&bytes, &columns);

        // verify the row
        assert!(metadata.is_allocated);
        assert_eq!(row, Row {
            id: 5,
            columns: vec![
                TableColumn::new("symbol", StringType(4), NullValue, 9),
                TableColumn::new("exchange", StringType(4), NullValue, 22),
                TableColumn::new("lastSale", Float64Type, NullValue, 35),
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
            make_table_file_from_bytes("dataframes", "resize", "quotes", make_columns(), &mut vec![
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 0,
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'R', b'I', b'C', b'E',
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E',
                0b1000_0000, 64, 83, 150, 102, 102, 102, 102, 102,
            ]);

        let mut frc: FileRowCollection = FileRowCollection::new(record_size, file);
        let _ = frc.resize(0).unwrap();
        assert_eq!(frc.length().unwrap(), 0);
    }

    #[test]
    fn test_resize_grow() {
        // create a dataframe with a single (encoded) row
        let (file, _, record_size) =
            make_table_file_from_bytes("dataframes", "resize", "quotes", make_columns(), &mut vec![
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 0,
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'R', b'I', b'C', b'E',
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E',
                0b1000_0000, 64, 96, 99, 102, 102, 102, 102, 102,
            ]);

        let mut frc: FileRowCollection = FileRowCollection::new(record_size, file);
        let _ = frc.resize(5).unwrap();
        assert_eq!(frc.size().unwrap(), 5);
        assert_eq!(frc.length().unwrap(), 5 * frc.record_size as u64);
    }
}