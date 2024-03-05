////////////////////////////////////////////////////////////////////
// row-collection module
////////////////////////////////////////////////////////////////////

use std::error::Error;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};

use crate::iocost::IOCost;
use crate::row_metadata::RowMetadata;

type DeviceError = Box<dyn Error>;

/// represents the underlying storage resource for the dataframe
pub trait RowCollection {
    /// returns the size of the underlying physical table
    fn length(&self) -> Result<u64, DeviceError>;

    fn next_row_id(&self) -> Result<usize, DeviceError>;

    fn overwrite(&mut self, id: usize, block: Vec<u8>) -> Result<IOCost, DeviceError>;

    fn read(&mut self, id: usize) -> Result<(Vec<u8>, IOCost), DeviceError>;

    fn read_field(&mut self, id: usize, column_offset: usize, max_physical_size: usize) -> Result<(Vec<u8>, IOCost), DeviceError>;

    fn read_rows(&mut self, from: usize, to: usize) -> Result<(Vec<Vec<u8>>, IOCost), DeviceError>;

    fn record_size(&self) -> usize;

    /// resizes the table
    fn resize(&mut self, new_size: usize) -> Result<IOCost, DeviceError>;

    /// returns the allocated sizes the table (in numbers of rows)
    fn size(&self) -> Result<usize, DeviceError>;
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
    fn length(&self) -> Result<u64, DeviceError> {
        Ok(self.file.metadata()?.len())
    }

    fn next_row_id(&self) -> Result<usize, DeviceError> {
        let file_len: u64 = (&self.file.metadata()?).len();
        let id: usize = (file_len / self.record_size as u64) as usize;
        Ok(id)
    }

    fn overwrite(&mut self, id: usize, block: Vec<u8>) -> Result<IOCost, DeviceError> {
        let offset: u64 = self.to_row_offset(id);
        let _ = &self.file.seek(SeekFrom::Start(offset))?;
        let _ = &self.file.write_all(&block)?;
        self.file.flush()?;
        Ok(IOCost::overwrite(1, self.record_size))
    }

    fn read(&mut self, id: usize) -> Result<(Vec<u8>, IOCost), DeviceError> {
        let offset: u64 = (id * self.record_size) as u64;
        let mut buffer: Vec<u8> = vec![0; self.record_size];
        let _ = &self.file.seek(SeekFrom::Start(offset))?;
        let _ = &self.file.read_exact(&mut buffer)?;
        let cost: IOCost = IOCost::read(1, self.record_size);
        Ok((buffer, cost))
    }

    fn read_field(&mut self, id: usize, column_offset: usize, column_max_size: usize) -> Result<(Vec<u8>, IOCost), DeviceError> {
        let row_offset: u64 = self.to_row_offset(id);
        let mut buffer: Vec<u8> = vec![0; column_max_size];
        let _ = &self.file.seek(SeekFrom::Start(row_offset + column_offset as u64))?;
        let _ = &self.file.read_exact(&mut buffer)?;
        let cost: IOCost = IOCost::read(1, self.record_size);
        Ok((buffer, cost))
    }

    fn read_rows(&mut self, from: usize, to: usize) -> Result<(Vec<Vec<u8>>, IOCost), DeviceError> {
        let mut rows: Vec<Vec<u8>> = Vec::with_capacity(to - from);
        let mut total_cost: IOCost = IOCost::new();
        for id in from..to {
            let (bytes, cost) = self.read(id)?;
            let metadata: RowMetadata = RowMetadata::decode(bytes[0]);
            if metadata.is_allocated {
                total_cost += cost;
                rows.push(bytes);
            }
        }

        Ok((rows, total_cost))
    }

    fn record_size(&self) -> usize {
        self.record_size
    }

    fn resize(&mut self, new_size: usize) -> Result<IOCost, DeviceError> {
        let new_length = new_size as u64 * self.record_size as u64;
        let old_length = self.file.metadata()?.len();
        // modify the file
        self.file.set_len(new_length)?;
        // return the cost
        let mut cost = IOCost::new();
        let shrinkage = (old_length as i64 - new_length as i64) / self.record_size as i64;
        if shrinkage > 0 {
            cost.deleted = shrinkage as usize;
        } else {
            cost.inserted = -shrinkage as usize;
        }
        Ok(cost)
    }

    /// returns the allocated sizes the table (in numbers of rows)
    fn size(&self) -> Result<usize, DeviceError> {
        Ok((self.file.metadata()?.len() as usize) / self.record_size)
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::codec;
    use crate::data_types::DataType::{Float64Type, StringType};
    use crate::field_metadata::FieldMetadata;
    use crate::fields::Field;
    use crate::iocost::IOCost;
    use crate::row_collection::{FileRowCollection, RowCollection};
    use crate::row_metadata::RowMetadata;
    use crate::rows::Row;
    use crate::table_columns::TableColumn;
    use crate::testdata::{make_row, make_table_file, make_table_file_from_bytes};
    use crate::typed_values::TypedValue;
    use crate::typed_values::TypedValue::{Float64Value, NullValue, StringValue};

    #[test]
    fn test_append_row_then_read_rows() {
        // create a dataframe with a single (encoded) row
        let (file, columns, record_size) = make_table_file_from_bytes("finance", "stocks", "quotes", &mut vec![
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
        let cost: IOCost = frc.overwrite(target_row_id, row_data).unwrap();
        assert_eq!(cost.updated, 1);
        assert_eq!(cost.bytes_written, frc.record_size());
        assert_eq!(frc.next_row_id().unwrap(), 2);

        // retrieve the rows (as binary)
        let (bytes, cost) = frc.read_rows(0, 2).unwrap();
        assert_eq!(bytes.len(), 2);
        assert_eq!(frc.size().unwrap(), 2);

        // decode and verify the rows
        let fmd: FieldMetadata = FieldMetadata::decode(0x80);
        let rows: Vec<Row> = Row::decode_rows(&columns, bytes);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], Row {
            id: 0,
            metadata: RowMetadata::decode(0x80),
            columns: vec![
                TableColumn::new("symbol", StringType(4), NullValue, 9),
                TableColumn::new("exchange", StringType(4), NullValue, 22),
                TableColumn::new("lastSale", Float64Type, NullValue, 35),
            ],
            fields: vec![
                Field::new(fmd.clone(), StringValue("RICE".into())),
                Field::new(fmd.clone(), StringValue("NYSE".into())),
                Field::new(fmd.clone(), Float64Value(78.35)),
            ],
        });
        assert_eq!(rows[1], Row {
            id: 1,
            metadata: RowMetadata::decode(0x80),
            columns: vec![
                TableColumn::new("symbol", StringType(4), NullValue, 9),
                TableColumn::new("exchange", StringType(4), NullValue, 22),
                TableColumn::new("lastSale", Float64Type, NullValue, 35),
            ],
            fields: vec![
                Field::new(fmd.clone(), StringValue("BEEF".into())),
                Field::new(fmd.clone(), StringValue("AMEX".into())),
                Field::new(fmd.clone(), Float64Value(100.0)),
            ],
        });
        assert_eq!(cost.scanned, 2);
        assert_eq!(cost.bytes_read, 2 * frc.record_size());
    }

    #[test]
    fn test_overwrite_row() {
        // create a new empty table file
        let (file, columns, record_size) =
            make_table_file("finance", "stocks", "quotes");
        file.set_len(0).unwrap();

        // create a new row
        let mut frc: FileRowCollection = FileRowCollection::new(record_size, file);
        let row: Row = make_row(2);
        let cost: IOCost = frc.overwrite(row.id, row.encode()).unwrap();
        assert_eq!(cost.updated, 1);
        assert_eq!(cost.bytes_written, record_size);

        // read and verify the row
        let (bytes, cost) = frc.read(row.id).unwrap();
        let new_row: Row = Row::decode(row.id, &bytes, &columns);
        let fmd: FieldMetadata = FieldMetadata::decode(0x80);
        assert_eq!(row, Row {
            id: 2,
            metadata: RowMetadata::decode(0x80),
            columns: vec![
                TableColumn::new("symbol", StringType(4), NullValue, 9),
                TableColumn::new("exchange", StringType(4), NullValue, 22),
                TableColumn::new("lastSale", Float64Type, NullValue, 35),
            ],
            fields: vec![
                Field::new(fmd.clone(), StringValue("AMD".into())),
                Field::new(fmd.clone(), StringValue("NYSE".into())),
                Field::new(fmd.clone(), Float64Value(78.35)),
            ],
        });
        assert_eq!(cost.scanned, 1);
        assert_eq!(cost.bytes_read, record_size);
    }

    #[test]
    fn test_read_field() {
        // create a dataframe with a single (encoded) row
        let (file, columns, record_size) = make_table_file_from_bytes("finance", "stocks", "quotes", &mut vec![
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 0,
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'F', b'A', b'C', b'T',
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E',
            0b1000_0000, 64, 83, 150, 102, 102, 102, 102, 102,
        ]);

        // read the first column
        let column: &TableColumn = &columns[0];
        let mut frc: FileRowCollection = FileRowCollection::new(record_size, file);
        let (buf, cost) = frc.read_field(0, column.offset, column.max_physical_size).unwrap();
        let value: TypedValue = TypedValue::decode(&column.data_type, &buf, 1);
        assert_eq!(value, StringValue("FACT".to_string()));
    }

    #[test]
    fn test_read_row() {
        // create a dataframe with a single (encoded) row
        let (file, columns, record_size) = make_table_file_from_bytes("finance", "stocks", "quotes", &mut vec![
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 5,
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'R', b'O', b'O', b'M',
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'A', b'M', b'E', b'X',
            0b1000_0000, 64, 83, 150, 102, 102, 102, 102, 102,
        ]);

        // read the row
        let mut frc: FileRowCollection = FileRowCollection::new(record_size, file);
        let (bytes, cost) = frc.read(0).unwrap();
        let row: Row = Row::decode(5, &bytes, &columns);

        // verify the row
        let fmd: FieldMetadata = FieldMetadata::decode(0x80);
        assert_eq!(row, Row {
            id: 5,
            metadata: RowMetadata::decode(0x80),
            columns: vec![
                TableColumn::new("symbol", StringType(4), NullValue, 9),
                TableColumn::new("exchange", StringType(4), NullValue, 22),
                TableColumn::new("lastSale", Float64Type, NullValue, 35),
            ],
            fields: vec![
                Field::new(fmd.clone(), StringValue("ROOM".into())),
                Field::new(fmd.clone(), StringValue("AMEX".into())),
                Field::new(fmd.clone(), Float64Value(78.35)),
            ],
        });
        assert_eq!(cost.scanned, 1);
        assert_eq!(cost.bytes_read, record_size);
    }

    #[test]
    fn test_resize_shrink() {
        // create a dataframe with a single (encoded) row
        let (file, _, record_size) = make_table_file_from_bytes("finance", "stocks", "quotes", &mut vec![
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
        let (file, _, record_size) = make_table_file_from_bytes("finance", "stocks", "quotes", &mut vec![
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 0,
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'R', b'I', b'C', b'E',
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E',
            0b1000_0000, 64, 83, 150, 102, 102, 102, 102, 102,
        ]);

        let mut frc: FileRowCollection = FileRowCollection::new(record_size, file);
        let _ = frc.resize(5).unwrap();
        assert_eq!(frc.size().unwrap(), 5);
        assert_eq!(frc.length().unwrap(), 5 * frc.record_size as u64);
    }
}