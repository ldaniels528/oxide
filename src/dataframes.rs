////////////////////////////////////////////////////////////////////
// dataframes module
////////////////////////////////////////////////////////////////////

use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};

use serde::Serialize;

use crate::dataframe_config::DataFrameConfig;
use crate::fields::Field;
use crate::iocost::IOCost;
use crate::namespaces::Namespace;
use crate::rows::Row;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue;

type DeviceError = Box<dyn Error>;

/// DataFrame is a logical representation of table
#[derive(Debug)]
pub struct DataFrame {
    pub(crate) ns: Namespace,
    pub(crate) columns: Vec<TableColumn>,
    pub(crate) config: DataFrameConfig,
    pub(crate) record_size: usize,
    pub(crate) file: File,
}

impl DataFrame {
    /// appends a new row to the table
    pub fn append(&mut self, row: Row) -> Result<IOCost, DeviceError> {
        let file_len: u64 = (&self.file.metadata()?).len();
        let new_row_id: usize = (file_len / self.record_size as u64) as usize;
        let new_row: Row = row.with_row_id(new_row_id);
        let offset: u64 = (new_row.id * self.record_size) as u64;
        let _ = &self.file.seek(SeekFrom::Start(offset))?;
        let _ = &self.file.write_all(&new_row.encode())?;
        self.file.flush()?;
        Ok(IOCost::append(1, self.record_size))
    }

    /// computes the total record size (in bytes)
    fn compute_record_size(columns: &Vec<TableColumn>) -> usize {
        Row::overhead() + columns.iter().map(|c| c.max_physical_size).sum::<usize>()
    }

    /// creates a new dataframe; persisting its configuration to disk.
    pub fn create(ns: Namespace, config: DataFrameConfig) -> Result<DataFrame, DeviceError> {
        // write the configuration file
        ns.write_config(&config)?;
        // open the file with read and write access
        let file: File = OpenOptions::new().create(true).read(true).write(true).open(ns.get_table_file_path())?;
        // return the dataframe
        Self::new(ns, config, file)
    }

    /// returns the size of the underlying physical table
    pub fn length(&self) -> Result<u64, DeviceError> {
        Ok(self.file.metadata()?.len())
    }

    /// creates a new dataframe.
    pub fn new(ns: Namespace, config: DataFrameConfig, file: File) -> Result<DataFrame, DeviceError> {
        // convert the logical columns to "realized" physical columns
        let columns: Vec<TableColumn> = TableColumn::from_columns(&config.columns)?;
        // compute the record size (+9 bytes for metadata and row ID)
        let record_size = Self::compute_record_size(&columns);
        Ok(DataFrame { ns, columns, config, record_size, file })
    }

    /// overwrites a specified row by ID
    pub fn overwrite(&mut self, row: Row) -> Result<IOCost, DeviceError> {
        let offset: u64 = (row.id * self.record_size) as u64;
        let _ = &self.file.seek(SeekFrom::Start(offset))?;
        let _ = &self.file.write_all(&row.encode())?;
        self.file.flush()?;
        Ok(IOCost::overwrite(1, self.record_size))
    }

    /// reads the specified field value from the specified row ID
    pub fn read_field(&mut self, id: usize, column_id: usize) -> Result<TypedValue, DeviceError> {
        let column: &TableColumn = &self.columns[column_id];
        let mut buffer: Vec<u8> = vec![0; column.max_physical_size];
        let row_offset: u64 = (id * self.record_size) as u64;
        let _ = &self.file.seek(SeekFrom::Start(row_offset + column.offset as u64))?;
        let _ = &self.file.read_exact(&mut buffer)?;
        let field: Field = Field::decode(&column.data_type, &buffer, 0);
        Ok(field.value)
    }

    /// reads a row by ID
    pub fn read_row(&mut self, id: usize) -> Result<(Row, IOCost), DeviceError> {
        let offset: u64 = (id * self.record_size) as u64;
        let mut buffer: Vec<u8> = vec![0; self.record_size];
        let _ = &self.file.seek(SeekFrom::Start(offset))?;
        let _ = &self.file.read_exact(&mut buffer)?;
        let row: Row = Row::decode(id, &buffer, &self.columns);
        let cost: IOCost = IOCost::read(1, self.record_size);
        Ok((row, cost))
    }

    /// reads a range of rows
    pub fn read_rows(&mut self, from: usize, to: usize) -> Result<(Vec<Row>, IOCost), DeviceError> {
        let mut rows: Vec<Row> = Vec::with_capacity(to - from);
        let mut total_cost: IOCost = IOCost::new();
        for id in from..to {
            let (row, cost) = self.read_row(id)?;
            if row.metadata.is_allocated {
                total_cost += cost;
                rows.push(row);
            }
        }

        Ok((rows, total_cost))
    }

    /// resizes the table
    pub fn resize(&mut self, new_size: usize) -> Result<IOCost, DeviceError> {
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
    pub fn size(&self) -> Result<usize, DeviceError> {
        Ok((self.file.metadata()?.len() as usize) / self.record_size)
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::data_types::DataType::{Float64Type, StringType};
    use crate::dataframes::DataFrame;
    use crate::field_metadata::FieldMetadata;
    use crate::fields::Field;
    use crate::iocost::IOCost;
    use crate::row_metadata::RowMetadata;
    use crate::rows::Row;
    use crate::table_columns::TableColumn;
    use crate::testdata::*;
    use crate::typed_values::TypedValue;
    use crate::typed_values::TypedValue::{Float64Value, NullValue, StringValue};

    #[test]
    fn test_append_row_then_read_rows() {
        // create a dataframe with a single (encoded) row
        let mut df: DataFrame = make_rows_from_bytes("finance", "stocks", "quotes", &mut vec![
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 0,
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'R', b'I', b'C', b'E',
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'P', b'I', b'P', b'E',
            0b1000_0000, 64, 83, 150, 102, 102, 102, 102, 102,
        ]).unwrap();

        // decode a second row and append it to the dataframe
        let columns: Vec<TableColumn> = make_table_columns();
        let row: Row = Row::decode(0, &vec![
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 1,
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'B', b'E', b'E', b'F',
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'C', b'A', b'K', b'E',
            0b1000_0000, 64, 89, 0, 0, 0, 0, 0, 0,
        ], &columns);
        let cost: IOCost = df.append(row).unwrap();
        assert_eq!(cost.inserted, 1);
        assert_eq!(cost.bytes_written, df.record_size);

        // verify the rows
        let (rows, cost) = df.read_rows(0, 2).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].id, 0);
        assert_eq!(rows[0].metadata.is_allocated, true);
        assert_eq!(rows[0].metadata.is_blob, false);
        assert_eq!(rows[0].metadata.is_encrypted, false);
        assert_eq!(rows[0].metadata.is_replicated, false);
        assert_eq!(rows[0].fields[0].metadata.is_active, true);
        assert_eq!(rows[0].fields[0].metadata.is_compressed, false);
        assert_eq!(rows[0].fields[0].value, StringValue("RICE".to_string()));
        assert_eq!(rows[0].fields[1].metadata.is_active, true);
        assert_eq!(rows[0].fields[1].metadata.is_compressed, false);
        assert_eq!(rows[0].fields[1].value, StringValue("PIPE".to_string()));
        assert_eq!(rows[0].fields[2].metadata.is_active, true);
        assert_eq!(rows[0].fields[2].metadata.is_compressed, false);
        assert_eq!(rows[0].fields[2].value, Float64Value(78.35));
        assert_eq!(rows[1].id, 1);
        assert_eq!(rows[1].metadata.is_allocated, true);
        assert_eq!(rows[1].metadata.is_blob, false);
        assert_eq!(rows[1].metadata.is_encrypted, false);
        assert_eq!(rows[1].metadata.is_replicated, false);
        assert_eq!(rows[1].fields[0].metadata.is_active, true);
        assert_eq!(rows[1].fields[0].metadata.is_compressed, false);
        assert_eq!(rows[1].fields[0].value, StringValue("BEEF".to_string()));
        assert_eq!(rows[1].fields[1].metadata.is_active, true);
        assert_eq!(rows[1].fields[1].metadata.is_compressed, false);
        assert_eq!(rows[1].fields[1].value, StringValue("CAKE".to_string()));
        assert_eq!(rows[1].fields[2].metadata.is_active, true);
        assert_eq!(rows[1].fields[2].metadata.is_compressed, false);
        assert_eq!(rows[1].fields[2].value, Float64Value(100.0));
        assert_eq!(cost.scanned, 2);
        assert_eq!(cost.bytes_read, 2 * df.record_size);
    }

    #[test]
    fn test_create_dataframe() {
        let df: DataFrame = make_dataframe("finance", "stocks", "quotes").unwrap();
        assert_eq!(df.ns.database, "finance");
        assert_eq!(df.ns.schema, "stocks");
        assert_eq!(df.ns.name, "quotes");
        assert_eq!(df.columns[0].name, "symbol");
        assert_eq!(df.columns[0].data_type, StringType(4));
        assert_eq!(df.columns[0].default_value, NullValue);
        assert_eq!(df.columns[1].name, "exchange");
        assert_eq!(df.columns[1].data_type, StringType(4));
        assert_eq!(df.columns[1].default_value, NullValue);
        assert_eq!(df.columns[2].name, "lastSale");
        assert_eq!(df.columns[2].data_type, Float64Type);
        assert_eq!(df.columns[2].default_value, NullValue);
    }

    #[test]
    fn test_read_row() {
        // create a dataframe with a single (encoded) row
        let mut df: DataFrame = make_rows_from_bytes("finance", "stocks", "quotes", &mut vec![
            0b1000_0000, 0xDE, 0xAD, 0xBA, 0xBE, 0xBE, 0xEF, 0xCA, 0xFE,
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'R', b'O', b'O', b'M',
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'K', b'I', b'N', b'G',
            0b1000_0000, 64, 83, 150, 102, 102, 102, 102, 102,
        ]).unwrap();

        // read the row
        let (row, cost) = df.read_row(0).unwrap();
        let fmd: FieldMetadata = FieldMetadata::decode(0x80);
        assert_eq!(row, Row {
            id: 0xDEAD_BABE_BEEF_CAFE,
            metadata: RowMetadata::decode(0x80),
            columns: vec![
                TableColumn::new("symbol", StringType(4), NullValue, 9),
                TableColumn::new("exchange", StringType(4), NullValue, 22),
                TableColumn::new("lastSale", Float64Type, NullValue, 35),
            ],
            fields: vec![
                Field::new(fmd.clone(), StringValue("ROOM".into())),
                Field::new(fmd.clone(), StringValue("KING".into())),
                Field::new(fmd.clone(), Float64Value(78.35)),
            ],
        });
        assert_eq!(cost.scanned, 1);
        assert_eq!(cost.bytes_read, df.record_size);
    }

    #[test]
    fn test_read_field() {
        // create a dataframe with a single (encoded) row
        let mut df: DataFrame = make_rows_from_bytes("finance", "stocks", "quotes", &mut vec![
            0b1000_0000, 0xDE, 0xAD, 0xBA, 0xBE, 0xBE, 0xEF, 0xCA, 0xFE,
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'R', b'O', b'O', b'M',
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'K', b'I', b'N', b'G',
            0b1000_0000, 64, 83, 150, 102, 102, 102, 102, 102,
        ]).unwrap();

        let value: TypedValue = df.read_field(0, 0).unwrap();
        assert_eq!(value, StringValue("ROOM".to_string()));
    }

    #[test]
    fn test_resize_table() {
        let mut df: DataFrame = make_dataframe("finance", "stocks", "quotes").unwrap();
        let _ = df.resize(0).unwrap();
        let cost1: IOCost = df.resize(5).unwrap();
        assert_eq!(df.length().unwrap(), (5 * df.record_size) as u64);
    }

    #[test]
    fn test_overwrite_row() {
        let mut df: DataFrame = make_dataframe("finance", "stocks", "quotes").unwrap();
        let row: Row = make_row(2);
        let cost: IOCost = df.overwrite(row).unwrap();
        assert_eq!(cost.updated, 1);
        assert_eq!(cost.bytes_written, df.record_size);
    }
}