////////////////////////////////////////////////////////////////////
// dataframes
////////////////////////////////////////////////////////////////////

use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::size_of;

use serde::Serialize;

use crate::dataframe_config::DataFrameConfig;
use crate::iocost::IOCost;
use crate::namespaces::Namespace;
use crate::rows::Row;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue;

/*
 * DataFrame is a logical representation of table
 */
#[derive(Debug)]
pub struct DataFrame {
    pub ns: Namespace,
    pub columns: Vec<TableColumn>,
    pub config: DataFrameConfig,
    pub record_size: usize,
    file: File,
}

impl DataFrame {
    pub fn append(&self, row: Row) -> Result<IOCost, Box<dyn Error>> {
        Err("not yet implemented".into())
    }

    pub fn compute_record_size(columns: &Vec<TableColumn>) -> usize {
        1 + size_of::<usize>() + columns.iter().map(|c| c.max_physical_size()).sum::<usize>()
    }

    pub fn create(ns: Namespace, config: DataFrameConfig) -> Result<DataFrame, Box<dyn Error>> {
        // write the configuration file
        ns.write_config(&config)?;
        // open the file with read and write access
        let file: File = OpenOptions::new().create(true).read(true).write(true).open(ns.get_table_file_path())?;
        // return the dataframe
        Self::new(ns, config, file)
    }

    pub fn new(ns: Namespace, config: DataFrameConfig, file: File) -> Result<DataFrame, Box<dyn Error>> {
        // convert the logical columns to "realized" physical columns
        let columns: Vec<TableColumn> = TableColumn::from_columns(&config.columns)?;
        // compute the record size (+9 bytes for metadata and row ID)
        let record_size = Self::compute_record_size(&columns);
        Ok(DataFrame { ns, columns, config, record_size, file })
    }

    pub fn overwrite(&mut self, row: Row) -> Result<IOCost, Box<dyn Error>> {
        let capacity = self.record_size;
        let offset = (row.id * capacity) as u64;
        let _ = &self.file.seek(SeekFrom::Start(offset))?;
        let _ = &self.file.write_all(&row.encode())?;
        Ok(IOCost::insert(1, 0, capacity))
    }

    pub fn read_field(&mut self, id: usize, column_id: usize) -> Result<TypedValue, Box<dyn Error>> {
        todo!()
    }

    pub fn read_row(&mut self, id: usize) -> Result<(Row, IOCost), Box<dyn Error>> {
        let capacity = self.record_size;
        let offset: u64 = (id * capacity) as u64;
        let mut buffer: Vec<u8> = Vec::with_capacity(capacity);
        let _ = &self.file.seek(SeekFrom::Start(offset))?;
        let _ = &self.file.read_exact(&mut buffer)?;
        let row: Row = Row::decode(id, &buffer, self.columns.clone());
        let cost: IOCost = IOCost::read(1, self.record_size);
        Ok((row, cost))
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::data_types::DataType::{Float64Type, StringType};
    use crate::dataframes::DataFrame;
    use crate::iocost::IOCost;
    use crate::testdata::*;
    use crate::typed_values::TypedValue::{Float64Value, NullValue, StringValue};

    #[test]
    fn test_create_dataframe() {
        let df: DataFrame = create_test_data_frame().unwrap();
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
    fn test_append() {
        let mut df: DataFrame = create_test_data_frame().unwrap();
        let row = create_test_row();
        let cost: IOCost = df.append(row).unwrap();
        assert_eq!(cost.inserted, 1);
        assert_eq!(cost.bytes_written, df.record_size as usize);
    }

    #[test]
    fn test_read_row() {
        let mut df: DataFrame = create_test_data_frame().unwrap();
        let (row, cost) = df.read_row(0).unwrap();
        assert_eq!(row.id, 0);
        assert_eq!(row.metadata.is_allocated, true);
        assert_eq!(row.metadata.is_blob, false);
        assert_eq!(row.metadata.is_encrypted, false);
        assert_eq!(row.metadata.is_replicated, false);
        assert_eq!(row.fields[0].metadata.is_active, true);
        assert_eq!(row.fields[0].metadata.is_compressed, false);
        assert_eq!(row.fields[0].value, StringValue("MANA".to_string()));
        assert_eq!(row.fields[1].metadata.is_active, true);
        assert_eq!(row.fields[1].metadata.is_compressed, false);
        assert_eq!(row.fields[1].value, StringValue("YHWH".to_string()));
        assert_eq!(row.fields[2].metadata.is_active, true);
        assert_eq!(row.fields[2].metadata.is_compressed, false);
        assert_eq!(row.fields[2].value, Float64Value(78.35));
        assert_eq!(cost.inserted, 1);
        assert_eq!(cost.bytes_written, df.record_size as usize);
    }

    #[test]
    fn test_overwrite() {
        let mut df: DataFrame = create_test_data_frame().unwrap();
        let cost: IOCost = df.overwrite(create_test_row()).unwrap();
        assert_eq!(cost.inserted, 1);
        assert_eq!(cost.bytes_written, df.record_size as usize);
    }
}