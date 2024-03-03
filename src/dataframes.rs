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

/// DataFrame is a logical representation of table
#[derive(Debug)]
pub struct DataFrame {
    pub ns: Namespace,
    pub columns: Vec<TableColumn>,
    pub config: DataFrameConfig,
    pub record_size: usize,
    file: File,
}

impl DataFrame {
    pub fn append(&mut self, row: Row) -> Result<IOCost, Box<dyn Error>> {
        let file_len: u64 = (&self.file.metadata()?).len();
        let new_row_id = (file_len / self.record_size as u64) as usize;
        let new_row: Row = row.with_row_id(new_row_id);
        let offset: u64 = (new_row.id * self.record_size) as u64;
        let _ = &self.file.seek(SeekFrom::Start(offset))?;
        let _ = &self.file.write_all(&new_row.encode())?;
        Ok(IOCost::append(1, self.record_size))
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

    pub fn length(&self) -> Result<u64, Box<dyn Error>> {
        let m = self.file.metadata()?;
        Ok(m.len())
    }

    pub fn new(ns: Namespace, config: DataFrameConfig, file: File) -> Result<DataFrame, Box<dyn Error>> {
        // convert the logical columns to "realized" physical columns
        let columns: Vec<TableColumn> = TableColumn::from_columns(&config.columns)?;
        // compute the record size (+9 bytes for metadata and row ID)
        let record_size = Self::compute_record_size(&columns);
        Ok(DataFrame { ns, columns, config, record_size, file })
    }

    pub fn overwrite(&mut self, row: Row) -> Result<IOCost, Box<dyn Error>> {
        let offset: u64 = (row.id * self.record_size) as u64;
        let _ = &self.file.seek(SeekFrom::Start(offset))?;
        let _ = &self.file.write_all(&row.encode())?;
        Ok(IOCost::overwrite(1, self.record_size))
    }

    pub fn read_field(&mut self, id: usize, column_id: usize) -> Result<TypedValue, Box<dyn Error>> {
        todo!()
    }

    pub fn read_row(&mut self, id: usize) -> Result<(Row, IOCost), Box<dyn Error>> {
        let offset: u64 = (id * self.record_size) as u64;
        let mut buffer: Vec<u8> = vec![0; self.record_size];
        let _ = &self.file.seek(SeekFrom::Start(offset))?;
        let _ = &self.file.read_exact(&mut buffer)?;
        let row: Row = Row::decode(id, &buffer, self.columns.clone());
        let cost: IOCost = IOCost::read(1, self.record_size);
        Ok((row, cost))
    }

    pub fn read_rows(&mut self, from: usize, to: usize) -> Result<(Vec<Row>, IOCost), Box<dyn Error>> {
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

    pub fn size(&self) -> Result<usize, Box<dyn Error>> {
        Ok((self.file.metadata()?.len() as usize) / self.record_size)
    }

    pub fn resize(&mut self, new_size: usize) -> Result<IOCost, Box<dyn Error>> {
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
}

// Unit tests
#[cfg(test)]
mod tests {
    use std::io::{Seek, SeekFrom, Write};

    use crate::data_types::DataType::{Float64Type, StringType};
    use crate::dataframes::DataFrame;
    use crate::iocost::IOCost;
    use crate::rows::Row;
    use crate::testdata::*;
    use crate::typed_values::TypedValue::{Float64Value, NullValue, StringValue};

    #[test]
    fn test_append() {
        let mut df: DataFrame = create_test_dataframe("finance", "stocks", "quotes").unwrap();
        let row: Row = create_test_row();
        let cost: IOCost = df.append(row).unwrap();
        assert_eq!(cost.inserted, 1);
        assert_eq!(cost.bytes_written, df.record_size);
    }

    #[test]
    fn test_create_dataframe() {
        let df: DataFrame = create_test_dataframe("finance", "stocks", "quotes").unwrap();
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
        let mut df: DataFrame = create_test_dataframe("finance", "stocks", "quotes").unwrap();
        df.resize(0).unwrap();
        df.file.seek(SeekFrom::Start(0)).unwrap();
        df.file.write_all(&mut vec![
            0b1000_0000, 0xDE, 0xAD, 0xBA, 0xBE, 0xBE, 0xEF, 0xCA, 0xFE,
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'R', b'O', b'O', b'M',
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'K', b'I', b'N', b'G',
            0b1000_0000, 64, 83, 150, 102, 102, 102, 102, 102,
        ]).unwrap();
        df.file.flush().unwrap();

        // read the row
        let (row, cost) = df.read_row(0).unwrap();

        assert_eq!(row.id, 0xDEAD_BABE_BEEF_CAFE);
        assert_eq!(row.metadata.is_allocated, true);
        assert_eq!(row.metadata.is_blob, false);
        assert_eq!(row.metadata.is_encrypted, false);
        assert_eq!(row.metadata.is_replicated, false);
        assert_eq!(row.fields[0].metadata.is_active, true);
        assert_eq!(row.fields[0].metadata.is_compressed, false);
        assert_eq!(row.fields[0].value, StringValue("ROOM".to_string()));
        assert_eq!(row.fields[1].metadata.is_active, true);
        assert_eq!(row.fields[1].metadata.is_compressed, false);
        assert_eq!(row.fields[1].value, StringValue("KING".to_string()));
        assert_eq!(row.fields[2].metadata.is_active, true);
        assert_eq!(row.fields[2].metadata.is_compressed, false);
        assert_eq!(row.fields[2].value, Float64Value(78.35));
        assert_eq!(cost.scanned, 1);
        assert_eq!(cost.bytes_read, df.record_size);
    }

    #[test]
    fn test_read_rows() {
        let mut df: DataFrame = create_test_dataframe("finance", "stocks", "quotes").unwrap();
        let (rows, cost) = df.read_rows(0, 1).unwrap();
        println!("rows {:?}", rows);
        assert!(rows.len() >= 1);
    }

    #[test]
    fn test_resize() {
        let mut df: DataFrame = create_test_dataframe("finance", "stocks", "quotes").unwrap();
        let cost0 = df.resize(0).unwrap();
        println!("{:?}", cost0);
        let cost1 = df.resize(5).unwrap();
        println!("{:?}", cost1);
        assert_eq!(df.length().unwrap(), (5 * df.record_size) as u64);
    }

    #[test]
    fn test_overwrite() {
        let mut df: DataFrame = create_test_dataframe("finance", "stocks", "quotes").unwrap();
        let row: Row = create_test_row().with_row_id(1);
        let cost: IOCost = df.overwrite(row).unwrap();
        assert_eq!(cost.updated, 1);
        assert_eq!(cost.bytes_written, df.record_size);
    }
}