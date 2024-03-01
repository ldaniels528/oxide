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

/*
 * DataFrame is a logical representation of table
 */
#[derive(Debug)]
pub struct DataFrame {
    pub ns: Namespace,
    pub columns: Vec<TableColumn>,
    pub config: DataFrameConfig,
    pub record_size: u64,
    file: File,
}

impl DataFrame {
    pub fn append_row(&self, row: Row) -> Result<IOCost, Box<dyn Error>> {
        Err("not yet implemented".into())
    }

    pub fn create(ns: Namespace,
                  columns: Vec<TableColumn>,
                  config: DataFrameConfig) -> Result<DataFrame, Box<dyn Error>> {
        // write the configuration file
        ns.write_config(&config)?;
        // open the file with read and write access
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(ns.get_table_file_path())?;
        // compute the record size (+9 bytes for metadata and row ID)
        let record_size = (1 + size_of::<usize>() + columns.iter().map(|c| c.max_physical_size()).sum::<usize>()) as u64;
        // return the dataframe
        Ok(DataFrame::new(ns, columns, config, record_size, file))
    }

    pub fn new(ns: Namespace,
               columns: Vec<TableColumn>,
               config: DataFrameConfig,
               record_size: u64,
               file: File) -> DataFrame {
        DataFrame {
            ns,
            columns,
            config,
            record_size,
            file,
        }
    }

    pub fn overwrite_row(&mut self, row: Row) -> Result<IOCost, Box<dyn Error>> {
        let capacity = self.record_size;
        let offset = row.id * capacity;
        let _ = &self.file.seek(SeekFrom::Start(offset))?;
        let _ = &self.file.write_all(&row.encode())?;
        Ok(IOCost {
            altered: 0,
            created: 0,
            destroyed: 0,
            deleted: 0,
            inserted: 0,
            matched: 0,
            scanned: 0,
            shuffled: 0,
            updated: 1,
            bytes_read: 0,
            bytes_written: capacity as u32,
        })
    }

    pub fn read_row(&mut self, id: u64) -> Result<Row, Box<dyn Error>> {
        let capacity = self.record_size;
        let offset = id * capacity;
        let mut buffer: Vec<u8> = Vec::with_capacity(capacity as usize);
        let _ = &self.file.seek(SeekFrom::Start(offset))?;
        let _ = &self.file.read_exact(&mut buffer)?;
        Ok(Row::decode(id, &buffer, self.columns.clone()))
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    #[test]
    fn test_create() {}

    #[test]
    fn test_append_row() {}

    #[test]
    fn test_read_row() {}

    #[test]
    fn test_overwrite_row() {}
}