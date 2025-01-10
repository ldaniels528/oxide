#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// BLOBStore class
////////////////////////////////////////////////////////////////////

use crate::columns::Column;
use crate::field;
use crate::namespaces::Namespace;
use crate::typed_values::TypedValue;
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};
use shared_lib::fail;
use std::fs;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::sync::Arc;

/// BLOB Store
#[derive(Clone)]
pub struct BLOBStore {
    file: Arc<File>,
    path: String,
}

impl BLOBStore {

    ////////////////////////////////////////////////////////////////
    // static methods
    ////////////////////////////////////////////////////////////////

    fn compute_allocated_size(data_len: usize) -> u64 {
        let data_len_growth = data_len.to_f64().unwrap_or(0.) * 1.25;
        let data_len_growth = data_len_growth.to_u64().unwrap_or(data_len as u64);
        HEADER_LEN as u64 + data_len_growth
    }

    /// Opens a blob store by namespace
    pub fn open(ns: &Namespace) -> std::io::Result<Self> {
        fs::create_dir_all(ns.get_root_path())?;
        Self::open_file(ns.get_blob_file_path().as_str(), true)
    }

    /// Opens a blob store by file
    pub fn open_file(path: &str, create_if_not_exists: bool) -> std::io::Result<Self> {
        let file = if create_if_not_exists {
            OpenOptions::new().read(true).write(true).create(true).open(path)?
        } else { OpenOptions::new().read(true).write(true).open(path)? };
        Ok(Self {
            file: Arc::new(file),
            path: path.to_string(),
        })
    }

    ////////////////////////////////////////////////////////////////
    // instance methods
    ////////////////////////////////////////////////////////////////

    /// Encodes a binary field
    pub fn encode_field(
        &self,
        column: &Column,
        value: &TypedValue,
    ) -> std::io::Result<Vec<u8>> {
        let (data_type, fixed_size) = (column.get_data_type(), column.get_fixed_size());
        let buffer = data_type.encode(value)?;
        let mut encoded = Vec::with_capacity(fixed_size);
        if buffer.len() <= fixed_size {
            encoded.push(field::ACTIVE_MASK);
            encoded.extend(buffer);
        } else {
            let key = self.insert(value)?;
            encoded.push(field::ACTIVE_MASK | field::EXTERNAL_MASK);
            encoded.extend(key.offset.to_be_bytes());
        }
        encoded.resize(fixed_size, 0u8);
        Ok(encoded)
    }

    pub fn insert<T>(&self, item: T) -> std::io::Result<BLOBCellMetadata>
    where
        T: serde::ser::Serialize,
    {
        match bincode::serialize(&item) {
            Ok(bytes) => self.insert_blob(bytes),
            Err(err) => fail(err.to_string())
        }
    }

    pub fn insert_blob(&self, bytes: Vec<u8>) -> std::io::Result<BLOBCellMetadata> {
        let limit = self.file.metadata()?.len();
        let offset = limit;
        let header = BLOBCellMetadata {
            offset,
            allocated: Self::compute_allocated_size(bytes.len()),
            used: (HEADER_LEN + bytes.len()) as u64,
        };
        match bincode::serialize(&header) {
            Ok(header_bytes) => {
                self.file.set_len(limit + header.allocated)?;
                let _ = self.file.write_at(&header_bytes, header.offset)?;
                let _ = self.file.write_at(&bytes, header.offset + header_bytes.len() as u64)?;
                Ok(header)
            }
            Err(err) => fail(err.to_string())
        }
    }

    /// Reads an object of type [T] from the blob store
    pub fn read<T>(&self, offset: u64) -> std::io::Result<(BLOBCellMetadata, T)>
    where
        T: serde::de::DeserializeOwned,
    {
        let (header, bytes) = self.read_blob(offset)?;
        match bincode::deserialize(&bytes) {
            Ok(item) => Ok((header, item)),
            Err(err) => fail(err.to_string())
        }
    }

    /// Reads a raw blob of data from the blob store
    pub fn read_blob(&self, offset: u64) -> std::io::Result<(BLOBCellMetadata, Vec<u8>)> {
        // first, read the header
        let header = self.read_header(offset)?;

        // next, read the byes indicated within the header
        let mut buffer: Vec<u8> = vec![0u8; header.used as usize];
        let _ = self.file.read_at(&mut buffer, offset + HEADER_LEN as u64)?;
        Ok((header, buffer))
    }

    /// Reads the header at the offset from the blob store
    pub fn read_header(&self, offset: u64) -> std::io::Result<BLOBCellMetadata> {
        let mut header_buf: Vec<u8> = vec![0u8; HEADER_LEN];
        let _ = self.file.read_at(&mut header_buf, offset)?;
        match bincode::deserialize::<BLOBCellMetadata>(&header_buf) {
            Ok(header) => Ok(header),
            Err(err) => fail(err.to_string())
        }
    }

    pub fn update<T>(
        &self,
        offset: u64,
        item: T,
    ) -> std::io::Result<BLOBCellMetadata>
    where
        T: serde::ser::Serialize,
    {
        match bincode::serialize(&item) {
            Ok(bytes) => self.update_blob(offset, bytes),
            Err(err) => fail(err.to_string())
        }
    }

    pub fn update_blob(
        &self,
        offset: u64,
        bytes: Vec<u8>,
    ) -> std::io::Result<BLOBCellMetadata> {
        // read the header
        let header = self.read_header(offset)?;

        // create a new header with the new amount used
        let mut new_header = header.clone();
        new_header.used = bytes.len() as u64;
        assert!(new_header.used <= new_header.allocated);

        // serialize the new header
        match bincode::serialize(&new_header) {
            Ok(header_bytes) => {
                // update the data
                let _ = self.file.write_at(&header_bytes, header.offset)?;
                let _ = self.file.write_at(&bytes, header.offset + header_bytes.len() as u64)?;
                Ok(header)
            }
            Err(err) => fail(err.to_string())
        }
    }
}

pub const HEADER_LEN: usize = 24;

/// BLOB Store: Cell Metadata
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct BLOBCellMetadata {
    pub offset: u64,
    pub allocated: u64,
    pub used: u64,
}

impl BLOBCellMetadata {
    pub fn new(
        offset: u64,
        allocated: u64,
        used: u64,
    ) -> Self {
        Self {
            offset,
            allocated,
            used,
        }
    }
}


/// Unit tests
#[cfg(test)]
mod tests {
    use crate::blobs::BLOBStore;
    use crate::namespaces::Namespace;
    use crate::typed_values::TypedValue;
    use crate::typed_values::TypedValue::StringValue;

    #[test]
    fn test_crud() {
        // create a new blob store
        let ns = Namespace::new("blobs", "crud", "data");
        let bs = BLOBStore::open(&ns).unwrap();

        // insert a new blob
        let key = bs.insert(StringValue("Hello World".into())).unwrap();
        println!("key0: {:?}", key);

        // read back the value by the key
        let (key1, value) = bs.read::<TypedValue>(key.offset).unwrap();
        println!("key1: {:?}", key1);
        println!("value1: {:?}", value);
        assert_eq!(value, StringValue("Hello World".into()));

        // next, update the value
        let key2 = bs.update(key1.offset, StringValue("Goodbye World".into())).unwrap();
        println!("key2: {:?}", key2);

        // read back the value by the key
        let (key3, value) = bs.read::<TypedValue>(key2.offset).unwrap();
        println!("key3: {:?}", key3);
        println!("value3: {:?}", value);
        assert_eq!(value, StringValue("Goodbye World".into()));
    }
}