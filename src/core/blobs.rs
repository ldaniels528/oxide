#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// BLOBStore class
////////////////////////////////////////////////////////////////////

use crate::blob_file_row_collection::BLOBFileRowCollection;
use crate::byte_row_collection::ByteRowCollection;
use crate::columns::Column;
use crate::dataframe::Dataframe::Model;
use crate::errors::throw;
use crate::errors::Errors::Exact;
use crate::field;
use crate::namespaces::Namespace;
use crate::parameter::Parameter;
use crate::row_collection::RowCollection;
use crate::typed_values::TypedValue;
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};
use shared_lib::cnv_error;
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
        let data_len_growth = data_len.to_f64().unwrap_or(0.) * 1.33;
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
            let key = match value {
                TypedValue::TableValue(Model(mrc)) => self.insert_value(mrc)?,
                other => self.insert_value(other)?
            };
            encoded.push(field::ACTIVE_MASK | field::EXTERNAL_MASK);
            encoded.extend(key.offset.to_be_bytes());
        }
        encoded.resize(fixed_size, 0u8);
        Ok(encoded)
    }

    pub fn get_path(&self) -> String {
        self.path.clone()
    }

    pub fn insert_bytes(&self, bytes: &Vec<u8>) -> std::io::Result<BLOBMetadata> {
        let limit = self.file.metadata()?.len();
        let offset = limit;
        let metadata = BLOBMetadata {
            offset,
            allocated: Self::compute_allocated_size(bytes.len()),
            used: (HEADER_LEN + bytes.len()) as u64,
        };
        let header_bytes = bincode::serialize(&metadata)
            .map_err(|e| cnv_error!(e))?;
        self.file.set_len(limit + metadata.allocated)?;
        let _ = self.file.write_at(&header_bytes, metadata.offset)?;
        let _ = self.file.write_at(&bytes, metadata.offset + header_bytes.len() as u64)?;
        Ok(metadata)
    }

    pub fn insert_byte_table(
        &self,
        brc: &ByteRowCollection
    ) -> std::io::Result<BLOBMetadata> {
        let bytes = brc.to_bytes();
        self.insert_bytes(&bytes)
    }

    pub fn insert_value<T>(&self, item: T) -> std::io::Result<BLOBMetadata>
    where
        T: serde::ser::Serialize,
    {
        match bincode::serialize(&item) {
            Ok(bytes) => self.insert_bytes(&bytes),
            Err(err) => throw(Exact(err.to_string()))
        }
    }
    
    pub fn read_blob_table(
        &self,
        metadata: &BLOBMetadata,
        params: &Vec<Parameter>
    ) -> std::io::Result<BLOBFileRowCollection> {
        Ok(BLOBFileRowCollection::new(
            self.clone(),
            params,
            metadata.clone()
        ))
    }

    pub fn read_blob_table_at(
        &self,
        offset: u64,
        params: &Vec<Parameter>
    ) -> std::io::Result<BLOBFileRowCollection> {
        let metadata = self.read_metadata(offset)?;
        self.read_blob_table(&metadata, params)
    }

    /// Reads a raw blob of data from the blob store
    pub fn read_block(
        &self,
        offset: u64
    ) -> std::io::Result<(BLOBMetadata, Vec<u8>)> {
        // read the metadata
        let metadata = self.read_metadata(offset)?;

        // read the byes indicated within the metadata
        let mut data: Vec<u8> = self.read_bytes(&metadata)?;
        Ok((metadata, data))
    }


    pub fn read_byte_table(
        &self,
        metadata: &BLOBMetadata,
        params: &Vec<Parameter>
    ) -> std::io::Result<ByteRowCollection> {
        let bytes = self.read_bytes(metadata)?;
        let columns = Column::from_parameters(params);
        let watermark = bytes.len();
        Ok(ByteRowCollection::decode(columns, bytes, watermark))
    }

    /// Reads a raw blob of data from the blob store
    pub fn read_bytes(&self, metadata: &BLOBMetadata) -> std::io::Result<Vec<u8>> {
        // read the byes indicated within the metadata
        let mut buffer: Vec<u8> = vec![0u8; metadata.get_data_len() as usize];
        let _ = self.file.read_at(&mut buffer, metadata.offset + HEADER_LEN as u64)?;
        Ok(buffer)
    }

    /// Reads a raw blob of data from the blob store
    pub fn read_bytes_used(&self, metadata: &BLOBMetadata) -> std::io::Result<Vec<u8>> {
        // read the byes indicated within the metadata
        let mut buffer: Vec<u8> = vec![0u8; metadata.used as usize];
        let _ = self.file.read_at(&mut buffer, metadata.offset + HEADER_LEN as u64)?;
        Ok(buffer)
    }
    
    /// Reads the header at the offset from the blob store
    pub fn read_metadata(&self, offset: u64) -> std::io::Result<BLOBMetadata> {
        let mut header_buf: Vec<u8> = vec![0u8; HEADER_LEN];
        let _ = self.file.read_at(&mut header_buf, offset)?;
        bincode::deserialize::<BLOBMetadata>(&header_buf)
            .map_err(|e| cnv_error!(e))
    }

    pub fn read_partial(
        &self,
        metadata: &BLOBMetadata,
        offset: u64,
        count: usize
    ) -> std::io::Result<Vec<u8>> {
        let mut buffer: Vec<u8> = vec![0u8; count];
        let result = self.file.read_at(&mut buffer, HEADER_LEN as u64 + metadata.offset + offset)
            .map(|_| buffer)?;
        Ok(result)
    }
    
    /// Reads an object of type [T] from the blob store
    pub fn read_value<T>(&self, metadata: &BLOBMetadata) -> std::io::Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let bytes = self.read_bytes_used(metadata)?;
        bincode::deserialize(&bytes).map_err(|e| cnv_error!(e))
    }

    pub fn update_bytes(
        &self,
        metadata: &BLOBMetadata,
        bytes: Vec<u8>,
    ) -> std::io::Result<BLOBMetadata> {
        // create a new header with the new amount used
        let mut new_header = metadata.clone();
        new_header.used = bytes.len() as u64;
        assert!(new_header.used <= new_header.allocated);

        // serialize the new header
        match bincode::serialize(&new_header) {
            Ok(header_bytes) => {
                // update the data
                let _ = self.file.write_at(&header_bytes, metadata.offset)?;
                let _ = self.file.write_at(&bytes, metadata.offset + header_bytes.len() as u64)?;
                Ok(new_header)
            }
            Err(err) => throw(Exact(err.to_string()))
        }
    }

    pub fn update_value<T>(
        &self,
        metadata: &BLOBMetadata,
        item: T,
    ) -> std::io::Result<BLOBMetadata>
    where
        T: serde::ser::Serialize,
    {
        match bincode::serialize(&item) {
            Ok(bytes) => self.update_bytes(metadata, bytes),
            Err(err) => throw(Exact(err.to_string()))
        }
    }

    pub fn write_partial(
        &self,
        metadata: &BLOBMetadata,
        offset: u64,
        bytes: &Vec<u8>
    ) -> std::io::Result<i64> {
        let _n_bytes = self.file.write_at(bytes.as_slice(), HEADER_LEN as u64 + metadata.offset + offset)?;
        Ok(1)
    }
}

pub const HEADER_LEN: usize = 24;

/// BLOB Store: Cell Metadata
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct BLOBMetadata {
    pub offset: u64,
    pub allocated: u64,
    pub used: u64,
}

impl BLOBMetadata {
    pub fn get_data_len(&self) -> u64 {
        self.used - HEADER_LEN as u64
    }
}

/// Unit tests
#[cfg(test)]
mod tests {
    use crate::blobs::BLOBStore;
    use crate::byte_code_compiler::ByteCodeCompiler;
    use crate::byte_row_collection::ByteRowCollection;
    use crate::dataframe::Dataframe::Binary;
    use crate::model_row_collection::ModelRowCollection;
    use crate::namespaces::Namespace;
    use crate::parameter::Parameter;
    use crate::row_collection::RowCollection;
    use crate::testdata::{make_quote, make_quote_parameters};
    use crate::typed_values::TypedValue;
    use crate::typed_values::TypedValue::StringValue;

    #[test]
    fn test_bytes() {
        // create a new blob store
        let ns = Namespace::new("blobs", "raw", "byte_data");
        let bs = BLOBStore::open(&ns).unwrap();
        println!("BLOBStore => {}", bs.get_path());
        
        // insert a message via insert_bytes(..)
        let message = b"This is a binary message";
        println!("message {:?}", message);
        let metadata = bs.insert_bytes(&message.to_vec()).unwrap();
        println!("metadata {:?}", metadata);

        // read a message as read_bytes(..)
        let bytes_message = bs.read_bytes(&metadata).unwrap();
        println!("bytes_message {:?}", bytes_message);
        assert_eq!(message.to_vec(), bytes_message);

        // read a message via read_block(..)
        let (block_metadata, block_message) = bs.read_block(metadata.offset).unwrap();
        println!("block_metadata {:?}", block_metadata);
        println!("block_message {:?}", block_message);
        assert_eq!(message.to_vec(), block_message);
    }

    #[test]
    fn test_raw_byte_table() {
        // create a new blob store
        let ns = Namespace::new("blobs", "raw", "byte_table");
        let bs = BLOBStore::open(&ns).unwrap();
        println!("BLOBStore => {}", bs.get_path());

        // create a byte table
        let (brc0, params) = create_binary_table();
        println!("brc0.watermark {}", brc0.get_watermark());

        // write the byte table
        let bytes0 = ByteCodeCompiler::encode_df(&Binary(brc0.clone()));
        println!("bytes0: {}", bytes0.len());
        let metadata0 = bs.insert_bytes(&bytes0).unwrap();
        println!("metadata0: {:?}", metadata0);

        // read the byte table
        let bytes1 = bs.read_bytes(&metadata0).unwrap();
        println!("bytes1: {}", bytes1.len());
        assert_eq!(bytes0, bytes1);
        
        let brc1 = ByteRowCollection::decode(
            brc0.get_columns().clone(),
            bytes1.clone(),
            bytes1.len()
        );
        assert_eq!(brc0.get_rows(), brc1.get_rows())
    }
    
    #[test]
    fn test_byte_tables() {
        // create a new blob store
        let ns = Namespace::new("blobs", "byte", "tables");
        let bs = BLOBStore::open(&ns).unwrap();
        println!("BLOBStore => {}", bs.get_path());
        
        // create a byte table
        let (brc0, params) = create_binary_table();

        // write the byte table
        let key0 = bs.insert_byte_table(&brc0).unwrap();
        println!("key0: {:?}", key0);
        
        // read the byte table
        let brc1 = bs.read_byte_table(&key0, &params).unwrap();
        assert_eq!(brc0.get_rows(), brc1.get_rows())
    }
    
    #[test]
    fn test_blob_tables() {
        // create a new blob store
        let ns = Namespace::new("blobs", "blob_tables", "stocks");
        let bs = BLOBStore::open(&ns).unwrap();
        println!("BLOBStore => {}", bs.get_path());
        
        // create a table
        let (brc0, params) = create_binary_table();
        let mrc = ModelRowCollection::from_columns_and_rows(
            brc0.get_columns(),
            &brc0.get_rows()
        );

        // insert a new blob table
        let key0 = bs.insert_value(&mrc).unwrap();
        println!("key0: {:?}", key0);

        // read the blob table
        let brc1 = bs.read_blob_table(&key0, &params).unwrap();
        assert_eq!(brc0.get_rows(), brc1.get_rows())
    }

    #[test]
    fn test_values() {
        // create a new blob store
        let ns = Namespace::new("blobs", "crud", "data");
        let bs = BLOBStore::open(&ns).unwrap();
        println!("BLOBStore => {}", bs.get_path());

        // insert a new blob
        let key0 = bs.insert_value(StringValue("Hello World".into())).unwrap();
        println!("key0: {:?}", key0);

        // read back the value by the key
        let value0 = bs.read_value::<TypedValue>(&key0).unwrap();
        println!("value0: {:?}", value0);
        assert_eq!(value0, StringValue("Hello World".into()));

        // next, update the value
        let key1 = bs.update_value(&key0, StringValue("The little brown fox ran down the road".into())).unwrap();
        println!("key1: {:?}", key1);

        // read back the value by the key
        let value1 = bs.read_value::<TypedValue>(&key1).unwrap();
        println!("value1: {:?}", value1);
        assert_eq!(value1, StringValue("The little brown fox ran down the road".into()));
    }
    
    fn create_binary_table() -> (ByteRowCollection, Vec<Parameter>) {
        let params = make_quote_parameters();
        let brc = ByteRowCollection::from_parameters_and_rows(&params, &vec![
            make_quote(0, "ABC", "AMEX", 12.33),
            make_quote(1, "UNO", "OTC", 0.2456),
            make_quote(2, "BIZ", "NYSE", 9.775),
            make_quote(3, "GOTO", "OTC", 0.1442),
            make_quote(4, "XYZ", "NYSE", 0.0289),
        ]);
        (brc, params)
    }
    
}