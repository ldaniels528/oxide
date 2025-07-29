#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// BLOBStore class
////////////////////////////////////////////////////////////////////

use crate::blob_file_row_collection::BLOBFileRowCollection;
use crate::byte_row_collection::ByteRowCollection;
use crate::columns::Column;
use crate::dataframe::Dataframe::ModelTable;
use crate::errors::throw;
use crate::errors::Errors::Exact;
use crate::field;
use crate::namespaces::Namespace;
use crate::parameter::Parameter;
use crate::row_collection::RowCollection;
use crate::typed_values::TypedValue;
use crate::utils::generate_uuid;
use num_traits::ToPrimitive;
use serde::de::Error;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use shared_lib::cnv_error;
use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::fs;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::sync::Arc;

const HEADER_LEN: usize = 40;

/// BLOB Store: Cell Metadata
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct BLOBMetadata {
    pub blob_id: u128,
    pub offset: u64,
    pub allocated: u64,
    pub used: u64,
}

impl BLOBMetadata {
    pub fn get_data_len(&self) -> u64 {
        self.used - HEADER_LEN as u64
    }
}

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
    pub fn create(ns: &Namespace) -> std::io::Result<Self> {
        fs::create_dir_all(ns.get_root_path())?;
        let bs = Self::open_file(ns.get_blob_file_path().as_str(), true)?;
        bs.truncate()?;
        Ok(bs)
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
                TypedValue::TableValue(ModelTable(mrc)) => self.insert_value(mrc)?,
                other => self.insert_value(other)?
            };
            encoded.push(field::ACTIVE_MASK | field::EXTERNAL_MASK);
            encoded.extend(key.offset.to_be_bytes());
        }
        encoded.resize(fixed_size, 0u8);
        Ok(encoded)
    }

    /// Scans the BLOBStore and returns all BLOBMetadata entries
    pub fn get_entries(&self) -> std::io::Result<Vec<BLOBMetadata>> {
        let mut metadata_list = Vec::new();
        let mut offset = 0u64;
        let file_len = self.file.metadata()?.len();

        while (offset + HEADER_LEN as u64) < file_len {
            // read the metadata at the offset
            let metadata = match self.read_metadata_by_offset(offset) {
                Ok(meta) => meta,
                Err(_) => break, // stop at first invalid/corrupt header
            };
            metadata_list.push(metadata.clone());

            // move to the next entry based on allocated space
            offset += metadata.allocated;
        }

        Ok(metadata_list)
    }

    pub fn get_path(&self) -> String {
        self.path.clone()
    }

    pub fn insert_bytes(&self, bytes: &Vec<u8>) -> std::io::Result<BLOBMetadata> {
        let limit = self.file.metadata()?.len();
        let offset = limit;
        let metadata = BLOBMetadata {
            blob_id: generate_uuid(),
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

    pub fn len(&self) -> std::io::Result<u64> {
        self.file.metadata().map(|m| m.len())
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

    pub fn read_blob_table_by_offset(
        &self,
        offset: u64,
        params: &Vec<Parameter>
    ) -> std::io::Result<BLOBFileRowCollection> {
        let metadata = self.read_metadata_by_offset(offset)?;
        self.read_blob_table(&metadata, params)
    }

    /// Reads a raw blob of data from the blob store
    pub fn read_bytes_by_offset(
        &self,
        offset: u64
    ) -> std::io::Result<(BLOBMetadata, Vec<u8>)> {
        // read the metadata
        let metadata = self.read_metadata_by_offset(offset)?;

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
    pub fn read_metadata_by_offset(&self, offset: u64) -> std::io::Result<BLOBMetadata> {
        let mut header_buf: Vec<u8> = vec![0u8; HEADER_LEN];
        let _ = self.file.read_at(&mut header_buf, offset)?;
        bincode::deserialize::<BLOBMetadata>(&header_buf)
            .map_err(|e| cnv_error!(e))
    }

    /// Reads the header at the offset from the blob store
    pub fn read_metadata_by_uuid(&self, uuid: u128) -> std::io::Result<Option<BLOBMetadata>> {
        Ok(self.get_entries()?.iter().find(|entry| entry.blob_id == uuid).map(|entry| entry.clone()))
    }

    pub fn read_partially(
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

    pub fn truncate(&self) -> std::io::Result<()> {
        self.file.set_len(0)
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

impl Serialize for BLOBStore {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("BLOBStore", 1)?;
        state.serialize_field("path", &self.path)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for BLOBStore {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // define a helper struct for deserialization
        #[derive(Deserialize)]
        struct BLOBStoreHelper {
            path: String,
        }

        let helper = BLOBStoreHelper::deserialize(deserializer)?;
        BLOBStore::open_file(helper.path.as_str(), false)
            .map_err(|e| D::Error::custom(e.to_string()))
    }
}

impl Eq for BLOBStore {}

impl Ord for BLOBStore {
    fn cmp(&self, other: &Self) -> Ordering {
        self.path.cmp(&other.path)
    }
}

impl PartialEq for BLOBStore {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
    }
}

impl PartialOrd for BLOBStore {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.path.partial_cmp(&other.path)
    }
}

impl Debug for BLOBStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BLOBStore({})", self.path)
    }
}

/// Unit tests
#[cfg(test)]
mod tests {
    use crate::blobs::BLOBStore;
    use crate::byte_row_collection::ByteRowCollection;
    use crate::interpreter::Interpreter;
    use crate::namespaces::Namespace;
    use crate::parameter::Parameter;
    use crate::test_util::{make_quote, make_quote_parameters};
    use crate::typed_values::TypedValue;

    fn make_binary_table() -> (ByteRowCollection, Vec<Parameter>) {
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
    
    fn make_blob_store(path: &str) -> BLOBStore {
        let ns = Namespace::parse(path).unwrap();
        let bs = BLOBStore::open(&ns).unwrap();
        println!("BLOBStore => {}", bs.get_path());
        bs
    }
    
    fn make_table_value() -> TypedValue {
        let mut interpreter = Interpreter::new();
        interpreter.evaluate(r#"
            |-------------------------------|
            | symbol | exchange | last_sale |
            |-------------------------------|
            | BOOM   | NYSE     | 113.76    |
            | ABC    | AMEX     | 24.98     |
            | JET    | NASDAQ   | 64.24     |
            |-------------------------------| 
        "#).unwrap()
    }

    /// BLOB metadata tests
    #[cfg(test)]
    mod blob_metadata_tests {
        use crate::blobs::{BLOBMetadata, HEADER_LEN};
        use crate::utils::generate_uuid;

        #[test]
        fn test_blob_metadata_encode() {
            let metadata = BLOBMetadata { blob_id: generate_uuid(), offset: 0, allocated: 38, used: 35 };
            let bytes = bincode::serialize(&metadata).unwrap();
            assert_eq!(bytes.len(), HEADER_LEN);

            let new_metadata = bincode::deserialize::<BLOBMetadata>(&bytes).unwrap();
            assert_eq!(metadata, new_metadata);
        }
    }
    
    /// BLOB Store tests
    #[cfg(test)]
    mod blob_store_tests {
        use super::*;
        use crate::blobs::BLOBMetadata;
        use crate::byte_code_compiler::ByteCodeCompiler;
        use crate::byte_row_collection::ByteRowCollection;
        use crate::data_types::DataType::{FixedSizeType, NumberType, StringType, TableType};
        use crate::dataframe::Dataframe::BinaryTable;
        use crate::model_row_collection::ModelRowCollection;
        use crate::number_kind::NumberKind::F64Kind;
        use crate::packages::blob_stores::metadata_to_table;
        use crate::row_collection::RowCollection;
        use crate::table_renderer::TableRenderer;
        use crate::test_util::make_lines_from_table;
        use crate::typed_values::TypedValue;
        use crate::typed_values::TypedValue::StringValue;

        #[test]
        fn test_bytes() {
            // create a new blob store
            let bs = make_blob_store("blobstore.raw.byte_data");
            bs.truncate().unwrap();

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
            let (block_metadata, block_message) = bs.read_bytes_by_offset(metadata.offset).unwrap();
            println!("block_metadata {:?}", block_metadata);
            println!("block_message {:?}", block_message);
            assert_eq!(message.to_vec(), block_message);
        }

        #[test]
        fn test_create_read_update_then_read() {
            // create a table value and put the table value in a BLOB
            let table_value0 = make_table_value();
            let bs = make_blob_store("blobs.update_then_read.stocks");
            bs.truncate().unwrap();
            let bmd = bs.insert_value(table_value0).unwrap();

            // read the blob and verify the content
            let table_value1: TypedValue = bs.read_value(&bmd).unwrap();
            assert_eq!(make_lines_from_table(table_value1.clone()), vec![
                "|-------------------------------|",
                "| symbol | exchange | last_sale |",
                "|-------------------------------|",
                "| BOOM   | NYSE     | 113.76    |",
                "| ABC    | AMEX     | 24.98     |",
                "| JET    | NASDAQ   | 64.24     |",
                "|-------------------------------|"]);

            // verify the content type
            assert_eq!(table_value1.get_type(), TableType(vec![
                Parameter::new("symbol", FixedSizeType(StringType.into(), 4)),
                Parameter::new("exchange", FixedSizeType(StringType.into(), 6)),
                Parameter::new("last_sale", NumberType(F64Kind)),
            ]));

            // update the blob
            let mut interpreter = Interpreter::new();
            let table_value2 = interpreter.evaluate(r#"
                |-------------------------------|
                | symbol | exchange | last_sale |
                |-------------------------------|
                | BOOM   | NYSE     | 113.76    |
                | ABC    | AMEX     | 24.98     |
                | JET    | NASDAQ   | 64.24     |
                | XYZ    | NYSE     | 11.22     |
                |-------------------------------|
            "#).unwrap();
            let bmd = bs.update_value(&bmd, table_value2).unwrap();

            // re-read it and verify the content
            let table_value3 = bs.read_value(&bmd).unwrap();
            assert_eq!(make_lines_from_table(table_value3), vec![
                "|-------------------------------|",
                "| symbol | exchange | last_sale |",
                "|-------------------------------|",
                "| BOOM   | NYSE     | 113.76    |",
                "| ABC    | AMEX     | 24.98     |",
                "| JET    | NASDAQ   | 64.24     |",
                "| XYZ    | NYSE     | 11.22     |",
                "|-------------------------------|"]);
        }

        #[test]
        fn test_raw_byte_table() {
            // create a new blob store
            let bs = make_blob_store("blobstore.raw.byte_table");
            bs.truncate().unwrap();

            // create a byte table
            let (brc0, params) = make_binary_table();
            println!("brc0.watermark {}", brc0.get_watermark());

            // write the byte table
            let bytes0 = ByteCodeCompiler::encode_df(&BinaryTable(brc0.clone()));
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
            let bs = make_blob_store("blobstore.bytes.tables");
            bs.truncate().unwrap();

            // create a byte table
            let (brc0, params) = make_binary_table();

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
            let bs = make_blob_store("blobstore.blob_tables.stocks");
            bs.truncate().unwrap();

            // create a table
            let (brc0, params) = make_binary_table();
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
        fn test_get_entries() {
            let bs = make_blob_store("blobstore.get_entries.byte_data");
            bs.truncate().unwrap();

            bs.insert_bytes(&b"Hello World".to_vec()).unwrap();
            bs.insert_bytes(&b"The little brown fox".to_vec()).unwrap();
            bs.insert_bytes(&b"joy and pain, sunshine and rain".to_vec()).unwrap();

            let entries = bs.get_entries().unwrap();
            for s in TableRenderer::from_dataframe(&metadata_to_table(&entries)) {
                println!("{}", s);
            }

            let mut new_entries = vec![];
            for (n, entry) in entries.iter().enumerate() {
                let mut new_entry = entry.clone();
                new_entry.blob_id = 1000 * (n + 1) as u128;
                new_entries.push(new_entry);
            }
            assert_eq!(new_entries, vec![
                BLOBMetadata { blob_id: 1000, offset: 0, allocated: 54, used: 51 },
                BLOBMetadata { blob_id: 2000, offset: 54, allocated: 66, used: 60 },
                BLOBMetadata { blob_id: 3000, offset: 120, allocated: 81, used: 71 }
            ]);
        }

        #[test]
        fn test_read_metadata_by_uuid() {
            let bs = make_blob_store("blobstore.read_metadata_by_uuid.byte_data");
            bs.truncate().unwrap();

            bs.insert_bytes(&b"Hello World".to_vec()).unwrap();
            bs.insert_bytes(&b"The little brown fox".to_vec()).unwrap();
            bs.insert_bytes(&b"joy and pain, sunshine and rain".to_vec()).unwrap();

            let entries = bs.get_entries().unwrap();
            for s in TableRenderer::from_dataframe(&metadata_to_table(&entries)) {
                println!("{}", s);
            }

            let metadata = bs.read_metadata_by_uuid(entries[1].blob_id).unwrap();
            assert_eq!(metadata, Some(BLOBMetadata {
                blob_id: entries[1].blob_id,
                offset: 54,
                allocated: 66,
                used: 60
            }));
        }

        #[test]
        fn test_values() {
            // create a new blob store
            let bs = make_blob_store("blobstore.crud.data");
            bs.truncate().unwrap();

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

    }

}