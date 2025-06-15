#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// BLOB file row-collection module
////////////////////////////////////////////////////////////////////

use crate::blobs::{BLOBCellMetadata, BLOBStore};
use crate::columns::Column;
use crate::field::FieldMetadata;
use crate::model_row_collection::ModelRowCollection;
use crate::parameter::Parameter;
use crate::row_collection::{RowCollection, RowEncoding};
use crate::row_metadata::RowMetadata;
use crate::structures::Row;
use crate::typed_values::TypedValue;
use serde::de::Error;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};

/// BLOB File-based RowCollection implementation
#[derive(Clone)]
pub struct BLOBFileRowCollection {
    blobs: BLOBStore,
    columns: Vec<Column>,
    metadata: BLOBCellMetadata,
    record_size: usize,
}

impl BLOBFileRowCollection {
    /// Creates a new BLOB File Row Collection
    pub fn new(
        blobs: BLOBStore,
        parameters: &Vec<Parameter>,
        metadata: BLOBCellMetadata,
    ) -> Self {
        let columns = Column::from_parameters(parameters);
        let record_size = Row::compute_record_size(&columns);
        Self { blobs, columns, metadata, record_size }
    }

    fn read_sync<F, T>(
        &self,
        f: F,
    ) -> std::io::Result<T>
    where
        F: Fn(&ModelRowCollection) -> std::io::Result<T>,
    {
        let mrc = self.blobs.read_value(&self.metadata)?;
        f(&mrc)
    }

    fn write_sync<F, T>(
        &mut self,
        f: F,
    ) -> std::io::Result<T>
    where
        F: Fn(&mut ModelRowCollection) -> std::io::Result<T>,
    {
        let mut mrc = self.blobs.read_value(&self.metadata)?;
        let result = f(&mut mrc)?;
        self.metadata = self.blobs.update_value(&self.metadata, mrc)?;
        Ok(result)
    }
}

impl Eq for BLOBFileRowCollection {}

impl Ord for BLOBFileRowCollection {
    fn cmp(&self, other: &Self) -> Ordering {
        self.record_size.cmp(&other.record_size)
    }
}

impl PartialEq for BLOBFileRowCollection {
    fn eq(&self, other: &Self) -> bool {
        self.record_size == other.record_size
    }
}

impl PartialOrd for BLOBFileRowCollection {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.record_size.partial_cmp(&other.record_size)
    }
}

impl Debug for BLOBFileRowCollection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BLOBFileRowCollection({})", Column::render(self.get_columns()))
    }
}

impl Serialize for BLOBFileRowCollection {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("BLOBFileRowCollection", 3)?;
        state.serialize_field("path", &self.blobs.get_path())?;
        state.serialize_field("parameters", &self.get_parameters())?;
        state.serialize_field("metadata", &self.metadata)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for BLOBFileRowCollection {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // define a helper struct for deserialization
        #[derive(Deserialize)]
        struct FileRowCollectionHelper {
            path: String,
            parameters: Vec<Parameter>,
            metadata: BLOBCellMetadata,
        }

        let helper = FileRowCollectionHelper::deserialize(deserializer)?;
        let full_blob_path = format!("{}.blob", helper.path);
        let blobs = BLOBStore::open_file(full_blob_path.as_str(), true)
            .map_err(|e| D::Error::custom(e))?;
        Ok(BLOBFileRowCollection::new(blobs, &helper.parameters, helper.metadata))
    }
}

impl RowCollection for  BLOBFileRowCollection {
    fn get_columns(&self) -> &Vec<Column> {
        &self.columns
    }

    fn get_record_size(&self) -> usize {
        self.record_size
    }

    fn get_rows(&self) -> Vec<Row> {
        self.read_sync(|mrc| Ok(mrc.get_rows())).unwrap_or(vec![])
    }

    fn len(&self) -> std::io::Result<usize> {
        self.read_sync(|mrc| mrc.len())
    }

    fn overwrite_field(&mut self, id: usize, column_id: usize, new_value: TypedValue) -> std::io::Result<i64> {
        self.write_sync(|mut mrc| mrc.overwrite_field(id, column_id, new_value.to_owned()))
    }

    fn overwrite_field_metadata(&mut self, id: usize, column_id: usize, metadata: FieldMetadata) -> std::io::Result<i64> {
        self.write_sync(|mut mrc| mrc.overwrite_field_metadata(id, column_id, metadata))
    }

    fn overwrite_row(&mut self, id: usize, row: Row) -> std::io::Result<i64> {
        self.write_sync(|mut mrc| mrc.overwrite_row(id, row.to_owned()))
    }

    fn overwrite_row_metadata(&mut self, id: usize, metadata: RowMetadata) -> std::io::Result<i64> {
        self.write_sync(|mut mrc| mrc.overwrite_row_metadata(id, metadata))
    }

    fn read_field(&self, id: usize, column_id: usize) -> std::io::Result<TypedValue> {
        self.read_sync(|mrc| mrc.read_field(id, column_id))
    }

    fn read_field_metadata(&self, id: usize, column_id: usize) -> std::io::Result<FieldMetadata> {
        self.read_sync(|mrc| mrc.read_field_metadata(id, column_id))
    }

    fn read_row(&self, id: usize) -> std::io::Result<(Row, RowMetadata)> {
        self.read_sync(|mrc| mrc.read_row(id))
    }

    fn read_row_metadata(&self, id: usize) -> std::io::Result<RowMetadata> {
        self.read_sync(|mrc| mrc.read_row_metadata(id))
    }

    fn resize(&mut self, new_size: usize) -> std::io::Result<bool> {
        self.write_sync(|mut mrc| mrc.resize(new_size))
    }
}

/// Unit tests
#[cfg(test)]
mod tests {
    use crate::blob_file_row_collection::BLOBFileRowCollection;
    use crate::blobs::BLOBStore;
    use crate::dataframe::Dataframe::Blob;
    use crate::model_row_collection::ModelRowCollection;
    use crate::namespaces::Namespace;
    use crate::row_collection::RowCollection;
    use crate::table_renderer::TableRenderer;
    use crate::testdata::{make_quote, make_quote_parameters};

    #[test]
    fn test_read_sync() {
        // create a new BLOB store
        let ns = Namespace::new("blobs", "read_sync", "stocks");
        let bs = BLOBStore::open(&ns).unwrap();

        // create a table
        let params = make_quote_parameters();
        let mrc0 = ModelRowCollection::from_parameters_and_rows(&params, &vec![
            make_quote(0, "ABC", "AMEX", 12.33),
            make_quote(1, "UNO", "OTC", 0.2456),
            make_quote(2, "BIZ", "NYSE", 9.775),
        ]);

        // insert the table into the BLOB store
        let key0 = bs.insert_value(&mrc0).unwrap();
        let blob_rc = BLOBFileRowCollection::new(bs, &params, key0);
        let lines = TableRenderer::from_dataframe(&Blob(blob_rc));
        assert_eq!(lines, vec![
            "|-------------------------------|", 
            "| symbol | exchange | last_sale |", 
            "|-------------------------------|", 
            "| ABC    | AMEX     | 12.33     |", 
            "| UNO    | OTC      | 0.2456    |", 
            "| BIZ    | NYSE     | 9.775     |", 
            "|-------------------------------|"])
    }

    #[test]
    fn test_write_sync() {
        // create a new BLOB store
        let ns = Namespace::new("blobs", "write_sync", "stocks");
        let bs = BLOBStore::open(&ns).unwrap();

        // create a table
        let params = make_quote_parameters();
        let mrc0 = ModelRowCollection::from_parameters_and_rows(&params, &vec![
            make_quote(0, "ABC", "AMEX", 12.33),
            make_quote(1, "UNO", "OTC", 0.2456),
            make_quote(2, "BIZ", "NYSE", 9.775),
        ]);

        // insert the table into the BLOB store
        let key0 = bs.insert_value(&mrc0).unwrap();
        let mut blob_rc = BLOBFileRowCollection::new(bs, &params, key0);
        blob_rc.append_rows(vec![
            make_quote(3, "GOTO", "OTC", 0.1442),
            make_quote(4, "XYZ", "NYSE", 0.0289),
        ]).unwrap();
        let lines = TableRenderer::from_dataframe(&Blob(blob_rc));
        assert_eq!(lines, vec![
            "|-------------------------------|", 
            "| symbol | exchange | last_sale |", 
            "|-------------------------------|", 
            "| ABC    | AMEX     | 12.33     |", 
            "| UNO    | OTC      | 0.2456    |", 
            "| BIZ    | NYSE     | 9.775     |", 
            "| GOTO   | OTC      | 0.1442    |", 
            "| XYZ    | NYSE     | 0.0289    |", 
            "|-------------------------------|"])
    }
}