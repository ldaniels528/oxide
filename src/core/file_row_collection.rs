////////////////////////////////////////////////////////////////////
// file row-collection module
////////////////////////////////////////////////////////////////////

use crate::dataframe_config::DataFrameConfig;
use crate::errors::Errors;
use crate::field_metadata::FieldMetadata;
use crate::machine::Machine;
use crate::model_row_collection::ModelRowCollection;
use crate::namespaces::Namespace;
use crate::outcomes::Outcomes;
use crate::platform::PlatformOps;
use crate::row_collection::RowCollection;
use crate::row_metadata::RowMetadata;
use crate::rows::Row;
use crate::table_columns::Column;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{ErrorValue, Outcome};
use serde::de::Error;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use shared_lib::fail;
use std::fmt::{Debug, Formatter};
use std::fs;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

/// File-based RowCollection implementation
#[derive(Clone)]
pub struct FileRowCollection {
    columns: Vec<Column>,
    file: Arc<File>,
    path: String,
    record_size: usize,
}

impl FileRowCollection {
    /// Creates a new table within the specified namespace and having the specified columns
    pub fn create_table(ns: &Namespace, columns: Vec<Column>) -> std::io::Result<Self> {
        let path = ns.get_table_file_path();
        let file = Arc::new(Self::table_file_create(ns)?);
        Ok(Self::new(columns, file, path.as_str()))
    }

    /// Encodes the [ByteRowCollection] into a byte vector
    pub fn encode(&self) -> Vec<u8> {
        let mut mrc = ModelRowCollection::new(self.columns.clone());
        for row in self.iter() {
            mrc.push_row(row);
        }
        mrc.encode()
    }

    pub fn get_related_filename(path: &str, extension: &str) -> (String, String) {
        let (oxide_home, untitled) = (Machine::oxide_home() , "untitled");
        let raw_file_path = Path::new(path);

        // determine the index_name and full_path
        let base_path = raw_file_path.parent()
            .map(|p| p.to_str().unwrap_or(oxide_home.as_str()))
            .unwrap_or(oxide_home.as_str());
        let table_filename = raw_file_path.file_name()
            .map(|p| p.to_str().unwrap_or(untitled))
            .unwrap_or(untitled);
        let index_filename = match table_filename.find(".") {
            Some(n) => format!("{}.{}", &table_filename[0..n], extension),
            None => format!("{}.{}", untitled, extension)
        };
        let full_path = format!("{}/{}", base_path, index_filename);
        (base_path.to_string(), full_path)
    }

    pub fn new(
        columns: Vec<Column>,
        file: Arc<File>,
        path: &str,
    ) -> Self {
        Self {
            record_size: Row::compute_record_size(&columns),
            columns,
            file,
            path: path.to_string(),
        }
    }

    pub fn open(ns: &Namespace) -> std::io::Result<Self> {
        Self::open_file(ns, Self::table_file_open(&ns)?)
    }

    fn open_file(ns: &Namespace, file: File) -> std::io::Result<Self> {
        let cfg = DataFrameConfig::load(&ns)?;
        let path = ns.get_table_file_path();
        let columns = Column::from_parameters(cfg.get_columns())?;
        Ok(Self::new(columns, Arc::new(file), path.as_str()))
    }

    pub fn open_or_create(ns: &Namespace) -> std::io::Result<Self> {
        match Self::table_file_open(&ns) {
            Ok(file) => Self::open_file(ns, file),
            Err(err) if err.to_string().starts_with("No such file") => {
                match Self::table_file_create(&ns) {
                    Ok(file) => {
                        let cfg = DataFrameConfig::new(
                            PlatformOps::get_oxide_history_parameters(), vec![], vec![]
                        );
                        cfg.save(&ns)?;
                        Self::open_file(ns, file)
                    },
                    Err(err) => fail(err.to_string())
                }
            }
            Err(err) => fail(err.to_string())
        }
    }

    /// convenience function to create, read or write a table file
    pub(crate) fn table_file_create(ns: &Namespace) -> std::io::Result<File> {
        fs::create_dir_all(ns.get_root_path())?;
        OpenOptions::new().truncate(true).create(true).read(true).write(true)
            .open(ns.get_table_file_path())
    }

    /// convenience function to read or write a table file
    pub(crate) fn table_file_open(ns: &Namespace) -> std::io::Result<File> {
        OpenOptions::new().read(true).write(true)
            .open(ns.get_table_file_path())
    }
}

impl Debug for FileRowCollection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FileRowCollection({})", self.record_size)
    }
}

impl RowCollection for FileRowCollection {
    fn create_related_structure(
        &self,
        columns: Vec<Column>,
        extension: &str,
    ) -> std::io::Result<Box<dyn RowCollection>> {
        // determine the base_path and full_path
        let path = self.path.as_str();
        let (base_path, full_path) =
            Self::get_related_filename(path, extension);

        // ensure the parent (base) directory exists
        fs::create_dir_all(base_path)?;

        // create and/or open the file
        let file = OpenOptions::new().truncate(true).create(true).read(true).write(true).open(full_path)?;
        let frc = Self::new(columns, Arc::new(file), path);
        Ok(Box::new(frc))
    }

    fn get_columns(&self) -> &Vec<Column> { &self.columns }

    fn get_rows(&self) -> Vec<Row> {
        self.iter().collect()
    }

    fn get_record_size(&self) -> usize { self.record_size }

    fn len(&self) -> std::io::Result<usize> {
        Ok((self.file.metadata()?.len() as usize) / self.record_size)
    }

    fn overwrite_field(
        &mut self,
        id: usize,
        column_id: usize,
        new_value: TypedValue,
    ) -> TypedValue {
        let column = &self.columns[column_id];
        let offset = self.convert_rowid_to_offset(id) + column.get_offset() as u64;
        let buffer = Row::encode_value(
            &new_value,
            &FieldMetadata::new(true),
            column.get_max_physical_size(),
        );
        match &self.file.write_at(&buffer, offset) {
            Ok(_) => TypedValue::Outcome(Outcomes::RowsAffected(1)),
            Err(err) => ErrorValue(Errors::Exact(err.to_string()))
        }
    }

    fn overwrite_field_metadata(
        &mut self,
        id: usize,
        column_id: usize,
        metadata: FieldMetadata,
    ) -> TypedValue {
        let row_offset = self.convert_rowid_to_offset(id);
        let column = &self.columns[column_id];
        let column_offset = column.get_offset() as u64;
        let offset = row_offset + column_offset;
        match &self.file.write_at(&[metadata.encode()], offset) {
            Ok(_) => TypedValue::Outcome(Outcomes::RowsAffected(1)),
            Err(err) => ErrorValue(Errors::Exact(err.to_string()))
        }
    }

    fn overwrite_row(&mut self, id: usize, row: Row) -> TypedValue {
        let offset = self.convert_rowid_to_offset(id);
        match &self.file.write_at(&row.encode(&self.columns), offset) {
            Ok(_) => TypedValue::Outcome(Outcomes::RowsAffected(1)),
            Err(err) => ErrorValue(Errors::Exact(err.to_string()))
        }
    }

    fn overwrite_row_metadata(&mut self, id: usize, metadata: RowMetadata) -> TypedValue {
        let offset = self.convert_rowid_to_offset(id);
        match &self.file.write_at(&[metadata.encode()], offset) {
            Ok(_) => TypedValue::Outcome(Outcomes::RowsAffected(1)),
            Err(err) => ErrorValue(Errors::Exact(err.to_string()))
        }
    }

    fn read_field(&self, id: usize, column_id: usize) -> TypedValue {
        let column = &self.columns[column_id];
        let row_offset = self.convert_rowid_to_offset(id);
        let mut buffer: Vec<u8> = vec![0; column.get_max_physical_size()];
        match &self.file.read_at(&mut buffer, row_offset + column.get_offset() as u64) {
            Ok(_) => {}
            Err(err) => return ErrorValue(Errors::Exact(err.to_string()))
        }
        Row::decode_value(&column.get_data_type(), &buffer, 0)
    }

    fn read_field_metadata(
        &self,
        id: usize,
        column_id: usize,
    ) -> std::io::Result<FieldMetadata> {
        let column = &self.columns[column_id];
        let row_offset = self.convert_rowid_to_offset(id);
        let offset = row_offset + column.get_offset() as u64;
        let mut buffer: Vec<u8> = vec![0u8; 1];
        let _ = &self.file.read_at(&mut buffer, offset)?;
        let meta = FieldMetadata::decode(buffer[0]);
        Ok(meta)
    }

    fn read_row(&self, id: usize) -> std::io::Result<(Row, RowMetadata)> {
        let offset = self.convert_rowid_to_offset(id);
        let mut buffer: Vec<u8> = vec![0; self.record_size];
        let _ = self.file.read_at(&mut buffer, offset)?;
        Ok(Row::decode(&buffer, &self.columns))
    }

    fn read_row_metadata(&self, id: usize) -> std::io::Result<RowMetadata> {
        let offset = self.convert_rowid_to_offset(id);
        let mut buffer: Vec<u8> = vec![0; 1];
        let _ = self.file.read_at(&mut buffer, offset)?;
        Ok(RowMetadata::decode(buffer[0]))
    }

    fn resize(&mut self, new_size: usize) -> TypedValue {
        let new_length = new_size as u64 * self.record_size as u64;
        match self.file.set_len(new_length) {
            Ok(..) => Outcome(Outcomes::Ack),
            Err(err) => ErrorValue(Errors::Exact(err.to_string()))
        }
    }
}

impl PartialEq for FileRowCollection {
    fn eq(&self, other: &Self) -> bool {
        self.columns == other.columns
            && self.path == other.path
            && self.record_size == other.record_size
    }
}

impl Serialize for FileRowCollection {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FileRowCollection", 2)?;
        state.serialize_field("columns", &self.columns)?;
        state.serialize_field("path", &self.path)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for FileRowCollection {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // define a helper struct for deserialization
        #[derive(Deserialize)]
        struct FileRowCollectionHelper {
            columns: Vec<Column>,
            path: String,
        }

        let helper = FileRowCollectionHelper::deserialize(deserializer)?;
        let file = File::open(&helper.path).map_err(D::Error::custom)?;
        Ok(FileRowCollection::new(helper.columns, Arc::new(file), helper.path.as_str()))
    }
}

#[cfg(test)]
mod tests {
    use crate::file_row_collection::FileRowCollection;
    use crate::namespaces::Namespace;
    use crate::row_collection::RowCollection;
    use crate::table_columns::Column;
    use crate::testdata::make_quote_parameters;

    #[test]
    fn test_get_columns() {
        let columns = make_quote_parameters();
        let phys_columns = Column::from_parameters(&columns).unwrap();
        let ns = Namespace::new("file_row_collection", "get_columns", "stocks");
        let frc = FileRowCollection::create_table(&ns, phys_columns.to_owned()).unwrap();
        assert_eq!(frc.get_columns().to_owned(), phys_columns)
    }
}
