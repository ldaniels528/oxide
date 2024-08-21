////////////////////////////////////////////////////////////////////
// file row-collection module
////////////////////////////////////////////////////////////////////

use std::fmt::{Debug, Formatter};
use std::fs;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

use crate::dataframe_config::DataFrameConfig;
use crate::field_metadata::FieldMetadata;
use crate::namespaces::Namespace;
use crate::row_collection::RowCollection;
use crate::row_metadata::RowMetadata;
use crate::rows::Row;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::ErrorValue;

/// File-based RowCollection implementation
#[derive(Clone)]
pub struct FileRowCollection {
    columns: Vec<TableColumn>,
    file: Arc<File>,
    path: String,
    record_size: usize,
}

impl FileRowCollection {
    /// Creates a new table within the specified namespace and having the specified columns
    pub fn create_table(ns: &Namespace, columns: Vec<TableColumn>) -> std::io::Result<Self> {
        let path = ns.get_table_file_path();
        let file = Arc::new(Self::table_file_create(ns)?);
        Ok(Self::new(columns, file, path.as_str()))
    }

    pub fn get_related_filename(path: &str, extension: &str) -> (String, String) {
        let (oxide_home, untitled) = (Namespace::oxide_home(), "untitled");
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
        columns: Vec<TableColumn>,
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
        let cfg = DataFrameConfig::load(&ns)?;
        let columns = TableColumn::from_columns(cfg.get_columns())?;
        let path = ns.get_table_file_path();
        let file = Arc::new(Self::table_file_open(&ns)?);
        Ok(Self::new(columns, file, path.as_str()))
    }

    pub fn open_path(path: &str) -> std::io::Result<Self> {
        Self::open(&Namespace::parse(path)?)
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
        columns: Vec<TableColumn>,
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

    fn get_columns(&self) -> &Vec<TableColumn> { &self.columns }

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
        let offset = self.convert_rowid_to_offset(id) + column.offset as u64;
        let buffer = Row::encode_value(
            &new_value,
            &FieldMetadata::new(true),
            column.max_physical_size,
        );
        match &self.file.write_at(&buffer, offset) {
            Ok(_) => TypedValue::RowsAffected(1),
            Err(err) => ErrorValue(err.to_string())
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
        let column_offset = column.offset as u64;
        let offset = row_offset + column_offset;
        match &self.file.write_at(&[metadata.encode()], offset) {
            Ok(_) => TypedValue::RowsAffected(1),
            Err(err) => ErrorValue(err.to_string())
        }
    }

    fn overwrite_row(&mut self, id: usize, row: Row) -> TypedValue {
        let offset = self.convert_rowid_to_offset(id);
        match &self.file.write_at(&row.encode(), offset) {
            Ok(_) => TypedValue::RowsAffected(1),
            Err(err) => ErrorValue(err.to_string())
        }
    }

    fn overwrite_row_metadata(&mut self, id: usize, metadata: RowMetadata) -> TypedValue {
        let offset = self.convert_rowid_to_offset(id);
        match &self.file.write_at(&[metadata.encode()], offset) {
            Ok(_) => TypedValue::RowsAffected(1),
            Err(err) => ErrorValue(err.to_string())
        }
    }

    fn read_field(&self, id: usize, column_id: usize) -> TypedValue {
        let column = &self.columns[column_id];
        let row_offset = self.convert_rowid_to_offset(id);
        let mut buffer: Vec<u8> = vec![0; column.max_physical_size];
        match &self.file.read_at(&mut buffer, row_offset + column.offset as u64) {
            Ok(_) => {}
            Err(err) => return ErrorValue(err.to_string())
        }
        Row::decode_value(&column.data_type, &buffer, 0)
    }

    fn read_field_metadata(
        &self,
        id: usize,
        column_id: usize,
    ) -> std::io::Result<FieldMetadata> {
        let column = &self.columns[column_id];
        let row_offset = self.convert_rowid_to_offset(id);
        let offset = row_offset + column.offset as u64;
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

    fn resize(&mut self, new_size: usize) -> std::io::Result<TypedValue> {
        let new_length = new_size as u64 * self.record_size as u64;
        self.file.set_len(new_length)?;
        Ok(TypedValue::Ack)
    }
}

#[cfg(test)]
mod tests {
    use crate::file_row_collection::FileRowCollection;
    use crate::namespaces::Namespace;
    use crate::row_collection::RowCollection;
    use crate::table_columns::TableColumn;
    use crate::testdata::make_quote_columns;

    #[test]
    fn test_get_columns() {
        let columns = make_quote_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let ns = Namespace::new("file_row_collection", "get_columns", "stocks");
        let frc = FileRowCollection::create_table(&ns, phys_columns.clone()).unwrap();
        assert_eq!(frc.get_columns().clone(), phys_columns)
    }
}
