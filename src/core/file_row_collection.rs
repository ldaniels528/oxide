////////////////////////////////////////////////////////////////////
// file row-collection module
////////////////////////////////////////////////////////////////////

use std::fmt::{Debug, Formatter};
use std::fs;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::sync::Arc;

use crate::dataframe_config::DataFrameConfig;
use crate::fields::Field;
use crate::namespaces::Namespace;
use crate::row_collection::RowCollection;
use crate::row_metadata::RowMetadata;
use crate::rows::Row;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue;

/// File-based RowCollection implementation
#[derive(Clone)]
pub struct FileRowCollection {
    columns: Vec<TableColumn>,
    file: Arc<File>,
    record_size: usize,
}

impl FileRowCollection {
    pub fn create(ns: Namespace, columns: Vec<TableColumn>) -> std::io::Result<Self> {
        let file = Arc::new(Self::open_crw(&ns)?);
        Ok(Self::new(columns, file))
    }

    pub fn new(columns: Vec<TableColumn>, file: Arc<File>) -> Self {
        let record_size = Row::compute_record_size(&columns);
        Self { columns, file, record_size }
    }

    pub fn open(ns: &Namespace) -> std::io::Result<Self> {
        let cfg = DataFrameConfig::load(&ns)?;
        let columns = TableColumn::from_columns(cfg.get_columns())?;
        let file = Arc::new(Self::open_rw(&ns)?);
        Ok(Self::new(columns, file))
    }

    /// convenience function to create, read or write a table file
    pub(crate) fn open_crw(ns: &Namespace) -> std::io::Result<File> {
        fs::create_dir_all(ns.get_root_path())?;
        OpenOptions::new().truncate(true).create(true).read(true).write(true).open(ns.get_table_file_path())
    }

    /// convenience function to read or write a table file
    pub(crate) fn open_rw(ns: &Namespace) -> std::io::Result<File> {
        OpenOptions::new().read(true).write(true).open(ns.get_table_file_path())
    }
}

impl Debug for FileRowCollection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FileRowCollection({})", self.record_size)
    }
}

impl RowCollection for FileRowCollection {
    fn get_columns(&self) -> &Vec<TableColumn> { &self.columns }

    fn get_record_size(&self) -> usize { self.record_size }

    fn index_of(&self, item: &Row) -> Option<usize> {
        for id in 0..self.len().unwrap() {
            let (row, metadata) = self.read(id).unwrap();
            if metadata.is_allocated && &row == item { return Some(id); }
        }
        None
    }

    fn len(&self) -> std::io::Result<usize> {
        Ok((self.file.metadata()?.len() as usize) / self.record_size)
    }

    fn overwrite(&mut self, id: usize, row: &Row) -> std::io::Result<TypedValue> {
        let offset = self.to_row_offset(id);
        let _ = &self.file.write_at(&row.encode(), offset)?;
        Ok(TypedValue::Ack)
    }

    fn overwrite_metadata(&mut self, id: usize, metadata: &RowMetadata) -> std::io::Result<TypedValue> {
        let offset = self.to_row_offset(id);
        let _ = &self.file.write_at(&[metadata.encode()], offset)?;
        Ok(TypedValue::Ack)
    }

    fn read(&self, id: usize) -> std::io::Result<(Row, RowMetadata)> {
        let offset = (id * self.record_size) as u64;
        let mut buffer: Vec<u8> = vec![0; self.record_size];
        let _ = &self.file.read_at(&mut buffer, offset)?;
        Ok(Row::decode(&buffer, &self.columns))
    }

    fn read_field(&self, id: usize, column_id: usize) -> std::io::Result<TypedValue> {
        let column = &self.columns[column_id];
        let row_offset = self.to_row_offset(id);
        let mut buffer: Vec<u8> = vec![0; column.max_physical_size];
        let _ = &self.file.read_at(&mut buffer, row_offset + column.offset as u64)?;
        let field = Field::decode(&column.data_type, &buffer, 0);
        Ok(field.value)
    }

    fn read_range(&self, index: std::ops::Range<usize>) -> std::io::Result<Vec<Row>> {
        let mut rows: Vec<Row> = Vec::with_capacity(index.len());
        for id in index {
            let (row, metadata) = self.read(id)?;
            if metadata.is_allocated {
                rows.push(row);
            }
        }
        Ok(rows)
    }

    fn resize(&mut self, new_size: usize) -> std::io::Result<TypedValue> {
        let new_length = new_size as u64 * self.record_size as u64;
        // modify the file
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
        let frc = FileRowCollection::create(ns, phys_columns.clone()).unwrap();
        assert_eq!(frc.get_columns().clone(), phys_columns)
    }
}
