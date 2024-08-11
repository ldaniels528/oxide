////////////////////////////////////////////////////////////////////
// file column row-collection module
////////////////////////////////////////////////////////////////////

use std::fmt::{Debug, Formatter};
use std::os::unix::fs::FileExt;

use crate::file_row_collection::FileRowCollection;
use crate::row_collection::RowCollection;
use crate::row_metadata::RowMetadata;
use crate::rows::Row;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue;

/// File-based Column-Embedded [RowCollection] implementation
#[derive(Clone)]
pub struct FileEmbeddedRowCollection {
    frc: FileRowCollection,
    embedded_row_id: usize,
    embedded_column_id: usize,
    columns: Vec<TableColumn>,
    record_size: usize,
}

impl FileEmbeddedRowCollection {
    ////////////////////////////////////////////////////////////////
    //  STATIC METHODS
    ////////////////////////////////////////////////////////////////

    pub fn new(
        frc: FileRowCollection,
        embedded_row_id: usize,
        embedded_column_id: usize,
        columns: Vec<TableColumn>,
    ) -> Self {
        let record_size = Row::compute_record_size(&columns);
        Self { frc, embedded_row_id, embedded_column_id, columns, record_size }
    }

    ////////////////////////////////////////////////////////////////
    //  INSTANCE METHODS
    ////////////////////////////////////////////////////////////////

    /// Retrieves the column-embedded table
    pub fn get_embedded_table(&self) -> std::io::Result<Box<dyn RowCollection>> {
        self.frc.read_field(self.embedded_row_id, self.embedded_column_id)?.to_table()
    }
}

impl Debug for FileEmbeddedRowCollection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FileColumnRowCollection({:?}, {}, {})", self.frc, self.embedded_row_id, self.embedded_column_id)
    }
}

impl RowCollection for FileEmbeddedRowCollection {
    fn get_columns(&self) -> &Vec<TableColumn> {
        &self.columns
    }

    fn get_record_size(&self) -> usize {
        self.record_size
    }

    fn len(&self) -> std::io::Result<usize> {
        self.get_embedded_table()?.len()
    }

    fn overwrite_row(&mut self, id: usize, row: Row) -> std::io::Result<TypedValue> {
        self.get_embedded_table()?.overwrite_row(id, row)
    }

    fn overwrite_field(
        &mut self,
        id: usize,
        column_id: usize,
        new_value: TypedValue
    ) -> std::io::Result<TypedValue> {
        self.get_embedded_table()?.overwrite_field(id, column_id, new_value)
    }

    fn overwrite_row_metadata(&mut self, id: usize, metadata: RowMetadata) -> std::io::Result<TypedValue> {
        self.get_embedded_table()?.overwrite_row_metadata(id, metadata)
    }

    fn read_field(&self, id: usize, column_id: usize) -> std::io::Result<TypedValue> {
        self.get_embedded_table()?.read_field(id, column_id)
    }

    fn read_row(&self, id: usize) -> std::io::Result<(Row, RowMetadata)> {
        self.get_embedded_table()?.read_row(id)
    }

    fn read_row_metadata(&self, id: usize) -> std::io::Result<RowMetadata> {
        self.get_embedded_table()?.read_row_metadata(id)
    }

    fn resize(&mut self, new_size: usize) -> std::io::Result<TypedValue> {
        self.get_embedded_table()?.resize(new_size)
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