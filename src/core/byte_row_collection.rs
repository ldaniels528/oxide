////////////////////////////////////////////////////////////////////
// byte row-collection module
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

use crate::fields::Field;
use crate::row_collection::RowCollection;
use crate::row_metadata::RowMetadata;
use crate::rows::Row;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue;

/// Byte-vector-based RowCollection implementation
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ByteRowCollection {
    columns: Vec<TableColumn>,
    row_data: Vec<Vec<u8>>,
    record_size: usize,
    watermark: usize,
}

impl ByteRowCollection {
    /// Returns true, if the given item matches a [Row] found within it
    pub fn contains(&self, item: &Row) -> bool { self.index_of(item).is_some() }

    /// Decodes a byte vector into a [ByteRowCollection]
    pub fn decode(columns: Vec<TableColumn>, bytes: Vec<u8>) -> Self {
        let record_size = Row::compute_record_size(&columns);
        let row_bytes = bytes.chunks(record_size)
            .map(|chunk| chunk.to_vec())
            .collect();
        Self::new(columns, row_bytes)
    }

    /// Encodes the [ByteRowCollection] into a byte vector
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = vec![];
        for row in &self.row_data { bytes.extend(row) }
        bytes
    }

    /// Creates a new [ByteRowCollection] from the specified rows
    pub fn from_rows(rows: Vec<Row>) -> Self {
        let mut encoded_rows = vec![];
        let columns = rows.first()
            .map(|row| row.get_columns().clone())
            .unwrap_or(vec![]);
        for row in rows { encoded_rows.push(row.encode()) }
        Self::new(columns, encoded_rows)
    }

    pub fn get_rows(&self) -> Vec<Row> {
        let mut rows = vec![];
        for buf in &self.row_data {
            let (row, rmd) = Row::decode(buf, &self.columns);
            if rmd.is_allocated { rows.push(row) }
        }
        rows
    }

    /// Creates a new [ByteRowCollection] from the specified row data
    pub fn new(columns: Vec<TableColumn>, rows: Vec<Vec<u8>>) -> Self {
        ByteRowCollection {
            record_size: Row::compute_record_size(&columns),
            watermark: rows.len(),
            columns,
            row_data: rows,
        }
    }
}

impl RowCollection for ByteRowCollection {
    fn get_columns(&self) -> &Vec<TableColumn> { &self.columns }

    fn get_record_size(&self) -> usize { self.record_size }

    fn index_of(&self, item: &Row) -> Option<usize> {
        let mut id = 0;
        for buf in &self.row_data {
            let (row, metadata) = Row::decode(buf, &self.columns);
            if metadata.is_allocated && item == &row { return Some(id); }
            id += 1
        }
        None
    }

    fn len(&self) -> std::io::Result<usize> { Ok(self.watermark) }

    fn overwrite(&mut self, id: usize, row: &Row) -> std::io::Result<TypedValue> {
        // resize the rows to prevent overflow
        if self.row_data.len() <= id {
            self.row_data.resize(id + 1, vec![]);
        }

        // set the block, update the watermark
        self.row_data[id] = row.encode();
        if self.watermark <= id {
            self.watermark = id + 1;
        }
        Ok(TypedValue::Ack)
    }

    fn overwrite_metadata(&mut self, id: usize, metadata: &RowMetadata) -> std::io::Result<TypedValue> {
        self.row_data[id][0] = metadata.encode();
        Ok(TypedValue::Ack)
    }

    fn read(&self, id: usize) -> std::io::Result<(Row, RowMetadata)> {
        Ok(Row::decode(&self.row_data[id], &self.columns))
    }

    fn read_field(&self, id: usize, column_id: usize) -> std::io::Result<TypedValue> {
        let column = &self.columns[column_id];
        let buffer = self.row_data[id][column.offset..(column.offset + column.max_physical_size)].to_vec();
        let field = Field::decode(&column.data_type, &buffer, 0);
        Ok(field.value)
    }

    fn read_range(&self, index: std::ops::Range<usize>) -> std::io::Result<Vec<Row>> {
        let rows = self.row_data[index].iter().flat_map(|b| {
            let (row, meta) = Row::decode(b, &self.columns);
            if meta.is_allocated { Some(row) } else { None }
        }).collect();
        Ok(rows)
    }

    fn resize(&mut self, new_size: usize) -> std::io::Result<TypedValue> {
        self.row_data.resize(new_size, vec![]);
        self.watermark = new_size;
        Ok(TypedValue::Ack)
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::byte_row_collection::ByteRowCollection;
    use crate::row_collection::RowCollection;
    use crate::table_columns::TableColumn;
    use crate::testdata::{make_quote, make_quote_columns};

    #[test]
    fn test_contains() {
        let (brc, phys_columns) = create_data_set();
        let row = make_quote(4, &phys_columns, "XYZ", "NYSE", 0.0289);
        assert!(brc.contains(&row));
    }

    #[test]
    fn test_encode_decode() {
        let (brc, phys_columns) = create_data_set();
        let encoded = brc.encode();
        assert_eq!(ByteRowCollection::decode(phys_columns, encoded), brc)
    }

    #[test]
    fn test_get_rows() {
        let (brc, phys_columns) = create_data_set();
        assert_eq!(brc.get_rows(), vec![
            make_quote(0, &phys_columns, "ABC", "AMEX", 12.33),
            make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
            make_quote(2, &phys_columns, "BIZ", "NYSE", 9.775),
            make_quote(3, &phys_columns, "GOTO", "OTC", 0.1442),
            make_quote(4, &phys_columns, "XYZ", "NYSE", 0.0289),
        ])
    }

    #[test]
    fn test_index_of() {
        let (brc, phys_columns) = create_data_set();
        let row = make_quote(4, &phys_columns, "XYZ", "NYSE", 0.0289);
        assert_eq!(brc.index_of(&row), Some(4));
    }

    fn create_data_set() -> (ByteRowCollection, Vec<TableColumn>) {
        let columns = make_quote_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let brc = ByteRowCollection::from_rows(vec![
            make_quote(0, &phys_columns, "ABC", "AMEX", 12.33),
            make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
            make_quote(2, &phys_columns, "BIZ", "NYSE", 9.775),
            make_quote(3, &phys_columns, "GOTO", "OTC", 0.1442),
            make_quote(4, &phys_columns, "XYZ", "NYSE", 0.0289),
        ]);
        (brc, phys_columns)
    }
}