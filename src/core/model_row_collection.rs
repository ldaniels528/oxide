////////////////////////////////////////////////////////////////////
// model row-collection module
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

use crate::row_collection::RowCollection;
use crate::row_metadata::RowMetadata;
use crate::rows::Row;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue;

/// Row-model-vector-based RowCollection implementation
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModelRowCollection {
    columns: Vec<TableColumn>,
    row_data: Vec<(RowMetadata, Row)>,
    record_size: usize,
    watermark: usize,
}

impl ModelRowCollection {
    /// Decodes a byte vector into a [ModelRowCollection]
    pub fn decode(columns: Vec<TableColumn>, bytes: Vec<u8>) -> Self {
        let record_size = Row::compute_record_size(&columns);
        let row_data = bytes.chunks(record_size)
            .map(|chunk| {
                let (row, rmd) = Row::decode(&chunk.to_vec(), &columns);
                (rmd, row)
            })
            .collect::<Vec<(RowMetadata, Row)>>();
        Self::new(columns, row_data)
    }

    /// Encodes the [ModelRowCollection] into a byte vector
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = vec![];
        for (rmd, row) in &self.row_data {
            if rmd.is_allocated { bytes.extend(row.encode()) }
        }
        bytes
    }

    pub fn from_rows(rows: Vec<Row>) -> ModelRowCollection {
        let columns = rows.first()
            .map(|row| row.get_columns().clone())
            .unwrap_or(vec![]);
        let row_data = rows.iter()
            .map(|r| (RowMetadata::new(true), r.clone()))
            .collect();
        Self::new(columns, row_data)
    }

    pub fn get_rows(&self) -> Vec<Row> {
        let mut rows = vec![];
        for (rmd, row) in &self.row_data {
            if rmd.is_allocated { rows.push(row.clone()) }
        }
        rows
    }

    pub fn new(columns: Vec<TableColumn>, row_data: Vec<(RowMetadata, Row)>) -> ModelRowCollection {
        ModelRowCollection {
            record_size: Row::compute_record_size(&columns),
            watermark: row_data.len(),
            columns,
            row_data,
        }
    }
}

impl RowCollection for ModelRowCollection {
    fn get_columns(&self) -> &Vec<TableColumn> { &self.columns }

    fn get_record_size(&self) -> usize { self.record_size }

    fn index_of(&self, item: &Row) -> Option<usize> {
        let mut id = 0;
        for (rmd, row) in &self.row_data {
            if rmd.is_allocated && item == row { return Some(id); }
            id += 1
        }
        None
    }

    fn len(&self) -> std::io::Result<usize> { Ok(self.watermark) }

    fn overwrite(&mut self, id: usize, row: &Row) -> std::io::Result<usize> {
        // resize the rows to prevent overflow
        if self.row_data.len() <= id {
            self.row_data.resize(id + 1, (RowMetadata::new(false), Row::empty(&self.columns)));
        }

        // set the block, update the watermark
        println!("B[{}] {:?}", id, row.to_row_js());
        self.row_data[id] = (RowMetadata::new(true), row.clone());
        println!("A[{}] {:?}", id, self.row_data[id].clone().1.to_row_js());
        if self.watermark <= id {
            self.watermark = id + 1;
        }
        Ok(1)
    }

    fn overwrite_metadata(&mut self, id: usize, metadata: &RowMetadata) -> std::io::Result<usize> {
        let (_, row) = self.row_data[id].clone();
        self.row_data[id] = (metadata.clone(), row);
        Ok(1)
    }

    fn read(&self, id: usize) -> std::io::Result<(Row, RowMetadata)> {
        let (metadata, row) =
            if id < self.watermark {
                self.row_data[id].clone()
            } else {
                (RowMetadata::new(false), Row::empty(&self.columns))
            };
        Ok((row, metadata))
    }

    fn read_field(&self, id: usize, column_id: usize) -> std::io::Result<TypedValue> {
        let field = &self.row_data[id].1.get_fields()[column_id];
        Ok(field.value.clone())
    }

    fn read_range(&self, index: std::ops::Range<usize>) -> std::io::Result<Vec<Row>> {
        let rows = self.row_data[index].iter().flat_map(|(meta, row)| {
            if meta.is_allocated { Some(row.clone()) } else { None }
        }).collect();
        Ok(rows)
    }

    fn resize(&mut self, new_size: usize) -> std::io::Result<()> {
        self.row_data.resize(new_size, (RowMetadata::new(true), Row::empty(&self.columns)));
        self.watermark = new_size;
        Ok(())
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::model_row_collection::ModelRowCollection;
    use crate::row_collection::RowCollection;
    use crate::table_columns::TableColumn;
    use crate::testdata::{make_quote_columns, make_quote};

    #[test]
    fn test_contains() {
        let (mrc, phys_columns) = create_data_set();
        let row = make_quote(3, &phys_columns, "GOTO", "OTC", 0.1442);
        assert!(mrc.contains(&row));
    }

    #[test]
    fn test_encode_decode() {
        let (mrc, phys_columns) = create_data_set();
        let encoded = mrc.encode();
        assert_eq!(ModelRowCollection::decode(phys_columns, encoded), mrc)
    }

    #[test]
    fn test_get_rows() {
        let (mrc, phys_columns) = create_data_set();
        assert_eq!(mrc.get_rows(), vec![
            make_quote(0, &phys_columns, "ABC", "AMEX", 12.33),
            make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
            make_quote(2, &phys_columns, "BIZ", "NYSE", 9.775),
            make_quote(3, &phys_columns, "GOTO", "OTC", 0.1442),
            make_quote(4, &phys_columns, "XYZ", "NYSE", 0.0289),
        ])
    }

    #[test]
    fn test_index_of() {
        let (mrc, phys_columns) = create_data_set();
        let row = make_quote(3, &phys_columns, "GOTO", "OTC", 0.1442);
        assert_eq!(mrc.index_of(&row), Some(3));
    }

    fn create_data_set() -> (ModelRowCollection, Vec<TableColumn>) {
        let columns = make_quote_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let mrc = ModelRowCollection::from_rows(vec![
            make_quote(0, &phys_columns, "ABC", "AMEX", 12.33),
            make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
            make_quote(2, &phys_columns, "BIZ", "NYSE", 9.775),
            make_quote(3, &phys_columns, "GOTO", "OTC", 0.1442),
            make_quote(4, &phys_columns, "XYZ", "NYSE", 0.0289),
        ]);
        (mrc, phys_columns)
    }
}
