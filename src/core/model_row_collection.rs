////////////////////////////////////////////////////////////////////
// model row-collection module
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

use crate::field_metadata::FieldMetadata;
use crate::row_collection::RowCollection;
use crate::row_metadata::RowMetadata;
use crate::rows::Row;
use crate::server::ColumnJs;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{Null, RowsAffected};

/// Row-model-vector-based RowCollection implementation
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModelRowCollection {
    columns: Vec<TableColumn>,
    row_data: Vec<(Row, RowMetadata)>,
    record_size: usize,
    watermark: usize,
}

impl ModelRowCollection {
    /// Creates a new [ModelRowCollection] prefilled with all rows from the given tables.
    pub fn combine(columns: Vec<TableColumn>, tables: Vec<&ModelRowCollection>) -> std::io::Result<ModelRowCollection> {
        let mut mrc = ModelRowCollection::new(columns);
        for table in tables {
            mrc.append_rows(table.get_rows());
        }
        Ok(mrc)
    }

    /// Creates a new [ModelRowCollection] from abstract columns
    pub fn construct(columns: &Vec<ColumnJs>) -> ModelRowCollection {
        match TableColumn::from_columns(columns) {
            Ok(columns) => ModelRowCollection::with_rows(columns, Vec::new()),
            Err(err) => panic!("{}", err.to_string())
        }
    }

    /// Decodes a byte vector into a [ModelRowCollection]
    pub fn decode(columns: Vec<TableColumn>, bytes: Vec<u8>) -> Self {
        let record_size = Row::compute_record_size(&columns);
        let row_data = bytes.chunks(record_size)
            .map(|chunk| Row::decode(&chunk.to_vec(), &columns))
            .collect::<Vec<(Row, RowMetadata)>>();
        Self::with_rows(columns, row_data)
    }

    /// Encodes the [ModelRowCollection] into a byte vector
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        for (row, rmd) in &self.row_data {
            if rmd.is_allocated { bytes.extend(row.encode()) }
        }
        bytes
    }

    /// Creates a new [ModelRowCollection] prefilled with the given rows.
    pub fn from_rows(rows: Vec<Row>) -> ModelRowCollection {
        let columns = rows.first()
            .map(|row| row.get_columns().clone())
            .unwrap_or(Vec::new());
        let row_data = rows.iter()
            .map(|r| (r.clone(), RowMetadata::new(true)))
            .collect();
        Self::with_rows(columns, row_data)
    }

    /// Creates a new [ModelRowCollection] prefilled with all rows from the given table.
    pub fn from_table(table: Box<&dyn RowCollection>) -> std::io::Result<ModelRowCollection> {
        let mut mrc = ModelRowCollection::new(table.get_columns().clone());
        for row_id in table.get_indices()? {
            if let Some(row) = table.read_one(row_id)? {
                let _ = mrc.append_row(row);
            }
        }
        Ok(mrc)
    }

    /// Returns all active rows
    pub fn get_rows(&self) -> Vec<Row> {
        let mut rows = Vec::new();
        for (row, rmd) in &self.row_data {
            if rmd.is_allocated { rows.push(row.clone()) }
        }
        rows
    }

    /// Creates a new [ModelRowCollection] from abstract columns
    pub fn new(columns: Vec<TableColumn>) -> ModelRowCollection {
        ModelRowCollection {
            record_size: Row::compute_record_size(&columns),
            watermark: 0,
            columns,
            row_data: Vec::new(),
        }
    }

    /// Creates a new [ModelRowCollection] prefilled with rows
    pub fn with_rows(columns: Vec<TableColumn>, row_data: Vec<(Row, RowMetadata)>) -> ModelRowCollection {
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

    fn iter(&self) -> Box<dyn Iterator<Item=Row> + '_> {
        let my_iter = self.row_data.iter()
            .filter(|(_, meta)| meta.is_allocated)
            .map(|(row, _)| row.clone());
        Box::new(my_iter)
    }

    fn len(&self) -> std::io::Result<usize> { Ok(self.watermark) }

    fn overwrite_field(
        &mut self,
        id: usize,
        column_id: usize,
        new_value: TypedValue,
    ) -> TypedValue {
        let (row, meta) = &self.row_data[id];
        let rows_affected = if meta.is_allocated {
            let old_values = row.get_values();
            let new_values = old_values.iter().zip(0..old_values.len())
                .map(|(v, n)| {
                    if n == column_id { new_value.clone() } else { v.clone() }
                }).collect();
            let new_row = Row::new(row.get_id(), row.get_columns().clone(), new_values);
            self.row_data[id] = (new_row, meta.clone());
            1
        } else { 0 };
        RowsAffected(rows_affected)
    }

    fn overwrite_field_metadata(
        &mut self,
        id: usize,
        column_id: usize,
        metadata: FieldMetadata,
    ) -> TypedValue {
        // get the old and new values
        let (row, rmd) = &self.row_data[id];
        let old_value = row[column_id].clone();
        let new_value = if metadata.is_active { old_value } else { Null };

        // build a new row
        let mut new_values = row.get_values();
        new_values[column_id] = new_value;
        let new_row = row.with_values(new_values);

        // update the row to reflect enabling/disabling a field
        self.row_data[id] = (new_row, rmd.clone());
        RowsAffected(1)
    }

    fn overwrite_row(&mut self, id: usize, row: Row) -> TypedValue {
        // resize the rows to prevent overflow
        if self.row_data.len() <= id {
            self.row_data.resize(id + 1, (Row::empty(&self.columns), RowMetadata::new(false)));
        }

        // set the block, update the watermark
        self.row_data[id] = (row.with_row_id(id), RowMetadata::new(true));
        if self.watermark <= id { self.watermark = id + 1; }
        RowsAffected(1)
    }

    fn overwrite_row_metadata(&mut self, id: usize, metadata: RowMetadata) -> TypedValue {
        let (row, _) = self.row_data[id].clone();
        self.row_data[id] = (row, metadata.clone());
        RowsAffected(1)
    }

    fn read_field(&self, id: usize, column_id: usize) -> TypedValue {
        self.row_data[id].0.get_values()[column_id].clone()
    }

    fn read_field_metadata(
        &self,
        id: usize,
        column_id: usize,
    ) -> std::io::Result<FieldMetadata> {
        let (row, _) = &self.row_data[id];
        let is_active = row[column_id] != Null;
        Ok(FieldMetadata::new(is_active))
    }

    fn read_row(&self, id: usize) -> std::io::Result<(Row, RowMetadata)> {
        let (metadata, row) =
            if id < self.watermark {
                self.row_data[id].clone()
            } else {
                (Row::empty(&self.columns), RowMetadata::new(false))
            };
        Ok((metadata, row))
    }

    fn read_row_metadata(&self, id: usize) -> std::io::Result<RowMetadata> {
        let metadata = if id < self.row_data.len() {
            self.row_data[id].1.clone()
        } else {
            RowMetadata::new(false)
        };
        Ok(metadata)
    }

    fn resize(&mut self, new_size: usize) -> TypedValue {
        self.row_data.resize(new_size, (Row::empty(&self.columns), RowMetadata::new(true)));
        self.watermark = new_size;
        TypedValue::Ack
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::model_row_collection::ModelRowCollection;
    use crate::row_collection::RowCollection;
    use crate::table_columns::TableColumn;
    use crate::testdata::{make_quote, make_quote_columns};
    use crate::typed_values::TypedValue::{Boolean, UInt64Value};

    #[test]
    fn test_contains() {
        let (mrc, phys_columns) = create_data_set();
        let row = make_quote(3, &phys_columns, "GOTO", "OTC", 0.1442);
        assert_eq!(mrc.contains(&row), Boolean(true));
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
        assert_eq!(mrc.index_of(&row), UInt64Value(3));
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
