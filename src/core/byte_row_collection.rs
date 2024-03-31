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
    rows: Vec<Vec<u8>>,
    record_size: usize,
    watermark: usize,
}

impl ByteRowCollection {
    pub fn new(columns: Vec<TableColumn>, rows: Vec<Vec<u8>>) -> ByteRowCollection {
        ByteRowCollection {
            record_size: Row::compute_record_size(&columns),
            columns,
            rows,
            watermark: 0,
        }
    }
}

impl RowCollection for ByteRowCollection {
    fn get_record_size(&self) -> usize { self.record_size }

    fn len(&self) -> std::io::Result<usize> { Ok(self.watermark) }

    fn overwrite(&mut self, id: usize, row: &Row) -> std::io::Result<usize> {
        // resize the rows to prevent overflow
        if self.rows.len() <= id {
            self.rows.resize(id + 1, vec![]);
        }

        // set the block, update the watermark
        self.rows[id] = row.encode();
        if self.watermark <= id {
            self.watermark = id + 1;
        }
        Ok(1)
    }

    fn overwrite_row_metadata(&mut self, id: usize, metadata: &RowMetadata) -> std::io::Result<usize> {
        self.rows[id][0] = metadata.encode();
        Ok(1)
    }

    fn read(&self, id: usize) -> std::io::Result<(Row, RowMetadata)> {
        Ok(Row::decode(&self.rows[id], &self.columns))
    }

    fn read_field(&self, id: usize, column_id: usize) -> std::io::Result<TypedValue> {
        let column = &self.columns[column_id];
        let buffer = self.rows[id][column.offset..(column.offset + column.max_physical_size)].to_vec();
        let field = Field::decode(&column.data_type, &buffer, 0);
        Ok(field.value)
    }

    fn read_range(&self, index: std::ops::Range<usize>) -> std::io::Result<Vec<Row>> {
        let rows = self.rows[index].iter().flat_map(|b| {
            let (row, meta) = Row::decode(b, &self.columns);
            if meta.is_allocated { Some(row) } else { None }
        }).collect();
        Ok(rows)
    }

    fn resize(&mut self, new_size: usize) -> std::io::Result<()> {
        self.rows.resize(new_size, vec![]);
        self.watermark = new_size;
        Ok(())
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::byte_row_collection::ByteRowCollection;
    use crate::data_types::DataType::{Float64Type, StringType};
    use crate::fields::Field;
    use crate::row_collection::RowCollection;
    use crate::rows::Row;
    use crate::table_columns::TableColumn;
    use crate::testdata::{make_quote, make_table_columns};
    use crate::typed_values::TypedValue::{Float64Value, Null, StringValue};

    #[test]
    fn test_append_row_then_read_rows() {
        // determine the record size of the row
        let columns = make_table_columns();
        let mut mrc = ByteRowCollection::new(columns.clone(), vec![]);

        // insert the rows
        let row0 = make_quote(0, &columns, "RICE", "NYSE", 56.77);
        let row1 = make_quote(1, &columns, "BEEF", "AMEX", 32.99);
        assert_eq!(mrc.overwrite(0, &row0).unwrap(), 1);
        assert_eq!(mrc.overwrite(1, &row1).unwrap(), 1);
        assert_eq!(mrc.len().unwrap(), 2);

        // retrieve the rows (as binary)
        let rows = mrc.read_range(0..2).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], Row {
            id: 0,
            columns: vec![
                TableColumn::new("symbol", StringType(4), Null, 9),
                TableColumn::new("exchange", StringType(4), Null, 22),
                TableColumn::new("lastSale", Float64Type, Null, 35),
            ],
            fields: vec![
                Field::new(StringValue("RICE".into())),
                Field::new(StringValue("NYSE".into())),
                Field::new(Float64Value(56.77)),
            ],
        });
        assert_eq!(rows[1], Row {
            id: 1,
            columns: vec![
                TableColumn::new("symbol", StringType(4), Null, 9),
                TableColumn::new("exchange", StringType(4), Null, 22),
                TableColumn::new("lastSale", Float64Type, Null, 35),
            ],
            fields: vec![
                Field::new(StringValue("BEEF".into())),
                Field::new(StringValue("AMEX".into())),
                Field::new(Float64Value(32.99)),
            ],
        });
    }

    #[test]
    fn test_overwrite_row() {
        // determine the record size of the row
        let columns = make_table_columns();
        let mut mrc = ByteRowCollection::new(columns.clone(), vec![]);

        // create a new row
        let row = make_quote(2, &columns, "AMD", "NYSE", 88.78);
        assert_eq!(mrc.overwrite(row.id, &row).unwrap(), 1);

        // read and verify the row
        let (new_row, _) = mrc.read(row.id).unwrap();
        assert_eq!(new_row, Row {
            id: 2,
            columns: vec![
                TableColumn::new("symbol", StringType(4), Null, 9),
                TableColumn::new("exchange", StringType(4), Null, 22),
                TableColumn::new("lastSale", Float64Type, Null, 35),
            ],
            fields: vec![
                Field::new(StringValue("AMD".into())),
                Field::new(StringValue("NYSE".into())),
                Field::new(Float64Value(88.78)),
            ],
        });
    }

    #[test]
    fn test_read_field() {
        // determine the record size of the row
        let columns = make_table_columns();
        let mut mrc = ByteRowCollection::new(columns.clone(), vec![]);

        let row = make_quote(0, &columns, "FACT", "NYSE", 111.56);
        assert_eq!(mrc.overwrite(0, &row).unwrap(), 1);

        // read the first column
        let value = mrc.read_field(0, 0).unwrap();
        assert_eq!(value, StringValue("FACT".into()));
    }

    #[test]
    fn test_read_row() {
        // determine the record size of the row
        let columns = make_table_columns();
        let mut mrc = ByteRowCollection::new(columns.clone(), vec![]);

        // write the row
        let row = make_quote(0, &columns, "ROOM", "AMEX", 34.44);
        assert_eq!(mrc.overwrite(0, &row).unwrap(), 1);

        // read the row
        let (row, metadata) = mrc.read(0).unwrap();

        // verify the row
        assert!(metadata.is_allocated);
        assert_eq!(row, Row {
            id: 0,
            columns: vec![
                TableColumn::new("symbol", StringType(4), Null, 9),
                TableColumn::new("exchange", StringType(4), Null, 22),
                TableColumn::new("lastSale", Float64Type, Null, 35),
            ],
            fields: vec![
                Field::new(StringValue("ROOM".into())),
                Field::new(StringValue("AMEX".into())),
                Field::new(Float64Value(34.44)),
            ],
        });
    }

    #[test]
    fn test_resize_shrink() {
        // determine the record size of the row
        let columns = make_table_columns();
        let mut mrc = ByteRowCollection::new(columns.clone(), vec![]);

        // write the row
        let row = make_quote(5, &columns, "RICE", "NYSE", 31.11);
        assert_eq!(mrc.overwrite(0, &row).unwrap(), 1);

        // shrink the table
        let _ = mrc.resize(0).unwrap();
        assert_eq!(mrc.len().unwrap(), 0);
    }

    #[test]
    fn test_resize_grow() {
        // determine the record size of the row
        let columns = make_table_columns();
        let mut mrc = ByteRowCollection::new(columns.clone(), vec![]);

        // write a row
        let row = make_quote(0, &columns, "RICE", "NYSE", 31.12);
        assert_eq!(mrc.overwrite(0, &row).unwrap(), 1);

        // grow the table
        let _ = mrc.resize(5).unwrap();
        assert_eq!(mrc.len().unwrap(), 5);
    }
}