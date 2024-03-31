////////////////////////////////////////////////////////////////////
// byte row-collection module
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

use crate::row_collection::RowCollection;

/// Byte-vector-based RowCollection implementation
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ByteRowCollection {
    rows: Vec<Vec<u8>>,
    record_size: usize,
    watermark: usize,
}

impl ByteRowCollection {
    pub fn new(rows: Vec<Vec<u8>>, record_size: usize) -> ByteRowCollection {
        ByteRowCollection {
            rows,
            record_size,
            watermark: 0,

        }
    }
}

impl RowCollection for ByteRowCollection {
    fn get_record_size(&self) -> usize { self.record_size }

    fn len(&self) -> std::io::Result<usize> { Ok(self.watermark) }

    fn overwrite(&mut self, id: usize, block: Vec<u8>) -> std::io::Result<usize> {
        // resize the rows to prevent overflow
        if self.rows.len() <= id {
            self.rows.resize(id + 1, vec![]);
        }

        // set the block, update the watermark
        self.rows[id] = block[0..self.record_size].to_owned();
        if self.watermark <= id {
            self.watermark = id + 1;
        }
        Ok(1)
    }

    fn overwrite_row_metadata(&mut self, id: usize, metadata: u8) -> std::io::Result<usize> {
        self.rows[id][0] = metadata;
        Ok(1)
    }

    fn read(&self, id: usize) -> std::io::Result<Vec<u8>> {
        Ok(self.rows[id].clone())
    }

    fn read_field(&self, id: usize, column_offset: usize, max_physical_size: usize) -> std::io::Result<Vec<u8>> {
        Ok(self.rows[id][column_offset..(column_offset + max_physical_size)].to_vec())
    }

    fn read_range(&self, from: usize, to: usize) -> std::io::Result<Vec<Vec<u8>>> {
        Ok(self.rows[from..to].to_vec())
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
    use crate::row;
    use crate::row_collection::RowCollection;
    use crate::rows::Row;
    use crate::table_columns::TableColumn;
    use crate::testdata::{make_quote, make_table_columns};
    use crate::typed_values::TypedValue;
    use crate::typed_values::TypedValue::{Float64Value, Null, StringValue};

    #[test]
    fn test_append_row_then_read_rows() {
        // determine the record size of the row
        let columns = make_table_columns();
        let record_size = row!(0, columns, vec![Null, Null, Null]).get_record_size();
        let mut mrc = ByteRowCollection::new(vec![], record_size);

        // insert the rows
        let row_data0: Vec<u8> = vec![
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 0,
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'R', b'I', b'C', b'E',
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E',
            0b1000_0000, 64, 83, 150, 102, 102, 102, 102, 102,
        ];
        let row_data1: Vec<u8> = vec![
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 1,
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'B', b'E', b'E', b'F',
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'A', b'M', b'E', b'X',
            0b1000_0000, 64, 89, 0, 0, 0, 0, 0, 0,
        ];
        assert_eq!(mrc.overwrite(0, row_data0).unwrap(), 1);
        assert_eq!(mrc.overwrite(1, row_data1).unwrap(), 1);
        assert_eq!(mrc.len().unwrap(), 2);

        // retrieve the rows (as binary)
        let bytes = mrc.read_range(0, 2).unwrap();
        assert_eq!(bytes.len(), 2);
        assert_eq!(mrc.len().unwrap(), 2);

        // decode and verify the rows
        let rows: Vec<Row> = Row::decode_rows(&columns, bytes);
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
                Field::new(Float64Value(78.35)),
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
                Field::new(Float64Value(100.0)),
            ],
        });
    }

    #[test]
    fn test_overwrite_row() {
        // determine the record size of the row
        let columns = make_table_columns();
        let record_size = row!(0, columns, vec![Null, Null, Null]).get_record_size();
        let mut mrc = ByteRowCollection::new(vec![], record_size);

        // create a new row
        let row = make_quote(2, &columns, "AMD", "NYSE", 88.78);
        assert_eq!(mrc.overwrite(row.id, row.encode()).unwrap(), 1);

        // read and verify the row
        let bytes = mrc.read(row.id).unwrap();
        let (new_row, _) = Row::decode(&bytes, &columns);
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
        let record_size = row!(0, columns, vec![Null, Null, Null]).get_record_size();
        let mut mrc = ByteRowCollection::new(vec![], record_size);

        assert_eq!(mrc.overwrite(0, vec![
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 0,
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'F', b'A', b'C', b'T',
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E',
            0b1000_0000, 64, 83, 150, 102, 102, 102, 102, 102,
        ]).unwrap(), 1);

        // read the first column
        let column: &TableColumn = &columns[0];
        let buf = mrc.read_field(0, column.offset, column.max_physical_size).unwrap();
        let value: TypedValue = TypedValue::decode(&column.data_type, &buf, 1);
        assert_eq!(value, StringValue("FACT".into()));
    }

    #[test]
    fn test_read_row() {
        // determine the record size of the row
        let columns = make_table_columns();
        let record_size = row!(0, columns, vec![Null, Null, Null]).get_record_size();
        let mut mrc = ByteRowCollection::new(vec![], record_size);

        assert_eq!(mrc.overwrite(0, vec![
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 5,
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'R', b'O', b'O', b'M',
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'A', b'M', b'E', b'X',
            0b1000_0000, 64, 83, 150, 102, 102, 102, 102, 102,
        ]).unwrap(), 1);

        // read the row
        let bytes = mrc.read(0).unwrap();
        let (row, metadata) = Row::decode(&bytes, &columns);

        // verify the row
        assert!(metadata.is_allocated);
        assert_eq!(row, Row {
            id: 5,
            columns: vec![
                TableColumn::new("symbol", StringType(4), Null, 9),
                TableColumn::new("exchange", StringType(4), Null, 22),
                TableColumn::new("lastSale", Float64Type, Null, 35),
            ],
            fields: vec![
                Field::new(StringValue("ROOM".into())),
                Field::new(StringValue("AMEX".into())),
                Field::new(Float64Value(78.35)),
            ],
        });
    }

    #[test]
    fn test_resize_shrink() {
        // determine the record size of the row
        let columns = make_table_columns();
        let record_size = row!(0, columns, vec![Null, Null, Null]).get_record_size();
        let mut mrc = ByteRowCollection::new(vec![], record_size);

        // write a row
        assert_eq!(mrc.overwrite(0, vec![
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 0,
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'R', b'I', b'C', b'E',
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E',
            0b1000_0000, 64, 83, 150, 102, 102, 102, 102, 102,
        ]).unwrap(), 1);

        // shrink the table
        let _ = mrc.resize(0).unwrap();
        assert_eq!(mrc.len().unwrap(), 0);
    }

    #[test]
    fn test_resize_grow() {
        // determine the record size of the row
        let columns = make_table_columns();
        let record_size = row!(0, columns, vec![Null, Null, Null]).get_record_size();
        let mut mrc = ByteRowCollection::new(vec![], record_size);

        // write a row
        assert_eq!(mrc.overwrite(0, vec![
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 0,
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'R', b'I', b'C', b'E',
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E',
            0b1000_0000, 64, 96, 99, 102, 102, 102, 102, 102,
        ]).unwrap(), 1);

        // grow the table
        let _ = mrc.resize(5).unwrap();
        assert_eq!(mrc.len().unwrap(), 5);
    }
}