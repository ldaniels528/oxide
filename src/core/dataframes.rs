////////////////////////////////////////////////////////////////////
// dataframes module
////////////////////////////////////////////////////////////////////

use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::{Add, AddAssign, Index};
use std::os::unix::fs::FileExt;

use crate::dataframe_config::DataFrameConfig;
use crate::fields::Field;
use crate::namespaces::Namespace;
use crate::row_collection::RowCollection;
use crate::row_metadata::RowMetadata;
use crate::rows::Row;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue;

/// DataFrame is a logical representation of table
#[derive(Debug)]
pub struct DataFrame {
    pub(crate) ns: Namespace,
    pub(crate) columns: Vec<TableColumn>,
    pub(crate) config: DataFrameConfig,
    pub(crate) record_size: usize,
    pub(crate) file: File,
}

impl AddAssign for DataFrame {
    fn add_assign(&mut self, rhs: Self) {
        fn do_add(lhs: &mut DataFrame, rhs: DataFrame) -> io::Result<()> {
            for id in 0..rhs.size()? {
                let (row, metadata) = rhs.read_row(id)?;
                if metadata.is_allocated {
                    lhs.append(&row)?;
                }
            }
            Ok(())
        }

        match do_add(self, rhs) {
            Ok(_) => (),
            Err(err) => panic!("{}", err.to_string())
        }
    }
}

impl DataFrame {
    /// creates a new [DataFrame]; persisting its configuration to disk.
    pub fn create(ns: Namespace, config: DataFrameConfig) -> io::Result<DataFrame> {
        config.save(&ns)?;
        let file = Self::open_crw(&ns)?;
        Self::new(ns, config, file)
    }

    /// loads a dataframe from disk.
    pub fn load(ns: Namespace) -> io::Result<DataFrame> {
        let cfg = DataFrameConfig::load(&ns)?;
        let file = Self::open_rw(&ns)?;
        DataFrame::new(ns, cfg, file)
    }

    /// creates a new dataframe.
    pub fn new(ns: Namespace, config: DataFrameConfig, file: File) -> io::Result<DataFrame> {
        let columns = TableColumn::from_columns(&config.columns)?;
        let record_size = Self::compute_record_size(&columns);
        Ok(DataFrame { ns, columns, config, record_size, file })
    }

    /// appends a new row to the table
    pub fn append(&mut self, row: &Row) -> io::Result<usize> {
        let file_len = (&self.file.metadata()?).len();
        let new_row_id = (file_len / self.record_size as u64) as usize;
        let new_row = row.with_row_id(new_row_id);
        let offset = self.to_offset(new_row.id);
        let _ = &self.file.seek(SeekFrom::Start(offset))?;
        let _ = &self.file.write_all(&new_row.encode())?;
        self.file.flush()?;
        Ok(1)
    }

    /// computes the total record size (in bytes)
    fn compute_record_size(columns: &Vec<TableColumn>) -> usize {
        Row::overhead() + columns.iter().map(|c| c.max_physical_size).sum::<usize>()
    }

    /// performs a top-down fold operation
    pub fn fold_left<A>(&self, init: A, f: fn(A, Row) -> A) -> io::Result<A> {
        let mut result: A = init;
        for id in 0..self.size()? {
            let (row, metadata) = self.read_row(id)?;
            if metadata.is_allocated {
                result = f(result, row);
            }
        }
        Ok(result)
    }

    /// performs a bottom-up fold operation
    pub fn fold_right<A>(&self, init: A, f: fn(A, Row) -> A) -> io::Result<A> {
        let mut result: A = init;
        for id in (0..self.size()?).rev() {
            let (row, metadata) = self.read_row(id)?;
            if metadata.is_allocated {
                result = f(result, row);
            }
        }
        Ok(result)
    }

    /// returns true if all allocated rows satisfy the provided function
    pub fn for_all(&self, f: fn(&Row) -> bool) -> io::Result<bool> {
        for id in 0..self.size()? {
            let (row, metadata) = self.read_row(id)?;
            if metadata.is_allocated && !f(&row) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// iterates through all allocated rows
    pub fn foreach(&self, f: fn(&Row) -> ()) -> io::Result<()> {
        for id in 0..self.size()? {
            let (row, metadata) = self.read_row(id)?;
            if metadata.is_allocated { f(&row); }
        }
        Ok(())
    }

    /// returns the size of the underlying physical table
    pub fn length(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    /// transforms the collection of rows into a collection of [A]
    pub fn map<A>(&self, f: fn(&Row) -> A) -> io::Result<Vec<A>> {
        let mut rows = Vec::new();
        for id in 0..self.size()? {
            let (row, metadata) = self.read_row(id)?;
            if metadata.is_allocated {
                &rows.push(f(&row));
            }
        }
        Ok(rows)
    }

    /// convenience function to create, read or write a table file
    fn open_crw(ns: &Namespace) -> io::Result<File> {
        OpenOptions::new().create(true).read(true).write(true).open(ns.get_table_file_path())
    }

    /// convenience function to read or write a table file
    fn open_rw(ns: &Namespace) -> io::Result<File> {
        OpenOptions::new().read(true).write(true).open(ns.get_table_file_path())
    }

    /// overwrites a specified row by ID
    pub fn overwrite(&mut self, row: Row) -> io::Result<usize> {
        let offset = self.to_offset(row.id);
        let _ = &self.file.seek(SeekFrom::Start(offset))?;
        let _ = &self.file.write_all(&row.encode())?;
        self.file.flush()?;
        Ok(1)
    }

    /// reads the specified field value from the specified row ID
    pub fn read_field(&self, id: usize, column_id: usize) -> io::Result<TypedValue> {
        let column = &self.columns[column_id];
        let mut buffer: Vec<u8> = vec![0; column.max_physical_size];
        let row_offset = self.to_offset(id);
        let _ = &self.file.read_at(&mut buffer, row_offset + column.offset as u64)?;
        let field = Field::decode(&column.data_type, &buffer, 0);
        Ok(field.value)
    }

    /// reads a row by ID
    pub fn read_row(&self, id: usize) -> io::Result<(Row, RowMetadata)> {
        let offset = self.to_offset(id);
        let mut buffer: Vec<u8> = vec![0; self.record_size];
        let _ = &self.file.read_at(&mut buffer, offset)?;
        Ok(Row::decode(&buffer, &self.columns))
    }

    /// reads a range of rows
    pub fn read_rows(&self, from: usize, to: usize) -> io::Result<Vec<Row>> {
        let mut rows: Vec<Row> = Vec::with_capacity(to - from);
        for id in from..to {
            let (row, metadata) = self.read_row(id)?;
            if metadata.is_allocated {
                rows.push(row);
            }
        }
        Ok(rows)
    }

    /// returns the rows in reverse order
    pub fn reverse(&self) -> io::Result<Vec<Row>> {
        let size = self.size()?;
        let mut rows: Vec<Row> = Vec::with_capacity(size);
        for id in (0..size).rev() {
            let (row, metadata) = self.read_row(id)?;
            if metadata.is_allocated {
                rows.push(row);
            }
        }
        Ok(rows)
    }

    /// resizes the table
    pub fn resize(&mut self, new_size: usize) -> io::Result<()> {
        let new_length = new_size as u64 * self.record_size as u64;
        self.file.set_len(new_length)?;
        Ok(())
    }

    /// returns the allocated sizes the table (in numbers of rows)
    pub fn size(&self) -> io::Result<usize> {
        Ok((self.file.metadata()?.len() as usize) / self.record_size)
    }

    fn to_offset(&self, id: usize) -> u64 { (id * self.record_size) as u64 }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::data_types::DataType::{Float64Type, StringType};
    use crate::dataframes::DataFrame;
    use crate::fields::Field;
    use crate::namespaces::Namespace;
    use crate::rows::Row;
    use crate::table_columns::TableColumn;
    use crate::testdata::*;
    use crate::typed_values::TypedValue;
    use crate::typed_values::TypedValue::{Float64Value, NullValue, StringValue};

    #[test]
    fn test_add_assign() {
        // create a dataframe with a single (encoded) row
        let mut df0: DataFrame = make_rows_from_bytes(
            "add_assign", "stocks", "quotes0", make_columns(), &mut vec![
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 0,
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'R', b'A', b'C', b'E',
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'A', b'S', b'D',
                0b1000_0000, 64, 94, 220, 204, 204, 204, 204, 205,
            ]).unwrap();
        // decode a second dataframe with a single (encoded) row
        let df1: DataFrame = make_rows_from_bytes(
            "add_assign", "stocks", "quotes1", make_columns(), &mut vec![
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 1,
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'B', b'E', b'E', b'R',
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'A', b'M', b'E', b'X',
                0b1000_0000, 64, 118, 81, 235, 133, 30, 184, 82,
            ]).unwrap();
        // concatenate the dataframes
        df0 += df1;
        // re-read the rows
        let rows = df0.read_rows(0, df0.size().unwrap()).unwrap();
        let columns = vec![
            TableColumn::new("symbol", StringType(4), NullValue, 9),
            TableColumn::new("exchange", StringType(4), NullValue, 22),
            TableColumn::new("lastSale", Float64Type, NullValue, 35),
        ];
        assert_eq!(rows, vec![
            Row {
                id: 0,
                columns: columns.clone(),
                fields: vec![
                    Field::new(StringValue("RACE".into())),
                    Field::new(StringValue("NASD".into())),
                    Field::new(Float64Value(123.45)),
                ],
            },
            Row {
                id: 1,
                columns: columns.clone(),
                fields: vec![
                    Field::new(StringValue("BEER".into())),
                    Field::new(StringValue("AMEX".into())),
                    Field::new(Float64Value(357.12)),
                ],
            },
        ]);
    }

    #[test]
    fn test_append_row_then_read_rows() {
        // create a dataframe with a single (encoded) row
        let mut df = make_rows_from_bytes(
            "dataframes", "stocks", "quotes", make_columns(), &mut vec![
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 0,
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'R', b'I', b'C', b'E',
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'P', b'I', b'P', b'E',
                0b1000_0000, 64, 69, 14, 20, 122, 225, 71, 174,
            ]).unwrap();

        // decode a second row and append it to the dataframe
        let columns = make_table_columns();
        let (row, _) = Row::decode(&vec![
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 1,
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'B', b'E', b'E', b'F',
            0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'C', b'A', b'K', b'E',
            0b1000_0000, 64, 89, 0, 0, 0, 0, 0, 0,
        ], &columns);
        assert_eq!(df.append(&row).unwrap(), 1);

        // verify the rows
        let rows = df.read_rows(0, 2).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].id, 0);
        assert_eq!(rows[0].fields[0].metadata.is_active, true);
        assert_eq!(rows[0].fields[0].metadata.is_compressed, false);
        assert_eq!(rows[0].fields[0].value, StringValue("RICE".into()));
        assert_eq!(rows[0].fields[1].metadata.is_active, true);
        assert_eq!(rows[0].fields[1].metadata.is_compressed, false);
        assert_eq!(rows[0].fields[1].value, StringValue("PIPE".into()));
        assert_eq!(rows[0].fields[2].metadata.is_active, true);
        assert_eq!(rows[0].fields[2].metadata.is_compressed, false);
        assert_eq!(rows[0].fields[2].value, Float64Value(42.11));
        assert_eq!(rows[1].id, 1);
        assert_eq!(rows[1].fields[0].metadata.is_active, true);
        assert_eq!(rows[1].fields[0].metadata.is_compressed, false);
        assert_eq!(rows[1].fields[0].value, StringValue("BEEF".into()));
        assert_eq!(rows[1].fields[1].metadata.is_active, true);
        assert_eq!(rows[1].fields[1].metadata.is_compressed, false);
        assert_eq!(rows[1].fields[1].value, StringValue("CAKE".into()));
        assert_eq!(rows[1].fields[2].metadata.is_active, true);
        assert_eq!(rows[1].fields[2].metadata.is_compressed, false);
        assert_eq!(rows[1].fields[2].value, Float64Value(100.0));
        assert_eq!(df.size().unwrap(), 2);
    }

    #[test]
    fn test_create_dataframe() {
        let df = make_dataframe(
            "dataframes", "create", "quotes", make_columns()).unwrap();
        assert_eq!(df.ns.database, "dataframes");
        assert_eq!(df.ns.schema, "create");
        assert_eq!(df.ns.name, "quotes");
        assert_eq!(df.columns[0].name, "symbol");
        assert_eq!(df.columns[0].data_type, StringType(4));
        assert_eq!(df.columns[0].default_value, NullValue);
        assert_eq!(df.columns[1].name, "exchange");
        assert_eq!(df.columns[1].data_type, StringType(4));
        assert_eq!(df.columns[1].default_value, NullValue);
        assert_eq!(df.columns[2].name, "lastSale");
        assert_eq!(df.columns[2].data_type, Float64Type);
        assert_eq!(df.columns[2].default_value, NullValue);
    }

    #[test]
    fn test_fold_left() {
        // create a dataframe with 3 rows, 3 columns
        let columns = make_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let mut df = make_dataframe("dataframes", "fold_left", "quotes", columns).unwrap();
        df.append(&Row::new(0, phys_columns.clone(), vec![
            Field::new(StringValue("UNO".into())),
            Field::new(StringValue("AMEX".into())),
            Field::new(Float64Value(11.77)),
        ])).unwrap();
        df.append(&Row::new(1, phys_columns.clone(), vec![
            Field::new(StringValue("DOS".into())),
            Field::new(StringValue("AMEX".into())),
            Field::new(Float64Value(33.22)),
        ])).unwrap();
        df.append(&Row::new(2, phys_columns, vec![
            Field::new(StringValue("TRES".into())),
            Field::new(StringValue("AMEX".into())),
            Field::new(Float64Value(55.44)),
        ])).unwrap();
        assert_eq!(df.fold_left(0, |total, row| total + row.id).unwrap(), 3)
    }

    #[test]
    fn test_fold_right() {
        // create a dataframe with 3 rows, 3 columns
        let columns = make_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let mut df = make_dataframe("dataframes", "fold_right", "quotes", columns).unwrap();
        df.resize(0).unwrap();
        df.append(&Row::new(0, phys_columns.clone(), vec![
            Field::new(StringValue("ONE".into())),
            Field::new(StringValue("AMEX".into())),
            Field::new(Float64Value(11.77)),
        ])).unwrap();
        df.append(&Row::new(1, phys_columns.clone(), vec![
            Field::new(StringValue("TWO".into())),
            Field::new(StringValue("AMEX".into())),
            Field::new(Float64Value(33.22)),
        ])).unwrap();
        df.append(&Row::new(2, phys_columns, vec![
            Field::new(StringValue("THRE".into())),
            Field::new(StringValue("AMEX".into())),
            Field::new(Float64Value(55.44)),
        ])).unwrap();
        assert_eq!(df.fold_right(0, |total, row| total + row.id).unwrap(), 3)
    }

    #[test]
    fn test_for_all() {
        // create a dataframe with 3 rows, 3 columns
        let columns = make_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let mut df = make_dataframe("dataframes", "for_all", "quotes", columns).unwrap();
        df.append(&Row::new(0, phys_columns.clone(), vec![
            Field::new(StringValue("ALPH".into())),
            Field::new(StringValue("AMEX".into())),
            Field::new(Float64Value(11.77)),
        ])).unwrap();
        df.append(&Row::new(1, phys_columns.clone(), vec![
            Field::new(StringValue("BETA".into())),
            Field::new(StringValue("NYSE".into())),
            Field::new(Float64Value(33.22)),
        ])).unwrap();
        df.append(&Row::new(2, phys_columns, vec![
            Field::new(StringValue("GMMA".into())),
            Field::new(StringValue("NASD".into())),
            Field::new(Float64Value(55.44)),
        ])).unwrap();
        assert_eq!(df.for_all(|row| row.id < 5).unwrap(), true)
    }

    #[test]
    fn test_foreach_row() {
        // create a dataframe with 3 (encoded) rows
        let mut df = make_rows_from_bytes(
            "dataframes", "foreach", "quotes", make_columns(), &mut vec![
                // row 1
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 0,
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'R', b'O', b'O', b'M',
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E',
                0b1000_0000, 64, 76, 21, 194, 143, 92, 40, 246,
                // row 2
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 1,
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'R', b'O', b'O', b'M',
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'A', b'M', b'E', b'X',
                0b1000_0000, 64, 114, 0, 0, 0, 0, 0, 0,
                // row 3
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 2,
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'R', b'O', b'O', b'M',
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'A', b'S', b'D',
                0b1000_0000, 64, 39, 250, 225, 71, 174, 20, 123,
            ]).unwrap();
        df.foreach(|row| println!("{:?}", row)).unwrap()
    }

    #[test]
    fn test_load_row() {
        let ns = Namespace::new("dataframes", "load_row", "quotes");
        let df = make_rows_from_bytes(
            "dataframes", "load_row", "quotes", make_columns(), &mut vec![
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 2,
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'R', b'O', b'O', b'M',
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'A', b'S', b'D',
                0b1000_0000, 64, 39, 250, 225, 71, 174, 20, 123,
            ]).unwrap();
        let df0 = DataFrame::load(ns.clone()).unwrap();
        df0.foreach(|row| println!("{:?}", row)).unwrap();
        let (row, metadata) = df0.read_row(0).unwrap();
        assert!(metadata.is_allocated);
        assert_eq!(row.id, 2);
    }

    #[test]
    fn test_map() {
        // create a dataframe with 3 (encoded) rows
        let mut df = make_rows_from_bytes(
            "dataframes", "foreach_row", "quotes", make_columns(), &mut vec![
                // row 1
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 5,
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'A', b'L', b'P', b'H',
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E',
                0b1000_0000, 64, 76, 21, 194, 143, 92, 40, 246,
                // row 2
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 7,
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'B', b'E', b'T', b'A',
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'A', b'M', b'E', b'X',
                0b1000_0000, 64, 114, 0, 0, 0, 0, 0, 0,
                // row 3
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 9,
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'G', b'M', b'M', b'A',
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'A', b'S', b'D',
                0b1000_0000, 64, 39, 250, 225, 71, 174, 20, 123,
            ]).unwrap();
        assert_eq!(df.map(|row| row.id).unwrap(), vec![5, 7, 9])
    }

    #[test]
    fn test_overwrite_row() {
        let mut df = make_dataframe(
            "dataframes", "overwrite_row", "quotes", make_columns()).unwrap();
        let row = make_row_from_fields(2, vec![
            Field::with_value(StringValue("AMD".into())),
            Field::with_value(StringValue("NYSE".into())),
            Field::with_value(Float64Value(123.45)),
        ]);
        assert_eq!(df.overwrite(row).unwrap(), 1);
    }

    #[test]
    fn test_read_row() {
        // create a dataframe with a single (encoded) row
        let mut df = make_rows_from_bytes(
            "dataframes", "read_row", "quotes", make_columns(), &mut vec![
                0b1000_0000, 0xDE, 0xAD, 0xBA, 0xBE, 0xBE, 0xEF, 0xCA, 0xFE,
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'R', b'O', b'O', b'M',
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'K', b'I', b'N', b'G',
                0b1000_0000, 64, 83, 150, 102, 102, 102, 102, 102,
            ]).unwrap();

        // read the row
        let (row, metadata) = df.read_row(0).unwrap();
        assert!(metadata.is_allocated);
        assert_eq!(row, Row {
            id: 0xDEAD_BABE_BEEF_CAFE,
            columns: vec![
                TableColumn::new("symbol", StringType(4), NullValue, 9),
                TableColumn::new("exchange", StringType(4), NullValue, 22),
                TableColumn::new("lastSale", Float64Type, NullValue, 35),
            ],
            fields: vec![
                Field::new(StringValue("ROOM".into())),
                Field::new(StringValue("KING".into())),
                Field::new(Float64Value(78.35)),
            ],
        });
    }

    #[test]
    fn test_read_field() {
        // create a dataframe with a single (encoded) row
        let mut df = make_rows_from_bytes(
            "dataframes", "read_field", "quotes", make_columns(), &mut vec![
                0b1000_0000, 0xDE, 0xAD, 0xBA, 0xBE, 0xBE, 0xEF, 0xCA, 0xFE,
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'R', b'O', b'O', b'M',
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'K', b'I', b'N', b'G',
                0b1000_0000, 64, 83, 150, 102, 102, 102, 102, 102,
            ]).unwrap();

        let value: TypedValue = df.read_field(0, 0).unwrap();
        assert_eq!(value, StringValue("ROOM".into()));
    }

    #[test]
    fn test_resize_table() {
        let mut df = make_dataframe(
            "dataframes", "resize", "quotes", make_columns()).unwrap();
        let _ = df.resize(0).unwrap();
        df.resize(5).unwrap();
        assert_eq!(df.length().unwrap(), (5 * df.record_size) as u64);
    }

    #[test]
    fn test_reverse() {
        // create a dataframe with 3 rows, 3 columns
        let columns = make_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let mut df = make_dataframe("dataframes", "reverse", "quotes", columns).unwrap();
        let row_a = Row::new(0, phys_columns.clone(), vec![
            Field::new(StringValue("A".into())),
            Field::new(StringValue("AMEX".into())),
            Field::new(Float64Value(11.77)),
        ]);
        let row_b = Row::new(1, phys_columns.clone(), vec![
            Field::new(StringValue("BB".into())),
            Field::new(StringValue("AMEX".into())),
            Field::new(Float64Value(11.77)),
        ]);
        let row_c = Row::new(2, phys_columns.clone(), vec![
            Field::new(StringValue("CCC".into())),
            Field::new(StringValue("AMEX".into())),
            Field::new(Float64Value(55.44)),
        ]);
        df.append(&row_a).unwrap();
        df.append(&row_b).unwrap();
        df.append(&row_c).unwrap();
        assert_eq!(df.reverse().unwrap(), vec![row_c, row_b, row_a])
    }
}