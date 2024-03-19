////////////////////////////////////////////////////////////////////
// dataframes module
////////////////////////////////////////////////////////////////////

use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Seek, SeekFrom, Write};
use std::ops::AddAssign;
use std::os::unix::fs::FileExt;

use crate::columns::Column;
use crate::dataframe_config::DataFrameConfig;
use crate::fields::Field;
use crate::namespaces::Namespace;
use crate::row_metadata::RowMetadata;
use crate::rows::Row;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{Null, Undefined};

/// DataFrame is a logical representation of table
#[derive(Debug)]
pub struct DataFrame {
    pub(crate) ns: Namespace,
    pub(crate) columns: Vec<TableColumn>,
    pub(crate) record_size: usize,
    //#[serde(skip_serializing, skip_deserializing)]
    pub(crate) file: File,
}

impl DataFrame {
    /// creates a new [DataFrame]; persisting its configuration to disk.
    pub fn create(ns: Namespace, config: DataFrameConfig) -> io::Result<DataFrame> {
        config.save(&ns)?;
        let file = Self::open_crw(&ns)?;
        Self::new(ns, &config.columns, file)
    }

    /// loads a dataframe from disk.
    pub fn load(ns: Namespace) -> io::Result<DataFrame> {
        let cfg = DataFrameConfig::load(&ns)?;
        let file = Self::open_rw(&ns)?;
        DataFrame::new(ns, &cfg.columns, file)
    }

    /// creates a new dataframe.
    pub fn new(ns: Namespace, columns: &Vec<Column>, file: File) -> io::Result<DataFrame> {
        let columns = TableColumn::from_columns(columns)?;
        let record_size = Self::compute_record_size(&columns);
        Ok(DataFrame { ns, columns, record_size, file })
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

    /// deletes an existing row from the table
    pub fn delete(&mut self, id: usize) -> io::Result<usize> {
        self.overwrite_row_metadata(id, &RowMetadata::new(false))
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
                rows.push(f(&row));
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

    /// overwrites the metadata of a specified row by ID
    pub fn overwrite_row_metadata(&mut self, id: usize, metadata: &RowMetadata) -> io::Result<usize> {
        let offset = self.to_offset(id);
        let bytes = [metadata.encode()];
        let _ = &self.file.write_at(&bytes, offset)?;
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
    pub fn read_rows(&self, index: std::ops::Range<usize>) -> io::Result<Vec<Row>> {
        let mut rows: Vec<Row> = Vec::with_capacity(index.len());
        for id in index {
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

    fn to_offset(&self, id: usize) -> u64 { (id as u64) * (self.record_size as u64) }

    /// restores a deleted row to an active state
    pub fn undelete(&mut self, id: usize) -> io::Result<usize> {
        self.overwrite_row_metadata(id, &RowMetadata::new(true))
    }

    /// updates a specified row by ID
    pub fn update(&mut self, row: Row) -> io::Result<usize> {
        // retrieve the original row
        let (orig_row, orig_rmd) = self.read_row(row.id)?;

        // if we retrieved an active row, construct a composite row
        let new_row = if orig_rmd.is_allocated {
            let mut fields = vec![];
            for (b, a) in row.fields.iter().zip(orig_row.fields.iter()) {
                let value = if b.value != Undefined { &b.value } else { &a.value };
                fields.push(Field::new(value.clone()));
            }
            Row::new(row.id, self.columns.clone(), fields)
        } else { row };

        // update the table
        self.overwrite(self.replace_undefined_with_null(new_row))
    }

    fn replace_undefined_with_null(&self, row: Row) -> Row {
        let columns = self.columns.clone();
        Row::new(row.id, columns.clone(), columns.iter().zip(row.fields.iter()).map(|(c, f)| {
            if f.value == Null || f.value == Undefined { Field::with_default(c) } else { f.clone() }
        }).collect())
    }
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
    use crate::typed_values::TypedValue::{Float64Value, Null, StringValue, Undefined};

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
        let rows = df0.read_rows(0..df0.size().unwrap()).unwrap();
        let columns = vec![
            TableColumn::new("symbol", StringType(4), Null, 9),
            TableColumn::new("exchange", StringType(4), Null, 22),
            TableColumn::new("lastSale", Float64Type, Null, 35),
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
        let rows = df.read_rows(0..2).unwrap();
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
        assert_eq!(df.columns[0].default_value, Null);
        assert_eq!(df.columns[1].name, "exchange");
        assert_eq!(df.columns[1].data_type, StringType(4));
        assert_eq!(df.columns[1].default_value, Null);
        assert_eq!(df.columns[2].name, "lastSale");
        assert_eq!(df.columns[2].data_type, Float64Type);
        assert_eq!(df.columns[2].default_value, Null);
    }

    #[test]
    fn test_delete_row() {
        // create a dataframe with 3 rows, 3 columns
        let columns = make_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let mut df = make_dataframe("dataframes", "delete_row", "quotes", columns).unwrap();
        df.append(&make_quote(0, &phys_columns, "UNO", "AMEX", 11.77)).unwrap();
        df.append(&make_quote(1, &phys_columns, "DOS", "AMEX", 33.22)).unwrap();
        df.append(&make_quote(2, &phys_columns, "TRES", "AMEX", 55.44)).unwrap();

        // delete the middle row
        assert_eq!(df.delete(1).unwrap(), 1);

        // verify the rows
        let rows = df.read_rows(0..df.size().unwrap()).unwrap();
        assert_eq!(rows, vec![
            Row::new(0, phys_columns.clone(), vec![
                Field::new(StringValue("UNO".into())),
                Field::new(StringValue("AMEX".into())),
                Field::new(Float64Value(11.77)),
            ]),
            Row::new(2, phys_columns.clone(), vec![
                Field::new(StringValue("TRES".into())),
                Field::new(StringValue("AMEX".into())),
                Field::new(Float64Value(55.44)),
            ]),
        ]);
    }

    #[test]
    fn test_fold_left() {
        // create a dataframe with 3 rows, 3 columns
        let columns = make_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let mut df = make_dataframe("dataframes", "fold_left", "quotes", columns).unwrap();
        df.append(&make_quote(0, &phys_columns, "UNO", "AMEX", 11.77)).unwrap();
        df.append(&make_quote(1, &phys_columns, "DOS", "AMEX", 33.22)).unwrap();
        df.append(&make_quote(2, &phys_columns, "TRES", "AMEX", 55.44)).unwrap();
        assert_eq!(df.fold_left(0, |total, row| total + row.id).unwrap(), 3)
    }

    #[test]
    fn test_fold_right() {
        // create a dataframe with 3 rows, 3 columns
        let columns = make_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let mut df = make_dataframe("dataframes", "fold_right", "quotes", columns).unwrap();
        df.append(&make_quote(0, &phys_columns, "ONE", "AMEX", 11.77)).unwrap();
        df.append(&make_quote(1, &phys_columns, "TWO", "AMEX", 33.22)).unwrap();
        df.append(&make_quote(2, &phys_columns, "THRE", "AMEX", 55.44)).unwrap();
        assert_eq!(df.fold_right(0, |total, row| total + row.id).unwrap(), 3)
    }

    #[test]
    fn test_for_all() {
        // create a dataframe with 3 rows, 3 columns
        let columns = make_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let mut df = make_dataframe("dataframes", "for_all", "quotes", columns).unwrap();
        df.append(&make_quote(0, &phys_columns, "ALPH", "AMEX", 11.77)).unwrap();
        df.append(&make_quote(1, &phys_columns, "BETA", "NYSE", 33.22)).unwrap();
        df.append(&make_quote(2, &phys_columns, "GMMA", "NASD", 55.44)).unwrap();
        assert_eq!(df.for_all(|row| row.id < 5).unwrap(), true)
    }

    #[test]
    fn test_foreach_row() {
        // create a dataframe with 3 (encoded) rows
        let df = make_rows_from_bytes(
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
        let _ = make_rows_from_bytes(
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
        let df = make_rows_from_bytes(
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
        let row = make_quote(2, &make_table_columns(), "AMD", "NYSE", 123.45);
        assert_eq!(df.overwrite(row).unwrap(), 1);
    }

    #[test]
    fn test_read_row() {
        // create a dataframe with a single (encoded) row
        let df = make_rows_from_bytes(
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
                TableColumn::new("symbol", StringType(4), Null, 9),
                TableColumn::new("exchange", StringType(4), Null, 22),
                TableColumn::new("lastSale", Float64Type, Null, 35),
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
        let df = make_rows_from_bytes(
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
        let row_a = make_quote(0, &phys_columns, "A", "AMEX", 11.77);
        let row_b = make_quote(1, &phys_columns, "BB", "AMEX", 33.22);
        let row_c = make_quote(2, &phys_columns, "CCC", "AMEX", 55.44);
        for row in vec![&row_a, &row_b, &row_c] { df.append(row).unwrap(); }
        assert_eq!(df.reverse().unwrap(), [row_c, row_b, row_a]);
    }

    #[test]
    fn test_undelete_row() {
        // create a dataframe with 3 rows, 3 columns
        let columns = make_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let mut df = make_dataframe("dataframes", "undelete_row", "quotes", columns).unwrap();
        df.append(&make_quote(0, &phys_columns, "UNO", "AMEX", 11.77)).unwrap();
        df.append(&make_quote(1, &phys_columns, "DOS", "AMEX", 33.22)).unwrap();
        df.append(&make_quote(2, &phys_columns, "TRES", "AMEX", 55.44)).unwrap();

        // delete the middle row
        assert_eq!(df.delete(1).unwrap(), 1);

        // define the verification rows
        let row_0 = Row::new(0, phys_columns.clone(), vec![
            Field::new(StringValue("UNO".into())),
            Field::new(StringValue("AMEX".into())),
            Field::new(Float64Value(11.77)),
        ]);
        let row_1 = Row::new(1, phys_columns.clone(), vec![
            Field::new(StringValue("DOS".into())),
            Field::new(StringValue("AMEX".into())),
            Field::new(Float64Value(33.22)),
        ]);
        let row_2 = Row::new(2, phys_columns.clone(), vec![
            Field::new(StringValue("TRES".into())),
            Field::new(StringValue("AMEX".into())),
            Field::new(Float64Value(55.44)),
        ]);

        // verify the row was deleted
        let rows = df.read_rows(0..df.size().unwrap()).unwrap();
        assert_eq!(rows, vec![row_0.clone(), row_2.clone()]);

        // restore the middle row
        assert_eq!(df.undelete(1).unwrap(), 1);

        // verify the row was restored
        let rows = df.read_rows(0..df.size().unwrap()).unwrap();
        assert_eq!(rows, vec![row_0.clone(), row_1.clone(), row_2.clone()]);
    }

    #[test]
    fn test_update_row() {
        // create a dataframe with 3 rows, 3 columns
        let columns = make_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let mut df = make_dataframe("dataframes", "test_update_row", "quotes", columns).unwrap();
        df.append(&make_quote(0, &phys_columns, "DIAS", "NYSE", 99.99)).unwrap();
        df.append(&make_quote(1, &phys_columns, "DORA", "AMEX", 33.32)).unwrap();
        df.append(&make_quote(2, &phys_columns, "INFO", "NASD", 22.00)).unwrap();

        // update the middle row
        let row_to_update = Row::new(1, phys_columns.clone(), vec![
            Field::new(Undefined),
            Field::new(Undefined),
            Field::new(Float64Value(33.33)),
        ]);
        assert_eq!(df.update(row_to_update.clone()).unwrap(), 1);

        // verify the row was updated
        let (updated_row, updated_rmd) = df.read_row(row_to_update.id).unwrap();
        assert!(updated_rmd.is_allocated);
        assert_eq!(updated_row, Row::new(1, phys_columns.clone(), vec![
            Field::new(StringValue("DORA".into())),
            Field::new(StringValue("AMEX".into())),
            Field::new(Float64Value(33.33)),
        ]))
    }
}