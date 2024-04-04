////////////////////////////////////////////////////////////////////
// dataframes module
////////////////////////////////////////////////////////////////////

use std::ops::AddAssign;

use crate::dataframe_config::DataFrameConfig;
use crate::fields::Field;
use crate::file_row_collection::FileRowCollection;
use crate::namespaces::Namespace;
use crate::row_collection::RowCollection;
use crate::row_metadata::RowMetadata;
use crate::rows::Row;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{Null, Undefined};

/// DataFrame is a logical representation of table
#[derive(Debug)]
pub struct DataFrame {
    ns: Namespace,
    columns: Vec<TableColumn>,
    device: Box<dyn RowCollection>,
}

impl DataFrame {
    /// creates a new [DataFrame]; persisting its configuration to disk.
    pub fn create(ns: Namespace, config: DataFrameConfig) -> std::io::Result<Self> {
        config.save(&ns)?;
        let table_columns = TableColumn::from_columns(&config.columns)?;
        let device = Box::new(FileRowCollection::create(ns.clone(), table_columns.clone())?);
        Ok(Self::new(ns, table_columns, device))
    }

    /// loads a dataframe from disk.
    pub fn load(ns: Namespace) -> std::io::Result<Self> {
        let cfg = DataFrameConfig::load(&ns)?;
        let device = Box::new(FileRowCollection::open(&ns)?);
        let table_columns = TableColumn::from_columns(&cfg.columns)?;
        Ok(Self::new(ns, table_columns, device))
    }

    /// creates a new dataframe.
    pub fn new(ns: Namespace, columns: Vec<TableColumn>, device: Box<dyn RowCollection>) -> Self {
        Self { ns, columns, device }
    }

    /// appends a new row to the table
    pub fn append(&mut self, row: &Row) -> std::io::Result<usize> {
        let new_row_id = self.device.len()?;
        self.device.overwrite(new_row_id, &row.with_row_id(new_row_id))
    }

    /// deletes an existing row from the table
    pub fn delete(&mut self, id: usize) -> std::io::Result<usize> {
        self.device.overwrite_row_metadata(id, &RowMetadata::new(false))
    }

    /// performs a top-down fold operation
    pub fn fold_left<A>(&self, init: A, f: fn(A, Row) -> A) -> std::io::Result<A> {
        let mut result: A = init;
        for id in 0..self.len()? {
            let (row, metadata) = self.read_row(id)?;
            if metadata.is_allocated { result = f(result, row) }
        }
        Ok(result)
    }

    /// performs a bottom-up fold operation
    pub fn fold_right<A>(&self, init: A, f: fn(A, Row) -> A) -> std::io::Result<A> {
        let mut result: A = init;
        for id in (0..self.len()?).rev() {
            let (row, metadata) = self.read_row(id)?;
            if metadata.is_allocated { result = f(result, row) }
        }
        Ok(result)
    }

    /// returns true if all allocated rows satisfy the provided function
    pub fn for_all(&self, f: fn(&Row) -> bool) -> std::io::Result<bool> {
        for id in 0..self.len()? {
            let (row, metadata) = self.read_row(id)?;
            if metadata.is_allocated && !f(&row) { return Ok(false); }
        }
        Ok(true)
    }

    /// iterates through all allocated rows
    pub fn foreach(&self, f: fn(&Row) -> ()) -> std::io::Result<()> {
        for id in 0..self.len()? {
            let (row, metadata) = self.read_row(id)?;
            if metadata.is_allocated { f(&row) }
        }
        Ok(())
    }

    /// returns the columns that define the table structure
    pub fn get_columns(&self) -> &Vec<TableColumn> { &self.columns }

    /// returns the allocated sizes the table (in numbers of rows)
    pub fn len(&self) -> std::io::Result<usize> {
        self.device.len()
    }

    /// transforms the collection of rows into a collection of [A]
    pub fn map<A>(&self, f: fn(Row) -> A) -> std::io::Result<Vec<A>> {
        let mut items = Vec::new();
        for id in 0..self.len()? {
            match self.read_then(id, f)? {
                None => {}
                Some(item) => items.push(item)
            }
        }
        Ok(items)
    }

    /// overwrites a specified row by ID
    pub fn overwrite(&mut self, row: Row) -> std::io::Result<usize> {
        self.device.overwrite(row.get_id(), &row)
    }

    /// overwrites the metadata of a specified row by ID
    pub fn overwrite_row_metadata(&mut self, id: usize, metadata: &RowMetadata) -> std::io::Result<usize> {
        self.device.overwrite_row_metadata(id, metadata)
    }

    /// reads the specified field value from the specified row ID
    pub fn read_field(&self, id: usize, column_id: usize) -> std::io::Result<TypedValue> {
        self.device.read_field(id, column_id)
    }

    /// reads a row by ID
    pub fn read_one(&self, id: usize) -> std::io::Result<Option<Row>> {
        let (row, metadata) = self.device.read(id)?;
        Ok(if metadata.is_allocated { Some(row) } else { None })
    }

    /// reads a range of rows
    pub fn read_range(&self, index: std::ops::Range<usize>) -> std::io::Result<Vec<Row>> {
        self.device.read_range(index)
    }

    /// reads a row by ID
    pub fn read_row(&self, id: usize) -> std::io::Result<(Row, RowMetadata)> {
        self.device.read(id)
    }

    /// reads a row and transforms it returning the [Option] of [A]
    pub fn read_then<A>(&self, id: usize, f: fn(Row) -> A) -> std::io::Result<Option<A>> {
        let (row, metadata) = self.read_row(id)?;
        Ok(if metadata.is_allocated { Some(f(row)) } else { None })
    }

    /// reads a row and pushes it into the specified vector if active
    pub fn read_then_push(&self, id: usize, rows: &mut Vec<Row>) -> std::io::Result<()> {
        let (row, metadata) = self.read_row(id)?;
        Ok(if metadata.is_allocated { rows.push(row) } else { () })
    }

    /// resizes the table
    pub fn resize(&mut self, new_size: usize) -> std::io::Result<usize> {
        self.device.resize(new_size)?;
        Ok(1)
    }

    /// returns the rows in reverse order
    pub fn reverse(&self) -> std::io::Result<Vec<Row>> {
        let size = self.len()?;
        let mut rows: Vec<Row> = Vec::with_capacity(size);
        for id in (0..size).rev() {
            self.read_then_push(id, &mut rows)?;
        }
        Ok(rows)
    }

    /// restores a deleted row to an active state
    pub fn undelete(&mut self, id: usize) -> std::io::Result<usize> {
        self.overwrite_row_metadata(id, &RowMetadata::new(true))
    }

    /// updates a specified row by ID
    pub fn update(&mut self, row: Row) -> std::io::Result<usize> {
        // retrieve the original row
        let (orig_row, orig_rmd) = self.read_row(row.get_id())?;

        // if we retrieved an active row, construct a composite row
        let new_row = if orig_rmd.is_allocated {
            let fields = orig_row.get_fields().iter().zip(row.get_fields().iter()).map(|(a, b)| {
                Field::new(match (&b.value, &a.value) {
                    (b, _)  if *b != Undefined => b.clone(),
                    (_, a)  if *a != Undefined => a.clone(),
                    _ => Null
                })
            }).collect();
            Row::new(row.get_id(), self.columns.clone(), fields)
        } else { row };

        // update the table
        self.overwrite(self.replace_undefined_with_null(new_row))
    }

    fn replace_undefined_with_null(&self, row: Row) -> Row {
        let columns = self.columns.clone();
        Row::new(row.get_id(), columns.clone(), columns.iter().zip(row.get_fields().iter()).map(|(c, f)| {
            if f.value == Null || f.value == Undefined { Field::with_default(c) } else { f.clone() }
        }).collect())
    }
}

impl AddAssign for DataFrame {
    fn add_assign(&mut self, rhs: Self) {
        fn do_add(lhs: &mut DataFrame, rhs: DataFrame) -> std::io::Result<()> {
            for id in 0..rhs.len()? {
                let (row, metadata) = rhs.read_row(id)?;
                if metadata.is_allocated { lhs.append(&row)?; }
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
    use std::time::{SystemTime, UNIX_EPOCH};

    use rand::{Rng, RngCore, thread_rng};

    use shared_lib::cnv_error;

    use crate::data_types::DataType::{Float64Type, StringType};
    use crate::dataframes::DataFrame;
    use crate::fields::Field;
    use crate::namespaces::Namespace;
    use crate::row;
    use crate::rows::Row;
    use crate::table_columns::TableColumn;
    use crate::testdata::*;
    use crate::typed_values::TypedValue;
    use crate::typed_values::TypedValue::{Float64Value, Null, StringValue, Undefined};

    #[test]
    fn test_add_assign() {
        let columns = make_table_columns();
        // create a dataframe with a single row
        let mut df0 = make_dataframe("dataframes", "add_assign", "quotes0", make_columns()).unwrap();
        df0.append(&make_quote(0, &columns, "RACE", "NASD", 123.45)).unwrap();
        // create a second dataframe with a single row
        let mut df1 = make_dataframe("dataframes", "add_assign", "quotes1", make_columns()).unwrap();
        df1.append(&make_quote(0, &columns, "BEER", "AMEX", 357.12)).unwrap();
        // concatenate the dataframes
        df0 += df1;
        // re-read the rows
        let rows = df0.read_range(0..df0.len().unwrap()).unwrap();
        assert_eq!(rows, vec![
            make_quote(0, &columns, "RACE", "NASD", 123.45),
            make_quote(1, &columns, "BEER", "AMEX", 357.12),
        ]);
    }

    #[test]
    fn test_append_row_then_read_rows() {
        let columns = make_table_columns();
        // create a dataframe with a single (encoded) row
        let mut df = make_dataframe("dataframes", "append", "quotes", make_columns()).unwrap();
        assert_eq!(1, df.append(&make_quote(0, &columns, "RICE", "PIPE", 42.11)).unwrap());
        // create a second row and append it to the dataframe
        assert_eq!(1, df.append(&make_quote(0, &columns, "BEEF", "CAKE", 100.0)).unwrap());

        // verify the rows
        let rows = df.read_range(0..df.len().unwrap()).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], make_quote(0, &columns, "RICE", "PIPE", 42.11));
        assert_eq!(rows[1], make_quote(1, &columns, "BEEF", "CAKE", 100.0));
        assert_eq!(df.len().unwrap(), 2);
    }

    #[test]
    fn test_create_dataframe() {
        let df = make_dataframe(
            "dataframes", "create", "quotes", make_columns()).unwrap();
        assert_eq!(df.ns.database, "dataframes");
        assert_eq!(df.ns.schema, "create");
        assert_eq!(df.ns.name, "quotes");
        assert_eq!(df.columns[0].get_name(), "symbol");
        assert_eq!(df.columns[0].data_type, StringType(4));
        assert_eq!(df.columns[0].default_value, Null);
        assert_eq!(df.columns[1].get_name(), "exchange");
        assert_eq!(df.columns[1].data_type, StringType(4));
        assert_eq!(df.columns[1].default_value, Null);
        assert_eq!(df.columns[2].get_name(), "lastSale");
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
        let rows = df.read_range(0..df.len().unwrap()).unwrap();
        assert_eq!(rows, vec![
            row!(0, phys_columns, vec![
                StringValue("UNO".into()), StringValue("AMEX".into()), Float64Value(11.77),
            ]),
            row!(2, phys_columns, vec![
                StringValue("TRES".into()), StringValue("AMEX".into()), Float64Value(55.44),
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
        assert_eq!(df.fold_left(0, |total, row| total + row.get_id()).unwrap(), 3)
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
        assert_eq!(df.fold_right(0, |total, row| total + row.get_id()).unwrap(), 3)
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
        assert_eq!(df.for_all(|row| row.get_id() < 5).unwrap(), true)
    }

    #[test]
    fn test_foreach_row() {
        let mut df = make_dataframe(
            "dataframes", "foreach_row", "quotes", make_columns()).unwrap();
        assert_eq!(df.append(&make_quote(0, &make_table_columns(), "WE", "NYSE", 123.45)).unwrap(), 1);
        assert_eq!(df.append(&make_quote(0, &make_table_columns(), "CAN", "NYSE", 88.22)).unwrap(), 1);
        assert_eq!(df.append(&make_quote(0, &make_table_columns(), "FLY", "AMEX", 51.11)).unwrap(), 1);
        assert_eq!(df.append(&make_quote(0, &make_table_columns(), "FAR", "NYSE", 42.33)).unwrap(), 1);
        assert_eq!(df.append(&make_quote(0, &make_table_columns(), "AWAY", "AMEX", 9.73)).unwrap(), 1);
        df.foreach(|row| println!("{:?}", row)).unwrap()
    }

    #[test]
    fn test_load() {
        let ns = Namespace::new("dataframes", "load", "quotes");
        let mut df = make_dataframe(&ns.database, &ns.schema, &ns.name, make_columns()).unwrap();
        let columns = make_table_columns();
        df.append(&make_quote(0, &columns, "SPAM", "NYSE", 11.99));

        let df0 = DataFrame::load(ns.clone()).unwrap();
        let (row, metadata) = df0.read_row(0).unwrap();
        assert!(metadata.is_allocated);
        assert_eq!(row, make_quote(0, &columns, "SPAM", "NYSE", 11.99));
    }

    #[test]
    fn test_map() {
        let mut df = make_dataframe(
            "dataframes", "transform", "quotes", make_columns()).unwrap();
        assert_eq!(df.append(&make_quote(0, &make_table_columns(), "WE", "NYSE", 123.45)).unwrap(), 1);
        assert_eq!(df.append(&make_quote(0, &make_table_columns(), "CAN", "NYSE", 88.22)).unwrap(), 1);
        assert_eq!(df.append(&make_quote(0, &make_table_columns(), "FLY", "AMEX", 51.11)).unwrap(), 1);
        assert_eq!(df.append(&make_quote(0, &make_table_columns(), "FAR", "NYSE", 42.33)).unwrap(), 1);
        assert_eq!(df.append(&make_quote(0, &make_table_columns(), "AWAY", "AMEX", 9.73)).unwrap(), 1);
        assert_eq!(df.map(|row| row.get("lastSale")).unwrap(), vec![
            Float64Value(123.45), Float64Value(88.22), Float64Value(51.11),
            Float64Value(42.33), Float64Value(9.73),
        ])
    }

    #[test]
    fn test_overwrite_row() {
        let mut df = make_dataframe(
            "dataframes", "overwrite_row", "quotes", make_columns()).unwrap();
        let row = make_quote(2, &make_table_columns(), "AMD", "NYSE", 123.45);
        assert_eq!(df.overwrite(row).unwrap(), 1);
    }

    #[test]
    fn test_read_one() {
        let mut df = make_dataframe(
            "dataframes", "read_one", "quotes", make_columns()).unwrap();
        let row = make_quote(0, &make_table_columns(), "AMD", "NYSE", 123.45);
        assert_eq!(df.overwrite(row).unwrap(), 1);
        assert_eq!(df.read_one(0).unwrap(), Some(make_quote(0, &make_table_columns(), "AMD", "NYSE", 123.45)))
    }

    #[test]
    fn test_read_range() {
        let mut df = make_dataframe(
            "dataframes", "read_range", "quotes", make_columns()).unwrap();
        assert_eq!(df.append(&make_quote(0, &make_table_columns(), "WE", "NYSE", 123.45)).unwrap(), 1);
        assert_eq!(df.append(&make_quote(0, &make_table_columns(), "CAN", "NYSE", 88.22)).unwrap(), 1);
        assert_eq!(df.append(&make_quote(0, &make_table_columns(), "FLY", "AMEX", 51.11)).unwrap(), 1);
        assert_eq!(df.append(&make_quote(0, &make_table_columns(), "FAR", "NYSE", 42.33)).unwrap(), 1);
        assert_eq!(df.append(&make_quote(0, &make_table_columns(), "AWAY", "AMEX", 123.45)).unwrap(), 1);
        assert_eq!(df.read_range(1..4).unwrap(), vec![
            make_quote(1, &make_table_columns(), "CAN", "NYSE", 88.22),
            make_quote(2, &make_table_columns(), "FLY", "AMEX", 51.11),
            make_quote(3, &make_table_columns(), "FAR", "NYSE", 42.33),
        ])
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
        assert_eq!(df.len().unwrap(), 5);
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
        let row_0 = row!(0, phys_columns, vec![
            StringValue("UNO".into()), StringValue("AMEX".into()), Float64Value(11.77),
        ]);
        let row_1 = row!(1, phys_columns, vec![
            StringValue("DOS".into()), StringValue("AMEX".into()), Float64Value(33.22),
        ]);
        let row_2 = row!(2, phys_columns, vec![
            StringValue("TRES".into()), StringValue("AMEX".into()), Float64Value(55.44),
        ]);

        // verify the row was deleted
        let rows = df.read_range(0..df.len().unwrap()).unwrap();
        assert_eq!(rows, vec![row_0.clone(), row_2.clone()]);

        // restore the middle row
        assert_eq!(df.undelete(1).unwrap(), 1);

        // verify the row was restored
        let rows = df.read_range(0..df.len().unwrap()).unwrap();
        assert_eq!(rows, vec![row_0.clone(), row_1.clone(), row_2.clone()]);
    }

    #[test]
    fn test_update_row() {
        // create a dataframe with 3 rows, 3 columns
        let columns = make_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let mut df = make_dataframe("dataframes", "update_row", "quotes", columns).unwrap();
        df.append(&make_quote(0, &phys_columns, "DIAS", "NYSE", 99.99)).unwrap();
        df.append(&make_quote(1, &phys_columns, "DORA", "AMEX", 33.32)).unwrap();
        df.append(&make_quote(2, &phys_columns, "INFO", "NASD", 22.00)).unwrap();

        // update the middle row
        let row_to_update = row!(1, phys_columns, vec![
            Undefined, Undefined, Float64Value(33.33),
        ]);
        assert_eq!(df.update(row_to_update.clone()).unwrap(), 1);

        // verify the row was updated
        let (updated_row, updated_rmd) = df.read_row(row_to_update.get_id()).unwrap();
        assert!(updated_rmd.is_allocated);
        assert_eq!(updated_row, row!(1, phys_columns, vec![
            StringValue("DORA".into()), StringValue("AMEX".into()), Float64Value(33.33),
        ]))
    }

    #[test]
    fn performance_test() -> std::io::Result<()> {
        let columns = vec![
            TableColumn::new("symbol", StringType(4), Null, 9),
            TableColumn::new("exchange", StringType(8), Null, 22),
            TableColumn::new("lastSale", Float64Type, Null, 39),
        ];
        let total = 100_000;
        let mut df = make_dataframe(
            "dataframes", "performance_test", "quotes", make_columns()).unwrap();

        test_write_performance(&mut df, &columns, total)?;
        test_read_performance(&df)?;
        Ok(())
    }

    fn test_write_performance(df: &mut DataFrame, columns: &Vec<TableColumn>, total: usize) -> std::io::Result<()> {
        use rand::distributions::Uniform;
        use rand::prelude::ThreadRng;
        let exchanges = ["AMEX", "NASDAQ", "NYSE", "OTCBB", "OTHEROTC"];
        let mut rng: ThreadRng = thread_rng();
        let start_time = SystemTime::now().duration_since(UNIX_EPOCH)
            .map_err(|e| cnv_error!(e))?.as_millis();
        for _ in 0..total {
            let symbol: String = (0..4)
                .map(|_| rng.gen_range(b'A'..=b'Z') as char)
                .collect();
            let exchange = exchanges[rng.next_u32() as usize % exchanges.len()];
            let last_sale = 400.0 * rng.sample(Uniform::new(0.0, 1.0));
            let row = make_quote(0, &columns, &symbol, exchange, last_sale);
            df.append(&row)?;
        }
        let end_time = SystemTime::now().duration_since(UNIX_EPOCH)
            .map_err(|e| cnv_error!(e))?.as_millis();
        let elapsed_time = end_time - start_time;
        let elapsed_time_sec = elapsed_time as f64 / 1000.;
        let rpm = total as f64 / elapsed_time as f64;
        println!("wrote {} row(s) in {} msec ({:.2} seconds, {:.2} records/msec)",
                 total, elapsed_time, elapsed_time_sec, rpm);
        Ok(())
    }

    fn test_read_performance(df: &DataFrame) -> std::io::Result<()> {
        let limit = df.len()?;
        let mut total = 0;
        let start_time = SystemTime::now().duration_since(UNIX_EPOCH)
            .map_err(|e| cnv_error!(e))?.as_millis();
        for id in 0..limit {
            let (_row, rmd) = df.read_row(id)?;
            if rmd.is_allocated { total += 1; }
        }
        let end_time = SystemTime::now().duration_since(UNIX_EPOCH)
            .map_err(|e| cnv_error!(e))?.as_millis();
        let elapsed_time = end_time - start_time;
        let elapsed_time_sec = elapsed_time as f64 / 1000.;
        let rpm = total as f64 / elapsed_time as f64;
        println!("read {} row(s) in {} msec ({:.2} seconds, {:.2} records/msec)",
                 total, elapsed_time, elapsed_time_sec, rpm);
        Ok(())
    }
}