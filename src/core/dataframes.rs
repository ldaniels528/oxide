////////////////////////////////////////////////////////////////////
// dataframes module
////////////////////////////////////////////////////////////////////

use std::collections::HashMap;
use std::ops::AddAssign;

use shared_lib::fail;

use crate::compiler::fail_value;
use crate::dataframe_config::DataFrameConfig;
use crate::expression::{Conditions, Expression};
use crate::field_metadata::FieldMetadata;
use crate::file_row_collection::FileRowCollection;
use crate::machine::Machine;
use crate::namespaces::Namespace;
use crate::row_collection::RowCollection;
use crate::row_metadata::RowMetadata;
use crate::rows::Row;
use crate::table_columns::Column;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{Null, Undefined};

/// DataFrame is a logical representation of table
#[derive(Debug)]
pub struct DataFrame {
    device: Box<dyn RowCollection>,
}

impl DataFrame {
    /// creates a new [DataFrame]; persisting its configuration to disk.
    pub fn create(ns: Namespace, config: DataFrameConfig) -> std::io::Result<Self> {
        config.save(&ns)?;
        let table_columns = Column::from_parameters(config.get_columns())?;
        let device = Box::new(FileRowCollection::create_table(&ns, table_columns.to_owned())?);
        Ok(Self::new(device))
    }

    /// creates a new [DataFrame] from an existing [RowCollection]
    pub fn from_row_collection(rc: Box<dyn RowCollection>) -> Self {
        Self::new(rc)
    }

    /// loads a dataframe from disk.
    pub fn load(ns: &Namespace) -> std::io::Result<Self> {
        let device = Box::new(FileRowCollection::open(ns)?);
        Ok(Self::new(device))
    }

    /// creates a new dataframe.
    pub fn new(device: Box<dyn RowCollection>) -> Self {
        Self { device }
    }

    /// appends a new row to the table
    pub fn append(&mut self, row: Row) -> std::io::Result<usize> {
        match self.device.append_row(row) {
            TypedValue::ErrorValue(err) => fail(err.to_string()),
            TypedValue::Outcome(oc) => Ok(oc.to_update_count()),
            other => fail_value("Outcome", &other)
        }
    }

    /// deletes an existing row by ID from the table
    pub fn delete(&mut self, id: usize) -> std::io::Result<usize> {
        match self.device.delete_row(id) {
            TypedValue::ErrorValue(err) => fail(err.to_string()),
            TypedValue::Outcome(oc) => Ok(oc.to_update_count()),
            other => fail_value("Outcome", &other)
        }
    }

    /// deletes rows from the table based on a condition
    pub fn delete_where(
        &mut self,
        machine: &Machine,
        condition: &Option<Conditions>,
        limit: TypedValue,
    ) -> std::io::Result<usize> {
        let mut deleted = 0;
        for id in self.device.get_indices_with_limit(limit)? {
            // read an active row
            if let Some(row) = self.device.read_one(id)? {
                // if the predicate matches the condition, delete the row.
                if row.matches(machine, condition, self.get_columns()) {
                    deleted += self.delete(id)?;
                }
            }
        }
        Ok(deleted)
    }

    /// performs a top-down fold operation
    pub fn fold_left<A>(&self, init: A, f: fn(A, Row) -> A) -> std::io::Result<A> {
        let mut result: A = init;
        for id in self.device.get_indices()? {
            let (row, metadata) = self.device.read_row(id)?;
            if metadata.is_allocated { result = f(result, row) }
        }
        Ok(result)
    }

    /// performs a bottom-up fold operation
    pub fn fold_right<A>(&self, init: A, f: fn(A, Row) -> A) -> std::io::Result<A> {
        let mut result: A = init;
        for id in self.device.get_indices()?.rev() {
            let (row, metadata) = self.device.read_row(id)?;
            if metadata.is_allocated { result = f(result, row) }
        }
        Ok(result)
    }

    /// returns true if all allocated rows satisfy the provided function
    pub fn for_all(&self, f: fn(&Row) -> bool) -> std::io::Result<bool> {
        for id in self.device.get_indices()? {
            let (row, metadata) = self.device.read_row(id)?;
            if metadata.is_allocated && !f(&row) { return Ok(false); }
        }
        Ok(true)
    }

    /// iterates through all allocated rows
    pub fn foreach(&self, f: fn(&Row) -> ()) -> std::io::Result<()> {
        for id in self.device.get_indices()? {
            let (row, metadata) = self.device.read_row(id)?;
            if metadata.is_allocated { f(&row) }
        }
        Ok(())
    }

    /// returns the columns that define the table structure
    pub fn get_columns(&self) -> &Vec<Column> {
        self.device.get_columns()
    }

    /// returns the allocated sizes the table (in numbers of rows)
    pub fn len(&self) -> std::io::Result<usize> {
        self.device.len()
    }

    /// transforms the collection of rows into a collection of [A]
    pub fn map<A>(&self, f: fn(Row) -> A) -> std::io::Result<Vec<A>> {
        let mut items = Vec::new();
        for id in self.device.get_indices()? {
            match self.read_then(id, f)? {
                None => {}
                Some(item) => items.push(item)
            }
        }
        Ok(items)
    }

    /// overwrites a specified row by ID
    pub fn overwrite(&mut self, row: Row) -> std::io::Result<usize> {
        match self.device.overwrite_row(row.get_id(), row) {
            TypedValue::Outcome(oc) => Ok(oc.to_update_count()),
            TypedValue::ErrorValue(err) => fail(err.to_string()),
            _ => Ok(0)
        }
    }

    /// overwrites rows that match the supplied criteria
    pub fn overwrite_where(
        &mut self,
        machine: &Machine,
        fields: &Vec<Expression>,
        values: &Vec<Expression>,
        condition: &Option<Conditions>,
        limit: TypedValue,
    ) -> std::io::Result<usize> {
        let mut overwritten = 0;
        for id in self.device.get_indices_with_limit(limit)? {
            // read an active row
            if let Some(row) = self.device.read_one(id)? {
                // if the predicate matches the condition, overwrite the row.
                if row.matches(machine, condition, self.get_columns()) {
                    let (machine, my_fields) =
                        machine.with_row(self.get_columns(), &row).evaluate_as_atoms(fields)?;
                    if let (_, TypedValue::ArrayValue(my_values)) = machine.evaluate_array(values)? {
                        let new_row = self.transform(&row, &my_fields, my_values.values())?;
                        overwritten += self.overwrite(new_row)?;
                    }
                }
            }
        }
        Ok(overwritten)
    }

    /// reads the specified field value from the specified row ID
    pub fn read_field(&self, id: usize, column_id: usize) -> std::io::Result<TypedValue> {
        Ok(self.device.read_field(id, column_id))
    }

    /// reads all active rows from the table; deleted rows are not read.
    pub fn read_active_rows(&self) -> std::io::Result<Vec<Row>> {
        self.device.read_active_rows()
    }

    /// reads an active row by ID
    pub fn read_one(&self, id: usize) -> std::io::Result<Option<Row>> {
        self.device.read_one(id)
    }

    /// reads a range of rows
    pub fn read_range(&self, index: std::ops::Range<usize>) -> std::io::Result<Vec<Row>> {
        self.device.read_range(index)
    }

    /// reads a row by ID
    pub fn read_row(&self, id: usize) -> std::io::Result<(Row, RowMetadata)> {
        self.device.read_row(id)
    }

    /// reads a row and transforms it returning the [Option] of [A]
    pub fn read_then<A>(&self, id: usize, f: fn(Row) -> A) -> std::io::Result<Option<A>> {
        let (row, metadata) = self.device.read_row(id)?;
        Ok(if metadata.is_allocated { Some(f(row)) } else { None })
    }

    /// reads a row and pushes it into the specified vector if active
    pub fn read_then_push(&self, id: usize, rows: &mut Vec<Row>) -> std::io::Result<()> {
        let (row, metadata) = self.device.read_row(id)?;
        Ok(if metadata.is_allocated { rows.push(row) } else { () })
    }

    /// reads all rows matching the supplied condition
    pub fn read_where(
        &self,
        machine: &Machine,
        condition: &Option<Conditions>,
        limit: TypedValue,
    ) -> std::io::Result<Vec<Row>> {
        let mut out = Vec::new();
        for id in self.device.get_indices_with_limit(limit)? {
            // read an active row
            if let Some(row) = self.device.read_one(id)? {
                // if the predicate matches the condition, include the row.
                if row.matches(machine, condition, self.get_columns()) { out.push(row); }
            }
        }
        Ok(out)
    }

    /// resizes the table
    pub fn resize(&mut self, new_size: usize) -> std::io::Result<usize> {
        self.device.resize(new_size);
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

    /// Creates a new [Row] from the supplied fields and values
    pub fn transform(
        &self,
        src_row: &Row,
        field_names: &Vec<String>,
        field_values: &Vec<TypedValue>,
    ) -> std::io::Result<Row> {
        // field and value vectors must have the same length
        let src_values = src_row.get_values();
        if field_names.len() != field_values.len() {
            return fail(format!("Data mismatch: fields ({}) vs values ({})", field_names.len(), field_values.len()));
        }
        // build a cache (mapping) of field names to values
        let cache = field_names.iter().zip(field_values.iter())
            .fold(HashMap::new(), |mut m, (k, v)| {
                m.insert(k.to_string(), v.to_owned());
                m
            });
        // build the new fields vector
        let fields = self.device.get_columns();
        let new_field_values = fields.iter().zip(src_values.iter())
            .map(|(field, value)| match cache.get(field.get_name()) {
                Some(Null | Undefined) => value.to_owned(),
                Some(tv) => tv.to_owned(),
                None => value.to_owned()
            })
            .collect::<Vec<TypedValue>>();
        // return the transformed row
        Ok(Row::new(src_row.get_id(), new_field_values))
    }

    /// restores a deleted row to an active state
    pub fn undelete(&mut self, id: usize) -> std::io::Result<usize> {
        Ok(self.device.undelete_row(id))
            .map(|v| v.to_usize())
    }

    /// restores rows from the table based on a condition
    pub fn undelete_where(
        &mut self,
        machine: &Machine,
        condition: &Option<Conditions>,
        limit: TypedValue,
    ) -> std::io::Result<usize> {
        let mut restored = 0;
        for id in self.device.get_indices_with_limit(limit)? {
            // read a row with its metadata
            let (row, metadata) = self.device.read_row(id)?;
            // if the row is inactive and the predicate matches the condition, restore the row.
            if !metadata.is_allocated && row.matches(machine, condition, self.get_columns()) {
                if self.device.undelete_row(id).is_ok() {
                    restored += 1
                }
            }
        }
        Ok(restored)
    }

    /// updates a specified row by ID
    pub fn update(&mut self, row: Row) -> std::io::Result<usize> {
        // get the column names
        let column_names = self.device.get_columns().iter()
            .map(|c| c.get_name().to_string())
            .collect::<Vec<String>>();

        // build the new row
        let new_row = match self.device.read_one(row.get_id())? {
            Some(orig_row) => self.transform(&orig_row, &column_names, &row.get_values())?,
            None => self.replace_undefined_with_null(row),
        };

        // update the table
        self.overwrite(new_row)
    }

    /// updates rows that match the supplied criteria
    pub fn update_where(
        &mut self,
        machine: &Machine,
        fields: &Vec<Expression>,
        values: &Vec<Expression>,
        condition: &Option<Conditions>,
        limit: TypedValue,
    ) -> std::io::Result<usize> {
        let mut updated = 0;
        for id in self.device.get_indices_with_limit(limit)? {
            // read an active row
            if let Some(row) = self.device.read_one(id)? {
                // if the predicate matches the condition, update the row.
                if row.matches(machine, condition, self.get_columns()) {
                    let (machine, field_names) =
                        machine.with_row(self.device.get_columns(), &row).evaluate_as_atoms(fields)?;
                    if let (_, TypedValue::ArrayValue(field_values)) = machine.evaluate_array(values)? {
                        let new_row = self.transform(&row, &field_names, field_values.values())?;
                        if self.device.overwrite_row(id, new_row).is_ok() {
                            updated += 1
                        }
                    }
                }
            }
        }
        Ok(updated)
    }

    fn replace_undefined_with_null(&self, row: Row) -> Row {
        let columns = self.get_columns().to_owned();
        Row::new(row.get_id(), columns.iter().zip(row.get_values().iter()).map(|(c, v)| {
            match v {
                Null | Undefined => c.get_default_value().to_owned(),
                v => v.to_owned()
            }
        }).collect())
    }
}

impl AddAssign for DataFrame {
    fn add_assign(&mut self, rhs: Self) {
        fn do_add(lhs: &mut DataFrame, rhs: DataFrame) -> std::io::Result<()> {
            for id in 0..rhs.len()? {
                let (row, metadata) = rhs.device.read_row(id)?;
                if metadata.is_allocated { lhs.append(row)?; }
            }
            Ok(())
        }

        match do_add(self, rhs) {
            Ok(_) => (),
            Err(err) => panic!("{}", err.to_string())
        }
    }
}

impl RowCollection for DataFrame {
    fn get_columns(&self) -> &Vec<Column> {
        self.device.get_columns()
    }

    fn get_record_size(&self) -> usize {
        self.device.get_record_size()
    }

    fn get_rows(&self) -> Vec<Row> {
        self.device.get_rows()
    }

    fn len(&self) -> std::io::Result<usize> {
        self.device.len()
    }

    fn overwrite_field(&mut self, id: usize, column_id: usize, new_value: TypedValue) -> TypedValue {
        self.device.overwrite_field(id, column_id, new_value)
    }

    fn overwrite_field_metadata(&mut self, id: usize, column_id: usize, metadata: FieldMetadata) -> TypedValue {
        self.device.overwrite_field_metadata(id, column_id, metadata)
    }

    fn overwrite_row(&mut self, id: usize, row: Row) -> TypedValue {
        self.device.overwrite_row(id, row)
    }

    fn overwrite_row_metadata(&mut self, id: usize, metadata: RowMetadata) -> TypedValue {
        self.device.overwrite_row_metadata(id, metadata)
    }

    fn read_field(&self, id: usize, column_id: usize) -> TypedValue {
        self.device.read_field(id, column_id)
    }

    fn read_field_metadata(&self, id: usize, column_id: usize) -> std::io::Result<FieldMetadata> {
        self.device.read_field_metadata(id, column_id)
    }

    fn read_row(&self, id: usize) -> std::io::Result<(Row, RowMetadata)> {
        self.device.read_row(id)
    }

    fn read_row_metadata(&self, id: usize) -> std::io::Result<RowMetadata> {
        self.device.read_row_metadata(id)
    }

    fn resize(&mut self, new_size: usize) -> TypedValue {
        self.device.resize(new_size)
    }
}


// Unit tests
#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use rand::{thread_rng, Rng, RngCore};

    use shared_lib::cnv_error;

    use crate::data_types::DataType::*;
    use crate::data_types::StorageTypes;
    use crate::dataframes::DataFrame;
    use crate::expression::Conditions::Equal;
    use crate::expression::Expression::*;
    use crate::machine::Machine;
    use crate::namespaces::Namespace;
    use crate::number_kind::NumberKind::F64Kind;
    use crate::numbers::Numbers::*;
    use crate::rows::Row;
    use crate::table_columns::Column;
    use crate::testdata::*;
    use crate::typed_values::TypedValue;
    use crate::typed_values::TypedValue::*;

    #[test]
    fn test_add_assign() {
        let columns = make_quote_columns();
        // create a dataframe with a single row
        let mut df0 = make_dataframe("dataframes", "add_assign", "stocks0", make_quote_parameters()).unwrap();
        df0.append(make_quote(0, "RACE", "NASD", 123.45)).unwrap();
        // create a second dataframe with a single row
        let mut df1 = make_dataframe("dataframes", "add_assign", "stocks1", make_quote_parameters()).unwrap();
        df1.append(make_quote(0, "BEER", "AMEX", 357.12)).unwrap();
        // concatenate the dataframes
        df0 += df1;
        // re-read the rows
        let rows = df0.read_active_rows().unwrap();
        assert_eq!(rows, vec![
            make_quote(0, "RACE", "NASD", 123.45),
            make_quote(1, "BEER", "AMEX", 357.12),
        ]);
    }

    #[test]
    fn test_create_dataframe() {
        let df = make_dataframe(
            "dataframes", "create", "stocks", make_quote_parameters()).unwrap();
        let columns = df.get_columns();
        assert_eq!(columns[0].get_name(), "symbol");
        assert_eq!(columns[0].get_data_type().to_owned(), StringType(StorageTypes::FixedSize(8)));
        assert_eq!(columns[0].get_default_value(), Null);
        assert_eq!(columns[1].get_name(), "exchange");
        assert_eq!(columns[1].get_data_type().to_owned(), StringType(StorageTypes::FixedSize(8)));
        assert_eq!(columns[1].get_default_value(), Null);
        assert_eq!(columns[2].get_name(), "last_sale");
        assert_eq!(columns[2].get_data_type().to_owned(), NumberType(F64Kind));
        assert_eq!(columns[2].get_default_value(), Null);
    }

    #[test]
    fn test_insert_row() {
        let columns = make_quote_columns();
        // create a dataframe with a single (encoded) row
        let mut df = make_dataframe("dataframes", "append", "stocks", make_quote_parameters()).unwrap();
        assert_eq!(0, df.append(make_quote(0, "RICE", "PIPE", 42.11)).unwrap());
        // create a second row and append it to the dataframe
        assert_eq!(1, df.append(make_quote(0, "BEEF", "CAKE", 100.0)).unwrap());

        // verify the rows
        let rows = df.read_active_rows().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], make_quote(0, "RICE", "PIPE", 42.11));
        assert_eq!(rows[1], make_quote(1, "BEEF", "CAKE", 100.0));
        assert_eq!(df.len().unwrap(), 2);
    }

    #[test]
    fn test_delete_row() {
        // create a dataframe with 3 rows, 3 columns
        let columns = make_quote_parameters();
        let phys_columns = Column::from_parameters(&columns).unwrap();
        let mut df = make_dataframe("dataframes", "delete_row", "stocks", columns).unwrap();
        df.append(make_quote(0, "UNO", "AMEX", 11.77)).unwrap();
        df.append(make_quote(1, "DOS", "AMEX", 33.22)).unwrap();
        df.append(make_quote(2, "TRES", "AMEX", 55.44)).unwrap();

        // delete the middle row
        assert_eq!(df.delete(1).unwrap(), 1);

        // verify the rows
        let rows = df.read_active_rows().unwrap();
        assert_eq!(rows, vec![
            Row::new(0, vec![
                StringValue("UNO".into()), StringValue("AMEX".into()), Number(F64Value(11.77)),
            ]),
            Row::new(2, vec![
                StringValue("TRES".into()), StringValue("AMEX".into()), Number(F64Value(55.44)),
            ]),
        ]);
    }

    #[test]
    fn test_fold_left() {
        // create a dataframe with 3 rows, 3 columns
        let columns = make_quote_parameters();
        let phys_columns = Column::from_parameters(&columns).unwrap();
        let mut df = make_dataframe("dataframes", "fold_left", "stocks", columns).unwrap();
        df.append(make_quote(0, "UNO", "AMEX", 11.77)).unwrap();
        df.append(make_quote(1, "DOS", "AMEX", 33.22)).unwrap();
        df.append(make_quote(2, "TRES", "AMEX", 55.44)).unwrap();
        assert_eq!(df.fold_left(0, |total, row| total + row.get_id()).unwrap(), 3)
    }

    #[test]
    fn test_fold_right() {
        // create a dataframe with 3 rows, 3 columns
        let columns = make_quote_parameters();
        let phys_columns = Column::from_parameters(&columns).unwrap();
        let mut df = make_dataframe("dataframes", "fold_right", "stocks", columns).unwrap();
        df.append(make_quote(0, "ONE", "AMEX", 11.77)).unwrap();
        df.append(make_quote(1, "TWO", "AMEX", 33.22)).unwrap();
        df.append(make_quote(2, "THRE", "AMEX", 55.44)).unwrap();
        assert_eq!(df.fold_right(0, |total, row| total + row.get_id()).unwrap(), 3)
    }

    #[test]
    fn test_for_all() {
        // create a dataframe with 3 rows, 3 columns
        let columns = make_quote_parameters();
        let phys_columns = Column::from_parameters(&columns).unwrap();
        let mut df = make_dataframe("dataframes", "for_all", "stocks", columns).unwrap();
        df.append(make_quote(0, "ALPH", "AMEX", 11.77)).unwrap();
        df.append(make_quote(1, "BETA", "NYSE", 33.22)).unwrap();
        df.append(make_quote(2, "GMMA", "NASD", 55.44)).unwrap();
        assert_eq!(df.for_all(|row| row.get_id() < 5).unwrap(), true)
    }

    #[test]
    fn test_foreach_row() {
        let mut df = make_dataframe(
            "dataframes", "foreach_row", "stocks", make_quote_parameters()).unwrap();
        assert_eq!(df.append(make_quote(0, "WE", "NYSE", 123.45)).unwrap(), 0);
        assert_eq!(df.append(make_quote(0, "CAN", "NYSE", 88.22)).unwrap(), 1);
        assert_eq!(df.append(make_quote(0, "FLY", "AMEX", 51.11)).unwrap(), 2);
        assert_eq!(df.append(make_quote(0, "FAR", "NYSE", 42.33)).unwrap(), 3);
        assert_eq!(df.append(make_quote(0, "AWAY", "AMEX", 9.73)).unwrap(), 4);
        df.foreach(|row| println!("{:?}", row)).unwrap()
    }

    #[test]
    fn test_load() {
        let ns = Namespace::parse("dataframes.load.stocks").unwrap();
        let mut df = make_dataframe(&ns.database, &ns.schema, &ns.name, make_quote_parameters()).unwrap();
        let columns = make_quote_columns();
        df.append(make_quote(0, "SPAM", "NYSE", 11.99)).unwrap();

        let df0 = DataFrame::load(&ns).unwrap();
        let (row, metadata) = df0.read_row(0).unwrap();
        assert!(metadata.is_allocated);
        assert_eq!(row, make_quote(0, "SPAM", "NYSE", 11.99));
    }

    #[test]
    fn test_map() {
        let mut df = make_dataframe(
            "dataframes", "transform", "stocks", make_quote_parameters()).unwrap();
        assert_eq!(df.append(make_quote(0, "WE", "NYSE", 123.45)).unwrap(), 0);
        assert_eq!(df.append(make_quote(0, "CAN", "NYSE", 88.22)).unwrap(), 1);
        assert_eq!(df.append(make_quote(0, "FLY", "AMEX", 51.11)).unwrap(), 2);
        assert_eq!(df.append(make_quote(0, "FAR", "NYSE", 42.33)).unwrap(), 3);
        assert_eq!(df.append(make_quote(0, "AWAY", "AMEX", 9.73)).unwrap(), 4);
        assert_eq!(df.map(|row| row.get(2)).unwrap(), vec![
            Number(F64Value(123.45)), Number(F64Value(88.22)),
            Number(F64Value(51.11)), Number(F64Value(42.33)),
            Number(F64Value(9.73)),
        ])
    }

    #[test]
    fn test_overwrite_row() {
        let mut df = make_dataframe(
            "dataframes", "overwrite_row", "stocks", make_quote_parameters()).unwrap();
        let row = make_quote(2, "AMD", "NYSE", 123.45);
        assert_eq!(df.overwrite(row).unwrap(), 1);
    }

    #[test]
    fn test_overwrite_where() {
        // create a table with sample data
        let mut df = make_dataframe(
            "dataframes", "overwrite_where", "stocks", make_quote_parameters()).unwrap();
        assert_eq!(df.append(make_quote(0, "WE", "NYSE", 123.45)).unwrap(), 0);
        assert_eq!(df.append(make_quote(0, "CAN", "NYSE", 88.22)).unwrap(), 1);
        assert_eq!(df.append(make_quote(0, "FLY", "AMEX", 51.11)).unwrap(), 2);
        assert_eq!(df.append(make_quote(0, "FAR", "NYSE", 42.33)).unwrap(), 3);
        assert_eq!(df.append(make_quote(0, "AWAY", "AMEX", 123.45)).unwrap(), 4);

        // updates rows where ...
        let machine = Machine::empty();
        let fields = vec![
            Variable("symbol".into()), Variable("exchange".into()), Variable("last_sale".into()),
        ];
        let values = vec![
            Literal(StringValue("XXX".into())), Literal(StringValue("YYY".into())), Literal(Number(F64Value(0.))),
        ];
        let condition = Some(Equal(
            Box::new(Variable("exchange".into())),
            Box::new(Literal(StringValue("NYSE".into()))),
        ));
        assert_eq!(df.overwrite_where(&machine, &fields, &values, &condition, Number(I64Value(2))).unwrap(), 2);

        // verify the rows
        assert_eq!(df.read_active_rows().unwrap(), vec![
            make_quote(0, "XXX", "YYY", 0.),
            make_quote(1, "XXX", "YYY", 0.),
            make_quote(2, "FLY", "AMEX", 51.11),
            make_quote(3, "FAR", "NYSE", 42.33),
            make_quote(4, "AWAY", "AMEX", 123.45),
        ])
    }

    #[test]
    fn test_read_one() {
        let mut df = make_dataframe(
            "dataframes", "read_one", "stocks", make_quote_parameters()).unwrap();
        let row = make_quote(0, "AMD", "NYSE", 123.45);
        assert_eq!(df.overwrite(row).unwrap(), 1);
        assert_eq!(df.read_one(0).unwrap(), Some(make_quote(0, "AMD", "NYSE", 123.45)))
    }

    #[test]
    fn test_read_range() {
        let mut df = make_dataframe(
            "dataframes", "read_range", "stocks", make_quote_parameters()).unwrap();
        assert_eq!(df.append(make_quote(0, "WE", "NYSE", 123.45)).unwrap(), 0);
        assert_eq!(df.append(make_quote(0, "CAN", "NYSE", 88.22)).unwrap(), 1);
        assert_eq!(df.append(make_quote(0, "FLY", "AMEX", 51.11)).unwrap(), 2);
        assert_eq!(df.append(make_quote(0, "FAR", "NYSE", 42.33)).unwrap(), 3);
        assert_eq!(df.append(make_quote(0, "AWAY", "AMEX", 123.45)).unwrap(), 4);
        assert_eq!(df.read_range(1..4).unwrap(), vec![
            make_quote(1, "CAN", "NYSE", 88.22),
            make_quote(2, "FLY", "AMEX", 51.11),
            make_quote(3, "FAR", "NYSE", 42.33),
        ])
    }

    #[test]
    fn test_read_row() {
        let mut df = make_dataframe(
            "dataframes", "read_row", "stocks", make_quote_parameters()).unwrap();
        let row = make_quote(0, "GE", "NASDAQ", 43.45);
        assert_eq!(df.overwrite(row).unwrap(), 1);

        // read and verify
        let (row, meta) = df.read_row(0).unwrap();
        assert!(meta.is_allocated);
        assert_eq!(row, make_quote(0, "GE", "NASDAQ", 43.45))
    }

    #[test]
    fn test_read_field() {
        let mut df = make_dataframe(
            "dataframes", "read_field", "stocks", make_quote_parameters()).unwrap();
        assert_eq!(0, df.append(make_quote(0, "DUCK", "QUACK", 78.35)).unwrap());

        let value: TypedValue = df.read_field(0, 0).unwrap();
        assert_eq!(value, StringValue("DUCK".into()));
    }

    #[test]
    fn test_resize_table() {
        let mut df = make_dataframe(
            "dataframes", "resize", "stocks", make_quote_parameters()).unwrap();
        let _ = df.resize(0).unwrap();
        df.resize(5).unwrap();
        assert_eq!(df.len().unwrap(), 5);
    }

    #[test]
    fn test_reverse() {
        // create a dataframe with 3 rows, 3 columns
        let parameters = make_quote_parameters();
        let phys_columns = Column::from_parameters(&parameters).unwrap();
        let mut df = make_dataframe("dataframes", "reverse", "stocks", parameters).unwrap();
        let row_a = make_quote(0, "A", "AMEX", 11.77);
        let row_b = make_quote(1, "BB", "AMEX", 33.22);
        let row_c = make_quote(2, "CCC", "AMEX", 55.44);
        for row in vec![&row_a, &row_b, &row_c] { df.append(row.to_owned()).unwrap(); }
        assert_eq!(df.reverse().unwrap(), [row_c, row_b, row_a]);
    }

    #[test]
    fn test_undelete_row() {
        // create a dataframe with 3 rows, 3 columns
        let columns = make_quote_parameters();
        let phys_columns = Column::from_parameters(&columns).unwrap();
        let mut df = make_dataframe("dataframes", "undelete_row", "stocks", columns).unwrap();
        assert_eq!(0, df.append(make_quote(0, "UNO", "AMEX", 11.77)).unwrap());
        assert_eq!(1, df.append(make_quote(1, "DOS", "AMEX", 33.22)).unwrap());
        assert_eq!(2, df.append(make_quote(2, "TRES", "AMEX", 55.44)).unwrap());

        // delete the middle row
        assert_eq!(df.delete(1).unwrap(), 1);

        // define the verification rows
        let row_0 = Row::new(0, vec![
            StringValue("UNO".into()), StringValue("AMEX".into()), Number(F64Value(11.77)),
        ]);
        let row_1 = Row::new(1, vec![
            StringValue("DOS".into()), StringValue("AMEX".into()), Number(F64Value(33.22)),
        ]);
        let row_2 = Row::new(2, vec![
            StringValue("TRES".into()), StringValue("AMEX".into()), Number(F64Value(55.44)),
        ]);

        // verify the row was deleted
        let rows = df.read_active_rows().unwrap();
        assert_eq!(rows, vec![row_0.to_owned(), row_2.to_owned()]);

        // restore the middle row
        assert_eq!(df.undelete(1).unwrap(), 1);

        // verify the row was restored
        let rows = df.read_active_rows().unwrap();
        assert_eq!(rows, vec![row_0.to_owned(), row_1.to_owned(), row_2.to_owned()]);
    }

    #[test]
    fn test_update_row() {
        // create a dataframe with 3 rows, 3 columns
        let columns = make_quote_parameters();
        let phys_columns = Column::from_parameters(&columns).unwrap();
        let mut df = make_dataframe("dataframes", "update_row", "stocks", columns).unwrap();
        assert_eq!(0, df.append(make_quote(0, "DIAS", "NYSE", 99.99)).unwrap());
        assert_eq!(1, df.append(make_quote(1, "DORA", "AMEX", 33.32)).unwrap());
        assert_eq!(2, df.append(make_quote(2, "INFO", "NASD", 22.00)).unwrap());

        // update the middle row
        let row_to_update = Row::new(1, vec![
            Undefined, Undefined, Number(F64Value(33.33)),
        ]);
        assert_eq!(df.update(row_to_update.to_owned()).unwrap(), 1);

        // verify the row was updated
        let (updated_row, updated_rmd) = df.read_row(row_to_update.get_id()).unwrap();
        assert!(updated_rmd.is_allocated);
        assert_eq!(updated_row, Row::new(1, vec![
            StringValue("DORA".into()), StringValue("AMEX".into()), Number(F64Value(33.33)),
        ]))
    }

    #[test]
    fn test_update_where() {
        // create a table with sample data
        let mut df = make_dataframe(
            "dataframes", "update_where", "stocks", make_quote_parameters()).unwrap();
        assert_eq!(df.append(make_quote(0, "WE", "NYSE", 123.45)).unwrap(), 0);
        assert_eq!(df.append(make_quote(1, "CAN", "NYSE", 88.22)).unwrap(), 1);
        assert_eq!(df.append(make_quote(2, "FLY", "AMEX", 51.11)).unwrap(), 2);
        assert_eq!(df.append(make_quote(3, "FAR", "NYSE", 42.33)).unwrap(), 3);
        assert_eq!(df.append(make_quote(4, "AWAY", "AMEX", 123.45)).unwrap(), 4);

        // updates rows where ...
        let machine = Machine::empty();
        let fields = vec![Variable("last_sale".into())];
        let values = vec![Literal(Number(F64Value(11.1111)))];
        let condition = Some(Equal(
            Box::new(Variable("exchange".into())),
            Box::new(Literal(StringValue("NYSE".into()))),
        ));
        assert_eq!(df.update_where(&machine, &fields, &values, &condition, Number(I64Value(2))).unwrap(), 2);

        // verify the rows,
        let rows = df.read_active_rows().unwrap();
        assert_eq!(rows, vec![
            make_quote(0, "WE", "NYSE", 11.1111),
            make_quote(1, "CAN", "NYSE", 11.1111),
            make_quote(2, "FLY", "AMEX", 51.11),
            make_quote(3, "FAR", "NYSE", 42.33),
            make_quote(4, "AWAY", "AMEX", 123.45),
        ])
    }

    #[test]
    fn test_performance() -> std::io::Result<()> {
        let columns = make_quote_parameters();
        let phys_columns = Column::from_parameters(&columns)?;
        let mut df = make_dataframe("dataframes", "performance_test", "stocks", columns)?;
        test_write_performance(&mut df, &phys_columns, 10_000)?;
        test_read_performance(&df)?;
        Ok(())
    }

    fn test_write_performance(df: &mut DataFrame, columns: &Vec<Column>, total: usize) -> std::io::Result<()> {
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
            let row = make_quote(0, &symbol, exchange, last_sale);
            df.append(row)?;
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