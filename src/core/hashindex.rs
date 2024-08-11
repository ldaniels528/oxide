////////////////////////////////////////////////////////////////////
// Hashing Row Collection
////////////////////////////////////////////////////////////////////

use std::ops::{Deref, DerefMut, Range};

use crate::file_row_collection::FileRowCollection;
use crate::namespaces::Namespace;
use crate::row_collection::RowCollection;
use crate::row_metadata::RowMetadata;
use crate::rows::Row;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{RowsAffected, StringValue, UInt64Value, Undefined};

/// Hash-Indexed-based RowCollection implementation
#[derive(Debug)]
pub struct HashingRowCollection {
    key_column_index: usize,
    buckets: u64,
    data_table: Box<dyn RowCollection>,
    keys_table: Box<dyn RowCollection>,
}

impl HashingRowCollection {
    //////////////////////////////////////////////////////////
    //  STATIC METHODS
    //////////////////////////////////////////////////////////

    /// Returns a hash-index ready to be queried
    pub fn create(
        key_column_index: usize,
        buckets: u64,
        data_table: Box<dyn RowCollection>,
    ) -> std::io::Result<HashingRowCollection> {
        let hash_key_table = data_table.create_hash_table(key_column_index)?;
        Ok(Self::new(key_column_index, buckets, data_table, hash_key_table))
    }

    /// Translates a key into its hash-key row offset
    pub fn from_key_to_row_id(key: &TypedValue, buckets: u64) -> usize {
        (key.hash_code() % buckets) as usize
    }

    /// Returns a hash-table ready to be queried
    pub fn new(
        key_column_index: usize,
        buckets: u64,
        data_table: Box<dyn RowCollection>,
        keys_table: Box<dyn RowCollection>,
    ) -> HashingRowCollection {
        Self {
            key_column_index,
            buckets,
            data_table,
            keys_table,
        }
    }

    //////////////////////////////////////////////////////////
    //  INSTANCE METHODS
    //////////////////////////////////////////////////////////

    /// Audits the hash table; returning any invalid keys
    pub fn audit(&mut self) -> std::io::Result<Vec<TypedValue>> {
        let (mut row_id, mut collision_keys) = (0, vec![]);
        let eof = self.len()?;
        while row_id < eof {
            // attempt to read the data row ...
            if let Some(row) = self.data_table.read_one(row_id)? {
                let key = &row[self.key_column_index];
                let hash_key_row_id = self.convert_key_to_row_id(key);
                // attempt to read the hash key row ...
                if let Some(hash_key_row) = self.keys_table.read_one(hash_key_row_id)? {
                    let ref_key_value = &hash_key_row[1];
                    if ref_key_value != key {
                        collision_keys.push(key.clone());
                    }
                }
            }
            row_id += 1;
        }
        Ok(collision_keys)
    }

    /// Translates a key into its hash-key row offset
    pub fn convert_key_to_row_id(&self, key: &TypedValue) -> usize {
        Self::from_key_to_row_id(key, self.buckets)
    }

    /// Performs a fast lookup (O(1)) via hash index on the key column
    pub fn find_row_by_key(
        &self,
        key: &TypedValue,
    ) -> std::io::Result<Option<Row>> {
        let hash_key_row_id = self.convert_key_to_row_id(key);
        if let Some(hash_key_row) = self.keys_table.read_one(hash_key_row_id)? {
            if let Some(data_row_id) = hash_key_row[0].assume_usize() {
                return self.data_table.read_one(data_row_id);
            }
        }
        Ok(None)
    }

    pub fn move_hash_key(
        &mut self,
        data_row_id: usize,
        prev_value: &TypedValue,
        new_value: TypedValue,
    ) -> std::io::Result<TypedValue> {
        let hash_key_columns = self.keys_table.get_columns();
        let hash_key_row_prev_id = self.convert_key_to_row_id(&prev_value);
        let hash_key_row_new_id = self.convert_key_to_row_id(&new_value);
        let hash_key_row = Row::new(hash_key_row_new_id, hash_key_columns.clone(), vec![
            UInt64Value(data_row_id as u64), new_value,
        ]);
        let a = self.keys_table.overwrite_row(hash_key_row_new_id, hash_key_row)?;
        let b = self.keys_table.delete_row(hash_key_row_prev_id)?;
        Ok(a + b)
    }

    /// (Re)builds the hash index
    pub fn rebuild(&mut self) -> std::io::Result<TypedValue> {
        let hash_key_columns = self.keys_table.get_columns();
        let (mut inserted_rows, mut row_id) = (0, 0);
        let eof = self.data_table.len()?;
        let mut hash_key_table = self.create_hash_table(self.key_column_index)?;
        while row_id < eof {
            // attempt to read a row ...
            if let Some(row) = self.data_table.read_one(row_id)? {
                // translate key into the hash-key row ID, then lookup the data row ID
                let key = &row[self.key_column_index];
                let hash_key_row_id = self.convert_key_to_row_id(key);
                let hash_key_row = Row::new(hash_key_row_id, hash_key_columns.clone(), vec![
                    UInt64Value(row_id as u64), key.clone(),
                ]);
                // attempt to overwrite the row
                if let RowsAffected(n) = hash_key_table.overwrite_row(hash_key_row_id, hash_key_row)? {
                    inserted_rows += n;
                }
            }
            row_id += 1;
        }
        self.keys_table = hash_key_table;
        Ok(RowsAffected(inserted_rows))
    }
}

impl RowCollection for HashingRowCollection {
    fn find_row(
        &self,
        search_column_index: usize,
        search_column_value: &TypedValue,
    ) -> std::io::Result<Option<Row>> {
        if search_column_index == self.key_column_index {
            self.find_row_by_key(search_column_value)
        } else {
            self.find_next(search_column_index, search_column_value, 0)
        }
    }

    fn get_columns(&self) -> &Vec<TableColumn> {
        self.data_table.get_columns()
    }

    fn get_record_size(&self) -> usize {
        self.data_table.get_record_size()
    }

    fn len(&self) -> std::io::Result<usize> {
        self.data_table.len()
    }

    fn overwrite_field(
        &mut self,
        id: usize,
        column_id: usize,
        new_value: TypedValue
    ) -> std::io::Result<TypedValue> {
        let prev_value = self.data_table.read_one(id)?
            .map(|row| row[self.key_column_index].clone())
            .unwrap_or(Undefined);

        // update the data and index tables
        let _ = self.move_hash_key(id, &prev_value, new_value.clone())?;
        self.data_table.overwrite_field(id, column_id, new_value)
    }

    fn overwrite_row(&mut self, id: usize, row: Row) -> std::io::Result<TypedValue> {
        let prev_value = self.data_table.read_one(id)?
            .map(|row| row[self.key_column_index].clone())
            .unwrap_or(Undefined);
        let new_value = row[self.key_column_index].clone();

        // update the data and index tables
        let _ = self.move_hash_key(id, &prev_value, new_value)?;
        self.data_table.overwrite_row(id, row)
    }

    fn overwrite_row_metadata(&mut self, id: usize, metadata: RowMetadata) -> std::io::Result<TypedValue> {
        self.data_table.overwrite_row_metadata(id, metadata)
    }

    fn read_field(&self, id: usize, column_id: usize) -> std::io::Result<TypedValue> {
        self.data_table.read_field(id, column_id)
    }

    fn read_range(&self, index: Range<usize>) -> std::io::Result<Box<dyn RowCollection>> {
        self.data_table.read_range(index)
    }

    fn read_row(&self, id: usize) -> std::io::Result<(Row, RowMetadata)> {
        self.data_table.read_row(id)
    }

    fn read_row_metadata(&self, id: usize) -> std::io::Result<RowMetadata> {
        self.data_table.read_row_metadata(id)
    }

    fn resize(&mut self, new_size: usize) -> std::io::Result<TypedValue> {
        self.data_table.resize(new_size)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use crate::file_row_collection::FileRowCollection;
    use crate::namespaces::Namespace;
    use crate::row_collection::RowCollection;
    use crate::rows::Row;
    use crate::testdata::{make_quote, make_table_columns, StockQuote};
    use crate::typed_values::TypedValue::{Ack, Float64Value, RowsAffected, StringValue};

    use super::*;

    #[test]
    fn test_append_then_query_hash_table() {
        // create a table and write some rows to it
        let ns = Namespace::new("hash_table", "create", "stocks");
        let columns = make_table_columns();
        let (symbol, exchange, last_sale) =
            (StringValue("ABC".into()), StringValue("NASDAQ".into()), Float64Value(5.04));

        // append-then-query the hash index
        let mut hrc = build_hash_table_with_samples(&ns, 0, 1000);
        let result = hrc.find_row_by_key(&symbol).unwrap();

        // verify the result
        // |-------------------------------|
        // | symbol | exchange | last_sale |
        // |-------------------------------|
        // | ABC    | NASDAQ   | 5.04      |
        // |-------------------------------|
        assert_eq!(result, Some(Row::new(7, columns.clone(), vec![
            symbol, exchange, last_sale,
        ])));
    }

    #[test]
    fn test_append_modify_then_query_hash_table() {
        // create a table and write some rows to it
        let ns = Namespace::new("hash_table", "create", "stocks");
        let columns = make_table_columns();
        let (symbol, exchange, last_sale) =
            (StringValue("CRT".into()), StringValue("OTC_BB".into()), Float64Value(1.2582));

        // append rows then query the hash index
        let mut hrc = build_hash_table_with_samples(&ns, 0, 1000);
        assert_eq!(
            hrc.find_row_by_key(&symbol).unwrap(),
            Some(Row::new(2, columns.clone(), vec![
                symbol, exchange, last_sale,
            ]))
        );

        // overwrite 'CRT' with 'CRT.Q' in the hash index
        assert_eq!(
            RowsAffected(1),
            hrc.overwrite_row(2, Row::new(2, columns.clone(), vec![
                StringValue("CRT.Q".into()), StringValue("OTC_BB".into()), Float64Value(1.2598),
            ])).unwrap());

        // verify the modified record (data table)
        // |-------------------------------|
        // | symbol | exchange | last_sale |
        // |-------------------------------|
        // | CRT.Q  | OTC_BB   | 1.2598    |
        // |-------------------------------|
        assert_eq!(
            hrc.find_row_by_key(&StringValue("CRT.Q".into())).unwrap(),
            Some(Row::new(2, columns.clone(), vec![
                StringValue("CRT.Q".into()), StringValue("OTC_BB".into()), Float64Value(1.2598),
            ])));

        // verify the deleted record (hash index table)
        assert_eq!(hrc.find_row_by_key(&StringValue("CRT".into())).unwrap(), None);
    }

    #[test]
    fn test_performance() {
        let ns = Namespace::new("hash_table", "performance", "stocks");
        let columns = make_table_columns();
        let max_count = 10_000;
        let mut stocks = build_hash_table(&ns, 0, 2000 * max_count);
        assert_eq!(Ack, stocks.resize(0).unwrap());

        for n in 0..max_count {
            let q = StockQuote::generate_quote();
            let id = n as usize;
            assert_eq!(RowsAffected(1), stocks.overwrite_row(id, Row::new(id, columns.clone(), vec![
                StringValue(q.symbol.clone()),
                StringValue(q.exchange.clone()),
                Float64Value(q.last_sale),
            ])).unwrap());
        }

        let rand_row_id = (max_count - 1) as usize;
        let column_id = stocks.key_column_index;
        let (symbol, msec) = measure_time(|| stocks.read_field(rand_row_id, column_id).unwrap());
        println!("[{:.4} msec] read_field({}, {}) -> {}", msec, rand_row_id, column_id, symbol.unwrap_value());

        let (row_b, msec_b) = measure_time(|| stocks.find_row_by_key(&symbol).unwrap());
        println!("[{:.4} msec] find_row_by_key({}) -> {:?}", msec_b, symbol.unwrap_value(), row_b);

        let (row_b, msec_b) = measure_time(|| stocks.find_row_by_key(&symbol).unwrap());
        println!("[{:.4} msec] find_row_by_key({}) -> {:?}", msec_b, symbol.unwrap_value(), row_b);

        let (row_a, msec_a) = measure_time(|| stocks.data_table.find_row(column_id, &symbol).unwrap());
        println!("[{:.4} msec] find_row({}, {}) -> {:?}", msec_a, column_id, symbol.unwrap_value(), row_a);

        // perform an audit of the hash
        let (collisions, msec) = measure_time(|| stocks.audit().unwrap());
        println!("[{:.4} msec] audit_collisions ({}) -> {:?}", msec, collisions.len(), collisions);

        // verification
        assert!(row_a.is_some() && row_b.is_some());
        assert_eq!(row_a, row_b);
        assert!(msec_b <= msec_a);
        assert!(collisions.len() < (max_count as f64 * 0.05) as usize);
    }

    fn measure_time<F, R>(process: F) -> (R, f64)
        where
            F: FnOnce() -> R,
    {
        // Record the start time
        let start = Instant::now();

        // Run the process
        let result = process();

        // Record the end time
        let end = Instant::now();

        // Calculate the elapsed time in nanoseconds
        let duration_ns = end.duration_since(start).as_nanos();

        // Convert to milliseconds (1 millisecond = 1,000,000 nanoseconds)
        (result, duration_ns as f64 / 1_000_000.0)
    }

    fn build_hash_table(ns: &Namespace, column_index: usize, buckets: u64) -> HashingRowCollection {
        // create a table and write some rows to it
        let columns = make_table_columns();
        let frc = FileRowCollection::create_table(&ns, columns.clone()).unwrap();
        let hrc = HashingRowCollection::create(column_index, buckets, Box::new(frc)).unwrap();
        //assert_eq!(RowsAffected(9), hrc.rebuild().unwrap());
        hrc
    }

    fn build_hash_table_with_samples(ns: &Namespace, column_index: usize, buckets: u64) -> HashingRowCollection {
        // create a table and write some rows to it
        let columns = make_table_columns();
        let mut frc = FileRowCollection::create_table(&ns, columns.clone()).unwrap();
        let quote_data = vec![
            ("ACT", "AMEX", 0.0021), ("ATT", "NYSE", 98.44), ("CRT", "OTC_BB", 1.2582),
            ("GE", "NYSE", 21.22), ("H", "OTC_BB", 0.0076), ("T", "NYSE", 43.167),
            ("X", "NASDAQ", 33.33), ("ABC", "NASDAQ", 5.04), ("PSY", "AMEX", 95.56),
        ];
        for (symbol, exchange, last_sale) in quote_data {
            assert_eq!(RowsAffected(1), frc.append_row(make_quote(0, &columns, symbol, exchange, last_sale)).unwrap());
        }
        // show the contents of the table
        // |-------------------------------|
        // | symbol | exchange | last_sale |
        // |-------------------------------|
        // | ACT    | AMEX     | 0.0021    |
        // | ATT    | NYSE     | 98.44     |
        // | CRT    | OTC_BB   | 1.2582    |
        // | GE     | NYSE     | 21.22     |
        // | H      | OTC_BB   | 0.0076    |
        // | T      | NYSE     | 43.167    |
        // | X      | NASDAQ   | 33.33     |
        // | ABC    | NASDAQ   | 5.04      |
        // | PSY    | AMEX     | 95.56     |
        // |-------------------------------|
        //for s in TableRenderer::from_rows(table.read_active_rows().unwrap()) { println!("{}", s) }

        // create and query the hash index
        let mut hrc = HashingRowCollection::create(column_index, buckets, Box::new(frc)).unwrap();
        assert_eq!(RowsAffected(9), hrc.rebuild().unwrap());

        // show the contents of the hash table
        // |---------------------|
        // | __row_id__ | symbol |
        // |---------------------|
        // | 3          | GE     |
        // | 2          | CRT    |
        // | 4          | H      |
        // | 5          | T      |
        // | 8          | PSY    |
        // | 1          | ATT    |
        // | 0          | ACT    |
        // | 6          | X      |
        // | 7          | ABC    |
        // |---------------------|
        //for s in TableRenderer::from_rows(hash_table.hash_key_table.read_active_rows().unwrap()) { println!("{}", s) }
        hrc
    }
}