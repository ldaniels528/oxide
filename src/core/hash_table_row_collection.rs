////////////////////////////////////////////////////////////////////
// HashTableRowCollection class
////////////////////////////////////////////////////////////////////

use std::ops::Range;

use log::warn;

use crate::data_types::DataType::NumberType;
use crate::errors::Errors;
use crate::errors::Errors::{Exact, HashTableOverflow, OutcomeExpected, RowsAffectedExpected};
use crate::field_metadata::FieldMetadata;
use crate::number_kind::NumberKind::*;
use crate::numbers::NumberValue::U64Value;
use crate::outcomes::Outcomes;
use crate::parameter::Parameter;
use crate::row_collection::RowCollection;
use crate::row_metadata::RowMetadata;
use crate::rows::Row;
use crate::table_columns::Column;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::*;

/// Hash-Table-based RowCollection implementation
#[derive(Debug)]
pub struct HashTableRowCollection {
    key_column_index: usize,
    bucket_count: u64,
    bucket_depth: u64,
    data_table: Box<dyn RowCollection>,
    keys_table: Box<dyn RowCollection>,
}

impl HashTableRowCollection {
    //////////////////////////////////////////////////////////
    //  STATIC METHODS
    //////////////////////////////////////////////////////////

    /// Generates the columns for the index base on the source column
    fn create_hash_keys_columns(src_column: &Column) -> std::io::Result<Vec<Column>> {
        Column::from_parameters(&vec![
            Parameter::new(
                "__row_id__",
                NumberType(U64Kind).to_type_declaration(),
                Some("null".into())),
            src_column.to_parameter()
        ])
    }

    fn create_hash_keys_row(
        data_row_id: usize,
        keys_row_id: usize,
        _keys_columns: &Vec<Column>,
        new_value: &TypedValue,
    ) -> Row {
        Row::new(keys_row_id, vec![
            Number(U64Value(data_row_id as u64)),
            new_value.to_owned(),
        ])
    }

    /// Returns a hash-index ready to be queried
    pub fn create_with_options(
        key_column_index: usize,
        bucket_count: u64,
        bucket_depth: u64,
        data_table: Box<dyn RowCollection>,
    ) -> std::io::Result<HashTableRowCollection> {
        let src_column = &data_table.get_columns()[key_column_index];
        let keys_columns = Self::create_hash_keys_columns(src_column)?;
        let keys_table = data_table.create_related_structure(keys_columns, key_column_index.to_string().as_str())?;
        Ok(Self::create_with_tables_and_options(key_column_index, bucket_count, bucket_depth, data_table, keys_table))
    }

    /// Returns a hash-table ready to be queried
    pub fn create_with_tables(
        key_column_index: usize,
        data_table: Box<dyn RowCollection>,
        keys_table: Box<dyn RowCollection>,
    ) -> HashTableRowCollection {
        let bucket_count = 100_000;
        let bucket_depth = 1_000;
        Self::create_with_tables_and_options(
            key_column_index,
            bucket_count,
            bucket_depth,
            data_table,
            keys_table,
        )
    }

    /// Returns a hash-table ready to be queried
    pub fn create_with_tables_and_options(
        key_column_index: usize,
        bucket_count: u64,
        bucket_depth: u64,
        data_table: Box<dyn RowCollection>,
        keys_table: Box<dyn RowCollection>,
    ) -> HashTableRowCollection {
        Self {
            key_column_index,
            bucket_count,
            bucket_depth,
            data_table,
            keys_table,
        }
    }

    /// Returns a hash-table ready to be queried
    pub fn new(
        key_column_index: usize,
        data_table: Box<dyn RowCollection>,
    ) -> std::io::Result<HashTableRowCollection> {
        let bucket_count = 100_000;
        let bucket_depth = bucket_count / 10;
        let src_column = &data_table.get_columns()[key_column_index];
        let keys_columns = Self::create_hash_keys_columns(src_column)?;
        let keys_table =
            data_table.create_related_structure(keys_columns, key_column_index.to_string().as_str())?;
        Ok(Self::create_with_tables_and_options(key_column_index, bucket_count, bucket_depth, data_table, keys_table))
    }

    //////////////////////////////////////////////////////////
    //  INSTANCE METHODS
    //////////////////////////////////////////////////////////

    /// Audits the hash table; returning any invalid keys
    pub fn audit(&mut self) -> TypedValue {
        let mut collisions = Vec::new();
        let data_table_row_id_range = match self.data_table.get_indices() {
            Ok(r) => r,
            Err(err) => return ErrorValue(Errors::Exact(err.to_string()))
        };
        for row_id in data_table_row_id_range {
            // attempt to read the data row ...
            if let Ok(Some(row_a)) = self.data_table.read_one(row_id) {
                let key_a = &row_a[self.key_column_index];
                match self.find_row_by_key(key_a) {
                    Ok(Some(row_b)) => {
                        let key_b = &row_b[self.key_column_index];
                        if key_a != key_b {
                            collisions.extend(vec![
                                key_a.to_owned(), key_b.to_owned(),
                            ])
                        }
                    }
                    Ok(None) => collisions.push(key_a.to_owned()),
                    Err(err) => return ErrorValue(Errors::Exact(err.to_string()))
                }
            }
        }
        TypedValue::Array(collisions)
    }

    /// Translates a key into its hash-key row offset
    fn convert_key_to_row_id(&self, key: &TypedValue) -> usize {
        let bucket_depth = 100;
        let start = (key.hash_code() % self.bucket_count) * bucket_depth;
        start as usize
    }

    /// Translates a key into its hash-key row offset
    fn convert_key_to_row_id_range(&self, key: &TypedValue) -> Range<u64> {
        let bucket_depth = 100;
        let start = (key.hash_code() % self.bucket_count) * bucket_depth;
        start..start + bucket_depth
    }

    /// Performs a fast lookup (O(1)) via hash index on the key column
    pub fn find_row_by_key(
        &self,
        key: &TypedValue,
    ) -> std::io::Result<Option<Row>> {
        let keys_row_id_range = self.convert_key_to_row_id_range(key);
        //println!("find_row_by_key:keys_row_id_range {:?}", keys_row_id_range);
        for row_id in keys_row_id_range {
            if let Some(row) = self.keys_table.read_one(row_id as usize)? {
                //println!("find_row_by_key[{}]:row {}", row_id, row.to_string());
                if row[1] == *key {
                    let data_row_id = match row[0].to_owned() {
                        Number(number) => number.to_usize(),
                        _ => 0
                    };
                    return self.read_one(data_row_id);
                }
            }
        }
        Ok(None)
    }

    pub fn get_key_column(&self) -> &Column {
        &self.get_columns()[self.key_column_index]
    }

    fn link_key_value(
        &mut self,
        data_row_id: usize,
        key_value: &TypedValue,
    ) -> TypedValue {
        let keys_columns = self.keys_table.get_columns();
        for keys_row_id in self.convert_key_to_row_id_range(key_value) {
            let my_keys_row_id = keys_row_id as usize;
            match self.keys_table.read_one(keys_row_id as usize) {
                Ok(Some(row)) if &row[0] == key_value =>
                    return self.keys_table.overwrite_row(
                        my_keys_row_id,
                        Self::create_hash_keys_row(data_row_id, my_keys_row_id, keys_columns, key_value)),
                Ok(Some(_)) => {}
                Ok(None) =>
                    return self.keys_table.overwrite_row(
                        my_keys_row_id,
                        Self::create_hash_keys_row(data_row_id, my_keys_row_id, keys_columns, key_value), ),
                Err(err) => return ErrorValue(Errors::Exact(err.to_string()))
            }
        }
        ErrorValue(HashTableOverflow(data_row_id, key_value.unwrap_value()))
    }

    fn move_key_value(
        &mut self,
        data_row_id: usize,
        prev_value: &TypedValue,
        new_value: TypedValue,
    ) -> TypedValue {
        let keys_columns = self.keys_table.get_columns();
        let keys_row_prev_id = self.convert_key_to_row_id(&prev_value);
        let keys_row_new_id = self.convert_key_to_row_id(&new_value);
        let keys_row = Self::create_hash_keys_row(data_row_id, keys_row_new_id, keys_columns, &new_value);
        let a = self.keys_table.overwrite_row(keys_row_new_id, keys_row);
        if matches!(a, ErrorValue(..)) { return a; }
        let b = self.keys_table.delete_row(keys_row_prev_id);
        if matches!(b, ErrorValue(..)) { return b; }
        a + b
    }

    /// (Re)builds the hash key table
    pub fn rebuild(&mut self) -> TypedValue {
        let keys_columns = self.keys_table.get_columns();
        let mut keys_table = match self.create_related_structure(keys_columns.to_owned(), self.key_column_index.to_string().as_str()) {
            Ok(table) => table,
            Err(err) => return ErrorValue(Errors::Exact(err.to_string()))
        };
        if keys_table.resize(0) != Outcome(Outcomes::Ack) {
            warn!("Failed to truncate index for column {}", self.get_key_column().get_name());
        }
        let mut inserted_rows = 0;
        let data_table_len = match self.data_table.len() {
            Ok(len) => len,
            Err(err) => return ErrorValue(Errors::Exact(err.to_string()))
        };
        for data_row_id in 0..data_table_len {
            // attempt to read a row ...
            if let Ok(Some(row)) = self.data_table.read_one(data_row_id) {
                // translate key into the hash-key row ID, then lookup the data row ID
                let key = &row[self.key_column_index];
                let keys_row_id = self.convert_key_to_row_id(key);
                let keys_row = Self::create_hash_keys_row(data_row_id, keys_row_id, keys_columns, key);

                // attempt to overwrite the row
                match keys_table.overwrite_row(keys_row_id, keys_row) {
                    ErrorValue(message) => return ErrorValue(message),
                    Outcome(oc) => inserted_rows += oc.to_update_count(),
                    _ => {}
                }
            }
        }
        self.keys_table = keys_table;
        Outcome(Outcomes::RowsAffected(inserted_rows))
    }
}

impl RowCollection for HashTableRowCollection {
    fn delete_row(&mut self, id: usize) -> TypedValue {
        let key_value = self.read_field(id, self.key_column_index);
        let keys_row_id = self.convert_key_to_row_id(&key_value);
        match key_value {
            ErrorValue(msg) => ErrorValue(msg),
            // no previous key value to delete
            TypedValue::Undefined => self.data_table.delete_row(keys_row_id),
            // delete the hash key value
            _ =>
                match self.keys_table.delete_row(keys_row_id) {
                    Outcome(..) => self.data_table.delete_row(id),
                    ErrorValue(msg) => ErrorValue(msg),
                    other => ErrorValue(OutcomeExpected(other.to_code()))
                }
        }
    }

    fn get_columns(&self) -> &Vec<Column> {
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
        new_value: TypedValue,
    ) -> TypedValue {
        match self.link_key_value(id, &new_value) {
            Outcome(..) => self.data_table.overwrite_field(id, column_id, new_value),
            ErrorValue(msg) => ErrorValue(msg),
            other => ErrorValue(OutcomeExpected(other.to_code()))
        }
    }

    fn overwrite_field_metadata(
        &mut self,
        id: usize,
        column_id: usize,
        metadata: FieldMetadata,
    ) -> TypedValue {
        self.data_table.overwrite_field_metadata(id, column_id, metadata)
    }

    fn overwrite_row(&mut self, id: usize, row: Row) -> TypedValue {
        let new_value = &row[self.key_column_index];
        match self.link_key_value(id, new_value) {
            Outcome(oc) if oc.to_update_count() > 0 => self.data_table.overwrite_row(id, row),
            Outcome(oc) => Outcome(oc),
            ErrorValue(err) => ErrorValue(err),
            other => ErrorValue(OutcomeExpected(other.to_code()))
        }
    }

    fn overwrite_row_metadata(&mut self, id: usize, metadata: RowMetadata) -> TypedValue {
        self.data_table.overwrite_row_metadata(id, metadata)
    }

    fn read_field(&self, id: usize, column_id: usize) -> TypedValue {
        self.data_table.read_field(id, column_id)
    }

    fn read_field_metadata(
        &self,
        id: usize,
        column_id: usize,
    ) -> std::io::Result<FieldMetadata> {
        self.data_table.read_field_metadata(id, column_id)
    }

    fn read_range(&self, index: Range<usize>) -> std::io::Result<Vec<Row>> {
        self.data_table.read_range(index)
    }

    fn read_row(&self, id: usize) -> std::io::Result<(Row, RowMetadata)> {
        self.data_table.read_row(id)
    }

    fn read_row_metadata(&self, id: usize) -> std::io::Result<RowMetadata> {
        self.data_table.read_row_metadata(id)
    }

    fn resize(&mut self, new_size: usize) -> TypedValue {
        self.data_table.resize(new_size)
    }

    fn scan_first(
        &self,
        search_column_index: usize,
        search_column_value: &TypedValue,
    ) -> std::io::Result<Option<Row>> {
        if search_column_index == self.key_column_index {
            self.find_row_by_key(search_column_value)
        } else {
            self.scan_next(search_column_index, search_column_value, 0)
        }
    }

    fn update_row(&mut self, id: usize, row: Row) -> TypedValue {
        let new_value = row[self.key_column_index].to_owned();
        let old_value = self.read_field(id, self.key_column_index);
        match old_value {
            ErrorValue(msg) => ErrorValue(msg),
            Undefined => self.data_table.update_row(id, row),
            _ =>
                match self.move_key_value(id, &old_value, new_value) {
                    Outcome(..) => self.data_table.update_row(id, row),
                    ErrorValue(err) => ErrorValue(err),
                    other => ErrorValue(RowsAffectedExpected(other.to_code()))
                }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use crate::file_row_collection::FileRowCollection;
    use crate::namespaces::Namespace;
    use crate::numbers::NumberValue::F64Value;
    use crate::row_collection::RowCollection;
    use crate::rows::Row;
    use crate::table_renderer::TableRenderer;
    use crate::testdata::{make_quote_columns, StockQuote};

    use super::*;

    #[test]
    fn test_append_then_find_row_by_key() {
        // create a table and write some rows to it
        let ns = Namespace::new("hash_key", "create", "stocks");
        let columns = make_quote_columns();
        let (symbol, exchange, last_sale) =
            (StringValue("ABC".into()), StringValue("NASDAQ".into()), Number(F64Value(5.04)));

        // append-then-query the hash index
        let hkrc = build_hash_key_table_with_samples(&ns, 0, 1000, 100);
        let result = hkrc.find_row_by_key(&symbol).unwrap();

        // verify the result
        // |-------------------------------|
        // | symbol | exchange | last_sale |
        // |-------------------------------|
        // | ABC    | NASDAQ   | 5.04      |
        // |-------------------------------|
        assert_eq!(result, Some(Row::new(7, vec![
            symbol, exchange, last_sale,
        ])));
    }

    #[test]
    fn test_append_modify_then_find_row_by_key() {
        // create a table and write some rows to it
        let ns = Namespace::new("hash_key", "append_modify", "stocks");
        let columns = make_quote_columns();
        let (symbol, exchange, last_sale) =
            (StringValue("CRT".into()), StringValue("OTC_BB".into()), Number(F64Value(1.2582)));

        // append rows then query the hash index
        let mut hkrc = build_hash_key_table_with_samples(&ns, 0, 1000, 100);
        assert_eq!(
            hkrc.find_row_by_key(&symbol).unwrap(),
            Some(Row::new(2, vec![symbol, exchange, last_sale]))
        );

        // overwrite 'CRT' with 'CRT.Q' in the hash index
        assert_eq!(
            Outcome(Outcomes::RowsAffected(1)),
            hkrc.overwrite_row(2, Row::new(2, vec![
                StringValue("CRT.Q".into()), StringValue("OTC_BB".into()), Number(F64Value(1.2598)),
            ])));

        // verify the modified record (data table)
        // |-------------------------------|
        // | symbol | exchange | last_sale |
        // |-------------------------------|
        // | CRT.Q  | OTC_BB   | 1.2598    |
        // |-------------------------------|
        assert_eq!(
            hkrc.find_row_by_key(&StringValue("CRT.Q".into())).unwrap(),
            Some(Row::new(2, vec![
                StringValue("CRT.Q".into()),
                StringValue("OTC_BB".into()),
                Number(F64Value(1.2598)),
            ])));

        // verify the deleted record (hash index table)
        //assert_eq!(hkrc.find_row_by_key(&StringValue("CRT".into())).unwrap(), None);
    }

    #[test]
    fn test_performance() {
        let ns = Namespace::new("hash_key", "performance", "stocks");
        let max_count = 25_000;
        let bucket_count = max_count / 10;
        let bucket_depth = bucket_count / 10;
        performance_test(ns, max_count, bucket_count, bucket_depth)
    }

    fn performance_test(
        ns: Namespace,
        max_count: u64,
        bucket_count: u64,
        bucket_depth: u64,
    ) {
        let columns = make_quote_columns();
        let bucket_count = max_count / 10;
        let bucket_depth = bucket_count / 10;
        let mut stocks = build_hash_key_table(&ns, 0, bucket_count, bucket_depth);
        assert_eq!(Outcome(Outcomes::Ack), stocks.resize(0));

        let mut timings = Vec::new();
        for n in 0..max_count {
            let q = StockQuote::generate_quote();
            let id = n as usize;
            let (outcome, msec) = measure_time(
                || stocks.overwrite_row(id, Row::new(id, vec![
                    StringValue(q.symbol.to_owned()),
                    StringValue(q.exchange.to_owned()),
                    Number(F64Value(q.last_sale)),
                ])));
            timings.push(msec);
            assert_eq!(Outcome(Outcomes::RowsAffected(1)), outcome);
        }

        let (msec_min, msec_max, msec_total) = timings.iter()
            .fold((timings[0], 0f64, 0f64), |(msec_min, msec_max, msec_total), msec| {
                (msec_min.min(*msec), msec_max.max(*msec), msec_total + *msec)
            });
        println!("Insert-into-index timings (msec) - avg: {:.4}, min: {:.4}, max: {:.4}",
                 msec_total / timings.len() as f64, msec_min, msec_max);

        let rand_row_id = (max_count - 1) as usize;
        let column_id = stocks.key_column_index;
        let (symbol, msec) = measure_time(|| stocks.read_field(rand_row_id, column_id));
        println!("[{:.4} msec] read_field({}, {}) -> {}", msec, rand_row_id, column_id, symbol.unwrap_value());

        let (row_b, msec_b) = measure_time(|| stocks.find_row_by_key(&symbol).unwrap());
        println!("[{:.4} msec] find_row_by_key({}) -> {}", msec_b, symbol.unwrap_value(), row_b.clone()
            .map(|r| r.to_string()).unwrap_or(String::new()));

        let (row_b, msec_b) = measure_time(|| stocks.find_row_by_key(&symbol).unwrap());
        println!("[{:.4} msec] find_row_by_key({}) -> {}", msec_b, symbol.unwrap_value(), row_b.clone()
            .map(|r| r.to_string()).unwrap_or(String::new()));

        let (row_b, msec_b) = measure_time(|| stocks.find_row_by_key(&symbol).unwrap());
        println!("[{:.4} msec] find_row_by_key({}) -> {}", msec_b, symbol.unwrap_value(), row_b.clone()
            .map(|r| r.to_string()).unwrap_or(String::new()));

        let (row_a, msec_a) = measure_time(|| stocks.data_table.scan_first(column_id, &symbol).unwrap());
        println!("[{:.4} msec] find_row({}, {}) -> {}", msec_a, column_id, symbol.unwrap_value(), row_a.clone()
            .map(|r| r.to_string()).unwrap_or(String::new()));

        // Insert-into-index timings (msec) - avg: 0.0225, min: 0.0124, max: 0.4536
        // [0.0017 msec] read_field(24999, 0) -> UWYH
        // [0.0370 msec] find_row_by_key(UWYH) -> Some("{ \"symbol\": \"UWYH\", \"exchange\": \"AMEX\", \"last_sale\": 30.419820155298005 }")
        // [0.0129 msec] find_row_by_key(UWYH) -> Some("{ \"symbol\": \"UWYH\", \"exchange\": \"AMEX\", \"last_sale\": 30.419820155298005 }")
        // [0.0127 msec] find_row_by_key(UWYH) -> Some("{ \"symbol\": \"UWYH\", \"exchange\": \"AMEX\", \"last_sale\": 30.419820155298005 }")
        // [47.8739 msec] find_row(0, UWYH) -> Some("{ \"symbol\": \"UWYH\", \"exchange\": \"AMEX\", \"last_sale\": 30.419820155298005 }")
        // [331.6797 msec] audit_collisions (0) -> []

        // perform an audit of the hash
        let collisions = match measure_time(|| stocks.audit()) {
            (TypedValue::Array(collisions), msec) => {
                println!("[{:.4} msec] audit_collisions ({}) -> {}", msec, collisions.len(), TypedValue::Array(collisions.to_owned()));
                collisions
            }
            (other, _) => {
                println!("[{:.4} msec] audit_collisions -> {}", msec, other);
                assert_eq!(other, TypedValue::Array(Vec::new()));
                Vec::new()
            }
        };

        // verification
        assert!(row_a.is_some() && row_b.is_some());
        assert_eq!(row_a, row_b);
        assert!(msec_b <= msec_a);
        assert!(collisions.is_empty())
    }

    fn measure_time<F, R>(process: F) -> (R, f64)
    where
        F: FnOnce() -> R,
    {
        // record the start time
        let start = Instant::now();

        // run the process
        let result = process();

        // record the end time
        let end = Instant::now();

        // compute the elapsed time in nanoseconds
        let duration_ns = end.duration_since(start).as_nanos();

        // convert to milliseconds (1 millisecond = 1,000,000 nanoseconds)
        (result, duration_ns as f64 / 1_000_000.0)
    }

    fn build_hash_key_table(
        ns: &Namespace,
        column_index: usize,
        bucket_count: u64,
        bucket_depth: u64,
    ) -> HashTableRowCollection {
        // create a table and write some rows to it
        let columns = make_quote_columns();
        let frc = FileRowCollection::create_table(&ns, columns.to_owned()).unwrap();
        let mut hkrc = HashTableRowCollection::create_with_options(column_index, bucket_count, bucket_depth, Box::new(frc)).unwrap();
        //assert_eq!(RowsAffected(9), hkrc.rebuild());
        hkrc
    }

    fn build_hash_key_table_with_samples(
        ns: &Namespace,
        column_index: usize,
        bucket_count: u64,
        bucket_depth: u64,
    ) -> HashTableRowCollection {
        // create a table and write some rows to it
        let columns = make_quote_columns();
        let mut frc = FileRowCollection::create_table(&ns, columns.to_owned()).unwrap();
        let quote_data = vec![
            ("ACT", "AMEX", 0.0021), ("ATT", "NYSE", 98.44), ("CRT", "OTC_BB", 1.2582),
            ("GE", "NYSE", 21.22), ("H", "OTC_BB", 0.0076), ("T", "NYSE", 43.167),
            ("X", "NASDAQ", 33.33), ("ABC", "NASDAQ", 5.04), ("PSY", "AMEX", 95.56),
        ];
        let rows = quote_data.iter()
            .map(|(symbol, exchange, last_sale)| {
                Row::new(0, vec![
                    StringValue(symbol.to_string()),
                    StringValue(exchange.to_string()),
                    Number(F64Value(*last_sale)),
                ])
            }).collect();
        assert_eq!(Outcome(Outcomes::RowsAffected(9)), frc.append_rows(rows));
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
        let mut hkrc = HashTableRowCollection::new(column_index, Box::new(frc)).unwrap();
        assert_eq!(Outcome(Outcomes::RowsAffected(9)), hkrc.rebuild());

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
        for s in TableRenderer::from_rows(
            hkrc.keys_table.get_columns().clone(),
            hkrc.keys_table.read_active_rows().unwrap()) { println!("{}", s) }
        hkrc
    }
}