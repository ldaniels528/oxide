#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Dataframe class
////////////////////////////////////////////////////////////////////

use crate::blob_file_row_collection::BLOBFileRowCollection;
use crate::byte_row_collection::ByteRowCollection;
use crate::columns::Column;
use crate::data_types::DataType;
use crate::expression::{Conditions, Expression};
use crate::field::FieldMetadata;
use crate::file_row_collection::FileRowCollection;
use crate::hybrid_row_collection::HybridRowCollection;
use crate::journaling::{EventSourceRowCollection, TableFunction};
use crate::machine::Machine;
use crate::model_row_collection::ModelRowCollection;
use crate::namespaces::Namespace;
use crate::object_config::ObjectConfig;
use crate::parameter::Parameter;
use crate::row_collection::RowCollection;
use crate::row_metadata::RowMetadata;
use crate::sequences::Sequence;
use crate::structures::{Row, Structure};
use crate::test_engine::TestState;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::StringValue;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// DataFrame is a logical representation of a table
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Dataframe {
    Binary(ByteRowCollection),
    Blob(BLOBFileRowCollection),
    Disk(FileRowCollection),
    EventSource(EventSourceRowCollection),
    Hybrid(HybridRowCollection),
    Model(ModelRowCollection),
    TestReport(ModelRowCollection, TestState),
    TableFn(Box<TableFunction>),
}

impl Dataframe {
    /// Creates a new table within the specified namespace and having the specified columns
    pub fn create_table(ns: &Namespace, params: &Vec<Parameter>) -> std::io::Result<Self> {
        let path = ns.get_table_file_path();
        let columns = Column::from_parameters(params);
        let config = ObjectConfig::build_table(params.clone());
        config.save(&ns)?;
        let file = Arc::new(FileRowCollection::table_file_create(ns)?);
        Ok(Self::Disk(FileRowCollection::new(columns, file, path.as_str())))
    }

    /// deletes rows from the table based on a condition
    pub fn delete_where(
        mut self,
        machine: &Machine,
        condition: &Option<Conditions>,
        limit: TypedValue,
    ) -> std::io::Result<i64> {
        let mut deleted = 0;
        for id in self.get_indices_with_limit(limit)? {
            // read an active row
            if let Some(row) = self.read_one(id)? {
                // if the predicate matches the condition, delete the row.
                if row.matches(machine, condition, self.get_columns()) {
                    deleted += self.delete_row(id)?;
                }
            }
        }
        Ok(deleted)
    }
    
    /// Creates a dataframe from a grid of raw text values
    /// ```
    /// |--------------------------------------|
    /// | symbol | exchange | last_sale | rank |
    /// |--------------------------------------|
    /// | BOOM   | NYSE     | 113.76    | 1    |
    /// | ABC    | AMEX     | 24.98     | 2    |
    /// | JET    | NASDAQ   | 64.24     | 3    |
    /// |--------------------------------------|
    /// ```
    pub fn from_cells(cells: &Vec<Vec<String>>) -> Self {
        let (params, body) = Self::get_parameters_and_values(cells);
        let mut rows = vec![];
        for (id, items) in body.iter().enumerate() {
            let row = Row::new(id, items.to_owned());
            rows.push(row)
        }
        Dataframe::Model(ModelRowCollection::from_parameters_and_rows(&params, &rows))
    }

    fn get_parameters_and_values(cells: &Vec<Vec<String>>) -> (Vec<Parameter>, Vec<Vec<TypedValue>>) {
        /// Detects the type of the given column
        fn detect_type(rows: &Vec<Vec<TypedValue>>, column_index: usize) -> DataType {
            let mut types = vec![];
            for row in rows { types.push(row[column_index].get_type()); }
            DataType::best_fit(types)
        }

        // interpret the cells as a table
        let header = cells[0].clone();
        let body = cells[1..].iter()
            .map(|row| row.iter()
                .map(|text| match TypedValue::wrap_value(text) {
                    Ok(value) => value,
                    Err(_) => StringValue(text.into())
                }).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        let params = header.iter().enumerate()
            .map(|(n, name)| Parameter::new(name, detect_type(&body, n)))
            .collect::<Vec<_>>();

        (params, body)
    }

    /// overwrites rows that match the supplied criteria
    pub fn overwrite_where(
        df: Dataframe,
        machine: &Machine,
        fields: &Vec<Expression>,
        values: &Vec<Expression>,
        condition: &Option<Conditions>,
        limit: TypedValue,
    ) -> std::io::Result<(Dataframe, i64)> {
        let mut overwritten = 0;
        let mut df = df;
        for id in df.get_indices_with_limit(limit)? {
            // read an active row
            if let Some(row) = df.read_one(id)? {
                // if the predicate matches the condition, overwrite the row.
                if row.matches(machine, condition, df.get_columns()) {
                    let (machine, my_fields) =
                        machine.with_row(df.get_columns(), &row).evaluate_as_atoms(fields)?;
                    if let (_, TypedValue::ArrayValue(my_values)) = machine.evaluate_as_array(values)? {
                        let new_row = row.transform(df.get_columns(), &my_fields, &my_values.get_values())?;
                        overwritten += df.overwrite_row(row.get_id(), new_row)?;
                    }
                }
            }
        }
        Ok((df, overwritten))
    }
    
    /// restores rows from the table based on a condition
    pub fn undelete_where(
        &mut self,
        machine: &Machine,
        condition: &Option<Conditions>,
        limit: TypedValue,
    ) -> std::io::Result<i64> {
        let mut restored = 0;
        for id in self.get_indices_with_limit(limit)? {
            // read a row with its metadata
            let (row, metadata) = self.read_row(id)?;
            // if the row is inactive and the predicate matches the condition, restore the row.
            if !metadata.is_allocated && row.matches(machine, condition, self.get_columns()) {
                if self.undelete_row(id).is_ok() {
                    restored += 1
                }
            }
        }
        Ok(restored)
    }

    /// Returns an aggregate collection of unique columns
    fn get_combined_parameters(dataframes: &[Dataframe]) -> Vec<Parameter> {
        let mut combined_params: Vec<Parameter> = Vec::new();
        for df in dataframes {
            for param in df.get_parameters() {
                // get the next parameter name
                let name = param.get_name();
                // does a parameter with this name already exist?
                match combined_params.iter().position(|p| p.get_name() == name) {
                    None => combined_params.push(param),
                    Some(index) => {
                        // if so, build a type that fits both
                        let data_type = DataType::best_fit(vec![
                            combined_params[index].get_data_type(),
                            param.get_data_type()
                        ]);
                        // attempt to get a default value
                        let default = combined_params[index].get_default_value()
                            .coalesce(param.get_default_value());
                        // replace the parameter
                        let new_param = Parameter::new_with_default(name, data_type, default);
                        combined_params[index] = new_param;
                    }
                }
            }
        }
        combined_params
    }

    pub fn combine_tables(dataframes: Vec<Dataframe>) -> Dataframe {
        /// Appends a source dataframe to a destination dataframe
        fn merge(dest: &mut Dataframe, src: &Dataframe) {
            let src_params = src.get_parameters();
            let dst_params = dest.get_parameters();
            for row in src.get_rows() {
                let hash = row.to_hash_typed_value(&src_params);
                let values = dst_params.iter().map(|param| {
                    hash.get(param.get_name()).map(|v| v.clone())
                        .unwrap_or(TypedValue::Null)
                }).collect::<Vec<_>>();
                dest.append_row(Row::new(0, values));
            }
        }

        // determine the combine columns
        let params = Self::get_combined_parameters(&dataframes);
        let columns = Column::from_parameters(&params);

        // create a new table with the combine columns
        let mut mrc = Self::Model(ModelRowCollection::new(columns));
        for df in dataframes { merge(&mut mrc, &df); }
        mrc
    }

    pub fn intersect(&self, that: &Dataframe) -> Dataframe {
        fn make_key(row: &Row) -> String {
            row.get_values().iter().map(|v| v.unwrap_value())
                .collect::<Vec<_>>()
                .join("|")
        }
        fn do_intersect(df_a: &Dataframe, df_b: &Dataframe) -> Dataframe {
            // cache the data from the smaller table
            let mut hm = HashMap::new();
            for row_b in df_b.iter() {
                hm.entry(make_key(&row_b)).or_insert(true);
            }

            // write matching rows from the larger table
            let mut mrc = ModelRowCollection::new(df_a.get_columns().clone());
            for row_a in df_a.iter() {
                if hm.contains_key(&make_key(&row_a)) {
                    mrc.append_row(row_a);
                }
            }
            Dataframe::Model(mrc)
        }
        
        if self.len().unwrap_or(0) > that.len().unwrap_or(0) {
            do_intersect(self, that)
        } else {
            do_intersect(that, self)
        }
    }

    pub fn product(&self, that: &Dataframe) -> Dataframe {
        let mut product_columns = vec![];
        product_columns.extend(self.get_columns().clone());
        product_columns.extend(that.get_columns().clone());

        let mut mrc = ModelRowCollection::new(product_columns);
        for row_a in self.iter() {
            for row_b in that.iter() {
                let mut values = vec![];
                values.extend(row_a.get_values());
                values.extend(row_b.get_values());
                mrc.append_row(Row::new(0, values));
            }
        }
        Self::Model(mrc)
    }

    pub fn union(&self, that: &Dataframe) -> Dataframe {
        let mut mrc = ModelRowCollection::new(self.get_columns().clone());
        mrc.append_rows(self.get_rows()).ok();
        mrc.append_rows(that.get_rows()).ok();
        Self::Model(mrc)
    }

    pub fn to_model(self) -> ModelRowCollection {
        let (rows, columns) = (self.get_rows(), self.get_columns());
        ModelRowCollection::from_columns_and_rows(columns, &rows)
    }
    
    pub fn sort_by_columns(
        &self,
        columns: &Vec<(usize, bool)>
    ) -> std::io::Result<Dataframe> {
        // Step 1: Gather active rows
        let mut rows = self.get_rows();

        // Step 2: Perform multi-column sort
        rows.sort_by(|a, b| {
            for (col_idx, ascending) in columns {
                let va = &a[*col_idx];
                let vb = &b[*col_idx];
                match va.partial_cmp(vb) {
                    Some(std::cmp::Ordering::Equal) => continue,
                    Some(ordering) => {
                        return if *ascending { ordering } else { ordering.reverse() };
                    },
                    None => return std::cmp::Ordering::Equal,
                }
            }
            std::cmp::Ordering::Equal
        });

        // Step 3: Construct new sorted table
        let mut sorted = ModelRowCollection::with_rows(self.get_columns().clone(), vec![]);
        for (i, mut row) in rows.into_iter().enumerate() {
            row.with_row_id(i);
            sorted.append_row(row);
        }

        Ok(Self::Model(sorted))
    }

    pub fn sublist(&self, start: usize, end: usize) -> Self {
        let mut mrc = ModelRowCollection::new(self.get_columns().clone());
        let mut row_id = start;
        while row_id < end {
            match self.read_one(row_id) {
                Ok(Some(row)) => {
                    mrc.append_row(row.clone());
                },
                Ok(None) => {  },
                Err(e) => { eprintln!("Error reading row[{row_id}]: {e}") }
            }
            row_id += 1;
        }
        Dataframe::Model(mrc)
    }

    /// updates rows that match the supplied criteria
    pub fn update_where(
        mut df: Dataframe,
        ms: &Machine,
        fields: &Vec<Expression>,
        values: &Vec<Expression>,
        condition: &Option<Conditions>,
        limit: TypedValue,
    ) -> std::io::Result<i64> {
        let columns = df.get_columns().clone();
        let mut updated = 0;
        for id in df.get_indices_with_limit(limit)? {
            // read an active row
            if let Some(row) = df.read_one(id)? {
                // if the predicate matches the condition, update the row.
                if row.matches(ms, condition, &columns) {
                    let (ms, field_names) =
                        ms.with_row(&columns, &row).evaluate_as_atoms(fields)?;
                    if let (_, TypedValue::ArrayValue(field_values)) = ms.evaluate_as_array(values)? {
                        let new_row = row.transform(&columns, &field_names, &field_values.get_values())?;
                        let result = df.overwrite_row(id, new_row);
                        if result.is_ok() { updated += 1 }
                    }
                }
            }
        }
        Ok(updated)
    }
}

impl RowCollection for Dataframe {
    fn get_columns(&self) -> &Vec<Column> {
        match self {
            Self::Binary(df) => df.get_columns(),
            Self::Disk(df) => df.get_columns(),
            Self::Blob(df) => df.get_columns(),
            Self::EventSource(df) => df.get_columns(),
            Self::Hybrid(df) => df.get_columns(),
            Self::Model(df) => df.get_columns(),
            Self::TestReport(df, ..) => df.get_columns(),
            Self::TableFn(df) => df.get_columns(),
        }
    }

    fn get_record_size(&self) -> usize {
        match self {
            Self::Binary(df) => df.get_record_size(),
            Self::Disk(df) => df.get_record_size(),
            Self::Blob(df) => df.get_record_size(),
            Self::EventSource(df) => df.get_record_size(),
            Self::Hybrid(df) => df.get_record_size(),
            Self::Model(df) => df.get_record_size(),
            Self::TestReport(df, ..) => df.get_record_size(),
            Self::TableFn(df) => df.get_record_size(),
        }
    }

    fn get_rows(&self) -> Vec<Row> {
        match self {
            Self::Binary(df) => df.get_rows(),
            Self::Disk(df) => df.get_rows(),
            Self::Blob(df) => df.get_rows(),
            Self::EventSource(df) => df.get_rows(),
            Self::Hybrid(df) => df.get_rows(),
            Self::Model(df) => df.get_rows(),
            Self::TestReport(df, ..) => df.get_rows(),
            Self::TableFn(df) => df.get_rows(),
        }
    }

    fn len(&self) -> std::io::Result<usize> {
        match self {
            Self::Binary(df) => df.len(),
            Self::Disk(df) => df.len(),
            Self::Blob(df) => df.len(),
            Self::EventSource(df) => df.len(),
            Self::Hybrid(df) => df.len(),
            Self::Model(df) => df.len(),
            Self::TestReport(df, ..) => df.len(),
            Self::TableFn(df) => df.len(),
        }
    }

    fn overwrite_field(&mut self, id: usize, column_id: usize, new_value: TypedValue) -> std::io::Result<i64> {
        match self {
            Self::Binary(df) => df.overwrite_field(id, column_id, new_value),
            Self::Disk(df) => df.overwrite_field(id, column_id, new_value),
            Self::Blob(df) => df.overwrite_field(id, column_id, new_value),
            Self::EventSource(df) => df.overwrite_field(id, column_id, new_value),
            Self::Hybrid(df) => df.overwrite_field(id, column_id, new_value),
            Self::Model(df) => df.overwrite_field(id, column_id, new_value),
            Self::TestReport(df, ..) => df.overwrite_field(id, column_id, new_value),
            Self::TableFn(df) => df.overwrite_field(id, column_id, new_value),
        }
    }

    fn overwrite_field_metadata(&mut self, id: usize, column_id: usize, metadata: FieldMetadata) -> std::io::Result<i64> {
        match self {
            Self::Binary(df) => df.overwrite_field_metadata(id, column_id, metadata),
            Self::Disk(df) => df.overwrite_field_metadata(id, column_id, metadata),
            Self::Blob(df) => df.overwrite_field_metadata(id, column_id, metadata),
            Self::EventSource(df) => df.overwrite_field_metadata(id, column_id, metadata),
            Self::Hybrid(df) => df.overwrite_field_metadata(id, column_id, metadata),
            Self::Model(df) => df.overwrite_field_metadata(id, column_id, metadata),
            Self::TestReport(df, ..) => df.overwrite_field_metadata(id, column_id, metadata),
            Self::TableFn(df) => df.overwrite_field_metadata(id, column_id, metadata),
        }
    }

    fn overwrite_row(&mut self, id: usize, row: Row) -> std::io::Result<i64> {
        match self {
            Self::Binary(df) => df.overwrite_row(id, row),
            Self::Disk(df) => df.overwrite_row(id, row),
            Self::Blob(df) => df.overwrite_row(id, row),
            Self::EventSource(df) => df.overwrite_row(id, row),
            Self::Hybrid(df) => df.overwrite_row(id, row),
            Self::Model(df) => df.overwrite_row(id, row),
            Self::TestReport(df, ..) => df.overwrite_row(id, row),
            Self::TableFn(df) => df.overwrite_row(id, row),
        }
    }

    fn overwrite_row_metadata(&mut self, id: usize, metadata: RowMetadata) -> std::io::Result<i64> {
        match self {
            Self::Binary(df) => df.overwrite_row_metadata(id, metadata),
            Self::Disk(df) => df.overwrite_row_metadata(id, metadata),
            Self::Blob(df) => df.overwrite_row_metadata(id, metadata),
            Self::EventSource(df) => df.overwrite_row_metadata(id, metadata),
            Self::Hybrid(df) => df.overwrite_row_metadata(id, metadata),
            Self::Model(df) => df.overwrite_row_metadata(id, metadata),
            Self::TestReport(df, ..) => df.overwrite_row_metadata(id, metadata),
            Self::TableFn(df) => df.overwrite_row_metadata(id, metadata),
        }
    }

    fn read_field(&self, id: usize, column_id: usize) -> std::io::Result<TypedValue> {
        match self {
            Self::Binary(df) => df.read_field(id, column_id),
            Self::Disk(df) => df.read_field(id, column_id),
            Self::Blob(df) => df.read_field(id, column_id),
            Self::EventSource(df) => df.read_field(id, column_id),
            Self::Hybrid(df) => df.read_field(id, column_id),
            Self::Model(df) => df.read_field(id, column_id),
            Self::TestReport(df, ..) => df.read_field(id, column_id),
            Self::TableFn(df) => df.read_field(id, column_id),
        }
    }

    fn read_field_metadata(&self, id: usize, column_id: usize) -> std::io::Result<FieldMetadata> {
        match self {
            Self::Binary(df) => df.read_field_metadata(id, column_id),
            Self::Disk(df) => df.read_field_metadata(id, column_id),
            Self::Blob(df) => df.read_field_metadata(id, column_id),
            Self::EventSource(df) => df.read_field_metadata(id, column_id),
            Self::Hybrid(df) => df.read_field_metadata(id, column_id),
            Self::Model(df) => df.read_field_metadata(id, column_id),
            Self::TestReport(df, ..) => df.read_field_metadata(id, column_id),
            Self::TableFn(df) => df.read_field_metadata(id, column_id),
        }
    }

    fn read_row(&self, id: usize) -> std::io::Result<(Row, RowMetadata)> {
        match self {
            Self::Binary(df) => df.read_row(id),
            Self::Disk(df) => df.read_row(id),
            Self::Blob(df) => df.read_row(id),
            Self::EventSource(df) => df.read_row(id),
            Self::Hybrid(df) => df.read_row(id),
            Self::Model(df) => df.read_row(id),
            Self::TestReport(df, ..) => df.read_row(id),
            Self::TableFn(df) => df.read_row(id),
        }
    }

    fn read_row_metadata(&self, id: usize) -> std::io::Result<RowMetadata> {
        match self {
            Self::Binary(df) => df.read_row_metadata(id),
            Self::Disk(df) => df.read_row_metadata(id),
            Self::Blob(df) => df.read_row_metadata(id),
            Self::EventSource(df) => df.read_row_metadata(id),
            Self::Hybrid(df) => df.read_row_metadata(id),
            Self::Model(df) => df.read_row_metadata(id),
            Self::TestReport(df, ..) => df.read_row_metadata(id),
            Self::TableFn(df) => df.read_row_metadata(id),
        }
    }

    fn resize(&mut self, new_size: usize) -> std::io::Result<bool> {
        match self {
            Self::Binary(df) => df.resize(new_size),
            Self::Disk(df) => df.resize(new_size),
            Self::Blob(df) => df.resize(new_size),
            Self::EventSource(df) => df.resize(new_size),
            Self::Hybrid(df) => df.resize(new_size),
            Self::Model(df) => df.resize(new_size),
            Self::TestReport(df, ..) => df.resize(new_size),
            Self::TableFn(df) => df.resize(new_size),
        }
    }
}

/// Unit tests
#[cfg(test)]
mod tests {
    use crate::byte_row_collection::ByteRowCollection;
    use crate::data_types::DataType::{FixedSizeType, NumberType, StringType};
    use crate::dataframe::Dataframe;
    use crate::dataframe::Dataframe::Model;
    use crate::model_row_collection::ModelRowCollection;
    use crate::number_kind::NumberKind::{F64Kind, I64Kind};
    use crate::numbers::Numbers::{F64Value, I64Value};
    use crate::parameter::Parameter;
    use crate::row_collection::RowCollection;
    use crate::structures::Row;
    use crate::table_renderer::TableRenderer;
    use crate::testdata::{make_quote, make_quote_columns};
    use crate::tokenizer;
    use crate::tokens::Token;
    use crate::typed_values::TypedValue::{Null, Number, StringValue};

    #[test]
    fn test_to_model() {
        let df = create_dataframe();
        let mrc = df.clone().to_model();
        assert_eq!(mrc.get_columns(), df.get_columns());
        assert_eq!(mrc.get_rows(), df.get_rows());
    }

    #[test]
    fn test_get_combined_parameters() {
        let dfs = vec![
            Model(ModelRowCollection::from_parameters(&vec![
                Parameter::new("symbol", StringType),
                Parameter::new("exchange", StringType)
            ])),
            Model(ModelRowCollection::from_parameters(&vec![
                Parameter::new("symbol", StringType),
                Parameter::new("exchange", StringType),
                Parameter::new("last_sale", NumberType(F64Kind))
            ])),
            Model(ModelRowCollection::from_parameters(&vec![
                Parameter::new("symbol", StringType),
                Parameter::new("last_sale", NumberType(F64Kind)),
                Parameter::new("beta", NumberType(F64Kind))
            ])),
        ];
        assert_eq!(Dataframe::get_combined_parameters(&dfs), vec![
            Parameter::new("symbol", StringType),
            Parameter::new("exchange", StringType),
            Parameter::new("last_sale", NumberType(F64Kind)),
            Parameter::new("beta", NumberType(F64Kind))
        ])
    }

    #[test]
    fn test_get_parameters_and_values() {
        let tokens = tokenizer::parse_fully(r#"
            |--------------------------------------|
            | symbol | exchange | last_sale | rank |
            |--------------------------------------|
            | BOOM   | NYSE     | 113.76    | 1    |
            | ABC    | AMEX     | 24.98     | 2    |
            | JET    | NASDAQ   | 64.24     | 3    |
            |--------------------------------------|
        "#);
        assert_eq!(tokens.len(), 1);
        let cells = match tokens[0].clone() {
            Token::DataframeLiteral { cells, .. } => cells,
            _ => vec![]
        };

        assert_eq!(cells.len(), 4);
        let (params, data) = Dataframe::get_parameters_and_values(&cells);
        assert_eq!(params, vec![
            Parameter::new("symbol", FixedSizeType(StringType.into(), 4)),
            Parameter::new("exchange", FixedSizeType(StringType.into(), 6)),
            Parameter::new("last_sale", NumberType(F64Kind)),
            Parameter::new("rank", NumberType(I64Kind))
        ]);
        assert_eq!(data, vec![
            vec![
                StringValue("BOOM".into()),
                StringValue("NYSE".into()),
                Number(F64Value(113.76)),
                Number(I64Value(1))
            ],
            vec![
                StringValue("ABC".into()),
                StringValue("AMEX".into()),
                Number(F64Value(24.98)),
                Number(I64Value(2))
            ],
            vec![
                StringValue("JET".into()),
                StringValue("NASDAQ".into()),
                Number(F64Value(64.24)),
                Number(I64Value(3))
            ]
        ])
    }

    #[test]
    fn test_combine_tables() {
        let dfs = vec![
            Model(ModelRowCollection::from_parameters_and_rows(&vec![
                Parameter::new("symbol", StringType),
                Parameter::new("exchange", StringType)
            ], &vec![
                Row::new(0, vec![StringValue("ABC".into()), StringValue("AMEX".into())]),
            ])),
            Model(ModelRowCollection::from_parameters_and_rows(&vec![
                Parameter::new("symbol", StringType),
                Parameter::new("exchange", StringType),
                Parameter::new("last_sale", NumberType(F64Kind))
            ], &vec![
                make_quote(1, "UNO", "OTC", 0.2456),
                make_quote(2, "BIZ", "NYSE", 23.66),
            ])),
            Model(ModelRowCollection::from_parameters_and_rows(&vec![
                Parameter::new("symbol", StringType),
                Parameter::new("market", StringType),
                Parameter::new("beta", NumberType(F64Kind))
            ], &vec![
                make_quote(3, "GOTO", "DSE", 0.1421),
                make_quote(4, "BOOM", "TSX", 0.0087),
                make_quote(5, "TRX", "ZTX", 0.9311),
            ])),
        ];
        let params = Dataframe::get_combined_parameters(&dfs);
        for (n, c) in params.iter().enumerate() {
            println!("{}. {}: {}", n + 1, c.get_name(), c.get_data_type().to_code())
        }
        let df = Dataframe::combine_tables(dfs);
        assert_eq!(df.get_parameters(), params);

        // |-------------------------------------------------|
        // | symbol | exchange | last_sale | market | beta   |
        // |-------------------------------------------------|
        // | ABC    | AMEX     | null      | null   | null   |
        // | UNO    | OTC      | 0.2456    | null   | null   |
        // | BIZ    | NYSE     | 23.66     | null   | null   |
        // | GOTO   | null     | null      | DSE    | 0.1421 |
        // | BOOM   | null     | null      | TSX    | 0.0087 |
        // | TRX    | null     | null      | ZTX    | 0.9311 |
        // |-------------------------------------------------|
        for r in TableRenderer::from_dataframe(&df) { println!("{}", r) }
        assert_eq!(df.get_rows(), vec![
            Row::new(0, vec![StringValue("ABC".into()), StringValue("AMEX".into()), Null, Null, Null]),
            Row::new(1, vec![StringValue("UNO".into()), StringValue("OTC".into()), Number(F64Value(0.2456)), Null, Null]),
            Row::new(2, vec![StringValue("BIZ".into()), StringValue("NYSE".into()), Number(F64Value(23.66)), Null, Null]),
            Row::new(3, vec![StringValue("GOTO".into()), Null, Null, StringValue("DSE".into()), Number(F64Value(0.1421))]),
            Row::new(4, vec![StringValue("BOOM".into()), Null, Null, StringValue("TSX".into()), Number(F64Value(0.0087))]),
            Row::new(5, vec![StringValue("TRX".into()), Null, Null, StringValue("ZTX".into()), Number(F64Value(0.9311))]),
        ])
    }

    fn create_dataframe() -> Dataframe {
        Dataframe::Binary(ByteRowCollection::from_columns_and_rows(make_quote_columns(), vec![
            make_quote(0, "AAB", "NYSE", 22.44),
            make_quote(1, "XYZ", "NASDAQ", 66.67),
            make_quote(2, "SSO", "NYSE", 123.44),
            make_quote(3, "RAND", "AMEX", 11.33),
            make_quote(4, "IBM", "NYSE", 21.22),
            make_quote(5, "ATT", "NYSE", 98.44),
            make_quote(6, "HOCK", "AMEX", 0.0076),
            make_quote(7, "XIE", "NASDAQ", 33.33),
        ]))
    }
}