#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Dataframe class
////////////////////////////////////////////////////////////////////

use crate::byte_row_collection::ByteRowCollection;
use crate::columns::Column;
use crate::dataframe::Dataframe::Model;
use crate::expression::{Conditions, Expression};
use crate::field::FieldMetadata;
use crate::file_row_collection::FileRowCollection;
use crate::hybrid_row_collection::HybridRowCollection;
use crate::journaling::JournaledRowCollection;
use crate::machine::Machine;
use crate::model_row_collection::ModelRowCollection;
use crate::namespaces::Namespace;
use crate::numbers::Numbers::RowsAffected;
use crate::object_config::ObjectConfig;
use crate::parameter::Parameter;
use crate::row_collection::RowCollection;
use crate::row_metadata::RowMetadata;
use crate::sequences::Sequence;
use crate::structures::Row;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::Number;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;

/// DataFrame is a logical representation of table
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Dataframe {
    Binary(ByteRowCollection),
    Disk(FileRowCollection),
    Hybrid(HybridRowCollection),
    Journaled(JournaledRowCollection),
    Model(ModelRowCollection),
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
    ) -> std::io::Result<TypedValue> {
        let mut deleted = 0;
        for id in self.get_indices_with_limit(limit)? {
            // read an active row
            if let Some(row) = self.read_one(id)? {
                // if the predicate matches the condition, delete the row.
                if row.matches(machine, condition, self.get_columns()) {
                    deleted += self.delete_row(id).to_result(|v| v.to_i64())?;
                }
            }
        }
        Ok(Number(RowsAffected(deleted)))
    }

    /// overwrites rows that match the supplied criteria
    pub fn overwrite_where(
        df: Dataframe,
        machine: &Machine,
        fields: &Vec<Expression>,
        values: &Vec<Expression>,
        condition: &Option<Conditions>,
        limit: TypedValue,
    ) -> std::io::Result<(Dataframe, TypedValue)> {
        let mut overwritten = 0;
        let mut df = df;
        for id in df.get_indices_with_limit(limit)? {
            // read an active row
            if let Some(row) = df.read_one(id)? {
                // if the predicate matches the condition, overwrite the row.
                if row.matches(machine, condition, df.get_columns()) {
                    let (machine, my_fields) =
                        machine.with_row(df.get_columns(), &row).evaluate_as_atoms(fields)?;
                    if let (_, TypedValue::ArrayValue(my_values)) = machine.evaluate_array(values)? {
                        let new_row = row.transform(df.get_columns(), &my_fields, &my_values.get_values())?;
                        overwritten += df.overwrite_row(row.get_id(), new_row).to_result(|v| v.to_i64())?;
                    }
                }
            }
        }
        Ok((df, Number(RowsAffected(overwritten))))
    }

    pub fn to_model(self) -> ModelRowCollection {
        let (rows, columns) = (self.get_rows(), self.get_columns());
        ModelRowCollection::from_columns_and_rows(columns, &rows)
    }

    /// restores rows from the table based on a condition
    pub fn undelete_where(
        &mut self,
        machine: &Machine,
        condition: &Option<Conditions>,
        limit: TypedValue,
    ) -> std::io::Result<TypedValue> {
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
        Ok(Number(RowsAffected(restored)))
    }

    /// Returns an aggregate collection of unique columns
    pub fn get_combined_parameters(dataframes: &[Dataframe]) -> Vec<Parameter> {
        let mut seen_names = HashSet::new();
        let mut combined_params = Vec::new();

        for df in dataframes {
            for param in df.get_parameters() {
                let name = param.get_name();
                if seen_names.insert(name.to_string()) {
                    combined_params.push(param);
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
        let mut mrc = Model(ModelRowCollection::new(columns));
        for df in dataframes { merge(&mut mrc, &df); }
        mrc
    }

    /// updates rows that match the supplied criteria
    pub fn update_where(
        mut rc: Dataframe,
        ms: &Machine,
        fields: &Vec<Expression>,
        values: &Vec<Expression>,
        condition: &Option<Conditions>,
        limit: TypedValue,
    ) -> std::io::Result<TypedValue> {
        let columns = rc.get_columns().clone();
        let mut updated = 0;
        for id in rc.get_indices_with_limit(limit)? {
            // read an active row
            if let Some(row) = rc.read_one(id)? {
                // if the predicate matches the condition, update the row.
                if row.matches(ms, condition, &columns) {
                    let (ms, field_names) =
                        ms.with_row(&columns, &row).evaluate_as_atoms(fields)?;
                    if let (_, TypedValue::ArrayValue(field_values)) = ms.evaluate_array(values)? {
                        let new_row = row.transform(&columns, &field_names, &field_values.get_values())?;
                        let result = rc.overwrite_row(id, new_row);
                        if result.is_ok() { updated += 1 }
                    }
                }
            }
        }
        Ok(Number(RowsAffected(updated)))
    }
}

impl RowCollection for Dataframe {
    fn get_columns(&self) -> &Vec<Column> {
        match self {
            Self::Binary(rc) => rc.get_columns(),
            Self::Disk(rc) => rc.get_columns(),
            Self::Hybrid(rc) => rc.get_columns(),
            Self::Journaled(rc) => rc.get_columns(),
            Self::Model(rc) => rc.get_columns(),
        }
    }

    fn get_record_size(&self) -> usize {
        match self {
            Self::Binary(rc) => rc.get_record_size(),
            Self::Disk(rc) => rc.get_record_size(),
            Self::Hybrid(rc) => rc.get_record_size(),
            Self::Journaled(rc) => rc.get_record_size(),
            Self::Model(rc) => rc.get_record_size(),
        }
    }

    fn get_rows(&self) -> Vec<Row> {
        match self {
            Self::Binary(rc) => rc.get_rows(),
            Self::Disk(rc) => rc.get_rows(),
            Self::Hybrid(rc) => rc.get_rows(),
            Self::Journaled(rc) => rc.get_rows(),
            Self::Model(rc) => rc.get_rows(),
        }
    }

    fn len(&self) -> std::io::Result<usize> {
        match self {
            Self::Binary(rc) => rc.len(),
            Self::Disk(rc) => rc.len(),
            Self::Hybrid(rc) => rc.len(),
            Self::Journaled(rc) => rc.len(),
            Self::Model(rc) => rc.len(),
        }
    }

    fn overwrite_field(&mut self, id: usize, column_id: usize, new_value: TypedValue) -> TypedValue {
        match self {
            Self::Binary(rc) => rc.overwrite_field(id, column_id, new_value),
            Self::Disk(rc) => rc.overwrite_field(id, column_id, new_value),
            Self::Hybrid(rc) => rc.overwrite_field(id, column_id, new_value),
            Self::Journaled(rc) => rc.overwrite_field(id, column_id, new_value),
            Self::Model(rc) => rc.overwrite_field(id, column_id, new_value),
        }
    }

    fn overwrite_field_metadata(&mut self, id: usize, column_id: usize, metadata: FieldMetadata) -> TypedValue {
        match self {
            Self::Binary(rc) => rc.overwrite_field_metadata(id, column_id, metadata),
            Self::Disk(rc) => rc.overwrite_field_metadata(id, column_id, metadata),
            Self::Hybrid(rc) => rc.overwrite_field_metadata(id, column_id, metadata),
            Self::Journaled(rc) => rc.overwrite_field_metadata(id, column_id, metadata),
            Self::Model(rc) => rc.overwrite_field_metadata(id, column_id, metadata),
        }
    }

    fn overwrite_row(&mut self, id: usize, row: Row) -> TypedValue {
        match self {
            Self::Binary(rc) => rc.overwrite_row(id, row),
            Self::Disk(rc) => rc.overwrite_row(id, row),
            Self::Hybrid(rc) => rc.overwrite_row(id, row),
            Self::Journaled(rc) => rc.overwrite_row(id, row),
            Self::Model(rc) => rc.overwrite_row(id, row),
        }
    }

    fn overwrite_row_metadata(&mut self, id: usize, metadata: RowMetadata) -> TypedValue {
        match self {
            Self::Binary(rc) => rc.overwrite_row_metadata(id, metadata),
            Self::Disk(rc) => rc.overwrite_row_metadata(id, metadata),
            Self::Hybrid(rc) => rc.overwrite_row_metadata(id, metadata),
            Self::Journaled(rc) => rc.overwrite_row_metadata(id, metadata),
            Self::Model(rc) => rc.overwrite_row_metadata(id, metadata),
        }
    }

    fn read_field(&self, id: usize, column_id: usize) -> TypedValue {
        match self {
            Self::Binary(rc) => rc.read_field(id, column_id),
            Self::Disk(rc) => rc.read_field(id, column_id),
            Self::Hybrid(rc) => rc.read_field(id, column_id),
            Self::Journaled(rc) => rc.read_field(id, column_id),
            Self::Model(rc) => rc.read_field(id, column_id),
        }
    }

    fn read_field_metadata(&self, id: usize, column_id: usize) -> std::io::Result<FieldMetadata> {
        match self {
            Self::Binary(rc) => rc.read_field_metadata(id, column_id),
            Self::Disk(rc) => rc.read_field_metadata(id, column_id),
            Self::Hybrid(rc) => rc.read_field_metadata(id, column_id),
            Self::Journaled(rc) => rc.read_field_metadata(id, column_id),
            Self::Model(rc) => rc.read_field_metadata(id, column_id),
        }
    }

    fn read_row(&self, id: usize) -> std::io::Result<(Row, RowMetadata)> {
        match self {
            Self::Binary(rc) => rc.read_row(id),
            Self::Disk(rc) => rc.read_row(id),
            Self::Hybrid(rc) => rc.read_row(id),
            Self::Journaled(rc) => rc.read_row(id),
            Self::Model(rc) => rc.read_row(id),
        }
    }

    fn read_row_metadata(&self, id: usize) -> std::io::Result<RowMetadata> {
        match self {
            Self::Binary(rc) => rc.read_row_metadata(id),
            Self::Disk(rc) => rc.read_row_metadata(id),
            Self::Hybrid(rc) => rc.read_row_metadata(id),
            Self::Journaled(rc) => rc.read_row_metadata(id),
            Self::Model(rc) => rc.read_row_metadata(id),
        }
    }

    fn resize(&mut self, new_size: usize) -> TypedValue {
        match self {
            Self::Binary(rc) => rc.resize(new_size),
            Self::Disk(rc) => rc.resize(new_size),
            Self::Hybrid(rc) => rc.resize(new_size),
            Self::Journaled(rc) => rc.resize(new_size),
            Self::Model(rc) => rc.resize(new_size),
        }
    }
}

/// Unit tests
#[cfg(test)]
mod tests {
    use crate::byte_row_collection::ByteRowCollection;
    use crate::data_types::DataType::{NumberType, StringType};
    use crate::dataframe::Dataframe;
    use crate::dataframe::Dataframe::Model;
    use crate::model_row_collection::ModelRowCollection;
    use crate::number_kind::NumberKind::F64Kind;
    use crate::numbers::Numbers::F64Value;
    use crate::parameter::Parameter;
    use crate::row_collection::RowCollection;
    use crate::structures::Row;
    use crate::table_renderer::TableRenderer;
    use crate::testdata::{make_quote, make_quote_columns};
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
                Parameter::new("symbol", StringType(0)),
                Parameter::new("exchange", StringType(0))
            ])),
            Model(ModelRowCollection::from_parameters(&vec![
                Parameter::new("symbol", StringType(0)),
                Parameter::new("exchange", StringType(0)),
                Parameter::new("last_sale", NumberType(F64Kind))
            ])),
            Model(ModelRowCollection::from_parameters(&vec![
                Parameter::new("symbol", StringType(0)),
                Parameter::new("last_sale", NumberType(F64Kind)),
                Parameter::new("beta", NumberType(F64Kind))
            ])),
        ];
        assert_eq!(Dataframe::get_combined_parameters(&dfs), vec![
            Parameter::new("symbol", StringType(0)),
            Parameter::new("exchange", StringType(0)),
            Parameter::new("last_sale", NumberType(F64Kind)),
            Parameter::new("beta", NumberType(F64Kind))
        ])
    }

    #[test]
    fn test_combine_tables() {
        let dfs = vec![
            Model(ModelRowCollection::from_parameters_and_rows(&vec![
                Parameter::new("symbol", StringType(0)),
                Parameter::new("exchange", StringType(0))
            ], &vec![
                Row::new(0, vec![StringValue("ABC".into()), StringValue("AMEX".into())]),
            ])),
            Model(ModelRowCollection::from_parameters_and_rows(&vec![
                Parameter::new("symbol", StringType(0)),
                Parameter::new("exchange", StringType(0)),
                Parameter::new("last_sale", NumberType(F64Kind))
            ], &vec![
                make_quote(1, "UNO", "OTC", 0.2456),
                make_quote(2, "BIZ", "NYSE", 23.66),
            ])),
            Model(ModelRowCollection::from_parameters_and_rows(&vec![
                Parameter::new("symbol", StringType(0)),
                Parameter::new("market", StringType(0)),
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
        Dataframe::Binary(ByteRowCollection::from_rows(
            make_quote_columns(),
            vec![
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