#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
//      Journaling trait
////////////////////////////////////////////////////////////////////

use crate::columns::Column;
use crate::data_types::DataType::{NumberType, StringType, TableType};
use crate::dataframe::Dataframe;
use crate::errors::Errors::{Exact, TypeMismatch};
use crate::errors::TypeMismatchErrors::StructExpected;
use crate::expression::Expression;
use crate::expression::Expression::{FunctionCall, Literal, Variable};
use crate::field::FieldMetadata;
use crate::file_row_collection::FileRowCollection;
use crate::interpreter::Interpreter;
use crate::machine::Machine;
use crate::namespaces::Namespace;
use crate::number_kind::NumberKind::{DateKind, U16Kind, U64Kind};
use crate::numbers::Numbers;
use crate::numbers::Numbers::{DateValue, U16Value, U64Value};
use crate::parameter::Parameter;
use crate::row_collection::RowCollection;
use crate::row_metadata::RowMetadata;
use crate::sequences::{Array, Sequence};
use crate::structures::{Row, Structure};
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{ArrayValue, ErrorValue, Function, Number, StringValue};
use actix::ActorTryFutureExt;
use chrono::Local;
use num_traits::ToPrimitive;
use serde::de::Error;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};

/// Implemented by [RowCollection] classes offering the ability
/// to replay events from a journal
pub trait Journaling: RowCollection {
    /// replays the journal to rebuild the current state
    fn replay(&mut self) -> TypedValue;
}

////////////////////////////////////////////////////////////////////
//      JournaledRowCollection class
////////////////////////////////////////////////////////////////////

/// Represents a Journaled Row Collection
#[derive(Clone)]
pub struct JournaledRowCollection {
    namespace: Namespace,
    columns: Vec<Column>,
    events: FileRowCollection,
    state: FileRowCollection,
}

impl JournaledRowCollection {

    ////////////////////////////////////////////////////////////////////
    //      static functions
    ////////////////////////////////////////////////////////////////////

    /// Creates a new table function
    pub fn new(
        ns: &Namespace,
        columns: &Vec<Parameter>,
    ) -> std::io::Result<Self> {
        let events_ns = Namespace::new(ns.database.clone(), ns.schema.clone(), format!("{}_events", ns.name));
        Ok(Self {
            namespace: ns.clone(),
            columns: Column::from_parameters(&columns),
            events: FileRowCollection::open_or_create(&events_ns, vec![
                Parameter::new("row_id", NumberType(U64Kind)),
                Parameter::new("column_id", NumberType(U16Kind)),
                Parameter::new("action", StringType(2)),
                Parameter::new("new_value", StringType(256)),
                Parameter::new("created_time", NumberType(DateKind)),
            ])?,
            state: FileRowCollection::open_or_create(ns, columns.clone())?,
        })
    }

    fn make_field_action(id: usize, column_id: usize, action: &str, value: &TypedValue) -> Row {
        Row::new(id, vec![
            Number(U64Value(id as u64)),
            Number(U16Value(column_id as u16)),
            StringValue(action.to_string()),
            StringValue(value.to_code()),
            Number(DateValue(Local::now().timestamp_millis())),
        ])
    }

    fn make_row_action(id: usize, column_id: usize, action: &str, row: &Row) -> Row {
        let array = ArrayValue(Array::from(row.get_values()));
        Self::make_field_action(id, column_id, action, &array)
    }

    ////////////////////////////////////////////////////////////////////
    //      instance functions
    ////////////////////////////////////////////////////////////////////

    /// returns the namespace
    pub fn get_namespace(&self) -> &Namespace {
        &self.namespace
    }
}

impl Debug for JournaledRowCollection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "JournaledRowCollection({})", self.get_record_size())
    }
}

impl Eq for JournaledRowCollection {}

impl Journaling for JournaledRowCollection {
    fn replay(&mut self) -> TypedValue {
        let mut rows_affected = 0;
        let empty_values = self.state.get_columns().iter().map(|_| TypedValue::Null).collect::<Vec<_>>();
        let mut interpreter = Interpreter::new();

        // rebuild the current state
        self.state.resize(0);
        for row in self.events.iter() {
            match row.get_values().as_slice() {
                [row_id, column_id, action, new_value, _date] => {
                    match action.unwrap_value().as_str() {
                        // change field action
                        "CF" =>
                            match interpreter.evaluate(new_value.unwrap_value().as_str()) {
                                Ok(value) => {
                                    let id = row_id.to_usize();
                                    while id >= self.state.len().unwrap_or(0) {
                                        self.state.append_row(Row::new(id, empty_values.clone()));
                                        rows_affected += 1;
                                    };
                                    self.state.overwrite_field(id, column_id.to_usize(), value);
                                }
                                Err(err) => eprintln!("{}", err)
                            }
                        // change row action
                        "CR" =>
                            match interpreter.evaluate(new_value.unwrap_value().as_str()) {
                                Ok(ArrayValue(value)) => {
                                    let id = row_id.to_usize();
                                    self.state.overwrite_row(id, Row::new(id, value.get_values().clone()));
                                    rows_affected += 1;
                                }
                                Ok(value) => eprintln!("Expected array value: {}", value.to_code()),
                                Err(err) => eprintln!("{}", err)
                            }
                        // delete row action
                        "DR" => {
                            self.state.delete_row(row_id.to_usize());
                            rows_affected += 1;
                        }
                        // unhandled action
                        other =>
                            eprintln!("Invalid action code {other}: {}",
                                      row.to_json_string(self.events.get_columns()))
                    }
                }
                other => {
                    eprintln!("Invalid event: {:?}", other.to_vec())
                }
            }
        }
        Number(Numbers::RowsAffected(rows_affected))
    }
}

impl Ord for JournaledRowCollection {
    fn cmp(&self, other: &Self) -> Ordering {
        self.get_record_size().cmp(&other.get_record_size())
    }
}

impl PartialEq for JournaledRowCollection {
    fn eq(&self, other: &Self) -> bool {
        self.get_record_size() == other.get_record_size()
    }
}

impl PartialOrd for JournaledRowCollection {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.get_record_size().partial_cmp(&other.get_record_size())
    }
}

impl RowCollection for JournaledRowCollection {
    fn delete_row(&mut self, id: usize) -> TypedValue {
        match self.read_row(id) {
            Ok((row, _)) => {
                self.events.append_row(Self::make_row_action(id, 0, "DR", &row));
                self.state.delete_row(id)
            }
            Err(err) => ErrorValue(Exact(err.to_string()))
        }
    }

    fn get_columns(&self) -> &Vec<Column> {
        self.state.get_columns()
    }

    fn get_record_size(&self) -> usize {
        self.state.get_record_size()
    }

    fn get_rows(&self) -> Vec<Row> {
        self.state.get_rows()
    }

    fn len(&self) -> std::io::Result<usize> {
        self.state.len()
    }

    fn overwrite_field(&mut self, id: usize, column_id: usize, new_value: TypedValue) -> TypedValue {
        self.events.append_row(Self::make_field_action(id, column_id, "CF", &new_value));
        self.state.overwrite_field(id, column_id, new_value)
    }

    fn overwrite_field_metadata(&mut self, id: usize, column_id: usize, metadata: FieldMetadata) -> TypedValue {
        self.state.overwrite_field_metadata(id, column_id, metadata)
    }

    fn overwrite_row(&mut self, id: usize, row: Row) -> TypedValue {
        self.events.append_row(Self::make_row_action(id, 0, "CR", &row));
        self.state.overwrite_row(id, row)
    }

    fn overwrite_row_metadata(&mut self, id: usize, metadata: RowMetadata) -> TypedValue {
        self.state.overwrite_row_metadata(id, metadata)
    }

    fn read_field(&self, id: usize, column_id: usize) -> TypedValue {
        self.state.read_field(id, column_id)
    }

    fn read_field_metadata(&self, id: usize, column_id: usize) -> std::io::Result<FieldMetadata> {
        self.state.read_field_metadata(id, column_id)
    }

    fn read_row(&self, id: usize) -> std::io::Result<(Row, RowMetadata)> {
        self.state.read_row(id)
    }

    fn read_row_metadata(&self, id: usize) -> std::io::Result<RowMetadata> {
        self.state.read_row_metadata(id)
    }

    fn resize(&mut self, new_size: usize) -> TypedValue {
        self.state.resize(new_size)
    }
}

impl Serialize for JournaledRowCollection {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("JournaledRowCollection", 2)?;
        state.serialize_field("columns", &self.get_columns())?;
        state.serialize_field("ns", &self.get_namespace())?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for JournaledRowCollection {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // define a helper struct for deserialization
        #[derive(Deserialize)]
        struct JournaledRowCollectionHelper {
            columns: Vec<Parameter>,
            ns: Namespace,
        }

        let helper = JournaledRowCollectionHelper::deserialize(deserializer)?;
        JournaledRowCollection::new(&helper.ns, &helper.columns).map_err(D::Error::custom)
    }
}

////////////////////////////////////////////////////////////////////
//      TableFunction class
////////////////////////////////////////////////////////////////////

/// Represents a Table Function
#[derive(Clone)]
pub struct TableFunction {
    columns: Vec<Column>,
    fx: TypedValue,
    journal: Dataframe,
    state: Dataframe,
    ms0: Machine,
}

impl TableFunction {

    ////////////////////////////////////////////////////////////////////
    //      static functions
    ////////////////////////////////////////////////////////////////////

    /// Creates a new table function
    pub fn new(
        params: Vec<Parameter>,
        code: Expression,
        journal: Dataframe,
        state: Dataframe,
        ms0: Machine,
    ) -> Self {
        Self {
            columns: Column::from_parameters(&params),
            journal,
            state,
            fx: Function {
                params: params.clone(),
                body: Box::from(code),
                returns: TableType(params, 0)
            },
            ms0,
        }
    }
}

impl Debug for TableFunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TableFunction({})", self.get_record_size())
    }
}

impl Journaling for TableFunction {
    fn replay(&mut self) -> TypedValue {
        let mut rows_affected = 0;
        self.state.resize(0);
        for row in self.journal.get_rows() {
            self.overwrite_row(row.get_id(), row);
            rows_affected += 1;
        }
        Number(Numbers::RowsAffected(rows_affected))
    }
}

impl RowCollection for TableFunction {
    fn get_columns(&self) -> &Vec<Column> {
        self.state.get_columns()
    }

    fn get_record_size(&self) -> usize {
        self.state.get_record_size()
    }

    fn get_rows(&self) -> Vec<Row> {
        self.state.get_rows()
    }

    fn len(&self) -> std::io::Result<usize> {
        self.state.len()
    }

    fn overwrite_field(&mut self, id: usize, column_id: usize, new_value: TypedValue) -> TypedValue {
        self.state.overwrite_field(id, column_id, new_value)
    }

    fn overwrite_field_metadata(&mut self, id: usize, column_id: usize, metadata: FieldMetadata) -> TypedValue {
        self.state.overwrite_field_metadata(id, column_id, metadata)
    }

    fn overwrite_row(&mut self, id: usize, row: Row) -> TypedValue {
        let ms = self.ms0
            .with_variable("id", Number(Numbers::RowId(id as u64)))
            .with_variable("self", self.fx.clone())
            .with_row(self.get_columns(), &row);
        let result = match ms.evaluate(&FunctionCall {
            fx: Box::new(Variable("self".into())),
            args: row.get_values().iter().map(|v| Literal(v.clone())).collect(),
        }).map(|(_, v)| v) {
            Ok(TypedValue::Structured(s)) => self.state.overwrite_row(id, s.to_row()),
            Ok(value) => ErrorValue(TypeMismatch(StructExpected("Struct()".into(), value.get_type_name()))),
            Err(err) => ErrorValue(Exact(err.to_string()))
        };
        if let ErrorValue(x) = result {
            // TODO figure out how to apply the error to df1
            eprintln!("{}", x)
        }
        self.journal.overwrite_row(id, row)
    }

    fn overwrite_row_metadata(&mut self, id: usize, metadata: RowMetadata) -> TypedValue {
        self.state.overwrite_row_metadata(id, metadata)
    }

    fn read_field(&self, id: usize, column_id: usize) -> TypedValue {
        self.state.read_field(id, column_id)
    }

    fn read_field_metadata(&self, id: usize, column_id: usize) -> std::io::Result<FieldMetadata> {
        self.state.read_field_metadata(id, column_id)
    }

    fn read_row(&self, id: usize) -> std::io::Result<(Row, RowMetadata)> {
        self.state.read_row(id)
    }

    fn read_row_metadata(&self, id: usize) -> std::io::Result<RowMetadata> {
        self.state.read_row_metadata(id)
    }

    fn resize(&mut self, new_size: usize) -> TypedValue {
        self.state.resize(new_size)
    }
}

/// Unit tests
#[cfg(test)]
mod tests {
    /// Unit tests
    #[cfg(test)]
    mod table_function_tests {
        use crate::compiler::Compiler;
        use crate::data_types::DataType::NumberType;
        use crate::dataframe::Dataframe::Model;
        use crate::journaling::{Journaling, TableFunction};
        use crate::machine::Machine;
        use crate::model_row_collection::ModelRowCollection;
        use crate::number_kind::NumberKind::U64Kind;
        use crate::numbers::Numbers;
        use crate::numbers::Numbers::F64Value;
        use crate::parameter::Parameter;
        use crate::row_collection::RowCollection;
        use crate::structures::Row;
        use crate::table_renderer::TableRenderer;
        use crate::testdata::{make_quote, make_quote_parameters};
        use crate::typed_values::TypedValue::{Number, StringValue};

        #[test]
        fn test_function() {
            let mut tf = TableFunction::new(
                make_quote_parameters(),
                Compiler::build(r#"
             { symbol: symbol, exchange: exchange, last_sale: last_sale * 2.0, rank: id + 1 }
            "#).unwrap(),
                Model(ModelRowCollection::from_parameters(&make_quote_parameters())),
                Model(ModelRowCollection::from_parameters(&{
                    let mut params = make_quote_parameters();
                    params.push(Parameter::new("rank", NumberType(U64Kind)));
                    params
                })),
                Machine::new_platform(),
            );

            // insert a new row
            let result = tf.overwrite_row(0, Row::new(0, vec![
                StringValue("ABC".into()),
                StringValue("NYSE".into()),
                Number(F64Value(3.25)),
            ]));

            // display the source data
            // |-------------------------------|
            // | symbol | exchange | last_sale |
            // |-------------------------------|
            // | ABC    | NYSE     | 3.25      |
            // |-------------------------------|
            for s in TableRenderer::from_dataframe(&tf.journal) { println!("{}", s) }

            // display the transformed data
            // |--------------------------------------|
            // | symbol | exchange | last_sale | rank |
            // |--------------------------------------|
            // | ABC    | NYSE     | 6.5       | 1    |
            // |--------------------------------------|
            for s in TableRenderer::from_dataframe(&tf.state) { println!("{}", s) }

            // verify the results
            assert_eq!(tf.get_rows(), vec![
                Row::new(0, vec![
                    StringValue("ABC".into()),
                    StringValue("NYSE".into()),
                    Number(F64Value(6.50)),
                    Number(F64Value(1.)),
                ])
            ]);
            assert_eq!(result, Number(Numbers::RowsAffected(1)))
        }

        #[test]
        fn test_replay() {
            let mut tfrc = TableFunction::new(
                make_quote_parameters(),
                Compiler::build(r#"
             { symbol: symbol, exchange: exchange, last_sale: last_sale * 2.0, rank: id + 1 }
            "#).unwrap(),
                Model(ModelRowCollection::from_parameters(&make_quote_parameters())),
                Model(ModelRowCollection::from_parameters(&{
                    let mut params = make_quote_parameters();
                    params.push(Parameter::new("rank", NumberType(U64Kind)));
                    params
                })),
                Machine::new_platform(),
            );

            // ensure row collection is completely empty
            tfrc.journal.resize(0);
            tfrc.state.resize(0);

            // insert some rows
            assert_eq!(Number(Numbers::RowsAffected(5)), tfrc.append_rows(vec![
                make_quote(0, "ABC", "AMEX", 11.77),
                make_quote(1, "UNO", "OTC", 0.2456),
                make_quote(2, "BIZ", "NYSE", 23.66),
                make_quote(3, "GOTO", "OTC", 0.1428),
                make_quote(4, "BOOM", "NASDAQ", 56.87),
            ]));

            // replay the outputs
            assert_eq!(tfrc.replay(), Number(Numbers::RowsAffected(5)));

            // verify the rows
            assert_eq!(tfrc.read_active_rows().unwrap(), vec![
                Row::new(0, vec![
                    StringValue("ABC".into()),
                    StringValue("AMEX".into()),
                    Number(F64Value(23.54)),
                    Number(F64Value(1.0))
                ]),
                Row::new(1, vec![
                    StringValue("UNO".into()),
                    StringValue("OTC".into()),
                    Number(F64Value(0.4912)),
                    Number(F64Value(2.0))
                ]),
                Row::new(2, vec![
                    StringValue("BIZ".into()),
                    StringValue("NYSE".into()),
                    Number(F64Value(47.32)),
                    Number(F64Value(3.0))
                ]),
                Row::new(3, vec![
                    StringValue("GOTO".into()),
                    StringValue("OTC".into()),
                    Number(F64Value(0.2856)),
                    Number(F64Value(4.0))
                ]),
                Row::new(4, vec![
                    StringValue("BOOM".into()),
                    StringValue("NASDAQ".into()),
                    Number(F64Value(113.74)),
                    Number(F64Value(5.0))
                ]),
            ]);
        }
    }

    /// Unit tests
    #[cfg(test)]
    mod journaled_collection_tests {
        use crate::file_row_collection::FileRowCollection;
        use crate::journaling::{JournaledRowCollection, Journaling};
        use crate::namespaces::Namespace;
        use crate::numbers::Numbers;
        use crate::numbers::Numbers::F64Value;
        use crate::row_collection::RowCollection;
        use crate::table_renderer::TableRenderer;
        use crate::testdata::{make_quote, make_quote_parameters};
        use crate::typed_values::TypedValue::Number;

        #[test]
        fn test_events_crud() {
            // 1. create an event sourced table:
            //  table stocks (symbol: String(8), exchange: String(8), last_sale: Date)
            //      with journaling
            let mut jrc = JournaledRowCollection::new(
                &Namespace::new("event_src", "basics", "stocks"),
                &make_quote_parameters(),
            ).unwrap();

            // ensure row collection is completely empty
            jrc.events.resize(0);
            jrc.state.resize(0);

            // insert some rows
            assert_eq!(Number(Numbers::RowsAffected(5)), jrc.append_rows(vec![
                make_quote(0, "ABC", "AMEX", 11.77),
                make_quote(1, "UNO", "OTC", 0.2456),
                make_quote(2, "BIZ", "NYSE", 23.66),
                make_quote(3, "GOTO", "OTC", 0.1428),
                make_quote(4, "BOOM", "NASDAQ", 56.87),
            ]));

            // display the initial state
            // |------------------------------------|
            // | id | symbol | exchange | last_sale |
            // |------------------------------------|
            // | 0  | ABC    | AMEX     | 11.77     |
            // | 1  | UNO    | OTC      | 0.2456    |
            // | 2  | BIZ    | NYSE     | 23.66     |
            // | 3  | GOTO   | OTC      | 0.1428    |
            // | 4  | BOOM   | NASDAQ   | 56.87     |
            // |------------------------------------|
            show(&jrc.state, "initial state");

            // replace a row
            assert_eq!(
                jrc.update_row(3, make_quote(3, "GOAT", "OTC", 0.1432)),
                Number(Numbers::RowsAffected(1)));

            // replace a field
            assert_eq!(
                jrc.overwrite_field(0, 2, Number(F64Value(11.88))),
                Number(Numbers::RowsAffected(1)));

            // delete a row
            assert_eq!(jrc.delete_row(1), Number(Numbers::RowsAffected(1)));

            // display the current state
            // |------------------------------------|
            // | id | symbol | exchange | last_sale |
            // |------------------------------------|
            // | 0  | ABC    | AMEX     | 11.88     |
            // | 2  | BIZ    | NYSE     | 23.66     |
            // | 3  | GOAT   | OTC      | 0.1432    |
            // | 4  | BOOM   | NASDAQ   | 56.87     |
            // |------------------------------------|
            show(&jrc.state, "current state");

            // display the events
            // |-----------------------------------------------------------------------------------------|
            // | id | row_id | column_id | action | new_value                 | created_at               |
            // |-----------------------------------------------------------------------------------------|
            // | 0  | 0      | 0         | CR     | ["ABC", "AMEX", 11.77]    | 2025-01-13T03:25:47.350Z |
            // | 1  | 1      | 0         | CR     | ["UNO", "OTC", 0.2456]    | 2025-01-13T03:25:47.351Z |
            // | 2  | 2      | 0         | CR     | ["BIZ", "NYSE", 23.66]    | 2025-01-13T03:25:47.351Z |
            // | 3  | 3      | 0         | CR     | ["GOTO", "OTC", 0.1428]   | 2025-01-13T03:25:47.351Z |
            // | 4  | 4      | 0         | CR     | ["BOOM", "NASDAQ", 56.87] | 2025-01-13T03:25:47.351Z |
            // | 5  | 3      | 0         | CR     | ["GOAT", "OTC", 0.1432]   | 2025-01-13T03:25:47.351Z |
            // | 6  | 0      | 2         | CF     | 11.88                     | 2025-01-13T03:25:47.351Z |
            // | 7  | 1      | 0         | DR     | ["UNO", "OTC", 0.2456]    | 2025-01-13T03:25:47.351Z |
            // |-----------------------------------------------------------------------------------------|
            show(&jrc.events, "events");

            // verify the current state
            assert_eq!(jrc.get_rows(), vec![
                make_quote(0, "ABC", "AMEX", 11.88),
                make_quote(2, "BIZ", "NYSE", 23.66),
                make_quote(3, "GOAT", "OTC", 0.1432),
                make_quote(4, "BOOM", "NASDAQ", 56.87),
            ]);

            // replay the events (after truncating the current state)
            jrc.state.resize(0);
            assert_eq!(jrc.replay(), Number(Numbers::RowsAffected(7)));

            // display the reconstituted state
            // |------------------------------------|
            // | id | symbol | exchange | last_sale |
            // |------------------------------------|
            // | 0  | ABC    | AMEX     | 11.88     |
            // | 2  | BIZ    | NYSE     | 23.66     |
            // | 3  | GOAT   | OTC      | 0.1432    |
            // | 4  | BOOM   | NASDAQ   | 56.87     |
            // |------------------------------------|
            show(&jrc.state, "reconstituted state");

            // verify the replayed current state
            assert_eq!(jrc.get_rows(), vec![
                make_quote(0, "ABC", "AMEX", 11.88),
                make_quote(2, "BIZ", "NYSE", 23.66),
                make_quote(3, "GOAT", "OTC", 0.1432),
                make_quote(4, "BOOM", "NASDAQ", 56.87),
            ])
        }

        fn show(frc: &FileRowCollection, label: &str) {
            println!("{}", label);
            let lines = TableRenderer::from_rows_with_ids(frc.get_columns(), &frc.get_rows()).unwrap();
            for s in lines { println!("{}", s) }
        }
    }

}