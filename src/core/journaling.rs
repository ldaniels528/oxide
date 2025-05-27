#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
//      Journaling trait
////////////////////////////////////////////////////////////////////

use crate::columns::Column;
use crate::data_types::DataType::*;
use crate::dataframe::Dataframe;
use crate::dataframe::Dataframe::Disk;
use crate::errors::Errors::{Exact, TypeMismatch};
use crate::errors::{throw, TypeMismatchErrors};
use crate::expression::Expression;
use crate::expression::Expression::{FunctionCall, Literal, Variable};
use crate::field::FieldMetadata;
use crate::file_row_collection::FileRowCollection;
use crate::interpreter::Interpreter;
use crate::machine;
use crate::machine::Machine;
use crate::namespaces::Namespace;
use crate::number_kind::NumberKind::I64Kind;
use crate::numbers::Numbers;
use crate::numbers::Numbers::I64Value;
use crate::object_config::ObjectConfig;
use crate::parameter::Parameter;
use crate::row_collection::RowCollection;
use crate::row_metadata::RowMetadata;
use crate::sequences::{Array, Sequence};
use crate::structures::{Row, Structure};
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::*;
use actix::ActorTryFutureExt;
use chrono::Local;
use num_traits::ToPrimitive;
use serde::de::Error;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::fs;
use std::fs::{File, OpenOptions};
use std::sync::Arc;

/// Implemented by [RowCollection] classes offering the ability
/// to replay events from a journal
pub trait Journaling: RowCollection {
    /// returns the table's journal
    fn get_journal(&self) -> Dataframe;

    /// replays the journal to rebuild the current state
    fn replay(&mut self) -> std::io::Result<TypedValue>;
}

////////////////////////////////////////////////////////////////////
//      Event-Source Row-Collection class
////////////////////////////////////////////////////////////////////

/// Represents an Event-Source Row Collection
#[derive(Clone)]
pub struct EventSourceRowCollection {
    namespace: Namespace,
    columns: Vec<Column>,
    events: FileRowCollection,
    state: FileRowCollection,
}

impl EventSourceRowCollection {

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
                Parameter::new("row_id", NumberType(I64Kind)),
                Parameter::new("column_id", NumberType(I64Kind)),
                Parameter::new("action", FixedSizeType(StringType.into(), 2)),
                Parameter::new("new_value", FixedSizeType(StringType.into(), 256)),
                Parameter::new("created_time", DateTimeType),
            ])?,
            state: FileRowCollection::open_or_create(ns, columns.clone())?,
        })
    }

    fn make_field_action(id: usize, column_id: usize, action: &str, value: &TypedValue) -> Row {
        Row::new(id, vec![
            Number(I64Value(id as i64)),
            Number(I64Value(column_id as i64)),
            StringValue(action.to_string()),
            StringValue(value.to_code()),
            DateValue(Local::now().timestamp_millis()),
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

impl Debug for EventSourceRowCollection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "EventSourceRowCollection({})", self.get_record_size())
    }
}

impl Eq for EventSourceRowCollection {}

impl Journaling for EventSourceRowCollection {
    fn get_journal(&self) -> Dataframe {
        Disk(self.events.clone())
    }

    fn replay(&mut self) -> std::io::Result<TypedValue> {
        let mut rows_affected = 0;
        let empty_values = self.state.get_columns().iter().map(|_| TypedValue::Null).collect::<Vec<_>>();
        let mut interpreter = Interpreter::new();

        // rebuild the current state
        self.state.resize(0)?;
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
                                    self.state.overwrite_field(id, column_id.to_usize(), value)?;
                                }
                                Err(err) => eprintln!("{}", err)
                            }
                        // change row action
                        "CR" =>
                            match interpreter.evaluate(new_value.unwrap_value().as_str()) {
                                Ok(ArrayValue(value)) => {
                                    let id = row_id.to_usize();
                                    self.state.overwrite_row(id, Row::new(id, value.get_values().clone()))?;
                                    rows_affected += 1;
                                }
                                Ok(value) => eprintln!("Expected array value: {}", value.to_code()),
                                Err(err) => eprintln!("{}", err)
                            }
                        // delete row action
                        "DR" => {
                            self.state.delete_row(row_id.to_usize())?;
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
        Ok(Number(Numbers::I64Value(rows_affected)))
    }
}

impl Ord for EventSourceRowCollection {
    fn cmp(&self, other: &Self) -> Ordering {
        self.get_record_size().cmp(&other.get_record_size())
    }
}

impl PartialEq for EventSourceRowCollection {
    fn eq(&self, other: &Self) -> bool {
        self.get_record_size() == other.get_record_size()
    }
}

impl PartialOrd for EventSourceRowCollection {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.get_record_size().partial_cmp(&other.get_record_size())
    }
}

impl RowCollection for EventSourceRowCollection {
    fn delete_row(&mut self, id: usize) -> std::io::Result<i64> {
        let (row, _) = self.read_row(id)?;
        self.events.append_row(Self::make_row_action(id, 0, "DR", &row));
        self.state.delete_row(id)
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

    fn overwrite_field(&mut self, id: usize, column_id: usize, new_value: TypedValue) -> std::io::Result<i64> {
        self.events.append_row(Self::make_field_action(id, column_id, "CF", &new_value));
        self.state.overwrite_field(id, column_id, new_value)
    }

    fn overwrite_field_metadata(&mut self, id: usize, column_id: usize, metadata: FieldMetadata) -> std::io::Result<i64> {
        self.state.overwrite_field_metadata(id, column_id, metadata)
    }

    fn overwrite_row(&mut self, id: usize, row: Row) -> std::io::Result<i64> {
        self.events.append_row(Self::make_row_action(id, 0, "CR", &row));
        self.state.overwrite_row(id, row)
    }

    fn overwrite_row_metadata(&mut self, id: usize, metadata: RowMetadata) -> std::io::Result<i64> {
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

    fn resize(&mut self, new_size: usize) -> std::io::Result<bool> {
        self.state.resize(new_size)
    }
}

impl Serialize for EventSourceRowCollection {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("EventSourceRowCollection", 2)?;
        state.serialize_field("columns", &self.get_columns())?;
        state.serialize_field("ns", &self.get_namespace())?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for EventSourceRowCollection {
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
        EventSourceRowCollection::new(&helper.ns, &helper.columns).map_err(D::Error::custom)
    }
}

////////////////////////////////////////////////////////////////////
//      TableFunction class
////////////////////////////////////////////////////////////////////

/// Represents a Table Function
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
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

    /// Creates a new table function within the specified namespace
    /// and having the specified input- and output-columns
    pub fn create_table_fn(
        ns: &Namespace,
        params: Vec<Parameter>,
        code: Expression,
        ms0: Machine,
    ) -> std::io::Result<Self> {
        // build a new configuration
        let cfg = ObjectConfig::build_table_fn(
            params.clone(),
            code.clone(),
        );

        // save the configuration
        cfg.save(ns)?;

        // instantiate the table function
        Self::initialize(ns, cfg, ms0, true)
    }

    pub fn from_namespace(
        ns: &Namespace,
        ms0: Machine,
    ) -> std::io::Result<Self> {
        Self::initialize(ns, ObjectConfig::load(&ns)?, ms0, false)
    }

    pub fn initialize(
        ns: &Namespace,
        cfg: ObjectConfig,
        ms0: Machine,
        is_create: bool,
    ) -> std::io::Result<Self> {
        match cfg {
            ObjectConfig::TableFnConfig { columns, code, .. } => {
                // build the journal and state columns
                let journal_columns = Column::from_parameters(&columns);
                let state_columns = match Expression::infer_with_hints(&code, &columns) {
                    StructureType(params) => Column::from_parameters(&params),
                    TableType(params, ..) => Column::from_parameters(&params),
                    z => return throw(TypeMismatch(TypeMismatchErrors::UnsupportedType(StructureType(vec![]), z)))
                };

                // build the journal device
                let journal_path = ns.get_table_fn_journal_file_path();
                let journal = FileRowCollection::new(
                    journal_columns.clone(),
                    Arc::new(Self::load_or_create_file(ns, journal_path.as_str(), is_create)?),
                    journal_path.as_str(),
                );

                // build the state device
                let state_path = ns.get_table_fn_state_file_path();
                let state = FileRowCollection::new(
                    state_columns,
                    Arc::new(Self::load_or_create_file(ns, state_path.as_str(), is_create)?),
                    state_path.as_str(),
                );

                Ok(Self::new(
                    columns,
                    code,
                    Dataframe::Disk(journal),
                    Dataframe::Disk(state),
                    ms0,
                ))
            }
            _ => throw(Exact("Table configuration is invalid for this device".into())),
        }
    }

    /// Creates a new table function
    pub fn new(
        params: Vec<Parameter>,
        code: Expression,
        journal: Dataframe,
        state: Dataframe,
        ms0: Machine,
    ) -> Self {
        // println!();
        // println!("TableFunction::params:  ({})", Parameter::render(&params));
        // println!("TableFunction::code:    {}", code.to_code());
        // println!("TableFunction::journal: ({})", Parameter::render(&journal.get_parameters()));
        // println!("TableFunction::state:   ({})", Parameter::render(&state.get_parameters()));

        Self {
            columns: Column::from_parameters(&params),
            fx: Function {
                params,
                body: Box::from(code),
                returns: TableType(state.get_parameters()),
            },
            journal,
            state,
            ms0,
        }
    }

    fn load_or_create_file(
        ns: &Namespace,
        file_path: &str,
        is_create: bool,
    ) -> std::io::Result<File> {
        match is_create {
            true => {
                fs::create_dir_all(ns.get_root_path())?;
                OpenOptions::new().truncate(true).create(true).read(true).write(true)
                    .open(file_path)
            }
            false => {
                OpenOptions::new().read(true).write(true)
                    .open(file_path)
            }
        }
    }
}

impl Debug for TableFunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TableFunction({})", self.get_record_size())
    }
}

impl Journaling for TableFunction {
    fn get_journal(&self) -> Dataframe {
        self.journal.clone()
    }

    fn replay(&mut self) -> std::io::Result<TypedValue> {
        let mut rows_affected = 0;
        self.state.resize(0)?;
        for row in self.journal.get_rows() {
            self.overwrite_row(row.get_id(), row)?;
            rows_affected += 1;
        }
        println!("{rows_affected} event(s) replayed.");
        Ok(Number(Numbers::I64Value(rows_affected)))
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

    fn overwrite_field(&mut self, id: usize, column_id: usize, new_value: TypedValue) -> std::io::Result<i64> {
        self.state.overwrite_field(id, column_id, new_value)
    }

    fn overwrite_field_metadata(&mut self, id: usize, column_id: usize, metadata: FieldMetadata) -> std::io::Result<i64> {
        self.state.overwrite_field_metadata(id, column_id, metadata)
    }

    fn overwrite_row(&mut self, id: usize, row: Row) -> std::io::Result<i64> {
        self.journal.overwrite_row(id, row.clone())?;
        let ms = self.ms0
            .with_variable(machine::FX_SELF, self.fx.clone())
            .with_row(self.get_columns(), &row);
        let fx_call = FunctionCall {
            fx: Box::new(Variable(machine::FX_SELF.into())),
            args: row.get_values().iter().map(|v| Literal(v.clone())).collect(),
        };
        match ms.evaluate(&fx_call).map(|(_, v)| v)? {
            TypedValue::Structured(s) => {
                let new_row = s.to_row().with_row_id(id);
                self.state.overwrite_row(id, new_row)
            }
            value => throw(TypeMismatch(TypeMismatchErrors::UnsupportedType(StructureType(self.get_parameters()), value.get_type()))),
        }
    }

    fn overwrite_row_metadata(&mut self, id: usize, metadata: RowMetadata) -> std::io::Result<i64> {
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

    fn resize(&mut self, new_size: usize) -> std::io::Result<bool> {
        self.state.resize(new_size)
    }
}

/// Unit tests
#[cfg(test)]
mod tests {
    /// journaled unit tests
    #[cfg(test)]
    mod journaled_collection_tests {
        use crate::file_row_collection::FileRowCollection;
        use crate::journaling::{EventSourceRowCollection, Journaling};
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
            //  table stocks (symbol: String(8), exchange: String(8), last_sale: Date):::{
            //      journaling: true
            //  }
            let mut jrc = EventSourceRowCollection::new(
                &Namespace::new("event_src", "basics", "stocks"),
                &make_quote_parameters(),
            ).unwrap();

            // ensure the row collection is completely empty
            jrc.events.resize(0).unwrap();
            jrc.state.resize(0).unwrap();

            // insert some rows
            assert_eq!(5, jrc.append_rows(vec![
                make_quote(0, "ABC", "AMEX", 11.77),
                make_quote(1, "UNO", "OTC", 0.2456),
                make_quote(2, "BIZ", "NYSE", 23.66),
                make_quote(3, "GOTO", "OTC", 0.1428),
                make_quote(4, "BOOM", "NASDAQ", 56.87),
            ]).unwrap());

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
            assert_eq!(1, jrc.update_row(3, make_quote(3, "GOAT", "OTC", 0.1432)).unwrap());

            // replace a field
            assert_eq!(1, jrc.overwrite_field(0, 2, Number(F64Value(11.88))).unwrap());

            // delete a row
            assert_eq!(1, jrc.delete_row(1).unwrap());

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
            jrc.state.resize(0).unwrap();
            assert_eq!(jrc.replay().unwrap(), Number(Numbers::I64Value(7)));

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

    /// table function unit tests
    #[cfg(test)]
    mod table_function_tests {
        use crate::compiler::Compiler;
        use crate::data_types::DataType::NumberType;
        use crate::dataframe::Dataframe::Model;
        use crate::journaling::{Journaling, TableFunction};
        use crate::machine::Machine;
        use crate::model_row_collection::ModelRowCollection;
        use crate::number_kind::NumberKind::I64Kind;
        use crate::numbers::Numbers;
        use crate::numbers::Numbers::{F64Value, I64Value};
        use crate::parameter::Parameter;
        use crate::row_collection::RowCollection;
        use crate::structures::Row;
        use crate::table_renderer::TableRenderer;
        use crate::testdata::{make_quote, make_quote_parameters};
        use crate::typed_values::TypedValue::{Number, StringValue};

        #[test]
        fn test_table_function() {
            let mut tf = make_table_function();

            // insert a new row
            let result = tf.overwrite_row(0, Row::new(0, vec![
                StringValue("ABC".into()),
                StringValue("NYSE".into()),
                Number(F64Value(3.25)),
            ])).unwrap();

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
                    Number(I64Value(1)),
                ])
            ]);
            assert_eq!(result, 1)
        }

        #[test]
        fn test_replay() {
            let mut tfrc = make_table_function();

            // ensure row collection is completely empty
            tfrc.journal.resize(0).unwrap();
            tfrc.state.resize(0).unwrap();

            // insert some rows
            assert_eq!(5, tfrc.append_rows(vec![
                make_quote(0, "ABC", "AMEX", 11.77),
                make_quote(1, "UNO", "OTC", 0.2456),
                make_quote(2, "BIZ", "NYSE", 23.66),
                make_quote(3, "GOTO", "OTC", 0.1428),
                make_quote(4, "BOOM", "NASDAQ", 56.87),
            ]).unwrap());

            // replay the outputs
            assert_eq!(tfrc.replay().unwrap(), Number(Numbers::I64Value(5)));

            // verify the rows
            assert_eq!(tfrc.read_active_rows().unwrap(), vec![
                Row::new(0, vec![
                    StringValue("ABC".into()),
                    StringValue("AMEX".into()),
                    Number(F64Value(23.54)),
                    Number(I64Value(1))
                ]),
                Row::new(1, vec![
                    StringValue("UNO".into()),
                    StringValue("OTC".into()),
                    Number(F64Value(0.4912)),
                    Number(I64Value(2))
                ]),
                Row::new(2, vec![
                    StringValue("BIZ".into()),
                    StringValue("NYSE".into()),
                    Number(F64Value(47.32)),
                    Number(I64Value(3))
                ]),
                Row::new(3, vec![
                    StringValue("GOTO".into()),
                    StringValue("OTC".into()),
                    Number(F64Value(0.2856)),
                    Number(I64Value(4))
                ]),
                Row::new(4, vec![
                    StringValue("BOOM".into()),
                    StringValue("NASDAQ".into()),
                    Number(F64Value(113.74)),
                    Number(I64Value(5))
                ]),
            ]);
        }

        fn make_table_function() -> TableFunction {
            let params = make_quote_parameters();
            TableFunction::new(
                params.clone(),
                Compiler::build(r#"
                 { symbol: symbol, exchange: exchange, last_sale: last_sale * 2.0, rank: __row_id__ + 1 }
                "#).unwrap(),
                Model(ModelRowCollection::from_parameters(&params)),
                Model(ModelRowCollection::from_parameters(&{
                    let mut new_params = params.clone();
                    new_params.push(Parameter::new("rank", NumberType(I64Kind)));
                    new_params
                })),
                Machine::new_platform(),
            )
        }
    }
}