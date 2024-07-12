////////////////////////////////////////////////////////////////////
// dataframe actor module
////////////////////////////////////////////////////////////////////

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::ops::Range;

use actix::prelude::*;
use serde::{Deserialize, Serialize};

use crate::dataframe_config::DataFrameConfig;
use crate::dataframes::DataFrame;
use crate::namespaces::Namespace;
use crate::rows::Row;
use crate::table_columns::TableColumn;

// define the Dataframe I/O actor
#[derive(Debug)]
pub struct DataframeActor {
    resources: HashMap<String, DataFrame>,
}

impl DataframeActor {
    /// default constructor
    pub fn new() -> Self {
        DataframeActor {
            resources: HashMap::new()
        }
    }

    fn append_row(&mut self, ns: Namespace, row: Row) -> std::io::Result<usize> {
        self.get_or_load_dataframe(ns)?.append(row)
    }

    fn create_table(&mut self, ns: Namespace, cfg: DataFrameConfig) -> std::io::Result<&mut DataFrame> {
        self.get_or_create_dataframe(ns, cfg)
    }

    fn delete_row(&mut self, ns: Namespace, id: usize) -> std::io::Result<usize> {
        self.get_or_load_dataframe(ns)?.delete(id)
    }

    fn get_columns(&mut self, ns: Namespace) -> std::io::Result<&Vec<TableColumn>> {
        Ok(&self.get_or_load_dataframe(ns)?.get_columns())
    }

    fn get_namespaces(&mut self) -> std::io::Result<Vec<String>> { // .sort_by(|a, b| b.cmp(a))
        Ok(self.resources.iter().map(|(s, _)| s.to_string()).collect::<Vec<String>>())
    }

    fn get_or_create_dataframe(&mut self, ns: Namespace, cfg: DataFrameConfig) -> std::io::Result<&mut DataFrame> {
        match self.resources.entry(ns.id()) {
            Entry::Occupied(v) => Ok(v.into_mut()),
            Entry::Vacant(x) =>
                Ok(x.insert(DataFrame::create(ns, cfg)?))
        }
    }

    fn get_or_load_dataframe(&mut self, ns: Namespace) -> std::io::Result<&mut DataFrame> {
        match self.resources.entry(ns.id()) {
            Entry::Occupied(v) => Ok(v.into_mut()),
            Entry::Vacant(x) => Ok(x.insert(DataFrame::load(ns)?))
        }
    }

    fn overwrite_row(&mut self, ns: Namespace, row: Row) -> std::io::Result<usize> {
        self.get_or_load_dataframe(ns)?.overwrite(row)
    }

    fn read_fully(&mut self, ns: Namespace) -> std::io::Result<Vec<Row>> {
        let df = self.get_or_load_dataframe(ns)?;
        let mut rows = vec![];
        for id in 0..df.len()? {
            df.read_then_push(id, &mut rows)?
        }
        Ok(rows)
    }

    fn read_range(&mut self, ns: Namespace, range: Range<usize>) -> std::io::Result<Vec<Row>> {
        let df = self.get_or_load_dataframe(ns)?;
        let mut rows = vec![];
        for id in range {
            df.read_then_push(id, &mut rows)?
        }
        Ok(rows)
    }

    fn read_row(&mut self, ns: Namespace, id: usize) -> std::io::Result<Option<Row>> {
        self.get_or_load_dataframe(ns)?.read_row(id).map(|(row, meta)| {
            if meta.is_allocated { Some(row) } else { None }
        })
    }

    fn update_row(&mut self, ns: Namespace, row: Row) -> std::io::Result<usize> {
        self.get_or_load_dataframe(ns)?.update(row)
    }
}

impl Actor for DataframeActor {
    type Context = Context<Self>;
}

impl Handler<IORequest> for DataframeActor {
    type Result = String;

    fn handle(&mut self, msg: IORequest, _: &mut Self::Context) -> Self::Result {
        match msg {
            IORequest::AppendRow { ns, row } =>
                handle_result(self.append_row(ns, row)),
            IORequest::CreateTable { ns, cfg } =>
                handle_result(self.create_table(ns, cfg).map(|_| 1usize)),
            IORequest::DeleteRow { ns, id } =>
                handle_result(self.delete_row(ns, id)),
            IORequest::GetColumns { ns } =>
                handle_result(self.get_columns(ns)),
            IORequest::GetNamespaces =>
                handle_result(self.get_namespaces()),
            IORequest::ReadFully { ns } =>
                handle_result(self.read_fully(ns)),
            IORequest::ReadRange { ns, range } =>
                handle_result(self.read_range(ns, range)),
            IORequest::ReadRow { ns, id } =>
                handle_result(self.read_row(ns, id)),
            IORequest::OverwriteRow { ns, row } =>
                handle_result(self.overwrite_row(ns, row)),
            IORequest::UpdateRow { ns, row } =>
                handle_result(self.update_row(ns, row)),
        }
    }
}

fn handle_result<T: serde::Serialize>(result: std::io::Result<T>) -> String {
    match result {
        Ok(outcome) => serde_json::json!(outcome).to_string(),
        Err(err) => err.to_string()
    }
}

////////////////////////////////////////////////////////////////////
// actor messages
////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Message, Serialize, Deserialize)]
#[rtype(result = "String")]
pub enum IORequest {
    AppendRow { ns: Namespace, row: Row },
    CreateTable { ns: Namespace, cfg: DataFrameConfig },
    DeleteRow { ns: Namespace, id: usize },
    GetColumns { ns: Namespace },
    GetNamespaces,
    ReadFully { ns: Namespace },
    ReadRow { ns: Namespace, id: usize },
    ReadRange { ns: Namespace, range: Range<usize> },
    OverwriteRow { ns: Namespace, row: Row },
    UpdateRow { ns: Namespace, row: Row },
}

////////////////////////////////////////////////////////////////////
// actor macros
////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! append_row {
    ($actor:expr, $ns:expr, $row:expr) => {
        $actor.send(crate::dataframe_actor::IORequest::AppendRow { ns: $ns.clone(), row: $row }).await
            .map(|s|serde_json::from_str::<usize>(&s).unwrap())
            .map_err(|e|crate::cnv_error!(e))
    }
}

#[macro_export]
macro_rules! create_table {
    ($actor:expr, $ns:expr, $columns:expr) => {
        $actor.send(crate::dataframe_actor::IORequest::CreateTable {
            ns: $ns.clone(),
            cfg: DataFrameConfig::new($columns, vec![], vec![])
        }).await
            .map(|s|serde_json::from_str::<usize>(&s).unwrap())
            .map_err(|e|crate::cnv_error!(e))
    }
}

#[macro_export]
macro_rules! create_table_from_config {
    ($actor:expr, $ns:expr, $cfg:expr) => {
        $actor.send(crate::dataframe_actor::IORequest::CreateTable { ns: $ns.clone(), cfg: $cfg }).await
            .map(|s|serde_json::from_str::<usize>(&s).unwrap())
            .map_err(|e|crate::cnv_error!(e))
    }
}

#[macro_export]
macro_rules! delete_row {
    ($actor:expr, $ns:expr, $id:expr) => {
        $actor.send(crate::dataframe_actor::IORequest::DeleteRow { ns: $ns.clone(), id: $id }).await
            .map(|s|serde_json::from_str::<usize>(&s).unwrap())
            .map_err(|e|crate::cnv_error!(e))
    }
}

#[macro_export]
macro_rules! get_columns {
    ($actor:expr, $ns:expr) => {
        $actor.send(crate::dataframe_actor::IORequest::GetColumns { ns: $ns.clone() }).await
            .map(|s|serde_json::from_str::<Vec<TableColumn>>(&s).unwrap())
            .map_err(|e|crate::cnv_error!(e))
    }
}

#[macro_export]
macro_rules! overwrite_row {
    ($actor:expr, $ns:expr, $row:expr) => {
        $actor.send(crate::dataframe_actor::IORequest::OverwriteRow { ns: $ns.clone(), row: $row }).await
            .map(|s|serde_json::from_str::<usize>(&s).unwrap())
            .map_err(|e|crate::cnv_error!(e))
    }
}

#[macro_export]
macro_rules! get_namespaces {
    ($actor:expr) => {
        $actor.send(crate::dataframe_actor::IORequest::GetNamespaces).await
            .map(|s|serde_json::from_str::<Vec<String>>(&s).unwrap())
            .map_err(|e|crate::cnv_error!(e))
    }
}

#[macro_export]
macro_rules! read_fully {
    ($actor:expr, $ns:expr) => {
        $actor.send(crate::dataframe_actor::IORequest::ReadFully { ns: $ns.clone() }).await
            .map(|s|serde_json::from_str::<Vec<Row>>(&s).unwrap())
            .map_err(|e|crate::cnv_error!(e))
    }
}

#[macro_export]
macro_rules! read_range {
    ($actor:expr, $ns:expr, $range:expr) => {
        $actor.send(crate::dataframe_actor::IORequest::ReadRange { ns: $ns.clone(), range: $range }).await
            .map(|s|serde_json::from_str::<Vec<Row>>(&s).unwrap())
            .map_err(|e|crate::cnv_error!(e))
    }
}

#[macro_export]
macro_rules! read_row {
    ($actor:expr, $ns:expr, $id:expr) => {
        $actor.send(crate::dataframe_actor::IORequest::ReadRow { ns: $ns.clone(), id: $id }).await
            .map(|s|serde_json::from_str::<Option<Row>>(&s).unwrap())
            .map_err(|e|crate::cnv_error!(e))
    }
}

#[macro_export]
macro_rules! update_row {
    ($actor:expr, $ns:expr, $row:expr) => {
        $actor.send(crate::dataframe_actor::IORequest::UpdateRow { ns: $ns.clone(), row: $row }).await
            .map(|s|serde_json::from_str::<usize>(&s).unwrap())
            .map_err(|e|crate::cnv_error!(e))
    }
}

////////////////////////////////////////////////////////////////////
// Unit tests
////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::vec;

    use actix::prelude::*;

    use crate::data_types::DataType::{Float64Type, StringType};
    use crate::row;
    use crate::testdata::{make_quote_columns, make_table_columns};
    use crate::typed_values::TypedValue::{Float64Value, Null, StringValue, Undefined};

    use super::*;

    #[actix::test]
    async fn test_get_columns() {
        let actor = DataframeActor::new().start();
        let ns = Namespace::parse("dataframe.columns.stocks").unwrap();
        assert_eq!(1, create_table!(actor, ns, make_quote_columns()).unwrap());
        assert_eq!(get_columns!(actor, ns).unwrap(), vec![
            TableColumn::new("symbol", StringType(8), Null, 9),
            TableColumn::new("exchange", StringType(8), Null, 26),
            TableColumn::new("last_sale", Float64Type, Null, 43),
        ]);
    }

    #[actix::test]
    async fn test_append_then_read() {
        let actor = DataframeActor::new().start();
        let ns = Namespace::parse("actors.append_then_read.stocks").unwrap();
        let table_columns = make_table_columns();

        // create the new empty table
        assert_eq!(1, create_table!(actor, ns, make_quote_columns()).unwrap());

        // append a new row to the table
        assert_eq!(1, append_row!(actor, ns, row!(0, table_columns, vec![
            StringValue("JUNO".into()), StringValue("AMEX".into()), Float64Value(11.88),
        ])).unwrap());

        // read the previously created row
        assert_eq!(read_row!(actor, ns, 0).unwrap().unwrap(), row!(0, table_columns, vec![
            StringValue("JUNO".into()), StringValue("AMEX".into()), Float64Value(11.88),
        ]));
    }

    #[actix::test]
    async fn test_dataframe_lifecycle() {
        let actor = DataframeActor::new().start();
        let ns = Namespace::parse("dataframe.actor_crud.stocks").unwrap();
        let table_columns = make_table_columns();

        // create the new empty table
        assert_eq!(1, create_table!(actor, ns, make_quote_columns()).unwrap());

        // append a new row to the table
        assert_eq!(1, append_row!(actor, ns, row!(111, table_columns, vec![
            StringValue("JUNO".into()), StringValue("AMEX".into()), Float64Value(22.88),
        ])).unwrap());

        // read the previously created row
        assert_eq!(read_row!(actor, ns, 0).unwrap().unwrap(), row!(0, table_columns, vec![
            StringValue("JUNO".into()), StringValue("AMEX".into()), Float64Value(22.88),
        ]));

        // overwrite a new row over offset 1
        assert_eq!(1, overwrite_row!(actor, ns, row!(1, table_columns, vec![
            StringValue("YARD".into()), StringValue("NYSE".into()), Float64Value(88.22),
        ])).unwrap());

        // read rows
        assert_eq!(read_range!(actor, ns, 0..2).unwrap(), vec![
            row!(0, table_columns, vec![
                StringValue("JUNO".into()), StringValue("AMEX".into()), Float64Value(22.88),
            ]),
            row!(1, table_columns, vec![
                StringValue("YARD".into()), StringValue("NYSE".into()), Float64Value(88.22),
            ]),
        ]);

        // update the row at offset 1
        assert_eq!(1, update_row!(actor, ns, row!(1, table_columns, vec![
            Undefined, Undefined, Float64Value(88.99),
        ])).unwrap());

        // re-read rows
        let rows = read_fully!(actor, ns).unwrap();
        assert_eq!(rows, vec![
            row!(0, table_columns, vec![
                StringValue("JUNO".into()), StringValue("AMEX".into()), Float64Value(22.88),
            ]),
            row!(1, table_columns, vec![
                StringValue("YARD".into()), StringValue("NYSE".into()), Float64Value(88.99),
            ]),
        ]);

        // delete row at offset 0
        assert_eq!(1, delete_row!(actor, ns, 0).unwrap());

        // re-read rows
        assert_eq!(read_fully!(actor, ns).unwrap(), vec![
            row!(1, table_columns, vec![
                StringValue("YARD".into()), StringValue("NYSE".into()), Float64Value(88.99),
            ]),
        ]);
    }

    #[actix::test]
    async fn test_namespaces() {
        let actor = DataframeActor::new().start();
        let ns0 = Namespace::parse("dataframe.namespaces1.stocks").unwrap();
        let ns1 = Namespace::parse("dataframe.namespaces2.stocks").unwrap();

        // create the new empty tables
        assert_eq!(1, create_table!(actor, ns0, make_quote_columns()).unwrap());
        assert_eq!(1, create_table!(actor, ns1, make_quote_columns()).unwrap());

        // append a new row to the table
        assert_eq!(1, append_row!(actor, ns0, row!(111, make_table_columns(), vec![
            StringValue("GE".into()), StringValue("NYSE".into()), Float64Value(48.88),
        ])).unwrap());

        // append a new row to the table
        assert_eq!(1, append_row!(actor, ns1, row!(112, make_table_columns(), vec![
            StringValue("IBM".into()), StringValue("NYSE".into()), Float64Value(122.88),
        ])).unwrap());

        // verify the namespaces
        let resp = get_namespaces!(actor).unwrap();
        assert_eq!(resp.len(), 2);
        assert!(resp.contains(&"dataframe.namespaces1.stocks".to_string()));
        assert!(resp.contains(&"dataframe.namespaces2.stocks".to_string()));
    }
}