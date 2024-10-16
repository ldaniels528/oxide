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
use crate::row_collection::RowCollection;
use crate::row_metadata::RowMetadata;
use crate::rows::Row;
use crate::table_columns::Column;

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

    fn get_columns(&mut self, ns: Namespace) -> std::io::Result<&Vec<Column>> {
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

    fn read_fully(
        &mut self,
        ns: Namespace,
    ) -> std::io::Result<(Vec<Column>, Vec<Row>)> {
        let df = self.get_or_load_dataframe(ns)?;
        let mut rows = Vec::new();
        for id in 0..df.len()? {
            df.read_then_push(id, &mut rows)?
        }
        Ok((df.get_columns().clone(), rows))
    }

    fn read_range(
        &mut self,
        ns: Namespace,
        range: Range<usize>,
    ) -> std::io::Result<(Vec<Column>, Vec<Row>)> {
        let df = self.get_or_load_dataframe(ns)?;
        let mut rows = Vec::new();
        for id in range {
            df.read_then_push(id, &mut rows)?
        }
        Ok((df.get_columns().clone(), rows))
    }

    fn read_row(
        &mut self,
        ns: Namespace,
        id: usize,
    ) -> std::io::Result<(Vec<Column>, Option<Row>)> {
        let df = self.get_or_load_dataframe(ns)?;
        df.read_row(id).map(|(row, meta)| {
            let columns = df.get_columns().clone();
            if meta.is_allocated { (columns, Some(row)) } else { (columns, None) }
        })
    }

    fn read_row_metadata(&mut self, ns: Namespace, id: usize) -> std::io::Result<RowMetadata> {
        self.get_or_load_dataframe(ns)?.read_row_metadata(id)
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
            IORequest::ReadRowMetadata { ns, id } =>
                handle_result(self.read_row_metadata(ns, id)),
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
    ReadRowMetadata { ns: Namespace, id: usize },
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
        $actor.send(crate::dataframe_actor::IORequest::AppendRow {
            ns: $ns.to_owned(),
            row: $row
        }).await
            .map(|s|serde_json::from_str::<usize>(&s).unwrap())
            .map_err(|e|crate::cnv_error!(e))
    }
}

#[macro_export]
macro_rules! create_table {
    ($actor:expr, $ns:expr, $columns:expr) => {
        $actor.send(crate::dataframe_actor::IORequest::CreateTable {
            ns: $ns.to_owned(),
            cfg: DataFrameConfig::new($columns, Vec::new(), Vec::new())
        }).await
            .map(|s|serde_json::from_str::<usize>(&s).unwrap())
            .map_err(|e|crate::cnv_error!(e))
    }
}

#[macro_export]
macro_rules! create_table_from_config {
    ($actor:expr, $ns:expr, $cfg:expr) => {
        $actor.send(crate::dataframe_actor::IORequest::CreateTable {
            ns: $ns.to_owned(),
            cfg: $cfg
        }).await
            .map(|s|serde_json::from_str::<usize>(&s).unwrap())
            .map_err(|e|crate::cnv_error!(e))
    }
}

#[macro_export]
macro_rules! delete_row {
    ($actor:expr, $ns:expr, $id:expr) => {
        $actor.send(crate::dataframe_actor::IORequest::DeleteRow {
            ns: $ns.to_owned(),
            id: $id
        }).await
            .map(|s|serde_json::from_str::<usize>(&s).unwrap())
            .map_err(|e|crate::cnv_error!(e))
    }
}

#[macro_export]
macro_rules! get_columns {
    ($actor:expr, $ns:expr) => {
        $actor.send(crate::dataframe_actor::IORequest::GetColumns {
            ns: $ns.to_owned()
        }).await
            .map(|s|serde_json::from_str::<Vec<Column>>(&s).unwrap())
            .map_err(|e|crate::cnv_error!(e))
    }
}

#[macro_export]
macro_rules! overwrite_row {
    ($actor:expr, $ns:expr, $row:expr) => {
        $actor.send(crate::dataframe_actor::IORequest::OverwriteRow {
            ns: $ns.to_owned(),
            row: $row
        }).await
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
        $actor.send(crate::dataframe_actor::IORequest::ReadFully {
            ns: $ns.to_owned()
        }).await
            .map(|s|serde_json::from_str::<(Vec<Column>, Vec<Row>)>(&s).unwrap())
            .map_err(|e|crate::cnv_error!(e))
    }
}

#[macro_export]
macro_rules! read_range {
    ($actor:expr, $ns:expr, $range:expr) => {
        $actor.send(crate::dataframe_actor::IORequest::ReadRange {
            ns: $ns.to_owned(),
            range: $range
        }).await
            .map(|s|serde_json::from_str::<(Vec<Column>, Vec<Row>)>(&s).unwrap())
            .map_err(|e|crate::cnv_error!(e))
    }
}

#[macro_export]
macro_rules! read_row {
    ($actor:expr, $ns:expr, $id:expr) => {
        $actor.send(crate::dataframe_actor::IORequest::ReadRow {
            ns: $ns.to_owned(),
            id: $id
        }).await
            .map(|s|serde_json::from_str::<(Vec<Column>, Option<Row>)>(&s).unwrap())
            .map_err(|e|crate::cnv_error!(e))
    }
}

#[macro_export]
macro_rules! read_row_metadata {
    ($actor:expr, $ns:expr, $id:expr) => {
        $actor.send(crate::dataframe_actor::IORequest::ReadRowMetadata {
            ns: $ns.to_owned(),
            id: $id
        }).await
            .map(|s|serde_json::from_str::<RowMetadata>(&s).unwrap())
            .map_err(|e|crate::cnv_error!(e))
    }
}

#[macro_export]
macro_rules! update_row {
    ($actor:expr, $ns:expr, $row:expr) => {
        $actor.send(crate::dataframe_actor::IORequest::UpdateRow {
            ns: $ns.to_owned(),
            row: $row
        }).await
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

    use crate::data_types::DataType::*;
    use crate::number_kind::NumberKind::F64Kind;
    use crate::numbers::NumberValue::*;
    use crate::testdata::{make_quote_parameters, make_quote_columns};
    use crate::typed_values::TypedValue::*;

    use super::*;

    #[actix::test]
    async fn test_get_columns() {
        let actor = DataframeActor::new().start();
        let ns = Namespace::parse("dataframe.columns.stocks").unwrap();
        assert_eq!(1, create_table!(actor, ns, make_quote_parameters()).unwrap());
        assert_eq!(get_columns!(actor, ns).unwrap(), vec![
            Column::new("symbol", StringType(8), Null, 9),
            Column::new("exchange", StringType(8), Null, 26),
            Column::new("last_sale", NumberType(F64Kind), Null, 43),
        ]);
    }

    #[actix::test]
    async fn test_append_then_read() {
        let actor = DataframeActor::new().start();
        let ns = Namespace::parse("actors.append_then_read.stocks").unwrap();
        let table_columns = make_quote_columns();

        // create the new empty table
        assert_eq!(1, create_table!(actor, ns, make_quote_parameters()).unwrap());

        // append a new row to the table
        assert_eq!(0, append_row!(actor, ns, Row::new(0, vec![
            StringValue("JUNO".into()), StringValue("AMEX".into()), Number(F64Value(11.88)),
        ])).unwrap());

        // read the previously created row
        let (columns, row_maybe) = read_row!(actor, ns, 0).unwrap();
        assert_eq!(row_maybe.unwrap(), Row::new(0, vec![
            StringValue("JUNO".into()), StringValue("AMEX".into()), Number(F64Value(11.88)),
        ]));
    }

    #[actix::test]
    async fn test_dataframe_lifecycle() {
        let actor = DataframeActor::new().start();
        let ns = Namespace::parse("dataframe.actor_crud.stocks").unwrap();
        let table_columns = make_quote_columns();

        // create the new empty table
        assert_eq!(1, create_table!(actor, ns, make_quote_parameters()).unwrap());

        // append a new row to the table
        assert_eq!(0, append_row!(actor, ns, Row::new(111, vec![
            StringValue("JUNO".into()), StringValue("AMEX".into()), Number(F64Value(22.88)),
        ])).unwrap());

        // read the previously created row
        let (columns, row_maybe) = read_row!(actor, ns, 0).unwrap();
        assert_eq!(row_maybe.unwrap(), Row::new(0, vec![
            StringValue("JUNO".into()), StringValue("AMEX".into()), Number(F64Value(22.88)),
        ]));

        // overwrite a new row over offset 1
        assert_eq!(1, overwrite_row!(actor, ns, Row::new(1, vec![
            StringValue("YARD".into()), StringValue("NYSE".into()), Number(F64Value(88.22)),
        ])).unwrap());

        // read rows
        let (columns, rows) = read_range!(actor, ns, 0..2).unwrap();
        assert_eq!(rows, vec![
            Row::new(0, vec![
                StringValue("JUNO".into()), StringValue("AMEX".into()), Number(F64Value(22.88)),
            ]),
            Row::new(1, vec![
                StringValue("YARD".into()), StringValue("NYSE".into()), Number(F64Value(88.22)),
            ]),
        ]);

        // update the row at offset 1
        assert_eq!(1, update_row!(actor, ns, Row::new(1, vec![
            Undefined, Undefined, Number(F64Value(88.99)),
        ])).unwrap());

        // re-read rows
        let (columns, rows) = read_fully!(actor, ns).unwrap();
        assert_eq!(rows, vec![
            Row::new(0, vec![
                StringValue("JUNO".into()), StringValue("AMEX".into()), Number(F64Value(22.88)),
            ]),
            Row::new(1, vec![
                StringValue("YARD".into()), StringValue("NYSE".into()), Number(F64Value(88.99)),
            ]),
        ]);

        // delete row at offset 0
        assert_eq!(1, delete_row!(actor, ns, 0).unwrap());

        // re-read rows
        let (columns, rows) = read_fully!(actor, ns).unwrap();
        assert_eq!(rows, vec![
            Row::new(1, vec![
                StringValue("YARD".into()), StringValue("NYSE".into()), Number(F64Value(88.99)),
            ]),
        ]);
    }

    #[actix::test]
    async fn test_namespaces() {
        let actor = DataframeActor::new().start();
        let ns0 = Namespace::parse("dataframe.namespaces1.stocks").unwrap();
        let ns1 = Namespace::parse("dataframe.namespaces2.stocks").unwrap();

        // create the new empty tables
        assert_eq!(1, create_table!(actor, ns0, make_quote_parameters()).unwrap());
        assert_eq!(1, create_table!(actor, ns1, make_quote_parameters()).unwrap());

        // append a new row to the table
        assert_eq!(0, append_row!(actor, ns0, Row::new(111, vec![
            StringValue("GE".into()), StringValue("NYSE".into()), Number(F64Value(48.88)),
        ])).unwrap());

        // append a new row to the table
        assert_eq!(0, append_row!(actor, ns1, Row::new(112, vec![
            StringValue("IBM".into()), StringValue("NYSE".into()), Number(F64Value(122.88)),
        ])).unwrap());

        // verify the namespaces
        let resp = get_namespaces!(actor).unwrap();
        assert_eq!(resp.len(), 2);
        assert!(resp.contains(&"dataframe.namespaces1.stocks".to_string()));
        assert!(resp.contains(&"dataframe.namespaces2.stocks".to_string()));
    }
}