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
use crate::row_metadata::RowMetadata;
use crate::rows::Row;
use crate::server::ColumnJs;
use crate::typed_values::TypedValue::{Float64Value, StringValue};

// define the Dataframe I/O actor
pub struct DataframeIO {
    resources: HashMap<String, DataFrame>,
}

impl DataframeIO {
    /// default constructor
    pub fn new() -> Self {
        DataframeIO {
            resources: HashMap::new()
        }
    }

    fn append_row(&mut self, ns: Namespace, row: Row) -> std::io::Result<usize> {
        self.get_or_load_dataframe(ns)?.append(&row)
    }

    fn create_table(&mut self, ns: Namespace, columns: Vec<ColumnJs>) -> std::io::Result<usize> {
        self.get_or_create_dataframe(ns, columns)?.resize(0)
    }

    fn delete_row(&mut self, ns: Namespace, id: usize) -> std::io::Result<usize> {
        self.get_or_load_dataframe(ns)?.delete(id)
    }

    fn get_namespaces(&mut self) -> std::io::Result<Vec<String>> { // .sort_by(|a, b| b.cmp(a))
        Ok(self.resources.iter().map(|(s, _)| s.to_string()).collect::<Vec<String>>())
    }

    fn get_or_create_dataframe(&mut self, ns: Namespace, columns: Vec<ColumnJs>) -> std::io::Result<&mut DataFrame> {
        match self.resources.entry(ns.id()) {
            Entry::Occupied(v) => Ok(v.into_mut()),
            Entry::Vacant(x) =>
                Ok(x.insert(DataFrame::create(ns, DataFrameConfig::new(columns, vec![], vec![]))?))
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
        for id in 0..df.size()? {
            df.read_and_push(id, &mut rows)?
        }
        Ok(rows)
    }

    fn read_range(&mut self, ns: Namespace, range: Range<usize>) -> std::io::Result<Vec<Row>> {
        let df = self.get_or_load_dataframe(ns)?;
        let mut rows = vec![];
        for id in range {
            df.read_and_push(id, &mut rows)?
        }
        Ok(rows)
    }

    fn read_row(&mut self, ns: Namespace, id: usize) -> std::io::Result<(Row, RowMetadata)> {
        self.get_or_load_dataframe(ns)?.read_row(id)
    }

    fn update_row(&mut self, ns: Namespace, row: Row) -> std::io::Result<usize> {
        self.get_or_load_dataframe(ns)?.update(row)
    }
}

impl Actor for DataframeIO {
    type Context = Context<Self>;
}

impl Handler<IORequest> for DataframeIO {
    type Result = String;

    fn handle(&mut self, msg: IORequest, _: &mut Self::Context) -> Self::Result {
        match msg {
            IORequest::AppendRow { ns, row } =>
                handle_result(self.append_row(ns, row)),
            IORequest::CreateTable { ns, columns } =>
                handle_result(self.create_table(ns, columns)),
            IORequest::DeleteRow { ns, id } =>
                handle_result(self.delete_row(ns, id)),
            IORequest::GetNamespaces =>
                handle_result(self.get_namespaces()),
            IORequest::ReadFully { ns } =>
                handle_result(self.read_fully(ns)),
            IORequest::ReadRange { ns, range } =>
                handle_result(self.read_range(ns, range)),
            IORequest::ReadRow { ns, id } =>
                handle_result(self.read_row(ns, id).map(|x| x.0)),
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
    CreateTable { ns: Namespace, columns: Vec<ColumnJs> },
    DeleteRow { ns: Namespace, id: usize },
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
    ($df_io:expr, $ns:expr, $row:expr) => {
        $df_io.send(IORequest::AppendRow { ns: $ns.clone(), row: $row })
    }
}

#[macro_export]
macro_rules! create_table {
    ($df_io:expr, $ns:expr, $columns:expr) => {
        $df_io.send(IORequest::CreateTable { ns: $ns.clone(), columns: $columns })
    }
}

#[macro_export]
macro_rules! delete_row {
    ($df_io:expr, $ns:expr, $id:expr) => {
        $df_io.send(IORequest::DeleteRow { ns: $ns.clone(), id: $id })
    }
}

#[macro_export]
macro_rules! overwrite_row {
    ($df_io:expr, $ns:expr, $row:expr) => {
        $df_io.send(IORequest::OverwriteRow { ns: $ns.clone(), row: $row })
    }
}

#[macro_export]
macro_rules! get_namespaces {
    ($df_io:expr) => {
        $df_io.send(IORequest::GetNamespaces).await
            .map(|s|serde_json::from_str::<Vec<String>>(&s).unwrap())
            .map_err(|e|crate::cnv_error!(e))
    }
}

#[macro_export]
macro_rules! read_fully {
    ($df_io:expr, $ns:expr) => {
        $df_io.send(IORequest::ReadFully { ns: $ns.clone() }).await
            .map(|s|serde_json::from_str::<Vec<Row>>(&s).unwrap())
            .map_err(|e|crate::cnv_error!(e))
    }
}

#[macro_export]
macro_rules! read_range {
    ($df_io:expr, $ns:expr, $range:expr) => {
        $df_io.send(IORequest::ReadRange { ns: $ns.clone(), range: $range }).await
            .map(|s|serde_json::from_str::<Vec<Row>>(&s).unwrap())
            .map_err(|e|crate::cnv_error!(e))
    }
}

#[macro_export]
macro_rules! read_row {
    ($df_io:expr, $ns:expr, $id:expr) => {
        $df_io.send(IORequest::ReadRow { ns: $ns.clone(), id: $id }).await
            .map(|s|serde_json::from_str::<Row>(&s).unwrap())
            .map_err(|e|crate::cnv_error!(e))
    }
}

#[macro_export]
macro_rules! update_row {
    ($df_io:expr, $ns:expr, $row:expr) => {
        $df_io.send(IORequest::UpdateRow { ns: $ns.clone(), row: $row })
    }
}

////////////////////////////////////////////////////////////////////
// Unit tests
////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use std::vec;

    use actix::prelude::*;

    use crate::fields::Field;
    use crate::row;
    use crate::testdata::{make_columns, make_table_columns};
    use crate::typed_values::TypedValue::{Float64Value, StringValue, Undefined};

    use super::*;

    #[actix::test]
    async fn test_actor_crud() {
        let df_io = DataframeIO::new().start();
        let ns = Namespace::new("dataframe", "actor_crud", "stocks");
        let table_columns = make_table_columns();

        // create the new empty table
        let resp = create_table!(df_io, ns, make_columns()).await.unwrap();
        assert_eq!(resp, "1");

        // append a new row to the table
        let resp = append_row!(df_io, ns, row!(111, table_columns, vec![
            StringValue("JUNO".into()), StringValue("AMEX".into()), Float64Value(22.88),
        ])).await.unwrap();
        assert_eq!(resp, "1");

        // read the previous row
        let row = read_row!(df_io, ns, 0).unwrap();
        assert_eq!(row, row!(0, table_columns, vec![
            StringValue("JUNO".into()), StringValue("AMEX".into()), Float64Value(22.88),
        ]));

        // overwrite a new row over offset 1
        let resp = overwrite_row!(df_io, ns, row!(1, table_columns, vec![
            StringValue("YARD".into()), StringValue("NYSE".into()), Float64Value(88.22),
        ])).await.unwrap();
        assert_eq!(resp, "1");

        // read rows
        let rows = read_range!(df_io, ns, 0..2).unwrap();
        assert_eq!(rows, vec![
            row!(0, table_columns, vec![
                StringValue("JUNO".into()), StringValue("AMEX".into()), Float64Value(22.88),
            ]),
            row!(1, table_columns, vec![
                StringValue("YARD".into()), StringValue("NYSE".into()), Float64Value(88.22),
            ]),
        ]);

        // update the row at offset 1
        let resp = update_row!(df_io, ns, row!(1, table_columns, vec![
            Undefined, Undefined, Float64Value(88.99),
        ])).await.unwrap();
        assert_eq!(resp, "1");

        // re-read rows
        let rows = read_fully!(df_io, ns).unwrap();
        assert_eq!(rows, vec![
            row!(0, table_columns, vec![
                StringValue("JUNO".into()), StringValue("AMEX".into()), Float64Value(22.88),
            ]),
            row!(1, table_columns, vec![
                StringValue("YARD".into()), StringValue("NYSE".into()), Float64Value(88.99),
            ]),
        ]);

        // delete row at offset 0
        let resp = delete_row!(df_io, ns, 0).await.unwrap();
        assert_eq!(resp, "1");

        // re-read rows
        let rows = read_fully!(df_io, ns).unwrap();
        assert_eq!(rows, vec![
            row!(1, table_columns, vec![
                StringValue("YARD".into()), StringValue("NYSE".into()), Float64Value(88.99),
            ]),
        ]);
    }

    #[actix::test]
    async fn test_namespaces() {
        let df_io = DataframeIO::new().start();
        let ns0 = Namespace::new("dataframe", "namespaces1", "stocks");
        let ns1 = Namespace::new("dataframe", "namespaces2", "stocks");

        // create the new empty tables
        let resp = create_table!(df_io, ns0, make_columns()).await.unwrap();
        assert_eq!(resp, "1");
        let resp = create_table!(df_io, ns1, make_columns()).await.unwrap();
        assert_eq!(resp, "1");

        // append a new row to the table
        let resp = append_row!(df_io, ns0, row!(111, make_table_columns(), vec![
            StringValue("GE".into()), StringValue("NYSE".into()), Float64Value(48.88),
        ])).await.unwrap();
        assert_eq!(resp, "1");

        // append a new row to the table
        let resp = append_row!(df_io, ns1, row!(112, make_table_columns(), vec![
            StringValue("IBM".into()), StringValue("NYSE".into()), Float64Value(122.88),
        ])).await.unwrap();
        assert_eq!(resp, "1");

        // verify the namespaces
        let resp = get_namespaces!(df_io).unwrap();
        assert_eq!(resp.len(), 2);
        assert!(resp.contains(&"dataframe.namespaces1.stocks".to_string()));
        assert!(resp.contains(&"dataframe.namespaces2.stocks".to_string()));
    }
}