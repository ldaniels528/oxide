////////////////////////////////////////////////////////////////////
// dataframe actor module
////////////////////////////////////////////////////////////////////

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use actix::prelude::*;
use log::error;
use serde::{Deserialize, Serialize};

use crate::dataframe_config::DataFrameConfig;
use crate::dataframes::DataFrame;
use crate::namespaces::Namespace;
use crate::rows::Row;
use crate::server::ColumnJs;
use crate::table_columns::TableColumn;

// define the Dataframe I/O actor
pub struct DataframeIO {
    resources: HashMap<String, DataFrame>,
}

impl DataframeIO {
    pub fn new() -> Self {
        DataframeIO {
            resources: HashMap::new()
        }
    }

    fn append_row(&mut self, ns: Namespace, row: Row) -> usize {
        let df = self.resources.entry(ns.id())
            .or_insert_with(|| DataFrame::load(ns).unwrap());
        df.append(&row).unwrap_or_else(|err| {
            error!("{}", err);
            0
        })
    }

    fn create_table(&mut self, ns: Namespace, columns: Vec<ColumnJs>) -> bool {
        match DataFrame::create(ns.clone(), DataFrameConfig::new(columns, vec![], vec![])) {
            Ok(mut df) => {
                df.resize(0);
                self.resources.insert(ns.id(), df);
                true
            }
            Err(err) => {
                error!("{}", err);
                false
            }
        }
    }

    fn overwrite_row(&mut self, ns: Namespace, row: Row) -> usize {
        let df = self.resources.entry(ns.id())
            .or_insert_with(|| DataFrame::load(ns).unwrap());
        df.overwrite(row).unwrap_or_else(|err| {
            error!("{}", err);
            0
        })
    }

    fn read_range(&mut self, ns: Namespace, range: Range<usize>) -> Vec<Row> {
        let df = self.resources.entry(ns.id())
            .or_insert_with(|| DataFrame::load(ns).unwrap());
        let mut rows = vec![];
        for id in range {
            rows.push(match df.read_row(id) {
                Ok((row, _meta)) => row,
                Err(err) => {
                    error!("{}", err);
                    Row::empty(&df.columns)
                }
            })
        }
        rows
    }

    fn read_row(&mut self, ns: Namespace, id: usize) -> Row {
        let df = self.resources.entry(ns.id())
            .or_insert_with(|| DataFrame::load(ns).unwrap());
        let row = match df.read_row(id) {
            Ok((row, _meta)) => row,
            Err(err) => {
                error!("{}", err);
                Row::empty(&df.columns)
            }
        };
        row.clone()
    }

    fn update_row(&mut self, ns: Namespace, row: Row) -> usize {
        let df = self.resources.entry(ns.id())
            .or_insert_with(|| DataFrame::load(ns).unwrap());
        df.update(row).unwrap_or_else(|err| {
            error!("{}", err);
            0
        })
    }
}

impl Actor for DataframeIO {
    type Context = Context<Self>;
}

impl Handler<IOMessage> for DataframeIO {
    type Result = String;

    fn handle(&mut self, msg: IOMessage, _: &mut Self::Context) -> Self::Result {
        match msg {
            IOMessage::CreateTable { ns, columns } =>
                serde_json::json!(self.create_table(ns, columns)).to_string(),
            IOMessage::AppendRow { ns, row } =>
                serde_json::json!(self.append_row(ns, row)).to_string(),
            IOMessage::ReadRange { ns, from, to } =>
                serde_json::json!(self.read_range(ns, from..to)).to_string(),
            IOMessage::ReadRow { ns, id } =>
                serde_json::json!(self.read_row(ns, id)).to_string(),
            IOMessage::OverwriteRow { ns, row } =>
                serde_json::json!(self.overwrite_row(ns, row)).to_string(),
            IOMessage::UpdateRow { ns, row } =>
                serde_json::json!(self.update_row(ns, row)).to_string(),
        }
    }
}

#[derive(Clone, Debug, Message, Serialize, Deserialize)]
#[rtype(result = "String")]
pub enum IOMessage {
    CreateTable { ns: Namespace, columns: Vec<ColumnJs> },
    AppendRow { ns: Namespace, row: Row },
    ReadRow { ns: Namespace, id: usize },
    ReadRange { ns: Namespace, from: usize, to: usize },
    OverwriteRow { ns: Namespace, row: Row },
    UpdateRow { ns: Namespace, row: Row },
}

impl IOMessage {
    pub fn append(ns: Namespace, row: Row) -> IOMessage {
        IOMessage::AppendRow { ns, row }
    }

    pub fn create_table(ns: Namespace, columns: Vec<ColumnJs>) -> IOMessage {
        IOMessage::CreateTable { ns, columns }
    }

    pub fn overwrite(ns: Namespace, row: Row) -> IOMessage {
        IOMessage::OverwriteRow { ns, row }
    }

    pub fn read(ns: Namespace, id: usize) -> IOMessage {
        IOMessage::ReadRow { ns, id }
    }

    pub fn read_range(ns: Namespace, from: usize, to: usize) -> IOMessage {
        IOMessage::ReadRange { ns, from, to }
    }

    pub fn update(ns: Namespace, row: Row) -> IOMessage {
        IOMessage::UpdateRow { ns, row }
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use std::vec;

    use actix::prelude::*;

    use crate::fields::Field;
    use crate::testdata::{make_columns, make_table_columns};
    use crate::typed_values::TypedValue::{Float64Value, StringValue, Undefined};

    use super::*;

    #[actix::test]
    async fn test_crud() {
        let df_io = DataframeIO::new().start();
        let ns = Namespace::new("dataframe", "crud", "quotes");
        let table_columns = make_table_columns();

        // create the new empty table
        let resp = df_io.send(IOMessage::create_table(ns.clone(), make_columns())).await.unwrap();
        assert_eq!(resp, "true");

        // append a new row to the table
        let row = Row::new(111, table_columns.clone(), vec![
            Field::new(StringValue("JUNO".into())),
            Field::new(StringValue("AMEX".into())),
            Field::new(Float64Value(22.88)),
        ]);
        let resp = df_io.send(IOMessage::append(ns.clone(), row)).await.unwrap();
        assert_eq!(resp, "1");

        // read the previous row
        let resp = df_io.send(IOMessage::read(ns.clone(), 0)).await.unwrap();
        let row = serde_json::from_str::<Row>(&resp).unwrap();
        assert_eq!(row, Row::new(0, table_columns.clone(), vec![
            Field::new(StringValue("JUNO".into())),
            Field::new(StringValue("AMEX".into())),
            Field::new(Float64Value(22.88)),
        ]));

        // overwrite a new row over offset 1
        let row = Row::new(1, table_columns.clone(), vec![
            Field::new(StringValue("YARD".into())),
            Field::new(StringValue("NYSE".into())),
            Field::new(Float64Value(88.22)),
        ]);
        let resp = df_io.send(IOMessage::overwrite(ns.clone(), row)).await.unwrap();
        assert_eq!(resp, "1");

        // read rows
        let resp = df_io.send(IOMessage::read_range(ns.clone(), 0, 2)).await.unwrap();
        let rows = serde_json::from_str::<Vec<Row>>(&resp).unwrap();
        assert_eq!(rows, vec![
            Row::new(0, table_columns.clone(), vec![
                Field::new(StringValue("JUNO".into())),
                Field::new(StringValue("AMEX".into())),
                Field::new(Float64Value(22.88)),
            ]),
            Row::new(1, table_columns.clone(), vec![
                Field::new(StringValue("YARD".into())),
                Field::new(StringValue("NYSE".into())),
                Field::new(Float64Value(88.22)),
            ]),
        ]);

        // update the row at offset 1
        let resp = df_io.send(IOMessage::update(ns.clone(), Row::new(1, table_columns.clone(), vec![
            Field::new(Undefined),
            Field::new(Undefined),
            Field::new(Float64Value(88.99)),
        ]))).await.unwrap();
        assert_eq!(resp, "1");

        // re-read rows
        let resp = df_io.send(IOMessage::read_range(ns.clone(), 0, 2)).await.unwrap();
        let rows = serde_json::from_str::<Vec<Row>>(&resp).unwrap();
        assert_eq!(rows, vec![
            Row::new(0, table_columns.clone(), vec![
                Field::new(StringValue("JUNO".into())),
                Field::new(StringValue("AMEX".into())),
                Field::new(Float64Value(22.88)),
            ]),
            Row::new(1, table_columns.clone(), vec![
                Field::new(StringValue("YARD".into())),
                Field::new(StringValue("NYSE".into())),
                Field::new(Float64Value(88.99)),
            ]),
        ]);
    }
}