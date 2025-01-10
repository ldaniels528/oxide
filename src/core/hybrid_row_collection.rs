#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// HybridRowCollection class
////////////////////////////////////////////////////////////////////

use crate::byte_row_collection::ByteRowCollection;
use crate::columns::Column;
use crate::field::FieldMetadata;
use crate::file_row_collection::FileRowCollection;
use crate::namespaces::Namespace;
use crate::parameter::Parameter;
use crate::row_collection::RowCollection;
use crate::row_metadata::RowMetadata;
use crate::structures::Row;
use crate::typed_values::TypedValue;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::fs;
use std::fs::OpenOptions;
use std::sync::Arc;

/// Hybrid (Memory and Disk) RowCollection implementation
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct HybridRowCollection {
    dividing_line: usize,
    brc: ByteRowCollection,
    frc: FileRowCollection,
}

impl HybridRowCollection {

    ////////////////////////////////////////////////////////////////////
    // static methods
    ////////////////////////////////////////////////////////////////////

    pub fn build(path: &str, columns: &Vec<Column>, capacity: usize) -> std::io::Result<Self> {
        let file = OpenOptions::new().truncate(true).create(true).read(true).write(true)
            .open(path)?;
        Ok(Self {
            dividing_line: capacity,
            brc: ByteRowCollection::new(columns.clone(), capacity),
            frc: FileRowCollection::new(columns.clone(), Arc::new(file), path),
        })
    }

    pub fn new(ns: &Namespace, parameters: &Vec<Parameter>, capacity: usize) -> std::io::Result<Self> {
        fs::create_dir_all(ns.get_root_path())?;
        Self::build(
            ns.get_table_file_path().as_str(),
            &Column::from_parameters(parameters),
            capacity)
    }

    ////////////////////////////////////////////////////////////////////
    // instance methods
    ////////////////////////////////////////////////////////////////////

    pub fn get_internals(&self) -> (&ByteRowCollection, &FileRowCollection) {
        (&self.brc, &self.frc)
    }

    fn localize(&self, id: usize) -> usize {
        if id >= self.dividing_line {
            id - self.dividing_line
        } else {
            id
        }
    }
}

impl RowCollection for HybridRowCollection {
    fn get_columns(&self) -> &Vec<Column> { self.brc.get_columns() }

    fn get_record_size(&self) -> usize { self.brc.get_record_size() }

    fn get_rows(&self) -> Vec<Row> {
        let mut rows = vec![];
        let brc_rows = self.brc.get_rows();
        let adjust = brc_rows.len();
        rows.extend(brc_rows);
        rows.extend(self.frc.get_rows().iter().enumerate()
            .map(|(id, row)| row.with_row_id(id + adjust))
            .collect::<Vec<_>>());
        rows
    }

    fn len(&self) -> std::io::Result<usize> {
        Ok(self.brc.len()? + self.frc.len()?)
    }

    fn overwrite_field(
        &mut self,
        id: usize,
        column_id: usize,
        new_value: TypedValue,
    ) -> TypedValue {
        if id < self.dividing_line {
            self.brc.overwrite_field(id, column_id, new_value)
        } else {
            self.frc.overwrite_field(self.localize(id), column_id, new_value)
        }
    }

    fn overwrite_field_metadata(
        &mut self,
        id: usize,
        column_id: usize,
        metadata: FieldMetadata,
    ) -> TypedValue {
        if id < self.dividing_line {
            self.brc.overwrite_field_metadata(id, column_id, metadata)
        } else {
            self.frc.overwrite_field_metadata(self.localize(id), column_id, metadata)
        }
    }

    fn overwrite_row(&mut self, id: usize, row: Row) -> TypedValue {
        if id < self.dividing_line {
            self.brc.overwrite_row(id, row)
        } else {
            let row_id = self.localize(id);
            self.frc.overwrite_row(row_id, row.with_row_id(row_id))
        }
    }

    fn overwrite_row_metadata(&mut self, id: usize, metadata: RowMetadata) -> TypedValue {
        if id < self.dividing_line {
            self.brc.overwrite_row_metadata(id, metadata)
        } else {
            self.frc.overwrite_row_metadata(self.localize(id), metadata)
        }
    }

    fn read_field(&self, id: usize, column_id: usize) -> TypedValue {
        if id < self.dividing_line {
            self.brc.read_field(id, column_id)
        } else {
            self.frc.read_field(self.localize(id), column_id)
        }
    }

    fn read_field_metadata(
        &self,
        id: usize,
        column_id: usize,
    ) -> std::io::Result<FieldMetadata> {
        if id < self.dividing_line {
            self.brc.read_field_metadata(id, column_id)
        } else {
            self.frc.read_field_metadata(self.localize(id), column_id)
        }
    }

    fn read_row(&self, id: usize) -> std::io::Result<(Row, RowMetadata)> {
        if id < self.dividing_line {
            self.brc.read_row(id)
        } else {
            self.frc.read_row(self.localize(id))
                .map(|(row, rmd)| (row.with_row_id(id), rmd))
        }
    }

    fn read_row_metadata(&self, id: usize) -> std::io::Result<RowMetadata> {
        if id < self.dividing_line {
            self.brc.read_row_metadata(id)
        } else {
            self.frc.read_row_metadata(self.localize(id))
        }
    }

    fn resize(&mut self, new_size: usize) -> TypedValue {
        if new_size < self.dividing_line {
            self.brc.resize(new_size);
            self.frc.resize(0)
        } else {
            self.frc.resize(self.localize(new_size))
        }
    }
}

/// Unit tests
#[cfg(test)]
mod tests {
    use crate::hybrid_row_collection::HybridRowCollection;
    use crate::namespaces::Namespace;
    use crate::numbers::Numbers::F64Value;
    use crate::row_collection::RowCollection;
    use crate::structures::Row;
    use crate::table_renderer::TableRenderer;
    use crate::testdata::{make_quote, make_quote_parameters};
    use crate::typed_values::TypedValue::{Number, StringValue};

    #[test]
    fn test_append_rows() {
        let hrc = create_hybrid_row_collection("hrc.append_rows.stocks");
        let rc: Box<dyn RowCollection> = Box::from(hrc);
        let output = TableRenderer::from_table_with_ids(&rc).unwrap();
        assert_eq!(output, vec![
            "|------------------------------------|",
            "| id | symbol | exchange | last_sale |",
            "|------------------------------------|",
            "| 0  | AAB    | NYSE     | 22.44     |",
            "| 1  | XYZ    | NASDAQ   | 66.67     |",
            "| 2  | SSO    | NYSE     | 123.44    |",
            "| 3  | RAND   | AMEX     | 11.33     |",
            "| 4  | IBM    | NYSE     | 21.22     |",
            "| 5  | ATT    | NYSE     | 98.44     |",
            "| 6  | HOCK   | AMEX     | 0.0076    |",
            "| 7  | XIE    | NASDAQ   | 33.33     |",
            "|------------------------------------|"])
    }

    #[test]
    fn test_get_rows() {
        let mut hrc = create_hybrid_row_collection("hrc.get_rows.stocks");
        assert_eq!(hrc.get_rows(), vec![
            make_quote(0, "AAB", "NYSE", 22.44),
            make_quote(1, "XYZ", "NASDAQ", 66.67),
            make_quote(2, "SSO", "NYSE", 123.44),
            make_quote(3, "RAND", "AMEX", 11.33),
            make_quote(4, "IBM", "NYSE", 21.22),
            make_quote(5, "ATT", "NYSE", 98.44),
            make_quote(6, "HOCK", "AMEX", 0.0076),
            make_quote(7, "XIE", "NASDAQ", 33.33),
        ]);
    }

    #[test]
    fn test_get_internals() {
        let mut hrc = create_hybrid_row_collection("hrc.get_internals.stocks");
        let (brc, frc) = hrc.get_internals();
        assert_eq!(brc.get_rows(), vec![
            make_quote(0, "AAB", "NYSE", 22.44),
            make_quote(1, "XYZ", "NASDAQ", 66.67),
            make_quote(2, "SSO", "NYSE", 123.44),
            make_quote(3, "RAND", "AMEX", 11.33),
        ]);
        assert_eq!(frc.get_rows(), vec![
            make_quote(0, "IBM", "NYSE", 21.22),
            make_quote(1, "ATT", "NYSE", 98.44),
            make_quote(2, "HOCK", "AMEX", 0.0076),
            make_quote(3, "XIE", "NASDAQ", 33.33),
        ]);
    }

    #[test]
    fn test_read_one_from_memory() {
        let mut hrc = create_hybrid_row_collection("hrc.read_mem.stocks");
        assert_eq!(
            hrc.read_one(1).unwrap(),
            Some(Row::new(1, vec![
                StringValue("XYZ".into()),
                StringValue("NASDAQ".into()),
                Number(F64Value(66.67)),
            ]))
        );
    }

    #[test]
    fn test_read_one_from_disk() {
        let mut hrc = create_hybrid_row_collection("hrc.read_disk.stocks");
        assert_eq!(
            hrc.read_one(7).unwrap(),
            Some(Row::new(7, vec![
                StringValue("XIE".into()),
                StringValue("NASDAQ".into()),
                Number(F64Value(33.33)),
            ]))
        );
    }

    fn create_hybrid_row_collection(ns_path: &str) -> HybridRowCollection {
        let ns = Namespace::parse(ns_path).unwrap();
        let mut hrc = HybridRowCollection::new(&ns, &make_quote_parameters(), 4).unwrap();
        hrc.resize(0);
        hrc.append_rows(create_data_set());
        hrc
    }

    fn create_data_set() -> Vec<Row> {
        vec![
            make_quote(0, "AAB", "NYSE", 22.44),
            make_quote(1, "XYZ", "NASDAQ", 66.67),
            make_quote(2, "SSO", "NYSE", 123.44),
            make_quote(3, "RAND", "AMEX", 11.33),
            make_quote(4, "IBM", "NYSE", 21.22),
            make_quote(5, "ATT", "NYSE", 98.44),
            make_quote(6, "HOCK", "AMEX", 0.0076),
            make_quote(7, "XIE", "NASDAQ", 33.33),
        ]
    }
}