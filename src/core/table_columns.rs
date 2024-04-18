////////////////////////////////////////////////////////////////////
// table columns module
////////////////////////////////////////////////////////////////////

use std::io;

use serde::{Deserialize, Serialize};

use crate::data_types::DataType;
use crate::rows::Row;
use crate::server::ColumnJs;
use crate::typed_values::TypedValue;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TableColumn {
    name: String,
    pub(crate) data_type: DataType,
    pub(crate) default_value: TypedValue,
    pub(crate) max_physical_size: usize,
    pub(crate) offset: usize,
}

impl TableColumn {
    pub fn new(name: impl Into<String>,
               data_type: DataType,
               default_value: TypedValue,
               offset: usize) -> TableColumn {
        let max_physical_size: usize = data_type.compute_max_physical_size();
        TableColumn {
            name: name.into(),
            data_type,
            default_value,
            max_physical_size,
            offset,
        }
    }

    pub fn from_column(column: &ColumnJs, offset: usize) -> io::Result<TableColumn> {
        Ok(Self::new(column.get_name(),
                     DataType::parse(column.get_column_type())?,
                     TypedValue::wrap_value_opt(column.get_default_value())?, offset))
    }

    pub fn from_columns(columns: &Vec<ColumnJs>) -> io::Result<Vec<TableColumn>> {
        let mut offset: usize = Row::overhead();
        let mut physical_columns: Vec<TableColumn> = Vec::with_capacity(columns.len());
        for column in columns {
            let physical_column: TableColumn = Self::from_column(&column, offset)?;
            offset += physical_column.max_physical_size;
            physical_columns.push(physical_column);
        }
        Ok(physical_columns)
    }

    pub fn get_name(&self) -> &String { &self.name }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::data_types::DataType::{Float64Type, StringType};
    use crate::testdata::make_columns;
    use crate::typed_values::TypedValue::{Float64Value, Null, StringValue};

    use super::*;

    #[test]
    fn test_new() {
        let column: TableColumn = TableColumn::new("last_sale", Float64Type, Float64Value(0.142857), 0);
        assert_eq!(column.name, "last_sale");
        assert_eq!(column.data_type, Float64Type);
        assert_eq!(column.default_value, Float64Value(0.142857));
        assert_eq!(column.max_physical_size, 9);
    }

    #[test]
    fn test_from_column() {
        let column_desc = ColumnJs::new("exchange", "String(10)", Some("N/A".into()));
        let column = TableColumn::from_column(&column_desc, 0)
            .expect("Deserialization error");
        assert_eq!(column.name, "exchange");
        assert_eq!(column.data_type, StringType(10));
        assert_eq!(column.default_value, StringValue("N/A".into()));
        assert_eq!(&column.data_type.to_column_type(), column_desc.get_column_type());
        assert_eq!(column.max_physical_size, 19);
    }

    #[test]
    fn test_differences() {
        let generated: Vec<TableColumn> = TableColumn::from_columns(&make_columns()).unwrap();
        let natural: Vec<TableColumn> = vec![
            TableColumn::new("symbol", StringType(8), Null, 9),
            TableColumn::new("exchange", StringType(8), Null, 26),
            TableColumn::new("last_sale", Float64Type, Null, 43),
        ];
        assert_eq!(generated, natural);
    }
}