////////////////////////////////////////////////////////////////////
// table columns module
////////////////////////////////////////////////////////////////////

use std::error::Error;

use serde::Serialize;

use crate::columns::Column;
use crate::data_types::DataType;
use crate::rows::Row;
use crate::typed_values::TypedValue;

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct TableColumn {
    pub(crate) name: String,
    pub(crate) data_type: DataType,
    pub(crate) default_value: TypedValue,
    pub(crate) max_physical_size: usize,
    pub(crate) offset: usize,
}

impl TableColumn {
    pub fn from_column(column: &Column, offset: usize) -> Result<TableColumn, Box<dyn Error>> {
        Ok(Self::new(&column.name,
                     DataType::parse(&column.column_type)?,
                     TypedValue::wrap_value(&column.default_value)?, offset))
    }

    pub fn from_columns(columns: &Vec<Column>) -> Result<Vec<TableColumn>, Box<dyn Error>> {
        let mut offset: usize = Row::overhead();
        let mut physical_columns: Vec<TableColumn> = Vec::with_capacity(columns.len());
        for column in columns {
            let physical_column: TableColumn = Self::from_column(&column, offset)?;
            offset += physical_column.max_physical_size;
            physical_columns.push(physical_column);
        }
        Ok(physical_columns)
    }

    pub fn new(name: &str, data_type: DataType, default_value: TypedValue, offset: usize) -> TableColumn {
        let max_physical_size: usize = data_type.compute_max_physical_size();
        TableColumn {
            name: name.to_string(),
            data_type,
            default_value,
            max_physical_size,
            offset,
        }
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::data_types::DataType::{Float64Type, StringType};
    use crate::typed_values::TypedValue::{Float64Value, StringValue};

    use super::*;

    #[test]
    fn test_new() {
        let column: TableColumn = TableColumn::new("lastSale", Float64Type, Float64Value(0.142857), 0);
        assert_eq!(column.name, "lastSale");
        assert_eq!(column.data_type, Float64Type);
        assert_eq!(column.default_value, Float64Value(0.142857));
        assert_eq!(column.max_physical_size, 9);
    }

    #[test]
    fn test_from_column() {
        let column_desc: Column = Column::new("exchange", "String(10)", "N/A");
        let column: TableColumn = TableColumn::from_column(&column_desc, 0)
            .expect("Deserialization error");
        assert_eq!(column.name, "exchange");
        assert_eq!(column.data_type, StringType(10));
        assert_eq!(column.default_value, StringValue("N/A".to_string()));
        assert_eq!(column.data_type.to_column_type(), column_desc.column_type);
        assert_eq!(column.max_physical_size, 19);
    }
}