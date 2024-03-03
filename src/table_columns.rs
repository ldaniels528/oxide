////////////////////////////////////////////////////////////////////
// table columns
////////////////////////////////////////////////////////////////////

use std::error::Error;

use serde::Serialize;

use crate::columns::Column;
use crate::data_types::DataType;
use crate::typed_values::TypedValue;

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct TableColumn {
    pub name: String,
    pub data_type: DataType,
    pub default_value: TypedValue,
}

impl TableColumn {
    pub fn from_column(column: &Column) -> Result<TableColumn, Box<dyn Error>> {
        Ok(Self::new(&column.name,
                     DataType::parse(&column.column_type)?,
                     TypedValue::wrap_value(&column.default_value)?))
    }

    pub fn from_columns(columns: &Vec<Column>) -> Result<Vec<TableColumn>, Box<dyn Error>> {
        //Ok(columns.iter().map(|c|Self::from_column(c)?).collect())
        let mut new_columns: Vec<TableColumn> = Vec::with_capacity(columns.len());
        for column in columns {
            new_columns.push(Self::from_column(&column)?);
        }
        Ok(new_columns)
    }

    pub fn new(name: &str, data_type: DataType, default_value: TypedValue) -> TableColumn {
        TableColumn {
            name: name.to_string(),
            data_type,
            default_value,
        }
    }

    pub fn max_physical_size(&self) -> usize {
        DataType::max_physical_size(&self.data_type)
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
        let column: TableColumn = TableColumn::new("lastSale", Float64Type, Float64Value(0.142857));
        assert_eq!(column.name, "lastSale");
        assert_eq!(column.data_type, Float64Type);
        assert_eq!(column.default_value, Float64Value(0.142857));
        assert_eq!(column.max_physical_size(), 9);
    }

    #[test]
    fn test_from_column() {
        let column_desc: Column = Column::new("exchange", "String(10)", "N/A");
        let column: TableColumn = TableColumn::from_column(&column_desc)
            .expect("Deserialization error");
        assert_eq!(column.name, "exchange");
        assert_eq!(column.data_type, StringType(10));
        assert_eq!(column.default_value, StringValue("N/A".to_string()));
        assert_eq!(column.data_type.to_column_type(), column_desc.column_type);
        assert_eq!(column.max_physical_size(), 19);
    }
}