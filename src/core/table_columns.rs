////////////////////////////////////////////////////////////////////
// table columns module
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

use crate::data_types::DataType;
use crate::rows::Row;
use crate::server::ColumnJs;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::ErrorValue;

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

    pub fn from_column(column: &ColumnJs, offset: usize) -> std::io::Result<TableColumn> {
        Ok(Self::new(
            column.get_name(),
            DataType::compile(column.get_column_type())?,
            TypedValue::wrap_value_opt(&column.get_default_value())?, offset))
    }

    pub fn from_columns(columns: &Vec<ColumnJs>) -> std::io::Result<Vec<TableColumn>> {
        let mut offset: usize = Row::overhead();
        let mut physical_columns: Vec<TableColumn> = Vec::with_capacity(columns.len());
        for column in columns {
            let physical_column = Self::from_column(&column, offset)?;
            offset += physical_column.max_physical_size;
            physical_columns.push(physical_column);
        }
        Ok(physical_columns)
    }

    pub fn validate_compatibility(cs0: &Vec<TableColumn>, cs1: &Vec<TableColumn>) -> TypedValue {
        match (cs0, cs1) {
            (a, b) if a.len() != b.len() =>
                ErrorValue(format!("Mismatched number of arguments: {} vs. {}", cs0.len(), cs1.len())),
            _ =>
                TypedValue::Ack
        }
    }

    pub fn get_name(&self) -> &str { self.name.as_str() }

    pub fn get_default_value(&self) -> TypedValue { self.default_value.to_owned() }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::data_types::DataType::*;
    use crate::number_kind::NumberKind::F64Kind;
    use crate::numbers::NumberValue::F64Value;
    use crate::testdata::make_quote_columns;
    use crate::typed_values::TypedValue::*;

    use super::*;

    #[test]
    fn test_new() {
        let column: TableColumn = TableColumn::new("last_sale", NumberType(F64Kind), Number(F64Value(0.142857)), 0);
        assert_eq!(column.name, "last_sale");
        assert_eq!(column.data_type, NumberType(F64Kind));
        assert_eq!(column.default_value, Number(F64Value(0.142857)));
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
        let generated: Vec<TableColumn> = TableColumn::from_columns(&make_quote_columns()).unwrap();
        let natural: Vec<TableColumn> = vec![
            TableColumn::new("symbol", StringType(8), Null, 9),
            TableColumn::new("exchange", StringType(8), Null, 26),
            TableColumn::new("last_sale", NumberType(F64Kind), Null, 43),
        ];
        assert_eq!(generated, natural);
    }
}