////////////////////////////////////////////////////////////////////
// Column class
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

use crate::data_types::DataType;
use crate::errors::Errors::ArgumentsMismatched;
use crate::outcomes::Outcomes;
use crate::parameter::Parameter;
use crate::rows::Row;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{ErrorValue, Outcome};

/// Represents a column in a table
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Column {
    name: String,
    data_type: DataType,
    default_value: TypedValue,
    max_physical_size: usize,
    offset: usize,
}

impl Column {
    pub fn new(name: impl Into<String>,
               data_type: DataType,
               default_value: TypedValue,
               offset: usize) -> Column {
        let max_physical_size: usize = data_type.compute_max_physical_size();
        Column {
            name: name.into(),
            data_type,
            default_value,
            max_physical_size,
            offset,
        }
    }

    pub fn from_parameter(parameter: &Parameter, offset: usize) -> std::io::Result<Column> {
        Ok(Self::new(
            parameter.get_name(),
            DataType::compile(parameter.get_param_type().clone().unwrap_or("".to_string()).as_str())?,
            TypedValue::wrap_value_opt(&parameter.get_default_value())?, offset))
    }

    pub fn from_parameters(parameters: &Vec<Parameter>) -> std::io::Result<Vec<Column>> {
        let mut offset: usize = Row::overhead();
        let mut physical_columns: Vec<Column> = Vec::with_capacity(parameters.len());
        for column in parameters {
            let physical_column = Self::from_parameter(&column, offset)?;
            offset += physical_column.max_physical_size;
            physical_columns.push(physical_column);
        }
        Ok(physical_columns)
    }

    pub fn validate_compatibility(cs0: &Vec<Column>, cs1: &Vec<Column>) -> TypedValue {
        match (cs0, cs1) {
            (a, b) if a.len() != b.len() =>
                ErrorValue(ArgumentsMismatched(cs0.len(), cs1.len())),
            _ =>
                Outcome(Outcomes::Ack)
        }
    }

    pub fn get_name(&self) -> &str {
        self.name.as_str()
    }

    pub fn get_data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn get_default_value(&self) -> TypedValue {
        self.default_value.to_owned()
    }

    pub fn get_max_physical_size(&self) -> usize {
        self.max_physical_size
    }

    pub fn get_offset(&self) -> usize {
        self.offset
    }

    pub fn to_parameter(&self) -> Parameter {
        Parameter::new(
            self.get_name(),
            self.get_data_type().to_type_declaration(),
            match self.get_default_value() {
                TypedValue::Null | TypedValue::Undefined => None,
                value => Some(value.to_code()),
            })
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::data_types::DataType::*;
    use crate::data_types::SizeTypes;
    use crate::number_kind::NumberKind::F64Kind;
    use crate::numbers::NumberValue::F64Value;
    use crate::testdata::make_quote_parameters;
    use crate::typed_values::TypedValue::*;

    use super::*;

    #[test]
    fn test_new() {
        let column: Column = Column::new("last_sale", NumberType(F64Kind), Number(F64Value(0.142857)), 0);
        assert_eq!(column.name, "last_sale");
        assert_eq!(column.data_type, NumberType(F64Kind));
        assert_eq!(column.default_value, Number(F64Value(0.142857)));
        assert_eq!(column.max_physical_size, 9);
    }

    #[test]
    fn test_from_column() {
        let column_desc = Parameter::new("exchange", Some("String(10)".into()), Some("N/A".into()));
        let column = Column::from_parameter(&column_desc, 0)
            .expect("Deserialization error");
        assert_eq!(column.name, "exchange");
        assert_eq!(column.data_type, StringType(SizeTypes::Fixed(10)));
        assert_eq!(column.default_value, StringValue("N/A".into()));
        assert_eq!(&column.data_type.to_type_declaration(), column_desc.get_param_type());
        assert_eq!(column.max_physical_size, 19);
    }

    #[test]
    fn test_differences() {
        let generated: Vec<Column> = Column::from_parameters(&make_quote_parameters()).unwrap();
        let natural: Vec<Column> = vec![
            Column::new("symbol", StringType(SizeTypes::Fixed(8)), Null, 9),
            Column::new("exchange", StringType(SizeTypes::Fixed(8)), Null, 26),
            Column::new("last_sale", NumberType(F64Kind), Null, 43),
        ];
        assert_eq!(generated, natural);
    }
}