#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Column class
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

use crate::data_types::DataType;
use crate::descriptor::Descriptor;
use crate::errors::Errors::ArgumentsMismatched;
use crate::numbers::Numbers;
use crate::parameter::Parameter;
use crate::structures::Row;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{ErrorValue, Number};

/// Represents a column in a table
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Column {
    name: String,
    data_type: DataType,
    default_value: TypedValue,
    fixed_size: usize,
    offset: usize,
}

impl Column {
    pub fn new(name: impl Into<String>,
               data_type: DataType,
               default_value: TypedValue,
               offset: usize) -> Self {
        let fixed_size = data_type.compute_fixed_size();
        Column {
            name: name.into(),
            data_type,
            default_value,
            fixed_size,
            offset,
        }
    }

    pub fn from_descriptor(descriptor: &Descriptor, offset: usize) -> std::io::Result<Self> {
        Ok(Self::new(
            descriptor.get_name(),
            DataType::from_str(descriptor.get_param_type().unwrap_or("".to_string()).as_str())?,
            TypedValue::wrap_value_opt(&descriptor.get_default_value())?, offset))
    }

    pub fn from_descriptors(descriptors: &Vec<Descriptor>) -> std::io::Result<Vec<Self>> {
        let mut offset: usize = Row::overhead();
        let mut columns: Vec<Column> = Vec::with_capacity(descriptors.len());
        for descriptor in descriptors {
            let column = Self::from_descriptor(&descriptor, offset)?;
            offset += column.fixed_size;
            columns.push(column);
        }
        Ok(columns)
    }

    pub fn from_parameter(parameter: &Parameter, offset: usize) -> Self {
        Self::new(
            parameter.get_name(),
            parameter.get_data_type(),
            parameter.get_default_value(), offset)
    }

    pub fn from_parameters(parameters: &Vec<Parameter>) -> Vec<Self> {
        let mut offset: usize = Row::overhead();
        let mut columns: Vec<Column> = Vec::with_capacity(parameters.len());
        for parameter in parameters {
            let column = Self::from_parameter(&parameter, offset);
            offset += column.fixed_size;
            columns.push(column);
        }
        columns
    }

    pub fn get_name(&self) -> &str {
        self.name.as_str()
    }

    pub fn get_data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn get_default_value(&self) -> TypedValue {
        self.default_value.clone()
    }

    pub fn get_fixed_size(&self) -> usize {
        self.fixed_size
    }

    pub fn get_offset(&self) -> usize {
        self.offset
    }

    pub fn to_descriptor(&self) -> Descriptor {
        Descriptor::new(
            self.get_name(),
            self.get_data_type().to_type_declaration(),
            match self.get_default_value() {
                TypedValue::Null | TypedValue::Undefined => None,
                value => Some(value.to_code()),
            })
    }

    pub fn to_parameter(&self) -> Parameter {
        Parameter::with_default(
            self.get_name(),
            self.get_data_type().clone(),
            self.get_default_value().clone())
    }

    pub fn validate_compatibility(cs0: &Vec<Self>, cs1: &Vec<Self>) -> TypedValue {
        match (cs0, cs1) {
            (a, b) if a.len() != b.len() =>
                ErrorValue(ArgumentsMismatched(cs0.len(), cs1.len())),
            _ =>
                Number(Numbers::Ack)
        }
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::data_types::DataType::*;
    use crate::data_types::StorageTypes::FixedSize;
    use crate::number_kind::NumberKind::F64Kind;
    use crate::numbers::Numbers::F64Value;
    use crate::testdata::make_quote_descriptors;
    use crate::typed_values::TypedValue::*;

    use super::*;

    #[test]
    fn test_new() {
        let column: Column = Column::new("last_sale", NumberType(F64Kind), Number(F64Value(0.142857)), 0);
        assert_eq!(column.name, "last_sale");
        assert_eq!(column.data_type, NumberType(F64Kind));
        assert_eq!(column.default_value, Number(F64Value(0.142857)));
        assert_eq!(column.fixed_size, 9);
    }

    #[test]
    fn test_from_column() {
        let column_desc = Descriptor::new("exchange", Some("String(10)".into()), Some("'N/A'".into()));
        let column = Column::from_descriptor(&column_desc, 0)
            .expect("Deserialization error");
        assert_eq!(column.name, "exchange");
        assert_eq!(column.data_type, StringType(FixedSize(10)));
        assert_eq!(column.default_value, StringValue("N/A".into()));
        assert_eq!(column.data_type.to_type_declaration(), column_desc.get_param_type());
        assert_eq!(column.fixed_size, 19);
    }

    #[test]
    fn test_from_parameters() {
        let parameters = vec![
            Parameter::new("symbol", StringType(FixedSize(8))),
            Parameter::new("exchange", StringType(FixedSize(8))),
            Parameter::new("last_sale", NumberType(F64Kind)),
        ];
        let columns = vec![
            Column::new("symbol", StringType(FixedSize(8)), Null, 9),
            Column::new("exchange", StringType(FixedSize(8)), Null, 26),
            Column::new("last_sale", NumberType(F64Kind), Null, 43),
        ];
        assert_eq!(Column::from_parameters(&parameters), columns);
    }

    #[test]
    fn test_differences() {
        let generated: Vec<Column> = Column::from_descriptors(&make_quote_descriptors()).unwrap();
        let natural: Vec<Column> = vec![
            Column::new("symbol", StringType(FixedSize(8)), Null, 9),
            Column::new("exchange", StringType(FixedSize(8)), Null, 26),
            Column::new("last_sale", NumberType(F64Kind), Null, 43),
        ];
        assert_eq!(generated, natural);
    }
}