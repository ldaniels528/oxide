#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Parameter class - represents a class or function parameter
////////////////////////////////////////////////////////////////////

use crate::columns::Column;
use crate::data_types::DataType;
use crate::data_types::DataType::DynamicType;

use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::Null;
use serde::{Deserialize, Serialize};

// Represents a class or function parameter
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Parameter {
    name: String,
    data_type: DataType,
    default_value: TypedValue,
}

impl Parameter {

    ////////////////////////////////////////////////////////////////////
    //  STATIC METHODS
    ////////////////////////////////////////////////////////////////////

    pub fn add(name: impl Into<String>) -> Self {
        Self::new(name, DynamicType)
    }

    pub fn are_compatible(
        parameters_a: &Vec<Parameter>,
        parameters_b: &Vec<Parameter>,
    ) -> bool {
        parameters_a.iter().zip(parameters_b.iter()).all(|(a, b)| a.is_compatible(b))
    }

    pub fn from_column(column: &Column) -> Self {
        Parameter::with_default(
            column.get_name(),
            column.get_data_type().clone(),
            column.get_default_value())
    }

    pub fn from_columns(columns: &Vec<Column>) -> Vec<Self> {
        columns.iter().map(Self::from_column).collect()
    }

    pub fn from_tuple(name: impl Into<String>, value: TypedValue) -> Self {
        Self::with_default(name.into(), value.get_type(), value)
    }

    pub fn new(name: impl Into<String>, param_type: DataType) -> Self {
        Self::with_default(name, param_type, Null)
    }

    pub fn render(parameters: &Vec<Parameter>) -> String {
        Self::render_f(parameters, |p| p.to_code())
    }

    pub fn render_f(parameters: &Vec<Parameter>, f: fn(&Parameter) -> String) -> String {
        parameters.iter().map(|p| f(p))
            .collect::<Vec<String>>()
            .join(", ")
    }

    pub fn with_default(name: impl Into<String>,
                        param_type: DataType,
                        default_value: TypedValue) -> Self {
        Parameter { name: name.into(), data_type: param_type, default_value }
    }

    ////////////////////////////////////////////////////////////////////
    //  INSTANCE METHODS
    ////////////////////////////////////////////////////////////////////

    pub fn get_name(&self) -> &str { &self.name }

    pub fn get_data_type(&self) -> DataType { self.data_type.clone() }

    pub fn get_default_value(&self) -> TypedValue { self.default_value.clone() }

    pub fn get_param_type(&self) -> Option<String> { self.data_type.to_type_declaration() }

    pub fn is_compatible(&self, parameter_b: &Parameter) -> bool {
        *self == *parameter_b || (
            self.name == parameter_b.name &&
                self.data_type.is_compatible(&parameter_b.data_type)
        )
    }

    pub fn to_code(&self) -> String {
        let mut buf = self.get_name().to_string();
        if let Some(typedef) = self.get_param_type() {
            if !typedef.trim().is_empty() {
                buf = format!("{}: {}", buf, typedef)
            }
        }
        match self.get_default_value() {
            TypedValue::Null | TypedValue::Undefined => {}
            default_value => buf = format!("{} := {}", buf, default_value.to_code())
        }
        buf
    }

    pub fn to_code_enum(&self) -> String {
        let mut buf = self.get_name().to_string();
        match self.get_default_value() {
            TypedValue::Null | TypedValue::Undefined => {}
            default_value => buf = format!("{} := {}", buf, default_value.to_code())
        }
        buf
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!(&self)
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_types::DataType::{NumberType, StringType};

    use crate::number_kind::NumberKind::F64Kind;
    use crate::typed_values::TypedValue::{Null, StringValue};

    #[test]
    fn test_from_columns() {
        let parameters = vec![
            Parameter::new("symbol", StringType(8)),
            Parameter::new("exchange", StringType(8)),
            Parameter::new("last_sale", NumberType(F64Kind)),
        ];
        let columns = vec![
            Column::new("symbol", StringType(8), Null, 9),
            Column::new("exchange", StringType(8), Null, 26),
            Column::new("last_sale", NumberType(F64Kind), Null, 43),
        ];
        assert_eq!(Parameter::from_columns(&columns), parameters);
    }

    #[test]
    fn test_to_code() {
        let param = Parameter::with_default("symbol", StringType(8), StringValue("N/A".into()));
        assert_eq!(param.to_code(), r#"symbol: String(8) := "N/A""#)
    }

    #[test]
    fn test_to_json() {
        let param = Parameter::new("symbol", StringType(8));
        assert_eq!(param.to_json().to_string(), r#"{"data_type":{"StringType":8},"default_value":"Null","name":"symbol"}"#.to_string())
    }
}