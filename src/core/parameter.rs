#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Parameter class - represents a class or function parameter
////////////////////////////////////////////////////////////////////

use crate::columns::Column;
use crate::data_types::DataType;
use crate::data_types::DataType::RuntimeResolvedType;

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
        Self::new(name, RuntimeResolvedType)
    }

    pub fn are_compatible(
        parameters_a: &Vec<Parameter>,
        parameters_b: &Vec<Parameter>,
    ) -> bool {
        parameters_a.iter().zip(parameters_b.iter()).all(|(a, b)| a.is_compatible(b))
    }

    pub fn from_column(column: &Column) -> Self {
        Parameter::new_with_default(
            column.get_name(),
            column.get_data_type().clone(),
            column.get_default_value())
    }

    pub fn from_columns(columns: &Vec<Column>) -> Vec<Self> {
        columns.iter().map(Self::from_column).collect()
    }

    pub fn from_tuple(name: impl Into<String>, value: TypedValue) -> Self {
        Self::new_with_default(name.into(), value.get_type(), value)
    }

    pub fn merge_parameters(
        mut current_params: Vec<Parameter>,
        incoming_params: Vec<Parameter>,
    ) -> Vec<Parameter> {
        for incoming_param in incoming_params {
            let name = incoming_param.get_name();
            match current_params.iter().position(|p| p.get_name() == name) {
                // Not found — add the new parameter
                None => current_params.push(incoming_param),
                // Found — normalize the types and replace
                Some(index) => {
                    let existing_param = &current_params[index];
                    let new_param = Parameter::new(
                        name,
                        DataType::best_fit(vec![
                            existing_param.get_data_type(),
                            incoming_param.get_data_type(),
                        ]),
                    );
                    current_params[index] = new_param;
                }
            }
        }
        current_params
    }

    pub fn new(name: impl Into<String>, param_type: DataType) -> Self {
        Self::new_with_default(name, param_type, Null)
    }

    pub fn new_with_default(name: impl Into<String>,
                            param_type: DataType,
                            default_value: TypedValue) -> Self {
        Parameter { name: name.into(), data_type: param_type, default_value }
    }

    pub fn render(parameters: &Vec<Parameter>) -> String {
        Self::render_f(parameters, |p| p.to_code())
    }

    pub fn render_f(parameters: &Vec<Parameter>, f: fn(&Parameter) -> String) -> String {
        parameters.iter().map(|p| f(p))
            .collect::<Vec<String>>()
            .join(", ")
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
        if let Some(type_decl) = self.get_param_type() {
            if !type_decl.trim().is_empty() {
                buf = format!("{}: {}", buf, type_decl)
            }
        }
        match self.get_default_value() {
            TypedValue::Null | TypedValue::Undefined => {}
            default_value => buf = format!("{} = {}", buf, default_value.to_code())
        }
        buf
    }

    pub fn to_code_enum(&self) -> String {
        let mut buf = self.get_name().to_string();
        match self.get_default_value() {
            TypedValue::Null | TypedValue::Undefined => {}
            default_value => buf = format!("{} = {}", buf, default_value.to_code())
        }
        buf
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!(&self)
    }

    pub fn with_default(&self, default_value: TypedValue) -> Self {
        let mut param = self.clone();
        param.default_value = default_value;
        param
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_types::DataType::{FixedSizeType, NumberType, StringType};

    use crate::number_kind::NumberKind::F64Kind;
    use crate::typed_values::TypedValue::{Null, StringValue};

    #[test]
    fn test_from_columns() {
        let parameters = vec![
            Parameter::new("symbol", FixedSizeType(StringType.into(), 8)),
            Parameter::new("exchange", FixedSizeType(StringType.into(), 8)),
            Parameter::new("last_sale", NumberType(F64Kind)),
        ];
        let columns = vec![
            Column::new("symbol", FixedSizeType(StringType.into(), 8), Null, 9),
            Column::new("exchange", FixedSizeType(StringType.into(), 8), Null, 26),
            Column::new("last_sale", NumberType(F64Kind), Null, 43),
        ];
        assert_eq!(Parameter::from_columns(&columns), parameters);
    }

    #[test]
    fn test_to_code() {
        let param = Parameter::new_with_default("symbol", FixedSizeType(StringType.into(), 8), StringValue("N/A".into()));
        assert_eq!(param.to_code(), r#"symbol: String(8) = "N/A""#)
    }

    #[test]
    fn test_to_json() {
        let param = Parameter::new("symbol", FixedSizeType(StringType.into(), 8));
        assert_eq!(param.to_json().to_string(), r#"{"data_type":{"FixedSizeType":["StringType",8]},"default_value":"Null","name":"symbol"}"#.to_string())
    }
}