#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Descriptor class - represents a descriptor for a parameter
////////////////////////////////////////////////////////////////////

use crate::columns::Column;
use crate::parameter::Parameter;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::Undefined;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Represents a parameter descriptor
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Descriptor {
    name: String,
    param_type: Option<String>, // i.e. Some("String(80)")
    default_value: Option<String>, // i.e. Some("'Hello World'")
}

impl Descriptor {

    ////////////////////////////////////////////////////////////////////
    //  STATIC METHODS
    ////////////////////////////////////////////////////////////////////

    pub fn from_column(column: &Column) -> Self {
        Descriptor::new(column.get_name(),
                        column.get_data_type().to_type_declaration(),
                        match &column.get_default_value() {
                            TypedValue::Null | Undefined => None,
                            v => Some(v.to_json().to_string())
                        })
    }

    pub fn from_columns(phys_columns: &Vec<Column>) -> Vec<Self> {
        phys_columns.iter().map(Self::from_column).collect()
    }

    pub fn from_parameter(param: &Parameter) -> Self {
        Descriptor::new(
            param.get_name(),
            param.get_data_type().to_type_declaration(),
            match param.get_default_value() {
                TypedValue::Null | Undefined => None,
                v => Some(v.to_code())
            })
    }

    pub fn from_parameters(parameters: &Vec<Parameter>) -> Vec<Self> {
        parameters.iter().map(Self::from_parameter).collect()
    }

    pub fn from_tuple(name: impl Into<String>, value: TypedValue) -> Self {
        Self::new(
            name.into(),
            value.get_type().to_type_declaration(),
            Some(value.to_code()),
        )
    }

    pub fn new(name: impl Into<String>,
               param_type: Option<String>,
               default_value: Option<String>) -> Self {
        Descriptor { name: name.into(), param_type, default_value }
    }

    pub fn render(descriptors: &Vec<Descriptor>) -> String {
        descriptors.iter().map(|d| d.to_code())
            .collect::<Vec<String>>()
            .join(", ")
    }

    ////////////////////////////////////////////////////////////////////
    //  INSTANCE METHODS
    ////////////////////////////////////////////////////////////////////

    pub fn get_name(&self) -> &str { &self.name }

    pub fn get_param_type(&self) -> Option<String> { self.param_type.clone() }

    pub fn get_default_value(&self) -> &Option<String> { &self.default_value }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!(self)
    }

    pub fn to_code(&self) -> String {
        let mut buf = self.get_name().to_string();
        if let Some(type_decl) = self.get_param_type() {
            if !type_decl.trim().is_empty() {
                buf = format!("{}: {}", buf, type_decl)
            }
        }
        if let Some(value) = self.get_default_value() {
            buf = format!("{} := {}", buf, value)
        }
        buf
    }
}

impl Display for Descriptor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_code())
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_types::DataType::{NumberType, StringType};
    use crate::data_types::StorageTypes::FixedSize;
    use crate::number_kind::NumberKind::F64Kind;
    use crate::typed_values::TypedValue::{Null, StringValue};

    #[test]
    fn test_from_columns() {
        let descriptors = vec![
            Descriptor::new("symbol", Some("String(8)".into()), None),
            Descriptor::new("exchange", Some("String(8)".into()), None),
            Descriptor::new("last_sale", Some("f64".into()), None),
        ];
        let columns = vec![
            Column::new("symbol", StringType(FixedSize(8)), Null, 9),
            Column::new("exchange", StringType(FixedSize(8)), Null, 26),
            Column::new("last_sale", NumberType(F64Kind), Null, 43),
        ];
        assert_eq!(Descriptor::from_columns(&columns), descriptors);
    }

    #[test]
    fn test_from_parameter() {
        let param = Parameter::with_default(
            "symbol",
            StringType(FixedSize(8)),
            StringValue("N/A".into()),
        );
        let descr = Descriptor::from_parameter(&param);
        assert_eq!(descr, Descriptor::new(
            "symbol", Some("String(8)".into()), Some("\"N/A\"".into())
        ))
    }

    #[test]
    fn test_from_parameters() {
        let descriptors = vec![
            Descriptor::new("symbol", Some("String(8)".into()), None),
            Descriptor::new("exchange", Some("String(8)".into()), None),
            Descriptor::new("last_sale", Some("f64".into()), None),
        ];
        let parameters = vec![
            Parameter::new("symbol", StringType(FixedSize(8))),
            Parameter::new("exchange", StringType(FixedSize(8))),
            Parameter::new("last_sale", NumberType(F64Kind)),
        ];
        assert_eq!(Descriptor::from_parameters(&parameters), descriptors)
    }

    #[test]
    fn test_to_json() {
        let descriptor = Descriptor::new("symbol", Some("String(8)".into()), None);
        assert_eq!(descriptor.to_json().to_string(), r#"{"default_value":null,"name":"symbol","param_type":"String(8)"}"#.to_string())
    }
}