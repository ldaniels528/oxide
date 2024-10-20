////////////////////////////////////////////////////////////////////
// Parameter class
////////////////////////////////////////////////////////////////////

use std::fmt::{Display, Formatter};
use log::error;
use serde::{Deserialize, Serialize};

use crate::decompiler::Decompiler;
use crate::structure::Structure;
use crate::table_columns::Column;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::Undefined;

// Represents a generic parameter
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Parameter {
    name: String,
    param_type: Option<String>,
    default_value: Option<String>,
}

impl Parameter {
    pub fn decode(buf: &Vec<u8>) -> Self {
        bincode::deserialize(buf).unwrap_or_else(|err| {
            error!("{}", err);
            Parameter::new(err.to_string(), Some("String(255)".into()), None)
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap_or_else(|err| {
            error!("{}", err);
            Vec::new()
        })
    }

    pub fn from_column(column: &Column) -> Self {
        Parameter::new(column.get_name(), column.get_data_type().to_type_declaration(), match &column.get_default_value() {
            TypedValue::Null | Undefined => None,
            v => Some(v.to_json().to_string())
        })
    }

    pub fn from_columns(phys_columns: &Vec<Column>) -> Vec<Self> {
        phys_columns.iter().map(|c| Self::from_column(c)).collect()
    }

    pub fn get_name(&self) -> &str { &self.name }

    pub fn get_param_type(&self) -> &Option<String> { &self.param_type }

    pub fn get_default_value(&self) -> &Option<String> { &self.default_value }

    pub fn new(name: impl Into<String>,
               param_type: Option<String>,
               default_value: Option<String>) -> Self {
        Parameter { name: name.into(), param_type, default_value }
    }

    pub fn render(columns: &Vec<Parameter>) -> String {
        columns.iter().map(|c| c.to_code())
            .collect::<Vec<String>>()
            .join(", ")
    }

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
            buf = format!("{} = {}", buf, value)
        }
        buf
    }
}

impl Display for Parameter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_code())
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::testdata::{make_quote_parameters, make_quote_columns};

    use super::*;

    #[test]
    fn test_parameter_conversion() {
        let parameters = Parameter::from_columns(&make_quote_columns());
        assert_eq!(parameters, make_quote_parameters());
    }

    #[test]
    fn test_parameter_new() {
        let parameters = vec![
            Parameter::new("symbol", Some("String(8)".into()), None),
            Parameter::new("exchange", Some("String(10)".into()), None),
            Parameter::new("last_sale", Some("f64".into()), Some("0.0".into())),
        ];
        assert_eq!(parameters, vec![
            Parameter {
                name: "symbol".into(),
                param_type: Some("String(8)".into()),
                default_value: None,
            },
            Parameter {
                name: "exchange".into(),
                param_type: Some("String(10)".into()),
                default_value: None,
            },
            Parameter {
                name: "last_sale".into(),
                param_type: Some("f64".into()),
                default_value: Some("0.0".into()),
            },
        ])
    }

    #[test]
    fn test_to_json() {
        let param = Parameter::new("symbol", Some("String(8)".into()), None);
        assert_eq!(param.to_json().to_string(), r#"{"default_value":null,"name":"symbol","param_type":"String(8)"}"#.to_string())
    }
}