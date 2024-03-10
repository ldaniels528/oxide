////////////////////////////////////////////////////////////////////
// columns module
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub column_type: String,
    pub default_value: Option<String>,
}

impl Column {
    pub fn new(name: impl Into<String>,
               column_type: impl Into<String>,
               default_value: Option<String>) -> Self {
        Column {
            name: name.into(),
            column_type: column_type.into(),
            default_value,
        }
    }
}


// Unit tests
#[cfg(test)]
mod tests {
    use super::Column;

    #[test]
    fn test_constructor() {
        let columns = vec![
            Column::new("symbol", "String(8)", None),
            Column::new("exchange", "String(10)", None),
            Column::new("lastSale", "Double", Some("0.0".into())),
        ];
        assert_eq!(columns, vec![
            Column {
                name: "symbol".into(),
                column_type: "String(8)".into(),
                default_value: None,
            },
            Column {
                name: "exchange".into(),
                column_type: "String(10)".into(),
                default_value: None,
            },
            Column {
                name: "lastSale".into(),
                column_type: "Double".into(),
                default_value: Some("0.0".into()),
            },
        ])
    }
}