////////////////////////////////////////////////////////////////////
// columns module
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub column_type: String,
    pub default_value: String,
}

impl Column {
    pub fn new(name: impl Into<String>,
               column_type: impl Into<String>,
               default_value: impl Into<String>) -> Self {
        Column {
            name: name.into(),
            column_type: column_type.into(),
            default_value: default_value.into(),
        }
    }
}


// Unit tests
#[cfg(test)]
mod tests {}