////////////////////////////////////////////////////////////////////
// columns module
////////////////////////////////////////////////////////////////////



use serde::Serialize;

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Column {
    pub name: String,
    pub column_type: String,
    pub default_value: String,
}

impl Column {
    pub fn new(name: &str, column_type: &str, default_value: &str) -> Self {
        Column {
            name: name.to_string(),
            column_type: column_type.to_string(),
            default_value: default_value.to_string(),
        }
    }
}


// Unit tests
#[cfg(test)]
mod tests {}