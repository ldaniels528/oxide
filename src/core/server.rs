////////////////////////////////////////////////////////////////////
// server module
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};
use serde_json::Value;

const VERSION: &str = "0.1.0";

pub type ServerError = Box<dyn std::error::Error>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RowJs {
    id: usize,
    columns: Vec<ColumnJs>,
}

impl RowJs {
    pub fn new(id: usize, columns: Vec<ColumnJs>) -> Self {
        Self { id, columns }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnJs {
    name: String,
    value: Value,
}

impl ColumnJs {
    pub fn new(name: &str, value: Value) -> Self {
        Self { name: name.into(), value }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SystemInfoJs {
    title: String,
    version: String,
}

impl SystemInfoJs {
    pub fn new() -> SystemInfoJs {
        SystemInfoJs {
            title: "TinyDB".into(),
            version: "0.1.0".into(),
        }
    }
}

pub fn get_address(port: &str) -> String {
    format!("127.0.0.1:{}", port)
}

pub async fn get_port_number(args: Vec<String>) -> Result<String, ServerError> {
    if args.len() > 1 {
        let re = regex::Regex::new(r"^\d+$").unwrap();
        let port: String = args[1].trim().into();
        if re.is_match(&port) {
            Ok(port)
        } else {
            Err(format!("Port '{}' is invalid", port).into())
        }
    } else {
        Ok("8080".into())
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_system_info() {
        assert_eq!(SystemInfoJs::new(), SystemInfoJs {
            title: "TinyDB".into(),
            version: VERSION.into(),
        })
    }

    #[test]
    fn test_get_address() {
        assert_eq!(get_address("8888"), "127.0.0.1:8888")
    }

    #[tokio::test]
    async fn test_get_port_number_valid_input() -> Result<(), ServerError> {
        let args: Vec<String> = vec!["test_program".into(), "1234".into()];
        let expected: String = "1234".into();
        let actual: String = get_port_number(args).await?;
        assert_eq!(actual, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_get_port_number_no_input() -> Result<(), ServerError> {
        let args: Vec<String> = vec!["test_program".into()];
        let expected: String = "8080".into();
        let actual: String = get_port_number(args).await?;
        assert_eq!(actual, expected);
        Ok(())
    }
}