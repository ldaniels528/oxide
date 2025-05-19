////////////////////////////////////////////////////////////////////
// shared libraries
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[macro_export]
macro_rules! cnv_error {
    ($e:expr) => {
        std::io::Error::new(std::io::ErrorKind::Other, $e)
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct RemoteCallRequest {
    code: String,
}

impl RemoteCallRequest {
    pub fn new(code: String) -> Self {
        RemoteCallRequest { code }
    }

    pub fn get_code(&self) -> &String { &self.code }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemoteCallResponse {
    result: Value,
    message: Option<String>,
}

impl RemoteCallResponse {
    pub fn from_string(json_string: &str) -> std::io::Result<RemoteCallResponse> {
        serde_json::from_str(json_string).map_err(|e| cnv_error!(e))
    }

    pub fn fail(message: String) -> Self {
        RemoteCallResponse {
            result: Value::Null,
            message: Some(message),
        }
    }

    pub fn success(value: Value) -> Self {
        RemoteCallResponse {
            result: value,
            message: None,
        }
    }

    pub fn get_message(&self) -> Option<String> { self.message.to_owned() }

    pub fn get_result(&self) -> Value { self.result.to_owned() }
}

