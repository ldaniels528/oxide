////////////////////////////////////////////////////////////////////
// Remote Procedure Call (RPC) module
////////////////////////////////////////////////////////////////////

use std::io;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::cnv_error;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RemoteCallRequest {
    code: String,
}

impl RemoteCallRequest {
    pub fn new(code: String) -> Self {
        RemoteCallRequest { code }
    }

    pub fn get_code(&self) -> &String { &self.code }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RemoteCallResponse {
    result: Value,
    message: Option<String>,
}

impl RemoteCallResponse {
    pub fn from_string(json_string: &str) -> io::Result<RemoteCallResponse> {
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

    pub fn get_message(&self) -> Option<String> { self.message.clone() }

    pub fn get_result(&self) -> Value { self.result.clone() }
}

// Unit tests
#[cfg(test)]
mod tests {}