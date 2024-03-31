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

    pub fn get_message(&self) -> Option<String> { self.message.clone() }

    pub fn get_result(&self) -> Value { self.result.clone() }
}

pub fn fail<A>(message: impl Into<String>) -> std::io::Result<A> {
    Err(std::io::Error::new(std::io::ErrorKind::Other, message.into()))
}

/// Extracts a tuple consisting of the first two arguments from the supplied commandline arguments
pub fn get_host_and_port(args: Vec<String>) -> std::io::Result<(String, String)> {
    // args: ['./myapp', 'arg1', 'arg2', ..]
    let (host, port) = match args.as_slice() {
        [_, port] => ("127.0.0.1".to_string(), port.to_string()),
        [_, host, port] => (host.to_string(), port.to_string()),
        [_, host, port, ..] => (host.to_string(), port.to_string()),
        _ => ("127.0.0.1".to_string(), "8080".to_string())
    };

    // validate the port number
    let port_regex = regex::Regex::new(r"^\d+$").map_err(|e| cnv_error!(e))?;
    if !port_regex.is_match(&port) {
        return fail(format!("Port number '{}' is invalid", port));
    }
    Ok((host, port))
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_commandline_arguments() {
        assert_eq!(get_host_and_port(vec![]).unwrap(),
                   ("127.0.0.1".to_string(), "8080".to_string()));

        assert_eq!(get_host_and_port(vec!["my_app".into(), "3333".into()]).unwrap(),
                   ("127.0.0.1".to_string(), "3333".to_string()));

        assert_eq!(get_host_and_port(vec!["my_app".into(), "0.0.0.0".into(), "9000".into()]).unwrap(),
                   ("0.0.0.0".to_string(), "9000".to_string()));

        assert_eq!(get_host_and_port(vec!["my_app".into(), "127.0.0.1".into(), "3333".into(), "zzz".into()]).unwrap(),
                   ("127.0.0.1".to_string(), "3333".to_string()));
    }
}