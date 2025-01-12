#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// server module
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};
use crate::platform::VERSION;

// JSON representation of Oxide system information
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SystemInfoJs {
    title: String,
    version: String,
}

impl SystemInfoJs {
    pub fn new() -> SystemInfoJs {
        SystemInfoJs {
            title: "Oxide".into(),
            version: VERSION.into(),
        }
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::server::SystemInfoJs;

    use super::*;

    #[test]
    fn test_create_system_info() {
        assert_eq!(SystemInfoJs::new(), SystemInfoJs {
            title: "Oxide".into(),
            version: VERSION.into(),
        })
    }
}