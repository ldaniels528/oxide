////////////////////////////////////////////////////////////////////
// Peers module
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RemoteCallForm {
    code: String,
}

impl RemoteCallForm {
    pub fn new(code: String) -> Self {
        RemoteCallForm { code }
    }

    pub fn get_code(&self) -> &String { &self.code }
}