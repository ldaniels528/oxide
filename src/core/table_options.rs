#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// TableOptions class
////////////////////////////////////////////////////////////////////

use crate::expression::Expression;
use serde::{Deserialize, Serialize};

/// Table Options
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct TableOptions {
    function: Option<Box<Expression>>,
    indices: Vec<usize>,
    is_journaling: bool,
}

impl TableOptions {

    ////////////////////////////////////////////////////////////////////
    // Static Methods
    ////////////////////////////////////////////////////////////////////

    pub fn new() -> TableOptions {
        Self {
            function: None,
            indices: vec![],
            is_journaling: false,
        }
    }

    ////////////////////////////////////////////////////////////////////
    // Instance Methods
    ////////////////////////////////////////////////////////////////////

    pub fn get_function(&self) -> &Option<Box<Expression>> {
        &self.function
    }

    pub fn get_indices(&self) -> &Vec<usize> {
        &self.indices
    }

    pub fn is_journaling(&self) -> bool {
        self.is_journaling
    }

    pub fn with_function(&self, function: Option<Box<Expression>>) -> Self {
        let mut opts = self.clone();
        opts.function = function;
        opts
    }

    pub fn with_index(&self, column_index: usize) -> Self {
        let mut opts = self.clone();
        opts.indices.push(column_index);
        opts
    }

    pub fn with_journaling(&self, is_journaling: bool) -> Self {
        let mut opts = self.clone();
        opts.is_journaling = is_journaling;
        opts
    }
}