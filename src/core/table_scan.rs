#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// TableScan module
////////////////////////////////////////////////////////////////////

use crate::expression::Conditions;
use crate::machine::Machine;
use crate::typed_values::TypedValue;
use serde::{Deserialize, Serialize};

/// Table Scan Plan
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TableScanPlan {
    position: usize,
    is_forward: bool,
    scan_type: TableScanTypes,
}

impl TableScanPlan {
    pub fn new(
        position: usize,
        is_forward: bool,
        scan_type: TableScanTypes,
    ) -> Self {
        Self { position, is_forward, scan_type }
    }

    pub fn column_scan(column_index: usize, column_value: TypedValue) -> Self {
        Self::new(0, true, TableScanTypes::ColumnScan {
            column_index,
            column_value,
        })
    }

    pub fn complete_scan() -> Self {
        Self::new(0, true, TableScanTypes::CompleteScan)
    }

    pub fn condition_scan(machine: Machine, condition: Conditions) -> Self {
        Self::new(0, true, TableScanTypes::ConditionScan {
            machine,
            condition,
        })
    }

    pub fn flip(&self) -> Self {
        let mut scan = self.clone();
        scan.is_forward = !self.is_forward;
        scan
    }

    pub fn is_forward(&self) -> bool {
        self.is_forward
    }

    pub fn get_position(&self) -> usize {
        self.position
    }

    pub fn get_scan_type(&self) -> TableScanTypes {
        self.scan_type.clone()
    }

    pub fn with_direction(&self, is_forward: bool) -> Self {
        let mut scan = self.clone();
        scan.is_forward = is_forward;
        scan
    }

    pub fn with_position(&self, position: usize) -> Self {
        let mut scan = self.clone();
        scan.position = position;
        scan
    }

    pub fn with_scan_type(&self, scan_type: TableScanTypes) -> Self {
        let mut scan = self.clone();
        scan.scan_type = scan_type;
        scan
    }
}

/// Table Scan Types
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum TableScanTypes {
    CompleteScan,
    ColumnScan {
        column_index: usize,
        column_value: TypedValue,
    },
    ConditionScan {
        machine: Machine,
        condition: Conditions,
    },
}