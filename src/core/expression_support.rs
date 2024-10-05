////////////////////////////////////////////////////////////////////
// expression support structures
////////////////////////////////////////////////////////////////////

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::expression::{Condition, Expression};
use crate::machine::Machine;
use crate::server::ColumnJs;
use crate::typed_values::TypedValue;

/// Represents a Creation Entity
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CreationEntity {
    IndexEntity {
        columns: Vec<Expression>,
    },
    TableEntity {
        columns: Vec<ColumnJs>,
        from: Option<Box<Expression>>,
    },
}

/// Represents the set of all Directives
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Directives {
    MustAck(Box<Expression>),
    MustDie(Box<Expression>),
    MustIgnoreAck(Box<Expression>),
    MustNotAck(Box<Expression>),
}

/// Represents the set of all Quarry Activities
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Excavation {
    Construct(Infrastructure),
    Query(Queryable),
    Mutate(Mutation),
}

/// Represents an infrastructure construction/deconstruction event
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Infrastructure {
    Create { path: Box<Expression>, entity: CreationEntity },
    Declare(CreationEntity),
    Drop(MutateTarget),
}

/// Represents a data modification event
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Mutation {
    Append {
        path: Box<Expression>,
        source: Box<Expression>,
    },
    Compact {
        path: Box<Expression>,
    },
    Delete {
        path: Box<Expression>,
        condition: Option<Condition>,
        limit: Option<Box<Expression>>,
    },
    IntoNs(Box<Expression>, Box<Expression>),
    Overwrite {
        path: Box<Expression>,
        source: Box<Expression>,
        condition: Option<Condition>,
        limit: Option<Box<Expression>>,
    },
    Scan {
        path: Box<Expression>,
    },
    Truncate {
        path: Box<Expression>,
        limit: Option<Box<Expression>>,
    },
    Undelete {
        path: Box<Expression>,
        condition: Option<Condition>,
        limit: Option<Box<Expression>>,
    },
    Update {
        path: Box<Expression>,
        source: Box<Expression>,
        condition: Option<Condition>,
        limit: Option<Box<Expression>>,
    },
}

/// Represents a Mutation Target
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum MutateTarget {
    IndexTarget {
        path: Box<Expression>,
    },
    TableTarget {
        path: Box<Expression>,
    },
}


/// Represents a queryable
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Queryable {
    Describe(Box<Expression>),
    Limit { from: Box<Expression>, limit: Box<Expression> },
    Reverse(Box<Expression>),
    Select {
        fields: Vec<Expression>,
        from: Option<Box<Expression>>,
        condition: Option<Condition>,
        group_by: Option<Vec<Expression>>,
        having: Option<Box<Expression>>,
        order_by: Option<Vec<Expression>>,
        limit: Option<Box<Expression>>,
    },
    Where { from: Box<Expression>, condition: Condition },
}

mod tests {
    use crate::machine::Machine;
    use crate::native::NativeFeature;

    use super::*;

    #[test]
    fn test_dynamic_dispatch() {
        let executor = NativeFeature::NativeCode(Arc::new(|ms: Machine, args: Vec<TypedValue>| {
            println!("Executing native code! {:?}", ms);
        }));

        // Execute the closure
        let ms = Machine::new();
        let args = vec![];
        executor.execute(ms, args);
    }
}