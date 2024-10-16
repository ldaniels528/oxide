////////////////////////////////////////////////////////////////////
// NativeFeature module
////////////////////////////////////////////////////////////////////

use std::sync::Arc;

use crate::machine::Machine;
use crate::typed_values::TypedValue;

/// Represents a Native Feature
pub enum NativeFeature {
    NativeCode(Arc<dyn Fn(Machine, Vec<TypedValue>) + Send + Sync + 'static>),
}

impl NativeFeature {
    // Execute the closure stored in the enum variant
    pub fn execute(self, ms: Machine, args: Vec<TypedValue>) {
        match self {
            NativeFeature::NativeCode(code) => code(ms, args),
        }
    }
}

impl Clone for NativeFeature {
    fn clone(&self) -> Self {
        match self {
            NativeFeature::NativeCode(f) =>
                NativeFeature::NativeCode(f.to_owned())
        }
    }
}

impl PartialEq for NativeFeature {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (NativeFeature::NativeCode(code1), NativeFeature::NativeCode(code2)) => {
                // Compare the Arc's internal raw pointers
                Arc::ptr_eq(code1, code2)
            }
        }
    }
}

// Unit tests
#[cfg(test)]
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