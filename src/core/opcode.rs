////////////////////////////////////////////////////////////////////
// opcode module
////////////////////////////////////////////////////////////////////

use crate::machine::{ErrorCode, MachineState};
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{Float64Value, Int64Value, Undefined};

/// arithmetic addition
pub fn add(vm: &mut MachineState) -> Option<ErrorCode> {
    let y = vm.pop().unwrap_or(Undefined);
    let x = vm.pop().unwrap_or(Undefined);
    vm.push(x + y);
    None
}

/// arithmetic decrement
pub fn dec(vm: &mut MachineState) -> Option<ErrorCode> {
    let x = vm.pop().unwrap_or(Undefined);
    vm.transform_numeric(x, |n| Int64Value(n - 1), |n| Float64Value(n - 1.))
}

/// arithmetic division
pub fn div(vm: &mut MachineState) -> Option<ErrorCode> {
    let y = vm.pop().unwrap_or(Undefined);
    let x = vm.pop().unwrap_or(Undefined);
    vm.push(x / y);
    None
}

/// arithmetic factorial
pub fn fact(vm: &mut MachineState) -> Option<ErrorCode> {
    fn fact_i64(n: i64) -> TypedValue {
        fn fact_i(n: i64) -> i64 { if n <= 1 { 1 } else { n * fact_i(n - 1) } }
        Int64Value(fact_i(n))
    }

    fn fact_f64(n: f64) -> TypedValue {
        fn fact_f(n: f64) -> f64 { if n <= 1. { 1. } else { n * fact_f(n - 1.) } }
        Float64Value(fact_f(n))
    }

    let number = vm.pop().unwrap_or(Undefined);
    vm.transform_numeric(number, |n| fact_i64(n), |n| fact_f64(n))
}

/// arithmetic increment
pub fn inc(vm: &mut MachineState) -> Option<ErrorCode> {
    let x = vm.pop().unwrap_or(Undefined);
    vm.transform_numeric(x, |n| Int64Value(n + 1), |n| Float64Value(n + 1.))
}

/// arithmetic multiplication
pub fn mul(vm: &mut MachineState) -> Option<ErrorCode> {
    let y = vm.pop().unwrap_or(Undefined);
    let x = vm.pop().unwrap_or(Undefined);
    vm.push(x * y);
    None
}

/// arithmetic subtraction
pub fn sub(vm: &mut MachineState) -> Option<ErrorCode> {
    let y = vm.pop().unwrap_or(Undefined);
    let x = vm.pop().unwrap_or(Undefined);
    vm.push(x - y);
    None
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::machine::MachineState;
    use crate::opcode::{dec, fact, inc};
    use crate::typed_values::TypedValue::{Float64Value, Int32Value, Int64Value};

    #[test]
    fn test_dec() {
        let mut vm = MachineState::new();
        vm.push(Int64Value(6));
        dec(&mut vm);
        assert_eq!(vm.pop(), Some(Int64Value(5)));
    }

    #[test]
    fn test_inc() {
        let mut vm = MachineState::new();
        vm.push(Int64Value(6));
        inc(&mut vm);
        assert_eq!(vm.pop(), Some(Int64Value(7)));
    }

    #[test]
    fn test_fact_i32() {
        let mut vm = MachineState::new();
        vm.push(Int32Value(5));
        fact(&mut vm);
        assert_eq!(vm.pop(), Some(Int64Value(120)));
    }

    #[test]
    fn test_fact_f64() {
        let mut vm = MachineState::new();
        vm.push(Float64Value(6.));
        fact(&mut vm);
        assert_eq!(vm.pop(), Some(Float64Value(720.)));
    }
}