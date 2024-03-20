////////////////////////////////////////////////////////////////////
// opcode module
////////////////////////////////////////////////////////////////////

use std::io;

use crate::machine::MachineState;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{Float64Value, Int64Value, Undefined};

/// arithmetic addition
pub fn add(vm: &MachineState) -> io::Result<MachineState> {
    let (vm, y) = vm.pop_or(Undefined);
    let (vm, x) = vm.pop_or(Undefined);
    Ok(vm.push(x + y))
}

/// arithmetic decrement
pub fn dec(vm: &MachineState) -> io::Result<MachineState> {
    let (vm, x) = vm.pop_or(Undefined);
    vm.transform_numeric(x, |n| Int64Value(n - 1), |n| Float64Value(n - 1.))
}

/// arithmetic division
pub fn div(vm: &MachineState) -> io::Result<MachineState> {
    let (vm, y) = vm.pop_or(Undefined);
    let (vm, x) = vm.pop_or(Undefined);
    Ok(vm.push(x / y))
}

/// arithmetic factorial
pub fn fact(vm: &MachineState) -> io::Result<MachineState> {
    fn fact_i64(n: i64) -> TypedValue {
        fn fact_i(n: i64) -> i64 { if n <= 1 { 1 } else { n * fact_i(n - 1) } }
        Int64Value(fact_i(n))
    }

    fn fact_f64(n: f64) -> TypedValue {
        fn fact_f(n: f64) -> f64 { if n <= 1. { 1. } else { n * fact_f(n - 1.) } }
        Float64Value(fact_f(n))
    }

    let (vm, number) = vm.pop_or(Undefined);
    vm.transform_numeric(number, |n| fact_i64(n), |n| fact_f64(n))
}

/// arithmetic increment
pub fn inc(vm: &MachineState) -> io::Result<MachineState> {
    let (vm, x) = vm.pop_or(Undefined);
    vm.transform_numeric(x, |n| Int64Value(n + 1), |n| Float64Value(n + 1.))
}

/// arithmetic multiplication
pub fn mul(vm: &MachineState) -> io::Result<MachineState> {
    let (vm, y) = vm.pop_or(Undefined);
    let (vm, x) = vm.pop_or(Undefined);
    Ok(vm.push(x * y))
}

/// arithmetic subtraction
pub fn sub(vm: &MachineState) -> io::Result<MachineState> {
    let (vm, y) = vm.pop_or(Undefined);
    let (vm, x) = vm.pop_or(Undefined);
    Ok(vm.push(x - y))
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::machine::MachineState;
    use crate::opcode::{add, dec, div, fact, inc, mul, sub};
    use crate::typed_values::TypedValue::{Float64Value, Int32Value, Int64Value};

    #[test]
    fn test_dec() {
        let vm = MachineState::new().push(Int64Value(6));
        let vm = dec(&vm).unwrap();
        let (vm, result) = vm.pop();
        assert_eq!(result, Some(Int64Value(5)));
    }

    #[test]
    fn test_inc() {
        let vm = MachineState::new().push(Int64Value(6));
        let vm = inc(&vm).unwrap();
        let (vm, result) = vm.pop();
        assert_eq!(result, Some(Int64Value(7)));
    }

    #[test]
    fn test_fact_i32() {
        let vm = MachineState::new().push(Int32Value(5));
        let vm = fact(&vm).unwrap();
        let (vm, result) = vm.pop();
        assert_eq!(result, Some(Int64Value(120)));
    }

    #[test]
    fn test_fact_f64() {
        let vm = MachineState::new().push(Float64Value(6.));
        let vm = fact(&vm).unwrap();
        let (vm, result) = vm.pop();
        assert_eq!(result, Some(Float64Value(720.)));
    }

    #[test]
    fn test_multiple_opcodes() {
        // execute the program
        let vm = MachineState::new()
            .push_all(vec![
                2.0, 3.0, // add
                4.0, // mul
                5.0, // sub
                2.0, // div
            ].iter().map(|n| Float64Value(*n)).collect())
            .execute(vec![add, mul, sub, div])
            .unwrap();

        // get the return value
        let (vm, ret_val) = vm.pop();
        assert_eq!(ret_val, Some(Float64Value(-0.08)));
        assert_eq!(vm.stack_len(), 0)
    }
}