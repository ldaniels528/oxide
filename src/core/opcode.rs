////////////////////////////////////////////////////////////////////
// opcode module
////////////////////////////////////////////////////////////////////

use std::io;

use crate::machine::MachineState;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{Float64Value, Int64Value, Undefined};

/// arithmetic addition
pub fn add(ms: &MachineState) -> io::Result<MachineState> {
    let (ms, y) = ms.pop_or(Undefined);
    let (ms, x) = ms.pop_or(Undefined);
    Ok(ms.push(x + y))
}

/// arithmetic decrement
pub fn dec(ms: &MachineState) -> io::Result<MachineState> {
    let (ms, x) = ms.pop_or(Undefined);
    ms.transform_numeric(x, |n| Int64Value(n - 1), |n| Float64Value(n - 1.))
}

/// arithmetic division
pub fn div(ms: &MachineState) -> io::Result<MachineState> {
    let (ms, y) = ms.pop_or(Undefined);
    let (ms, x) = ms.pop_or(Undefined);
    Ok(ms.push(x / y))
}

/// arithmetic factorial
pub fn fact(ms: &MachineState) -> io::Result<MachineState> {
    fn fact_i64(n: i64) -> TypedValue {
        fn fact_i(n: i64) -> i64 { if n <= 1 { 1 } else { n * fact_i(n - 1) } }
        Int64Value(fact_i(n))
    }

    fn fact_f64(n: f64) -> TypedValue {
        fn fact_f(n: f64) -> f64 { if n <= 1. { 1. } else { n * fact_f(n - 1.) } }
        Float64Value(fact_f(n))
    }

    let (ms, number) = ms.pop_or(Undefined);
    ms.transform_numeric(number, |n| fact_i64(n), |n| fact_f64(n))
}

/// arithmetic increment
pub fn inc(ms: &MachineState) -> io::Result<MachineState> {
    let (ms, x) = ms.pop_or(Undefined);
    ms.transform_numeric(x, |n| Int64Value(n + 1), |n| Float64Value(n + 1.))
}

/// arithmetic multiplication
pub fn mul(ms: &MachineState) -> io::Result<MachineState> {
    let (ms, y) = ms.pop_or(Undefined);
    let (ms, x) = ms.pop_or(Undefined);
    Ok(ms.push(x * y))
}

/// arithmetic subtraction
pub fn sub(ms: &MachineState) -> io::Result<MachineState> {
    let (ms, y) = ms.pop_or(Undefined);
    let (ms, x) = ms.pop_or(Undefined);
    Ok(ms.push(x - y))
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::machine::MachineState;
    use crate::opcode::{add, dec, div, fact, inc, mul, sub};
    use crate::typed_values::TypedValue::{Float64Value, Int32Value, Int64Value};

    #[test]
    fn test_dec() {
        let ms = MachineState::build().push(Int64Value(6));
        let ms = dec(&ms).unwrap();
        let (_, result) = ms.pop();
        assert_eq!(result, Some(Int64Value(5)));
    }

    #[test]
    fn test_inc() {
        let ms = MachineState::build().push(Int64Value(6));
        let ms = inc(&ms).unwrap();
        let (_, result) = ms.pop();
        assert_eq!(result, Some(Int64Value(7)));
    }

    #[test]
    fn test_fact_i32() {
        let ms = MachineState::build().push(Int32Value(5));
        let ms = fact(&ms).unwrap();
        let (_, result) = ms.pop();
        assert_eq!(result, Some(Int64Value(120)));
    }

    #[test]
    fn test_fact_f64() {
        let ms = MachineState::build().push(Float64Value(6.));
        let ms = fact(&ms).unwrap();
        let (_, result) = ms.pop();
        assert_eq!(result, Some(Float64Value(720.)));
    }

    #[test]
    fn test_multiple_opcodes() {
        // execute the program
        let (ms, value) = MachineState::build()
            .push_all(vec![
                2., 3., // add
                4., // mul
                5., // sub
                2., // div
            ].iter().map(|n| Float64Value(*n)).collect())
            .execute(&vec![add, mul, sub, div])
            .unwrap();

        // verify the result
        assert_eq!(value, Float64Value(-0.08));
        assert_eq!(ms.stack_len(), 0)
    }
}