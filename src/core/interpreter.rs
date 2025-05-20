#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Interpreter class
////////////////////////////////////////////////////////////////////

use crate::compiler::Compiler;
use crate::expression::Expression;
use crate::machine::Machine;
use crate::typed_values::TypedValue;
use num_traits::real::Real;
use serde::{Deserialize, Serialize};

/// Represents the Oxide language interpreter.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Interpreter {
    machine: Machine,
}

impl Interpreter {

    ////////////////////////////////////////////////////////////////
    // static methods
    ////////////////////////////////////////////////////////////////

    /// Constructs a new Interpreter using the provided machine state
    pub fn build(machine: Machine) -> Self {
        Self { machine }
    }

    /// Constructs a new Interpreter
    pub fn new() -> Self {
        Self::build(Machine::new_platform())
    }

    ////////////////////////////////////////////////////////////////
    // instance methods
    ////////////////////////////////////////////////////////////////

    /// Executes the supplied source code returning the result of the evaluation
    pub fn evaluate(&mut self, source_code: &str) -> std::io::Result<TypedValue> {
        let opcode = Compiler::build(source_code)?;
        self.invoke(&opcode)
    }

    /// returns a variable by name
    pub fn get(&self, name: &str) -> Option<TypedValue> {
        self.machine.get(name)
    }

    /// Executes the supplied source code returning the result of the evaluation
    pub fn invoke(&mut self, opcode: &Expression) -> std::io::Result<TypedValue> {
        let (machine, result) = self.machine.evaluate(&opcode)?;
        self.machine = machine;
        Ok(result)
    }

    /// Sets the value of a variable
    pub fn with_variable(&mut self, name: &str, value: TypedValue) {
        self.machine = self.machine.with_variable(name, value);
    }
}

/// Unit tests
#[cfg(test)]
mod tests {
    use crate::interpreter::Interpreter;
    use crate::numbers::Numbers::*;
    use crate::testdata::*;
    use crate::typed_values::TypedValue::*;

    #[test]
    fn test_let() {
        verify_exact_code(r#"
            let (x, y) = (5, 7)
            x * y
        "#, "35");
    }

    #[test]
    fn test_basic_state_manipulation() {
        let mut interpreter = Interpreter::new();
        interpreter = verify_exact_value_with(interpreter, "x := 5", Boolean(true));
        interpreter = verify_exact_value_with(interpreter, "-x", Number(I64Value(-5)));
        interpreter = verify_exact_value_with(interpreter, "x := x + 1", Boolean(true));
        interpreter = verify_exact_value_with(interpreter, "x", Number(I64Value(6)));
        interpreter = verify_exact_value_with(interpreter, "x < 7", Boolean(true));
        interpreter = verify_exact_value_with(interpreter, "x := x ** 2", Boolean(true));
        interpreter = verify_exact_value_with(interpreter, "x", Number(F64Value(36.)));
        interpreter = verify_exact_value_with(interpreter, "x / 0", Number(NaNValue));
        interpreter = verify_exact_value_with(interpreter, "x := x - 1", Boolean(true));
        interpreter = verify_exact_value_with(interpreter, "x % 5", Number(F64Value(0.)));
        interpreter = verify_exact_value_with(interpreter, "x < 35", Boolean(false));
        interpreter = verify_exact_value_with(interpreter, "x >= 35", Boolean(true));
    }

    #[test]
    fn test_math_pi() {
        let pi = std::f64::consts::PI;
        verify_exact_value("2 * π", Number(F64Value(2. * pi)))
    }

    #[test]
    fn test_feature_with_scenarios() {
        verify_exact_table(r#"
            use testing
            Feature "Matches function" {
                Scenario "Compare Array contents: Equal" {
                    assert(matches(
                        [ 1 "a" "b" "c" ],
                        [ 1 "a" "b" "c" ]
                    ))
                }
                Scenario "Compare Array contents: Not Equal" {
                    assert(!matches(
                        [ 1 "a" "b" "c" ],
                        [ 0 "x" "y" "z" ]
                    ))
                }
                Scenario "Compare JSON contents (in sequence)" {
                    assert(matches(
                            { first: "Tom" last: "Lane" },
                            { first: "Tom" last: "Lane" }))
                }
                Scenario "Compare JSON contents (out of sequence)" {
                    assert(matches(
                            { scores: [82 78 99], id: "A1537" },
                            { id: "A1537", scores: [82 78 99] }))
                }
            }"#, vec![
            "|--------------------------------------------------------------------------------------------------------------------------|",
            "| id | level | item                                                                                      | passed | result |",
            "|--------------------------------------------------------------------------------------------------------------------------|",
            "| 0  | 0     | Matches function                                                                          | true   | true   |",
            "| 1  | 1     | Compare Array contents: Equal                                                             | true   | true   |",
            r#"| 2  | 2     | assert(matches([1, "a", "b", "c"], [1, "a", "b", "c"]))                                   | true   | true   |"#,
            "| 3  | 1     | Compare Array contents: Not Equal                                                         | true   | true   |",
            r#"| 4  | 2     | assert(!matches([1, "a", "b", "c"], [0, "x", "y", "z"]))                                  | true   | true   |"#,
            "| 5  | 1     | Compare JSON contents (in sequence)                                                       | true   | true   |",
            r#"| 6  | 2     | assert(matches({first: "Tom", last: "Lane"}, {first: "Tom", last: "Lane"}))               | true   | true   |"#,
            "| 7  | 1     | Compare JSON contents (out of sequence)                                                   | true   | true   |",
            r#"| 8  | 2     | assert(matches({scores: [82, 78, 99], id: "A1537"}, {id: "A1537", scores: [82, 78, 99]})) | true   | true   |"#,
            "|--------------------------------------------------------------------------------------------------------------------------|"
        ]);
    }

    #[test]
    fn test_functional_pipeline_1arg() {
        verify_exact_code(r#"
            "Hello" |> tools::reverse
        "#, "\"olleH\"");
    }

    #[test]
    fn test_functional_pipeline_2args() {
        let mut interpreter = Interpreter::new();
        interpreter = verify_exact_code_with(interpreter, r#"
            fn add(a, b) => a + b
        "#, "true");

        interpreter = verify_exact_code_with(interpreter, r#"
            fn inverse(a) => 1.0 / a
        "#, "true");

        interpreter = verify_exact_code_with(interpreter, r#"
            (2, 3) |>> add 
        "#, "5");

        verify_exact_code_with(interpreter, r#"
            ((2, 3) |>> add) |> inverse
        "#, "0.2");
    }

    #[test]
    fn test_tuple_assignment() {
        verify_exact_value(r#"
            (a, b, c) := (3, 5, 7)
            a + b + c
        "#, Number(I64Value(15)))
    }

    /// Control-Flow tests
    #[cfg(test)]
    mod control_flow_tests {
        use crate::interpreter::Interpreter;
        use crate::numbers::Numbers::I64Value;
        use crate::testdata::*;
        use crate::typed_values::TypedValue::*;

        #[test]
        fn test_for_each_item_in_an_array() {
            verify_exact_code(r#"
                for item in [1, 5, 6, 11, 17] yield item
            "#, "[1, 5, 6, 11, 17]");
        }

        #[test]
        fn test_for_each_array_in_an_array() {
            verify_exact_code(r#"
                for [a, b] in [[1, 5], [6, 11], [17, 21]] {
                   yield b - a
                }
            "#, "[4, 5, 4]");
        }

        #[test]
        fn test_for_each_tuple_in_an_array() {
            verify_exact_code(r#"
                for (a, b) in [(1, 5), (6, 11), (17, 21)] {
                   yield a + b
                }
            "#, "[6, 17, 38]");
        }

        #[test]
        fn test_for_each_row_in_a_table() {
            verify_exact_code(r#"
                for row in tools::to_table(['apple', 'berry', 'kiwi', 'lime'])
                    yield row::value
            "#, r#"["apple", "berry", "kiwi", "lime"]"#);
        }

        #[test]
        fn test_for_yield_expression() {
            verify_exact_code(r#"
                for(i = 0, i < 5, i = i + 1) yield i * 5
            "#, "[0, 5, 10, 15, 20]");
        }

        #[test]
        fn test_if_when_result_is_defined() {
            verify_exact_code(r#"
                x := 7
                if(x > 5) "Yes"
            "#, "\"Yes\"");
        }

        #[test]
        fn test_if_when_result_is_undefined() {
            verify_exact_value(r#"
                x := 4
                if(x > 5) "Yes"
            "#, Undefined);
        }

        #[test]
        fn test_if_else_expression() {
            verify_exact_code(r#"
                x := 4
                if(x > 5) "Yes"
                else if(x < 5) "Maybe"
                else "No"
            "#, "\"Maybe\"");
        }

        #[test]
        fn test_if_function() {
            verify_exact_code(r#"
                x := 4
                iff(x > 5, "Yes", iff(x < 5, "Maybe", "No"))
            "#, "\"Maybe\"");
        }

        #[ignore]
        #[test]
        fn test_match() {
            verify_exact_code(r#"
                code := 100
                match code [
                   n: 100 ~> "Accepted",
                   n: 101..=104 ~> 'Escalated',
                   n: n > 0 && n < 100 ~> "Pending",
                   n ~> "Rejected"
                ]
            "#, "\"Accepted\"");
        }

        #[test]
        fn test_while_loop() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_value_with(interpreter, "x := 0", Boolean(true));
            interpreter = verify_exact_value_with(interpreter, r#"
                while (x < 5) x := x + 1
            "#, Boolean(true));
            interpreter = verify_exact_value_with(interpreter, "x", Number(I64Value(5)));
        }
    }

    /// Function tests
    #[cfg(test)]
    mod function_tests {
        use crate::interpreter::Interpreter;
        use crate::numbers::Numbers::I64Value;
        use crate::testdata::*;
        use crate::typed_values::TypedValue::*;

        #[test]
        fn test_function_lambda() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_value_with(interpreter, r#"
                product := fn (a, b) => a * b
            "#, Boolean(true));
            interpreter = verify_exact_value_with(interpreter, r#"
                product(2, 5)
            "#, Number(I64Value(10)))
        }

        #[test]
        fn test_function_named() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_value_with(interpreter, r#"
                fn product(a, b) => a * b
            "#, Boolean(true));
            interpreter = verify_exact_value_with(interpreter, r#"
                product(3, 7)
            "#, Number(I64Value(21)))
        }

        #[test]
        fn test_function_recursion_1() {
            verify_exact_value(r#"
                f := fn(n: i64) => if(n <= 1) 1 else n * f(n - 1)
                f(5)
            "#, Number(I64Value(120)))
        }

        #[test]
        fn test_function_recursion_2() {
            verify_exact_value(r#"
                f := fn(n) => iff(n <= 1, 1, n * f(n - 1))
                f(6)
            "#, Number(I64Value(720)))
        }
    }
}