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
    fn test_math_pi() {
        let pi = std::f64::consts::PI;
        verify_exact_value("2 * π", Number(F64Value(2. * pi)))
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
    fn test_feature_with_scenarios() {
        verify_exact_table(r#"
            import testing
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
    fn test_fold_over() {
        verify_exact_code(r#"
            "Hello" |> tools::reverse
        "#, "\"olleH\"");
    }

    #[ignore]
    #[test]
    fn test_fold_over_chaining() {
        verify_exact_value(r#"
            "Hello" |> util::md5 |> util::hex
        "#, StringValue("8b1a9953c4611296a827abf8c47804d7".to_string()));
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
        fn test_foreach_item_in_an_array() {
            verify_exact_code(r#"
                foreach item in [1, 5, 6, 11, 17]
                    oxide::println(item)
            "#, "true");
            // 1
            // 5
            // 6
            // 11
            // 17
        }

        #[test]
        fn test_foreach_row_in_a_table() {
            verify_exact_code(r#"
                foreach row in tools::to_table(['apple', 'berry', 'kiwi', 'lime'])
                    oxide::println(row)
            "#, "true");
            // {"value":"apple"}
            // {"value":"berry"}
            // {"value":"kiwi"}
            // {"value":"lime"}
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

        #[test]
        fn test_while_loop() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_value_with(interpreter, "x := 0", Boolean(true));
            interpreter = verify_exact_value_with(interpreter, r#"
                while (x < 5)
                    x := x + 1
            "#, Boolean(true));
            interpreter = verify_exact_value_with(interpreter, "x", Number(I64Value(5)));
        }
    }

    /// Structure tests
    #[cfg(test)]
    mod directive_tests {
        use crate::interpreter::Interpreter;
        use crate::testdata::*;
        use crate::typed_values::TypedValue::*;

        #[test]
        fn test_directive_die() {
            verify_outcome_whence(
                Interpreter::new(),
                r#"[!] "Kaboom!!!""#,
                |r| r.is_err_and(|err| err.to_string() == "Kaboom!!!".to_string()),
            );
        }

        #[test]
        fn test_directive_ignore_failure() {
            verify_exact_code(r#"[~] 7 / 0"#, "true");
        }

        #[test]
        fn test_directive_must_be_false() {
            verify_exact_code(r#"
                [+] x := 67
                [-] x < 67
            "#, "false");
        }

        #[test]
        fn test_directive_must_be_true() {
            verify_exact_code("[+] x := 67", "true");
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
            interpreter = verify_exact_value_with(interpreter,r#"
                fn product(a, b) => a * b
            "#, Boolean(true));
            interpreter = verify_exact_value_with(interpreter,r#"
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