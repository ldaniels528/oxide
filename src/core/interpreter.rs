#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Interpreter class
////////////////////////////////////////////////////////////////////

use crate::compiler::Compiler;
use crate::expression::Expression;
use crate::machine::Machine;
use crate::typed_values::TypedValue;
use serde::{Deserialize, Serialize};

/// Represents the Oxide language interpreter.
#[derive(Clone, Debug, Serialize, Deserialize)]
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

    /// Executes the supplied source code returning the result of the evaluation
    pub async fn evaluate_async(&mut self, source_code: &str) -> std::io::Result<TypedValue> {
        let opcode = Compiler::build(source_code)?;
        self.invoke_async(&opcode).await
    }

    /// returns a variable by name
    pub fn get(&self, name: &str) -> Option<TypedValue> {
        self.machine.get(name)
    }
    
    pub fn get_machine(&self) -> &Machine {
        &self.machine
    }

    /// Executes the supplied source code returning the result of the evaluation
    pub fn invoke(&mut self, opcode: &Expression) -> std::io::Result<TypedValue> {
        let (machine, result) = self.machine.evaluate(&opcode)?;
        self.machine = machine;
        Ok(result)
    }

    /// Executes the supplied source code returning the result of the evaluation
    pub async fn invoke_async(&mut self, opcode: &Expression) -> std::io::Result<TypedValue> {
        let (machine, result) = self.machine.evaluate_async(&opcode).await?;
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

    /// Alias tests
    #[cfg(test)]
    mod alias_tests {
        use crate::interpreter::Interpreter;
        use crate::test_util::{verify_exact_code_async_and_sync, verify_exact_code_with, verify_exact_table_async_and_sync, verify_exact_table_with};

        #[actix::test]
        async fn test_alias_function_call() {
            verify_exact_code_async_and_sync(r#"
                alias count(n) = for i in 0..n yield i * 2
                count(5)
            "#, "[0, 2, 4, 6, 8]").await
        }

        #[test]
        fn test_alias_function_call_overloaded() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_table_with(interpreter,r#"
                alias add(n) = n + n
                alias add(n, m) = n + m
                aliases
            "#, vec![
                "|------------------------------|",
                "| id | value                   |",
                "|------------------------------|",
                "| 0  | alias add(n) = n + n    |",
                "| 1  | alias add(n, m) = n + m |",
                "|------------------------------|"]);

            interpreter = verify_exact_code_with(interpreter,r#"
                add(5)
            "#, "10");

            interpreter = verify_exact_code_with(interpreter,r#"
                add(5, 7)
            "#, "12");
        }

        #[actix::test]
        async fn test_alias_identifier_call() {
            verify_exact_code_async_and_sync(r#"
                alias nn = for i in 0..5 yield i * 2
                nn
            "#, "[0, 2, 4, 6, 8]").await
        }

        #[actix::test]
        async fn test_aliases() {
            verify_exact_table_async_and_sync(r#"
                alias add(n) = n + n
                alias add(n, m) = n + m
                aliases
            "#, vec![
                "|------------------------------|",
                "| id | value                   |",
                "|------------------------------|",
                "| 0  | alias add(n) = n + n    |",
                "| 1  | alias add(n, m) = n + m |",
                "|------------------------------|"]).await;
        }

    }

    /// Assignment tests
    #[cfg(test)]
    mod assignment_tests {
        use crate::interpreter::Interpreter;
        use crate::numbers::Numbers::{F64Value, I64Value, NaNValue};
        use crate::test_util::{verify_exact_code_async_and_sync, verify_exact_table_with, verify_exact_value_with};
        use crate::typed_values::TypedValue::{Boolean, Number};

        #[actix::test]
        async fn test_assignment_via_placeholder_async() {
            verify_exact_code_async_and_sync(r#"
                let _ = 5
            "#, "true").await
        }

        #[actix::test]
        async fn test_assignment_pseudo_numeric_async() {
            verify_exact_code_async_and_sync(r#"
                5_000_000
            "#, "5000000").await
        }

        #[actix::test]
        async fn test_assignment_via_array_destructuring_async() {
            verify_exact_code_async_and_sync(r#"
                let [a, b, c, d] = [3, 5, 7, 9]
                [a, b, c, d]
            "#, "[3, 5, 7, 9]").await
        }

        #[actix::test]
        async fn test_assignment_via_tuple_destructuring() {
            verify_exact_code_async_and_sync(r#"
            let (x, y, z) = (3, 5, 7)
            x + y + z
        "#, "15").await
        }

        #[actix::test]
        async fn test_euler_mascheroni_constant() {
            verify_exact_code_async_and_sync(r#"
                let x = 2 * γ
                x
            "#, "1.1544313298030657").await
        }

        #[actix::test]
        async fn test_euler_number() {
            verify_exact_code_async_and_sync(r#"
                let n = 2 * 𝑒
                n
            "#, "5.43656365691809").await
        }

        #[actix::test]
        async fn test_golden_ratio() {
            verify_exact_code_async_and_sync(r#"
                let g = 2 * φ
                g
            "#, "3.23606797749979").await
        }

        #[actix::test]
        async fn test_pi() {
            verify_exact_code_async_and_sync(r#"
                let r = 5
                let c = 2 * π * r
                c
            "#, "31.41592653589793").await
        }

        #[test]
        fn test_basic_state_manipulation() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_value_with(interpreter, "x = 5", Boolean(true));
            interpreter = verify_exact_value_with(interpreter, "-x", Number(I64Value(-5)));
            interpreter = verify_exact_value_with(interpreter, "x = x + 1", Boolean(true));
            interpreter = verify_exact_value_with(interpreter, "x", Number(I64Value(6)));
            interpreter = verify_exact_value_with(interpreter, "x < 7", Boolean(true));
            interpreter = verify_exact_value_with(interpreter, "x = x ** 2", Boolean(true));
            interpreter = verify_exact_value_with(interpreter, "x", Number(F64Value(36.)));
            interpreter = verify_exact_value_with(interpreter, "x / 0", Number(NaNValue));
            interpreter = verify_exact_value_with(interpreter, "x = x - 1", Boolean(true));
            interpreter = verify_exact_value_with(interpreter, "x % 5", Number(F64Value(0.)));
            interpreter = verify_exact_value_with(interpreter, "x < 35", Boolean(false));
            verify_exact_value_with(interpreter, "x >= 35", Boolean(true));
        }

        #[test]
        fn test_colon_colon_capture() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_table_with(interpreter, r#"
                stocks = []
                stocks<::push({ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 })
                stocks<::push({ symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 })
                stocks<::push({ symbol: "TIX", exchange: "NASDAQ", last_sale: 22.33 })
                stocks
            "#, vec![
                "|------------------------------------|",
                "| id | symbol | exchange | last_sale |",
                "|------------------------------------|",
                "| 0  | ABC    | AMEX     | 12.49     |",
                "| 1  | BOOM   | NYSE     | 56.88     |",
                "| 2  | TIX    | NASDAQ   | 22.33     |",
                "|------------------------------------|"]);

            let _interpreter = verify_exact_table_with(interpreter, r#"
                stocks<::pop()
                stocks
            "#, vec![
                "|------------------------------------|",
                "| id | symbol | exchange | last_sale |",
                "|------------------------------------|",
                "| 0  | ABC    | AMEX     | 12.49     |",
                "| 1  | BOOM   | NYSE     | 56.88     |",
                "|------------------------------------|"]);
        }
    }

    /// bitwise tests
    #[cfg(test)]
    mod bitwise_tests {
        use crate::test_util::verify_exact_code_async_and_sync;

        #[actix::test]
        async fn test_bitwise_and() {
            verify_exact_code_async_and_sync(r#"
                0b1101 & 0b0111
            "#, "5"
            ).await;
        }

        #[actix::test]
        async fn test_bitwise_or() {
            verify_exact_code_async_and_sync(r#"
                0b1101 | 0b0110
            "#, "15"
            ).await;
        }

        #[actix::test]
        async fn test_bitwise_shl() {
            verify_exact_code_async_and_sync(r#"
                0b1101 << 0b0010
            "#, "52"
            ).await;
        }

        #[actix::test]
        async fn test_bitwise_shr() {
            verify_exact_code_async_and_sync(r#"
                0b1101 >> 0b0010
            "#, "3"
            ).await;
        }

        #[actix::test]
        async fn test_bitwise_xor() {
            verify_exact_code_async_and_sync(r#"
                0b1101 ^ 0b0111
            "#, "10"
            ).await;
        }

    }

    /// Coalesce tests
    #[cfg(test)]
    mod coalesce_tests {
        use crate::test_util::verify_exact_code_async_and_sync;

        #[actix::test]
        async fn test_coalesce_not_null_or_undefined() {
            verify_exact_code_async_and_sync(r#"
                "Hello" ? "it was null or undefined"
            "#, "\"Hello\""
            ).await;
        }

        #[actix::test]
        async fn test_coalesce_null() {
            verify_exact_code_async_and_sync(r#"
                null ? "it was null or undefined"
            "#, "\"it was null or undefined\""
            ).await;
        }

        #[actix::test]
        async fn test_coalesce_undefined() {
            verify_exact_code_async_and_sync(r#"
                undefined ? "it was null or undefined"
            "#, "\"it was null or undefined\""
            ).await;
        }

        // Error::new('Boom!')
        #[actix::test]
        async fn test_coalesce_error() {
            verify_exact_code_async_and_sync(r#"
                (Error::new()) !? "An error occurred"
            "#, "\"An error occurred\""
            ).await;
        }

        #[actix::test]
        async fn test_coalesce_thrown_error() {
            verify_exact_code_async_and_sync(r#"
                (throw "Boom!") !? "An error occurred"
            "#, "\"An error occurred\""
            ).await;
        }

        #[actix::test]
        async fn test_coalesce_no_thrown_error() {
            verify_exact_code_async_and_sync(r#"
                "No problem" !? "An error occurred"
            "#, "\"No problem\""
            ).await;
        }
    }

    /// Condition expression tests
    #[cfg(test)]
    mod condition_expression_tests {
        use crate::test_util::verify_exact_code_async_and_sync;

        #[actix::test]
        async fn test_contains_with_array() {
            verify_exact_code_async_and_sync("[1, 5, 7, 9]::contains(5)", "true").await;
            verify_exact_code_async_and_sync("[1, 5, 7, 9]::contains(6)", "false").await;
        }

        #[actix::test]
        async fn test_contains_with_binary_string() {
            verify_exact_code_async_and_sync("0Bdeadbeefcafebabe::contains(0xbe)", "true").await;
            verify_exact_code_async_and_sync("0Bdeadbeefcafebabe::contains(0xcc)", "false").await;
        }

        #[actix::test]
        async fn test_contains_with_range_exclusive() {
            verify_exact_code_async_and_sync("(1..20)::contains(19)", "true").await;
            verify_exact_code_async_and_sync("(1..20)::contains(20)", "false").await;
        }

        #[actix::test]
        async fn test_contains_with_range_inclusive() {
            verify_exact_code_async_and_sync("(1..=20)::contains(20)", "true").await;
            verify_exact_code_async_and_sync("(1..=20)::contains(21)", "false").await;
        }

        #[actix::test]
        async fn test_contains_with_string() {
            verify_exact_code_async_and_sync(r#""🔥🎁💎🎉"::contains('🎁')"#, "true").await;
            verify_exact_code_async_and_sync(r#""🔥🎁💎🎉"::contains('🔮')"#, "false").await;
        }

        #[actix::test]
        async fn test_contains_with_structure() {
            verify_exact_code_async_and_sync(r#"{ first: "Tom", last: "Lane" }::contains("first")"#, "true").await;
            verify_exact_code_async_and_sync(r#"{ first: "Tom", last: "Lane" }::contains("middle")"#, "false").await;
        }

        #[actix::test]
        async fn test_in_array() {
            verify_exact_code_async_and_sync("8 in [1, 4, 2, 8, 5, 7]", "true").await;
            verify_exact_code_async_and_sync("3 in [1, 4, 2, 8, 5, 7]", "false").await;
        }

        #[actix::test]
        async fn test_in_binary_string() {
            verify_exact_code_async_and_sync("0xbe in 0Bdeadbeefcafebabe", "true").await;
            verify_exact_code_async_and_sync("0xcc in 0Bdeadbeefcafebabe", "false").await;
        }

        #[actix::test]
        async fn test_in_range_exclusive() {
            verify_exact_code_async_and_sync("19 in 1..20", "true").await;
            verify_exact_code_async_and_sync("20 in 1..20", "false").await;
        }

        #[actix::test]
        async fn test_in_range_inclusive() {
            verify_exact_code_async_and_sync("20 in 1..=20", "true").await;
            verify_exact_code_async_and_sync("21 in 1..=20", "false").await;
        }

        #[actix::test]
        async fn test_like() {
            verify_exact_code_async_and_sync("'Hello' like 'H*o'", "true").await;
            verify_exact_code_async_and_sync("'Hello' like 'H.ll.'", "true").await;
            verify_exact_code_async_and_sync("'Hello' like 'H%ll%'", "false").await;
        }

        #[actix::test]
        async fn test_matches_exact() {
            verify_exact_code_async_and_sync(r#"
                a = { first: "Tom", last: "Lane", scores: [82, 78, 99] }
                b = { first: "Tom", last: "Lane", scores: [82, 78, 99] }
                a matches b
            "#, "true").await;
        }

        #[actix::test]
        async fn test_matches_unordered() {
            verify_exact_code_async_and_sync(r#"
                a = { scores: [82, 78, 99], first: "Tom", last: "Lane" }
                b = { last: "Lane", first: "Tom", scores: [82, 78, 99] }
                a matches b
            "#, "true").await;
        }

        #[actix::test]
        async fn test_matches_not_match_1() {
            verify_exact_code_async_and_sync(r#"
                a = { first: "Tom", last: "Lane" }
                b = { first: "Jerry", last: "Lane" }
                a matches b
            "#, "false").await;
        }

        #[actix::test]
        async fn test_matches_not_match_2() {
            verify_exact_code_async_and_sync(r#"
                a = { key: "123", values: [1, 74, 88] }
                b = { key: "123", values: [1, 74, 88, 0] }
                a matches b
            "#, "false").await;
        }
    }

    /// Control-Flow tests
    #[cfg(test)]
    mod control_flow_tests {
        use crate::errors::Errors;
        use crate::interpreter::Interpreter;
        use crate::test_util::*;
        use crate::typed_values::TypedValue::*;

        #[actix::test]
        async fn test_for_each_byte_in_a_binary_string() {
            verify_exact_code_async_and_sync(r#"
                for b in 0Bdeadbeeffacebade yield b
            "#, "[0xde, 0xad, 0xbe, 0xef, 0xfa, 0xce, 0xba, 0xde]").await;
        }

        #[actix::test]
        async fn test_for_each_char_in_a_string() {
            verify_exact_code_async_and_sync(r#"
                for c in "Hello World" yield c
            "#, "['H', 'e', 'l', 'l', 'o', ' ', 'W', 'o', 'r', 'l', 'd']").await;
        }

        #[actix::test]
        async fn test_for_each_char_in_range() {
            verify_exact_code_async_and_sync(r#"
                for t in 'A'..'E' yield t
            "#, "['A', 'B', 'C', 'D']").await;
        }

        #[actix::test]
        async fn test_for_each_date_in_range_exclusive() {
            verify_exact_code_async_and_sync(r#"
                for t in 2025-08-21T23:14:31.000Z..2025-08-21T23:14:31.003Z yield t
            "#, "[2025-08-21T23:14:31.000Z, 2025-08-21T23:14:31.001Z, 2025-08-21T23:14:31.002Z]").await;
        }

        #[actix::test]
        async fn test_for_each_date_in_range_inclusive() {
            verify_exact_code_async_and_sync(r#"
                for t in 2025-08-21T23:14:31.000Z..=2025-08-21T23:14:31.002Z yield t
            "#, "[2025-08-21T23:14:31.000Z, 2025-08-21T23:14:31.001Z, 2025-08-21T23:14:31.002Z]").await;
        }

        #[actix::test]
        async fn test_for_each_number_in_range() {
            verify_exact_code_async_and_sync(r#"
                for t in 100..103 yield t
            "#, "[100, 101, 102]").await;
        }

        #[actix::test]
        async fn test_for_each_item_in_an_array() {
            verify_exact_code_async_and_sync(r#"
                for item in [1, 5, 6, 11, 17] yield item / 2.0
            "#, "[0.5, 2.5, 3, 5.5, 8.5]").await;
        }

        #[actix::test]
        async fn test_for_each_array_in_an_array() {
            verify_exact_code_async_and_sync(r#"
                for [a, b, c] in [[1, 5, 7], [6, 11, 15], [17, 21, 27]] 
                   yield (b - a) * c
            "#, "[28, 75, 108]").await;
        }

        #[actix::test]
        async fn test_for_each_tuple_in_an_array() {
            verify_exact_code_async_and_sync(r#"
                for (a, b) in [(1, 5), (6, 11), (17, 21)] 
                   yield a + b
            "#, "[6, 17, 38]").await;
        }

        #[actix::test]
        async fn test_for_each_pair_field_in_a_structure() {
            verify_exact_code_async_and_sync(r#"
                for (key, value) in {symbol: "ABC", exchange: "NYSE", last_sale: 23.77} 
                    yield (value, key)
            "#, r#"[("ABC", "symbol"), ("NYSE", "exchange"), (23.77, "last_sale")]"#).await;
        }
        
        #[actix::test]
        async fn test_for_each_row_field_in_a_table() {
            verify_exact_code_async_and_sync(r#"
                for row in [
                    {symbol: "ABC", exchange: "NYSE", last_sale: 23.77},
                    {symbol: "BOX", exchange: "AMEX", last_sale: 123.43},
                    {symbol: "GMO", exchange: "NASD", last_sale: 5.007}
                ]::to(Table) yield row.last_sale
            "#, r#"[23.77, 123.43, 5.007]"#).await;
        }

        #[actix::test]
        async fn test_for_each_row_in_a_table() {
            verify_exact_table_async_and_sync(r#"
                let results =
                    for row in ['apple', 'berry', 'kiwi', 'lime']::to(Table)
                        yield { fruit: row.value }
                results::to(Table)
            "#, vec![
                "|------------|",
                "| id | fruit |",
                "|------------|",
                "| 0  | apple |",
                "| 1  | berry |",
                "| 2  | kiwi  |",
                "| 3  | lime  |",
                "|------------|"]).await;
        }

        #[actix::test]
        async fn test_for_each_row_in_a_table_yield_row_id() {
            verify_exact_table_async_and_sync(r#"
                stocks =   
                    |--------------------------------|
                    | symbol | exchange  | last_sale |
                    |--------------------------------|
                    | GIF    | NYSE      | 11.75     |
                    | TRX    | NASDAQ    | 32.96     |
                    | SHMN   | OTCBB     | 5.02      |
                    | XCD    | OTCBB     | 1.37      |
                    | DRMQ   | OTHER_OTC | 0.02      |
                    | JTRQ   | OTHER_OTC | 0.0001    |
                    |--------------------------------|
                for row in stocks yield ('A' + __row_id__, 100 * (1 + __row_id__), row.last_sale)
            "#, vec![
                "|------------------------|",
                "| id | t0 | t1  | t2     |",
                "|------------------------|",
                "| 0  | A  | 100 | 11.75  |",
                "| 1  | B  | 200 | 32.96  |",
                "| 2  | C  | 300 | 5.02   |",
                "| 3  | D  | 400 | 1.37   |",
                "| 4  | E  | 500 | 0.02   |",
                "| 5  | F  | 600 | 0.0001 |",
                "|------------------------|"]).await;
        }

        #[actix::test]
        async fn test_for_each_unicode_char_in_a_string() {
            verify_exact_code_async_and_sync(r#"
                for c in "🔥🎁💎🎉" yield c
            "#, "['🔥', '🎁', '💎', '🎉']").await;
        }

        #[actix::test]
        async fn test_for_yield_iteration() {
            verify_exact_code_async_and_sync(r#"
                for(i = 0, i < 5, i = i + 1) yield i * 5
            "#, "[0, 5, 10, 15, 20]").await;
        }

        #[actix::test]
        async fn test_if_when_result_is_defined() {
            verify_exact_code_async_and_sync(r#"
                x = 7
                if(x > 5) "Yes"
            "#, "\"Yes\"").await;
        }

        #[actix::test]
        async fn test_if_else_expression() {
            verify_exact_code_async_and_sync(r#"
                x = 4
                if(x > 5) "Yes"
                else if(x < 5) "Maybe"
                else "No"
            "#, "\"Maybe\"").await;
        }

        #[actix::test]
        async fn test_ternary_if_function() {
            verify_exact_code_async_and_sync(r#"
                x = 4
                if(x > 5, "Yes", if(x < 5, "Maybe", "No"))
            "#, "\"Maybe\"").await;
        }

        #[actix::test]
        async fn test_include_expect_failure() {
            let mut interpreter = Interpreter::new();
            let result = interpreter.evaluate(r#"include 123"#);
            assert!(matches!(result, Err(..)));

            let result = interpreter.evaluate_async(r#"include 123"#).await;
            assert!(matches!(result, Err(..)));
        }
        
        #[actix::test]
        async fn test_match_case_1() {
            verify_exact_code_async_and_sync(r#"
                let code = 100
                match code {
                   100 => "Accepted"
                   n when n in 101..=104 => "Escalated"
                   n when n < 100 => "Pending"
                   n => "Rejected"
                }
            "#, "\"Accepted\"").await;
        }

        #[actix::test]
        async fn test_match_case_2() {
            verify_exact_code_async_and_sync(r#"
                let code = 102
                match code {
                   100 => "Accepted"
                   n when n in 101..=104 => "Escalated"
                   n when n < 100 => "Pending"
                   n => "Rejected"
                }
            "#, "\"Escalated\"").await;
        }

        #[actix::test]
        async fn test_match_case_3() {
            verify_exact_code_async_and_sync(r#"
                let code = 42
                match code {
                   100 => "Accepted"
                   n when n in 101..=104 => "Escalated"
                   n when n < 100 => "Pending"
                   n => "Rejected"
                }
            "#, "\"Pending\"").await;
        }

        #[actix::test]
        async fn test_match_case_4() {
            verify_exact_code_async_and_sync(r#"
                let code = 110
                match code {
                   100 => "Accepted"
                   n when n in 101..=104 => "Escalated"
                   n when n < 100 => "Pending"
                   n => "Rejected"
                }
            "#, "\"Rejected\"").await;
        }

        #[actix::test]
        async fn test_do_while_yield_loop() {
            verify_exact_code_async_and_sync(r#"
                let i = 0
                do {
                    yield (i := i + 1) * 5
                } while (i < 5)
            "#, "[5, 10, 15, 20, 25]").await;
        }
        
        #[actix::test]
        async fn test_while_yield_loop() {
            verify_exact_code_async_and_sync(r#"
                let i = 0
                while (i < 5) yield (i := i + 1) * 3
            "#, "[3, 6, 9, 12, 15]").await;
        }

        #[actix::test]
        async fn test_throw_error() {
            verify_exact_value_async(r#"
                throw("Boom!")
            "#, ErrorValue(Errors::Exact("Boom!".into()))).await;
        }
    }

    /// Data structure tests
    #[cfg(test)]
    mod data_structure_tests {
        use crate::test_util::verify_exact_code_async_and_sync;

        #[actix::test]
        async fn test_array_zip() {
            verify_exact_code_async_and_sync(r#"
                [1, 2, 3] <|> ['A', 'B', 'C']
            "#, r#"[(1, 'A'), (2, 'B'), (3, 'C')]"#).await;
        }

    }

    /// Declarative tests
    #[cfg(test)]
    mod declarative_tests {
        use crate::test_util::*;
        use crate::typed_values::TypedValue;

        #[actix::test]
        async fn test_as() {
            verify_exact_code_async_and_sync(r#"
                DateTime::new(1752122250353) as String
            "#, "\"2025-07-10T04:37:30.353Z\"").await;
        }

        #[actix::test]
        async fn test_list_files() {
            verify_exact_value_where(r#"
                ls where is_directory is true
            "#, |v| matches!(v, TypedValue::TableValue(..)));

            verify_exact_value_where_async(r#"
                ls where is_directory is true
            "#, |v| matches!(v, TypedValue::TableValue(..))).await;
        }

        #[actix::test]
        async fn test_once_async() {
            verify_exact_code_and_inferred_type(r#"
                let y = 0
                for x in 0..5 {
                    once { y += 1 }
                    yield x * y
                }
            "#, "[0, 1, 2, 3, 4]", "Array(i64)");

            verify_exact_code_async_and_sync(r#"
                let answer = true
                for x in 0..4 {
                    answer = !answer
                    once yield answer
                    yield answer
                }
            "#, "[false, false, true, false, true]").await;
        }

        #[test]
        fn test_whenever_is_triggered() {
            verify_exact_code_and_inferred_type(r#"
                // Executes the block at the moment the condition becomes true.
                let (x, y) = (1, 0)
                whenever x == 0 {
                    x = x + 1
                    y = y + 1
                }
                x = x - 1
                x + y
            "#, "2", "i64");
        }

        #[test]
        fn test_whenever_is_not_triggered_initially() {
            verify_exact_code_and_inferred_type(r#"
                // The block will be executed after the second assignment.
                let (x, y) = (1, 0)
                whenever x == 0 || y == 0 {
                    x = x + 1
                    y = y + 1
                }
                let (x, y) = (2, 3)
                x + y
            "#, "5", "i64");
        }

        #[test]
        fn test_whenever_is_not_triggered() {
            verify_exact_code_and_inferred_type(r#"
                // The block will not be executed if the condition is already true.
                let (x, y) = (1, 0)
                whenever x == 0 || y == 0 {
                    x = x + 1
                    y = y + 1
                }
                x + y
            "#, "1", "i64");
        }

        #[test]
        fn test_with_blob_store_len() {
            verify_exact_code_and_inferred_type(r#"
                blobs::create("interpreter.with.len_blocking") with bs -> {
                    let id = bs::append("Hello World")
                    bs::len()
                }
            "#, "70", "i64");
        }

        #[actix::test]
        async fn test_with_blob_store_read() {
            verify_exact_code_async(r#"
                blobs::create("interpreter.with.read") with bs -> {
                    let id = bs::append("Hello World")
                    bs::read(id)
                }
            "#, "\"Hello World\"").await;
        }
    }

    /// Demo tests
    #[cfg(test)]
    mod demo_tests {
        use crate::interpreter::Interpreter;
        use crate::test_util::{verify_exact_code_with, verify_exact_table_with};

        #[test]
        fn test_demo_blackjack() {
            // deck = tables::load("games.blackjack.deck")
            // player = tables::load("games.blackjack.player")
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_code_with(interpreter, r#"
                use agg, tables, oxide, util
                
                // define a type to represent a card deck/hand
                Cards = Table(face: String(2), suit: String(2))
                
                // create the card faces & suits
                faces = select face: value from ((2..=10)::map(n -> n::to(String)) ++ ["J", "Q", "K", "A"])::to(Table)
                suits = select suit: value from ["♥️", "♦️", "♣️", "♠️"]::to(Table)
                
                // create the deck, player-hand and dealer-hand
                deck = Cards::new::save_as("games.blackjack.deck")
                player = Cards::new::save_as("games.blackjack.player")
                dealer = Cards::new::save_as("games.blackjack.dealer")
            "#, "true");

            // verify the `suits` collection
            interpreter = verify_exact_table_with(interpreter, r#"
                suits
            "#, vec![
                "|-----------|", 
                "| id | suit |", 
                "|-----------|", 
                "| 0  | ♥️   |", 
                "| 1  | ♦️   |", 
                "| 2  | ♣️   |", 
                "| 3  | ♠️   |", 
                "|-----------|"]);
            
            interpreter = verify_exact_code_with(interpreter, r#"
                fn show_title() -> {
                  println(
                        "| 👑BLACKJACK DEMO v0.1👑
                         |        _             _                                 _
                         |  _ __ | | __ _ _   _(_)_ __   __ _    ___ __ _ _ __ __| |___
                         | | '_ \| |/ _` | | | | | '_ \ / _` |  / __/ _` | '__/ _` / __|
                         | | |_) | | (_| | |_| | | | | | (_| | | (_| (_| | | | (_| \__ \
                         | | .__/|_|\__,_|\__, |_|_| |_|\__, |  \___\__,_|_|  \__,_|___/
                         | |_|            |___/         |___/
                         |"::strip_margin('|'))
                }
            "#, "true");

            interpreter = verify_exact_code_with(interpreter, r#"
                fn card_face_up(face, suit) -> {
                    let faceL = if(face::len() < 2, face + " ", face)
                    let faceR = if(face::len() < 2, " " + face, face);
                    [
                      "┌─────────┐",
                      "│ %s   %s │"::sprintf(faceL, suit),
                      "│         │",
                      "│   %s    │"::sprintf(suit),
                      "│         │",
                      "│ %s   %s │"::sprintf(suit, faceR),
                      "└─────────┘"
                    ]
                }
            "#, "true");

            // verify the `card_face_up` function
            interpreter = verify_exact_table_with(interpreter, r#"
                card_face_up("A", "♥️")
            "#, vec![
                "|------------------|",
                "| id | value       |",
                "|------------------|",
                "| 0  | ┌─────────┐ |",
                "| 1  | │ A    ♥️ │ |",
                "| 2  | │         │ |",
                "| 3  | │   ♥️    │ |",
                "| 4  | │         │ |",
                "| 5  | │ ♥️    A │ |",
                "| 6  | └─────────┘ |",
                "|------------------|"]);            
            
            interpreter = verify_exact_code_with(interpreter, r#"
                fn card_face_down() -> [
                    "┌─────────┐",
                    "│▒░▒░▒░▒░▒│",
                    "│░▒░▒░▒░▒░│",
                    "│▒░▒░▒░▒░▒│",
                    "│░▒░▒░▒░▒░│",
                    "│▒░▒░▒░▒░▒│",
                    "└─────────┘"
                ]
            "#, "true");

            interpreter = verify_exact_code_with(interpreter, r#"
                fn compute_score(hand, aceScore: i64) -> {
                    let rows =
                        select
                            score: sum(match face {
                                "A" => aceScore
                                face when face::to(i64) in 2..9 => face::to(i64)
                                _ => 10
                            })
                        from hand;
                    let row = rows[0];
                    row.score
                }
            "#, "true");

            interpreter = verify_exact_code_with(interpreter, r#"
                fn compute_card_score(hand) -> {
                    let v11 = compute_score(hand, 11)
                    if(v11 <= 21, v11, compute_score(hand, 1))
                }
            "#, "true");

            interpreter = verify_exact_code_with(interpreter, r#"
                fn hit(hand) -> {
                    deck::shuffle()
                    card <~ deck
                    card ~> hand
                }
            "#, "true");

            interpreter = verify_exact_code_with(interpreter, r#"
                fn show_hand(hand) -> {
                    let lines = []
                    isVisible = true
                    for card in hand {
                        let _card = if(isVisible is true, card_face_up(card.face, card.suit), card_face_down())
                        lines = if(lines::len() == 0, _card, (lines <|> _card)::map(a -> a::to_array::join(" ")))
                    }
                
                    // add a face down card in the last position
                    if (hand == dealer) {
                        _card = card_face_down()
                        lines = if(lines::len() == 0, _card, (lines <|> _card)::map(a -> a::to_array::join(" ")))
                    }
                
                    // write to STDOUT
                    println((lines::join('\n')) + '\n')
                }
            "#, "true");

            interpreter = verify_exact_code_with(interpreter, r#"
                fn create_gamestate() -> {
                    {
                        name: (__realname__::split(" "))[0],
                        money: 1000.0,
                        bet: 25.0,
                        bet_factor: 1.0,
                        dealer_score: 0,
                        player_score: 0,
                        level: 1,
                        loop: 0,
                        is_jousting: true,
                        is_alive: true,
                        show_cards: false
                    }
                }
            "#, "true");

            interpreter = verify_exact_code_with(interpreter, r#"
                fn game_over(gamestate) -> {
                    update_gamestate(gamestate)
                    if (gamestate.dealer_score == gamestate.player_score)
                        round_completed(gamestate, '🤝Draw.', 0)
                    else if (gamestate.player_score == 21)
                        round_completed(gamestate, '👍You Win!! - Player BlackJack!', 1.5 * bet)
                    else if (gamestate.dealer_score == 21)
                        round_completed(gamestate, '👎You Lose - Dealer BlackJack!', -bet)
                    else if (gamestate.dealer_score > 21)
                        round_completed(gamestate, '👍You Win!! - Dealer Busts: %d'::sprintf(gamestate.dealer_score), bet)
                    else if (gamestate.player_score > 21)
                        round_completed(gamestate, '👎You Lose - Player Busts: %d'::sprintf(gamestate.player_score), -bet)
                    else if (gamestate.player_score > gamestate.dealer_score)
                        round_completed(gamestate, '👍You Win!! - %d vs. %d'::sprintf(gamestate.player_score, gamestate.dealer_score), -bet)
                    else round_completed(gamestate, '👎You Lose - %d vs. %d'::sprintf(gamestate.dealer_score, gamestate.player_score), -bet)
                    gamestate.level += 1
                }
            "#, "true");

            interpreter = verify_exact_code_with(interpreter, r#"
                fn update_gamestate(gamestate) -> {
                    gamestate.dealer_score = (compute_card_score(dealer) ? 0)
                    gamestate.player_score = (compute_card_score(player) ? 0)
                    gamestate
                }
            "#, "true");

            interpreter = verify_exact_code_with(interpreter, r#"
                fn prompt_user(gamestate) -> {
                    println("Choose %s[H]it, [S]tand or [Q]uit? "::sprintf(if(gamestate.loop == 0, "[D]ouble-down, ", "")))
                    choice = io::stdin()::trim::to_uppercase
                    if ((choice::starts_with("D") is true) && (loop == 0)) gamestate.bet_factor = 2.0
                    else if(choice::starts_with("H")) { hit(player); gamestate.show_cards = true }
                    else if(choice::starts_with("Q")) { gamestate.is_jousting = false; gamestate.is_alive = false }
                    else if(choice::starts_with("S")) gamestate.is_jousting = false
                }
            "#, "true");

            interpreter = verify_exact_code_with(interpreter, r#"
                fn dealer_intelligence(gamestate, finish) -> {
                    update_gamestate(gamestate)
                    let modified = false
                    if((gamestate.player_score <= 21) && (gamestate.dealer_score < gamestate.player_score)) {
                        let cost =
                            if (finish) while (gamestate.player_score > gamestate.dealer_score) hit(dealer)
                            else if(gamestate.player_score > gamestate.dealer_score) hit(dealer)
                        modified = modified || (cost.inserted > 0)
                    }
                    modified
                }
            "#, "true");

            interpreter = verify_exact_code_with(interpreter, r#"
                fn round_completed(gamestate, message: String, betDelta: f64) -> {
                    println('%s\n'::sprintf(message))
                    gamestate.money += gamestate.bet_factor * betDelta
                }
            "#, "true");

            interpreter = verify_exact_code_with(interpreter, r#"
                fn show_game_table(gamestate) -> {
                    println("DEALER - %d/21"::sprintf(gamestate.dealer_score))
                    show_hand(dealer)
                    println("YOU (%s) - %s%d/21"::sprintf(
                        gamestate.name,
                        if(gamestate.bet_factor == 2.0, "2x ", ""),
                        gamestate.player_score))
                    show_hand(player)
                }
            "#, "true");

            interpreter = verify_exact_code_with(interpreter, r#"
                fn show_separator(gamestate) -> {
                    separator = ("※" * 120)
                    println('\n' + separator)
                    println(" Player: %s \t Credit: $%.2f \t Bet: $%.2f \t Round: %d"::sprintf(
                         gamestate.name, gamestate.money, gamestate.bet, gamestate.level))
                    println(separator + '\n')
                }
            "#, "true");

            // let tv = interpreter.evaluate("hand").unwrap();
            // for s in make_lines_from_table(tv) {
            //     println!("{s}")
            // }

            interpreter = verify_exact_code_with(interpreter, r#"
                show_title()
            
                // setup the basic game state
                gamestate = create_gamestate();
                
                // reset and shuffle the deck
                (faces * suits)::to(Table) ~>> deck
            "#, "52");

            interpreter = verify_exact_code_with(interpreter, r#"
                deck::shuffle()
            "#, "true");
            
            interpreter = verify_exact_code_with(interpreter, r#"
                // put some cards into the hands of the player and dealer
                for hand in [dealer, player] {
                    hand::resize(0)
                    hit(hand)
                }
            "#, "1");

            interpreter = verify_exact_code_with(interpreter, r#"
                gamestate.is_jousting = true
                gamestate.bet_factor = 1.0
            "#, "true");

            let _interpreter = verify_exact_code_with(interpreter, r#"
                show_separator(gamestate)
            "#, "true");
        }
    }

    /// element_at tests
    #[cfg(test)]
    mod element_at_tests {
        use crate::data_types::DataType::{CharType, FixedSizeType, NumberType, StringType, StructureType};
        use crate::number_kind::NumberKind::F64Kind;
        use crate::parameter::Parameter;
        use crate::test_util::{verify_data_type, verify_exact_code_async_and_sync};

        #[actix::test]
        async fn test_infer_element_at_array() {
            verify_exact_code_async_and_sync(r#"
                let arr = [1, 4, 2, 8, 5, 7]
                arr[1]
            "#, "4").await;
        }

        #[actix::test]
        async fn test_infer_element_at_bytes() {
            verify_exact_code_async_and_sync(r#"
                let bytes = 0B576879206469642074686520636869636b656e202e2e2e
                bytes[0]
            "#, "0x57").await;
        }

        #[actix::test]
        async fn test_infer_element_at_enum() {
            verify_exact_code_async_and_sync(r#"
                let exchange =  Enum(AMEX, NASDAQ, NYSE, OTCBB)::new(1)
                exchange[5]
            "#, "'Q'").await;
        }

        #[test]
        fn test_infer_element_at_string() {
            verify_data_type(r#"
                let s = "Why did the chicken ..."
                s[0]
            "#, CharType);
        }

        #[test]
        fn test_infer_element_at_table() {
            verify_data_type(r#"
                let stocks =
                    |--------------------------------|
                    | symbol | exchange  | last_sale |
                    |--------------------------------|
                    | TRX    | NASDAQ    | 32.96     |
                    |--------------------------------|
                stocks[0]
            "#, StructureType(vec![
                Parameter::new("symbol", FixedSizeType(StringType.into(), 3)),
                Parameter::new("exchange", FixedSizeType(StringType.into(), 6)),
                Parameter::new("last_sale", NumberType(F64Kind)),
            ]));
        }

    }

    /// Function tests
    #[cfg(test)]
    mod function_tests {
        use crate::interpreter::Interpreter;
        use crate::test_util::*;

        #[actix::test]
        async fn test_function_lambda_with_inferred_types() {
            verify_exact_code_async_and_sync(r#"
                let product = (a, b) -> a * b
                product(2, 5)
            "#, "10").await
        }

        #[actix::test]
        async fn test_function_lambda_with_parameter_types() {
            verify_exact_code_async_and_sync(r#"
                let product = (a: i64, b: i64) -> a * b
                product(3, 3)
            "#, "9").await
        }

        #[actix::test]
        async fn test_function_lambda_with_parameter_types_and_return_type() {
            verify_exact_code_async_and_sync(r#"
                let product = (a: i64, b: i64): i64 -> a * b
                product(3, 9)
            "#, "27").await
        }

        #[actix::test]
        async fn test_function_named_with_inferred_types() {
            verify_exact_code_async_and_sync(r#"
                fn product(a, b) -> a * b
                product(4, 5)
            "#, "20").await
        }

        #[actix::test]
        async fn test_function_named_with_parameter_types() {
            verify_exact_code_async_and_sync(r#"
                fn product(a: i64, b: i64) -> a * b
                product(4, 9)
            "#, "36").await
        }

        #[actix::test]
        async fn test_function_named_with_parameter_types_and_return_type() {
            verify_exact_code_async_and_sync(r#"
                fn product(a: i64, b: i64): i64 -> a * b
                product(5, 6)
            "#, "30").await
        }

        #[ignore]
        #[actix::test]
        async fn test_function_recursion_1() {
            verify_exact_code_async_and_sync(r#"
                let f = (n: i64) -> if(n <= 1) 1 else n * f(n - 1)
                f(5)
            "#, "120").await
        }

        #[ignore]
        #[actix::test]
        async fn test_function_recursion_2() {
            verify_exact_code_async_and_sync(r#"
                let f = n -> if(n <= 1, 1, n * f(n - 1))
                f(6)
            "#, "720").await
        }

        #[actix::test]
        async fn test_functional_pipeline() {
            verify_exact_code_async_and_sync(r#"
                let f = s -> s::reverse
                "Hello" |> f
            "#, "\"olleH\"").await;
        }

        #[actix::test]
        async fn test_functional_pipeline_with_destructure() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_code_with_async(interpreter, r#"
                fn add(a, b) -> a + b
            "#, "true").await;

            interpreter = verify_exact_code_with_async(interpreter, r#"
                fn inverse(a) -> 1.0 / a
            "#, "true").await;

            interpreter = verify_exact_code_with_async(interpreter, r#"
                (2, 3) |>> add
            "#, "5").await;

            verify_exact_code_with_async(interpreter, r#"
                (2, 3) |>> add |> inverse
            "#, "0.2").await;
        }

        #[actix::test]
        async fn test_functional_sugar_with_filter_map() {
            verify_exact_code_async_and_sync(r#"
                let arr = [1, 2, 3, 4]
                arr
                    ::filter(x -> (x % 2) == 0)
                    ::map(x -> x * 10)
            "#, "[20, 40]").await;
        }

        #[actix::test]
        async fn test_functional_sugar() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_code_with(interpreter, r#"
                let kb = n -> n * 1024
                let mb = n -> kb(n) * 1024
                let gb = n -> mb(n) * 1024
            "#, "true");
            interpreter = verify_exact_code_with(interpreter, r#"
                3.2:::kb
            "#, "3276.8");
            interpreter = verify_exact_code_with(interpreter, r#"
                3.2:::mb
            "#, "3355443.2");
            let _interpreter = verify_exact_code_with(interpreter, r#"
                3.2:::gb
            "#, "3435973836.8");
        }

        #[actix::test]
        async fn test_functional_sugar_with_package_fn() {
            verify_exact_code_async_and_sync(r#"
                backwards(a) -> a::reverse()
                "Hello World":::backwards()
            "#, "\"dlroW olleH\"").await;
        }

        #[actix::test]
        async fn test_platform_function_execute_and_capture() {
            verify_exact_table_async_and_sync(r#"
                stocks = []
                stocks<::push({ symbol: "YUMI", exchange: "NYSE", last_sale: 13.82 })
                stocks<::push({ symbol: "DTXM", exchange: "NYSE", last_sale: 51.24 })
                stocks<::to(Table)
            "#, vec![
                "|------------------------------------|",
                "| id | symbol | exchange | last_sale |",
                "|------------------------------------|",
                "| 0  | YUMI   | NYSE     | 13.82     |",
                "| 1  | DTXM   | NYSE     | 51.24     |",
                "|------------------------------------|"]).await;
        }

        #[actix::test]
        async fn test_user_function_execute_and_capture() {
            verify_exact_code_async_and_sync(r#"
                fn multiply(n) -> n * 2
                value = 8
                value<:::multiply()
            "#, "16").await;
        }
    }

    /// Package "infix" tests
    #[cfg(test)]
    mod infix_tests {
        use crate::interpreter::Interpreter;
        use crate::test_util::{verify_exact_code_async_and_sync, verify_exact_code_with_async, verify_exact_unwrapped_with_async};

        #[actix::test]
        async fn test_infix_field_access() {
            verify_exact_code_async_and_sync(r#"
                let stock = { symbol: "TED", exchange: "AMEX", last_sale: 13.37 }
                stock.last_sale
            "#, "13.37").await;
        }

        #[actix::test]
        async fn test_infix_field_assignment() {
            verify_exact_code_async_and_sync(r#"
                let stock = { symbol: "RLPH", exchange: "NYSE", last_sale: 13.37 }
                stock.last_sale = 35.78
                stock.last_sale
            "#, "35.78").await;
        }

        #[actix::test]
        async fn test_infix_function_call() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_code_with_async(interpreter,r#"
            stock = Struct(
                symbol: String(8) = "ABC",
                exchange: String(8) = "NYSE",
                last_sale: f64 = 23.67
            )::new
            mod stock {
                fn contains_symbol(symbol) -> symbol is self.symbol
            }
            "#, "true").await;

            // verify the structure
            interpreter = verify_exact_unwrapped_with_async(interpreter,r#"
                stock
            "#, r#"{"contains_symbol":{"code":"symbol == self.symbol","params":[{"data_type":"RuntimeResolvedType","default_value":"Null","name":"symbol"}],"returns":"Boolean"},"exchange":"NYSE","last_sale":23.67,"symbol":"ABC"}"#
            ).await;

            // verify the negative case
            interpreter = verify_exact_code_with_async(interpreter,r#"
                stock.contains_symbol("XYZ")
            "#, "false").await;

            // verify the positive case
            let _interpreter = verify_exact_code_with_async(interpreter,r#"
                stock.contains_symbol("ABC")
            "#, "true").await;
        }
    }
    
}