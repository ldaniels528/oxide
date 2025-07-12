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
    use crate::numbers::Numbers::*;
    use crate::typed_values::TypedValue::*;

    /// Assignment tests
    #[cfg(test)]
    mod assignment_tests {
        use crate::interpreter::Interpreter;
        use crate::numbers::Numbers::{F64Value, I64Value, NaNValue};
        use crate::testdata::{verify_exact_code, verify_exact_value_with};
        use crate::typed_values::TypedValue::{Boolean, Number};

        #[test]
        fn test_assignment_via_array_destructuring() {
            verify_exact_code(r#"
            let [a, b, c, d] = [3, 5, 7, 9]
            a + b + c + d
        "#, "24")
        }

        #[test]
        fn test_assignment_via_tuple_destructuring() {
            verify_exact_code(r#"
            let (x, y, z) = (3, 5, 7)
            x + y + z
        "#, "15")
        }

        #[test]
        fn test_euler_mascheroni_constant() {
            verify_exact_code(r#"
                let x = 2 * γ
                x
            "#, "1.1544313298030657")
        }

        #[test]
        fn test_euler_number() {
            verify_exact_code(r#"
                let n = 2 * 𝑒
                n
            "#, "5.43656365691809")
        }

        #[test]
        fn test_golden_ratio() {
            verify_exact_code(r#"
                let g = 2 * φ
                g
            "#, "3.23606797749979")
        }

        #[test]
        fn test_pi() {
            verify_exact_code(r#"
                let r = 5
                let c = 2 * π * r
                c
            "#, "31.41592653589793")
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
            interpreter = verify_exact_value_with(interpreter, "x >= 35", Boolean(true));
        }
    }

    /// Coalesce tests
    #[cfg(test)]
    mod coalesce_tests {
        use crate::testdata::verify_exact_code;

        #[test]
        fn test_coalesce_not_null_or_undefined() {
            verify_exact_code(r#"
                "Hello" ? "it was null or undefined"
            "#, "\"Hello\"");
        }

        #[test]
        fn test_coalesce_null() {
            verify_exact_code(r#"
                null ? "it was null or undefined"
            "#, "\"it was null or undefined\"");
        }

        #[test]
        fn test_coalesce_undefined() {
            verify_exact_code(r#"
                undefined ? "it was null or undefined"
            "#, "\"it was null or undefined\"");
        }

        // Error::new('Boom!')
        #[test]
        fn test_coalesce_error() {
            verify_exact_code(r#"
                (Error::new()) !? "An error occurred"
            "#, "\"An error occurred\"");
        }

        #[test]
        fn test_coalesce_thrown_error() {
            verify_exact_code(r#"
                (throw "Boom!") !? "An error occurred"
            "#, "\"An error occurred\"");
        }

        #[test]
        fn test_coalesce_no_thrown_error() {
            verify_exact_code(r#"
                "No problem" !? "An error occurred"
            "#, "\"No problem\"");
        }
    }

    /// Condition expression tests
    #[cfg(test)]
    mod condition_expression_tests {
        use crate::testdata::verify_exact_value;
        use crate::typed_values::TypedValue::Boolean;

        #[test]
        fn test_contains_with_array() {
            verify_exact_value("[1, 5, 7, 9] contains 5", Boolean(true));
            verify_exact_value("[1, 5, 7, 9] contains 6", Boolean(false));
        }

        #[test]
        fn test_contains_with_binary_string() {
            verify_exact_value("0Bdeadbeefcafebabe contains 0xbe", Boolean(true));
            verify_exact_value("0Bdeadbeefcafebabe contains 0xcc", Boolean(false));
        }

        #[test]
        fn test_contains_with_range_exclusive() {
            verify_exact_value("(1..20) contains 19", Boolean(true));
            verify_exact_value("(1..20) contains 20", Boolean(false));
        }

        #[test]
        fn test_contains_with_range_inclusive() {
            verify_exact_value("(1..=20) contains 20", Boolean(true));
            verify_exact_value("(1..=20) contains 21", Boolean(false));
        }

        #[test]
        fn test_contains_with_string() {
            verify_exact_value(r#""🔥🎁💎🎉" contains '🎁'"#, Boolean(true));
            verify_exact_value(r#""🔥🎁💎🎉" contains '🔮'"#, Boolean(false));
        }

        #[test]
        fn test_contains_with_structure() {
            verify_exact_value(r#"{ first: "Tom", last: "Lane" } contains "first""#, Boolean(true));
            verify_exact_value(r#"{ first: "Tom", last: "Lane" } contains "middle""#, Boolean(false));
        }

        #[test]
        fn test_in_array() {
            verify_exact_value("8 in [1, 4, 2, 8, 5, 7]", Boolean(true));
            verify_exact_value("3 in [1, 4, 2, 8, 5, 7]", Boolean(false));
        }

        #[test]
        fn test_in_binary_string() {
            verify_exact_value("0xbe in 0Bdeadbeefcafebabe", Boolean(true));
            verify_exact_value("0xcc in 0Bdeadbeefcafebabe", Boolean(false));
        }

        #[test]
        fn test_in_range_exclusive() {
            verify_exact_value("19 in 1..20", Boolean(true));
            verify_exact_value("20 in 1..20", Boolean(false));
        }

        #[test]
        fn test_in_range_inclusive() {
            verify_exact_value("20 in 1..=20", Boolean(true));
            verify_exact_value("21 in 1..=20", Boolean(false));
        }

        #[test]
        fn test_like() {
            verify_exact_value("'Hello' like 'H*o'", Boolean(true));
            verify_exact_value("'Hello' like 'H.ll.'", Boolean(true));
            verify_exact_value("'Hello' like 'H%ll%'", Boolean(false));
        }

        #[test]
        fn test_matches_exact() {
            verify_exact_value(r#"
                a = { first: "Tom", last: "Lane", scores: [82, 78, 99] }
                b = { first: "Tom", last: "Lane", scores: [82, 78, 99] }
                a matches b
            "#, Boolean(true));
        }

        #[test]
        fn test_matches_unordered() {
            verify_exact_value(r#"
                a = { scores: [82, 78, 99], first: "Tom", last: "Lane" }
                b = { last: "Lane", first: "Tom", scores: [82, 78, 99] }
                a matches b
            "#, Boolean(true));
        }

        #[test]
        fn test_matches_not_match_1() {
            verify_exact_value(r#"
                a = { first: "Tom", last: "Lane" }
                b = { first: "Jerry", last: "Lane" }
                a matches b
            "#, Boolean(false));
        }

        #[test]
        fn test_matches_not_match_2() {
            verify_exact_value(r#"
                a = { key: "123", values: [1, 74, 88] }
                b = { key: "123", values: [1, 74, 88, 0] }
                a matches b
            "#, Boolean(false));
        }
    }

    /// Control-Flow tests
    #[cfg(test)]
    mod control_flow_tests {
        use crate::errors::Errors;
        use crate::interpreter::Interpreter;
        use crate::testdata::*;
        use crate::typed_values::TypedValue::*;

        #[test]
        fn test_for_each_byte_in_a_binary_string() {
            verify_exact_code(r#"
                for b in 0Bdeadbeeffacebade yield b
            "#, "[0xde, 0xad, 0xbe, 0xef, 0xfa, 0xce, 0xba, 0xde]");
        }

        #[test]
        fn test_for_each_char_in_a_string() {
            verify_exact_code(r#"
                for c in "Hello World" yield c
            "#, "['H', 'e', 'l', 'l', 'o', ' ', 'W', 'o', 'r', 'l', 'd']");
        }

        #[test]
        fn test_for_each_unicode_char_in_a_string() {
            verify_exact_code(r#"
                for c in "🔥🎁💎🎉" yield c
            "#, "['🔥', '🎁', '💎', '🎉']");
        }

        #[test]
        fn test_for_each_item_in_an_array() {
            verify_exact_code(r#"
                for item in [1, 5, 6, 11, 17] yield item / 2.0
            "#, "[0.5, 2.5, 3, 5.5, 8.5]");
        }

        #[test]
        fn test_for_each_array_in_an_array() {
            verify_exact_code(r#"
                for [a, b, c] in [[1, 5, 7], [6, 11, 15], [17, 21, 27]] 
                   yield (b - a) * c
            "#, "[28, 75, 108]");
        }

        #[test]
        fn test_for_each_tuple_in_an_array() {
            verify_exact_code(r#"
                for (a, b) in [(1, 5), (6, 11), (17, 21)] 
                   yield a + b
            "#, "[6, 17, 38]");
        }

        #[test]
        fn test_for_each_pair_field_in_a_structure() {
            verify_exact_code(r#"
                for (key, value) in {symbol: "ABC", exchange: "NYSE", last_sale: 23.77} 
                    yield (value, key)
            "#, r#"[("ABC", "symbol"), ("NYSE", "exchange"), (23.77, "last_sale")]"#);
        }


        #[test]
        fn test_for_each_row_field_in_a_table() {
            verify_exact_code(r#"
                for row in [
                    {symbol: "ABC", exchange: "NYSE", last_sale: 23.77},
                    {symbol: "BOX", exchange: "AMEX", last_sale: 123.43},
                    {symbol: "GMO", exchange: "NASD", last_sale: 5.007}
                ]::to_table() yield row.last_sale
            "#, r#"[23.77, 123.43, 5.007]"#);
        }

        #[test]
        fn test_for_each_row_in_a_table() {
            verify_exact_table(r#"
                let results =
                    for row in ['apple', 'berry', 'kiwi', 'lime']::to_table()
                        yield { fruit: row.value }
                results::to_table()
            "#, vec![
                "|------------|",
                "| id | fruit |", 
                "|------------|", 
                "| 0  | apple |", 
                "| 1  | berry |", 
                "| 2  | kiwi  |", 
                "| 3  | lime  |", 
                "|------------|"]);
        }

        #[test]
        fn test_for_each_row_in_a_table_yield_row_id() {
            verify_exact_table(r#"
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
                for row in stocks yield (__row_id__, row.last_sale)
            "#, vec![
                "|------------------|", 
                "| id | value       |", 
                "|------------------|", 
                "| 0  | (0, 11.75)  |", 
                "| 1  | (1, 32.96)  |", 
                "| 2  | (2, 5.02)   |", 
                "| 3  | (3, 1.37)   |", 
                "| 4  | (4, 0.02)   |", 
                "| 5  | (5, 0.0001) |", 
                "|------------------|"]);
        }

        #[test]
        fn test_for_yield_iteration() {
            verify_exact_code(r#"
                for(i = 0, i < 5, i = i + 1) yield i * 5
            "#, "[0, 5, 10, 15, 20]");
        }

        #[test]
        fn test_if_when_result_is_defined() {
            verify_exact_code(r#"
                x = 7
                if(x > 5) "Yes"
            "#, "\"Yes\"");
        }

        #[test]
        fn test_if_when_result_is_undefined() {
            verify_exact_code(r#"
                x = 4
                if(x > 5) "Yes"
            "#, "undefined");
        }

        #[test]
        fn test_if_else_expression() {
            verify_exact_code(r#"
                x = 4
                if(x > 5) "Yes"
                else if(x < 5) "Maybe"
                else "No"
            "#, "\"Maybe\"");
        }

        #[test]
        fn test_ternary_if_function() {
            verify_exact_code(r#"
                x = 4
                if(x > 5, "Yes", if(x < 5, "Maybe", "No"))
            "#, "\"Maybe\"");
        }
        
        #[test]
        fn test_match_case_1() {
            verify_exact_code(r#"
                let code = 100
                match code {
                   100 => "Accepted"
                   n when n in 101..=104 => "Escalated"
                   n when n < 100 => "Pending"
                   n => "Rejected"
                }
            "#, "\"Accepted\"");
        }

        #[test]
        fn test_match_case_2() {
            verify_exact_code(r#"
                let code = 102
                match code {
                   100 => "Accepted"
                   n when n in 101..=104 => "Escalated"
                   n when n < 100 => "Pending"
                   n => "Rejected"
                }
            "#, "\"Escalated\"");
        }

        #[test]
        fn test_match_case_3() {
            verify_exact_code(r#"
                let code = 42
                match code {
                   100 => "Accepted"
                   n when n in 101..=104 => "Escalated"
                   n when n < 100 => "Pending"
                   n => "Rejected"
                }
            "#, "\"Pending\"");
        }

        #[test]
        fn test_match_case_4() {
            verify_exact_code(r#"
                let code = 110
                match code {
                   100 => "Accepted"
                   n when n in 101..=104 => "Escalated"
                   n when n < 100 => "Pending"
                   n => "Rejected"
                }
            "#, "\"Rejected\"");
        }

        #[test]
        fn test_do_while_yield_loop() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_code_with(interpreter, "i = 0", "true");
            interpreter = verify_exact_code_with(interpreter, r#"
                do {
                    yield (i := i + 1) * 5
                } while (i < 5)
            "#, "[5, 10, 15, 20, 25]");
        }
        
        #[test]
        fn test_while_yield_loop() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_code_with(interpreter, "i = 0", "true");
            interpreter = verify_exact_code_with(interpreter, r#"
                while (i < 5) yield (i := i + 1) * 3
            "#, "[3, 6, 9, 12, 15]");
        }

        #[test]
        fn test_throw_error() {
            verify_exact_value(r#"
                throw("Boom!")
            "#, ErrorValue(Errors::Exact("Boom!".into())));
        }
    }

    /// Data structure tests
    #[cfg(test)]
    mod data_structure_tests {
        use crate::testdata::verify_exact_code;

        #[test]
        fn test_array_zip() {
            verify_exact_code(r#"
                [1, 2, 3] <|> ['A', 'B', 'C']
            "#, r#"[(1, 'A'), (2, 'B'), (3, 'C')]"#);
        }
        
    }

    /// Declarative tests
    #[cfg(test)]
    mod declarative_tests {
        use crate::testdata::{verify_exact_code, verify_exact_value_where};
        use crate::typed_values::TypedValue;

        #[test]
        fn test_list_files() {
            verify_exact_value_where(r#"
                ls where is_directory is true
            "#, |v| matches!(v, TypedValue::TableValue(..)));
        }

        #[test]
        fn test_whenever_is_triggered() {
            verify_exact_code(r#"
                // Executes the block at the moment the condition becomes true.
                let (x, y) = (1, 0)
                whenever x == 0 {
                    x = x + 1
                    y = y + 1
                }
                x = x - 1
                x + y
            "#, "2");
        }

        #[test]
        fn test_whenever_is_not_triggered_initially() {
            verify_exact_code(r#"
                // The block will be executed after the second assignment.
                let (x, y) = (1, 0)
                whenever x == 0 || y == 0 {
                    x = x + 1
                    y = y + 1
                }
                let (x, y) = (2, 3)
                x + y
            "#, "5");
        }

        #[test]
        fn test_whenever_is_not_triggered() {
            verify_exact_code(r#"
                // The block will not be executed if the condition is already true.
                let (x, y) = (1, 0)
                whenever x == 0 || y == 0 {
                    x = x + 1
                    y = y + 1
                }
                x + y
            "#, "1");
        }
    }

    /// Demo tests
    #[cfg(test)]
    mod demo_tests {
        use crate::interpreter::Interpreter;
        use crate::testdata::{verify_exact_code_with, verify_exact_table_with};

        //#[ignore]
        #[test]
        fn test_demo_blackjack() {
            // deck = nsd::load("games.blackjack.deck")
            // player = nsd::load("games.blackjack.player")
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_code_with(interpreter, r#"
                use agg, nsd, oxide, util
                
                // define a type to represent a card deck/hand
                Cards = Table(face: String(2), suit: String(2))
                
                // create the card faces & suits
                faces = select face: value from ((2..=10)::map(n -> n::to_string()) ++ ["J", "Q", "K", "A"])::to_table()
                suits = select suit: value from ["♥️", "♦️", "♣️", "♠️"]::to_table()
                
                // create the deck, player-hand and dealer-hand
                deck = nsd::save("games.blackjack.deck", Cards::new())
                player = nsd::save("games.blackjack.player", Cards::new())
                dealer = nsd::save("games.blackjack.dealer", Cards::new())
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
                sprintf("│ %s   %s │", faceL, suit),
                        "│         │",
                sprintf("│   %s    │", suit),
                        "│         │",
                sprintf("│ %s   %s │", suit, faceR),
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
                                face when face:::to(i64) in 2..9 => face:::to(i64)
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
                        round_completed(gamestate, sprintf('👍You Win!! - Dealer Busts: %d', gamestate.dealer_score), bet)
                    else if (gamestate.player_score > 21)
                        round_completed(gamestate, sprintf('👎You Lose - Player Busts: %d', gamestate.player_score), -bet)
                    else if (gamestate.player_score > gamestate.dealer_score)
                        round_completed(gamestate, sprintf('👍You Win!! - %d vs. %d', gamestate.player_score, gamestate.dealer_score), -bet)
                    else round_completed(gamestate, sprintf('👎You Lose - %d vs. %d', gamestate.dealer_score, gamestate.player_score), -bet)
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
                    println(sprintf("Choose %s[H]it, [S]tand or [Q]uit? ", if(gamestate.loop == 0, "[D]ouble-down, ", "")))
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
                    println(sprintf('%s\n', message))
                    gamestate.money += gamestate.bet_factor * betDelta
                }
            "#, "true");

            interpreter = verify_exact_code_with(interpreter, r#"
                fn show_game_table(gamestate) -> {
                    println(sprintf("DEALER - %d/21", gamestate.dealer_score))
                    show_hand(dealer)
                    println(sprintf("YOU (%s) - %s%d/21",
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
                    println(sprintf(" Player: %s \t Credit: $%.2f \t Bet: $%.2f \t Round: %d",
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
                (faces * suits)::to_table ~>> deck
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

            interpreter = verify_exact_code_with(interpreter, r#"
                show_separator(gamestate)
            "#, "true");
        }
    }

    /// Function tests
    #[cfg(test)]
    mod function_tests {
        use crate::interpreter::Interpreter;
        use crate::testdata::*;
        use crate::typed_values::TypedValue::*;

        #[test]
        fn test_function_lambda_with_inferred_types() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_code_with(interpreter, r#"
                let product = (a, b) -> a * b
            "#, "true");
            interpreter = verify_exact_code_with(interpreter, r#"
                product(2, 5)
            "#, "10")
        }

        #[test]
        fn test_function_lambda_with_parameter_types() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_code_with(interpreter, r#"
                let product = (a: i64, b: i64) -> a * b
            "#, "true");
            interpreter = verify_exact_code_with(interpreter, r#"
                product(3, 3)
            "#, "9")
        }

        #[test]
        fn test_function_lambda_with_parameter_types_and_return_type() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_code_with(interpreter, r#"
                let product = (a: i64, b: i64): i64 -> a * b
            "#, "true");
            interpreter = verify_exact_code_with(interpreter, r#"
                product(3, 9)
            "#, "27")
        }

        #[test]
        fn test_function_named_with_inferred_types() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_code_with(interpreter, r#"
                fn product(a, b) -> a * b
            "#, "true");
            interpreter = verify_exact_code_with(interpreter, r#"
                product(4, 5)
            "#, "20")
        }

        #[test]
        fn test_function_named_with_parameter_types() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_code_with(interpreter, r#"
                fn product(a: i64, b: i64) -> a * b
            "#, "true");
            interpreter = verify_exact_code_with(interpreter, r#"
                product(4, 9)
            "#, "36")
        }

        #[test]
        fn test_function_named_with_parameter_types_and_return_type() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_code_with(interpreter, r#"
                fn product(a: i64, b: i64): i64 -> a * b
            "#, "true");
            interpreter = verify_exact_code_with(interpreter, r#"
                product(5, 6)
            "#, "30")
        }

        #[test]
        fn test_function_recursion_1() {
            verify_exact_code(r#"
                let f = (n: i64) -> if(n <= 1) 1 else n * f(n - 1)
                f(5)
            "#, "120")
        }

        #[test]
        fn test_function_recursion_2() {
            verify_exact_code(r#"
                let f = n -> if(n <= 1, 1, n * f(n - 1))
                f(6)
            "#, "720")
        }

        #[test]
        fn test_functional_pipeline() {
            verify_exact_code(r#"
                let f = s -> s::reverse
                "Hello" |> f
            "#, "\"olleH\"");
        }

        #[test]
        fn test_functional_pipeline_with_destructure() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_code_with(interpreter, r#"
                fn add(a, b) -> a + b
            "#, "true");

            interpreter = verify_exact_code_with(interpreter, r#"
                fn inverse(a) -> 1.0 / a
            "#, "true");

            interpreter = verify_exact_code_with(interpreter, r#"
                (2, 3) |>> add 
            "#, "5");

            verify_exact_code_with(interpreter, r#"
                (2, 3) |>> add |> inverse
            "#, "0.2");
        }

        #[test]
        fn test_functional_sugar_with_filter_map() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_code_with(interpreter, r#"
                let arr = [1, 2, 3, 4]
                arr
                    ::filter(x -> (x % 2) == 0)
                    ::map(x -> x * 10)
            "#, "[20, 40]");
        }

        #[test]
        fn test_functional_sugar() {
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
            interpreter = verify_exact_code_with(interpreter, r#"
                3.2:::gb
            "#, "3435973836.8");
        }
    }

    /// Package "infix" tests
    #[cfg(test)]
    mod infix_tests {
        use crate::interpreter::Interpreter;
        use crate::testdata::{verify_exact_code, verify_exact_code_with, verify_exact_unwrapped_with};

        #[test]
        fn test_infix_field_access() {
            verify_exact_code(r#"
                let stock = { symbol: "TED", exchange: "AMEX", last_sale: 13.37 }
                stock.last_sale
            "#, "13.37");
        }

        #[test]
        fn test_infix_field_assignment() {
            verify_exact_code(r#"
                let stock = { symbol: "RLPH", exchange: "NYSE", last_sale: 13.37 }
                stock.last_sale = 35.78
                stock.last_sale
            "#, "35.78");
        }

        #[test]
        fn test_infix_function_call() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_code_with(interpreter,r#"
            stock = Struct(
                symbol: String(8) = "ABC",
                exchange: String(8) = "NYSE",
                last_sale: f64 = 23.67
            )::new
            mod stock {
                fn contains_symbol(symbol) -> symbol is self.symbol
            }
            "#, "true");

            // verify the structure
            interpreter = verify_exact_unwrapped_with(interpreter,r#"
                stock
            "#, r#"{"contains_symbol":{"code":"symbol == self.symbol","params":[{"data_type":"RuntimeResolvedType","default_value":"Null","name":"symbol"}],"returns":"Boolean"},"exchange":"NYSE","last_sale":23.67,"symbol":"ABC"}"#);

            // verify the negative case
            interpreter = verify_exact_code_with(interpreter,r#"
                stock.contains_symbol("XYZ")
            "#, "false");

            // verify the positive case
            interpreter = verify_exact_code_with(interpreter,r#"
                stock.contains_symbol("ABC")
            "#, "true");
        }
    }

    /// Package "testing" tests
    #[cfg(test)]
    mod type_of_tests {
        use crate::errors::Errors::Exact;
        use crate::testdata::{verify_exact_code, verify_exact_value};
        use crate::typed_values::TypedValue::ErrorValue;

        #[test]
        fn test_user_defined_type() {
            verify_exact_code(r#"
                Cards = Table(face: String(2), suit: String(2))  
                type_of(Cards::new())
            "#, "Table(face: String(2), suit: String(2))")
        }

        #[test]
        fn test_type_of_array_bool() {
            verify_exact_code("type_of([true, false])", "Array(Boolean):::(2)");
        }

        #[test]
        fn test_type_of_array_i64() {
            verify_exact_code("type_of([12, 76, 444])", "Array(i64):::(3)");
        }

        #[test]
        fn test_type_of_array_str() {
            verify_exact_code("type_of(['ciao', 'hello', 'world'])", "Array(String(5)):::(3)");
        }

        #[test]
        fn test_type_of_array_f64() {
            verify_exact_code("type_of([12.5, 123.2, 76.78])", "Array(f64):::(3)");
        }

        #[test]
        fn test_type_of_bool() {
            verify_exact_code("type_of(false)", "Boolean");
            verify_exact_code("type_of(true)", "Boolean");
        }

        #[test]
        fn test_type_of_date() {
            verify_exact_code("type_of(2025-07-06T21:00:29.412Z)", "DateTime");
        }

        #[test]
        fn test_type_of_fn() {
            verify_exact_code("type_of((a, b) -> a + b)", "fn(a, b)");
        }

        #[test]
        fn test_type_of_i64() {
            verify_exact_code("type_of(1234)", "i64");
        }

        #[test]
        fn test_type_of_f64() {
            verify_exact_code("type_of(12.394)", "f64");
        }

        #[test]
        fn test_type_of_string() {
            verify_exact_code("type_of('1234')", "String(4)");
            verify_exact_code(r#"type_of("abcde")"#, "String(5)");
        }

        #[test]
        fn test_type_of_structure_hard() {
            verify_exact_code(
                r#"type_of(Struct(symbol: String(3) = "ABC"))"#,
                r#"Struct(symbol: String(3) = "ABC")"#,
            );
        }

        #[test]
        fn test_type_of_structure_soft() {
            verify_exact_code(
                r#"type_of({symbol:"ABC"})"#,
                r#"Struct(symbol: String(3) = "ABC")"#,
            );
        }

        #[test]
        fn test_type_of_table() {
            verify_exact_code(
                r#"type_of(Table(symbol: String(8), exchange: String(8), last_sale: f64)::new)"#,
                "Table(symbol: String(8), exchange: String(8), last_sale: f64)",
            );
        }

        #[test]
        fn test_type_of_tuple() {
            verify_exact_code(
                "type_of(('ABC', 123.2, 2025-01-13T03:25:47.350Z))", 
                "(String(3), f64, DateTime)"
            );
        }

        #[test]
        fn test_type_of_uuid() {
            verify_exact_code("type_of(oxide::uuid())", "UUID");
        }

        #[test]
        fn test_type_of_variable() {
            verify_exact_value(
                "type_of(my_var)",
                ErrorValue(Exact("Variable 'my_var' not found".into())));
        }

        #[test]
        fn test_type_of_null() {
            verify_exact_code("type_of(null)", "");
        }

        #[test]
        fn test_type_of_undefined() {
            verify_exact_code("type_of(undefined)", "");
        }
    }
    
}