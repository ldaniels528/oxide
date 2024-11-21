////////////////////////////////////////////////////////////////////
// Interpreter class
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

use crate::compiler::Compiler;
use crate::machine::Machine;
use crate::typed_values::TypedValue;

/// Represents the Oxide language interpreter.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Interpreter {
    machine: Machine,
}

impl Interpreter {

    ////////////////////////////////////////////////////////////////
    // static methods
    ////////////////////////////////////////////////////////////////

    /// Constructs a new Interpreter
    pub fn new() -> Self {
        Self { machine: Machine::new_platform() }
    }

    ////////////////////////////////////////////////////////////////
    // instance methods
    ////////////////////////////////////////////////////////////////

    /// Executes the supplied source code returning the result of the evaluation
    pub(crate) fn evaluate(&mut self, source_code: &str) -> std::io::Result<TypedValue> {
        let opcode = Compiler::compile_script(source_code)?;
        let (machine, result) = self.machine.evaluate(&opcode)?;
        self.machine = machine;
        Ok(result)
    }

    /// Executes the supplied source code returning the result of the evaluation
    pub async fn evaluate_async(&mut self, source_code: &str) -> std::io::Result<TypedValue> {
        let opcode = Compiler::compile_script(source_code)?;
        let (machine, result) = self.machine.evaluate_async(&opcode).await?;
        self.machine = machine;
        Ok(result)
    }

    /// returns a variable by name
    pub fn get(&self, name: &str) -> Option<TypedValue> {
        self.machine.get(name)
    }

    /// Sets the value of a variable
    pub fn with_variable(&mut self, name: &str, value: TypedValue) {
        self.machine = self.machine.with_variable(name, value);
    }
}

/// Unit tests
#[cfg(test)]
mod tests {
    use crate::errors::Errors::Exact;
    use crate::expression::Expression::*;
    use crate::interpreter::Interpreter;
    use crate::model_row_collection::ModelRowCollection;
    use crate::numbers::NumberValue::{F64Value, I64Value, NaNValue, U128Value};
    use crate::outcomes::Outcomes::{Ack, RowsAffected};
    use crate::structures::*;
    use crate::table_columns::Column;
    use crate::testdata::*;
    use crate::typed_values::TypedValue::*;

    #[test]
    fn test_array_definition() {
        use crate::numbers::NumberValue::*;
        verify_exact("[0, 1, 3, 5]", Array(vec![
            Number(I64Value(0)), Number(I64Value(1)),
            Number(I64Value(3)), Number(I64Value(5)),
        ]));
    }

    #[test]
    fn test_array_access_element_at_index() {
        verify_exact("[-0.01, 8.25, 3.8, -4.5][2]", Number(F64Value(3.8)));
    }

    #[test]
    fn test_array_addition() {
        verify_exact(r#"
            a := ['cat', 'dog']
            b := ['mouse', 'rat']
            a + b
        "#, Array(vec![
            StringValue("cat".into()), StringValue("dog".into()),
            StringValue("mouse".into()), StringValue("rat".into())
        ]));
    }

    #[test]
    fn test_array_multiplication() {
        verify_exact(r#"
            a := ['cat', 'dog']
            a * 3
        "#, Array(vec![
            StringValue("cat".into()), StringValue("dog".into()),
            StringValue("cat".into()), StringValue("dog".into()),
            StringValue("cat".into()), StringValue("dog".into()),
        ]));
    }

    #[test]
    fn test_array_ranges() {
        use crate::numbers::NumberValue::*;
        verify_exact("0..4", Array(vec![
            Number(I64Value(0)), Number(I64Value(1)),
            Number(I64Value(2)), Number(I64Value(3)),
        ]));
    }

    #[test]
    fn test_basic_state_manipulation() {
        use crate::numbers::NumberValue::*;
        let mut interpreter = Interpreter::new();
        assert_eq!(interpreter.evaluate("x := 5").unwrap(), Outcome(Ack));
        assert_eq!(interpreter.evaluate("$x").unwrap(), Number(I64Value(5)));
        assert_eq!(interpreter.evaluate("-x").unwrap(), Number(I64Value(-5)));
        assert_eq!(interpreter.evaluate("x¡").unwrap(), Number(U128Value(120)));
        assert_eq!(interpreter.evaluate("x := x + 1").unwrap(), Outcome(Ack));
        assert_eq!(interpreter.evaluate("x").unwrap(), Number(I64Value(6)));
        assert_eq!(interpreter.evaluate("x < 7").unwrap(), Boolean(true));
        assert_eq!(interpreter.evaluate("x := x ** 2").unwrap(), Outcome(Ack));
        assert_eq!(interpreter.evaluate("x").unwrap(), Number(F64Value(36.)));
        assert_eq!(interpreter.evaluate("x / 0").unwrap(), Number(NaNValue));
        assert_eq!(interpreter.evaluate("x := x - 1").unwrap(), Outcome(Ack));
        assert_eq!(interpreter.evaluate("x % 5").unwrap(), Number(F64Value(0.)));
        assert_eq!(interpreter.evaluate("x < 35").unwrap(), Boolean(false));
        assert_eq!(interpreter.evaluate("x >= 35").unwrap(), Boolean(true));
    }

    #[test]
    fn test_between() {
        verify_exact("20 between 1 and 20", Boolean(true));
        verify_exact("21 between 1 and 20", Boolean(false));
    }

    #[test]
    fn test_betwixt() {
        verify_exact("20 betwixt 1 and 20", Boolean(false));
        verify_exact("19 betwixt 1 and 20", Boolean(true));
    }

    #[test]
    fn test_directive_die() {
        verify_exact(r#"[!] "Kaboom!!!""#, ErrorValue(Exact("Kaboom!!!".into())));
    }

    #[test]
    fn test_directive_ignore_failure() {
        verify_exact(r#"[~] 7 / 0"#, Outcome(Ack));
    }

    #[test]
    fn test_directive_must_be_false() {
        verify_exact(r#"
            [+] x := 67
            [-] x < 67
        "#, Boolean(false));
    }

    #[test]
    fn test_directive_must_be_true() {
        verify_exact("[+] x := 67", Outcome(Ack));
    }

    #[test]
    fn test_directives_pipeline() {
        verify_exact_table(r#"
            [+] stocks := ns("interpreter.pipeline.stocks")
            [+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
            [+] [{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                 { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
            [+] delete from stocks where last_sale < 30.0
            [+] from stocks
        "#, vec![
            "|-------------------------------|",
            "| symbol | exchange | last_sale |",
            "|-------------------------------|",
            "| BOOM   | NYSE     | 56.88     |",
            "| JET    | NASDAQ   | 32.12     |",
            "|-------------------------------|"
        ]);
    }

    #[test]
    fn test_feature_with_scenarios() {
        verify_exact_table_with_ids(r#"
            import oxide
            feature "Matches function" {
                scenario "Compare Array contents: Equal" {
                    assert(matches(
                        [ 1 "a" "b" "c" ],
                        [ 1 "a" "b" "c" ]
                    ))
                }
                scenario "Compare Array contents: Not Equal" {
                    assert(!matches(
                        [ 1 "a" "b" "c" ],
                        [ 0 "x" "y" "z" ]
                    ))
                }
                scenario "Compare JSON contents (in sequence)" {
                    assert(matches(
                            { first: "Tom" last: "Lane" },
                            { first: "Tom" last: "Lane" }))
                }
                scenario "Compare JSON contents (out of sequence)" {
                    assert(matches(
                            { scores: [82 78 99], id: "A1537" },
                            { id: "A1537", scores: [82 78 99] }))
                }
            }"#, vec![
            "|--------------------------------------------------------------------------------------------------------------------------|",
            "| id | level | item                                                                                      | passed | result |",
            "|--------------------------------------------------------------------------------------------------------------------------|",
            "| 0  | 0     | Matches function                                                                          | true   | ack    |",
            "| 1  | 1     | Compare Array contents: Equal                                                             | true   | ack    |",
            r#"| 2  | 2     | assert(matches([1, "a", "b", "c"], [1, "a", "b", "c"]))                                   | true   | true   |"#,
            "| 3  | 1     | Compare Array contents: Not Equal                                                         | true   | ack    |",
            r#"| 4  | 2     | assert(!matches([1, "a", "b", "c"], [0, "x", "y", "z"]))                                  | true   | true   |"#,
            "| 5  | 1     | Compare JSON contents (in sequence)                                                       | true   | ack    |",
            r#"| 6  | 2     | assert(matches({first: "Tom", last: "Lane"}, {first: "Tom", last: "Lane"}))               | true   | true   |"#,
            "| 7  | 1     | Compare JSON contents (out of sequence)                                                   | true   | ack    |",
            r#"| 8  | 2     | assert(matches({scores: [82, 78, 99], id: "A1537"}, {id: "A1537", scores: [82, 78, 99]})) | true   | true   |"#,
            "|--------------------------------------------------------------------------------------------------------------------------|"
        ]);
    }

    #[test]
    fn test_function_lambda() {
        let mut interpreter = Interpreter::new();
        assert_eq!(Outcome(Ack), interpreter.evaluate(r#"
            product := fn (a, b) => a * b
        "#).unwrap());

        assert_eq!(Number(I64Value(10)), interpreter.evaluate(r#"
            product(2, 5)
        "#).unwrap())
    }

    #[test]
    fn test_function_named() {
        let mut interpreter = Interpreter::new();
        assert_eq!(Outcome(Ack), interpreter.evaluate(r#"
            fn product(a, b) => a * b
        "#).unwrap());

        assert_eq!(Number(I64Value(10)), interpreter.evaluate(r#"
            product(2, 5)
        "#).unwrap())
    }

    #[test]
    fn test_function_recursion_1() {
        verify_exact(r#"
            f := fn(n: i64) => if(n <= 1) 1 else n * f(n - 1)
            f(5)
        "#, Number(I64Value(120)))
    }

    #[test]
    fn test_function_recursion_2() {
        verify_exact(r#"
            f := fn(n) => iff(n <= 1, 1, n * f(n - 1))
            f(6)
        "#, Number(I64Value(720)))
    }

    #[test]
    fn test_hard_structure() {
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
            struct(symbol: String(8), exchange: String(8), last_sale: f64)
        "#).unwrap();
        let phys_columns = make_quote_columns();
        assert_eq!(result, StructureHard(HardStructure::new(phys_columns.clone(), Vec::new())));
    }

    #[test]
    fn test_hard_structure_with_default_values() {
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
            struct(
                symbol: String(8) = "ABC",
                exchange: String(8) = "NYSE",
                last_sale: f64 = 23.67
            )
        "#).unwrap();
        match result {
            StructureHard(s) =>
                assert_eq!(s.get_values(), vec![
                    StringValue("ABC".into()),
                    StringValue("NYSE".into()),
                    Number(F64Value(23.67)),
                ]),
            x => assert_eq!(x, Undefined)
        }
    }

    #[test]
    fn test_hard_structure_field() {
        verify_exact(r#"
            stock := struct(
                symbol: String(8) = "ABC",
                exchange: String(8) = "NYSE",
                last_sale: f64 = 23.67
            )
            stock::symbol
        "#, StringValue("ABC".into()));
    }

    #[test]
    fn test_hard_structure_field_assignment() {
        verify_exact(r#"
            stock := struct(
                symbol: String(8) = "ABC",
                exchange: String(8) = "NYSE",
                last_sale: f64 = 23.67
            )
            stock::last_sale := 24.11
            stock::last_sale
        "#, Number(F64Value(24.11)));
    }

    #[test]
    fn test_hard_structure_module_method() {
        verify_exact(r#"
            stock := struct(
                symbol: String(8) = "ABC",
                exchange: String(8) = "NYSE",
                last_sale: f64 = 23.67
            )

            mod stock {
                fn is_this_you(symbol) => symbol == self::symbol
            }

            stock::is_this_you('ABC')
        "#, Boolean(true));
    }

    #[test]
    fn test_hard_structure_import() {
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
            stock := struct(
                symbol: String(8) = "ABC",
                exchange: String(8) = "NYSE",
                last_sale: f64 = 23.67
            )
            import stock
        "#).unwrap();
        assert_eq!(interpreter.get("symbol"), Some(StringValue("ABC".into())));
        assert_eq!(interpreter.get("exchange"), Some(StringValue("NYSE".into())));
        assert_eq!(interpreter.get("last_sale"), Some(Number(F64Value(23.67))));
    }

    #[test]
    fn test_hard_structure_from_table() {
        verify_exact(r#"
            [+] stocks := ns("interpreter.struct.stocks")
            [+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
            [+] [{ symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
                 { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                 { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                 { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                 { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }] ~> stocks
            stocks[0]
        "#, StructureHard(HardStructure::from_parameters_and_values(
            &make_quote_parameters(), vec![
                StringValue("ABC".into()),
                StringValue("AMEX".into()),
                Number(F64Value(11.11)),
            ],
        ).unwrap()));
    }

    #[test]
    fn test_if_when_result_is_defined() {
        verify_exact(r#"
            x := 7
            if(x > 5) "Yes"
        "#, StringValue("Yes".into()));
    }

    #[test]
    fn test_if_when_result_is_undefined() {
        verify_exact(r#"
            x := 4
            if(x > 5) "Yes"
        "#, Undefined);
    }

    #[test]
    fn test_if_else_expression() {
        verify_exact(r#"
            x := 4
            if(x > 5) "Yes"
            else if(x < 5) "Maybe"
            else "No"
        "#, StringValue("Maybe".into()));
    }

    #[test]
    fn test_like() {
        verify_exact("'Hello' like 'H*o'", Boolean(true));
        verify_exact("'Hello' like 'H.ll.'", Boolean(true));
        verify_exact("'Hello' like 'H%ll%'", Boolean(false));
    }

    #[test]
    fn test_soft_structure() {
        let code = r#"
            {
                fields:[
                    { name: "symbol", value: "ABC" },
                    { name: "exchange", value: "AMEX" },
                    { name: "last_sale", value: 11.77 }
                ]
            }"#;
        let model = StructureSoft(SoftStructure::new(&vec![
            ("fields", Array(vec![
                StructureSoft(SoftStructure::new(&vec![
                    ("name", StringValue("symbol".into())),
                    ("value", StringValue("ABC".into()))
                ])),
                StructureSoft(SoftStructure::new(&vec![
                    ("name", StringValue("exchange".into())),
                    ("value", StringValue("AMEX".into()))
                ])),
                StructureSoft(SoftStructure::new(&vec![
                    ("name", StringValue("last_sale".into())),
                    ("value", Number(F64Value(11.77)))
                ])),
            ]))
        ]));
        verify_exact(code, model.clone());
        assert_eq!(
            model.to_code().chars().filter(|c| !c.is_whitespace())
                .collect::<String>(),
            code.chars().filter(|c| !c.is_whitespace())
                .collect::<String>())
    }

    #[test]
    fn test_soft_structure_field() {
        verify_exact(r#"
            stock := { symbol:"AAA", price:123.45 }
            stock::symbol
        "#, StringValue("AAA".into()));
    }

    #[test]
    fn test_soft_structure_field_assignment() {
        verify_exact(r#"
            stock := { symbol:"AAA", price:123.45 }
            stock::price := 124.11
            stock::price
        "#, Number(F64Value(124.11)));
    }

    #[test]
    fn test_soft_structure_method() {
        verify_exact(r#"
            stock := {
                symbol:"ABC",
                price:123.45,
                last_sale: 23.67,
                is_this_you: fn(symbol) => symbol == self::symbol
            }
            stock::is_this_you('ABC')
        "#, Boolean(true));
    }

    #[test]
    fn test_soft_structure_module_method() {
        verify_exact(r#"
            stock := {
                symbol: "ABC",
                exchange: "NYSE",
                last_sale: 23.67
            }

            mod stock {
                fn is_this_you(symbol) => symbol == self::symbol
            }

            stock::is_this_you('ABC')
        "#, Boolean(true));
    }

    #[test]
    fn test_soft_structure_import() {
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
            quote := { symbol: "ABC", exchange: "AMEX" }
            import quote
        "#).unwrap();
        let machine = interpreter;
        assert_eq!(machine.get("symbol"), Some(StringValue("ABC".into())));
        assert_eq!(machine.get("exchange"), Some(StringValue("AMEX".into())));
    }

    #[test]
    fn test_soft_structure_json_literal_1() {
        verify_exact(r#"
            {
              "columns": [{
                  "name": "symbol",
                  "param_type": "String(8)",
                  "default_value": null
                }, {
                  "name": "exchange",
                  "param_type": "String(8)",
                  "default_value": null
                }, {
                  "name": "last_sale",
                  "param_type": "f64",
                  "default_value": null
                }],
              "indices": [],
              "partitions": []
            }
        "#, StructureSoft(SoftStructure::new(&vec![
            ("columns", Array(vec![
                StructureSoft(SoftStructure::new(&vec![
                    ("name", StringValue("symbol".into())),
                    ("param_type", StringValue("String(8)".into())),
                    ("default_value", Null),
                ])),
                StructureSoft(SoftStructure::new(&vec![
                    ("name", StringValue("exchange".into())),
                    ("param_type", StringValue("String(8)".into())),
                    ("default_value", Null)
                ])),
                StructureSoft(SoftStructure::new(&vec![
                    ("name", StringValue("last_sale".into())),
                    ("param_type", StringValue("f64".into())),
                    ("default_value", Null),
                ])),
            ])),
            ("indices", Array(Vec::new())),
            ("partitions", Array(Vec::new())),
        ])));
    }

    #[test]
    fn test_soft_structure_json_literal_2() {
        verify_exact(r#"
            {
              columns: [{
                  name: "symbol",
                  param_type: "String(8)",
                  default_value: null
                }, {
                  name: "exchange",
                  param_type: "String(8)",
                  default_value: null
                }, {
                  name: "last_sale",
                  param_type: "f64",
                  default_value: null
                }]
            }
        "#, StructureSoft(SoftStructure::new(&vec![
            ("columns", Array(vec![
                StructureSoft(SoftStructure::new(&vec![
                    ("name", StringValue("symbol".into())),
                    ("param_type", StringValue("String(8)".into())),
                    ("default_value", Null),
                ])),
                StructureSoft(SoftStructure::new(&vec![
                    ("name", StringValue("exchange".into())),
                    ("param_type", StringValue("String(8)".into())),
                    ("default_value", Null)
                ])),
                StructureSoft(SoftStructure::new(&vec![
                    ("name", StringValue("last_sale".into())),
                    ("param_type", StringValue("f64".into())),
                    ("default_value", Null),
                ])),
            ]))
        ])));
    }

    #[test]
    fn test_structure_module() {
        verify_exact(r#"
            mod abc {
                fn hello(name) => str::format("hello {}", name)
            }
            abc::hello('world')
        "#, StringValue("hello world".into()));
    }

    #[test]
    fn test_if_function() {
        verify_exact(r#"
            x := 4
            iff(x > 5, "Yes", iff(x < 5, "Maybe", "No"))
        "#, StringValue("Maybe".into()));
    }

    #[test]
    fn test_table_create_ephemeral() {
        verify_exact(r#"
        table(
            symbol: String(8),
            exchange: String(8),
            last_sale: f64
        )"#, TableValue(ModelRowCollection::with_rows(
            make_quote_columns(), Vec::new(),
        )))
    }

    #[test]
    fn test_table_create_durable() {
        verify_exact(r#"
            create table ns("interpreter.create.stocks") (
                symbol: String(8),
                exchange: String(8),
                last_sale: f64
            )"#, Outcome(Ack))
    }

    #[test]
    fn test_table_crud_in_namespace() {
        let mut interpreter = Interpreter::new();
        let columns = make_quote_parameters();
        let phys_columns = Column::from_parameters(&columns).unwrap();

        // set up the interpreter
        assert_eq!(Outcome(Ack), interpreter.evaluate(r#"
            stocks := ns("interpreter.crud.stocks")
        "#).unwrap());

        // create the table
        assert_eq!(Outcome(RowsAffected(0)), interpreter.evaluate(r#"
            table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
        "#).unwrap());

        // append a row
        assert_eq!(Outcome(RowsAffected(1)), interpreter.evaluate(r#"
            append stocks from { symbol: "ABC", exchange: "AMEX", last_sale: 11.77 }
        "#).unwrap());

        // write another row
        assert_eq!(Outcome(RowsAffected(1)), interpreter.evaluate(r#"
            { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 } ~> stocks
        "#).unwrap());

        // write some more rows
        assert_eq!(Outcome(RowsAffected(2)), interpreter.evaluate(r#"
            [{ symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
             { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 }] ~> stocks
        "#).unwrap());

        // write even more rows
        assert_eq!(Outcome(RowsAffected(2)), interpreter.evaluate(r#"
            append stocks from [
                { symbol: "BOOM", exchange: "NASDAQ", last_sale: 56.87 },
                { symbol: "TRX", exchange: "NASDAQ", last_sale: 7.9311 }
            ]
        "#).unwrap());

        // remove some rows
        assert_eq!(Outcome(RowsAffected(4)), interpreter.evaluate(r#"
            delete from stocks where last_sale > 1.0
        "#).unwrap());

        // overwrite a row
        assert_eq!(Outcome(RowsAffected(1)), interpreter.evaluate(r#"
            overwrite stocks
            via {symbol: "GOTO", exchange: "OTC", last_sale: 0.1421}
            where symbol == "GOTO"
        "#).unwrap());

        // verify the remaining rows
        assert_eq!(
            interpreter.evaluate("from stocks").unwrap(),
            TableValue(ModelRowCollection::from_rows(phys_columns.clone(), vec![
                make_quote(1, "UNO", "OTC", 0.2456),
                make_quote(3, "GOTO", "OTC", 0.1421),
            ]))
        );

        // restore the previously deleted rows
        assert_eq!(Outcome(RowsAffected(4)), interpreter.evaluate(r#"
            undelete from stocks where last_sale > 1.0
        "#).unwrap());

        // verify the existing rows
        assert_eq!(
            interpreter.evaluate("from stocks").unwrap(),
            TableValue(ModelRowCollection::from_rows(phys_columns.clone(), vec![
                make_quote(0, "ABC", "AMEX", 11.77),
                make_quote(1, "UNO", "OTC", 0.2456),
                make_quote(2, "BIZ", "NYSE", 23.66),
                make_quote(3, "GOTO", "OTC", 0.1421),
                make_quote(4, "BOOM", "NASDAQ", 56.87),
                make_quote(5, "TRX", "NASDAQ", 7.9311),
            ]))
        );
    }

    #[test]
    fn test_table_select_from_namespace() {
        // create a table with test data
        let columns = make_quote_parameters();
        let phys_columns = Column::from_parameters(&columns).unwrap();

        // append some rows
        let mut interpreter = Interpreter::new();
        assert_eq!(Outcome(RowsAffected(5)), interpreter.evaluate(r#"
            stocks := ns("interpreter.select1.stocks")
            table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
            append stocks from [
                { symbol: "ABC", exchange: "AMEX", last_sale: 11.77 },
                { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }
            ]
        "#).unwrap());

        // compile and execute the code
        assert_eq!(interpreter.evaluate(r#"
            select symbol, exchange, last_sale from stocks
            where last_sale > 1.0
            order by symbol
            limit 5
        "#).unwrap(), TableValue(ModelRowCollection::from_rows(phys_columns.clone(), vec![
            make_quote(0, "ABC", "AMEX", 11.77),
            make_quote(2, "BIZ", "NYSE", 23.66),
        ])));
    }

    #[test]
    fn test_table_select_from_variable() {
        // create a table with test data
        let columns = make_quote_parameters();
        let phys_columns = Column::from_parameters(&columns).unwrap();

        // set up the interpreter
        verify_exact_table_with_ids(r#"
            [+] stocks := ns("interpreter.select.stocks")
            [+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
            [+] [{ symbol: "ABC", exchange: "AMEX", last_sale: 11.77 },
                 { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                 { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                 { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                 { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }] ~> stocks
            [+] select symbol, exchange, last_sale
                from stocks
                where last_sale > 1.0
                order by symbol
                limit 5
        "#, vec![
            "|------------------------------------|",
            "| id | symbol | exchange | last_sale |",
            "|------------------------------------|",
            "| 0  | ABC    | AMEX     | 11.77     |",
            "| 2  | BIZ    | NYSE     | 23.66     |",
            "|------------------------------------|"
        ]);
    }

    #[test]
    fn test_while_loop() {
        let mut interpreter = Interpreter::new();
        assert_eq!(Outcome(Ack), interpreter.evaluate("x := 0").unwrap());
        assert_eq!(Outcome(Ack), interpreter.evaluate(r#"
            while (x < 5)
                x := x + 1
        "#).unwrap());
        assert_eq!(Number(I64Value(5)), interpreter.evaluate("x").unwrap());
    }
}