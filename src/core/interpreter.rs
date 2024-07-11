////////////////////////////////////////////////////////////////////
// interpreter module
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
        Interpreter {
            machine: Machine::new(),
        }
    }

    ////////////////////////////////////////////////////////////////
    // instance methods
    ////////////////////////////////////////////////////////////////

    /// Executes the supplied source code returning the result of the evaluation
    pub fn evaluate(&mut self, source_code: &str) -> std::io::Result<TypedValue> {
        let opcode = Compiler::compile_script(source_code)?;
        let (machine, result) = self.machine.evaluate(&opcode)?;
        self.machine = machine;
        Ok(result)
    }

    pub fn with_variable(&mut self, name: &str, value: TypedValue) {
        self.machine = self.machine.with_variable(name, value);
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::interpreter::Interpreter;
    use crate::model_row_collection::ModelRowCollection;
    use crate::namespaces::Namespace;
    use crate::table_columns::TableColumn;
    use crate::testdata::{make_dataframe_ns, make_quote, make_quote_columns, make_table_columns};
    use crate::typed_values::TypedValue;

    #[test]
    fn test_basic_state_retention() {
        let mut interpreter = Interpreter::new();
        assert_eq!(interpreter.evaluate("x := 5").unwrap(), TypedValue::Ack);
        assert_eq!(interpreter.evaluate("x¡").unwrap(), TypedValue::Float64Value(120.));
        assert_eq!(interpreter.evaluate("x := x + 1").unwrap(), TypedValue::Ack);
        assert_eq!(interpreter.evaluate("x").unwrap(), TypedValue::Int64Value(6));
        assert_eq!(interpreter.evaluate("x < 7").unwrap(), TypedValue::Boolean(true));
        assert_eq!(interpreter.evaluate("x := x ** 2").unwrap(), TypedValue::Ack);
        assert_eq!(interpreter.evaluate("x").unwrap(), TypedValue::Int64Value(36));
        assert_eq!(interpreter.evaluate("x / 0").unwrap(), TypedValue::Int64Value(9223372036854775807));
        assert_eq!(interpreter.evaluate("x := x - 1").unwrap(), TypedValue::Ack);
        assert_eq!(interpreter.evaluate("x % 5").unwrap(), TypedValue::Int64Value(0));
        assert_eq!(interpreter.evaluate("x < 35").unwrap(), TypedValue::Boolean(false));
        assert_eq!(interpreter.evaluate("x >= 35").unwrap(), TypedValue::Boolean(true));
    }

    #[test]
    fn test_directive_must_be_true() {
        verify_results("[+] x := 67", TypedValue::Ack);
    }

    #[test]
    fn test_directive_must_be_false() {
        verify_results(r#"
            [+] x := 67
            [-] x < 67
        "#, TypedValue::Boolean(false));
    }

    #[test]
    fn test_directive_die() {
        verify_results(r#"
            [!] "Kaboom!!!"
        "#, TypedValue::ErrorValue("Kaboom!!!".to_string()));
    }

    #[test]
    fn test_eval_valid() {
        verify_results(r#"
            eval "2 ** 4"
        "#, TypedValue::Int64Value(16))
    }

    #[test]
    fn test_eval_invalid() {
        let mut interpreter = Interpreter::new();
        let value = interpreter.evaluate(r#"
            eval 123
        "#).unwrap();
        assert_eq!(value, TypedValue::ErrorValue("Type mismatch - expected String, got i64".into()))
    }

    #[test]
    fn test_if_defined() {
        let mut interpreter = Interpreter::new();
        let value = interpreter.evaluate(r#"
            x := 7
            if(x > 5) "Yes"
        "#).unwrap();
        assert_eq!(value, TypedValue::StringValue("Yes".to_string()));
    }

    #[test]
    fn test_if_undefined() {
        let mut interpreter = Interpreter::new();
        let value = interpreter.evaluate(r#"
            x := 4
            if(x > 5) "Yes"
        "#).unwrap();
        assert_eq!(value, TypedValue::Undefined);
    }

    #[test]
    fn test_if_else() {
        let mut interpreter = Interpreter::new();
        let value = interpreter.evaluate(r#"
            x := 4
            if(x > 5) "Yes"
            else if(x < 5) "Maybe"
            else "No"
        "#).unwrap();
        assert_eq!(value, TypedValue::StringValue("Maybe".to_string()));
    }

    #[test]
    fn test_iff() {
        verify_results(r#"
            x := 4
            iff(x > 5, "Yes", iff(x < 5, "Maybe", "No"))
        "#, TypedValue::StringValue("Maybe".to_string()));
    }

    #[test]
    fn test_include_file_valid() {
        let phys_columns = make_table_columns();
        verify_results(r#"
        include "./demoes/language/include_file.ox"
        "#, TypedValue::TableValue(ModelRowCollection::from_rows(vec![
            make_quote(0, &phys_columns, "ABC", "AMEX", 12.49),
            make_quote(1, &phys_columns, "BOOM", "NYSE", 56.88),
            make_quote(2, &phys_columns, "JET", "NASDAQ", 32.12),
        ])));
    }

    #[test]
    fn test_include_file_invalid() {
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
        include 123
        "#).unwrap();
        assert_eq!(
            result, TypedValue::ErrorValue("Type mismatch - expected String, got i64".to_string())
        )
    }

    #[test]
    fn verify_write_source_into_target() {
        // create a durable table
        verify_results(r#"
            table(symbol: String(8), exchange: String(8), last_sale: f64) 
                into ns("interpreter.into.stocks")
        "#, TypedValue::RowsAffected(0));

        // insert rows into the durable table
        verify_results(r#"
            from [
                { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }
            ] into ns("interpreter.into.stocks")
        "#, TypedValue::RowsAffected(3));

        // re-read the results
        let columns = make_quote_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        verify_results(r#"
            from ns("interpreter.into.stocks")
        "#, TypedValue::TableValue(ModelRowCollection::from_rows(vec![
            make_quote(0, &phys_columns, "ABC", "AMEX", 12.49),
            make_quote(1, &phys_columns, "BOOM", "NYSE", 56.88),
            make_quote(2, &phys_columns, "JET", "NASDAQ", 32.12),
        ])));
    }

    #[test]
    fn test_lambda_functions() {
        let mut interpreter = Interpreter::new();
        assert_eq!(TypedValue::Ack, interpreter.evaluate(r#"
        product := fn (a, b) => a * b
        "#).unwrap());

        assert_eq!(TypedValue::Int64Value(10), interpreter.evaluate(r#"
        product(2, 5)
        "#).unwrap())
    }

    #[test]
    fn test_named_functions() {
        let mut interpreter = Interpreter::new();
        assert_eq!(TypedValue::Ack, interpreter.evaluate(r#"
        fn product(a, b) => a * b
        "#).unwrap());

        assert_eq!(TypedValue::Int64Value(10), interpreter.evaluate(r#"
        product(2, 5)
        "#).unwrap())
    }

    #[test]
    fn test_create_ephemeral_table() {
        verify_results(r#"
        table(
            symbol: String(8),
            exchange: String(8),
            last_sale: f64
        )"#, TypedValue::TableValue(ModelRowCollection::new(
            make_table_columns(), vec![],
        )))
    }

    #[test]
    fn test_create_durable_table() {
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
        create table ns("interpreter.create.stocks") (
            symbol: String(8),
            exchange: String(8),
            last_sale: f64)
        "#).unwrap();
        assert_eq!(result, TypedValue::Ack)
    }

    #[test]
    fn test_append_row_to_namespace() {
        // create a table with test data
        let columns = make_quote_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let ns = Namespace::parse("interpreter.append.stocks").unwrap();
        let mut df = make_dataframe_ns(ns, columns.clone()).unwrap();
        assert_eq!(1, df.append(&make_quote(0, &phys_columns, "ABC", "AMEX", 11.77)).unwrap());
        assert_eq!(1, df.append(&make_quote(1, &phys_columns, "UNO", "OTC", 0.2456)).unwrap());
        assert_eq!(1, df.append(&make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66)).unwrap());
        assert_eq!(1, df.append(&make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428)).unwrap());

        // insert some rows
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
            stocks := ns("interpreter.append.stocks")
            append stocks from { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 }
        "#).unwrap();
        assert_eq!(result, TypedValue::RowsAffected(1));

        // verify the remaining rows
        assert_eq!(df.read_all_rows().unwrap(), vec![
            make_quote(0, &phys_columns, "ABC", "AMEX", 11.77),
            make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
            make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66),
            make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428),
            make_quote(4, &phys_columns, "BOOM", "NYSE", 56.88),
        ]);
    }

    #[test]
    fn test_append_rows_to_namespace() {
        // create a table with test data
        let columns = make_quote_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let ns = Namespace::parse("interpreter.append_rows.stocks").unwrap();
        let mut df = make_dataframe_ns(ns, columns.clone()).unwrap();
        df.resize(0).unwrap();

        // insert some rows
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
            stocks := ns("interpreter.append_rows.stocks")
            append stocks from [
                { symbol: "ABC", exchange: "AMEX", last_sale: 11.88 },
                { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                { symbol: "GOTO", exchange: "NYSE", last_sale: 0.1428 },
                { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 }
            ]
        "#).unwrap();
        assert_eq!(result, TypedValue::RowsAffected(5));

        // verify the rows
        assert_eq!(df.read_all_rows().unwrap(), vec![
            make_quote(0, &phys_columns, "ABC", "AMEX", 11.88),
            make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
            make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66),
            make_quote(3, &phys_columns, "GOTO", "NYSE", 0.1428),
            make_quote(4, &phys_columns, "BOOM", "NYSE", 56.88),
        ]);
    }

    #[test]
    fn test_delete_from_namespace() {
        // create a table with test data
        let columns = make_quote_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let ns = Namespace::parse("interpreter.delete.stocks").unwrap();
        let mut df = make_dataframe_ns(ns, columns.clone()).unwrap();
        assert_eq!(1, df.append(&make_quote(0, &phys_columns, "ABC", "AMEX", 11.77)).unwrap());
        assert_eq!(1, df.append(&make_quote(1, &phys_columns, "UNO", "OTC", 0.2456)).unwrap());
        assert_eq!(1, df.append(&make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66)).unwrap());
        assert_eq!(1, df.append(&make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428)).unwrap());
        assert_eq!(1, df.append(&make_quote(4, &phys_columns, "BOOM", "NASDAQ", 56.87)).unwrap());

        // delete some rows
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
            stocks := ns("interpreter.delete.stocks")
            delete from stocks where last_sale >= 1.0
        "#).unwrap();
        assert_eq!(result, TypedValue::RowsAffected(3));

        // verify the remaining rows
        assert_eq!(df.read_all_rows().unwrap(), vec![
            make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
            make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428),
        ]);
    }

    #[test]
    fn test_overwrite_rows_in_namespace() {
        // create a table with test data
        let columns = make_quote_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let ns = Namespace::parse("interpreter.overwrite.stocks").unwrap();
        let mut df = make_dataframe_ns(ns, columns.clone()).unwrap();
        assert_eq!(1, df.append(&make_quote(0, &phys_columns, "ABC", "AMEX", 11.77)).unwrap());
        assert_eq!(1, df.append(&make_quote(1, &phys_columns, "UNO", "OTC", 0.2456)).unwrap());
        assert_eq!(1, df.append(&make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66)).unwrap());
        assert_eq!(1, df.append(&make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428)).unwrap());
        assert_eq!(1, df.append(&make_quote(4, &phys_columns, "BOOM", "NASDAQ", 56.87)).unwrap());

        // overwrite a row
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
            stocks := ns("interpreter.overwrite.stocks")
            overwrite stocks
            via {symbol: "BOOM", exchange: "NYSE", last_sale: 56.99}
            where symbol == "BOOM"
        "#).unwrap();
        assert_eq!(result, TypedValue::RowsAffected(1));

        // verify the remaining rows
        assert_eq!(df.read_all_rows().unwrap(), vec![
            make_quote(0, &phys_columns, "ABC", "AMEX", 11.77),
            make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
            make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66),
            make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428),
            make_quote(4, &phys_columns, "BOOM", "NYSE", 56.99),
        ]);
    }

    #[test]
    fn test_select_from_namespace() {
        // create a table with test data
        let columns = make_quote_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let ns = Namespace::parse("interpreter.select.stocks").unwrap();
        let mut df = make_dataframe_ns(ns, columns.clone()).unwrap();
        assert_eq!(1, df.append(&make_quote(0, &phys_columns, "ABC", "AMEX", 11.77)).unwrap());
        assert_eq!(1, df.append(&make_quote(1, &phys_columns, "UNO", "OTC", 0.2456)).unwrap());
        assert_eq!(1, df.append(&make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66)).unwrap());
        assert_eq!(1, df.append(&make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428)).unwrap());
        assert_eq!(1, df.append(&make_quote(4, &phys_columns, "BOOM", "NASDAQ", 0.0872)).unwrap());

        // compile and execute the code
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
            stocks := ns("interpreter.select.stocks")
            select symbol, exchange, last_sale
            from stocks
            where last_sale > 1.0
            order by symbol
            limit 5
        "#).unwrap();
        assert_eq!(result, TypedValue::TableValue(ModelRowCollection::from_rows(vec![
            make_quote(0, &phys_columns, "ABC", "AMEX", 11.77),
            make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66),
        ])));
    }

    #[test]
    fn test_select_from_variable() {
        // stage the data
        let phys_columns = TableColumn::from_columns(&make_quote_columns()).unwrap();
        let mut interpreter = Interpreter::new();
        interpreter.with_variable("stocks", TypedValue::TableValue(
            ModelRowCollection::from_rows(vec![
                make_quote(0, &phys_columns, "ABC", "AMEX", 11.88),
                make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
                make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66),
                make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428),
                make_quote(4, &phys_columns, "BOOM", "NASDAQ", 56.87),
            ])));

        // execute the code
        let result = interpreter.evaluate(r#"
            select symbol, exchange, last_sale
            from stocks
            where last_sale < 1.0
            order by symbol
            limit 5
        "#).unwrap();
        assert_eq!(result, TypedValue::TableValue(ModelRowCollection::from_rows(vec![
            make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
            make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428),
        ])));
    }

    #[test]
    fn test_reverse_from_namespace() {
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
            [+] stocks := ns("interpreter.reverse.stocks")
            [~] drop table stocks
            [+] create table `stocks` (
                    symbol: String(8),
                    exchange: String(8),
                    last_sale: f64)
            [+] append stocks from [
                    { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                    { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                    { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }
                ]
        "#).unwrap();
        assert_eq!(result, TypedValue::RowsAffected(3));

        let result = interpreter.evaluate(r#"
            reverse from stocks
        "#).unwrap();
        let phys_columns = make_table_columns();
        assert_eq!(result, TypedValue::TableValue(ModelRowCollection::from_rows(vec![
            make_quote(2, &phys_columns, "JET", "NASDAQ", 32.12),
            make_quote(1, &phys_columns, "BOOM", "NYSE", 56.88),
            make_quote(0, &phys_columns, "ABC", "AMEX", 12.49),
        ])));
    }

    #[test]
    fn test_reverse_from_variable() {
        let phys_columns = make_table_columns();
        let mut interpreter = Interpreter::new();
        interpreter.with_variable("stocks", TypedValue::TableValue(
            ModelRowCollection::from_rows(vec![
                make_quote(0, &phys_columns, "ABC", "AMEX", 11.88),
                make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
                make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66),
                make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428),
                make_quote(4, &phys_columns, "BOOM", "NASDAQ", 56.87),
            ])));

        let result = interpreter.evaluate(r#"
            reverse from stocks
        "#).unwrap();
        assert_eq!(result, TypedValue::TableValue(ModelRowCollection::from_rows(vec![
            make_quote(4, &phys_columns, "BOOM", "NASDAQ", 56.87),
            make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428),
            make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66),
            make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
            make_quote(0, &phys_columns, "ABC", "AMEX", 11.88),
        ])));
    }

    #[test]
    fn test_processing_pipeline() {
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
            [+] stocks := ns("interpreter.pipeline.stocks")
            [~] drop table stocks
            [+] create table `stocks` (
                    symbol: String(8),
                    exchange: String(8),
                    last_sale: f64)
            [+] append stocks from [
                    { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                    { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                    { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }]
            [+] from stocks
        "#).unwrap();
        let phys_columns = make_table_columns();
        assert_eq!(result, TypedValue::TableValue(ModelRowCollection::from_rows(vec![
            make_quote(0, &phys_columns, "ABC", "AMEX", 12.49),
            make_quote(1, &phys_columns, "BOOM", "NYSE", 56.88),
            make_quote(2, &phys_columns, "JET", "NASDAQ", 32.12),
        ])));

        let result = interpreter.evaluate(r#"
            delete from stocks where last_sale < 30.0
        "#).unwrap();
        assert_eq!(result, TypedValue::RowsAffected(1));
    }

    #[test]
    fn write_to_stderr() {
        verify_results(r#"
            stderr "Goodbye Cruel World"
        "#, TypedValue::Ack);
    }

    #[test]
    fn write_to_stdout() {
        verify_results(r#"
            stdout "Hello World"
        "#, TypedValue::Ack);
    }

    #[ignore]
    #[test]
    fn test_while_loop() {
        verify_results(r#"
            x := 0
            while (x < 5)
                x := x + 1
            x
        "#, TypedValue::Int64Value(5));
    }

    fn verify_results(code: &str, expected: TypedValue) {
        let mut interpreter = Interpreter::new();
        let actual = interpreter.evaluate(code).unwrap();
        assert_eq!(actual, expected);
    }
}