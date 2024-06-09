////////////////////////////////////////////////////////////////////
// interpreter module
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

use crate::compiler::CompilerState;
use crate::machine::MachineState;
use crate::typed_values::TypedValue;

/// Represents the Oxide language interpreter.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Interpreter {
    machine: MachineState,
}

impl Interpreter {

    ////////////////////////////////////////////////////////////////
    // static methods
    ////////////////////////////////////////////////////////////////

    /// Constructs a new Interpreter
    pub fn new() -> Self {
        Interpreter {
            machine: MachineState::new(),
        }
    }

    ////////////////////////////////////////////////////////////////
    // instance methods
    ////////////////////////////////////////////////////////////////

    /// Executes the supplied source code returning the result of the evaluation
    pub fn evaluate(&mut self, source_code: &str) -> std::io::Result<TypedValue> {
        let opcode = CompilerState::compile_script(source_code)?;
        let (ms, result) = self.machine.evaluate(&opcode)?;
        self.machine = ms;
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
    use crate::typed_values::TypedValue::{ErrorValue, Int64Value};

    #[test]
    fn test_evaluate_n_pow_2() {
        let mut interpreter = Interpreter::new();
        interpreter.with_variable("n", TypedValue::Int64Value(5));
        let result = interpreter.evaluate("n ** 2").unwrap();
        assert_eq!(result, TypedValue::Int64Value(25))
    }

    #[test]
    fn test_evaluate_n_gt_5() {
        let mut interpreter = Interpreter::new();
        interpreter.with_variable("n", TypedValue::Int64Value(7));
        let result = interpreter.evaluate("n > 5").unwrap();
        assert_eq!(result, TypedValue::Boolean(true))
    }

    #[test]
    fn test_eval_pass() {
        let mut interpreter = Interpreter::new();
        let value = interpreter.evaluate(r#"
        eval "5 + 7"
        "#).unwrap();
        assert_eq!(value, Int64Value(12))
    }

    #[test]
    fn test_eval_fail() {
        let mut interpreter = Interpreter::new();
        let value = interpreter.evaluate(r#"
        eval 123
        "#).unwrap();
        assert_eq!(value, ErrorValue("Type mismatch - expected String, got i64".into()))
    }

    #[test]
    fn test_lambda_functions() {
        let mut interpreter = Interpreter::new();
        interpreter.evaluate(r#"
        f := lambda (a, b) => a * b
        "#).unwrap();

        let value = interpreter.evaluate(r#"
        f(2, 5)
        "#).unwrap();
        assert_eq!(value, TypedValue::Int64Value(10))
    }

    #[test]
    fn test_named_functions() {
        let mut interpreter = Interpreter::new();
        interpreter.evaluate(r#"
        fn f(a, b) => a * b
        "#).unwrap();

        let value = interpreter.evaluate(r#"
        f(2, 5)
        "#).unwrap();
        assert_eq!(value, TypedValue::Int64Value(10))
    }

    #[test]
    fn test_state_retention() {
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate("x := 5").unwrap();
        assert_eq!(result, TypedValue::Undefined);

        let result = interpreter.evaluate("x¡").unwrap();
        assert_eq!(result, TypedValue::Float64Value(120.))
    }

    #[test]
    fn test_ql_create_table_in_namespace() {
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
        create table ns("interpreter.create.stocks") (
            symbol: String(8),
            exchange: String(8),
            last_sale: f64)
        "#).unwrap();
        assert_eq!(result, TypedValue::Boolean(true))
    }

    #[test]
    fn test_ql_delete_from_namespace() {
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
            delete from ns("interpreter.delete.stocks")
            where last_sale >= 1.0
        "#).unwrap();
        assert_eq!(result, TypedValue::RowID(3));

        // verify the remaining rows
        assert_eq!(df.read_fully().unwrap(), vec![
            make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
            make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428),
        ]);
    }

    #[test]
    fn test_ql_into_namespace() {
        // create a table with test data
        let columns = make_quote_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let ns = Namespace::parse("interpreter.into.stocks").unwrap();
        let mut df = make_dataframe_ns(ns, columns.clone()).unwrap();
        assert_eq!(1, df.append(&make_quote(0, &phys_columns, "ABC", "AMEX", 11.77)).unwrap());
        assert_eq!(1, df.append(&make_quote(1, &phys_columns, "UNO", "OTC", 0.2456)).unwrap());
        assert_eq!(1, df.append(&make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66)).unwrap());
        assert_eq!(1, df.append(&make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428)).unwrap());

        // insert some rows
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
            into ns("interpreter.into.stocks")
            from { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 }
        "#).unwrap();
        assert_eq!(result, TypedValue::RowID(1));

        // verify the remaining rows
        assert_eq!(df.read_fully().unwrap(), vec![
            make_quote(0, &phys_columns, "ABC", "AMEX", 11.77),
            make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
            make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66),
            make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428),
            make_quote(4, &phys_columns, "BOOM", "NYSE", 56.88),
        ]);
    }

    #[test]
    fn test_ql_overwrite_rows_in_namespace() {
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
            overwrite ns("interpreter.overwrite.stocks")
            via {symbol: "BOOM", exchange: "NYSE", last_sale: 56.99}
            where symbol == "BOOM"
        "#).unwrap();
        assert_eq!(result, TypedValue::RowID(1));

        // verify the remaining rows
        assert_eq!(df.read_fully().unwrap(), vec![
            make_quote(0, &phys_columns, "ABC", "AMEX", 11.77),
            make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
            make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66),
            make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428),
            make_quote(4, &phys_columns, "BOOM", "NYSE", 56.99),
        ]);
    }

    #[test]
    fn test_ql_select_from_namespace() {
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
            select symbol, exchange, last_sale
            from ns("interpreter.select.stocks")
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
    fn test_ql_select_from_variable() {
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
    fn test_ql_table_lifecycle_in_namespace() {
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
            drop table ns("interpreter.create.stocks")
        "#).unwrap();
        assert!(matches!(result, TypedValue::Boolean(_)));

        let result = interpreter.evaluate(r#"
            create table ns("interpreter.create.stocks") (
                symbol: String(8),
                exchange: String(8),
                last_sale: f64
            )
        "#).unwrap();
        assert_eq!(result, TypedValue::Boolean(true));

        let result = interpreter.evaluate(r#"
            into ns("interpreter.create.stocks")
                from { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 }
        "#).unwrap();
        assert_eq!(result, TypedValue::RowID(1));

        let result = interpreter.evaluate(r#"
            into ns("interpreter.create.stocks")
                from { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 }
        "#).unwrap();
        assert_eq!(result, TypedValue::RowID(1));

        let result = interpreter.evaluate(r#"
            into ns("interpreter.create.stocks")
                from { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }
        "#).unwrap();
        assert_eq!(result, TypedValue::RowID(1));

        let result = interpreter.evaluate(r#"
            from ns("interpreter.create.stocks")
        "#).unwrap();
        let phys_columns = make_table_columns();
        assert_eq!(result, TypedValue::TableValue(ModelRowCollection::from_rows(vec![
            make_quote(0, &phys_columns, "ABC", "AMEX", 12.49),
            make_quote(1, &phys_columns, "BOOM", "NYSE", 56.88),
            make_quote(2, &phys_columns, "JET", "NASDAQ", 32.12),
        ])));
    }

    #[test]
    fn test_variables_indirectly() {
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
            x := 5
            y := 7
            x * y
        "#).unwrap();
        assert_eq!(result, TypedValue::Int64Value(35));
    }
}