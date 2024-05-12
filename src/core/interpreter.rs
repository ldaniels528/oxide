////////////////////////////////////////////////////////////////////
// interpreter module
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

use crate::compiler::CompilerState;
use crate::machine::MachineState;
use crate::token_slice::TokenSlice;
use crate::typed_values::TypedValue;

/// Represents the language interpreter.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Interpreter {
    compiler: CompilerState,
    machine: MachineState,
}

impl Interpreter {
    /// Constructs a new Interpreter
    pub fn new() -> Self {
        Interpreter {
            compiler: CompilerState::new(),
            machine: MachineState::new(),
        }
    }

    /// Executes the supplied source code returning the result of the evaluation
    pub fn evaluate(&mut self, source_code: &str) -> std::io::Result<TypedValue> {
        let ts = TokenSlice::from_string(source_code);
        let (opcodes, _) = self.compiler.compile_all(ts)?;
        let (ms, result) = self.machine.evaluate_scope(&opcodes)?;
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
    use crate::testdata::{make_dataframe_ns, make_quote, make_quote_columns};
    use crate::typed_values::TypedValue::{Boolean, Float64Value, Int64Value, RecordNumber, TableValue, Undefined};

    #[test]
    fn test_evaluate_n_pow_2() {
        let mut interpreter = Interpreter::new();
        interpreter.with_variable("n", Int64Value(5));
        let result = interpreter.evaluate("n ** 2").unwrap();
        assert_eq!(result, Int64Value(25))
    }

    #[test]
    fn test_evaluate_n_gt_5() {
        let mut interpreter = Interpreter::new();
        interpreter.with_variable("n", Int64Value(7));
        let result = interpreter.evaluate("n > 5").unwrap();
        assert_eq!(result, Boolean(true))
    }

    #[test]
    fn test_state_retention() {
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate("x := 5").unwrap();
        assert_eq!(result, Undefined);

        let result = interpreter.evaluate("x¡").unwrap();
        assert_eq!(result, Float64Value(120.))
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
        assert_eq!(result, RecordNumber(3));

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
            from { symbol: "BOOM", exchange: "NYSE", last_sale: 56.99 }
            "#).unwrap();
        assert_eq!(result, RecordNumber(1));

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
        assert_eq!(result, RecordNumber(1));

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
        assert_eq!(1, df.append(&make_quote(4, &phys_columns, "BOOM", "NASDAQ", 56.87)).unwrap());

        // compile and execute the code
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"
            select symbol, exchange, last_sale
            from ns("interpreter.select.stocks")
            where last_sale > 1.0
            order by symbol
            limit 5
            "#).unwrap();
        assert_eq!(result, TableValue(ModelRowCollection::from_rows(vec![
            make_quote(0, &phys_columns, "ABC", "AMEX", 11.77),
            make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66),
            make_quote(4, &phys_columns, "BOOM", "NASDAQ", 56.87),
        ])));
    }

    #[test]
    fn test_ql_select_from_variable() {
        // stage the data
        let phys_columns = TableColumn::from_columns(&make_quote_columns()).unwrap();
        let mut interpreter = Interpreter::new();
        interpreter.with_variable("stocks", TableValue(
            ModelRowCollection::from_rows(vec![
                make_quote(0, &phys_columns, "ABC", "AMEX", 11.77),
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
        assert_eq!(result, TableValue(ModelRowCollection::from_rows(vec![
            make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
            make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428),
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
        assert_eq!(result, Int64Value(35));
    }
}