////////////////////////////////////////////////////////////////////
// interpreter module
////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

use crate::compiler::Compiler;
use crate::machine::MachineState;
use crate::token_slice::TokenSlice;
use crate::typed_values::TypedValue;

/// Represents the language interpreter.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Interpreter {
    compiler: Compiler,
    machine: MachineState,
}

impl Interpreter {
    /// Constructs a new Interpreter
    pub fn build() -> Interpreter {
        Interpreter {
            compiler: Compiler::new(),
            machine: MachineState::build(),
        }
    }

    /// Executes the supplied source code returning the result of the evaluation
    pub fn evaluate(&mut self, source_code: &str) -> std::io::Result<TypedValue> {
        let ts = TokenSlice::from_string(source_code);
        let (opcodes, _) = self.compiler.compile_all(ts)?;
        let (ms, result) = self.machine.evaluate_all(&opcodes)?;
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
    use crate::byte_row_collection::ByteRowCollection;
    use crate::interpreter::Interpreter;
    use crate::namespaces::Namespace;
    use crate::table_columns::TableColumn;
    use crate::testdata::{make_columns, make_dataframe_ns, make_quote};
    use crate::typed_values::TypedValue::{Float64Value, Int64Value, MemoryTable, Undefined};

    #[test]
    fn test_session() {
        let mut interpreter = Interpreter::build();
        let result = interpreter.evaluate(r#"
        x := 5
        "#.to_string().as_str()).unwrap();
        assert_eq!(result, Undefined);

        let result = interpreter.evaluate(r#"
        x¡
        "#.to_string().as_str()).unwrap();
        assert_eq!(result, Float64Value(120.))
    }

    #[ignore]
    #[test]
    fn test_sql_into_namespace() {
        // create a table with test data
        let columns = make_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let ns = Namespace::parse("interpreter.append.stocks").unwrap();
        let mut df = make_dataframe_ns(ns, columns.clone()).unwrap();
        assert_eq!(1, df.append(&make_quote(0, &phys_columns, "ABC", "AMEX", 11.77)).unwrap());
        assert_eq!(1, df.append(&make_quote(1, &phys_columns, "UNO", "OTC", 0.2456)).unwrap());
        assert_eq!(1, df.append(&make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66)).unwrap());
        assert_eq!(1, df.append(&make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428)).unwrap());

        // insert some rows
        let mut interpreter = Interpreter::build();
        let result = interpreter.evaluate(r#"
            into ns("interpreter.append.stocks") (symbol, exchange, last_sale) [("BOOM", "NYSE", 56.99)]
            "#).unwrap();
        assert_eq!(result, Int64Value(3));

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
    fn test_sql_delete_from_namespace() {
        // create a table with test data
        let columns = make_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let ns = Namespace::parse("interpreter.delete.stocks").unwrap();
        let mut df = make_dataframe_ns(ns, columns.clone()).unwrap();
        assert_eq!(1, df.append(&make_quote(0, &phys_columns, "ABC", "AMEX", 11.77)).unwrap());
        assert_eq!(1, df.append(&make_quote(1, &phys_columns, "UNO", "OTC", 0.2456)).unwrap());
        assert_eq!(1, df.append(&make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66)).unwrap());
        assert_eq!(1, df.append(&make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428)).unwrap());
        assert_eq!(1, df.append(&make_quote(4, &phys_columns, "BOOM", "NASDAQ", 56.87)).unwrap());

        // delete some rows
        let mut interpreter = Interpreter::build();
        let result = interpreter.evaluate(r#"
            delete from ns("interpreter.delete.stocks")
            where last_sale >= 1.0
            "#).unwrap();
        assert_eq!(result, Int64Value(3));

        // verify the remaining rows
        assert_eq!(df.read_fully().unwrap(), vec![
            make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
            make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428),
        ]);
    }

    #[ignore]
    #[test]
    fn test_sql_overwrite_rows_in_namespace() {
        // create a table with test data
        let columns = make_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let mut interpreter = Interpreter::build();
        interpreter.with_variable("stocks", MemoryTable(
            ByteRowCollection::from_rows(phys_columns.clone(), vec![
                make_quote(0, &phys_columns, "ABC", "AMEX", 11.77),
                make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
                make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66),
                make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428),
                make_quote(4, &phys_columns, "BOOM", "NASDAQ", 56.87),
            ])
        ));

        // overwrite a row
        let result = interpreter.evaluate(r#"
            overwrite stocks (symbol, exchange, last_sale)
            values ("BOOM", "NYSE", 56.99)
            where symbol == "BOOM"
            "#).unwrap();
        assert_eq!(result, Int64Value(3));

        // verify the remaining rows
        assert_eq!(interpreter.evaluate("stocks").unwrap(), MemoryTable(
            ByteRowCollection::from_rows(phys_columns.clone(), vec![
                make_quote(0, &phys_columns, "ABC", "AMEX", 11.77),
                make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
                make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66),
                make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428),
                make_quote(4, &phys_columns, "BOOM", "NYSE", 56.99),
            ])));
    }

    #[test]
    fn test_sql_select_from_namespace() {
        // create a table with test data
        let columns = make_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        let mut interpreter = Interpreter::build();
        interpreter.with_variable("stocks", MemoryTable(
            ByteRowCollection::from_rows(phys_columns.clone(), vec![
                make_quote(0, &phys_columns, "ABC", "AMEX", 11.77),
                make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
                make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66),
                make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428),
                make_quote(4, &phys_columns, "BOOM", "NASDAQ", 56.87)])
        ));

        // compile and execute the code
        let result = interpreter.evaluate(r#"
            select symbol, exchange, last_sale
            from stocks
            where last_sale > 1.0
            order by symbol
            limit 5
            "#).unwrap();
        assert_eq!(result, MemoryTable(ByteRowCollection::from_rows(phys_columns.clone(), vec![
            make_quote(0, &phys_columns, "ABC", "AMEX", 11.77),
            make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66),
            make_quote(4, &phys_columns, "BOOM", "NASDAQ", 56.87),
        ])));
    }

    #[test]
    fn test_sql_select_from_variable() {
        // stage the data
        let phys_columns = TableColumn::from_columns(&make_columns()).unwrap();
        let mut interpreter = Interpreter::build();
        interpreter.with_variable("stocks", MemoryTable(
            ByteRowCollection::from_rows(phys_columns.clone(), vec![
                make_quote(0, &phys_columns, "ABC", "AMEX", 11.77),
                make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
                make_quote(2, &phys_columns, "BIZ", "NYSE", 23.66),
                make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428),
                make_quote(4, &phys_columns, "BOOM", "NASDAQ", 56.87)])
        ));

        // execute the code
        let result = interpreter.evaluate(r#"
            select symbol, exchange, last_sale
            from stocks
            where last_sale < 1.0
            order by symbol
            limit 5
            "#).unwrap();
        assert_eq!(result, MemoryTable(ByteRowCollection::from_rows(phys_columns.clone(), vec![
            make_quote(1, &phys_columns, "UNO", "OTC", 0.2456),
            make_quote(3, &phys_columns, "GOTO", "OTC", 0.1428),
        ])));
    }

    #[test]
    fn test_variables_indirectly() {
        let mut interpreter = Interpreter::build();
        let result = interpreter.evaluate(r#"
            x := 5
            y := 7
            x * y
        "#).unwrap();
        assert_eq!(result, Int64Value(35));
    }
}