#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// QueryEngine classes
////////////////////////////////////////////////////////////////////

use crate::columns::Column;
use crate::cursor::Cursor;
use crate::data_types::DataType;
use crate::data_types::DataType::TableType;

use crate::compiler;
use crate::dataframe::Dataframe;
use crate::dataframe::Dataframe::*;
use crate::errors::Errors::*;
use crate::errors::TypeMismatchErrors::{CollectionExpected, UnsupportedType};
use crate::errors::{throw, SyntaxErrors, TypeMismatchErrors};
use crate::expression::Expression::*;
use crate::expression::{Conditions, Expression};
use crate::machine::Machine;
use crate::model_row_collection::ModelRowCollection;
use crate::numbers::Numbers::I64Value;
use crate::parameter::Parameter;
use crate::row_collection::RowCollection;
use crate::sequences::Sequence;
use crate::structures::Row;
use crate::structures::Structure;
use crate::structures::Structures::{Firm, Soft};
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::From;
use std::ops::Deref;

/// Pulls a row (retrieves then deletes it) from a table structure 
/// #### Examples
/// ```
/// stock <~ stocks
/// ```
/// ```
/// stock <~ (stocks where exchange is "NASDAQ")
/// ```
pub fn eval_pull_row(
    ms: &Machine,
    container: &Expression,
    source: &Expression,
) -> std::io::Result<(Machine, TypedValue)> {
    // extract the query components
    let (from, condition, _limit) =
        compiler::unwind_sql_update_graph(source.clone(), |from, condition, limit| {
            (from, condition, limit)
        })?;

    // get a reference to the dataframe
    let (ms, mut df) = ms.eval_as_dataframe(&from)?;

    // pop the next row from the table
    let row = df.pop_matching_row(&condition);

    // return the table
    match container {
        Identifier(name) => Ok((ms.with_variable(name, row), Boolean(true))),
        other => throw(Exact(other.to_code()))
    }
}

/// Pulls all qualifying rows (retrieves then deletes them) from a table structure
/// #### Examples
/// ```
/// my_stocks <<~ (stocks where exchange is "NASDAQ" limit 3)
/// ```
/// ```
/// my_stocks <<~ (stocks limit 3)
/// ```
/// ```
/// remaining_stocks <<~ stocks
/// ```
pub fn eval_pull_rows(
    ms: &Machine,
    container: &Expression,
    source: &Expression,
) -> std::io::Result<(Machine, TypedValue)> {
    // extract the query components
    let (from, condition, limit) =
        compiler::unwind_sql_update_graph(source.clone(), |from, condition, limit| {
            (from, condition, limit)
        })?;

    // get a reference to the dataframe
    let (ms, mut df) = ms.eval_as_dataframe(&from)?;
    
    // determine the optional limit
    let (ms, limit) = ms.evaluate_opt(&limit)?;
    let limit = limit.map(|result| result.to_usize());

    // pull the rows up to the limit
    let (mut count, mut done) = (0, false);
    let mut mrc = ModelRowCollection::new(df.get_columns().clone());
    while !done && (limit.is_none() || limit.is_some_and(|n| count < n)) {
        match df.pop_matching_row(&condition) {
            Structured(Firm(row, _)) => { 
                mrc.overwrite_row(count, row)?; 
                count += 1
            }
            _ => done = true
        }
    }
    
    // return the table
    match container {
        Identifier(name) => Ok((ms.with_variable(name, TableValue(Model(mrc))), Boolean(true))),
        other => throw(Exact(other.to_code()))
    }
}

/// Evaluates the queryable [Expression] (e.g. from, limit and where)
/// #### Examples
/// ```
/// let stocks = nsd::load("query_engine.select.stocks")
/// stocks where last_sale > 1.0 limit 1
/// ```
pub fn eval_table_or_view_row_selection(
    ms: &Machine,
    src: &Expression,
    condition: &Conditions,
    limit: &TypedValue,
) -> std::io::Result<(Machine, TypedValue)> {
    let (machine, df) = ms.eval_as_dataframe(src)?;
    let limit = limit.to_usize();
    let columns = df.get_columns().clone();
    let mut cursor = Cursor::filter(Box::new(df), condition.to_owned());
    let mrc = ModelRowCollection::from_columns_and_rows(&columns, &cursor.take(limit)?);
    Ok((machine, TableValue(Model(mrc))))
}

/// Performs an insert, update or overwrite of row(s) within a table.
/// #### Examples
/// ##### insert a row into the table
/// ```
/// { symbol: "XYZ", exchange: "NASDAQ", last_sale: 0.0872 } 
///     ~> stocks
/// ```
/// ##### update a row within the table
/// ```
/// { symbol: "BKP", last_sale: 0.1421 } 
///    ~> (stocks where symbol is "BKPQ")
/// ```
/// ##### overwrite a row within the table
/// ```
/// { symbol: "BKP", exchange: "OTCBB", last_sale: 0.1421 } 
///    ~>> (stocks where symbol is "BKPQ")
/// ```
pub fn eval_write_to_target(
    ms: &Machine,
    source: &Expression,
    target: &Expression,
    condition: Option<Conditions>,
    limit: Option<Box<Expression>>,
    is_overwrite: bool,
) -> std::io::Result<(Machine, TypedValue)> {
    // recursively unwind the target options
    match target {
        GroupBy { .. } => throw(Exact("group_by is not supported in a write context".into())),
        Having { .. } => throw(Exact("having is not supported in a write context".into())),
        Limit { from, limit } => {
            eval_write_to_target(ms, source, from, condition, Some(limit.deref().clone().into()), is_overwrite)
        }
        OrderBy { .. } => throw(Exact("order_by is not supported in a write context".into())),
        Select { .. } => throw(Exact("order_by is not supported in a write context".into())),
        Where { from, condition } => {
            eval_write_to_target(ms, source, from, Some(condition.clone()), limit, is_overwrite)
        }
        // perform the update
        _ =>
            match &condition {
                None => do_table_row_insert(ms, source, target),
                Some(..) if is_overwrite =>
                    do_table_row_overwrite(ms, target, source, &condition, &limit),
                Some(..) =>
                    do_table_row_update(ms, target, source, &condition, &limit)
            }
    }
}

/// Evaluates table-row insert statement
/// #### Examples
/// ```
/// let stocks = nsd::load("query_engine.examples.stocks")
/// { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 } ~> stocks
/// ```
/// ```
/// let stocks = nsd::load("query_engine.examples.stocks")
/// [{ symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
///  { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
///  { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
/// ```
pub fn do_table_row_insert(
    ms: &Machine,
    source: &Expression,
    target: &Expression,
) -> std::io::Result<(Machine, TypedValue)>  {
    let ms = ms.to_owned();
    let (machine, rows) = match source { 
        Literal(Kind(TableType(_params))) => (ms, vec![]),
        Literal(TableValue(rc)) => (ms, rc.get_rows()),
        source => do_table_rows_from_query(&ms, source, target)?,
    };

    // write the rows to the target
    let mut inserted = 0;
    let mut rc = expect_row_collection(&machine, target)?;
    inserted += rc.append_rows(rows)?;
    Ok((machine, Number(I64Value(inserted))))
}

/// Evaluates a SQL DELETE operation
/// #### Examples
/// ```
/// let stocks = nsd::load("query_engine.select.stocks")
/// delete stocks where last_sale > 1.0 limit 1
/// ```
pub fn do_table_row_delete(
    ms: &Machine,
    from: &Expression,
    condition: &Option<Conditions>,
    limit: &Option<Box<Expression>>,
) -> std::io::Result<(Machine, TypedValue)> {
    let (ms, limit) = ms.evaluate_or_undef(limit)?;
    let (ms, table) = ms.evaluate(from)?;
    let mut df = table.to_dataframe()?;
    Ok((ms.clone(), df.delete_where(&ms, &condition, limit)?))
}

/// Evaluates a SQL UPDATE/REPLACE operation
/// #### Examples
/// ```
/// let stocks = nsd::load("query_engine.select.stocks")
/// { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 } 
///     ~>> (stocks where last_sale > 1.0 limit 1)
/// ```
pub fn do_table_row_overwrite(
    ms: &Machine,
    target: &Expression,
    source: &Expression,
    condition: &Option<Conditions>,
    limit: &Option<Box<Expression>>,
) -> std::io::Result<(Machine, TypedValue)> {
    let (ms, limit) = ms.evaluate_or_undef(limit)?;
    let (ms, df) = ms.eval_as_dataframe(target)?;
    let (fields, values) = separate_fields_and_values(&ms, &target, &source)?;
    let (_, overwritten) = Dataframe::overwrite_where(df, &ms, &fields, &values, condition, limit)?;
    Ok((ms, overwritten))
}

/// Evaluates a SQL UNDELETE/RESTORE operation
/// #### Examples
/// ```
/// let stocks = nsd::load("query_engine.select.stocks")
/// undelete stocks where last_sale > 1.0 limit 1
/// ```
pub fn do_table_row_undelete(
    ms: &Machine,
    from: &Expression,
    condition: &Option<Conditions>,
    limit: &Option<Box<Expression>>,
) -> std::io::Result<(Machine, TypedValue)> {
    let (ms, limit) = ms.evaluate_or_undef(limit)?;
    let (ms, table) = ms.evaluate(from)?;
    let mut df = table.to_dataframe()?;
    df.undelete_where(&ms, &condition, limit).map(|restored| (ms, restored))
}

/// Evaluates a SQL UPDATE/MODIFY operation
/// #### Examples
/// ```
/// let stocks = nsd::load("query_engine.select.stocks")
/// { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 } 
///     ~> (stocks where last_sale > 1.0 limit 1)
/// ```
pub fn do_table_row_update(
    ms: &Machine,
    table: &Expression,
    source: &Expression,
    condition: &Option<Conditions>,
    limit: &Option<Box<Expression>>,
) -> std::io::Result<(Machine, TypedValue)> {
    let (ms, limit) = ms.evaluate_or_undef(limit)?;
    let (ms, df) = ms.eval_as_dataframe(table)?;
    let (fields, values) = separate_fields_and_values(&ms, &table, &source)?;
    Dataframe::update_where(df, &ms, &fields, &values, &condition, limit)
        .map(|modified| (ms, modified))
}

pub fn do_table_rows_from_query(
    ms: &Machine,
    source: &Expression,
    table: &Expression,
) -> std::io::Result<(Machine, Vec<Row>)> {
    // determine the row collection
    let rc = expect_row_collection(ms, table)?;

    // retrieve rows from the source
    let rows = expect_rows(ms, source, rc.get_columns())?;
    Ok((ms.clone(), rows))
}

/// Evaluates a SQL query components (where, limit, select, order_by, group_by)
/// #### Examples
/// ```
/// select symbol, exchange, last_sale 
/// from stocks 
/// where last_sale > 1.0 limit 1
/// order_by last_sale
/// ```
pub fn do_table_query(
    ms: &Machine,
    query_comp: &Expression,
) -> std::io::Result<(Machine, TypedValue)> {
    compiler::unwind_sql_query_graph(
        query_comp.clone(),
        |from, fields, condition, group_by, having, order_by, limit| {
            let cond = match condition {
                None => Conditions::True,
                Some(Literal(Undefined)) => Conditions::True,
                Some(Condition(cond)) => cond,
                Some(z) => return throw(TypeMismatch(TypeMismatchErrors::BooleanExpected(z.to_code())))
            };
            if fields.is_empty() {
                let (ms, limit) = ms.evaluate_or_undef(&limit)?;
                eval_table_or_view_row_selection(&ms, &from, &cond, &limit)
            } else {
                let from = Some(from.clone().into());
                do_select(&ms, &fields, &from, &Some(cond), &group_by, &having, &order_by, &limit)
            }
        })
}

/// Evaluates a SQL SELECT query
/// #### Examples
/// ```
/// let stocks = nsd::load("query_engine.select.stocks")
/// select symbol, exchange, last_sale 
/// from stocks 
/// where last_sale > 1.0 limit 1
/// order_by last_sale
/// ```
pub fn do_select(
    ms: &Machine,
    fields: &Vec<Expression>,
    from: &Option<Box<Expression>>,
    condition: &Option<Conditions>,
    group_by: &Option<Vec<Expression>>,
    having: &Option<Box<Expression>>,
    order_by: &Option<Vec<Expression>>,
    limit: &Option<Box<Expression>>,
) -> std::io::Result<(Machine, TypedValue)> {
    let (ms, table_v) = ms.evaluate_or_undef(from)?;
    match table_v {
        TableValue(rc) => do_select_from_dataframe(ms, rc, fields, condition, group_by, having, order_by, limit),
        z => throw(TypeMismatch(CollectionExpected(z.to_code())))
    }
}

fn do_select_from_dataframe(
    ms: Machine,
    df0: Dataframe,
    fields: &Vec<Expression>,
    condition: &Option<Conditions>,
    group_by: &Option<Vec<Expression>>,
    having: &Option<Box<Expression>>,
    order_by: &Option<Vec<Expression>>,
    limit: &Option<Box<Expression>>,
) -> std::io::Result<(Machine, TypedValue)> {
    // cache the initial state
    let ms0 = ms.clone();

    // step 1: determine output layout and limits
    let (_, rc1, new_columns, limit) =
        step_1_determine_layout_and_limit(ms, df0, fields, limit)?;

    // step 2: transform the eligible rows
    let (_, rc2) =
        match step_2_transform_eligible_rows(&ms0, &rc1, fields, &new_columns, condition)? {
            (ms, TableValue(rc)) => (ms, rc),
            (_ms, other) => return throw(TypeMismatch(UnsupportedType(
                TableType(new_columns.iter()
                              .map(|c| c.to_parameter())
                              .collect::<Vec<_>>()),
                other.get_type(),
            ))),
        };

    // step 3: aggregate the dataset
    let rc3 = match group_by {
        Some(agg_fields) => step_3_aggregate_table(rc2, agg_fields, having)?,
        None => rc2
    };

    // step 4: sort the dataset
    let rc4 = match order_by {
        Some(order_fields) => step_4_sort_table(rc3, order_fields)?,
        None => rc3
    };

    // step 5: limit the dataset
    let rc5 = match limit {
        Number(cut_off) => step_5_limit_table(rc4, cut_off.to_usize())?,
        _ => rc4
    };

    // return the table value
    Ok((ms0, TableValue(rc5)))
}

fn step_1_determine_layout_and_limit(
    ms0: Machine,
    rc0: Dataframe,
    fields: &Vec<Expression>,
    limit: &Option<Box<Expression>>,
) -> std::io::Result<(Machine, Dataframe, Vec<Column>, TypedValue)> {
    let columns = rc0.get_columns();
    let (ms, limit) = ms0.evaluate_or_undef(limit)?;
    let new_columns = resolve_fields_as_columns(columns, fields)?;
    Ok((ms, rc0, new_columns, limit))
}

fn step_2_transform_eligible_rows(
    ms: &Machine,
    rc1: &Dataframe,
    fields: &Vec<Expression>,
    new_columns: &Vec<Column>,
    condition: &Option<Conditions>,
) -> std::io::Result<(Machine, TypedValue)> {
    let (ms, result) = match transform_table(ms, rc1, fields, new_columns, condition)? {
        (ms, TableValue(rc)) => (ms, TableValue(rc)),
        (_ms, other) => return throw(TypeMismatch(UnsupportedType(
            TableType(vec![]),
            other.get_type(),
        ))),
    };
    Ok((ms, result))
}

fn step_3_aggregate_table(
    src: Dataframe,
    group_by: &Vec<Expression>,
    having: &Option<Box<Expression>>,
) -> std::io::Result<Dataframe> {
    // TODO add aggregation code to rc
    Ok(src)
}

fn step_4_sort_table(
    src: Dataframe,
    sort_fields: &Vec<Expression>,
) -> std::io::Result<Dataframe> {
    // TODO add sorting code to rc
    Ok(src)
}

fn step_5_limit_table(
    src: Dataframe,
    cut_off: usize,
) -> std::io::Result<Dataframe> {
    let mut dest = ModelRowCollection::new(src.get_columns().clone());
    let mut count: usize = 0;
    for row in src.iter() {
        if count >= cut_off { break; }
        dest.overwrite_row(row.get_id(), row)?;
        count += 1;
    }
    Ok(Model(dest))
}

fn expect_row_collection(
    ms: &Machine,
    table: &Expression,
) -> std::io::Result<Box<dyn RowCollection>> {
    let (_, v_table) = ms.evaluate(table)?;
    v_table.to_table()
}

fn expect_rows(
    ms: &Machine,
    source: &Expression,
    columns: &Vec<Column>,
) -> std::io::Result<Vec<Row>> {
    let (_, source) = ms.evaluate(source)?;
    match source {
        ErrorValue(err) => throw(err),
        ArrayValue(items) => {
            let mut rows = Vec::new();
            for tuples in items.iter() {
                if let Structured(structure) = tuples {
                    rows.push(Row::from_tuples(0, columns, &structure.to_name_values()))
                }
            }
            Ok(rows)
        }
        Structured(s) => Ok(vec![Row::from_tuples(0, columns, &s.to_name_values())]),
        TableValue(rcv) => Ok(rcv.get_rows()),
        tv => throw(TypeMismatch(UnsupportedType(TableType(Parameter::from_columns(columns)), tv.get_type())))
    }
}

fn separate_fields_and_values(
    ms: &Machine,
    target: &Expression,
    source: &Expression,
) -> std::io::Result<(Vec<Expression>, Vec<Expression>)> {
    if let (_, Structured(Soft(structure))) = ms.evaluate(&source)? {
        let writable = expect_row_collection(&ms, target)?;
        let columns = writable.get_columns();
        let row = Row::from_tuples(0, columns, &structure.to_name_values());
        let (fields, values) = split_row_into_fields_and_values(columns, &row);
        Ok((fields, values))
    } else {
        throw(Exact("Expected a data object".to_string()))
    }
}

/// Returns a tuple containing fields and values from the given [Column]s and [Row]s
fn split_row_into_fields_and_values(
    columns: &Vec<Column>, 
    row: &Row
) -> (Vec<Expression>, Vec<Expression>) {
    let my_fields = columns.iter()
        .map(|tc| Identifier(tc.get_name().to_string()))
        .collect::<Vec<_>>();
    let my_values = row.get_values().iter()
        .map(|v| Literal(v.to_owned()))
        .collect::<Vec<_>>();
    (my_fields, my_values)
}

fn transform_table(
    ms0: &Machine,
    rc0: &Dataframe,
    field_values: &Vec<Expression>,
    field_columns: &Vec<Column>,
    condition: &Option<Conditions>,
) -> std::io::Result<(Machine, TypedValue)> {
    let columns = rc0.get_columns();
    let mut rc1 = ModelRowCollection::new(field_columns.clone());
    for row in rc0.iter() {
        let ms = row.pollute(&ms0, columns);
        if row.matches(&ms, condition, columns) {
            match ms.eval_as_array(field_values)? {
                (_ms, ArrayValue(array)) => {
                    let new_row = Row::new(row.get_id(), array.get_values().clone());
                    rc1.overwrite_row(new_row.get_id(), new_row);
                }
                (_ms, z) => return throw(TypeMismatch(TypeMismatchErrors::ArrayExpected(z.to_string())))
            }
        }
    }
    Ok((ms0.clone(), TableValue(Model(rc1))))
}

fn resolve_fields_as_columns(
    columns: &Vec<Column>,
    fields: &Vec<Expression>,
) -> std::io::Result<Vec<Column>> {
    // create a lookup for column-name-to-data-type from the source columns
    let column_dict: HashMap<String, DataType> = columns.iter()
        .map(|c| (c.get_name().to_string(), c.get_data_type().clone()))
        .collect();

    // build the field vector (new_columns)
    let mut offset = Row::overhead();
    let mut new_columns = Vec::new();
    for field in fields {
        let column = resolve_field_as_column(field, columns, &column_dict, offset)?;
        let fixed_size = column.get_data_type().compute_fixed_size();
        new_columns.push(column);
        offset += fixed_size;
    }
    Ok(new_columns)
}

fn resolve_field_as_column(
    field: &Expression,
    columns: &Vec<Column>,
    column_dict: &HashMap<String, DataType>,
    offset: usize,
) -> std::io::Result<Column> {
    match field {
        // label: value
        NamedValue(label, expr) =>
            match expr.deref() {
                // price: last_sale
                Identifier(name) =>
                    match column_dict.get(name) {
                        Some(dt) => Ok(Column::new(label, dt.clone(), Null, offset)),
                        None => column_not_found(name, columns),
                    }
                // md5sum: util::md5(sku)
                other =>
                    match Expression::infer(other) {
                        dt => Ok(Column::new(label, dt.clone(), Null, offset)),
                    }
            }
        // last_sale
        Identifier(name) =>
            match column_dict.get(name) {
                Some(dt) => Ok(Column::new(name, dt.clone(), Null, offset)),
                None => column_not_found(name, columns),
            }
        other =>
            throw(SyntaxError(SyntaxErrors::TypeIdentifierExpected(other.to_code())))
    }
}

fn column_not_found<A>(name: &str, columns: &Vec<Column>) -> std::io::Result<A> {
    throw(ColumnNotFoundInColumns(name.into(), columns.iter()
        .map(|c| c.get_name().into()).collect::<Vec<_>>()))
}

/// Oxide QL tests
#[cfg(test)]
mod tests {
    use crate::columns::Column;
    use crate::dataframe::Dataframe::Model;
    use crate::interpreter::Interpreter;
    use crate::model_row_collection::ModelRowCollection;
    use crate::testdata::*;
    use crate::typed_values::TypedValue::*;

    #[test]
    fn test_table_literal() {
        verify_exact_table(r#"
            let stocks =
                |--------------------------------------|
                | symbol | exchange | last_sale | rank |
                |--------------------------------------|
                | BOOM   | NYSE     | 113.76    | 1    |
                | ABC    | AMEX     | 24.98     | 2    |
                | JET    | NASDAQ   | 64.24     | 3    |
                |--------------------------------------|
            stocks::reverse()
            "#, vec![
            "|-------------------------------------------|", 
            "| id | symbol | exchange | last_sale | rank |", 
            "|-------------------------------------------|", 
            "| 0  | JET    | NASDAQ   | 64.24     | 3    |", 
            "| 1  | ABC    | AMEX     | 24.98     | 2    |", 
            "| 2  | BOOM   | NYSE     | 113.76    | 1    |", 
            "|-------------------------------------------|"]);
    }

    #[test]
    fn test_table_new() {
        verify_exact_value(r#"
            Table::new(
                symbol: String(8),
                exchange: String(8),
                last_sale: f64
            )
        "#, TableValue(Model(ModelRowCollection::with_rows(
            make_quote_columns(), Vec::new(),
        ))))
    }

    #[test]
    fn test_table_save_table_literal_in_namespace() {
        let params = make_quote_parameters();
        let columns = Column::from_parameters(&params);
        let mut interpreter = Interpreter::new();

        // create the table in a namespace
        interpreter = verify_exact_code_with(interpreter, r#"
            stocks = nsd::save(
                "query_engine.literal.stocks",
                |--------------------------------------|
                | symbol | exchange | last_sale | rank |
                |--------------------------------------|
                | BOOM   | NYSE     | 113.76    | 1    |
                | ABC    | AMEX     | 24.98     | 2    |
                | JET    | NASDAQ   | 64.24     | 3    |
                |--------------------------------------|
            )
        "#, "true");

        // verify the contents
        interpreter = verify_exact_table_with(interpreter, r#"
            stocks
        "#, vec![
            "|-------------------------------------------|", 
            "| id | symbol | exchange | last_sale | rank |", 
            "|-------------------------------------------|", 
            "| 0  | BOOM   | NYSE     | 113.76    | 1    |", 
            "| 1  | ABC    | AMEX     | 24.98     | 2    |", 
            "| 2  | JET    | NASDAQ   | 64.24     | 3    |", 
            "|-------------------------------------------|"]);
        }

    #[test]
    fn test_table_crud_in_namespace() {
        let params = make_quote_parameters();
        let columns = Column::from_parameters(&params);
        let mut interpreter = Interpreter::new();
        
        // create the table in a namespace
        interpreter = verify_exact_code_with(interpreter, r#"
            stocks = nsd::save(
                "query_engine.crud.stocks",
                Table::new(symbol: String(8), exchange: String(8), last_sale: f64)
            )
        "#, "true");

        // insert a row
        interpreter = verify_exact_code_with(interpreter, r#"
            { symbol: "ABC", exchange: "AMEX", last_sale: 11.77 } ~> stocks
        "#, "1");
        
        // insert a collection of rows
        interpreter = verify_exact_code_with(interpreter, r#"
            [{ symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
             { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
             { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
             { symbol: "BOOM", exchange: "NASDAQ", last_sale: 56.87 },
             { symbol: "TRX", exchange: "NASDAQ", last_sale: 7.9311 }] ~> stocks
        "#, "5");

        // remove some rows
        interpreter = verify_exact_code_with(interpreter, r#"
            delete stocks where last_sale > 1.0
        "#, "4");

        // overwrite a row
        interpreter = verify_exact_code_with(interpreter, r#"
            { symbol: "GO2", exchange: "AMEX", last_sale: 0.1421 }
                ~> (stocks where symbol is "GOTO")
        "#, "1");

        // verify the remaining rows
        interpreter = verify_exact_table_with(interpreter, r#"
            stocks
        "#, vec![
            "|------------------------------------|", 
            "| id | symbol | exchange | last_sale |", 
            "|------------------------------------|", 
            "| 1  | UNO    | OTC      | 0.2456    |", 
            "| 3  | GO2    | AMEX     | 0.1421    |", 
            "|------------------------------------|"]);

        // restore the previously deleted rows
        interpreter = verify_exact_code_with(interpreter, r#"
            undelete stocks where last_sale > 1.0
        "#, "4");

        // verify the existing rows
        verify_exact_table_with(interpreter, "stocks", vec![
            "|------------------------------------|",
            "| id | symbol | exchange | last_sale |",
            "|------------------------------------|",
            "| 0  | ABC    | AMEX     | 11.77     |",
            "| 1  | UNO    | OTC      | 0.2456    |",
            "| 2  | BIZ    | NYSE     | 23.66     |",
            "| 3  | GO2    | AMEX     | 0.1421    |",
            "| 4  | BOOM   | NASDAQ   | 56.87     |",
            "| 5  | TRX    | NASDAQ   | 7.9311    |",
            "|------------------------------------|"
        ]);
    }

    #[test]
    fn test_table_lifo_queue_in_namespace() {
        // create the table in a namespace
        let mut interpreter = Interpreter::new();
        interpreter = verify_exact_code_with(interpreter, r#"
            stocks = nsd::save(
                "query_engine.push_and_pull.stocks",
                Table::new(symbol: String(8), exchange: String(8), last_sale: f64)
            )
        "#, "true");

        // push a collection of rows into the table
        interpreter = verify_exact_code_with(interpreter, r#"
            [{ symbol: "BMX", exchange: "NYSE", last_sale: 56.88 },
             { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
             { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 },
             { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
             { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
             { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
             { symbol: "BOOM", exchange: "NASDAQ", last_sale: 56.87 },
             { symbol: "TRX", exchange: "NASDAQ", last_sale: 7.9311 }] ~> stocks
        "#, "8");
        
        // pull a row from the table
        interpreter = verify_exact_code_with(interpreter, r#"
          stock <~ stocks
          stock
        "#, r#"{"exchange":"NASDAQ","last_sale":7.9311,"symbol":"TRX"}"#);

        // pull another row from the table
        interpreter = verify_exact_code_with(interpreter, r#"
          stock <~ stocks
          stock
        "#, r#"{"exchange":"NASDAQ","last_sale":56.87,"symbol":"BOOM"}"#);

        // pull the next 3 rows from the table
        interpreter = verify_exact_table_with(interpreter, r#"
          my_stocks <<~ (stocks limit 3)
          my_stocks
        "#, vec![
            "|------------------------------------|", 
            "| id | symbol | exchange | last_sale |", 
            "|------------------------------------|", 
            "| 0  | GOTO   | OTC      | 0.1428    |", 
            "| 1  | BIZ    | NYSE     | 23.66     |", 
            "| 2  | UNO    | OTC      | 0.2456    |", 
            "|------------------------------------|"]);

        // pull the remaining rows from the table
        interpreter = verify_exact_table_with(interpreter, r#"
          my_stocks <<~ stocks
          my_stocks
        "#, vec![
            "|------------------------------------|", 
            "| id | symbol | exchange | last_sale |", 
            "|------------------------------------|", 
            "| 0  | JET    | NASDAQ   | 32.12     |", 
            "| 1  | ABC    | AMEX     | 12.49     |", 
            "| 2  | BMX    | NYSE     | 56.88     |", 
            "|------------------------------------|"]);
    }

    #[test]
    fn test_table_lifo_queue_with_condition_in_namespace() {
        // create the table in a namespace
        let mut interpreter = Interpreter::new();
        interpreter = verify_exact_code_with(interpreter, r#"
            stocks = nsd::save(
                "query_engine.push_and_pull_cnd.stocks",
                Table::new(symbol: String(8), exchange: String(8), last_sale: f64)
            )
        "#, "true");

        // push a collection of rows into the table
        interpreter = verify_exact_code_with(interpreter, r#"
            [{ symbol: "BMX", exchange: "NYSE", last_sale: 56.88 },
             { symbol: "ABC", exchange: "NASDAQ", last_sale: 12.49 },
             { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 },
             { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
             { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
             { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
             { symbol: "BOOM", exchange: "NASDAQ", last_sale: 56.87 },
             { symbol: "TRX", exchange: "NASDAQ", last_sale: 7.9311 }] ~> stocks
        "#, "8");

        // pull the next 3 NASDAQ rows from the table
        interpreter = verify_exact_table_with(interpreter, r#"
          my_stocks <<~ (stocks where exchange is "NASDAQ" limit 3)
          my_stocks
        "#, vec![
            "|------------------------------------|", 
            "| id | symbol | exchange | last_sale |", 
            "|------------------------------------|", 
            "| 0  | TRX    | NASDAQ   | 7.9311    |", 
            "| 1  | BOOM   | NASDAQ   | 56.87     |", 
            "| 2  | JET    | NASDAQ   | 32.12     |", 
            "|------------------------------------|"]);

        // verify the remaining rows
        interpreter = verify_exact_table_with(interpreter, r#"
          stocks
        "#, vec![
            "|------------------------------------|", 
            "| id | symbol | exchange | last_sale |", 
            "|------------------------------------|", 
            "| 0  | BMX    | NYSE     | 56.88     |", 
            "| 1  | ABC    | NASDAQ   | 12.49     |", 
            "| 3  | UNO    | OTC      | 0.2456    |", 
            "| 4  | BIZ    | NYSE     | 23.66     |", 
            "| 5  | GOTO   | OTC      | 0.1428    |", 
            "|------------------------------------|"]);
    }


    #[test]
    fn test_table_push_then_delete() {
        verify_exact_table(r#"
            stocks = nsd::save(
                "query_engine.into_then_delete.stocks",
                Table::new(symbol: String(8), exchange: String(8), last_sale: f64)
            )
            [{ symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
             { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
             { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
            delete stocks where last_sale < 30.0
            stocks
        "#, vec![
            "|------------------------------------|",
            "| id | symbol | exchange | last_sale |",
            "|------------------------------------|",
            "| 0  | BOOM   | NYSE     | 56.88     |",
            "| 2  | JET    | NASDAQ   | 32.12     |",
            "|------------------------------------|"
        ]);
    }

    #[test]
    fn test_table_select_from_namespace() {
        // create a table with test data
        let columns = make_quote_parameters();
        let phys_columns = Column::from_parameters(&columns);

        // create a table and append some rows
        let mut interpreter = Interpreter::new();
        interpreter = verify_exact_code_with(interpreter, r#"
            stocks = nsd::save(
                "query_engine.select1.stocks",
                Table::new(symbol: String(8), exchange: String(8), last_sale: f64)
            )
            [{ symbol: "ABC", exchange: "AMEX", last_sale: 11.77 },
             { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
             { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
             { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
             { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }] ~> stocks
        "#, "5");

        interpreter = verify_exact_table_with(interpreter, r#"
            stocks where exchange is "OTC"
        "#, vec![
            "|------------------------------------|", 
            "| id | symbol | exchange | last_sale |", 
            "|------------------------------------|", 
            "| 1  | UNO    | OTC      | 0.2456    |", 
            "| 3  | GOTO   | OTC      | 0.1428    |", 
            "|------------------------------------|"]);

        // compile and execute the code
        interpreter = verify_exact_table_with(interpreter, r#"
            select symbol, last_sale from stocks
            where last_sale < 1.0
            order_by symbol
            limit 2
        "#, vec![
            "|-------------------------|",
            "| id | symbol | last_sale |",
            "|-------------------------|",
            "| 1  | UNO    | 0.2456    |",
            "| 3  | GOTO   | 0.1428    |",
            "|-------------------------|"]);
    }

    #[test]
    fn test_table_select_from_variable() {
        // create a table with test data
        let params = make_quote_parameters();
        let columns = Column::from_parameters(&params);

        // set up the interpreter
        verify_exact_table(r#"
            stocks = nsd::save(
                "query_engine.select.stocks",
                Table::new(symbol: String(8), exchange: String(8), last_sale: f64)
            )
            [{ symbol: "ABC", exchange: "AMEX", last_sale: 11.77 },
             { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
             { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
             { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
             { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }] ~> stocks
            select symbol, exchange, last_sale
            from stocks
            where last_sale > 1.0
            order_by symbol
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
    fn test_table_embedded_describe() {
        verify_exact_table(r#"
            stocks = nsd::save(
                "query_engine.embedded_a.stocks",
                Table::new(symbol: String(8), exchange: String(8), history: Table(last_sale: f64, processed_time: Date))
            )
            stocks::describe()
        "#, vec![
            "|-------------------------------------------------------------------------------------------|",
            "| id | name     | type                                        | default_value | is_nullable |",
            "|-------------------------------------------------------------------------------------------|",
            "| 0  | symbol   | String(8)                                   | null          | true        |",
            "| 1  | exchange | String(8)                                   | null          | true        |",
            "| 2  | history  | Table(last_sale: f64, processed_time: Date) | null          | true        |",
            "|-------------------------------------------------------------------------------------------|"])
    }

    #[test]
    fn test_table_embedded_empty() {
        verify_exact_table(r#"
            stocks = nsd::save(
                "query_engine.embedded_b.stocks",
                Table::new(symbol: String(8), exchange: String(8), history: Table(last_sale: f64, processed_time: Date))
            )
            [{ symbol: "BIZ", exchange: "NYSE" }, 
             { symbol: "GOTO", exchange: "OTC" }] ~> stocks
            stocks
        "#, vec![
            "|----------------------------------|",
            "| id | symbol | exchange | history |",
            "|----------------------------------|",
            "| 0  | BIZ    | NYSE     | []      |",
            "| 1  | GOTO   | OTC      | []      |",
            "|----------------------------------|"])
    }

    #[test]
    fn test_table_append_rows_with_embedded_table() {
        verify_exact_table(r#"
            stocks = nsd::save(
                "query_engine.embedded_c.stocks",
                Table::new(symbol: String(8), exchange: String(8), history: Table(last_sale: f64))
            );
            [{ symbol: "BIZ", exchange: "NYSE", history: tools::to_table({ last_sale: 23.66 }) },
             { symbol: "GOTO", exchange: "OTC", history: tools::to_table([
                    { last_sale: 0.051 }, 
                    { last_sale: 0.048 }
                ])
             }] ~> stocks
            stocks
        "#, vec![
            r#"|--------------------------------------------------------------------|"#, 
            r#"| id | symbol | exchange | history                                   |"#, 
            r#"|--------------------------------------------------------------------|"#, 
            r#"| 0  | BIZ    | NYSE     | [{"last_sale":23.66}]                     |"#, 
            r#"| 1  | GOTO   | OTC      | [{"last_sale":0.051},{"last_sale":0.048}] |"#,
            r#"|--------------------------------------------------------------------|"#]);
    }

    #[test]
    fn test_select_from_where_order_by_limit() {
        // create a table with test data
        let params = make_quote_parameters();
        let columns = Column::from_parameters(&params);

        // set up the interpreter
        verify_exact_table(r#"
            stocks = nsd::save(
                "query-engine.select.stocks",
                Table::new(symbol: String(8), exchange: String(8), last_sale: f64)
            )
            [{ symbol: "ABC", exchange: "AMEX", last_sale: 11.77 },
             { symbol: "BIZ", exchange: "NYSE", last_sale: 0.66 },
             { symbol: "UNO", exchange: "OTC", last_sale: 13.2456 },
             { symbol: "GOTO", exchange: "OTC", last_sale: 24.1428 },
             { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }] ~> stocks
            select symbol, exchange, price: last_sale, symbol_md5: util::md5(symbol)
            from stocks
            where last_sale > 1.0
            order_by symbol
            limit 2
        "#, vec![
            "|-----------------------------------------------------------------------|", 
            "| id | symbol | exchange | price   | symbol_md5                         |", 
            "|-----------------------------------------------------------------------|", 
            "| 0  | ABC    | AMEX     | 11.77   | 0v902fbdd2b1df0c4f70b4a5d23525e932 |",
            "| 2  | UNO    | OTC      | 13.2456 | 0v59f822bcaa8e119bde63eb00919b367a |",
            "|-----------------------------------------------------------------------|"]);
    }
    
    #[test]
    fn test_referenced_embedded_table() {
        let mut interpreter = Interpreter::new();
        interpreter = verify_exact_code_with(interpreter, r#"
            let stocks = nsd::save(
                "machine.examples.stocks",
                Table::new(symbol: String(8), exchange: String(8), history: Table(last_sale: f64, processed_time: Date))
            )
        "#, "true");
        
        // write a row to the `stocks` table
        interpreter = verify_exact_code_with(interpreter, r#"
            { symbol: "BIZ", exchange: "NYSE", history: tools::to_table([
                { last_sale: 11.67, processed_time: 2025-01-13T03:25:47.350Z }
            ])} ~> stocks
        "#, "1");

        // verify the contents of `stocks`
        interpreter = verify_exact_table_with(interpreter, r#"
            stocks
         "#, vec![
            r#"|--------------------------------------------------------------------------------------------|"#, 
            r#"| id | symbol | exchange | history                                                           |"#, 
            r#"|--------------------------------------------------------------------------------------------|"#, 
            r#"| 0  | BIZ    | NYSE     | [{"last_sale":11.67,"processed_time":"2025-01-13T03:25:47.350Z"}] |"#,
            r#"|--------------------------------------------------------------------------------------------|"#]);
        
        // get a reference to the embedded table
        interpreter = verify_exact_code_with(interpreter, r#"
            let history = &stocks(0, 2)
        "#, "true");

        // verify the contents of `history`
        interpreter = verify_exact_table_with(interpreter, r#"
            history
         "#, vec![
            "|-------------------------------------------|", 
            "| id | last_sale | processed_time           |", 
            "|-------------------------------------------|", 
            "| 0  | 11.67     | 2025-01-13T03:25:47.350Z |", 
            "|-------------------------------------------|"]);
        
        let history = interpreter.get("history");
        println!("history {history:?}");

        // write another row to the `stocks` table
        interpreter = verify_exact_code_with(interpreter, r#"
            { symbol: "ABY", exchange: "NYSE", history: tools::to_table([
                { last_sale: 78.33, processed_time: 2025-01-13T03:25:47.392Z }
            ])} ~> stocks
        "#, "1");

        // { symbol: "BIZ", exchange: "NYSE", history: [{ last_sale: 11.67, processed_time: 2025-01-13T03:25:47.350Z }] }
        interpreter = verify_exact_table_with(interpreter, r#"
            stocks
         "#, vec![
            r#"|--------------------------------------------------------------------------------------------|"#, 
            r#"| id | symbol | exchange | history                                                           |"#, 
            r#"|--------------------------------------------------------------------------------------------|"#, 
            r#"| 0  | BIZ    | NYSE     | [{"last_sale":11.67,"processed_time":"2025-01-13T03:25:47.350Z"}] |"#,
            r#"| 1  | ABY    | NYSE     | [{"last_sale":78.33,"processed_time":"2025-01-13T03:25:47.392Z"}] |"#,
            r#"|--------------------------------------------------------------------------------------------|"#]);

        // write a row to the `history` table
        interpreter = verify_exact_code_with(interpreter, r#"
            { last_sale: 11.68, processed_time: 2025-01-13T03:25:49.120Z }
                ~> history       
        "#, "1");
        
        // verify the contents of `history`
        interpreter = verify_exact_table_with(interpreter, r#"
            // NOTE: after the update, history may have been moved
            let history = &stocks(0, 2)  
            history
         "#, vec![
            "|-------------------------------------------|", 
            "| id | last_sale | processed_time           |", 
            "|-------------------------------------------|", 
            "| 0  | 11.67     | 2025-01-13T03:25:47.350Z |", 
            "| 1  | 11.68     | 2025-01-13T03:25:49.120Z |", 
            "|-------------------------------------------|"]);
    }

    #[test]
    fn test_overwrite_where() {
        let mut interpreter = Interpreter::new();
        interpreter = verify_exact_code_with(interpreter, r#"
            stocks = nsd::save(
                "query_engine.write.stocks",
                Table::new(symbol: String(8), exchange: String(10), last_sale: f64)
            )
            [{ symbol: "ABC", exchange: "AMEX", last_sale: 11.77 },
             { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
             { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
             { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
             { symbol: "BKPQ", exchange: "OTCBB", last_sale: 0.0786 }] ~> stocks
        "#, "5");
        
        interpreter = verify_exact_code_with(interpreter, r#"
            {symbol: "BKP", exchange: "OTHER_OTC", last_sale: 0.1421} 
                 ~>> (stocks where symbol is "BKPQ")
        "#, "1");

        verify_exact_table_with(interpreter, r#"
            stocks
        "#, vec![
            "|-------------------------------------|", 
            "| id | symbol | exchange  | last_sale |", 
            "|-------------------------------------|", 
            "| 0  | ABC    | AMEX      | 11.77     |", 
            "| 1  | UNO    | OTC       | 0.2456    |", 
            "| 2  | BIZ    | NYSE      | 23.66     |", 
            "| 3  | GOTO   | OTC       | 0.1428    |", 
            "| 4  | BKP    | OTHER_OTC | 0.1421    |", 
            "|-------------------------------------|"]);
    }

    #[test]
    fn test_update_where() {
        let mut interpreter = Interpreter::new();
        interpreter = verify_exact_code_with(interpreter, r#"
            stocks = nsd::save(
                "query_engine.write.stocks",
                Table::new(symbol: String(8), exchange: String(8), last_sale: f64)
            )
            [{ symbol: "ABC", exchange: "AMEX", last_sale: 11.77 },
             { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
             { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
             { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
             { symbol: "BKPQ", exchange: "OTCBB", last_sale: 0.0786 }] ~> stocks
        "#, "5");

        interpreter = verify_exact_code_with(interpreter, r#"
            {symbol: "BKP", last_sale: 0.1421} 
                 ~> (stocks where symbol is "BKPQ")
        "#, "1");

        verify_exact_table_with(interpreter, r#"
            stocks
        "#, vec![
            "|------------------------------------|",
            "| id | symbol | exchange | last_sale |",
            "|------------------------------------|",
            "| 0  | ABC    | AMEX     | 11.77     |",
            "| 1  | UNO    | OTC      | 0.2456    |",
            "| 2  | BIZ    | NYSE     | 23.66     |",
            "| 3  | GOTO   | OTC      | 0.1428    |",
            "| 4  | BKP    | OTCBB    | 0.1421    |",
            "|------------------------------------|"]);
    }
}