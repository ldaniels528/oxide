////////////////////////////////////////////////////////////////////
// query-engine module
////////////////////////////////////////////////////////////////////

use crate::data_types::DataType;
use crate::data_types::DataType::{TableType, UnionType};
use crate::data_types::StorageTypes::BLOBSized;
use crate::errors::Errors::{CollectionExpected, Exact, Syntax, TypeMismatch};
use crate::expression::Expression::{AsValue, Variable};
use crate::expression::{Conditions, Expression};
use crate::file_row_collection::FileRowCollection;
use crate::inferences::Inferences;
use crate::machine::Machine;
use crate::model_row_collection::ModelRowCollection;
use crate::row_collection::RowCollection;
use crate::rows::Row;
use crate::table_columns::Column;
use crate::table_values::TableValues;
use crate::table_values::TableValues::{Disk, Model};
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{ArrayValue, ErrorValue, NamespaceValue, Null, Number, TableValue};
use shared_lib::fail;
use std::collections::HashMap;
use std::ops::Deref;

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
    match ms.evaluate_opt(from) {
        Ok((ms, table_v)) =>
            match table_v {
                ErrorValue(msg) => Ok((ms, ErrorValue(msg))),
                NamespaceValue(ns) =>
                    match FileRowCollection::open(&ns) {
                        Ok(frc) =>
                            Ok(do_select_go(ms, Disk(frc), fields, condition, group_by, having, order_by, limit)),
                        Err(err) =>
                            Ok((ms, ErrorValue(Exact(err.to_string()))))
                    }
                TableValue(rc) =>
                    Ok(do_select_go(ms, rc, fields, condition, group_by, having, order_by, limit)),
                z =>
                    Ok((ms, ErrorValue(CollectionExpected(z.to_code()))))
            }
        Err(err) => Ok((ms.clone(), ErrorValue(Exact(err.to_string()))))
    }
}

fn do_select_go(
    ms: Machine,
    rc0: TableValues,
    fields: &Vec<Expression>,
    condition: &Option<Conditions>,
    group_by: &Option<Vec<Expression>>,
    having: &Option<Box<Expression>>,
    order_by: &Option<Vec<Expression>>,
    limit: &Option<Box<Expression>>,
) -> (Machine, TypedValue) {
    // cache the initial state
    let ms0 = ms.clone();

    // step 1: determine layout and limits
    let (_, rc1, new_columns, limit) =
        step_1_determine_layout_and_limit(ms, rc0, fields, limit);

    // step 2: transform the eligible rows
    let (_, rc2) =
        match step_2_transform_eligible_rows(&ms0, &rc1, fields, &new_columns, condition) {
            (ms, ErrorValue(err)) => return (ms, ErrorValue(err)),
            (ms, TableValue(rc)) => (ms, rc),
            (ms, other) => return (ms, ErrorValue(TypeMismatch(
                TableType(new_columns.iter()
                              .map(|c| c.to_parameter())
                              .collect::<Vec<_>>(), BLOBSized),
                other.get_type(),
            ))),
        };

    // step 3: aggregate the dataset
    let rc3 = match group_by {
        Some(agg_fields) => step_3_aggregate_table(rc2, agg_fields, having),
        None => rc2
    };

    // step 4: sort the dataset
    let rc4 = match order_by {
        Some(order_fields) => step_4_sort_table(rc3, order_fields),
        None => rc3
    };

    // step 5: limit the dataset
    let rc5 = match limit {
        Number(cut_off) => step_5_limit_table(rc4, cut_off.to_usize()),
        _ => rc4
    };

    // return the table value
    (ms0, TableValue(rc5))
}

fn step_1_determine_layout_and_limit(
    ms0: Machine,
    rc0: TableValues,
    fields: &Vec<Expression>,
    limit: &Option<Box<Expression>>,
) -> (Machine, TableValues, Vec<Column>, TypedValue) {
    let columns = rc0.get_columns();
    let (ms, limit) = ms0.evaluate_opt(limit)
        .unwrap_or_else(|err| (ms0, ErrorValue(Exact(err.to_string()))));
    let new_columns = match resolve_fields_as_columns(columns, fields) {
        Ok(new_columns) => new_columns,
        Err(err) => return (ms, rc0, vec![], ErrorValue(Exact(err.to_string())))
    };
    (ms, rc0, new_columns, limit)
}

fn step_2_transform_eligible_rows(
    ms: &Machine,
    rc1: &TableValues,
    fields: &Vec<Expression>,
    new_columns: &Vec<Column>,
    condition: &Option<Conditions>,
) -> (Machine, TypedValue) {
    let (ms, result) = match transform_table(ms, rc1, fields, new_columns, condition) {
        (ms, ErrorValue(err)) => return (ms, ErrorValue(err)),
        (ms, TableValue(rc)) => (ms, TableValue(rc)),
        (ms, other) => return (ms, ErrorValue(TypeMismatch(
            TableType(vec![], BLOBSized),
            other.get_type(),
        ))),
    };
    (ms, result)
}

fn step_3_aggregate_table(
    src: TableValues,
    group_by: &Vec<Expression>,
    having: &Option<Box<Expression>>,
) -> TableValues {
    // todo add aggregation code to rc
    src
}

fn step_4_sort_table(
    src: TableValues,
    sort_fields: &Vec<Expression>,
) -> TableValues {
    // todo add sorting code to rc
    src
}

fn step_5_limit_table(
    src: TableValues,
    cut_off: usize,
) -> TableValues {
    let mut dest = ModelRowCollection::new(src.get_columns().clone());
    let mut count: usize = 0;
    for row in src.iter() {
        if count >= cut_off { break; }
        dest.overwrite_row(row.get_id(), row);
        count += 1;
    }
    Model(dest)
}

fn transform_table(
    ms0: &Machine,
    rc0: &TableValues,
    field_values: &Vec<Expression>,
    field_columns: &Vec<Column>,
    condition: &Option<Conditions>,
) -> (Machine, TypedValue) {
    let columns = rc0.get_columns();
    let mut rc1 = ModelRowCollection::new(field_columns.clone());
    for row in rc0.iter() {
        let ms = row.pollute(&ms0, columns);
        if row.matches(&ms, condition, columns) {
            match ms.evaluate_array(field_values) {
                Ok((_ms, ArrayValue(array))) => {
                    let new_row = Row::new(row.get_id(), array.values().clone());
                    rc1.overwrite_row(new_row.get_id(), new_row);
                }
                Ok((ms, z)) => return (ms, ErrorValue(Exact(z.to_code()))),
                Err(err) => return (ms, ErrorValue(Exact(err.to_string())))
            }
        }
    }
    (ms0.clone(), TableValue(Model(rc1)))
}

fn resolve_fields_as_columns(
    columns: &Vec<Column>,
    fields: &Vec<Expression>,
) -> std::io::Result<Vec<Column>> {
    // create a lookup for column-name-to-data-type from the source columns
    let column_dict: HashMap<String, DataType> = columns.iter()
        .map(|c| (c.get_name().to_string(), c.get_data_type().clone()))
        .collect();

    // build the fields vector (new_columns)
    let mut offset = Row::overhead();
    let mut new_columns = Vec::new();
    for field in fields {
        let column = resolve_field_as_column(field, columns, &column_dict, offset)?;
        let fixed_size = column.get_data_type().compute_max_physical_size();
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
        AsValue(label, expr) =>
            match expr.deref() {
                // price: last_sale
                Variable(name) =>
                    match column_dict.get(name) {
                        Some(dt) => Ok(Column::new(label, dt.clone(), Null, offset)),
                        None => fail(column_not_found(name, columns)),
                    }
                // md5sum: util::md5(sku)
                other =>
                    match Inferences::infer(other) {
                        UnionType(v) => fail(format!("Variable type detected - {v:?}")),
                        dt => Ok(Column::new(label, dt.clone(), Null, offset)),
                    }
            }
        // last_sale
        Variable(name) =>
            match column_dict.get(name) {
                Some(dt) => Ok(Column::new(name, dt.clone(), Null, offset)),
                None => fail(column_not_found(name, columns)),
            }
        other =>
            fail(format!("{}", Syntax(other.to_code()).to_string()))
    }
}

fn column_not_found(name: &str, columns: &Vec<Column>) -> String {
    format!("Column {name} was not found in {}", columns.iter()
        .map(|c| c.get_name()).collect::<Vec<_>>().join(", "))
}

/// Unit tests
#[cfg(test)]
mod tests {
    use crate::table_columns::Column;
    use crate::testdata::*;

    #[test]
    fn test_select_from_where_order_by_limit() {
        // create a table with test data
        let params = make_quote_parameters();
        let columns = Column::from_parameters(&params).unwrap();

        // set up the interpreter
        verify_exact_table_with_ids(r#"
            [+] stocks := ns("query-engine.select.stocks")
            [+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
            [+] [{ symbol: "ABC", exchange: "AMEX", last_sale: 11.77 },
                 { symbol: "UNO", exchange: "OTC", last_sale: 13.2456 },
                 { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                 { symbol: "GOTO", exchange: "OTC", last_sale: 24.1428 },
                 { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }] ~> stocks
            [+] select symbol, exchange, price: last_sale, msn: util::md5(symbol)
                from stocks
                where last_sale > 1.0
                order by symbol
                limit 2
        "#, vec![
            "|---------------------------------------------------------------------|",
            "| id | symbol | exchange | price   | msn                              |",
            "|---------------------------------------------------------------------|",
            "| 0  | ABC    | AMEX     | 11.77   | 902fbdd2b1df0c4f70b4a5d23525e932 |",
            "| 1  | UNO    | OTC      | 13.2456 | 59f822bcaa8e119bde63eb00919b367a |",
            "|---------------------------------------------------------------------|"]);
    }
}