////////////////////////////////////////////////////////////////////
// QueryEngine classes
////////////////////////////////////////////////////////////////////

use crate::compiler::{fail_expr, fail_unexpected};
use crate::cursor::Cursor;
use crate::data_types::DataType;
use crate::data_types::DataType::{TableType, UnionType};
use crate::data_types::StorageTypes::BLOBSized;
use crate::dataframe::Dataframe;
use crate::dataframe::Dataframe::*;
use crate::dataframe_config::{DataFrameConfig, HashIndexConfig};
use crate::errors::throw;
use crate::errors::Errors::*;
use crate::expression::Conditions::True;
use crate::expression::CreationEntity::{IndexEntity, TableEntity};
use crate::expression::DatabaseOps::Mutate;
use crate::expression::Expression::*;
use crate::expression::MutateTarget::{IndexTarget, TableTarget};
use crate::expression::Mutation::Declare;
use crate::expression::{Conditions, DatabaseOps, Expression, Mutation, Queryable};
use crate::file_row_collection::FileRowCollection;
use crate::inferences::Inferences;
use crate::machine::Machine;
use crate::model_row_collection::ModelRowCollection;
use crate::namespaces::Namespace;
use crate::numbers::Numbers::Ack;
use crate::numbers::Numbers::RowsAffected;
use crate::parameter::Parameter;
use crate::row_collection::RowCollection;
use crate::rows::Row;
use crate::structures::Structure;
use crate::table_columns::Column;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::*;
use serde::{Deserialize, Serialize};
use shared_lib::fail;
use std::collections::HashMap;
use std::fs;
use std::ops::Deref;

/// Evaluates the database operation
pub fn evaluate(
    ms: &Machine,
    database_op: &DatabaseOps,
) -> std::io::Result<(Machine, TypedValue)> {
    match database_op {
        DatabaseOps::Query(q) => do_inquiry(ms, q),
        DatabaseOps::Mutate(m) => do_mutation(ms, m),
    }
}

pub fn do_inquiry(
    ms: &Machine,
    expression: &Queryable,
) -> std::io::Result<(Machine, TypedValue)> {
    use crate::expression::Queryable::*;
    match expression {
        Limit { from, limit } => {
            let (ms, limit) = ms.evaluate(limit)?;
            do_table_row_query(&ms, from, &True, limit)
        }
        Select { fields, from, condition, group_by, having, order_by, limit } =>
            do_select(&ms, fields, from, condition, group_by, having, order_by, limit),
        Where { from, condition } =>
            do_table_row_query(&ms, from, condition, Undefined),
    }
}

pub fn do_mutation(
    ms: &Machine,
    mutation: &Mutation,
) -> std::io::Result<(Machine, TypedValue)> {
    use crate::expression::Mutation::*;
    match mutation {
        Append { path, source } =>
            do_table_row_append(&ms, path, source),
        Create { path, entity: IndexEntity { columns } } =>
            do_table_create_index(&ms, path, columns),
        Create { path, entity: TableEntity { columns, from } } =>
            do_table_create_table(&ms, path, columns, from),
        Declare(IndexEntity { columns }) =>
            do_table_declare_index(&ms, columns),
        Declare(TableEntity { columns, from }) =>
            do_table_declare_table(&ms, columns, from),
        Delete { path, condition, limit } =>
            do_table_row_delete(&ms, path, condition, limit),
        Drop(IndexTarget { path }) => do_table_drop(&ms, path),
        Drop(TableTarget { path }) => do_table_drop(&ms, path),
        IntoNs(source, target) =>
            do_table_into(&ms, target, source),
        Overwrite { path, source, condition, limit } =>
            do_table_row_overwrite(&ms, path, source, condition, limit),
        Truncate { path, limit } =>
            match limit.deref() {
                None => do_table_row_resize(&ms, path, Boolean(false)),
                Some(limit) => {
                    let (ms, limit) = ms.evaluate(limit)?;
                    do_table_row_resize(&ms, path, limit)
                }
            }
        Undelete { path, condition, limit } =>
            do_table_row_undelete(&ms, path, condition, limit),
        Update { path, source, condition, limit } =>
            do_table_row_update(&ms, path, source, condition, limit),
    }
}

fn do_table_into(
    ms: &Machine,
    table: &Expression,
    source: &Expression,
) -> std::io::Result<(Machine, TypedValue)> {
    let machine = ms.to_owned();
    let (machine, rows) = match source {
        From(source) =>
            do_rows_from_query(&ms, source, table)?,
        Literal(TableValue(rc)) => (machine, rc.get_rows()),
        Literal(NamespaceValue(ns)) => {
            (machine, FileRowCollection::open(ns)?.read_active_rows()?)
        }
        DatabaseOp(Mutate(Declare(TableEntity { columns, from }))) =>
            do_rows_from_table_declaration(&machine, table, from, columns)?,
        source =>
            do_rows_from_query(&ms, source, table)?,
    };

    // write the rows to the target
    let mut inserted = 0;
    let mut rc = expect_row_collection(&machine, table)?;
    match rc.append_rows(rows) {
        ErrorValue(err) => return throw(err),
        Number(oc) => inserted += oc.to_i64(),
        _ => {}
    }
    Ok((machine, Number(RowsAffected(inserted))))
}

pub fn do_table_ns(
    ms: &Machine,
    expr: &Expression,
) -> std::io::Result<(Machine, TypedValue)> {
    let (ms, result) = ms.evaluate(expr)?;
    match result {
        ErrorValue(err) => Ok((ms, ErrorValue(err))),
        StringValue(path) =>
            match path.split('.').collect::<Vec<_>>().as_slice() {
                [d, s, n] => Ok((ms, NamespaceValue(Namespace::new(d, s, n)))),
                _ => Ok((ms, ErrorValue(InvalidNamespace(path))))
            }
        NamespaceValue(ns) => Ok((ms, NamespaceValue(ns))),
        other => throw(TypeMismatch(TableType(vec![], BLOBSized), other.get_type())),
    }
}

fn do_table_row_resize(
    ms: &Machine,
    table: &Expression,
    limit: TypedValue,
) -> std::io::Result<(Machine, TypedValue)> {
    let (machine, table) = ms.evaluate(table)?;
    match table.to_table_value() {
        ErrorValue(err) => Ok((machine, ErrorValue(err))),
        TableValue(mut df) => Ok((machine, df.resize(limit.to_usize()))),
        other => throw(TypeMismatch(TableType(vec![], BLOBSized), other.get_type()))
    }
}

fn do_table_row_append(
    ms: &Machine,
    table: &Expression,
    source: &Expression,
) -> std::io::Result<(Machine, TypedValue)> {
    let machine = ms.to_owned();
    if let From(source) = source {
        // determine the writable target
        let mut writable = expect_row_collection(&ms, table)?;

        // retrieve rows from the source
        let columns = writable.get_columns();
        let rows = expect_rows(&ms, source, columns)?;

        // write the rows to the target
        Ok((machine, writable.append_rows(rows)))
    } else {
        throw(QueryableExpected(source.to_string()))
    }
}

fn do_table_row_delete(
    ms: &Machine,
    from: &Expression,
    condition: &Option<Conditions>,
    limit: &Option<Box<Expression>>,
) -> std::io::Result<(Machine, TypedValue)> {
    let (ms, limit) = ms.evaluate_opt(limit)?;
    let (ms, table) = ms.evaluate(from)?;
    match table.to_table_value() {
        TableValue(mut rc) => Ok((ms.clone(), rc.delete_where(&ms, &condition, limit)?)),
        other => Ok((ms, ErrorValue(TypeMismatch(TableType(vec![], BLOBSized), other.get_type()))))
    }
}

fn do_table_row_overwrite(
    ms: &Machine,
    table: &Expression,
    source: &Expression,
    condition: &Option<Conditions>,
    limit: &Option<Box<Expression>>,
) -> std::io::Result<(Machine, TypedValue)> {
    let (machine, limit) = ms.evaluate_opt(limit)?;
    let (machine, tv_table) = machine.evaluate(table)?;
    let (fields, values) = expect_via(&ms, &table, &source)?;
    match tv_table.to_table_value() {
        ErrorValue(err) => Ok((machine, ErrorValue(err))),
        TableValue(rc) => {
            let (_, overwritten) = Dataframe::overwrite_where(rc, &machine, &fields, &values, condition, limit)?;
            Ok((machine, overwritten))
        }
        other => throw(TypeMismatch(TableType(vec![], BLOBSized), other.get_type()))
    }
}

/// Evaluates the queryable [Expression] (e.g. from, limit and where)
/// e.g.: from ns("interpreter.select.stocks") where last_sale > 1.0 limit 1
pub fn do_table_row_query(
    ms: &Machine,
    src: &Expression,
    condition: &Conditions,
    limit: TypedValue,
) -> std::io::Result<(Machine, TypedValue)> {
    let (machine, table) = ms.evaluate(src)?;
    let limit = limit.to_usize();
    match table.to_table_value() {
        ErrorValue(err) => Ok((machine, ErrorValue(err))),
        TableValue(rcv) => {
            let phys_columns = rcv.get_columns().clone();
            let mut cursor = Cursor::filter(Box::new(rcv), condition.to_owned());
            let table_value = TableValue(Model(ModelRowCollection::from_rows(&phys_columns, &cursor.take(limit)?)));
            Ok((machine, table_value))
        }
        other => throw(TypeMismatch(TableType(vec![], BLOBSized), other.get_type()))
    }
}

fn do_table_row_undelete(
    ms: &Machine,
    from: &Expression,
    condition: &Option<Conditions>,
    limit: &Option<Box<Expression>>,
) -> std::io::Result<(Machine, TypedValue)> {
    let (ms, limit) = ms.evaluate_opt(limit)?;
    let (ms, table) = ms.evaluate(from)?;
    match table.to_table_value() {
        ErrorValue(err) => Ok((ms, ErrorValue(err))),
        TableValue(mut rc) =>
            match rc.undelete_where(&ms, &condition, limit) {
                Ok(restored) => Ok((ms, restored)),
                Err(err) => throw(Exact(err.to_string())),
            }
        other => throw(TypeMismatch(TableType(vec![], BLOBSized), other.get_type()))
    }
}

fn do_table_row_update(
    ms: &Machine,
    table: &Expression,
    source: &Expression,
    condition: &Option<Conditions>,
    limit: &Option<Box<Expression>>,
) -> std::io::Result<(Machine, TypedValue)> {
    let (ms, limit) = ms.evaluate_opt(limit)?;
    let (ms, tv_table) = ms.evaluate(table)?;
    let (fields, values) = expect_via(&ms, &table, &source)?;
    match tv_table.to_table_value() {
        ErrorValue(err) => Ok((ms, ErrorValue(err))),
        TableValue(rc) =>
            match Dataframe::update_where(rc, &ms, &fields, &values, &condition, limit) {
                Ok(modified) => Ok((ms, modified)),
                Err(err) => throw(Exact(err.to_string())),
            }
        other => throw(TypeMismatch(TableType(vec![], BLOBSized), other.get_type()))
    }
}

fn do_table_create_index(
    ms: &Machine,
    index: &Expression,
    columns: &Vec<Expression>,
) -> std::io::Result<(Machine, TypedValue)> {
    let (machine, result) = ms.evaluate(index)?;
    match result {
        ErrorValue(msg) => throw(msg),
        Null | Undefined => Ok((machine, result)),
        TableValue(_rcv) =>
            throw(Exact("Memory collections do not yet support indexes".to_string())),
        NamespaceValue(ns) => {
            let (machine, columns) = ms.evaluate_as_atoms(columns)?;
            let config = DataFrameConfig::load(&ns)?;
            let mut indices = config.get_indices().to_owned();
            indices.push(HashIndexConfig::new(columns, false));
            let config = DataFrameConfig::new(
                config.get_columns().to_owned(),
                indices.to_owned(),
                config.get_partitions().to_owned(),
            );
            config.save(&ns)?;
            Ok((machine, Number(Ack)))
        }
        z => throw(CollectionExpected(z.to_code()))
    }
}

fn do_table_create_table(
    ms: &Machine,
    table: &Expression,
    columns: &Vec<Parameter>,
    from: &Option<Box<Expression>>,
) -> std::io::Result<(Machine, TypedValue)> {
    let (machine, result) = ms.evaluate(table)?;
    match result.to_owned() {
        Null | Undefined => Ok((machine, result)),
        ErrorValue(err) => Ok((machine, ErrorValue(err))),
        TableValue(_rcv) => throw(Exact("Memory collections do not 'create' keyword".to_string())),
        NamespaceValue(ns) => {
            FileRowCollection::create_table(&ns, columns)?;
            Ok((machine, Number(Ack)))
        }
        x => throw(CollectionExpected(x.to_code()))
    }
}

fn do_table_declare_index(
    ms: &Machine,
    columns: &Vec<Expression>,
) -> std::io::Result<(Machine, TypedValue)> {
    // TODO determine how to implement
    throw(NotImplemented("declare index".to_string()))
}

fn do_table_declare_table(
    ms: &Machine,
    columns: &Vec<Parameter>,
    from: &Option<Box<Expression>>,
) -> std::io::Result<(Machine, TypedValue)> {
    let columns = Column::from_parameters(columns)?;
    Ok((ms.to_owned(), TableValue(Model(ModelRowCollection::with_rows(columns, Vec::new())))))
}

fn do_table_drop(ms: &Machine, table: &Expression) -> std::io::Result<(Machine, TypedValue)> {
    let (machine, table) = ms.evaluate(table)?;
    match table {
        ErrorValue(err) => Ok((machine, ErrorValue(err))),
        NamespaceValue(ns) => {
            let result = fs::remove_file(ns.get_table_file_path());
            Ok((machine, if result.is_ok() { Number(Ack) } else { Boolean(false) }))
        }
        _ => Ok((machine, Boolean(false)))
    }
}

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
                ErrorValue(err) => throw(err),
                NamespaceValue(ns) =>
                    match FileRowCollection::open(&ns) {
                        Ok(frc) =>
                            Ok(do_select_go(ms, Disk(frc), fields, condition, group_by, having, order_by, limit)),
                        Err(err) => throw(Exact(err.to_string()))
                    }
                TableValue(rc) =>
                    Ok(do_select_go(ms, rc, fields, condition, group_by, having, order_by, limit)),
                z => throw(CollectionExpected(z.to_code()))
            }
        Err(err) => throw(Exact(err.to_string()))
    }
}

fn do_select_go(
    ms: Machine,
    df0: Dataframe,
    fields: &Vec<Expression>,
    condition: &Option<Conditions>,
    group_by: &Option<Vec<Expression>>,
    having: &Option<Box<Expression>>,
    order_by: &Option<Vec<Expression>>,
    limit: &Option<Box<Expression>>,
) -> (Machine, TypedValue) {
    // cache the initial state
    let ms0 = ms.clone();

    // step 1: determine output layout and limits
    let (_, rc1, new_columns, limit) =
        step_1_determine_layout_and_limit(ms, df0, fields, limit);

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
    rc0: Dataframe,
    fields: &Vec<Expression>,
    limit: &Option<Box<Expression>>,
) -> (Machine, Dataframe, Vec<Column>, TypedValue) {
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
    rc1: &Dataframe,
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
    src: Dataframe,
    group_by: &Vec<Expression>,
    having: &Option<Box<Expression>>,
) -> Dataframe {
    // todo add aggregation code to rc
    src
}

fn step_4_sort_table(
    src: Dataframe,
    sort_fields: &Vec<Expression>,
) -> Dataframe {
    // todo add sorting code to rc
    src
}

fn step_5_limit_table(
    src: Dataframe,
    cut_off: usize,
) -> Dataframe {
    let mut dest = ModelRowCollection::new(src.get_columns().clone());
    let mut count: usize = 0;
    for row in src.iter() {
        if count >= cut_off { break; }
        dest.overwrite_row(row.get_id(), row);
        count += 1;
    }
    Model(dest)
}

fn do_rows_from_table_declaration(
    ms: &Machine,
    table: &Expression,
    from: &Option<Box<Expression>>,
    columns: &Vec<Parameter>,
) -> std::io::Result<(Machine, Vec<Row>)> {
    let machine = ms.to_owned();
    // create the config and an empty data file
    let ns = expect_namespace(&ms, table)?;
    let cfg = DataFrameConfig::new(columns.to_owned(), Vec::new(), Vec::new());
    cfg.save(&ns)?;
    FileRowCollection::table_file_create(&ns)?;
    // decipher the "from" expression
    let columns = Column::from_parameters(columns)?;
    let results = match from {
        Some(expr) => expect_rows(&machine, expr.deref(), &columns)?,
        None => Vec::new()
    };
    Ok((machine, results))
}

fn do_rows_from_query(
    ms: &Machine,
    source: &Expression,
    table: &Expression,
) -> std::io::Result<(Machine, Vec<Row>)> {
    // determine the row collection
    let machine = ms.to_owned();
    let rc = expect_row_collection(&machine, table)?;

    // retrieve rows from the source
    let rows = expect_rows(&machine, source, rc.get_columns())?;
    Ok((machine, rows))
}

fn expect_namespace(
    ms: &Machine,
    table: &Expression,
) -> std::io::Result<Namespace> {
    let (_, v_table) = ms.evaluate(table)?;
    match v_table {
        NamespaceValue(ns) => Ok(ns),
        x => fail_unexpected("Table namespace", &x)
    }
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
                if let StructureSoft(structure) = tuples {
                    rows.push(Row::from_tuples(0, columns, &structure.get_tuples()))
                }
            }
            Ok(rows)
        }
        NamespaceValue(ns) => FileRowCollection::open(&ns)?.read_active_rows(),
        StructureHard(s) => Ok(vec![Row::from_tuples(0, columns, &s.get_tuples())]),
        StructureSoft(s) => Ok(vec![Row::from_tuples(0, columns, &s.get_tuples())]),
        TableValue(rcv) => Ok(rcv.get_rows()),
        tv => throw(TypeMismatch(TableType(Parameter::from_columns(columns), BLOBSized), tv.get_type()))
    }
}

fn expect_via(
    ms: &Machine,
    table: &Expression,
    source: &Expression,
) -> std::io::Result<(Vec<Expression>, Vec<Expression>)> {
    // split utility
    fn split(columns: &Vec<Column>, row: &Row) -> (Vec<Expression>, Vec<Expression>) {
        let my_fields = columns.iter()
            .map(|tc| Variable(tc.get_name().to_string()))
            .collect::<Vec<_>>();
        let my_values = row.get_values().iter()
            .map(|v| Literal(v.to_owned()))
            .collect::<Vec<_>>();
        (my_fields, my_values)
    }

    let writable = expect_row_collection(&ms, table)?;
    let (fields, values) = {
        if let Via(my_source) = source {
            if let (_, StructureSoft(structure)) = ms.evaluate(&my_source)? {
                let columns = writable.get_columns();
                let row = Row::from_tuples(0, columns, &structure.get_tuples());
                split(columns, &row)
            } else {
                return fail_expr("Expected a data object".to_string(), &my_source);
            }
        } else {
            return fail_expr("Expected keyword 'via'".to_string(), &source);
        }
    };
    Ok((fields, values))
}

fn transform_table(
    ms0: &Machine,
    rc0: &Dataframe,
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
                 { symbol: "BIZ", exchange: "NYSE", last_sale: 0.66 },
                 { symbol: "UNO", exchange: "OTC", last_sale: 13.2456 },
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
            "| 2  | UNO    | OTC      | 13.2456 | 59f822bcaa8e119bde63eb00919b367a |",
            "|---------------------------------------------------------------------|"]);
    }
}