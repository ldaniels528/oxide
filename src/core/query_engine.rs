#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// QueryEngine classes
////////////////////////////////////////////////////////////////////

use crate::columns::Column;
use crate::cursor::Cursor;
use crate::data_types::DataType;
use crate::data_types::DataType::TableType;

use crate::dataframe::Dataframe;
use crate::dataframe::Dataframe::*;
use crate::errors::Errors::*;
use crate::errors::TypeMismatchErrors::{CollectionExpected, FunctionArgsExpected, NamespaceExpected, TableExpected, UnsupportedType};
use crate::errors::{throw, SyntaxErrors, TypeMismatchErrors};
use crate::expression::Conditions::True;
use crate::expression::CreationEntity::{IndexEntity, TableEntity, TableFnEntity};
use crate::expression::Expression::*;
use crate::expression::MutateTarget::{IndexTarget, TableTarget};
use crate::expression::Mutations::Declare;
use crate::expression::{Conditions, DatabaseOps, Expression, Mutations, Queryables, NULL};
use crate::file_row_collection::FileRowCollection;
use crate::journaling::{EventSourceRowCollection, TableFunction};
use crate::machine::Machine;
use crate::model_row_collection::ModelRowCollection;
use crate::namespaces::Namespace;
use crate::numbers::Numbers::I64Value;
use crate::object_config::{HashIndexConfig, ObjectConfig};
use crate::parameter::Parameter;
use crate::row_collection::RowCollection;
use crate::sequences::Sequence;
use crate::structures::Structure;
use crate::structures::Structures::Soft;
use crate::structures::{Row, Structures};
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::From;
use std::fs;
use std::ops::Deref;

/// Represents a Table Option
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
enum TableOption {
    Journaling,
    Transform(Expression),
    TTL(u64),
}

/// Evaluates the database operation
pub fn evaluate(
    ms: &Machine,
    database_op: &DatabaseOps,
    options: &Expression,
) -> std::io::Result<(Machine, TypedValue)> {
    match database_op {
        DatabaseOps::Queryable(q) => eval_inquiry(ms, q, options),
        DatabaseOps::Mutation(m) => eval_mutation(ms, m, options),
    }
}

pub fn eval_inquiry(
    ms: &Machine,
    queryable: &Queryables,
    _options: &Expression,
) -> std::io::Result<(Machine, TypedValue)> {
    match queryable {
        Queryables::Limit { from, limit } => {
            let (ms, limit) = ms.evaluate(limit)?;
            eval_table_or_view_query(&ms, from, &True, &limit)
        }
        Queryables::Select { fields, from, condition, group_by, having, order_by, limit } =>
            do_select(&ms, fields, from, condition, group_by, having, order_by, limit),
        Queryables::Where { from, condition } =>
            eval_table_or_view_query(&ms, from, condition, &Undefined),
    }
}

pub fn eval_mutation(
    ms: &Machine,
    mutation: &Mutations,
    options: &Expression,
) -> std::io::Result<(Machine, TypedValue)> {
    match mutation {
        Mutations::Append { path, source } =>
            do_table_row_append(&ms, path, source),
        Mutations::Create { path, entity } => match entity {
            IndexEntity { columns } =>
                do_create_table_index(&ms, path, columns),
            TableEntity { columns, from } =>
                do_create_table(&ms, path, columns, from, options),
            TableFnEntity { fx } =>
                do_create_table_fn(&ms, path, fx),
        }
        Mutations::Declare(entity) => match entity {
            IndexEntity { columns } =>
                do_declare_table_index(&ms, columns),
            TableEntity { columns, from } =>
                do_declare_table(&ms, columns, from, options),
            TableFnEntity { fx } =>
                do_declare_table_fn(&ms, fx),
        }
        Mutations::Delete { path, condition, limit } =>
            do_table_row_delete(&ms, path, condition, limit),
        Mutations::Drop(target) => match target {
            IndexTarget { path } => do_table_drop(&ms, path),
            TableTarget { path } => do_table_drop(&ms, path),
        }
        Mutations::IntoNs(source, target) =>
            eval_into_ns(&ms, target, source),
        Mutations::Overwrite { path, source, condition, limit } =>
            do_table_row_overwrite(&ms, path, source, condition, limit),
        Mutations::Truncate { path, limit } =>
            match limit {
                None => do_table_row_resize(&ms, path, Boolean(false)),
                Some(limit) => {
                    let (ms, limit) = ms.evaluate(limit)?;
                    do_table_row_resize(&ms, path, limit)
                }
            }
        Mutations::Undelete { path, condition, limit } =>
            do_table_row_undelete(&ms, path, condition, limit),
        Mutations::Update { path, source, condition, limit } =>
            do_table_row_update(&ms, path, source, condition, limit),
    }
}

pub fn eval_into_ns(
    ms: &Machine,
    table: &Expression,
    source: &Expression,
) -> std::io::Result<(Machine, TypedValue)> {
    let m0 = ms.to_owned();
    let (machine, rows) = match source {
        From(source) => do_table_rows_from_query(&ms, source, table)?,
        Literal(Kind(TableType(_params, _size))) => (m0, vec![]),
        Literal(NamespaceValue(ns)) => (m0, FileRowCollection::open(ns)?.read_active_rows()?),
        Literal(TableValue(rc)) => (m0, rc.get_rows()),
        DatabaseOp(DatabaseOps::Mutation(Declare(TableEntity { columns, from }))) =>
            do_table_rows_from_table_declaration(&m0, table, from, columns)?,
        source => do_table_rows_from_query(&ms, source, table)?,
    };

    // write the rows to the target
    let mut inserted = 0;
    let mut rc = expect_row_collection(&machine, table)?;
    inserted += rc.append_rows(rows)?;
    Ok((machine, Number(I64Value(inserted))))
}

pub fn eval_iter(
    ms: &Machine,
    container: &Expression,
    source: &Expression,
) -> std::io::Result<(Machine, TypedValue)> {
    // todo tables need an iterable indexer
    let (ms, result) = ms.evaluate(source)?;
    let mut df = result.to_dataframe()?;
    let offset = df.len()?;
    let response = df.read_one(if offset > 0 { offset - 1 } else { offset })?
        .map(|r| Structured(Structures::Firm(r, df.get_parameters())))
        .unwrap_or(Undefined);
    match container {
        Variable(name) => Ok((ms.with_variable(name, response), Boolean(true))),
        other => throw(Exact(other.to_code()))
    }
}

pub fn eval_ns(
    ms: &Machine,
    expr: &Expression,
) -> std::io::Result<(Machine, TypedValue)> {
    let (ms, result) = ms.evaluate(expr)?;
    match result {
        ErrorValue(err) => throw(err),
        StringValue(path) =>
            match path.split('.').collect::<Vec<_>>().as_slice() {
                [d, s, n] => Ok((ms, NamespaceValue(Namespace::new(d, s, n)))),
                _ => Ok((ms, ErrorValue(InvalidNamespace(path))))
            }
        NamespaceValue(ns) => Ok((ms, NamespaceValue(ns))),
        other => throw(TypeMismatch(UnsupportedType(TableType(vec![], 0), other.get_type()))),
    }
}

/// Evaluates the queryable [Expression] (e.g. from, limit and where)
/// e.g.: from ns("query_engine.select.stocks") where last_sale > 1.0 limit 1
pub fn eval_table_or_view_query(
    ms: &Machine,
    src: &Expression,
    condition: &Conditions,
    limit: &TypedValue,
) -> std::io::Result<(Machine, TypedValue)> {
    //println!("do_table_or_view_query: src = {src:?}, condition = {condition:?}, limit = {limit:?}");
    let (machine, df) = ms.eval_as_dataframe(src)?;
    let limit = limit.to_usize();
    let columns = df.get_columns().clone();
    let mut cursor = Cursor::filter(Box::new(df), condition.to_owned());
    let mrc = ModelRowCollection::from_columns_and_rows(&columns, &cursor.take(limit)?);
    Ok((machine, TableValue(Model(mrc))))
}

fn do_create_table(
    ms: &Machine,
    table: &Expression,
    columns: &Vec<Parameter>,
    from: &Option<Box<Expression>>,
    options: &Expression,
) -> std::io::Result<(Machine, TypedValue)> {
    let (ms, result) = ms.evaluate(table)?;
    match result.to_owned() {
        Null | Undefined => Ok((ms, result)),
        ErrorValue(err) => throw(err),
        TableValue(_rcv) => throw(Exact("Memory collections do not use 'create' keyword".to_string())),
        NamespaceValue(ns) => do_create_table_ns(&ms, &ns, columns, from, options),
        x => throw(TypeMismatch(CollectionExpected(x.to_code())))
    }
}

fn do_create_table_fn(
    ms: &Machine,
    table_ns: &Expression,
    fx: &Expression,
) -> std::io::Result<(Machine, TypedValue)> {
    // decode the model
    match fx.clone() {
        Expression::FnExpression { params, body, .. } => {
            // create the input and output tables
            let (ms, ns_value) = ms.evaluate(table_ns)?;
            match ns_value {
                NamespaceValue(ns) => {
                    TableFunction::create_table_fn(
                        &ns,
                        params,
                        match body {
                            None => NULL,
                            Some(expr) => expr.deref().clone()
                        },
                        Machine::new_platform(), //ms.clone(),
                    )?;
                    Ok((ms, Boolean(true)))
                }
                x => throw(TypeMismatch(NamespaceExpected(x.to_code())))
            }
        }
        // Expression::Literal(Function { params, body, returns }) =>
        //     create_table_fn(ms, params, returns, body.deref().clone()),
        other => throw(TypeMismatch(FunctionArgsExpected(other.to_code())))
    }
}

fn do_create_table_index(
    ms: &Machine,
    index: &Expression,
    columns: &Vec<Expression>,
) -> std::io::Result<(Machine, TypedValue)> {
    let (ms1, result) = ms.evaluate(index)?;
    match result {
        Null | Undefined => Ok((ms1, result)),
        TableValue(_rcv) => throw(UnsupportedFeature("in-memory indices".into())),
        NamespaceValue(ns) => {
            // evaluate the columns
            let (ms2, columns) = ms.eval_as_atoms(columns)?;

            // load the configuration
            let config = ObjectConfig::load(&ns)?;

            // update the indices
            let mut indices = config.get_indices();
            indices.push(HashIndexConfig::new(columns, false));

            // update the configuration
            let updated_config = config.with_indices(indices);
            updated_config.save(&ns)?;
            Ok((ms2, Boolean(true)))
        }
        z => throw(TypeMismatch(CollectionExpected(z.to_code())))
    }
}

fn do_create_table_ns(
    ms: &Machine,
    ns: &Namespace,
    columns: &Vec<Parameter>,
    from: &Option<Box<Expression>>,
    options: &Expression,
) -> std::io::Result<(Machine, TypedValue)> {
    // determine the table kind
    let (ms, table_opts) = do_decode_create_table_options(&ms, options)?;
    let df = match table_opts {
        opts if opts.contains(&TableOption::Journaling) =>
            EventSource(EventSourceRowCollection::new(&ns, columns)?),
        _ => Disk(FileRowCollection::create_table(&ns, columns)?)
    };
    // append the rows of the "from" clause
    let (ms, _df) = populate_dataframe_opt(&ms, df, from)?;
    Ok((ms, Boolean(true)))
}

fn do_declare_table(
    ms: &Machine,
    columns: &Vec<Parameter>,
    from: &Option<Box<Expression>>,
    options: &Expression,
) -> std::io::Result<(Machine, TypedValue)> {
    let columns = Column::from_parameters(columns);

    // determine the table kind
    let (ms, table_opts) = do_decode_create_table_options(&ms, options)?;
    let df = match table_opts {
        opts if opts.contains(&TableOption::Journaling) =>
            return throw(UnsupportedFeature("journaling".into())),
        opts if opts.iter().any(|o| matches!(o, TableOption::TTL(..))) =>
            return throw(UnsupportedFeature("ttl".into())),
        _ => Model(ModelRowCollection::with_rows(columns, Vec::new()))
    };
    // append the rows of the "from" clause
    let (ms, df) = populate_dataframe_opt(&ms, df, from)?;
    Ok((ms, TableValue(df)))
}

fn do_declare_table_fn(
    ms: &Machine,
    fx: &Box<Expression>,
) -> std::io::Result<(Machine, TypedValue)> {
    match fx.deref().clone() {
        Expression::FnExpression { params, body, returns } => Ok(todo!()),
        Expression::Literal(Function { params, body, returns }) => Ok(todo!()),
        other => throw(TypeMismatch(FunctionArgsExpected(other.to_code())))
    }
}

fn do_declare_table_index(
    ms: &Machine,
    columns: &Vec<Expression>,
) -> std::io::Result<(Machine, TypedValue)> {
    // TODO determine how to implement
    throw(NotImplemented("declare index".to_string()))
}

fn do_decode_create_table_options(
    ms: &Machine,
    options: &Expression,
) -> std::io::Result<(Machine, Vec<TableOption>)> {
    let mut table_opts = Vec::new();
    let (ms, opts) = ms.evaluate(options)?;
    match opts {
        Structured(s) =>
            for (param, val) in s.get_parameters().iter().zip(s.get_values()) {
                match (param.get_name(), val) {
                    ("journaling", Boolean(true)) =>
                        table_opts.push(TableOption::Journaling),
                    ("ttl", Number(n)) =>
                        table_opts.push(TableOption::TTL(n.to_u64())),
                    _ => {}
                }
            }
        Null | Undefined => {}
        z => return throw(TypeMismatch(TypeMismatchErrors::StructExpected(z.to_code(), z.to_code())))
    }
    Ok((ms, table_opts))
}

fn do_table_drop(ms: &Machine, table: &Expression) -> std::io::Result<(Machine, TypedValue)> {
    let (machine, table) = ms.evaluate(table)?;
    match table {
        NamespaceValue(ns) => {
            let result = fs::remove_file(ns.get_table_file_path());
            Ok((machine, Boolean(result.is_ok())))
        }
        _ => Ok((machine, Boolean(false)))
    }
}

fn do_table_row_append(
    ms: &Machine,
    table_expr: &Expression,
    from_expr: &Expression,
) -> std::io::Result<(Machine, TypedValue)> {
    // evaluate the table expression (table_expr)
    let (ms, table) = ms.evaluate(table_expr)?;
    let mut df = table.to_dataframe()?;

    // evaluate the query expression (from_expr)
    let (ms, result) = ms.evaluate(from_expr)?;
    let src = result.to_dataframe()?;

    // write the rows to the dataframe
    df.append_rows(src.read_active_rows()?)
        .map(|n| (ms, Number(I64Value(n))))
}

fn do_table_row_delete(
    ms: &Machine,
    from: &Expression,
    condition: &Option<Conditions>,
    limit: &Option<Box<Expression>>,
) -> std::io::Result<(Machine, TypedValue)> {
    let (ms, limit) = ms.evaluate_opt(limit)?;
    let (ms, table) = ms.evaluate(from)?;
    let mut df = table.to_dataframe()?;
    Ok((ms.clone(), df.delete_where(&ms, &condition, limit)?))
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
    let df = tv_table.to_dataframe()?;
    let (_, overwritten) = Dataframe::overwrite_where(df, &machine, &fields, &values, condition, limit)?;
    Ok((machine, overwritten))
}

fn do_table_row_resize(
    ms: &Machine,
    table: &Expression,
    limit: TypedValue,
) -> std::io::Result<(Machine, TypedValue)> {
    let (ms, table) = ms.evaluate(table)?;
    let mut df = table.to_dataframe()?;
    df.resize(limit.to_usize())
        .map(|r| (ms, Boolean(r)))
}

fn do_table_row_undelete(
    ms: &Machine,
    from: &Expression,
    condition: &Option<Conditions>,
    limit: &Option<Box<Expression>>,
) -> std::io::Result<(Machine, TypedValue)> {
    let (ms, limit) = ms.evaluate_opt(limit)?;
    let (ms, table) = ms.evaluate(from)?;
    let mut df = table.to_dataframe()?;
    df.undelete_where(&ms, &condition, limit).map(|restored| (ms, restored))
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
    let df = tv_table.to_dataframe()?;
    Dataframe::update_where(df, &ms, &fields, &values, &condition, limit)
        .map(|modified| (ms, modified))
}

fn do_table_rows_from_table_declaration(
    ms: &Machine,
    table: &Expression,
    from: &Option<Box<Expression>>,
    columns: &Vec<Parameter>,
) -> std::io::Result<(Machine, Vec<Row>)> {
    // create the config and an empty data file
    let ns = expect_namespace(&ms, table)?;
    let cfg = ObjectConfig::build_table(columns.clone());
    cfg.save(&ns)?;
    FileRowCollection::table_file_create(&ns)?;
    // decipher the "from" expression
    let columns = Column::from_parameters(columns);
    let results = match from {
        Some(expr) => expect_rows(ms, expr.deref(), &columns)?,
        None => Vec::new()
    };
    Ok((ms.clone(), results))
}

fn do_table_rows_from_query(
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

fn do_select(
    ms: &Machine,
    fields: &Vec<Expression>,
    from: &Option<Box<Expression>>,
    condition: &Option<Conditions>,
    group_by: &Option<Vec<Expression>>,
    having: &Option<Box<Expression>>,
    order_by: &Option<Vec<Expression>>,
    limit: &Option<Box<Expression>>,
) -> std::io::Result<(Machine, TypedValue)> {
    let (ms, table_v) = ms.evaluate_opt(from)?;
    match table_v {
        NamespaceValue(ns) => {
            let frc = FileRowCollection::open(&ns)?;
            do_select_from_dataframe(ms, Disk(frc), fields, condition, group_by, having, order_by, limit)
        }
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
                              .collect::<Vec<_>>(), 0),
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

fn populate_dataframe(
    ms: &Machine,
    mut target: Dataframe,
    from: &Expression,
) -> std::io::Result<(Machine, Dataframe)> {
    let (ms, source) = ms.eval_as_dataframe(from)?;
    target.append_rows(source.read_active_rows()?);
    Ok((ms, target))
}

fn populate_dataframe_opt(
    ms: &Machine,
    mut target: Dataframe,
    from: &Option<Box<Expression>>,
) -> std::io::Result<(Machine, Dataframe)> {
    if let Some(source) = from {
        populate_dataframe(ms, target, source)
    } else { Ok((ms.clone(), target)) }
}

fn step_1_determine_layout_and_limit(
    ms0: Machine,
    rc0: Dataframe,
    fields: &Vec<Expression>,
    limit: &Option<Box<Expression>>,
) -> std::io::Result<(Machine, Dataframe, Vec<Column>, TypedValue)> {
    let columns = rc0.get_columns();
    let (ms, limit) = ms0.evaluate_opt(limit)?;
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
            TableType(vec![], 0),
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
    // todo add aggregation code to rc
    Ok(src)
}

fn step_4_sort_table(
    src: Dataframe,
    sort_fields: &Vec<Expression>,
) -> std::io::Result<Dataframe> {
    // todo add sorting code to rc
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
        dest.overwrite_row(row.get_id(), row);
        count += 1;
    }
    Ok(Model(dest))
}

fn expect_namespace(
    ms: &Machine,
    table: &Expression,
) -> std::io::Result<Namespace> {
    let (_, v_table) = ms.evaluate(table)?;
    match v_table {
        NamespaceValue(ns) => Ok(ns),
        x => throw(TypeMismatch(TableExpected(x.get_type_name(), x.to_code())))
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
                if let Structured(structure) = tuples {
                    rows.push(Row::from_tuples(0, columns, &structure.to_name_values()))
                }
            }
            Ok(rows)
        }
        NamespaceValue(ns) => FileRowCollection::open(&ns)?.read_active_rows(),
        Structured(s) => Ok(vec![Row::from_tuples(0, columns, &s.to_name_values())]),
        TableValue(rcv) => Ok(rcv.get_rows()),
        tv => throw(TypeMismatch(UnsupportedType(TableType(Parameter::from_columns(columns), 0), tv.get_type())))
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
            if let (_, Structured(Soft(structure))) = ms.evaluate(&my_source)? {
                let columns = writable.get_columns();
                let row = Row::from_tuples(0, columns, &structure.to_name_values());
                split(columns, &row)
            } else {
                return throw(Exact("Expected a data object".to_string()));
            }
        } else {
            return throw(SyntaxError(SyntaxErrors::KeywordExpected("via".to_string())));
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

    // build the fields vector (new_columns)
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
        AsValue(label, expr) =>
            match expr.deref() {
                // price: last_sale
                Variable(name) =>
                    match column_dict.get(name) {
                        Some(dt) => Ok(Column::new(label, dt.clone(), Null, offset)),
                        None => throw(Exact(column_not_found(name, columns))),
                    }
                // md5sum: util::md5(sku)
                other =>
                    match Expression::infer(other) {
                        dt => Ok(Column::new(label, dt.clone(), Null, offset)),
                    }
            }
        // last_sale
        Variable(name) =>
            match column_dict.get(name) {
                Some(dt) => Ok(Column::new(name, dt.clone(), Null, offset)),
                None => throw(Exact(column_not_found(name, columns))),
            }
        other =>
            throw(SyntaxError(SyntaxErrors::TypeIdentifierExpected(other.to_code())))
    }
}

fn column_not_found(name: &str, columns: &Vec<Column>) -> String {
    format!("Column {name} was not found in {}", columns.iter()
        .map(|c| c.get_name()).collect::<Vec<_>>().join(", "))
}

/// SQL tests
#[cfg(test)]
mod tests {
    use crate::columns::Column;
    use crate::dataframe::Dataframe::Model;
    use crate::interpreter::Interpreter;
    use crate::model_row_collection::ModelRowCollection;
    use crate::numbers::Numbers::I64Value;
    use crate::testdata::*;
    use crate::typed_values::TypedValue::*;

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
    fn test_table_create_ephemeral() {
        verify_exact_value(r#"
            table(
                symbol: String(8),
                exchange: String(8),
                last_sale: f64
            )
        "#, TableValue(Model(ModelRowCollection::with_rows(
            make_quote_columns(), Vec::new(),
        ))))
    }

    #[test]
    fn test_table_create_durable() {
        verify_exact_value(r#"
            create table ns("query_engine.create.stocks") (
                symbol: String(8),
                exchange: String(8),
                last_sale: f64
            )
        "#, Boolean(true))
    }

    #[test]
    fn test_create_table_fn() {
        let mut interpreter = Interpreter::new();
        interpreter = verify_exact_value_with(interpreter, r#"
            stocks := ns("query_engine.table_fn.stocks")
            drop table stocks
            create table stocks fn(
               symbol: String(8), exchange: String(8), last_sale: f64
            ) => {
                    symbol: symbol,
                    exchange: exchange,
                    last_sale: last_sale * 2.0,
                    rank: __row_id__ + 1
                 }
        "#, Boolean(true));

        interpreter = verify_exact_value_with(interpreter, r#"
            [{ symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
             { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
             { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
        "#, Number(I64Value(3)));

        verify_exact_table_with(interpreter, "from stocks", vec![
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
        interpreter = verify_exact_value_with(interpreter, r#"
            stocks := ns("query_engine.crud.stocks")
        "#, Boolean(true));

        // create the table
        interpreter = verify_exact_value_with(interpreter, r#"
            table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
        "#, Number(I64Value(0)));

        // append a row
        interpreter = verify_exact_value_with(interpreter, r#"
            append stocks from { symbol: "ABC", exchange: "AMEX", last_sale: 11.77 }
        "#, Number(I64Value(1)));

        // write another row
        interpreter = verify_exact_value_with(interpreter, r#"
            { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 } ~> stocks
        "#, Number(I64Value(1)));

        // write some more rows
        interpreter = verify_exact_value_with(interpreter, r#"
            [{ symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
             { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 }] ~> stocks
        "#, Number(I64Value(2)));

        // write even more rows
        interpreter = verify_exact_value_with(interpreter, r#"
            append stocks from [
                { symbol: "BOOM", exchange: "NASDAQ", last_sale: 56.87 },
                { symbol: "TRX", exchange: "NASDAQ", last_sale: 7.9311 }
            ]
        "#, Number(I64Value(2)));

        // remove some rows
        interpreter = verify_exact_value_with(interpreter, r#"
            delete from stocks where last_sale > 1.0
        "#, Number(I64Value(4)));

        // overwrite a row
        interpreter = verify_exact_value_with(interpreter, r#"
            overwrite stocks via {symbol: "GOTO", exchange: "OTC", last_sale: 0.1421}
            where symbol is "GOTO"
        "#, Number(I64Value(1)));

        // verify the remaining rows
        interpreter = verify_exact_value_with(interpreter, r#"
            from stocks
        "#, TableValue(Model(ModelRowCollection::from_columns_and_rows(&columns, &vec![
            make_quote(1, "UNO", "OTC", 0.2456),
            make_quote(3, "GOTO", "OTC", 0.1421),
        ]))));

        // restore the previously deleted rows
        interpreter = verify_exact_value_with(interpreter, r#"
            undelete from stocks where last_sale > 1.0
        "#, Number(I64Value(4)));

        // verify the existing rows
        verify_exact_table_with(interpreter, "from stocks", vec![
            "|------------------------------------|",
            "| id | symbol | exchange | last_sale |",
            "|------------------------------------|",
            "| 0  | ABC    | AMEX     | 11.77     |",
            "| 1  | UNO    | OTC      | 0.2456    |",
            "| 2  | BIZ    | NYSE     | 23.66     |",
            "| 3  | GOTO   | OTC      | 0.1421    |",
            "| 4  | BOOM   | NASDAQ   | 56.87     |",
            "| 5  | TRX    | NASDAQ   | 7.9311    |",
            "|------------------------------------|"
        ]);
    }

    #[test]
    fn test_table_into_then_delete() {
        verify_exact_table(r#"
            stocks := ns("query_engine.into_then_delete.stocks")
            table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
            [{ symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
             { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
             { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
            delete from stocks where last_sale < 30.0
            from stocks
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
        interpreter = verify_exact_value_with(interpreter, r#"
            stocks := ns("query_engine.select1.stocks")
            table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
            append stocks from [
                { symbol: "ABC", exchange: "AMEX", last_sale: 11.77 },
                { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }
            ]
        "#, Number(I64Value(5)));

        // compile and execute the code
        interpreter = verify_exact_table_with(interpreter, r#"
            select symbol, last_sale from stocks
            where last_sale < 1.0
            order by symbol
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
            stocks := ns("query_engine.select.stocks")
            table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
            [{ symbol: "ABC", exchange: "AMEX", last_sale: 11.77 },
             { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
             { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
             { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
             { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }] ~> stocks
            select symbol, exchange, last_sale
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
    fn test_table_embedded_describe() {
        verify_exact_table(r#"
            stocks := ns("query_engine.embedded_a.stocks")
            table(symbol: String(8), exchange: String(8), history: Table(last_sale: f64, processed_time: Date)) ~> stocks
            tools::describe(stocks)
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
            stocks := ns("query_engine.embedded_b.stocks")
            table(symbol: String(8), exchange: String(8), history: Table(last_sale: f64, processed_time: Date)) ~> stocks
            rows := [{ symbol: "BIZ", exchange: "NYSE" }, { symbol: "GOTO", exchange: "OTC" }]
            rows ~> stocks
            from stocks
        "#, vec![
            "|----------------------------------|",
            "| id | symbol | exchange | history |",
            "|----------------------------------|",
            "| 0  | BIZ    | NYSE     | null    |",
            "| 1  | GOTO   | OTC      | null    |",
            "|----------------------------------|"])
    }

    #[test]
    fn test_read_next_row() {
        verify_exact_table(r#"
            stocks := ns("query_engine.read_next_row.stocks")
            table(symbol: String(8), exchange: String(8), history: Table(last_sale: f64, processed_time: Date)) ~> stocks
            rows := [{ symbol: "BIZ", exchange: "NYSE" }, { symbol: "GOTO", exchange: "OTC" }]
            rows ~> stocks
            // read the last row
            last_row <~ stocks
            last_row
        "#, vec![
            "|----------------------------------|",
            "| id | symbol | exchange | history |",
            "|----------------------------------|",
            "| 1  | GOTO   | OTC      | null    |",
            "|----------------------------------|"])
    }

    #[test]
    fn test_table_append_rows_with_embedded_table() {
        verify_exact_table(r#"
            stocks := ns("query_engine.embedded_c.stocks")
            table(symbol: String(8), exchange: String(8), history: Table(last_sale: f64)) ~> stocks
            append stocks from [
                { symbol: "BIZ", exchange: "NYSE", history: { last_sale: 23.66 } },
                { symbol: "GOTO", exchange: "OTC", history: [{ last_sale: 0.051 }, { last_sale: 0.048 }] }
            ]
            from stocks
        "#, vec![
            r#"|---------------------------------------------------------------------|"#,
            r#"| id | symbol | exchange | history                                    |"#,
            r#"|---------------------------------------------------------------------|"#,
            r#"| 0  | BIZ    | NYSE     | {"last_sale":23.66}                        |"#,
            r#"| 1  | GOTO   | OTC      | [{"last_sale":0.051}, {"last_sale":0.048}] |"#,
            r#"|---------------------------------------------------------------------|"#]);
    }

    #[test]
    fn test_select_from_where_order_by_limit() {
        // create a table with test data
        let params = make_quote_parameters();
        let columns = Column::from_parameters(&params);

        // set up the interpreter
        verify_exact_table(r#"
            stocks := ns("query-engine.select.stocks")
            table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
            [{ symbol: "ABC", exchange: "AMEX", last_sale: 11.77 },
             { symbol: "BIZ", exchange: "NYSE", last_sale: 0.66 },
             { symbol: "UNO", exchange: "OTC", last_sale: 13.2456 },
             { symbol: "GOTO", exchange: "OTC", last_sale: 24.1428 },
             { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }] ~> stocks
            select symbol, exchange, price: last_sale, symbol_md5: util::md5(symbol)
            from stocks
            where last_sale > 1.0
            order by symbol
            limit 2
        "#, vec![
            "|---------------------------------------------------------------------|",
            "| id | symbol | exchange | price   | symbol_md5                       |",
            "|---------------------------------------------------------------------|",
            "| 0  | ABC    | AMEX     | 11.77   | 902fbdd2b1df0c4f70b4a5d23525e932 |",
            "| 2  | UNO    | OTC      | 13.2456 | 59f822bcaa8e119bde63eb00919b367a |",
            "|---------------------------------------------------------------------|"]);
    }
}