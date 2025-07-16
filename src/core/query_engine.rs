#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// QueryEngine - query and aggregation services
////////////////////////////////////////////////////////////////////

use crate::columns::Column;
use crate::data_types::DataType;

use crate::dataframe::Dataframe;
use crate::dataframe::Dataframe::*;
use crate::errors::Errors::*;
use crate::errors::{column_not_found, throw, SyntaxErrors, TypeMismatchErrors};
use crate::expression::Conditions::True;
use crate::expression::Expression::*;
use crate::expression::{Conditions, Expression};
use crate::machine::Machine;
use crate::model_row_collection::ModelRowCollection;
use crate::numbers::Numbers::I64Value;
use crate::packages::PackageOps;
use crate::parameter::Parameter;
use crate::row_collection::RowCollection;
use crate::sequences::Sequence;
use crate::structures::Structure;
use crate::structures::Structures::{Firm, Soft};
use crate::structures::{Row, Structures};
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::From;
use std::hash::Hash;
use std::ops::Deref;

/// Manages and executes SQL-like queries
pub struct QueryEngine;

impl QueryEngine {
    
    /// Evaluates a query language expression
    pub fn evaluate(
        ms: &Machine,
        expression: &Expression,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match expression {
            Delete { .. } => {
                let (from, condition, limit) = Self::unwind_update_graph(expression.clone())?;
                Self::eval_delete_rows(ms, from, condition, limit)
            }
            Undelete { .. } => {
                let (from, condition, limit) = Self::unwind_update_graph(expression.clone())?;
                Self::eval_undelete_rows(ms, from, condition, limit)
            }
            expr => {
                let is_deselect = matches!(expr, Deselect { .. });
                Self::unwind_query_graph(
                    expression.clone(),
                    |from, fields, condition, group_by, having, order_by, limit| {
                        Self::eval_query(ms, from, fields, condition, group_by, having, order_by, limit, is_deselect)
                    })
            }
        }
    }
    
    /// Evaluates a SQL DELETE operation
    /// #### Examples
    /// ```
    /// let stocks = nsd::load("query_engine.select.stocks")
    /// delete stocks where last_sale > 1.0 limit 1
    /// ```
    fn eval_delete_rows(
        ms: &Machine,
        from: Expression,
        condition: Option<Conditions>,
        maybe_limit: Option<Box<Expression>>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let (ms, limit) = ms.evaluate_or_undef(&maybe_limit)?;
        let (ms, mut df) = ms.evaluate_as_dataframe(&from)?;
        df.delete_where(&ms, &condition, limit)
            .map(|deleted| (ms, Number(I64Value(deleted))))
    }
    
    /// Evaluates a SQL UNDELETE/RESTORE operation
    /// #### Examples
    /// ```
    /// let stocks = nsd::load("query_engine.select.stocks")
    /// undelete stocks where last_sale > 1.0 limit 1
    /// ```
    fn eval_undelete_rows(
        ms: &Machine,
        from: Expression,
        condition: Option<Conditions>,
        maybe_limit: Option<Box<Expression>>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let (ms, limit) = ms.evaluate_or_undef(&maybe_limit)?;
        let (ms, mut df) = ms.evaluate_as_dataframe(&from)?;
        df.undelete_where(&ms, &condition, limit)
            .map(|restored| (ms, Number(I64Value(restored))))
    }
    
    /// Evaluates a SQL query
    /// #### Examples
    /// ##### Filtering
    /// ```
    /// stocks where last_sale > 1.0 limit 1
    /// ```
    /// ##### Transformation
    /// ```
    /// select symbol, exchange, last_sale 
    /// from stocks 
    /// where last_sale > 1.0 limit 1
    /// order_by last_sale::desc
    /// ```
    /// ##### Summarization
    /// ```
    /// select 
    ///     total_sale: agg::sum(last_sale),
    ///     min_sale: agg::min(last_sale),
    ///     max_sale: agg::max(last_sale)
    /// from stocks
    /// ```
    /// ##### Aggregation
    /// ```
    /// select 
    ///     exchange,
    ///     total_sale: agg::sum(last_sale),
    ///     min_sale: agg::min(last_sale),
    ///     max_sale: agg::max(last_sale)
    /// from stocks
    /// group_by exchange
    /// having total_sale > 1000.00
    /// order_by total_sale::desc
    /// ```
    fn eval_query(
        ms: &Machine,
        from: Expression,
        fields: Vec<Expression>,
        condition: Option<Conditions>,
        maybe_group_by: Option<Vec<Expression>>,
        having: Option<Conditions>,
        maybe_order_by: Option<Vec<Expression>>,
        maybe_limit: Option<Box<Expression>>,
        is_deselect: bool,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let (ms, df0) = ms.evaluate_as_dataframe(&from)?;
        let fields = if is_deselect {
            Self::get_fields_for_deselection(&df0, &fields)
        } else { fields };
        let (ms, df1) =
            if maybe_group_by.is_some() {
                Self::perform_aggregation(ms, df0, fields, condition, maybe_group_by, having, maybe_order_by, maybe_limit)?
            } else if Self::has_summarization_fields(&ms, &fields) {
                Self::perform_summarization(ms, df0, fields, condition, false)?
            } else {
                Self::perform_transformation(ms, df0, fields, condition, maybe_order_by, maybe_limit)?
            };
        Ok((ms, TableValue(df1)))
    }
    
    fn get_fields_for_deselection(
        df: &Dataframe,
        deselect_fields: &Vec<Expression>
    ) -> Vec<Expression> {
        use std::collections::HashSet;
        
        // determine the columns to exclude
        let mut excluded = HashSet::new();
        for field in deselect_fields.iter() {
            match field {
                Identifier(name) => { excluded.insert(name.clone()); },
                _ => {}
            }
        }
        
        // generate the new fields list
        let mut new_fields = df.get_parameters().iter()
            .filter(|p| !excluded.contains(p.get_name()))
            .map(|p| Identifier(p.get_name().to_string()))
            .collect::<Vec<_>>();
        new_fields
    }
    
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
        let (from, condition, _limit) = Self::unwind_update_graph(source.clone())?;
    
        // get a reference to the dataframe
        let (ms, mut df) = ms.evaluate_as_dataframe(&from)?;
    
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
        let (from, condition, limit) = Self::unwind_update_graph(source.clone())?;
    
        // get a reference to the dataframe
        let (ms, mut df) = ms.evaluate_as_dataframe(&from)?;
        
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
            Identifier(name) => Ok((ms.with_variable(name, TableValue(ModelTable(mrc))), Boolean(true))),
            other => throw(Exact(other.to_code()))
        }
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
    pub fn eval_push_rows(
        ms: &Machine,
        source: &Expression,
        target: &Expression,
        is_overwrite: bool,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let (target, condition, limit) = Self::unwind_update_graph(target.clone())?;
        match &condition {
            None => Self::do_table_rows_insert(ms, source, &target),
            Some(..) if is_overwrite =>
                Self::do_table_rows_overwrite(ms, &target, source, &condition, &limit),
            Some(..) =>
                Self::do_table_rows_update(ms, &target, source, &condition, &limit)
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
    fn do_table_rows_insert(
        ms: &Machine,
        source: &Expression,
        target: &Expression,
    ) -> std::io::Result<(Machine, TypedValue)>  {
        let ms = ms.to_owned();
        // determine the target row collection
        let (ms, mut dest) = ms.evaluate_as_dataframe(target)?;
        // retrieve rows from the source
        let (_ms, src) = ms.evaluate_as_dataframe(source)?;
        // write the rows to the target
        let src_params = src.get_parameters();
        let dest_params = dest.get_parameters();
        let dest_rows = Structures::transform_rows(
            &src_params,
            &src.get_rows(),
            &dest_params
        );
        dest.append_rows(dest_rows)
            .map(|inserted| (ms, Number(I64Value(inserted))))
    }
    
    /// Evaluates a SQL UPDATE/REPLACE operation
    /// #### Examples
    /// ```
    /// let stocks = nsd::load("query_engine.select.stocks")
    /// { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 } 
    ///     ~>> (stocks where last_sale > 1.0 limit 1)
    /// ```
    fn do_table_rows_overwrite(
        ms: &Machine,
        target: &Expression,
        source: &Expression,
        condition: &Option<Conditions>,
        maybe_limit: &Option<Box<Expression>>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let (ms, limit) = ms.evaluate_or_undef(maybe_limit)?;
        let (ms, df) = ms.evaluate_as_dataframe(target)?;
        let (fields, values) = Self::separate_fields_and_values(&ms, &target, &source)?;
        Dataframe::overwrite_where(df, &ms, &fields, &values, condition, limit)
            .map(|(_, overwritten)| (ms, Number(I64Value(overwritten))))
    }
    
    /// Evaluates a SQL UPDATE/MODIFY operation
    /// #### Examples
    /// ```
    /// let stocks = nsd::load("query_engine.select.stocks")
    /// { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 } 
    ///     ~> (stocks where last_sale > 1.0 limit 1)
    /// ```
    fn do_table_rows_update(
        ms: &Machine,
        table: &Expression,
        source: &Expression,
        condition: &Option<Conditions>,
        maybe_limit: &Option<Box<Expression>>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let (ms, limit) = ms.evaluate_or_undef(maybe_limit)?;
        let (ms, df) = ms.evaluate_as_dataframe(table)?;
        let (fields, values) = Self::separate_fields_and_values(&ms, &table, &source)?;
        Dataframe::update_where(df, &ms, &fields, &values, &condition, limit)
            .map(|modified| (ms, Number(I64Value(modified))))
    }
    
    /// Performs an aggregation query
    /// ##### Example
    /// ```
    /// select 
    ///     exchange,
    ///     total_sale: agg::sum(last_sale),
    ///     min_sale: agg::min(last_sale),
    ///     max_sale: agg::max(last_sale)
    /// from stocks
    /// group_by exchange
    /// having total_sale > 1000.00
    /// order_by total_sale::desc
    /// ```
    fn perform_aggregation(
        ms: Machine,
        df: Dataframe,
        fields: Vec<Expression>,
        condition: Option<Conditions>,
        maybe_group_by: Option<Vec<Expression>>,
        having: Option<Conditions>,
        maybe_order_by: Option<Vec<Expression>>,
        maybe_limit: Option<Box<Expression>>,
    ) -> std::io::Result<(Machine, Dataframe)> {
        match maybe_group_by {
            None => Ok((ms, df)),
            Some(group_by) => {
                // split rows into dataframes by key 
                let (ms, mut sink) = Self::do_table_partition_by_key(ms, df, condition, group_by)?;
                
                // perform the summarization of each dataframe by key
                let mut partitions = vec![];
                for mrc in sink.values() {
                   let (_ms, mdf) = Self::perform_summarization(ms.clone(), ModelTable(mrc.to_owned()), fields.clone(), None, true)?;
                    partitions.push(mdf)
                }
                
                // recombine the dataset
                let combined = Dataframe::combine_tables(partitions);
    
                // filter the dataset via having condition
                let (ms, filtered) = Self::do_table_filter(ms, combined, having)?;
    
                // sort the combined dataset
                let sorted = Self::do_table_sort(filtered, maybe_order_by)?;
                
                // apply the optional limit
                Self::do_table_limit(ms, sorted, maybe_limit)
            }
        }
    }
    
    /// Performs a summarization query
    /// #### Example
    /// ```
    /// select 
    ///     total_sale: agg::sum(last_sale),
    ///     min_sale: agg::min(last_sale),
    ///     max_sale: agg::max(last_sale)
    /// from stocks
    /// ```
    fn perform_summarization(
        ms: Machine,
        df: Dataframe,
        fields: Vec<Expression>,
        condition: Option<Conditions>,
        is_group_by: bool,
    ) -> std::io::Result<(Machine, Dataframe)> {
        let params = Self::build_aggregation_parameters(&fields, df.get_columns(), is_group_by)?;
        let mut ms0 = ms.clone();
        let condition = condition.unwrap_or(True);
        let mut agg_values = fields.iter().map(|_| Null).collect::<Vec<_>>();
        for row in df.iter() {
            ms0 = ms0.with_row(df.get_columns(), &row);
            if ms0.is_true(&condition)?.1 {
                agg_values = vec![];
                for field in &fields {
                    let (ms1, value) = ms0.evaluate(field)?;
                    ms0 = ms1;
                    agg_values.push(value);
                }
            }
        }
        let mrc = ModelRowCollection::from_parameters_and_rows(&params, &vec![
            Row::new(0, agg_values)
        ]);
        Ok((ms, ModelTable(mrc)))
    }
    
    /// Evaluates a transformation query
    /// #### Example
    /// ```
    /// select symbol, exchange, last_sale 
    /// from stocks 
    /// where last_sale > 1.0 limit 1
    /// order_by last_sale::desc
    /// ```
    fn perform_transformation(
        ms: Machine,
        df: Dataframe,
        fields: Vec<Expression>,
        condition: Option<Conditions>,
        maybe_order_by: Option<Vec<Expression>>,
        maybe_limit: Option<Box<Expression>>,
    ) -> std::io::Result<(Machine, Dataframe)> {
        // step 1: determine output layout
        let (ms0, rc1) = (ms.clone(), df);
        let fields = Self::generate_fields_list(fields, rc1.get_columns());
        let columns = Self::resolve_fields_as_columns(rc1.get_columns(), &fields)?;
    
        // step 2: transform and filter the dataset
        let (ms0, rc2) = Self::do_table_transform_and_filter(&ms0, &rc1, &fields, &columns, &condition)?;
        
        // step 3: sort the dataset
        let rc3 = Self::do_table_sort(rc2, maybe_order_by)?;
    
        // step 4: limit the dataset
        Self::do_table_limit(ms0, rc3, maybe_limit)
    }
    
    fn do_table_filter(
        ms: Machine,
        src: Dataframe,
        condition: Option<Conditions>
    ) -> std::io::Result<(Machine, Dataframe)> {
        let mut mrc = ModelRowCollection::new(src.get_columns().clone());
        let condition = condition.unwrap_or(True);
        for row in src.iter() {
            if ms.with_row(src.get_columns(), &row).is_true(&condition)?.1 {
                mrc.append_row(row)?;
            }
        }
        Ok((ms, ModelTable(mrc)))
    }
    
    fn do_table_limit(
        ms: Machine,
        src: Dataframe,
        maybe_limit: Option<Box<Expression>>,
    ) -> std::io::Result<(Machine, Dataframe)> {
        if let (ms, Number(limit)) = ms.evaluate_or_undef(&maybe_limit)? {
            let mut dest = ModelRowCollection::new(src.get_columns().clone());
            let (mut row_id, cut_off) = (0, limit.to_usize());
            while row_id < cut_off {
                match src.read_one(row_id)? {
                    None => {}
                    Some(row) => { dest.append_row(row)?; }
                }
                row_id += 1;
            }
            Ok((ms, ModelTable(dest)))
        } else {
            Ok((ms, src))
        }
    }
    
    fn do_table_sort(
        src: Dataframe,
        maybe_order_by: Option<Vec<Expression>>,
    ) -> std::io::Result<Dataframe> {
        match &maybe_order_by {
            None => Ok(src),
            Some(_order_by) => {
                let columns = src.get_columns();
                let ordering = Self::determine_order_by_columns(maybe_order_by, &columns)?;
                src.sort_by_columns(&ordering)
            }
        }
    }
    
    fn do_table_partition_by_key(
        ms: Machine,
        df: Dataframe,
        condition: Option<Conditions>,
        group_by: Vec<Expression>,
    ) -> std::io::Result<(Machine, HashMap<String, ModelRowCollection>)> {
        let mut sink = HashMap::new();
        let condition = condition.unwrap_or(True);
        let columns = df.get_columns();
        for row in df.iter() {
            let ms0 = ms.with_row(columns, &row);
            if ms0.is_true(&condition)?.1 {
                let (_ms0, values) = ms0.evaluate_as_vec(&group_by)?;
                let key = values.iter().map(|v| v.unwrap_value())
                    .collect::<Vec<_>>()
                    .join("$");
                let mut mrc = sink.entry(key).or_insert(ModelRowCollection::new(columns.clone()));
                mrc.append_row(row)?;
            }
        }
        Ok((ms, sink))
    }
    
    fn do_table_transform_and_filter(
        ms0: &Machine,
        rc0: &Dataframe,
        field_values: &Vec<Expression>,
        field_columns: &Vec<Column>,
        condition: &Option<Conditions>,
    ) -> std::io::Result<(Machine, Dataframe)> {
        let columns = rc0.get_columns();
        let mut rc1 = ModelRowCollection::new(field_columns.clone());
        for row in rc0.iter() {
            let ms = row.pollute(&ms0, columns);
            if row.matches(&ms, condition, columns) {
                match ms.evaluate_as_array(field_values)? {
                    (_ms, ArrayValue(array)) => {
                        let new_row = Row::new(row.get_id(), array.get_values().clone());
                        rc1.overwrite_row(new_row.get_id(), new_row)?;
                    }
                    (_ms, z) => return throw(TypeMismatch(TypeMismatchErrors::ArrayExpected(z.to_string())))
                }
            }
        }
        Ok((ms0.clone(), ModelTable(rc1)))
    }
    
    fn build_aggregation_parameters(
        fields: &Vec<Expression>,
        columns: &Vec<Column>,
        is_group_by: bool,
    ) -> std::io::Result<Vec<Parameter>> {
        let mut params = vec![];
        for field in fields {
            match field {
                Identifier(name) if is_group_by =>
                    match columns.iter().find(|c| c.get_name() == name) {
                        None => return column_not_found(name.as_str(), columns),
                        Some(column) => params.push(column.to_parameter())
                    }
                NamedValue(name, value) =>
                    params.push(Parameter::new(name, value.infer_type())),
                other =>
                    return throw(Exact(format!("All fields must be aliased near {}", other)))
            }
        }
        Ok(params)
    }
    
    fn has_summarization_fields(ms: &Machine, fields: &Vec<Expression>) -> bool {
        !fields.is_empty() && fields.iter().any(|field| match field {
            NamedValue(_, value) =>
                match value.deref() {
                    Literal(..) => true,
                    ColonColon(a, _) => match a.deref() {
                        Identifier(package) => package == "agg",
                        _ => false
                    }
                    FunctionCall { fx, .. } => {
                        match ms.evaluate(&fx) {
                            Ok((_ms, PlatformOp(PackageOps::Agg(..)))) => true,
                            _ => false
                        }
                    }
                    expr => expr.is_pure()
                }
            _ => false
        })
    }
    
    fn determine_order_by_columns(
        maybe_order_columns: Option<Vec<Expression>>,
        columns: &Vec<Column>,
    ) -> std::io::Result<Vec<(usize, bool)>> {
        match maybe_order_columns {
            Some(order_columns) => {
                let mut orderings = vec![];
                for order_column in order_columns.iter() {
                    let ordering = match order_column {
                        // order_by symbol::asc | order_by 3::desc
                        ColonColon(cell, order) => {
                            let is_ascending = Self::determine_sort_order(order)?;
                            Self::get_column_id_then(cell, columns, |id| (id, is_ascending))?
                        }
                        // order_by symbol | order_by 3
                        cell => Self::get_column_id_then(cell, columns, |id| (id, true))?
                    };
                    orderings.push(ordering)
                }
                Ok(orderings)
            }
            _ => Ok(vec![])
        }
    }
    
    /// Returns true if (asc)ending order or false for desc(ending) order
    fn determine_sort_order(order: &Expression) -> std::io::Result<bool> {
        match order {
            Identifier(name) =>
                match name.as_str() {
                    "asc" => Ok(true),
                    "desc" => Ok(false),
                    _ => throw(Exact("Expected column::asc or column::desc".into()))
                }
            _ => throw(Exact("order_by expression expected".into()))
        }
    }
    
    fn get_column_id_then<F, G>(
        cell: &Expression,
        columns: &Vec<Column>,
        f: F,
    ) -> std::io::Result<G>
    where
        F: Fn(usize) -> G,
    {
        match cell {
            // symbol (eg. group_by symbol)
            Identifier(name) =>
                match columns.iter().position(|c| c.get_name() == name) {
                    Some(column_id) => Ok(f(column_id)),
                    None => column_not_found(name.as_str(), columns)
                }
            // 5 (eg. group_by 5)
            Literal(Number(number)) =>
                match number.to_usize() {
                    index if index < 1 || index > columns.len() =>
                        throw(Exact("Column index is out of range".into())),
                    column_index => Ok(f(column_index - 1))
                }
            _ => throw(Exact("Column identifier expected".into()))
        }
    }
    
    fn generate_fields_list(
        fields: Vec<Expression>,
        columns: &Vec<Column>
    ) -> Vec<Expression> {
        if fields.is_empty() {
            columns.iter()
                .map(|c| Identifier(c.get_name().into()))
                .collect()
        } else {
            fields
        }
    }
    
    fn separate_fields_and_values(
        ms: &Machine,
        target: &Expression,
        source: &Expression,
    ) -> std::io::Result<(Vec<Expression>, Vec<Expression>)> {
        if let (_, Structured(Soft(structure))) = ms.evaluate(&source)? {
            let (_ms, dest) = ms.evaluate_as_dataframe(target)?;
            let columns = dest.get_columns();
            let row = Row::from_tuples(0, columns, &structure.to_name_values());
            let (fields, values) = Self::split_row_into_fields_and_values(columns, &row);
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
            let column = Self::resolve_field_as_column(field, columns, &column_dict, offset)?;
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
                    other => Ok(Column::new(label, Expression::infer(other), Null, offset))
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
    
    pub fn unwind_query_graph<F, E>(
        expr: Expression,
        f: F
    ) -> std::io::Result<E>
    where
        F: Fn(Expression, Vec<Expression>, Option<Conditions>, Option<Vec<Expression>>, Option<Conditions>, Option<Vec<Expression>>, Option<Box<Expression>>) -> std::io::Result<E>,
    {
        fn unwind<T, S>(
            from: Expression,
            fields: Vec<Expression>,
            condition: Option<Conditions>,
            group_by: Option<Vec<Expression>>,
            having: Option<Conditions>,
            order_by: Option<Vec<Expression>>,
            limit: Option<Box<Expression>>,
            f: T,
        ) -> std::io::Result<S>
        where
            T: Fn(Expression, Vec<Expression>, Option<Conditions>, Option<Vec<Expression>>, Option<Conditions>, Option<Vec<Expression>>, Option<Box<Expression>>) -> std::io::Result<S>,
        {
            match from {
                Delete { from } =>
                    unwind(from.deref().clone(), fields, condition, group_by, having, order_by, limit, f),
                Deselect { fields, from } =>
                    unwind(from.deref().clone(), fields, condition, group_by, having, order_by, limit, f),
                GroupBy { from, columns: group_by } =>
                    unwind(from.deref().clone(), fields, condition, Some(group_by), having, order_by, limit, f),
                Having { from, condition: having } =>
                    unwind(from.deref().clone(), fields, condition, group_by, Some(having), order_by, limit, f),
                Limit { from, limit } =>
                    unwind(from.deref().clone(), fields, condition, group_by, having, order_by, Some(limit), f),
                OrderBy { from, columns: order_by } =>
                    unwind(from.deref().clone(), fields, condition, group_by, having, Some(order_by), limit, f),
                Select { fields, from } =>
                    unwind(from.deref().clone(), fields, condition, group_by, having, order_by, limit, f),
                Undelete { from } =>
                    unwind(from.deref().clone(), fields, condition, group_by, having, order_by, limit, f),
                Where { from, condition } =>
                    unwind(from.deref().clone(), fields, Some(condition), group_by, having, order_by, limit, f),
                _ => f(from, fields, condition, group_by, having, order_by, limit)
            }
        }
        unwind(expr, vec![], None, None, None, None, None, f)
    }
    
    fn unwind_update_graph(
        expression: Expression
    ) -> std::io::Result<(Expression, Option<Conditions>, Option<Box<Expression>>)> {
        Self::unwind_query_graph(
            expression.clone(),
            |from, fields, condition, group_by, having, order_by, limit| {
                if !fields.is_empty() {
                    return throw(Exact("fields are not supported in this context".into()))
                } else if group_by.is_some() {
                    return throw(Exact("group_by is not supported in this context".into()))
                } else if having.is_some() {
                    return throw(Exact("having is not supported in this context".into()))
                } else if order_by.is_some() {
                    return throw(Exact("order_by is not supported in this context".into()))
                }
                Ok((from, condition, limit))
            })
    }

}

/// Oxide QL tests
#[cfg(test)]
mod tests {
    use crate::dataframe::Dataframe::ModelTable;
    use crate::interpreter::Interpreter;
    use crate::model_row_collection::ModelRowCollection;
    use crate::testdata::*;
    use crate::typed_values::TypedValue::*;

    #[test]
    fn test_delete_where() {
        let mut interpreter = Interpreter::new();
        interpreter = verify_exact_code_with(interpreter, r#"
            stocks = nsd::save(
                "query_engine.delete.stocks",
                [{ symbol: "ABC", exchange: "AMEX", last_sale: 11.77 },
                 { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                 { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                 { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                 { symbol: "BKPQ", exchange: "OTCBB", last_sale: 0.0786 }] 
            )
        "#, "true");

        // remove some rows
        interpreter = verify_exact_code_with(interpreter, r#"
            delete stocks where last_sale > 1.0
        "#, "2");

        // verify the contents
        interpreter = verify_exact_table_with(interpreter, r#"
            stocks
        "#, vec![
            "|------------------------------------|", 
            "| id | symbol | exchange | last_sale |", 
            "|------------------------------------|", 
            "| 1  | UNO    | OTC      | 0.2456    |", 
            "| 3  | GOTO   | OTC      | 0.1428    |", 
            "| 4  | BKPQ   | OTCBB    | 0.0786    |", 
            "|------------------------------------|"]);
    }

    #[test]
    fn test_select_from_structure() {
        verify_exact_table(r#"
            select name, age from { name: 'Tom', age: 37, sex: 'M' }
        "#, vec![
            "|-----------------|", 
            "| id | name | age |", 
            "|-----------------|", 
            "| 0  | Tom  | 37  |", 
            "|-----------------|"])
    }

    #[test]
    fn test_table_literal_reverse() {
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
            Table(
                symbol: String(8),
                exchange: String(8),
                last_sale: f64
            )::new
        "#, TableValue(ModelTable(ModelRowCollection::with_rows(
            make_quote_columns(), Vec::new(),
        ))))
    }

    #[test]
    fn test_table_intersect() {
        let mut interpreter = Interpreter::new();
        interpreter = verify_exact_table_with(interpreter, r#"
            let table_a = 
                |--------------------------------------|
                | symbol | exchange | last_sale | rank |
                |--------------------------------------|
                | ASX    | NYSE     | 113.76    | 1    |
                | ABC    | AMEX     | 24.98     | 2    |
                | JET    | NASDAQ   | 64.24     | 3    |
                |--------------------------------------|
            let table_b = 
                |--------------------------------------|
                | symbol | exchange | last_sale | rank |
                |--------------------------------------|
                | BOOM   | NYSE     | 113.76    | 1    |
                | IBM    | NASDAQ   | 64.24     | 3    |
                | ABC    | AMEX     | 24.98     | 2    |
                |--------------------------------------|    
            table_a & table_b
        "#, vec![
            "|-------------------------------------------|", 
            "| id | symbol | exchange | last_sale | rank |", 
            "|-------------------------------------------|", 
            "| 0  | ABC    | AMEX     | 24.98     | 2    |", 
            "|-------------------------------------------|"]);
    }

    #[test]
    fn test_table_product() {
        let mut interpreter = Interpreter::new();
        interpreter = verify_exact_code_with(interpreter, r#"
            faces = select face: value from (
                (2..=10)::map(n -> n::to_string()) ++ ["J", "Q", "K", "A"]
            )::to_table()
            suits = select suit: value from ["♥️", "♦️", "♣️", "♠️"]::to_table()
        "#, "true");

        interpreter = verify_exact_table_with(interpreter, r#"
            faces * suits limit 5
        "#, vec![
            "|------------------|", 
            "| id | face | suit |", 
            "|------------------|", 
            "| 0  | 2    | ♥️   |", 
            "| 1  | 2    | ♦️   |", 
            "| 2  | 2    | ♣️   |",
            "| 3  | 2    | ♠️   |", 
            "| 4  | 3    | ♥️   |", 
            "|------------------|"]);
    }

    #[test]
    fn test_table_union() {
        let mut interpreter = Interpreter::new();
        interpreter = verify_exact_table_with(interpreter, r#"
            let table_a = 
                |--------------------------------------|
                | symbol | exchange | last_sale | rank |
                |--------------------------------------|
                | ASX    | NYSE     | 113.76    | 1    |
                | ABC    | AMEX     | 24.98     | 2    |
                | JET    | NASDAQ   | 64.24     | 3    |
                |--------------------------------------|
            let table_b = 
                |--------------------------------------|
                | symbol | exchange | last_sale | rank |
                |--------------------------------------|
                | BOOM   | AMEX      | 56.89    | 1    |
                | IBM    | NYSE      | 64.24    | 3    |
                | CAT    | OTCBB     | 24.98    | 2    |
                |--------------------------------------|    
            table_a | table_b
        "#, vec![
            "|-------------------------------------------|", 
            "| id | symbol | exchange | last_sale | rank |", 
            "|-------------------------------------------|", 
            "| 0  | ASX    | NYSE     | 113.76    | 1    |", 
            "| 1  | ABC    | AMEX     | 24.98     | 2    |", 
            "| 2  | JET    | NASDAQ   | 64.24     | 3    |", 
            "| 3  | BOOM   | AMEX     | 56.89     | 1    |", 
            "| 4  | IBM    | NYSE     | 64.24     | 3    |", 
            "| 5  | CAT    | OTCBB    | 24.98     | 2    |", 
            "|-------------------------------------------|"]);
    }

    #[test]
    fn test_save_table_literal_to_namespace() {
        // create the table in a namespace
        let mut interpreter = Interpreter::new();
        interpreter = verify_exact_code_with(interpreter, r#"
            stocks = nsd::save(
                "query_engine.from_literal.stocks",
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
        // create the table in a namespace
        let mut interpreter = Interpreter::new();
        interpreter = verify_exact_code_with(interpreter, r#"
            stocks = nsd::save(
                "query_engine.crud.stocks",
                Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
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
    fn test_table_deselect_from_variable() {
        verify_exact_table(r#"
            // define a new table
            let stocks = 
                |--------------------------------------|
                | symbol | exchange | last_sale | rank |
                |--------------------------------------|
                | BOOM   | NYSE     | 113.76    | 1    |
                | ABC    | AMEX     | 24.98     | 2    |
                | JET    | NASDAQ   | 64.24     | 3    |
                |--------------------------------------|
             
            // filter out rank
            deselect rank from stocks
        "#, vec![
            "|------------------------------------|",
            "| id | symbol | exchange | last_sale |",
            "|------------------------------------|",
            "| 0  | BOOM   | NYSE     | 113.76    |",
            "| 1  | ABC    | AMEX     | 24.98     |",
            "| 2  | JET    | NASDAQ   | 64.24     |",
            "|------------------------------------|"]);
    }
    
    #[test]
    fn test_table_summarization_agg_qualified() {
        verify_exact_table(r#"
            select
                sale_date: 2025-01-13T03:25:47.350Z,
                min_sale: agg::min(last_sale),
                max_sale: agg::max(last_sale),
                avg_sale: agg::avg(last_sale),
                total_sale: agg::sum(last_sale),
                volume: agg::count(last_sale)
            from
                |--------------------------------|
                | symbol | exchange  | last_sale |
                |--------------------------------|
                | GIF    | NYSE      | 11.77     |
                | TRX    | NASDAQ    | 32.97     |
                | RLP    | NYSE      | 23.66     |
                | GTO    | NASDAQ    | 51.23     |
                | BST    | NASDAQ    | 214.88    |
                |--------------------------------|
        "#, vec![
            "|--------------------------------------------------------------------------------------|", 
            "| id | sale_date                | min_sale | max_sale | avg_sale | total_sale | volume |", 
            "|--------------------------------------------------------------------------------------|", 
            "| 0  | 2025-01-13T03:25:47.350Z | 11.77    | 214.88   | 66.902   | 334.51     | 5      |", 
            "|--------------------------------------------------------------------------------------|"]);
    }

    #[test]
    fn test_table_summarization_agg_imported() {
        verify_exact_table(r#"
            use agg
            select
                code: 5 + 5,
                min_sale: min(last_sale),
                max_sale: max(last_sale),
                avg_sale: avg(last_sale),
                total_sale: sum(last_sale),
                volume: count(last_sale)
            from
                |--------------------------------|
                | symbol | exchange  | last_sale |
                |--------------------------------|
                | GIF    | NYSE      | 11.77     |
                | TRX    | NASDAQ    | 32.97     |
                | RLP    | NYSE      | 23.66     |
                | GTO    | NASDAQ    | 51.23     |
                | BST    | NASDAQ    | 214.88    |
                |--------------------------------|
        "#, vec![
            "|------------------------------------------------------------------|", 
            "| id | code | min_sale | max_sale | avg_sale | total_sale | volume |", 
            "|------------------------------------------------------------------|", 
            "| 0  | 10   | 11.77    | 214.88   | 66.902   | 334.51     | 5      |", 
            "|------------------------------------------------------------------|"]);
    }
    
    #[test]
    fn test_table_select_from_group_by() {
        verify_exact_table(r#"
            select exchange, total_sale: agg::sum(last_sale)
            from 
                |--------------------------------|
                | symbol | exchange  | last_sale |
                |--------------------------------|
                | GIF    | NYSE      | 11.77     |
                | TRX    | NASDAQ    | 32.97     |
                | RLP    | NYSE      | 23.66     |
                | GTO    | NASDAQ    | 51.23     |
                | BST    | NASDAQ    | 214.88    |
                |--------------------------------|
            group_by exchange
            order_by total_sale::asc
        "#, vec![
            "|----------------------------|", 
            "| id | exchange | total_sale |", 
            "|----------------------------|", 
            "| 0  | NYSE     | 35.43      |", 
            "| 1  | NASDAQ   | 299.08     |", 
            "|----------------------------|"]);
    }

    #[test]
    fn test_table_select_from_group_by_having() {
        verify_exact_table(r#"
            use agg
            select 
                exchange,
                min_sale: min(last_sale),
                max_sale: max(last_sale),
                avg_sale: avg(last_sale),
                total_sale: sum(last_sale),
                qty: count(last_sale)            
            from 
                |--------------------------------|
                | symbol | exchange  | last_sale |
                |--------------------------------|
                | GIF    | NYSE      | 11.75     |
                | TRX    | NASDAQ    | 32.96     |
                | RLP    | NYSE      | 23.66     |
                | GTO    | NASDAQ    | 51.23     |
                | BST    | NASDAQ    | 214.88    |
                | SHMN   | OTCBB     | 5.02      |
                | XCD    | OTCBB     | 1.37      |
                |--------------------------------|
            group_by exchange
            having total_sale > 10.0
            order_by total_sale::asc
        "#, vec![
            "|-------------------------------------------------------------------|", 
            "| id | exchange | min_sale | max_sale | avg_sale | total_sale | qty |", 
            "|-------------------------------------------------------------------|", 
            "| 0  | NYSE     | 11.75    | 23.66    | 17.705   | 35.41      | 2   |", 
            "| 1  | NASDAQ   | 32.96    | 214.88   | 99.69    | 299.07     | 3   |", 
            "|-------------------------------------------------------------------|"]);
    }

    #[test]
    fn test_table_lifo_queue_in_namespace() {
        // create the table in a namespace
        let mut interpreter = Interpreter::new();
        interpreter = verify_exact_code_with(interpreter, r#"
            stocks = nsd::save(
                "query_engine.push_and_pull.stocks",
                [{ symbol: "BMX", exchange: "NYSE", last_sale: 56.88 },
                 { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 },
                 { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                 { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                 { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                 { symbol: "BOOM", exchange: "NASDAQ", last_sale: 56.87 },
                 { symbol: "TRX", exchange: "NASDAQ", last_sale: 7.9311 }] 
            )
        "#, "true");
        
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
            // create a new table
            stocks = nsd::save(
                "query_engine.push_and_pull_cnd.stocks",
                [{ symbol: "BMX", exchange: "NYSE", last_sale: 56.88 },
                 { symbol: "ABC", exchange: "NASDAQ", last_sale: 12.49 },
                 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 },
                 { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                 { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                 { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                 { symbol: "BOOM", exchange: "NASDAQ", last_sale: 56.87 },
                 { symbol: "TRX", exchange: "NASDAQ", last_sale: 7.9311 }] 
            )
        "#, "true");

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
    fn test_table_order_by_column_index() {
        verify_exact_table(r#"
            |--------------------------------|
            | symbol | exchange  | last_sale |
            |--------------------------------|
            | ABC    | AMEX      | 11.77     |
            | UNO    | OTC       | 0.2456    |
            | BIZ    | NYSE      | 23.66     |
            | GOTO   | OTC       | 0.1428    |
            | BKP    | OTHER_OTC | 0.1421    |
            |--------------------------------|
            order_by 3::desc
        "#, vec![
            "|-------------------------------------|",
            "| id | symbol | exchange  | last_sale |",
            "|-------------------------------------|",
            "| 0  | BIZ    | NYSE      | 23.66     |",
            "| 1  | ABC    | AMEX      | 11.77     |",
            "| 2  | UNO    | OTC       | 0.2456    |",
            "| 3  | GOTO   | OTC       | 0.1428    |",
            "| 4  | BKP    | OTHER_OTC | 0.1421    |",
            "|-------------------------------------|"]);
    }

    #[test]
    fn test_table_push_then_delete() {
        verify_exact_table(r#"
            // create a new table
            stocks = nsd::save(
                "query_engine.push_then_delete.stocks",
                [{ symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                 { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] 
            )
             
            // delete some data 
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
        // create a table and append some rows
        let mut interpreter = Interpreter::new();
        interpreter = verify_exact_code_with(interpreter, r#"
            // create a new table
            stocks = nsd::save(
                "query_engine.select1.stocks",
                [{ symbol: "ABC", exchange: "AMEX", last_sale: 11.77 },
                 { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                 { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                 { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                 { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }]
            )
        "#, "true");

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
            "| 0  | BOOM   | 0.0872    |",
            "| 1  | GOTO   | 0.1428    |",
            "|-------------------------|"]);
    }

    #[test]
    fn test_table_select_from_variable() {
        verify_exact_table(r#"
            // create a new table
            stocks = nsd::save(
                "query_engine.select2.stocks",
                [{ symbol: "ABC", exchange: "AMEX", last_sale: 11.77 },
                 { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                 { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                 { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                 { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }]
            )
             
            // perform transformation and filtering 
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
            "| 1  | BIZ    | NYSE     | 23.66     |",
            "|------------------------------------|"]);
    }

    #[test]
    fn test_select_from_where_order_by_limit() {
        verify_exact_table(r#"
            // create a new table
            stocks = nsd::save(
                "query_engine.select3.stocks",
                [{ symbol: "ABC", exchange: "AMEX", last_sale: 11.77 },
                 { symbol: "BIZ", exchange: "NYSE", last_sale: 0.66 },
                 { symbol: "UNO", exchange: "OTC", last_sale: 13.2456 },
                 { symbol: "GOTO", exchange: "OTC", last_sale: 24.1428 },
                 { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }] 
            )

            // perform transformation and filtering
            select symbol, exchange, price: last_sale, symbol_md5: util::md5(symbol)
            from stocks
            where last_sale > 1.0
            order_by symbol::asc
            limit 2
        "#, vec![
            "|-----------------------------------------------------------------------|",
            "| id | symbol | exchange | price   | symbol_md5                         |",
            "|-----------------------------------------------------------------------|",
            "| 0  | ABC    | AMEX     | 11.77   | 0B902fbdd2b1df0c4f70b4a5d23525e932 |",
            "| 1  | GOTO   | OTC      | 24.1428 | 0B4b8bb3c94a9676b5f34ace4d7102e5b9 |",
            "|-----------------------------------------------------------------------|"]);
    }

    #[test]
    fn test_table_embedded_describe() {
        verify_exact_table(r#"
            // create a new table
            stocks = nsd::save(
                "query_engine.embedded_a.stocks",
                Table(symbol: String(8), exchange: String(8), history: Table(last_sale: f64, processed_time: DateTime))::new
            )
            
            // describe the table
            stocks::describe()
        "#, vec![
            "|-----------------------------------------------------------------------------------------------|", 
            "| id | name     | type                                            | default_value | is_nullable |", 
            "|-----------------------------------------------------------------------------------------------|", 
            "| 0  | symbol   | String(8)                                       | null          | true        |", 
            "| 1  | exchange | String(8)                                       | null          | true        |", 
            "| 2  | history  | Table(last_sale: f64, processed_time: DateTime) | null          | true        |", 
            "|-----------------------------------------------------------------------------------------------|"])
    }

    #[test]
    fn test_table_embedded_empty() {
        verify_exact_table(r#"
            // create a new table
            stocks = nsd::save(
                "query_engine.embedded_b.stocks",
                Table(symbol: String(8), exchange: String(8), history: Table(last_sale: f64, processed_time: DateTime))::new
            )
            
            // insert some data
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
            // create a new table
            stocks = nsd::save(
                "query_engine.embedded_c.stocks",
                Table(symbol: String(8), exchange: String(8), history: Table(last_sale: f64))::new
            )
            
            // insert some data
            [{ symbol: "BIZ", exchange: "NYSE", history: { last_sale: 23.66 }::to_table() },
             { symbol: "GOTO", exchange: "OTC", history: [
                    { last_sale: 0.051 }, 
                    { last_sale: 0.048 }
                ]::to_table()
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
    fn test_referenced_embedded_table() {
        let mut interpreter = Interpreter::new();
        interpreter = verify_exact_code_with(interpreter, r#"
            let stocks = nsd::save(
                "query_engine.examples.stocks",
                Table(symbol: String(8), exchange: String(8), history: Table(last_sale: f64, processed_time: DateTime))::new
            )
        "#, "true");
        
        // write a row to the `stocks` table
        interpreter = verify_exact_code_with(interpreter, r#"
            { symbol: "BIZ", exchange: "NYSE", history: [
                { last_sale: 11.67, processed_time: 2025-01-13T03:25:47.350Z }
            ]::to_table()} ~> stocks
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
            { symbol: "ABY", exchange: "NYSE", history: [
                { last_sale: 78.33, processed_time: 2025-01-13T03:25:47.392Z }
            ]::to_table()} ~> stocks
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
            nsd::drop("query_engine.overwrite.stocks")
            stocks = nsd::save(
                "query_engine.overwrite.stocks",
                [{ symbol: "ABC", exchange: "AMEX", last_sale: 11.77 },
                 { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                 { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                 { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                 { symbol: "BKP", exchange: "OTC", last_sale: 0.0786 }]
            )
        "#, "true");

        // verify the contents of `stocks`
        interpreter = verify_exact_table_with(interpreter, r#"
            stocks
         "#, vec![
            "|------------------------------------|", 
            "| id | symbol | exchange | last_sale |", 
            "|------------------------------------|", 
            "| 0  | ABC    | AMEX     | 11.77     |", 
            "| 1  | UNO    | OTC      | 0.2456    |", 
            "| 2  | BIZ    | NYSE     | 23.66     |", 
            "| 3  | GOTO   | OTC      | 0.1428    |", 
            "| 4  | BKP    | OTC      | 0.0786    |", 
            "|------------------------------------|"]);
        
        interpreter = verify_exact_code_with(interpreter, r#"
            {symbol: "BKPQ", exchange: "OTHER_OTC", last_sale: 0.1421} 
                 ~>> (stocks where symbol is "BKP")
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
            "| 4  | BKPQ   | OTHER_OTC | 0.1421    |", 
            "|-------------------------------------|"]);
    }

    #[test]
    fn test_update_where() {
        let mut interpreter = Interpreter::new();
        interpreter = verify_exact_code_with(interpreter, r#"
            stocks = nsd::save(
                "query_engine.update.stocks",
                [{ symbol: "ABC", exchange: "AMEX", last_sale: 11.77 },
                 { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                 { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                 { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                 { symbol: "BKPQ", exchange: "OTCBB", last_sale: 0.0786 }] 
            )
        "#, "true");

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