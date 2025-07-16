#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Platform Packages module
////////////////////////////////////////////////////////////////////

use crate::byte_code_compiler::ByteCodeCompiler;
use crate::compiler::Compiler;
use crate::data_types::DataType;
use crate::data_types::DataType::*;
use crate::dataframe::Dataframe;
use crate::dataframe::Dataframe::{DiskTable, EventSource, ModelTable, TableFn};
use crate::errors::throw;
use crate::errors::Errors::*;
use crate::errors::TypeMismatchErrors::*;
use crate::expression::Expression::{CodeBlock, FunctionCall, Literal, Multiply, StructureExpression};
use crate::file_row_collection::FileRowCollection;
use crate::journaling::{EventSourceRowCollection, Journaling, TableFunction};
use crate::machine::Machine;
use crate::model_row_collection::ModelRowCollection;
use crate::namespaces::Namespace;
use crate::number_kind::NumberKind::*;
use crate::numbers::Numbers::*;
use crate::object_config::{HashIndexConfig, ObjectConfig};
use crate::packages::PackageOps::{Arrays, Cal};
use crate::parameter::Parameter;
use crate::row_collection::RowCollection;
use crate::sequences::Sequences::{TheArray, TheDataframe, TheRange, TheTuple};
use crate::sequences::{range_diff, Array, Sequence};
use crate::server_engine;
use crate::sprintf::StringPrinter;
use crate::structures::Structures::{Hard, Soft};
use crate::structures::{Row, SoftStructure, Structure, Structures};
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::*;
use crate::utils::*;
use crate::web_engine::ws_commander;
use chrono::{Datelike, Local, MappedLocalTime, NaiveDate, NaiveDateTime, TimeZone, Timelike, Weekday};
use crossterm::style::Stylize;
use num_traits::ToPrimitive;
use rand::prelude::ThreadRng;
use rand::{thread_rng, Rng, RngCore};
use serde::{Deserialize, Serialize};
use shared_lib::cnv_error;
use std::collections::HashMap;
use std::fs::File;
use std::io::{stderr, stdout, Read, Write};
use std::ops::Deref;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::time::UNIX_EPOCH;
use std::{env, fs};
use uuid::Uuid;

// platform version constants
pub const MAJOR_VERSION: u8 = 1;
pub const MINOR_VERSION: u8 = 47;
pub const VERSION: &str = "0.47";

// duration unit constants
const MILLIS: i64 = 1;
const SECONDS: i64 = 1000 * MILLIS;
const MINUTES: i64 = 60 * SECONDS;
const HOURS: i64 = 60 * MINUTES;
const DAYS: i64 = 24 * HOURS;

/// Represents an Oxide Platform Package
pub trait Package {
    fn get_name(&self) -> String;
    fn get_package_name(&self) -> String;
    fn get_description(&self) -> String;
    fn get_examples(&self) -> Vec<String>;
    fn get_parameter_types(&self) -> Vec<DataType>;
    fn get_return_type(&self) -> DataType;
    fn evaluate(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)>;
}

/// Represents an enumeration of Oxide Platform Package Functions
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum PackageOps {
    Agg(AggPkg),
    Arrays(ArraysPkg),
    Cal(CalPkg),
    Durations(DurationsPkg),
    Io(IoPkg),
    Math(MathPkg),
    Nsd(NsdPkg),
    Os(OsPkg),
    Oxide(OxidePkg),
    Strings(StringsPkg),
    Tools(ToolsPkg),
    Utils(UtilsPkg),
    Www(WwwPkg),
}

impl PackageOps {
    /////////////////////////////////////////////////////////
    //      STATIC METHODS
    /////////////////////////////////////////////////////////

    /// Builds a mapping of the package name to function vector
    pub fn build_packages() -> HashMap<String, Vec<PackageOps>> {
        Self::get_contents()
            .iter()
            .fold(HashMap::new(), |mut hm, op| {
                hm.entry(op.get_package_name())
                    .or_insert_with(Vec::new)
                    .push(op.to_owned());
                hm
            })
    }

    pub fn decode(bytes: Vec<u8>) -> std::io::Result<PackageOps> {
        ByteCodeCompiler::unwrap_as_result(bincode::deserialize(bytes.as_slice()))
    }

    pub fn find_function(package: &str, name: &str) -> Option<PackageOps> {
        Self::get_contents()
            .iter()
            .find(|pf| pf.get_package_name() == package && pf.get_name() == name)
            .map(|pf| pf.clone())
    }

    pub fn get_contents() -> Vec<PackageOps> {
        let mut contents = Vec::with_capacity(150);
        contents.extend(AggPkg::get_contents());
        contents.extend(IoPkg::get_contents());
        contents.extend(MathPkg::get_contents());
        contents.extend(NsdPkg::get_contents());
        contents.extend(OsPkg::get_contents());
        contents.extend(OxidePkg::get_contents());
        contents.extend(UtilsPkg::get_contents());
        contents.extend(WwwPkg::get_contents());
        contents
    }

    pub fn get_all_packages() -> Vec<PackageOps> {
        let mut contents = Vec::with_capacity(150);
        contents.extend(AggPkg::get_contents());
        contents.extend(ArraysPkg::get_contents());
        contents.extend(CalPkg::get_contents());
        contents.extend(DurationsPkg::get_contents());
        contents.extend(IoPkg::get_contents());
        contents.extend(MathPkg::get_contents());
        contents.extend(NsdPkg::get_contents());
        contents.extend(OsPkg::get_contents());
        contents.extend(OxidePkg::get_contents());
        contents.extend(StringsPkg::get_contents());
        contents.extend(ToolsPkg::get_contents());
        contents.extend(UtilsPkg::get_contents());
        contents.extend(WwwPkg::get_contents());
        contents
    }

    /////////////////////////////////////////////////////////
    //      INSTANCE METHODS
    /////////////////////////////////////////////////////////

    pub fn encode(&self) -> std::io::Result<Vec<u8>> {
        ByteCodeCompiler::unwrap_as_result(bincode::serialize(self))
    }

    pub fn get_package(&self) -> Box<dyn Package> {
        match self {
            PackageOps::Agg(pkg) => Box::new(pkg.clone()),
            PackageOps::Arrays(pkg) => Box::new(pkg.clone()),
            PackageOps::Cal(pkg) => Box::new(pkg.clone()),
            PackageOps::Durations(pkg) => Box::new(pkg.clone()),
            PackageOps::Io(pkg) => Box::new(pkg.clone()),
            PackageOps::Math(pkg) => Box::new(pkg.clone()),
            PackageOps::Nsd(pkg) => Box::new(pkg.clone()),
            PackageOps::Os(pkg) => Box::new(pkg.clone()),
            PackageOps::Oxide(pkg) => Box::new(pkg.clone()),
            PackageOps::Strings(pkg) => Box::new(pkg.clone()),
            PackageOps::Tools(pkg) => Box::new(pkg.clone()),
            PackageOps::Utils(pkg) => Box::new(pkg.clone()),
            PackageOps::Www(pkg) => Box::new(pkg.clone()),
        }
    }

    pub fn get_parameters(&self) -> Vec<Parameter> {
        let names = match self.get_parameter_types()
            .iter()
            .map(|dt| match dt {
                FixedSizeType(data_type, _) => data_type.deref().clone(),
                _ => dt.clone()
            })
            .collect::<Vec<_>>()
            .as_slice() {
            [BooleanType] => vec!['b'],
            [NumberType(..)] => vec!['n'],
            [StringType] => vec!['s'],
            [StringType, NumberType(..)] => vec!['s', 'n'],
            [TableType(..)] => vec!['t'],
            [TableType(..), NumberType(..)] => vec!['t', 'n'],
            [StringType, NumberType(..), NumberType(..)] => vec!['s', 'm', 'n'],
            params => params
                .iter()
                .enumerate()
                .map(|(n, _)| (n as u8 + b'a') as char)
                .collect(),
        };

        names
            .iter()
            .zip(self.get_parameter_types().iter())
            .enumerate()
            .map(|(n, (name, dt))| Parameter::new(name.to_string(), dt.clone()))
            .collect()
    }

    pub fn get_type(&self) -> DataType {
        PlatformOpsType(self.clone())
    }

    pub fn to_code(&self) -> String {
        self.to_code_with_params(&self.get_parameters())
    }

    pub fn to_code_with_params(&self, parameters: &Vec<Parameter>) -> String {
        let pkg = self.get_package_name();
        let name = self.get_name();
        let params = parameters
            .iter()
            .map(|p| p.to_code())
            .collect::<Vec<_>>()
            .join(", ");
        format!("{pkg}::{name}({params})")
    }

    fn adapter_pf_fn1<F>(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
        f: F,
    ) -> std::io::Result<(Machine, TypedValue)>
    where
        F: Fn(Machine, &TypedValue, &PackageOps) -> std::io::Result<(Machine, TypedValue)>,
    {
        match args.as_slice() {
            [a] => f(ms, a, self),
            args => throw(TypeMismatch(ArgumentsMismatched(1, args.len()))),
        }
    }

    /// Applies the given function to every item in items
    fn apply_fn_over_vec(
        ms: Machine,
        items: &Vec<TypedValue>,
        function: &TypedValue,
        logic: fn(TypedValue, TypedValue) -> std::io::Result<Option<TypedValue>>,
        complete: fn(Vec<TypedValue>) -> TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let mut new_items = vec![];
        // apply the function over all items in the array
        for item in items.iter().cloned() {
            // apply the function on the current item
            let (_, result) = ms.evaluate(&FunctionCall {
                fx: Literal(function.clone()).into(),
                args: vec![Literal(item.clone())],
            })?;
            // if an outcome was produced, capture it
            if let Some(outcome) = logic(item, result)? {
                new_items.push(outcome)
            }
        }
        Ok((ms, complete(new_items)))
    }

    fn apply_fn_over_table(
        ms: Machine,
        src: &Dataframe,
        function: &TypedValue,
        logic: fn(TypedValue, TypedValue) -> std::io::Result<Option<TypedValue>>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        // cache the source columns and column names
        let src_columns = src.get_columns();
        let src_column_names = src_columns
            .iter()
            .map(|col| col.get_name().to_string())
            .collect::<Vec<_>>();

        // apply the function over all rows of the table
        let (mut new_arr, mut dest_params, mut is_table) = (vec![], vec![], true);
        for src_row in src.get_rows() {
            // build the typed-value version of the row
            let src_tuple_val = src_column_names
                .iter()
                .zip(src_row.get_values())
                .map(|(key, value)| (key.to_string(), value))
                .collect::<Vec<_>>();
            // build the expression variant of the row
            let src_tuple_expr = src_tuple_val
                .iter()
                .map(|(key, value)| (key.to_string(), Literal(value.clone())))
                .collect::<Vec<_>>();
            // apply the function on the current row
            let ms1 = ms.with_row(src_columns, &src_row);
            let (_, result) = ms1.evaluate(&FunctionCall {
                fx: Literal(function.clone()).into(),
                args: vec![StructureExpression(src_tuple_expr)],
            })?;
            // if an outcome was produced, capture it
            if let Some(outcome) = logic(
                Structured(Soft(SoftStructure::from_tuples(src_tuple_val))),
                result,
            )? {
                let outcome_params = match &outcome {
                    Structured(s) => s.get_parameters(),
                    TableValue(df) => df.get_parameters(),
                    _ => {
                        is_table = false;
                        vec![]
                    }
                };
                dest_params = Parameter::merge_parameters(dest_params, outcome_params);
                new_arr.push(outcome)
            }
        }

        // return a table (preferably) or an array
        if is_table {
            Ok((ms, TableValue(ModelTable({
                let mut dest_rows = vec![];
                for item in new_arr {
                    let transformed_rows = match item {
                        Structured(s) => vec![Row::new(0, s.get_values())],
                        TableValue(df) => df.get_rows(),
                        z => return throw(TypeMismatch(StructExpected(z.to_code())))
                    };
                    dest_rows.extend(transformed_rows)
                }
                let mut dest = ModelRowCollection::from_parameters(&dest_params);
                dest.append_rows(dest_rows)?;
                dest
            })),
            ))
        } else {
            Ok((ms, ArrayValue(Array::from(new_arr))))
        }
    }

    fn open_namespace(ns: &Namespace) -> TypedValue {
        match FileRowCollection::open(ns) {
            Err(err) => ErrorValue(Exact(err.to_string())),
            Ok(frc) => {
                let columns = frc.get_columns();
                match frc.read_active_rows() {
                    Err(err) => ErrorValue(Exact(err.to_string())),
                    Ok(rows) => TableValue(ModelTable(ModelRowCollection::from_columns_and_rows(
                        columns, &rows,
                    ))),
                }
            }
        }
    }
}

impl Package for PackageOps {
    fn get_name(&self) -> String {
        self.get_package().get_name()
    }

    fn get_package_name(&self) -> String {
        self.get_package().get_package_name()
    }

    fn get_description(&self) -> String {
        self.get_package().get_description()
    }

    fn get_examples(&self) -> Vec<String> {
        // trim all example code
        self.get_package().get_examples()
            .iter()
            .map(|s| s.trim().to_string())
            .collect::<Vec<_>>()
    }

    fn get_parameter_types(&self) -> Vec<DataType> {
        self.get_package().get_parameter_types()
    }

    fn get_return_type(&self) -> DataType {
        self.get_package().get_return_type()
    }

    /// Evaluates the platform function
    fn evaluate(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        self.get_package().evaluate(ms, args)
    }
}

/// Represents a Data Format
pub enum DataFormats {
    CSV,
    JSON,
}

impl DataFormats {
    pub fn get_name(&self) -> String {
        (match self {
            DataFormats::CSV => "CSV",
            DataFormats::JSON => "JSON",
        }).to_string()
    }
}

/// Aggregate package
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum AggPkg {
    Avg,
    Count,
    Max,
    Min,
    Sum,
}

impl AggPkg {
    /// stateful reduce function
    fn agg_reduce_stateful_fn<F>(
        label: &str,
        ms: Machine,
        value: &TypedValue,
        f: F,
    ) -> std::io::Result<(Machine, TypedValue)>
    where
        F: Fn(TypedValue, TypedValue) -> TypedValue,
    {
        let ms0 = match ms.get(label) {
            None => ms.with_variable(label, f(Undefined, value.clone())),
            Some(prev_value) => ms.with_variable(label, f(prev_value, value.clone())),
        };
        let result = ms0.get_or_else(label, || Null);
        Ok((ms0, result))
    }

    /// aggregate function: returns the average of values in a column
    fn do_agg_avg(
        ms: Machine,
        value: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        // track the sum
        let (ms1, sum) = Self::agg_reduce_stateful_fn("$avg_sum", ms, value, |v0, v1| {
            match (v0, v1) {
                (Number(n0), Number(n1)) => Number(n0 + n1),
                _ => value.clone()
            }
        })?;
        // track the count
        let (ms2, count) = Self::agg_reduce_stateful_fn("$avg_count", ms1, value, |v0, v1| {
            let n1 = I64Value(if v1 == Null || v1 == Undefined { 0 } else { 1 });
            match (v0, v1) {
                (Number(count), _) => Number(count + n1),
                _ => Number(n1)
            }
        })?;
        // compute the average
        let result = match (sum, count) {
            (Number(a), Number(b)) => 
                if b.is_effectively_zero() { Null } else { Number(a / b) }
            _ => value.clone()
        };
        Ok((ms2, result))
    }

    /// aggregate function: returns the count of non-null values in a column
    fn do_agg_count(
        ms: Machine,
        value: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        Self::agg_reduce_stateful_fn("$count", ms, value, |v0, v1| {
            let n1 = I64Value(if v1 == Null || v1 == Undefined { 0 } else { 1 });
            match (v0, v1) {
                (Number(count), _) => Number(count + n1),
                _ => Number(n1)
            }
        })
    }
    
    /// aggregate function: returns the maximum value (highest) in a column
    fn do_agg_max(
        ms: Machine,
        value: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        Self::agg_reduce_stateful_fn("$max", ms, value, |v0, v1| {
            match (v0, v1) {
                (DateTimeValue(n0), DateTimeValue(n1)) =>
                    if n0 > n1 { DateTimeValue(n0) } else { DateTimeValue(n1) },
                (Number(n0), Number(n1)) =>
                    if n0 > n1 { Number(n0) } else { Number(n1) },
                (UUIDValue(n0), UUIDValue(n1)) =>
                    if n0 > n1 { UUIDValue(n0) } else { UUIDValue(n1) },
                _ => value.clone()
            }
        })
    }

    /// aggregate function: returns the minimum value (lowest) in a column
    fn do_agg_min(
        ms: Machine,
        value: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        Self::agg_reduce_stateful_fn("$min", ms, value, |v0, v1| {
            match (v0, v1) {
                (DateTimeValue(n0), DateTimeValue(n1)) =>
                    if n0 < n1 { DateTimeValue(n0) } else { DateTimeValue(n1) }
                (Number(n0), Number(n1)) =>
                    if n0 < n1 { Number(n0) } else { Number(n1) }
                (UUIDValue(n0), UUIDValue(n1)) =>
                    if n0 < n1 { UUIDValue(n0) } else { UUIDValue(n1) }
                _ => value.clone()
            }
        })
    }

    /// aggregate function: sum value
    fn do_agg_sum(
        ms: Machine,
        value: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        Self::agg_reduce_stateful_fn("$sum", ms, value, |v0, v1| {
            match (v0, v1) {
                (Number(n0), Number(n1)) => Number(n0 + n1),
                _ => value.clone()
            }
        })
    }

    pub fn get_contents() -> Vec<PackageOps> {
        vec![
            PackageOps::Agg(AggPkg::Avg),
            PackageOps::Agg(AggPkg::Count),
            PackageOps::Agg(AggPkg::Max),
            PackageOps::Agg(AggPkg::Min),
            PackageOps::Agg(AggPkg::Sum),
        ]
    }
}

impl Package for AggPkg {
    fn get_name(&self) -> String {
        (match self {
            AggPkg::Avg => "avg",
            AggPkg::Count => "count",
            AggPkg::Max => "max",
            AggPkg::Min => "min",
            AggPkg::Sum => "sum",
        }).into()
    }

    fn get_package_name(&self) -> String {
        "agg".into()
    }

    fn get_description(&self) -> String {
        (match self {
            AggPkg::Avg => "returns the average of values in a column",
            AggPkg::Count => "returns the counts of rows or non-null fields",
            AggPkg::Max => "returns the maximum value of a collection of fields",
            AggPkg::Min => "returns the minimum value of a collection of fields",
            AggPkg::Sum => "returns the sum of a collection of fields",
        }).to_string()
    }

    fn get_examples(&self) -> Vec<String> {
        match self {
            AggPkg::Avg => vec![
                strip_margin(r#"
                    |use agg
                    |select exchange, avg_sale: avg(last_sale)
                    |from
                    |    |--------------------------------|
                    |    | symbol | exchange  | last_sale |
                    |    |--------------------------------|
                    |    | GIF    | NYSE      | 11.77     |
                    |    | TRX    | NASDAQ    | 32.97     |
                    |    | RLP    | NYSE      | 23.66     |
                    |    | GTO    | NASDAQ    | 51.23     |
                    |    | BST    | NASDAQ    | 214.88    |
                    |    |--------------------------------|
                    |group_by exchange
                    "#, '|')
            ],
            AggPkg::Count => vec![
                strip_margin(r#"
                    |use agg
                    |select exchange, qty: count(last_sale)
                    |from
                    |    |--------------------------------|
                    |    | symbol | exchange  | last_sale |
                    |    |--------------------------------|
                    |    | GIF    | NYSE      | 11.77     |
                    |    | TRX    | NASDAQ    | 32.97     |
                    |    | RLP    | NYSE      | 23.66     |
                    |    | GTO    | NASDAQ    | 51.23     |
                    |    | BST    | NASDAQ    | 214.88    |
                    |    |--------------------------------|
                    |group_by exchange                    
                    "#, '|')
            ],
            AggPkg::Max => vec![
                strip_margin(r#"
                    |use agg
                    |select exchange, max_sale: max(last_sale)
                    |from
                    |    |--------------------------------|
                    |    | symbol | exchange  | last_sale |
                    |    |--------------------------------|
                    |    | GIF    | NYSE      | 11.77     |
                    |    | TRX    | NASDAQ    | 32.97     |
                    |    | RLP    | NYSE      | 23.66     |
                    |    | GTO    | NASDAQ    | 51.23     |
                    |    | BST    | NASDAQ    | 214.88    |
                    |    |--------------------------------|
                    |group_by exchange                    
                    "#, '|')
            ],
            AggPkg::Min => vec![
                strip_margin(r#"
                    |use agg
                    |select exchange, min_sale: min(last_sale)
                    |from
                    |    |--------------------------------|
                    |    | symbol | exchange  | last_sale |
                    |    |--------------------------------|
                    |    | GIF    | NYSE      | 11.77     |
                    |    | TRX    | NASDAQ    | 32.97     |
                    |    | RLP    | NYSE      | 23.66     |
                    |    | GTO    | NASDAQ    | 51.23     |
                    |    | BST    | NASDAQ    | 214.88    |
                    |    |--------------------------------|
                    |group_by exchange                    
                    "#, '|')
            ],
            AggPkg::Sum => vec![
                strip_margin(r#"
                    |use agg
                    |select exchange, total_sale: sum(last_sale)
                    |from
                    |    |--------------------------------|
                    |    | symbol | exchange  | last_sale |
                    |    |--------------------------------|
                    |    | GIF    | NYSE      | 11.77     |
                    |    | TRX    | NASDAQ    | 32.97     |
                    |    | RLP    | NYSE      | 23.66     |
                    |    | GTO    | NASDAQ    | 51.23     |
                    |    | BST    | NASDAQ    | 214.88    |
                    |    |--------------------------------|
                    |group_by exchange                    
                    "#, '|')
            ]
        }
    }

    fn get_parameter_types(&self) -> Vec<DataType> {
        match self {
            AggPkg::Avg => vec![
                RuntimeResolvedType
            ],
            AggPkg::Count => vec![
                RuntimeResolvedType
            ],
            AggPkg::Max | AggPkg::Min => vec![
                RuntimeResolvedType
            ],
            AggPkg::Sum => vec![
                RuntimeResolvedType
            ],
        }
    }

    fn get_return_type(&self) -> DataType {
        match self {
            AggPkg::Avg | AggPkg::Max | AggPkg::Min => NumberType(F64Kind),
            AggPkg::Count => NumberType(I64Kind),
            AggPkg::Sum => NumberType(F64Kind),
        }
    }

    fn evaluate(
        &self, 
        ms: Machine, 
        args: Vec<TypedValue>
    ) -> std::io::Result<(Machine, TypedValue)> {
        match self {
            AggPkg::Avg => extract_value_fn1(ms, args, Self::do_agg_avg),
            AggPkg::Count => extract_value_fn1(ms, args, Self::do_agg_count),
            AggPkg::Max => extract_value_fn1(ms, args, Self::do_agg_max),
            AggPkg::Min => extract_value_fn1(ms, args, Self::do_agg_min),
            AggPkg::Sum => extract_value_fn1(ms, args, Self::do_agg_sum),
        }
    }
}

/// Arrays package
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum ArraysPkg {
    Filter,
    IsEmpty,
    Len,
    Map,
    Pop,
    Push,
    Reduce,
    Reverse,
    ToArray,
}

impl ArraysPkg {
    fn do_arrays_pop(
        ms: Machine,
        value: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let array = pull_array(value)?;
        let (new_array, item) = array.pop();
        let result = TupleValue(vec![
            ArrayValue(new_array),
            item.unwrap_or(Null),
        ]);
        Ok((ms, result))
    }

    fn do_arrays_reduce(
        ms: Machine,
        items: &TypedValue,
        initial: &TypedValue,
        function: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match function {
            Function { .. } =>
                match items.to_sequence()? {
                    TheArray(array) => {
                        let mut result = initial.clone();
                        for item in array.get_values() {
                            // apply the function on the current item
                            let (_, result1) = ms.evaluate(&FunctionCall {
                                fx: Literal(function.clone()).into(),
                                args: vec![Literal(result), Literal(item)],
                            })?;
                            result = result1
                        }
                        Ok((ms, result))
                    }
                    TheDataframe(..) => Self::do_arrays_reduce(ms, &items.to_array()?, initial, function),
                    TheRange(..) => Self::do_arrays_reduce(ms, &items.to_array()?, initial, function),
                    TheTuple(..) => Self::do_arrays_reduce(ms, &items.to_array()?, initial, function),
                },
            z => throw(TypeMismatch(FunctionExpected(z.to_code()))),
        }
    }

    fn do_arrays_reverse(
        ms: Machine,
        items: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let result = match items.clone() {
            ByteStringValue(mut bytes) => ByteStringValue({ bytes.reverse(); bytes }),
            StringValue(string) => StringValue(string.chars().rev().collect()),
            _ => match items.to_sequence()? {
                TheArray(a) => ArrayValue(a.rev()),
                TheDataframe(df) => df.reverse_table_value()?,
                TheRange(..) => Self::do_arrays_reverse(ms.clone(), &items.to_array()?)?.1,
                TheTuple(tuple) => TupleValue(tuple.iter().cloned().rev().collect()),
            }
        };
        Ok((ms, result))
    }

    pub fn get_contents() -> Vec<PackageOps> {
        vec![
            PackageOps::Arrays(ArraysPkg::Filter),
            PackageOps::Arrays(ArraysPkg::IsEmpty),
            PackageOps::Arrays(ArraysPkg::Len),
            PackageOps::Arrays(ArraysPkg::Map),
            PackageOps::Arrays(ArraysPkg::Pop),
            PackageOps::Arrays(ArraysPkg::Push),
            PackageOps::Arrays(ArraysPkg::Reduce),
            PackageOps::Arrays(ArraysPkg::Reverse),
            PackageOps::Arrays(ArraysPkg::ToArray),
        ]
    }
}

impl Package for ArraysPkg {
    fn get_name(&self) -> String {
        match self {
            ArraysPkg::Filter => "filter".into(),
            ArraysPkg::IsEmpty => "is_empty".into(),
            ArraysPkg::Len => "len".into(),
            ArraysPkg::Map => "map".into(),
            ArraysPkg::Pop => "pop".into(),
            ArraysPkg::Push => "push".into(),
            ArraysPkg::Reduce => "reduce".into(),
            ArraysPkg::Reverse => "reverse".into(),
            ArraysPkg::ToArray => "to_array".into(),
        }
    }

    fn get_package_name(&self) -> String {
        "arrays".into()
    }

    fn get_description(&self) -> String {
        match self {
            ArraysPkg::Filter => "Filters an array based on a function".into(),
            ArraysPkg::IsEmpty => "Returns true if the array is empty".into(),
            ArraysPkg::Len => "Returns the length of an array".into(),
            ArraysPkg::Map => "Transform an array based on a function".into(),
            ArraysPkg::Pop => "Removes and returns a value or object from an array".into(),
            ArraysPkg::Push => "Appends a value or object to an array".into(),
            ArraysPkg::Reduce => "Reduces an array to a single value".into(),
            ArraysPkg::Reverse => "Returns a reverse copy of an array".into(),
            ArraysPkg::ToArray => "Converts a collection into an array".into(),
        }
    }

    fn get_examples(&self) -> Vec<String> {
        match self {
            ArraysPkg::Filter => vec![
                strip_margin(r#"
                    |1..7::filter(n -> (n % 2) == 0)
               "#, '|')
            ],
            ArraysPkg::IsEmpty => vec![
                strip_margin(r#"
                    |[1, 3, 5]::is_empty
               "#, '|'),
                strip_margin(r#"
                    |[]::is_empty
               "#, '|')
            ],
            ArraysPkg::Len => vec![
                strip_margin(r#"
                    |[1, 5, 2, 4, 6, 0]::len()
               "#, '|')
            ],
            ArraysPkg::Map => vec![
                strip_margin(r#"
                    |[1, 2, 3]::map(n -> n * 2)
               "#, '|')
            ],
            ArraysPkg::Pop => vec![
                strip_margin(r#"
                    |stocks = []
                    |stocks::push({ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 })
                    |stocks::push({ symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 })
                    |stocks
                "#, '|')
            ],
            ArraysPkg::Push => vec![
                strip_margin(r#"
                    |stocks = [
                    |    { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                    |    { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                    |    { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }
                    |]
                    |stocks::push({ symbol: "DEX", exchange: "OTC_BB", last_sale: 0.0086 })
                    |stocks::to_table()
                "#, '|')
            ],
            ArraysPkg::Reduce => vec![
                strip_margin(r#"
                    |1..=5::reduce(0, (a, b) -> a + b)
                "#, '|'),
                strip_margin(r#"
                    |numbers = [1, 2, 3, 4, 5]
                    |numbers::reduce(0, (a, b) -> a + b)
                "#,
                '|')
            ],
            ArraysPkg::Reverse => vec![
                strip_margin(r#"
                    |['cat', 'dog', 'ferret', 'mouse']::reverse()
                "#, '|')
            ],
            ArraysPkg::ToArray => vec![
                strip_margin(r#"
                    ||--------------------------------------|
                    || symbol | exchange | last_sale | rank |
                    ||--------------------------------------|
                    || BOOM   | NYSE     | 113.76    | 1    |
                    || ABC    | AMEX     | 24.98     | 2    |
                    || JET    | NASDAQ   | 64.24     | 3    |
                    ||--------------------------------------| 
                    |::to_array
                "#, '|')
            ],
        }
    }

    fn get_parameter_types(&self) -> Vec<DataType> {
        match self {
            ArraysPkg::Filter => vec![
                ArrayType(RuntimeResolvedType.into()),
                FunctionType(
                    vec![Parameter::new("item", RuntimeResolvedType)],
                    BooleanType.into(),
                ),
            ],
            ArraysPkg::IsEmpty => vec![
                ArrayType(RuntimeResolvedType.into()),
            ],
            ArraysPkg::Len => vec![
                ArrayType(RuntimeResolvedType.into())
            ],
            ArraysPkg::Map => vec![
                ArrayType(RuntimeResolvedType.into()),
                FunctionType(
                    vec![Parameter::new("item", RuntimeResolvedType)],
                    RuntimeResolvedType.into(),
                ),
            ],
            ArraysPkg::Pop | ArraysPkg::Reverse => vec![
                ArrayType(RuntimeResolvedType.into())
            ],
            ArraysPkg::Push => vec![
                ArrayType(RuntimeResolvedType.into()), RuntimeResolvedType
            ],
            ArraysPkg::Reduce => vec![
                ArrayType(RuntimeResolvedType.into()), RuntimeResolvedType, FunctionType(vec![
                    Parameter::new("a", RuntimeResolvedType),
                    Parameter::new("b", RuntimeResolvedType),
                ], RuntimeResolvedType.into())
            ],
            ArraysPkg::ToArray => vec![RuntimeResolvedType],
        }
    }

    fn get_return_type(&self) -> DataType {
        match self {
            // Array
            ArraysPkg::Filter
            | ArraysPkg::Map
            | ArraysPkg::Reverse
            | ArraysPkg::ToArray => ArrayType(RuntimeResolvedType.into()),
            // Number
            ArraysPkg::Len => NumberType(I64Kind),
            // Boolean
            ArraysPkg::IsEmpty
            | ArraysPkg::Pop | ArraysPkg::Push => BooleanType,
            // UnresolvedType
            ArraysPkg::Reduce => RuntimeResolvedType,
        }
    }

    fn evaluate(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match self {
            ArraysPkg::Filter => extract_value_fn2(ms, args, ToolsPkg::do_tools_filter),
            ArraysPkg::IsEmpty => extract_array_fn1(ms, args, |a| Boolean(a.is_empty())),
            ArraysPkg::Len => extract_array_fn1(ms, args, |a| Number(I64Value(a.len() as i64))),
            ArraysPkg::Map => extract_value_fn2(ms, args, ToolsPkg::do_tools_map),
            ArraysPkg::Pop => extract_value_fn1(ms, args, ArraysPkg::do_arrays_pop),
            ArraysPkg::Push => ToolsPkg::do_tools_push(ms, args),
            ArraysPkg::Reduce => extract_value_fn3(ms, args, Self::do_arrays_reduce),
            ArraysPkg::Reverse => extract_value_fn1(ms, args, ArraysPkg::do_arrays_reverse),
            ArraysPkg::ToArray => extract_array_fn1(ms, args, |a| ArrayValue(a)),
        }
    }
}

/// Calender package
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum CalPkg {
    DateDay,
    DateHour12,
    DateHour24,
    DateMinute,
    DateMonth,
    DateSecond,
    DateYear,
    IsLeapYear,
    IsWeekday,
    IsWeekend,
    Minus,
    Plus,
    ToMillis,
}

impl CalPkg {
    fn adapter_pf_fn1<F>(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
        f: F,
    ) -> std::io::Result<(Machine, TypedValue)>
    where
        F: Fn(Machine, &TypedValue, &CalPkg) -> std::io::Result<(Machine, TypedValue)>,
    {
        match args.as_slice() {
            [a] => f(ms, a, self),
            args => throw(TypeMismatch(ArgumentsMismatched(1, args.len()))),
        }
    }

    fn do_cal_date_part(
        ms: Machine,
        value: &TypedValue,
        plat: &CalPkg,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match value {
            DateTimeValue(epoch_millis) => {
                let datetime = {
                    match Local.timestamp_millis_opt(*epoch_millis) {
                        MappedLocalTime::Single(dt) => dt,
                        _ => return throw(Exact(format!("Incorrect timestamp_millis {}", epoch_millis))),
                    }
                };
                match plat {
                    CalPkg::DateDay => Ok((ms, Number(I64Value(datetime.day() as i64)))),
                    CalPkg::DateHour12 => Ok((ms, Number(I64Value(datetime.hour12().1 as i64)))),
                    CalPkg::DateHour24 => Ok((ms, Number(I64Value(datetime.hour() as i64)))),
                    CalPkg::DateMinute => Ok((ms, Number(I64Value(datetime.minute() as i64)))),
                    CalPkg::DateMonth => Ok((ms, Number(I64Value(datetime.month() as i64)))),
                    CalPkg::DateSecond => Ok((ms, Number(I64Value(datetime.second() as i64)))),
                    CalPkg::DateYear => Ok((ms, Number(I64Value(datetime.year() as i64)))),
                    CalPkg::IsLeapYear => Self::is_leapyear(ms, value),
                    CalPkg::IsWeekday => Ok((ms, Boolean(Self::is_weekday(*epoch_millis)?))),
                    CalPkg::IsWeekend => Ok((ms, Boolean(Self::is_weekend(*epoch_millis)?))),
                    pf => throw(PlatformOpError(Cal(pf.to_owned()))),
                }
            }
            other => throw(TypeMismatch(DateExpected(other.to_code()))),
        }
    }

    fn do_cal_date_minus(
        ms: Machine,
        date: &TypedValue,
        duration: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        Ok((ms, DateTimeValue(date.to_i64() - duration.to_i64())))
    }

    fn do_cal_date_plus(
        ms: Machine,
        date: &TypedValue,
        duration: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        Ok((ms, DateTimeValue(date.to_i64() + duration.to_i64())))
    }
    
    fn do_cal_to_millis(
        ms: Machine,
        date: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        Ok((ms, Number(I64Value(date.to_i64()))))
    }

    pub fn get_contents() -> Vec<PackageOps> {
        vec![
            PackageOps::Cal(CalPkg::DateDay),
            PackageOps::Cal(CalPkg::DateHour12),
            PackageOps::Cal(CalPkg::DateHour24),
            PackageOps::Cal(CalPkg::DateMinute),
            PackageOps::Cal(CalPkg::DateMonth),
            PackageOps::Cal(CalPkg::DateSecond),
            PackageOps::Cal(CalPkg::DateYear),
            PackageOps::Cal(CalPkg::IsLeapYear),
            PackageOps::Cal(CalPkg::IsWeekday),
            PackageOps::Cal(CalPkg::IsWeekend),
            PackageOps::Cal(CalPkg::Minus),
            PackageOps::Cal(CalPkg::Plus),
            PackageOps::Cal(CalPkg::ToMillis),
        ]
    }

    pub fn is_leapyear(ms: Machine, value: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        let state = match value {
            DateTimeValue(millis) => match millis_to_naive_date(*millis) {
                Some(date) => date.leap_year(),
                None => return throw(Exact("Invalid date".into()))
            }
            Number(year) => is_leap_year(year.to_i64()),
            z => return throw(TypeMismatch(DateExpected(z.to_code())))
        };
        Ok((ms, Boolean(state)))
    }

    pub fn is_weekday(epoch_millis: i64) -> std::io::Result<bool> {
        Self::is_weekend(epoch_millis).map(|is_weekend| !is_weekend)
    }
    
    pub fn is_weekend(epoch_millis: i64) -> std::io::Result<bool> {
        let date = Self::naive_date_from_epoch_millis(epoch_millis)?;
        Ok(matches!(date.weekday(), Weekday::Sat | Weekday::Sun))
    }

    fn naive_date_from_epoch_millis(epoch_millis: i64) -> std::io::Result<NaiveDate> {
        let secs = epoch_millis / 1000;
        let nsecs = ((epoch_millis % 1000) * 1_000_000) as u32;
        match NaiveDateTime::from_timestamp_opt(secs, nsecs) {
            Some(datetime) => Ok(datetime.date()),
            None => throw(Exact(format!("Incorrect timestamp_millis {}", epoch_millis))),
        }
    }
}

impl Package for CalPkg {
    fn get_name(&self) -> String {
        (match self {
            CalPkg::DateDay => "day",
            CalPkg::DateHour12 => "hour12",
            CalPkg::DateHour24 => "hour24",
            CalPkg::DateMinute => "minute",
            CalPkg::DateMonth => "month",
            CalPkg::DateSecond => "second",
            CalPkg::DateYear => "year",
            CalPkg::Minus => "minus",
            CalPkg::IsLeapYear => "is_leap_year",
            CalPkg::IsWeekday => "is_weekday",
            CalPkg::IsWeekend => "is_weekend",
            CalPkg::Plus => "plus",
            CalPkg::ToMillis => "to_millis",
        }).into()
    }

    fn get_package_name(&self) -> String {
        "cal".into()
    }

    fn get_description(&self) -> String {
        (match self {
            CalPkg::DateDay => "Returns the day of the month of a Date",
            CalPkg::DateHour12 => "Returns the hour of the day of a Date",
            CalPkg::DateHour24 => "Returns the hour (military time) of the day of a Date",
            CalPkg::DateMinute => "Returns the minute of the hour of a Date",
            CalPkg::DateMonth => "Returns the month of the year of a Date",
            CalPkg::DateSecond => "Returns the seconds of the minute of a Date",
            CalPkg::DateYear => "Returns the year of a Date",
            CalPkg::IsLeapYear => "Returns true if the year of the date falls on a leap year",
            CalPkg::IsWeekday => "Returns true if the date falls on a weekday",
            CalPkg::IsWeekend => "Returns true if the date falls on a weekend",
            CalPkg::Minus => "Subtracts a duration from a date",
            CalPkg::Plus => "Adds a duration to a date",
            CalPkg::ToMillis => "Returns the time in milliseconds of a date",
        }).into()
    }

    fn get_examples(&self) -> Vec<String> {
        match self {
            // cal
            CalPkg::DateDay => vec![
                strip_margin(r#"
                    |2025-07-06T21:59:02.425Z::day
                "#, '|')
            ],
            CalPkg::DateHour12 => vec![
                strip_margin(r#"
                    |2025-07-06T21:59:02.425Z::hour12
                "#, '|')
            ],
            CalPkg::DateHour24 => vec![
                strip_margin(r#"
                    |2025-07-06T21:59:02.425Z::hour24
                "#, '|')
            ],
            CalPkg::DateMinute => vec![
                strip_margin(r#"
                    |2025-07-06T21:59:02.425Z::minute
                "#, '|')
            ],
            CalPkg::DateMonth => vec![
                strip_margin(r#"
                    |2025-07-06T21:59:02.425Z::month
                "#, '|')
            ],
            CalPkg::DateSecond => vec![
                strip_margin(r#"
                    |2025-07-06T21:59:02.425Z::second
                "#, '|')
            ],
            CalPkg::DateYear => vec![
                strip_margin(r#"
                    |2025-07-06T21:59:02.425Z::year
                "#, '|')
            ],
            CalPkg::IsLeapYear => vec![
                "2024-07-06T21:00:29.412Z::is_leapyear".to_string(),
                "2025-07-06T21:00:29.412Z::is_leapyear".to_string(),
                "2024::is_leapyear".to_string(),
                "2025::is_leapyear".to_string()
            ],
            CalPkg::IsWeekday => vec!["2025-07-06T21:00:29.412Z::is_weekday".to_string()],
            CalPkg::IsWeekend => vec!["2025-07-06T21:00:29.412Z::is_weekend".to_string()],
            CalPkg::Minus => vec![
                strip_margin(r#"
                    |2025-07-06T21:59:02.425Z::minus(3::days)
                "#, '|')
            ],
            CalPkg::Plus => vec![
                strip_margin(r#"
                    |2025-07-06T21:59:02.425Z::plus(30::days)
                "#, '|')
            ],
            CalPkg::ToMillis => vec![
                strip_margin(r#"
                    |2025-07-06T21:59:02.425Z::to_millis
                "#, '|')
            ]
        }
    }

    fn get_parameter_types(&self) -> Vec<DataType> {
        match self {
            // ()
            | CalPkg::ToMillis => vec![],
            // DateTime
            CalPkg::DateDay
            | CalPkg::DateHour12
            | CalPkg::DateHour24
            | CalPkg::DateMinute
            | CalPkg::DateMonth
            | CalPkg::DateSecond
            | CalPkg::DateYear
            | CalPkg::IsLeapYear
            | CalPkg::IsWeekday
            | CalPkg::IsWeekend => vec![DateTimeType],
            // (DateTime, Number)
            CalPkg::Minus
            | CalPkg::Plus => vec![DateTimeType, NumberType(I64Kind)],
        }
    }

    fn get_return_type(&self) -> DataType {
        match self {
            // Boolean
            CalPkg::IsLeapYear
            | CalPkg::IsWeekday
            | CalPkg::IsWeekend => BooleanType,
            // DateTime
            CalPkg::Minus
            | CalPkg::Plus => DateTimeType,
            // Number
            CalPkg::DateDay
            | CalPkg::DateHour12
            | CalPkg::DateHour24
            | CalPkg::DateMinute
            | CalPkg::DateMonth
            | CalPkg::DateSecond
            | CalPkg::DateYear
            | CalPkg::ToMillis => NumberType(I64Kind),
        }
    }

    fn evaluate(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match self {
            CalPkg::DateDay => self.adapter_pf_fn1(ms, args, Self::do_cal_date_part),
            CalPkg::DateHour24 => self.adapter_pf_fn1(ms, args, Self::do_cal_date_part),
            CalPkg::DateHour12 => self.adapter_pf_fn1(ms, args, Self::do_cal_date_part),
            CalPkg::DateMinute => self.adapter_pf_fn1(ms, args, Self::do_cal_date_part),
            CalPkg::DateMonth => self.adapter_pf_fn1(ms, args, Self::do_cal_date_part),
            CalPkg::DateSecond => self.adapter_pf_fn1(ms, args, Self::do_cal_date_part),
            CalPkg::DateYear => self.adapter_pf_fn1(ms, args, Self::do_cal_date_part),
            CalPkg::IsLeapYear => extract_value_fn1(ms, args, Self::is_leapyear),
            CalPkg::IsWeekday => self.adapter_pf_fn1(ms, args, Self::do_cal_date_part),
            CalPkg::IsWeekend => self.adapter_pf_fn1(ms, args, Self::do_cal_date_part),
            CalPkg::Minus => extract_value_fn2(ms, args, Self::do_cal_date_minus),
            CalPkg::Plus => extract_value_fn2(ms, args, Self::do_cal_date_plus),
            CalPkg::ToMillis => extract_value_fn1(ms, args, Self::do_cal_to_millis),
        }
    }
}

/// Durations package
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum DurationsPkg {
    Days,
    Hours,
    Millis,
    Minutes,
    Seconds,
}

impl DurationsPkg {
    fn adapter_pf_fn1<F>(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
        f: F,
    ) -> std::io::Result<(Machine, TypedValue)>
    where
        F: Fn(Machine, &TypedValue, &DurationsPkg) -> std::io::Result<(Machine, TypedValue)>,
    {
        match args.as_slice() {
            [a] => f(ms, a, self),
            args => throw(TypeMismatch(ArgumentsMismatched(1, args.len()))),
        }
    }

    fn do_durations(
        ms: Machine,
        value: &TypedValue,
        pkg: &Self,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let factor = match pkg {
            DurationsPkg::Days => DAYS,
            DurationsPkg::Hours => HOURS,
            DurationsPkg::Millis => MILLIS,
            DurationsPkg::Minutes => MINUTES,
            DurationsPkg::Seconds => SECONDS,
        };
        let op = Multiply(
            Literal(value.clone()).into(),
            Literal(Number(I64Value(factor))).into(),
        );
        ms.evaluate(&op)
    }
    
    pub fn get_contents() -> Vec<PackageOps> {
        vec![
            PackageOps::Durations(DurationsPkg::Days),
            PackageOps::Durations(DurationsPkg::Hours),
            PackageOps::Durations(DurationsPkg::Millis),
            PackageOps::Durations(DurationsPkg::Minutes),
            PackageOps::Durations(DurationsPkg::Seconds),
        ]
    }
}

impl Package for DurationsPkg {
    fn get_name(&self) -> String {
        match self {
            DurationsPkg::Days => "days".into(),
            DurationsPkg::Hours => "hours".into(),
            DurationsPkg::Millis => "millis".into(),
            DurationsPkg::Minutes => "minutes".into(),
            DurationsPkg::Seconds => "seconds".into(),
        }
    }

    fn get_package_name(&self) -> String {
        "durations".into()
    }

    fn get_description(&self) -> String {
        match self {
            DurationsPkg::Days => "Converts a number into the equivalent number of days".into(),
            DurationsPkg::Hours => "Converts a number into the equivalent number of hours".into(),
            DurationsPkg::Millis => "Converts a number into the equivalent number of millis".into(),
            DurationsPkg::Minutes => {
                "Converts a number into the equivalent number of minutes".into()
            }
            DurationsPkg::Seconds => {
                "Converts a number into the equivalent number of seconds".into()
            }
        }
    }

    fn get_examples(&self) -> Vec<String> {
        match self {
            DurationsPkg::Days => vec![
                strip_margin(r#"
                    |3::days
                "#, '|')
            ],
            DurationsPkg::Hours => vec![
                strip_margin(r#"
                    |8::hours
                "#, '|')
            ],
            DurationsPkg::Millis => vec![
                strip_margin(r#"
                    |8::millis
                "#, '|')
            ],
            DurationsPkg::Minutes => vec![
                strip_margin(r#"
                    |30::minutes
                "#, '|')
            ],
            DurationsPkg::Seconds => vec![
                strip_margin(r#"
                    |30::seconds
                "#, '|')
            ],
        }
    }

    fn get_parameter_types(&self) -> Vec<DataType> {
        vec![NumberType(I64Kind)]
    }

    fn get_return_type(&self) -> DataType {
        NumberType(I64Kind)
    }

    fn evaluate(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match self {
            DurationsPkg::Days => self.adapter_pf_fn1(ms, args, Self::do_durations),
            DurationsPkg::Hours => self.adapter_pf_fn1(ms, args, Self::do_durations),
            DurationsPkg::Millis => self.adapter_pf_fn1(ms, args, Self::do_durations),
            DurationsPkg::Minutes => self.adapter_pf_fn1(ms, args, Self::do_durations),
            DurationsPkg::Seconds => self.adapter_pf_fn1(ms, args, Self::do_durations),
        }
    }
}

/// I/O package
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum IoPkg {
    FileCreate,
    FileExists,
    FileReadText,
    StdErr,
    StdIn,
    StdOut,
}

impl IoPkg {
    fn do_io_create_file(
        ms: Machine,
        path_v: &TypedValue,
        contents_v: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let path = pull_string(path_v)?;
        let mut file = File::create(path)?;
        let n_bytes = file.write(contents_v.unwrap_value().as_bytes())? as u64;
        Ok((ms, Number(I64Value(n_bytes as i64))))
    }

    fn do_io_exists(
        ms: Machine,
        path_value: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let path = pull_string(path_value)?;
        Ok((ms, Boolean(Path::new(path.as_str()).exists())))
    }

    pub fn do_io_list_files(
        ms: Machine,
        path_value: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let path = pull_string(path_value)?;
        let mut mrc = ModelRowCollection::from_parameters(&Self::get_io_files_parameters());
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let (metadata, path) = (entry.metadata()?, entry.file_name());
            mrc.append_row(Row::new(0, vec![
                StringValue(path.display().to_string()),
                Boolean(metadata.is_dir()),
                Boolean(metadata.is_file()),
                Boolean(metadata.is_symlink()),
                StringValue(StringPrinter::format("0o%o", vec![
                    Number(U64Value(metadata.mode() as u64))
                ]).map_err(|e| cnv_error!(e))?),
                Number(U64Value(metadata.len())),
                Number(U64Value(metadata.size())),
                DateTimeValue(metadata.accessed()?.duration_since(UNIX_EPOCH)
                    .map_err(|e| cnv_error!(e))?.as_millis() as i64),
                DateTimeValue(metadata.modified()?.duration_since(UNIX_EPOCH)
                    .map_err(|e| cnv_error!(e))?.as_millis() as i64),
                DateTimeValue(metadata.created()?.duration_since(UNIX_EPOCH)
                    .map_err(|e| cnv_error!(e))?.as_millis() as i64),
            ]));
        }
        Ok((ms, TableValue(ModelTable(mrc))))
    }

    fn do_io_read_text_file(
        ms: Machine,
        path_v: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let path = pull_string(path_v)?;
        let mut buffer = String::new();
        let mut file = File::open(path)?;
        let _count = file.read_to_string(&mut buffer)?;
        Ok((ms, StringValue(buffer)))
    }

    fn do_io_stderr(ms: Machine, value: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        let mut out = stderr();
        out.write(format!("{}", value.unwrap_value()).as_bytes())?;
        out.flush()?;
        Ok((ms, Boolean(true)))
    }
    
    fn do_io_stdin(ms: Machine) -> std::io::Result<(Machine, TypedValue)> {
        let mut input = String::new();
        let _ = std::io::stdin().read_line(&mut input)?;
        Ok((ms, StringValue(input)))
    }

    pub fn do_io_stdout(
        ms: Machine,
        value: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let mut out = stdout();
        out.write(format!("{}\n", value.unwrap_value()).as_bytes())?;
        out.flush()?;
        Ok((ms, Boolean(true)))
    }
    
    pub fn get_contents() -> Vec<PackageOps> {
        vec![
            PackageOps::Io(IoPkg::FileCreate),
            PackageOps::Io(IoPkg::FileExists),
            PackageOps::Io(IoPkg::FileReadText),
            PackageOps::Io(IoPkg::StdErr),
            PackageOps::Io(IoPkg::StdIn),
            PackageOps::Io(IoPkg::StdOut),
        ]
    }

    pub fn get_io_files_parameters() -> Vec<Parameter> {
        vec![
            Parameter::new("name", StringType),
            Parameter::new("is_directory", BooleanType),
            Parameter::new("is_file", BooleanType),
            Parameter::new("is_symlink", BooleanType),
            Parameter::new("mode", NumberType(U64Kind)),
            Parameter::new("length", NumberType(U64Kind)),
            Parameter::new("size", NumberType(U64Kind)),
            Parameter::new("accessed_time", DateTimeType),
            Parameter::new("modified_time", DateTimeType),
            Parameter::new("created_time", DateTimeType),
        ]
    }
}

impl Package for IoPkg {
    fn get_name(&self) -> String {
        (match self {
            IoPkg::FileCreate => "create_file",
            IoPkg::FileExists => "exists",
            IoPkg::FileReadText => "read_text_file",
            IoPkg::StdErr => "stderr",
            IoPkg::StdIn => "stdin",
            IoPkg::StdOut => "stdout",
        }).to_string()
    }

    fn get_package_name(&self) -> String {
        "io".into()
    }

    fn get_description(&self) -> String {
        (match self {
            IoPkg::FileCreate => "Creates a new file",
            IoPkg::FileExists => "Returns true if the source path exists",
            IoPkg::FileReadText => "Reads the contents of a text file into memory",
            IoPkg::StdErr => "Writes a string to STDERR",
            IoPkg::StdIn => "Reads input from STDIN as a string",
            IoPkg::StdOut => "Writes a string to STDOUT",
        }).to_string()
    }

    fn get_examples(&self) -> Vec<String> {
        match self {
            IoPkg::FileCreate => vec![strip_margin(
                r#"
                    |io::create_file("quote.json", {
                    |   symbol: "TRX",
                    |   exchange: "NYSE",
                    |   last_sale: 45.32
                    |})
                "#, '|', )
            ],
            IoPkg::FileExists => vec![r#"io::exists("quote.json")"#.to_string()],
            IoPkg::FileReadText => vec![
                strip_margin(r#"
                    |use io, util
                    |file = "temp_secret.txt"
                    |file:::create_file(md5("**keep**this**secret**"))
                    |file:::read_text_file()
                "#, '|')
            ],
            IoPkg::StdErr => vec![r#"io::stderr("Goodbye Cruel World")"#.to_string()],
            IoPkg::StdIn => vec![],
            IoPkg::StdOut => vec![r#"io::stdout("Hello World")"#.to_string()],
        }
    }

    fn get_parameter_types(&self) -> Vec<DataType> {
        match self {
            IoPkg::FileCreate => vec![StringType, StringType],
            IoPkg::FileExists | IoPkg::FileReadText | IoPkg::StdErr | IoPkg::StdOut => {
                vec![StringType]
            }
            IoPkg::StdIn => vec![],
        }
    }

    fn get_return_type(&self) -> DataType {
        match self {
            IoPkg::FileReadText => ArrayType(RuntimeResolvedType.into()),
            IoPkg::FileCreate | IoPkg::FileExists => BooleanType,
            IoPkg::StdErr | IoPkg::StdIn | IoPkg::StdOut => StringType,
        }
    }

    fn evaluate(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match self {
            IoPkg::FileCreate => extract_value_fn2(ms, args, Self::do_io_create_file),
            IoPkg::FileExists => extract_value_fn1(ms, args, Self::do_io_exists),
            IoPkg::FileReadText => extract_value_fn1(ms, args, Self::do_io_read_text_file),
            IoPkg::StdErr => extract_value_fn1(ms, args, Self::do_io_stderr),
            IoPkg::StdIn => extract_value_fn0(ms, args, Self::do_io_stdin),
            IoPkg::StdOut => extract_value_fn1(ms, args, Self::do_io_stdout),
        }
    }
}

/// Math package
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum MathPkg {
    Abs,
    Ceil,
    Floor,
    Max,
    Min,
    Pow,
    Round,
    Sqrt,
}

impl MathPkg {
    pub fn get_contents() -> Vec<PackageOps> {
        vec![
            PackageOps::Math(MathPkg::Abs),
            PackageOps::Math(MathPkg::Ceil),
            PackageOps::Math(MathPkg::Floor),
            PackageOps::Math(MathPkg::Max),
            PackageOps::Math(MathPkg::Min),
            PackageOps::Math(MathPkg::Pow),
            PackageOps::Math(MathPkg::Round),
            PackageOps::Math(MathPkg::Sqrt),
        ]
    }
}

impl Package for MathPkg {
    fn get_name(&self) -> String {
        match self {
            MathPkg::Abs => "abs".into(),
            MathPkg::Ceil => "ceil".into(),
            MathPkg::Floor => "floor".into(),
            MathPkg::Max => "max".into(),
            MathPkg::Min => "min".into(),
            MathPkg::Pow => "pow".into(),
            MathPkg::Round => "round".into(),
            MathPkg::Sqrt => "sqrt".into(),
        }
    }

    fn get_package_name(&self) -> String {
        "math".into()
    }

    fn get_description(&self) -> String {
        match self {
            MathPkg::Abs => "abs(x): Returns the absolute value of x.".into(),
            MathPkg::Ceil => {
                "ceil(x): Returns the smallest integer greater than or equal to x.".into()
            }
            MathPkg::Floor => {
                "floor(x): Returns the largest integer less than or equal to x.".into()
            }
            MathPkg::Max => "max(a, b): Returns the larger of a and b".into(),
            MathPkg::Min => "min(a, b): Returns the smaller of a and b.".into(),
            MathPkg::Pow => "pow(x, y): Returns x raised to the power of y.".into(),
            MathPkg::Round => "round(x): Rounds x to the nearest integer.".into(),
            MathPkg::Sqrt => "sqrt(x): Returns the square root of x.".into(),
        }
    }

    fn get_examples(&self) -> Vec<String> {
        match self {
            MathPkg::Abs => vec!["math::abs(-81)".into()],
            MathPkg::Ceil => vec!["math::ceil(5.7)".into()],
            MathPkg::Floor => vec!["math::floor(5.7)".into()],
            MathPkg::Max => vec!["math::max(81, 78)".into()],
            MathPkg::Min => vec!["math::min(81, 78)".into()],
            MathPkg::Pow => vec!["math::pow(2, 3)".into()],
            MathPkg::Round => vec!["math::round(5.3)".into()],
            MathPkg::Sqrt => vec!["math::sqrt(25)".into()],
        }
    }

    fn get_parameter_types(&self) -> Vec<DataType> {
        match self {
            // single-parameter (f64)
            MathPkg::Abs | MathPkg::Ceil | MathPkg::Floor | MathPkg::Round | MathPkg::Sqrt => {
                vec![NumberType(F64Kind)]
            }
            // two-parameter (f64, f64)
            MathPkg::Max | MathPkg::Min | MathPkg::Pow => {
                vec![NumberType(F64Kind), NumberType(F64Kind)]
            }
        }
    }

    fn get_return_type(&self) -> DataType {
        match self {
            // i64
            MathPkg::Abs
            | MathPkg::Ceil
            | MathPkg::Floor
            | MathPkg::Max
            | MathPkg::Min
            | MathPkg::Pow
            | MathPkg::Round
            | MathPkg::Sqrt => NumberType(I64Kind),
        }
    }

    fn evaluate(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match self {
            MathPkg::Abs => extract_number_fn1(ms, args, |n| n.abs()),
            MathPkg::Ceil => extract_number_fn1(ms, args, |n| n.ceil()),
            MathPkg::Floor => extract_number_fn1(ms, args, |n| n.floor()),
            MathPkg::Max => extract_number_fn2(ms, args, |n, m| n.max(m)),
            MathPkg::Min => extract_number_fn2(ms, args, |n, m| n.min(m)),
            MathPkg::Pow => extract_number_fn2(ms, args, |n, m| n.pow(m)),
            MathPkg::Round => extract_number_fn1(ms, args, |n| n.round()),
            MathPkg::Sqrt => extract_number_fn1(ms, args, |n| n.sqrt()),
        }
    }
}

/// NSD package
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum NsdPkg {
    CreateEventSrc,
    CreateFn,
    CreateIndex,
    Drop,
    Exists,
    Journal,
    Load,
    Replay,
    Resize,
    Save,
    Truncate
}

impl NsdPkg {
    /// Creates a journaled event-source
    /// #### Examples
    /// ```
    /// nsd::create_event_src(
    ///   "examples.event_src.stocks",
    ///   Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
    /// )
    /// ```
    pub fn do_nsd_create_event_src(
        ms: Machine,
        path_v: &TypedValue,
        table_type_v: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let path = pull_string(path_v)?;
        let ns = Namespace::parse(path.as_str())?;
        match table_type_v {
            TableValue(df) => {
                let erc = EventSourceRowCollection::new(&ns, &df.get_parameters())?;
                Ok((ms, TableValue(EventSource(erc.into()))))
            }
            other => throw(TypeMismatch(FunctionExpected(other.to_code())))
        }
    }
    
    /// Creates a journaled table function
    /// #### Examples
    /// ```
    /// nsd::create_fn(
    ///   "examples.table_fn.stocks",
    ///   (symbol: String(8), exchange: String(8), last_sale: f64) -> {
    ///       symbol: symbol,
    ///       exchange: exchange,
    ///       last_sale: last_sale * 2.0,
    ///       event_time: DateTime::new()
    ///   })
    /// ```
    pub fn do_nsd_create_fn(
        ms: Machine,
        path_v: &TypedValue,
        fn_v: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let path = pull_string(path_v)?;
        let ns = Namespace::parse(path.as_str())?;
        match fn_v.clone() { 
            Function { params, body, .. } => {
                let frc = TableFunction::create_table_fn(
                    &ns,
                    params,
                    body.deref().clone(),
                    ms.clone(),
                )?;
                Ok((ms, TableValue(TableFn(frc.into()))))
            }
            other => throw(TypeMismatch(FunctionExpected(other.to_code())))
        }
    }

    /// Creates an index on a host table
    /// #### Examples
    /// ```
    /// nsd::create_index("packages.indices.stocks", ["symbol", "exchange"])
    /// ```
    pub fn do_nsd_create_index(
        ms: Machine,
        path_v: &TypedValue,
        index_columns_v: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        // get the namespace
        let path = pull_string(path_v)?;
        let ns = Namespace::parse(path.as_str())?;

        // get the index columns
        let column_names = pull_array(index_columns_v)?
            .get_values()
            .iter().map(|v| pull_string(v)).collect::<Result<Vec<String>, _>>()?;

        // load the configuration
        let config = ObjectConfig::load(&ns)?;

        // update the indices
        let mut indices = config.get_indices();
        indices.push(HashIndexConfig::new(column_names, false));

        // update the configuration
        let updated_config = config.with_indices(indices);
        updated_config.save(&ns)?;
        Ok((ms, Boolean(true)))
    }

    /// Deletes a dataframe from a namespace
    /// #### Examples
    /// ```
    /// nsd::drop("packages.remove.stocks")
    /// ```
    pub fn do_nsd_drop(ms: Machine, path_v: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        let path = pull_string(path_v)?;
        let ns = Namespace::parse(path.as_str())?;
        let result1 = fs::remove_dir_all(ns.get_root_path());
        let result2 = fs::remove_dir_all(ns.with_events_name().get_root_path());
        Ok((ms, Boolean(result1.is_ok() || result2.is_ok())))
    }

    /// Indicates whether a dataframe exists within a namespace
    /// #### Examples
    /// ```
    /// nsd::exists("packages.remove.stocks")
    /// ```
    pub fn do_nsd_exists(ms: Machine, path_v: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        let path = pull_string(path_v)?;
        let ns = Namespace::parse(path.as_str())?;
        Ok((ms, Boolean(Path::new(ns.get_table_file_path().as_str()).exists())))
    }

    /// Retrieves the journal for a dataframe (table function or event source)
    /// #### Examples
    /// ```
    /// nsd::journal("packages.journal.stocks")
    /// ```
    fn do_nsd_journal(ms: Machine, value: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        match value.to_dataframe()? {
            EventSource(mut df) => Ok((ms, TableValue(df.get_journal()))),
            TableFn(mut df) => Ok((ms, TableValue(df.get_journal()))),
            _ => throw(TypeMismatch(UnsupportedType(
                TableType(vec![]),
                value.get_type(),
            ))),
        }
    }
    
    /// Loads a dataframe from a namespace
    /// #### Examples
    /// ```
    /// let stocks = nsd::load("packages.loading.stocks")
    /// ``` 
    pub fn do_nsd_load(ms: Machine, path_v: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        let path = pull_string(path_v)?;
        let ns = Namespace::parse(path.as_str())?;
        let frc = FileRowCollection::open(&ns)?;
        Ok((ms, TableValue(DiskTable(frc))))
    }

    /// Rebuilds a dataframe by replaying its journal
    /// #### Examples
    /// ```
    /// let stocks = nsd::load("packages.loading.stocks")
    /// nsd::replay(stocks)
    /// ``` 
    fn do_nsd_replay(ms: Machine, table: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        match table.to_dataframe()? {
            EventSource(mut df) => Ok((ms, df.replay()?)),
            TableFn(mut df) => Ok((ms, df.replay()?)),
            _ => throw(TypeMismatch(UnsupportedType(
                TableType(vec![]),
                table.get_type(),
            ))),
        }
    }

    /// Changes the size of a dataframe 
    /// #### Examples
    /// ```
    /// nsd::resize("packages.examples.stocks", 100)
    /// ``` 
    fn do_nsd_resize(
        ms: Machine,
        namespace_or_df: &TypedValue,
        new_size: &TypedValue
    ) -> std::io::Result<(Machine, TypedValue)> {
        /// Resizes the [Dataframe]
        fn resize_table(
            ms: Machine,
            mut df: Dataframe,
            new_size: &TypedValue,
        ) -> std::io::Result<(Machine, TypedValue)>{
            let size = pull_number(new_size)?;
            Ok((ms, Boolean(df.resize(size.to_usize())?)))
        }

        // process either a namespace or dataframe
        match namespace_or_df.clone() {
            TableValue(mut df) => resize_table(ms, df, new_size),
            StringValue(..) =>
                match Self::do_nsd_load(ms, namespace_or_df)? {
                    (ms, TableValue(mut df)) => resize_table(ms, df, new_size),
                    (_, other) => throw(TypeMismatch(TableExpected(other.to_code())))
                }
            other => throw(TypeMismatch(TableExpected(other.to_code())))
        }
    }

    /// Truncate a dataframe; deleting all rows and reducing its size to zero.
    /// #### Examples
    /// ```
    /// nsd::truncate("packages.examples.stocks")
    /// ```
    fn do_nsd_truncate(
        ms: Machine,
        namespace: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match Self::do_nsd_load(ms, namespace)? {
            (ms, TableValue(mut df)) => {
                Ok((ms, Boolean(df.resize(0)?)))
            }
            (_, other) => throw(TypeMismatch(TableExpected(other.to_code())))
        }
    }

    /// Creates or replaces a dataframe within a namespace
    pub fn do_nsd_save(
        ms: Machine,
        path_v: &TypedValue,
        contents_v: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let path = pull_string(path_v)?;
        match contents_v.to_table_or_value() {
            TableValue(mrc) => {
                let ns = Namespace::parse(path.as_str())?;
                let params = mrc.get_parameters();
                let mut frc = FileRowCollection::create_table(&ns, &params)?;
                frc.append_rows(mrc.get_rows())?;
                Ok((ms, TableValue(DiskTable(frc))))
            }
            x => throw(Exact(format!("Expected type near {}", x.to_code())))
        }
    }
    
    pub fn get_contents() -> Vec<PackageOps> {
        vec![
            PackageOps::Nsd(NsdPkg::CreateEventSrc),
            PackageOps::Nsd(NsdPkg::CreateFn),
            PackageOps::Nsd(NsdPkg::CreateIndex),
            PackageOps::Nsd(NsdPkg::Drop),
            PackageOps::Nsd(NsdPkg::Exists),
            PackageOps::Nsd(NsdPkg::Journal),
            PackageOps::Nsd(NsdPkg::Load),
            PackageOps::Nsd(NsdPkg::Replay),
            PackageOps::Nsd(NsdPkg::Resize),
            PackageOps::Nsd(NsdPkg::Save),
            PackageOps::Nsd(NsdPkg::Truncate),
        ]
    }
}

impl Package for NsdPkg {
    fn get_name(&self) -> String {
        match self {
            NsdPkg::CreateEventSrc => "create_event_src".into(),
            NsdPkg::CreateFn => "create_fn".into(),
            NsdPkg::CreateIndex => "create_index".into(),
            NsdPkg::Drop => "drop".into(),
            NsdPkg::Exists => "exists".into(),
            NsdPkg::Journal => "journal".into(),
            NsdPkg::Load => "load".into(),
            NsdPkg::Replay => "replay".into(),
            NsdPkg::Resize => "resize".into(),
            NsdPkg::Save => "save".into(),
            NsdPkg::Truncate => "truncate".into(),
        }
    }

    fn get_package_name(&self) -> String {
        "nsd".into()
    }

    fn get_description(&self) -> String {
        match self {
            NsdPkg::CreateEventSrc => "Creates a journaled event-source".into(),
            NsdPkg::CreateFn => "Creates a journaled table function".into(),
            NsdPkg::CreateIndex => "Creates a table index".into(),
            NsdPkg::Drop => "Deletes a dataframe from a namespace".into(),
            NsdPkg::Exists => "Returns true if the source path exists".into(),
            NsdPkg::Journal => "Retrieves the journal for an event-source or table function".into(),
            NsdPkg::Load => "Loads a dataframe from a namespace".into(),
            NsdPkg::Replay => "Reconstructs the state of a journaled table".into(),
            NsdPkg::Resize => "Changes the size of a dataframe".into(),
            NsdPkg::Save => "Creates a new dataframe".into(),
            NsdPkg::Truncate => "Truncate a dataframe; deleting all rows and reducing its size to zero".into(),
        }
    }

    fn get_examples(&self) -> Vec<String> {
        match self {
            NsdPkg::CreateEventSrc => vec![
                strip_margin(r#"
                    |nsd::create_event_src(
                    |   "examples.event_src.stocks",
                    |   Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                    |)
                "#, '|'),
            ],
            NsdPkg::CreateFn => vec![
                strip_margin(r#"
                    |nsd::create_fn(
                    |   "examples.table_fn.stocks",
                    |   (symbol: String(8), exchange: String(8), last_sale: f64) -> {
                    |       symbol: symbol,
                    |       exchange: exchange,
                    |       last_sale: last_sale * 2.0,
                    |       event_time: DateTime::new()
                    |   })
                "#, '|'),
            ],
            NsdPkg::CreateIndex => vec![],
            NsdPkg::Drop => vec![
                strip_margin(r#"
                    |nsd::save('packages.remove.stocks', Table(
                    |    symbol: String(8),
                    |    exchange: String(8),
                    |    last_sale: f64
                    |)::new)
                    |
                    |nsd::drop('packages.remove.stocks')
                    |nsd::exists('packages.remove.stocks')
                    |"#, '|')
            ],
            NsdPkg::Exists => vec![
                strip_margin(r#"
                    |nsd::save('packages.exists.stocks', Table(
                    |   symbol: String(8),
                    |   exchange: String(8),
                    |   last_sale: f64
                    |)::new)
                    |nsd::exists("packages.exists.stocks")
                "#, '|'),
                strip_margin(r#"
                    |nsd::exists("packages.not_exists.stocks")
                "#, '|')
            ],
            NsdPkg::Journal => vec![
                strip_margin(r#"
                    |use nsd
                    |nsd::drop("examples.journal.stocks");
                    |stocks = nsd::create_fn(
                    |   "examples.journal.stocks",
                    |   (symbol: String(8), exchange: String(8), last_sale: f64) -> {
                    |       symbol: symbol,
                    |       exchange: exchange,
                    |       last_sale: last_sale * 2.0,
                    |       ingest_time: DateTime::new()
                    |   });
                    |[{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                    | { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                    | { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
                    |stocks::journal()
                "#, '|')
            ],
            NsdPkg::Load => vec![
                strip_margin(r#"
                    |let stocks =
                    |   nsd::save('packages.save_load.stocks', Table(
                    |       symbol: String(8),
                    |       exchange: String(8),
                    |       last_sale: f64
                    |   )::new)
                    |
                    |let rows = 
                    |   [{ symbol: "CAZ", exchange: "AMEX", last_sale: 65.13 },
                    |    { symbol: "BAL", exchange: "NYSE", last_sale: 82.78 },
                    |    { symbol: "RCE", exchange: "NASDAQ", last_sale: 124.09 }] 
                    |
                    |rows ~> stocks
                    |
                    |nsd::load('packages.save_load.stocks')
                    |"#, '|')
            ],
            NsdPkg::Replay => vec![
                strip_margin(r#"
                    |use nsd
                    |nsd::drop("examples.replay.stocks");
                    |stocks = nsd::create_fn(
                    |   "examples.replay.stocks",
                    |   (symbol: String(8), exchange: String(8), last_sale: f64) -> {
                    |       symbol: symbol,
                    |       exchange: exchange,
                    |       last_sale: last_sale * 2.0,
                    |       rank: __row_id__ + 1
                    |   });
                    |[{ symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                    | { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                    | { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
                    |stocks::replay()
                "#, '|')
            ],
            NsdPkg::Resize => vec![
                strip_margin(r#"
                    |use nsd
                    |let stocks =
                    |   nsd::save('packages.resize.stocks', Table(
                    |       symbol: String(8),
                    |       exchange: String(8),
                    |       last_sale: f64
                    |   )::new)
                    |[{ symbol: "TCO", exchange: "NYSE", last_sale: 38.53 },
                    | { symbol: "SHMN", exchange: "NYSE", last_sale: 6.57 },
                    | { symbol: "HMU", exchange: "NASDAQ", last_sale: 27.12 }] ~> stocks
                    |'packages.resize.stocks':::resize(1)
                    |stocks
                "#, '|')                
            ],
            NsdPkg::Save => vec![
                strip_margin(r#"
                    |let stocks =
                    |   nsd::save('packages.save.stocks', Table(
                    |       symbol: String(8),
                    |       exchange: String(8),
                    |       last_sale: f64
                    |   )::new)
                    |[{ symbol: "TCO", exchange: "NYSE", last_sale: 38.53 },
                    | { symbol: "SHMN", exchange: "NYSE", last_sale: 6.57 },
                    | { symbol: "HMU", exchange: "NASDAQ", last_sale: 27.12 }] ~> stocks
                    |stocks
                    |"#, '|')
            ],
            NsdPkg::Truncate => vec![],
        }
    }

    fn get_parameter_types(&self) -> Vec<DataType> {
        match self {
            // (String, Table)
            NsdPkg::CreateEventSrc
            | NsdPkg::Save => vec![
                StringType, TableType(vec![]),
            ],
            // (String, Function)
            NsdPkg::CreateFn => vec![
                StringType, FunctionType(vec![], StructureType(vec![]).into())
            ],
            // (String, Array)
            NsdPkg::CreateIndex => vec![
                StringType, ArrayType(RuntimeResolvedType.into())
            ],
            // (String)
            NsdPkg::Exists
            | NsdPkg::Load
            | NsdPkg::Drop
            | NsdPkg::Truncate => vec![StringType],
            // (Table)
            NsdPkg::Journal
            | NsdPkg::Replay => vec![TableType(vec![])],
            // (Table, i64)
            | NsdPkg::Resize => vec![
                StringType, NumberType(I64Kind)
            ],
        }
    }

    fn get_return_type(&self) -> DataType {
        match self {
            NsdPkg::CreateEventSrc
            | NsdPkg::CreateFn
            | NsdPkg::Journal
            | NsdPkg::Load
            | NsdPkg::Save => TableType(vec![]),
            NsdPkg::CreateIndex
            | NsdPkg::Drop
            | NsdPkg::Exists
            | NsdPkg::Replay
            | NsdPkg::Resize
            | NsdPkg::Truncate => BooleanType,
        }
    }

    fn evaluate(&self, ms: Machine, args: Vec<TypedValue>) -> std::io::Result<(Machine, TypedValue)> {
        match self {
            NsdPkg::CreateEventSrc => extract_value_fn2(ms, args, Self::do_nsd_create_event_src),
            NsdPkg::CreateFn => extract_value_fn2(ms, args, Self::do_nsd_create_fn),
            NsdPkg::CreateIndex => extract_value_fn2(ms, args, Self::do_nsd_create_index),
            NsdPkg::Drop => extract_value_fn1(ms, args, Self::do_nsd_drop),
            NsdPkg::Exists => extract_value_fn1(ms, args, Self::do_nsd_exists),
            NsdPkg::Journal => extract_value_fn1(ms, args, Self::do_nsd_journal),
            NsdPkg::Load => extract_value_fn1(ms, args, Self::do_nsd_load),
            NsdPkg::Replay => extract_value_fn1(ms, args, Self::do_nsd_replay),
            NsdPkg::Resize => extract_value_fn2(ms, args, Self::do_nsd_resize),
            NsdPkg::Save => extract_value_fn2(ms, args, Self::do_nsd_save),
            NsdPkg::Truncate => extract_value_fn1(ms, args, Self::do_nsd_truncate),
        }
    }
}

/// Oxide package
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum OxidePkg {
    Compile,
    Debug,
    Eval,
    Help,
    History,
    Home,
    Inspect,
    Printf,
    Println,
    Reset,
    Sprintf,
    UUID,
    Version,
}

impl OxidePkg {
    fn do_oxide_compile(ms: Machine, value: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        let source = pull_string(value)?;
        let code = Compiler::build(source.as_str())?;
        Ok((ms, Function {
            params: vec![],
            body: Box::new(code),
            returns: RuntimeResolvedType,
        }))
    }

    fn do_oxide_debug(ms: Machine, value: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        let source = pull_string(value)?;
        let code = Compiler::build(source.as_str());
        Ok((ms, StringValue(format!("{:?}", code))))
    }

    fn do_oxide_eval(
        ms: Machine,
        query_value: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let query = pull_string(query_value)?;
        let opcode = Compiler::build(query.as_str())?;
        ms.evaluate(&opcode)
    }

    /// returns a table describing all modules
    fn do_oxide_help(ms: Machine) -> std::io::Result<(Machine, TypedValue)> {
        let mut mrc = ModelRowCollection::from_parameters(&OxidePkg::get_oxide_help_parameters());
        for (module_name, module) in ms.get_variables().iter() {
            match module {
                Structured(Hard(mod_struct)) => {
                    for (name, func) in mod_struct.to_name_values() {
                        mrc.append_row(Row::new(
                            0,
                            vec![
                                // name
                                StringValue(name),
                                // module
                                StringValue(module_name.to_string()),
                                // signature
                                StringValue(func.to_code()),
                                // description
                                match func {
                                    PlatformOp(pf) => StringValue(pf.get_description()),
                                    _ => Null,
                                },
                                // returns
                                match func {
                                    PlatformOp(pf) => StringValue(pf.get_return_type().to_code()),
                                    _ => Null,
                                },
                            ],
                        ));
                    }
                }
                _ => {}
            }
        }
        Ok((ms, TableValue(ModelTable(mrc))))
    }

    fn do_oxide_history(
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        // re-executes a saved command
        fn re_run(ms: Machine, pid: usize) -> std::io::Result<(Machine, TypedValue)> {
            let frc = FileRowCollection::open_or_create(
                &OxidePkg::get_oxide_history_ns(),
                OxidePkg::get_oxide_history_parameters(),
            )?;
            let row_maybe = frc.read_one(pid)?;
            let code = row_maybe
                .map(|r| {
                    r.get_values()
                        .last()
                        .map(|v| v.unwrap_value())
                        .unwrap_or(String::new())
                })
                .unwrap_or(String::new());
            for line in code.split(|c| c == ';').collect::<Vec<_>>() {
                println!(">>> {}", line);
            }
            let model = Compiler::build(code.as_str())?;
            ms.evaluate(&model)
        }

        // evaluate based on the arguments
        match args.as_slice() {
            // history()
            [] => {
                let frc = FileRowCollection::open_or_create(
                    &OxidePkg::get_oxide_history_ns(),
                    OxidePkg::get_oxide_history_parameters(),
                )?;
                Ok((ms, TableValue(DiskTable(frc))))
            }
            // history(11)
            [Number(pid)] => re_run(ms.to_owned(), pid.to_usize()),
            // history(..)
            other => throw(TypeMismatch(ArgumentsMismatched(other.len(), 1))),
        }
    }

    fn do_oxide_inspect(
        ms: Machine,
        source_code: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let code = Compiler::build(pull_string(source_code)?.as_str())?;
        let ops = match code { 
            CodeBlock(ops) => ops,
            op => vec![op]
        };
        let mut mrc = ModelRowCollection::from_parameters(&OxidePkg::get_oxide_inspect_parameters());
        for (row_id, expr) in ops.iter().enumerate() {
            mrc.overwrite_row(row_id, Row::new(
                row_id,
                vec![
                    StringValue(expr.to_code()),
                    StringValue(format!("{:?}", expr)),
                ],
            ))?;
        }
        Ok((ms, TableValue(ModelTable(mrc))))
    }
    
    fn do_oxide_printf(ms: Machine, args: Vec<TypedValue>) -> std::io::Result<(Machine, TypedValue)> {
        let (ms, result) = Self::do_oxide_sprintf(ms, args)?;
        println!("{}", result.unwrap_value());
        Ok((ms, Boolean(true)))
    }

    fn do_oxide_sprintf(ms: Machine, args: Vec<TypedValue>) -> std::io::Result<(Machine, TypedValue)> {
        let format = pull_string(args.get(0).unwrap())?;
        let args = args[1..].to_vec();
        let result = StringPrinter::format(&format, args).map_err(|e| cnv_error!(e))?;
        Ok((ms, StringValue(result)))
    }

    fn do_oxide_uuid(
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let result = match args.as_slice() {
            [ByteStringValue(bytes)] => 
                Uuid::from_slice(bytes.as_slice()).map_err(|e| cnv_error!(e))?.as_u128(),
            [Number(U128Value(n))] => *n,
            [StringValue(s)] => string_to_uuid(s)?,
            [_other] => return throw(Exact("String or u128 value expected".into())),
            [] => Uuid::new_v4().as_u128(),
            _ => return throw(TypeMismatch(ArgumentsMismatched(0, args.len()))),
        };
        Ok((ms, TypedValue::UUIDValue(result)))
    }
    
    fn do_oxide_version(ms: Machine) -> std::io::Result<(Machine, TypedValue)> {
        Ok((ms, StringValue(VERSION.into())))
    }

    pub fn get_contents() -> Vec<PackageOps> {
        vec![
            PackageOps::Oxide(OxidePkg::Compile),
            PackageOps::Oxide(OxidePkg::Debug),
            PackageOps::Oxide(OxidePkg::Eval),
            PackageOps::Oxide(OxidePkg::Help),
            PackageOps::Oxide(OxidePkg::History),
            PackageOps::Oxide(OxidePkg::Home),
            PackageOps::Oxide(OxidePkg::Inspect),
            PackageOps::Oxide(OxidePkg::Printf),
            PackageOps::Oxide(OxidePkg::Println),
            PackageOps::Oxide(OxidePkg::Reset),
            PackageOps::Oxide(OxidePkg::Sprintf),
            PackageOps::Oxide(OxidePkg::UUID),
            PackageOps::Oxide(OxidePkg::Version),
        ]
    }

    pub fn get_oxide_help_parameters() -> Vec<Parameter> {
        vec![
            Parameter::new("name", FixedSizeType(StringType.into(), 20)),
            Parameter::new("module", FixedSizeType(StringType.into(), 20)),
            Parameter::new("signature", FixedSizeType(StringType.into(), 32)),
            Parameter::new("description", FixedSizeType(StringType.into(), 60)),
            Parameter::new("returns", FixedSizeType(StringType.into(), 32)),
        ]
    }

    pub fn get_oxide_history_ns() -> Namespace {
        Namespace::new("oxide", "public", "history")
    }

    pub fn get_oxide_history_parameters() -> Vec<Parameter> {
        vec![
            Parameter::new("session_id", NumberType(I64Kind)),
            Parameter::new("user_id", NumberType(I64Kind)),
            Parameter::new("cpu_time_ms", NumberType(F64Kind)),
            Parameter::new("input", FixedSizeType(StringType.into(), 65536)),
        ]
    }
    
    pub fn get_oxide_inspect_parameters() -> Vec<Parameter> {
        vec![
            Parameter::new("code", FixedSizeType(StringType.into(), 8192)),
            Parameter::new("model", FixedSizeType(StringType.into(), 8192)),
        ]
    }
}

impl Package for OxidePkg {
    fn get_name(&self) -> String {
        match self {
            OxidePkg::Compile => "compile".into(),
            OxidePkg::Debug => "debug".into(),
            OxidePkg::Eval => "eval".into(),
            OxidePkg::Help => "help".into(),
            OxidePkg::History => "history".into(),
            OxidePkg::Home => "home".into(),
            OxidePkg::Inspect => "inspect".into(),
            OxidePkg::Printf => "printf".into(),
            OxidePkg::Println => "println".into(),
            OxidePkg::Reset => "reset".into(),
            OxidePkg::Sprintf => "sprintf".into(),
            OxidePkg::UUID => "uuid".into(),
            OxidePkg::Version => "version".into(),
        }
    }

    fn get_package_name(&self) -> String {
        "oxide".into()
    }

    fn get_description(&self) -> String {
        match self {
            OxidePkg::Compile => "Compiles source code from a string input".into(),
            OxidePkg::Debug => {
                "Compiles source code from a string input; returning a debug string".into()
            }
            OxidePkg::Eval => "Evaluates a string containing Oxide code".into(),
            OxidePkg::Help => "Integrated help function".into(),
            OxidePkg::History => {
                "Returns all commands successfully executed during the session".into()
            }
            OxidePkg::Home => "Returns the Oxide home directory path".into(),
            OxidePkg::Inspect => "Returns a table describing the structure of a model".into(),
            OxidePkg::Printf => "C-style \"printf\" function".into(),
            OxidePkg::Println => "Print line function".into(),
            OxidePkg::Reset => "Clears the scope of all user-defined objects".into(),
            OxidePkg::Sprintf => "C-style \"sprintf\" function".into(),
            OxidePkg::UUID => "Returns a random 128-bit UUID".into(),
            OxidePkg::Version => "Returns the Oxide version".into(),
        }
    }

    fn get_examples(&self) -> Vec<String> {
        match self {
            OxidePkg::Compile => vec![strip_margin(
                r#"
                    |code = oxide::compile("2 ** 4")
                    |code()
                "#,
                '|',
            )],
            OxidePkg::Debug => vec![r#"oxide::debug("2 ** 4")"#.into()],
            OxidePkg::Eval => vec![strip_margin(
                r#"
                    |a = 'Hello '
                    |b = 'World'
                    |oxide::eval("a + b")
                "#,
                '|',
            )],
            OxidePkg::Help => vec![r#"oxide::help() limit 3"#.into()],
            OxidePkg::History => vec![],
            OxidePkg::Home => vec!["oxide::home()".into()],
            OxidePkg::Inspect => vec![
                strip_margin(r#"
                    |oxide::inspect("{ x = 1 x = x + 1 }")
                "#, '|'),
                strip_margin(r#"
                    |oxide::inspect("stock::is_this_you('ABC')")
                "#, '|')
            ],
            OxidePkg::Printf => vec![r#"oxide::printf("Hello %s", "World")"#.into()],
            OxidePkg::Println => vec![r#"oxide::println("Hello World")"#.into()],
            OxidePkg::Reset => vec!["oxide::reset()".into()],
            OxidePkg::Sprintf => vec![r#"oxide::sprintf("Hello %s", "World")"#.into()],
            OxidePkg::UUID => vec!["oxide::uuid()".into()],
            OxidePkg::Version => vec!["oxide::version()".into()],
        }
    }

    fn get_parameter_types(&self) -> Vec<DataType> {
        match self {
            OxidePkg::Compile
            | OxidePkg::Debug
            | OxidePkg::Eval
            | OxidePkg::Inspect
            | OxidePkg::Println => vec![StringType],
            OxidePkg::Home
            | OxidePkg::Reset
            | OxidePkg::Help
            | OxidePkg::History
            | OxidePkg::Version
            | OxidePkg::UUID => vec![],
            OxidePkg::Printf
            | OxidePkg::Sprintf => vec![
                StringType, ArrayType(RuntimeResolvedType.into())
            ],
        }
    }

    fn get_return_type(&self) -> DataType {
        match self {
            // function
            OxidePkg::Compile | OxidePkg::Debug => FunctionType(vec![], RuntimeResolvedType.into()),
            OxidePkg::Eval | OxidePkg::Home => StringType,
            OxidePkg::Help => TableType(OxidePkg::get_oxide_help_parameters()),
            OxidePkg::History => TableType(OxidePkg::get_oxide_history_parameters()),
            OxidePkg::Inspect => TableType(OxidePkg::get_oxide_inspect_parameters()),
            OxidePkg::Printf
            | OxidePkg::Println
            | OxidePkg::Reset => BooleanType,
            // string
            OxidePkg::Sprintf => StringType,
            // f64
            OxidePkg::Version => NumberType(F64Kind),
            OxidePkg::UUID => NumberType(U128Kind),
        }
    }

    fn evaluate(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match self {
            OxidePkg::Compile => extract_value_fn1(ms, args, Self::do_oxide_compile),
            OxidePkg::Debug => extract_value_fn1(ms, args, Self::do_oxide_debug),
            OxidePkg::Eval => extract_value_fn1(ms, args, Self::do_oxide_eval),
            OxidePkg::Help => extract_value_fn0(ms, args, Self::do_oxide_help),
            OxidePkg::History => Self::do_oxide_history(ms, args),
            OxidePkg::Home => extract_value_fn0(ms, args, |ms| Ok((ms, StringValue(Machine::oxide_home())))),
            OxidePkg::Inspect => extract_value_fn1(ms, args, Self::do_oxide_inspect),
            OxidePkg::Printf => Self::do_oxide_printf(ms, args),
            OxidePkg::Println => extract_value_fn1(ms, args, IoPkg::do_io_stdout),
            OxidePkg::Reset => extract_value_fn0(ms, args, |ms| Ok((Machine::new_platform(), Boolean(true)))),
            OxidePkg::Sprintf => Self::do_oxide_sprintf(ms, args),
            OxidePkg::UUID => Self::do_oxide_uuid(ms, args),
            OxidePkg::Version => extract_value_fn0(ms, args, Self::do_oxide_version),
        }
    }
}

/// OS package
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum OsPkg {
    Call,
    Clear,
    CurrentDir,
    Env,
}

impl OsPkg {
    fn do_os_call(ms: Machine, args: Vec<TypedValue>) -> std::io::Result<(Machine, TypedValue)> {
        fn split_first<T>(vec: Vec<T>) -> Option<(T, Vec<T>)> {
            let mut iter = vec.into_iter();
            iter.next().map(|first| (first, iter.collect()))
        }

        let items: Vec<_> = args.iter().map(|i| i.unwrap_value()).collect();
        if let Some((command, cmd_args)) = split_first(items) {
            let output = std::process::Command::new(command)
                .args(cmd_args)
                .output()?;
            if output.status.success() {
                let raw_text = String::from_utf8_lossy(&output.stdout);
                Ok((ms, StringValue(raw_text.to_string())))
            } else {
                let message = String::from_utf8_lossy(&output.stderr);
                Ok((ms, ErrorValue(Exact(message.to_string()))))
            }
        } else {
            Ok((
                ms,
                ErrorValue(TypeMismatch(CollectionExpected(
                    args.iter()
                        .map(|e| e.to_code())
                        .collect::<Vec<_>>()
                        .join(", "),
                ))),
            ))
        }
    }

    fn do_os_clear_screen(ms: Machine) -> std::io::Result<(Machine, TypedValue)> {
        print!("\x1B[2J\x1B[H");
        std::io::stdout().flush()?;
        Ok((ms, Boolean(true)))
    }

    fn do_os_current_dir(ms: Machine) -> std::io::Result<(Machine, TypedValue)> {
        let dir = env::current_dir()?;
        Ok((ms, StringValue(dir.display().to_string())))
    }

    fn do_os_env(ms: Machine) -> std::io::Result<(Machine, TypedValue)> {
        use std::env;
        let mut mrc = ModelRowCollection::from_parameters(&vec![
            Parameter::new("key", FixedSizeType(StringType.into(), 256)),
            Parameter::new("value", FixedSizeType(StringType.into(), 8192)),
        ]);
        for (key, value) in env::vars() {
                mrc.append_row(Row::new(0, vec![StringValue(key), StringValue(value)]))?;
        }
        Ok((ms, TableValue(ModelTable(mrc))))
    }

    pub(crate) fn get_contents() -> Vec<PackageOps> {
        vec![
            PackageOps::Os(OsPkg::Call),
            PackageOps::Os(OsPkg::Clear),
            PackageOps::Os(OsPkg::CurrentDir),
            PackageOps::Os(OsPkg::Env),
        ]
    }

    pub fn get_os_env_parameters() -> Vec<Parameter> {
        vec![
            Parameter::new("key", FixedSizeType(StringType.into(), 256)),
            Parameter::new("value", FixedSizeType(StringType.into(), 8192)),
        ]
    }
}

impl Package for OsPkg {
    fn get_name(&self) -> String {
        match self {
            OsPkg::Call => "call".into(),
            OsPkg::Clear => "clear".into(),
            OsPkg::CurrentDir => "current_dir".into(),
            OsPkg::Env => "env".into(),
        }
    }

    fn get_package_name(&self) -> String {
        "os".into()
    }

    fn get_description(&self) -> String {
        match self {
            OsPkg::Call => "Invokes an operating system application".into(),
            OsPkg::Clear => "Clears the terminal/screen".into(),
            OsPkg::CurrentDir => "Returns the current directory".into(),
            OsPkg::Env => "Returns a table of the OS environment variables".into(),
        }
    }

    fn get_examples(&self) -> Vec<String> {
        match self {
            OsPkg::Call => vec![strip_margin(
                r#"
                    |stocks = nsd::save(
                    |   "examples.os.call",
                    |    Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                    |)
                    |os::call("chmod", "777", oxide::home())
                "#,
                '|',
            )],
            OsPkg::Clear => vec!["os::clear()".into()],
            OsPkg::CurrentDir => vec![strip_margin(r#"
                    |cur_dir = os::current_dir()
                    |prefix = if(cur_dir::ends_with("core"), "../..", ".")
                    |path_str = prefix + "/demoes/language/include_file.oxide"
                    |include path_str
                "#,
                '|',
            )],
            OsPkg::Env => vec!["os::env()".into()],
        }
    }

    fn get_parameter_types(&self) -> Vec<DataType> {
        match self {
            // zero-parameter
            OsPkg::Call => vec![StringType],
            OsPkg::Clear | OsPkg::CurrentDir | OsPkg::Env => vec![],
        }
    }

    fn get_return_type(&self) -> DataType {
        match self {
            OsPkg::Call | OsPkg::CurrentDir => StringType,
            OsPkg::Clear => BooleanType,
            OsPkg::Env => TableType(OsPkg::get_os_env_parameters()),
        }
    }

    fn evaluate(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match self {
            OsPkg::Call => Self::do_os_call(ms, args),
            OsPkg::CurrentDir => extract_value_fn0(ms, args, Self::do_os_current_dir),
            OsPkg::Clear => extract_value_fn0(ms, args, Self::do_os_clear_screen),
            OsPkg::Env => extract_value_fn0(ms, args, Self::do_os_env),
        }
    }
}

/// Strings package
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum StringsPkg {
    EndsWith,
    Format,
    IndexOf,
    Join,
    Left,
    Len,
    Right,
    Split,
    StartsWith,
    StripMargin,
    Substring,
    SuperScript,
    ToLowercase,
    ToString,
    ToUppercase,
    Trim,
}

impl StringsPkg {
    fn do_str_ends_with(
        ms: Machine,
        string_value: &TypedValue,
        slice_value: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let src = pull_string(string_value)?;
        let slice = pull_string(slice_value)?;
        Ok((ms, Boolean(src.ends_with(slice.as_str()))))
    }

    /// Formats a string based on a template
    /// Ex: format("This {} the {}", "is", "way") => "This is the way"
    fn do_str_format(ms: Machine, args: Vec<TypedValue>) -> std::io::Result<(Machine, TypedValue)> {
        // internal parsing function
        fn format_text(
            ms: Machine,
            template: String,
            replacements: Vec<TypedValue>,
        ) -> (Machine, TypedValue) {
            let mut result = String::from(template);
            let mut replacement_iter = replacements.iter();

            // replace each placeholder "{}" with the next element from the vector
            while let Some(pos) = result.find("{}") {
                // get the next replacement, if available
                if let Some(replacement) = replacement_iter.next() {
                    result.replace_range(pos..pos + 2, replacement.unwrap_value().as_str());
                // Replace the "{}" with the replacement
                } else {
                    break; // no more replacements available, break out of the loop
                }
            }

            (ms, StringValue(result))
        }

        // parse the arguments
        if args.is_empty() {
            Ok((ms, StringValue(String::new())))
        } else {
            let format_str = pull_string(&args[0])?;
            let format_args = args[1..].to_owned();
            Ok(format_text(ms, format_str, format_args))
        }
    }

    /// str::index_of("Hello World", "World")
    fn do_str_index_of(
        ms: Machine,
        host_str: &TypedValue,
        search_str: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let host = pull_string(host_str)?;
        let search = pull_string(search_str)?;
        match host.find(search.as_str()) {
            None => Ok((ms, Undefined)),
            Some(index) => Ok((ms, Number(I64Value(index as i64)))),
        }
    }

    /// Combines a sequence into a String
    /// #### Examples
    /// ##### Arrays
    /// ```
    /// ["a", "b", "c"]::join(", ") => "a, b, c"
    /// ```
    /// ##### Tuples
    /// ```
    /// (1, 2, 3)::join(", ") => "1, 2, 3"
    /// ```
    fn do_str_join(
        ms: Machine,
        sequence: &TypedValue,
        delim: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let items = pull_sequence(sequence)?;
        let mut buf = String::new();
        for item in items.iter() {
            if !buf.is_empty() {
                buf.extend(delim.unwrap_value().chars())
            }
            buf.extend(item.unwrap_value().chars());
        }
        Ok((ms, StringValue(buf)))
    }

    fn do_str_left(
        ms: Machine,
        string: &TypedValue,
        n_chars: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let s = pull_string(string)?;
        match n_chars {
            Number(nv) if nv.to_i64() < 0 => {
                Self::do_str_right(ms.to_owned(), string, &Number(I64Value(-nv.to_i64())))
            }
            Number(nv) => Ok((ms, StringValue(s[0..nv.to_usize()].to_string()))),
            _ => Ok((ms, Undefined)),
        }
    }

    fn do_str_len(ms: Machine, string: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        let len = match string {
            ByteStringValue(b) => b.len(),
            CharValue(c) => c.len_utf8(),
            StringValue(s) => s.chars().count(),
            other => pull_string(other)?.len()
        };
        Ok((ms, Number(I64Value(len as i64))))
    }

    fn do_str_right(
        ms: Machine,
        string: &TypedValue,
        n_chars: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let s = pull_string(string)?;
        match n_chars {
            Number(nv) if nv.to_i64() < 0 => {
                Self::do_str_left(ms.to_owned(), string, &Number(I64Value(-nv.to_i64())))
            }
            Number(nv) => {
                let strlen = s.len();
                Ok((
                    ms,
                    StringValue(s[(strlen - nv.to_usize())..strlen].to_string()),
                ))
            }
            _ => Ok((ms, Undefined)),
        }
    }

    fn do_str_split(
        ms: Machine,
        string_v: &TypedValue,
        delimiter_v: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let src = pull_string(string_v)?;
        let delimiters = pull_string(delimiter_v)?;
        let pcs = src
            .split(|c| delimiters.contains(c))
            .map(|s| StringValue(s.to_string()))
            .collect::<Vec<_>>();
        Ok((ms, ArrayValue(Array::from(pcs))))
    }

    fn do_str_start_with(
        ms: Machine,
        string_value: &TypedValue,
        slice_value: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let src = pull_string(string_value)?;
        let slice = pull_string(slice_value)?;
        Ok((ms, Boolean(src.starts_with(slice.as_str()))))
    }

    fn do_str_strip_margin(
        ms: Machine,
        string_value: &TypedValue,
        margin_value: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let src = pull_string(string_value)?;
        let margin = pull_string(margin_value)?;
        if let Some(margin_char) = margin.chars().next() {
            Ok((ms, StringValue(strip_margin(src.as_str(), margin_char))))
        } else {
            throw(TypeMismatch(CharExpected(margin.into())))
        }
    }

    fn do_str_substring(
        ms: Machine,
        string: &TypedValue,
        a: &TypedValue,
        b: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        Ok((ms, match string {
            StringValue(s) => match (a, b) {
                (Number(na), Number(nb)) => {
                    StringValue(s[na.to_usize()..nb.to_usize()].to_string())
                }
                (..) => Undefined,
            },
            _ => Undefined,
        }))
    }
    
    fn do_str_superscript(
        ms: Machine,
        number: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        Ok((ms, StringValue(superscript(number.to_usize()))))
    }

    fn do_str_to_lowercase(
        ms: Machine,
        string_val: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        Ok((ms, StringValue(string_val.unwrap_value().to_lowercase())))
    }

    fn do_str_to_string(ms: Machine, value: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        let result = match value {
            ByteStringValue(bytes) =>
                String::from_utf8(bytes.to_vec())
                    .unwrap_or_else(|_| format!("0B{}", bytes.iter()
                        .map(|b| format!("{:02x}", b))
                        .collect::<Vec<_>>().join(""))),
            x => x.unwrap_value()
        };
        Ok((ms, StringValue(result)))
    }

    fn do_str_to_uppercase(
        ms: Machine,
        string_val: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        Ok((ms, StringValue(string_val.unwrap_value().to_uppercase())))
    }

    fn do_str_trim(
        ms: Machine,
        string_val: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        Ok((ms, StringValue(string_val.unwrap_value().trim().to_string())))
    }

    pub fn get_contents() -> Vec<PackageOps> {
        vec![
            PackageOps::Strings(StringsPkg::EndsWith),
            PackageOps::Strings(StringsPkg::Format),
            PackageOps::Strings(StringsPkg::IndexOf),
            PackageOps::Strings(StringsPkg::Join),
            PackageOps::Strings(StringsPkg::Left),
            PackageOps::Strings(StringsPkg::Len),
            PackageOps::Strings(StringsPkg::Right),
            PackageOps::Strings(StringsPkg::Split),
            PackageOps::Strings(StringsPkg::StartsWith),
            PackageOps::Strings(StringsPkg::StripMargin),
            PackageOps::Strings(StringsPkg::Substring),
            PackageOps::Strings(StringsPkg::SuperScript),
            PackageOps::Strings(StringsPkg::ToLowercase),
            PackageOps::Strings(StringsPkg::ToString),
            PackageOps::Strings(StringsPkg::ToUppercase),
            PackageOps::Strings(StringsPkg::Trim),
        ]
    }
}

impl Package for StringsPkg {
    fn get_name(&self) -> String {
        match self {
            StringsPkg::EndsWith => "ends_with".into(),
            StringsPkg::Format => "format".into(),
            StringsPkg::IndexOf => "index_of".into(),
            StringsPkg::Join => "join".into(),
            StringsPkg::Left => "left".into(),
            StringsPkg::Len => "len".into(),
            StringsPkg::Right => "right".into(),
            StringsPkg::Split => "split".into(),
            StringsPkg::StartsWith => "starts_with".into(),
            StringsPkg::StripMargin => "strip_margin".into(),
            StringsPkg::Substring => "substring".into(),
            StringsPkg::SuperScript => "superscript".into(),
            StringsPkg::ToLowercase => "to_lowercase".into(),
            StringsPkg::ToString => "to_string".into(),
            StringsPkg::ToUppercase => "to_uppercase".into(),
            StringsPkg::Trim => "trim".into(),
        }
    }

    fn get_package_name(&self) -> String {
        "str".into()
    }

    fn get_description(&self) -> String {
        (match self {
            StringsPkg::EndsWith => "Returns true if string `a` ends with string `b`",
            StringsPkg::Format => "Returns an argument-formatted string",
            StringsPkg::IndexOf => "Returns the index of string `b` in string `a`",
            StringsPkg::Join => "Combines an array into a string",
            StringsPkg::Left => "Returns n-characters from left-to-right",
            StringsPkg::Len => "Returns the number of characters contained in the string",
            StringsPkg::Right => "Returns n-characters from right-to-left",
            StringsPkg::Split => "Splits string `a` by delimiter string `b`",
            StringsPkg::StartsWith => "Returns true if string `a` starts with string `b`",
            StringsPkg::StripMargin => "Returns the string with all characters on each line are striped up to the margin character",
            StringsPkg::Substring => "Returns a substring of string `s` from `m` to `n`",
            StringsPkg::SuperScript => "Returns a superscript of a number `n`",
            StringsPkg::ToLowercase => "Converts a value to lowercase text-based representation",
            StringsPkg::ToString => "Converts a value to its text-based representation",
            StringsPkg::ToUppercase => "Converts a value to uppercase text-based representation",
            StringsPkg::Trim => "Trims whitespace from a string",
        }).into()
    }

    fn get_examples(&self) -> Vec<String> {
        match self {
            StringsPkg::EndsWith => vec![
                r#"'Hello World'::ends_with('World')"#.into()
            ],
            StringsPkg::Format => vec![
                r#""This {} the {}"::format("is", "way")"#.into()
            ],
            StringsPkg::IndexOf => vec![
                r#"'The little brown fox'::index_of('brown')"#.into()
            ],
            StringsPkg::Join => vec![
                r#"['1', 5, 9, '13']::join(', ')"#.into()
            ],
            StringsPkg::Left => vec![
                r#"'Hello World'::left(5)"#.into()
            ],
            StringsPkg::Len => vec![
                r#"'The little brown fox'::len()"#.into()
            ],
            StringsPkg::Right => vec![
                "'Hello World'::right(5)".into()
            ],
            StringsPkg::Split => vec![
                r#"'Hello,there World'::split(' ,')"#.into(),
            ],
            StringsPkg::StartsWith => vec![
                "'Hello World'::starts_with('World')".into()
            ],
            StringsPkg::StripMargin => vec![
                strip_margin(r#"
                    |"|Code example:
                    | |
                    | |stocks
                    | |where exchange is 'NYSE'
                    | |"::strip_margin('|')
                    |"#, '|')
            ],
            StringsPkg::Substring => vec![
                "'Hello World'::substring(0, 5)".into()
            ],
            StringsPkg::SuperScript => vec![
                "5::superscript()".into()
            ],
            StringsPkg::ToLowercase => vec![
                "'Hello'::to_lowercase".into()
            ],
            StringsPkg::ToString => vec![
                "125.75::to_string()".into()
            ],
            StringsPkg::ToUppercase => vec![
                "'Hello'::to_uppercase".into()
            ],
            StringsPkg::Trim => vec![
                "' hello '::trim".into()
            ],
        }
    }

    fn get_parameter_types(&self) -> Vec<DataType> {
        match self {
            // one-parameter (string)
            StringsPkg::Len
            | StringsPkg::ToLowercase
            | StringsPkg::ToUppercase
            | StringsPkg::Trim => vec![StringType],
            // two-parameter (string, string)
            StringsPkg::EndsWith
            | StringsPkg::Format
            | StringsPkg::Split
            | StringsPkg::StartsWith
            | StringsPkg::StripMargin => vec![StringType, StringType],
            // two-parameter (string, i64)
            StringsPkg::IndexOf | StringsPkg::Left | StringsPkg::Right => {
                vec![StringType, NumberType(I64Kind)]
            }
            // two-parameter (array, string)
            StringsPkg::Join => vec![
                ArrayType(RuntimeResolvedType.into()), StringType
            ],
            // three-parameter (string, i64, i64)
            StringsPkg::Substring => vec![StringType, NumberType(I64Kind), NumberType(I64Kind)],
            StringsPkg::SuperScript => vec![NumberType(I64Kind)],
            StringsPkg::ToString => vec![RuntimeResolvedType],
        }
    }

    fn get_return_type(&self) -> DataType {
        match self {
            // Boolean
            StringsPkg::EndsWith 
            | StringsPkg::StartsWith => BooleanType,
            // String
            StringsPkg::Format
            | StringsPkg::Join
            | StringsPkg::Left
            | StringsPkg::Right
            | StringsPkg::StripMargin
            | StringsPkg::Substring
            | StringsPkg::SuperScript
            | StringsPkg::ToLowercase
            | StringsPkg::ToString
            | StringsPkg::ToUppercase
            | StringsPkg::Trim => StringType,
            // Number
            StringsPkg::IndexOf 
            | StringsPkg::Len => NumberType(I64Kind),
            // Array of String
            StringsPkg::Split => ArrayType(StringType.into()),
        }
    }

    fn evaluate(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match self {
            StringsPkg::EndsWith => extract_value_fn2(ms, args, Self::do_str_ends_with),
            StringsPkg::Format => Self::do_str_format(ms, args),
            StringsPkg::IndexOf => extract_value_fn2(ms, args, Self::do_str_index_of),
            StringsPkg::Join => extract_value_fn2(ms, args, Self::do_str_join),
            StringsPkg::Left => extract_value_fn2(ms, args, Self::do_str_left),
            StringsPkg::Len => extract_value_fn1(ms, args, Self::do_str_len),
            StringsPkg::Right => extract_value_fn2(ms, args, Self::do_str_right),
            StringsPkg::Split => extract_value_fn2(ms, args, Self::do_str_split),
            StringsPkg::StartsWith => extract_value_fn2(ms, args, Self::do_str_start_with),
            StringsPkg::StripMargin => extract_value_fn2(ms, args, Self::do_str_strip_margin),
            StringsPkg::Substring => extract_value_fn3(ms, args, Self::do_str_substring),
            StringsPkg::SuperScript => extract_value_fn1(ms, args, Self::do_str_superscript),
            StringsPkg::ToLowercase => extract_value_fn1(ms, args, Self::do_str_to_lowercase),
            StringsPkg::ToString => extract_value_fn1(ms, args, Self::do_str_to_string),
            StringsPkg::ToUppercase => extract_value_fn1(ms, args, Self::do_str_to_uppercase),
            StringsPkg::Trim => extract_value_fn1(ms, args, Self::do_str_trim),
        }
    }
}

/// Tools package
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum ToolsPkg {
    Compact,
    Describe,
    Fetch,
    Filter,
    Latest,
    Len,
    Keys,
    Map,
    Pop,
    Push,
    Reverse,
    Scan,
    Shuffle,
    ToArray,
    ToCSV,
    ToJSON,
    ToTable,
}

impl ToolsPkg {
    fn do_tools_compact(ms: Machine, table: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        let mut df = table.to_dataframe()?;
        Ok((ms, Number(I64Value(df.compact()?))))
    }

    fn do_tools_describe(ms: Machine, item: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        item.to_dataframe().map(|df| (ms, df.describe()))
    }

    /// Retrieves a raw structure from a table
    /// #### Examples
    /// ```
    /// stocks::fetch(5)
    /// ```
    fn do_tools_fetch(
        ms: Machine,
        table: &TypedValue,
        row_offset: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let offset = row_offset.to_usize();
        let df = table.to_dataframe()?;
        let columns = df.get_columns();
        let (row, _) = df.read_row(offset)?;
        Ok((ms, TableValue(ModelTable(ModelRowCollection::from_columns_and_rows(
            columns, &vec![row],
        )))))
    }

    pub fn do_tools_filter(
        ms: Machine,
        items: &TypedValue,
        function: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match function {
            Function { .. } => {
                // define the filtering function
                let filter = |item: TypedValue, result: TypedValue| match result {
                    Boolean(is_true) => Ok(if is_true { Some(item) } else { None }),
                    z => throw(TypeMismatch(BooleanExpected(z.to_code()))),
                };

                // apply the function to every element in the array
                match items {
                    ByteStringValue(bytes) =>
                        PackageOps::apply_fn_over_vec(ms, &u8_vec_to_values(bytes), function, filter,
                                                      |items| ByteStringValue(values_to_u8_vec(&items))),
                    _ =>
                        match items.to_sequence()? {
                            TheArray(array) =>
                                PackageOps::apply_fn_over_vec(ms, &array.get_values(), function, filter,
                                                              |items| ArrayValue(Array::from(items))),
                            TheDataframe(df) =>
                                PackageOps::apply_fn_over_table(ms, &df, function, filter),
                            TheRange(..) => Self::do_tools_filter(ms, &items.to_array()?, function),
                            TheTuple(items) =>
                                PackageOps::apply_fn_over_vec(ms, &items, function, filter,
                                                              |items| TupleValue(items)),
                        }
                }
            }
            z => throw(TypeMismatch(FunctionExpected(z.to_code()))),
        }
    }
    
    fn do_tools_keys(ms: Machine, value: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        let params = match value  {
            Structured(s) => s.get_parameters(),
            TableValue(df) => df.get_parameters(),
            other => return throw(TypeMismatch(ParameterExpected(other.to_code()))),
        };
        let names = params.iter()
            .map(|param| StringValue(param.get_name().into()))
            .collect();
        Ok((ms, ArrayValue(Array::from(names))))
    }

    fn do_tools_latest(ms: Machine, value: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        let result = match value.to_sequence()? {
            TheArray(..) => Undefined,
            TheDataframe(df) =>
                match df.find_last_active_row_id() {
                    Ok(Some(id)) => Number(I64Value(id as i64)),
                    Ok(None) => Undefined,
                    Err(err) => ErrorValue(Exact(err.to_string()))
                }
            TheRange(..) => Undefined,
            TheTuple(..) => Undefined,
        };
        Ok((ms, result))
    }

    fn do_tools_length(ms: Machine, value: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        let result = match value.to_sequence()? {
            TheArray(array) => Number(I64Value(array.len() as i64)),
            TheDataframe(df) => Number(I64Value(df.len()? as i64)),
            TheRange(a, b, incl) => range_diff(&a, &b, incl),
            TheTuple(tuple) => Number(I64Value(tuple.len() as i64)),
        };
        Ok((ms, result))
    }

    pub(crate) fn do_tools_map(
        ms: Machine,
        items: &TypedValue,
        function: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match function {
            Function { .. } => match items.to_sequence()? {
                TheArray(array) => {
                    PackageOps::apply_fn_over_vec(ms, &array.get_values(), function, |item, result| {
                        Ok(Some(result))
                    }, |items| ArrayValue(Array::from(items)))
                }
                TheDataframe(df) => {
                    PackageOps::apply_fn_over_table(ms, &df, function, |item, result| {
                        Ok(Some(result))
                    })
                }
                TheRange(..) => Self::do_tools_map(ms, &items.to_array()?, function),
                TheTuple(items) => {
                    PackageOps::apply_fn_over_vec(ms, &items, function, |item, result| {
                        Ok(Some(result))
                    }, |items| TupleValue(items))
                }
            },
            z => throw(TypeMismatch(FunctionExpected(z.to_code()))),
        }
    }

    pub(crate) fn do_tools_pop(
        ms: Machine,
        value: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match value.to_sequence()? {
            TheDataframe(mut df) => df
                .pop_row()
                .to_dataframe()
                .map(|df| (ms, TableValue(df))),
            TheArray(..) => ArraysPkg::do_arrays_pop(ms, value),
            TheRange(..) => throw(UnsupportedFeature("Range::pop()".into())),
            TheTuple(..) => throw(UnsupportedFeature("Tuple::pop()".into())),
        }
    }

    pub(crate) fn do_tools_push(
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let (seq_like, row_like) = TypedValue::parse_two_args(args)?;
        match row_like {
            TupleValue(vv) => {
                let seq = seq_like.to_sequence()?;
                let result = match seq {
                    TheDataframe(mut df) => Number(U64Value(df.push_row(Row::new(0, vv))?)),
                    TheArray(mut arr) => arr.push(TupleValue(vv)),
                    TheRange(..) => return throw(UnsupportedFeature("Range::push()".into())),
                    TheTuple(mut tpl) => {
                        tpl.push(TupleValue(vv));
                        TupleValue(tpl)
                    }
                };
                Ok((ms, result))
            }
            Structured(structure) => {
                let seq = seq_like.to_sequence()?;
                let result = match seq {
                    TheDataframe(mut df) => Number(U64Value(
                        df.push_row(Structures::transform_row(
                            &structure.get_parameters(),
                            &structure.get_values(),
                            &df.get_parameters()
                        ))?
                    )),
                    TheArray(mut arr) => arr.push(Structured(structure)),
                    TheRange(..) => return throw(UnsupportedFeature("Range::push()".into())),
                    TheTuple(mut tpl) => {
                        tpl.push(Structured(structure));
                        TupleValue(tpl)
                    }
                };
                Ok((ms, result))
            }
            z => throw(TypeMismatch(StructExpected(z.to_code()))),
        }
    }

    fn do_tools_reverse(ms: Machine, value: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        match value {
            StringValue(s) => Ok((ms, StringValue(s.chars().rev().collect()))),
            _ => match value.to_sequence()? {
                TheArray(a) => Ok((ms, ArrayValue(a.rev()))),
                TheDataframe(df) => Ok((ms, df.reverse_table_value()?)),
                TheRange(..) => Self::do_tools_reverse(ms, &value.to_array()?),
                TheTuple(items) => Ok((ms, TupleValue(items.iter().rev().cloned().collect()))),
            }
        }
    }

    fn do_tools_scan(ms: Machine, tv_table: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        let df = tv_table.to_dataframe()?;
        let rows = df.examine_rows()?;
        let columns = rows
            .first()
            .map(|row| df.get_columns().to_owned())
            .unwrap_or(Vec::new());
        let mrc = ModelRowCollection::from_columns_and_rows(&columns, &rows);
        Ok((ms, TableValue(ModelTable(mrc))))
    }
    
    fn do_tools_shuffle(ms: Machine, tv_table: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        let mut df = tv_table.to_dataframe()?;
        Ok((ms, Boolean(df.shuffle()?)))
    }

    fn do_tools_to_array(ms: Machine, value: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        match value {
            UUIDValue(v) => Ok((ms, ArrayValue(Array::from(u8_vec_to_values(&v.to_be_bytes().to_vec()))))),
            _ => Ok((ms, ArrayValue(value.to_sequence()?.to_array())))
        }
    }

    fn do_tools_to_csv(ms: Machine, value: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        Self::do_tools_to_xxx(ms, value, DataFormats::CSV)
    }

    fn do_tools_to_json(ms: Machine, value: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        Self::do_tools_to_xxx(ms, value, DataFormats::JSON)
    }

    fn do_tools_to_xxx(
        ms: Machine,
        value: &TypedValue,
        format: DataFormats,
    ) -> std::io::Result<(Machine, TypedValue)> {
        /// transform the [RowCollection] into CSV or JSON
        let rc: Box<dyn RowCollection> = value.to_table()?;
        match format {
            DataFormats::CSV => Ok((ms, ArrayValue(Array::from(
                rc.iter()
                    .map(|row| StringValue(row.to_csv()))
                    .collect::<Vec<_>>(),
            )))),
            DataFormats::JSON => Ok((ms, ArrayValue(Array::from(
                rc.iter()
                    .map(|row| row.to_json_string(rc.get_columns()))
                    .map(StringValue)
                    .collect::<Vec<_>>(),
            )))),
        }
    }

    fn do_tools_to_table(
        ms: Machine,
        value: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let rc = value.to_table()?;
        let columns = rc.get_columns();
        let rows = rc.read_active_rows()?;
        let mrc = ModelRowCollection::from_columns_and_rows(columns, &rows);
        Ok((ms, TableValue(ModelTable(mrc))))
    }

    pub fn get_contents() -> Vec<PackageOps> {
        vec![
            PackageOps::Tools(ToolsPkg::Compact),
            PackageOps::Tools(ToolsPkg::Describe),
            PackageOps::Tools(ToolsPkg::Fetch),
            PackageOps::Tools(ToolsPkg::Filter),
            PackageOps::Tools(ToolsPkg::Latest),
            PackageOps::Tools(ToolsPkg::Len),
            PackageOps::Tools(ToolsPkg::Map),
            PackageOps::Tools(ToolsPkg::Pop),
            PackageOps::Tools(ToolsPkg::Push),
            PackageOps::Tools(ToolsPkg::Reverse),
            PackageOps::Tools(ToolsPkg::Scan),
            PackageOps::Tools(ToolsPkg::Shuffle),
            PackageOps::Tools(ToolsPkg::ToArray),
            PackageOps::Tools(ToolsPkg::ToCSV),
            PackageOps::Tools(ToolsPkg::ToJSON),
            PackageOps::Tools(ToolsPkg::ToTable),
        ]
    }    

    pub fn get_tools_describe_parameters() -> Vec<Parameter> {
        vec![
            Parameter::new("name", FixedSizeType(StringType.into(), 128)),
            Parameter::new("type", FixedSizeType(StringType.into(), 128)),
            Parameter::new("default_value", FixedSizeType(StringType.into(), 128)),
            Parameter::new("is_nullable", BooleanType),
        ]
    }
}

impl Package for ToolsPkg {
    fn get_name(&self) -> String {
        (match self {
            ToolsPkg::Compact => "compact",
            ToolsPkg::Describe => "describe",
            ToolsPkg::Fetch => "fetch",
            ToolsPkg::Filter => "filter",
            ToolsPkg::Latest => "latest",
            ToolsPkg::Keys => "keys",
            ToolsPkg::Len => "len",
            ToolsPkg::Map => "map",
            ToolsPkg::Pop => "pop",
            ToolsPkg::Push => "push",
            ToolsPkg::Reverse => "reverse",
            ToolsPkg::Scan => "scan",
            ToolsPkg::Shuffle => "shuffle",
            ToolsPkg::ToArray => "to_array",
            ToolsPkg::ToCSV => "to_csv",
            ToolsPkg::ToJSON => "to_json",
            ToolsPkg::ToTable => "to_table",
        }).into()
    }

    fn get_package_name(&self) -> String {
        "tools".into()
    }

    fn get_description(&self) -> String {
        (match self {
            ToolsPkg::Compact => "Shrinks a table by removing deleted rows",
            ToolsPkg::Describe => "Describes a table or structure",
            ToolsPkg::Fetch => "Retrieves a raw structure from a table",
            ToolsPkg::Filter => "Filters a collection based on a function",
            ToolsPkg::Keys => "returns the keys of a structure (column names of a table)",
            ToolsPkg::Latest => "Returns the row_id of last inserted record",
            ToolsPkg::Len => "Returns the length of a table",
            ToolsPkg::Map => "Transform a collection based on a function",
            ToolsPkg::Pop => "Removes and returns a value or object from a Sequence",
            ToolsPkg::Push => "Appends a value or object to a Sequence",
            ToolsPkg::Reverse => "Returns a reverse copy of a table, string or array",
            ToolsPkg::Scan => "Returns existence metadata for a table",
            ToolsPkg::Shuffle => "Shuffles a collection in random order",
            ToolsPkg::ToArray => "Converts a collection into an array",
            ToolsPkg::ToCSV => "Converts a collection to CSV format",
            ToolsPkg::ToJSON => "Converts a collection to JSON format",
            ToolsPkg::ToTable => "Converts an object into a to_table",
        }).into()
    }

    fn get_examples(&self) -> Vec<String> {
        match self {
            // tools
            ToolsPkg::Compact => vec![
                strip_margin(r#"
                    |stocks = nsd::save(
                    |   "examples.compact.stocks",
                    |   Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                    |)
                    |[{ symbol: "DMX", exchange: "NYSE", last_sale: 99.99 },
                    | { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                    | { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                    | { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                    | { symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
                    | { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 },
                    | { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
                    |delete stocks where last_sale > 1.0
                    |stocks
                "#, '|')
            ],
            ToolsPkg::Describe => vec![
                strip_margin(r#"
                    |{
                    |   symbol: "BIZ",
                    |   exchange: "NYSE",
                    |   last_sale: 23.66
                    |}::describe()
                "#, '|'),
                strip_margin(r#"
                    |stocks =
                    |    |--------------------------------------|
                    |    | symbol | exchange | last_sale | rank |
                    |    |--------------------------------------|
                    |    | BOOM   | NYSE     | 113.76    | 1    |
                    |    | ABC    | AMEX     | 24.98     | 2    |
                    |    | JET    | NASDAQ   | 64.24     | 3    |
                    |    |--------------------------------------|
                    |stocks::describe()
                "#, '|')
            ],
            ToolsPkg::Fetch => vec![
                strip_margin(r#"
                    |stocks = nsd::save(
                    |   "examples.fetch.stocks",
                    |   Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                    |)
                    |[{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                    | { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                    | { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
                    |stocks::fetch(2)
                "#, '|')
            ],
            ToolsPkg::Filter => vec![
                strip_margin(r#"
                    |(1..11)::filter(n -> (n % 2) == 0)
                "#, '|')
            ],
            ToolsPkg::Keys => vec![
                strip_margin(r#"
                    |stocks = 
                    |    |--------------------------------|
                    |    | symbol | exchange  | last_sale |
                    |    |--------------------------------|
                    |    | GIF    | NYSE      | 11.75     |
                    |    | TRX    | NASDAQ    | 32.96     |
                    |    | SHMN   | OTCBB     | 5.02      |
                    |    | XCD    | OTCBB     | 1.37      |
                    |    | DRMQ   | OTHER_OTC | 0.02      |
                    |    | JTRQ   | OTHER_OTC | 0.0001    |
                    |    |--------------------------------|
                    |stocks::keys()
                "#, '|')
            ],
            ToolsPkg::Latest => vec![
                strip_margin(r#"
                    |stocks = nsd::save(
                    |   "packages.tools_latest.stocks",
                    |    |--------------------------------|
                    |    | symbol | exchange  | last_sale |
                    |    |--------------------------------|
                    |    | GIF    | NYSE      | 11.75     |
                    |    | TRX    | NASDAQ    | 32.96     |
                    |    | SHMN   | OTCBB     | 5.02      |
                    |    | XCD    | OTCBB     | 1.37      |
                    |    | DRMQ   | OTHER_OTC | 0.02      |
                    |    | JTRQ   | OTHER_OTC | 0.0001    |
                    |    |--------------------------------|
                    |)
                    |delete stocks where last_sale < 1
                    |row_id = stocks::latest()
                    |stocks[row_id]
                "#, '|')
            ],
            ToolsPkg::Len => vec![
                strip_margin(r#"
                    |stocks = nsd::save(
                    |   "examples.table_len.stocks",
                    |   [{ symbol: "WKRP", exchange: "NYSE", last_sale: 11.11 },
                    |    { symbol: "ACDC", exchange: "AMEX", last_sale: 35.11 },
                    |    { symbol: "UELO", exchange: "NYSE", last_sale: 90.12 }] 
                    |)
                    |stocks::len()
                "#,
                '|')
            ],
            ToolsPkg::Map => vec![
                strip_margin(r#"
                    |stocks = nsd::save(
                    |   "examples.map_over_table.stocks",
                    |   Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                    |)
                    |[{ symbol: "WKRP", exchange: "NYSE", last_sale: 11.11 },
                    | { symbol: "ACDC", exchange: "AMEX", last_sale: 35.11 },
                    | { symbol: "UELO", exchange: "NYSE", last_sale: 90.12 }] ~> stocks
                    |stocks::map(row -> {
                    |    symbol: symbol,
                    |    exchange: exchange,
                    |    last_sale: last_sale,
                    |    processed_time: DateTime::new()
                    |})
                "#, '|')
            ],
            ToolsPkg::Pop => vec![
                strip_margin(r#"
                    |stocks = nsd::save(
                    |   "examples.tools_pop.stocks",
                    |   Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                    |)
                    |[{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                    | { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                    | { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
                    |stocks::pop()
                "#, '|')
            ],
            ToolsPkg::Push => vec![
                strip_margin(r#"
                    |stocks = nsd::save(
                    |   "examples.tools_push.stocks",
                    |   Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                    |)
                    |[{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                    | { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                    | { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
                    |stocks::push({ symbol: "XYZ", exchange: "NASDAQ", last_sale: 24.78 })
                    |stocks
                "#, '|')
            ],
            ToolsPkg::Reverse => vec![
                strip_margin(r#"
                    |['cat', 'dog', 'ferret', 'mouse']::reverse::to_table
                "#, '|')
            ],
            ToolsPkg::Scan => vec![
                strip_margin(r#"
                    |stocks = nsd::save(
                    |   "examples.scan.stocks",
                    |   Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                    |)
                    |[{ symbol: "ABC", exchange: "AMEX", last_sale: 12.33 },
                    | { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                    | { symbol: "BIZ", exchange: "NYSE", last_sale: 9.775 },
                    | { symbol: "GOTO", exchange: "OTC", last_sale: 0.1442 },
                    | { symbol: "XYZ", exchange: "NYSE", last_sale: 0.0289 }] ~> stocks
                    |delete stocks where last_sale > 1.0
                    |stocks::scan()
                "#, '|')
            ],
            ToolsPkg::Shuffle => vec![
                strip_margin(r#"
                    |stocks = nsd::save(
                    |   "examples.shuffle.stocks",
                    |   Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                    |)
                    |[{ symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
                    | { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                    | { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                    | { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                    | { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }] ~> stocks
                    |stocks::shuffle()
                    |stocks
                "#, '|')                
            ],
            ToolsPkg::ToArray => vec![
                r#""Hello"::to_array"#.into()
            ],
            ToolsPkg::ToCSV => vec![
                strip_margin(r#"
                    |stocks = nsd::save(
                    |   "examples.csv.stocks",
                    |   Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                    |)
                    |[{ symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
                    | { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                    | { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                    | { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                    | { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }] ~> stocks
                    |stocks::to_csv()
                "#, '|')
            ],
            ToolsPkg::ToJSON => vec![
                strip_margin(r#"
                    |stocks = nsd::save(
                    |   "examples.json.stocks",
                    |   Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                    |)
                    |[{ symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
                    | { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                    | { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                    | { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                    | { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }] ~> stocks
                    |stocks::to_json()
                "#, '|')
            ],
            ToolsPkg::ToTable => vec![
                strip_margin(r#"
                    |['cat', 'dog', 'ferret', 'mouse']::to_table()
                "#, '|')
            ],
        }
    }

    fn get_parameter_types(&self) -> Vec<DataType> {
        match self {
            // Boolean
            ToolsPkg::Shuffle => vec![BooleanType],
            // Table
            ToolsPkg::Compact
            | ToolsPkg::Describe
            | ToolsPkg::Keys
            | ToolsPkg::Latest
            | ToolsPkg::Len
            | ToolsPkg::Pop
            | ToolsPkg::Reverse
            | ToolsPkg::Scan
            | ToolsPkg::ToCSV
            | ToolsPkg::ToJSON => vec![TableType(vec![])],
            // (Table, Number)
            ToolsPkg::Fetch => vec![TableType(vec![]), NumberType(I64Kind)],
            // Runtime
            ToolsPkg::ToArray
            | ToolsPkg::ToTable => vec![RuntimeResolvedType],
            // (Runtime, Runtime)
            ToolsPkg::Filter
            | ToolsPkg::Map
            | ToolsPkg::Push => vec![RuntimeResolvedType, RuntimeResolvedType]
        }
    }

    fn get_return_type(&self) -> DataType {
        match self {
            // Array
            ToolsPkg::Keys => ArrayType(StringType.into()),
            // Boolean
            ToolsPkg::Push
            | ToolsPkg::Shuffle => BooleanType,
            // Number
            ToolsPkg::Latest
            | ToolsPkg::Len => NumberType(I64Kind),
            // Structure
            ToolsPkg::Pop => StructureType(vec![]),
            // Table
            ToolsPkg::Compact
            | ToolsPkg::Fetch
            | ToolsPkg::Filter
            | ToolsPkg::Map
            | ToolsPkg::Reverse
            | ToolsPkg::Scan
            | ToolsPkg::ToArray
            | ToolsPkg::ToCSV
            | ToolsPkg::ToJSON
            | ToolsPkg::ToTable => TableType(vec![]),
            ToolsPkg::Describe => TableType(ToolsPkg::get_tools_describe_parameters()),
        }
    }

    fn evaluate(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match self {
            ToolsPkg::Compact => extract_value_fn1(ms, args, Self::do_tools_compact),
            ToolsPkg::Describe => extract_value_fn1(ms, args, Self::do_tools_describe),
            ToolsPkg::Fetch => extract_value_fn2(ms, args, Self::do_tools_fetch),
            ToolsPkg::Filter => extract_value_fn2(ms, args, Self::do_tools_filter),
            ToolsPkg::Keys => extract_value_fn1(ms, args, Self::do_tools_keys),
            ToolsPkg::Latest => extract_value_fn1(ms, args, Self::do_tools_latest),
            ToolsPkg::Len => extract_value_fn1(ms, args, Self::do_tools_length),
            ToolsPkg::Map => extract_value_fn2(ms, args, Self::do_tools_map),
            ToolsPkg::Pop => extract_value_fn1(ms, args, Self::do_tools_pop),
            ToolsPkg::Push => Self::do_tools_push(ms, args),
            ToolsPkg::Reverse => extract_value_fn1(ms, args, Self::do_tools_reverse),
            ToolsPkg::Scan => extract_value_fn1(ms, args, Self::do_tools_scan),
            ToolsPkg::Shuffle => extract_value_fn1(ms, args, Self::do_tools_shuffle),
            ToolsPkg::ToArray => extract_value_fn1(ms, args, Self::do_tools_to_array),
            ToolsPkg::ToCSV => extract_value_fn1(ms, args, Self::do_tools_to_csv),
            ToolsPkg::ToJSON => extract_value_fn1(ms, args, Self::do_tools_to_json),
            ToolsPkg::ToTable => extract_value_fn1(ms, args, Self::do_tools_to_table),
        }
    }
}

/// Utils package
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum UtilsPkg {
    Base62Decode,
    Base62Encode,
    Base64Decode,
    Base64Encode,
    Gzip,
    Gunzip,
    Hex,
    MD5,
    Random,
    Round,
    To,
    ToASCII,
    ToBytes,
    ToDate,
    ToU8,
    ToF64,
    ToI64,
    ToU64,
    ToI128,
    ToU128,
}

impl UtilsPkg {
    fn adapter_pf_fn1<F>(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
        f: F,
    ) -> std::io::Result<(Machine, TypedValue)>
    where
        F: Fn(Machine, &TypedValue, &UtilsPkg) -> std::io::Result<(Machine, TypedValue)>,
    {
        match args.as_slice() {
            [a] => f(ms, a, self),
            args => throw(TypeMismatch(ArgumentsMismatched(1, args.len()))),
        }
    }

    fn do_util_base62_decode(ms: Machine, a: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        let v = base62::decode(a.to_bytes()).map_err(|e| cnv_error!(e))?;
        Ok((ms, ByteStringValue(v.to_be_bytes().to_vec())))
    }

    fn do_util_base62_encode(ms: Machine, a: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        Ok((ms, StringValue(base62::encode(a.to_u128()))))
    }

    fn do_util_base64_decode(ms: Machine, a: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        let bytes = base64::decode(a.to_bytes()).map_err(|e| cnv_error!(e))?;
        Ok((ms, ByteStringValue(bytes)))
    }

    fn do_util_base64_encode(ms: Machine, a: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        Ok((ms, StringValue(base64::encode(a.to_bytes()))))
    }
    
    fn do_util_to_bytes(
        ms: Machine, 
        value: &TypedValue
    ) -> std::io::Result<(Machine, TypedValue)> {
        let result = match value.clone() {
            ArrayValue(array) => array.get_values().iter()
                .map(|v| v.to_u8()).collect::<Vec<_>>(),
            BLOBValue(b) => b.read_bytes()?,
            ByteStringValue(b) => b,
            CharValue(c) => {
                let mut buf = [0; 4]; 
                let s = c.encode_utf8(&mut buf); 
                s.as_bytes().to_vec()
            }
            DateTimeValue(t) => t.to_be_bytes().to_vec(),
            Number(n) => n.encode(),
            StringValue(s) => s.bytes().collect(),
            TableValue(df) => df.to_bytes(),
            UUIDValue(uuid) => uuid.to_be_bytes().to_vec(),
            z => return throw(Exact(format!("{} cannot be converted to a ByteString", z.get_type_decl())))
        };
        Ok((ms, ByteStringValue(result)))
    }

    fn do_util_gzip(ms: Machine, a: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        use flate2::write::GzEncoder;
        use flate2::Compression;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(a.to_bytes().as_slice())?;
        Ok((ms, ByteStringValue(encoder.finish()?.to_vec())))
    }

    fn do_util_gunzip(ms: Machine, a: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        use flate2::read::GzDecoder;
        let bytes = a.to_bytes();
        let mut decoder = GzDecoder::new(bytes.as_slice());
        let mut output = Vec::new();
        decoder.read_to_end(&mut output)?;
        Ok((ms, ByteStringValue(output)))
    }

    fn do_util_numeric_conv(
        ms: Machine,
        value: &TypedValue,
        plat: &UtilsPkg,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let result = match plat {
            UtilsPkg::ToDate => DateTimeValue(value.to_i64()),
            UtilsPkg::ToF64 => Number(F64Value(value.to_f64())),
            UtilsPkg::ToI64 => Number(I64Value(value.to_i64())),
            UtilsPkg::ToI128 => Number(I128Value(value.to_i128())),
            UtilsPkg::ToU8 => Number(U8Value(value.to_u8())),
            UtilsPkg::ToU64 => Number(U64Value(value.to_u64())),
            UtilsPkg::ToU128 => Number(U128Value(value.to_u128())),
            plat => return throw(UnsupportedPlatformOps(PackageOps::Utils(plat.to_owned()))),
        };
        Ok((ms, result))
    }

    fn do_util_md5(ms: Machine, value: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        match md5::compute(value.to_bytes()) {
            md5::Digest(bytes) => Ok((ms, ByteStringValue(bytes.to_vec()))),
        }
    }
    
    fn do_util_random(
        ms: Machine,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let mut rng: ThreadRng = thread_rng();
        Ok((ms, Number(U64Value(rng.next_u64())),))
    }

    fn do_util_round(
        ms: Machine,
        value: &TypedValue,
        places: &TypedValue
    ) -> std::io::Result<(Machine, TypedValue)> {
        use rust_decimal::Decimal;
        use rust_decimal::prelude::ToPrimitive;
        use num_traits::FromPrimitive;

        let result = Decimal::from_f64(value.to_f64())
            .and_then(|decimal| decimal.round_dp(places.to_u32()).to_f64())
            .and_then(|rounded| Some(Number(F64Value(rounded))))
            .unwrap_or(Undefined);
        Ok((ms, result))
    }

    fn do_util_to_ascii(ms: Machine, value: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        Ok((ms, StringValue(format!("{}", value.to_u8() as char))))
    }

    fn do_util_to_hex(ms: Machine, value: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        Ok((ms, StringValue(format!("{}", StringValue(hex::encode(value.to_bytes()))))))
    }
    
    fn do_util_to_xxx(
        ms: Machine, 
        value: &TypedValue, 
        to_type: &TypedValue
    ) -> std::io::Result<(Machine, TypedValue)> {
        match to_type { 
            Kind(data_type) => Ok((ms, value.convert_to(data_type)?)),
            other => {
                let data_type = DataType::decipher_type(&Literal(other.clone()))?;
                Ok((ms, value.convert_to(&data_type)?))
            },
        }
    }

    pub(crate) fn get_contents() -> Vec<PackageOps> {
        vec![
            PackageOps::Utils(UtilsPkg::Base62Decode),
            PackageOps::Utils(UtilsPkg::Base64Decode),
            PackageOps::Utils(UtilsPkg::Base62Encode),
            PackageOps::Utils(UtilsPkg::Base64Encode),
            PackageOps::Utils(UtilsPkg::Gzip),
            PackageOps::Utils(UtilsPkg::Gunzip),
            PackageOps::Utils(UtilsPkg::Hex),
            PackageOps::Utils(UtilsPkg::MD5),
            PackageOps::Utils(UtilsPkg::Random),
            PackageOps::Utils(UtilsPkg::Round),
            PackageOps::Utils(UtilsPkg::To),
            PackageOps::Utils(UtilsPkg::ToASCII),
            PackageOps::Utils(UtilsPkg::ToBytes),
            PackageOps::Utils(UtilsPkg::ToDate),
            PackageOps::Utils(UtilsPkg::ToU8),
            PackageOps::Utils(UtilsPkg::ToF64),
            PackageOps::Utils(UtilsPkg::ToI64),
            PackageOps::Utils(UtilsPkg::ToU64),
            PackageOps::Utils(UtilsPkg::ToI128),
            PackageOps::Utils(UtilsPkg::ToU128),
        ]
    }
}

impl Package for UtilsPkg {
    fn get_name(&self) -> String {
        (match self {
            UtilsPkg::Base62Decode => "base62_decode",
            UtilsPkg::Base64Decode => "base64_decode",
            UtilsPkg::Base62Encode => "base62_encode",
            UtilsPkg::Base64Encode => "base64_encode",
            UtilsPkg::Gzip => "gzip",
            UtilsPkg::Gunzip => "gunzip",
            UtilsPkg::Hex => "hex",
            UtilsPkg::MD5 => "md5",
            UtilsPkg::Random => "random",
            UtilsPkg::Round => "round",
            UtilsPkg::To => "to",
            UtilsPkg::ToASCII => "to_ascii",
            UtilsPkg::ToBytes => "to_bytes",
            UtilsPkg::ToDate => "to_date",
            UtilsPkg::ToF64 => "to_f64",
            UtilsPkg::ToI64 => "to_i64",
            UtilsPkg::ToI128 => "to_i128",
            UtilsPkg::ToU8 => "to_u8",
            UtilsPkg::ToU64 => "to_u64",
            UtilsPkg::ToU128 => "to_u128",
        }).into()
    }

    fn get_package_name(&self) -> String {
        "util".into()
    }

    fn get_description(&self) -> String {
        (match self {
            UtilsPkg::Base62Decode => "Converts a Base62 string to binary",
            UtilsPkg::Base64Decode => "Converts a Base64 string to binary",
            UtilsPkg::Base62Encode => "Converts ASCII to Base62",
            UtilsPkg::Base64Encode => "Translates bytes into Base 64",
            UtilsPkg::Gzip => "Compresses bytes via gzip",
            UtilsPkg::Gunzip => "Decompresses bytes via gzip",
            UtilsPkg::Hex => "Translates bytes into hexadecimal",
            UtilsPkg::MD5 => "Creates a MD5 digest",
            UtilsPkg::Random => "Returns a random numeric value",
            UtilsPkg::Round => "Rounds a Float to a specific number of decimal places",
            UtilsPkg::To => "Converts a value to the desired type",
            UtilsPkg::ToASCII => "Converts an integer to ASCII",
            UtilsPkg::ToBytes => "Converts a value to a ByteString",
            UtilsPkg::ToDate => "Converts a value to DateTime",
            UtilsPkg::ToF64 => "Converts a value to f64",
            UtilsPkg::ToI64 => "Converts a value to i64",
            UtilsPkg::ToI128 => "Converts a value to i128",
            UtilsPkg::ToU8 => "Converts a value to u8",
            UtilsPkg::ToU64 => "Converts a value to u64",
            UtilsPkg::ToU128 => "Converts a value to u128",
        }).into()
    }

    fn get_examples(&self) -> Vec<String> {
        match self {
            UtilsPkg::Base62Decode => vec![
                "'Hello World'::base62_encode::base62_decode::to_string".into()
            ],
            UtilsPkg::Base64Decode => vec![
                "'Hello World'::base64_encode::base64_decode::to_string".into()
            ],
            UtilsPkg::Base62Encode => vec!["'Hello World'::base62_encode".into()],
            UtilsPkg::Base64Encode => vec!["'Hello World'::base64_encode".into()],
            UtilsPkg::Gzip => vec!["util::gzip('Hello World')".into()],
            UtilsPkg::Gunzip => vec!["util::gunzip(util::gzip('Hello World'))".into()],
            UtilsPkg::Hex => vec!["util::hex('Hello World')".into()],
            UtilsPkg::MD5 => vec!["util::md5('Hello World')".into()],
            UtilsPkg::Random => vec!["util::random()".into()],
            UtilsPkg::Round => vec!["util::round(1.42857, 2)".into()],
            UtilsPkg::To => vec![],
            UtilsPkg::ToASCII => vec!["util::to_ascii(177)".into()],
            UtilsPkg::ToBytes => vec!["'The little brown fox'::to_bytes".into()],
            UtilsPkg::ToDate => vec!["util::to_date(177)".into()],
            UtilsPkg::ToF64 => vec!["util::to_f64(4321)".into()],
            UtilsPkg::ToI64 => vec!["util::to_i64(88)".into()],
            UtilsPkg::ToI128 => vec!["util::to_i128(88)".into()],
            UtilsPkg::ToU8 => vec!["util::to_u8(257)".into()],
            UtilsPkg::ToU64 => vec!["util::to_u64(88)".into()],
            UtilsPkg::ToU128 => vec!["util::to_u128(88)".into()],
        }
    }

    fn get_parameter_types(&self) -> Vec<DataType> {
        match self {
            UtilsPkg::Random => vec![],
            UtilsPkg::To => vec![RuntimeResolvedType, RuntimeResolvedType],
            UtilsPkg::ToASCII => vec![NumberType(I64Kind)],
            _ => vec![RuntimeResolvedType],
        }
    }

    fn get_return_type(&self) -> DataType {
        match self {
            // ByteString
            UtilsPkg::Base62Decode
            | UtilsPkg::Base64Decode
            | UtilsPkg::Gzip
            | UtilsPkg::Gunzip
            | UtilsPkg::ToBytes => ByteStringType,
            UtilsPkg::MD5 => FixedSizeType(ByteStringType.into(), 16),
            // DateTime
            UtilsPkg::ToDate => DateTimeType,
            // String
            UtilsPkg::Base62Encode
            | UtilsPkg::Base64Encode
            | UtilsPkg::ToASCII
            | UtilsPkg::Hex => StringType,
            // Number
            UtilsPkg::Random
            | UtilsPkg::Round 
            | UtilsPkg::ToF64 => NumberType(F64Kind),
            UtilsPkg::ToI64 => NumberType(I64Kind),
            UtilsPkg::ToI128 => NumberType(I128Kind),
            UtilsPkg::ToU8 => NumberType(U8Kind),
            UtilsPkg::ToU64 => NumberType(U64Kind),
            UtilsPkg::ToU128 => NumberType(U128Kind),
            // Runtime
            UtilsPkg::To => RuntimeResolvedType,
        }
    }

    fn evaluate(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match self {
            UtilsPkg::Base62Encode => extract_value_fn1(ms, args, Self::do_util_base62_encode),
            UtilsPkg::Base64Encode => extract_value_fn1(ms, args, Self::do_util_base64_encode),
            UtilsPkg::Gzip => extract_value_fn1(ms, args, Self::do_util_gzip),
            UtilsPkg::Gunzip => extract_value_fn1(ms, args, Self::do_util_gunzip),
            UtilsPkg::Hex => extract_value_fn1(ms, args, Self::do_util_to_hex),
            UtilsPkg::MD5 => extract_value_fn1(ms, args, Self::do_util_md5),
            UtilsPkg::Random => extract_value_fn0(ms, args, Self::do_util_random),
            UtilsPkg::Round => extract_value_fn2(ms, args, Self::do_util_round),
            UtilsPkg::To => extract_value_fn2(ms, args, Self::do_util_to_xxx),
            UtilsPkg::ToASCII => extract_value_fn1(ms, args, Self::do_util_to_ascii),
            UtilsPkg::ToBytes => extract_value_fn1(ms, args, Self::do_util_to_bytes),
            UtilsPkg::ToDate => self.adapter_pf_fn1(ms, args, Self::do_util_numeric_conv),
            UtilsPkg::ToF64 => self.adapter_pf_fn1(ms, args, Self::do_util_numeric_conv),
            UtilsPkg::ToI64 => self.adapter_pf_fn1(ms, args, Self::do_util_numeric_conv),
            UtilsPkg::ToI128 => self.adapter_pf_fn1(ms, args, Self::do_util_numeric_conv),
            UtilsPkg::ToU8 => self.adapter_pf_fn1(ms, args, Self::do_util_numeric_conv),
            UtilsPkg::ToU64 => self.adapter_pf_fn1(ms, args, Self::do_util_numeric_conv),        
            UtilsPkg::ToU128 => self.adapter_pf_fn1(ms, args, Self::do_util_numeric_conv),
            UtilsPkg::Base62Decode => extract_value_fn1(ms, args, Self::do_util_base62_decode),
            UtilsPkg::Base64Decode => extract_value_fn1(ms, args, Self::do_util_base64_decode),
        }
    }
}

/// WWW package
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum WwwPkg {
    HttpServe,
    URLDecode,
    URLEncode,
    WsConnect,
    WsSendBytes,
    WsSendText,
}

impl WwwPkg {
    
    fn do_http_serve(
        ms: Machine,
        port: &TypedValue,
        maybe_api_cfg: Option<&TypedValue>
    ) -> std::io::Result<(Machine, TypedValue)> {
        let port = port.to_u16();
        match maybe_api_cfg {
            None => { server_engine::start_http_server(port); }
            Some(cfg_value) => {
                let api_cfg = server_engine::convert_to_user_api_config(cfg_value)?;
                server_engine::start_http_server_with_user_apis(port, api_cfg);
            }
        }
        Ok((ms, Boolean(true)))
    }

    fn do_www_url_decode(ms: Machine, url: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        let uri = pull_string(url)?;
        let decoded = urlencoding::decode(uri.as_str()).map_err(|e| cnv_error!(e))?;
        Ok((ms, StringValue(decoded.to_string())))
    }

    fn do_www_url_encode(ms: Machine, url: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        let uri = pull_string(url)?;
        let encoded_url = urlencoding::encode(uri.as_str());
        Ok((ms, StringValue(encoded_url.to_string())))
    }

    fn do_ws_connect(
        ms: Machine,
        host: &TypedValue,
        port: &TypedValue,
        path: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let conn = futures::executor::block_on(ws_commander::connect_ws(
            pull_string(host)?.as_str(),
            pull_number(port)?.to_u16(),
            pull_string(path)?.as_str(),
        ))?;
        Ok((ms, conn))
    }

    fn do_ws_send_bytes(
        ms: Machine,
        conn: &TypedValue,
        message: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let conn = pull_number(conn)?;
        let response = futures::executor::block_on(ws_commander::send_binary_command(
            conn.to_u128(),
            message.to_bytes(),
        ))?;
        Ok((ms, response))
    }

    fn do_ws_send_text(
        ms: Machine,
        conn: &TypedValue,
        message: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let conn = pull_number(conn)?;
        let response = futures::executor::block_on(ws_commander::send_text_command(
            conn.to_u128(),
            message.unwrap_value().as_str(),
        ))?;
        Ok((ms, response))
    }

    pub fn get_contents() -> Vec<PackageOps> {
        vec![
            PackageOps::Www(WwwPkg::HttpServe),
            PackageOps::Www(WwwPkg::URLDecode),
            PackageOps::Www(WwwPkg::URLEncode),
            // PackageOps::Www(WwwPkg::WsConnect),
            // PackageOps::Www(WwwPkg::WsSendBytes),
            // PackageOps::Www(WwwPkg::WsSendText),
        ]
    }
}

impl Package for WwwPkg {
    fn get_name(&self) -> String {
        (match self {
            WwwPkg::HttpServe => "serve",
            WwwPkg::URLDecode => "url_decode",
            WwwPkg::URLEncode => "url_encode",
            WwwPkg::WsConnect => "connect",
            WwwPkg::WsSendBytes => "send_bytes",
            WwwPkg::WsSendText => "send_text",
        }).into()
    }

    fn get_package_name(&self) -> String {
        (match self {
            WwwPkg::HttpServe => "http",
            WwwPkg::URLDecode |
            WwwPkg::URLEncode => "www",
            WwwPkg::WsConnect |
            WwwPkg::WsSendBytes |
            WwwPkg::WsSendText => "ws",
        }).into()
    }

    fn get_description(&self) -> String {
        (match self {
            WwwPkg::HttpServe => "Starts a local HTTP service",
            WwwPkg::URLDecode => "Decodes a URL-encoded string",
            WwwPkg::URLEncode => "Encodes a URL string",
            WwwPkg::WsConnect => "Establishes a web socket connection",
            WwwPkg::WsSendBytes => "Transfers a binary message via a web socket connection",
            WwwPkg::WsSendText => "Transfers a text message via a web socket connection",
        }).into()
    }

    fn get_examples(&self) -> Vec<String> {
        match self {
            WwwPkg::HttpServe => vec![
                strip_margin(r#"
                    |http::serve(8787)
                    |stocks = nsd::save(
                    |   "examples.www.stocks",
                    |   Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                    |)
                    |[{ symbol: "XINU", exchange: "NYSE", last_sale: 8.11 },
                    | { symbol: "BOX", exchange: "NYSE", last_sale: 56.88 },
                    | { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 },
                    | { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                    | { symbol: "MIU", exchange: "OTCBB", last_sale: 2.24 }] ~> stocks
                    |GET http://localhost:8787/examples/www/stocks/1/4
                "#, '|')
            ],
            WwwPkg::URLDecode => vec![
                "'http%3A%2F%2Fshocktrade.com%3Fname%3Dthe%20hero%26t%3D9998'::url_decode()"
                    .into(),
            ],
            WwwPkg::URLEncode => vec![
                "'http://shocktrade.com?name=the hero&t=9998'::url_encode()".into()
            ],
            WwwPkg::WsConnect => vec![
                strip_margin(r#"
                    |ws::connect("localhost", 8287, "/api/ws")
                "#, '|'),
            ],
            WwwPkg::WsSendBytes => vec![
                strip_margin(r#"
                    |let conn = ws::connect("localhost", 8288, "/api/ws")
                    |conn::send_bytes(0B5eb63bbbe01eeed093cb22bb8f5acdc3)
                "#, '|'),
            ],
            WwwPkg::WsSendText => vec![
                strip_margin(r#"
                    |let conn = ws::connect("localhost", 8289, "/api/ws")
                    |conn::send_text("hello world")
                "#, '|'),
            ],
        }
    }

    fn get_parameter_types(&self) -> Vec<DataType> {
        match self {
            WwwPkg::HttpServe => vec![NumberType(I64Kind)],
            WwwPkg::URLDecode 
            | WwwPkg::URLEncode => vec![StringType],
            WwwPkg::WsConnect => vec![StringType, NumberType(I64Kind), StringType],
            WwwPkg::WsSendBytes => vec![UUIDType, ByteStringType],
            WwwPkg::WsSendText => vec![UUIDType, StringType],
        }
    }

    fn get_return_type(&self) -> DataType {
        match self {
            WwwPkg::HttpServe => BooleanType,
            WwwPkg::URLDecode 
            | WwwPkg::URLEncode => StringType,
            WwwPkg::WsConnect => UUIDType,
            WwwPkg::WsSendBytes => StringType,
            WwwPkg::WsSendText => StringType,
        }
    }

    fn evaluate(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match self {
            WwwPkg::URLDecode => extract_value_fn1(ms, args, WwwPkg::do_www_url_decode),
            WwwPkg::URLEncode => extract_value_fn1(ms, args, WwwPkg::do_www_url_encode),
            WwwPkg::HttpServe => extract_value_fn1_or_2(ms, args, WwwPkg::do_http_serve),
            WwwPkg::WsConnect => extract_value_fn3(ms, args, WwwPkg::do_ws_connect),
            WwwPkg::WsSendBytes => extract_value_fn2(ms, args, WwwPkg::do_ws_send_bytes),
            WwwPkg::WsSendText => extract_value_fn2(ms, args, WwwPkg::do_ws_send_text),
        }
    }
}

/// Unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::interpreter::Interpreter;
    use crate::packages::PackageOps::*;

    //thread::sleep(Duration::from_secs(2));

    #[test]
    fn test_encode_decode() {
        for expected in PackageOps::get_contents() {
            let bytes = expected.encode().unwrap();
            assert_eq!(bytes.len(), 8);

            let actual = PackageOps::decode(bytes).unwrap();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn test_include_expect_failure() {
        let mut interpreter = Interpreter::new();
        let result = interpreter.evaluate(r#"include 123"#);
        assert!(matches!(result, Err(..)))
    }

    #[test]
    fn test_examples() {
        let mut errors = 0;
        let mut interpreter = Interpreter::new();
        for op in PackageOps::get_all_packages() {
            println!("{}::{} - {}", op.get_package_name(), op.get_name(), op.get_description());
            for (n, example) in op.get_examples().iter().enumerate() {
                match interpreter.evaluate(example.as_str()) {
                    Ok(response) => assert_ne!(response, Undefined),
                    Err(err) => {
                        eprintln!(
                            "{}\nExample{}\n{}\nERROR: {}", "*".repeat(60), superscript(n + 1), example, err);
                        errors += 1
                    }
                }
            }
        }
        assert_eq!(errors, 0)
    }

    #[test]
    fn generate_test_to_code() {
        // NOTE: this test generates the test cases for `test_to_code`
        let mut last_module: String = String::new();
        for pf in PackageOps::get_all_packages() {
            if last_module != pf.get_package_name() {
                last_module = pf.get_package_name();
                println!("// {}", last_module)
            }
            let opcode = match &pf {
                Agg(op) => format!("Agg(AggPkg::{:?})", op),
                Arrays(op) => format!("Arrays(ArraysPkg::{:?})", op),
                Cal(op) => format!("Cal(CalPkg::{:?})", op),
                Durations(op) => format!("Durations(DurationsPkg::{:?})", op),
                Io(op) => format!("Io(IoPkg::{:?})", op),
                Math(op) => format!("Math(MathPkg::{:?})", op),
                Nsd(op) => format!("Nsd(NsdPkg::{:?})", op),
                Oxide(op) => format!("Oxide(OxidePkg::{:?})", op),
                Os(op) => format!("Os(OsPkg::{:?})", op),
                Strings(op) => format!("Strings(StringsPkg::{:?})", op),
                Tools(op) => format!("Tools(ToolsPkg::{:?})", op),
                Utils(op) => format!("Utils(UtilsPkg::{:?})", op),
                Www(op) => format!("Www(WwwPkg::{:?})", op),
            };
            println!("assert_eq!({}.to_code(), \"{}\");", opcode, pf.to_code())
        }
    }

    #[test]
    fn test_to_code() {
        // agg
        assert_eq!(Agg(AggPkg::Avg).to_code(), "agg::avg(a)");
        assert_eq!(Agg(AggPkg::Count).to_code(), "agg::count(a)");
        assert_eq!(Agg(AggPkg::Max).to_code(), "agg::max(a)");
        assert_eq!(Agg(AggPkg::Min).to_code(), "agg::min(a)");
        assert_eq!(Agg(AggPkg::Sum).to_code(), "agg::sum(a)");
        // arrays
        assert_eq!(Arrays(ArraysPkg::Filter).to_code(), "arrays::filter(a: Array(), b: fn(item): Boolean)");
        assert_eq!(Arrays(ArraysPkg::IsEmpty).to_code(), "arrays::is_empty(a: Array())");
        assert_eq!(Arrays(ArraysPkg::Len).to_code(), "arrays::len(a: Array())");
        assert_eq!(Arrays(ArraysPkg::Map).to_code(), "arrays::map(a: Array(), b: fn(item))");
        assert_eq!(Arrays(ArraysPkg::Pop).to_code(), "arrays::pop(a: Array())");
        assert_eq!(Arrays(ArraysPkg::Push).to_code(), "arrays::push(a: Array(), b)");
        assert_eq!(Arrays(ArraysPkg::Reduce).to_code(), "arrays::reduce(a: Array(), b, c: fn(a, b))");
        assert_eq!(Arrays(ArraysPkg::Reverse).to_code(), "arrays::reverse(a: Array())");
        assert_eq!(Arrays(ArraysPkg::ToArray).to_code(), "arrays::to_array(a)");
        // cal
        assert_eq!(Cal(CalPkg::DateDay).to_code(), "cal::day(a: DateTime)");
        assert_eq!(Cal(CalPkg::DateHour12).to_code(), "cal::hour12(a: DateTime)");
        assert_eq!(Cal(CalPkg::DateHour24).to_code(), "cal::hour24(a: DateTime)");
        assert_eq!(Cal(CalPkg::DateMinute).to_code(), "cal::minute(a: DateTime)");
        assert_eq!(Cal(CalPkg::DateMonth).to_code(), "cal::month(a: DateTime)");
        assert_eq!(Cal(CalPkg::DateSecond).to_code(), "cal::second(a: DateTime)");
        assert_eq!(Cal(CalPkg::DateYear).to_code(), "cal::year(a: DateTime)");
        assert_eq!(Cal(CalPkg::IsLeapYear).to_code(), "cal::is_leap_year(a: DateTime)");
        assert_eq!(Cal(CalPkg::IsWeekday).to_code(), "cal::is_weekday(a: DateTime)");
        assert_eq!(Cal(CalPkg::IsWeekend).to_code(), "cal::is_weekend(a: DateTime)");
        assert_eq!(Cal(CalPkg::Minus).to_code(), "cal::minus(a: DateTime, b: i64)");
        assert_eq!(Cal(CalPkg::Plus).to_code(), "cal::plus(a: DateTime, b: i64)");
        assert_eq!(Cal(CalPkg::ToMillis).to_code(), "cal::to_millis()");
        // durations
        assert_eq!(Durations(DurationsPkg::Days).to_code(), "durations::days(n: i64)");
        assert_eq!(Durations(DurationsPkg::Hours).to_code(), "durations::hours(n: i64)");
        assert_eq!(Durations(DurationsPkg::Millis).to_code(), "durations::millis(n: i64)");
        assert_eq!(Durations(DurationsPkg::Minutes).to_code(), "durations::minutes(n: i64)");
        assert_eq!(Durations(DurationsPkg::Seconds).to_code(), "durations::seconds(n: i64)");
        // io
        assert_eq!(Io(IoPkg::FileCreate).to_code(), "io::create_file(a: String, b: String)");
        assert_eq!(Io(IoPkg::FileExists).to_code(), "io::exists(s: String)");
        assert_eq!(Io(IoPkg::FileReadText).to_code(), "io::read_text_file(s: String)");
        assert_eq!(Io(IoPkg::StdErr).to_code(), "io::stderr(s: String)");
        assert_eq!(Io(IoPkg::StdIn).to_code(), "io::stdin()");
        assert_eq!(Io(IoPkg::StdOut).to_code(), "io::stdout(s: String)");
        // math
        assert_eq!(Math(MathPkg::Abs).to_code(), "math::abs(n: f64)");
        assert_eq!(Math(MathPkg::Ceil).to_code(), "math::ceil(n: f64)");
        assert_eq!(Math(MathPkg::Floor).to_code(), "math::floor(n: f64)");
        assert_eq!(Math(MathPkg::Max).to_code(), "math::max(a: f64, b: f64)");
        assert_eq!(Math(MathPkg::Min).to_code(), "math::min(a: f64, b: f64)");
        assert_eq!(Math(MathPkg::Pow).to_code(), "math::pow(a: f64, b: f64)");
        assert_eq!(Math(MathPkg::Round).to_code(), "math::round(n: f64)");
        assert_eq!(Math(MathPkg::Sqrt).to_code(), "math::sqrt(n: f64)");
        // nsd
        assert_eq!(Nsd(NsdPkg::CreateEventSrc).to_code(), "nsd::create_event_src(a: String, b: Table)");
        assert_eq!(Nsd(NsdPkg::CreateFn).to_code(), "nsd::create_fn(a: String, b: fn(): Struct)");
        assert_eq!(Nsd(NsdPkg::CreateIndex).to_code(), "nsd::create_index(a: String, b: Array())");
        assert_eq!(Nsd(NsdPkg::Drop).to_code(), "nsd::drop(s: String)");
        assert_eq!(Nsd(NsdPkg::Exists).to_code(), "nsd::exists(s: String)");
        assert_eq!(Nsd(NsdPkg::Journal).to_code(), "nsd::journal(t: Table)");
        assert_eq!(Nsd(NsdPkg::Load).to_code(), "nsd::load(s: String)");
        assert_eq!(Nsd(NsdPkg::Replay).to_code(), "nsd::replay(t: Table)");
        assert_eq!(Nsd(NsdPkg::Resize).to_code(), "nsd::resize(s: String, n: i64)");
        assert_eq!(Nsd(NsdPkg::Save).to_code(), "nsd::save(a: String, b: Table)");
        assert_eq!(Nsd(NsdPkg::Truncate).to_code(), "nsd::truncate(s: String)");
        // os
        assert_eq!(Os(OsPkg::Call).to_code(), "os::call(s: String)");
        assert_eq!(Os(OsPkg::Clear).to_code(), "os::clear()");
        assert_eq!(Os(OsPkg::CurrentDir).to_code(), "os::current_dir()");
        assert_eq!(Os(OsPkg::Env).to_code(), "os::env()");
        // oxide
        assert_eq!(Oxide(OxidePkg::Compile).to_code(), "oxide::compile(s: String)");
        assert_eq!(Oxide(OxidePkg::Debug).to_code(), "oxide::debug(s: String)");
        assert_eq!(Oxide(OxidePkg::Eval).to_code(), "oxide::eval(s: String)");
        assert_eq!(Oxide(OxidePkg::Help).to_code(), "oxide::help()");
        assert_eq!(Oxide(OxidePkg::History).to_code(), "oxide::history()");
        assert_eq!(Oxide(OxidePkg::Home).to_code(), "oxide::home()");
        assert_eq!(Oxide(OxidePkg::Inspect).to_code(), "oxide::inspect(s: String)");
        assert_eq!(Oxide(OxidePkg::Printf).to_code(), "oxide::printf(a: String, b: Array())");
        assert_eq!(Oxide(OxidePkg::Println).to_code(), "oxide::println(s: String)");
        assert_eq!(Oxide(OxidePkg::Reset).to_code(), "oxide::reset()");
        assert_eq!(Oxide(OxidePkg::Sprintf).to_code(), "oxide::sprintf(a: String, b: Array())");
        assert_eq!(Oxide(OxidePkg::UUID).to_code(), "oxide::uuid()");
        assert_eq!(Oxide(OxidePkg::Version).to_code(), "oxide::version()");
        // str
        assert_eq!(Strings(StringsPkg::EndsWith).to_code(), "str::ends_with(a: String, b: String)");
        assert_eq!(Strings(StringsPkg::Format).to_code(), "str::format(a: String, b: String)");
        assert_eq!(Strings(StringsPkg::IndexOf).to_code(), "str::index_of(s: String, n: i64)");
        assert_eq!(Strings(StringsPkg::Join).to_code(), "str::join(a: Array(), b: String)");
        assert_eq!(Strings(StringsPkg::Left).to_code(), "str::left(s: String, n: i64)");
        assert_eq!(Strings(StringsPkg::Len).to_code(), "str::len(s: String)");
        assert_eq!(Strings(StringsPkg::Right).to_code(), "str::right(s: String, n: i64)");
        assert_eq!(Strings(StringsPkg::Split).to_code(), "str::split(a: String, b: String)");
        assert_eq!(Strings(StringsPkg::StartsWith).to_code(), "str::starts_with(a: String, b: String)");
        assert_eq!(Strings(StringsPkg::StripMargin).to_code(), "str::strip_margin(a: String, b: String)");
        assert_eq!(Strings(StringsPkg::Substring).to_code(), "str::substring(s: String, m: i64, n: i64)");
        assert_eq!(Strings(StringsPkg::SuperScript).to_code(), "str::superscript(n: i64)");
        assert_eq!(Strings(StringsPkg::ToLowercase).to_code(), "str::to_lowercase(s: String)");
        assert_eq!(Strings(StringsPkg::ToString).to_code(), "str::to_string(a)");
        assert_eq!(Strings(StringsPkg::ToUppercase).to_code(), "str::to_uppercase(s: String)");
        assert_eq!(Strings(StringsPkg::Trim).to_code(), "str::trim(s: String)");
        // tools
        assert_eq!(Tools(ToolsPkg::Compact).to_code(), "tools::compact(t: Table)");
        assert_eq!(Tools(ToolsPkg::Describe).to_code(), "tools::describe(t: Table)");
        assert_eq!(Tools(ToolsPkg::Fetch).to_code(), "tools::fetch(t: Table, n: i64)");
        assert_eq!(Tools(ToolsPkg::Filter).to_code(), "tools::filter(a, b)");
        assert_eq!(Tools(ToolsPkg::Latest).to_code(), "tools::latest(t: Table)");
        assert_eq!(Tools(ToolsPkg::Len).to_code(), "tools::len(t: Table)");
        assert_eq!(Tools(ToolsPkg::Map).to_code(), "tools::map(a, b)");
        assert_eq!(Tools(ToolsPkg::Pop).to_code(), "tools::pop(t: Table)");
        assert_eq!(Tools(ToolsPkg::Push).to_code(), "tools::push(a, b)");
        assert_eq!(Tools(ToolsPkg::Reverse).to_code(), "tools::reverse(t: Table)");
        assert_eq!(Tools(ToolsPkg::Scan).to_code(), "tools::scan(t: Table)");
        assert_eq!(Tools(ToolsPkg::Shuffle).to_code(), "tools::shuffle(b: Boolean)");
        assert_eq!(Tools(ToolsPkg::ToArray).to_code(), "tools::to_array(a)");
        assert_eq!(Tools(ToolsPkg::ToCSV).to_code(), "tools::to_csv(t: Table)");
        assert_eq!(Tools(ToolsPkg::ToJSON).to_code(), "tools::to_json(t: Table)");
        assert_eq!(Tools(ToolsPkg::ToTable).to_code(), "tools::to_table(a)");
        // util
        assert_eq!(Utils(UtilsPkg::Base62Decode).to_code(), "util::base62_decode(a)");
        assert_eq!(Utils(UtilsPkg::Base64Decode).to_code(), "util::base64_decode(a)");
        assert_eq!(Utils(UtilsPkg::Base62Encode).to_code(), "util::base62_encode(a)");
        assert_eq!(Utils(UtilsPkg::Base64Encode).to_code(), "util::base64_encode(a)");
        assert_eq!(Utils(UtilsPkg::Gzip).to_code(), "util::gzip(a)");
        assert_eq!(Utils(UtilsPkg::Gunzip).to_code(), "util::gunzip(a)");
        assert_eq!(Utils(UtilsPkg::Hex).to_code(), "util::hex(a)");
        assert_eq!(Utils(UtilsPkg::MD5).to_code(), "util::md5(a)");
        assert_eq!(Utils(UtilsPkg::Random).to_code(), "util::random()");
        assert_eq!(Utils(UtilsPkg::Round).to_code(), "util::round(a)");
        assert_eq!(Utils(UtilsPkg::To).to_code(), "util::to(a, b)");
        assert_eq!(Utils(UtilsPkg::ToASCII).to_code(), "util::to_ascii(n: i64)");
        assert_eq!(Utils(UtilsPkg::ToBytes).to_code(), "util::to_bytes(a)");
        assert_eq!(Utils(UtilsPkg::ToDate).to_code(), "util::to_date(a)");
        assert_eq!(Utils(UtilsPkg::ToU8).to_code(), "util::to_u8(a)");
        assert_eq!(Utils(UtilsPkg::ToF64).to_code(), "util::to_f64(a)");
        assert_eq!(Utils(UtilsPkg::ToI64).to_code(), "util::to_i64(a)");
        assert_eq!(Utils(UtilsPkg::ToU64).to_code(), "util::to_u64(a)");
        assert_eq!(Utils(UtilsPkg::ToI128).to_code(), "util::to_i128(a)");
        assert_eq!(Utils(UtilsPkg::ToU128).to_code(), "util::to_u128(a)");
        // http
        assert_eq!(Www(WwwPkg::HttpServe).to_code(), "http::serve(n: i64)");
        // www
        assert_eq!(Www(WwwPkg::URLDecode).to_code(), "www::url_decode(s: String)");
        assert_eq!(Www(WwwPkg::URLEncode).to_code(), "www::url_encode(s: String)");
    }

    /// Package "array" tests
    #[cfg(test)]
    mod agg_tests {
        use crate::testdata::verify_exact_table;

        #[test]
        fn test_agg_max_min_sum() {
            verify_exact_table(r#"
                select 
                    total_sale: agg::sum(last_sale),
                    min_sale: agg::min(last_sale),
                    max_sale: agg::max(last_sale)
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
                    "|---------------------------------------|",
                    "| id | total_sale | min_sale | max_sale |",
                    "|---------------------------------------|",
                    "| 0  | 334.51     | 11.77    | 214.88   |",
                    "|---------------------------------------|"]);
        }
    }

    /// Package "array" tests
    #[cfg(test)]
    mod array_tests {
        use crate::testdata::verify_exact_code;

        #[test]
        fn test_arrays_filter() {
            verify_exact_code(r#"
                [123, 56, 89, 66]::filter(n -> (n % 3) == 0)
           "#,
                "[123, 66]",
            )
        }

        #[test]
        fn test_arrays_filter_with_range() {
            verify_exact_code(
                r#"
                1..7::filter(n -> (n % 2) == 0)
           "#,
                "[2, 4, 6]",
            )
        }

        #[test]
        fn test_arrays_len() {
            verify_exact_code(
                r#"
                [3, 5, 7, 9]::len()
           "#,
                "4",
            )
        }

        #[test]
        fn test_arrays_len_map_with_range() {
            verify_exact_code(
                r#"
                1..5::len()
           "#,
                "4",
            )
        }

        #[test]
        fn test_arrays_map() {
            verify_exact_code(
                r#"
                [1, 2, 3]::map(n -> n * 2)
           "#,
                "[2, 4, 6]",
            )
        }

        #[test]
        fn test_arrays_map_with_range() {
            verify_exact_code(
                r#"
                1..4::map(n -> n * 2)
           "#,
                "[2, 4, 6]",
            )
        }

        #[test]
        fn test_arrays_pop() {
            verify_exact_code(r#"
                stocks = ["ABC", "BOOM", "JET", "DEX"]
                stocks::pop()
            "#, r#"(["ABC", "BOOM", "JET"], "DEX")"#);
        }

        #[ignore]
        #[test]
        fn test_arrays_push() {
            verify_exact_code(
                r#"
                stocks = ["ABC", "BOOM", "JET"]
                stocks = stocks::push("DEX")
                stocks
            "#,
                r#"["ABC", "BOOM", "JET", "DEX"]"#,
            );
        }

        #[test]
        fn test_arrays_reduce() {
            verify_exact_code(r#"
                 numbers = [1, 2, 3, 4, 5]
                 numbers::reduce(0, (a, b) -> a + b)
            "#, "15");
        }

        #[test]
        fn test_arrays_reduce_with_range() {
            verify_exact_code(r#"
                 1..=5::reduce(0, (a, b) -> a + b)
            "#, "15");
        }

        #[test]
        fn test_arrays_reverse() {
            verify_exact_code(r#"
                ['cat', 'dog', 'ferret', 'mouse']::reverse()
            "#, r#"["mouse", "ferret", "dog", "cat"]"#)
        }

        #[test]
        fn test_arrays_reverse_with_range() {
            verify_exact_code(r#"
                1..=5::reverse()
            "#, r#"[5, 4, 3, 2, 1]"#)
        }

        #[test]
        fn test_arrays_to_array() {
            verify_exact_code(r#"
                 ("a", "b", "c")::to_array()
            "#, r#"["a", "b", "c"]"#);
        }
    }

    /// Package "cal" tests
    #[cfg(test)]
    mod cal_tests {
        use crate::numbers::Numbers::*;
        use crate::testdata::{verify_exact_code, verify_exact_value, verify_exact_value_where};
        use crate::typed_values::TypedValue::{Boolean, DateTimeValue, Number};

        #[test]
        fn test_cal_is_weekend() {
            verify_exact_value(r#"
                2025-07-06T20:19:26.930Z::is_weekend
            "#, Boolean(true));
        }

        #[test]
        fn test_cal_day_of() {
            verify_exact_value_where(r#"
                2025-07-06T20:19:26.930Z::day
            "#,
                |n| matches!(n, Number(I64Value(..))),
            );
        }

        #[test]
        fn test_cal_hour24() {
            verify_exact_value_where(r#"
                2025-07-06T20:19:26.930Z::hour24
            "#,
                |n| matches!(n, Number(I64Value(..))),
            );
        }

        #[test]
        fn test_cal_hour12() {
            verify_exact_value_where(r#"
                2025-07-06T20:19:26.930Z::hour12
            "#,
                |n| matches!(n, Number(I64Value(..))),
            );
        }

        #[test]
        fn test_cal_minute_of() {
            verify_exact_value_where(r#"
                2025-07-06T20:19:26.930Z::minute
            "#,
                |n| matches!(n, Number(I64Value(..))),
            );
        }

        #[test]
        fn test_cal_month_of() {
            verify_exact_value_where(r#"
                2025-07-06T20:19:26.930Z::month
            "#,
                |n| matches!(n, Number(I64Value(..))),
            );
        }

        #[test]
        fn test_cal_second_of() {
            verify_exact_value_where(r#"
                2025-07-06T20:19:26.930Z::second
            "#,
                |n| matches!(n, Number(I64Value(..))),
            );
        }

        #[test]
        fn test_cal_year_of() {
            verify_exact_value_where(r#"
                2025-07-06T20:19:26.930Z::year
            "#,
                |n| matches!(n, Number(I64Value(..))),
            );
        }

        #[test]
        fn test_cal_minus() {
            verify_exact_value_where(r#"
                2025-07-06T20:19:26.930Z::minus(3::days)
            "#,
                                     |n| matches!(n, DateTimeValue(..)),
            );
        }

        #[test]
        fn test_cal_plus() {
            verify_exact_value_where(r#"
                2025-07-06T20:19:26.930Z::plus(30::days)
            "#,
                |n| matches!(n, DateTimeValue(..)));
        }

        #[test]
        fn test_cal_to_millis() {
            verify_exact_code(r#"
                2025-07-06T20:19:26.930Z::to_millis
            "#, "1751833166930");
        }
    }

    /// Package "durations" tests
    #[cfg(test)]
    mod duration_tests {
        use super::*;
        use crate::numbers::Numbers::{F64Value, I64Value};
        use crate::testdata::verify_exact_value;
        use crate::typed_values::TypedValue::Number;
        use num_traits::ToPrimitive;

        #[test]
        fn test_durations_days() {
            verify_exact_value(r#"
                3::days
            "#,
                Number(I64Value(3 * DAYS)),
            );
        }

        #[test]
        fn test_durations_hours() {
            verify_exact_value(r#"
                8::hours
            "#,
                Number(I64Value(8 * HOURS)),
            );
        }

        #[test]
        fn test_durations_hours_f64() {
            verify_exact_value(r#"
                0.5::hours
            "#,
                Number(F64Value(30.0 * MINUTES.to_f64().unwrap())),
            );
        }

        #[test]
        fn test_durations_millis() {
            verify_exact_value(r#"
                1000::millis
            "#,
                Number(I64Value(1 * SECONDS)),
            );
        }

        #[test]
        fn test_durations_minutes() {
            verify_exact_value(r#"
                30::minutes
            "#,
                Number(I64Value(30 * MINUTES)),
            );
        }

        #[test]
        fn test_durations_seconds() {
            verify_exact_value(r#"
                20::seconds
            "#,
                Number(I64Value(20 * SECONDS)),
            );
        }
    }

    /// Package "http" tests
    #[cfg(test)]
    mod http_tests {
        use super::*;
        use crate::packages::PackageOps;
        use crate::testdata::*;
        use crate::typed_values::TypedValue::*;
        use PackageOps::*;

        #[test]
        fn test_http_serve() {
            verify_exact_table(r#"
                http::serve(7656)
                stocks = nsd::save(
                   "packages.http.stocks",
                   Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                )
                [{ symbol: "XINU", exchange: "NYSE", last_sale: 8.11 },
                 { symbol: "BOX", exchange: "NYSE", last_sale: 56.88 },
                 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 },
                 { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                 { symbol: "MIU", exchange: "OTCBB", last_sale: 2.24 }] ~> stocks
                GET http://localhost:7656/packages/http/stocks/1/4
            "#, vec![
                "|------------------------------------|", 
                "| id | exchange | last_sale | symbol |", 
                "|------------------------------------|", 
                "| 0  | NYSE     | 56.88     | BOX    |",
                "| 1  | NASDAQ   | 32.12     | JET    |", 
                "| 2  | AMEX     | 12.49     | ABC    |", 
                "|------------------------------------|"])
        }
    }

    /// Package "io" tests
    #[cfg(test)]
    mod io_tests {
        use super::*;
        use crate::packages::PackageOps;
        use crate::testdata::verify_exact_value;
        use crate::typed_values::TypedValue::*;
        use PackageOps::*;

        #[test]
        fn test_io_create_file_qualified() {
            verify_exact_value(r#"
                io::create_file("quote.json", { symbol: "TRX", exchange: "NYSE", last_sale: 45.32 })
            "#,
                Number(I64Value(52)),
            );

            verify_exact_value(r#"
                io::exists("quote.json")
            "#,
                Boolean(true),
            );
        }

        #[test]
        fn test_io_create_file_postfix() {
            verify_exact_value(r#"
                use io
                "quote.json":::create_file({
                    symbol: "TRX",
                    exchange: "NYSE",
                    last_sale: 45.32
                })
            "#, Number(I64Value(52)));

            verify_exact_value(r#"
                use io
                "quote.json":::exists()
            "#, Boolean(true));
        }

        #[test]
        fn test_io_file_exists() {
            verify_exact_value(r#"
                use io
                path_str = oxide::home()
                path_str:::exists()
            "#, Boolean(true))
        }

        #[test]
        fn test_io_create_and_read_text_file() {
            verify_exact_value(r#"
                use io
                file = "temp_secret.txt"
                file:::create_file("**keep**this**secret**"::md5())
                file:::read_text_file()
            "#, StringValue("0B47338bd5f35bbb239092c36e30775b4a".into()))
        }

        #[test]
        fn test_io_stderr() {
            verify_exact_value(r#"io::stderr("Goodbye Cruel World")"#, Boolean(true));
        }

        #[test]
        fn test_io_stdout() {
            verify_exact_value(r#"io::stdout("Hello World")"#, Boolean(true));
        }
    }

    /// Package "math" tests
    #[cfg(test)]
    mod math_tests {
        use crate::numbers::Numbers::{F64Value, I64Value};
        use crate::testdata::verify_exact_value;
        use crate::typed_values::TypedValue::Number;

        #[test]
        fn test_math_abs() {
            verify_exact_value(
                r#"
                math::abs(-81)
            "#,
                Number(I64Value(81)),
            )
        }

        #[test]
        fn test_math_ceil() {
            verify_exact_value(
                r#"
                math::ceil(7.7)
            "#,
                Number(F64Value(8.0)),
            )
        }

        #[test]
        fn test_math_floor() {
            verify_exact_value(
                r#"
                math::floor(7.7)
            "#,
                Number(F64Value(7.0)),
            )
        }

        #[test]
        fn test_math_max() {
            verify_exact_value(
                r#"
                math::max(17, 71)
            "#,
                Number(I64Value(71)),
            )
        }

        #[test]
        fn test_math_min() {
            verify_exact_value(
                r#"
                math::min(17, 71)
            "#,
                Number(I64Value(17)),
            )
        }

        #[test]
        fn test_math_pow() {
            verify_exact_value(
                r#"
                math::pow(2, 3)
            "#,
                Number(F64Value(8.0)),
            )
        }

        #[test]
        fn test_math_round() {
            verify_exact_value(
                r#"
                math::round(17.51)
            "#,
                Number(F64Value(18.0)),
            )
        }

        #[test]
        fn test_math_sqrt() {
            verify_exact_value(
                r#"
                math::sqrt(25.0)
            "#,
                Number(F64Value(5.0)),
            )
        }
    }

    /// Package "nsd" tests
    #[cfg(test)]
    mod nsd_tests {
        use crate::dataframe::Dataframe::DiskTable;
        use crate::interpreter::Interpreter;
        use crate::numbers::Numbers::I64Value;
        use crate::testdata::{verify_exact_code_with, verify_exact_table_with, verify_exact_value_whence, verify_exact_value_where, verify_exact_value_with};
        use crate::typed_values::TypedValue::{Boolean, Number, TableValue};

        #[test]
        fn test_nsd_create_event_source() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_value_whence(interpreter, r#"
                nsd::drop("packages.events.stocks")
            "#, |r| matches!(r, Boolean(..)));

            interpreter = verify_exact_value_with(interpreter, r#"
                nsd::exists("packages.events.stocks")
            "#, Boolean(false));
            
            interpreter = verify_exact_value_with(interpreter, r#"
                stocks = nsd::create_event_src(
                    "packages.events.stocks",
                    Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                )
            "#, Boolean(true));

            interpreter = verify_exact_value_with(interpreter, r#"
                [{ symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                 { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
            "#, Number(I64Value(3)));

            interpreter = verify_exact_table_with(interpreter, "stocks", vec![
                "|------------------------------------|", 
                "| id | symbol | exchange | last_sale |", 
                "|------------------------------------|", 
                "| 0  | BOOM   | NYSE     | 56.88     |", 
                "| 1  | ABC    | AMEX     | 12.49     |", 
                "| 2  | JET    | NASDAQ   | 32.12     |", 
                "|------------------------------------|"]);

            verify_exact_table_with(interpreter, r#"
                use nsd
                select row_id, column_id, action, new_value from stocks::journal()
            "#, vec![
                r#"|-------------------------------------------------------------|"#, 
                r#"| id | row_id | column_id | action | new_value                |"#, 
                r#"|-------------------------------------------------------------|"#, 
                r#"| 0  | 0      | 0         | CR     | ["BOOM", "NYSE", 56.88]  |"#, 
                r#"| 1  | 1      | 0         | CR     | ["ABC", "AMEX", 12.49]   |"#, 
                r#"| 2  | 2      | 0         | CR     | ["JET", "NASDAQ", 32.12] |"#,
                r#"|-------------------------------------------------------------|"#]);
        }
        
        #[test]
        fn test_nsd_create_fn() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_value_whence(interpreter, r#"
                nsd::drop("packages.table_fn.stocks")
            "#, |r| matches!(r, Boolean(..)));

            interpreter = verify_exact_value_with(interpreter, r#"
                nsd::exists("packages.table_fn.stocks")
            "#, Boolean(false));

            interpreter = verify_exact_value_with(interpreter, r#"
                stocks = nsd::create_fn(
                    "packages.table_fn.stocks",
                    (symbol: String(8), exchange: String(8), last_sale: f64) -> {
                        symbol: symbol,
                        exchange: exchange,
                        last_sale: last_sale * 2.0,
                        rank: __row_id__ + 1
                    })
            "#, Boolean(true));

            interpreter = verify_exact_value_with(interpreter, r#"
                [{ symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                 { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
            "#, Number(I64Value(3)));

            interpreter = verify_exact_table_with(interpreter, "stocks", vec![
                "|-------------------------------------------|",
                "| id | symbol | exchange | last_sale | rank |",
                "|-------------------------------------------|",
                "| 0  | BOOM   | NYSE     | 113.76    | 1    |",
                "| 1  | ABC    | AMEX     | 24.98     | 2    |",
                "| 2  | JET    | NASDAQ   | 64.24     | 3    |",
                "|-------------------------------------------|"]);

            verify_exact_table_with(interpreter, r#"
                use nsd
                stocks::journal()
            "#, vec![
                "|------------------------------------|", 
                "| id | symbol | exchange | last_sale |", 
                "|------------------------------------|", 
                "| 0  | BOOM   | NYSE     | 56.88     |", 
                "| 1  | ABC    | AMEX     | 12.49     |", 
                "| 2  | JET    | NASDAQ   | 32.12     |", 
                "|------------------------------------|"]);
        }

        #[test]
        fn test_nsd_resize() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_table_with(interpreter,r#"
                let stocks =
                   nsd::save('packages.resize.stocks', Table(
                       symbol: String(8),
                       exchange: String(8),
                       last_sale: f64
                   )::new)
                [{ symbol: "TCO", exchange: "NYSE", last_sale: 38.53 },
                 { symbol: "SHMN", exchange: "NYSE", last_sale: 6.57 },
                 { symbol: "HMU", exchange: "NASDAQ", last_sale: 27.12 }] ~> stocks
                stocks
            "#, vec![
                "|------------------------------------|", 
                "| id | symbol | exchange | last_sale |", 
                "|------------------------------------|", 
                "| 0  | TCO    | NYSE     | 38.53     |", 
                "| 1  | SHMN   | NYSE     | 6.57      |", 
                "| 2  | HMU    | NASDAQ   | 27.12     |", 
                "|------------------------------------|"]);

            interpreter = verify_exact_code_with(interpreter,r#"
                nsd::resize('packages.resize.stocks', 2)
            "#, "true");

            interpreter = verify_exact_table_with(interpreter, r#"
                stocks
            "#, vec![
                "|------------------------------------|",
                "| id | symbol | exchange | last_sale |",
                "|------------------------------------|",
                "| 0  | TCO    | NYSE     | 38.53     |",
                "| 1  | SHMN   | NYSE     | 6.57      |",
                "|------------------------------------|"]);

            interpreter = verify_exact_code_with(interpreter,r#"
                use nsd
                stocks::resize(1)
            "#, "true");

            verify_exact_table_with(interpreter, r#"                
                stocks
            "#, vec![
                "|------------------------------------|",
                "| id | symbol | exchange | last_sale |",
                "|------------------------------------|",
                "| 0  | TCO    | NYSE     | 38.53     |",
                "|------------------------------------|"]);
        }

        #[test]
        fn test_nsd_save_namespace() {
            verify_exact_value_where(r#"
                nsd::save("platform.save.stocks", Table(
                    symbol: String(8),
                    exchange: String(8),
                    last_sale: f64
                )::new)
            "#, |df| matches!(df, TableValue(DiskTable(..))))
        }

        #[test]
        fn test_nsd_save_and_load_namespace() {
            verify_exact_value_where(r#"
                let stocks =
                    nsd::save("platform.nsd.ns_save_and_load", Table(
                        symbol: String(8),
                        exchange: String(8),
                        last_sale: f64
                    )::new)

                [{ symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                 { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks

                nsd::load("platform.nsd.ns_save_and_load")      
            "#, |df| matches!(df, TableValue(..)))
        }
    }

    /// Package "os" tests
    #[cfg(test)]
    mod os_tests {
        use super::*;
        use crate::packages::PackageOps;
        use crate::testdata::{
            make_quote_columns, verify_exact_table, verify_exact_value, verify_exact_value_where,
        };
        use crate::typed_values::TypedValue::*;
        use PackageOps::*;

        #[test]
        fn test_os_call() {
            verify_exact_value(r#"
                nsd::save("platform.os.call", Table(
                    symbol: String(8),
                    exchange: String(8),
                    last_sale: f64
                )::new)
                os::call("chmod", "777", oxide::home())
            "#,
                StringValue(String::new()),
            )
        }

        #[test]
        fn test_os_clear() {
            verify_exact_value("os::clear()", Boolean(true))
        }

        #[test]
        fn test_os_current_dir() {
            let phys_columns = make_quote_columns();
            verify_exact_table(r#"
                cur_dir = os::current_dir()
                prefix = if(cur_dir::ends_with("core"), "../..", ".")
                path_str = prefix + "/demoes/language/include_file.oxide"
                include path_str
            "#,
                vec![
                    "|------------------------------------|",
                    "| id | symbol | exchange | last_sale |",
                    "|------------------------------------|",
                    "| 0  | ABC    | AMEX     | 12.49     |",
                    "| 1  | BOOM   | NYSE     | 56.88     |",
                    "| 2  | JET    | NASDAQ   | 32.12     |",
                    "|------------------------------------|",
                ],
            );
        }

        #[test]
        fn test_os_env() {
            verify_exact_value_where("os::env()", |v| matches!(v, TableValue(..)))
        }
    }

    /// Package "oxide" tests
    #[cfg(test)]
    mod oxide_tests {
        use super::*;
        use crate::errors::Errors::Exact;
        use crate::interpreter::Interpreter;
        use crate::packages::PackageOps;
        use crate::testdata::{verify_exact_table, verify_exact_value, verify_exact_value_where, verify_exact_value_with};
        use crate::typed_values::TypedValue::*;
        use PackageOps::*;

        #[test]
        fn test_oxide_compile() {
            verify_exact_value(r#"
                code = oxide::compile("2 ** 4")
                code()
            "#, Number(F64Value(16.)));
        }

        #[test]
        fn test_oxide_compile_closure() {
            verify_exact_value(r#"
                n = 5
                code = oxide::compile("n * n")
                code()
            "#, Number(I64Value(25)));
        }

        #[test]
        fn test_oxide_eval_closure() {
            verify_exact_value(r#"
                a = 'Hello '
                b = 'World'
                oxide::eval("a + b")
            "#, StringValue("Hello World".to_string()));
        }

        #[test]
        fn test_oxide_eval_qualified() {
            verify_exact_value("oxide::eval('2 ** 4')", Number(F64Value(16.)));
            verify_exact_value(
                "oxide::eval(123)",
                ErrorValue(Exact("Type Mismatch: Expected a String near 123".into())),
            );
        }

        #[test]
        fn test_oxide_eval_postfix() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_value_with(interpreter, "use oxide", Boolean(true));
            interpreter =
                verify_exact_value_with(interpreter, "'2 ** 4':::eval()", Number(F64Value(16.)));
            interpreter = verify_exact_value_with(
                interpreter,
                "123:::eval()",
                ErrorValue(Exact("Type Mismatch: Expected a String near 123".into())),
            );
        }

        #[test]
        fn test_oxide_help() {
            // fully-qualified
            verify_exact_value_where("oxide::help()", |v| matches!(v, TableValue(..)));

            // imported
            verify_exact_value_where(r#"
                use oxide
                help()
            "#,
                |v| matches!(v, TableValue(..)),
            );
        }

        #[test]
        fn test_oxide_history() {
            let mut interpreter = Interpreter::new();
            let result = interpreter.evaluate("oxide::history()").unwrap();
            assert!(matches!(result, TableValue(..)))
        }

        #[test]
        fn test_oxide_home() {
            verify_exact_value("oxide::home()", StringValue(Machine::oxide_home()));
        }

        #[test]
        fn test_oxide_inspect() {
            verify_exact_table(r#"
                oxide::inspect("{ x = 1; x = x + 1 }")
            "#, vec![
                r#"|-----------------------------------------------------------------------------------------------------|"#,
                r#"| id | code      | model                                                                              |"#,
                r#"|-----------------------------------------------------------------------------------------------------|"#,
                r#"| 0  | x = 1     | SetVariables(Identifier("x"), Literal(Number(I64Value(1))))                        |"#,
                r#"| 1  | x = x + 1 | SetVariables(Identifier("x"), Plus(Identifier("x"), Literal(Number(I64Value(1))))) |"#,
                r#"|-----------------------------------------------------------------------------------------------------|"#])
        }
        
        #[test]
        fn test_oxide_printf() {
            verify_exact_value(r#"oxide::printf("Hello %s", "World")"#, Boolean(true));
        }
        
        #[test]
        fn test_oxide_println() {
            verify_exact_value(r#"oxide::println("Hello World")"#, Boolean(true));
        }

        #[test]
        fn test_oxide_sprintf() {
            verify_exact_value(r#"oxide::sprintf("Hello %s", "World")"#, StringValue("Hello World".into()));
        }

        #[test]
        fn test_oxide_uuid() {
            verify_exact_value_where(
                r#"oxide::uuid()"#,
                |v| matches!(v, UUIDValue(..)))
        }
        
        #[test]
        fn test_oxide_uuid_from_binary() {
            verify_exact_value(
                r#"oxide::uuid(0Bfeeddeadbeefdeaffadecafebabeface)"#,
                UUIDValue(0xfeeddead_beef_deaf_fade_cafebabeface))
        }

        #[test]
        fn test_oxide_uuid_from_string() {
            verify_exact_value(
                r#"oxide::uuid("feeddead-beef-deaf-fade-cafebabeface")"#,
                UUIDValue(0xfeeddead_beef_deaf_fade_cafebabeface))
        }
        
        #[test]
        fn test_oxide_uuid_from_u128() {
            verify_exact_value(
                r#"oxide::uuid(0xfeeddead_beef_deaf_fade_cafebabeface)"#,
                UUIDValue(0xfeeddead_beef_deaf_fade_cafebabeface))
        }

        #[test]
        fn test_oxide_version() {
            verify_exact_value("oxide::version()", StringValue(VERSION.into()))
        }
    }

    /// Package "str" tests
    #[cfg(test)]
    mod str_tests {
        use super::*;
        use crate::packages::PackageOps;
        use crate::testdata::*;
        use crate::typed_values::TypedValue::*;
        use PackageOps::*;

        #[test]
        fn test_ends_with_true() {
            verify_exact_value(r#"
                'Hello World'::ends_with('World')
            "#, Boolean(true));
        }

        #[test]
        fn test_ends_with_false() {
            verify_exact_value(r#"
                'Hello World'::ends_with('Hello')
            "#, Boolean(false));
        }

        #[test]
        fn test_format() {
            verify_exact_value(r#"
                "This {} the {}"::format("is", "way")
            "#, StringValue("This is the way".into()));
        }

        #[test]
        fn test_index_of_qualified() {
            verify_exact_value(r#"
                'The little brown fox'::index_of('brown')
            "#, Number(I64Value(11)));
        }

        #[test]
        fn test_join() {
            verify_exact_value(r#"
                ['1', 5, 9, '13']::join(', ')
            "#, StringValue("1, 5, 9, 13".into()));
        }

        #[test]
        fn test_left_positive() {
            verify_exact_value(r#"
                'Hello World'::left(5)
            "#, StringValue("Hello".into()));
        }

        #[test]
        fn test_left_negative() {
            verify_exact_value(r#"
                'Hello World'::left(-5)
            "#, StringValue("World".into()));
        }

        #[test]
        fn test_left_valid() {
            verify_exact_value(r#"
                'Hello World'::left(5)
            "#, StringValue("Hello".into()));
        }

        #[test]
        fn test_len() {
            verify_exact_value(r#"
                'The little brown fox'::len()
            "#, Number(I64Value(20)));
        }

        #[test]
        fn test_right_positive() {
            verify_exact_value(r#"
                'Hello World'::right(5)
            "#, StringValue("World".into()));
        }

        #[test]
        fn test_right_negative() {
            verify_exact_value(r#"
                'Hello World'::right(-5)
            "#, StringValue("Hello".into()));
        }

        #[test]
        fn test_split() {
            verify_exact_value(r#"
                'Hello World'::split(' ')
            "#, ArrayValue(Array::from(vec![
                StringValue("Hello".into()),
                StringValue("World".into()),
            ])));
        }

        #[test]
        fn test_split_multiple_chars() {
            verify_exact_value(r#"
                'Hello,there World'::split(' ,')
            "#, ArrayValue(Array::from(vec![
                StringValue("Hello".into()),
                StringValue("there".into()),
                StringValue("World".into()),
            ])));
        }

        #[test]
        fn test_starts_with_true() {
            verify_exact_value(r#"
                'Hello World'::starts_with('Hello')
            "#, Boolean(true));
        }

        #[test]
        fn test_starts_with_false() {
            verify_exact_value(r#"
                'Hello World'::starts_with('World')
            "#, Boolean(false))
        }

        #[test]
        fn test_strip_margin() {
            verify_exact_value(
                strip_margin(r#"
                |"|Code example:
                | |
                | |stocks where exchange is 'NYSE'
                | |"::strip_margin('|')"#, '|').as_str(),
                StringValue("Code example:\n\nstocks where exchange is 'NYSE'\n".into()),
            )
        }

        #[test]
        fn test_substring_defined() {
            verify_exact_value(r#"
                'Hello World'::substring(0, 5)
            "#, StringValue("Hello".into()));
        }

        #[test]
        fn test_superscript() {
            verify_exact_code(r#"
                123::superscript()
            "#, r#""¹²³""#)
        }
        
        #[test]
        fn test_to_string_() {
            verify_exact_value(r#"
                123::to_string()
            "#, StringValue("123".into()));
        }
    }

    /// Package "tools" tests
    #[cfg(test)]
    mod tools_tests {
        use super::*;
        use crate::interpreter::Interpreter;
        use crate::number_kind::NumberKind::F64Kind;
        use crate::packages::PackageOps;
        use crate::structures::HardStructure;
        use crate::structures::Structures::Hard;
        use crate::testdata::*;
        use crate::typed_values::TypedValue::*;
        use PackageOps::*;

        #[test]
        fn test_tools_compact() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_table_with(interpreter, r#"
                stocks = nsd::save(
                    "platform.compact.stocks",
                    [{ symbol: "DMX", exchange: "NYSE", last_sale: 99.99 },
                     { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                     { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                     { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                     { symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
                     { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 },
                     { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] 
                )
                delete stocks where last_sale > 1.0
                stocks
            "#,
                vec![
                    "|------------------------------------|",
                    "| id | symbol | exchange | last_sale |",
                    "|------------------------------------|",
                    "| 1  | UNO    | OTC      | 0.2456    |",
                    "| 3  | GOTO   | OTC      | 0.1428    |",
                    "| 5  | BOOM   | NASDAQ   | 0.0872    |",
                    "|------------------------------------|",
                ],
            );

            verify_exact_table_with(interpreter, r#"
                stocks::compact()
                stocks
            "#,
                vec![
                    "|------------------------------------|",
                    "| id | symbol | exchange | last_sale |",
                    "|------------------------------------|",
                    "| 0  | BOOM   | NASDAQ   | 0.0872    |",
                    "| 1  | UNO    | OTC      | 0.2456    |",
                    "| 2  | GOTO   | OTC      | 0.1428    |",
                    "|------------------------------------|",
                ],
            );
        }
        
        #[test]
        fn test_tools_describe_dataframe_literal() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_table_with(interpreter, r#"
                stocks =
                    |--------------------------------------|
                    | symbol | exchange | last_sale | rank |
                    |--------------------------------------|
                    | BOOM   | NYSE     | 113.76    | 1    |
                    | ABC    | AMEX     | 24.98     | 2    |
                    | JET    | NASDAQ   | 64.24     | 3    |
                    |--------------------------------------|
                stocks::describe()
            "#, vec![
                "|----------------------------------------------------------|",
                "| id | name      | type      | default_value | is_nullable |",
                "|----------------------------------------------------------|",
                "| 0  | symbol    | String(4) | null          | true        |",
                "| 1  | exchange  | String(6) | null          | true        |",
                "| 2  | last_sale | f64       | null          | true        |",
                "| 3  | rank      | i64       | null          | true        |",
                "|----------------------------------------------------------|"]);
        }

        #[test]
        fn test_tools_describe() {
            verify_exact_table(r#"
                { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 }::describe()
            "#, vec![
                    "|----------------------------------------------------------|",
                    "| id | name      | type      | default_value | is_nullable |",
                    "|----------------------------------------------------------|",
                    "| 0  | symbol    | String(3) | BIZ           | true        |",
                    "| 1  | exchange  | String(4) | NYSE          | true        |",
                    "| 2  | last_sale | f64       | 23.66         | true        |",
                    "|----------------------------------------------------------|",
                ],
            );

            // postfix
            verify_exact_table(r#"
                stocks = nsd::save(
                    "platform.describe.stocks",
                    Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                )
                stocks::describe()
            "#,
                vec![
                    "|----------------------------------------------------------|",
                    "| id | name      | type      | default_value | is_nullable |",
                    "|----------------------------------------------------------|",
                    "| 0  | symbol    | String(8) | null          | true        |",
                    "| 1  | exchange  | String(8) | null          | true        |",
                    "| 2  | last_sale | f64       | null          | true        |",
                    "|----------------------------------------------------------|",
                ],
            );
        }

        #[test]
        fn test_tools_fetch() {
            verify_exact_table(r#"
                stocks = nsd::save(
                    "platform.fetch.stocks",
                    Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                )
                let rows = [
                    { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                    { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                    { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] 
                rows ~> stocks
                stocks::fetch(2)
            "#,
                vec![
                    "|------------------------------------|",
                    "| id | symbol | exchange | last_sale |",
                    "|------------------------------------|",
                    "| 2  | JET    | NASDAQ   | 32.12     |",
                    "|------------------------------------|",
                ],
            )
        }

        #[test]
        fn test_tools_filter_over_array() {
            verify_exact_value(r#"
                (1..7)::filter(n -> (n % 2) == 0)
           "#,
                ArrayValue(Array::from(vec![
                    Number(I64Value(2)),
                    Number(I64Value(4)),
                    Number(I64Value(6)),
                ])),
            )
        }

        #[test]
        fn test_tools_filter_over_table() {
            verify_exact_table(r#"
                stocks = nsd::save(
                    "platform.filter_over_table.stocks",
                    Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                )
                let rows = [
                    { symbol: "WKRP", exchange: "NYSE", last_sale: 11.11 },
                    { symbol: "ACDC", exchange: "AMEX", last_sale: 37.43 },
                    { symbol: "UELO", exchange: "NYSE", last_sale: 91.82 }] 
                rows ~> stocks
                stocks::filter(row -> exchange is "AMEX")
           "#,
                vec![
                    "|------------------------------------|",
                    "| id | symbol | exchange | last_sale |",
                    "|------------------------------------|",
                    "| 0  | ACDC   | AMEX     | 37.43     |",
                    "|------------------------------------|",
                ],
            )
        }

        #[test]
        fn test_tools_latest() {
            verify_exact_value(r#"
                stocks = nsd::save(
                   "packages.tools_latest.stocks",
                    |--------------------------------|
                    | symbol | exchange  | last_sale |
                    |--------------------------------|
                    | GIF    | NYSE      | 11.75     |
                    | TRX    | NASDAQ    | 32.96     |
                    | SHMN   | OTCBB     | 5.02      |
                    | XCD    | OTCBB     | 1.37      |
                    | DRMQ   | OTHER_OTC | 0.02      |
                    | JTRQ   | OTHER_OTC | 0.0001    |
                    |--------------------------------|
                )
                delete stocks where last_sale < 1
                stocks::latest()
           "#, Number(I64Value(3)))
        }

        #[test]
        fn test_tools_keys_struct() {
            verify_exact_code(r#"
                stock = {symbol: "ZAP", exchange: "AMEX", last_sale: 56.88}
                stock::keys()
           "#, r#"["symbol", "exchange", "last_sale"]"#)
        }

        #[test]
        fn test_tools_keys_table() {
            verify_exact_code(r#"
                stocks = 
                    |--------------------------------|
                    | symbol | exchange  | last_sale |
                    |--------------------------------|
                    | TRX    | NASDAQ    | 32.96     |
                    | SHMN   | OTCBB     | 5.02      |
                    | XCD    | OTCBB     | 1.37      |
                    | JTRQ   | OTHER_OTC | 0.0001    |
                    |--------------------------------|
                stocks::keys()
           "#, r#"["symbol", "exchange", "last_sale"]"#)
        }

        #[test]
        fn test_tools_map_over_array() {
            verify_exact_value(r#"
                [1, 2, 3]::map(n -> n * 2)
           "#,
                ArrayValue(Array::from(vec![
                    Number(I64Value(2)),
                    Number(I64Value(4)),
                    Number(I64Value(6)),
                ])),
            )
        }

        #[test]
        fn test_tools_map_over_table() {
            verify_exact_table(r#"
                stocks = nsd::save(
                    "platform.map_over_table.stocks",
                    Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                )
                let rows = [
                    { symbol: "WKRP", exchange: "NYSE", last_sale: 11.11 },
                    { symbol: "ACDC", exchange: "AMEX", last_sale: 35.11 },
                    { symbol: "UELO", exchange: "NYSE", last_sale: 90.12 }] 
                rows ~> stocks
                stocks::map(row -> {
                    symbol: symbol,
                    exchange: exchange,
                    last_sale: last_sale,
                    magnitude: last_sale * 2.0
                })
           "#,
                vec![
                    "|------------------------------------------------|",
                    "| id | symbol | exchange | last_sale | magnitude |",
                    "|------------------------------------------------|",
                    "| 0  | WKRP   | NYSE     | 11.11     | 22.22     |",
                    "| 1  | ACDC   | AMEX     | 35.11     | 70.22     |",
                    "| 2  | UELO   | NYSE     | 90.12     | 180.24    |",
                    "|------------------------------------------------|",
                ],
            )
        }

        #[test]
        fn test_tools_pop() {
            verify_exact_table(r#"
                stocks = nsd::save(
                    "platform.pop.stocks",
                    Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                )
                let rows = [
                    { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                    { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                    { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] 
                rows ~> stocks
                stocks::pop()
            "#,
                vec![
                    "|------------------------------------|",
                    "| id | symbol | exchange | last_sale |",
                    "|------------------------------------|",
                    "| 2  | JET    | NASDAQ   | 32.12     |",
                    "|------------------------------------|",
                ],
            );
            verify_exact_table(r#"
                stocks = nsd::load("platform.pop.stocks")
                stocks::pop()
            "#,
                vec![
                    "|------------------------------------|",
                    "| id | symbol | exchange | last_sale |",
                    "|------------------------------------|",
                    "| 1  | BOOM   | NYSE     | 56.88     |",
                    "|------------------------------------|",
                ],
            );
        }

        #[test]
        fn test_tools_push_array_evaluate() {
            verify_exact_table(r#"
                stocks = [
                    { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                    { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                    { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }
                ]
                stocks = stocks::push({ symbol: "DEX", exchange: "OTC_BB", last_sale: 0.0086 })
                stocks
            "#,
                vec![
                    "|------------------------------------|",
                    "| id | symbol | exchange | last_sale |",
                    "|------------------------------------|",
                    "| 0  | ABC    | AMEX     | 12.49     |",
                    "| 1  | BOOM   | NYSE     | 56.88     |",
                    "| 2  | JET    | NASDAQ   | 32.12     |",
                    "| 3  | DEX    | OTC_BB   | 0.0086    |",
                    "|------------------------------------|",
                ],
            );
        }

        #[test]
        fn test_tools_push_table_evaluate() {
            verify_exact_table(r#"
                stocks = nsd::save(
                    "platform.push.stocks",
                    Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                )
                let rows = [
                    { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                    { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                    { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] 
                rows ~> stocks
                stocks::push({ symbol: "DEX", exchange: "OTC_BB", last_sale: 0.0086 })
                stocks
            "#,
                vec![
                    "|------------------------------------|",
                    "| id | symbol | exchange | last_sale |",
                    "|------------------------------------|",
                    "| 0  | ABC    | AMEX     | 12.49     |",
                    "| 1  | BOOM   | NYSE     | 56.88     |",
                    "| 2  | JET    | NASDAQ   | 32.12     |",
                    "| 3  | DEX    | OTC_BB   | 0.0086    |",
                    "|------------------------------------|",
                ],
            );
        }

        #[test]
        fn test_tools_push_tuple_evaluate() {
            verify_exact_table(
                r#"
                stocks = [
                    ("ABC", "AMEX", 12.49),
                    ("BOOM", "NYSE", 56.88),
                    ("JET", "NASDAQ", 32.12)
                ]
                stocks = stocks::push(("DEX", "OTC_BB", 0.0086))
                stocks::to_table()
            "#,
                vec![
                    r#"|--------------------------------|"#,
                    r#"| id | value                     |"#,
                    r#"|--------------------------------|"#,
                    r#"| 0  | ("ABC", "AMEX", 12.49)    |"#,
                    r#"| 1  | ("BOOM", "NYSE", 56.88)   |"#,
                    r#"| 2  | ("JET", "NASDAQ", 32.12)  |"#,
                    r#"| 3  | ("DEX", "OTC_BB", 0.0086) |"#,
                    r#"|--------------------------------|"#,
                ],
            );
        }

        #[test]
        fn test_tools_replay() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_value_whence(interpreter, r#"
                nsd::drop("platform.replay.stocks")
            "#, |result| matches!(result, Boolean(_)));
            interpreter = verify_exact_value_with(interpreter, r#"
                stocks = nsd::create_fn(
                    "platform.replay.stocks",
                    (symbol: String(8), exchange: String(8), last_sale: f64) -> {
                        symbol: symbol,
                        exchange: exchange,
                        last_sale: last_sale * 2.0,
                        rank: __row_id__ + 1
                    })
            "#, Boolean(true));
            interpreter = verify_exact_value_with(interpreter, r#"
                [{ symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                 { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
            "#,
            Number(I64Value(3)));
            interpreter = verify_exact_table_with(interpreter, r#"
                stocks::replay()
                stocks
            "#, vec![
                "|-------------------------------------------|",
                "| id | symbol | exchange | last_sale | rank |",
                "|-------------------------------------------|",
                "| 0  | BOOM   | NYSE     | 113.76    | 1    |",
                "| 1  | ABC    | AMEX     | 24.98     | 2    |",
                "| 2  | JET    | NASDAQ   | 64.24     | 3    |",
                "|-------------------------------------------|",
            ])
        }

        #[test]
        fn test_tools_reverse_arrays() {
            verify_exact_table(r#"
                ['cat', 'dog', 'ferret', 'mouse']::reverse::to_table
            "#,
                vec![
                    "|-------------|",
                    "| id | value  |",
                    "|-------------|",
                    "| 0  | mouse  |",
                    "| 1  | ferret |",
                    "| 2  | dog    |",
                    "| 3  | cat    |",
                    "|-------------|",
                ],
            )
        }

        #[test]
        fn test_tools_reverse_strings() {
            verify_exact_value(r#"
                backwards(a) -> a::reverse()
                "Hello World":::backwards()
            "#,
                StringValue("dlroW olleH".into()),
            );
        }

        #[test]
        fn test_tools_reverse_tables_function() {
            // fully-qualified (ephemeral)
            verify_exact_table(
                r#"
                stocks = [
                    { symbol: "ABC", exchange: "AMEX", last_sale: 12.33 },
                    { symbol: "BIZ", exchange: "NYSE", last_sale: 9.775 },
                    { symbol: "XYZ", exchange: "NASDAQ", last_sale: 89.11 }
                ]::to_table()
                stocks::reverse()
            "#,
                vec![
                    "|------------------------------------|",
                    "| id | symbol | exchange | last_sale |",
                    "|------------------------------------|",
                    "| 0  | XYZ    | NASDAQ   | 89.11     |",
                    "| 1  | BIZ    | NYSE     | 9.775     |",
                    "| 2  | ABC    | AMEX     | 12.33     |",
                    "|------------------------------------|",
                ],
            );
        }

        #[test]
        fn test_tools_reverse_tables_method() {
            // postfix (durable)
            verify_exact_table(r#"
                stocks = nsd::save(
                    "platform.reverse.stocks",
                    Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                )
                let rows = [
                    { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                    { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                    { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] 
                rows ~> stocks
                stocks::reverse()
            "#,
                vec![
                    "|------------------------------------|",
                    "| id | symbol | exchange | last_sale |",
                    "|------------------------------------|",
                    "| 0  | JET    | NASDAQ   | 32.12     |",
                    "| 1  | BOOM   | NYSE     | 56.88     |",
                    "| 2  | ABC    | AMEX     | 12.49     |",
                    "|------------------------------------|",
                ],
            );
        }

        #[test]
        fn test_tools_scan() {
            let mut interpreter = Interpreter::new();
            let result = interpreter.evaluate(r#"
                stocks = nsd::save(
                    "platform.scan.stocks",
                    Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                )
                let rows = [
                    { symbol: "ABC", exchange: "AMEX", last_sale: 12.33 },
                    { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                    { symbol: "BIZ", exchange: "NYSE", last_sale: 9.775 },
                    { symbol: "GOTO", exchange: "OTC", last_sale: 0.1442 },
                    { symbol: "XYZ", exchange: "NYSE", last_sale: 0.0289 }] 
                rows ~> stocks
                delete stocks where last_sale > 1.0
                stocks::scan()
            "#,
                )
                .unwrap();
            assert_eq!(
                result.to_table().unwrap().read_active_rows().unwrap(),
                vec![
                    make_scan_quote(0, "ABC", "AMEX", 12.33, false),
                    make_scan_quote(1, "UNO", "OTC", 0.2456, true),
                    make_scan_quote(2, "BIZ", "NYSE", 9.775, false),
                    make_scan_quote(3, "GOTO", "OTC", 0.1442, true),
                    make_scan_quote(4, "XYZ", "NYSE", 0.0289, true),
                ]
            )
        }

        #[test]
        fn test_tools_to_array_with_tuples_qualified() {
            verify_exact_code(r#"
                 ("a", "b", "c")::to_array()
            "#,
                r#"["a", "b", "c"]"#,
            );
        }

        #[test]
        fn test_tools_to_array_with_strings_qualified() {
            verify_exact_value(r#"
                 "Hello"::to_array()
            "#,
                ArrayValue(Array::from(vec![
                    CharValue('H'.into()),
                    CharValue('e'.into()),
                    CharValue('l'.into()),
                    CharValue('l'.into()),
                    CharValue('o'.into()),
                ])),
            );
        }

        #[test]
        fn test_tools_to_array_with_tables() {
            // fully qualified
            let mut interpreter = Interpreter::new();
            let result = interpreter
                .evaluate(r#"
                 [
                     { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                     { symbol: "DMX", exchange: "OTC_BB", last_sale: 1.17 }
                 ]::to_table::to_array()
            "#,
                )
                .unwrap();
            let params = vec![
                Parameter::new_with_default("symbol", FixedSizeType(StringType.into(), 3), StringValue("BIZ".into())),
                Parameter::new_with_default("exchange", FixedSizeType(StringType.into(), 6), StringValue("NYSE".into())),
                Parameter::new_with_default("last_sale", NumberType(F64Kind), Number(F64Value(23.66))),
            ];
            assert_eq!(
                result,
                ArrayValue(Array::from(vec![
                    Structured(Hard(HardStructure::new(
                        params.clone(),
                        vec![
                            StringValue("BIZ".into()),
                            StringValue("NYSE".into()),
                            Number(F64Value(23.66))
                        ]
                    ))),
                    Structured(Hard(HardStructure::new(
                        params.clone(),
                        vec![
                            StringValue("DMX".into()),
                            StringValue("OTC_BB".into()),
                            Number(F64Value(1.17))
                        ]
                    )))
                ]))
            );
        }

        #[test]
        fn test_tools_to_csv() {
            verify_exact_value(r#"
                stocks = nsd::save(
                    "platform.csv.stocks",
                    Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                )
                let rows = [
                    { symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
                    { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                    { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                    { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                    { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }] 
                rows ~> stocks
                stocks::to_csv()
            "#,
                ArrayValue(Array::from(vec![
                    StringValue(r#""ABC","AMEX",11.11"#.into()),
                    StringValue(r#""UNO","OTC",0.2456"#.into()),
                    StringValue(r#""BIZ","NYSE",23.66"#.into()),
                    StringValue(r#""GOTO","OTC",0.1428"#.into()),
                    StringValue(r#""BOOM","NASDAQ",0.0872"#.into()),
                ])),
            );
        }

        #[test]
        fn test_tools_to_json() {
            verify_exact_value(r#"
                stocks = nsd::save(
                    "platform.json.stocks",
                    Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                )
                let rows = [
                    { symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
                    { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                    { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                    { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                    { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }] 
                rows ~> stocks
                stocks::to_json()
            "#,
                ArrayValue(Array::from(vec![
                    StringValue(r#"{"symbol":"ABC","exchange":"AMEX","last_sale":11.11}"#.into()),
                    StringValue(r#"{"symbol":"UNO","exchange":"OTC","last_sale":0.2456}"#.into()),
                    StringValue(r#"{"symbol":"BIZ","exchange":"NYSE","last_sale":23.66}"#.into()),
                    StringValue(r#"{"symbol":"GOTO","exchange":"OTC","last_sale":0.1428}"#.into()),
                    StringValue(
                        r#"{"symbol":"BOOM","exchange":"NASDAQ","last_sale":0.0872}"#.into(),
                    ),
                ])),
            );
        }

        #[test]
        fn test_tools_to_table_with_arrays() {
            verify_exact_table(r#"
                ['cat', 'dog', 'ferret', 'mouse']::to_table()
            "#,
                vec![
                    "|-------------|",
                    "| id | value  |",
                    "|-------------|",
                    "| 0  | cat    |",
                    "| 1  | dog    |",
                    "| 2  | ferret |",
                    "| 3  | mouse  |",
                    "|-------------|",
                ],
            )
        }

        #[test]
        fn test_tools_to_table_with_hard_structures() {
            verify_exact_table(r#"
                Struct(
                    symbol: String(8) = "ABC",
                    exchange: String(8) = "NYSE",
                    last_sale: f64 = 45.67
                )::new::to_table()
            "#,
                vec![
                    "|------------------------------------|",
                    "| id | symbol | exchange | last_sale |",
                    "|------------------------------------|",
                    "| 0  | ABC    | NYSE     | 45.67     |",
                    "|------------------------------------|",
                ],
            )
        }

        #[test]
        fn test_tools_to_table_with_soft_and_hard_structures() {
            verify_exact_table(r#"
                stocks = [
                    { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                    { symbol: "DMX", exchange: "OTC_BB", last_sale: 1.17 }
                ]::to_table()

                [
                    stocks,
                    Struct(
                        symbol: String(8),
                        exchange: String(8),
                        last_sale: f64
                    )::new("ABC", "OTHER_OTC", 0.67),
                    { symbol: "TRX", exchange: "AMEX", last_sale: 29.88 },
                    { symbol: "BMX", exchange: "NASDAQ", last_sale: 46.11 }
                ]::to_table()
            "#,
                vec![
                    "|-------------------------------------|",
                    "| id | symbol | exchange  | last_sale |",
                    "|-------------------------------------|",
                    "| 0  | BIZ    | NYSE      | 23.66     |",
                    "| 1  | DMX    | OTC_BB    | 1.17      |",
                    "| 2  | ABC    | OTHER_OTC | 0.67      |",
                    "| 3  | TRX    | AMEX      | 29.88     |",
                    "| 4  | BMX    | NASDAQ    | 46.11     |",
                    "|-------------------------------------|",
                ],
            )
        }
    }

    /// Package "util" tests
    #[cfg(test)]
    mod util_tests {
        use super::*;
        use crate::data_types::DataType::ByteStringType;
        use crate::interpreter::Interpreter;
        use crate::packages::PackageOps;
        use crate::testdata::*;
        use crate::typed_values::TypedValue::*;
        use PackageOps::*;

        #[test]
        fn test_util_base62() {
            verify_exact_value(
                "'Hello World'::base62_encode",
                StringValue("73XpUgyMwkGr29M".into()),
            )
        }

        #[test]
        fn test_util_base64() {
            verify_exact_value(
                "'Hello World'::base64_encode",
                StringValue("SGVsbG8gV29ybGQ=".into()),
            )
        }

        #[test]
        fn test_util_gzip_and_gunzip() {
            verify_exact_value(r#"
                compressed = 'Hello World'::gzip()
                compressed::gunzip()
            "#, ByteStringValue(b"Hello World".to_vec()))
        }

        #[test]
        fn test_util_hex() {
            verify_exact_value(
                "'Hello World'::hex",
                StringValue("48656c6c6f20576f726c64".into()),
            )
        }

        #[test]
        fn test_util_md5() {
            verify_exact_code(
                "'Hello World'::md5()",
                "0Bb10a8db164e0754105b7a99be72e3fe5",
            )
        }

        #[test]
        fn test_util_md5_type() {
            verify_data_type("util::md5(a)", FixedSizeType(ByteStringType.into(), 16));
        }

        #[test]
        fn test_util_round() {
            verify_exact_code(
                "util::round(99.69333333333333, 4)", 
                "99.6933");
        }

        #[test]
        fn test_util_to_date_string() {
            verify_exact_code(r#"
                1376438453123::to_date
            "#, "2013-08-14T00:00:53.123Z")
        }

        #[test]
        fn test_util_to_string_number() {
            verify_exact_code(r#"
                "8"::to(i64())
            "#, r#"8"#)
        }

        #[test]
        fn test_util_to_string_array() {
            verify_exact_code(r#"
                "Hello there"::to(Array())
            "#, r#"["H", "e", "l", "l", "o", " ", "t", "h", "e", "r", "e"]"#)
        }

        #[test]
        fn test_util_to_string_binary() {
            verify_exact_code(r#"
                "Hello there"::to(ByteString())
            "#, "0B48656c6c6f207468657265")
        }

        #[test]
        fn test_util_to_ascii() {
            verify_exact_code("util::to_ascii(177)", "\"±\"")
        }

        #[test]
        fn test_util_to_hex() {
            verify_exact_code("util::hex('Hello World')", "\"48656c6c6f20576f726c64\"")
        }

        #[test]
        fn test_util_to_f32_to_u128() {
            use crate::numbers::Numbers::*;
            let mut interpreter = Interpreter::new();
            interpreter.evaluate("use util").unwrap();

            // floating-point kinds
            interpreter = verify_exact_value_whence(interpreter, "777_9311:::to_f64()", |n| {
                n == Number(F64Value(7779311.))
            });

            // signed-integer kinds
            interpreter =
                verify_exact_value_whence(interpreter, "1_234_5678_987.43:::to_i128()", |n| {
                    n == Number(I128Value(12345678987))
                });
            interpreter = verify_exact_value_whence(interpreter, "123456789.42:::to_i64()", |n| {
                n == Number(I64Value(123456789))
            });

            // unsigned-integer kinds
            interpreter = verify_exact_value_whence(interpreter, "12789.43:::to_u128()", |n| {
                n == Number(U128Value(12789))
            });

            // scope checks
            let mut interpreter = Interpreter::new();

            // initially 'to_u128' should not be in scope
            assert_eq!(interpreter.get("to_u128"), None);

            // import all conversion members
            interpreter.evaluate("use util").unwrap();

            // after the import, 'to_u128' should be in scope
            assert_eq!(
                interpreter.get("to_u128"),
                Some(PlatformOp(PackageOps::Utils(UtilsPkg::ToU128)))
            );
        }

        #[test]
        fn test_util_base62_decode() {
            verify_exact_value(r#"
                util::base62_decode('Hello World'::base62_encode)
            "#, ByteStringValue(b"\0\0\0\0\0Hello World".into()));
            
            verify_exact_value(r#"
                util::base62_decode(util::base62_encode('little brown fox'))
            "#, ByteStringValue(b"little brown fox".into()));
        }

        #[test]
        fn test_util_base64_decode() {
            verify_exact_value(r#"
                util::base64_decode('Hello World'::base64_encode)
            "#, ByteStringValue(b"Hello World".into()));
            
            verify_exact_value(
                "util::base64_decode('little brown fox'::base64_encode)",
                ByteStringValue(b"little brown fox".into()));
        }
    }

    /// Package "www" tests
    #[cfg(test)]
    mod www_tests {
        use crate::testdata::verify_exact_value;
        use crate::typed_values::TypedValue::StringValue;

        #[test]
        fn test_www_url_decode() {
            verify_exact_value(
                "www::url_decode('http%3A%2F%2Fshocktrade.com%3Fname%3Dthe%20hero%26t%3D9998')",
                StringValue("http://shocktrade.com?name=the hero&t=9998".to_string()),
            )
        }

        #[test]
        fn test_www_url_encode() {
            verify_exact_value(
                "www::url_encode('http://shocktrade.com?name=the hero&t=9998')",
                StringValue(
                    "http%3A%2F%2Fshocktrade.com%3Fname%3Dthe%20hero%26t%3D9998".to_string(),
                ),
            )
        }
    }
}
