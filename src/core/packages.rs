#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Platform Packages module
////////////////////////////////////////////////////////////////////

use crate::byte_code_compiler::ByteCodeCompiler;
use crate::compiler::Compiler;
use crate::connections::ConnectionTypes::BLOBStoreHandleType;
use crate::connections::{blob_stores, webservers, websockets};
use crate::data_types::DataType;
use crate::data_types::DataType::*;
use crate::dataframe::Dataframe;
use crate::dataframe::Dataframe::{DiskTable, EventSource, ModelTable, TableFn};
use crate::errors::Errors::*;
use crate::errors::SyntaxErrors::IllegalDate;
use crate::errors::TypeMismatchErrors::*;
use crate::errors::{column_not_found, throw};
use crate::expression::Expression::{CodeBlock, FunctionCall, Literal, Multiply, StructureExpression};
use crate::expression::{Expression, Observations};
use crate::extractions::*;
use crate::file_row_collection::FileRowCollection;
use crate::journaling::{EventSourceRowCollection, Journaling, TableFunction};
use crate::machine::Machine;
use crate::model_row_collection::ModelRowCollection;
use crate::namespaces::Namespace;
use crate::number_kind::NumberKind::*;
use crate::numbers::Numbers::*;
use crate::object_config::{HashIndexConfig, ObjectConfig};
use crate::parameter::Parameter;
use crate::row_collection::RowCollection;
use crate::sequences::Sequences::{TheArray, TheDataframe, TheRange, TheTuple};
use crate::sequences::{range_diff, Array, Sequence};
use crate::sprintf::StringPrinter;
use crate::structures::Structures::{Hard, Soft};
use crate::structures::{HardStructure, Row, SoftStructure, Structure, Structures};
use crate::type_engine::{TypeEngine, TypeHints};
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::*;
use crate::utils::*;
use crate::{server_engine, type_engine};
use async_trait::async_trait;
use chrono::{Datelike, Local, MappedLocalTime, NaiveDate, NaiveDateTime, TimeZone, Timelike, Weekday};
use crossterm::terminal;
use itertools::Itertools;
use mysql_async::prelude::Query;
use num_traits::ToPrimitive;
use once_cell::sync::Lazy;
use rand::prelude::ThreadRng;
use rand::{thread_rng, RngCore};
use serde::{Deserialize, Serialize};
use shared_lib::cnv_error;
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
pub const MINOR_VERSION: u8 = 50;
pub const VERSION: &str = "0.50";

// duration unit constants
pub const MILLIS: i64 = 1;
pub const SECONDS: i64 = 1000 * MILLIS;
pub const MINUTES: i64 = 60 * SECONDS;
pub const HOURS: i64 = 60 * MINUTES;
pub const DAYS: i64 = 24 * HOURS;

/// Builds a mapping of the package name to function vector
pub static PACKAGE_OPS: Lazy<im::HashMap<String, Vec<PackageOps>>> = Lazy::new(|| {
    PackageOps::get_contents().iter().fold(im::HashMap::new(), |mut hm, op| {
        hm.entry(op.get_package_name())
            .or_insert_with(Vec::new)
            .push(op.to_owned());
        hm
    })
});

/// Represents an Oxide Platform Package
#[async_trait]
pub trait Package: Send + Sync {
    fn evaluate(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)>;
    async fn evaluate_async(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        self.evaluate(ms, args)
    }
    fn get_name(&self) -> String;
    fn get_package_name(&self) -> String;
    fn get_description(&self) -> String;
    fn get_examples(&self) -> Vec<String>;
    fn get_parameter_types(&self) -> Vec<DataType>;
    fn get_return_type(&self) -> DataType;
    fn infer_type(&self, hints: TypeHints, _args: &[Expression]) -> (TypeHints, DataType) {
        (hints, self.get_return_type())
    }
}

/// Represents an enumeration of Oxide Platform Package Functions
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum PackageOps {
    Agg(AggPkg),
    Blobs(BlobsPkg),
    Chars(CharsPkg),
    Collections(CollectionsPkg),
    Dates(DatesPkg),
    Durations(DurationsPkg),
    Io(IoPkg),
    Math(MathPkg),
    Os(OsPkg),
    Oxide(OxidePkg),
    Strings(StringsPkg),
    Tables(TablesPkg),
    Utils(UtilsPkg),
    Www(WwwPkg),
}

impl PackageOps {
    /////////////////////////////////////////////////////////
    //      STATIC METHODS
    /////////////////////////////////////////////////////////

    pub fn decode(bytes: Vec<u8>) -> std::io::Result<PackageOps> {
        ByteCodeCompiler::unwrap_as_result(bincode::deserialize(bytes.as_slice()))
    }

    pub fn find_function(package: &str, name: &str) -> Option<PackageOps> {
        PACKAGE_OPS.get(package)
            .and_then(|ops| ops.iter().find(|pf| pf.get_name() == name))
            .cloned()
    }

    pub fn get_contents() -> Vec<PackageOps> {
        let mut contents = Vec::with_capacity(150);
        contents.extend(AggPkg::get_contents());
        contents.extend(BlobsPkg::get_contents());
        contents.extend(CollectionsPkg::get_contents());
        contents.extend(DatesPkg::get_contents());
        contents.extend(DurationsPkg::get_contents());
        contents.extend(IoPkg::get_contents());
        contents.extend(MathPkg::get_contents());
        contents.extend(OsPkg::get_contents());
        contents.extend(OxidePkg::get_contents());
        contents.extend(StringsPkg::get_contents());
        contents.extend(TablesPkg::get_contents());
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
            PackageOps::Blobs(pkg) => Box::new(pkg.clone()),
            PackageOps::Chars(pkg) => Box::new(pkg.clone()),
            PackageOps::Collections(pkg) => Box::new(pkg.clone()),
            PackageOps::Dates(pkg) => Box::new(pkg.clone()),
            PackageOps::Durations(pkg) => Box::new(pkg.clone()),
            PackageOps::Io(pkg) => Box::new(pkg.clone()),
            PackageOps::Math(pkg) => Box::new(pkg.clone()),
            PackageOps::Os(pkg) => Box::new(pkg.clone()),
            PackageOps::Oxide(pkg) => Box::new(pkg.clone()),
            PackageOps::Strings(pkg) => Box::new(pkg.clone()),
            PackageOps::Tables(pkg) => Box::new(pkg.clone()),
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
            .map(|(name, dt)| Parameter::new(name.to_string(), dt.clone()))
            .collect()
    }

    pub fn get_type(&self) -> DataType {
        PackageFunctionType(self.clone())
    }

    pub fn lookup_by_name(package_name: &str, name: &str) -> Option<PackageOps> {
        PACKAGE_OPS.get(package_name)
            .and_then(|ops| ops.iter().find(|op| op.get_name() == name)).cloned()
    }

    pub fn lookup_by_package_name(package_name: &str) -> Option<Vec<PackageOps>> {
        PACKAGE_OPS.get(package_name).cloned()
    }

    pub fn to_code(&self) -> String {
        let pkg = self.get_package_name();
        let name = self.get_name();
        let params = Parameter::render(&self.get_parameters());
        format!("{pkg}::{name}({params}){}", match self.get_return_type().to_code() {
            s if !s.is_empty() => format!(": {}", s),
            _ => String::new()
        })
    }

    /// Applies the given function to every item in items
    fn apply_fn_over_vec(
        ms: Machine,
        items: &[TypedValue],
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

}

#[async_trait]
impl Package for PackageOps {
    fn evaluate(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        self.get_package().evaluate(ms, args)
    }

    async fn evaluate_async(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        self.get_package().evaluate_async(ms, args).await
    }

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

    fn infer_type(&self, hints: TypeHints, args: &[Expression]) -> (TypeHints, DataType) {
        self.get_package().infer_type(hints, args)
    }
}

/// Represents a Data Format
pub enum DataFormats {
    CSV,
    JSON,
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

#[async_trait]
impl Package for AggPkg {
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
}

/// BLOB Stores
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum BlobsPkg {
    Append,
    Close,
    Create,
    Entries,
    Len,
    Load,
    Read,
    Truncate,
    Update,
}

impl BlobsPkg {
    async fn do_blobs_append_async(
        ms: Machine,
        v0: TypedValue,
        v1: TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let store_id = pull_blobstore_uuid(&v0)?;
        blob_stores::append(store_id, v1).await.map(|v| (ms, v))
    }

    fn do_blobs_close(
        ms: Machine,
        v0: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let store_id = pull_blobstore_uuid(&v0)?;
        blob_stores::close(store_id).map(|v| (ms, v))
    }

    fn do_blobs_create(
        ms: Machine,
        v0: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let path = pull_string(v0)?;
        blob_stores::create(path.as_str()).map(|v| (ms, v))
    }

    fn do_blobs_load(
        ms: Machine,
        v0: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let path = pull_string(v0)?;
        blob_stores::load(path.as_str()).map(|v| (ms, v))
    }

    async fn do_blobs_entries_async(
        ms: Machine,
        v0: TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let store_id = pull_blobstore_uuid(&v0)?;
        blob_stores::entries(store_id).await.map(|v| (ms, v))
    }

    async fn do_blobs_len_async(
        ms: Machine,
        v0: TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let store_id = pull_blobstore_uuid(&v0)?;
        blob_stores::len(store_id).await.map(|n| (ms, Number(U64Value(n))))
    }

    async fn do_blobs_read_async(
        ms: Machine,
        v0: TypedValue,
        v1: TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let store_id = pull_blobstore_uuid(&v0)?;
        let blob_id = pull_uuid(&v1)?;
        blob_stores::read(store_id, blob_id).await.map(|v| (ms, v))
    }

    async fn do_blobs_truncate_async(
        ms: Machine,
        v0: TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let store_id = pull_blobstore_uuid(&v0)?;
        blob_stores::truncate(store_id).await.map(|v| (ms, v))
    }

    async fn do_blobs_update_async(
        ms: Machine,
        v0: TypedValue,
        v1: TypedValue,
        v2: TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        if ms.is_debugging() { println!("packages: do_blobs_update_async {:?}, {:?}, {:?}", v0, v1, v2); }
        let store_id = pull_blobstore_uuid(&v0)?;
        let blob_id = pull_uuid(&v1)?;
        blob_stores::update(store_id, blob_id, &v2).await.map(|v| (ms, v))
    }

    pub fn get_contents() -> Vec<PackageOps> {
        vec![
            PackageOps::Blobs(BlobsPkg::Append),
            PackageOps::Blobs(BlobsPkg::Close),
            PackageOps::Blobs(BlobsPkg::Create),
            PackageOps::Blobs(BlobsPkg::Entries),
            PackageOps::Blobs(BlobsPkg::Len),
            PackageOps::Blobs(BlobsPkg::Load),
            PackageOps::Blobs(BlobsPkg::Read),
            PackageOps::Blobs(BlobsPkg::Truncate),
            PackageOps::Blobs(BlobsPkg::Update),
        ]
    }
}

#[async_trait]
impl Package for BlobsPkg {
    fn evaluate(&self, ms: Machine, args: Vec<TypedValue>) -> std::io::Result<(Machine, TypedValue)> {
        if ms.is_debugging() { println!("packages: evaluate {:?}", self); }
        match self {
            BlobsPkg::Append => extract_value_fn2(ms, args, |ms, v0, v1| {
                let store_id = pull_blobstore_uuid(v0)?;
                blob_stores::append_blocking(store_id, v1).map(|v| (ms, v))
            }),
            BlobsPkg::Close => extract_value_fn1(ms, args, Self::do_blobs_close),
            BlobsPkg::Create => extract_value_fn1(ms, args, |ms, v0| Self::do_blobs_create(ms, v0)),
            BlobsPkg::Entries => extract_value_fn1(ms, args, |ms, v0| {
                let store_id = pull_blobstore_uuid(v0)?;
                blob_stores::entries_blocking(store_id).map(|v| (ms, v))
            }),
            BlobsPkg::Len => extract_value_fn1(ms, args, |ms, v0| {
                let store_id = pull_blobstore_uuid(v0)?;
                blob_stores::len_blocking(store_id).map(|n| (ms, Number(U64Value(n))))
            }),
            BlobsPkg::Load => extract_value_fn1(ms, args, |ms, v0| Self::do_blobs_load(ms, v0)),
            BlobsPkg::Read => extract_value_fn2(ms, args, |ms, v0, v1| {
                let store_id = pull_blobstore_uuid(v0)?;
                let blob_id = pull_uuid(v1)?;
                blob_stores::read_blocking(store_id, blob_id).map(|v| (ms, v))
            }),
            BlobsPkg::Truncate => extract_value_fn1(ms, args, |ms, value| {
                let store_id = pull_blobstore_uuid(value)?;
                blob_stores::truncate_blocking(store_id).map(|v| (ms, v))
            }),
            BlobsPkg::Update => extract_value_fn3(ms, args, |ms, v0, v1, v2| {
                let store_id = pull_blobstore_uuid(v0)?;
                let blob_id = pull_uuid(v1)?;
                blob_stores::update_blocking(store_id, blob_id, v2).map(|v| (ms, v))
            }),
        }
    }

    async fn evaluate_async(&self, ms: Machine, args: Vec<TypedValue>) -> std::io::Result<(Machine, TypedValue)> {
        if ms.is_debugging() { println!("packages: evaluate_async {:?}", self); }
        match self {
            BlobsPkg::Append => extract_value_fn2_async(ms, args, |ms, v0, v1| Self::do_blobs_append_async(ms, v0, v1)).await,
            BlobsPkg::Close => extract_value_fn1(ms, args, |ms, v0| Self::do_blobs_close(ms, v0)),
            BlobsPkg::Create => extract_value_fn1(ms, args, |ms, v0| Self::do_blobs_create(ms, v0)),
            BlobsPkg::Entries => extract_value_fn1_async(ms, args, |ms, v0| Self::do_blobs_entries_async(ms, v0)).await,
            BlobsPkg::Len => extract_value_fn1_async(ms, args, |ms, v0| Self::do_blobs_len_async(ms, v0)).await,
            BlobsPkg::Load => extract_value_fn1(ms, args, |ms, v0| Self::do_blobs_load(ms, v0)),
            BlobsPkg::Read => extract_value_fn2_async(ms, args, |ms, v0, v1| Self::do_blobs_read_async(ms, v0, v1)).await,
            BlobsPkg::Truncate => extract_value_fn1_async(ms, args, |ms, v0| Self::do_blobs_truncate_async(ms, v0)).await,
            BlobsPkg::Update => extract_value_fn3_async(ms, args, |ms, v0, v1, v2| Self::do_blobs_update_async(ms, v0, v1, v2)).await,
        }
    }

    fn get_name(&self) -> String {
        (match self {
            BlobsPkg::Append => "append",
            BlobsPkg::Close => "close",
            BlobsPkg::Create => "create",
            BlobsPkg::Entries => "entries",
            BlobsPkg::Len => "len",
            BlobsPkg::Load => "load",
            BlobsPkg::Read => "read",
            BlobsPkg::Truncate => "truncate",
            BlobsPkg::Update => "update",
        }).into()
    }

    fn get_package_name(&self) -> String {
        "blobs".into()
    }

    fn get_description(&self) -> String {
        (match self {
            BlobsPkg::Append => "Appends a BLOB",
            BlobsPkg::Close => "Closes a BLOB",
            BlobsPkg::Create => "Creates a new BLOB Store",
            BlobsPkg::Entries => "Returns all BLOB entries",
            BlobsPkg::Len => "Returns the size in bytes of the BLOB",
            BlobsPkg::Load => "Loads an existing BLOB Store",
            BlobsPkg::Read => "Reads a BLOB",
            BlobsPkg::Truncate => "Truncates a new BLOB Store",
            BlobsPkg::Update => "Updates a BLOB",
        }).into()
    }

    fn get_examples(&self) -> Vec<String> {
        match self {
            BlobsPkg::Append
            | BlobsPkg::Close
            | BlobsPkg::Create
            | BlobsPkg::Read
            | BlobsPkg::Truncate => vec![
                strip_margin(r#"
                    |let bs = blobs::create("builtins.blob.append")
                    |bs::truncate()
                    |let id = bs::append("Hello World")
                    |let result = bs::read(id)
                    |blobs::close(id)
                    |result
                "#, '|')
            ],
            BlobsPkg::Entries => vec![
                strip_margin(r#"
                    |let bs = blobs::create("builtins.blob.entries")
                    |bs::truncate()
                    |bs::append("Hello World")
                    |bs::append("The little brown fox")
                    |bs::append("Goodbye World")
                    |bs::entries()
                "#, '|')
            ],
            BlobsPkg::Len => vec![
                strip_margin(r#"
                    |let bs = blobs::create("builtins.blob.len")
                    |bs::truncate()
                    |bs::append("Hello World")
                    |bs::append("The little brown fox")
                    |bs::append("Goodbye World")
                    |bs::len()
                "#, '|')
            ],
            BlobsPkg::Load => vec![],
            BlobsPkg::Update => vec![
                strip_margin(r#"
                    |let bs = blobs::create("builtins.blob.append")
                    |bs::truncate()
                    |let id0 = bs::append("Hello World")
                    |let id1 = bs::update(id0, "The brown fox")
                    |bs::read(id1)
                "#, '|')
            ],
        }
    }

    fn get_parameter_types(&self) -> Vec<DataType> {
        match self {
            // BLOBStoreHandle
            BlobsPkg::Close
            | BlobsPkg::Entries
            | BlobsPkg::Len
            | BlobsPkg::Truncate => vec![ConnectionType(BLOBStoreHandleType)],
            // (BLOBStoreHandle, UUID)
            BlobsPkg::Read => vec![ConnectionType(BLOBStoreHandleType), UUIDType],
            // (BLOBStoreHandle, UUID, ???)
            BlobsPkg::Update => vec![
                ConnectionType(BLOBStoreHandleType),
                UUIDType,
                RuntimeResolvedType
            ],
            // (BLOBStoreHandle, ???)
            BlobsPkg::Append => vec![ConnectionType(BLOBStoreHandleType), RuntimeResolvedType],
            // String
            BlobsPkg::Create
            | BlobsPkg::Load => vec![StringType],
        }
    }

    fn get_return_type(&self) -> DataType {
        match self {
            // Boolean
            BlobsPkg::Close | BlobsPkg::Truncate => BooleanType,
            // BLOBStoreHandle
            BlobsPkg::Create | BlobsPkg::Load => ConnectionType(BLOBStoreHandleType),
            // Number::i64
            BlobsPkg::Len => NumberType(I64Kind),
            // Table
            BlobsPkg::Entries => TableType(blob_stores::get_metadata_parameters()),
            // UUID
            BlobsPkg::Append | BlobsPkg::Update => UUIDType,
            // ???
            BlobsPkg::Read => RuntimeResolvedType,

        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum CharsPkg {
    Lower,
    Upper,
}

#[async_trait]
impl Package for CharsPkg {
    fn evaluate(&self, ms: Machine, args: Vec<TypedValue>) -> std::io::Result<(Machine, TypedValue)> {
        match self {
            CharsPkg::Lower => extract_char_fn1(ms, args, |ms, c| Ok((ms, CharValue(c.to_ascii_lowercase())))),
            CharsPkg::Upper => extract_char_fn1(ms, args, |ms, c| Ok((ms, CharValue(c.to_ascii_uppercase())))),
        }
    }

    fn get_name(&self) -> String {
        match self {
            CharsPkg::Lower => "lower".into(),
            CharsPkg::Upper => "upper".into(),
        }
    }

    fn get_package_name(&self) -> String {
        "chars".into()
    }

    fn get_description(&self) -> String {
        match self {
            CharsPkg::Lower => "Converts the character to lowercase".into(),
            CharsPkg::Upper => "Converts the character to uppercase".into(),
        }
    }

    fn get_examples(&self) -> Vec<String> {
        match self {
            CharsPkg::Lower => vec!["'A'::lower()".into()],
            CharsPkg::Upper => vec!["'a'::upper()".into()],
        }
    }

    fn get_parameter_types(&self) -> Vec<DataType> {
        match self {
            CharsPkg::Lower
            | CharsPkg::Upper => vec![CharType]
        }
    }

    fn get_return_type(&self) -> DataType {
        match self {
            CharsPkg::Lower
            | CharsPkg::Upper => CharType
        }
    }
}

/// Collections package
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum CollectionsPkg {
    Contains,
    Filter,
    Head,
    IsEmpty,
    Keys,
    Len,
    Map,
    Pop,
    Push,
    Reduce,
    Reverse,
    Tail,
}

impl CollectionsPkg {
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

    fn do_tools_length(ms: Machine, value: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        let result = match value.to_sequence()? {
            TheArray(array) => Number(I64Value(array.len() as i64)),
            TheDataframe(df) => Number(I64Value(df.len()? as i64)),
            TheRange(a, b, incl) => range_diff(&a, &b, incl),
            TheTuple(tuple) => Number(I64Value(tuple.len() as i64)),
        };
        Ok((ms, result))
    }

    pub fn do_tools_map(
        ms: Machine,
        items: &TypedValue,
        function: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match function {
            Function { .. } =>
                match items.to_sequence()? {
                    TheArray(array) => {
                        PackageOps::apply_fn_over_vec(ms, &array.get_values(), function, |_item, result| {
                            Ok(Some(result))
                        }, |items| ArrayValue(Array::from(items)))
                    }
                    TheDataframe(df) => {
                        PackageOps::apply_fn_over_table(ms, &df, function, |_item, result| {
                            Ok(Some(result))
                        })
                    }
                    TheRange(..) => Self::do_tools_map(ms, &items.to_array()?, function),
                    TheTuple(items) => {
                        PackageOps::apply_fn_over_vec(ms, &items, function, |_item, result| {
                            Ok(Some(result))
                        }, |items| TupleValue(items))
                    }
                }
            z => throw(TypeMismatch(FunctionExpected(z.to_code()))),
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

    pub fn get_tools_describe_parameters() -> Vec<Parameter> {
        vec![
            Parameter::new("name", StringType),
            Parameter::new("type", StringType),
            Parameter::new("default_value", StringType),
            Parameter::new("is_nullable", BooleanType),
        ]
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

    fn do_arrays_pop(
        ms: Machine,
        value: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let array = pull_array(value)?;
        let (new_array, _item) = array.pop();
        Ok((ms, ArrayValue(new_array)))
    }

    pub fn do_arrays_push(
        ms: Machine,
        items: &TypedValue,
        item: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let mut array = pull_array(items)?;
        array.push(item.clone());
        Ok((ms, ArrayValue(array)))
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
            PackageOps::Collections(CollectionsPkg::Contains),
            PackageOps::Collections(CollectionsPkg::Filter),
            PackageOps::Collections(CollectionsPkg::Head),
            PackageOps::Collections(CollectionsPkg::IsEmpty),
            PackageOps::Collections(CollectionsPkg::Keys),
            PackageOps::Collections(CollectionsPkg::Len),
            PackageOps::Collections(CollectionsPkg::Map),
            PackageOps::Collections(CollectionsPkg::Pop),
            PackageOps::Collections(CollectionsPkg::Push),
            PackageOps::Collections(CollectionsPkg::Reduce),
            PackageOps::Collections(CollectionsPkg::Reverse),
            PackageOps::Collections(CollectionsPkg::Tail),
        ]
    }

    fn infer_head(
        &self,
        hints: TypeHints,
        args: &[Expression]
    ) -> (TypeHints, DataType) {
        match self {
            CollectionsPkg::Head =>
                match args {
                    [container] =>
                        match TypeEngine::infer(hints, container) {
                            (hints, ArrayType(dt)) => (hints, type_engine::unbox(dt)),
                            (hints, StringType) => (hints, CharType),
                            (hints, TableType(params)) => (hints, StructureType(params.clone())),
                            (hints, TupleType(dts)) => (hints, dts.first().cloned().unwrap_or(RuntimeResolvedType)),
                            (hints, _) => (hints, self.get_return_type())
                        }
                    _ => (hints, self.get_return_type())
                }
            _ => (hints, self.get_return_type())
        }
    }
}

#[async_trait]
impl Package for CollectionsPkg {
    fn evaluate(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match self {
            CollectionsPkg::Contains => extract_value_fn2(ms, args, |ms, items, item| Ok((ms, Boolean(items.contains(item))))),
            CollectionsPkg::Filter => extract_value_fn2(ms, args, Self::do_tools_filter),
            CollectionsPkg::Head => extract_value_fn1(ms, args, |ms, v| Ok((ms, v.head()))),
            CollectionsPkg::IsEmpty => extract_array_fn1(ms, args, |a| Boolean(a.is_empty())),
            CollectionsPkg::Keys => extract_value_fn1(ms, args, Self::do_tools_keys),
            CollectionsPkg::Len => extract_value_fn1(ms, args, Self::do_tools_length),
            CollectionsPkg::Map => extract_value_fn2(ms, args, Self::do_tools_map),
            CollectionsPkg::Pop => extract_value_fn1(ms, args, Self::do_arrays_pop),
            CollectionsPkg::Push => extract_value_fn2(ms, args, Self::do_arrays_push),
            CollectionsPkg::Reduce => extract_value_fn3(ms, args, Self::do_arrays_reduce),
            CollectionsPkg::Reverse => extract_value_fn1(ms, args, Self::do_arrays_reverse),
            CollectionsPkg::Tail => extract_value_fn1(ms, args, |ms, v| Ok((ms, v.tail()))),
        }
    }

    fn get_name(&self) -> String {
        (match self {
            CollectionsPkg::Contains => "contains",
            CollectionsPkg::Filter => "filter",
            CollectionsPkg::Head => "head",
            CollectionsPkg::IsEmpty => "is_empty",
            CollectionsPkg::Keys => "keys",
            CollectionsPkg::Len => "len",
            CollectionsPkg::Map => "map",
            CollectionsPkg::Pop => "pop",
            CollectionsPkg::Push => "push",
            CollectionsPkg::Reduce => "reduce",
            CollectionsPkg::Reverse => "reverse",
            CollectionsPkg::Tail => "tail",
        }).into()
    }

    fn get_package_name(&self) -> String {
        "collections".into()
    }

    fn get_description(&self) -> String {
        (match self {
            CollectionsPkg::Contains => "Returns true if the array contains the specific item",
            CollectionsPkg::Filter => "Filters an array based on a function",
            CollectionsPkg::Head => "Returns true if the array head contains the specific item",
            CollectionsPkg::IsEmpty => "Returns true if the array is empty",
            CollectionsPkg::Keys => "returns the keys of a structure (column names of a table)",
            CollectionsPkg::Len => "Returns the length of an array",
            CollectionsPkg::Map => "Transform an array based on a function",
            CollectionsPkg::Pop => "Removes and returns a value or object from an array",
            CollectionsPkg::Push => "Appends a value or object to an array",
            CollectionsPkg::Reduce => "Reduces an array to a single value",
            CollectionsPkg::Reverse => "Returns a reverse copy of an array",
            CollectionsPkg::Tail => "Every element in an array after the first element",
        }).into()
    }

    fn get_examples(&self) -> Vec<String> {
        match self {
            CollectionsPkg::Contains => vec![
                strip_margin(r#"
                    |1..7::contains(5)
               "#, '|')
            ],
            CollectionsPkg::Filter => vec![
                strip_margin(r#"
                    |1..7::filter(n -> (n % 2) == 0)
               "#, '|')
            ],
            CollectionsPkg::Head => vec![
                strip_margin(r#"
                    |['abc', 'def', 'ghi']::head()
               "#, '|')
            ],
            CollectionsPkg::IsEmpty => vec![
                strip_margin(r#"
                    |[1, 3, 5]::is_empty
               "#, '|'),
                strip_margin(r#"
                    |[]::is_empty
               "#, '|')
            ],
            CollectionsPkg::Keys => vec![
                strip_margin(r#"
                    |stock = { symbol: "WKRP", exchange: "NYSE", last_sale: 11.11 }
                    |stock::keys()
                "#, '|'),
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
            CollectionsPkg::Len => vec![
                strip_margin(r#"
                    |[1, 5, 2, 4, 6, 0]::len()
                "#, '|'),
                r#"'The little brown fox'::len()"#.into(),
                strip_margin(r#"
                    |stocks = tables::save(
                    |   "examples.table_len.stocks",
                    |   [{ symbol: "WKRP", exchange: "NYSE", last_sale: 11.11 },
                    |    { symbol: "ACDC", exchange: "AMEX", last_sale: 35.11 },
                    |    { symbol: "UELO", exchange: "NYSE", last_sale: 90.12 }]
                    |)
                    |stocks::len()
                "#, '|')
            ],
            CollectionsPkg::Map => vec![
                strip_margin(r#"
                    |[1, 2, 3]::map(n -> n * 2)
                "#, '|'),
                strip_margin(r#"
                    |stocks = tables::save(
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
            CollectionsPkg::Pop => vec![
                strip_margin(r#"
                    |stocks = []
                    |stocks = stocks::push({ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 })
                    |stocks = stocks::push({ symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 })
                    |stocks
                "#, '|')
            ],
            CollectionsPkg::Push => vec![
                strip_margin(r#"
                    |stocks = [
                    |    { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                    |    { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                    |    { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }
                    |]
                    |stocks::push({ symbol: "DEX", exchange: "OTC_BB", last_sale: 0.0086 })
                    |stocks::to(Table)
                "#, '|')
            ],
            CollectionsPkg::Reduce => vec![
                strip_margin(r#"
                    |1..=5::reduce(0, (a, b) -> a + b)
                "#, '|'),
                strip_margin(r#"
                    |numbers = [1, 2, 3, 4, 5]
                    |numbers::reduce(0, (a, b) -> a + b)
                "#,
                             '|')
            ],
            CollectionsPkg::Reverse => vec![
                strip_margin(r#"
                    |['cat', 'dog', 'ferret', 'mouse']::reverse()
                "#, '|')
            ],
            CollectionsPkg::Tail => vec![
                strip_margin(r#"
                    ||--------------------------------------|
                    || symbol | exchange | last_sale | rank |
                    ||--------------------------------------|
                    || BOOM   | NYSE     | 113.76    | 1    |
                    || ABC    | AMEX     | 24.98     | 2    |
                    || JET    | NASDAQ   | 64.24     | 3    |
                    ||--------------------------------------|
                    |::tail()
                "#, '|')
            ],
        }
    }

    fn get_parameter_types(&self) -> Vec<DataType> {
        match self {
            CollectionsPkg::Contains => vec![
                ArrayType(RuntimeResolvedType.into()),
                RuntimeResolvedType,
            ],
            CollectionsPkg::Filter => vec![
                ArrayType(RuntimeResolvedType.into()),
                FunctionType(
                    vec![Parameter::new("item", RuntimeResolvedType)],
                    BooleanType.into(),
                ),
            ],
            CollectionsPkg::Head => vec![RuntimeResolvedType],
            CollectionsPkg::IsEmpty => vec![RuntimeResolvedType],
            CollectionsPkg::Keys => vec![StructureType(vec![])],
            CollectionsPkg::Len => vec![RuntimeResolvedType],
            CollectionsPkg::Map => vec![
                ArrayType(RuntimeResolvedType.into()),
                FunctionType(
                    vec![Parameter::new("item", RuntimeResolvedType)],
                    RuntimeResolvedType.into(),
                ),
            ],
            CollectionsPkg::Pop | CollectionsPkg::Reverse => vec![
                ArrayType(RuntimeResolvedType.into())
            ],
            CollectionsPkg::Push => vec![
                ArrayType(RuntimeResolvedType.into()), RuntimeResolvedType
            ],
            CollectionsPkg::Reduce => vec![
                ArrayType(RuntimeResolvedType.into()), RuntimeResolvedType, FunctionType(vec![
                    Parameter::new("a", RuntimeResolvedType),
                    Parameter::new("b", RuntimeResolvedType),
                ], RuntimeResolvedType.into())
            ],
            CollectionsPkg::Tail => vec![RuntimeResolvedType],
        }
    }

    fn get_return_type(&self) -> DataType {
        match self {
            // Array()
            CollectionsPkg::Pop
            | CollectionsPkg::Push => ArrayType(RuntimeResolvedType.into()),
            // Array(String)
            CollectionsPkg::Keys => ArrayType(StringType.into()),
            // Array(???)
            CollectionsPkg::Filter
            | CollectionsPkg::Map
            | CollectionsPkg::Reverse
            | CollectionsPkg::Tail => ArrayType(RuntimeResolvedType.into()),
            // Number
            CollectionsPkg::Len => NumberType(I64Kind),
            // Boolean
            CollectionsPkg::Contains
            | CollectionsPkg::IsEmpty => BooleanType,
            // ???
            CollectionsPkg::Head
            | CollectionsPkg::Reduce => RuntimeResolvedType,
        }
    }

    fn infer_type(&self, hints: TypeHints, args: &[Expression]) -> (TypeHints, DataType) {
        match self {
            CollectionsPkg::Head => self.infer_head(hints, args),
            _ => (hints, self.get_return_type()),
        }
    }
}

/// Dates package
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum DatesPkg {
    DateDay,
    DateHour12,
    DateHour24,
    DateMinus,
    DateMinute,
    DateMonth,
    DatePlus,
    DateSecond,
    DateYear,
    IsLeapYear,
    IsWeekday,
    IsWeekend,
    ToMillis,
}

impl DatesPkg {
    fn adapter_pf_fn1<F>(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
        f: F,
    ) -> std::io::Result<(Machine, TypedValue)>
    where
        F: Fn(Machine, &TypedValue, &DatesPkg) -> std::io::Result<(Machine, TypedValue)>,
    {
        match args.as_slice() {
            [a] => f(ms, a, self),
            args => throw(TypeMismatch(ArgumentsMismatched(1, args.len()))),
        }
    }

    fn do_dates_date_part(
        ms: Machine,
        value: &TypedValue,
        plat: &DatesPkg,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match value {
            DateTimeValue(epoch_millis) => {
                let datetime =
                    match Local.timestamp_millis_opt(*epoch_millis) {
                        MappedLocalTime::Single(dt) => dt,
                        _ => return throw(Exact(format!("Incorrect timestamp_millis {}", epoch_millis))),
                    };
                match plat {
                    DatesPkg::DateDay => Ok((ms, Number(I64Value(datetime.day() as i64)))),
                    DatesPkg::DateHour12 => Ok((ms, Number(I64Value(datetime.hour12().1 as i64)))),
                    DatesPkg::DateHour24 => Ok((ms, Number(I64Value(datetime.hour() as i64)))),
                    DatesPkg::DateMinute => Ok((ms, Number(I64Value(datetime.minute() as i64)))),
                    DatesPkg::DateMonth => Ok((ms, Number(I64Value(datetime.month() as i64)))),
                    DatesPkg::DateSecond => Ok((ms, Number(I64Value(datetime.second() as i64)))),
                    DatesPkg::DateYear => Ok((ms, Number(I64Value(datetime.year() as i64)))),
                    DatesPkg::IsLeapYear => Self::is_leapyear(ms, value),
                    DatesPkg::IsWeekday => Ok((ms, Boolean(Self::is_weekday(*epoch_millis)?))),
                    DatesPkg::IsWeekend => Ok((ms, Boolean(Self::is_weekend(*epoch_millis)?))),
                    pf => throw(PlatformOpError(PackageOps::Dates(pf.to_owned()))),
                }
            }
            other => throw(TypeMismatch(DateExpected(other.to_code()))),
        }
    }

    fn do_dates_date_minus(
        ms: Machine,
        date: &TypedValue,
        duration: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        Ok((ms, DateTimeValue(date.to_i64() - duration.to_i64())))
    }

    fn do_dates_date_plus(
        ms: Machine,
        date: &TypedValue,
        duration: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        Ok((ms, DateTimeValue(date.to_i64() + duration.to_i64())))
    }

    fn do_dates_to_millis(
        ms: Machine,
        date: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        Ok((ms, Number(I64Value(date.to_i64()))))
    }

    pub fn get_contents() -> Vec<PackageOps> {
        vec![
            PackageOps::Dates(DatesPkg::DateDay),
            PackageOps::Dates(DatesPkg::DateHour12),
            PackageOps::Dates(DatesPkg::DateHour24),
            PackageOps::Dates(DatesPkg::DateMinute),
            PackageOps::Dates(DatesPkg::DateMonth),
            PackageOps::Dates(DatesPkg::DateSecond),
            PackageOps::Dates(DatesPkg::DateYear),
            PackageOps::Dates(DatesPkg::IsLeapYear),
            PackageOps::Dates(DatesPkg::IsWeekday),
            PackageOps::Dates(DatesPkg::IsWeekend),
            PackageOps::Dates(DatesPkg::DateMinus),
            PackageOps::Dates(DatesPkg::DatePlus),
            PackageOps::Dates(DatesPkg::ToMillis),
        ]
    }

    pub fn is_leapyear(ms: Machine, value: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        let state = match value {
            DateTimeValue(millis) => match millis_to_naive_date(*millis) {
                Some(date) => date.leap_year(),
                None => return throw(SyntaxError(IllegalDate(value.to_code()))),
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
        let nanos = ((epoch_millis % 1000) * 1_000_000) as u32;
        match NaiveDateTime::from_timestamp_opt(secs, nanos) {
            Some(datetime) => Ok(datetime.date()),
            None => throw(Exact(format!("Incorrect timestamp_millis {}", epoch_millis))),
        }
    }
}

#[async_trait]
impl Package for DatesPkg {
    fn evaluate(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match self {
            DatesPkg::DateDay => self.adapter_pf_fn1(ms, args, Self::do_dates_date_part),
            DatesPkg::DateHour24 => self.adapter_pf_fn1(ms, args, Self::do_dates_date_part),
            DatesPkg::DateHour12 => self.adapter_pf_fn1(ms, args, Self::do_dates_date_part),
            DatesPkg::DateMinute => self.adapter_pf_fn1(ms, args, Self::do_dates_date_part),
            DatesPkg::DateMonth => self.adapter_pf_fn1(ms, args, Self::do_dates_date_part),
            DatesPkg::DateSecond => self.adapter_pf_fn1(ms, args, Self::do_dates_date_part),
            DatesPkg::DateYear => self.adapter_pf_fn1(ms, args, Self::do_dates_date_part),
            DatesPkg::IsLeapYear => extract_value_fn1(ms, args, Self::is_leapyear),
            DatesPkg::IsWeekday => self.adapter_pf_fn1(ms, args, Self::do_dates_date_part),
            DatesPkg::IsWeekend => self.adapter_pf_fn1(ms, args, Self::do_dates_date_part),
            DatesPkg::DateMinus => extract_value_fn2(ms, args, Self::do_dates_date_minus),
            DatesPkg::DatePlus => extract_value_fn2(ms, args, Self::do_dates_date_plus),
            DatesPkg::ToMillis => extract_value_fn1(ms, args, Self::do_dates_to_millis),
        }
    }

    fn get_name(&self) -> String {
        (match self {
            DatesPkg::DateDay => "day",
            DatesPkg::DateHour12 => "hour12",
            DatesPkg::DateHour24 => "hour24",
            DatesPkg::DateMinute => "minute",
            DatesPkg::DateMonth => "month",
            DatesPkg::DateSecond => "second",
            DatesPkg::DateYear => "year",
            DatesPkg::DateMinus => "minus",
            DatesPkg::IsLeapYear => "is_leapyear",
            DatesPkg::IsWeekday => "is_weekday",
            DatesPkg::IsWeekend => "is_weekend",
            DatesPkg::DatePlus => "plus",
            DatesPkg::ToMillis => "to_millis",
        }).into()
    }

    fn get_package_name(&self) -> String {
        "cal".into()
    }

    fn get_description(&self) -> String {
        (match self {
            DatesPkg::DateDay => "Returns the day of the month of a Date",
            DatesPkg::DateHour12 => "Returns the hour of the day of a Date",
            DatesPkg::DateHour24 => "Returns the hour (military time) of the day of a Date",
            DatesPkg::DateMinute => "Returns the minute of the hour of a Date",
            DatesPkg::DateMonth => "Returns the month of the year of a Date",
            DatesPkg::DateSecond => "Returns the seconds of the minute of a Date",
            DatesPkg::DateYear => "Returns the year of a Date",
            DatesPkg::IsLeapYear => "Returns true if the year of the date falls on a leap year",
            DatesPkg::IsWeekday => "Returns true if the date falls on a weekday",
            DatesPkg::IsWeekend => "Returns true if the date falls on a weekend",
            DatesPkg::DateMinus => "Subtracts a duration from a date",
            DatesPkg::DatePlus => "Adds a duration to a date",
            DatesPkg::ToMillis => "Returns the time in milliseconds of a date",
        }).into()
    }

    fn get_examples(&self) -> Vec<String> {
        match self {
            DatesPkg::DateDay => vec![
                strip_margin(r#"
                    |2025-07-06T21:59:02.425Z::day
                "#, '|')
            ],
            DatesPkg::DateHour12 => vec![
                strip_margin(r#"
                    |2025-07-06T21:59:02.425Z::hour12
                "#, '|')
            ],
            DatesPkg::DateHour24 => vec![
                strip_margin(r#"
                    |2025-07-06T21:59:02.425Z::hour24
                "#, '|')
            ],
            DatesPkg::DateMinute => vec![
                strip_margin(r#"
                    |2025-07-06T21:59:02.425Z::minute
                "#, '|')
            ],
            DatesPkg::DateMonth => vec![
                strip_margin(r#"
                    |2025-07-06T21:59:02.425Z::month
                "#, '|')
            ],
            DatesPkg::DateSecond => vec![
                strip_margin(r#"
                    |2025-07-06T21:59:02.425Z::second
                "#, '|')
            ],
            DatesPkg::DateYear => vec![
                strip_margin(r#"
                    |2025-07-06T21:59:02.425Z::year
                "#, '|')
            ],
            DatesPkg::IsLeapYear => vec![
                "2024-07-06T21:00:29.412Z::is_leapyear".to_string(),
                "2025-07-06T21:00:29.412Z::is_leapyear".to_string(),
                "2024::is_leapyear".to_string(),
                "2025::is_leapyear".to_string()
            ],
            DatesPkg::IsWeekday => vec!["2025-07-06T21:00:29.412Z::is_weekday".to_string()],
            DatesPkg::IsWeekend => vec!["2025-07-06T21:00:29.412Z::is_weekend".to_string()],
            DatesPkg::DateMinus => vec![
                strip_margin(r#"
                    |2025-07-06T21:59:02.425Z::minus(3::days)
                "#, '|')
            ],
            DatesPkg::DatePlus => vec![
                strip_margin(r#"
                    |2025-07-06T21:59:02.425Z::plus(30::days)
                "#, '|')
            ],
            DatesPkg::ToMillis => vec![
                strip_margin(r#"
                    |2025-07-06T21:59:02.425Z::to_millis
                "#, '|')
            ]
        }
    }

    fn get_parameter_types(&self) -> Vec<DataType> {
        match self {
            // DateTime
            DatesPkg::DateDay
            | DatesPkg::DateHour12
            | DatesPkg::DateHour24
            | DatesPkg::DateMinute
            | DatesPkg::DateMonth
            | DatesPkg::DateSecond
            | DatesPkg::DateYear
            | DatesPkg::IsLeapYear
            | DatesPkg::IsWeekday
            | DatesPkg::IsWeekend
            | DatesPkg::ToMillis => vec![DateTimeType],
            // (DateTime, Number)
            DatesPkg::DateMinus
            | DatesPkg::DatePlus => vec![DateTimeType, NumberType(I64Kind)],
        }
    }

    fn get_return_type(&self) -> DataType {
        match self {
            // Boolean
            DatesPkg::IsLeapYear
            | DatesPkg::IsWeekday
            | DatesPkg::IsWeekend => BooleanType,
            // DateTime
            DatesPkg::DateMinus
            | DatesPkg::DatePlus => DateTimeType,
            // Number
            DatesPkg::DateDay
            | DatesPkg::DateHour12
            | DatesPkg::DateHour24
            | DatesPkg::DateMinute
            | DatesPkg::DateMonth
            | DatesPkg::DateSecond
            | DatesPkg::DateYear
            | DatesPkg::ToMillis => NumberType(I64Kind),
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

#[async_trait]
impl Package for DurationsPkg {
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
            DurationsPkg::Minutes => "Converts a number into the equivalent number of minutes".into(),
            DurationsPkg::Seconds => "Converts a number into the equivalent number of seconds".into(),
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
            ]))?;
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

#[async_trait]
impl Package for IoPkg {
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
            IoPkg::FileCreate => vec![
                strip_margin(r#"
                    |io::create_file("quote.json", {
                    |   symbol: "TRX",
                    |   exchange: "NYSE",
                    |   last_sale: 45.32
                    |})
                "#, '|')
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
            // Boolean
            IoPkg::FileExists
            | IoPkg::StdErr
            | IoPkg::StdOut => BooleanType,
            // Number
            IoPkg::FileCreate => NumberType(I64Kind),
            // String
            IoPkg::FileReadText
            | IoPkg::StdIn => StringType,
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

#[async_trait]
impl Package for MathPkg {
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
            // f64
            MathPkg::Abs
            | MathPkg::Ceil
            | MathPkg::Floor
            | MathPkg::Max
            | MathPkg::Min
            | MathPkg::Pow
            | MathPkg::Round
            | MathPkg::Sqrt => NumberType(F64Kind),
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
    Home,
    Inspect,
    Printf,
    Println,
    Reset,
    Scope,
    UUID,
    Version,
}

impl OxidePkg {

    fn do_oxide_compile(
        ms: Machine,
        source_value: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let source = pull_string(source_value)?;
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
        let mut packages = PACKAGE_OPS.iter().collect::<Vec<_>>();
        packages.sort_by(|a, b| a.0.cmp(&b.0));
        for (_, modules) in packages {
            for pkg_op in modules {
                mrc.append_row(Row::new(
                    0,
                    vec![
                        // package name
                        StringValue(pkg_op.get_package_name()),
                        // name
                        StringValue(pkg_op.get_name()),
                        // description
                        StringValue(pkg_op.get_description()),
                        // signature
                        StringValue(pkg_op.to_code()),
                    ],
                ))?;
            }
        }
        Ok((ms, TableValue(ModelTable(mrc))))
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
                row_id, vec![StringValue(format!("{:?}", expr))],
            ))?;
        }
        Ok((ms, TableValue(ModelTable(mrc))))
    }

    fn do_oxide_printf(ms: Machine, args: Vec<TypedValue>) -> std::io::Result<(Machine, TypedValue)> {
        let (ms, result) = StringsPkg::do_str_sprintf(ms, args)?;
        print!("{}", result.unwrap_value());
        Ok((ms, Boolean(true)))
    }

    fn do_oxide_scope(
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match args.as_slice() {
            [] => {
                let mut mrc = ModelRowCollection::from_parameters(&OxidePkg::get_oxide_scope_parameters());
                let mut id = 0;
                // add aliases
                let mut aliases = ms.get_aliases().iter()
                    .map(|a| (a.get_name(), a.get_body().to_code()))
                    .collect::<Vec<_>>();
                aliases.sort_by_key(|k| k.0);
                for (name, code) in aliases.iter() {
                    mrc.append_row(Row::new(id, vec![
                        StringValue(name.to_string()),
                        StringValue("Alias".to_string()),
                        StringValue(code.to_string()),
                    ]))?;
                    id += 1;
                }
                // add observables
                let clean = |s: String| {
                    [("\n", ";"), (";;", ";"), ("{;", "{"), (";}", "}"), ("};", "}")]
                        .into_iter()
                        .fold(s.to_owned(), |acc, (pat, rep)| acc.replace(pat, rep))
                };
                let mut observables = ms.get_observations().iter()
                    .map(|ob| match ob {
                        Observations::VariableObservation { condition, code } =>
                            (clean(condition.to_code()), clean(code.to_code())),
                    })
                    .collect::<Vec<_>>();
                observables.sort_by_key(|k| k.0.clone());
                for (cond, code) in observables.iter() {
                    mrc.append_row(Row::new(id, vec![
                        StringValue(cond.to_string()),
                        StringValue("Observable".to_string()),
                        StringValue(code.to_string()),
                    ]))?;
                    id += 1;
                }
                // add variables
                let mut variables: Vec<(&String, &TypedValue)> = ms.get_variables().iter().collect::<Vec<_>>();
                variables.sort_by_key(|k| k.0);
                for (name, value) in variables.iter() {
                    mrc.append_row(Row::new(id, vec![
                        StringValue(name.to_string()),
                        StringValue(value.get_type().get_name()),
                        StringValue(value.to_code()),
                    ]))?;
                    id += 1;
                }
                Ok((ms, TableValue(ModelTable(mrc))))
            }
            args => throw(TypeMismatch(ArgumentsMismatched(0, args.len()))),
        }
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
            [] => generate_uuid(),
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
            PackageOps::Oxide(OxidePkg::Home),
            PackageOps::Oxide(OxidePkg::Inspect),
            PackageOps::Oxide(OxidePkg::Printf),
            PackageOps::Oxide(OxidePkg::Println),
            PackageOps::Oxide(OxidePkg::Reset),
            PackageOps::Oxide(OxidePkg::Scope),
            PackageOps::Oxide(OxidePkg::UUID),
            PackageOps::Oxide(OxidePkg::Version),
        ]
    }

    pub fn get_oxide_help_parameters() -> Vec<Parameter> {
        vec![
            Parameter::new("module", StringType),
            Parameter::new("name", StringType),
            Parameter::new("description", StringType),
            Parameter::new("signature", StringType),
        ]
    }

    pub fn get_oxide_inspect_parameters() -> Vec<Parameter> {
        vec![
            Parameter::new("model", StringType),
        ]
    }
    
    pub fn get_oxide_scope_parameters() -> Vec<Parameter> {
        vec![
            Parameter::new("identifier", StringType),
            Parameter::new("type", StringType),
            Parameter::new("value", StringType),
        ]
    }
}

#[async_trait]
impl Package for OxidePkg {
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
            OxidePkg::Home => extract_value_fn0(ms, args, |ms| Ok((ms, StringValue(Machine::oxide_home())))),
            OxidePkg::Inspect => extract_value_fn1(ms, args, Self::do_oxide_inspect),
            OxidePkg::Printf => Self::do_oxide_printf(ms, args),
            OxidePkg::Println => extract_value_fn1(ms, args, IoPkg::do_io_stdout),
            OxidePkg::Reset => extract_value_fn0(ms, args, |_ms| Ok((Machine::new_platform(), Boolean(true)))),
            OxidePkg::Scope => Self::do_oxide_scope(ms, args),
            OxidePkg::UUID => Self::do_oxide_uuid(ms, args),
            OxidePkg::Version => extract_value_fn0(ms, args, Self::do_oxide_version),
        }
    }

    fn get_name(&self) -> String {
        match self {
            OxidePkg::Compile => "compile".into(),
            OxidePkg::Debug => "debug".into(),
            OxidePkg::Eval => "eval".into(),
            OxidePkg::Help => "help".into(),
            OxidePkg::Home => "home".into(),
            OxidePkg::Inspect => "inspect".into(),
            OxidePkg::Printf => "printf".into(),
            OxidePkg::Println => "println".into(),
            OxidePkg::Reset => "reset".into(),
            OxidePkg::Scope => "scope".into(),
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
            OxidePkg::Home => "Returns the Oxide home directory path".into(),
            OxidePkg::Inspect => "Returns a table describing the structure of a model".into(),
            OxidePkg::Printf => "C-style \"printf\" function".into(),
            OxidePkg::Println => "Print line function".into(),
            OxidePkg::Reset => "Clears the scope of all user-defined objects".into(),
            OxidePkg::Scope => "Returns the scope of all user-defined objects".into(),
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
            OxidePkg::UUID => vec!["oxide::uuid()".into()],
            OxidePkg::Scope => vec!["oxide::scope()".into()],
            OxidePkg::Version => vec!["oxide::version()".into()],
        }
    }

    fn get_parameter_types(&self) -> Vec<DataType> {
        match self {
            // ()
            OxidePkg::Home
            | OxidePkg::Reset
            | OxidePkg::Help
            | OxidePkg::Scope
            | OxidePkg::Version
            | OxidePkg::UUID => vec![],
            // (String)
            OxidePkg::Compile
            | OxidePkg::Debug
            | OxidePkg::Eval
            | OxidePkg::Inspect
            | OxidePkg::Println => vec![StringType],
            // (String, ..)
            OxidePkg::Printf => vec![
                StringType, ArrayType(RuntimeResolvedType.into())
            ],
        }
    }

    fn get_return_type(&self) -> DataType {
        match self {
            // Boolean
            OxidePkg::Printf
            | OxidePkg::Println
            | OxidePkg::Reset => BooleanType,
            // Function
            OxidePkg::Compile 
            | OxidePkg::Debug => FunctionType(vec![], RuntimeResolvedType.into()),
            // Number::f64
            OxidePkg::Version => NumberType(F64Kind),
            OxidePkg::UUID => UUIDType,
            // String
            OxidePkg::Eval | OxidePkg::Home => StringType,
            // Table
            OxidePkg::Help => TableType(OxidePkg::get_oxide_help_parameters()),
            OxidePkg::Inspect => TableType(OxidePkg::get_oxide_inspect_parameters()),
            OxidePkg::Scope => TableType(OxidePkg::get_oxide_scope_parameters()),
        }
    }
}

/// OS package
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum OsPkg {
    Call,
    Clear,
    ConsoleInfo,
    CurrentDir,
    Env,
    Platform,
    UserId,
    UserName,
    UserRealName,
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

    fn do_os_console_info(ms: Machine) -> std::io::Result<(Machine, TypedValue)> {
        let (columns, rows) = terminal::size()
            .ok().map(|(cols, rows)| (Number(U16Value(cols)), Number(U16Value(rows))))
            .unwrap_or((Undefined, Undefined));
        let (width, height) = terminal::window_size()
            .ok().map(|ws| (Number(U16Value(ws.width)), Number(U16Value(ws.height))))
            .unwrap_or((Undefined, Undefined));
        let is_raw_mode_enabled =
            terminal::is_raw_mode_enabled().ok().map(Boolean).unwrap_or(Undefined);
        let supports_keyboard_enhancement =
            terminal::supports_keyboard_enhancement().ok().map(Boolean).unwrap_or(Undefined);
        Ok((ms, Structured(Hard(HardStructure::new(
            Self::get_os_console_info_parameters(), vec![
                columns, rows, width, height,
                is_raw_mode_enabled, supports_keyboard_enhancement
            ],
        )))))
    }

    fn do_os_current_dir(ms: Machine) -> std::io::Result<(Machine, TypedValue)> {
        let dir = env::current_dir()?;
        Ok((ms, StringValue(dir.display().to_string())))
    }

    fn do_os_env(ms: Machine) -> std::io::Result<(Machine, TypedValue)> {
        use std::env;
        let mut mrc = ModelRowCollection::from_parameters(&vec![
            Parameter::new("key", FixedSizeType(StringType.into(), 256)),
            Parameter::new("value", StringType),
        ]);
        for (key, value) in env::vars() {
                mrc.append_row(Row::new(0, vec![StringValue(key), StringValue(value)]))?;
        }
        Ok((ms, TableValue(ModelTable(mrc))))
    }

    pub fn get_contents() -> Vec<PackageOps> {
        vec![
            PackageOps::Os(OsPkg::Call),
            PackageOps::Os(OsPkg::Clear),
            PackageOps::Os(OsPkg::ConsoleInfo),
            PackageOps::Os(OsPkg::CurrentDir),
            PackageOps::Os(OsPkg::Env),
            PackageOps::Os(OsPkg::Platform),
            PackageOps::Os(OsPkg::UserId),
            PackageOps::Os(OsPkg::UserName),
            PackageOps::Os(OsPkg::UserRealName),
        ]
    }

    pub fn get_os_console_info_parameters() -> Vec<Parameter> {
        vec![
            Parameter::new("columns", NumberType(U16Kind)),
            Parameter::new("rows", NumberType(U16Kind)),
            Parameter::new("width", NumberType(U16Kind)),
            Parameter::new("height", NumberType(U16Kind)),
            Parameter::new("is_raw_mode_enabled", BooleanType),
            Parameter::new("supports_keyboard_enhancement", BooleanType),
        ]
    }

    pub fn get_os_env_parameters() -> Vec<Parameter> {
        vec![
            Parameter::new("key", FixedSizeType(StringType.into(), 256)),
            Parameter::new("value", StringType),
        ]
    }

    pub fn do_os_platform(ms: Machine) -> std::io::Result<(Machine, TypedValue)> {
        Ok((ms, StringValue(whoami::platform().to_string())))
    }

    pub fn do_os_real_name(ms: Machine) -> std::io::Result<(Machine, TypedValue)> {
        Ok((ms, StringValue(whoami::realname())))
    }

    pub fn do_os_user_id(ms: Machine) -> std::io::Result<(Machine, TypedValue)> {
        Ok((ms, Number(I64Value(users::get_current_uid().to_i64().unwrap_or(-1)))))
    }

    pub fn do_os_user_name(ms: Machine) -> std::io::Result<(Machine, TypedValue)> {
        Ok((ms, StringValue(whoami::username())))
    }

}

#[async_trait]
impl Package for OsPkg {
    fn evaluate(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match self {
            OsPkg::Call => Self::do_os_call(ms, args),
            OsPkg::Clear => extract_value_fn0(ms, args, Self::do_os_clear_screen),
            OsPkg::ConsoleInfo => extract_value_fn0(ms, args, Self::do_os_console_info),
            OsPkg::CurrentDir => extract_value_fn0(ms, args, Self::do_os_current_dir),
            OsPkg::Env => extract_value_fn0(ms, args, Self::do_os_env),
            OsPkg::Platform => extract_value_fn0(ms, args, Self::do_os_platform),
            OsPkg::UserRealName => extract_value_fn0(ms, args, Self::do_os_real_name),
            OsPkg::UserId => extract_value_fn0(ms, args, Self::do_os_user_id),
            OsPkg::UserName => extract_value_fn0(ms, args, Self::do_os_user_name),
        }
    }

    fn get_name(&self) -> String {
        match self {
            OsPkg::Call => "call".into(),
            OsPkg::Clear => "clear".into(),
            OsPkg::ConsoleInfo => "console_info".into(),
            OsPkg::CurrentDir => "current_dir".into(),
            OsPkg::Env => "env".into(),
            OsPkg::Platform => "platform".into(),
            OsPkg::UserId => "user_id".into(),
            OsPkg::UserName => "user_name".into(),
            OsPkg::UserRealName => "user_real_name".into(),
        }
    }

    fn get_package_name(&self) -> String {
        "os".into()
    }

    fn get_description(&self) -> String {
        match self {
            OsPkg::Call => "Invokes an operating system application".into(),
            OsPkg::Clear => "Clears the terminal/screen".into(),
            OsPkg::ConsoleInfo => "Returns OS-specific console information".into(),
            OsPkg::CurrentDir => "Returns the current directory".into(),
            OsPkg::Env => "Returns a table of the OS environment variables".into(),
            OsPkg::Platform => "Returns the OS/Platform identifier".into(),
            OsPkg::UserRealName => "Returns the real name of current user".into(),
            OsPkg::UserId => "Returns the user ID of current user".into(),
            OsPkg::UserName => "Returns the username of current user".into(),
        }
    }

    fn get_examples(&self) -> Vec<String> {
        match self {
            OsPkg::Call => vec![strip_margin(
                r#"
                    |stocks = tables::save(
                    |   "examples.os.call",
                    |    Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                    |)
                    |os::call("chmod", "777", oxide::home())
                "#,
                '|',
            )],
            OsPkg::Clear => vec!["os::clear()".into()],
            OsPkg::ConsoleInfo => vec!["os::console_info()".into()],
            OsPkg::CurrentDir => vec![
                strip_margin(r#"
                    |cur_dir = os::current_dir()
                    |prefix = if(cur_dir::ends_with("core"), "../..", ".")
                    |path_str = prefix + "/demoes/language/include_file.oxide"
                    |include path_str
                "#, '|')
            ],
            OsPkg::Env => vec!["os::env()".into()],
            OsPkg::Platform => vec!["os::platform()".into()],
            OsPkg::UserId => vec!["os::user_id()".into()],
            OsPkg::UserName => vec!["os::user_name()".into()],
            OsPkg::UserRealName => vec!["os::user_real_name()".into()],
        }
    }

    fn get_parameter_types(&self) -> Vec<DataType> {
        match self {
            // ()
            OsPkg::Clear
            | OsPkg::ConsoleInfo
            | OsPkg::CurrentDir
            | OsPkg::Env
            | OsPkg::Platform
            | OsPkg::UserRealName
            | OsPkg::UserId
            | OsPkg::UserName => vec![],
            // (String)
            OsPkg::Call => vec![StringType],
        }
    }

    fn get_return_type(&self) -> DataType {
        match self {
            // Boolean
            OsPkg::Clear => BooleanType,
            // i64
            OsPkg::UserId => NumberType(I64Kind),
            // String
            OsPkg::Call
            | OsPkg::CurrentDir
            | OsPkg::Platform
            | OsPkg::UserRealName
            | OsPkg::UserName => StringType,
            // Structure
            OsPkg::ConsoleInfo => StructureType(OsPkg::get_os_console_info_parameters()),
            // Table
            OsPkg::Env => TableType(OsPkg::get_os_env_parameters()),
        }
    }
}

/// Strings package
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum StringsPkg {
    EndsWith,
    Format,
    Position,
    Join,
    Left,
    Right,
    Split,
    Sprintf,
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

    /// str::position("Hello World", "World")
    fn do_str_position(
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

    fn do_str_sprintf(ms: Machine, args: Vec<TypedValue>) -> std::io::Result<(Machine, TypedValue)> {
        let format = pull_string(args.get(0).unwrap())?;
        let args = args[1..].to_vec();
        let result = StringPrinter::format(&format, args).map_err(|e| cnv_error!(e))?;
        Ok((ms, StringValue(result)))
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
            PackageOps::Strings(StringsPkg::Position),
            PackageOps::Strings(StringsPkg::Join),
            PackageOps::Strings(StringsPkg::Left),
            PackageOps::Strings(StringsPkg::Right),
            PackageOps::Strings(StringsPkg::Split),
            PackageOps::Strings(StringsPkg::Sprintf),
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

#[async_trait]
impl Package for StringsPkg {
    fn evaluate(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match self {
            StringsPkg::EndsWith => extract_value_fn2(ms, args, Self::do_str_ends_with),
            StringsPkg::Format => Self::do_str_format(ms, args),
            StringsPkg::Position => extract_value_fn2(ms, args, Self::do_str_position),
            StringsPkg::Join => extract_value_fn2(ms, args, Self::do_str_join),
            StringsPkg::Left => extract_value_fn2(ms, args, Self::do_str_left),
            StringsPkg::Right => extract_value_fn2(ms, args, Self::do_str_right),
            StringsPkg::Split => extract_value_fn2(ms, args, Self::do_str_split),
            StringsPkg::Sprintf => Self::do_str_sprintf(ms, args),
            StringsPkg::StartsWith => extract_value_fn2(ms, args, Self::do_str_start_with),
            StringsPkg::StripMargin => extract_value_fn2(ms, args, Self::do_str_strip_margin),
            StringsPkg::Substring => extract_value_fn3(ms, args, Self::do_str_substring),
            StringsPkg::SuperScript => extract_value_fn1(ms, args, Self::do_str_superscript),
            StringsPkg::ToLowercase => extract_value_fn1(ms, args, Self::do_str_to_lowercase),
            StringsPkg::ToString => extract_value_fn1(ms, args, |ms, v| Ok((ms, StringValue(v.unwrap_value())))),
            StringsPkg::ToUppercase => extract_value_fn1(ms, args, Self::do_str_to_uppercase),
            StringsPkg::Trim => extract_value_fn1(ms, args, Self::do_str_trim),
        }
    }

    fn get_name(&self) -> String {
        match self {
            StringsPkg::EndsWith => "ends_with".into(),
            StringsPkg::Format => "format".into(),
            StringsPkg::Position => "position".into(),
            StringsPkg::Join => "join".into(),
            StringsPkg::Left => "left".into(),
            StringsPkg::Right => "right".into(),
            StringsPkg::Split => "split".into(),
            StringsPkg::Sprintf => "sprintf".into(),
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
            StringsPkg::Position => "Returns the index of string `b` in string `a`",
            StringsPkg::Join => "Combines an array into a string",
            StringsPkg::Left => "Returns n-characters from left-to-right",
            StringsPkg::Right => "Returns n-characters from right-to-left",
            StringsPkg::Split => "Splits string `a` by delimiter string `b`",
            StringsPkg::Sprintf => "C-style \"sprintf\" function".into(),
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
            StringsPkg::Position => vec![
                r#"'The little brown fox'::position('brown')"#.into()
            ],
            StringsPkg::Join => vec![
                r#"['1', 5, 9, '13']::join(', ')"#.into()
            ],
            StringsPkg::Left => vec![
                r#"'Hello World'::left(5)"#.into()
            ],
            StringsPkg::Right => vec![
                "'Hello World'::right(5)".into()
            ],
            StringsPkg::Split => vec![
                r#"'Hello,there World'::split(' ,')"#.into(),
            ],
            StringsPkg::Sprintf => vec![
                r#""Hello %s"::sprintf("World")"#.into()
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
                "125.75::to(String)".into()
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
            // (String)
            StringsPkg::ToLowercase
            | StringsPkg::ToUppercase
            | StringsPkg::Trim => vec![StringType],
            // (String, String)
            StringsPkg::EndsWith
            | StringsPkg::Format
            | StringsPkg::Split
            | StringsPkg::StartsWith
            | StringsPkg::StripMargin => vec![StringType, StringType],
            // (String, ???)
            StringsPkg::Sprintf => vec![
                StringType, ArrayType(RuntimeResolvedType.into())
            ],
            // (String, i64)
            StringsPkg::Position | StringsPkg::Left | StringsPkg::Right => {
                vec![StringType, NumberType(I64Kind)]
            }
            // (Array, String)
            StringsPkg::Join => vec![
                ArrayType(RuntimeResolvedType.into()), StringType
            ],
            // (String, i64, i64)
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
            | StringsPkg::Sprintf
            | StringsPkg::StripMargin
            | StringsPkg::Substring
            | StringsPkg::SuperScript
            | StringsPkg::ToLowercase
            | StringsPkg::ToString
            | StringsPkg::ToUppercase
            | StringsPkg::Trim => StringType,
            // Number
            StringsPkg::Position => NumberType(I64Kind),
            // Array of String
            StringsPkg::Split => ArrayType(StringType.into()),
        }
    }
}

/// Tables package
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum TablesPkg {
    Compact,
    CreateEventSrc,
    CreateFn,
    CreateIndex,
    Describe,
    Drop,
    Exists,
    Journal,
    Latest,
    Load,
    Pop,
    PullCell,
    PullColumn,
    PullRow,
    Push,
    RecordSize,
    Reduce,
    Replay,
    Resize,
    Save,
    SaveAs,
    Scan,
    Shuffle,
    ToCSV,
    ToJSON,
    Truncate,
}

impl TablesPkg {

    fn do_tables_pull_cell(
        ms: Machine,
        df: Dataframe,
        row: &TypedValue,
        col: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let row_index = pull_number(row)?.to_usize();
        let column_name = pull_string(col)?;
        match df.get_columns().iter().position(|c| c.get_name() == column_name) {
            None => column_not_found(column_name.as_str(), df.get_columns()),
            Some(col_index) => {
                let result = match df.read_one(row_index)? {
                    None => Null,
                    Some(row) => {
                        let values = row.get_values();
                        if col_index < values.len() {  values[col_index].clone() }
                        else { Null }
                    }
                };
                Ok((ms, result))
            }
        }
    }

    fn do_tables_pull_column(
        ms: Machine,
        df: Dataframe,
        col: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let column_name = pull_string(col)?;
        match df.get_columns().iter().position(|c| c.get_name() == column_name) {
            None => column_not_found(column_name.as_str(), df.get_columns()),
            Some(index) => Ok((ms, ArrayValue(Array::from(df.column(index)?))))
        }
    }

    fn do_tables_pull_row(
        ms: Machine,
        df: Dataframe,
        row_offset: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let offset = row_offset.to_usize();
        let columns = df.get_columns();
        let (row, _) = df.read_row(offset)?;
        Ok((ms, TableValue(ModelTable(ModelRowCollection::from_columns_and_rows(
            columns, &vec![row],
        )))))
    }

    fn do_tables_push(
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let (seq_like, row_like) = match args.as_slice() {
            [a, b] =>(a.clone(), b.clone()),
            z => return throw(TypeMismatch(ArgumentsMismatched(2, z.len())))
        };
        match row_like {
            TupleValue(vv) =>
                Ok((ms, match seq_like.to_sequence()? {
                    TheDataframe(mut df) => Number(U64Value(df.push_row(Row::new(0, vv))?)),
                    TheArray(mut arr) => arr.push(TupleValue(vv)),
                    TheRange(..) => return throw(UnsupportedFeature("Range::push()".into())),
                    TheTuple(mut tpl) => {
                        tpl.push(TupleValue(vv));
                        TupleValue(tpl)
                    }
                })),
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

    fn do_tables_scan(
        ms: Machine,
        df: Dataframe
    ) -> std::io::Result<(Machine, TypedValue)> {
        let rows = df.examine_rows()?;
        let columns = rows
            .first()
            .map(|_row| df.get_columns().to_owned())
            .unwrap_or(Vec::new());
        let mrc = ModelRowCollection::from_columns_and_rows(&columns, &rows);
        Ok((ms, TableValue(ModelTable(mrc))))
    }

    /// transform the [RowCollection] into CSV or JSON
    fn do_tables_to_xxx(
        ms: Machine,
        df: Dataframe,
        format: DataFormats,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match format {
            DataFormats::CSV => Ok((ms, ArrayValue(Array::from(
                df.iter()
                    .map(|row| StringValue(row.to_csv()))
                    .collect::<Vec<_>>(),
            )))),
            DataFormats::JSON => Ok((ms, ArrayValue(Array::from(
                df.iter()
                    .map(|row| row.to_json_string(df.get_columns()))
                    .map(StringValue)
                    .collect::<Vec<_>>(),
            )))),
        }
    }

    /// Creates a journaled event-source
    /// #### Examples
    /// ```
    /// tables::create_event_src(
    ///   "examples.event_src.stocks",
    ///   Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
    /// )
    /// ```
    fn do_nsd_create_event_src(
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
    /// tables::create_fn(
    ///   "examples.table_fn.stocks",
    ///   (symbol: String(8), exchange: String(8), last_sale: f64) -> {
    ///       symbol: symbol,
    ///       exchange: exchange,
    ///       last_sale: last_sale * 2.0,
    ///       event_time: DateTime::new()
    ///   })
    /// ```
    fn do_nsd_create_fn(
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
                )?;
                Ok((ms, TableValue(TableFn(frc.into()))))
            }
            other => throw(TypeMismatch(FunctionExpected(other.to_code())))
        }
    }

    /// Creates an index on a host table
    /// #### Examples
    /// ```
    /// tables::create_index("packages.indices.stocks", ["symbol", "exchange"])
    /// ```
    fn do_nsd_create_index(
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
    /// tables::drop("packages.remove.stocks")
    /// ```
    fn do_nsd_drop(ms: Machine, path_v: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        let path = pull_string(path_v)?;
        let ns = Namespace::parse(path.as_str())?;
        let result1 = fs::remove_dir_all(ns.get_root_path());
        let result2 = fs::remove_dir_all(ns.with_events_name().get_root_path());
        Ok((ms, Boolean(result1.is_ok() || result2.is_ok())))
    }

    /// Indicates whether a dataframe exists within a namespace
    /// #### Examples
    /// ```
    /// tables::exists("packages.remove.stocks")
    /// ```
    fn do_nsd_exists(ms: Machine, path_v: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        let path = pull_string(path_v)?;
        let ns = Namespace::parse(path.as_str())?;
        Ok((ms, Boolean(Path::new(ns.get_table_file_path().as_str()).exists())))
    }

    /// Retrieves the journal for a dataframe (table function or event source)
    /// #### Examples
    /// ```
    /// tables::journal("packages.journal.stocks")
    /// ```
    fn do_nsd_journal(ms: Machine, value: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        match value.to_dataframe()? {
            EventSource(df) => Ok((ms, TableValue(df.get_journal()))),
            TableFn(df) => Ok((ms, TableValue(df.get_journal()))),
            _ => throw(TypeMismatch(UnsupportedType(
                TableType(vec![]),
                value.get_type(),
            ))),
        }
    }

    /// Loads a dataframe from a namespace
    /// #### Examples
    /// ```
    /// let stocks = tables::load("packages.loading.stocks")
    /// ```
    fn do_nsd_load(ms: Machine, path_v: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        let path = pull_string(path_v)?;
        let ns = Namespace::parse(path.as_str())?;
        let frc = FileRowCollection::open(&ns)?;
        Ok((ms, TableValue(DiskTable(frc))))
    }

    /// Rebuilds a dataframe by replaying its journal
    /// #### Examples
    /// ```
    /// let stocks = tables::load("packages.loading.stocks")
    /// tables::replay(stocks)
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
    /// tables::resize("packages.examples.stocks", 100)
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
            TableValue(df) => resize_table(ms, df, new_size),
            StringValue(..) =>
                match Self::do_nsd_load(ms, namespace_or_df)? {
                    (ms, TableValue(df)) => resize_table(ms, df, new_size),
                    (_, other) => throw(TypeMismatch(TableExpected(other.to_code())))
                }
            other => throw(TypeMismatch(TableExpected(other.to_code())))
        }
    }

    /// Truncate a dataframe; deleting all rows and reducing its size to zero.
    /// #### Examples
    /// ```
    /// tables::truncate("packages.examples.stocks")
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
    fn do_nsd_save(
        ms: Machine,
        path_v: &TypedValue,
        contents_v: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let path = pull_string(path_v)?;
        let df = contents_v.to_dataframe()?;
        let ns = Namespace::parse(path.as_str())?;
        let params = df.get_parameters();
        let mut frc = FileRowCollection::create_table(&ns, &params)?;
        frc.append_rows(df.get_rows())?;
        Ok((ms, TableValue(DiskTable(frc))))
    }

    pub fn get_contents() -> Vec<PackageOps> {
        vec![
            PackageOps::Tables(TablesPkg::Compact),
            PackageOps::Tables(TablesPkg::CreateEventSrc),
            PackageOps::Tables(TablesPkg::CreateFn),
            PackageOps::Tables(TablesPkg::CreateIndex),
            PackageOps::Tables(TablesPkg::Describe),
            PackageOps::Tables(TablesPkg::Drop),
            PackageOps::Tables(TablesPkg::Exists),
            PackageOps::Tables(TablesPkg::Journal),
            PackageOps::Tables(TablesPkg::Latest),
            PackageOps::Tables(TablesPkg::Load),
            PackageOps::Tables(TablesPkg::Pop),
            PackageOps::Tables(TablesPkg::PullCell),
            PackageOps::Tables(TablesPkg::PullColumn),
            PackageOps::Tables(TablesPkg::PullRow),
            PackageOps::Tables(TablesPkg::Push),
            PackageOps::Tables(TablesPkg::RecordSize),
            PackageOps::Tables(TablesPkg::Reduce),
            PackageOps::Tables(TablesPkg::Replay),
            PackageOps::Tables(TablesPkg::Resize),
            PackageOps::Tables(TablesPkg::Save),
            PackageOps::Tables(TablesPkg::SaveAs),
            PackageOps::Tables(TablesPkg::Scan),
            PackageOps::Tables(TablesPkg::Shuffle),
            PackageOps::Tables(TablesPkg::ToCSV),
            PackageOps::Tables(TablesPkg::ToJSON),
            PackageOps::Tables(TablesPkg::Truncate),
        ]
    }

    fn infer_pull_cell(
        &self,
        hints: TypeHints,
        args: &[Expression]
    ) -> (TypeHints, DataType) {
        match args {
            [table, _row, column] =>
                match TypeEngine::infer(hints, table) {
                    (hints, TableType(params)) =>
                        match find_string(column).and_then(|name| params.iter().find(|p| p.get_name() == name)) {
                            None => (hints, self.get_return_type()),
                            Some(param) => (hints, param.get_data_type())
                        }
                    (hints, _) => (hints, self.get_return_type())
                }
            _ => (hints, self.get_return_type())
        }
    }

    fn infer_pull_column(
        &self,
        hints: TypeHints,
        args: &[Expression]
    ) -> (TypeHints, DataType) {
        match args {
            [table, column] =>
                match TypeEngine::infer(hints, table) {
                    (hints, TableType(params)) =>
                        match find_string(column).and_then(|name| params.iter().find(|p| p.get_name() == name)) {
                            None => (hints, self.get_return_type()),
                            Some(param) => (hints, ArrayType(param.get_data_type().into()))
                        }
                    (hints, _) => (hints, self.get_return_type())
                }
            _ => (hints, self.get_return_type())
        }
    }

}

#[async_trait]
impl Package for TablesPkg {
    fn evaluate(&self, ms: Machine, args: Vec<TypedValue>) -> std::io::Result<(Machine, TypedValue)> {
        match self {
            TablesPkg::Compact => extract_table_fn1(ms, args, |ms, mut df| Ok((ms, Number(I64Value(df.compact()?))))),
            TablesPkg::Describe => extract_table_fn1(ms, args, |ms, df| Ok((ms, df.describe()?))),
            TablesPkg::Latest => extract_table_fn1(ms, args, |ms, df|
                df.find_last_active_row_id()
                    .map(|id_maybe| (ms, id_maybe.map(|id| Number(I64Value(id as i64))).unwrap_or(Undefined)))),
            TablesPkg::Pop => extract_table_fn1(ms, args, |ms, mut df| df.pop_row().to_dataframe().map(|df| (ms, TableValue(df)))),
            TablesPkg::PullCell => extract_table_fn3(ms, args, Self::do_tables_pull_cell),
            TablesPkg::PullColumn => extract_table_fn2(ms, args, Self::do_tables_pull_column),
            TablesPkg::PullRow => extract_table_fn2(ms, args, Self::do_tables_pull_row),
            TablesPkg::Push => Self::do_tables_push(ms, args),
            TablesPkg::RecordSize => extract_table_fn1(ms, args, |ms, df| Ok((ms, Number(U64Value(df.get_record_size() as u64))))),
            TablesPkg::Reduce => extract_value_fn3(ms, args, CollectionsPkg::do_arrays_reduce),
            TablesPkg::SaveAs => extract_value_fn2(ms, args, |ms, tbl, path| TablesPkg::do_nsd_save(ms, path, tbl)),
            TablesPkg::Scan => extract_table_fn1(ms, args, Self::do_tables_scan),
            TablesPkg::Shuffle => extract_table_fn1(ms, args, |ms, mut df| Ok((ms, Boolean(df.shuffle()?)))),
            TablesPkg::ToCSV => extract_table_fn1(ms, args, |ms, df| Self::do_tables_to_xxx(ms, df, DataFormats::CSV)),
            TablesPkg::ToJSON => extract_table_fn1(ms, args, |ms, df| Self::do_tables_to_xxx(ms, df, DataFormats::JSON)),

            TablesPkg::CreateEventSrc => extract_value_fn2(ms, args, Self::do_nsd_create_event_src),
            TablesPkg::CreateFn => extract_value_fn2(ms, args, Self::do_nsd_create_fn),
            TablesPkg::CreateIndex => extract_value_fn2(ms, args, Self::do_nsd_create_index),
            TablesPkg::Drop => extract_value_fn1(ms, args, Self::do_nsd_drop),
            TablesPkg::Exists => extract_value_fn1(ms, args, Self::do_nsd_exists),
            TablesPkg::Journal => extract_value_fn1(ms, args, Self::do_nsd_journal),
            TablesPkg::Load => extract_value_fn1(ms, args, Self::do_nsd_load),
            TablesPkg::Replay => extract_value_fn1(ms, args, Self::do_nsd_replay),
            TablesPkg::Resize => extract_value_fn2(ms, args, Self::do_nsd_resize),
            TablesPkg::Save => extract_value_fn2(ms, args, Self::do_nsd_save),
            TablesPkg::Truncate => extract_value_fn1(ms, args, Self::do_nsd_truncate),
        }
    }

    fn get_name(&self) -> String {
        (match self {
            TablesPkg::Compact => "compact",
            TablesPkg::CreateEventSrc => "create_event_src".into(),
            TablesPkg::CreateFn => "create_fn".into(),
            TablesPkg::CreateIndex => "create_index".into(),
            TablesPkg::Describe => "describe",
            TablesPkg::Drop => "drop".into(),
            TablesPkg::Exists => "exists".into(),
            TablesPkg::Journal => "journal".into(),
            TablesPkg::Latest => "latest",
            TablesPkg::Load => "load".into(),
            TablesPkg::Pop => "pop",
            TablesPkg::PullCell => "cell",
            TablesPkg::PullColumn => "column",
            TablesPkg::PullRow => "row",
            TablesPkg::Push => "push",
            TablesPkg::RecordSize => "record_size",
            TablesPkg::Reduce => "reduce",
            TablesPkg::Replay => "replay".into(),
            TablesPkg::Resize => "resize".into(),
            TablesPkg::Save => "save".into(),
            TablesPkg::SaveAs => "save_as",
            TablesPkg::Scan => "scan",
            TablesPkg::Shuffle => "shuffle",
            TablesPkg::ToCSV => "to_csv",
            TablesPkg::ToJSON => "to_json",
            TablesPkg::Truncate => "truncate".into(),
        }).into()
    }

    fn get_package_name(&self) -> String {
        "tables".into()
    }

    fn get_description(&self) -> String {
        (match self {
            TablesPkg::Compact => "Shrinks a table by removing deleted rows",
            TablesPkg::CreateEventSrc => "Creates a journaled event-source".into(),
            TablesPkg::CreateFn => "Creates a journaled table function".into(),
            TablesPkg::CreateIndex => "Creates a table index".into(),
            TablesPkg::Describe => "Describes a table or structure",
            TablesPkg::Drop => "Deletes a dataframe from a namespace".into(),
            TablesPkg::Exists => "Returns true if the source path exists".into(),
            TablesPkg::Journal => "Retrieves the journal for an event-source or table function".into(),
            TablesPkg::Latest => "Returns the row_id of last inserted record",
            TablesPkg::Load => "Loads a dataframe from a namespace".into(),
            TablesPkg::Pop => "Removes and returns a value or object from a Sequence",
            TablesPkg::PullCell => "Returns a cell from a table",
            TablesPkg::PullColumn => "Returns a column slice of a table",
            TablesPkg::PullRow => "Retrieves a raw structure from a table",
            TablesPkg::Push => "Appends a value or object to a Sequence",
            TablesPkg::RecordSize => "returns the table's record size",
            TablesPkg::Reduce => "Reduces a table to a single row",
            TablesPkg::Replay => "Reconstructs the state of a journaled table".into(),
            TablesPkg::Resize => "Changes the size of a dataframe".into(),
            TablesPkg::Save => "Creates a new dataframe".into(),
            TablesPkg::SaveAs => "Saves a table to a namespace; overwriting any existing rows",
            TablesPkg::Scan => "Returns existence metadata for a table",
            TablesPkg::Shuffle => "Shuffles a collection in random order",
            TablesPkg::ToCSV => "Converts a collection to CSV format",
            TablesPkg::ToJSON => "Converts a collection to JSON format",
            TablesPkg::Truncate => "Truncate a dataframe; deleting all rows and reducing its size to zero".into(),
        }).into()
    }

    fn get_examples(&self) -> Vec<String> {
        match self {
            TablesPkg::PullCell => vec![
                strip_margin(r#"
                    |stocks =
                    |   [{ symbol: "DMX", exchange: "NYSE", last_sale: 99.99 },
                    |    { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                    |    { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                    |    { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                    |    { symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
                    |    { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 },
                    |    { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }]
                    |       ::to(Table)
                    |stocks::cell(3, "last_sale")
                "#, '|')
            ],
            TablesPkg::PullColumn => vec![
                strip_margin(r#"
                    |stocks =
                    |   [{ symbol: "DMX", exchange: "NYSE", last_sale: 99.99 },
                    |    { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                    |    { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                    |    { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                    |    { symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
                    |    { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 },
                    |    { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }]
                    |       ::to(Table)
                    |stocks::column("last_sale")
                "#, '|')
            ],
            TablesPkg::Compact => vec![
                strip_margin(r#"
                    |stocks = tables::save(
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
            TablesPkg::Describe => vec![
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
            TablesPkg::PullRow => vec![
                strip_margin(r#"
                    |stocks = tables::save(
                    |   "examples.pull_row.stocks",
                    |   Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                    |)
                    |[{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                    | { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                    | { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
                    |stocks::row(2)
                "#, '|')
            ],
            TablesPkg::Latest => vec![
                strip_margin(r#"
                    |stocks = tables::save(
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
            TablesPkg::Pop => vec![
                strip_margin(r#"
                    |stocks = tables::save(
                    |   "examples.tools_pop.stocks",
                    |   Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                    |)
                    |[{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                    | { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                    | { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
                    |stocks::pop()
                "#, '|')
            ],
            TablesPkg::Push => vec![
                strip_margin(r#"
                    |stocks = tables::save(
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
            TablesPkg::RecordSize => vec![
                strip_margin(r#"
                    |stocks = Table(
                    |   symbol: String(8),
                    |   exchange: Enum(AMEX, NYSE, NASDAQ, OTCBB),
                    |   last_sale: f64
                    |)::new
                    |stocks::record_size()
                "#, '|')
            ],
            TablesPkg::Reduce => vec![
                strip_margin(r#"
                    |stocks =
                    |    |--------------------------------|
                    |    | symbol | exchange  | last_sale |
                    |    |--------------------------------|
                    |    | ROFL   | OTCBB     | 6.37      |
                    |    | IKR    | NYSE      | 11.75     |
                    |    | LOL    | NASDAQ    | 32.96     |
                    |    | SMH    | OTCBB     | 5.02      |
                    |    |--------------------------------|
                    |stocks::tail::reduce(stocks::head, (a, b) -> {
                    |   if((a.last_sale) > (b.last_sale), a, b)
                    |})
                "#, '|')
            ],
            TablesPkg::SaveAs => vec![
                strip_margin(r#"
                    |stocks =
                    |    |--------------------------------|
                    |    | symbol | exchange  | last_sale |
                    |    |--------------------------------|
                    |    | IKR    | NYSE      | 11.75     |
                    |    | LOL    | NASDAQ    | 32.96     |
                    |    | SMH    | OTCBB     | 5.02      |
                    |    | ROFL   | OTCBB     | 1.37      |
                    |    |--------------------------------|
                    |stocks::save_as("examples.save_as.stocks")
                "#, '|')
            ],
            TablesPkg::Scan => vec![
                strip_margin(r#"
                    |stocks = tables::save(
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
            TablesPkg::Shuffle => vec![
                strip_margin(r#"
                    |stocks = tables::save(
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
            TablesPkg::ToCSV => vec![
                strip_margin(r#"
                    |stocks = tables::save(
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
            TablesPkg::ToJSON => vec![
                strip_margin(r#"
                    |stocks = tables::save(
                    |   "examples.json.stocks",
                    |   Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                    |)
                    |[{ symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
                    | { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                    | { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                    | { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                    | { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }] ~> stocks
                    |stocks::to_json()
                "#, '|'),
            ],

            TablesPkg::CreateEventSrc => vec![
                strip_margin(r#"
                    |tables::create_event_src(
                    |   "examples.event_src.stocks",
                    |   Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                    |)
                "#, '|'),
            ],
            TablesPkg::CreateFn => vec![
                strip_margin(r#"
                    |tables::create_fn(
                    |   "examples.table_fn.stocks",
                    |   (symbol: String(8), exchange: String(8), last_sale: f64) -> {
                    |       symbol: symbol,
                    |       exchange: exchange,
                    |       last_sale: last_sale * 2.0,
                    |       event_time: DateTime::new()
                    |   })
                "#, '|'),
            ],
            TablesPkg::CreateIndex => vec![],
            TablesPkg::Drop => vec![
                strip_margin(r#"
                    |tables::save('packages.remove.stocks', Table(
                    |    symbol: String(8),
                    |    exchange: String(8),
                    |    last_sale: f64
                    |)::new)
                    |
                    |tables::drop('packages.remove.stocks')
                    |tables::exists('packages.remove.stocks')
                    |"#, '|')
            ],
            TablesPkg::Exists => vec![
                strip_margin(r#"
                    |tables::save('packages.exists.stocks', Table(
                    |   symbol: String(8),
                    |   exchange: String(8),
                    |   last_sale: f64
                    |)::new)
                    |tables::exists("packages.exists.stocks")
                "#, '|'),
                strip_margin(r#"
                    |tables::exists("packages.not_exists.stocks")
                "#, '|')
            ],
            TablesPkg::Journal => vec![
                strip_margin(r#"
                    |use tables
                    |tables::drop("examples.journal.stocks");
                    |stocks = tables::create_fn(
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
            TablesPkg::Load => vec![
                strip_margin(r#"
                    |let stocks =
                    |   tables::save('packages.save_load.stocks', Table(
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
                    |tables::load('packages.save_load.stocks')
                    |"#, '|')
            ],
            TablesPkg::Replay => vec![
                strip_margin(r#"
                    |use tables
                    |tables::drop("examples.replay.stocks");
                    |stocks = tables::create_fn(
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
            TablesPkg::Resize => vec![
                strip_margin(r#"
                    |use tables
                    |let stocks =
                    |   tables::save('packages.resize.stocks', Table(
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
            TablesPkg::Save => vec![
                strip_margin(r#"
                    |let stocks =
                    |   tables::save('packages.save.stocks', Table(
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
            TablesPkg::Truncate => vec![],
        }
    }

    fn get_parameter_types(&self) -> Vec<DataType> {
        match self {
            // Table
            TablesPkg::Compact
            | TablesPkg::Describe
            | TablesPkg::Latest
            | TablesPkg::Pop
            | TablesPkg::RecordSize
            | TablesPkg::Scan
            | TablesPkg::ToCSV
            | TablesPkg::ToJSON => vec![TableType(vec![])],
            // (Table, Boolean)
            TablesPkg::Shuffle => vec![TableType(vec![]), BooleanType],
            // (Table, Number)
            TablesPkg::PullRow => vec![TableType(vec![]), NumberType(AnyKind)],
            // (Table, Number, String)
            TablesPkg::PullCell => vec![TableType(vec![]), NumberType(I64Kind), StringType],
            // (Table, String)
            TablesPkg::PullColumn => vec![TableType(vec![]), StringType],
            TablesPkg::SaveAs => vec![TableType(vec![]), StringType],
            // (Table, Struct, Function)
            TablesPkg::Reduce => vec![
                TableType(vec![]), StructureType(vec![]), FunctionType(vec![
                    Parameter::new("a", RuntimeResolvedType),
                    Parameter::new("b", RuntimeResolvedType),
                ], RuntimeResolvedType.into())
            ],
            // (Table, Table)
            TablesPkg::Push => vec![TableType(vec![]), TableType(vec![])],

            // (String, Table)
            TablesPkg::CreateEventSrc
            | TablesPkg::Save => vec![
                StringType, TableType(vec![]),
            ],
            // (String, Function)
            TablesPkg::CreateFn => vec![
                StringType, FunctionType(vec![], StructureType(vec![]).into())
            ],
            // (String, Array)
            TablesPkg::CreateIndex => vec![
                StringType, ArrayType(RuntimeResolvedType.into())
            ],
            // (String)
            TablesPkg::Exists
            | TablesPkg::Load
            | TablesPkg::Drop
            | TablesPkg::Truncate => vec![StringType],
            // (Table)
            TablesPkg::Journal
            | TablesPkg::Replay => vec![TableType(vec![])],
            // (Table, i64)
            | TablesPkg::Resize => vec![
                StringType, NumberType(I64Kind)
            ],
        }
    }

    fn get_return_type(&self) -> DataType {
        match self {
            // Array
            TablesPkg::PullColumn => ArrayType(RuntimeResolvedType.into()),
            // Boolean
            TablesPkg::CreateIndex
            | TablesPkg::Drop
            | TablesPkg::Exists
            | TablesPkg::Replay
            | TablesPkg::Resize
            | TablesPkg::Push
            | TablesPkg::Shuffle
            | TablesPkg::Truncate => BooleanType,
            // Number
            TablesPkg::Latest | TablesPkg::RecordSize => NumberType(U64Kind),
            // Runtime
            TablesPkg::PullCell => RuntimeResolvedType,
            // Structure
            TablesPkg::Pop => StructureType(vec![]),
            // Table
            TablesPkg::Compact
            | TablesPkg::CreateEventSrc
            | TablesPkg::CreateFn
            | TablesPkg::Journal
            | TablesPkg::Load
            | TablesPkg::Save
            | TablesPkg::PullRow
            | TablesPkg::Reduce
            | TablesPkg::SaveAs
            | TablesPkg::Scan
            | TablesPkg::ToCSV
            | TablesPkg::ToJSON => TableType(vec![]),
            TablesPkg::Describe => TableType(CollectionsPkg::get_tools_describe_parameters()),
        }
    }

    fn infer_type(&self, hints: TypeHints, args: &[Expression]) -> (TypeHints, DataType) {
        match self {
            TablesPkg::PullCell => self.infer_pull_cell(hints, args),
            TablesPkg::PullColumn => self.infer_pull_column(hints, args),
            _ => (hints, self.get_return_type())
        }
    }
}

/// Utils package
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum UtilsPkg {
    Base36Decode,
    Base36Encode,
    Base62Decode,
    Base62Encode,
    Base64Decode,
    Base64Encode,
    Binary,
    GetType,
    Gunzip,
    Gzip,
    Hex,
    IsA,
    MD5,
    Octal,
    Random,
    Round,
    To
}

impl UtilsPkg {

    fn do_util_base36_decode(ms: Machine, a: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        let n = decode_base36(a.unwrap_value().as_str())?;
        Ok((ms, Number(U128Value(n))))
    }

    fn do_util_base36_encode(ms: Machine, a: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        Ok((ms, StringValue(encode_base36(a.to_u128())?)))
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

    fn do_util_binary(ms: Machine, a: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        Ok((ms, StringValue(format!("{:b}", a.to_u128()))))
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

    fn do_util_is_a(
        ms: Machine,
        value: &TypedValue,
        kind: &TypedValue
    ) -> std::io::Result<(Machine, TypedValue)> {
        let my_kind = pull_kind(kind)?;
        Ok((ms, Boolean(value.get_type().is_a(&my_kind))))
    }

    fn do_util_md5(ms: Machine, value: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        match md5::compute(value.to_bytes()) {
            md5::Digest(bytes) => Ok((ms, UUIDValue(u128::from_be_bytes(bytes)))),
        }
    }

    fn do_util_octal(ms: Machine, a: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        Ok((ms, StringValue(format!("{:o}", a.to_u128()))))
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

    fn do_util_to_hex(ms: Machine, value: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        Ok((ms, match value {
            Number(n) => StringValue(format!("{:x}", n.to_u128())),
            other => StringValue(format!("{}", StringValue(hex::encode(other.to_bytes()))))
        }))
    }

    fn do_util_to_xxx(
        ms: Machine,
        value: &TypedValue,
        to_type: &TypedValue
    ) -> std::io::Result<(Machine, TypedValue)> {
        match to_type {
            Kind(data_type) => Ok((ms, value.convert_to(data_type)?)),
            other => {
                let data_type = TypeEngine::decipher_model(&Literal(other.clone()))?;
                Ok((ms, value.convert_to(&data_type)?))
            },
        }
    }

    pub fn get_contents() -> Vec<PackageOps> {
        vec![
            PackageOps::Utils(UtilsPkg::Base36Decode),
            PackageOps::Utils(UtilsPkg::Base36Encode),
            PackageOps::Utils(UtilsPkg::Base62Decode),
            PackageOps::Utils(UtilsPkg::Base62Encode),
            PackageOps::Utils(UtilsPkg::Base64Decode),
            PackageOps::Utils(UtilsPkg::Base64Encode),
            PackageOps::Utils(UtilsPkg::Binary),
            PackageOps::Utils(UtilsPkg::GetType),
            PackageOps::Utils(UtilsPkg::Gunzip),
            PackageOps::Utils(UtilsPkg::Gzip),
            PackageOps::Utils(UtilsPkg::Hex),
            PackageOps::Utils(UtilsPkg::IsA),
            PackageOps::Utils(UtilsPkg::MD5),
            PackageOps::Utils(UtilsPkg::Octal),
            PackageOps::Utils(UtilsPkg::Random),
            PackageOps::Utils(UtilsPkg::Round),
            PackageOps::Utils(UtilsPkg::To),
        ]
    }

    fn infer_get_type(&self, hints: TypeHints, args: &[Expression]) -> (TypeHints, DataType) {
        match args {
            [type_arg] => TypeEngine::infer(hints, type_arg),
            _ => (hints, self.get_return_type())
        }
    }

    fn infer_to(&self, hints: TypeHints, args: &[Expression]) -> (TypeHints, DataType) {
        match args {
            [_, type_arg] => TypeEngine::infer(hints, type_arg),
            _ => (hints, self.get_return_type())
        }
    }
}

#[async_trait]
impl Package for UtilsPkg {
    fn evaluate(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match self {
            UtilsPkg::Base36Decode => extract_value_fn1(ms, args, Self::do_util_base36_decode),
            UtilsPkg::Base36Encode => extract_value_fn1(ms, args, Self::do_util_base36_encode),
            UtilsPkg::Base62Decode => extract_value_fn1(ms, args, Self::do_util_base62_decode),
            UtilsPkg::Base62Encode => extract_value_fn1(ms, args, Self::do_util_base62_encode),
            UtilsPkg::Base64Decode => extract_value_fn1(ms, args, Self::do_util_base64_decode),
            UtilsPkg::Base64Encode => extract_value_fn1(ms, args, Self::do_util_base64_encode),
            UtilsPkg::Binary => extract_value_fn1(ms, args, Self::do_util_binary),
            UtilsPkg::GetType => extract_value_fn1(ms, args, |ms, v| Ok((ms, Kind(v.get_type())))),
            UtilsPkg::Gunzip => extract_value_fn1(ms, args, Self::do_util_gunzip),
            UtilsPkg::Gzip => extract_value_fn1(ms, args, Self::do_util_gzip),
            UtilsPkg::Hex => extract_value_fn1(ms, args, Self::do_util_to_hex),
            UtilsPkg::IsA => extract_value_fn2(ms, args, Self::do_util_is_a),
            UtilsPkg::MD5 => extract_value_fn1(ms, args, Self::do_util_md5),
            UtilsPkg::Octal => extract_value_fn1(ms, args, Self::do_util_octal),
            UtilsPkg::Random => extract_value_fn0(ms, args, Self::do_util_random),
            UtilsPkg::Round => extract_value_fn2(ms, args, Self::do_util_round),
            UtilsPkg::To => extract_value_fn2(ms, args, Self::do_util_to_xxx),
        }
    }

    fn get_name(&self) -> String {
        (match self {
            UtilsPkg::Base36Decode => "base36_decode",
            UtilsPkg::Base36Encode => "base36_encode",
            UtilsPkg::Base62Decode => "base62_decode",
            UtilsPkg::Base62Encode => "base62_encode",
            UtilsPkg::Base64Decode => "base64_decode",
            UtilsPkg::Base64Encode => "base64_encode",
            UtilsPkg::Binary => "binary",
            UtilsPkg::GetType => "get_type",
            UtilsPkg::Gunzip => "gunzip",
            UtilsPkg::Gzip => "gzip",
            UtilsPkg::Hex => "hex",
            UtilsPkg::IsA => "is_a",
            UtilsPkg::MD5 => "md5",
            UtilsPkg::Octal => "octal",
            UtilsPkg::Random => "random",
            UtilsPkg::Round => "round",
            UtilsPkg::To => "to",
        }).into()
    }

    fn get_package_name(&self) -> String {
        "util".into()
    }

    fn get_description(&self) -> String {
        (match self {
            UtilsPkg::Base36Decode => "Converts a Base36 string to binary",
            UtilsPkg::Base36Encode => "Translates bytes into Base36",
            UtilsPkg::Base62Decode => "Converts a Base62 string to binary",
            UtilsPkg::Base62Encode => "Translates bytes into Base62",
            UtilsPkg::Base64Decode => "Converts a Base64 string to binary",
            UtilsPkg::Base64Encode => "Translates bytes into Base64",
            UtilsPkg::Binary => "Converts a numeric value to a binary string",
            UtilsPkg::GetType => "Returns the object's type",
            UtilsPkg::Gunzip => "Decompresses bytes via gzip",
            UtilsPkg::Gzip => "Compresses bytes via gzip",
            UtilsPkg::Hex => "Translates bytes into hexadecimal",
            UtilsPkg::IsA => "Indicates where a value is of a specific type",
            UtilsPkg::MD5 => "Creates a MD5 digest",
            UtilsPkg::Octal => "Converts a numeric value to an octal string",
            UtilsPkg::Random => "Returns a random numeric value",
            UtilsPkg::Round => "Rounds a Float to a specific number of decimal places",
            UtilsPkg::To => "Converts a value to the desired type",
        }).into()
    }

    fn get_examples(&self) -> Vec<String> {
        match self {
            UtilsPkg::Base36Decode => vec!["'C3PO'::base36_decode".into()],
            UtilsPkg::Base36Encode => vec!["564684::base36_encode".into()],
            UtilsPkg::Base62Decode => vec![
                "'Hello World'::base62_encode::base62_decode::to(String)".into()
            ],
            UtilsPkg::Base62Encode => vec!["'Hello World'::base62_encode".into()],
            UtilsPkg::Base64Decode => vec![
                "'Hello World'::base64_encode::base64_decode::to(String)".into()
            ],
            UtilsPkg::Base64Encode => vec!["'Hello World'::base64_encode".into()],
            UtilsPkg::Binary => vec!["util::binary(13)".into()],
            UtilsPkg::GetType => vec!["'Z'::get_type()".into()],
            UtilsPkg::Gunzip => vec!["util::gunzip(util::gzip('Hello World'))".into()],
            UtilsPkg::Gzip => vec!["util::gzip('Hello World')".into()],
            UtilsPkg::Hex => vec!["util::hex('Hello World')".into()],
            UtilsPkg::IsA => vec![
                "'A'::is_a(Char)".into(),
                "233::is_a(Char)".into(),
            ],
            UtilsPkg::MD5 => vec!["util::md5('Hello World')".into()],
            UtilsPkg::Octal => vec!["util::octal(345)".into()],
            UtilsPkg::Random => vec!["util::random()".into()],
            UtilsPkg::Round => vec!["util::round(1.42857, 2)".into()],
            UtilsPkg::To => vec![],
        }
    }

    fn get_parameter_types(&self) -> Vec<DataType> {
        match self {
            // ()
            UtilsPkg::Random => vec![],
            // (???)
            UtilsPkg::Gzip
            | UtilsPkg::Base36Decode
            | UtilsPkg::Base36Encode
            | UtilsPkg::Base62Decode
            | UtilsPkg::Base62Encode
            | UtilsPkg::Base64Decode
            | UtilsPkg::Base64Encode
            | UtilsPkg::GetType
            | UtilsPkg::Gunzip
            | UtilsPkg::Hex
            | UtilsPkg::IsA
            | UtilsPkg::MD5 => vec![RuntimeResolvedType],
            // (Number)
            UtilsPkg::Binary
            | UtilsPkg::Octal
            | UtilsPkg::Round => vec![NumberType(AnyKind)],
            // (???, ???)
            UtilsPkg::To => vec![RuntimeResolvedType, RuntimeResolvedType],
        }
    }

    fn get_return_type(&self) -> DataType {
        match self {
            // Boolean
            UtilsPkg::IsA => BooleanType,
            // Bytes
            UtilsPkg::Base36Decode
            | UtilsPkg::Base62Decode
            | UtilsPkg::Base64Decode
            | UtilsPkg::Gzip
            | UtilsPkg::Gunzip => ByteStringType,
            // String
            UtilsPkg::Base36Encode
            | UtilsPkg::Base62Encode
            | UtilsPkg::Base64Encode
            | UtilsPkg::Binary
            | UtilsPkg::Hex
            | UtilsPkg::Octal => StringType,
            // Number
            UtilsPkg::Random => NumberType(I64Kind),
            UtilsPkg::Round => NumberType(F64Kind),
            // Runtime
            UtilsPkg::GetType
            | UtilsPkg::To => RuntimeResolvedType,
            // UUID
            UtilsPkg::MD5 => UUIDType,
        }
    }

    fn infer_type(&self, hints: TypeHints, args: &[Expression]) -> (TypeHints, DataType) {
        match self {
            UtilsPkg::GetType => self.infer_get_type(hints, args),
            UtilsPkg::To => self.infer_to(hints, args),
            _ => (hints, self.get_return_type()),
        }
    }
}

/// WWW package
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum WwwPkg {
    HttpRandomPort,
    HttpStart,
    HttpStop,
    URLDecode,
    URLEncode,
    WsClose,
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
        let port = pull_number(&port)?.to_u16();
        match maybe_api_cfg {
            None => { server_engine::start_http_server(port, vec![]); }
            Some(cfg_value) => {
                let api_cfg = server_engine::convert_to_user_api_config(cfg_value)?;
                server_engine::start_http_server(port, api_cfg);
            }
        }
        Ok((ms, Boolean(true)))
    }

    fn do_http_stop(
        ms: Machine,
        port: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let port = pull_number(&port)?.to_u16();
        let success = webservers::stop_server_blocking(port)?;
        Ok((ms, Boolean(success)))
    }

    async fn do_http_serve_async(
        ms: Machine,
        port: TypedValue,
        maybe_api_cfg: Option<TypedValue>
    ) -> std::io::Result<(Machine, TypedValue)> {
        let port = pull_number(&port)?.to_u16();
        match maybe_api_cfg {
            None => { webservers::start_server(port).await?; }
            Some(cfg_value) => {
                let api_cfg = server_engine::convert_to_user_api_config(&cfg_value)?;
                webservers::start_server_with_api(port, api_cfg).await?;
            }
        }
        Ok((ms, Boolean(true)))
    }

    async fn do_http_stop_async(
        ms: Machine,
        port: TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let port = pull_number(&port)?.to_u16();
        let success = webservers::stop_server(port).await?;
        Ok((ms, Boolean(success)))
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

    fn do_ws_close(
        ms: Machine,
        conn: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let ws_conn = pull_uuid(conn)?;
        let message = futures::executor::block_on(websockets::close(ws_conn))?;
        Ok((ms, message))
    }

    async fn do_ws_close_async(
        ms: Machine,
        conn: TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let ws_conn = pull_uuid(&conn)?;
        let message = websockets::close(ws_conn).await?;
        Ok((ms, message))
    }

    fn do_ws_connect(
        ms: Machine,
        host: &TypedValue,
        port: &TypedValue,
        path: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let conn = futures::executor::block_on(websockets::connect_ws(
            pull_string(host)?.as_str(),
            pull_number(port)?.to_u16(),
            pull_string(path)?.as_str(),
        ))?;
        Ok((ms, conn))
    }

    async fn do_ws_connect_async(
        ms: Machine,
        host: TypedValue,
        port: TypedValue,
        path: TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let conn = websockets::connect_ws(
            pull_string(&host)?.as_str(),
            pull_number(&port)?.to_u16(),
            pull_string(&path)?.as_str(),
        ).await?;
        Ok((ms, conn))
    }

    fn do_ws_send_bytes(
        ms: Machine,
        conn: &TypedValue,
        message: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let ws_conn = pull_uuid(conn)?;
        let response = futures::executor::block_on(websockets::send_binary_command(
            ws_conn,
            message.to_bytes(),
        ))?;
        Ok((ms, response))
    }

    async fn do_ws_send_bytes_async(
        ms: Machine,
        conn: TypedValue,
        message: TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let ws_conn = pull_uuid(&conn)?;
        let response = websockets::send_binary_command(
            ws_conn,
            message.to_bytes(),
        ).await?;
        Ok((ms, response))
    }

    fn do_ws_send_text(
        ms: Machine,
        conn: &TypedValue,
        message: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let ws_conn = pull_uuid(conn)?;
        let response = futures::executor::block_on(websockets::send_text_command(
            ws_conn,
            message.unwrap_value().as_str(),
        ))?;
        Ok((ms, response))
    }

    async fn do_ws_send_text_async(
        ms: Machine,
        conn: TypedValue,
        message: TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let ws_conn = pull_uuid(&conn)?;
        let response = websockets::send_text_command(
            ws_conn,
            message.unwrap_value().as_str(),
        ).await?;
        Ok((ms, response))
    }

    pub fn get_contents() -> Vec<PackageOps> {
        vec![
            PackageOps::Www(WwwPkg::HttpRandomPort),
            PackageOps::Www(WwwPkg::HttpStart),
            PackageOps::Www(WwwPkg::HttpStop),
            PackageOps::Www(WwwPkg::URLDecode),
            PackageOps::Www(WwwPkg::URLEncode),
            PackageOps::Www(WwwPkg::WsClose),
            PackageOps::Www(WwwPkg::WsConnect),
            PackageOps::Www(WwwPkg::WsSendBytes),
            PackageOps::Www(WwwPkg::WsSendText),
        ]
    }
}

#[async_trait]
impl Package for WwwPkg {
    fn evaluate(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match self {
            WwwPkg::HttpRandomPort => extract_value_fn0(
                ms, args, |ms| Ok((ms, Number(I64Value(webservers::get_random_port()? as i64))))),
            WwwPkg::HttpStart => extract_value_fn1_or_2(ms, args, WwwPkg::do_http_serve),
            WwwPkg::HttpStop => extract_value_fn1(ms, args, WwwPkg::do_http_stop),
            WwwPkg::URLDecode => extract_value_fn1(ms, args, WwwPkg::do_www_url_decode),
            WwwPkg::URLEncode => extract_value_fn1(ms, args, WwwPkg::do_www_url_encode),
            WwwPkg::WsClose => extract_value_fn1(ms, args, WwwPkg::do_ws_close),
            WwwPkg::WsConnect => extract_value_fn3(ms, args, WwwPkg::do_ws_connect),
            WwwPkg::WsSendBytes => extract_value_fn2(ms, args, WwwPkg::do_ws_send_bytes),
            WwwPkg::WsSendText => extract_value_fn2(ms, args, WwwPkg::do_ws_send_text),
        }
    }

    async fn evaluate_async(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match self {
            WwwPkg::HttpStart => extract_value_fn1_or_2_async(ms, args, |ms, a, b| WwwPkg::do_http_serve_async(ms, a, b)).await,
            WwwPkg::HttpStop => extract_value_fn1_async(ms, args, |ms, a| WwwPkg::do_http_stop_async(ms, a)).await,
            WwwPkg::WsClose => extract_value_fn1_async(ms, args, |ms, a| WwwPkg::do_ws_close_async(ms, a)).await,
            WwwPkg::WsConnect => extract_value_fn3_async(ms, args, |ms, a, b, c| WwwPkg::do_ws_connect_async(ms, a, b, c)).await,
            WwwPkg::WsSendBytes => extract_value_fn2_async(ms, args, |ms, a, b| WwwPkg::do_ws_send_bytes_async(ms, a, b)).await,
            WwwPkg::WsSendText => extract_value_fn2_async(ms, args, |ms, a, b| WwwPkg::do_ws_send_text_async(ms, a, b)).await,
            _ => self.evaluate(ms, args),
        }
    }

    fn get_name(&self) -> String {
        (match self {
            WwwPkg::HttpRandomPort => "get_random_port",
            WwwPkg::HttpStart => "start",
            WwwPkg::HttpStop => "stop",
            WwwPkg::URLDecode => "url_decode",
            WwwPkg::URLEncode => "url_encode",
            WwwPkg::WsClose => "close",
            WwwPkg::WsConnect => "connect",
            WwwPkg::WsSendBytes => "send_bytes",
            WwwPkg::WsSendText => "send_text",
        }).into()
    }

    fn get_package_name(&self) -> String {
        (match self {
            WwwPkg::HttpRandomPort
            | WwwPkg::HttpStart
            | WwwPkg::HttpStop => "http",
            WwwPkg::URLDecode
            | WwwPkg::URLEncode => "www",
            WwwPkg::WsClose
            | WwwPkg::WsConnect
            | WwwPkg::WsSendBytes
            | WwwPkg::WsSendText => "ws",
        }).into()
    }

    fn get_description(&self) -> String {
        (match self {
            WwwPkg::HttpRandomPort => "Returns an used random port number",
            WwwPkg::HttpStart => "Starts a local HTTP service",
            WwwPkg::HttpStop => "Stops a local HTTP service",
            WwwPkg::URLDecode => "Decodes a URL-encoded string",
            WwwPkg::URLEncode => "Encodes a URL string",
            WwwPkg::WsClose => "Closes a web socket connection",
            WwwPkg::WsConnect => "Establishes a web socket connection",
            WwwPkg::WsSendBytes => "Transfers a binary message via a web socket connection",
            WwwPkg::WsSendText => "Transfers a text message via a web socket connection",
        }).into()
    }

    fn get_examples(&self) -> Vec<String> {
        match self {
            WwwPkg::HttpRandomPort => vec![
                "http::get_random_port()".into()
            ],
            WwwPkg::HttpStart => vec![
                strip_margin(r#"
                    |http::start(8745)
                    |stocks = tables::save(
                    |   "examples.http_serve.stocks",
                    |   Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                    |)
                    |[{ symbol: "XINU", exchange: "NYSE", last_sale: 8.11 },
                    | { symbol: "BOX", exchange: "NYSE", last_sale: 56.88 },
                    | { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 },
                    | { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                    | { symbol: "MIU", exchange: "OTCBB", last_sale: 2.24 }] ~> stocks
                    |GET http://localhost:8745/examples/http_serve/stocks/1/4
                "#, '|')
            ],
            WwwPkg::HttpStop => vec![],
            WwwPkg::URLDecode => vec![
                "'http%3A%2F%2Fshocktrade.com%3Fname%3Dthe%20hero%26t%3D9998'::url_decode()"
                    .into(),
            ],
            WwwPkg::URLEncode => vec![
                "'http://shocktrade.com?name=the hero&t=9998'::url_encode()".into()
            ],
            WwwPkg::WsClose => vec![],
            WwwPkg::WsConnect => vec![
                // strip_margin(r#"
                //     |ws::connect("0.0.0.0", 8287, "/api/ws")
                // "#, '|'),
            ],
            WwwPkg::WsSendBytes => vec![
                // strip_margin(r#"
                //     |let conn = ws::connect("0.0.0.0", 8288, "/api/ws")
                //     |conn::send_bytes(0B5eb63bbbe01eeed093cb22bb8f5acdc3)
                // "#, '|'),
            ],
            WwwPkg::WsSendText => vec![
                // strip_margin(r#"
                //     |let conn = ws::connect("0.0.0.0", 8289, "/api/ws")
                //     |conn::send_text("hello world")
                // "#, '|'),
            ],
        }
    }

    fn get_parameter_types(&self) -> Vec<DataType> {
        match self {
            // ()
            WwwPkg::HttpRandomPort => vec![],
            // (i64)
            WwwPkg::HttpStart
            | WwwPkg::HttpStop => vec![NumberType(I64Kind)],
            // (String)
            WwwPkg::URLDecode
            | WwwPkg::URLEncode => vec![StringType],
            // (String, i64, String)
            WwwPkg::WsConnect => vec![StringType, NumberType(I64Kind), StringType],
            // (UUID)
            WwwPkg::WsClose => vec![UUIDType],
            // (UUID, Bytes)
            WwwPkg::WsSendBytes => vec![UUIDType, ByteStringType],
            // (UUID, String)
            WwwPkg::WsSendText => vec![UUIDType, StringType],
        }
    }

    fn get_return_type(&self) -> DataType {
        match self {
            WwwPkg::HttpRandomPort => NumberType(I64Kind),
            WwwPkg::HttpStart
            | WwwPkg::HttpStop => BooleanType,
            WwwPkg::URLDecode
            | WwwPkg::URLEncode => StringType,
            WwwPkg::WsClose => StringType,
            WwwPkg::WsConnect => UUIDType,
            WwwPkg::WsSendBytes => StringType,
            WwwPkg::WsSendText => StringType,
        }
    }
}

/// Unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::packages::PackageOps::*;

    #[test]
    fn test_lookup_by_name() {
        let op = PackageOps::lookup_by_name("str", "sprintf");
        assert_eq!(op, Some(Strings(StringsPkg::Sprintf)));

        let op = op.unwrap();
        assert_eq!(op.get_type(), PackageFunctionType(op));
    }

    #[test]
    fn test_package_encode_decode() {
        for expected in PackageOps::get_contents() {
            let bytes = expected.encode().unwrap();
            assert_eq!(bytes.len(), 8);

            let actual = PackageOps::decode(bytes).unwrap();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn test_package_fn_to_code_generation() {
        // NOTE: this test generates the test cases for `test_package_fn_to_code`
        let mut last_module: String = String::new();
        for pf in PackageOps::get_contents() {
            if last_module != pf.get_package_name() {
                last_module = pf.get_package_name();
                println!("// {}", last_module)
            }
            let opcode = match &pf {
                Agg(op) => format!("Agg(AggPkg::{:?})", op),
                Blobs(op) => format!("Blobs(BlobsPkg::{:?})", op),
                Chars(op) => format!("Chars(CharsPkg::{:?})", op),
                Collections(op) => format!("Collections(CollectionsPkg::{:?})", op),
                Dates(op) => format!("Dates(DatesPkg::{:?})", op),
                Durations(op) => format!("Durations(DurationsPkg::{:?})", op),
                Io(op) => format!("Io(IoPkg::{:?})", op),
                Math(op) => format!("Math(MathPkg::{:?})", op),
                Oxide(op) => format!("Oxide(OxidePkg::{:?})", op),
                Os(op) => format!("Os(OsPkg::{:?})", op),
                Strings(op) => format!("Strings(StringsPkg::{:?})", op),
                Tables(op) => format!("Tables(TablesPkg::{:?})", op),
                Utils(op) => format!("Utils(UtilsPkg::{:?})", op),
                Www(op) => format!("Www(WwwPkg::{:?})", op),
            };
            println!("assert_eq!({}.to_code(), \"{}\");", opcode, pf.to_code())
        }
    }

    #[test]
    fn test_package_fn_to_code() {
        // agg
        assert_eq!(Agg(AggPkg::Avg).to_code(), "agg::avg(a): f64");
        assert_eq!(Agg(AggPkg::Count).to_code(), "agg::count(a): i64");
        assert_eq!(Agg(AggPkg::Max).to_code(), "agg::max(a): f64");
        assert_eq!(Agg(AggPkg::Min).to_code(), "agg::min(a): f64");
        assert_eq!(Agg(AggPkg::Sum).to_code(), "agg::sum(a): f64");
        // blobs
        assert_eq!(Blobs(BlobsPkg::Append).to_code(), "blobs::append(a: BLOBStore, b): UUID");
        assert_eq!(Blobs(BlobsPkg::Close).to_code(), "blobs::close(a: BLOBStore): Boolean");
        assert_eq!(Blobs(BlobsPkg::Create).to_code(), "blobs::create(s: String): BLOBStore");
        assert_eq!(Blobs(BlobsPkg::Entries).to_code(), "blobs::entries(a: BLOBStore): Table(blob_id: UUID, offset: u64, allocated: u64, used: u64)");
        assert_eq!(Blobs(BlobsPkg::Len).to_code(), "blobs::len(a: BLOBStore): i64");
        assert_eq!(Blobs(BlobsPkg::Load).to_code(), "blobs::load(s: String): BLOBStore");
        assert_eq!(Blobs(BlobsPkg::Read).to_code(), "blobs::read(a: BLOBStore, b: UUID)");
        assert_eq!(Blobs(BlobsPkg::Truncate).to_code(), "blobs::truncate(a: BLOBStore): Boolean");
        assert_eq!(Blobs(BlobsPkg::Update).to_code(), "blobs::update(a: BLOBStore, b: UUID, c): UUID");
        // collections
        assert_eq!(Collections(CollectionsPkg::Contains).to_code(), "collections::contains(a: Array(), b): Boolean");
        assert_eq!(Collections(CollectionsPkg::Filter).to_code(), "collections::filter(a: Array(), b: fn(item): Boolean): Array()");
        assert_eq!(Collections(CollectionsPkg::Head).to_code(), "collections::head(a)");
        assert_eq!(Collections(CollectionsPkg::IsEmpty).to_code(), "collections::is_empty(a): Boolean");
        assert_eq!(Collections(CollectionsPkg::Keys).to_code(), "collections::keys(a: Struct): Array(String)");
        assert_eq!(Collections(CollectionsPkg::Len).to_code(), "collections::len(a): i64");
        assert_eq!(Collections(CollectionsPkg::Map).to_code(), "collections::map(a: Array(), b: fn(item)): Array()");
        assert_eq!(Collections(CollectionsPkg::Pop).to_code(), "collections::pop(a: Array()): Array()");
        assert_eq!(Collections(CollectionsPkg::Push).to_code(), "collections::push(a: Array(), b): Array()");
        assert_eq!(Collections(CollectionsPkg::Reduce).to_code(), "collections::reduce(a: Array(), b, c: fn(a, b))");
        assert_eq!(Collections(CollectionsPkg::Reverse).to_code(), "collections::reverse(a: Array()): Array()");
        assert_eq!(Collections(CollectionsPkg::Tail).to_code(), "collections::tail(a): Array()");
        // cal
        assert_eq!(Dates(DatesPkg::DateDay).to_code(), "cal::day(a: DateTime): i64");
        assert_eq!(Dates(DatesPkg::DateHour12).to_code(), "cal::hour12(a: DateTime): i64");
        assert_eq!(Dates(DatesPkg::DateHour24).to_code(), "cal::hour24(a: DateTime): i64");
        assert_eq!(Dates(DatesPkg::DateMinute).to_code(), "cal::minute(a: DateTime): i64");
        assert_eq!(Dates(DatesPkg::DateMonth).to_code(), "cal::month(a: DateTime): i64");
        assert_eq!(Dates(DatesPkg::DateSecond).to_code(), "cal::second(a: DateTime): i64");
        assert_eq!(Dates(DatesPkg::DateYear).to_code(), "cal::year(a: DateTime): i64");
        assert_eq!(Dates(DatesPkg::IsLeapYear).to_code(), "cal::is_leapyear(a: DateTime): Boolean");
        assert_eq!(Dates(DatesPkg::IsWeekday).to_code(), "cal::is_weekday(a: DateTime): Boolean");
        assert_eq!(Dates(DatesPkg::IsWeekend).to_code(), "cal::is_weekend(a: DateTime): Boolean");
        assert_eq!(Dates(DatesPkg::DateMinus).to_code(), "cal::minus(a: DateTime, b: i64): DateTime");
        assert_eq!(Dates(DatesPkg::DatePlus).to_code(), "cal::plus(a: DateTime, b: i64): DateTime");
        assert_eq!(Dates(DatesPkg::ToMillis).to_code(), "cal::to_millis(a: DateTime): i64");
        // durations
        assert_eq!(Durations(DurationsPkg::Days).to_code(), "durations::days(n: i64): i64");
        assert_eq!(Durations(DurationsPkg::Hours).to_code(), "durations::hours(n: i64): i64");
        assert_eq!(Durations(DurationsPkg::Millis).to_code(), "durations::millis(n: i64): i64");
        assert_eq!(Durations(DurationsPkg::Minutes).to_code(), "durations::minutes(n: i64): i64");
        assert_eq!(Durations(DurationsPkg::Seconds).to_code(), "durations::seconds(n: i64): i64");
        // io
        assert_eq!(Io(IoPkg::FileCreate).to_code(), "io::create_file(a: String, b: String): i64");
        assert_eq!(Io(IoPkg::FileExists).to_code(), "io::exists(s: String): Boolean");
        assert_eq!(Io(IoPkg::FileReadText).to_code(), "io::read_text_file(s: String): String");
        assert_eq!(Io(IoPkg::StdErr).to_code(), "io::stderr(s: String): Boolean");
        assert_eq!(Io(IoPkg::StdIn).to_code(), "io::stdin(): String");
        assert_eq!(Io(IoPkg::StdOut).to_code(), "io::stdout(s: String): Boolean");
        // math
        assert_eq!(Math(MathPkg::Abs).to_code(), "math::abs(n: f64): f64");
        assert_eq!(Math(MathPkg::Ceil).to_code(), "math::ceil(n: f64): f64");
        assert_eq!(Math(MathPkg::Floor).to_code(), "math::floor(n: f64): f64");
        assert_eq!(Math(MathPkg::Max).to_code(), "math::max(a: f64, b: f64): f64");
        assert_eq!(Math(MathPkg::Min).to_code(), "math::min(a: f64, b: f64): f64");
        assert_eq!(Math(MathPkg::Pow).to_code(), "math::pow(a: f64, b: f64): f64");
        assert_eq!(Math(MathPkg::Round).to_code(), "math::round(n: f64): f64");
        assert_eq!(Math(MathPkg::Sqrt).to_code(), "math::sqrt(n: f64): f64");
        // os
        assert_eq!(Os(OsPkg::Call).to_code(), "os::call(s: String): String");
        assert_eq!(Os(OsPkg::Clear).to_code(), "os::clear(): Boolean");
        assert_eq!(Os(OsPkg::CurrentDir).to_code(), "os::current_dir(): String");
        assert_eq!(Os(OsPkg::Env).to_code(), "os::env(): Table(key: String(256), value: String)");
        // oxide
        assert_eq!(Oxide(OxidePkg::Compile).to_code(), "oxide::compile(s: String): fn()");
        assert_eq!(Oxide(OxidePkg::Debug).to_code(), "oxide::debug(s: String): fn()");
        assert_eq!(Oxide(OxidePkg::Eval).to_code(), "oxide::eval(s: String): String");
        assert_eq!(Oxide(OxidePkg::Help).to_code(), "oxide::help(): Table(name: String, module: String, signature: String, description: String, returns: String)");
        assert_eq!(Oxide(OxidePkg::Home).to_code(), "oxide::home(): String");
        assert_eq!(Oxide(OxidePkg::Inspect).to_code(), "oxide::inspect(s: String): Table(code: String, model: String)");
        assert_eq!(Oxide(OxidePkg::Printf).to_code(), "oxide::printf(a: String, b: Array()): Boolean");
        assert_eq!(Oxide(OxidePkg::Println).to_code(), "oxide::println(s: String): Boolean");
        assert_eq!(Oxide(OxidePkg::Reset).to_code(), "oxide::reset(): Boolean");
        assert_eq!(Oxide(OxidePkg::Scope).to_code(), "oxide::scope(): Table(identifier: String, type: String, value: String)");
        assert_eq!(Oxide(OxidePkg::UUID).to_code(), "oxide::uuid(): UUID");
        assert_eq!(Oxide(OxidePkg::Version).to_code(), "oxide::version(): f64");
        // str
        assert_eq!(Strings(StringsPkg::EndsWith).to_code(), "str::ends_with(a: String, b: String): Boolean");
        assert_eq!(Strings(StringsPkg::Format).to_code(), "str::format(a: String, b: String): String");
        assert_eq!(Strings(StringsPkg::Position).to_code(), "str::position(s: String, n: i64): i64");
        assert_eq!(Strings(StringsPkg::Join).to_code(), "str::join(a: Array(), b: String): String");
        assert_eq!(Strings(StringsPkg::Left).to_code(), "str::left(s: String, n: i64): String");
        assert_eq!(Strings(StringsPkg::Right).to_code(), "str::right(s: String, n: i64): String");
        assert_eq!(Strings(StringsPkg::Split).to_code(), "str::split(a: String, b: String): Array(String)");
        assert_eq!(Strings(StringsPkg::Sprintf).to_code(), "str::sprintf(a: String, b: Array()): String");
        assert_eq!(Strings(StringsPkg::StartsWith).to_code(), "str::starts_with(a: String, b: String): Boolean");
        assert_eq!(Strings(StringsPkg::StripMargin).to_code(), "str::strip_margin(a: String, b: String): String");
        assert_eq!(Strings(StringsPkg::Substring).to_code(), "str::substring(s: String, m: i64, n: i64): String");
        assert_eq!(Strings(StringsPkg::SuperScript).to_code(), "str::superscript(n: i64): String");
        assert_eq!(Strings(StringsPkg::ToLowercase).to_code(), "str::to_lowercase(s: String): String");
        assert_eq!(Strings(StringsPkg::ToString).to_code(), "str::to_string(a): String");
        assert_eq!(Strings(StringsPkg::ToUppercase).to_code(), "str::to_uppercase(s: String): String");
        assert_eq!(Strings(StringsPkg::Trim).to_code(), "str::trim(s: String): String");
        // tables
        assert_eq!(Tables(TablesPkg::Compact).to_code(), "tables::compact(t: Table): Table");
        assert_eq!(Tables(TablesPkg::CreateEventSrc).to_code(), "tables::create_event_src(a: String, b: Table): Table");
        assert_eq!(Tables(TablesPkg::CreateFn).to_code(), "tables::create_fn(a: String, b: fn(): Struct): Table");
        assert_eq!(Tables(TablesPkg::CreateIndex).to_code(), "tables::create_index(a: String, b: Array()): Boolean");
        assert_eq!(Tables(TablesPkg::Describe).to_code(), "tables::describe(t: Table): Table(name: String, type: String, default_value: String, is_nullable: Boolean)");
        assert_eq!(Tables(TablesPkg::Drop).to_code(), "tables::drop(s: String): Boolean");
        assert_eq!(Tables(TablesPkg::Exists).to_code(), "tables::exists(s: String): Boolean");
        assert_eq!(Tables(TablesPkg::Journal).to_code(), "tables::journal(t: Table): Table");
        assert_eq!(Tables(TablesPkg::Latest).to_code(), "tables::latest(t: Table): u64");
        assert_eq!(Tables(TablesPkg::Load).to_code(), "tables::load(s: String): Table");
        assert_eq!(Tables(TablesPkg::Pop).to_code(), "tables::pop(t: Table): Struct");
        assert_eq!(Tables(TablesPkg::PullCell).to_code(), "tables::cell(a: Table, b: i64, c: String)");
        assert_eq!(Tables(TablesPkg::PullColumn).to_code(), "tables::column(a: Table, b: String): Array()");
        assert_eq!(Tables(TablesPkg::PullRow).to_code(), "tables::row(t: Table, n: Number): Table");
        assert_eq!(Tables(TablesPkg::Push).to_code(), "tables::push(a: Table, b: Table): Boolean");
        assert_eq!(Tables(TablesPkg::RecordSize).to_code(), "tables::record_size(t: Table): u64");
        assert_eq!(Tables(TablesPkg::Reduce).to_code(), "tables::reduce(a: Table, b: Struct, c: fn(a, b)): Table");
        assert_eq!(Tables(TablesPkg::Replay).to_code(), "tables::replay(t: Table): Boolean");
        assert_eq!(Tables(TablesPkg::Resize).to_code(), "tables::resize(s: String, n: i64): Boolean");
        assert_eq!(Tables(TablesPkg::Save).to_code(), "tables::save(a: String, b: Table): Table");
        assert_eq!(Tables(TablesPkg::SaveAs).to_code(), "tables::save_as(a: Table, b: String): Table");
        assert_eq!(Tables(TablesPkg::Scan).to_code(), "tables::scan(t: Table): Table");
        assert_eq!(Tables(TablesPkg::Shuffle).to_code(), "tables::shuffle(a: Table, b: Boolean): Boolean");
        assert_eq!(Tables(TablesPkg::ToCSV).to_code(), "tables::to_csv(t: Table): Table");
        assert_eq!(Tables(TablesPkg::ToJSON).to_code(), "tables::to_json(t: Table): Table");
        assert_eq!(Tables(TablesPkg::Truncate).to_code(), "tables::truncate(s: String): Boolean");
        // util
        assert_eq!(Utils(UtilsPkg::Base36Decode).to_code(), "util::base36_decode(a): Bytes");
        assert_eq!(Utils(UtilsPkg::Base36Encode).to_code(), "util::base36_encode(a): String");
        assert_eq!(Utils(UtilsPkg::Base62Decode).to_code(), "util::base62_decode(a): Bytes");
        assert_eq!(Utils(UtilsPkg::Base62Encode).to_code(), "util::base62_encode(a): String");
        assert_eq!(Utils(UtilsPkg::Base64Decode).to_code(), "util::base64_decode(a): Bytes");
        assert_eq!(Utils(UtilsPkg::Base64Encode).to_code(), "util::base64_encode(a): String");
        assert_eq!(Utils(UtilsPkg::Binary).to_code(), "util::binary(n: Number): String");
        assert_eq!(Utils(UtilsPkg::GetType).to_code(), "util::get_type(a)");
        assert_eq!(Utils(UtilsPkg::Gunzip).to_code(), "util::gunzip(a): Bytes");
        assert_eq!(Utils(UtilsPkg::Gzip).to_code(), "util::gzip(a): Bytes");
        assert_eq!(Utils(UtilsPkg::Hex).to_code(), "util::hex(a): String");
        assert_eq!(Utils(UtilsPkg::IsA).to_code(), "util::is_a(a): Boolean");
        assert_eq!(Utils(UtilsPkg::MD5).to_code(), "util::md5(a): UUID");
        assert_eq!(Utils(UtilsPkg::Octal).to_code(), "util::octal(n: Number): String");
        assert_eq!(Utils(UtilsPkg::Random).to_code(), "util::random(): i64");
        assert_eq!(Utils(UtilsPkg::Round).to_code(), "util::round(n: Number): f64");
        assert_eq!(Utils(UtilsPkg::To).to_code(), "util::to(a, b)");
        // http
        assert_eq!(Www(WwwPkg::HttpRandomPort).to_code(), "http::get_random_port(): i64");
        assert_eq!(Www(WwwPkg::HttpStart).to_code(), "http::start(n: i64): Boolean");
        assert_eq!(Www(WwwPkg::HttpStop).to_code(), "http::stop(n: i64): Boolean");
        // www
        assert_eq!(Www(WwwPkg::URLDecode).to_code(), "www::url_decode(s: String): String");
        assert_eq!(Www(WwwPkg::URLEncode).to_code(), "www::url_encode(s: String): String");
        // ws
        assert_eq!(Www(WwwPkg::WsClose).to_code(), "ws::close(a: UUID): String");
        assert_eq!(Www(WwwPkg::WsConnect).to_code(), "ws::connect(a: String, b: i64, c: String): UUID");
        assert_eq!(Www(WwwPkg::WsSendBytes).to_code(), "ws::send_bytes(a: UUID, b: Bytes): String");
        assert_eq!(Www(WwwPkg::WsSendText).to_code(), "ws::send_text(a: UUID, b: String): String");
    }

    /// Package "array" tests
    #[cfg(test)]
    mod agg_tests {
        use crate::test_util::verify_exact_table_async;

        #[actix::test]
        async fn test_agg_max_min_sum() {
            verify_exact_table_async(r#"
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
                    "|---------------------------------------|"]).await;
        }
    }

    /// Package "blobs" tests
    #[cfg(test)]
    mod blobs_tests {
        use crate::test_util::*;

        #[test]
        fn test_blob_store_append() {
            verify_exact_code_and_inferred_type(r#"
                let bs = blobs::create("packages.blobs.append_blocking")
                let id = bs::append("Hello World")
                let result = bs::read(id)
                blobs::close(id)
                result
            "#, "\"Hello World\"", "");
        }

        #[test]
        fn test_blob_store_len() {
            verify_exact_code_and_inferred_type(r#"
                let bs = blobs::create("packages.blobs.len_blocking")
                let id = bs::append("Hello World")
                let size = bs::len()
                blobs::close(id)
                size
            "#, "70", "i64");
        }
    }

    /// Package "email" tests
    #[cfg(test)]
    mod email_tests {
        use lettre::message::Mailbox;
        use lettre::transport::smtp::authentication::Credentials;
        use lettre::{Message, SmtpTransport, Transport};

        /// #### Example
        /// ```
        /// smtp::send({
        ///     from: "Your Name <your_email@gmail.com>"
        ///     to: "Recipient <recipient@example.com>"
        ///     subject: "Test Email from Rust"
        ///     body: "Hello! This is a plain text email sent from Rust."
        ///     relay: "smtp.gmail.com"
        ///     credentials: ["your_email@gmail.com", "your_app_password"]
        /// })
        /// ```
        #[ignore]
        #[test]
        fn test_send_email() -> Result<(), Box<dyn std::error::Error>> {
            let email = Message::builder()
                .from("Your Name <your_email@gmail.com>".parse::<Mailbox>()?)
                .to("Recipient <recipient@example.com>".parse::<Mailbox>()?)
                .subject("Test Email from Rust")
                .body(lettre::message::Body::new("Hello! This is a plain text email sent from Rust.".to_string()))?;

            // Replace with your actual email and app password
            let creds = Credentials::new(
                "your_email@gmail.com".to_string(),
                "your_app_password".to_string(),
            );

            // Gmail SMTP server (use STARTTLS)
            let mailer = SmtpTransport::relay("smtp.gmail.com")?
                .credentials(creds)
                .build();

            match mailer.send(&email) {
                Ok(_) => println!("✅ Email sent successfully!"),
                Err(e) => eprintln!("❌ Failed to send email: {e:?}"),
            }

            Ok(())
        }
    }

    /// Package "http" tests
    #[cfg(test)]
    mod http_tests {
        use super::*;
        use crate::test_util::*;

        #[test]
        fn test_http_serve() {
            let port = webservers::get_random_port().unwrap();
            verify_exact_table(format!(r#"
                http::start({port})
                stocks = tables::save(
                   "packages.http_serve.stocks",
                   Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                )
                [{{ symbol: "XINU", exchange: "NYSE", last_sale: 8.11 }},
                 {{ symbol: "BOX", exchange: "NYSE", last_sale: 56.88 }},
                 {{ symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }},
                 {{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 }},
                 {{ symbol: "MIU", exchange: "OTCBB", last_sale: 2.24 }}] ~> stocks
                GET http://localhost:{port}/packages/http/stocks/1/4
            "#).as_str(), vec![
                "|------------------------------------|",
                "| id | exchange | last_sale | symbol |",
                "|------------------------------------|",
                "| 0  | NYSE     | 56.88     | BOX    |",
                "| 1  | NASDAQ   | 32.12     | JET    |",
                "| 2  | AMEX     | 12.49     | ABC    |",
                "|------------------------------------|"])
        }

        #[actix::test]
        async fn test_http_serve_async() {
            let port = webservers::get_random_port().unwrap();
            verify_exact_table_async(format!(r#"
                http::start({port})
                stocks = tables::save(
                   "packages.http_serve_async.stocks",
                   [{{ symbol: "XINU", exchange: "NYSE", last_sale: 8.11 }},
                    {{ symbol: "BOX", exchange: "NYSE", last_sale: 56.88 }},
                    {{ symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }},
                    {{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 }},
                    {{ symbol: "MIU", exchange: "OTCBB", last_sale: 2.24 }}]
                )
                select symbol, exchange, last_sale
                from (GET http://localhost:{port}/packages/http/stocks/0/5)
                where exchange in ["NYSE", "NASDAQ"]
            "#).as_str(), vec![
                "|------------------------------------|",
                "| id | symbol | exchange | last_sale |",
                "|------------------------------------|",
                "| 0  | XINU   | NYSE     | 8.11      |",
                "| 1  | BOX    | NYSE     | 56.88     |",
                "| 2  | JET    | NASDAQ   | 32.12     |",
                "|------------------------------------|"]).await
        }
    }

    /// Package "io" tests
    #[cfg(test)]
    mod io_tests {
        use crate::test_util::*;

        #[test]
        fn test_io_create_file_qualified() {
            verify_exact_code_and_inferred_type(r#"
                io::create_file("quote.json", { symbol: "TRX", exchange: "NYSE", last_sale: 45.32 })
            "#,"52", "i64");

            verify_exact_code_and_inferred_type(r#"
                io::exists("quote.json")
            "#, "true", "Boolean");
        }

        #[test]
        fn test_io_file_exists() {
            verify_exact_code_and_inferred_type(r#"
                path_str = oxide::home()
                io::exists(path_str)
            "#, "true", "Boolean")
        }

        #[test]
        fn test_io_create_and_read_text_file() {
            verify_exact_code_and_inferred_type(r#"
                use io
                file = "temp_secret.txt"
                file:::create_file("**keep**this**secret**"::md5())
                io::read_text_file(file)
            "#, "\"47338bd5-f35b-bb23-9092-c36e30775b4a\"", "String")
        }

        #[test]
        fn test_io_stderr() {
            verify_exact_code_and_inferred_type(r#"
                io::stderr("Goodbye Cruel World")
            "#, "true", "Boolean");
        }

        #[test]
        fn test_io_stdout() {
            verify_exact_code_and_inferred_type(r#"
                io::stdout("Goodbye Cruel World")
            "#, "true", "Boolean");
        }
    }

    /// Package "mysql" tests
    #[cfg(test)]
    mod mysql_tests {
        use mysql_async::{prelude::*, Pool};

        #[ignore]
        #[actix::test]
        async fn test_mysql_async() -> Result<(), Box<dyn std::error::Error>> {
            // Replace with your own connection string
            let url = "mysql://user:password@localhost:3306/test_db";

            // Create a connection pool
            let pool = Pool::new(url);

            // Get a connection from the pool
            let mut conn = pool.get_conn().await?;

            // Create a table (if not exists)
            conn.query_drop(r#"
                CREATE TABLE IF NOT EXISTS users (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    name VARCHAR(100) NOT NULL
                )"#).await?;

            // Insert a user
            conn.exec_drop("INSERT INTO users (name) VALUES (:name)",
                           params! {"name" => "Alice" }).await?;

            // Query the users
            let result: Vec<(u32, String)> =
                conn.query("SELECT id, name FROM users").await?;

            for row in result {
                println!("User: {:?}", row);
            }

            // Gracefully disconnect
            conn.disconnect().await?;
            Ok(())
        }
    }

    /// Package "tables" tests
    #[cfg(test)]
    mod nsd_tests {
        use crate::dataframe::Dataframe::DiskTable;
        use crate::interpreter::Interpreter;
        use crate::namespaces::Namespace;
        use crate::object_config::{HashIndexConfig, ObjectConfig};
        use crate::test_util::*;
        use crate::typed_values::TypedValue::{Boolean, StringValue, TableValue};

        #[test]
        fn test_nsd_create_event_source() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_value_whence(interpreter, r#"
                tables::drop("packages.events.stocks")
            "#, |r| matches!(r, Boolean(..)));

            interpreter = verify_exact_code_and_inferred_type_with(interpreter, r#"
                tables::exists("packages.events.stocks")
            "#, "false", "Boolean");

            interpreter = verify_exact_code_and_inferred_type_with(interpreter, r#"
                stocks = tables::create_event_src(
                    "packages.events.stocks",
                    Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                )
            "#, "true", "Boolean");

            interpreter = verify_exact_code_and_inferred_type_with(interpreter, r#"
                [{ symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                 { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
            "#, "3", "i64");

            interpreter = verify_exact_table_with(interpreter, "stocks", vec![
                "|------------------------------------|",
                "| id | symbol | exchange | last_sale |",
                "|------------------------------------|",
                "| 0  | BOOM   | NYSE     | 56.88     |",
                "| 1  | ABC    | AMEX     | 12.49     |",
                "| 2  | JET    | NASDAQ   | 32.12     |",
                "|------------------------------------|"]);

            verify_exact_table_with(interpreter, r#"
                use tables
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
                tables::drop("packages.table_fn.stocks")
            "#, |r| matches!(r, Boolean(..)));

            interpreter = verify_exact_code_and_inferred_type_with(interpreter, r#"
                tables::exists("packages.table_fn.stocks")
            "#, "false", "Boolean");

            interpreter = verify_exact_code_and_inferred_type_with(interpreter, r#"
                stocks = tables::create_fn(
                    "packages.table_fn.stocks",
                    (symbol: String(8), exchange: String(8), last_sale: f64) -> {
                        symbol: symbol,
                        exchange: exchange,
                        last_sale: last_sale * 2.0,
                        rank: __row_id__ + 1
                    })
            "#, "true", "Boolean");

            interpreter = verify_exact_code_and_inferred_type_with(interpreter, r#"
                [{ symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                 { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
            "#, "3", "i64");

            interpreter = verify_exact_table_with(interpreter, "stocks", vec![
                "|-------------------------------------------|",
                "| id | symbol | exchange | last_sale | rank |",
                "|-------------------------------------------|",
                "| 0  | BOOM   | NYSE     | 113.76    | 1    |",
                "| 1  | ABC    | AMEX     | 24.98     | 2    |",
                "| 2  | JET    | NASDAQ   | 64.24     | 3    |",
                "|-------------------------------------------|"]);

            verify_exact_table_with(interpreter, r#"
                use tables
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
        fn test_nsd_create_index() {
            let path = "packages.create_index.stocks";
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_table_with(interpreter, format!(r#"
                let stocks =
                   tables::save("{path}", [
                      {{ symbol: "ROFL", exchange: "AMEX", last_sale: 38.53 }},
                      {{ symbol: "LOLZ", exchange: "NYSE", last_sale: 6.57 }},
                      {{ symbol: "HMU", exchange: "NASDAQ", last_sale: 27.12 }},
                      {{ symbol: "SMH", exchange: "NYSE", last_sale: 16.95 }}
                   ])
                stocks
            "#).as_str(), vec![
                "|------------------------------------|",
                "| id | symbol | exchange | last_sale |",
                "|------------------------------------|",
                "| 0  | ROFL   | AMEX     | 38.53     |",
                "| 1  | LOLZ   | NYSE     | 6.57      |",
                "| 2  | HMU    | NASDAQ   | 27.12     |",
                "| 3  | SMH    | NYSE     | 16.95     |",
                "|------------------------------------|"]);

            // verify no indices currently exist
            let cfg0 = ObjectConfig::load(&Namespace::parse(path).unwrap()).unwrap();
            assert_eq!(cfg0.get_indices(), vec![]);
            assert_eq!(cfg0.get_partitions(), Some(vec![]));

            // create a new index
            verify_exact_code_and_inferred_type_with(interpreter, format!(r#"
               tables::create_index("{path}", [ "symbol" ])
            "#).as_str(), "true", "Boolean");

            // verify 1 index exists
            let cfg1 = ObjectConfig::load(&Namespace::parse(path).unwrap()).unwrap();
            assert_eq!(cfg1.get_indices(), vec![
                HashIndexConfig::new(
                    vec!["symbol".into()],
                     false
                )
            ])
        }

        #[test]
        fn test_nsd_resize() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_table_with(interpreter,r#"
                let stocks =
                   tables::save('packages.resize.stocks', Table(
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

            interpreter = verify_exact_code_and_inferred_type_with(interpreter,r#"
                tables::resize('packages.resize.stocks', 2)
            "#, "true", "Boolean");

            interpreter = verify_exact_table_with(interpreter, r#"
                stocks
            "#, vec![
                "|------------------------------------|",
                "| id | symbol | exchange | last_sale |",
                "|------------------------------------|",
                "| 0  | TCO    | NYSE     | 38.53     |",
                "| 1  | SHMN   | NYSE     | 6.57      |",
                "|------------------------------------|"]);

            interpreter = verify_exact_code_and_inferred_type_with(interpreter,r#"
                stocks::resize(1)
            "#, "true", "");

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
                tables::save("platform.save.stocks", Table(
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
                    tables::save("platform.tables.ns_save_and_load", Table(
                        symbol: String(8),
                        exchange: String(8),
                        last_sale: f64
                    )::new)

                [{ symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                 { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks

                tables::load("platform.tables.ns_save_and_load")
            "#, |df| matches!(df, TableValue(..)))
        }

        #[test]
        fn test_nsd_truncate() {
            let mut interpreter = Interpreter::new();
            interpreter.with_variable("path", StringValue("platform.truncate.stocks".into()));
            interpreter = verify_exact_table_with(interpreter, r#"
                let stocks =
                    [{ symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                     { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                     { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }]
                        ::to(Table)::save_as(path)
                stocks
            "#, vec![
                "|------------------------------------|",
                "| id | symbol | exchange | last_sale |",
                "|------------------------------------|",
                "| 0  | BOOM   | NYSE     | 56.88     |",
                "| 1  | ABC    | AMEX     | 12.49     |",
                "| 2  | JET    | NASDAQ   | 32.12     |",
                "|------------------------------------|"]);

            let _ = verify_exact_table_with(interpreter, r#"
                tables::truncate(path)
                stocks
            "#, vec![
                "|------------------------------------|",
                "| id | symbol | exchange | last_sale |",
                "|------------------------------------|",
                "|------------------------------------|"]);
        }
    }

    /// Package "os" tests
    #[cfg(test)]
    mod os_tests {
        use super::*;
        use crate::test_util::*;

        #[test]
        fn test_os_call() {
            verify_exact_code_and_inferred_type(r#"
                tables::save("platform.os.call", Table(
                    symbol: String(8),
                    exchange: String(8),
                    last_sale: f64
                )::new)
                os::call("chmod", "777", oxide::home())
            "#, "\"\"", "String")
        }

        #[test]
        fn test_os_clear() {
            verify_exact_code_and_inferred_type("os::clear()", "true", "Boolean")
        }

        #[test]
        fn test_os_current_dir() {
            verify_exact_table(r#"
                cur_dir = os::current_dir()
                prefix = if(cur_dir::ends_with("core"), "../..", ".")
                path_str = prefix + "/demoes/language/include_file.oxide"
                include path_str
            "#, vec![
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
        use crate::test_util::*;

        #[test]
        fn test_oxide_compile() {
            verify_exact_code_and_inferred_type(r#"
                code = oxide::compile("2 ** 4")
                code()
            "#, "16", "");
        }

        #[test]
        fn test_oxide_compile_closure() {
            verify_exact_code_and_inferred_type(r#"
                n = 5
                code = oxide::compile("n * n")
                code()
            "#, "25", "");
        }

        #[test]
        fn test_oxide_eval_closure() {
            verify_exact_code_and_inferred_type(r#"
                a = 'Hello '
                b = 'World'
                oxide::eval("a + b")
            "#, "\"Hello World\"", "String");
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
            let _ = verify_exact_value_with(
                interpreter,
                "123:::eval()",
                ErrorValue(Exact("Type Mismatch: Expected a String near 123".into())),
            );
        }

        #[test]
        fn test_oxide_help() {
            // fully-qualified
            verify_exact_value_where(
                "oxide::help()", |v| matches!(v, TableValue(..)));

            // imported
            verify_exact_value_where(r#"
                use oxide
                help()
            "#, |v| matches!(v, TableValue(..)));

            // return type
            verify_data_type("oxide::help()", TableType(OxidePkg::get_oxide_help_parameters()))
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
            verify_exact_code_and_inferred_type(r#"
                oxide::printf("Hello %s", "World")
            "#, "true", "Boolean");
        }

        #[test]
        fn test_oxide_println() {
            verify_exact_code_and_inferred_type(r#"
                oxide::println("Hello World")
            "#, "true", "Boolean");
        }

        #[test]
        fn test_oxide_sprintf() {
            verify_exact_code_and_inferred_type(r#"
                str::sprintf("Hello %s", "World")
            "#, "\"Hello World\"", "String");
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

    /// Package "util" tests
    #[cfg(test)]
    mod util_tests {
        use crate::test_util::*;

        #[test]
        fn test_util_base62_decode_as_bytes() {
            verify_exact_code_and_inferred_type(r#"
                util::base62_decode(util::base62_encode('little brown fox'))
            "#, "0B6c6974746c652062726f776e20666f78", "Bytes");
        }

        #[test]
        fn test_util_base62_decode_as_string() {
            verify_exact_code_and_inferred_type(r#"
                util::base62_decode(util::base62_encode('little brown fox'))::to(String)
            "#, "\"little brown fox\"", "String");
        }

        #[test]
        fn test_util_base62_decode_leading_zeroes() {
            verify_exact_code_and_inferred_type(r#"
                util::base62_decode('Hello World'::base62_encode)::to(String)
            "#, "\"\0\0\0\0\0Hello World\"", "String");
        }

        #[test]
        fn test_util_base64_decode_as_bytes() {
            verify_exact_code_and_inferred_type(r#"
                util::base64_decode('Hello World'::base64_encode)
            "#, "0B48656c6c6f20576f726c64", "Bytes");
        }

        #[test]
        fn test_util_base64_decode_as_string() {
            verify_exact_code_and_inferred_type(r#"
                util::base64_decode('Hello World'::base64_encode)::to(String)
            "#, "\"Hello World\"", "String");
        }

        #[test]
        fn test_util_base64_encode() {
            verify_exact_code_and_inferred_type(
                "'Hello World'::base64_encode", "\"SGVsbG8gV29ybGQ=\"", "String")
        }

        #[test]
        fn test_util_md5() {
            verify_exact_code_and_inferred_type(
                "util::md5('hello')", "5d41402a-bc4b-2a76-b971-9d911017c592", "UUID");
        }

        #[test]
        fn test_util_round() {
            verify_exact_code_and_inferred_type(
                "util::round(99.69333333333333, 4)", "99.6933", "f64");
        }

        #[test]
        fn test_util_to_string_array() {
            verify_exact_code_and_inferred_type(r#"
                "Hello there"::to(Array())
            "#, r#"['H', 'e', 'l', 'l', 'o', ' ', 't', 'h', 'e', 'r', 'e']"#, "Array()")
        }

        #[test]
        fn test_util_to_string_binary() {
            verify_exact_code_and_inferred_type(r#"
                "Hello there"::to(Bytes())
            "#, "0B48656c6c6f207468657265", "Bytes")
        }

        #[test]
        fn test_util_to_ascii() {
            verify_exact_code_and_inferred_type("177::to(Char)", "'±'", "Char")
        }

        #[test]
        fn test_util_to_hex() {
            verify_exact_code_and_inferred_type("util::hex('Hello World')", "\"48656c6c6f20576f726c64\"", "String")
        }
    }

    /// Package "www" tests
    #[cfg(test)]
    mod www_tests {
        use crate::connections::Connections::WebSocketHandle;
        use crate::interpreter::Interpreter;
        use crate::numbers::Numbers::I64Value;
        use crate::packages::{webservers, websockets};
        use crate::test_util::{make_lines_from_table, verify_exact_code_with, verify_exact_code_with_async, verify_exact_table_with_async, verify_exact_value};
        use crate::typed_values::TypedValue::{Connection, Number, StringValue};
        use crate::web_engine::WebSocketClient;

        #[actix::test]
        async fn test_websocket_with_client_script() {
            let port: u16 = webservers::get_random_port().unwrap();
            let mut interpreter = Interpreter::new();
            interpreter.with_variable("port", Number(I64Value(port as i64)));
            interpreter = verify_exact_code_with(interpreter, r#"
                http::start(port, {
                    "/api/ws" : {
                        "WS" : (() -> {
                            "on_open" : ((conn, message) -> message)
                            "on_message" : ((conn, message) -> {
                                let stocks = tables::load("packages.user_websockets_script.stocks")
                                stocks where symbol is message::to(String)
                            })
                            "on_close" : ((conn, message) -> message)
                        })
                    }
                })
            "#, "true");

            let _ = verify_exact_code_with(interpreter, r#"
                let stocks = tables::save("packages.user_websockets_script.stocks",
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
            "#, "true");

            // connect the web socket
            let mut wsc = WebSocketClient::connect("0.0.0.0", port, "/api/ws").await.unwrap();

            // send a text message
            wsc.send_text_message("JTRQ").await.unwrap();

            let value = wsc.read_next().await.unwrap();
            assert_eq!(make_lines_from_table(value), vec![
                "|--------------------------------|",
                "| symbol | exchange  | last_sale |",
                "|--------------------------------|",
                "| JTRQ   | OTHER_OTC | 0.0001    |",
                "|--------------------------------|"]);

            // send a binary message
            wsc.send_binary_message(b"TRX".to_vec()).await.unwrap();

            let value = wsc.read_next().await.unwrap();
            assert_eq!(make_lines_from_table(value), vec![
                "|-------------------------------|",
                "| symbol | exchange | last_sale |",
                "|-------------------------------|",
                "| TRX    | NASDAQ   | 32.96     |",
                "|-------------------------------|"]);

            // close the connection
            let outcome = wsc.close().await.unwrap();
            assert_eq!(wsc.read_next().await.unwrap(), StringValue(String::new()));
        }

        #[actix::test]
        async fn test_websocket_platform_script() {
            let port: u16 = webservers::get_random_port().unwrap();
            let path = "/api/ws";

            let mut interpreter = Interpreter::new();
            interpreter.with_variable("port", Number(I64Value(port as i64)));
            interpreter.with_variable("path", StringValue(path.into()));
            interpreter = verify_exact_code_with_async(interpreter, r#"
                http::start(port, {
                    "/api/ws" : {
                        "WS" : (() -> {
                            "on_open" : ((conn, message) -> message)
                            "on_message" : ((conn, message) -> {
                                let stocks = tables::load("packages.websocket_builtins.stocks")
                                stocks where symbol is message::to(String)
                            })
                            "on_close" : ((conn, message) -> message)
                        })
                    }
                })
            "#, "true").await;

            interpreter = verify_exact_code_with_async(interpreter, r#"
                let stocks = tables::save("packages.websocket_builtins.stocks",
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
            "#, "true").await;

            interpreter = verify_exact_code_with_async(interpreter, format!(r#"
                let conn = ws::connect("0.0.0.0", {port}, "{path}")
            "#).as_str(), "true").await;

            interpreter = verify_exact_table_with_async(interpreter, r#"
                conn::send_text("SHMN")
            "#, vec![
                "|------------------------------------|",
                "| id | symbol | exchange | last_sale |",
                "|------------------------------------|",
                "| 2  | SHMN   | OTCBB    | 5.02      |",
                "|------------------------------------|"]).await;

            interpreter = verify_exact_table_with_async(interpreter, r#"
                conn::send_bytes("DRMQ"::to(Bytes))
            "#, vec![
                "|-------------------------------------|",
                "| id | symbol | exchange  | last_sale |",
                "|-------------------------------------|",
                "| 4  | DRMQ   | OTHER_OTC | 0.02      |",
                "|-------------------------------------|"]).await;

            interpreter = verify_exact_code_with_async(interpreter, r#"
                conn::close()
            "#, "\"\"").await;

            interpreter = verify_exact_code_with_async(interpreter, r#"
                http::stop(port)
            "#, "true").await;
        }

        #[actix::test]
        async fn test_websockets_package_script() {
            let port: u16 = webservers::get_random_port().unwrap();
            let mut interpreter = Interpreter::new();
            interpreter.with_variable("port", Number(I64Value(port as i64)));
            interpreter = verify_exact_code_with(interpreter, r#"
                http::start(port, {
                    "/api/ws" : {
                        "WS" : (() -> {
                            "on_open" : ((conn, message) -> message)
                            "on_message" : ((conn, message) -> {
                                let stocks = tables::load("package.websockets.stocks")
                                stocks where symbol is message::to(String)
                            })
                            "on_close" : ((conn, message) -> message)
                        })
                    }
                })
            "#, "true");

            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_code_with(interpreter, r#"
                let stocks = tables::save("package.websockets.stocks",
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
            "#, "true");

            // connect the web socket
            if let Connection(WebSocketHandle(client_id)) = websockets::connect_ws("0.0.0.0", port, "/api/ws").await.unwrap() {
                // send a text command
                let value = websockets::send_text_command(client_id, "SHMN").await.unwrap();
                assert_eq!(make_lines_from_table(value), vec![
                    "|-------------------------------|",
                    "| symbol | exchange | last_sale |",
                    "|-------------------------------|",
                    "| SHMN   | OTCBB    | 5.02      |",
                    "|-------------------------------|"]);

                // send a binary command
                let value = websockets::send_binary_command(client_id, b"DRMQ".to_vec()).await.unwrap();
                assert_eq!(make_lines_from_table(value), vec![
                    "|--------------------------------|",
                    "| symbol | exchange  | last_sale |",
                    "|--------------------------------|",
                    "| DRMQ   | OTHER_OTC | 0.02      |",
                    "|--------------------------------|"]);

                // close the connection
                let outcome = websockets::close(client_id).await.unwrap();
                assert_eq!(outcome, StringValue(String::new()));
            } else {
                assert!(false);
            }
        }

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
                StringValue("http%3A%2F%2Fshocktrade.com%3Fname%3Dthe%20hero%26t%3D9998".to_string())
            )
        }
    }
}
