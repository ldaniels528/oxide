#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Platform Packages module
////////////////////////////////////////////////////////////////////

use crate::compiler::Compiler;
use crate::data_types::DataType;
use crate::data_types::DataType::*;
use crate::dataframe::Dataframe::{Disk, EventSource, Model, TableFn};
use crate::errors::Errors::*;
use crate::errors::TypeMismatchErrors::*;
use crate::errors::{throw, TypeMismatchErrors};
use crate::expression::Expression::{CodeBlock, FunctionCall, Literal, Multiply, Scenario, StructureExpression};
use crate::file_row_collection::FileRowCollection;
use crate::formatting::DataFormats;
use crate::journaling::Journaling;
use crate::machine::Machine;
use crate::model_row_collection::ModelRowCollection;
use crate::namespaces::Namespace;
use crate::number_kind::NumberKind::*;
use crate::numbers::Numbers::*;
use crate::parameter::Parameter;
use crate::platform::PackageOps::Cal;
use crate::platform::{Package, PackageOps, VERSION};
use crate::row_collection::RowCollection;
use crate::sequences::Sequences::{TheArray, TheDataframe, TheRange, TheTuple};
use crate::sequences::{range_diff, Array, Sequence};
use crate::structures::Structures::{Hard, Soft};
use crate::structures::{Row, Structure};
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{
    ArrayValue, Binary, Boolean, ErrorValue, Function, NamespaceValue, Null, Number, PlatformOp,
    Sequenced, StringValue, Structured, TableValue, TupleValue, Undefined,
};
use crate::utils::{extract_array_fn1, extract_number_fn1, extract_number_fn2, extract_value_fn0, extract_value_fn1, extract_value_fn2, extract_value_fn3, pull_array, pull_string, strip_margin, superscript};
use crate::{machine, oxide_server};
use chrono::{Datelike, Local, TimeZone, Timelike};
use serde::{Deserialize, Serialize};
use shared_lib::cnv_error;
use std::env;
use std::fs::File;
use std::io::{stderr, stdout, Read, Write};
use std::ops::Deref;
use std::path::Path;
use uuid::Uuid;

// duration unit constants
const MILLIS: i64 = 1;
const SECONDS: i64 = 1000 * MILLIS;
const MINUTES: i64 = 60 * SECONDS;
const HOURS: i64 = 60 * MINUTES;
const DAYS: i64 = 24 * HOURS;

/// Arrays package
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum ArraysPkg {
    Filter,
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
            Function { .. } => match items.to_sequence()? {
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
                TheRange(..) => Self::do_arrays_reduce(ms, &items.to_array(), initial, function),
                z => throw(TypeMismatch(ArrayExpected(z.unwrap_value()))),
            },
            z => throw(TypeMismatch(TypeMismatchErrors::FunctionExpected(
                z.to_code(),
            ))),
        }
    }

    pub fn get_contents() -> Vec<PackageOps> {
        vec![
            PackageOps::Arrays(ArraysPkg::Filter),
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
            ArraysPkg::Filter => vec![strip_margin(
                r#"
                    |arrays::filter(1..7, n -> (n % 2) == 0)
               "#,
                '|',
            )],
            ArraysPkg::Len => vec![strip_margin(
                r#"
                    |arrays::len([1, 5, 2, 4, 6, 0])
               "#,
                '|',
            )],
            ArraysPkg::Map => vec![strip_margin(
                r#"
                    |arrays::map([1, 2, 3], n -> n * 2)
               "#,
                '|',
            )],
            ArraysPkg::Pop => vec![strip_margin(
                r#"
                    |use arrays
                    |stocks = []
                    |stocks:::push({ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 })
                    |stocks:::push({ symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 })
                    |stocks
                "#,
                '|',
            )],
            ArraysPkg::Push => vec![strip_margin(
                r#"
                    |use arrays
                    |stocks = [
                    |    { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                    |    { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                    |    { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }
                    |]
                    |stocks:::push({ symbol: "DEX", exchange: "OTC_BB", last_sale: 0.0086 })
                    |from stocks
                "#,
                '|',
            )],
            ArraysPkg::Reduce => vec![
                strip_margin(r#"
                    |arrays::reduce(1..=5, 0, (a, b) -> a + b)
                "#, '|'),
                strip_margin(
                r#"
                    |use arrays::reduce
                    |numbers = [1, 2, 3, 4, 5]
                    |numbers:::reduce(0, (a, b) -> a + b)
                "#,
                '|')
            ],
            ArraysPkg::Reverse => vec![strip_margin(
                r#"
                    |arrays::reverse(['cat', 'dog', 'ferret', 'mouse'])
                "#,
                '|',
            )],
            ArraysPkg::ToArray => vec![strip_margin(
                r#"
                    |arrays::to_array(tools::to_table([
                    |   { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                    |   { symbol: "DMX", exchange: "OTC_BB", last_sale: 1.17 }
                    |]))
                "#,
                '|',
            )],
        }
    }

    fn get_parameter_types(&self) -> Vec<DataType> {
        match self {
            ArraysPkg::Filter => vec![
                ArrayType(0),
                FunctionType(
                    vec![Parameter::new("item", UnresolvedType)],
                    BooleanType.into(),
                ),
            ],
            ArraysPkg::Len => vec![ArrayType(0)],
            ArraysPkg::Map => vec![
                ArrayType(0),
                FunctionType(
                    vec![Parameter::new("item", UnresolvedType)],
                    UnresolvedType.into(),
                ),
            ],
            ArraysPkg::Pop | ArraysPkg::Reverse => vec![ArrayType(0)],
            ArraysPkg::Push => vec![ArrayType(0), UnresolvedType],
            ArraysPkg::Reduce => vec![
                ArrayType(0), UnresolvedType, FunctionType(vec![
                    Parameter::new("a", UnresolvedType),
                    Parameter::new("b", UnresolvedType),
                ], UnresolvedType.into())
            ],
            ArraysPkg::ToArray => vec![UnresolvedType],
        }
    }

    fn get_return_type(&self) -> DataType {
        match self {
            ArraysPkg::Filter
            | ArraysPkg::Map
            | ArraysPkg::Reverse
            | ArraysPkg::ToArray => ArrayType(0),
            ArraysPkg::Len => NumberType(I64Kind),
            ArraysPkg::Pop | ArraysPkg::Push => BooleanType,
            ArraysPkg::Reduce => UnresolvedType,
        }
    }

    fn evaluate(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match self {
            ArraysPkg::Filter => extract_value_fn2(ms, args, ToolsPkg::do_tools_filter),
            ArraysPkg::Len => extract_array_fn1(ms, args, |a| Number(I64Value(a.len() as i64))),
            ArraysPkg::Map => extract_value_fn2(ms, args, ToolsPkg::do_tools_map),
            ArraysPkg::Pop => extract_value_fn1(ms, args, ArraysPkg::do_arrays_pop),
            ArraysPkg::Push => ToolsPkg::do_tools_push(ms, args),
            ArraysPkg::Reduce => extract_value_fn3(ms, args, Self::do_arrays_reduce),
            ArraysPkg::Reverse => extract_array_fn1(ms, args, |a| ArrayValue(a.rev())),
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
    Minus,
    Now,
    Plus,
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
            Number(DateValue(epoch_millis)) => {
                let datetime = {
                    let seconds = epoch_millis / 1000;
                    let millis_part = epoch_millis % 1000;
                    Local.timestamp(seconds, (millis_part * 1_000_000) as u32)
                };
                match plat {
                    CalPkg::DateDay => Ok((ms, Number(U32Value(datetime.day())))),
                    CalPkg::DateHour12 => Ok((ms, Number(U32Value(datetime.hour12().1)))),
                    CalPkg::DateHour24 => Ok((ms, Number(U32Value(datetime.hour())))),
                    CalPkg::DateMinute => Ok((ms, Number(U32Value(datetime.minute())))),
                    CalPkg::DateMonth => Ok((ms, Number(U32Value(datetime.month())))),
                    CalPkg::DateSecond => Ok((ms, Number(U32Value(datetime.second())))),
                    CalPkg::DateYear => Ok((ms, Number(I32Value(datetime.year())))),
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
        Ok((ms, Number(DateValue(date.to_i64() - duration.to_i64()))))
    }

    fn do_cal_date_plus(
        ms: Machine,
        date: &TypedValue,
        duration: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        Ok((ms, Number(DateValue(date.to_i64() + duration.to_i64()))))
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
            PackageOps::Cal(CalPkg::Minus),
            PackageOps::Cal(CalPkg::Now),
            PackageOps::Cal(CalPkg::Plus),
        ]
    }
}

impl Package for CalPkg {
    fn get_name(&self) -> String {
        match self {
            CalPkg::DateDay => "day_of".into(),
            CalPkg::DateHour12 => "hour12".into(),
            CalPkg::DateHour24 => "hour24".into(),
            CalPkg::DateMinute => "minute_of".into(),
            CalPkg::DateMonth => "month_of".into(),
            CalPkg::DateSecond => "second_of".into(),
            CalPkg::DateYear => "year_of".into(),
            CalPkg::Minus => "minus".into(),
            CalPkg::Now => "now".into(),
            CalPkg::Plus => "plus".into(),
        }
    }

    fn get_package_name(&self) -> String {
        "cal".into()
    }

    fn get_description(&self) -> String {
        match self {
            CalPkg::DateDay => "Returns the day of the month of a Date".into(),
            CalPkg::DateHour12 => "Returns the hour of the day of a Date".into(),
            CalPkg::DateHour24 => "Returns the hour (military time) of the day of a Date".into(),
            CalPkg::DateMinute => "Returns the minute of the hour of a Date".into(),
            CalPkg::DateMonth => "Returns the month of the year of a Date".into(),
            CalPkg::DateSecond => "Returns the seconds of the minute of a Date".into(),
            CalPkg::DateYear => "Returns the year of a Date".into(),
            CalPkg::Minus => "Subtracts a duration from a date".into(),
            CalPkg::Now => "Returns the current local date and time".into(),
            CalPkg::Plus => "Adds a duration to a date".into(),
        }
    }

    fn get_examples(&self) -> Vec<String> {
        match self {
            // cal
            CalPkg::DateDay => vec![strip_margin(
                r#"
                    |use cal
                    |now():::day_of()
                "#,
                '|',
            )],
            CalPkg::DateHour12 => vec![strip_margin(
                r#"
                    |use cal
                    |now():::hour12()
                "#,
                '|',
            )],
            CalPkg::DateHour24 => vec![strip_margin(
                r#"
                    |use cal
                    |now():::hour24()
                "#,
                '|',
            )],
            CalPkg::DateMinute => vec![strip_margin(
                r#"
                    |use cal
                    |now():::minute_of()
                "#,
                '|',
            )],
            CalPkg::DateMonth => vec![strip_margin(
                r#"
                    |use cal
                    |now():::month_of()
                "#,
                '|',
            )],
            CalPkg::DateSecond => vec![strip_margin(
                r#"
                    |use cal
                    |now():::second_of()
                "#,
                '|',
            )],
            CalPkg::DateYear => vec![strip_margin(
                r#"
                    |use cal
                    |now():::year_of()
                "#,
                '|',
            )],
            CalPkg::Minus => vec![strip_margin(
                r#"
                    |use cal, durations
                    |cal::minus(now(), 3:::days())
                "#,
                '|',
            )],
            CalPkg::Now => vec!["cal::now()".to_string()],
            CalPkg::Plus => vec![strip_margin(
                r#"
                    |use cal, durations
                    |cal::plus(now(), 30:::days())
                "#,
                '|',
            )],
        }
    }

    fn get_parameter_types(&self) -> Vec<DataType> {
        match self {
            CalPkg::Now => vec![],
            // single-parameter (date)
            CalPkg::DateDay
            | CalPkg::DateHour12
            | CalPkg::DateHour24
            | CalPkg::DateMinute
            | CalPkg::DateMonth
            | CalPkg::DateSecond
            | CalPkg::DateYear => vec![NumberType(DateKind)],
            // two-parameter (date, i64)
            CalPkg::Minus | CalPkg::Plus => vec![NumberType(DateKind), NumberType(I64Kind)],
        }
    }

    fn get_return_type(&self) -> DataType {
        match self {
            CalPkg::DateDay
            | CalPkg::DateHour12
            | CalPkg::DateHour24
            | CalPkg::DateMinute
            | CalPkg::DateMonth
            | CalPkg::DateSecond => NumberType(U32Kind),
            CalPkg::DateYear => NumberType(I32Kind),
            // date
            CalPkg::Minus | CalPkg::Now | CalPkg::Plus => NumberType(DateKind),
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
            CalPkg::Minus => extract_value_fn2(ms, args, Self::do_cal_date_minus),
            CalPkg::Now => extract_value_fn0(ms, args, |ms| {
                Ok((ms, Number(DateValue(Local::now().timestamp_millis()))))
            }),
            CalPkg::Plus => extract_value_fn2(ms, args, Self::do_cal_date_plus),
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
            DurationsPkg::Days => vec![strip_margin(
                r#"
                    |use durations
                    |3:::days()
                "#,
                '|',
            )],
            DurationsPkg::Hours => vec![strip_margin(
                r#"
                    |use durations
                    |8:::hours()
                "#,
                '|',
            )],
            DurationsPkg::Millis => vec![strip_margin(
                r#"
                    |use durations
                    |8:::millis()
                "#,
                '|',
            )],
            DurationsPkg::Minutes => vec![strip_margin(
                r#"
                    |use durations
                    |30:::minutes()
                "#,
                '|',
            )],
            DurationsPkg::Seconds => vec![strip_margin(
                r#"
                    |use durations
                    |30:::seconds()
                "#,
                '|',
            )],
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
        Ok((ms, Number(U64Value(n_bytes))))
    }

    fn do_io_exists(
        ms: Machine,
        path_value: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let path = pull_string(path_value)?;
        Ok((ms, Boolean(Path::new(path.as_str()).exists())))
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

    pub(crate) fn do_io_stdout(
        ms: Machine,
        value: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let mut out = stdout();
        out.write(format!("{}", value.unwrap_value()).as_bytes())?;
        out.flush()?;
        Ok((ms, Boolean(true)))
    }
    
    pub fn get_contents() -> Vec<PackageOps> {
        vec![
            PackageOps::Io(IoPkg::FileCreate),
            PackageOps::Io(IoPkg::FileExists),
            PackageOps::Io(IoPkg::FileReadText),
            PackageOps::Io(IoPkg::StdErr),
            PackageOps::Io(IoPkg::StdOut),
        ]
    }
}

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
            IoPkg::StdOut => extract_value_fn1(ms, args, Self::do_io_stdout),
        }
    }

    fn get_description(&self) -> String {
        match self {
            IoPkg::FileCreate => "Creates a new file".into(),
            IoPkg::FileExists => "Returns true if the source path exists".into(),
            IoPkg::FileReadText => "Reads the contents of a text file into memory".into(),
            IoPkg::StdErr => "Writes a string to STDERR".into(),
            IoPkg::StdOut => "Writes a string to STDOUT".into(),
        }
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
                "#,
                '|',
            )],
            IoPkg::FileExists => vec![r#"io::exists("quote.json")"#.to_string()],
            IoPkg::FileReadText => vec![strip_margin(
                r#"
                    |use io, util
                    |file = "temp_secret.txt"
                    |file:::create_file(md5("**keep**this**secret**"))
                    |file:::read_text_file()
                "#,
                '|',
            )],
            IoPkg::StdErr => vec![r#"io::stderr("Goodbye Cruel World")"#.to_string()],
            IoPkg::StdOut => vec![r#"io::stdout("Hello World")"#.to_string()],
        }
    }

    fn get_name(&self) -> String {
        match self {
            IoPkg::FileCreate => "create_file".into(),
            IoPkg::FileExists => "exists".into(),
            IoPkg::FileReadText => "read_text_file".into(),
            IoPkg::StdErr => "stderr".into(),
            IoPkg::StdOut => "stdout".into(),
        }
    }

    fn get_package_name(&self) -> String {
        "io".into()
    }

    fn get_parameter_types(&self) -> Vec<DataType> {
        match self {
            IoPkg::FileCreate => vec![StringType(0), StringType(0)],
            IoPkg::FileExists | IoPkg::FileReadText | IoPkg::StdErr | IoPkg::StdOut => {
                vec![StringType(0)]
            }
        }
    }

    fn get_return_type(&self) -> DataType {
        match self {
            IoPkg::FileReadText => ArrayType(0),
            IoPkg::FileCreate | IoPkg::FileExists => BooleanType,
            IoPkg::StdErr | IoPkg::StdOut => StringType(0),
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

/// Oxide package
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum OxidePkg {
    Compile,
    Debug,
    Eval,
    Help,
    History,
    Home,
    Println,
    Reset,
    UUID,
    Version,
}

impl OxidePkg {
    fn do_oxide_compile(ms: Machine, value: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        let source = pull_string(value)?;
        let code = Compiler::build(source.as_str())?;
        Ok((
            ms,
            Function {
                params: vec![],
                body: Box::new(code),
                returns: UnresolvedType,
            },
        ))
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
        Ok((ms, TableValue(Model(mrc))))
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
                Ok((ms, TableValue(Disk(frc))))
            }
            // history(11)
            [Number(pid)] => re_run(ms.to_owned(), pid.to_usize()),
            // history(..)
            other => throw(TypeMismatch(ArgumentsMismatched(other.len(), 1))),
        }
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
            PackageOps::Oxide(OxidePkg::Println),
            PackageOps::Oxide(OxidePkg::Reset),
            PackageOps::Oxide(OxidePkg::UUID),
            PackageOps::Oxide(OxidePkg::Version),
        ]
    }

    pub fn get_oxide_help_parameters() -> Vec<Parameter> {
        vec![
            Parameter::new("name", StringType(20)),
            Parameter::new("module", StringType(20)),
            Parameter::new("signature", StringType(32)),
            Parameter::new("description", StringType(60)),
            Parameter::new("returns", StringType(32)),
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
            Parameter::new("input", StringType(65536)),
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
            OxidePkg::Println => "println".into(),
            OxidePkg::Reset => "reset".into(),
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
            OxidePkg::Println => "Print line function".into(),
            OxidePkg::Eval => "Evaluates a string containing Oxide code".into(),
            OxidePkg::Help => "Integrated help function".into(),
            OxidePkg::History => {
                "Returns all commands successfully executed during the session".into()
            }
            OxidePkg::Home => "Returns the Oxide home directory path".into(),
            OxidePkg::Reset => "Clears the scope of all user-defined objects".into(),
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
            OxidePkg::Println => vec![r#"oxide::println("Hello World")"#.into()],
            OxidePkg::Eval => vec![strip_margin(
                r#"
                    |a = 'Hello '
                    |b = 'World'
                    |oxide::eval("a + b")
                "#,
                '|',
            )],
            OxidePkg::Help => vec![r#"from oxide::help() limit 3"#.into()],
            OxidePkg::History => vec!["from oxide::history() limit 3".into()],
            OxidePkg::Home => vec!["oxide::home()".into()],
            OxidePkg::Reset => vec!["oxide::reset()".into()],
            OxidePkg::UUID => vec!["oxide::uuid()".into()],
            OxidePkg::Version => vec!["oxide::version()".into()],
        }
    }

    fn get_parameter_types(&self) -> Vec<DataType> {
        match self {
            OxidePkg::Home
            | OxidePkg::Reset
            | OxidePkg::Help
            | OxidePkg::History
            | OxidePkg::Version
            | OxidePkg::UUID => vec![],
            // single-parameter (string)
            OxidePkg::Println | OxidePkg::Compile | OxidePkg::Debug | OxidePkg::Eval => {
                vec![StringType(0)]
            }
        }
    }

    fn get_return_type(&self) -> DataType {
        match self {
            // function
            OxidePkg::Compile | OxidePkg::Debug => FunctionType(vec![], UnresolvedType.into()),
            OxidePkg::Eval | OxidePkg::Home => StringType(0),
            OxidePkg::Help => TableType(OxidePkg::get_oxide_help_parameters(), 0),
            OxidePkg::History => TableType(OxidePkg::get_oxide_history_parameters(), 0),
            OxidePkg::Println | OxidePkg::Reset => BooleanType,
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
            OxidePkg::Home => {
                extract_value_fn0(ms, args, |ms| Ok((ms, StringValue(Machine::oxide_home()))))
            }
            OxidePkg::Println => extract_value_fn1(ms, args, IoPkg::do_io_stdout),
            OxidePkg::Reset => {
                extract_value_fn0(ms, args, |ms| Ok((Machine::new_platform(), Boolean(true))))
            }
            OxidePkg::UUID => UtilsPkg::do_util_uuid(ms, args),
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
            Parameter::new("key", StringType(256)),
            Parameter::new("value", StringType(8192)),
        ]);
        for (key, value) in env::vars() {
            if let ErrorValue(err) =
                mrc.append_row(Row::new(0, vec![StringValue(key), StringValue(value)]))
            {
                return Ok((ms, ErrorValue(err)));
            }
        }
        Ok((ms, TableValue(Model(mrc))))
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
            Parameter::new("key", StringType(256)),
            Parameter::new("value", StringType(8192)),
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
                    |create table ns("examples.os.call") (
                    |    symbol: String(8),
                    |    exchange: String(8),
                    |    last_sale: f64
                    |)
                    |os::call("chmod", "777", oxide::home())
                "#,
                '|',
            )],
            OsPkg::Clear => vec!["os::clear()".into()],
            OsPkg::CurrentDir => vec![strip_margin(
                r#"
                    |use str
                    |cur_dir = os::current_dir()
                    |prefix = iff(cur_dir:::ends_with("core"), "../..", ".")
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
            OsPkg::Call => vec![StringType(0)],
            OsPkg::Clear | OsPkg::CurrentDir | OsPkg::Env => vec![],
        }
    }

    fn get_return_type(&self) -> DataType {
        match self {
            OsPkg::Call | OsPkg::CurrentDir => StringType(0),
            OsPkg::Clear => BooleanType,
            OsPkg::Env => TableType(OsPkg::get_os_env_parameters(), 0),
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
    Join,
    IndexOf,
    Left,
    Len,
    Right,
    Split,
    StartsWith,
    StripMargin,
    Substring,
    SuperScript,
    ToString,
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

    /// str::join(\["a", "b", "c"], ", ") => "a, b, c"
    fn do_str_join(
        ms: Machine,
        array: &TypedValue,
        delim: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let items = pull_array(array)?;
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
        let s = pull_string(string)?;
        Ok((ms, Number(I64Value(s.len() as i64))))
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

    pub(crate) fn get_contents() -> Vec<PackageOps> {
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
            PackageOps::Strings(StringsPkg::ToString),
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
            StringsPkg::ToString => "to_string".into(),
        }
    }

    fn get_package_name(&self) -> String {
        "str".into()
    }

    fn get_description(&self) -> String {
        match self {
            StringsPkg::EndsWith => "Returns true if string `a` ends with string `b`".into(),
            StringsPkg::Format => "Returns an argument-formatted string".into(),
            StringsPkg::IndexOf => "Returns the index of string `b` in string `a`".into(),
            StringsPkg::Join => "Combines an array into a string".into(),
            StringsPkg::Left => "Returns n-characters from left-to-right".into(),
            StringsPkg::Len => "Returns the number of characters contained in the string".into(),
            StringsPkg::Right => "Returns n-characters from right-to-left".into(),
            StringsPkg::Split => "Splits string `a` by delimiter string `b`".into(),
            StringsPkg::StartsWith => "Returns true if string `a` starts with string `b`".into(),
            StringsPkg::StripMargin => "Returns the string with all characters on each line are striped up to the margin character".into(),
            StringsPkg::Substring => "Returns a substring of string `s` from `m` to `n`".into(),
            StringsPkg::SuperScript => "Returns a superscript of a number `n`".into(),
            StringsPkg::ToString => "Converts a value to its text-based representation".into(),
        }
    }

    fn get_examples(&self) -> Vec<String> {
        match self {
            StringsPkg::EndsWith => vec![r#"str::ends_with('Hello World', 'World')"#.into()],
            StringsPkg::Format => vec![r#"str::format("This {} the {}", "is", "way")"#.into()],
            StringsPkg::IndexOf => {
                vec![r#"str::index_of('The little brown fox', 'brown')"#.into()]
            }
            StringsPkg::Join => vec![r#"str::join(['1', 5, 9, '13'], ', ')"#.into()],
            StringsPkg::Left => vec![r#"str::left('Hello World', 5)"#.into()],
            StringsPkg::Len => vec![r#"str::len('The little brown fox')"#.into()],
            StringsPkg::Right => vec!["str::right('Hello World', 5)".into()],
            StringsPkg::Split => vec![r#"str::split('Hello,there World', ' ,')"#.into()],
            StringsPkg::StartsWith => vec!["str::starts_with('Hello World', 'World')".into()],
            StringsPkg::StripMargin => vec![strip_margin(
                r#"
                    |str::strip_margin("
                    ||Code example:
                    ||
                    ||from stocks
                    ||where exchange is 'NYSE'
                    |", '|')"#,
                '|',
            )],
            StringsPkg::Substring => vec!["str::substring('Hello World', 0, 5)".into()],
            StringsPkg::SuperScript => vec!["str::superscript(5)".into()],
            StringsPkg::ToString => vec!["str::to_string(125.75)".into()],
        }
    }

    fn get_parameter_types(&self) -> Vec<DataType> {
        match self {
            // two-parameter (string, string)
            StringsPkg::EndsWith
            | StringsPkg::Format
            | StringsPkg::Split
            | StringsPkg::StartsWith
            | StringsPkg::StripMargin => vec![StringType(0), StringType(0)],
            // two-parameter (string, i64)
            StringsPkg::IndexOf | StringsPkg::Left | StringsPkg::Right => {
                vec![StringType(0), NumberType(I64Kind)]
            }
            StringsPkg::Len => vec![StringType(0)],
            // two-parameter (array, string)
            StringsPkg::Join => vec![ArrayType(0), StringType(0)],
            // three-parameter (string, i64, i64)
            StringsPkg::Substring => vec![StringType(0), NumberType(I64Kind), NumberType(I64Kind)],
            StringsPkg::SuperScript => vec![NumberType(I64Kind)],
            StringsPkg::ToString => vec![UnresolvedType],
        }
    }

    fn get_return_type(&self) -> DataType {
        match self {
            // Boolean
            StringsPkg::EndsWith | StringsPkg::StartsWith => BooleanType,
            StringsPkg::Format
            | StringsPkg::Join
            | StringsPkg::Left
            | StringsPkg::Right
            | StringsPkg::StripMargin
            | StringsPkg::Substring
            | StringsPkg::ToString => StringType(0),
            // Number
            StringsPkg::IndexOf | StringsPkg::Len => NumberType(I64Kind),
            // Array
            StringsPkg::Split => ArrayType(0),
            // String
            StringsPkg::SuperScript => StringType(0),
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
            StringsPkg::ToString => {
                extract_value_fn1(ms, args, |ms, v| Ok((ms, StringValue(v.unwrap_value()))))
            }
        }
    }
}

/// Testing package
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum TestingPkg {
    Assert,
    Feature,
    Matches,
    TypeOf,
}

impl TestingPkg {
    fn do_testing_assert(
        ms: Machine,
        value: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match value {
            ErrorValue(msg) => throw(msg.to_owned()),
            Boolean(false) => throw(AssertionError("true".to_string(), "false".to_string())),
            z => Ok((ms, z.to_owned())),
        }
    }

    fn do_testing_feature(
        ms: Machine,
        title: &TypedValue,
        body: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        // get the feature title
        let title = match title {
            StringValue(s) => Literal(StringValue(s.to_string())),
            other => {
                return throw(TypeMismatch(UnsupportedType(
                    StringType(0),
                    other.get_type(),
                )))
            }
        };

        // get the feature scenarios
        let scenarios = match body {
            Structured(Soft(ss)) => ss
                .to_name_values()
                .iter()
                .map(|(title, tv)| match tv {
                    Function { body: code, .. } => Scenario {
                        title: Box::new(Literal(StringValue(title.to_string()))),
                        verifications: match code.deref() {
                            CodeBlock(ops) => ops.clone(),
                            other => vec![other.clone()],
                        },
                    },
                    other => Literal(other.clone()),
                })
                .collect::<Vec<_>>(),
            other => {
                return throw(TypeMismatch(UnsupportedType(
                    ArrayType(0),
                    other.get_type(),
                )))
            }
        };

        // execute the feature
        ms.do_feature(&Box::from(title), &scenarios)
    }

    fn do_testing_matches(
        ms: Machine,
        a: &TypedValue,
        b: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        Ok((ms, a.matches(b)))
    }

    fn do_testing_type_of(ms: Machine, a: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        Ok((ms, StringValue(a.get_type_name())))
    }

    pub(crate) fn get_contents() -> Vec<PackageOps> {
        vec![
            PackageOps::Testing(TestingPkg::Assert),
            PackageOps::Testing(TestingPkg::Feature),
            PackageOps::Testing(TestingPkg::Matches),
            PackageOps::Testing(TestingPkg::TypeOf),
        ]
    }
}

impl Package for TestingPkg {
    fn get_name(&self) -> String {
        match self {
            TestingPkg::Assert => "assert".into(),
            TestingPkg::Feature => "feature".into(),
            TestingPkg::Matches => "matches".into(),
            TestingPkg::TypeOf => "type_of".into(),
        }
    }

    fn get_package_name(&self) -> String {
        "testing".into()
    }

    fn get_description(&self) -> String {
        match self {
            TestingPkg::Assert => "Evaluates an assertion returning true or an error".into(),
            TestingPkg::Feature => "Creates a new test feature".into(),
            TestingPkg::Matches => "Compares two values".into(),
            TestingPkg::TypeOf => "Returns the type of a value".into(),
        }
    }

    fn get_examples(&self) -> Vec<String> {
        match self {
            // testing
            TestingPkg::Assert => vec![strip_margin(
                r#"
                    |use testing
                    |assert(matches(
                    |   [ 1 "a" "b" "c" ],
                    |   [ 1 "a" "b" "c" ]
                    |))
                "#,
                '|',
            )],
            TestingPkg::Feature => vec![strip_margin(
                r#"
                    |use testing
                    |feature("Matches function", {
                    |    "Compare Array contents: Equal": ctx -> {
                    |        assert(matches(
                    |            [ 1 "a" "b" "c" ],
                    |            [ 1 "a" "b" "c" ]))
                    |    },
                    |    "Compare Array contents: Not Equal": ctx -> {
                    |        assert(!matches(
                    |            [ 1 "a" "b" "c" ],
                    |            [ 0 "x" "y" "z" ]))
                    |    },
                    |    "Compare JSON contents (in sequence)": ctx -> {
                    |        assert(matches(
                    |            { first: "Tom" last: "Lane" },
                    |            { first: "Tom" last: "Lane" }))
                    |    },
                    |    "Compare JSON contents (out of sequence)": ctx -> {
                    |        assert(matches(
                    |            { scores: [82 78 99], id: "A1537" },
                    |            { id: "A1537", scores: [82 78 99] }))
                    |    }
                    })"#,
                '|',
            )],
            TestingPkg::Matches => vec![strip_margin(
                r#"
                    |use testing::matches
                    |a = { scores: [82, 78, 99], first: "Tom", last: "Lane" }
                    |b = { last: "Lane", first: "Tom", scores: [82, 78, 99] }
                    |matches(a, b)
                "#,
                '|',
            )],
            TestingPkg::TypeOf => vec!["testing::type_of([12, 76, 444])".into()],
        }
    }

    fn get_parameter_types(&self) -> Vec<DataType> {
        match self {
            // single-parameter (boolean)
            TestingPkg::Assert => vec![BooleanType],
            // single-parameter (dynamic)
            TestingPkg::TypeOf => vec![UnresolvedType],
            // two-parameter (lazy, lazy)
            TestingPkg::Matches => {
                vec![UnresolvedType, UnresolvedType]
            }
            // two-parameter (string, struct)
            TestingPkg::Feature => vec![StringType(0), StructureType(vec![])],
        }
    }

    fn get_return_type(&self) -> DataType {
        match self {
            TestingPkg::Matches => BooleanType,
            // outcome
            TestingPkg::Assert => BooleanType,
            // string
            TestingPkg::TypeOf => StringType(0),
            // table
            TestingPkg::Feature => TableType(UtilsPkg::get_testing_feature_parameters(), 0),
        }
    }

    fn evaluate(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match self {
            TestingPkg::Assert => extract_value_fn1(ms, args, Self::do_testing_assert),
            TestingPkg::Feature => extract_value_fn2(ms, args, Self::do_testing_feature),
            TestingPkg::Matches => extract_value_fn2(ms, args, Self::do_testing_matches),
            TestingPkg::TypeOf => extract_value_fn1(ms, args, Self::do_testing_type_of),
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
    Journal,
    Len,
    Map,
    Pop,
    Push,
    Replay,
    Reverse,
    RowId,
    Scan,
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
    /// ex: util::fetch(stocks, 5)
    /// ex: stocks:::fetch(5)
    fn do_tools_fetch(
        ms: Machine,
        table: &TypedValue,
        row_offset: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let offset = row_offset.to_usize();
        let df = table.to_dataframe()?;
        let columns = df.get_columns();
        let (row, _) = df.read_row(offset)?;
        Ok((
            ms,
            TableValue(Model(ModelRowCollection::from_columns_and_rows(
                columns,
                &vec![row],
            ))),
        ))
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
                    z => throw(TypeMismatch(TypeMismatchErrors::BooleanExpected(
                        z.to_code(),
                    ))),
                };

                // apply the function to every element in the array
                match items.to_sequence()? {
                    TheArray(array) => {
                        PackageOps::apply_fn_over_array(ms, &array, function, filter)
                    }
                    TheDataframe(df) => PackageOps::apply_fn_over_table(ms, &df, function, filter),
                    TheRange(..) => Self::do_tools_filter(ms, &items.to_array(), function),
                    TheTuple(..) => throw(TypeMismatch(SequenceExpected(items.get_type()))),
                }
            }
            z => throw(TypeMismatch(TypeMismatchErrors::FunctionExpected(
                z.to_code(),
            ))),
        }
    }

    fn do_tools_journal(ms: Machine, value: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        match value.to_dataframe()? {
            EventSource(mut df) => Ok((ms, TableValue(df.get_journal()))),
            TableFn(mut df) => Ok((ms, TableValue(df.get_journal()))),
            _ => throw(TypeMismatch(UnsupportedType(
                TableType(vec![], 0),
                value.get_type(),
            ))),
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

    pub(crate) fn do_tools_map(
        ms: Machine,
        items: &TypedValue,
        function: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match function {
            Function { .. } => match items.to_sequence()? {
                TheArray(array) => {
                    PackageOps::apply_fn_over_array(ms, &array, function, |item, result| {
                        Ok(Some(result))
                    })
                }
                TheDataframe(df) => {
                    PackageOps::apply_fn_over_table(ms, &df, function, |item, result| {
                        Ok(Some(result))
                    })
                }
                TheRange(..) => Self::do_tools_map(ms, &items.to_array(), function),
                TheTuple(..) => throw(TypeMismatch(SequenceExpected(items.get_type()))),
            },
            z => throw(TypeMismatch(TypeMismatchErrors::FunctionExpected(
                z.to_code(),
            ))),
        }
    }

    pub(crate) fn do_tools_pop(
        ms: Machine,
        value: &TypedValue,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match value.to_sequence()? {
            TheDataframe(mut df) => df
                .pop_row(df.get_parameters())
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
                    TheDataframe(mut df) => df.push_row(Row::new(0, vv)),
                    TheArray(mut arr) => arr.push(TupleValue(vv)),
                    TheRange(..) => return throw(UnsupportedFeature("Range::push()".into())),
                    TheTuple(mut tpl) => {
                        tpl.push(TupleValue(vv));
                        TupleValue(tpl)
                    }
                };
                Ok((ms, result))
            }
            Sequenced(source) => {
                let seq = seq_like.to_sequence()?;
                let result = match seq {
                    TheDataframe(mut df) => df.push_row(Row::new(0, source.get_values())),
                    TheArray(mut arr) => arr.push(Sequenced(source)),
                    TheRange(..) => return throw(UnsupportedFeature("Range::pop()".into())),
                    TheTuple(mut tpl) => {
                        tpl.push(Sequenced(source));
                        TupleValue(tpl)
                    }
                };
                Ok((ms, result))
            }
            Structured(structure) => {
                let seq = seq_like.to_sequence()?;
                let result = match seq {
                    TheDataframe(mut df) => df.push_row(structure.to_row()),
                    TheArray(mut arr) => arr.push(Structured(structure)),
                    TheRange(..) => return throw(UnsupportedFeature("Range::push()".into())),
                    TheTuple(mut tpl) => {
                        tpl.push(Structured(structure));
                        TupleValue(tpl)
                    }
                };
                Ok((ms, result))
            }
            z => throw(TypeMismatch(StructExpected(z.get_type_name(), z.to_code()))),
        }
    }

    fn do_tools_replay(ms: Machine, value: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        match value.to_dataframe()? {
            EventSource(mut df) => Ok((ms, df.replay()?)),
            TableFn(mut df) => Ok((ms, df.replay()?)),
            _ => throw(TypeMismatch(UnsupportedType(
                TableType(vec![], 0),
                value.get_type(),
            ))),
        }
    }

    fn do_tools_reverse(ms: Machine, value: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        match value {
            ArrayValue(a) => Ok((ms, ArrayValue(a.rev()))),
            NamespaceValue(ns) => Ok((ms, ns.load_table()?.reverse_table_value())),
            StringValue(s) => Ok((ms, StringValue(s.chars().rev().collect()))),
            TableValue(df) => Ok((ms, df.reverse_table_value())),
            other => throw(TypeMismatch(UnsupportedType(
                UnresolvedType,
                other.get_type(),
            ))),
        }
    }

    fn do_tools_row_id(ms: Machine) -> std::io::Result<(Machine, TypedValue)> {
        let result = ms
            .get(machine::ROW_ID)
            .unwrap_or_else(|| Number(U64Value(0)));
        Ok((ms, result))
    }

    fn do_tools_scan(ms: Machine, tv_table: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        let df = tv_table.to_dataframe()?;
        let rows = df.examine_rows()?;
        let columns = rows
            .first()
            .map(|row| df.get_columns().to_owned())
            .unwrap_or(Vec::new());
        let mrc = ModelRowCollection::from_columns_and_rows(&columns, &rows);
        Ok((ms, TableValue(Model(mrc))))
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
            DataFormats::CSV => Ok((
                ms,
                ArrayValue(Array::from(
                    rc.iter()
                        .map(|row| StringValue(row.to_csv()))
                        .collect::<Vec<_>>(),
                )),
            )),
            DataFormats::JSON => Ok((
                ms,
                ArrayValue(Array::from(
                    rc.iter()
                        .map(|row| row.to_json_string(rc.get_columns()))
                        .map(StringValue)
                        .collect::<Vec<_>>(),
                )),
            )),
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
        Ok((ms, TableValue(Model(mrc))))
    }

    pub(crate) fn get_contents() -> Vec<PackageOps> {
        vec![
            PackageOps::Tools(ToolsPkg::Compact),
            PackageOps::Tools(ToolsPkg::Describe),
            PackageOps::Tools(ToolsPkg::Fetch),
            PackageOps::Tools(ToolsPkg::Filter),
            PackageOps::Tools(ToolsPkg::Journal),
            PackageOps::Tools(ToolsPkg::Len),
            PackageOps::Tools(ToolsPkg::Map),
            PackageOps::Tools(ToolsPkg::Pop),
            PackageOps::Tools(ToolsPkg::Push),
            PackageOps::Tools(ToolsPkg::Replay),
            PackageOps::Tools(ToolsPkg::Reverse),
            PackageOps::Tools(ToolsPkg::RowId),
            PackageOps::Tools(ToolsPkg::Scan),
            PackageOps::Tools(ToolsPkg::ToArray),
            PackageOps::Tools(ToolsPkg::ToCSV),
            PackageOps::Tools(ToolsPkg::ToJSON),
            PackageOps::Tools(ToolsPkg::ToTable),
        ]
    }    
    
    pub fn get_tools_describe_parameters() -> Vec<Parameter> {
        vec![
            Parameter::new("name", StringType(128)),
            Parameter::new("type", StringType(128)),
            Parameter::new("default_value", StringType(128)),
            Parameter::new("is_nullable", BooleanType),
        ]
    }
}

impl Package for ToolsPkg {
    fn get_name(&self) -> String {
        match self {
            ToolsPkg::Compact => "compact".into(),
            ToolsPkg::Describe => "describe".into(),
            ToolsPkg::Fetch => "fetch".into(),
            ToolsPkg::Filter => "filter".into(),
            ToolsPkg::Journal => "journal".into(),
            ToolsPkg::Len => "len".into(),
            ToolsPkg::Map => "map".into(),
            ToolsPkg::Pop => "pop".into(),
            ToolsPkg::Push => "push".into(),
            ToolsPkg::Replay => "replay".into(),
            ToolsPkg::Reverse => "reverse".into(),
            ToolsPkg::RowId => "row_id".into(),
            ToolsPkg::Scan => "scan".into(),
            ToolsPkg::ToArray => "to_array".into(),
            ToolsPkg::ToCSV => "to_csv".into(),
            ToolsPkg::ToJSON => "to_json".into(),
            ToolsPkg::ToTable => "to_table".into(),
        }
    }

    fn get_package_name(&self) -> String {
        "tools".into()
    }

    fn get_description(&self) -> String {
        match self {
            ToolsPkg::Compact => "Shrinks a table by removing deleted rows".into(),
            ToolsPkg::Describe => "Describes a table or structure".into(),
            ToolsPkg::Fetch => "Retrieves a raw structure from a table".into(),
            ToolsPkg::Filter => "Filters a collection based on a function".into(),
            ToolsPkg::Journal => {
                "Retrieves the journal for an event-source or table function".into()
            }
            ToolsPkg::Len => "Returns the length of a table".into(),
            ToolsPkg::Map => "Transform a collection based on a function".into(),
            ToolsPkg::Pop => "Removes and returns a value or object from a Sequence".into(),
            ToolsPkg::Push => "Appends a value or object to a Sequence".into(),
            ToolsPkg::Replay => "Reconstructs the state of a journaled table".into(),
            ToolsPkg::Reverse => "Returns a reverse copy of a table, string or array".into(),
            ToolsPkg::RowId => "Returns the unique ID for the last retrieved row".into(),
            ToolsPkg::Scan => "Returns existence metadata for a table".into(),
            ToolsPkg::ToArray => "Converts a collection into an array".into(),
            ToolsPkg::ToCSV => "Converts a collection to CSV format".into(),
            ToolsPkg::ToJSON => "Converts a collection to JSON format".into(),
            ToolsPkg::ToTable => "Converts an object into a to_table".into(),
        }
    }

    fn get_examples(&self) -> Vec<String> {
        match self {
            // tools
            ToolsPkg::Compact => vec![strip_margin(
                r#"
                    |stocks = ns("examples.compact.stocks")
                    |table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                    |[{ symbol: "DMX", exchange: "NYSE", last_sale: 99.99 },
                    | { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                    | { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                    | { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                    | { symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
                    | { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 },
                    | { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
                    |delete from stocks where last_sale > 1.0
                    |from stocks
                "#,
                '|',
            )],
            ToolsPkg::Describe => vec![strip_margin(
                r#"
                    |tools::describe({
                    |   symbol: "BIZ",
                    |   exchange: "NYSE",
                    |   last_sale: 23.66
                    |})
                "#,
                '|',
            )],
            ToolsPkg::Fetch => vec![strip_margin(
                r#"
                    |stocks = ns("examples.fetch.stocks")
                    |table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                    |[{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                    | { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                    | { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
                    |tools::fetch(stocks, 2)
                "#,
                '|',
            )],
            ToolsPkg::Filter => vec![strip_margin(
                r#"
                    |tools::filter(1..11, n -> (n % 2) == 0)
                "#,
                '|',
            )],
            ToolsPkg::Journal => vec![strip_margin(
                r#"
                    |use tools
                    |stocks = ns("examples.journal.stocks")
                    |drop table stocks
                    |create table stocks fn(
                    |   symbol: String(8), exchange: String(8), last_sale: f64
                    |) => {
                    |    symbol: symbol,
                    |    exchange: exchange,
                    |    last_sale: last_sale,
                    |    ingest_time: cal::now()
                    |}
                    |[{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                    | { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                    | { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
                    |stocks:::journal()
                "#,
                '|',
            )],
            ToolsPkg::Len => vec![strip_margin(
                r#"
                    |stocks = ns("examples.table_len.stocks")
                    |table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                    |[{ symbol: "WKRP", exchange: "NYSE", last_sale: 11.11 },
                    | { symbol: "ACDC", exchange: "AMEX", last_sale: 35.11 },
                    | { symbol: "UELO", exchange: "NYSE", last_sale: 90.12 }] ~> stocks
                    |tools::len(stocks)
                "#,
                '|',
            )],
            ToolsPkg::Map => vec![strip_margin(
                r#"
                    |stocks = ns("examples.map_over_table.stocks")
                    |table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                    |[{ symbol: "WKRP", exchange: "NYSE", last_sale: 11.11 },
                    | { symbol: "ACDC", exchange: "AMEX", last_sale: 35.11 },
                    | { symbol: "UELO", exchange: "NYSE", last_sale: 90.12 }] ~> stocks
                    |use tools
                    |stocks:::map(fn(row) => {
                    |    symbol: symbol,
                    |    exchange: exchange,
                    |    last_sale: last_sale,
                    |    processed_time: cal::now()
                    |})
                "#,
                '|',
            )],
            ToolsPkg::Pop => vec![strip_margin(
                r#"
                    |use tools
                    |stocks = ns("examples.tools_pop.stocks")
                    |table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                    |[{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                    | { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                    | { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
                    |stocks:::pop()
                "#,
                '|',
            )],
            ToolsPkg::Push => vec![strip_margin(
                r#"
                    |use tools
                    |stocks = ns("examples.push.stocks")
                    |table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                    |[{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                    | { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                    | { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
                    |stocks:::push({ symbol: "XYZ", exchange: "NASDAQ", last_sale: 24.78 })
                    |stocks
                "#,
                '|',
            )],
            ToolsPkg::Replay => vec![strip_margin(
                r#"
                    |use tools
                    |stocks = ns("examples.table_fn.stocks")
                    |drop table stocks
                    |create table stocks fn(
                    |   symbol: String(8), exchange: String(8), last_sale: f64
                    |) => {
                    |    symbol: symbol,
                    |    exchange: exchange,
                    |    last_sale: last_sale * 2.0,
                    |    rank: __row_id__ + 1
                    |}
                    |[{ symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                    | { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                    | { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
                    |stocks:::replay()
                "#,
                '|',
            )],
            ToolsPkg::Reverse => vec![strip_margin(
                r#"
                    |use tools
                    |to_table(reverse(
                    |   ['cat', 'dog', 'ferret', 'mouse']
                    |))
                "#,
                '|',
            )],
            ToolsPkg::RowId => vec!["tools::row_id()".into()],
            ToolsPkg::Scan => vec![strip_margin(
                r#"
                    |use tools
                    |stocks = ns("examples.scan.stocks")
                    |table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                    |[{ symbol: "ABC", exchange: "AMEX", last_sale: 12.33 },
                    | { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                    | { symbol: "BIZ", exchange: "NYSE", last_sale: 9.775 },
                    | { symbol: "GOTO", exchange: "OTC", last_sale: 0.1442 },
                    | { symbol: "XYZ", exchange: "NYSE", last_sale: 0.0289 }] ~> stocks
                    |delete from stocks where last_sale > 1.0
                    |stocks:::scan()
                "#,
                '|',
            )],
            ToolsPkg::ToArray => vec![r#"tools::to_array("Hello")"#.into()],
            ToolsPkg::ToCSV => vec![strip_margin(
                r#"
                    |use tools::to_csv
                    |stocks = ns("examples.csv.stocks")
                    |table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                    |[{ symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
                    | { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                    | { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                    | { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                    | { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }] ~> stocks
                    |stocks:::to_csv()
                "#,
                '|',
            )],
            ToolsPkg::ToJSON => vec![strip_margin(
                r#"
                    |use tools::to_json
                    |stocks = ns("examples.json.stocks")
                    |table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                    |[{ symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
                    | { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                    | { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                    | { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                    | { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }] ~> stocks
                    |stocks:::to_json()
                "#,
                '|',
            )],
            ToolsPkg::ToTable => vec![strip_margin(
                r#"
                    |tools::to_table(['cat', 'dog', 'ferret', 'mouse'])
                "#,
                '|',
            )],
        }
    }

    fn get_parameter_types(&self) -> Vec<DataType> {
        match self {
            // single-parameter (table)
            ToolsPkg::Compact
            | ToolsPkg::Describe
            | ToolsPkg::Len
            | ToolsPkg::Journal
            | ToolsPkg::Pop
            | ToolsPkg::Replay
            | ToolsPkg::Reverse
            | ToolsPkg::Scan
            | ToolsPkg::ToArray
            | ToolsPkg::ToCSV
            | ToolsPkg::ToJSON => vec![TableType(Vec::new(), 0)],
            ToolsPkg::Fetch => vec![TableType(vec![], 0), NumberType(U64Kind)],
            ToolsPkg::Filter | ToolsPkg::Map | ToolsPkg::Push => {
                vec![UnresolvedType, UnresolvedType]
            }
            ToolsPkg::RowId => vec![],
            ToolsPkg::ToTable => vec![UnresolvedType],
        }
    }

    fn get_return_type(&self) -> DataType {
        match self {
            ToolsPkg::Compact
            | ToolsPkg::Fetch
            | ToolsPkg::Journal
            | ToolsPkg::Reverse
            | ToolsPkg::Scan
            | ToolsPkg::ToTable => TableType(Vec::new(), 0),
            ToolsPkg::Describe => TableType(ToolsPkg::get_tools_describe_parameters(), 0),
            ToolsPkg::Len => NumberType(I64Kind),
            ToolsPkg::Replay => BooleanType,
            ToolsPkg::RowId => NumberType(I64Kind),
            ToolsPkg::ToCSV
            | ToolsPkg::ToJSON
            | ToolsPkg::Filter
            | ToolsPkg::Map
            | ToolsPkg::ToArray => TableType(vec![], 0),
            // row|structure
            ToolsPkg::Pop => StructureType(vec![]),
            ToolsPkg::Push => BooleanType,
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
            ToolsPkg::Journal => extract_value_fn1(ms, args, Self::do_tools_journal),
            ToolsPkg::Len => extract_value_fn1(ms, args, Self::do_tools_length),
            ToolsPkg::Map => extract_value_fn2(ms, args, Self::do_tools_map),
            ToolsPkg::Pop => extract_value_fn1(ms, args, Self::do_tools_pop),
            ToolsPkg::Push => Self::do_tools_push(ms, args),
            ToolsPkg::Replay => extract_value_fn1(ms, args, Self::do_tools_replay),
            ToolsPkg::Reverse => extract_value_fn1(ms, args, Self::do_tools_reverse),
            ToolsPkg::RowId => extract_value_fn0(ms, args, Self::do_tools_row_id),
            ToolsPkg::Scan => extract_value_fn1(ms, args, Self::do_tools_scan),
            ToolsPkg::ToArray => extract_array_fn1(ms, args, |a| ArrayValue(a)),
            ToolsPkg::ToCSV => extract_value_fn1(ms, args, Self::do_tools_to_csv),
            ToolsPkg::ToJSON => extract_value_fn1(ms, args, Self::do_tools_to_json),
            ToolsPkg::ToTable => extract_value_fn1(ms, args, Self::do_tools_to_table),
        }
    }
}

/// Utils package
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum UtilsPkg {
    Base64,
    Binary,
    Gzip,
    Gunzip,
    Hex,
    MD5,
    ToASCII,
    ToDate,
    ToF32,
    ToF64,
    ToI8,
    ToI16,
    ToI32,
    ToI64,
    ToI128,
    ToU8,
    ToU16,
    ToU32,
    ToU64,
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

    fn do_util_base64(ms: Machine, a: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        Ok((ms, StringValue(base64::encode(a.to_bytes()))))
    }

    fn do_util_binary(ms: Machine, a: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        Ok((ms, StringValue(format!("{:b}", a.to_u64()))))
    }

    fn do_util_gzip(ms: Machine, a: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        use flate2::write::GzEncoder;
        use flate2::Compression;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(a.to_bytes().as_slice())?;
        Ok((ms, Binary(encoder.finish()?.to_vec())))
    }

    fn do_util_gunzip(ms: Machine, a: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        use flate2::read::GzDecoder;
        let bytes = a.to_bytes();
        let mut decoder = GzDecoder::new(bytes.as_slice());
        let mut output = Vec::new();
        decoder.read_to_end(&mut output)?;
        Ok((ms, Binary(output)))
    }

    fn do_util_numeric_conv(
        ms: Machine,
        value: &TypedValue,
        plat: &UtilsPkg,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let result = match plat {
            UtilsPkg::ToDate => Number(DateValue(value.to_i64())),
            UtilsPkg::ToF32 => Number(F32Value(value.to_f32())),
            UtilsPkg::ToF64 => Number(F64Value(value.to_f64())),
            UtilsPkg::ToI8 => Number(I8Value(value.to_i8())),
            UtilsPkg::ToI16 => Number(I16Value(value.to_i16())),
            UtilsPkg::ToI32 => Number(I32Value(value.to_i32())),
            UtilsPkg::ToI64 => Number(I64Value(value.to_i64())),
            UtilsPkg::ToI128 => Number(I128Value(value.to_i128())),
            UtilsPkg::ToU8 => Number(U8Value(value.to_u8())),
            UtilsPkg::ToU16 => Number(U16Value(value.to_u16())),
            UtilsPkg::ToU32 => Number(U32Value(value.to_u32())),
            UtilsPkg::ToU64 => Number(U64Value(value.to_u64())),
            UtilsPkg::ToU128 => Number(U128Value(value.to_u128())),
            plat => return throw(UnsupportedPlatformOps(PackageOps::Utils(plat.to_owned()))),
        };
        Ok((ms, result))
    }

    fn do_util_md5(ms: Machine, value: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        match md5::compute(value.to_bytes()) {
            md5::Digest(bytes) => Ok((ms, Binary(bytes.to_vec()))),
        }
    }

    fn do_util_to_ascii(ms: Machine, value: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        Ok((ms, StringValue(format!("{}", value.to_u8() as char))))
    }

    fn do_util_to_hex(ms: Machine, value: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        Ok((
            ms,
            StringValue(format!("{}", StringValue(hex::encode(value.to_bytes())))),
        ))
    }

    pub(crate) fn do_util_uuid(
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match args.is_empty() {
            false => throw(TypeMismatch(ArgumentsMismatched(0, args.len()))),
            true => Ok((ms, Number(UUIDValue(Uuid::new_v4().as_u128())))),
        }
    }

    pub(crate) fn get_contents() -> Vec<PackageOps> {
        vec![
            PackageOps::Utils(UtilsPkg::Base64),
            PackageOps::Utils(UtilsPkg::Binary),
            PackageOps::Utils(UtilsPkg::Gzip),
            PackageOps::Utils(UtilsPkg::Gunzip),
            PackageOps::Utils(UtilsPkg::Hex),
            PackageOps::Utils(UtilsPkg::MD5),
            PackageOps::Utils(UtilsPkg::ToASCII),
            PackageOps::Utils(UtilsPkg::ToDate),
            PackageOps::Utils(UtilsPkg::ToF32),
            PackageOps::Utils(UtilsPkg::ToF64),
            PackageOps::Utils(UtilsPkg::ToI8),
            PackageOps::Utils(UtilsPkg::ToI16),
            PackageOps::Utils(UtilsPkg::ToI32),
            PackageOps::Utils(UtilsPkg::ToI64),
            PackageOps::Utils(UtilsPkg::ToI128),
            PackageOps::Utils(UtilsPkg::ToU8),
            PackageOps::Utils(UtilsPkg::ToU16),
            PackageOps::Utils(UtilsPkg::ToU32),
            PackageOps::Utils(UtilsPkg::ToU64),
            PackageOps::Utils(UtilsPkg::ToU128),
        ]
    }

    pub fn get_testing_feature_parameters() -> Vec<Parameter> {
        vec![
            Parameter::new("level", NumberType(U16Kind)),
            Parameter::new("item", StringType(256)),
            Parameter::new("passed", BooleanType),
            Parameter::new("result", StringType(256)),
        ]
    }
}

impl Package for UtilsPkg {
    fn get_name(&self) -> String {
        match self {
            UtilsPkg::Base64 => "base64".into(),
            UtilsPkg::Binary => "to_binary".into(),
            UtilsPkg::Gzip => "gzip".into(),
            UtilsPkg::Gunzip => "gunzip".into(),
            UtilsPkg::Hex => "hex".into(),
            UtilsPkg::MD5 => "md5".into(),
            UtilsPkg::ToASCII => "to_ascii".into(),
            UtilsPkg::ToDate => "to_date".into(),
            UtilsPkg::ToF32 => "to_f32".into(),
            UtilsPkg::ToF64 => "to_f64".into(),
            UtilsPkg::ToI8 => "to_i8".into(),
            UtilsPkg::ToI16 => "to_i16".into(),
            UtilsPkg::ToI32 => "to_i32".into(),
            UtilsPkg::ToI64 => "to_i64".into(),
            UtilsPkg::ToI128 => "to_i128".into(),
            UtilsPkg::ToU8 => "to_u8".into(),
            UtilsPkg::ToU16 => "to_u16".into(),
            UtilsPkg::ToU32 => "to_u32".into(),
            UtilsPkg::ToU64 => "to_u64".into(),
            UtilsPkg::ToU128 => "to_u128".into(),
        }
    }

    fn get_package_name(&self) -> String {
        "util".into()
    }

    fn get_description(&self) -> String {
        match self {
            UtilsPkg::Base64 => "Translates bytes into Base 64".into(),
            UtilsPkg::Binary => "Translates a numeric value into binary".into(),
            UtilsPkg::Gzip => "Compresses bytes via gzip".into(),
            UtilsPkg::Gunzip => "Decompresses bytes via gzip".into(),
            UtilsPkg::Hex => "Translates bytes into hexadecimal".into(),
            UtilsPkg::MD5 => "Creates a MD5 digest".into(),
            UtilsPkg::ToASCII => "Converts an integer to ASCII".into(),
            UtilsPkg::ToDate => "Converts a value to Date".into(),
            UtilsPkg::ToF32 => "Converts a value to f32".into(),
            UtilsPkg::ToF64 => "Converts a value to f64".into(),
            UtilsPkg::ToI8 => "Converts a value to i8".into(),
            UtilsPkg::ToI16 => "Converts a value to i16".into(),
            UtilsPkg::ToI32 => "Converts a value to i32".into(),
            UtilsPkg::ToI64 => "Converts a value to i64".into(),
            UtilsPkg::ToI128 => "Converts a value to i128".into(),
            UtilsPkg::ToU8 => "Converts a value to u8".into(),
            UtilsPkg::ToU16 => "Converts a value to u16".into(),
            UtilsPkg::ToU32 => "Converts a value to u32".into(),
            UtilsPkg::ToU64 => "Converts a value to u64".into(),
            UtilsPkg::ToU128 => "Converts a value to u128".into(),
        }
    }

    fn get_examples(&self) -> Vec<String> {
        match self {
            UtilsPkg::Base64 => vec!["util::base64('Hello World')".into()],
            UtilsPkg::Binary => vec!["util::to_binary(0b1011 & 0b1101)".into()],
            UtilsPkg::Gzip => vec!["util::gzip('Hello World')".into()],
            UtilsPkg::Gunzip => vec!["util::gunzip(util::gzip('Hello World'))".into()],
            UtilsPkg::Hex => vec!["util::hex('Hello World')".into()],
            UtilsPkg::MD5 => vec!["util::md5('Hello World')".into()],
            UtilsPkg::ToASCII => vec!["util::to_ascii(177)".into()],
            UtilsPkg::ToDate => vec!["util::to_date(177)".into()],
            UtilsPkg::ToF32 => vec!["util::to_f32(4321)".into()],
            UtilsPkg::ToF64 => vec!["util::to_f64(4321)".into()],
            UtilsPkg::ToI8 => vec!["util::to_i8(88)".into()],
            UtilsPkg::ToI16 => vec!["util::to_i16(88)".into()],
            UtilsPkg::ToI32 => vec!["util::to_i32(88)".into()],
            UtilsPkg::ToI64 => vec!["util::to_i64(88)".into()],
            UtilsPkg::ToI128 => vec!["util::to_i128(88)".into()],
            UtilsPkg::ToU8 => vec!["util::to_u8(88)".into()],
            UtilsPkg::ToU16 => vec!["util::to_u16(88)".into()],
            UtilsPkg::ToU32 => vec!["util::to_u32(88)".into()],
            UtilsPkg::ToU64 => vec!["util::to_u64(88)".into()],
            UtilsPkg::ToU128 => vec!["util::to_u128(88)".into()],
        }
    }

    fn get_parameter_types(&self) -> Vec<DataType> {
        match self {
            // single-parameter (u32)
            UtilsPkg::ToASCII => vec![NumberType(U32Kind)],
            _ => vec![UnresolvedType],
        }
    }

    fn get_return_type(&self) -> DataType {
        match self {
            // bytes
            UtilsPkg::Gzip | UtilsPkg::Gunzip => BinaryType(0),
            UtilsPkg::MD5 => BinaryType(16),
            // date
            UtilsPkg::ToDate => NumberType(DateKind),
            UtilsPkg::ToF32 => NumberType(F32Kind),
            UtilsPkg::ToF64 => NumberType(F64Kind),
            UtilsPkg::ToI8 => NumberType(I8Kind),
            UtilsPkg::ToI16 => NumberType(I16Kind),
            UtilsPkg::ToI32 => NumberType(I32Kind),
            UtilsPkg::ToI64 => NumberType(I64Kind),
            UtilsPkg::ToI128 => NumberType(I128Kind),
            UtilsPkg::ToU8 => NumberType(U8Kind),
            UtilsPkg::ToU16 => NumberType(U16Kind),
            UtilsPkg::ToU32 => NumberType(U32Kind),
            UtilsPkg::ToU64 => NumberType(U64Kind),
            UtilsPkg::ToU128 => NumberType(U128Kind),
            UtilsPkg::Base64 | UtilsPkg::Binary | UtilsPkg::ToASCII | UtilsPkg::Hex => {
                StringType(0)
            }
        }
    }

    fn evaluate(
        &self,
        ms: Machine,
        args: Vec<TypedValue>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match self {
            UtilsPkg::Base64 => extract_value_fn1(ms, args, Self::do_util_base64),
            UtilsPkg::Binary => extract_value_fn1(ms, args, Self::do_util_binary),
            UtilsPkg::Gzip => extract_value_fn1(ms, args, Self::do_util_gzip),
            UtilsPkg::Gunzip => extract_value_fn1(ms, args, Self::do_util_gunzip),
            UtilsPkg::MD5 => extract_value_fn1(ms, args, Self::do_util_md5),
            UtilsPkg::ToASCII => extract_value_fn1(ms, args, Self::do_util_to_ascii),
            UtilsPkg::Hex => extract_value_fn1(ms, args, Self::do_util_to_hex),
            UtilsPkg::ToDate => self.adapter_pf_fn1(ms, args, Self::do_util_numeric_conv),
            UtilsPkg::ToF32 => self.adapter_pf_fn1(ms, args, Self::do_util_numeric_conv),
            UtilsPkg::ToF64 => self.adapter_pf_fn1(ms, args, Self::do_util_numeric_conv),
            UtilsPkg::ToI8 => self.adapter_pf_fn1(ms, args, Self::do_util_numeric_conv),
            UtilsPkg::ToI16 => self.adapter_pf_fn1(ms, args, Self::do_util_numeric_conv),
            UtilsPkg::ToI32 => self.adapter_pf_fn1(ms, args, Self::do_util_numeric_conv),
            UtilsPkg::ToI64 => self.adapter_pf_fn1(ms, args, Self::do_util_numeric_conv),
            UtilsPkg::ToI128 => self.adapter_pf_fn1(ms, args, Self::do_util_numeric_conv),
            UtilsPkg::ToU8 => self.adapter_pf_fn1(ms, args, Self::do_util_numeric_conv),
            UtilsPkg::ToU16 => self.adapter_pf_fn1(ms, args, Self::do_util_numeric_conv),
            UtilsPkg::ToU32 => self.adapter_pf_fn1(ms, args, Self::do_util_numeric_conv),
            UtilsPkg::ToU64 => self.adapter_pf_fn1(ms, args, Self::do_util_numeric_conv),
            UtilsPkg::ToU128 => self.adapter_pf_fn1(ms, args, Self::do_util_numeric_conv),
        }
    }
}

/// WWW package
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum WwwPkg {
    Serve,
    URLDecode,
    URLEncode,
}

impl WwwPkg {
    fn do_www_serve(ms: Machine, port: &TypedValue) -> std::io::Result<(Machine, TypedValue)> {
        oxide_server::start_http_server(port.to_u16());
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

    pub(crate) fn get_contents() -> Vec<PackageOps> {
        vec![
            PackageOps::Www(WwwPkg::URLDecode),
            PackageOps::Www(WwwPkg::URLEncode),
            PackageOps::Www(WwwPkg::Serve),
        ]
    }
}

impl Package for WwwPkg {
    fn get_name(&self) -> String {
        match self {
            WwwPkg::Serve => "serve".into(),
            WwwPkg::URLDecode => "url_decode".into(),
            WwwPkg::URLEncode => "url_encode".into(),
        }
    }

    fn get_package_name(&self) -> String {
        "www".into()
    }

    fn get_description(&self) -> String {
        match self {
            WwwPkg::Serve => "Starts a local HTTP service".into(),
            WwwPkg::URLDecode => "Decodes a URL-encoded string".into(),
            WwwPkg::URLEncode => "Encodes a URL string".into(),
        }
    }

    fn get_examples(&self) -> Vec<String> {
        match self {
            WwwPkg::Serve => vec![strip_margin(
                r#"
                    |www::serve(8822)
                    |stocks = ns("examples.www.quotes")
                    |table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                    |[{ symbol: "XINU", exchange: "NYSE", last_sale: 8.11 },
                    | { symbol: "BOX", exchange: "NYSE", last_sale: 56.88 },
                    | { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 },
                    | { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                    | { symbol: "MIU", exchange: "OTCBB", last_sale: 2.24 }] ~> stocks
                    |GET http://localhost:8822/examples/www/quotes/1/4
                "#,
                '|',
            )],
            WwwPkg::URLDecode => vec![
                "www::url_decode('http%3A%2F%2Fshocktrade.com%3Fname%3Dthe%20hero%26t%3D9998')"
                    .into(),
            ],
            WwwPkg::URLEncode => {
                vec!["www::url_encode('http://shocktrade.com?name=the hero&t=9998')".into()]
            }
        }
    }

    fn get_parameter_types(&self) -> Vec<DataType> {
        match self {
            WwwPkg::Serve => vec![NumberType(U32Kind)],
            WwwPkg::URLDecode | WwwPkg::URLEncode => vec![StringType(0)],
        }
    }

    fn get_return_type(&self) -> DataType {
        match self {
            WwwPkg::Serve => BooleanType,
            WwwPkg::URLDecode | WwwPkg::URLEncode => StringType(0),
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
            WwwPkg::Serve => extract_value_fn1(ms, args, WwwPkg::do_www_serve),
        }
    }
}

/// Unit tests
#[cfg(test)]
mod tests {
    use super::*;

    /// Package "array" tests
    #[cfg(test)]
    mod array_tests {
        use crate::testdata::verify_exact_code;

        #[test]
        fn test_arrays_filter() {
            verify_exact_code(
                r#"
                arrays::filter([123, 56, 89, 66], n -> (n % 3) == 0)
           "#,
                "[123, 66]",
            )
        }

        #[test]
        fn test_arrays_filter_with_range() {
            verify_exact_code(
                r#"
                arrays::filter(1..7, n -> (n % 2) == 0)
           "#,
                "[2, 4, 6]",
            )
        }

        #[test]
        fn test_arrays_len() {
            verify_exact_code(
                r#"
                arrays::len([3, 5, 7, 9])
           "#,
                "4",
            )
        }

        #[test]
        fn test_arrays_len_map_with_range() {
            verify_exact_code(
                r#"
                arrays::len(1..5)
           "#,
                "4",
            )
        }

        #[test]
        fn test_arrays_map() {
            verify_exact_code(
                r#"
                arrays::map([1, 2, 3], n -> n * 2)
           "#,
                "[2, 4, 6]",
            )
        }

        #[test]
        fn test_arrays_map_with_range() {
            verify_exact_code(
                r#"
                arrays::map(1..4, n -> n * 2)
           "#,
                "[2, 4, 6]",
            )
        }

        #[test]
        fn test_arrays_pop() {
            verify_exact_code(r#"
                stocks = ["ABC", "BOOM", "JET", "DEX"]
                arrays::pop(stocks)
            "#, r#"(["ABC", "BOOM", "JET"], "DEX")"#);
        }

        #[ignore]
        #[test]
        fn test_arrays_push() {
            verify_exact_code(
                r#"
                stocks = ["ABC", "BOOM", "JET"]
                stocks = arrays::push(stocks, "DEX")
                stocks
            "#,
                r#"["ABC", "BOOM", "JET", "DEX"]"#,
            );
        }

        #[test]
        fn test_arrays_reduce() {
            verify_exact_code(r#"
                 use arrays::reduce
                 numbers = [1, 2, 3, 4, 5]
                 numbers:::reduce(0, (a, b) -> a + b)
            "#, "15");
        }

        #[test]
        fn test_arrays_reduce_with_range() {
            verify_exact_code(r#"
                 arrays::reduce(1..=5, 0, (a, b) -> a + b)
            "#, "15");
        }

        #[test]
        fn test_arrays_reverse() {
            verify_exact_code(r#"
                arrays::reverse(['cat', 'dog', 'ferret', 'mouse'])
            "#, r#"["mouse", "ferret", "dog", "cat"]"#)
        }

        #[test]
        fn test_arrays_reverse_with_range() {
            verify_exact_code(r#"
                arrays::reverse(1..=5)
            "#, r#"[5, 4, 3, 2, 1]"#)
        }

        #[test]
        fn test_arrays_to_array() {
            verify_exact_code(r#"
                 arrays::to_array(("a", "b", "c"))
            "#, r#"["a", "b", "c"]"#);
        }
    }

    /// Package "cal" tests
    #[cfg(test)]
    mod cal_tests {
        use crate::numbers::Numbers::*;
        use crate::testdata::verify_exact_value_where;
        use crate::typed_values::TypedValue::Number;

        #[test]
        fn test_cal_now() {
            verify_exact_value_where(
                r#"
                cal::now()
            "#,
                |n| matches!(n, Number(DateValue(..))),
            );
        }

        #[test]
        fn test_cal_day_of() {
            verify_exact_value_where(
                r#"
                use cal
                now():::day_of()
            "#,
                |n| matches!(n, Number(U32Value(..))),
            );
        }

        #[test]
        fn test_cal_hour24() {
            verify_exact_value_where(
                r#"
                use cal
                now():::hour24()
            "#,
                |n| matches!(n, Number(U32Value(..))),
            );
        }

        #[test]
        fn test_cal_hour12() {
            verify_exact_value_where(
                r#"
                use cal
                now():::hour12()
            "#,
                |n| matches!(n, Number(U32Value(..))),
            );
        }

        #[test]
        fn test_cal_minute_of() {
            verify_exact_value_where(
                r#"
                use cal
                now():::minute_of()
            "#,
                |n| matches!(n, Number(U32Value(..))),
            );
        }

        #[test]
        fn test_cal_month_of() {
            verify_exact_value_where(
                r#"
                use cal
                now():::month_of()
            "#,
                |n| matches!(n, Number(U32Value(..))),
            );
        }

        #[test]
        fn test_cal_second_of() {
            verify_exact_value_where(
                r#"
                use cal
                now():::second_of()
            "#,
                |n| matches!(n, Number(U32Value(..))),
            );
        }

        #[test]
        fn test_cal_year_of() {
            verify_exact_value_where(
                r#"
                use cal
                now():::year_of()
            "#,
                |n| matches!(n, Number(I32Value(..))),
            );
        }

        #[test]
        fn test_cal_minus() {
            verify_exact_value_where(
                r#"
                use cal, durations
                cal::minus(now(), 3:::days())
            "#,
                |n| matches!(n, Number(DateValue(..))),
            );
        }

        #[test]
        fn test_cal_plus() {
            verify_exact_value_where(
                r#"
                use cal, durations
                cal::plus(now(), 30:::days())
            "#,
                |n| matches!(n, Number(DateValue(..))),
            );
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
            verify_exact_value(
                r#"
                use durations
                3:::days()
            "#,
                Number(I64Value(3 * DAYS)),
            );
        }

        #[test]
        fn test_durations_hours() {
            verify_exact_value(
                r#"
                use durations
                8:::hours()
            "#,
                Number(I64Value(8 * HOURS)),
            );
        }

        #[test]
        fn test_durations_hours_f64() {
            verify_exact_value(
                r#"
                use durations
                0.5:::hours()
            "#,
                Number(F64Value(30.0 * MINUTES.to_f64().unwrap())),
            );
        }

        #[test]
        fn test_durations_millis() {
            verify_exact_value(
                r#"
                use durations
                1000:::millis()
            "#,
                Number(I64Value(1 * SECONDS)),
            );
        }

        #[test]
        fn test_durations_minutes() {
            verify_exact_value(
                r#"
                use durations
                30:::minutes()
            "#,
                Number(I64Value(30 * MINUTES)),
            );
        }

        #[test]
        fn test_durations_seconds() {
            verify_exact_value(
                r#"
                use durations
                20:::seconds()
            "#,
                Number(I64Value(20 * SECONDS)),
            );
        }
    }

    /// Package "io" tests
    #[cfg(test)]
    mod io_tests {
        use super::*;
        use crate::platform::PackageOps;
        use crate::testdata::verify_exact_value;
        use crate::typed_values::TypedValue::*;
        use PackageOps::*;

        #[test]
        fn test_io_create_file_qualified() {
            verify_exact_value(
                r#"
                io::create_file("quote.json", { symbol: "TRX", exchange: "NYSE", last_sale: 45.32 })
            "#,
                Number(U64Value(52)),
            );

            verify_exact_value(
                r#"
                io::exists("quote.json")
            "#,
                Boolean(true),
            );
        }

        #[test]
        fn test_io_create_file_postfix() {
            verify_exact_value(
                r#"
                use io
                "quote.json":::create_file({
                    symbol: "TRX",
                    exchange: "NYSE",
                    last_sale: 45.32
                })
            "#,
                Number(U64Value(52)),
            );

            verify_exact_value(
                r#"
                use io
                "quote.json":::exists()
            "#,
                Boolean(true),
            );
        }

        #[test]
        fn test_io_file_exists() {
            verify_exact_value(
                r#"
            use io
            path_str = oxide::home()
            path_str:::exists()
        "#,
                Boolean(true),
            )
        }

        #[test]
        fn test_io_create_and_read_text_file() {
            verify_exact_value(
                r#"
                use io, util
                file = "temp_secret.txt"
                file:::create_file(md5("**keep**this**secret**"))
                file:::read_text_file()
            "#,
                StringValue("47338bd5f35bbb239092c36e30775b4a".into()),
            )
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

    /// Package "os" tests
    #[cfg(test)]
    mod os_tests {
        use super::*;
        use crate::platform::PackageOps;
        use crate::testdata::{
            make_quote_columns, verify_exact_table, verify_exact_value, verify_exact_value_where,
        };
        use crate::typed_values::TypedValue::*;
        use PackageOps::*;

        #[test]
        fn test_os_call() {
            verify_exact_value(
                r#"
                create table ns("platform.os.call") (
                    symbol: String(8),
                    exchange: String(8),
                    last_sale: f64
                )
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
            verify_exact_table(
                r#"
                use str
                cur_dir = os::current_dir()
                prefix = iff(cur_dir:::ends_with("core"), "../..", ".")
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
        use crate::platform::PackageOps;
        use crate::testdata::{
            verify_exact_value, verify_exact_value_where, verify_exact_value_with,
        };
        use crate::typed_values::TypedValue::*;
        use PackageOps::*;

        #[test]
        fn test_oxide_compile() {
            verify_exact_value(
                r#"
                code = oxide::compile("2 ** 4")
                code()
            "#,
                Number(F64Value(16.)),
            );
        }

        #[test]
        fn test_oxide_compile_closure() {
            verify_exact_value(
                r#"
                n = 5
                code = oxide::compile("n * n")
                code()
            "#,
                Number(I64Value(25)),
            );
        }

        #[test]
        fn test_oxide_eval_closure() {
            verify_exact_value(
                r#"
                a = 'Hello '
                b = 'World'
                oxide::eval("a + b")
            "#,
                StringValue("Hello World".to_string()),
            );
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

            // postfix
            verify_exact_value_where(
                r#"
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
        fn test_oxide_println() {
            verify_exact_value(r#"oxide::println("Hello World")"#, Boolean(true));
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
        use crate::errors::Errors::Exact;
        use crate::platform::PackageOps;
        use crate::testdata::*;
        use crate::typed_values::TypedValue::*;
        use PackageOps::*;

        #[test]
        fn test_str_ends_with_postfix_true() {
            verify_exact_value(
                r#"
                use str
                'Hello World':::ends_with('World')
            "#,
                Boolean(true),
            );
        }

        #[test]
        fn test_str_ends_with_postfix_false() {
            verify_exact_value(
                r#"
                use str
                'Hello World':::ends_with('Hello')
            "#,
                Boolean(false),
            );
        }

        #[test]
        fn test_str_ends_with_qualified_true() {
            verify_exact_value(
                r#"
                str::ends_with('Hello World', 'World')
            "#,
                Boolean(true),
            );
        }

        #[test]
        fn test_str_ends_with_qualified_false() {
            verify_exact_value(
                r#"
                str::ends_with('Hello World', 'Hello')
            "#,
                Boolean(false),
            )
        }

        #[test]
        fn test_str_format_qualified() {
            verify_exact_value(
                r#"
                str::format("This {} the {}", "is", "way")
            "#,
                StringValue("This is the way".into()),
            );
        }

        #[test]
        fn test_str_format_postfix() {
            verify_exact_value(
                r#"
                use str::format
                "This {} the {}":::format("is", "way")
            "#,
                StringValue("This is the way".into()),
            );
        }

        #[test]
        fn test_str_format_in_scope() {
            verify_exact_value(
                r#"
                use str::format
                format("This {} the {}", "is", "way")
            "#,
                StringValue("This is the way".into()),
            );
        }

        #[test]
        fn test_str_index_of_qualified() {
            verify_exact_value(
                r#"
                str::index_of('The little brown fox', 'brown')
            "#,
                Number(I64Value(11)),
            );
        }

        #[test]
        fn test_str_index_of_postfix() {
            verify_exact_value(
                r#"
                use str
                'The little brown fox':::index_of('brown')
            "#,
                Number(I64Value(11)),
            );
        }

        #[test]
        fn test_str_join() {
            verify_exact_value(
                r#"
                str::join(['1', 5, 9, '13'], ', ')
            "#,
                StringValue("1, 5, 9, 13".into()),
            );
        }

        #[test]
        fn test_str_left_qualified_positive() {
            verify_exact_value(
                r#"
                str::left('Hello World', 5)
            "#,
                StringValue("Hello".into()),
            );
        }

        #[test]
        fn test_str_left_qualified_negative() {
            verify_exact_value(
                r#"
                str::left('Hello World', -5)
            "#,
                StringValue("World".into()),
            );
        }

        #[test]
        fn test_str_left_postfix_valid() {
            verify_exact_value(
                r#"
                use str, util
                'Hello World':::left(5)
            "#,
                StringValue("Hello".into()),
            );
        }

        #[test]
        fn test_str_left_postfix_invalid() {
            // postfix - invalid case
            verify_exact_value(
                r#"
                use str, util
                12345:::left(5)
            "#,
                ErrorValue(Exact("Type Mismatch: Expected a String near 12345".into())),
            );
        }

        #[test]
        fn test_str_left_in_scope() {
            // attempt a non-import function from the same package
            verify_exact_value_where(
                r#"
                left('Hello World', 5)
            "#,
                |v| matches!(v, ErrorValue(..)),
            );
        }

        #[test]
        fn test_str_len_qualified() {
            verify_exact_value(
                r#"
                str::len('The little brown fox')
            "#,
                Number(I64Value(20)),
            );
        }

        #[test]
        fn test_str_len_postfix() {
            verify_exact_value(
                r#"
                use str
                'The little brown fox':::len()
            "#,
                Number(I64Value(20)),
            );
        }

        #[test]
        fn test_str_right_qualified_string_positive() {
            verify_exact_value(
                r#"
                str::right('Hello World', 5)
            "#,
                StringValue("World".into()),
            );
        }

        #[test]
        fn test_str_right_qualified_string_negative() {
            verify_exact_value(
                r#"
                str::right('Hello World', -5)
            "#,
                StringValue("Hello".into()),
            );
        }

        #[test]
        fn test_str_right_qualified_not_string_is_undefined() {
            verify_exact_value(
                r#"
                str::right(7779311, 5)
            "#,
                ErrorValue(Exact(
                    "Type Mismatch: Expected a String near 7779311".into(),
                )),
            );
        }

        #[test]
        fn test_str_right_postfix_string_negative() {
            verify_exact_value(
                r#"
                use str, util
                'Hello World':::right(-5)
            "#,
                StringValue("Hello".into()),
            );
        }

        #[test]
        fn test_str_split_qualified() {
            verify_exact_value(
                r#"
                str::split('Hello,there World', ' ,')
            "#,
                ArrayValue(Array::from(vec![
                    StringValue("Hello".into()),
                    StringValue("there".into()),
                    StringValue("World".into()),
                ])),
            );
        }

        #[test]
        fn test_str_split_postfix() {
            verify_exact_value(
                r#"
                use str
                'Hello World':::split(' ')
            "#,
                ArrayValue(Array::from(vec![
                    StringValue("Hello".into()),
                    StringValue("World".into()),
                ])),
            );
        }

        #[test]
        fn test_str_split_in_scope() {
            // in-scope
            verify_exact_value(
                r#"
                use str
                split('Hello,there World;Yeah!', ' ,;')
            "#,
                ArrayValue(Array::from(vec![
                    StringValue("Hello".into()),
                    StringValue("there".into()),
                    StringValue("World".into()),
                    StringValue("Yeah!".into()),
                ])),
            );
        }

        #[test]
        fn test_str_starts_with_postfix_true() {
            verify_exact_value(
                r#"
                use str
                'Hello World':::starts_with('Hello')
            "#,
                Boolean(true),
            );
        }

        #[test]
        fn test_str_starts_with_qualified_true() {
            verify_exact_value(
                r#"
                str::starts_with('Hello World', 'Hello')
            "#,
                Boolean(true),
            );
        }

        #[test]
        fn test_str_starts_with_qualified_false() {
            verify_exact_value(
                r#"
                str::starts_with('Hello World', 'World')
            "#,
                Boolean(false),
            )
        }

        #[test]
        fn test_str_strip_margin() {
            verify_exact_value(
                strip_margin(
                    r#"
                |str::strip_margin("
                ||Code example:
                ||
                ||from stocks
                ||where exchange is 'NYSE'
                |", '|')"#,
                    '|')
                .as_str(),
                StringValue("\nCode example:\n\nfrom stocks\nwhere exchange is 'NYSE'".into()),
            )
        }

        #[test]
        fn test_str_substring_defined() {
            // fully-qualified
            verify_exact_value(
                r#"
                str::substring('Hello World', 0, 5)
            "#,
                StringValue("Hello".into()),
            );
        }

        #[test]
        fn test_str_substring_undefined() {
            // fully-qualified (negative case)
            verify_exact_value(
                r#"
                str::substring(8888, 0, 5)
            "#,
                Undefined,
            )
        }

        #[test]
        fn test_str_superscript() {
            verify_exact_code(r#"
                str::superscript(123)
            "#, r#""¹²³""#)
        }

        #[test]
        fn test_str_to_string_qualified() {
            verify_exact_code(r#"
                str::to_string(125.75)
            "#, r#""125.75""#)
        }

        #[test]
        fn test_str_to_string_postfix() {
            verify_exact_value(
                r#"
                use str::to_string
                123:::to_string()
            "#,
                StringValue("123".into()),
            );
        }
    }

    /// Package "testing" tests
    #[cfg(test)]
    mod testing_tests {
        use crate::testdata::{verify_exact_code, verify_exact_table, verify_exact_value};
        use crate::typed_values::TypedValue::Boolean;

        #[test]
        fn test_testing_feature() {
            verify_exact_table(r#"
            use testing
            feature("Matches function", {
                "Compare Array contents: Equal": ctx -> {
                    assert(matches(
                        [ 1 "a" "b" "c" ],
                        [ 1 "a" "b" "c" ]))
                },
                "Compare Array contents: Not Equal": ctx -> {
                    assert(!matches(
                        [ 1 "a" "b" "c" ],
                        [ 0 "x" "y" "z" ]))
                },
                "Compare JSON contents (in sequence)": ctx -> {
                    assert(matches(
                        { first: "Tom" last: "Lane" },
                        { first: "Tom" last: "Lane" }))
                },
                "Compare JSON contents (out of sequence)": ctx -> {
                    assert(matches(
                        { scores: [82 78 99], id: "A1537" },
                        { id: "A1537", scores: [82 78 99] }))
                }
            })"#, vec![
                "|--------------------------------------------------------------------------------------------------------------------------|",
                "| id | level | item                                                                                      | passed | result |",
                "|--------------------------------------------------------------------------------------------------------------------------|",
                "| 0  | 0     | Matches function                                                                          | true   | true   |",
                "| 1  | 1     | Compare Array contents: Equal                                                             | true   | true   |",
                r#"| 2  | 2     | assert(matches([1, "a", "b", "c"], [1, "a", "b", "c"]))                                   | true   | true   |"#,
                "| 3  | 1     | Compare Array contents: Not Equal                                                         | true   | true   |",
                r#"| 4  | 2     | assert(!matches([1, "a", "b", "c"], [0, "x", "y", "z"]))                                  | true   | true   |"#,
                "| 5  | 1     | Compare JSON contents (in sequence)                                                       | true   | true   |",
                r#"| 6  | 2     | assert(matches({first: "Tom", last: "Lane"}, {first: "Tom", last: "Lane"}))               | true   | true   |"#,
                "| 7  | 1     | Compare JSON contents (out of sequence)                                                   | true   | true   |",
                r#"| 8  | 2     | assert(matches({scores: [82, 78, 99], id: "A1537"}, {id: "A1537", scores: [82, 78, 99]})) | true   | true   |"#,
                "|--------------------------------------------------------------------------------------------------------------------------|"
            ]);
        }

        #[test]
        fn test_testing_matches_exact() {
            // test a perfect match
            verify_exact_value(
                r#"
                a = { first: "Tom", last: "Lane", scores: [82, 78, 99] }
                b = { first: "Tom", last: "Lane", scores: [82, 78, 99] }
                testing::matches(a, b)
            "#,
                Boolean(true),
            );
        }

        #[test]
        fn test_testing_matches_unordered() {
            // test an unordered match
            verify_exact_value(
                r#"
                use testing::matches
                a = { scores: [82, 78, 99], first: "Tom", last: "Lane" }
                b = { last: "Lane", first: "Tom", scores: [82, 78, 99] }
                matches(a, b)
            "#,
                Boolean(true),
            );
        }

        #[test]
        fn test_testing_matches_not_match_1() {
            // test when things do not match 1
            verify_exact_value(
                r#"
                use testing
                a = { first: "Tom", last: "Lane" }
                b = { first: "Jerry", last: "Lane" }
                a:::matches(b)
            "#,
                Boolean(false),
            );
        }

        #[test]
        fn test_testing_matches_not_match_2() {
            // test when things do not match 2
            verify_exact_value(
                r#"
                a = { key: "123", values: [1, 74, 88] }
                b = { key: "123", values: [1, 74, 88, 0] }
                testing::matches(a, b)
            "#,
                Boolean(false),
            );
        }

        #[test]
        fn test_testing_type_of_array_bool() {
            verify_exact_string("testing::type_of([true, false])", "Array(2)");
        }

        #[test]
        fn test_testing_type_of_array_i64() {
            verify_exact_string("testing::type_of([12, 76, 444])", "Array(3)");
        }

        #[test]
        fn test_testing_type_of_array_str() {
            verify_exact_string("testing::type_of(['ciao', 'hello', 'world'])", "Array(3)");
        }

        #[test]
        fn test_testing_type_of_array_f64() {
            verify_exact_string("testing::type_of([12, 'hello', 76.78])", "Array(3)");
        }

        #[test]
        fn test_testing_type_of_bool() {
            verify_exact_string("testing::type_of(false)", "Boolean");
            verify_exact_string("testing::type_of(true)", "Boolean");
        }

        #[test]
        fn test_testing_type_of_date() {
            verify_exact_string("testing::type_of(cal::now())", "Date");
        }

        #[test]
        fn test_testing_type_of_fn() {
            verify_exact_string("testing::type_of((a, b) -> a + b)", "fn(a, b)");
        }

        #[test]
        fn test_testing_type_of_i64() {
            verify_exact_string("testing::type_of(1234)", "i64");
        }

        #[test]
        fn test_testing_type_of_f64() {
            verify_exact_string("testing::type_of(12.394)", "f64");
        }

        #[test]
        fn test_testing_type_of_string() {
            verify_exact_string("testing::type_of('1234')", "String(4)");
            verify_exact_string(r#"testing::type_of("abcde")"#, "String(5)");
        }

        #[test]
        fn test_testing_type_of_structure_hard() {
            verify_exact_string(
                r#"testing::type_of(Struct(symbol: String(3) = "ABC"))"#,
                r#"Struct(symbol: String(3) = "ABC")"#,
            );
        }

        #[test]
        fn test_testing_type_of_structure_soft() {
            verify_exact_string(
                r#"testing::type_of({symbol:"ABC"})"#,
                r#"Struct(symbol: String(3) = "ABC")"#,
            );
        }

        #[test]
        fn test_testing_type_of_namespace() {
            verify_exact_string(r#"testing::type_of(ns("a.b.c"))"#, "Table");
        }

        #[test]
        fn test_testing_type_of_table() {
            verify_exact_string(
                r#"testing::type_of(table(symbol: String(8), exchange: String(8), last_sale: f64))"#,
                "Table(symbol: String(8), exchange: String(8), last_sale: f64)",
            );
        }

        #[test]
        fn test_testing_type_of_uuid() {
            verify_exact_string("testing::type_of(oxide::uuid())", "UUID");
        }

        #[test]
        fn test_testing_type_of_variable() {
            verify_exact_string("testing::type_of(my_var)", "");
        }

        #[test]
        fn test_testing_type_of_null() {
            verify_exact_string("testing::type_of(null)", "");
        }

        #[test]
        fn test_testing_type_of_undefined() {
            verify_exact_string("testing::type_of(undefined)", "");
        }

        fn verify_exact_string(code: &str, expected: &str) {
            verify_exact_code(code, format!("\"{}\"", expected).as_str())
        }
    }

    /// Package "tools" tests
    #[cfg(test)]
    mod tools_tests {
        use super::*;
        use crate::compiler::Compiler;
        use crate::expression::Expression::{
            ColonColon, FunctionCall, StructureExpression, Variable,
        };
        use crate::interpreter::Interpreter;
        use crate::number_kind::NumberKind::F64Kind;
        use crate::platform::PackageOps;
        use crate::structures::HardStructure;
        use crate::structures::Structures::Hard;
        use crate::testdata::*;
        use crate::typed_values::TypedValue::*;
        use PackageOps::*;

        #[test]
        fn test_tools_compact() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_table_with(
                interpreter,
                r#"
                stocks = ns("platform.compact.stocks")
                table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                [{ symbol: "DMX", exchange: "NYSE", last_sale: 99.99 },
                 { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                 { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                 { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                 { symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
                 { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 },
                 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
                delete from stocks where last_sale > 1.0
                from stocks
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

            verify_exact_table_with(
                interpreter,
                r#"
                use tools
                stocks:::compact()
                from stocks
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
        fn test_tools_describe() {
            // fully-qualified
            verify_exact_table(
                r#"
                tools::describe({ symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 })
            "#,
                vec![
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
            verify_exact_table(
                r#"
                use tools
                stocks = ns("platform.describe.stocks")
                table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                stocks:::describe()
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
            // fully-qualified
            verify_exact_table(
                r#"
                stocks = ns("platform.fetch.stocks")
                table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                [{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                 { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
                tools::fetch(stocks, 2)
            "#,
                vec![
                    "|------------------------------------|",
                    "| id | symbol | exchange | last_sale |",
                    "|------------------------------------|",
                    "| 2  | JET    | NASDAQ   | 32.12     |",
                    "|------------------------------------|",
                ],
            );

            // postfix
            verify_exact_table(
                r#"
                use tools
                stocks = to_table([
                    { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                    { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                    { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }
                ])
                stocks:::fetch(1)
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
        fn test_tools_filter_over_array() {
            verify_exact_value(
                r#"
                tools::filter(1..7, n -> (n % 2) == 0)
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
            verify_exact_table(
                r#"
                stocks = ns("platform.filter_over_table.stocks")
                table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                [{ symbol: "WKRP", exchange: "NYSE", last_sale: 11.11 },
                 { symbol: "ACDC", exchange: "AMEX", last_sale: 37.43 },
                 { symbol: "UELO", exchange: "NYSE", last_sale: 91.82 }] ~> stocks
                use tools
                stocks:::filter(row -> exchange is "AMEX")
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
        fn test_tools_journal() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_value_whence(
                interpreter,
                r#"
                stocks = ns("platform.journal.stocks")
                drop table stocks
            "#,
                |result| matches!(result, Boolean(_)),
            );
            interpreter = verify_exact_value_with(
                interpreter,
                r#"
                create table stocks fn(
                   symbol: String(8), exchange: String(8), last_sale: f64
                ) => {
                    symbol: symbol,
                    exchange: exchange,
                    last_sale: last_sale * 2.0,
                    event_time: cal::now()
                 }
            "#,
                Boolean(true),
            );
            interpreter = verify_exact_value_with(
                interpreter,
                r#"
                [{ symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                 { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
            "#,
                Number(I64Value(3)),
            );
            interpreter = verify_exact_table_with(
                interpreter,
                r#"
                use tools
                stocks:::journal()
            "#,
                vec![
                    "|------------------------------------|",
                    "| id | symbol | exchange | last_sale |",
                    "|------------------------------------|",
                    "| 0  | BOOM   | NYSE     | 56.88     |",
                    "| 1  | ABC    | AMEX     | 12.49     |",
                    "| 2  | JET    | NASDAQ   | 32.12     |",
                    "|------------------------------------|",
                ],
            )
        }

        #[test]
        fn test_tools_map_over_array() {
            verify_exact_value(
                r#"
                tools::map([1, 2, 3], n -> n * 2)
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
            verify_exact_table(
                r#"
                stocks = ns("platform.map_over_table.stocks")
                table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                [{ symbol: "WKRP", exchange: "NYSE", last_sale: 11.11 },
                 { symbol: "ACDC", exchange: "AMEX", last_sale: 35.11 },
                 { symbol: "UELO", exchange: "NYSE", last_sale: 90.12 }] ~> stocks
                use tools
                stocks:::map(row -> {
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
            verify_exact_table(
                r#"
                use tools
                stocks = ns("platform.pop.stocks")
                table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                [{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                 { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
                stocks:::pop()
            "#,
                vec![
                    "|------------------------------------|",
                    "| id | symbol | exchange | last_sale |",
                    "|------------------------------------|",
                    "| 2  | JET    | NASDAQ   | 32.12     |",
                    "|------------------------------------|",
                ],
            );
            verify_exact_table(
                r#"
                stocks = ns("platform.pop.stocks")
                tools::pop(stocks)
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
        fn test_tools_push_compile() {
            let code = Compiler::build(
                r#"
                tools::push(stocks, { symbol: "DEX", exchange: "OTC_BB", last_sale: 0.0086 })
            "#,
            )
            .unwrap();
            assert_eq!(
                code,
                ColonColon(
                    Box::from(Variable("tools".into())),
                    Box::from(FunctionCall {
                        fx: Box::from(Variable("push".into())),
                        args: vec![
                            Variable("stocks".into()),
                            StructureExpression(vec![
                                ("symbol".to_string(), Literal(StringValue("DEX".into()))),
                                (
                                    "exchange".to_string(),
                                    Literal(StringValue("OTC_BB".to_string()))
                                ),
                                ("last_sale".to_string(), Literal(Number(F64Value(0.0086))))
                            ])
                        ]
                    })
                )
            )
        }

        #[test]
        fn test_tools_push_array_evaluate() {
            verify_exact_table(
                r#"
                stocks = [
                    { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                    { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                    { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }
                ]
                stocks = tools::push(stocks, { symbol: "DEX", exchange: "OTC_BB", last_sale: 0.0086 })
                from stocks
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
            verify_exact_table(
                r#"
                stocks = ns("platform.push.stocks")
                table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                [{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                 { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
                tools::push(stocks, { symbol: "DEX", exchange: "OTC_BB", last_sale: 0.0086 })
                from stocks
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
                stocks = tools::push(stocks, ("DEX", "OTC_BB", 0.0086))
                from tools::to_table(stocks)
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
            interpreter = verify_exact_value_whence(
                interpreter,
                r#"
                stocks = ns("platform.replay.stocks")
                drop table stocks
            "#,
                |result| matches!(result, Boolean(_)),
            );
            interpreter = verify_exact_value_with(
                interpreter,
                r#"
                create table stocks fn(
                   symbol: String(8), exchange: String(8), last_sale: f64
                ) => {
                    symbol: symbol,
                    exchange: exchange,
                    last_sale: last_sale * 2.0,
                    rank: __row_id__ + 1
                 }
            "#,
                Boolean(true),
            );
            interpreter = verify_exact_value_with(
                interpreter,
                r#"
                [{ symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                 { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
            "#,
                Number(I64Value(3)),
            );
            interpreter = verify_exact_table_with(
                interpreter,
                r#"
                use tools
                stocks:::replay()
                from stocks
            "#,
                vec![
                    "|-------------------------------------------|",
                    "| id | symbol | exchange | last_sale | rank |",
                    "|-------------------------------------------|",
                    "| 0  | BOOM   | NYSE     | 113.76    | 1    |",
                    "| 1  | ABC    | AMEX     | 24.98     | 2    |",
                    "| 2  | JET    | NASDAQ   | 64.24     | 3    |",
                    "|-------------------------------------------|",
                ],
            )
        }

        #[test]
        fn test_tools_reverse_arrays() {
            verify_exact_table(
                r#"
                use tools
                to_table(reverse(['cat', 'dog', 'ferret', 'mouse']))
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
            verify_exact_value(
                r#"
                backwards(a) -> tools::reverse(a)
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
                use tools
                stocks = to_table([
                    { symbol: "ABC", exchange: "AMEX", last_sale: 12.33 },
                    { symbol: "BIZ", exchange: "NYSE", last_sale: 9.775 },
                    { symbol: "XYZ", exchange: "NASDAQ", last_sale: 89.11 }
                ])
                reverse(stocks)
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
            verify_exact_table(
                r#"
                use tools
                stocks = ns("platform.reverse.stocks")
                table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                [{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                 { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
                stocks:::reverse()
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
            let result = interpreter
                .evaluate(
                    r#"
                use tools
                stocks = ns("platform.scan.stocks")
                table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                [{ symbol: "ABC", exchange: "AMEX", last_sale: 12.33 },
                 { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                 { symbol: "BIZ", exchange: "NYSE", last_sale: 9.775 },
                 { symbol: "GOTO", exchange: "OTC", last_sale: 0.1442 },
                 { symbol: "XYZ", exchange: "NYSE", last_sale: 0.0289 }] ~> stocks
                delete from stocks where last_sale > 1.0
                stocks:::scan()
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
            verify_exact_code(
                r#"
                 tools::to_array(("a", "b", "c"))
            "#,
                r#"["a", "b", "c"]"#,
            );
        }

        #[test]
        fn test_tools_to_array_with_strings_qualified() {
            verify_exact_value(
                r#"
                 tools::to_array("Hello")
            "#,
                ArrayValue(Array::from(vec![
                    StringValue("H".into()),
                    StringValue("e".into()),
                    StringValue("l".into()),
                    StringValue("l".into()),
                    StringValue("o".into()),
                ])),
            );
        }

        #[test]
        fn test_tools_to_array_with_strings_postfix() {
            verify_exact_value(
                r#"
                use tools
                "World":::to_array()
            "#,
                ArrayValue(Array::from(vec![
                    StringValue("W".into()),
                    StringValue("o".into()),
                    StringValue("r".into()),
                    StringValue("l".into()),
                    StringValue("d".into()),
                ])),
            );
        }

        #[test]
        fn test_tools_to_array_with_tables() {
            // fully qualified
            let mut interpreter = Interpreter::new();
            let result = interpreter
                .evaluate(
                    r#"
                 tools::to_array(tools::to_table([
                     { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                     { symbol: "DMX", exchange: "OTC_BB", last_sale: 1.17 }
                 ]))
            "#,
                )
                .unwrap();
            let params = vec![
                Parameter::with_default("symbol", StringType(3), StringValue("BIZ".into())),
                Parameter::with_default("exchange", StringType(4), StringValue("NYSE".into())),
                Parameter::with_default("last_sale", NumberType(F64Kind), Number(F64Value(23.66))),
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
            verify_exact_value(
                r#"
                use tools::to_csv
                stocks = ns("platform.csv.stocks")
                table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                [{ symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
                 { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                 { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                 { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                 { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }] ~> stocks
                stocks:::to_csv()
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
            verify_exact_value(
                r#"
                use tools::to_json
                stocks = ns("platform.json.stocks")
                table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                [{ symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
                 { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                 { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                 { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                 { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }] ~> stocks
                stocks:::to_json()
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
            verify_exact_table(
                r#"
                tools::to_table(['cat', 'dog', 'ferret', 'mouse'])
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
            verify_exact_table(
                r#"
                tools::to_table(Struct(
                    symbol: String(8) = "ABC",
                    exchange: String(8) = "NYSE",
                    last_sale: f64 = 45.67
                ))
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
            verify_exact_table(
                r#"
                stocks = tools::to_table([
                    { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                    { symbol: "DMX", exchange: "OTC_BB", last_sale: 1.17 }
                ])

                tools::to_table([
                    stocks,
                    Struct(
                        symbol: String(8) = "ABC",
                        exchange: String(8) = "OTHER_OTC",
                        last_sale: f64 = 0.67),
                    { symbol: "TRX", exchange: "AMEX", last_sale: 29.88 },
                    { symbol: "BMX", exchange: "NASDAQ", last_sale: 46.11 }
                ])
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
        use crate::data_types::DataType::BinaryType;
        use crate::interpreter::Interpreter;
        use crate::platform::PackageOps;
        use crate::testdata::*;
        use crate::typed_values::TypedValue::*;
        use PackageOps::*;

        #[test]
        fn test_util_base64() {
            verify_exact_value(
                "util::base64('Hello World')",
                StringValue("SGVsbG8gV29ybGQ=".into()),
            )
        }

        #[test]
        fn test_util_gzip_and_gunzip() {
            verify_exact_value(
                r#"
                compressed = util::gzip('Hello World')
                util::gunzip(compressed)
            "#,
                Binary(b"Hello World".to_vec()),
            )
        }

        #[test]
        fn test_util_hex() {
            verify_exact_value(
                "util::hex('Hello World')",
                StringValue("48656c6c6f20576f726c64".into()),
            )
        }

        #[test]
        fn test_util_md5() {
            verify_exact_code(
                "util::md5('Hello World')",
                "b10a8db164e0754105b7a99be72e3fe5",
            )
        }

        #[test]
        fn test_util_md5_type() {
            verify_data_type("util::md5(a)", BinaryType(16));
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
            interpreter = verify_exact_value_whence(interpreter, "1015:::to_f32()", |n| {
                n == Number(F32Value(1015.))
            });
            interpreter = verify_exact_value_whence(interpreter, "7779311:::to_f64()", |n| {
                n == Number(F64Value(7779311.))
            });

            // signed-integer kinds
            interpreter =
                verify_exact_value_whence(interpreter, "12345678987.43:::to_i128()", |n| {
                    n == Number(I128Value(12345678987))
                });
            interpreter = verify_exact_value_whence(interpreter, "123456789.42:::to_i64()", |n| {
                n == Number(I64Value(123456789))
            });
            interpreter = verify_exact_value_whence(interpreter, "-765.65:::to_i32()", |n| {
                n == Number(I32Value(-765))
            });
            interpreter = verify_exact_value_whence(interpreter, "-567.311:::to_i16()", |n| {
                n == Number(I16Value(-567))
            });
            interpreter = verify_exact_value_whence(interpreter, "-125.089:::to_i8()", |n| {
                n == Number(I8Value(-125))
            });

            // unsigned-integer kinds
            interpreter = verify_exact_value_whence(interpreter, "12789.43:::to_u128()", |n| {
                n == Number(U128Value(12789))
            });
            interpreter = verify_exact_value_whence(interpreter, "12.3:::to_u64()", |n| {
                n == Number(U64Value(12))
            });
            interpreter = verify_exact_value_whence(interpreter, "765.65:::to_u32()", |n| {
                n == Number(U32Value(765))
            });
            interpreter = verify_exact_value_whence(interpreter, "567.311:::to_u16()", |n| {
                n == Number(U16Value(567))
            });
            interpreter = verify_exact_value_whence(interpreter, "125.089:::to_u8()", |n| {
                n == Number(U8Value(125))
            });

            // scope checks
            let mut interpreter = Interpreter::new();

            // initially 'to_u8' should not be in scope
            assert_eq!(interpreter.get("to_u8"), None);
            assert_eq!(interpreter.get("to_u16"), None);
            assert_eq!(interpreter.get("to_u32"), None);
            assert_eq!(interpreter.get("to_u64"), None);
            assert_eq!(interpreter.get("to_u128"), None);

            // import all conversion members
            interpreter.evaluate("use util").unwrap();

            // after the import, 'to_u8' should be in scope
            assert_eq!(
                interpreter.get("to_u8"),
                Some(PlatformOp(PackageOps::Utils(UtilsPkg::ToU8)))
            );
            assert_eq!(
                interpreter.get("to_u16"),
                Some(PlatformOp(PackageOps::Utils(UtilsPkg::ToU16)))
            );
            assert_eq!(
                interpreter.get("to_u32"),
                Some(PlatformOp(PackageOps::Utils(UtilsPkg::ToU32)))
            );
            assert_eq!(
                interpreter.get("to_u64"),
                Some(PlatformOp(PackageOps::Utils(UtilsPkg::ToU64)))
            );
            assert_eq!(
                interpreter.get("to_u128"),
                Some(PlatformOp(PackageOps::Utils(UtilsPkg::ToU128)))
            );
        }
    }

    /// Package "www" tests
    #[cfg(test)]
    mod www_tests {
        use super::*;
        use crate::columns::Column;
        use crate::interpreter::Interpreter;
        use crate::platform::PackageOps;
        use crate::structures::Structures::Soft;
        use crate::structures::{HardStructure, SoftStructure};
        use crate::testdata::*;
        use crate::typed_values::TypedValue::*;
        use serde_json::json;
        use PackageOps::*;

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

        #[test]
        fn test_www_serve() {
            let mut interpreter = Interpreter::new();
            let result = interpreter
                .evaluate(
                    r#"
                www::serve(8822)
                stocks = ns("platform.www.quotes")
                table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                [{ symbol: "XINU", exchange: "NYSE", last_sale: 8.11 },
                 { symbol: "BOX", exchange: "NYSE", last_sale: 56.88 },
                 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 },
                 { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                 { symbol: "MIU", exchange: "OTCBB", last_sale: 2.24 }] ~> stocks
                GET http://localhost:8822/platform/www/quotes/1/4
            "#,
                )
                .unwrap();
            assert_eq!(
                result.to_json(),
                json!([
                    {"symbol": "BOX", "exchange": "NYSE", "last_sale": 56.88},
                    {"symbol": "JET", "exchange": "NASDAQ", "last_sale": 32.12},
                    {"symbol": "ABC", "exchange": "AMEX", "last_sale": 12.49}
                ])
            );
        }

        #[test]
        fn test_www_serve_workflow() {
            let mut interpreter = Interpreter::new();

            // create the table
            let result = interpreter
                .evaluate(
                    r#"
                stocks = ns("platform.www.stocks")
                table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
            "#,
                )
                .unwrap();
            assert_eq!(result, Number(I64Value(0)));

            // set up a listener on port 8838
            let result = interpreter
                .evaluate(
                    r#"
                www::serve(8838)
            "#,
                )
                .unwrap();
            assert_eq!(result, Boolean(true));

            // append a new row
            let row_id = interpreter
                .evaluate(
                    r#"
                POST {
                    url: http://localhost:8838/platform/www/stocks/0
                    body: { symbol: "ABC", exchange: "AMEX", last_sale: 11.77 }
                }
            "#,
                )
                .unwrap();
            assert!(matches!(row_id, Number(I64Value(..))));

            // fetch the previously created row
            let row = interpreter
                .evaluate(
                    format!(
                        r#"
                GET http://localhost:8838/platform/www/stocks/{row_id}
            "#
                    )
                    .as_str(),
                )
                .unwrap();
            assert_eq!(
                row.to_json(),
                json!({"exchange":"AMEX","symbol":"ABC","last_sale":11.77})
            );

            // replace the previously created row
            let result = interpreter
                .evaluate(
                    format!(
                        r#"
                PUT {{
                    url: http://localhost:8838/platform/www/stocks/{row_id}
                    body: {{ symbol: "ABC", exchange: "AMEX", last_sale: 11.79 }}
                }}
            "#
                    )
                    .as_str(),
                )
                .unwrap();
            assert_eq!(result, Number(I64Value(1)));

            // re-fetch the previously updated row
            let row = interpreter
                .evaluate(
                    format!(
                        r#"
                GET http://localhost:8838/platform/www/stocks/{row_id}
            "#
                    )
                    .as_str(),
                )
                .unwrap();
            assert_eq!(
                row.to_json(),
                json!({"symbol":"ABC","exchange":"AMEX","last_sale":11.79})
            );

            // update the previously created row
            let result = interpreter
                .evaluate(
                    format!(
                        r#"
                PATCH {{
                    url: http://localhost:8838/platform/www/stocks/{row_id}
                    body: {{ last_sale: 11.81 }}
                }}
            "#
                    )
                    .as_str(),
                )
                .unwrap();
            assert_eq!(result, Number(I64Value(1)));

            // re-fetch the previously updated row
            let row = interpreter
                .evaluate(
                    format!(
                        r#"
                GET http://localhost:8838/platform/www/stocks/{row_id}
            "#
                    )
                    .as_str(),
                )
                .unwrap();
            assert_eq!(
                row.to_json(),
                json!({"last_sale":11.81,"symbol":"ABC","exchange":"AMEX"})
            );

            // fetch the headers for the previously updated row
            let result = interpreter
                .evaluate(
                    format!(
                        r#"
                HEAD http://localhost:8838/platform/www/stocks/{row_id}
            "#
                    )
                    .as_str(),
                )
                .unwrap();
            println!("HEAD: {}", result.to_string());
            assert!(matches!(result, Structured(Soft(..))));

            // delete the previously updated row
            let result = interpreter
                .evaluate(
                    format!(
                        r#"
                DELETE http://localhost:8838/platform/www/stocks/{row_id}
            "#
                    )
                    .as_str(),
                )
                .unwrap();
            assert_eq!(result, Number(I64Value(1)));

            // verify the deleted row is empty
            let row = interpreter
                .evaluate(
                    format!(
                        r#"
                GET http://localhost:8838/platform/www/stocks/{row_id}
            "#
                    )
                    .as_str(),
                )
                .unwrap();
            assert_eq!(row, Structured(Soft(SoftStructure::empty())));
        }

        #[test]
        fn test_www_serve_workflow_script() {
            let mut interpreter = Interpreter::new();

            // create the table
            let result = interpreter
                .evaluate(
                    r#"
                // setup a listener on port 8838
                www::serve(8838)

                // create the table
                table(symbol: String(8), exchange: String(8), last_sale: f64)
                    ~> ns("platform.http_workflow.stocks")

                use testing
                row_id = POST {
                    url: http://localhost:8838/platform/http_workflow/stocks/0
                    body: { symbol: "ABC", exchange: "AMEX", last_sale: 11.77 }
                }
                assert(matches(row_id, 0))
                GET http://localhost:8838/platform/http_workflow/stocks/0
            "#,
                )
                .unwrap();
            assert_eq!(
                result.to_code(),
                r#"{exchange: "AMEX", last_sale: 11.77, symbol: "ABC"}"#
            );
        }
    }
}
