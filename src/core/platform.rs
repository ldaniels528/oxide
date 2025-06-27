#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Platform Operations class
////////////////////////////////////////////////////////////////////

use crate::byte_code_compiler::ByteCodeCompiler;
use crate::data_types::DataType;
use crate::data_types::DataType::*;
use crate::sequences::{Array, Sequence};

use crate::dataframe::Dataframe;
use crate::dataframe::Dataframe::Model;
use crate::errors::throw;
use crate::errors::Errors::*;
use crate::errors::TypeMismatchErrors::*;
use crate::expression::Expression::{FunctionCall, Literal, StructureExpression};
use crate::file_row_collection::FileRowCollection;
use crate::journaling::Journaling;
use crate::machine::Machine;
use crate::model_row_collection::ModelRowCollection;
use crate::namespaces::Namespace;
use crate::number_kind::NumberKind::*;
use crate::numbers::Numbers::*;
use crate::packages::*;
use crate::parameter::Parameter;
use crate::platform::PackageOps::Agg;
use crate::row_collection::RowCollection;
use crate::structures::Structure;
use crate::structures::Structures::Soft;
use crate::structures::{Row, SoftStructure};
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::*;
use actix::ActorStreamExt;
use chrono::{Datelike, TimeZone, Timelike};
use crossterm::style::Stylize;
use num_traits::real::Real;
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::ops::Deref;

pub const MAJOR_VERSION: u8 = 1;
pub const MINOR_VERSION: u8 = 46;
pub const VERSION: &str = "0.46";

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

    pub(crate) fn apply_fn_over_array(
        ms: Machine,
        array: &Array,
        function: &TypedValue,
        logic: fn(TypedValue, TypedValue) -> std::io::Result<Option<TypedValue>>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let mut new_arr = vec![];
        // apply the function over all items in the array
        for item in array.get_values() {
            // apply the function on the current item
            let (_, result) = ms.evaluate(&FunctionCall {
                fx: Literal(function.clone()).into(),
                args: vec![Literal(item.clone())],
            })?;
            // if an outcome was produced, capture it
            if let Some(outcome) = logic(item, result)? {
                new_arr.push(outcome)
            }
        }
        Ok((ms, ArrayValue(Array::from(new_arr))))
    }

    pub(crate) fn apply_fn_over_table(
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
            Ok((ms, TableValue(Model({
                let mut dest_rows = vec![];
                for item in new_arr {
                    let transformed_rows = match item {
                        Structured(s) => vec![Row::new(0, s.get_values())],
                        TableValue(df) => df.get_rows(),
                        z => return throw(TypeMismatch(StructExpected(z.to_code(), z.to_code(), )))
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
                    Ok(rows) => TableValue(Model(ModelRowCollection::from_columns_and_rows(
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

/// Unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::interpreter::Interpreter;
    use crate::platform::PackageOps;
    use crate::typed_values::TypedValue::*;
    use PackageOps::*;

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

    #[ignore]
    #[test]
    fn test_examples() {
        let mut errors = 0;
        let mut interpreter = Interpreter::new();
        for op in PackageOps::get_contents() {
            for example in op.get_examples() {
                match interpreter.evaluate(example.as_str()) {
                    Ok(response) => assert_ne!(response, Undefined),
                    Err(err) => {
                        println!("{}", "*".repeat(60));
                        println!("{}", example);
                        eprintln!("ERROR: {}", err);
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
        for pf in PackageOps::get_contents() {
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
        // arrays
        assert_eq!(
            Arrays(ArraysPkg::Filter).to_code(),
            "arrays::filter(a: Array(), b: fn(item): Boolean)"
        );
        assert_eq!(Arrays(ArraysPkg::Len).to_code(), "arrays::len(a: Array())");
        assert_eq!(
            Arrays(ArraysPkg::Map).to_code(),
            "arrays::map(a: Array(), b: fn(item))"
        );
        assert_eq!(Arrays(ArraysPkg::Pop).to_code(), "arrays::pop(a: Array())");
        assert_eq!(
            Arrays(ArraysPkg::Push).to_code(),
            "arrays::push(a: Array(), b)"
        );
        assert_eq!(
            Arrays(ArraysPkg::Reverse).to_code(),
            "arrays::reverse(a: Array())"
        );
        assert_eq!(Arrays(ArraysPkg::ToArray).to_code(), "arrays::to_array(a)");
        // cal
        assert_eq!(Cal(CalPkg::DateDay).to_code(), "cal::day_of(a: Date)");
        assert_eq!(Cal(CalPkg::DateHour12).to_code(), "cal::hour12(a: Date)");
        assert_eq!(Cal(CalPkg::DateHour24).to_code(), "cal::hour24(a: Date)");
        assert_eq!(Cal(CalPkg::DateMinute).to_code(), "cal::minute_of(a: Date)");
        assert_eq!(Cal(CalPkg::DateMonth).to_code(), "cal::month_of(a: Date)");
        assert_eq!(Cal(CalPkg::DateSecond).to_code(), "cal::second_of(a: Date)");
        assert_eq!(Cal(CalPkg::DateYear).to_code(), "cal::year_of(a: Date)");
        assert_eq!(Cal(CalPkg::Minus).to_code(), "cal::minus(a: Date, b: i64)");
        assert_eq!(Cal(CalPkg::Now).to_code(), "cal::now()");
        assert_eq!(Cal(CalPkg::Plus).to_code(), "cal::plus(a: Date, b: i64)");
        // durations
        assert_eq!(
            Durations(DurationsPkg::Days).to_code(),
            "durations::days(n: i64)"
        );
        assert_eq!(
            Durations(DurationsPkg::Hours).to_code(),
            "durations::hours(n: i64)"
        );
        assert_eq!(
            Durations(DurationsPkg::Millis).to_code(),
            "durations::millis(n: i64)"
        );
        assert_eq!(
            Durations(DurationsPkg::Minutes).to_code(),
            "durations::minutes(n: i64)"
        );
        assert_eq!(
            Durations(DurationsPkg::Seconds).to_code(),
            "durations::seconds(n: i64)"
        );
        // io
        assert_eq!(
            Io(IoPkg::FileCreate).to_code(),
            "io::create_file(a: String, b: String)"
        );
        assert_eq!(Io(IoPkg::FileExists).to_code(), "io::exists(s: String)");
        assert_eq!(
            Io(IoPkg::FileReadText).to_code(),
            "io::read_text_file(s: String)"
        );
        assert_eq!(Io(IoPkg::StdErr).to_code(), "io::stderr(s: String)");
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
        // os
        assert_eq!(Os(OsPkg::Call).to_code(), "os::call(s: String)");
        assert_eq!(Os(OsPkg::Clear).to_code(), "os::clear()");
        assert_eq!(Os(OsPkg::CurrentDir).to_code(), "os::current_dir()");
        assert_eq!(Os(OsPkg::Env).to_code(), "os::env()");
        // oxide
        assert_eq!(
            Oxide(OxidePkg::Compile).to_code(),
            "oxide::compile(s: String)"
        );
        assert_eq!(Oxide(OxidePkg::Debug).to_code(), "oxide::debug(s: String)");
        assert_eq!(Oxide(OxidePkg::Eval).to_code(), "oxide::eval(s: String)");
        assert_eq!(Oxide(OxidePkg::Help).to_code(), "oxide::help()");
        assert_eq!(Oxide(OxidePkg::History).to_code(), "oxide::history()");
        assert_eq!(Oxide(OxidePkg::Home).to_code(), "oxide::home()");
        assert_eq!(
            Oxide(OxidePkg::Println).to_code(),
            "oxide::println(s: String)"
        );
        assert_eq!(Oxide(OxidePkg::Reset).to_code(), "oxide::reset()");
        assert_eq!(Oxide(OxidePkg::UUID).to_code(), "oxide::uuid()");
        assert_eq!(Oxide(OxidePkg::Version).to_code(), "oxide::version()");
        // str
        assert_eq!(
            Strings(StringsPkg::EndsWith).to_code(),
            "str::ends_with(a: String, b: String)"
        );
        assert_eq!(
            Strings(StringsPkg::Format).to_code(),
            "str::format(a: String, b: String)"
        );
        assert_eq!(
            Strings(StringsPkg::IndexOf).to_code(),
            "str::index_of(s: String, n: i64)"
        );
        assert_eq!(
            Strings(StringsPkg::Join).to_code(),
            "str::join(a: Array(), b: String)"
        );
        assert_eq!(
            Strings(StringsPkg::Left).to_code(),
            "str::left(s: String, n: i64)"
        );
        assert_eq!(Strings(StringsPkg::Len).to_code(), "str::len(s: String)");
        assert_eq!(
            Strings(StringsPkg::Right).to_code(),
            "str::right(s: String, n: i64)"
        );
        assert_eq!(
            Strings(StringsPkg::Split).to_code(),
            "str::split(a: String, b: String)"
        );
        assert_eq!(
            Strings(StringsPkg::StartsWith).to_code(),
            "str::starts_with(a: String, b: String)"
        );
        assert_eq!(
            Strings(StringsPkg::StripMargin).to_code(),
            "str::strip_margin(a: String, b: String)"
        );
        assert_eq!(
            Strings(StringsPkg::Substring).to_code(),
            "str::substring(s: String, m: i64, n: i64)"
        );
        assert_eq!(Strings(StringsPkg::ToString).to_code(), "str::to_string(a)");
        // tools
        assert_eq!(
            Tools(ToolsPkg::Compact).to_code(),
            "tools::compact(t: Table)"
        );
        assert_eq!(
            Tools(ToolsPkg::Describe).to_code(),
            "tools::describe(t: Table)"
        );
        assert_eq!(
            Tools(ToolsPkg::Fetch).to_code(),
            "tools::fetch(t: Table, n: i64)"
        );
        assert_eq!(Tools(ToolsPkg::Filter).to_code(), "tools::filter(a, b)");
        assert_eq!(
            Nsd(NsdPkg::Journal).to_code(),
            "nsd::journal(t: Table)"
        );
        assert_eq!(Tools(ToolsPkg::Len).to_code(), "tools::len(t: Table)");
        assert_eq!(Tools(ToolsPkg::Map).to_code(), "tools::map(a, b)");
        assert_eq!(Tools(ToolsPkg::Pop).to_code(), "tools::pop(t: Table)");
        assert_eq!(Tools(ToolsPkg::Push).to_code(), "tools::push(a, b)");
        assert_eq!(Nsd(NsdPkg::Replay).to_code(), "nsd::replay(t: Table)");
        assert_eq!(
            Tools(ToolsPkg::Reverse).to_code(),
            "tools::reverse(t: Table)"
        );
        assert_eq!(Tools(ToolsPkg::RowId).to_code(), "tools::row_id()");
        assert_eq!(Tools(ToolsPkg::Scan).to_code(), "tools::scan(t: Table)");
        assert_eq!(
            Tools(ToolsPkg::ToArray).to_code(),
            "tools::to_array(t: Table)"
        );
        assert_eq!(Tools(ToolsPkg::ToCSV).to_code(), "tools::to_csv(t: Table)");
        assert_eq!(
            Tools(ToolsPkg::ToJSON).to_code(),
            "tools::to_json(t: Table)"
        );
        assert_eq!(Tools(ToolsPkg::ToTable).to_code(), "tools::to_table(a)");
        // util
        assert_eq!(Utils(UtilsPkg::Base64).to_code(), "util::base64(a)");
        assert_eq!(Utils(UtilsPkg::Binary).to_code(), "util::to_binary(a)");
        assert_eq!(Utils(UtilsPkg::Gzip).to_code(), "util::gzip(a)");
        assert_eq!(Utils(UtilsPkg::Gunzip).to_code(), "util::gunzip(a)");
        assert_eq!(Utils(UtilsPkg::Hex).to_code(), "util::hex(a)");
        assert_eq!(Utils(UtilsPkg::MD5).to_code(), "util::md5(a)");
        assert_eq!(Utils(UtilsPkg::ToASCII).to_code(), "util::to_ascii(n: i64)");
        assert_eq!(Utils(UtilsPkg::ToDate).to_code(), "util::to_date(a)");
        assert_eq!(Utils(UtilsPkg::ToF64).to_code(), "util::to_f64(a)");
        assert_eq!(Utils(UtilsPkg::ToI64).to_code(), "util::to_i64(a)");
        assert_eq!(Utils(UtilsPkg::ToI128).to_code(), "util::to_i128(a)");
        assert_eq!(Utils(UtilsPkg::ToU128).to_code(), "util::to_u128(a)");
        // www
        assert_eq!(
            Www(WwwPkg::URLDecode).to_code(),
            "www::url_decode(s: String)"
        );
        assert_eq!(
            Www(WwwPkg::URLEncode).to_code(),
            "www::url_encode(s: String)"
        );
        assert_eq!(Www(WwwPkg::Serve).to_code(), "http::serve(n: i64)");
    }
}
