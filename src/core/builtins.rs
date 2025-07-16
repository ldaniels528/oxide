#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
//  Builtins - internal package manager
////////////////////////////////////////////////////////////////////

use crate::data_types::DataType;
use crate::packages::PackageOps::*;
use crate::packages::*;
use crate::typed_values::TypedValue;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Builtins - internal package manager
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Builtins {
    functions: HashMap<String, HashMap<String, PackageOps>>
}

impl Builtins {

    ////////////////////////////////////////////////////////////////////
    //  Constructor
    ////////////////////////////////////////////////////////////////////
    
    pub fn new() -> Builtins {
        let mut functions = HashMap::new();
        functions.insert("Array".into(), Self::array_functions());
        functions.insert("BLOB".into(), Self::blobs_functions());
        functions.insert("ByteString".into(), Self::bytestring_functions());
        functions.insert("Char".into(), Self::char_functions());
        functions.insert("DateTime".into(), Self::datetime_functions());
        functions.insert("Error".into(), Self::error_functions());
        functions.insert("Function".into(), Self::function_functions());
        functions.insert("Number".into(), Self::number_functions());
        functions.insert("String".into(), Self::string_functions());
        functions.insert("Struct".into(), Self::structure_functions());
        functions.insert("Table".into(), Self::table_functions());
        functions.insert("Tuple".into(), Self::tuple_functions());
        functions.insert("UUID".into(), Self::uuid_functions());
        Builtins {
            functions
        }
    }

    ////////////////////////////////////////////////////////////////////
    //  Instance methods
    ////////////////////////////////////////////////////////////////////

    /// Builds a mapping of the package name to function vector
    pub fn build_packages(&self) -> HashMap<String, Vec<PackageOps>> {
        PackageOps::build_packages()
    }

    pub fn lookup_by_name(&self, type_name: &str, name: &str) -> Option<PackageOps> {
        self.functions.get(type_name)
            .and_then(|map| map.get(name)).cloned()
    }

    pub fn lookup_by_type(&self, data_type: &DataType, name: &str) -> Option<PackageOps> {
        let type_name = data_type.get_name();
        self.lookup_by_name(type_name.as_str(), name)
    }

    pub fn lookup_by_value(&self, host: &TypedValue, name: &str) -> Option<PackageOps> {
        self.lookup_by_type(&host.get_type(), name)
    }
    
    ////////////////////////////////////////////////////////////////////
    //  Internal
    ////////////////////////////////////////////////////////////////////

    fn add_common_functions(
        mut functions: HashMap<String, PackageOps>
    ) -> HashMap<String, PackageOps> {
        functions.insert("to".into(), Utils(UtilsPkg::To));
        functions.insert("to_bytes".into(), Utils(UtilsPkg::ToBytes));
        functions.insert("to_string".into(), Strings(StringsPkg::ToString));
        functions
    }

    fn add_transformation_functions(
        mut functions: HashMap<String, PackageOps>
    ) -> HashMap<String, PackageOps> {
        functions.insert("filter".into(), Arrays(ArraysPkg::Filter));
        functions.insert("is_empty".into(), Arrays(ArraysPkg::IsEmpty));
        functions.insert("len".into(), Arrays(ArraysPkg::Len));
        functions.insert("map".into(), Tools(ToolsPkg::Map));
        functions.insert("reduce".into(), Arrays(ArraysPkg::Reduce));
        functions.insert("reverse".into(), Arrays(ArraysPkg::Reverse));
        functions.insert("to_array".into(), Tools(ToolsPkg::ToArray));
        functions.insert("to_table".into(), Tools(ToolsPkg::ToTable));
        Self::add_common_functions(functions)
    }
    
    fn array_functions() -> HashMap<String, PackageOps> {
        let mut functions = HashMap::new();
        functions.insert("join".into(), Strings(StringsPkg::Join));
        functions.insert("pop".into(), Arrays(ArraysPkg::Pop));
        functions.insert("push".into(), Arrays(ArraysPkg::Push));
        Self::add_transformation_functions(functions)
    }

    fn blobs_functions() -> HashMap<String, PackageOps> {
        let mut functions = HashMap::new();
        functions.insert("len".into(), Arrays(ArraysPkg::Len));
        Self::add_common_functions(functions)
    }

    fn bytestring_functions() -> HashMap<String, PackageOps> {
        let mut functions = HashMap::new();
        functions.insert("base62_decode".into(), Utils(UtilsPkg::Base62Decode));
        functions.insert("base62_encode".into(), Utils(UtilsPkg::Base62Encode));
        functions.insert("base64_decode".into(), Utils(UtilsPkg::Base64Decode));
        functions.insert("base64_encode".into(), Utils(UtilsPkg::Base64Encode));
        functions.insert("gunzip".into(), Utils(UtilsPkg::Gunzip));
        functions.insert("gzip".into(), Utils(UtilsPkg::Gzip));
        functions.insert("hex".into(), Utils(UtilsPkg::Hex));
        functions.insert("md5".into(), Utils(UtilsPkg::MD5));
        Self::add_transformation_functions(functions)
    }

    fn char_functions() -> HashMap<String, PackageOps> {
        let mut functions = HashMap::new();
        functions.insert("len".into(), Strings(StringsPkg::Len));
        Self::add_common_functions(functions)
    }

    fn datetime_functions() -> HashMap<String, PackageOps> {
        let mut functions = HashMap::new();
        functions.insert("day".into(), Cal(CalPkg::DateDay));
        functions.insert("hour12".into(), Cal(CalPkg::DateHour12));
        functions.insert("hour24".into(), Cal(CalPkg::DateHour24));
        functions.insert("is_leapyear".into(), Cal(CalPkg::IsLeapYear));
        functions.insert("is_weekday".into(), Cal(CalPkg::IsWeekday));
        functions.insert("is_weekend".into(), Cal(CalPkg::IsWeekend));
        functions.insert("minus".into(), Cal(CalPkg::Minus));
        functions.insert("minute".into(), Cal(CalPkg::DateMinute));
        functions.insert("month".into(), Cal(CalPkg::DateMonth));
        functions.insert("plus".into(), Cal(CalPkg::Plus));
        functions.insert("second".into(), Cal(CalPkg::DateSecond));
        functions.insert("to_f64".into(), Utils(UtilsPkg::ToF64));
        functions.insert("to_i64".into(), Utils(UtilsPkg::ToI64));
        functions.insert("to_millis".into(), Cal(CalPkg::ToMillis));
        functions.insert("to_u64".into(), Utils(UtilsPkg::ToU64));
        functions.insert("to_u128".into(), Utils(UtilsPkg::ToU128));
        functions.insert("year".into(), Cal(CalPkg::DateYear));
        Self::add_common_functions(functions)
    }

    fn error_functions() -> HashMap<String, PackageOps> {
        let mut functions = HashMap::new();
        Self::add_common_functions(functions)
    }

    fn function_functions() -> HashMap<String, PackageOps> {
        let mut functions = HashMap::new();
        Self::add_common_functions(functions)
    }

    fn number_functions() -> HashMap<String, PackageOps> {
        let mut functions = HashMap::new();
        functions.insert("day".into(), Durations(DurationsPkg::Days));
        functions.insert("days".into(), Durations(DurationsPkg::Days));
        functions.insert("hour".into(), Durations(DurationsPkg::Hours));
        functions.insert("hours".into(), Durations(DurationsPkg::Hours));
        functions.insert("millis".into(), Durations(DurationsPkg::Millis));
        functions.insert("msec".into(), Durations(DurationsPkg::Millis));
        functions.insert("msecs".into(), Durations(DurationsPkg::Millis));
        functions.insert("minute".into(), Durations(DurationsPkg::Minutes));
        functions.insert("minutes".into(), Durations(DurationsPkg::Minutes));
        functions.insert("second".into(), Durations(DurationsPkg::Seconds));
        functions.insert("seconds".into(), Durations(DurationsPkg::Seconds));
        functions.insert("abs".into(), Math(MathPkg::Abs));
        functions.insert("ceil".into(), Math(MathPkg::Ceil));
        functions.insert("floor".into(), Math(MathPkg::Floor));
        functions.insert("is_leapyear".into(), Cal(CalPkg::IsLeapYear));
        functions.insert("max".into(), Math(MathPkg::Max));
        functions.insert("min".into(), Math(MathPkg::Min));
        functions.insert("pow".into(), Math(MathPkg::Pow));
        functions.insert("round".into(), Math(MathPkg::Round));
        functions.insert("sqrt".into(), Math(MathPkg::Sqrt));
        functions.insert("superscript".into(), Strings(StringsPkg::SuperScript));
        functions.insert("serve".into(), Www(WwwPkg::HttpServe));
        functions.insert("to_date".into(), Utils(UtilsPkg::ToDate));
        functions.insert("to_f64".into(), Utils(UtilsPkg::ToF64));
        functions.insert("to_i64".into(), Utils(UtilsPkg::ToI64));
        functions.insert("to_i128".into(), Utils(UtilsPkg::ToI128));
        functions.insert("to_u8".into(), Utils(UtilsPkg::ToU8));
        functions.insert("to_u64".into(), Utils(UtilsPkg::ToU64));
        functions.insert("to_u128".into(), Utils(UtilsPkg::ToU128));
        Self::add_common_functions(functions)
    }

    fn string_functions() -> HashMap<String, PackageOps> {
        let mut functions = HashMap::new();
        functions.insert("base62_decode".into(), Utils(UtilsPkg::Base62Decode));
        functions.insert("base62_encode".into(), Utils(UtilsPkg::Base62Encode));
        functions.insert("base64_decode".into(), Utils(UtilsPkg::Base64Decode));
        functions.insert("base64_encode".into(), Utils(UtilsPkg::Base64Encode));
        functions.insert("ends_with".into(), Strings(StringsPkg::EndsWith));
        functions.insert("format".into(), Strings(StringsPkg::Format));
        functions.insert("gunzip".into(), Utils(UtilsPkg::Gunzip));
        functions.insert("gzip".into(), Utils(UtilsPkg::Gzip));
        functions.insert("hex".into(), Utils(UtilsPkg::Hex));
        functions.insert("index_of".into(), Strings(StringsPkg::IndexOf));
        functions.insert("join".into(), Strings(StringsPkg::Join));
        functions.insert("left".into(), Strings(StringsPkg::Left));
        functions.insert("md5".into(), Utils(UtilsPkg::MD5));
        functions.insert("right".into(), Strings(StringsPkg::Right));
        functions.insert("split".into(), Strings(StringsPkg::Split));
        functions.insert("starts_with".into(), Strings(StringsPkg::StartsWith));
        functions.insert("strip_margin".into(), Strings(StringsPkg::StripMargin));
        functions.insert("substring".into(), Strings(StringsPkg::Substring));
        functions.insert("to_lowercase".into(), Strings(StringsPkg::ToLowercase));
        functions.insert("to_uppercase".into(), Strings(StringsPkg::ToUppercase));
        functions.insert("trim".into(), Strings(StringsPkg::Trim));
        functions.insert("url_decode".into(), Www(WwwPkg::URLDecode));
        functions.insert("url_encode".into(), Www(WwwPkg::URLEncode));
        Self::add_transformation_functions(functions)
    }

    fn structure_functions() -> HashMap<String, PackageOps> {
        let mut functions = HashMap::new();
        functions.insert("describe".into(), Tools(ToolsPkg::Describe));
        functions.insert("keys".into(), Tools(ToolsPkg::Keys));
        functions.insert("to_csv".into(), Tools(ToolsPkg::ToCSV));
        functions.insert("to_json".into(), Tools(ToolsPkg::ToJSON));
        Self::add_transformation_functions(functions)
    }

    fn table_functions() -> HashMap<String, PackageOps> {
        let mut functions = HashMap::new();
        functions.insert("journal".into(), Nsd(NsdPkg::Journal));
        functions.insert("replay".into(), Nsd(NsdPkg::Replay));
        functions.insert("resize".into(), Nsd(NsdPkg::Resize));
        functions.insert("compact".into(), Tools(ToolsPkg::Compact));
        functions.insert("describe".into(), Tools(ToolsPkg::Describe));
        functions.insert("fetch".into(), Tools(ToolsPkg::Fetch));
        functions.insert("keys".into(), Tools(ToolsPkg::Keys));
        functions.insert("latest".into(), Tools(ToolsPkg::Latest));
        functions.insert("pop".into(), Tools(ToolsPkg::Pop));
        functions.insert("push".into(), Tools(ToolsPkg::Push));
        functions.insert("reduce".into(), Arrays(ArraysPkg::Reduce));
        functions.insert("scan".into(), Tools(ToolsPkg::Scan));
        functions.insert("save".into(), Nsd(NsdPkg::Save));
        functions.insert("shuffle".into(), Tools(ToolsPkg::Shuffle));
        functions.insert("to_csv".into(), Tools(ToolsPkg::ToCSV));
        functions.insert("to_json".into(), Tools(ToolsPkg::ToJSON));
        Self::add_transformation_functions(functions)
    }

    fn tuple_functions() -> HashMap<String, PackageOps> {
        let mut functions = HashMap::new();
        functions.insert("join".into(), Strings(StringsPkg::Join));
        Self::add_transformation_functions(functions)
    }

    fn uuid_functions() -> HashMap<String, PackageOps> {
        let mut functions = HashMap::new();
        functions.insert("to_array".into(), Tools(ToolsPkg::ToArray));
        functions.insert("send_bytes".into(), Www(WwwPkg::WsSendBytes));
        functions.insert("send_text".into(), Www(WwwPkg::WsSendText));
        Self::add_common_functions(functions)
    }
}

/// Unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_types::DataType::TupleType;
    use crate::typed_values::TypedValue::{StringValue, TupleValue};

    #[test]
    fn test_lookup_by_name() {
        let builtins = Builtins::new();
        let result = builtins.lookup_by_name("Array", "to_table");
        assert_eq!(result, Some(Tools(ToolsPkg::ToTable)));
    }

    #[test]
    fn test_lookup_by_type() {
        let builtins = Builtins::new();
        let data_type = TupleType(vec![]);
        let result = builtins.lookup_by_type(&data_type, "to_string");
        assert_eq!(result, Some(Strings(StringsPkg::ToString)));
    }

    #[test]
    fn test_lookup_by_value() {
        let builtins = Builtins::new();
        let host = TupleValue(vec![
            StringValue("a".into()),
            StringValue("b".into()),
            StringValue("c".into()),
        ]);
        let result = builtins.lookup_by_value(&host, "to_string");
        assert_eq!(result, Some(Strings(StringsPkg::ToString)));
    }
    
}