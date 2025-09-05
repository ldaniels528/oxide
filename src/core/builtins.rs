#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
//  Builtins - internal package manager
////////////////////////////////////////////////////////////////////

use crate::data_types::DataType;
use crate::packages::PackageOps::*;
use crate::packages::*;
use crate::typed_values::TypedValue;
use serde::{Deserialize, Serialize};

use im::HashMap;
use once_cell::sync::Lazy;

/// Builds a mapping of the class name to a mapping of built-in functions
pub static BUILTIN_OPS: Lazy<HashMap<String, HashMap<String, PackageOps>>> = Lazy::new(|| {
    HashMap::new()
        .update("Array".into(), to_hm(Builtins::array_functions()))
        .update("BLOB".into(), to_hm(Builtins::blob_functions()))
        .update("BLOBStore".into(), to_hm(Builtins::blobstore_functions()))
        .update("Boolean".into(), to_hm(Builtins::boolean_functions()))
        .update("Bytes".into(), to_hm(Builtins::bytestring_functions()))
        .update("Char".into(), to_hm(Builtins::char_functions()))
        .update("DateTime".into(), to_hm(Builtins::datetime_functions()))
        .update("Enum".into(), to_hm(Builtins::enum_functions()))
        .update("Error".into(), to_hm(Builtins::error_functions()))
        .update("Function".into(), to_hm(Builtins::function_functions()))
        .update("Number".into(), to_hm(Builtins::number_functions()))
        .update("Runtime".into(), to_hm(Builtins::runtime_type_functions()))
        .update("String".into(), to_hm(Builtins::string_functions()))
        .update("Struct".into(), to_hm(Builtins::structure_functions()))
        .update("Table".into(), to_hm(Builtins::table_functions()))
        .update("Tuple".into(), to_hm(Builtins::tuple_functions()))
        .update("UUID".into(), to_hm(Builtins::uuid_functions()))
        .update("WebSocket".into(), to_hm(Builtins::websocket_functions()))
});

/// Builtins - internal package manager
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Builtins;

impl Builtins {
    
    ////////////////////////////////////////////////////////////////////
    //  Instance methods
    ////////////////////////////////////////////////////////////////////
    
    pub fn lookup_by_name(type_name: &str, name: &str) -> Option<PackageOps> {
        BUILTIN_OPS.get(type_name)
            .and_then(|map| map.get(name)).cloned()
    }

    pub fn lookup_by_type(data_type: &DataType, name: &str) -> Option<PackageOps> {
        let type_name = data_type.get_name();
        Self::lookup_by_name(type_name.as_str(), name)
    }

    pub fn lookup_by_value(host: &TypedValue, name: &str) -> Option<PackageOps> {
        Self::lookup_by_type(&host.get_type(), name)
    }
    
    ////////////////////////////////////////////////////////////////////
    //  Internal
    ////////////////////////////////////////////////////////////////////

    fn add_common_functions(mut functions: Vec<PackageOps>) -> Vec<PackageOps> {
        functions.push(Utils(UtilsPkg::GetType));
        functions.push(Utils(UtilsPkg::IsA));
        functions.push(Utils(UtilsPkg::To));
        functions
    }

    fn add_transformation_functions(mut functions: Vec<PackageOps>) -> Vec<PackageOps> {
        functions.push(Collections(CollectionsPkg::Contains));
        functions.push(Collections(CollectionsPkg::Filter));
        functions.push(Collections(CollectionsPkg::Head));
        functions.push(Collections(CollectionsPkg::IsEmpty));
        functions.push(Collections(CollectionsPkg::Len));
        functions.push(Collections(CollectionsPkg::Map));
        functions.push(Collections(CollectionsPkg::Reduce));
        functions.push(Collections(CollectionsPkg::Reverse));
        functions.push(Collections(CollectionsPkg::Tail));
        Self::add_common_functions(functions)
    }

    fn array_functions() -> Vec<PackageOps> {
        let mut functions = Self::add_transformation_functions(Vec::new());
        functions.push(Strings(StringsPkg::Join));
        functions.push(Collections(CollectionsPkg::Pop));
        functions.push(Collections(CollectionsPkg::Push));
        functions
    }

    fn blob_functions() -> Vec<PackageOps> {
        let mut functions = Self::add_common_functions(Vec::new());
        functions.push(Collections(CollectionsPkg::Len));
        functions
    }

    fn blobstore_functions() -> Vec<PackageOps> {
        let mut functions = Self::add_common_functions(Vec::new());
        functions.push(Blobs(BlobsPkg::Append));
        functions.push(Blobs(BlobsPkg::Close));
        functions.push(Blobs(BlobsPkg::Create));
        functions.push(Blobs(BlobsPkg::Entries));
        functions.push(Blobs(BlobsPkg::Len));
        functions.push(Blobs(BlobsPkg::Load));
        functions.push(Blobs(BlobsPkg::Read));
        functions.push(Blobs(BlobsPkg::Truncate));
        functions.push(Blobs(BlobsPkg::Update));
        functions
    }

    fn boolean_functions() -> Vec<PackageOps> {
        Self::add_common_functions(Vec::new())
    }

    fn bytestring_functions() -> Vec<PackageOps> {
        let mut functions = Self::add_transformation_functions(Vec::new());
        functions.push(Utils(UtilsPkg::Base36Decode));
        functions.push(Utils(UtilsPkg::Base36Encode));
        functions.push(Utils(UtilsPkg::Base62Decode));
        functions.push(Utils(UtilsPkg::Base62Encode));
        functions.push(Utils(UtilsPkg::Base64Decode));
        functions.push(Utils(UtilsPkg::Base64Encode));
        functions.push(Utils(UtilsPkg::Gunzip));
        functions.push(Utils(UtilsPkg::Gzip));
        functions.push(Utils(UtilsPkg::Hex));
        functions.push(Utils(UtilsPkg::MD5));
        functions
    }

    fn char_functions() -> Vec<PackageOps> {
        let mut functions = Self::add_common_functions(Vec::new());
        functions.push(Chars(CharsPkg::Lower));
        functions.push(Chars(CharsPkg::Upper));
        functions
    }

    fn datetime_functions() -> Vec<PackageOps> {
        let mut functions = Self::add_common_functions(Vec::new());
        functions.push(Dates(DatesPkg::DateDay));
        functions.push(Dates(DatesPkg::DateHour12));
        functions.push(Dates(DatesPkg::DateHour24));
        functions.push(Dates(DatesPkg::DateMinute));
        functions.push(Dates(DatesPkg::DateMonth));
        functions.push(Dates(DatesPkg::DateSecond));
        functions.push(Dates(DatesPkg::DateYear));
        functions.push(Dates(DatesPkg::IsLeapYear));
        functions.push(Dates(DatesPkg::IsWeekday));
        functions.push(Dates(DatesPkg::IsWeekend));
        functions.push(Dates(DatesPkg::DateMinus));
        functions.push(Dates(DatesPkg::DatePlus));
        functions.push(Dates(DatesPkg::ToMillis));
        functions
    }

    fn enum_functions() -> Vec<PackageOps> {
        Self::add_common_functions(Vec::new())
    }

    fn error_functions() -> Vec<PackageOps> {
        Self::add_common_functions(Vec::new())
    }

    fn function_functions() -> Vec<PackageOps> {
        Self::add_common_functions(Vec::new())
    }

    fn number_functions() -> Vec<PackageOps> {
        let mut functions = Self::add_common_functions(Vec::new());
        functions.push(Math(MathPkg::Abs));
        functions.push(Utils(UtilsPkg::Base36Encode));
        functions.push(Utils(UtilsPkg::Binary));
        functions.push(Math(MathPkg::Ceil));
        functions.push(Durations(DurationsPkg::Days));
        functions.push(Math(MathPkg::Floor));
        functions.push(Durations(DurationsPkg::Hours));
        functions.push(Utils(UtilsPkg::Hex));
        functions.push(Dates(DatesPkg::IsLeapYear));
        functions.push(Math(MathPkg::Max));
        functions.push(Durations(DurationsPkg::Millis));
        functions.push(Math(MathPkg::Min));
        functions.push(Durations(DurationsPkg::Minutes));
        functions.push(Utils(UtilsPkg::Octal));
        functions.push(Math(MathPkg::Pow));
        functions.push(Math(MathPkg::Round));
        functions.push(Durations(DurationsPkg::Seconds));
        functions.push(Math(MathPkg::Sqrt));
        functions.push(Strings(StringsPkg::SuperScript));
        functions
    }

    fn runtime_type_functions() -> Vec<PackageOps> {
        Self::add_common_functions(Vec::new())
    }

    fn string_functions() -> Vec<PackageOps> {
        let mut functions = Self::add_transformation_functions(Vec::new());
        functions.push(Utils(UtilsPkg::Base36Decode));
        functions.push(Utils(UtilsPkg::Base62Decode));
        functions.push(Utils(UtilsPkg::Base62Encode));
        functions.push(Utils(UtilsPkg::Base64Decode));
        functions.push(Utils(UtilsPkg::Base64Encode));
        functions.push(Oxide(OxidePkg::Compile));
        functions.push(Strings(StringsPkg::EndsWith));
        functions.push(Oxide(OxidePkg::Eval));
        functions.push(Strings(StringsPkg::Format));
        functions.push(Utils(UtilsPkg::Gunzip));
        functions.push(Utils(UtilsPkg::Gzip));
        functions.push(Utils(UtilsPkg::Hex));
        functions.push(Oxide(OxidePkg::Inspect));
        functions.push(Strings(StringsPkg::Join));
        functions.push(Strings(StringsPkg::Left));
        functions.push(Utils(UtilsPkg::MD5));
        functions.push(Strings(StringsPkg::Position));
        functions.push(Oxide(OxidePkg::Printf));
        functions.push(Oxide(OxidePkg::Println));
        functions.push(Strings(StringsPkg::Right));
        functions.push(Strings(StringsPkg::Split));
        functions.push(Strings(StringsPkg::Sprintf));
        functions.push(Strings(StringsPkg::StartsWith));
        functions.push(Strings(StringsPkg::StripMargin));
        functions.push(Strings(StringsPkg::Substring));
        functions.push(Strings(StringsPkg::ToLowercase));
        functions.push(Strings(StringsPkg::ToUppercase));
        functions.push(Strings(StringsPkg::Trim));
        functions.push(Www(WwwPkg::URLDecode));
        functions.push(Www(WwwPkg::URLEncode));
        functions
    }

    fn structure_functions() -> Vec<PackageOps> {
        let mut functions = Self::add_transformation_functions(Vec::new());
        functions.push(Tables(TablesPkg::Describe));
        functions.push(Collections(CollectionsPkg::Keys));
        functions.push(Tables(TablesPkg::ToCSV));
        functions.push(Tables(TablesPkg::ToJSON));
        functions
    }

    fn table_functions() -> Vec<PackageOps> {
        let mut functions = Self::add_transformation_functions(Vec::new());
        functions.push(Tables(TablesPkg::Compact));
        functions.push(Tables(TablesPkg::Describe));
        functions.push(Tables(TablesPkg::Journal));
        functions.push(Collections(CollectionsPkg::Keys));
        functions.push(Tables(TablesPkg::Latest));
        functions.push(Tables(TablesPkg::Pop));
        functions.push(Tables(TablesPkg::PullCell));
        functions.push(Tables(TablesPkg::PullColumn));
        functions.push(Tables(TablesPkg::PullRow));
        functions.push(Tables(TablesPkg::Push));
        functions.push(Tables(TablesPkg::RecordSize));
        functions.push(Tables(TablesPkg::Reduce));
        functions.push(Tables(TablesPkg::Replay));
        functions.push(Tables(TablesPkg::Resize));
        functions.push(Tables(TablesPkg::Save));
        functions.push(Tables(TablesPkg::SaveAs));
        functions.push(Tables(TablesPkg::Scan));
        functions.push(Tables(TablesPkg::Shuffle));
        functions.push(Tables(TablesPkg::ToCSV));
        functions.push(Tables(TablesPkg::ToJSON));
        functions
    }

    fn tuple_functions() -> Vec<PackageOps> {
        let mut functions = Self::add_transformation_functions(Vec::new());
        functions.push(Strings(StringsPkg::Join));
        functions
    }

    fn uuid_functions() -> Vec<PackageOps> {
        let mut functions = Self::add_common_functions(Vec::new());
        functions.push(Utils(UtilsPkg::Hex));
        functions
    }

    fn websocket_functions() -> Vec<PackageOps> {
        let mut functions = Self::add_common_functions(Vec::new());
        functions.push(Www(WwwPkg::WsClose));
        functions.push(Www(WwwPkg::WsSendBytes));
        functions.push(Www(WwwPkg::WsSendText));
        functions
    }
}

fn to_hm(ops: Vec<PackageOps>) -> HashMap<String, PackageOps> {
    let mut hm = HashMap::new();
    for op in ops {
        hm = hm.update(op.get_name().into(), op);
    }
    hm
}

/// Unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_types::DataType::TupleType;
    use crate::typed_values::TypedValue::{StringValue, TupleValue};

    #[test]
    fn test_lookup_by_name() {
        let result = Builtins::lookup_by_name("Array", "to");
        assert_eq!(result, Some(Utils(UtilsPkg::To)));
    }

    #[test]
    fn test_lookup_by_type() {
        let data_type = TupleType(vec![]);
        let result = Builtins::lookup_by_type(&data_type, "map");
        assert_eq!(result, Some(Collections(CollectionsPkg::Map)));
    }

    #[test]
    fn test_lookup_by_value() {
        let host = TupleValue(vec![
            StringValue("a".into()),
            StringValue("b".into()),
            StringValue("c".into()),
        ]);
        let result = Builtins::lookup_by_value(&host, "join");
        assert_eq!(result, Some(Strings(StringsPkg::Join)));
    }

    /// Package "Array" tests
    #[cfg(test)]
    mod array_tests {
        use crate::test_util::*;

        #[test]
        fn test_array_filter() {
            verify_exact_code_and_inferred_type(r#"
                [1, 3, 4, 8, 9, 12, 13]::filter(n -> (n % 2) == 1)
           "#, "[1, 3, 9, 13]", "Array()")
        }

        #[test]
        fn test_array_is_a() {
            verify_exact_code_and_inferred_type(r#"
                ['a', 'b', 'c']::is_a(Array)
            "#, "true", "Boolean");
        }

        #[test]
        fn test_array_is_empty_false() {
            verify_exact_code_and_inferred_type(r#"
                [3, 5, 7, 9]::is_empty()
           "#, "false", "Boolean");
        }

        #[test]
        fn test_array_is_empty_true() {
            verify_exact_code_and_inferred_type(r#"
                []::is_empty()
           "#, "true", "Boolean");
        }

        #[test]
        fn test_array_join() {
            verify_exact_code_and_inferred_type(r#"
                ["1", "5", "9", "13"]::join(", ")
            "#, "\"1, 5, 9, 13\"", "String");
        }

        #[test]
        fn test_array_len() {
            verify_exact_code_and_inferred_type(r#"
                [3, 5, 7, 9]::len()
           "#, "4", "i64");
        }

        #[test]
        fn test_array_map() {
            verify_exact_code_and_inferred_type(
                "[1, 2, 3]::map(n -> n * 2)",
                "[2, 4, 6]", "Array()");
        }

        #[test]
        fn test_array_pop() {
            verify_exact_code_and_inferred_type(r#"
                stocks = ["ABC", "BOOM", "JET", "DEX"]
                stocks::pop()
            "#, r#"["ABC", "BOOM", "JET"]"#, "Array()");
        }

        #[test]
        fn test_array_push() {
            verify_exact_code_and_inferred_type(r#"
                stocks = ["ABC", "BOOM", "JET"]
                stocks::push("DEX")
            "#, r#"["ABC", "BOOM", "JET", "DEX"]"#, "Array()");
        }

        #[test]
        fn test_array_reduce() {
            verify_exact_code_and_inferred_type(r#"
                 numbers = [1, 2, 3, 4, 5]
                 numbers::reduce(0, (a, b) -> a + b)
            "#, "15", "");
        }

        #[test]
        fn test_array_reverse() {
            verify_exact_code_and_inferred_type(r#"
                ['cat', 'dog', 'ferret', 'mouse']::reverse()
            "#, r#"["mouse", "ferret", "dog", "cat"]"#, "Array()")
        }

        #[test]
        fn test_array_tail() {
            verify_exact_code_and_inferred_type(r#"
                ['cat', 'dog', 'ferret', 'mouse']::tail()
            "#, r#"["dog", "ferret", "mouse"]"#, "Array()")
        }

        #[actix::test]
        async fn test_array_tuples_to_table() {
            verify_exact_table_async_and_sync(r#"
                stocks = [
                    ("ABC", "AMEX", 12.49),
                    ("BOOM", "NYSE", 56.88),
                    ("JET", "NASDAQ", 32.12),
                    ("DEX", "OTC_BB", 0.0086)
                ]
                stocks::to(Table)
            "#, vec![
                "|-----------------------------|",
                "| id | t0   | t1     | t2     |",
                "|-----------------------------|",
                "| 0  | ABC  | AMEX   | 12.49  |",
                "| 1  | BOOM | NYSE   | 56.88  |",
                "| 2  | JET  | NASDAQ | 32.12  |",
                "| 3  | DEX  | OTC_BB | 0.0086 |",
                "|-----------------------------|"]).await;
        }

        #[test]
        fn test_type_of_array_bool() {
            verify_exact_code_and_inferred_type(
                "[true, false]::get_type()",
                "Array(Boolean, 2)", "Array(Boolean, 2)");
        }

        #[test]
        fn test_type_of_array_i64() {
            verify_exact_code_and_inferred_type(
                "[12, 76, 444]::get_type()",
                "Array(i64, 3)", "Array(i64, 3)");
        }

        #[test]
        fn test_type_of_array_str() {
            verify_exact_code_and_inferred_type(
                "['ciao', 'hello', 'world']::get_type()",
                "Array(String(5), 3)", "Array(String(5), 3)");
        }

        #[test]
        fn test_type_of_array_f64() {
            verify_exact_code_and_inferred_type(
                "[12.5, 123.2, 76.78]::get_type()",
                "Array(f64, 3)", "Array(f64, 3)");
        }
    }

    /// Package "BLOB" tests
    #[cfg(test)]
    mod blob_tests {
        use crate::test_util::*;

        #[actix::test]
        async fn test_blob_append() {
            verify_exact_code_async(r#"
                let bs = blobs::create("builtins.blob.append")
                let id = bs::append("Hello World")
                bs::read(id)
            "#, "\"Hello World\"").await;
        }

        #[test]
        fn test_blob_append_blocking() {
            verify_exact_code(r#"
                let bs = blobs::create("builtins.blob.append_blocking")
                let id = bs::append("Hello World")
                let result = bs::read(id)
                blobs::close(id)
                result
            "#, "\"Hello World\"");
        }

        #[actix::test]
        async fn test_blob_entries() {
            verify_exact_table_async(r#"
                let bs = blobs::create("builtins.blob.entries")
                bs::append("Hello World")
                bs::append("The little brown fox")
                bs::append("Goodbye World")
                deselect blob_id from bs::entries()
            "#, vec![
                "|--------------------------------|",
                "| id | offset | allocated | used |",
                "|--------------------------------|",
                "| 0  | 0      | 70        | 63   |",
                "| 1  | 70     | 82        | 72   |",
                "| 2  | 152    | 73        | 65   |",
                "|--------------------------------|"]).await;
        }

        #[test]
        fn test_blob_entries_blocking() {
            verify_exact_table(r#"
                let bs = blobs::create("builtins.blob.entries_blocking")
                bs::append("Hello World")
                bs::append("The little brown fox")
                bs::append("Goodbye World")
                deselect blob_id from bs::entries()
            "#, vec![
                "|--------------------------------|",
                "| id | offset | allocated | used |",
                "|--------------------------------|",
                "| 0  | 0      | 70        | 63   |",
                "| 1  | 70     | 82        | 72   |",
                "| 2  | 152    | 73        | 65   |",
                "|--------------------------------|"]);
        }

        #[actix::test]
        async fn test_blob_len() {
            verify_exact_code_async(r#"
                let bs = blobs::create("builtins.blob.len")
                bs::append("Why did the chicken cross the road?")
                bs::append("To get to the other side!")
                bs::len()
            "#, "191").await;
        }

        #[test]
        fn test_blob_len_blocking() {
            verify_exact_code_and_inferred_type(r#"
                let bs = blobs::create("builtins.blob.len_blocking")
                bs::append("Why did the chicken cross the road?")
                bs::len()
            "#, "102", "i64");
        }

        #[test]
        fn test_blob_update() {
            verify_exact_code(r#"
                let bs = blobs::load("builtins.blob.update")
                bs::truncate()
                let id0 = bs::append("Hello World")
                let id1 = bs::update(id0, "The brown fox")
                bs::read(id1)
            "#, "\"The brown fox\"");
        }
    }

    /// Package "Boolean" tests
    #[cfg(test)]
    mod boolean_tests {
        use crate::test_util::verify_exact_code_async_and_sync;

        #[actix::test]
        async fn test_boolean_get_type_false() {
            verify_exact_code_async_and_sync("false::get_type()", "Boolean").await;
        }

        #[actix::test]
        async fn test_boolean_get_type_true() {
            verify_exact_code_async_and_sync("true::get_type()", "Boolean").await;
        }
    }

    /// Package "Bytes" tests
    #[cfg(test)]
    mod bytes_tests {
        use crate::test_util::*;

        #[test]
        fn test_bytes_to_char() {
            verify_exact_code_and_inferred_type(r#"
                0Bf09f9191::to(Char)
            "#, "'👑'", "Char")
        }

        #[test]
        fn test_bytes_to_f64() {
            verify_exact_code_and_inferred_type(r#"
                0B40884f72e48e8a72::to(f64)
            "#, "777.9311", "f64")
        }

        #[test]
        fn test_bytes_to_i64() {
            verify_exact_code_and_inferred_type(r#"
                0B000000000016ecfc::to(i64)
            "#, "1502460", "i64")
        }

        #[test]
        fn test_bytes_is_a() {
            verify_exact_code_and_inferred_type(r#"
                0B000000000016ecfc::is_a(Bytes)
            "#, "true", "Boolean");
        }

        #[test]
        fn test_bytes_to_string() {
            verify_exact_code_and_inferred_type(r#"
                0B616e20656e636f646564206d657373616765::to(String)
            "#, r#""an encoded message""#, "String")
        }

        #[test]
        fn test_bytes_to_u64() {
            verify_exact_code_and_inferred_type(r#"
                0B000000000016ecfc::to(u64)
            "#, "1502460", "u64")
        }

        #[test]
        fn test_bytes_to_u8() {
            verify_exact_code_and_inferred_type(r#"
                0B7f::to(u8)
            "#, "0x7f", "u8")
        }

        #[test]
        fn test_bytes_to_uuid() {
            verify_exact_code_and_inferred_type(r#"
                0Bb11db7721dbd4839be068392f88c1924::to(UUID)
            "#, "b11db772-1dbd-4839-be06-8392f88c1924", "UUID")
        }
    }

    /// Package "Char" tests
    #[cfg(test)]
    mod char_tests {
        use crate::test_util::*;

        #[test]
        fn test_char_is_a() {
            verify_exact_code_and_inferred_type(r#"
                'Z'::is_a(Char)
            "#, "true", "Boolean");
        }

        #[test]
        fn test_char_lower() {
            verify_exact_code_and_inferred_type(r#"
                'Z'::lower()
            "#, "'z'", "Char");
        }

        #[test]
        fn test_char_to_bytes() {
            verify_exact_code_and_inferred_type(r#"
                '$'::to(Bytes)
            "#, "0B24", "Bytes");
        }

        #[test]
        fn test_char_to_string() {
            verify_exact_code_and_inferred_type(r#"
                'A'::to(String)
            "#, "\"A\"", "String");
        }

        #[test]
        fn test_char_upper() {
            verify_exact_code_and_inferred_type(r#"
                'a'::upper()
            "#, "'A'", "Char");
        }

        #[test]
        fn test_unicode_char_to_u64_to_unicode_char() {
            verify_exact_code_and_inferred_type(r#"
                '🔴'::to(u64)::to(Char)
            "#, "'🔴'", "Char");
        }

        #[test]
        fn test_unicode_char_to_bytes() {
            verify_exact_code_and_inferred_type(r#"
                '🔥'::to(Bytes)
            "#, "0Bf09f94a5", "Bytes");
        }

        #[test]
        fn test_unicode_char_to_u64() {
            verify_exact_code_and_inferred_type(r#"
                '🔴'::to(u64)
            "#, "3029639152", "u64");
        }

        #[actix::test]
        async fn test_unicode_char_to_string() {
            verify_exact_code_async_and_sync(r#"
                '🎁'::to(String)
            "#, "\"🎁\"").await;
        }
    }

    /// Package "common to all types" tests
    #[cfg(test)]
    mod common_tests {
        use crate::test_util::*;

        #[actix::test]
        async fn test_f64_is_a_f64() {
            verify_exact_code_async_and_sync("88.99::is_a(f64)", "true").await;
        }

        #[actix::test]
        async fn test_f64_is_a_number() {
            verify_exact_code_async_and_sync("97.23::is_a(Number)", "true").await;
        }

        #[actix::test]
        async fn test_f64_is_not_i64() {
            verify_exact_code_async_and_sync("679.13::is_a(i64)", "false").await;
        }

        #[actix::test]
        async fn test_i64_is_a_i64() {
            verify_exact_code_async_and_sync("55::is_a(i64)", "true").await;
        }

        #[actix::test]
        async fn test_i64_is_a_number() {
            verify_exact_code_async_and_sync("127::is_a(Number)", "true").await;
        }

        #[actix::test]
        async fn test_i64_is_not_date() {
            verify_exact_code_async_and_sync("8899::is_a(DateTime)", "false").await;
        }

        #[actix::test]
        async fn test_i64_is_not_f64() {
            verify_exact_code_async_and_sync("8899::is_a(f64)", "false").await;
        }

        #[actix::test]
        async fn test_i64_is_not_string() {
            verify_exact_code_async_and_sync("8899::is_a(String)", "false").await;
        }
    }

    /// Package "Datetime" tests
    #[cfg(test)]
    mod datetime_tests {
        use crate::test_util::verify_exact_code;

        #[test]
        fn test_datetime_day() {
            verify_exact_code(r#"
                2025-07-06T20:19:26.930Z::day
            "#, "6");
        }

        #[test]
        fn test_datetime_get_type() {
            verify_exact_code("2025-07-06T21:00:29.412Z::get_type()", "DateTime");
        }

        #[test]
        fn test_datetime_hour24() {
            verify_exact_code(r#"
                2025-07-06T20:19:26.930Z::hour24
            "#, "13");
        }

        #[test]
        fn test_datetime_hour12() {
            verify_exact_code(r#"
                2025-07-06T20:19:26.930Z::hour12
            "#, "1");
        }

        #[test]
        fn test_datetime_is_a() {
            verify_exact_code(r#"
                2025-07-06T20:19:26.930Z::is_a(DateTime)
            "#, "true");
        }

        #[test]
        fn test_datetime_is_weekend() {
            verify_exact_code(r#"
                2025-07-06T20:19:26.930Z::is_weekend
            "#, "true");
        }

        #[test]
        fn test_datetime_minus() {
            verify_exact_code(r#"
                2025-07-06T20:19:26.930Z::minus(3::days)
            "#, "2025-07-03T20:19:26.930Z");
        }

        #[test]
        fn test_datetime_minute() {
            verify_exact_code(r#"
                2025-07-06T20:19:26.930Z::minute
            "#, "19");
        }

        #[test]
        fn test_datetime_month() {
            verify_exact_code(r#"
                2025-07-06T20:19:26.930Z::month
            "#, "7");
        }

        #[test]
        fn test_datetime_plus() {
            verify_exact_code(r#"
                2025-07-06T20:19:26.930Z::plus(30::days)
            "#, "2025-08-05T20:19:26.930Z");
        }

        #[test]
        fn test_datetime_second() {
            verify_exact_code("2025-07-06T20:19:26.930Z::second", "26");
        }

        #[test]
        fn test_datetime_to_millis() {
            verify_exact_code(r#"
                2025-07-06T20:19:26.930Z::to_millis
            "#, "1751833166930");
        }

        #[test]
        fn test_datetime_year() {
            verify_exact_code("2025-07-06T20:19:26.930Z::year", "2025");
        }
    }

    /// Package "Function" tests
    #[cfg(test)]
    mod function_tests {
        use crate::test_util::verify_exact_code;

        #[test]
        fn test_fn_get_type() {
            verify_exact_code("((a, b) -> a + b)::get_type()", "fn(a, b)");
        }
    }

    /// Package "Number" tests
    #[cfg(test)]
    mod number_tests {
        use super::*;
        use crate::numbers::Numbers::{I128Value, I64Value, U128Value, U64Value, U8Value};
        use crate::test_util::{verify_exact_code, verify_exact_value};
        use crate::typed_values::TypedValue::Number;
        use num_traits::ToPrimitive;

        #[test]
        fn test_number_abs() {
            verify_exact_code("(-81)::abs()", "81")
        }

        #[test]
        fn test_number_base36_encode() {
            verify_exact_code("564684::base36_encode()", "\"C3PO\"")
        }

        #[test]
        fn test_number_ceil() {
            verify_exact_code("7.7::ceil()", "8")
        }

        #[test]
        fn test_number_days() {
            verify_exact_code("3::days", (3 * DAYS).to_string().as_str());
        }

        #[test]
        fn test_number_floor() {
            verify_exact_code("7.7::floor()", "7")
        }

        #[test]
        fn test_number_get_type_f64() {
            verify_exact_code("12.394::get_type()", "f64");
        }

        #[test]
        fn test_number_get_type_i64() {
            verify_exact_code("1234::get_type()", "i64");
        }

        #[test]
        fn test_number_hours_i64() {
            verify_exact_code("8::hours", (8 * HOURS).to_string().as_str());
        }

        #[test]
        fn test_number_hours_f64() {
            verify_exact_code("0.5::hours", (30.0 * MINUTES.to_f64().unwrap()).to_string().as_str());
        }

        #[test]
        fn test_number_is_a() {
            verify_exact_code("500::is_a(i64)", "true");
        }

        #[test]
        fn test_number_max() {
            verify_exact_code("17::max(71)", "71")
        }

        #[test]
        fn test_number_millis() {
            verify_exact_code("1000::millis", (1 * SECONDS).to_string().as_str());
        }

        #[test]
        fn test_number_min() {
            verify_exact_code("17::min(71)", "17")
        }

        #[test]
        fn test_number_minutes() {
            verify_exact_code("30::minutes", (30 * MINUTES).to_string().as_str());
        }

        #[test]
        fn test_number_pow() {
            verify_exact_code("2::pow(3)", "8")
        }

        #[test]
        fn test_number_round() {
            verify_exact_code("17.51::round()", "18")
        }

        #[test]
        fn test_number_seconds() {
            verify_exact_code("20::seconds", (20 * SECONDS).to_string().as_str());
        }

        #[test]
        fn test_number_sqrt() {
            verify_exact_code("25.0::sqrt()", "5")
        }

        #[test]
        fn test_number_superscript() {
            verify_exact_code("123::superscript()", r#""¹²³""#);
        }

        #[test]
        fn test_number_to_char() {
            verify_exact_code("177::to(Char)", "'±'")
        }

        #[test]
        fn test_number_to_date() {
            verify_exact_code("1376438453123::to(DateTime)", "2013-08-14T00:00:53.123Z")
        }

        #[test]
        fn test_number_to_f64() {
            use crate::numbers::Numbers::*;
            verify_exact_value("777_9311::to(f64)", Number(F64Value(7779311.)));
        }

        #[test]
        fn test_number_to_i128() {
            verify_exact_value("1_234_5678_987.43::to(i128)", Number(I128Value(12345678987)));
        }

        #[test]
        fn test_number_to_i64() {
            verify_exact_value("123456789.42::to(i64)", Number(I64Value(123456789)));
        }

        #[test]
        fn test_number_to_u128() {
            verify_exact_value("12789.43::to(u128)",  Number(U128Value(12789)));
        }

        #[test]
        fn test_number_to_u64() {
            verify_exact_value("123456789.42::to(u64)", Number(U64Value(123456789)));
        }

        #[test]
        fn test_number_to_u8() {
            verify_exact_value("123456789.42::to(u8)", Number(U8Value(255)));
        }

        #[test]
        fn test_number_to_string() {
            verify_exact_code("123::to(String)", "\"123\"");
        }
    }

    /// Package "Range" tests
    #[cfg(test)]
    mod range_tests {
        use crate::test_util::verify_exact_code;

        #[test]
        fn test_range_filter_exclusive() {
            verify_exact_code("1..8::filter(n -> (n % 2) == 0)", "[2, 4, 6]");
        }

        #[test]
        fn test_range_filter_inclusive() {
            verify_exact_code("1..=8::filter(n -> (n % 2) == 0)", "[2, 4, 6, 8]");
        }

        #[test]
        fn test_range_is_a() {
            verify_exact_code("1..8::is_a(Array)", "true");
        }

        #[test]
        fn test_range_is_empty_exclusive_false() {
            verify_exact_code("0..4::is_empty()", "false");
        }

        #[test]
        fn test_range_is_empty_exclusive_true() {
            verify_exact_code("0..0::is_empty()", "true");
        }

        #[test]
        fn test_range_is_empty_inclusive_false() {
            verify_exact_code("0..=4::is_empty()", "false");
        }

        #[test]
        fn test_range_is_empty_inclusive_true() {
            verify_exact_code("0..=(-1)::is_empty()", "true");
        }

        #[test]
        fn test_range_len_exclusive() {
            verify_exact_code("1..5::len()", "4")
        }

        #[test]
        fn test_range_len_inclusive() {
            verify_exact_code("1..=5::len()", "5")
        }

        #[test]
        fn test_range_map_exclusive() {
            verify_exact_code("1..4::map(n -> n * 2)", "[2, 4, 6]");
        }

        #[test]
        fn test_range_map_inclusive() {
            verify_exact_code("1..=4::map(n -> n * 2)", "[2, 4, 6, 8]");
        }

        #[test]
        fn test_range_pop_exclusive() {
            verify_exact_code("'a'..'d'::pop()", "['a', 'b']");
        }

        #[test]
        fn test_range_pop_inclusive() {
            verify_exact_code("'a'..='d'::pop()", "['a', 'b', 'c']");
        }

        #[test]
        fn test_range_push_exclusive() {
            verify_exact_code(r#"
                stocks = 'a'..'c'
                stocks::push('d')
            "#, r#"['a', 'b', 'd']"#);
        }

        #[test]
        fn test_range_push_inclusive() {
            verify_exact_code(r#"
                stocks = 'a'..='c'
                stocks::push('d')
            "#, r#"['a', 'b', 'c', 'd']"#);
        }

        #[test]
        fn test_range_reduce_exclusive() {
            verify_exact_code("1..5::reduce(0, (a, b) -> a + b)", "10");
        }

        #[test]
        fn test_range_reduce_inclusive() {
            verify_exact_code("1..=5::reduce(0, (a, b) -> a + b)", "15");
        }

        #[test]
        fn test_range_reverse_exclusive() {
            verify_exact_code("1..5::reverse()", "[4, 3, 2, 1]")
        }

        #[test]
        fn test_range_reverse_inclusive() {
            verify_exact_code("1..=5::reverse()", "[5, 4, 3, 2, 1]")
        }

        #[test]
        fn test_range_to_string_exclusive() {
            verify_exact_code("1..5::to(String)", r#""[1, 2, 3, 4]""#);
        }

        #[test]
        fn test_range_to_string_inclusive() {
            verify_exact_code("1..=5::to(String)", r#""[1, 2, 3, 4, 5]""#);
        }
    }

    /// Package "Runtime" tests
    #[cfg(test)]
    mod runtime_tests {
        use crate::errors::Errors::Exact;
        use crate::test_util::{verify_exact_code, verify_exact_value};
        use crate::typed_values::TypedValue::ErrorValue;

        #[test]
        fn test_kind_get_type() {
            verify_exact_code(r#"
                Cards = Table(face: String(2), suit: String(2))
                Cards::new::get_type()
            "#, "Table(face: String(2), suit: String(2))")
        }

        #[test]
        fn test_null_get_type() {
            verify_exact_code("null::get_type()", "");
        }

        #[test]
        fn test_undefined_get_type() {
            verify_exact_code("undefined::get_type()", "");
        }

        #[test]
        fn test_variable_get_type() {
            verify_exact_value(
                "my_var::get_type()",
                ErrorValue(Exact("Identifier 'my_var' not found".into())));
        }
    }

    /// Package "String" tests
    #[cfg(test)]
    mod string_tests {
        use crate::test_util::{verify_exact_code, verify_exact_table, verify_exact_value};
        use crate::typed_values::TypedValue::{ByteStringValue, StringValue};
        use crate::utils::strip_margin;

        #[test]
        fn test_string_base36_decode() {
            verify_exact_code("'C3PO'::base36_decode", "564684");
        }

        #[test]
        fn test_string_base62_decode() {
            verify_exact_code(r#"
                "73XpUgyMwkGr29M"::base62_decode::to(String)
            "#, "\"\0\0\0\0\0Hello World\"");
        }

        #[test]
        fn test_string_base62_encode() {
            verify_exact_code(r#"
                'Hello World'::base62_encode
            "#, "\"73XpUgyMwkGr29M\"");
        }

        #[test]
        fn test_string_base64_decode() {
            verify_exact_code(r#"
                "SGVsbG8gV29ybGQ="::base64_decode::to(String)
            "#, "\"Hello World\"");
        }

        #[test]
        fn test_string_base64_encode() {
            verify_exact_code(r#"
                'Hello World'::base64_encode()
            "#, "\"SGVsbG8gV29ybGQ=\"");
        }

        #[test]
        fn test_string_compile() {
            verify_exact_code(r#"
                n = 5
                code = "n * n"::compile()
                code()
            "#, "25");
        }

        #[test]
        fn test_string_ends_with_true() {
            verify_exact_code(r#"
                'Hello World'::ends_with('World')
            "#, "true");
        }

        #[test]
        fn test_string_ends_with_false() {
            verify_exact_code(r#"
                'Hello World'::ends_with('Hello')
            "#, "false");
        }

        #[test]
        fn test_string_eval() {
            verify_exact_code(r#"
                a = 'Hello '
                b = 'World'
                "a + b"::eval()
            "#, "\"Hello World\"");
        }

        #[test]
        fn test_string_format() {
            verify_exact_code(r#"
                "This {} the {}"::format("is", "way")
            "#, "\"This is the way\"");
        }

        #[test]
        fn test_string_get_type() {
            verify_exact_code("'abcde'::get_type()", "String(5)");
        }

        #[test]
        fn test_string_gzip() {
            verify_exact_code(r#"
                'Hello World'::gzip()
            "#, "0B1f8b08000000000000fff348cdc9c95708cf2fca49010056b1174a0b000000")
        }

        #[test]
        fn test_string_gunzip() {
            verify_exact_value(r#"
                0B1f8b08000000000000fff348cdc9c95708cf2fca49010056b1174a0b000000::gunzip()
            "#, ByteStringValue(b"Hello World".to_vec()))
        }

        #[test]
        fn test_string_hex() {
            verify_exact_code(
                "'Hello World'::hex()",
                "\"48656c6c6f20576f726c64\"",
            )
        }

        #[test]
        fn test_string_inspect() {
            verify_exact_table(r#"
                "{ x = 1; x = x + 1 }"::inspect()
            "#, vec![
                r#"|-----------------------------------------------------------------------------------------------------|"#,
                r#"| id | code      | model                                                                              |"#,
                r#"|-----------------------------------------------------------------------------------------------------|"#,
                r#"| 0  | x = 1     | SetVariables(Identifier("x"), Literal(Number(I64Value(1))))                        |"#,
                r#"| 1  | x = x + 1 | SetVariables(Identifier("x"), Plus(Identifier("x"), Literal(Number(I64Value(1))))) |"#,
                r#"|-----------------------------------------------------------------------------------------------------|"#])
        }

        #[test]
        fn test_string_is_a() {
            verify_exact_code(r#"
                "did it work?"::is_a(String)
            "#, "true");
        }

        #[test]
        fn test_string_left_positive() {
            verify_exact_code(r#"
                'Hello World'::left(5)
            "#, "\"Hello\"");
        }

        #[test]
        fn test_string_left_negative() {
            verify_exact_code(r#"
                'Hello World'::left(-5)
            "#, "\"World\"");
        }

        #[test]
        fn test_string_left_valid() {
            verify_exact_code(r#"
                'Hello World'::left(5)
            "#, "\"Hello\"");
        }

        #[test]
        fn test_string_len() {
            verify_exact_code(r#"
                'The little brown fox'::len()
            "#, "20");
        }

        #[test]
        fn test_string_md5() {
            verify_exact_code(
                "'Hello World'::md5()",
                "b10a8db1-64e0-7541-05b7-a99be72e3fe5",
            )
        }

        #[test]
        fn test_string_position() {
            verify_exact_code(r#"
                'The little brown fox'::position('brown')
            "#, "11");
        }

        #[test]
        fn test_string_reverse() {
            verify_exact_code(r#"
                "Hello World"::reverse()
            "#, "\"dlroW olleH\"");
        }

        #[test]
        fn test_string_right_positive() {
            verify_exact_code(r#"
                'Hello World'::right(5)
            "#, "\"World\"");
        }

        #[test]
        fn test_string_right_negative() {
            verify_exact_code(r#"
                'Hello World'::right(-5)
            "#, "\"Hello\"");
        }

        #[test]
        fn test_string_split() {
            verify_exact_code(r#"
                'Hello World'::split(' ')
            "#, r#"["Hello", "World"]"#);
        }

        #[test]
        fn test_string_split_multiple_chars() {
            verify_exact_code(r#"
                'Hello,there World'::split(' ,')
            "#, r#"["Hello", "there", "World"]"#);
        }

        #[test]
        fn test_string_starts_with_true() {
            verify_exact_code(r#"
                'Hello World'::starts_with('Hello')
            "#, "true");
        }

        #[test]
        fn test_string_starts_with_false() {
            verify_exact_code(r#"
                'Hello World'::starts_with('World')
            "#, "false");
        }

        #[test]
        fn test_string_strip_margin() {
            verify_exact_code(
                strip_margin(r#"
                |"|Code example:
                | |
                | |stocks where exchange is 'NYSE'
                | |"::strip_margin('|')"#, '|').as_str(),
                "\"Code example:\n\nstocks where exchange is 'NYSE'\n\""
            );
        }

        #[test]
        fn test_string_substring() {
            verify_exact_code(r#"
                'Hello World'::substring(0, 5)
            "#, "\"Hello\"");
        }

        #[test]
        fn test_string_url_decode() {
            verify_exact_value(
                "'http%3A%2F%2Fshocktrade.com%3Fname%3Dthe%20hero%26t%3D9998'::url_decode()",
                StringValue("http://shocktrade.com?name=the hero&t=9998".to_string()),
            )
        }

        #[test]
        fn test_string_url_encode() {
            verify_exact_value(
                "'http://shocktrade.com?name=the hero&t=9998'::url_encode()",
                StringValue("http%3A%2F%2Fshocktrade.com%3Fname%3Dthe%20hero%26t%3D9998".to_string())
            )
        }

        #[test]
        fn test_string_to_array() {
            verify_exact_code(r#"
                 "Hello"::to(Array)
            "#, "['H', 'e', 'l', 'l', 'o']");
        }

        #[test]
        fn test_string_to_bytes() {
            verify_exact_code(r#"
                "Hello there"::to(Bytes)
            "#, "0B48656c6c6f207468657265")
        }

        #[test]
        fn test_string_to_datetime() {
            verify_exact_code(r#"
                 "2025-07-20T16:47:25.127Z"::to(DateTime)
            "#, "2025-07-20T16:47:25.127Z");
        }

        #[test]
        fn test_string_to_f64() {
            verify_exact_code(r#"
                "8.25"::to(f64)
            "#, "8.25")
        }

        #[test]
        fn test_string_to_i128() {
            verify_exact_code(r#"
                "8231"::to(i128)
            "#, "8231")
        }

        #[test]
        fn test_string_to_i64() {
            verify_exact_code(r#"
                "8231"::to(i64)
            "#, "8231")
        }

        #[test]
        fn test_string_to_u128() {
            verify_exact_code(r#"
                "8231"::to(u128)
            "#, "8231")
        }

        #[test]
        fn test_string_to_u64() {
            verify_exact_code(r#"
                "8231"::to(u64)
            "#, "8231")
        }

        #[test]
        fn test_string_to_u8() {
            verify_exact_code(r#"
                "123"::to(u8)
            "#, "0x7b")
        }

        #[test]
        fn test_string_to_uuid() {
            verify_exact_code(r#"
                "b11db772-1dbd-4839-be06-8392f88c1924"::to(UUID)
            "#, "b11db772-1dbd-4839-be06-8392f88c1924")
        }
    }

    /// Package "Struct" tests
    #[cfg(test)]
    mod struct_tests {
        use crate::test_util::{verify_exact_code_and_inferred_type, verify_exact_table};

        #[test]
        fn test_struct_describe() {
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
            ]);
        }

        #[test]
        fn test_struct_is_a() {
            verify_exact_code_and_inferred_type(r#"
                {symbol: "ZAP", exchange: "AMEX", last_sale: 56.88}
                    ::is_a(Struct)
            "#, "true", "Boolean");
        }

        #[test]
        fn test_struct_keys() {
            verify_exact_code_and_inferred_type(r#"
                stock = {symbol: "ZAP", exchange: "AMEX", last_sale: 56.88}
                stock::keys()
           "#, r#"["symbol", "exchange", "last_sale"]"#, "Array(String)")
        }

        #[test]
        fn test_struct_to_table_hard() {
            verify_exact_table(r#"
                Struct(
                    symbol: String(8) = "ABC",
                    exchange: String(8) = "NYSE",
                    last_sale: f64 = 45.67
                )::new::to(Table)
            "#, vec![
                "|------------------------------------|",
                "| id | symbol | exchange | last_sale |",
                "|------------------------------------|",
                "| 0  | ABC    | NYSE     | 45.67     |",
                "|------------------------------------|",
            ])
        }

        #[test]
        fn test_struct_to_table_soft() {
            verify_exact_table(r#"
                [{ symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                 { symbol: "DMX", exchange: "OTC_BB", last_sale: 1.17 },
                 { symbol: "TRX", exchange: "AMEX", last_sale: 29.88 },
                 { symbol: "BMX", exchange: "NASDAQ", last_sale: 46.11 }
                ]::to(Table)
            "#, vec![
                "|------------------------------------|",
                "| id | symbol | exchange | last_sale |",
                "|------------------------------------|",
                "| 0  | BIZ    | NYSE     | 23.66     |",
                "| 1  | DMX    | OTC_BB   | 1.17      |",
                "| 2  | TRX    | AMEX     | 29.88     |",
                "| 3  | BMX    | NASDAQ   | 46.11     |",
                "|------------------------------------|"])
        }

        #[test]
        fn test_struct_to_table_mixed() {
            verify_exact_table(r#"
                stocks = [
                    { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                    { symbol: "DMX", exchange: "OTC_BB", last_sale: 1.17 }
                ]::to(Table)

                [stocks,
                 Struct(symbol: String(8), exchange: String(8), last_sale: f64)
                    ::new("ABC", "OTHER_OTC", 0.67),
                 { symbol: "TRX", exchange: "AMEX", last_sale: 29.88 },
                 { symbol: "BMX", exchange: "NASDAQ", last_sale: 46.11 }
                ]::to(Table)
            "#, vec![
                "|-------------------------------------|",
                "| id | symbol | exchange  | last_sale |",
                "|-------------------------------------|",
                "| 0  | BIZ    | NYSE      | 23.66     |",
                "| 1  | DMX    | OTC_BB    | 1.17      |",
                "| 2  | ABC    | OTHER_OTC | 0.67      |",
                "| 3  | TRX    | AMEX      | 29.88     |",
                "| 4  | BMX    | NASDAQ    | 46.11     |",
                "|-------------------------------------|",
            ])
        }

        #[test]
        fn test_type_of_structure_hard() {
            verify_exact_code_and_inferred_type(
                r#"Struct(symbol: String(3) = "ABC")::get_type()"#,
                r#"Struct(symbol: String(3) = "ABC")"#,
                r#"Struct(symbol: String(3) = "ABC")"#
            );
        }

        #[test]
        fn test_type_of_structure_soft() {
            verify_exact_code_and_inferred_type(
                r#"{symbol:"ABC"}::get_type()"#,
                r#"Struct(symbol: String(3) = "ABC")"#,
                r#"Struct(symbol: String(3))"#
            );
        }
    }

    /// Package "Table" tests
    #[cfg(test)]
    mod table_tests {
        use crate::interpreter::Interpreter;
        use crate::numbers::Numbers::I64Value;
        use crate::sequences::Array;
        use crate::test_util::*;
        use crate::typed_values::TypedValue::{ArrayValue, Boolean, Number, StringValue};

        #[test]
        fn test_table_compact() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_table_with(interpreter, r#"
                stocks =
                    [{ symbol: "DWMX", exchange: "NYSE", last_sale: 99.99 },
                     { symbol: "UNAM", exchange: "OTCBB", last_sale: 0.2456 },
                     { symbol: "BDGR", exchange: "NYSE", last_sale: 23.66 },
                     { symbol: "XPLD", exchange: "OTCBB", last_sale: 0.1428 },
                     { symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
                     { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 },
                     { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }]
                 ::to(Table)::save_as("builtins.table_compact.stocks")
                delete stocks where last_sale > 1.0
                stocks
            "#, vec![
                "|------------------------------------|",
                "| id | symbol | exchange | last_sale |",
                "|------------------------------------|",
                "| 1  | UNAM   | OTCBB    | 0.2456    |",
                "| 3  | XPLD   | OTCBB    | 0.1428    |",
                "| 5  | BOOM   | NASDAQ   | 0.0872    |",
                "|------------------------------------|",
            ]);

            verify_exact_table_with(interpreter, r#"
                stocks::compact()
                stocks
            "#, vec![
                "|------------------------------------|",
                "| id | symbol | exchange | last_sale |",
                "|------------------------------------|",
                "| 0  | BOOM   | NASDAQ   | 0.0872    |",
                "| 1  | UNAM   | OTCBB    | 0.2456    |",
                "| 2  | XPLD   | OTCBB    | 0.1428    |",
                "|------------------------------------|",
            ]);
        }

        #[test]
        fn test_table_describe() {
            verify_exact_table(r#"
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
        fn test_table_filter() {
            verify_exact_table(r#"
                |-------------------------------|
                | symbol | exchange | last_sale |
                |-------------------------------|
                | WKRP   | NYSE     | 11.11     |
                | ACDC   | AMEX     | 37.43     |
                | UELO   | NYSE     | 91.82     |
                |-------------------------------|
                ::filter(row -> exchange is "AMEX")
           "#, vec![
                "|------------------------------------|",
                "| id | symbol | exchange | last_sale |",
                "|------------------------------------|",
                "| 0  | ACDC   | AMEX     | 37.43     |",
                "|------------------------------------|",
            ])
        }

        #[test]
        fn test_table_head() {
            verify_exact_table(r#"
                stocks =
                    |--------------------------------------|
                    | symbol | exchange | last_sale | rank |
                    |--------------------------------------|
                    | BOOM   | NYSE     | 113.76    | 1    |
                    | ABC    | AMEX     | 24.98     | 2    |
                    | JET    | NASDAQ   | 64.24     | 3    |
                    |--------------------------------------|
                stocks::head()
            "#, vec![
                "|-------------------------------------------|",
                "| id | symbol | exchange | last_sale | rank |",
                "|-------------------------------------------|",
                "| 0  | BOOM   | NYSE     | 113.76    | 1    |",
                "|-------------------------------------------|"]);
        }

        #[test]
        fn test_table_is_a() {
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
                stocks::is_a(Table)
            "#, "true");
        }

        #[test]
        fn test_table_keys() {
            verify_exact_code_and_inferred_type(r#"
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
           "#, r#"["symbol", "exchange", "last_sale"]"#, "Array(String)")
        }

        #[test]
        fn test_table_latest() {
            verify_exact_code(r#"
                stocks =
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
                    ::save_as("packages.tools_latest.stocks")
                delete stocks where last_sale < 1
                stocks::latest()
           "#, "3")
        }

        #[test]
        fn test_table_map() {
            verify_exact_table(r#"
                stocks = Table(
                    symbol: String(8),
                    exchange: String(8),
                    last_sale: f64
                )::new::save_as("platform.map_over_table.stocks")

                [{ symbol: "WKRP", exchange: "NYSE", last_sale: 11.11 },
                 { symbol: "ACDC", exchange: "AMEX", last_sale: 35.11 },
                 { symbol: "UELO", exchange: "NYSE", last_sale: 90.12 }] ~> stocks

                stocks::map(row -> {
                    symbol: symbol,
                    exchange: exchange,
                    last_sale: last_sale,
                    magnitude: last_sale * 2.0
                })
           "#, vec![
                "|------------------------------------------------|",
                "| id | symbol | exchange | last_sale | magnitude |",
                "|------------------------------------------------|",
                "| 0  | WKRP   | NYSE     | 11.11     | 22.22     |",
                "| 1  | ACDC   | AMEX     | 35.11     | 70.22     |",
                "| 2  | UELO   | NYSE     | 90.12     | 180.24    |",
                "|------------------------------------------------|",
            ])
        }

        #[test]
        fn test_table_pop() {
            verify_exact_table(r#"
                stocks =
                    [{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                     { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                     { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }]
                        ::to(Table)::save_as("builtins.pop.stocks")
                stocks::pop()
            "#, vec![
                "|------------------------------------|",
                "| id | symbol | exchange | last_sale |",
                "|------------------------------------|",
                "| 2  | JET    | NASDAQ   | 32.12     |",
                "|------------------------------------|",
            ]);
            verify_exact_table(r#"
                stocks = tables::load("builtins.pop.stocks")
                stocks::pop()
            "#, vec![
                "|------------------------------------|",
                "| id | symbol | exchange | last_sale |",
                "|------------------------------------|",
                "| 1  | BOOM   | NYSE     | 56.88     |",
                "|------------------------------------|",
            ]);
        }

        #[test]
        fn test_table_pull_cell() {
            verify_exact_code_and_inferred_type(r#"
                let stocks =
                    |--------------------------------------|
                    | symbol | exchange | last_sale | rank |
                    |--------------------------------------|
                    | BOOM   | NYSE     | 113.76    | 1    |
                    | ABC    | AMEX     | 24.98     | 2    |
                    | JET    | NASDAQ   | 64.24     | 3    |
                    |--------------------------------------|
                stocks::cell(2, "last_sale")
           "#, "64.24", "f64")
        }

        #[test]
        fn test_table_pull_column() {
            verify_exact_code_and_inferred_type(r#"
                let stocks =
                    |--------------------------------------|
                    | symbol | exchange | last_sale | rank |
                    |--------------------------------------|
                    | BOOM   | NYSE     | 113.76    | 1    |
                    | ABC    | AMEX     | 24.98     | 2    |
                    | JET    | NASDAQ   | 64.24     | 3    |
                    | QUAD   | OTCBB    | 0.00      | 3    |
                    |--------------------------------------|
                stocks::column("last_sale")
           "#, "[113.76, 24.98, 64.24, 0.0]", "Array(f64)")
        }

        #[test]
        fn test_table_pull_row() {
            verify_exact_table(r#"
                stocks = Table(
                    symbol: String(8),
                    exchange: Enum(AMEX, NYSE, NASDAQ, OTCBB),
                    last_sale: f64
                )::new::save_as("builtins.pull_row.stocks")

                let rows = [
                    { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                    { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                    { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }]
                rows ~> stocks

                stocks::row(2)
            "#, vec![
                "|------------------------------------|",
                "| id | symbol | exchange | last_sale |",
                "|------------------------------------|",
                "| 2  | JET    | NASDAQ   | 32.12     |",
                "|------------------------------------|",
            ])
        }

        #[test]
        fn test_table_push() {
            verify_exact_table(r#"
                stocks =
                    |-------------------------------|
                    | symbol | exchange | last_sale |
                    |-------------------------------|
                    | ABC    | AMEX     | 12.49     |
                    | BOOM   | NYSE     | 56.88     |
                    | JET    | NASDAQ   | 32.12     |
                    |-------------------------------|
                    ::save_as("builtins.push.stocks")

                stocks::push({ symbol: "DEX", exchange: "OTC_BB", last_sale: 0.0086 })
                stocks
            "#, vec![
                "|------------------------------------|",
                "| id | symbol | exchange | last_sale |",
                "|------------------------------------|",
                "| 0  | ABC    | AMEX     | 12.49     |",
                "| 1  | BOOM   | NYSE     | 56.88     |",
                "| 2  | JET    | NASDAQ   | 32.12     |",
                "| 3  | DEX    | OTC_BB   | 0.0086    |",
                "|------------------------------------|",
            ]);
        }

        #[test]
        fn test_table_record_size() {
            verify_exact_code(r#"
                 Table(
                    symbol: String(8),
                    exchange: String(8),
                    last_sale: f64
                 )::new::record_size()
            "#, "52");
        }

        #[test]
        fn test_table_record_size_with_enum() {
            verify_exact_code(r#"
                 Table(
                    symbol: String(8),
                    exchange: Enum(AMEX, NYSE, NASDAQ, OTCBB),
                    last_sale: f64
                 )::new::record_size()
            "#, "38");
        }

        #[test]
        fn test_table_replay() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_value_whence(interpreter, r#"
                tables::drop("platform.replay.stocks")
            "#, |result| matches!(result, Boolean(_)));
            interpreter = verify_exact_value_with(interpreter, r#"
                stocks = tables::create_fn(
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
            "#, Number(I64Value(3)));
            let _interpreter = verify_exact_table_with(interpreter, r#"
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
            ]);
        }

        #[test]
        fn test_table_reverse() {
            verify_exact_table(r#"
                [{ symbol: "ABC", exchange: "AMEX", last_sale: 12.33 },
                 { symbol: "BIZ", exchange: "NYSE", last_sale: 9.775 },
                 { symbol: "XYZ", exchange: "NASDAQ", last_sale: 89.11 }]
                    ::to(Table)::reverse()
            "#,
                vec![
                    "|------------------------------------|",
                    "| id | symbol | exchange | last_sale |",
                    "|------------------------------------|",
                    "| 0  | XYZ    | NASDAQ   | 89.11     |",
                    "| 1  | BIZ    | NYSE     | 9.775     |",
                    "| 2  | ABC    | AMEX     | 12.33     |",
                    "|------------------------------------|",
                ]);
        }

        #[actix::test]
        async fn test_save_as() {
            verify_exact_table_async_and_sync(r#"
                stocks =
                    |--------------------------------|
                    | symbol | exchange  | last_sale |
                    |--------------------------------|
                    | IKR    | NYSE      | 11.75     |
                    | LOL    | NASDAQ    | 32.96     |
                    | SMH    | OTCBB     | 5.02      |
                    | ROFL   | OTCBB     | 1.37      |
                    |--------------------------------|
                stocks::save_as("builtins.save_as.stocks")
            "#, vec![
                "|------------------------------------|",
                "| id | symbol | exchange | last_sale |",
                "|------------------------------------|",
                "| 0  | IKR    | NYSE     | 11.75     |",
                "| 1  | LOL    | NASDAQ   | 32.96     |",
                "| 2  | SMH    | OTCBB    | 5.02      |",
                "| 3  | ROFL   | OTCBB    | 1.37      |",
                "|------------------------------------|"]).await;
        }

        #[test]
        fn test_table_scan() {
            let mut interpreter = Interpreter::new();
            let result = interpreter.evaluate(r#"
                stocks =
                    [{ symbol: "ABC", exchange: "AMEX", last_sale: 12.33 },
                     { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                     { symbol: "BIZ", exchange: "NYSE", last_sale: 9.775 },
                     { symbol: "GOTO", exchange: "OTC", last_sale: 0.1442 },
                     { symbol: "XYZ", exchange: "NYSE", last_sale: 0.0289 }]
                        ::to(Table)::save_as("builtins.scan.stocks")

                delete stocks where last_sale > 1.0
                stocks::scan()
            "#).unwrap();
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

        #[actix::test]
        async fn test_table_tail() {
            verify_exact_table_async_and_sync(r#"
                stocks =
                    |--------------------------------------|
                    | symbol | exchange | last_sale | rank |
                    |--------------------------------------|
                    | BOOM   | NYSE     | 113.76    | 1    |
                    | ABC    | AMEX     | 24.98     | 2    |
                    | JET    | NASDAQ   | 64.24     | 3    |
                    |--------------------------------------|
                stocks::tail()
            "#, vec![
                "|-------------------------------------------|",
                "| id | symbol | exchange | last_sale | rank |",
                "|-------------------------------------------|",
                "| 0  | ABC    | AMEX     | 24.98     | 2    |",
                "| 1  | JET    | NASDAQ   | 64.24     | 3    |",
                "|-------------------------------------------|"]).await;
        }

        #[test]
        fn test_table_to_csv() {
            verify_exact_value(r#"
                [{ symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
                 { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                 { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                 { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                 { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }]
                    ::to(Table)::to_csv()
            "#, ArrayValue(Array::from(vec![
                StringValue(r#""ABC","AMEX",11.11"#.into()),
                StringValue(r#""UNO","OTC",0.2456"#.into()),
                StringValue(r#""BIZ","NYSE",23.66"#.into()),
                StringValue(r#""GOTO","OTC",0.1428"#.into()),
                StringValue(r#""BOOM","NASDAQ",0.0872"#.into()),
            ])));
        }

        #[test]
        fn test_table_to_json() {
            verify_exact_value(r#"
                [{ symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
                 { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                 { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                 { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                 { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }]
                    ::to(Table)::to_json()
            "#, ArrayValue(Array::from(vec![
                StringValue(r#"{"symbol":"ABC","exchange":"AMEX","last_sale":11.11}"#.into()),
                StringValue(r#"{"symbol":"UNO","exchange":"OTC","last_sale":0.2456}"#.into()),
                StringValue(r#"{"symbol":"BIZ","exchange":"NYSE","last_sale":23.66}"#.into()),
                StringValue(r#"{"symbol":"GOTO","exchange":"OTC","last_sale":0.1428}"#.into()),
                StringValue(r#"{"symbol":"BOOM","exchange":"NASDAQ","last_sale":0.0872}"#.into()),
            ])),
            );
        }

        #[test]
        fn test_type_of_table() {
            verify_exact_code(
                "Table(symbol: String(8), exchange: String(8), last_sale: f64)::new::get_type()",
                "Table(symbol: String(8), exchange: String(8), last_sale: f64)",
            );
        }
    }

    /// Package "Tuple" tests
    #[cfg(test)]
    mod tuple_tests {
        use crate::test_util::*;

        #[test]
        fn test_tuple_filter() {
            verify_exact_code(r#"
                 ('a', 'b', 5, 7)::filter(v -> v::is_a(Char))
            "#, "('a', 'b')");
        }

        #[test]
        fn test_tuple_head() {
            verify_exact_code_and_inferred_type(r#"
                ('1', 5, 9, '13')::head()
            "#, "'1'", "Char");
        }

        #[test]
        fn test_tuple_is_a() {
            verify_exact_code_and_inferred_type(r#"
                (1, 'c', 'd')::is_a(Tuple)
            "#, "true", "Boolean");
        }

        #[test]
        fn test_tuple_join() {
            verify_exact_code_and_inferred_type(r#"
                ('1', 5, 9, '13')::join(', ')
            "#, "\"1, 5, 9, 13\"", "String");
        }

        #[test]
        fn test_tuple_len() {
            verify_exact_code_and_inferred_type(r#"
                 ('a', 'b', 5, 7)::len()
            "#, "4", "i64");
        }

        #[test]
        fn test_tuple_map() {
            verify_exact_code_and_inferred_type(r#"
                 ('a', 'b', 5, 7)::map(v -> v + 1)
            "#, "('b', 'c', 6, 8)", "Array()");
        }

        #[test]
        fn test_tuple_reverse() {
            verify_exact_code_and_inferred_type(r#"
                 ('a', 'b', 5)::reverse()
            "#, "(5, 'b', 'a')", "Array()");
        }

        #[test]
        fn test_tuple_tail() {
            verify_exact_code_and_inferred_type(r#"
                ('cat', 'dog', 'ferret', 'mouse')::tail()
            "#, r#"("dog", "ferret", "mouse")"#, "Array()")
        }

        #[test]
        fn test_tuple_to_array() {
            verify_exact_code_and_inferred_type(r#"
                 ("a", "b", "c")::to(Array)
            "#, r#"["a", "b", "c"]"#, "Array()");
        }

        #[test]
        fn test_tuple_to_table() {
            verify_exact_table(r#"
                stocks = [
                    ("ABC", "AMEX", 12.49),
                    ("BOOM", "NYSE", 56.88),
                    ("JET", "NASDAQ", 32.12)
                ]
                stocks<::push(("DEX", "OTC_BB", 0.0086))
                stocks<::to(Table)
                stocks
            "#, vec![
                "|-----------------------------|",
                "| id | t0   | t1     | t2     |",
                "|-----------------------------|",
                "| 0  | ABC  | AMEX   | 12.49  |",
                "| 1  | BOOM | NYSE   | 56.88  |",
                "| 2  | JET  | NASDAQ | 32.12  |",
                "| 3  | DEX  | OTC_BB | 0.0086 |",
                "|-----------------------------|"]);
        }

        #[test]
        fn test_tuple_get_type() {
            verify_exact_code_and_inferred_type(
                "('ABC', 123.2, 2025-01-13T03:25:47.350Z)::get_type()",
                "(String(3), f64, DateTime)", "(String(3), f64, DateTime)"
            );
        }
    }

    /// Package "UUID" tests
    #[cfg(test)]
    mod uuid_tests {
        use crate::test_util::{verify_exact_code_and_inferred_type, verify_exact_code_async_and_sync};

        #[test]
        fn test_uuid_get_type() {
            verify_exact_code_and_inferred_type("UUID::new::get_type()", "UUID", "UUID");
        }

        #[test]
        fn test_uuid_is_a() {
            verify_exact_code_and_inferred_type(r#"
                b11db772-1dbd-4839-be06-8392f88c1924::is_a(UUID)
            "#, "true", "Boolean");
        }

        #[actix::test]
        async fn test_uuid_to_array() {
            verify_exact_code_async_and_sync(r#"
                 b11db772-1dbd-4839-be06-8392f88c1924::to(Array)
            "#, "[0xb1, 0x1d, 0xb7, 0x72, 0x1d, 0xbd, 0x48, 0x39, 0xbe, 0x06, 0x83, 0x92, 0xf8, 0x8c, 0x19, 0x24]").await;
        }

        #[test]
        fn test_uuid_to_bytes() {
            verify_exact_code_and_inferred_type(r#"
                 b11db772-1dbd-4839-be06-8392f88c1924::to(Bytes)
            "#, "0Bb11db7721dbd4839be068392f88c1924", "Bytes");
        }

        #[test]
        fn test_uuid_to_string() {
            verify_exact_code_and_inferred_type(r#"
                 b11db772-1dbd-4839-be06-8392f88c1924::to(String)
            "#, "\"b11db772-1dbd-4839-be06-8392f88c1924\"", "String");
        }


        #[test]
        fn test_uuid_to_u128() {
            verify_exact_code_and_inferred_type(r#"
                 b11db772-1dbd-4839-be06-8392f88c1924::to(u128)
            "#, "235427652584999507727293429181426964772", "u128");
        }
    }

}