#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Sequence trait
////////////////////////////////////////////////////////////////////

use crate::data_types::DataType;
use crate::data_types::DataType::{ArrayType, NumberType, TableType, UnresolvedType};
use crate::data_types::DataType::{FixedSizeType, TupleType};
use crate::dataframe::Dataframe;
use crate::dataframe::Dataframe::Model;
use crate::errors::throw;
use crate::errors::Errors::{Exact, TypeMismatch, UnsupportedFeature};
use crate::errors::TypeMismatchErrors::StructExpected;
use crate::model_row_collection::ModelRowCollection;
use crate::number_kind::NumberKind::I64Kind;
use crate::numbers::Numbers::I64Value;
use crate::row_collection::RowCollection;
use crate::structures::Structures::{Firm, Hard};
use crate::structures::{Structure, Structures};
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{ArrayValue, Boolean, ErrorValue, Number, Structured, TupleValue, Undefined};
use log::error;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::Ordering;
use std::ops::Index;
use std::ops::{Add, BitAnd, BitOr, BitXor, Div, Mul, Neg, Not, Rem, Shl, Shr, Sub};
use std::slice::Iter;

/// Represents a linear sequence of [TypedValue]s
pub trait Sequence {
    /// Returns true, if the [Sequence] contains the specified item
    fn contains(&self, item: &TypedValue) -> bool;

    /// Returns the [Option] of a [TypedValue] from specified index
    fn get(&self, index: usize) -> Option<TypedValue>;

    /// Returns the [Option] of a [TypedValue] from specified index or the
    /// default value if not found
    fn get_or_else(&self, index: usize, default: TypedValue) -> TypedValue;

    /// Returns the type
    fn get_type(&self) -> DataType;

    fn get_values(&self) -> Vec<TypedValue>;

    fn iter(&self) -> Iter<'_, TypedValue>;

    fn len(&self) -> usize;

    fn push(&mut self, value: TypedValue) -> TypedValue;

    fn sublist(&self, start: usize, end: usize) -> Self;
    
    fn to_array(&self) -> Array;

    fn to_tuple(&self) -> Tuple;

    fn unwrap_value(&self) -> String;
}

/// Represents a linear sequence of [TypedValue]s
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Sequences {
    TheArray(Array),
    TheDataframe(Dataframe),
    TheRange(Box<TypedValue>, Box<TypedValue>, bool),
    TheTuple(Vec<TypedValue>),
}

impl Sequences {}

impl Sequence for Sequences {
    fn contains(&self, item: &TypedValue) -> bool {
        match self {
            Sequences::TheArray(array) => array.contains(item),
            Sequences::TheDataframe(df) =>
                match item {
                    Structured(s) => df.contains(&s.to_row()),
                    _ => false
                }
            Sequences::TheRange(a, b, incl) => is_in_range(item, a, b, *incl),
            Sequences::TheTuple(tuple) => tuple.contains(item),
        }
    }

    fn get(&self, index: usize) -> Option<TypedValue> {
        match self {
            Sequences::TheArray(array) => array.get(index),
            Sequences::TheDataframe(df) =>
                match df.read_one(index) {
                    Ok(Some(row)) => Some(TypedValue::TupleValue(row.get_values())),
                    Ok(None) => None,
                    Err(err) => {
                        error!("Sequences::get() ~> df.read_one({}) |{}", index, err);
                        None
                    }
                }
            Sequences::TheRange(..) => self.to_array().get(index),
            Sequences::TheTuple(tuple) => tuple.get(index).map(|v| v.clone()),
        }
    }

    fn get_or_else(&self, index: usize, default: TypedValue) -> TypedValue {
        match self {
            Sequences::TheArray(array) => array.get_or_else(index, default),
            Sequences::TheDataframe(df) =>
                match df.read_one(index) {
                    Ok(None) => TypedValue::Null,
                    Ok(Some(row)) => Structured(Firm(row, df.get_parameters())),
                    Err(err) => ErrorValue(Exact(err.to_string()))
                }
            Sequences::TheRange(..) => self.to_array().get_or_else(index, default),
            Sequences::TheTuple(tuple) => tuple.get(index).map(|v| v.clone()).unwrap_or(default),
        }
    }

    fn get_type(&self) -> DataType {
        match self {
            Sequences::TheArray(array) => array.get_type(),
            Sequences::TheDataframe(df) => TableType(df.get_parameters()),
            Sequences::TheRange(..) => ArrayType(NumberType(I64Kind).into()),
            Sequences::TheTuple(tuple) => TupleType(tuple.iter().map(|v| v.get_type()).collect()),
        }
    }

    fn get_values(&self) -> Vec<TypedValue> {
        match self.clone() {
            Sequences::TheArray(array) => array.get_values(),
            Sequences::TheDataframe(df) => df.to_array().get_values(),
            Sequences::TheRange(..) => self.to_array().get_values(),
            Sequences::TheTuple(tuple) => tuple,
        }
    }

    fn iter(&self) -> Iter<'_, TypedValue> {
        match self {
            Sequences::TheArray(array) => array.iter(),
            Sequences::TheDataframe(df) =>
                Box::leak(Box::new(
                    df.iter()
                        .map(|row| Structured(Firm(row, df.get_parameters())))
                        .collect::<Vec<_>>()
                )).iter(),
            Sequences::TheRange(..) => Box::leak(Box::new(self.to_array())).iter(),
            Sequences::TheTuple(tuple) => tuple.iter(),
        }
    }

    fn len(&self) -> usize {
        match self {
            Sequences::TheArray(array) => array.len(),
            Sequences::TheDataframe(df) => df.len().unwrap_or(0),
            Sequences::TheRange(..) => self.to_array().len(),
            Sequences::TheTuple(tuple) => tuple.len(),
        }
    }

    fn push(&mut self, value: TypedValue) -> TypedValue {
        match self {
            Sequences::TheArray(array) => {
                array.push(value);
                Boolean(true)
            }
            Sequences::TheDataframe(df) =>
                match value {
                    Structured(s) => df.push_row(s.to_row()),
                    other => ErrorValue(TypeMismatch(StructExpected("Struct".into(), other.to_code())))
                }
            Sequences::TheRange(..) => self.to_array().push(value),
            Sequences::TheTuple(tuple) => {
                tuple.push(value);
                Boolean(true)
            }
        }
    }

    fn sublist(&self, start: usize, end: usize) -> Self {
        match self {
            Sequences::TheArray(array) => Sequences::TheArray(array.sublist(start, end)),
            Sequences::TheDataframe(df) => Sequences::TheDataframe(df.sublist(start, end)), 
            Sequences::TheRange(_, _, inclusive) => Sequences::TheRange(
                Number(I64Value(start as i64)).into(), 
                Number(I64Value(end as i64)).into(), 
                *inclusive
            ),
            Sequences::TheTuple(tuple) => Sequences::TheTuple(tuple.clone()),
        }
    }

    fn to_array(&self) -> Array {
        match self {
            Sequences::TheArray(array) => array.to_array(),
            Sequences::TheDataframe(df) => df.to_array(),
            Sequences::TheRange(a, b, inclusive) => Array::from_range(a, b, *inclusive),
            Sequences::TheTuple(tuple) => Array::from(tuple.to_vec()),
        }
    }

    fn to_tuple(&self) -> Tuple {
        match self {
            Sequences::TheArray(array) => array.to_tuple(),
            Sequences::TheDataframe(df) => df.to_array().to_tuple(),
            Sequences::TheRange(..) => self.to_array().to_tuple(),
            Sequences::TheTuple(tuple) => Tuple::new(tuple.to_vec()),
        }
    }

    fn unwrap_value(&self) -> String {
        match self {
            Sequences::TheArray(array) => array.unwrap_value(),
            Sequences::TheDataframe(df) => df.to_array().unwrap_value(),
            Sequences::TheRange(..) => self.to_array().unwrap_value(),
            Sequences::TheTuple(tuple) =>
                format!("[{}]", tuple.iter().map(|v| v.unwrap_value())
                    .collect::<Vec<_>>().join(", "))
        }
    }
}

////////////////////////////////////////////////////////////////////
// Array class
////////////////////////////////////////////////////////////////////

/// Represents an elastic array of values
#[derive(Clone, Debug, Eq, Ord, PartialEq, Serialize, Deserialize)]
pub struct Array {
    the_array: Vec<TypedValue>,
}

impl Array {

    ////////////////////////////////////////////////////////////////////
    // static methods
    ////////////////////////////////////////////////////////////////////

    /// Constructs an [Array] from a vector of [TypedValue]s
    pub fn from(
        items: Vec<TypedValue>,
    ) -> Self {
        Self {
            the_array: items,
        }
    }
    
    pub fn from_range(a: &TypedValue, b: &TypedValue, inclusive: bool) -> Self {
        Self::from(range_to_vec(a, b, inclusive))
    }

    /// Creates a new empty [Array]
    pub fn new() -> Array {
        Self {
            the_array: Vec::new(),
        }
    }

    ////////////////////////////////////////////////////////////////////
    // instanced methods
    ////////////////////////////////////////////////////////////////////

    /// Removes all elements from the [Array]
    pub fn clear(&mut self) {
        self.the_array.clear()
    }

    /// Returns the component type; the resolved internal type
    pub fn get_component_type(&self) -> DataType {
        let kinds = self.the_array.iter()
            .map(|item| item.get_type())
            .fold(Vec::new(), |mut kinds, kind| {
                if !kinds.contains(&kind) {
                    kinds.push(kind);
                    kinds
                } else {
                    kinds
                }
            });
        match kinds.as_slice() {
            [] => UnresolvedType,
            [kind] => kind.clone(),
            kinds => DataType::best_fit(kinds.to_vec())
        }
    }

    /// Returns true, if the [Array] is empty
    pub fn is_empty(&self) -> bool {
        self.the_array.is_empty()
    }

    pub fn map(&self, f: fn(&TypedValue) -> TypedValue) -> Self {
        Array::from(self.the_array.iter().map(f).collect::<Vec<_>>())
    }

    pub fn pop(&self) -> (Array, Option<TypedValue>) {
        let mut items = self.the_array.clone();
        let value = items.pop();
        (Array::from(items), value)
    }

    pub fn push_all(&mut self, values: Vec<TypedValue>) {
        self.the_array.extend(values)
    }

    pub fn rev(&self) -> Self {
        Array::from(self.the_array.iter().rev()
            .map(|i| i.clone())
            .collect::<Vec<_>>())
    }

    pub fn sublist(&self, start: usize, end: usize) -> Self {
        Array::from(self.the_array[start..end].to_vec())
    }
}

impl Index<usize> for Array {
    type Output = TypedValue;

    fn index(&self, index: usize) -> &Self::Output {
        if index < self.the_array.len() { &self.the_array[index] } else { &Undefined }
    }
}

impl PartialOrd for Array {
    fn partial_cmp(&self, rhs: &Self) -> Option<Ordering> {
        self.get_values().partial_cmp(&rhs.get_values())
    }
}

impl Sequence for Array {
    /// Returns true, if the [Array] contains the specified item
    fn contains(&self, item: &TypedValue) -> bool {
      self.the_array.contains(item) || self.the_array.iter().any(|i| i.contains(item))
    }

    /// Returns the [Option] of a [TypedValue] from specified index
    fn get(&self, index: usize) -> Option<TypedValue> {
        idx_vec_opt(&self.the_array, index)
    }

    /// Returns the [Option] of a [TypedValue] from specified index or the
    /// default value if not found
    fn get_or_else(&self, index: usize, default: TypedValue) -> TypedValue {
        self.get(index).unwrap_or(default)
    }

    /// Returns the type
    fn get_type(&self) -> DataType {
        let data_type = self.get_component_type();
        FixedSizeType(ArrayType(data_type.into()).into(), self.the_array.len())
    }

    fn get_values(&self) -> Vec<TypedValue> {
        self.the_array.clone()
    }

    fn iter(&self) -> Iter<'_, TypedValue> {
        self.the_array.iter()
    }

    fn len(&self) -> usize {
        self.the_array.len()
    }

    fn push(&mut self, value: TypedValue) -> TypedValue {
        self.the_array.push(value);
        ArrayValue(self.clone())
    }

    fn sublist(&self, start: usize, end: usize) -> Self {
        Self::from(self.the_array[start..end].to_vec())
    }

    fn to_array(&self) -> Array {
        Array::from(self.the_array.clone())
    }

    fn to_tuple(&self) -> Tuple {
        Tuple::new(self.the_array.clone())
    }

    fn unwrap_value(&self) -> String {
        format!("[{}]", self.the_array.iter().map(|i| i.unwrap_value())
            .collect::<Vec<_>>().join(", "))
    }
}

////////////////////////////////////////////////////////////////////
//  Tuple class
////////////////////////////////////////////////////////////////////

/// Represents a linear sequence of values
#[derive(Clone, Debug, Eq, Ord, PartialEq, Serialize, Deserialize)]
pub struct Tuple {
    the_tuple: Vec<TypedValue>,
}

impl Tuple {

    ////////////////////////////////////////////////////////////////////
    // static methods
    ////////////////////////////////////////////////////////////////////

    /// Constructs a [Tuple] from  a vector of [TypedValue]s
    pub fn new(
        items: Vec<TypedValue>,
    ) -> Self {
        Self { the_tuple: items }
    }

    ////////////////////////////////////////////////////////////////////
    // instanced methods
    ////////////////////////////////////////////////////////////////////

    pub fn map(&self, f: fn(&TypedValue) -> TypedValue) -> Self {
        Self::new(self.the_tuple.iter().map(f).collect::<Vec<_>>())
    }

    pub fn rev(&self) -> Self {
        Self::new(self.the_tuple.iter().rev()
            .map(|i| i.clone())
            .collect::<Vec<_>>())
    }

    pub fn unwrap_value(&self) -> String {
        format!("({})", self.the_tuple.iter().map(|i| i.unwrap_value())
            .collect::<Vec<_>>().join(", "))
    }
}

impl Index<usize> for Tuple {
    type Output = TypedValue;

    fn index(&self, index: usize) -> &Self::Output {
        if index < self.the_tuple.len() { &self.the_tuple[index] } else { &Undefined }
    }
}

impl PartialOrd for Tuple {
    fn partial_cmp(&self, rhs: &Self) -> Option<Ordering> {
        self.get_values().partial_cmp(&rhs.get_values())
    }
}

impl Sequence for Tuple {
    /// Returns true, if the [Array] contains the specified item
    fn contains(&self, item: &TypedValue) -> bool {
        self.the_tuple.contains(item)
    }

    /// Returns the [Option] of a [TypedValue] from specified index
    fn get(&self, index: usize) -> Option<TypedValue> {
        idx_vec_opt(&self.the_tuple, index)
    }

    /// Returns the [Option] of a [TypedValue] from specified index or the
    /// default value if not found
    fn get_or_else(&self, index: usize, default: TypedValue) -> TypedValue {
        self.get(index).unwrap_or(default)
    }

    /// Returns the type
    fn get_type(&self) -> DataType {
        TupleType(self.the_tuple.iter().map(|v| v.get_type()).collect())
    }

    fn get_values(&self) -> Vec<TypedValue> {
        self.the_tuple.clone()
    }

    fn iter(&self) -> Iter<'_, TypedValue> {
        self.the_tuple.iter()
    }

    fn len(&self) -> usize {
        self.the_tuple.len()
    }

    fn push(&mut self, value: TypedValue) -> TypedValue {
        self.the_tuple.push(value);
        TupleValue(self.the_tuple.clone())
    }

    fn sublist(&self, _start: usize, _end: usize) -> Self {
        self.clone()
    }

    fn to_array(&self) -> Array {
        Array::from(self.the_tuple.clone())
    }

    fn to_tuple(&self) -> Tuple {
        Tuple::new(self.the_tuple.clone())
    }

    fn unwrap_value(&self) -> String {
        format!("({})", self.the_tuple.iter().map(|i| i.unwrap_value())
            .collect::<Vec<_>>().join(", "))
    }
}

pub fn add_vec(aa: Vec<TypedValue>, bb: Vec<TypedValue>) -> Vec<TypedValue> {
    aa.iter().zip(bb.iter())
        .map(|(a, b)| a.clone().add(b.clone()))
        .collect()
}

pub fn bitand_vec(aa: Vec<TypedValue>, bb: Vec<TypedValue>) -> Vec<TypedValue> {
    aa.iter().zip(bb.iter())
        .map(|(a, b)| a.clone().bitand(b.clone()))
        .collect()
}

pub fn bitor_vec(aa: Vec<TypedValue>, bb: Vec<TypedValue>) -> Vec<TypedValue> {
    aa.iter().zip(bb.iter())
        .map(|(a, b)| a.clone().bitor(b.clone()))
        .collect()
}

pub fn bitxor_vec(aa: Vec<TypedValue>, bb: Vec<TypedValue>) -> Vec<TypedValue> {
    aa.iter().zip(bb.iter())
        .map(|(a, b)| a.clone().bitxor(b.clone()))
        .collect()
}

pub fn div_vec(aa: Vec<TypedValue>, bb: Vec<TypedValue>) -> Vec<TypedValue> {
    aa.iter().zip(bb.iter())
        .map(|(a, b)| a.clone().div(b.clone()))
        .collect()
}

pub fn idx_vec(aa: &Vec<TypedValue>, index: usize) -> TypedValue {
    if index < aa.len() { aa[index].clone() } else { Undefined }
}

pub fn idx_vec_opt(aa: &Vec<TypedValue>, index: usize) -> Option<TypedValue> {
    if index < aa.len() { Some(aa[index].clone()) } else { None }
}

pub fn mul_vec(aa: Vec<TypedValue>, bb: Vec<TypedValue>) -> Vec<TypedValue> {
    aa.iter().zip(bb.iter())
        .map(|(a, b)| a.clone().mul(b.clone()))
        .collect()
}

pub fn neg_vec(aa: Vec<TypedValue>) -> Vec<TypedValue> {
    aa.iter().map(|a| a.clone().neg()).collect()
}

pub fn not_vec(aa: Vec<TypedValue>) -> Vec<TypedValue> {
    aa.iter().map(|a| a.clone().not()).collect()
}

pub fn pow_vec(aa: Vec<TypedValue>, bb: Vec<TypedValue>) -> Vec<TypedValue> {
    aa.iter().zip(bb.iter())
        .map(|(a, b)| a.clone().pow(b).unwrap_or(Undefined))
        .collect()
}

pub fn rem_vec(aa: Vec<TypedValue>, bb: Vec<TypedValue>) -> Vec<TypedValue> {
    aa.iter().zip(bb.iter())
        .map(|(a, b)| a.clone().rem(b.clone()))
        .collect()
}

pub fn shl_vec(aa: Vec<TypedValue>, bb: Vec<TypedValue>) -> Vec<TypedValue> {
    aa.iter().zip(bb.iter())
        .map(|(a, b)| a.clone().shl(b.clone()))
        .collect()
}

pub fn shr_vec(aa: Vec<TypedValue>, bb: Vec<TypedValue>) -> Vec<TypedValue> {
    aa.iter().zip(bb.iter())
        .map(|(a, b)| a.clone().shr(b.clone()))
        .collect()
}

pub fn sub_vec(aa: Vec<TypedValue>, bb: Vec<TypedValue>) -> Vec<TypedValue> {
    aa.iter().zip(bb.iter())
        .map(|(a, b)| a.clone().sub(b.clone()))
        .collect()
}

pub fn is_in_range(value: &TypedValue, min: &TypedValue, max: &TypedValue, inclusive: bool) -> bool {
    value >= min && (if inclusive { value <= max } else { value < max })
}

pub fn range_diff(min: &TypedValue, max: &TypedValue, inclusive: bool) -> TypedValue {
    (max.clone() - min.clone()) + Number(I64Value(if inclusive { 1 } else { 0 }))
}

pub fn range_to_vec(a: &TypedValue, b: &TypedValue, inclusive: bool) -> Vec<TypedValue> {
    let mut values = vec![];
    let mut n = a.clone();
    while n < *b {
        values.push(n.clone());
        n = n.clone().add(Number(I64Value(1)));
    }
    if inclusive { values.push(b.clone()) }
    values
}

/// Unit tests
#[cfg(test)]
mod tests {
    /// Unit core tests
    #[cfg(test)]
    mod core_tests {}

    /// Unit array tests
    #[cfg(test)]
    mod array_tests {
        use crate::data_types::DataType;
        use crate::data_types::DataType::{ArrayType, FixedSizeType, StringType};
        use crate::numbers::Numbers::I64Value;
        use crate::sequences::{Array, Sequence, Tuple};
        use crate::testdata::*;
        use crate::typed_values::TypedValue::*;
        use crate::typed_values::TypedValue::{Boolean, StringValue};

        #[test]
        fn test_clear_and_is_empty() {
            let mut array = create_array();
            assert_eq!(array.is_empty(), false);

            array.clear();
            assert_eq!(array.is_empty(), true);
        }

        #[test]
        fn test_get() {
            let mut array = create_array();
            assert_eq!(array.get(1), Some(StringValue("NASDAQ".into())));
        }

        #[test]
        fn test_get_or_else_positive() {
            let mut array = create_array();
            assert_eq!(array.get_or_else(1, StringValue("N/A".into())), StringValue("NASDAQ".into()));
        }

        #[test]
        fn test_get_or_else_negative() {
            let mut array = create_array();
            assert_eq!(array.get_or_else(7, StringValue("N/A".into())), StringValue("N/A".into()));
        }

        #[test]
        fn test_get_component_type() {
            let mut array = Array::new();
            array.push(StringValue("Hello".into()));
            assert_eq!(array.get_component_type(), FixedSizeType(StringType.into(), 5));

            array.push(Boolean(true));
            assert_eq!(array.get_component_type(), DataType::BooleanType);

            array.push(StringValue("Hello World".into()));
            array.push(StringValue("Hello World".into()));
            assert_eq!(array.get_component_type(), FixedSizeType(StringType.into(), 11));

            assert_eq!(array.get_type(), FixedSizeType(ArrayType(FixedSizeType(StringType.into(), 11).into()).into(), 4));
        }

        #[test]
        fn test_len() {
            let mut array = create_array();
            assert_eq!(array.len(), 4)
        }

        #[test]
        fn test_pop() {
            let mut array = create_array();
            let (new_array, value) = array.pop();
            assert_eq!(value, Some(StringValue("OTC-BB".into())));
            assert_eq!(new_array, Array::from(vec![
                StringValue("NYSE".into()),
                StringValue("NASDAQ".into()),
                StringValue("AMEX".into()),
            ]));
        }

        #[test]
        fn test_push_all() {
            let array_0 = create_array();
            let mut array_1 = Array::new();
            array_1.push_all(vec![
                StringValue("NYSE".into()),
                StringValue("NASDAQ".into()),
                StringValue("AMEX".into()),
                StringValue("OTC-BB".into()),
            ]);
            assert_eq!(array_0, array_1)
        }

        #[test]
        fn test_map_and_iter() {
            let array = create_array()
                .map(|s| s.matches(&StringValue("AMEX".into())));

            let mut sb = String::new();
            for v in array.iter() {
                sb.extend(v.unwrap_value().chars());
                sb.extend(", ".chars())
            }
            assert_eq!(sb, "false, false, true, false, ")
        }

        #[test]
        fn test_to_array() {
            let array = create_array();
            assert_eq!(array.to_tuple(), Tuple::new(vec![
                StringValue("NYSE".into()),
                StringValue("NASDAQ".into()),
                StringValue("AMEX".into()),
                StringValue("OTC-BB".into()),
            ]));
        }

        fn create_array() -> Array {
            let mut array = Array::new();
            array.push(StringValue("NYSE".into()));
            array.push(StringValue("NASDAQ".into()));
            array.push(StringValue("AMEX".into()));
            array.push(StringValue("OTC-BB".into()));
            array
        }

        #[test]
        fn test_array_assignment() {
            verify_exact_value(r#"
                [a, b, c, d] := [2, 3, 5, 7]
                a + b + c + d
            "#, Number(I64Value(17)))
        }

        #[test]
        fn test_array_literal() {
            verify_exact_code("[0, 1, 3, 5]", "[0, 1, 3, 5]");
        }

        #[test]
        fn test_array_element_at_index() {
            verify_exact_code("[-0.01, 8.25, 3.8, -4.5][2]", "3.8");
        }

        #[test]
        fn test_array_addition_numbers() {
            verify_exact_code("[13, 2, 56, 12, 67, 2] + 1",
                              "[14, 3, 57, 13, 68, 3]");
        }

        #[test]
        fn test_array_plus_array_with_numbers() {
            verify_exact_code(
                "[13, 2, 56, 12] + [0, 1, 3, 5]",
                "[[13, 14, 16, 18], [2, 3, 5, 7], [56, 57, 59, 61], [12, 13, 15, 17]]");
        }

        #[test]
        fn test_array_times_array_with_numbers() {
            verify_exact_code(
                "[13, 2, 56, 12] * [0, 1, 3, 5]",
                "[[0, 13, 39, 65], [0, 2, 6, 10], [0, 56, 168, 280], [0, 12, 36, 60]]");
        }

        #[test]
        fn test_array_addition_string() {
            verify_exact_code("['cat', 'dog'] + ['mouse', 'rat']",
                              r#"[["mousecat", "ratcat"], ["mousedog", "ratdog"]]"#);
        }

        #[test]
        fn test_array_concatenation() {
            verify_exact_code("['cat', 'dog'] ++ ['mouse', 'rat']",
                              r#"["cat", "dog", "mouse", "rat"]"#);
        }

        #[test]
        fn test_array_multiplication_numbers() {
            verify_exact_code("[13, 2, 56, 12, 67, 2] * 2",
                              "[26, 4, 112, 24, 134, 4]");
        }

        #[test]
        fn test_array_multiplication_strings() {
            verify_exact_code("['cat', 'dog'] * 3",
                              r#"["catcatcat", "dogdogdog"]"#);
        }

        #[test]
        fn test_array_ranges() {
            verify_exact_code("0..4", r#"[0, 1, 2, 3]"#);
        }
    }

    /// Unit tuple tests
    #[cfg(test)]
    mod tuple_tests {
        use crate::data_types::DataType;
        use crate::data_types::DataType::{StringType, TupleType};
        use crate::number_kind::NumberKind;
        use crate::numbers::Numbers::F64Value;
        use crate::sequences::{Array, Sequence, Tuple};
        use crate::testdata::verify_exact_code;
        use crate::typed_values::TypedValue::{Number, StringValue};

        #[test]
        fn test_contains_positive() {
            let tuple = create_tuple();
            assert!(tuple.contains(&StringValue("NYSE".into())));
        }

        #[test]
        fn test_contains_negative() {
            let tuple = create_tuple();
            assert!(!tuple.contains(&StringValue("NASDAQ".into())));
        }

        #[test]
        fn test_get() {
            let tuple = create_tuple();
            assert_eq!(tuple.get(1), Some(StringValue("NYSE".into())));
        }

        #[test]
        fn test_get_or_else_positive() {
            let tuple = create_tuple();
            assert_eq!(tuple.get_or_else(1, StringValue("N/A".into())), StringValue("NYSE".into()));
        }

        #[test]
        fn test_get_or_else_negative() {
            let tuple = create_tuple();
            assert_eq!(tuple.get_or_else(3, StringValue("N/A".into())), StringValue("N/A".into()));
        }

        #[test]
        fn test_get_type() {
            let tuple = create_tuple();
            assert_eq!(tuple.get_type(), TupleType(vec![
                DataType::FixedSizeType(StringType.into(), 3),
                DataType::FixedSizeType(StringType.into(), 4),
                DataType::NumberType(NumberKind::F64Kind)
            ]));
        }

        #[test]
        fn test_len() {
            let tuple = create_tuple();
            assert_eq!(tuple.len(), 3);
        }

        #[test]
        fn test_map_and_iter() {
            let tuple = create_tuple()
                .map(|s| s.matches(&StringValue("NYSE".into())));

            let mut sb = String::new();
            for v in tuple.iter() {
                sb.extend(v.unwrap_value().chars());
                sb.extend(", ".chars())
            }
            assert_eq!(sb, "false, true, false, ")
        }

        #[test]
        fn test_rev() {
            let tuple = create_tuple();
            assert_eq!(tuple.rev(), Tuple::new(vec![
                Number(F64Value(23.66)),
                StringValue("NYSE".into()),
                StringValue("ABC".into()),
            ]));
        }

        #[test]
        fn test_to_array() {
            let tuple = create_tuple();
            assert_eq!(tuple.to_array(), Array::from(vec![
                StringValue("ABC".into()),
                StringValue("NYSE".into()),
                Number(F64Value(23.66)),
            ]));
        }

        #[test]
        fn test_values() {
            let tuple = create_tuple();
            assert_eq!(tuple.get_values(), vec![
                StringValue("ABC".into()),
                StringValue("NYSE".into()),
                Number(F64Value(23.66)),
            ]);
        }

        #[test]
        fn test_tuple_assignment() {
            verify_exact_code(r#"
                (a, b, c) := (3, 5, 7)
                a + b + c
            "#, "15")
        }

        #[test]
        fn test_tuple_addition() {
            verify_exact_code("(1, 1, 1) + (3, 5, 7)", "(4, 6, 8)")
        }

        #[test]
        fn test_tuple_bitwise_and() {
            verify_exact_code("(0b111, 0b101, 0b001) & (0b111, 0b010, 0b111)", "(7, 0, 1)")
        }

        #[test]
        fn test_tuple_bitwise_or() {
            verify_exact_code("(0b111, 0b101, 0b001) | (0b111, 0b010, 0b000)", "(7, 7, 1)")
        }

        #[test]
        fn test_tuple_bitwise_shl() {
            verify_exact_code("(3, 5, 7) << (1, 1, 1)", "(6, 10, 14)")
        }

        #[test]
        fn test_tuple_bitwise_shr() {
            verify_exact_code("(3, 5, 7) >> (1, 1, 1)", "(1, 2, 3)")
        }

        #[test]
        fn test_tuple_bitwise_xor() {
            verify_exact_code("(0b111, 0b101, 0b001) ^ (0b111, 0b010, 0b100)", "(0, 7, 5)")
        }

        #[test]
        fn test_tuple_division() {
            verify_exact_code("(50, 20, 12) / (2, 4, 6)", "(25, 5, 2)")
        }

        #[test]
        fn test_tuple_exponent() {
            verify_exact_code("(2, 3, 4) ** (2, 2, 2)", "(4, 9, 16)")
        }

        #[test]
        fn test_tuple_element_at_index() {
            verify_exact_code("(-0.01, 8.25, 3.8, -4.5)[2]", "3.8");
        }

        #[test]
        fn test_tuple_multiplication() {
            verify_exact_code("(1, 0, 1) * (4, 3, 9)", "(4, 0, 9)")
        }

        #[test]
        fn test_tuple_neg() {
            verify_exact_code("-(4, 3, 9)", "(-4, -3, -9)")
        }

        #[test]
        fn test_tuple_not() {
            verify_exact_code("!(true, false, false)", "(false, true, true)")
        }

        #[test]
        fn test_tuple_remainder() {
            verify_exact_code("(20, 7, 54) % (3, 3, 9)", "(2, 1, 0)")
        }

        #[test]
        fn test_tuple_subtraction() {
            verify_exact_code("(3, 1, 7) - (1, 5, 1)", "(2, -4, 6)")
        }

        fn create_tuple() -> Tuple {
            Tuple::new(vec![
                StringValue("ABC".into()),
                StringValue("NYSE".into()),
                Number(F64Value(23.66)),
            ])
        }
    }
}