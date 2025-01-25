#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Sequence trait
////////////////////////////////////////////////////////////////////

use crate::data_types::DataType;
use crate::data_types::DataType::TupleType;
use crate::data_types::DataType::{ArrayType, Indeterminate, TableType, VaryingType};
use crate::dataframe::Dataframe;
use crate::errors::Errors::Exact;
use crate::row_collection::RowCollection;
use crate::structures::Structures::{Firm, Hard};
use crate::structures::{Structure, Structures};
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{ErrorValue, Structured, TupleValue, Undefined};
use log::error;
use serde::{Deserialize, Serialize};
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

    //fn map(&self, f: fn(&TypedValue) -> TypedValue) -> Self;

    //fn rev(&self) -> Self;

    fn to_array(&self) -> Array;

    fn to_tuple(&self) -> Tuple;
}

/// Represents a linear sequence of [TypedValue]s
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Sequences {
    TheArray(Array),
    TheDataframe(Dataframe),
    TheStructure(Structures),
    TheTuple(Tuple),
}

impl Sequences {}

impl Sequence for Sequences {
    fn contains(&self, item: &TypedValue) -> bool {
        match self {
            Sequences::TheArray(array) => array.contains(item),
            Sequences::TheDataframe(df) =>
                match item {
                    TypedValue::Structured(s) => df.contains(&s.to_row()).is_true(),
                    _ => false
                }
            Sequences::TheStructure(s) =>
                match item {
                    TypedValue::StringValue(name) => s.contains(name),
                    _ => false
                }
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
            Sequences::TheStructure(s) => idx_vec_opt(&s.get_values(), index),
            Sequences::TheTuple(tuple) => tuple.get(index),
        }
    }

    fn get_or_else(&self, index: usize, default: TypedValue) -> TypedValue {
        match self {
            Sequences::TheArray(array) => array.get_or_else(index, default),
            Sequences::TheDataframe(df) =>
                match df.read_one(index) {
                    Ok(None) => TypedValue::Null,
                    Ok(Some(row)) => Structured(Firm(row, df.get_columns().clone())),
                    Err(err) => ErrorValue(Exact(err.to_string()))
                }
            Sequences::TheStructure(s) => idx_vec(&s.get_values(), index),
            Sequences::TheTuple(tuple) => tuple.get_or_else(index, default),
        }
    }

    fn get_type(&self) -> DataType {
        match self {
            Sequences::TheArray(array) => array.get_type(),
            Sequences::TheDataframe(df) => TableType(df.get_parameters(), 0),
            Sequences::TheStructure(s) => s.get_type(),
            Sequences::TheTuple(tuple) => tuple.get_type(),
        }
    }

    fn get_values(&self) -> Vec<TypedValue> {
        match self.clone() {
            Sequences::TheArray(array) => array.get_values(),
            Sequences::TheDataframe(df) => df.to_array().get_values(),
            Sequences::TheStructure(s) => s.get_values(),
            Sequences::TheTuple(tuple) => tuple.get_values(),
        }
    }

    fn iter(&self) -> Iter<'_, TypedValue> {
        match self {
            Sequences::TheArray(array) => array.iter(),
            Sequences::TheDataframe(df) =>
                Box::leak(Box::new(
                    df.iter()
                        .map(|row| Structured(Firm(row, df.get_columns().clone())))
                        .collect::<Vec<_>>()
                )).iter(),
            Sequences::TheStructure(s) => s.iter(),
            Sequences::TheTuple(tuple) => tuple.iter(),
        }
    }

    fn len(&self) -> usize {
        match self {
            Sequences::TheArray(array) => array.len(),
            Sequences::TheDataframe(df) => df.len().unwrap_or(0),
            Sequences::TheStructure(s) => s.len(),
            Sequences::TheTuple(tuple) => tuple.len(),
        }
    }

    fn to_array(&self) -> Array {
        match self {
            Sequences::TheArray(array) => array.to_array(),
            Sequences::TheDataframe(df) => df.to_array(),
            Sequences::TheStructure(s) => s.to_array(),
            Sequences::TheTuple(tuple) => tuple.to_array(),
        }
    }

    fn to_tuple(&self) -> Tuple {
        match self {
            Sequences::TheArray(array) => array.to_tuple(),
            Sequences::TheDataframe(df) => df.to_array().to_tuple(),
            Sequences::TheStructure(s) => s.to_tuple(),
            Sequences::TheTuple(tuple) => tuple.to_tuple(),
        }
    }
}

////////////////////////////////////////////////////////////////////
// Array class
////////////////////////////////////////////////////////////////////

/// Represents an elastic array of values
#[derive(Clone, Debug, Eq, Ord, PartialEq, Serialize, Deserialize)]
pub struct Array {
    items: Vec<TypedValue>,
}

impl Array {

    ////////////////////////////////////////////////////////////////////
    // static methods
    ////////////////////////////////////////////////////////////////////

    /// Constructs an [Array] from  a vector of [TypedValue]s
    pub fn from(
        items: Vec<TypedValue>,
    ) -> Array {
        Self {
            items,
        }
    }

    /// Creates a new empty [Array]
    pub fn new() -> Array {
        Self {
            items: Vec::new(),
        }
    }

    ////////////////////////////////////////////////////////////////////
    // instanced methods
    ////////////////////////////////////////////////////////////////////

    /// Removes all elements from the [Array]
    pub fn clear(&mut self) {
        self.items.clear()
    }

    /// Returns the component type; the resolved internal type
    pub fn get_component_type(&self) -> DataType {
        let kinds = self.items.iter()
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
            [] => Indeterminate,
            [kind] => kind.clone(),
            kinds => VaryingType(kinds.to_vec())
        }
    }

    /// Returns true, if the [Array] is empty
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn map(&self, f: fn(&TypedValue) -> TypedValue) -> Self {
        Array::from(self.items.iter().map(f).collect::<Vec<_>>())
    }

    pub fn push(&mut self, value: TypedValue) {
        self.items.push(value)
    }

    pub fn push_all(&mut self, values: Vec<TypedValue>) {
        self.items.extend(values)
    }

    pub fn pop(&mut self) -> Option<TypedValue> {
        self.items.pop()
    }

    pub fn rev(&self) -> Self {
        Array::from(self.items.iter().rev()
            .map(|i| i.clone())
            .collect::<Vec<_>>())
    }
}

impl Index<usize> for Array {
    type Output = TypedValue;

    fn index(&self, index: usize) -> &Self::Output {
        if index < self.items.len() { &self.items[index] } else { &Undefined }
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
        self.items.contains(item)
    }

    /// Returns the [Option] of a [TypedValue] from specified index
    fn get(&self, index: usize) -> Option<TypedValue> {
        idx_vec_opt(&self.items, index)
    }

    /// Returns the [Option] of a [TypedValue] from specified index or the
    /// default value if not found
    fn get_or_else(&self, index: usize, default: TypedValue) -> TypedValue {
        self.get(index).unwrap_or(default)
    }

    /// Returns the type
    fn get_type(&self) -> DataType {
        ArrayType(self.items.len())
    }

    fn get_values(&self) -> Vec<TypedValue> {
        self.items.clone()
    }

    fn iter(&self) -> Iter<'_, TypedValue> {
        self.items.iter()
    }

    fn len(&self) -> usize {
        self.items.len()
    }

    fn to_array(&self) -> Array {
        Array::from(self.items.clone())
    }

    fn to_tuple(&self) -> Tuple {
        Tuple::new(self.items.clone())
    }
}

////////////////////////////////////////////////////////////////////
//  Tuple class
////////////////////////////////////////////////////////////////////

/// Represents a linear sequence of values
#[derive(Clone, Debug, Eq, Ord, PartialEq, Serialize, Deserialize)]
pub struct Tuple {
    items: Vec<TypedValue>,
}

impl Tuple {

    ////////////////////////////////////////////////////////////////////
    // static methods
    ////////////////////////////////////////////////////////////////////

    /// Constructs a [Tuple] from  a vector of [TypedValue]s
    pub fn new(
        items: Vec<TypedValue>,
    ) -> Self {
        Self { items }
    }

    ////////////////////////////////////////////////////////////////////
    // instanced methods
    ////////////////////////////////////////////////////////////////////

    pub fn map(&self, f: fn(&TypedValue) -> TypedValue) -> Self {
        Self::new(self.items.iter().map(f).collect::<Vec<_>>())
    }

    pub fn rev(&self) -> Self {
        Self::new(self.items.iter().rev()
            .map(|i| i.clone())
            .collect::<Vec<_>>())
    }
}

impl Index<usize> for Tuple {
    type Output = TypedValue;

    fn index(&self, index: usize) -> &Self::Output {
        if index < self.items.len() { &self.items[index] } else { &Undefined }
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
        self.items.contains(item)
    }

    /// Returns the [Option] of a [TypedValue] from specified index
    fn get(&self, index: usize) -> Option<TypedValue> {
        idx_vec_opt(&self.items, index)
    }

    /// Returns the [Option] of a [TypedValue] from specified index or the
    /// default value if not found
    fn get_or_else(&self, index: usize, default: TypedValue) -> TypedValue {
        self.get(index).unwrap_or(default)
    }

    /// Returns the type
    fn get_type(&self) -> DataType {
        TupleType(self.items.iter().map(|v| v.get_type()).collect())
    }

    fn get_values(&self) -> Vec<TypedValue> {
        self.items.clone()
    }

    fn iter(&self) -> Iter<'_, TypedValue> {
        self.items.iter()
    }

    fn len(&self) -> usize {
        self.items.len()
    }

    fn to_array(&self) -> Array {
        Array::from(self.items.clone())
    }

    fn to_tuple(&self) -> Tuple {
        Tuple::new(self.items.clone())
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

/// Unit tests
#[cfg(test)]
mod tests {
    /// Unit core tests
    #[cfg(test)]
    mod core_tests {}

    /// Unit array tests
    #[cfg(test)]
    mod array_tests {
        use crate::data_types::DataType::{ArrayType, BooleanType, StringType, VaryingType};
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
            assert_eq!(array.get_component_type(), StringType(5));

            array.push(Boolean(true));
            assert_eq!(array.get_component_type(), VaryingType(vec![
                StringType(5), BooleanType
            ]));

            array.push(StringValue("Hello World".into()));
            array.push(StringValue("Hello World".into()));
            assert_eq!(array.get_component_type(), VaryingType(vec![
                StringType(5), BooleanType, StringType(11)
            ]));

            assert_eq!(array.get_type(), ArrayType(4));
        }

        #[test]
        fn test_len() {
            let mut array = create_array();
            assert_eq!(array.len(), 4)
        }

        #[test]
        fn test_pop() {
            let mut array = create_array();
            assert_eq!(array.pop(), Some(StringValue("OTC-BB".into())))
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
            verify_exact(r#"
                [a, b, c, d] := [2, 3, 5, 7]
                a + b + c + d
            "#, Number(I64Value(17)))
        }

        #[test]
        fn test_array_literal() {
            verify_exact_text("[0, 1, 3, 5]", "[0, 1, 3, 5]");
        }

        #[test]
        fn test_array_element_at_index() {
            verify_exact_text("[-0.01, 8.25, 3.8, -4.5][2]", "3.8");
        }

        #[test]
        fn test_array_addition_numbers() {
            verify_exact_text("[13, 2, 56, 12, 67, 2] + 1",
                              "[14, 3, 57, 13, 68, 3]");
        }

        #[test]
        fn test_array_plus_array_with_numbers() {
            verify_exact_text(
                "[13, 2, 56, 12] + [0, 1, 3, 5]",
                "[[13, 14, 16, 18], [2, 3, 5, 7], [56, 57, 59, 61], [12, 13, 15, 17]]");
        }

        #[test]
        fn test_array_times_array_with_numbers() {
            verify_exact_text(
                "[13, 2, 56, 12] * [0, 1, 3, 5]",
                "[[0, 13, 39, 65], [0, 2, 6, 10], [0, 56, 168, 280], [0, 12, 36, 60]]");
        }

        #[test]
        fn test_array_addition_string() {
            verify_exact_text("['cat', 'dog'] + ['mouse', 'rat']",
                              r#"[["mousecat", "ratcat"], ["mousedog", "ratdog"]]"#);
        }

        #[test]
        fn test_array_concatenation() {
            verify_exact_text("['cat', 'dog'] ++ ['mouse', 'rat']",
                              r#"["cat", "dog", "mouse", "rat"]"#);
        }

        #[test]
        fn test_array_multiplication_numbers() {
            verify_exact_text("[13, 2, 56, 12, 67, 2] * 2",
                              "[26, 4, 112, 24, 134, 4]");
        }

        #[test]
        fn test_array_multiplication_strings() {
            verify_exact_text("['cat', 'dog'] * 3",
                              r#"["catcatcat", "dogdogdog"]"#);
        }

        #[test]
        fn test_array_ranges() {
            verify_exact_text("0..4", r#"[0, 1, 2, 3]"#);
        }
    }

    /// Unit tuple tests
    #[cfg(test)]
    mod tuple_tests {
        use crate::data_types::DataType;
        use crate::data_types::DataType::TupleType;
        use crate::number_kind::NumberKind;
        use crate::numbers::Numbers::F32Value;
        use crate::sequences::{Array, Sequence, Tuple};
        use crate::testdata::verify_exact_text;
        use crate::typed_values::TypedValue::{Number, StringValue};

        #[test]
        fn test_contains_positive() {
            let mut tuple = create_tuple();
            assert!(tuple.contains(&StringValue("NYSE".into())));
        }

        #[test]
        fn test_contains_negative() {
            let mut tuple = create_tuple();
            assert!(!tuple.contains(&StringValue("NASDAQ".into())));
        }

        #[test]
        fn test_get() {
            let mut tuple = create_tuple();
            assert_eq!(tuple.get(1), Some(StringValue("NYSE".into())));
        }

        #[test]
        fn test_get_or_else_positive() {
            let mut tuple = create_tuple();
            assert_eq!(tuple.get_or_else(1, StringValue("N/A".into())), StringValue("NYSE".into()));
        }

        #[test]
        fn test_get_or_else_negative() {
            let mut tuple = create_tuple();
            assert_eq!(tuple.get_or_else(3, StringValue("N/A".into())), StringValue("N/A".into()));
        }

        #[test]
        fn test_get_type() {
            let mut tuple = create_tuple();
            assert_eq!(tuple.get_type(), TupleType(vec![
                DataType::StringType(3),
                DataType::StringType(4),
                DataType::NumberType(NumberKind::F32Kind)
            ]));
        }

        #[test]
        fn test_len() {
            let mut tuple = create_tuple();
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
            let mut tuple = create_tuple();
            assert_eq!(tuple.rev(), Tuple::new(vec![
                Number(F32Value(23.66)),
                StringValue("NYSE".into()),
                StringValue("ABC".into()),
            ]));
        }

        #[test]
        fn test_to_array() {
            let mut tuple = create_tuple();
            assert_eq!(tuple.to_array(), Array::from(vec![
                StringValue("ABC".into()),
                StringValue("NYSE".into()),
                Number(F32Value(23.66)),
            ]));
        }

        #[test]
        fn test_values() {
            let mut tuple = create_tuple();
            assert_eq!(tuple.get_values(), vec![
                StringValue("ABC".into()),
                StringValue("NYSE".into()),
                Number(F32Value(23.66)),
            ]);
        }

        #[test]
        fn test_tuple_assignment() {
            verify_exact_text(r#"
                (a, b, c) := (3, 5, 7)
                a + b + c
            "#, "15")
        }

        #[test]
        fn test_tuple_addition() {
            verify_exact_text("(1, 1, 1) + (3, 5, 7)", "(4, 6, 8)")
        }

        #[test]
        fn test_tuple_bitwise_and() {
            verify_exact_text("(0b111, 0b101, 0b001) & (0b111, 0b010, 0b111)", "(7, 0, 1)")
        }

        #[test]
        fn test_tuple_bitwise_or() {
            verify_exact_text("(0b111, 0b101, 0b001) | (0b111, 0b010, 0b000)", "(7, 7, 1)")
        }

        #[test]
        fn test_tuple_bitwise_shl() {
            verify_exact_text("(3, 5, 7) << (1, 1, 1)", "(6, 10, 14)")
        }

        #[test]
        fn test_tuple_bitwise_shr() {
            verify_exact_text("(3, 5, 7) >> (1, 1, 1)", "(1, 2, 3)")
        }

        #[test]
        fn test_tuple_bitwise_xor() {
            verify_exact_text("(0b111, 0b101, 0b001) ^ (0b111, 0b010, 0b100)", "(0, 7, 5)")
        }

        #[test]
        fn test_tuple_division() {
            verify_exact_text("(50, 20, 12) / (2, 4, 6)", "(25, 5, 2)")
        }

        #[test]
        fn test_tuple_exponent() {
            verify_exact_text("(2, 3, 4) ** (2, 2, 2)", "(4, 9, 16)")
        }

        #[test]
        fn test_tuple_element_at_index() {
            verify_exact_text("(-0.01, 8.25, 3.8, -4.5)[2]", "3.8");
        }

        #[test]
        fn test_tuple_multiplication() {
            verify_exact_text("(1, 0, 1) * (4, 3, 9)", "(4, 0, 9)")
        }

        #[test]
        fn test_tuple_neg() {
            verify_exact_text("-(4, 3, 9)", "(-4, -3, -9)")
        }

        #[test]
        fn test_tuple_not() {
            verify_exact_text("!(true, false, false)", "(false, true, true)")
        }

        #[test]
        fn test_tuple_remainder() {
            verify_exact_text("(20, 7, 54) % (3, 3, 9)", "(2, 1, 0)")
        }

        #[test]
        fn test_tuple_subtraction() {
            verify_exact_text("(3, 1, 7) - (1, 5, 1)", "(2, -4, 6)")
        }

        fn create_tuple() -> Tuple {
            Tuple::new(vec![
                StringValue("ABC".into()),
                StringValue("NYSE".into()),
                Number(F32Value(23.66)),
            ])
        }
    }
}