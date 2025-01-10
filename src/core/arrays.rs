#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Array class
////////////////////////////////////////////////////////////////////

use crate::data_types::DataType;
use crate::data_types::DataType::{ArrayType, VaryingType};
use crate::row_collection::RowCollection;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::Undefined;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::ops::Index;
use std::slice::Iter;

/// Represents an array of type T
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

    /// Returns true, if the [Array] contains the specified item
    pub fn contains(&self, item: &TypedValue) -> bool {
        self.items.contains(item)
    }

    /// Returns the [Option] of a [TypedValue] from specified index
    pub fn get(&self, index: usize) -> Option<TypedValue> {
        if index < self.items.len() { Some(self.items[index].clone()) } else { None }
    }

    /// Returns the [Option] of a [TypedValue] from specified index or the
    /// default value if not found
    pub fn get_or_else(&self, index: usize, default: TypedValue) -> TypedValue {
        self.get(index).unwrap_or(default)
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
            [kind] => kind.clone(),
            kinds => VaryingType(kinds.to_vec())
        }
    }

    /// Returns the type
    pub fn get_type(&self) -> DataType {
        ArrayType(self.items.len())
    }

    /// Returns true, if the [Array] is empty
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn iter(&self) -> Iter<'_, TypedValue> {
        self.items.iter()
    }

    pub fn len(&self) -> usize {
        self.items.len()
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

    pub fn values(&self) -> &Vec<TypedValue> {
        &self.items
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
        self.values().partial_cmp(rhs.values())
    }
}

/// Unit tests
#[cfg(test)]
mod tests {
    use crate::arrays::Array;
    use crate::data_types::DataType::{ArrayType, BooleanType, StringType, VaryingType};
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

    fn create_array() -> Array {
        let mut array = Array::new();
        array.push(StringValue("NYSE".into()));
        array.push(StringValue("NASDAQ".into()));
        array.push(StringValue("AMEX".into()));
        array.push(StringValue("OTC-BB".into()));
        array
    }
}