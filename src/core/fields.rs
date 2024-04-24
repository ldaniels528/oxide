////////////////////////////////////////////////////////////////////
// fields module
////////////////////////////////////////////////////////////////////

use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::data_types::DataType;
use crate::field_metadata::FieldMetadata;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue::*;
use crate::typed_values::TypedValue;

#[derive(Clone, Debug, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Field {
    pub(crate) metadata: FieldMetadata,
    pub(crate) value: TypedValue,
}

impl Field {
    pub fn decode(data_type: &DataType, buffer: &Vec<u8>, offset: usize) -> Self {
        let metadata: FieldMetadata = FieldMetadata::decode(buffer[offset]);
        let value: TypedValue = if metadata.is_active {
            TypedValue::decode(&data_type, buffer, offset + 1)
        } else { Null };
        Self::new(value)
    }

    pub fn encode(&self, capacity: usize) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::with_capacity(capacity);
        buf.push(self.metadata.encode());
        buf.extend(self.value.encode());
        buf.resize(capacity, 0u8);
        buf
    }

    pub fn new(value: TypedValue) -> Self {
        Self { metadata: FieldMetadata::new(true), value }
    }

    pub fn with_default(column: &TableColumn) -> Self {
        Self::with_value(column.default_value.clone())
    }

    pub fn with_null() -> Self {
        Self::with_value(Null)
    }

    pub fn with_value(value: TypedValue) -> Self {
        Self::new(value)
    }
}

impl Display for Field {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Field({})", self.value.unwrap_value())
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::data_types::DataType::StringType;

    use super::*;

    #[test]
    fn test_decode() {
        let buf: Vec<u8> = vec![0x80, 0, 0, 0, 0, 0, 0, 0, 5, b'H', b'e', b'l', b'l', b'o'];
        let field: Field = Field::decode(&StringType(5), &buf, 0);
        assert_eq!(field.metadata.is_active, true);
        assert_eq!(field.metadata.is_compressed, false);
        assert_eq!(field.value, StringValue("Hello".into()));
    }

    #[test]
    fn test_encode() {
        let buf: Vec<u8> = vec![0x80, 0, 0, 0, 0, 0, 0, 0, 4, b'H', b'A', b'N', b'D', 0];
        let column: TableColumn = TableColumn::new("symbol", StringType(5), Null, 0);
        let field: Field = Field::with_value(StringValue("HAND".into()));
        assert_eq!(field.encode(column.max_physical_size), buf);
    }

    #[test]
    fn test_with_default() {
        let column: TableColumn = TableColumn::new("symbol", StringType(4), StringValue("N/A".into()), 0);
        let field: Field = Field::with_default(&column);
        assert_eq!(field.value, StringValue("N/A".into()));
    }

    #[test]
    fn test_with_null() {
        let field: Field = Field::with_null();
        assert_eq!(field.value, Null);
    }

    #[test]
    fn test_with_value() {
        let field: Field = Field::with_value(StringValue("INTC".into()));
        assert_eq!(field.value, StringValue("INTC".into()));
    }
}