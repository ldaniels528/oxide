////////////////////////////////////////////////////////////////////
// fields module
////////////////////////////////////////////////////////////////////



use serde::Serialize;

use crate::data_types::DataType;

use crate::field_metadata::FieldMetadata;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue::*;
use crate::typed_values::TypedValue;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Field {
    pub(crate) metadata: FieldMetadata,
    pub(crate) value: TypedValue,
}

impl Field {
    pub fn decode(data_type: &DataType, buffer: &Vec<u8>, offset: usize) -> Self {
        let metadata: FieldMetadata = FieldMetadata::decode(buffer[offset]);
        let value: TypedValue = if metadata.is_active {
            TypedValue::decode(&data_type, buffer, offset + 1)
        } else { NullValue };
        Self::new(metadata, value)
    }

    pub fn encode(&self, capacity: usize) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::with_capacity(capacity);
        buf.push(self.metadata.encode());
        buf.extend(self.value.encode());
        buf.resize(capacity, 0u8);
        buf
    }

    pub fn new(metadata: FieldMetadata, value: TypedValue) -> Self {
        Self { metadata, value }
    }

    pub fn with_default(column: TableColumn) -> Self {
        Self::with_value(column.default_value)
    }

    pub fn with_null() -> Self {
        Self::with_value(NullValue)
    }

    pub fn with_value(value: TypedValue) -> Self {
        Self::new(FieldMetadata {
            is_active: true,
            is_compressed: false,
        }, value)
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode() {
        let buf: Vec<u8> = vec![0x80, 0, 0, 0, 0, 0, 0, 0, 5, b'H', b'e', b'l', b'l', b'o'];
        let field: Field = Field::decode(&StringType(5), &buf, 0);
        assert_eq!(field.metadata.is_active, true);
        assert_eq!(field.metadata.is_compressed, false);
        assert_eq!(field.value, StringValue("Hello".to_string()));
    }

    #[test]
    fn test_encode() {
        let buf: Vec<u8> = vec![0x80, 0, 0, 0, 0, 0, 0, 0, 4, b'H', b'A', b'N', b'D', 0];
        let column: TableColumn = TableColumn::new("symbol", StringType(5), NullValue, 0);
        let field: Field = Field::with_value(StringValue("HAND".to_string()));
        assert_eq!(field.encode(column.max_physical_size), buf);
    }

    #[test]
    fn test_with_default() {
        let column: TableColumn = TableColumn::new("symbol", StringType(4), StringValue("N/A".to_string()), 0);
        let field: Field = Field::with_default(column);
        assert_eq!(field.value, StringValue("N/A".to_string()));
    }

    #[test]
    fn test_with_null() {
        let field: Field = Field::with_null();
        assert_eq!(field.value, NullValue);
    }

    #[test]
    fn test_with_value() {
        let field: Field = Field::with_value(StringValue("INTC".to_string()));
        assert_eq!(field.value, StringValue("INTC".to_string()));
    }
}