#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Structure trait
////////////////////////////////////////////////////////////////////

use crate::byte_code_compiler::ByteCodeCompiler;
use crate::columns::Column;
use crate::descriptor::Descriptor;
use crate::errors::Errors::Exact;
use crate::expression::Conditions;
use crate::field_metadata::FieldMetadata;
use crate::machine::Machine;
use crate::model_row_collection::ModelRowCollection;
use crate::numbers::Numbers::U64Value;
use crate::parameter::Parameter;
use crate::row_metadata::RowMetadata;
use crate::structures::Structures::{Hard, Soft};
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::*;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use shared_lib::fail;
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::ops::Index;

/// Represents a JSON-like data structure
pub trait Structure {
    fn contains(&self, name: &str) -> bool {
        self.get_tuples().iter()
            .find(|(k, _)| *k == *name)
            .is_some()
    }

    /// Encodes the [Structure] into a byte vector
    fn encode(&self) -> std::io::Result<Vec<u8>>;

    /// Retrieves a field by name
    fn get(&self, name: &str) -> TypedValue {
        self.get_tuples().iter()
            .find(|(my_name, _)| *my_name == *name)
            .map(|(_, v)| v.to_owned())
            .unwrap_or(Undefined)
    }

    /// Retrieves the structure's descriptors
    fn get_descriptors(&self) -> Vec<Descriptor>;

    /// Retrieves the structure's parameters
    fn get_parameters(&self) -> Vec<Parameter>;

    /// Retrieves the state of the structure as key-value pairs
    fn get_tuples(&self) -> Vec<(String, TypedValue)>;

    /// Retrieves the structure values
    fn get_values(&self) -> Vec<TypedValue>;

    /// Writes the internal state to the [Machine]
    fn pollute(&self, ms0: Machine) -> Machine {
        // add all scope variables (includes functions)
        let tuples = self.get_tuples();
        let ms1 = tuples.iter()
            .fold(ms0, |ms, (name, value)| {
                ms.with_variable(name, value.to_owned())
            });
        // add self-reference
        ms1.with_variable("self", Structured(Soft(SoftStructure::from_tuples(tuples))))
    }

    /// Decompiles the structure back to source code
    fn to_code(&self) -> String;

    fn to_hash_map(&self) -> HashMap<String, Value> {
        self.get_tuples().iter()
            .map(|(name, value)| (name.to_string(), value.to_json()))
            .fold(HashMap::new(), |mut m, (name, value)| {
                m.insert(name, value);
                m
            })
    }

    fn to_json(&self) -> Value;

    fn to_row(&self) -> Row {
        Row::new(0, self.get_values())
    }

    fn to_table(&self) -> ModelRowCollection {
        let row = self.to_row();
        let columns = Column::from_parameters(&self.get_parameters());
        ModelRowCollection::from_columns_and_rows(&columns, &vec![row])
    }

    fn update(&self, name: &str, value: TypedValue) -> TypedValue;
}

////////////////////////////////////////////////////////////////////
// Structures enumeration
////////////////////////////////////////////////////////////////////

/// Represents a polymorphic structure; which can be a [HardStructure], [SoftStructure]
/// or a [Row] with [Column]s.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Structures {
    Firm(Row, Vec<Column>),
    Hard(HardStructure),
    Soft(SoftStructure),
}

impl Display for Structures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

impl Structure for Structures {
    /// Encodes the [Structure] into a byte vector
    fn encode(&self) -> std::io::Result<Vec<u8>> {
        ByteCodeCompiler::unwrap_as_result(bincode::serialize(self))
    }

    fn get_descriptors(&self) -> Vec<Descriptor> {
        match self {
            Structures::Firm(row, ..) => row.get_descriptors(),
            Structures::Hard(hs) => hs.get_descriptors(),
            Structures::Soft(ss) => ss.get_descriptors(),
        }
    }

    fn get_parameters(&self) -> Vec<Parameter> {
        match self {
            Structures::Firm(row, ..) => row.get_parameters(),
            Structures::Hard(hs) => hs.get_parameters(),
            Structures::Soft(ss) => ss.get_parameters(),
        }
    }

    fn get_tuples(&self) -> Vec<(String, TypedValue)> {
        match self {
            Structures::Firm(row, columns) => row.get_named_tuples(columns),
            Structures::Hard(hs) => hs.get_tuples(),
            Structures::Soft(ss) => ss.get_tuples(),
        }
    }

    fn get_values(&self) -> Vec<TypedValue> {
        match self {
            Structures::Firm(row, ..) => row.get_values(),
            Structures::Hard(hs) => hs.get_values(),
            Structures::Soft(ss) => ss.get_values(),
        }
    }

    /// Returns the structure as source code
    /// ex: Struct(symbol: String(8), exchange: String(8), last_sale: f64)
    fn to_code(&self) -> String {
        match self {
            Structures::Firm(row, columns) => row.to_code_with_keys(columns),
            Structures::Hard(hs) => hs.to_code(),
            Structures::Soft(ss) => ss.to_code(),
        }
    }

    /// Returns the structure as a JSON object
    fn to_json(&self) -> Value {
        match self {
            Structures::Firm(row, columns) => row.to_json_object(columns),
            Structures::Hard(hs) => hs.to_json(),
            Structures::Soft(ss) => ss.to_json(),
        }
    }

    fn to_table(&self) -> ModelRowCollection {
        match self {
            Structures::Firm(row, ..) => row.to_table(),
            Structures::Hard(hs) => hs.to_table(),
            Structures::Soft(ss) => ss.to_table(),
        }
    }

    fn update(&self, name: &str, value: TypedValue) -> TypedValue {
        match self {
            Structures::Firm(row, ..) => row.update(name, value),
            Structures::Hard(hs) => hs.update(name, value),
            Structures::Soft(ss) => ss.update(name, value),
        }
    }
}

////////////////////////////////////////////////////////////////////
// HardStructure class
////////////////////////////////////////////////////////////////////

/// Represents a schema-based JSON-like data structure
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct HardStructure {
    fields: Vec<Parameter>,
    values: Vec<TypedValue>,
}

impl HardStructure {

    ////////////////////////////////////////////////////////////////////
    //  Constructors
    ////////////////////////////////////////////////////////////////////

    pub fn empty() -> Self {
        Self::from_columns_and_values(Vec::new(), Vec::new())
    }

    pub fn new(fields: Vec<Column>, values: Vec<TypedValue>) -> Self {
        Self::from_columns_and_values(fields, values)
    }

    ////////////////////////////////////////////////////////////////////
    //  Static Methods
    ////////////////////////////////////////////////////////////////////

    pub fn decode(bytes: Vec<u8>) -> std::io::Result<Self> {
        ByteCodeCompiler::unwrap_as_result(bincode::deserialize(bytes.as_slice()))
    }

    pub fn from_columns_and_row(columns: &Vec<Column>, row: &Row) -> HardStructure {
        Self::new(columns.to_owned(), row.get_values())
    }

    pub fn from_columns_and_values(
        columns: Vec<Column>,
        values: Vec<TypedValue>,
    ) -> Self {
        let params = Parameter::from_columns(&columns);
        let my_values = match (params.len(), values.len()) {
            // more fields than values?
            (f, v) if f > v => {
                let mut my_values = values;
                for n in v..f { my_values.push(columns[n].get_default_value()) }
                my_values
            }
            // more values than fields?
            (f, v) if f < v => values[0..f].to_vec(),
            // same length
            (..) => values
        };
        Self { fields: params, values: my_values }
    }

    pub fn from_descriptors_and_values(
        descriptors: &Vec<Descriptor>,
        values: Vec<TypedValue>,
    ) -> std::io::Result<HardStructure> {
        Ok(Self::new(Column::from_descriptors(descriptors)?, values))
    }

    pub fn from_descriptors(
        parameters: &Vec<Descriptor>
    ) -> std::io::Result<HardStructure> {
        let fields = Column::from_descriptors(parameters)?;
        let values = parameters.iter().map(|c|
            c.get_default_value().to_owned()
                .map(|s| TypedValue::wrap_value(s.as_str())
                    .unwrap_or_else(|err| ErrorValue(Exact(err.to_string()))))
                .unwrap_or(Null)
        ).collect();
        Ok(Self::new(fields, values))
    }

    pub fn from_parameters(
        parameters: &Vec<Parameter>
    ) -> HardStructure {
        let fields = Column::from_parameters(parameters);
        let values = parameters.iter()
            .map(|c| c.get_default_value()).collect();
        Self::new(fields, values)
    }

    pub fn from_parameters_and_values(
        parameters: &Vec<Parameter>,
        values: Vec<TypedValue>,
    ) -> HardStructure {
        Self::new(Column::from_parameters(parameters), values)
    }

    ////////////////////////////////////////////////////////////////////
    //  Instance Methods
    ////////////////////////////////////////////////////////////////////

    pub fn get_fields(&self) -> Vec<Parameter> {
        self.fields.to_owned()
    }

    pub fn with_variable(&self, name: &str, value: TypedValue) -> Self {
        match self.fields.iter().position(|col| col.get_name() == name) {
            // if the name exists within the structure, update it.
            Some(index) => {
                let mut hs = self.clone();
                hs.values = hs.values.iter().enumerate()
                    .fold(Vec::new(), |mut list, (n, item)| {
                        list.push(if index == n {
                            value.to_owned()
                        } else {
                            item.to_owned()
                        });
                        list
                    });
                hs
            }
            // otherwise, create a new structure
            None => {
                let mut fields = self.fields.clone();
                fields.push(Parameter::with_default(name, value.get_type(), value.to_owned()));
                let mut values = self.values.clone();
                values.push(value);
                HardStructure::from_parameters_and_values(&fields, values)
            }
        }
    }
}

impl Display for HardStructure {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mapping = self.fields.iter().zip(self.values.iter())
            .map(|(c, v)| format!("\"{}\":{}", c.get_name(), v.to_code()))
            .collect::<Vec<_>>();
        write!(f, "{{{}}}", mapping.join(","))
    }
}

impl Structure for HardStructure {
    /// Encodes the [HardStructure] into a byte vector
    fn encode(&self) -> std::io::Result<Vec<u8>> {
        ByteCodeCompiler::unwrap_as_result(bincode::serialize(self))
    }

    fn get_descriptors(&self) -> Vec<Descriptor> {
        Descriptor::from_parameters(&self.fields)
    }

    fn get_parameters(&self) -> Vec<Parameter> {
        self.fields.clone()
    }

    fn get_tuples(&self) -> Vec<(String, TypedValue)> {
        let mut tuples = Vec::new();
        tuples.extend(self.fields.iter().zip(self.values.iter())
            .map(|(c, v)| (c.get_name().to_string(), v.to_owned()))
            .collect::<Vec<_>>());
        tuples.sort_by(|(s0, t0), (s1, t1)| s1.cmp(&s0));
        tuples
    }

    fn get_values(&self) -> Vec<TypedValue> {
        self.values.to_owned()
    }

    /// Returns the structure as source code
    /// ex: Struct(symbol: String(8), exchange: String(8), last_sale: f64)
    fn to_code(&self) -> String {
        let inside = self.fields.iter().zip(self.values.iter())
            .map(|(field, value)| {
                let field_name = field.get_name();
                let type_name = field.get_data_type()
                    .to_type_declaration()
                    .map_or_else(String::new, |name| format!(": {name}"));
                let field_value = match value {
                    Null | Undefined =>
                        match field.get_default_value() {
                            Null | Undefined => String::new(),
                            other => other.to_code()
                        }
                    other => format!(" = {}", other.to_code())
                };
                format!("{field_name}{type_name}{field_value}")
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!("Struct({inside})")
    }

    /// Returns the structure as a JSON object
    fn to_json(&self) -> Value {
        let mapping = self.fields.iter().zip(self.values.iter())
            .map(|(f, v)|
                (f.get_name().to_string(), match f.get_default_value() {
                    Null | Undefined => v.to_json(),
                    v => v.to_json()
                }))
            .fold(Map::new(), |mut m, (name, value)| {
                m.insert(name, value);
                m
            });
        Value::Object(mapping)
    }

    fn to_table(&self) -> ModelRowCollection {
        ModelRowCollection::from_parameters_and_rows(&self.fields, &vec![self.to_row()])
    }

    fn update(&self, name: &str, value: TypedValue) -> TypedValue {
        Structured(Hard(self.with_variable(name, value)))
    }
}

////////////////////////////////////////////////////////////////////
// SoftStructure class
////////////////////////////////////////////////////////////////////

/// Represents a structure JSON-like data structure
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct SoftStructure {
    tuples: Vec<(String, TypedValue)>,
}

impl SoftStructure {
    pub fn decode(bytes: Vec<u8>) -> std::io::Result<Self> {
        ByteCodeCompiler::unwrap_as_result(bincode::deserialize(bytes.as_slice()))
    }

    pub fn empty() -> Self {
        Self { tuples: Vec::new() }
    }

    pub fn from_tuples(tuples: Vec<(String, TypedValue)>) -> Self {
        Self { tuples }
    }

    pub fn new(tuples: &Vec<(&str, TypedValue)>) -> Self {
        let tuples = tuples.iter()
            .map(|(k, v)| (k.to_string(), v.to_owned()))
            .collect::<Vec<_>>();
        Self { tuples }
    }

    pub fn with_variable(&self, name: &str, value: TypedValue) -> Self {
        let tup_maybe = self.tuples.iter()
            .find(|(k, v)| *k == *name);
        let new_tuples = match tup_maybe {
            None => {
                let mut new_tuples = self.tuples.clone();
                new_tuples.push((name.into(), value));
                new_tuples
            }
            Some(_) =>
                self.tuples.iter().map(|(k, v)| if *k == *name {
                    (k.to_string(), value.clone())
                } else {
                    (k.to_string(), v.to_owned())
                }).collect::<Vec<_>>()
        };
        Self {
            tuples: new_tuples,
        }
    }
}

impl Display for SoftStructure {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_code())
    }
}

impl Structure for SoftStructure {
    /// Encodes the [SoftStructure] into a byte vector
    fn encode(&self) -> std::io::Result<Vec<u8>> {
        ByteCodeCompiler::unwrap_as_result(bincode::serialize(self))
    }

    fn get_descriptors(&self) -> Vec<Descriptor> {
        self.tuples.iter()
            .map(|(k, v)| Descriptor::from_tuple(k, v.to_owned()))
            .collect::<Vec<_>>()
    }

    fn get_parameters(&self) -> Vec<Parameter> {
        self.tuples.iter()
            .map(|(f, v)| Parameter::with_default(f, v.get_type(), v.to_owned()))
            .collect()
    }

    fn get_tuples(&self) -> Vec<(String, TypedValue)> {
        self.tuples.clone()
    }

    fn get_values(&self) -> Vec<TypedValue> {
        self.tuples.iter()
            .map(|(_, v)| v.to_owned())
            .collect()
    }

    /// Returns the structure as source code
    fn to_code(&self) -> String {
        format!("{{{}}}", self.tuples.iter()
            .map(|(name, value)| (name.to_string(), value.to_code()))
            .map(|(k, v)| format!("{k}: {v}"))
            .collect::<Vec<_>>()
            .join(", "))
    }

    /// Returns the structure as a JSON object
    fn to_json(&self) -> Value {
        let mapping = self.tuples.iter()
            .map(|(name, value)| (name.to_string(), value.to_json()))
            .fold(Map::new(), |mut m, (name, value)| {
                m.insert(name, value);
                m
            });
        Value::Object(mapping)
    }

    fn update(&self, name: &str, value: TypedValue) -> TypedValue {
        Structured(Soft(self.with_variable(name, value)))
    }
}

/// Represents a row of a table structure.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Row {
    id: usize,
    values: Vec<TypedValue>,
}

impl Row {

    ////////////////////////////////////////////////////////////////////
    //      Static Methods
    ////////////////////////////////////////////////////////////////////

    /// Computes the total record size (in bytes)
    pub fn compute_record_size(columns: &Vec<Column>) -> usize {
        Row::overhead() + columns.iter()
            .map(|c| c.get_fixed_size()).sum::<usize>()
    }

    /// Decodes the supplied buffer returning a row and its metadata
    pub fn decode(columns: &Vec<Column>, buffer: &Vec<u8>) -> (Self, RowMetadata) {
        // if the buffer is empty, just return an empty row
        if buffer.len() == 0 {
            return (Self::empty(columns), RowMetadata::new(false));
        }
        let metadata = RowMetadata::from_bytes(buffer, 0);
        let id = ByteCodeCompiler::decode_row_id(buffer, 1);
        let values: Vec<TypedValue> = columns.iter().map(|column| {
            column.get_data_type().decode_field_value(&buffer, column.get_offset())
        }).collect();
        (Self::new(id, values), metadata)
    }

    /// Decodes the supplied buffer returning a collection of rows.
    pub fn decode_rows(columns: &Vec<Column>, row_data: Vec<Vec<u8>>) -> Vec<Self> {
        let mut rows = Vec::new();
        for row_bytes in row_data {
            let (row, metadata) = Self::decode(&columns, &row_bytes);
            if metadata.is_allocated { rows.push(row); }
        }
        rows
    }

    /// Returns an empty row.
    pub fn empty(columns: &Vec<Column>) -> Self {
        Self::new(0, columns.iter().map(|_| Null).collect())
    }

    pub fn from_buffer(
        columns: &Vec<Column>,
        buffer: &mut ByteCodeCompiler,
    ) -> std::io::Result<(Self, RowMetadata)> {
        // if the buffer is empty, just return an empty row
        let size = buffer.next_u64();
        if size == 0 {
            return Ok((Self::empty(columns), RowMetadata::new(false)));
        }
        let metadata = RowMetadata::decode(buffer.next_u8());
        let id = buffer.next_row_id();
        let mut values = Vec::new();
        for column in columns {
            let data_type = column.get_data_type();
            let offset = column.get_offset();
            let field = data_type.decode_field_value_bcc(buffer, offset)?;
            values.push(field);
        }
        Ok((Self::new(id as usize, values), metadata))
    }

    pub fn from_json(columns: &Vec<Column>, value: &Value) -> Row {
        let values = columns.iter()
            .map(|column| value.get(column.get_name())
                .map(|value| TypedValue::from_json(value))
                .unwrap_or(Undefined))
            .collect::<Vec<_>>();
        Row::new(0, values)
    }

    pub fn from_tuples(
        id: usize,
        columns: &Vec<Column>,
        tuples: &Vec<(String, TypedValue)>,
    ) -> Self {
        // build a cache of the tuples as a hashmap
        let mut cache = HashMap::new();
        for (name, value) in tuples {
            cache.insert(name.to_string(), value.to_owned());
        }
        // construct the fields
        let mut values = Vec::new();
        for c in columns {
            if let Some(value) = cache.get(c.get_name()) {
                values.push(value.to_owned());
            } else {
                values.push(Undefined)
            }
        }
        Row::new(id, values)
    }

    ////////////////////////////////////////////////////////////////////
    //      Instance Methods
    ////////////////////////////////////////////////////////////////////

    pub fn as_hard(&self, columns: &Vec<Column>) -> HardStructure {
        HardStructure::new(columns.to_owned(), self.values.to_owned())
    }

    pub fn as_soft(&self, columns: &Vec<Column>) -> SoftStructure {
        SoftStructure::from_tuples(self.get_named_tuples(columns))
    }

    /// Returns the binary-encoded equivalent of the row.
    pub fn encode(&self, phys_columns: &Vec<Column>) -> Vec<u8> {
        let capacity = Self::compute_record_size(phys_columns);
        let mut buf = Vec::with_capacity(capacity);
        // include the field metadata and row ID
        buf.push(RowMetadata::new(true).encode());
        buf.extend(ByteCodeCompiler::encode_row_id(self.id));
        // include the fields
        let fmd = FieldMetadata::new(true);
        let bb: Vec<u8> = self.values.iter().zip(phys_columns.iter())
            .flat_map(|(value, column)|
                column.get_data_type().encode_field(value, &fmd, column.get_fixed_size())
            ).collect();
        buf.extend(bb);
        buf.resize(capacity, 0u8);
        buf
    }

    pub fn get(&self, index: usize) -> TypedValue {
        let value_len = self.values.len();
        if index < value_len { self.values[index].to_owned() } else { Undefined }
    }

    fn get_named_tuples(&self, columns: &Vec<Column>) -> Vec<(String, TypedValue)> {
        columns.iter().zip(self.values.iter())
            .fold(vec![], |mut list, (column, value)| {
                list.push((column.get_name().to_string(), value.clone()));
                list
            })
    }

    fn get_row_offset(&self, columns: &Vec<Column>, id: usize) -> u64 {
        (id as u64) * (Self::compute_record_size(columns) as u64)
    }

    pub fn get_values(&self) -> Vec<TypedValue> { self.values.to_owned() }

    pub fn get_id(&self) -> usize { self.id }

    pub fn matches(
        &self,
        machine: &Machine,
        condition: &Option<Conditions>,
        columns: &Vec<Column>,
    ) -> bool {
        if let Some(condition) = condition {
            let machine = machine.with_row(columns, &self);
            match machine.evaluate_cond(condition) {
                Ok((_, Boolean(true) | Null | Undefined)) => true,
                Ok(_) => false,
                Err(..) => false
            }
        } else { true }
    }

    /// Primary Constructor
    pub fn new(id: usize, values: Vec<TypedValue>) -> Self {
        Self { id, values }
    }

    /// Represents the number of bytes before the start of column data, which includes
    /// the embedded row metadata (1-byte) and row ID (4- or 8-bytes)
    pub fn overhead() -> usize { 1 + size_of::<usize>() }

    pub fn pollute(&self, ms: &Machine, columns: &Vec<Column>) -> Machine {
        columns.iter().zip(self.values.iter())
            .fold(ms.clone(), |ms, (column, value)|
                ms.with_variable(column.get_name(), value.clone()))
    }

    pub fn replace_undefined_with_null(&self, columns: &Vec<Column>) -> Row {
        Row::new(self.get_id(), columns.iter().zip(self.get_values().iter())
            .map(|(column, value)| {
                match value {
                    Null | Undefined => column.get_default_value().to_owned(),
                    v => v.to_owned()
                }
            }).collect())
    }

    pub fn rows_to_json(columns: &Vec<Column>, rows: &Vec<Row>) -> Vec<HashMap<String, Value>> {
        rows.iter().fold(Vec::new(), |mut vec, row| {
            vec.push(row.to_json_hash(columns));
            vec
        })
    }

    /// Returns the structure as source code
    pub fn to_code_with_keys(&self, columns: &Vec<Column>) -> String {
        format!("{{{}}}", self.get_named_tuples(columns).iter()
            .map(|(name, value)| (name.to_string(), value.to_code()))
            .map(|(k, v)| format!("{k}: {v}"))
            .collect::<Vec<_>>()
            .join(", "))
    }

    /// Transforms the row into CSV format
    pub fn to_csv(&self) -> String {
        self.get_values().iter()
            .map(|v| v.to_code())
            .collect::<Vec<_>>().join(",")
    }

    /// Transforms the row into JSON format
    pub fn to_json_hash(&self, columns: &Vec<Column>) -> HashMap<String, Value> {
        columns.iter().zip(self.get_values().iter())
            .fold(HashMap::new(), |mut hm, (c, v)| {
                hm.insert(c.get_name().to_string(), v.to_json());
                hm
            })
    }

    /// Returns the structure as a JSON object
    pub fn to_json_object(&self, columns: &Vec<Column>) -> Value {
        let mapping = self.get_named_tuples(columns).iter()
            .map(|(name, value)| (name.to_string(), value.to_json()))
            .fold(Map::new(), |mut m, (name, value)| {
                m.insert(name, value);
                m
            });
        Value::Object(mapping)
    }

    /// Transforms the row into JSON
    pub fn to_json_string(&self, columns: &Vec<Column>) -> String {
        let inside = columns.iter().zip(self.values.iter())
            .map(|(k, v)|
                format!(r#""{}":{}"#, k.get_name(), v.to_code()))
            .collect::<Vec<_>>()
            .join(",");
        format!("{{{}}}", inside)
    }

    pub fn to_string(&self) -> String {
        format!("[{}]", self.values.iter()
            .map(|tv| tv.to_code())
            .collect::<Vec<_>>().join(", "))
    }

    /// Creates a new [Row] from the supplied fields and values
    pub fn transform(
        &self,
        columns: &Vec<Column>,
        field_names: &Vec<String>,
        field_values: &Vec<TypedValue>,
    ) -> std::io::Result<Row> {
        // field and value vectors must have the same length
        let src_values = self.get_values();
        if field_names.len() != field_values.len() {
            return fail(format!("Data mismatch: fields ({}) vs values ({})", field_names.len(), field_values.len()));
        }
        // build a cache (mapping) of field names to values
        let cache = field_names.iter().zip(field_values.iter())
            .fold(HashMap::new(), |mut m, (k, v)| {
                m.insert(k.to_string(), v.to_owned());
                m
            });
        // build the new fields vector
        let new_field_values = columns.iter().zip(src_values.iter())
            .map(|(column, value)| match cache.get(column.get_name()) {
                Some(Null | Undefined) => value.to_owned(),
                Some(tv) => tv.to_owned(),
                None => value.to_owned()
            })
            .collect::<Vec<TypedValue>>();
        // return the transformed row
        Ok(Row::new(self.get_id(), new_field_values))
    }

    /// Returns a [Vec] containing the values in order of the fields within the row.
    pub fn unwrap(&self) -> Vec<&TypedValue> {
        let mut values = Vec::new();
        for value in &self.values { values.push(value) }
        values
    }

    pub fn with_row_id(&self, id: usize) -> Self {
        Self::new(id, self.values.to_owned())
    }

    pub fn with_values(&self, values: Vec<TypedValue>) -> Self {
        Self::new(self.id, values)
    }

    pub fn with_variable(&self, name: &str, value: TypedValue) -> SoftStructure {
        let mut new_tuples = Vec::with_capacity(self.values.len() + 2);
        new_tuples.push(("_id".to_string(), Number(U64Value(self.id as u64))));
        new_tuples.extend(self.get_tuples());
        new_tuples.push((name.to_string(), value));
        SoftStructure::from_tuples(new_tuples)
    }
}

impl Display for Row {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

impl Index<usize> for Row {
    type Output = TypedValue;

    fn index(&self, id: usize) -> &Self::Output {
        &self.values[id]
    }
}

impl Structure for Row {
    /// Encodes the [Row] into a byte vector
    fn encode(&self) -> std::io::Result<Vec<u8>> {
        ByteCodeCompiler::unwrap_as_result(bincode::serialize(self))
    }

    fn get_descriptors(&self) -> Vec<Descriptor> {
        self.get_tuples().iter()
            .map(|(k, v)| Descriptor::from_tuple(k, v.to_owned()))
            .collect::<Vec<_>>()
    }

    fn get_parameters(&self) -> Vec<Parameter> {
        self.get_tuples().iter()
            .map(|(k, v)| Parameter::with_default(k, v.get_type(), v.to_owned()))
            .collect()
    }

    fn get_tuples(&self) -> Vec<(String, TypedValue)> {
        self.values.iter().enumerate()
            .fold(vec![], |mut list, (nth, item)| {
                list.push((format!("{}", (nth as u8 + b'a') as char), item.clone()));
                list
            })
    }

    fn get_values(&self) -> Vec<TypedValue> {
        self.get_tuples().iter()
            .map(|(_, v)| v.to_owned())
            .collect()
    }

    /// Returns the structure as source code
    fn to_code(&self) -> String {
        format!("{{{}}}", self.get_tuples().iter()
            .map(|(name, value)| (name.to_string(), value.to_code()))
            .map(|(k, v)| format!("{k}: {v}"))
            .collect::<Vec<_>>()
            .join(", "))
    }

    /// Returns the structure as a JSON object
    fn to_json(&self) -> Value {
        let mapping = self.get_tuples().iter()
            .map(|(name, value)| (name.to_string(), value.to_json()))
            .fold(Map::new(), |mut m, (name, value)| {
                m.insert(name, value);
                m
            });
        Value::Object(mapping)
    }

    fn update(&self, name: &str, value: TypedValue) -> TypedValue {
        Structured(Soft(self.with_variable(name, value)))
    }
}

/// Unit tests
#[cfg(test)]
mod tests {
    /// Unit tests
    #[cfg(test)]
    mod common_tests {}

    /// Unit tests
    #[cfg(test)]
    mod hard_structure_tests {
        use crate::columns::Column;
        use crate::data_types::DataType::*;
        use crate::data_types::StorageTypes::FixedSize;
        use crate::number_kind::NumberKind::F64Kind;
        use crate::numbers::Numbers::F64Value;
        use crate::parameter::Parameter;
        use crate::row_collection::RowCollection;
        use crate::structures::{HardStructure, Structure};
        use crate::testdata::{make_quote, make_quote_columns, make_quote_descriptors, make_quote_parameters};
        use crate::typed_values::TypedValue::*;

        #[test]
        fn test_encode_decode_nonempty() {
            let columns = make_quote_descriptors();
            let phys_columns = Column::from_descriptors(&columns).unwrap();
            let model = HardStructure::new(phys_columns, vec![
                StringValue("EDF".to_string()),
                StringValue("NYSE".to_string()),
                Number(F64Value(11.11)),
            ]);
            assert_eq!(model.to_json().to_string(), r#"{"exchange":"NYSE","last_sale":11.11,"symbol":"EDF"}"#.to_string());

            let bytes = model.encode().unwrap();
            assert_eq!(model, HardStructure::decode(bytes).unwrap())
        }

        #[test]
        fn test_from_logical_columns_and_values() {
            let columns = make_quote_columns();
            let structure = HardStructure::new(columns, vec![
                StringValue("ABC".to_string()),
                StringValue("NYSE".to_string()),
                Number(F64Value(11.11)),
            ]);
            assert_eq!(structure.get("symbol"), StringValue("ABC".to_string()));
            assert_eq!(structure.get("exchange"), StringValue("NYSE".to_string()));
            assert_eq!(structure.get("last_sale"), Number(F64Value(11.11)));
            assert_eq!(structure.get_values(), vec![
                StringValue("ABC".to_string()),
                StringValue("NYSE".to_string()),
                Number(F64Value(11.11)),
            ]);
            assert_eq!(structure.to_string(), r#"{"symbol":"ABC","exchange":"NYSE","last_sale":11.11}"#)
        }

        #[test]
        fn test_from_physical_columns_and_values() {
            let columns = make_quote_descriptors();
            let phys_columns = Column::from_descriptors(&columns).unwrap();
            let structure = HardStructure::new(phys_columns, vec![
                StringValue("ABC".to_string()),
                StringValue("NYSE".to_string()),
                Number(F64Value(11.11)),
            ]);
            assert_eq!(structure.get("symbol"), StringValue("ABC".to_string()));
            assert_eq!(structure.get("exchange"), StringValue("NYSE".to_string()));
            assert_eq!(structure.get("last_sale"), Number(F64Value(11.11)));
            assert_eq!(structure.get_values(), vec![
                StringValue("ABC".to_string()),
                StringValue("NYSE".to_string()),
                Number(F64Value(11.11)),
            ]);
            assert_eq!(structure.to_string(), r#"{"symbol":"ABC","exchange":"NYSE","last_sale":11.11}"#)
        }

        #[test]
        fn test_from_row() {
            let columns = make_quote_descriptors();
            let phys_columns = Column::from_descriptors(&columns).unwrap();
            let structure = HardStructure::from_columns_and_row(&phys_columns,
                                                                &make_quote(0, "ABC", "AMEX", 11.77),
            );
            assert_eq!(structure.get("symbol"), StringValue("ABC".to_string()));
            assert_eq!(structure.get("exchange"), StringValue("AMEX".to_string()));
            assert_eq!(structure.get("last_sale"), Number(F64Value(11.77)));
            assert_eq!(structure.get_values(), vec![
                StringValue("ABC".to_string()),
                StringValue("AMEX".to_string()),
                Number(F64Value(11.77)),
            ]);
            assert_eq!(structure.to_string(), r#"{"symbol":"ABC","exchange":"AMEX","last_sale":11.77}"#)
        }

        #[test]
        fn test_new() {
            let structure = HardStructure::new(make_quote_columns(), Vec::new());
            assert_eq!(structure.get_fields(), vec![
                Parameter::new("symbol", StringType(FixedSize(8))),
                Parameter::new("exchange", StringType(FixedSize(8))),
                Parameter::new("last_sale", NumberType(F64Kind)),
            ]);
            assert_eq!(structure.get("symbol"), Null);
            assert_eq!(structure.get("exchange"), Null);
            assert_eq!(structure.get("last_sale"), Null);
            assert_eq!(structure.get_values(), vec![Null, Null, Null]);
            assert_eq!(structure.to_string(), r#"{"symbol":null,"exchange":null,"last_sale":null}"#)
        }

        #[test]
        fn test_structure_from_columns() {
            let structure = HardStructure::from_descriptors(&make_quote_descriptors()).unwrap();
            assert_eq!(structure.get("symbol"), Null);
            assert_eq!(structure.get("exchange"), Null);
            assert_eq!(structure.get("last_sale"), Null);
            assert_eq!(structure.get_values(), vec![Null, Null, Null]);
            assert_eq!(structure.to_string(), r#"{"symbol":null,"exchange":null,"last_sale":null}"#)
        }

        #[test]
        fn test_to_code_1() {
            let structure = HardStructure::from_columns_and_values(make_quote_columns(), Vec::new());
            assert_eq!(
                structure.to_code(),
                r#"Struct(symbol: String(8), exchange: String(8), last_sale: f64)"#)
        }

        #[test]
        fn test_to_code_2() {
            let structure = HardStructure::new(make_quote_columns(), vec![
                StringValue("ZZY".into()),
                StringValue("NYSE".into()),
                Number(F64Value(77.66)),
            ]);
            assert_eq!(
                structure.to_code(),
                r#"Struct(symbol: String(8) = "ZZY", exchange: String(8) = "NYSE", last_sale: f64 = 77.66)"#)
        }

        #[test]
        fn test_to_json() {
            let structure = HardStructure::new(make_quote_columns(), vec![
                StringValue("WSKY".into()),
                StringValue("NASDAQ".into()),
                Number(F64Value(17.76)),
            ]);
            assert_eq!(structure.to_json().to_string(), r#"{"exchange":"NASDAQ","last_sale":17.76,"symbol":"WSKY"}"#)
        }

        #[test]
        fn test_to_row() {
            let structure = HardStructure::new(make_quote_columns(), vec![
                StringValue("ZZY".into()),
                StringValue("NYSE".into()),
                Number(F64Value(77.66)),
            ]);
            let values = structure.to_row().get_values();
            assert_eq!(values, vec![
                StringValue("ZZY".into()),
                StringValue("NYSE".into()),
                Number(F64Value(77.66)),
            ])
        }

        #[test]
        fn test_to_table() {
            let phys_columns = make_quote_columns();
            let structure = HardStructure::new(phys_columns.clone(), vec![
                StringValue("ABB".into()),
                StringValue("NYSE".into()),
                Number(F64Value(37.25)),
            ]);
            let table = structure.to_table();
            let values = table.iter()
                .map(|row| row.get_values())
                .collect::<Vec<_>>();
            assert_eq!(values, vec![
                vec![
                    StringValue("ABB".to_string()),
                    StringValue("NYSE".to_string()),
                    Number(F64Value(37.25)),
                ]
            ])
        }

        #[test]
        fn test_with_variable() {
            let structure = HardStructure::new(make_quote_columns(), vec![
                StringValue("ICE".to_string()),
                StringValue("NASDAQ".to_string()),
                Number(F64Value(22.11)),
            ]);

            // build the actual structure
            let new_name = "name";
            let new_value = StringValue("Oxide".into());
            let actual = structure.with_variable(new_name, new_value.to_owned());

            // build the expected structure
            let expected_columns: Vec<Column> = {
                let mut my_params = make_quote_parameters();
                my_params.push(Parameter::with_default(new_name, new_value.get_type(), new_value));
                Column::from_parameters(&my_params)
            };
            let expected = HardStructure::from_columns_and_values(expected_columns, vec![
                StringValue("ICE".to_string()),
                StringValue("NASDAQ".to_string()),
                Number(F64Value(22.11)),
                StringValue("Oxide".into()),
            ]);
            assert_eq!(expected, actual);
        }
    }

    /// Unit tests
    #[cfg(test)]
    mod soft_structure_tests {
        use crate::numbers::Numbers::U8Value;
        use crate::structures::{SoftStructure, Structure};
        use crate::typed_values::TypedValue::{Number, StringValue};

        #[test]
        fn test_encode_decode_empty() {
            let model = SoftStructure::empty();
            assert_eq!(model.to_json().to_string(), r#"{}"#.to_string());

            let bytes = model.encode().unwrap();
            assert_eq!(model, SoftStructure::decode(bytes).unwrap())
        }

        #[test]
        fn test_encode_decode_nonempty() {
            let model = SoftStructure::new(&vec![
                ("first_name", StringValue("Thomas".into())),
                ("last_name", StringValue("Brady".into())),
                ("age", Number(U8Value(41))),
            ]);

            assert_eq!(
                model.to_code(),
                r#"{first_name: "Thomas", last_name: "Brady", age: 41}"#.to_string());

            assert_eq!(
                model.to_json().to_string(),
                r#"{"age":41,"first_name":"Thomas","last_name":"Brady"}"#.to_string());

            let bytes = model.encode().unwrap();
            assert_eq!(model, SoftStructure::decode(bytes).unwrap())
        }
    }

    // Unit tests
    #[cfg(test)]
    mod row_tests {
        use crate::numbers::Numbers::*;
        use crate::structures::Row;
        use crate::testdata::{make_quote, make_quote_columns};
        use crate::typed_values::TypedValue::*;

        #[test]
        fn test_make_quote() {
            let row = make_quote(187, "KING", "YHWH", 100.00);
            assert_eq!(row, Row {
                id: 187,
                values: vec![
                    StringValue("KING".into()),
                    StringValue("YHWH".into()),
                    Number(F64Value(100.00)),
                ],
            });
        }

        #[test]
        fn test_decode() {
            let buf: Vec<u8> = vec![
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 187,
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'M', b'A', b'N', b'A', 0, 0, 0, 0,
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E', 0, 0, 0, 0,
                0b1000_0000, 64, 83, 150, 102, 102, 102, 102, 102,
            ];
            let (row, rmd) = Row::decode(&make_quote_columns(), &buf);
            assert!(rmd.is_allocated);
            assert_eq!(row, make_quote(187, "MANA", "NYSE", 78.35));
        }

        #[test]
        fn test_decode_rows() {
            let columns = make_quote_columns();
            let rows_a = vec![
                make_quote(0, "BEAM", "NYSE", 11.99),
                make_quote(1, "LITE", "AMEX", 78.35),
            ];
            let rows_b = Row::decode_rows(&columns, vec![vec![
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 0,
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'B', b'E', b'A', b'M', 0, 0, 0, 0,
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E', 0, 0, 0, 0,
                0b1000_0000, 64, 39, 250, 225, 71, 174, 20, 123,
            ], vec![
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 1,
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'L', b'I', b'T', b'E', 0, 0, 0, 0,
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'A', b'M', b'E', b'X', 0, 0, 0, 0,
                0b1000_0000, 64, 83, 150, 102, 102, 102, 102, 102,
            ]]);
            assert_eq!(rows_a, rows_b);
        }

        #[test]
        fn test_empty() {
            let columns = make_quote_columns();
            let row_a = Row::empty(&columns);
            let row_b = Row::new(0, vec![Null, Null, Null]);
            assert_eq!(row_a, row_b);
        }

        #[test]
        fn test_encode() {
            let phys_columns = make_quote_columns();
            let row = make_quote(255, "RED", "NYSE", 78.35);
            assert_eq!(row.encode(&phys_columns), vec![
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 255,
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 3, b'R', b'E', b'D', 0, 0, 0, 0, 0,
                0b1000_0000, 0, 0, 0, 0, 0, 0, 0, 4, b'N', b'Y', b'S', b'E', 0, 0, 0, 0,
                0b1000_0000, 64, 83, 150, 102, 102, 102, 102, 102,
            ]);
        }

        #[test]
        fn test_fields_by_index() {
            let row = make_quote(213, "YRU", "OTC", 88.44);
            assert_eq!(row.id, 213);
            assert_eq!(row[0], StringValue("YRU".into()));
            assert_eq!(row[1], StringValue("OTC".into()));
            assert_eq!(row[2], Number(F64Value(88.44)));
        }

        #[test]
        fn test_get() {
            let row = Row::new(111, vec![
                StringValue("GE".into()), StringValue("NYSE".into()), Number(F64Value(48.88)),
            ]);
            assert_eq!(row.get(0), StringValue("GE".into()));
            assert_eq!(row.get(1), StringValue("NYSE".into()));
            assert_eq!(row.get(2), Number(F64Value(48.88)));
            assert_eq!(row.get(3), Undefined);
        }

        #[test]
        fn test_to_row_offset() {
            let phys_columns = make_quote_columns();
            let row = Row::new(111, vec![
                StringValue("GE".into()), StringValue("NYSE".into()), Number(F64Value(48.88)),
            ]);
            assert_eq!(row.get_row_offset(&phys_columns, 2), 2 * Row::compute_record_size(&phys_columns) as u64);
        }

        #[test]
        fn test_to_string() {
            let row = make_quote(106, "XRS", "NYSE", 55.44);
            assert_eq!(row.to_string(), r#"["XRS", "NYSE", 55.44]"#.to_string());
        }

        #[test]
        fn test_unwrap() {
            let row = make_quote(100, "ZZZ", "AMEX", 0.9876);
            assert_eq!(row.id, 100);
            assert_eq!(row.unwrap(), vec![
                &StringValue("ZZZ".into()), &StringValue("AMEX".into()), &Number(F64Value(0.9876)),
            ]);
        }

        #[test]
        fn test_with_variable() {
            let row = make_quote(100, "ABC", "NYSE", 19.86);
            let obj = row.with_variable("n", Number(I64Value(32)));
            assert_eq!(obj.to_string(), r#"{_id: 100, a: "ABC", b: "NYSE", c: 19.86, n: 32}"#);
        }

        #[test]
        fn test_as_struct_with_variable() {
            let obj =
                make_quote(100, "ABC", "NYSE", 19.88)
                    .as_hard(&make_quote_columns())
                    .with_variable("last_sale", Number(F64Value(19.87)));
            assert_eq!(obj.to_string(), r#"{"symbol":"ABC","exchange":"NYSE","last_sale":19.87}"#);
        }
    }
}