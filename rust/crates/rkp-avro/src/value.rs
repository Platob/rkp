//! The neutral value model the codecs move in and out of.
//!
//! Logical types deliberately have no variants here.  A host binding converts
//! its own date, decimal, and UUID objects while it walks the schema, so the
//! core only ever sees the physical representation the format defines.

use crate::error::{self, Result};
use crate::schema::{Kind, Schema};

/// One decoded Avro value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Bytes(Vec<u8>),
    String(String),
    /// Record fields in schema order.
    Record(Vec<Value>),
    /// The index of an enum symbol.
    Enum(usize),
    Fixed(Vec<u8>),
    Array(Vec<Value>),
    Map(Vec<(String, Value)>),
    /// A union branch index with its value.
    Union(usize, Box<Value>),
}

impl Value {
    /// Return the name of this variant, for error messages.
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "null",
            Value::Boolean(_) => "boolean",
            Value::Int(_) => "int",
            Value::Long(_) => "long",
            Value::Float(_) => "float",
            Value::Double(_) => "double",
            Value::Bytes(_) => "bytes",
            Value::String(_) => "string",
            Value::Record(_) => "record",
            Value::Enum(_) => "enum",
            Value::Fixed(_) => "fixed",
            Value::Array(_) => "array",
            Value::Map(_) => "map",
            Value::Union(_, _) => "union",
        }
    }
}

/// Return whether a value is structurally usable for one schema node.
///
/// Host bindings use this to pick a union branch without duplicating the
/// format's rules.
pub fn matches(schema: &Schema, index: usize, value: &Value) -> bool {
    let node = schema.node(index);
    match (node.kind, value) {
        (Kind::Null, Value::Null) => true,
        (Kind::Boolean, Value::Boolean(_)) => true,
        (Kind::Int, Value::Int(_)) => true,
        (Kind::Int, Value::Long(inner)) => i32::try_from(*inner).is_ok(),
        (Kind::Long, Value::Long(_)) | (Kind::Long, Value::Int(_)) => true,
        (Kind::Float, Value::Float(_)) | (Kind::Float, Value::Double(_)) => true,
        (Kind::Double, Value::Double(_)) | (Kind::Double, Value::Float(_)) => true,
        (Kind::Bytes, Value::Bytes(_)) => true,
        (Kind::String, Value::String(_)) => true,
        (Kind::Record, Value::Record(fields)) => fields.len() == node.fields.len(),
        (Kind::Enum, Value::Enum(symbol)) => *symbol < node.symbols.len(),
        (Kind::Fixed, Value::Fixed(bytes)) => bytes.len() == node.size,
        (Kind::Array, Value::Array(_)) => true,
        (Kind::Map, Value::Map(_)) => true,
        (Kind::Union, Value::Union(branch, inner)) => node
            .children
            .get(*branch)
            .is_some_and(|child| matches(schema, *child, inner)),
        _ => false,
    }
}

/// Return the first union branch a value fits, if any.
pub fn resolve_branch(schema: &Schema, index: usize, value: &Value) -> Result<usize> {
    let node = schema.node(index);
    if node.kind != Kind::Union {
        return error::encode("resolve_branch expects a union node");
    }
    for (branch, child) in node.children.iter().enumerate() {
        if matches(schema, *child, value) {
            return Ok(branch);
        }
    }
    error::encode(format!(
        "value of type {} does not match any union branch",
        value.type_name()
    ))
}
