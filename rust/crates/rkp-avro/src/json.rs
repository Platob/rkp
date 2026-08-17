//! Avro's JSON encoding: branch-tagged unions and Latin-1 bytes.

use serde_json::{Map, Value as Json};

use crate::error::{self, Result};
use crate::schema::{Kind, Schema};
use crate::value::Value;

/// Project one value into Avro's JSON encoding.
pub fn to_json(schema: &Schema, index: usize, value: &Value) -> Result<Json> {
    let node = schema.node(index);
    match (node.kind, value) {
        (Kind::Null, Value::Null) => Ok(Json::Null),
        (Kind::Boolean, Value::Boolean(inner)) => Ok(Json::Bool(*inner)),
        (Kind::Int, Value::Int(inner)) => Ok(Json::from(*inner)),
        (Kind::Int, Value::Long(inner)) | (Kind::Long, Value::Long(inner)) => {
            Ok(Json::from(*inner))
        }
        (Kind::Long, Value::Int(inner)) => Ok(Json::from(*inner)),
        (Kind::Float, Value::Float(inner)) => Ok(number(f64::from(*inner))),
        (Kind::Float, Value::Double(inner)) | (Kind::Double, Value::Double(inner)) => {
            Ok(number(*inner))
        }
        (Kind::Double, Value::Float(inner)) => Ok(number(f64::from(*inner))),
        (Kind::Bytes, Value::Bytes(inner)) | (Kind::Fixed, Value::Fixed(inner)) => {
            Ok(Json::String(latin1(inner)))
        }
        (Kind::String, Value::String(inner)) => Ok(Json::String(inner.clone())),
        (Kind::Enum, Value::Enum(symbol)) => match node.symbols.get(*symbol) {
            Some(name) => Ok(Json::String(name.clone())),
            None => error::encode(format!(
                "enum '{}' has no symbol at index {symbol}",
                node.fullname()
            )),
        },
        (Kind::Record, Value::Record(values)) => {
            if values.len() != node.fields.len() {
                return error::encode(format!(
                    "record '{}' expects {} values, got {}",
                    node.fullname(),
                    node.fields.len(),
                    values.len()
                ));
            }
            let mut object = Map::with_capacity(values.len());
            for (field, item) in node.fields.iter().zip(values) {
                object.insert(field.name.clone(), to_json(schema, field.node, item)?);
            }
            Ok(Json::Object(object))
        }
        (Kind::Array, Value::Array(items)) => {
            let mut encoded = Vec::with_capacity(items.len());
            for item in items {
                encoded.push(to_json(schema, node.children[0], item)?);
            }
            Ok(Json::Array(encoded))
        }
        (Kind::Map, Value::Map(entries)) => {
            let mut object = Map::with_capacity(entries.len());
            for (key, item) in entries {
                object.insert(key.clone(), to_json(schema, node.children[0], item)?);
            }
            Ok(Json::Object(object))
        }
        (Kind::Union, Value::Union(branch, inner)) => {
            let child = match node.children.get(*branch) {
                Some(child) => *child,
                None => return error::encode(format!("union has no branch at index {branch}")),
            };
            if schema.node(child).kind == Kind::Null {
                return Ok(Json::Null);
            }
            let mut object = Map::with_capacity(1);
            object.insert(
                schema.node(child).fullname(),
                to_json(schema, child, inner)?,
            );
            Ok(Json::Object(object))
        }
        (kind, other) => error::encode(format!(
            "expected {}, got {}",
            kind.name(),
            other.type_name()
        )),
    }
}

/// Restore one value from Avro's JSON encoding.
pub fn from_json(schema: &Schema, index: usize, value: &Json) -> Result<Value> {
    let node = schema.node(index);
    match node.kind {
        Kind::Null => Ok(Value::Null),
        Kind::Boolean => match value.as_bool() {
            Some(inner) => Ok(Value::Boolean(inner)),
            None => error::decode("expected a JSON boolean"),
        },
        Kind::Int => match value.as_i64() {
            Some(inner) => match i32::try_from(inner) {
                Ok(narrowed) => Ok(Value::Int(narrowed)),
                Err(_) => error::decode(format!("Avro int {inner} is out of range")),
            },
            None => error::decode("expected a JSON integer"),
        },
        Kind::Long => match value.as_i64() {
            Some(inner) => Ok(Value::Long(inner)),
            None => error::decode("expected a JSON integer"),
        },
        Kind::Float => match value.as_f64() {
            Some(inner) => Ok(Value::Float(inner as f32)),
            None => error::decode("expected a JSON number"),
        },
        Kind::Double => match value.as_f64() {
            Some(inner) => Ok(Value::Double(inner)),
            None => error::decode("expected a JSON number"),
        },
        Kind::Bytes => Ok(Value::Bytes(unlatin1(value)?)),
        Kind::Fixed => {
            let raw = unlatin1(value)?;
            if raw.len() != node.size {
                return error::decode(format!(
                    "fixed '{}' requires {} bytes",
                    node.fullname(),
                    node.size
                ));
            }
            Ok(Value::Fixed(raw))
        }
        Kind::String => match value.as_str() {
            Some(text) => Ok(Value::String(text.to_string())),
            None => error::decode("expected a JSON string"),
        },
        Kind::Enum => {
            let text = match value.as_str() {
                Some(text) => text,
                None => return error::decode("expected an enum symbol"),
            };
            match node.symbols.iter().position(|item| item == text) {
                Some(position) => Ok(Value::Enum(position)),
                None => match &node.enum_default {
                    Some(default) => Ok(Value::Enum(
                        node.symbols
                            .iter()
                            .position(|item| item == default)
                            .unwrap_or(0),
                    )),
                    None => {
                        error::decode(format!("'{text}' is not a symbol of '{}'", node.fullname()))
                    }
                },
            }
        }
        Kind::Record => {
            let object = match value.as_object() {
                Some(object) => object,
                None => {
                    return error::decode(format!(
                        "record '{}' expects a JSON object",
                        node.fullname()
                    ));
                }
            };
            let mut values = Vec::with_capacity(node.fields.len());
            for field in &node.fields {
                let item = match object.get(&field.name).or(field.default.as_ref()) {
                    Some(item) => item,
                    None => {
                        return error::decode(format!(
                            "record '{}' is missing field '{}'",
                            node.fullname(),
                            field.name
                        ));
                    }
                };
                values.push(from_json(schema, field.node, item)?);
            }
            Ok(Value::Record(values))
        }
        Kind::Array => {
            let items = match value.as_array() {
                Some(items) => items,
                None => return error::decode("expected a JSON array"),
            };
            let mut values = Vec::with_capacity(items.len());
            for item in items {
                values.push(from_json(schema, node.children[0], item)?);
            }
            Ok(Value::Array(values))
        }
        Kind::Map => {
            let object = match value.as_object() {
                Some(object) => object,
                None => return error::decode("expected a JSON object"),
            };
            let mut entries = Vec::with_capacity(object.len());
            for (key, item) in object {
                entries.push((key.clone(), from_json(schema, node.children[0], item)?));
            }
            Ok(Value::Map(entries))
        }
        Kind::Union => {
            if value.is_null() {
                return match node
                    .children
                    .iter()
                    .position(|child| schema.node(*child).kind == Kind::Null)
                {
                    Some(branch) => Ok(Value::Union(branch, Box::new(Value::Null))),
                    None => error::decode("union does not accept null"),
                };
            }
            let object = match value.as_object() {
                Some(object) if object.len() == 1 => object,
                _ => {
                    return error::decode(
                        "Avro JSON unions must be a single-entry object keyed by branch",
                    );
                }
            };
            let (name, inner) = object.iter().next().expect("length checked");
            for (branch, child) in node.children.iter().enumerate() {
                if schema.node(*child).fullname() == *name {
                    return Ok(Value::Union(
                        branch,
                        Box::new(from_json(schema, *child, inner)?),
                    ));
                }
            }
            error::decode(format!("unknown union branch '{name}'"))
        }
    }
}

fn number(value: f64) -> Json {
    serde_json::Number::from_f64(value)
        .map(Json::Number)
        .unwrap_or(Json::Null)
}

fn latin1(raw: &[u8]) -> String {
    raw.iter().map(|byte| char::from(*byte)).collect()
}

fn unlatin1(value: &Json) -> Result<Vec<u8>> {
    let text = match value.as_str() {
        Some(text) => text,
        None => return error::decode("expected Latin-1 encoded bytes"),
    };
    let mut raw = Vec::with_capacity(text.len());
    for character in text.chars() {
        let point = character as u32;
        if point > 0xff {
            return error::decode("Avro JSON bytes must be Latin-1 characters");
        }
        raw.push(point as u8);
    }
    Ok(raw)
}
