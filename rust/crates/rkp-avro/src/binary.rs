//! Avro's binary encoding, driven directly by the schema arena.

use crate::error::{self, Result};
use crate::schema::{Kind, Schema};
use crate::value::Value;

/// Append one zig-zag encoded variable-length integer.
pub fn write_long(value: i64, out: &mut Vec<u8>) {
    let mut encoded = ((value << 1) ^ (value >> 63)) as u64;
    while encoded & !0x7f != 0 {
        out.push(((encoded & 0x7f) | 0x80) as u8);
        encoded >>= 7;
    }
    out.push(encoded as u8);
}

/// A cursor over encoded bytes.
pub struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
    limit: usize,
}

impl<'a> Reader<'a> {
    /// Start reading at ``pos``, refusing to read past ``limit``.
    pub fn new(data: &'a [u8], pos: usize, limit: usize) -> Reader<'a> {
        Reader { data, pos, limit }
    }

    /// Start reading a whole buffer.
    pub fn whole(data: &'a [u8]) -> Reader<'a> {
        Reader {
            data,
            pos: 0,
            limit: data.len(),
        }
    }

    /// Return the cursor position.
    pub fn pos(&self) -> usize {
        self.pos
    }

    /// Move the cursor.
    pub fn seek(&mut self, pos: usize) {
        self.pos = pos;
    }

    /// Return the exclusive read limit.
    pub fn limit(&self) -> usize {
        self.limit
    }

    /// Return the bytes left before the limit.
    pub fn remaining(&self) -> usize {
        self.limit.saturating_sub(self.pos)
    }

    /// Read one zig-zag encoded variable-length integer.
    pub fn read_long(&mut self) -> Result<i64> {
        let mut result: u64 = 0;
        let mut shift = 0;
        loop {
            if self.pos >= self.limit {
                return error::decode("truncated Avro variable-length integer");
            }
            let byte = self.data[self.pos];
            self.pos += 1;
            result |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
            if shift > 63 {
                return error::decode("Avro variable-length integer is too wide");
            }
        }
        Ok(((result >> 1) as i64) ^ -((result & 1) as i64))
    }

    /// Read a fixed number of raw bytes.
    pub fn read_bytes(&mut self, size: usize) -> Result<&'a [u8]> {
        let end = match self.pos.checked_add(size) {
            Some(end) => end,
            None => return error::decode("Avro payload length overflows"),
        };
        if end > self.limit {
            return error::decode("truncated Avro payload");
        }
        let slice = &self.data[self.pos..end];
        self.pos = end;
        Ok(slice)
    }
}

/// Encode one value against a schema node.
pub fn encode_node(schema: &Schema, index: usize, value: &Value, out: &mut Vec<u8>) -> Result<()> {
    let node = schema.node(index);
    match (node.kind, value) {
        (Kind::Null, Value::Null) => Ok(()),
        (Kind::Boolean, Value::Boolean(inner)) => {
            out.push(u8::from(*inner));
            Ok(())
        }
        (Kind::Int, Value::Int(inner)) => {
            write_long(i64::from(*inner), out);
            Ok(())
        }
        (Kind::Int, Value::Long(inner)) => match i32::try_from(*inner) {
            Ok(narrowed) => {
                write_long(i64::from(narrowed), out);
                Ok(())
            }
            Err(_) => error::encode(format!("value {inner} does not fit in an Avro int")),
        },
        (Kind::Long, Value::Long(inner)) => {
            write_long(*inner, out);
            Ok(())
        }
        (Kind::Long, Value::Int(inner)) => {
            write_long(i64::from(*inner), out);
            Ok(())
        }
        (Kind::Float, Value::Float(inner)) => {
            out.extend_from_slice(&inner.to_le_bytes());
            Ok(())
        }
        (Kind::Float, Value::Double(inner)) => {
            out.extend_from_slice(&(*inner as f32).to_le_bytes());
            Ok(())
        }
        (Kind::Double, Value::Double(inner)) => {
            out.extend_from_slice(&inner.to_le_bytes());
            Ok(())
        }
        (Kind::Double, Value::Float(inner)) => {
            out.extend_from_slice(&f64::from(*inner).to_le_bytes());
            Ok(())
        }
        (Kind::Bytes, Value::Bytes(inner)) => {
            write_long(inner.len() as i64, out);
            out.extend_from_slice(inner);
            Ok(())
        }
        (Kind::String, Value::String(inner)) => {
            let encoded = inner.as_bytes();
            write_long(encoded.len() as i64, out);
            out.extend_from_slice(encoded);
            Ok(())
        }
        (Kind::Fixed, Value::Fixed(inner)) => {
            if inner.len() != node.size {
                return error::encode(format!(
                    "fixed '{}' requires {} bytes, got {}",
                    node.fullname(),
                    node.size,
                    inner.len()
                ));
            }
            out.extend_from_slice(inner);
            Ok(())
        }
        (Kind::Enum, Value::Enum(symbol)) => {
            if *symbol >= node.symbols.len() {
                return error::encode(format!(
                    "enum '{}' has no symbol at index {symbol}",
                    node.fullname()
                ));
            }
            write_long(*symbol as i64, out);
            Ok(())
        }
        (Kind::Record, Value::Record(values)) => {
            if values.len() != node.fields.len() {
                return error::encode(format!(
                    "record '{}' expects {} values, got {}",
                    node.fullname(),
                    node.fields.len(),
                    values.len()
                ));
            }
            for (field, item) in node.fields.iter().zip(values) {
                encode_node(schema, field.node, item, out)?;
            }
            Ok(())
        }
        (Kind::Array, Value::Array(items)) => {
            if !items.is_empty() {
                write_long(items.len() as i64, out);
                for item in items {
                    encode_node(schema, node.children[0], item, out)?;
                }
            }
            write_long(0, out);
            Ok(())
        }
        (Kind::Map, Value::Map(entries)) => {
            if !entries.is_empty() {
                write_long(entries.len() as i64, out);
                for (key, item) in entries {
                    let encoded = key.as_bytes();
                    write_long(encoded.len() as i64, out);
                    out.extend_from_slice(encoded);
                    encode_node(schema, node.children[0], item, out)?;
                }
            }
            write_long(0, out);
            Ok(())
        }
        (Kind::Union, Value::Union(branch, inner)) => match node.children.get(*branch) {
            Some(child) => {
                write_long(*branch as i64, out);
                encode_node(schema, *child, inner, out)
            }
            None => error::encode(format!("union has no branch at index {branch}")),
        },
        (kind, other) => error::encode(format!(
            "expected {}, got {}",
            kind.name(),
            other.type_name()
        )),
    }
}

/// Decode one value against a schema node.
pub fn decode_node(schema: &Schema, index: usize, reader: &mut Reader<'_>) -> Result<Value> {
    let node = schema.node(index);
    match node.kind {
        Kind::Null => Ok(Value::Null),
        Kind::Boolean => Ok(Value::Boolean(reader.read_bytes(1)?[0] != 0)),
        Kind::Int => {
            let value = reader.read_long()?;
            match i32::try_from(value) {
                Ok(narrowed) => Ok(Value::Int(narrowed)),
                Err(_) => error::decode(format!("Avro int {value} is out of range")),
            }
        }
        Kind::Long => Ok(Value::Long(reader.read_long()?)),
        Kind::Float => {
            let raw = reader.read_bytes(4)?;
            Ok(Value::Float(f32::from_le_bytes([
                raw[0], raw[1], raw[2], raw[3],
            ])))
        }
        Kind::Double => {
            let raw = reader.read_bytes(8)?;
            Ok(Value::Double(f64::from_le_bytes([
                raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
            ])))
        }
        Kind::Bytes => {
            let size = length(reader.read_long()?)?;
            Ok(Value::Bytes(reader.read_bytes(size)?.to_vec()))
        }
        Kind::String => {
            let size = length(reader.read_long()?)?;
            let raw = reader.read_bytes(size)?;
            match std::str::from_utf8(raw) {
                Ok(text) => Ok(Value::String(text.to_string())),
                Err(_) => error::decode("Avro string is not valid UTF-8"),
            }
        }
        Kind::Fixed => Ok(Value::Fixed(reader.read_bytes(node.size)?.to_vec())),
        Kind::Enum => {
            let symbol = reader.read_long()?;
            if symbol < 0 || symbol as usize >= node.symbols.len() {
                return match &node.enum_default {
                    Some(default) => {
                        let position = node
                            .symbols
                            .iter()
                            .position(|item| item == default)
                            .unwrap_or(0);
                        Ok(Value::Enum(position))
                    }
                    None => error::decode(format!("enum index {symbol} is out of range")),
                };
            }
            Ok(Value::Enum(symbol as usize))
        }
        Kind::Record => {
            let mut values = Vec::with_capacity(node.fields.len());
            for field in &node.fields {
                values.push(decode_node(schema, field.node, reader)?);
            }
            Ok(Value::Record(values))
        }
        Kind::Array => {
            let mut items = Vec::new();
            loop {
                let count = reader.read_long()?;
                if count == 0 {
                    break;
                }
                let count = if count < 0 {
                    // A negative count is followed by the block's byte size.
                    reader.read_long()?;
                    count.unsigned_abs() as usize
                } else {
                    count as usize
                };
                items.reserve(count);
                for _ in 0..count {
                    items.push(decode_node(schema, node.children[0], reader)?);
                }
            }
            Ok(Value::Array(items))
        }
        Kind::Map => {
            let mut entries = Vec::new();
            loop {
                let count = reader.read_long()?;
                if count == 0 {
                    break;
                }
                let count = if count < 0 {
                    reader.read_long()?;
                    count.unsigned_abs() as usize
                } else {
                    count as usize
                };
                entries.reserve(count);
                for _ in 0..count {
                    let size = length(reader.read_long()?)?;
                    let raw = reader.read_bytes(size)?;
                    let key = match std::str::from_utf8(raw) {
                        Ok(text) => text.to_string(),
                        Err(_) => return error::decode("Avro map key is not valid UTF-8"),
                    };
                    entries.push((key, decode_node(schema, node.children[0], reader)?));
                }
            }
            Ok(Value::Map(entries))
        }
        Kind::Union => {
            let branch = reader.read_long()?;
            if branch < 0 || branch as usize >= node.children.len() {
                return error::decode(format!("union branch {branch} is out of range"));
            }
            let child = node.children[branch as usize];
            Ok(Value::Union(
                branch as usize,
                Box::new(decode_node(schema, child, reader)?),
            ))
        }
    }
}

fn length(value: i64) -> Result<usize> {
    if value < 0 {
        return error::decode(format!("negative Avro payload length {value}"));
    }
    Ok(value as usize)
}

/// Encode one value against a schema's root.
pub fn encode(schema: &Schema, value: &Value, out: &mut Vec<u8>) -> Result<()> {
    encode_node(schema, schema.root(), value, out)
}

/// Decode one value against a schema's root.
pub fn decode(schema: &Schema, data: &[u8]) -> Result<Value> {
    let mut reader = Reader::whole(data);
    decode_node(schema, schema.root(), &mut reader)
}
