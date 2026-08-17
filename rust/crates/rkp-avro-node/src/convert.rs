//! JavaScript value conversion, guided by the schema.
//!
//! The core owns the physical format and knows nothing about host objects.
//! This module owns the other half: which JavaScript value each schema node
//! becomes, and which JavaScript values it accepts back.  The two directions
//! are inverses — whatever [`into_js`] produces, [`into_value`] re-encodes to
//! the same bytes — which is the property `js/test/values.test.js` pins down.
//!
//! | Avro                 | JavaScript                                          |
//! | -------------------- | --------------------------------------------------- |
//! | `null`               | `null` (`undefined` also accepted)                  |
//! | `boolean`            | `boolean`, strictly — never a coercion              |
//! | `int`, `float`, `double` | `number`                                        |
//! | `long`               | `number` while `Number.isSafeInteger`, else `bigint`|
//! | `bytes`, `fixed`     | `Buffer` (any byte view accepted)                   |
//! | `string`             | `string`                                            |
//! | `record`             | plain object keyed by field name (array accepted)   |
//! | `map`                | plain object                                        |
//! | `array`              | `Array`                                             |
//! | `enum`               | `string` symbol                                     |
//! | `union`              | the bare branch value; `null` for the null branch   |
//!
//! Logical types are resolved against what JavaScript can hold losslessly:
//!
//! | logical type                          | JavaScript               |
//! | ------------------------------------- | ------------------------ |
//! | `date`, `timestamp-millis`            | `Date`                   |
//! | `local-timestamp-millis`              | `Date` (UTC fields)      |
//! | `time-millis`, `time-micros`          | `number` since midnight  |
//! | `timestamp-micros`, `timestamp-nanos` | `bigint` of raw units    |
//! | `local-timestamp-micros/-nanos`       | `bigint` of raw units    |
//! | `decimal`                             | decimal `string`         |
//! | `uuid`                                | `string`                 |
//! | `duration`                            | `[months, days, millis]` |
//!
//! Sub-millisecond instants become `bigint` rather than a `Date` because a
//! `Date` would silently drop the precision the file actually carries, and a
//! local timestamp becomes a `Date` read through its **UTC** fields because a
//! zone-free wall clock has no instant to point at.  A time of day stays a
//! plain count: a `Date` would have to invent a date to go with it.  Every one
//! of these also accepts the raw `number`/`bigint`, and every timestamp
//! accepts a `Date`, so writing back what was read is always valid.

use napi::bindgen_prelude::*;
use napi::{Env, JsValue, ValueType, sys};

use rkp_avro::schema::{Kind, Logical, Schema};
use rkp_avro::value::Value;

const MILLIS_PER_DAY: i64 = 86_400_000;
const SAFE_INTEGER: i64 = 9_007_199_254_740_991;

/// Convert one JavaScript value into a core value, guided by a schema node.
pub fn into_value(env: &Env, schema: &Schema, index: usize, value: &Unknown<'_>) -> Result<Value> {
    let node = schema.node(index);
    match node.kind {
        Kind::Null => match value.get_type()? {
            ValueType::Null | ValueType::Undefined => Ok(Value::Null),
            other => Err(encode(format!("expected null, got {}", name_of(other)))),
        },
        Kind::Boolean => Ok(Value::Boolean(boolean_of(value)?)),
        Kind::Int => {
            let number = match &node.logical {
                Some(Logical::Date) => match date_millis(env, value)? {
                    Some(millis) => millis.div_euclid(MILLIS_PER_DAY),
                    None => integer_of(value)?,
                },
                _ => integer_of(value)?,
            };
            match i32::try_from(number) {
                Ok(narrowed) => Ok(Value::Int(narrowed)),
                Err(_) => Err(encode(format!(
                    "value {number} does not fit in an Avro int"
                ))),
            }
        }
        Kind::Long => {
            let number = match &node.logical {
                Some(logical) if is_timestamp(logical) => match date_millis(env, value)? {
                    Some(millis) => scale_timestamp(millis * 1000, logical),
                    None => integer_of(value)?,
                },
                _ => integer_of(value)?,
            };
            Ok(Value::Long(number))
        }
        Kind::Float => Ok(Value::Float(number_of(value)? as f32)),
        Kind::Double => Ok(Value::Double(number_of(value)?)),
        Kind::Bytes => match &node.logical {
            Some(Logical::Decimal { scale, .. }) if !is_buffer(env, value) => {
                let unscaled = decimal_unscaled(value, *scale)?;
                Ok(Value::Bytes(twos_complement(unscaled, None)))
            }
            _ => Ok(Value::Bytes(bytes_of(value)?)),
        },
        Kind::String => match &node.logical {
            Some(Logical::Uuid) => Ok(Value::String(text_of(value)?)),
            _ => match value.get_type()? {
                ValueType::String => Ok(Value::String(text_of(value)?)),
                other => Err(encode(format!("expected string, got {}", name_of(other)))),
            },
        },
        Kind::Fixed => {
            let raw = match &node.logical {
                Some(Logical::Decimal { scale, .. }) if !is_buffer(env, value) => {
                    twos_complement(decimal_unscaled(value, *scale)?, Some(node.size))
                }
                Some(Logical::Uuid) if !is_buffer(env, value) => uuid_bytes(&text_of(value)?)?,
                Some(Logical::Duration) if is_array(env, value) => {
                    let parts = unsafe { value.cast::<Array>()? };
                    let mut raw = Vec::with_capacity(12);
                    for position in 0..parts.len().min(3) {
                        let part: f64 = parts
                            .get(position)?
                            .ok_or_else(|| encode("duration part is missing"))?;
                        raw.extend_from_slice(&(part as u32).to_le_bytes());
                    }
                    raw
                }
                _ => bytes_of(value)?,
            };
            if raw.len() != node.size {
                return Err(encode(format!(
                    "fixed '{}' requires {} bytes, got {}",
                    node.fullname(),
                    node.size,
                    raw.len()
                )));
            }
            Ok(Value::Fixed(raw))
        }
        Kind::Enum => {
            let text = text_of(value)?;
            match node.symbols.iter().position(|symbol| *symbol == text) {
                Some(position) => Ok(Value::Enum(position)),
                None => match &node.enum_default {
                    Some(default) => Ok(Value::Enum(
                        node.symbols
                            .iter()
                            .position(|symbol| symbol == default)
                            .unwrap_or(0),
                    )),
                    None => Err(encode(format!(
                        "'{text}' is not a symbol of enum '{}'",
                        node.fullname()
                    ))),
                },
            }
        }
        Kind::Record => {
            let mut values = Vec::with_capacity(node.fields.len());
            if is_array(env, value) {
                let positional = unsafe { value.cast::<Array>()? };
                if positional.len() as usize != node.fields.len() {
                    return Err(encode(format!(
                        "record '{}' expects {} positional values, got {}",
                        node.fullname(),
                        node.fields.len(),
                        positional.len()
                    )));
                }
                for (position, field) in node.fields.iter().enumerate() {
                    let item: Unknown = positional
                        .get(position as u32)?
                        .ok_or_else(|| encode("positional record value is missing"))?;
                    values.push(into_value(env, schema, field.node, &item)?);
                }
                return Ok(Value::Record(values));
            }
            let row = object_of(value, || {
                encode(format!(
                    "record '{}' expects an object or an array",
                    node.fullname()
                ))
            })?;
            for field in &node.fields {
                let item: Option<Unknown> = row.get(field.name.as_str())?;
                match item {
                    Some(item) if !matches!(item.get_type()?, ValueType::Undefined) => {
                        values.push(into_value(env, schema, field.node, &item)?);
                    }
                    _ => match &field.default {
                        Some(default) => {
                            let item = json_into_js(env, default)?;
                            values.push(into_value(env, schema, field.node, &item)?);
                        }
                        None => {
                            return Err(encode(format!(
                                "record '{}' is missing field '{}'",
                                node.fullname(),
                                field.name
                            )));
                        }
                    },
                }
            }
            Ok(Value::Record(values))
        }
        Kind::Array => {
            if !is_array(env, value) {
                return Err(encode(format!(
                    "expected an array, got {}",
                    name_of(value.get_type()?)
                )));
            }
            let items = unsafe { value.cast::<Array>()? };
            let mut converted = Vec::with_capacity(items.len() as usize);
            for position in 0..items.len() {
                let item: Unknown = items
                    .get(position)?
                    .ok_or_else(|| encode("array element is missing"))?;
                converted.push(into_value(env, schema, node.children[0], &item)?);
            }
            Ok(Value::Array(converted))
        }
        Kind::Map => {
            if is_array(env, value) || is_buffer(env, value) {
                // Arrays and buffers are objects too; letting them through
                // would make `["array", "map"]` unions resolve by accident.
                return Err(encode("expected a map object, got an array or a buffer"));
            }
            let mapping = object_of(value, || {
                encode(format!(
                    "expected a map object, got {}",
                    value
                        .get_type()
                        .map(name_of)
                        .unwrap_or("an unreadable value")
                ))
            })?;
            let keys = Object::keys(&mapping)?;
            let mut entries = Vec::with_capacity(keys.len());
            for key in keys {
                let item: Unknown = mapping
                    .get(key.as_str())?
                    .ok_or_else(|| encode(format!("map entry '{key}' is missing")))?;
                entries.push((key, into_value(env, schema, node.children[0], &item)?));
            }
            Ok(Value::Map(entries))
        }
        Kind::Union => {
            if matches!(value.get_type()?, ValueType::Null | ValueType::Undefined)
                && let Some(branch) = node
                    .children
                    .iter()
                    .position(|child| schema.node(*child).kind == Kind::Null)
            {
                return Ok(Value::Union(branch, Box::new(Value::Null)));
            }
            for (branch, child) in node.children.iter().enumerate() {
                if schema.node(*child).kind == Kind::Null {
                    continue;
                }
                if let Ok(inner) = into_value(env, schema, *child, value) {
                    return Ok(Value::Union(branch, Box::new(inner)));
                }
            }
            Err(encode(format!(
                "value of type {} does not match any union branch",
                name_of(value.get_type()?)
            )))
        }
    }
}

/// Convert one core value into a JavaScript value, guided by a schema node.
pub fn into_js<'env>(
    env: &'env Env,
    schema: &Schema,
    index: usize,
    value: &Value,
) -> Result<Unknown<'env>> {
    let node = schema.node(index);
    match (node.kind, value) {
        (Kind::Null, _) => Null.into_unknown(env),
        (Kind::Boolean, Value::Boolean(inner)) => inner.into_unknown(env),
        (Kind::Int, Value::Int(inner)) => match &node.logical {
            Some(Logical::Date) => env
                .create_date((i64::from(*inner) * MILLIS_PER_DAY) as f64)?
                .into_unknown(env),
            _ => (*inner).into_unknown(env),
        },
        (Kind::Long, Value::Long(inner)) => match &node.logical {
            // Only millisecond instants survive a `Date` intact.
            Some(logical) if is_date_shaped(logical) => {
                env.create_date(*inner as f64)?.into_unknown(env)
            }
            Some(logical) if is_timestamp(logical) => BigInt::from(*inner).into_unknown(env),
            _ => long_into_js(env, *inner),
        },
        (Kind::Float, Value::Float(inner)) => f64::from(*inner).into_unknown(env),
        (Kind::Double, Value::Double(inner)) => (*inner).into_unknown(env),
        (Kind::Bytes, Value::Bytes(raw)) => match &node.logical {
            Some(Logical::Decimal { scale, .. }) => {
                unscaled_text(from_twos_complement(raw), *scale).into_unknown(env)
            }
            _ => Buffer::from(raw.as_slice()).into_unknown(env),
        },
        (Kind::Fixed, Value::Fixed(raw)) => match &node.logical {
            Some(Logical::Decimal { scale, .. }) => {
                unscaled_text(from_twos_complement(raw), *scale).into_unknown(env)
            }
            Some(Logical::Uuid) => uuid_text(raw).into_unknown(env),
            Some(Logical::Duration) => {
                let parts: Vec<u32> = raw
                    .chunks(4)
                    .map(|chunk| u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                    .collect();
                parts.into_unknown(env)
            }
            _ => Buffer::from(raw.as_slice()).into_unknown(env),
        },
        (Kind::String, Value::String(text)) => text.as_str().into_unknown(env),
        (Kind::Enum, Value::Enum(symbol)) => match node.symbols.get(*symbol) {
            Some(name) => name.as_str().into_unknown(env),
            None => Err(schema_error(format!(
                "enum '{}' has no symbol at index {symbol}",
                node.fullname()
            ))),
        },
        (Kind::Record, Value::Record(values)) => {
            let mut row = Object::new(env)?;
            for (field, item) in node.fields.iter().zip(values) {
                row.set(field.name.as_str(), into_js(env, schema, field.node, item)?)?;
            }
            row.into_unknown(env)
        }
        (Kind::Array, Value::Array(items)) => {
            let mut converted = Vec::with_capacity(items.len());
            for item in items {
                converted.push(into_js(env, schema, node.children[0], item)?);
            }
            converted.into_unknown(env)
        }
        (Kind::Map, Value::Map(entries)) => {
            let mut mapping = Object::new(env)?;
            for (key, item) in entries {
                mapping.set(key.as_str(), into_js(env, schema, node.children[0], item)?)?;
            }
            mapping.into_unknown(env)
        }
        (Kind::Union, Value::Union(branch, inner)) => match node.children.get(*branch) {
            Some(child) => into_js(env, schema, *child, inner),
            None => Err(schema_error(format!(
                "union has no branch at index {branch}"
            ))),
        },
        (kind, other) => Err(schema_error(format!(
            "cannot project {} as {}",
            other.type_name(),
            kind.name()
        ))),
    }
}

/// Convert decoded JSON into plain JavaScript data.
pub fn json_into_js<'env>(env: &'env Env, value: &serde_json::Value) -> Result<Unknown<'env>> {
    match value {
        serde_json::Value::Null => Null.into_unknown(env),
        serde_json::Value::Bool(inner) => (*inner).into_unknown(env),
        serde_json::Value::Number(number) => match number.as_i64() {
            Some(inner) => long_into_js(env, inner),
            None => number.as_f64().unwrap_or(f64::NAN).into_unknown(env),
        },
        serde_json::Value::String(text) => text.as_str().into_unknown(env),
        serde_json::Value::Array(items) => {
            let mut converted = Vec::with_capacity(items.len());
            for item in items {
                converted.push(json_into_js(env, item)?);
            }
            converted.into_unknown(env)
        }
        serde_json::Value::Object(entries) => {
            let mut mapping = Object::new(env)?;
            for (key, item) in entries {
                mapping.set(key.as_str(), json_into_js(env, item)?)?;
            }
            mapping.into_unknown(env)
        }
    }
}

/// Convert plain JavaScript data into JSON.
pub fn js_into_json(env: &Env, value: &Unknown<'_>) -> Result<serde_json::Value> {
    match value.get_type()? {
        ValueType::Null | ValueType::Undefined => Ok(serde_json::Value::Null),
        ValueType::Boolean => Ok(serde_json::Value::Bool(boolean_of(value)?)),
        ValueType::Number => {
            let number = number_of(value)?;
            if number.fract() == 0.0 && number.abs() <= SAFE_INTEGER as f64 {
                return Ok(serde_json::Value::from(number as i64));
            }
            Ok(serde_json::Number::from_f64(number)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null))
        }
        ValueType::BigInt => Ok(serde_json::Value::from(integer_of(value)?)),
        ValueType::String => Ok(serde_json::Value::String(text_of(value)?)),
        ValueType::Object if is_array(env, value) => {
            let items = unsafe { value.cast::<Array>()? };
            let mut array = Vec::with_capacity(items.len() as usize);
            for position in 0..items.len() {
                let item: Unknown = items
                    .get(position)?
                    .ok_or_else(|| encode("array element is missing"))?;
                array.push(js_into_json(env, &item)?);
            }
            Ok(serde_json::Value::Array(array))
        }
        ValueType::Object if is_buffer(env, value) => {
            // Avro's JSON encoding writes bytes as Latin-1 text.
            let raw = bytes_of(value)?;
            Ok(serde_json::Value::String(
                raw.iter().map(|byte| char::from(*byte)).collect(),
            ))
        }
        ValueType::Object => {
            let mapping = object_of(value, || encode("expected a JSON object"))?;
            let mut object = serde_json::Map::new();
            for key in Object::keys(&mapping)? {
                let item: Unknown = mapping
                    .get(key.as_str())?
                    .ok_or_else(|| encode(format!("object entry '{key}' is missing")))?;
                object.insert(key, js_into_json(env, &item)?);
            }
            Ok(serde_json::Value::Object(object))
        }
        other => Err(encode(format!(
            "cannot convert {} into JSON",
            name_of(other)
        ))),
    }
}

fn long_into_js<'env>(env: &'env Env, value: i64) -> Result<Unknown<'env>> {
    // Stay a `number` while that is exact, so ordinary longs read naturally.
    if value.abs() <= SAFE_INTEGER {
        return (value as f64).into_unknown(env);
    }
    BigInt::from(value).into_unknown(env)
}

fn boolean_of(value: &Unknown<'_>) -> Result<bool> {
    // Deliberately not `coerce_to_bool`: a union whose branches include
    // `boolean` must not swallow every other value that happens to be truthy.
    match value.get_type()? {
        ValueType::Boolean => Ok(unsafe { value.cast::<bool>()? }),
        other => Err(encode(format!(
            "expected a boolean, got {}",
            name_of(other)
        ))),
    }
}

fn number_of(value: &Unknown<'_>) -> Result<f64> {
    match value.get_type()? {
        ValueType::Number => Ok(unsafe { value.cast::<f64>()? }),
        ValueType::BigInt => Ok(integer_of(value)? as f64),
        other => Err(encode(format!("expected a number, got {}", name_of(other)))),
    }
}

fn integer_of(value: &Unknown<'_>) -> Result<i64> {
    match value.get_type()? {
        ValueType::Number => {
            let number = unsafe { value.cast::<f64>()? };
            if number.fract() != 0.0 {
                return Err(encode(format!("expected an integer, got {number}")));
            }
            if number.abs() > SAFE_INTEGER as f64 {
                return Err(encode(format!(
                    "value {number} is beyond Number.MAX_SAFE_INTEGER; \
                     pass a bigint to keep an Avro long exact"
                )));
            }
            Ok(number as i64)
        }
        ValueType::BigInt => {
            let (number, lossless) = unsafe { value.cast::<BigInt>()? }.get_i64();
            if !lossless {
                return Err(encode(format!(
                    "value {number} does not fit in an Avro long"
                )));
            }
            Ok(number)
        }
        other => Err(encode(format!(
            "expected an integer, got {}",
            name_of(other)
        ))),
    }
}

fn text_of(value: &Unknown<'_>) -> Result<String> {
    match value.get_type()? {
        ValueType::String => Ok(unsafe { value.cast::<String>()? }),
        other => Err(encode(format!("expected a string, got {}", name_of(other)))),
    }
}

/// Read the bytes behind a `Buffer`, any byte-wide `TypedArray`, or an
/// `ArrayBuffer`.
///
/// A string is deliberately *not* accepted here.  Avro's JSON encoding does
/// spell bytes as Latin-1 text, but accepting that on the binary path would
/// make every `["bytes", "string"]` union resolve to its `bytes` branch.
fn bytes_of(value: &Unknown<'_>) -> Result<Vec<u8>> {
    if let Ok(buffer) = unsafe { value.cast::<Buffer>() } {
        return Ok(buffer.to_vec());
    }
    if let Ok(view) = unsafe { value.cast::<Uint8Array>() } {
        return Ok(view.to_vec());
    }
    if let Ok(raw) = unsafe { value.cast::<ArrayBuffer>() } {
        return Ok(raw.to_vec());
    }
    Err(encode(format!(
        "expected a Buffer, Uint8Array, or ArrayBuffer, got {}",
        name_of(value.get_type()?)
    )))
}

fn object_of<'a, F>(value: &Unknown<'a>, message: F) -> Result<Object<'a>>
where
    F: FnOnce() -> Error,
{
    if !matches!(value.get_type()?, ValueType::Object) {
        return Err(message());
    }
    unsafe { value.cast::<Object>() }
}

fn date_millis(env: &Env, value: &Unknown<'_>) -> Result<Option<i64>> {
    if !is_date(env, value) {
        return Ok(None);
    }
    let moment = unsafe { value.cast::<napi::JsDate>()? };
    Ok(Some(moment.value_of()? as i64))
}

fn is_array(env: &Env, value: &Unknown<'_>) -> bool {
    let mut answer = false;
    unsafe { sys::napi_is_array(env.raw(), value.raw(), &mut answer) };
    answer
}

/// Return whether a value already carries raw bytes, in any of the three
/// shapes [`bytes_of`] reads.
fn is_buffer(env: &Env, value: &Unknown<'_>) -> bool {
    for test in [
        sys::napi_is_buffer,
        sys::napi_is_typedarray,
        sys::napi_is_arraybuffer,
    ] {
        let mut answer = false;
        unsafe { test(env.raw(), value.raw(), &mut answer) };
        if answer {
            return true;
        }
    }
    false
}

fn is_date(env: &Env, value: &Unknown<'_>) -> bool {
    let mut answer = false;
    unsafe { sys::napi_is_date(env.raw(), value.raw(), &mut answer) };
    answer
}

fn name_of(value_type: ValueType) -> &'static str {
    match value_type {
        ValueType::Undefined => "undefined",
        ValueType::Null => "null",
        ValueType::Boolean => "a boolean",
        ValueType::Number => "a number",
        ValueType::String => "a string",
        ValueType::Symbol => "a symbol",
        ValueType::Object => "an object",
        ValueType::Function => "a function",
        ValueType::External => "an external",
        ValueType::BigInt => "a bigint",
        _ => "an unknown value",
    }
}

fn decimal_unscaled(value: &Unknown<'_>, scale: u32) -> Result<i128> {
    let text = match value.get_type()? {
        ValueType::String => text_of(value)?,
        ValueType::Number => {
            let number = unsafe { value.cast::<f64>()? };
            format!("{number}")
        }
        ValueType::BigInt => integer_of(value)?.to_string(),
        other => {
            return Err(encode(format!(
                "expected a decimal string or number, got {}",
                name_of(other)
            )));
        }
    };
    parse_decimal(&text, scale)
}

/// Parse a decimal literal into its unscaled integer at the schema's scale.
fn parse_decimal(text: &str, scale: u32) -> Result<i128> {
    let trimmed = text.trim();
    let (negative, rest) = match trimmed.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, trimmed.strip_prefix('+').unwrap_or(trimmed)),
    };
    let (mantissa, exponent) = match rest.find(['e', 'E']) {
        Some(position) => {
            let exponent: i32 = rest[position + 1..]
                .parse()
                .map_err(|_| encode(format!("'{text}' is not a decimal number")))?;
            (&rest[..position], exponent)
        }
        None => (rest, 0),
    };
    let (whole, fraction) = match mantissa.split_once('.') {
        Some((whole, fraction)) => (whole, fraction),
        None => (mantissa, ""),
    };
    if whole.is_empty() && fraction.is_empty() {
        return Err(encode(format!("'{text}' is not a decimal number")));
    }
    let digits: String = format!("{whole}{fraction}");
    if !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(encode(format!("'{text}' is not a decimal number")));
    }
    let mut unscaled: i128 = digits
        .parse()
        .map_err(|_| encode(format!("'{text}' does not fit in an Avro decimal")))?;
    // Line the value up with the schema's scale, rounding nothing away.
    let shift = i64::from(scale) + i64::from(exponent) - fraction.len() as i64;
    if shift >= 0 {
        for _ in 0..shift {
            unscaled = unscaled
                .checked_mul(10)
                .ok_or_else(|| encode(format!("'{text}' does not fit in an Avro decimal")))?;
        }
    } else {
        for _ in 0..-shift {
            if unscaled % 10 != 0 {
                return Err(encode(format!(
                    "'{text}' has more precision than scale {scale} keeps"
                )));
            }
            unscaled /= 10;
        }
    }
    Ok(if negative { -unscaled } else { unscaled })
}

fn unscaled_text(unscaled: i128, scale: u32) -> String {
    if scale == 0 {
        return unscaled.to_string();
    }
    let negative = unscaled < 0;
    let digits = unscaled.unsigned_abs().to_string();
    let scale = scale as usize;
    let padded = if digits.len() <= scale {
        format!("{}{digits}", "0".repeat(scale - digits.len() + 1))
    } else {
        digits
    };
    let split = padded.len() - scale;
    format!(
        "{}{}.{}",
        if negative { "-" } else { "" },
        &padded[..split],
        &padded[split..]
    )
}

fn twos_complement(unscaled: i128, width: Option<usize>) -> Vec<u8> {
    let full = unscaled.to_be_bytes();
    match width {
        Some(size) => {
            let mut raw = vec![if unscaled < 0 { 0xff } else { 0x00 }; size];
            let take = size.min(full.len());
            raw[size - take..].copy_from_slice(&full[full.len() - take..]);
            raw
        }
        None => {
            let mut start = 0;
            while start + 1 < full.len() {
                let byte = full[start];
                let next_high = full[start + 1] & 0x80 != 0;
                let redundant = (unscaled >= 0 && byte == 0x00 && !next_high)
                    || (unscaled < 0 && byte == 0xff && next_high);
                if !redundant {
                    break;
                }
                start += 1;
            }
            full[start..].to_vec()
        }
    }
}

fn from_twos_complement(raw: &[u8]) -> i128 {
    if raw.is_empty() {
        return 0;
    }
    let mut value: i128 = if raw[0] & 0x80 != 0 { -1 } else { 0 };
    for byte in raw {
        value = (value << 8) | i128::from(*byte);
    }
    value
}

fn uuid_bytes(text: &str) -> Result<Vec<u8>> {
    let digits: String = text.chars().filter(|item| *item != '-').collect();
    if digits.len() != 32 || !digits.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(encode(format!("'{text}' is not a UUID")));
    }
    (0..16)
        .map(|index| {
            u8::from_str_radix(&digits[index * 2..index * 2 + 2], 16)
                .map_err(|_| encode(format!("'{text}' is not a UUID")))
        })
        .collect()
}

fn uuid_text(raw: &[u8]) -> String {
    let hex: String = raw.iter().map(|byte| format!("{byte:02x}")).collect();
    if hex.len() != 32 {
        return hex;
    }
    format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    )
}

fn is_timestamp(logical: &Logical) -> bool {
    matches!(
        logical,
        Logical::TimestampMillis
            | Logical::TimestampMicros
            | Logical::TimestampNanos
            | Logical::LocalTimestampMillis
            | Logical::LocalTimestampMicros
            | Logical::LocalTimestampNanos
    )
}

/// Return whether a `Date` holds this logical type without losing precision.
fn is_date_shaped(logical: &Logical) -> bool {
    matches!(
        logical,
        Logical::TimestampMillis | Logical::LocalTimestampMillis
    )
}

fn scale_timestamp(micros: i64, logical: &Logical) -> i64 {
    match logical {
        Logical::TimestampMillis | Logical::LocalTimestampMillis => micros / 1000,
        Logical::TimestampNanos | Logical::LocalTimestampNanos => micros * 1000,
        _ => micros,
    }
}

/// A value that cannot be encoded against its schema: `AvroEncodeError`.
fn encode(message: impl Into<String>) -> Error {
    crate::errors::encode(message)
}

/// A decoded value the schema cannot describe: `AvroDecodeError`.
fn schema_error(message: impl Into<String>) -> Error {
    crate::errors::decode(message)
}
