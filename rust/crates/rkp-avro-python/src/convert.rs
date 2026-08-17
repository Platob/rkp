//! Python value conversion, guided by the schema.
//!
//! Logical types are resolved here rather than in the core: the core owns the
//! physical format, and each host owns the objects its users actually hold.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{
    PyBool, PyByteArray, PyBytes, PyDate, PyDateAccess, PyDateTime, PyDelta, PyDeltaAccess, PyDict,
    PyFloat, PyInt, PyList, PyMapping, PySequence, PyString, PyTime, PyTimeAccess, PyTuple,
};

use rkp_avro::schema::{Kind, Logical, Schema};
use rkp_avro::value::Value;

use crate::errors::{encode_error, schema_error};

static DECIMAL: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static UUID: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static UTC: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

fn decimal_type(py: Python<'_>) -> PyResult<&Bound<'_, PyAny>> {
    DECIMAL
        .get_or_try_init(py, || {
            Ok(py.import("decimal")?.getattr("Decimal")?.unbind())
        })
        .map(|value| value.bind(py))
}

fn uuid_type(py: Python<'_>) -> PyResult<&Bound<'_, PyAny>> {
    UUID.get_or_try_init(py, || Ok(py.import("uuid")?.getattr("UUID")?.unbind()))
        .map(|value| value.bind(py))
}

fn utc(py: Python<'_>) -> PyResult<&Bound<'_, PyAny>> {
    UTC.get_or_try_init(py, || {
        Ok(py
            .import("datetime")?
            .getattr("timezone")?
            .getattr("utc")?
            .unbind())
    })
    .map(|value| value.bind(py))
}

const MICROS_PER_DAY: i64 = 86_400_000_000;

fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    // Howard Hinnant's civil-from-days inverse, valid for the proleptic
    // Gregorian calendar Python itself uses.
    let year = i64::from(if month <= 2 { year - 1 } else { year });
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month = i64::from(month);
    let day = i64::from(day);
    let day_of_year = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let days = days + 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    (
        (year + i64::from(month <= 2)) as i32,
        month as u32,
        day as u32,
    )
}

fn datetime_micros(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    let moment = value.cast::<PyDateTime>()?;
    let days = days_from_civil(
        moment.get_year(),
        u32::from(moment.get_month()),
        u32::from(moment.get_day()),
    );
    let mut micros = days * MICROS_PER_DAY
        + i64::from(moment.get_hour()) * 3_600_000_000
        + i64::from(moment.get_minute()) * 60_000_000
        + i64::from(moment.get_second()) * 1_000_000
        + i64::from(moment.get_microsecond());
    let offset = value.call_method0("utcoffset")?;
    if !offset.is_none() {
        let delta = offset.cast::<PyDelta>()?;
        micros -= i64::from(delta.get_days()) * MICROS_PER_DAY
            + i64::from(delta.get_seconds()) * 1_000_000
            + i64::from(delta.get_microseconds());
    }
    Ok(micros)
}

fn micros_datetime(py: Python<'_>, micros: i64, aware: bool) -> PyResult<Py<PyAny>> {
    let days = micros.div_euclid(MICROS_PER_DAY);
    let rest = micros.rem_euclid(MICROS_PER_DAY);
    let (year, month, day) = civil_from_days(days);
    let hour = (rest / 3_600_000_000) as u8;
    let minute = ((rest / 60_000_000) % 60) as u8;
    let second = ((rest / 1_000_000) % 60) as u8;
    let microsecond = (rest % 1_000_000) as u32;
    let zone = if aware { Some(utc(py)?.clone()) } else { None };
    let moment = PyDateTime::new(
        py,
        year,
        month as u8,
        day as u8,
        hour,
        minute,
        second,
        microsecond,
        zone.as_ref().map(|value| value.cast().unwrap()),
    )?;
    Ok(moment.into_any().unbind())
}

fn time_micros(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    let clock = value.cast::<PyTime>()?;
    Ok(i64::from(clock.get_hour()) * 3_600_000_000
        + i64::from(clock.get_minute()) * 60_000_000
        + i64::from(clock.get_second()) * 1_000_000
        + i64::from(clock.get_microsecond()))
}

fn micros_time(py: Python<'_>, micros: i64) -> PyResult<Py<PyAny>> {
    let micros = micros.rem_euclid(MICROS_PER_DAY);
    let clock = PyTime::new(
        py,
        (micros / 3_600_000_000) as u8,
        ((micros / 60_000_000) % 60) as u8,
        ((micros / 1_000_000) % 60) as u8,
        (micros % 1_000_000) as u32,
        None,
    )?;
    Ok(clock.into_any().unbind())
}

fn date_days(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    let day = value.cast::<PyDate>()?;
    Ok(days_from_civil(
        day.get_year(),
        u32::from(day.get_month()),
        u32::from(day.get_day()),
    ))
}

fn days_date(py: Python<'_>, days: i64) -> PyResult<Py<PyAny>> {
    let (year, month, day) = civil_from_days(days);
    Ok(PyDate::new(py, year, month as u8, day as u8)?
        .into_any()
        .unbind())
}

fn decimal_unscaled(value: &Bound<'_, PyAny>, scale: u32) -> PyResult<i128> {
    let py = value.py();
    let decimal = if value.is_instance(decimal_type(py)?)? {
        value.clone()
    } else {
        decimal_type(py)?.call1((value.str()?,))?
    };
    let shifted = decimal.call_method1("scaleb", (scale,))?;
    let integral = shifted.call_method0("to_integral_value")?;
    let as_int = py.get_type::<PyInt>().call1((integral,))?;
    as_int.extract::<i128>()
}

fn unscaled_decimal(py: Python<'_>, unscaled: i128, scale: u32) -> PyResult<Py<PyAny>> {
    let value = decimal_type(py)?.call1((unscaled.to_string(),))?;
    if scale == 0 {
        return Ok(value.unbind());
    }
    let scaled = value.call_method1("scaleb", (-(scale as i64),))?;
    Ok(scaled.unbind())
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

fn bytes_of(value: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(raw) = value.cast::<PyBytes>() {
        return Ok(raw.as_bytes().to_vec());
    }
    if let Ok(raw) = value.cast::<PyByteArray>() {
        return Ok(raw.to_vec());
    }
    if let Ok(text) = value.cast::<PyString>() {
        return Ok(text.to_cow()?.chars().map(|item| item as u8).collect());
    }
    value.extract::<Vec<u8>>()
}

fn is_mapping(value: &Bound<'_, PyAny>) -> bool {
    value.cast::<PyDict>().is_ok() || value.cast::<PyMapping>().is_ok()
}

/// Convert one Python object into a core value, guided by a schema node.
pub fn into_value(schema: &Schema, index: usize, value: &Bound<'_, PyAny>) -> PyResult<Value> {
    let py = value.py();
    let node = schema.node(index);
    match node.kind {
        Kind::Null => {
            if value.is_none() {
                Ok(Value::Null)
            } else {
                Err(encode_error(format!(
                    "expected null, got {}",
                    type_name(value)
                )))
            }
        }
        Kind::Boolean => Ok(Value::Boolean(value.is_truthy()?)),
        Kind::Int => {
            let number = match &node.logical {
                Some(Logical::Date) => {
                    if value.is_instance_of::<PyDateTime>() {
                        date_days(&value.call_method0("date")?)?
                    } else if value.is_instance_of::<PyDate>() {
                        date_days(value)?
                    } else {
                        extract_int(value)?
                    }
                }
                Some(Logical::TimeMillis) => {
                    if value.is_instance_of::<PyTime>() {
                        time_micros(value)? / 1000
                    } else {
                        extract_int(value)?
                    }
                }
                _ => extract_int(value)?,
            };
            match i32::try_from(number) {
                Ok(narrowed) => Ok(Value::Int(narrowed)),
                Err(_) => Err(encode_error(format!(
                    "value {number} does not fit in an Avro int"
                ))),
            }
        }
        Kind::Long => {
            let number = match &node.logical {
                Some(Logical::TimeMicros) => {
                    if value.is_instance_of::<PyTime>() {
                        time_micros(value)?
                    } else {
                        extract_int(value)?
                    }
                }
                Some(logical) if is_timestamp(logical) => {
                    if value.is_instance_of::<PyDateTime>() {
                        let micros = datetime_micros(value)?;
                        scale_timestamp(micros, logical)
                    } else {
                        extract_int(value)?
                    }
                }
                _ => extract_int(value)?,
            };
            Ok(Value::Long(number))
        }
        Kind::Float => Ok(Value::Float(value.extract::<f64>()? as f32)),
        Kind::Double => Ok(Value::Double(value.extract::<f64>()?)),
        Kind::Bytes => match &node.logical {
            Some(Logical::Decimal { scale, .. }) if !value.is_instance_of::<PyBytes>() => {
                let unscaled = decimal_unscaled(value, *scale)?;
                Ok(Value::Bytes(twos_complement(unscaled, None)))
            }
            _ => Ok(Value::Bytes(bytes_of(value)?)),
        },
        Kind::String => match &node.logical {
            Some(Logical::Uuid) => Ok(Value::String(value.str()?.to_string())),
            _ => {
                if let Ok(text) = value.cast::<PyString>() {
                    Ok(Value::String(text.to_cow()?.into_owned()))
                } else if value.is_instance_of::<PyBytes>() {
                    Ok(Value::String(
                        String::from_utf8_lossy(&bytes_of(value)?).into_owned(),
                    ))
                } else {
                    Err(encode_error(format!(
                        "expected string, got {}",
                        type_name(value)
                    )))
                }
            }
        },
        Kind::Fixed => {
            let raw = match &node.logical {
                Some(Logical::Decimal { scale, .. }) if !value.is_instance_of::<PyBytes>() => {
                    let unscaled = decimal_unscaled(value, *scale)?;
                    twos_complement(unscaled, Some(node.size))
                }
                Some(Logical::Uuid) if !value.is_instance_of::<PyBytes>() => {
                    let identity = uuid_type(py)?.call1((value.str()?,))?;
                    bytes_of(&identity.getattr("bytes")?)?
                }
                Some(Logical::Duration) if value.is_instance_of::<PyTuple>() => {
                    let parts: Vec<u32> = value.extract()?;
                    let mut raw = Vec::with_capacity(12);
                    for part in parts.iter().take(3) {
                        raw.extend_from_slice(&part.to_le_bytes());
                    }
                    raw
                }
                _ => bytes_of(value)?,
            };
            if raw.len() != node.size {
                return Err(encode_error(format!(
                    "fixed '{}' requires {} bytes, got {}",
                    node.fullname(),
                    node.size,
                    raw.len()
                )));
            }
            Ok(Value::Fixed(raw))
        }
        Kind::Enum => {
            let text = if let Ok(text) = value.cast::<PyString>() {
                text.to_cow()?.into_owned()
            } else if let Ok(inner) = value.getattr("value") {
                inner.str()?.to_string()
            } else {
                value.str()?.to_string()
            };
            match node.symbols.iter().position(|symbol| *symbol == text) {
                Some(position) => Ok(Value::Enum(position)),
                None => match &node.enum_default {
                    Some(default) => Ok(Value::Enum(
                        node.symbols
                            .iter()
                            .position(|symbol| symbol == default)
                            .unwrap_or(0),
                    )),
                    None => Err(encode_error(format!(
                        "'{text}' is not a symbol of enum '{}'",
                        node.fullname()
                    ))),
                },
            }
        }
        Kind::Record => {
            let mut values = Vec::with_capacity(node.fields.len());
            if is_mapping(value) {
                let mapping = value.cast::<PyMapping>()?;
                for field in &node.fields {
                    let item = match mapping.get_item(&field.name) {
                        Ok(item) => item,
                        Err(_) => match &field.default {
                            Some(default) => json_to_python(py, default)?.into_bound(py),
                            None => {
                                return Err(encode_error(format!(
                                    "record '{}' is missing field '{}'",
                                    node.fullname(),
                                    field.name
                                )));
                            }
                        },
                    };
                    values.push(into_value(schema, field.node, &item)?);
                }
            } else if value.hasattr("__dataclass_fields__")? && !value.is_instance_of::<PyType>() {
                for field in &node.fields {
                    let item = match value.getattr(field.name.as_str()) {
                        Ok(item) => item,
                        Err(_) => match &field.default {
                            Some(default) => json_to_python(py, default)?.into_bound(py),
                            None => {
                                return Err(encode_error(format!(
                                    "record '{}' is missing field '{}'",
                                    node.fullname(),
                                    field.name
                                )));
                            }
                        },
                    };
                    values.push(into_value(schema, field.node, &item)?);
                }
            } else if let Ok(sequence) = value.cast::<PySequence>() {
                let length = sequence.len()?;
                if length != node.fields.len() {
                    return Err(encode_error(format!(
                        "record '{}' expects {} positional values, got {}",
                        node.fullname(),
                        node.fields.len(),
                        length
                    )));
                }
                for (position, field) in node.fields.iter().enumerate() {
                    let item = sequence.get_item(position)?;
                    values.push(into_value(schema, field.node, &item)?);
                }
            } else {
                return Err(encode_error(format!(
                    "record '{}' expects a mapping, dataclass, or sequence, got {}",
                    node.fullname(),
                    type_name(value)
                )));
            }
            Ok(Value::Record(values))
        }
        Kind::Array => {
            if value.is_instance_of::<PyString>() || is_mapping(value) {
                return Err(encode_error(format!(
                    "expected an array, got {}",
                    type_name(value)
                )));
            }
            let mut items = Vec::new();
            for item in value.try_iter()? {
                items.push(into_value(schema, node.children[0], &item?)?);
            }
            Ok(Value::Array(items))
        }
        Kind::Map => {
            if !is_mapping(value) {
                return Err(encode_error(format!(
                    "expected a map, got {}",
                    type_name(value)
                )));
            }
            let mapping = value.cast::<PyMapping>()?;
            let keys = mapping.keys()?;
            let mut entries = Vec::with_capacity(keys.len());
            for key in keys.try_iter()? {
                let key = key?;
                let item = mapping.get_item(&key)?;
                entries.push((
                    key.str()?.to_string(),
                    into_value(schema, node.children[0], &item)?,
                ));
            }
            Ok(Value::Map(entries))
        }
        Kind::Union => {
            if value.is_none()
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
                if let Ok(inner) = into_value(schema, *child, value) {
                    return Ok(Value::Union(branch, Box::new(inner)));
                }
            }
            Err(encode_error(format!(
                "value of type {} does not match any union branch",
                type_name(value)
            )))
        }
    }
}

/// Convert one core value into a Python object, guided by a schema node.
pub fn into_python(
    py: Python<'_>,
    schema: &Schema,
    index: usize,
    value: &Value,
) -> PyResult<Py<PyAny>> {
    let node = schema.node(index);
    match (node.kind, value) {
        (Kind::Null, _) => Ok(py.None()),
        (Kind::Boolean, Value::Boolean(inner)) => {
            Ok(inner.into_pyobject(py)?.to_owned().into_any().unbind())
        }
        (Kind::Int, Value::Int(inner)) => match &node.logical {
            Some(Logical::Date) => days_date(py, i64::from(*inner)),
            Some(Logical::TimeMillis) => micros_time(py, i64::from(*inner) * 1000),
            _ => Ok(inner.into_pyobject(py)?.into_any().unbind()),
        },
        (Kind::Long, Value::Long(inner)) => match &node.logical {
            Some(Logical::TimeMicros) => micros_time(py, *inner),
            Some(logical) if is_timestamp(logical) => {
                let micros = unscale_timestamp(*inner, logical);
                micros_datetime(py, micros, !is_local(logical))
            }
            _ => Ok(inner.into_pyobject(py)?.into_any().unbind()),
        },
        (Kind::Float, Value::Float(inner)) => {
            Ok(PyFloat::new(py, f64::from(*inner)).into_any().unbind())
        }
        (Kind::Double, Value::Double(inner)) => Ok(PyFloat::new(py, *inner).into_any().unbind()),
        (Kind::Bytes, Value::Bytes(raw)) => match &node.logical {
            Some(Logical::Decimal { scale, .. }) => {
                unscaled_decimal(py, from_twos_complement(raw), *scale)
            }
            _ => Ok(PyBytes::new(py, raw).into_any().unbind()),
        },
        (Kind::Fixed, Value::Fixed(raw)) => match &node.logical {
            Some(Logical::Decimal { scale, .. }) => {
                unscaled_decimal(py, from_twos_complement(raw), *scale)
            }
            Some(Logical::Uuid) => {
                let identity = uuid_type(py)?.call(
                    (),
                    Some(&{
                        let kwargs = PyDict::new(py);
                        kwargs.set_item("bytes", PyBytes::new(py, raw))?;
                        kwargs
                    }),
                )?;
                Ok(identity.unbind())
            }
            Some(Logical::Duration) => {
                let parts: Vec<u32> = raw
                    .chunks(4)
                    .map(|chunk| u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                    .collect();
                Ok(PyTuple::new(py, parts)?.into_any().unbind())
            }
            _ => Ok(PyBytes::new(py, raw).into_any().unbind()),
        },
        (Kind::String, Value::String(text)) => match &node.logical {
            Some(Logical::Uuid) => Ok(uuid_type(py)?.call1((text.as_str(),))?.unbind()),
            _ => Ok(PyString::new(py, text).into_any().unbind()),
        },
        (Kind::Enum, Value::Enum(symbol)) => match node.symbols.get(*symbol) {
            Some(name) => Ok(PyString::new(py, name).into_any().unbind()),
            None => Err(schema_error(format!(
                "enum '{}' has no symbol at index {symbol}",
                node.fullname()
            ))),
        },
        (Kind::Record, Value::Record(values)) => {
            let row = PyDict::new(py);
            for (field, item) in node.fields.iter().zip(values) {
                row.set_item(
                    field.name.as_str(),
                    into_python(py, schema, field.node, item)?,
                )?;
            }
            Ok(row.into_any().unbind())
        }
        (Kind::Array, Value::Array(items)) => {
            let list = PyList::empty(py);
            for item in items {
                list.append(into_python(py, schema, node.children[0], item)?)?;
            }
            Ok(list.into_any().unbind())
        }
        (Kind::Map, Value::Map(entries)) => {
            let mapping = PyDict::new(py);
            for (key, item) in entries {
                mapping.set_item(key, into_python(py, schema, node.children[0], item)?)?;
            }
            Ok(mapping.into_any().unbind())
        }
        (Kind::Union, Value::Union(branch, inner)) => match node.children.get(*branch) {
            Some(child) => into_python(py, schema, *child, inner),
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

/// Convert decoded JSON into plain Python objects.
pub fn json_to_python(py: Python<'_>, value: &serde_json::Value) -> PyResult<Py<PyAny>> {
    Ok(match value {
        serde_json::Value::Null => py.None(),
        serde_json::Value::Bool(inner) => PyBool::new(py, *inner).to_owned().into_any().unbind(),
        serde_json::Value::Number(number) => {
            if let Some(inner) = number.as_i64() {
                inner.into_pyobject(py)?.into_any().unbind()
            } else if let Some(inner) = number.as_u64() {
                inner.into_pyobject(py)?.into_any().unbind()
            } else {
                PyFloat::new(py, number.as_f64().unwrap_or(f64::NAN))
                    .into_any()
                    .unbind()
            }
        }
        serde_json::Value::String(text) => PyString::new(py, text).into_any().unbind(),
        serde_json::Value::Array(items) => {
            let list = PyList::empty(py);
            for item in items {
                list.append(json_to_python(py, item)?)?;
            }
            list.into_any().unbind()
        }
        serde_json::Value::Object(entries) => {
            let mapping = PyDict::new(py);
            for (key, item) in entries {
                mapping.set_item(key, json_to_python(py, item)?)?;
            }
            mapping.into_any().unbind()
        }
    })
}

/// Convert plain Python objects into JSON.
pub fn python_to_json(value: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
    if value.is_none() {
        return Ok(serde_json::Value::Null);
    }
    if let Ok(inner) = value.cast::<PyBool>() {
        return Ok(serde_json::Value::Bool(inner.is_true()));
    }
    if let Ok(inner) = value.cast::<PyString>() {
        return Ok(serde_json::Value::String(inner.to_cow()?.into_owned()));
    }
    if value.is_instance_of::<PyInt>() {
        return Ok(serde_json::Value::from(value.extract::<i64>()?));
    }
    if value.is_instance_of::<PyFloat>() {
        return Ok(serde_json::Number::from_f64(value.extract::<f64>()?)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null));
    }
    if let Ok(mapping) = value.cast::<PyDict>() {
        let mut object = serde_json::Map::with_capacity(mapping.len());
        for (key, item) in mapping.iter() {
            object.insert(key.str()?.to_string(), python_to_json(&item)?);
        }
        return Ok(serde_json::Value::Object(object));
    }
    if let Ok(items) = value.cast::<PyList>() {
        let mut array = Vec::with_capacity(items.len());
        for item in items.iter() {
            array.push(python_to_json(&item)?);
        }
        return Ok(serde_json::Value::Array(array));
    }
    if let Ok(items) = value.cast::<PyTuple>() {
        let mut array = Vec::with_capacity(items.len());
        for item in items.iter() {
            array.push(python_to_json(&item)?);
        }
        return Ok(serde_json::Value::Array(array));
    }
    Err(PyValueError::new_err(format!(
        "cannot convert {} into JSON",
        type_name(value)
    )))
}

fn extract_int(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    if value.is_instance_of::<PyBool>() {
        return Ok(i64::from(value.is_truthy()?));
    }
    if !value.is_instance_of::<PyInt>() {
        return Err(encode_error(format!(
            "expected an integer, got {}",
            type_name(value)
        )));
    }
    value.extract::<i64>().map_err(|_| {
        encode_error(format!(
            "value {} does not fit in an Avro long",
            value.str().map(|text| text.to_string()).unwrap_or_default()
        ))
    })
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

fn is_local(logical: &Logical) -> bool {
    matches!(
        logical,
        Logical::LocalTimestampMillis
            | Logical::LocalTimestampMicros
            | Logical::LocalTimestampNanos
    )
}

fn scale_timestamp(micros: i64, logical: &Logical) -> i64 {
    match logical {
        Logical::TimestampMillis | Logical::LocalTimestampMillis => micros / 1000,
        Logical::TimestampNanos | Logical::LocalTimestampNanos => micros * 1000,
        _ => micros,
    }
}

fn unscale_timestamp(value: i64, logical: &Logical) -> i64 {
    match logical {
        Logical::TimestampMillis | Logical::LocalTimestampMillis => value * 1000,
        Logical::TimestampNanos | Logical::LocalTimestampNanos => value / 1000,
        _ => value,
    }
}

fn type_name(value: &Bound<'_, PyAny>) -> String {
    value
        .get_type()
        .name()
        .map(|name| name.to_string())
        .unwrap_or_else(|_| "object".to_string())
}

use pyo3::types::PyType;
