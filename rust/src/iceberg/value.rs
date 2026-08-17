//! Iceberg's two renderings of one scalar value: its text and its bytes.
//!
//! Two places in the format store a value as text rather than as data: a
//! partition directory name and a snapshot summary entry. Both need the same
//! rendering, and neither can use the core [`Value`]'s serialization, because
//! `"XNAS"` must become `XNAS` and not `"XNAS"`.
//!
//! The textual rendering is deliberately not the inverse of anything. A
//! partition path spells a null value `null`, which is indistinguishable from
//! the string `"null"`, so a reader takes partition values from the manifest and
//! treats the path as layout only.
//!
//! The other rendering is the *single-value binary* one, which is what a
//! manifest bound and a manifest-list field summary carry. It is emitted only
//! for the types whose Parquet statistic bytes already are that encoding, which
//! is what lets [`super::statistics`] hand a footer's bytes straight to a
//! manifest and lets a scan compare a filter against them without decoding
//! either side. A type outside that set has no bound rather than a bound that
//! means something else.

use std::cmp::Ordering;

use smol_str::{SmolStr, format_smolstr};

use crate::{DataType, Value};

/// The literal Iceberg writes for a null partition value.
pub(super) const NULL_TEXT: &str = "null";

/// Render one scalar value the way Iceberg spells it in text.
pub(super) fn scalar_text(value: &Value) -> SmolStr {
    match value {
        Value::Null => SmolStr::new_static(NULL_TEXT),
        Value::Bool(flag) => SmolStr::new(if *flag { "true" } else { "false" }),
        Value::I64(number) => format_smolstr!("{number}"),
        Value::U64(number) => format_smolstr!("{number}"),
        Value::I128(number) => format_smolstr!("{number}"),
        Value::U128(number) => format_smolstr!("{number}"),
        Value::Float(number) => format_smolstr!("{}", number.as_f64()),
        Value::Decimal(unscaled, scale) => format_smolstr!("{unscaled}e{}", -i32::from(*scale)),
        Value::String(text) => text.clone(),
        Value::Date(days) => format_smolstr!("{days}"),
        Value::Time(count, _) | Value::Duration(count, _) => format_smolstr!("{count}"),
        Value::Timestamp(count, _, _) => format_smolstr!("{count}"),
        // Bytes and containers have no partition or summary spelling; the JSON
        // form is at least lossless and readable rather than invented.
        other => crate::json::to_vec(other)
            .ok()
            .and_then(|encoded| String::from_utf8(encoded).ok())
            .map_or_else(|| SmolStr::new_static(NULL_TEXT), SmolStr::new),
    }
}

/// Return whether a Parquet statistic byte string is also the Iceberg one.
///
/// A decimal is the case that differs - Parquet stores it big-endian in a fixed
/// width, Iceberg stores the minimal two's-complement big-endian - so a decimal
/// column gets counts but no bounds. A missing statistic costs a planner one
/// file read; a wrong one costs correctness.
pub(super) const fn is_portable(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Date32
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
    )
}

/// Encode one scalar as the single value a manifest bound carries.
///
/// The datatype decides the encoding rather than the value's own variant,
/// because a column declared `Int32` still arrives as a 64-bit
/// [`Value::I64`]. A value that does not fit the column, and every type whose
/// encoding is not [`is_portable`], has no bytes rather than the wrong ones.
pub(super) fn single_value(value: &Value, data_type: &DataType) -> Option<Vec<u8>> {
    match data_type {
        DataType::Boolean => value.as_bool().map(|flag| vec![u8::from(flag)]),
        DataType::Int32 | DataType::Date32 => i32::try_from(count(value)?)
            .ok()
            .map(|number| number.to_le_bytes().to_vec()),
        DataType::Int64 | DataType::Time64(_) | DataType::Timestamp(_, _) => {
            Some(count(value)?.to_le_bytes().to_vec())
        }
        #[allow(clippy::cast_possible_truncation)]
        DataType::Float32 => value
            .as_f64()
            .map(|number| (number as f32).to_le_bytes().to_vec()),
        DataType::Float64 => value.as_f64().map(|number| number.to_le_bytes().to_vec()),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            value.as_str().map(|text| text.as_bytes().to_vec())
        }
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => value.as_bytes().map(<[u8]>::to_vec),
        _ => None,
    }
}

/// Read the integer count a value holds, whatever it counts.
///
/// A date counts days, a time counts its unit since midnight, and a timestamp
/// counts its unit since the epoch, so all three are one integer to an encoder.
fn count(value: &Value) -> Option<i64> {
    match value {
        Value::Date(days) => Some(i64::from(*days)),
        Value::Time(count, _) | Value::Timestamp(count, _, _) | Value::Duration(count, _) => {
            Some(*count)
        }
        other => other.as_i64(),
    }
}

/// Compare two single values the way their datatype orders them.
///
/// A little-endian integer does not order as bytes do, so folding bounds across
/// row groups, and testing a filter against one, has to decode before it
/// compares. Text and bytes are the exception: they order lexicographically in
/// both encodings.
pub(super) fn compare_single(left: &[u8], right: &[u8], data_type: &DataType) -> Ordering {
    match data_type {
        DataType::Boolean => left.first().cmp(&right.first()),
        DataType::Int32 | DataType::Date32 => int32(left).cmp(&int32(right)),
        DataType::Int64 | DataType::Time64(_) | DataType::Timestamp(_, _) => {
            int64(left).cmp(&int64(right))
        }
        DataType::Float32 => float32(left).total_cmp(&float32(right)),
        DataType::Float64 => float64(left).total_cmp(&float64(right)),
        _ => left.cmp(right),
    }
}

/// Decode a little-endian 32-bit integer, treating a short value as zero.
fn int32(bytes: &[u8]) -> i32 {
    bytes
        .get(..4)
        .and_then(|slice| <[u8; 4]>::try_from(slice).ok())
        .map_or(0, i32::from_le_bytes)
}

/// Decode a little-endian 64-bit integer, treating a short value as zero.
fn int64(bytes: &[u8]) -> i64 {
    bytes
        .get(..8)
        .and_then(|slice| <[u8; 8]>::try_from(slice).ok())
        .map_or(0, i64::from_le_bytes)
}

/// Decode a little-endian 32-bit float, treating a short value as zero.
fn float32(bytes: &[u8]) -> f32 {
    bytes
        .get(..4)
        .and_then(|slice| <[u8; 4]>::try_from(slice).ok())
        .map_or(0.0, f32::from_le_bytes)
}

/// Decode a little-endian 64-bit float, treating a short value as zero.
fn float64(bytes: &[u8]) -> f64 {
    bytes
        .get(..8)
        .and_then(|slice| <[u8; 8]>::try_from(slice).ok())
        .map_or(0.0, f64::from_le_bytes)
}
