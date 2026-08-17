//! Partition specs, their transforms, and the Hive layout they write.
//!
//! A partition spec says how a row's column values become the directory a data
//! file lands in, and which values a manifest records for that file. Iceberg
//! writes those directories in exactly the `column=value` shape
//! [`Url::hive_partitions`](crate::Url::hive_partitions) already reads, so a
//! table this module writes is also a lake the rest of the crate can walk.
//!
//! A transform is a total function on values, and only [`Transform::Identity`]
//! and [`Transform::Void`] can be inverted. That matters for writing: a table
//! partitioned by `bucket[16]` needs the bucket hash to place a row, so a write
//! against such a spec is refused by name rather than silently producing files
//! in the wrong partition. Reading is unaffected - a manifest already records
//! which partition each file belongs to.

use std::fmt;
use std::str::FromStr;

use smol_str::{SmolStr, format_smolstr};

use crate::{DataType, Error, Field, Result, Value};

/// The identifier Iceberg assigns to the first partition field of a table.
pub const FIRST_PARTITION_ID: i32 = 1000;

/// How a source column value becomes a partition value.
///
/// ```
/// use yggdryl::iceberg::Transform;
///
/// # fn main() -> yggdryl::Result<()> {
/// assert_eq!(Transform::from_str("bucket[16]")?, Transform::Bucket(16));
/// assert_eq!(Transform::Bucket(16).to_string(), "bucket[16]");
///
/// // Only the invertible transforms can place a row without hashing it.
/// assert!(Transform::Identity.is_invertible());
/// assert!(!Transform::Bucket(16).is_invertible());
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum Transform {
    /// The source value, unchanged.
    Identity,
    /// A hash of the source value, modulo a bucket count.
    Bucket(i32),
    /// The source value shortened to a width.
    Truncate(i32),
    /// Years since 1970, from a date or timestamp.
    Year,
    /// Months since 1970-01, from a date or timestamp.
    Month,
    /// Days since 1970-01-01, from a date or timestamp.
    Day,
    /// Hours since 1970-01-01T00, from a timestamp.
    Hour,
    /// Always null, which is how a spec retires a partition field.
    Void,
}

impl Transform {
    /// Parse an Iceberg transform name.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Parse`] naming the vocabulary and the input.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(value: &str) -> Result<Self> {
        <Self as FromStr>::from_str(value)
    }

    /// Return whether a row's partition value can be computed here.
    ///
    /// An invertible transform needs nothing but the value itself, so a write
    /// can place the row. Everything else needs Iceberg's hash or its calendar
    /// arithmetic, neither of which this module implements.
    pub const fn is_invertible(self) -> bool {
        matches!(self, Self::Identity | Self::Void)
    }

    /// Return the datatype a partition value has, given its source column.
    ///
    /// # Errors
    ///
    /// Returns an error when the transform cannot apply to the source type.
    pub fn result_type(self, source: &DataType) -> Result<DataType> {
        Ok(match self {
            Self::Identity => source.clone(),
            Self::Bucket(_) => DataType::Int32,
            Self::Truncate(_) => source.clone(),
            Self::Year | Self::Month | Self::Day => DataType::Int32,
            Self::Hour => DataType::Int32,
            // A retired partition field reads as null of no useful width.
            Self::Void => DataType::Null,
        })
    }
}

impl FromStr for Transform {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        let trimmed = value.trim();
        match trimmed {
            "identity" => return Ok(Self::Identity),
            "year" => return Ok(Self::Year),
            "month" => return Ok(Self::Month),
            "day" => return Ok(Self::Day),
            "hour" => return Ok(Self::Hour),
            "void" => return Ok(Self::Void),
            _ => {}
        }
        if let Some(rest) = trimmed.strip_prefix("bucket") {
            return Ok(Self::Bucket(bracketed(rest, "bucket")?));
        }
        if let Some(rest) = trimmed.strip_prefix("truncate") {
            return Ok(Self::Truncate(bracketed(rest, "truncate")?));
        }
        Err(Error::Parse {
            target: "iceberg transform",
            position: 0,
            reason: format_smolstr!(
                "expected an Iceberg transform (identity, bucket[n], truncate[w], year, month, \
                 day, hour, void), got {trimmed:?}"
            ),
        })
    }
}

impl fmt::Display for Transform {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identity => formatter.write_str("identity"),
            Self::Bucket(count) => write!(formatter, "bucket[{count}]"),
            Self::Truncate(width) => write!(formatter, "truncate[{width}]"),
            Self::Year => formatter.write_str("year"),
            Self::Month => formatter.write_str("month"),
            Self::Day => formatter.write_str("day"),
            Self::Hour => formatter.write_str("hour"),
            Self::Void => formatter.write_str("void"),
        }
    }
}

/// Read `[n]` or `(n)` after a transform keyword.
fn bracketed(rest: &str, keyword: &str) -> Result<i32> {
    let trimmed = rest.trim();
    let inner = trimmed
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .or_else(|| {
            trimmed
                .strip_prefix('(')
                .and_then(|value| value.strip_suffix(')'))
        })
        .ok_or_else(|| Error::Parse {
            target: "iceberg transform",
            position: 0,
            reason: format_smolstr!("expected {keyword}[n], got {keyword}{rest}"),
        })?;
    inner.trim().parse::<i32>().map_err(|_| Error::Parse {
        target: "iceberg transform",
        position: 0,
        reason: format_smolstr!(
            "expected an integer {keyword} parameter, got {:?}",
            inner.trim()
        ),
    })
}

/// One partition column: a source column, a transform, and a name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionField {
    /// Identifier of the schema field this partitions on.
    pub source_id: i32,
    /// Identifier of the partition field itself, unique within a table.
    pub field_id: i32,
    /// The partition column's name, which is also its directory prefix.
    pub name: SmolStr,
    /// How the source value becomes the partition value.
    pub transform: Transform,
}

impl PartitionField {
    /// Partition on a source column's value unchanged.
    pub fn identity(source_id: i32, field_id: i32, name: impl Into<SmolStr>) -> Self {
        Self {
            source_id,
            field_id,
            name: name.into(),
            transform: Transform::Identity,
        }
    }

    /// Read one partition field object.
    ///
    /// # Errors
    ///
    /// Returns an error when a required key is missing or a transform is not
    /// one Iceberg names.
    pub fn from_json(document: &Value) -> Result<Self> {
        let name = document
            .get_key_str("name")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid(SmolStr::new_static("expected a partition field \"name\"")))?;
        let source_id = narrow(document.get_key_str("source-id"), "source-id", name)?;
        // v1 wrote no field-id, because a v1 spec numbers its fields in order.
        let field_id = document
            .get_key_str("field-id")
            .and_then(Value::as_i64)
            .map(|id| i32::try_from(id).unwrap_or(FIRST_PARTITION_ID))
            .unwrap_or(FIRST_PARTITION_ID);
        let transform = Transform::from_str(
            document
                .get_key_str("transform")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    invalid(format_smolstr!(
                        "expected a partition field \"transform\" on {name:?}"
                    ))
                })?,
        )?;
        Ok(Self {
            source_id,
            field_id,
            name: SmolStr::new(name),
            transform,
        })
    }

    /// Write one partition field object.
    ///
    /// # Errors
    ///
    /// Returns an error only when the mapping cannot be built.
    pub fn to_json(&self) -> Result<Value> {
        Value::from_mapping([
            (Value::from("name"), Value::from(self.name.clone())),
            (
                Value::from("transform"),
                Value::from(self.transform.to_string()),
            ),
            (
                Value::from("source-id"),
                Value::from(i64::from(self.source_id)),
            ),
            (
                Value::from("field-id"),
                Value::from(i64::from(self.field_id)),
            ),
        ])
    }
}

/// An ordered set of partition fields, identified by a spec id.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionSpec {
    /// Identifier of this spec within the table.
    pub spec_id: i32,
    /// The partition columns, in the order they nest as directories.
    pub fields: Vec<PartitionField>,
}

impl PartitionSpec {
    /// The unpartitioned spec, which every table has as spec zero.
    pub const fn unpartitioned() -> Self {
        Self {
            spec_id: 0,
            fields: Vec::new(),
        }
    }

    /// Build a spec that partitions on the named columns' values unchanged.
    ///
    /// # Errors
    ///
    /// Returns an error when a named column is not in the schema or carries no
    /// field identifier.
    pub fn identity(spec_id: i32, schema: &Field, columns: &[&str]) -> Result<Self> {
        let mut fields = Vec::with_capacity(columns.len());
        for (offset, column) in columns.iter().enumerate() {
            let source = schema.get_field_by_name(column).ok_or_else(|| {
                invalid(format_smolstr!(
                    "expected a schema column to partition on, got {column:?}"
                ))
            })?;
            let source_id = source.id()?.ok_or_else(|| {
                invalid(format_smolstr!(
                    "expected a PARQUET:field_id on the partition source {column:?}; call \
                     assign_field_ids first"
                ))
            })?;
            fields.push(PartitionField::identity(
                source_id,
                FIRST_PARTITION_ID + i32::try_from(offset).unwrap_or_default(),
                *column,
            ));
        }
        Ok(Self { spec_id, fields })
    }

    /// Return whether this spec places every file in one partition.
    pub fn is_unpartitioned(&self) -> bool {
        self.fields.is_empty()
    }

    /// Return the highest partition field identifier this spec uses.
    pub fn last_field_id(&self) -> i32 {
        self.fields
            .iter()
            .map(|field| field.field_id)
            .max()
            .unwrap_or(FIRST_PARTITION_ID - 1)
    }

    /// Return the source column names, in partition order.
    pub fn source_names(&self, schema: &Field) -> Result<Vec<SmolStr>> {
        let mut names = Vec::with_capacity(self.fields.len());
        for field in &self.fields {
            names.push(SmolStr::new(source_column(schema, field.source_id)?.name()));
        }
        Ok(names)
    }

    /// Reject a spec that cannot place a row without Iceberg's own hashing.
    ///
    /// # Errors
    ///
    /// Returns an error naming the first transform that is not invertible.
    pub fn require_writable(&self) -> Result<()> {
        for field in &self.fields {
            if !field.transform.is_invertible() {
                return Err(invalid(format_smolstr!(
                    "expected an invertible partition transform to place a row (identity, void), \
                     got {} on {:?}",
                    field.transform,
                    field.name
                )));
            }
        }
        Ok(())
    }

    /// Return the non-null struct Field the partition tuple has.
    ///
    /// This is the schema of a manifest's `partition` column, which is what
    /// makes a partition value readable without consulting the path.
    ///
    /// # Errors
    ///
    /// Returns an error when a source column is missing from `schema` or a
    /// transform cannot apply to it.
    pub fn partition_field(&self, schema: &Field) -> Result<Field> {
        let mut children = Vec::with_capacity(self.fields.len());
        for field in &self.fields {
            let source = source_column(schema, field.source_id)?;
            let data_type = field.transform.result_type(source.data_type())?;
            // A partition value is nullable even when its source is not: a
            // spec can retire a field, and `void` produces nothing but null.
            let mut child = Field::new(field.name.as_str(), data_type, true);
            child.set_id(field.field_id);
            children.push(child);
        }
        Ok(Field::new(
            "partition",
            DataType::from_fields(children)?,
            false,
        ))
    }

    /// Return the Hive-style directory chain one partition tuple names.
    ///
    /// `values` is one value per partition field, in spec order. A null value
    /// writes the literal `null`, which is what Iceberg's own writers spell and
    /// why the manifest, not the path, is the authority on a partition value.
    ///
    /// # Errors
    ///
    /// Returns an error when the tuple is not one value per partition field.
    pub fn partition_path(&self, values: &[Value]) -> Result<String> {
        if values.len() != self.fields.len() {
            return Err(invalid(format_smolstr!(
                "expected {} partition values for spec {}, got {}",
                self.fields.len(),
                self.spec_id,
                values.len()
            )));
        }
        let mut path = String::new();
        for (field, value) in self.fields.iter().zip(values) {
            if !path.is_empty() {
                path.push('/');
            }
            path.push_str(&field.name);
            path.push('=');
            path.push_str(&super::value::scalar_text(value));
        }
        Ok(path)
    }

    /// Read a partition spec object, in either the v1 or the v2 shape.
    ///
    /// # Errors
    ///
    /// Returns an error when the document is neither a spec object nor the
    /// bare field array a v1 table writes.
    pub fn from_json(document: &Value) -> Result<Self> {
        // v1 wrote `partition-spec` as a bare array of fields with no id.
        if let Some(entries) = document.as_sequence() {
            let mut fields = Vec::with_capacity(entries.len());
            for (offset, entry) in entries.iter().enumerate() {
                let mut field = PartitionField::from_json(entry)?;
                if entry.get_key_str("field-id").is_none() {
                    field.field_id = FIRST_PARTITION_ID + i32::try_from(offset).unwrap_or_default();
                }
                fields.push(field);
            }
            return Ok(Self { spec_id: 0, fields });
        }

        let spec_id = document
            .get_key_str("spec-id")
            .and_then(Value::as_i64)
            .and_then(|id| i32::try_from(id).ok())
            .unwrap_or_default();
        let entries = document
            .get_key_str("fields")
            .and_then(Value::as_sequence)
            .ok_or_else(|| {
                invalid(format_smolstr!(
                    "expected a \"fields\" array in partition spec {spec_id}"
                ))
            })?;
        let mut fields = Vec::with_capacity(entries.len());
        for (offset, entry) in entries.iter().enumerate() {
            let mut field = PartitionField::from_json(entry)?;
            if entry.get_key_str("field-id").is_none() {
                field.field_id = FIRST_PARTITION_ID + i32::try_from(offset).unwrap_or_default();
            }
            fields.push(field);
        }
        Ok(Self { spec_id, fields })
    }

    /// Write this spec as a v2 partition spec object.
    ///
    /// # Errors
    ///
    /// Returns an error only when the mapping cannot be built.
    pub fn to_json(&self) -> Result<Value> {
        let mut fields = Vec::with_capacity(self.fields.len());
        for field in &self.fields {
            fields.push(field.to_json()?);
        }
        Value::from_mapping([
            (Value::from("spec-id"), Value::from(i64::from(self.spec_id))),
            (Value::from("fields"), Value::from_sequence(fields)),
        ])
    }

    /// Write this spec as the bare field array a v1 table stores.
    ///
    /// # Errors
    ///
    /// Returns an error only when a field mapping cannot be built.
    pub fn to_v1_json(&self) -> Result<Value> {
        let mut fields = Vec::with_capacity(self.fields.len());
        for field in &self.fields {
            fields.push(field.to_json()?);
        }
        Ok(Value::from_sequence(fields))
    }
}

/// Find the schema column one partition field reads.
fn source_column(schema: &Field, source_id: i32) -> Result<&Field> {
    find_by_id(schema, source_id).ok_or_else(|| {
        invalid(format_smolstr!(
            "expected a schema column with field id {source_id} to partition on, got none"
        ))
    })
}

/// Walk a field tree looking for one identifier.
fn find_by_id(field: &Field, id: i32) -> Option<&Field> {
    if field.id().ok().flatten() == Some(id) {
        return Some(field);
    }
    field
        .data_type()
        .as_fields()?
        .iter()
        .find_map(|child| find_by_id(child, id))
}

/// Narrow one required integer key of a partition field.
fn narrow(value: Option<&Value>, key: &str, name: &str) -> Result<i32> {
    value
        .and_then(Value::as_i64)
        .and_then(|id| i32::try_from(id).ok())
        .ok_or_else(|| {
            invalid(format_smolstr!(
                "expected a 32-bit integer {key:?} on partition field {name:?}"
            ))
        })
}

/// Report a malformed Iceberg partition document.
fn invalid(reason: SmolStr) -> Error {
    Error::Codec {
        format: "iceberg",
        position: 0,
        reason,
    }
}
