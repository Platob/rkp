//! Table metadata: the JSON document that is the table.
//!
//! Everything else in a table - manifests, data files, the directory layout -
//! is reachable only from this one document, which is why committing a change
//! means writing a new one. It exists in three format versions, and the
//! differences are small but load-bearing:
//!
//! - **v1** stores the current schema as `schema` and the current partition
//!   spec as a bare `partition-spec` array, and has no sequence numbers.
//! - **v2** makes `schemas`/`partition-specs` the authority, adds
//!   `last-sequence-number`, and numbers every snapshot.
//! - **v3** adds row lineage: `next-row-id` on the table and `first-row-id` /
//!   `added-rows` on each snapshot.
//!
//! Reading accepts all three and normalizes the singular forms into the plural
//! ones, so the rest of the module never asks which version it is looking at.
//! Writing emits exactly what the declared version requires.

use smol_str::{SmolStr, format_smolstr};

use super::partition::PartitionSpec;
use super::snapshot::{MAIN_BRANCH, Snapshot, SnapshotRef};
use super::{Transform, schema_from_json, schema_to_json};
use crate::{Error, Field, Result, Value};

/// Which revision of the Iceberg table specification a table is written to.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum FormatVersion {
    /// The original format: one schema, one spec, no sequence numbers.
    V1,
    /// Row-level deletes, sequence numbers, and multiple schemas and specs.
    #[default]
    V2,
    /// Row lineage, nanosecond temporals, and default values.
    V3,
}

impl FormatVersion {
    /// Return the integer a metadata document stores.
    pub const fn number(self) -> i32 {
        match self {
            Self::V1 => 1,
            Self::V2 => 2,
            Self::V3 => 3,
        }
    }

    /// Read the version one stored integer names.
    ///
    /// # Errors
    ///
    /// Returns an error naming the value when it is not 1, 2, or 3.
    pub fn from_number(number: i64) -> Result<Self> {
        match number {
            1 => Ok(Self::V1),
            2 => Ok(Self::V2),
            3 => Ok(Self::V3),
            other => Err(invalid(format_smolstr!(
                "expected an Iceberg format version of 1, 2, or 3, got {other}"
            ))),
        }
    }
}

/// One column a table's rows are sorted by.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SortField {
    /// Identifier of the schema field sorted on.
    pub source_id: i32,
    /// How the value is transformed before comparison.
    pub transform: Transform,
    /// Either `asc` or `desc`.
    pub direction: SmolStr,
    /// Either `nulls-first` or `nulls-last`.
    pub null_order: SmolStr,
}

/// An identified ordering a table's writers maintain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SortOrder {
    /// Identifier of this order within the table.
    pub order_id: i32,
    /// The sort columns, most significant first.
    pub fields: Vec<SortField>,
}

impl SortOrder {
    /// The unsorted order, which every table has as order zero.
    pub const fn unsorted() -> Self {
        Self {
            order_id: 0,
            fields: Vec::new(),
        }
    }

    /// Read one sort order object.
    ///
    /// # Errors
    ///
    /// Returns an error when a sort field names a transform Iceberg does not.
    pub fn from_json(document: &Value) -> Result<Self> {
        let order_id = document
            .get_key_str("order-id")
            .and_then(Value::as_i64)
            .and_then(|id| i32::try_from(id).ok())
            .unwrap_or_default();
        let mut fields = Vec::new();
        for entry in document
            .get_key_str("fields")
            .map(Value::sequence_iter)
            .unwrap_or_default()
        {
            fields.push(SortField {
                source_id: entry
                    .get_key_str("source-id")
                    .and_then(Value::as_i64)
                    .and_then(|id| i32::try_from(id).ok())
                    .unwrap_or_default(),
                transform: Transform::from_str(
                    entry
                        .get_key_str("transform")
                        .and_then(Value::as_str)
                        .unwrap_or("identity"),
                )?,
                direction: SmolStr::new(
                    entry
                        .get_key_str("direction")
                        .and_then(Value::as_str)
                        .unwrap_or("asc"),
                ),
                null_order: SmolStr::new(
                    entry
                        .get_key_str("null-order")
                        .and_then(Value::as_str)
                        .unwrap_or("nulls-first"),
                ),
            });
        }
        Ok(Self { order_id, fields })
    }

    /// Write one sort order object.
    ///
    /// # Errors
    ///
    /// Returns an error only when the mapping cannot be built.
    pub fn to_json(&self) -> Result<Value> {
        let mut fields = Vec::with_capacity(self.fields.len());
        for field in &self.fields {
            fields.push(Value::from_mapping([
                (
                    Value::from("source-id"),
                    Value::from(i64::from(field.source_id)),
                ),
                (
                    Value::from("transform"),
                    Value::from(field.transform.to_string()),
                ),
                (
                    Value::from("direction"),
                    Value::from(field.direction.clone()),
                ),
                (
                    Value::from("null-order"),
                    Value::from(field.null_order.clone()),
                ),
            ])?);
        }
        Value::from_mapping([
            (
                Value::from("order-id"),
                Value::from(i64::from(self.order_id)),
            ),
            (Value::from("fields"), Value::from_sequence(fields)),
        ])
    }
}

/// The complete state of an Iceberg table at one point in time.
#[derive(Clone, Debug)]
pub struct TableMetadata {
    /// Which revision of the specification this document is written to.
    pub format_version: FormatVersion,
    /// A stable identifier for the table itself, not for any one version.
    pub table_uuid: SmolStr,
    /// The table's base location, as a URI.
    pub location: SmolStr,
    /// Highest assigned sequence number, absent in v1.
    pub last_sequence_number: i64,
    /// When this document was written, in milliseconds since the Unix epoch.
    pub last_updated_ms: i64,
    /// Highest assigned column identifier.
    pub last_column_id: i32,
    /// Every schema the table has had, by identifier.
    pub schemas: Vec<Field>,
    /// The schema new data is written against.
    pub current_schema_id: i32,
    /// Every partition spec the table has had.
    pub partition_specs: Vec<PartitionSpec>,
    /// The spec new data is written against.
    pub default_spec_id: i32,
    /// Highest assigned partition field identifier.
    pub last_partition_id: i32,
    /// Every sort order the table has had.
    pub sort_orders: Vec<SortOrder>,
    /// The order new data is written in.
    pub default_sort_order_id: i32,
    /// Free-form table properties.
    pub properties: Vec<(SmolStr, SmolStr)>,
    /// The snapshot a reader sees, when the table has one.
    pub current_snapshot_id: Option<i64>,
    /// Every retained snapshot.
    pub snapshots: Vec<Snapshot>,
    /// When each snapshot became current, oldest first.
    pub snapshot_log: Vec<(i64, i64)>,
    /// Every previous metadata document, oldest first.
    pub metadata_log: Vec<(i64, SmolStr)>,
    /// Named branches and tags.
    pub refs: Vec<(SmolStr, SnapshotRef)>,
    /// Next unassigned row identifier, required in v3.
    pub next_row_id: Option<i64>,
}

impl TableMetadata {
    /// Describe a new, empty table.
    ///
    /// The table has a schema, a spec, and no snapshot, which is exactly what
    /// a freshly created Iceberg table is: reading it must yield no rows rather
    /// than fail.
    ///
    /// # Errors
    ///
    /// Returns an error when the schema is not a valid non-null struct root.
    pub fn new(
        format_version: FormatVersion,
        location: impl Into<SmolStr>,
        schema: Field,
        spec: PartitionSpec,
    ) -> Result<Self> {
        schema.validate_struct_root()?;
        let last_column_id = super::last_field_id(&schema)?;
        let last_partition_id = spec.last_field_id();
        let current_schema_id = schema
            .get_metadata(super::schema::SCHEMA_ID_KEY)
            .and_then(|id| id.parse::<i32>().ok())
            .unwrap_or_default();
        Ok(Self {
            format_version,
            table_uuid: uuid(),
            location: location.into(),
            last_sequence_number: 0,
            last_updated_ms: now_ms(),
            last_column_id,
            schemas: vec![schema],
            current_schema_id,
            default_spec_id: spec.spec_id,
            partition_specs: vec![spec],
            last_partition_id,
            sort_orders: vec![SortOrder::unsorted()],
            default_sort_order_id: 0,
            properties: Vec::new(),
            current_snapshot_id: None,
            snapshots: Vec::new(),
            snapshot_log: Vec::new(),
            metadata_log: Vec::new(),
            refs: Vec::new(),
            next_row_id: (format_version >= FormatVersion::V3).then_some(0),
        })
    }

    /// Return the schema new data is written against.
    ///
    /// # Errors
    ///
    /// Returns an error when no schema carries `current-schema-id`.
    pub fn current_schema(&self) -> Result<&Field> {
        self.schema_by_id(self.current_schema_id).ok_or_else(|| {
            invalid(format_smolstr!(
                "expected a schema with id {}, got {} schemas",
                self.current_schema_id,
                self.schemas.len()
            ))
        })
    }

    /// Return one schema by identifier.
    pub fn schema_by_id(&self, schema_id: i32) -> Option<&Field> {
        self.schemas.iter().find(|schema| {
            schema
                .get_metadata(super::schema::SCHEMA_ID_KEY)
                .and_then(|id| id.parse::<i32>().ok())
                .unwrap_or_default()
                == schema_id
        })
    }

    /// Return the snapshot a reader sees, when the table has one.
    ///
    /// A table with snapshots can still have no current one - a table that was
    /// just created, or one rolled back past its first commit - so this is an
    /// `Option` rather than a failure.
    pub fn current_snapshot(&self) -> Option<&Snapshot> {
        let current = self.current_snapshot_id?;
        self.snapshot_by_id(current)
    }

    /// Return one snapshot by identifier.
    pub fn snapshot_by_id(&self, snapshot_id: i64) -> Option<&Snapshot> {
        self.snapshots
            .iter()
            .find(|snapshot| snapshot.snapshot_id == snapshot_id)
    }

    /// Return the partition spec new data is written against.
    ///
    /// # Errors
    ///
    /// Returns an error when no spec carries `default-spec-id`.
    pub fn default_spec(&self) -> Result<&PartitionSpec> {
        self.spec_by_id(self.default_spec_id).ok_or_else(|| {
            invalid(format_smolstr!(
                "expected a partition spec with id {}, got {} specs",
                self.default_spec_id,
                self.partition_specs.len()
            ))
        })
    }

    /// Return one partition spec by identifier.
    pub fn spec_by_id(&self, spec_id: i32) -> Option<&PartitionSpec> {
        self.partition_specs
            .iter()
            .find(|spec| spec.spec_id == spec_id)
    }

    /// Return one table property.
    pub fn property(&self, key: &str) -> Option<&str> {
        self.properties
            .iter()
            .find_map(|(name, value)| (name == key).then(|| value.as_str()))
    }

    /// Add a schema, or return the identifier of an equal one already present.
    ///
    /// This is what schema evolution is at the metadata level: the old schema
    /// stays, so a snapshot written under it still reads correctly, and the new
    /// one becomes current. Column identifiers continue above `last-column-id`,
    /// which is why an added column can never be confused with a dropped one.
    ///
    /// # Errors
    ///
    /// Returns an error when the schema is not a valid non-null struct root.
    pub fn add_schema(&mut self, mut schema: Field) -> Result<i32> {
        schema.validate_struct_root()?;
        let next_id = self
            .schemas
            .iter()
            .map(|existing| {
                existing
                    .get_metadata(super::schema::SCHEMA_ID_KEY)
                    .and_then(|id| id.parse::<i32>().ok())
                    .unwrap_or_default()
            })
            .max()
            .map_or(0, |highest| highest + 1);
        schema.insert_metadata(super::schema::SCHEMA_ID_KEY, next_id.to_string())?;
        self.last_column_id = self.last_column_id.max(super::last_field_id(&schema)?);
        self.schemas.push(schema);
        Ok(next_id)
    }

    /// Read a table metadata document of any format version.
    ///
    /// # Errors
    ///
    /// Returns an error when a required key is missing, when the format
    /// version is not one this build implements, or when a nested document is
    /// malformed.
    pub fn from_json(document: &Value) -> Result<Self> {
        let format_version = FormatVersion::from_number(
            document
                .get_key_str("format-version")
                .and_then(Value::as_i64)
                .ok_or_else(|| {
                    invalid(SmolStr::new_static(
                        "expected a table metadata \"format-version\"",
                    ))
                })?,
        )?;
        let location = document
            .get_key_str("location")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                invalid(SmolStr::new_static(
                    "expected a table metadata \"location\"",
                ))
            })?;

        // v1 stores the current schema as `schema`; v2 made `schemas` the
        // authority. Reading accepts both and normalizes to the plural.
        let mut schemas = Vec::new();
        for entry in document
            .get_key_str("schemas")
            .map(Value::sequence_iter)
            .unwrap_or_default()
        {
            schemas.push(schema_from_json("row", entry)?);
        }
        if schemas.is_empty() {
            if let Some(schema) = document.get_key_str("schema") {
                schemas.push(schema_from_json("row", schema)?);
            }
        }
        if schemas.is_empty() {
            return Err(invalid(SmolStr::new_static(
                "expected a table metadata \"schemas\" array or a v1 \"schema\" object",
            )));
        }

        let mut partition_specs = Vec::new();
        for entry in document
            .get_key_str("partition-specs")
            .map(Value::sequence_iter)
            .unwrap_or_default()
        {
            partition_specs.push(PartitionSpec::from_json(entry)?);
        }
        if partition_specs.is_empty() {
            partition_specs.push(match document.get_key_str("partition-spec") {
                Some(spec) => PartitionSpec::from_json(spec)?,
                None => PartitionSpec::unpartitioned(),
            });
        }

        let mut sort_orders = Vec::new();
        for entry in document
            .get_key_str("sort-orders")
            .map(Value::sequence_iter)
            .unwrap_or_default()
        {
            sort_orders.push(SortOrder::from_json(entry)?);
        }
        if sort_orders.is_empty() {
            sort_orders.push(SortOrder::unsorted());
        }

        let mut snapshots = Vec::new();
        for entry in document
            .get_key_str("snapshots")
            .map(Value::sequence_iter)
            .unwrap_or_default()
        {
            snapshots.push(Snapshot::from_json(entry)?);
        }

        let mut refs = Vec::new();
        for (name, entry) in document
            .get_key_str("refs")
            .map(Value::mapping_iter)
            .unwrap_or_default()
        {
            if let Some(name) = name.as_str() {
                refs.push((SmolStr::new(name), SnapshotRef::from_json(entry)?));
            }
        }

        // A table with no snapshot spells that as an absent key or as -1.
        let current_snapshot_id = document
            .get_key_str("current-snapshot-id")
            .and_then(Value::as_i64)
            .filter(|id| *id >= 0);

        Ok(Self {
            format_version,
            table_uuid: SmolStr::new(
                document
                    .get_key_str("table-uuid")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            ),
            location: SmolStr::new(location),
            last_sequence_number: document
                .get_key_str("last-sequence-number")
                .and_then(Value::as_i64)
                .unwrap_or_default(),
            last_updated_ms: document
                .get_key_str("last-updated-ms")
                .and_then(Value::as_i64)
                .unwrap_or_default(),
            last_column_id: document
                .get_key_str("last-column-id")
                .and_then(Value::as_i64)
                .and_then(|id| i32::try_from(id).ok())
                .unwrap_or_default(),
            current_schema_id: document
                .get_key_str("current-schema-id")
                .and_then(Value::as_i64)
                .and_then(|id| i32::try_from(id).ok())
                .unwrap_or_default(),
            schemas,
            default_spec_id: document
                .get_key_str("default-spec-id")
                .and_then(Value::as_i64)
                .and_then(|id| i32::try_from(id).ok())
                .unwrap_or_default(),
            last_partition_id: document
                .get_key_str("last-partition-id")
                .and_then(Value::as_i64)
                .and_then(|id| i32::try_from(id).ok())
                .unwrap_or(super::FIRST_PARTITION_ID - 1),
            partition_specs,
            default_sort_order_id: document
                .get_key_str("default-sort-order-id")
                .and_then(Value::as_i64)
                .and_then(|id| i32::try_from(id).ok())
                .unwrap_or_default(),
            sort_orders,
            properties: document
                .get_key_str("properties")
                .map(Value::mapping_iter)
                .unwrap_or_default()
                .filter_map(|(key, value)| {
                    Some((
                        SmolStr::new(key.as_str()?),
                        super::value::scalar_text(value),
                    ))
                })
                .collect(),
            current_snapshot_id,
            snapshots,
            snapshot_log: log_entries(document, "snapshot-log", "snapshot-id"),
            metadata_log: metadata_log(document),
            refs,
            next_row_id: document.get_key_str("next-row-id").and_then(Value::as_i64),
        })
    }

    /// Write this table metadata as the document its format version requires.
    ///
    /// # Errors
    ///
    /// Returns an error when a schema has no field identifiers or a nested
    /// document cannot be built.
    pub fn to_json(&self) -> Result<Value> {
        let mut entries: Vec<(Value, Value)> = vec![
            (
                Value::from("format-version"),
                Value::from(i64::from(self.format_version.number())),
            ),
            (
                Value::from("table-uuid"),
                Value::from(self.table_uuid.clone()),
            ),
            (Value::from("location"), Value::from(self.location.clone())),
        ];
        if self.format_version >= FormatVersion::V2 {
            entries.push((
                Value::from("last-sequence-number"),
                Value::from(self.last_sequence_number),
            ));
        }
        entries.push((
            Value::from("last-updated-ms"),
            Value::from(self.last_updated_ms),
        ));
        entries.push((
            Value::from("last-column-id"),
            Value::from(i64::from(self.last_column_id)),
        ));

        let mut schemas = Vec::with_capacity(self.schemas.len());
        for schema in &self.schemas {
            schemas.push(schema_to_json(schema)?);
        }
        if self.format_version == FormatVersion::V1 {
            // A v1 reader that predates `schemas` still needs the singular key.
            entries.push((
                Value::from("schema"),
                schema_to_json(self.current_schema()?)?,
            ));
        }
        entries.push((Value::from("schemas"), Value::from_sequence(schemas)));
        entries.push((
            Value::from("current-schema-id"),
            Value::from(i64::from(self.current_schema_id)),
        ));

        let mut specs = Vec::with_capacity(self.partition_specs.len());
        for spec in &self.partition_specs {
            specs.push(spec.to_json()?);
        }
        if self.format_version == FormatVersion::V1 {
            entries.push((
                Value::from("partition-spec"),
                self.default_spec()?.to_v1_json()?,
            ));
        }
        entries.push((Value::from("partition-specs"), Value::from_sequence(specs)));
        entries.push((
            Value::from("default-spec-id"),
            Value::from(i64::from(self.default_spec_id)),
        ));
        entries.push((
            Value::from("last-partition-id"),
            Value::from(i64::from(self.last_partition_id)),
        ));

        let mut orders = Vec::with_capacity(self.sort_orders.len());
        for order in &self.sort_orders {
            orders.push(order.to_json()?);
        }
        entries.push((Value::from("sort-orders"), Value::from_sequence(orders)));
        entries.push((
            Value::from("default-sort-order-id"),
            Value::from(i64::from(self.default_sort_order_id)),
        ));

        entries.push((
            Value::from("properties"),
            Value::from_mapping(
                self.properties
                    .iter()
                    .map(|(key, value)| (Value::from(key.clone()), Value::from(value.clone()))),
            )?,
        ));

        if let Some(current) = self.current_snapshot_id {
            entries.push((Value::from("current-snapshot-id"), Value::from(current)));
        }
        let mut snapshots = Vec::with_capacity(self.snapshots.len());
        for snapshot in &self.snapshots {
            snapshots.push(snapshot.to_json(self.format_version)?);
        }
        entries.push((Value::from("snapshots"), Value::from_sequence(snapshots)));

        entries.push((
            Value::from("snapshot-log"),
            Value::from_sequence(
                self.snapshot_log
                    .iter()
                    .map(|(timestamp, snapshot_id)| {
                        Value::from_mapping([
                            (Value::from("timestamp-ms"), Value::from(*timestamp)),
                            (Value::from("snapshot-id"), Value::from(*snapshot_id)),
                        ])
                    })
                    .collect::<Result<Vec<_>>>()?,
            ),
        ));
        entries.push((
            Value::from("metadata-log"),
            Value::from_sequence(
                self.metadata_log
                    .iter()
                    .map(|(timestamp, file)| {
                        Value::from_mapping([
                            (Value::from("timestamp-ms"), Value::from(*timestamp)),
                            (Value::from("metadata-file"), Value::from(file.clone())),
                        ])
                    })
                    .collect::<Result<Vec<_>>>()?,
            ),
        ));

        let mut refs = Vec::with_capacity(self.refs.len());
        for (name, reference) in &self.refs {
            refs.push((Value::from(name.clone()), reference.to_json()?));
        }
        entries.push((Value::from("refs"), Value::from_mapping(refs)?));

        if self.format_version >= FormatVersion::V3 {
            entries.push((
                Value::from("next-row-id"),
                Value::from(self.next_row_id.unwrap_or_default()),
            ));
        }

        Value::from_mapping(entries)
    }

    /// Make `snapshot` the current one, recording it in the log and on `main`.
    pub fn set_current_snapshot(&mut self, snapshot: Snapshot) {
        self.last_updated_ms = snapshot.timestamp_ms;
        if let Some(sequence) = snapshot.sequence_number {
            self.last_sequence_number = self.last_sequence_number.max(sequence);
        }
        self.current_snapshot_id = Some(snapshot.snapshot_id);
        self.snapshot_log
            .push((snapshot.timestamp_ms, snapshot.snapshot_id));
        let reference = SnapshotRef::branch(snapshot.snapshot_id);
        match self.refs.iter_mut().find(|(name, _)| name == MAIN_BRANCH) {
            Some(entry) => entry.1 = reference,
            None => self
                .refs
                .push((SmolStr::new_static(MAIN_BRANCH), reference)),
        }
        self.snapshots.push(snapshot);
    }
}

/// Read a `snapshot-log`-shaped array of timestamped identifiers.
fn log_entries(document: &Value, key: &str, value_key: &str) -> Vec<(i64, i64)> {
    document
        .get_key_str(key)
        .map(Value::sequence_iter)
        .unwrap_or_default()
        .filter_map(|entry| {
            Some((
                entry.get_key_str("timestamp-ms")?.as_i64()?,
                entry.get_key_str(value_key)?.as_i64()?,
            ))
        })
        .collect()
}

/// Read the `metadata-log` array of timestamped previous documents.
fn metadata_log(document: &Value) -> Vec<(i64, SmolStr)> {
    document
        .get_key_str("metadata-log")
        .map(Value::sequence_iter)
        .unwrap_or_default()
        .filter_map(|entry| {
            Some((
                entry.get_key_str("timestamp-ms")?.as_i64()?,
                SmolStr::new(entry.get_key_str("metadata-file")?.as_str()?),
            ))
        })
        .collect()
}

/// Return the current wall-clock time in milliseconds since the Unix epoch.
pub(super) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

/// Produce a random version-4 UUID for a newly created table.
///
/// A table identifier only has to be unique, so process-seeded hashing is
/// enough and avoids a dependency whose only job would be sixteen bytes.
pub(super) fn uuid() -> SmolStr {
    use std::hash::{BuildHasher, Hasher};

    let state = std::collections::hash_map::RandomState::new();
    let mut bytes = [0_u8; 16];
    for (half, chunk) in bytes.chunks_mut(8).enumerate() {
        let mut hasher = state.build_hasher();
        hasher.write_usize(half);
        hasher.write_i64(now_ms());
        chunk.copy_from_slice(&hasher.finish().to_le_bytes()[..chunk.len()]);
    }
    // Stamp the version and variant so the value is a well-formed UUIDv4.
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;

    let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    format_smolstr!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

/// Report a malformed Iceberg table metadata document.
fn invalid(reason: SmolStr) -> Error {
    Error::Codec {
        format: "iceberg",
        position: 0,
        reason,
    }
}
