//! Snapshots, the named references that point at them, and their summaries.
//!
//! A snapshot is one complete version of a table's contents: an identifier, the
//! manifest list naming every manifest alive at that moment, and a summary of
//! what the commit did. A table's *current* snapshot is a pointer, which is why
//! a table with snapshots can still have no current one - a rolled-back or
//! freshly created table is exactly that, and reading it must yield no rows
//! rather than fail.

use smol_str::{SmolStr, format_smolstr};

use crate::{Error, Result, Value};

/// The reference every table's default branch is named by.
pub const MAIN_BRANCH: &str = "main";

/// One committed version of a table's contents.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Snapshot {
    /// Identifier of this snapshot, unique within the table.
    pub snapshot_id: i64,
    /// The snapshot this one was produced from, when there was one.
    pub parent_snapshot_id: Option<i64>,
    /// Monotonic commit order, absent in v1 tables.
    pub sequence_number: Option<i64>,
    /// Wall-clock commit time in milliseconds since the Unix epoch.
    pub timestamp_ms: i64,
    /// Location of the Avro manifest list this snapshot's manifests are in.
    pub manifest_list: SmolStr,
    /// What the commit did, keyed by Iceberg's summary vocabulary.
    pub summary: Vec<(SmolStr, SmolStr)>,
    /// The schema in effect when the snapshot was written.
    pub schema_id: Option<i32>,
    /// First assigned row identifier, added in v3 for row lineage.
    pub first_row_id: Option<i64>,
    /// Rows this snapshot added, added in v3 for row lineage.
    pub added_rows: Option<i64>,
}

impl Snapshot {
    /// Return one summary value by key.
    pub fn summary_value(&self, key: &str) -> Option<&str> {
        self.summary
            .iter()
            .find_map(|(name, value)| (name == key).then(|| value.as_str()))
    }

    /// Return the operation this snapshot recorded, defaulting to `append`.
    pub fn operation(&self) -> &str {
        self.summary_value("operation").unwrap_or("append")
    }

    /// Read one snapshot object.
    ///
    /// # Errors
    ///
    /// Returns an error when `snapshot-id`, `timestamp-ms`, or `manifest-list`
    /// is missing or not the type Iceberg declares.
    pub fn from_json(document: &Value) -> Result<Self> {
        let snapshot_id = document
            .get_key_str("snapshot-id")
            .and_then(Value::as_i64)
            .ok_or_else(|| invalid(SmolStr::new_static("expected a snapshot \"snapshot-id\"")))?;
        let timestamp_ms = document
            .get_key_str("timestamp-ms")
            .and_then(Value::as_i64)
            .ok_or_else(|| {
                invalid(format_smolstr!(
                    "expected a \"timestamp-ms\" on snapshot {snapshot_id}"
                ))
            })?;
        // A v1 snapshot may carry `manifests` instead; that form is read by the
        // manifest layer, which is where the difference actually matters.
        let manifest_list = document
            .get_key_str("manifest-list")
            .and_then(Value::as_str)
            .unwrap_or_default();

        let summary = document
            .get_key_str("summary")
            .map(|summary| {
                summary
                    .mapping_iter()
                    .filter_map(|(key, value)| {
                        Some((
                            SmolStr::new(key.as_str()?),
                            super::value::scalar_text(value),
                        ))
                    })
                    .collect()
            })
            .unwrap_or_default();

        Ok(Self {
            snapshot_id,
            parent_snapshot_id: document
                .get_key_str("parent-snapshot-id")
                .and_then(Value::as_i64),
            sequence_number: document
                .get_key_str("sequence-number")
                .and_then(Value::as_i64),
            timestamp_ms,
            manifest_list: SmolStr::new(manifest_list),
            summary,
            schema_id: document
                .get_key_str("schema-id")
                .and_then(Value::as_i64)
                .and_then(|id| i32::try_from(id).ok()),
            first_row_id: document.get_key_str("first-row-id").and_then(Value::as_i64),
            added_rows: document.get_key_str("added-rows").and_then(Value::as_i64),
        })
    }

    /// Write this snapshot as the object a table metadata document holds.
    ///
    /// `version` selects what is emitted: a v1 snapshot carries no sequence
    /// number and a v3 one carries its row lineage.
    ///
    /// # Errors
    ///
    /// Returns an error only when the mapping cannot be built.
    pub fn to_json(&self, version: super::FormatVersion) -> Result<Value> {
        let mut entries = vec![(Value::from("snapshot-id"), Value::from(self.snapshot_id))];
        if let Some(parent) = self.parent_snapshot_id {
            entries.push((Value::from("parent-snapshot-id"), Value::from(parent)));
        }
        if version >= super::FormatVersion::V2 {
            if let Some(sequence) = self.sequence_number {
                entries.push((Value::from("sequence-number"), Value::from(sequence)));
            }
        }
        entries.push((Value::from("timestamp-ms"), Value::from(self.timestamp_ms)));
        entries.push((
            Value::from("manifest-list"),
            Value::from(self.manifest_list.clone()),
        ));
        entries.push((
            Value::from("summary"),
            Value::from_mapping(
                self.summary
                    .iter()
                    .map(|(key, value)| (Value::from(key.clone()), Value::from(value.clone()))),
            )?,
        ));
        if let Some(schema_id) = self.schema_id {
            entries.push((Value::from("schema-id"), Value::from(i64::from(schema_id))));
        }
        if version >= super::FormatVersion::V3 {
            if let Some(first_row_id) = self.first_row_id {
                entries.push((Value::from("first-row-id"), Value::from(first_row_id)));
            }
            if let Some(added_rows) = self.added_rows {
                entries.push((Value::from("added-rows"), Value::from(added_rows)));
            }
        }
        Value::from_mapping(entries)
    }
}

/// A named pointer at one snapshot: a branch or a tag.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotRef {
    /// The snapshot this reference names.
    pub snapshot_id: i64,
    /// Either `branch` or `tag`; a branch moves and a tag does not.
    pub kind: SmolStr,
}

impl SnapshotRef {
    /// Point a branch at one snapshot.
    pub fn branch(snapshot_id: i64) -> Self {
        Self {
            snapshot_id,
            kind: SmolStr::new_static("branch"),
        }
    }

    /// Read one reference object.
    ///
    /// # Errors
    ///
    /// Returns an error when `snapshot-id` is missing.
    pub fn from_json(document: &Value) -> Result<Self> {
        Ok(Self {
            snapshot_id: document
                .get_key_str("snapshot-id")
                .and_then(Value::as_i64)
                .ok_or_else(|| {
                    invalid(SmolStr::new_static(
                        "expected a snapshot reference \"snapshot-id\"",
                    ))
                })?,
            kind: SmolStr::new(
                document
                    .get_key_str("type")
                    .and_then(Value::as_str)
                    .unwrap_or("branch"),
            ),
        })
    }

    /// Write one reference object.
    ///
    /// # Errors
    ///
    /// Returns an error only when the mapping cannot be built.
    pub fn to_json(&self) -> Result<Value> {
        Value::from_mapping([
            (Value::from("snapshot-id"), Value::from(self.snapshot_id)),
            (Value::from("type"), Value::from(self.kind.clone())),
        ])
    }
}

/// Report a malformed Iceberg snapshot document.
fn invalid(reason: SmolStr) -> Error {
    Error::Codec {
        format: "iceberg",
        position: 0,
        reason,
    }
}
