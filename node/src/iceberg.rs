//! Apache Iceberg tables, reached from JavaScript through one handle.
//!
//! A table is a folder: `metadata/` holds the JSON documents and the Avro
//! manifests, `data/` holds the Parquet files, and every one of them is a child
//! of the handle the table was built from. Nothing here opens a path, so the
//! same JavaScript works over a local directory today and over an object store
//! the moment a backend for one exists.

use std::collections::HashMap;

use napi::bindgen_prelude::{BigInt, Buffer, ClassInstance, Either, Result};
use napi_derive::napi;
use yggdryl::Field as CoreField;
use yggdryl::generic::Holder;
use yggdryl::iceberg::{
    DataFile, FormatVersion, ManifestContent, ManifestFile, PartitionSpec as CorePartitionSpec,
    Snapshot, Table as CoreTable, assign_field_ids, schema_from_json, schema_to_json,
};

use crate::arrow::JsBatchReader;
use crate::codec::JsCodecValue;
use crate::field::JsField;
use crate::io::{JsIOBase, LocationInput, folder_from_input};
use crate::napi_error;

/// A partition spec, or the column names one would be built from.
pub type PartitionInput<'a> = Either<ClassInstance<'a, JsPartitionSpec>, Vec<String>>;

/// Read the format version a number names, defaulting to v2.
fn format_version(value: Option<u32>) -> Result<FormatVersion> {
    match value {
        Some(number) => FormatVersion::from_number(i64::from(number)).map_err(napi_error),
        None => Ok(FormatVersion::V2),
    }
}

/// Resolve what a caller partitioned by, defaulting to unpartitioned.
///
/// Column names are the short spelling of the only spec a write can use, so
/// they build an identity spec against the schema they name columns of.
fn partition_spec(
    value: Option<PartitionInput<'_>>,
    schema: &CoreField,
) -> Result<CorePartitionSpec> {
    match value {
        None => Ok(CorePartitionSpec::unpartitioned()),
        Some(Either::A(spec)) => Ok(spec.inner.clone()),
        Some(Either::B(columns)) => {
            let names: Vec<&str> = columns.iter().map(String::as_str).collect();
            CorePartitionSpec::identity(0, schema, &names).map_err(napi_error)
        }
    }
}

/// One partition field of a spec.
#[napi(object)]
pub struct PartitionFieldView {
    /// Identifier of the schema column the value is derived from.
    pub source_id: i32,
    /// Identifier of the partition field itself.
    pub field_id: i32,
    /// The directory name this field writes.
    pub name: String,
    /// The transform applied to the source column.
    pub transform: String,
}

/// One per-column count a manifest records, keyed by field identifier.
#[napi(object)]
pub struct FieldCount {
    /// The schema field identifier the count belongs to.
    pub field_id: i32,
    /// The recorded count.
    pub count: i64,
}

/// One per-column bound a manifest records, as its encoded bytes.
#[napi(object)]
pub struct FieldBound {
    /// The schema field identifier the bound belongs to.
    pub field_id: i32,
    /// The single-value encoding of the bound.
    pub value: Buffer,
}

/// One committed version of a table's contents.
#[napi(object)]
pub struct SnapshotView {
    /// Identifier of this snapshot, unique within the table.
    pub snapshot_id: BigInt,
    /// The snapshot this one was produced from, when there was one.
    pub parent_snapshot_id: Option<BigInt>,
    /// Monotonic commit order, absent in v1 tables.
    pub sequence_number: Option<i64>,
    /// Wall-clock commit time in milliseconds since the Unix epoch.
    pub timestamp_ms: i64,
    /// Location of the Avro manifest list this snapshot's manifests are in.
    pub manifest_list: String,
    /// What the commit did, defaulting to `append`.
    pub operation: String,
    /// The commit summary, keyed by Iceberg's summary vocabulary.
    pub summary: HashMap<String, String>,
    /// The schema in effect when the snapshot was written.
    pub schema_id: Option<i32>,
}

/// One manifest of the current snapshot.
#[napi(object)]
pub struct ManifestFileView {
    /// The manifest's location, as a URI.
    pub manifest_path: String,
    /// Size of the manifest in bytes.
    pub manifest_length: i64,
    /// The partition spec the manifest's entries were written under.
    pub partition_spec_id: i32,
    /// Whether the manifest lists `data` files or `deletes`.
    pub content: String,
    /// Commit order assigned when the manifest was added.
    pub sequence_number: i64,
    /// Lowest commit order of any entry in the manifest.
    pub min_sequence_number: i64,
    /// The snapshot that added the manifest.
    pub added_snapshot_id: BigInt,
    /// Files the manifest marks added.
    pub added_files_count: i32,
    /// Files the manifest marks existing.
    pub existing_files_count: i32,
    /// Files the manifest marks deleted.
    pub deleted_files_count: i32,
    /// Rows in the added files.
    pub added_rows_count: i64,
    /// Rows in the existing files.
    pub existing_rows_count: i64,
    /// Rows in the deleted files.
    pub deleted_rows_count: i64,
}

/// One live data file of the current snapshot, with the spec that placed it.
///
/// This is a class rather than a plain object because a partition value crosses
/// as the native [`Value`](crate::codec::JsCodecValue) the manifest recorded.
/// Rendering it as text here would have to spell a null `null`, which is exactly
/// what makes a directory name unable to answer the question.
#[napi(js_name = "DataFile")]
pub struct JsDataFile {
    /// The manifest's record of the file.
    file: DataFile,
    /// The spec its partition tuple is ordered by.
    spec: CorePartitionSpec,
}

#[napi]
impl JsDataFile {
    /// Zero for rows, one for position deletes, two for equality deletes.
    #[napi(getter)]
    pub const fn content(&self) -> i32 {
        self.file.content
    }

    /// The file's location, as a URI.
    #[napi(getter)]
    pub fn file_path(&self) -> String {
        self.file.file_path.to_string()
    }

    /// The encoding the file uses.
    #[napi(getter)]
    pub fn file_format(&self) -> String {
        self.file.file_format.to_string()
    }

    /// The partition tuple the manifest records, in spec order.
    #[napi(getter)]
    pub fn partition(&self) -> Vec<JsCodecValue> {
        self.file
            .partition
            .iter()
            .map(|value| JsCodecValue::from_core(value.clone()))
            .collect()
    }

    /// The partition field names, in the same order as the tuple.
    #[napi(getter)]
    pub fn partition_names(&self) -> Vec<String> {
        self.spec
            .fields
            .iter()
            .map(|field| field.name.to_string())
            .collect()
    }

    /// Rows in the file.
    #[napi(getter)]
    pub const fn record_count(&self) -> i64 {
        self.file.record_count
    }

    /// Size of the file in bytes.
    #[napi(getter)]
    pub const fn file_size_in_bytes(&self) -> i64 {
        self.file.file_size_in_bytes
    }

    /// Stored bytes per column.
    #[napi(getter)]
    pub fn column_sizes(&self) -> Vec<FieldCount> {
        counts(&self.file.column_sizes)
    }

    /// Values per column.
    #[napi(getter)]
    pub fn value_counts(&self) -> Vec<FieldCount> {
        counts(&self.file.value_counts)
    }

    /// Nulls per column.
    #[napi(getter)]
    pub fn null_value_counts(&self) -> Vec<FieldCount> {
        counts(&self.file.null_value_counts)
    }

    /// Serialized minimum per column, where the two encodings agree on one.
    #[napi(getter)]
    pub fn lower_bounds(&self) -> Vec<FieldBound> {
        bounds(&self.file.lower_bounds)
    }

    /// Serialized maximum per column, where the two encodings agree on one.
    #[napi(getter)]
    pub fn upper_bounds(&self) -> Vec<FieldBound> {
        bounds(&self.file.upper_bounds)
    }

    /// The sort order the file was written in, when one applies.
    #[napi(getter)]
    pub const fn sort_order_id(&self) -> Option<i32> {
        self.file.sort_order_id
    }

    /// Return the file's location, so a data file prints as where it is.
    #[napi]
    pub fn to_string(&self) -> String {
        self.file_path()
    }
}

fn counts(values: &[(i32, i64)]) -> Vec<FieldCount> {
    values
        .iter()
        .map(|(field_id, count)| FieldCount {
            field_id: *field_id,
            count: *count,
        })
        .collect()
}

fn bounds(values: &[(i32, Vec<u8>)]) -> Vec<FieldBound> {
    values
        .iter()
        .map(|(field_id, value)| FieldBound {
            field_id: *field_id,
            value: value.clone().into(),
        })
        .collect()
}

fn snapshot_view(snapshot: &Snapshot) -> SnapshotView {
    SnapshotView {
        snapshot_id: BigInt::from(snapshot.snapshot_id),
        parent_snapshot_id: snapshot.parent_snapshot_id.map(BigInt::from),
        sequence_number: snapshot.sequence_number,
        timestamp_ms: snapshot.timestamp_ms,
        manifest_list: snapshot.manifest_list.to_string(),
        operation: snapshot.operation().to_owned(),
        summary: snapshot
            .summary
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect(),
        schema_id: snapshot.schema_id,
    }
}

/// Name what a manifest's entries describe.
///
/// The core enum is non-exhaustive, so a content this build does not have a
/// word for crosses as the integer Iceberg stores rather than as a panic.
fn manifest_content(content: ManifestContent) -> String {
    match content {
        ManifestContent::Data => "data".to_owned(),
        ManifestContent::Deletes => "deletes".to_owned(),
        other => other.code().to_string(),
    }
}

fn manifest_view(manifest: &ManifestFile) -> ManifestFileView {
    ManifestFileView {
        manifest_path: manifest.manifest_path.to_string(),
        manifest_length: manifest.manifest_length,
        partition_spec_id: manifest.partition_spec_id,
        content: manifest_content(manifest.content),
        sequence_number: manifest.sequence_number,
        min_sequence_number: manifest.min_sequence_number,
        added_snapshot_id: BigInt::from(manifest.added_snapshot_id),
        added_files_count: manifest.added_files_count,
        existing_files_count: manifest.existing_files_count,
        deleted_files_count: manifest.deleted_files_count,
        added_rows_count: manifest.added_rows_count,
        existing_rows_count: manifest.existing_rows_count,
        deleted_rows_count: manifest.deleted_rows_count,
    }
}

/// How a table turns column values into the directories it writes.
#[napi(js_name = "PartitionSpec")]
pub struct JsPartitionSpec {
    pub(crate) inner: CorePartitionSpec,
}

impl JsPartitionSpec {
    pub(crate) const fn from_core(inner: CorePartitionSpec) -> Self {
        Self { inner }
    }
}

#[napi]
impl JsPartitionSpec {
    /// Describe a table that writes every row into one place.
    #[napi(factory)]
    pub fn unpartitioned() -> Self {
        Self::from_core(CorePartitionSpec::unpartitioned())
    }

    /// Partition by the named columns, storing each value as it stands.
    ///
    /// Identity is one of the two transforms that can place a row, so this is
    /// the spec a write can use; a `bucket`, `truncate`, or calendar spec reads
    /// here but is refused by name when it would have to place a row.
    #[napi(factory)]
    pub fn identity(schema: &JsField, columns: Vec<String>, spec_id: Option<i32>) -> Result<Self> {
        let names: Vec<&str> = columns.iter().map(String::as_str).collect();
        CorePartitionSpec::identity(spec_id.unwrap_or(0), &schema.inner, &names)
            .map(Self::from_core)
            .map_err(napi_error)
    }

    /// The identifier this spec is recorded under.
    #[napi(getter)]
    pub const fn spec_id(&self) -> i32 {
        self.inner.spec_id
    }

    /// The partition fields, in the order the directories nest.
    #[napi(getter)]
    pub fn fields(&self) -> Vec<PartitionFieldView> {
        self.inner
            .fields
            .iter()
            .map(|field| PartitionFieldView {
                source_id: field.source_id,
                field_id: field.field_id,
                name: field.name.to_string(),
                transform: field.transform.to_string(),
            })
            .collect()
    }

    /// Return whether this spec writes every row into one place.
    #[napi]
    pub fn is_unpartitioned(&self) -> bool {
        self.inner.is_unpartitioned()
    }
}

/// An Iceberg table reached entirely through one container handle.
#[napi(js_name = "Table")]
pub struct JsTable {
    inner: CoreTable<Holder>,
}

impl JsTable {
    const fn from_core(inner: CoreTable<Holder>) -> Self {
        Self { inner }
    }

    /// The root name a scan's batches are described by.
    fn root_name(&self) -> Result<String> {
        Ok(self.inner.schema().map_err(napi_error)?.name().to_owned())
    }
}

#[napi]
impl JsTable {
    /// Create a table, writing its first metadata document.
    ///
    /// `partitionBy` takes a [`PartitionSpec`](JsPartitionSpec) or the column
    /// names to partition on, and defaults to unpartitioned. The schema must
    /// carry field identifiers, which `assignFieldIds` supplies.
    #[napi(factory)]
    pub fn create(
        root: LocationInput<'_>,
        schema: &JsField,
        partition_by: Option<PartitionInput<'_>>,
        version: Option<u32>,
    ) -> Result<Self> {
        CoreTable::create(
            folder_from_input(root)?,
            format_version(version)?,
            schema.inner.clone(),
            partition_spec(partition_by, &schema.inner)?,
        )
        .map(Self::from_core)
        .map_err(napi_error)
    }

    /// Open the table a container handle addresses.
    #[napi(factory)]
    pub fn open(root: LocationInput<'_>) -> Result<Self> {
        CoreTable::open(folder_from_input(root)?)
            .map(Self::from_core)
            .map_err(napi_error)
    }

    /// Open the table if it exists, creating it otherwise.
    #[napi(factory)]
    pub fn open_or_create(
        root: LocationInput<'_>,
        schema: &JsField,
        partition_by: Option<PartitionInput<'_>>,
        version: Option<u32>,
    ) -> Result<Self> {
        CoreTable::open_or_create(
            folder_from_input(root)?,
            format_version(version)?,
            schema.inner.clone(),
            partition_spec(partition_by, &schema.inner)?,
        )
        .map(Self::from_core)
        .map_err(napi_error)
    }

    /// The folder the table lives in.
    #[napi(getter)]
    pub fn root(&self) -> Result<JsIOBase> {
        JsIOBase::folder_at(&self.location())
    }

    /// The table's base location, as a URI.
    #[napi(getter)]
    pub fn location(&self) -> String {
        self.inner.metadata().location.to_string()
    }

    /// A stable identifier for the table itself, not for any one version.
    #[napi(getter)]
    pub fn table_uuid(&self) -> String {
        self.inner.metadata().table_uuid.to_string()
    }

    /// Which revision of the specification the metadata is written to.
    #[napi(getter)]
    pub const fn format_version(&self) -> i32 {
        self.inner.metadata().format_version.number()
    }

    /// The version number of the current metadata document.
    #[napi(getter)]
    pub const fn version(&self) -> u32 {
        self.inner.version()
    }

    /// Free-form table properties.
    #[napi(getter)]
    pub fn properties(&self) -> HashMap<String, String> {
        self.inner
            .metadata()
            .properties
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    /// The name of the current metadata document.
    #[napi(getter)]
    pub fn metadata_file_name(&self) -> String {
        self.inner.metadata_file_name()
    }

    /// The location of the current metadata document, as a URI.
    #[napi(getter)]
    pub fn metadata_location(&self) -> Result<String> {
        self.inner.metadata_location().map_err(napi_error)
    }

    /// The schema new data is written against.
    #[napi(getter)]
    pub fn schema(&self) -> Result<JsField> {
        self.inner
            .schema()
            .map(|schema| JsField::from_core(schema.clone()))
            .map_err(napi_error)
    }

    /// The partition spec new data is written against.
    #[napi(getter)]
    pub fn spec(&self) -> JsPartitionSpec {
        let metadata = self.inner.metadata();
        JsPartitionSpec::from_core(
            metadata
                .partition_specs
                .iter()
                .find(|spec| spec.spec_id == metadata.default_spec_id)
                .cloned()
                .unwrap_or_else(CorePartitionSpec::unpartitioned),
        )
    }

    /// The snapshot a reader sees, or `null` when the table has none.
    ///
    /// A freshly created or rolled-back table has snapshots but no current one,
    /// and reading it yields no rows rather than failing.
    #[napi(getter)]
    pub fn current_snapshot(&self) -> Option<SnapshotView> {
        self.inner.current_snapshot().map(snapshot_view)
    }

    /// Every schema the table has had, oldest first.
    #[napi(getter)]
    pub fn schemas(&self) -> Vec<JsField> {
        self.inner
            .metadata()
            .schemas
            .iter()
            .cloned()
            .map(JsField::from_core)
            .collect()
    }

    /// Every retained snapshot, oldest first.
    #[napi(getter)]
    pub fn snapshots(&self) -> Vec<SnapshotView> {
        self.inner
            .metadata()
            .snapshots
            .iter()
            .map(snapshot_view)
            .collect()
    }

    /// Every manifest the current snapshot points at.
    #[napi]
    pub fn manifests(&self) -> Result<Vec<ManifestFileView>> {
        Ok(self
            .inner
            .manifests()
            .map_err(napi_error)?
            .iter()
            .map(manifest_view)
            .collect())
    }

    /// Every live data file of the current snapshot.
    #[napi]
    pub fn data_files(&self) -> Result<Vec<JsDataFile>> {
        Ok(self
            .inner
            .data_files()
            .map_err(napi_error)?
            .into_iter()
            .map(|(file, spec)| JsDataFile { file, spec })
            .collect())
    }

    /// Read every row of the current snapshot, keeping the columns `field` names.
    ///
    /// Unlike a plain handle read, a scan *casts* each file to the root it is
    /// given after pushing the columns down, which is what makes a table whose
    /// schema evolved readable as one shape.
    #[napi]
    pub fn scan(&self, field: Option<&JsField>) -> Result<JsBatchReader> {
        let root_name = self.root_name()?;
        let reader = self
            .inner
            .scan(field.map(|field| &field.inner))
            .map_err(napi_error)?;
        Ok(JsBatchReader::from_core(reader, &root_name))
    }

    /// Append `batches` as a new snapshot, keeping everything already stored.
    #[napi]
    pub fn append(&mut self, batches: &mut JsBatchReader) -> Result<()> {
        self.inner.append(batches.take()?).map_err(napi_error)
    }

    /// Replace every row with `batches` as a new snapshot.
    ///
    /// The previous snapshot stays readable; only the current pointer moves.
    #[napi]
    pub fn overwrite(&mut self, batches: &mut JsBatchReader) -> Result<()> {
        self.inner.overwrite(batches.take()?).map_err(napi_error)
    }

    /// Add a schema, make it current, and write a new metadata document.
    #[napi]
    pub fn evolve_schema(&mut self, schema: &JsField) -> Result<i32> {
        self.inner
            .evolve_schema(schema.inner.clone())
            .map_err(napi_error)
    }

    /// Return where the table lives, so a table prints as its location.
    #[napi]
    pub fn to_string(&self) -> String {
        self.location()
    }
}

/// Number every column of a schema, so an Iceberg table can carry it.
///
/// Returns a copy: a Field is a value here, and numbering one in place would
/// change a schema another table already holds.
#[napi(js_name = "icebergAssignFieldIdsNative", skip_typescript)]
pub fn iceberg_assign_field_ids(schema: &JsField, start: Option<i32>) -> Result<JsField> {
    let mut schema: CoreField = schema.inner.clone();
    assign_field_ids(&mut schema, start.unwrap_or(1)).map_err(napi_error)?;
    Ok(JsField::from_core(schema))
}

/// Read an Iceberg schema document as a root Field.
#[napi(js_name = "icebergSchemaFromJsonNative", skip_typescript)]
pub fn iceberg_schema_from_json(name: String, document: &JsCodecValue) -> Result<JsField> {
    schema_from_json(&name, &document.inner)
        .map(JsField::from_core)
        .map_err(napi_error)
}

/// Write a root Field as an Iceberg schema document.
#[napi(js_name = "icebergSchemaToJsonNative", skip_typescript)]
pub fn iceberg_schema_to_json(schema: &JsField) -> Result<JsCodecValue> {
    schema_to_json(&schema.inner)
        .map(JsCodecValue::from_core)
        .map_err(napi_error)
}
