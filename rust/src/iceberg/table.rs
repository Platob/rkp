//! An Iceberg table as a folder handle and nothing else.
//!
//! A table *is* a directory: `metadata/` holds the JSON documents and the Avro
//! manifests, `data/` holds the Parquet files, and every one of them is reached
//! with [`IOBase::child_by`] against the handle the table was constructed from.
//! There is no path opening and no file-system call anywhere below here, which
//! is what makes the same code work over a local folder today and over an
//! object store the moment a backend for one exists.
//!
//! ```no_run
//! use yggdryl::iceberg::{FormatVersion, PartitionSpec, Table, assign_field_ids};
//! use yggdryl::local::Folder;
//! use yggdryl::{DataType, Field};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mut schema = DataType::from_fields([
//!     DataType::Int64.required_field("id"),
//!     DataType::Utf8.nullable_field("venue"),
//! ])?
//! .required_field("row");
//! assign_field_ids(&mut schema, 1)?;
//!
//! let folder = Folder::new(std::env::temp_dir().join("trades"))?;
//! let spec = PartitionSpec::identity(0, &schema, &["venue"])?;
//! let table = Table::create(folder, FormatVersion::V2, schema, spec)?;
//!
//! // A table with no snapshot yet reads as no rows, never as a failure.
//! assert!(table.current_snapshot().is_none());
//! assert_eq!(table.scan(None)?.count(), 0);
//! # Ok(())
//! # }
//! ```
//!
//! # What a commit costs
//!
//! Committing means writing a new metadata document, so an append writes at
//! least one Parquet file per partition - more when a partition's rows exceed
//! [`Table::target_file_size`] - one manifest, one manifest list, and one
//! metadata JSON. Nothing is mutated in place, which is what makes the previous
//! snapshot still readable afterwards.

use std::collections::HashMap;

use arrow_array::{Array, ArrayRef, RecordBatch, UInt32Array};
use arrow_schema::SortOptions;
use smol_str::{SmolStr, format_smolstr};

use super::manifest::{
    DataFile, FieldSummary, FileFormat, ManifestContent, ManifestEntry, ManifestFile,
    read_manifest_list, write_manifest, write_manifest_list,
};
use super::metadata::{FormatVersion, TableMetadata, now_ms, uuid};
use super::partition::PartitionSpec;
use super::scan::{Filter, ScanPart, ScanPlan, ScanTask};
use super::snapshot::Snapshot;
use super::value::{compare_single, is_portable, single_value};
use crate::arrow::BatchReader;
use crate::field::cast::ArrowCast;
use crate::generic::{Holder, IORecordOptions};
use crate::io::IOBase;
use crate::{DataType, Error, Field, Result, Value};

/// The directory a table keeps its metadata documents and manifests in.
const METADATA_DIR: &str = "metadata";

/// The directory a table keeps its data files in.
const DATA_DIR: &str = "data";

/// The file naming the current metadata version, as `HadoopTables` writes it.
const VERSION_HINT: &str = "version-hint.text";

/// The property naming the size a data file aims for, in bytes.
///
/// The table property carries this name exactly; the schema-root fallback is
/// the same name under the `iceberg:` protocol prefix.
const TARGET_FILE_SIZE_KEY: &str = "write.target-file-size-bytes";

/// The size a data file aims for when nothing configures one.
///
/// This is Iceberg's own documented default for
/// `write.target-file-size-bytes`: 512 MiB.
const DEFAULT_TARGET_FILE_SIZE: u64 = 512 * 1024 * 1024;

/// An Iceberg table reached entirely through one container handle.
///
/// The handle is whatever [`IOBase`] implementation addresses the table's
/// folder. Everything below - metadata documents, manifest lists, manifests,
/// data files - is a child of it.
#[derive(Debug)]
pub struct Table<H: IOBase> {
    /// The folder the table lives in.
    root: H,
    /// The parsed current metadata document.
    metadata: TableMetadata,
    /// The version number of the metadata document that was last written.
    version: u32,
}

impl<H: IOBase> Table<H> {
    /// Create a table, writing its first metadata document.
    ///
    /// The table has a schema and a partition spec but no snapshot, which is
    /// exactly what a newly created Iceberg table is.
    ///
    /// # Errors
    ///
    /// Returns an error when the handle is not a container, when the schema is
    /// not a non-null struct root carrying field identifiers, or when the
    /// metadata document cannot be written.
    pub fn create(
        root: H,
        format_version: FormatVersion,
        schema: Field,
        spec: PartitionSpec,
    ) -> Result<Self> {
        let location = root.url().map(ToString::to_string).ok_or_else(|| {
            invalid(SmolStr::new_static(
                "expected a located container to create a table in, got a handle with no URL",
            ))
        })?;
        let metadata = TableMetadata::new(format_version, location, schema, spec)?;
        let mut table = Self {
            root,
            metadata,
            version: 0,
        };
        table.commit_metadata()?;
        Ok(table)
    }

    /// Open the table a container handle addresses.
    ///
    /// The current document is the one `metadata/version-hint.text` names; a
    /// table written by something that keeps no hint falls back to the
    /// highest-numbered `*.metadata.json` in the metadata directory.
    ///
    /// # Errors
    ///
    /// Returns an error when the folder holds no metadata document, or when
    /// the document is not table metadata.
    pub fn open(root: H) -> Result<Self> {
        let metadata_dir = root.child_by(METADATA_DIR)?;
        match find_metadata(&metadata_dir)? {
            Some((version, document)) => Ok(Self {
                root,
                metadata: TableMetadata::from_json(&document)?,
                version,
            }),
            None => Err(missing_metadata(&metadata_dir)),
        }
    }

    /// Open the table a handle addresses, or say plainly that it is not one.
    ///
    /// This is the question [`IOBase`]'s record methods ask of every container
    /// they are handed: a folder holding a metadata document is read through its
    /// snapshots, and a folder that is not one is read as the leaves beneath it.
    /// A folder that *is* a table but whose current document is malformed is an
    /// error rather than a `None`, because that is a broken table and not an
    /// ordinary directory.
    ///
    /// # Errors
    ///
    /// Returns an error when a metadata document is found but is not table
    /// metadata.
    pub fn locate(root: H) -> Result<Option<Self>> {
        let metadata_dir = root.child_by(METADATA_DIR)?;
        let Some((version, document)) = find_metadata(&metadata_dir)? else {
            return Ok(None);
        };
        Ok(Some(Self {
            root,
            metadata: TableMetadata::from_json(&document)?,
            version,
        }))
    }

    /// Open the table if it exists, creating it otherwise.
    ///
    /// # Errors
    ///
    /// Returns the failure of whichever operation ran.
    pub fn open_or_create(
        root: H,
        format_version: FormatVersion,
        schema: Field,
        spec: PartitionSpec,
    ) -> Result<Self> {
        let metadata_dir = root.child_by(METADATA_DIR)?;
        if find_metadata(&metadata_dir)?.is_some() {
            return Self::open(root);
        }
        Self::create(root, format_version, schema, spec)
    }

    /// Borrow the container the table lives in.
    pub const fn root(&self) -> &H {
        &self.root
    }

    /// Borrow the current table metadata.
    pub const fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }

    /// Return the version number of the current metadata document.
    pub const fn version(&self) -> u32 {
        self.version
    }

    /// Return the name of the current metadata document.
    pub fn metadata_file_name(&self) -> String {
        format!("v{}.metadata.json", self.version)
    }

    /// Return the location of the current metadata document, as a URI.
    ///
    /// # Errors
    ///
    /// Returns an error when the metadata child has no URL.
    pub fn metadata_location(&self) -> Result<String> {
        Ok(format!(
            "{}/{METADATA_DIR}/{}",
            self.metadata.location.trim_end_matches('/'),
            self.metadata_file_name()
        ))
    }

    /// Borrow the schema new data is written against.
    ///
    /// # Errors
    ///
    /// Returns an error when no schema carries the current schema identifier.
    pub fn schema(&self) -> Result<&Field> {
        self.metadata.current_schema()
    }

    /// Borrow the snapshot a reader sees, when the table has one.
    pub fn current_snapshot(&self) -> Option<&Snapshot> {
        self.metadata.current_snapshot()
    }

    /// Return the size a data file aims for, in bytes.
    ///
    /// The table property `write.target-file-size-bytes` decides. A table that
    /// does not set it falls back to the schema root's
    /// `iceberg:write.target-file-size-bytes` protocol property, and a table
    /// that sets neither uses Iceberg's own default of 512 MiB.
    ///
    /// What a write measures against this target is the Arrow in-memory size
    /// of the accumulated batches ([`RecordBatch::get_array_memory_size`]),
    /// estimated *before* encoding. Parquet compresses what it writes, so data
    /// files land under the target rather than at it.
    ///
    /// # Errors
    ///
    /// Returns a typed error naming the key and the value when either property
    /// is present but does not spell a positive byte count; a configured
    /// target is never silently replaced by the default.
    pub fn target_file_size(&self) -> Result<u64> {
        if let Some(text) = self.metadata.property(TARGET_FILE_SIZE_KEY) {
            return parsed_target(SmolStr::new_static(TARGET_FILE_SIZE_KEY), text);
        }
        let root = self.schema()?.iceberg();
        match root.get(TARGET_FILE_SIZE_KEY) {
            Some(text) => parsed_target(SmolStr::new(root.key(TARGET_FILE_SIZE_KEY)), text),
            None => Ok(DEFAULT_TARGET_FILE_SIZE),
        }
    }

    /// Return every manifest the current snapshot points at.
    ///
    /// A table with no current snapshot has no manifests, which is not a
    /// failure: an empty table simply reads as nothing.
    ///
    /// # Errors
    ///
    /// Returns an error when the manifest list cannot be reached or decoded.
    pub fn manifests(&self) -> Result<Vec<ManifestFile>> {
        match self.current_snapshot() {
            Some(snapshot) => self.manifests_at(snapshot),
            None => Ok(Vec::new()),
        }
    }

    /// Return every manifest one retained snapshot points at.
    ///
    /// # Errors
    ///
    /// Returns an error when the manifest list cannot be reached or decoded.
    pub fn manifests_at(&self, snapshot: &Snapshot) -> Result<Vec<ManifestFile>> {
        if snapshot.manifest_list.is_empty() {
            return Ok(Vec::new());
        }
        let handle = self.child_at(&snapshot.manifest_list)?;
        read_manifest_list(&handle)
    }

    /// Return the retained snapshot a branch or tag names.
    ///
    /// # Errors
    ///
    /// Returns an error naming the refs the table does have when `name` is not
    /// one of them, or when the ref points at a snapshot that is not retained.
    pub fn snapshot_by_ref(&self, name: &str) -> Result<&Snapshot> {
        let reference = self
            .metadata
            .refs
            .iter()
            .find_map(|(candidate, reference)| (candidate == name).then_some(reference))
            .ok_or_else(|| {
                let known: Vec<&str> = self
                    .metadata
                    .refs
                    .iter()
                    .map(|(name, _)| name.as_str())
                    .collect();
                invalid(format_smolstr!(
                    "expected a branch or tag this table has, got {name:?}; it has [{}]",
                    known.join(", ")
                ))
            })?;
        self.metadata
            .snapshot_by_id(reference.snapshot_id)
            .ok_or_else(|| {
                invalid(format_smolstr!(
                    "expected the ref {name:?} to point at a retained snapshot, got {}",
                    reference.snapshot_id
                ))
            })
    }

    /// Plan a scan: decide which data files the metadata says have to be read.
    ///
    /// `filters` is a list of `(column, value)` pairs, the same vocabulary
    /// [`IOBase::children_where`] filters a lake with. Nothing here lists a
    /// directory: the snapshot names a manifest list, whose summaries skip
    /// whole manifests, whose entries carry the partition tuples and column
    /// statistics that skip individual files. The plan reports what it skipped,
    /// so "a filtered read touches only the files the metadata says it must" is
    /// a number a caller can check.
    ///
    /// # Errors
    ///
    /// Returns an error when a filter names a column the schema does not
    /// declare, or when a manifest that had to be read cannot be reached or
    /// decoded.
    pub fn plan(&self, filters: &[(&str, &str)]) -> Result<ScanPlan> {
        let resolved = super::scan::filters(self.schema()?, filters)?;
        self.planned(&resolved)
    }

    /// Plan a scan of one retained snapshot rather than the current one.
    ///
    /// This is the planning half of time travel: the snapshot's manifest list
    /// is walked with the same three-level pruning a current-snapshot plan
    /// uses, so a filtered read of history skips exactly what a filtered read
    /// of the present skips.
    ///
    /// # Errors
    ///
    /// Returns an error when no retained snapshot carries `snapshot_id`, when
    /// a filter names a column the snapshot's schema does not declare, or when
    /// a manifest cannot be reached or decoded.
    pub fn plan_at(&self, snapshot_id: i64, filters: &[(&str, &str)]) -> Result<ScanPlan> {
        let snapshot = self.require_snapshot(snapshot_id)?;
        let schema = self.schema_of(snapshot)?;
        let resolved = super::scan::filters(schema, filters)?;
        let manifests = self.manifests_at(snapshot)?;
        self.plan_manifests(&manifests, &resolved)
    }

    /// Read one retained snapshot's rows: time travel as an ordinary scan.
    ///
    /// The rows are read as the schema that was current when the snapshot was
    /// written, so a column added later does not appear and a column dropped
    /// later still does. `filters` and `field` mean exactly what they mean on
    /// [`Self::scan_where`].
    ///
    /// # Errors
    ///
    /// Returns an error when no retained snapshot carries `snapshot_id`, when
    /// a filter names a column that schema does not declare, or when a
    /// manifest cannot be read.
    pub fn scan_at(
        &self,
        snapshot_id: i64,
        filters: &[(&str, &str)],
        field: Option<&Field>,
    ) -> Result<BatchReader> {
        let snapshot = self.require_snapshot(snapshot_id)?;
        let stored = self.schema_of(snapshot)?.clone();
        let resolved = super::scan::filters(&stored, filters)?;
        let manifests = self.manifests_at(snapshot)?;
        let plan = self.plan_manifests(&manifests, &resolved)?;
        self.reader(plan.tasks, &stored, field, resolved)
    }

    /// Return one retained snapshot, or say which ids are retained.
    fn require_snapshot(&self, snapshot_id: i64) -> Result<&Snapshot> {
        self.metadata.snapshot_by_id(snapshot_id).ok_or_else(|| {
            let retained: Vec<String> = self
                .metadata
                .snapshots
                .iter()
                .map(|snapshot| snapshot.snapshot_id.to_string())
                .collect();
            invalid(format_smolstr!(
                "expected a retained snapshot id, got {snapshot_id}; the table retains [{}]",
                retained.join(", ")
            ))
        })
    }

    /// Return the schema one snapshot was written under, or the current one.
    fn schema_of(&self, snapshot: &Snapshot) -> Result<&Field> {
        match snapshot.schema_id {
            Some(schema_id) => self.metadata.schema_by_id(schema_id).ok_or_else(|| {
                invalid(format_smolstr!(
                    "expected the snapshot's schema {schema_id} among the table's schemas, got none"
                ))
            }),
            None => self.schema(),
        }
    }

    /// Plan a scan from filters that are already resolved.
    fn planned(&self, filters: &[Filter]) -> Result<ScanPlan> {
        let manifests = self.manifests()?;
        self.plan_manifests(&manifests, filters)
    }

    /// Plan one set of manifests under one set of resolved filters.
    fn plan_manifests(&self, manifests: &[ManifestFile], filters: &[Filter]) -> Result<ScanPlan> {
        super::scan::plan(
            manifests,
            &|spec_id| {
                self.metadata
                    .spec_by_id(spec_id)
                    .cloned()
                    .unwrap_or_else(PartitionSpec::unpartitioned)
            },
            &|location| self.child_at(location),
            filters,
        )
    }

    /// Return every live data file of the current snapshot, with its spec.
    ///
    /// # Errors
    ///
    /// Returns an error when a manifest cannot be reached or decoded. A
    /// manifest naming a file that is not there is *not* an error here: a scan
    /// reports that, because a missing file is a read failure and not a
    /// metadata failure.
    pub fn data_files(&self) -> Result<Vec<(DataFile, PartitionSpec)>> {
        Ok(self
            .plan(&[])?
            .tasks
            .into_iter()
            .map(|task| (task.entry.data_file, task.spec))
            .collect())
    }

    /// Commit a metadata-only change as the next table version.
    ///
    /// `change` receives the metadata to mutate - table properties, a new
    /// schema from [`TableMetadata::add_schema`], a snapshot ref - and the
    /// result is written as one new metadata document, exactly as a data
    /// commit writes one. An error from the change, or from the write, leaves
    /// the table's in-memory state exactly as it was: a failed commit is a
    /// commit that never happened.
    ///
    /// ```no_run
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let folder = yggdryl::local::Folder::new(std::env::temp_dir().join("t"))?;
    /// # let mut table = yggdryl::iceberg::Table::open(folder)?;
    /// table.commit_changes(|metadata| {
    ///     metadata.set_property("commit.retry.num-retries", "4")?;
    ///     Ok(())
    /// })?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns the change's own failure, or the write failure of the new
    /// document.
    pub fn commit_changes(
        &mut self,
        change: impl FnOnce(&mut TableMetadata) -> Result<()>,
    ) -> Result<()> {
        // The change runs on a copy, so a rejected change costs nothing.
        let mut updated = self.metadata.clone();
        change(&mut updated)?;
        updated.last_updated_ms = now_ms();

        let previous = std::mem::replace(&mut self.metadata, updated);
        let previous_version = self.version;
        if let Err(error) = self.commit_metadata() {
            // A failed write must leave the handle describing the table that
            // is actually there, which is still the previous version.
            self.metadata = previous;
            self.version = previous_version;
            return Err(error);
        }
        Ok(())
    }

    /// Render when each snapshot became current, oldest first.
    ///
    /// The columns are `made_current_at`, `snapshot_id`, `parent_id`, and
    /// `is_current_ancestor`, the names PyIceberg's `history` table uses.
    ///
    /// # Errors
    ///
    /// Returns an error only when the batch cannot be assembled.
    pub fn inspect_history(&self) -> Result<BatchReader> {
        super::inspect::history(&self.metadata)
    }

    /// Render every retained snapshot with its operation and summary.
    ///
    /// The columns are `committed_at`, `snapshot_id`, `parent_id`,
    /// `operation`, `manifest_list`, and the free-form `summary` map.
    ///
    /// # Errors
    ///
    /// Returns an error only when the batch cannot be assembled.
    pub fn inspect_snapshots(&self) -> Result<BatchReader> {
        super::inspect::snapshots(&self.metadata)
    }

    /// Render the live data files of the current snapshot.
    ///
    /// The columns are `file_path`, `file_format`, `spec_id`, the rendered
    /// `partition` chain, `record_count`, and `file_size_in_bytes`.
    ///
    /// # Errors
    ///
    /// Returns an error when a manifest cannot be reached or decoded.
    pub fn inspect_files(&self) -> Result<BatchReader> {
        let entries = self.data_files()?;
        super::inspect::files(&entries)
    }

    /// Read every row of the current snapshot, keeping the columns `field` names.
    ///
    /// # Errors
    ///
    /// Returns an error when a manifest cannot be read or the scan root cannot
    /// be projected.
    pub fn scan(&self, field: Option<&Field>) -> Result<BatchReader> {
        self.scan_where(&[], field)
    }

    /// Read the rows matching `filters`, keeping the columns `field` names.
    ///
    /// Each data file is read through [`IOBase::read_arrow_batch_reader`] with
    /// the scan root as its declared schema, so a projected scan skips the
    /// column chunks it does not want rather than reading and discarding them.
    /// What each file yields is then cast to the scan's own root, which is what
    /// makes a table whose schema evolved readable as one shape: a file written
    /// before a column existed contributes null for it.
    ///
    /// A partition column the data file does not store is restored from the
    /// manifest's partition tuple, typed as the schema declares it. The
    /// manifest is the authority rather than the directory name, because a null
    /// partition value is spelled `null` in a path and a path cannot say
    /// whether that is the string or the absence.
    ///
    /// A filter on a partition column is answered by [`Self::plan`] alone -
    /// every row of a file whose tuple matches holds that value - and a filter
    /// on any other column is applied to the rows the surviving files hold,
    /// because statistics bound a file rather than select a row.
    ///
    /// # Errors
    ///
    /// Returns an error when a filter names a column the schema does not
    /// declare, when a manifest cannot be read, or when the scan root cannot be
    /// projected.
    pub fn scan_where(
        &self,
        filters: &[(&str, &str)],
        field: Option<&Field>,
    ) -> Result<BatchReader> {
        let stored = self.schema()?.clone();
        let resolved = super::scan::filters(&stored, filters)?;
        let plan = self.planned(&resolved)?;
        self.reader(plan.tasks, &stored, field, resolved)
    }

    /// Build the reader over one set of planned files.
    fn reader(
        &self,
        tasks: Vec<ScanTask>,
        stored: &Field,
        field: Option<&Field>,
        filters: Vec<Filter>,
    ) -> Result<BatchReader> {
        let root = field.map_or_else(|| stored.clone(), Clone::clone);
        let read_root = super::scan::read_root(&root, stored, &filters)?;

        let mut parts = Vec::new();
        for task in tasks {
            let handle = self.child_at(&task.entry.data_file.file_path)?;
            parts.push(ScanPart {
                handle,
                partition: super::scan::partition_columns(
                    &task.spec,
                    stored,
                    &task.entry.data_file,
                )?,
                residual: task.residual,
            });
        }
        super::scan::reader(parts, root, read_root, field.cloned(), filters)
    }

    /// Append `batches` as a new snapshot, keeping everything already stored.
    ///
    /// # Errors
    ///
    /// Returns an error when the partition spec cannot place a row, when a
    /// batch cannot be cast to the table schema, or when any write fails.
    pub fn append(&mut self, batches: BatchReader) -> Result<()> {
        self.commit(batches, "append", Retained::All)?;
        Ok(())
    }

    /// Replace every row with `batches` as a new snapshot.
    ///
    /// The previous snapshot is retained and still readable; only the current
    /// pointer moves, which is what makes an overwrite reversible.
    ///
    /// # Errors
    ///
    /// Returns an error when the partition spec cannot place a row, when a
    /// batch cannot be cast to the table schema, or when any write fails.
    pub fn overwrite(&mut self, batches: BatchReader) -> Result<()> {
        self.overwrite_where(&[], batches)
    }

    /// Replace only the rows `filters` selects, keeping every other file.
    ///
    /// A file the filters exclude is carried into the new snapshot exactly as
    /// it is - the same location, the same statistics, the commit order it was
    /// written with - so overwriting one partition of a thousand rewrites one
    /// partition. A manifest the summaries excluded outright is not even
    /// rewritten: it stays in the manifest list as it was.
    ///
    /// # Errors
    ///
    /// Returns an error when a filter names a column the schema does not
    /// declare, when the partition spec cannot place a row, or when any read or
    /// write fails.
    pub fn overwrite_where(
        &mut self,
        filters: &[(&str, &str)],
        batches: BatchReader,
    ) -> Result<()> {
        let plan = self.plan(filters)?;
        self.commit(
            batches,
            "overwrite",
            Retained::Only {
                manifests: plan.skipped,
                entries: plan.excluded,
            },
        )?;
        Ok(())
    }

    /// Merge `batches` into the stored rows, matching on the `merge_by` columns.
    ///
    /// # Errors
    ///
    /// Returns the failure of the read, the join, or the commit.
    pub fn merge(&mut self, batches: BatchReader, merge_by: &[String], safe: bool) -> Result<()> {
        self.merge_where(&[], batches, merge_by, safe)
    }

    /// Merge `batches` into the rows `filters` selects, on the `merge_by` columns.
    ///
    /// This is the one place the *column statistics* decide what is read. A row
    /// can only update a file whose recorded bounds for every match-key column
    /// contain one of the incoming keys, so the files whose bounds cannot are
    /// neither read nor rewritten: they are carried into the new snapshot
    /// untouched. That makes an upsert cost the files it can actually change
    /// rather than the whole table, and it stays correct however coarse the
    /// statistics are, because a file that is not read keeps every row it had.
    ///
    /// # Errors
    ///
    /// Returns an error when `merge_by` names a column the schema does not
    /// declare, and the failure of any read, join, or write otherwise.
    pub fn merge_where(
        &mut self,
        filters: &[(&str, &str)],
        batches: BatchReader,
        merge_by: &[String],
        safe: bool,
    ) -> Result<()> {
        if merge_by.is_empty() {
            return self.overwrite_where(filters, batches);
        }
        let schema = self.schema()?.clone();

        // The incoming side is held, and this is why: the files a merge has to
        // read are the ones whose statistics say they can hold an incoming key,
        // and a key range cannot be taken from a reader that has not been read.
        // The stored side is what that buys - only the files that can actually
        // change are decoded - so the whole table is never in memory even when
        // the write is not streamed.
        let mut incoming = Vec::new();
        for batch in batches {
            let batch = schema.cast_arrow_batch(batch.map_err(Error::Arrow)?, safe)?;
            if batch.num_rows() > 0 {
                incoming.push(batch);
            }
        }
        let bounds = KeyBounds::of(&incoming, &schema, merge_by)?;

        let plan = self.plan(filters)?;
        let mut selected = Vec::new();
        let mut carried = plan.excluded;
        for task in plan.tasks {
            if bounds.may_hold(&task.entry.data_file) {
                selected.push(task);
            } else {
                carried.push(task);
            }
        }

        let stored = self.reader(selected, &schema, None, Vec::new())?;
        let arrow_schema = crate::arrow::schema_from_field(&schema)?;
        let merged = crate::io::merge::merged(
            stored,
            crate::arrow::batch_reader(arrow_schema, incoming),
            &schema,
            merge_by,
            safe,
        )?;
        self.commit(
            merged,
            "overwrite",
            Retained::Only {
                manifests: plan.skipped,
                entries: carried,
            },
        )?;
        Ok(())
    }

    /// Merge the current snapshot's undersized data files, one partition at a time.
    ///
    /// The live files are grouped by spec and partition tuple - a data file
    /// belongs to exactly one partition, so files of different partitions are
    /// never merged into one - and a group is rewritten when it holds at least
    /// two files and at least one of them is smaller than
    /// [`Self::target_file_size`]. The rewritten rows go through the same
    /// rolling writer an append uses, so a compacted partition lands in files
    /// of roughly the target size, and every file of every other group is
    /// carried into the new snapshot untouched: same location, same
    /// statistics, same commit order.
    ///
    /// The commit is one `replace` snapshot, so the pre-compaction snapshot
    /// stays retained and [`Self::scan_at`] still reads exactly the rows it
    /// always read. A table with nothing to compact is left exactly as it is:
    /// no snapshot is committed and the returned `Compaction` is all zeros.
    ///
    /// # Errors
    ///
    /// Returns an error when the target size is configured but unparseable,
    /// when a manifest cannot be read, or when any read or write of the
    /// rewrite fails.
    pub fn compact(&mut self) -> Result<Compaction> {
        let target = i64::try_from(self.target_file_size()?).unwrap_or(i64::MAX);
        let plan = self.plan(&[])?;

        // Group the live files by (spec, partition tuple), in plan order.
        let mut groups: Vec<(i32, Vec<Value>, Vec<ScanTask>)> = Vec::new();
        for task in plan.tasks {
            match groups.iter_mut().find(|(spec_id, partition, _)| {
                *spec_id == task.spec.spec_id && *partition == task.entry.data_file.partition
            }) {
                Some((_, _, tasks)) => tasks.push(task),
                None => {
                    let partition = task.entry.data_file.partition.clone();
                    groups.push((task.spec.spec_id, partition, vec![task]));
                }
            }
        }

        let mut selected: Vec<ScanTask> = Vec::new();
        let mut carried = plan.excluded;
        for (_, _, tasks) in groups {
            let undersized = tasks
                .iter()
                .any(|task| task.entry.data_file.file_size_in_bytes < target);
            if tasks.len() >= 2 && undersized {
                selected.extend(tasks);
            } else {
                carried.extend(tasks);
            }
        }

        // Nothing qualifies, so nothing is committed: a snapshot that changes
        // no file would still cost a manifest, a list, and a document.
        if selected.is_empty() {
            return Ok(Compaction::default());
        }

        let files_before = selected.len();
        let bytes_rewritten: i64 = selected
            .iter()
            .map(|task| task.entry.data_file.file_size_in_bytes)
            .sum();

        let schema = self.schema()?.clone();
        let rows = self.reader(selected, &schema, None, Vec::new())?;
        let files_after = self.commit(
            rows,
            "replace",
            Retained::Only {
                manifests: plan.skipped,
                entries: carried,
            },
        )?;
        Ok(Compaction {
            files_before,
            files_after,
            bytes_rewritten,
        })
    }

    /// Add a schema and make it current, then write a new metadata document.
    ///
    /// Returns the new schema's identifier. Data written under the previous
    /// schema stays readable: [`Self::scan`] casts every file to the scan root,
    /// so a column added here reads as null in the files that predate it.
    ///
    /// # Errors
    ///
    /// Returns an error when the schema is not a non-null struct root or the
    /// metadata document cannot be written.
    pub fn evolve_schema(&mut self, schema: Field) -> Result<i32> {
        let schema_id = self.metadata.add_schema(schema)?;
        self.metadata.current_schema_id = schema_id;
        self.metadata.last_updated_ms = now_ms();
        self.commit_metadata()?;
        Ok(schema_id)
    }

    /// Resolve one recorded location into a child of the table's folder.
    ///
    /// Everything a table names is inside it, so a location is turned back into
    /// a relative name and resolved with [`IOBase::child_by`]. That is what
    /// keeps this module free of path handling: the backend decides what a
    /// child is, and a table written on one storage system moves to another by
    /// rewriting its locations rather than its code.
    pub(super) fn child_at(&self, location: &str) -> Result<Holder> {
        let relative = relative_location(&self.metadata.location, location)?;
        self.root.child_by(&relative)
    }

    /// Write the current metadata as the next numbered document.
    fn commit_metadata(&mut self) -> Result<()> {
        // A bad in-memory state is refused before a document exists, so a
        // broken table can only be read, never written.
        self.metadata.validate()?;
        let previous = (self.version > 0)
            .then(|| self.metadata_location())
            .transpose()?;
        self.version += 1;
        if let Some(previous) = previous {
            self.metadata
                .metadata_log
                .push((self.metadata.last_updated_ms, SmolStr::new(previous)));
        }

        let document = self.metadata.to_json()?;
        let encoded = crate::json::to_vec(&document)?;
        let name = self.metadata_file_name();
        let mut handle = self.root.child_by(&format!("{METADATA_DIR}/{name}"))?;
        handle.write_all_bytes(&encoded)?;

        // The hint is how a catalog-free reader finds the current document.
        let mut hint = self
            .root
            .child_by(&format!("{METADATA_DIR}/{VERSION_HINT}"))?;
        hint.write_all_bytes(self.version.to_string().as_bytes())
    }

    /// Write the data files, the manifest, the manifest list, and the metadata.
    ///
    /// Returns how many data files the commit wrote. Each partition group's
    /// rows are rolled into files of roughly [`Self::target_file_size`] bytes,
    /// and one running index numbers every file of the commit.
    fn commit(
        &mut self,
        batches: BatchReader,
        operation: &str,
        retained: Retained,
    ) -> Result<usize> {
        let schema = self.schema()?.clone();
        let spec = self.metadata.default_spec()?.clone();
        spec.require_writable()?;
        let target = self.target_file_size()?;

        let snapshot_id = snapshot_id();
        let sequence_number = self.metadata.last_sequence_number + 1;
        let partition = spec.partition_field(&schema)?;
        let sources = spec.source_names(&schema)?;

        let mut written = Vec::new();
        for (values, group) in grouped_batches(batches, &schema, &spec, &sources, &partition)? {
            for file in rolled(group, target) {
                written.push(self.write_data_file(
                    written.len(),
                    snapshot_id,
                    &schema,
                    &spec,
                    &values,
                    file,
                )?);
            }
        }
        let files_written = written.len();

        let added_records: i64 = written.iter().map(|file| file.record_count).sum();
        let added_size: i64 = written.iter().map(|file| file.file_size_in_bytes).sum();
        let added_files = i32::try_from(written.len()).unwrap_or(i32::MAX);

        let mut manifests = match retained {
            Retained::All => self.manifests()?,
            Retained::Only { manifests, entries } => {
                let mut kept = manifests;
                kept.extend(self.carried_manifests(
                    &entries,
                    &schema,
                    snapshot_id,
                    sequence_number,
                )?);
                kept
            }
        };

        if !written.is_empty() {
            let entries: Vec<ManifestEntry> = written
                .into_iter()
                .map(|file| ManifestEntry::added(snapshot_id, file))
                .collect();
            let mut manifest = self.write_manifest_file(
                &format!("{snapshot_id}-m0.avro"),
                &schema,
                &spec,
                &entries,
                snapshot_id,
                sequence_number,
            )?;
            manifest.added_files_count = added_files;
            manifest.added_rows_count = added_records;
            manifests.push(manifest);
        }

        let list_name = format!("snap-{snapshot_id}-1-{}.avro", uuid());
        let mut list = self.root.child_by(&format!("{METADATA_DIR}/{list_name}"))?;
        write_manifest_list(
            &mut list,
            self.metadata.format_version,
            snapshot_id,
            self.metadata.current_snapshot_id,
            sequence_number,
            &manifests,
        )?;

        let total_records: i64 = manifests
            .iter()
            .map(|manifest| manifest.added_rows_count + manifest.existing_rows_count)
            .sum();
        let total_files: i32 = manifests
            .iter()
            .map(|manifest| manifest.added_files_count + manifest.existing_files_count)
            .sum();

        let snapshot = Snapshot {
            snapshot_id,
            parent_snapshot_id: self.metadata.current_snapshot_id,
            sequence_number: (self.metadata.format_version >= FormatVersion::V2)
                .then_some(sequence_number),
            timestamp_ms: now_ms(),
            manifest_list: SmolStr::new(self.location_of(METADATA_DIR, &list_name)),
            summary: vec![
                (SmolStr::new_static("operation"), SmolStr::new(operation)),
                (
                    SmolStr::new_static("added-data-files"),
                    format_smolstr!("{added_files}"),
                ),
                (
                    SmolStr::new_static("added-records"),
                    format_smolstr!("{added_records}"),
                ),
                (
                    SmolStr::new_static("added-files-size"),
                    format_smolstr!("{added_size}"),
                ),
                (
                    SmolStr::new_static("total-data-files"),
                    format_smolstr!("{total_files}"),
                ),
                (
                    SmolStr::new_static("total-records"),
                    format_smolstr!("{total_records}"),
                ),
            ],
            schema_id: Some(self.metadata.current_schema_id),
            first_row_id: (self.metadata.format_version >= FormatVersion::V3)
                .then(|| self.metadata.next_row_id.unwrap_or_default()),
            added_rows: (self.metadata.format_version >= FormatVersion::V3)
                .then_some(added_records),
        };
        if self.metadata.format_version >= FormatVersion::V3 {
            self.metadata.next_row_id =
                Some(self.metadata.next_row_id.unwrap_or_default() + added_records);
        }
        self.metadata.set_current_snapshot(snapshot);
        self.commit_metadata()?;
        Ok(files_written)
    }

    /// Write one partition's rows as a Parquet data file and describe it.
    fn write_data_file(
        &self,
        index: usize,
        snapshot_id: i64,
        schema: &Field,
        spec: &PartitionSpec,
        values: &[Value],
        batches: Vec<RecordBatch>,
    ) -> Result<DataFile> {
        let directory = spec.partition_path(values)?;
        let name = format!("{index:05}-{snapshot_id}-{}.parquet", uuid());
        let relative = if directory.is_empty() {
            format!("{DATA_DIR}/{name}")
        } else {
            format!("{DATA_DIR}/{directory}/{name}")
        };

        let mut handle = self.root.child_by(&relative)?;
        let options = handle
            .record_options()?
            .with_safe(false)
            .with_schema(schema.clone());
        let arrow_schema = crate::arrow::schema_from_field(schema)?;
        handle.write_arrow_batch_reader(
            crate::arrow::batch_reader(arrow_schema, batches),
            &options,
        )?;
        handle.flush()?;

        let statistics = crate::parquet::read_statistics(&handle)?;
        let mut file = super::statistics::data_file(schema, &statistics)?;
        file.file_path = SmolStr::new(self.location_of(DATA_DIR, &{
            if directory.is_empty() {
                name.clone()
            } else {
                format!("{directory}/{name}")
            }
        }));
        file.file_format = FileFormat::Parquet;
        file.file_size_in_bytes = i64::try_from(handle.size()).unwrap_or_default();
        file.partition = values.to_vec();
        Ok(file)
    }

    /// Write one manifest and describe it as a manifest list row.
    ///
    /// The counts a caller cares about are filled in afterwards, because what
    /// makes an entry added or existing is the commit's business rather than
    /// this write's.
    fn write_manifest_file(
        &self,
        name: &str,
        schema: &Field,
        spec: &PartitionSpec,
        entries: &[ManifestEntry],
        snapshot_id: i64,
        sequence_number: i64,
    ) -> Result<ManifestFile> {
        let mut handle = self.root.child_by(&format!("{METADATA_DIR}/{name}"))?;
        write_manifest(
            &mut handle,
            self.metadata.format_version,
            schema,
            spec,
            entries,
        )?;
        handle.flush()?;
        Ok(ManifestFile {
            manifest_path: SmolStr::new(self.location_of(METADATA_DIR, name)),
            manifest_length: i64::try_from(handle.size()).unwrap_or_default(),
            partition_spec_id: spec.spec_id,
            content: ManifestContent::Data,
            sequence_number,
            // A carried entry keeps the order it was written with, so the
            // manifest's floor is the oldest entry in it rather than this
            // commit's own number.
            min_sequence_number: entries
                .iter()
                .filter_map(|entry| entry.sequence_number)
                .min()
                .unwrap_or(sequence_number)
                .min(sequence_number),
            added_snapshot_id: snapshot_id,
            added_files_count: 0,
            existing_files_count: 0,
            deleted_files_count: 0,
            added_rows_count: 0,
            existing_rows_count: 0,
            deleted_rows_count: 0,
            partitions: summaries(spec, schema, entries)?,
            first_row_id: None,
        })
    }

    /// Rewrite the files a commit keeps as existing entries, one manifest per spec.
    ///
    /// A manifest's `partition` column has the shape of one spec, so files
    /// written under two specs cannot share a manifest however few of them there
    /// are.
    fn carried_manifests(
        &self,
        tasks: &[ScanTask],
        schema: &Field,
        snapshot_id: i64,
        sequence_number: i64,
    ) -> Result<Vec<ManifestFile>> {
        let mut grouped: Vec<(PartitionSpec, Vec<ManifestEntry>)> = Vec::new();
        for task in tasks {
            match grouped
                .iter_mut()
                .find(|(spec, _)| spec.spec_id == task.spec.spec_id)
            {
                Some((_, entries)) => entries.push(task.entry.existing()),
                None => grouped.push((task.spec.clone(), vec![task.entry.existing()])),
            }
        }

        let mut manifests = Vec::with_capacity(grouped.len());
        for (index, (spec, entries)) in grouped.into_iter().enumerate() {
            let name = format!("{snapshot_id}-m{}.avro", index + 1);
            let mut manifest = self.write_manifest_file(
                &name,
                schema,
                &spec,
                &entries,
                snapshot_id,
                sequence_number,
            )?;
            manifest.existing_files_count = i32::try_from(entries.len()).unwrap_or(i32::MAX);
            manifest.existing_rows_count = entries
                .iter()
                .map(|entry| entry.data_file.record_count)
                .sum();
            manifests.push(manifest);
        }
        Ok(manifests)
    }

    /// Build the URI of one child of a table directory.
    fn location_of(&self, directory: &str, name: &str) -> String {
        format!(
            "{}/{directory}/{name}",
            self.metadata.location.trim_end_matches('/')
        )
    }
}

/// What one [`Table::compact`] call did, in numbers a caller can assert on.
///
/// A compaction with nothing to do reports zeros, because it commits nothing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Compaction {
    /// How many live data files were read and replaced.
    pub files_before: usize,
    /// How many data files the rewrite produced in their place.
    pub files_after: usize,
    /// The recorded size of the replaced files, in bytes.
    pub bytes_rewritten: i64,
}

/// Parse one configured target file size, or say why the value is not one.
fn parsed_target(key: SmolStr, text: &str) -> Result<u64> {
    match text.parse::<u64>() {
        Ok(bytes) if bytes > 0 => Ok(bytes),
        _ => Err(Error::InvalidMetadataValue {
            key,
            reason: format_smolstr!("expected a positive byte count, got {text:?}"),
        }),
    }
}

/// Split one partition group's batches into files of roughly `target` bytes.
///
/// The estimate is the Arrow in-memory size of each batch
/// ([`RecordBatch::get_array_memory_size`]), taken *before* encoding: Parquet
/// compresses what it writes, so the files land under the target rather than
/// at it. A file closes at the first batch boundary at or past the target and
/// a batch is never split, so one batch larger than the target is one file.
fn rolled(batches: Vec<RecordBatch>, target: u64) -> Vec<Vec<RecordBatch>> {
    let mut files: Vec<Vec<RecordBatch>> = Vec::new();
    let mut current: Vec<RecordBatch> = Vec::new();
    let mut held: u64 = 0;
    for batch in batches {
        let size = u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX);
        held = held.saturating_add(size);
        current.push(batch);
        if held >= target {
            files.push(std::mem::take(&mut current));
            held = 0;
        }
    }
    if !current.is_empty() {
        files.push(current);
    }
    files
}

/// What a commit keeps of the files the current snapshot already names.
enum Retained {
    /// Every live file, left in the manifests that already list it.
    All,
    /// These manifests untouched, plus these files rewritten as existing entries.
    Only {
        /// Manifests the plan never opened, kept in the list exactly as they are.
        manifests: Vec<ManifestFile>,
        /// Files that survive the write, carried into one new manifest per spec.
        entries: Vec<ScanTask>,
    },
}

/// Summarize the partition values a manifest's entries hold, in spec order.
///
/// This is what lets the *next* plan skip the manifest without reading it, so a
/// commit pays one fold over the tuples it already has in hand.
fn summaries(
    spec: &PartitionSpec,
    schema: &Field,
    entries: &[ManifestEntry],
) -> Result<Vec<FieldSummary>> {
    if spec.is_unpartitioned() {
        return Ok(Vec::new());
    }
    let partition = spec.partition_field(schema)?;
    let mut summaries = vec![FieldSummary::default(); partition.field_len()];
    for entry in entries {
        for (index, child) in partition.fields().iter().enumerate() {
            let Some(value) = entry.data_file.partition.get(index) else {
                continue;
            };
            let Some(summary) = summaries.get_mut(index) else {
                continue;
            };
            if matches!(value, Value::Null) {
                summary.contains_null = true;
                continue;
            }
            let Some(encoded) = single_value(value, child.data_type()) else {
                continue;
            };
            fold(&mut summary.lower_bound, &encoded, child.data_type(), true);
            fold(&mut summary.upper_bound, &encoded, child.data_type(), false);
        }
    }
    Ok(summaries)
}

/// Keep the smaller or larger of a running bound and one candidate.
fn fold(current: &mut Option<Vec<u8>>, candidate: &[u8], data_type: &DataType, minimum: bool) {
    match current {
        None => *current = Some(candidate.to_vec()),
        Some(held) => {
            let ordering = compare_single(candidate, held, data_type);
            if (minimum && ordering.is_lt()) || (!minimum && ordering.is_gt()) {
                *current = Some(candidate.to_vec());
            }
        }
    }
}

/// The range of key values one write brings, per match-key column.
///
/// A file can only be changed by a merge if every match-key column's recorded
/// range overlaps the incoming one, so this is what turns a set of statistics
/// into a list of files worth reading.
struct KeyBounds {
    /// One bound per match-key column, in the order the caller named them.
    columns: Vec<KeyBound>,
}

/// One match-key column's incoming range.
struct KeyBound {
    /// The column's field identifier, which statistics are keyed by.
    id: i32,
    /// The column's datatype, which says how a bound compares.
    data_type: DataType,
    /// Whether nothing about this column can exclude a file.
    unbounded: bool,
    /// Whether an incoming key holds no value for this column.
    has_null: bool,
    /// The smallest incoming value, encoded.
    lower: Option<Vec<u8>>,
    /// The largest incoming value, encoded.
    upper: Option<Vec<u8>>,
}

impl KeyBounds {
    /// Measure the incoming rows' range for every match-key column.
    fn of(batches: &[RecordBatch], schema: &Field, merge_by: &[String]) -> Result<Self> {
        let mut columns = Vec::with_capacity(merge_by.len());
        for name in merge_by {
            let field = schema.get_field_by_name(name).ok_or_else(|| {
                let stored = schema
                    .fields()
                    .iter()
                    .map(|field| field.name())
                    .collect::<Vec<_>>()
                    .join(", ");
                invalid(crate::text::expected_got(
                    format_args!(
                        "a merge_by column the table schema declares, got {name:?}; it has"
                    ),
                    crate::text::elide_display(&stored),
                ))
            })?;
            let id = field.parquet_field_id()?;
            let mut bound = KeyBound {
                id: id.unwrap_or_default(),
                data_type: field.data_type().clone(),
                unbounded: id.is_none() || !is_portable(field.data_type()),
                has_null: false,
                lower: None,
                upper: None,
            };
            for batch in batches {
                let Some(column) = batch.column_by_name(name) else {
                    continue;
                };
                bound.has_null = bound.has_null || column.null_count() > 0;
                if bound.unbounded {
                    continue;
                }
                if let Some(encoded) = extreme(column, field, false)? {
                    fold(&mut bound.lower, &encoded, &bound.data_type, true);
                }
                if let Some(encoded) = extreme(column, field, true)? {
                    fold(&mut bound.upper, &encoded, &bound.data_type, false);
                }
            }
            columns.push(bound);
        }
        Ok(Self { columns })
    }

    /// Return whether a data file can hold a row one of these keys matches.
    fn may_hold(&self, file: &DataFile) -> bool {
        self.columns.iter().all(|column| column.may_hold(file))
    }
}

impl KeyBound {
    /// Return whether one file's statistics leave room for this column's keys.
    fn may_hold(&self, file: &DataFile) -> bool {
        if self.unbounded {
            return true;
        }
        let nulls = file
            .null_value_counts
            .iter()
            .find_map(|(id, count)| (*id == self.id).then_some(*count));
        // A file with no recorded null count may still hold one, so only a
        // recorded zero rules a null key out.
        if self.has_null && nulls != Some(0) {
            return true;
        }
        let (Some(lower), Some(upper)) = (self.lower.as_deref(), self.upper.as_deref()) else {
            // Nothing but nulls arrived for this column, and the null case above
            // already decided what that can match.
            return false;
        };
        let file_lower = file
            .lower_bounds
            .iter()
            .find_map(|(id, bytes)| (*id == self.id).then_some(bytes.as_slice()));
        let file_upper = file
            .upper_bounds
            .iter()
            .find_map(|(id, bytes)| (*id == self.id).then_some(bytes.as_slice()));
        let (Some(file_lower), Some(file_upper)) = (file_lower, file_upper) else {
            // A file that records no range for the key has to be read.
            return true;
        };
        !(compare_single(upper, file_lower, &self.data_type).is_lt()
            || compare_single(lower, file_upper, &self.data_type).is_gt())
    }
}

/// Encode the smallest or largest value one column holds.
///
/// The extreme is found by a bounded sort rather than a scan of decoded values:
/// one index is all a bound needs, and asking Arrow for it keeps the work in the
/// kernel instead of in a per-row conversion.
fn extreme(column: &ArrayRef, field: &Field, descending: bool) -> Result<Option<Vec<u8>>> {
    if column.null_count() == column.len() {
        return Ok(None);
    }
    let options = SortOptions {
        descending,
        // Nulls last, so the first index is the extreme value rather than an
        // absent one, whichever direction the sort runs in.
        nulls_first: false,
    };
    let indices = arrow_ord::sort::sort_to_indices(column.as_ref(), Some(options), Some(1))
        .map_err(Error::Arrow)?;
    let Some(row) = indices.values().first().copied() else {
        return Ok(None);
    };
    let slice = column.slice(usize::try_from(row).unwrap_or_default(), 1);
    let scalar = crate::arrow::ArrowScalar::from_parts(field.clone().with_nullable(true), slice)
        .and_then(|scalar| scalar.to_value())
        .map_err(|error| invalid(format_smolstr!("{error}")))?;
    Ok(single_value(&scalar, field.data_type()))
}

/// Split every incoming batch into one group per partition tuple.
///
/// A data file belongs to exactly one partition, so a partitioned write has to
/// group its rows before it can write anything; an unpartitioned one does not
/// and passes straight through as a single group.
fn grouped_batches(
    batches: BatchReader,
    schema: &Field,
    spec: &PartitionSpec,
    sources: &[SmolStr],
    partition: &Field,
) -> Result<Vec<(Vec<Value>, Vec<RecordBatch>)>> {
    let mut groups: Vec<(Vec<Value>, Vec<RecordBatch>)> = Vec::new();
    let mut index: HashMap<String, usize> = HashMap::new();

    for batch in batches {
        let batch = schema.cast_arrow_batch(batch.map_err(Error::Arrow)?, false)?;
        if batch.num_rows() == 0 {
            continue;
        }
        if spec.is_unpartitioned() {
            match groups.first_mut() {
                Some(group) => group.1.push(batch),
                None => groups.push((Vec::new(), vec![batch])),
            }
            continue;
        }

        for (key, rows) in row_groups(&batch, sources)? {
            let position = match index.get(&key) {
                Some(position) => *position,
                None => {
                    let first = *rows.first().unwrap_or(&0);
                    let values = tuple_at(&batch, sources, partition, first)?;
                    groups.push((values, Vec::new()));
                    index.insert(key, groups.len() - 1);
                    groups.len() - 1
                }
            };
            let indices = UInt32Array::from(rows);
            let taken = arrow_select::take::take_record_batch(&batch, &indices)?;
            groups[position].1.push(taken);
        }
    }
    Ok(groups)
}

/// Group a batch's row indices by the text of their partition source values.
fn row_groups(batch: &RecordBatch, sources: &[SmolStr]) -> Result<Vec<(String, Vec<u32>)>> {
    let mut formatters = Vec::with_capacity(sources.len());
    for source in sources {
        let column = batch.column_by_name(source).ok_or_else(|| {
            invalid(format_smolstr!(
                "expected a partition source column {source:?} in the batch, got none"
            ))
        })?;
        formatters.push(arrow_cast::display::ArrayFormatter::try_new(
            column.as_ref(),
            &arrow_cast::display::FormatOptions::default(),
        )?);
    }

    let mut order: Vec<(String, Vec<u32>)> = Vec::new();
    let mut seen: HashMap<String, usize> = HashMap::new();
    for row in 0..batch.num_rows() {
        let mut key = String::new();
        for (offset, formatter) in formatters.iter().enumerate() {
            if offset > 0 {
                key.push('\u{1}');
            }
            // A null is not the text a formatter prints for it, so it gets a
            // marker no formatted value can collide with.
            let column = batch.column(
                batch
                    .schema()
                    .index_of(&sources[offset])
                    .map_err(Error::Arrow)?,
            );
            if column.is_null(row) {
                key.push('\u{0}');
            } else {
                key.push_str(&formatter.value(row).to_string());
            }
        }
        match seen.get(&key) {
            Some(position) => order[*position]
                .1
                .push(u32::try_from(row).unwrap_or_default()),
            None => {
                seen.insert(key.clone(), order.len());
                order.push((key, vec![u32::try_from(row).unwrap_or_default()]));
            }
        }
    }
    Ok(order)
}

/// Read one row's partition tuple out of a batch.
fn tuple_at(
    batch: &RecordBatch,
    sources: &[SmolStr],
    partition: &Field,
    row: u32,
) -> Result<Vec<Value>> {
    let mut values = Vec::with_capacity(sources.len());
    for (source, field) in sources.iter().zip(partition.fields()) {
        let column = batch.column_by_name(source).ok_or_else(|| {
            invalid(format_smolstr!(
                "expected a partition source column {source:?} in the batch, got none"
            ))
        })?;
        let slice = column.slice(row as usize, 1);
        let scalar = crate::arrow::ArrowScalar::from_parts(field.clone(), slice)
            .map_err(|error| invalid(format_smolstr!("{error}")))?;
        values.push(
            scalar
                .to_value()
                .map_err(|error| invalid(format_smolstr!("{error}")))?,
        );
    }
    Ok(values)
}

/// Return the metadata document with the highest version, and its number.
///
/// A folder that holds none is `None` rather than an error, because that is the
/// question "is this a table" and the answer "no" is not a failure.
fn find_metadata(metadata_dir: &Holder) -> Result<Option<(u32, Value)>> {
    // A folder that is not a table has no metadata directory at all, and the
    // laziness contract makes that a handle that simply is not a container.
    if !metadata_dir.is_container() {
        return Ok(None);
    }

    let hint = metadata_dir.child_by(VERSION_HINT)?;
    if hint.size() > 0 {
        let text = String::from_utf8_lossy(&hint.read_all()?).trim().to_owned();
        if let Ok(version) = text.parse::<u32>() {
            let document = metadata_dir.child_by(&format!("v{version}.metadata.json"))?;
            if document.size() > 0 {
                return Ok(Some((
                    version,
                    crate::json::from_slice(&document.read_all()?)?,
                )));
            }
        }
    }

    // No usable hint: take the highest-numbered document that is actually there.
    let mut best: Option<(u32, Holder)> = None;
    for entry in metadata_dir.ls(false, false)? {
        let Some(name) = entry
            .url()
            .and_then(|url| url.file_name().map(ToOwned::to_owned))
        else {
            continue;
        };
        let Some(stem) = name.strip_suffix(".metadata.json") else {
            continue;
        };
        // Both `v3` and Iceberg's own `00003-<uuid>` numbering start with digits.
        let digits: String = stem
            .trim_start_matches('v')
            .chars()
            .take_while(char::is_ascii_digit)
            .collect();
        let Ok(version) = digits.parse::<u32>() else {
            continue;
        };
        if best.as_ref().is_none_or(|(highest, _)| version > *highest) {
            best = Some((version, entry));
        }
    }

    let Some((version, document)) = best else {
        return Ok(None);
    };
    Ok(Some((
        version,
        crate::json::from_slice(&document.read_all()?)?,
    )))
}

/// Report a folder that holds no Iceberg metadata document.
fn missing_metadata(metadata_dir: &Holder) -> Error {
    invalid(format_smolstr!(
        "expected an Iceberg metadata document under {}, got none",
        metadata_dir
            .url()
            .map_or_else(|| "an unlocated folder".to_owned(), ToString::to_string)
    ))
}

/// Turn one absolute location into a name relative to the table's folder.
fn relative_location(base: &str, location: &str) -> Result<String> {
    // Separators are normalized because an implementation that wrote the table
    // on Windows may have spelled its own location with backslashes.
    let normalized_base = base.replace('\\', "/");
    let normalized_base = normalized_base.trim_end_matches('/');
    let normalized = location.replace('\\', "/");
    if let Some(rest) = normalized.strip_prefix(normalized_base) {
        return Ok(rest.trim_start_matches('/').to_owned());
    }
    // A table moved after it was written names its own old location; falling
    // back to the last `data/` or `metadata/` segment keeps it readable.
    for directory in [DATA_DIR, METADATA_DIR] {
        if let Some(position) = normalized.rfind(&format!("/{directory}/")) {
            return Ok(normalized[position + 1..].to_owned());
        }
    }
    Err(invalid(format_smolstr!(
        "expected a location inside the table at {normalized_base:?}, got {location:?}"
    )))
}

/// Produce a positive random snapshot identifier.
fn snapshot_id() -> i64 {
    use std::hash::{BuildHasher, Hasher};

    let state = std::collections::hash_map::RandomState::new();
    let mut hasher = state.build_hasher();
    hasher.write_i64(now_ms());
    // Iceberg identifiers are signed but conventionally positive.
    (hasher.finish() >> 1) as i64
}

/// Report a malformed or unreachable Iceberg table.
fn invalid(reason: SmolStr) -> Error {
    Error::Codec {
        format: "iceberg",
        position: 0,
        reason,
    }
}
