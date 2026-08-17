//! Planning a read from the table's metadata, then performing it.
//!
//! **A scan never lists a directory.** The current snapshot names a manifest
//! list, the manifest list names manifests, and a manifest names data files with
//! their partition tuples and their column statistics. That chain is what
//! decides which files are opened, so a table whose `data/` folder also holds a
//! stray file, an orphan left by a failed commit, or a file an overwrite
//! replaced reads as exactly the rows its snapshot says it has.
//!
//! Each level of the chain also prunes:
//!
//! - a **manifest list row** carries one [`FieldSummary`] per partition field,
//!   so a manifest whose summary excludes a value is skipped without being read;
//! - a **manifest entry** carries the file's partition tuple, so a file outside
//!   the addressed partition is skipped without being opened;
//! - a **data file** carries per-column bounds and null counts, so a file whose
//!   statistics cannot hold the value is skipped without being opened.
//!
//! A filter is a column name and a value as text, the same vocabulary
//! [`IOBase::children_where`](crate::io::IOBase::children_where) filters a lake
//! with. Against a partition column it is compared to the text the layout
//! spells, and `null` therefore names the absence exactly as a directory name
//! does; against any other column it is compared to the value a cast from that
//! text produces, and the rows the file does hold are filtered after the file is
//! read, because file statistics bound a file and do not select a row.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, Scalar, StringArray, UInt32Array};
use arrow_schema::{Schema as ArrowSchema, SchemaRef};
use smol_str::{SmolStr, format_smolstr};

use super::manifest::{DataFile, EntryStatus, ManifestContent, ManifestEntry, ManifestFile};
use super::partition::{PartitionSpec, Transform};
use super::value::{NULL_TEXT, compare_single, single_value};
use crate::arrow::BatchReader;
use crate::field::cast::ArrowCast;
use crate::generic::Holder;
use crate::{DataType, Error, Field, Result, Value};

/// One data file a scan reads, with everything a rewrite of it would need.
#[derive(Clone, Debug)]
pub struct ScanTask {
    /// The manifest entry, with the numbers its manifest supplied filled in.
    pub entry: ManifestEntry,
    /// The spec the entry's partition tuple is written against.
    pub spec: PartitionSpec,
    /// The filters this file's partition tuple did not already settle.
    pub residual: Vec<usize>,
}

impl ScanTask {
    /// Borrow the data file this task reads.
    pub const fn data_file(&self) -> &DataFile {
        &self.entry.data_file
    }
}

/// The files one scan reads, and what the metadata let it leave alone.
///
/// The three lists are also what a *write* needs: what it rewrites is `tasks`,
/// and what it carries into the new snapshot untouched is `excluded` plus the
/// whole of `skipped` - a manifest nobody opened does not have to be rewritten
/// to stay true.
#[derive(Clone, Debug, Default)]
pub struct ScanPlan {
    /// The data files the scan will open, in manifest order.
    pub tasks: Vec<ScanTask>,
    /// Live files a read manifest listed that the filters excluded.
    pub excluded: Vec<ScanTask>,
    /// Manifests excluded on their manifest-list summary alone, never opened.
    pub skipped: Vec<ManifestFile>,
    /// Manifests that had to be read because their summaries allowed a match.
    pub manifests_read: usize,
}

impl ScanPlan {
    /// Return the rows the planned files hold, as the manifests counted them.
    pub fn record_count(&self) -> i64 {
        self.tasks
            .iter()
            .map(|task| task.data_file().record_count)
            .sum()
    }

    /// Return how many live data files the metadata let the scan skip.
    pub fn files_skipped(&self) -> usize {
        self.excluded.len()
    }

    /// Return how many manifests the metadata let the scan skip.
    pub fn manifests_skipped(&self) -> usize {
        self.skipped.len()
    }
}

/// One resolved filter: a schema column and the value it must hold.
#[derive(Clone, Debug)]
pub(super) struct Filter {
    /// The schema column the filter names.
    field: Field,
    /// The column's field identifier, which is what statistics are keyed by.
    id: i32,
    /// The value exactly as the caller spelled it.
    text: SmolStr,
    /// The value the text names, read through the column's own datatype.
    value: Value,
    /// Whether the text names the absence of a value.
    is_null: bool,
    /// The value encoded as a single value, when the type has that encoding.
    encoded: Option<Vec<u8>>,
    /// The value as a one-element array, for comparing it against rows.
    scalar: ArrayRef,
}

impl Filter {
    /// Resolve one `(column, value)` pair against the table schema.
    ///
    /// # Errors
    ///
    /// Returns an error naming the columns the schema does have when the filter
    /// names one it does not, or when the column carries no field identifier.
    pub(super) fn resolve(schema: &Field, column: &str, text: &str) -> Result<Self> {
        let field = schema.get_field_by_name(column).cloned().ok_or_else(|| {
            let columns = schema
                .fields()
                .iter()
                .map(|field| field.name())
                .collect::<Vec<_>>()
                .join(", ");
            invalid(crate::text::expected_got(
                format_args!("a filter column the table schema declares, got {column:?}; it has"),
                crate::text::elide_display(&columns),
            ))
        })?;
        let id = field.id()?.ok_or_else(|| {
            invalid(format_smolstr!(
                "expected a PARQUET:field_id on the filter column {column:?}, got none"
            ))
        })?;

        // The value is parsed the way a partition directory's text is parsed
        // everywhere else in the crate: one text array, cast to the column's own
        // type. A value the type cannot read becomes null, which is what makes
        // it match nothing rather than fail a scan.
        let nullable = field.clone().with_nullable(true);
        let text_array: ArrayRef = Arc::new(StringArray::from(vec![text]));
        let scalar = arrow_cast::cast(&text_array, nullable.to_arrow()?.data_type())
            .map_err(Error::Arrow)?;
        let value = crate::arrow::ArrowScalar::from_parts(nullable, Arc::clone(&scalar))
            .and_then(|scalar| scalar.to_value())
            .map_err(|error| invalid(format_smolstr!("{error}")))?;

        Ok(Self {
            encoded: single_value(&value, field.data_type()),
            id,
            is_null: text == NULL_TEXT,
            text: SmolStr::new(text),
            value,
            field,
            scalar,
        })
    }

    /// Borrow the column name this filter reads.
    fn column(&self) -> &str {
        self.field.name()
    }
}

/// Resolve every `(column, value)` pair against the table schema.
///
/// # Errors
///
/// Returns the first unresolvable filter's failure.
pub(super) fn filters(schema: &Field, pairs: &[(&str, &str)]) -> Result<Vec<Filter>> {
    pairs
        .iter()
        .map(|(column, text)| Filter::resolve(schema, column, text))
        .collect()
}

/// Return the position of the partition field one filter is settled by.
///
/// Only [`Transform::Identity`] settles a filter: it is the one transform whose
/// partition value *is* the column value, so every row of a file whose tuple
/// matches holds the filter's value. A bucket, a truncation, or a calendar
/// transform stores something else, so a filter on its source column falls
/// through to the file's statistics and then to the rows themselves.
fn partition_index(spec: &PartitionSpec, filter: &Filter) -> Option<usize> {
    spec.fields
        .iter()
        .position(|field| field.transform == Transform::Identity && field.source_id == filter.id)
}

/// Return whether a manifest's summaries leave room for every filter.
///
/// This is the cheapest level: the summary is a row of the manifest list, so a
/// manifest excluded here is never opened at all.
fn manifest_matches(manifest: &ManifestFile, spec: &PartitionSpec, filters: &[Filter]) -> bool {
    for filter in filters {
        let Some(position) = partition_index(spec, filter) else {
            continue;
        };
        let Some(summary) = manifest.partitions.get(position) else {
            // A manifest list written without summaries says nothing, so
            // nothing may be skipped on it.
            continue;
        };
        let data_type = partition_type(spec, position, filter);
        if filter.is_null {
            if !summary.contains_null {
                return false;
            }
            continue;
        }
        let Some(encoded) = filter.encoded.as_deref() else {
            continue;
        };
        if let Some(lower) = summary.lower_bound.as_deref() {
            if compare_single(encoded, lower, &data_type).is_lt() {
                return false;
            }
        }
        if let Some(upper) = summary.upper_bound.as_deref() {
            if compare_single(encoded, upper, &data_type).is_gt() {
                return false;
            }
        }
    }
    true
}

/// Return the datatype one partition field's values are encoded with.
///
/// An identity transform keeps the source column's type, which is the only
/// transform a summary is consulted for; anything else falls back to the
/// filter's own column so the comparison at least stays self-consistent.
fn partition_type(spec: &PartitionSpec, position: usize, filter: &Filter) -> DataType {
    spec.fields
        .get(position)
        .and_then(|field| field.transform.result_type(filter.field.data_type()).ok())
        .unwrap_or_else(|| filter.field.data_type().clone())
}

/// Return whether one data file can hold a row matching every filter.
///
/// The filters the partition tuple settles are answered exactly; the rest are
/// answered from the file's own statistics, which bound the file rather than
/// select a row - so the ones that survive are handed back as `residual` for the
/// read to apply to the rows.
fn file_matches(file: &DataFile, spec: &PartitionSpec, filters: &[Filter]) -> Option<Vec<usize>> {
    let mut residual = Vec::new();
    for (position, filter) in filters.iter().enumerate() {
        if let Some(index) = partition_index(spec, filter) {
            let value = file.partition.get(index).cloned().unwrap_or(Value::Null);
            // The manifest holds the value, so the values are what agree or
            // disagree. The text is compared too, because a filter spelled the
            // way the directory spells it is the other way a caller addresses a
            // partition, and a table written before this renderer settled can
            // still hold a value whose text is the only thing that matches.
            if value != filter.value && super::value::scalar_text(&value) != filter.text {
                return None;
            }
            continue;
        }
        if !statistics_allow(file, filter) {
            return None;
        }
        residual.push(position);
    }
    Some(residual)
}

/// Return whether a file's column statistics leave room for one filter.
fn statistics_allow(file: &DataFile, filter: &Filter) -> bool {
    let nulls = lookup(&file.null_value_counts, filter.id);
    if filter.is_null {
        // A column with a recorded null count of zero holds no null anywhere in
        // the file. Without that count the file has to be read.
        return nulls != Some(0);
    }
    if nulls == Some(file.record_count) && file.record_count > 0 {
        return false;
    }
    let Some(encoded) = filter.encoded.as_deref() else {
        return true;
    };
    let data_type = filter.field.data_type();
    if let Some(lower) = bound(&file.lower_bounds, filter.id) {
        if compare_single(encoded, lower, data_type).is_lt() {
            return false;
        }
    }
    if let Some(upper) = bound(&file.upper_bounds, filter.id) {
        if compare_single(encoded, upper, data_type).is_gt() {
            return false;
        }
    }
    true
}

/// Read one integer statistic by field id.
fn lookup(counts: &[(i32, i64)], id: i32) -> Option<i64> {
    counts
        .iter()
        .find_map(|(key, count)| (*key == id).then_some(*count))
}

/// Read one encoded bound by field id.
fn bound(bounds: &[(i32, Vec<u8>)], id: i32) -> Option<&[u8]> {
    bounds
        .iter()
        .find_map(|(key, bytes)| (*key == id).then_some(bytes.as_slice()))
}

/// Plan a scan of `manifests`, opening only what the summaries allow.
///
/// `manifest_at` resolves a recorded manifest location into a handle, which is
/// the table's business rather than the planner's - everything here works
/// through what that closure returns.
///
/// # Errors
///
/// Returns an error when a manifest that had to be read cannot be reached or
/// decoded.
pub(super) fn plan(
    manifests: &[ManifestFile],
    spec_of: &dyn Fn(i32) -> PartitionSpec,
    manifest_at: &dyn Fn(&str) -> Result<Holder>,
    filters: &[Filter],
) -> Result<ScanPlan> {
    let mut plan = ScanPlan::default();
    for manifest in manifests {
        if manifest.content != ManifestContent::Data {
            continue;
        }
        let spec = spec_of(manifest.partition_spec_id);
        if !manifest_matches(manifest, &spec, filters) {
            plan.skipped.push(manifest.clone());
            continue;
        }
        plan.manifests_read += 1;

        let handle = manifest_at(&manifest.manifest_path)?;
        for mut entry in super::manifest::read_manifest(&handle)? {
            if entry.status == EntryStatus::Deleted {
                continue;
            }
            entry.inherit(manifest);
            // A file with no rows is neither read nor carried: reading it would
            // cost a footer for an empty batch, and keeping it would carry a
            // name that holds nothing.
            if entry.data_file.record_count == 0 {
                continue;
            }
            let matched = file_matches(&entry.data_file, &spec, filters);
            let task = ScanTask {
                entry,
                spec: spec.clone(),
                residual: matched.clone().unwrap_or_default(),
            };
            if matched.is_some() {
                plan.tasks.push(task);
            } else {
                plan.excluded.push(task);
            }
        }
    }
    Ok(plan)
}

/// One data file a scan still has to open.
pub(super) struct ScanPart {
    /// The handle addressing the data file.
    pub(super) handle: Holder,
    /// Partition columns to restore, when the file does not store them.
    pub(super) partition: Vec<(Field, Value)>,
    /// The filters this file's rows still have to be tested against.
    pub(super) residual: Vec<usize>,
}

/// The data file a scan is currently reading, and what it still has to apply.
struct Open {
    /// The file's own reader.
    reader: BatchReader,
    /// Partition columns to restore, when the file does not store them.
    partition: Vec<(Field, Value)>,
    /// The filters this file's rows still have to be tested against.
    residual: Vec<usize>,
}

/// A reader over every data file one plan selected.
struct Scan {
    /// The files not yet opened.
    parts: std::vec::IntoIter<ScanPart>,
    /// The file currently being read, with what it still has to apply.
    current: Option<Open>,
    /// The root each file is read and filtered as.
    read_root: Field,
    /// The root every batch is finally cast to.
    root: Field,
    /// The Arrow projection of that root.
    schema: SchemaRef,
    /// The column pushdown handed to each file, when the caller gave one.
    target: Option<Field>,
    /// Whether the read root holds columns the scan root does not.
    project: bool,
    /// The filters, shared by every part that still has one to apply.
    filters: Arc<Vec<Filter>>,
}

/// Build the reader over one set of planned files.
///
/// `root` is what the reader reports and every batch is cast to; the read root
/// is that plus whatever the filters need, so a column a filter tests is read
/// and then dropped rather than left out of the pushdown.
///
/// # Errors
///
/// Returns an error when either root cannot be projected into Arrow.
pub(super) fn reader(
    parts: Vec<ScanPart>,
    root: Field,
    read_root: Field,
    target: Option<Field>,
    filters: Vec<Filter>,
) -> Result<BatchReader> {
    Ok(Box::new(Scan {
        project: read_root.field_len() != root.field_len(),
        schema: crate::arrow::schema_from_field(&root)?,
        parts: parts.into_iter(),
        current: None,
        read_root,
        root,
        target,
        filters: Arc::new(filters),
    }))
}

impl Iterator for Scan {
    type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(open) = self.current.as_mut() {
                match open.reader.next() {
                    Some(Ok(batch)) => {
                        let residual = &open.residual;
                        let produced = restore_partitions(&batch, &open.partition)
                            .and_then(|batch| Ok(self.read_root.cast_arrow_batch(batch, false)?))
                            .and_then(|batch| apply_filters(batch, &self.filters, residual))
                            .and_then(|batch| {
                                if self.project {
                                    return Ok(self.root.cast_arrow_batch(batch, false)?);
                                }
                                Ok(batch)
                            });
                        return Some(produced.map_err(scan_error));
                    }
                    Some(Err(error)) => return Some(Err(error)),
                    None => self.current = None,
                }
            }
            let part = self.parts.next()?;
            let options = match self.file_options(&part) {
                Ok(options) => options,
                Err(error) => return Some(Err(scan_error(error))),
            };
            match crate::io::IOBase::read_arrow_batch_reader(&part.handle, &options) {
                Ok(reader) => {
                    self.current = Some(Open {
                        reader,
                        partition: part.partition,
                        residual: part.residual,
                    });
                }
                Err(error) => return Some(Err(scan_error(error))),
            }
        }
    }
}

impl Scan {
    /// Resolve the options one data file is read under.
    ///
    /// The read root is handed down as the file's declared schema, which is what
    /// makes it the encoding's own projection mask. The partition columns are
    /// removed from it first: the file need not store them, so asking for them
    /// here would fill them with nulls and hide the manifest values
    /// [`restore_partitions`] is about to put back.
    fn file_options(&self, part: &ScanPart) -> Result<crate::generic::RecordOptions> {
        use crate::generic::IORecordOptions;
        use crate::io::IOBase;

        let options = part.handle.record_options()?;
        if self.target.is_none() {
            // Nothing was asked for, so nothing is pushed down and the file's
            // own columns come back as they are.
            return Ok(options);
        }
        let columns: Vec<&str> = part
            .partition
            .iter()
            .map(|(field, _)| field.name())
            .collect();
        match self.read_root.without_fields(&columns) {
            Ok(stored) if stored.field_len() > 0 => Ok(options.with_schema(stored)),
            // A read root that is nothing but partition columns leaves the file
            // read unprojected; there is no column left to ask it for.
            _ => Ok(options),
        }
    }
}

impl arrow_array::RecordBatchReader for Scan {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Keep only the rows every residual filter accepts.
///
/// The filters are applied one after another rather than combined, because two
/// masks applied in turn select exactly the rows their conjunction would and
/// each one narrows what the next has to test.
fn apply_filters(
    batch: RecordBatch,
    filters: &[Filter],
    residual: &[usize],
) -> Result<RecordBatch> {
    let mut batch = batch;
    for position in residual {
        let Some(filter) = filters.get(*position) else {
            continue;
        };
        let Ok(index) = batch.schema().index_of(filter.column()) else {
            continue;
        };
        let column = batch.column(index);
        let mask = if filter.is_null {
            BooleanArray::from(
                (0..column.len())
                    .map(|row| column.is_null(row))
                    .collect::<Vec<bool>>(),
            )
        } else {
            // A null never equals a value, so the comparison's own null mask is
            // already the answer for a row that holds nothing.
            arrow_ord::cmp::eq(&column, &Scalar::new(Arc::clone(&filter.scalar)))
                .map_err(Error::Arrow)?
        };
        batch = arrow_select::filter::filter_record_batch(&batch, &mask).map_err(Error::Arrow)?;
    }
    Ok(batch)
}

/// Report a scan failure through the reader's own error type.
pub(super) fn scan_error(error: Error) -> arrow_schema::ArrowError {
    arrow_schema::ArrowError::ExternalError(Box::new(error))
}

/// Add the partition columns a data file left out, typed as declared.
fn restore_partitions(batch: &RecordBatch, partition: &[(Field, Value)]) -> Result<RecordBatch> {
    let missing: Vec<&(Field, Value)> = partition
        .iter()
        .filter(|(field, _)| batch.schema().index_of(field.name()).is_err())
        .collect();
    if missing.is_empty() {
        return Ok(batch.clone());
    }

    let mut fields: Vec<Arc<arrow_schema::Field>> =
        batch.schema().fields().iter().map(Arc::clone).collect();
    let mut columns = batch.columns().to_vec();
    for (field, value) in missing {
        let scalar = crate::arrow::ArrowScalar::from_value(field.clone(), value.clone())
            .map_err(|error| invalid(format_smolstr!("{error}")))?;
        columns.push(repeat(&scalar.to_array(), batch.num_rows())?);
        fields.push(field.to_arrow_ref()?);
    }
    let schema =
        Arc::new(ArrowSchema::new(fields).with_metadata(batch.schema().metadata().clone()));
    RecordBatch::try_new(schema, columns).map_err(Error::Arrow)
}

/// Repeat one value into a column of `rows` rows.
fn repeat(value: &ArrayRef, rows: usize) -> Result<ArrayRef> {
    let indices = UInt32Array::from(vec![0_u32; rows]);
    arrow_select::take::take(value.as_ref(), &indices, None).map_err(Error::Arrow)
}

/// Pair each partition column's Field with the value the manifest recorded.
pub(super) fn partition_columns(
    spec: &PartitionSpec,
    schema: &Field,
    file: &DataFile,
) -> Result<Vec<(Field, Value)>> {
    let partition = spec.partition_field(schema)?;
    Ok(partition
        .fields()
        .iter()
        .zip(file.partition.iter())
        .map(|(field, value)| (field.clone(), value.clone()))
        .collect())
}

/// Return the root a file is read as: the scan's own root plus what it filters.
///
/// A filter may name a column the caller never asked for, and the rows still
/// have to be tested against it, so the column is read and then dropped by the
/// final cast rather than left out of the pushdown.
pub(super) fn read_root(root: &Field, schema: &Field, filters: &[Filter]) -> Result<Field> {
    let mut children: Vec<Field> = root.fields().to_vec();
    for filter in filters {
        if children.iter().any(|child| child.name() == filter.column()) {
            continue;
        }
        let Some(column) = schema.get_field_by_name(filter.column()) else {
            continue;
        };
        children.push(column.clone());
    }
    if children.len() == root.field_len() {
        return Ok(root.clone());
    }
    Field::from_parts(
        root.name(),
        DataType::from_fields(children)?,
        root.is_nullable(),
        root.metadata_iter(),
    )
}

/// Report a scan a table's metadata cannot describe.
fn invalid(reason: SmolStr) -> Error {
    Error::Codec {
        format: "iceberg",
        position: 0,
        reason,
    }
}
