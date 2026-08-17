//! An Apache Iceberg table, over the same `IOBase` handle Python already has.
//!
//! A table is a folder and nothing else, so this binding takes the handle a
//! caller already built with [`crate::io::PyIOBase`] and hands it to the core
//! [`Table`]. Rows cross the boundary the way they do everywhere else here -
//! as a `pyarrow.RecordBatchReader` over the Arrow C Stream interface - so a
//! scan is lazy on both sides and a commit copies nothing.
//!
//! The metadata values below (a snapshot, a manifest, a data file, a partition
//! spec) are read-only views of the core structs. They exist so a caller can
//! ask what a commit produced without opening the Avro files by hand; none of
//! them can be constructed from Python, because only a commit writes one.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple, PyType};

use yggdryl::generic::Holder;
use yggdryl::iceberg::{
    DataFile, FormatVersion, ManifestContent, ManifestFile, PartitionField, PartitionSpec,
    Snapshot, Table, assign_field_ids, schema_from_json, schema_to_json,
};
use yggdryl::io::IOBase as _;
use yggdryl::{Field as CoreField, Value};

use crate::field::PyField;
use crate::io::PyIOBase;
use crate::record::{batch_reader_from_value, batch_reader_to_pyarrow, core_root_field_from_value};
use crate::value_error;

/// The root name given to a schema that arrives as a bare Arrow schema.
const SCHEMA_ROOT_NAME: &str = "row";

/// Number every field of a schema, depth first, and return the numbered copy.
///
/// Iceberg resolves a column by identifier rather than by position, so a schema
/// reaches [`PyTable::create`] already numbered. This is the core's numbering,
/// exposed because a caller building a schema from Python annotations or from a
/// `PyArrow` schema has no identifiers to start from.
///
/// # Errors
///
/// Raises `ValueError` when the value is not a non-null struct root.
#[pyfunction(name = "assign_field_ids")]
#[pyo3(signature = (schema, start = 1))]
pub(crate) fn iceberg_assign_field_ids(schema: &Bound<'_, PyAny>, start: i32) -> PyResult<PyField> {
    let mut root = core_root_field_from_value(schema, SCHEMA_ROOT_NAME)?;
    assign_field_ids(&mut root, start).map_err(value_error)?;
    Ok(PyField::from_inner(root))
}

/// Read one Iceberg schema document as a native root Field.
///
/// The document is an ordinary mapping - what `json.load` produces - because an
/// Iceberg schema is ordinary JSON. `name` is what the struct root is called,
/// since the document names the columns and never the record.
///
/// # Errors
///
/// Raises `ValueError` when the document is not an Iceberg struct schema.
#[pyfunction(name = "schema_from_json")]
pub(crate) fn iceberg_schema_from_json(
    name: &str,
    document: &Bound<'_, PyAny>,
) -> PyResult<PyField> {
    let document = crate::value::from_py(document)?;
    schema_from_json(name, &document)
        .map(PyField::from_inner)
        .map_err(value_error)
}

/// Write a native root Field as an Iceberg schema document.
///
/// # Errors
///
/// Raises `ValueError` when the root is not a non-null struct whose columns
/// carry field identifiers.
#[pyfunction(name = "schema_to_json")]
pub(crate) fn iceberg_schema_to_json(
    py: Python<'_>,
    schema: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let root = core_root_field_from_value(schema, SCHEMA_ROOT_NAME)?;
    let document = schema_to_json(&root).map_err(value_error)?;
    crate::value::as_py(py, &document)
}

/// Read a core format version out of the number or name Python spells it with.
fn format_version_from_value(value: &Bound<'_, PyAny>) -> PyResult<FormatVersion> {
    if let Ok(number) = value.extract::<i64>() {
        return FormatVersion::from_number(number).map_err(value_error);
    }
    Err(PyValueError::new_err(
        "expected an Iceberg format version of 1, 2, or 3",
    ))
}

/// Read a core partition spec out of what Python names one with.
///
/// A sequence of column names is the spelling a caller reaches for, and it
/// means the identity transform over those columns - the only transform that
/// can place a row without inverting a hash.
fn spec_from_value(value: &Bound<'_, PyAny>, schema: &CoreField) -> PyResult<PartitionSpec> {
    if let Ok(spec) = value.extract::<PyRef<'_, PyPartitionSpec>>() {
        return Ok(spec.inner.clone());
    }
    let columns = crate::media::strings_from_iterable(value, "partition_by")?;
    let borrowed: Vec<&str> = columns.iter().map(String::as_str).collect();
    PartitionSpec::identity(0, schema, &borrowed).map_err(value_error)
}

/// Project one Iceberg partition value as the Python value it stands for.
fn partition_values<'py>(py: Python<'py>, values: &[Value]) -> PyResult<Bound<'py, PyTuple>> {
    let projected: Vec<Py<PyAny>> = values
        .iter()
        .map(|value| crate::value::as_py(py, value))
        .collect::<PyResult<_>>()?;
    PyTuple::new(py, projected)
}

/// Project a field-id-keyed statistic as a mapping.
fn counts_by_id<'py>(py: Python<'py>, counts: &[(i32, i64)]) -> PyResult<Bound<'py, PyDict>> {
    let mapping = PyDict::new(py);
    for (id, count) in counts {
        mapping.set_item(id, count)?;
    }
    Ok(mapping)
}

/// Project a field-id-keyed bound as a mapping of encoded values.
fn bounds_by_id<'py>(py: Python<'py>, bounds: &[(i32, Vec<u8>)]) -> PyResult<Bound<'py, PyDict>> {
    let mapping = PyDict::new(py);
    for (id, value) in bounds {
        mapping.set_item(id, pyo3::types::PyBytes::new(py, value))?;
    }
    Ok(mapping)
}

/// An Iceberg table reached entirely through one container handle.
#[pyclass(name = "Table", module = "yggdryl._native", skip_from_py_object)]
pub(crate) struct PyTable {
    inner: Table<Holder>,
}

impl PyTable {
    fn from_core(inner: Table<Holder>) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PyTable {
    /// Create a table, writing its first metadata document.
    ///
    /// `partition_by` accepts a [`PartitionSpec`] or the column names to
    /// partition on; the default is unpartitioned. The schema must carry field
    /// identifiers, which [`assign_field_ids`] supplies.
    #[classmethod]
    #[pyo3(signature = (root, schema, partition_by = None, *, format_version = None))]
    fn create(
        _cls: &Bound<'_, PyType>,
        root: &PyIOBase,
        schema: &Bound<'_, PyAny>,
        partition_by: Option<&Bound<'_, PyAny>>,
        format_version: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let schema = core_root_field_from_value(schema, SCHEMA_ROOT_NAME)?;
        let spec = match partition_by {
            Some(value) => spec_from_value(value, &schema)?,
            None => PartitionSpec::unpartitioned(),
        };
        let version = match format_version {
            Some(value) => format_version_from_value(value)?,
            None => FormatVersion::V2,
        };
        Table::create(root.folder_holder()?, version, schema, spec)
            .map(Self::from_core)
            .map_err(value_error)
    }

    /// Open the table a container handle addresses.
    #[classmethod]
    fn open(_cls: &Bound<'_, PyType>, root: &PyIOBase) -> PyResult<Self> {
        Table::open(root.folder_holder()?)
            .map(Self::from_core)
            .map_err(value_error)
    }

    /// Open the table if it exists, creating it otherwise.
    #[classmethod]
    #[pyo3(signature = (root, schema, partition_by = None, *, format_version = None))]
    fn open_or_create(
        _cls: &Bound<'_, PyType>,
        root: &PyIOBase,
        schema: &Bound<'_, PyAny>,
        partition_by: Option<&Bound<'_, PyAny>>,
        format_version: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let schema = core_root_field_from_value(schema, SCHEMA_ROOT_NAME)?;
        let spec = match partition_by {
            Some(value) => spec_from_value(value, &schema)?,
            None => PartitionSpec::unpartitioned(),
        };
        let version = match format_version {
            Some(value) => format_version_from_value(value)?,
            None => FormatVersion::V2,
        };
        Table::open_or_create(root.folder_holder()?, version, schema, spec)
            .map(Self::from_core)
            .map_err(value_error)
    }

    /// The folder the table lives in.
    #[getter]
    fn root(&self) -> PyResult<PyIOBase> {
        Ok(PyIOBase::from_core(
            Holder::folder(
                self.inner
                    .root()
                    .url()
                    .ok_or_else(|| PyValueError::new_err("this table has no location"))?
                    .to_path()
                    .map_err(value_error)?,
            )
            .map_err(value_error)?,
        ))
    }

    /// The table's base location, as a URI.
    #[getter]
    fn location(&self) -> &str {
        self.inner.metadata().location.as_str()
    }

    /// The revision of the specification the metadata is written to.
    #[getter]
    fn format_version(&self) -> i32 {
        self.inner.metadata().format_version.number()
    }

    /// The stable identifier of the table itself.
    #[getter]
    fn table_uuid(&self) -> &str {
        self.inner.metadata().table_uuid.as_str()
    }

    /// The version number of the current metadata document.
    #[getter]
    fn version(&self) -> u32 {
        self.inner.version()
    }

    /// The name of the current metadata document.
    #[getter]
    fn metadata_file_name(&self) -> String {
        self.inner.metadata_file_name()
    }

    /// The location of the current metadata document, as a URI.
    #[getter]
    fn metadata_location(&self) -> PyResult<String> {
        self.inner.metadata_location().map_err(value_error)
    }

    /// The schema new data is written against.
    #[getter]
    fn schema(&self) -> PyResult<PyField> {
        self.inner
            .schema()
            .cloned()
            .map(PyField::from_inner)
            .map_err(value_error)
    }

    /// The partition spec new data is written against.
    #[getter]
    fn spec(&self) -> PyResult<PyPartitionSpec> {
        self.inner
            .metadata()
            .default_spec()
            .cloned()
            .map(PyPartitionSpec::from_core)
            .map_err(value_error)
    }

    /// The snapshot a reader sees, when the table has one.
    ///
    /// A table that has been created but never written has none, which is not a
    /// failure: it simply reads as no rows.
    #[getter]
    fn current_snapshot(&self) -> Option<PySnapshot> {
        self.inner
            .current_snapshot()
            .cloned()
            .map(PySnapshot::from_core)
    }

    /// Every retained snapshot, oldest first.
    #[getter]
    fn snapshots(&self) -> Vec<PySnapshot> {
        self.inner
            .metadata()
            .snapshots
            .iter()
            .cloned()
            .map(PySnapshot::from_core)
            .collect()
    }

    /// The free-form table properties the metadata document carries.
    #[getter]
    fn properties<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let properties = PyDict::new(py);
        for (key, value) in &self.inner.metadata().properties {
            properties.set_item(key.as_str(), value.as_str())?;
        }
        Ok(properties)
    }

    /// Every schema the table has had, by identifier.
    #[getter]
    fn schemas(&self) -> Vec<PyField> {
        self.inner
            .metadata()
            .schemas
            .iter()
            .cloned()
            .map(PyField::from_inner)
            .collect()
    }

    /// Every manifest the current snapshot points at.
    fn manifests(&self) -> PyResult<Vec<PyManifestFile>> {
        Ok(self
            .inner
            .manifests()
            .map_err(value_error)?
            .into_iter()
            .map(PyManifestFile::from_core)
            .collect())
    }

    /// Every live data file of the current snapshot, with the spec it was
    /// written under.
    fn data_files(&self) -> PyResult<Vec<(PyDataFile, PyPartitionSpec)>> {
        Ok(self
            .inner
            .data_files()
            .map_err(value_error)?
            .into_iter()
            .map(|(file, spec)| {
                (
                    PyDataFile::from_core(file),
                    PyPartitionSpec::from_core(spec),
                )
            })
            .collect())
    }

    /// Read the current snapshot as a `pyarrow.RecordBatchReader`.
    ///
    /// `field` is pushed down to each data file as its column projection and is
    /// then cast to the scan root, so files written under different schemas read
    /// as one shape.
    /// That cast is what makes a table whose schema evolved readable as one
    /// shape: a file written before a column existed contributes null for it.
    #[pyo3(signature = (field = None))]
    fn scan<'py>(
        &self,
        py: Python<'py>,
        field: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let field = field
            .map(|field| core_root_field_from_value(field, SCHEMA_ROOT_NAME))
            .transpose()?;
        let reader = self.inner.scan(field.as_ref()).map_err(value_error)?;
        batch_reader_to_pyarrow(py, reader)
    }

    /// Append `batches` as a new snapshot, keeping everything already stored.
    fn append(&mut self, batches: &Bound<'_, PyAny>) -> PyResult<()> {
        let batches = batch_reader_from_value(batches)?;
        self.inner.append(batches).map_err(value_error)
    }

    /// Replace every row with `batches` as a new snapshot.
    fn overwrite(&mut self, batches: &Bound<'_, PyAny>) -> PyResult<()> {
        let batches = batch_reader_from_value(batches)?;
        self.inner.overwrite(batches).map_err(value_error)
    }

    /// Add a schema, make it current, and write a new metadata document.
    fn evolve_schema(&mut self, schema: &Bound<'_, PyAny>) -> PyResult<i32> {
        let schema = core_root_field_from_value(schema, SCHEMA_ROOT_NAME)?;
        self.inner.evolve_schema(schema).map_err(value_error)
    }

    fn __repr__(&self) -> String {
        format!(
            "Table({:?}, format_version={}, version={})",
            self.inner.metadata().location.as_str(),
            self.inner.metadata().format_version.number(),
            self.inner.version(),
        )
    }
}

/// How a source column becomes a partition column.
#[pyclass(
    name = "PartitionField",
    module = "yggdryl._native",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub(crate) struct PyPartitionField {
    inner: PartitionField,
}

#[pymethods]
impl PyPartitionField {
    /// The partition column's name, which is also its directory prefix.
    #[getter]
    fn name(&self) -> &str {
        self.inner.name.as_str()
    }

    /// The transform's Iceberg name, such as `identity` or `bucket[16]`.
    #[getter]
    fn transform(&self) -> String {
        self.inner.transform.to_string()
    }

    /// The identifier of the schema field this partitions on.
    #[getter]
    fn source_id(&self) -> i32 {
        self.inner.source_id
    }

    /// The identifier of the partition field itself.
    #[getter]
    fn field_id(&self) -> i32 {
        self.inner.field_id
    }

    fn __repr__(&self) -> String {
        format!(
            "PartitionField({:?}, transform={:?})",
            self.inner.name.as_str(),
            self.inner.transform.to_string(),
        )
    }
}

/// The columns a table partitions on, in directory order.
#[pyclass(
    name = "PartitionSpec",
    module = "yggdryl._native",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub(crate) struct PyPartitionSpec {
    inner: PartitionSpec,
}

impl PyPartitionSpec {
    fn from_core(inner: PartitionSpec) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PyPartitionSpec {
    /// The unpartitioned spec, which every table has as spec zero.
    #[classmethod]
    fn unpartitioned(_cls: &Bound<'_, PyType>) -> Self {
        Self::from_core(PartitionSpec::unpartitioned())
    }

    /// Partition on the named columns' values, unchanged.
    ///
    /// Identity is one of the two transforms that can place a row, so it is the
    /// one a table written from here uses.
    #[classmethod]
    #[pyo3(signature = (schema, columns, *, spec_id = 0))]
    fn identity(
        _cls: &Bound<'_, PyType>,
        schema: &Bound<'_, PyAny>,
        columns: &Bound<'_, PyAny>,
        spec_id: i32,
    ) -> PyResult<Self> {
        let schema = core_root_field_from_value(schema, SCHEMA_ROOT_NAME)?;
        let names = crate::media::strings_from_iterable(columns, "columns")?;
        let borrowed: Vec<&str> = names.iter().map(String::as_str).collect();
        PartitionSpec::identity(spec_id, &schema, &borrowed)
            .map(Self::from_core)
            .map_err(value_error)
    }

    /// The identifier of this spec within the table.
    #[getter]
    fn spec_id(&self) -> i32 {
        self.inner.spec_id
    }

    /// Whether the spec partitions on nothing.
    fn is_unpartitioned(&self) -> bool {
        self.inner.is_unpartitioned()
    }

    /// The partition columns, in the order they nest as directories.
    #[getter]
    fn fields(&self) -> Vec<PyPartitionField> {
        self.inner
            .fields
            .iter()
            .cloned()
            .map(|inner| PyPartitionField { inner })
            .collect()
    }

    fn __len__(&self) -> usize {
        self.inner.fields.len()
    }

    fn __repr__(&self) -> String {
        format!(
            "PartitionSpec(spec_id={}, fields={})",
            self.inner.spec_id,
            self.inner.fields.len(),
        )
    }
}

/// One commit: what a table looked like at a point in time.
#[pyclass(
    name = "Snapshot",
    module = "yggdryl._native",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub(crate) struct PySnapshot {
    inner: Snapshot,
}

impl PySnapshot {
    fn from_core(inner: Snapshot) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PySnapshot {
    /// The identifier of this snapshot, unique within the table.
    #[getter]
    fn snapshot_id(&self) -> i64 {
        self.inner.snapshot_id
    }

    /// The snapshot this one was produced from, when there was one.
    #[getter]
    fn parent_snapshot_id(&self) -> Option<i64> {
        self.inner.parent_snapshot_id
    }

    /// The commit order, absent in v1 tables.
    #[getter]
    fn sequence_number(&self) -> Option<i64> {
        self.inner.sequence_number
    }

    /// When the commit happened, in milliseconds since the Unix epoch.
    #[getter]
    fn timestamp_ms(&self) -> i64 {
        self.inner.timestamp_ms
    }

    /// The location of the manifest list this snapshot's manifests are in.
    #[getter]
    fn manifest_list(&self) -> &str {
        self.inner.manifest_list.as_str()
    }

    /// What the commit did, defaulting to `append`.
    #[getter]
    fn operation(&self) -> &str {
        self.inner.operation()
    }

    /// The schema in effect when the snapshot was written.
    #[getter]
    fn schema_id(&self) -> Option<i32> {
        self.inner.schema_id
    }

    /// Everything the commit recorded about itself.
    #[getter]
    fn summary<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let summary = PyDict::new(py);
        for (key, value) in &self.inner.summary {
            summary.set_item(key.as_str(), value.as_str())?;
        }
        Ok(summary)
    }

    fn __repr__(&self) -> String {
        format!(
            "Snapshot({}, operation={:?})",
            self.inner.snapshot_id,
            self.inner.operation(),
        )
    }
}

/// One manifest of a snapshot: which files it covers and what they hold.
#[pyclass(
    name = "ManifestFile",
    module = "yggdryl._native",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub(crate) struct PyManifestFile {
    inner: ManifestFile,
}

impl PyManifestFile {
    fn from_core(inner: ManifestFile) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PyManifestFile {
    /// The manifest's location, as a URI.
    #[getter]
    fn path(&self) -> &str {
        self.inner.manifest_path.as_str()
    }

    /// The size of the manifest in bytes.
    #[getter]
    fn length(&self) -> i64 {
        self.inner.manifest_length
    }

    /// The identifier of the spec the manifest's entries were written under.
    #[getter]
    fn partition_spec_id(&self) -> i32 {
        self.inner.partition_spec_id
    }

    /// Whether the manifest lists data files rather than delete files.
    fn is_data(&self) -> bool {
        self.inner.content == ManifestContent::Data
    }

    /// The commit order assigned when the manifest was added.
    #[getter]
    fn sequence_number(&self) -> i64 {
        self.inner.sequence_number
    }

    /// The snapshot that added the manifest.
    #[getter]
    fn added_snapshot_id(&self) -> i64 {
        self.inner.added_snapshot_id
    }

    /// The files the manifest marks added.
    #[getter]
    fn added_files_count(&self) -> i32 {
        self.inner.added_files_count
    }

    /// The files the manifest marks existing.
    #[getter]
    fn existing_files_count(&self) -> i32 {
        self.inner.existing_files_count
    }

    /// The files the manifest marks deleted.
    #[getter]
    fn deleted_files_count(&self) -> i32 {
        self.inner.deleted_files_count
    }

    /// The rows in the added files.
    #[getter]
    fn added_rows_count(&self) -> i64 {
        self.inner.added_rows_count
    }

    /// The rows in the existing files.
    #[getter]
    fn existing_rows_count(&self) -> i64 {
        self.inner.existing_rows_count
    }

    fn __repr__(&self) -> String {
        format!(
            "ManifestFile({:?}, added_files_count={})",
            self.inner.manifest_path.as_str(),
            self.inner.added_files_count,
        )
    }
}

/// One data file a manifest lists.
#[pyclass(
    name = "DataFile",
    module = "yggdryl._native",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub(crate) struct PyDataFile {
    inner: DataFile,
}

impl PyDataFile {
    fn from_core(inner: DataFile) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PyDataFile {
    /// The file's location, as a URI.
    #[getter]
    fn path(&self) -> &str {
        self.inner.file_path.as_str()
    }

    /// The encoding the file uses, such as `PARQUET`.
    #[getter]
    fn file_format(&self) -> String {
        self.inner.file_format.to_string()
    }

    /// The partition tuple, one value per partition field of the spec.
    ///
    /// The manifest is the authority on a partition value, not the directory
    /// name: a null is spelled `null` in a path, and a path cannot say whether
    /// that is the string or the absence.
    #[getter]
    fn partition<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        partition_values(py, &self.inner.partition)
    }

    /// The rows in the file.
    #[getter]
    fn record_count(&self) -> i64 {
        self.inner.record_count
    }

    /// The size of the file in bytes.
    #[getter]
    fn file_size_in_bytes(&self) -> i64 {
        self.inner.file_size_in_bytes
    }

    /// The values per column, keyed by field identifier.
    #[getter]
    fn value_counts<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        counts_by_id(py, &self.inner.value_counts)
    }

    /// The nulls per column, keyed by field identifier.
    #[getter]
    fn null_value_counts<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        counts_by_id(py, &self.inner.null_value_counts)
    }

    /// The stored bytes per column, keyed by field identifier.
    #[getter]
    fn column_sizes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        counts_by_id(py, &self.inner.column_sizes)
    }

    /// The minimum per column, keyed by field identifier.
    ///
    /// A bound travels as the encoded value Iceberg stores, not as a decoded
    /// scalar, and it is present only for the types whose encoding the two
    /// formats agree on - which is what makes it safe to compare.
    #[getter]
    fn lower_bounds<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        bounds_by_id(py, &self.inner.lower_bounds)
    }

    /// The maximum per column, keyed by field identifier.
    #[getter]
    fn upper_bounds<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        bounds_by_id(py, &self.inner.upper_bounds)
    }

    /// The byte offsets a reader may split the file at.
    #[getter]
    fn split_offsets(&self) -> Vec<i64> {
        self.inner.split_offsets.clone()
    }

    /// The sort order the file was written in, when one applies.
    #[getter]
    fn sort_order_id(&self) -> Option<i32> {
        self.inner.sort_order_id
    }

    /// Zero for rows, one for position deletes, two for equality deletes.
    #[getter]
    fn content(&self) -> i32 {
        self.inner.content
    }

    fn __repr__(&self) -> String {
        format!(
            "DataFile({:?}, record_count={})",
            self.inner.file_path.as_str(),
            self.inner.record_count,
        )
    }
}
