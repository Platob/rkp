//! The record surface: `PyArrow` readers in, `PyArrow` readers out.
//!
//! Columnar data crosses this boundary through the Arrow C Stream interface and
//! nothing else. A Python caller hands in anything that exports a stream - a
//! `pyarrow.RecordBatchReader`, a `Table`, a `RecordBatch`, any object
//! implementing `__arrow_c_stream__` - and gets a `pyarrow.RecordBatchReader`
//! back, so the batches themselves are never copied and never rebuilt: the two
//! runtimes point at the same buffers.
//!
//! [`PyRecordOptions`] is the settings value every record call takes. It is the
//! core [`RecordOptions`] and not a Python model of one, so the encoding a
//! handle uses is still derived from its media type rather than guessed here.

use arrow_array::RecordBatch;
use arrow_array::ffi_stream::ArrowArrayStreamReader;
use arrow_pyarrow::{FromPyArrow, IntoPyArrow};
use arrow_schema::Schema as ArrowSchema;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyType};

use yggdryl::arrow::BatchReader;
use yggdryl::generic::{IORecordOptions, RecordOptions};
use yggdryl::{Field as CoreField, Level};

use crate::field::{PyField, core_field_from_value};
use crate::media::{PyMimeType, core_media_type_from_value};
use crate::value_error;

/// Read a core root Field out of anything Python describes rows with.
///
/// A root is a non-null Struct Field, and Python spells one four ways: the
/// native wrapper, a field expression, a `pyarrow.Schema`, or a
/// `pyarrow.Field`. `root_name` names the struct when the spelling carries no
/// name of its own, because Arrow names columns and never the record.
pub(crate) fn core_root_field_from_value(
    value: &Bound<'_, PyAny>,
    root_name: &str,
) -> PyResult<CoreField> {
    if value.extract::<PyRef<'_, PyField>>().is_ok() || value.extract::<&str>().is_ok() {
        return core_field_from_value(value);
    }
    if is_pyarrow_schema(value) {
        let schema = ArrowSchema::from_pyarrow_bound(value)?;
        return yggdryl::arrow::record_schema_from_arrow(root_name, &schema).map_err(value_error);
    }
    core_field_from_value(value)
}

/// Report whether a value is a `pyarrow.Schema`.
///
/// A Schema and a Field both export `__arrow_c_schema__`, and a Schema exported
/// that way arrives as an unnamed struct Field, so the two have to be told
/// apart before the import rather than after it.
fn is_pyarrow_schema(value: &Bound<'_, PyAny>) -> bool {
    value
        .py()
        .import("pyarrow")
        .and_then(|module| module.getattr("Schema"))
        .and_then(|class| value.is_instance(&class))
        .unwrap_or(false)
}

/// Read a core batch reader out of anything `PyArrow` exports batches as.
///
/// The C Stream interface is the fast path and the one every current `PyArrow`
/// container implements, so a reader, a table, and a batch all arrive without a
/// copy. A plain sequence of batches is accepted too, because that is what a
/// caller who built rows one batch at a time is holding.
///
/// # Errors
///
/// Returns a `TypeError` when the value exports no batches, or a `ValueError`
/// when a sequence is empty and therefore names no schema.
pub(crate) fn batch_reader_from_value(value: &Bound<'_, PyAny>) -> PyResult<BatchReader> {
    if value.hasattr("__arrow_c_stream__")? {
        let reader = ArrowArrayStreamReader::from_pyarrow_bound(value)?;
        return Ok(Box::new(reader));
    }
    // A `RecordBatch` older than the C stream protocol still exports one batch
    // through the C array protocol, so it is recognized before the reader
    // fallback, which accepts nothing but a `RecordBatchReader`.
    if let Ok(batch) = RecordBatch::from_pyarrow_bound(value) {
        let schema = batch.schema();
        return Ok(yggdryl::arrow::batch_reader(schema, [batch]));
    }
    if value.hasattr("_export_to_c")? {
        let reader = ArrowArrayStreamReader::from_pyarrow_bound(value)?;
        return Ok(Box::new(reader));
    }
    if let Ok(batches) = value.try_iter() {
        let mut collected: Vec<RecordBatch> = Vec::new();
        for batch in batches {
            collected.push(RecordBatch::from_pyarrow_bound(&batch?)?);
        }
        let schema = collected.first().map(RecordBatch::schema).ok_or_else(|| {
            PyValueError::new_err(
                "expected at least one batch to take a schema from, got an empty sequence",
            )
        })?;
        return Ok(yggdryl::arrow::batch_reader(schema, collected));
    }
    Err(PyTypeError::new_err(
        "expected a pyarrow.RecordBatchReader, Table, RecordBatch, Arrow C stream exporter, or \
         iterable of RecordBatch",
    ))
}

/// Hand a core batch reader to Python as a `pyarrow.RecordBatchReader`.
///
/// The reader stays lazy across the boundary: `PyArrow` pulls one batch at a time
/// through the C stream, so a resource larger than memory is readable from
/// Python exactly as it is from Rust.
pub(crate) fn batch_reader_to_pyarrow(
    py: Python<'_>,
    reader: BatchReader,
) -> PyResult<Bound<'_, PyAny>> {
    reader.into_pyarrow(py)
}

/// Report that a setting belongs to an encoding these options are not.
fn not_parquet(options: &RecordOptions, setting: &str) -> PyErr {
    PyValueError::new_err(format!(
        "expected Parquet options to {setting}, got {} options",
        options.mime_type()
    ))
}

/// Name a page compression the way the Parquet parser accepts it.
///
/// The codec's own `Display` prints the level as its internal type, which its
/// own `FromStr` then refuses, so the accepted spelling is written here rather
/// than recovered from that text.
fn compression_name(compression: parquet::basic::Compression) -> String {
    use parquet::basic::Compression as C;

    match compression {
        C::UNCOMPRESSED => "uncompressed".to_owned(),
        C::SNAPPY => "snappy".to_owned(),
        C::GZIP(level) => format!("gzip({})", level.compression_level()),
        C::LZO => "lzo".to_owned(),
        C::BROTLI(level) => format!("brotli({})", level.compression_level()),
        C::LZ4 => "lz4".to_owned(),
        C::ZSTD(level) => format!("zstd({})", level.compression_level()),
        C::LZ4_RAW => "lz4_raw".to_owned(),
    }
}

/// The settings one record read or write takes.
#[pyclass(
    name = "RecordOptions",
    module = "yggdryl._native",
    skip_from_py_object
)]
#[derive(Clone)]
pub(crate) struct PyRecordOptions {
    pub(crate) inner: RecordOptions,
}

impl PyRecordOptions {
    pub(crate) fn from_core(inner: RecordOptions) -> Self {
        Self { inner }
    }
}

/// Read core record options out of a value, or derive them from a media type.
///
/// A caller who only wants to name the encoding passes the media type itself,
/// which is the same derivation `IOBase.record_options` performs.
pub(crate) fn core_record_options_from_value(value: &Bound<'_, PyAny>) -> PyResult<RecordOptions> {
    if let Ok(options) = value.extract::<PyRef<'_, PyRecordOptions>>() {
        return Ok(options.inner.clone());
    }
    RecordOptions::for_media_type(&core_media_type_from_value(value)?).map_err(value_error)
}

#[pymethods]
impl PyRecordOptions {
    /// Derive the options for the encoding a media type names.
    #[new]
    fn new(media_type: &Bound<'_, PyAny>) -> PyResult<Self> {
        RecordOptions::for_media_type(&core_media_type_from_value(media_type)?)
            .map(Self::from_core)
            .map_err(value_error)
    }

    /// Derive the options for the encoding a media type names.
    #[classmethod]
    fn for_media_type(_cls: &Bound<'_, PyType>, media_type: &Bound<'_, PyAny>) -> PyResult<Self> {
        Self::new(media_type)
    }

    /// The MIME type of the encoding these options describe.
    #[getter]
    fn mime_type(&self) -> PyMimeType {
        PyMimeType::from_core(self.inner.mime_type())
    }

    /// The declared canonical schema, when one was declared.
    #[getter]
    fn schema(&self) -> Option<PyField> {
        self.inner.schema().cloned().map(PyField::from_inner)
    }

    #[setter]
    fn set_schema(&mut self, schema: &Bound<'_, PyAny>) -> PyResult<()> {
        let field = core_root_field_from_value(schema, self.inner.root_name())?;
        self.inner.set_schema(field);
        Ok(())
    }

    /// The root Field name used when a schema is inferred.
    #[getter]
    fn root_name(&self) -> &str {
        self.inner.root_name()
    }

    #[setter]
    fn set_root_name(&mut self, root_name: &str) {
        // The trait's setter names a `SmolStr`, which is the core's string type
        // and not a dependency of this crate; its builder takes anything that
        // converts into one, so the builder is the route from a Python string.
        self.inner = self.inner.clone().with_root_name(root_name);
    }

    /// Whether a cast may null a value it cannot convert.
    #[getter]
    fn safe(&self) -> bool {
        self.inner.safe()
    }

    #[setter]
    fn set_safe(&mut self, safe: bool) {
        self.inner.set_safe(safe);
    }

    /// The row-per-batch bound, when one is set.
    #[getter]
    fn batch_size(&self) -> Option<usize> {
        self.inner.batch_size()
    }

    #[setter]
    fn set_batch_size(&mut self, batch_size: Option<usize>) {
        self.inner.set_batch_size(batch_size);
    }

    /// The compression level applied to a content coding, on the 0-to-9 scale.
    #[getter]
    fn level(&self) -> u8 {
        self.inner.level().get()
    }

    #[setter]
    fn set_level(&mut self, level: u8) {
        self.inner.set_level(Level::new(level));
    }

    /// The column names a write matches rows on; empty means overwrite.
    #[getter]
    fn merge_by(&self) -> Vec<String> {
        self.inner.merge_by().to_vec()
    }

    #[setter]
    fn set_merge_by(&mut self, merge_by: Vec<String>) {
        self.inner.set_merge_by(merge_by);
    }

    /// The page compression applied inside a Parquet file, if this is one.
    ///
    /// A setting one encoding has is absent on the others rather than invented,
    /// so this is `None` for an Arrow IPC stream, whose coding belongs to the
    /// handle instead.
    #[getter]
    fn compression(&self) -> Option<String> {
        match &self.inner {
            RecordOptions::Parquet(options) => Some(compression_name(options.compression)),
            RecordOptions::Ipc(_) => None,
        }
    }

    #[setter]
    fn set_compression(&mut self, compression: &str) -> PyResult<()> {
        let RecordOptions::Parquet(options) = &mut self.inner else {
            return Err(not_parquet(&self.inner, "set a page compression"));
        };
        // The target field names the type, so the parquet crate's own parser is
        // what accepts `uncompressed`, `snappy`, `zstd(3)`, and the rest.
        options.compression = compression.parse().map_err(value_error)?;
        Ok(())
    }

    /// The maximum rows per row group, for the encodings that have them.
    #[getter]
    fn max_row_group_size(&self) -> Option<usize> {
        match &self.inner {
            RecordOptions::Parquet(options) => Some(options.max_row_group_size),
            RecordOptions::Ipc(_) => None,
        }
    }

    #[setter]
    fn set_max_row_group_size(&mut self, rows: usize) -> PyResult<()> {
        match &mut self.inner {
            RecordOptions::Parquet(options) => {
                options.max_row_group_size = rows;
                Ok(())
            }
            RecordOptions::Ipc(_) => Err(not_parquet(&self.inner, "set a row-group size")),
        }
    }

    /// The file-level metadata written into a footer, for the encodings that
    /// have one.
    #[getter]
    fn key_value_metadata<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyDict>>> {
        let RecordOptions::Parquet(options) = &self.inner else {
            return Ok(None);
        };
        let pairs = PyDict::new(py);
        for (key, value) in &options.key_value_metadata {
            pairs.set_item(key, value)?;
        }
        Ok(Some(pairs))
    }

    #[setter]
    fn set_key_value_metadata(&mut self, metadata: &Bound<'_, PyAny>) -> PyResult<()> {
        let RecordOptions::Parquet(options) = &mut self.inner else {
            return Err(not_parquet(&self.inner, "set footer metadata"));
        };
        let items = if metadata.hasattr("items")? {
            metadata.call_method0("items")?
        } else {
            metadata.clone()
        };
        let mut pairs = Vec::new();
        for item in items.try_iter()? {
            pairs.push(item?.extract::<(String, String)>()?);
        }
        options.key_value_metadata = pairs;
        Ok(())
    }

    fn __repr__(&self) -> String {
        format!(
            "RecordOptions({:?}, root_name={:?}, safe={})",
            self.inner.mime_type().as_str(),
            self.inner.root_name(),
            if self.inner.safe() { "True" } else { "False" },
        )
    }

    fn __copy__(&self) -> Self {
        self.clone()
    }

    fn __deepcopy__(&self, _memo: &Bound<'_, PyAny>) -> Self {
        self.clone()
    }
}
