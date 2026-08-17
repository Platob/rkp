//! Native Python view of Yggdryl fields and metadata.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::RecordBatch as ArrowRecordBatch;
use arrow_pyarrow::{FromPyArrow, PyArrowType, ToPyArrow};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use pyo3::class::basic::CompareOp;
use pyo3::exceptions::{PyKeyError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBool, PyDict, PyString};
use yggdryl::ArrowCast;
use yggdryl::arrow::DefaultArrowScalar;
use yggdryl::{DataType as CoreDataType, Field as CoreField, Scheme as CoreScheme};

use crate::datatype::{
    PyDataType, arrow_array_from_pyarrow, arrow_array_to_pyarrow, arrow_scalar_to_pyarrow_type,
    core_data_type_from_value, core_field_to_pyarrow, default_arrow_scalar_to_pyarrow,
};
use crate::media::{
    PyMediaType, PyMimeType, core_media_type_from_value, core_mime_type_from_value,
};
use crate::uri::{PyUrl, core_url_from_value};
use crate::{PyDifferenceIterator, compare, value_error};

pub(crate) fn core_field_from_value(value: &Bound<'_, PyAny>) -> PyResult<CoreField> {
    if let Ok(value) = value.extract::<PyRef<'_, PyField>>() {
        return Ok(value.inner.clone());
    }
    if let Ok(value) = value.extract::<&str>() {
        return CoreField::from_str(value).map_err(value_error);
    }

    let imported: PyResult<CoreField> = (|| {
        let arrow_field = ArrowField::from_pyarrow_bound(value)?;
        let mut field = CoreField::try_from(arrow_field).map_err(value_error)?;

        // PyArrow's Field C Schema bridge can omit datatype-only flags such as
        // Map.keys_sorted. Its standalone datatype bridge is lossless, so use
        // that authoritative type when it differs from the field projection.
        let py_data_type = value.getattr("type")?;
        let arrow_data_type = ArrowDataType::from_pyarrow_bound(&py_data_type)?;
        let data_type = CoreDataType::try_from(arrow_data_type).map_err(value_error)?;
        if field.data_type() != &data_type {
            field = field.try_with_data_type(data_type).map_err(value_error)?;
        }
        Ok(field)
    })();
    imported.map_err(|error| {
        if error.is_instance_of::<PyTypeError>(value.py()) {
            PyTypeError::new_err("expected a yggdryl.Field, field string, or PyArrow Field")
        } else {
            error
        }
    })
}

fn extend_metadata_pairs(
    value: &Bound<'_, PyAny>,
    pairs: &mut BTreeMap<String, String>,
) -> PyResult<()> {
    let iterable = if value.hasattr("items")? {
        value.call_method0("items")?
    } else {
        value.clone()
    };
    for item in iterable.try_iter()? {
        let (key, value) = item?.extract::<(String, String)>()?;
        pairs.insert(key, value);
    }
    Ok(())
}

/// A Yggdryl field whose mapping protocol directly manages core metadata.
#[pyclass(name = "Field", module = "yggdryl._native", skip_from_py_object)]
#[derive(Clone)]
pub(crate) struct PyField {
    pub(crate) inner: CoreField,
    read_only: bool,
}

#[derive(Clone, Copy)]
struct FieldId(i32);

#[derive(Clone, Copy)]
struct ContentLength(u64);

impl FromPyObject<'_, '_> for FieldId {
    type Error = PyErr;

    fn extract(value: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        if value.is_instance_of::<PyBool>() {
            return Err(PyTypeError::new_err(
                "field id must be an integer, not bool",
            ));
        }
        value.extract::<i32>().map(Self)
    }
}

impl FromPyObject<'_, '_> for ContentLength {
    type Error = PyErr;

    fn extract(value: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        if value.is_instance_of::<PyBool>() {
            return Err(PyTypeError::new_err(
                "content length must be an unsigned integer, not bool",
            ));
        }
        value.extract::<u64>().map(Self)
    }
}

impl PyField {
    pub(crate) fn from_inner(inner: CoreField) -> Self {
        Self {
            inner,
            read_only: false,
        }
    }

    fn require_mutable(&self) -> PyResult<()> {
        if self.read_only {
            Err(PyTypeError::new_err(
                "frozen record schema fields are read-only",
            ))
        } else {
            Ok(())
        }
    }
}

#[pymethods]
impl PyField {
    #[new]
    #[pyo3(signature = (name, data_type, nullable=true, metadata=None))]
    fn new(
        name: String,
        data_type: &Bound<'_, PyAny>,
        nullable: bool,
        metadata: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let data_type = core_data_type_from_value(data_type)?;
        if let Some(metadata) = metadata {
            let mut pairs = BTreeMap::new();
            extend_metadata_pairs(metadata, &mut pairs)?;
            return CoreField::from_parts(name, data_type, nullable, pairs)
                .map(Self::from_inner)
                .map_err(value_error);
        }
        let field = CoreField::new(name, data_type, nullable);
        field.validate().map_err(value_error)?;
        Ok(Self::from_inner(field))
    }

    #[staticmethod]
    fn from_value(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        core_field_from_value(value).map(Self::from_inner)
    }

    /// Infers a native field from a name and Python type annotation.
    #[staticmethod]
    #[pyo3(signature = (name, hint, metadata=None))]
    fn from_pyhint(
        name: &str,
        hint: &Bound<'_, PyAny>,
        metadata: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let module = hint.py().import("yggdryl.records._hints")?;
        let inferred = if let Some(metadata) = metadata {
            module
                .getattr("field_from_pyhint")?
                .call1((name, hint, metadata))?
        } else {
            module.getattr("field_from_pyhint")?.call1((name, hint))?
        };
        let inferred = inferred.extract::<PyRef<'_, Self>>()?;
        Ok(inferred.clone())
    }

    #[staticmethod]
    fn from_str(value: &str) -> PyResult<Self> {
        CoreField::from_str(value)
            .map(Self::from_inner)
            .map_err(value_error)
    }

    #[staticmethod]
    fn from_arrow(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        core_field_from_value(value).map(Self::from_inner)
    }

    /// Imports one complete Arrow Schema through Yggdryl's Arrow IPC metadata
    /// rules. This is private glue for the Python record class factory.
    #[staticmethod]
    fn _record_root_from_arrow_schema(
        value: PyArrowType<ArrowSchema>,
        name: &str,
    ) -> PyResult<Self> {
        let PyArrowType(schema) = value;
        yggdryl::arrow::record_schema_from_arrow(name, &schema)
            .map(Self::from_inner)
            .map_err(value_error)
    }

    /// Projects a complete record root as an Arrow transport Schema while
    /// keeping the reserved dictionary-ID sidecar out of Field metadata.
    fn _record_root_to_arrow_transport_schema(&self) -> PyResult<PyArrowType<ArrowSchema>> {
        yggdryl::arrow::record_schema_to_arrow(&self.inner)
            .map(PyArrowType)
            .map_err(value_error)
    }

    #[staticmethod]
    fn from_json(value: &str) -> PyResult<Self> {
        CoreField::from_json(value)
            .map(Self::from_inner)
            .map_err(value_error)
    }

    fn to_arrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        core_field_to_pyarrow(py, &self.inner)
    }

    /// Returns the cached Python annotation corresponding to this Field.
    fn default_pyhint<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let field = Py::new(py, self.clone())?;
        py.import("yggdryl.records._defaults")?
            .getattr("_default_pyhint_from_field")?
            .call1((field,))
    }

    /// Returns the core-selected Field default as an exact `PyArrow` Scalar.
    fn default_arrow_scalar<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let scalar = self.inner.default_arrow_scalar().map_err(value_error)?;
        default_arrow_scalar_to_pyarrow(py, scalar)
    }

    /// Returns the core-selected Field default in its cached Python type plan.
    fn default_pyvalue<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let scalar = self.inner.default_arrow_scalar().map_err(value_error)?;
        let scalar = default_arrow_scalar_to_pyarrow(py, scalar)?;
        let field = Py::new(py, self.clone())?;
        py.import("yggdryl.records._defaults")?
            .getattr("_default_pyvalue_from_field")?
            .call1((field, scalar))
    }

    /// Returns a recursively normalized field for a named compatibility target.
    fn to_scheme_compat(&self, target: &str) -> PyResult<Self> {
        let target = CoreScheme::from_str(target).map_err(value_error)?;
        self.inner
            .to_scheme_compat(&target)
            .map(Self::from_inner)
            .map_err(value_error)
    }

    /// Constructs a `PyArrow` scalar with this field's exact datatype.
    #[pyo3(signature = (value, *, safe=true))]
    fn arrow_scalar<'py>(
        &self,
        py: Python<'py>,
        value: &Bound<'py, PyAny>,
        safe: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Project the complete Field so registered extension metadata can be
        // rehydrated by PyArrow before selecting its scalar target type.
        let arrow_field = core_field_to_pyarrow(py, &self.inner)?;
        let target = arrow_field.getattr("type")?;
        let scalar = arrow_scalar_to_pyarrow_type(py, value, target, safe)?;
        if !self.inner.is_nullable() && !scalar.getattr("is_valid")?.extract::<bool>()? {
            return Err(PyValueError::new_err(format!(
                "field {:?} is not nullable",
                self.inner.name()
            )));
        }
        Ok(scalar)
    }

    /// Casts and null/default-fills one `PyArrow` Array through the exact Field.
    #[pyo3(signature = (value, *, safe=true))]
    fn cast_arrow_array<'py>(
        &self,
        py: Python<'py>,
        value: &Bound<'py, PyAny>,
        safe: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let input = arrow_array_from_pyarrow(value)?;
        let array = self
            .inner
            .cast_arrow_array(Arc::clone(&input), safe)
            .map_err(value_error)?;
        if Arc::ptr_eq(&input, &array) {
            let has_extension_metadata = self.inner.has_metadata("ARROW:extension:name")
                || self.inner.has_metadata("ARROW:extension:metadata");
            if !has_extension_metadata {
                return Ok(value.clone());
            }
            // An Arrow Array carries only its storage datatype on the Rust
            // side.  Extension identity is Field metadata, so a storage array
            // can be a native no-op while still requiring an exact Field C
            // Schema export to rehydrate its PyArrow ExtensionType.
            let target = core_field_to_pyarrow(py, &self.inner)?.getattr("type")?;
            if value.getattr("type")?.eq(&target)? {
                return Ok(value.clone());
            }
        }
        arrow_array_to_pyarrow(py, &array, Some(&self.inner))
    }

    /// Reconciles one `PyArrow` `RecordBatch` to this exact Struct Field.
    #[pyo3(signature = (value, *, safe=true))]
    fn cast_arrow_batch<'py>(
        &self,
        py: Python<'py>,
        value: &Bound<'py, PyAny>,
        safe: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let batch = ArrowRecordBatch::from_pyarrow_bound(value)?;
        let source_schema = batch.schema();
        let source_columns = batch.columns().to_vec();
        let cast = self
            .inner
            .cast_arrow_batch(batch, safe)
            .map_err(value_error)?;
        if Arc::ptr_eq(&source_schema, &cast.schema())
            && source_columns
                .iter()
                .zip(cast.columns())
                .all(|(left, right)| Arc::ptr_eq(left, right))
        {
            return Ok(value.clone());
        }
        cast.to_pyarrow(py)
    }

    #[allow(clippy::wrong_self_convention)]
    fn into_arrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        core_field_to_pyarrow(py, &self.inner)
    }

    fn to_json(&self) -> PyResult<String> {
        self.inner.to_json().map_err(value_error)
    }

    #[allow(clippy::wrong_self_convention)]
    fn into_json(&self) -> PyResult<String> {
        self.inner.clone().into_json().map_err(value_error)
    }

    #[getter]
    fn name(&self) -> &str {
        self.inner.name()
    }

    #[getter]
    fn data_type(&self) -> PyDataType {
        PyDataType {
            inner: self.inner.data_type().clone(),
        }
    }

    #[getter]
    fn nullable(&self) -> bool {
        self.inner.is_nullable()
    }

    #[getter]
    fn id(&self) -> PyResult<Option<i32>> {
        self.inner.id().map_err(value_error)
    }

    #[getter]
    fn dictionary_id(&self) -> Option<i64> {
        self.inner.dictionary_id()
    }

    #[getter]
    fn dictionary_is_ordered(&self) -> Option<bool> {
        self.inner.dictionary_is_ordered()
    }

    #[getter]
    fn alias(&self) -> Option<&str> {
        self.inner.alias()
    }

    #[getter]
    fn catalog_name(&self) -> Option<&str> {
        self.inner.catalog_name()
    }

    #[getter]
    fn schema_name(&self) -> Option<&str> {
        self.inner.schema_name()
    }

    #[getter]
    fn table_name(&self) -> Option<&str> {
        self.inner.table_name()
    }

    #[getter]
    fn location(&self) -> PyResult<Option<PyUrl>> {
        self.inner
            .location()
            .map(|value| value.map(PyUrl::from_core))
            .map_err(value_error)
    }

    #[getter]
    fn accept(&self) -> Option<&str> {
        self.inner.accept()
    }

    #[getter]
    fn accept_encoding(&self) -> Option<&str> {
        self.inner.accept_encoding()
    }

    #[getter]
    fn accept_language(&self) -> Option<&str> {
        self.inner.accept_language()
    }

    #[getter]
    fn accept_ranges(&self) -> Option<&str> {
        self.inner.accept_ranges()
    }

    #[getter]
    fn cache_control(&self) -> Option<&str> {
        self.inner.cache_control()
    }

    #[getter]
    fn content_disposition(&self) -> Option<&str> {
        self.inner.content_disposition()
    }

    #[getter]
    fn content_encoding(&self) -> Option<&str> {
        self.inner.content_encoding()
    }

    #[getter]
    fn content_language(&self) -> Option<&str> {
        self.inner.content_language()
    }

    #[getter]
    fn content_length(&self) -> PyResult<Option<u64>> {
        self.inner.content_length().map_err(value_error)
    }

    #[getter]
    fn content_location(&self) -> Option<&str> {
        self.inner.content_location()
    }

    #[getter]
    fn content_range(&self) -> Option<&str> {
        self.inner.content_range()
    }

    #[getter]
    fn content_type(&self) -> Option<&str> {
        self.inner.content_type()
    }

    #[getter]
    fn mime_type(&self) -> PyResult<PyMimeType> {
        self.inner
            .mime_type()
            .map(PyMimeType::from_core)
            .map_err(value_error)
    }

    #[getter]
    fn media_type(&self) -> PyResult<PyMediaType> {
        self.inner
            .media_type()
            .map(PyMediaType::from_core)
            .map_err(value_error)
    }

    #[getter]
    fn etag(&self) -> Option<&str> {
        self.inner.etag()
    }

    #[getter]
    fn expires(&self) -> Option<&str> {
        self.inner.expires()
    }

    #[getter]
    fn last_modified(&self) -> Option<&str> {
        self.inner.last_modified()
    }

    #[getter]
    fn http_location(&self) -> PyResult<Option<PyUrl>> {
        self.inner
            .http_location()
            .map(|value| value.map(PyUrl::from_core))
            .map_err(value_error)
    }

    #[getter]
    fn range(&self) -> Option<&str> {
        self.inner.range()
    }

    #[getter]
    fn vary(&self) -> Option<&str> {
        self.inner.vary()
    }

    fn set_dictionary_options(&mut self, id: i64, is_ordered: bool) -> PyResult<()> {
        self.require_mutable()?;
        self.inner
            .set_dictionary_options(id, is_ordered)
            .map_err(value_error)
    }

    fn set_id(&mut self, id: FieldId) -> PyResult<()> {
        self.require_mutable()?;
        self.inner.set_id(id.0);
        Ok(())
    }

    fn remove_id(&mut self) -> PyResult<Option<i32>> {
        self.require_mutable()?;
        self.inner.remove_id().map_err(value_error)
    }

    fn set_alias(&mut self, value: String) -> PyResult<()> {
        self.require_mutable()?;
        self.inner.set_alias(value).map_err(value_error)
    }

    fn remove_alias(&mut self) -> PyResult<Option<String>> {
        self.require_mutable()?;
        Ok(self.inner.remove_alias())
    }

    fn set_catalog_name(&mut self, value: String) -> PyResult<()> {
        self.require_mutable()?;
        self.inner.set_catalog_name(value).map_err(value_error)
    }

    fn remove_catalog_name(&mut self) -> PyResult<Option<String>> {
        self.require_mutable()?;
        Ok(self.inner.remove_catalog_name())
    }

    fn set_schema_name(&mut self, value: String) -> PyResult<()> {
        self.require_mutable()?;
        self.inner.set_schema_name(value).map_err(value_error)
    }

    fn remove_schema_name(&mut self) -> PyResult<Option<String>> {
        self.require_mutable()?;
        Ok(self.inner.remove_schema_name())
    }

    fn set_table_name(&mut self, value: String) -> PyResult<()> {
        self.require_mutable()?;
        self.inner.set_table_name(value).map_err(value_error)
    }

    fn remove_table_name(&mut self) -> PyResult<Option<String>> {
        self.require_mutable()?;
        Ok(self.inner.remove_table_name())
    }

    fn set_location(&mut self, value: &Bound<'_, PyAny>) -> PyResult<()> {
        self.require_mutable()?;
        self.inner.set_location(core_url_from_value(value)?);
        Ok(())
    }

    fn remove_location(&mut self) -> PyResult<Option<PyUrl>> {
        self.require_mutable()?;
        self.inner
            .remove_location()
            .map(|value| value.map(PyUrl::from_core))
            .map_err(value_error)
    }

    fn set_accept(&mut self, value: String) -> PyResult<()> {
        self.require_mutable()?;
        self.inner.set_accept(value).map_err(value_error)
    }

    fn remove_accept(&mut self) -> PyResult<Option<String>> {
        self.require_mutable()?;
        Ok(self.inner.remove_accept())
    }

    fn set_accept_encoding(&mut self, value: String) -> PyResult<()> {
        self.require_mutable()?;
        self.inner.set_accept_encoding(value).map_err(value_error)
    }

    fn remove_accept_encoding(&mut self) -> PyResult<Option<String>> {
        self.require_mutable()?;
        Ok(self.inner.remove_accept_encoding())
    }

    fn set_accept_language(&mut self, value: String) -> PyResult<()> {
        self.require_mutable()?;
        self.inner.set_accept_language(value).map_err(value_error)
    }

    fn remove_accept_language(&mut self) -> PyResult<Option<String>> {
        self.require_mutable()?;
        Ok(self.inner.remove_accept_language())
    }

    fn set_accept_ranges(&mut self, value: String) -> PyResult<()> {
        self.require_mutable()?;
        self.inner.set_accept_ranges(value).map_err(value_error)
    }

    fn remove_accept_ranges(&mut self) -> PyResult<Option<String>> {
        self.require_mutable()?;
        Ok(self.inner.remove_accept_ranges())
    }

    fn set_cache_control(&mut self, value: String) -> PyResult<()> {
        self.require_mutable()?;
        self.inner.set_cache_control(value).map_err(value_error)
    }

    fn remove_cache_control(&mut self) -> PyResult<Option<String>> {
        self.require_mutable()?;
        Ok(self.inner.remove_cache_control())
    }

    fn set_content_disposition(&mut self, value: String) -> PyResult<()> {
        self.require_mutable()?;
        self.inner
            .set_content_disposition(value)
            .map_err(value_error)
    }

    fn remove_content_disposition(&mut self) -> PyResult<Option<String>> {
        self.require_mutable()?;
        Ok(self.inner.remove_content_disposition())
    }

    fn set_content_encoding(&mut self, value: String) -> PyResult<()> {
        self.require_mutable()?;
        self.inner.set_content_encoding(value).map_err(value_error)
    }

    fn remove_content_encoding(&mut self) -> PyResult<Option<String>> {
        self.require_mutable()?;
        Ok(self.inner.remove_content_encoding())
    }

    fn set_content_language(&mut self, value: String) -> PyResult<()> {
        self.require_mutable()?;
        self.inner.set_content_language(value).map_err(value_error)
    }

    fn remove_content_language(&mut self) -> PyResult<Option<String>> {
        self.require_mutable()?;
        Ok(self.inner.remove_content_language())
    }

    fn set_content_length(&mut self, value: ContentLength) -> PyResult<()> {
        self.require_mutable()?;
        self.inner.set_content_length(value.0);
        Ok(())
    }

    fn remove_content_length(&mut self) -> PyResult<Option<u64>> {
        self.require_mutable()?;
        self.inner.remove_content_length().map_err(value_error)
    }

    fn set_content_location(&mut self, value: String) -> PyResult<()> {
        self.require_mutable()?;
        self.inner.set_content_location(value).map_err(value_error)
    }

    fn remove_content_location(&mut self) -> PyResult<Option<String>> {
        self.require_mutable()?;
        Ok(self.inner.remove_content_location())
    }

    fn set_content_range(&mut self, value: String) -> PyResult<()> {
        self.require_mutable()?;
        self.inner.set_content_range(value).map_err(value_error)
    }

    fn remove_content_range(&mut self) -> PyResult<Option<String>> {
        self.require_mutable()?;
        Ok(self.inner.remove_content_range())
    }

    fn set_content_type(&mut self, value: String) -> PyResult<()> {
        self.require_mutable()?;
        self.inner.set_content_type(value).map_err(value_error)
    }

    fn remove_content_type(&mut self) -> PyResult<Option<String>> {
        self.require_mutable()?;
        Ok(self.inner.remove_content_type())
    }

    fn set_mime_type(&mut self, value: &Bound<'_, PyAny>) -> PyResult<()> {
        self.require_mutable()?;
        self.inner.set_mime_type(core_mime_type_from_value(value)?);
        Ok(())
    }

    fn remove_mime_type(&mut self) -> PyResult<Option<PyMimeType>> {
        self.require_mutable()?;
        self.inner
            .remove_mime_type()
            .map(|value| value.map(PyMimeType::from_core))
            .map_err(value_error)
    }

    fn set_media_type(&mut self, value: &Bound<'_, PyAny>) -> PyResult<()> {
        self.require_mutable()?;
        self.inner
            .set_media_type(core_media_type_from_value(value)?)
            .map_err(value_error)
    }

    fn remove_media_type(&mut self) -> PyResult<Option<PyMediaType>> {
        self.require_mutable()?;
        self.inner
            .remove_media_type()
            .map(|value| value.map(PyMediaType::from_core))
            .map_err(value_error)
    }

    fn set_etag(&mut self, value: String) -> PyResult<()> {
        self.require_mutable()?;
        self.inner.set_etag(value).map_err(value_error)
    }

    fn remove_etag(&mut self) -> PyResult<Option<String>> {
        self.require_mutable()?;
        Ok(self.inner.remove_etag())
    }

    fn set_expires(&mut self, value: String) -> PyResult<()> {
        self.require_mutable()?;
        self.inner.set_expires(value).map_err(value_error)
    }

    fn remove_expires(&mut self) -> PyResult<Option<String>> {
        self.require_mutable()?;
        Ok(self.inner.remove_expires())
    }

    fn set_last_modified(&mut self, value: String) -> PyResult<()> {
        self.require_mutable()?;
        self.inner.set_last_modified(value).map_err(value_error)
    }

    fn remove_last_modified(&mut self) -> PyResult<Option<String>> {
        self.require_mutable()?;
        Ok(self.inner.remove_last_modified())
    }

    fn set_http_location(&mut self, value: &Bound<'_, PyAny>) -> PyResult<()> {
        self.require_mutable()?;
        self.inner.set_http_location(core_url_from_value(value)?);
        Ok(())
    }

    fn remove_http_location(&mut self) -> PyResult<Option<PyUrl>> {
        self.require_mutable()?;
        self.inner
            .remove_http_location()
            .map(|value| value.map(PyUrl::from_core))
            .map_err(value_error)
    }

    fn set_range(&mut self, value: String) -> PyResult<()> {
        self.require_mutable()?;
        self.inner.set_range(value).map_err(value_error)
    }

    fn remove_range(&mut self) -> PyResult<Option<String>> {
        self.require_mutable()?;
        Ok(self.inner.remove_range())
    }

    fn set_vary(&mut self, value: String) -> PyResult<()> {
        self.require_mutable()?;
        self.inner.set_vary(value).map_err(value_error)
    }

    fn remove_vary(&mut self) -> PyResult<Option<String>> {
        self.require_mutable()?;
        Ok(self.inner.remove_vary())
    }

    fn get_property(&self, scheme: &str, name: &str) -> PyResult<Option<&str>> {
        let scheme = CoreScheme::from_str(scheme).map_err(value_error)?;
        Ok(self.inner.get_property(&scheme, name))
    }

    fn has_property(&self, scheme: &str, name: &str) -> PyResult<bool> {
        let scheme = CoreScheme::from_str(scheme).map_err(value_error)?;
        Ok(self.inner.has_property(&scheme, name))
    }

    fn set_property(
        &mut self,
        scheme: &str,
        name: &str,
        value: String,
    ) -> PyResult<Option<String>> {
        self.require_mutable()?;
        let scheme = CoreScheme::from_str(scheme).map_err(value_error)?;
        self.inner
            .set_property(&scheme, name, value)
            .map_err(value_error)
    }

    fn remove_property(&mut self, scheme: &str, name: &str) -> PyResult<Option<String>> {
        self.require_mutable()?;
        let scheme = CoreScheme::from_str(scheme).map_err(value_error)?;
        Ok(self.inner.remove_property(&scheme, name))
    }

    fn property_iter(&self, scheme: &str) -> PyResult<PyFieldPropertyIterator> {
        let scheme = CoreScheme::from_str(scheme).map_err(value_error)?;
        Ok(PyFieldPropertyIterator::new(&self.inner, scheme))
    }

    fn clear_properties(&mut self, scheme: &str) -> PyResult<()> {
        self.require_mutable()?;
        let scheme = CoreScheme::from_str(scheme).map_err(value_error)?;
        self.inner.clear_properties(&scheme);
        Ok(())
    }

    fn __len__(&self) -> usize {
        self.inner.metadata_len()
    }

    fn __iter__(&self) -> PyFieldMetadataIterator {
        PyFieldMetadataIterator::new(&self.inner, MetadataIteratorKind::Keys)
    }

    fn __contains__(&self, key: &Bound<'_, PyAny>) -> bool {
        key.extract::<&str>()
            .is_ok_and(|key| self.inner.has_metadata(key))
    }

    fn __getitem__(&self, key: &str) -> PyResult<String> {
        self.inner
            .get_metadata(key)
            .map(str::to_owned)
            .ok_or_else(|| PyKeyError::new_err(key.to_owned()))
    }

    fn __setitem__(&mut self, key: String, value: String) -> PyResult<()> {
        self.require_mutable()?;
        self.inner
            .insert_metadata(key, value)
            .map(|_| ())
            .map_err(value_error)
    }

    fn __delitem__(&mut self, key: &str) -> PyResult<()> {
        self.require_mutable()?;
        self.inner
            .remove_metadata(key)
            .map(|_| ())
            .ok_or_else(|| PyKeyError::new_err(key.to_owned()))
    }

    #[pyo3(signature = (key, default=None, /))]
    fn get(&self, py: Python<'_>, key: &str, default: Option<Py<PyAny>>) -> Py<PyAny> {
        self.inner.get_metadata(key).map_or_else(
            || default.unwrap_or_else(|| py.None()),
            |value| PyString::new(py, value).into_any().unbind(),
        )
    }

    fn keys(&self) -> PyFieldMetadataIterator {
        PyFieldMetadataIterator::new(&self.inner, MetadataIteratorKind::Keys)
    }

    fn values(&self) -> PyFieldMetadataIterator {
        PyFieldMetadataIterator::new(&self.inner, MetadataIteratorKind::Values)
    }

    fn items(&self) -> PyFieldMetadataIterator {
        PyFieldMetadataIterator::new(&self.inner, MetadataIteratorKind::Items)
    }

    #[pyo3(signature = (values=None, /, **kwargs))]
    fn update(
        &mut self,
        values: Option<&Bound<'_, PyAny>>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        self.require_mutable()?;
        let mut pairs = BTreeMap::new();
        if let Some(values) = values {
            extend_metadata_pairs(values, &mut pairs)?;
        }
        if let Some(kwargs) = kwargs {
            for (key, value) in kwargs.iter() {
                pairs.insert(key.extract()?, value.extract()?);
            }
        }
        self.inner.update_metadata(pairs).map_err(value_error)
    }

    fn clear(&mut self) -> PyResult<()> {
        self.require_mutable()?;
        self.inner.clear_metadata();
        Ok(())
    }

    /// Compare recursively, optionally ignoring metadata on every Field.
    #[pyo3(signature = (other, with_metadata=true))]
    fn equals(&self, other: &Self, with_metadata: bool) -> bool {
        self.inner.equals(&other.inner, with_metadata)
    }

    /// Iterate stable, terminal-readable recursive difference lines.
    #[pyo3(signature = (other, with_metadata=true, return_equal=false))]
    fn show_diffs(
        &self,
        other: &Self,
        with_metadata: bool,
        return_equal: bool,
    ) -> PyDifferenceIterator {
        PyDifferenceIterator::from_fields(&self.inner, &other.inner, with_metadata, return_equal)
    }

    /// Join every recursive difference line.
    ///
    /// ``return_equal`` reports the equal marker instead of an empty
    /// string when the two values match.
    #[pyo3(signature = (other, with_metadata=true, return_equal=true))]
    fn show_diff(&self, other: &Self, with_metadata: bool, return_equal: bool) -> String {
        self.inner
            .show_diff(&other.inner, with_metadata, return_equal)
    }

    /// Makes a cached record schema value immutable at the Python boundary.
    fn _freeze(&mut self) {
        self.read_only = true;
    }

    fn __str__(&self) -> String {
        self.inner.to_string()
    }

    fn __repr__(&self) -> String {
        format!("Field.from_str({:?})", self.inner.to_string())
    }

    fn __richcmp__(&self, other: &Bound<'_, PyAny>, operation: CompareOp) -> PyResult<Py<PyAny>> {
        let Ok(other) = other.extract::<PyRef<'_, Self>>() else {
            return Ok(other.py().NotImplemented());
        };
        Ok(compare(self.inner.cmp(&other.inner), operation)
            .into_pyobject(other.py())?
            .to_owned()
            .into_any()
            .unbind())
    }

    fn __hash__(&self) -> u64 {
        self.inner.stable_layout_hash()
    }

    fn __reduce__(&self, py: Python<'_>) -> PyResult<(Py<PyAny>, (String,))> {
        let callable = py.get_type::<Self>().getattr("from_str")?.unbind();
        Ok((callable, (self.inner.to_string(),)))
    }

    fn __copy__(&self) -> Self {
        self.clone()
    }

    fn __deepcopy__(&self, _memo: &Bound<'_, PyAny>) -> Self {
        self.clone()
    }
}

#[derive(Clone, Copy)]
enum MetadataIteratorKind {
    Keys,
    Values,
    Items,
}

/// Snapshot iterator over a field's sorted metadata.
#[pyclass(module = "yggdryl._native")]
pub(crate) struct PyFieldMetadataIterator {
    inner: CoreField,
    after_key: Option<String>,
    remaining: usize,
    kind: MetadataIteratorKind,
}

/// Snapshot iterator over one protocol's field properties.
#[pyclass(module = "yggdryl._native")]
pub(crate) struct PyFieldPropertyIterator {
    inner: CoreField,
    scheme: CoreScheme,
    after_name: Option<String>,
    remaining: usize,
}

impl PyFieldPropertyIterator {
    fn new(field: &CoreField, scheme: CoreScheme) -> Self {
        let remaining = field.property_iter(&scheme).count();
        Self {
            inner: field.clone(),
            scheme,
            after_name: None,
            remaining,
        }
    }
}

#[pymethods]
impl PyFieldPropertyIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let Some((name, value)) = self
            .inner
            .next_property_entry(&self.scheme, self.after_name.as_deref())
        else {
            self.remaining = 0;
            return Ok(None);
        };
        let name = name.to_owned();
        let output = (name.as_str(), value)
            .into_pyobject(py)?
            .into_any()
            .unbind();
        self.after_name = Some(name);
        self.remaining = self.remaining.saturating_sub(1);
        Ok(Some(output))
    }

    fn __length_hint__(&self) -> usize {
        self.remaining
    }
}

impl PyFieldMetadataIterator {
    fn new(field: &CoreField, kind: MetadataIteratorKind) -> Self {
        Self {
            inner: field.clone(),
            after_key: None,
            remaining: field.metadata_len(),
            kind,
        }
    }
}

#[pymethods]
impl PyFieldMetadataIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let Some((key, value)) = self.inner.next_metadata_entry(self.after_key.as_deref()) else {
            self.remaining = 0;
            return Ok(None);
        };
        let key = key.to_owned();
        let output = match self.kind {
            MetadataIteratorKind::Keys => PyString::new(py, &key).into_any().unbind(),
            MetadataIteratorKind::Values => PyString::new(py, value).into_any().unbind(),
            MetadataIteratorKind::Items => {
                (key.as_str(), value).into_pyobject(py)?.into_any().unbind()
            }
        };
        self.after_key = Some(key);
        self.remaining = self.remaining.saturating_sub(1);
        Ok(Some(output))
    }

    fn __length_hint__(&self) -> usize {
        self.remaining
    }
}
