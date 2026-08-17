//! The Python extension module behind :mod:`rkp.avro`.
//!
//! The Rust core owns the Avro format; this crate owns the translation between
//! that core and Python objects.  File provenance, the public class model, and
//! the codec facade stay in Python, where the rest of `rkp` already lives.

mod convert;
mod errors;

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};

use rkp_avro::container::{self, Container};
use rkp_avro::schema::{Kind, Logical, Schema as CoreSchema};
use rkp_avro::{Value, binary, json};

use convert::{into_python, into_value, json_to_python, python_to_json};
use errors::{to_py, value_error};

/// One record field, as the Python model sees it.
#[pyclass(
    name = "FieldInfo",
    module = "rkp._avro",
    frozen,
    get_all,
    skip_from_py_object
)]
#[derive(Clone)]
struct FieldInfo {
    name: String,
    node: usize,
    default: Option<String>,
    doc: Option<String>,
    order: Option<String>,
    aliases: Vec<String>,
    attributes: String,
}

/// One schema node, as the Python model sees it.
#[pyclass(
    name = "NodeInfo",
    module = "rkp._avro",
    frozen,
    get_all,
    skip_from_py_object
)]
#[derive(Clone)]
struct NodeInfo {
    index: usize,
    kind: String,
    logical: Option<String>,
    precision: Option<u32>,
    scale: Option<u32>,
    name: Option<String>,
    namespace: Option<String>,
    fullname: String,
    doc: Option<String>,
    aliases: Vec<String>,
    attributes: String,
    fields: Vec<FieldInfo>,
    symbols: Vec<String>,
    enum_default: Option<String>,
    size: usize,
    children: Vec<usize>,
    is_error: bool,
}

/// A parsed Avro schema.
#[pyclass(name = "Schema", module = "rkp._avro", frozen)]
struct PySchema {
    inner: CoreSchema,
}

#[pymethods]
impl PySchema {
    /// Parse a schema from its JSON declaration.
    #[staticmethod]
    fn parse(text: &str) -> PyResult<PySchema> {
        CoreSchema::parse_str(text)
            .map(|inner| PySchema { inner })
            .map_err(to_py)
    }

    /// Return the schema's JSON declaration as text.
    fn json(&self) -> String {
        self.inner.to_json_string()
    }

    /// Return the specification's parsing canonical form.
    fn canonical_form(&self) -> &str {
        self.inner.canonical_form()
    }

    /// Return the 64-bit Rabin fingerprint of the canonical form.
    fn fingerprint(&self) -> u64 {
        self.inner.fingerprint()
    }

    /// Return the index of the root node.
    fn root(&self) -> usize {
        self.inner.root()
    }

    /// Return how many nodes the schema holds.
    fn node_count(&self) -> usize {
        self.inner.nodes().len()
    }

    /// Return one node's description.
    fn node(&self, index: usize) -> PyResult<NodeInfo> {
        if index >= self.inner.nodes().len() {
            return Err(value_error(format!("schema has no node {index}")));
        }
        let node = self.inner.node(index);
        let (logical, precision, scale) = match &node.logical {
            Some(Logical::Decimal { precision, scale }) => {
                (Some("decimal".to_string()), Some(*precision), Some(*scale))
            }
            Some(logical) => (Some(logical.name().to_string()), None, None),
            None => (None, None, None),
        };
        Ok(NodeInfo {
            index,
            kind: node.kind.name().to_string(),
            logical,
            precision,
            scale,
            name: node.name.clone(),
            namespace: node.namespace.clone(),
            fullname: node.fullname(),
            doc: node.doc.clone(),
            aliases: node.aliases.clone(),
            attributes: serde_json::Value::Object(node.attributes.clone()).to_string(),
            fields: node
                .fields
                .iter()
                .map(|field| FieldInfo {
                    name: field.name.clone(),
                    node: field.node,
                    default: field.default.as_ref().map(ToString::to_string),
                    doc: field.doc.clone(),
                    order: field.order.clone(),
                    aliases: field.aliases.clone(),
                    attributes: serde_json::Value::Object(field.attributes.clone()).to_string(),
                })
                .collect(),
            symbols: node.symbols.clone(),
            enum_default: node.enum_default.clone(),
            size: node.size,
            children: node.children.clone(),
            is_error: node.is_error,
        })
    }

    /// Return a schema rooted at one of this schema's nodes.
    fn subschema(&self, index: usize) -> PyResult<PySchema> {
        if index >= self.inner.nodes().len() {
            return Err(value_error(format!("schema has no node {index}")));
        }
        Ok(PySchema {
            inner: self.inner.subschema(index),
        })
    }

    /// Encode one value into Avro's binary representation.
    fn encode<'py>(
        &self,
        py: Python<'py>,
        value: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let converted = into_value(&self.inner, self.inner.root(), value)?;
        let mut out = Vec::with_capacity(64);
        binary::encode(&self.inner, &converted, &mut out).map_err(to_py)?;
        Ok(PyBytes::new(py, &out))
    }

    /// Decode one value from Avro's binary representation.
    fn decode(&self, py: Python<'_>, data: &[u8]) -> PyResult<Py<PyAny>> {
        let value = binary::decode(&self.inner, data).map_err(to_py)?;
        into_python(py, &self.inner, self.inner.root(), &value)
    }

    /// Encode one value with Avro's single-object framing.
    fn encode_single_object<'py>(
        &self,
        py: Python<'py>,
        value: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let converted = into_value(&self.inner, self.inner.root(), value)?;
        let framed = rkp_avro::encode_single_object(&self.inner, &converted).map_err(to_py)?;
        Ok(PyBytes::new(py, &framed))
    }

    /// Decode single-object framed data, validating its fingerprint.
    fn decode_single_object(&self, py: Python<'_>, data: &[u8]) -> PyResult<Py<PyAny>> {
        let value = rkp_avro::decode_single_object(&self.inner, data).map_err(to_py)?;
        into_python(py, &self.inner, self.inner.root(), &value)
    }

    /// Project one value into Avro's JSON encoding, as plain Python data.
    // `into_` names a conversion throughout `rkp`, and this method is the one
    // `rkp.avro.into_json` calls, so it keeps the Python name rather than the
    // Rust convention for a method that borrows.
    #[allow(clippy::wrong_self_convention)]
    fn into_json(&self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let converted = into_value(&self.inner, self.inner.root(), value)?;
        let encoded = json::to_json(&self.inner, self.inner.root(), &converted).map_err(to_py)?;
        json_to_python(py, &encoded)
    }

    /// Restore one value from Avro's JSON encoding.
    fn out_of_json(&self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let decoded = python_to_json(value)?;
        let restored = json::from_json(&self.inner, self.inner.root(), &decoded).map_err(to_py)?;
        into_python(py, &self.inner, self.inner.root(), &restored)
    }

    fn __eq__(&self, other: &PySchema) -> bool {
        self.inner == other.inner
    }

    fn __hash__(&self) -> u64 {
        self.inner.fingerprint()
    }

    fn __repr__(&self) -> String {
        format!("<rkp._avro.Schema {}>", self.inner.canonical_form())
    }
}

/// One container block's framing.
#[pyclass(
    name = "BlockInfo",
    module = "rkp._avro",
    frozen,
    get_all,
    skip_from_py_object
)]
#[derive(Clone)]
struct BlockInfo {
    ordinal: usize,
    offset: usize,
    data_offset: usize,
    size: usize,
    count: usize,
    first: usize,
}

/// One Avro object container, addressable by record index.
#[pyclass(name = "Container", module = "rkp._avro")]
struct PyContainer {
    inner: Container,
    schema: CoreSchema,
}

#[pymethods]
impl PyContainer {
    /// Create an empty container and its header.
    #[staticmethod]
    #[pyo3(signature = (schema, codec, metadata, sync_marker, sync_interval))]
    fn create(
        schema: &PySchema,
        codec: &str,
        metadata: Vec<(String, Vec<u8>)>,
        sync_marker: &[u8],
        sync_interval: usize,
    ) -> PyResult<PyContainer> {
        let marker = marker_of(sync_marker)?;
        let inner = Container::create(
            schema.inner.clone(),
            codec,
            &metadata,
            marker,
            sync_interval,
        )
        .map_err(to_py)?;
        Ok(PyContainer {
            schema: schema.inner.clone(),
            inner,
        })
    }

    /// Open an existing container image.
    #[staticmethod]
    #[pyo3(signature = (data, sync_interval, cache_bytes))]
    fn open(data: Vec<u8>, sync_interval: usize, cache_bytes: usize) -> PyResult<PyContainer> {
        let inner = Container::open(data, sync_interval, cache_bytes).map_err(to_py)?;
        let schema = inner.schema().clone();
        Ok(PyContainer { inner, schema })
    }

    /// Open a container file by mapping it rather than reading it.
    #[staticmethod]
    #[pyo3(signature = (path, sync_interval, cache_bytes))]
    fn open_path(path: &str, sync_interval: usize, cache_bytes: usize) -> PyResult<PyContainer> {
        // Map here rather than in the core so a filesystem failure keeps its
        // errno and reaches Python as the OSError subclass it always was.
        let image = rkp_avro::Image::map(std::path::Path::new(path))?;
        let inner = Container::open_image(image, sync_interval, cache_bytes).map_err(to_py)?;
        let schema = inner.schema().clone();
        Ok(PyContainer { inner, schema })
    }

    /// Copy a mapped image into owned memory and release the file.
    fn detach(&mut self) {
        self.inner.detach();
    }

    /// Return whether this container reads from a mapped file.
    #[getter]
    fn is_mapped(&self) -> bool {
        self.inner.is_mapped()
    }

    /// Return the container's writer schema.
    fn schema(&self) -> PySchema {
        PySchema {
            inner: self.schema.clone(),
        }
    }

    /// Return the block codec name.
    fn codec(&self) -> &str {
        self.inner.codec()
    }

    /// Return the header metadata as a name to bytes mapping.
    fn metadata<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let mapping = PyDict::new(py);
        for (key, value) in self.inner.metadata() {
            mapping.set_item(key, PyBytes::new(py, value))?;
        }
        Ok(mapping)
    }

    /// Return the file's sync marker.
    fn sync_marker<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.inner.sync_marker())
    }

    /// Return the staged-bytes threshold that closes a block.
    fn sync_interval(&self) -> usize {
        self.inner.sync_interval()
    }

    /// Set the staged-bytes threshold used when framing new blocks.
    fn set_sync_interval(&mut self, sync_interval: usize) -> PyResult<()> {
        self.inner.set_sync_interval(sync_interval).map_err(to_py)
    }

    fn __len__(&mut self) -> usize {
        self.inner.len()
    }

    /// Decode one record by index.
    fn get(&mut self, py: Python<'_>, index: usize) -> PyResult<Py<PyAny>> {
        let value = self.inner.get(index).map_err(to_py)?;
        into_python(py, &self.schema, self.schema.root(), &value)
    }

    /// Decode a half-open record range.
    fn range(&mut self, py: Python<'_>, start: usize, stop: usize) -> PyResult<Py<PyAny>> {
        let values = self.inner.range(start, stop).map_err(to_py)?;
        let list = PyList::empty(py);
        for value in &values {
            list.append(into_python(py, &self.schema, self.schema.root(), value)?)?;
        }
        Ok(list.into_any().unbind())
    }

    /// Decode every record of one block.
    fn read_block(&mut self, py: Python<'_>, ordinal: usize) -> PyResult<Py<PyAny>> {
        let values = self.inner.read_block(ordinal).map_err(to_py)?;
        let list = PyList::empty(py);
        for value in &values {
            list.append(into_python(py, &self.schema, self.schema.root(), value)?)?;
        }
        Ok(list.into_any().unbind())
    }

    /// Encode one record onto the end of the container.
    fn append(&mut self, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let converted = into_value(&self.schema, self.schema.root(), value)?;
        self.inner.append(&converted).map_err(to_py)
    }

    /// Replace one record.
    fn set(&mut self, index: usize, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let converted = into_value(&self.schema, self.schema.root(), value)?;
        self.inner.set(index, converted).map_err(to_py)
    }

    /// Replace the records in ``[start, stop)``.
    fn splice(&mut self, start: usize, stop: usize, values: &Bound<'_, PyAny>) -> PyResult<()> {
        let mut converted = Vec::new();
        for item in values.try_iter()? {
            converted.push(into_value(&self.schema, self.schema.root(), &item?)?);
        }
        self.inner.splice(start, stop, converted).map_err(to_py)
    }

    /// Return every block's framing.
    fn blocks(&mut self) -> PyResult<Vec<BlockInfo>> {
        Ok(self
            .inner
            .blocks()
            .map_err(to_py)?
            .into_iter()
            .map(block_info)
            .collect())
    }

    /// Return the block holding one record index.
    fn block_of(&mut self, index: usize) -> PyResult<BlockInfo> {
        self.inner.block_of(index).map(block_info).map_err(to_py)
    }

    /// Re-frame every block at the current sync interval.
    fn compact(&mut self) -> PyResult<()> {
        self.inner.compact().map_err(to_py)
    }

    /// Apply every pending change and return the container image.
    fn image<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let image = self.inner.image().map_err(to_py)?;
        Ok(PyBytes::new(py, image))
    }

    /// Return the bytes framed since the given durable length.
    ///
    /// A host appending to a file writes exactly this, so an append never
    /// materializes the file it appends to.
    fn tail<'py>(&mut self, py: Python<'py>, persisted: usize) -> PyResult<Bound<'py, PyBytes>> {
        let tail = self.inner.tail(persisted).map_err(to_py)?;
        Ok(PyBytes::new(py, tail))
    }

    /// Return whether changes are staged.
    #[getter]
    fn dirty(&self) -> bool {
        self.inner.dirty()
    }

    /// Return whether persisting requires a rewrite rather than an append.
    #[getter]
    fn needs_rewrite(&self) -> bool {
        self.inner.needs_rewrite()
    }

    /// Return how many bytes are already framed in the image.
    #[getter]
    fn framed_len(&self) -> usize {
        self.inner.framed_len()
    }

    /// Return how many leading bytes have not moved since the last write-out.
    #[getter]
    fn stable(&self) -> usize {
        self.inner.stable()
    }

    /// Record that the whole image is durable, so appends resume.
    fn mark_persisted(&mut self) {
        self.inner.mark_persisted();
    }

    /// Return the structural generation, bumped by every change.
    #[getter]
    fn generation(&self) -> u64 {
        self.inner.generation()
    }

    /// Return the resident size of the image, index, and payload cache.
    #[getter]
    fn nbytes(&self) -> usize {
        self.inner.nbytes()
    }
}

fn block_info(block: container::Block) -> BlockInfo {
    BlockInfo {
        ordinal: block.ordinal,
        offset: block.offset,
        data_offset: block.data_offset,
        size: block.size,
        count: block.count,
        first: block.first,
    }
}

fn marker_of(raw: &[u8]) -> PyResult<[u8; container::SYNC_SIZE]> {
    if raw.len() != container::SYNC_SIZE {
        return Err(value_error(format!(
            "sync_marker must be exactly {} bytes",
            container::SYNC_SIZE
        )));
    }
    let mut marker = [0u8; container::SYNC_SIZE];
    marker.copy_from_slice(raw);
    Ok(marker)
}

/// Return the Rabin fingerprint of arbitrary bytes.
#[pyfunction]
fn rabin(payload: &[u8]) -> u64 {
    rkp_avro::rabin(payload)
}

/// Return the core's version, for diagnostics.
#[pyfunction]
fn core_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Return every kind name the schema model uses.
#[pyfunction]
fn kinds<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
    let names = [
        Kind::Null,
        Kind::Boolean,
        Kind::Int,
        Kind::Long,
        Kind::Float,
        Kind::Double,
        Kind::Bytes,
        Kind::String,
        Kind::Record,
        Kind::Enum,
        Kind::Fixed,
        Kind::Array,
        Kind::Map,
        Kind::Union,
    ]
    .map(|kind| kind.name());
    PyTuple::new(py, names)
}

#[pymodule]
fn _avro(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PySchema>()?;
    module.add_class::<PyContainer>()?;
    module.add_class::<NodeInfo>()?;
    module.add_class::<FieldInfo>()?;
    module.add_class::<BlockInfo>()?;
    module.add_function(wrap_pyfunction!(rabin, module)?)?;
    module.add_function(wrap_pyfunction!(core_version, module)?)?;
    module.add_function(wrap_pyfunction!(kinds, module)?)?;
    module.add("CODECS", PyTuple::new(module.py(), container::CODECS)?)?;
    module.add("SYNC_SIZE", container::SYNC_SIZE)?;
    module.add("DEFAULT_SYNC_INTERVAL", container::DEFAULT_SYNC_INTERVAL)?;
    module.add("RANDOM_SYNC_INTERVAL", container::RANDOM_SYNC_INTERVAL)?;
    module.add("DEFAULT_CACHE_BYTES", container::DEFAULT_CACHE_BYTES)?;
    module.add("MAGIC", PyBytes::new(module.py(), &container::MAGIC))?;
    errors::register(module)?;
    Ok(())
}

/// Re-exported for the container facade, which needs a value-shaped default.
#[allow(dead_code)]
fn unused(value: &Value) -> &'static str {
    value.type_name()
}
