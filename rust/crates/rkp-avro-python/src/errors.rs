//! The Python exception types the core's errors map onto.

use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyValueError};
use pyo3::prelude::*;

create_exception!(
    _avro,
    AvroError,
    PyException,
    "Base class for every Avro failure raised by rkp.avro."
);
create_exception!(
    _avro,
    AvroSchemaError,
    AvroError,
    "An Avro schema is malformed, unknown, or internally inconsistent."
);
create_exception!(
    _avro,
    AvroEncodeError,
    AvroError,
    "A value cannot be encoded against its declared Avro schema."
);
create_exception!(
    _avro,
    AvroDecodeError,
    AvroError,
    "Encoded Avro data is truncated or inconsistent with its schema."
);

/// Map one core error onto its Python exception.
pub fn to_py(error: rkp_avro::Error) -> PyErr {
    let message = error.message().to_string();
    match error {
        rkp_avro::Error::Schema(_) => AvroSchemaError::new_err(message),
        rkp_avro::Error::Encode(_) => AvroEncodeError::new_err(message),
        rkp_avro::Error::Decode(_) => AvroDecodeError::new_err(message),
        rkp_avro::Error::Container(_) => AvroError::new_err(message),
    }
}

/// Raise a schema error.
pub fn schema_error(message: impl Into<String>) -> PyErr {
    AvroSchemaError::new_err(message.into())
}

/// Raise an encode error.
pub fn encode_error(message: impl Into<String>) -> PyErr {
    AvroEncodeError::new_err(message.into())
}

/// Raise a plain value error, for host-side argument checks.
pub fn value_error(message: impl Into<String>) -> PyErr {
    PyValueError::new_err(message.into())
}

/// Register every exception on the module.
pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = module.py();
    module.add("AvroError", py.get_type::<AvroError>())?;
    module.add("AvroSchemaError", py.get_type::<AvroSchemaError>())?;
    module.add("AvroEncodeError", py.get_type::<AvroEncodeError>())?;
    module.add("AvroDecodeError", py.get_type::<AvroDecodeError>())?;
    Ok(())
}
