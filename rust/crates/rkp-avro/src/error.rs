//! One error type for every layer of the Avro core.

use std::fmt;

/// What went wrong, kept coarse so hosts can map each kind to their own type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// A schema is malformed, unknown, or internally inconsistent.
    Schema(String),
    /// A value cannot be encoded against its declared schema.
    Encode(String),
    /// Encoded data is truncated or inconsistent with its schema.
    Decode(String),
    /// A container operation is impossible or unsafe.
    Container(String),
}

impl Error {
    /// Return the message without its kind.
    pub fn message(&self) -> &str {
        match self {
            Error::Schema(message)
            | Error::Encode(message)
            | Error::Decode(message)
            | Error::Container(message) => message,
        }
    }

    /// Return a stable machine-readable kind, used by host bindings.
    pub fn kind(&self) -> &'static str {
        match self {
            Error::Schema(_) => "schema",
            Error::Encode(_) => "encode",
            Error::Decode(_) => "decode",
            Error::Container(_) => "container",
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message())
    }
}

impl std::error::Error for Error {}

/// The crate's result alias.
pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn schema<T>(message: impl Into<String>) -> Result<T> {
    Err(Error::Schema(message.into()))
}

pub(crate) fn encode<T>(message: impl Into<String>) -> Result<T> {
    Err(Error::Encode(message.into()))
}

pub(crate) fn decode<T>(message: impl Into<String>) -> Result<T> {
    Err(Error::Decode(message.into()))
}

pub(crate) fn container<T>(message: impl Into<String>) -> Result<T> {
    Err(Error::Container(message.into()))
}
