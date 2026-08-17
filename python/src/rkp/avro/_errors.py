"""Errors raised by the dependency-free Avro implementation."""

from __future__ import annotations

__all__ = ["AvroDecodeError", "AvroEncodeError", "AvroError", "AvroSchemaError"]


class AvroError(Exception):
    """Base class for every Avro failure raised by :mod:`rkp.avro`."""


class AvroSchemaError(AvroError, ValueError):
    """An Avro schema is malformed, unknown, or internally inconsistent."""


class AvroEncodeError(AvroError, ValueError):
    """A value cannot be encoded against its declared Avro schema."""


class AvroDecodeError(AvroError, ValueError):
    """Encoded Avro data is truncated or inconsistent with its schema."""
