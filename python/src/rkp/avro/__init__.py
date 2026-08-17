"""Apache Avro schemas, binary data, and random-access container files.

The implementation is the Rust crate ``rkp-avro``, loaded through the
:mod:`rkp._avro` extension module: it owns schema parsing, canonical form,
fingerprints, the binary and JSON encodings, and object containers addressable
by record index.  This package owns the Python surface — the schema model, the
:class:`Avro` container with its file provenance, and the codec facade — and
adds no format logic of its own.
"""

from __future__ import annotations

import os
from collections.abc import Iterable, Mapping
from typing import IO, Any

from .. import _avro
from .._avro import AvroDecodeError, AvroEncodeError, AvroError, AvroSchemaError
from ._file import (
    CODECS,
    DEFAULT_CACHE_BYTES,
    DEFAULT_SYNC_INTERVAL,
    MAGIC,
    MODES,
    RANDOM_SYNC_INTERVAL,
    SYNC_SIZE,
    Avro,
    AvroBlock,
    read_container,
    write_container,
)
from ._model import (
    PRIMITIVE_NAMES,
    ArraySchema,
    AvroField,
    AvroSchema,
    EnumSchema,
    FixedSchema,
    MapSchema,
    NamedSchema,
    PrimitiveSchema,
    RecordSchema,
    UnionSchema,
    parse_schema,
    schema_into_json,
)

__all__ = [
    "CODECS",
    "DEFAULT_CACHE_BYTES",
    "DEFAULT_SYNC_INTERVAL",
    "MAGIC",
    "MODES",
    "PRIMITIVE_NAMES",
    "RANDOM_SYNC_INTERVAL",
    "SYNC_SIZE",
    "ArraySchema",
    "Avro",
    "AvroBlock",
    "AvroDecodeError",
    "AvroEncodeError",
    "AvroError",
    "AvroField",
    "AvroSchema",
    "AvroSchemaError",
    "EnumSchema",
    "FixedSchema",
    "MapSchema",
    "NamedSchema",
    "PrimitiveSchema",
    "RecordSchema",
    "UnionSchema",
    "canonical_form",
    "compile_decoder",
    "compile_encoder",
    "core_version",
    "decode",
    "decode_single_object",
    "dump",
    "dumps",
    "dumps_schema",
    "encode",
    "encode_into",
    "encode_single_object",
    "fingerprint",
    "fingerprint_bytes",
    "into_json",
    "load",
    "loads",
    "loads_schema",
    "out_of_json",
    "parse_schema",
    "read_container",
    "schema_into_json",
    "write_container",
]


def core_version() -> str:
    """Return the version of the Rust core behind this package."""

    return _avro.core_version()


def canonical_form(schema: Any) -> str:
    """Return the specification's parsing canonical form."""

    return parse_schema(schema).canonical_form()


def fingerprint(schema: Any) -> int:
    """Return the 64-bit CRC-64-AVRO (Rabin) fingerprint of a schema."""

    if isinstance(schema, (bytes, bytearray, memoryview)):
        return _avro.rabin(bytes(schema))
    if isinstance(schema, str) and not schema.strip().startswith(("{", "[", '"')):
        return _avro.rabin(schema.encode("utf-8"))
    if isinstance(schema, str):
        return _avro.rabin(schema.encode("utf-8"))
    return parse_schema(schema).fingerprint()


def fingerprint_bytes(schema: Any) -> bytes:
    """Return the little-endian fingerprint used by single-object encoding."""

    return fingerprint(schema).to_bytes(8, "little")


def dumps_schema(schema: Any, *, indent: int | None = None) -> str:
    """Return one schema's JSON declaration as text."""

    from ..json import dumps as _dumps_json

    return _dumps_json(parse_schema(schema).into_json(), indent=indent)


def loads_schema(data: str | bytes | bytearray | memoryview) -> AvroSchema:
    """Parse one schema from its JSON declaration."""

    return parse_schema(data)


def encode(schema: Any, value: Any) -> bytes:
    """Encode one value into Avro's binary representation."""

    return parse_schema(schema)._rooted().encode(value)


def encode_into(schema: Any, value: Any, out: bytearray) -> bytearray:
    """Append one encoded value to a caller-owned buffer."""

    if not isinstance(out, bytearray):
        raise TypeError("out must be a bytearray")
    out += parse_schema(schema)._rooted().encode(value)
    return out


def decode(schema: Any, data: bytes | bytearray | memoryview) -> Any:
    """Decode one value from Avro's binary representation."""

    return parse_schema(schema)._rooted().decode(bytes(data))


def encode_single_object(schema: Any, value: Any) -> bytes:
    """Encode one value using Avro's single-object framing."""

    return parse_schema(schema)._rooted().encode_single_object(value)


def decode_single_object(schema: Any, data: bytes | bytearray | memoryview) -> Any:
    """Decode single-object framed data, validating its schema fingerprint."""

    return parse_schema(schema)._rooted().decode_single_object(bytes(data))


def compile_encoder(schema: Any) -> Any:
    """Return an encoder bound to one schema, skipping per-call lookup."""

    core = parse_schema(schema)._rooted()

    def encode_value(value: Any, out: bytearray | None = None) -> Any:
        encoded = core.encode(value)
        if out is None:
            return encoded
        out += encoded
        return out

    return encode_value


def compile_decoder(schema: Any) -> Any:
    """Return a decoder bound to one schema, skipping per-call lookup."""

    core = parse_schema(schema)._rooted()

    def decode_value(data: bytes | bytearray | memoryview) -> Any:
        return core.decode(bytes(data))

    return decode_value


def into_json(schema: Any, value: Any) -> Any:
    """Project one value into Avro's JSON encoding as plain Python data."""

    return parse_schema(schema)._rooted().into_json(value)


def out_of_json(schema: Any, value: Any) -> Any:
    """Restore one value from Avro's JSON encoding."""

    return parse_schema(schema)._rooted().out_of_json(value)


def dumps(schema: Any, value: Any, **kwargs: Any) -> str:
    """Encode one value as Avro JSON text."""

    from ..json import dumps as _dumps_json

    return _dumps_json(into_json(schema, value), **kwargs)


def loads(
    schema: Any,
    data: str | bytes | bytearray | memoryview,
    **kwargs: Any,
) -> Any:
    """Decode Avro JSON text into Python values."""

    from ..json import loads as _loads_json

    return out_of_json(schema, _loads_json(data, **kwargs))


def dump(
    destination: str | os.PathLike[str] | IO[bytes],
    schema: Any,
    values: Iterable[Any],
    *,
    codec: str = "null",
    metadata: Mapping[str, Any] | None = None,
    sync_marker: bytes | None = None,
) -> bytes | None:
    """Write values to an Avro object container file."""

    return write_container(
        destination,
        schema,
        values,
        codec=codec,
        metadata=metadata,
        sync_marker=sync_marker,
    )


def load(
    source: str | bytes | bytearray | memoryview | os.PathLike[str] | IO[bytes],
    *,
    schema: Any = None,
) -> list[Any]:
    """Read every value of an Avro object container file."""

    return list(read_container(source, schema=schema))
