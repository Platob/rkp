"""Apache Avro schemas, binary data, and container files without dependencies.

The package mirrors :mod:`rkp.json` and :mod:`rkp.yaml`: schemas parse into an
immutable model, codecs compile against that model once, and nothing here
imports PyArrow.  :mod:`rkp.records.avro` bridges the model to records, Arrow,
and Iceberg.
"""

from __future__ import annotations

import os
from collections.abc import Iterable, Mapping
from typing import IO, Any

from ._binary import (
    Reader,
    compile_decoder,
    compile_encoder,
    decode,
    decode_single_object,
    encode,
    encode_into,
    encode_single_object,
)
from ._container import (
    CODECS,
    AvroReader,
    AvroWriter,
    read_container,
    write_container,
)
from ._errors import AvroDecodeError, AvroEncodeError, AvroError, AvroSchemaError
from ._json import (
    compile_json_decoder,
    compile_json_encoder,
    into_json,
    out_of_json,
)
from ._json import dumps as _dumps_avro_json
from ._json import loads as _loads_avro_json
from ._schema import (
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
    canonical_form,
    fingerprint,
    fingerprint_bytes,
    parse_schema,
    schema_into_json,
)

__all__ = [
    "CODECS",
    "ArraySchema",
    "AvroDecodeError",
    "AvroEncodeError",
    "AvroError",
    "AvroField",
    "AvroReader",
    "AvroSchema",
    "AvroSchemaError",
    "AvroWriter",
    "EnumSchema",
    "FixedSchema",
    "MapSchema",
    "NamedSchema",
    "PrimitiveSchema",
    "Reader",
    "RecordSchema",
    "UnionSchema",
    "canonical_form",
    "compile_decoder",
    "compile_encoder",
    "compile_json_decoder",
    "compile_json_encoder",
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


def dumps(schema: Any, value: Any, **kwargs: Any) -> str:
    """Encode one value as Avro JSON text."""

    return _dumps_avro_json(schema, value, **kwargs)


def loads(
    schema: Any,
    data: str | bytes | bytearray | memoryview,
    **kwargs: Any,
) -> Any:
    """Decode Avro JSON text into Python values."""

    return _loads_avro_json(schema, data, **kwargs)


def dumps_schema(schema: Any, *, indent: int | None = None) -> str:
    """Return one schema's JSON declaration as text."""

    from ..json import dumps as _dumps_json

    return _dumps_json(schema_into_json(parse_schema(schema)), indent=indent)


def loads_schema(data: str | bytes | bytearray | memoryview) -> AvroSchema:
    """Parse one schema from its JSON declaration."""

    return parse_schema(data)


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
