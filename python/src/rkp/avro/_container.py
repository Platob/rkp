"""Avro object container files with the standard-library block codecs."""

from __future__ import annotations

import bz2
import lzma
import os
import zlib
from collections.abc import Iterable, Iterator, Mapping
from typing import IO, Any, Self

from .._io import read as _read
from .._io import write_bytes as _write_bytes
from ._binary import Reader, _write_long, compile_decoder, compile_encoder
from ._errors import AvroDecodeError, AvroError
from ._schema import AvroSchema, parse_schema, schema_into_json

__all__ = [
    "CODECS",
    "AvroReader",
    "AvroWriter",
    "read_container",
    "write_container",
]

MAGIC = b"Obj\x01"
SYNC_SIZE = 16
DEFAULT_SYNC_INTERVAL = 64 * 1024
CODECS = ("null", "deflate", "bzip2", "xz")

_SCHEMA_KEY = "avro.schema"
_CODEC_KEY = "avro.codec"


def _compress(codec: str, payload: bytes) -> bytes:
    if codec == "null":
        return payload
    if codec == "deflate":
        compressor = zlib.compressobj(9, zlib.DEFLATED, -zlib.MAX_WBITS)
        return compressor.compress(payload) + compressor.flush()
    if codec == "bzip2":
        return bz2.compress(payload)
    if codec == "xz":
        return lzma.compress(payload)
    raise AvroError(
        f"unsupported Avro container codec {codec!r}; supported codecs are "
        + ", ".join(CODECS)
    )


def _decompress(codec: str, payload: bytes) -> bytes:
    if codec == "null":
        return payload
    if codec == "deflate":
        return zlib.decompress(payload, -zlib.MAX_WBITS)
    if codec == "bzip2":
        return bz2.decompress(payload)
    if codec == "xz":
        return lzma.decompress(payload)
    raise AvroError(
        f"unsupported Avro container codec {codec!r}; supported codecs are "
        + ", ".join(CODECS)
    )


class AvroWriter:
    """Incremental object-container writer.

    Values accumulate into one block until ``sync_interval`` bytes are staged.
    Without a stream the encoded file is returned by :meth:`close`, which keeps
    small in-memory documents as convenient as the text codecs.
    """

    __slots__ = (
        "_block",
        "_buffer",
        "_closed",
        "_count",
        "_encode",
        "_metadata",
        "_stream",
        "_sync_interval",
        "codec",
        "schema",
        "sync_marker",
    )

    def __init__(
        self,
        schema: Any,
        *,
        stream: IO[bytes] | None = None,
        codec: str = "null",
        metadata: Mapping[str, Any] | None = None,
        sync_marker: bytes | None = None,
        sync_interval: int = DEFAULT_SYNC_INTERVAL,
    ) -> None:
        if codec not in CODECS:
            raise AvroError(
                f"unsupported Avro container codec {codec!r}; supported codecs are "
                + ", ".join(CODECS)
            )
        if type(sync_interval) is not int or sync_interval <= 0:
            raise ValueError("sync_interval must be a positive integer")
        if sync_marker is not None and len(sync_marker) != SYNC_SIZE:
            raise ValueError(f"sync_marker must be exactly {SYNC_SIZE} bytes")
        self.schema: AvroSchema = parse_schema(schema)
        self.codec = codec
        self.sync_marker = sync_marker if sync_marker is not None else os.urandom(16)
        self._encode = compile_encoder(self.schema)
        self._metadata = dict(metadata or {})
        self._stream = stream
        self._sync_interval = sync_interval
        self._block = bytearray()
        self._count = 0
        self._closed = False
        self._buffer = bytearray()
        self._write(self._header())

    def _header(self) -> bytes:
        from ..json import dumps as _dumps_json

        metadata: dict[str, bytes] = {}
        for key, value in self._metadata.items():
            if key in {_SCHEMA_KEY, _CODEC_KEY}:
                continue
            metadata[str(key)] = (
                value if isinstance(value, bytes) else str(value).encode("utf-8")
            )
        metadata[_SCHEMA_KEY] = _dumps_json(schema_into_json(self.schema)).encode(
            "utf-8"
        )
        metadata[_CODEC_KEY] = self.codec.encode("ascii")

        out = bytearray(MAGIC)
        _write_long(len(metadata), out)
        for key, value in metadata.items():
            encoded_key = key.encode("utf-8")
            _write_long(len(encoded_key), out)
            out += encoded_key
            _write_long(len(value), out)
            out += value
        _write_long(0, out)
        out += self.sync_marker
        return bytes(out)

    def _write(self, payload: bytes) -> None:
        if self._stream is not None:
            self._stream.write(payload)
        else:
            self._buffer += payload

    def append(self, value: Any) -> None:
        """Encode one value into the current block."""

        if self._closed:
            raise AvroError("cannot append to a closed Avro writer")
        self._encode(value, self._block)
        self._count += 1
        if len(self._block) >= self._sync_interval:
            self.flush()

    def extend(self, values: Iterable[Any]) -> None:
        """Encode many values, flushing whenever a block fills."""

        for value in values:
            self.append(value)

    def flush(self) -> None:
        """Write the staged block, if any, followed by the sync marker."""

        if not self._count:
            return
        payload = _compress(self.codec, bytes(self._block))
        out = bytearray()
        _write_long(self._count, out)
        _write_long(len(payload), out)
        out += payload
        out += self.sync_marker
        self._write(bytes(out))
        self._block.clear()
        self._count = 0

    def close(self) -> bytes:
        """Flush the final block and return the encoded file when buffered."""

        if not self._closed:
            self.flush()
            self._closed = True
        return bytes(self._buffer)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exception: object) -> None:
        self.close()


class AvroReader:
    """Iterate the values of an Avro object container file."""

    __slots__ = ("_decode", "_reader", "_sync", "codec", "metadata", "schema")

    def __init__(
        self,
        source: str | bytes | bytearray | memoryview | os.PathLike[str] | IO[bytes],
        *,
        schema: Any = None,
    ) -> None:
        payload = _read(source)
        if isinstance(payload, str):
            payload = payload.encode("utf-8")
        reader = Reader(bytes(payload))
        if reader.read_bytes(4) != MAGIC:
            raise AvroDecodeError("missing Avro object container magic bytes")
        self.metadata = _read_metadata(reader)
        self._sync = reader.read_bytes(SYNC_SIZE)
        declared = self.metadata.get(_SCHEMA_KEY)
        if declared is None:
            raise AvroDecodeError("Avro container metadata has no schema")
        self.schema = parse_schema(
            declared.decode("utf-8") if schema is None else schema
        )
        self.codec = self.metadata.get(_CODEC_KEY, b"null").decode("ascii")
        self._decode = compile_decoder(self.schema)
        self._reader = reader

    @property
    def writer_schema(self) -> AvroSchema:
        """Return the schema declared by the file's own metadata."""

        declared = self.metadata[_SCHEMA_KEY].decode("utf-8")
        return parse_schema(declared)

    def __iter__(self) -> Iterator[Any]:
        reader = self._reader
        decode = self._decode
        while reader.remaining > 0:
            count = reader.read_long()
            size = reader.read_long()
            payload = _decompress(self.codec, reader.read_bytes(size))
            if reader.read_bytes(SYNC_SIZE) != self._sync:
                raise AvroDecodeError("Avro container block sync marker mismatch")
            block = Reader(payload)
            for _ in range(count):
                yield decode(block)


def _read_metadata(reader: Reader) -> dict[str, bytes]:
    metadata: dict[str, bytes] = {}
    while True:
        count = reader.read_long()
        if count == 0:
            return metadata
        if count < 0:
            count = -count
            reader.read_long()
        for _ in range(count):
            key = reader.read_bytes(reader.read_long()).decode("utf-8")
            metadata[key] = reader.read_bytes(reader.read_long())


def write_container(
    destination: str | os.PathLike[str] | IO[bytes],
    schema: Any,
    values: Iterable[Any],
    *,
    codec: str = "null",
    metadata: Mapping[str, Any] | None = None,
    sync_marker: bytes | None = None,
    sync_interval: int = DEFAULT_SYNC_INTERVAL,
) -> bytes | None:
    """Write values as an Avro object container file."""

    writer = AvroWriter(
        schema,
        codec=codec,
        metadata=metadata,
        sync_marker=sync_marker,
        sync_interval=sync_interval,
    )
    writer.extend(values)
    return _write_bytes(destination, writer.close())


def read_container(
    source: str | bytes | bytearray | memoryview | os.PathLike[str] | IO[bytes],
    *,
    schema: Any = None,
) -> AvroReader:
    """Open an Avro object container file for iteration."""

    return AvroReader(source, schema=schema)
