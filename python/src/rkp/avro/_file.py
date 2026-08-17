"""The container facade: provenance, modes, and persistence around the core.

The Rust core owns the container format and its random access; this module owns
what Python users expect from a file object — paths, streams, in-memory
buffers, open modes, slicing, and iteration.
"""

from __future__ import annotations

import os
from collections.abc import Iterable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import IO, Any, NamedTuple, Self

from .. import _avro
from .._io import is_path_string
from .._io import read as _read
from .._io import write_bytes as _write_bytes
from ._model import AvroSchema, parse_schema

__all__ = [
    "CODECS",
    "DEFAULT_CACHE_BYTES",
    "DEFAULT_SYNC_INTERVAL",
    "MODES",
    "RANDOM_SYNC_INTERVAL",
    "SYNC_SIZE",
    "Avro",
    "AvroBlock",
    "read_container",
    "write_container",
]

CODECS: tuple[str, ...] = tuple(_avro.CODECS)
MODES: tuple[str, ...] = ("r", "r+", "a", "w")
SYNC_SIZE: int = _avro.SYNC_SIZE
DEFAULT_SYNC_INTERVAL: int = _avro.DEFAULT_SYNC_INTERVAL
RANDOM_SYNC_INTERVAL: int = _avro.RANDOM_SYNC_INTERVAL
DEFAULT_CACHE_BYTES: int = _avro.DEFAULT_CACHE_BYTES
MAGIC: bytes = _avro.MAGIC


class AvroBlock(NamedTuple):
    """One data block's framing, located without decompressing its payload."""

    ordinal: int
    offset: int
    data_offset: int
    size: int
    # The specification calls this field the block's object count, so it keeps
    # that name and shadows the ``tuple.count`` method it inherits.
    count: int  # type: ignore[assignment]
    first: int

    @property
    def stop(self) -> int:
        """Return the global index just past this block's last record."""

        return self.first + self.count

    @property
    def end(self) -> int:
        """Return the byte offset just past this block's sync marker."""

        return self.data_offset + self.size + SYNC_SIZE


class Avro:
    """One Avro object container opened for reading and random writing.

    Records are addressable by index: ``container[7]`` decodes one record
    without scanning the file, and ``container[7] = row`` rewrites only the
    block that holds it.  Edits buffer per block and are applied in one pass by
    :meth:`flush`, so scattered random writes cost one rewrite rather than one
    each.
    """

    __slots__ = (
        "_closed",
        "_core",
        "_mode",
        "_origin",
        "_path",
        "_persisted",
        "_stream",
    )

    def __init__(
        self,
        source: str
        | bytes
        | bytearray
        | memoryview
        | os.PathLike[str]
        | IO[bytes]
        | None = None,
        *,
        mode: str = "r",
        schema: Any = None,
        codec: str = "null",
        metadata: Mapping[str, Any] | None = None,
        sync_marker: bytes | None = None,
        sync_interval: int = DEFAULT_SYNC_INTERVAL,
        cache_bytes: int = DEFAULT_CACHE_BYTES,
    ) -> None:
        """Open an Avro object container for reading, appending, or writing."""

        if mode not in MODES:
            raise ValueError("mode must be 'r', 'r+', 'a', or 'w'")
        if type(sync_interval) is not int or sync_interval <= 0:
            raise ValueError("sync_interval must be a positive integer")
        if type(cache_bytes) is not int or cache_bytes < 0:
            raise ValueError("cache_bytes must be a non-negative integer")
        if mode != "w" and any(item is not None for item in (metadata, sync_marker)):
            raise ValueError(
                "metadata and sync_marker describe a new container; "
                "they cannot be set when opening an existing one"
            )
        if mode != "w" and codec != "null":
            raise ValueError(
                "codec describes a new container; an existing container "
                "declares its own codec in the file header"
            )

        self._mode = mode
        self._closed = False
        if mode == "w":
            if schema is None:
                raise ValueError("creating an Avro container requires a schema")
            if codec not in CODECS:
                raise _avro.AvroError(
                    f"unsupported Avro container codec {codec!r}; supported "
                    "codecs are " + ", ".join(CODECS)
                )
            if sync_marker is not None and len(sync_marker) != SYNC_SIZE:
                raise ValueError(f"sync_marker must be exactly {SYNC_SIZE} bytes")
            self._path, self._stream, self._origin = _resolve_destination(source)
            self._core = _avro.Container.create(
                parse_schema(schema)._rooted(),
                codec,
                [
                    (
                        str(key),
                        value if isinstance(value, bytes) else str(value).encode(),
                    )
                    for key, value in (metadata or {}).items()
                ],
                sync_marker if sync_marker is not None else os.urandom(SYNC_SIZE),
                sync_interval,
            )
            self._persisted = 0
        else:
            if source is None:
                raise ValueError("opening an Avro container requires a source")
            self._path, self._stream, self._origin = _resolve_source(source)
            payload = _read(source)
            if isinstance(payload, str):
                payload = payload.encode("utf-8")
            image = bytes(payload)
            if (
                not image.startswith(MAGIC)
                and self._origin == "memory"
                and isinstance(source, str)
            ):
                raise _avro.AvroDecodeError(
                    "missing Avro object container magic bytes; a separator-free "
                    "string is a buffer, not a path, so pass Path(...) for a file name"
                )
            self._core = _avro.Container.open(image, sync_interval, cache_bytes)
            self._persisted = len(image)
            if schema is not None and parse_schema(schema) != self.schema:
                raise _avro.AvroDecodeError(
                    "the requested Avro schema does not match the container's "
                    "own writer schema"
                )

    @classmethod
    def create(
        cls,
        schema: Any,
        destination: str | os.PathLike[str] | IO[bytes] | None = None,
        *,
        codec: str = "null",
        metadata: Mapping[str, Any] | None = None,
        sync_marker: bytes | None = None,
        sync_interval: int = DEFAULT_SYNC_INTERVAL,
    ) -> Self:
        """Open a new, empty container writing to ``destination``."""

        return cls(
            destination,
            mode="w",
            schema=schema,
            codec=codec,
            metadata=metadata,
            sync_marker=sync_marker,
            sync_interval=sync_interval,
        )

    @property
    def schema(self) -> AvroSchema:
        """Return the container's writer schema."""

        core = self._core.schema()
        return parse_schema(core.json())

    @property
    def writer_schema(self) -> AvroSchema:
        """Return the schema declared by the container itself."""

        return self.schema

    @property
    def codec(self) -> str:
        """Return the block codec name."""

        return self._core.codec()

    @property
    def metadata(self) -> Mapping[str, bytes]:
        """Return the container's header metadata."""

        return self._core.metadata()

    @property
    def sync_marker(self) -> bytes:
        """Return the file's sync marker."""

        return self._core.sync_marker()

    @property
    def sync_interval(self) -> int:
        """Return the staged-bytes threshold that closes a block."""

        return self._core.sync_interval()

    @property
    def mode(self) -> str:
        """Return the mode the container was opened with."""

        return self._mode

    @property
    def path(self) -> Path | None:
        """Return the file this container reads and writes, when it has one."""

        return self._path

    @property
    def closed(self) -> bool:
        """Return whether the container has been closed."""

        return self._closed

    @property
    def writable(self) -> bool:
        """Return whether records may be replaced, inserted, or deleted."""

        return self._mode in {"r+", "w"}

    @property
    def appendable(self) -> bool:
        """Return whether records may be appended."""

        return self._mode in {"r+", "a", "w"}

    @property
    def dirty(self) -> bool:
        """Return whether changes are staged but not yet written out."""

        return bool(self._core.dirty) or self._persisted != self._core.framed_len

    @property
    def nbytes(self) -> int:
        """Return the resident size of the image, index, and payload cache."""

        return self._core.nbytes

    def __len__(self) -> int:
        """Return the number of records, including any staged appends."""

        return len(self._core)

    def __getitem__(self, index: int | slice) -> Any:
        """Decode one record by index, or a list of records for a slice."""

        if isinstance(index, slice):
            start, stop, step = index.indices(len(self))
            if step != 1:
                raise ValueError("Avro containers only support contiguous slices")
            return self._core.range(start, max(start, stop))
        return self._core.get(self._resolve(index))

    def __setitem__(self, index: int | slice, value: Any) -> None:
        """Replace one record, or a contiguous run of records."""

        self._require_writable("replace records in")
        if isinstance(index, slice):
            start, stop, step = index.indices(len(self))
            if step != 1:
                raise ValueError("Avro containers only support contiguous slices")
            self._core.splice(start, max(start, stop), list(value))
            return
        self._core.set(self._resolve(index), value)

    def __delitem__(self, index: int | slice) -> None:
        """Delete one record, or a contiguous run of records."""

        self._require_writable("delete records from")
        if isinstance(index, slice):
            start, stop, step = index.indices(len(self))
            if step != 1:
                raise ValueError("Avro containers only support contiguous slices")
            self._core.splice(start, max(start, stop), [])
            return
        position = self._resolve(index)
        self._core.splice(position, position + 1, [])

    def __iter__(self) -> Iterator[Any]:
        """Stream every record, without disturbing the random-access cache."""

        generation = self._core.generation
        position = 0
        while position < len(self):
            block = self._core.range(position, position + 1024)
            if not block:
                return
            for record in block:
                if generation != self._core.generation:
                    raise RuntimeError("Avro container changed during iteration")
                yield record
            position += len(block)

    def get(self, index: int, default: Any = None) -> Any:
        """Return one record, or ``default`` when the index is out of range."""

        try:
            position = self._resolve(index)
        except IndexError:
            return default
        return self._core.get(position)

    def iter_from(self, start: int = 0, stop: int | None = None) -> Iterator[Any]:
        """Stream a half-open record range, seeking to its first block."""

        total = len(self)
        first = max(0, min(total + start if start < 0 else start, total))
        last = total if stop is None else (total + stop if stop < 0 else stop)
        last = max(first, min(last, total))
        return iter(self._core.range(first, last))

    def blocks(self) -> tuple[AvroBlock, ...]:
        """Return every block's framing, located without decompressing it."""

        return tuple(_block(item) for item in self._core.blocks())

    def block_of(self, index: int) -> AvroBlock:
        """Return the block that holds one record index."""

        return _block(self._core.block_of(self._resolve(index)))

    def read_block(self, ordinal: int) -> list[Any]:
        """Decode one whole block."""

        return self._core.read_block(ordinal)

    def iter_blocks(self) -> Iterator[tuple[AvroBlock, list[Any]]]:
        """Stream every block with its decoded records."""

        for block in self.blocks():
            yield block, self.read_block(block.ordinal)

    def append(self, value: Any) -> None:
        """Encode one record onto the end of the container."""

        self._require_appendable()
        self._core.append(value)

    def extend(self, values: Iterable[Any]) -> None:
        """Encode many records onto the end, framing whenever a block fills."""

        self._require_appendable()
        append = self._core.append
        for value in values:
            append(value)

    def insert(self, index: int, value: Any) -> None:
        """Insert one record before ``index``, renumbering the records after it."""

        self._require_writable("insert records into")
        total = len(self)
        position = max(0, min(total + index if index < 0 else index, total))
        if position == total:
            self._core.append(value)
            return
        self._core.splice(position, position, [value])

    def pop(self, index: int = -1) -> Any:
        """Remove and return one record."""

        self._require_writable("remove records from")
        position = self._resolve(index)
        value = self._core.get(position)
        self._core.splice(position, position + 1, [])
        return value

    def clear(self) -> None:
        """Drop every record, keeping the header, schema, codec, and marker."""

        self._require_writable("clear")
        self.truncate(0)

    def truncate(self, index: int | None = None) -> int:
        """Drop every record at or after ``index``; return the new length."""

        self._require_writable("truncate")
        total = len(self)
        position = total if index is None else (total + index if index < 0 else index)
        position = max(0, min(position, total))
        if position < total:
            self._core.splice(position, total, [])
        return len(self)

    def compact(self) -> None:
        """Re-frame every block at ``sync_interval``, dropping fragmentation."""

        self._require_writable("compact")
        self._core.compact()

    def flush(self) -> None:
        """Materialize pending changes and write them to the container target."""

        if self._closed:
            raise _avro.AvroError("cannot flush a closed Avro container")
        if self._mode == "r":
            raise _avro.AvroError(
                "this Avro container is open for reading; "
                "open it with mode='r+' to write"
            )
        self._persist()

    def save(self, destination: str | os.PathLike[str] | IO[bytes]) -> bytes | None:
        """Materialize and write the whole image elsewhere."""

        return _write_bytes(destination, self.into_bytes())

    def into_bytes(self) -> bytes:
        """Return the materialized container image without writing it."""

        return self._core.image()

    def close(self) -> bytes | None:
        """Flush, release the container, and return the image when buffered."""

        if not self._closed:
            if self._mode != "r":
                self._persist()
            self._closed = True
        return self._core.image() if self._origin == "memory" else None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exception: object) -> None:
        self.close()

    def __repr__(self) -> str:
        target = self._path if self._path is not None else self._origin
        return (
            f"<Avro {self.schema.fullname!r} mode={self._mode!r} "
            f"codec={self.codec!r} target={target!r}>"
        )

    def _resolve(self, index: int) -> int:
        if type(index) is not int:
            raise TypeError("Avro record indices must be integers")
        total = len(self)
        position = total + index if index < 0 else index
        if not 0 <= position < total:
            raise IndexError(f"Avro record index {index} is out of range")
        return position

    def _require_writable(self, operation: str) -> None:
        if self._closed:
            raise _avro.AvroError("cannot use a closed Avro container")
        if not self.writable:
            raise _avro.AvroError(
                f"cannot {operation} an Avro container opened with "
                f"mode={self._mode!r}; use mode='r+'"
            )

    def _require_appendable(self) -> None:
        if self._closed:
            raise _avro.AvroError("cannot use a closed Avro container")
        if not self.appendable:
            raise _avro.AvroError(
                "cannot append to an Avro container opened with "
                f"mode={self._mode!r}; use mode='a' or mode='r+'"
            )

    def _persist(self) -> None:
        image = self._core.image()
        # The core reports how much of the image has not moved since the last
        # write-out, so an append stays an append however the image was reached.
        extends = 0 < self._persisted <= min(self._core.stable, len(image))
        if self._origin == "memory":
            pass
        elif self._origin == "stream":
            stream = self._stream
            if extends:
                stream.seek(self._persisted)
                stream.write(image[self._persisted :])
            else:
                stream.seek(0)
                stream.write(image)
                truncate = getattr(stream, "truncate", None)
                if callable(truncate):
                    truncate()
            flush = getattr(stream, "flush", None)
            if callable(flush):
                flush()
        else:
            path = self._path
            assert path is not None
            if extends and path.exists() and path.stat().st_size == self._persisted:
                # Nothing already durable moved, so only new frames are written.
                with open(path, "r+b") as stream:
                    stream.seek(0, os.SEEK_END)
                    stream.write(image[self._persisted :])
            else:
                _atomic_write(path, image)
        self._persisted = len(image)
        self._core.mark_persisted()


def _block(info: Any) -> AvroBlock:
    return AvroBlock(
        info.ordinal,
        info.offset,
        info.data_offset,
        info.size,
        info.count,
        info.first,
    )


def _atomic_write(path: Path, payload: bytes) -> None:
    """Replace a container file without leaving a torn image behind."""

    temporary = path.with_name(f"{path.name}.rkp-{os.getpid()}.tmp")
    try:
        with open(temporary, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def _resolve_source(source: Any) -> tuple[Path | None, Any, str]:
    if isinstance(source, os.PathLike):
        return Path(os.fspath(source)), None, "path"
    if isinstance(source, str):
        if is_path_string(source):
            return Path(source), None, "path"
        return None, None, "memory"
    if isinstance(source, (bytes, bytearray, memoryview)):
        return None, None, "memory"
    if callable(getattr(source, "read", None)):
        seekable = getattr(source, "seekable", None)
        writable = getattr(source, "writable", None)
        if callable(seekable) and seekable() and callable(writable) and writable():
            return None, source, "stream"
        return None, None, "memory"
    raise TypeError(
        "an Avro container source must be a path, bytes-like object, or "
        "readable binary stream"
    )


def _resolve_destination(destination: Any) -> tuple[Path | None, Any, str]:
    if destination is None:
        return None, None, "memory"
    if isinstance(destination, os.PathLike):
        return Path(os.fspath(destination)), None, "path"
    if isinstance(destination, str):
        if is_path_string(destination):
            return Path(destination), None, "path"
        return None, None, "memory"
    if callable(getattr(destination, "write", None)):
        return None, destination, "stream"
    raise TypeError(
        "an Avro container destination must be a path or writable binary stream"
    )


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

    container = Avro.create(
        schema,
        codec=codec,
        metadata=metadata,
        sync_marker=sync_marker,
        sync_interval=sync_interval,
    )
    container.extend(values)
    return _write_bytes(destination, container.into_bytes())


def read_container(
    source: str | bytes | bytearray | memoryview | os.PathLike[str] | IO[bytes],
    *,
    schema: Any = None,
    mode: str = "r",
    cache_bytes: int = DEFAULT_CACHE_BYTES,
) -> Avro:
    """Open an Avro object container file for reading or random writing."""

    return Avro(source, mode=mode, schema=schema, cache_bytes=cache_bytes)


def _sequence(values: Any) -> Sequence[Any]:  # pragma: no cover - typing helper
    return list(values)
