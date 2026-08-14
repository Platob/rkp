"""JSON encoding and decoding for records, dataclasses, and plain values."""

from __future__ import annotations

import dataclasses
import os
from collections.abc import Mapping
from typing import IO, Any, TypeVar

from .._io import read as _read
from .._io import write as _write
from .._io import write_bytes as _write_bytes
from ..records.interop import dataclass_from_dict, to_dict
from ._codec import decode as _decode
from ._codec import encode as _encode

__all__ = ["dump", "dump_bytes", "dumps", "dumps_bytes", "load", "loads"]

T = TypeVar("T")


def loads(
    data: str | bytes | bytearray | memoryview,
    *,
    cls: type[T] | None = None,
    encoding: str = "utf-8",
    safe: bool = True,
    on_error: str = "raise",
    **kwargs: Any,
) -> T | Any:
    """Decode JSON text, optionally constructing a dataclass or record."""

    if isinstance(data, memoryview):
        data = data.tobytes()
    if isinstance(data, (bytes, bytearray)):
        data = data.decode(encoding)
    if not isinstance(data, str):
        raise TypeError("JSON data must be str, bytes, bytearray, or memoryview")
    decoded = _decode(data, **kwargs)
    return _materialize(decoded, cls=cls, safe=safe, on_error=on_error)


def load(
    source: str
    | bytes
    | bytearray
    | memoryview
    | os.PathLike[str]
    | IO[str]
    | IO[bytes],
    *,
    cls: type[T] | None = None,
    encoding: str = "utf-8",
    safe: bool = True,
    on_error: str = "raise",
    **kwargs: Any,
) -> T | Any:
    """Decode JSON from a path, document string, or caller-owned stream."""

    return loads(
        _read(source),
        cls=cls,
        encoding=encoding,
        safe=safe,
        on_error=on_error,
        **kwargs,
    )


def dumps(
    datum: Any,
    *,
    ensure_ascii: bool = False,
    encoding: str = "utf-8",
    **kwargs: Any,
) -> str:
    """Encode a dataclass, record, or plain value as JSON text."""

    result = _encode(to_dict(datum), ensure_ascii=ensure_ascii, **kwargs)
    result.encode(encoding)
    return result


def dumps_bytes(
    datum: Any,
    *,
    ensure_ascii: bool = False,
    encoding: str = "utf-8",
    **kwargs: Any,
) -> bytes:
    """Encode a dataclass, record, or plain value directly to bytes."""

    return _encode(to_dict(datum), ensure_ascii=ensure_ascii, **kwargs).encode(encoding)


def dump(
    datum: Any,
    destination: str | os.PathLike[str] | IO[str] | IO[bytes],
    *,
    encoding: str = "utf-8",
    ensure_ascii: bool = False,
    **kwargs: Any,
) -> str | None:
    """Encode to a path/stream, or return a separator-free string buffer."""

    return _write(
        destination,
        dumps(
            datum,
            ensure_ascii=ensure_ascii,
            encoding=encoding,
            **kwargs,
        ),
        encoding,
    )


def dump_bytes(
    datum: Any,
    destination: str | os.PathLike[str] | IO[bytes],
    *,
    encoding: str = "utf-8",
    ensure_ascii: bool = False,
    **kwargs: Any,
) -> bytes | None:
    """Encode directly to a binary path/stream or bytes string buffer."""

    return _write_bytes(
        destination,
        dumps_bytes(
            datum,
            ensure_ascii=ensure_ascii,
            encoding=encoding,
            **kwargs,
        ),
    )


def _materialize(
    decoded: Any,
    *,
    cls: type[T] | None,
    safe: bool,
    on_error: str,
) -> T | Any:
    if cls is None:
        return decoded
    if dataclasses.is_dataclass(cls):
        if not isinstance(decoded, Mapping):
            raise TypeError(f"decoded data for {cls.__qualname__} must be a mapping")
        return dataclass_from_dict(cls, decoded, safe=safe, on_error=on_error)
    return cls(decoded)  # type: ignore[call-arg,return-value]
