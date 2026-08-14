"""Dependency-free YAML encoding and decoding for records and dataclasses."""

from __future__ import annotations

import dataclasses
import os
from collections.abc import Callable, Mapping
from typing import IO, Any, TypeVar, cast

from .._io import read as _read
from .._io import write as _write
from .._io import write_bytes as _write_bytes
from ..records.interop import dataclass_from_dict, to_dict
from ._emitter import emit
from ._parser import parse

__all__ = ["dump", "dump_bytes", "dumps", "dumps_bytes", "load", "loads"]

T = TypeVar("T")
_TYPE_TAG = "__rkp_type__"
_DATA_TAG = "data"


def loads(
    data: str | bytes | bytearray | memoryview,
    *,
    cls: type[T] | None = None,
    encoding: str = "utf-8",
    safe: bool = True,
    on_error: str = "raise",
    **kwargs: Any,
) -> T | Any:
    """Decode one safe YAML 1.2 document, optionally constructing ``cls``."""

    if isinstance(data, memoryview):
        data = data.tobytes()
    if isinstance(data, (bytes, bytearray)):
        data = data.decode(encoding)
    if not isinstance(data, str):
        raise TypeError("YAML data must be str, bytes, bytearray, or memoryview")
    if kwargs:
        names = ", ".join(sorted(kwargs))
        raise TypeError(f"unsupported YAML load option(s): {names}")
    decoded = _restore_mappings(parse(data))
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
    """Decode YAML from a path or caller-owned stream."""

    return loads(
        _read(source),
        cls=cls,
        encoding=encoding,
        safe=safe,
        on_error=on_error,
        **kwargs,
    )


def dumps(datum: Any, *, encoding: str = "utf-8", **kwargs: Any) -> str:
    """Encode a dataclass, record, or plain value as safe YAML text."""

    result = _dump_text(datum, kwargs)
    result.encode(encoding)
    return result


def dumps_bytes(datum: Any, *, encoding: str = "utf-8", **kwargs: Any) -> bytes:
    """Encode a dataclass, record, or plain value directly to bytes."""

    return _dump_text(datum, kwargs).encode(encoding)


def _dump_text(datum: Any, options: Mapping[str, Any]) -> str:
    supported = {
        "sort_keys",
        "indent",
        "explicit_start",
        "explicit_end",
        "line_break",
    }
    unknown = set(options) - supported
    if unknown:
        names = ", ".join(sorted(unknown))
        raise TypeError(f"unsupported YAML dump option(s): {names}")
    return emit(_encode_mappings(to_dict(datum)), **options)


def dump(
    datum: Any,
    destination: str | os.PathLike[str] | IO[str] | IO[bytes],
    *,
    encoding: str = "utf-8",
    **kwargs: Any,
) -> str | None:
    """Encode to a path/stream, or return a separator-free string buffer."""

    return _write(destination, dumps(datum, encoding=encoding, **kwargs), encoding)


def dump_bytes(
    datum: Any,
    destination: str | os.PathLike[str] | IO[bytes],
    *,
    encoding: str = "utf-8",
    **kwargs: Any,
) -> bytes | None:
    """Encode directly to a binary path/stream or bytes string buffer."""

    return _write_bytes(
        destination,
        dumps_bytes(datum, encoding=encoding, **kwargs),
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
    return cast(Callable[[Any], T], cls)(decoded)


def _encode_mappings(value: Any) -> Any:
    if isinstance(value, dict):
        pairs: list[list[Any]] = []
        simple: dict[Any, Any] = {}
        needs_pairs = False
        for key, item in value.items():
            encoded_key = _encode_mappings(key)
            encoded_item = _encode_mappings(item)
            if not isinstance(encoded_key, (str, int, float, bool, type(None), bytes)):
                needs_pairs = True
            pairs.append([encoded_key, encoded_item])
            if not needs_pairs:
                simple[encoded_key] = encoded_item
        if needs_pairs:
            return {_TYPE_TAG: "mapping", _DATA_TAG: pairs}
        if set(simple) == {_TYPE_TAG, _DATA_TAG} and simple.get(_TYPE_TAG) in {
            "mapping",
            "dict",
        }:
            return {_TYPE_TAG: "dict", _DATA_TAG: simple}
        return simple
    if isinstance(value, (list, tuple)):
        return [_encode_mappings(item) for item in value]
    return value


def _restore_mappings(value: Any) -> Any:
    if isinstance(value, list):
        return [_restore_mappings(item) for item in value]
    if isinstance(value, dict):
        if (
            set(value) == {_TYPE_TAG, _DATA_TAG}
            and value.get(_TYPE_TAG) == "mapping"
            and isinstance(value.get(_DATA_TAG), list)
        ):
            return {
                _hashable(_restore_mappings(pair[0])): _restore_mappings(pair[1])
                for pair in value[_DATA_TAG]
                if isinstance(pair, list) and len(pair) == 2
            }
        if (
            set(value) == {_TYPE_TAG, _DATA_TAG}
            and value.get(_TYPE_TAG) == "dict"
            and isinstance(value.get(_DATA_TAG), dict)
        ):
            return {
                key: _restore_mappings(item) for key, item in value[_DATA_TAG].items()
            }
        return {key: _restore_mappings(item) for key, item in value.items()}
    return value


def _hashable(value: Any) -> Any:
    if isinstance(value, list):
        return tuple(_hashable(item) for item in value)
    if isinstance(value, dict):
        return tuple((_hashable(key), _hashable(item)) for key, item in value.items())
    return value
