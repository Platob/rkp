"""Shared path-or-buffer handling for text and byte codecs."""

from __future__ import annotations

import io
import os
from typing import IO

__all__ = ["is_path_string", "read", "write", "write_bytes"]


def is_path_string(value: str) -> bool:
    """Return whether a string is a path under the public codec rule.

    Strings are paths only when they contain a forward or backward separator.
    Use an explicit :class:`os.PathLike` object for separator-free filenames.
    """

    return "/" in value or "\\" in value


def read(
    source: str
    | bytes
    | bytearray
    | memoryview
    | os.PathLike[str]
    | IO[str]
    | IO[bytes],
) -> str | bytes | bytearray | memoryview:
    """Read from a path/stream, or return a separator-free string buffer."""

    if isinstance(source, str):
        if not is_path_string(source):
            return source
        with open(source, "rb") as stream:
            return stream.read()
    if isinstance(source, (bytes, bytearray, memoryview)):
        return source
    if isinstance(source, os.PathLike):
        with open(source, "rb") as stream:
            return stream.read()
    reader = getattr(source, "read", None)
    if not callable(reader):
        raise TypeError("source must be text, bytes-like, a path, or a readable stream")
    return reader()


def write(
    destination: str | os.PathLike[str] | IO[str] | IO[bytes],
    data: str,
    encoding: str,
) -> str | None:
    """Write text, returning it when ``destination`` is a string buffer."""

    if isinstance(destination, str):
        if not is_path_string(destination):
            return data
        with open(destination, "w", encoding=encoding, newline="") as stream:
            stream.write(data)
        return None
    if isinstance(destination, os.PathLike):
        with open(destination, "w", encoding=encoding, newline="") as stream:
            stream.write(data)
        return None
    writer = getattr(destination, "write", None)
    if not callable(writer):
        raise TypeError("destination must be a string buffer, path, or writable stream")
    if isinstance(destination, (io.RawIOBase, io.BufferedIOBase)):
        writer(data.encode(encoding))
        return None
    if isinstance(destination, io.TextIOBase):
        writer(data)
        return None
    try:
        writer(data)
    except TypeError:
        writer(data.encode(encoding))
    return None


def write_bytes(
    destination: str | os.PathLike[str] | IO[bytes],
    data: bytes,
) -> bytes | None:
    """Write bytes without text probing or transcoding.

    As with :func:`write`, a separator-free string is an immutable buffer and
    returns ``data``. Paths are opened in binary mode and caller-owned streams
    remain open.
    """

    if not isinstance(data, bytes):
        raise TypeError("data must be bytes")
    if isinstance(destination, str):
        if not is_path_string(destination):
            return data
        with open(destination, "wb") as stream:
            stream.write(data)
        return None
    if isinstance(destination, os.PathLike):
        with open(destination, "wb") as stream:
            stream.write(data)
        return None
    if isinstance(destination, io.TextIOBase):
        raise TypeError("destination must be a path or writable binary stream")
    writer = getattr(destination, "write", None)
    if not callable(writer):
        raise TypeError("destination must be a path or writable binary stream")
    writer(data)
    return None
