"""Thread-safe SQLite cache for remote FIX dictionary pages and artifacts."""

from __future__ import annotations

import hashlib
import os
import sqlite3
import threading
import time
import zlib
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Self

from ._errors import FixCacheError
from ._paths import default_fix_cache_path

__all__ = ["FixCache", "FixCacheEntry"]

_CACHE_SCHEMA_VERSION = 1
_MAX_CACHE_VALUE_BYTES = 256 * 1024 * 1024
_INITIALIZATION_GUARD = threading.Lock()
_INITIALIZATION_LOCKS: dict[Path, threading.Lock] = {}


@dataclass(frozen=True, slots=True)
class FixCacheEntry:
    """One verified cached HTTP response."""

    url: str
    body: bytes
    fetched_at: float
    content_type: str | None = None
    encoding: str | None = None
    etag: str | None = None
    last_modified: str | None = None


class FixCache:
    """A compact, persistent, process-safe local cache.

    Response bodies and normalized parser artifacts are zlib-compressed in
    SQLite. SHA-256 digests detect disk corruption before untrusted cached
    bytes are parsed. SQLite WAL permits readers while a scraper sync writes.
    """

    def __init__(
        self,
        path: str | os.PathLike[str] | None = None,
        *,
        memory_entries: int = 128,
    ) -> None:
        if type(memory_entries) is not int or memory_entries < 0:
            raise TypeError("memory_entries must be a non-negative integer")
        selected = default_fix_cache_path() if path is None else Path(path)
        self.path = selected
        self._memory_entries = memory_entries
        self._memory: OrderedDict[tuple[str, str], object] = OrderedDict()
        self._lock = threading.RLock()
        self._closed = False
        resolved = selected.resolve(strict=False)
        with _INITIALIZATION_GUARD:
            initialization_lock = _INITIALIZATION_LOCKS.setdefault(
                resolved, threading.Lock()
            )
        with initialization_lock:
            self._open()

    def _open(self) -> None:
        connection: sqlite3.Connection | None = None
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            for attempt in range(8):
                try:
                    connection = sqlite3.connect(
                        self.path,
                        timeout=30,
                        check_same_thread=False,
                        isolation_level=None,
                    )
                    connection.execute("PRAGMA busy_timeout=30000")
                    self._connection = connection
                    connection.execute("PRAGMA journal_mode=WAL")
                    connection.execute("PRAGMA synchronous=NORMAL")
                    connection.execute("PRAGMA foreign_keys=ON")
                    self._initialize()
                    return
                except sqlite3.OperationalError as exc:
                    if connection is not None:
                        connection.close()
                        connection = None
                    if not _is_locked(exc) or attempt == 7:
                        raise
                    time.sleep(min(0.8, 0.025 * (2**attempt)))
        except FixCacheError:
            if connection is not None:
                try:
                    connection.close()
                except sqlite3.Error:
                    pass
            self._closed = True
            raise
        except (OSError, sqlite3.Error) as exc:
            if connection is not None:
                try:
                    connection.close()
                except sqlite3.Error:
                    pass
            self._closed = True
            raise FixCacheError(f"cannot open FIX cache {self.path}: {exc}") from exc

    def _initialize(self) -> None:
        self._connection.executescript(
            """
            BEGIN IMMEDIATE;
            CREATE TABLE IF NOT EXISTS cache_info (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS responses (
                url TEXT PRIMARY KEY,
                body BLOB NOT NULL,
                digest TEXT NOT NULL,
                fetched_at REAL NOT NULL,
                content_type TEXT,
                encoding TEXT,
                etag TEXT,
                last_modified TEXT
            );
            CREATE TABLE IF NOT EXISTS artifacts (
                key TEXT PRIMARY KEY,
                payload BLOB NOT NULL,
                digest TEXT NOT NULL,
                updated_at REAL NOT NULL
            );
            COMMIT;
            """
        )
        self._connection.execute(
            "INSERT OR IGNORE INTO cache_info(key, value) VALUES('schema_version', ?)",
            (str(_CACHE_SCHEMA_VERSION),),
        )
        row = self._connection.execute(
            "SELECT value FROM cache_info WHERE key = 'schema_version'"
        ).fetchone()
        if row is None or row[0] != str(_CACHE_SCHEMA_VERSION):
            raise FixCacheError(
                f"unsupported FIX cache schema {row[0] if row else None!r}; expected "
                f"{_CACHE_SCHEMA_VERSION}"
            )

    def get_response(self, url: str) -> FixCacheEntry | None:
        """Return a verified response entry, or ``None`` on a cache miss."""

        _require_text("url", url)
        memory_key = ("response", url)
        with self._lock:
            self._ensure_open()
            cached = self._memory_get(memory_key)
            if cached is not None:
                return cached if isinstance(cached, FixCacheEntry) else None
            try:
                row = self._connection.execute(
                    """SELECT body, digest, fetched_at, content_type, encoding,
                              etag, last_modified
                       FROM responses WHERE url = ?""",
                    (url,),
                ).fetchone()
            except sqlite3.Error as exc:
                raise FixCacheError(
                    f"cannot read cached response {url}: {exc}"
                ) from exc
            if row is None:
                return None
            body = _restore(row[0], row[1], f"response {url}")
            result = FixCacheEntry(
                url=url,
                body=body,
                fetched_at=float(row[2]),
                content_type=row[3],
                encoding=row[4],
                etag=row[5],
                last_modified=row[6],
            )
            self._memory_put(memory_key, result)
            return result

    def put_response(
        self,
        url: str,
        body: bytes,
        *,
        fetched_at: float | None = None,
        content_type: str | None = None,
        encoding: str | None = None,
        etag: str | None = None,
        last_modified: str | None = None,
    ) -> FixCacheEntry:
        """Atomically insert or replace one response."""

        _require_text("url", url)
        if not isinstance(body, bytes):
            raise TypeError("body must be bytes")
        timestamp = time.time() if fetched_at is None else fetched_at
        if not isinstance(timestamp, (int, float)) or timestamp < 0:
            raise TypeError("fetched_at must be a non-negative number")
        entry = FixCacheEntry(
            url,
            body,
            float(timestamp),
            content_type,
            encoding,
            etag,
            last_modified,
        )
        packed, digest = _store(body)
        with self._lock:
            self._ensure_open()
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                self._connection.execute(
                    """INSERT INTO responses(
                           url, body, digest, fetched_at, content_type, encoding,
                           etag, last_modified
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT(url) DO UPDATE SET
                           body=excluded.body,
                           digest=excluded.digest,
                           fetched_at=excluded.fetched_at,
                           content_type=excluded.content_type,
                           encoding=excluded.encoding,
                           etag=excluded.etag,
                           last_modified=excluded.last_modified""",
                    (
                        url,
                        packed,
                        digest,
                        entry.fetched_at,
                        content_type,
                        encoding,
                        etag,
                        last_modified,
                    ),
                )
                self._connection.execute("COMMIT")
            except sqlite3.Error as exc:
                self._rollback()
                raise FixCacheError(f"cannot cache response {url}: {exc}") from exc
            self._memory_put(("response", url), entry)
        return entry

    def touch_response(self, url: str, *, fetched_at: float | None = None) -> None:
        """Refresh a cached response timestamp after HTTP 304."""

        timestamp = time.time() if fetched_at is None else fetched_at
        with self._lock:
            self._ensure_open()
            try:
                cursor = self._connection.execute(
                    "UPDATE responses SET fetched_at = ? WHERE url = ?",
                    (timestamp, url),
                )
            except sqlite3.Error as exc:
                raise FixCacheError(
                    f"cannot update cached response {url}: {exc}"
                ) from exc
            if cursor.rowcount == 0:
                raise FixCacheError(f"cannot touch missing cached response {url}")
            self._memory.pop(("response", url), None)

    def get_artifact(self, key: str) -> bytes | None:
        """Return a verified normalized-parser artifact."""

        _require_text("artifact key", key)
        memory_key = ("artifact", key)
        with self._lock:
            self._ensure_open()
            cached = self._memory_get(memory_key)
            if isinstance(cached, bytes):
                return cached
            try:
                row = self._connection.execute(
                    "SELECT payload, digest FROM artifacts WHERE key = ?", (key,)
                ).fetchone()
            except sqlite3.Error as exc:
                raise FixCacheError(
                    f"cannot read cached artifact {key}: {exc}"
                ) from exc
            if row is None:
                return None
            payload = _restore(row[0], row[1], f"artifact {key}")
            self._memory_put(memory_key, payload)
            return payload

    def put_artifact(
        self, key: str, payload: bytes, *, family: str | None = None
    ) -> None:
        """Atomically insert or replace a normalized-parser artifact."""

        _require_text("artifact key", key)
        if family is not None:
            _require_text("artifact family", family)
            if not key.startswith(family):
                raise ValueError("artifact key must start with its family")
        if not isinstance(payload, bytes):
            raise TypeError("artifact payload must be bytes")
        packed, digest = _store(payload)
        with self._lock:
            self._ensure_open()
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                if family is not None:
                    self._connection.execute(
                        """DELETE FROM artifacts
                           WHERE substr(key, 1, ?) = ? AND key <> ?""",
                        (len(family), family, key),
                    )
                self._connection.execute(
                    """INSERT INTO artifacts(key, payload, digest, updated_at)
                       VALUES (?, ?, ?, ?)
                       ON CONFLICT(key) DO UPDATE SET
                           payload=excluded.payload,
                           digest=excluded.digest,
                           updated_at=excluded.updated_at""",
                    (key, packed, digest, time.time()),
                )
                self._connection.execute("COMMIT")
            except sqlite3.Error as exc:
                self._rollback()
                raise FixCacheError(f"cannot cache artifact {key}: {exc}") from exc
            if family is not None:
                for memory_key in tuple(self._memory):
                    if memory_key[0] == "artifact" and memory_key[1].startswith(family):
                        self._memory.pop(memory_key, None)
            self._memory_put(("artifact", key), payload)

    def info(self) -> dict[str, int | str]:
        """Return stable cache location and entry counts."""

        with self._lock:
            self._ensure_open()
            responses = self._connection.execute(
                "SELECT COUNT(*) FROM responses"
            ).fetchone()
            artifacts = self._connection.execute(
                "SELECT COUNT(*) FROM artifacts"
            ).fetchone()
            return {
                "path": str(self.path),
                "responses": int(responses[0]),
                "artifacts": int(artifacts[0]),
                "memory_entries": len(self._memory),
            }

    def clear(self) -> None:
        """Remove all entries from this explicitly selected cache database."""

        with self._lock:
            self._ensure_open()
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                self._connection.execute("DELETE FROM responses")
                self._connection.execute("DELETE FROM artifacts")
                self._connection.execute("COMMIT")
            except sqlite3.Error as exc:
                self._rollback()
                raise FixCacheError(
                    f"cannot clear FIX cache {self.path}: {exc}"
                ) from exc
            self._memory.clear()

    def close(self) -> None:
        """Close the underlying SQLite connection."""

        with self._lock:
            if not self._closed:
                try:
                    self._connection.close()
                except sqlite3.Error as exc:
                    raise FixCacheError(
                        f"cannot close FIX cache {self.path}: {exc}"
                    ) from exc
                finally:
                    self._closed = True
                    self._memory.clear()

    def __enter__(self) -> Self:
        self._ensure_open()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _memory_get(self, key: tuple[str, str]) -> object | None:
        value = self._memory.get(key)
        if value is not None:
            self._memory.move_to_end(key)
        return value

    def _memory_put(self, key: tuple[str, str], value: object) -> None:
        if self._memory_entries == 0:
            return
        self._memory[key] = value
        self._memory.move_to_end(key)
        while len(self._memory) > self._memory_entries:
            self._memory.popitem(last=False)

    def _rollback(self) -> None:
        try:
            self._connection.execute("ROLLBACK")
        except sqlite3.Error:
            pass

    def _ensure_open(self) -> None:
        if self._closed:
            raise FixCacheError("FIX cache is closed")


def _store(value: bytes) -> tuple[bytes, str]:
    return zlib.compress(value, level=9), hashlib.sha256(value).hexdigest()


def _restore(value: bytes, digest: str, label: str) -> bytes:
    try:
        decompressor = zlib.decompressobj()
        restored = decompressor.decompress(value, _MAX_CACHE_VALUE_BYTES + 1)
        if len(restored) > _MAX_CACHE_VALUE_BYTES or decompressor.unconsumed_tail:
            raise FixCacheError(
                f"corrupt cached {label}: decompressed value is too large"
            )
        restored += decompressor.flush()
    except zlib.error as exc:
        raise FixCacheError(f"corrupt cached {label}: invalid compression") from exc
    if (
        len(restored) > _MAX_CACHE_VALUE_BYTES
        or not decompressor.eof
        or decompressor.unused_data
    ):
        raise FixCacheError(f"corrupt cached {label}: invalid compressed payload")
    actual = hashlib.sha256(restored).hexdigest()
    if actual != digest:
        raise FixCacheError(f"corrupt cached {label}: checksum mismatch")
    return restored


def _require_text(name: str, value: object) -> None:
    if not isinstance(value, str) or not value:
        raise TypeError(f"{name} must be a non-empty string")


def _is_locked(error: sqlite3.OperationalError) -> bool:
    message = str(error).casefold()
    return "locked" in message or "busy" in message
