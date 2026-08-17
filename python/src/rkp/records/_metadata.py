"""Dependency-free wire metadata shared by record protocol adapters."""

from __future__ import annotations

import datetime as dt
import enum
import json
from collections.abc import Mapping
from decimal import Decimal
from typing import Any

PARQUET_FIELD_ID = b"PARQUET:field_id"
ORC_FIELD_ID = b"iceberg.id"
PRIMARY_KEY = b"primary_key"
PARTITION_KEY = b"partition_key"
INDEX_KEY = b"index_key"
SCHEMA_ID = b"iceberg.schema_id"
IDENTIFIER_FIELD_IDS = b"iceberg.identifier_field_ids"
INITIAL_DEFAULT = b"iceberg.initial_default"
WRITE_DEFAULT = b"iceberg.write_default"
DOC = b"doc"
CATALOG_NAME = b"catalog_name"
SCHEMA_NAME = b"schema_name"
TABLE_NAME = b"table_name"
CATALOG_METADATA_KEYS = (CATALOG_NAME, SCHEMA_NAME, TABLE_NAME)
MAX_FIELD_SEQ = 2_147_483_647


def field_seq_from_metadata(
    metadata: Mapping[bytes, bytes] | None,
    *,
    path: str,
) -> int | None:
    """Read one consistent Iceberg/Parquet sequence from wire metadata."""

    if not metadata:
        return None
    candidates: list[tuple[bytes, int]] = []
    for key in (PARQUET_FIELD_ID, ORC_FIELD_ID):
        if key not in metadata:
            continue
        raw = metadata[key]
        try:
            value = int(raw.decode("ascii"))
        except (AttributeError, UnicodeDecodeError, ValueError) as exc:
            raise ValueError(f"invalid field seq {raw!r} at {path!r}") from exc
        if not 1 <= value <= MAX_FIELD_SEQ:
            raise ValueError(
                f"field seq at {path!r} must be between 1 and {MAX_FIELD_SEQ}"
            )
        candidates.append((key, value))
    if not candidates:
        return None
    if any(value != candidates[0][1] for _, value in candidates[1:]):
        raise ValueError(f"conflicting field seq values at {path!r}")
    return candidates[0][1]


def normalize_metadata(values: Mapping[Any, Any]) -> dict[bytes, bytes]:
    """Normalize protocol metadata keys and values to their wire encoding."""

    normalized: dict[bytes, bytes] = {}
    for key, value in values.items():
        if isinstance(key, bytes):
            key_bytes = key
        elif isinstance(key, str):
            key_bytes = key.encode("utf-8")
        else:
            key_bytes = str(key).encode("utf-8")
        if key_bytes in normalized:
            name = key_bytes.decode("utf-8", "replace")
            raise TypeError(f"duplicate metadata key {name!r}")
        normalized[key_bytes] = _metadata_value(value)
    for key in CATALOG_METADATA_KEYS:
        if key in normalized:
            metadata_name(normalized, key)
    return normalized


def validate_metadata_name(value: Any, *, name: str) -> str:
    """Validate one portable catalog/schema/table name."""

    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError(f"{name} metadata must be valid UTF-8") from exc
    if not isinstance(value, str) or not value:
        raise TypeError(f"{name} metadata must be a non-empty string")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise ValueError(f"{name} metadata contains a control character")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError(f"{name} metadata must be valid UTF-8") from exc
    return value


def metadata_name(
    metadata: Mapping[bytes, bytes] | None,
    key: bytes,
) -> str | None:
    """Read and validate one portable name from normalized wire metadata."""

    if key not in CATALOG_METADATA_KEYS:
        raise ValueError("key must be a portable catalog metadata key")
    if not metadata:
        return None
    legacy_key = b"rkp." + key
    raw = metadata.get(key)
    legacy_raw = metadata.get(legacy_key)
    if raw is None:
        raw = legacy_raw
    elif legacy_raw is not None and legacy_raw != raw:
        name = key.decode("ascii")
        raise ValueError(f"conflicting {name} metadata values")
    if raw is None:
        return None
    try:
        value = raw.decode("utf-8")
    except (AttributeError, UnicodeDecodeError) as exc:
        name = key.decode("ascii")
        raise ValueError(f"{name} metadata must be valid UTF-8") from exc
    return validate_metadata_name(value, name=key.decode("ascii"))


def metadata_enabled(value: bytes | bytearray | memoryview | str | None) -> bool:
    """Return whether a normalized metadata flag represents an enabled value."""

    if value is None:
        return False
    if isinstance(value, str):
        normalized = value.encode("utf-8")
    else:
        normalized = bytes(value)
    return normalized.strip().lower() not in {b"", b"false", b"no", b"null"}


def _metadata_value(value: Any) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, str):
        return value.encode("utf-8")
    if value is True:
        return b"true"
    if value is False:
        return b"false"
    if value is None:
        return b"null"
    if isinstance(value, enum.Enum):
        return _metadata_value(value.value)
    if isinstance(value, (int, float, Decimal)):
        return str(value).encode("ascii")
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat().encode("utf-8")
    try:
        return json.dumps(
            _json_compatible(value),
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    except (TypeError, ValueError):
        return str(value).encode("utf-8")


def _json_compatible(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_compatible(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_json_compatible(item) for item in value]
    return value
