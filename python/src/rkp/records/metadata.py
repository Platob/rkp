"""Protocol-neutral metadata attached to decorated record classes."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import EllipsisType, MappingProxyType
from typing import Any, cast

from ._metadata import CATALOG_NAME, SCHEMA_NAME, TABLE_NAME, validate_metadata_name

__all__ = ["RecordMetadata", "record_metadata"]

_WIRE_NAME_KEYS = {
    "catalog_name": CATALOG_NAME,
    "schema_name": SCHEMA_NAME,
    "table_name": TABLE_NAME,
}


@dataclass(frozen=True, slots=True)
class RecordMetadata:
    """One immutable metadata contract shared by every record adapter.

    Values stay protocol-neutral here.  Arrow is responsible for normalizing
    them to its ``bytes -> bytes`` schema metadata representation.
    """

    metadata: Mapping[Any, Any]

    def __init__(self, metadata: Mapping[Any, Any] | None = None) -> None:
        if metadata is not None and not isinstance(metadata, Mapping):
            raise TypeError("record metadata must be a mapping or None")
        normalized = _canonical_metadata(metadata or {})
        object.__setattr__(self, "metadata", _freeze_mapping(normalized))

    @property
    def catalog_name(self) -> str | None:
        return _name_from_metadata(self.metadata, "catalog_name")

    @property
    def schema_name(self) -> str | None:
        return _name_from_metadata(self.metadata, "schema_name")

    @property
    def table_name(self) -> str | None:
        return _name_from_metadata(self.metadata, "table_name")

    @property
    def payload_metadata(self) -> Mapping[Any, Any]:
        """Return metadata excluding the three portable naming controls."""

        return MappingProxyType(
            {
                key: value
                for key, value in self.metadata.items()
                if _normalized_key(key) not in _WIRE_NAME_KEYS.values()
            }
        )

    def has(self, name: str) -> bool:
        """Return whether one canonical metadata key is present."""

        if not isinstance(name, str):
            raise TypeError("metadata name must be a string")
        encoded = name.encode("utf-8")
        return any(_normalized_key(key) == encoded for key in self.metadata)

    def merged(
        self,
        metadata: Mapping[Any, Any] | None | EllipsisType = ...,
        *,
        catalog_name: str | None | EllipsisType = ...,
        schema_name: str | None | EllipsisType = ...,
        table_name: str | None | EllipsisType = ...,
    ) -> RecordMetadata:
        """Return a higher-precedence immutable metadata snapshot.

        Omitted values inherit.  ``metadata=None`` clears generic payload but
        retains names, while an explicit ``None`` name clears that name.
        """

        values = dict(self.metadata)
        if metadata is None:
            values = {
                key: value
                for key, value in values.items()
                if _normalized_key(key) in _WIRE_NAME_KEYS.values()
            }
        elif metadata is not ...:
            if not isinstance(metadata, Mapping):
                raise TypeError("record metadata must be a mapping, None, or Ellipsis")
            override = _canonical_metadata(metadata)
            for key in metadata:
                normalized = _normalized_key(key)
                if (
                    normalized.startswith(b"rkp.")
                    and normalized[4:] in _WIRE_NAME_KEYS.values()
                ):
                    normalized = normalized[4:]
                if normalized in _WIRE_NAME_KEYS.values() and metadata[key] is None:
                    _remove_normalized_key(values, normalized)
            for key, value in override.items():
                _remove_normalized_key(values, _normalized_key(key))
                values[key] = value

        for key, value in (
            ("catalog_name", catalog_name),
            ("schema_name", schema_name),
            ("table_name", table_name),
        ):
            if value is ...:
                continue
            _remove_normalized_key(values, _WIRE_NAME_KEYS[key])
            if value is not None:
                values[key] = validate_metadata_name(value, name=key)
        return RecordMetadata(values)


def record_metadata(value: Any) -> RecordMetadata:
    """Return the cached immutable metadata attached by :func:`record`."""

    candidate = value if isinstance(value, type) else type(value)
    if not isinstance(candidate, type):
        raise TypeError("record_metadata expects a decorated record type or instance")
    metadata = candidate.__dict__.get("__rkp_metadata__")
    if isinstance(metadata, RecordMetadata):
        return metadata
    if candidate.__dict__.get("__rkp_record__") is True:
        # Compatibility for a decorated class produced before metadata support.
        return _EMPTY_RECORD_METADATA
    raise TypeError("record_metadata expects a decorated record type or instance")


def _canonical_metadata(values: Mapping[Any, Any]) -> dict[Any, Any]:
    result: dict[Any, Any] = {}
    normalized_keys: dict[bytes, Any] = {}
    for key, value in values.items():
        encoded = _normalized_key(key)
        if encoded.startswith(b"rkp.") and encoded[4:] in _WIRE_NAME_KEYS.values():
            encoded = encoded[4:]
        if encoded in normalized_keys:
            raise TypeError(
                f"duplicate metadata key {encoded.decode('utf-8', 'replace')!r}"
            )
        normalized_keys[encoded] = key
        if encoded in _WIRE_NAME_KEYS.values():
            name = encoded.decode("ascii")
            if value is None:
                continue
            value = validate_metadata_name(value, name=name)
            key = name
        result[key] = value
    return result


def _name_from_metadata(metadata: Mapping[Any, Any], name: str) -> str | None:
    # Construction canonicalizes and validates these values exactly once.
    return cast(str | None, metadata.get(name))


def _normalized_key(key: Any) -> bytes:
    if isinstance(key, bytes):
        return key
    if isinstance(key, str):
        return key.encode("utf-8")
    return str(key).encode("utf-8")


def _remove_normalized_key(values: dict[Any, Any], normalized: bytes) -> None:
    for key in tuple(values):
        if _normalized_key(key) == normalized:
            del values[key]


def _freeze_mapping(value: Mapping[Any, Any]) -> Mapping[Any, Any]:
    return MappingProxyType({key: _freeze_value(item) for key, item in value.items()})


def _freeze_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return _freeze_mapping(value)
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_value(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_freeze_value(item) for item in value)
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    return value


_EMPTY_RECORD_METADATA = RecordMetadata()
