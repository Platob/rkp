"""Safe tagged JSON extensions for bytes and non-string mapping keys."""

from __future__ import annotations

import base64
import binascii
from typing import Any

_TYPE_TAG = "__rkp_type__"
_DATA_TAG = "data"
_TAG_TYPES = frozenset({"bytes", "dict", "mapping"})


def encode_extended(value: Any) -> Any:
    """Convert extended Python values into unambiguous JSON objects."""

    if isinstance(value, bytes):
        return {
            _TYPE_TAG: "bytes",
            _DATA_TAG: base64.b64encode(value).decode("ascii"),
        }
    if isinstance(value, list):
        return [encode_extended(item) for item in value]
    if isinstance(value, dict):
        encoded: dict[Any, Any] = {}
        pairs: list[list[Any]] = []
        needs_pairs = False
        for key, item in value.items():
            encoded_key = encode_extended(key)
            encoded_item = encode_extended(item)
            if not isinstance(encoded_key, str):
                needs_pairs = True
            pairs.append([encoded_key, encoded_item])
            if not needs_pairs:
                encoded[encoded_key] = encoded_item
        if needs_pairs:
            return {_TYPE_TAG: "mapping", _DATA_TAG: pairs}
        if _is_tag(encoded):
            return {_TYPE_TAG: "dict", _DATA_TAG: encoded}
        return encoded
    return value


def restore_extended(value: Any) -> Any:
    """Restore safe tagged values while leaving ordinary dictionaries intact."""

    if isinstance(value, list):
        return [restore_extended(item) for item in value]
    if isinstance(value, dict):
        if _tagged_as(value, "bytes", str):
            try:
                return base64.b64decode(value[_DATA_TAG], validate=True)
            except (ValueError, binascii.Error):
                pass
        if _tagged_as(value, "dict", dict):
            return {
                key: restore_extended(item) for key, item in value[_DATA_TAG].items()
            }
        if _tagged_as(value, "mapping", list):
            return {
                _hashable(restore_extended(pair[0])): restore_extended(pair[1])
                for pair in value[_DATA_TAG]
                if isinstance(pair, list) and len(pair) == 2
            }
        return {key: restore_extended(item) for key, item in value.items()}
    return value


def _tagged_as(value: dict[Any, Any], kind: str, data_type: type[Any]) -> bool:
    return (
        set(value) == {_TYPE_TAG, _DATA_TAG}
        and value.get(_TYPE_TAG) == kind
        and isinstance(value.get(_DATA_TAG), data_type)
    )


def _is_tag(value: dict[Any, Any]) -> bool:
    return set(value) == {_TYPE_TAG, _DATA_TAG} and value.get(_TYPE_TAG) in _TAG_TYPES


def _hashable(value: Any) -> Any:
    if isinstance(value, list):
        return tuple(_hashable(item) for item in value)
    if isinstance(value, dict):
        return tuple((_hashable(key), _hashable(item)) for key, item in value.items())
    return value
