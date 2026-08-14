"""Thin stdlib JSON adapter used by the public codec package."""

from __future__ import annotations

import json
from typing import Any

from ._tags import encode_extended, restore_extended


def decode(data: str, **kwargs: Any) -> Any:
    """Decode standard JSON and restore rkp's safe tagged values."""

    return restore_extended(json.loads(data, **kwargs))


def encode(value: Any, *, ensure_ascii: bool, **kwargs: Any) -> str:
    """Encode a plain value after applying rkp's safe tagged extension."""

    kwargs.setdefault("allow_nan", False)
    return json.dumps(
        encode_extended(value),
        ensure_ascii=ensure_ascii,
        **kwargs,
    )
