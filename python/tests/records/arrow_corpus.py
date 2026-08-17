"""Every Arrow type RKP can be handed, in one place.

Round-trip tests across protocols keep re-deriving "one of each type", and they
keep disagreeing about what the set is.  This module is that set, built from
PyArrow's own type constructors so a new Arrow release shows up as a new case
rather than as silence.

``ARROW_TYPES`` is the corpus.  ``arrow_type_names()`` gives stable ids for
``pytest.mark.parametrize``.  Nothing here asserts anything: what each protocol
is expected to do with a given type belongs in that protocol's tests.
"""

from __future__ import annotations

from collections.abc import Iterator

import pyarrow as pa

__all__ = [
    "ARROW_TYPES",
    "NESTED_ARROW_TYPES",
    "PRIMITIVE_ARROW_TYPES",
    "arrow_fields",
    "arrow_type_names",
    "optional",
]


def _maybe(name: str, *args: object) -> Iterator[pa.DataType]:
    """Yield a type only when this PyArrow build has its constructor."""

    factory = getattr(pa, name, None)
    if factory is None:  # pragma: no cover - depends on the PyArrow version
        return
    try:
        yield factory(*args)
    except (NotImplementedError, TypeError):  # pragma: no cover - version drift
        return


def _primitives() -> list[pa.DataType]:
    types: list[pa.DataType] = [
        pa.null(),
        pa.bool_(),
        pa.int8(),
        pa.int16(),
        pa.int32(),
        pa.int64(),
        pa.uint8(),
        pa.uint16(),
        pa.uint32(),
        pa.uint64(),
        pa.float16(),
        pa.float32(),
        pa.float64(),
        pa.decimal128(1, 0),
        pa.decimal128(10, 3),
        pa.decimal128(38, 38),
        pa.date32(),
        pa.date64(),
        pa.time32("s"),
        pa.time32("ms"),
        pa.time64("us"),
        pa.time64("ns"),
        pa.string(),
        pa.large_string(),
        pa.binary(),
        pa.large_binary(),
        pa.binary(0),
        pa.binary(16),
    ]
    types.extend(pa.timestamp(unit) for unit in ("s", "ms", "us", "ns"))
    types.extend(pa.timestamp(unit, tz="UTC") for unit in ("s", "ms", "us", "ns"))
    # A named zone is not the same fact as "UTC-adjusted", and round trips have
    # historically lost the distinction.
    types.append(pa.timestamp("us", tz="America/New_York"))
    types.append(pa.timestamp("ns", tz="+05:30"))
    types.extend(pa.duration(unit) for unit in ("s", "ms", "us", "ns"))
    types.extend(_maybe("decimal256", 40, 2))
    types.extend(_maybe("month_day_nano_interval"))
    types.extend(_maybe("string_view"))
    types.extend(_maybe("binary_view"))
    types.extend(_maybe("uuid"))
    return types


def _nested() -> list[pa.DataType]:
    inner = pa.int64()
    types: list[pa.DataType] = [
        pa.list_(inner),
        pa.list_(pa.field("element", inner, nullable=False)),
        pa.large_list(inner),
        pa.list_(inner, 4),
        pa.map_(pa.string(), inner),
        pa.map_(pa.string(), pa.list_(inner)),
        pa.struct([pa.field("a", inner), pa.field("b", pa.string(), nullable=False)]),
        pa.struct([]),
        pa.dictionary(pa.int32(), pa.string()),
        pa.dictionary(pa.int8(), pa.int64(), ordered=True),
        # Deep nesting is where field-id propagation and recursion bugs live.
        pa.list_(pa.struct([pa.field("m", pa.map_(pa.string(), pa.list_(inner)))])),
    ]
    types.extend(_maybe("list_view", inner))
    types.extend(_maybe("large_list_view", inner))
    types.extend(_maybe("run_end_encoded", pa.int32(), pa.string()))
    return types


PRIMITIVE_ARROW_TYPES: tuple[pa.DataType, ...] = tuple(_primitives())
NESTED_ARROW_TYPES: tuple[pa.DataType, ...] = tuple(_nested())
ARROW_TYPES: tuple[pa.DataType, ...] = PRIMITIVE_ARROW_TYPES + NESTED_ARROW_TYPES


def arrow_type_names(
    types: tuple[pa.DataType, ...] = ARROW_TYPES,
) -> tuple[str, ...]:
    """Return stable parametrize ids, one per type."""

    return tuple(str(item) for item in types)


def optional(arrow_type: pa.DataType) -> bool:
    """Return whether Arrow allows a non-nullable field of this type."""

    # PyArrow refuses ``pa.field(pa.null(), nullable=False)``.
    return pa.types.is_null(arrow_type)


def arrow_fields(
    types: tuple[pa.DataType, ...] = ARROW_TYPES,
    *,
    name: str = "value",
    metadata: dict[bytes, bytes] | None = None,
) -> tuple[pa.Field, ...]:
    """Wrap the corpus in fields, respecting Arrow's nullability rules."""

    return tuple(
        pa.field(
            name,
            arrow_type,
            nullable=optional(arrow_type) or index % 2 == 0,
            metadata=metadata,
        )
        for index, arrow_type in enumerate(types)
    )
