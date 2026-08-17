"""Protocol-neutral field and data type model shared by every RKP adapter.

Arrow remains RKP's canonical physical representation, but each protocol
(Iceberg, Avro, Glue) needs the same structural questions answered: what kind
of value is this, what precision does it carry, is it required, and which
stable identity does it own.  Centralizing that here keeps one traversal, one
identity rule, and one error vocabulary instead of a mapping table per
protocol.

The model is deliberately small and hashable so adapters can cache on it.
"""

from __future__ import annotations

import base64
import binascii
import dataclasses
import datetime as dt
import enum
import uuid as uuid_module
from collections.abc import Mapping, Sequence
from decimal import Decimal, InvalidOperation
from types import MappingProxyType
from typing import Any

import pyarrow as pa

from ._metadata import (
    DOC,
    INITIAL_DEFAULT,
    ORC_FIELD_ID,
    PARQUET_FIELD_ID,
    PRIMARY_KEY,
    WRITE_DEFAULT,
    field_seq_from_metadata,
    metadata_enabled,
)

__all__ = [
    "DataType",
    "FieldSpec",
    "TypeKind",
    "arrow_fields_into_specs",
    "arrow_into_field_spec",
    "arrow_type_into_data_type",
    "data_type_into_arrow_type",
    "default_from_json",
    "default_into_json",
    "field_spec_into_arrow",
    "join_path",
]

MISSING: Any = ...

_DOC = DOC
_TIME_UNITS = ("s", "ms", "us", "ns")


class TypeKind(enum.Enum):
    """The logical value kinds RKP maps between protocols."""

    BOOLEAN = "boolean"
    INT32 = "int32"
    INT64 = "int64"
    FLOAT32 = "float32"
    FLOAT64 = "float64"
    DECIMAL = "decimal"
    DATE = "date"
    TIME = "time"
    TIMESTAMP = "timestamp"
    STRING = "string"
    BINARY = "binary"
    FIXED = "fixed"
    UUID = "uuid"
    LIST = "list"
    MAP = "map"
    STRUCT = "struct"
    UNKNOWN = "unknown"

    @property
    def is_nested(self) -> bool:
        """Return whether the kind contains other fields."""

        return self in {TypeKind.LIST, TypeKind.MAP, TypeKind.STRUCT}


@dataclasses.dataclass(frozen=True, slots=True)
class DataType:
    """One protocol-neutral data type with its parameters."""

    kind: TypeKind
    precision: int | None = None
    scale: int | None = None
    unit: str | None = None
    adjusted_to_utc: bool = False
    length: int | None = None
    fields: tuple[FieldSpec, ...] = ()
    element: FieldSpec | None = None
    key: FieldSpec | None = None
    value: FieldSpec | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.kind, TypeKind):
            raise TypeError("kind must be a TypeKind")
        if self.kind is TypeKind.DECIMAL:
            if type(self.precision) is not int or not 1 <= self.precision <= 38:
                raise ValueError("decimal precision must be between 1 and 38")
            if type(self.scale) is not int or not 0 <= self.scale <= self.precision:
                raise ValueError("decimal scale must be between 0 and the precision")
        if self.kind in {TypeKind.TIME, TypeKind.TIMESTAMP} and (
            self.unit not in _TIME_UNITS
        ):
            raise ValueError(f"unit must be one of {', '.join(_TIME_UNITS)}")
        if self.kind is TypeKind.FIXED and (
            type(self.length) is not int or self.length < 0
        ):
            raise ValueError("fixed length must be a non-negative integer")
        if self.kind is TypeKind.LIST and self.element is None:
            raise ValueError("list types require an element field")
        if self.kind is TypeKind.MAP and (self.key is None or self.value is None):
            raise ValueError("map types require key and value fields")

    @property
    def children(self) -> tuple[FieldSpec, ...]:
        """Return every nested field in canonical traversal order."""

        if self.kind is TypeKind.STRUCT:
            return self.fields
        if self.kind is TypeKind.LIST:
            assert self.element is not None
            return (self.element,)
        if self.kind is TypeKind.MAP:
            assert self.key is not None and self.value is not None
            return (self.key, self.value)
        return ()


@dataclasses.dataclass(frozen=True, slots=True)
class FieldSpec:
    """One named field with identity, nullability, and carried metadata."""

    name: str
    data_type: DataType
    required: bool = True
    field_id: int | None = None
    doc: str | None = None
    primary_key: bool = False
    default: Any = MISSING
    write_default: Any = MISSING
    metadata: Mapping[bytes, bytes] = dataclasses.field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name:
            raise TypeError("field names must be non-empty strings")
        if not isinstance(self.data_type, DataType):
            raise TypeError("data_type must be a DataType")
        if self.metadata and not isinstance(self.metadata, MappingProxyType):
            object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))

    @property
    def optional(self) -> bool:
        """Return whether the field accepts null values."""

        return not self.required

    @property
    def has_default(self) -> bool:
        """Return whether a protocol-level default value was configured."""

        return self.default is not MISSING

    def replace(self, **changes: Any) -> FieldSpec:
        """Return a copy with the given attributes replaced."""

        return dataclasses.replace(self, **changes)


def join_path(parent: str, child: str) -> str:
    """Join one dotted diagnostic path used by every adapter."""

    return f"{parent}.{child}" if parent else child


def arrow_into_field_spec(field: pa.Field, *, path: str = "") -> FieldSpec:
    """Convert one Arrow field into the neutral model."""

    if not isinstance(field, pa.Field):
        raise TypeError("arrow_into_field_spec expects a pyarrow.Field")
    field_path = join_path(path, field.name)
    metadata = field.metadata
    data_type = arrow_type_into_data_type(field.type, path=field_path)
    if not metadata:
        # The common case carries no metadata at all, so skip every lookup
        # rather than paying for them once per field of a wide schema.
        return FieldSpec(
            name=field.name,
            data_type=data_type,
            required=not field.nullable,
        )
    doc = metadata.get(_DOC)
    return FieldSpec(
        name=field.name,
        data_type=data_type,
        required=not field.nullable,
        field_id=field_seq_from_metadata(metadata, path=field_path),
        doc=doc.decode("utf-8") if doc is not None else None,
        primary_key=metadata_enabled(metadata.get(PRIMARY_KEY)),
        default=_metadata_default(data_type, metadata, INITIAL_DEFAULT, field_path),
        write_default=_metadata_default(data_type, metadata, WRITE_DEFAULT, field_path),
        metadata=MappingProxyType(metadata),
    )


def arrow_type_into_data_type(arrow_type: pa.DataType, *, path: str = "") -> DataType:
    """Convert one Arrow data type into the neutral model.

    Widths that upcast without loss are normalized: unsigned integers,
    half floats, and second/millisecond temporal units all resolve to the
    portable representation every protocol in RKP can express.
    """

    if not isinstance(arrow_type, pa.DataType):
        raise TypeError("arrow_type_into_data_type expects a pyarrow.DataType")

    # Parameter-free types resolve through one dictionary lookup keyed by
    # Arrow's numeric type id; only parameterized and nested types walk the
    # predicate chain below.
    resolved = _SCALAR_BY_TYPE_ID.get(arrow_type.id)
    if resolved is not None:
        return resolved

    types = pa.types
    if types.is_boolean(arrow_type):
        return _BOOLEAN
    if types.is_null(arrow_type):
        return _UNKNOWN
    if types.is_integer(arrow_type):
        return _integer_data_type(arrow_type, path)
    if types.is_float32(arrow_type) or types.is_float16(arrow_type):
        return _FLOAT32
    if types.is_float64(arrow_type):
        return _FLOAT64
    if types.is_decimal(arrow_type):
        precision = arrow_type.precision
        if precision > 38:
            raise _unsupported(path, arrow_type)
        return DataType(TypeKind.DECIMAL, precision=precision, scale=arrow_type.scale)
    if types.is_date(arrow_type):
        return _DATE
    if types.is_time(arrow_type):
        return DataType(TypeKind.TIME, unit=_arrow_unit(arrow_type.unit))
    if types.is_timestamp(arrow_type):
        return DataType(
            TypeKind.TIMESTAMP,
            unit=_arrow_unit(arrow_type.unit),
            adjusted_to_utc=arrow_type.tz is not None,
        )
    if types.is_string(arrow_type) or types.is_large_string(arrow_type):
        return _STRING
    if _is_string_view(arrow_type):
        return _STRING
    if types.is_fixed_size_binary(arrow_type):
        return DataType(TypeKind.FIXED, length=arrow_type.byte_width)
    if types.is_binary(arrow_type) or types.is_large_binary(arrow_type):
        return _BINARY
    if _is_binary_view(arrow_type):
        return _BINARY
    if types.is_struct(arrow_type):
        return DataType(
            TypeKind.STRUCT,
            fields=tuple(
                arrow_into_field_spec(child, path=path) for child in arrow_type
            ),
        )
    if _is_list_like(arrow_type):
        return DataType(
            TypeKind.LIST,
            element=arrow_into_field_spec(arrow_type.value_field, path=path),
        )
    if types.is_map(arrow_type):
        return DataType(
            TypeKind.MAP,
            key=arrow_into_field_spec(arrow_type.key_field, path=path),
            value=arrow_into_field_spec(arrow_type.item_field, path=path),
        )
    if types.is_dictionary(arrow_type):
        return arrow_type_into_data_type(arrow_type.value_type, path=path)
    if _is_uuid(arrow_type):
        return _UUID
    storage = getattr(arrow_type, "storage_type", None)
    if storage is not None:
        return arrow_type_into_data_type(storage, path=path)
    raise _unsupported(path, arrow_type)


def field_spec_into_arrow(
    spec: FieldSpec,
    *,
    large_types: bool = False,
    include_field_ids: bool = True,
    include_primary_keys: bool = True,
) -> pa.Field:
    """Convert one neutral field back into Arrow.

    ``large_types`` selects the large string/binary/list projection that
    Iceberg readers use, keeping RKP's output byte-compatible with the
    reference implementation's Arrow schemas.
    """

    if not isinstance(spec, FieldSpec):
        raise TypeError("field_spec_into_arrow expects a FieldSpec")
    metadata: dict[bytes, bytes] = {
        key: value
        for key, value in (spec.metadata or {}).items()
        if key
        not in {
            PARQUET_FIELD_ID,
            ORC_FIELD_ID,
            PRIMARY_KEY,
            _DOC,
            INITIAL_DEFAULT,
            WRITE_DEFAULT,
        }
    }
    if include_field_ids and spec.field_id is not None:
        metadata[PARQUET_FIELD_ID] = str(spec.field_id).encode("ascii")
    if spec.doc:
        metadata[_DOC] = spec.doc.encode("utf-8")
    if include_primary_keys and spec.primary_key:
        metadata[PRIMARY_KEY] = b"true"
    for key, value in (
        (INITIAL_DEFAULT, spec.default),
        (WRITE_DEFAULT, spec.write_default),
    ):
        if value is not MISSING:
            metadata[key] = default_into_json(spec.data_type, value).encode("utf-8")
    return pa.field(
        spec.name,
        data_type_into_arrow_type(
            spec.data_type,
            large_types=large_types,
            include_field_ids=include_field_ids,
            include_primary_keys=include_primary_keys,
        ),
        nullable=not spec.required,
        metadata=metadata or None,
    )


def data_type_into_arrow_type(
    data_type: DataType,
    *,
    large_types: bool = False,
    include_field_ids: bool = True,
    include_primary_keys: bool = True,
) -> pa.DataType:
    """Convert one neutral data type back into Arrow."""

    if not isinstance(data_type, DataType):
        raise TypeError("data_type_into_arrow_type expects a DataType")
    kind = data_type.kind
    if kind is TypeKind.BOOLEAN:
        return pa.bool_()
    if kind is TypeKind.INT32:
        return pa.int32()
    if kind is TypeKind.INT64:
        return pa.int64()
    if kind is TypeKind.FLOAT32:
        return pa.float32()
    if kind is TypeKind.FLOAT64:
        return pa.float64()
    if kind is TypeKind.DECIMAL:
        return pa.decimal128(int(data_type.precision or 0), int(data_type.scale or 0))
    if kind is TypeKind.DATE:
        return pa.date32()
    if kind is TypeKind.TIME:
        return pa.time64("us" if data_type.unit in {"s", "ms"} else data_type.unit)
    if kind is TypeKind.TIMESTAMP:
        unit = "us" if data_type.unit in {"s", "ms"} else data_type.unit
        return pa.timestamp(unit, tz="UTC" if data_type.adjusted_to_utc else None)
    if kind is TypeKind.STRING:
        return pa.large_string() if large_types else pa.string()
    if kind is TypeKind.BINARY:
        return pa.large_binary() if large_types else pa.binary()
    if kind is TypeKind.FIXED:
        return pa.binary(int(data_type.length or 0))
    if kind is TypeKind.UUID:
        return _uuid_arrow_type()
    if kind is TypeKind.UNKNOWN:
        return pa.null()

    convert = {
        "large_types": large_types,
        "include_field_ids": include_field_ids,
        "include_primary_keys": include_primary_keys,
    }
    if kind is TypeKind.STRUCT:
        return pa.struct(
            [field_spec_into_arrow(child, **convert) for child in data_type.fields]
        )
    if kind is TypeKind.LIST:
        assert data_type.element is not None
        element = field_spec_into_arrow(data_type.element, **convert)
        return pa.large_list(element) if large_types else pa.list_(element)
    assert data_type.key is not None and data_type.value is not None
    return pa.map_(
        field_spec_into_arrow(data_type.key, **convert),
        field_spec_into_arrow(data_type.value, **convert),
    )


def _integer_data_type(arrow_type: pa.DataType, path: str) -> DataType:
    types = pa.types
    if types.is_int8(arrow_type) or types.is_int16(arrow_type):
        return _INT32
    if types.is_int32(arrow_type):
        return _INT32
    if types.is_int64(arrow_type):
        return _INT64
    if types.is_uint8(arrow_type) or types.is_uint16(arrow_type):
        return _INT32
    if types.is_uint32(arrow_type):
        # Unsigned 32-bit values overflow a signed int, so widen rather than
        # silently truncating the way a same-width mapping would.
        return _INT64
    raise _unsupported(path, arrow_type)


def _arrow_unit(unit: str) -> str:
    if unit not in _TIME_UNITS:  # pragma: no cover - PyArrow constrains this
        raise ValueError(f"unsupported Arrow time unit {unit!r}")
    return unit


def _is_list_like(arrow_type: pa.DataType) -> bool:
    types = pa.types
    predicates = (
        types.is_list,
        types.is_large_list,
        types.is_fixed_size_list,
        getattr(types, "is_list_view", None),
        getattr(types, "is_large_list_view", None),
    )
    return any(predicate(arrow_type) for predicate in predicates if predicate)


def _is_string_view(arrow_type: pa.DataType) -> bool:
    predicate = getattr(pa.types, "is_string_view", None)
    return bool(predicate and predicate(arrow_type))


def _is_binary_view(arrow_type: pa.DataType) -> bool:
    predicate = getattr(pa.types, "is_binary_view", None)
    return bool(predicate and predicate(arrow_type))


def _is_uuid(arrow_type: pa.DataType) -> bool:
    predicate = getattr(pa.types, "is_uuid", None)
    if predicate is not None and predicate(arrow_type):
        return True
    # Arrow's canonical UUID type is a built-in extension in recent PyArrow
    # releases, so it is not an instance of the Python ExtensionType base.
    return getattr(arrow_type, "extension_name", None) == "arrow.uuid"


def _uuid_arrow_type() -> pa.DataType:
    factory = getattr(pa, "uuid", None)
    # PyArrow gained a canonical UUID extension type in 18.0; older releases
    # use the equivalent fixed-width storage.
    return factory() if factory is not None else pa.binary(16)


def _unsupported(path: str, arrow_type: pa.DataType) -> TypeError:
    location = path or "value"
    return TypeError(f"Column {location!r} has an unsupported type: {arrow_type}")


def arrow_fields_into_specs(
    fields: Sequence[pa.Field], *, path: str = ""
) -> tuple[FieldSpec, ...]:
    """Convert an Arrow field forest into neutral specs."""

    return tuple(arrow_into_field_spec(field, path=path) for field in fields)


def default_from_json(data_type: DataType, raw: str | bytes) -> Any:
    """Decode one single-value JSON default against its data type.

    The encoding follows Iceberg's single-value serialization, which is also
    the least ambiguous portable form for Arrow field metadata.
    """

    from ..json import loads as _loads_json

    text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else raw
    value = _loads_json(text)
    return _default_value(data_type, value)


def default_into_json(data_type: DataType, value: Any) -> str:
    """Encode one default value using single-value JSON serialization."""

    from ..json import dumps as _dumps_json

    return _dumps_json(_json_default(data_type, value))


def _metadata_default(
    data_type: DataType,
    metadata: Mapping[bytes, bytes],
    key: bytes,
    path: str,
) -> Any:
    raw = metadata.get(key)
    if raw is None:
        return MISSING
    try:
        return default_from_json(data_type, raw)
    except (ValueError, TypeError, InvalidOperation) as exc:
        name = key.decode("ascii")
        raise ValueError(f"invalid {name} metadata at {path!r}: {exc}") from exc


def _default_value(data_type: DataType, value: Any) -> Any:
    kind = data_type.kind
    if value is None or kind is TypeKind.UNKNOWN:
        return None
    if kind is TypeKind.BOOLEAN:
        if not isinstance(value, bool):
            raise ValueError("boolean defaults must be true or false")
        return value
    if kind in {TypeKind.INT32, TypeKind.INT64}:
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError("integer defaults must be JSON integers")
        return value
    if kind in {TypeKind.FLOAT32, TypeKind.FLOAT64}:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("floating point defaults must be JSON numbers")
        return float(value)
    if kind is TypeKind.DECIMAL:
        return Decimal(str(value))
    if kind is TypeKind.DATE:
        return dt.date.fromisoformat(str(value))
    if kind is TypeKind.TIME:
        return dt.time.fromisoformat(str(value))
    if kind is TypeKind.TIMESTAMP:
        parsed = dt.datetime.fromisoformat(str(value))
        if data_type.adjusted_to_utc and parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.UTC)
        if not data_type.adjusted_to_utc and parsed.tzinfo is not None:
            parsed = parsed.astimezone(dt.UTC).replace(tzinfo=None)
        return parsed
    if kind is TypeKind.UUID:
        return uuid_module.UUID(str(value))
    if kind in {TypeKind.BINARY, TypeKind.FIXED}:
        return _decode_binary(str(value))
    if kind is TypeKind.STRING:
        return str(value)
    raise ValueError(f"{kind.value} fields cannot declare a default value")


def _json_default(data_type: DataType, value: Any) -> Any:
    kind = data_type.kind
    if value is None or kind is TypeKind.UNKNOWN:
        return None
    if kind is TypeKind.BOOLEAN:
        return bool(value)
    if kind in {TypeKind.INT32, TypeKind.INT64}:
        return int(value)
    if kind in {TypeKind.FLOAT32, TypeKind.FLOAT64}:
        return float(value)
    if kind is TypeKind.DECIMAL:
        return str(value)
    if kind in {TypeKind.DATE, TypeKind.TIME, TypeKind.TIMESTAMP}:
        return value.isoformat() if hasattr(value, "isoformat") else str(value)
    if kind is TypeKind.UUID:
        return str(value)
    if kind in {TypeKind.BINARY, TypeKind.FIXED}:
        if isinstance(value, str):
            return value
        return binascii.hexlify(bytes(value)).decode("ascii")
    if kind is TypeKind.STRING:
        return str(value)
    raise ValueError(f"{kind.value} fields cannot declare a default value")


def _decode_binary(text: str) -> bytes:
    candidate = text[2:] if text.lower().startswith("0x") else text
    try:
        return binascii.unhexlify(candidate)
    except (binascii.Error, ValueError):
        try:
            return base64.b64decode(text, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ValueError("binary defaults must be hexadecimal") from exc


def _scalar_type_ids() -> Mapping[int, DataType]:
    candidates: list[tuple[Any, DataType]] = [
        (pa.bool_, _BOOLEAN),
        (pa.null, _UNKNOWN),
        (pa.int8, _INT32),
        (pa.int16, _INT32),
        (pa.int32, _INT32),
        (pa.int64, _INT64),
        (pa.uint8, _INT32),
        (pa.uint16, _INT32),
        (pa.uint32, _INT64),
        (pa.float16, _FLOAT32),
        (pa.float32, _FLOAT32),
        (pa.float64, _FLOAT64),
        (pa.string, _STRING),
        (pa.large_string, _STRING),
        (getattr(pa, "string_view", None), _STRING),
        (pa.binary, _BINARY),
        (pa.large_binary, _BINARY),
        (getattr(pa, "binary_view", None), _BINARY),
        (pa.date32, _DATE),
        (pa.date64, _DATE),
    ]
    resolved: dict[int, DataType] = {}
    for factory, data_type in candidates:
        if factory is None:  # pragma: no cover - depends on the PyArrow release
            continue
        resolved.setdefault(factory().id, data_type)
    return MappingProxyType(resolved)


_BOOLEAN = DataType(TypeKind.BOOLEAN)
_INT32 = DataType(TypeKind.INT32)
_INT64 = DataType(TypeKind.INT64)
_FLOAT32 = DataType(TypeKind.FLOAT32)
_FLOAT64 = DataType(TypeKind.FLOAT64)
_DATE = DataType(TypeKind.DATE)
_STRING = DataType(TypeKind.STRING)
_BINARY = DataType(TypeKind.BINARY)
_UUID = DataType(TypeKind.UUID)
_UNKNOWN = DataType(TypeKind.UNKNOWN)

_SCALAR_BY_TYPE_ID: Mapping[int, DataType] = _scalar_type_ids()
