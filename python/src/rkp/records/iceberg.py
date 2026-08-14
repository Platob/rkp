"""Apache Iceberg schema interoperability for records and Arrow.

This is the only :mod:`rkp.records` module that imports PyIceberg eagerly.
The records core and its JSON/YAML codecs remain usable without PyIceberg;
public facades import this optional adapter only when requested.
"""

from __future__ import annotations

import dataclasses
import typing
from collections.abc import Hashable, Iterable, Mapping, Sequence
from functools import cache
from types import EllipsisType
from typing import Any, Literal, cast

import pyarrow as pa
from pyiceberg.io.pyarrow import pyarrow_to_schema, schema_to_pyarrow
from pyiceberg.schema import Schema
from pyiceberg.types import ListType, MapType, NestedField, StructType

from ._metadata import (
    IDENTIFIER_FIELD_IDS,
    MAX_FIELD_SEQ,
    ORC_FIELD_ID,
    PARQUET_FIELD_ID,
    PRIMARY_KEY,
    SCHEMA_ID,
    field_seq_from_metadata,
    metadata_enabled,
    normalize_metadata,
)
from .arrow import (
    dataclass_into_arrow_field,
    into_arrow_field,
    record_into_arrow_field,
)
from .interop import is_record_type

__all__ = [
    "arrow_into_iceberg_field",
    "arrow_into_iceberg_schema",
    "dataclass_into_iceberg_field",
    "dataclass_into_iceberg_schema",
    "iceberg_fields_into_schema",
    "iceberg_into_arrow_field",
    "iceberg_into_arrow_schema",
    "into_iceberg_field",
    "into_iceberg_schema",
    "record_into_iceberg_field",
    "record_into_iceberg_schema",
]


def into_iceberg_field(
    value: Any,
    annotation: Any = ...,
    *,
    name: str | None = None,
    nullable: bool | None = None,
    owner: type[Any] | None = None,
    field_id_start: int = 1,
    format_version: int = 2,
    downcast_ns_timestamp_to_us: bool | None | EllipsisType = ...,
    localns: Mapping[str, Any] | None = None,
) -> NestedField:
    """Return one Iceberg field from an RKP, dataclass, or Arrow field.

    This is the canonical field-level adapter used by schema conversion.
    Dataclass types produce a named struct field; a standalone dataclass
    ``Field`` uses ``owner`` to resolve postponed annotations.  When the
    downcast policy is omitted or ``None``, nanoseconds are downcast for
    Iceberg v1/v2 and preserved for v3.
    """

    effective_downcast = _validate_conversion_options(
        field_id_start, format_version, downcast_ns_timestamp_to_us
    )

    if isinstance(value, NestedField):
        if annotation is not ...:
            raise TypeError("annotation must be omitted for an Iceberg field")
        if any(item is not None for item in (name, nullable, owner, localns)):
            raise TypeError("Iceberg fields cannot be overridden during conversion")
        _validate_iceberg_fields((value,))
        _validate_schema_format_version(
            iceberg_fields_into_schema(value), format_version
        )
        return value

    if isinstance(value, pa.Field):
        if annotation is not ...:
            raise TypeError("annotation must be omitted for an Arrow field")
        if owner is not None or localns is not None:
            raise TypeError("owner and localns are not valid for an Arrow field")
        arrow_field = _override_arrow_field(value, name=name, nullable=nullable)
        return arrow_into_iceberg_field(
            arrow_field,
            field_id_start=field_id_start,
            format_version=format_version,
            downcast_ns_timestamp_to_us=effective_downcast,
        )

    if isinstance(value, dataclasses.Field):
        if annotation is not ...:
            raise TypeError("annotation must be omitted for a dataclass Field")
        if localns is not None:
            raise TypeError("localns is only valid for a dataclass type")
        arrow_field = into_arrow_field(value, nullable=nullable, owner=owner)
        arrow_field = _override_arrow_field(arrow_field, name=name, nullable=None)
        return arrow_into_iceberg_field(
            arrow_field,
            field_id_start=field_id_start,
            format_version=format_version,
            downcast_ns_timestamp_to_us=effective_downcast,
        )

    if annotation is ... and (
        isinstance(value, type) or typing.get_origin(value) is not None
    ):
        if owner is not None:
            raise TypeError("owner is only valid for a dataclass Field")
        return dataclass_into_iceberg_field(
            value,
            name=name,
            nullable=False if nullable is None else nullable,
            field_id_start=field_id_start,
            format_version=format_version,
            downcast_ns_timestamp_to_us=effective_downcast,
            localns=localns,
        )

    if name is not None:
        raise TypeError("name is only valid for an existing field or dataclass type")
    if owner is not None or localns is not None:
        raise TypeError("owner/localns require a dataclass field/type")
    arrow_field = into_arrow_field(value, annotation, nullable=nullable)
    return arrow_into_iceberg_field(
        arrow_field,
        field_id_start=field_id_start,
        format_version=format_version,
        downcast_ns_timestamp_to_us=effective_downcast,
    )


def arrow_into_iceberg_field(
    field: pa.Field,
    *,
    field_id_start: int = 1,
    format_version: int = 2,
    downcast_ns_timestamp_to_us: bool | None | EllipsisType = ...,
) -> NestedField:
    """Convert one Arrow field, recursively assigning missing Iceberg IDs."""

    if not isinstance(field, pa.Field):
        raise TypeError("arrow_into_iceberg_field expects a pyarrow.Field")
    effective_downcast = _validate_conversion_options(
        field_id_start, format_version, downcast_ns_timestamp_to_us
    )
    converted = _convert_arrow_fields(
        (field,),
        field_id_start=field_id_start,
        format_version=format_version,
        downcast_ns_timestamp_to_us=effective_downcast,
    )
    return converted.fields[0]


def dataclass_into_iceberg_field(
    dataclass_type: type[Any],
    *,
    name: str | None = None,
    nullable: bool = False,
    field_id_start: int = 1,
    format_version: int = 2,
    downcast_ns_timestamp_to_us: bool | None | EllipsisType = ...,
    localns: Mapping[str, Any] | None = None,
) -> NestedField:
    """Infer one Iceberg struct field from an ordinary dataclass type."""

    effective_downcast = _validate_conversion_options(
        field_id_start, format_version, downcast_ns_timestamp_to_us
    )
    return _dataclass_iceberg_conversion(
        dataclass_type,
        name=name,
        nullable=nullable,
        field_id_start=field_id_start,
        format_version=format_version,
        downcast_ns_timestamp_to_us=effective_downcast,
        localns=localns,
    ).fields[0]


def record_into_iceberg_field(
    record_type: type[Any],
    *,
    name: str | None = None,
    nullable: bool = False,
    field_id_start: int = 1,
    format_version: int = 2,
    downcast_ns_timestamp_to_us: bool | None | EllipsisType = ...,
) -> NestedField:
    """Return a cached Iceberg struct field for a decorated record type."""

    if not is_record_type(record_type):
        raise TypeError("record_into_iceberg_field expects a decorated record type")
    effective_downcast = _validate_conversion_options(
        field_id_start, format_version, downcast_ns_timestamp_to_us
    )
    return _cached_record_iceberg_conversion(
        cast(Hashable, record_type),
        name=name,
        nullable=nullable,
        field_id_start=field_id_start,
        format_version=format_version,
        downcast_ns_timestamp_to_us=effective_downcast,
    ).fields[0]


def into_iceberg_schema(
    value: Any,
    *,
    schema_id: int | None = None,
    field_id_start: int = 1,
    identifier_field_ids: Iterable[int] | None = None,
    format_version: int = 2,
    downcast_ns_timestamp_to_us: bool | None | EllipsisType = ...,
    localns: Mapping[str, Any] | None = None,
    owner: type[Any] | None = None,
) -> Schema:
    """Return an Iceberg schema from a dataclass, Arrow schema, or Schema.

    Existing Iceberg schemas are returned unchanged when no schema or
    identifier override is supplied.  Dataclass and Arrow inputs receive
    deterministic IDs for every nested field before conversion.
    """

    effective_downcast = _validate_conversion_options(
        field_id_start, format_version, downcast_ns_timestamp_to_us
    )
    if localns is not None and not isinstance(localns, Mapping):
        raise TypeError("localns must be a mapping or None")

    if isinstance(value, Schema):
        if owner is not None:
            raise TypeError("owner is only valid for a dataclass Field")
        _validate_iceberg_fields(value.fields)
        _validate_schema_format_version(value, format_version)
        if schema_id is None and identifier_field_ids is None:
            return value
        selected_schema_id = value.schema_id if schema_id is None else schema_id
        selected_identifiers = (
            value.identifier_field_ids
            if identifier_field_ids is None
            else _identifier_tuple(identifier_field_ids)
        )
        _validate_schema_id(selected_schema_id)
        return iceberg_fields_into_schema(
            *value.fields,
            schema_id=selected_schema_id,
            identifier_field_ids=selected_identifiers,
        )

    if isinstance(value, NestedField):
        if owner is not None:
            raise TypeError("owner is only valid for a dataclass Field")
        selected_schema_id = 0 if schema_id is None else schema_id
        _validate_schema_id(selected_schema_id)
        conversion = _IcebergFields((value,), ())
        result = _conversion_into_schema(
            conversion,
            schema_id=selected_schema_id,
            identifier_field_ids=identifier_field_ids,
            flatten_struct=False,
        )
        _validate_schema_format_version(result, format_version)
        return result

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        if owner is not None:
            raise TypeError("owner is only valid for a dataclass Field")
        result = iceberg_fields_into_schema(
            *value,
            schema_id=0 if schema_id is None else schema_id,
            identifier_field_ids=identifier_field_ids,
        )
        _validate_schema_format_version(result, format_version)
        return result

    if isinstance(value, dataclasses.Field):
        conversion = _convert_arrow_fields(
            (into_arrow_field(value, owner=owner),),
            field_id_start=field_id_start,
            format_version=format_version,
            downcast_ns_timestamp_to_us=effective_downcast,
        )
        return _conversion_into_schema(
            conversion,
            schema_id=0 if schema_id is None else schema_id,
            identifier_field_ids=identifier_field_ids,
            flatten_struct=False,
        )
    elif isinstance(value, pa.Field):
        if owner is not None:
            raise TypeError("owner is only valid for a dataclass Field")
        conversion = _convert_arrow_fields(
            (value,),
            field_id_start=field_id_start,
            format_version=format_version,
            downcast_ns_timestamp_to_us=effective_downcast,
        )
        return _conversion_into_schema(
            conversion,
            schema_id=0 if schema_id is None else schema_id,
            identifier_field_ids=identifier_field_ids,
            flatten_struct=False,
        )
    elif isinstance(value, pa.Schema):
        if owner is not None:
            raise TypeError("owner is only valid for a dataclass Field")
        arrow_schema = value
    elif isinstance(value, type) or typing.get_origin(value) is not None:
        if owner is not None:
            raise TypeError("owner is only valid for a dataclass Field")
        if is_record_type(value):
            return record_into_iceberg_schema(
                value,
                schema_id=0 if schema_id is None else schema_id,
                field_id_start=field_id_start,
                identifier_field_ids=identifier_field_ids,
                format_version=format_version,
                downcast_ns_timestamp_to_us=effective_downcast,
            )
        return dataclass_into_iceberg_schema(
            value,
            schema_id=0 if schema_id is None else schema_id,
            field_id_start=field_id_start,
            identifier_field_ids=identifier_field_ids,
            format_version=format_version,
            downcast_ns_timestamp_to_us=effective_downcast,
            localns=localns,
        )
    else:
        if owner is not None:
            raise TypeError("owner is only valid for a dataclass Field")
        as_arrow = getattr(value, "as_arrow", None)
        if not callable(as_arrow):
            raise TypeError(
                "into_iceberg_schema expects a dataclass type/Field, Arrow "
                "Field/Schema, or Iceberg Field/Schema"
            )
        arrow_schema = as_arrow()
        if not isinstance(arrow_schema, pa.Schema):
            raise TypeError("as_arrow() must return a pyarrow.Schema")

    return arrow_into_iceberg_schema(
        arrow_schema,
        schema_id=schema_id,
        field_id_start=field_id_start,
        identifier_field_ids=identifier_field_ids,
        format_version=format_version,
        downcast_ns_timestamp_to_us=effective_downcast,
    )


def arrow_into_iceberg_schema(
    schema: pa.Schema,
    *,
    schema_id: int | None = None,
    field_id_start: int = 1,
    identifier_field_ids: Iterable[int] | None = None,
    format_version: int = 2,
    downcast_ns_timestamp_to_us: bool | None | EllipsisType = ...,
) -> Schema:
    """Convert an Arrow schema through PyIceberg's public schema adapter.

    PyIceberg requires an ID on every Arrow field, including struct members,
    list elements, and map keys/values. Explicit ``seq`` values are projected
    to standard field-ID metadata and preserved; missing values are allocated
    deterministically while avoiding all globally reserved sequences. The
    omitted downcast policy adapts to ``format_version``: v1/v2 use
    microseconds, while v3 preserves nanoseconds.
    """

    if not isinstance(schema, pa.Schema):
        raise TypeError("arrow_into_iceberg_schema expects a pyarrow.Schema")
    effective_downcast = _validate_conversion_options(
        field_id_start, format_version, downcast_ns_timestamp_to_us
    )

    inferred_schema_id = _schema_id_from_metadata(schema.metadata)
    selected_schema_id = inferred_schema_id if schema_id is None else schema_id
    _validate_schema_id(selected_schema_id)

    converted = _convert_arrow_fields(
        tuple(schema),
        field_id_start=field_id_start,
        format_version=format_version,
        downcast_ns_timestamp_to_us=effective_downcast,
    )
    return _conversion_into_schema(
        converted,
        schema_id=selected_schema_id,
        identifier_field_ids=(
            _identifier_ids_from_metadata(schema.metadata)
            if identifier_field_ids is None
            else identifier_field_ids
        ),
    )


def iceberg_fields_into_schema(
    *fields: NestedField,
    schema_id: int = 0,
    identifier_field_ids: Iterable[int] | None = None,
) -> Schema:
    """Safely compose already-converted Iceberg fields into a schema.

    Unlike PyIceberg's constructor, this validates positive, globally unique
    IDs throughout nested structs, lists, and maps before composition.
    """

    return _compose_iceberg_schema(
        tuple(fields),
        schema_id=schema_id,
        identifier_field_ids=identifier_field_ids,
    )


def _compose_iceberg_schema(
    fields: tuple[NestedField, ...],
    *,
    schema_id: int,
    identifier_field_ids: Iterable[int] | None,
    available_ids: set[int] | None = None,
) -> Schema:
    """Compose a schema, optionally reusing a completed field validation."""

    _validate_schema_id(schema_id)
    if available_ids is None:
        available_ids = _validate_iceberg_fields(fields)
    identifiers = (
        () if identifier_field_ids is None else _identifier_tuple(identifier_field_ids)
    )
    unknown = set(identifiers).difference(available_ids)
    if unknown:
        raise ValueError(
            "identifier_field_ids are not contained in the Iceberg fields: "
            + ", ".join(map(str, sorted(unknown)))
        )
    try:
        return Schema(
            *fields,
            schema_id=schema_id,
            identifier_field_ids=identifiers,
        )
    except (TypeError, ValueError) as exc:
        raise TypeError(f"cannot compose Iceberg schema: {exc}") from exc


def iceberg_into_arrow_schema(
    schema: Schema,
    *,
    metadata: Mapping[str | bytes, Any] | None = None,
    include_field_ids: bool = True,
) -> pa.Schema:
    """Convert an Iceberg schema to Arrow while retaining schema identity.

    Iceberg identifiers are represented as ``primary_key`` field metadata so
    a subsequent RKP Arrow-to-Iceberg conversion can reconstruct them.
    """

    if not isinstance(schema, Schema):
        raise TypeError("iceberg_into_arrow_schema expects an Iceberg Schema")
    if metadata is not None and not isinstance(metadata, Mapping):
        raise TypeError("metadata must be a mapping or None")
    if type(include_field_ids) is not bool:
        raise TypeError("include_field_ids must be bool")
    _validate_iceberg_fields(schema.fields)

    schema_metadata = normalize_metadata(metadata or {})
    schema_metadata[SCHEMA_ID] = str(schema.schema_id).encode("ascii")
    schema_metadata.pop(IDENTIFIER_FIELD_IDS, None)
    if include_field_ids and len(schema.identifier_field_ids) > 1:
        schema_metadata[IDENTIFIER_FIELD_IDS] = b",".join(
            str(field_id).encode("ascii") for field_id in schema.identifier_field_ids
        )
    converted = schema_to_pyarrow(
        schema,
        metadata=schema_metadata,
        include_field_ids=True,
    )
    if not isinstance(converted, pa.Schema):
        raise TypeError("PyIceberg returned a non-schema Arrow value")
    if schema.identifier_field_ids:
        converted = pa.schema(
            _mark_identifier_fields(list(converted), set(schema.identifier_field_ids)),
            metadata=converted.metadata,
        )
    if not include_field_ids:
        converted = pa.schema(
            _strip_field_ids(list(converted)),
            metadata=converted.metadata,
        )
    return converted


def iceberg_into_arrow_field(
    field: NestedField,
    *,
    include_field_id: bool = True,
    primary_key: bool = False,
    identifier_field_ids: Iterable[int] | None = None,
) -> pa.Field:
    """Convert one Iceberg field through PyIceberg's public Arrow adapter."""

    if not isinstance(field, NestedField):
        raise TypeError("iceberg_into_arrow_field expects an Iceberg NestedField")
    if type(include_field_id) is not bool:
        raise TypeError("include_field_id must be bool")
    if type(primary_key) is not bool:
        raise TypeError("primary_key must be bool")
    _validate_iceberg_fields((field,))
    identifiers = (
        () if identifier_field_ids is None else _identifier_tuple(identifier_field_ids)
    )
    if primary_key and field.field_id not in identifiers:
        identifiers = (*identifiers, field.field_id)
    unknown_identifiers = set(identifiers).difference(_iceberg_field_ids(field))
    if unknown_identifiers:
        raise ValueError(
            "identifier_field_ids are not contained in the Iceberg field: "
            + ", ".join(map(str, sorted(unknown_identifiers)))
        )
    schema = iceberg_fields_into_schema(
        field,
        identifier_field_ids=identifiers,
    )
    # Convert with IDs internally even when the caller does not want them so
    # nested identifier membership can still be projected to primary metadata.
    converted = schema_to_pyarrow(schema, include_field_ids=True)
    if not isinstance(converted, pa.Schema) or len(converted) != 1:
        raise TypeError("PyIceberg returned an invalid Arrow field wrapper")
    result = converted.field(0)
    if identifiers:
        result = _mark_identifier_fields([result], set(identifiers))[0]
    if not include_field_id:
        result = _strip_field_ids([result])[0]
    return result


def _iceberg_field_ids(field: NestedField) -> set[int]:
    result = {field.field_id}
    field_type = field.field_type
    if isinstance(field_type, StructType):
        for child in field_type.fields:
            result.update(_iceberg_field_ids(child))
    elif isinstance(field_type, ListType):
        result.update(_iceberg_field_ids(field_type.element_field))
    elif isinstance(field_type, MapType):
        result.update(_iceberg_field_ids(field_type.key_field))
        result.update(_iceberg_field_ids(field_type.value_field))
    return result


def _validate_iceberg_fields(fields: Iterable[NestedField]) -> set[int]:
    used: dict[int, str] = {}

    def visit(field: NestedField, path: str) -> None:
        if not isinstance(field, NestedField):
            raise TypeError("Iceberg schema fields must be NestedField values")
        field_path = _join_path(path, field.name)
        _validate_field_id(field.field_id, field_path)
        previous = used.get(field.field_id)
        if previous is not None:
            raise ValueError(
                f"duplicate Iceberg field ID {field.field_id} at "
                f"{field_path!r}; already used by {previous!r}"
            )
        used[field.field_id] = field_path
        field_type = field.field_type
        if isinstance(field_type, StructType):
            for child in field_type.fields:
                visit(child, field_path)
        elif isinstance(field_type, ListType):
            visit(field_type.element_field, field_path)
        elif isinstance(field_type, MapType):
            visit(field_type.key_field, field_path)
            visit(field_type.value_field, field_path)

    for root in fields:
        visit(root, "")
    return set(used)


def dataclass_into_iceberg_schema(
    dataclass_type: type[Any],
    *,
    schema_id: int = 0,
    field_id_start: int = 1,
    identifier_field_ids: Iterable[int] | None = None,
    format_version: int = 2,
    downcast_ns_timestamp_to_us: bool | None | EllipsisType = ...,
    localns: Mapping[str, Any] | None = None,
) -> Schema:
    """Compose an ordinary dataclass schema from its canonical struct field."""

    effective_downcast = _validate_conversion_options(
        field_id_start, format_version, downcast_ns_timestamp_to_us
    )
    conversion = _dataclass_iceberg_conversion(
        dataclass_type,
        name=None,
        nullable=False,
        field_id_start=field_id_start,
        format_version=format_version,
        downcast_ns_timestamp_to_us=effective_downcast,
        localns=localns,
    )
    return _conversion_into_schema(
        conversion,
        schema_id=schema_id,
        identifier_field_ids=identifier_field_ids,
        flatten_struct=True,
    )


def record_into_iceberg_schema(
    record_type: type[Any],
    *,
    schema_id: int = 0,
    field_id_start: int = 1,
    identifier_field_ids: Iterable[int] | None = None,
    format_version: int = 2,
    downcast_ns_timestamp_to_us: bool | None | EllipsisType = ...,
) -> Schema:
    """Return a cached Iceberg schema for a decorated record type."""

    if not is_record_type(record_type):
        raise TypeError("record_into_iceberg_schema expects a decorated record type")
    effective_downcast = _validate_conversion_options(
        field_id_start, format_version, downcast_ns_timestamp_to_us
    )
    normalized_identifiers = (
        None
        if identifier_field_ids is None
        else _identifier_tuple(identifier_field_ids)
    )
    return _cached_record_into_iceberg_schema(
        cast(Hashable, record_type),
        schema_id=schema_id,
        field_id_start=field_id_start,
        identifier_field_ids=normalized_identifiers,
        format_version=format_version,
        downcast_ns_timestamp_to_us=effective_downcast,
    )


@cache
def _cached_record_into_iceberg_schema(
    record_type: type[Any],
    *,
    schema_id: int,
    field_id_start: int,
    identifier_field_ids: tuple[int, ...] | None,
    format_version: int,
    downcast_ns_timestamp_to_us: bool,
) -> Schema:
    conversion = _cached_record_iceberg_conversion(
        cast(Hashable, record_type),
        name=None,
        nullable=False,
        field_id_start=field_id_start,
        format_version=format_version,
        downcast_ns_timestamp_to_us=downcast_ns_timestamp_to_us,
    )
    return _conversion_into_schema(
        conversion,
        schema_id=schema_id,
        identifier_field_ids=identifier_field_ids,
        flatten_struct=True,
    )


@dataclasses.dataclass(frozen=True, slots=True)
class _IcebergFields:
    fields: tuple[NestedField, ...]
    identifier_field_ids: tuple[int, ...]


def _conversion_into_schema(
    conversion: _IcebergFields,
    *,
    schema_id: int,
    identifier_field_ids: Iterable[int] | None,
    flatten_struct: bool = False,
) -> Schema:
    fields = conversion.fields
    if (
        flatten_struct
        and len(fields) == 1
        and isinstance(fields[0].field_type, StructType)
    ):
        fields = tuple(fields[0].field_type.fields)
    available_ids = _validate_iceberg_fields(fields)
    inferred = tuple(
        field_id
        for field_id in conversion.identifier_field_ids
        if field_id in available_ids
    )
    selected = inferred if identifier_field_ids is None else identifier_field_ids
    return _compose_iceberg_schema(
        fields,
        schema_id=schema_id,
        identifier_field_ids=selected,
        available_ids=available_ids,
    )


def _convert_arrow_fields(
    fields: Sequence[pa.Field],
    *,
    field_id_start: int,
    format_version: int,
    downcast_ns_timestamp_to_us: bool,
) -> _IcebergFields:
    """Convert an Arrow field forest through one coordinated ID context."""

    effective_downcast = _validate_conversion_options(
        field_id_start, format_version, downcast_ns_timestamp_to_us
    )
    if any(not isinstance(field, pa.Field) for field in fields):
        raise TypeError("Iceberg conversion expects pyarrow.Field values")
    _validate_arrow_timestamp_zones(fields)
    identified, identifiers = _FieldSeqAllocator(
        tuple(fields), start=field_id_start
    ).apply()
    try:
        converted = pyarrow_to_schema(
            pa.schema(identified),
            downcast_ns_timestamp_to_us=effective_downcast,
            format_version=cast(Literal[1, 2, 3], format_version),
        )
    except Exception as exc:
        raise TypeError(f"cannot convert Arrow field to Iceberg: {exc}") from exc
    return _IcebergFields(tuple(converted.fields), identifiers)


def _validate_arrow_timestamp_zones(fields: Sequence[pa.Field]) -> None:
    """Reject zones PyIceberg would erase when converting nano timestamps."""

    allowed = {None, "UTC", "Etc/UTC", "Z", "+00:00"}

    def visit_type(arrow_type: pa.DataType, path: str) -> None:
        if pa.types.is_timestamp(arrow_type):
            if arrow_type.tz not in allowed:
                raise TypeError(
                    f"Iceberg timestamp at {path!r} must be naive or use a UTC "
                    f"timezone; got {arrow_type.tz!r}"
                )
        elif pa.types.is_struct(arrow_type):
            visit_fields(list(arrow_type), path)
        elif _is_list_like(arrow_type):
            child = arrow_type.value_field
            visit_type(child.type, _join_path(path, child.name))
        elif pa.types.is_map(arrow_type):
            visit_type(arrow_type.key_type, _join_path(path, "key"))
            visit_type(arrow_type.item_type, _join_path(path, "value"))
        elif pa.types.is_dictionary(arrow_type):
            visit_type(arrow_type.value_type, path)

    def visit_fields(items: Sequence[pa.Field], path: str) -> None:
        for item in items:
            item_path = _join_path(path, item.name)
            visit_type(item.type, item_path)

    visit_fields(fields, "")


def _dataclass_iceberg_conversion(
    dataclass_type: type[Any],
    *,
    name: str | None,
    nullable: bool,
    field_id_start: int,
    format_version: int,
    downcast_ns_timestamp_to_us: bool,
    localns: Mapping[str, Any] | None,
) -> _IcebergFields:
    arrow_field = dataclass_into_arrow_field(
        dataclass_type,
        name=name,
        nullable=nullable,
        localns=localns,
    )
    return _convert_arrow_fields(
        (arrow_field,),
        field_id_start=field_id_start,
        format_version=format_version,
        downcast_ns_timestamp_to_us=downcast_ns_timestamp_to_us,
    )


@cache
def _cached_record_iceberg_conversion(
    record_type: type[Any],
    *,
    name: str | None,
    nullable: bool,
    field_id_start: int,
    format_version: int,
    downcast_ns_timestamp_to_us: bool,
) -> _IcebergFields:
    return _convert_arrow_fields(
        (
            record_into_arrow_field(
                cast(Hashable, record_type),
                name=name,
                nullable=nullable,
            ),
        ),
        field_id_start=field_id_start,
        format_version=format_version,
        downcast_ns_timestamp_to_us=downcast_ns_timestamp_to_us,
    )


@dataclasses.dataclass(slots=True)
class _FieldSeqAllocator:
    fields: Sequence[pa.Field]
    start: int = 1
    _reserved: dict[int, str] = dataclasses.field(init=False, default_factory=dict)
    _next: int = dataclasses.field(init=False)
    _identifiers: list[int] = dataclasses.field(init=False, default_factory=list)
    _all_parquet_identified: bool = dataclasses.field(init=False, default=True)

    def __post_init__(self) -> None:
        self._next = self.start
        self._collect_explicit_fields(self.fields, path="")

    def apply(self) -> tuple[tuple[pa.Field, ...], tuple[int, ...]]:
        if self._all_parquet_identified:
            return tuple(self.fields), tuple(self._identifiers)
        self._identifiers.clear()
        fields = self._assign_fields(self.fields, path="")
        return tuple(fields), tuple(self._identifiers)

    def _collect_explicit_fields(
        self, fields: Sequence[pa.Field], *, path: str
    ) -> None:
        for field in fields:
            field_path = _join_path(path, field.name)
            metadata = field.metadata or {}
            explicit = _field_seq(field, field_path, metadata=metadata)
            if explicit is None:
                self._all_parquet_identified = False
            else:
                previous = self._reserved.get(explicit)
                if previous is not None:
                    raise ValueError(
                        f"duplicate Iceberg field ID {explicit} at "
                        f"{field_path!r}; already used by {previous!r}"
                    )
                self._reserved[explicit] = field_path
                if PARQUET_FIELD_ID not in metadata:
                    self._all_parquet_identified = False
                if metadata_enabled(metadata.get(PRIMARY_KEY)):
                    self._identifiers.append(explicit)
        for field in fields:
            field_path = _join_path(path, field.name)
            self._collect_explicit_type(field.type, path=field_path)

    def _collect_explicit_type(self, arrow_type: pa.DataType, *, path: str) -> None:
        if pa.types.is_struct(arrow_type):
            self._collect_explicit_fields(list(arrow_type), path=path)
        elif _is_list_like(arrow_type):
            self._collect_explicit_fields([arrow_type.value_field], path=path)
        elif pa.types.is_map(arrow_type):
            self._collect_explicit_fields(
                [arrow_type.key_field, arrow_type.item_field], path=path
            )

    def _assign_fields(
        self, fields: Sequence[pa.Field], *, path: str
    ) -> list[pa.Field]:
        # Allocate all siblings before descending.  This matches Iceberg's
        # conventional fresh-ID traversal and keeps top-level IDs compact.
        staged: list[tuple[pa.Field, str, int, dict[bytes, bytes]]] = []
        for field in fields:
            field_path = _join_path(path, field.name)
            metadata = dict(field.metadata or {})
            field_id = _field_seq(field, field_path, metadata=metadata)
            if field_id is None:
                field_id = self._allocate()
            metadata[PARQUET_FIELD_ID] = str(field_id).encode("ascii")
            if metadata_enabled(metadata.get(PRIMARY_KEY)):
                self._identifiers.append(field_id)
            staged.append((field, field_path, field_id, metadata))

        result: list[pa.Field] = []
        for field, field_path, _field_id, metadata in staged:
            result.append(
                pa.field(
                    field.name,
                    self._assign_type(field.type, path=field_path),
                    nullable=field.nullable,
                    metadata=metadata,
                )
            )
        return result

    def _assign_type(self, arrow_type: pa.DataType, *, path: str) -> pa.DataType:
        if pa.types.is_struct(arrow_type):
            return pa.struct(self._assign_fields(list(arrow_type), path=path))
        if pa.types.is_list(arrow_type):
            child = self._assign_fields([arrow_type.value_field], path=path)[0]
            return pa.list_(child)
        if pa.types.is_large_list(arrow_type):
            child = self._assign_fields([arrow_type.value_field], path=path)[0]
            return pa.large_list(child)
        if pa.types.is_fixed_size_list(arrow_type):
            child = self._assign_fields([arrow_type.value_field], path=path)[0]
            return pa.list_(child, arrow_type.list_size)
        if pa.types.is_map(arrow_type):
            key, value = self._assign_fields(
                [arrow_type.key_field, arrow_type.item_field], path=path
            )
            return pa.map_(key, value, keys_sorted=arrow_type.keys_sorted)
        return arrow_type

    def _allocate(self) -> int:
        while self._next in self._reserved:
            self._next += 1
        if self._next > MAX_FIELD_SEQ:
            raise ValueError("no valid field seq values remain")
        value = self._next
        self._reserved[value] = "<generated>"
        self._next += 1
        return value


def _mark_identifier_fields(
    fields: Sequence[pa.Field], identifiers: set[int]
) -> list[pa.Field]:
    result: list[pa.Field] = []
    for field in fields:
        metadata = dict(field.metadata or {})
        field_id = _field_seq(field, field.name, metadata=metadata)
        if field_id in identifiers:
            metadata[PRIMARY_KEY] = b"true"
        result.append(
            pa.field(
                field.name,
                _mark_identifier_type(field.type, identifiers),
                nullable=field.nullable,
                metadata=metadata or None,
            )
        )
    return result


def _mark_identifier_type(
    arrow_type: pa.DataType, identifiers: set[int]
) -> pa.DataType:
    if pa.types.is_struct(arrow_type):
        return pa.struct(_mark_identifier_fields(list(arrow_type), identifiers))
    if pa.types.is_list(arrow_type):
        return pa.list_(
            _mark_identifier_fields([arrow_type.value_field], identifiers)[0]
        )
    if pa.types.is_large_list(arrow_type):
        return pa.large_list(
            _mark_identifier_fields([arrow_type.value_field], identifiers)[0]
        )
    if pa.types.is_fixed_size_list(arrow_type):
        child = _mark_identifier_fields([arrow_type.value_field], identifiers)[0]
        return pa.list_(child, arrow_type.list_size)
    if pa.types.is_map(arrow_type):
        key, value = _mark_identifier_fields(
            [arrow_type.key_field, arrow_type.item_field], identifiers
        )
        return pa.map_(key, value, keys_sorted=arrow_type.keys_sorted)
    return arrow_type


def _strip_field_ids(fields: Sequence[pa.Field]) -> list[pa.Field]:
    result: list[pa.Field] = []
    for field in fields:
        metadata = dict(field.metadata or {})
        metadata.pop(PARQUET_FIELD_ID, None)
        metadata.pop(ORC_FIELD_ID, None)
        result.append(
            pa.field(
                field.name,
                _strip_field_id_type(field.type),
                nullable=field.nullable,
                metadata=metadata or None,
            )
        )
    return result


def _strip_field_id_type(arrow_type: pa.DataType) -> pa.DataType:
    if pa.types.is_struct(arrow_type):
        return pa.struct(_strip_field_ids(list(arrow_type)))
    if pa.types.is_list(arrow_type):
        return pa.list_(_strip_field_ids([arrow_type.value_field])[0])
    if pa.types.is_large_list(arrow_type):
        return pa.large_list(_strip_field_ids([arrow_type.value_field])[0])
    if pa.types.is_fixed_size_list(arrow_type):
        child = _strip_field_ids([arrow_type.value_field])[0]
        return pa.list_(child, arrow_type.list_size)
    if pa.types.is_map(arrow_type):
        key, value = _strip_field_ids([arrow_type.key_field, arrow_type.item_field])
        return pa.map_(key, value, keys_sorted=arrow_type.keys_sorted)
    return arrow_type


def _field_seq(
    field: pa.Field,
    path: str,
    *,
    metadata: Mapping[bytes, bytes] | None | EllipsisType = ...,
) -> int | None:
    snapshot = field.metadata if metadata is ... else metadata
    return field_seq_from_metadata(snapshot, path=path)


def _identifier_tuple(values: Iterable[int]) -> tuple[int, ...]:
    if isinstance(values, (str, bytes, bytearray)):
        raise TypeError("identifier_field_ids must be an iterable of integers")
    try:
        result = tuple(values)
    except TypeError as exc:
        raise TypeError("identifier_field_ids must be an iterable of integers") from exc
    for value in result:
        _validate_field_id(value, "identifier_field_ids")
    if len(set(result)) != len(result):
        raise ValueError("identifier_field_ids must be unique")
    return result


def _override_arrow_field(
    field: pa.Field,
    *,
    name: str | None,
    nullable: bool | None,
) -> pa.Field:
    if name is not None and not isinstance(name, str):
        raise TypeError("name must be a string or None")
    if nullable is not None and type(nullable) is not bool:
        raise TypeError("nullable must be bool or None")
    if name is None and nullable is None:
        return field
    return pa.field(
        field.name if name is None else name,
        field.type,
        nullable=field.nullable if nullable is None else nullable,
        metadata=field.metadata,
    )


def _validate_conversion_options(
    field_id_start: int,
    format_version: int,
    downcast_ns_timestamp_to_us: bool | None | EllipsisType,
) -> bool:
    _validate_field_id_start(field_id_start)
    _validate_format_version(format_version)
    if downcast_ns_timestamp_to_us is None or downcast_ns_timestamp_to_us is ...:
        return format_version < 3
    if type(downcast_ns_timestamp_to_us) is not bool:
        raise TypeError("downcast_ns_timestamp_to_us must be bool, None, or Ellipsis")
    return downcast_ns_timestamp_to_us


def _validate_schema_format_version(schema: Schema, format_version: int) -> None:
    try:
        schema.check_format_version_compatibility(format_version)
    except Exception as exc:
        raise TypeError(
            f"Iceberg schema is incompatible with format version "
            f"{format_version}: {exc}"
        ) from exc


def _validate_field_id(value: int, path: str) -> None:
    if type(value) is not int or not 1 <= value <= MAX_FIELD_SEQ:
        raise ValueError(
            f"Iceberg field ID at {path!r} must be between 1 and {MAX_FIELD_SEQ}"
        )


def _validate_field_id_start(value: int) -> None:
    if type(value) is not int or not 1 <= value <= MAX_FIELD_SEQ:
        raise ValueError(f"field_id_start must be between 1 and {MAX_FIELD_SEQ}")


def _validate_schema_id(value: int) -> None:
    if type(value) is not int or value < 0:
        raise ValueError("schema_id must be a non-negative integer")


def _validate_format_version(value: int) -> None:
    if type(value) is not int or value not in (1, 2, 3):
        raise ValueError("format_version must be 1, 2, or 3")


def _schema_id_from_metadata(metadata: Mapping[bytes, bytes] | None) -> int:
    if not metadata or SCHEMA_ID not in metadata:
        return 0
    raw = metadata[SCHEMA_ID]
    try:
        value = int(raw.decode("ascii"))
    except (AttributeError, UnicodeDecodeError, ValueError) as exc:
        raise ValueError(f"invalid Iceberg schema ID metadata {raw!r}") from exc
    _validate_schema_id(value)
    return value


def _identifier_ids_from_metadata(
    metadata: Mapping[bytes, bytes] | None,
) -> tuple[int, ...] | None:
    if not metadata or IDENTIFIER_FIELD_IDS not in metadata:
        return None
    raw = metadata[IDENTIFIER_FIELD_IDS]
    try:
        values = tuple(int(item) for item in raw.decode("ascii").split(","))
    except (AttributeError, UnicodeDecodeError, ValueError) as exc:
        raise ValueError(
            f"invalid Iceberg identifier field ID metadata {raw!r}"
        ) from exc
    return _identifier_tuple(values)


def _is_list_like(arrow_type: pa.DataType) -> bool:
    return (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    )


def _join_path(parent: str, child: str) -> str:
    return f"{parent}.{child}" if parent else child
