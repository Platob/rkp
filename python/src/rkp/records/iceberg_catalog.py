"""Catalog-level Iceberg operations for records and Arrow.

:mod:`rkp.records.iceberg` converts schemas; this module drives a live
PyIceberg catalog with the same definitions: create the namespace and table,
project partition specs and sort orders from field roles, write records, and
read them back as records.  Format versions 1, 2, and 3 are configured through
the standard ``format-version`` table property.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Iterator, Mapping
from types import EllipsisType
from typing import Any, Literal, TypeVar, cast

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import (
    PARTITION_FIELD_ID_START,
    PartitionField,
    PartitionSpec,
)
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.sorting import (
    NullOrder,
    SortDirection,
    SortField,
    SortOrder,
)
from pyiceberg.transforms import (
    IdentityTransform,
    Transform,
    UnknownTransform,
    parse_transform,
)

from ._metadata import INDEX_KEY, PARTITION_KEY, metadata_enabled
from .arrow import (
    arrow_into_records,
    into_arrow_schema,
    records_into_arrow_batches,
    schema_name,
    table_name,
)
from .iceberg import iceberg_into_arrow_schema, into_iceberg_schema

__all__ = [
    "create_iceberg_table",
    "iceberg_table_into_arrow",
    "iceberg_table_into_records",
    "into_iceberg_partition_spec",
    "into_iceberg_sort_order",
    "load_iceberg_table",
    "records_into_arrow_table",
    "records_into_iceberg_table",
    "sync_iceberg_table_schema",
]

T = TypeVar("T")

Identifier = str | tuple[str, ...]

_FORMAT_VERSION = "format-version"
_NUMERIC = re.compile(r"[+-]?\d+")


def create_iceberg_table(
    catalog: Catalog,
    value: Any,
    *,
    identifier: Identifier | None = None,
    format_version: int = 2,
    location: str | None = None,
    properties: Mapping[str, str] | None = None,
    partition_spec: PartitionSpec | None = None,
    partition_keys: Iterable[str] | None = None,
    sort_order: SortOrder | None = None,
    schema_id: int = 0,
    field_id_start: int = 1,
    identifier_field_ids: Iterable[int] | None = None,
    downcast_ns_timestamp_to_us: bool | None | EllipsisType = ...,
    create_namespace: bool = True,
    exists_ok: bool = True,
) -> Table:
    """Create (or load) the catalog table described by ``value``.

    The identifier defaults to the record's ``schema_name``/``table_name``
    metadata.  Partition specs and sort orders are projected from the
    ``partition_key`` and ``index_key`` field roles unless given explicitly.
    """

    _validate_catalog(catalog)
    _validate_format_version(format_version)
    resolved = resolve_identifier(value, identifier)
    schema = into_iceberg_schema(
        value,
        schema_id=schema_id,
        field_id_start=field_id_start,
        identifier_field_ids=identifier_field_ids,
        format_version=format_version,
        downcast_ns_timestamp_to_us=downcast_ns_timestamp_to_us,
    )
    if partition_spec is None:
        partition_spec = into_iceberg_partition_spec(
            value,
            schema=schema,
            partition_keys=partition_keys,
        )
    if sort_order is None:
        sort_order = into_iceberg_sort_order(value, schema=schema)

    if create_namespace and len(resolved) > 1:
        catalog.create_namespace_if_not_exists(resolved[:-1])
    if exists_ok and catalog.table_exists(resolved):
        return catalog.load_table(resolved)

    table_properties: dict[str, str] = {_FORMAT_VERSION: str(format_version)}
    table_properties.update({str(k): str(v) for k, v in (properties or {}).items()})
    try:
        return catalog.create_table(
            identifier=resolved,
            schema=schema,
            location=location,
            partition_spec=partition_spec,
            sort_order=sort_order,
            properties=table_properties,
        )
    except NotImplementedError as exc:
        # RKP converts v3 schemas, but table metadata still has to be written
        # by the runtime.  Surface that boundary instead of a bare traceback.
        raise NotImplementedError(
            f"the installed PyIceberg ({_pyiceberg_version()}) cannot write "
            f"format version {format_version} table metadata: {exc}"
        ) from exc


def load_iceberg_table(
    catalog: Catalog,
    value: Any,
    *,
    identifier: Identifier | None = None,
) -> Table:
    """Load one catalog table for a record type, dataclass, or identifier."""

    _validate_catalog(catalog)
    return catalog.load_table(resolve_identifier(value, identifier))


def sync_iceberg_table_schema(
    table: Table,
    value: Any,
    *,
    format_version: int | None = None,
    downcast_ns_timestamp_to_us: bool | None | EllipsisType = ...,
) -> Table:
    """Evolve a live table so it contains every column of ``value``.

    Existing columns keep their IDs; new columns are added and required
    columns that disappeared become optional, which is exactly Iceberg's
    ``union by name`` evolution.
    """

    _validate_table(table)
    selected_version = (
        int(table.format_version) if format_version is None else format_version
    )
    _validate_format_version(selected_version)
    schema = into_iceberg_schema(
        value,
        format_version=selected_version,
        downcast_ns_timestamp_to_us=downcast_ns_timestamp_to_us,
    )
    with table.update_schema() as update:
        update.union_by_name(schema)
    return table


def records_into_arrow_table(
    records: Iterable[Any],
    schema: Schema | pa.Schema,
    *,
    record_type: type[Any] | None = None,
    batch_size: int = 65_536,
) -> pa.Table:
    """Materialize records as an Arrow table matching an Iceberg schema."""

    arrow_schema = (
        iceberg_into_arrow_schema(schema, include_field_ids=False)
        if isinstance(schema, Schema)
        else schema
    )
    batches = list(
        records_into_arrow_batches(
            records,
            batch_size=batch_size,
            record_type=record_type,
            schema=arrow_schema,
        )
    )
    return pa.Table.from_batches(batches, schema=arrow_schema)


def records_into_iceberg_table(
    table: Table,
    records: Iterable[Any],
    *,
    record_type: type[Any] | None = None,
    batch_size: int = 65_536,
    mode: Literal["append", "overwrite"] = "append",
    snapshot_properties: Mapping[str, str] | None = None,
) -> Table:
    """Write records into a live Iceberg table through Arrow."""

    _validate_table(table)
    if mode not in {"append", "overwrite"}:
        raise ValueError("mode must be 'append' or 'overwrite'")
    arrow_table = records_into_arrow_table(
        records,
        table.schema(),
        record_type=record_type,
        batch_size=batch_size,
    )
    properties = dict(snapshot_properties or {})
    if mode == "append":
        table.append(arrow_table, snapshot_properties=properties)
    else:
        table.overwrite(arrow_table, snapshot_properties=properties)
    return table


def iceberg_table_into_arrow(
    table: Table,
    *,
    row_filter: Any = None,
    selected_fields: tuple[str, ...] = ("*",),
    limit: int | None = None,
    case_sensitive: bool = True,
) -> pa.Table:
    """Scan a live Iceberg table into one Arrow table."""

    _validate_table(table)
    scan = table.scan(
        selected_fields=selected_fields,
        case_sensitive=case_sensitive,
        limit=limit,
        **({} if row_filter is None else {"row_filter": row_filter}),
    )
    return scan.to_arrow()


def iceberg_table_into_records(
    record_type: type[T],
    table: Table,
    *,
    row_filter: Any = None,
    limit: int | None = None,
    case_sensitive: bool = True,
    safe: bool = True,
    on_error: Literal["raise", "default"] = "raise",
) -> Iterator[T]:
    """Scan a live Iceberg table and construct records from its rows.

    Schema validation is relaxed because Iceberg's Arrow projection uses
    equivalent large physical types; row conversion stays typed and
    alias-aware.
    """

    arrow_table = iceberg_table_into_arrow(
        table,
        row_filter=row_filter,
        limit=limit,
        case_sensitive=case_sensitive,
    )
    return arrow_into_records(
        record_type,
        arrow_table,
        safe=safe,
        on_error=on_error,
        validate_schema=False,
    )


def into_iceberg_partition_spec(
    value: Any,
    *,
    schema: Schema | None = None,
    partition_keys: Iterable[str] | None = None,
    spec_id: int = 0,
    field_id_start: int = PARTITION_FIELD_ID_START,
) -> PartitionSpec:
    """Project a partition spec from ``partition_key`` field roles.

    A boolean role partitions by identity, an integer role also fixes the
    partition column order, and a string role names an Iceberg transform such
    as ``"day"``, ``"bucket[16]"``, or ``"truncate[8]"``.
    """

    iceberg_schema = schema if schema is not None else into_iceberg_schema(value)
    roles = _field_roles(value, PARTITION_KEY, partition_keys)
    fields: list[PartitionField] = []
    for index, (name, transform) in enumerate(roles):
        source = iceberg_schema.find_field(name)
        fields.append(
            PartitionField(
                source_id=source.field_id,
                field_id=field_id_start + index,
                transform=transform,
                name=_partition_name(name, transform),
            )
        )
    return PartitionSpec(*fields, spec_id=spec_id)


def into_iceberg_sort_order(
    value: Any,
    *,
    schema: Schema | None = None,
    sort_keys: Iterable[str] | None = None,
    order_id: int = 1,
) -> SortOrder:
    """Project a sort order from ``index_key`` field roles."""

    iceberg_schema = schema if schema is not None else into_iceberg_schema(value)
    roles = _field_roles(value, INDEX_KEY, sort_keys)
    if not roles:
        return SortOrder(order_id=0)
    return SortOrder(
        *(
            SortField(
                source_id=iceberg_schema.find_field(name).field_id,
                transform=transform,
                direction=SortDirection.ASC,
                null_order=NullOrder.NULLS_LAST,
            )
            for name, transform in roles
        ),
        order_id=order_id,
    )


def resolve_identifier(value: Any, identifier: Identifier | None) -> tuple[str, ...]:
    """Return the catalog identifier for a value or explicit override."""

    if identifier is not None:
        if isinstance(identifier, str):
            resolved = tuple(part for part in identifier.split(".") if part)
        else:
            resolved = tuple(str(part) for part in identifier)
        if not resolved:
            raise ValueError("identifier must contain at least a table name")
        return resolved
    if isinstance(value, (str, tuple)):
        return resolve_identifier(None, cast(Identifier, value))
    name = table_name(value)
    if not name:
        raise TypeError("cannot infer a table identifier; pass identifier=")
    namespace = schema_name(value) or "default"
    return (*(part for part in namespace.split(".") if part), name)


def _field_roles(
    value: Any,
    key: bytes,
    requested: Iterable[str] | None,
) -> tuple[tuple[str, Transform[Any, Any]], ...]:
    arrow_schema = value if isinstance(value, pa.Schema) else into_arrow_schema(value)
    if requested is not None:
        if isinstance(requested, (str, bytes)):
            raise TypeError("field roles must be an ordered iterable of names")
        selected = tuple(str(name) for name in requested)
        missing = [name for name in selected if name not in arrow_schema.names]
        if missing:
            raise ValueError("unknown field names: " + ", ".join(sorted(missing)))
        return tuple((name, IdentityTransform()) for name in selected)

    positioned: list[tuple[int, int, str, Transform[Any, Any]]] = []
    unpositioned: list[tuple[int, str, Transform[Any, Any]]] = []
    for index, field in enumerate(arrow_schema):
        raw = (field.metadata or {}).get(key)
        if not metadata_enabled(raw):
            continue
        position, transform = _role_spec(raw, field.name)
        if position is None:
            unpositioned.append((index, field.name, transform))
        else:
            positioned.append((position, index, field.name, transform))
    positioned.sort(key=lambda item: (item[0], item[1]))
    return tuple(
        [(name, transform) for _, _, name, transform in positioned]
        + [(name, transform) for _, name, transform in unpositioned]
    )


def _role_spec(
    raw: bytes | None,
    name: str,
) -> tuple[int | None, Transform[Any, Any]]:
    text = (raw or b"").decode("utf-8", "replace").strip()
    if not text or text.lower() in {"true", "yes", "1"}:
        return None, IdentityTransform()
    if _NUMERIC.fullmatch(text):
        return int(text), IdentityTransform()
    try:
        transform = parse_transform(text)
    except Exception as exc:
        raise ValueError(
            f"invalid Iceberg transform {text!r} for field {name!r}"
        ) from exc
    # PyIceberg parses unrecognized names into a placeholder rather than
    # failing, which would silently create an unusable partition spec.
    if isinstance(transform, UnknownTransform):
        raise ValueError(  # noqa: TRY004 - the value is malformed, not mistyped
            f"invalid Iceberg transform {text!r} for field {name!r}"
        )
    return None, transform


def _partition_name(name: str, transform: Transform[Any, Any]) -> str:
    if isinstance(transform, IdentityTransform):
        return name
    return f"{name}_{transform}".replace("[", "_").replace("]", "")


def _pyiceberg_version() -> str:
    import pyiceberg

    return str(getattr(pyiceberg, "__version__", "unknown"))


def _validate_catalog(catalog: Any) -> None:
    if not isinstance(catalog, Catalog):
        raise TypeError("catalog must be a PyIceberg Catalog")


def _validate_table(table: Any) -> None:
    if not isinstance(table, Table):
        raise TypeError("table must be a PyIceberg Table")


def _validate_format_version(value: int) -> None:
    if type(value) is not int or value not in (1, 2, 3):
        raise ValueError("format_version must be 1, 2, or 3")
