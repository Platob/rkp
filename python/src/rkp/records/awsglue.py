"""AWS Glue Data Catalog interoperability and Athena/Hive DDL generation.

Schema conversion and DDL rendering depend only on the required PyArrow
runtime. :class:`GlueCatalog` imports boto3 only when it must create a client,
so callers can inject a boto3-compatible client (including Moto's) directly.
"""

from __future__ import annotations

import base64
import copy
import dataclasses
import datetime as dt
import enum
import json
import math
import pathlib
import re
import typing
import uuid
from collections.abc import Iterable, Mapping, Sequence
from decimal import Decimal
from typing import Any

import pyarrow as pa

from ._metadata import (
    INDEX_KEY,
    ORC_FIELD_ID,
    PARQUET_FIELD_ID,
    PARTITION_KEY,
    PRIMARY_KEY,
    field_seq_from_metadata,
    metadata_enabled,
)
from .arrow import (
    into_arrow_schema,
)
from .arrow import (
    schema_name as arrow_schema_name,
)
from .arrow import (
    table_name as arrow_table_name,
)

__all__ = [
    "GlueCatalog",
    "arrow_into_glue_column",
    "arrow_into_glue_columns",
    "arrow_type_into_glue_type",
    "glue_into_arrow_field",
    "glue_into_arrow_schema",
    "into_glue_columns",
    "into_glue_database_ddl",
    "into_glue_ddl",
    "into_glue_drop_database_ddl",
    "into_glue_drop_table_ddl",
    "into_glue_partition_projection",
    "into_glue_partition_values",
    "into_glue_table_input",
]

_ARROW_SCHEMA_PARAMETER = "rkp.arrow_schema"
_COLUMN_ORDER_PARAMETER = "rkp.column_order"
_PARTITION_ORDER_METADATA = b"rkp.partition_order"
_MAX_COMMENT_LENGTH = 255
_MAX_DESCRIPTION_LENGTH = 2_048
_MAX_LOCATION_LENGTH = 1_024
_MAX_PARAMETER_KEY_LENGTH = 255
_MAX_PARAMETER_VALUE_LENGTH = 512_000
_MAX_TYPE_LENGTH = 131_072
_PARTITION_PLACEHOLDER = re.compile(r"\$\{([^{}]+)\}")
_PROJECTION_RELATIVE_DATE = re.compile(
    r"\s*NOW\s*(?:[+-]\s*\d+\s*"
    r"(?:YEARS?|MONTHS?|WEEKS?|DAYS?|HOURS?|MINUTES?|SECONDS?)\s*)?",
    re.IGNORECASE,
)
_PROJECTION_TYPES = frozenset({"date", "enum", "injected", "integer"})
_PROJECTION_INTERVAL_UNITS = frozenset(
    {
        "DAYS",
        "HOURS",
        "MILLIS",
        "MINUTES",
        "MONTHS",
        "SECONDS",
        "WEEKS",
        "YEARS",
    }
)
_RKP_NULLABLE = "rkp.nullable"
_RKP_SEQ = "rkp.seq"
_ROLE_PARAMETERS = {
    PRIMARY_KEY: "rkp.primary_key",
    PARTITION_KEY: "rkp.partition_key",
    INDEX_KEY: "rkp.index_key",
}
_PROTOCOL_METADATA = {
    b"doc",
    PARQUET_FIELD_ID,
    ORC_FIELD_ID,
    PRIMARY_KEY,
    PARTITION_KEY,
    INDEX_KEY,
}


class _Format(typing.NamedTuple):
    input_format: str
    output_format: str
    serde: str
    ddl_storage: str


_FORMATS = {
    "parquet": _Format(
        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
        "PARQUET",
    ),
    "orc": _Format(
        "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
        "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
        "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
        "ORC",
    ),
    "avro": _Format(
        "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat",
        "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat",
        "org.apache.hadoop.hive.serde2.avro.AvroSerDe",
        "AVRO",
    ),
    "json": _Format(
        "org.apache.hadoop.mapred.TextInputFormat",
        "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        "org.openx.data.jsonserde.JsonSerDe",
        "TEXTFILE",
    ),
    "csv": _Format(
        "org.apache.hadoop.mapred.TextInputFormat",
        "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        "org.apache.hadoop.hive.serde2.OpenCSVSerde",
        "TEXTFILE",
    ),
}


def arrow_type_into_glue_type(value: pa.DataType, *, path: str = "value") -> str:
    """Map a PyArrow type to Glue's Hive-compatible type syntax."""

    if not isinstance(value, pa.DataType):
        raise TypeError("arrow_type_into_glue_type expects a pyarrow.DataType")
    try:
        return _arrow_type_into_glue_type(value, path=path, ddl=False)
    except (TypeError, ValueError) as exc:
        if path in str(exc):
            raise
        raise TypeError(f"cannot convert Arrow type at {path!r}: {exc}") from exc


def arrow_into_glue_column(field: pa.Field, *, path: str = "") -> dict[str, Any]:
    """Convert one Arrow field to an AWS Glue ``Column`` mapping."""

    if not isinstance(field, pa.Field):
        raise TypeError("arrow_into_glue_column expects a pyarrow.Field")
    _validate_unique_fields(pa.schema([field]))
    return _arrow_into_glue_column_validated(field, path=path)


def _arrow_into_glue_column_validated(
    field: pa.Field,
    *,
    path: str = "",
) -> dict[str, Any]:
    field_path = _join_path(path, field.name)
    metadata = field.metadata or {}
    parameters: dict[str, str] = {}
    for key, value in metadata.items():
        if key in _PROTOCOL_METADATA:
            continue
        parameters[_decode_metadata(key, field_path)] = _decode_metadata(
            value, field_path
        )
    parameters[_RKP_NULLABLE] = "true" if field.nullable else "false"
    seq = field_seq_from_metadata(metadata, path=field_path)
    if seq is not None:
        parameters[_RKP_SEQ] = str(seq)
    for metadata_key, parameter_key in _ROLE_PARAMETERS.items():
        if metadata_key in metadata:
            parameters[parameter_key] = _decode_metadata(
                metadata[metadata_key], field_path
            )
    if pa.types.is_null(field.type):
        parameters["rkp.arrow_type"] = "null"
    if pa.types.is_timestamp(field.type):
        parameters["rkp.timestamp_unit"] = field.type.unit
        if field.type.tz is not None:
            parameters["rkp.timezone"] = field.type.tz

    glue_type = arrow_type_into_glue_type(field.type, path=field_path)
    if len(glue_type) > _MAX_TYPE_LENGTH:
        raise ValueError(
            f"Glue type at {field_path!r} exceeds {_MAX_TYPE_LENGTH} characters"
        )
    _validate_string_mapping("Glue column Parameters", parameters)
    result: dict[str, Any] = {
        "Name": field.name,
        "Type": glue_type,
        "Parameters": dict(sorted(parameters.items())),
    }
    doc = metadata.get(b"doc")
    if doc is not None:
        comment = _decode_metadata(doc, field_path)
        if len(comment) > _MAX_COMMENT_LENGTH:
            raise ValueError(
                f"Glue column Comment at {field_path!r} exceeds "
                f"{_MAX_COMMENT_LENGTH} characters"
            )
        result["Comment"] = comment
    return result


def arrow_into_glue_columns(schema: pa.Schema) -> list[dict[str, Any]]:
    """Convert an Arrow schema to ordered Glue ``Column`` mappings."""

    if not isinstance(schema, pa.Schema):
        raise TypeError("arrow_into_glue_columns expects a pyarrow.Schema")
    _validate_unique_fields(schema)
    return [_arrow_into_glue_column_validated(field) for field in schema]


def into_glue_columns(value: Any) -> list[dict[str, Any]]:
    """Convert any supported record/dataclass/schema into Glue columns."""

    return arrow_into_glue_columns(into_arrow_schema(value))


def into_glue_partition_values(
    value: Any,
    schema: Any = None,
    *,
    partition_keys: Iterable[str] | None = None,
) -> list[str]:
    """Project typed partition values in the schema's canonical key order.

    ``value`` may be a record/dataclass instance or a mapping keyed by its
    serialized Arrow field names.  Records carry their own schema.  Mappings
    require either ``schema`` (a supported schema value or Glue table) or an
    explicit ordered ``partition_keys`` iterable.

    Values cross the Arrow type boundary before rendering, so aliases, enum
    storage types, integer widths, dates, timestamps, and decimals follow the
    same physical contract used by the rest of RKP's protocol adapters.
    """

    arrow_schema = _partition_value_schema(value, schema)
    selected: Sequence[str]
    if arrow_schema is None:
        selected = _partition_names_without_schema(partition_keys)
        fields_by_name: dict[str, pa.Field] = {}
    else:
        _validate_unique_fields(arrow_schema)
        selected = _partition_names(arrow_schema, partition_keys)
        fields_by_name = {field.name: field for field in arrow_schema}
    if not selected:
        raise ValueError("partition values require at least one partition key")

    row = _partition_value_mapping(value)
    missing = [name for name in selected if name not in row]
    if missing:
        raise ValueError(
            "partition value mapping is missing: "
            + ", ".join(repr(item) for item in missing)
        )

    result: list[str] = []
    for name in selected:
        field = fields_by_name.get(name)
        if field is not None and not _is_partition_type(field.type):
            raise TypeError(f"partition key {name!r} must use a primitive Glue type")
        result.append(_render_partition_value(row[name], field=field, name=name))
    return _partition_values(result)


def into_glue_partition_projection(
    value: Any,
    projections: Mapping[str, Any] | None = None,
    *,
    partition_keys: Iterable[str] | None = None,
    location_template: str | None = None,
    enabled: bool = True,
) -> dict[str, str]:
    """Build validated Athena partition-projection table parameters.

    Each projection is keyed by a partition column and has one of the AWS
    projection types ``enum``, ``integer``, ``date``, or ``injected``.  A
    string is accepted as shorthand for ``{"type": string}``; otherwise a
    mapping exposes the type-specific properties documented by Athena.
    """

    if type(enabled) is not bool:
        raise TypeError("enabled must be bool")
    schema = into_arrow_schema(value)
    _validate_unique_fields(schema)
    selected = _partition_names(schema, partition_keys)
    by_name = {field.name: field for field in schema}
    for name in selected:
        if not _is_partition_type(by_name[name].type):
            raise TypeError(f"partition key {name!r} must use a primitive Glue type")

    if projections is None:
        normalized: dict[str, Any] = {}
    elif isinstance(projections, Mapping):
        normalized = dict(projections)
    else:
        raise TypeError("projections must be a mapping or None")
    if not all(isinstance(name, str) and name for name in normalized):
        raise TypeError("projection names must be non-empty strings")

    selected_set = set(selected)
    unexpected = set(normalized) - selected_set
    if unexpected:
        raise ValueError(
            "projections are not partition keys: "
            + ", ".join(repr(item) for item in sorted(unexpected))
        )
    missing = selected_set - set(normalized)
    if enabled and missing:
        raise ValueError(
            "enabled partition projection is missing: "
            + ", ".join(repr(item) for item in sorted(missing))
        )
    if enabled and not selected:
        raise ValueError("enabled partition projection requires partition keys")

    result = {"projection.enabled": "true" if enabled else "false"}
    for name in selected:
        if name in normalized:
            result.update(
                _normalize_partition_projection(
                    name,
                    normalized[name],
                    field=by_name[name],
                )
            )
    if location_template is not None:
        result["storage.location.template"] = _projection_location_template(
            location_template,
            selected,
        )
    _validate_string_mapping("partition projection", result)
    return dict(sorted(result.items()))


def into_glue_table_input(
    value: Any,
    *,
    name: str | None = None,
    location: str | None = None,
    format: str = "parquet",
    description: str | None = None,
    parameters: Mapping[str, Any] | None = None,
    serde_parameters: Mapping[str, Any] | None = None,
    partition_keys: Iterable[str] | None = None,
    partition_projection: Mapping[str, Any] | None = None,
    partition_location_template: str | None = None,
    partition_projection_enabled: bool = True,
) -> dict[str, Any]:
    """Build a classic external-table ``TableInput`` for AWS Glue.

    Root partition columns are removed from ``StorageDescriptor.Columns`` and
    emitted through ``PartitionKeys`` as required by Athena/Hive tables.
    """

    schema = into_arrow_schema(value)
    _validate_unique_fields(schema)
    _validate_nonempty_schema(schema)
    inferred_name = arrow_table_name(schema)
    if name is None and inferred_name is None:
        raise TypeError("name is required when Arrow metadata has no table_name")
    table_name = _identifier("table", inferred_name if name is None else name)
    # Glue lowercases identifiers.  Store the effective live name in the
    # embedded Arrow schema so it cannot drift from TableInput.Name.
    schema = into_arrow_schema(schema, metadata={"table_name": table_name})
    storage_format, format_name = _format(format)
    normalized_location = _optional_string(
        "location", location, max_length=_MAX_LOCATION_LENGTH
    )
    normalized_description = _optional_string(
        "description",
        description,
        allow_empty=True,
        max_length=_MAX_DESCRIPTION_LENGTH,
    )
    table_parameters = _string_mapping("parameters", parameters)
    table_parameters["EXTERNAL"] = "TRUE"
    table_parameters["classification"] = format_name
    selected = _partition_names(schema, partition_keys)
    schema = _schema_with_partitions(schema, selected)
    if type(partition_projection_enabled) is not bool:
        raise TypeError("partition_projection_enabled must be bool")
    if (
        partition_projection is not None
        or partition_location_template is not None
        or not partition_projection_enabled
    ):
        projected = into_glue_partition_projection(
            schema,
            partition_projection,
            partition_keys=selected,
            location_template=partition_location_template,
            enabled=partition_projection_enabled,
        )
        overlap = _partition_projection_parameter_keys(table_parameters)
        if overlap:
            raise ValueError(
                "partition projection conflicts with parameters: "
                + ", ".join(repr(item) for item in sorted(overlap))
            )
        table_parameters.update(projected)
    table_parameters[_COLUMN_ORDER_PARAMETER] = json.dumps(
        schema.names, separators=(",", ":")
    )
    table_parameters[_ARROW_SCHEMA_PARAMETER] = base64.b64encode(
        schema.serialize().to_pybytes()
    ).decode("ascii")
    _validate_string_mapping("parameters", table_parameters)

    selected_set = set(selected)
    columns: list[dict[str, Any]] = []
    partitions_by_name: dict[str, dict[str, Any]] = {}
    for field in schema:
        converted = _arrow_into_glue_column_validated(field)
        if field.name in selected_set:
            if not _is_partition_type(field.type):
                raise TypeError(
                    f"partition key {field.name!r} must use a primitive Glue type"
                )
            partitions_by_name[field.name] = converted
        else:
            columns.append(converted)

    serde = {
        "SerializationLibrary": storage_format.serde,
        "Parameters": _string_mapping("serde_parameters", serde_parameters),
    }
    descriptor: dict[str, Any] = {
        "Columns": columns,
        "InputFormat": storage_format.input_format,
        "OutputFormat": storage_format.output_format,
        "SerdeInfo": serde,
    }
    if normalized_location is not None:
        descriptor["Location"] = normalized_location

    result: dict[str, Any] = {
        "Name": table_name,
        "TableType": "EXTERNAL_TABLE",
        "StorageDescriptor": descriptor,
        "PartitionKeys": [partitions_by_name[item] for item in selected],
        "Parameters": dict(sorted(table_parameters.items())),
    }
    if normalized_description is not None:
        result["Description"] = normalized_description
    return result


def glue_into_arrow_field(column: Mapping[str, Any]) -> pa.Field:
    """Convert a Glue ``Column`` mapping to an Arrow field."""

    if not isinstance(column, Mapping):
        raise TypeError("Glue column must be a mapping")
    name = column.get("Name")
    glue_type = column.get("Type")
    name = _identifier("Glue column Name", name, lower=False)
    if not isinstance(glue_type, str) or not glue_type:
        raise TypeError(f"Glue column {name!r} Type must be a non-empty string")
    if len(glue_type) > _MAX_TYPE_LENGTH:
        raise ValueError(
            f"Glue column {name!r} Type exceeds {_MAX_TYPE_LENGTH} characters"
        )
    parameters = column.get("Parameters", {}) or {}
    if not isinstance(parameters, Mapping):
        raise TypeError(f"Glue column {name!r} Parameters must be a mapping")
    normalized = _string_mapping(f"column {name!r} Parameters", parameters)
    nullable = _parse_bool(normalized.pop(_RKP_NULLABLE, "true"), _RKP_NULLABLE)
    metadata: dict[bytes, bytes] = {}
    comment = column.get("Comment")
    if comment is not None:
        if not isinstance(comment, str):
            raise TypeError(f"Glue column {name!r} Comment must be a string")
        if len(comment) > _MAX_COMMENT_LENGTH:
            raise ValueError(
                f"Glue column {name!r} Comment exceeds {_MAX_COMMENT_LENGTH} characters"
            )
        metadata[b"doc"] = comment.encode("utf-8")
    seq = normalized.pop(_RKP_SEQ, None)
    if seq is not None:
        try:
            seq_value = int(seq)
        except ValueError as exc:
            raise ValueError(f"invalid rkp.seq for Glue column {name!r}") from exc
        # Reuse the shared bound/conflict validation.
        field_seq_from_metadata(
            {PARQUET_FIELD_ID: str(seq_value).encode("ascii")}, path=name
        )
        metadata[PARQUET_FIELD_ID] = str(seq_value).encode("ascii")
    for metadata_key, parameter_key in _ROLE_PARAMETERS.items():
        value = normalized.pop(parameter_key, None)
        if value is not None:
            if not value:
                raise ValueError(f"{parameter_key} must not be empty")
            metadata[metadata_key] = value.encode("utf-8")
    arrow_marker = normalized.pop("rkp.arrow_type", None)
    if arrow_marker not in {None, "null"}:
        raise ValueError(f"invalid rkp.arrow_type for Glue column {name!r}")
    timestamp_unit = normalized.pop("rkp.timestamp_unit", None)
    timezone = normalized.pop("rkp.timezone", None)
    for key, value in normalized.items():
        metadata[key.encode("utf-8")] = value.encode("utf-8")

    arrow_type = _GlueTypeParser(glue_type).parse()
    if arrow_marker == "null":
        if timestamp_unit is not None or timezone is not None:
            raise ValueError(
                f"timestamp markers cannot be used with null Glue column {name!r}"
            )
        arrow_type = pa.null()
        nullable = True
    elif pa.types.is_timestamp(arrow_type):
        timestamp_unit = "us" if timestamp_unit is None else timestamp_unit
        if timestamp_unit not in {"s", "ms", "us", "ns"}:
            raise ValueError(f"invalid timestamp unit for Glue column {name!r}")
        arrow_type = pa.timestamp(timestamp_unit, tz=timezone)
    elif timestamp_unit is not None or timezone is not None:
        raise ValueError(f"timestamp markers require a timestamp Glue column {name!r}")
    result = pa.field(name, arrow_type, nullable=nullable, metadata=metadata or None)
    _validate_unique_fields(pa.schema([result]))
    return result


def glue_into_arrow_schema(value: Mapping[str, Any]) -> pa.Schema:
    """Recover an Arrow schema from a Glue Table or StorageDescriptor."""

    if not isinstance(value, Mapping):
        raise TypeError("Glue table or storage descriptor must be a mapping")
    table_parameters = value.get("Parameters", {}) or {}
    if not isinstance(table_parameters, Mapping):
        raise TypeError("Glue table Parameters must be a mapping")
    _, raw_columns, raw_partitions = _glue_table_parts(value)
    encoded_schema = table_parameters.get(_ARROW_SCHEMA_PARAMETER)
    if encoded_schema is not None:
        if not isinstance(encoded_schema, str):
            raise TypeError(f"{_ARROW_SCHEMA_PARAMETER} must be a string")
        try:
            raw = base64.b64decode(encoded_schema, validate=True)
            schema = pa.ipc.read_schema(pa.BufferReader(raw))
        except (TypeError, ValueError, pa.ArrowException) as exc:
            raise ValueError("invalid embedded RKP Arrow schema") from exc
        schema = _validate_embedded_schema(
            schema,
            raw_columns=raw_columns,
            raw_partitions=raw_partitions,
            table_parameters=table_parameters,
        )
        return _schema_with_live_glue_identity(schema, value)

    fields = [glue_into_arrow_field(item) for item in raw_columns]
    partition_order: list[str] = []
    for item in raw_partitions:
        partition = glue_into_arrow_field(item)
        partition_order.append(partition.name)
        metadata = dict(partition.metadata or {})
        metadata[PARTITION_KEY] = b"true"
        fields.append(
            pa.field(
                partition.name,
                partition.type,
                nullable=partition.nullable,
                metadata=metadata,
            )
        )
    order = table_parameters.get(_COLUMN_ORDER_PARAMETER)
    if order is not None:
        if not isinstance(order, str):
            raise TypeError(f"{_COLUMN_ORDER_PARAMETER} must be a string")
        try:
            names = json.loads(order)
        except json.JSONDecodeError as exc:
            raise ValueError("invalid Glue rkp.column_order metadata") from exc
        if not isinstance(names, list) or not all(
            isinstance(item, str) for item in names
        ):
            raise ValueError("Glue rkp.column_order must contain a string list")
        by_name = {field.name: field for field in fields}
        if (
            len(by_name) != len(fields)
            or len(names) != len(set(names))
            or set(names) != set(by_name)
        ):
            raise ValueError("Glue rkp.column_order does not match table columns")
        fields = [by_name[name] for name in names]
    schema_metadata = None
    if partition_order:
        schema_metadata = {
            _PARTITION_ORDER_METADATA: json.dumps(
                partition_order, separators=(",", ":")
            ).encode("utf-8")
        }
    schema = pa.schema(fields, metadata=schema_metadata)
    _validate_unique_fields(schema)
    return _schema_with_live_glue_identity(schema, value)


def into_glue_ddl(
    value: Any,
    *,
    name: str | None = None,
    database: str | None = None,
    location: str | None = None,
    format: str = "parquet",
    if_not_exists: bool = True,
    description: str | None = None,
    properties: Mapping[str, Any] | None = None,
    serde_properties: Mapping[str, Any] | None = None,
    partition_keys: Iterable[str] | None = None,
    partition_projection: Mapping[str, Any] | None = None,
    partition_location_template: str | None = None,
    partition_projection_enabled: bool = True,
) -> str:
    """Generate deterministic Athena/Hive ``CREATE EXTERNAL TABLE`` DDL."""

    if type(if_not_exists) is not bool:
        raise TypeError("if_not_exists must be bool")
    schema = into_arrow_schema(value)
    _validate_unique_fields(schema)
    _validate_nonempty_schema(schema)
    inferred_name = arrow_table_name(schema)
    if name is None and inferred_name is None:
        raise TypeError("name is required when Arrow metadata has no table_name")
    table_name = _identifier("table", inferred_name if name is None else name)
    inferred_database = arrow_schema_name(schema)
    database_name = _optional_identifier(
        "database", inferred_database if database is None else database
    )
    normalized_location = _optional_string(
        "location", location, max_length=_MAX_LOCATION_LENGTH
    )
    normalized_description = _optional_string(
        "description",
        description,
        allow_empty=True,
        max_length=_MAX_DESCRIPTION_LENGTH,
    )
    storage_format, format_name = _format(format)
    selected = _partition_names(schema, partition_keys)
    selected_set = set(selected)
    by_name = {field.name: field for field in schema}
    for partition_name in selected:
        if not _is_partition_type(by_name[partition_name].type):
            raise TypeError(
                f"partition key {partition_name!r} must use a primitive Glue type"
            )
    body = [field for field in schema if field.name not in selected_set]
    prefix = "CREATE EXTERNAL TABLE"
    if if_not_exists:
        prefix += " IF NOT EXISTS"
    qualified = _quote_identifier(table_name)
    if database_name is not None:
        qualified = f"{_quote_identifier(database_name)}.{qualified}"
    lines = [f"{prefix} {qualified}"]
    if body:
        lines.extend(["(", _render_ddl_fields(body), ")"])
    if normalized_description is not None:
        lines.append(f"COMMENT {_quote_literal(normalized_description)}")
    if selected:
        partitions = [by_name[item] for item in selected]
        lines.extend(["PARTITIONED BY (", _render_ddl_fields(partitions), ")"])
    if serde_properties:
        lines.append(f"ROW FORMAT SERDE {_quote_literal(storage_format.serde)}")
        lines.extend(
            _render_properties_clause(
                "WITH SERDEPROPERTIES",
                _string_mapping("serde_properties", serde_properties),
            )
        )
    elif format_name in {"json", "csv"}:
        lines.append(f"ROW FORMAT SERDE {_quote_literal(storage_format.serde)}")
    lines.append(f"STORED AS {storage_format.ddl_storage}")
    if normalized_location is not None:
        lines.append(f"LOCATION {_quote_literal(normalized_location)}")
    table_properties = _string_mapping("properties", properties)
    if type(partition_projection_enabled) is not bool:
        raise TypeError("partition_projection_enabled must be bool")
    if (
        partition_projection is not None
        or partition_location_template is not None
        or not partition_projection_enabled
    ):
        projected = into_glue_partition_projection(
            schema,
            partition_projection,
            partition_keys=selected,
            location_template=partition_location_template,
            enabled=partition_projection_enabled,
        )
        overlap = _partition_projection_parameter_keys(table_properties)
        if overlap:
            raise ValueError(
                "partition projection conflicts with properties: "
                + ", ".join(repr(item) for item in sorted(overlap))
            )
        table_properties.update(projected)
    table_properties["classification"] = format_name
    table_properties["EXTERNAL"] = "TRUE"
    lines.extend(_render_properties_clause("TBLPROPERTIES", table_properties))
    return "\n".join(lines) + ";"


def into_glue_database_ddl(
    name: str,
    *,
    if_not_exists: bool = True,
    description: str | None = None,
    location: str | None = None,
    properties: Mapping[str, Any] | None = None,
) -> str:
    """Generate deterministic Athena ``CREATE DATABASE`` DDL."""

    database = _identifier("database", name)
    if type(if_not_exists) is not bool:
        raise TypeError("if_not_exists must be bool")
    lines = [
        "CREATE DATABASE"
        + (" IF NOT EXISTS" if if_not_exists else "")
        + f" {_quote_identifier(database)}"
    ]
    normalized_description = _optional_string(
        "description",
        description,
        allow_empty=True,
        max_length=_MAX_DESCRIPTION_LENGTH,
    )
    normalized_location = _optional_string(
        "location", location, max_length=_MAX_LOCATION_LENGTH
    )
    if normalized_description is not None:
        lines.append(f"COMMENT {_quote_literal(normalized_description)}")
    if normalized_location is not None:
        lines.append(f"LOCATION {_quote_literal(normalized_location)}")
    normalized_properties = _string_mapping("properties", properties)
    if normalized_properties:
        lines.extend(
            _render_properties_clause("WITH DBPROPERTIES", normalized_properties)
        )
    return "\n".join(lines) + ";"


def into_glue_drop_table_ddl(
    name: str,
    *,
    database: str | None = None,
    if_exists: bool = True,
) -> str:
    """Generate a safely quoted ``DROP TABLE`` statement."""

    table = _identifier("table", name)
    database_name = _optional_identifier("database", database)
    if type(if_exists) is not bool:
        raise TypeError("if_exists must be bool")
    qualified = _quote_identifier(table)
    if database_name is not None:
        qualified = f"{_quote_identifier(database_name)}.{qualified}"
    return f"DROP TABLE{' IF EXISTS' if if_exists else ''} {qualified};"


def into_glue_drop_database_ddl(
    name: str,
    *,
    if_exists: bool = True,
    cascade: bool = False,
) -> str:
    """Generate a safely quoted ``DROP DATABASE`` statement."""

    database = _identifier("database", name)
    if type(if_exists) is not bool or type(cascade) is not bool:
        raise TypeError("if_exists and cascade must be bool")
    suffix = " CASCADE" if cascade else ""
    return f"DROP DATABASE{' IF EXISTS' if if_exists else ''} {_quote_identifier(database)}{suffix};"


class GlueCatalog:
    """Small, injectable AWS Glue Data Catalog client facade."""

    def __init__(
        self,
        client: Any = ...,
        *,
        catalog_id: str | None = None,
        region_name: str | None = None,
    ) -> None:
        if catalog_id is not None and (
            not isinstance(catalog_id, str) or not 1 <= len(catalog_id) <= 255
        ):
            raise TypeError(
                "catalog_id must be a string of 1 to 255 characters or None"
            )
        if region_name is not None and (
            not isinstance(region_name, str) or not region_name
        ):
            raise TypeError("region_name must be a non-empty string or None")
        if client is ...:
            try:
                import boto3
            except ModuleNotFoundError as exc:
                raise ImportError(
                    "AWS Glue support requires boto3; install rkp[awsglue]"
                ) from exc
            client = boto3.client("glue", region_name=region_name)
        elif region_name is not None:
            raise TypeError("region_name cannot be used with an injected client")
        if client is None:
            raise TypeError("client must be a Glue-compatible client or omitted")
        self.client = client
        self.catalog_id = catalog_id

    def ensure_database(
        self,
        name: str,
        *,
        description: str | None = None,
        location_uri: str | None = None,
        parameters: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Create a database or update its requested definition."""

        database_name = _identifier("database", name)
        database_input = self._database_input(
            database_name,
            description=description,
            location_uri=location_uri,
            parameters=parameters,
        )
        try:
            self.client.create_database(**self._catalog(), DatabaseInput=database_input)
        except self.client.exceptions.AlreadyExistsException:
            self.client.update_database(
                **self._catalog(),
                Name=database_name,
                DatabaseInput=database_input,
            )
        return self.get_database(database_name)

    def create_database(
        self,
        name: str,
        *,
        description: str | None = None,
        location_uri: str | None = None,
        parameters: Mapping[str, Any] | None = None,
        exist_ok: bool = False,
    ) -> dict[str, Any]:
        """Create a database and return its stored definition."""

        if type(exist_ok) is not bool:
            raise TypeError("exist_ok must be bool")
        database_name = _identifier("database", name)
        try:
            self.client.create_database(
                **self._catalog(),
                DatabaseInput=self._database_input(
                    database_name,
                    description=description,
                    location_uri=location_uri,
                    parameters=parameters,
                ),
            )
        except self.client.exceptions.AlreadyExistsException:
            if not exist_ok:
                raise
        return self.get_database(database_name)

    def get_database(self, name: str) -> dict[str, Any]:
        """Return one database or propagate Glue's not-found error."""

        response = self.client.get_database(
            **self._catalog(), Name=_identifier("database", name)
        )
        return response["Database"]

    def update_database(
        self,
        name: str,
        *,
        description: str | None = None,
        location_uri: str | None = None,
        parameters: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Update a database definition and return its stored value."""

        database_name = _identifier("database", name)
        self.client.update_database(
            **self._catalog(),
            Name=database_name,
            DatabaseInput=self._database_input(
                database_name,
                description=description,
                location_uri=location_uri,
                parameters=parameters,
            ),
        )
        return self.get_database(database_name)

    def delete_database(self, name: str, *, missing_ok: bool = False) -> bool:
        """Delete a database, returning whether one was removed."""

        if type(missing_ok) is not bool:
            raise TypeError("missing_ok must be bool")
        try:
            self.client.delete_database(
                **self._catalog(), Name=_identifier("database", name)
            )
        except self.client.exceptions.EntityNotFoundException:
            if not missing_ok:
                raise
            return False
        return True

    def list_databases(self) -> list[dict[str, Any]]:
        """Return all databases, transparently consuming paginator pages."""

        return self._paginate("get_databases", "DatabaseList", **self._catalog())

    def create_table(
        self,
        database: str,
        table_input: Mapping[str, Any],
        *,
        exist_ok: bool = False,
    ) -> dict[str, Any]:
        """Create a table and return its stored definition."""

        if type(exist_ok) is not bool:
            raise TypeError("exist_ok must be bool")
        normalized = _table_input(table_input)
        try:
            self.client.create_table(
                **self._catalog(),
                DatabaseName=_identifier("database", database),
                TableInput=normalized,
            )
        except self.client.exceptions.AlreadyExistsException:
            if not exist_ok:
                raise
        return self.get_table(database, normalized["Name"])

    def update_table(
        self,
        database: str,
        table_input: Mapping[str, Any],
        *,
        skip_archive: bool = True,
        version_id: str | None = None,
    ) -> dict[str, Any]:
        """Update a table and return its stored definition."""

        if type(skip_archive) is not bool:
            raise TypeError("skip_archive must be bool")
        normalized = _table_input(table_input)
        request: dict[str, Any] = {
            **self._catalog(),
            "DatabaseName": _identifier("database", database),
            "TableInput": normalized,
            "SkipArchive": skip_archive,
        }
        if version_id is not None:
            request["VersionId"] = _identifier("version_id", version_id, lower=False)
        self.client.update_table(**request)
        return self.get_table(database, normalized["Name"])

    def upsert_table(
        self,
        database: str,
        table_input: Mapping[str, Any],
        *,
        skip_archive: bool = True,
    ) -> dict[str, Any]:
        """Create a table, or update it when it already exists."""

        if type(skip_archive) is not bool:
            raise TypeError("skip_archive must be bool")
        normalized = _table_input(table_input)
        try:
            return self.create_table(database, normalized)
        except self.client.exceptions.AlreadyExistsException:
            return self.update_table(
                database,
                normalized,
                skip_archive=skip_archive,
            )

    def get_table(self, database: str, name: str) -> dict[str, Any]:
        """Return one table or propagate Glue's not-found error."""

        response = self.client.get_table(
            **self._catalog(),
            DatabaseName=_identifier("database", database),
            Name=_identifier("table", name),
        )
        return response["Table"]

    def delete_table(
        self,
        database: str,
        name: str,
        *,
        missing_ok: bool = False,
    ) -> bool:
        """Delete a table, returning whether one was removed."""

        if type(missing_ok) is not bool:
            raise TypeError("missing_ok must be bool")
        try:
            self.client.delete_table(
                **self._catalog(),
                DatabaseName=_identifier("database", database),
                Name=_identifier("table", name),
            )
        except self.client.exceptions.EntityNotFoundException:
            if not missing_ok:
                raise
            return False
        return True

    def list_tables(self, database: str) -> list[dict[str, Any]]:
        """Return every table in a database across paginator pages."""

        return self._paginate(
            "get_tables",
            "TableList",
            **self._catalog(),
            DatabaseName=_identifier("database", database),
        )

    def partition_values(
        self,
        database: str,
        table: str,
        value: Any,
    ) -> list[str]:
        """Project a record or mapping using a live table's partition order."""

        definition = self.get_table(database, table)
        return into_glue_partition_values(value, definition)

    def create_partition_from(
        self,
        database: str,
        table: str,
        value: Any,
        *,
        location: str | None = None,
        storage_descriptor: Mapping[str, Any] | None = None,
        parameters: Mapping[str, Any] | None = None,
        exist_ok: bool = False,
    ) -> dict[str, Any]:
        """Create a partition by projecting values from a record or mapping.

        The live table supplies canonical partition order and physical Arrow
        types.  Passing ``location`` clones the table storage descriptor and
        replaces its location; an explicit descriptor can be supplied when a
        caller needs a different partition layout.
        """

        definition = self.get_table(database, table)
        partition_input: dict[str, Any] = {
            "Values": into_glue_partition_values(value, definition)
        }
        if storage_descriptor is not None:
            if not isinstance(storage_descriptor, Mapping):
                raise TypeError("storage_descriptor must be a mapping or None")
            partition_input["StorageDescriptor"] = copy.deepcopy(
                dict(storage_descriptor)
            )
        if location is not None:
            normalized_location = _optional_string(
                "location", location, max_length=_MAX_LOCATION_LENGTH
            )
            if "StorageDescriptor" not in partition_input:
                descriptor = definition.get("StorageDescriptor")
                if not isinstance(descriptor, Mapping):
                    raise TypeError("Glue table StorageDescriptor must be a mapping")
                partition_input["StorageDescriptor"] = copy.deepcopy(dict(descriptor))
            partition_input["StorageDescriptor"]["Location"] = normalized_location
        if parameters is not None:
            partition_input["Parameters"] = _string_mapping("parameters", parameters)
        return self.create_partition(
            database,
            table,
            partition_input,
            exist_ok=exist_ok,
        )

    def create_partition(
        self,
        database: str,
        table: str,
        partition_input: Mapping[str, Any],
        *,
        exist_ok: bool = False,
    ) -> dict[str, Any]:
        """Create a partition and return its stored definition."""

        if type(exist_ok) is not bool:
            raise TypeError("exist_ok must be bool")
        if not isinstance(partition_input, Mapping):
            raise TypeError("partition_input must be a mapping")
        normalized = copy.deepcopy(dict(partition_input))
        values = normalized.get("Values")
        values = _partition_values(values)
        normalized["Values"] = values
        try:
            self.client.create_partition(
                **self._catalog(),
                DatabaseName=_identifier("database", database),
                TableName=_identifier("table", table),
                PartitionInput=normalized,
            )
        except self.client.exceptions.AlreadyExistsException:
            if not exist_ok:
                raise
        return self.get_partition(database, table, values)

    def update_partition(
        self,
        database: str,
        table: str,
        values: Sequence[str],
        partition_input: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Update a partition and return its stored value."""

        if not isinstance(partition_input, Mapping):
            raise TypeError("partition_input must be a mapping")
        normalized = copy.deepcopy(dict(partition_input))
        new_values = normalized.get("Values")
        new_values = _partition_values(new_values)
        normalized["Values"] = new_values
        self.client.update_partition(
            **self._catalog(),
            DatabaseName=_identifier("database", database),
            TableName=_identifier("table", table),
            PartitionValueList=_partition_values(values),
            PartitionInput=normalized,
        )
        return self.get_partition(database, table, new_values)

    def upsert_partition(
        self,
        database: str,
        table: str,
        partition_input: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Create a partition, or update the existing partition by values."""

        if not isinstance(partition_input, Mapping):
            raise TypeError("partition_input must be a mapping")
        values = partition_input.get("Values")
        values = _partition_values(values)
        try:
            return self.create_partition(database, table, partition_input)
        except self.client.exceptions.AlreadyExistsException:
            return self.update_partition(database, table, values, partition_input)

    def batch_create_partitions(
        self,
        database: str,
        table: str,
        partitions: Iterable[Mapping[str, Any]],
    ) -> dict[str, Any]:
        """Create up to Glue's per-request partition limit in one call."""

        if isinstance(partitions, Mapping):
            raise TypeError("partitions must be an iterable of mappings")
        try:
            normalized = [copy.deepcopy(dict(item)) for item in partitions]
        except (TypeError, ValueError) as exc:
            raise TypeError("partitions must be an iterable of mappings") from exc
        for item in normalized:
            values = item.get("Values")
            item["Values"] = _partition_values(values)
        if not 1 <= len(normalized) <= 100:
            raise ValueError("Glue batch_create_partition accepts 1 to 100 items")
        return self.client.batch_create_partition(
            **self._catalog(),
            DatabaseName=_identifier("database", database),
            TableName=_identifier("table", table),
            PartitionInputList=normalized,
        )

    def batch_delete_partitions(
        self,
        database: str,
        table: str,
        values: Iterable[Sequence[str]],
    ) -> dict[str, Any]:
        """Delete up to Glue's per-request partition limit in one call."""

        if isinstance(values, (str, bytes, bytearray)):
            raise TypeError("values must be an iterable of partition value lists")
        try:
            normalized = [{"Values": _partition_values(item)} for item in values]
        except TypeError as exc:
            raise TypeError(
                "values must be an iterable of partition value lists"
            ) from exc
        if not 1 <= len(normalized) <= 25:
            raise ValueError("Glue batch_delete_partition accepts 1 to 25 items")
        return self.client.batch_delete_partition(
            **self._catalog(),
            DatabaseName=_identifier("database", database),
            TableName=_identifier("table", table),
            PartitionsToDelete=normalized,
        )

    def get_partition(
        self, database: str, table: str, values: Sequence[str]
    ) -> dict[str, Any]:
        """Return the partition identified by its ordered values."""

        normalized_values = _partition_values(values)
        response = self.client.get_partition(
            **self._catalog(),
            DatabaseName=_identifier("database", database),
            TableName=_identifier("table", table),
            PartitionValues=normalized_values,
        )
        return response["Partition"]

    def delete_partition(
        self,
        database: str,
        table: str,
        values: Sequence[str],
        *,
        missing_ok: bool = False,
    ) -> bool:
        """Delete a partition, returning whether one was removed."""

        if type(missing_ok) is not bool:
            raise TypeError("missing_ok must be bool")
        try:
            self.client.delete_partition(
                **self._catalog(),
                DatabaseName=_identifier("database", database),
                TableName=_identifier("table", table),
                PartitionValues=_partition_values(values),
            )
        except self.client.exceptions.EntityNotFoundException:
            if not missing_ok:
                raise
            return False
        return True

    def list_partitions(self, database: str, table: str) -> list[dict[str, Any]]:
        """Return every table partition across paginator pages."""

        return self._paginate(
            "get_partitions",
            "Partitions",
            **self._catalog(),
            DatabaseName=_identifier("database", database),
            TableName=_identifier("table", table),
        )

    def _catalog(self) -> dict[str, str]:
        return {} if self.catalog_id is None else {"CatalogId": self.catalog_id}

    def _paginate(self, operation: str, result_key: str, **kwargs: Any) -> list[Any]:
        paginator = self.client.get_paginator(operation)
        result: list[Any] = []
        for page in paginator.paginate(**kwargs):
            result.extend(page.get(result_key, []))
        return result

    @staticmethod
    def _database_input(
        name: str,
        *,
        description: str | None,
        location_uri: str | None,
        parameters: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {"Name": name}
        normalized_description = _optional_string(
            "description",
            description,
            allow_empty=True,
            max_length=_MAX_DESCRIPTION_LENGTH,
        )
        normalized_location = _optional_string(
            "location_uri", location_uri, max_length=_MAX_LOCATION_LENGTH
        )
        if normalized_description is not None:
            result["Description"] = normalized_description
        if normalized_location is not None:
            result["LocationUri"] = normalized_location
        normalized_parameters = _string_mapping("parameters", parameters)
        if normalized_parameters:
            result["Parameters"] = normalized_parameters
        return result


def _arrow_type_into_glue_type(value: pa.DataType, *, path: str, ddl: bool) -> str:
    keyword = str.upper if ddl else str.lower
    if pa.types.is_boolean(value):
        return keyword("boolean")
    if pa.types.is_int8(value):
        return keyword("tinyint")
    if pa.types.is_int16(value):
        return keyword("smallint")
    if pa.types.is_int32(value):
        return keyword("int")
    if pa.types.is_int64(value):
        return keyword("bigint")
    if pa.types.is_uint8(value):
        return keyword("smallint")
    if pa.types.is_uint16(value):
        return keyword("int")
    if pa.types.is_uint32(value):
        return keyword("bigint")
    if pa.types.is_uint64(value):
        return keyword("decimal") + "(20,0)"
    if pa.types.is_float16(value) or pa.types.is_float32(value):
        return keyword("float")
    if pa.types.is_float64(value):
        return keyword("double")
    if pa.types.is_decimal(value):
        if value.precision > 38 or not 0 <= value.scale <= value.precision:
            raise TypeError(
                f"{path}: Glue decimal requires precision up to 38 and "
                "scale between 0 and precision"
            )
        return keyword("decimal") + f"({value.precision},{value.scale})"
    if pa.types.is_string(value) or pa.types.is_large_string(value):
        return keyword("string")
    if (
        pa.types.is_binary(value)
        or pa.types.is_large_binary(value)
        or pa.types.is_fixed_size_binary(value)
    ):
        return keyword("binary")
    if pa.types.is_date(value):
        return keyword("date")
    if pa.types.is_timestamp(value):
        return keyword("timestamp")
    if pa.types.is_null(value):
        return keyword("string")
    if pa.types.is_dictionary(value):
        return _arrow_type_into_glue_type(value.value_type, path=path, ddl=ddl)
    if (
        pa.types.is_list(value)
        or pa.types.is_large_list(value)
        or pa.types.is_fixed_size_list(value)
    ):
        item = _arrow_type_into_glue_type(
            value.value_type, path=f"{path}.element", ddl=ddl
        )
        return f"{keyword('array')}<{item}>"
    if pa.types.is_map(value):
        if not _is_partition_type(value.key_type):
            raise TypeError(f"{path}.key: Glue map keys must be primitive")
        key = _arrow_type_into_glue_type(value.key_type, path=f"{path}.key", ddl=ddl)
        item = _arrow_type_into_glue_type(
            value.item_type, path=f"{path}.value", ddl=ddl
        )
        return f"{keyword('map')}<{key},{item}>"
    if pa.types.is_struct(value):
        rendered_children: list[str] = []
        for field in value:
            child_path = _join_path(path, field.name)
            rendered = (
                f"{_quote_identifier(field.name)}:"
                f"{_arrow_type_into_glue_type(field.type, path=child_path, ddl=ddl)}"
            )
            doc = (field.metadata or {}).get(b"doc")
            if doc is not None:
                comment = _decode_metadata(doc, child_path)
                if len(comment) > _MAX_COMMENT_LENGTH:
                    raise ValueError(
                        f"Glue column Comment at {child_path!r} exceeds "
                        f"{_MAX_COMMENT_LENGTH} characters"
                    )
                rendered += f" {keyword('comment')} {_quote_literal(comment)}"
            rendered_children.append(rendered)
        children = ",".join(rendered_children)
        return f"{keyword('struct')}<{children}>"
    raise TypeError(f"{path}: unsupported Arrow type {value}")


def _render_ddl_fields(fields: Sequence[pa.Field]) -> str:
    rendered: list[str] = []
    for field in fields:
        line = (
            f"  {_quote_identifier(field.name)} "
            f"{_arrow_type_into_glue_type(field.type, path=field.name, ddl=True)}"
        )
        doc = (field.metadata or {}).get(b"doc")
        if doc is not None:
            comment = _decode_metadata(doc, field.name)
            if len(comment) > _MAX_COMMENT_LENGTH:
                raise ValueError(
                    f"Glue column Comment at {field.name!r} exceeds "
                    f"{_MAX_COMMENT_LENGTH} characters"
                )
            line += f" COMMENT {_quote_literal(comment)}"
        rendered.append(line)
    return ",\n".join(rendered)


def _partition_names(
    schema: pa.Schema, requested: Iterable[str] | None
) -> tuple[str, ...]:
    if requested is None:
        encoded_order = (schema.metadata or {}).get(_PARTITION_ORDER_METADATA)
        if encoded_order is not None:
            try:
                restored = json.loads(encoded_order.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValueError("invalid Arrow rkp.partition_order metadata") from exc
            if (
                not isinstance(restored, list)
                or not all(isinstance(item, str) and item for item in restored)
                or len(restored) != len(set(restored))
                or not set(restored) <= set(schema.names)
            ):
                raise ValueError("invalid Arrow rkp.partition_order metadata")
            return tuple(restored)
        enabled = [
            field
            for field in schema
            if metadata_enabled((field.metadata or {}).get(PARTITION_KEY))
        ]
        positioned: list[tuple[int, int, pa.Field]] = []
        unpositioned: list[tuple[int, pa.Field]] = []
        seen_positions: dict[int, str] = {}
        for index, field in enumerate(enabled):
            position = _partition_position((field.metadata or {}).get(PARTITION_KEY))
            if position is None:
                unpositioned.append((index, field))
                continue
            previous = seen_positions.get(position)
            if previous is not None:
                raise ValueError(
                    "duplicate partition key position "
                    f"{position!r} for {previous!r} and {field.name!r}"
                )
            seen_positions[position] = field.name
            positioned.append((position, index, field))
        positioned.sort(key=lambda item: (item[0], item[1]))
        return tuple(
            [field.name for _, _, field in positioned]
            + [field.name for _, field in unpositioned]
        )
    if isinstance(requested, (str, bytes, bytearray, set, frozenset)):
        raise TypeError("partition_keys must be an ordered iterable of field names")
    try:
        result = tuple(requested)
    except TypeError as exc:
        raise TypeError("partition_keys must be an iterable of field names") from exc
    if not all(isinstance(item, str) and item for item in result):
        raise TypeError("partition key names must be non-empty strings")
    if len(set(result)) != len(result):
        raise ValueError("duplicate partition key names are not allowed")
    missing = set(result).difference(schema.names)
    if missing:
        raise ValueError(
            "partition keys are missing from the schema: " + ", ".join(sorted(missing))
        )
    return result


def _partition_position(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, str):
        encoded = value.encode("utf-8")
    else:
        try:
            encoded = bytes(value)
        except (TypeError, ValueError):
            return None
    text = encoded.strip()
    if re.fullmatch(rb"[+-]?\d+", text) is None:
        return None
    return int(text)


def _schema_with_partitions(schema: pa.Schema, selected: Sequence[str]) -> pa.Schema:
    selected_set = set(selected)
    fields: list[pa.Field] = []
    for field in schema:
        metadata = dict(field.metadata or {})
        if field.name in selected_set:
            metadata[PARTITION_KEY] = b"true"
        else:
            metadata.pop(PARTITION_KEY, None)
        fields.append(
            pa.field(
                field.name,
                field.type,
                nullable=field.nullable,
                metadata=metadata or None,
            )
        )
    schema_metadata = dict(schema.metadata or {})
    if selected:
        schema_metadata[_PARTITION_ORDER_METADATA] = json.dumps(
            list(selected), separators=(",", ":")
        ).encode("utf-8")
    else:
        schema_metadata.pop(_PARTITION_ORDER_METADATA, None)
    return pa.schema(fields, metadata=schema_metadata or None)


def _glue_table_parts(
    value: Mapping[str, Any],
) -> tuple[Mapping[str, Any], Sequence[Any], Sequence[Any]]:
    descriptor = value.get("StorageDescriptor", value)
    if not isinstance(descriptor, Mapping):
        raise TypeError("Glue StorageDescriptor must be a mapping")
    raw_columns = descriptor.get("Columns", []) or []
    raw_partitions = value.get("PartitionKeys", []) or []
    if not isinstance(raw_columns, Sequence) or isinstance(
        raw_columns, (str, bytes, bytearray)
    ):
        raise TypeError("Glue Columns must be a sequence")
    if not isinstance(raw_partitions, Sequence) or isinstance(
        raw_partitions, (str, bytes, bytearray)
    ):
        raise TypeError("Glue PartitionKeys must be a sequence")
    return descriptor, raw_columns, raw_partitions


def _validate_embedded_schema(
    schema: pa.Schema,
    *,
    raw_columns: Sequence[Any],
    raw_partitions: Sequence[Any],
    table_parameters: Mapping[str, Any],
) -> pa.Schema:
    """Validate a lossless RKP schema against Glue's live projection."""

    _validate_unique_fields(schema)
    _validate_nonempty_schema(schema)
    live_columns = [glue_into_arrow_field(item) for item in raw_columns]
    live_partitions = [glue_into_arrow_field(item) for item in raw_partitions]
    column_names = [field.name for field in live_columns]
    partition_names = [field.name for field in live_partitions]
    live_names = column_names + partition_names
    if len(live_names) != len(set(live_names)):
        raise ValueError("Glue columns and partition keys contain duplicate names")
    if set(live_names) != set(schema.names):
        raise ValueError("embedded RKP Arrow schema does not match Glue columns")
    expected_columns = [
        name for name in schema.names if name not in set(partition_names)
    ]
    if column_names != expected_columns:
        raise ValueError("Glue column order does not match embedded RKP Arrow schema")

    encoded_order = table_parameters.get(_COLUMN_ORDER_PARAMETER)
    if encoded_order is not None:
        if not isinstance(encoded_order, str):
            raise TypeError(f"{_COLUMN_ORDER_PARAMETER} must be a string")
        try:
            restored_order = json.loads(encoded_order)
        except json.JSONDecodeError as exc:
            raise ValueError("invalid Glue rkp.column_order metadata") from exc
        if restored_order != schema.names:
            raise ValueError("Glue rkp.column_order does not match embedded schema")

    partition_set = set(partition_names)
    for live in live_columns + live_partitions:
        source = schema.field(live.name)
        source_is_partition = metadata_enabled(
            (source.metadata or {}).get(PARTITION_KEY)
        )
        if source_is_partition != (live.name in partition_set):
            raise ValueError(
                f"Glue partition role for {live.name!r} does not match embedded schema"
            )
        projected = glue_into_arrow_field(_arrow_into_glue_column_validated(source))
        if not live.equals(projected, check_metadata=True):
            raise ValueError(
                f"Glue column {live.name!r} does not match embedded RKP Arrow schema"
            )

    metadata = dict(schema.metadata or {})
    if partition_names:
        metadata[_PARTITION_ORDER_METADATA] = json.dumps(
            partition_names, separators=(",", ":")
        ).encode("utf-8")
    else:
        metadata.pop(_PARTITION_ORDER_METADATA, None)
    return schema.with_metadata(metadata or None)


def _validate_nonempty_schema(schema: pa.Schema) -> None:
    if len(schema) == 0:
        raise ValueError("Glue tables and DDL require at least one field")


def _validate_unique_fields(schema: pa.Schema) -> None:
    def visit_fields(fields: Sequence[pa.Field], path: str) -> None:
        seen: set[str] = set()
        for field in fields:
            field_path = _join_path(path, field.name)
            if not 1 <= len(field.name) <= 255:
                raise ValueError(
                    f"Arrow field name at {field_path!r} must have 1 to 255 characters"
                )
            if field.name in seen:
                raise ValueError(f"duplicate Arrow field name at {field_path!r}")
            seen.add(field.name)
            visit_type(field.type, field_path)

    def visit_type(value: pa.DataType, path: str) -> None:
        if pa.types.is_struct(value):
            visit_fields(list(value), path)
        elif (
            pa.types.is_list(value)
            or pa.types.is_large_list(value)
            or pa.types.is_fixed_size_list(value)
        ):
            visit_type(value.value_type, f"{path}.element")
        elif pa.types.is_map(value):
            visit_type(value.key_type, f"{path}.key")
            visit_type(value.item_type, f"{path}.value")
        elif pa.types.is_dictionary(value):
            visit_type(value.value_type, path)

    visit_fields(list(schema), "")


def _is_partition_type(value: pa.DataType) -> bool:
    if pa.types.is_dictionary(value):
        return _is_partition_type(value.value_type)
    return not any(
        predicate(value)
        for predicate in (
            pa.types.is_struct,
            pa.types.is_list,
            pa.types.is_large_list,
            pa.types.is_fixed_size_list,
            pa.types.is_map,
            pa.types.is_union,
            pa.types.is_null,
        )
    )


def _format(value: str) -> tuple[_Format, str]:
    if not isinstance(value, str) or not value:
        raise TypeError("format must be a non-empty string")
    normalized = value.lower()
    try:
        return _FORMATS[normalized], normalized
    except KeyError as exc:
        raise ValueError(
            f"unsupported Glue storage format {value!r}; expected one of "
            + ", ".join(sorted(_FORMATS))
        ) from exc


def _schema_with_live_glue_identity(
    schema: pa.Schema,
    table: Mapping[str, Any],
) -> pa.Schema:
    """Overlay live Glue identity on the canonical Arrow metadata boundary."""

    updates: dict[str | bytes, Any] = {}
    for source, target, kind in (
        ("CatalogId", "catalog_name", "catalog"),
        ("DatabaseName", "schema_name", "database"),
        ("Name", "table_name", "table"),
    ):
        value = table.get(source)
        if value is not None:
            updates[target] = _identifier(kind, value, lower=False)
    if not updates:
        return schema
    return into_arrow_schema(schema, metadata=updates)


def _identifier(kind: str, value: Any, *, lower: bool = True) -> str:
    if not isinstance(value, str) or not 1 <= len(value) <= 255:
        raise TypeError(f"{kind} must be a string of 1 to 255 characters")
    return value.lower() if lower else value


def _optional_identifier(kind: str, value: Any) -> str | None:
    return None if value is None else _identifier(kind, value)


def _optional_string(
    name: str,
    value: Any,
    *,
    allow_empty: bool = False,
    max_length: int | None = None,
) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or (not value and not allow_empty):
        raise TypeError(f"{name} must be a non-empty string or None")
    if max_length is not None and len(value) > max_length:
        raise ValueError(f"{name} exceeds {max_length} characters")
    return value


def _string_mapping(name: str, value: Mapping[Any, Any] | None) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a mapping or None")
    result: dict[str, str] = {}
    for key, item in value.items():
        if not isinstance(key, str) or not key:
            raise TypeError(f"{name} keys must be non-empty strings")
        if isinstance(item, bool):
            result[key] = "true" if item else "false"
        elif item is None:
            result[key] = "null"
        elif isinstance(item, (str, int, float)):
            result[key] = str(item)
        else:
            result[key] = json.dumps(item, sort_keys=True, separators=(",", ":"))
    _validate_string_mapping(name, result)
    return dict(sorted(result.items()))


def _validate_string_mapping(name: str, value: Mapping[str, str]) -> None:
    for key, item in value.items():
        if not 1 <= len(key) <= _MAX_PARAMETER_KEY_LENGTH:
            raise ValueError(
                f"{name} keys must have 1 to {_MAX_PARAMETER_KEY_LENGTH} characters"
            )
        if len(item) > _MAX_PARAMETER_VALUE_LENGTH:
            raise ValueError(
                f"{name} values must not exceed "
                f"{_MAX_PARAMETER_VALUE_LENGTH} characters"
            )


def _decode_metadata(value: bytes, path: str) -> str:
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"Glue metadata at {path!r} must be UTF-8") from exc


def _parse_bool(value: str, name: str) -> bool:
    normalized = value.strip().lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    raise ValueError(f"{name} must be 'true' or 'false'")


def _quote_identifier(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def _quote_literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def _render_properties_clause(name: str, values: Mapping[str, str]) -> list[str]:
    if not values:
        return []
    rendered = [
        f"  {_quote_literal(key)}={_quote_literal(value)}"
        for key, value in sorted(values.items())
    ]
    return [f"{name} (", ",\n".join(rendered), ")"]


def _join_path(parent: str, child: str) -> str:
    return child if not parent else f"{parent}.{child}"


def _table_input(value: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("table_input must be a mapping")
    result = copy.deepcopy(dict(value))
    result["Name"] = _identifier("table", result.get("Name"))
    return result


def _partition_value_schema(value: Any, schema: Any) -> pa.Schema | None:
    if schema is not None:
        if isinstance(schema, Mapping):
            return glue_into_arrow_schema(schema)
        return into_arrow_schema(schema)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return into_arrow_schema(value)
    if isinstance(value, Mapping):
        return None
    raise TypeError("partition values require a record/dataclass instance or a mapping")


def _partition_names_without_schema(
    partition_keys: Iterable[str] | None,
) -> list[str]:
    if partition_keys is None:
        raise TypeError("mapping partition values require schema or partition_keys")
    if isinstance(
        partition_keys,
        (str, bytes, bytearray, set, frozenset),
    ):
        raise TypeError("partition_keys must be an ordered iterable of field names")
    try:
        result = list(partition_keys)
    except TypeError as exc:
        raise TypeError("partition_keys must be an iterable of field names") from exc
    if not all(isinstance(item, str) and item for item in result):
        raise TypeError("partition key names must be non-empty strings")
    if len(result) != len(set(result)):
        raise ValueError("duplicate partition key names are not allowed")
    return result


def _partition_value_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            raise TypeError("partition value mapping keys must be strings")
        return typing.cast(Mapping[str, Any], value)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        from .interop import resolved_type_hints, serialized_field_name

        try:
            hints = resolved_type_hints(type(value))
        except TypeError:
            hints = {}
        return {
            serialized_field_name(field, hints.get(field.name, field.type)): getattr(
                value, field.name
            )
            for field in dataclasses.fields(value)
        }
    raise TypeError("partition values require a record/dataclass instance or a mapping")


def _render_partition_value(
    value: Any,
    *,
    field: pa.Field | None,
    name: str,
) -> str:
    normalized = _partition_python_value(value)
    if field is not None:
        arrow_type_into_glue_type(field.type, path=name)
        scalar_type = _partition_scalar_type(field.type)
        try:
            normalized = pa.scalar(normalized, type=scalar_type).as_py()
        except (TypeError, ValueError, OverflowError) as exc:
            raise TypeError(
                f"partition value for {name!r} is incompatible with {field.type}"
            ) from exc
    if normalized is None:
        raise ValueError(f"partition value for {name!r} cannot be null")
    if isinstance(normalized, bool):
        return "true" if normalized else "false"
    if isinstance(normalized, bytes):
        return base64.b64encode(normalized).decode("ascii")
    if isinstance(normalized, Decimal):
        return format(normalized, "f")
    if isinstance(normalized, float):
        if not math.isfinite(normalized):
            raise ValueError(f"partition value for {name!r} must be finite")
        return str(normalized)
    if isinstance(normalized, dt.datetime):
        return _render_athena_timestamp(normalized, name=name)
    if isinstance(normalized, (dt.date, dt.time)):
        return normalized.isoformat()
    if isinstance(normalized, (str, int)):
        return str(normalized)
    raise TypeError(f"partition value for {name!r} cannot be rendered as a Glue string")


def _render_athena_timestamp(value: dt.datetime, *, name: str) -> str:
    if value.tzinfo is not None:
        value = value.astimezone(dt.UTC).replace(tzinfo=None)
    nanosecond = getattr(value, "nanosecond", 0)
    if value.microsecond % 1_000 or nanosecond:
        raise ValueError(
            f"partition value for {name!r} exceeds Athena's millisecond "
            "timestamp precision"
        )
    return value.isoformat(sep=" ", timespec="milliseconds")


def _partition_python_value(value: Any) -> Any:
    while isinstance(value, enum.Enum):
        value = value.value
    if isinstance(value, (pathlib.PurePath, uuid.UUID)):
        return str(value)
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    return value


def _partition_scalar_type(value: pa.DataType) -> pa.DataType:
    while True:
        if pa.types.is_dictionary(value):
            value = value.value_type
        elif isinstance(value, pa.ExtensionType):
            value = value.storage_type
        else:
            return value


def _normalize_partition_projection(
    name: str,
    value: Any,
    *,
    field: pa.Field,
) -> dict[str, str]:
    if isinstance(value, str):
        config: dict[str, Any] = {"type": value}
    elif isinstance(value, Mapping):
        config = dict(value)
    else:
        raise TypeError(f"projection {name!r} must be a type string or mapping")
    if not all(isinstance(key, str) and key for key in config):
        raise TypeError(f"projection {name!r} keys must be non-empty strings")
    if "interval_unit" in config:
        if "interval.unit" in config:
            raise ValueError(
                f"projection {name!r} cannot define both interval_unit and interval.unit"
            )
        config["interval.unit"] = config.pop("interval_unit")

    raw_type = config.pop("type", None)
    if not isinstance(raw_type, str) or not raw_type.strip():
        raise TypeError(f"projection {name!r} type must be a non-empty string")
    projection_type = raw_type.strip().lower()
    if projection_type not in _PROJECTION_TYPES:
        raise ValueError(
            f"unsupported projection type {raw_type!r} for {name!r}; expected "
            + ", ".join(sorted(_PROJECTION_TYPES))
        )
    _validate_projection_field_type(name, projection_type, field)

    prefix = f"projection.{name}."
    result = {prefix + "type": projection_type}
    if projection_type == "enum":
        _projection_options(name, config, required={"values"})
        result[prefix + "values"] = _projection_enum_values(name, config["values"])
    elif projection_type == "integer":
        _projection_options(
            name,
            config,
            required={"range"},
            optional={"digits", "interval"},
        )
        result[prefix + "range"] = _projection_integer_range(name, config["range"])
        for option in ("interval", "digits"):
            if option in config:
                result[prefix + option] = str(
                    _positive_integer(f"projection {name!r} {option}", config[option])
                )
    elif projection_type == "date":
        _projection_options(
            name,
            config,
            required={"format", "range"},
            optional={"interval", "interval.unit"},
        )
        date_format, python_format, has_default_interval = _projection_date_format(
            name, config["format"]
        )
        result[prefix + "format"] = date_format
        result[prefix + "range"] = _projection_date_range(
            name,
            config["range"],
            python_format=python_format,
        )
        if not has_default_interval:
            missing_interval = {
                option
                for option in ("interval", "interval.unit")
                if option not in config
            }
            if missing_interval:
                raise ValueError(
                    f"projection {name!r} format requires: "
                    + ", ".join(repr(item) for item in sorted(missing_interval))
                )
        if "interval" in config:
            result[prefix + "interval"] = str(
                _positive_integer(f"projection {name!r} interval", config["interval"])
            )
        if "interval.unit" in config:
            unit = config["interval.unit"]
            if (
                not isinstance(unit, str)
                or unit.strip().upper() not in _PROJECTION_INTERVAL_UNITS
            ):
                raise ValueError(
                    f"projection {name!r} interval.unit must be one of "
                    + ", ".join(sorted(_PROJECTION_INTERVAL_UNITS))
                )
            result[prefix + "interval.unit"] = unit.strip().upper()
    else:
        _projection_options(name, config)
    return result


def _projection_options(
    name: str,
    config: Mapping[str, Any],
    *,
    required: set[str] | None = None,
    optional: set[str] | None = None,
) -> None:
    required = required or set()
    allowed = required | (optional or set())
    missing = required - set(config)
    if missing:
        raise ValueError(
            f"projection {name!r} is missing: "
            + ", ".join(repr(item) for item in sorted(missing))
        )
    unexpected = set(config) - allowed
    if unexpected:
        raise ValueError(
            f"projection {name!r} has unsupported options: "
            + ", ".join(repr(item) for item in sorted(unexpected))
        )


def _validate_projection_field_type(
    name: str,
    projection_type: str,
    field: pa.Field,
) -> None:
    glue_type = arrow_type_into_glue_type(field.type, path=name)
    compatible = {
        "date": {"date", "string", "timestamp"},
        "enum": {"string"},
        "injected": {"string"},
        "integer": {"bigint", "int", "smallint", "string", "tinyint"},
    }[projection_type]
    if glue_type not in compatible:
        raise TypeError(
            f"{projection_type} projection {name!r} is incompatible with Glue type "
            f"{glue_type!r}"
        )


def _projection_enum_values(name: str, value: Any) -> str:
    if isinstance(value, str):
        items = value.split(",")
    elif isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        items = list(value)
    else:
        raise TypeError(f"projection {name!r} values must be a sequence of strings")
    if not items:
        raise ValueError(f"projection {name!r} values cannot be empty")
    if not all(isinstance(item, str) and item and "," not in item for item in items):
        raise TypeError(
            f"projection {name!r} values must be non-empty comma-free strings"
        )
    if len(items) != len(set(items)):
        raise ValueError(f"projection {name!r} values must be unique")
    return ",".join(items)


def _projection_pair(name: str, value: Any) -> tuple[Any, Any]:
    if isinstance(value, str):
        items: list[Any] = value.split(",")
    elif isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        items = list(value)
    else:
        raise TypeError(f"projection {name!r} range must contain two values")
    if len(items) != 2:
        raise ValueError(f"projection {name!r} range must contain exactly two values")
    return items[0], items[1]


def _projection_integer_range(name: str, value: Any) -> str:
    lower, upper = _projection_pair(name, value)
    rendered: list[str] = []
    parsed: list[int] = []
    for item in (lower, upper):
        if isinstance(item, bool) or not isinstance(item, (str, int)):
            raise TypeError(f"projection {name!r} integer range must contain integers")
        text = str(item).strip()
        if re.fullmatch(r"[+-]?\d+", text) is None:
            raise ValueError(f"projection {name!r} integer range is invalid")
        number = int(text)
        if not -(2**63) <= number <= 2**63 - 1:
            raise ValueError(f"projection {name!r} integer range exceeds int64")
        rendered.append(text)
        parsed.append(number)
    if parsed[0] > parsed[1]:
        raise ValueError(f"projection {name!r} range minimum exceeds maximum")
    return ",".join(rendered)


def _projection_date_format(name: str, value: Any) -> tuple[str, str | None, bool]:
    if not isinstance(value, str) or not value.strip():
        raise TypeError(f"projection {name!r} format must be a non-empty string")
    if "," in value or any(ord(character) < 32 for character in value):
        raise ValueError(f"projection {name!r} format contains invalid characters")

    allowed = frozenset("GBguyDMLdQqYwWEecFahKkHmsSAnNVvzOXxp")
    time_fields = frozenset("BahKkHmsSAnNVvzOXx")
    python_tokens = {
        "yyyy": "%Y",
        "uuuu": "%Y",
        "yy": "%y",
        "uu": "%y",
        "MM": "%m",
        "dd": "%d",
        "HH": "%H",
        "hh": "%I",
        "mm": "%M",
        "ss": "%S",
        "a": "%p",
    }
    python_parts: list[str] = []
    python_supported = True
    fields: set[str] = set()
    position = 0
    while position < len(value):
        character = value[position]
        if character == "'":
            if position + 1 < len(value) and value[position + 1] == "'":
                python_parts.append("'")
                position += 2
                continue
            position += 1
            literal: list[str] = []
            while position < len(value):
                if value[position] != "'":
                    literal.append(value[position])
                    position += 1
                    continue
                if position + 1 < len(value) and value[position + 1] == "'":
                    literal.append("'")
                    position += 2
                    continue
                position += 1
                break
            else:
                raise ValueError(f"projection {name!r} format has an open quote")
            python_parts.append("".join(literal).replace("%", "%%"))
            continue
        if character.isascii() and character.isalpha():
            end = position + 1
            while end < len(value) and value[end] == character:
                end += 1
            token = value[position:end]
            if character not in allowed:
                raise ValueError(
                    f"projection {name!r} format has unsupported pattern {token!r}"
                )
            fields.add(character)
            if set(token) == {"S"} and 1 <= len(token) <= 6:
                python_parts.append("%f")
            elif token in python_tokens:
                python_parts.append(python_tokens[token])
            else:
                python_supported = False
            position = end
            continue
        python_parts.append("%%" if character == "%" else character)
        position += 1

    has_time = bool(fields & time_fields)
    month_precision = bool(fields & {"M", "L"}) and not bool(
        fields & ({"d", "D", "w", "W"} | time_fields)
    )
    day_precision = bool(fields & {"d", "D"}) and not has_time
    return (
        value,
        "".join(python_parts) if python_supported else None,
        month_precision or day_precision,
    )


def _projection_date_range(
    name: str,
    value: Any,
    *,
    python_format: str | None,
) -> str:
    lower, upper = _projection_pair(name, value)
    if not all(isinstance(item, str) and item for item in (lower, upper)):
        raise TypeError(f"projection {name!r} date range must contain strings")
    parsed: list[dt.datetime | None] = []
    for item in (lower, upper):
        if not item.strip() or "," in item or any(ord(character) < 32 for character in item):
            raise ValueError(f"projection {name!r} date range is invalid")
        stripped = item.strip()
        if _PROJECTION_RELATIVE_DATE.fullmatch(item) is not None:
            parsed.append(None)
            continue
        if stripped.upper().startswith("NOW"):
            raise ValueError(f"projection {name!r} relative date range is invalid")
        if python_format is None:
            parsed.append(None)
            continue
        try:
            parsed.append(dt.datetime.strptime(item, python_format))
        except ValueError as exc:
            raise ValueError(
                f"projection {name!r} date range does not match its format"
            ) from exc
    if parsed[0] is not None and parsed[1] is not None and parsed[0] > parsed[1]:
        raise ValueError(f"projection {name!r} range minimum exceeds maximum")
    return f"{lower},{upper}"


def _positive_integer(name: str, value: Any) -> int:
    if isinstance(value, bool):
        raise TypeError(f"{name} must be a positive integer")
    if isinstance(value, int):
        result = value
    elif isinstance(value, str) and re.fullmatch(r"\+?\d+", value.strip()):
        result = int(value)
    else:
        raise TypeError(f"{name} must be a positive integer")
    if result <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return result


def _projection_location_template(value: Any, selected: Sequence[str]) -> str:
    template = _optional_string(
        "location_template", value, max_length=_MAX_LOCATION_LENGTH
    )
    if template is None:  # pragma: no cover - guarded by the caller
        raise TypeError("location_template must be a non-empty string")
    if (
        not template.startswith("s3://")
        or any(character.isspace() for character in template)
        or "/" not in template[5:]
        or not template[5:].split("/", 1)[0]
    ):
        raise ValueError("location_template must be an absolute s3:// path")
    if not template.endswith("/") or template.endswith("//"):
        raise ValueError("location_template must end with a single '/'")
    matches = list(_PARTITION_PLACEHOLDER.finditer(template))
    placeholders = {match.group(1) for match in matches}
    remainder = _PARTITION_PLACEHOLDER.sub("", template)
    if "${" in remainder:
        raise ValueError("location_template contains a malformed placeholder")
    unterminated = [
        match.group(1)
        for match in matches
        if match.end() == len(template) or template[match.end()] != "/"
    ]
    if unterminated:
        raise ValueError(
            "location_template placeholders must end with '/': "
            + ", ".join(f"${{{item}}}" for item in sorted(set(unterminated)))
        )
    expected = set(selected)
    missing = expected - placeholders
    unexpected = placeholders - expected
    if missing or unexpected:
        details: list[str] = []
        if missing:
            details.append(
                "missing " + ", ".join(f"${{{item}}}" for item in sorted(missing))
            )
        if unexpected:
            details.append(
                "unknown " + ", ".join(f"${{{item}}}" for item in sorted(unexpected))
            )
        raise ValueError("location_template placeholders: " + "; ".join(details))
    return template


def _partition_projection_parameter_keys(value: Mapping[str, Any]) -> set[str]:
    return {
        key
        for key in value
        if key.startswith("projection.") or key == "storage.location.template"
    }


def _partition_values(values: Any) -> list[str]:
    if isinstance(values, (str, bytes, bytearray)) or not isinstance(values, Sequence):
        raise TypeError("partition values must be a sequence of strings")
    result = list(values)
    if not result:
        raise ValueError("partition values must contain at least one item")
    if not all(isinstance(item, str) and 1 <= len(item) <= 1024 for item in result):
        raise TypeError("partition values must be strings of 1 to 1024 characters")
    return result


class _GlueTypeParser:
    def __init__(self, source: str) -> None:
        self.source = source
        self.position = 0

    def parse(self) -> pa.DataType:
        result = self._type()
        self._space()
        if self.position != len(self.source):
            self._error("unexpected trailing type syntax")
        return result

    def _type(self) -> pa.DataType:
        word = self._word().lower()
        primitives: dict[str, pa.DataType] = {
            "boolean": pa.bool_(),
            "bool": pa.bool_(),
            "tinyint": pa.int8(),
            "smallint": pa.int16(),
            "int": pa.int32(),
            "integer": pa.int32(),
            "bigint": pa.int64(),
            "float": pa.float32(),
            "real": pa.float32(),
            "double": pa.float64(),
            "string": pa.string(),
            "binary": pa.binary(),
            "date": pa.date32(),
            "timestamp": pa.timestamp("us"),
        }
        if word in primitives:
            return primitives[word]
        if word in {"varchar", "char"}:
            self._optional_length(word)
            return pa.string()
        if word == "decimal":
            self._space()
            if not self._take("("):
                return pa.decimal128(10, 0)
            precision = self._integer()
            if self._take(","):
                scale = self._integer(signed=True)
            else:
                scale = 0
            self._expect(")")
            if not 1 <= precision <= 38:
                self._error("decimal precision must be between 1 and 38")
            if not 0 <= scale <= precision:
                self._error("decimal scale must be between 0 and precision")
            return pa.decimal128(precision, scale)
        if word in {"array", "list"}:
            self._expect("<")
            child = self._type()
            self._expect(">")
            return pa.list_(pa.field("item", child, nullable=True))
        if word == "map":
            self._expect("<")
            key = self._type()
            if not _is_partition_type(key):
                self._error("Glue map keys must be primitive")
            self._expect(",")
            item = self._type()
            self._expect(">")
            return pa.map_(
                pa.field("key", key, nullable=False),
                pa.field("value", item, nullable=True),
            )
        if word == "struct":
            self._expect("<")
            fields: list[pa.Field] = []
            self._space()
            if self._take(">"):
                return pa.struct([])
            while True:
                name = self._field_name()
                self._expect(":")
                child_type = self._type()
                metadata = None
                if self._take_word("comment"):
                    comment = self._quoted_literal()
                    if len(comment) > _MAX_COMMENT_LENGTH:
                        self._error(
                            f"struct field comment exceeds {_MAX_COMMENT_LENGTH} characters"
                        )
                    metadata = {b"doc": comment.encode("utf-8")}
                fields.append(
                    pa.field(name, child_type, nullable=True, metadata=metadata)
                )
                if self._take(">"):
                    return pa.struct(fields)
                self._expect(",")
        self._error(f"unsupported Glue type {word!r}")

    def _word(self) -> str:
        self._space()
        start = self.position
        while self.position < len(self.source) and (
            self.source[self.position].isalnum() or self.source[self.position] == "_"
        ):
            self.position += 1
        if start == self.position:
            self._error("expected a type name")
        return self.source[start : self.position]

    def _field_name(self) -> str:
        self._space()
        if self._take("`"):
            pieces: list[str] = []
            while self.position < len(self.source):
                character = self.source[self.position]
                self.position += 1
                if character != "`":
                    pieces.append(character)
                elif (
                    self.position < len(self.source)
                    and self.source[self.position] == "`"
                ):
                    pieces.append("`")
                    self.position += 1
                else:
                    return "".join(pieces)
            self._error("unterminated quoted field name")
        start = self.position
        while (
            self.position < len(self.source) and self.source[self.position] not in ":,>"
        ):
            self.position += 1
        result = self.source[start : self.position].strip()
        if not result:
            self._error("expected a struct field name")
        return result

    def _integer(self, *, signed: bool = False) -> int:
        self._space()
        start = self.position
        if (
            signed
            and self.position < len(self.source)
            and self.source[self.position] in "+-"
        ):
            self.position += 1
        while self.position < len(self.source) and self.source[self.position].isdigit():
            self.position += 1
        if start == self.position:
            self._error("expected an integer")
        try:
            return int(self.source[start : self.position])
        except ValueError as exc:
            raise ValueError("invalid integer in Glue type") from exc

    def _optional_length(self, kind: str) -> None:
        self._space()
        if self._take("("):
            length = self._integer()
            self._expect(")")
            maximum = 255 if kind == "char" else 65_535
            if not 1 <= length <= maximum:
                self._error(f"{kind} length must be between 1 and {maximum}")

    def _take_word(self, word: str) -> bool:
        self._space()
        end = self.position + len(word)
        if self.source[self.position : end].lower() != word:
            return False
        if end < len(self.source) and (
            self.source[end].isalnum() or self.source[end] == "_"
        ):
            return False
        self.position = end
        return True

    def _quoted_literal(self) -> str:
        self._space()
        if self.position >= len(self.source) or self.source[self.position] != "'":
            self._error("expected a quoted comment")
        self.position += 1
        result: list[str] = []
        while self.position < len(self.source):
            character = self.source[self.position]
            self.position += 1
            if character == "'":
                if (
                    self.position < len(self.source)
                    and self.source[self.position] == "'"
                ):
                    result.append("'")
                    self.position += 1
                    continue
                return "".join(result)
            if character == "\\":
                if self.position >= len(self.source):
                    self._error("unterminated escape in quoted comment")
                result.append(self.source[self.position])
                self.position += 1
                continue
            result.append(character)
        self._error("unterminated quoted comment")

    def _expect(self, token: str) -> None:
        if not self._take(token):
            self._error(f"expected {token!r}")

    def _take(self, token: str) -> bool:
        self._space()
        if self.source.startswith(token, self.position):
            self.position += len(token)
            return True
        return False

    def _space(self) -> None:
        while self.position < len(self.source) and self.source[self.position].isspace():
            self.position += 1

    def _error(self, message: str) -> typing.NoReturn:
        raise ValueError(f"{message} at position {self.position} in {self.source!r}")
