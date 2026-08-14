"""Public utilities for Arrow, Spark, Iceberg, and AWS Glue records."""

from __future__ import annotations

import importlib
from collections.abc import Iterable, Mapping
from types import EllipsisType
from typing import Any

from .records.interop import (
    dataclass_from_dict,
    is_record,
    is_record_type,
    record_from_dict,
    resolved_type_hints,
    serialized_field_name,
    to_dict,
)

__all__ = [
    "arrow_batch_into_records",
    "arrow_into_glue_column",
    "arrow_into_glue_columns",
    "arrow_into_iceberg_field",
    "arrow_into_iceberg_schema",
    "arrow_into_records",
    "arrow_into_spark_dataframe",
    "arrow_into_spark_field",
    "arrow_type_into_glue_type",
    "arrow_type_into_spark_type",
    "catalog_name",
    "dataclass_from_dict",
    "dataclass_into_arrow_field",
    "dataclass_into_arrow_schema",
    "dataclass_into_iceberg_field",
    "dataclass_into_iceberg_schema",
    "glue_into_arrow_field",
    "glue_into_arrow_schema",
    "iceberg_fields_into_schema",
    "iceberg_into_arrow_field",
    "iceberg_into_arrow_schema",
    "into_arrow_field",
    "into_arrow_schema",
    "into_arrow_type",
    "into_glue_columns",
    "into_glue_database_ddl",
    "into_glue_ddl",
    "into_glue_drop_database_ddl",
    "into_glue_drop_table_ddl",
    "into_glue_partition_projection",
    "into_glue_partition_values",
    "into_glue_table_input",
    "into_iceberg_field",
    "into_iceberg_schema",
    "into_spark_schema",
    "is_record",
    "is_record_type",
    "record_from_dict",
    "records_into_arrow_batch",
    "records_into_arrow_batches",
    "records_into_arrow_reader",
    "records_into_spark_dataframe",
    "resolved_type_hints",
    "schema_metadata",
    "schema_name",
    "serialized_field_name",
    "spark_dataframe_into_arrow",
    "spark_dataframe_into_records",
    "spark_into_arrow_field",
    "spark_into_arrow_schema",
    "spark_type_into_arrow_type",
    "table_name",
    "to_dict",
]


def arrow_type_into_spark_type(
    value: Any,
    *,
    prefer_timestamp_ntz: bool = True,
) -> Any:
    """Convert an Arrow type to Spark SQL without eagerly importing PySpark."""

    return _spark().arrow_type_into_spark_type(
        value,
        prefer_timestamp_ntz=prefer_timestamp_ntz,
    )


def spark_type_into_arrow_type(
    value: Any,
    *,
    timezone: str | None = "UTC",
    prefers_large_types: bool = False,
) -> Any:
    """Convert a Spark SQL type to Arrow through the optional adapter."""

    return _spark().spark_type_into_arrow_type(
        value,
        timezone=timezone,
        prefers_large_types=prefers_large_types,
    )


def arrow_into_spark_field(
    field: Any,
    *,
    prefer_timestamp_ntz: bool = True,
) -> Any:
    """Convert one Arrow field to Spark while preserving its contract."""

    return _spark().arrow_into_spark_field(
        field,
        prefer_timestamp_ntz=prefer_timestamp_ntz,
    )


def spark_into_arrow_field(
    field: Any,
    *,
    timezone: str | None = "UTC",
    prefers_large_types: bool = False,
) -> Any:
    """Convert one Spark SQL field to Arrow."""

    return _spark().spark_into_arrow_field(
        field,
        timezone=timezone,
        prefers_large_types=prefers_large_types,
    )


def into_spark_schema(
    value: Any,
    *,
    prefer_timestamp_ntz: bool = True,
) -> Any:
    """Build a Spark SQL schema through the canonical Arrow schema."""

    return _spark().into_spark_schema(
        value,
        prefer_timestamp_ntz=prefer_timestamp_ntz,
    )


def spark_into_arrow_schema(
    schema: Any,
    *,
    timezone: str | None = "UTC",
    prefers_large_types: bool = False,
    metadata: Mapping[str | bytes, Any] | None = None,
) -> Any:
    """Convert a Spark SQL schema back into Arrow."""

    return _spark().spark_into_arrow_schema(
        schema,
        timezone=timezone,
        prefers_large_types=prefers_large_types,
        metadata=metadata,
    )


def arrow_into_spark_dataframe(source: Any, *, spark: Any = None) -> Any:
    """Create a Spark DataFrame directly from an Arrow source."""

    return _spark().arrow_into_spark_dataframe(source, spark=spark)


def records_into_spark_dataframe(
    records: Iterable[Any],
    *,
    record_type: type[Any] | None = None,
    spark: Any = None,
    batch_size: int = 65_536,
) -> Any:
    """Create a Spark DataFrame from bounded Arrow record batches."""

    return _spark().records_into_spark_dataframe(
        records,
        record_type=record_type,
        spark=spark,
        batch_size=batch_size,
    )


def spark_dataframe_into_arrow(
    dataframe: Any,
    *,
    metadata: Mapping[str | bytes, Any] | None = None,
) -> Any:
    """Collect a Spark DataFrame as an Arrow table."""

    return _spark().spark_dataframe_into_arrow(dataframe, metadata=metadata)


def spark_dataframe_into_records(
    dataframe: Any,
    record_type: type[Any],
    *,
    batch_size: int = 65_536,
    safe: bool = True,
    on_error: str = "raise",
    validate_schema: bool = True,
) -> Any:
    """Collect a Spark DataFrame through Arrow and construct records."""

    return _spark().spark_dataframe_into_records(
        dataframe,
        record_type,
        batch_size=batch_size,
        safe=safe,
        on_error=on_error,
        validate_schema=validate_schema,
    )


def arrow_batch_into_records(
    record_type: type[Any],
    batch: Any,
    *,
    safe: bool = True,
    on_error: str = "raise",
    validate_schema: bool = True,
) -> Any:
    """Lazily construct records from one Arrow record batch."""

    return _arrow().arrow_batch_into_records(
        record_type,
        batch,
        safe=safe,
        on_error=on_error,
        validate_schema=validate_schema,
    )


def arrow_into_records(
    record_type: type[Any],
    source: Any,
    *,
    safe: bool = True,
    on_error: str = "raise",
    validate_schema: bool = True,
) -> Any:
    """Lazily construct records from an Arrow source."""

    return _arrow().arrow_into_records(
        record_type,
        source,
        safe=safe,
        on_error=on_error,
        validate_schema=validate_schema,
    )


def records_into_arrow_batch(
    records: Iterable[Any],
    *,
    record_type: type[Any] | None = None,
    schema: Any = None,
) -> Any:
    """Build one Arrow record batch from an iterable of records."""

    return _arrow().records_into_arrow_batch(
        records,
        record_type=record_type,
        schema=schema,
    )


def records_into_arrow_batches(
    records: Iterable[Any],
    *,
    batch_size: int = 65_536,
    record_type: type[Any] | None = None,
    schema: Any = None,
) -> Any:
    """Lazily build bounded Arrow record batches."""

    return _arrow().records_into_arrow_batches(
        records,
        batch_size=batch_size,
        record_type=record_type,
        schema=schema,
    )


def records_into_arrow_reader(
    records: Iterable[Any],
    *,
    batch_size: int = 65_536,
    record_type: type[Any] | None = None,
    schema: Any = None,
) -> Any:
    """Expose records through a streaming Arrow record-batch reader."""

    return _arrow().records_into_arrow_reader(
        records,
        batch_size=batch_size,
        record_type=record_type,
        schema=schema,
    )


def into_arrow_type(annotation: Any) -> Any:
    """Infer an Arrow type, importing PyArrow only for this call."""

    return _arrow().into_arrow_type(annotation)


def into_arrow_field(
    name: Any,
    annotation: Any = ...,
    *,
    nullable: bool | None = None,
    owner: type[Any] | None = None,
) -> Any:
    """Infer an Arrow field, importing PyArrow only for this call."""

    if annotation is ...:
        return _arrow().into_arrow_field(
            name,
            nullable=nullable,
            owner=owner,
        )
    return _arrow().into_arrow_field(
        name,
        annotation,
        nullable=nullable,
        owner=owner,
    )


def dataclass_into_arrow_field(
    dataclass_type: type[Any],
    *,
    name: str | None = None,
    nullable: bool = False,
    localns: dict[str, Any] | None = None,
) -> Any:
    """Infer a struct field for a dataclass, loading PyArrow lazily."""

    return _arrow().dataclass_into_arrow_field(
        dataclass_type,
        name=name,
        nullable=nullable,
        localns=localns,
    )


def into_arrow_schema(
    value: Any,
    *,
    metadata: dict[str | bytes, Any] | None = None,
    localns: dict[str, Any] | None = None,
) -> Any:
    """Build an Arrow schema from a dataclass, Arrow, or Iceberg schema."""

    return _arrow().into_arrow_schema(
        value,
        metadata=metadata,
        localns=localns,
    )


def dataclass_into_arrow_schema(
    dataclass_type: type[Any],
    *,
    metadata: dict[str | bytes, Any] | None = None,
    localns: dict[str, Any] | None = None,
) -> Any:
    """Infer a top-level Arrow schema from an ordinary dataclass."""

    return _arrow().dataclass_into_arrow_schema(
        dataclass_type,
        metadata=metadata,
        localns=localns,
    )


def schema_metadata(value: Any) -> dict[bytes, bytes]:
    """Return normalized portable metadata from the canonical Arrow schema."""

    return _arrow().schema_metadata(value)


def catalog_name(value: Any) -> str | None:
    """Return a record, dataclass, or Arrow schema's catalog name."""

    return _arrow().catalog_name(value)


def schema_name(value: Any) -> str | None:
    """Return a record, dataclass, or Arrow schema's namespace name."""

    return _arrow().schema_name(value)


def table_name(value: Any) -> str | None:
    """Return a record, dataclass, or Arrow schema's table name."""

    return _arrow().table_name(value)


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
) -> Any:
    """Build an Iceberg schema, loading its optional adapter lazily."""

    return _iceberg().into_iceberg_schema(
        value,
        schema_id=schema_id,
        field_id_start=field_id_start,
        identifier_field_ids=identifier_field_ids,
        format_version=format_version,
        downcast_ns_timestamp_to_us=downcast_ns_timestamp_to_us,
        localns=localns,
        owner=owner,
    )


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
) -> Any:
    """Build one Iceberg field through the canonical optional adapter."""

    kwargs = {
        "name": name,
        "nullable": nullable,
        "owner": owner,
        "field_id_start": field_id_start,
        "format_version": format_version,
        "downcast_ns_timestamp_to_us": downcast_ns_timestamp_to_us,
        "localns": localns,
    }
    if annotation is ...:
        return _iceberg().into_iceberg_field(value, **kwargs)
    return _iceberg().into_iceberg_field(value, annotation, **kwargs)


def arrow_into_iceberg_field(
    field: Any,
    *,
    field_id_start: int = 1,
    format_version: int = 2,
    downcast_ns_timestamp_to_us: bool | None | EllipsisType = ...,
) -> Any:
    """Convert one Arrow field to an Iceberg NestedField."""

    return _iceberg().arrow_into_iceberg_field(
        field,
        field_id_start=field_id_start,
        format_version=format_version,
        downcast_ns_timestamp_to_us=downcast_ns_timestamp_to_us,
    )


def arrow_into_iceberg_schema(
    schema: Any,
    *,
    schema_id: int | None = None,
    field_id_start: int = 1,
    identifier_field_ids: Iterable[int] | None = None,
    format_version: int = 2,
    downcast_ns_timestamp_to_us: bool | None | EllipsisType = ...,
) -> Any:
    """Convert an Arrow schema to an Iceberg Schema."""

    return _iceberg().arrow_into_iceberg_schema(
        schema,
        schema_id=schema_id,
        field_id_start=field_id_start,
        identifier_field_ids=identifier_field_ids,
        format_version=format_version,
        downcast_ns_timestamp_to_us=downcast_ns_timestamp_to_us,
    )


def dataclass_into_iceberg_field(
    dataclass_type: type[Any],
    *,
    name: str | None = None,
    nullable: bool = False,
    field_id_start: int = 1,
    format_version: int = 2,
    downcast_ns_timestamp_to_us: bool | None | EllipsisType = ...,
    localns: Mapping[str, Any] | None = None,
) -> Any:
    """Infer one Iceberg struct field from an ordinary dataclass."""

    return _iceberg().dataclass_into_iceberg_field(
        dataclass_type,
        name=name,
        nullable=nullable,
        field_id_start=field_id_start,
        format_version=format_version,
        downcast_ns_timestamp_to_us=downcast_ns_timestamp_to_us,
        localns=localns,
    )


def dataclass_into_iceberg_schema(
    dataclass_type: type[Any],
    *,
    schema_id: int = 0,
    field_id_start: int = 1,
    identifier_field_ids: Iterable[int] | None = None,
    format_version: int = 2,
    downcast_ns_timestamp_to_us: bool | None | EllipsisType = ...,
    localns: Mapping[str, Any] | None = None,
) -> Any:
    """Infer an Iceberg schema from an ordinary dataclass."""

    return _iceberg().dataclass_into_iceberg_schema(
        dataclass_type,
        schema_id=schema_id,
        field_id_start=field_id_start,
        identifier_field_ids=identifier_field_ids,
        format_version=format_version,
        downcast_ns_timestamp_to_us=downcast_ns_timestamp_to_us,
        localns=localns,
    )


def iceberg_into_arrow_schema(
    schema: Any,
    *,
    metadata: Mapping[str | bytes, Any] | None = None,
    include_field_ids: bool = True,
) -> Any:
    """Convert an Iceberg schema to Arrow with identity metadata."""

    return _iceberg().iceberg_into_arrow_schema(
        schema,
        metadata=metadata,
        include_field_ids=include_field_ids,
    )


def iceberg_into_arrow_field(
    field: Any,
    *,
    include_field_id: bool = True,
    primary_key: bool = False,
    identifier_field_ids: Iterable[int] | None = None,
) -> Any:
    """Convert one Iceberg NestedField to an Arrow field."""

    return _iceberg().iceberg_into_arrow_field(
        field,
        include_field_id=include_field_id,
        primary_key=primary_key,
        identifier_field_ids=identifier_field_ids,
    )


def iceberg_fields_into_schema(
    *fields: Any,
    schema_id: int = 0,
    identifier_field_ids: Iterable[int] | None = None,
) -> Any:
    """Safely compose Iceberg fields with global identity validation."""

    return _iceberg().iceberg_fields_into_schema(
        *fields,
        schema_id=schema_id,
        identifier_field_ids=identifier_field_ids,
    )


def arrow_type_into_glue_type(value: Any, *, path: str = "value") -> str:
    """Convert one Arrow data type to Glue/Hive type syntax."""

    return _awsglue().arrow_type_into_glue_type(value, path=path)


def arrow_into_glue_column(field: Any, *, path: str = "") -> dict[str, Any]:
    """Convert one Arrow field to a Glue Column mapping."""

    return _awsglue().arrow_into_glue_column(field, path=path)


def arrow_into_glue_columns(schema: Any) -> list[dict[str, Any]]:
    """Convert an Arrow schema to Glue columns."""

    return _awsglue().arrow_into_glue_columns(schema)


def into_glue_columns(value: Any) -> list[dict[str, Any]]:
    """Convert a record/dataclass/schema to Glue columns."""

    return _awsglue().into_glue_columns(value)


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
    """Build a classic AWS Glue external TableInput."""

    return _awsglue().into_glue_table_input(
        value,
        name=name,
        location=location,
        format=format,
        description=description,
        parameters=parameters,
        serde_parameters=serde_parameters,
        partition_keys=partition_keys,
        partition_projection=partition_projection,
        partition_location_template=partition_location_template,
        partition_projection_enabled=partition_projection_enabled,
    )


def into_glue_partition_values(
    value: Any,
    schema: Any = None,
    *,
    partition_keys: Iterable[str] | None = None,
) -> list[str]:
    """Project typed partition values in canonical Glue key order."""

    return _awsglue().into_glue_partition_values(
        value,
        schema,
        partition_keys=partition_keys,
    )


def into_glue_partition_projection(
    value: Any,
    projections: Mapping[str, Any] | None = None,
    *,
    partition_keys: Iterable[str] | None = None,
    location_template: str | None = None,
    enabled: bool = True,
) -> dict[str, str]:
    """Build validated Athena partition-projection table parameters."""

    return _awsglue().into_glue_partition_projection(
        value,
        projections,
        partition_keys=partition_keys,
        location_template=location_template,
        enabled=enabled,
    )


def glue_into_arrow_field(column: Mapping[str, Any]) -> Any:
    """Convert a Glue Column mapping into an Arrow field."""

    return _awsglue().glue_into_arrow_field(column)


def glue_into_arrow_schema(value: Mapping[str, Any]) -> Any:
    """Convert a Glue table/storage descriptor into an Arrow schema."""

    return _awsglue().glue_into_arrow_schema(value)


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
    """Generate deterministic Athena/Hive CREATE TABLE DDL."""

    return _awsglue().into_glue_ddl(
        value,
        name=name,
        database=database,
        location=location,
        format=format,
        if_not_exists=if_not_exists,
        description=description,
        properties=properties,
        serde_properties=serde_properties,
        partition_keys=partition_keys,
        partition_projection=partition_projection,
        partition_location_template=partition_location_template,
        partition_projection_enabled=partition_projection_enabled,
    )


def into_glue_database_ddl(
    name: str,
    *,
    if_not_exists: bool = True,
    description: str | None = None,
    location: str | None = None,
    properties: Mapping[str, Any] | None = None,
) -> str:
    """Generate deterministic CREATE DATABASE DDL."""

    return _awsglue().into_glue_database_ddl(
        name,
        if_not_exists=if_not_exists,
        description=description,
        location=location,
        properties=properties,
    )


def into_glue_drop_table_ddl(
    name: str,
    *,
    database: str | None = None,
    if_exists: bool = True,
) -> str:
    """Generate deterministic DROP TABLE DDL."""

    return _awsglue().into_glue_drop_table_ddl(
        name, database=database, if_exists=if_exists
    )


def into_glue_drop_database_ddl(
    name: str,
    *,
    if_exists: bool = True,
    cascade: bool = False,
) -> str:
    """Generate deterministic DROP DATABASE DDL."""

    return _awsglue().into_glue_drop_database_ddl(
        name, if_exists=if_exists, cascade=cascade
    )


def _arrow() -> Any:
    return importlib.import_module("rkp.records.arrow")


def _iceberg() -> Any:
    try:
        return importlib.import_module("rkp.records.iceberg")
    except ModuleNotFoundError as exc:
        if exc.name == "pyiceberg" or (exc.name and exc.name.startswith("pyiceberg.")):
            raise ImportError(
                "Iceberg support requires PyIceberg; install it with "
                "'pip install rkp[iceberg]'"
            ) from exc
        raise


def _awsglue() -> Any:
    return importlib.import_module("rkp.records.awsglue")


def _spark() -> Any:
    try:
        return importlib.import_module("rkp.records.spark")
    except ModuleNotFoundError as exc:
        if exc.name == "pyspark" or (exc.name and exc.name.startswith("pyspark.")):
            raise ImportError(
                "Spark support requires PySpark; install it with "
                "'pip install rkp[spark]'"
            ) from exc
        raise
