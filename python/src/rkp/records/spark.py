"""PySpark interoperability built on the canonical Arrow record boundary.

This is the only :mod:`rkp.records` module that imports PySpark eagerly.
Public facades import it lazily, so the core package does not require a JVM or
load PySpark unless Spark functionality is requested.
"""

from __future__ import annotations

import base64
import inspect
import json
from collections import abc as cabc
from typing import Any, Literal

import pyarrow as pa
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.pandas.types import from_arrow_type, to_arrow_type
from pyspark.sql.types import DataType, StructField, StructType

from ._metadata import normalize_metadata
from .arrow import (
    arrow_into_records,
    into_arrow_schema,
    records_into_arrow_batches,
)

__all__ = [
    "arrow_into_spark_dataframe",
    "arrow_into_spark_field",
    "arrow_type_into_spark_type",
    "into_spark_schema",
    "records_into_spark_dataframe",
    "spark_dataframe_into_arrow",
    "spark_dataframe_into_records",
    "spark_into_arrow_field",
    "spark_into_arrow_schema",
    "spark_type_into_arrow_type",
]

_FIELD_SCHEMA_KEY = "__rkp_arrow_field_schema__"
_SPARK_METADATA_KEY = b"SPARK::metadata::json"


def arrow_type_into_spark_type(
    value: pa.DataType,
    *,
    prefer_timestamp_ntz: bool = True,
) -> DataType:
    """Convert one Arrow data type into its Spark SQL equivalent."""

    if not isinstance(value, pa.DataType):
        raise TypeError("value must be a pyarrow.DataType")
    if type(prefer_timestamp_ntz) is not bool:
        raise TypeError("prefer_timestamp_ntz must be bool")
    try:
        return from_arrow_type(value, prefer_timestamp_ntz)
    except Exception as exc:
        raise TypeError(f"cannot convert Arrow type {value} to Spark: {exc}") from exc


def spark_type_into_arrow_type(
    value: DataType,
    *,
    timezone: str | None = "UTC",
    prefers_large_types: bool = False,
) -> pa.DataType:
    """Convert one Spark SQL data type into its Arrow equivalent.

    PySpark changed this private conversion helper's timestamp keyword between
    Spark 4.0 and 4.2.  RKP resolves the installed signature at runtime while
    exposing one stable public API.
    """

    if not isinstance(value, DataType):
        raise TypeError("value must be a pyspark.sql.types.DataType")
    _validate_reverse_options(timezone, prefers_large_types)
    try:
        return _to_arrow_type_compat(
            value,
            timezone=timezone,
            prefers_large_types=prefers_large_types,
        )
    except Exception as exc:
        raise TypeError(f"cannot convert Spark type {value} to Arrow: {exc}") from exc


def arrow_into_spark_field(
    field: pa.Field,
    *,
    prefer_timestamp_ntz: bool = True,
) -> StructField:
    """Convert one Arrow field while retaining its complete Arrow contract."""

    if not isinstance(field, pa.Field):
        raise TypeError("field must be a pyarrow.Field")
    try:
        spark_type = arrow_type_into_spark_type(
            field.type,
            prefer_timestamp_ntz=prefer_timestamp_ntz,
        )
    except (TypeError, ValueError) as exc:
        raise TypeError(f"cannot convert Arrow field {field.name!r}: {exc}") from exc
    spark_metadata = _arrow_metadata_into_spark(field.metadata)
    return StructField(
        field.name,
        spark_type,
        field.nullable,
        {
            **spark_metadata,
            _FIELD_SCHEMA_KEY: _encode_arrow_schema(pa.schema([field])),
        },
    )


def spark_into_arrow_field(
    field: StructField,
    *,
    timezone: str | None = "UTC",
    prefers_large_types: bool = False,
) -> pa.Field:
    """Convert one Spark field, restoring an attached Arrow field if present."""

    if not isinstance(field, StructField):
        raise TypeError("field must be a pyspark.sql.types.StructField")
    _validate_reverse_options(timezone, prefers_large_types)
    encoded = field.metadata.get(_FIELD_SCHEMA_KEY)
    if encoded is not None:
        original = _decode_arrow_schema(encoded, path=f"Spark field {field.name!r}")
        if len(original) != 1:
            raise ValueError(f"invalid Arrow metadata on Spark field {field.name!r}")
        restored = original.field(0)
        if not _spark_field_matches_arrow(field, restored):
            raise ValueError(
                f"Spark field {field.name!r} no longer matches its attached Arrow schema"
            )
        return restored

    try:
        arrow_type = spark_type_into_arrow_type(
            field.dataType,
            timezone=timezone,
            prefers_large_types=prefers_large_types,
        )
    except (TypeError, ValueError) as exc:
        raise TypeError(f"cannot convert Spark field {field.name!r}: {exc}") from exc
    metadata = _spark_metadata_into_arrow(field.metadata)
    return pa.field(
        field.name,
        arrow_type,
        nullable=field.nullable,
        metadata=metadata or None,
    )


def into_spark_schema(
    value: Any,
    *,
    prefer_timestamp_ntz: bool = True,
) -> StructType:
    """Build a Spark schema through RKP's canonical Arrow schema adapter."""

    if type(prefer_timestamp_ntz) is not bool:
        raise TypeError("prefer_timestamp_ntz must be bool")
    if isinstance(value, StructType):
        return value
    arrow_schema = into_arrow_schema(value)
    fields: list[StructField] = []
    for field in arrow_schema:
        converted = arrow_into_spark_field(
            field,
            prefer_timestamp_ntz=prefer_timestamp_ntz,
        )
        fields.append(
            StructField(
                converted.name,
                converted.dataType,
                converted.nullable,
                {
                    **converted.metadata,
                    _FIELD_SCHEMA_KEY: _encode_arrow_schema(
                        pa.schema([field], metadata=arrow_schema.metadata)
                    ),
                },
            )
        )
    return StructType(fields)


def spark_into_arrow_schema(
    schema: StructType,
    *,
    timezone: str | None = "UTC",
    prefers_large_types: bool = False,
    metadata: cabc.Mapping[str | bytes, Any] | None = None,
) -> pa.Schema:
    """Convert a Spark schema back into Arrow, restoring portable metadata."""

    if not isinstance(schema, StructType):
        raise TypeError("schema must be a pyspark.sql.types.StructType")
    _validate_reverse_options(timezone, prefers_large_types)
    if metadata is not None and not isinstance(metadata, cabc.Mapping):
        raise TypeError("metadata must be a mapping or None")

    restored_fields: list[pa.Field] = []
    restored_metadata: dict[bytes, bytes] | None = None
    for field in schema:
        encoded = field.metadata.get(_FIELD_SCHEMA_KEY)
        if encoded is None:
            restored_fields.append(
                spark_into_arrow_field(
                    field,
                    timezone=timezone,
                    prefers_large_types=prefers_large_types,
                )
            )
            continue
        attached = _decode_arrow_schema(
            encoded,
            path=f"Spark field {field.name!r}",
        )
        if len(attached) != 1:
            raise ValueError(f"invalid Arrow metadata on Spark field {field.name!r}")
        if not _spark_field_matches_arrow(field, attached.field(0)):
            raise ValueError(
                f"Spark field {field.name!r} no longer matches its attached Arrow schema"
            )
        candidate_metadata = dict(attached.metadata or {})
        if restored_metadata is None:
            restored_metadata = candidate_metadata
        elif candidate_metadata != restored_metadata:
            raise ValueError("Spark fields contain conflicting Arrow schema metadata")
        restored_fields.append(attached.field(0))
    restored = pa.schema(restored_fields, metadata=restored_metadata or None)
    if metadata is None:
        return restored
    merged = dict(restored.metadata or {})
    merged.update(normalize_metadata(metadata))
    return restored.with_metadata(merged or None)


def arrow_into_spark_dataframe(
    source: (
        pa.RecordBatch | pa.Table | pa.RecordBatchReader | cabc.Iterable[pa.RecordBatch]
    ),
    *,
    spark: SparkSession | None = None,
) -> DataFrame:
    """Create a Spark DataFrame directly from an Arrow source, without pandas."""

    table = _arrow_source_into_table(source)
    session = _spark_session(spark)
    schema = into_spark_schema(table.schema)
    if table.num_rows == 0:
        # Spark 4.2's direct Arrow path cannot localize some empty typed
        # chunked arrays (notably timestamps).  Constructing from an empty
        # Python sequence retains the exact supplied StructType.
        return session.createDataFrame([], schema=schema)
    try:
        return session.createDataFrame(table, schema=schema)
    except Exception as exc:
        raise TypeError(f"cannot create Spark DataFrame from Arrow: {exc}") from exc


def records_into_spark_dataframe(
    records: cabc.Iterable[Any],
    *,
    record_type: type[Any] | None = None,
    spark: SparkSession | None = None,
    batch_size: int = 65_536,
) -> DataFrame:
    """Build a Spark DataFrame from bounded Arrow record batches."""

    batches = list(
        records_into_arrow_batches(
            records,
            record_type=record_type,
            batch_size=batch_size,
        )
    )
    if batches:
        table = pa.Table.from_batches(batches)
    else:
        if record_type is None:
            raise TypeError("empty records require record_type")
        table = pa.Table.from_batches([], schema=into_arrow_schema(record_type))
    return arrow_into_spark_dataframe(table, spark=spark)


def spark_dataframe_into_arrow(
    dataframe: DataFrame,
    *,
    metadata: cabc.Mapping[str | bytes, Any] | None = None,
) -> pa.Table:
    """Collect a Spark DataFrame as an Arrow table with its Arrow contract."""

    if not isinstance(dataframe, DataFrame):
        raise TypeError("dataframe must be a pyspark.sql.DataFrame")
    if metadata is not None and not isinstance(metadata, cabc.Mapping):
        raise TypeError("metadata must be a mapping or None")
    target = spark_into_arrow_schema(dataframe.schema, metadata=metadata)
    if len(dataframe.schema) == 0:
        count = dataframe.count()
        sentinel = pa.table({"__rkp_row__": pa.nulls(count)})
        # PyArrow currently resets the logical row count when metadata is
        # replaced on a zero-column table.  Empty StructType cannot carry the
        # original schema metadata anyway, so preserve rows over metadata.
        return sentinel.select([])
    to_arrow = getattr(dataframe, "toArrow", None)
    if not callable(to_arrow):
        raise TypeError("direct Arrow collection requires PySpark 4.0 or newer")
    try:
        table = to_arrow()
    except Exception as exc:
        raise TypeError(f"cannot collect Spark DataFrame as Arrow: {exc}") from exc
    try:
        return table.cast(target, safe=True)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError, ValueError) as exc:
        raise TypeError(f"Spark Arrow result does not match its schema: {exc}") from exc


def spark_dataframe_into_records(
    dataframe: DataFrame,
    record_type: type[Any],
    *,
    batch_size: int = 65_536,
    safe: bool = True,
    on_error: Literal["raise", "default"] = "raise",
    validate_schema: bool = True,
) -> cabc.Iterator[Any]:
    """Collect a Spark DataFrame through Arrow and lazily construct records."""

    if type(batch_size) is not int or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")
    table = spark_dataframe_into_arrow(dataframe)
    return arrow_into_records(
        record_type,
        table.to_batches(max_chunksize=batch_size),
        safe=safe,
        on_error=on_error,
        validate_schema=validate_schema,
    )


def _to_arrow_type_compat(
    value: DataType,
    *,
    timezone: str | None,
    prefers_large_types: bool,
) -> pa.DataType:
    parameters = inspect.signature(to_arrow_type).parameters
    kwargs: dict[str, Any] = {}
    if "error_on_duplicated_field_names_in_struct" in parameters:
        kwargs["error_on_duplicated_field_names_in_struct"] = True
    if "timezone" in parameters:
        kwargs["timezone"] = timezone
    elif "timestamp_utc" in parameters:
        kwargs["timestamp_utc"] = timezone is not None
    if "prefers_large_types" in parameters:
        kwargs["prefers_large_types"] = prefers_large_types
    return to_arrow_type(value, **kwargs)


def _arrow_source_into_table(source: Any) -> pa.Table:
    if isinstance(source, pa.Table):
        return source
    if isinstance(source, pa.RecordBatch):
        return pa.Table.from_batches([source])
    if isinstance(source, pa.RecordBatchReader):
        return source.read_all()
    if isinstance(source, (str, bytes, bytearray, memoryview, cabc.Mapping)):
        raise TypeError(
            "source must be a RecordBatch, Table, RecordBatchReader, "
            "or iterable of RecordBatch objects"
        )
    try:
        batches = list(iter(source))
    except TypeError as exc:
        raise TypeError(
            "source must be a RecordBatch, Table, RecordBatchReader, "
            "or iterable of RecordBatch objects"
        ) from exc
    for index, batch in enumerate(batches):
        if not isinstance(batch, pa.RecordBatch):
            raise TypeError(
                f"Arrow source yielded {type(batch).__qualname__} at index {index}; "
                "expected pyarrow.RecordBatch"
            )
    if not batches:
        raise TypeError("an empty Arrow batch iterable has no schema")
    return pa.Table.from_batches(batches)


def _spark_session(value: SparkSession | None) -> SparkSession:
    if value is not None:
        if not isinstance(value, SparkSession):
            raise TypeError("spark must be a pyspark.sql.SparkSession or None")
        return value
    active = SparkSession.getActiveSession()
    return active if active is not None else SparkSession.builder.getOrCreate()


def _validate_reverse_options(
    timezone: str | None,
    prefers_large_types: bool,
) -> None:
    if timezone is not None and not isinstance(timezone, str):
        raise TypeError("timezone must be a string or None")
    if type(prefers_large_types) is not bool:
        raise TypeError("prefers_large_types must be bool")


def _encode_arrow_schema(schema: pa.Schema) -> str:
    return base64.b64encode(schema.serialize().to_pybytes()).decode("ascii")


def _decode_arrow_schema(value: Any, *, path: str) -> pa.Schema:
    if not isinstance(value, str):
        raise TypeError(f"{path} Arrow metadata must be a base64 string")
    try:
        payload = base64.b64decode(value, validate=True)
        return pa.ipc.read_schema(pa.BufferReader(payload))
    except (ValueError, pa.ArrowInvalid, pa.ArrowIOError) as exc:
        raise ValueError(f"invalid Arrow metadata on {path}") from exc


def _spark_metadata_into_arrow(metadata: dict[str, Any]) -> dict[bytes, bytes]:
    payload = {
        key: value for key, value in metadata.items() if key != _FIELD_SCHEMA_KEY
    }
    if not payload:
        return {}
    return {
        _SPARK_METADATA_KEY: json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    }


def _arrow_metadata_into_spark(
    metadata: dict[bytes, bytes] | None,
) -> dict[str, Any]:
    if not metadata or _SPARK_METADATA_KEY not in metadata:
        return {}
    try:
        decoded = json.loads(metadata[_SPARK_METADATA_KEY].decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid Spark metadata on Arrow field") from exc
    if not isinstance(decoded, dict) or not all(
        isinstance(key, str) for key in decoded
    ):
        raise ValueError("Spark metadata on Arrow field must be a JSON object")
    return decoded


def _spark_field_layout_equal(actual: StructField, expected: StructField) -> bool:
    return (
        actual.name == expected.name
        and actual.nullable is expected.nullable
        and _without_metadata(actual.dataType.jsonValue())
        == _without_metadata(expected.dataType.jsonValue())
    )


def _spark_field_matches_arrow(actual: StructField, original: pa.Field) -> bool:
    return any(
        _spark_field_layout_equal(
            actual,
            arrow_into_spark_field(
                original,
                prefer_timestamp_ntz=prefer_timestamp_ntz,
            ),
        )
        for prefer_timestamp_ntz in (True, False)
    )


def _without_metadata(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _without_metadata(item)
            for key, item in value.items()
            if key != "metadata"
        }
    if isinstance(value, list):
        return [_without_metadata(item) for item in value]
    return value
