"""Methods installed by :func:`rkp.records.record`."""

from __future__ import annotations

import importlib
import os
from types import EllipsisType
from typing import Any

from .._io import is_path_string

__all__ = ["install_record_methods"]


class _DisabledMethod:
    def __get__(self, instance: Any, owner: type[Any] | None = None) -> Any:
        raise AttributeError("serialization method is disabled")


def install_record_methods(cls: type[Any]) -> None:
    """Install enabled codec methods and generic dispatch on ``cls``."""

    methods: dict[str, Any] = {
        "from_arrow": classmethod(_from_arrow),
        "from_arrow_batch": classmethod(_from_arrow_batch),
        "into_arrow_batch": classmethod(_into_arrow_batch),
        "into_arrow_batches": classmethod(_into_arrow_batches),
        "into_arrow_reader": classmethod(_into_arrow_reader),
        "into_arrow_field": classmethod(_into_arrow_field),
        "into_arrow_schema": classmethod(_into_arrow_schema),
        "into_iceberg_field": classmethod(_into_iceberg_field),
        "into_iceberg_schema": classmethod(_into_iceberg_schema),
        "into_spark_schema": classmethod(_into_spark_schema),
        "into_spark_dataframe": classmethod(_into_spark_dataframe),
        "from_spark": classmethod(_from_spark),
        "into_glue_table_input": classmethod(_into_glue_table_input),
        "into_glue_ddl": classmethod(_into_glue_ddl),
        "into_glue_partition_projection": classmethod(_into_glue_partition_projection),
        "into_glue_partition_values": _into_glue_partition_values,
        "load": classmethod(_load),
        "loads": classmethod(_loads),
        "dump": _dump,
        "dump_bytes": _dump_bytes,
        "dumps": _dumps,
        "dumps_bytes": _dumps_bytes,
    }
    if cls.__dict__.get("__record_with_json__", False):
        methods.update(
            {
                "load_json": classmethod(_load_json),
                "loads_json": classmethod(_loads_json),
                "dump_json": _dump_json,
                "dump_json_bytes": _dump_json_bytes,
                "dumps_json": _dumps_json,
                "dumps_json_bytes": _dumps_json_bytes,
            }
        )
    else:
        for name in (
            "load_json",
            "loads_json",
            "dump_json",
            "dump_json_bytes",
            "dumps_json",
            "dumps_json_bytes",
        ):
            if name not in cls.__dict__:
                setattr(cls, name, _DisabledMethod())
    if cls.__dict__.get("__record_with_yaml__", False):
        methods.update(
            {
                "load_yaml": classmethod(_load_yaml),
                "loads_yaml": classmethod(_loads_yaml),
                "dump_yaml": _dump_yaml,
                "dump_yaml_bytes": _dump_yaml_bytes,
                "dumps_yaml": _dumps_yaml,
                "dumps_yaml_bytes": _dumps_yaml_bytes,
            }
        )
    else:
        for name in (
            "load_yaml",
            "loads_yaml",
            "dump_yaml",
            "dump_yaml_bytes",
            "dumps_yaml",
            "dumps_yaml_bytes",
        ):
            if name not in cls.__dict__:
                setattr(cls, name, _DisabledMethod())
    for name, method in methods.items():
        if name not in cls.__dict__ and not hasattr(cls, name):
            setattr(cls, name, method)


def _into_arrow_field(
    cls: type[Any],
    name: str | None = None,
    *,
    nullable: bool = False,
) -> Any:
    arrow = importlib.import_module("rkp.records.arrow")
    return arrow.record_into_arrow_field(cls, name=name, nullable=nullable)


def _into_arrow_schema(cls: type[Any]) -> Any:
    """Return the record fields as a cached top-level Arrow schema."""

    arrow = importlib.import_module("rkp.records.arrow")
    return arrow.record_into_arrow_schema(cls)


def _from_arrow_batch(
    cls: type[Any],
    batch: Any,
    *,
    safe: bool = True,
    on_error: str = "raise",
    validate_schema: bool = True,
) -> Any:
    """Lazily construct records from one Arrow record batch."""

    arrow = importlib.import_module("rkp.records.arrow")
    return arrow.arrow_batch_into_records(
        cls,
        batch,
        safe=safe,
        on_error=on_error,
        validate_schema=validate_schema,
    )


def _from_arrow(
    cls: type[Any],
    source: Any,
    *,
    safe: bool = True,
    on_error: str = "raise",
    validate_schema: bool = True,
) -> Any:
    """Lazily construct records from an Arrow source."""

    arrow = importlib.import_module("rkp.records.arrow")
    return arrow.arrow_into_records(
        cls,
        source,
        safe=safe,
        on_error=on_error,
        validate_schema=validate_schema,
    )


def _into_arrow_batch(
    cls: type[Any],
    records: Any,
    *,
    schema: Any = None,
) -> Any:
    """Build one Arrow record batch from an iterable of this record type."""

    arrow = importlib.import_module("rkp.records.arrow")
    return arrow.records_into_arrow_batch(records, record_type=cls, schema=schema)


def _into_arrow_batches(
    cls: type[Any],
    records: Any,
    *,
    batch_size: int = 65_536,
    schema: Any = None,
) -> Any:
    """Lazily build bounded Arrow batches from records of this type."""

    arrow = importlib.import_module("rkp.records.arrow")
    return arrow.records_into_arrow_batches(
        records,
        batch_size=batch_size,
        record_type=cls,
        schema=schema,
    )


def _into_arrow_reader(
    cls: type[Any],
    records: Any,
    *,
    batch_size: int = 65_536,
    schema: Any = None,
) -> Any:
    """Expose records of this type through a streaming Arrow reader."""

    arrow = importlib.import_module("rkp.records.arrow")
    return arrow.records_into_arrow_reader(
        records,
        batch_size=batch_size,
        record_type=cls,
        schema=schema,
    )


def _into_spark_schema(
    cls: type[Any],
    *,
    prefer_timestamp_ntz: bool = True,
) -> Any:
    """Return this record's Spark SQL schema through Arrow."""

    spark = _spark_adapter()
    return spark.into_spark_schema(
        cls,
        prefer_timestamp_ntz=prefer_timestamp_ntz,
    )


def _into_spark_dataframe(
    cls: type[Any],
    records: Any,
    *,
    spark: Any = None,
    batch_size: int = 65_536,
) -> Any:
    """Build a Spark DataFrame from records of this type."""

    adapter = _spark_adapter()
    return adapter.records_into_spark_dataframe(
        records,
        record_type=cls,
        spark=spark,
        batch_size=batch_size,
    )


def _from_spark(
    cls: type[Any],
    dataframe: Any,
    *,
    batch_size: int = 65_536,
    safe: bool = True,
    on_error: str = "raise",
    validate_schema: bool = True,
) -> Any:
    """Collect a Spark DataFrame through Arrow into this record type."""

    spark = _spark_adapter()
    return spark.spark_dataframe_into_records(
        dataframe,
        cls,
        batch_size=batch_size,
        safe=safe,
        on_error=on_error,
        validate_schema=validate_schema,
    )


def _into_iceberg_schema(
    cls: type[Any],
    *,
    schema_id: int = 0,
    field_id_start: int = 1,
    identifier_field_ids: Any = None,
    format_version: int = 2,
    downcast_ns_timestamp_to_us: bool | None | EllipsisType = ...,
) -> Any:
    """Return a cached Iceberg schema for the record."""

    iceberg = _iceberg_adapter()
    return iceberg.record_into_iceberg_schema(
        cls,
        schema_id=schema_id,
        field_id_start=field_id_start,
        identifier_field_ids=identifier_field_ids,
        format_version=format_version,
        downcast_ns_timestamp_to_us=downcast_ns_timestamp_to_us,
    )


def _into_iceberg_field(
    cls: type[Any],
    name: str | None = None,
    *,
    nullable: bool = False,
    field_id_start: int = 1,
    format_version: int = 2,
    downcast_ns_timestamp_to_us: bool | None | EllipsisType = ...,
) -> Any:
    """Return a cached Iceberg struct field for the record."""

    iceberg = _iceberg_adapter()
    return iceberg.record_into_iceberg_field(
        cls,
        name=name,
        nullable=nullable,
        field_id_start=field_id_start,
        format_version=format_version,
        downcast_ns_timestamp_to_us=downcast_ns_timestamp_to_us,
    )


def _iceberg_adapter() -> Any:
    try:
        return importlib.import_module("rkp.records.iceberg")
    except ModuleNotFoundError as exc:
        if exc.name == "pyiceberg" or (exc.name and exc.name.startswith("pyiceberg.")):
            raise ImportError(
                "Iceberg support requires PyIceberg; install it with "
                "'pip install rkp[iceberg]'"
            ) from exc
        raise


def _spark_adapter() -> Any:
    try:
        return importlib.import_module("rkp.records.spark")
    except ModuleNotFoundError as exc:
        if exc.name == "pyspark" or (exc.name and exc.name.startswith("pyspark.")):
            raise ImportError(
                "Spark support requires PySpark; install it with "
                "'pip install rkp[spark]'"
            ) from exc
        raise


def _into_glue_table_input(
    cls: type[Any],
    *,
    name: str | None = None,
    location: str | None = None,
    format: str = "parquet",
    description: str | None = None,
    parameters: Any = None,
    serde_parameters: Any = None,
    partition_keys: Any = None,
    partition_projection: Any = None,
    partition_location_template: str | None = None,
    partition_projection_enabled: bool = True,
) -> dict[str, Any]:
    """Build a Glue TableInput from the record schema."""

    awsglue = importlib.import_module("rkp.records.awsglue")
    return awsglue.into_glue_table_input(
        cls,
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


def _into_glue_ddl(
    cls: type[Any],
    *,
    name: str | None = None,
    database: str | None = None,
    location: str | None = None,
    format: str = "parquet",
    if_not_exists: bool = True,
    description: str | None = None,
    properties: Any = None,
    serde_properties: Any = None,
    partition_keys: Any = None,
    partition_projection: Any = None,
    partition_location_template: str | None = None,
    partition_projection_enabled: bool = True,
) -> str:
    """Generate Athena/Hive DDL from the record schema."""

    awsglue = importlib.import_module("rkp.records.awsglue")
    return awsglue.into_glue_ddl(
        cls,
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


def _into_glue_partition_projection(
    cls: type[Any],
    projections: Any = None,
    *,
    partition_keys: Any = None,
    location_template: str | None = None,
    enabled: bool = True,
) -> dict[str, str]:
    """Build Athena partition-projection properties for the record schema."""

    awsglue = importlib.import_module("rkp.records.awsglue")
    return awsglue.into_glue_partition_projection(
        cls,
        projections,
        partition_keys=partition_keys,
        location_template=location_template,
        enabled=enabled,
    )


def _into_glue_partition_values(
    self: Any,
    *,
    partition_keys: Any = None,
) -> list[str]:
    """Project this record's partition values in canonical schema order."""

    awsglue = importlib.import_module("rkp.records.awsglue")
    return awsglue.into_glue_partition_values(
        self,
        partition_keys=partition_keys,
    )


def _load_json(
    cls: type[Any],
    source: Any,
    *,
    encoding: str = "utf-8",
    safe: bool = True,
    on_error: str = "raise",
    **kwargs: Any,
) -> Any:
    _require_enabled(cls, "json")
    return _codec("json").load(
        source,
        cls=cls,
        encoding=encoding,
        safe=safe,
        on_error=on_error,
        **kwargs,
    )


def _loads_json(
    cls: type[Any],
    data: Any,
    *,
    encoding: str = "utf-8",
    safe: bool = True,
    on_error: str = "raise",
    **kwargs: Any,
) -> Any:
    _require_enabled(cls, "json")
    return _codec("json").loads(
        data,
        cls=cls,
        encoding=encoding,
        safe=safe,
        on_error=on_error,
        **kwargs,
    )


def _dump_json(
    self: Any,
    destination: Any,
    *,
    encoding: str = "utf-8",
    **kwargs: Any,
) -> str | None:
    _require_enabled(type(self), "json")
    return _codec("json").dump(self, destination, encoding=encoding, **kwargs)


def _dumps_json(self: Any, *, encoding: str = "utf-8", **kwargs: Any) -> str:
    _require_enabled(type(self), "json")
    return _codec("json").dumps(self, encoding=encoding, **kwargs)


def _dump_json_bytes(
    self: Any,
    destination: Any,
    *,
    encoding: str = "utf-8",
    **kwargs: Any,
) -> bytes | None:
    _require_enabled(type(self), "json")
    return _codec("json").dump_bytes(
        self,
        destination,
        encoding=encoding,
        **kwargs,
    )


def _dumps_json_bytes(
    self: Any,
    *,
    encoding: str = "utf-8",
    **kwargs: Any,
) -> bytes:
    _require_enabled(type(self), "json")
    return _codec("json").dumps_bytes(self, encoding=encoding, **kwargs)


def _load_yaml(
    cls: type[Any],
    source: Any,
    *,
    encoding: str = "utf-8",
    safe: bool = True,
    on_error: str = "raise",
    **kwargs: Any,
) -> Any:
    _require_enabled(cls, "yaml")
    return _codec("yaml").load(
        source,
        cls=cls,
        encoding=encoding,
        safe=safe,
        on_error=on_error,
        **kwargs,
    )


def _loads_yaml(
    cls: type[Any],
    data: Any,
    *,
    encoding: str = "utf-8",
    safe: bool = True,
    on_error: str = "raise",
    **kwargs: Any,
) -> Any:
    _require_enabled(cls, "yaml")
    return _codec("yaml").loads(
        data,
        cls=cls,
        encoding=encoding,
        safe=safe,
        on_error=on_error,
        **kwargs,
    )


def _dump_yaml(
    self: Any,
    destination: Any,
    *,
    encoding: str = "utf-8",
    **kwargs: Any,
) -> str | None:
    _require_enabled(type(self), "yaml")
    return _codec("yaml").dump(self, destination, encoding=encoding, **kwargs)


def _dumps_yaml(self: Any, *, encoding: str = "utf-8", **kwargs: Any) -> str:
    _require_enabled(type(self), "yaml")
    return _codec("yaml").dumps(self, encoding=encoding, **kwargs)


def _dump_yaml_bytes(
    self: Any,
    destination: Any,
    *,
    encoding: str = "utf-8",
    **kwargs: Any,
) -> bytes | None:
    _require_enabled(type(self), "yaml")
    return _codec("yaml").dump_bytes(
        self,
        destination,
        encoding=encoding,
        **kwargs,
    )


def _dumps_yaml_bytes(
    self: Any,
    *,
    encoding: str = "utf-8",
    **kwargs: Any,
) -> bytes:
    _require_enabled(type(self), "yaml")
    return _codec("yaml").dumps_bytes(self, encoding=encoding, **kwargs)


def _load(
    cls: type[Any],
    source: Any,
    *,
    format: str | None = None,
    encoding: str = "utf-8",
    safe: bool = True,
    on_error: str = "raise",
    **kwargs: Any,
) -> Any:
    selected = _select_format(cls, format, source, operation="load")
    return _codec(selected).load(
        source,
        cls=cls,
        encoding=encoding,
        safe=safe,
        on_error=on_error,
        **kwargs,
    )


def _loads(
    cls: type[Any],
    data: Any,
    *,
    format: str | None = None,
    encoding: str = "utf-8",
    safe: bool = True,
    on_error: str = "raise",
    **kwargs: Any,
) -> Any:
    selected = _select_format(cls, format, None, operation="loads")
    return _codec(selected).loads(
        data,
        cls=cls,
        encoding=encoding,
        safe=safe,
        on_error=on_error,
        **kwargs,
    )


def _dump(
    self: Any,
    destination: Any,
    *,
    format: str | None = None,
    encoding: str = "utf-8",
    **kwargs: Any,
) -> str | None:
    selected = _select_format(type(self), format, destination, operation="dump")
    return _codec(selected).dump(self, destination, encoding=encoding, **kwargs)


def _dumps(
    self: Any,
    *,
    format: str | None = None,
    encoding: str = "utf-8",
    **kwargs: Any,
) -> str:
    selected = _select_format(type(self), format, None, operation="dumps")
    return _codec(selected).dumps(self, encoding=encoding, **kwargs)


def _dump_bytes(
    self: Any,
    destination: Any,
    *,
    format: str | None = None,
    encoding: str = "utf-8",
    **kwargs: Any,
) -> bytes | None:
    selected = _select_format(type(self), format, destination, operation="dump_bytes")
    return _codec(selected).dump_bytes(
        self,
        destination,
        encoding=encoding,
        **kwargs,
    )


def _dumps_bytes(
    self: Any,
    *,
    format: str | None = None,
    encoding: str = "utf-8",
    **kwargs: Any,
) -> bytes:
    selected = _select_format(type(self), format, None, operation="dumps_bytes")
    return _codec(selected).dumps_bytes(self, encoding=encoding, **kwargs)


def _select_format(
    cls: type[Any],
    requested: str | None,
    target: Any,
    *,
    operation: str,
) -> str:
    if requested is not None:
        if not isinstance(requested, str):
            raise TypeError("format must be 'json', 'yaml', or None")
        selected = requested.lower().lstrip(".")
        if selected == "yml":
            selected = "yaml"
        if selected not in {"json", "yaml"}:
            raise ValueError("format must be 'json' or 'yaml'")
        _require_enabled(cls, selected)
        return selected

    filename = _filename(target)
    if filename is not None:
        suffix = os.path.splitext(filename)[1].lower()
        if suffix == ".json":
            _require_enabled(cls, "json")
            return "json"
        if suffix in {".yaml", ".yml"}:
            _require_enabled(cls, "yaml")
            return "yaml"
        raise ValueError(
            f"cannot infer {operation} format from {filename!r}; "
            "pass format='json' or format='yaml'"
        )

    if target is not None:
        if (isinstance(target, str) and not is_path_string(target)) or isinstance(
            target, (bytes, bytearray, memoryview)
        ):
            if cls.__dict__.get("__record_with_json__", False):
                return "json"
            if cls.__dict__.get("__record_with_yaml__", False):
                return "yaml"
            raise RuntimeError(
                f"no serialization format is enabled for {cls.__qualname__}"
            )
        raise ValueError(
            f"cannot infer {operation} format from an unnamed stream; "
            "pass format='json' or format='yaml'"
        )

    # String codecs cannot be distinguished reliably because YAML accepts
    # JSON.  Prefer JSON deterministically, then YAML when it is the only one.
    if cls.__dict__.get("__record_with_json__", False):
        return "json"
    if cls.__dict__.get("__record_with_yaml__", False):
        return "yaml"
    raise RuntimeError(f"no serialization format is enabled for {cls.__qualname__}")


def _filename(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value if is_path_string(value) else None
    if isinstance(value, os.PathLike):
        return os.fspath(value)
    name = getattr(value, "name", None)
    return os.fspath(name) if isinstance(name, (str, os.PathLike)) else None


def _require_enabled(cls: type[Any], format: str) -> None:
    if not cls.__dict__.get(f"__record_with_{format}__", False):
        raise RuntimeError(
            f"{format.upper()} support is disabled for {cls.__qualname__}"
        )


def _codec(format: str) -> Any:
    return importlib.import_module(f"rkp.{format}")
