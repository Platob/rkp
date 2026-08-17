"""Dataclass records and their interoperability field configuration."""

from .base import Record
from .decorator import record
from .fields import Field, FieldOptions, field, field_options
from .interop import (
    dataclass_from_dict,
    is_record,
    is_record_type,
    record_from_dict,
    resolved_type_hints,
    serialized_field_name,
    to_dict,
)
from .metadata import RecordMetadata, record_metadata


def __getattr__(name: str):
    """Lazily expose Arrow and Spark interoperability helpers."""

    if name in _INTEROP_EXPORTS:
        from .. import utils

        return getattr(utils, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


_INTEROP_EXPORTS = {
    "arrow_batch_into_records",
    "arrow_into_avro_field",
    "arrow_into_avro_schema",
    "arrow_into_records",
    "arrow_into_spark_dataframe",
    "arrow_into_spark_field",
    "arrow_type_into_spark_type",
    "avro_into_arrow_field",
    "avro_into_arrow_schema",
    "avro_into_iceberg_schema",
    "avro_into_records",
    "create_iceberg_table",
    "dataclass_into_avro_schema",
    "iceberg_into_avro_schema",
    "iceberg_table_into_arrow",
    "iceberg_table_into_records",
    "into_avro_schema",
    "into_glue_partition_projection",
    "into_glue_partition_values",
    "into_iceberg_partition_spec",
    "into_iceberg_sort_order",
    "into_spark_schema",
    "load_iceberg_table",
    "records_into_arrow_batch",
    "records_into_arrow_batches",
    "records_into_arrow_reader",
    "records_into_avro",
    "records_into_iceberg_table",
    "records_into_spark_dataframe",
    "spark_dataframe_into_arrow",
    "spark_dataframe_into_records",
    "spark_into_arrow_field",
    "spark_into_arrow_schema",
    "spark_type_into_arrow_type",
    "sync_iceberg_table_schema",
}

__all__ = [
    "Field",
    "FieldOptions",
    "Record",
    "RecordMetadata",
    "arrow_batch_into_records",
    "arrow_into_avro_field",
    "arrow_into_avro_schema",
    "arrow_into_records",
    "arrow_into_spark_dataframe",
    "arrow_into_spark_field",
    "arrow_type_into_spark_type",
    "avro_into_arrow_field",
    "avro_into_arrow_schema",
    "avro_into_iceberg_schema",
    "avro_into_records",
    "create_iceberg_table",
    "dataclass_from_dict",
    "dataclass_into_avro_schema",
    "field",
    "field_options",
    "iceberg_into_avro_schema",
    "iceberg_table_into_arrow",
    "iceberg_table_into_records",
    "into_avro_schema",
    "into_glue_partition_projection",
    "into_glue_partition_values",
    "into_iceberg_partition_spec",
    "into_iceberg_sort_order",
    "into_spark_schema",
    "is_record",
    "is_record_type",
    "load_iceberg_table",
    "record",
    "record_from_dict",
    "record_metadata",
    "records_into_arrow_batch",
    "records_into_arrow_batches",
    "records_into_arrow_reader",
    "records_into_avro",
    "records_into_iceberg_table",
    "records_into_spark_dataframe",
    "resolved_type_hints",
    "serialized_field_name",
    "spark_dataframe_into_arrow",
    "spark_dataframe_into_records",
    "spark_into_arrow_field",
    "spark_into_arrow_schema",
    "spark_type_into_arrow_type",
    "sync_iceberg_table_schema",
    "to_dict",
]
