"""Static consumer probe for methods installed by ``@record``.

This module is checked by mypy and is intentionally not executed by pytest:
the protocol calls model consumer code without starting Spark or contacting
external services.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, assert_type

import pyarrow as pa
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.types import NestedField
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from rkp import Record, record


@record
class Event(Record):
    identifier: int
    label: str


def check_codecs(event: Event) -> None:
    assert_type(Event.from_dict({"identifier": 1, "label": "created"}), Event)
    assert_type(Event.loads('{"identifier": 1, "label": "created"}'), Event)
    assert_type(
        Event.load(memoryview(b'{"identifier": 1, "label": "created"}')),
        Event,
    )
    assert_type(Event.load_yaml("identifier: 1\nlabel: created\n"), Event)
    assert_type(Event.loads_json(event.dumps_json()), Event)
    assert_type(event.dumps(), str)
    assert_type(event.dumps_bytes(), bytes)
    assert_type(event.dumps_yaml(), str)
    assert_type(event.dumps_json_bytes(), bytes)
    assert_type(event.dumps_yaml_bytes(), bytes)
    assert_type(event.dump("buffer"), str | None)
    assert_type(event.dump_bytes("buffer"), bytes | None)
    assert_type(event.dump_json_bytes("buffer"), bytes | None)
    assert_type(event.dump_yaml_bytes("buffer"), bytes | None)


def check_arrow(event: Event, batch: pa.RecordBatch, table: pa.Table) -> None:
    assert_type(Event.into_arrow_field(), pa.Field)
    assert_type(Event.into_arrow_schema(), pa.Schema)
    assert_type(Event.from_arrow_batch(batch), Iterator[Event])
    assert_type(Event.from_arrow(table), Iterator[Event])
    assert_type(Event.into_arrow_batch([event]), pa.RecordBatch)
    assert_type(Event.into_arrow_batches([event]), Iterator[pa.RecordBatch])
    assert_type(Event.into_arrow_reader([event]), pa.RecordBatchReader)


def check_optional_protocols(
    event: Event,
    dataframe: DataFrame,
    spark: SparkSession,
) -> None:
    assert_type(Event.into_iceberg_field(), NestedField)
    assert_type(Event.into_iceberg_schema(), IcebergSchema)
    assert_type(Event.into_spark_schema(), StructType)
    assert_type(Event.into_spark_dataframe([event], spark=spark), DataFrame)
    assert_type(Event.from_spark(dataframe), Iterator[Event])
    assert_type(Event.into_glue_table_input(), dict[str, Any])
    assert_type(Event.into_glue_ddl(), str)
    assert_type(Event.into_glue_partition_projection(enabled=False), dict[str, str])
    assert_type(event.into_glue_partition_values(partition_keys=["label"]), list[str])
