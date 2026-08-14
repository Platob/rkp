from __future__ import annotations

import os
import shutil
import sys
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

pytest.importorskip("pyspark", reason="Spark integration requires rkp[spark]")
from pyspark.sql import SparkSession
from rkp import (
    Record,
    arrow_into_spark_dataframe,
    field,
    into_spark_schema,
    record,
    records_into_arrow_batch,
    records_into_spark_dataframe,
    spark_dataframe_into_arrow,
    spark_dataframe_into_records,
    spark_into_arrow_schema,
)


@record
class SparkMetric(Record):
    name: str
    value: float | None


@record
class SparkEvent(Record):
    identifier: int = field(alias="event_id", primary_key=True)
    label: str | None = None
    active: bool = True
    occurred_at: datetime = datetime(1970, 1, 1, tzinfo=UTC)
    metrics: list[SparkMetric] = field(default_factory=list)
    dimensions: dict[str, int | None] = field(default_factory=dict)
    payload: bytes = b""


@record
class SparkDuration(Record):
    elapsed: timedelta


@record
class EmptySparkRecord(Record):
    pass


EVENTS = (
    SparkEvent(
        1,
        "first",
        True,
        datetime(2026, 8, 14, 10, 0, 0, 123456, tzinfo=UTC),
        [SparkMetric("temperature", 21.5), SparkMetric("missing", None)],
        {"width": 10, "optional": None},
        b"\x00\xff",
    ),
    SparkEvent(2, None, False, datetime(1970, 1, 1, tzinfo=UTC), [], {}, b""),
)


@pytest.fixture(scope="module")
def spark() -> Iterator[Any]:
    java_home = os.environ.get("JAVA_HOME")
    java_from_home = (
        Path(java_home) / "bin" / ("java.exe" if os.name == "nt" else "java")
        if java_home
        else None
    )
    if shutil.which("java") is None and not (
        java_from_home is not None and java_from_home.is_file()
    ):
        pytest.skip("a Java runtime is required for the local Spark integration")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[1]")
        .appName("rkp-arrow-integration")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    try:
        yield session
    finally:
        session.stop()


def test_spark_schema_round_trip_uses_arrow_as_canonical_boundary() -> None:
    spark_schema = into_spark_schema(SparkEvent)
    restored = spark_into_arrow_schema(spark_schema)
    expected = SparkEvent.into_arrow_schema().remove_metadata()

    assert restored.remove_metadata() == expected
    assert spark_schema.fieldNames()[0] == "event_id"
    assert spark_schema[0].nullable is False
    assert spark_schema[1].nullable is True


def test_spark_schema_round_trip_preserves_protocol_and_nested_metadata() -> None:
    schema = pa.schema(
        [
            pa.field(
                "payload",
                pa.struct(
                    [
                        pa.field(
                            "value",
                            pa.string(),
                            nullable=False,
                            metadata={b"nested": b"yes"},
                        )
                    ]
                ),
                nullable=True,
                metadata={b"field": b"payload"},
            )
        ],
        metadata={
            b"catalog_name": b"main",
            b"schema_name": b"analytics",
            b"table_name": b"events",
            b"owner": b"integration",
        },
    )

    restored = spark_into_arrow_schema(into_spark_schema(schema))

    assert restored.equals(schema, check_metadata=True)


def test_arrow_table_round_trips_through_spark_without_pandas(spark: Any) -> None:
    batch = records_into_arrow_batch(EVENTS, record_type=SparkEvent)
    dataframe = arrow_into_spark_dataframe(pa.Table.from_batches([batch]), spark=spark)
    arrow_table = spark_dataframe_into_arrow(dataframe.orderBy("event_id"))

    assert isinstance(arrow_table, pa.Table)
    assert tuple(SparkEvent.from_arrow(arrow_table, validate_schema=False)) == EVENTS


def test_record_spark_methods_share_the_functional_api(spark: Any) -> None:
    assert SparkEvent.into_spark_schema() == into_spark_schema(SparkEvent)

    dataframe = SparkEvent.into_spark_dataframe(
        EVENTS,
        spark=spark,
        batch_size=1,
    )

    assert (
        tuple(
            SparkEvent.from_spark(
                dataframe.orderBy("event_id"),
                batch_size=1,
            )
        )
        == EVENTS
    )


def test_records_use_bounded_arrow_conversion_around_spark_collection(
    spark: Any,
) -> None:
    consumed: list[int] = []

    def source() -> Iterator[SparkEvent]:
        for event in EVENTS:
            consumed.append(event.identifier)
            yield event

    dataframe = records_into_spark_dataframe(
        source(),
        record_type=SparkEvent,
        spark=spark,
        batch_size=1,
    )
    restored = tuple(
        spark_dataframe_into_records(
            dataframe.orderBy("event_id"),
            SparkEvent,
            batch_size=1,
        )
    )

    assert consumed == [1, 2]
    assert restored == EVENTS


def test_empty_record_collection_keeps_the_record_schema(spark: Any) -> None:
    dataframe = records_into_spark_dataframe(
        [], record_type=SparkEvent, spark=spark, batch_size=2
    )

    assert dataframe.count() == 0
    assert dataframe.schema == into_spark_schema(SparkEvent)
    assert tuple(spark_dataframe_into_records(dataframe, SparkEvent)) == ()


def test_zero_field_records_preserve_rows_despite_spark_to_arrow_edge(
    spark: Any,
) -> None:
    values = (EmptySparkRecord(), EmptySparkRecord(), EmptySparkRecord())
    dataframe = records_into_spark_dataframe(
        values,
        record_type=EmptySparkRecord,
        spark=spark,
    )

    assert dataframe.count() == len(values)
    table = spark_dataframe_into_arrow(dataframe)
    assert table.num_columns == 0
    assert table.num_rows == len(values)
    assert tuple(spark_dataframe_into_records(dataframe, EmptySparkRecord)) == values


def test_duration_round_trips_through_spark_interval(spark: Any) -> None:
    values = (SparkDuration(timedelta(days=2, seconds=3, microseconds=4)),)
    dataframe = records_into_spark_dataframe(
        values,
        record_type=SparkDuration,
        spark=spark,
    )

    assert (
        spark_into_arrow_schema(dataframe.schema) == SparkDuration.into_arrow_schema()
    )
    assert tuple(spark_dataframe_into_records(dataframe, SparkDuration)) == values


def test_unsupported_arrow_types_report_the_field_path() -> None:
    schema = pa.schema([pa.field("counter", pa.uint64(), nullable=False)])

    with pytest.raises((TypeError, ValueError), match=r"(?i)counter|uint64"):
        into_spark_schema(schema)
