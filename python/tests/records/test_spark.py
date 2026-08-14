from __future__ import annotations

import pyarrow as pa
import pytest

pytest.importorskip("pyspark", reason="Spark interop requires rkp[spark]")
from pyspark.sql.types import (
    ArrayType,
    DayTimeIntervalType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from rkp import (
    arrow_into_spark_field,
    arrow_type_into_spark_type,
    into_spark_schema,
    spark_into_arrow_field,
    spark_into_arrow_schema,
    spark_type_into_arrow_type,
)


def test_type_adapters_cover_nested_decimal_and_interval_types() -> None:
    arrow_type = pa.struct(
        [
            pa.field("amount", pa.decimal128(18, 4), nullable=False),
            pa.field("labels", pa.list_(pa.field("item", pa.string())), nullable=True),
            pa.field("elapsed", pa.duration("us"), nullable=False),
        ]
    )

    spark_type = arrow_type_into_spark_type(arrow_type)
    restored = spark_type_into_arrow_type(spark_type)

    assert isinstance(spark_type[0].dataType, DecimalType)
    assert isinstance(spark_type[1].dataType, ArrayType)
    assert isinstance(spark_type[2].dataType, DayTimeIntervalType)
    assert restored == arrow_type


def test_field_adapter_preserves_nested_and_spark_specific_metadata() -> None:
    original = pa.field(
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
        metadata={b"doc": b"Payload"},
    )

    spark_field = arrow_into_spark_field(original)
    assert spark_into_arrow_field(spark_field).equals(original, check_metadata=True)

    native = StructField("value", LongType(), False, {"comment": "from Spark"})
    arrow_field = spark_into_arrow_field(native)
    round_trip = arrow_into_spark_field(arrow_field)
    assert round_trip.metadata["comment"] == "from Spark"


def test_schema_projection_retains_arrow_schema_metadata() -> None:
    schema = pa.schema(
        [
            pa.field("first", pa.int64(), nullable=False),
            pa.field("second", pa.string(), metadata={b"doc": b"selected"}),
        ],
        metadata={b"table_name": b"events", b"owner": b"tests"},
    )

    spark_schema = into_spark_schema(schema)
    projected = StructType([spark_schema[1]])
    restored = spark_into_arrow_schema(projected)

    assert restored.names == ["second"]
    assert restored.metadata == schema.metadata
    assert restored.field(0).metadata == {b"doc": b"selected"}


def test_naive_timestamp_round_trip_accepts_either_spark_timestamp_policy() -> None:
    schema = pa.schema([pa.field("created", pa.timestamp("us"), nullable=False)])

    for prefer_timestamp_ntz in (True, False):
        spark_schema = into_spark_schema(
            schema,
            prefer_timestamp_ntz=prefer_timestamp_ntz,
        )
        assert spark_into_arrow_schema(spark_schema) == schema


def test_attached_arrow_schema_is_validated_against_live_spark_layout() -> None:
    schema = into_spark_schema(pa.schema([pa.field("value", pa.int64())]))
    stale = StructType(
        [StructField("value", StringType(), True, dict(schema[0].metadata))]
    )

    with pytest.raises(ValueError, match="no longer matches"):
        spark_into_arrow_schema(stale)


@pytest.mark.parametrize(
    ("operation", "match"),
    [
        (lambda: arrow_type_into_spark_type(pa.uint64()), "uint64"),
        (lambda: arrow_type_into_spark_type("int64"), "DataType"),
        (lambda: spark_type_into_arrow_type("long"), "DataType"),
        (lambda: into_spark_schema(pa.schema([]), prefer_timestamp_ntz=1), "bool"),
        (lambda: spark_into_arrow_schema(StructType([]), timezone=1), "timezone"),
    ],
)
def test_spark_adapters_reject_invalid_inputs(operation: object, match: str) -> None:
    with pytest.raises((TypeError, ValueError), match=match):
        operation()  # type: ignore[operator]
