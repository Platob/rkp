from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pyarrow as pa
import pytest
from pyiceberg.types import (
    DecimalType,
    FixedType,
    IntegerType,
    LongType,
    TimestampNanoType,
    TimestamptzType,
    UnknownType,
    UUIDType,
)
from rkp import (
    Record,
    arrow_into_iceberg_schema,
    field,
    iceberg_into_arrow_schema,
    into_iceberg_schema,
    record,
)


@record(alias="versioned")
class Versioned(Record):
    identifier: int = field(seq=1, primary_key=True)
    counter: int = field(seq=2, type=pa.int32())
    amount: Decimal = field(seq=3, type=pa.decimal128(9, 2))
    digest: bytes = field(seq=4, type=pa.binary(8))
    occurred_at: dt.datetime = field(seq=5)


@pytest.mark.parametrize("format_version", [1, 2, 3])
def test_every_supported_format_version_maps_the_same_scalar_types(
    format_version: int,
) -> None:
    schema = Versioned.into_iceberg_schema(format_version=format_version)

    assert isinstance(schema.find_field("identifier").field_type, LongType)
    assert isinstance(schema.find_field("counter").field_type, IntegerType)
    assert isinstance(schema.find_field("amount").field_type, DecimalType)
    assert isinstance(schema.find_field("digest").field_type, FixedType)
    assert isinstance(schema.find_field("occurred_at").field_type, TimestamptzType)
    assert schema.identifier_field_ids == [1]


@pytest.mark.parametrize("format_version", [1, 2])
def test_v3_only_types_are_rejected_by_earlier_versions(format_version: int) -> None:
    unknown = pa.schema([pa.field("payload", pa.null(), nullable=True)])
    nanos = pa.schema([pa.field("moment", pa.timestamp("ns"), nullable=False)])

    with pytest.raises(ValueError, match="format version"):
        arrow_into_iceberg_schema(unknown, format_version=format_version)
    with pytest.raises(TypeError, match=r"unsupported type: timestamp\[ns\]"):
        arrow_into_iceberg_schema(
            nanos,
            format_version=format_version,
            downcast_ns_timestamp_to_us=False,
        )


def test_v3_accepts_unknown_and_nanosecond_columns() -> None:
    schema = arrow_into_iceberg_schema(
        pa.schema(
            [
                pa.field("payload", pa.null(), nullable=True),
                pa.field("moment", pa.timestamp("ns"), nullable=False),
                pa.field("identity", _uuid_type(), nullable=False),
            ]
        ),
        format_version=3,
    )

    assert isinstance(schema.find_field("payload").field_type, UnknownType)
    assert schema.find_field("payload").required is False
    assert isinstance(schema.find_field("moment").field_type, TimestampNanoType)
    assert isinstance(schema.find_field("identity").field_type, UUIDType)


def test_v3_defaults_travel_through_arrow_metadata() -> None:
    arrow_schema = pa.schema(
        [
            pa.field(
                "count",
                pa.int64(),
                nullable=False,
                metadata={
                    b"PARQUET:field_id": b"1",
                    b"iceberg.initial_default": b"7",
                    b"iceberg.write_default": b"9",
                },
            )
        ]
    )

    schema = arrow_into_iceberg_schema(arrow_schema, format_version=3)
    count = schema.find_field("count")

    assert (count.initial_default, count.write_default) == (7, 9)
    restored = iceberg_into_arrow_schema(schema)
    assert restored.field("count").metadata[b"iceberg.initial_default"] == b"7"
    assert restored.field("count").metadata[b"iceberg.write_default"] == b"9"
    assert into_iceberg_schema(restored, format_version=3) == schema


@pytest.mark.parametrize("format_version", [1, 2])
def test_defaults_require_format_version_three(format_version: int) -> None:
    arrow_schema = pa.schema(
        [
            pa.field(
                "label",
                pa.string(),
                nullable=True,
                metadata={
                    b"PARQUET:field_id": b"1",
                    b"iceberg.initial_default": b'"none"',
                },
            )
        ]
    )

    with pytest.raises(TypeError, match="requires format version 3"):
        arrow_into_iceberg_schema(arrow_schema, format_version=format_version)


def test_format_version_is_validated() -> None:
    with pytest.raises(ValueError, match="format_version must be 1, 2, or 3"):
        Versioned.into_iceberg_schema(format_version=4)
    with pytest.raises(ValueError, match="format_version must be 1, 2, or 3"):
        arrow_into_iceberg_schema(Versioned.into_arrow_schema(), format_version=0)


def _uuid_type() -> pa.DataType:
    factory = getattr(pa, "uuid", None)
    return factory() if factory is not None else pa.binary(16)
