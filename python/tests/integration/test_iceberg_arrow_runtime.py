from __future__ import annotations

from datetime import UTC, datetime

import pyarrow as pa
import pytest

pytest.importorskip("pyiceberg", reason="Iceberg integration requires rkp[iceberg]")

from rkp import (
    Record,
    arrow_into_records,
    iceberg_into_arrow_schema,
    record,
    records_into_arrow_batch,
)


@record
class IcebergMetric(Record):
    name: str
    value: float | None


@record(table_name="iceberg_events")
class IcebergEvent(Record):
    identifier: int
    observed_at: datetime
    metrics: list[IcebergMetric]
    labels: dict[str, str | None]


EVENTS = (
    IcebergEvent(
        1,
        datetime(2026, 8, 14, 12, 30, tzinfo=UTC),
        [IcebergMetric("temperature", 21.5), IcebergMetric("missing", None)],
        {"region": "eu-west", "optional": None},
    ),
    IcebergEvent(2, datetime(1970, 1, 1, tzinfo=UTC), [], {}),
)


@pytest.mark.parametrize("format_version", [1, 2, 3])
def test_records_cross_the_arrow_iceberg_arrow_boundary(format_version: int) -> None:
    iceberg_schema = IcebergEvent.into_iceberg_schema(format_version=format_version)
    protocol_schema = iceberg_into_arrow_schema(
        iceberg_schema,
        metadata=IcebergEvent.into_arrow_schema().metadata,
    )

    batch = records_into_arrow_batch(EVENTS, schema=protocol_schema)
    restored = tuple(arrow_into_records(IcebergEvent, batch, validate_schema=False))

    assert restored == EVENTS
    assert batch.schema.metadata == protocol_schema.metadata
    assert batch.schema.metadata is not None
    assert batch.schema.metadata[b"table_name"] == b"iceberg_events"
    assert batch.schema.metadata[b"iceberg.schema_id"] == b"0"
    identifier_id = iceberg_schema.find_field("identifier").field_id
    assert identifier_id > 0
    assert batch.schema.field("identifier").metadata == {
        b"PARQUET:field_id": str(identifier_id).encode("ascii")
    }
    assert pa.types.is_large_list(batch.schema.field("metrics").type)


def test_iceberg_protocol_schema_requires_relaxed_record_validation() -> None:
    protocol_schema = iceberg_into_arrow_schema(IcebergEvent.into_iceberg_schema())
    batch = records_into_arrow_batch(EVENTS, schema=protocol_schema)

    with pytest.raises((TypeError, ValueError), match=r"(?i)schema|field"):
        tuple(arrow_into_records(IcebergEvent, batch))

    assert (
        tuple(arrow_into_records(IcebergEvent, batch, validate_schema=False)) == EVENTS
    )
