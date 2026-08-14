from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime

import pyarrow as pa
import pytest

POSTGRES_URI = os.environ.get("RKP_TEST_POSTGRES_URI")
if not POSTGRES_URI:
    pytest.skip(
        "set RKP_TEST_POSTGRES_URI to run the live PostgreSQL ADBC integration",
        allow_module_level=True,
    )

adbc_dbapi = pytest.importorskip(
    "adbc_driver_postgresql.dbapi",
    reason="PostgreSQL integration requires adbc-driver-postgresql",
)

from rkp import (
    Record,
    arrow_into_records,
    record,
    records_into_arrow_batches,
)


@record
class PostgresEvent(Record):
    identifier: int
    label: str | None
    active: bool
    score: float
    occurred_at: datetime
    payload: bytes


EVENTS = (
    PostgresEvent(
        1,
        "first",
        True,
        1.5,
        datetime(2026, 8, 14, 10, 0, tzinfo=UTC),
        b"\x00\xff",
    ),
    PostgresEvent(2, None, False, -0.25, datetime(1970, 1, 1, tzinfo=UTC), b""),
)


def _quoted(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def test_records_round_trip_through_postgresql_arrow_protocol() -> None:
    table_name = f"rkp_arrow_{uuid.uuid4().hex}"
    quoted_table = _quoted(table_name)
    connection = adbc_dbapi.connect(POSTGRES_URI)
    try:
        with connection.cursor() as cursor:
            batches = records_into_arrow_batches(
                EVENTS,
                record_type=PostgresEvent,
                batch_size=1,
            )
            cursor.adbc_ingest(
                table_name,
                pa.RecordBatchReader.from_batches(
                    PostgresEvent.into_arrow_schema(),
                    batches,
                ),
                mode="create",
                temporary=True,
            )
            connection.commit()
            cursor.execute(f"SELECT * FROM {quoted_table} ORDER BY identifier")
            result = tuple(
                arrow_into_records(
                    PostgresEvent,
                    cursor.fetch_record_batch(),
                    validate_schema=False,
                )
            )

        assert result == EVENTS
    finally:
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {quoted_table}")
            connection.commit()
        finally:
            connection.close()
