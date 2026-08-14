"""Round-trip records through PostgreSQL ADBC when explicitly enabled."""

from __future__ import annotations

import os
import uuid

import pyarrow as pa
from rkp import Record, arrow_into_records, record, records_into_arrow_batches


@record
class Event(Record):
    identifier: int
    label: str | None


def _quoted(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def main() -> None:
    uri = os.environ.get("RKP_TEST_POSTGRES_URI")
    if not uri:
        print("Set RKP_TEST_POSTGRES_URI to a disposable PostgreSQL database.")
        return
    try:
        from adbc_driver_postgresql import dbapi
    except ModuleNotFoundError as exc:
        raise SystemExit("Install ADBC: uv add --dev adbc-driver-postgresql") from exc

    values = (Event(1, "created"), Event(2, None))
    table_name = f"rkp_docs_{uuid.uuid4().hex}"
    quoted_table = _quoted(table_name)
    connection = dbapi.connect(uri)
    try:
        with connection.cursor() as cursor:
            reader = pa.RecordBatchReader.from_batches(
                Event.into_arrow_schema(),
                records_into_arrow_batches(values, record_type=Event, batch_size=1),
            )
            cursor.adbc_ingest(
                table_name,
                reader,
                mode="create",
                temporary=True,
            )
            connection.commit()
            cursor.execute(f"SELECT * FROM {quoted_table} ORDER BY identifier")
            restored = tuple(
                arrow_into_records(
                    Event,
                    cursor.fetch_record_batch(),
                    validate_schema=False,
                )
            )
            assert restored == values
    finally:
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {quoted_table}")
            connection.commit()
        finally:
            connection.close()


if __name__ == "__main__":
    main()
