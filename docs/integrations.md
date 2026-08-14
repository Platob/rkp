# Integrations

Arrow batches and `RecordBatchReader` are the generic integration surface. A
database, queue, object store, or execution engine can consume and produce
Arrow without an RKP-specific adapter.

## PostgreSQL through ADBC

Install the PostgreSQL ADBC driver for the application or development
environment:

```console
uv add --project python --dev adbc-driver-postgresql
```

Convert records into a reader for ingestion, then decode the query's reader:

```python
import pyarrow as pa

from adbc_driver_postgresql import dbapi
from rkp import Record, arrow_into_records, record, records_into_arrow_batches


@record
class Event(Record):
    identifier: int
    label: str | None


values = (Event(1, "created"), Event(2, None))

with dbapi.connect(postgres_uri) as connection:
    with connection.cursor() as cursor:
        reader = pa.RecordBatchReader.from_batches(
            Event.into_arrow_schema(),
            records_into_arrow_batches(
                values,
                record_type=Event,
                batch_size=8192,
            ),
        )
        cursor.adbc_ingest("events", reader, mode="append")
        connection.commit()

        cursor.execute("SELECT identifier, label FROM events ORDER BY identifier")
        restored = tuple(
            arrow_into_records(
                Event,
                cursor.fetch_record_batch(),
                validate_schema=False,
            )
        )
```

The relaxed validation in this example acknowledges that PostgreSQL owns the
result's physical Arrow contract and metadata. Safe typed row conversion still
applies. Keep full validation enabled when the producer promises the exact RKP
schema.

## Safe live example

[`examples/postgres_adbc.py`](examples/postgres_adbc.py) is inert unless
`RKP_TEST_POSTGRES_URI` is set. When enabled, it creates a uniquely named
temporary table and drops it in `finally`. Point it only at a disposable test
database:

```console
RKP_TEST_POSTGRES_URI=postgresql://localhost/rkp_test \
  uv run --project python --with adbc-driver-postgresql \
  python docs/examples/postgres_adbc.py
```

PowerShell:

```powershell
$env:RKP_TEST_POSTGRES_URI = "postgresql://localhost/rkp_test"
uv run --project python --with adbc-driver-postgresql python docs/examples/postgres_adbc.py
```

The repository's live integration test uses the same opt-in variable:

```console
RKP_TEST_POSTGRES_URI=postgresql://localhost/rkp_test \
  uv run --project python --with adbc-driver-postgresql \
  pytest python/tests/integration/test_postgres_adbc.py -q
```

## Designing another Arrow integration

1. Publish a canonical schema with `RecordType.into_arrow_schema()`.
2. Stream records with `into_arrow_batches()` or `into_arrow_reader()`.
3. Prefer protocol APIs that accept/return `RecordBatchReader` to avoid an
   extra table-shaped copy.
4. Decode with `RecordType.from_arrow(source)`.
5. Disable schema validation only for a known equivalent protocol projection,
   and test its edge types explicitly.

This pattern is also used internally by the [Spark](spark.md) and
[Iceberg](iceberg.md) integrations.
