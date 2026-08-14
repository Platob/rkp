"""Live PostgreSQL Arrow protocol benchmarks using ADBC.

Set ``RKP_TEST_POSTGRES_URI`` to a disposable PostgreSQL database. The runner
creates one uniquely named temporary table and removes it in a ``finally`` block.
"""

from __future__ import annotations

import argparse
import gc
import json
import os
import platform
import statistics
import sys
import timeit
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError, version
from typing import Any

import adbc_driver_postgresql
import adbc_driver_postgresql.dbapi as adbc_dbapi
import pyarrow as pa
from rkp import (
    Record,
    arrow_into_records,
    record,
    records_into_arrow_batch,
    records_into_arrow_batches,
)


@record
class PostgresBenchmarkEvent(Record):
    identifier: int
    label: str | None
    active: bool
    score: float
    occurred_at: datetime
    payload: bytes


ROW_COUNT = 256
ROWS = tuple(
    PostgresBenchmarkEvent(
        index,
        None if index % 7 == 0 else f"event-{index}",
        index % 2 == 0,
        float(index) / 3,
        datetime(2026, 8, 14, index % 24, index % 60, tzinfo=UTC),
        index.to_bytes(4, "little"),
    )
    for index in range(ROW_COUNT)
)
BATCH = records_into_arrow_batch(ROWS, record_type=PostgresBenchmarkEvent)
TABLE = pa.Table.from_batches([BATCH])


@dataclass(frozen=True, slots=True)
class Benchmark:
    name: str
    operation: Callable[[], Any]


def _measure(
    benchmark: Benchmark, *, minimum_seconds: float, repeat: int
) -> dict[str, Any]:
    benchmark.operation()
    number = 1
    while number < 1_024:
        if timeit.timeit(benchmark.operation, number=number) >= minimum_seconds:
            break
        number *= 2
    gc.collect()
    samples = timeit.repeat(benchmark.operation, number=number, repeat=repeat)
    per_operation = [sample / number for sample in samples]
    return {
        "name": benchmark.name,
        "iterations": number,
        "median_seconds": statistics.median(per_operation),
        "minimum_seconds": min(per_operation),
        "maximum_seconds": max(per_operation),
        "samples_seconds": per_operation,
    }


def _quoted(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _duration(seconds: float) -> str:
    if seconds < 1e-3:
        return f"{seconds * 1e6:.1f} us"
    return f"{seconds * 1e3:.2f} ms"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--min-time", type=float, default=0.2)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--json", action="store_true")
    arguments = parser.parse_args(argv)
    if arguments.min_time <= 0:
        parser.error("--min-time must be greater than zero")
    if arguments.repeat < 1:
        parser.error("--repeat must be at least one")
    uri = os.environ.get("RKP_TEST_POSTGRES_URI")
    if not uri:
        parser.error("RKP_TEST_POSTGRES_URI must point to a disposable database")

    table_name = f"rkp_benchmark_{uuid.uuid4().hex}"
    quoted_table = _quoted(table_name)
    connection = adbc_dbapi.connect(uri)
    try:
        with connection.cursor() as cursor:
            cursor.adbc_ingest(table_name, TABLE, mode="create", temporary=True)
        connection.commit()

        def ingest_table_replace() -> Any:
            with connection.cursor() as cursor:
                result = cursor.adbc_ingest(table_name, TABLE, mode="replace")
            connection.commit()
            return result

        def ingest_reader_replace() -> Any:
            batches = records_into_arrow_batches(
                ROWS,
                record_type=PostgresBenchmarkEvent,
                batch_size=64,
            )
            reader = pa.RecordBatchReader.from_batches(
                PostgresBenchmarkEvent.into_arrow_schema(), batches
            )
            with connection.cursor() as cursor:
                result = cursor.adbc_ingest(table_name, reader, mode="replace")
            connection.commit()
            return result

        def fetch_arrow() -> Any:
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT * FROM {quoted_table} ORDER BY identifier")
                return cursor.fetch_arrow_table()

        def fetch_records() -> Any:
            return tuple(
                arrow_into_records(
                    PostgresBenchmarkEvent,
                    fetch_arrow(),
                    validate_schema=False,
                )
            )

        def fetch_record_stream() -> Any:
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT * FROM {quoted_table} ORDER BY identifier")
                return tuple(
                    arrow_into_records(
                        PostgresBenchmarkEvent,
                        cursor.fetch_record_batch(),
                        validate_schema=False,
                    )
                )

        benchmarks = (
            Benchmark(
                "records_to_arrow_batch",
                lambda: records_into_arrow_batch(
                    ROWS, record_type=PostgresBenchmarkEvent
                ),
            ),
            Benchmark("adbc_ingest_table_replace", ingest_table_replace),
            Benchmark("adbc_ingest_reader_replace", ingest_reader_replace),
            Benchmark("adbc_fetch_arrow_table", fetch_arrow),
            Benchmark("adbc_fetch_table_records", fetch_records),
            Benchmark("adbc_fetch_record_stream", fetch_record_stream),
        )
        results = [
            _measure(
                benchmark,
                minimum_seconds=arguments.min_time,
                repeat=arguments.repeat,
            )
            for benchmark in benchmarks
        ]
        try:
            rkp_version = version("rkp")
        except PackageNotFoundError:
            rkp_version = "source checkout"
        environment = {
            "python": platform.python_version(),
            "implementation": platform.python_implementation(),
            "platform": platform.platform(),
            "rkp": rkp_version,
            "pyarrow": pa.__version__,
            "adbc_driver_postgresql": adbc_driver_postgresql.__version__,
            "rows_per_operation": ROW_COUNT,
        }
        if arguments.json:
            json.dump(
                {"environment": environment, "benchmarks": results},
                sys.stdout,
                indent=2,
            )
            print()
            return 0

        print(
            f"Python {environment['python']}, RKP {environment['rkp']}, "
            f"PyArrow {environment['pyarrow']}, "
            f"ADBC PostgreSQL {environment['adbc_driver_postgresql']}"
        )
        print(f"Fixture: {ROW_COUNT} rows; connection startup excluded")
        print()
        print(f"{'benchmark':36} {'median':>12} {'best':>12} {'iterations':>12}")
        print("-" * 76)
        for result in results:
            print(
                f"{result['name']:36} "
                f"{_duration(result['median_seconds']):>12} "
                f"{_duration(result['minimum_seconds']):>12} "
                f"{result['iterations']:>12}"
            )
        return 0
    finally:
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {quoted_table}")
            connection.commit()
        finally:
            connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
