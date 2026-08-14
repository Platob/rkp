"""Microbenchmarks for streaming records through Arrow batches."""

from __future__ import annotations

import argparse
import gc
import json
import platform
import statistics
import sys
import timeit
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError, version
from typing import Any

import pyarrow as pa
from rkp import (
    Record,
    arrow_batch_into_records,
    arrow_into_records,
    record,
    records_into_arrow_batch,
    records_into_arrow_batches,
    records_into_arrow_reader,
)


@record
class BenchmarkMetric(Record):
    name: str
    value: float | None


@record(metadata={"benchmark": "arrow-batches"})
class BenchmarkEvent(Record):
    identifier: int
    label: str | None
    occurred_at: datetime
    metrics: list[BenchmarkMetric]
    dimensions: dict[str, int | None]
    payload: bytes


ROW_COUNT = 1_024
ROWS = tuple(
    BenchmarkEvent(
        index,
        None if index % 7 == 0 else f"event-{index}",
        datetime(2026, 8, 14, index % 24, index % 60, tzinfo=UTC),
        [BenchmarkMetric("value", float(index)), BenchmarkMetric("empty", None)],
        {"shard": index % 16, "optional": None},
        index.to_bytes(4, "little"),
    )
    for index in range(ROW_COUNT)
)
BATCH = records_into_arrow_batch(ROWS, record_type=BenchmarkEvent)
BATCHES = tuple(
    records_into_arrow_batches(ROWS, record_type=BenchmarkEvent, batch_size=128)
)
TABLE = pa.Table.from_batches(BATCHES)


def _records_to_batch() -> Any:
    return records_into_arrow_batch(ROWS, record_type=BenchmarkEvent)


def _records_to_batches_128() -> Any:
    return tuple(
        records_into_arrow_batches(ROWS, record_type=BenchmarkEvent, batch_size=128)
    )


def _records_reader_read_all_128() -> Any:
    reader = records_into_arrow_reader(
        ROWS,
        record_type=BenchmarkEvent,
        batch_size=128,
    )
    return reader.read_all()


def _batch_to_records_validated() -> Any:
    return tuple(arrow_batch_into_records(BenchmarkEvent, BATCH))


def _batch_to_records_unvalidated() -> Any:
    return tuple(arrow_batch_into_records(BenchmarkEvent, BATCH, validate_schema=False))


def _table_to_records() -> Any:
    return tuple(arrow_into_records(BenchmarkEvent, TABLE))


def _batch_pylist_reference() -> Any:
    return BATCH.to_pylist()


@dataclass(frozen=True, slots=True)
class Benchmark:
    name: str
    operation: Callable[[], Any]


BENCHMARKS = (
    Benchmark("records_to_batch", _records_to_batch),
    Benchmark("records_to_batches_128", _records_to_batches_128),
    Benchmark("records_reader_read_all_128", _records_reader_read_all_128),
    Benchmark("batch_to_records_validated", _batch_to_records_validated),
    Benchmark("batch_to_records_unvalidated", _batch_to_records_unvalidated),
    Benchmark("table_to_records", _table_to_records),
    Benchmark("batch_pylist_reference", _batch_pylist_reference),
)


def _measure(
    benchmark: Benchmark, *, minimum_seconds: float, repeat: int
) -> dict[str, Any]:
    benchmark.operation()
    number = 1
    while number < 1_048_576:
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


def _environment() -> dict[str, Any]:
    try:
        rkp_version = version("rkp")
    except PackageNotFoundError:
        rkp_version = "source checkout"
    return {
        "python": platform.python_version(),
        "implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "rkp": rkp_version,
        "pyarrow": pa.__version__,
        "rows_per_operation": ROW_COUNT,
        "columns": len(BATCH.schema),
        "stream_batches": len(BATCHES),
    }


def _duration(seconds: float) -> str:
    if seconds < 1e-3:
        return f"{seconds * 1e6:.1f} us"
    return f"{seconds * 1e3:.2f} ms"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--min-time", type=float, default=0.2)
    parser.add_argument("--repeat", type=int, default=7)
    parser.add_argument("--json", action="store_true")
    arguments = parser.parse_args(argv)
    if arguments.min_time <= 0:
        parser.error("--min-time must be greater than zero")
    if arguments.repeat < 1:
        parser.error("--repeat must be at least one")

    results = [
        _measure(
            benchmark,
            minimum_seconds=arguments.min_time,
            repeat=arguments.repeat,
        )
        for benchmark in BENCHMARKS
    ]
    environment = _environment()
    if arguments.json:
        json.dump(
            {"environment": environment, "benchmarks": results}, sys.stdout, indent=2
        )
        print()
        return 0

    print(
        f"Python {environment['python']}, RKP {environment['rkp']}, "
        f"PyArrow {environment['pyarrow']}"
    )
    print(
        f"Fixture: {environment['rows_per_operation']} rows, "
        f"{environment['columns']} columns, {environment['stream_batches']} batches"
    )
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


if __name__ == "__main__":
    raise SystemExit(main())
