"""Microbenchmarks for RKP's Arrow and Iceberg record interoperability.

The runner calibrates every case independently, prints median per-operation
times, and can emit JSON without requiring pytest-benchmark or pyperf.
"""

from __future__ import annotations

import argparse
import gc
import json
import logging
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
import pyiceberg
import rkp
from pyiceberg.io.pyarrow import pyarrow_to_schema, schema_to_pyarrow
from rkp import Record, field, record
from rkp.records import iceberg as iceberg_adapter

arrow_into_iceberg_schema = iceberg_adapter.arrow_into_iceberg_schema


@record
class GeoPoint(Record):
    latitude: float
    longitude: float


@record
class Address(Record):
    street: str
    city: str
    postcode: str | None
    location: GeoPoint


@record
class LineItem(Record):
    sku: str
    quantity: int
    price: float
    attributes: dict[str, str | None]


@record(alias="benchmark_event")
class BenchmarkEvent(Record):
    event_id: int = field(primary_key=True)
    occurred_at: datetime
    address: Address
    items: list[LineItem]
    labels: set[str]
    dimensions: tuple[int, str | None, float]
    properties: dict[str, int | None]


@record
class RuntimeMetric(Record):
    name: str
    value: float | None


@record(table_name="iceberg_runtime_events")
class RuntimeEvent(Record):
    event_id: int
    occurred_at: datetime
    metrics: list[RuntimeMetric]
    attributes: dict[str, str | None]


def _nested_arrow_schema(width: int = 32) -> pa.Schema:
    """Build a wide field forest with struct, list, and map descendants."""

    roots: list[pa.Field] = []
    for index in range(width):
        detail = pa.struct(
            [
                pa.field("counter", pa.int64(), nullable=False),
                pa.field(
                    "labels",
                    pa.list_(pa.field("element", pa.string(), nullable=True)),
                    nullable=False,
                ),
                pa.field(
                    "attributes",
                    pa.map_(
                        pa.field("key", pa.string(), nullable=False),
                        pa.field("value", pa.int64(), nullable=True),
                    ),
                    nullable=False,
                ),
            ]
        )
        roots.append(pa.field(f"event_{index}", detail, nullable=index % 2 == 0))
    return pa.schema(roots)


# Construct fixtures outside timed regions. The generated schema contains 224
# nested fields at the default width, enough to expose traversal overhead while
# keeping the benchmark convenient for local iteration.
NESTED_ARROW_SCHEMA = _nested_arrow_schema()
NANOSECOND_ARROW_SCHEMA = pa.schema(
    [
        pa.field("local_time", pa.timestamp("ns"), nullable=False),
        pa.field("utc_time", pa.timestamp("ns", tz="UTC"), nullable=False),
    ]
)
# Adaptive v1/v2 conversion logs one precision warning per nano field. Logging
# is not conversion work, so keep it out of benchmark measurements.
logging.getLogger("pyiceberg.io.pyarrow").setLevel(logging.ERROR)
NESTED_ICEBERG_SCHEMA = arrow_into_iceberg_schema(NESTED_ARROW_SCHEMA)
# Keep the physical Arrow types identical between the allocation and explicit
# identity cases. An Iceberg round trip canonicalizes strings and lists to
# large variants, which would otherwise confound this comparison. This private
# adapter access is benchmark-only, like the cache resets below.
IDENTIFIED_ARROW_SCHEMA = pa.schema(
    iceberg_adapter._FieldSeqAllocator(tuple(NESTED_ARROW_SCHEMA)).apply()[0]
)
REVERSE_FIELD = NESTED_ICEBERG_SCHEMA.fields[0]
RUNTIME_ROW_COUNT = 256
RUNTIME_ROWS = tuple(
    RuntimeEvent(
        index,
        datetime(2026, 8, 14, index % 24, index % 60, tzinfo=UTC),
        [RuntimeMetric("value", float(index)), RuntimeMetric("empty", None)],
        {"partition": str(index % 16), "optional": None},
    )
    for index in range(RUNTIME_ROW_COUNT)
)
RUNTIME_CANONICAL_BATCH = rkp.records_into_arrow_batch(
    RUNTIME_ROWS, record_type=RuntimeEvent
)
RUNTIME_ICEBERG_ARROW_SCHEMA = rkp.iceberg_into_arrow_schema(
    RuntimeEvent.into_iceberg_schema(),
    metadata=RuntimeEvent.into_arrow_schema().metadata,
)
RUNTIME_ICEBERG_BATCH = rkp.records_into_arrow_batch(
    RUNTIME_ROWS,
    schema=RUNTIME_ICEBERG_ARROW_SCHEMA,
)


def _clear_record_field_cache() -> None:
    iceberg_adapter._cached_record_iceberg_conversion.cache_clear()


def _clear_record_schema_cache() -> None:
    iceberg_adapter._cached_record_into_iceberg_schema.cache_clear()
    iceberg_adapter._cached_record_iceberg_conversion.cache_clear()


def _record_field_cold() -> Any:
    _clear_record_field_cache()
    return BenchmarkEvent.into_iceberg_field()


def _record_schema_cold() -> Any:
    _clear_record_schema_cache()
    return BenchmarkEvent.into_iceberg_schema()


def _record_field_cached() -> Any:
    return BenchmarkEvent.into_iceberg_field()


def _record_schema_cached() -> Any:
    return BenchmarkEvent.into_iceberg_schema()


def _nested_allocate_ids() -> Any:
    return arrow_into_iceberg_schema(NESTED_ARROW_SCHEMA)


def _nested_explicit_ids() -> Any:
    return arrow_into_iceberg_schema(IDENTIFIED_ARROW_SCHEMA)


def _nested_pyiceberg_bulk() -> Any:
    return pyarrow_to_schema(IDENTIFIED_ARROW_SCHEMA, format_version=2)


def _nanoseconds_v2_adaptive() -> Any:
    return arrow_into_iceberg_schema(NANOSECOND_ARROW_SCHEMA, format_version=2)


def _nanoseconds_v3_adaptive() -> Any:
    return arrow_into_iceberg_schema(NANOSECOND_ARROW_SCHEMA, format_version=3)


def _nanoseconds_v3_forced_downcast() -> Any:
    return arrow_into_iceberg_schema(
        NANOSECOND_ARROW_SCHEMA,
        format_version=3,
        downcast_ns_timestamp_to_us=True,
    )


def _reverse_schema() -> Any:
    return rkp.iceberg_into_arrow_schema(NESTED_ICEBERG_SCHEMA)


def _reverse_schema_pyiceberg_bulk() -> Any:
    return schema_to_pyarrow(NESTED_ICEBERG_SCHEMA, include_field_ids=True)


def _reverse_field() -> Any:
    return rkp.iceberg_into_arrow_field(REVERSE_FIELD)


def _runtime_records_to_canonical_batch() -> Any:
    return rkp.records_into_arrow_batch(RUNTIME_ROWS, record_type=RuntimeEvent)


def _runtime_records_to_iceberg_batch() -> Any:
    return rkp.records_into_arrow_batch(
        RUNTIME_ROWS,
        schema=RUNTIME_ICEBERG_ARROW_SCHEMA,
    )


def _runtime_canonical_batch_to_records() -> Any:
    return tuple(rkp.arrow_batch_into_records(RuntimeEvent, RUNTIME_CANONICAL_BATCH))


def _runtime_iceberg_batch_to_records() -> Any:
    return tuple(
        rkp.arrow_batch_into_records(
            RuntimeEvent,
            RUNTIME_ICEBERG_BATCH,
            validate_schema=False,
        )
    )


@dataclass(frozen=True, slots=True)
class Benchmark:
    name: str
    operation: Callable[[], Any]


BENCHMARKS = (
    Benchmark("record_field_cold", _record_field_cold),
    Benchmark("record_field_cached", _record_field_cached),
    Benchmark("record_schema_cold", _record_schema_cold),
    Benchmark("record_schema_cached", _record_schema_cached),
    Benchmark("nested_allocate_ids", _nested_allocate_ids),
    Benchmark("nested_explicit_ids", _nested_explicit_ids),
    Benchmark("nested_pyiceberg_bulk", _nested_pyiceberg_bulk),
    Benchmark("nanoseconds_v2_adaptive", _nanoseconds_v2_adaptive),
    Benchmark("nanoseconds_v3_adaptive", _nanoseconds_v3_adaptive),
    Benchmark("nanoseconds_v3_forced_downcast", _nanoseconds_v3_forced_downcast),
    Benchmark("reverse_schema", _reverse_schema),
    Benchmark("reverse_schema_pyiceberg_bulk", _reverse_schema_pyiceberg_bulk),
    Benchmark("reverse_field", _reverse_field),
    Benchmark(
        "runtime_records_to_canonical_batch", _runtime_records_to_canonical_batch
    ),
    Benchmark("runtime_records_to_iceberg_batch", _runtime_records_to_iceberg_batch),
    Benchmark(
        "runtime_canonical_batch_to_records", _runtime_canonical_batch_to_records
    ),
    Benchmark("runtime_iceberg_batch_to_records", _runtime_iceberg_batch_to_records),
)


def _calibrate(operation: Callable[[], Any], minimum_seconds: float) -> int:
    number = 1
    while number < 1_048_576:
        elapsed = timeit.timeit(operation, number=number)
        if elapsed >= minimum_seconds:
            return number
        number *= 2
    return number


def _measure(
    benchmark: Benchmark,
    *,
    minimum_seconds: float,
    repeat: int,
) -> dict[str, Any]:
    benchmark.operation()
    number = _calibrate(benchmark.operation, minimum_seconds)
    gc.collect()
    samples = timeit.repeat(benchmark.operation, number=number, repeat=repeat)
    seconds_per_operation = [sample / number for sample in samples]
    median = statistics.median(seconds_per_operation)
    return {
        "name": benchmark.name,
        "iterations": number,
        "median_seconds": median,
        "minimum_seconds": min(seconds_per_operation),
        "maximum_seconds": max(seconds_per_operation),
        "samples_seconds": seconds_per_operation,
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
        "pyiceberg": pyiceberg.__version__,
        "nested_top_level_fields": len(NESTED_ARROW_SCHEMA),
        "nested_total_fields": len(NESTED_ICEBERG_SCHEMA.column_names),
        "runtime_rows": RUNTIME_ROW_COUNT,
    }


def _format_duration(seconds: float) -> str:
    if seconds < 1e-6:
        return f"{seconds * 1e9:.1f} ns"
    if seconds < 1e-3:
        return f"{seconds * 1e6:.1f} us"
    return f"{seconds * 1e3:.2f} ms"


def _print_report(environment: dict[str, Any], results: list[dict[str, Any]]) -> None:
    versions = (
        f"Python {environment['python']}, RKP {environment['rkp']}, "
        f"PyArrow {environment['pyarrow']}, PyIceberg {environment['pyiceberg']}"
    )
    print(versions)
    print(
        f"Nested fixture: {environment['nested_top_level_fields']} roots, "
        f"{environment['nested_total_fields']} total fields"
    )
    print()
    print(f"{'benchmark':38} {'median':>12} {'best':>12} {'iterations':>12}")
    print("-" * 78)
    for result in results:
        print(
            f"{result['name']:38} "
            f"{_format_duration(result['median_seconds']):>12} "
            f"{_format_duration(result['minimum_seconds']):>12} "
            f"{result['iterations']:>12}"
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--min-time",
        type=float,
        default=0.2,
        help="minimum calibration time per case in seconds (default: 0.2)",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=7,
        help="sample count after calibration (default: 7)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="write machine-readable JSON instead of a table",
    )
    arguments = parser.parse_args(argv)
    if arguments.min_time <= 0:
        parser.error("--min-time must be greater than zero")
    if arguments.repeat < 1:
        parser.error("--repeat must be at least one")

    # Prime the public caches once so cached cases measure steady state even if
    # their execution order is changed later.
    BenchmarkEvent.into_iceberg_field()
    BenchmarkEvent.into_iceberg_schema()
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
            {"environment": environment, "benchmarks": results},
            sys.stdout,
            indent=2,
        )
        print()
    else:
        _print_report(environment, results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
