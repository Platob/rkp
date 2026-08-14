"""Microbenchmarks for RKP's pure Arrow and AWS Glue schema adapter.

The runner calibrates every case independently, prints median per-operation
times, and can emit JSON without requiring pytest-benchmark or pyperf. It does
not create a boto3 client or make network calls.
"""

from __future__ import annotations

import argparse
import copy
import gc
import json
import platform
import statistics
import sys
import timeit
from collections.abc import Callable
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, version
from typing import Any

import pyarrow as pa
from rkp import (
    Record,
    catalog_name,
    field,
    into_arrow_schema,
    record,
    schema_metadata,
    schema_name,
    table_name,
)
from rkp.records import awsglue as glue_adapter


@record(
    metadata={"owner": "benchmarks"},
    catalog_name="benchmark_catalog",
    schema_name="benchmark_schema",
    table_name="benchmark_events",
)
class BenchmarkRecord(Record):
    tenant_id: str = field(partition_key=True)
    event_id: int


def _nested_arrow_schema(width: int = 16) -> pa.Schema:
    """Build a representative event schema with nested collection types."""

    roots = [
        pa.field(
            "tenant_id",
            pa.string(),
            nullable=False,
            metadata={b"partition_key": b"true", b"doc": b"Tenant partition"},
        ),
        pa.field(
            "event_id",
            pa.int64(),
            nullable=False,
            metadata={b"primary_key": b"true", b"PARQUET:field_id": b"1"},
        ),
        pa.field("occurred_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("event_day", pa.date32(), nullable=False),
    ]
    for index in range(width):
        measurement = pa.struct(
            [
                pa.field("name", pa.string(), nullable=False),
                pa.field("value", pa.float64(), nullable=True),
                pa.field("unit", pa.string(), nullable=True),
            ]
        )
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
                        pa.field("value", pa.string(), nullable=True),
                    ),
                    nullable=False,
                ),
                pa.field(
                    "measurements",
                    pa.list_(
                        pa.field("element", measurement, nullable=False),
                    ),
                    nullable=False,
                ),
            ]
        )
        roots.append(
            pa.field(
                f"payload_{index}",
                detail,
                nullable=index % 2 == 0,
                metadata={b"doc": f"Payload group {index}".encode()},
            )
        )
    return pa.schema(roots, metadata={b"benchmark": b"awsglue"})


def _count_fields(schema: pa.Schema) -> int:
    def count_type(value: pa.DataType) -> int:
        if pa.types.is_struct(value):
            return sum(1 + count_type(field.type) for field in value)
        if (
            pa.types.is_list(value)
            or pa.types.is_large_list(value)
            or pa.types.is_fixed_size_list(value)
        ):
            return 1 + count_type(value.value_type)
        if pa.types.is_map(value):
            return 2 + count_type(value.key_type) + count_type(value.item_type)
        if pa.types.is_dictionary(value):
            return count_type(value.value_type)
        return 0

    return sum(1 + count_type(field.type) for field in schema)


# Construct reusable inputs outside timed regions. The embedded and fallback
# reverse cases therefore isolate conversion and validation rather than fixture
# assembly. The fallback table deliberately lacks RKP's lossless Arrow payload.
NESTED_ARROW_SCHEMA = _nested_arrow_schema()
EMBEDDED_TABLE_INPUT = glue_adapter.into_glue_table_input(
    NESTED_ARROW_SCHEMA,
    name="benchmark_events",
    location="s3://rkp-benchmarks/events/",
)
FALLBACK_TABLE_INPUT = copy.deepcopy(EMBEDDED_TABLE_INPUT)
FALLBACK_TABLE_INPUT["Parameters"].pop("rkp.arrow_schema")
BENCHMARK_RECORD = BenchmarkRecord("tenant-1", 42)


def _arrow_columns() -> Any:
    return glue_adapter.arrow_into_glue_columns(NESTED_ARROW_SCHEMA)


def _cached_record_arrow_schema() -> Any:
    return into_arrow_schema(BenchmarkRecord)


def _portable_metadata_accessors() -> Any:
    return (
        catalog_name(BenchmarkRecord),
        schema_name(BenchmarkRecord),
        table_name(BenchmarkRecord),
        schema_metadata(BenchmarkRecord),
    )


def _table_input() -> Any:
    return glue_adapter.into_glue_table_input(
        NESTED_ARROW_SCHEMA,
        name="benchmark_events",
        location="s3://rkp-benchmarks/events/",
    )


def _ddl() -> Any:
    return glue_adapter.into_glue_ddl(
        NESTED_ARROW_SCHEMA,
        name="benchmark_events",
        database="analytics",
        location="s3://rkp-benchmarks/events/",
    )


def _embedded_reverse_validation() -> Any:
    return glue_adapter.glue_into_arrow_schema(EMBEDDED_TABLE_INPUT)


def _fallback_reverse_parsing() -> Any:
    return glue_adapter.glue_into_arrow_schema(FALLBACK_TABLE_INPUT)


def _partition_values() -> Any:
    return glue_adapter.into_glue_partition_values(BENCHMARK_RECORD)


def _partition_projection() -> Any:
    return glue_adapter.into_glue_partition_projection(
        BenchmarkRecord,
        {"tenant_id": {"type": "enum", "values": ("tenant-1", "tenant-2")}},
    )


@dataclass(frozen=True, slots=True)
class Benchmark:
    name: str
    operation: Callable[[], Any]


BENCHMARKS = (
    Benchmark("cached_record_arrow_schema", _cached_record_arrow_schema),
    Benchmark("portable_metadata_accessors", _portable_metadata_accessors),
    Benchmark("arrow_columns", _arrow_columns),
    Benchmark("table_input", _table_input),
    Benchmark("ddl", _ddl),
    Benchmark("embedded_reverse_validation", _embedded_reverse_validation),
    Benchmark("fallback_reverse_parsing", _fallback_reverse_parsing),
    Benchmark("partition_values", _partition_values),
    Benchmark("partition_projection", _partition_projection),
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
        "nested_top_level_fields": len(NESTED_ARROW_SCHEMA),
        "nested_total_fields": _count_fields(NESTED_ARROW_SCHEMA),
        "embedded_parameter_bytes": len(
            EMBEDDED_TABLE_INPUT["Parameters"]["rkp.arrow_schema"]
        ),
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
        f"PyArrow {environment['pyarrow']}"
    )
    print(versions)
    print(
        f"Nested fixture: {environment['nested_top_level_fields']} roots, "
        f"{environment['nested_total_fields']} total fields, "
        f"{environment['embedded_parameter_bytes']} embedded bytes"
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
