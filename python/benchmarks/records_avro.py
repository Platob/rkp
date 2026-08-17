"""Microbenchmarks for RKP's Avro implementation.

The format itself lives in the Rust crate ``rkp-avro`` and reaches Python
through the ``rkp._avro`` extension module, so these cases measure that core
plus the conversion of Python values across the boundary.  They cover schema
handling (parsing, canonical form, fingerprints), compiled binary encoding and
decoding, the JSON encoding, object container files with the stdlib codecs, the
record round trip, and the random-access container operations the Rust core
exists for: reading one record by index from a cold and from a warm container,
building a container's block index, replacing one record, and appending one.
RKP's JSON codec is measured on the same rows as a reference point for text
encoding cost.
"""

from __future__ import annotations

import argparse
import gc
import itertools
import json
import platform
import random
import statistics
import sys
import timeit
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from importlib.metadata import PackageNotFoundError, version
from typing import Any

import rkp.json as rkp_json
from rkp import Record, avro, field, record
from rkp.records import avro as avro_records
from rkp.records.arrow import record_into_native_mapping


@record
class BenchmarkMetric(Record):
    name: str
    value: float | None


@record(table_name="avro_events")
class BenchmarkEvent(Record):
    identifier: int = field(seq=1, primary_key=True)
    label: str | None
    occurred_at: datetime
    observed_on: date
    amount: Decimal
    metrics: list[BenchmarkMetric]
    dimensions: dict[str, int | None]
    payload: bytes


ROW_COUNT = 1_024
SYNC_MARKER = b"rkp-benchmark--16"[:16]
SEED = 20_260_817


def _event(index: int) -> BenchmarkEvent:
    return BenchmarkEvent(
        index,
        None if index % 7 == 0 else f"event-{index}",
        datetime(2026, 8, 14, index % 24, index % 60, tzinfo=UTC),
        date(2026, 8, (index % 28) + 1),
        Decimal(index) / 100,
        [BenchmarkMetric("value", float(index)), BenchmarkMetric("empty", None)],
        {"shard": index % 16, "optional": None},
        index.to_bytes(4, "little"),
    )


ROWS = tuple(_event(index) for index in range(ROW_COUNT))

SCHEMA = avro_records.into_avro_schema(BenchmarkEvent)
ICEBERG_SCHEMA = avro_records.into_avro_schema(BenchmarkEvent, flavor="iceberg")
SCHEMA_JSON = avro.dumps_schema(SCHEMA)
NATIVE_ROWS = tuple(record_into_native_mapping(row) for row in ROWS)
ENCODE = avro.compile_encoder(SCHEMA)
DECODE = avro.compile_decoder(SCHEMA)
BINARY_ROWS = tuple(ENCODE(row) for row in NATIVE_ROWS)
JSON_ROWS = tuple(avro.dumps(SCHEMA, row) for row in NATIVE_ROWS)

# The row written by the random-write case: one whole record of comparable
# size, so replacing a record neither grows nor shrinks its block materially.
UPDATE_ROW = record_into_native_mapping(_event(7_777))
APPEND_ROW = record_into_native_mapping(_event(9_999))

CONTAINER_NULL = avro_records.records_into_avro(
    ROWS, record_type=BenchmarkEvent, sync_marker=SYNC_MARKER
)
CONTAINER_DEFLATE = avro_records.records_into_avro(
    ROWS,
    record_type=BenchmarkEvent,
    codec="deflate",
    sync_marker=SYNC_MARKER,
)


def _random_access_container() -> bytes:
    """Write the same rows framed for indexed reads rather than bulk writes."""

    container = avro.Avro.create(
        SCHEMA,
        codec="null",
        sync_marker=SYNC_MARKER,
        sync_interval=avro.RANDOM_SYNC_INTERVAL,
    )
    container.extend(NATIVE_ROWS)
    return container.into_bytes()


CONTAINER_RANDOM = _random_access_container()

# One deterministic scan order, so cold and warm reads touch the same records
# in the same sequence and the numbers stay comparable across runs.
INDEX_ORDER = tuple(random.Random(SEED).sample(range(ROW_COUNT), ROW_COUNT))
READ_COLD_INDICES = itertools.cycle(INDEX_ORDER)
READ_WARM_INDICES = itertools.cycle(INDEX_ORDER)
WRITE_INDICES = itertools.cycle(INDEX_ORDER)

WARM_CONTAINER = avro.read_container(CONTAINER_RANDOM)
RANDOM_BLOCKS = len(WARM_CONTAINER.blocks())
for _block in WARM_CONTAINER.blocks():
    # Decoding one record per block leaves every payload in the block cache.
    WARM_CONTAINER[_block.first]


def _schema_parse_cold() -> Any:
    return avro.parse_schema(json.loads(SCHEMA_JSON))


def _schema_from_record_cached() -> Any:
    return avro_records.into_avro_schema(BenchmarkEvent)


def _schema_from_record_cold() -> Any:
    avro_records.record_into_avro_schema.cache_clear()
    return avro_records.into_avro_schema(BenchmarkEvent)


def _schema_canonical_form() -> Any:
    return avro.canonical_form(avro.parse_schema(json.loads(SCHEMA_JSON)))


def _schema_fingerprint() -> Any:
    return avro.fingerprint(avro.parse_schema(json.loads(SCHEMA_JSON)))


def _binary_encode_rows() -> Any:
    out = bytearray()
    for row in NATIVE_ROWS:
        ENCODE(row, out)
    return out


def _binary_decode_rows() -> Any:
    return [DECODE(payload) for payload in BINARY_ROWS]


def _json_encode_rows() -> Any:
    return [avro.dumps(SCHEMA, row) for row in NATIVE_ROWS]


def _json_decode_rows() -> Any:
    return [avro.loads(SCHEMA, payload) for payload in JSON_ROWS]


def _rkp_json_encode_rows() -> Any:
    return [rkp_json.dumps(row) for row in ROWS]


def _container_write_null() -> Any:
    return avro_records.records_into_avro(ROWS, record_type=BenchmarkEvent)


def _container_write_deflate() -> Any:
    return avro_records.records_into_avro(
        ROWS, record_type=BenchmarkEvent, codec="deflate"
    )


def _container_read_null() -> Any:
    return list(avro.read_container(CONTAINER_NULL))


def _container_read_deflate() -> Any:
    return list(avro.read_container(CONTAINER_DEFLATE))


def _container_open_index() -> Any:
    """Open a container image and build the block index behind ``len()``."""

    return len(avro.read_container(CONTAINER_RANDOM))


def _container_read_one_cold() -> Any:
    """Open a container and decode one record, with nothing yet cached."""

    container = avro.read_container(CONTAINER_RANDOM)
    return container[next(READ_COLD_INDICES)]


def _container_read_one_warm() -> Any:
    """Decode one record from a container whose blocks are already cached."""

    return WARM_CONTAINER[next(READ_WARM_INDICES)]


def _container_write_one() -> Any:
    """Replace one record by index and materialize the container image."""

    container = avro.Avro(CONTAINER_RANDOM, mode="r+")
    container[next(WRITE_INDICES)] = UPDATE_ROW
    return container.into_bytes()


def _container_append_one() -> Any:
    """Append one record to an existing container and materialize its image."""

    container = avro.Avro(CONTAINER_RANDOM, mode="a")
    container.append(APPEND_ROW)
    return container.into_bytes()


def _records_into_avro() -> Any:
    return avro_records.records_into_avro(ROWS, record_type=BenchmarkEvent)


def _avro_into_records() -> Any:
    return tuple(avro_records.avro_into_records(BenchmarkEvent, CONTAINER_NULL))


def _avro_into_arrow_schema() -> Any:
    return avro_records.avro_into_arrow_schema(SCHEMA)


def _iceberg_flavor_schema() -> Any:
    avro_records.record_into_avro_schema.cache_clear()
    return avro_records.into_avro_schema(BenchmarkEvent, flavor="iceberg")


@dataclass(frozen=True, slots=True)
class Benchmark:
    name: str
    operation: Callable[[], Any]


BENCHMARKS = (
    Benchmark("schema_parse_cold", _schema_parse_cold),
    Benchmark("schema_from_record_cold", _schema_from_record_cold),
    Benchmark("schema_from_record_cached", _schema_from_record_cached),
    Benchmark("schema_iceberg_flavor_cold", _iceberg_flavor_schema),
    Benchmark("schema_canonical_form", _schema_canonical_form),
    Benchmark("schema_fingerprint", _schema_fingerprint),
    Benchmark("schema_into_arrow", _avro_into_arrow_schema),
    Benchmark("binary_encode_rows", _binary_encode_rows),
    Benchmark("binary_decode_rows", _binary_decode_rows),
    Benchmark("json_encode_rows", _json_encode_rows),
    Benchmark("json_decode_rows", _json_decode_rows),
    Benchmark("rkp_json_encode_rows", _rkp_json_encode_rows),
    Benchmark("container_write_null", _container_write_null),
    Benchmark("container_write_deflate", _container_write_deflate),
    Benchmark("container_read_null", _container_read_null),
    Benchmark("container_read_deflate", _container_read_deflate),
    Benchmark("container_open_index", _container_open_index),
    Benchmark("container_read_one_cold", _container_read_one_cold),
    Benchmark("container_read_one_warm", _container_read_one_warm),
    Benchmark("container_write_one", _container_write_one),
    Benchmark("container_append_one", _container_append_one),
    Benchmark("records_into_avro", _records_into_avro),
    Benchmark("avro_into_records", _avro_into_records),
)


def _measure(
    benchmark: Benchmark,
    *,
    minimum_seconds: float,
    repeat: int,
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
        "rkp_avro_core": avro.core_version(),
        "rows_per_operation": ROW_COUNT,
        "record_fields": len(SCHEMA.fields),
        "binary_bytes": sum(len(payload) for payload in BINARY_ROWS),
        "container_null_bytes": len(CONTAINER_NULL),
        "container_deflate_bytes": len(CONTAINER_DEFLATE),
        "container_random_bytes": len(CONTAINER_RANDOM),
        "container_random_blocks": RANDOM_BLOCKS,
        "container_random_sync_interval": avro.RANDOM_SYNC_INTERVAL,
        "schema_fingerprint": f"{avro.fingerprint(SCHEMA):016x}",
    }


def _duration(seconds: float) -> str:
    if seconds < 1e-6:
        return f"{seconds * 1e9:.1f} ns"
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
        f"rkp-avro core {environment['rkp_avro_core']}"
    )
    print(
        f"Fixture: {environment['rows_per_operation']} rows, "
        f"{environment['record_fields']} fields, "
        f"{environment['binary_bytes']} binary bytes, "
        f"schema {environment['schema_fingerprint']}"
    )
    print(
        f"Container sizes: null {environment['container_null_bytes']} B, "
        f"deflate {environment['container_deflate_bytes']} B, "
        f"random-access {environment['container_random_bytes']} B in "
        f"{environment['container_random_blocks']} blocks of "
        f"{environment['container_random_sync_interval']} B"
    )
    print()
    print(f"{'benchmark':32} {'median':>12} {'best':>12} {'iterations':>12}")
    print("-" * 72)
    for result in results:
        print(
            f"{result['name']:32} "
            f"{_duration(result['median_seconds']):>12} "
            f"{_duration(result['minimum_seconds']):>12} "
            f"{result['iterations']:>12}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
