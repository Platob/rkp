"""Benchmarks for direct Arrow and record interop with a local Spark 4 session."""

from __future__ import annotations

import argparse
import gc
import json
import os
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
import pyspark
from pyspark.sql import SparkSession
from rkp import (
    Record,
    arrow_into_spark_dataframe,
    into_spark_schema,
    record,
    records_into_arrow_batch,
    records_into_spark_dataframe,
    spark_dataframe_into_arrow,
    spark_dataframe_into_records,
    spark_into_arrow_schema,
)


@record
class SparkBenchmarkEvent(Record):
    identifier: int
    label: str | None
    active: bool
    occurred_at: datetime
    values: list[float]
    dimensions: dict[str, int | None]


ROW_COUNT = 1_024
ROWS = tuple(
    SparkBenchmarkEvent(
        index,
        None if index % 7 == 0 else f"event-{index}",
        index % 2 == 0,
        datetime(2026, 8, 14, index % 24, index % 60, tzinfo=UTC),
        [float(index), float(index + 1)],
        {"partition": index % 16, "optional": None},
    )
    for index in range(ROW_COUNT)
)
ARROW_BATCH = records_into_arrow_batch(ROWS, record_type=SparkBenchmarkEvent)
ARROW_TABLE = pa.Table.from_batches([ARROW_BATCH])


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

    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("rkp-arrow-benchmark")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "4096")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        dataframe = arrow_into_spark_dataframe(ARROW_TABLE, spark=spark)
        # Materialize the reusable frame before timing reverse conversions.
        dataframe.cache().count()
        spark_schema = into_spark_schema(SparkBenchmarkEvent)

        benchmarks = (
            Benchmark(
                "record_to_spark_schema",
                lambda: into_spark_schema(SparkBenchmarkEvent),
            ),
            Benchmark(
                "spark_to_arrow_schema",
                lambda: spark_into_arrow_schema(spark_schema),
            ),
            Benchmark(
                "arrow_table_to_dataframe",
                lambda: arrow_into_spark_dataframe(ARROW_TABLE, spark=spark),
            ),
            Benchmark(
                "records_to_dataframe",
                lambda: records_into_spark_dataframe(
                    ROWS,
                    record_type=SparkBenchmarkEvent,
                    spark=spark,
                    batch_size=4096,
                ),
            ),
            Benchmark(
                "dataframe_to_arrow_table",
                lambda: spark_dataframe_into_arrow(dataframe),
            ),
            Benchmark(
                "dataframe_to_records",
                lambda: tuple(
                    spark_dataframe_into_records(dataframe, SparkBenchmarkEvent)
                ),
            ),
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
            "pyspark": pyspark.__version__,
            "spark": spark.version,
            "spark_master": spark.sparkContext.master,
            "arrow_enabled": spark.conf.get(
                "spark.sql.execution.arrow.pyspark.enabled"
            ),
            "arrow_max_records_per_batch": spark.conf.get(
                "spark.sql.execution.arrow.maxRecordsPerBatch"
            ),
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
            f"PyArrow {environment['pyarrow']}, Spark {environment['spark']}"
        )
        print(f"Fixture: {ROW_COUNT} rows; local session and cache warm-up excluded")
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
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
