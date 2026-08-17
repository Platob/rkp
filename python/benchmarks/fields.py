"""Allocation-sensitive baselines for Python datatype and field boundaries.

Run after ``maturin develop`` with::

    python benchmarks/fields.py --iterations 10000

The wide metadata cases guard the bulk accumulator against accidental
quadratic duplicate handling. The inference cases exercise native nested
builders without projecting through PyArrow.
"""

from __future__ import annotations

import argparse
import gc
import statistics
import timeit
from collections.abc import Callable
from typing import Annotated

from yggdryl import DataType, Field, MediaType, MimeType, fields

WIDE_METADATA = tuple(
    (f"key_{index:04d}", str(index)) for index in range(1_024)
)
WIDE_UPDATE = (*WIDE_METADATA, ("key_0000", "updated"))
WIDE_DIFF_LEFT = Field(
    "root",
    DataType.from_fields(
        Field(f"left_{index:04d}", "int32") for index in range(1_024)
    ),
)
WIDE_DIFF_RIGHT = Field(
    "root",
    DataType.from_fields(
        Field(f"right_{index:04d}", "int32") for index in range(1_024)
    ),
)
KNOWN_MIME = "application/json"
CUSTOM_MIME = "application/vnd.benchmark+json"
COMPOUND_MEDIA = "text/csv;encodings=application/gzip,application/zstd"
CONTENT_TYPE = "text/csv; charset=utf-8"
CONTENT_ENCODING = "gzip, zstd"
DEFAULT_STRUCT = DataType.from_fields(
    (
        Field("count", "uint32", nullable=False),
        Field("label", "utf8", nullable=False),
    )
)
DEFAULT_FIELD = Field("optional", "decimal128(18,4)")
VARIANT_MEMBERS = (
    Field("integer", "int64", nullable=False),
    Field("text", "utf8", nullable=False),
)


def _construct_wide_metadata() -> Field:
    return Field("payload", "utf8", metadata=WIDE_UPDATE)


def _update_wide_metadata() -> Field:
    field = Field("payload", "utf8")
    field.update(WIDE_UPDATE)
    return field


def _infer_nested_datatype() -> DataType:
    return DataType.from_pyhint(
        dict[str, list[Annotated[int | None, {"unit": "ticks"}]]]
    )


def _build_nested_typed_field() -> Field:
    return fields.map_of("counts", str, int, nullable=False)


def _build_generic_time_datatype() -> DataType:
    return DataType.time("us")


def _build_generic_time_field() -> Field:
    return fields.time("at", "us", nullable=False)


def _build_variant_datatype() -> DataType:
    return DataType.variant(VARIANT_MEMBERS)


def _build_variant_field() -> Field:
    return fields.variant("payload", VARIANT_MEMBERS, nullable=False)


def _infer_variant_datatype() -> DataType:
    return DataType.from_pyhint(int | str)


def _create_wide_difference_iterator() -> object:
    return WIDE_DIFF_LEFT.show_diffs(WIDE_DIFF_RIGHT)


def _first_wide_difference() -> str:
    return next(WIDE_DIFF_LEFT.show_diffs(WIDE_DIFF_RIGHT))


def _cached_default_hint() -> object:
    return DEFAULT_STRUCT.default_pyhint()


def _default_python_record() -> object:
    return DEFAULT_STRUCT.default_pyvalue()


def _default_arrow_scalar() -> object:
    return DEFAULT_FIELD.default_arrow_scalar()


def _spark_compatibility() -> DataType:
    return DEFAULT_STRUCT.to_scheme_compat("spark")


def _measure(name: str, operation: Callable[[], object], iterations: int) -> None:
    samples = timeit.repeat(operation, number=iterations, repeat=7)
    median = statistics.median(samples)
    nanoseconds = median * 1_000_000_000 / iterations
    print(f"{name:32} {nanoseconds:12.1f} ns/op")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--iterations", type=int, default=10_000)
    args = parser.parse_args()
    if args.iterations < 1:
        parser.error("--iterations must be positive")

    gc.disable()
    try:
        wide_iterations = max(1, args.iterations // 100)
        _measure(
            "wide metadata construction",
            _construct_wide_metadata,
            wide_iterations,
        )
        _measure("wide metadata update", _update_wide_metadata, wide_iterations)
        _measure("native nested inference", _infer_nested_datatype, args.iterations)
        _measure("native typed map", _build_nested_typed_field, args.iterations)
        _measure("generic time datatype", _build_generic_time_datatype, args.iterations)
        _measure("generic time field", _build_generic_time_field, args.iterations)
        _measure("native variant datatype", _build_variant_datatype, args.iterations)
        _measure("native variant field", _build_variant_field, args.iterations)
        _measure("inferred variant datatype", _infer_variant_datatype, args.iterations)
        _measure(
            "wide diff iterator creation",
            _create_wide_difference_iterator,
            args.iterations,
        )
        _measure("wide diff first line", _first_wide_difference, args.iterations)
        _measure("cached default hint", _cached_default_hint, args.iterations)
        _measure("default Python record", _default_python_record, args.iterations)
        _measure("default Arrow scalar", _default_arrow_scalar, args.iterations)
        _measure("Spark compatibility", _spark_compatibility, args.iterations)
        _measure(
            "MIME known parse",
            lambda: MimeType.from_str(KNOWN_MIME),
            args.iterations,
        )
        _measure(
            "MIME custom parse",
            lambda: MimeType.from_str(CUSTOM_MIME),
            args.iterations,
        )
        _measure(
            "media compound parse",
            lambda: MediaType.from_str(COMPOUND_MEDIA),
            args.iterations,
        )
        _measure(
            "media header inference",
            lambda: MediaType.from_content_headers(CONTENT_TYPE, CONTENT_ENCODING),
            args.iterations,
        )
    finally:
        gc.enable()


if __name__ == "__main__":
    main()
