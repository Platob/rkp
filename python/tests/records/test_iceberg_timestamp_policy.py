from __future__ import annotations

from dataclasses import dataclass, fields
from dataclasses import field as dataclass_field
from datetime import datetime
from inspect import signature
from typing import Any

import pyarrow as pa
import pytest
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    StructType,
    TimestampNanoType,
    TimestampType,
    TimestamptzNanoType,
    TimestamptzType,
)
from rkp import (
    Record,
    arrow_into_iceberg_field,
    arrow_into_iceberg_schema,
    dataclass_into_iceberg_field,
    dataclass_into_iceberg_schema,
    field,
    into_iceberg_field,
    into_iceberg_schema,
    record,
)

_LOCAL_NS = pa.field("local_time", pa.timestamp("ns"), nullable=False)
_UTC_NS = pa.field("utc_time", pa.timestamp("ns", tz="UTC"), nullable=False)
_NS_SCHEMA = pa.schema([_LOCAL_NS, _UTC_NS])


@dataclass
class PlainNanosecondEvent:
    local_time: datetime = dataclass_field(
        metadata={"rkp": {"type": pa.timestamp("ns")}}
    )
    utc_time: datetime = dataclass_field(
        metadata={"rkp": {"type": pa.timestamp("ns", tz="UTC")}}
    )


@record
class NanosecondEvent(Record):
    local_time: datetime = field(type=pa.timestamp("ns"))
    utc_time: datetime = field(type=pa.timestamp("ns", tz="UTC"))


def _assert_schema_precision(schema: Schema, *, nanoseconds: bool) -> None:
    local_type: type[Any] = TimestampNanoType if nanoseconds else TimestampType
    utc_type: type[Any] = TimestamptzNanoType if nanoseconds else TimestamptzType
    assert isinstance(schema.find_field("local_time").field_type, local_type)
    assert isinstance(schema.find_field("utc_time").field_type, utc_type)


def _assert_struct_precision(field: NestedField, *, nanoseconds: bool) -> None:
    assert isinstance(field.field_type, StructType)
    _assert_schema_precision(
        Schema(*field.field_type.fields),
        nanoseconds=nanoseconds,
    )


@pytest.mark.parametrize(
    ("format_version", "nanoseconds"),
    [(1, False), (2, False), (3, True)],
)
def test_arrow_adaptive_default_matches_iceberg_format_capability(
    format_version: int,
    nanoseconds: bool,
) -> None:
    schema = arrow_into_iceberg_schema(
        _NS_SCHEMA,
        format_version=format_version,
    )
    facade_schema = into_iceberg_schema(
        _NS_SCHEMA,
        format_version=format_version,
    )
    local = arrow_into_iceberg_field(
        _LOCAL_NS,
        format_version=format_version,
    )
    facade_utc = into_iceberg_field(
        _UTC_NS,
        format_version=format_version,
    )

    _assert_schema_precision(schema, nanoseconds=nanoseconds)
    _assert_schema_precision(facade_schema, nanoseconds=nanoseconds)
    assert isinstance(
        local.field_type,
        TimestampNanoType if nanoseconds else TimestampType,
    )
    assert isinstance(
        facade_utc.field_type,
        TimestamptzNanoType if nanoseconds else TimestamptzType,
    )


@pytest.mark.parametrize("format_version", [1, 2])
def test_explicit_false_disables_legacy_format_downcast(
    format_version: int,
) -> None:
    with pytest.raises(TypeError, match=r"unsupported type: timestamp\[ns\]"):
        arrow_into_iceberg_field(
            _LOCAL_NS,
            format_version=format_version,
            downcast_ns_timestamp_to_us=False,
        )
    with pytest.raises(TypeError, match=r"unsupported type: timestamp\[ns\]"):
        into_iceberg_schema(
            _NS_SCHEMA,
            format_version=format_version,
            downcast_ns_timestamp_to_us=False,
        )


def test_explicit_true_downcasts_even_when_format_v3_supports_nanoseconds() -> None:
    field_result = into_iceberg_field(
        _UTC_NS,
        format_version=3,
        downcast_ns_timestamp_to_us=True,
    )
    schema_result = arrow_into_iceberg_schema(
        _NS_SCHEMA,
        format_version=3,
        downcast_ns_timestamp_to_us=True,
    )

    assert isinstance(field_result.field_type, TimestamptzType)
    _assert_schema_precision(schema_result, nanoseconds=False)


@pytest.mark.parametrize(
    ("format_version", "nanoseconds"),
    [(2, False), (3, True)],
)
def test_plain_dataclass_helpers_and_facades_share_the_adaptive_default(
    format_version: int,
    nanoseconds: bool,
) -> None:
    direct_field = dataclass_into_iceberg_field(
        PlainNanosecondEvent,
        format_version=format_version,
    )
    direct_schema = dataclass_into_iceberg_schema(
        PlainNanosecondEvent,
        format_version=format_version,
    )
    facade_field = into_iceberg_field(
        PlainNanosecondEvent,
        format_version=format_version,
    )
    facade_schema = into_iceberg_schema(
        PlainNanosecondEvent,
        format_version=format_version,
    )

    _assert_struct_precision(direct_field, nanoseconds=nanoseconds)
    _assert_schema_precision(direct_schema, nanoseconds=nanoseconds)
    _assert_struct_precision(facade_field, nanoseconds=nanoseconds)
    _assert_schema_precision(facade_schema, nanoseconds=nanoseconds)


def test_plain_dataclass_explicit_overrides_win_over_adaptive_policy() -> None:
    with pytest.raises(TypeError, match=r"unsupported type: timestamp\[ns\]"):
        dataclass_into_iceberg_schema(
            PlainNanosecondEvent,
            format_version=2,
            downcast_ns_timestamp_to_us=False,
        )

    preserved = dataclass_into_iceberg_field(
        PlainNanosecondEvent,
        format_version=3,
        downcast_ns_timestamp_to_us=False,
    )
    downcast = into_iceberg_schema(
        PlainNanosecondEvent,
        format_version=3,
        downcast_ns_timestamp_to_us=True,
    )

    _assert_struct_precision(preserved, nanoseconds=True)
    _assert_schema_precision(downcast, nanoseconds=False)


@pytest.mark.parametrize(
    ("format_version", "nanoseconds"),
    [(2, False), (3, True)],
)
def test_record_methods_and_field_convenience_share_the_adaptive_default(
    format_version: int,
    nanoseconds: bool,
) -> None:
    record_field = NanosecondEvent.into_iceberg_field(format_version=format_version)
    record_schema = NanosecondEvent.into_iceberg_schema(format_version=format_version)
    attached = fields(NanosecondEvent)[0]
    attached_field = attached.into_iceberg_field(
        owner=NanosecondEvent,
        format_version=format_version,
    )
    attached_schema = attached.into_iceberg_schema(
        owner=NanosecondEvent,
        format_version=format_version,
    )

    _assert_struct_precision(record_field, nanoseconds=nanoseconds)
    _assert_schema_precision(record_schema, nanoseconds=nanoseconds)
    assert isinstance(
        attached_field.field_type,
        TimestampNanoType if nanoseconds else TimestampType,
    )
    assert isinstance(
        attached_schema.find_field("local_time").field_type,
        TimestampNanoType if nanoseconds else TimestampType,
    )


def test_record_explicit_overrides_are_distinct_cached_policies() -> None:
    default_v3 = NanosecondEvent.into_iceberg_schema(format_version=3)
    explicit_preserve = NanosecondEvent.into_iceberg_schema(
        format_version=3,
        downcast_ns_timestamp_to_us=False,
    )
    explicit_downcast = NanosecondEvent.into_iceberg_schema(
        format_version=3,
        downcast_ns_timestamp_to_us=True,
    )

    _assert_schema_precision(default_v3, nanoseconds=True)
    _assert_schema_precision(explicit_preserve, nanoseconds=True)
    _assert_schema_precision(explicit_downcast, nanoseconds=False)
    assert default_v3 is explicit_preserve
    assert default_v3 is NanosecondEvent.into_iceberg_schema(format_version=3)
    assert explicit_downcast is NanosecondEvent.into_iceberg_schema(
        format_version=3,
        downcast_ns_timestamp_to_us=True,
    )

    with pytest.raises(TypeError, match=r"unsupported type: timestamp\[ns\]"):
        NanosecondEvent.into_iceberg_schema(
            format_version=2,
            downcast_ns_timestamp_to_us=False,
        )


@pytest.mark.parametrize(
    ("format_version", "effective", "nanoseconds"),
    [(2, True, False), (3, False, True)],
)
def test_all_unset_forms_resolve_before_record_schema_caching(
    format_version: int,
    effective: bool,
    nanoseconds: bool,
) -> None:
    omitted = NanosecondEvent.into_iceberg_schema(format_version=format_version)
    explicit_none = NanosecondEvent.into_iceberg_schema(
        format_version=format_version,
        downcast_ns_timestamp_to_us=None,
    )
    explicit_ellipsis = NanosecondEvent.into_iceberg_schema(
        format_version=format_version,
        downcast_ns_timestamp_to_us=...,
    )
    explicit_effective = NanosecondEvent.into_iceberg_schema(
        format_version=format_version,
        downcast_ns_timestamp_to_us=effective,
    )

    _assert_schema_precision(omitted, nanoseconds=nanoseconds)
    assert explicit_none is omitted
    assert explicit_ellipsis is omitted
    assert explicit_effective is omitted


@pytest.mark.parametrize("invalid", [0, 1, "true", object()])
def test_invalid_downcast_policy_values_are_rejected(invalid: object) -> None:
    with pytest.raises(TypeError, match="downcast_ns_timestamp_to_us must be bool"):
        arrow_into_iceberg_schema(
            _NS_SCHEMA,
            format_version=3,
            downcast_ns_timestamp_to_us=invalid,  # type: ignore[arg-type]
        )


def test_non_utc_nanosecond_timezone_is_not_silently_made_naive_in_v3() -> None:
    non_utc = pa.field(
        "observed_at",
        pa.timestamp("ns", tz="Europe/Paris"),
        nullable=False,
    )

    with pytest.raises(TypeError, match=r"must be naive or use a UTC timezone"):
        arrow_into_iceberg_field(non_utc, format_version=3)


@pytest.mark.parametrize("format_version", [1, 2])
def test_downcast_does_not_rewrite_an_existing_iceberg_nanosecond_schema(
    format_version: int,
) -> None:
    native = Schema(
        NestedField(1, "local_time", TimestampNanoType(), required=True),
        NestedField(2, "utc_time", TimestamptzNanoType(), required=True),
    )

    with pytest.raises(
        TypeError,
        match=rf"incompatible with format version {format_version}",
    ):
        into_iceberg_schema(
            native,
            format_version=format_version,
            downcast_ns_timestamp_to_us=True,
        )


def test_public_default_uses_global_ellipsis_as_the_auto_policy() -> None:
    assert (
        signature(arrow_into_iceberg_schema)
        .parameters["downcast_ns_timestamp_to_us"]
        .default
        is ...
    )
    assert (
        signature(NanosecondEvent.into_iceberg_schema)
        .parameters["downcast_ns_timestamp_to_us"]
        .default
        is ...
    )


def test_non_utc_timestamp_zone_is_rejected_before_pyiceberg_can_erase_it() -> None:
    non_utc = pa.schema(
        [
            pa.field(
                "payload",
                pa.struct(
                    [pa.field("created_at", pa.timestamp("ns", tz="Europe/Paris"))]
                ),
            )
        ]
    )

    with pytest.raises(TypeError, match=r"payload\.created_at.*UTC.*Europe/Paris"):
        arrow_into_iceberg_schema(non_utc, format_version=3)
