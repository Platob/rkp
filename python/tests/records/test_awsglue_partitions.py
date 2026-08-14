from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal

import boto3
import pyarrow as pa
import pytest
from moto import mock_aws

from rkp import (
    GlueCatalog,
    Record,
    field,
    into_glue_ddl,
    into_glue_partition_projection,
    into_glue_partition_values,
    into_glue_table_input,
    record,
)


@record(alias="projected_events")
class ProjectedEvent(Record):
    region: str = field(alias="aws_region", partition_key=1)
    event_day: date = field(partition_key=2)
    shard: int = field(partition_key=3)
    payload: str = ""


@record(alias="ordered_events")
class OrderedEvent(Record):
    month: str = field(partition_key=2)
    year: str = field(alias="yyyy", partition_key=1)
    region: str = field(partition_key=True)


PROJECTIONS = {
    "aws_region": {"type": "enum", "values": ["eu-west-1", "us-east-1"]},
    "event_day": {
        "type": "date",
        "range": ["2025-01-01", "NOW"],
        "format": "yyyy-MM-dd",
        "interval": 1,
        "interval_unit": "days",
    },
    "shard": {"type": "integer", "range": [0, 31], "digits": 2},
}
LOCATION_TEMPLATE = (
    "s3://warehouse/events/region=${aws_region}/day=${event_day}/shard=${shard}/"
)


def test_partition_values_use_alias_order_and_arrow_types() -> None:
    event = ProjectedEvent("eu-west-1", date(2026, 8, 14), 7, "value")

    assert into_glue_partition_values(event) == ["eu-west-1", "2026-08-14", "7"]
    assert event.into_glue_partition_values() == [
        "eu-west-1",
        "2026-08-14",
        "7",
    ]

    schema = ProjectedEvent.into_arrow_schema()
    assert into_glue_partition_values(
        {
            "aws_region": "us-east-1",
            "event_day": date(2026, 8, 15),
            "shard": 8,
        },
        schema,
    ) == ["us-east-1", "2026-08-15", "8"]


def test_mapping_partition_values_can_use_an_explicit_order_without_schema() -> None:
    assert into_glue_partition_values(
        {"enabled": True, "day": date(2026, 8, 14)},
        partition_keys=["day", "enabled"],
    ) == ["2026-08-14", "true"]


def test_partition_role_positions_order_values_and_table_keys() -> None:
    event = OrderedEvent("08", "2026", "eu-west-1")

    assert into_glue_partition_values(event) == ["2026", "08", "eu-west-1"]
    table = into_glue_table_input(OrderedEvent)
    assert [item["Name"] for item in table["PartitionKeys"]] == [
        "yyyy",
        "month",
        "region",
    ]
    assert into_glue_partition_values(
        event,
        partition_keys=["month", "yyyy", "region"],
    ) == ["08", "2026", "eu-west-1"]


def test_duplicate_partition_role_positions_are_rejected() -> None:
    schema = pa.schema(
        [
            pa.field("first", pa.string(), metadata={b"partition_key": b"1"}),
            pa.field("second", pa.string(), metadata={b"partition_key": b"1"}),
        ]
    )

    with pytest.raises(ValueError, match="duplicate partition key position"):
        into_glue_partition_values({"first": "a", "second": "b"}, schema)


def test_partition_values_render_athena_primitive_strings() -> None:
    schema = pa.schema(
        [
            pa.field("day", pa.date32()),
            pa.field("moment", pa.timestamp("us", tz="UTC")),
            pa.field("amount", pa.decimal128(10, 2)),
            pa.field("digest", pa.binary()),
        ]
    )
    values = {
        "day": date(2026, 8, 14),
        "moment": datetime(2026, 8, 14, 1, 2, 3, 456000, tzinfo=UTC),
        "amount": Decimal("12.30"),
        "digest": b"\x00\xff",
    }

    assert into_glue_partition_values(values, schema, partition_keys=schema.names) == [
        "2026-08-14",
        "2026-08-14 01:02:03.456",
        "12.30",
        "AP8=",
    ]

    values["moment"] = datetime(
        2026,
        8,
        14,
        3,
        2,
        3,
        456000,
        tzinfo=timezone(timedelta(hours=2)),
    )
    assert into_glue_partition_values(
        values,
        schema,
        partition_keys=["moment"],
    ) == ["2026-08-14 01:02:03.456"]


def test_partition_timestamp_rejects_precision_athena_cannot_represent() -> None:
    schema = pa.schema([pa.field("moment", pa.timestamp("us"))])

    with pytest.raises(ValueError, match="millisecond timestamp precision"):
        into_glue_partition_values(
            {"moment": datetime(2026, 8, 14, 1, 2, 3, 456789)},
            schema,
            partition_keys=["moment"],
        )


def test_partition_values_validate_missing_null_type_and_nested_values() -> None:
    schema = ProjectedEvent.into_arrow_schema()
    with pytest.raises(ValueError, match="missing.*event_day"):
        into_glue_partition_values({"aws_region": "eu", "shard": 1}, schema)
    with pytest.raises(ValueError, match="cannot be null"):
        into_glue_partition_values(
            {"aws_region": "eu", "event_day": None, "shard": 1}, schema
        )
    with pytest.raises(TypeError, match="incompatible"):
        into_glue_partition_values(
            {"aws_region": "eu", "event_day": date(2026, 8, 14), "shard": "bad"},
            schema,
        )
    with pytest.raises(TypeError, match="primitive"):
        into_glue_partition_values(
            {"items": ["a"]},
            pa.schema([pa.field("items", pa.list_(pa.string()))]),
            partition_keys=["items"],
        )


def test_projection_builds_canonical_aws_properties_and_record_convenience() -> None:
    expected = {
        "projection.aws_region.type": "enum",
        "projection.aws_region.values": "eu-west-1,us-east-1",
        "projection.enabled": "true",
        "projection.event_day.format": "yyyy-MM-dd",
        "projection.event_day.interval": "1",
        "projection.event_day.interval.unit": "DAYS",
        "projection.event_day.range": "2025-01-01,NOW",
        "projection.event_day.type": "date",
        "projection.shard.digits": "2",
        "projection.shard.range": "0,31",
        "projection.shard.type": "integer",
        "storage.location.template": LOCATION_TEMPLATE,
    }

    assert (
        into_glue_partition_projection(
            ProjectedEvent,
            PROJECTIONS,
            location_template=LOCATION_TEMPLATE,
        )
        == expected
    )
    assert (
        ProjectedEvent.into_glue_partition_projection(
            PROJECTIONS,
            location_template=LOCATION_TEMPLATE,
        )
        == expected
    )


def test_projection_supports_millisecond_date_intervals() -> None:
    schema = pa.schema(
        [pa.field("moment", pa.string(), metadata={b"partition_key": b"true"})]
    )

    assert into_glue_partition_projection(
        schema,
        {
            "moment": {
                "type": "date",
                "format": "yyyy-MM-dd HH:mm:ss.SSS",
                "range": ["2026-01-01 00:00:00.000", "NOW"],
                "interval": 1,
                "interval_unit": "millis",
            }
        },
    )["projection.moment.interval.unit"] == "MILLIS"


@pytest.mark.parametrize(
    ("definition", "match"),
    [
        (
            {"type": "date", "format": "   ", "range": ["2025", "NOW"]},
            "non-empty string",
        ),
        (
            {
                "type": "date",
                "format": "yyyy-MM-dd",
                "range": ["garbage", "NOW"],
            },
            "does not match",
        ),
        (
            {
                "type": "date",
                "format": "yyyy-MM-dd",
                "range": ["2025-01-01", "NOW++"],
            },
            "relative date range",
        ),
        (
            {
                "type": "date",
                "format": "yyyy-MM-dd HH",
                "range": ["2025-01-01 00", "NOW"],
            },
            "requires.*interval",
        ),
        (
            {
                "type": "date",
                "format": "yyyy-MM-dd",
                "range": ["2026-01-01", "2025-01-01"],
            },
            "minimum exceeds",
        ),
    ],
)
def test_projection_rejects_invalid_date_definitions(
    definition: dict[str, object], match: str
) -> None:
    schema = pa.schema(
        [pa.field("day", pa.string(), metadata={b"partition_key": b"true"})]
    )

    with pytest.raises((TypeError, ValueError), match=match):
        into_glue_partition_projection(schema, {"day": definition})


def test_table_input_and_ddl_include_the_same_projection_properties() -> None:
    table = into_glue_table_input(
        ProjectedEvent,
        location="s3://warehouse/events/",
        partition_projection=PROJECTIONS,
        partition_location_template=LOCATION_TEMPLATE,
    )
    ddl = into_glue_ddl(
        ProjectedEvent,
        location="s3://warehouse/events/",
        partition_projection=PROJECTIONS,
        partition_location_template=LOCATION_TEMPLATE,
    )

    projection = {
        key: value
        for key, value in table["Parameters"].items()
        if key.startswith("projection.") or key == "storage.location.template"
    }
    assert projection == into_glue_partition_projection(
        ProjectedEvent,
        PROJECTIONS,
        location_template=LOCATION_TEMPLATE,
    )
    for key, value in projection.items():
        assert f"'{key}'='{value}'" in ddl


@pytest.mark.parametrize(
    ("projections", "match"),
    [
        ({}, "missing"),
        ({**PROJECTIONS, "unknown": "injected"}, "not partition"),
        ({**PROJECTIONS, "shard": {"type": "integer"}}, "missing.*range"),
        (
            {**PROJECTIONS, "shard": {"type": "integer", "range": [2, 1]}},
            "minimum exceeds",
        ),
        (
            {**PROJECTIONS, "aws_region": {"type": "enum", "values": ["a", "a"]}},
            "unique",
        ),
    ],
)
def test_projection_rejects_incomplete_or_invalid_definitions(
    projections: dict[str, object], match: str
) -> None:
    with pytest.raises(ValueError, match=match):
        into_glue_partition_projection(ProjectedEvent, projections)


def test_projection_validates_field_compatibility_template_and_conflicts() -> None:
    incompatible = {
        **PROJECTIONS,
        "shard": {"type": "injected"},
    }
    with pytest.raises(TypeError, match="incompatible"):
        into_glue_partition_projection(ProjectedEvent, incompatible)
    with pytest.raises(ValueError, match="placeholders.*missing"):
        into_glue_partition_projection(
            ProjectedEvent,
            PROJECTIONS,
            location_template="s3://warehouse/${event_day}/",
        )
    with pytest.raises(ValueError, match="conflicts"):
        into_glue_table_input(
            ProjectedEvent,
            parameters={"projection.enabled": "false"},
            partition_projection=PROJECTIONS,
        )


def test_projection_can_be_persisted_while_disabled() -> None:
    projected = into_glue_partition_projection(
        ProjectedEvent,
        {"aws_region": "injected"},
        enabled=False,
    )

    assert projected == {
        "projection.aws_region.type": "injected",
        "projection.enabled": "false",
    }


@mock_aws
def test_catalog_projects_and_creates_a_partition_from_a_record() -> None:
    client = boto3.client("glue", region_name="eu-west-1")
    catalog = GlueCatalog(client)
    catalog.create_database("analytics")
    catalog.create_table(
        "analytics",
        into_glue_table_input(
            ProjectedEvent,
            location="s3://warehouse/events/",
        ),
    )
    event = ProjectedEvent("eu-west-1", date(2026, 8, 14), 7, "value")

    assert catalog.partition_values("analytics", "projected_events", event) == [
        "eu-west-1",
        "2026-08-14",
        "7",
    ]
    created = catalog.create_partition_from(
        "analytics",
        "projected_events",
        event,
        location=("s3://warehouse/events/region=eu-west-1/day=2026-08-14/shard=7/"),
        parameters={"ready": True},
    )
    assert created["Values"] == ["eu-west-1", "2026-08-14", "7"]
    assert created["Parameters"] == {"ready": "true"}
    assert created["StorageDescriptor"]["Location"].endswith("/shard=7/")
