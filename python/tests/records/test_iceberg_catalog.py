from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from rkp import Record, field, record
from rkp.records import iceberg_catalog as catalog_adapter

pytest.importorskip(
    "sqlalchemy",
    reason="the PyIceberg SQL catalog requires SQLAlchemy",
)

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    IdentityTransform,
)


@record(schema_name="analytics", table_name="catalog_events")
class Event(Record):
    event_id: int = field(seq=1, primary_key=True)
    occurred_at: dt.datetime = field(seq=2, partition_key="day")
    shard: str = field(seq=3, partition_key="bucket[8]")
    label: str = field(seq=4, index_key=True)
    payload: str | None = field(seq=5, default=None)


@record(schema_name="analytics", table_name="catalog_events")
class EvolvedEvent(Record):
    event_id: int = field(seq=1, primary_key=True)
    occurred_at: dt.datetime = field(seq=2, partition_key="day")
    shard: str = field(seq=3, partition_key="bucket[8]")
    label: str = field(seq=4, index_key=True)
    payload: str | None = field(seq=5, default=None)
    extra: int | None = field(seq=6, default=None)


@record(table_name="unpartitioned")
class Simple(Record):
    identifier: int
    label: str | None = None


def _rows(count: int = 4) -> list[Event]:
    return [
        Event(
            index,
            dt.datetime(2026, 8, index + 1, 12, tzinfo=dt.UTC),
            f"shard-{index}",
            "label",
            None if index % 2 else f"payload-{index}",
        )
        for index in range(count)
    ]


@pytest.fixture
def catalog(tmp_path: Path) -> Iterator[Any]:
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    yield SqlCatalog(
        "rkp_test",
        uri="sqlite:///:memory:",
        warehouse=f"file://{warehouse}",
    )


@pytest.mark.parametrize("format_version", [1, 2])
def test_tables_are_created_at_the_requested_format_version(
    catalog: Any, format_version: int
) -> None:
    table = catalog_adapter.create_iceberg_table(
        catalog,
        Event,
        identifier=("analytics", f"events_v{format_version}"),
        format_version=format_version,
    )

    assert table.metadata.format_version == format_version
    assert table.schema().find_field("event_id").field_id == 1
    assert table.schema().identifier_field_ids == [1]
    assert catalog.namespace_exists(("analytics",))


def test_writing_v3_table_metadata_reports_the_runtime_limit(catalog: Any) -> None:
    with pytest.raises(NotImplementedError, match="format version 3 table metadata"):
        catalog_adapter.create_iceberg_table(
            catalog,
            Event,
            identifier=("analytics", "events_v3"),
            format_version=3,
        )


def test_partition_specs_and_sort_orders_come_from_field_roles(
    catalog: Any,
) -> None:
    table = catalog_adapter.create_iceberg_table(catalog, Event)

    spec_fields = table.spec().fields
    assert [item.name for item in spec_fields] == [
        "occurred_at_day",
        "shard_bucket_8",
    ]
    assert isinstance(spec_fields[0].transform, DayTransform)
    assert isinstance(spec_fields[1].transform, BucketTransform)
    assert [item.source_id for item in spec_fields] == [2, 3]
    assert [item.source_id for item in table.sort_order().fields] == [4]


def test_explicit_partition_keys_use_identity_transforms() -> None:
    spec = catalog_adapter.into_iceberg_partition_spec(Event, partition_keys=["shard"])

    assert [item.name for item in spec.fields] == ["shard"]
    assert isinstance(spec.fields[0].transform, IdentityTransform)
    assert catalog_adapter.into_iceberg_partition_spec(Simple).fields == ()
    assert catalog_adapter.into_iceberg_sort_order(Simple).order_id == 0
    with pytest.raises(ValueError, match="unknown field names"):
        catalog_adapter.into_iceberg_partition_spec(Event, partition_keys=["missing"])


def test_records_round_trip_through_a_live_table(catalog: Any) -> None:
    table = catalog_adapter.create_iceberg_table(catalog, Event)
    rows = _rows()

    catalog_adapter.records_into_iceberg_table(table, rows, record_type=Event)
    restored = list(catalog_adapter.iceberg_table_into_records(Event, table))

    assert sorted(restored, key=lambda item: item.event_id) == rows
    arrow_table = catalog_adapter.iceberg_table_into_arrow(table)
    assert arrow_table.num_rows == len(rows)
    assert set(arrow_table.schema.names) == set(Event.into_arrow_schema().names)


def test_overwrite_replaces_the_previous_snapshot(catalog: Any) -> None:
    table = catalog_adapter.create_iceberg_table(catalog, Event)
    catalog_adapter.records_into_iceberg_table(table, _rows(), record_type=Event)

    catalog_adapter.records_into_iceberg_table(
        table, _rows(2), record_type=Event, mode="overwrite"
    )

    assert len(list(catalog_adapter.iceberg_table_into_records(Event, table))) == 2
    with pytest.raises(ValueError, match="mode must be"):
        catalog_adapter.records_into_iceberg_table(
            table, _rows(1), record_type=Event, mode="upsert"
        )


def test_schema_evolution_adds_columns_without_rewriting_ids(catalog: Any) -> None:
    table = catalog_adapter.create_iceberg_table(catalog, Event)
    catalog_adapter.records_into_iceberg_table(table, _rows(2), record_type=Event)

    catalog_adapter.sync_iceberg_table_schema(table, EvolvedEvent)

    assert table.schema().find_field("event_id").field_id == 1
    assert table.schema().find_field("extra").required is False
    catalog_adapter.records_into_iceberg_table(
        table,
        [
            EvolvedEvent(
                9,
                dt.datetime(2026, 9, 1, tzinfo=dt.UTC),
                "shard-9",
                "label",
                None,
                42,
            )
        ],
        record_type=EvolvedEvent,
    )
    values = {
        item.event_id: item.extra
        for item in catalog_adapter.iceberg_table_into_records(EvolvedEvent, table)
    }
    assert values == {0: None, 1: None, 9: 42}


def test_identifiers_default_to_record_metadata(catalog: Any) -> None:
    assert catalog_adapter.resolve_identifier(Event, None) == (
        "analytics",
        "catalog_events",
    )
    assert catalog_adapter.resolve_identifier(Simple, None) == (
        "default",
        "unpartitioned",
    )
    assert catalog_adapter.resolve_identifier(None, "db.table") == ("db", "table")
    assert catalog_adapter.resolve_identifier(None, ("db", "table")) == ("db", "table")

    created = catalog_adapter.create_iceberg_table(catalog, Event)
    assert catalog_adapter.load_iceberg_table(catalog, Event).name() == created.name()
    # Creating twice is idempotent by default.
    assert catalog_adapter.create_iceberg_table(catalog, Event).name() == created.name()


def test_arguments_are_validated(catalog: Any) -> None:
    table = catalog_adapter.create_iceberg_table(catalog, Simple)

    with pytest.raises(TypeError, match="catalog must be"):
        catalog_adapter.create_iceberg_table(object(), Simple)
    with pytest.raises(TypeError, match="table must be"):
        catalog_adapter.records_into_iceberg_table(object(), [], record_type=Simple)
    with pytest.raises(ValueError, match="format_version must be"):
        catalog_adapter.create_iceberg_table(catalog, Simple, format_version=4)
    with pytest.raises(ValueError, match="invalid Iceberg transform"):
        catalog_adapter.into_iceberg_partition_spec(_BadTransform)
    assert table.spec().fields == ()


@record(table_name="bad_transform")
class _BadTransform(Record):
    identifier: int = field(partition_key="not-a-transform")
