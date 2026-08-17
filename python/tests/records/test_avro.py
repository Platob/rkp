from __future__ import annotations

import datetime as dt
import uuid
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
import rkp
from rkp import Record, field, record
from rkp.avro import (
    ArraySchema,
    FixedSchema,
    MapSchema,
    PrimitiveSchema,
    RecordSchema,
    UnionSchema,
    parse_schema,
    read_container,
)
from rkp.records import avro as avro_adapter


@record
class Metric(Record):
    name: str
    value: float | None


@record(alias="avro_events", schema_name="analytics")
class Event(Record):
    event_id: int = field(alias="eventId", seq=1, primary_key=True)
    label: str | None
    occurred_at: dt.datetime
    observed_on: dt.date
    amount: Decimal
    identity: uuid.UUID
    metrics: list[Metric]
    dimensions: dict[str, int | None]
    payload: bytes


@dataclass
class PlainRow:
    identifier: int
    label: str | None


ROWS = (
    Event(
        1,
        "first",
        dt.datetime(2026, 8, 17, 12, tzinfo=dt.UTC),
        dt.date(2026, 8, 17),
        Decimal("1.25"),
        uuid.UUID("3f2504e0-4f89-11d3-9a0c-0305e82c3301"),
        [Metric("a", 1.0), Metric("b", None)],
        {"shard": 3, "missing": None},
        b"\x00\x01",
    ),
    Event(
        2,
        None,
        dt.datetime(2026, 8, 18, 6, tzinfo=dt.UTC),
        dt.date(2026, 8, 18),
        Decimal("-3.5"),
        uuid.UUID("3f2504e0-4f89-11d3-9a0c-0305e82c3302"),
        [],
        {},
        b"",
    ),
)


def test_record_schema_uses_aliases_identity_and_logical_types() -> None:
    schema = avro_adapter.into_avro_schema(Event)

    assert isinstance(schema, RecordSchema)
    assert schema.name == "avro_events"
    assert [item.name for item in schema.fields][:3] == [
        "eventId",
        "label",
        "occurred_at",
    ]
    assert schema.field("eventId").attributes["field-id"] == 1
    assert isinstance(schema.field("label").type, UnionSchema)
    assert schema.field("label").default is None
    occurred = schema.field("occurred_at").type
    assert isinstance(occurred, PrimitiveSchema)
    assert occurred.logical == "timestamp-micros"
    assert isinstance(schema.field("metrics").type, ArraySchema)
    assert isinstance(schema.field("dimensions").type, MapSchema)
    assert schema is avro_adapter.into_avro_schema(Event)


def _arrow_uuid_type() -> pa.DataType:
    factory = getattr(pa, "uuid", None)
    return factory() if factory is not None else pa.binary(16)


def test_iceberg_flavor_uses_fixed_decimals_uuids_and_adjust_to_utc() -> None:
    schema = avro_adapter.into_avro_schema(Event, flavor="iceberg")

    amount = schema.field("amount").type
    identity = schema.field("identity").type
    occurred = schema.field("occurred_at").type
    assert isinstance(amount, FixedSchema) and amount.logical == "decimal"
    assert amount.size == 16 and amount.precision == 38
    # ``uuid.UUID`` annotations use RKP's canonical string representation, so
    # the fixed-backed UUID appears when the Arrow type is a real UUID.
    assert isinstance(identity, PrimitiveSchema) and identity.primitive == "string"
    uuid_field = pa.field("identity", _arrow_uuid_type(), nullable=False)
    converted = avro_adapter.arrow_into_avro_field(uuid_field, flavor="iceberg").type
    assert isinstance(converted, FixedSchema) and converted.size == 16
    assert isinstance(occurred, PrimitiveSchema)
    assert occurred.attributes["adjust-to-utc"] is True
    with pytest.raises(ValueError, match="flavor must be"):
        avro_adapter.into_avro_schema(Event, flavor="parquet")


def test_arrow_round_trip_preserves_the_field_contract() -> None:
    arrow_schema = Event.into_arrow_schema()
    schema = avro_adapter.arrow_into_avro_schema(arrow_schema)

    restored = avro_adapter.avro_into_arrow_schema(schema)
    assert restored.names == arrow_schema.names
    for name in arrow_schema.names:
        assert restored.field(name).type == arrow_schema.field(name).type
        assert restored.field(name).nullable == arrow_schema.field(name).nullable
    assert rkp.table_name(restored) == "avro_events"


def test_dataclasses_and_generic_dispatch_agree() -> None:
    from_dataclass = avro_adapter.dataclass_into_avro_schema(PlainRow)
    generic = avro_adapter.into_avro_schema(PlainRow)

    assert from_dataclass == generic
    assert [item.name for item in generic.fields] == ["identifier", "label"]
    assert avro_adapter.into_avro_schema(Event.into_arrow_schema()) == (
        avro_adapter.into_avro_schema(Event)
    )
    assert avro_adapter.into_avro_schema(generic.into_json()) == generic
    with pytest.raises(TypeError, match="record-shaped"):
        avro_adapter.into_avro_schema("string")


def test_records_round_trip_through_an_object_container() -> None:
    payload = avro_adapter.records_into_avro(ROWS, record_type=Event)

    assert list(avro_adapter.avro_into_records(Event, payload)) == list(ROWS)
    assert next(iter(read_container(payload)))["eventId"] == 1
    assert list(Event.from_avro(Event.into_avro(ROWS, codec="deflate"))) == list(ROWS)


def test_container_round_trip_survives_a_file(tmp_path: Path) -> None:
    destination = tmp_path / "events.avro"
    destination.write_bytes(avro_adapter.records_into_avro(ROWS, record_type=Event))

    assert list(avro_adapter.avro_into_records(Event, destination)) == list(ROWS)


def test_writing_requires_a_resolvable_schema() -> None:
    with pytest.raises(TypeError, match="empty records require"):
        avro_adapter.records_into_avro([])
    with pytest.raises(TypeError, match="cannot infer record_type"):
        avro_adapter.records_into_avro([{"eventId": 1}])
    with pytest.raises(TypeError, match="all records must be Event"):
        avro_adapter.records_into_avro([Metric("a", 1.0)], record_type=Event)
    with pytest.raises(TypeError, match="decorated record type"):
        avro_adapter.avro_into_records(PlainRow, b"")


def test_avro_declarations_without_ids_still_convert_to_arrow() -> None:
    declaration: dict[str, Any] = {
        "type": "record",
        "name": "external",
        "fields": [
            {"name": "identifier", "type": "long"},
            {"name": "label", "type": ["null", "string"], "default": None},
            {
                "name": "pairs",
                "type": {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "pair",
                        "fields": [
                            {"name": "key", "type": "string"},
                            {"name": "value", "type": ["null", "long"]},
                        ],
                    },
                },
            },
        ],
    }

    arrow_schema = avro_adapter.avro_into_arrow_schema(declaration)

    assert arrow_schema.names == ["identifier", "label", "pairs"]
    assert arrow_schema.field("identifier").type == pa.int64()
    assert arrow_schema.field("label").nullable is True
    assert pa.types.is_list(arrow_schema.field("pairs").type)
    assert avro_adapter.avro_into_field_specs(declaration)[0].name == "identifier"


def test_non_string_map_keys_use_the_array_of_pairs_representation() -> None:
    @record
    class Keyed(Record):
        lookup: dict[int, str]

    schema = avro_adapter.into_avro_schema(Keyed)
    lookup = schema.field("lookup").type

    assert isinstance(lookup, ArraySchema)
    assert lookup.attributes["logicalType"] == "map"
    restored = avro_adapter.avro_into_arrow_schema(schema)
    assert pa.types.is_map(restored.field("lookup").type)
    assert restored.field("lookup").type.key_type == pa.int64()


def test_field_level_conversion_matches_schema_conversion() -> None:
    arrow_field = Event.into_arrow_schema().field("eventId")
    avro_field = avro_adapter.arrow_into_avro_field(arrow_field)

    assert avro_field.name == "eventId"
    assert avro_field.attributes["field-id"] == 1
    assert avro_adapter.avro_into_arrow_field(avro_field).equals(
        arrow_field, check_metadata=True
    )


def test_public_facade_exposes_the_avro_adapter() -> None:
    assert rkp.into_avro_schema(Event) == avro_adapter.into_avro_schema(Event)
    payload = rkp.records_into_avro(ROWS, record_type=Event)
    assert list(rkp.avro_into_records(Event, payload)) == list(ROWS)
    assert rkp.avro_into_arrow_schema(rkp.into_avro_schema(Event)).names == (
        Event.into_arrow_schema().names
    )
    assert parse_schema(rkp.into_avro_schema(Event).into_json()) == (
        rkp.into_avro_schema(Event)
    )
