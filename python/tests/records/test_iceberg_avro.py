from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DecimalType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    TimestampNanoType,
    UUIDType,
)
from rkp import Record, field, record
from rkp.avro import FixedSchema, PrimitiveSchema, RecordSchema, canonical_form
from rkp.records.iceberg import avro_into_iceberg_schema, iceberg_into_avro_schema


@record(alias="events", schema_name="analytics")
class Event(Record):
    event_id: int = field(seq=1, primary_key=True)
    occurred_at: dt.datetime
    amount: Decimal
    tags: list[str | None]
    attributes: dict[str, int | None]


def test_iceberg_avro_carries_every_identity_attribute() -> None:
    schema = Event.into_iceberg_schema()

    avro_schema = iceberg_into_avro_schema(schema, name="events")

    assert isinstance(avro_schema, RecordSchema)
    assert avro_schema.name == "events"
    assert avro_schema.field("event_id").attributes["field-id"] == 1
    tags = avro_schema.field("tags").type
    attributes = avro_schema.field("attributes").type
    assert tags.attributes["element-id"] == schema.find_field("tags.element").field_id
    assert (
        attributes.attributes["key-id"] == schema.find_field("attributes.key").field_id
    )
    assert (
        attributes.attributes["value-id"]
        == schema.find_field("attributes.value").field_id
    )


def test_iceberg_avro_uses_fixed_decimals_and_adjust_to_utc() -> None:
    avro_schema = iceberg_into_avro_schema(Event.into_iceberg_schema())

    amount = avro_schema.field("amount").type
    occurred = avro_schema.field("occurred_at").type
    assert isinstance(amount, FixedSchema)
    assert (amount.logical, amount.precision, amount.scale) == ("decimal", 38, 18)
    # Iceberg's minimum fixed width for decimal(38, 18).
    assert amount.size == 16
    assert isinstance(occurred, PrimitiveSchema)
    assert occurred.logical == "timestamp-micros"
    assert occurred.attributes["adjust-to-utc"] is True


def test_avro_round_trip_preserves_the_iceberg_schema() -> None:
    schema = Event.into_iceberg_schema()

    restored = avro_into_iceberg_schema(iceberg_into_avro_schema(schema))

    assert restored.as_struct() == schema.as_struct()
    assert restored.find_field("event_id").field_id == 1


def test_nanosecond_timestamps_survive_the_v3_avro_representation() -> None:
    schema = Schema(
        NestedField(1, "moment", TimestampNanoType(), required=True),
        NestedField(2, "identity", UUIDType(), required=True),
        NestedField(3, "amount", DecimalType(9, 2), required=False),
    )

    avro_schema = iceberg_into_avro_schema(schema, name="v3")
    restored = avro_into_iceberg_schema(avro_schema, format_version=3)

    moment = avro_schema.field("moment").type
    identity = avro_schema.field("identity").type
    assert isinstance(moment, PrimitiveSchema) and moment.logical == "timestamp-nanos"
    assert isinstance(identity, FixedSchema) and identity.logical == "uuid"
    assert restored.as_struct() == schema.as_struct()


def test_single_fields_and_names_are_supported() -> None:
    schema = Event.into_iceberg_schema()
    nested = schema.find_field("attributes")

    converted = iceberg_into_avro_schema(nested)
    assert converted.name == "attributes"
    assert converted.field("attributes").attributes["field-id"] == nested.field_id

    named = iceberg_into_avro_schema(schema, name="renamed", namespace="rkp.test")
    assert named.fullname == "rkp.test.renamed"
    with pytest.raises(TypeError, match="Iceberg Schema or NestedField"):
        iceberg_into_avro_schema("events")


def test_external_avro_declarations_receive_deterministic_ids() -> None:
    declaration = {
        "type": "record",
        "name": "external",
        "fields": [
            {"name": "identifier", "type": "long"},
            {"name": "labels", "type": {"type": "array", "items": "string"}},
        ],
    }

    schema = avro_into_iceberg_schema(declaration, schema_id=4)

    assert schema.schema_id == 4
    assert isinstance(schema.find_field("identifier").field_type, LongType)
    labels = schema.find_field("labels")
    assert isinstance(labels.field_type, ListType)
    ids = [schema.find_field(name).field_id for name in schema.column_names]
    assert len(ids) == len(set(ids))
    assert all(field_id > 0 for field_id in ids)


def test_canonical_form_is_stable_across_conversions() -> None:
    schema = Schema(
        NestedField(1, "identifier", LongType(), required=True),
        NestedField(
            2,
            "lookup",
            MapType(3, StringType(), 4, LongType(), value_required=False),
            required=True,
        ),
    )

    first = iceberg_into_avro_schema(schema, name="stable")
    second = iceberg_into_avro_schema(schema, name="stable")

    assert canonical_form(first) == canonical_form(second)
    assert first.fingerprint() == second.fingerprint()
