from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass, fields
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any

import pyarrow as pa
import pytest
from pyiceberg.io.pyarrow import pyarrow_to_schema
from pyiceberg.schema import Schema
from pyiceberg.types import (
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    TimestamptzType,
    UnknownType,
)
from rkp import (
    Record,
    arrow_into_iceberg_field,
    arrow_into_iceberg_schema,
    dataclass_into_arrow_schema,
    dataclass_into_iceberg_field,
    dataclass_into_iceberg_schema,
    field,
    iceberg_fields_into_schema,
    iceberg_into_arrow_field,
    iceberg_into_arrow_schema,
    into_arrow_schema,
    into_iceberg_field,
    into_iceberg_schema,
    record,
)


@record
class Address(Record):
    city: str
    zip_code: int | None = field(alias="zip")


@record(alias="events")
class Event(Record):
    event_id: int = field(alias="eventId", primary_key=True)
    address: Address
    tags: list[str | None]
    attributes: dict[str, int | None]
    pair: tuple[int, str | None]
    created_at: datetime


@dataclass
class PlainDataclass:
    identifier: int
    label: str | None


@record
class ExplicitChild(Record):
    code: int = field(field_id=303)


@record
class ExplicitIds(Record):
    identifier: int = field(field_id=101)
    child: ExplicitChild = field(field_id=202)
    generated: str


@record
class DuplicateIds(Record):
    first: int = field(field_id=7)
    second: str = field(field_id=7)


@record
class DynamicValue(Record):
    payload: Any


@record
class SequencedChild(Record):
    generated: int
    explicit: str = field(seq=2)


@record
class SequencedParent(Record):
    generated: int
    child: SequencedChild = field(seq=4)
    explicit: str = field(seq=1)


@record
class NestedDuplicateSeqChild(Record):
    duplicate: int = field(seq=17)


@record
class NestedDuplicateSeqParent(Record):
    top_level: int = field(seq=17)
    child: NestedDuplicateSeqChild


@record
class SequencedCollections(Record):
    items: list[Annotated[int, {"rkp": {"seq": 14}}]] = field(seq=13)
    lookup: dict[
        Annotated[str, {"rkp": {"seq": 16}}],
        Annotated[int, {"rkp": {"seq": 17}}],
    ] = field(seq=15)


@record
class DuplicateCollectionSeq(Record):
    items: list[Annotated[int, {"rkp": {"seq": 31}}]] = field(seq=31)


def _deep_arrow_fields(schema: pa.Schema) -> list[pa.Field]:
    result: list[pa.Field] = []

    def visit(field: pa.Field) -> None:
        result.append(field)
        if pa.types.is_struct(field.type):
            for child in field.type:
                visit(child)
        elif (
            pa.types.is_list(field.type)
            or pa.types.is_large_list(field.type)
            or pa.types.is_fixed_size_list(field.type)
        ):
            visit(field.type.value_field)
        elif pa.types.is_map(field.type):
            visit(field.type.key_field)
            visit(field.type.item_field)

    for root_field in schema:
        visit(root_field)
    return result


def test_field_adapters_are_the_canonical_iceberg_conversion_primitive() -> None:
    inferred = into_iceberg_field("counter", int, field_id_start=17)
    arrow = pa.field(
        "documented",
        pa.int64(),
        nullable=False,
        metadata={b"PARQUET:field_id": b"41", b"doc": b"Stable value"},
    )
    converted = arrow_into_iceberg_field(arrow)

    assert isinstance(inferred, NestedField)
    assert inferred.field_id == 17
    assert inferred.name == "counter"
    assert inferred.required is True
    assert isinstance(inferred.field_type, LongType)
    assert converted == NestedField(
        41,
        "documented",
        LongType(),
        required=True,
        doc="Stable value",
    )
    assert into_iceberg_field(converted) is converted


def test_dataclass_and_record_iceberg_fields_are_named_structs() -> None:
    plain = dataclass_into_iceberg_field(
        PlainDataclass,
        name="plain",
        nullable=True,
        field_id_start=50,
    )
    record_field = into_iceberg_field(Event)

    assert plain.field_id == 50
    assert plain.name == "plain"
    assert plain.required is False
    assert isinstance(plain.field_type, StructType)
    assert [child.name for child in plain.field_type.fields] == [
        "identifier",
        "label",
    ]
    assert [child.field_id for child in plain.field_type.fields] == [51, 52]
    assert record_field == Event.into_iceberg_field()
    assert record_field.name == "events"
    assert isinstance(record_field.field_type, StructType)


def test_record_iceberg_field_is_cached_per_signature() -> None:
    first = Event.into_iceberg_field()
    renamed = Event.into_iceberg_field("eventEnvelope", field_id_start=100)

    assert Event.into_iceberg_field() is first
    assert Event.into_iceberg_field("eventEnvelope", field_id_start=100) is renamed
    assert renamed is not first
    assert renamed.name == "eventEnvelope"
    assert renamed.field_id == 100


def test_rkp_field_convenience_uses_its_attached_owner_and_metadata() -> None:
    record_field = fields(ExplicitIds)[0]

    converted = record_field.into_iceberg_field(owner=ExplicitIds)

    assert converted == into_iceberg_field(record_field, owner=ExplicitIds)
    assert converted.field_id == 101
    assert converted.name == "identifier"
    assert converted.required is True
    assert isinstance(converted.field_type, LongType)

    schema = record_field.into_iceberg_schema(owner=ExplicitIds)
    assert tuple(schema.fields) == (converted,)

    nested_record_field = fields(ExplicitIds)[1]
    nested_converted = nested_record_field.into_iceberg_field(owner=ExplicitIds)
    nested_schema = nested_record_field.into_iceberg_schema(owner=ExplicitIds)
    assert isinstance(nested_converted.field_type, StructType)
    assert tuple(nested_schema.fields) == (nested_converted,)
    assert nested_schema.find_field("child.code").field_id == 303


def test_already_converted_fields_use_safe_schema_composition() -> None:
    first = into_iceberg_field("first", int)
    second = into_iceberg_field("second", str)

    with pytest.raises(ValueError, match="duplicate Iceberg field ID"):
        iceberg_fields_into_schema(first, second)
    with pytest.raises(ValueError, match="duplicate Iceberg field ID"):
        into_iceberg_schema([first, second])

    unique = into_iceberg_field("second", str, field_id_start=2)
    schema = iceberg_fields_into_schema(first, unique)
    assert tuple(schema.fields) == (first, unique)


def test_record_and_dataclass_schemas_are_composed_from_their_struct_fields() -> None:
    record_field = Event.into_iceberg_field()
    record_schema = Event.into_iceberg_schema()
    plain_field = dataclass_into_iceberg_field(PlainDataclass)
    plain_schema = dataclass_into_iceberg_schema(PlainDataclass)

    assert isinstance(record_field.field_type, StructType)
    assert tuple(record_schema.fields) == tuple(record_field.field_type.fields)
    assert record_schema.identifier_field_ids == [
        record_field.field_type.field_by_name("eventId").field_id
    ]
    assert isinstance(plain_field.field_type, StructType)
    assert tuple(plain_schema.fields) == tuple(plain_field.field_type.fields)


def test_nested_identifier_survives_iceberg_field_arrow_schema_roundtrip() -> None:
    iceberg_field = Event.into_iceberg_field()
    assert isinstance(iceberg_field.field_type, StructType)
    address = iceberg_field.field_type.field_by_name("address")
    assert isinstance(address.field_type, StructType)
    city = address.field_type.field_by_name("city")

    arrow_field = iceberg_into_arrow_field(
        iceberg_field,
        identifier_field_ids=[city.field_id],
    )
    arrow_city = arrow_field.type.field("address").type.field("city")
    roundtripped = into_iceberg_schema(arrow_field)

    assert arrow_city.metadata == {
        b"PARQUET:field_id": str(city.field_id).encode("ascii"),
        b"primary_key": b"true",
    }
    assert roundtripped.identifier_field_ids == [city.field_id]
    assert roundtripped.find_field("events.address.city") == city


def test_primary_metadata_survives_when_reverse_field_ids_are_omitted() -> None:
    iceberg_field = Event.into_iceberg_field()
    assert isinstance(iceberg_field.field_type, StructType)
    event_id = iceberg_field.field_type.field_by_name("eventId")

    arrow_field = iceberg_into_arrow_field(
        iceberg_field,
        include_field_id=False,
        identifier_field_ids=[event_id.field_id],
    )
    arrow_identifier = arrow_field.type.field("eventId")

    assert arrow_identifier.metadata == {b"primary_key": b"true"}
    rebuilt = into_iceberg_schema(arrow_field)
    assert len(rebuilt.identifier_field_ids) == 1
    assert rebuilt.find_field("events.eventId").field_id in rebuilt.identifier_field_ids


def test_record_into_arrow_schema_flattens_root_and_is_cached() -> None:
    schema = Event.into_arrow_schema()

    assert isinstance(schema, pa.Schema)
    assert schema is Event.into_arrow_schema()
    assert schema == into_arrow_schema(Event)
    assert schema.names == [
        "eventId",
        "address",
        "tags",
        "attributes",
        "pair",
        "created_at",
    ]
    assert "events" not in schema.names

    assert schema.field("eventId").nullable is False
    address = schema.field("address")
    assert address.nullable is False
    assert address.type.field("city").nullable is False
    assert address.type.field("zip").nullable is True

    tags = schema.field("tags")
    assert tags.nullable is False
    assert tags.type.value_field.nullable is True

    attributes = schema.field("attributes")
    assert attributes.type.key_field.nullable is False
    assert attributes.type.item_field.nullable is True

    pair = schema.field("pair")
    assert [child.name for child in pair.type] == ["_1", "_2"]
    assert pair.type.field("_1").nullable is False
    assert pair.type.field("_2").nullable is True
    assert schema.field("created_at").type == pa.timestamp("us", tz="UTC")


def test_record_into_iceberg_schema_maps_nested_types_and_primary_key() -> None:
    schema = Event.into_iceberg_schema()

    assert isinstance(schema, Schema)
    event_id = schema.find_field("eventId")
    assert isinstance(event_id.field_type, LongType)
    assert event_id.required is True
    assert schema.identifier_field_ids == [event_id.field_id]

    address = schema.find_field("address")
    assert isinstance(address.field_type, StructType)
    assert address.required is True
    assert schema.find_field("address.city").required is True
    assert schema.find_field("address.zip").required is False

    tags = schema.find_field("tags")
    assert isinstance(tags.field_type, ListType)
    assert tags.required is True
    assert tags.field_type.element_required is False
    assert isinstance(tags.field_type.element_type, StringType)

    attributes = schema.find_field("attributes")
    assert isinstance(attributes.field_type, MapType)
    assert attributes.required is True
    assert attributes.field_type.key_field.required is True
    assert attributes.field_type.value_required is False

    pair = schema.find_field("pair")
    assert isinstance(pair.field_type, StructType)
    assert schema.find_field("pair._1").required is True
    assert schema.find_field("pair._2").required is False
    assert isinstance(
        schema.find_field("created_at").field_type,
        TimestamptzType,
    )


def test_iceberg_schema_has_complete_deterministic_field_ids() -> None:
    first = Event.into_iceberg_schema()
    second = Event.into_iceberg_schema()
    first_ids = [first.find_field(name).field_id for name in first.column_names]
    second_ids = [second.find_field(name).field_id for name in second.column_names]

    assert first_ids == second_ids
    assert len(first_ids) == len(set(first_ids))
    assert all(0 < field_id <= 2_147_483_647 for field_id in first_ids)

    arrow_with_ids = first.as_arrow()
    arrow_ids = []
    for arrow_field in _deep_arrow_fields(arrow_with_ids):
        assert arrow_field.metadata is not None
        encoded_id = arrow_field.metadata.get(b"PARQUET:field_id")
        assert encoded_id is not None
        arrow_ids.append(int(encoded_id))
    assert set(arrow_ids) == set(first_ids)


def test_iceberg_arrow_logical_roundtrip() -> None:
    original = dataclass_into_iceberg_schema(PlainDataclass)
    roundtripped = pyarrow_to_schema(original.as_arrow(), format_version=2)

    assert roundtripped == original
    # PyIceberg uses large Arrow strings/lists on output, so the Iceberg
    # round-trip is the stable logical comparison rather than strict Arrow
    # equality with the original RKP schema.
    assert into_arrow_schema(original) == original.as_arrow()


def test_identity_aware_reverse_adapter_preserves_schema_and_primary_ids() -> None:
    original = Event.into_iceberg_schema(schema_id=17)
    arrow_schema = iceberg_into_arrow_schema(
        original,
        metadata={"owner": "rkp"},
    )

    assert arrow_schema.metadata == {
        b"owner": b"rkp",
        b"iceberg.schema_id": b"17",
    }
    assert arrow_schema.field("eventId").metadata == {
        b"PARQUET:field_id": str(original.find_field("eventId").field_id).encode(),
        b"primary_key": b"true",
    }
    assert into_iceberg_schema(arrow_schema) == original


def test_reverse_adapter_preserves_multiple_identifier_order() -> None:
    original = Schema(
        NestedField(1, "first", LongType(), required=True),
        NestedField(2, "second", StringType(), required=True),
        identifier_field_ids=[2, 1],
    )

    arrow_schema = iceberg_into_arrow_schema(original)

    assert arrow_schema.metadata == {
        b"iceberg.schema_id": b"0",
        b"iceberg.identifier_field_ids": b"2,1",
    }
    assert into_iceberg_schema(arrow_schema) == original


def test_reverse_without_ids_clears_numeric_identifier_metadata() -> None:
    original = Schema(
        NestedField(10, "first", LongType(), required=True),
        NestedField(20, "second", StringType(), required=True),
        identifier_field_ids=[20, 10],
    )

    arrow_schema = iceberg_into_arrow_schema(
        original,
        include_field_ids=False,
        metadata={b"iceberg.identifier_field_ids": b"stale"},
    )

    assert arrow_schema.metadata == {b"iceberg.schema_id": b"0"}
    rebuilt = into_iceberg_schema(arrow_schema)
    assert len(rebuilt.identifier_field_ids) == 2
    assert set(rebuilt.identifier_field_ids) == {
        rebuilt.find_field("first").field_id,
        rebuilt.find_field("second").field_id,
    }


def test_explicit_field_ids_are_preserved_and_auto_ids_do_not_collide() -> None:
    schema = ExplicitIds.into_iceberg_schema()
    ids = [schema.find_field(name).field_id for name in schema.column_names]

    assert schema.find_field("identifier").field_id == 101
    assert schema.find_field("child").field_id == 202
    assert schema.find_field("child.code").field_id == 303
    assert len(ids) == len(set(ids))
    assert all(field_id > 0 for field_id in ids)


def test_seq_drives_iceberg_ids_and_generated_ids_skip_global_reservations() -> None:
    first = SequencedParent.into_iceberg_schema()
    second = SequencedParent.into_iceberg_schema()

    assert first.find_field("explicit").field_id == 1
    assert first.find_field("child").field_id == 4
    assert first.find_field("child.explicit").field_id == 2

    ids = [first.find_field(name).field_id for name in first.column_names]
    assert len(ids) == len(set(ids))
    assert set(ids) == {
        second.find_field(name).field_id for name in second.column_names
    }


def test_duplicate_seq_is_rejected_across_nested_field_boundaries() -> None:
    with pytest.raises(ValueError, match=r"duplicate Iceberg field ID 17"):
        NestedDuplicateSeqParent.into_iceberg_schema()


def test_seq_projects_recursively_through_list_and_map_fields() -> None:
    schema = SequencedCollections.into_iceberg_schema()

    assert schema.find_field("items").field_id == 13
    assert schema.find_field("items.element").field_id == 14
    assert schema.find_field("lookup").field_id == 15
    assert schema.find_field("lookup.key").field_id == 16
    assert schema.find_field("lookup.value").field_id == 17


def test_duplicate_seq_is_rejected_inside_a_collection() -> None:
    with pytest.raises(ValueError, match=r"duplicate Iceberg field ID 31"):
        DuplicateCollectionSeq.into_iceberg_schema()


def test_seq_uses_iceberg_int32_bounds() -> None:
    maximum = 2_147_483_647

    @record
    class Maximum(Record):
        value: int = field(seq=maximum)

    assert Maximum.into_arrow_schema().field("value").metadata == {
        b"PARQUET:field_id": str(maximum).encode("ascii")
    }
    assert Maximum.into_iceberg_schema().find_field("value").field_id == maximum
    with pytest.raises(TypeError, match=rf"between 1 and {maximum}"):
        field(seq=maximum + 1)


def test_field_id_allocator_reports_exhaustion_at_int32_maximum() -> None:
    maximum = 2_147_483_647

    assert into_iceberg_field("last", int, field_id_start=maximum).field_id == maximum
    with pytest.raises(ValueError, match="no valid field seq values remain"):
        into_iceberg_field("pair", tuple[int, int], field_id_start=maximum)


def test_arrow_wire_identity_aliases_are_centralized_as_seq() -> None:
    legacy = pa.field(
        "value",
        pa.int64(),
        metadata={b"iceberg.id": b"77"},
    )
    matching = pa.field(
        "value",
        pa.int64(),
        metadata={
            b"PARQUET:field_id": b"78",
            b"iceberg.id": b"78",
        },
    )
    conflicting = pa.field(
        "value",
        pa.int64(),
        metadata={
            b"PARQUET:field_id": b"78",
            b"iceberg.id": b"79",
        },
    )

    assert arrow_into_iceberg_field(legacy).field_id == 77
    assert arrow_into_iceberg_field(matching).field_id == 78
    with pytest.raises(ValueError, match="conflicting field seq values"):
        arrow_into_iceberg_field(conflicting)


@pytest.mark.parametrize("encoded", [b"0", b"2147483648", b"true", b"1.5"])
def test_arrow_wire_seq_rejects_invalid_values(encoded: bytes) -> None:
    arrow_field = pa.field(
        "value",
        pa.int64(),
        metadata={b"PARQUET:field_id": encoded},
    )

    with pytest.raises(
        ValueError, match=r"(?i)(invalid|between).*field seq|field seq.*between"
    ):
        arrow_into_iceberg_field(arrow_field)


def test_duplicate_explicit_field_ids_are_rejected() -> None:
    with pytest.raises((TypeError, ValueError), match=r"(?i)duplicate|field.?id"):
        DuplicateIds.into_iceberg_schema()


def test_any_requires_iceberg_v3() -> None:
    with pytest.raises(
        (TypeError, ValueError),
        match=r"(?i)null|unknown|format.?version|version 2",
    ):
        DynamicValue.into_iceberg_schema(format_version=2)

    schema = DynamicValue.into_iceberg_schema(format_version=3)
    payload = schema.find_field("payload")
    assert isinstance(payload.field_type, UnknownType)
    assert payload.required is False


def test_existing_unknown_iceberg_field_still_requires_v3() -> None:
    unknown = NestedField(1, "payload", UnknownType(), required=False)

    with pytest.raises(TypeError, match=r"(?i)format version 2|incompatible"):
        into_iceberg_field(unknown, format_version=2)
    with pytest.raises(TypeError, match=r"(?i)format version 2|incompatible"):
        into_iceberg_schema(unknown, format_version=2)

    assert into_iceberg_field(unknown, format_version=3) is unknown
    assert into_iceberg_schema(unknown, format_version=3).fields == (unknown,)


def test_ordinary_dataclass_schema_utilities_match_generic_dispatch() -> None:
    arrow_schema = dataclass_into_arrow_schema(PlainDataclass)
    iceberg_schema = dataclass_into_iceberg_schema(PlainDataclass)

    assert arrow_schema == into_arrow_schema(PlainDataclass)
    assert iceberg_schema == into_iceberg_schema(PlainDataclass)

    event_arrow_schema = Event.into_arrow_schema()
    assert arrow_into_iceberg_schema(event_arrow_schema) == into_iceberg_schema(
        event_arrow_schema
    )
    assert arrow_schema.names == ["identifier", "label"]
    assert iceberg_schema.find_field("identifier").required is True
    assert iceberg_schema.find_field("label").required is False


def test_iceberg_dependency_is_lazy_and_arrow_remains_available() -> None:
    script = r"""
import builtins
real_import = builtins.__import__
def guarded(name, *args, **kwargs):
    if name.split(".", 1)[0] == "pyiceberg":
        raise ModuleNotFoundError("blocked pyiceberg", name="pyiceberg")
    return real_import(name, *args, **kwargs)
builtins.__import__ = guarded

from dataclasses import fields
from rkp import Record, field, into_iceberg_field, into_iceberg_schema, record

@record
class Value(Record):
    number: int = field()

assert Value.into_arrow_schema().names == ["number"]
try:
    Value.into_iceberg_schema()
except ImportError as error:
    assert "rkp[iceberg]" in str(error)
else:
    raise AssertionError("Iceberg call unexpectedly succeeded")

try:
    Value.into_iceberg_field()
except ImportError as error:
    assert "rkp[iceberg]" in str(error)
else:
    raise AssertionError("Iceberg field call unexpectedly succeeded")

try:
    into_iceberg_field(Value)
except ImportError as error:
    assert "rkp[iceberg]" in str(error)
else:
    raise AssertionError("Iceberg field utility unexpectedly succeeded")

try:
    fields(Value)[0].into_iceberg_field(owner=Value)
except ImportError as error:
    assert "rkp[iceberg]" in str(error)
else:
    raise AssertionError("RKP Field Iceberg call unexpectedly succeeded")

try:
    into_iceberg_schema(Value)
except ImportError as error:
    assert "rkp[iceberg]" in str(error)
else:
    raise AssertionError("Iceberg utility unexpectedly succeeded")
"""
    environment = dict(os.environ)
    source = str(Path(__file__).parents[1] / "src")
    environment["PYTHONPATH"] = source
    completed = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env=environment,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
