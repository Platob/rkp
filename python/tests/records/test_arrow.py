from __future__ import annotations

import collections
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from datetime import datetime
from typing import Annotated, Any, Generic, Literal, NamedTuple, TypedDict, TypeVar

import pyarrow as pa
import pytest
from rkp import (
    Record,
    dataclass_into_arrow_field,
    field,
    into_arrow_field,
    into_arrow_type,
    record,
)


@record
class Coordinates(Record):
    latitude: float
    longitude: float


@record
class Place(Record):
    name: str
    coordinates: Coordinates
    population: int | None = None


def test_record_infers_an_ordered_nested_struct() -> None:
    expected = pa.field(
        "place",
        pa.struct(
            [
                pa.field("name", pa.string(), nullable=False),
                pa.field(
                    "coordinates",
                    pa.struct(
                        [
                            pa.field("latitude", pa.float64(), nullable=False),
                            pa.field("longitude", pa.float64(), nullable=False),
                        ]
                    ),
                    nullable=False,
                ),
                pa.field("population", pa.int64(), nullable=True),
            ]
        ),
        nullable=False,
    )

    assert Place.into_arrow_field() == expected
    assert Place("Paris", Coordinates(48.8566, 2.3522)).into_arrow_field() == expected


def test_record_arrow_field_is_cached_per_class() -> None:
    first = Place.into_arrow_field()

    assert Place.into_arrow_field() is first
    assert Coordinates.into_arrow_field() is Coordinates.into_arrow_field()
    assert Coordinates.into_arrow_field() is not first


def test_standard_dataclass_subclass_uses_the_public_arrow_utility() -> None:
    @dataclass
    class StandardDataclass(Record):
        value: int

    assert dataclass_into_arrow_field(StandardDataclass) == pa.field(
        "standarddataclass",
        pa.struct([pa.field("value", pa.int64(), nullable=False)]),
        nullable=False,
    )


def test_local_postponed_record_references_are_resolved() -> None:
    @record
    class LocalChild(Record):
        value: int

    @record()
    class LocalParent(Record):
        child: LocalChild

    assert LocalParent.into_arrow_field() == pa.field(
        "localparent",
        pa.struct(
            [
                pa.field(
                    "child",
                    pa.struct([pa.field("value", pa.int64(), nullable=False)]),
                    nullable=False,
                )
            ]
        ),
        nullable=False,
    )


def test_records_nested_in_a_class_resolve_sibling_names() -> None:
    class Namespace:
        @record
        class Child(Record):
            value: int

        @record
        class Parent(Record):
            child: Child  # noqa: F821

    assert Namespace.Parent.into_arrow_field().type == pa.struct(
        [
            pa.field(
                "child",
                pa.struct([pa.field("value", pa.int64(), nullable=False)]),
                nullable=False,
            )
        ]
    )


def test_default_record_name_is_lowered_and_alias_can_override_it() -> None:
    @record
    class HTTPEvent(Record):
        value: int

    @record(alias="events")
    class AliasedEvent(Record):
        value: int

    assert HTTPEvent.into_arrow_field().name == "httpevent"
    assert AliasedEvent.into_arrow_field().name == "events"
    assert AliasedEvent.into_arrow_field("override").name == "override"


def test_arrow_datetime_uses_utc_and_non_optional_is_not_nullable() -> None:
    assert into_arrow_field("created", datetime) == pa.field(
        "created", pa.timestamp("us", tz="UTC"), nullable=False
    )
    assert into_arrow_field("optional", datetime | None).nullable is True
    assert into_arrow_field("explicit", datetime, nullable=True).nullable is True


def test_fixed_tuple_is_struct_and_variadic_tuple_is_list() -> None:
    assert into_arrow_type(tuple[int, str | None]) == pa.struct(
        [
            pa.field("_1", pa.int64(), nullable=False),
            pa.field("_2", pa.string(), nullable=True),
        ]
    )
    assert into_arrow_type(tuple[int, ...]) == pa.list_(
        pa.field("item", pa.int64(), nullable=False)
    )


def test_broad_collections_keep_nested_item_nullability() -> None:
    @dataclass
    class Child:
        value: int

    nested = pa.struct([pa.field("value", pa.int64(), nullable=False)])
    assert into_arrow_type(collections.deque[Child | None]) == pa.list_(
        pa.field("item", nested, nullable=True)
    )
    assert into_arrow_type(dict[str, Child | None]) == pa.map_(
        pa.field("key", pa.string(), nullable=False),
        pa.field("value", nested, nullable=True),
    )
    assert into_arrow_type(set[str]) == pa.list_(
        pa.field("item", pa.string(), nullable=False)
    )


def test_any_and_object_use_nullable_null_type() -> None:
    assert into_arrow_field("anything", Any) == pa.field(
        "anything", pa.null(), nullable=True
    )
    assert into_arrow_field("object", object) == pa.field(
        "object", pa.null(), nullable=True
    )
    assert into_arrow_type(list[Any]) == pa.list_(
        pa.field("item", pa.null(), nullable=True)
    )
    assert into_arrow_type(dict) == pa.map_(
        pa.field("key", pa.string(), nullable=False),
        pa.field("value", pa.null(), nullable=True),
    )


def test_custom_field_controls_type_alias_nullability_and_metadata() -> None:
    @record
    class User(Record):
        identifier: int = field(
            alias="userId",
            type=pa.uint64(),
            nullable=False,
            metadata={"source": "api", "rank": 2},
            primary_key=True,
            partition_key="day",
            index_key=0,
        )

    child = User.into_arrow_field().type[0]
    assert child.name == "userId"
    assert child.type == pa.uint64()
    assert child.nullable is False
    assert child.metadata == {
        b"source": b"api",
        b"rank": b"2",
        b"primary_key": b"true",
        b"partition_key": b"day",
        b"index_key": b"0",
    }


def test_primary_key_cannot_be_nullable() -> None:
    @record
    class BadKey(Record):
        value: int | None = field(primary_key=True)

    with pytest.raises(TypeError, match="primary key"):
        BadKey.into_arrow_field()


def test_annotated_accepts_arrow_types_fields_and_configuration() -> None:
    annotation = Annotated[
        int,
        pa.field(
            "wire",
            pa.uint32(),
            nullable=True,
            metadata={"source": "annotation"},
        ),
        {"rkp": {"roles": {"index": True}}},
    ]
    result = into_arrow_field("value", annotation)
    assert result.name == "wire"
    assert result.type == pa.uint32()
    assert result.nullable is True
    assert result.metadata == {
        b"source": b"annotation",
        b"index_key": b"true",
    }


def test_type_factory_and_parameters_use_canonical_metadata() -> None:
    @record
    class Event(Record):
        created: datetime = field(
            metadata={
                "rkp": {
                    "type": pa.timestamp,
                    "parameters": {"unit": "ms", "tz": "Europe/Paris"},
                }
            },
        )

    assert Event.into_arrow_field().type[0].type == pa.timestamp(
        "ms", tz="Europe/Paris"
    )


def test_typed_dict_named_tuple_literal_and_union_hints() -> None:
    class Details(TypedDict):
        required: int
        optional: str | None

    class Pair(NamedTuple):
        left: int
        right: str

    assert into_arrow_type(Details) == pa.struct(
        [
            pa.field("required", pa.int64(), nullable=False),
            pa.field("optional", pa.string(), nullable=True),
        ]
    )
    assert into_arrow_type(Pair) == pa.struct(
        [
            pa.field("left", pa.int64(), nullable=False),
            pa.field("right", pa.string(), nullable=False),
        ]
    )
    assert into_arrow_type(Literal[1, 2]) == pa.int64()
    assert pa.types.is_union(into_arrow_type(int | str))


def test_nested_record_field_keeps_containing_alias() -> None:
    @record(alias="children")
    class Child(Record):
        value: int

    @record
    class Parent(Record):
        child: Child = field(alias="nested")

    assert Parent.into_arrow_field().type[0].name == "nested"


def test_record_arrow_field_is_cached_per_signature_and_class() -> None:
    @record
    class Cached(Record):
        value: int

    assert Cached.into_arrow_field() is Cached.into_arrow_field()
    assert Cached.into_arrow_field("other") is Cached.into_arrow_field("other")


def test_recursive_dataclass_reports_the_cycle() -> None:
    @record
    class Node(Record):
        child: Node | None

    with pytest.raises(TypeError, match="recursive"):
        Node.into_arrow_field()


def test_parameterized_generic_dataclass_substitutes_typevars() -> None:
    item_type = TypeVar("item_type")

    @dataclass
    class Box(Generic[item_type]):
        value: item_type

    assert into_arrow_type(Box[int]) == pa.struct(
        [pa.field("value", pa.int64(), nullable=False)]
    )


def test_counter_and_items_view_collection_hints() -> None:
    from collections.abc import ItemsView

    assert into_arrow_type(collections.Counter[str]) == pa.map_(
        pa.field("key", pa.string(), nullable=False),
        pa.field("value", pa.int64(), nullable=False),
    )
    assert into_arrow_type(ItemsView[str, int]) == pa.list_(
        pa.field(
            "item",
            pa.struct(
                [
                    pa.field("_1", pa.string(), nullable=False),
                    pa.field("_2", pa.int64(), nullable=False),
                ]
            ),
            nullable=False,
        )
    )


def test_higher_precedence_field_override_clears_factory_parameters() -> None:
    @record
    class Override(Record):
        value: Annotated[
            int,
            {
                "rkp": {
                    "type": pa.timestamp,
                    "parameters": {"unit": "ms"},
                }
            },
        ] = field(type=pa.int64())

    assert Override.into_arrow_field().type[0].type == pa.int64()


def test_explicit_false_key_flag_clears_annotated_true() -> None:
    @record
    class Override(Record):
        value: Annotated[int, {"rkp": {"roles": {"primary": True}}}] = field(
            primary_key=False
        )

    assert Override.into_arrow_field().type[0].metadata is None


def test_explicit_controls_clear_annotated_arrow_field_values() -> None:
    annotated_field = pa.field(
        "legacy",
        pa.int64(),
        nullable=True,
        metadata={
            b"doc": b"old",
            b"primary_key": b"true",
            b"partition_key": b"day",
            b"index_key": b"true",
        },
    )

    @record
    class Cleared(Record):
        value: Annotated[int, annotated_field] = field(
            alias=None,
            nullable=None,
            doc=None,
            primary_key=False,
            partition_key=False,
            index_key=False,
        )

    result = Cleared.into_arrow_field().type[0]
    assert result.name == "value"
    assert result.nullable is False
    assert result.metadata is None


def test_seq_projects_to_arrow_field_identity_metadata() -> None:
    @record
    class Sequenced(Record):
        identifier: int = field(seq=701)

    assert Sequenced.into_arrow_field().type[0].metadata == {
        b"PARQUET:field_id": b"701"
    }


def test_explicit_none_seq_clears_annotated_identity() -> None:
    @record
    class Cleared(Record):
        value: Annotated[int, {"rkp": {"seq": 701}}] = field(seq=None)

    assert Cleared.into_arrow_field().type[0].metadata is None


def test_explicit_none_seq_clears_identity_from_annotated_arrow_field() -> None:
    annotated_field = pa.field(
        "wire",
        pa.int64(),
        metadata={
            b"PARQUET:field_id": b"701",
            b"iceberg.id": b"701",
        },
    )

    @record
    class Cleared(Record):
        value: Annotated[int, annotated_field] = field(seq=None)

    assert Cleared.into_arrow_field().type[0].metadata is None


def test_explicit_none_seq_clears_identity_from_payload_metadata() -> None:
    @record
    class Cleared(Record):
        value: int = field(
            seq=None,
            metadata={
                b"PARQUET:field_id": b"701",
                b"iceberg.id": b"701",
            },
        )

    assert Cleared.into_arrow_field().type[0].metadata is None


def test_explicit_seq_overrides_annotated_identity() -> None:
    @record
    class Overridden(Record):
        value: Annotated[int, {"rkp": {"seq": 701}}] = field(seq=702)

    assert Overridden.into_arrow_field().type[0].metadata == {
        b"PARQUET:field_id": b"702"
    }


def test_standard_dataclass_seq_metadata_uses_the_same_arrow_projection() -> None:
    @dataclass
    class Standard:
        value: int = dataclass_field(metadata={"rkp": {"seq": 702}})

    assert dataclass_into_arrow_field(Standard).type[0].metadata == {
        b"PARQUET:field_id": b"702"
    }
