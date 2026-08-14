from __future__ import annotations

from dataclasses import FrozenInstanceError, fields, is_dataclass

import pytest
from rkp import Record, field, record


@record
class Coordinates(Record):
    latitude: float
    longitude: float


def test_record_is_a_dataclass_decorator() -> None:
    point = Coordinates(48.8566, 2.3522)

    assert is_dataclass(Coordinates)
    assert point == Coordinates(latitude=48.8566, longitude=2.3522)
    assert repr(point).startswith("Coordinates(")


def test_record_forwards_dataclass_options() -> None:
    @record(frozen=True, order=True, slots=True, kw_only=True)
    class Version(Record):
        major: int
        minor: int = 0

    first = Version(major=1)
    second = Version(major=2)

    assert first < second
    assert not hasattr(first, "__dict__")
    with pytest.raises(FrozenInstanceError):
        first.major = 3  # type: ignore[misc]


def test_record_requires_the_record_base() -> None:
    with pytest.raises(TypeError, match="Record subclasses"):

        @record
        class NotARecord:
            value: int


def test_record_inheritance_preserves_dataclass_field_order() -> None:
    @record
    class Entity(Record):
        identifier: int

    @record
    class NamedEntity(Entity):
        name: str

    assert [field.name for field in fields(NamedEntity)] == ["identifier", "name"]


def test_undecorated_record_has_a_clear_error() -> None:
    class Undecorated(Record):
        value: int

    with pytest.raises(AttributeError, match="into_arrow_field"):
        Undecorated.into_arrow_field()


def test_record_decorator_supports_class_alias_and_codec_options() -> None:
    @record(alias="events", with_json=False, with_yaml=False, slots=True)
    class Event(Record):
        value: int

    assert Event.alias == "events"
    assert not hasattr(Event, "__dict__") or not hasattr(Event(1), "__dict__")
    assert not hasattr(Event, "loads_json")
    assert not hasattr(Event, "loads_yaml")


def test_record_rejects_an_alias_that_ambiguously_shadows_a_native_name() -> None:
    class Ambiguous(Record):
        first: int = field(alias="second")
        second: int = field(alias="third")

    with pytest.raises(
        TypeError,
        match=(
            r"ambiguous input field name 'second'.*"
            r"Ambiguous\.first.*Ambiguous\.second"
        ),
    ):
        record(Ambiguous)


def test_noncolliding_aliases_round_trip_through_dict_json_and_yaml() -> None:
    @record
    class Aliased(Record):
        first: int = field(alias="wire_first")
        second: str = field(alias="wire_second")

    value = Aliased(1, "two")

    assert Aliased.from_dict({"first": "1", "second": "two"}) == value
    assert Aliased.loads_json(value.dumps_json()) == value
    assert Aliased.loads_yaml(value.dumps_yaml()) == value
