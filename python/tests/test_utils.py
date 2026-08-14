from __future__ import annotations

import datetime as dt
from dataclasses import InitVar, dataclass, field, fields
from typing import Annotated, ClassVar, NewType

import pyarrow as pa
import pytest
from rkp.utils import dataclass_into_arrow_field, into_arrow_field, into_arrow_type
from typing_extensions import TypeAliasType


@dataclass
class PlainDataclass:
    enabled: bool
    payload: bytes
    ignored_class_value: ClassVar[str] = "ignored"
    ignored_init_value: InitVar[str] = "ignored"


@dataclass
class ForwardContainer:
    child: PlainDataclass


@pytest.mark.parametrize(
    ("annotation", "expected"),
    [
        (bool, pa.bool_()),
        (int, pa.int64()),
        (float, pa.float64()),
        (str, pa.string()),
        (bytes, pa.binary()),
        (dt.date, pa.date32()),
        (dt.datetime, pa.timestamp("us", tz="UTC")),
        (dt.time, pa.time64("us")),
        (dt.timedelta, pa.duration("us")),
    ],
)
def test_into_arrow_type_maps_scalar_annotations(
    annotation: object, expected: pa.DataType
) -> None:
    assert into_arrow_type(annotation) == expected


def test_into_arrow_type_maps_collections_and_new_types() -> None:
    UserId = NewType("UserId", int)

    assert into_arrow_type(list[str]) == pa.list_(
        pa.field("item", pa.string(), nullable=False)
    )
    assert into_arrow_type(tuple[int, ...]) == pa.list_(
        pa.field("item", pa.int64(), nullable=False)
    )
    assert into_arrow_type(dict[str, int | None]) == pa.map_(
        pa.string(), pa.field("value", pa.int64(), nullable=True)
    )
    assert into_arrow_type(UserId) == pa.int64()


def test_into_arrow_field_infers_and_can_override_nullability() -> None:
    assert into_arrow_field("required", int) == pa.field(
        "required", pa.int64(), nullable=False
    )
    assert into_arrow_field("optional", int | None) == pa.field(
        "optional", pa.int64(), nullable=True
    )
    assert into_arrow_field("overridden", int, nullable=True).nullable


def test_into_arrow_field_accepts_a_dataclass_field() -> None:
    dataclass_field = fields(PlainDataclass)[0]

    assert into_arrow_field(dataclass_field) == pa.field(
        "enabled", pa.bool_(), nullable=False
    )


def test_into_arrow_field_resolves_a_postponed_field_with_its_owner() -> None:
    @dataclass
    class CollectionField:
        values: list[int]

    dataclass_field = fields(CollectionField)[0]

    assert into_arrow_field(dataclass_field, owner=CollectionField) == pa.field(
        "values",
        pa.list_(pa.field("item", pa.int64(), nullable=False)),
        nullable=False,
    )


def test_dataclass_into_arrow_field_uses_real_fields_and_resolved_hints() -> None:
    expected = pa.field(
        "forwardcontainer",
        pa.struct(
            [
                pa.field(
                    "child",
                    pa.struct(
                        [
                            pa.field("enabled", pa.bool_(), nullable=False),
                            pa.field("payload", pa.binary(), nullable=False),
                        ]
                    ),
                    nullable=False,
                )
            ]
        ),
        nullable=False,
    )

    assert dataclass_into_arrow_field(ForwardContainer) == expected


def test_annotated_keeps_its_underlying_type() -> None:
    assert into_arrow_type(Annotated[int, "application metadata"]) == pa.int64()


def test_literal_none_maps_to_a_nullable_null_field() -> None:
    assert into_arrow_field("missing", None) == pa.field(
        "missing", pa.null(), nullable=True
    )


def test_type_alias_preserves_optional_nullability() -> None:
    optional_alias = TypeAliasType("OptionalAlias", int | None)

    assert into_arrow_field("value", optional_alias) == pa.field(
        "value", pa.int64(), nullable=True
    )
    assert into_arrow_type(list[optional_alias]) == pa.list_(
        pa.field("item", pa.int64(), nullable=True)
    )


def test_dataclass_class_local_alias_is_resolved() -> None:
    @dataclass
    class WithAlias:
        Alias = int
        value: Alias

    assert dataclass_into_arrow_field(WithAlias).type == pa.struct(
        [pa.field("value", pa.int64(), nullable=False)]
    )


def test_subclass_alias_does_not_reinterpret_inherited_annotations() -> None:
    @dataclass
    class AliasBase:
        Alias = int
        base: Alias

    @dataclass
    class AliasChild(AliasBase):
        Alias = str
        child: Alias

    assert dataclass_into_arrow_field(AliasChild).type == pa.struct(
        [
            pa.field("base", pa.int64(), nullable=False),
            pa.field("child", pa.string(), nullable=False),
        ]
    )


def test_broad_annotations_use_the_closest_arrow_representation() -> None:
    assert into_arrow_type(object) == pa.null()
    assert into_arrow_type(list) == pa.list_(pa.field("item", pa.null(), nullable=True))
    assert into_arrow_type(dict) == pa.map_(
        pa.field("key", pa.string(), nullable=False),
        pa.field("value", pa.null(), nullable=True),
    )
    assert pa.types.is_union(into_arrow_type(int | str))
    assert into_arrow_type(tuple[int, str]) == pa.struct(
        [
            pa.field("_1", pa.int64(), nullable=False),
            pa.field("_2", pa.string(), nullable=False),
        ]
    )


def test_dataclass_any_like_field_uses_nullable_null_type() -> None:
    @dataclass
    class Unsupported:
        value: object = field(default_factory=object)

    assert dataclass_into_arrow_field(Unsupported).type[0] == pa.field(
        "value", pa.null(), nullable=True
    )


def test_dataclass_helper_rejects_non_dataclasses() -> None:
    with pytest.raises(TypeError, match="dataclass type"):
        dataclass_into_arrow_field(str)


def test_dataclass_helper_rejects_an_undecorated_dataclass_subclass() -> None:
    @dataclass
    class Base:
        base: int

    class UndecoratedChild(Base):
        child: str

    with pytest.raises(TypeError, match="decorated as a dataclass"):
        dataclass_into_arrow_field(UndecoratedChild)
