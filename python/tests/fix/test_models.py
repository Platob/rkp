from __future__ import annotations

import json
from dataclasses import MISSING, FrozenInstanceError, fields, is_dataclass
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from types import NoneType
from typing import get_args

import pytest
from rkp import Field, Record, field_options
from rkp.fix import FixDictionary, FixEnumValue, FixField, FixParseError


def _optional_members(annotation: object) -> frozenset[object]:
    return frozenset(get_args(annotation))


@pytest.mark.parametrize(
    ("fix_type", "python_type"),
    [
        ("String", str),
        ("char", str),
        ("Country", str),
        ("Price", Decimal),
        ("Qty", Decimal),
        ("Percentage", Decimal),
        ("int", int),
        ("Length", int),
        ("SeqNum", int),
        ("Boolean", bool),
        ("data", bytes),
        ("LocalMktDate", date),
        ("UTCDateOnly", date),
        ("UTCTimeOnly", time),
        ("TZTimeOnly", str),
        ("UTCTimestamp", datetime),
    ],
)
def test_fix_type_mapping_is_protocol_appropriate(
    fix_type: str, python_type: type[object]
) -> None:
    source = FixField(tag=1, name="Value", fix_type=fix_type, version="4.4")

    required = source.into_spec(required=True)
    optional = source.into_spec()

    assert required.annotation is python_type
    assert required.field.default is MISSING
    assert field_options(required.field).nullable is False
    assert _optional_members(optional.annotation) == {python_type, NoneType}
    assert optional.field.default is None
    assert field_options(optional.field).nullable is True


def test_enum_codes_remain_wire_values_and_field_metadata() -> None:
    source = FixField(
        tag=54,
        name="Side",
        fix_type="char",
        version="4.4",
        description="Side of order.",
        values=(
            FixEnumValue("1", "Buy"),
            FixEnumValue("2", "Sell"),
            FixEnumValue("A", "Cross short exempt"),
        ),
        source_url="https://www.onixs.biz/fix-dictionary/4.4/tagNum_54.html",
    )

    first = source.into_spec(required=True)
    second = source.into_spec(required=True)
    options = field_options(first.field)

    assert first.name == "side"
    assert first.annotation is str
    assert isinstance(first.field, Field)
    assert first.field is not second.field
    assert options.alias == "Side"
    assert options.seq == 54
    assert options.doc == "Side of order."
    assert options.nullable is False
    assert source.values == (
        FixEnumValue("1", "Buy"),
        FixEnumValue("2", "Sell"),
        FixEnumValue("A", "Cross short exempt"),
    )


def test_fix_models_are_frozen_and_reject_invalid_identity() -> None:
    value = FixEnumValue("1", "Buy")
    source = FixField(
        tag=54,
        name="Side",
        fix_type="char",
        version="4.4",
        values=(value,),
    )

    with pytest.raises(FrozenInstanceError):
        source.tag = 55  # type: ignore[misc]
    with pytest.raises((TypeError, ValueError), match=r"(?i)tag"):
        FixField(tag=0, name="Bad", fix_type="String", version="4.4")
    with pytest.raises((TypeError, ValueError), match=r"(?i)name"):
        FixField(tag=1, name="", fix_type="String", version="4.4")


def test_dictionary_sorts_fields_and_supports_constant_time_style_lookup() -> None:
    side = FixField(tag=54, name="Side", fix_type="char", version="4.4")
    client_order_id = FixField(tag=11, name="ClOrdID", fix_type="String", version="4.4")
    dictionary = FixDictionary(version="4.4", fields=(side, client_order_id))

    assert tuple(item.tag for item in dictionary.fields) == (11, 54)
    assert dictionary.field(11) is client_order_id
    assert dictionary.field("11") is client_order_id
    assert dictionary.field("ClOrdID") is client_order_id
    assert dictionary.field("clordid") is client_order_id
    with pytest.raises(KeyError):
        dictionary.field(9999)


@pytest.mark.parametrize(
    "fields_with_collision",
    [
        (
            FixField(tag=11, name="ClOrdID", fix_type="String", version="4.4"),
            FixField(tag=11, name="Other", fix_type="String", version="4.4"),
        ),
        (
            FixField(tag=11, name="ClOrdID", fix_type="String", version="4.4"),
            FixField(tag=12, name="ClOrdID", fix_type="String", version="4.4"),
        ),
    ],
)
def test_dictionary_rejects_ambiguous_fields(
    fields_with_collision: tuple[FixField, FixField],
) -> None:
    with pytest.raises((TypeError, ValueError), match=r"(?i)duplicate|unique"):
        FixDictionary(version="4.4", fields=fields_with_collision)


def test_dictionary_normalizes_version_and_validates_source_url() -> None:
    source = FixField(54, "Side", "char", "4.4")

    dictionary = FixDictionary(" 4.4 ", (source,))

    assert dictionary.version == "4.4"
    with pytest.raises(TypeError, match="source_url"):
        FixDictionary("4.4", (source,), source_url=123)  # type: ignore[arg-type]


@pytest.mark.parametrize("option", ["required", "fields"])
def test_dictionary_rejects_scalar_selector_strings(option: str) -> None:
    dictionary = FixDictionary("4.4", (FixField(54, "Side", "char", "4.4"),))

    with pytest.raises(TypeError, match="iterable"):
        dictionary.specs(**{option: "Side"})


def test_dictionary_snapshot_is_deterministic_and_round_trips(tmp_path: Path) -> None:
    path = tmp_path / "fix-4.4.json"
    dictionary = FixDictionary(
        version="4.4",
        fields=(
            FixField(
                tag=54,
                name="Side",
                fix_type="char",
                version="4.4",
                description="Side of order.",
                values=(FixEnumValue("1", "Buy"), FixEnumValue("2", "Sell")),
                source_url=("https://www.onixs.biz/fix-dictionary/4.4/tagNum_54.html"),
            ),
        ),
        source_url="https://www.onixs.biz/fix-dictionary.html",
    )

    dictionary.dump(path)
    initial = path.read_bytes()
    restored = FixDictionary.load(path)
    restored.dump(path)

    assert restored == dictionary
    assert path.read_bytes() == initial
    payload = json.loads(initial)
    assert payload["version"] == "4.4"
    assert payload["fields"][0]["tag"] == 54
    assert payload["fields"][0]["values"] == [
        {"value": "1", "description": "Buy"},
        {"value": "2", "description": "Sell"},
    ]


def test_dictionary_snapshot_wraps_corrupt_gzip_as_parse_error(tmp_path: Path) -> None:
    path = tmp_path / "corrupt.json.gz"
    path.write_bytes(b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x03notdeflate")

    with pytest.raises(FixParseError, match="cannot load FIX dictionary snapshot"):
        FixDictionary.load(path)


def test_dictionary_snapshot_wraps_parent_creation_errors(tmp_path: Path) -> None:
    parent = tmp_path / "not-a-directory"
    parent.write_text("occupied", encoding="utf-8")
    dictionary = FixDictionary("4.4", ())

    with pytest.raises(FixParseError, match="cannot write FIX dictionary snapshot"):
        dictionary.dump(parent / "dictionary.json")


def test_dictionary_builds_a_real_record_with_fresh_rkp_fields() -> None:
    dictionary = FixDictionary(
        version="4.4",
        fields=(
            FixField(tag=11, name="ClOrdID", fix_type="String", version="4.4"),
            FixField(tag=44, name="Price", fix_type="Price", version="4.4"),
            FixField(tag=54, name="Side", fix_type="char", version="4.4"),
        ),
    )

    Order = dictionary.into_record(
        "NewOrderSingle",
        required=(11, "Side"),
    )

    assert is_dataclass(Order)
    assert issubclass(Order, Record)
    assert [item.name for item in fields(Order)] == ["cl_ord_id", "price", "side"]
    assert [item.seq for item in fields(Order)] == [11, 44, 54]
    assert [field_options(item).alias for item in fields(Order)] == [
        "ClOrdID",
        "Price",
        "Side",
    ]
    order = Order(cl_ord_id="client-1", side="1")
    assert order.price is None
    assert order.dumps_json() == ('{"ClOrdID": "client-1", "Price": null, "Side": "1"}')


def test_dictionary_can_select_and_rename_generated_fields() -> None:
    dictionary = FixDictionary(
        version="4.4",
        fields=(
            FixField(tag=11, name="ClOrdID", fix_type="String", version="4.4"),
            FixField(tag=54, name="Side", fix_type="char", version="4.4"),
        ),
    )

    Identifiers = dictionary.into_record(
        name="Identifiers",
        fields=(11,),
        required=(11,),
    )

    assert [item.name for item in fields(Identifiers)] == ["cl_ord_id"]
    assert Identifiers(cl_ord_id="a").dumps_json() == '{"ClOrdID": "a"}'


def test_generated_record_preserves_fix_identity_through_arrow() -> None:
    dictionary = FixDictionary(
        version="4.4",
        fields=(
            FixField(
                tag=54,
                name="Side",
                fix_type="char",
                version="4.4",
                description="Side of order.",
                values=(FixEnumValue("1", "Buy"), FixEnumValue("2", "Sell")),
            ),
        ),
    )

    Order = dictionary.into_record("Order", required=(54,))
    schema = Order.into_arrow_schema()
    side = schema.field("Side")

    assert schema.names == ["Side"]
    assert side.nullable is False
    assert side.metadata is not None
    assert side.metadata[b"fix.version"] == b"4.4"
    assert side.metadata[b"fix.tag"] == b"54"
    assert side.metadata[b"fix.type"] == b"char"
    assert side.metadata[b"fix.values"] == b'[["1","Buy"],["2","Sell"]]'
    assert side.metadata[b"doc"] == b"Side of order."
    assert side.metadata[b"PARQUET:field_id"] == b"54"


def test_generated_record_disambiguates_python_identifier_collisions() -> None:
    dictionary = FixDictionary(
        version="4.4",
        fields=(
            FixField(1, "Settl-Date", "LocalMktDate", "4.4"),
            FixField(2, "SettlDate", "LocalMktDate", "4.4"),
        ),
    )

    Generated = dictionary.into_record("Generated")

    assert [item.name for item in fields(Generated)] == [
        "settl_date",
        "settl_date_2",
    ]
    assert [field_options(item).alias for item in fields(Generated)] == [
        "Settl-Date",
        "SettlDate",
    ]


def test_generated_record_disambiguates_secondary_name_collisions() -> None:
    dictionary = FixDictionary(
        version="4.4",
        fields=(
            FixField(1, "Foo-Bar-3", "String", "4.4"),
            FixField(2, "Foo-Bar", "String", "4.4"),
            FixField(3, "FooBar", "String", "4.4"),
        ),
    )

    Generated = dictionary.into_record("Generated")

    assert [(item.name, item.seq) for item in fields(Generated)] == [
        ("foo_bar_3", 1),
        ("foo_bar", 2),
        ("foo_bar_3_2", 3),
    ]


def test_generated_records_cannot_disable_keyword_only_safety() -> None:
    dictionary = FixDictionary(
        "4.4",
        (
            FixField(1, "Optional", "String", "4.4"),
            FixField(2, "Required", "String", "4.4"),
        ),
    )

    with pytest.raises(ValueError, match="keyword-only"):
        dictionary.into_record("Unsafe", required=(2,), kw_only=False)
