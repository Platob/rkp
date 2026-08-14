from __future__ import annotations

import dataclasses
import json
from pathlib import Path
from types import NoneType
from typing import get_args, get_type_hints

import pyarrow as pa
import pytest
from rkp import field_options, record_metadata
from rkp.fix import FixParseError
from rkp.fix._models import (
    FixComponent,
    FixComponentMember,
    FixDictionary,
    FixField,
    FixFieldMember,
    FixMessage,
    FixRepeatingGroup,
)


def _dictionary() -> FixDictionary:
    version = "4.4"
    return FixDictionary(
        version,
        (
            FixField(11, "ClOrdID", "String", version, "Client order ID."),
            FixField(54, "Side", "char", version),
            FixField(55, "Symbol", "String", version, "Instrument symbol."),
            FixField(447, "PartyIDSource", "char", version),
            FixField(448, "PartyID", "String", version),
            FixField(452, "PartyRole", "int", version),
            FixField(453, "NoPartyIDs", "NumInGroup", version),
        ),
        "https://www.onixs.biz/fix-dictionary/4.4/",
        components=(
            FixComponent(
                "Instrument",
                version,
                (FixFieldMember(55, required=True, comment="Security symbol."),),
                "Instrument identity.",
                "https://www.onixs.biz/fix-dictionary/4.4/compBlock_Instrument.html",
            ),
            FixComponent(
                "PartyIdentity",
                version,
                (
                    FixFieldMember(448, required=True),
                    FixFieldMember(447),
                ),
            ),
        ),
        messages=(
            FixMessage(
                "New Order Single",
                "D",
                version,
                (
                    FixFieldMember(11, required=True),
                    FixComponentMember("Instrument", required=True),
                    FixFieldMember(54, required=True),
                    FixRepeatingGroup(
                        453,
                        (
                            FixComponentMember("PartyIdentity", required=True),
                            FixFieldMember(452, required=True),
                        ),
                        comment="Parties on the order.",
                    ),
                ),
                "Order entry message.",
                "https://www.onixs.biz/fix-dictionary/4.4/msgType_D_68.html",
            ),
            FixMessage(
                "Order Cancel Request",
                "F",
                version,
                (FixComponentMember("Instrument", required=True),),
            ),
        ),
    )


def _concrete(annotation: object) -> object:
    return next(item for item in get_args(annotation) if item is not NoneType)


def test_structure_models_are_normalized_immutable_and_indexed() -> None:
    dictionary = _dictionary()

    assert dictionary.component("instrument") is dictionary.components[0]
    assert dictionary.message("D") is dictionary.message("new order single")
    with pytest.raises(KeyError, match="unknown FIX message"):
        dictionary.message("d")
    with pytest.raises(dataclasses.FrozenInstanceError):
        dictionary.message("D").name = "Changed"  # type: ignore[misc]
    with pytest.raises(KeyError, match="component"):
        dictionary.component("Unknown")
    with pytest.raises(KeyError, match="message"):
        dictionary.message("Z")


def test_dictionary_validates_structure_references_groups_and_cycles() -> None:
    field_definition = FixField(1, "NotAGroup", "String", "4.4")

    with pytest.raises(ValueError, match="unknown FIX tag 2"):
        FixDictionary(
            "4.4",
            (field_definition,),
            messages=(FixMessage("Bad", "Z", "4.4", (FixFieldMember(2),)),),
        )
    with pytest.raises(ValueError, match="unknown FIX component"):
        FixDictionary(
            "4.4",
            (field_definition,),
            messages=(FixMessage("Bad", "Z", "4.4", (FixComponentMember("Missing"),)),),
        )
    with pytest.raises(ValueError, match="NumInGroup"):
        FixDictionary(
            "4.4",
            (field_definition,),
            messages=(
                FixMessage(
                    "Bad",
                    "Z",
                    "4.4",
                    (FixRepeatingGroup(1, (FixFieldMember(1),)),),
                ),
            ),
        )

    first = FixComponent("First", "4.4", (FixComponentMember("Second"),))
    second = FixComponent("Second", "4.4", (FixComponentMember("First"),))
    with pytest.raises(ValueError, match=r"cyclic.*First.*Second.*First"):
        FixDictionary("4.4", (), components=(first, second))


def test_repeating_group_requires_non_empty_members() -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        FixRepeatingGroup(453, ())


def test_message_projection_builds_nested_component_and_repeating_records() -> None:
    dictionary = _dictionary()
    Order = dictionary.into_message_record("D")
    assert dictionary.into_message_record("D") is Order
    hints = get_type_hints(Order)
    Instrument = hints["instrument"]
    group_annotation = _concrete(hints["no_party_ids"])
    GroupEntry = get_args(group_annotation)[0]
    PartyIdentity = get_type_hints(GroupEntry)["party_identity"]
    Cancel = dictionary.into_message_record("F")

    assert Order.__name__ == "NewOrderSingle"
    assert Order.alias == "New Order Single"
    assert Instrument.alias == "Instrument"
    assert dictionary.into_component_record("Instrument") is Instrument
    assert get_type_hints(Cancel)["instrument"] is Instrument
    assert [item.name for item in dataclasses.fields(Order)] == [
        "cl_ord_id",
        "instrument",
        "side",
        "no_party_ids",
    ]
    assert [field_options(item).alias for item in dataclasses.fields(Order)] == [
        "ClOrdID",
        "Instrument",
        "Side",
        "NoPartyIDs",
    ]
    assert [item.seq for item in dataclasses.fields(Order)] == [11, None, 54, 453]
    group_field = dataclasses.fields(Order)[-1]
    assert field_options(group_field).doc == "Parties on the order."
    assert field_options(group_field).payload_metadata["fix.repeating"] is True

    order = Order(
        cl_ord_id="client-1",
        instrument=Instrument(symbol="AAPL"),
        side="1",
        no_party_ids=(
            GroupEntry(
                party_identity=PartyIdentity(
                    party_id="broker",
                    party_id_source="D",
                ),
                party_role=1,
            ),
        ),
    )
    encoded = order.dumps_json()
    restored = Order.loads_json(encoded)

    assert restored == order
    assert json.loads(encoded) == {
        "ClOrdID": "client-1",
        "Instrument": {"Symbol": "AAPL"},
        "Side": "1",
        "NoPartyIDs": [
            {
                "PartyIdentity": {
                    "PartyID": "broker",
                    "PartyIDSource": "D",
                },
                "PartyRole": 1,
            }
        ],
    }
    with pytest.raises(TypeError):
        Order("client-1", Instrument(symbol="AAPL"), "1")  # type: ignore[misc]


def test_nested_projection_preserves_arrow_metadata_and_round_trips_batch() -> None:
    dictionary = _dictionary()
    Order = dictionary.into_message_record("D")
    hints = get_type_hints(Order)
    Instrument = hints["instrument"]
    GroupEntry = get_args(_concrete(hints["no_party_ids"]))[0]
    PartyIdentity = get_type_hints(GroupEntry)["party_identity"]
    value = Order(
        cl_ord_id="client-1",
        instrument=Instrument(symbol="IBM"),
        side="2",
        no_party_ids=(
            GroupEntry(
                party_identity=PartyIdentity(party_id="venue"),
                party_role=3,
            ),
        ),
    )

    schema = Order.into_arrow_schema()
    instrument = schema.field("Instrument")
    groups = schema.field("NoPartyIDs")
    batch = Order.into_arrow_batch((value,))

    assert pa.types.is_struct(instrument.type)
    assert instrument.type.names == ["Symbol"]
    assert pa.types.is_list(groups.type)
    assert groups.type.value_type.names == ["PartyIdentity", "PartyRole"]
    assert groups.metadata is not None
    assert groups.metadata[b"fix.kind"] == b"repeating_group"
    assert groups.metadata[b"fix.comment"] == b"Parties on the order."
    assert schema.metadata is not None
    assert schema.metadata[b"fix.msg_type"] == b"D"
    assert tuple(Order.from_arrow_batch(batch)) == (value,)


def test_component_projection_has_fix_record_metadata_and_custom_name() -> None:
    dictionary = _dictionary()
    Instrument = dictionary.into_component_record("Instrument", name="Security")
    assert dictionary.into_component_record("Instrument", name="Security") is Instrument
    metadata = record_metadata(Instrument)

    assert Instrument.__name__ == "Security"
    assert Instrument.alias == "Instrument"
    assert metadata.metadata["fix.kind"] == "component"
    assert metadata.metadata["fix.name"] == "Instrument"
    assert Instrument(symbol="MSFT").dumps_json() == '{"Symbol": "MSFT"}'


def test_projection_disambiguates_member_identifiers_and_rejects_duplicate_seq() -> (
    None
):
    dictionary = FixDictionary(
        "4.4",
        (
            FixField(1, "Foo-Bar-3", "String", "4.4"),
            FixField(2, "Foo-Bar", "String", "4.4"),
            FixField(3, "FooBar", "String", "4.4"),
        ),
        messages=(
            FixMessage(
                "Names",
                "N",
                "4.4",
                (FixFieldMember(1), FixFieldMember(2), FixFieldMember(3)),
            ),
        ),
    )

    Names = dictionary.into_message_record("N")
    assert [item.name for item in dataclasses.fields(Names)] == [
        "foo_bar_3",
        "foo_bar",
        "foo_bar_3_2",
    ]

    duplicate = FixDictionary(
        "4.4",
        (FixField(1, "Value", "String", "4.4"),),
        messages=(
            FixMessage(
                "Duplicate",
                "X",
                "4.4",
                (FixFieldMember(1), FixFieldMember(1)),
            ),
        ),
    )
    with pytest.raises(ValueError, match="duplicate FIX tag 1"):
        duplicate.into_message_record("X")


def test_structure_snapshot_v2_round_trips_and_v1_remains_loadable(
    tmp_path: Path,
) -> None:
    dictionary = _dictionary()
    path = tmp_path / "dictionary.json.gz"
    dictionary.dump(path)

    restored = FixDictionary.load(path)
    assert restored == dictionary
    assert restored.message("D").members[-1] == dictionary.message("D").members[-1]

    legacy_path = tmp_path / "legacy.json"
    legacy_path.write_text(
        json.dumps(
            {
                "format": "rkp.fix.dictionary",
                "format_version": 1,
                "version": "4.4",
                "source_url": "legacy",
                "fields": [
                    {
                        "tag": 54,
                        "name": "Side",
                        "type": "char",
                        "description": "",
                        "values": [],
                        "source_url": "",
                        "status": None,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    legacy = FixDictionary.load(legacy_path)

    assert legacy.field(54).name == "Side"
    assert legacy.components == ()
    assert legacy.messages == ()


@pytest.mark.parametrize("format_version", [True, False, 3])
def test_structure_snapshot_rejects_invalid_format_versions(
    tmp_path: Path, format_version: object
) -> None:
    path = tmp_path / "invalid-version.json"
    path.write_text(
        json.dumps(
            {
                "format": "rkp.fix.dictionary",
                "format_version": format_version,
                "version": "4.4",
                "fields": [],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(FixParseError, match="unsupported.*version"):
        FixDictionary.load(path)


def test_structure_snapshot_rejects_excessive_member_depth(tmp_path: Path) -> None:
    member: dict[str, object] = {
        "kind": "field",
        "tag": 1,
    }
    for _ in range(66):
        member = {
            "kind": "repeating_group",
            "tag": 1,
            "members": [member],
        }
    path = tmp_path / "too-deep.json"
    path.write_text(
        json.dumps(
            {
                "format": "rkp.fix.dictionary",
                "format_version": 2,
                "version": "4.4",
                "fields": [{"tag": 1, "name": "Group", "type": "NumInGroup"}],
                "components": [],
                "messages": [
                    {
                        "name": "Deep",
                        "msg_type": "Z",
                        "members": [member],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(FixParseError, match="maximum depth"):
        FixDictionary.load(path)
