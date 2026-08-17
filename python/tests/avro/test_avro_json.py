from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest
from rkp.avro import (
    AvroDecodeError,
    AvroEncodeError,
    dumps,
    into_json,
    loads,
    out_of_json,
    parse_schema,
)

SCHEMA = parse_schema(
    {
        "type": "record",
        "name": "Message",
        "fields": [
            {"name": "identifier", "type": "long"},
            {"name": "label", "type": ["null", "string"], "default": None},
            {"name": "payload", "type": "bytes"},
            {"name": "choice", "type": ["long", "string"]},
            {
                "name": "moment",
                "type": {"type": "long", "logicalType": "timestamp-micros"},
            },
            {
                "name": "amount",
                "type": {
                    "type": "bytes",
                    "logicalType": "decimal",
                    "precision": 9,
                    "scale": 2,
                },
            },
        ],
    }
)
ROW = {
    "identifier": 1,
    "label": "ada",
    "payload": b"\x00\xff",
    "choice": "text",
    "moment": dt.datetime(2026, 8, 17, tzinfo=dt.UTC),
    "amount": Decimal("1.25"),
}


def test_unions_are_tagged_by_branch_name() -> None:
    encoded = into_json(SCHEMA, ROW)

    assert encoded["label"] == {"string": "ada"}
    assert encoded["choice"] == {"string": "text"}
    assert into_json(SCHEMA, {**ROW, "label": None})["label"] is None


def test_bytes_use_latin_1_text_and_round_trip() -> None:
    encoded = into_json(SCHEMA, ROW)

    assert encoded["payload"] == "\x00ÿ"
    assert out_of_json(SCHEMA, encoded) == ROW


def test_logical_values_encode_through_their_underlying_type() -> None:
    encoded = into_json(SCHEMA, ROW)

    assert encoded["moment"] == 1_786_924_800_000_000
    assert isinstance(encoded["amount"], str)
    assert out_of_json(SCHEMA, encoded)["amount"] == Decimal("1.25")


def test_text_round_trip_uses_the_rkp_json_codec() -> None:
    text = dumps(SCHEMA, ROW)

    assert '"identifier": 1' in text
    assert loads(SCHEMA, text) == ROW
    assert loads(SCHEMA, text.encode("utf-8")) == ROW


def test_invalid_documents_are_rejected() -> None:
    with pytest.raises(AvroDecodeError, match="single-entry object"):
        out_of_json(SCHEMA, {**into_json(SCHEMA, ROW), "choice": "bare"})
    with pytest.raises(AvroDecodeError, match="unknown union branch"):
        out_of_json(SCHEMA, {**into_json(SCHEMA, ROW), "choice": {"float": 1.0}})
    with pytest.raises(AvroDecodeError, match="missing field"):
        out_of_json(SCHEMA, {"identifier": 1})
    with pytest.raises(AvroDecodeError, match="expects a JSON object"):
        out_of_json(SCHEMA, [1, 2])
    with pytest.raises(AvroEncodeError, match="missing field"):
        into_json(SCHEMA, {"identifier": 1})


def test_enum_symbols_are_validated_in_both_directions() -> None:
    schema = parse_schema({"type": "enum", "name": "Kind", "symbols": ["A", "B"]})

    assert into_json(schema, "B") == "B"
    assert out_of_json(schema, "A") == "A"
    with pytest.raises(AvroEncodeError, match="not a symbol"):
        into_json(schema, "C")
    with pytest.raises(AvroDecodeError, match="not a symbol"):
        out_of_json(schema, "C")
