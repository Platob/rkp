from __future__ import annotations

import datetime as dt
import uuid
from decimal import Decimal
from typing import Any

import pytest
from rkp.avro import (
    AvroDecodeError,
    AvroEncodeError,
    Reader,
    compile_decoder,
    compile_encoder,
    decode,
    decode_single_object,
    encode,
    encode_into,
    encode_single_object,
    parse_schema,
)

EVENT = parse_schema(
    {
        "type": "record",
        "name": "Event",
        "namespace": "rkp.test",
        "fields": [
            {"name": "identifier", "type": "long"},
            {"name": "label", "type": ["null", "string"], "default": None},
            {"name": "ratio", "type": "double"},
            {"name": "flag", "type": "boolean"},
            {"name": "payload", "type": "bytes"},
            {
                "name": "kind",
                "type": {"type": "enum", "name": "Kind", "symbols": ["A", "B"]},
            },
            {"name": "digest", "type": {"type": "fixed", "name": "Digest", "size": 4}},
            {"name": "tags", "type": {"type": "array", "items": "string"}},
            {"name": "counts", "type": {"type": "map", "values": "long"}},
        ],
    }
)
ROW: dict[str, Any] = {
    "identifier": -7,
    "label": "ada",
    "ratio": 0.5,
    "flag": True,
    "payload": b"\x00\x01",
    "kind": "B",
    "digest": b"abcd",
    "tags": ["x", "y"],
    "counts": {"a": 1, "b": 2},
}

LOGICAL = parse_schema(
    {
        "type": "record",
        "name": "Logical",
        "fields": [
            {"name": "day", "type": {"type": "int", "logicalType": "date"}},
            {"name": "clock", "type": {"type": "long", "logicalType": "time-micros"}},
            {"name": "millis", "type": {"type": "int", "logicalType": "time-millis"}},
            {
                "name": "moment",
                "type": {"type": "long", "logicalType": "timestamp-micros"},
            },
            {
                "name": "local",
                "type": {"type": "long", "logicalType": "local-timestamp-millis"},
            },
            {
                "name": "nanos",
                "type": {"type": "long", "logicalType": "timestamp-nanos"},
            },
            {
                "name": "amount",
                "type": {
                    "type": "bytes",
                    "logicalType": "decimal",
                    "precision": 12,
                    "scale": 3,
                },
            },
            {"name": "key", "type": {"type": "string", "logicalType": "uuid"}},
        ],
    }
)


def test_binary_round_trip_preserves_every_kind() -> None:
    payload = encode(EVENT, ROW)

    assert decode(EVENT, payload) == ROW
    assert compile_encoder(EVENT) is compile_encoder(EVENT)
    assert compile_decoder(EVENT) is compile_decoder(EVENT)


def test_optional_unions_use_a_single_branch_byte() -> None:
    present = encode(EVENT, ROW)
    absent = encode(EVENT, {**ROW, "label": None})

    assert len(present) - len(absent) == len("ada") + 1
    assert decode(EVENT, absent)["label"] is None


def test_logical_types_round_trip_as_python_values() -> None:
    row = {
        "day": dt.date(2026, 8, 17),
        "clock": dt.time(1, 2, 3, 456789),
        "millis": dt.time(4, 5, 6, 123000),
        "moment": dt.datetime(2026, 8, 17, 12, 30, tzinfo=dt.UTC),
        "local": dt.datetime(2026, 8, 17, 12, 30),  # noqa: DTZ001 - local logical type
        "nanos": dt.datetime(2026, 8, 17, 12, 30, tzinfo=dt.UTC),
        "amount": Decimal("12.345"),
        "key": uuid.UUID("3f2504e0-4f89-11d3-9a0c-0305e82c3301"),
    }

    restored = decode(LOGICAL, encode(LOGICAL, row))

    assert restored == row
    assert isinstance(restored["amount"], Decimal)
    assert isinstance(restored["key"], uuid.UUID)


def test_records_accept_dataclasses_and_sequences() -> None:
    from dataclasses import dataclass

    pair = parse_schema(
        {
            "type": "record",
            "name": "Pair",
            "fields": [
                {"name": "left", "type": "int"},
                {"name": "right", "type": "string"},
            ],
        }
    )

    @dataclass
    class Pair:
        left: int
        right: str

    assert decode(pair, encode(pair, Pair(1, "a"))) == {"left": 1, "right": "a"}
    # Tuple annotations become Arrow/Avro structs, so positional rows encode.
    assert decode(pair, encode(pair, (1, "a"))) == {"left": 1, "right": "a"}
    with pytest.raises(AvroEncodeError, match="expects 2 positional values"):
        encode(pair, (1,))
    with pytest.raises(AvroEncodeError, match="expects a mapping, dataclass"):
        encode(pair, 5)


def test_missing_fields_use_defaults_and_otherwise_fail() -> None:
    schema = parse_schema(
        {
            "type": "record",
            "name": "WithDefault",
            "fields": [
                {"name": "count", "type": "int", "default": 3},
                {"name": "name", "type": "string"},
            ],
        }
    )

    assert decode(schema, encode(schema, {"name": "x"})) == {"count": 3, "name": "x"}
    with pytest.raises(AvroEncodeError, match="missing field 'name'"):
        encode(schema, {"count": 1})


def test_unions_select_the_matching_branch() -> None:
    schema = parse_schema(
        ["null", "long", "string", {"type": "array", "items": "long"}]
    )

    for value in (None, 5, "text", [1, 2]):
        assert decode(schema, encode(schema, value)) == value
    with pytest.raises(AvroEncodeError, match="does not match any union branch"):
        encode(schema, {"unsupported": True})


@pytest.mark.parametrize(
    ("schema", "value", "message"),
    [
        ('"int"', 2**31, "does not fit in an Avro int"),
        ('"long"', 2**63, "does not fit in an Avro long"),
        ('"null"', 1, "expected null"),
        ('"string"', 4, "expected string"),
        ('"bytes"', 4, "expected bytes"),
    ],
)
def test_encoding_rejects_out_of_contract_values(
    schema: str, value: Any, message: str
) -> None:
    with pytest.raises(AvroEncodeError, match=message):
        encode(parse_schema(schema), value)


def test_truncated_payloads_raise_decode_errors() -> None:
    payload = encode(EVENT, ROW)

    with pytest.raises(AvroDecodeError):
        decode(EVENT, payload[:4])
    with pytest.raises(AvroDecodeError, match="truncated"):
        Reader(b"").read_long()


def test_single_object_encoding_carries_the_schema_fingerprint() -> None:
    framed = encode_single_object(EVENT, ROW)

    assert framed[:2] == b"\xc3\x01"
    assert decode_single_object(EVENT, framed) == ROW
    with pytest.raises(AvroDecodeError, match="fingerprint"):
        decode_single_object(LOGICAL, framed)
    with pytest.raises(AvroDecodeError, match="marker"):
        decode_single_object(EVENT, framed[2:])


def test_encode_into_appends_to_a_caller_owned_buffer() -> None:
    out = bytearray(b"header")
    encode_into(EVENT, ROW, out)

    assert out.startswith(b"header")
    assert decode(EVENT, Reader(bytes(out), 6)) == ROW
    with pytest.raises(TypeError, match="bytearray"):
        encode_into(EVENT, ROW, b"immutable")  # type: ignore[arg-type]


def test_array_blocks_with_negative_counts_are_decodable() -> None:
    schema = parse_schema({"type": "array", "items": "long"})
    # A writer may emit a negative count followed by the block byte size.
    payload = bytearray()
    payload += bytes([0x03])  # zig-zag encoded -2
    payload += bytes([0x04])  # block size in bytes
    payload += bytes([0x02, 0x04])  # values 1 and 2
    payload += bytes([0x00])

    assert decode(schema, bytes(payload)) == [1, 2]
