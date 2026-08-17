from __future__ import annotations

from typing import Any

import pytest
from rkp.avro import (
    ArraySchema,
    AvroSchemaError,
    EnumSchema,
    FixedSchema,
    MapSchema,
    PrimitiveSchema,
    RecordSchema,
    UnionSchema,
    canonical_form,
    dumps_schema,
    fingerprint,
    loads_schema,
    parse_schema,
    schema_into_json,
)

RECORD = {
    "type": "record",
    "name": "Node",
    "namespace": "rkp.test",
    "doc": "A recursive node",
    "fields": [
        {"name": "value", "type": "long", "doc": "payload"},
        {"name": "next", "type": ["null", "Node"], "default": None},
        {"name": "labels", "type": {"type": "array", "items": "string"}},
        {"name": "lookup", "type": {"type": "map", "values": "int"}},
    ],
}


def test_primitive_names_parse_to_shared_singletons() -> None:
    assert parse_schema("string") is parse_schema("string")
    assert isinstance(parse_schema("null"), PrimitiveSchema)
    with pytest.raises(AvroSchemaError, match="unknown Avro schema name"):
        parse_schema("int64")


def test_named_types_resolve_namespaces_and_recursion() -> None:
    schema = parse_schema(RECORD)

    assert isinstance(schema, RecordSchema)
    assert schema.fullname == "rkp.test.Node"
    assert schema.doc == "A recursive node"
    next_field = schema.field("next")
    assert isinstance(next_field.type, UnionSchema)
    assert next_field.type.is_optional
    assert next_field.type.options[1] is schema
    assert next_field.has_default and next_field.default is None
    assert isinstance(schema.field("labels").type, ArraySchema)
    assert isinstance(schema.field("lookup").type, MapSchema)
    with pytest.raises(KeyError):
        schema.field("missing")


def test_json_round_trip_preserves_the_declaration() -> None:
    schema = parse_schema(RECORD)

    emitted = schema_into_json(schema)
    assert emitted["name"] == "Node"
    assert emitted["namespace"] == "rkp.test"
    # A repeated named reference is emitted as its fullname, never re-declared.
    assert emitted["fields"][1]["type"] == ["null", "rkp.test.Node"]
    assert parse_schema(emitted) == schema
    assert loads_schema(dumps_schema(schema)) == schema


def test_canonical_form_strips_documentation_and_orders_keys() -> None:
    form = canonical_form(parse_schema(RECORD))

    assert form.startswith('{"name":"rkp.test.Node","type":"record","fields":[')
    assert "doc" not in form
    assert "default" not in form
    assert parse_schema(form) == parse_schema(RECORD)


def _reference_fingerprint(payload: bytes) -> int:
    """Rabin fingerprint written straight from the Avro specification."""

    empty = 0xC15D213AA4D7A795
    table = []
    for index in range(256):
        value = index
        for _ in range(8):
            value = (value >> 1) ^ (empty & -(value & 1))
        table.append(value)
    result = empty
    for byte in payload:
        result = (result >> 8) ^ table[(result ^ byte) & 0xFF]
    return result


@pytest.mark.parametrize(
    ("schema", "expected"),
    [
        ('"null"', 0x63DD24E7CC258F8A),
        ('"string"', 0x8F014872634503C7),
    ],
)
def test_rabin_fingerprints_match_the_published_vectors(
    schema: str, expected: int
) -> None:
    assert fingerprint(parse_schema(schema)) == expected
    assert fingerprint(schema) == expected


@pytest.mark.parametrize(
    "declaration",
    [RECORD, {"type": "array", "items": "string"}, ["null", "long"], "boolean"],
)
def test_rabin_fingerprints_match_an_independent_implementation(
    declaration: Any,
) -> None:
    schema = parse_schema(declaration)
    payload = canonical_form(schema).encode("utf-8")

    assert fingerprint(schema) == _reference_fingerprint(payload)
    assert schema.fingerprint() == fingerprint(schema)


def test_schema_equality_and_hashing_use_canonical_form() -> None:
    first = parse_schema(RECORD)
    second = parse_schema(dict(RECORD))

    assert first == second
    assert hash(first) == hash(second)
    assert len({first, second}) == 1
    assert first != parse_schema("string")


def test_logical_types_are_validated_and_invalid_ones_are_ignored() -> None:
    decimal = parse_schema(
        {"type": "bytes", "logicalType": "decimal", "precision": 9, "scale": 2}
    )
    assert isinstance(decimal, PrimitiveSchema)
    assert (decimal.logical, decimal.precision, decimal.scale) == ("decimal", 9, 2)

    ignored = parse_schema({"type": "string", "logicalType": "decimal"})
    assert isinstance(ignored, PrimitiveSchema)
    assert ignored.logical is None
    # An unusable annotation survives as an ordinary attribute, which is what
    # the specification tells readers to do.
    assert schema_into_json(ignored)["logicalType"] == "decimal"

    with pytest.raises(AvroSchemaError, match="does not fit in fixed"):
        parse_schema(
            {
                "type": "fixed",
                "name": "Small",
                "size": 1,
                "logicalType": "decimal",
                "precision": 12,
                "scale": 2,
            }
        )


def test_named_type_validation_rejects_malformed_declarations() -> None:
    with pytest.raises(AvroSchemaError, match="duplicate Avro type name"):
        parse_schema(
            {
                "type": "record",
                "name": "Duplicate",
                "fields": [
                    {
                        "name": "left",
                        "type": {"type": "enum", "name": "Duplicate", "symbols": ["A"]},
                    }
                ],
            }
        )
    with pytest.raises(AvroSchemaError, match="duplicate field"):
        parse_schema(
            {
                "type": "record",
                "name": "Repeated",
                "fields": [{"name": "x", "type": "int"}, {"name": "x", "type": "int"}],
            }
        )
    with pytest.raises(AvroSchemaError, match="unions cannot immediately contain"):
        UnionSchema((PrimitiveSchema("null"), UnionSchema((PrimitiveSchema("int"),))))
    with pytest.raises(AvroSchemaError, match="duplicate branch"):
        parse_schema(["int", "int"])
    with pytest.raises(AvroSchemaError, match="invalid Avro name"):
        parse_schema({"type": "record", "name": "9bad", "fields": []})


def test_enum_and_fixed_declarations_are_validated() -> None:
    enum_schema = parse_schema(
        {
            "type": "enum",
            "name": "Suit",
            "symbols": ["HEART", "SPADE"],
            "default": "HEART",
        }
    )
    assert isinstance(enum_schema, EnumSchema)
    assert enum_schema.symbols == ("HEART", "SPADE")

    fixed = parse_schema({"type": "fixed", "name": "Md5", "size": 16})
    assert isinstance(fixed, FixedSchema)
    assert fixed.size == 16

    with pytest.raises(AvroSchemaError, match="is not a symbol"):
        parse_schema({"type": "enum", "name": "Bad", "symbols": ["A"], "default": "B"})
    with pytest.raises(AvroSchemaError, match="duplicate symbols"):
        parse_schema({"type": "enum", "name": "Bad", "symbols": ["A", "A"]})
    with pytest.raises(AvroSchemaError, match="non-negative integer"):
        parse_schema({"type": "fixed", "name": "Bad", "size": -1})


@pytest.mark.parametrize(
    "value",
    [3, object(), {"name": "NoType"}, {"type": "array"}, {"type": "map"}],
)
def test_unparseable_declarations_raise_schema_errors(value: Any) -> None:
    with pytest.raises(AvroSchemaError):
        parse_schema(value)
