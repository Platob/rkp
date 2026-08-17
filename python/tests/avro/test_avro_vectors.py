"""Conformance vectors shared with the Node binding.

``vectors.json`` pins one canonical form, one fingerprint, and one binary
encoding per schema shape.  ``js/test/vectors.test.js`` asserts the same file,
so a change that moves the bytes in one host but not the other fails in both.

The vectors deliberately carry no decoded values: the bytes are the contract,
and each host decodes them into the objects its own users hold.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from rkp.avro import canonical_form, decode, encode, fingerprint, parse_schema

VECTORS = Path(__file__).with_name("vectors.json")
CASES: list[dict[str, Any]] = json.loads(VECTORS.read_text(encoding="utf-8"))


def test_the_vector_file_covers_every_shape() -> None:
    names = {case["name"] for case in CASES}

    assert len(names) == len(CASES)
    assert {"record", "map", "array", "enum", "fixed"} <= names
    assert any(name.startswith("logical-") for name in names)
    assert any(name.startswith("optional-union") for name in names)


@pytest.mark.parametrize("case", CASES, ids=lambda case: str(case["name"]))
def test_schemas_and_encodings_match_the_shared_vectors(case: dict[str, Any]) -> None:
    schema = parse_schema(case["schema"])
    payload = bytes.fromhex(case["binary"])

    assert canonical_form(schema) == case["canonical_form"]
    assert f"{fingerprint(schema):016x}" == case["fingerprint"]
    assert encode(schema, decode(schema, payload)) == payload
