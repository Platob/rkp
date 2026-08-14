from __future__ import annotations

import pytest
import rkp.yaml


def test_loads_quoted_scalars_and_yaml_12_plain_strings() -> None:
    text = r"""
single: 'it''s safe'
double: "line\nbreak"
quoted_true: "true"
quoted_null: 'null'
yes_word: yes
off_word: off
integer: -12
scientific: 6.02e23
"""

    assert rkp.yaml.loads(text) == {
        "single": "it's safe",
        "double": "line\nbreak",
        "quoted_true": "true",
        "quoted_null": "null",
        "yes_word": "yes",
        "off_word": "off",
        "integer": -12,
        "scientific": 6.02e23,
    }


def test_dumps_quotes_ambiguous_strings_for_lossless_round_trip() -> None:
    value = {
        "values": [
            "true",
            "null",
            "001",
            "6.02e23",
            "with: colon",
            "#hash",
            "",
            " leading",
            "trailing ",
            "line\nbreak",
        ],
        "payload": b"\x00\xff",
    }

    assert rkp.yaml.loads(rkp.yaml.dumps(value)) == value


def test_binary_and_unicode_edge_values_round_trip() -> None:
    value = {
        "numeric_base64": bytes.fromhex("fbbef4"),
        'quoted"key': "line\x7f\x85\u2028\u2029",
    }

    assert rkp.yaml.loads(rkp.yaml.dumps(value)) == value
    assert rkp.yaml.loads(r'"\uD83D\uDE00"') == "\U0001f600"
    with pytest.raises(ValueError, match="Unicode escape"):
        rkp.yaml.loads(r'"\uD800"')
