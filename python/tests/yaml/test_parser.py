from __future__ import annotations

import pytest
import rkp.yaml


def test_loads_block_mappings_sequences_and_core_scalars() -> None:
    text = """
# Service configuration
service:
  enabled: true
  retries: 3
  ratio: -1.25
  missing: null
  alternate_missing: ~
  ports:
    - 8080
    - 8443
  labels:
    environment: production # an inline comment
    empty: ""
"""

    assert rkp.yaml.loads(text) == {
        "service": {
            "enabled": True,
            "retries": 3,
            "ratio": -1.25,
            "missing": None,
            "alternate_missing": None,
            "ports": [8080, 8443],
            "labels": {"environment": "production", "empty": ""},
        }
    }


def test_loads_flow_collections() -> None:
    text = """
matrix: [[1, 2], [3, 4]]
options: {enabled: true, label: "x,y", nested: [null, false]}
"""

    assert rkp.yaml.loads(text) == {
        "matrix": [[1, 2], [3, 4]],
        "options": {
            "enabled": True,
            "label": "x,y",
            "nested": [None, False],
        },
    }


def test_loads_literal_and_folded_block_scalars() -> None:
    text = """
literal: |
  first line
  second line
folded: >
  first line
  second line
"""

    assert rkp.yaml.loads(text) == {
        "literal": "first line\nsecond line\n",
        "folded": "first line second line\n",
    }


@pytest.mark.parametrize(
    "text",
    [
        "duplicate: 1\nduplicate: 2\n",
        "parent:\n   child: 1\n  sibling: 2\n",
        "values: [1, 2\n",
        "first: 1\n---\nsecond: 2\n",
    ],
)
def test_loads_rejects_malformed_or_multiple_documents(text: str) -> None:
    with pytest.raises(ValueError):
        rkp.yaml.loads(text)


@pytest.mark.parametrize(
    "text",
    [
        "value: !application/object dangerous\n",
        "value: !!python/object/new:builtins.object {}\n",
        "shared: &shared [1, 2]\ncopy: *shared\n",
    ],
)
def test_loads_rejects_tags_anchors_and_aliases(text: str) -> None:
    with pytest.raises(ValueError, match="tag|anchor|alias|unsupported"):
        rkp.yaml.loads(text)
