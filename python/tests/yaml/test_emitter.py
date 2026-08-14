from __future__ import annotations

import pytest
import rkp.yaml


def test_multiline_keep_chomping_and_document_markers_round_trip() -> None:
    value = {"text": "first\n\n", "marker": "..."}
    encoded = rkp.yaml.dumps(
        value,
        explicit_start=True,
        explicit_end=True,
    )

    assert encoded.startswith("---\n")
    assert encoded.endswith("...\n")
    assert rkp.yaml.loads(encoded) == value


def test_yaml_dump_rejects_cycles() -> None:
    cyclic: list[object] = []
    cyclic.append(cyclic)

    with pytest.raises(ValueError, match="cyclic"):
        rkp.yaml.dumps(cyclic)
