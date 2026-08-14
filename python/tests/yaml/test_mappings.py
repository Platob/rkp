from __future__ import annotations

import rkp.yaml


def test_yaml_preserves_structured_mapping_keys() -> None:
    value = {(1, 2): "tuple", b"key": "bytes", 3: "integer"}

    assert rkp.yaml.loads(rkp.yaml.dumps(value)) == value
