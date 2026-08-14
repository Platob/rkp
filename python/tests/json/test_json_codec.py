from __future__ import annotations

import io

import pytest
import rkp.json
from rkp import Record, field, record


@record
class JsonChild(Record):
    value: int


@record
class JsonPayload(Record):
    identifier: int = field(alias="id")
    child: JsonChild = field()
    labels: set[str] = field(default_factory=set)


def test_json_package_exposes_only_the_public_codec_api() -> None:
    assert rkp.json.__package__ == "rkp.json"
    assert rkp.json.__file__.endswith(
        "json\\__init__.py"
    ) or rkp.json.__file__.endswith("json/__init__.py")
    assert rkp.json.__all__ == [
        "dump",
        "dump_bytes",
        "dumps",
        "dumps_bytes",
        "load",
        "loads",
    ]
    assert all(callable(getattr(rkp.json, name)) for name in rkp.json.__all__)


def test_json_module_round_trips_records_and_streams() -> None:
    value = JsonPayload(7, JsonChild(3), {"one", "two"})

    assert rkp.json.loads(rkp.json.dumps(value), cls=JsonPayload) == value

    binary = io.BytesIO()
    rkp.json.dump(value, binary)
    assert not binary.closed
    binary.seek(0)
    assert rkp.json.load(binary, cls=JsonPayload) == value
    assert not binary.closed


def test_json_dump_rejects_cycles() -> None:
    cyclic: list[object] = []
    cyclic.append(cyclic)

    with pytest.raises(ValueError, match="cyclic"):
        rkp.json.dumps(cyclic)


def test_json_bytes_api_round_trips_without_text_stream_probing() -> None:
    value = JsonPayload(7, JsonChild(3), {"one", "two"})
    encoded = rkp.json.dumps_bytes(value)

    assert isinstance(encoded, bytes)
    assert rkp.json.loads(memoryview(encoded), cls=JsonPayload) == value

    stream = io.BytesIO()
    assert rkp.json.dump_bytes(value, stream) is None
    assert stream.getvalue() == encoded
    assert not stream.closed

    with pytest.raises(TypeError, match="binary stream"):
        rkp.json.dump_bytes(value, io.StringIO())  # type: ignore[arg-type]
