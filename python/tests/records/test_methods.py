from __future__ import annotations

import io
from pathlib import Path
from typing import NamedTuple

import pytest
from rkp import Record, field, record


@record
class MethodChild(Record):
    value: int


@record(alias="payloads")
class MethodPayload(Record):
    identifier: int = field(alias="id")
    child: MethodChild = field()
    labels: set[str] = field(default_factory=set)


def test_json_record_methods_round_trip() -> None:
    value = MethodPayload(7, MethodChild(3), {"one", "two"})

    text = value.dumps_json(sort_keys=True)
    assert MethodPayload.loads_json(text) == value

    binary = io.BytesIO()
    value.dump_json(binary)
    assert not binary.closed
    binary.seek(0)
    assert MethodPayload.load_json(binary) == value
    assert not binary.closed


def test_yaml_record_methods_round_trip() -> None:
    value = MethodPayload(7, MethodChild(3), {"one", "two"})

    text = value.dumps_yaml()
    assert MethodPayload.loads_yaml(text) == value

    stream = io.StringIO()
    value.dump_yaml(stream)
    assert not stream.closed
    stream.seek(0)
    assert MethodPayload.load_yaml(stream) == value


def test_record_byte_methods_round_trip_paths_buffers_and_memoryviews(
    tmp_path: Path,
) -> None:
    value = MethodPayload(7, MethodChild(3), {"one", "two"})

    json_data = value.dumps_json_bytes(sort_keys=True)
    yaml_data = value.dumps_yaml_bytes()
    assert isinstance(json_data, bytes)
    assert isinstance(yaml_data, bytes)
    assert MethodPayload.load_json(memoryview(json_data)) == value
    assert MethodPayload.load_yaml(memoryview(yaml_data)) == value
    assert MethodPayload.load(memoryview(json_data)) == value

    json_stream = io.BytesIO()
    assert value.dump_json_bytes(json_stream, sort_keys=True) is None
    assert json_stream.getvalue() == json_data
    json_stream.seek(0)
    assert MethodPayload.load_json(json_stream) == value

    json_path = tmp_path / "record.json"
    yaml_path = tmp_path / "record.yaml"
    assert value.dump_bytes(json_path, sort_keys=True) is None
    assert value.dump_bytes(yaml_path) is None
    assert json_path.read_bytes() == json_data
    assert yaml_path.read_bytes() == yaml_data
    assert MethodPayload.load(json_path) == value
    assert MethodPayload.load(yaml_path) == value


def test_record_byte_string_buffers_and_generic_format_dispatch() -> None:
    value = MethodPayload(7, MethodChild(3), {"one"})

    assert value.dump_json_bytes("") == value.dumps_json_bytes()
    assert value.dump_yaml_bytes("buffer") == value.dumps_yaml_bytes()
    assert value.dumps_bytes() == value.dumps_json_bytes()
    assert value.dumps_bytes(format="yaml") == value.dumps_yaml_bytes()
    assert value.dump_bytes("buffer") == value.dumps_json_bytes()
    assert value.dump_bytes("buffer", format="yaml") == value.dumps_yaml_bytes()


def test_generic_methods_infer_paths_and_stream_names(tmp_path: Path) -> None:
    value = MethodPayload(7, MethodChild(3), {"one"})
    json_path = tmp_path / "record.JSON"
    yaml_path = tmp_path / "record.yml"

    value.dump(json_path)
    value.dump(yaml_path)
    assert MethodPayload.load(json_path) == value
    assert MethodPayload.load(yaml_path) == value

    assert MethodPayload.loads(value.dumps()) == value
    assert MethodPayload.loads(value.dumps(format="yaml"), format="yaml") == value

    unnamed = io.StringIO()
    with pytest.raises(ValueError, match="cannot infer"):
        value.dump(unnamed)


def test_codec_flags_control_generated_methods() -> None:
    @record(with_yaml=False)
    class JsonOnly(Record):
        value: int

    @record(with_json=False)
    class YamlOnly(Record):
        value: int

    assert not hasattr(JsonOnly, "loads_yaml")
    assert not hasattr(JsonOnly, "dumps_yaml_bytes")
    assert not hasattr(YamlOnly, "loads_json")
    assert not hasattr(YamlOnly, "dump_json_bytes")
    assert JsonOnly.loads('{"value": 2}') == JsonOnly(2)
    assert YamlOnly.loads("value: 2") == YamlOnly(2)

    with pytest.raises(RuntimeError, match="YAML support is disabled"):
        JsonOnly.loads("value: 2", format="yaml")


def test_named_tuple_and_range_round_trip() -> None:
    class Pair(NamedTuple):
        left: int
        right: str

    @record
    class Rich(Record):
        pair: Pair
        values: range

    value = Rich(Pair(1, "two"), range(1, 6, 2))
    assert Rich.loads_json(value.dumps_json()) == value
