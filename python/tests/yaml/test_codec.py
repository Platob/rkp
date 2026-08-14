from __future__ import annotations

import io
import os
import subprocess
import sys
from pathlib import Path

import pytest
import rkp.yaml
from rkp import Record, field, record


@record
class YamlChild(Record):
    value: int


@record
class YamlPayload(Record):
    identifier: int = field(alias="id")
    child: YamlChild = field()
    labels: list[str] = field(default_factory=list)
    note: str = ""


def test_yaml_package_exposes_the_codec_api() -> None:
    assert Path(rkp.yaml.__file__).name == "__init__.py"
    assert rkp.yaml.__all__ == [
        "dump",
        "dump_bytes",
        "dumps",
        "dumps_bytes",
        "load",
        "loads",
    ]
    assert all(callable(getattr(rkp.yaml, name)) for name in rkp.yaml.__all__)


def test_nested_record_round_trip_and_safe_materialization() -> None:
    value = YamlPayload(
        identifier=7,
        child=YamlChild(3),
        labels=["one", "two"],
        note="true: but text",
    )

    text = value.dumps_yaml()
    assert "id:" in text
    assert YamlPayload.loads_yaml(text) == value
    assert rkp.yaml.loads(text, cls=YamlPayload) == value
    assert YamlPayload.loads_yaml("id: '8'\nchild: {value: '4'}") == (
        YamlPayload(8, YamlChild(4))
    )


def test_load_dump_paths_and_text_and_binary_streams(
    tmp_path: Path,
) -> None:
    value = {"message": "dÃ©jÃ  vu", "items": [1, 2, 3]}
    path = tmp_path / "payload.yaml"

    rkp.yaml.dump(value, path, encoding="utf-16")
    assert rkp.yaml.load(path, encoding="utf-16") == value

    text_stream = io.StringIO()
    rkp.yaml.dump(value, text_stream)
    assert not text_stream.closed
    text_stream.seek(0)
    assert rkp.yaml.load(text_stream) == value
    assert not text_stream.closed

    binary_stream = io.BytesIO()
    rkp.yaml.dump(value, binary_stream, encoding="utf-16")
    assert not binary_stream.closed
    binary_stream.seek(0)
    assert rkp.yaml.load(binary_stream, encoding="utf-16") == value
    assert not binary_stream.closed


def test_yaml_sorting_and_input_validation() -> None:
    sorted_text = rkp.yaml.dumps({"z": 1, "a": 2}, sort_keys=True)
    assert sorted_text.index("a:") < sorted_text.index("z:")

    with pytest.raises(TypeError, match="str, bytes, bytearray, or memoryview"):
        rkp.yaml.loads(123)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="readable stream"):
        rkp.yaml.load(object())  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="writable stream"):
        rkp.yaml.dump({}, object())  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="unsupported YAML load option"):
        rkp.yaml.loads("value: 1", Loader=object)


def test_yaml_codec_does_not_import_pyyaml() -> None:
    script = r"""
import builtins
import sys

real_import = builtins.__import__
def guarded(name, *args, **kwargs):
    if name == "yaml" or name.startswith("yaml."):
        raise AssertionError(f"unexpected PyYAML import: {name}")
    return real_import(name, *args, **kwargs)

builtins.__import__ = guarded
import rkp.yaml as yaml_codec
from rkp import Record, record

@record
class Value(Record):
    number: int

source = Value(3)
assert yaml_codec.loads(yaml_codec.dumps(source), cls=Value) == source
assert Value.loads_yaml(Value(4).dumps_yaml()) == Value(4)
assert "yaml" not in sys.modules
"""
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(Path(__file__).parents[2] / "src")
    completed = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env=environment,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr


def test_yaml_bytes_api_round_trips_paths_and_binary_buffers(tmp_path: Path) -> None:
    value = YamlPayload(7, YamlChild(3), ["one"], "déjà vu")
    encoded = rkp.yaml.dumps_bytes(value, encoding="utf-16")

    assert (
        rkp.yaml.loads(memoryview(encoded), cls=YamlPayload, encoding="utf-16") == value
    )

    stream = io.BytesIO()
    assert rkp.yaml.dump_bytes(value, stream, encoding="utf-16") is None
    assert stream.getvalue() == encoded
    assert not stream.closed

    path = tmp_path / "payload.yaml"
    assert rkp.yaml.dump_bytes(value, path, encoding="utf-16") is None
    assert path.read_bytes() == encoded
    assert rkp.yaml.load(path, cls=YamlPayload, encoding="utf-16") == value
