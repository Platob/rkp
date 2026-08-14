from __future__ import annotations

import builtins
import io
from pathlib import Path
from typing import Any, Self

import pytest
import rkp.json
import rkp.yaml
from rkp import Record, record


@record
class StringPayload(Record):
    value: int


@pytest.mark.parametrize(
    ("codec", "document"),
    [
        (rkp.json, '{"value": 7}'),
        (rkp.yaml, "value: 7"),
    ],
)
def test_codec_load_treats_separator_free_strings_as_documents(
    codec: Any, document: str
) -> None:
    assert codec.load(document) == {"value": 7}


def test_record_load_treats_separator_free_strings_as_documents() -> None:
    assert StringPayload.load('{"value": "7"}') == StringPayload(7)
    assert StringPayload.load("value: '8'", format="yaml") == StringPayload(8)


@pytest.mark.parametrize(
    ("codec", "expected"),
    [
        (rkp.json, '{"value": 7}'),
        (rkp.yaml, "value: 7\n"),
    ],
)
def test_codec_dump_returns_separator_free_string_buffer(
    codec: Any, expected: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)

    assert codec.dump({"value": 7}, "") == expected
    assert not list(tmp_path.iterdir())


def test_record_dump_returns_string_buffer_through_every_dispatch() -> None:
    value = StringPayload(8)

    assert value.dump_json("") == '{"value": 8}'
    assert value.dump_yaml("") == "value: 8\n"
    assert value.dump("") == '{"value": 8}'
    assert value.dump("buffer", format="yaml") == "value: 8\n"


@pytest.mark.parametrize("path", ["virtual/payload.json", r"virtual\payload.json"])
def test_a_string_with_either_path_separator_is_opened(
    monkeypatch: pytest.MonkeyPatch, path: str
) -> None:
    opened: list[str] = []

    def open_document(source: object, *args: object, **kwargs: object) -> io.BytesIO:
        opened.append(str(source))
        return io.BytesIO(b'{"value": 9}')

    monkeypatch.setattr(builtins, "open", open_document)

    assert rkp.json.load(path) == {"value": 9}
    assert StringPayload.load(path) == StringPayload(9)
    assert opened == [path, path]


@pytest.mark.parametrize("path", ["virtual/output.json", r"virtual\output.json"])
def test_a_string_with_either_separator_is_written_as_a_path(
    monkeypatch: pytest.MonkeyPatch, path: str
) -> None:
    writes: list[tuple[str, str]] = []

    class _Writer(io.StringIO):
        def __enter__(self) -> Self:
            return self

        def __exit__(self, *args: object) -> None:
            writes.append((path, self.getvalue()))
            self.close()

    monkeypatch.setattr(builtins, "open", lambda *args, **kwargs: _Writer())

    assert rkp.json.dump({"value": 9}, path) is None
    assert writes == [(path, '{"value": 9}')]


def test_separator_free_filename_string_is_content_but_pathlike_is_a_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    path = Path("payload.yaml")
    path.write_text("value: 11\n", encoding="utf-8")

    assert rkp.yaml.load("payload.yaml") == "payload.yaml"
    assert rkp.yaml.load(path) == {"value": 11}
    assert StringPayload.load(path) == StringPayload(11)


@pytest.mark.parametrize(
    ("codec", "filename"),
    [
        (rkp.json, "output.json"),
        (rkp.yaml, "output.yaml"),
    ],
)
def test_separator_free_string_destination_returns_text_instead_of_writing(
    codec: Any,
    filename: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    Path(filename).write_text("existing", encoding="utf-8")

    encoded = codec.dump({"value": 12}, filename)

    assert isinstance(encoded, str)
    assert codec.loads(encoded) == {"value": 12}
    assert Path(filename).read_text(encoding="utf-8") == "existing"


def test_record_dump_returns_text_for_a_separator_free_string_destination() -> None:
    value = StringPayload(12)

    assert value.dump_json("buffer") == value.dumps_json()
    assert value.dump_yaml("buffer") == value.dumps_yaml()
    assert value.dump("buffer") == value.dumps()
    assert value.dump("buffer", format="yaml") == value.dumps(format="yaml")


@pytest.mark.parametrize("codec", [rkp.json, rkp.yaml])
def test_byte_dump_preserves_the_string_path_rule(
    codec: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    expected = codec.dumps_bytes({"value": 12})

    assert codec.dump_bytes({"value": 12}, "output") == expected
    assert not (tmp_path / "output").exists()

    path = str(tmp_path / "nested" / "output")
    (tmp_path / "nested").mkdir()
    assert codec.dump_bytes({"value": 12}, path) is None
    assert Path(path).read_bytes() == expected


@pytest.mark.parametrize("format", ["json", "yaml"])
def test_string_io_streams_are_caller_owned_and_use_the_current_position(
    format: str,
) -> None:
    value = StringPayload(13)
    buffer = io.StringIO("prefix")
    buffer.seek(0, io.SEEK_END)
    document_start = buffer.tell()

    assert value.dump(buffer, format=format) is None
    assert not buffer.closed

    buffer.seek(document_start)
    assert StringPayload.load(buffer, format=format) == value
    assert not buffer.closed


class _NamedStringBuffer(io.StringIO):
    name = "payload.yaml"


def test_named_string_buffer_supplies_format_without_becoming_a_path() -> None:
    value = StringPayload(17)
    buffer = _NamedStringBuffer()

    assert value.dump(buffer) is None
    assert not buffer.closed
    buffer.seek(0)
    assert StringPayload.load(buffer) == value
    assert not buffer.closed


class _NamedBytesBuffer(io.BytesIO):
    name = "payload.yaml"


def test_named_binary_buffer_supplies_format_for_byte_methods() -> None:
    value = StringPayload(18)
    buffer = _NamedBytesBuffer()

    assert value.dump_bytes(buffer) is None
    assert buffer.getvalue() == value.dumps_yaml_bytes()
    assert not buffer.closed
    buffer.seek(0)
    assert StringPayload.load(buffer) == value
    assert not buffer.closed


@pytest.mark.parametrize("format", ["json", "yaml"])
def test_binary_streams_are_caller_owned_and_use_the_current_position(
    format: str,
) -> None:
    value = StringPayload(19)
    buffer = io.BytesIO(b"prefix")
    buffer.seek(0, io.SEEK_END)
    document_start = buffer.tell()

    assert value.dump_bytes(buffer, format=format) is None
    assert not buffer.closed
    buffer.seek(document_start)
    assert StringPayload.load(buffer, format=format) == value
    assert not buffer.closed
