from __future__ import annotations

from pathlib import Path

import pytest
from rkp.avro import (
    CODECS,
    AvroDecodeError,
    AvroError,
    AvroWriter,
    dump,
    load,
    parse_schema,
    read_container,
    write_container,
)

SCHEMA = parse_schema(
    {
        "type": "record",
        "name": "Row",
        "fields": [
            {"name": "identifier", "type": "long"},
            {"name": "label", "type": ["null", "string"], "default": None},
        ],
    }
)
ROWS = [
    {"identifier": index, "label": None if index % 2 else "x"} for index in range(9)
]
SYNC = b"0123456789abcdef"


@pytest.mark.parametrize("codec", CODECS)
def test_every_stdlib_codec_round_trips(codec: str) -> None:
    writer = AvroWriter(SCHEMA, codec=codec, sync_marker=SYNC)
    writer.extend(ROWS)
    payload = writer.close()

    reader = read_container(payload)
    assert payload.startswith(b"Obj\x01")
    assert reader.codec == codec
    assert reader.schema == SCHEMA
    assert list(reader) == ROWS


def test_blocks_flush_at_the_sync_interval() -> None:
    writer = AvroWriter(SCHEMA, sync_interval=1, sync_marker=SYNC)
    writer.extend(ROWS)
    payload = writer.close()

    # One sync marker follows the header and one follows every flushed block.
    assert payload.count(SYNC) == len(ROWS) + 1
    assert list(read_container(payload)) == ROWS


def test_metadata_is_preserved_and_reserved_keys_are_owned() -> None:
    payload = AvroWriter(
        SCHEMA,
        metadata={"writer": "rkp", "avro.codec": "ignored"},
        sync_marker=SYNC,
    )
    payload.extend(ROWS[:1])
    reader = read_container(payload.close())

    assert reader.metadata["writer"] == b"rkp"
    assert reader.metadata["avro.codec"] == b"null"
    assert reader.writer_schema == SCHEMA


def test_paths_and_streams_use_the_shared_codec_io(tmp_path: Path) -> None:
    destination = tmp_path / "rows.avro"

    assert write_container(destination, SCHEMA, ROWS, sync_marker=SYNC) is None
    assert list(read_container(destination)) == ROWS
    assert load(destination) == ROWS
    with destination.open("rb") as stream:
        assert list(read_container(stream)) == ROWS

    other = tmp_path / "again.avro"
    assert dump(other, SCHEMA, ROWS) is None
    assert load(other) == ROWS


def test_streaming_writer_targets_a_binary_stream(tmp_path: Path) -> None:
    destination = tmp_path / "streamed.avro"
    with (
        destination.open("wb") as stream,
        AvroWriter(SCHEMA, stream=stream, sync_marker=SYNC) as writer,
    ):
        writer.extend(ROWS)

    assert list(read_container(destination)) == ROWS


def test_invalid_containers_and_options_are_rejected() -> None:
    with pytest.raises(AvroError, match="unsupported Avro container codec"):
        AvroWriter(SCHEMA, codec="snappy")
    with pytest.raises(ValueError, match="sync_marker"):
        AvroWriter(SCHEMA, sync_marker=b"short")
    with pytest.raises(ValueError, match="sync_interval"):
        AvroWriter(SCHEMA, sync_interval=0)
    with pytest.raises(AvroDecodeError, match="magic bytes"):
        read_container(b"not-an-avro-file")

    writer = AvroWriter(SCHEMA, sync_marker=SYNC)
    writer.extend(ROWS)
    payload = bytearray(writer.close())
    payload[-1] ^= 0xFF
    with pytest.raises(AvroDecodeError, match="sync marker"):
        list(read_container(bytes(payload)))

    closed = AvroWriter(SCHEMA)
    closed.close()
    with pytest.raises(AvroError, match="closed"):
        closed.append(ROWS[0])
