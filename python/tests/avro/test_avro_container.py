from __future__ import annotations

import io
import random
from pathlib import Path
from typing import Any

import pytest
from rkp.avro import (
    CODECS,
    Avro,
    AvroBlock,
    AvroDecodeError,
    AvroError,
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


def _written(**options: Any) -> bytes:
    container = Avro.create(SCHEMA, sync_marker=SYNC, **options)
    container.extend(ROWS)
    return container.close() or b""


@pytest.mark.parametrize("codec", CODECS)
def test_every_codec_round_trips_and_indexes(codec: str) -> None:
    payload = _written(codec=codec)

    container = read_container(payload)

    assert payload.startswith(b"Obj\x01")
    assert container.codec == codec
    assert container.schema == SCHEMA
    assert list(container) == ROWS
    assert len(container) == len(ROWS)
    assert container[0] == ROWS[0]
    assert container[4] == ROWS[4]
    assert container[-1] == ROWS[-1]
    assert container[2:5] == ROWS[2:5]
    assert container.get(99) is None
    assert container.get(99, "fallback") == "fallback"
    with pytest.raises(IndexError, match="out of range"):
        container[99]


def test_iteration_is_re_entrant_and_leaves_no_cursor() -> None:
    container = read_container(_written())

    assert list(container) == ROWS
    assert list(container) == ROWS
    first = iter(container)
    second = iter(container)
    assert next(first) == ROWS[0]
    assert next(second) == ROWS[0]
    assert container[6] == ROWS[6]
    assert list(container.iter_from(3, 5)) == ROWS[3:5]
    assert list(container.iter_from(-2)) == ROWS[-2:]


def test_blocks_tile_the_file_and_locate_records() -> None:
    payload = _written(sync_interval=16)

    container = read_container(payload)
    blocks = container.blocks()

    assert len(blocks) > 1
    assert all(isinstance(block, AvroBlock) for block in blocks)
    assert blocks[-1].end == len(payload)
    assert sum(block.count for block in blocks) == len(ROWS)
    assert [block.first for block in blocks] == [
        sum(item.count for item in blocks[:index]) for index in range(len(blocks))
    ]
    for index, row in enumerate(ROWS):
        block = container.block_of(index)
        assert block.first <= index < block.stop
        assert container.read_block(block.ordinal)[index - block.first] == row
    assert [row for _, rows in container.iter_blocks() for row in rows] == ROWS


def test_blocks_flush_at_the_sync_interval() -> None:
    payload = _written(sync_interval=1)

    # One sync marker follows the header and one follows every flushed block.
    assert payload.count(SYNC) == len(ROWS) + 1
    assert len(read_container(payload).blocks()) == len(ROWS)


def test_random_reads_reuse_one_decoded_block() -> None:
    container = read_container(_written(codec="deflate", sync_interval=16))

    cold = container.nbytes
    assert container[3] == ROWS[3]
    warm = container.nbytes
    assert warm > cold
    assert container[4] == ROWS[4]
    # The second read of the same block adds no payload to the cache.
    assert container.nbytes == warm


def test_a_small_cache_budget_still_returns_every_record() -> None:
    payload = _written(codec="deflate", sync_interval=1)

    container = read_container(payload, cache_bytes=1)

    assert [container[index] for index in range(len(ROWS))] == ROWS


def test_metadata_is_preserved_and_reserved_keys_are_owned() -> None:
    container = Avro.create(
        SCHEMA,
        metadata={"writer": "rkp", "avro.codec": "ignored"},
        sync_marker=SYNC,
    )
    container.extend(ROWS[:1])
    reader = read_container(container.close() or b"")

    assert reader.metadata["writer"] == b"rkp"
    assert reader.metadata["avro.codec"] == b"null"
    assert reader.writer_schema == SCHEMA
    assert reader.sync_marker == SYNC


def test_records_are_replaced_in_place_of_the_owning_block() -> None:
    payload = _written(sync_interval=16)
    container = Avro(payload, mode="r+")
    prefix = container.block_of(len(ROWS) - 1).offset

    container[len(ROWS) - 1] = {"identifier": 900, "label": "edited"}

    assert container[len(ROWS) - 1] == {"identifier": 900, "label": "edited"}
    assert container[0] == ROWS[0]
    image = container.into_bytes()
    # Blocks before the edited one are copied byte for byte.
    assert image[:prefix] == payload[:prefix]
    assert list(read_container(image))[-1] == {"identifier": 900, "label": "edited"}


def test_insert_delete_and_pop_renumber_immediately() -> None:
    container = Avro(_written(sync_interval=16), mode="r+")

    container.insert(0, {"identifier": -1, "label": "first"})
    removed = container.pop(3)
    del container[1]

    expected = [{"identifier": -1, "label": "first"}, *ROWS]
    assert removed == expected.pop(3)
    expected.pop(1)
    assert len(container) == len(expected)
    assert list(container) == expected
    assert container[1] == expected[1]
    assert list(read_container(container.into_bytes())) == expected


def test_contiguous_slices_are_replaced_and_deleted_across_blocks() -> None:
    container = Avro(_written(sync_interval=1), mode="r+")

    container[2:5] = [{"identifier": 20, "label": None}]
    del container[0:2]

    expected = [{"identifier": 20, "label": None}, *ROWS[5:]]
    assert list(container) == expected
    assert list(read_container(container.into_bytes())) == expected
    with pytest.raises(ValueError, match="contiguous slices"):
        container[::2] = []
    with pytest.raises(ValueError, match="contiguous slices"):
        del container[::2]
    with pytest.raises(ValueError, match="contiguous slices"):
        container[::2]


def test_staged_appends_are_readable_and_editable_before_framing() -> None:
    container = Avro(_written(), mode="r+")

    container.append({"identifier": 100, "label": "staged"})
    assert len(container) == len(ROWS) + 1
    assert container[-1] == {"identifier": 100, "label": "staged"}

    container[-1] = {"identifier": 101, "label": "changed"}
    assert container[-1] == {"identifier": 101, "label": "changed"}
    del container[-1]
    assert len(container) == len(ROWS)
    assert list(read_container(container.into_bytes())) == ROWS


def test_truncate_clear_and_compact_keep_the_header() -> None:
    container = Avro(_written(sync_interval=1), mode="r+")

    assert container.truncate(4) == 4
    assert list(container) == ROWS[:4]

    container.compact()
    assert len(container.blocks()) == 1
    assert list(container) == ROWS[:4]

    container.clear()
    assert len(container) == 0
    assert container.blocks() == ()
    assert list(container) == []
    reopened = read_container(container.into_bytes())
    assert reopened.schema == SCHEMA
    assert reopened.sync_marker == SYNC
    assert list(reopened) == []
    with pytest.raises(IndexError, match="out of range"):
        reopened[0]


def test_mutating_during_iteration_is_refused() -> None:
    container = Avro(_written(sync_interval=1), mode="r+")

    with pytest.raises(RuntimeError, match="changed during iteration"):
        for index, _row in enumerate(container):
            if index == 1:
                container[0] = {"identifier": 0, "label": "changed"}


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
    assert write_container("buffer", SCHEMA, ROWS, sync_marker=SYNC) == _written()


def test_a_path_container_writes_through_and_appends_in_place(
    tmp_path: Path,
) -> None:
    destination = tmp_path / "rows.avro"
    with Avro.create(SCHEMA, destination, sync_marker=SYNC, sync_interval=16) as new:
        new.extend(ROWS)
    original = destination.read_bytes()

    with Avro(destination, mode="a") as appended:
        appended.append({"identifier": 99, "label": "appended"})
    grown = destination.read_bytes()

    # An append never rewrites the records that were already durable.
    assert grown.startswith(original)
    assert len(list(read_container(destination))) == len(ROWS) + 1

    with Avro(destination, mode="r+") as edited:
        edited[0] = {"identifier": 0, "label": "rewritten"}
    assert read_container(destination)[0] == {"identifier": 0, "label": "rewritten"}
    assert [item.name for item in tmp_path.iterdir()] == ["rows.avro"]


def test_scattered_edits_stay_durable_across_reopens(tmp_path: Path) -> None:
    """Whatever an edit does to the image, what lands on disk must match it.

    The append fast path only writes the tail, so a rewrite it failed to notice
    would leave a file that still parses and still lies.
    """

    destination = tmp_path / "rows.avro"
    expected = [{"identifier": index, "label": f"v{index}"} for index in range(12)]
    write_container(destination, SCHEMA, expected, sync_interval=32)
    rng = random.Random(20260817)

    for step in range(60):
        mode = "a" if step % 4 == 3 else "r+"
        with Avro(destination, mode=mode) as container:
            assert list(container) == expected
            if mode == "a":
                row = {"identifier": 1000 + step, "label": None}
                container.append(row)
                expected.append(row)
            else:
                choice = rng.randrange(4)
                position = rng.randrange(len(expected))
                if choice == 0:
                    row = {"identifier": -step, "label": f"s{step}"}
                    container[position] = row
                    expected[position] = row
                elif choice == 1:
                    row = {"identifier": -1000 - step, "label": None}
                    container.insert(position, row)
                    expected.insert(position, row)
                elif choice == 2:
                    del container[position]
                    expected.pop(position)
                else:
                    container.compact()
            assert list(container) == expected
        assert list(read_container(destination)) == expected
        assert [item.name for item in tmp_path.iterdir()] == ["rows.avro"]


def test_a_seekable_stream_container_writes_back_in_place() -> None:
    stream = io.BytesIO(_written())

    with Avro(stream, mode="r+") as container:
        container.append({"identifier": 42, "label": "streamed"})
        container[0] = {"identifier": 0, "label": "edited"}

    restored = list(read_container(stream.getvalue()))
    assert restored[0] == {"identifier": 0, "label": "edited"}
    assert restored[-1] == {"identifier": 42, "label": "streamed"}
    assert not stream.closed


def test_a_write_only_stream_receives_the_image_on_close() -> None:
    stream = io.BytesIO()

    with Avro.create(SCHEMA, stream, sync_marker=SYNC, sync_interval=16) as container:
        container.extend(ROWS)

    assert list(read_container(stream.getvalue())) == ROWS
    assert not stream.closed


def test_modes_gate_what_the_container_may_do() -> None:
    payload = _written()

    reader = Avro(payload)
    assert (reader.mode, reader.writable, reader.appendable) == ("r", False, False)
    with pytest.raises(AvroError, match="use mode='r\\+'"):
        reader[0] = ROWS[0]
    with pytest.raises(AvroError, match="use mode='a' or mode='r\\+'"):
        reader.append(ROWS[0])
    with pytest.raises(AvroError, match="open for reading"):
        reader.flush()

    appender = Avro(payload, mode="a")
    assert (appender.writable, appender.appendable) == (False, True)
    appender.append({"identifier": 5, "label": None})
    with pytest.raises(AvroError, match="use mode='r\\+'"):
        del appender[0]

    writer = Avro(payload, mode="r+")
    writer[0] = {"identifier": 0, "label": "ok"}
    assert writer.dirty is True
    assert writer.path is None
    assert "mode='r+'" in repr(writer)


def test_creation_options_are_refused_when_opening_an_existing_container() -> None:
    payload = _written()

    with pytest.raises(ValueError, match="metadata and sync_marker"):
        Avro(payload, sync_marker=SYNC)
    with pytest.raises(ValueError, match="metadata and sync_marker"):
        Avro(payload, metadata={"a": "b"})
    with pytest.raises(ValueError, match="codec describes a new container"):
        Avro(payload, codec="deflate")
    with pytest.raises(ValueError, match="requires a schema"):
        Avro.create(None)
    with pytest.raises(ValueError, match="requires a source"):
        Avro(None)
    with pytest.raises(ValueError, match="mode must be"):
        Avro(payload, mode="rw")


def test_invalid_containers_and_options_are_rejected() -> None:
    with pytest.raises(AvroError, match="unsupported Avro container codec"):
        Avro.create(SCHEMA, codec="snappy")
    with pytest.raises(ValueError, match="sync_marker"):
        Avro.create(SCHEMA, sync_marker=b"short")
    with pytest.raises(ValueError, match="sync_interval"):
        Avro.create(SCHEMA, sync_interval=0)
    with pytest.raises(ValueError, match="cache_bytes"):
        Avro(_written(), cache_bytes=-1)
    with pytest.raises(AvroDecodeError, match="magic bytes"):
        read_container(b"not-an-avro-file")
    with pytest.raises(AvroDecodeError, match="does not match"):
        read_container(_written(), schema=parse_schema("string"))

    closed = Avro.create(SCHEMA)
    closed.close()
    with pytest.raises(AvroError, match="closed"):
        closed.append(ROWS[0])
    with pytest.raises(AvroError, match="closed"):
        closed.flush()


def test_a_separator_free_string_names_a_buffer_not_a_file() -> None:
    with pytest.raises(AvroDecodeError, match="magic bytes.*Path"):
        read_container("rows.avro")


def test_truncated_containers_report_the_intact_prefix() -> None:
    payload = _written(sync_interval=16)

    with pytest.raises(AvroDecodeError, match="truncated Avro container block"):
        read_container(payload[:-1])
    with pytest.raises(AvroDecodeError, match="truncated Avro container block"):
        read_container(payload[:-20])


def test_corrupt_framing_is_rejected_before_it_is_decoded() -> None:
    payload = bytearray(_written(sync_interval=16))
    payload[-1] ^= 0xFF
    with pytest.raises(AvroDecodeError, match="sync marker mismatch"):
        read_container(bytes(payload))

    body = read_container(_written()).blocks()[0].offset
    negative = bytearray(_written())
    # A corrupt zig-zag count decodes to a negative record count.
    negative[body] = 0x03
    with pytest.raises(AvroDecodeError, match="negative Avro container block framing"):
        read_container(bytes(negative))


def test_a_wrong_record_count_is_caught_instead_of_decoding_garbage() -> None:
    payload = bytearray(_written(sync_interval=1))
    body = read_container(bytes(payload)).blocks()[0].offset
    # Claim two records in a block whose payload holds exactly one.
    payload[body] = 0x04

    with pytest.raises(AvroDecodeError, match="reads past its payload|ends elsewhere"):
        read_container(bytes(payload))[0]
