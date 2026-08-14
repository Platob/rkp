from __future__ import annotations

import enum
from datetime import UTC, datetime, time
from typing import NamedTuple, TypedDict

import pyarrow as pa
import pytest
from rkp import (
    Record,
    arrow_batch_into_records,
    arrow_into_records,
    record,
    records_into_arrow_batch,
    records_into_arrow_batches,
    records_into_arrow_reader,
)


class Pair(NamedTuple):
    name: str
    count: int


class Detail(TypedDict):
    enabled: bool
    note: str | None


class State(enum.IntEnum):
    READY = 1
    DONE = 2


@record
class StructuredRow(Record):
    pair: Pair
    fixed: tuple[int, str]
    detail: Detail
    state: State
    at: time
    created: datetime


def test_struct_and_scalar_values_round_trip_through_arrow_rows() -> None:
    value = StructuredRow(
        Pair("items", 3),
        (7, "seven"),
        {"enabled": True, "note": None},
        State.DONE,
        time(12, 34, 56, 789),
        datetime(2026, 8, 14, 12, 34, tzinfo=UTC),
    )

    batch = records_into_arrow_batch([value])

    assert tuple(arrow_batch_into_records(StructuredRow, batch)) == (value,)


def test_arrow_reader_helper_streams_bounded_batches() -> None:
    values = [
        StructuredRow(
            Pair(str(index), index),
            (index, str(index)),
            {"enabled": bool(index % 2), "note": None},
            State.READY,
            time(index),
            datetime(2026, 8, 14, index, tzinfo=UTC),
        )
        for index in range(3)
    ]

    reader = records_into_arrow_reader(
        values,
        record_type=StructuredRow,
        batch_size=2,
    )

    assert isinstance(reader, pa.RecordBatchReader)
    assert tuple(arrow_into_records(StructuredRow, reader)) == tuple(values)


def test_record_reader_method_handles_empty_iterable() -> None:
    reader = StructuredRow.into_arrow_reader([], batch_size=2)

    assert reader.schema == StructuredRow.into_arrow_schema()
    assert tuple(reader) == ()


def test_duplicate_arrow_map_keys_are_rejected() -> None:
    @record
    class Mapped(Record):
        values: dict[str, int]

    schema = Mapped.into_arrow_schema()
    array = pa.array(
        [[("key", 1), ("key", 2)]],
        type=schema.field("values").type,
    )
    batch = pa.RecordBatch.from_arrays([array], schema=schema)

    with pytest.raises((KeyError, TypeError, ValueError), match="duplicate|key"):
        tuple(Mapped.from_arrow_batch(batch))


def test_arrow_helpers_validate_empty_and_heterogeneous_inputs() -> None:
    @record
    class First(Record):
        value: int

    @record
    class Second(Record):
        value: int

    with pytest.raises(TypeError, match="empty records"):
        records_into_arrow_batch([])
    with pytest.raises(TypeError, match="all records"):
        records_into_arrow_batch([First(1), Second(2)])
    with pytest.raises(TypeError, match="record_type or schema"):
        records_into_arrow_reader([First(1)])

    wrong_schema = pa.schema([pa.field("other", pa.int64())])
    with pytest.raises(TypeError, match="schema fields"):
        records_into_arrow_batch([First(1)], schema=wrong_schema)


def test_arrow_runtime_rejects_nullable_union_before_consuming_rows() -> None:
    @record
    class UnionRow(Record):
        value: int | str | None

    consumed = False

    def rows():
        nonlocal consumed
        consumed = True
        yield UnionRow(1)

    with pytest.raises(TypeError, match=r"union type at 'value'"):
        records_into_arrow_batch(rows(), record_type=UnionRow)
    assert consumed is False

    with pytest.raises(TypeError, match=r"union type at 'value'"):
        records_into_arrow_batches(rows(), record_type=UnionRow)
    assert consumed is False

    with pytest.raises(TypeError, match=r"union type at 'value'"):
        records_into_arrow_reader(rows(), record_type=UnionRow)
    assert consumed is False


def test_arrow_runtime_reports_nested_union_paths_for_explicit_schemas() -> None:
    union = pa.dense_union(
        [
            pa.field("integer", pa.int64(), nullable=False),
            pa.field("text", pa.string(), nullable=False),
        ]
    )
    schema = pa.schema(
        [
            pa.field(
                "payload",
                pa.struct(
                    [
                        pa.field(
                            "choices",
                            pa.list_(pa.field("item", union, nullable=True)),
                        )
                    ]
                ),
            )
        ]
    )

    with pytest.raises(TypeError, match=r"union type at 'payload\.choices\[\]'"):
        records_into_arrow_batch([], schema=schema)


def test_schema_authoritative_mapping_rows_require_exact_string_fields() -> None:
    schema = pa.schema(
        [
            pa.field("identifier", pa.int64(), nullable=False),
            pa.field("label", pa.string(), nullable=True),
        ]
    )

    batch = records_into_arrow_batch(
        [{"identifier": 1, "label": None}],
        schema=schema,
    )
    assert batch.to_pylist() == [{"identifier": 1, "label": None}]

    with pytest.raises(
        TypeError,
        match=r"row at index 0.*missing 'label'.*unexpected 'extra'",
    ):
        records_into_arrow_batch([{"identifier": 1, "extra": "lost"}], schema=schema)

    with pytest.raises(TypeError, match=r"row at index 0.*non-string.*1"):
        records_into_arrow_batch(
            [{"identifier": 1, "label": None, 1: "lost"}],
            schema=schema,
        )


def test_lazy_mapping_batches_report_the_global_row_index() -> None:
    schema = pa.schema([pa.field("identifier", pa.int64(), nullable=False)])
    batches = records_into_arrow_batches(
        [
            {"identifier": 1},
            {"identifier": 2},
            {"wrong": 3},
        ],
        schema=schema,
        batch_size=1,
    )

    assert next(batches).to_pylist() == [{"identifier": 1}]
    assert next(batches).to_pylist() == [{"identifier": 2}]
    with pytest.raises(
        TypeError,
        match=r"row at index 2.*missing 'identifier'.*unexpected 'wrong'",
    ):
        next(batches)


def test_mapping_reader_preserves_row_diagnostics() -> None:
    schema = pa.schema([pa.field("identifier", pa.int64(), nullable=False)])
    reader = records_into_arrow_reader(
        [{"identifier": 1}, {}],
        schema=schema,
        batch_size=1,
    )

    assert reader.read_next_batch().to_pylist() == [{"identifier": 1}]
    with pytest.raises(TypeError, match=r"row at index 1.*missing 'identifier'"):
        reader.read_next_batch()
