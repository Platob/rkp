from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pyarrow as pa
import pytest
from rkp import (
    Record,
    arrow_batch_into_records,
    arrow_into_records,
    field,
    record,
    records_into_arrow_batch,
    records_into_arrow_batches,
    records_into_arrow_reader,
)


@record
class ArrowAddress(Record):
    city: str
    postcode: str | None


@record(metadata={"suite": "arrow-runtime"})
class ArrowEvent(Record):
    identifier: int = field(alias="event_id")
    label: str | None = None
    address: ArrowAddress | None = None
    tags: list[str] = field(default_factory=list)
    attributes: dict[str, int | None] = field(default_factory=dict)
    payload: bytes = b""
    occurred_at: datetime = datetime(1970, 1, 1, tzinfo=UTC)


@record
class ArrowScalarEdge(Record):
    identifier: UUID
    day: date
    elapsed: timedelta
    amount: Decimal


@record
class EmptyRecord(Record):
    pass


@record
class ArrowMapEdge(Record):
    values: dict[str, int]


EVENTS = (
    ArrowEvent(
        1,
        "created",
        ArrowAddress("Paris", None),
        ["edge", "arrow"],
        {"attempt": 1, "optional": None},
        b"\x00\xff",
        datetime(2026, 8, 14, 10, 11, 12, 123456, tzinfo=UTC),
    ),
    ArrowEvent(
        2,
        None,
        None,
        [],
        {},
        b"",
        datetime(1970, 1, 1, tzinfo=UTC),
    ),
    ArrowEvent(
        3,
        "updated",
        ArrowAddress("Tokyo", "100-0001"),
        ["unicode-雪"],
        {"negative": -7},
        b"payload",
        datetime(2038, 1, 19, 3, 14, 7, tzinfo=UTC),
    ),
)


def test_records_round_trip_through_one_alias_aware_batch() -> None:
    batch = records_into_arrow_batch(EVENTS, record_type=ArrowEvent)

    assert batch.schema == ArrowEvent.into_arrow_schema()
    assert batch.num_rows == len(EVENTS)
    assert batch.column_names[0] == "event_id"
    assert batch.schema.metadata == {
        b"suite": b"arrow-runtime",
        b"table_name": b"arrowevent",
    }
    assert tuple(arrow_batch_into_records(ArrowEvent, batch)) == EVENTS


def test_record_batch_methods_share_the_functional_api() -> None:
    batch = ArrowEvent.into_arrow_batch(EVENTS)

    assert batch.equals(records_into_arrow_batch(EVENTS, record_type=ArrowEvent))
    assert tuple(ArrowEvent.from_arrow_batch(batch)) == EVENTS
    assert tuple(ArrowEvent.from_arrow(batch)) == EVENTS


def test_nonempty_batches_infer_the_record_type() -> None:
    batch = records_into_arrow_batch(EVENTS)

    assert batch.schema == ArrowEvent.into_arrow_schema()
    assert tuple(arrow_batch_into_records(ArrowEvent, batch)) == EVENTS


def test_arrow_scalar_edge_types_round_trip_without_json_normalization() -> None:
    value = ArrowScalarEdge(
        UUID("12345678-1234-5678-1234-567812345678"),
        date(2026, 8, 14),
        timedelta(days=2, microseconds=7),
        Decimal("1234567890.000000000000000001"),
    )

    batch = records_into_arrow_batch([value])

    assert tuple(arrow_batch_into_records(ArrowScalarEdge, batch)) == (value,)


def test_zero_field_records_preserve_their_row_count() -> None:
    values = (EmptyRecord(), EmptyRecord(), EmptyRecord())

    batch = records_into_arrow_batch(values)

    assert batch.num_columns == 0
    assert batch.num_rows == len(values)
    assert tuple(arrow_batch_into_records(EmptyRecord, batch)) == values


def test_duplicate_arrow_map_keys_are_rejected_instead_of_overwritten() -> None:
    schema = ArrowMapEdge.into_arrow_schema()
    values = pa.array(
        [[("duplicate", 1), ("duplicate", 2)]],
        type=schema.field("values").type,
    )
    batch = pa.RecordBatch.from_arrays([values], schema=schema)

    with pytest.raises(
        (KeyError, TypeError), match=r"(?i)duplicate (?:map )?key|mapping"
    ):
        tuple(arrow_batch_into_records(ArrowMapEdge, batch))


def test_arrow_sources_accept_batch_table_reader_and_lazy_batch_iterable() -> None:
    batches = tuple(
        records_into_arrow_batches(EVENTS, record_type=ArrowEvent, batch_size=2)
    )
    expected_sizes = [2, 1]

    assert [batch.num_rows for batch in batches] == expected_sizes
    assert tuple(arrow_into_records(ArrowEvent, batches[0])) == EVENTS[:2]
    assert (
        tuple(arrow_into_records(ArrowEvent, pa.Table.from_batches(batches))) == EVENTS
    )

    reader = pa.RecordBatchReader.from_batches(batches[0].schema, batches)
    assert tuple(arrow_into_records(ArrowEvent, reader)) == EVENTS

    yielded: list[int] = []

    def source() -> Iterator[pa.RecordBatch]:
        for index, batch in enumerate(batches):
            yielded.append(index)
            yield batch

    records = arrow_into_records(ArrowEvent, source())
    assert yielded == []
    assert next(records) == EVENTS[0]
    assert yielded == [0]
    assert tuple(records) == EVENTS[1:]
    assert yielded == [0, 1]


def test_record_reader_composes_the_two_streaming_directions() -> None:
    reader = records_into_arrow_reader(
        EVENTS,
        record_type=ArrowEvent,
        batch_size=2,
    )

    assert reader.schema == ArrowEvent.into_arrow_schema()
    assert tuple(arrow_into_records(ArrowEvent, reader)) == EVENTS


def test_batch_production_is_lazy_bounded_and_consumes_input_once() -> None:
    consumed: list[int] = []

    def source() -> Iterator[ArrowEvent]:
        for value in EVENTS:
            consumed.append(value.identifier)
            yield value

    batches = records_into_arrow_batches(source(), record_type=ArrowEvent, batch_size=2)
    assert consumed == []

    first = next(batches)
    assert first.num_rows == 2
    assert consumed == [1, 2]

    second = next(batches)
    assert second.num_rows == 1
    assert consumed == [1, 2, 3]
    with pytest.raises(StopIteration):
        next(batches)
    assert consumed == [1, 2, 3]


def test_empty_inputs_have_explicit_single_and_streaming_semantics() -> None:
    batch = records_into_arrow_batch([], record_type=ArrowEvent)

    assert batch.num_rows == 0
    assert batch.schema == ArrowEvent.into_arrow_schema()
    assert (
        list(records_into_arrow_batches([], record_type=ArrowEvent, batch_size=2)) == []
    )
    assert list(arrow_batch_into_records(ArrowEvent, batch)) == []


def test_schema_validation_ignores_protocol_metadata_but_not_layout() -> None:
    batch = records_into_arrow_batch(EVENTS, record_type=ArrowEvent)
    protocol_schema = pa.schema(
        [field.with_metadata({b"protocol": b"external"}) for field in batch.schema],
        metadata={b"producer": b"external"},
    )
    metadata_only = pa.RecordBatch.from_arrays(batch.columns, schema=protocol_schema)

    assert tuple(arrow_batch_into_records(ArrowEvent, metadata_only)) == EVENTS

    wrong_order = pa.RecordBatch.from_arrays(
        list(reversed(batch.columns)),
        schema=pa.schema(list(reversed(batch.schema))),
    )
    with pytest.raises((TypeError, ValueError), match=r"(?i)schema|field|event_id"):
        tuple(arrow_batch_into_records(ArrowEvent, wrong_order))


def test_batch_iterable_reports_a_delayed_non_batch_item() -> None:
    valid = records_into_arrow_batch(EVENTS[:1], record_type=ArrowEvent)
    records = arrow_into_records(ArrowEvent, [valid, "not-a-batch"])  # type: ignore[list-item]

    assert next(records) == EVENTS[0]
    with pytest.raises(TypeError, match=r"index 1|RecordBatch"):
        next(records)


def test_disabled_schema_validation_still_uses_safe_record_coercion() -> None:
    batch = pa.RecordBatch.from_pylist(
        [{"event_id": "41"}],
        schema=pa.schema([pa.field("event_id", pa.string(), nullable=False)]),
    )

    assert tuple(
        arrow_batch_into_records(
            ArrowEvent,
            batch,
            validate_schema=False,
            on_error="default",
        )
    ) == (ArrowEvent(41),)


@pytest.mark.parametrize("batch_size", [0, -1, True, 1.5])
def test_invalid_batch_sizes_fail_before_consuming_records(batch_size: object) -> None:
    consumed = False

    def source() -> Iterator[ArrowEvent]:
        nonlocal consumed
        consumed = True
        yield EVENTS[0]

    with pytest.raises((TypeError, ValueError), match="batch_size"):
        list(
            records_into_arrow_batches(
                source(),
                record_type=ArrowEvent,
                batch_size=batch_size,  # type: ignore[arg-type]
            )
        )
    assert consumed is False


def test_batch_schema_override_can_add_protocol_metadata() -> None:
    canonical = ArrowEvent.into_arrow_schema()
    protocol_schema = canonical.with_metadata(
        {**(canonical.metadata or {}), b"protocol": b"integration"}
    )

    batch = records_into_arrow_batch(
        EVENTS, record_type=ArrowEvent, schema=protocol_schema
    )

    assert batch.schema == protocol_schema
    assert tuple(arrow_batch_into_records(ArrowEvent, batch)) == EVENTS
