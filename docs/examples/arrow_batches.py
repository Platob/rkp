"""Move records through Arrow schemas, batches, and streaming readers."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime

import pyarrow as pa
from rkp import (
    Record,
    arrow_into_records,
    field,
    record,
    records_into_arrow_batch,
    records_into_arrow_batches,
    records_into_arrow_reader,
)


@record(table_name="events")
class Event(Record):
    identifier: int = field(alias="event_id", seq=1, primary_key=True)
    label: str | None = None
    occurred_at: datetime = datetime(1970, 1, 1, tzinfo=UTC)


EVENTS = (
    Event(1, "created", datetime(2026, 8, 14, 10, 0, tzinfo=UTC)),
    Event(2, None),
)


def source() -> Iterator[Event]:
    yield from EVENTS


def main() -> None:
    schema = Event.into_arrow_schema()
    assert schema.field(0).name == "event_id"

    batch = records_into_arrow_batch(EVENTS, record_type=Event)
    assert tuple(Event.from_arrow_batch(batch)) == EVENTS

    batches = records_into_arrow_batches(source(), record_type=Event, batch_size=1)
    table = pa.Table.from_batches(batches, schema=schema)
    assert tuple(arrow_into_records(Event, table)) == EVENTS

    reader = records_into_arrow_reader(EVENTS, record_type=Event, batch_size=1)
    assert tuple(Event.from_arrow(reader)) == EVENTS

    # The classmethod forms are equivalent and retain the record type for empty input.
    empty_reader = Event.into_arrow_reader([], batch_size=128)
    assert empty_reader.schema == schema
    assert tuple(Event.from_arrow(empty_reader)) == ()
    print(batch)


if __name__ == "__main__":
    main()
