# Apache Arrow

PyArrow is required by the core package. RKP infers Arrow types, fields, and
schemas from annotations and uses the resulting schema for every tabular
interop path.

## Types, fields, and schemas

```python
from dataclasses import dataclass, fields
from datetime import UTC, datetime

import pyarrow as pa

from rkp import (
    Record,
    dataclass_into_arrow_field,
    dataclass_into_arrow_schema,
    field,
    into_arrow_field,
    into_arrow_schema,
    into_arrow_type,
    record,
)


@dataclass
class Point:
    x: float
    y: float


@record(table_name="events")
class Event(Record):
    identifier: int = field(alias="event_id", seq=1)
    location: Point
    occurred_at: datetime = datetime(1970, 1, 1, tzinfo=UTC)


assert into_arrow_type(list[int]) == pa.list_(pa.int64())
assert into_arrow_field("label", str | None).nullable
assert dataclass_into_arrow_field(Point).type.num_fields == 2
assert dataclass_into_arrow_schema(Point).names == ["x", "y"]
assert into_arrow_field(fields(Event)[0], owner=Event).name == "event_id"
assert into_arrow_schema(Event) == Event.into_arrow_schema()
```

Inference covers scalar annotations, timestamps, decimals, UUIDs,
collections, maps, fixed tuples, nested dataclasses/records, `TypedDict`,
`NamedTuple`, enums, literals, unions, and explicit `Annotated`/field metadata.
Optional annotations control field nullability.

A plain `datetime` annotation maps to `timestamp[us, tz=UTC]`. Supply aware UTC
values for unambiguous instants. PyArrow interprets a naive value in that UTC
field as UTC and returns an aware UTC value, so its `tzinfo` changes on the
round trip. Override the field with `pa.timestamp("us")` when the domain is a
timezone-free wall clock. Iceberg applies a stricter UTC-or-naive policy at its
own protocol boundary.

Union annotations can be represented in inferred schemas as Arrow dense union
types, but runtime records-to-Arrow conversion intentionally rejects union
output: PyArrow requires an explicitly constructed `UnionArray` to choose type
codes and offsets. Use a tagged record/struct for portable runtime rows, or
construct that array and batch directly. Optional `T | None` is ordinary
nullability and is fully supported.

`into_arrow_schema()` accepts a dataclass type or instance, Arrow field/schema,
Iceberg schema, or an object whose `as_arrow()` method returns a schema.
`metadata=` overlays normalized schema metadata.

## One batch

```python
from rkp import arrow_batch_into_records, records_into_arrow_batch

events = (
    Event(1, Point(2.0, 3.0)),
    Event(2, Point(-1.0, 0.5)),
)

batch = records_into_arrow_batch(events, record_type=Event)
assert batch.schema == Event.into_arrow_schema()
assert tuple(arrow_batch_into_records(Event, batch)) == events

# Equivalent generated methods.
batch = Event.into_arrow_batch(events)
assert tuple(Event.from_arrow_batch(batch)) == events
```

For a non-empty iterable, `record_type` can be inferred. Empty input requires
`record_type=` or `schema=` so the result still has a contract. A supplied
schema is authoritative and may add transport metadata.

## Bounded batches and readers

```python
from rkp import (
    arrow_into_records,
    records_into_arrow_batches,
    records_into_arrow_reader,
)

batches = records_into_arrow_batches(
    iter(events),
    record_type=Event,
    batch_size=1,
)
assert [batch.num_rows for batch in batches] == [1, 1]

reader = records_into_arrow_reader(events, record_type=Event, batch_size=1)
assert tuple(arrow_into_records(Event, reader)) == events

# The class carries the type, including for an empty stream.
reader = Event.into_arrow_reader([], batch_size=1024)
assert reader.schema == Event.into_arrow_schema()
```

`records_into_arrow_batches()` is lazy, bounds each conversion by `batch_size`,
and consumes the source once. An empty source yields no batches. A
`RecordBatchReader` must publish its schema before consuming data, so its helper
always requires `record_type` or `schema`.

`arrow_into_records()` accepts a `RecordBatch`, `Table`, `RecordBatchReader`, or
iterable of batches. It returns a lazy iterator and checks delayed batch errors
when reached.

When raw mappings are the outgoing rows, their keys must be strings and match
the selected schema exactly. This prevents PyArrow from silently discarding an
unexpected key or filling a missing required column with null. Record and
dataclass inputs continue to use the alias-aware field projection.

## Validation and protocol schemas

Incoming validation compares names, ordering, nullability, and physical types
but ignores transport-added field/schema metadata. Keep it enabled for normal
round trips:

```python
restored = Event.from_arrow(batch, validate_schema=True)
```

Some protocols intentionally change an equivalent physical representation—for
example, Iceberg may return large list/string types. Accept that known boundary
while retaining alias-aware safe row conversion:

```python
restored = Event.from_arrow(protocol_batch, validate_schema=False)
```

`safe=` and `on_error="raise" | "default"` feed the same typed constructor as
`from_dict()`. Arrow maps reject duplicate keys instead of silently
overwriting them.

Run the complete batch/reader example:

```console
uv run --project python python docs/examples/arrow_batches.py
```

Source: [`examples/arrow_batches.py`](examples/arrow_batches.py).
