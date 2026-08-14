# Records, fields, and metadata

Records are dataclasses. `@record` accepts the Python 3.11 dataclass options
(`frozen`, `slots`, `kw_only`, and the rest), plus record-specific controls:

```python
from rkp import Record, field, record


@record(
    frozen=True,
    slots=True,
    alias="events",
    catalog_name="lakehouse",
    schema_name="analytics",
    table_name="user_events",
    metadata={"owner": "growth", "retention_days": 30},
    with_json=True,
    with_yaml=True,
)
class Event(Record):
    event_id: int = field(
        alias="eventId",
        seq=1,
        primary_key=True,
        doc="Stable event identifier",
    )
    region: str = field(partition_key=1, index_key=True)
    note: str | None = None
```

`alias` names a record when it is nested as a field. `catalog_name`,
`schema_name`, and `table_name` are protocol-neutral names carried by Arrow
schema metadata. If no table name is declared, a lowercased class-name fallback
is used.

## Field controls

`field()` mirrors `dataclasses.field()` and stores every interop option in one
immutable metadata mapping.

| Control | Purpose |
| --- | --- |
| `alias` | Shared JSON/YAML/Arrow wire name. |
| `type` | Adapter-specific physical type override. |
| `nullable` | Explicit nullability override. |
| `doc` | Documentation carried into schema protocols. |
| `seq` | Stable positive identity used as the Iceberg/Parquet field ID. |
| `primary_key` | Primary/identifier role; a Boolean, position, or name token. |
| `partition_key` | Partition role and optional ordering token. |
| `index_key` | Index/sort role and optional ordering token. |
| `metadata` | Portable payload metadata plus the reserved `rkp` controls. |

`field_id` and `iceberg_field_id` are compatibility aliases for `seq`; new code
should use `seq`. IDs must be unique across the complete nested schema.

For a type factory with parameters, use the canonical metadata mapping:

```python
import pyarrow as pa
from rkp import field

occurred_at = field(
    metadata={
        "source": "event-clock",
        "rkp": {
            "type": pa.timestamp,
            "parameters": {"unit": "ms", "tz": "UTC"},
        },
    }
)
```

Inspect an attached field without depending on its storage layout:

```python
from dataclasses import fields
from rkp import field_options

options = field_options(fields(Event)[0])
assert options.alias == "eventId"
assert options.seq == 1
assert options.primary_key is True
assert options.doc == "Stable event identifier"
```

## Construction and plain values

```python
from rkp import dataclass_from_dict, record_from_dict, to_dict

event = Event.from_dict({"eventId": "42", "region": "eu-west"})
same = record_from_dict(Event, {"eventId": 42, "region": "eu-west"})
also_same = dataclass_from_dict(Event, {"eventId": 42, "region": "eu-west"})

assert event == same == also_same
assert to_dict(event)["eventId"] == 42
assert to_dict(event, by_alias=False)["event_id"] == 42
```

`dataclass_from_dict()` also supports ordinary dataclass types. Conversion
handles nested dataclasses, mappings, collections, tuples and named tuples,
enums, dates/times, decimals, UUIDs, paths, and unions. Unknown or duplicate
input names fail with a field path.

## Portable metadata

```python
from rkp import (
    catalog_name,
    record_metadata,
    schema_metadata,
    schema_name,
    table_name,
)

metadata = record_metadata(Event)
assert metadata.catalog_name == "lakehouse"
assert metadata.payload_metadata["owner"] == "growth"
assert catalog_name(Event) == "lakehouse"
assert schema_name(Event) == "analytics"
assert table_name(Event) == "user_events"
assert schema_metadata(Event)[b"retention_days"] == b"30"
```

`RecordMetadata.merged()` returns a new immutable snapshot. Omitted values use
the global `...` sentinel and inherit; an explicit `None` clears a portable
name or generic payload layer.

See the complete local example in [`examples/basic.py`](examples/basic.py).
