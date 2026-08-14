# Apache Iceberg

Install the optional PyIceberg adapter:

```console
uv sync --project python --extra iceberg
```

RKP treats an Iceberg field as the primary conversion unit, then coordinates
those conversions into a schema with globally unique IDs.

## Record to Iceberg

```python
from datetime import datetime

from rkp import Record, field, record


@record(table_name="observations")
class Observation(Record):
    identifier: int = field(seq=1, primary_key=True)
    observed_at: datetime
    labels: dict[str, str | None]


iceberg_field = Observation.into_iceberg_field()
iceberg_schema = Observation.into_iceberg_schema(
    schema_id=10,
    format_version=2,
)
assert iceberg_schema.schema_id == 10
assert iceberg_schema.find_field("identifier").field_id == 1
```

`seq` is projected to the Iceberg field ID. Missing IDs are allocated
deterministically without colliding with explicit nested IDs. `primary_key`
becomes Iceberg identifier membership. Partition and index roles remain Arrow
metadata because Iceberg models partition specs and sort orders outside its
schema.

Ordinary dataclasses, attached fields, Arrow fields/schemas, and existing
Iceberg objects use the same utilities:

```python
from dataclasses import fields

from rkp import (
    arrow_into_iceberg_field,
    arrow_into_iceberg_schema,
    dataclass_into_iceberg_field,
    dataclass_into_iceberg_schema,
    iceberg_fields_into_schema,
    into_iceberg_field,
    into_iceberg_schema,
)

attached = fields(Observation)[0]
one = into_iceberg_field(attached, owner=Observation)
same = attached.into_iceberg_field(owner=Observation)
composed = iceberg_fields_into_schema(one, identifier_field_ids=[one.field_id])

from_arrow = arrow_into_iceberg_schema(Observation.into_arrow_schema())
assert into_iceberg_schema(from_arrow) is from_arrow
```

Use `iceberg_fields_into_schema()` when composing independently converted
fields: it validates positive, globally unique IDs, including list elements and
map keys/values.

## Return through Arrow

```python
from rkp import iceberg_into_arrow_field, iceberg_into_arrow_schema

canonical = Observation.into_arrow_schema()
protocol_schema = iceberg_into_arrow_schema(
    iceberg_schema,
    metadata=canonical.metadata,
    include_field_ids=True,
)
protocol_field = iceberg_into_arrow_field(
    iceberg_schema.find_field("identifier"),
    primary_key=True,
)
```

PyIceberg `Schema` has no arbitrary metadata container. Pass the source Arrow
metadata explicitly when catalog/schema/table names must survive an
Iceberg-only boundary.

Iceberg's Arrow projection may use equivalent large physical types. When a
batch uses that projection, reconstruct records with
`validate_schema=False`; row conversion remains typed and alias-aware.

## Timestamp policy

`format_version=` accepts Iceberg versions 1, 2, and 3. Leaving
`downcast_ns_timestamp_to_us` unset (`...`) or passing `None` selects an
adaptive default:

- v1/v2 downcast nanoseconds to microseconds;
- v3 preserves nanoseconds.

Set `False` for strict lossless conversion (v1/v2 then reject nanoseconds), or
`True` to force microseconds. Timezone-aware Arrow timestamps must be UTC or an
equivalent zero-offset name.

This option controls schema conversion, not later writes. Configure the
corresponding PyIceberg writer setting when writing nanosecond data to a
microsecond schema.

Run the local example:

```console
uv run --project python --extra iceberg python docs/examples/iceberg.py
```

Source: [`examples/iceberg.py`](examples/iceberg.py).
