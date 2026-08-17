# Apache Iceberg

Install the optional PyIceberg adapter:

```console
uv sync --project python --extra iceberg
```

RKP treats an Iceberg field as the primary conversion unit, then coordinates
those conversions into a schema with globally unique IDs.

Conversion is RKP's own: Arrow and Iceberg types meet in the neutral field
model of `rkp.records.datatypes`, which every adapter (Arrow, Avro, Iceberg,
Glue) shares. That keeps one traversal, one identity rule, and one error
vocabulary, and it lets RKP accept Arrow types that convert losslessly —
unsigned integers, half floats, second/millisecond temporal units, view types,
dictionaries, and fixed-size lists — instead of rejecting them.

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

## Avro representation

Iceberg describes its own schemas in Avro, and RKP emits exactly that
representation through its built-in [Avro](avro.md) implementation:

```python
from rkp import avro_into_iceberg_schema, iceberg_into_avro_schema

avro_schema = iceberg_into_avro_schema(iceberg_schema, name="observations")
assert avro_schema.field("identifier").attributes["field-id"] == 1
assert avro_into_iceberg_schema(avro_schema).as_struct() == iceberg_schema.as_struct()
```

Field IDs become `field-id`, `element-id`, `key-id`, and `value-id`
attributes, decimals and UUIDs use fixed storage sized by their precision, and
timestamps carry an explicit `adjust-to-utc` flag. The result is an
`rkp.avro` schema, so it can be fingerprinted, reduced to canonical form, or
used directly to encode data.

An Avro declaration without `field-id` attributes is still convertible:
missing IDs are allocated deterministically by the same allocator used
everywhere else.

## Catalog operations

With a live PyIceberg catalog, the same record contract creates the namespace
and table, projects its partition spec and sort order, writes records, and
reads them back:

```python
from rkp import (
    create_iceberg_table,
    iceberg_table_into_records,
    records_into_iceberg_table,
    sync_iceberg_table_schema,
)

table = create_iceberg_table(catalog, Observation, format_version=2)
records_into_iceberg_table(table, values, record_type=Observation)
restored = list(iceberg_table_into_records(Observation, table))
sync_iceberg_table_schema(table, ObservationV2)
```

- The identifier defaults to the record's `schema_name`/`table_name` metadata;
  pass `identifier=("analytics", "observations")` or `"analytics.observations"`
  to override it.
- `partition_key` roles become the `PartitionSpec`. `True` partitions by
  identity, an integer also fixes the partition column order, and a string
  names an Iceberg transform: `"day"`, `"hour"`, `"month"`, `"year"`,
  `"bucket[16]"`, `"truncate[8]"`, `"void"`. `index_key` roles become the
  `SortOrder`.
- `records_into_iceberg_table()` writes through the table's own Arrow
  projection with `mode="append"` or `mode="overwrite"`.
- `sync_iceberg_table_schema()` evolves a live table by Iceberg's union-by-name
  rules: existing columns keep their IDs and new columns are added.
- `into_iceberg_partition_spec()` and `into_iceberg_sort_order()` expose the
  projections on their own, without a catalog.

`create_iceberg_table()` is idempotent by default (`exists_ok=True`) and
writes `format-version` as a table property.

## Format versions 1, 2, and 3

`format_version=` accepts 1, 2, and 3 everywhere a schema is built. Schema
conversion is complete for all three:

| Capability | v1 | v2 | v3 |
| --- | --- | --- | --- |
| Scalar, struct, list, and map types | yes | yes | yes |
| Identifier fields from `primary_key` | yes | yes | yes |
| Nanosecond timestamps | downcast | downcast | preserved |
| `unknown` columns (Arrow `null`) | rejected | rejected | yes |
| `initial-default` / `write-default` | rejected | rejected | yes |

Field defaults travel as Arrow metadata so they survive a full round trip:

```python
import pyarrow as pa
from rkp import arrow_into_iceberg_schema

schema = arrow_into_iceberg_schema(
    pa.schema(
        [
            pa.field(
                "count",
                pa.int64(),
                nullable=False,
                metadata={
                    b"PARQUET:field_id": b"1",
                    b"iceberg.initial_default": b"7",
                    b"iceberg.write_default": b"9",
                },
            )
        ]
    ),
    format_version=3,
)
assert schema.find_field("count").initial_default == 7
```

Defaults are encoded with Iceberg's single-value JSON serialization, so dates,
timestamps, decimals, UUIDs, and binaries are unambiguous.

Table *metadata* writing is a runtime concern, not a schema one: PyIceberg
0.11.x cannot yet write v3 table metadata, so `create_iceberg_table(...,
format_version=3)` raises a `NotImplementedError` that names the installed
PyIceberg version. v3 schema conversion, the Avro representation, and
nanosecond types are unaffected.

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
