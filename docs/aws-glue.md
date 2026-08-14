# AWS Glue and DDL

Schema conversion, table inputs, and DDL generation are core features and do
not import boto3. Install the optional extra only when `GlueCatalog()` should
create its own AWS client:

```console
uv sync --project python --extra awsglue
```

## Columns and table inputs

```python
from rkp import (
    Record,
    arrow_into_glue_column,
    arrow_into_glue_columns,
    arrow_type_into_glue_type,
    field,
    glue_into_arrow_field,
    glue_into_arrow_schema,
    into_glue_columns,
    into_glue_table_input,
    record,
)


@record(schema_name="analytics", table_name="events")
class Event(Record):
    identifier: int = field(primary_key=True, doc="Event identifier")
    day: str = field(partition_key=1)
    payload: str | None = None


arrow_schema = Event.into_arrow_schema()
assert arrow_type_into_glue_type(arrow_schema.field("payload").type) == "string"
assert arrow_into_glue_column(arrow_schema.field("identifier"))["Name"] == "identifier"
assert arrow_into_glue_columns(arrow_schema) == into_glue_columns(Event)

table_input = into_glue_table_input(
    Event,
    location="s3://example-bucket/events/",
    format="parquet",
    description="Typed events",
    parameters={"classification": "parquet"},
    partition_keys=["day"],
)
assert table_input["PartitionKeys"][0]["Name"] == "day"
assert glue_into_arrow_schema(table_input).names == arrow_schema.names
assert glue_into_arrow_field(table_input["PartitionKeys"][0]).name == "day"
```

Supported classic external formats are Parquet, ORC, Avro, JSON, and CSV.
Partition columns are emitted in `PartitionKeys` and not duplicated in
`StorageDescriptor.Columns`.

RKP embeds the complete Arrow schema in table parameters for lossless reverse
conversion, then validates it against live Glue columns and partition keys.
External table definitions without that payload are parsed from Glue/Hive type
syntax.

## Partition values and projection

Partition values can be projected directly from a record. The output always
uses the table's partition-key order and Glue's required string representation;
field aliases and physical types are resolved through Arrow:

```python
from datetime import date

from rkp import into_glue_partition_values

event = Event(identifier=1, day="2026-08-14", payload=None)
assert into_glue_partition_values(event) == ["2026-08-14"]

# A mapping can use an Arrow schema, Glue table definition, or an explicit key order.
values = into_glue_partition_values(
    {"day": date(2026, 8, 15)},
    partition_keys=["day"],
)
assert values == ["2026-08-15"]
```

`event.into_glue_partition_values()` is the equivalent record convenience.
Null, missing, nested, out-of-range, and non-finite values are rejected before
an AWS request. `GlueCatalog.partition_values()` uses the live table definition,
and `create_partition_from()` can create a partition directly from a record or
mapping:

```python
catalog.create_partition_from(
    "analytics",
    "events",
    event,
    location="s3://example-bucket/events/day=2026-08-14/",
)
```

Athena partition projection is available as a validated structured mapping:

```python
projection = {
    "day": {
        "type": "date",
        "range": ["2025-01-01", "NOW"],
        "format": "yyyy-MM-dd",
        "interval": 1,
        "interval_unit": "DAYS",
    }
}

table_input = Event.into_glue_table_input(
    location="s3://example-bucket/events/",
    partition_projection=projection,
    partition_location_template="s3://example-bucket/events/day=${day}/",
)
```

The same options work with `into_glue_ddl()` and its record method. Supported
projection types are `enum`, `integer`, `date`, and `injected`; RKP validates
their required properties, compatible partition types, integer bounds, enum
values, and every `${column}` placeholder in a custom location template. See
the AWS documentation for [supported projection types](https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html)
and [custom location templates](https://docs.aws.amazon.com/athena/latest/ug/partition-projection-setting-up.html).

## Deterministic DDL

```python
from rkp import (
    into_glue_database_ddl,
    into_glue_ddl,
    into_glue_drop_database_ddl,
    into_glue_drop_table_ddl,
)

create_database = into_glue_database_ddl(
    "analytics",
    description="Analytics data",
    location="s3://example-bucket/analytics/",
)
create_table = into_glue_ddl(
    Event,
    database="analytics",
    location="s3://example-bucket/events/",
    format="parquet",
    properties={"owner": "data-platform"},
)
drop_table = into_glue_drop_table_ddl("events", database="analytics")
drop_database = into_glue_drop_database_ddl("analytics", cascade=True)
```

`Event.into_glue_ddl(...)` and `Event.into_glue_table_input(...)` are equivalent
record conveniences. Identifiers and literals are escaped and output ordering
is deterministic. Applications must still choose identifiers accepted by the
query engine that executes the DDL.

These helpers create classic Hive-compatible external table definitions. They
do not create Iceberg table metadata in S3; use a PyIceberg catalog for an
Iceberg table lifecycle.

## GlueCatalog

`GlueCatalog` is a small synchronous facade over a Glue-compatible client. A
configured boto3 client, Moto client, or test double can be injected:

```python
from rkp import GlueCatalog

catalog = GlueCatalog(glue_client, catalog_id="123456789012")
database = catalog.ensure_database(
    "analytics",
    description="Analytics data",
)
table = catalog.upsert_table("analytics", table_input)
stored = catalog.get_table("analytics", "events")
```

Omit the client to lazily construct `boto3.client("glue",
region_name=...)`. The database operations are:

```python
catalog.create_database("analytics", exist_ok=True)
catalog.get_database("analytics")
catalog.update_database("analytics", description="Updated")
catalog.ensure_database("analytics")
catalog.list_databases()
catalog.delete_database("analytics", missing_ok=True)
```

Tables provide `create_table`, `get_table`, `update_table`, `upsert_table`,
`list_tables`, and `delete_table`. Partition methods mirror that lifecycle:

```python
partition = {
    "Values": ["2026-08-14"],
    "StorageDescriptor": {
        **table_input["StorageDescriptor"],
        "Location": "s3://example-bucket/events/day=2026-08-14/",
    },
}

catalog.upsert_partition("analytics", "events", partition)
catalog.get_partition("analytics", "events", ["2026-08-14"])
catalog.list_partitions("analytics", "events")
catalog.delete_partition(
    "analytics", "events", ["2026-08-14"], missing_ok=True
)
```

`batch_create_partitions()` accepts 1–100 partition inputs per request;
`batch_delete_partitions()` accepts 1–25 ordered value lists. List methods
consume all paginator pages.

The runnable example uses a tiny injected in-memory client, so it needs no AWS
credentials and performs no network calls:

```console
uv run --project python python docs/examples/glue.py
```

Source: [`examples/glue.py`](examples/glue.py).
