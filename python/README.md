# rkp

`rkp` adds a small record layer to Python dataclasses, with built-in JSON,
YAML, and Apache Avro serialization, Apache Arrow interoperability, and
optional Apache Iceberg, Apache Spark, and AWS Glue Data Catalog
interoperability.

Protocol conversion runs through one neutral field model in
`rkp.records.datatypes`, so Arrow, Avro, Iceberg, and Glue agree on names,
nullability, stable identities, and precision instead of each adapter owning
its own mapping table.

The dependency-free `rkp.fix` package generates those same records and Arrow
metadata from selected OnixS FIX fields, components, repeating groups, and
messages. It uses an optimized SQLite cache and deterministic JSON.gz snapshots
under `~/.config/fix`; importing it never performs network I/O and full crawls
require an explicit call.

```python
from datetime import datetime
from rkp import Record, field, record


@record(
    alias="users",
    catalog_name="lakehouse",
    schema_name="analytics",
    table_name="user_facts",
    metadata={"owner": "identity"},
)
class User(Record):
    user_id: int = field(
        alias="userId",
        doc="Stable public identifier",
        seq=1,
        primary_key=True,
    )
    name: str = field(index_key=True)
    created_at: datetime = field(partition_key="day")
    email: str | None = None


user = User.from_dict(
    {
        "userId": "7",
        "name": "Ada",
        "created_at": "2026-08-13T12:00:00Z",
    }
)

json_text = user.dumps_json()
same_user = User.loads_json(json_text)
arrow_field = User.into_arrow_field()
arrow_schema = User.into_arrow_schema()
# With the optional ``rkp[iceberg]`` integration installed:
iceberg_field = User.into_iceberg_field()
iceberg_schema = User.into_iceberg_schema()
glue_table = User.into_glue_table_input(location="s3://warehouse/users/")
athena_ddl = User.into_glue_ddl(
    database="analytics",
    location="s3://warehouse/users/",
)
```

Record-level metadata is immutable and protocol-neutral. `catalog_name`,
`schema_name`, and `table_name` are projected as canonical Arrow schema
metadata under the UTF-8 byte keys `catalog_name`, `schema_name`, and
`table_name`, alongside ordinary payload metadata. Arrow is the handoff between
adapters: Glue derives its database/table identity from the schema, while
Iceberg callers can preserve the same context when converting back to Arrow.
Explicit adapter arguments still take priority.

```python
from rkp import catalog_name, record_metadata, schema_metadata, schema_name, table_name

assert catalog_name(User) == "lakehouse"
assert schema_name(arrow_schema) == "analytics"
assert table_name(arrow_schema) == "user_facts"
assert schema_metadata(User)[b"owner"] == b"identity"
assert record_metadata(User).table_name == "user_facts"
```

Omitted decorator metadata inherits from a record base class. An explicit
`None` clears a catalog or schema name; `table_name=None` resets the table name
to the record alias or lowercased class name. Plain dataclasses receive that
same lowercased table-name fallback. The accessors are free functions rather
than class attributes, so dataclass fields named `schema_name` or `table_name`
remain ordinary record data.

Records are standard dataclasses. `field()` is a real dataclass field with
portable scalar controls: `alias`, `type`, `nullable`, `doc`, `seq`,
`primary_key`, `partition_key`, and `index_key`. Concrete annotations become
non-nullable Arrow fields, optional annotations become nullable, and explicit
field configuration takes priority.

`seq` is the stable cross-protocol field identity. It is not a declaration
position: values must remain unique across the complete nested schema and are
projected to Arrow's `PARQUET:field_id` metadata and Iceberg field IDs.
`field_id` and `iceberg_field_id` remain accepted as compatibility aliases,
but new metadata is normalized and stored only as `rkp.seq`. Missing `seq`
values are assigned deterministically when an Iceberg schema is built.

`metadata` is the only mapping-valued field option. Ordinary top-level entries
are payload metadata and are emitted as Arrow field metadata. Adapters consume
the reserved `rkp` container, so the container itself never appears as a JSON,
YAML, or Arrow payload key. Type-factory arguments live beside the type in that
namespace:

```python
import pyarrow as pa

created = field(
    metadata={
        "source": "event-clock",
        "rkp": {
            "type": pa.timestamp,
            "parameters": {"unit": "ms", "tz": "UTC"},
        },
    }
)
```

The same canonical metadata carries aliases, nullability, documentation,
stable field identities, and primary/partition/index roles through the record
codecs and schema adapters.

Arrow inference handles scalar and typing annotations, UTC timestamps,
collections, maps, nested dataclasses/records, TypedDict, NamedTuple, enums,
and fixed tuples. Fixed tuples become structs with `_1`, `_2`, ... field names.
The root field name is the class alias or the lowercased class name.

Records stream through Arrow without a JSON-shaped normalization step. A
single iterable can become one `RecordBatch`, bounded batches, or a
`RecordBatchReader`; batches, tables, readers, and lazy batch iterables convert
back to records with the same alias-aware and type-safe construction policy:

```python
from rkp import arrow_into_records, records_into_arrow_reader

users = [user]
reader = records_into_arrow_reader(users, record_type=User, batch_size=8192)
restored = arrow_into_records(User, reader)

# Equivalent conveniences installed by @record:
reader = User.into_arrow_reader(users, batch_size=8192)
restored = User.from_arrow(reader)
```

Apache Avro is built in and dependency-free. `rkp.avro` implements schemas,
the binary encoding, the JSON encoding, object container files, canonical form,
and Rabin fingerprints; codecs are compiled once per schema into cached closure
trees. `rkp.records.avro` bridges that implementation to records and Arrow, and
`flavor="iceberg"` emits Iceberg's own Avro representation (fixed-backed
decimals and UUIDs, `field-id` attributes, explicit `adjust-to-utc`):

```python
avro_schema = User.into_avro_schema()
payload = User.into_avro(users, codec="deflate")
restored = list(User.from_avro(payload))
```

Apache Spark 4 interop uses that Arrow boundary directly and does not require
pandas. Arrow field and schema metadata are carried in Spark field metadata,
validated against the live Spark schema, then restored on the reverse path.

```python
# ``spark`` is an existing pyspark.sql.SparkSession.
frame = User.into_spark_dataframe(users, spark=spark, batch_size=8192)
spark_schema = User.into_spark_schema()
restored = User.from_spark(frame)
```

`spark_dataframe_into_arrow()` uses Spark's `DataFrame.toArrow()` and therefore
collects the DataFrame to the driver; `batch_size` bounds record decoding after
that collection. Spark has no metadata container for an empty `StructType`, so
schema metadata cannot survive a zero-column Spark boundary, though RKP does
preserve the logical row count. The `spark` extra constrains PyArrow to the
minimum supported by the Spark 4 Arrow runtime.

`into_arrow_schema()` exposes the inferred record fields as a top-level
`pyarrow.Schema`. Iceberg interop is field-first: `into_iceberg_field()`
returns a real `pyiceberg.types.NestedField`, and `into_iceberg_schema()`
coordinates those same field conversions under one global sequence allocator
before assembling `pyiceberg.schema.Schema(*fields)`. This keeps aliases,
requiredness, documentation, stable IDs, and nested struct/list/map handling in
one adapter.
Whole record/dataclass schemas flatten only their synthetic root struct;
converting a real struct-valued field keeps it as one schema column.
The synthetic root also consumes an allocated identity, so generated child IDs
can differ from converting an already-flattened Arrow schema. Assign explicit
`seq` values wherever schema-evolution identity matters.
Set a positive, stable `seq` for columns whose identity must survive
schema evolution; it is written to Arrow's `PARQUET:field_id`
metadata and used by the Iceberg adapter. Field `doc` values also round-trip
through Arrow's `doc` metadata and Iceberg field documentation.
PyIceberg `Schema` has no portable container for arbitrary schema metadata;
retain the source Arrow metadata and pass it to
`iceberg_into_arrow_schema(..., metadata=source.metadata)` when catalog context
must survive an Iceberg-only boundary.

Nanosecond timestamp conversion adapts to the requested Iceberg format
version. Leaving `downcast_ns_timestamp_to_us` unset (`...`, or passing `None`)
downcasts nanoseconds for format v1/v2, which can only represent microseconds,
and preserves nanoseconds for format v3. Set it to `False` for strict,
lossless behavior (v1/v2 then reject nanosecond fields), or `True` to force
microseconds even with v3. The adaptive v1/v2 behavior is intentionally lossy.
Timezone-aware Arrow timestamps must use UTC (`UTC`, `Etc/UTC`, `Z`, or
`+00:00`); other zones are rejected rather than silently becoming naive.
This policy applies to Arrow-to-Iceberg schema conversion. It does not
configure later table writes: PyIceberg writers receiving nanosecond data for
a microsecond schema still require their
`downcast-ns-timestamp-to-us-on-write` setting. With the currently supported
PyIceberg 0.11.x line, v3 nano schema types are available even though a full v3
table-metadata lifecycle is not yet supported.

Arrow and Iceberg types meet in RKP's own neutral field model rather than in
PyIceberg's converters, which keeps one traversal and accepts every Arrow type
that converts losslessly. Format versions 1, 2, and 3 are supported for schema
conversion, including v3 `unknown` columns, nanosecond timestamps, and
`initial-default`/`write-default` values carried as Arrow metadata.
`iceberg_into_avro_schema()` and `avro_into_iceberg_schema()` expose Iceberg's
Avro representation of a schema.

`rkp.records.iceberg_catalog` drives a live PyIceberg catalog from the same
record contract: `create_iceberg_table()`, `records_into_iceberg_table()`,
`iceberg_table_into_records()`, `iceberg_table_into_arrow()`, and
`sync_iceberg_table_schema()` for union-by-name evolution. Partition specs and
sort orders are projected from field roles, where a string `partition_key`
names an Iceberg transform such as `"day"` or `"bucket[16]"`. PyIceberg 0.11.x
cannot write v3 table metadata, so creating a v3 table raises a clear
`NotImplementedError` while v3 schema conversion keeps working.

`primary_key` maps to Iceberg identifier fields. Iceberg keeps partitioning
and sorting in separate `PartitionSpec` and `SortOrder` objects, so
`partition_key` and `index_key` remain available as Arrow metadata rather
than being folded into the Iceberg schema. For bidirectional schema work, an
attached custom `Field` can be converted directly with
`dataclasses.fields(User)[0].into_iceberg_field(owner=User)`. The public
utilities `into_iceberg_field()`, `arrow_into_iceberg_field()`, and
`iceberg_into_arrow_field()` expose the same conversion unit for ordinary
dataclass and Arrow code. `rkp.records.iceberg` also exposes the corresponding
schema adapters. Compose independently converted fields with
`iceberg_fields_into_schema()` (or `into_iceberg_schema([...])`) so RKP can
validate IDs globally; PyIceberg itself permits duplicate IDs that make field
lookup ambiguous. Iceberg identifier membership belongs to `Schema`, not
`NestedField`, so pass `identifier_field_ids=` when composing fields manually.

`@record(with_json=True, with_yaml=True)` installs JSON/YAML-specific methods
and generic `load`, `loads`, `dump`, and `dumps` methods. Generic file methods
infer `.json`, `.yaml`, or `.yml`; string methods default to JSON when it is
enabled. `Record.from_dict(..., safe=True)` recursively checks/casts annotated
values. `safe=False` forwards values unchanged. With `on_error="default"`,
field defaults or sensible typed zero values are used after conversion errors.

Codec `load`/`dump` methods distinguish paths from in-memory string buffers:
a `str` is a path only when it contains `/` or `\\`; otherwise it is document
text. Use `Path("record.json")`, `"./record.json"`, or `".\\record.json"` for
a separator-free filename. Because strings are immutable, dumping to a string
buffer returns the encoded text, for example `text = user.dump_json("")`.

For byte-oriented applications, records also expose `dumps_bytes`,
`dumps_json_bytes`, and `dumps_yaml_bytes`, plus matching `dump*_bytes`
destination methods. These encode once and write directly to a binary stream
or a binary-mode path. The existing load methods accept bytes-like documents,
`BytesIO` streams, and paths, so no separate byte-loading API is necessary.

The core package includes PyArrow and its own optimized YAML codec. Apache
Iceberg, PySpark, and the AWS SDK remain optional integrations:

```shell
uv add "rkp[iceberg]"
uv add "rkp[spark]"
uv add "rkp[awsglue]"
uv add "rkp[all]"
```

The Iceberg extra installs PyIceberg's Arrow adapter dependencies; the required
PyArrow package itself is already provided by the core installation. Glue
schema conversion, reverse conversion, and DDL generation are part of the core
API and do not import boto3. Install `rkp[awsglue]` only when `GlueCatalog`
should create its own AWS client; an already configured Glue-compatible client
can instead be injected directly.

The Glue adapters map Arrow structs, arrays, maps, field documentation, `seq`,
and record roles into Glue columns and deterministic Athena/Hive DDL. Classic
external table descriptors support Parquet, ORC, Avro, JSON, and CSV.
Partition fields are emitted through `PartitionKeys`, not duplicated in
`StorageDescriptor.Columns`. RKP also embeds the complete Arrow schema for a
lossless reverse conversion. On decoding, that payload is validated and must
agree with the live storage columns and partition keys; live partition order is
preserved. Tables without the RKP payload are reconstructed from their Glue
types and column-order metadata instead.

`into_glue_partition_values()` projects partition fields from a typed record
or mapping in canonical Glue order. `into_glue_partition_projection()` builds
validated Athena `enum`, `integer`, `date`, and `injected` projection
properties; both table-input and DDL helpers accept the same structured
projection mapping and an optional custom S3 location template.

`GlueCatalog` is an injectable facade over database, table, and partition CRUD.
With no client argument it lazily imports boto3 from the `awsglue` extra:

```python
from rkp import GlueCatalog

catalog = GlueCatalog()  # or GlueCatalog(boto3.client("glue"))
catalog.ensure_database("analytics")
catalog.upsert_table("analytics", glue_table)
stored = catalog.get_table("analytics", "users")
```

Database and table methods include explicit `create_*`, `get_*`, `update_*`,
`upsert_*`, `delete_*`, and paginated `list_*` operations where applicable.
Partition methods provide the same lifecycle, plus
`batch_create_partitions()` for 1 to 100 inputs and
`batch_delete_partitions()` for 1 to 25 value lists per AWS request.
`partition_values()` and `create_partition_from()` project typed records using
the live table's partition order:

```python
descriptor = glue_table["StorageDescriptor"]
partition = {
    "Values": ["2026-08-13T12:00:00Z"],
    "StorageDescriptor": {
        **descriptor,
        "Location": "s3://warehouse/users/created_at=2026-08-13/",
    },
}
catalog.upsert_partition("analytics", "users", partition)
partitions = catalog.list_partitions("analytics", "users")
catalog.batch_delete_partitions(
    "analytics", "users", [["2026-08-13T12:00:00Z"]]
)

# A record instance can supply the ordered Values list directly.
catalog.create_partition_from(
    "analytics",
    "users",
    user,
    location="s3://warehouse/users/created_at=2026-08-13/",
)
```

DDL helpers cover table and database creation and teardown:

```python
from rkp import (
    into_glue_database_ddl,
    into_glue_drop_database_ddl,
    into_glue_drop_table_ddl,
)

create_database = into_glue_database_ddl("analytics")
drop_table = into_glue_drop_table_ddl("users", database="analytics")
drop_database = into_glue_drop_database_ddl("analytics", cascade=True)
```

Identifiers and string literals are escaped in generated DDL, and supplied
names are normalized to lowercase. Escaping is a syntax and injection safety
measure; it does not make every Glue-valid identifier valid in Athena. Use
database, table, and column names that also satisfy the naming rules of the
query engine that will execute the statement.

The generated Glue table is a classic Hive-compatible external table. Native
Iceberg table creation also creates and manages Iceberg metadata in S3; use
PyIceberg's Glue catalog for that lifecycle rather than treating a schema-only
conversion as a complete Iceberg table creation.

Development:

```shell
uv sync --project python --extra test --group dev
uv run --project python --extra test --group dev pytest -q
uv build --project python --no-sources
```

Preview or validate the GitHub Pages site from the repository root:

```shell
uv run --project python --only-group docs mkdocs serve --config-file mkdocs.yml
uv run --project python --locked --only-group docs mkdocs build --strict --config-file mkdocs.yml
```

Protocol microbenchmarks live in `benchmarks/`; for example:

```shell
uv run --extra iceberg python benchmarks/records_iceberg.py
```

Use `--json` for machine-readable results. Benchmarks are kept separate from
correctness tests so performance trends never become timing-sensitive CI
assertions. See `benchmarks/README.md` for the Arrow, Avro, Spark, Iceberg, Glue, and
PostgreSQL runners.

The project uses uv's native `uv_build` backend for editable installs, source
distributions, and wheels. Run `uv build --no-sources` before publishing to
verify the package without any local source overrides.

Supported Python versions are 3.11 and 3.12.
