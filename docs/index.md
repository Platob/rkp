# rkp

`rkp` turns typed Python dataclasses into portable records. One record contract
can drive JSON and YAML codecs, Apache Arrow schemas and streams, and optional
Spark, Iceberg, and AWS Glue integrations.

```python
from rkp import Record, field, record


@record(table_name="users")
class User(Record):
    identifier: int = field(alias="user_id", seq=1, primary_key=True)
    name: str
    email: str | None = None


user = User.from_dict({"user_id": "7", "name": "Ada"})
assert user == User(identifier=7, name="Ada")
assert User.loads_json(user.dumps_json()) == user
assert User.into_arrow_schema().names == ["user_id", "name", "email"]
```

Arrow is the common protocol boundary. Records can become one batch, bounded
batches, or a streaming `RecordBatchReader`; Spark, Iceberg, Glue, and ADBC
reuse the same schema and row representation.

## Start here

- [Getting started](getting-started.md) installs the core and runs the first
  example.
- [Records, fields, and metadata](records.md) defines the shared data contract.
- [FIX dictionaries](fix.md) generate cached fields, components, repeating groups, and messages.
- [JSON and YAML](codecs.md) covers strings, paths, streams, and generated
  record methods.
- [Arrow](arrow.md) moves from schema inference to lazy batch readers.
- [Spark](spark.md), [Iceberg](iceberg.md), and [AWS Glue](aws-glue.md) build on
  that Arrow contract.
- [PostgreSQL ADBC](integrations.md) demonstrates a live Arrow protocol
  integration.
- [Public API](reference/index.md) is a compact signature index.

The complete runnable examples are in [`docs/examples`](examples/basic.py).
