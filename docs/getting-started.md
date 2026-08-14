# Getting started

## Requirements

`rkp` supports Python 3.11 and 3.12. PyArrow is part of the core installation.
Spark, Iceberg, and boto3-backed Glue catalog access are optional.

From a package index:

```console
python -m pip install rkp
```

From this repository, with [uv](https://docs.astral.sh/uv/):

```console
uv sync --project python
```

Install only the integrations an application uses:

```console
uv sync --project python --extra iceberg
uv sync --project python --extra spark
uv sync --project python --extra awsglue
uv sync --project python --extra all
```

## Define a record

Subclass `Record` and apply `@record`. The decorator exposes standard
dataclass behavior plus serialization and schema methods.

```python
from rkp import Record, field, record, to_dict


@record(alias="users", schema_name="analytics", table_name="user_facts")
class User(Record):
    identifier: int = field(alias="userId", seq=1, primary_key=True)
    name: str
    email: str | None = None


user = User.from_dict({"userId": "7", "name": "Ada"})
assert user.identifier == 7
assert to_dict(user) == {"userId": 7, "name": "Ada", "email": None}
```

Safe construction recursively follows annotations and accepts field aliases.
Use `safe=False` only when the input already has exactly the constructor's
Python values and field names. With `on_error="default"`, conversion failures
use declared defaults, optional `None`, or an annotation-appropriate fallback.

## Run the example

[`examples/basic.py`](examples/basic.py) adds catalog metadata and verifies the
wire representation:

```console
uv run --project python python docs/examples/basic.py
```

Continue with [records and fields](records.md), then choose a serialization or
interop guide. To generate record fields from a locally cached FIX dictionary,
see [FIX dictionaries and structures](fix.md).
