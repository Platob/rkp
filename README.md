# rkp

`rkp` turns typed Python dataclasses into portable records. One definition can
drive JSON, YAML, and Apache Avro serialization, Apache Arrow schemas and
batches, and optional Spark, Iceberg, and AWS Glue integrations.

Avro is implemented in Rust (`rust/crates/rkp-avro`) and exposed through a
Python extension module and a Node addon, so `rkp.avro` and `@rkp/avro` share
one implementation of schemas, encodings, fingerprints, and container files.

```python
from rkp import Record, field, record


@record(schema_name="analytics", table_name="users")
class User(Record):
    identifier: int = field(alias="user_id", seq=1, primary_key=True)
    name: str
    email: str | None = None


user = User.from_dict({"user_id": "7", "name": "Ada"})
assert User.loads_json(user.dumps_json()) == user
assert User.into_arrow_schema().names == ["user_id", "name", "email"]
```

## Documentation

The [documentation](https://platob.github.io/rkp/) progresses from the record
model and built-in codecs through FIX field/structure generation, Arrow
streaming, Avro, Spark, Iceberg (schemas, the Avro representation, and live
catalogs), Glue, and a live PostgreSQL ADBC integration. Runnable examples live in
[`docs/examples`](docs/examples).

Install the core from the package index with `pip install rkp`. Optional
integrations are available as `rkp[spark]`, `rkp[iceberg]`,
`rkp[awsglue]`, or `rkp[all]`.

## Repository layout

- `rust/` is a Cargo workspace: `rkp-avro` implements Avro (schemas, the binary
  and JSON encodings, fingerprints, and random-access container files), and two
  binding crates wrap it for Python and Node. See [`rust/README.md`](rust/README.md).
- `python/` contains the distributable Python project, tests, lockfile, and
  protocol benchmarks. Its wheel is built by maturin and bundles the
  `rkp._avro` extension module, so a local checkout needs a
  [Rust toolchain](https://rustup.rs).
- `js/` is the `@rkp/avro` npm package, the Node addon over the same core
  (pre-alpha, no prebuilt binaries yet). See [`js/README.md`](js/README.md).
- `docs/` contains the MkDocs source and runnable examples.
- `.github/workflows/` validates the project and publishes the documentation.

For a local checkout:

```console
uv sync --project python --extra test
uv run --project python pytest -q
uv run --project python --only-group docs mkdocs serve -f mkdocs.yml
cargo test --manifest-path rust/Cargo.toml
cd js && npm install && npm run build && npm test
```

See [the development guide](docs/benchmarks-development.md) for the complete
quality, packaging, benchmark, and documentation commands.

Licensed under the [Apache License 2.0](LICENSE).
