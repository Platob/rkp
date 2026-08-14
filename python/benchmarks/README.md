# RKP conversion benchmarks

The benchmark runners use only the standard library plus their declared RKP
runtime dependencies. They deliberately live outside the test suite:
benchmarks report trends and optimization headroom, while tests enforce
correctness.

Run the Iceberg suite from the `python` directory:

```console
uv run --extra iceberg python benchmarks/records_iceberg.py
```

Run the Arrow batch runtime suite (PyArrow is a required dependency):

```console
uv run python benchmarks/records_arrow.py
```

Run the optimized local Spark 4 suite. The runner enables direct Arrow
execution, uses one local worker, and excludes Spark session startup:

```console
uv run --extra spark python benchmarks/records_spark.py
```

The PostgreSQL runner uses the ADBC Arrow protocol and deliberately has no
implicit database default. Point it at a disposable database; it creates a
uniquely named temporary table and removes it in a `finally` block:

```console
RKP_TEST_POSTGRES_URI=postgresql://localhost/rkp_test \
  uv run --with adbc-driver-postgresql python benchmarks/records_postgres.py
```

Run the pure AWS Glue schema adapter suite without boto3, Moto, credentials,
or network access:

```console
uv run python benchmarks/records_awsglue.py
```

Use `--json` for machine-readable output suitable for retaining as a CI
artifact. For a quicker local probe, lower the calibration target:

```console
uv run --extra iceberg python benchmarks/records_iceberg.py --min-time 0.05 --repeat 3
uv run python benchmarks/records_arrow.py --min-time 0.05 --repeat 3
uv run --extra spark python benchmarks/records_spark.py --min-time 0.05 --repeat 3
uv run python benchmarks/records_awsglue.py --min-time 0.05 --repeat 3
```

The Arrow runtime cases compare one-shot and bounded streaming batch creation,
streaming reader materialization, validated and relaxed record reconstruction,
table iteration, and PyArrow's raw `to_pylist()` cost as a lower-bound
reference.

The Spark cases compare schema conversion, direct Arrow table ingestion,
record ingestion, driver-side Arrow collection, and record reconstruction.
Session creation and reusable DataFrame materialization happen before
measurement. Spark's `toArrow()` collects to the driver; reverse `batch_size`
bounds record decoding after collection rather than Spark transport. The
PostgreSQL cases compare local record batching, ADBC replace ingestion, Arrow
query results, and complete query-to-record reconstruction; connection startup
is excluded, while transactions and network transfer remain part of the
protocol operations being measured.

The Iceberg cases cover:

- cold and cached record-to-Iceberg field conversion;
- cold and cached record-to-Iceberg schema conversion;
- coordinated ID allocation for a wide, recursively nested Arrow schema;
- conversion of the same tree when IDs are already explicit;
- adaptive nanosecond handling for Iceberg v2 and v3, plus forced v3 downcast;
- full-schema and single-field Iceberg-to-Arrow conversion;
- canonical and Iceberg-projected Arrow batch runtime round trips;
- direct bulk PyIceberg calls as lower-bound reference points.

"Cold" record cases clear only RKP's Iceberg conversion caches between calls.
The record's cached Arrow inference remains warm, which isolates the Iceberg
adapter instead of timing class creation or annotation resolution. The private
cache access is confined to this benchmark and is not an example of supported
application API usage.

The AWS Glue cases use one representative Arrow schema containing structs,
lists, maps, timestamps, field metadata, and a partition key. They cover:

- cached record-to-Arrow dispatch and portable catalog metadata access;
- Arrow-to-Glue column conversion;
- complete classic external-table input generation;
- deterministic Athena/Hive DDL generation;
- typed partition-value rendering and validated Athena partition projection;
- lossless embedded Arrow schema decoding and live Glue projection validation;
- fallback Glue type parsing without embedded RKP schema metadata.

The table fixtures are assembled before measurement for both reverse cases.
The suite measures only the pure schema adapter: it deliberately excludes Glue
client creation, AWS requests, and Moto emulation.

Absolute timings depend on the host, Python, PyArrow, and (for the Iceberg
suite) PyIceberg versions. Compare results on the same machine and focus on
ratios and changes across commits. Run correctness tests before interpreting a
faster result.
