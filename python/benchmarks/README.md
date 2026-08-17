# RKP conversion benchmarks

The benchmark runners use only the standard library plus their declared RKP
runtime dependencies. They deliberately live outside the test suite:
benchmarks report trends and optimization headroom, while tests enforce
correctness.

Run the Iceberg suite from the `python` directory:

```console
uv run --extra iceberg python benchmarks/records_iceberg.py
```

Run the Avro suite. It needs no optional dependency:

```console
uv run python benchmarks/records_avro.py
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
uv run python benchmarks/records_avro.py --min-time 0.05 --repeat 3
uv run python benchmarks/records_arrow.py --min-time 0.05 --repeat 3
uv run --extra spark python benchmarks/records_spark.py --min-time 0.05 --repeat 3
uv run python benchmarks/records_awsglue.py --min-time 0.05 --repeat 3
```

The Iceberg suite covers cold and cached record conversion, global field-ID
allocation, format versions 1, 2, and 3, the Iceberg-flavored Avro
representation and its fingerprint, reverse Arrow conversion, protocol-projected
batches, and a live SQL catalog. The catalog cases (namespace and table
creation, table loading, appends, and scans back to Arrow and to records) run
only when PyIceberg's SQL catalog dependencies are installed; pass
`--no-catalog` to skip them, and note that repeated appends accumulate
snapshots by design. PyIceberg 0.11.x cannot write v3 table metadata, so
catalog table creation is measured at format versions 1 and 2 while schema
conversion is measured at all three.

The Avro suite measures the Rust core, the `rkp-avro` crate, through the
`rkp._avro` extension module. Every case crosses the Python boundary into that
crate, so a timing covers both the format work in Rust and the conversion of
Python values on the way in and out — there is no pure-Python Avro path left to
compare against. The suite covers schema parsing, cold and cached record schema
construction, canonical form, Rabin fingerprints, compiled binary encoding and
decoding, the Avro JSON encoding, container files with the `null` and `deflate`
codecs, and the record round trip. RKP's JSON codec runs on the same rows as a
text-encoding reference point. The header line reports the core's version from
`rkp.avro.core_version()` next to the Python and RKP versions.

Five cases measure what the random-access container exists for. They share one
fixture written at `RANDOM_SYNC_INTERVAL` — 8 KiB blocks rather than the 64 KiB
bulk-write default — because indexed reads are what small blocks buy:

- `container_open_index` opens the image and builds the block index behind
  `len(container)`;
- `container_read_one_cold` opens the image and decodes one record by index,
  paying the index build and one block decode;
- `container_read_one_warm` decodes one record from a container whose blocks
  are already in the core's payload cache;
- `container_write_one` reopens the image as `r+`, replaces one record by
  index, and materializes the image with `into_bytes()`;
- `container_append_one` reopens the image, appends one record, and
  materializes the image.

The write and append cases reopen the fixture on every iteration so each one
starts from the same container state; subtract `container_open_index` to read
them as the cost of the edit alone. Cold and warm reads walk the same
deterministic index order, so they compare directly. A cold read costs one whole
block decode rather than one record decode: warming a block scans every record
in it to learn where each record starts, and only then decodes the one that was
asked for.

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

## The Rust core

Correctness for the Avro format is enforced in Rust, not here. The crate keeps
its own suite in the workspace at `../rust`; run it before trusting any Avro
number:

```console
cd ../rust && cargo test              # the whole workspace
cd ../rust && cargo test -p rkp-avro  # the core crate and its doctests
```

The crate has no `cargo bench` target. When the core itself needs profiling
rather than the binding, add a bench under `../rust/crates/rkp-avro/benches`
and run it under `--release`; timing Avro decoding in the `dev` profile
measures the profile rather than the code.

That caveat applies to this suite too. The Avro cases are only meaningful when
the loaded extension module was compiled with optimizations. A wheel built by
the `maturin` PEP 517 backend (`uv sync`, `uv build`, `pip install .`) is a
release build; `maturin develop` without `--release` leaves a `dev`-profile
core in place, which measures several times slower — on this fixture roughly
3x on binary encode and decode, 3x to 4x on the indexed container reads and the
single-record edits, and 1.5x to 2x on bulk container writes, where Python-side
row conversion dominates. Cases that never enter the core, such as
`rkp_json_encode_rows`, do not move at all, so a mixed table is easy to
misread. Check which core is loaded before reporting numbers:

```console
uv run python -c "import rkp._avro; print(rkp._avro.__file__)"
ls -l ../rust/target/debug/lib_avro.so ../rust/target/release/lib_avro.so
```

The extension is a copy of one of those two objects, so matching sizes name the
profile. Rebuild in release with `maturin develop --release` from this
directory's parent, which reads the manifest path from `pyproject.toml`.

Absolute timings depend on the host, Python, PyArrow, and (for the Iceberg
suite) PyIceberg versions, and — for the Avro suite — on the Rust profile and
toolchain the core was built with. Compare results on the same machine and
focus on ratios and changes across commits. Run correctness tests, in both
Python and Rust, before interpreting a faster result.
