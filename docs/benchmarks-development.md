# Benchmarks and development

## Development environment

From the repository root:

```console
uv sync --project python --extra test
uv run --project python pytest -q
```

Focused integration tests:

```console
uv run --project python pytest python/tests/integration/test_arrow_batches.py -q
uv run --project python --extra iceberg pytest python/tests/integration/test_iceberg_arrow_runtime.py -q
uv run --project python --extra spark pytest python/tests/integration/test_spark_arrow.py -q
```

Spark tests require Java. The PostgreSQL test is opt-in through
`RKP_TEST_POSTGRES_URI`; see [Integrations](integrations.md).

Build distributable artifacts using the configured uv backend:

```console
uv build --project python --no-sources
```

## Benchmarks

Benchmarks report trends and do not impose timing assertions on correctness
tests. Run each protocol from the same machine/runtime when comparing a change:

```console
uv run --project python python python/benchmarks/records_arrow.py
uv run --project python --extra iceberg python python/benchmarks/records_iceberg.py
uv run --project python --extra spark python python/benchmarks/records_spark.py
uv run --project python python python/benchmarks/records_awsglue.py
```

The PostgreSQL runner requires a disposable database and ADBC driver:

```console
RKP_TEST_POSTGRES_URI=postgresql://localhost/rkp_test \
  uv run --project python --with adbc-driver-postgresql \
  python python/benchmarks/records_postgres.py
```

Every runner accepts `--json` for machine-readable results. For a quick local
probe, reduce calibration while keeping multiple repetitions:

```console
uv run --project python python python/benchmarks/records_arrow.py \
  --min-time 0.05 --repeat 3
```

The suites cover:

- Arrow one-shot/bounded encoding, reader construction, validated and relaxed
  decoding, and raw PyArrow lower bounds;
- Spark schema conversion, direct Arrow ingestion, DataFrame collection, and
  record reconstruction, excluding session startup;
- Iceberg cold/cached schema work, global ID allocation, timestamp policy,
  reverse Arrow conversion, and protocol-projected batches;
- Glue schema/column/table/DDL conversion without AWS requests;
- PostgreSQL ADBC reader ingestion and result decoding, including network and
  transaction cost but excluding connection startup.

Absolute values depend on the host and dependency versions. Compare ratios and
changes across commits after the correctness suite is green. More details are
kept beside the runners in `python/benchmarks/README.md`.

## Documentation checks

The root MkDocs configuration treats `docs/` as its source. Preview and build
the same site CI publishes:

```console
uv run --project python --only-group docs mkdocs serve -f mkdocs.yml
uv run --project python --locked --only-group docs mkdocs build --strict -f mkdocs.yml
```

Run the local examples before publishing:

```console
uv run --project python python docs/examples/basic.py
uv run --project python python docs/examples/codecs.py
uv run --project python python docs/examples/arrow_batches.py
uv run --project python python docs/examples/glue.py
```

Optional/live examples are guarded as described on their integration pages.

## Publishing to GitHub Pages

The repository workflow at `.github/workflows/docs.yml` validates the strict
build on pull requests. A push to `main` that changes documentation inputs (or
a manual `workflow_dispatch`) uploads `site/` as a Pages artifact and deploys
it to <https://platob.github.io/rkp/>.

One repository setting cannot be committed: in **Settings → Pages**, select
**GitHub Actions** as the publishing source. After that one-time setting, the
workflow owns deployment; do not commit the generated `site/` directory or
maintain a separate `gh-pages` branch by hand.

The build and deploy jobs use the minimum GitHub permissions they need. All
third-party actions are pinned to immutable commit SHAs, and production
deployments are serialized through the `github-pages` environment.

## Review and release checklist

Before publishing a release:

```console
uv lock --check --directory python
uv run --project python --extra test --group dev pytest -q
uv run --project python --group dev ruff check python/src python/tests python/benchmarks docs/examples
uv run --project python --group dev mypy python/src/rkp python/tests/typing/record_methods.py --ignore-missing-imports
uv build --project python --no-sources --clear
uv run --project python --group dev twine check python/dist/*
uv run --project python --extra test --group dev pip-audit
uv run --project python --locked --only-group docs mkdocs build --strict -f mkdocs.yml
```

Run the test suite under both supported Python versions. Live PostgreSQL is
deliberately opt-in, so a skipped ADBC test is expected unless
`RKP_TEST_POSTGRES_URI` names a disposable database. Inspect the wheel metadata
and install the wheel without extras to verify that PyArrow is present while
PyIceberg, boto3, and PySpark remain optional.

Known protocol boundaries should remain explicit in review: outgoing Arrow
union rows are rejected until callers construct union arrays themselves;
naive datetimes become UTC-aware through the default Arrow timestamp; Spark
DataFrame conversion materializes data on the driver; and zero-column Spark
schemas cannot retain Arrow schema metadata.

## Review checklist

Before merging a protocol or schema change, review it at each shared boundary:

1. Add the Python annotation/field case to Arrow schema inference.
2. Exercise one-shot, bounded-batch, reader, empty-input, alias, nested, and
   metadata behavior where applicable.
3. Verify safe decode errors include the complete record field path.
4. Test the protocol's physical Arrow projection both with strict validation
   and, when intentionally different, with a documented relaxed boundary.
5. Keep optional imports behind their public lazy facade and check the error
   names the required extra.
6. Run Python 3.11 and 3.12 tests, static checks, the strict documentation
   build, and wheel/sdist smoke installation.
7. Benchmark hot-path changes without turning elapsed time into a test
   assertion.

The main architectural constraint is deliberate: codecs and protocol adapters
share record aliases, nullability, identity, roles, and metadata instead of
maintaining competing representations. Arrow is the canonical tabular
handoff. New integrations should consume that contract rather than add another
row-normalization layer.

Document any relaxation explicitly. Current notable limits are Arrow union
runtime output requiring a caller-built `UnionArray`, Spark reverse conversion
materializing on the driver, zero-column Spark schemas losing schema metadata,
Iceberg v1/v2 nanosecond downcasting by default, and live PostgreSQL/AWS tests
requiring caller-provided infrastructure or emulation.
