# Runtime integration tests

The Arrow and Iceberg runtime tests are self-contained:

```console
uv run --extra test pytest tests/integration/test_arrow_batches.py \
  tests/integration/test_iceberg_arrow_runtime.py
```

Spark tests require the `spark` extra and a Java runtime. They use one local
worker, disable the UI, force UTC, and enable Spark's direct Arrow execution:

```console
uv run --extra test --extra spark pytest tests/integration/test_spark_arrow.py
```

The PostgreSQL test is opt-in. Install the ADBC driver and set
`RKP_TEST_POSTGRES_URI` to a disposable database. The test creates a uniquely
named temporary table, streams record batches in and out, and drops the table in a
`finally` block:

```console
RKP_TEST_POSTGRES_URI=postgresql://localhost/rkp_test \
  uv run --extra test --with adbc-driver-postgresql \
  pytest tests/integration/test_postgres_adbc.py
```

An absent PostgreSQL URI, ADBC driver, PySpark installation, or Java runtime is
reported as an explicit skip rather than making the core test suite depend on
local infrastructure.
