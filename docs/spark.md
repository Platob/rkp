# Apache Spark

The optional Spark adapter targets PySpark 4 and moves rows through Arrow
without pandas.

```console
uv sync --project python --extra spark
```

A Java runtime supported by the installed Spark version is also required.

## Schemas

```python
from rkp import (
    Record,
    into_spark_schema,
    record,
    spark_into_arrow_schema,
)


@record(table_name="metrics")
class Metric(Record):
    identifier: int
    value: float | None


spark_schema = into_spark_schema(Metric)
assert spark_schema == Metric.into_spark_schema()

arrow_schema = spark_into_arrow_schema(spark_schema)
assert arrow_schema.equals(Metric.into_arrow_schema(), check_metadata=True)
```

`into_spark_schema()` accepts a record/dataclass, Arrow schema, or existing
Spark `StructType`. RKP stores the complete Arrow field contract in Spark field
metadata and verifies it on conversion back.

The granular adapters are useful at custom boundaries:

```python
from rkp import (
    arrow_into_spark_field,
    arrow_type_into_spark_type,
    spark_into_arrow_field,
    spark_type_into_arrow_type,
)

arrow_field = Metric.into_arrow_schema().field("value")
spark_field = arrow_into_spark_field(arrow_field)
assert spark_into_arrow_field(spark_field).equals(arrow_field, check_metadata=True)
assert spark_type_into_arrow_type(
    arrow_type_into_spark_type(arrow_field.type)
) == arrow_field.type
```

Use `prefer_timestamp_ntz=` for Arrow-to-Spark timestamp selection. Reverse
conversion accepts `timezone=` and `prefers_large_types=` and adapts to the
installed Spark 4 conversion signature.

## DataFrames

```python
from rkp import (
    arrow_into_spark_dataframe,
    records_into_spark_dataframe,
    spark_dataframe_into_arrow,
    spark_dataframe_into_records,
)

values = (Metric(1, 3.5), Metric(2, None))

frame = records_into_spark_dataframe(
    values,
    record_type=Metric,
    spark=spark,
    batch_size=1,
)
assert tuple(
    spark_dataframe_into_records(frame.orderBy("identifier"), Metric)
) == values

table = spark_dataframe_into_arrow(frame)
same_frame = arrow_into_spark_dataframe(table, spark=spark)

# Equivalent record conveniences.
frame = Metric.into_spark_dataframe(values, spark=spark)
restored = Metric.from_spark(frame.orderBy("identifier"))
```

Arrow input may be a batch, table, reader, or non-empty iterable of batches.
Pass `record_type=` for empty record input. If `spark=` is omitted, RKP uses the
active session or `SparkSession.builder.getOrCreate()`.

Spark's `DataFrame.toArrow()` collects the full DataFrame to the driver.
Consequently, reverse-path `batch_size` bounds record decoding after
collection; it is not a distributed-memory limit. Use it only when the result
is safe to collect.

Zero-column records retain their row count, but Spark `StructType` has no
schema-level metadata container, so metadata cannot survive that special
empty-schema boundary. Unsupported Arrow/Spark types fail with their field
path.

The runnable example starts no JVM unless explicitly enabled:

```console
RKP_RUN_SPARK_EXAMPLE=1 \
  uv run --project python --extra spark python docs/examples/spark.py
```

PowerShell:

```powershell
$env:RKP_RUN_SPARK_EXAMPLE = "1"
uv run --project python --extra spark python docs/examples/spark.py
```

Source: [`examples/spark.py`](examples/spark.py).
