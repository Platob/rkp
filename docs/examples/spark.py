"""Use Spark 4 through the Arrow record boundary (opt-in local process)."""

from __future__ import annotations

import os

from rkp import Record, record


@record
class Metric(Record):
    identifier: int
    value: float | None


def main() -> None:
    if os.environ.get("RKP_RUN_SPARK_EXAMPLE") != "1":
        print("Set RKP_RUN_SPARK_EXAMPLE=1 to start a local Spark session.")
        return

    try:
        from pyspark.sql import SparkSession
    except ModuleNotFoundError as exc:
        raise SystemExit("Install the Spark extra: uv sync --extra spark") from exc

    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("rkp-docs")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    try:
        values = (Metric(1, 3.5), Metric(2, None))
        dataframe = Metric.into_spark_dataframe(values, spark=spark, batch_size=1)
        restored = tuple(Metric.from_spark(dataframe.orderBy("identifier")))
        assert restored == values
        assert dataframe.schema == Metric.into_spark_schema()
        dataframe.show()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
