"""Convert one record contract through Iceberg, Avro, Arrow, and a catalog."""

from __future__ import annotations

import tempfile
from datetime import UTC, datetime

from rkp import (
    Record,
    avro_into_iceberg_schema,
    create_iceberg_table,
    field,
    iceberg_into_arrow_schema,
    iceberg_into_avro_schema,
    iceberg_table_into_records,
    record,
    records_into_arrow_batch,
    records_into_iceberg_table,
)


@record(table_name="observations")
class Observation(Record):
    identifier: int = field(seq=1, primary_key=True)
    observed_at: datetime
    labels: dict[str, str | None]


def main() -> None:
    try:
        iceberg_schema = Observation.into_iceberg_schema(format_version=2)
    except ImportError as exc:
        raise SystemExit("Install the Iceberg extra: uv sync --extra iceberg") from exc

    canonical = Observation.into_arrow_schema()
    protocol_schema = iceberg_into_arrow_schema(
        iceberg_schema,
        metadata=canonical.metadata,
    )
    values = (
        Observation(
            1,
            datetime(2026, 8, 14, 10, 0, tzinfo=UTC),
            {"region": "eu-west", "optional": None},
        ),
    )
    batch = records_into_arrow_batch(values, schema=protocol_schema)
    # Iceberg can choose large physical Arrow containers, so validate the
    # record names/coercions while accepting that protocol projection.
    assert tuple(Observation.from_arrow(batch, validate_schema=False)) == values

    # Iceberg describes its schemas in Avro; RKP emits that representation and
    # reads it back without losing field identities.
    avro_schema = iceberg_into_avro_schema(iceberg_schema, name="observations")
    assert avro_schema.field("identifier").attributes["field-id"] == 1
    restored = avro_into_iceberg_schema(avro_schema)
    assert restored.as_struct() == iceberg_schema.as_struct()

    print(iceberg_schema)
    _run_catalog(values)


def _run_catalog(values: tuple[Observation, ...]) -> None:
    """Create a disposable catalog table, write records, and read them back."""

    try:
        from pyiceberg.catalog.sql import SqlCatalog
    except ImportError:
        print("Install SQLAlchemy to run the catalog section of this example")
        return

    with tempfile.TemporaryDirectory() as warehouse:
        catalog = SqlCatalog(
            "example",
            uri="sqlite:///:memory:",
            warehouse=f"file://{warehouse}",
        )
        table = create_iceberg_table(
            catalog,
            Observation,
            identifier=("analytics", "observations"),
            format_version=2,
        )
        records_into_iceberg_table(table, values, record_type=Observation)
        stored = tuple(iceberg_table_into_records(Observation, table))
        assert stored == values
        print(f"catalog rows: {len(stored)}, partition spec: {table.spec()}")


if __name__ == "__main__":
    main()
