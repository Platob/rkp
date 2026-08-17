"""Move one record contract through Avro schemas, data, and Arrow."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from rkp import (
    Record,
    avro,
    avro_into_arrow_schema,
    field,
    into_avro_schema,
    record,
)


@record(table_name="observations")
class Observation(Record):
    identifier: int = field(seq=1, primary_key=True)
    observed_at: datetime
    amount: Decimal
    labels: dict[str, str | None]


def main() -> None:
    schema = Observation.into_avro_schema()
    assert into_avro_schema(Observation) is schema
    assert schema.field("identifier").attributes["field-id"] == 1

    # The Arrow contract survives the round trip in both directions.
    arrow_schema = Observation.into_arrow_schema()
    assert avro_into_arrow_schema(schema).names == arrow_schema.names

    values = (
        Observation(
            1,
            datetime(2026, 8, 14, 10, 0, tzinfo=UTC),
            Decimal("12.34"),
            {"region": "eu-west", "optional": None},
        ),
    )
    payload = Observation.into_avro(values, codec="deflate")
    assert tuple(Observation.from_avro(payload)) == values

    # Iceberg's own Avro representation uses fixed decimals and adjust-to-utc.
    iceberg_flavored = Observation.into_avro_schema(flavor="iceberg")
    amount = iceberg_flavored.field("amount").type
    assert amount.logical_type == "decimal"

    print(avro.dumps_schema(schema, indent=2))
    print(f"{len(payload)} bytes, fingerprint {avro.fingerprint(schema):016x}")


if __name__ == "__main__":
    main()
