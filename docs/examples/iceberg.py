"""Convert one record contract through Iceberg and back to Arrow."""

from __future__ import annotations

from datetime import UTC, datetime

from rkp import (
    Record,
    field,
    iceberg_into_arrow_schema,
    record,
    records_into_arrow_batch,
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
    print(iceberg_schema)


if __name__ == "__main__":
    main()
