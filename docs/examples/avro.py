"""Move one record contract through Avro schemas, a container file, and Arrow."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

from rkp import (
    Record,
    avro,
    avro_into_arrow_schema,
    field,
    into_avro_schema,
    record,
)
from rkp.avro import Avro


@record(table_name="observations")
class Observation(Record):
    identifier: int = field(seq=1, primary_key=True)
    observed_at: datetime
    amount: Decimal
    labels: dict[str, str | None]


def observation(identifier: int) -> Observation:
    """Return one observation, distinguishable by its identifier."""

    return Observation(
        identifier,
        datetime(2026, 8, 14, 10, 0, tzinfo=UTC),
        Decimal("12.34"),
        {"region": "eu-west", "optional": None},
    )


def main() -> None:
    schema = Observation.into_avro_schema()
    assert into_avro_schema(Observation) is schema
    assert schema.field("identifier").attributes["field-id"] == 1

    # The Arrow contract survives the round trip in both directions.
    arrow_schema = Observation.into_arrow_schema()
    assert avro_into_arrow_schema(schema).names == arrow_schema.names

    # Iceberg's own Avro representation uses fixed decimals and adjust-to-utc.
    iceberg_flavored = Observation.into_avro_schema(flavor="iceberg")
    amount = iceberg_flavored.field("amount").type
    assert amount.logical_type == "decimal"

    values = [observation(identifier) for identifier in range(64)]

    with TemporaryDirectory() as folder:
        path = Path(folder) / "observations.avro"

        # Build: one container file, framed into several compressed blocks.
        with Avro.create(schema, path, codec="deflate", sync_interval=512) as new:
            new.extend(values)

        # Random read: reaching a record decodes its block, not the file.
        container = Avro(path)
        assert len(container) == len(values)
        assert container[40]["identifier"] == 40
        assert container[-1]["identifier"] == 63
        assert [row["identifier"] for row in container.iter_from(8, 11)] == [8, 9, 10]
        block = container.block_of(40)
        assert len(container.blocks()) > 1
        assert block.first <= 40 < block.stop
        assert container.read_block(block.ordinal)[40 - block.first] == container[40]

        # Random write: edits stage per block and are applied in one pass.
        with Avro(path, mode="r+") as editable:
            editable[40] = {**editable[40], "labels": {"region": "us-east"}}
            del editable[0]
            editable.insert(0, observation(900))
            editable.append(observation(901))
            removed = editable.pop()
            assert removed["identifier"] == 901
            editable.compact()

        # Reopen: the edits are durable, and the record contract is unchanged.
        reopened = Avro(path)
        assert len(reopened) == len(values)
        assert reopened[0]["identifier"] == 900
        assert reopened[40]["labels"] == {"region": "us-east"}
        assert len(reopened.blocks()) == 1

        restored = list(Observation.from_avro(path))
        assert restored[0] == observation(900)
        assert restored[40].labels == {"region": "us-east"}
        assert restored[1:40] == values[1:40]
        assert restored[41:] == values[41:]

        size = path.stat().st_size

    print(avro.dumps_schema(schema, indent=2))
    print(
        f"{len(restored)} records, {size} bytes, "
        f"codec {reopened.codec}, fingerprint {avro.fingerprint(schema):016x}"
    )


if __name__ == "__main__":
    main()
