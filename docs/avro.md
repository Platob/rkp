# Apache Avro

`rkp.avro` is a complete, dependency-free Avro implementation: schemas, the
binary encoding, the JSON encoding, and object container files. It needs no
extra install and never imports PyArrow.

`rkp.records.avro` bridges that implementation to records, ordinary
dataclasses, Arrow, and Iceberg through the shared field model in
`rkp.records.datatypes`, so one record contract keeps the same names,
nullability, identities, and precision in every protocol.

## Schemas from records

```python
from datetime import datetime
from decimal import Decimal

from rkp import Record, field, into_avro_schema, record


@record(table_name="observations")
class Observation(Record):
    identifier: int = field(seq=1, primary_key=True)
    observed_at: datetime
    amount: Decimal
    labels: dict[str, str | None]


schema = Observation.into_avro_schema()
assert schema.name == "observations"
assert schema.field("identifier").attributes["field-id"] == 1
assert schema.field("observed_at").type.logical_type == "timestamp-micros"
assert into_avro_schema(Observation) is schema
```

Optional fields become `["null", T]` unions with a `null` default, `seq`
becomes the `field-id` attribute, `doc` becomes the Avro field doc, and
`primary_key` travels as a `primary-key` attribute. Schemas are immutable,
hashable, and cached per record type.

Every conversion direction is available as a free function:

```python
from rkp import (
    arrow_into_avro_field,
    arrow_into_avro_schema,
    avro_into_arrow_field,
    avro_into_arrow_schema,
    dataclass_into_avro_schema,
)

arrow_schema = Observation.into_arrow_schema()
assert arrow_into_avro_schema(arrow_schema) == schema
assert avro_into_arrow_schema(schema).names == arrow_schema.names
```

An Avro declaration written by another engine converts without any RKP
metadata: pass the JSON text, the decoded structure, or a parsed schema.

## Flavors

`flavor="iceberg"` emits exactly the Avro representation Iceberg uses:
fixed-backed decimals sized to their precision, fixed(16) UUIDs, and an
explicit `adjust-to-utc` attribute on timestamps.

```python
iceberg_flavored = Observation.into_avro_schema(flavor="iceberg")
amount = iceberg_flavored.field("amount").type
assert amount.logical_type == "decimal"
assert amount.size == 16
```

Avro map keys are always strings, so a `dict[int, str]` field uses Iceberg's
array-of-pairs representation and converts back to an Arrow map unchanged.

## Records to Avro data

```python
records = [
    Observation(1, datetime.now().astimezone(), Decimal("1.25"), {"a": None}),
]
payload = Observation.into_avro(records, codec="deflate")
assert list(Observation.from_avro(payload)) == records
```

`into_avro()` writes an object container file. Values keep their Python types
on the way in, so timestamps, dates, decimals, and UUIDs are encoded through
Avro logical types rather than through text. Container codecs are `null`,
`deflate`, `bzip2`, and `xz` — all from the standard library.

The free functions `records_into_avro()` and `avro_into_records()` accept the
same arguments, and `records_into_avro(destination=...)` is available through
`rkp.avro.write_container` for paths and streams:

```python
from rkp.avro import read_container, write_container

write_container("observations.avro", schema, [{"identifier": 1}])
rows = list(read_container("observations.avro"))
```

## The protocol layer

`rkp.avro` is usable on its own for schema and value work:

```python
import rkp.avro as avro

parsed = avro.parse_schema({"type": "record", "name": "point",
                            "fields": [{"name": "x", "type": "long"}]})
encoded = avro.encode(parsed, {"x": 7})
assert avro.decode(parsed, encoded) == {"x": 7}

# Canonical form and the CRC-64-AVRO (Rabin) fingerprint of the specification.
assert avro.canonical_form(parsed) == '{"name":"point","type":"record",' \
    '"fields":[{"name":"x","type":"long"}]}'
fingerprint = avro.fingerprint(parsed)

# Single-object framing carries that fingerprint with the payload.
framed = avro.encode_single_object(parsed, {"x": 7})
assert avro.decode_single_object(parsed, framed) == {"x": 7}
```

Encoders and decoders are compiled once per schema into closure trees and
cached on the schema object, so encoding a stream never re-inspects the
declaration. `avro.compile_encoder(schema)` and `avro.compile_decoder(schema)`
expose those closures when a hot loop should skip even the dispatch above.

Records, mappings, and positional sequences all encode against a record schema,
which keeps `tuple[...]` annotations (Arrow structs) directly encodable.

The JSON encoding follows the specification rather than a naive dump: unions
are tagged by branch name and `bytes`/`fixed` values use Latin-1 text.

```python
assert avro.dumps(parsed, {"x": 7}) == '{"x": 7}'
assert avro.loads(parsed, '{"x": 7}') == {"x": 7}
```

## Iceberg

`iceberg_into_avro_schema()` and `avro_into_iceberg_schema()` connect the two
protocols; see [Iceberg](iceberg.md#avro-representation).

Run the local example:

```console
uv run --project python python docs/examples/avro.py
```

Source: [`examples/avro.py`](examples/avro.py).
