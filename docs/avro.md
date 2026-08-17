# Apache Avro

Avro in RKP is a Rust core with thin host bindings. The crate `rkp-avro`
(`rust/crates/rkp-avro`) owns the format — schema parsing, canonical form,
Rabin fingerprints, the binary and JSON encodings, and object container files
addressable by record index. Nothing about the format is implemented twice.

Two bindings wrap that one core:

- `rkp._avro` is a [PyO3](https://pyo3.rs) extension module built by
  [maturin](https://www.maturin.rs) from `rust/crates/rkp-avro-python`. It ships
  inside the `rkp` wheel, so `import rkp.avro` needs no optional dependency —
  but building the project from source needs a Rust toolchain.
- `@rkp/avro` is a [napi-rs](https://napi.rs) addon in `js/`, for Node 20 and
  later. It exposes the same surface — `parseSchema`, `Schema`, and the `Avro`
  container — over the same core, so a file written by one host is read by the
  other. It is pre-alpha and ships no prebuilt binaries yet; see
  [`js/README.md`](https://github.com/Platob/rkp/tree/main/js).

`python/tests/avro/vectors.json` pins one canonical form, one fingerprint, and
one binary encoding per schema shape, and both test suites assert it, so the
two hosts cannot drift apart silently.

The Python package `rkp.avro` owns the Python surface and no format logic: the
immutable schema model, the `Avro` container class with its file provenance,
and the module-level codec facade. Value conversion belongs to the binding, not
the core — `datetime`, `date`, `time`, `Decimal`, and `UUID` are mapped to and
from Avro logical types in Rust, so each host language keeps the objects its
users actually hold.

```python
import rkp.avro as avro

print(avro.core_version())  # the version of the Rust core behind this package
```

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

Every schema object — `RecordSchema`, `EnumSchema`, `FixedSchema`,
`ArraySchema`, `MapSchema`, `UnionSchema`, `PrimitiveSchema` — is a view over
one node of a parsed core schema. Constructing one assembles a JSON declaration
and hands it to the core, so parsing, validation, canonical form, and
fingerprints have exactly one implementation.

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

## One container, read and written by index

`rkp.avro.Avro` is a single class for object container files. It reads and
writes the same container and addresses records by position, so there is no
reader/writer pair to keep in step.

```python
from pathlib import Path

from rkp.avro import Avro

rows = [{"identifier": index, "label": f"row-{index}"} for index in range(20)]
schema = {
    "type": "record",
    "name": "reading",
    "fields": [
        {"name": "identifier", "type": "long"},
        {"name": "label", "type": ["null", "string"], "default": None},
    ],
}

with Avro.create(schema, Path("readings.avro"), codec="deflate") as new:
    new.extend(rows)
```

Reading is random. Reaching record *k* walks block headers — which are framed
with an explicit record count and byte size — and decodes exactly the one block
that holds it, so an indexed read never scans the file:

```python
container = Avro(Path("readings.avro"))

assert len(container) == 20
assert container[7] == rows[7]
assert container[-1] == rows[-1]
assert container[2:5] == rows[2:5]
assert container.get(999) is None
assert list(container.iter_from(3, 5)) == rows[3:5]
```

Writing is random too, under `mode="r+"`. Edits buffer per block and are
applied in one pass, so scattered writes cost one rewrite rather than one each,
and blocks before the first edit are copied byte for byte:

```python
with Avro(Path("readings.avro"), mode="r+") as container:
    container[7] = {"identifier": 700, "label": "edited"}
    del container[3]
    container.insert(0, {"identifier": -1, "label": "first"})
    removed = container.pop()               # pop returns the record it removes
    container.append({"identifier": 99, "label": "tail"})
    container[0:2] = [{"identifier": 0, "label": "merged"}]
    container.truncate(16)
    container.compact()
```

Slices must be contiguous: a step other than `1` raises `ValueError`. Mutating
a container while iterating it raises `RuntimeError` rather than yielding a
stale row. Appends are readable and editable before they are framed.

### Modes and persistence

| Mode   | Read | Replace, insert, delete | Append | Opens                  |
| ------ | ---- | ----------------------- | ------ | ---------------------- |
| `"r"`  | yes  | no                      | no     | an existing container  |
| `"a"`  | yes  | no                      | yes    | an existing container  |
| `"r+"` | yes  | yes                     | yes    | an existing container  |
| `"w"`  | yes  | yes                     | yes    | a new, empty container |

`Avro.create(schema, destination, ...)` is `mode="w"` with the creation
options: `codec`, `metadata`, `sync_marker`, and `sync_interval`. Those
describe a *new* container, so passing them when opening an existing one is a
`ValueError` — an existing file declares its own codec, schema, and marker in
its header.

A container's target can be a path, a seekable binary stream, or nothing at
all:

```python
buffered = Avro.create(schema)          # no destination: an in-memory image
buffered.extend(rows)
image = buffered.close()                # close() returns the bytes
assert isinstance(image, bytes)

edited = Avro(image, mode="r+")         # bytes in, bytes out
edited[0] = {"identifier": 0, "label": "in memory"}
same_image = edited.into_bytes()        # materialize without writing
edited.save(Path("copy.avro"))          # or write the image elsewhere
```

`flush()` materializes pending changes and writes them to the target;
`close()` flushes and releases, returning the image only for a memory-backed
container. Write-out is not blindly a rewrite: when nothing already durable
moved — the common append case — only the new bytes are appended to the file or
stream. Otherwise the file is replaced atomically through a temporary file, so
a torn image is never left behind.

Following the shared codec rule, a `str` source is document bytes rather than a
path unless it contains `/` or `\`; use `Path("readings.avro")` for a
separator-free file name.

### Blocks

Blocks are visible, because they are the unit of both compression and random
access:

```python
container = Avro(Path("readings.avro"))

for block in container.blocks():
    print(block.ordinal, block.first, block.count, block.size)

block = container.block_of(7)
assert block.first <= 7 < block.stop
assert container.read_block(block.ordinal)[7 - block.first] == container[7]
```

`AvroBlock` is a `NamedTuple` of `ordinal`, `offset`, `data_offset`, `size`,
`count`, and `first`, plus the derived `stop` and `end`. Locating one costs no
decompression.

`sync_interval` is the staged-bytes threshold that closes a block: large blocks
compress better, small blocks make indexed reads cheaper.
`rkp.avro.DEFAULT_SYNC_INTERVAL` is 64 KiB and `RANDOM_SYNC_INTERVAL` is 8 KiB
for read-mostly, index-heavy files. `compact()` re-frames every block at the
current interval, which is how a container that has absorbed many small edits
gets its layout back. Decoded block payloads are cached under the `cache_bytes`
budget (32 MiB by default); `container.nbytes` reports the resident size of the
image, index, and cache.

Container codecs are `null`, `deflate`, `bzip2`, and `xz`, all compiled into
the core rather than resolved at runtime:

```python
from rkp.avro import CODECS

assert CODECS == ("null", "deflate", "bzip2", "xz")
```

For whole-file work, `write_container()`, `read_container()`, `dump()`, and
`load()` wrap the same class:

```python
from rkp.avro import load, read_container, write_container

destination = Path("observations.avro")
write_container(destination, schema, [{"identifier": 1, "label": None}])
rows_back = list(read_container(destination))
assert load(destination) == rows_back
```

`read_container(source, *, schema=None, mode="r", cache_bytes=...)` returns an
`Avro`, so `mode="r+"` there is the same random-write container.

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

`into_avro()` writes an object container file and returns its bytes. Values
keep their Python types on the way in, so timestamps, dates, times, decimals,
and UUIDs are encoded through Avro logical types rather than through text, and
come back as `datetime`, `date`, `time`, `Decimal`, and `UUID` objects.

The free functions `records_into_avro()` and `avro_into_records()` accept the
same arguments. `rkp.records.avro.avro_into_records()` additionally takes
`start` and `stop`, which select a record range without decoding the blocks
before it — a partial read of a large container is cheap for the same reason
`container[7]` is.

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
assert framed[:2] == b"\xc3\x01"
assert avro.decode_single_object(parsed, framed) == {"x": 7}
```

`decode_single_object()` validates the framed fingerprint against the reader
schema and refuses a mismatch instead of decoding garbage.

Each of those functions accepts a schema in any accepted form — parsed model,
JSON text, or decoded structure — and normalizes it per call.
`avro.compile_encoder(schema)` and `avro.compile_decoder(schema)` bind the
schema once and return a callable, which is what a hot loop wants:

```python
encode_point = avro.compile_encoder(parsed)
decode_point = avro.compile_decoder(parsed)
assert decode_point(encode_point({"x": 7})) == {"x": 7}
```

Mappings, dataclass instances, and positional sequences all encode against a
record schema, which keeps `tuple[...]` annotations (Arrow structs) directly
encodable. Mappings and dataclasses are matched by Avro field name and
sequences by position; a missing field falls back to its declared default, and
a union branch is chosen by trying the branches in declaration order, with
`null` matched first for `None`. Record types whose fields declare an `alias`
should go through `into_avro()` or `records_into_avro()`, which project the
serialized names the schema was built from.

The JSON encoding follows the specification rather than a naive dump: unions
are tagged by branch name and `bytes`/`fixed` values use Latin-1 text.

```python
assert avro.dumps(parsed, {"x": 7}) == '{"x": 7}'
assert avro.loads(parsed, '{"x": 7}') == {"x": 7}
```

`into_json()` and `out_of_json()` expose the same projection as plain Python
data, without the text step.

## Iceberg

`iceberg_into_avro_schema()` and `avro_into_iceberg_schema()` connect the two
protocols; see [Iceberg](iceberg.md#avro-representation).

Run the local example:

```console
uv run --project python python docs/examples/avro.py
```

Source: [`examples/avro.py`](examples/avro.py).
