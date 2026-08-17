# JavaScript

A Node-API view of the same values the Rust core holds, with conventional JavaScript casing and protocols.

```javascript
const { DataType, Field, Url } = require('@yggdryl/node')
const assert = require('node:assert/strict')

const schema = new Field(
  'row',
  DataType.fromFields([new Field('id', 'int64', false)]),
  false,
)

assert.equal(schema.dataType.kind, 'struct')
assert.equal(String(Url.fromPath('C:/market data/trades.arrows')),
  'file:///C:/market%20data/trades.arrows')
```

This page documents the JavaScript boundary only: what the package adds on top of the core, and how
it converts what you hand it. The behaviour itself is documented once, on the
[core pages](../index.md).

## Build from the repository

```console
npm install --prefix node
npm run --prefix node build:debug
npm test --prefix node
```

`npm test` runs `node --test tests/*.test.js` and then `tsc --noEmit`, so the shipped `.d.ts`
declarations are checked against the tests that use them.

## What it exposes

| Name | Documented in |
| --- | --- |
| `DataType` | [datatype](../core/datatype.md) |
| `Field`, `fields` | [field](../core/field.md) |
| `Uri`, `Url`, `Urn` | [uri](../core/uri.md) |
| `IOBase` | [io](../core/io.md) |
| `BatchReader`, `RecordOptions` | [io](../core/io.md), [ipc](../core/ipc.md), [parquet](../core/parquet.md) |
| `iceberg` | [iceberg](../core/iceberg.md) |
| `MimeType`, `MediaType`, `Timezone` | [enums](../core/enums.md) |
| `codec`, `json`, `toml`, `yaml`, `Value` | [text](../core/text.md) and the format pages |

The compression codings are Rust-only today; a handle applies the one its name declares without
being told, so [gzip](../core/gzip.md), [zlib](../core/zlib.md), and [zstd](../core/zstd.md) are
reachable through `IOBase` even though their modules are not.

## Inference at the boundary

Every constructor accepts the obvious JavaScript spelling of its argument and converts once, in
Rust. Prefer the generic `from` entry points: they dispatch on what they were handed.

```javascript
const { DataType, Field, MediaType, MimeType, Url } = require('@yggdryl/node')
const assert = require('node:assert/strict')

// A datatype expression is a datatype.
assert.equal(String(new Field('id', 'int64', false).dataType), 'int64')
assert.equal(DataType.from('list<int32>').kind, 'list')

// A media type is its canonical name.
assert.equal(String(MimeType.from('application/json')), 'application/json')
assert.equal(String(MediaType.from('application/json')), 'application/json')

// A path is a location.
assert.equal(String(Url.fromPath('C:/tmp/a.json')), 'file:///C:/tmp/a.json')
```

There is no JavaScript-side parser: `DataType.from` and its siblings call the matching native
constructor.

## Values cross as their natural shape

A JavaScript value becomes the nearest native value, and comes back as the nearest JavaScript value
to that. No class name travels beside the data, so a shape the core does not have arrives as the
shape it was lowered to.

```javascript
const { json } = require('@yggdryl/node')
const assert = require('node:assert/strict')

const decoded = json.loads(json.dumps({
  venues: new Set(['XPAR', 'XNAS']),
  book: new Map([[1, 'bid']]),
  source: new URL('https://example.com/feed'),
  match: /a\/b/giu,
  raw: Buffer.from([0, 255]),
  id: 2n ** 100n,
}))

assert.deepEqual(decoded.venues, ['XPAR', 'XNAS'])        // a Set is a list
assert.ok(decoded.book instanceof Map)                    // a non-text key keeps the Map
assert.equal(decoded.source, 'https://example.com/feed')  // a URL is its href
assert.equal(decoded.match, '/a\\/b/giu')                 // a RegExp is its literal
assert.deepEqual(decoded.raw, Buffer.from([0, 255]))
assert.equal(decoded.id, 2n ** 100n)
```

| You write | It is stored as | It reads back as |
| --- | --- | --- |
| `undefined`, `null` | null | `null` |
| `boolean`, `string` | boolean, string | the same |
| `number` | 64-bit integer or float | `number` |
| `bigint` | exact 64- or 128-bit integer | `number` inside the safe range, `bigint` outside |
| `Buffer`, `Uint8Array`, `Uint8ClampedArray`, `ArrayBuffer` | bytes | `Buffer` |
| every other typed array | sequence | `Array` |
| `Array`, `Set` | sequence | `Array` |
| `Map` | mapping | `Map` when some key is not text, plain object when every key is |
| plain object, class instance | mapping | plain object |
| `Date` | timestamp, milliseconds, no zone | `Date` |
| `URL` | its `href` string | `string` |
| `RegExp` | its literal string, flags included | `string` |
| `DataType`, `Field`, `Uri`, `Url`, `Urn` | its canonical string | `string` |
| `Value` | itself | `Date` when one holds it exactly, otherwise `Value` |

These are the losses, and they are deliberate: a `Set` comes back a list, a `URL` and a `RegExp`
come back strings, a class instance comes back a plain object, a `Map` of text keys comes back a
plain object, and `undefined` comes back `null`. Reconstructing any of them takes one line of your
own code and needs no cooperation from the codec:

```javascript
const { json } = require('@yggdryl/node')
const assert = require('node:assert/strict')

class Order {
  constructor(id) {
    this.id = id
  }
}

const decoded = json.loads(json.dumps({ order: new Order(7), venues: new Set(['XPAR']) }))
assert.deepEqual(decoded, { order: { id: 7 }, venues: ['XPAR'] })

const order = Object.assign(new Order(0), decoded.order)
assert.ok(order instanceof Order)
assert.deepEqual(new Set(decoded.venues), new Set(['XPAR']))
```

Nothing in a document can make this binding name a class, look one up, or run a constructor,
because there is no name in the document to look up. A `bigint` wider than 128 bits has no exact
native integer and is refused rather than rounded, and reading back a `Date`, a `Buffer`, or a
`Map` never depends on a method the caller can replace: the encoder reads those off the intrinsic
prototypes.

## Temporal and exact-decimal values

`Value` is the JavaScript spelling of what JavaScript has no type for: an exact decimal, a date, a
time of day, a duration, and a timestamp at a resolution or in a zone a `Date` cannot hold. A
`Date` is exactly a naive count of whole milliseconds, so anything that is one crosses as a `Date`
in both directions.

```javascript
const { Value, json } = require('@yggdryl/node')
const assert = require('node:assert/strict')

const at = new Date('2026-08-15T12:30:00.000Z')
assert.ok(Value.fromJs(at).equals(Value.timestamp(1786797000000n, 'ms')))
assert.ok(json.loads(json.dumps(at)) instanceof Date)

// A microsecond instant in a named zone is not a Date, so it stays exact.
const micros = Value.timestamp(1700000000123456n, 'us', 'UTC')
const decoded = json.loads(json.dumps({ at: micros })).at
assert.equal(decoded.kind, 'timestamp')
assert.equal(decoded.count, 1700000000123456n)
assert.equal(decoded.unit, 'us')
assert.equal(decoded.zone, 'UTC')
assert.ok(decoded.equals(micros))
```

`Value.timestamp(count, unit, zone?)`, `Value.date(days)`, `Value.time(count, unit)`,
`Value.duration(count, unit)`, and `Value.decimal(unscaled, scale)` build one; `kind`, `count`,
`unit`, `zone`, `unscaled`, and `scale` read one back. A date counts days since the epoch and has
no unit; a decimal is `unscaled` times ten to the minus `scale`, which is the only representation
that round-trips, because `0.1` has no finite binary expansion.

```javascript
const { Value } = require('@yggdryl/node')
const assert = require('node:assert/strict')

const price = Value.decimal(-1050n, 2) // -10.50
assert.equal(price.unscaled, -1050n)
assert.equal(price.scale, 2)

// Equality is what a value names, not how it was written.
assert.ok(price.equals(Value.decimal(-105n, 1)))
assert.ok(Value.duration(1n, 's').equals(Value.duration(1000n, 'ms')))
assert.equal(Value.date(19723).unit, null)
```

## fromJs and asJs

`Value.fromJs` and `Value.prototype.asJs` are the conversion pair. Every `load` and `dump` crosses
them - `dumps` is `fromJs` with bytes on the far side, `loads` is `asJs` - so calling them directly
is how you see what a value becomes before any format is involved.

```javascript
const { Value, json } = require('@yggdryl/node')
const assert = require('node:assert/strict')

assert.equal(Value.fromJs(new Set([1, 2])).kind, 'sequence')
assert.deepEqual(Value.fromJs(new Set([1, 2])).asJs(), [1, 2])
assert.equal(Value.fromJs(new Map([['id', 1]])).kind, 'mapping')

const value = { id: 1, at: new Date(0), tags: new Set(['a']) }
assert.deepEqual(json.loads(json.dumps(value)), Value.fromJs(value).asJs())
```

Both accept the same `{ maxDepth }` the codec functions do, in the inclusive range 1 to 48.

## Field metadata is a Map

`Field` implements the `Map` protocol over its metadata, so ordinary idioms work and the ordering is
the native one.

```javascript
const { Field } = require('@yggdryl/node')
const assert = require('node:assert/strict')

const field = new Field('trade', 'int64', false, { source: 'book' })
field.set('venue', 'XPAR')

assert.equal(field.get('source'), 'book')
assert.ok(field.has('venue'))
assert.equal(field.size, 2)
assert.deepEqual([...field.keys()].sort(), ['source', 'venue'])

field.delete('venue')
assert.ok(!field.has('venue'))
```

Typed identifiers and protocol properties (`dictionaryId`, `contentType`, `etag`, and the rest) are
accessors rather than map keys, because they are validated.

## Arrow

Apache Arrow JS values cross the boundary as copied IPC. The package is explicit about that: this is
not a zero-copy bridge.

```javascript
const { DataType } = require('@yggdryl/node')
const assert = require('node:assert/strict')

const scalar = DataType.from('int64').defaultArrowScalar()
assert.equal(String(scalar), '0')
```

`defaultJSValue`, `defaultJSHint`, and `defaultArrowScalar` are schema-directed projections of the
native default planner; the JavaScript layer caches identity but never decides what a default is.

## Records cross as one batch per stream

`BatchReader` is the one record shape: a read returns one and a write consumes one, exactly as
[`IOBase`](../core/io.md) does in Rust. `BatchReader.from` accepts whatever a caller already holds -
another reader, an Apache Arrow JS `Table` or `RecordBatch`, an array of batches, or Arrow IPC bytes -
and iterating one yields Arrow JS record batches.

```javascript
const assert = require('node:assert/strict')
const arrow = require('apache-arrow')
const { BatchReader, IOBase, MimeType } = require('@yggdryl/node')

const table = new arrow.Table({
  id: arrow.vectorFromArray([1n, 2n], new arrow.Int64()),
})

// An in-memory handle says what it holds; a named one reads it off its name.
const handle = IOBase.fromBytes()
handle.mediaType = MimeType.ARROW_STREAM
handle.writeArrowBatchReader(BatchReader.from(table))

const reader = handle.readArrowBatchReader()
assert.equal(reader.field.name, 'row')
assert.equal([...reader].reduce((rows, batch) => rows + batch.numRows, 0), 2)

// A stream is read once, and says so rather than reading as empty.
assert.ok(reader.consumed)
```

Each batch crosses as its own self-contained Arrow IPC stream, so its schema travels with it and
Arrow JS needs no separate handshake. That per-batch header is what a copied boundary costs, and it
is stated here rather than hidden. `toIpc` drains the reader into one stream and `toTable` into one
Arrow JS table, for the cases where a caller does want everything at once.

The encoding is never named by a call: `recordOptions()` derives it from the handle's media type, and
`RecordOptions` carries the shared settings plus the Parquet-only ones, which read as `null` on an
encoding that has none.

```javascript
const assert = require('node:assert/strict')
const { RecordOptions } = require('@yggdryl/node')

const parquet = RecordOptions.from('trades.parquet')
assert.equal(String(parquet.mimeType), 'application/vnd.apache.parquet')
assert.equal(parquet.compression, 'zstd(1)')
assert.equal(parquet.withCompression('snappy').compression, 'snappy')

// A setting one encoding has is absent on the others rather than invented.
const stream = RecordOptions.from('trades.arrows')
assert.equal(stream.compression, null)
assert.equal(stream.maxRowGroupSize, null)
```

## Anything in, a reader out

`readArrow`, `writeArrow`, and `appendArrow` are the same three calls with the argument widened to
whatever your last library handed you. Each one becomes the single native reader and is passed to the
same native method, so widening the argument never adds a second way to write.

```javascript
const assert = require('node:assert/strict')
const arrow = require('apache-arrow')
const { IOBase, MimeType } = require('@yggdryl/node')

const table = new arrow.Table({
  id: arrow.vectorFromArray([1n, 2n], new arrow.Int64()),
})

function handle() {
  const stream = IOBase.fromBytes()
  stream.mediaType = MimeType.ARROW_STREAM
  return stream
}

// A table, a reader, named columns, and plain records all write.
for (const rows of [
  table,
  arrow.RecordBatchReader.from(arrow.tableToIPC(table)),
  { id: [1n, 2n] },
  [{ id: 1n }, { id: 2n }],
]) {
  const target = handle()
  target.writeArrow(rows)
  assert.equal(target.readArrow().toTable().numRows, 2)
}
```

| You are holding | What happens |
| --- | --- |
| a native `BatchReader` or Arrow IPC bytes | used as it stands |
| an Arrow JS `Table`, `RecordBatch`, or `RecordBatchReader` | its batches, encoded as one stream |
| an Arrow JS `Vector` | the one column a declared schema names, and refused when none does |
| an object of named columns | `tableFromArrays` |
| an object of scalar values | one row |
| an array or iterable of any of those | concatenated into one stream |
| an array of plain records | `tableFromJSON`, inferred from all of them at once |

Arrow JS has no C Data consumer, so this boundary encodes one Arrow IPC stream in both directions -
the batches were going to be materialized either way, which is why an array and a generator cost the
same here and why the Python side can stream where this one cannot.

An **async** source is the one shape that changes the call's shape: its rows do not exist until they
are awaited, so `writeArrow` returns a promise for it and nothing for every synchronous source. An
Arrow JS reader implements both iteration protocols and is treated as the synchronous one.

```javascript
const assert = require('node:assert/strict')
const arrow = require('apache-arrow')
const { IOBase, MimeType } = require('@yggdryl/node')

async function main() {
  const table = new arrow.Table({ id: arrow.vectorFromArray([1n], new arrow.Int64()) })
  async function* pages() {
    yield table
    yield table
  }

  const handle = IOBase.fromBytes()
  handle.mediaType = MimeType.ARROW_STREAM
  await handle.writeArrow(pages())

  assert.equal(handle.readArrow().toTable().numRows, 2)
}

main()
```

`apache-arrow` is loaded only when a value actually has to be materialized, and a build without it
reports that package by name rather than failing somewhere inside a conversion.

## Iceberg is a namespace

The table format sits on top of the record encodings in the core, so it is one name here rather than
a handful of top-level classes.

```javascript
const assert = require('node:assert/strict')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const arrow = require('apache-arrow')
const { Field, fields, iceberg } = require('@yggdryl/node')

const schema = iceberg.assignFieldIds(
  fields.struct('row', [Field.from('id: int64'), Field.from('venue: utf8')], {
    nullable: false,
  }),
)

const root = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'yggdryl-docs-')), 'trades')
const table = iceberg.Table.create(root, schema, ['venue'])
table.append(
  new arrow.Table({
    id: arrow.vectorFromArray([1n, 2n], new arrow.Int64()),
    venue: arrow.vectorFromArray(['XNAS', 'XNYS'], new arrow.Utf8()),
  }),
)

assert.equal(table.currentSnapshot.operation, 'append')
assert.equal(table.dataFiles().length, 2)
assert.equal(table.scan().toTable().numRows, 2)

fs.rmSync(path.dirname(root), { recursive: true, force: true })
```

`iceberg.Table`, `iceberg.PartitionSpec`, and `iceberg.DataFile` are the classes;
`iceberg.assignFieldIds`, `iceberg.schemaFromJson`, and `iceberg.schemaToJson` are the functions. A
snapshot and a manifest arrive as plain objects, because they are records of what happened rather
than values with behaviour, and a 64-bit identifier crosses as a `bigint` so a snapshot id past 2^53
is exact.

## Errors

A native error crosses unchanged and arrives as a `TypeError` or `RangeError` carrying the message
the Rust error produced, including its path or byte offset.

```javascript
const { DataType } = require('@yggdryl/node')
const assert = require('node:assert/strict')

assert.throws(() => DataType.from('decimal(0,0)'), /precision/)
```

<!-- notebooks: generated by scripts/build_docs_notebooks.py -->

## Notebooks

Every example on this page, as a notebook generated from these blocks and
shipped unexecuted:
[JavaScript](../notebooks/extensions_javascript-javascript.ipynb){ download }.

<!-- /notebooks -->
