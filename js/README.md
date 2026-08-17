# `@rkp/avro`

Node.js bindings for the [rkp](https://github.com/Platob/rkp) Avro core.

Apache Avro schemas, binary data, and random-access container files. The logic
lives in the Rust crate `rkp-avro`; this package is a thin
[napi-rs](https://napi.rs) wrapper around it, so JS, Python, and Rust all share
one implementation.

> **Status: pre-alpha (0.0.1).** The API below is real and tested, but nothing
> about it is frozen yet, and there are no published prebuilt binaries — you
> need to [build from source](#build-from-source).

## Install

```sh
npm install @rkp/avro
```

Requires **Node.js >= 20**.

## Quick start

```js
const { parseSchema, Avro } = require('@rkp/avro')

const schema = parseSchema({
  type: 'record',
  name: 'Event',
  fields: [
    { name: 'identifier', type: 'long' },
    { name: 'label', type: ['null', 'string'], default: null },
    { name: 'at', type: { type: 'long', logicalType: 'timestamp-millis' } },
  ],
})

const bytes = schema.encode({ identifier: 1, label: 'ada', at: new Date() })
schema.decode(bytes) // => { identifier: 1, label: 'ada', at: 2024-03-05T… }

const file = Avro.create(schema, { codec: 'deflate' })
file.append({ identifier: 1, label: 'ada', at: new Date() })
const image = file.image() // a Buffer you can write anywhere

const reopened = Avro.open(image)
reopened.length() // => 1
reopened.get(0) // decodes exactly one block, not the whole file
```

## Value mapping

The core deals in Avro's physical types and never sees a `Date` or a `Buffer`;
this package decides what each schema node looks like in JavaScript. The two
directions are inverses: whatever `decode` gives you, `encode` turns back into
the same bytes.

| Avro                     | JavaScript                                             |
| ------------------------ | ------------------------------------------------------ |
| `null`                   | `null` — `undefined` is also accepted when encoding     |
| `boolean`                | `boolean`, strictly; no truthiness coercion             |
| `int`, `float`, `double` | `number`                                                |
| `long`                   | `number` while `Number.isSafeInteger`, otherwise `bigint` |
| `bytes`, `fixed`         | `Buffer` — any `Uint8Array` or `ArrayBuffer` is accepted |
| `string`                 | `string`                                                |
| `record`                 | plain object keyed by field name; an array of values is accepted positionally |
| `map`                    | plain object                                            |
| `array`                  | `Array`                                                 |
| `enum`                   | the symbol `string`                                     |
| `union`                  | the bare branch value, `null` for the null branch       |

### The long rule

An Avro `long` is 64 bits and a JavaScript `number` is not. So: **a `long`
decodes to a `number` when the value is exactly representable
(`Number.isSafeInteger`), and to a `bigint` when it is not.** Encoding accepts
both, and refuses a `number` that is too large to be an exact integer rather
than silently rounding it:

```js
const long = parseSchema('"long"')
long.decode(long.encode(7)) // => 7          (number)
long.decode(long.encode(2n ** 61n)) // => 2305843009213693952n  (bigint)
long.encode(2 ** 60) // throws AvroEncodeError: beyond Number.MAX_SAFE_INTEGER
```

An `int` is always a `number`; it cannot overflow one.

### Logical types

| Logical type                          | JavaScript                       |
| ------------------------------------- | -------------------------------- |
| `date`                                | `Date` at UTC midnight           |
| `timestamp-millis`                    | `Date`                           |
| `timestamp-micros`, `timestamp-nanos` | `bigint` of raw micros/nanos     |
| `local-timestamp-millis`              | `Date`, read through its UTC fields |
| `local-timestamp-micros`/`-nanos`     | `bigint` of raw micros/nanos     |
| `time-millis`, `time-micros`          | `number` of millis/micros after midnight |
| `decimal`                             | decimal `string`, e.g. `'-1234.567'` |
| `uuid`                                | `string`, hyphenated             |
| `duration`                            | `[months, days, milliseconds]`   |

Three of those need a word of explanation:

- **Sub-millisecond instants stay integers.** A `Date` holds milliseconds, so
  `timestamp-micros` and `timestamp-nanos` decode to a `bigint` of their raw
  units rather than a `Date` that quietly drops what the file actually carries.
- **Local timestamps use a `Date`'s UTC fields.** A local timestamp is a wall
  clock with no zone, so it points at no instant at all. `local-timestamp-millis`
  therefore round-trips through `Date.UTC(...)`: the calendar fields you read
  with `getUTCHours()` are the wall clock the file recorded. Build them with
  `new Date(Date.UTC(y, m, d, …))`, never `new Date(y, m, d, …)`.
- **Times of day stay counts.** JavaScript has no time-of-day type, and a
  `Date` would have to invent a date to go with it, so `time-millis` and
  `time-micros` are plain numbers of units since midnight.

Every logical type also accepts its raw `number`/`bigint` on the way in, and
every timestamp accepts a `Date`, so nothing forces you through the sugar.

## API

### `parseSchema(declaration): Schema`

Parse a JSON string or a plain object — `parseSchema(require('./user.avsc'))`
and `parseSchema(readFileSync('user.avsc', 'utf8'))` agree. A union is an
array of branches. Also available as `Schema.parse`.

### `class Schema`

| Method                        | Returns                                                |
| ----------------------------- | ------------------------------------------------------ |
| `toJSON()`                    | the declaration as a plain object — so `JSON.stringify(schema)` works |
| `json()`                      | the declaration as JSON text                            |
| `canonicalForm()`             | Avro [Parsing Canonical Form](https://avro.apache.org/docs/current/specification/#parsing-canonical-form-for-schemas) |
| `fingerprint()`               | the CRC-64-AVRO (Rabin) fingerprint, as an unsigned **`bigint`** |
| `fingerprintHex()`            | the same value as 16 lowercase hex digits               |
| `fingerprintBytes()`          | the same value as the 8 little-endian bytes Avro frames with |
| `encode(value)`               | `Buffer` of Avro binary                                 |
| `decode(buffer)`              | the value                                               |
| `encodeSingleObject(value)`   | `Buffer`, single-object framed with the fingerprint     |
| `decodeSingleObject(buffer)`  | the value, after checking the fingerprint matches       |
| `toAvroJson(value)`           | Avro's JSON encoding, as plain data                     |
| `fromAvroJson(value)`         | the value, from Avro's JSON encoding                    |
| `equals(other)`               | whether two schemas share a canonical form              |
| `fullname`, `typeName`        | the root's name and structural kind                     |

`fingerprint()` returns a **`bigint`**, unsigned, so the specification's vector
for `"null"` reads `0x63dd24e7cc258f8an`.

Avro's JSON encoding is the interchange form, not the natural JavaScript one:
unions become single-entry objects (`{ string: 'ada' }`), bytes become Latin-1
strings, and logical types are their raw counts. Note that `toAvroJson` of a
`long` beyond `Number.MAX_SAFE_INTEGER` is a `bigint`, which `JSON.stringify`
refuses without a replacer — that is the price of not rounding.

### `class Avro`

A container file held as bytes. The core owns no files: you hand `open` an
image and take one back from `image()`, and where those bytes live is yours to
decide.

```js
Avro.create(schema, { codec, metadata, syncMarker, syncInterval })
Avro.open(buffer, { syncInterval, cacheBytes })
```

`create` takes a `Schema` or any declaration `parseSchema` accepts. `codec` is
one of `codecs()` — `null`, `deflate`, `bzip2`, `xz` — and defaults to `null`.
`metadata` is a string-to-string object; `avro.schema` and `avro.codec` belong
to the container and are ignored here. `syncMarker` is 16 bytes, random by
default. `syncInterval` is the staged-byte threshold that closes a block:
smaller blocks make indexed reads cheaper and files slightly larger.

| Method                          | What it does                                          |
| ------------------------------- | ----------------------------------------------------- |
| `length()`                      | how many records, staged appends included              |
| `get(index)`                    | one record, decoding exactly one block                 |
| `set(index, value)`             | replace one record                                     |
| `append(value)`                 | add one record to the end                              |
| `extend(values)`                | add many                                               |
| `splice(start, stop, values)`   | replace the records in `[start, stop)`                 |
| `range(start, stop)`            | decode a half-open range                               |
| `toArray()`                     | decode everything, in order                            |
| `blocks()`                      | every block's framing, located without decompressing   |
| `blockOf(index)`                | the block holding one record                           |
| `readBlock(ordinal)`            | every record of one block                              |
| `compact()`                     | re-frame the whole file at the current sync interval   |
| `image()`                       | the materialized file, as a `Buffer`                   |
| `schema()`, `metadata()`        | the writer schema, the header metadata                 |
| `codec`, `syncMarker`, `syncInterval`, `dirty`, `nbytes` | properties         |

Reads are by index, not by scan: reaching record *k* walks block headers and
decompresses the one block that holds it. Writes buffer per block, so a
scattered `set` costs one rewrite at `image()` rather than one each — and
blocks before the first edit are copied byte for byte.

`length()` is a method rather than a property because counting has to settle
the staged block first, which is more work than a `.length` should hide.

### Errors

Every failure is an `AvroError` or one of its three subclasses, all exported
from the package:

```js
const { AvroError, AvroSchemaError, AvroEncodeError, AvroDecodeError } = require('@rkp/avro')
```

| Class             | Raised when                                                   |
| ----------------- | ------------------------------------------------------------- |
| `AvroSchemaError` | a schema is malformed, unknown, or internally inconsistent     |
| `AvroEncodeError` | a value cannot be encoded against its declared schema          |
| `AvroDecodeError` | encoded data is truncated or inconsistent with its schema      |
| `AvroError`       | the base class, and the class of container failures            |

They are real JavaScript classes, so `instanceof` and subclassing both behave.
The addon decides which one every failure is; `index.js` declares them and
hands them to the addon at require time.

### Module helpers

- `rabin(payload)` — the CRC-64-AVRO fingerprint of arbitrary bytes, as a `bigint`.
- `codecs()` — every block codec the build supports.
- `constants()` — `syncSize`, `defaultSyncInterval`, `randomSyncInterval`,
  `defaultCacheBytes`, `magic`.
- `coreVersion()` — the `rkp-avro` crate version behind the addon.

## Build from source

You need a [Rust toolchain](https://rustup.rs) (edition 2024, Rust >= 1.85) in
addition to Node >= 20.

```sh
git clone https://github.com/Platob/rkp.git
cd rkp/js
npm install
npm run build
```

That compiles `rust/crates/rkp-avro-node` in release mode and drops the addon
next to `index.js` as `rkp-avro.<triple>.node` — for example
`rkp-avro.linux-x64-gnu.node`.

### Scripts

| Script                | What it does                                                       |
| --------------------- | ------------------------------------------------------------------ |
| `npm run build`       | Release build of the addon (optimized).                             |
| `npm run build:debug` | Debug build — faster to compile, much larger, unoptimized.          |
| `npm test`            | Runs the suite with Node's built-in runner (`node --test`).         |

`test/vectors.test.js` asserts `python/tests/avro/vectors.json`, the same file
the Python suite asserts: one canonical form, one fingerprint, and one binary
encoding per schema shape. Decoding those bytes into JavaScript objects and
encoding them back has to land on the very same bytes, so a change that moves
the bytes in one host but not the other fails in both.

`build` and `build:debug` write the **same** `rkp-avro.<triple>.node`
filename, so whichever ran last is the one that loads. Re-run `npm run build`
before publishing or benchmarking.

### Generated files

`@napi-rs/cli` generates two files from the `#[napi]` items in the Rust crate:

- `binding.js` — the loader. Detects platform/libc and `require()`s the right
  `.node` file, falling back to the per-platform npm packages.
- `binding.d.ts` — TypeScript signatures derived from the Rust source.

Both are **tracked in git** (napi-rs expects them in the published tarball) and
must not be hand-edited — `npm run build` overwrites them. The compiled
`*.node` artifacts are gitignored.

`index.js` and `index.d.ts` are hand-written. They re-export from `binding.*`
and add the one thing that has to be JavaScript: the error classes.

### Why CommonJS?

Native addons can only be loaded through `require()` / `process.dlopen` — ESM
has no loader for `.node` files. napi-rs's generated `binding.js` leans on that
directly: it does platform detection with a chain of `try { require(...) }
catch {}`, which has no clean ESM equivalent. Shipping the package as CJS keeps
that loader idiomatic, and costs ESM consumers nothing, because Node resolves
named exports out of CJS via `cjs-module-lexer`.

That last part is a real constraint on `index.js`: the lexer only recognises
**static** `module.exports.name = ...` assignments. Building the export table
from a loop or an array of names silently breaks
`import { parseSchema } from '@rkp/avro'`.

## Layout

```
rkp/
├── rust/crates/
│   ├── rkp-avro/          core implementation (shared by all bindings)
│   ├── rkp-avro-node/     #[napi] wrapper — the addon this package builds
│   │   ├── src/lib.rs     the exported surface: parseSchema, Schema, Avro
│   │   ├── src/convert.rs the JavaScript value mapping, both directions
│   │   └── src/errors.rs  core failures to JavaScript error classes
│   └── rkp-avro-python/   PyO3 wrapper
└── js/                    this package
    ├── index.js           curated public surface + the error classes
    ├── index.d.ts         types, re-exported from binding.d.ts
    ├── binding.js         generated loader        (do not edit)
    ├── binding.d.ts       generated types         (do not edit)
    └── test/              node --test suite
```

## License

Apache-2.0. See [LICENSE](../LICENSE).
