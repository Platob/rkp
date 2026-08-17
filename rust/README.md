# rkp Rust workspace

The Avro implementation behind [rkp](https://github.com/Platob/rkp). The format
is implemented once, in Rust, and wrapped by one crate per host language.

```
rust/
├── Cargo.toml            workspace manifest (edition 2024, Rust >= 1.85)
└── crates/
    ├── rkp-avro/         the core: schemas, encodings, containers
    ├── rkp-avro-python/  PyO3 extension module, imported as rkp._avro
    └── rkp-avro-node/    napi-rs addon, published as @rkp/avro
```

## The crates

**`rkp-avro`** owns the format and nothing else — it has no knowledge of files
or host objects. It implements schema parsing, the specification's parsing
canonical form, CRC-64-AVRO (Rabin) fingerprints, the binary and JSON
encodings, single-object framing, and object container files that are readable
*and* writable at any record index. Blocks are located by walking frame headers
without decompressing anything, so reaching record *k* decodes exactly one
block. The `null`, `deflate`, `bzip2`, and `xz` codecs are compiled in through
`flate2`, `bzip2`, and `liblzma`; `serde_json` is the only other dependency.

```rust
use rkp_avro::{Schema, Value, binary};

let schema = Schema::parse_str(r#"{"type":"record","name":"point",
    "fields":[{"name":"x","type":"long"}]}"#).unwrap();
let mut encoded = Vec::new();
binary::encode(&schema, &Value::Record(vec![Value::Long(7)]), &mut encoded).unwrap();
assert_eq!(binary::decode(&schema, &encoded).unwrap(),
           Value::Record(vec![Value::Long(7)]));
```

**`rkp-avro-python`** is the PyO3 binding, built as the `rkp._avro` extension
module. It owns the translation between core values and Python objects,
including the logical types — `date`, `time`, `datetime`, `Decimal`, and `UUID`
are resolved here rather than in the core, because each host holds its own
objects. File provenance, the public class model, and the codec facade stay in
Python, in `rkp.avro`.

**`rkp-avro-node`** is the napi-rs addon behind the `@rkp/avro` npm package,
and does for JavaScript what the PyO3 crate does for Python: `src/convert.rs`
owns the value mapping in both directions — including the logical types, where
`Date`, decimal strings, and `bigint` are chosen for what JavaScript can hold
losslessly — while `src/errors.rs` maps the core's error variants onto the
`AvroError` classes the package exports. See [`js/README.md`](../js/README.md)
for the mapping tables and the generated-file layout.

Only `rkp-avro` is publishable; both binding crates set `publish = false` and
ship inside their host packages instead.

## Tests

The core's tests live beside it and cover fingerprint vectors from the
specification, canonical form, recursive named types, binary and JSON round
trips, single-object framing, container random read/write across every codec,
and rejection of corrupt containers:

```console
cargo test                                  # inside rust/
cargo test --manifest-path rust/Cargo.toml  # from the repository root
```

That also runs the crate-level doctest. `cargo fmt` and
`cargo clippy --workspace --all-targets` cover the same workspace.

`cargo test` covers the core alone, because the workspace names it as its only
default member. The two binding crates are `cdylib`s that resolve their host's
symbols only once CPython or Node loads them, so a test binary linked against
either has nothing to bind to. They are tested where they actually run:

```console
uv run --project python pytest -q python/tests/avro   # the PyO3 binding
cd js && npm test                                     # the napi-rs addon
```

Both suites assert `python/tests/avro/vectors.json`, which pins one canonical
form, one fingerprint, and one binary encoding per schema shape — so a change
that moves the bytes in one host but not the other fails in both.

## Building the extensions

The **Python** extension is built by [maturin](https://www.maturin.rs), which
`python/pyproject.toml` configures as the build backend
(`manifest-path = "../rust/crates/rkp-avro-python/Cargo.toml"`,
`module-name = "rkp._avro"`). Any install or build of the Python project
compiles it, so a Rust toolchain has to be on `PATH` first:

```console
uv sync --project python --extra test     # compiles the extension
uv build --project python --no-sources    # wheel + sdist
```

The **Node** addon is built by `@napi-rs/cli` from the `js/` package, which
writes `rkp-avro.<triple>.node` next to `index.js`:

```console
cd js && npm install && npm run build
```

## License

Apache-2.0. See [LICENSE](../LICENSE).
