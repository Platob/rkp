# Yggdryl implementation rules

These rules apply to the entire repository. Yggdryl is a small schema,
resource-identifier, and structured-codec core, not a trading engine or
protocol stack. Keep the native public domain model centered on `DataType`,
`Field`, `Uri`, `Url`, `Urn`, and the byte-oriented codec `Value`; Python and
JavaScript are runtime views of those Rust values, never independent schema or
codec implementations. Python records are a language-level convenience layer
that compiles annotations into native values.

## Order of work

- **A feature is validated fully in Rust first - implementation, edge-case
  tests, documentation - and only then implemented in the Python and JavaScript
  extensions. Never build a binding for a core surface that is not already
  proven.** A binding written against an unsettled core encodes decisions the
  core has not made yet, so it has to be rewritten when the core settles, and
  its tests pin behavior the core never agreed to. "Proven" means the Rust
  surface has its edge cases covered, its documentation page written with
  running examples, and, where the feature is an exchange format, a check
  against an outside implementation of that format. A stage that touches
  `rust/` and stops there is complete work, not half of a change.

## Source layout and scope

- The repository root owns the workspace manifest, the shared dependency pins,
  and the shared lints. Its members are `rust/` (the core crate), `python/`,
  and `node/`. Every member uses the same directory names: `src/`, `tests/`,
  and `benchmarks/`. There are no per-language `examples/` directories -
  runnable examples live in the documentation, where every language appears
  side by side.
- **A struct `Field` is the schema.** There is no separate record or schema
  type: a non-null `Struct` field describes rows, and a row is one ordered
  `Value::Sequence` with a value per child field. Everything that used to take
  a schema object takes a `Field`, and everything that described one now
  describes a field. Never reintroduce a parallel `Record`/`RecordSchema` pair.
- Schema behavior belongs in the categorized `rust/src/datatype/` or
  `rust/src/field/` module. Keep generic `Field` state and core mutation in
  `field/mod.rs`, Arrow projection in `field/arrow.rs`, typed per-datatype
  casting in `field/cast.rs`, recursive grammar in `field/parser.rs`,
  structural serialization in `field/serde.rs`, comparison in `field/diff.rs`,
  schema-directed row validation in `field/value.rs`, and the compile-time
  markers in `field/typed.rs` with their per-family modules
  (`scalar`, `integer`, `floating`, `temporal`, `decimal`, `binary`, `nested`).
- Keep the immutable metadata value in `rust/src/metadata.rs`; all cache-aware
  mutation stays on `Field`. Metadata is owned and validated by `Field` only -
  never attach metadata to a bare `DataType` and never add a binding-side
  metadata model.
- Keep shared static value vocabularies below `rust/src/enums/`: `DataTypeId`,
  `DataTypeKind`, `MimeType`, `MediaType`, `Scheme`, `TimeUnit`, `UnionMode`,
  `Codec`, `Level`, and `IOKind`. Datatype, identifier, I/O, and binding
  modules reuse these enums instead of defining local copies. One native
  `TimeUnit` owns both temporal resolutions and Arrow interval layouts.
  `MimeType`/`MediaType` own MIME spelling, suffix, content-coding, and
  compound-filename inference for the whole workspace, including the
  file-system types `inode/directory` and `inode/file`.
- Keep the generic enums - the ones that name every implementation of one
  contract - below `rust/src/generic/`: `Holder` over every `IOBase`
  implementation, `Media` over every record encoding bound to a handle, and
  `RecordOptions` over every encoding's settings. A generic enum delegates the
  whole contract to the variant it holds and adds no behavior of its own.
- Keep byte storage below `rust/src/io/`: the `IOBase` trait, the in-memory
  `Buffer`, and the transparent-compression `Coded` wrapper. Keep the shared
  record settings in `generic/options.rs` as `IORecordOptions` and
  `RecordSettings`.
- Keep each content coding in its own top-level module: `rust/src/gzip/`,
  `rust/src/zlib/`, `rust/src/zstd/`. Each exposes the same four operations -
  `load`/`dump` for whole buffers, `reader`/`writer` for streams - plus a
  transparent handle (`Gzip`, `Zlib`, `Zstd`) that implements `IOBase` by
  decompressing reads and compressing writes. `Codec` in `enums/codec.rs`
  dispatches across them; never add a fourth spelling of a coding.
- Keep each file-system backend in one module folder supplying the same three
  roles, and name them the same way in every backend:
  - a generic `Path` that reports `IOKind` by looking at what is actually
    there and runs every operation through the specialized implementation that
    fits, so a caller who does not know what a location is can still use it;
  - a `Folder` container that lists and resolves children;
  - a `File` leaf that holds bytes.
  `rust/src/local/` is the local implementation, whose `File` is a memory
  mapping. A remote backend (S3, GCS, Azure) is a sibling module supplying the
  same three roles - never a change to `io` or to `local`.
- Keep Arrow interop in `rust/src/arrow/` (Struct scalars, StructArray,
  RecordBatch, IPC readers and writers, and `schema_from_field`). Recursive
  casting belongs with the schema it casts to, in `rust/src/field/cast/`: the
  plan engine in `cast/plan.rs`, the typed per-datatype surface in
  `cast/mod.rs`. There is no separate cast or media module. The `arrow` feature is default-enabled; callers
  that need schema projection without Arrow arrays use `default-features =
  false`. Never reintroduce a separate Arrow crate.
- Keep record encodings in one module each: `rust/src/ipc/` for Arrow IPC and
  `rust/src/parquet/` for Apache Parquet (behind the non-default `parquet`
  feature). Each module owns free functions over any `IOBase` handle plus a
  stateful type (`Ipc`, `Parquet`) that holds the handle, its options, and its
  cache. `IOBase`'s record methods dispatch to those free functions, so a bare
  handle can read and write records with no extra type.
- Keep the Apache Iceberg table format below `rust/src/iceberg/` (behind the
  non-default `iceberg` feature, which implies `parquet`), split by what each
  file owns: the type vocabulary in `types.rs`, schema documents in `schema.rs`,
  partition specs and transforms in `partition.rs`, snapshots and refs in
  `snapshot.rs`, table metadata in `metadata.rs`, manifest lists and manifests
  in `manifest.rs`, the Avro object container in `avro.rs`, Parquet footer
  statistics in `statistics.rs`, Iceberg's two renderings of a scalar - its text
  and its single-value bytes - in `value.rs`, scan planning and the reader it
  produces in `scan.rs`, and the `Table` over one handle in `table.rs`. A table
  format sits on top of the record encodings; it never becomes one.
- There is no tabular descriptor, dataset, or in-memory table type. A handle
  plus `RecordOptions` is the whole surface: an in-memory table is a `Buffer`
  read and written through `IOBase`'s record methods.
- URI, URL, URN, and resource-path behavior belongs in `rust/src/uri.rs`.
  Shared structured-text values, formats, limits, wire envelopes, display
  helpers, and byte-position utilities belong below `rust/src/text/`. JSON
  implementation belongs below `rust/src/json/`, YAML below `rust/src/yaml/`,
  and TOML below `rust/src/toml/`; all use the one shared `Value`. Keep
  `rust/src/codec.rs` as a compatibility-only re-export facade.
  `rust/src/lib.rs` may contain exports and shared error plumbing only.
- Keep the native packages directly under `rust/python/` and `rust/node/`.
  Mirror core domains in each package as
  `src/{datatype,field,media,uri,codec}.rs`; keep its `lib.rs` for shared
  boundary helpers, exports, and module registration only. Keep Python-only
  annotation behavior below `python/yggdryl/records/`.
- Keep protocol-specific metadata as inert string properties named
  `<scheme>:<property>`. Reuse `Scheme` for the prefix and one generic property
  API; do not add protocol execution, network clients, or duplicate
  per-protocol maps to the core. A per-protocol *view* is not a duplicate map:
  it remembers the prefix and reads out of the one shared snapshot, which is
  what keeps a `scheme:name` key from being spelled by hand at a call site. The
  exact Arrow/Parquet compatibility key `PARQUET:field_id` is the reserved
  exception: own it through Field's typed ID API rather than normalizing it as
  a generic protocol property. A module that carries state in its own namespace
  - Iceberg's `iceberg:doc`, `iceberg:transform`, `iceberg:spec-id` - reaches it
  through that view rather than through a private key constant.
- Store HTTP representation metadata under canonical lowercase `http:*`
  property keys for both HTTP and HTTPS.

## Storage and I/O contract

- **`IOBase` is the one storage abstraction.** It is positional, not
  cursor-based: `pread`/`pwrite` take an explicit offset, so a footer-first
  container reads its index without seeking a shared cursor and two readers
  share one handle without coordinating. Everything else - streaming adapters,
  whole-value reads, compression, record reads and writes - is derived from
  those two primitives. Never add a second storage trait.
- **Handles are as lazy as possible.** Constructing a handle must not touch the
  underlying resource: it is a description of where bytes would live, not proof
  that they do. Non-existence is resolved at the operation:
  - reads skip - `pread` on a missing resource yields `0` bytes and `size`
    reports `0`, so absence is emptiness rather than an error;
  - writes create - `pwrite`, `truncate`, and `reserve` create the resource and
    any parent it needs on first use.
  Metadata follows the same rule: `media_type` is computed on demand, from
  content or from a location, and re-derived after the bytes change.
- `IOKind` is how a handle says what it addresses (`Memory`, `File`,
  `Directory`, `Unknown`). A generic location reads it to choose an
  implementation; everything else uses it to tell a container from a leaf.
  `is_container` derives from it - never re-answer the question independently.
- A handle that wraps another handle mirrors it: implement `IOBase` with the
  `delegate_iobase!` macro and override only what the wrapper changes (usually
  `open`/`close`, which manage a cache). This is how `Coded`, `Gzip`, `Ipc`,
  `Parquet`, and `Media` all expose their raw bytes.
- `open`/`close` are the contextual pair: `open` materializes the handle and
  caches whatever repeated calls would otherwise re-derive (a schema, a
  footer), and `close` publishes and releases it. Bindings bind their scope
  dunders to exactly these.
- **The record surface is exactly three methods, named for what they do.**
  `IOBase::read_arrow_batch_reader(options)` returns an `arrow::BatchReader`,
  `IOBase::write_arrow_batch_reader(reader, options)` replaces or merges, and
  `IOBase::append_arrow_batch_reader(reader, options)` adds after what is there.
  Every other record operation is expressed through those three, so exactly one
  place decodes an encoding and exactly one place encodes it. Never reintroduce
  `overwrite_arrow_batches`, `append_arrow_batches`, `upsert_arrow_batches`, or a
  partitioned spelling of any of them: a second entry point into an encoding is a
  second place for the encodings to drift apart. A record read or write never
  takes or returns a collected `Vec`, a slice, or a borrowed iterator of batches
  - a shape that must be materialized first cannot describe a resource larger
  than memory. `arrow::batch_reader` is the one constructor that turns batches a
  caller already holds into a reader. Record access otherwise stays centralized
  on `IOBase`: `record_options` and `read_arrow_field` complete the surface, and
  both are answered through the three rather than beside them. The encoding is
  never guessed - it comes from the handle's media type through
  `RecordOptions::for_media_type`, and a container answers with the encoding of
  the leaves beneath it - and the shared settings (schema, root name, cast
  strictness, batch size, compression level, match key) are flattened fields on
  each `IORecordOptions` implementation.
- **A declared schema selects and casts during the read.** The columns
  `IORecordOptions::schema` names that the resource stores are handed to the
  encoding as its own projection - a Parquet `ProjectionMask` over root columns,
  an Arrow IPC projection - and what comes back is cast to the declared shape one
  batch at a time. Never emulate the projection by reading everything and
  dropping columns after, and never emulate the cast by leaving a caller to do it
  again. A projection can only drop columns, so reordering, converting, and a
  column the resource does not hold are the cast's business; no declared schema
  preserves the stored shape exactly. Say plainly what each encoding's projection
  saves: Parquet skips locating and decoding a column chunk, while an Arrow IPC
  record batch is one contiguous message, so its projection saves the decode and
  the allocation but not the bytes.
- **A write is a replacement or a key-matched merge, and nothing else.**
  `IORecordOptions::merge_by` names the columns forming the match key. Empty
  means overwrite: a declared schema is applied to the incoming rows, then the
  result is safe-cast to the schema the resource already stores when it stores
  one, so an overwrite replaces rows rather than redefining columns. Non-empty
  means merge: the stored rows are read, the incoming reader is joined against
  them one batch at a time, a row whose key is already stored updates it, a row
  whose key is not appends, and the merged contents are rewritten. The join is
  streamed over the incoming side, and whatever has to be held says so in a
  comment with its reason. Positional batch upsert is gone: a position is not a
  row identity, and a match key is.
- A location may name a set rather than one resource. `Url::is_glob` decides
  that, `glob_parts` splits the fixed root from the pattern, and `matches_glob`
  applies the `.gitignore` rule: no separator matches the name at any depth, a
  separator anchors at the path root. A glob is folder-like *before* the
  backend is touched, so `IOKind` reports a container and `ls` expands the
  pattern rather than looking for a directory named `**`. `IOBase::glob`
  descends every fixed prefix segment before it lists anything.
- A Hive path carries data, and the three record methods handle it themselves.
  `Url::hive_partitions` reads `column=value` directories, `hive_partitions_under`
  reads only the ones below an addressed root, and `IOBase::children_where`
  yields the leaves carrying a set of them - never containers. A handle
  addressing a container reads across every leaf holding its encoding and routes
  each row of a write to the leaf its partition values name, so a caller never
  has to know whether they addressed one file or a partitioned tree. A container
  holding a *table format* is the one exception, and it is asked first: a folder
  with an Iceberg metadata document is read and written through that table's
  snapshots rather than through its leaves. The layout
  is the authority on which columns are partition columns, because nothing in a
  batch says which of its columns belong in a path: a folder whose leaves spell
  out `column=value` partitions by those columns, and a folder whose leaves do
  not takes the layout from the declared schema, whose partition-marked fields
  say it - and a folder that has neither is one table in one leaf. A declaration
  that contradicts a stored layout is refused naming both, because one write
  cannot mean two trees. `io/partition.rs` moves those columns between the
  path and the rows, typed as the schema declares, left alone when the data
  already carries them, and spelled `null` when a value is absent - which is the
  one thing a path cannot say for itself, so a declared nullable column is what
  turns the text back into a null. A restored partition column carries the
  marker, so a lake read back reports the layout it was stored in.
- One renderer spells every partition value. `io::partition::partition_text`
  renders one value the way `partition_values` renders a whole column - the
  encoding's own display, `null` for the absence of one - so a table format
  writing a directory name and a folder writing the same one cannot disagree
  about how a date is spelled. Never add a second partition-text renderer.
- Content coding belongs to the handle, not to the format. A handle named
  `trades.arrows.gz` round-trips compressed with no extra argument, because
  `IOBase::codec` recovers the coding from the media type. Parquet is the
  documented exception: it compresses internally, so a handle declaring an
  outer coding is rejected rather than double-compressed.

## Table format contract

- **A table is a folder, reached through `IOBase` and nothing else.**
  `rust/src/iceberg/` constructs a `Table` from one container handle and finds
  its metadata documents, manifest lists, manifests, and data files with
  `child_by` and `ls` against that handle. No path is opened, `std::fs` is never
  called, and a recorded absolute location is turned back into a relative name
  before it is resolved - which is what makes the same code work over a local
  directory today and over an object store the moment a backend for one exists.
  A table without a catalog is located the way `HadoopTables` locates one:
  `metadata/version-hint.text`, falling back to the highest-numbered
  `*.metadata.json`.
- **No dependency for the table format itself.** The metadata documents are
  ordinary JSON and are read and written by `rust/src/json/` through the shared
  `Value`; the manifests are Avro and the object container is implemented in
  `rust/src/iceberg/avro.rs`, because it is a header and some blocks. Data files
  are whatever `rust/src/parquet/` wrote, read back through
  `read_arrow_batch_reader`'s column pushdown, and their manifest statistics come from
  the footer that write already produced. Never add an Iceberg, Avro, or catalog
  crate, and never let `serde_json` back into this module. The published
  `iceberg` crate was evaluated against this design and does not fit it: it pins
  arrow and parquet 55 against this workspace's 59, so a `RecordBatch` cannot
  even cross the boundary; it reaches storage through an `opendal`-backed
  `FileIO` whose backends are cargo features rather than an extension point, so
  it cannot be driven through `IOBase`; and it is async throughout while `IOBase`
  is sync and positional. Re-evaluate only if all three change.
- **A scan is planned from the metadata, never from a listing.** The current
  snapshot names the manifest list, the list's `FieldSummary` rows skip whole
  manifests unopened, a manifest entry's partition tuple skips one file, and a
  data file's column bounds and null counts skip one more. `Table::plan` reports
  what it read and what it skipped, so pruning is a number a test asserts on.
  A filter is a `(column, value)` text pair - the same vocabulary
  `IOBase::children_where` uses - compared to the text the layout spells for an
  `identity` partition column and to the value a cast from that text produces for
  every other column; a statistic bounds a file and does not select a row, so the
  surviving files are filtered row by row afterwards. Never answer a scan by
  walking `data/`.
- **A table is reached through the same three record methods as a folder.** A
  container handle that holds a metadata document reads through its snapshot,
  writes as one commit, and appends as one commit, so
  `write_arrow_batch_reader`'s `merge_by` upserts an Iceberg table exactly as it
  upserts a leaf. A handle addressing one of the table's `column=value`
  directories addresses that partition of it. The merge reads only the data files
  whose recorded bounds for the key columns overlap the incoming keys and carries
  every other file into the new snapshot as an `existing` entry - same location,
  same statistics, same commit order - which is correct however coarse the
  statistics are, because a file that is not read keeps every row it had. The
  incoming side is what a table merge holds, and the comment says why: a key range
  cannot come from a reader that has not been read.
- **The manifest is the authority on a partition value; the path is layout.**
  An Iceberg table writes the same `column=value` directories `hive_partitions`
  reads - through the one renderer every partition value goes through, so a date
  is `day=2024-01-01` in a table and in a lake alike - and a scan must still take
  partition values from the manifest tuple: a null value is spelled `null` in a
  path, and a path cannot say whether that is the string or the absence. A scan
  compares the manifest's *value* to the filter's, with the text as a fallback,
  because a value is what the manifest holds and text is what a path can say.
- **A spec and a schema say the same thing, so neither is invented twice.**
  `PartitionSpec::partition_field` stamps each tuple child with its transform,
  its source column, and the partition marker, so `PartitionSpec::from_field`
  reads the spec back off the tuple; `mark_partitions` marks the schema's own
  columns, and `PartitionSpec::from_schema` builds an identity spec from a
  schema that already carries them. A table's stored schema therefore reports
  its layout whether it was just created or just opened.
- **A transform that cannot place a row is refused by name.** Only `identity`
  and `void` are invertible here, so a write against a spec using `bucket`,
  `truncate`, or a calendar transform reports which transform stopped it rather
  than writing rows into the wrong partition. Reading such a table is
  unaffected.
- **Statistics are emitted only where the two encodings agree.** A manifest
  bound travels as an encoded value; emit one only for the types whose Parquet
  statistic bytes *are* the Iceberg single-value encoding, and emit counts alone
  for the rest. A missing statistic costs a planner one file read; a wrong one
  costs correctness.
- **A column change is a new schema, built by `SchemaUpdate` and gated by
  `can_promote`.** The legal promotions are exactly Int32→Int64,
  Float32→Float64, and decimal(P,S)→decimal(P′,S) with P′≥P at the same scale;
  everything else is refused naming both sides. Added columns are numbered
  above `last-column-id` by the core walk, a dropped column's identifier is
  never reused, and a renamed column keeps its identifier. `TableMetadata`
  owns the update vocabulary (`set_property`, `set_location`, `assign_uuid`,
  `upgrade_format_version`, `set_current_schema`, `add_spec`,
  `set_default_spec`, `add_sort_order`, `set_default_sort_order`,
  `set_snapshot_ref`, `remove_snapshot_ref`, `remove_snapshots`) and
  `TableMetadata::validate` runs on load and before every commit, so a broken
  document can be read but never written.
- **Every retained snapshot is a complete table, and reading one is an
  ordinary scan.** Time travel is `scan_at`/`plan_at` with a snapshot id and
  `snapshot_by_ref` for a branch or tag; the snapshot is read as the schema it
  was written under. A metadata-only change - a property, a ref, an evolved
  schema - commits through `Table::commit_changes`, which writes one new
  document and leaves the in-memory table unchanged on any failure. The
  table's own record renders as record batches through `inspect_history`,
  `inspect_snapshots`, and `inspect_files`, under PyIceberg's column names;
  never add a second, struct-shaped spelling of the same report.
- **A catalog is a warehouse folder, reached through `IOBase` and nothing
  else.** `iceberg::Catalog` maps a dotted name (`nyc.taxis`) onto nested
  folders, creates a table from a schema whose own partition marks supply the
  spec, and answers create-or-append (`append`) and `overwrite` so a caller
  who only has rows and a name needs nothing else. It holds no network code
  and no transaction protocol; a REST catalog remains future work behind an
  HTTP storage backend. Drop and rename are deliberately absent until the
  storage contract gains delete and move primitives - name that reason, do not
  emulate them.
- **A data file has a target size, and one key names it.** The table property
  `write.target-file-size-bytes` - falling back to the schema root's
  `iceberg:write.target-file-size-bytes` protocol property, then Iceberg's 512
  MiB default - rolls a partition's stream into multiple files at batch
  boundaries, sized by Arrow in-memory bytes (so Parquet lands under the
  target, and the docs say so). `Table::compact` rewrites the small-file
  groups the same rolling way, commits as `replace` carrying every untouched
  file, reports what it rewrote, and is a no-op that commits nothing when
  there is nothing to do. A present-but-unparseable size is a typed error
  naming key and value, never a silent default.
- **Every knob is one field of `iceberg::IcebergOptions`, resolved
  explicit-then-property-then-default.** The keys are Iceberg's own spellings
  (`commit.retry.num-retries`, `commit.retry.min-wait-ms`,
  `commit.retry.max-wait-ms`, `write.target-file-size-bytes`,
  `read.parallelism`, `read.parallel.min-files`,
  `read.parallel.min-file-size-bytes`); the property layer falls back to the
  schema root's `iceberg:` protocol spelling exactly as the target size always
  has, and each field has exactly one resolver function - never a second
  resolution path. An explicit option set with `Table::set_options` lives on
  the handle alone, is never written to the table, and shadows its property
  without parsing it, so a broken stored value can be shadowed first and
  repaired after; each operation resolves only the keys it consults, so a
  broken `read.*` property cannot block the metadata-only commit that fixes
  it. Do not add a knob outside this value.
- **Every commit goes through one retrying gate, and what a retry may do is
  the operation's nature.** The gate re-checks the current version, counts
  each newer version as being beaten once, and backs off with full jitter up
  to `commit.retry.num-retries` times. `append`, `commit_changes`, and
  everything routed through them *rebase*: data files and the added-entries
  manifest are written once and reused, the intent re-applies on the winner's
  document. `overwrite`, `merge`, and `compact` never rebase - they planned
  against files the winner may have replaced and their input is consumed - so
  they wait, re-observe, and exhaust into a `CommitConflict` naming both
  versions, with in-memory state restored. Say plainly, wherever this
  surfaces, that `IOBase` has no compare-and-swap: the check is best-effort on
  plain storage, retries shrink the undetected-race window and cannot close
  it, and a failed commit leaves at worst orphan data files no snapshot names.
- **Branches and tags are metadata, with retention per ref.** `SnapshotRef`
  carries `min-snapshots-to-keep`, `max-snapshot-age-ms`, and
  `max-ref-age-ms`; `expire_snapshots` honors every ref's own fields before
  its age cutoff, `main` itself never expires, and a fast-forward must reach
  the branch head by walking parent ids so it can never lose history. The
  `Table` conveniences (`create_branch`, `create_tag`, `remove_ref`,
  `fast_forward`, `expire_snapshots`, `scan_ref`) commit through the retrying
  gate; writing *to* a non-`main` branch is future work (a commit's parent is
  always the current snapshot) - name that limit, do not emulate branch
  writes.
- **A scan fans out only when the plan earns it, and in plan order always.**
  Parallel decode starts when `read.parallelism` is at least 2 and at least
  `read.parallel.min-files` planned files carry recorded sizes of at least
  `read.parallel.min-file-size-bytes` (defaults 16 files, 4 MiB, parallelism
  clamped to the host's 1..=8); below the thresholds the sequential
  single-open path runs. Workers decode and refine whole files, at most
  `read.parallelism` in flight, and a reorder buffer releases batches
  strictly in plan order, so parallel and sequential scans are
  indistinguishable but for speed - never let an optimization change what a
  caller observes, and never hammer storage beyond the configured width.
- **An exchange format is validated against an outside implementation.** Round
  tripping through this crate's own reader proves only that the reader and the
  writer agree with each other. `scripts/check_iceberg_interop.py` and the
  `rust/tests/iceberg_interop.rs` target run the exchange with PyIceberg in both
  directions and compare the actual rows; the Rust half announces on stdout when
  the external table is absent, so a skipped half can never read as a pass.

## Documentation organization

- Build the public site with the root `mkdocs.yml`; keep publishable pages
  below `docs/` and keep the strict GitHub Pages build green. Every added,
  removed, or renamed page must update the MkDocs navigation and all affected
  links in the same change.
- Write guides example-first: put the smallest runnable Rust, Python, or
  JavaScript example before its explanation, then explain only the behavior,
  invariants, and tradeoffs the example cannot show. Prefer several focused
  examples over a long narrative or one oversized example.
- When a concept is public in multiple runtimes, present equivalent examples
  in linked Material tabs labeled `Rust`, `Python`, and `JavaScript`, in that
  order. Keep a single-language example only for an intentional runtime
  boundary and say why; never invent binding-side logic merely to fill a tab.
- **One page per core module folder, named after it.** `docs/core/<module>.md`
  documents `yggdryl::<module>` and nothing else, so the site tree and the source
  tree are the same tree: `enums`, `datatype`, `field`, `arrow`, `io`, `generic`,
  `local`, `gzip`, `zlib`, `zstd`, `ipc`, `parquet`, `iceberg`, `uri`, `text`,
  `json`, `yaml`, `toml`. `docs/extensions/python.md` and
  `docs/extensions/javascript.md` document only their language boundary, never a
  second description of core behavior.
- **Every page opens with one H1 and exactly one short sentence** saying what the
  module is for, before any prose or example. That sentence is what a reader sees
  in a search result.
- **Every example appears in all three languages** - Rust, Python, JavaScript, in
  that order - showing the same operation. A module the bindings do not expose
  carries this note directly under its lead sentence instead, and shows Rust
  alone: `!!! note "Rust only"` / `The Python and JavaScript packages do not
  expose this module yet.` Never fabricate a binding tab.
- **Each tab is idiomatic in its own language, not a transliteration.** The three
  tabs perform the same operation; they do not perform it the same way.
  - Rust shows the explicit typed call and the `?` on a `Result`.
  - Python uses the language protocols the binding implements: `len(field)`,
    `field["key"]`, `for name in field`, `"key" in field`, `field.get(...)`,
    unpacking, f-strings, `with` for a scoped handle, and keyword arguments with
    their real defaults (`safe=True`). Show a bare `str`, `bytes`, `pathlib.Path`,
    or PyArrow value where the binding coerces one, because that is how the
    binding is meant to be used.
  - JavaScript uses the generic entry points and the JS protocols: `DataType.from`,
    `Field.from`, `Url.fromPath`, `Map` iteration over metadata, spread and
    destructuring, and `for...of`. Prefer the generic `from`/`fromX` constructor
    over a longer explicit path when both exist.
  - Never show a language doing something it cannot: check
    `.api-bindings.txt` first.
- Split the datatype implementation by responsibility under
  `rust/src/datatype/` (scalar/integer/floating/temporal/nested, parsing,
  Arrow interop, and serialization). Modules must own real implementation;
  do not create empty category files around a monolithic `mod.rs`.
- Keep Rust examples, tests, and benchmarks categorized like the core. The
  datatype and field targets use small `datatype.rs`/`field.rs` dispatchers
  with real cases below `rust/{tests,benches}/{datatype,field}/`. Mirror the
  implementation responsibility (`generic`, `comparison`, `typed`, parser,
  Arrow) and split typed cases further into scalar/integer/floating/temporal/
  binary/decimal/nested only when each file owns meaningful cases; do not add
  empty category shells. URI remains a focused domain file.
  Structured-text, JSON, YAML, and TOML targets mirror `text/`, `json/`,
  `yaml/`, and `toml/` ownership directly; retain stable benchmark group IDs when splitting a
  Criterion target so measurements remain comparable. Core Arrow runtime
  regressions are `rust/tests/{arrow_record,default_scalar,tabular,value_bounds}.rs`;
  its examples are `rust/examples/{arrow_record,tabular}.rs`, and its benchmark
  sources are `rust/benches/{record_arrow,tabular}.rs` while the stable Cargo
  benchmark targets remain `record` and `tabular`.
  Record I/O over a handle is the Cargo benchmark target `io`, dispatched from
  `rust/benchmarks/io.rs` over `rust/benchmarks/io/{record,pushdown}.rs`. It
  requires the `parquet` feature, because the column-pushdown comparison needs
  the one encoding whose projection also skips reading. A pushdown benchmark
  reports the bytes each read materializes as its Criterion throughput, so
  "moves less data" is a measured number rather than an inference from elapsed
  time.
  The Iceberg exchange with an outside implementation is the Cargo test target
  `iceberg_interop` (`rust/tests/iceberg_interop.rs`, requiring the `iceberg`
  feature), driven by `python scripts/check_iceberg_interop.py`. It writes a
  table for PyIceberg to read and reads one PyIceberg wrote; when the external
  half is absent it prints `SKIPPED` rather than passing quietly, and the driver
  fails on that word.
  Keep extension-owned
  examples, tests, and benchmarks inside that extension and group them by the
  same feature named in its documentation. Do not create repository-root
  extension examples or duplicate core examples in a binding.
- Every example in the documentation must run, in every language it is shown in.
  `python scripts/check_docs_examples.py` compiles the `rust` blocks as tests,
  runs the `python` blocks under `python/.venv`, and runs the `javascript` blocks
  under node with `@yggdryl/node` rewired to this repository. Each block is
  self-contained - its own imports, no state carried from a previous block, and
  at least one assertion proving what the prose claims. A block that genuinely
  cannot stand alone is tagged `<lang>,ignore`, which the checker reports rather
  than hides.
- The notebooks under `docs/notebooks/` and the `Notebooks` section that links
  them are generated from those same blocks by
  `python scripts/build_docs_notebooks.py`, which the checker runs and then
  re-runs to prove it settled. They are outputs, committed so the site can serve
  them: edit the block, never the notebook or the text between the page's
  `<!-- notebooks: ... -->` markers. The notebooks ship unexecuted and add no
  dependency, because no kernel is installed here and a reader runs them
  elsewhere.
- Treat `https://platob.github.io/rkp/` as the canonical user guide. The root
  README is a short landing page linking there; implementation details and
  exhaustive examples belong in the categorized MkDocs pages.
- Keep documentation tooling isolated in `requirements-docs.txt`; it must not
  become a runtime dependency of the Rust core or either extension.

## Exact method vocabulary

Use names according to ownership and cost. Do not add aliases with different
verbs for the same operation.

- `new`: direct, obvious, infallible construction from native parts.
- `from_*`: construct a new core value from a named representation or foreign
  value. A `from_*` function returns `Result` when input needs validation.
  Examples: `from_str`, `from_arrow`, `from_json`, `from_fields`.
- `into_*`: consume `self` and produce another owned representation. Reuse
  allocations or cached state when possible. Examples: `into_arrow`,
  `into_json`, `into_fields`.
- `to_*`: borrow `self` and produce an owned value, potentially allocating or
  incrementing an `Arc`. Examples: `to_arrow`, `to_arrow_ref`, `to_json`.
- `as_*`: return a borrowed view and never allocate. Examples: `as_str`,
  `as_fields`, `as_metadata`.
- `is_*` and `has_*`: side-effect-free predicates.
- `get`/`get_*`: borrowed lookup with no allocation. Use `get_mut` only when
  mutation cannot bypass validation or cache invalidation.
- `set_*`: validated in-place replacement. An error must leave `self`
  unchanged.
- `with_*`: consuming persistent update returning the changed value. Prefix
  with `try_` only when the sibling `set_*` is fallible.
- `clear_*` removes all values in a named category; `remove_*` removes one
  keyed/indexed value and returns the prior value when useful.

Implement standard conversion traits (`From`, `TryFrom`, `FromStr`, `AsRef`)
in addition to discoverable inherent `from_*`/`into_*` methods. The inherent
methods are the stable API used by bindings.

### Stable core spellings

Keep these conversion names exact; do not add alternate aliases:

- `Scheme`: common protocol values are associated uppercase constants;
  arbitrary valid schemes use `from_str`, while `as_str` is the borrowed
  canonical view. `Scheme` also names the schema-compatibility targets listed
  in `COMPATIBILITY_TARGETS`; a scheme that is not one parses normally and is
  rejected by `to_scheme_compat`, never by the parser.
- `DataTypeId` and `DataTypeKind` are distinct and must not be conflated.
  `DataType::id` is the parameter-free identity of one variant (`int32`,
  `decimal128`), `DataType::kind` is the coarse family that variant belongs to
  (`integer`, `decimal`), and `DataType::name` is `id().as_str()`. Bindings
  expose both as `id` and `kind` with those exact meanings. A comparison that
  wants "which variant is this" uses `id`; only behavior that is uniform
  across a whole family dispatches on `kind`.
- `TimeUnit`: `from_str` and `FromStr` parse temporal and interval spellings;
  `as_str` and `AsRef<str>` expose the same allocation-free canonical view;
  `is_temporal` and `is_interval` distinguish its disjoint Arrow categories.
  Arrow imports are `from_arrow_time` and `from_arrow_interval`; consuming,
  category-safe exports are `into_arrow_time` and `into_arrow_interval`.
  Mirror them with infallible `From<arrow_schema::{TimeUnit, IntervalUnit}>`
  imports and fallible `TryFrom<TimeUnit>` exports. A temporal unit must never
  silently project as an interval unit or the reverse. Serialize stable
  snake_case full-name tokens; deserialize through `TimeUnit::from_str` so
  documented parser aliases remain accepted.
- `MimeType`: common values are associated uppercase constants and unknown
  valid type/subtype values remain supported. Use `from_str`,
  `from_extension`, `from_path`, `from_content_type`, and
  `from_content_coding`; borrowed identity is `as_str`, `top_level`,
  `subtype`, and `structured_suffix`. `extension`, `content_coding`, and
  `format` are the one reverse-inference table. Category predicates include
  top-level media classes plus `is_textual`, `is_structured`, `is_tabular`,
  `is_encoding`, `is_archive`, and conservative `is_binary`.
- `MediaType`: `new` supplies an unencoded base, `from_parts` validates an
  ordered encoding sequence, and `from_str`, `from_extension`,
  `from_extensions`, `from_path`, and `from_content_headers` perform the
  matching compound inference. `base`, `encodings`, and `encoding` borrow its
  state. Mutation is `set_base`, `set_encodings`, `push_encoding`, and
  `clear_encodings`; persistent variants are `with_base` and
  `try_with_encodings`. The default is `application/octet-stream` with no
  encodings. Preserve encoding application order and repeated layers; only
  MIME values classified by the one core table as encodings may occupy the
  encoding sequence.
- `Scheme` doubles as the compatibility vocabulary: `Scheme::ARROW`,
  `SPARK`, `POLARS`, `PANDAS`, and `ICEBERG` are the targets
  `to_scheme_compat` accepts, and `Scheme::COMPATIBILITY_TARGETS` lists them.
  `is_compatibility_target` and `is_storage` classify a scheme; `default_port`
  answers the network ones. Never add a second scheme-like enum. The Iceberg
  target widens what widens losslessly; the Iceberg *codec* stays strict, so
  `PrimitiveType::from_data_type` still refuses a datatype the format cannot
  spell rather than widening it behind a writer's back.
- `Codec`: the closed content codings are `Identity`, `Gzip`, `Zlib`,
  `Deflate`, and `Zstd`; `from_str` accepts the legacy `x-` prefix,
  `from_mime_type`/`from_media_type`/`from_url` recover a coding from a name,
  and `load`/`dump`/`reader`/`writer` apply it. `Level` is one shared 0-to-9
  scale mapped onto each codec's native range.
- `IOKind`: the closed roles are `Memory`, `File`, `Directory`, and `Unknown`,
  with `is_container`, `is_leaf`, and `is_known` as the derived predicates.
- `DataType`: `from_str`, `from_arrow`, `from_json`, `from_fields`,
  `to_arrow`, `into_arrow`, `to_json`, `into_json`, `as_fields`,
  `default_value`, `is_default_value`, and `to_scheme_compat`. The generic
  finite-variant constructor is exactly `variant`; it accepts Fields in
  declaration order, assigns Arrow type IDs `0..`, and returns the canonical
  dense `Union` representation rather than introducing a second logical
  datatype. A variant therefore has `kind() == "union"`, serializes and
  displays as a dense union, and retains lossless Arrow round trips. It accepts
  at most 128 members. The `variant(...)` parser spelling is an input alias
  for this constructor: it accepts only dense layout and sequential IDs from
  zero, while canonical display remains `union(dense,...)`.
  The generic wide-decimal constructor is exactly `decimal`; it selects `decimal128` for
  precision 1..=38 and `decimal256` for precision 39..=76, then delegates all
  validation to that explicit constructor. The generic time-of-day constructor
  is exactly `time`; it selects `time32` for seconds/milliseconds and `time64`
  for microseconds/nanoseconds, then delegates validation to that explicit
  constructor. Interval layouts are rejected without selecting a physical
  width.
- `Field`: `from_parts`, `from_str`, `from_arrow`, `from_arrow_ref`,
  `from_json`, `to_arrow`, `to_arrow_ref`, `into_arrow`, `into_arrow_ref`,
  `to_json`, `into_json`, `default_value`, and `to_scheme_compat`.
- Field mutation: `set_name`, `set_data_type`, `set_nullable`,
  `set_dictionary_options`, `set_metadata`, `insert_metadata`,
  `update_metadata`, `remove_metadata`, and `clear_metadata`.
- Persistent Field updates: `with_name`, `try_with_data_type`,
  `with_nullable`, `try_with_dictionary_options`, `try_with_metadata`,
  `try_with_metadata_entries`, and `with_metadata_removed`.
- Shared Field properties: borrowed `alias`, `catalog_name`, `schema_name`,
  `table_name`; typed `location`; matching `set_*`, `remove_*`, and consuming
  `try_with_*`/`with_location` methods. Protocol properties use only
  `get_property`, `has_property`, `set_property`, `remove_property`,
  `clear_properties`, `property_iter`, `try_with_property`, and
  `with_properties_cleared`.
- One protocol's properties also have a view that remembers the protocol:
  `Metadata::protocol`, `Field::protocol`, and `Field::protocol_mut`, plus one
  named accessor per well-known protocol generated from the single list in
  `metadata.rs` (`field.iceberg()`, `field.iceberg_mut()`,
  `metadata.postgres()`, ...; `https` is deliberately absent because it shares
  the canonical `http:` namespace). A view is a borrow, never a copy. Its
  vocabulary is the collection one - `get`, `contains_key`, `len`, `is_empty`,
  `iter`, `next_entry`, `key`, `insert`, `update`, `set`, `remove`, `clear` -
  where `set` replaces only that protocol's properties and leaves every other
  key alone. Mutation still goes through Field's cache-aware methods; a view
  never touches metadata storage itself. Bindings project the view as one live
  mapping object (Python mapping dunders, JavaScript Map protocol), not as a
  snapshot copy.
- A Field can act as a partition column: the reserved `field:partition` marker,
  read with `is_partition` and written with `set_partition`/`with_partition`,
  says a path spells this column out. A struct root answers
  `partition_fields`, `partition_field_names`, `partition_field_len`,
  `has_partition_fields`, `only_partition_fields`, `without_partition_fields`,
  and `with_partition_fields`. An unmarked field stores no marker at all, so
  two schemas that partition the same way stay exactly equal.
- Arrow/Parquet Field identity is exactly `parquet_field_id`,
  `set_parquet_field_id`, `remove_parquet_field_id`, and
  `with_parquet_field_id`, with the schema-tree walks
  `assign_parquet_field_ids`, `max_parquet_field_id`, and
  `field_by_parquet_field_id`. Store it under the exact Arrow convention
  `PARQUET:field_id` as a canonical signed 32-bit decimal integer. Generic
  metadata construction, import, parsing, and deserialization must apply the
  same validation and canonicalization; do not introduce a second field-id
  key, do not add a generically named `id` alias on `Field`, and do not
  confuse it with an independent protocol property such as
  `iceberg:field_id`.
- HTTP Field reads expose raw `accept*`, `cache_control`, `content_*`, `etag`,
  `expires`, `last_modified`, `range`, and `vary` values plus typed
  `content_length`, `http_location`, `mime_type`, and `media_type` projections.
  Raw mutation uses matching `set_*`/`remove_*` methods. `set_mime_type`
  changes only Content-Type; `set_media_type` and `remove_media_type` update
  Content-Type and Content-Encoding as one transaction. Unsupported HTTP
  codings or malformed prior state must leave metadata and Arrow caches
  unchanged. Keep the preexisting bare `location` distinct from
  `http_location`.
- `Metadata`: `new`, `from_entries`, `from_arrow`, `from_json`, `to_arrow`,
  `into_arrow`, `to_json`, `into_json`, `get`, `contains_key`, `iter`, and
  `protocol`.
- `Uri`, `Url`, and `Urn`: `from_str`, `from_path`, `from_uri`, `to_json`,
  `into_json`, `to_uri`, and `into_uri` where the conversion is meaningful.
  `Uri` additionally uses `to_url`, `into_url`, `to_urn`, and `into_urn`;
  file `Uri`/`Url` values use `to_path` and `into_path`. Component
  access is spelled `scheme`, `authority`, `path`, `query`, `fragment`,
  `namespace`, and `namespace_specific`. Component newtypes expose borrowed
  strings through `as_str`; full identifiers use `Display`/`to_string` so the
  canonical form is not duplicated in storage.
- Resource-path access is spelled `path_segments`, `file_name`, `extension`,
  `extensions`, and `stem`. Iterators borrow the identifier and must not
  allocate. Filename mutation is `set_file_name`, `set_stem`,
  `set_extension`, `set_extensions`, `remove_extension`, and
  `clear_extensions`; it is atomic and preserves unrelated URI components.
  MIME access is `mime_type` and `media_type`; matching setters rewrite only
  the inferred filename suffix chain through the core preferred-extension
  table and reject a type with no known extension without changing the value.
- `Format`: `from_str`, `from_extension`, `from_path`, `as_str`, and
  `extension`. The stable variants are `Json`, `JsonLines`, `Yaml`, and `Toml`;
  extension inference recognizes `.json`, `.jsonl`, `.ndjson`, `.yaml`, and
  `.yml`, and `.toml` without opening a file.
- Structured text: generic format dispatch lives in `text::{from_str,
  from_slice, from_reader, from_str_all, from_slice_all, from_reader_all,
  from_reader_iter, from_str_inferred, from_slice_inferred, to_vec, into_vec,
  to_writer, to_writer_all}`. First-class
  `json`, `yaml`, and `toml` modules mirror the same borrowed-string, byte,
  reader, and writer vocabulary without a format argument; JSON Lines additionally uses
  `json::from_lines_str` for borrowed line-delimited text. `codec` re-exports
  the complete generic surface for source compatibility only. A
  `_with_limits` form is used only where caller limits differ from safe
  defaults; do not duplicate string-first serializers. Redirected TOML output
  calls `toml::validate_for_write_with_limits` before opening or truncating a
  destination, using the runtime's decode limits, then streams through
  `to_writer`. Rust callers using core defaults may use `validate_for_write`.
- `TypedValue` is one value paired with the datatype it belongs to, validated
  against it on construction. It carries the same compile-time markers a Field
  does - `TypedValue<K>` where `K` is a `FieldType`, one alias per datatype
  (`Int64Value`, `Utf8Value`, ...), and `AnyType` for a pairing that has not
  been narrowed, which is the default. Narrowed construction is `try_from_parts`
  and `try_from_value`, the dynamic pairing keeps `from_parts` and `from_value`,
  and a statically known datatype adds `new`. Never add a second marker family:
  a value and a field spell the same one.
- Codec values: validated construction uses `Value::from_sequence` and
  `Value::from_mapping`; `Float` exposes borrowed state through `as_f64` and
  consumes through `into_f64`. There is no application-tag carrier: a name over
  an untyped payload is not a type, because nothing checks that the payload
  matches the name. Every kind a `Value` holds is a variant that carries its own
  parts, and a name a format spells that no variant holds is ordinary data.
  Never reintroduce `Tag`, `TaggedValue`, or a `Value::Tagged` variant.

Python mirrors these names in `snake_case`. JavaScript exposes the same native
operations in conventional casing (`from_str` maps to `fromString`, other
underscores map to camelCase); generic `from`/`from_value` inference belongs only
at the extension boundary and must immediately dispatch to the matching core
`from_*` or conversion trait.

## Error message contract

An error must let a caller fix the input without reading the source. State
what was expected, what was actually observed, and where. A message that only
names a rule is incomplete.

- Always show the offending value next to the expectation. Prefer the
  `expected X, got Y` shape and render both sides with the same formatter so a
  reader can diff them by eye. Write
  `temporal precision must be between 0 and 9, got 12`, never
  `temporal precision must be between 0 and 9`. Write
  `dictionary key must be an integer datatype, got utf8`, never
  `dictionary key is not an integer`.
- Locate the failure. Nested schema, record, and value errors carry the
  dot/bracket path to the failing node; parser and codec errors carry the byte
  position; tabular errors carry the batch index or resource URL. A path is
  required whenever recursion can reach more than one node.
- When two structured values disagree, reuse the shared diff vocabulary rather
  than hand-writing a comparison sentence. Schema mismatches must render
  through the same `show_diff`/`show_diffs` symbols (`≠`, `−`, `+`, `→`, `↳`,
  `✓`) used by value comparison so one display grammar covers both, and must
  report every differing node rather than only the first.
- Prefer a typed variant with named fields over an interpolated catch-all
  string. A variant that carries `expected`, `actual`, and `path` as fields can
  be inspected and localized by bindings; a formatted `String` cannot. Do not
  widen a catch-all variant to cover a case that deserves its own fields.
- Quote user-supplied names and string values with `{value:?}` so empty
  strings, trailing spaces, and control characters are visible. Render
  datatypes and fields through their canonical `Display`, not `Debug`.
- Truncate unbounded values with the shared text limits before interpolating
  them; an error message must never allocate proportionally to an input
  payload.
- Errors must leave the receiver unchanged, and the message must not imply a
  partial write occurred.
- Keep one layered error family: `yggdryl::Error` owns schema, identifier, and
  codec failures, and `yggdryl::arrow::Error` wraps it for runtime boundaries.
  Do not introduce a third parallel enum; a downstream backend reports through
  `Error::external` and preserves its source chain.
- Bindings surface the native message unchanged and map variants to idiomatic
  exception types. Never rewrite, re-prefix, or re-translate a core message in
  Python or JavaScript.

## Native value behavior

- Implement `Clone`, `Debug`, `Display`, `Eq`, `Ord`, `Hash`, `Serialize`, and
  `Deserialize` wherever the value semantics permit it. Ignore caches in
  equality, ordering, hashing, and serialization.
- Field and datatype comparison uses `equals(other, with_metadata)`,
  `show_diffs(other, with_metadata)`, and `show_diff(other, with_metadata)`.
  `with_metadata=false` ignores only Field metadata recursively. Keep
  `show_diffs` lazy and make terminal output stable UTF-8 without ANSI escape
  sequences: prefer compact symbols such as `≠`, `−`, `+`, `→`, `↳`, and `✓`
  with an unambiguous path on every line. `show_diff` joins the iterator with
  newlines and returns `✓ equal` when it is empty. FFI runtimes must store the
  core owning difference cursor; never collect the native iterator into a
  list merely to satisfy host-language lifetime rules.
- `Display` is canonical and must round-trip through `FromStr` without loss.
  `Debug` is diagnostic and must not be the serialization format.
- Collections expose deterministic iteration, length, indexed lookup, named
  lookup where names exist, `IntoIterator`, and `Index` only when panic-on-
  missing is normal Rust collection behavior. Mutation must preserve ordering,
  uniqueness, validation, and Arrow-cache correctness.
- Serialization is version-independent structural data. Deserialization routes
  through the same validation used by constructors and parsers.
- Never panic, unwrap, or rely on unsafe code for caller-controlled input.
- `Scheme`, `Authority`, and `UriPath` are validated, owned, non-null values.
  RFC-permitted absence is represented by an empty component, never `Option` or
  a binding-language null; query and fragment are optional components.

## Parser contract

- `DataType::from_str` and `Field::from_str` are the only recursive schema
  grammar engines. Bindings must call them rather than pre-parsing type
  expressions.
- `TimeUnit::from_str` is the one flat unit-value parser. It accepts canonical
  unit display plus documented, ASCII case-insensitive temporal and interval
  aliases; datatype parsing must reuse it rather than keep a second unit-word
  table. Direct SQL `year`/`years` and `day`/`days` aliases map to `YearMonth`
  and `DayTime`. A bare datatype `interval` defaults to `MonthDayNano`; its
  canonical display must make that layout explicit.
- Accept canonical Yggdryl output plus common Arrow, SQL, Hive, and Spark forms.
  Matching of type keywords is ASCII case-insensitive; field names and quoted
  values retain case and Unicode.
- Support arbitrarily nested lists/arrays, structs/rows, maps, dictionaries,
  unions, run-end encoding, decimals, fixed-size values, and temporal
  parameters up to an explicit recursion limit.
- Treat `variant(...)` as finite dense-union input sugar, not a new logical
  datatype. Assign omitted member IDs sequentially from zero, reject sparse
  mode and non-sequential explicit IDs, enforce Arrow's 128-member type-ID
  limit while consuming input, and canonicalize display to `union(dense,...)`.
- Generic `decimal` and `numeric` syntax must call `DataType::decimal` so the
  parser and programmatic constructor select the same physical width. Explicit
  `decimal128` and `decimal256` spellings retain their exact width limits.
- Generic `time` syntax must call `DataType::time` so SQL precision and unit
  spellings select the same physical width as programmatic construction.
  Explicit `time32` and `time64` spellings retain their exact unit limits.
- Accept balanced optional outer `()`, `[]`, `{}`, single quotes, or double
  quotes. Never strip unmatched or interior delimiters heuristically.
- Split only at top-level separators, honor quoting and escapes, reject trailing
  tokens, duplicate field names/type IDs, invalid nullability, and malformed
  numeric parameters, and return errors with a byte position and context.
- Add round-trip and adversarial tests for every new grammar branch. Benchmark
  cold scalar parsing, deeply nested parsing, and parse/display round trips.

## JSON, YAML, and TOML codec contract

- The Rust boundary is UTF-8 byte-first: parse borrowed byte slices or `Read`,
  and emit `Vec<u8>` or `Write`. Borrowed `&str` conveniences use the same
  parser directly and must not allocate an intermediate UTF-8/byte input
  buffer or repeat UTF-8 validation. Parsing still allocates the owned strings and
  containers required by the resulting `Value`; never describe construction
  of an owned value tree as allocation-free parsing.
- One native `Value` is the lossless superset for all three formats. It preserves
  signed and unsigned 64/128-bit integers, total-order floating values, bytes,
  sequences, and ordered arbitrary-key mappings. Shared nested
  values use immutable `Arc` storage and empty values avoid backing allocation.
- Plain JSON remains plain JSON. Values outside JSON's data model use one
  exact, versioned `$yggdryl` envelope. An ordinary mapping that has the same
  shape must be escaped through the mapping envelope so user data can never be
  mistaken for a typed value.
- The `!yggdryl/*` YAML machine tags name kinds the value model has, and each
  one selects the payload it names. Every other YAML tag is the annotation YAML
  defines it to be: no value can hold a free-form name, so the node decodes as
  the plain value it annotates rather than failing a readable document. Nothing
  on the write path emits a non-core YAML tag. Comments are never consulted for
  decoding because parsers may discard them; never make type safety or
  reconstruction depend on comments.
- TOML implements TOML 1.1 with the newest pinned `toml` release that retains
  the Rust 1.85 floor. Decode its borrowed, spanned `DeTable`/`DeValue` tree so
  user keys cannot collide with Serde's private datetime marker and syntax,
  duplicate-key, budget, and envelope errors retain original byte positions.
  The root is always a table: empty and comment-only explicit TOML documents
  decode as an empty mapping. Native date/time values decode to the temporal
  they name: an offset or local date-time to a timestamp whose count is UTC and
  whose zone is the offset or absent, a local date to a date, a local time to a
  time, each at the coarsest unit that keeps every digit its spelling carries. A
  temporal projects back as native TOML date/time syntax exactly when TOML can
  spell it, meaning a four-digit year, a zone that is a fixed offset rather than
  a place, and a clock reading inside one day; every other temporal, and every
  decimal, uses the typed envelope so a round trip never changes a value.
- TOML has no multi-document stream. Its `from_*_all` forms return exactly one
  value, including the empty root table, and its `to_writer_all` form rejects
  zero values or a second value before writing. Root-scalar, null, bytes,
  unsigned/wide integer, decimal, arbitrary-key mapping, and unspellable
  temporal values use a versioned `$yggdryl` envelope carrying the same
  payload JSON and YAML carry; escape user mappings that would collide with
  that envelope. Every envelope kind names a `Value` variant, so a `type` that
  names nothing the value model holds is not an envelope and its table decodes
  as the ordinary mapping it is. Native TOML integers decode as signed 64-bit values and
  overflow is rejected; preserve each non-native integer storage variant in
  its typed envelope. Count the root table as depth one and preflight the exact
  wire projection before writing so every emitted value is accepted by the
  same published hard depth cap.
- A decoded document never names a class. A binding constructs a class only from
  a target the caller passed in, and must never import a module, evaluate a
  name, invoke a constructor, or mutate globals because of anything an untrusted
  document contains.
- Default limits bound input bytes, nesting depth, produced nodes, document
  count, aliases, and other expansion work. Limits apply while reading, not
  after materializing an unbounded tree. Depth and node budgets apply per
  document; byte and document budgets apply to the complete stream. Errors
  identify the format and the original byte position when the parser provides
  it. Recursive implementations also publish and enforce a conservative hard
  parser depth; a caller-supplied `Limits` value must never be able to turn
  nesting into stack exhaustion.
- Multi-document streaming iterators consume one JSON Lines row/YAML document
  at a time and surface an error at the failing item; streaming writers encode
  directly to the caller's sink with backpressure. A single-document async
  convenience may use one explicitly bounded byte buffer when the language
  runtime cannot bridge its async reader into Rust `Read`; document that
  distinction and never describe the bounded single-document helper as
  incremental parsing.
- Format inference is deterministic. Real path suffixes select JSON, JSON
  Lines, YAML, or TOML. Bindings treat a path-like object as a path, byte-like input
  as content, and a string as a path only when it names an existing file;
  otherwise the string is UTF-8 document content. An explicit format wins.
- Content inference uses core `text::from_{str,slice}_inferred` so a successful
  document is not parsed twice. JSON wins, empty/comment-only input retains its
  YAML interpretation, a complete nonempty TOML document wins next, and all
  remaining input is YAML. JSON Lines is never content-inferred. Use
  `text::infer_format(&[u8])` only when the decoded `Value` is not needed.
  Empty/comment-only inference returns `Format::Yaml`; the combined inferred
  decode then preserves YAML's existing zero-document error.
- Benchmark slice parsing, reader streaming, vector emission, writer emission,
  enveloped/exotic values, wide mappings, and deep structures. Retain a baseline
  for allocation-sensitive hot paths and report throughput rather than calling
  unmeasured code optimized.

## Arrow and allocation contract

- Represent every `DataType` variant with one sealed, zero-sized marker and a
  discoverable `*Field` alias over `TypedField<K>`. `TypedField<K>` contains
  exactly one generic `Field`; `TypedFieldRef<'_, K>` contains exactly one
  borrowed Field pointer. Never duplicate datatype parameters, metadata,
  caches, or child state in the typed layer. Generic-to-typed owned and
  borrowed conversions validate the complete Field before proving the marker.
  Do not expose `DerefMut`, `as_field_mut`, or another unchecked route that can
  replace the datatype behind `K`; marker-safe mutation must retain the same
  variant and leave the value unchanged on error.
- Preserve lossless Arrow schema parity. Validate at construction/import and
  at a cold projection boundary; do not validate again per record or cache hit.
- Project Arrow C Data Interface schemas only through
  `DataType::to_arrow_ffi` and `Field::to_arrow_ffi`. These core methods own
  recursive child, dictionary, nullability, ordering, metadata, and datatype
  flag repair for the pinned Arrow version. Bindings must import that schema
  directly and must not maintain another recursive FFI-schema builder.
- Import both Arrow unit enums into the unified `TimeUnit` without failure.
  Export through the matching fallible category conversion and return an error
  for temporal-to-interval or interval-to-temporal requests; never coerce.
- Borrowed `to_arrow*` methods may clone shared references; consuming
  `into_arrow*` methods should move strings, metadata, and uniquely owned child
  state when Arrow permits it.
- Cache complete Arrow field projections. No-op mutations retain the cache;
  effective mutations invalidate exactly once. Cache state never affects value
  traits or serialized output.
- Treat Arrow dictionary ID and ordering flags as owned Field value state until
  the pinned Arrow version removes them; never rely on a cached foreign Field
  to preserve state that the core model cannot rebuild.
- Core scalar `DataType` construction, getters, metadata lookup, iteration
  setup, and cloning shared nested values must not allocate. Constructing a
  foreign runtime object such as `pyarrow.Scalar` is a projection and is not
  covered by that claim. Empty collections have no per-value backing
  allocation. Bulk metadata updates validate once, then mutate a unique map or
  perform one copy-on-write detachment. Runtime adapters must first accumulate
  a wide metadata overlay in one ordered or hashed map (last write wins), then
  cross the core mutation boundary once; never resolve duplicate keys with a
  repeated linear scan.
- Do not build per-record maps or schemas. Measure before claiming an
  optimization and keep Criterion out of production dependency graphs.
- Canonical defaults are `DataType::default_value` and
  `Field::default_value`. A datatype prefers a present zero/empty value;
  `Null` and transparent logical wrappers with only a null default are the
  intrinsic exceptions. A nullable Field prefers logical null, including the
  required physical Union tag or RunEndEncoded values layout.
  Struct and fixed-list datatype defaults delegate each child slot to its
  Field default. Preflight recursion, node expansion, and byte allocation
  before materializing; caller-built public enum variants must not bypass
  those caps.
- Schema compatibility targets are the shared `Scheme` values
  (`arrow`, `spark`, `polars`, `pandas`, `iceberg`);
  `Scheme::COMPATIBILITY_TARGETS` is the list. Arrow is a validated cache-preserving no-op, and every other target
  runs the one generic recursive walker with a per-target scalar matrix. A
  rewrite must preserve Field name/nullability/metadata, invalidate a populated
  Arrow cache exactly once, and reject extension storage rather than relabeling
  it. Never fork a per-target walker.
- A root `Field` validates once (`validate_struct_root`), caches its Arrow
  projection, and stays cheap to clone; a row is one canonical ordered
  `Value::Sequence` validated against it with `validate_value` and normalized
  with `canonicalize_value`. Named materialization uses the field's own index
  (`index_of`, `get_field_by_name`) and rejects missing, unknown, duplicate, or
  non-string keys before committing.
- Arrow runtime materialization belongs in `yggdryl::arrow`. `StructScalar`
  pairs the exact root Field with a real one-row StructArray and
  exposes zero-copy indexed and named child slices. Batch and IPC readers
  validate a stream schema once, decode rows lazily, retain at most one batch,
  and stop after the first failing row. Conversion is exhaustive and
  schema-directed; never use JSON as an Arrow value bridge.
- `yggdryl::arrow::ArrowScalar` owns an exact Field and one immutable one-row
  `ArrayRef`. `from_parts` validates foreign arrays and `from_value` validates
  caller values. The `DefaultArrowScalar` extension trait is implemented for
  `DataType` and `Field` and must use the bounded core default planner without
  a binding-side placeholder table or redundant trusted-value validation.
- Keep recursive casting in `yggdryl::cast`. `ArrowCast` owns schema-directed
  array and RecordBatch casts for core `DataType` and `Field`; a typed field
  additionally casts to its own array type
  (`Int64Field::cast_arrow_array -> Int64Array`) through
  `field::ArrowFieldType`, so per-datatype logic stays with the datatype. Keep
  the runtime behind the default-enabled `arrow` feature and never duplicate
  its recursive cast table in either binding. Struct reconciliation is
  ASCII-case-insensitive, rejects ambiguous folded names, follows target
  order, drops extra columns, and fills missing nullable/required columns with
  null/canonical defaults respectively. Recurse before an outer layout cast so
  nested Struct names are never matched positionally. Exact inputs must still
  pass logical-null and Map validation, then retain their owned arrays/batch.
- Propagate wrapper exposure when validating or casting nested Arrow values.
  Null Struct/list/map parents, inactive union members, unreferenced
  dictionary values, and unused run-end values must not make hidden child
  nulls or conversion failures observable. Physically required hidden slots
  use schema-valid placeholders, never an invented logical default.
- Preflight every newly materialized Arrow array before allocation with the
  shared one-million-slot and 64-MiB fixed-buffer budgets. Missing columns are
  charged in one aggregate plan, repeated Dictionary defaults retain one
  vocabulary value, and nullable variable-list/map/dictionary columns do not
  charge child payloads that `new_null_array` never creates. Exact borrowed
  arrays are not charged as new allocations.
- `MediaDescriptor` owns one URL, a media type, the exact non-null Struct root
  Field, and the cached Arrow Schema. `ArrowTable` owns the batch protocol
  directly - validated cursor reads, a non-consuming snapshot, atomic
  overwrite-all, lazy batch and record readers, and an eager immutable
  `ArrowDataset` - as inherent methods. There is no media trait layered over
  `IOBase`: an encoded resource is read and written through `IOBase`'s record
  methods or through `Media`. Do not retain a parallel `write` alias:
  whole-resource replacement is spelled `overwrite` in every runtime.
- Append, positional upsert, and indexed setter operations update the
  shallow-cloned `RecordBatch` vector transactionally: an error leaves the
  table exactly as it was. Batch upsert replaces an
  existing index, appends only at `index == len`, and rejects gaps; it never
  invents row primary-key or conflict semantics. `BatchSelector` accepts an
  index or a canonical string/path/URL location. An exact descriptor location
  selects the whole resource, while the same URL with a numeric fragment
  selects one batch; a mismatched location fails before backend I/O. Indexed
  setters replace an existing batch, and an omitted/whole-resource setter
  overwrites all. Cursor reads never define snapshot contents, and successful
  mutation resets an in-memory reader cursor.
- `ArrowTable` is the growable in-memory table. It owns a `Vec<RecordBatch>`,
  shallow-clones buffers for snapshots and indexed reads, implements direct
  replace/append/overwrite without a storage adapter, and remains subject to
  the same descriptor, cast, logical-value, row-count, slot, and fixed-buffer
  bounds as every other reader. Its `IOBase` byte view is the Arrow IPC
  encoding of its batches, produced on demand and dropped on every mutation. `ArrowDataset` remains
  the immutable validated holder; do not conflate it with `ArrowTable`.
- A read target is an optional non-null Struct Field. No target means validate
  and preserve the declared source layout; a target builds one reusable
  recursive cast plan. Schema discovery for a not-yet-initialized sink belongs at the
  lower optional-target cast boundary, not in a dishonest optional `field()`
  accessor. `safe` follows Arrow's cast-failure policy and never bypasses
  physical validity, logical nullability, Map invariants, or descriptor checks.
- Record adapters must bulk-materialize and consume through the existing
  native Field-directed Arrow paths. Keep readers batch- and row-lazy,
  retain at most one source batch for row iteration, reuse one cast plan per
  fixed input schema, and do not allocate row maps or use JSON as an Arrow
  bridge. Python exposes PyArrow holders; JavaScript uses its standard copied
  IPC boundary and must not claim zero-copy interop.
- Arrow IPC dictionary IDs are transport-local. `yggdryl::arrow` preserves native
  Field IDs through one reserved, versioned root-Schema metadata sidecar keyed
  by logical Field path, accepting duplicate canonical IDs and removing the
  sidecar on import. Explicit reads without it keep caller IDs authoritative
  while remaining strict about ordering, key/value layout, nullability, and
  extension identity. Reject a direct Dictionary-of-Dictionary before IPC
  output because Arrow IPC 59 cannot represent the inner encoding.

## Resource identifier contract

- Parse URI syntax once in Rust. Bindings must never split schemes,
  authorities, paths, queries, fragments, URN namespaces, or suffixes.
- Canonical output lowercases schemes and uses forward slashes for file paths.
  Windows drive paths normalize to `file:///C:/...`; UNC paths normalize to
  `file://server/share/...`. The result must be independent of the host OS.
- Validate schemes, authority delimiters, percent escapes, URN namespace IDs,
  and namespace-specific strings at construction. Errors report the original
  byte offset and identifier context.
- Path-segment, file-name, and extension lookup borrows the canonical path.
  Cloning compact identifiers and reading components must not allocate.
- Compound media inference walks filename suffixes from right to left, then
  reports encodings in application order. Unknown or missing base suffixes
  use `application/octet-stream`; archive formats are base representations,
  not transparent content encodings. URI-family bindings call these native
  operations and never split or rewrite filename strings themselves.
- Filename/stem/extension changes validate a complete replacement path before
  committing. Preserve scheme, authority, query, fragment, leading path
  syntax, URL constraints, and URN namespace constraints; invalid input must
  leave the original identifier unchanged.
- File URI/path conversion must preserve every accepted component. Reject
  queries, fragments, unsafe decoded controls, encoded separators, percent
  escapes that create a Windows drive prefix, and escaped authority syntax
  that a decoded UNC path would reinterpret. UTF-8, spaces, tabs, and literal
  percent signs in UNC servers must round-trip through `from_path`.
- `Display` is canonical and losslessly parseable for all identifier values.
  Serde uses structural objects rather than a platform path or debug format.

## Binding boundary contract

These rules apply to both the Python and JavaScript extensions.

- **Rust proves a feature before any binding exposes it.** A feature is
  validated fully in Rust first - implementation, edge-case tests, and
  documentation - and only then implemented in the Python and JavaScript
  extensions. Never build a binding for a core surface that is not already
  proven. A binding written against an unsettled core encodes decisions the
  core has not made yet, so it has to be rewritten when the core settles, and
  its tests pin behavior the core never agreed to.
- **Parity is the goal.** Every core module a caller can reach from Rust should
  be reachable from Python and JavaScript: the enums, the value tree, the
  compression codings, the storage handles, and the record media. A module that
  is Rust-only is a gap to close, not a boundary to defend, and its
  documentation page says so with the `Rust only` note until it closes.
- **Cross-language interop goes through Arrow.** When two runtimes exchange
  columnar data, they exchange Arrow: PyArrow arrays, batches, and schemas on
  the Python side, Apache Arrow JS on the JavaScript side, and the C Data
  Interface or IPC in between. Do not invent a second wire format, and do not
  claim zero-copy where the binding copies.
- **Values cross through one canonical spelling.** A temporal crosses as the
  `Value` variant that names an Arrow datatype - `Timestamp`, `Date`, `Time`,
  `Duration` - so a value written in one runtime reads back as the native
  temporal type in the others. There is no language-specific fallback spelling:
  a runtime value with no canonical `Value` variant crosses as the plain shape
  it has, never as a name a document could carry.
- **Each binding exposes the conversion pair explicitly.** Python has
  `as_py`/`from_py` and JavaScript has `asJs`/`fromJs`, and every `load`/`dump`
  entry point routes through them rather than reimplementing conversion inline.
- **Infer at the boundary, compute in Rust.** A binding may look at what it was
  handed and pick the right core call; it may never reimplement the operation.
  Provide the generic entry points a dynamic language expects - `from_arrow`,
  `from_str`, `from_dict`, `from_path`, and a single `from_`/`from` that
  inspects its argument - and have each redirect immediately to the optimized
  core method for that input.
- **Coerce convenient argument types.** Anywhere the core takes a native value,
  the binding also accepts the obvious spelling of it and converts once at the
  boundary: a `str` where a `MediaType`, `MimeType`, `Url`, `Codec`, `IOKind`,
  or `DataType` is expected; a path-like where a `Url` is expected; a mapping
  where `Metadata` is expected; a PyArrow or Arrow-JS value where an Arrow value
  is expected. Conversion happens through the core `from_*` method, never by
  stringifying an arbitrary object.
- **Stay idiomatic.** Python uses its protocols - mapping dunders, `len`,
  iteration, `in`, context managers for scoped handles, keyword arguments with
  real defaults. JavaScript uses its own - `Map` protocols, iterables, spread,
  and the generic `from` constructors. The same operation, not the same syntax.
- Keep the argument name and meaning identical across languages. A parameter
  called `media_type` accepts the same set of spellings everywhere.
- **Every binding feature carries its own tests and benchmarks.** A capability
  added to a binding is not done until it has edge-case tests in that language's
  suite and a benchmark measuring the boundary it crosses.
- Surface the native error message unchanged and map variants to idiomatic
  exception types. Never rewrite, re-prefix, or re-translate a core message.
- Bind scope constructs (`with`, `using`, `Symbol.dispose`) to `IOBase::open`
  and `IOBase::close` rather than inventing a binding-side cache.

## Python extension

- `IOBase` and `Url` are `pathlib`-shaped. Anything `pathlib.Path` or
  `PurePath` answers, they answer under the same name, backed by the core:
  `name`, `stem`, `suffix`, `suffixes`, `parts`, `parent`, `parents`,
  `joinpath`, `/`, `with_name`, `with_stem`, `with_suffix`, `match`,
  `relative_to`, `is_relative_to`, `exists`, `is_dir`, `is_file`, `iterdir`,
  `glob`, `rglob`, `read_bytes`, `read_text`, `write_bytes`, `write_text`,
  `mkdir`, `touch`, `unlink`. There are no modes and no cursor - the core is
  fully random-access - and failures raise what `pathlib` raises for the same
  mistake. Never reimplement a rule in Python that the core already decides.
- Infer inputs at the boundary: accept the native wrapper, strings, PyArrow
  schema values, and Python type/typing annotations where meaningful, then
  redirect immediately to core `from_*` methods or `from_pyhint`. A Python
  string instance remains a datatype expression; the Python `str` type infers
  native UTF-8. Never stringify arbitrary objects as an inference fallback.
- Expose `DataType.decimal(precision, scale=0)` as a thin call to the core
  selector. Accept exact integer-like arguments and base-10 integer strings,
  reject booleans and floats, and do not implement decimal-width rules again
  in the binding.
- Provide idiomatic `__str__`, `__repr__`, rich comparison, `__hash__`, pickle,
  and JSON behavior. `DataType` acts like a read-only child collection;
  `Field` exposes metadata through mapping dunders (`len`, iteration,
  containment, get/set/delete) and `get`, `keys`, `values`, `items`, `update`.
- Keep conversion work in Rust, use the Arrow C Data Interface, and never
  duplicate recursive parsing, validation, comparison, or hashing in Python.
- Python Arrow scalar construction is exactly
  `DataType.arrow_scalar(value, *, safe=True)` and
  `Field.arrow_scalar(value, *, safe=True)`. Both return a `pyarrow.Scalar` of
  the projected physical datatype. Preserve an exact-type input Scalar by
  identity; cast a mismatched Scalar with PyArrow's matching `safe` policy.
  For non-Scalar input, `safe=True` uses typed scalar construction and
  `safe=False` infers once, reusing an exact inferred Scalar or applying one
  unsafe cast; when standalone inference cannot represent a target-shaped
  value such as map pairs, fall back to typed construction. A bare `DataType`
  permits a typed null, while a non-nullable `Field` rejects Python `None` and
  every resulting top-level null Scalar. Nested child nullability belongs to
  record/schema builders. This is a Python runtime adapter over the core Arrow
  module, not a second scalar implementation. Both packages delegate real
  StructArray, RecordBatch, and IPC materialization to `yggdryl::arrow`; neither
  may maintain a second binding-only schema or value model.
- Mirror identifier components as read-only properties and path segments as a
  read-only sequence. Python path-like/string inference must immediately call
  the corresponding Rust `from_*` method. `MimeType` is an immutable, hashable
  native value; `MediaType` is a native copy-on-write value whose explicit
  mutations make its Python wrapper unhashable. Both are shared by identifier
  and Field APIs. Python must not infer suffixes, content codings, or media
  categories in Python code.
- Keep `yggdryl.json`, `yggdryl.toml`, and `yggdryl.yaml` as thin byte-oriented
  facades over the native codec adapter. Recursive conversion between Python
  objects and core `Value` happens in Rust; Python orchestration may resolve I/O, record targets,
  and explicit registries but must not serialize through `dict -> str -> Rust`.
- Support Python scalar, bytes-like, temporal, decimal, UUID, enum, path,
  collection, mapping, dataclass, and Yggdryl record values by converting each
  into the `Value` variant that holds its parts. An object with no such variant
  crosses as the plain shape it has and never as a name: an integer wider than
  the native 128-bit range keeps its magnitude as decimal text and loses its
  type, because the magnitude is the part a value can hold.
- Records expose exact `from_json`, `from_toml`, `from_yaml`, `from_`,
  `into_json`, `into_toml`, `into_yaml`, and `into_` methods. They delegate
  decoded mappings to the same safe conversion/error policy as `from_dict`
  and delegate output to `to_dict`;
  do not fork a second record caster. Encoders return bytes when no destination
  is supplied and never close caller-owned streams.
- **`pyarrow.RecordBatchReader` is the record shape in Python.** Every record
  read returns one and every record write consumes one, across the Arrow C
  Stream interface and nothing else, so neither side copies or rebuilds a batch.
  A write accepts anything PyArrow exports a stream from - a reader, a `Table`, a
  `RecordBatch`, any `__arrow_c_stream__` exporter - plus a sequence of batches,
  because that is what a caller who built rows batch by batch is holding. Never
  add a row-level read or write, and never return a `Table` where the core
  returns a reader: a resource larger than memory must stay readable from Python.
- The record methods keep the core's names and argument order exactly -
  `record_options`, `read_arrow_field`, `read_arrow_batch_reader`,
  `write_arrow_batch_reader`, and `append_arrow_batch_reader`. There is no read
  target argument: the schema lives on the options, because it selects and casts
  in one pass. `options` is the one keyword argument, it accepts the settings
  value or anything that names an encoding, and omitting it derives the encoding
  from the handle's media type. `IOBase.media_type` is settable so an in-memory
  handle can say what it holds, and `with` binds `open`/`close`, which is what
  publishes a written file at its exact length for another reader.
- `RecordOptions` is the core settings value, never a Python model of one. A
  setting one encoding has reads as `None` on an encoding that has none rather
  than being invented, and setting it there raises naming the encoding that was
  found. A foreign codec name crosses as the text that format's own parser
  accepts, never as its `Display` when the two disagree.
- **A table format is a module, not a pile of top-level names.**
  `yggdryl::iceberg` is `yggdryl.iceberg`, holding `Table`, `Catalog`,
  `Compaction`, `SchemaUpdate`, `PartitionField`, `PartitionSpec`, `Snapshot`,
  `ManifestFile`, `DataFile`, `assign_field_ids`, `can_promote`,
  `schema_from_json`, and `schema_to_json` and nothing else. A table is built
  from an `IOBase` handle and from nothing else, a scan is a
  `pyarrow.RecordBatchReader`, and the metadata values are read-only views of
  the core structs that only a commit can produce. Both bindings take the same
  arguments in the same order - a spec or the column names, then the format
  version - so a table written from one language reads the same call from the
  other.
- **The bindings commit granularly, never through a closure.** Core
  `commit_changes` takes a function; across FFI the same intent is
  `update_properties(updates, removes)` (one commit, nothing when both are
  empty) and `update_schema()` - a builder that records `add_column`,
  `drop_column`, `rename_column`, `update_doc`, `make_nullable`, and
  `update_type` calls, then `commit` replays them onto a fresh core
  `SchemaUpdate`, adds and selects the schema, and writes one document. In
  Python the builder is a context manager that commits on clean exit and
  discards on exception; a spent builder refuses further use. Time travel is
  `scan_at(snapshot_id, filters, schema)`, refs resolve with
  `snapshot_by_ref`, compaction is `compact()` returning the counts, and the
  inspection tables come back as the language's record-reader shape under the
  same column names as the core.
- **The catalog crosses with its inference.** `Catalog(warehouse)` accepts a
  handle or anything that names a folder; `create_table` accepts a native
  Field, an expression, an Arrow schema, or an iterable of Fields;
  `append`/`overwrite` accept exactly what `Table.append` accepts and return
  the table. Names stay dotted in both languages.

### Python records and annotations

- Keep annotation inference and record conversion below
  `rust/python/yggdryl/records/`. It may inspect Python typing
  objects, but must construct and expose the native `DataType` and `Field`
  wrappers; never create a parallel schema representation. Nested list, map,
  union, tuple/items-view, dataclass, and record hints must use native datatype
  and field builders directly. Do not create PyArrow types merely to import
  them back into Yggdryl during annotation inference.
- Expose annotation entry points as `DataType.from_pyhint` and
  `Field.from_pyhint`. `Optional[T]`, `T | None`, or a union containing `None`
  supplies the default Field nullability; an `Annotated` `nullable` option
  explicitly overrides that default and governs safe record input/output at
  every nested Field boundary. A default value of `None` alone never changes
  schema nullability and must still satisfy the cached Field when selected.
- Annotation Field options are exactly `arrow_type`, `nullable`, `metadata`,
  `id`, `dictionary_id`, and `dictionary_is_ordered`, accepted as `(key,
  value)` extras or entries in an options mapping. Reserved-leading tuples
  must contain exactly two items. Resolve structural options left-to-right,
  validate only the final winner, merge metadata entry-wise in the same
  order, then overlay explicit caller/dataclass metadata. Each mapping that
  mentions a dictionary option must contain both dictionary keys; tuple
  extras may supply the pair separately. A sole physical member promoted
  through Optional, NewType, TypeVar, or an alias supplies the Field baseline;
  outer options overlay it. A parent `arrow_type` instead owns and shadows its
  complete physical subtree.
- `arrow_type` accepts only an actual PyArrow datatype. Preserve an explicit
  ExtensionType through one native Field import, reject conflicting
  `ARROW:extension:*` overlays, and require its serialized extension metadata
  to be UTF-8. Bare `DataType.from_pyhint` applies only `arrow_type`, rejects
  ExtensionType (directing callers to Field/records), and rejects all
  recognized Field-only options; legacy all-string metadata-only mappings
  remain ignored there.
- `@record` must produce a genuine standard-library dataclass. Cache one tuple
  of native child Fields and one native root Field per decorated class. Never
  rebuild a schema per instance or conversion. Persist `python.module`,
  `python.class`, `python.qualname`, and `python.kind` as Field metadata so
  Arrow remains the central interchange representation. Freeze published
  cached Fields: metadata mutation must raise instead of allowing a child
  singleton to diverge from its enclosing root Struct projection.
- The records module should re-export the useful standard `dataclasses`
  surface. Its `to_dict` and `from_dict` functions must support both Yggdryl
  records and ordinary dataclass types. Generated record methods delegate to
  those functions rather than duplicating conversion logic.
- `safe=False` is the explicit shallow fast path. `safe=True` recursively
  validates and casts annotations with path-aware errors. Boolean casting is
  exact and must reject ambiguous truthiness. It enforces fixed-size-list
  arity and rejects temporal values that Arrow would truncate at the target
  unit. A naive datetime is interpreted as UTC only for an exact `UTC`
  timestamp target; other zoned timestamps require aware datetimes,
  timezone-less timestamps require naive datetimes, and Arrow time values
  reject aware times. The only error policies are
  `errors="raise"` and `errors="default"`; the latter uses a declared default
  or default factory and still raises when neither exists.
- Resolve inherited and forward annotations once per class. Detect recursive
  type graphs, preserve declaration order, avoid shared mutable defaults, and
  benchmark cached schema access plus safe and shallow conversion separately.
- Never retain an entire decorator frame: pending records keep only
  annotation-reachable bindings, and completed schemas keep resolved nested
  hint maps. Parameterized aliases use a per-conversion binding context; do
  not publish a specialization as the singleton schema of its generic origin.
- Safe output validates existing dataclass instances without reconstructing
  them or invoking `__init__`/`__post_init__`. Thread declared generic hints
  through nested collection/export traversal instead of using runtime
  re-instantiation as validation.
- Arrow-to-record class factories are exactly
  `Record.from_arrow_field(field, *, class_name=None, module=None)` and
  `Record.from_arrow_schema(schema, *, class_name=None, module=None)`. Import
  every Arrow field through the native
  `Field.from_arrow` boundary once, cache those exact native Fields on the
  generated dataclass, and derive Python casting annotations from the cached
  `Field`/`DataType` graph. The derived annotations are a language view only;
  never regenerate an imported schema through `from_pyhint`, because doing so
  can erase integer widths, nested layout, dictionary state, or metadata.
  Assemble every record root through native `DataType.from_fields`; never
  round-trip child Fields through `pa.struct` merely to rebuild a native
  datatype and never keep a second Arrow-to-Python type table.
  A struct root contributes its children and a scalar root becomes one column.
  Require unique Arrow column names that are already valid non-keyword Python
  identifiers; never silently rename the physical schema. Explicit
  `class_name`/`module` win, then valid `python.class`/`python.module` metadata,
  then the documented root-name/`ArrowRecord` and `__main__` fallbacks. Replace
  only the four generated Python identity keys and retain other imported root
  and child metadata. Preserve Arrow Schema metadata on the generated root and
  project it back through `into_arrow_schema`; accept UTF-8 byte key/value
  pairs and reject non-UTF-8 metadata explicitly instead of decoding lossily.
  The reserved `yggdryl:ipc:dictionary-ids` Schema key is transport state, not
  imported root metadata: route whole-Schema import and transport projection
  through `yggdryl::arrow`, restore nested canonical IDs once, and keep the key
  out of the native root and public `into_arrow_schema` projection. Collection
  Arrow outputs may use one separately cached transport Schema so subsequent
  IPC retains those IDs; never parse or rebuild this sidecar in Python.
- Record collection imports are exactly `from_dicts`,
  `from_arrow_record_batch`, `from_arrow_record_batch_reader`,
  `from_arrow_table`, and `from_arrow`. They return lazy iterators and
  reuse the internal `from_dict` caster with the same `safe` and `errors`
  contract, without rebuilding schemas or materializing a new dictionary per
  Arrow row solely as conversion glue. Generic
  `from_arrow` accepts a PyArrow RecordBatch, Table, RecordBatchReader, Arrow C
  stream exporter through `__arrow_c_stream__`, or iterable of RecordBatch
  values; never invoke an arbitrary `to_arrow` method as inference. Validate a
  physical schema at its batch/source boundary, never per row; compatibility
  compares names, order, datatypes, and nullability recursively while
  deliberately ignoring transport metadata.
  When schema validation is disabled but safe input needs a physical mismatch
  cast, cache the projected target Arrow type once and call `Scalar.cast`
  directly; calling `Field.arrow_scalar` there would reproject a C Field per
  mismatched cell. On output, normalize only an explicit Scalar whose type
  differs from the cached target through `Field.arrow_scalar`; exact Scalars
  and ordinary values remain on the bulk path. Use the same helper for
  one-cell error localization after a bulk Arrow array builder fails. Record
  output `safe=False` skips annotation validation only; it must still enforce Arrow
  physical validity and must never enable unsafe overflow casts.
  Keep the Python package floor at PyArrow 18 for C-stream import. 15 supplied
  the generic C-stream reader, but the run-end-encoded map and extension-type
  scalar paths this adapter relies on were only correct from 18. Do not use
  newer `maps_as_pydicts` conveniences: normalize Arrow map pair sequences in
  the adapter and reject duplicate keys before the shared mapping caster.
- Record Arrow exports are exactly `into_arrow_field`, `into_arrow_schema`,
  `into_arrow_record_batch`, `into_arrow_record_batches`,
  `into_arrow_table`, and `into_arrow_record_batch_reader`. The collection
  forms are classmethods over record iterables; `into_arrow_record_batches`
  and `into_arrow_record_batch_reader` use a positive `batch_size` that
  defaults to 65,536. Batch iteration stays lazy and bounded, and empty eager
  outputs use the class's cached schema. Reuse
  the internal `to_dict` projection and its `safe` behavior rather than
  maintaining a second row exporter, but append projected values directly to
  Arrow columns instead of allocating a temporary row map. Resolve and
  validate the cached native schema once, not once per row or output batch.
  Output iterables accept instances of the receiving record class only and
  expose `safe` but not `errors`; mapping rows must first pass explicitly
  through `from_dicts`, where defaults and failure policy belong.
- Arrow/tabular regressions must cover empty and one-shot iterables, failing
  rows after a successful batch, schema metadata differences, incompatible
  nested schemas, nullability, dictionary and exact numeric widths, deep
  structs/lists/maps, temporal and decimal values, and safe versus shallow
  conversion. Benchmarks separate class materialization, schema validation,
  row casting, batch/table construction, and bounded reader iteration; prepare
  fixtures outside measured loops and do not claim allocation improvements
  from timings alone.

## JavaScript extension

- Accept native wrappers and strings through small inferred factories, then
  delegate to core. Use camelCase only at the JavaScript boundary while mapping
  directly to the Rust vocabulary.
- Expose `DataType.fromFields(iterable)` as the public native Struct assembly
  boundary. Typed-field factories may retain private native constructor
  handles inside the loader, but those handles must not remain on the public
  runtime class or in published TypeScript declarations.
- Provide `toString`, `toJSON`, equality, comparison, stable hashing, cloning,
  child access/iteration, and Map-like field metadata operations (`size`,
  `get`, `set`, `delete`, `has`, `keys`, `values`, `entries`, `update`).
- Implement convenience protocols in the JavaScript loader when Node-API
  cannot expose a language symbol cleanly; the native module remains the source
  of values and validation.
- JavaScript record helpers close over one native struct `Field`, define
  collision-safe getters once, and materializes rows through direct
  schema-guided Node-API conversion. Nested Struct values remain native
  Records and reuse cached child layouts; never build a per-row schema, name
  map, JSON object bridge, or parallel JavaScript value model.
- Apache Arrow JS interop uses the standard copied IPC boundary because Arrow
  JS has no C Data consumer. Parse and validate the native schema before
  consuming a one-shot output iterable, cache the resulting Arrow JS Schema,
  validate input source schemas once, and keep IPC row cursors bounded and
  lazy. Public Arrow JS objects expose transport-local dictionary IDs; the
  native Record and reserved IPC sidecar retain canonical Field IDs exactly.
- **`BatchReader` is the record shape in JavaScript too.** A read returns one, a
  write consumes one, and it is one-shot: reading it or handing it to a write
  consumes it, and a second consumer is told so rather than seeing an empty
  stream. One batch crosses as one self-contained Arrow IPC stream, so its
  schema travels with it; say that per-batch header is what a copied boundary
  costs rather than implying a shared handshake. `BatchReader.from` is the one
  inference point - another reader, an Arrow JS `Table` or `RecordBatch`, an
  array of batches, or Arrow IPC bytes - and `toIpc`/`toTable` are the two ways
  to drain one. Never add a row-level read or write.
- Record and table calls take the native settings value or anything that names
  an encoding, and the encoding is never an argument: `recordOptions()` derives
  it from the handle's media type, and `mediaType` is settable so an in-memory
  handle can say what it holds. A setting one encoding has reads as `null` on
  an encoding that has none rather than being invented, and a foreign codec name
  crosses as the text that format's own parser accepts, never as its `Display`
  when the two disagree.
- **A table format is a namespace, not a pile of top-level classes.**
  `yggdryl::iceberg` is `iceberg` in the loader, holding `Table`, `Catalog`,
  `PartitionSpec`, `DataFile`, `assignFieldIds`, `canPromote`,
  `schemaFromJson`, and `schemaToJson` and nothing else, and those names
  appear nowhere else on the package. The schema-update builder is reached
  only through `table.updateSchema()`, and a compaction report is a plain
  object, because it records what happened rather than carrying behavior. A snapshot and a manifest arrive as plain objects, because they
  record what happened rather than carry behavior; a 64-bit identifier crosses
  as a `bigint`, because a snapshot id past 2^53 is exact and a number is not.
  Both bindings take the same arguments in the same order - a spec or the
  column names, then the format version - so a table written from one language
  reads the same call from the other.
- Expose identifier components as read-only properties and path segments as an
  iterable array/view. Windows normalization, suffix mutation, MIME/media
  inference, and preferred-extension selection remain core-only. Export the
  same native `MimeType` and `MediaType` objects used by Field metadata rather
  than JavaScript string-union lookalikes or a second lookup table.
- Keep JavaScript JSON/TOML/YAML facades byte-first (`Buffer`/typed bytes) and route
  recursive object conversion through native `Value`. Support exact `bigint`,
  byte, Date, Array, plain object, Map, and Set semantics; a class is
  constructed only from a target the caller passed in, never from a name a
  document carries. Generic operations follow core vocabulary in camel case and
  infer only from real path suffixes when no format is given.
- When Node-API has already decoded a structured JSON value, pass the
  `serde_json::Value` directly through the core type's Serde implementation.
  Never serialize it to a temporary string and invoke a second parser; retain
  the same core validation and error mapping on the direct path.
- Before a Node-API `serde_json::Value` argument sees caller-owned JavaScript,
  build one iterative bounded plain-data snapshot. Reject cycles, proxies,
  accessors, symbols, and over-limit depth/nodes before NAPI's recursive value
  conversion, then pass only the detached snapshot; a validate-then-revisit
  boundary is vulnerable to getters and proxy time-of-check/time-of-use changes.
- Reserve `javascript:builtins.<Name>` for genuine JavaScript built-ins,
  `javascript:<yggdrylType-or-name>` for application classes, and `yggdryl:<Name>`
  for native wrappers. Detect built-ins and wrappers by native identity, never
  by `constructor.name`; reject application identities that enter a reserved
  namespace.
- Keep the Node `maxDepth` default and ceiling at 48 while recursive N-API
  traversal is used. Raising it requires an iterative traversal plus an
  isolated subprocess regression proving over-limit caller data cannot abort
  Node. Generic JSON Lines `from` returns rows and `into` consumes a row
  iterable; never type it as a scalar-shaped round trip.

## Required checks

Run formatting, warning-free Clippy, workspace tests, parser/interop/text,
JSON, YAML, and TOML benchmarks, Rustdoc with warnings denied, the Rust 1.85
core check, Python native and codec tests, Node native/codec/type tests, and
`python -m mkdocs build --strict` before handoff.
Run the test and Clippy passes twice: once with default features and once with
`--features "parquet iceberg"`, because the Parquet encoding and the table
format over it are behind non-default features and are otherwise never
compiled. Both extensions build the core with `arrow`, `parquet`, and
`iceberg`, so `maturin develop` and `npm run build` compile that combination
too.
The Rust 1.85 check covers `yggdryl` with its default Arrow runtime and a
`--no-default-features --lib` build of the explicit runtime opt-out.
Remove generated targets, the MkDocs `site/`, virtual environments, native
binaries, caches, and `node_modules` after validation.

## Releases

- `.github/workflows/release.yml` publishes all three surfaces from one `v*`
  tag: the `yggdryl` crate to crates.io, wheels (five platforms, CPython
  3.10-3.14) plus the sdist to PyPI, and `@yggdryl/node` to npm carrying
  every platform's native module in one package - the generated loader picks
  the binary at require time. Running the workflow by hand builds and
  verifies everything without publishing; use that to rehearse.
- One version, spelled three times: the workspace `Cargo.toml`,
  `python/pyproject.toml`, and `node/package.json` must agree, and the tag
  must be `v` plus that version - the preflight job refuses anything else.
  Bump all three in the same commit; nothing else carries a version.
- Credentials are repository configuration, never workflow content: the
  `CARGO_REGISTRY_TOKEN` and `NPM_TOKEN` secrets, and PyPI trusted publishing
  (OIDC) bound to the `pypi` environment. Do not add a fourth registry or a
  stored PyPI password.
- Every built artifact is smoke tested on its own platform before anything
  publishes - a wheel by an end-to-end table round trip, a native module by
  the full Node test suite - and the npm publish refuses a package missing
  any platform binary. Keep it that way: an artifact that was never imported
  is not released.
