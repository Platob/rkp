# FIX dictionaries and structures

`rkp.fix` turns selected definitions from the OnixS FIX Dictionary into
immutable fields, components, repeating groups, and message specifications.
The FIX tag becomes the stable RKP `seq`, the canonical FIX name becomes the
field alias, and the description, type, version, enum values, and attributed
source are carried into Arrow metadata.

Components remain nested record/Arrow structs. A `NumInGroup` counter followed
by indented structure rows becomes a repeating `tuple[Entry, ...]` and an Arrow
`list<struct<...>>`. Requiredness and comments are retained at the exact
message/component position where OnixS publishes them.

## Create fields without the network

```python
from rkp.fix import FixDictionary, FixEnumValue, FixField


dictionary = FixDictionary(
    version="4.4",
    fields=(
        FixField(11, "ClOrdID", "String", "4.4"),
        FixField(
            54,
            "Side",
            "char",
            "4.4",
            description="Side of order.",
            values=(FixEnumValue("1", "Buy"), FixEnumValue("2", "Sell")),
        ),
    ),
)

NewOrder = dictionary.into_record(
    "NewOrder",
    required=("ClOrdID", "Side"),
)
order = NewOrder(cl_ord_id="client-1", side="1")

assert order.dumps_json() == '{"ClOrdID": "client-1", "Side": "1"}'
assert NewOrder.into_arrow_schema().field("Side").metadata[
    b"PARQUET:field_id"
] == b"54"
```

`FixField.into_spec(required=False)` is the lower-level projection. It returns
the Python member name, semantic annotation, and a fresh `rkp.Field`. A new
field is created on every call because dataclass processing attaches ownership
state to a field instance.

FIX types map to `str`, `int`, `Decimal`, `bool`, `bytes`, `date`, `time`, or
`datetime`. Unknown/vendor types safely remain `str`, while `fix.type` retains
their exact source spelling. Enum codes remain strings so leading zeroes,
ranges, and vendor values are never coerced away. Generic record conversion
does not implement FIX wire details such as Boolean `Y`/`N` or timestamp leap
seconds; those belong in a future FIX codec.

Run the local example:

```console
uv run --project python python docs/examples/fix_fields.py
```

## Fetch only the fields you need

```python
from pathlib import Path

from rkp.fix import FixCache, OnixsFixScraper


with FixCache(Path(".cache") / "fix.sqlite3") as cache:
    with OnixsFixScraper(cache, min_interval=0.5) as scraper:
        fix44 = scraper.dictionary(
            "4.4",
            tags=(11, 38, 44, 54, 60),
            persist_to="schemas/fix-4.4-order-fields.json.gz",
        )

    # Reproducible cache-only build; it never falls back to the network.
    with OnixsFixScraper(cache) as scraper:
        same = scraper.dictionary("4.4", (11, 38, 44, 54, 60), offline=True)
```

Importing `rkp.fix` never accesses the network. The scraper first fetches one
field index, then only selected detail pages. `scrape_all()` is an explicit
operation because the current dictionary contains thousands of fields.
Concurrent hydration is bounded, results are sorted by tag, and requests for
the same URL are coalesced.

`latest` resolves the current canonical edition once and pins that edition for
the sync:

```python
with FixCache(Path(".cache") / "fix.sqlite3") as cache:
    with OnixsFixScraper(cache) as scraper:
        current = scraper.dictionary("latest", (54, 60), workers=2)
```

Supported classic names include `4.0` through `5.0.SP2` and `FIXT1.1`;
explicit `5.0.SP2 EP…` editions and `latest` use the modern page layout.

## Fetch messages, components, and repeating groups

Selecting a message recursively follows the component links published in its
structure, then hydrates only the field tags used by that graph:

```python
from rkp.fix import OnixsFixScraper


with OnixsFixScraper(min_interval=0.5) as scraper:
    catalog = scraper.dictionary("4.4", messages=("D",))

NewOrderSingle = catalog.into_message_record("D")
schema = NewOrderSingle.into_arrow_schema()
```

`list_messages()` and `message()` expose lightweight indexes and one symbolic
message structure. Modern editions also publish `list_components()` and
`component()`; classic editions have no global component index, so components
are discovered from selected messages. URLs are always taken from source pages
rather than guessed. Component cycles, missing tags/components, malformed
indentation, and repeating counters whose FIX type is not `NumInGroup` are
rejected before a record class is generated.

Generated message records use the same `rkp.Field` definitions as every other
adapter. They therefore round-trip through JSON/YAML, Arrow batches, Spark,
Iceberg, and Glue without a parallel FIX-only schema implementation. This is a
schema projection, not a FIX tag-value wire codec; Boolean `Y`/`N`, SOH framing,
checksum calculation, and leap-second timestamp text remain application-level
wire concerns.

## Cache and portable snapshots

`FixCache` uses only the standard library. Its SQLite database stores:

- zlib-compressed source HTML with SHA-256 integrity checks, timestamps,
  `Last-Modified`, and `ETag` when supplied;
- compact normalized parser artifacts keyed by parser version and content
  digest;
- a small in-memory LRU for hot entries.

SQLite WAL, short write transactions, and per-URL single-flight locking make
the cache safe for bounded concurrent scraping. By default all normalized FIX
metadata lives under `~/.config/fix`: the SQLite cache is
`~/.config/fix/cache-v1.sqlite3` and portable snapshots are written beneath
`~/.config/fix/dictionaries/`. Set `RKP_FIX_HOME` to move the whole directory,
`XDG_CONFIG_HOME` to change the generic config root, or `RKP_FIX_CACHE` to
override only the SQLite file. `cache.info()` reports counts and
`cache.clear()` clears only that explicitly selected database.

Portable snapshots are canonical JSON or deterministic JSON.gz:

```python
from rkp.fix import load_fix_dictionary


path = fix44.persist()  # ~/.config/fix/dictionaries/fix-4-4.json.gz
restored = load_fix_dictionary(path)
assert restored == fix44
```

Writes use a same-directory temporary file, `fsync`, and atomic replacement.
Loads validate the snapshot format, size, fields, structures, component graph,
duplicate tags/names, and enum values. Snapshot v2 stores messages/components
while the loader remains compatible with fields-only v1 snapshots. No cache or
snapshot uses pickle or executable code.

## Source and responsible use

The scraper is constrained to the configured dictionary origin and path,
limits response sizes, identifies itself, supports retry/backoff, and rejects
unsafe redirects. Normal CI uses synthetic local fixtures; it never crawls the
live site.

The OnixS robots policy currently permits the dictionary path, but the pages
are marked “All Rights Reserved” and no reusable dictionary-content license is
published. RKP therefore ships no pre-scraped corpus. Keep cached pages and
derived descriptions local, retain `fix.source`/`fix.source_url` attribution,
and obtain the necessary OnixS/FIX permission before redistributing a cached
dictionary.

See the [OnixS FIX Dictionary](https://www.onixs.biz/fix-dictionary.html) and
its [robots policy](https://www.onixs.biz/robots.txt) for the current source
and crawl policy.
