# Public API

Most applications import from `rkp`. The protocol modules remain available for
specialized typing, but the root facades keep optional dependencies lazy.

## Package

`rkp.__version__` contains the installed distribution version:

```python
import rkp

print(rkp.__version__)
```

## Records

```python
record(cls=None, /, *, init=True, repr=True, eq=True, order=False,
       unsafe_hash=False, frozen=False, match_args=True, kw_only=False,
       slots=False, weakref_slot=False, alias=None, metadata=...,
       catalog_name=..., schema_name=..., table_name=...,
       with_yaml=True, with_json=True)

field(*, default=MISSING, default_factory=MISSING, init=True, repr=True,
      hash=None, compare=True, metadata=None, kw_only=MISSING,
      alias=..., type=..., nullable=..., doc=..., seq=...,
      field_id=..., iceberg_field_id=..., primary_key=...,
      partition_key=..., index_key=...)

field_options(dataclass_field)
record_metadata(record_type_or_instance)
```

- `Record.from_dict(datum, safe=True, on_error="raise")` constructs a concrete
  decorated record.
- `FieldOptions` exposes validated metadata, configuration, documentation,
  identity, nullability, alias, type, and key-role properties.
- `RecordMetadata` exposes the metadata mappings, portable name properties,
  `has(name)`, and `merged(...)`.

General conversion and introspection:

```python
dataclass_from_dict(cls, datum, safe=True, on_error="raise")
record_from_dict(cls, datum, safe=True, on_error="raise")
to_dict(datum, *, by_alias=True)
is_record(value)
is_record_type(value)
serialized_field_name(dataclass_field, annotation=None)
resolved_type_hints(cls, *, localns=None)
```

## FIX dictionaries and structures

The dependency-free `rkp.fix` namespace provides:

```python
FixEnumValue(value, description)
FixField(tag, name, fix_type, version, description="", values=(),
         source_url="", status=None)
FixField.into_spec(*, required=False, name=None)

FixFieldMember(tag, required=False, comment="")
FixComponentMember(name, required=False, comment="")
FixRepeatingGroup(tag, members, required=False, comment="")
FixComponent(name, version, members, description="", source_url="")
FixMessage(name, msg_type, version, members, description="", source_url="")

FixDictionary(version, fields, source_url=..., components=(), messages=())
FixDictionary.field(tag_or_name)
FixDictionary.component(name)
FixDictionary.message(msg_type_or_name)
FixDictionary.specs(*, required=(), fields=None)
FixDictionary.into_record(name="FixRecord", *, required=(), fields=None,
                          module=None, **record_options)
FixDictionary.into_component_record(component, *, name=None, module=None,
                                    **record_options)
FixDictionary.into_message_record(message, *, name=None, module=None,
                                  **record_options)
FixDictionary.dump(destination=None, *, compress=None)
FixDictionary.persist()
FixDictionary.load(source)
FixDictionary.load_default(version)
load_fix_dictionary(source)
load_default_fix_dictionary(version)
fix_home()
default_fix_cache_path()
default_fix_dictionary_path(version)

FixCache(path=None, *, memory_entries=128)
OnixsFixScraper(cache=None, *, base_url=..., opener=None, timeout=30,
                ttl=604800, min_interval=0.5, max_response_bytes=16777216,
                retries=2, user_agent=...)
OnixsFixScraper.list_fields(version="4.4", *, refresh=False, offline=False)
OnixsFixScraper.field(version, tag_or_name, *, refresh=False, offline=False)
OnixsFixScraper.list_messages(version="4.4", *, refresh=False, offline=False)
OnixsFixScraper.message(version, msg_type_or_name, *, refresh=False,
                        offline=False)
OnixsFixScraper.list_components(version="latest", *, refresh=False,
                                offline=False)
OnixsFixScraper.component(version, name, *, refresh=False, offline=False)
OnixsFixScraper.dictionary(version, tags=(), *, messages=(), components=(),
                           refresh=False, offline=False, persist_to=None,
                           workers=4)
OnixsFixScraper.scrape_all(version, *, refresh=False, offline=False,
                           persist_to=None, workers=4)
scrape_onixs_fields(version="4.4", *, tags=None, all=False, cache=None,
                    refresh=False, offline=False, persist_to=None, workers=4)
```

See [FIX dictionaries and structures](../fix.md) for caching, type projection, source
attribution, and responsible-use details. Importing the namespace never causes
network access.

## JSON and YAML

`rkp.json`:

```python
loads(data, *, cls=None, encoding="utf-8", safe=True,
      on_error="raise", **json_options)
load(source, *, cls=None, encoding="utf-8", safe=True,
     on_error="raise", **json_options)
dumps(datum, *, ensure_ascii=False, encoding="utf-8", **json_options)
dumps_bytes(datum, *, ensure_ascii=False, encoding="utf-8", **json_options)
dump(datum, destination, *, encoding="utf-8", ensure_ascii=False,
     **json_options)
dump_bytes(datum, destination, *, encoding="utf-8", ensure_ascii=False,
           **json_options)
```

`rkp.yaml` has the same load-side signature. Its dump side is:

```python
dumps(datum, *, encoding="utf-8", **yaml_options)
dumps_bytes(datum, *, encoding="utf-8", **yaml_options)
dump(datum, destination, *, encoding="utf-8", **yaml_options)
dump_bytes(datum, destination, *, encoding="utf-8", **yaml_options)
```

Every decorated record gains `load_json`, `loads_json`, `dump_json`, and
`dumps_json`, the corresponding YAML methods, and generic `load`, `loads`,
`dump`, and `dumps` methods accepting `format="json" | "yaml"`. Byte-oriented
serialization is available through `dump_bytes`, `dumps_bytes`,
`dump_json_bytes`, `dumps_json_bytes`, `dump_yaml_bytes`, and
`dumps_yaml_bytes`; the existing load methods accept bytes and binary streams.

## Arrow

Schema inference:

```python
into_arrow_type(annotation)
into_arrow_field(name_or_field, annotation=..., *, nullable=None, owner=None)
dataclass_into_arrow_field(dataclass_type, *, name=None, nullable=False,
                           localns=None)
into_arrow_schema(value, *, metadata=None, localns=None)
dataclass_into_arrow_schema(dataclass_type, *, metadata=None, localns=None)

schema_metadata(value)
catalog_name(value)
schema_name(value)
table_name(value)
```

Runtime conversion:

```python
records_into_arrow_batch(records, *, record_type=None, schema=None)
records_into_arrow_batches(records, *, batch_size=65_536,
                           record_type=None, schema=None)
records_into_arrow_reader(records, *, batch_size=65_536,
                          record_type=None, schema=None)
arrow_batch_into_records(record_type, batch, *, safe=True,
                         on_error="raise", validate_schema=True)
arrow_into_records(record_type, source, *, safe=True,
                   on_error="raise", validate_schema=True)
```

Generated record forms retain the class automatically:

```python
RecordType.into_arrow_field(name=None, *, nullable=False)
RecordType.into_arrow_schema()
RecordType.into_arrow_batch(records, *, schema=None)
RecordType.into_arrow_batches(records, *, batch_size=65_536, schema=None)
RecordType.into_arrow_reader(records, *, batch_size=65_536, schema=None)
RecordType.from_arrow_batch(batch, *, safe=True, on_error="raise",
                            validate_schema=True)
RecordType.from_arrow(source, *, safe=True, on_error="raise",
                      validate_schema=True)
```

## Spark

All functions below lazily require the `spark` extra:

```python
arrow_type_into_spark_type(value, *, prefer_timestamp_ntz=True)
spark_type_into_arrow_type(value, *, timezone="UTC",
                           prefers_large_types=False)
arrow_into_spark_field(field, *, prefer_timestamp_ntz=True)
spark_into_arrow_field(field, *, timezone="UTC", prefers_large_types=False)
into_spark_schema(value, *, prefer_timestamp_ntz=True)
spark_into_arrow_schema(schema, *, timezone="UTC", prefers_large_types=False,
                        metadata=None)

arrow_into_spark_dataframe(source, *, spark=None)
records_into_spark_dataframe(records, *, record_type=None, spark=None,
                             batch_size=65_536)
spark_dataframe_into_arrow(dataframe, *, metadata=None)
spark_dataframe_into_records(dataframe, record_type, *, batch_size=65_536,
                             safe=True, on_error="raise",
                             validate_schema=True)
```

Generated conveniences are `RecordType.into_spark_schema(...)`,
`RecordType.into_spark_dataframe(...)`, and `RecordType.from_spark(...)`.

## Iceberg

All functions below lazily require the `iceberg` extra:

```python
into_iceberg_field(value, annotation=..., *, name=None, nullable=None,
                   owner=None, field_id_start=1, format_version=2,
                   downcast_ns_timestamp_to_us=..., localns=None)
arrow_into_iceberg_field(field, *, field_id_start=1, format_version=2,
                         downcast_ns_timestamp_to_us=...)
dataclass_into_iceberg_field(dataclass_type, *, name=None, nullable=False,
                             field_id_start=1, format_version=2,
                             downcast_ns_timestamp_to_us=..., localns=None)

into_iceberg_schema(value, *, schema_id=None, field_id_start=1,
                    identifier_field_ids=None, format_version=2,
                    downcast_ns_timestamp_to_us=..., localns=None, owner=None)
arrow_into_iceberg_schema(schema, *, schema_id=None, field_id_start=1,
                          identifier_field_ids=None, format_version=2,
                          downcast_ns_timestamp_to_us=...)
dataclass_into_iceberg_schema(dataclass_type, *, schema_id=0,
                              field_id_start=1, identifier_field_ids=None,
                              format_version=2,
                              downcast_ns_timestamp_to_us=..., localns=None)
iceberg_fields_into_schema(*fields, schema_id=0, identifier_field_ids=None)

iceberg_into_arrow_field(field, *, include_field_id=True, primary_key=False,
                         identifier_field_ids=None)
iceberg_into_arrow_schema(schema, *, metadata=None, include_field_ids=True)

iceberg_into_avro_schema(schema, *, name=None, namespace=None, doc=None)
avro_into_iceberg_schema(schema, *, schema_id=0, field_id_start=1,
                         identifier_field_ids=None, format_version=2)
```

Generated records expose `into_iceberg_field(...)` and
`into_iceberg_schema(...)`. An attached RKP `Field` also exposes both methods
with an optional `owner=` for annotation resolution.

Catalog operations against a live PyIceberg catalog:

```python
create_iceberg_table(catalog, value, *, identifier=None, format_version=2,
                     location=None, properties=None, partition_spec=None,
                     partition_keys=None, sort_order=None, schema_id=0,
                     field_id_start=1, identifier_field_ids=None,
                     downcast_ns_timestamp_to_us=..., create_namespace=True,
                     exists_ok=True)
load_iceberg_table(catalog, value, *, identifier=None)
sync_iceberg_table_schema(table, value, *, format_version=None,
                          downcast_ns_timestamp_to_us=...)
records_into_iceberg_table(table, records, *, record_type=None,
                           batch_size=65536, mode="append",
                           snapshot_properties=None)
iceberg_table_into_arrow(table, *, row_filter=None, selected_fields=("*",),
                         limit=None, case_sensitive=True)
iceberg_table_into_records(record_type, table, *, row_filter=None, limit=None,
                           case_sensitive=True, safe=True, on_error="raise")
into_iceberg_partition_spec(value, *, schema=None, partition_keys=None,
                            spec_id=0)
into_iceberg_sort_order(value, *, schema=None, sort_keys=None, order_id=1)
```

## Avro

The `rkp.avro` package needs no optional dependency; it is backed by the Rust
core through the bundled `rkp._avro` extension module.

Schemas, values, and framing:

```python
core_version()
parse_schema(value, *, namespace=None)
schema_into_json(schema)
dumps_schema(schema, *, indent=None)
loads_schema(data)
canonical_form(schema)
fingerprint(schema)
fingerprint_bytes(schema)

encode(schema, value)
encode_into(schema, value, out)
decode(schema, data)
encode_single_object(schema, value)
decode_single_object(schema, data)
compile_encoder(schema)
compile_decoder(schema)

dumps(schema, value, **kwargs)
loads(schema, data, **kwargs)
into_json(schema, value)
out_of_json(schema, value)
```

Container files:

```python
Avro(source=None, *, mode="r", schema=None, codec="null", metadata=None,
     sync_marker=None, sync_interval=DEFAULT_SYNC_INTERVAL,
     cache_bytes=DEFAULT_CACHE_BYTES)
Avro.create(schema, destination=None, *, codec="null", metadata=None,
            sync_marker=None, sync_interval=DEFAULT_SYNC_INTERVAL)

read_container(source, *, schema=None, mode="r",
               cache_bytes=DEFAULT_CACHE_BYTES)
write_container(destination, schema, values, *, codec="null", metadata=None,
                sync_marker=None, sync_interval=DEFAULT_SYNC_INTERVAL)
dump(destination, schema, values, *, codec="null", metadata=None,
     sync_marker=None)
load(source, *, schema=None)
```

One `Avro` reads and writes the same container, addressing records by index:

```python
len(container), container[index], container[start:stop]
container[index] = value, container[start:stop] = values
del container[index], del container[start:stop]
iter(container)

container.get(index, default=None)
container.iter_from(start=0, stop=None)
container.blocks()
container.block_of(index)
container.read_block(ordinal)
container.iter_blocks()

container.append(value)
container.extend(values)
container.insert(index, value)
container.pop(index=-1)
container.clear()
container.truncate(index=None)
container.compact()

container.flush()
container.save(destination)
container.into_bytes()
container.close()
```

Its properties are `schema`, `writer_schema`, `codec`, `metadata`,
`sync_marker`, `sync_interval`, `mode`, `path`, `closed`, `writable`,
`appendable`, `dirty`, and `nbytes`. Modes are `"r"`, `"r+"`, `"a"`, and
`"w"`; slices must be contiguous.

`AvroBlock(ordinal, offset, data_offset, size, count, first)` is a
`NamedTuple` with the derived `stop` and `end` properties.

Module constants are `CODECS`, `MODES`, `MAGIC`, `SYNC_SIZE`,
`DEFAULT_SYNC_INTERVAL`, `RANDOM_SYNC_INTERVAL`, `DEFAULT_CACHE_BYTES`, and
`PRIMITIVE_NAMES`.

The schema model exposes `AvroSchema` (`type_name`, `name`, `fullname`,
`logical_type`, `attributes`, `into_json()`, `canonical_form()`,
`fingerprint()`) with `RecordSchema` (`fields`, `field(name)`, `is_error`),
`EnumSchema` (`symbols`, `default`), `FixedSchema` (`size`, `logical`,
`precision`, `scale`), `ArraySchema` (`items`), `MapSchema` (`values`),
`UnionSchema` (`options`, `is_optional`), `PrimitiveSchema` (`primitive`,
`logical`, `precision`, `scale`), the `NamedSchema` base (`declared_name`,
`namespace`, `doc`, `aliases`), and `AvroField(name, type, default, doc,
order, aliases, attributes)` with `has_default` and `into_json()`.

Failures raise `AvroError` or one of its subclasses `AvroSchemaError`,
`AvroEncodeError`, and `AvroDecodeError`.

Record and Arrow adapters are exported from `rkp`:

```python
into_avro_schema(value, *, name=None, namespace=None, doc=None,
                 flavor="standard", include_field_ids=True, localns=None)
arrow_into_avro_schema(schema, *, name=None, namespace=None, doc=None,
                       flavor="standard", include_field_ids=True)
arrow_into_avro_field(field, *, namespace=None, flavor="standard",
                      include_field_ids=True)
dataclass_into_avro_schema(dataclass_type, *, name=None, namespace=None,
                           flavor="standard", include_field_ids=True,
                           localns=None)
avro_into_arrow_schema(schema, *, metadata=None, large_types=False)
avro_into_arrow_field(field, *, large_types=False)
records_into_avro(records, *, record_type=None, schema=None, codec="null",
                  metadata=None, sync_marker=None)
avro_into_records(record_type, source, *, schema=None, safe=True,
                  on_error="raise")
```

Generated records expose `into_avro_schema(...)`, `into_avro(records, ...)`,
and `from_avro(source, ...)`. `rkp.records.avro.avro_into_records()` takes two
further keywords, `start=0` and `stop=None`, which decode a record range
without reading the blocks before it.

## AWS Glue

Pure schema and DDL adapters:

```python
arrow_type_into_glue_type(value, *, path="value")
arrow_into_glue_column(field, *, path="")
arrow_into_glue_columns(schema)
into_glue_columns(value)
into_glue_table_input(value, *, name=None, location=None, format="parquet",
                      description=None, parameters=None, serde_parameters=None,
                      partition_keys=None, partition_projection=None,
                      partition_location_template=None)
into_glue_partition_values(value, schema=None, *, partition_keys=None)
into_glue_partition_projection(value, projections=None, *, partition_keys=None,
                               location_template=None, enabled=True)
glue_into_arrow_field(column)
glue_into_arrow_schema(value)

into_glue_ddl(value, *, name=None, database=None, location=None,
              format="parquet", if_not_exists=True, description=None,
              properties=None, serde_properties=None, partition_keys=None,
              partition_projection=None, partition_location_template=None)
into_glue_database_ddl(name, *, if_not_exists=True, description=None,
                       location=None, properties=None)
into_glue_drop_table_ddl(name, *, database=None, if_exists=True)
into_glue_drop_database_ddl(name, *, if_exists=True, cascade=False)
```

Generated record methods also expose `into_glue_partition_values()`,
`into_glue_partition_projection(...)`, `into_glue_table_input(...)`, and
`into_glue_ddl(...)` with the same table options.

`GlueCatalog(client=..., *, catalog_id=None, region_name=None)` provides:

- database: `ensure_database`, `create_database`, `get_database`,
  `update_database`, `delete_database`, and `list_databases`;
- table: `create_table`, `get_table`, `update_table`, `upsert_table`,
  `delete_table`, and `list_tables`;
- partition: `create_partition`, `get_partition`, `update_partition`,
  `upsert_partition`, `delete_partition`, `list_partitions`,
  `batch_create_partitions`, `batch_delete_partitions`, `partition_values`,
  and `create_partition_from`.

See the focused guides for behavior and limitations: [Arrow](../arrow.md),
[Avro](../avro.md),
[Spark](../spark.md), [Iceberg](../iceberg.md), and
[AWS Glue](../aws-glue.md).
