from __future__ import annotations

from datetime import date, datetime

import boto3
import pyarrow as pa
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws
from rkp import (
    GlueCatalog,
    Record,
    arrow_into_glue_columns,
    field,
    glue_into_arrow_schema,
    into_glue_database_ddl,
    into_glue_ddl,
    into_glue_drop_database_ddl,
    into_glue_drop_table_ddl,
    into_glue_table_input,
    record,
)


@record
class Coordinates(Record):
    latitude: float
    longitude: float


@record(alias="events")
class GlueEvent(Record):
    identifier: int = field(
        alias="event_id",
        seq=11,
        primary_key=True,
        doc="Stable event identifier",
    )
    occurred_at: datetime
    event_day: date = field(partition_key=True, doc="UTC event date")
    coordinates: Coordinates
    tags: list[str | None]
    attributes: dict[str, int | None]


def test_arrow_columns_preserve_nested_types_docs_and_rkp_metadata() -> None:
    columns = arrow_into_glue_columns(GlueEvent.into_arrow_schema())
    by_name = {column["Name"]: column for column in columns}

    assert list(by_name) == [
        "event_id",
        "occurred_at",
        "event_day",
        "coordinates",
        "tags",
        "attributes",
    ]
    assert by_name["event_id"] == {
        "Name": "event_id",
        "Type": "bigint",
        "Comment": "Stable event identifier",
        "Parameters": {
            "rkp.nullable": "false",
            "rkp.primary_key": "true",
            "rkp.seq": "11",
        },
    }
    assert by_name["occurred_at"]["Type"] == "timestamp"
    assert by_name["coordinates"]["Type"] == (
        "struct<`latitude`:double,`longitude`:double>"
    )
    assert by_name["tags"]["Type"] == "array<string>"
    assert by_name["attributes"]["Type"] == "map<string,bigint>"
    assert by_name["event_day"]["Comment"] == "UTC event date"
    assert by_name["event_day"]["Parameters"]["rkp.partition_key"] == "true"


def test_table_input_separates_partition_columns_and_configures_parquet() -> None:
    table = into_glue_table_input(
        GlueEvent,
        name="Events",
        location="s3://warehouse/events/",
        description="Normalized events",
        parameters={"owner": "data-platform"},
    )

    assert table["Name"] == "events"
    assert table["Description"] == "Normalized events"
    assert table["TableType"] == "EXTERNAL_TABLE"
    assert table["PartitionKeys"] == [
        {
            "Name": "event_day",
            "Type": "date",
            "Comment": "UTC event date",
            "Parameters": {
                "rkp.nullable": "false",
                "rkp.partition_key": "true",
            },
        }
    ]
    descriptor = table["StorageDescriptor"]
    assert [column["Name"] for column in descriptor["Columns"]] == [
        "event_id",
        "occurred_at",
        "coordinates",
        "tags",
        "attributes",
    ]
    assert descriptor["Location"] == "s3://warehouse/events/"
    assert "parquet" in descriptor["InputFormat"].lower()
    assert "parquet" in descriptor["OutputFormat"].lower()
    assert "parquet" in descriptor["SerdeInfo"]["SerializationLibrary"].lower()
    assert table["Parameters"]["owner"] == "data-platform"
    assert table["Parameters"]["classification"] == "parquet"


def test_table_input_accepts_iceberg_schema_and_an_explicit_format() -> None:
    iceberg_schema = GlueEvent.into_iceberg_schema()

    table = into_glue_table_input(
        iceberg_schema,
        name="events",
        location="s3://warehouse/events-orc/",
        format="ORC",
        partition_keys=["event_day"],
        serde_parameters={"z-last": "2", "a-first": "1"},
    )

    assert table["Parameters"]["classification"] == "orc"
    assert [column["Name"] for column in table["PartitionKeys"]] == ["event_day"]
    descriptor = table["StorageDescriptor"]
    assert "orc" in descriptor["InputFormat"].lower()
    assert "orc" in descriptor["OutputFormat"].lower()
    assert "orc" in descriptor["SerdeInfo"]["SerializationLibrary"].lower()
    serde = descriptor["SerdeInfo"]["Parameters"]
    assert list(serde) == sorted(serde)

    ddl = into_glue_ddl(
        iceberg_schema,
        name="events",
        database="analytics",
        location="s3://warehouse/events-orc/",
        format="orc",
        partition_keys=["event_day"],
    )
    assert "STORED AS ORC" in ddl


def test_table_input_rejects_unknown_storage_formats() -> None:
    with pytest.raises(ValueError, match=r"(?i)unsupported.*format"):
        into_glue_table_input(
            GlueEvent,
            location="s3://warehouse/events/",
            format="made-up",
        )


def test_glue_table_roundtrip_recovers_schema_and_field_roles() -> None:
    table = into_glue_table_input(
        GlueEvent,
        location="s3://warehouse/events/",
    )

    restored = glue_into_arrow_schema(table)

    assert set(restored.names) == set(GlueEvent.into_arrow_schema().names)
    assert restored.field("event_id").type == pa.int64()
    assert restored.field("event_id").nullable is False
    assert restored.field("event_id").metadata == {
        b"doc": b"Stable event identifier",
        b"primary_key": b"true",
        b"PARQUET:field_id": b"11",
    }
    assert restored.field("event_day").metadata == {
        b"doc": b"UTC event date",
        b"partition_key": b"true",
    }
    assert pa.types.is_struct(restored.field("coordinates").type)
    assert pa.types.is_list(restored.field("tags").type)
    assert pa.types.is_map(restored.field("attributes").type)


def test_external_glue_type_parser_recovers_nested_and_partition_fields() -> None:
    table = {
        "StorageDescriptor": {
            "Columns": [
                {
                    "Name": "payload",
                    "Type": (
                        "struct<`left``side`:bigint,"
                        "`items`:array<map<string,decimal(9,2)>>>"
                    ),
                    "Parameters": {"rkp.nullable": "false", "source": "external"},
                }
            ]
        },
        "PartitionKeys": [
            {
                "Name": "event_day",
                "Type": "date",
                "Parameters": {"rkp.nullable": "false"},
            }
        ],
    }

    schema = glue_into_arrow_schema(table)

    payload = schema.field("payload")
    assert payload.nullable is False
    assert payload.metadata == {b"source": b"external"}
    assert payload.type[0].name == "left`side"
    assert payload.type[0].type == pa.int64()
    assert payload.type[1].type.value_type.item_type == pa.decimal128(9, 2)
    partition = schema.field("event_day")
    assert partition.metadata == {b"partition_key": b"true"}


def test_record_convenience_methods_delegate_to_public_glue_adapters() -> None:
    expected = into_glue_table_input(
        GlueEvent,
        location="s3://warehouse/events/",
    )

    assert (
        GlueEvent.into_glue_table_input(location="s3://warehouse/events/") == expected
    )
    assert GlueEvent.into_glue_ddl(
        location="s3://warehouse/events/",
        database="analytics",
    ) == into_glue_ddl(
        GlueEvent,
        location="s3://warehouse/events/",
        database="analytics",
    )


def test_glue_ddl_is_deterministic_partitioned_and_safely_quoted() -> None:
    first = into_glue_ddl(
        GlueEvent,
        database="analytics",
        location="s3://warehouse/event's/",
        properties={"source": "event's"},
    )
    second = into_glue_ddl(
        GlueEvent,
        database="analytics",
        location="s3://warehouse/event's/",
        properties={"source": "event's"},
    )

    assert first == second
    assert "CREATE EXTERNAL TABLE IF NOT EXISTS" in first
    assert "`analytics`.`events`" in first
    assert "`event_id` BIGINT" in first
    assert "`coordinates` STRUCT<" in first
    assert "PARTITIONED BY" in first
    assert "`event_day` DATE" in first
    assert "STORED AS PARQUET" in first
    assert "s3://warehouse/event''s/" in first
    assert "event''s" in first


def test_database_and_drop_ddl_generators_are_deterministic_and_quoted() -> None:
    database = into_glue_database_ddl(
        "Analytics`Raw",
        description="Owner's datasets",
        location="s3://warehouse/raw/",
        properties={"z": "last", "a": "first"},
    )

    assert database.startswith("CREATE DATABASE IF NOT EXISTS `analytics``raw`")
    assert "Owner''s datasets" in database
    assert database.index("'a'='first'") < database.index("'z'='last'")
    assert (
        into_glue_drop_table_ddl("Events", database="Analytics")
        == "DROP TABLE IF EXISTS `analytics`.`events`;"
    )
    assert (
        into_glue_drop_database_ddl("Analytics", cascade=True)
        == "DROP DATABASE IF EXISTS `analytics` CASCADE;"
    )


@pytest.mark.parametrize(
    ("storage_format", "storage_keyword"),
    [
        ("parquet", "PARQUET"),
        ("orc", "ORC"),
        ("avro", "AVRO"),
        ("json", "TEXTFILE"),
        ("csv", "TEXTFILE"),
    ],
)
def test_all_storage_formats_have_table_and_ddl_definitions(
    storage_format: str, storage_keyword: str
) -> None:
    table = into_glue_table_input(
        GlueEvent,
        location="s3://warehouse/events/",
        format=storage_format,
    )
    ddl = into_glue_ddl(
        GlueEvent,
        location="s3://warehouse/events/",
        format=storage_format,
    )

    assert table["Parameters"]["classification"] == storage_format
    assert table["StorageDescriptor"]["SerdeInfo"]["SerializationLibrary"]
    assert f"STORED AS {storage_keyword}" in ddl
    if storage_format in {"json", "csv"}:
        assert "ROW FORMAT SERDE" in ddl


@pytest.mark.parametrize(
    ("arrow_type", "expected"),
    [
        (pa.bool_(), "boolean"),
        (pa.int8(), "tinyint"),
        (pa.int16(), "smallint"),
        (pa.int32(), "int"),
        (pa.int64(), "bigint"),
        (pa.uint8(), "smallint"),
        (pa.uint16(), "int"),
        (pa.uint32(), "bigint"),
        (pa.uint64(), "decimal(20,0)"),
        (pa.float32(), "float"),
        (pa.float64(), "double"),
        (pa.decimal128(18, 3), "decimal(18,3)"),
        (pa.binary(), "binary"),
        (pa.date32(), "date"),
        (pa.timestamp("us", tz="UTC"), "timestamp"),
        (pa.null(), "string"),
        (pa.dictionary(pa.int32(), pa.string()), "string"),
    ],
)
def test_glue_primitive_type_matrix(
    arrow_type: pa.DataType,
    expected: str,
) -> None:
    column = arrow_into_glue_columns(pa.schema([pa.field("value", arrow_type)]))[0]

    assert column["Type"] == expected


@pytest.mark.parametrize(
    "arrow_type",
    [
        pa.time64("us"),
        pa.duration("us"),
        pa.decimal256(39, 2),
    ],
)
def test_unsupported_glue_types_report_the_field_path(
    arrow_type: pa.DataType,
) -> None:
    schema = pa.schema(
        [pa.field("payload", pa.struct([pa.field("unsupported", arrow_type)]))]
    )

    with pytest.raises(TypeError, match=r"payload.*unsupported"):
        arrow_into_glue_columns(schema)


def test_partition_keys_must_exist_be_unique_and_be_primitive() -> None:
    schema = pa.schema(
        [
            pa.field("value", pa.int64()),
            pa.field("nested", pa.list_(pa.string())),
        ]
    )

    with pytest.raises(ValueError, match=r"(?i)partition.*missing"):
        into_glue_table_input(schema, name="bad", partition_keys=["missing"])
    with pytest.raises(ValueError, match=r"(?i)duplicate.*partition"):
        into_glue_table_input(
            schema,
            name="bad",
            partition_keys=["value", "value"],
        )
    with pytest.raises(TypeError, match=r"(?i)partition.*primitive"):
        into_glue_table_input(schema, name="bad", partition_keys=["nested"])


def test_explicit_partition_override_is_reflected_in_embedded_roundtrip() -> None:
    source = pa.schema(
        [
            pa.field("plain", pa.string(), metadata={b"partition_key": b"true"}),
            pa.field("selected", pa.date32(), metadata={b"partition_key": b"false"}),
        ]
    )

    table = into_glue_table_input(
        source,
        name="override",
        partition_keys=["selected"],
    )
    restored = glue_into_arrow_schema(table)

    assert (restored.field("plain").metadata or {}).get(b"partition_key") is None
    assert restored.field("selected").metadata == {b"partition_key": b"true"}


def test_partition_key_order_survives_glue_roundtrip() -> None:
    source = pa.schema(
        [
            pa.field("first", pa.string()),
            pa.field("second", pa.string()),
            pa.field("value", pa.int64()),
        ]
    )
    table = into_glue_table_input(
        source,
        name="ordered",
        partition_keys=["second", "first"],
    )

    restored = glue_into_arrow_schema(table)
    regenerated = into_glue_table_input(restored, name="ordered")

    assert [item["Name"] for item in regenerated["PartitionKeys"]] == [
        "second",
        "first",
    ]


def test_clearing_partition_keys_removes_the_persisted_order() -> None:
    source = pa.schema(
        [
            pa.field("first", pa.string()),
            pa.field("second", pa.string()),
            pa.field("value", pa.int64()),
        ]
    )
    partitioned = into_glue_table_input(
        source,
        name="ordered",
        partition_keys=["second", "first"],
    )
    restored = glue_into_arrow_schema(partitioned)

    unpartitioned = into_glue_table_input(
        restored,
        name="ordered",
        partition_keys=[],
    )
    roundtripped = glue_into_arrow_schema(unpartitioned)

    assert unpartitioned["PartitionKeys"] == []
    assert into_glue_table_input(roundtripped, name="ordered")["PartitionKeys"] == []


def test_embedded_schema_is_checked_against_live_glue_columns() -> None:
    table = into_glue_table_input(GlueEvent)
    table["StorageDescriptor"]["Columns"][0]["Type"] = "string"

    with pytest.raises(ValueError, match=r"(?i)does not match embedded"):
        glue_into_arrow_schema(table)

    table = into_glue_table_input(GlueEvent)
    table["PartitionKeys"].append(dict(table["PartitionKeys"][0]))
    with pytest.raises(ValueError, match=r"(?i)duplicate"):
        glue_into_arrow_schema(table)

    table = into_glue_table_input(GlueEvent)
    table["PartitionKeys"][0]["Name"] = "unknown"
    with pytest.raises(ValueError, match=r"(?i)does not match"):
        glue_into_arrow_schema(table)


def test_external_type_parser_validates_nested_types_and_comments() -> None:
    nested = glue_into_arrow_schema(
        {
            "Columns": [
                {
                    "Name": "payload",
                    "Type": (
                        "struct<`identifier`:bigint COMMENT 'Stable ''ID''',"
                        "`label`:varchar(64)>"
                    ),
                }
            ]
        }
    )

    assert nested.field("payload").type[0].metadata == {b"doc": b"Stable 'ID'"}
    with pytest.raises(ValueError, match=r"(?i)map keys must be primitive"):
        glue_into_arrow_schema(
            {"Columns": [{"Name": "bad", "Type": "map<array<int>,string>"}]}
        )
    with pytest.raises(ValueError, match=r"(?i)duplicate.*field"):
        glue_into_arrow_schema(
            {"Columns": [{"Name": "bad", "Type": "struct<a:int,a:string>"}]}
        )
    for glue_type in ("char(0)", "char(256)", "varchar(0)", "varchar(65536)"):
        with pytest.raises(ValueError, match=r"(?i)length"):
            glue_into_arrow_schema({"Columns": [{"Name": "bad", "Type": glue_type}]})


def test_nested_arrow_docs_roundtrip_through_glue_type_syntax() -> None:
    source = pa.schema(
        [
            pa.field(
                "payload",
                pa.struct(
                    [
                        pa.field(
                            "identifier",
                            pa.int64(),
                            metadata={b"doc": b"Stable 'ID'"},
                        )
                    ]
                ),
            )
        ]
    )

    column = arrow_into_glue_columns(source)[0]
    restored = glue_into_arrow_schema({"Columns": [column]})

    assert "comment 'Stable ''ID'''" in column["Type"]
    assert restored.field("payload").type[0].metadata == {b"doc": b"Stable 'ID'"}


def test_glue_builders_enforce_service_payload_bounds() -> None:
    with pytest.raises(ValueError, match=r"(?i)at least one field"):
        into_glue_table_input(pa.schema([]), name="empty")
    with pytest.raises(ValueError, match=r"(?i)at least one field"):
        into_glue_ddl(pa.schema([]), name="empty")
    with pytest.raises(ValueError, match=r"(?i)comment.*255"):
        arrow_into_glue_columns(
            pa.schema([pa.field("value", pa.int64(), metadata={b"doc": b"x" * 256})])
        )
    with pytest.raises(ValueError, match=r"(?i)keys.*255"):
        into_glue_table_input(
            pa.schema([pa.field("value", pa.int64())]),
            name="bounded",
            parameters={"x" * 256: "value"},
        )
    with pytest.raises(ValueError, match=r"(?i)512000"):
        into_glue_table_input(
            pa.schema([pa.field("value", pa.int64())]),
            name="bounded",
            parameters={"value": "x" * 512_001},
        )


def test_glue_rejects_duplicate_fields_and_invalid_decimal_bounds() -> None:
    duplicate = pa.schema([pa.field("same", pa.int64()), pa.field("same", pa.string())])
    with pytest.raises(ValueError, match=r"(?i)duplicate.*field"):
        into_glue_table_input(duplicate, name="bad")
    nested_duplicate = pa.schema(
        [
            pa.field(
                "items",
                pa.list_(
                    pa.struct(
                        [pa.field("same", pa.int64()), pa.field("same", pa.string())]
                    )
                ),
            )
        ]
    )
    with pytest.raises(ValueError, match=r"(?i)duplicate.*field"):
        into_glue_table_input(nested_duplicate, name="bad")
    with pytest.raises(TypeError, match=r"(?i)decimal.*scale"):
        arrow_into_glue_columns(
            pa.schema([pa.field("bad_decimal", pa.decimal128(10, -2))])
        )


def test_ddl_escapes_backslashes_and_omits_empty_column_parentheses() -> None:
    partition_only = pa.schema(
        [pa.field("day", pa.date32(), metadata={b"partition_key": b"true"})]
    )
    ddl = into_glue_ddl(
        partition_only,
        name="partitioned",
        location="s3://warehouse/path\\",
        properties={"slash": "\\"},
    )

    assert "`partitioned`\nPARTITIONED BY" in ddl
    assert "s3://warehouse/path\\\\" in ddl
    assert "'slash'='\\\\'" in ddl


@mock_aws
def test_glue_catalog_database_and_table_crud_upserts_through_moto() -> None:
    client = boto3.client("glue", region_name="eu-west-1")
    catalog = GlueCatalog(client)

    catalog.ensure_database("analytics", description="Analytics database")
    catalog.ensure_database("analytics", description="Analytics database")
    database = client.get_database(Name="analytics")["Database"]
    assert database["Name"] == "analytics"
    assert database["Description"] == "Analytics database"

    initial = into_glue_table_input(
        GlueEvent,
        location="s3://warehouse/events/v1/",
        description="Version one",
    )
    catalog.upsert_table("analytics", initial)
    created = client.get_table(DatabaseName="analytics", Name="events")["Table"]
    assert created["Description"] == "Version one"
    assert created["StorageDescriptor"]["Location"].endswith("/v1/")

    updated = into_glue_table_input(
        GlueEvent,
        location="s3://warehouse/events/v2/",
        description="Version two",
    )
    catalog.upsert_table("analytics", updated)
    stored = catalog.get_table("analytics", "events")
    assert stored["Description"] == "Version two"
    assert stored["StorageDescriptor"]["Location"].endswith("/v2/")

    catalog.delete_table("analytics", "events")
    catalog.delete_table("analytics", "events", missing_ok=True)
    with pytest.raises(ClientError) as error:
        client.get_table(DatabaseName="analytics", Name="events")
    assert error.value.response["Error"]["Code"] == "EntityNotFoundException"


@mock_aws
def test_glue_catalog_surfaces_absence_without_masking_other_client_errors() -> None:
    client = boto3.client("glue", region_name="eu-west-1")
    catalog = GlueCatalog(client)
    catalog.ensure_database("analytics")

    with pytest.raises(ClientError) as missing:
        catalog.get_table("analytics", "missing")
    assert missing.value.response["Error"]["Code"] == "EntityNotFoundException"
    with pytest.raises(ClientError) as error:
        catalog.get_table("unknown", "missing")
    assert error.value.response["Error"]["Code"] == "EntityNotFoundException"


@mock_aws
def test_glue_catalog_database_listing_and_partition_lifecycle() -> None:
    client = boto3.client("glue", region_name="eu-west-1")
    catalog = GlueCatalog(client)
    catalog.create_database("Analytics", description="Initial")
    updated = catalog.update_database("analytics", description="Updated")
    assert updated["Description"] == "Updated"
    assert [item["Name"] for item in catalog.list_databases()] == ["analytics"]

    table_input = into_glue_table_input(
        GlueEvent,
        location="s3://warehouse/events/",
    )
    catalog.create_table("analytics", table_input)
    assert [item["Name"] for item in catalog.list_tables("analytics")] == ["events"]
    descriptor = table_input["StorageDescriptor"]

    first = {
        "Values": ["2026-08-13"],
        "StorageDescriptor": {
            **descriptor,
            "Location": "s3://warehouse/events/event_day=2026-08-13/",
        },
    }
    created = catalog.create_partition("analytics", "events", first)
    assert created["Values"] == ["2026-08-13"]
    changed = {
        **first,
        "Parameters": {"version": "2"},
    }
    updated_partition = catalog.upsert_partition("analytics", "events", changed)
    assert updated_partition["Parameters"] == {"version": "2"}

    response = catalog.batch_create_partitions(
        "analytics",
        "events",
        [
            {"Values": ["2026-08-14"], "StorageDescriptor": descriptor},
            {"Values": ["2026-08-15"], "StorageDescriptor": descriptor},
        ],
    )
    assert response.get("Errors", []) == []
    assert {
        tuple(item["Values"]) for item in catalog.list_partitions("analytics", "events")
    } == {
        ("2026-08-13",),
        ("2026-08-14",),
        ("2026-08-15",),
    }
    deleted = catalog.batch_delete_partitions(
        "analytics", "events", [["2026-08-14"], ["2026-08-15"]]
    )
    assert deleted.get("Errors", []) == []
    assert catalog.delete_partition("analytics", "events", ["2026-08-13"]) is True
    assert catalog.delete_database("analytics") is True


@mock_aws
def test_glue_catalog_validates_partition_and_batch_limits_locally() -> None:
    client = boto3.client("glue", region_name="eu-west-1")
    catalog = GlueCatalog(client)
    catalog.ensure_database("analytics")
    catalog.create_table(
        "analytics",
        into_glue_table_input(
            GlueEvent,
            location="s3://warehouse/events/",
        ),
    )

    with pytest.raises(ValueError, match=r"(?i)at least one"):
        catalog.create_partition("analytics", "events", {"Values": []})
    with pytest.raises(TypeError, match=r"1 to 1024"):
        catalog.create_partition("analytics", "events", {"Values": [""]})
    with pytest.raises(ValueError, match=r"1 to 100"):
        catalog.batch_create_partitions("analytics", "events", [])
    with pytest.raises(ValueError, match=r"1 to 25"):
        catalog.batch_delete_partitions("analytics", "events", [])
    with pytest.raises(TypeError, match="exist_ok must be bool"):
        catalog.create_table(
            "analytics",
            into_glue_table_input(GlueEvent),
            exist_ok=1,  # type: ignore[arg-type]
        )
    with pytest.raises(TypeError, match="skip_archive must be bool"):
        catalog.upsert_table(
            "analytics",
            into_glue_table_input(GlueEvent),
            skip_archive="yes",  # type: ignore[arg-type]
        )


def _moto_table_input(location: str) -> dict[str, object]:
    return {
        "Name": "events",
        "TableType": "EXTERNAL_TABLE",
        "StorageDescriptor": {
            "Columns": [{"Name": "payload", "Type": "string"}],
            "Location": location,
        },
        "PartitionKeys": [{"Name": "event_day", "Type": "date"}],
    }


@mock_aws
def test_glue_catalog_forwards_catalog_id_and_exist_ok_returns_existing_items() -> None:
    client = boto3.client("glue", region_name="eu-west-1")
    catalog_id = "999999999999"
    requests: list[tuple[str, str | None]] = []

    def capture_catalog_id(
        params: dict[str, object], model: object, **_: object
    ) -> None:
        requests.append((model.name, params.get("CatalogId")))  # type: ignore[attr-defined]

    client.meta.events.register("before-parameter-build.glue.*", capture_catalog_id)
    catalog = GlueCatalog(client, catalog_id=catalog_id)

    database = catalog.create_database("analytics", description="Original")
    existing_database = catalog.create_database(
        "analytics",
        description="Ignored",
        exist_ok=True,
    )
    assert database["CatalogId"] == catalog_id
    assert existing_database["Description"] == "Original"

    table_input = _moto_table_input("s3://warehouse/events/original/")
    table = catalog.create_table("analytics", table_input)
    existing_table_input = _moto_table_input("s3://warehouse/events/ignored/")
    existing_table = catalog.create_table(
        "analytics",
        existing_table_input,
        exist_ok=True,
    )
    # Moto currently reports its default account on tables even when the request
    # carries a custom catalog, so request-event capture verifies forwarding.
    assert table["Name"] == "events"
    assert existing_table["StorageDescriptor"]["Location"].endswith("/original/")

    descriptor = table_input["StorageDescriptor"]
    partition = catalog.create_partition(
        "analytics",
        "events",
        {
            "Values": ["2026-08-13"],
            "StorageDescriptor": {
                **descriptor,
                "Location": "s3://warehouse/events/original-partition/",
            },
        },
    )
    existing_partition = catalog.create_partition(
        "analytics",
        "events",
        {
            "Values": ["2026-08-13"],
            "StorageDescriptor": {
                **descriptor,
                "Location": "s3://warehouse/events/ignored-partition/",
            },
        },
        exist_ok=True,
    )
    assert partition["Values"] == ["2026-08-13"]
    assert existing_partition["StorageDescriptor"]["Location"].endswith(
        "/original-partition/"
    )

    assert requests
    assert {catalog for _, catalog in requests} == {catalog_id}
    assert {operation for operation, _ in requests} >= {
        "CreateDatabase",
        "GetDatabase",
        "CreateTable",
        "GetTable",
        "CreatePartition",
        "GetPartition",
    }


@mock_aws
def test_glue_catalog_moves_partition_and_missing_ok_is_idempotent() -> None:
    client = boto3.client("glue", region_name="eu-west-1")
    catalog = GlueCatalog(client)
    catalog.create_database("analytics")
    table_input = _moto_table_input("s3://warehouse/events/")
    catalog.create_table("analytics", table_input)
    descriptor = table_input["StorageDescriptor"]
    catalog.create_partition(
        "analytics",
        "events",
        {"Values": ["2026-08-13"], "StorageDescriptor": descriptor},
    )

    moved = catalog.update_partition(
        "analytics",
        "events",
        ["2026-08-13"],
        {
            "Values": ["2026-08-14"],
            "StorageDescriptor": descriptor,
            "Parameters": {"moved": "true"},
        },
    )

    assert moved["Values"] == ["2026-08-14"]
    assert moved["Parameters"] == {"moved": "true"}
    with pytest.raises(ClientError) as old_partition:
        catalog.get_partition("analytics", "events", ["2026-08-13"])
    assert old_partition.value.response["Error"]["Code"] == "EntityNotFoundException"

    assert catalog.delete_partition("analytics", "events", ["2026-08-14"]) is True
    assert (
        catalog.delete_partition(
            "analytics",
            "events",
            ["2026-08-14"],
            missing_ok=True,
        )
        is False
    )
    assert catalog.delete_table("analytics", "events") is True
    assert catalog.delete_table("analytics", "events", missing_ok=True) is False
    assert catalog.delete_database("analytics") is True
    assert catalog.delete_database("analytics", missing_ok=True) is False


@mock_aws
def test_glue_catalog_batch_operations_return_partial_errors() -> None:
    client = boto3.client("glue", region_name="eu-west-1")
    catalog = GlueCatalog(client)
    catalog.create_database("analytics")
    table_input = _moto_table_input("s3://warehouse/events/")
    catalog.create_table("analytics", table_input)
    descriptor = table_input["StorageDescriptor"]
    catalog.create_partition(
        "analytics",
        "events",
        {"Values": ["2026-08-13"], "StorageDescriptor": descriptor},
    )

    created = catalog.batch_create_partitions(
        "analytics",
        "events",
        [
            {"Values": ["2026-08-13"], "StorageDescriptor": descriptor},
            {"Values": ["2026-08-14"], "StorageDescriptor": descriptor},
        ],
    )

    assert created["Errors"] == [
        {
            "PartitionValues": ["2026-08-13"],
            "ErrorDetail": {
                "ErrorCode": "AlreadyExistsException",
                "ErrorMessage": "Partition already exists.",
            },
        }
    ]
    assert catalog.get_partition("analytics", "events", ["2026-08-14"])["Values"] == [
        "2026-08-14"
    ]

    deleted = catalog.batch_delete_partitions(
        "analytics",
        "events",
        [["2026-08-14"], ["2026-08-15"]],
    )

    assert deleted["Errors"] == [
        {
            "PartitionValues": ["2026-08-15"],
            "ErrorDetail": {
                "ErrorCode": "EntityNotFoundException",
                "ErrorMessage": "Partition not found",
            },
        }
    ]
    with pytest.raises(ClientError):
        catalog.get_partition("analytics", "events", ["2026-08-14"])


@mock_aws
def test_glue_catalog_list_accumulates_every_paginator_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = boto3.client("glue", region_name="eu-west-1")
    catalog = GlueCatalog(client, catalog_id="999999999999")
    catalog.create_database("first")
    catalog.create_database("second")
    catalog.create_database("third")
    original_get_paginator = client.get_paginator
    paginate_requests: list[dict[str, object]] = []

    class SplitMotoPaginator:
        def __init__(self, operation: str) -> None:
            self.operation = operation
            self.delegate = original_get_paginator(operation)

        def paginate(self, **kwargs: object) -> object:
            paginate_requests.append(dict(kwargs))
            pages = list(self.delegate.paginate(**kwargs))
            databases = [
                database for page in pages for database in page.get("DatabaseList", [])
            ]
            yield {"DatabaseList": databases[:1]}
            yield {"DatabaseList": databases[1:]}

    monkeypatch.setattr(
        client,
        "get_paginator",
        lambda operation: SplitMotoPaginator(operation),
    )

    databases = catalog.list_databases()

    assert [database["Name"] for database in databases] == [
        "first",
        "second",
        "third",
    ]
    assert paginate_requests == [{"CatalogId": "999999999999"}]
