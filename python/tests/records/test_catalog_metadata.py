from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa
import pytest
from rkp import (
    Record,
    RecordMetadata,
    catalog_name,
    glue_into_arrow_schema,
    iceberg_into_arrow_schema,
    into_arrow_schema,
    into_glue_ddl,
    into_glue_table_input,
    into_iceberg_schema,
    record,
    record_metadata,
    schema_metadata,
    schema_name,
    table_name,
)


@record(
    alias="event_envelope",
    metadata={"owner": "platform", "retention": "30d"},
    catalog_name="LakeHouse",
    schema_name="Analytics",
    table_name="EventFacts",
)
class CatalogEvent(Record):
    identifier: int
    payload: str


@record(
    metadata={"layer": "base", "owner": "core"},
    catalog_name="MainCatalog",
    schema_name="Raw",
    table_name="BaseEvents",
)
class MetadataBase(Record):
    identifier: int


@record(metadata={"layer": "child"}, schema_name="Curated")
class MetadataChild(MetadataBase):
    label: str


@record(metadata=None)
class PayloadClearedMetadata(MetadataBase):
    label: str


@record(
    alias="cleared_events",
    catalog_name=None,
    schema_name=None,
    table_name=None,
)
class ClearedMetadata(MetadataBase):
    label: str


@dataclass
class PlainRow:
    identifier: int


@dataclass
class PlainEnvelope:
    row: PlainRow


@record(schema_name="public")
class MetadataNameCollision(Record):
    schema_name: str
    table_name: str


@record(
    metadata={
        "catalog_name": "MetadataCatalog",
        "schema_name": "MetadataSchema",
        "table_name": "MetadataTable",
        "owner": "metadata-only",
    }
)
class MetadataConfiguredRecord(Record):
    identifier: int


def test_record_metadata_accessors_share_one_cached_immutable_contract() -> None:
    metadata = record_metadata(CatalogEvent)
    value = CatalogEvent(1, "datum")

    assert isinstance(metadata, RecordMetadata)
    assert metadata is record_metadata(CatalogEvent)
    assert metadata is record_metadata(value)
    assert catalog_name(CatalogEvent) == "LakeHouse"
    assert catalog_name(value) == "LakeHouse"
    assert schema_name(CatalogEvent) == "Analytics"
    assert table_name(CatalogEvent) == "EventFacts"
    assert schema_metadata(CatalogEvent) == {
        b"owner": b"platform",
        b"retention": b"30d",
        b"catalog_name": b"LakeHouse",
        b"schema_name": b"Analytics",
        b"table_name": b"EventFacts",
    }
    assert schema_metadata(value) == schema_metadata(CatalogEvent)

    with pytest.raises((AttributeError, TypeError)):
        metadata.table_name = "mutated"  # type: ignore[misc]
    with pytest.raises(TypeError):
        metadata.metadata["owner"] = "mutated"  # type: ignore[index]


def test_record_arrow_schema_projects_portable_metadata_and_stays_cached() -> None:
    schema = CatalogEvent.into_arrow_schema()

    assert schema is CatalogEvent.into_arrow_schema()
    assert schema is into_arrow_schema(CatalogEvent)
    assert schema.metadata == schema_metadata(CatalogEvent)
    assert schema.metadata is not None
    assert not any(key.startswith(b"rkp.") for key in schema.metadata)
    assert CatalogEvent.into_arrow_field().name == "event_envelope"
    assert table_name(schema) == "EventFacts"

    restored = pa.ipc.read_schema(pa.BufferReader(schema.serialize()))
    assert restored.metadata == schema.metadata
    assert table_name(restored) == "EventFacts"

    # Arrow schemas are immutable even though ``metadata`` returns a mutable
    # snapshot on supported PyArrow versions.
    snapshot = schema.metadata
    assert snapshot is not None
    snapshot[b"table_name"] = b"mutated"
    assert table_name(schema) == "EventFacts"


def test_generic_metadata_mapping_can_supply_all_catalog_names() -> None:
    assert catalog_name(MetadataConfiguredRecord) == "MetadataCatalog"
    assert schema_name(MetadataConfiguredRecord) == "MetadataSchema"
    assert table_name(MetadataConfiguredRecord) == "MetadataTable"
    assert schema_metadata(MetadataConfiguredRecord) == {
        b"catalog_name": b"MetadataCatalog",
        b"schema_name": b"MetadataSchema",
        b"table_name": b"MetadataTable",
        b"owner": b"metadata-only",
    }


def test_metadata_inheritance_merges_payload_and_explicit_none_clears_names() -> None:
    assert schema_metadata(MetadataChild) == {
        b"layer": b"child",
        b"owner": b"core",
        b"catalog_name": b"MainCatalog",
        b"schema_name": b"Curated",
        b"table_name": b"BaseEvents",
    }

    assert catalog_name(ClearedMetadata) is None
    assert schema_name(ClearedMetadata) is None
    assert table_name(ClearedMetadata) == "cleared_events"
    assert schema_metadata(ClearedMetadata) == {
        b"layer": b"base",
        b"owner": b"core",
        b"table_name": b"cleared_events",
    }
    assert schema_metadata(PayloadClearedMetadata) == {
        b"catalog_name": b"MainCatalog",
        b"schema_name": b"Raw",
        b"table_name": b"BaseEvents",
    }


def test_plain_dataclasses_use_arrow_as_the_metadata_projection_boundary() -> None:
    row_schema = into_arrow_schema(PlainRow)
    envelope_schema = into_arrow_schema(PlainEnvelope)

    assert catalog_name(PlainRow) is None
    assert schema_name(PlainRow) is None
    assert table_name(PlainRow) == "plainrow"
    assert row_schema.metadata == {b"table_name": b"plainrow"}
    assert schema_metadata(PlainRow(1)) == {b"table_name": b"plainrow"}
    assert envelope_schema.metadata == {b"table_name": b"plainenvelope"}
    assert envelope_schema.field("row").metadata is None
    assert envelope_schema.field("row").type.field("identifier").metadata is None


def test_record_data_fields_do_not_collide_with_generic_metadata_accessors() -> None:
    value = MetadataNameCollision("datum-schema", "datum-table")

    assert value.schema_name == "datum-schema"
    assert value.table_name == "datum-table"
    assert schema_name(MetadataNameCollision) == "public"
    assert table_name(MetadataNameCollision) == "metadatanamecollision"
    assert MetadataNameCollision.into_arrow_schema().names == [
        "schema_name",
        "table_name",
    ]


def test_explicit_arrow_metadata_overlays_record_metadata() -> None:
    schema = into_arrow_schema(
        CatalogEvent,
        metadata={
            "owner": "experimentation",
            "request_id": "abc",
            "schema_name": "Scratch",
            "table_name": "AdHocEvents",
        },
    )

    assert schema.metadata == {
        b"owner": b"experimentation",
        b"retention": b"30d",
        b"request_id": b"abc",
        b"catalog_name": b"LakeHouse",
        b"schema_name": b"Scratch",
        b"table_name": b"AdHocEvents",
    }
    assert catalog_name(schema) == "LakeHouse"
    assert schema_name(schema) == "Scratch"
    assert table_name(schema) == "AdHocEvents"

    cleared = into_arrow_schema(
        CatalogEvent,
        metadata={"catalog_name": None, "owner": None},
    )
    assert catalog_name(cleared) is None
    assert b"owner" not in schema_metadata(cleared)


def test_glue_consumes_names_from_arrow_and_explicit_arguments_win() -> None:
    schema = pa.schema(
        [pa.field("identifier", pa.int64(), nullable=False)],
        metadata={
            b"catalog_name": b"123456789012",
            b"schema_name": b"Reporting",
            b"table_name": b"DailyFacts",
        },
    )

    table_input = into_glue_table_input(schema)
    overridden_input = into_glue_table_input(schema, name="TemporaryFacts")
    ddl = into_glue_ddl(schema)
    overridden = into_glue_ddl(schema, database="Scratch", name="TemporaryFacts")

    assert table_input["Name"] == "dailyfacts"
    assert overridden_input["Name"] == "temporaryfacts"
    assert table_name(glue_into_arrow_schema(overridden_input)) == "temporaryfacts"
    assert "`reporting`.`dailyfacts`" in ddl
    assert "`scratch`.`temporaryfacts`" in overridden


def test_glue_reverse_uses_live_catalog_identity_over_embedded_metadata() -> None:
    table = CatalogEvent.into_glue_table_input()
    table.update(
        {
            "CatalogId": "live-catalog",
            "DatabaseName": "live-schema",
            "Name": "live-table",
        }
    )

    restored = glue_into_arrow_schema(table)

    assert catalog_name(restored) == "live-catalog"
    assert schema_name(restored) == "live-schema"
    assert table_name(restored) == "live-table"
    assert restored.metadata is not None
    assert restored.metadata[b"catalog_name"] == b"live-catalog"
    assert restored.metadata[b"schema_name"] == b"live-schema"
    assert restored.metadata[b"table_name"] == b"live-table"

    descriptor_only = glue_into_arrow_schema(table["StorageDescriptor"])
    assert catalog_name(descriptor_only) is None
    assert schema_name(descriptor_only) is None
    assert table_name(descriptor_only) is None


def test_iceberg_roundtrip_recovers_names_from_arrow_metadata() -> None:
    source = CatalogEvent.into_arrow_schema()
    iceberg = into_iceberg_schema(source)

    restored = iceberg_into_arrow_schema(iceberg, metadata=source.metadata)

    assert catalog_name(restored) == "LakeHouse"
    assert schema_name(restored) == "Analytics"
    assert table_name(restored) == "EventFacts"
    assert restored.metadata is not None
    assert restored.metadata[b"owner"] == b"platform"


def test_conflicting_text_and_wire_metadata_keys_are_rejected() -> None:
    with pytest.raises((TypeError, ValueError), match="table_name"):
        into_arrow_schema(
            CatalogEvent,
            metadata={"table_name": "first", b"table_name": b"second"},
        )


def test_prefixed_name_metadata_remains_a_read_compatibility_alias() -> None:
    legacy = pa.schema(
        [pa.field("value", pa.int64())],
        metadata={
            b"rkp.catalog_name": b"LegacyCatalog",
            b"rkp.schema_name": b"LegacySchema",
            b"rkp.table_name": b"LegacyTable",
        },
    )

    assert catalog_name(legacy) == "LegacyCatalog"
    assert schema_name(legacy) == "LegacySchema"
    assert table_name(legacy) == "LegacyTable"

    updated = into_arrow_schema(legacy, metadata={"rkp.table_name": "ModernTable"})
    assert table_name(updated) == "ModernTable"
    assert updated.metadata is not None
    assert updated.metadata[b"table_name"] == b"ModernTable"
    assert b"rkp.table_name" not in updated.metadata


@pytest.mark.parametrize(
    ("key", "value"),
    [
        (b"catalog_name", b""),
        (b"schema_name", b"\xff"),
        (b"table_name", b"bad\x00name"),
    ],
)
def test_arrow_name_metadata_is_validated(key: bytes, value: bytes) -> None:
    schema = pa.schema([pa.field("value", pa.int64())], metadata={key: value})
    accessor = {
        b"catalog_name": catalog_name,
        b"schema_name": schema_name,
        b"table_name": table_name,
    }[key]

    with pytest.raises((TypeError, ValueError), match=key.decode("ascii")):
        accessor(schema)
