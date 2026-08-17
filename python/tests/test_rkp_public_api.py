from importlib.metadata import version

import rkp

GLUE_EXPORTS = {
    "GlueCatalog",
    "arrow_into_glue_column",
    "arrow_into_glue_columns",
    "arrow_type_into_glue_type",
    "glue_into_arrow_field",
    "glue_into_arrow_schema",
    "into_glue_columns",
    "into_glue_database_ddl",
    "into_glue_ddl",
    "into_glue_drop_database_ddl",
    "into_glue_drop_table_ddl",
    "into_glue_partition_projection",
    "into_glue_partition_values",
    "into_glue_table_input",
}

AVRO_EXPORTS = {
    "arrow_into_avro_field",
    "arrow_into_avro_schema",
    "avro_into_arrow_field",
    "avro_into_arrow_schema",
    "avro_into_iceberg_schema",
    "avro_into_records",
    "dataclass_into_avro_schema",
    "iceberg_into_avro_schema",
    "into_avro_schema",
    "records_into_avro",
}

ICEBERG_CATALOG_EXPORTS = {
    "create_iceberg_table",
    "iceberg_table_into_arrow",
    "iceberg_table_into_records",
    "into_iceberg_partition_spec",
    "into_iceberg_sort_order",
    "load_iceberg_table",
    "records_into_iceberg_table",
    "sync_iceberg_table_schema",
}


def test_every_declared_public_export_is_available() -> None:
    assert all(hasattr(rkp, name) for name in rkp.__all__)
    assert rkp.__version__ == version("rkp")


def test_aws_glue_public_surface_is_stable() -> None:
    assert GLUE_EXPORTS <= set(rkp.__all__)


def test_avro_public_surface_is_stable() -> None:
    assert AVRO_EXPORTS <= set(rkp.__all__)


def test_iceberg_catalog_public_surface_is_stable() -> None:
    assert ICEBERG_CATALOG_EXPORTS <= set(rkp.__all__)


def test_the_avro_package_declares_its_own_surface() -> None:
    import rkp.avro

    assert all(hasattr(rkp.avro, name) for name in rkp.avro.__all__)
    assert {"parse_schema", "encode", "decode", "write_container"} <= set(
        rkp.avro.__all__
    )
