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


def test_every_declared_public_export_is_available() -> None:
    assert all(hasattr(rkp, name) for name in rkp.__all__)
    assert rkp.__version__ == version("rkp")


def test_aws_glue_public_surface_is_stable() -> None:
    assert GLUE_EXPORTS <= set(rkp.__all__)
