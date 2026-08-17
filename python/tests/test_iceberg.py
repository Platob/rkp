"""An Iceberg table, built on the handle every other test already uses."""

from __future__ import annotations

import pathlib

import pyarrow as pa
import pytest

from yggdryl import IOBase
from yggdryl.iceberg import (
    PartitionSpec,
    Table,
    assign_field_ids,
    schema_from_json,
    schema_to_json,
)

SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("venue", pa.string()),
    ]
)


def _rows(start: int = 1) -> pa.RecordBatch:
    """Three rows across two venues and the absence of one."""
    return pa.record_batch(
        {"id": [start, start + 1, start + 2], "venue": ["XNAS", "XNYS", None]},
        schema=SCHEMA,
    )


@pytest.fixture
def numbered() -> object:
    """The shared schema, with the field identifiers Iceberg resolves by."""
    return assign_field_ids(SCHEMA)


@pytest.fixture
def table(tmp_path: pathlib.Path, numbered: object) -> Table:
    """A partitioned table with nothing written to it yet."""
    return Table.create(IOBase(tmp_path / "trades"), numbered, ["venue"])


class TestSchemasCarryIdentifiers:
    """Iceberg resolves a column by identifier, not by position."""

    def test_numbering_a_pyarrow_schema_returns_a_native_root(self) -> None:
        numbered = assign_field_ids(SCHEMA)

        assert numbered.name == "row"
        assert [child.id for child in numbered.data_type] == [1, 2]
        # The input is untouched: the numbered schema is a new value.
        assert SCHEMA.field("id").metadata is None

    def test_numbering_starts_where_it_is_told_to(self) -> None:
        numbered = assign_field_ids(SCHEMA, 10)

        assert [child.id for child in numbered.data_type] == [10, 11]

    def test_a_root_that_is_not_a_non_null_struct_is_refused(
        self, tmp_path: pathlib.Path
    ) -> None:
        with pytest.raises(ValueError):
            Table.create(IOBase(tmp_path / "scalar"), "row:int64 not null")

    def test_a_schema_document_round_trips(self) -> None:
        document = {
            "type": "struct",
            "schema-id": 0,
            "fields": [
                {"id": 1, "name": "id", "required": True, "type": "long"},
                {"id": 2, "name": "venue", "required": False, "type": "string"},
            ],
        }

        schema = schema_from_json("row", document)
        assert schema.data_type.kind == "struct"
        assert not schema.nullable
        # `required` inverts into nullability, and `id` becomes PARQUET:field_id.
        assert not schema.data_type[0].nullable
        assert schema.data_type[1].nullable
        assert [child.id for child in schema.data_type] == [1, 2]

        assert schema_to_json(schema) == document

    def test_a_document_that_is_not_a_schema_is_refused(self) -> None:
        with pytest.raises(ValueError):
            schema_from_json("row", {"type": "long"})


class TestCreatingAndOpening:
    """A table is a folder, and it is found without a catalog."""

    def test_a_new_table_has_a_schema_and_no_snapshot(self, table: Table) -> None:
        assert table.format_version == 2
        assert table.version == 1
        assert table.current_snapshot is None
        assert table.schemas != []
        assert [field.name for field in table.spec.fields] == ["venue"]
        assert table.spec.fields[0].transform == "identity"

        # An empty table reads as no rows rather than as a failure.
        assert table.scan().read_all().num_rows == 0

    def test_the_metadata_document_is_where_a_reader_looks(
        self, table: Table, tmp_path: pathlib.Path
    ) -> None:
        assert table.metadata_file_name == "v1.metadata.json"
        assert table.metadata_location.endswith("metadata/v1.metadata.json")

        metadata = IOBase(tmp_path / "trades" / "metadata")
        assert {entry.name for entry in metadata} == {
            "v1.metadata.json",
            "version-hint.text",
        }
        assert metadata.joinpath("version-hint.text").read_text() == "1"

    def test_open_finds_the_current_document(
        self, table: Table, tmp_path: pathlib.Path
    ) -> None:
        table.append(_rows())

        reopened = Table.open(IOBase(tmp_path / "trades"))
        assert reopened.version == table.version
        assert reopened.table_uuid == table.table_uuid
        assert reopened.scan().read_all().num_rows == 3

    def test_open_or_create_does_not_write_over_a_table(
        self, table: Table, tmp_path: pathlib.Path, numbered: object
    ) -> None:
        table.append(_rows())

        same = Table.open_or_create(IOBase(tmp_path / "trades"), numbered, ["venue"])
        assert same.scan().read_all().num_rows == 3

    def test_a_buffer_is_not_a_table(self, numbered: object) -> None:
        # A table is a folder, and an in-memory buffer names no folder.
        with pytest.raises(ValueError, match="file URI"):
            Table.create(IOBase.from_bytes(), numbered)


class TestCommits:
    """Each commit writes data files, a manifest, a list, and a document."""

    def test_appending_keeps_what_is_already_stored(self, table: Table) -> None:
        table.append(_rows())
        table.append(_rows(4))

        assert table.scan().read_all().num_rows == 6
        assert table.version == 3
        assert len(table.snapshots) == 2
        assert table.current_snapshot is not None
        assert table.current_snapshot.operation == "append"
        assert (
            table.current_snapshot.parent_snapshot_id == table.snapshots[0].snapshot_id
        )

    def test_overwriting_replaces_every_row(self, table: Table) -> None:
        table.append(_rows())
        table.overwrite(_rows(10))

        rows = table.scan().read_all()
        assert rows.column("id").to_pylist() == [10, 11, 12]
        assert table.current_snapshot is not None
        assert table.current_snapshot.operation == "overwrite"
        # The previous snapshot is retained, which is what makes this reversible.
        assert len(table.snapshots) == 2

    def test_a_commit_takes_anything_pyarrow_streams(self, table: Table) -> None:
        table.append(pa.Table.from_batches([_rows()]))
        table.append(pa.RecordBatchReader.from_batches(SCHEMA, [_rows(4)]))

        assert table.scan().read_all().num_rows == 6


class TestPartitioning:
    """The manifest is the authority on a partition value; the path is layout."""

    def test_one_file_per_partition_lands_in_a_named_directory(
        self, table: Table
    ) -> None:
        table.append(_rows())

        files = table.data_files()
        assert len(files) == 3
        assert sorted(file.partition[0] for file, _ in files if file.partition[0]) == [
            "XNAS",
            "XNYS",
        ]
        assert [spec.fields[0].name for _, spec in files] == ["venue"] * 3
        assert all(file.file_format == "PARQUET" for file, _ in files)
        assert {file.record_count for file, _ in files} == {1}

    def test_a_null_partition_is_the_absence_and_not_the_word(
        self, table: Table
    ) -> None:
        table.append(_rows())

        absent = [file for file, _ in table.data_files() if file.partition[0] is None]
        assert len(absent) == 1
        # The directory spells it `null`, and only the manifest can say which.
        assert "venue=null" in absent[0].path

        rows = table.scan().read_all()
        assert rows.column("venue").to_pylist() == ["XNAS", "XNYS", None]

    def test_a_data_file_is_a_child_of_the_table(
        self, table: Table, tmp_path: pathlib.Path
    ) -> None:
        table.append(_rows())
        file, _ = table.data_files()[0]

        assert file.path.startswith(table.location)
        assert file.file_size_in_bytes > 0
        assert file.value_counts != {}
        assert file.content == 0, "rows, not deletes"

    def test_a_bound_travels_as_the_encoded_value(self, table: Table) -> None:
        table.append(_rows())
        file, _ = table.data_files()[0]

        # A bound is the encoded value Iceberg stores, keyed by field id, which
        # is what lets a planner skip a file without opening it.
        assert isinstance(file.lower_bounds[1], bytes)
        assert file.lower_bounds[1] == file.upper_bounds[1]
        assert file.null_value_counts[1] == 0

    def test_the_manifest_describes_what_the_commit_added(self, table: Table) -> None:
        table.append(_rows())

        manifests = table.manifests()
        assert len(manifests) == 1
        assert manifests[0].is_data()
        assert manifests[0].added_files_count == 3
        assert manifests[0].added_rows_count == 3
        assert manifests[0].path.endswith(".avro")

    def test_an_unpartitioned_table_writes_one_file(
        self, tmp_path: pathlib.Path, numbered: object
    ) -> None:
        table = Table.create(IOBase(tmp_path / "flat"), numbered)

        assert table.spec.is_unpartitioned()
        table.append(_rows())
        assert len(table.data_files()) == 1

    def test_a_spec_may_be_built_before_the_table(self, numbered: object) -> None:
        spec = PartitionSpec.identity(numbered, ["venue"], spec_id=0)

        assert len(spec) == 1
        assert spec.fields[0].source_id == 2
        assert spec.fields[0].field_id == 1000
        assert not spec.is_unpartitioned()


class TestScans:
    """A scan pushes columns down to each file and casts to the scan root."""

    def test_a_projected_scan_reads_the_columns_it_names(self, table: Table) -> None:
        table.append(_rows())
        wanted = pa.schema([pa.field("id", pa.int64(), nullable=False)])

        projected = table.scan(wanted).read_all()
        assert projected.column_names == ["id"]
        assert projected.num_rows == 3

    def test_an_evolved_schema_reads_as_one_shape(self, table: Table) -> None:
        table.append(_rows())

        widened = assign_field_ids(
            pa.schema(
                [
                    pa.field("id", pa.int64(), nullable=False),
                    pa.field("venue", pa.string()),
                    pa.field("price", pa.float64()),
                ]
            )
        )
        schema_id = table.evolve_schema(widened)

        assert schema_id == 1
        assert len(table.schemas) == 2
        rows = table.scan().read_all()
        # The files predate the column, so it reads as null rather than failing.
        assert rows.column_names == ["id", "venue", "price"]
        assert rows.column("price").to_pylist() == [None, None, None]

        table.append(
            pa.record_batch(
                {"id": [4], "venue": ["XNAS"], "price": [1.5]},
                schema=pa.schema(
                    [
                        pa.field("id", pa.int64(), nullable=False),
                        pa.field("venue", pa.string()),
                        pa.field("price", pa.float64()),
                    ]
                ),
            )
        )
        assert table.scan().read_all().column("price").to_pylist() == [
            None,
            None,
            None,
            1.5,
        ]

    def test_a_scan_is_a_reader_that_knows_its_schema_first(
        self, table: Table
    ) -> None:
        table.append(_rows())

        scan = table.scan()
        assert isinstance(scan, pa.RecordBatchReader)
        assert scan.schema.names == ["id", "venue"]
        assert scan.read_next_batch().num_rows == 1
