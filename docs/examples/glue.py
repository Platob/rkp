"""Build Glue table/DDL values and exercise an injected in-memory client."""

from __future__ import annotations

import copy
from typing import Any

from rkp import GlueCatalog, Record, field, record


@record(schema_name="analytics", table_name="events")
class Event(Record):
    identifier: int = field(primary_key=True)
    day: str = field(partition_key=True)
    payload: str | None = None


class _AlreadyExists(Exception):
    pass


class _NotFound(Exception):
    pass


class MemoryGlueClient:
    """Only the client surface used below; real clients can be injected too."""

    class exceptions:
        AlreadyExistsException = _AlreadyExists
        EntityNotFoundException = _NotFound

    def __init__(self) -> None:
        self.databases: dict[str, dict[str, Any]] = {}
        self.tables: dict[tuple[str, str], dict[str, Any]] = {}

    def create_database(self, *, DatabaseInput: dict[str, Any], **_: Any) -> None:
        name = DatabaseInput["Name"]
        if name in self.databases:
            raise _AlreadyExists(name)
        self.databases[name] = copy.deepcopy(DatabaseInput)

    def get_database(self, *, Name: str, **_: Any) -> dict[str, Any]:
        return {"Database": copy.deepcopy(self.databases[Name])}

    def create_table(
        self, *, DatabaseName: str, TableInput: dict[str, Any], **_: Any
    ) -> None:
        key = (DatabaseName, TableInput["Name"])
        if key in self.tables:
            raise _AlreadyExists(str(key))
        self.tables[key] = copy.deepcopy(TableInput)

    def get_table(self, *, DatabaseName: str, Name: str, **_: Any) -> dict[str, Any]:
        return {"Table": copy.deepcopy(self.tables[(DatabaseName, Name)])}


def main() -> None:
    table_input = Event.into_glue_table_input(
        location="s3://example-bucket/events/",
        format="parquet",
    )
    ddl = Event.into_glue_ddl(
        database="analytics",
        location="s3://example-bucket/events/",
    )
    assert table_input["Name"] == "events"
    assert "CREATE EXTERNAL TABLE IF NOT EXISTS" in ddl

    # Dependency injection makes GlueCatalog testable without credentials/network.
    catalog = GlueCatalog(MemoryGlueClient())
    catalog.create_database("analytics")
    stored = catalog.create_table("analytics", table_input)
    assert stored["Name"] == "events"
    print(ddl)


if __name__ == "__main__":
    main()
