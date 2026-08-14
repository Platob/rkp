"""Create a typed record, coerce input, and inspect portable metadata."""

from __future__ import annotations

from rkp import (
    Record,
    catalog_name,
    field,
    record,
    record_metadata,
    schema_name,
    table_name,
    to_dict,
)


@record(
    alias="users",
    catalog_name="lakehouse",
    schema_name="analytics",
    table_name="user_facts",
    metadata={"owner": "identity"},
)
class User(Record):
    user_id: int = field(
        alias="userId",
        seq=1,
        primary_key=True,
        doc="Stable public identifier",
    )
    name: str = field(index_key=True)
    email: str | None = None


def main() -> None:
    user = User.from_dict({"userId": "7", "name": "Ada"})

    assert user == User(user_id=7, name="Ada")
    assert to_dict(user) == {"userId": 7, "name": "Ada", "email": None}
    assert catalog_name(User) == "lakehouse"
    assert schema_name(User) == "analytics"
    assert table_name(User) == "user_facts"
    assert record_metadata(User).payload_metadata["owner"] == "identity"
    print(user)


if __name__ == "__main__":
    main()
