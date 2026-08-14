from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated

import pyarrow as pa
from pyiceberg.types import IntegerType
from rkp import Record, dataclass_from_dict, field, record, to_dict


@record
class InteropChild(Record):
    value: int


@record
class InteropPayload(Record):
    identifier: int = field(alias="id")
    child: InteropChild = field()


def test_dataclass_utility_handles_plain_dataclasses() -> None:
    @dataclass
    class Plain:
        enabled: bool
        child: InteropChild

    assert dataclass_from_dict(
        Plain, {"enabled": "false", "child": {"value": "5"}}
    ) == Plain(False, InteropChild(5))


def test_to_dict_honors_record_field_aliases() -> None:
    value = InteropPayload(identifier=7, child=InteropChild(3))

    assert to_dict(value) == {"id": 7, "child": {"value": 3}}


def test_non_init_state_round_trips() -> None:
    @record
    class Stateful(Record):
        value: int
        cache: dict[str, int] = field(init=False, default_factory=dict)

    source = Stateful(1)
    source.cache["answer"] = 42
    assert Stateful.loads_json(source.dumps_json()) == source


def test_annotated_alias_is_shared_by_codecs_and_arrow() -> None:
    @record
    class AnnotatedAlias(Record):
        value: Annotated[int, {"rkp": {"alias": "wireValue"}}]

    source = AnnotatedAlias(3)
    assert '"wireValue"' in source.dumps_json()
    assert AnnotatedAlias.loads_json('{"wireValue": "4"}') == AnnotatedAlias(4)
    assert AnnotatedAlias.into_arrow_field().type[0].name == "wireValue"


def test_explicit_none_alias_clears_an_annotated_alias_for_every_protocol() -> None:
    @record
    class NativeName(Record):
        value: Annotated[int, {"rkp": {"alias": "wireValue"}}] = field(alias=None)

    source = NativeName(3)
    assert source.dumps_json() == '{"value": 3}'
    assert NativeName.loads_yaml("value: 4\n") == NativeName(4)
    assert NativeName.into_arrow_field().type[0].name == "value"


def test_one_field_contract_drives_codecs_arrow_and_iceberg() -> None:
    @record
    class SharedField(Record):
        value: int = field(
            alias="wireValue",
            type=pa.int32(),
            nullable=False,
            doc="Stable public value",
            field_id=701,
            primary_key=True,
            partition_key="day",
            index_key=2,
            metadata={"source": "api"},
        )

    value = SharedField(7)
    json_text = value.dumps_json()
    yaml_text = value.dumps_yaml()
    assert SharedField.loads_json(json_text) == value
    assert SharedField.loads_yaml(yaml_text) == value
    assert '"wireValue"' in json_text
    assert "wireValue:" in yaml_text
    assert all(
        private not in document
        for document in (json_text, yaml_text)
        for private in ("rkp", "source", "field_id", "primary", "partition", "index")
    )

    arrow_field = SharedField.into_arrow_schema().field("wireValue")
    assert arrow_field.type == pa.int32()
    assert arrow_field.nullable is False
    assert arrow_field.metadata == {
        b"source": b"api",
        b"doc": b"Stable public value",
        b"PARQUET:field_id": b"701",
        b"primary_key": b"true",
        b"partition_key": b"day",
        b"index_key": b"2",
    }
    assert b"rkp" not in arrow_field.metadata

    iceberg_schema = SharedField.into_iceberg_schema()
    iceberg_field = iceberg_schema.find_field("wireValue")
    assert isinstance(iceberg_field.field_type, IntegerType)
    assert iceberg_field.required is True
    assert iceberg_field.doc == "Stable public value"
    assert iceberg_field.field_id == 701
    assert iceberg_schema.identifier_field_ids == [701]
