from __future__ import annotations

from dataclasses import fields, is_dataclass
from inspect import signature

import pytest
from rkp import Field, FieldOptions, Record, field, field_options, record


def test_field_uses_global_ellipsis_for_omitted_configuration() -> None:
    parameters = signature(field).parameters

    for name in (
        "alias",
        "type",
        "nullable",
        "doc",
        "seq",
        "field_id",
        "iceberg_field_id",
        "primary_key",
        "partition_key",
        "index_key",
    ):
        assert parameters[name].default is Ellipsis
    assert "arrow_type" not in parameters
    assert "arrow_metadata" not in parameters
    assert "parameters" not in parameters

    omitted = field()
    assert isinstance(omitted, Field)
    assert omitted.metadata == {}
    assert field_options(omitted).type is Ellipsis


def test_seq_is_the_canonical_stable_field_identity() -> None:
    record_field = field(seq=41)
    options = field_options(record_field)

    assert record_field.metadata == {"rkp": {"seq": 41}}
    assert record_field.seq == 41
    assert options.seq == 41
    # ``field_id`` remains a read-compatible name, but is no longer stored.
    assert options.field_id == 41
    assert "field_id" not in options.config


def test_legacy_field_identity_names_normalize_to_seq() -> None:
    direct = field(field_id=7)
    direct_iceberg = field(iceberg_field_id=8)
    metadata = field(metadata={"rkp": {"iceberg_field_id": 8}})
    matching = field(seq=9, field_id=9)

    assert direct.metadata == {"rkp": {"seq": 7}}
    assert direct_iceberg.metadata == {"rkp": {"seq": 8}}
    assert metadata.metadata == {"rkp": {"seq": 8}}
    assert matching.metadata == {"rkp": {"seq": 9}}

    with pytest.raises(TypeError, match=r"(?i)conflicting.*seq.*field_id"):
        field(seq=9, field_id=10)
    with pytest.raises(TypeError, match=r"(?i)conflicting.*seq.*field_id"):
        field(metadata={"rkp": {"seq": 9, "iceberg_field_id": 10}})


@pytest.mark.parametrize("legacy", [True, 1.0])
def test_every_identity_alias_is_validated_before_comparison(legacy: object) -> None:
    with pytest.raises(TypeError, match=r"(?i)seq.*integer between"):
        field(seq=1, field_id=legacy)  # type: ignore[arg-type]


def test_explicit_none_seq_is_retained_as_an_identity_clear() -> None:
    record_field = field(seq=None)
    options = field_options(record_field)

    assert record_field.metadata == {"rkp": {"seq": None}}
    assert record_field.seq is None
    assert options.seq is None
    assert options.field_id is None
    assert options.has("seq")
    assert options.has("field_id")


def test_explicit_seq_overrides_or_clears_lower_precedence_metadata() -> None:
    overridden = field(metadata={"rkp": {"seq": 40}}, seq=41)
    cleared = field(metadata={"rkp": {"field_id": 40}}, seq=None)

    assert overridden.metadata == {"rkp": {"seq": 41}}
    assert cleared.metadata == {"rkp": {"seq": None}}


@pytest.mark.parametrize("value", [True, False, 0, -1, 1.5, "1"])
def test_seq_rejects_non_positive_or_non_integer_values(value: object) -> None:
    with pytest.raises(TypeError, match=r"(?i)seq.*integer between"):
        field(seq=value)  # type: ignore[arg-type]


def test_field_stores_all_interop_configuration_in_metadata() -> None:
    record_field = field(
        alias="wireValue",
        type="uint64",
        nullable=False,
        doc="Stable public identifier",
        field_id=41,
        primary_key=True,
        partition_key="day",
        index_key=0,
        metadata={"source": "api"},
    )

    assert record_field.metadata == {
        "source": "api",
        "rkp": {
            "alias": "wireValue",
            "type": "uint64",
            "nullable": False,
            "doc": "Stable public identifier",
            "seq": 41,
            "roles": {"primary": True, "partition": "day", "index": 0},
        },
    }
    assert not hasattr(record_field, "arrow_type")
    assert not hasattr(record_field, "arrow_metadata")
    assert not hasattr(record_field, "parameters")


def test_field_options_projects_one_metadata_mapping() -> None:
    metadata = {
        "source": "api",
        "rkp": {
            "alias": "wireValue",
            "type": "uint64",
            "parameters": {"unit": "ms"},
            "nullable": False,
            "doc": "Stable public identifier",
            "field_id": 41,
            "roles": {"primary": True, "partition": "day", "index": 0},
        },
    }
    options = field_options(field(metadata=metadata))

    assert [item.name for item in fields(FieldOptions)] == ["metadata"]
    assert options.type == "uint64"
    assert options.type_parameters == {"unit": "ms"}
    assert options.payload_metadata == {"source": "api"}
    assert options.roles == {"primary": True, "partition": "day", "index": 0}
    assert options.alias == "wireValue"
    assert options.nullable is False
    assert options.doc == "Stable public identifier"
    assert options.field_id == 41
    assert options.primary_key is True
    assert options.partition_key == "day"
    assert options.index_key == 0
    assert not hasattr(options, "arrow_type")
    assert not hasattr(options, "arrow_metadata")
    assert not hasattr(options, "parameters")


def test_explicit_field_values_override_canonical_metadata() -> None:
    metadata = {
        "rkp": {
            "type": "int64",
            "parameters": {"unit": "ms"},
            "roles": {"primary": True, "partition": "day", "index": 1},
        }
    }

    inherited_options = field_options(field(metadata=metadata))
    assert inherited_options.type == "int64"
    assert inherited_options.type_parameters == {"unit": "ms"}
    assert inherited_options.roles == {
        "primary": True,
        "partition": "day",
        "index": 1,
    }

    explicit_options = field_options(
        field(
            metadata=metadata,
            type=None,
            primary_key=False,
            partition_key=False,
            index_key=False,
        )
    )
    assert explicit_options.type is None
    assert explicit_options.type_parameters == {}
    assert explicit_options.roles == {
        "primary": False,
        "partition": False,
        "index": False,
    }


def test_record_field_is_a_real_dataclass_field() -> None:
    @record
    class User(Record):
        identifier: int = field(
            alias="id",
            nullable=False,
            type="uint64",
            doc="Stable identifier",
            field_id=7,
            primary_key=True,
            partition_key="day",
            index_key=0,
            metadata={"source": "api"},
        )

    assert is_dataclass(User)
    dc_field = fields(User)[0]
    assert isinstance(dc_field, Field)
    options = field_options(dc_field)
    assert options.alias == "id"
    assert options.type == "uint64"
    assert options.nullable is False
    assert options.doc == "Stable identifier"
    assert options.field_id == 7
    assert options.payload_metadata == {"source": "api"}
    assert options.roles == {"primary": True, "partition": "day", "index": 0}


@pytest.mark.parametrize(
    "kwargs",
    [
        {"alias": ""},
        {"nullable": "yes"},
        {"doc": object()},
        {"field_id": 0},
        {"primary_key": object()},
        {"metadata": {"rkp": {"type": "int64", "arrow_type": "int32"}}},
    ],
)
def test_field_rejects_invalid_configuration(kwargs: dict[str, object]) -> None:
    with pytest.raises((TypeError, ValueError)):
        field(**kwargs)


def test_standard_dataclass_metadata_uses_the_same_options() -> None:
    from dataclasses import dataclass
    from dataclasses import field as dataclass_field

    @dataclass
    class Standard:
        value: int = dataclass_field(
            metadata={
                "source": "stdlib",
                "rkp": {
                    "alias": "renamed",
                    "nullable": True,
                    "doc": "A standard field",
                    "roles": {"primary": False},
                },
            }
        )

    options = field_options(fields(Standard)[0])
    assert options.alias == "renamed"
    assert options.nullable is True
    assert options.doc == "A standard field"
    assert options.roles == {"primary": False}
    assert options.payload_metadata == {"source": "stdlib"}
