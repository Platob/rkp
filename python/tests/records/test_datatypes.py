from __future__ import annotations

import datetime as dt
import uuid
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pytest
from rkp.records.datatypes import (
    MISSING,
    DataType,
    FieldSpec,
    TypeKind,
    arrow_fields_into_specs,
    arrow_into_field_spec,
    arrow_type_into_data_type,
    data_type_into_arrow_type,
    default_from_json,
    default_into_json,
    field_spec_into_arrow,
    join_path,
)


@pytest.mark.parametrize(
    ("arrow_type", "kind"),
    [
        (pa.bool_(), TypeKind.BOOLEAN),
        (pa.int8(), TypeKind.INT32),
        (pa.int32(), TypeKind.INT32),
        (pa.int64(), TypeKind.INT64),
        (pa.uint16(), TypeKind.INT32),
        (pa.float16(), TypeKind.FLOAT32),
        (pa.float64(), TypeKind.FLOAT64),
        (pa.string(), TypeKind.STRING),
        (pa.large_string(), TypeKind.STRING),
        (pa.binary(), TypeKind.BINARY),
        (pa.large_binary(), TypeKind.BINARY),
        (pa.date32(), TypeKind.DATE),
        (pa.date64(), TypeKind.DATE),
        (pa.null(), TypeKind.UNKNOWN),
        (pa.binary(16), TypeKind.FIXED),
        (pa.dictionary(pa.int32(), pa.string()), TypeKind.STRING),
    ],
)
def test_scalar_arrow_types_resolve_to_portable_kinds(
    arrow_type: pa.DataType, kind: TypeKind
) -> None:
    assert arrow_type_into_data_type(arrow_type).kind is kind


def test_unsigned_and_narrow_widths_upcast_without_loss() -> None:
    # A uint32 value does not fit in a signed 32-bit column, so it widens.
    assert arrow_type_into_data_type(pa.uint32()).kind is TypeKind.INT64
    assert arrow_type_into_data_type(pa.time32("ms")).unit == "ms"
    assert data_type_into_arrow_type(arrow_type_into_data_type(pa.time32("ms"))) == (
        pa.time64("us")
    )
    assert data_type_into_arrow_type(
        arrow_type_into_data_type(pa.timestamp("s", tz="UTC"))
    ) == pa.timestamp("us", tz="UTC")


def test_parameterized_types_keep_their_parameters() -> None:
    decimal = arrow_type_into_data_type(pa.decimal128(9, 2))
    stamp = arrow_type_into_data_type(pa.timestamp("ns", tz="UTC"))

    assert (decimal.kind, decimal.precision, decimal.scale) == (TypeKind.DECIMAL, 9, 2)
    assert (stamp.unit, stamp.adjusted_to_utc) == ("ns", True)
    assert data_type_into_arrow_type(decimal) == pa.decimal128(9, 2)
    assert data_type_into_arrow_type(stamp) == pa.timestamp("ns", tz="UTC")


def test_nested_types_carry_their_children() -> None:
    arrow_type = pa.struct(
        [
            pa.field("items", pa.list_(pa.field("element", pa.string()))),
            pa.field(
                "lookup",
                pa.map_(
                    pa.field("key", pa.string(), nullable=False),
                    pa.field("value", pa.int64()),
                ),
            ),
        ]
    )

    data_type = arrow_type_into_data_type(arrow_type, path="root")

    assert data_type.kind is TypeKind.STRUCT
    assert [child.name for child in data_type.children] == ["items", "lookup"]
    items, lookup = data_type.fields
    assert items.data_type.kind is TypeKind.LIST
    assert items.data_type.children[0].name == "element"
    assert lookup.data_type.kind is TypeKind.MAP
    assert [child.name for child in lookup.data_type.children] == ["key", "value"]
    assert data_type_into_arrow_type(data_type) == arrow_type


def test_large_projection_matches_the_iceberg_arrow_representation() -> None:
    data_type = arrow_type_into_data_type(pa.list_(pa.field("element", pa.string())))

    assert data_type_into_arrow_type(data_type, large_types=True) == pa.large_list(
        pa.field("element", pa.large_string())
    )
    assert (
        data_type_into_arrow_type(DataType(TypeKind.BINARY), large_types=True)
        == pa.large_binary()
    )


def test_field_specs_project_identity_documentation_and_roles() -> None:
    field = pa.field(
        "identifier",
        pa.int64(),
        nullable=False,
        metadata={
            b"PARQUET:field_id": b"12",
            b"doc": b"stable",
            b"primary_key": b"true",
            b"owner": b"rkp",
        },
    )

    spec = arrow_into_field_spec(field)

    assert (spec.name, spec.field_id, spec.doc) == ("identifier", 12, "stable")
    assert spec.required is True and spec.optional is False
    assert spec.primary_key is True
    assert spec.metadata[b"owner"] == b"rkp"

    restored = field_spec_into_arrow(spec)
    assert restored.equals(field, check_metadata=True)
    without_identity = field_spec_into_arrow(
        spec, include_field_ids=False, include_primary_keys=False
    )
    assert without_identity.metadata == {b"doc": b"stable", b"owner": b"rkp"}


def test_specs_are_replaceable_and_paths_are_shared() -> None:
    spec = arrow_into_field_spec(pa.field("value", pa.int64()))

    assert spec.replace(name="renamed").name == "renamed"
    assert join_path("", "root") == "root"
    assert join_path("root", "child") == "root.child"
    assert [
        item.name for item in arrow_fields_into_specs([pa.field("a", pa.int64())])
    ] == ["a"]


@pytest.mark.parametrize(
    ("data_type", "value", "text"),
    [
        (DataType(TypeKind.BOOLEAN), True, "true"),
        (DataType(TypeKind.INT64), 5, "5"),
        (DataType(TypeKind.FLOAT64), 1.5, "1.5"),
        (DataType(TypeKind.STRING), "text", '"text"'),
        (DataType(TypeKind.DECIMAL, precision=9, scale=2), Decimal("1.25"), '"1.25"'),
        (DataType(TypeKind.DATE), dt.date(2026, 8, 17), '"2026-08-17"'),
        (DataType(TypeKind.TIME, unit="us"), dt.time(1, 2, 3), '"01:02:03"'),
        (
            DataType(TypeKind.TIMESTAMP, unit="us"),
            dt.datetime(2026, 8, 17, 1, 2, 3),  # noqa: DTZ001 - naive column
            '"2026-08-17T01:02:03"',
        ),
        (
            DataType(TypeKind.UUID),
            uuid.UUID("3f2504e0-4f89-11d3-9a0c-0305e82c3301"),
            '"3f2504e0-4f89-11d3-9a0c-0305e82c3301"',
        ),
        (DataType(TypeKind.BINARY), b"\x00\x01", '"0001"'),
    ],
)
def test_default_values_use_single_value_json(
    data_type: DataType, value: Any, text: str
) -> None:
    encoded = default_into_json(data_type, value)

    assert encoded == text
    assert default_from_json(data_type, encoded) == value


def test_defaults_round_trip_through_arrow_metadata() -> None:
    spec = FieldSpec(
        name="count",
        data_type=DataType(TypeKind.INT64),
        required=False,
        field_id=3,
        default=7,
        write_default=9,
    )

    field = field_spec_into_arrow(spec)
    assert field.metadata[b"iceberg.initial_default"] == b"7"
    assert field.metadata[b"iceberg.write_default"] == b"9"

    restored = arrow_into_field_spec(field)
    assert (restored.default, restored.write_default) == (7, 9)
    assert restored.has_default is True
    assert arrow_into_field_spec(pa.field("x", pa.int64())).default is MISSING


def test_invalid_models_and_types_are_rejected() -> None:
    with pytest.raises(ValueError, match="decimal precision"):
        DataType(TypeKind.DECIMAL, precision=0, scale=0)
    with pytest.raises(ValueError, match="decimal scale"):
        DataType(TypeKind.DECIMAL, precision=4, scale=9)
    with pytest.raises(ValueError, match="unit must be one of"):
        DataType(TypeKind.TIMESTAMP, unit="weeks")
    with pytest.raises(ValueError, match="list types require"):
        DataType(TypeKind.LIST)
    with pytest.raises(TypeError, match="kind must be a TypeKind"):
        DataType("string")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="field names must be"):
        FieldSpec("", DataType(TypeKind.STRING))
    with pytest.raises(TypeError, match="unsupported type"):
        arrow_type_into_data_type(pa.duration("us"), path="span")
    with pytest.raises(TypeError, match="unsupported type"):
        arrow_type_into_data_type(pa.uint64())
    with pytest.raises(ValueError, match="invalid iceberg.initial_default"):
        arrow_into_field_spec(
            pa.field(
                "count",
                pa.int64(),
                metadata={b"iceberg.initial_default": b'"text"'},
            )
        )
    assert TypeKind.STRUCT.is_nested and not TypeKind.STRING.is_nested
