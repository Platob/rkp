"""Apache Avro interoperability for records, dataclasses, and Arrow.

Conversion runs through the neutral field model in :mod:`rkp.records.datatypes`
so Avro, Arrow, and Iceberg agree on identity, nullability, and precision.  The
``"iceberg"`` flavor emits exactly the Avro representation Iceberg uses for its
schemas: ``field-id`` attributes, fixed-backed decimals and UUIDs, and explicit
``adjust-to-utc`` timestamps.
"""

from __future__ import annotations

import dataclasses
import math
from collections.abc import Hashable, Iterable, Iterator, Mapping, Sequence
from functools import cache
from typing import Any, Literal, TypeVar, cast

import pyarrow as pa

from ..avro import (
    ArraySchema,
    Avro,
    AvroField,
    AvroSchema,
    AvroSchemaError,
    EnumSchema,
    FixedSchema,
    MapSchema,
    PrimitiveSchema,
    RecordSchema,
    UnionSchema,
    into_json,
    parse_schema,
    read_container,
)
from ._metadata import TABLE_NAME, metadata_name, normalize_metadata
from .arrow import (
    into_arrow_schema,
    record_into_arrow_schema,
    record_into_native_mapping,
)
from .datatypes import (
    MISSING,
    DataType,
    FieldSpec,
    TypeKind,
    arrow_into_field_spec,
    field_spec_into_arrow,
    join_path,
)
from .interop import dataclass_from_dict, is_record_type

__all__ = [
    "FLAVORS",
    "arrow_into_avro_field",
    "arrow_into_avro_schema",
    "avro_into_arrow_field",
    "avro_into_arrow_schema",
    "avro_into_field_specs",
    "avro_into_records",
    "dataclass_into_avro_schema",
    "field_spec_into_avro_field",
    "field_specs_into_avro_schema",
    "into_avro_schema",
    "record_into_avro_schema",
    "records_into_avro",
]

T = TypeVar("T")

Flavor = Literal["standard", "iceberg"]
FLAVORS: tuple[str, ...] = ("standard", "iceberg")

_FIELD_ID = "field-id"
_ELEMENT_ID = "element-id"
_KEY_ID = "key-id"
_VALUE_ID = "value-id"
_PRIMARY_KEY = "primary-key"
_ADJUST_TO_UTC = "adjust-to-utc"
_MAP_LOGICAL = "map"

_TIMESTAMP_LOGICAL = {
    "s": "timestamp-millis",
    "ms": "timestamp-millis",
    "us": "timestamp-micros",
    "ns": "timestamp-nanos",
}
_AVRO_PRIMITIVE_KINDS: Mapping[str, DataType] = {
    "boolean": DataType(TypeKind.BOOLEAN),
    "int": DataType(TypeKind.INT32),
    "long": DataType(TypeKind.INT64),
    "float": DataType(TypeKind.FLOAT32),
    "double": DataType(TypeKind.FLOAT64),
    "bytes": DataType(TypeKind.BINARY),
    "string": DataType(TypeKind.STRING),
    "null": DataType(TypeKind.UNKNOWN),
}


def into_avro_schema(
    value: Any,
    *,
    name: str | None = None,
    namespace: str | None = None,
    doc: str | None = None,
    flavor: Flavor = "standard",
    include_field_ids: bool = True,
    localns: Mapping[str, Any] | None = None,
) -> RecordSchema:
    """Return an Avro record schema for any RKP-convertible value.

    Records, ordinary dataclasses, Arrow schemas/fields, and already-parsed
    Avro declarations all resolve through the same neutral field model.
    """

    if isinstance(value, AvroSchema):
        if not isinstance(value, RecordSchema):
            raise TypeError("into_avro_schema expects a record-shaped Avro schema")
        return value
    if isinstance(value, (str, bytes, bytearray, Mapping)):
        parsed = parse_schema(value)
        if not isinstance(parsed, RecordSchema):
            raise TypeError("into_avro_schema expects a record-shaped Avro schema")
        return parsed
    if (
        localns is None
        and isinstance(value, type)
        and is_record_type(value)
        and name is None
        and namespace is None
        and doc is None
    ):
        return record_into_avro_schema(
            cast(Hashable, value),
            flavor=flavor,
            include_field_ids=include_field_ids,
        )
    arrow_schema = (
        value
        if isinstance(value, pa.Schema)
        else into_arrow_schema(value, localns=localns)
    )
    return arrow_into_avro_schema(
        arrow_schema,
        name=name,
        namespace=namespace,
        doc=doc,
        flavor=flavor,
        include_field_ids=include_field_ids,
    )


def arrow_into_avro_schema(
    schema: pa.Schema,
    *,
    name: str | None = None,
    namespace: str | None = None,
    doc: str | None = None,
    flavor: Flavor = "standard",
    include_field_ids: bool = True,
) -> RecordSchema:
    """Convert an Arrow schema into one Avro record schema."""

    if not isinstance(schema, pa.Schema):
        raise TypeError("arrow_into_avro_schema expects a pyarrow.Schema")
    resolved = name
    if resolved is None:
        resolved = metadata_name(schema.metadata or {}, TABLE_NAME) or "record"
    return field_specs_into_avro_schema(
        tuple(arrow_into_field_spec(field) for field in schema),
        name=resolved,
        namespace=namespace,
        doc=doc,
        flavor=flavor,
        include_field_ids=include_field_ids,
    )


def arrow_into_avro_field(
    field: pa.Field,
    *,
    namespace: str | None = None,
    flavor: Flavor = "standard",
    include_field_ids: bool = True,
) -> AvroField:
    """Convert one Arrow field into an Avro record field."""

    if not isinstance(field, pa.Field):
        raise TypeError("arrow_into_avro_field expects a pyarrow.Field")
    return field_spec_into_avro_field(
        arrow_into_field_spec(field),
        namespace=namespace,
        flavor=flavor,
        include_field_ids=include_field_ids,
    )


def dataclass_into_avro_schema(
    dataclass_type: type[Any],
    *,
    name: str | None = None,
    namespace: str | None = None,
    flavor: Flavor = "standard",
    include_field_ids: bool = True,
    localns: Mapping[str, Any] | None = None,
) -> RecordSchema:
    """Infer an Avro record schema from an ordinary dataclass type."""

    if not isinstance(dataclass_type, type) or not dataclasses.is_dataclass(
        dataclass_type
    ):
        raise TypeError("dataclass_into_avro_schema expects a dataclass type")
    return arrow_into_avro_schema(
        into_arrow_schema(dataclass_type, localns=localns),
        name=name,
        namespace=namespace,
        flavor=flavor,
        include_field_ids=include_field_ids,
    )


@cache
def record_into_avro_schema(
    record_type: type[Any],
    *,
    flavor: Flavor = "standard",
    include_field_ids: bool = True,
) -> RecordSchema:
    """Return a cached Avro record schema for a decorated record type."""

    if not is_record_type(record_type):
        raise TypeError("record_into_avro_schema expects a decorated record type")
    return arrow_into_avro_schema(
        record_into_arrow_schema(cast(Hashable, record_type)),
        flavor=flavor,
        include_field_ids=include_field_ids,
    )


def avro_into_arrow_schema(
    schema: Any,
    *,
    metadata: Mapping[str | bytes, Any] | None = None,
    large_types: bool = False,
) -> pa.Schema:
    """Convert an Avro record schema into an Arrow schema."""

    parsed = parse_schema(schema)
    if not isinstance(parsed, RecordSchema):
        raise TypeError("avro_into_arrow_schema expects a record-shaped Avro schema")
    schema_metadata = normalize_metadata(dict(metadata or {}))
    schema_metadata.setdefault(TABLE_NAME, parsed.name.encode("utf-8"))
    return pa.schema(
        [
            field_spec_into_arrow(spec, large_types=large_types)
            for spec in avro_into_field_specs(parsed)
        ],
        metadata=schema_metadata,
    )


def avro_into_arrow_field(field: AvroField, *, large_types: bool = False) -> pa.Field:
    """Convert one Avro record field into an Arrow field."""

    if not isinstance(field, AvroField):
        raise TypeError("avro_into_arrow_field expects an AvroField")
    return field_spec_into_arrow(
        avro_field_into_field_spec(field), large_types=large_types
    )


def field_specs_into_avro_schema(
    specs: Sequence[FieldSpec],
    *,
    name: str,
    namespace: str | None = None,
    doc: str | None = None,
    flavor: Flavor = "standard",
    include_field_ids: bool = True,
) -> RecordSchema:
    """Compose neutral field specs into one Avro record schema."""

    _validate_flavor(flavor)
    return RecordSchema(
        declared_name=_avro_name(name),
        namespace=namespace,
        doc=doc,
        fields=tuple(
            field_spec_into_avro_field(
                spec,
                namespace=namespace,
                flavor=flavor,
                include_field_ids=include_field_ids,
                path=_avro_name(name),
            )
            for spec in specs
        ),
    )


def field_spec_into_avro_field(
    spec: FieldSpec,
    *,
    namespace: str | None = None,
    flavor: Flavor = "standard",
    include_field_ids: bool = True,
    path: str = "",
) -> AvroField:
    """Convert one neutral field spec into an Avro record field."""

    _validate_flavor(flavor)
    if not isinstance(spec, FieldSpec):
        raise TypeError("field_spec_into_avro_field expects a FieldSpec")
    field_path = join_path(path, spec.name)
    encoded = _data_type_into_avro(
        spec.data_type,
        namespace=namespace,
        flavor=flavor,
        include_field_ids=include_field_ids,
        path=field_path,
        field_id=spec.field_id,
    )
    attributes: dict[str, Any] = {}
    if include_field_ids and spec.field_id is not None:
        attributes[_FIELD_ID] = spec.field_id
    if spec.primary_key:
        # Avro has no identifier concept, so RKP's role travels as an ordinary
        # attribute.  Canonical form and fingerprints ignore it.
        attributes[_PRIMARY_KEY] = True
    default: Any = MISSING
    if spec.default is not MISSING and spec.default is not None:
        # Avro defaults must match the first union branch, so a concrete
        # default reorders the optional union instead of being dropped.
        default = into_json(encoded, spec.default)
        if not spec.required:
            encoded = UnionSchema((encoded, PrimitiveSchema("null")))
    elif not spec.required:
        encoded = _optional(encoded)
        default = None
    return AvroField(
        name=_avro_name(spec.name),
        type=encoded,
        default=default,
        doc=spec.doc,
        attributes=attributes,
    )


def avro_into_field_specs(schema: Any) -> tuple[FieldSpec, ...]:
    """Convert an Avro record schema into neutral field specs."""

    parsed = parse_schema(schema)
    if not isinstance(parsed, RecordSchema):
        raise TypeError("avro_into_field_specs expects a record-shaped Avro schema")
    return tuple(avro_field_into_field_spec(field) for field in parsed.fields)


def avro_field_into_field_spec(field: AvroField) -> FieldSpec:
    """Convert one Avro record field into the neutral model."""

    if not isinstance(field, AvroField):
        raise TypeError("avro_field_into_field_spec expects an AvroField")
    schema, required = _strip_optional(field.type)
    return FieldSpec(
        name=field.name,
        data_type=_avro_into_data_type(schema),
        required=required,
        field_id=_attribute_id(field.attributes, _FIELD_ID),
        doc=field.doc,
        primary_key=bool(field.attributes.get(_PRIMARY_KEY, False)),
    )


def records_into_avro(
    records: Iterable[Any],
    *,
    record_type: type[Any] | None = None,
    schema: Any = None,
    codec: str = "null",
    metadata: Mapping[str, Any] | None = None,
    sync_marker: bytes | None = None,
) -> bytes:
    """Encode records as an Avro object container file.

    Rows keep their native Python values, so timestamps, decimals, and UUIDs
    are encoded through Avro's logical types rather than through text.
    """

    iterator = iter(records)
    selected_type = record_type
    first: Any = None
    has_first = False
    if schema is None and selected_type is None:
        try:
            first = next(iterator)
        except StopIteration as exc:
            raise TypeError("empty records require record_type or schema") from exc
        has_first = True
        if not dataclasses.is_dataclass(first) or isinstance(first, type):
            raise TypeError("cannot infer record_type; pass record_type or schema")
        selected_type = type(first)
    avro_schema = (
        parse_schema(schema)
        if schema is not None
        else into_avro_schema(cast(type[Any], selected_type))
    )
    container = Avro.create(
        avro_schema,
        codec=codec,
        metadata=metadata,
        sync_marker=sync_marker,
    )
    if has_first:
        container.append(_avro_row(first, selected_type))
    for item in iterator:
        container.append(_avro_row(item, selected_type))
    image = container.into_bytes()
    container.close()
    return image


def avro_into_records(
    record_type: type[T],
    source: Any,
    *,
    schema: Any = None,
    safe: bool = True,
    on_error: Literal["raise", "default"] = "raise",
    start: int = 0,
    stop: int | None = None,
) -> Iterator[T]:
    """Lazily construct records from an Avro object container file.

    ``start`` and ``stop`` select a record range without decoding the blocks
    before it, which is what makes a partial read of a large container cheap.
    """

    if not isinstance(record_type, type) or not is_record_type(record_type):
        raise TypeError("record_type must be a decorated record type")

    def converted() -> Iterator[T]:
        container = read_container(source, schema=schema)
        rows = (
            container.iter_from(start, stop)
            if start or stop is not None
            else iter(container)
        )
        for index, row in enumerate(rows, start=start):
            try:
                yield dataclass_from_dict(
                    record_type,
                    row,
                    safe=safe,
                    on_error=on_error,
                )
            except (TypeError, ValueError, OverflowError) as exc:
                raise TypeError(
                    f"cannot construct {record_type.__qualname__} "
                    f"from Avro row {index}: {exc}"
                ) from exc

    return converted()


def _avro_row(value: Any, record_type: type[Any] | None) -> Any:
    if isinstance(value, Mapping):
        return value
    if record_type is not None and not isinstance(value, record_type):
        raise TypeError(
            f"all records must be {record_type.__qualname__}; "
            f"got {type(value).__qualname__}"
        )
    return record_into_native_mapping(value)


def _validate_flavor(flavor: str) -> None:
    if flavor not in FLAVORS:
        raise ValueError("flavor must be 'standard' or 'iceberg'")


def _optional(schema: AvroSchema) -> AvroSchema:
    if isinstance(schema, UnionSchema):
        if schema.is_optional:
            return schema
        return UnionSchema((PrimitiveSchema("null"), *schema.options))
    if isinstance(schema, PrimitiveSchema) and schema.primitive == "null":
        return schema
    return UnionSchema((PrimitiveSchema("null"), schema))


def _strip_optional(schema: AvroSchema) -> tuple[AvroSchema, bool]:
    if not isinstance(schema, UnionSchema):
        return schema, True
    concrete = tuple(
        option
        for option in schema.options
        if not (isinstance(option, PrimitiveSchema) and option.primitive == "null")
    )
    optional = len(concrete) != len(schema.options)
    if not concrete:
        return PrimitiveSchema("null"), not optional
    if len(concrete) == 1:
        return concrete[0], not optional
    return UnionSchema(concrete), not optional


def _data_type_into_avro(
    data_type: DataType,
    *,
    namespace: str | None,
    flavor: Flavor,
    include_field_ids: bool,
    path: str,
    field_id: int | None,
) -> AvroSchema:
    kind = data_type.kind
    if kind is TypeKind.BOOLEAN:
        return PrimitiveSchema("boolean")
    if kind is TypeKind.INT32:
        return PrimitiveSchema("int")
    if kind is TypeKind.INT64:
        return PrimitiveSchema("long")
    if kind is TypeKind.FLOAT32:
        return PrimitiveSchema("float")
    if kind is TypeKind.FLOAT64:
        return PrimitiveSchema("double")
    if kind is TypeKind.STRING:
        return PrimitiveSchema("string")
    if kind is TypeKind.BINARY:
        return PrimitiveSchema("bytes")
    if kind is TypeKind.UNKNOWN:
        return PrimitiveSchema("null")
    if kind is TypeKind.DATE:
        return PrimitiveSchema("int", logical="date")
    if kind is TypeKind.TIME:
        return PrimitiveSchema("long", logical="time-micros")
    if kind is TypeKind.TIMESTAMP:
        logical = _TIMESTAMP_LOGICAL[data_type.unit or "us"]
        if flavor == "iceberg":
            return PrimitiveSchema(
                "long",
                logical=logical,
                attributes={_ADJUST_TO_UTC: data_type.adjusted_to_utc},
            )
        if not data_type.adjusted_to_utc:
            logical = f"local-{logical}"
        return PrimitiveSchema("long", logical=logical)
    if kind is TypeKind.DECIMAL:
        precision = int(data_type.precision or 0)
        scale = int(data_type.scale or 0)
        if flavor == "iceberg":
            return FixedSchema(
                declared_name=_type_name("decimal", path, field_id),
                namespace=namespace,
                size=_decimal_size(precision),
                logical="decimal",
                precision=precision,
                scale=scale,
            )
        return PrimitiveSchema(
            "bytes", logical="decimal", precision=precision, scale=scale
        )
    if kind is TypeKind.UUID:
        if flavor == "iceberg":
            return FixedSchema(
                declared_name=_type_name("uuid", path, field_id),
                namespace=namespace,
                size=16,
                logical="uuid",
            )
        return PrimitiveSchema("string", logical="uuid")
    if kind is TypeKind.FIXED:
        return FixedSchema(
            declared_name=_type_name("fixed", path, field_id),
            namespace=namespace,
            size=int(data_type.length or 0),
        )

    def nested_field(spec: FieldSpec) -> AvroField:
        return field_spec_into_avro_field(
            spec,
            namespace=namespace,
            flavor=flavor,
            include_field_ids=include_field_ids,
            path=path,
        )

    if kind is TypeKind.STRUCT:
        return RecordSchema(
            declared_name=_type_name("r", path, field_id),
            namespace=namespace,
            fields=tuple(nested_field(child) for child in data_type.fields),
        )
    if kind is TypeKind.LIST:
        element = cast(FieldSpec, data_type.element)
        items = _data_type_into_avro(
            element.data_type,
            namespace=namespace,
            flavor=flavor,
            include_field_ids=include_field_ids,
            path=join_path(path, "element"),
            field_id=element.field_id,
        )
        attributes: dict[str, Any] = {}
        if include_field_ids and element.field_id is not None:
            attributes[_ELEMENT_ID] = element.field_id
        return ArraySchema(
            _optional(items) if not element.required else items,
            attributes=attributes,
        )

    key = cast(FieldSpec, data_type.key)
    value = cast(FieldSpec, data_type.value)
    value_schema = _data_type_into_avro(
        value.data_type,
        namespace=namespace,
        flavor=flavor,
        include_field_ids=include_field_ids,
        path=join_path(path, "value"),
        field_id=value.field_id,
    )
    if not value.required:
        value_schema = _optional(value_schema)
    if key.data_type.kind is TypeKind.STRING:
        attributes = {}
        if include_field_ids:
            if key.field_id is not None:
                attributes[_KEY_ID] = key.field_id
            if value.field_id is not None:
                attributes[_VALUE_ID] = value.field_id
        return MapSchema(value_schema, attributes=attributes)
    # Avro map keys are always strings, so non-string keys use Iceberg's
    # array-of-pairs representation, which stays lossless in both directions.
    key_schema = _data_type_into_avro(
        key.data_type,
        namespace=namespace,
        flavor=flavor,
        include_field_ids=include_field_ids,
        path=join_path(path, "key"),
        field_id=key.field_id,
    )
    pair = RecordSchema(
        declared_name=_type_name("k", path, key.field_id),
        namespace=namespace,
        fields=(
            AvroField(
                name="key",
                type=key_schema,
                attributes=(
                    {_FIELD_ID: key.field_id}
                    if include_field_ids and key.field_id is not None
                    else {}
                ),
            ),
            AvroField(
                name="value",
                type=value_schema,
                default=None if not value.required else MISSING,
                attributes=(
                    {_FIELD_ID: value.field_id}
                    if include_field_ids and value.field_id is not None
                    else {}
                ),
            ),
        ),
    )
    return ArraySchema(pair, attributes={"logicalType": _MAP_LOGICAL})


def _avro_into_data_type(schema: AvroSchema) -> DataType:
    if isinstance(schema, PrimitiveSchema):
        return _avro_primitive_into_data_type(schema)
    if isinstance(schema, EnumSchema):
        return DataType(TypeKind.STRING)
    if isinstance(schema, FixedSchema):
        if schema.logical == "uuid" and schema.size == 16:
            return DataType(TypeKind.UUID)
        if schema.logical == "decimal" and schema.precision:
            return DataType(
                TypeKind.DECIMAL,
                precision=schema.precision,
                scale=schema.scale or 0,
            )
        return DataType(TypeKind.FIXED, length=schema.size)
    if isinstance(schema, ArraySchema):
        if schema.attributes.get("logicalType") == _MAP_LOGICAL and isinstance(
            schema.items, RecordSchema
        ):
            return _pairs_into_map(schema.items)
        items, required = _strip_optional(schema.items)
        return DataType(
            TypeKind.LIST,
            element=FieldSpec(
                name="element",
                data_type=_avro_into_data_type(items),
                required=required,
                field_id=_attribute_id(schema.attributes, _ELEMENT_ID),
            ),
        )
    if isinstance(schema, MapSchema):
        values, required = _strip_optional(schema.values)
        return DataType(
            TypeKind.MAP,
            key=FieldSpec(
                name="key",
                data_type=DataType(TypeKind.STRING),
                required=True,
                field_id=_attribute_id(schema.attributes, _KEY_ID),
            ),
            value=FieldSpec(
                name="value",
                data_type=_avro_into_data_type(values),
                required=required,
                field_id=_attribute_id(schema.attributes, _VALUE_ID),
            ),
        )
    if isinstance(schema, RecordSchema):
        return DataType(
            TypeKind.STRUCT,
            fields=tuple(avro_field_into_field_spec(field) for field in schema.fields),
        )
    if isinstance(schema, UnionSchema):
        raise AvroSchemaError(
            "Arrow conversion supports optional unions only; "
            f"got {len(schema.options)} concrete branches"
        )
    raise AvroSchemaError(f"unsupported Avro schema {schema!r}")


def _avro_primitive_into_data_type(schema: PrimitiveSchema) -> DataType:
    logical = schema.logical
    if logical == "date":
        return DataType(TypeKind.DATE)
    if logical in {"time-millis", "time-micros"}:
        return DataType(TypeKind.TIME, unit="us")
    if logical is not None and "timestamp" in logical:
        base = logical.removeprefix("local-")
        unit = {
            "timestamp-millis": "ms",
            "timestamp-micros": "us",
            "timestamp-nanos": "ns",
        }[base]
        adjusted = not logical.startswith("local-")
        raw = schema.attributes.get(_ADJUST_TO_UTC)
        if raw is not None:
            adjusted = bool(raw)
        return DataType(TypeKind.TIMESTAMP, unit=unit, adjusted_to_utc=adjusted)
    if logical in {"decimal", "big-decimal"} and schema.precision:
        return DataType(
            TypeKind.DECIMAL,
            precision=schema.precision,
            scale=schema.scale or 0,
        )
    if logical == "uuid":
        return DataType(TypeKind.UUID)
    resolved = _AVRO_PRIMITIVE_KINDS.get(schema.primitive)
    if resolved is None:  # pragma: no cover - the parser rejects other names
        raise AvroSchemaError(f"unsupported Avro primitive {schema.primitive!r}")
    return resolved


def _pairs_into_map(pair: RecordSchema) -> DataType:
    names = [field.name for field in pair.fields]
    if names != ["key", "value"]:
        raise AvroSchemaError(
            "array-backed Avro maps require exactly 'key' and 'value' fields"
        )
    key_field, value_field = pair.fields
    key_schema, _ = _strip_optional(key_field.type)
    value_schema, value_required = _strip_optional(value_field.type)
    return DataType(
        TypeKind.MAP,
        key=FieldSpec(
            name="key",
            data_type=_avro_into_data_type(key_schema),
            required=True,
            field_id=_attribute_id(key_field.attributes, _FIELD_ID),
        ),
        value=FieldSpec(
            name="value",
            data_type=_avro_into_data_type(value_schema),
            required=value_required,
            field_id=_attribute_id(value_field.attributes, _FIELD_ID),
        ),
    )


def _attribute_id(attributes: Mapping[str, Any], key: str) -> int | None:
    raw = attributes.get(key)
    if raw is None:
        return None
    if isinstance(raw, bool) or not isinstance(raw, int):
        try:
            return int(str(raw))
        except ValueError as exc:
            raise AvroSchemaError(f"invalid Avro {key} attribute {raw!r}") from exc
    return raw


def _decimal_size(precision: int) -> int:
    """Return Iceberg's minimum fixed width for a decimal precision."""

    return math.ceil((precision * math.log2(10) + 1) / 8)


def _type_name(prefix: str, path: str, field_id: int | None) -> str:
    if field_id is not None:
        return f"{prefix}{field_id}"
    sanitized = "_".join(_avro_name(part) for part in path.split(".") if part)
    return f"{prefix}_{sanitized}" if sanitized else prefix


def _avro_name(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise AvroSchemaError("Avro names must be non-empty strings")
    characters = [
        character if character.isalnum() or character == "_" else "_"
        for character in value
    ]
    if characters[0].isdigit():
        characters.insert(0, "_")
    return "".join(characters)
