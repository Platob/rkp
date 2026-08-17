"""Apache Avro schema model, parser, canonical form, and fingerprints.

The model is intentionally dependency-free and immutable.  Every schema is
parsed once into hashable objects that the binary, JSON, and container codecs
compile against, so repeated encoding never re-reads the JSON declaration.
"""

from __future__ import annotations

import dataclasses
import re
from collections.abc import Iterable, Iterator, Mapping, Sequence
from types import MappingProxyType
from typing import Any, ClassVar

from ._errors import AvroSchemaError

__all__ = [
    "ArraySchema",
    "AvroField",
    "AvroSchema",
    "EnumSchema",
    "FixedSchema",
    "MapSchema",
    "NamedSchema",
    "PrimitiveSchema",
    "RecordSchema",
    "UnionSchema",
    "canonical_form",
    "fingerprint",
    "fingerprint_bytes",
    "parse_schema",
    "schema_into_json",
]

PRIMITIVE_NAMES = (
    "null",
    "boolean",
    "int",
    "long",
    "float",
    "double",
    "bytes",
    "string",
)

MISSING: Any = ...

_EMPTY: Mapping[str, Any] = MappingProxyType({})
_NAME_PART = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_RESERVED_ATTRIBUTES = frozenset(
    {
        "aliases",
        "default",
        "doc",
        "fields",
        "items",
        "logicalType",
        "name",
        "namespace",
        "precision",
        "scale",
        "size",
        "symbols",
        "type",
        "values",
    }
)
# Logical annotations recognized by the codecs.  Anything else is preserved as
# an ordinary attribute, which is what the specification requires readers to do.
_LOGICAL_TYPES: Mapping[str, tuple[str, ...]] = MappingProxyType(
    {
        "decimal": ("bytes", "fixed"),
        "big-decimal": ("bytes",),
        "uuid": ("string", "fixed"),
        "date": ("int",),
        "time-millis": ("int",),
        "time-micros": ("long",),
        "timestamp-millis": ("long",),
        "timestamp-micros": ("long",),
        "timestamp-nanos": ("long",),
        "local-timestamp-millis": ("long",),
        "local-timestamp-micros": ("long",),
        "local-timestamp-nanos": ("long",),
        "duration": ("fixed",),
    }
)
_CANONICAL_ORDER = ("name", "type", "fields", "symbols", "items", "values", "size")


@dataclasses.dataclass(frozen=True, eq=False)
class AvroSchema:
    """Base class for every parsed Avro schema node."""

    type_name: ClassVar[str] = ""

    def __post_init__(self) -> None:
        pass

    @property
    def name(self) -> str:
        """Return the schema's declared or primitive type name."""

        return self.type_name

    @property
    def fullname(self) -> str:
        """Return the fully qualified name used by canonical form."""

        return self.type_name

    @property
    def logical_type(self) -> str | None:
        """Return the recognized logical annotation, when present."""

        return None

    def into_json(self) -> Any:
        """Return the JSON-compatible declaration for this schema."""

        return schema_into_json(self)

    def canonical_form(self) -> str:
        """Return the specification's parsing canonical form."""

        return canonical_form(self)

    def fingerprint(self) -> int:
        """Return the 64-bit Rabin fingerprint of the canonical form."""

        return fingerprint(self)

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if not isinstance(other, AvroSchema):
            return NotImplemented
        return self.canonical_form() == other.canonical_form()

    def __hash__(self) -> int:
        return hash(self.canonical_form())


@dataclasses.dataclass(frozen=True, eq=False)
class PrimitiveSchema(AvroSchema):
    """One of Avro's eight primitive types with optional logical metadata."""

    primitive: str
    logical: str | None = None
    precision: int | None = None
    scale: int | None = None
    attributes: Mapping[str, Any] = dataclasses.field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.primitive not in PRIMITIVE_NAMES:
            raise AvroSchemaError(f"unknown Avro primitive type {self.primitive!r}")
        object.__setattr__(self, "attributes", _freeze(self.attributes))

    @property
    def type_name(self) -> str:  # type: ignore[override]
        return self.primitive

    @property
    def name(self) -> str:
        return self.primitive

    @property
    def fullname(self) -> str:
        return self.primitive

    @property
    def logical_type(self) -> str | None:
        return self.logical


@dataclasses.dataclass(frozen=True, eq=False)
class NamedSchema(AvroSchema):
    """Base class for Avro's named types: record, enum, and fixed."""

    declared_name: str = ""
    namespace: str | None = None
    aliases: tuple[str, ...] = ()
    doc: str | None = None
    attributes: Mapping[str, Any] = dataclasses.field(default_factory=dict)

    def __post_init__(self) -> None:
        _validate_name(self.declared_name)
        if self.namespace is not None:
            _validate_namespace(self.namespace)
        object.__setattr__(self, "aliases", tuple(self.aliases))
        for alias in self.aliases:
            _validate_fullname(alias)
        object.__setattr__(self, "attributes", _freeze(self.attributes))

    @property
    def name(self) -> str:
        return self.declared_name

    @property
    def fullname(self) -> str:
        if "." in self.declared_name:
            return self.declared_name
        if self.namespace:
            return f"{self.namespace}.{self.declared_name}"
        return self.declared_name


@dataclasses.dataclass(frozen=True, eq=False)
class FixedSchema(NamedSchema):
    """A fixed-size byte sequence, optionally carrying a logical type."""

    type_name: ClassVar[str] = "fixed"
    size: int = 0
    logical: str | None = None
    precision: int | None = None
    scale: int | None = None

    def __post_init__(self) -> None:
        NamedSchema.__post_init__(self)
        if type(self.size) is not int or self.size < 0:
            raise AvroSchemaError("fixed size must be a non-negative integer")

    @property
    def logical_type(self) -> str | None:
        return self.logical


@dataclasses.dataclass(frozen=True, eq=False)
class EnumSchema(NamedSchema):
    """A named enumeration of unique symbols."""

    type_name: ClassVar[str] = "enum"
    symbols: tuple[str, ...] = ()
    default: str | None = None

    def __post_init__(self) -> None:
        NamedSchema.__post_init__(self)
        object.__setattr__(self, "symbols", tuple(self.symbols))
        if not self.symbols:
            raise AvroSchemaError(f"enum {self.fullname!r} requires symbols")
        for symbol in self.symbols:
            _validate_name(symbol, kind="enum symbol")
        if len(set(self.symbols)) != len(self.symbols):
            raise AvroSchemaError(f"enum {self.fullname!r} has duplicate symbols")
        if self.default is not None and self.default not in self.symbols:
            raise AvroSchemaError(
                f"enum {self.fullname!r} default {self.default!r} is not a symbol"
            )


@dataclasses.dataclass(frozen=True, eq=False)
class AvroField:
    """One record field with its schema, default, and extra attributes."""

    name: str
    type: AvroSchema
    default: Any = MISSING
    doc: str | None = None
    order: str | None = None
    aliases: tuple[str, ...] = ()
    attributes: Mapping[str, Any] = dataclasses.field(default_factory=dict)

    def __post_init__(self) -> None:
        _validate_name(self.name, kind="field name")
        if not isinstance(self.type, AvroSchema):
            raise AvroSchemaError(f"field {self.name!r} requires a parsed Avro schema")
        if self.order is not None and self.order not in {
            "ascending",
            "descending",
            "ignore",
        }:
            raise AvroSchemaError(
                f"field {self.name!r} order must be ascending, descending, or ignore"
            )
        object.__setattr__(self, "aliases", tuple(self.aliases))
        object.__setattr__(self, "attributes", _freeze(self.attributes))

    @property
    def has_default(self) -> bool:
        """Return whether the field declares a reader default value."""

        return self.default is not MISSING

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AvroField):
            return NotImplemented
        return (
            self.name == other.name
            and self.type == other.type
            and self.default == other.default
            and self.aliases == other.aliases
        )

    def __hash__(self) -> int:
        return hash((self.name, self.type, self.aliases))


@dataclasses.dataclass(frozen=True, eq=False)
class RecordSchema(NamedSchema):
    """A named record; ``fields`` is finalized after recursive parsing."""

    type_name: ClassVar[str] = "record"
    fields: tuple[AvroField, ...] = ()
    is_error: bool = False

    def __post_init__(self) -> None:
        NamedSchema.__post_init__(self)
        object.__setattr__(self, "fields", tuple(self.fields))
        self._validate_fields()

    def _validate_fields(self) -> None:
        seen: set[str] = set()
        for field in self.fields:
            if not isinstance(field, AvroField):
                raise AvroSchemaError(
                    f"record {self.fullname!r} fields must be AvroField values"
                )
            if field.name in seen:
                raise AvroSchemaError(
                    f"record {self.fullname!r} has duplicate field {field.name!r}"
                )
            seen.add(field.name)

    def field(self, name: str) -> AvroField:
        """Return one field by name."""

        for field in self.fields:
            if field.name == name:
                return field
        raise KeyError(name)

    def _replace_fields(self, fields: Iterable[AvroField]) -> None:
        object.__setattr__(self, "fields", tuple(fields))
        self._validate_fields()


@dataclasses.dataclass(frozen=True, eq=False)
class ArraySchema(AvroSchema):
    """A homogeneous array of items."""

    type_name: ClassVar[str] = "array"
    items: AvroSchema = dataclasses.field(
        default_factory=lambda: PrimitiveSchema("null")
    )
    attributes: Mapping[str, Any] = dataclasses.field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.items, AvroSchema):
            raise AvroSchemaError("array items must be a parsed Avro schema")
        object.__setattr__(self, "attributes", _freeze(self.attributes))


@dataclasses.dataclass(frozen=True, eq=False)
class MapSchema(AvroSchema):
    """A map from strings to a homogeneous value schema."""

    type_name: ClassVar[str] = "map"
    values: AvroSchema = dataclasses.field(
        default_factory=lambda: PrimitiveSchema("null")
    )
    attributes: Mapping[str, Any] = dataclasses.field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.values, AvroSchema):
            raise AvroSchemaError("map values must be a parsed Avro schema")
        object.__setattr__(self, "attributes", _freeze(self.attributes))


@dataclasses.dataclass(frozen=True, eq=False)
class UnionSchema(AvroSchema):
    """An ordered union of branch schemas."""

    type_name: ClassVar[str] = "union"
    options: tuple[AvroSchema, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "options", tuple(self.options))
        if not self.options:
            raise AvroSchemaError("union requires at least one branch")
        seen: set[str] = set()
        for option in self.options:
            if not isinstance(option, AvroSchema):
                raise AvroSchemaError("union branches must be parsed Avro schemas")
            if isinstance(option, UnionSchema):
                raise AvroSchemaError("unions cannot immediately contain unions")
            key = option.fullname
            if key in seen:
                raise AvroSchemaError(f"union has duplicate branch {key!r}")
            seen.add(key)

    def __iter__(self) -> Iterator[AvroSchema]:
        return iter(self.options)

    @property
    def is_optional(self) -> bool:
        """Return whether the union contains Avro's ``null`` branch."""

        return any(option.fullname == "null" for option in self.options)


def parse_schema(value: Any, *, namespace: str | None = None) -> AvroSchema:
    """Parse an Avro schema declaration into the immutable model.

    ``value`` accepts an already-parsed schema, a JSON string, or the decoded
    JSON structure.  Named types are registered so recursive and repeated
    references resolve exactly like the reference implementation.
    """

    if isinstance(value, AvroSchema):
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        value = bytes(value).decode("utf-8")
    if isinstance(value, str) and value.strip().startswith(("{", "[", '"')):
        from ..json import loads as _loads_json

        value = _loads_json(value)
    return _parse(value, namespace, {})


def schema_into_json(schema: AvroSchema) -> Any:
    """Return the JSON-compatible declaration of a parsed schema."""

    if not isinstance(schema, AvroSchema):
        raise AvroSchemaError("schema_into_json expects a parsed Avro schema")
    return _emit(schema, set(), None)


def canonical_form(schema: AvroSchema) -> str:
    """Return the parsing canonical form defined by the Avro specification.

    The result is memoized on the immutable schema so equality, hashing, and
    codec compilation never re-traverse a large declaration.
    """

    if not isinstance(schema, AvroSchema):
        raise AvroSchemaError("canonical_form expects a parsed Avro schema")
    cached = schema.__dict__.get("_canonical_form")
    if cached is None:
        cached = _canonical(schema, set())
        object.__setattr__(schema, "_canonical_form", cached)
    return cached


_EMPTY64 = 0xC15D213AA4D7A795
_FINGERPRINT_TABLE: tuple[int, ...] = ()


def _fingerprint_table() -> tuple[int, ...]:
    global _FINGERPRINT_TABLE
    if not _FINGERPRINT_TABLE:
        table: list[int] = []
        for index in range(256):
            value = index
            for _ in range(8):
                value = (value >> 1) ^ (_EMPTY64 & -(value & 1))
            table.append(value)
        _FINGERPRINT_TABLE = tuple(table)
    return _FINGERPRINT_TABLE


def fingerprint(schema: AvroSchema | str | bytes) -> int:
    """Return the 64-bit CRC-64-AVRO (Rabin) fingerprint of a schema."""

    if isinstance(schema, AvroSchema):
        payload = canonical_form(schema).encode("utf-8")
    elif isinstance(schema, str):
        payload = schema.encode("utf-8")
    else:
        payload = bytes(schema)
    table = _fingerprint_table()
    result = _EMPTY64
    for byte in payload:
        result = (result >> 8) ^ table[(result ^ byte) & 0xFF]
    return result


def fingerprint_bytes(schema: AvroSchema | str | bytes) -> bytes:
    """Return the little-endian fingerprint used by single-object encoding."""

    return fingerprint(schema).to_bytes(8, "little")


def _parse(
    value: Any,
    namespace: str | None,
    named: dict[str, AvroSchema],
) -> AvroSchema:
    if isinstance(value, AvroSchema):
        return value
    if isinstance(value, str):
        return _parse_name(value, namespace, named)
    if isinstance(value, (list, tuple)):
        return UnionSchema(tuple(_parse(item, namespace, named) for item in value))
    if isinstance(value, Mapping):
        return _parse_mapping(value, namespace, named)
    raise AvroSchemaError(f"cannot parse Avro schema from {type(value).__qualname__}")


def _parse_name(
    value: str,
    namespace: str | None,
    named: Mapping[str, AvroSchema],
) -> AvroSchema:
    primitive = _PRIMITIVE_SINGLETONS.get(value)
    if primitive is not None:
        return primitive
    candidates = [value]
    if namespace and "." not in value:
        candidates.insert(0, f"{namespace}.{value}")
    for candidate in candidates:
        resolved = named.get(candidate)
        if resolved is not None:
            return resolved
    raise AvroSchemaError(f"unknown Avro schema name {value!r}")


def _parse_mapping(
    value: Mapping[str, Any],
    namespace: str | None,
    named: dict[str, AvroSchema],
) -> AvroSchema:
    if "type" not in value:
        raise AvroSchemaError("Avro schema objects require a 'type' attribute")
    declared = value["type"]
    if not isinstance(declared, str):
        # ``{"type": {...}}`` wrappers carry attributes around an inner schema.
        return _parse(declared, namespace, named)

    attributes = _extra_attributes(value)
    if declared in PRIMITIVE_NAMES:
        logical, precision, scale = _logical_annotation(value, declared)
        if logical is None and not attributes and "logicalType" not in value:
            return _PRIMITIVE_SINGLETONS[declared]
        if logical is None and "logicalType" in value:
            attributes = {**attributes, "logicalType": value["logicalType"]}
        return PrimitiveSchema(
            declared,
            logical=logical,
            precision=precision,
            scale=scale,
            attributes=attributes,
        )
    if declared == "array":
        if "items" not in value:
            raise AvroSchemaError("array schemas require 'items'")
        return ArraySchema(
            _parse(value["items"], namespace, named),
            attributes=attributes,
        )
    if declared == "map":
        if "values" not in value:
            raise AvroSchemaError("map schemas require 'values'")
        return MapSchema(
            _parse(value["values"], namespace, named),
            attributes=attributes,
        )
    if declared in {"record", "error", "enum", "fixed"}:
        return _parse_named(declared, value, namespace, named, attributes)
    return _parse_name(declared, namespace, named)


def _parse_named(
    declared: str,
    value: Mapping[str, Any],
    namespace: str | None,
    named: dict[str, AvroSchema],
    attributes: Mapping[str, Any],
) -> AvroSchema:
    raw_name = value.get("name")
    if not isinstance(raw_name, str) or not raw_name:
        raise AvroSchemaError(f"{declared} schemas require a non-empty 'name'")
    declared_namespace = value.get("namespace")
    if declared_namespace is not None and not isinstance(declared_namespace, str):
        raise AvroSchemaError("namespace must be a string")
    effective_namespace: str | None
    if "." in raw_name:
        effective_namespace = raw_name.rsplit(".", 1)[0]
        short_name = raw_name.rsplit(".", 1)[1]
    else:
        short_name = raw_name
        effective_namespace = (
            declared_namespace if declared_namespace is not None else namespace
        )
    effective_namespace = effective_namespace or None
    doc = value.get("doc")
    if doc is not None and not isinstance(doc, str):
        raise AvroSchemaError("doc must be a string")
    aliases = _parse_aliases(value.get("aliases"))

    if declared in {"record", "error"}:
        record_schema = RecordSchema(
            declared_name=short_name,
            namespace=effective_namespace,
            aliases=aliases,
            doc=doc,
            attributes=attributes,
            is_error=declared == "error",
        )
        _register(named, record_schema)
        raw_fields = value.get("fields")
        if not isinstance(raw_fields, Sequence) or isinstance(raw_fields, (str, bytes)):
            raise AvroSchemaError(
                f"record {record_schema.fullname!r} requires a list of 'fields'"
            )
        record_schema._replace_fields(
            _parse_field(item, effective_namespace, named) for item in raw_fields
        )
        return record_schema

    if declared == "enum":
        symbols = value.get("symbols")
        if not isinstance(symbols, Sequence) or isinstance(symbols, (str, bytes)):
            raise AvroSchemaError("enum schemas require a list of 'symbols'")
        default = value.get("default")
        if default is not None and not isinstance(default, str):
            raise AvroSchemaError("enum default must be a symbol name")
        enum_schema = EnumSchema(
            declared_name=short_name,
            namespace=effective_namespace,
            aliases=aliases,
            doc=doc,
            attributes=attributes,
            symbols=tuple(str(symbol) for symbol in symbols),
            default=default,
        )
        _register(named, enum_schema)
        return enum_schema

    size = value.get("size")
    if type(size) is not int or size < 0:
        raise AvroSchemaError("fixed schemas require a non-negative integer 'size'")
    logical, precision, scale = _logical_annotation(value, "fixed", size=size)
    if logical is None and "logicalType" in value:
        attributes = {**attributes, "logicalType": value["logicalType"]}
    fixed_schema = FixedSchema(
        declared_name=short_name,
        namespace=effective_namespace,
        aliases=aliases,
        doc=doc,
        attributes=attributes,
        size=size,
        logical=logical,
        precision=precision,
        scale=scale,
    )
    _register(named, fixed_schema)
    return fixed_schema


def _parse_field(
    value: Any,
    namespace: str | None,
    named: dict[str, AvroSchema],
) -> AvroField:
    if not isinstance(value, Mapping):
        raise AvroSchemaError("record fields must be JSON objects")
    name = value.get("name")
    if not isinstance(name, str) or not name:
        raise AvroSchemaError("record fields require a non-empty 'name'")
    if "type" not in value:
        raise AvroSchemaError(f"record field {name!r} requires a 'type'")
    field_type = _parse(value["type"], namespace, named)
    doc = value.get("doc")
    if doc is not None and not isinstance(doc, str):
        raise AvroSchemaError(f"record field {name!r} doc must be a string")
    order = value.get("order")
    if order is not None and not isinstance(order, str):
        raise AvroSchemaError(f"record field {name!r} order must be a string")
    default = value.get("default", MISSING)
    attributes = {
        key: item
        for key, item in value.items()
        if key not in _RESERVED_ATTRIBUTES and key != "order"
    }
    return AvroField(
        name=name,
        type=field_type,
        default=default,
        doc=doc,
        order=order,
        aliases=_parse_aliases(value.get("aliases")),
        attributes=attributes,
    )


def _parse_aliases(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str) or not isinstance(value, Sequence):
        raise AvroSchemaError("aliases must be a list of names")
    return tuple(str(item) for item in value)


def _register(named: dict[str, AvroSchema], schema: AvroSchema) -> None:
    fullname = schema.fullname
    if fullname in named:
        raise AvroSchemaError(f"duplicate Avro type name {fullname!r}")
    named[fullname] = schema


def _extra_attributes(value: Mapping[str, Any]) -> dict[str, Any]:
    return {key: item for key, item in value.items() if key not in _RESERVED_ATTRIBUTES}


def _logical_annotation(
    value: Mapping[str, Any],
    underlying: str,
    *,
    size: int | None = None,
) -> tuple[str | None, int | None, int | None]:
    logical = value.get("logicalType")
    if logical is None:
        return None, None, None
    if not isinstance(logical, str):
        raise AvroSchemaError("logicalType must be a string")
    allowed = _LOGICAL_TYPES.get(logical)
    # Unrecognized or misapplied annotations are ignored by readers, so the
    # underlying type still governs encoding while the attribute survives.
    if allowed is None or underlying not in allowed:
        return None, None, None
    if logical == "duration" and size != 12:
        return None, None, None
    if logical not in {"decimal", "big-decimal"}:
        return logical, None, None
    if logical == "big-decimal":
        return logical, None, None
    precision = value.get("precision")
    scale = value.get("scale", 0)
    if type(precision) is not int or precision <= 0:
        return None, None, None
    if type(scale) is not int or scale < 0 or scale > precision:
        return None, None, None
    if size is not None and precision > _max_fixed_precision(size):
        raise AvroSchemaError(
            f"decimal precision {precision} does not fit in fixed({size})"
        )
    return logical, precision, scale


def _max_fixed_precision(size: int) -> int:
    if size <= 0:
        return 0
    limit = (1 << (8 * size - 1)) - 1
    return len(str(limit))


def _freeze(value: Mapping[str, Any] | None) -> Mapping[str, Any]:
    if not value:
        return _EMPTY
    if not isinstance(value, Mapping):
        raise AvroSchemaError("schema attributes must be a mapping")
    return MappingProxyType(dict(value))


def _validate_name(value: Any, *, kind: str = "name") -> None:
    if not isinstance(value, str) or not value:
        raise AvroSchemaError(f"Avro {kind} must be a non-empty string")
    for part in value.split("."):
        if _NAME_PART.fullmatch(part) is None:
            raise AvroSchemaError(f"invalid Avro {kind} {value!r}")


def _validate_namespace(value: str) -> None:
    if value == "":
        return
    for part in value.split("."):
        if _NAME_PART.fullmatch(part) is None:
            raise AvroSchemaError(f"invalid Avro namespace {value!r}")


def _validate_fullname(value: Any) -> None:
    _validate_name(value, kind="alias")


def _emit(schema: AvroSchema, emitted: set[str], namespace: str | None) -> Any:
    if isinstance(schema, PrimitiveSchema):
        if (
            schema.logical is None
            and not schema.attributes
            and schema.precision is None
        ):
            return schema.primitive
        result: dict[str, Any] = {"type": schema.primitive}
        result.update(schema.attributes)
        if schema.logical is not None:
            result["logicalType"] = schema.logical
            if schema.precision is not None:
                result["precision"] = schema.precision
                result["scale"] = schema.scale or 0
        return result
    if isinstance(schema, UnionSchema):
        return [_emit(option, emitted, namespace) for option in schema.options]
    if isinstance(schema, ArraySchema):
        result = {"type": "array", "items": _emit(schema.items, emitted, namespace)}
        result.update(schema.attributes)
        return result
    if isinstance(schema, MapSchema):
        result = {"type": "map", "values": _emit(schema.values, emitted, namespace)}
        result.update(schema.attributes)
        return result

    assert isinstance(schema, NamedSchema)
    fullname = schema.fullname
    if fullname in emitted:
        return fullname
    emitted.add(fullname)
    result = {
        "type": "error" if getattr(schema, "is_error", False) else schema.type_name,
        "name": schema.declared_name,
    }
    if schema.namespace and schema.namespace != namespace:
        result["namespace"] = schema.namespace
    if schema.doc:
        result["doc"] = schema.doc
    if schema.aliases:
        result["aliases"] = list(schema.aliases)
    inner_namespace = schema.namespace or namespace
    if isinstance(schema, RecordSchema):
        result["fields"] = [
            _emit_field(field, emitted, inner_namespace) for field in schema.fields
        ]
    elif isinstance(schema, EnumSchema):
        result["symbols"] = list(schema.symbols)
        if schema.default is not None:
            result["default"] = schema.default
    elif isinstance(schema, FixedSchema):
        result["size"] = schema.size
        if schema.logical is not None:
            result["logicalType"] = schema.logical
            if schema.precision is not None:
                result["precision"] = schema.precision
                result["scale"] = schema.scale or 0
    result.update(schema.attributes)
    return result


def _emit_field(field: AvroField, emitted: set[str], namespace: str | None) -> Any:
    result: dict[str, Any] = {
        "name": field.name,
        "type": _emit(field.type, emitted, namespace),
    }
    if field.doc:
        result["doc"] = field.doc
    if field.has_default:
        result["default"] = field.default
    if field.order is not None:
        result["order"] = field.order
    if field.aliases:
        result["aliases"] = list(field.aliases)
    result.update(field.attributes)
    return result


def _canonical(schema: AvroSchema, emitted: set[str]) -> str:
    if isinstance(schema, PrimitiveSchema):
        return f'"{schema.primitive}"'
    if isinstance(schema, UnionSchema):
        return (
            "[" + ",".join(_canonical(item, emitted) for item in schema.options) + "]"
        )
    if isinstance(schema, ArraySchema):
        return '{"type":"array","items":' + _canonical(schema.items, emitted) + "}"
    if isinstance(schema, MapSchema):
        return '{"type":"map","values":' + _canonical(schema.values, emitted) + "}"

    assert isinstance(schema, NamedSchema)
    fullname = schema.fullname
    if fullname in emitted:
        return f'"{fullname}"'
    emitted.add(fullname)
    parts: dict[str, str] = {"name": f'"{fullname}"', "type": f'"{schema.type_name}"'}
    if isinstance(schema, RecordSchema):
        parts["fields"] = (
            "["
            + ",".join(
                '{"name":"'
                + field.name
                + '","type":'
                + _canonical(field.type, emitted)
                + "}"
                for field in schema.fields
            )
            + "]"
        )
    elif isinstance(schema, EnumSchema):
        parts["symbols"] = "[" + ",".join(f'"{item}"' for item in schema.symbols) + "]"
    elif isinstance(schema, FixedSchema):
        parts["size"] = str(schema.size)
    ordered = (key for key in _CANONICAL_ORDER if key in parts)
    return "{" + ",".join(f'"{key}":{parts[key]}' for key in ordered) + "}"


NULL = PrimitiveSchema("null")
BOOLEAN = PrimitiveSchema("boolean")
INT = PrimitiveSchema("int")
LONG = PrimitiveSchema("long")
FLOAT = PrimitiveSchema("float")
DOUBLE = PrimitiveSchema("double")
BYTES = PrimitiveSchema("bytes")
STRING = PrimitiveSchema("string")

_PRIMITIVE_SINGLETONS: Mapping[str, PrimitiveSchema] = MappingProxyType(
    {
        "null": NULL,
        "boolean": BOOLEAN,
        "int": INT,
        "long": LONG,
        "float": FLOAT,
        "double": DOUBLE,
        "bytes": BYTES,
        "string": STRING,
    }
)
