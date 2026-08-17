"""The Avro schema model, projected from the Rust core.

Every class here is a thin, immutable view over one node of a parsed schema
held by :mod:`rkp._avro`.  Constructing a class assembles the JSON declaration
and hands it to the core, so parsing, validation, canonical form, and
fingerprints have exactly one implementation.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterable, Iterator, Mapping, Sequence
from types import MappingProxyType
from typing import Any, Self

from .. import _avro
from ..json import dumps as _dumps_json
from ..json import loads as _loads_json

__all__ = [
    "PRIMITIVE_NAMES",
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


class AvroSchema:
    """One node of a parsed Avro schema."""

    __slots__ = ("_core", "_index", "_info", "_json")

    def __init__(self, core: Any, index: int) -> None:
        self._core = core
        self._index = index
        self._info = core.node(index)
        self._json: Any = None

    @property
    def type_name(self) -> str:
        """Return the node's structural type name."""

        return self._info.kind

    @property
    def name(self) -> str:
        """Return the declared or primitive name."""

        return self._info.name or self._info.kind

    @property
    def fullname(self) -> str:
        """Return the fully qualified name used by canonical form."""

        return self._info.fullname

    @property
    def logical_type(self) -> str | None:
        """Return the recognized logical annotation, when present."""

        return self._info.logical

    @property
    def attributes(self) -> Mapping[str, Any]:
        """Return the declaration attributes the specification does not own."""

        return MappingProxyType(_loads_json(self._info.attributes))

    def into_json(self) -> Any:
        """Return this node's JSON declaration."""

        if self._json is None:
            self._json = _loads_json(self._core.subschema(self._index).json())
        return self._json

    def canonical_form(self) -> str:
        """Return the specification's parsing canonical form."""

        return self._core.subschema(self._index).canonical_form()

    def fingerprint(self) -> int:
        """Return the 64-bit Rabin fingerprint of the canonical form."""

        return self._core.subschema(self._index).fingerprint()

    def _rooted(self) -> Any:
        return self._core.subschema(self._index)

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if not isinstance(other, AvroSchema):
            return NotImplemented
        return self.canonical_form() == other.canonical_form()

    def __hash__(self) -> int:
        return hash(self.canonical_form())

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.fullname}>"


class PrimitiveSchema(AvroSchema):
    """One of Avro's eight primitive types, with optional logical metadata."""

    __slots__ = ()

    def __new__(
        cls,
        primitive: str | None = None,
        *,
        logical: str | None = None,
        precision: int | None = None,
        scale: int | None = None,
        attributes: Mapping[str, Any] | None = None,
        _core: Any = None,
        _index: int = 0,
    ) -> Self:
        if _core is not None:
            return object.__new__(cls)
        if primitive not in PRIMITIVE_NAMES:
            raise _avro.AvroSchemaError(f"unknown Avro primitive type {primitive!r}")
        declaration: dict[str, Any] = dict(attributes or {})
        declaration["type"] = primitive
        if logical is not None:
            declaration["logicalType"] = logical
            if precision is not None:
                declaration["precision"] = precision
                declaration["scale"] = scale or 0
        return _build(cls, declaration)

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        _init_view(self, kwargs)

    @property
    def primitive(self) -> str:
        """Return the underlying primitive type name."""

        return self._info.kind

    @property
    def logical(self) -> str | None:
        """Return the recognized logical annotation, when present."""

        return self._info.logical

    @property
    def precision(self) -> int | None:
        """Return a decimal annotation's precision."""

        return self._info.precision

    @property
    def scale(self) -> int | None:
        """Return a decimal annotation's scale."""

        return self._info.scale


class NamedSchema(AvroSchema):
    """Base class for Avro's named types: record, enum, and fixed."""

    __slots__ = ()

    @property
    def declared_name(self) -> str:
        """Return the short declared name."""

        return self._info.name or ""

    @property
    def namespace(self) -> str | None:
        """Return the declared namespace."""

        return self._info.namespace

    @property
    def doc(self) -> str | None:
        """Return the declaration's documentation."""

        return self._info.doc

    @property
    def aliases(self) -> tuple[str, ...]:
        """Return the declared aliases."""

        return tuple(self._info.aliases)


class FixedSchema(NamedSchema):
    """A fixed-size byte sequence, optionally carrying a logical type."""

    __slots__ = ()

    def __new__(
        cls,
        *,
        declared_name: str = "",
        namespace: str | None = None,
        aliases: Iterable[str] = (),
        doc: str | None = None,
        attributes: Mapping[str, Any] | None = None,
        size: int = 0,
        logical: str | None = None,
        precision: int | None = None,
        scale: int | None = None,
        _core: Any = None,
        _index: int = 0,
    ) -> Self:
        if _core is not None:
            return object.__new__(cls)
        declaration = _named_declaration(
            "fixed", declared_name, namespace, aliases, doc, attributes
        )
        declaration["size"] = size
        if logical is not None:
            declaration["logicalType"] = logical
            if precision is not None:
                declaration["precision"] = precision
                declaration["scale"] = scale or 0
        return _build(cls, declaration)

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        _init_view(self, kwargs)

    @property
    def size(self) -> int:
        """Return the declared byte width."""

        return self._info.size

    @property
    def logical(self) -> str | None:
        """Return the recognized logical annotation, when present."""

        return self._info.logical

    @property
    def precision(self) -> int | None:
        """Return a decimal annotation's precision."""

        return self._info.precision

    @property
    def scale(self) -> int | None:
        """Return a decimal annotation's scale."""

        return self._info.scale


class EnumSchema(NamedSchema):
    """A named enumeration of unique symbols."""

    __slots__ = ()

    def __new__(
        cls,
        *,
        declared_name: str = "",
        namespace: str | None = None,
        aliases: Iterable[str] = (),
        doc: str | None = None,
        attributes: Mapping[str, Any] | None = None,
        symbols: Iterable[str] = (),
        default: str | None = None,
        _core: Any = None,
        _index: int = 0,
    ) -> Self:
        if _core is not None:
            return object.__new__(cls)
        declaration = _named_declaration(
            "enum", declared_name, namespace, aliases, doc, attributes
        )
        declaration["symbols"] = list(symbols)
        if default is not None:
            declaration["default"] = default
        return _build(cls, declaration)

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        _init_view(self, kwargs)

    @property
    def symbols(self) -> tuple[str, ...]:
        """Return the declared symbols, in order."""

        return tuple(self._info.symbols)

    @property
    def default(self) -> str | None:
        """Return the reader default symbol, when declared."""

        return self._info.enum_default


@dataclasses.dataclass(frozen=True, slots=True, eq=False)
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
        if not isinstance(self.name, str) or not self.name:
            raise _avro.AvroSchemaError("Avro field name must be a non-empty string")
        if not isinstance(self.type, AvroSchema):
            raise _avro.AvroSchemaError(
                f"field {self.name!r} requires a parsed Avro schema"
            )
        if self.order is not None and self.order not in {
            "ascending",
            "descending",
            "ignore",
        }:
            raise _avro.AvroSchemaError(
                f"field {self.name!r} order must be ascending, descending, or ignore"
            )
        object.__setattr__(self, "aliases", tuple(self.aliases))
        object.__setattr__(
            self, "attributes", MappingProxyType(dict(self.attributes or {}))
        )

    @property
    def has_default(self) -> bool:
        """Return whether the field declares a reader default value."""

        return self.default is not MISSING

    def into_json(self) -> dict[str, Any]:
        """Return this field's JSON declaration."""

        declaration: dict[str, Any] = {
            "name": self.name,
            "type": self.type.into_json(),
        }
        if self.doc:
            declaration["doc"] = self.doc
        if self.has_default:
            declaration["default"] = self.default
        if self.order is not None:
            declaration["order"] = self.order
        if self.aliases:
            declaration["aliases"] = list(self.aliases)
        declaration.update(self.attributes)
        return declaration

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


class RecordSchema(NamedSchema):
    """A named record."""

    __slots__ = ()

    def __new__(
        cls,
        *,
        declared_name: str = "",
        namespace: str | None = None,
        aliases: Iterable[str] = (),
        doc: str | None = None,
        attributes: Mapping[str, Any] | None = None,
        fields: Iterable[AvroField] = (),
        is_error: bool = False,
        _core: Any = None,
        _index: int = 0,
    ) -> Self:
        if _core is not None:
            return object.__new__(cls)
        declaration = _named_declaration(
            "error" if is_error else "record",
            declared_name,
            namespace,
            aliases,
            doc,
            attributes,
        )
        declaration["fields"] = [field.into_json() for field in fields]
        return _build(cls, declaration)

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        _init_view(self, kwargs)

    @property
    def fields(self) -> tuple[AvroField, ...]:
        """Return every field, in declaration order."""

        return tuple(
            AvroField(
                name=item.name,
                type=_view(self._core, item.node),
                default=(
                    MISSING if item.default is None else _loads_json(item.default)
                ),
                doc=item.doc,
                order=item.order,
                aliases=tuple(item.aliases),
                attributes=_loads_json(item.attributes),
            )
            for item in self._info.fields
        )

    @property
    def is_error(self) -> bool:
        """Return whether the record was declared as an error type."""

        return self._info.is_error

    def field(self, name: str) -> AvroField:
        """Return one field by name."""

        for field in self.fields:
            if field.name == name:
                return field
        raise KeyError(name)


class ArraySchema(AvroSchema):
    """A homogeneous array of items."""

    __slots__ = ()

    def __new__(
        cls,
        items: AvroSchema | None = None,
        *,
        attributes: Mapping[str, Any] | None = None,
        _core: Any = None,
        _index: int = 0,
    ) -> Self:
        if _core is not None:
            return object.__new__(cls)
        if not isinstance(items, AvroSchema):
            raise _avro.AvroSchemaError("array items must be a parsed Avro schema")
        declaration: dict[str, Any] = dict(attributes or {})
        declaration["type"] = "array"
        declaration["items"] = items.into_json()
        return _build(cls, declaration)

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        _init_view(self, kwargs)

    @property
    def items(self) -> AvroSchema:
        """Return the element schema."""

        return _view(self._core, self._info.children[0])


class MapSchema(AvroSchema):
    """A map from strings to a homogeneous value schema."""

    __slots__ = ()

    def __new__(
        cls,
        values: AvroSchema | None = None,
        *,
        attributes: Mapping[str, Any] | None = None,
        _core: Any = None,
        _index: int = 0,
    ) -> Self:
        if _core is not None:
            return object.__new__(cls)
        if not isinstance(values, AvroSchema):
            raise _avro.AvroSchemaError("map values must be a parsed Avro schema")
        declaration: dict[str, Any] = dict(attributes or {})
        declaration["type"] = "map"
        declaration["values"] = values.into_json()
        return _build(cls, declaration)

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        _init_view(self, kwargs)

    @property
    def values(self) -> AvroSchema:
        """Return the value schema."""

        return _view(self._core, self._info.children[0])


class UnionSchema(AvroSchema):
    """An ordered union of branch schemas."""

    __slots__ = ()

    def __new__(
        cls,
        options: Sequence[AvroSchema] | None = None,
        *,
        _core: Any = None,
        _index: int = 0,
    ) -> Self:
        if _core is not None:
            return object.__new__(cls)
        branches = list(options or ())
        if not branches:
            raise _avro.AvroSchemaError("union requires at least one branch")
        for branch in branches:
            if not isinstance(branch, AvroSchema):
                raise _avro.AvroSchemaError(
                    "union branches must be parsed Avro schemas"
                )
        return _build(cls, [branch.into_json() for branch in branches])

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        _init_view(self, kwargs)

    @property
    def options(self) -> tuple[AvroSchema, ...]:
        """Return every branch, in declaration order."""

        return tuple(_view(self._core, child) for child in self._info.children)

    @property
    def is_optional(self) -> bool:
        """Return whether the union contains Avro's ``null`` branch."""

        return any(option.type_name == "null" for option in self.options)

    def __iter__(self) -> Iterator[AvroSchema]:
        return iter(self.options)


_VIEWS: Mapping[str, type[AvroSchema]] = MappingProxyType(
    {
        "record": RecordSchema,
        "enum": EnumSchema,
        "fixed": FixedSchema,
        "array": ArraySchema,
        "map": MapSchema,
        "union": UnionSchema,
    }
)


def _view(core: Any, index: int) -> AvroSchema:
    kind = core.node(index).kind
    view = _VIEWS.get(kind, PrimitiveSchema)
    instance = object.__new__(view)
    AvroSchema.__init__(instance, core, index)
    return instance


def _init_view(instance: AvroSchema, kwargs: Mapping[str, Any]) -> None:
    core = kwargs.get("_core")
    if core is not None:
        AvroSchema.__init__(instance, core, int(kwargs.get("_index", 0)))


def _build(view: type[AvroSchema], declaration: Any) -> Any:
    core = _avro.Schema.parse(_dumps_json(declaration))
    instance = object.__new__(view)
    AvroSchema.__init__(instance, core, core.root())
    return instance


def _named_declaration(
    kind: str,
    declared_name: str,
    namespace: str | None,
    aliases: Iterable[str],
    doc: str | None,
    attributes: Mapping[str, Any] | None,
) -> dict[str, Any]:
    declaration: dict[str, Any] = dict(attributes or {})
    declaration["type"] = kind
    declaration["name"] = declared_name
    if namespace:
        declaration["namespace"] = namespace
    if doc:
        declaration["doc"] = doc
    alias_list = list(aliases)
    if alias_list:
        declaration["aliases"] = alias_list
    return declaration


def parse_schema(value: Any, *, namespace: str | None = None) -> AvroSchema:
    """Parse an Avro schema declaration into the immutable model."""

    if isinstance(value, AvroSchema):
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        value = bytes(value).decode("utf-8")
    if isinstance(value, str) and not value.strip().startswith(("{", "[", '"')):
        # A bare name is a declaration too, once it is quoted as JSON.
        value = _dumps_json(value)
    text = (
        value if isinstance(value, str) else _dumps_json(_declaration(value, namespace))
    )
    if namespace and isinstance(value, str):
        text = _dumps_json(_declaration(_loads_json(text), namespace))
    core = _avro.Schema.parse(text)
    return _view(core, core.root())


def _declaration(value: Any, namespace: str | None) -> Any:
    if namespace and isinstance(value, Mapping) and "namespace" not in value:
        return {**value, "namespace": namespace}
    return value


def schema_into_json(schema: Any) -> Any:
    """Return the JSON-compatible declaration of a parsed schema."""

    return parse_schema(schema).into_json()
