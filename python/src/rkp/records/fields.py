"""Dataclass-compatible fields with one shared interoperability metadata map."""

from __future__ import annotations

import dataclasses
import inspect
from collections.abc import Callable, Mapping
from types import EllipsisType, MappingProxyType
from typing import Any, Generic, TypeVar, cast

from ._metadata import MAX_FIELD_SEQ

__all__ = ["Field", "FieldOptions", "field", "field_options"]

T = TypeVar("T")
KeySpec = bool | int | str
_UNSET: EllipsisType = ...
_FIELD_HAS_DOC = "doc" in inspect.signature(dataclasses.Field).parameters
_RKP_METADATA = "rkp"
_ROLE_NAMES = ("primary", "partition", "index")
_ROLE_KEYS = {
    "primary_key": "primary",
    "partition_key": "partition",
    "index_key": "index",
}
_CONTROL_KEYS = frozenset(
    {
        "alias",
        "name",
        "type",
        "arrow_type",
        "nullable",
        "metadata",
        "arrow_metadata",
        "parameters",
        "seq",
        "field_id",
        "iceberg_field_id",
        "roles",
        "keys",
        "doc",
        *_ROLE_KEYS,
    }
)


class Field(dataclasses.Field, Generic[T]):
    """A dataclass field whose interoperability state lives in ``metadata``.

    The class intentionally adds no protocol-specific stored state. Read-only
    projections such as :attr:`seq` resolve from the same immutable metadata
    contract used by dataclass tools, codecs, Arrow, and Iceberg.
    """

    def __init__(
        self,
        default: Any,
        default_factory: Any,
        init: bool,
        repr: bool,
        hash: bool | None,
        compare: bool,
        metadata: Mapping[Any, Any] | None,
        kw_only: bool | dataclasses._MISSING_TYPE,
    ) -> None:
        metadata_argument = cast(Mapping[Any, Any], metadata)
        kw_only_argument = cast(bool, kw_only)
        if _FIELD_HAS_DOC:
            super().__init__(  # type: ignore[call-arg]
                default,
                default_factory,
                init,
                repr,
                hash,
                compare,
                metadata_argument,
                kw_only_argument,
                None,
            )
        else:
            super().__init__(
                default,
                default_factory,
                init,
                repr,
                hash,
                compare,
                metadata_argument,
                kw_only_argument,
            )

    @property
    def seq(self) -> int | None:
        """Stable cross-protocol identity projected to an Iceberg field ID."""

        config = self.metadata.get(_RKP_METADATA, {})
        if isinstance(config, Mapping) and "seq" in config:
            return _validate_seq(config["seq"])
        return field_options(self).seq

    def into_iceberg_field(
        self,
        *,
        owner: type[Any] | None = None,
        name: str | None = None,
        nullable: bool | None = None,
        field_id_start: int = 1,
        format_version: int = 2,
        downcast_ns_timestamp_to_us: bool | None | EllipsisType = ...,
    ) -> Any:
        """Convert this attached dataclass field to an Iceberg field.

        The optional adapter is imported only when called.  Pass ``owner``
        when postponed annotations need resolution against their dataclass.
        """

        from ..utils import into_iceberg_field

        return into_iceberg_field(
            self,
            owner=owner,
            name=name,
            nullable=nullable,
            field_id_start=field_id_start,
            format_version=format_version,
            downcast_ns_timestamp_to_us=downcast_ns_timestamp_to_us,
        )

    def into_iceberg_schema(
        self,
        *,
        owner: type[Any] | None = None,
        schema_id: int = 0,
        field_id_start: int = 1,
        identifier_field_ids: Any = None,
        format_version: int = 2,
        downcast_ns_timestamp_to_us: bool | None | EllipsisType = ...,
    ) -> Any:
        """Build a one-column Iceberg schema from this attached field."""

        from ..utils import into_iceberg_schema

        return into_iceberg_schema(
            self,
            owner=owner,
            schema_id=schema_id,
            field_id_start=field_id_start,
            identifier_field_ids=identifier_field_ids,
            format_version=format_version,
            downcast_ns_timestamp_to_us=downcast_ns_timestamp_to_us,
        )


def field(
    *,
    default: T | dataclasses._MISSING_TYPE = dataclasses.MISSING,
    default_factory: Callable[[], T] | dataclasses._MISSING_TYPE = dataclasses.MISSING,
    init: bool = True,
    repr: bool = True,
    hash: bool | None = None,
    compare: bool = True,
    metadata: Mapping[Any, Any] | None = None,
    kw_only: bool | dataclasses._MISSING_TYPE = dataclasses.MISSING,
    alias: str | None | EllipsisType = _UNSET,
    type: Any = _UNSET,
    nullable: bool | None | EllipsisType = _UNSET,
    doc: str | None | EllipsisType = _UNSET,
    seq: int | None | EllipsisType = _UNSET,
    field_id: int | None | EllipsisType = _UNSET,
    iceberg_field_id: int | None | EllipsisType = _UNSET,
    primary_key: KeySpec | EllipsisType = _UNSET,
    partition_key: KeySpec | EllipsisType = _UNSET,
    index_key: KeySpec | EllipsisType = _UNSET,
) -> Any:
    """Create a record field backed by one canonical metadata mapping.

    Portable metadata is stored at the top level. Interoperability controls
    are normalized beneath the reserved ``"rkp"`` key. ``seq`` is the stable
    cross-protocol identity; ``field_id`` is its compatibility input alias.
    ``type`` is opaque here and interpreted by the selected adapter.
    """

    if (
        default is not dataclasses.MISSING
        and default_factory is not dataclasses.MISSING
    ):
        raise ValueError("cannot specify both default and default_factory")

    controls: dict[str, Any] = {}
    if alias is not _UNSET:
        controls["alias"] = alias
    if type is not _UNSET:
        controls["type"] = type
    if nullable is not _UNSET:
        controls["nullable"] = nullable
    if doc is not _UNSET:
        controls["doc"] = doc
    if seq is not _UNSET:
        controls["seq"] = seq
    if field_id is not _UNSET:
        controls["field_id"] = field_id
    if iceberg_field_id is not _UNSET:
        controls["iceberg_field_id"] = iceberg_field_id

    roles: dict[str, KeySpec] = {}
    for name, value in (
        ("primary", primary_key),
        ("partition", partition_key),
        ("index", index_key),
    ):
        if value is not _UNSET:
            roles[name] = cast(KeySpec, value)
    if roles:
        controls["roles"] = roles

    options = FieldOptions(metadata or {})
    if controls:
        options = options.merged(FieldOptions({_RKP_METADATA: controls}))

    return Field(
        default,
        default_factory,
        init,
        repr,
        hash,
        compare,
        options.metadata,
        kw_only,
    )


@dataclasses.dataclass(frozen=True, slots=True)
class FieldOptions:
    """Validated field options projected from one immutable metadata mapping."""

    metadata: Mapping[Any, Any] = dataclasses.field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", _canonical_metadata(self.metadata))

    @property
    def config(self) -> Mapping[str, Any]:
        return cast(Mapping[str, Any], self.metadata.get(_RKP_METADATA, {}))

    @property
    def payload_metadata(self) -> Mapping[Any, Any]:
        return MappingProxyType(
            {key: value for key, value in self.metadata.items() if key != _RKP_METADATA}
        )

    @property
    def alias(self) -> str | None:
        return cast(str | None, self.config.get("alias"))

    @property
    def type(self) -> Any:
        return self.config.get("type", _UNSET)

    @property
    def type_override(self) -> Any:
        return self.type

    @property
    def type_parameters(self) -> Mapping[str, Any]:
        return cast(
            Mapping[str, Any], self.config.get("parameters", MappingProxyType({}))
        )

    @property
    def nullable(self) -> bool | None:
        return cast(bool | None, self.config.get("nullable"))

    @property
    def doc(self) -> str | None:
        return cast(str | None, self.config.get("doc"))

    @property
    def field_id(self) -> int | None:
        """Compatibility alias for :attr:`seq`."""

        return self.seq

    @property
    def seq(self) -> int | None:
        return cast(int | None, self.config.get("seq"))

    @property
    def roles(self) -> Mapping[str, KeySpec]:
        return cast(
            Mapping[str, KeySpec],
            self.config.get("roles", MappingProxyType({})),
        )

    @property
    def primary_key(self) -> KeySpec:
        return self.roles.get("primary", False)

    @property
    def partition_key(self) -> KeySpec:
        return self.roles.get("partition", False)

    @property
    def index_key(self) -> KeySpec:
        return self.roles.get("index", False)

    @property
    def primary_key_explicit(self) -> bool:
        return "primary" in self.roles

    @property
    def partition_key_explicit(self) -> bool:
        return "partition" in self.roles

    @property
    def index_key_explicit(self) -> bool:
        return "index" in self.roles

    def has(self, name: str) -> bool:
        """Return whether a canonical control was explicitly configured."""

        if name in {"field_id", "iceberg_field_id"}:
            name = "seq"
        if name in _ROLE_KEYS:
            return _ROLE_KEYS[name] in self.roles
        return name in self.config

    def merged(self, override: FieldOptions) -> FieldOptions:
        """Merge a higher-precedence option layer into this one."""

        if not isinstance(override, FieldOptions):
            raise TypeError("override must be FieldOptions")
        return FieldOptions(_merge_metadata(self.metadata, override.metadata))


def field_options(value: dataclasses.Field[Any]) -> FieldOptions:
    """Extract shared options from a custom or standard dataclass field."""

    if not isinstance(value, dataclasses.Field):
        raise TypeError("field_options expects a dataclasses.Field")
    return FieldOptions(value.metadata or {})


def _options_from_mapping(value: Mapping[Any, Any]) -> FieldOptions:
    """Normalize an Annotated/config mapping through the field contract."""

    return FieldOptions(value)


def _canonical_metadata(value: Mapping[Any, Any]) -> Mapping[Any, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("metadata must be a mapping")

    payload = dict(value)
    nested = payload.pop(_RKP_METADATA, {})
    if not isinstance(nested, Mapping):
        raise TypeError("field metadata 'rkp' value must be a mapping")
    controls: dict[str, Any] = dict(nested)

    # Legacy top-level controls are accepted when reading ordinary dataclass
    # metadata, but every new Field is rewritten to the namespaced shape.
    for key in tuple(payload):
        if key in _CONTROL_KEYS:
            controls[key] = payload.pop(key)

    for key in tuple(controls):
        if key not in _CONTROL_KEYS:
            raise TypeError(f"unknown rkp field option {key!r}")

    embedded_metadata = controls.pop("metadata", {})
    if not isinstance(embedded_metadata, Mapping):
        raise TypeError("metadata field option must be a mapping")
    payload.update(embedded_metadata)

    legacy_arrow_metadata = controls.pop("arrow_metadata", {})
    if not isinstance(legacy_arrow_metadata, Mapping):
        raise TypeError("arrow_metadata field option must be a mapping")
    payload.update(legacy_arrow_metadata)

    _normalize_aliases(controls)
    _normalize_identity(controls)
    _normalize_roles(controls)

    if "alias" in controls:
        controls["alias"] = _validate_alias(controls["alias"])
    if "nullable" in controls:
        controls["nullable"] = _validate_nullable(controls["nullable"])
    if "doc" in controls:
        controls["doc"] = _validate_doc(controls["doc"])
    if "seq" in controls:
        controls["seq"] = _validate_seq(controls["seq"])
    if "parameters" in controls:
        parameters = controls["parameters"]
        if not isinstance(parameters, Mapping):
            raise TypeError("type parameters must be a mapping")
        if not all(isinstance(key, str) for key in parameters):
            raise TypeError("type parameter names must be strings")
        controls["parameters"] = _freeze_mapping(parameters)

    result: dict[Any, Any] = dict(payload)
    if controls:
        result[_RKP_METADATA] = _freeze_mapping(controls)
    return _freeze_mapping(result)


def _merge_metadata(
    base: Mapping[Any, Any], override: Mapping[Any, Any]
) -> Mapping[Any, Any]:
    lower = FieldOptions(base)
    higher = FieldOptions(override)
    payload = dict(lower.payload_metadata)
    payload.update(higher.payload_metadata)

    controls = dict(lower.config)
    incoming = dict(higher.config)
    if "type" in incoming and "parameters" not in incoming:
        controls.pop("parameters", None)

    lower_roles = dict(cast(Mapping[str, Any], controls.pop("roles", {})))
    higher_roles = dict(cast(Mapping[str, Any], incoming.pop("roles", {})))
    lower_roles.update(higher_roles)
    controls.update(incoming)
    if lower_roles:
        controls["roles"] = lower_roles
    if controls:
        payload[_RKP_METADATA] = controls
    return payload


def _normalize_aliases(controls: dict[str, Any]) -> None:
    alias_candidates = [
        (key, controls[key]) for key in ("alias", "name") if key in controls
    ]
    if len(alias_candidates) > 1 and alias_candidates[0][1] != alias_candidates[1][1]:
        raise TypeError("conflicting alias and name field options")
    if alias_candidates:
        controls["alias"] = alias_candidates[-1][1]
    controls.pop("name", None)

    type_candidates = [
        (key, controls[key]) for key in ("type", "arrow_type") if key in controls
    ]
    if len(type_candidates) > 1 and type_candidates[0][1] != type_candidates[1][1]:
        raise TypeError("conflicting type and arrow_type field options")
    if type_candidates:
        controls["type"] = type_candidates[0][1]
    controls.pop("arrow_type", None)


def _normalize_identity(controls: dict[str, Any]) -> None:
    candidates = [
        (key, _validate_seq(controls[key]))
        for key in ("seq", "field_id", "iceberg_field_id")
        if key in controls
    ]
    if any(value != candidates[0][1] for _, value in candidates[1:]):
        raise TypeError("conflicting seq and field_id options")
    if candidates:
        controls["seq"] = candidates[0][1]
    controls.pop("field_id", None)
    controls.pop("iceberg_field_id", None)


def _normalize_roles(controls: dict[str, Any]) -> None:
    roles: dict[str, Any] = {}
    legacy_keys = controls.pop("keys", {})
    canonical_roles = controls.pop("roles", {})
    for name, values in (("keys", legacy_keys), ("roles", canonical_roles)):
        if not isinstance(values, Mapping):
            raise TypeError(f"{name} field option must be a mapping")
        roles.update(values)
    for key, role in _ROLE_KEYS.items():
        if key in controls:
            roles[role] = controls.pop(key)
    unknown = set(roles).difference(_ROLE_NAMES)
    if unknown:
        raise TypeError(f"unknown field roles: {', '.join(sorted(map(str, unknown)))}")
    if roles:
        controls["roles"] = _freeze_mapping(
            {
                name: _validate_key_spec(f"{name}_key", value)
                for name, value in roles.items()
            }
        )


def _freeze_mapping(value: Mapping[Any, Any]) -> Mapping[Any, Any]:
    return MappingProxyType(
        {
            key: _freeze_mapping(item) if isinstance(item, Mapping) else item
            for key, item in value.items()
        }
    )


def _validate_alias(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise TypeError("alias must be a non-empty string or None")
    return value


def _validate_nullable(value: Any) -> bool | None:
    if value is not None and type(value) is not bool:
        raise TypeError("nullable must be bool or None")
    return value


def _validate_doc(value: Any) -> str | None:
    if value is not None and not isinstance(value, str):
        raise TypeError("doc must be a string or None")
    return value


def _validate_seq(value: Any) -> int | None:
    if value is None:
        return None
    if type(value) is not int or not 1 <= value <= MAX_FIELD_SEQ:
        raise TypeError(
            f"seq must be an integer between 1 and {MAX_FIELD_SEQ}, or None"
        )
    return value


def _validate_key_spec(name: str, value: Any) -> KeySpec:
    if isinstance(value, (bool, int, str)) and not (
        isinstance(value, str) and not value
    ):
        return value
    raise TypeError(f"{name} must be bool, int, or a non-empty string")
