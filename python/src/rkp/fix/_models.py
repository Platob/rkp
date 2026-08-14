"""Immutable FIX field definitions and RKP field projections."""

from __future__ import annotations

import dataclasses
import gzip
import io
import json
import keyword
import os
import re
import tempfile
import threading
import types
import zlib
from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from types import MappingProxyType
from typing import Any, TypeAlias, cast

from ..records import Field, Record, field, record
from ._errors import FixParseError
from ._paths import default_fix_dictionary_path

__all__ = [
    "FixComponent",
    "FixComponentMember",
    "FixDictionary",
    "FixEnumValue",
    "FixField",
    "FixFieldMember",
    "FixFieldSpec",
    "FixMessage",
    "FixRepeatingGroup",
    "FixStructureMember",
]

_SNAPSHOT_FORMAT = "rkp.fix.dictionary"
_SNAPSHOT_VERSION = 2
_MAX_STRUCTURE_DEPTH = 64
_INTEGER_TYPES = frozenset(
    {
        "DayOfMonth",
        "Int",
        "Length",
        "NumInGroup",
        "SeqNum",
        "TagNum",
    }
)
_DECIMAL_TYPES = frozenset(
    {"Amt", "float", "Percentage", "Price", "PriceOffset", "Qty"}
)
_BYTES_TYPES = frozenset({"data", "XMLData"})
_TIME_TYPES = frozenset({"LocalMktTime", "UTCTimeOnly"})
_DATE_TYPES = frozenset({"LocalMktDate", "UTCDateOnly"})
_TIMESTAMP_TYPES = frozenset({"TZTimestamp", "UTCTimestamp"})
_INTEGER_TYPES_NORMALIZED = frozenset(item.casefold() for item in _INTEGER_TYPES)
_DECIMAL_TYPES_NORMALIZED = frozenset(item.casefold() for item in _DECIMAL_TYPES)
_BYTES_TYPES_NORMALIZED = frozenset(item.casefold() for item in _BYTES_TYPES)
_DATE_TYPES_NORMALIZED = frozenset(item.casefold() for item in _DATE_TYPES)
_TIME_TYPES_NORMALIZED = frozenset(item.casefold() for item in _TIME_TYPES)
_TIMESTAMP_TYPES_NORMALIZED = frozenset(item.casefold() for item in _TIMESTAMP_TYPES)
_MAX_SNAPSHOT_BYTES = 64 * 1024 * 1024
_PATH_LOCK_GUARD = threading.Lock()
_PATH_LOCKS: dict[Path, threading.Lock] = {}


@dataclasses.dataclass(frozen=True, slots=True)
class FixEnumValue:
    """One exact FIX wire value and its human-readable meaning."""

    value: str
    description: str

    def __post_init__(self) -> None:
        if not isinstance(self.value, str) or not self.value:
            raise TypeError("FIX enum value must be a non-empty string")
        if not isinstance(self.description, str) or not self.description:
            raise TypeError("FIX enum description must be a non-empty string")


@dataclasses.dataclass(frozen=True, slots=True)
class FixFieldSpec:
    """A Python member name, annotation, and fresh RKP dataclass field."""

    name: str
    annotation: Any
    field: Field[Any]


@dataclasses.dataclass(frozen=True, slots=True)
class FixField:
    """A normalized OnixS FIX field definition."""

    tag: int
    name: str
    fix_type: str
    version: str
    description: str = ""
    values: tuple[FixEnumValue, ...] = ()
    source_url: str = ""
    status: str | None = None
    _python_name: str = dataclasses.field(init=False, repr=False, compare=False)
    _metadata: Mapping[str, Any] = dataclasses.field(
        init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        if type(self.tag) is not int or self.tag <= 0:
            raise TypeError("FIX tag must be a positive integer")
        for label, value in (
            ("name", self.name),
            ("type", self.fix_type),
            ("version", self.version),
        ):
            if not isinstance(value, str) or not value.strip():
                raise TypeError(f"FIX field {label} must be a non-empty string")
        if not isinstance(self.description, str):
            raise TypeError("FIX field description must be a string")
        if not isinstance(self.values, tuple) or not all(
            isinstance(item, FixEnumValue) for item in self.values
        ):
            raise TypeError("FIX field values must be a tuple of FixEnumValue")
        if len({item.value for item in self.values}) != len(self.values):
            raise ValueError(f"FIX field {self.tag} has duplicate enum values")
        if not isinstance(self.source_url, str):
            raise TypeError("FIX field source_url must be a string")
        if self.status is not None and not isinstance(self.status, str):
            raise TypeError("FIX field status must be a string or None")

        object.__setattr__(self, "name", self.name.strip())
        object.__setattr__(self, "fix_type", self.fix_type.strip())
        object.__setattr__(self, "version", self.version.strip())
        object.__setattr__(self, "description", self.description.strip())
        object.__setattr__(self, "_python_name", _python_identifier(self.name))
        payload: dict[str, Any] = {
            "fix.version": self.version,
            "fix.tag": self.tag,
            "fix.name": self.name,
            "fix.type": self.fix_type,
            "fix.source": "OnixS FIX Dictionary",
            "fix.source_url": self.source_url,
            "fix.values": tuple((item.value, item.description) for item in self.values),
        }
        if self.status:
            payload["fix.status"] = self.status
        object.__setattr__(self, "_metadata", MappingProxyType(payload))

    @property
    def python_name(self) -> str:
        """A deterministic valid Python identifier for the FIX name."""

        return self._python_name

    @property
    def annotation(self) -> type[Any]:
        """The semantic Python scalar type for this FIX data type."""

        return _python_type(self.fix_type)

    @property
    def metadata(self) -> Mapping[str, Any]:
        """Optimized immutable metadata shared with Arrow and other adapters."""

        return self._metadata

    def into_spec(
        self, *, required: bool = False, name: str | None = None
    ) -> FixFieldSpec:
        """Create a fresh RKP field specification for this definition.

        FIX requiredness belongs to a message/component context, so standalone
        fields default to optional. A fresh :class:`Field` is returned on each
        call because dataclass processing attaches ownership state to fields.
        """

        if type(required) is not bool:
            raise TypeError("required must be bool")
        selected_name = self.python_name if name is None else name
        if not isinstance(selected_name, str) or not selected_name.isidentifier():
            raise TypeError("field name must be a valid Python identifier")
        if keyword.iskeyword(selected_name):
            raise TypeError("field name must not be a Python keyword")

        annotation: Any = self.annotation
        options: dict[str, Any] = {
            "alias": self.name,
            "doc": self.description or None,
            "metadata": self.metadata,
            "nullable": not required,
            "seq": self.tag,
        }
        if required:
            generated = field(**options)
        else:
            annotation = annotation | None
            generated = field(default=None, **options)
        return FixFieldSpec(selected_name, annotation, cast(Field[Any], generated))


@dataclasses.dataclass(frozen=True, slots=True)
class FixFieldMember:
    """One scalar FIX field at a message or component position."""

    tag: int
    required: bool = False
    comment: str = ""

    def __post_init__(self) -> None:
        _validate_member_tag(self.tag)
        _validate_required(self.required)
        object.__setattr__(self, "comment", _normalized_text(self.comment, "comment"))


@dataclasses.dataclass(frozen=True, slots=True)
class FixComponentMember:
    """A named component embedded at one structure position."""

    name: str
    required: bool = False
    comment: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _non_empty_text(self.name, "component name"))
        _validate_required(self.required)
        object.__setattr__(self, "comment", _normalized_text(self.comment, "comment"))


@dataclasses.dataclass(frozen=True, slots=True)
class FixRepeatingGroup:
    """A NumInGroup counter and the ordered structure of each repeated entry."""

    tag: int
    members: tuple[FixStructureMember, ...]
    required: bool = False
    comment: str = ""

    def __post_init__(self) -> None:
        _validate_member_tag(self.tag)
        _validate_members(self.members)
        if not self.members:
            raise ValueError("FIX repeating group members must not be empty")
        _validate_required(self.required)
        object.__setattr__(self, "comment", _normalized_text(self.comment, "comment"))


FixStructureMember: TypeAlias = FixFieldMember | FixComponentMember | FixRepeatingGroup


@dataclasses.dataclass(frozen=True, slots=True)
class FixComponent:
    """A reusable, ordered FIX component definition."""

    name: str
    version: str
    members: tuple[FixStructureMember, ...]
    description: str = ""
    source_url: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _non_empty_text(self.name, "component name"))
        object.__setattr__(self, "version", _non_empty_text(self.version, "version"))
        _validate_members(self.members)
        object.__setattr__(
            self, "description", _normalized_text(self.description, "description")
        )
        object.__setattr__(
            self, "source_url", _normalized_text(self.source_url, "source_url")
        )


@dataclasses.dataclass(frozen=True, slots=True)
class FixMessage:
    """An ordered FIX message structure identified by its wire MsgType."""

    name: str
    msg_type: str
    version: str
    members: tuple[FixStructureMember, ...]
    description: str = ""
    source_url: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _non_empty_text(self.name, "message name"))
        object.__setattr__(
            self, "msg_type", _non_empty_text(self.msg_type, "message MsgType")
        )
        object.__setattr__(self, "version", _non_empty_text(self.version, "version"))
        _validate_members(self.members)
        object.__setattr__(
            self, "description", _normalized_text(self.description, "description")
        )
        object.__setattr__(
            self, "source_url", _normalized_text(self.source_url, "source_url")
        )


@dataclasses.dataclass(frozen=True, slots=True)
class FixDictionary:
    """A locally complete, indexed set of normalized FIX structures."""

    version: str
    fields: tuple[FixField, ...]
    source_url: str = "https://www.onixs.biz/fix-dictionary.html"
    components: tuple[FixComponent, ...] = ()
    messages: tuple[FixMessage, ...] = ()
    _by_tag: Mapping[int, FixField] = dataclasses.field(
        init=False, repr=False, compare=False
    )
    _by_name: Mapping[str, FixField] = dataclasses.field(
        init=False, repr=False, compare=False
    )
    _by_component_name: Mapping[str, FixComponent] = dataclasses.field(
        init=False, repr=False, compare=False
    )
    _by_message_type: Mapping[str, FixMessage] = dataclasses.field(
        init=False, repr=False, compare=False
    )
    _by_message_name: Mapping[str, FixMessage] = dataclasses.field(
        init=False, repr=False, compare=False
    )
    _component_record_cache: dict[tuple[str, str | None, str | None], type[Record]] = (
        dataclasses.field(init=False, repr=False, compare=False, default_factory=dict)
    )
    _message_record_cache: dict[tuple[str, str | None, str | None], type[Record]] = (
        dataclasses.field(init=False, repr=False, compare=False, default_factory=dict)
    )
    _record_cache_lock: threading.RLock = dataclasses.field(
        init=False,
        repr=False,
        compare=False,
        default_factory=threading.RLock,
    )

    def __post_init__(self) -> None:
        if not isinstance(self.version, str) or not self.version.strip():
            raise TypeError("FIX dictionary version must be a non-empty string")
        if not isinstance(self.source_url, str):
            raise TypeError("FIX dictionary source_url must be a string")
        if not isinstance(self.fields, tuple) or not all(
            isinstance(item, FixField) for item in self.fields
        ):
            raise TypeError("fields must be a tuple of FixField")
        if not isinstance(self.components, tuple) or not all(
            isinstance(item, FixComponent) for item in self.components
        ):
            raise TypeError("components must be a tuple of FixComponent")
        if not isinstance(self.messages, tuple) or not all(
            isinstance(item, FixMessage) for item in self.messages
        ):
            raise TypeError("messages must be a tuple of FixMessage")
        normalized_version = self.version.strip()
        ordered = tuple(sorted(self.fields, key=lambda item: item.tag))
        ordered_components = tuple(
            sorted(self.components, key=lambda item: item.name.casefold())
        )
        ordered_messages = tuple(
            sorted(
                self.messages,
                key=lambda item: (item.msg_type, item.name.casefold()),
            )
        )
        by_tag: dict[int, FixField] = {}
        by_name: dict[str, FixField] = {}
        for field_definition in ordered:
            if field_definition.version.casefold() != normalized_version.casefold():
                raise ValueError(
                    f"FIX field {field_definition.tag} version "
                    f"{field_definition.version!r} does not match "
                    f"dictionary version {normalized_version!r}"
                )
            if field_definition.tag in by_tag:
                raise ValueError(f"duplicate FIX tag {field_definition.tag}")
            folded = field_definition.name.casefold()
            if folded in by_name:
                raise ValueError(f"duplicate FIX field name {field_definition.name!r}")
            by_tag[field_definition.tag] = field_definition
            by_name[folded] = field_definition
        by_component_name: dict[str, FixComponent] = {}
        for component_definition in ordered_components:
            _validate_structure_version(
                component_definition.version,
                normalized_version,
                component_definition.name,
            )
            folded = component_definition.name.casefold()
            if folded in by_component_name:
                raise ValueError(
                    f"duplicate FIX component name {component_definition.name!r}"
                )
            by_component_name[folded] = component_definition
        by_message_type: dict[str, FixMessage] = {}
        by_message_name: dict[str, FixMessage] = {}
        for message_definition in ordered_messages:
            _validate_structure_version(
                message_definition.version,
                normalized_version,
                message_definition.name,
            )
            message_type = message_definition.msg_type
            folded_name = message_definition.name.casefold()
            if message_type in by_message_type:
                raise ValueError(
                    f"duplicate FIX MsgType {message_definition.msg_type!r}"
                )
            if folded_name in by_message_name:
                raise ValueError(
                    f"duplicate FIX message name {message_definition.name!r}"
                )
            by_message_type[message_type] = message_definition
            by_message_name[folded_name] = message_definition

        _validate_structure_references(
            ordered_components,
            ordered_messages,
            by_tag=by_tag,
            by_component_name=by_component_name,
        )
        object.__setattr__(self, "version", normalized_version)
        object.__setattr__(self, "fields", ordered)
        object.__setattr__(self, "components", ordered_components)
        object.__setattr__(self, "messages", ordered_messages)
        object.__setattr__(self, "_by_tag", MappingProxyType(by_tag))
        object.__setattr__(self, "_by_name", MappingProxyType(by_name))
        object.__setattr__(
            self, "_by_component_name", MappingProxyType(by_component_name)
        )
        object.__setattr__(self, "_by_message_type", MappingProxyType(by_message_type))
        object.__setattr__(self, "_by_message_name", MappingProxyType(by_message_name))

    def __len__(self) -> int:
        return len(self.fields)

    def __iter__(self):
        return iter(self.fields)

    def field(self, tag_or_name: int | str) -> FixField:
        """Look up a field in O(1) by tag or case-insensitive FIX name."""

        if type(tag_or_name) is int:
            try:
                return self._by_tag[tag_or_name]
            except KeyError:
                raise KeyError(f"unknown FIX tag {tag_or_name}") from None
        if isinstance(tag_or_name, str) and tag_or_name:
            if tag_or_name.isdecimal():
                tag = int(tag_or_name)
                if tag in self._by_tag:
                    return self._by_tag[tag]
            try:
                return self._by_name[tag_or_name.casefold()]
            except KeyError:
                raise KeyError(f"unknown FIX field {tag_or_name!r}") from None
        raise TypeError("FIX field lookup expects an integer tag or non-empty name")

    def component(self, name: str) -> FixComponent:
        """Look up a component in O(1) by its case-insensitive FIX name."""

        if not isinstance(name, str) or not name:
            raise TypeError("FIX component lookup expects a non-empty name")
        try:
            return self._by_component_name[name.casefold()]
        except KeyError:
            raise KeyError(f"unknown FIX component {name!r}") from None

    def message(self, msg_type_or_name: str) -> FixMessage:
        """Look up a message in O(1) by MsgType or case-insensitive name."""

        if not isinstance(msg_type_or_name, str) or not msg_type_or_name:
            raise TypeError("FIX message lookup expects a non-empty MsgType or name")
        item = self._by_message_type.get(msg_type_or_name)
        if item is None:
            item = self._by_message_name.get(msg_type_or_name.casefold())
        if item is None:
            raise KeyError(f"unknown FIX message {msg_type_or_name!r}")
        return item

    def into_component_record(
        self,
        component: str | FixComponent,
        *,
        name: str | None = None,
        module: str | None = None,
        **record_options: Any,
    ) -> type[Record]:
        """Build a nested, keyword-only RKP record for one FIX component."""

        definition = self._selected_component(component)
        cache_key = (definition.name.casefold(), name, module)
        if not record_options:
            with self._record_cache_lock:
                cached = self._component_record_cache.get(cache_key)
            if cached is not None:
                return cached
        class_name = (
            _python_class_identifier(definition.name)
            if name is None
            else _validated_record_name(name)
        )
        builder = _StructureRecordBuilder(self, module, record_options)
        result = builder.component(definition, class_name=class_name, top_level=True)
        if not record_options:
            with self._record_cache_lock:
                result = self._component_record_cache.setdefault(cache_key, result)
        return result

    def into_message_record(
        self,
        message: str | FixMessage,
        *,
        name: str | None = None,
        module: str | None = None,
        **record_options: Any,
    ) -> type[Record]:
        """Build a nested, keyword-only RKP record for one FIX message."""

        definition = self._selected_message(message)
        cache_key = (definition.msg_type, name, module)
        if not record_options:
            with self._record_cache_lock:
                cached = self._message_record_cache.get(cache_key)
            if cached is not None:
                return cached
        class_name = (
            _python_class_identifier(definition.name)
            if name is None
            else _validated_record_name(name)
        )
        builder = _StructureRecordBuilder(self, module, record_options)
        result = builder.message(definition, class_name=class_name)
        if not record_options:
            with self._record_cache_lock:
                result = self._message_record_cache.setdefault(cache_key, result)
        return result

    def specs(
        self,
        *,
        required: Iterable[int | str] = (),
        fields: Iterable[int | str] | None = None,
    ) -> tuple[FixFieldSpec, ...]:
        """Project selected definitions into fresh, collision-safe RKP specs."""

        selected = self._select(fields)
        required_values = _selector_values("required", required)
        required_tags = {self.field(value).tag for value in required_values}
        names: set[str] = set()
        result: list[FixFieldSpec] = []
        for definition in selected:
            base = definition.python_name
            candidate = base
            if candidate in names:
                candidate = f"{base}_{definition.tag}"
            counter = 2
            while candidate in names:
                candidate = f"{base}_{definition.tag}_{counter}"
                counter += 1
            names.add(candidate)
            result.append(
                definition.into_spec(
                    required=definition.tag in required_tags, name=candidate
                )
            )
        unknown_required = required_tags.difference(item.tag for item in selected)
        if unknown_required:
            values = ", ".join(map(str, sorted(unknown_required)))
            raise ValueError(f"required FIX fields are not selected: {values}")
        return tuple(result)

    def into_record(
        self,
        name: str = "FixRecord",
        *,
        required: Iterable[int | str] = (),
        fields: Iterable[int | str] | None = None,
        module: str | None = None,
        **record_options: Any,
    ) -> type[Record]:
        """Build a decorated ``Record`` class from selected FIX fields."""

        if (
            not isinstance(name, str)
            or not name.isidentifier()
            or keyword.iskeyword(name)
        ):
            raise TypeError("record name must be a valid non-keyword identifier")
        specs = self.specs(required=required, fields=fields)
        namespace: dict[str, Any] = {
            "__annotations__": {item.name: item.annotation for item in specs},
            "__module__": module or __name__,
        }
        namespace.update({item.name: item.field for item in specs})
        candidate = types.new_class(
            name, (Record,), {}, lambda ns: ns.update(namespace)
        )
        if "kw_only" in record_options and record_options["kw_only"] is not True:
            raise ValueError("generated FIX records must be keyword-only")
        options: dict[str, Any] = dict(record_options)
        options["kw_only"] = True
        return record(candidate, **options)

    def dump(
        self,
        destination: str | os.PathLike[str] | None = None,
        *,
        compress: bool | None = None,
    ) -> Path:
        """Atomically write a deterministic portable JSON or JSON.gz snapshot."""

        path = (
            default_fix_dictionary_path(self.version)
            if destination is None
            else Path(destination)
        )
        if compress is None:
            compress = path.suffix.casefold() in {".gz", ".gzip"}
        if type(compress) is not bool:
            raise TypeError("compress must be bool or None")
        data = _snapshot_bytes(self)
        if compress:
            data = gzip.compress(data, compresslevel=9, mtime=0)
        _atomic_write(path, data)
        return path

    def persist(self) -> Path:
        """Write this dictionary to its default ``~/.config/fix`` snapshot."""

        return self.dump()

    @classmethod
    def load(cls, source: str | os.PathLike[str]) -> FixDictionary:
        """Load and validate a portable dictionary snapshot."""

        path = Path(source)
        try:
            if path.stat().st_size > _MAX_SNAPSHOT_BYTES:
                raise FixParseError(
                    f"FIX dictionary snapshot exceeds {_MAX_SNAPSHOT_BYTES} bytes"
                )
            data = path.read_bytes()
            if data.startswith(b"\x1f\x8b"):
                with gzip.GzipFile(fileobj=io.BytesIO(data)) as stream:
                    data = stream.read(_MAX_SNAPSHOT_BYTES + 1)
                if len(data) > _MAX_SNAPSHOT_BYTES:
                    raise FixParseError(
                        "decompressed FIX dictionary snapshot exceeds "
                        f"{_MAX_SNAPSHOT_BYTES} bytes"
                    )
            payload = json.loads(data)
        except FixParseError:
            raise
        except (
            OSError,
            EOFError,
            UnicodeDecodeError,
            json.JSONDecodeError,
            zlib.error,
            RecursionError,
        ) as exc:
            raise FixParseError(
                f"cannot load FIX dictionary snapshot {path}: {exc}"
            ) from exc
        return _dictionary_from_payload(payload)

    @classmethod
    def load_default(cls, version: str) -> FixDictionary:
        """Load a version from its default ``~/.config/fix`` snapshot."""

        return cls.load(default_fix_dictionary_path(version))

    def _select(self, values: Iterable[int | str] | None) -> tuple[FixField, ...]:
        if values is None:
            return self.fields
        values = _selector_values("fields", values)
        selected: list[FixField] = []
        seen: set[int] = set()
        for value in values:
            item = self.field(value)
            if item.tag not in seen:
                seen.add(item.tag)
                selected.append(item)
        return tuple(sorted(selected, key=lambda item: item.tag))

    def _selected_component(self, value: str | FixComponent) -> FixComponent:
        if isinstance(value, str):
            return self.component(value)
        if not isinstance(value, FixComponent):
            raise TypeError("component must be a FIX component name or FixComponent")
        selected = self.component(value.name)
        if selected != value:
            raise ValueError(f"component {value.name!r} does not belong to dictionary")
        return selected

    def _selected_message(self, value: str | FixMessage) -> FixMessage:
        if isinstance(value, str):
            return self.message(value)
        if not isinstance(value, FixMessage):
            raise TypeError("message must be a FIX MsgType/name or FixMessage")
        selected = self.message(value.msg_type)
        if selected != value:
            raise ValueError(f"message {value.name!r} does not belong to dictionary")
        return selected


class _StructureRecordBuilder:
    """Build one connected graph of reusable dynamic record classes."""

    def __init__(
        self,
        dictionary: FixDictionary,
        module: str | None,
        record_options: Mapping[str, Any],
    ) -> None:
        if "kw_only" in record_options and record_options["kw_only"] is not True:
            raise ValueError("generated FIX records must be keyword-only")
        self.dictionary = dictionary
        self.module_key = module
        self.module = module or __name__
        self.share_components = not record_options
        self.record_options = dict(record_options)
        self.record_options["kw_only"] = True
        self.components: dict[str, type[Record]] = {}
        self.active_components: set[str] = set()

    def component(
        self,
        definition: FixComponent,
        *,
        class_name: str | None = None,
        top_level: bool = False,
    ) -> type[Record]:
        key = definition.name.casefold()
        cached = self.components.get(key)
        if cached is not None:
            return cached
        shared_key: tuple[str, str | None, str | None] | None = None
        if self.share_components:
            default_name = _python_class_identifier(definition.name)
            shared_name = (
                None if class_name is None or class_name == default_name else class_name
            )
            shared_key = (key, shared_name, self.module_key)
            with self.dictionary._record_cache_lock:
                cached = self.dictionary._component_record_cache.get(shared_key)
            if cached is not None:
                self.components[key] = cached
                return cached
        if key in self.active_components:
            raise ValueError(f"cyclic FIX component reference at {definition.name!r}")
        self.active_components.add(key)
        try:
            if top_level:
                _validate_unique_structure_tags(
                    definition.members,
                    dictionary=self.dictionary,
                    structure=f"component {definition.name!r}",
                )
            specs = self._member_specs(
                definition.members,
                owner_name=definition.name,
            )
            generated = self._record_type(
                class_name or _python_class_identifier(definition.name),
                specs,
                alias=definition.name,
                metadata={
                    "fix.version": definition.version,
                    "fix.kind": "component",
                    "fix.name": definition.name,
                    "fix.source": "OnixS FIX Dictionary",
                    "fix.source_url": definition.source_url,
                },
            )
            if shared_key is not None:
                with self.dictionary._record_cache_lock:
                    generated = self.dictionary._component_record_cache.setdefault(
                        shared_key, generated
                    )
            self.components[key] = generated
            return generated
        finally:
            self.active_components.remove(key)

    def message(self, definition: FixMessage, *, class_name: str) -> type[Record]:
        _validate_unique_structure_tags(
            definition.members,
            dictionary=self.dictionary,
            structure=f"message {definition.name!r}",
        )
        specs = self._member_specs(definition.members, owner_name=definition.name)
        return self._record_type(
            class_name,
            specs,
            alias=definition.name,
            metadata={
                "fix.version": definition.version,
                "fix.kind": "message",
                "fix.name": definition.name,
                "fix.msg_type": definition.msg_type,
                "fix.source": "OnixS FIX Dictionary",
                "fix.source_url": definition.source_url,
            },
        )

    def _member_specs(
        self,
        members: tuple[FixStructureMember, ...],
        *,
        owner_name: str,
    ) -> tuple[FixFieldSpec, ...]:
        names: set[str] = set()
        specs: list[FixFieldSpec] = []
        for member in members:
            if isinstance(member, FixFieldMember):
                field_definition = self.dictionary.field(member.tag)
                python_name = _collision_safe_name(
                    field_definition.python_name, field_definition.tag, names
                )
                specs.append(
                    self._scalar_spec(field_definition, member, python_name=python_name)
                )
            elif isinstance(member, FixComponentMember):
                component_definition = self.dictionary.component(member.name)
                python_name = _collision_safe_name(
                    _python_identifier(component_definition.name), "component", names
                )
                nested = self.component(component_definition)
                specs.append(
                    self._component_spec(
                        component_definition,
                        member,
                        nested,
                        python_name=python_name,
                    )
                )
            else:
                group_definition = self.dictionary.field(member.tag)
                python_name = _collision_safe_name(
                    group_definition.python_name, group_definition.tag, names
                )
                specs.append(
                    self._group_spec(
                        group_definition,
                        member,
                        python_name=python_name,
                        owner_name=owner_name,
                    )
                )
        return tuple(specs)

    def _scalar_spec(
        self,
        definition: FixField,
        member: FixFieldMember,
        *,
        python_name: str,
    ) -> FixFieldSpec:
        metadata = dict(definition.metadata)
        metadata.update(
            {
                "fix.kind": "field",
                "fix.required": member.required,
            }
        )
        if member.comment:
            metadata["fix.comment"] = member.comment
        return _member_spec(
            python_name,
            definition.annotation,
            alias=definition.name,
            required=member.required,
            doc=member.comment or definition.description,
            metadata=metadata,
            seq=definition.tag,
        )

    def _component_spec(
        self,
        definition: FixComponent,
        member: FixComponentMember,
        nested: type[Record],
        *,
        python_name: str,
    ) -> FixFieldSpec:
        metadata: dict[str, Any] = {
            "fix.version": definition.version,
            "fix.kind": "component",
            "fix.name": definition.name,
            "fix.component": definition.name,
            "fix.required": member.required,
            "fix.source": "OnixS FIX Dictionary",
            "fix.source_url": definition.source_url,
        }
        if member.comment:
            metadata["fix.comment"] = member.comment
        return _member_spec(
            python_name,
            nested,
            alias=definition.name,
            required=member.required,
            doc=member.comment or definition.description,
            metadata=metadata,
        )

    def _group_spec(
        self,
        definition: FixField,
        member: FixRepeatingGroup,
        *,
        python_name: str,
        owner_name: str,
    ) -> FixFieldSpec:
        entry_name = _python_class_identifier(f"{owner_name} {definition.name} Entry")
        entry = self._record_type(
            entry_name,
            self._member_specs(member.members, owner_name=definition.name),
            alias=f"{definition.name}Entry",
            metadata={
                "fix.version": definition.version,
                "fix.kind": "repeating_group_entry",
                "fix.name": definition.name,
                "fix.tag": definition.tag,
                "fix.source": "OnixS FIX Dictionary",
                "fix.source_url": definition.source_url,
            },
        )
        metadata = dict(definition.metadata)
        metadata.update(
            {
                "fix.kind": "repeating_group",
                "fix.repeating": True,
                "fix.required": member.required,
            }
        )
        if member.comment:
            metadata["fix.comment"] = member.comment
        group_annotation: Any = types.GenericAlias(tuple, (entry, Ellipsis))
        return _member_spec(
            python_name,
            group_annotation,
            alias=definition.name,
            required=member.required,
            doc=member.comment or definition.description,
            metadata=metadata,
            seq=definition.tag,
        )

    def _record_type(
        self,
        class_name: str,
        specs: tuple[FixFieldSpec, ...],
        *,
        alias: str,
        metadata: Mapping[str, Any],
    ) -> type[Record]:
        namespace: dict[str, Any] = {
            "__annotations__": {item.name: item.annotation for item in specs},
            "__module__": self.module,
        }
        namespace.update({item.name: item.field for item in specs})
        candidate = types.new_class(
            class_name, (Record,), {}, lambda ns: ns.update(namespace)
        )
        options = dict(self.record_options)
        options["alias"] = alias
        options["metadata"] = _merged_fix_metadata(
            metadata, options.get("metadata", ...)
        )
        return record(candidate, **options)


def _member_spec(
    name: str,
    annotation: Any,
    *,
    alias: str,
    required: bool,
    doc: str,
    metadata: Mapping[str, Any],
    seq: int | None = None,
) -> FixFieldSpec:
    options: dict[str, Any] = {
        "alias": alias,
        "doc": doc or None,
        "metadata": metadata,
        "nullable": not required,
    }
    if seq is not None:
        options["seq"] = seq
    if required:
        generated = field(**options)
    else:
        annotation = annotation | None
        generated = field(default=None, **options)
    return FixFieldSpec(name, annotation, cast(Field[Any], generated))


def _validate_member_tag(value: Any) -> None:
    if type(value) is not int or value <= 0:
        raise TypeError("FIX member tag must be a positive integer")


def _validate_required(value: Any) -> None:
    if type(value) is not bool:
        raise TypeError("FIX member required must be bool")


def _normalized_text(value: Any, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"FIX {name} must be a string")
    return value.strip()


def _non_empty_text(value: Any, name: str) -> str:
    result = _normalized_text(value, name)
    if not result:
        raise TypeError(f"FIX {name} must be a non-empty string")
    return result


def _validate_members(value: Any) -> None:
    if not isinstance(value, tuple) or not all(
        isinstance(item, (FixFieldMember, FixComponentMember, FixRepeatingGroup))
        for item in value
    ):
        raise TypeError(
            "FIX members must be a tuple of field, component, or repeating-group members"
        )


def _validate_structure_version(
    actual: str, expected: str, structure_name: str
) -> None:
    if actual.casefold() != expected.casefold():
        raise ValueError(
            f"FIX structure {structure_name!r} version {actual!r} does not match "
            f"dictionary version {expected!r}"
        )


def _validate_structure_references(
    components: tuple[FixComponent, ...],
    messages: tuple[FixMessage, ...],
    *,
    by_tag: Mapping[int, FixField],
    by_component_name: Mapping[str, FixComponent],
) -> None:
    def validate_members(
        members: tuple[FixStructureMember, ...], *, owner: str, depth: int = 0
    ) -> None:
        if depth > _MAX_STRUCTURE_DEPTH:
            raise ValueError(
                f"{owner} exceeds maximum FIX structure depth {_MAX_STRUCTURE_DEPTH}"
            )
        for member in members:
            if isinstance(member, FixComponentMember):
                if member.name.casefold() not in by_component_name:
                    raise ValueError(
                        f"{owner} references unknown FIX component {member.name!r}"
                    )
                continue
            field_definition = by_tag.get(member.tag)
            if field_definition is None:
                raise ValueError(f"{owner} references unknown FIX tag {member.tag}")
            if isinstance(member, FixRepeatingGroup):
                if field_definition.fix_type.casefold() != "numingroup":
                    raise ValueError(
                        f"{owner} repeating group tag {member.tag} must have "
                        "FIX type NumInGroup"
                    )
                validate_members(
                    member.members,
                    owner=f"{owner} repeating group {field_definition.name!r}",
                    depth=depth + 1,
                )

    for component in components:
        validate_members(component.members, owner=f"component {component.name!r}")
    for message in messages:
        validate_members(message.members, owner=f"message {message.name!r}")

    state: dict[str, int] = {}
    stack: list[str] = []

    def referenced_components(
        members: tuple[FixStructureMember, ...],
    ) -> Iterable[str]:
        for member in members:
            if isinstance(member, FixComponentMember):
                yield member.name
            elif isinstance(member, FixRepeatingGroup):
                yield from referenced_components(member.members)

    def visit(component: FixComponent) -> None:
        key = component.name.casefold()
        status = state.get(key, 0)
        if status == 2:
            return
        if status == 1:
            start = next(
                index for index, item in enumerate(stack) if item.casefold() == key
            )
            cycle = " -> ".join((*stack[start:], component.name))
            raise ValueError(f"cyclic FIX component reference: {cycle}")
        state[key] = 1
        stack.append(component.name)
        for name in referenced_components(component.members):
            visit(by_component_name[name.casefold()])
        stack.pop()
        state[key] = 2

    for component in components:
        visit(component)


def _validate_unique_structure_tags(
    members: tuple[FixStructureMember, ...],
    *,
    dictionary: FixDictionary,
    structure: str,
) -> None:
    seen: dict[int, str] = {}

    def visit(
        values: tuple[FixStructureMember, ...],
        path: str,
        active_components: frozenset[str],
    ) -> None:
        for member in values:
            if isinstance(member, FixComponentMember):
                component = dictionary.component(member.name)
                key = component.name.casefold()
                if key in active_components:
                    raise ValueError(
                        f"cyclic FIX component reference at {component.name!r}"
                    )
                visit(
                    component.members,
                    f"{path}.{component.name}",
                    active_components | {key},
                )
                continue
            definition = dictionary.field(member.tag)
            field_path = f"{path}.{definition.name}"
            previous = seen.get(member.tag)
            if previous is not None:
                raise ValueError(
                    f"{structure} contains duplicate FIX tag {member.tag} at "
                    f"{previous} and {field_path}; explicit seq values must be unique"
                )
            seen[member.tag] = field_path
            if isinstance(member, FixRepeatingGroup):
                visit(member.members, field_path, active_components)

    visit(members, structure, frozenset())


def _collision_safe_name(base: str, suffix: int | str, names: set[str]) -> str:
    candidate = base
    if candidate in names:
        candidate = f"{base}_{suffix}"
    counter = 2
    while candidate in names:
        candidate = f"{base}_{suffix}_{counter}"
        counter += 1
    names.add(candidate)
    return candidate


def _validated_record_name(value: Any) -> str:
    if (
        not isinstance(value, str)
        or not value.isidentifier()
        or keyword.iskeyword(value)
    ):
        raise TypeError("record name must be a valid non-keyword identifier")
    return value


def _python_class_identifier(value: str) -> str:
    words = re.sub(r"[^0-9A-Za-z_]+", " ", value).replace("_", " ").split()
    result = "".join(word[:1].upper() + word[1:] for word in words) or "FixRecord"
    if result[0].isdigit():
        result = f"Fix{result}"
    if keyword.iskeyword(result):
        result += "Record"
    return _validated_record_name(result)


def _merged_fix_metadata(
    identity: Mapping[str, Any], supplied: Any
) -> Mapping[str, Any]:
    if supplied is ... or supplied is None:
        return dict(identity)
    if not isinstance(supplied, Mapping):
        raise TypeError("record metadata must be a mapping, None, or Ellipsis")
    result = dict(supplied)
    result.update(identity)
    return result


def _python_type(fix_type: str) -> type[Any]:
    normalized = fix_type.casefold()
    if normalized == "boolean":
        return bool
    if normalized in _INTEGER_TYPES_NORMALIZED:
        return int
    if normalized in _DECIMAL_TYPES_NORMALIZED:
        return Decimal
    if normalized in _BYTES_TYPES_NORMALIZED:
        return bytes
    if normalized in _TIMESTAMP_TYPES_NORMALIZED:
        return datetime
    if normalized in _DATE_TYPES_NORMALIZED:
        return date
    if normalized in _TIME_TYPES_NORMALIZED:
        return time
    return str


def _selector_values(name: str, value: Iterable[int | str]) -> Iterable[int | str]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Iterable):
        raise TypeError(f"{name} must be an iterable of FIX tags or names")
    return value


def _python_identifier(value: str) -> str:
    # Split acronym boundaries without mangling familiar FIX names such as ID.
    result = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    result = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", result)
    result = re.sub(r"\W+", "_", result).strip("_").lower() or "field"
    # Preserve plural acronym suffixes used throughout FIX (IDs, MICs, ...).
    result = re.sub(r"_([a-z])_([a-z])s\b", r"_\1\2s", result)
    if result[0].isdigit():
        result = f"field_{result}"
    if keyword.iskeyword(result):
        result += "_"
    return result


def _snapshot_bytes(dictionary: FixDictionary) -> bytes:
    payload = {
        "format": _SNAPSHOT_FORMAT,
        "format_version": _SNAPSHOT_VERSION,
        "version": dictionary.version,
        "source_url": dictionary.source_url,
        "fields": [
            {
                "tag": item.tag,
                "name": item.name,
                "type": item.fix_type,
                "description": item.description,
                "values": [dataclasses.asdict(value) for value in item.values],
                "source_url": item.source_url,
                "status": item.status,
            }
            for item in dictionary.fields
        ],
        "components": [
            {
                "name": item.name,
                "version": item.version,
                "members": [_member_payload(member) for member in item.members],
                "description": item.description,
                "source_url": item.source_url,
            }
            for item in dictionary.components
        ],
        "messages": [
            {
                "name": item.name,
                "msg_type": item.msg_type,
                "version": item.version,
                "members": [_member_payload(member) for member in item.members],
                "description": item.description,
                "source_url": item.source_url,
            }
            for item in dictionary.messages
        ],
    }
    return json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _dictionary_from_payload(payload: Any) -> FixDictionary:
    if not isinstance(payload, Mapping):
        raise FixParseError("FIX dictionary snapshot must contain an object")
    if payload.get("format") != _SNAPSHOT_FORMAT:
        raise FixParseError("unsupported FIX dictionary snapshot format")
    format_version = payload.get("format_version")
    if type(format_version) is not int or format_version not in (
        1,
        _SNAPSHOT_VERSION,
    ):
        raise FixParseError("unsupported FIX dictionary snapshot version")
    raw_fields = payload.get("fields")
    if not isinstance(raw_fields, Sequence) or isinstance(raw_fields, (str, bytes)):
        raise FixParseError("FIX dictionary snapshot fields must be a list")
    try:
        fields = tuple(
            FixField(
                tag=item["tag"],
                name=item["name"],
                fix_type=item["type"],
                version=payload["version"],
                description=item.get("description", ""),
                values=tuple(FixEnumValue(**value) for value in item.get("values", [])),
                source_url=item.get("source_url", ""),
                status=item.get("status"),
            )
            for item in raw_fields
        )
        components: tuple[FixComponent, ...] = ()
        messages: tuple[FixMessage, ...] = ()
        if format_version >= 2:
            raw_components = _snapshot_list(payload, "components")
            raw_messages = _snapshot_list(payload, "messages")
            components = tuple(
                FixComponent(
                    name=item["name"],
                    version=item.get("version", payload["version"]),
                    members=_members_from_payload(item.get("members")),
                    description=item.get("description", ""),
                    source_url=item.get("source_url", ""),
                )
                for item in raw_components
            )
            messages = tuple(
                FixMessage(
                    name=item["name"],
                    msg_type=item["msg_type"],
                    version=item.get("version", payload["version"]),
                    members=_members_from_payload(item.get("members")),
                    description=item.get("description", ""),
                    source_url=item.get("source_url", ""),
                )
                for item in raw_messages
            )
        return FixDictionary(
            version=payload["version"],
            fields=fields,
            source_url=payload.get("source_url", ""),
            components=components,
            messages=messages,
        )
    except (KeyError, TypeError, ValueError, RecursionError) as exc:
        raise FixParseError(f"invalid FIX dictionary snapshot: {exc}") from exc


def _member_payload(member: FixStructureMember) -> dict[str, Any]:
    common: dict[str, Any] = {
        "required": member.required,
        "comment": member.comment,
    }
    if isinstance(member, FixFieldMember):
        return {"kind": "field", "tag": member.tag, **common}
    if isinstance(member, FixComponentMember):
        return {"kind": "component", "name": member.name, **common}
    return {
        "kind": "repeating_group",
        "tag": member.tag,
        "members": [_member_payload(item) for item in member.members],
        **common,
    }


def _snapshot_list(payload: Mapping[Any, Any], name: str) -> Sequence[Any]:
    value = payload.get(name, [])
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError(f"FIX dictionary snapshot {name} must be a list")
    return value


def _members_from_payload(
    value: Any, *, depth: int = 0
) -> tuple[FixStructureMember, ...]:
    if depth > _MAX_STRUCTURE_DEPTH:
        raise ValueError(f"FIX structure exceeds maximum depth {_MAX_STRUCTURE_DEPTH}")
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError("FIX structure members must be a list")
    result: list[FixStructureMember] = []
    for item in value:
        if not isinstance(item, Mapping):
            raise TypeError("FIX structure member must be an object")
        common = {
            "required": item.get("required", False),
            "comment": item.get("comment", ""),
        }
        kind = item.get("kind")
        if kind == "field":
            result.append(FixFieldMember(tag=item["tag"], **common))
        elif kind == "component":
            result.append(FixComponentMember(name=item["name"], **common))
        elif kind in {"group", "repeating_group"}:
            result.append(
                FixRepeatingGroup(
                    tag=item["tag"],
                    members=_members_from_payload(item.get("members"), depth=depth + 1),
                    **common,
                )
            )
        else:
            raise ValueError(f"unknown FIX structure member kind {kind!r}")
    return tuple(result)


def _atomic_write(path: Path, data: bytes) -> None:
    resolved = path.resolve(strict=False)
    with _PATH_LOCK_GUARD:
        lock = _PATH_LOCKS.setdefault(resolved, threading.Lock())
    with lock:
        temporary: Path | None = None
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(
                mode="wb", dir=path.parent, prefix=f".{path.name}.", delete=False
            ) as stream:
                temporary = Path(stream.name)
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        except OSError as exc:
            if temporary is not None:
                try:
                    temporary.unlink(missing_ok=True)
                except OSError:
                    pass
            raise FixParseError(
                f"cannot write FIX dictionary snapshot {path}: {exc}"
            ) from exc
