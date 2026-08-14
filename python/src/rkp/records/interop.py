"""Dependency-free dataclass and record conversion utilities."""

from __future__ import annotations

import collections
import collections.abc as cabc
import copy
import dataclasses
import datetime as dt
import enum
import pathlib
import sys
import types
import typing
import uuid
from contextvars import ContextVar
from decimal import Decimal
from functools import cache
from typing import Any, TypeVar

from .fields import FieldOptions, _options_from_mapping, field_options

__all__ = [
    "dataclass_from_dict",
    "is_record",
    "is_record_type",
    "record_from_dict",
    "resolved_type_hints",
    "serialized_field_name",
    "to_dict",
]

T = TypeVar("T")
_NONE_TYPE = type(None)
_UNION_ORIGINS = (typing.Union, types.UnionType)
_FALLBACK_STACK: ContextVar[tuple[Any, ...]] = ContextVar(
    "rkp_record_fallback_stack", default=()
)
_WRAPPER_ORIGINS = tuple(
    wrapper
    for wrapper in (
        getattr(typing, "Final", None),
        getattr(typing, "Required", None),
        getattr(typing, "NotRequired", None),
        getattr(typing, "ReadOnly", None),
        getattr(typing, "ClassVar", None),
    )
    if wrapper is not None
)


@dataclasses.dataclass(frozen=True, slots=True)
class _DataclassInputPlan:
    dataclass_type: type[Any]
    hints: cabc.Mapping[str, Any]
    all_fields: tuple[dataclasses.Field[Any], ...]
    fields: tuple[dataclasses.Field[Any], ...]
    by_input: cabc.Mapping[str, dataclasses.Field[Any]]
    non_init_by_input: cabc.Mapping[str, dataclasses.Field[Any]]


@dataclasses.dataclass(frozen=True, slots=True)
class _AnnotationPlan:
    annotation: Any
    origin: Any
    arguments: tuple[Any, ...]
    dataclass_spec: tuple[type[Any], dict[typing.TypeVar, Any]] | None
    typed_dict_spec: tuple[type[Any], dict[typing.TypeVar, Any]] | None
    named_tuple_spec: tuple[type[Any], dict[typing.TypeVar, Any]] | None
    is_mapping: bool
    is_collection: bool


def is_record_type(value: Any) -> bool:
    """Return whether ``value`` is a decorated ``Record`` class."""

    return isinstance(value, type) and value.__dict__.get("__rkp_record__") is True


def is_record(value: Any) -> bool:
    """Return whether ``value`` is an instance of a decorated record."""

    return is_record_type(type(value))


def serialized_field_name(
    dc_field: dataclasses.Field[Any], annotation: Any | None = None
) -> str:
    """Return the wire/schema name shared by codecs and Arrow fields."""

    if not isinstance(dc_field, dataclasses.Field):
        raise TypeError("serialized_field_name expects a dataclasses.Field")
    options = field_options(dc_field)
    if options.has("alias"):
        alias = options.alias
    else:
        alias = _annotated_alias(annotation) if annotation is not None else None
    return alias or dc_field.name


def resolved_type_hints(
    cls: type[Any], *, localns: cabc.Mapping[str, Any] | None = None
) -> dict[str, Any]:
    """Resolve direct annotations for every class in ``cls``'s MRO.

    Resolution happens per defining class so a subclass's class-local aliases
    cannot reinterpret inherited annotations.  The decorator's captured local
    namespace also makes function-local and postponed references work.
    """

    if not isinstance(cls, type):
        raise TypeError("type hints can only be resolved for a class")
    if localns is not None and not isinstance(localns, cabc.Mapping):
        raise TypeError("localns must be a mapping or None")
    if localns is None:
        return dict(_resolved_type_hint_items(typing.cast(Any, cls)))
    return _resolve_type_hints(cls, localns=localns)


@cache
def _resolved_type_hint_items(cls: type[Any]) -> tuple[tuple[str, Any], ...]:
    """Cache immutable hint plans used by every row conversion hot path."""

    return tuple(_resolve_type_hints(cls).items())


def _resolve_type_hints(
    cls: type[Any], *, localns: cabc.Mapping[str, Any] | None = None
) -> dict[str, Any]:

    supplied_localns = localns
    result: dict[str, Any] = {}
    inherited_namespace: dict[str, Any] = {}
    for base in reversed(cls.__mro__):
        direct = base.__dict__.get("__annotations__")  # noqa: RUF063
        # Python 3.14 may store annotations behind ``__annotate_func__``
        # rather than directly in the class dictionary.  Access the descriptor
        # only for a class that owns that function, avoiding inherited hints.
        if direct is None and "__annotate_func__" in base.__dict__:
            direct = getattr(base, "__annotations__", {})
        if direct is None:
            direct = {}
        module = sys.modules.get(base.__module__)
        globalns = vars(module) if module is not None else {}
        local_namespace = dict(inherited_namespace)
        local_namespace.update(base.__dict__.get("__rkp_localns__", {}))
        local_namespace.update(vars(base))
        local_namespace[base.__name__] = base
        if base is cls and supplied_localns is not None:
            local_namespace.update(supplied_localns)
        for parameter in (
            *getattr(base, "__parameters__", ()),
            *getattr(base, "__type_params__", ()),
        ):
            local_namespace.setdefault(parameter.__name__, parameter)
        if direct:
            probe = type(
                f"_{base.__name__}RecordHints",
                (),
                {
                    "__annotations__": dict(direct),
                    "__module__": base.__module__,
                },
            )
            try:
                all_hints = typing.get_type_hints(
                    probe,
                    globalns=globalns,
                    localns=local_namespace,
                    include_extras=True,
                )
            except (NameError, TypeError) as exc:
                raise TypeError(
                    f"cannot resolve annotations for {base.__qualname__}: {exc}"
                ) from exc
            for name in direct:
                if name in all_hints:
                    result[name] = all_hints[name]
        inherited_namespace.update(vars(base))
        inherited_namespace.update(local_namespace)
    return result


def dataclass_from_dict(
    cls: type[T],
    datum: cabc.Mapping[str, Any],
    safe: bool = True,
    on_error: str = "raise",
) -> T:
    """Construct any dataclass recursively from a mapping.

    Custom record field aliases are accepted as input names.  In unsafe mode
    this deliberately performs no alias translation or conversion.
    """

    dataclass_spec = _annotation_plan(cls).dataclass_spec
    if dataclass_spec is None:
        raise TypeError("cls must be a dataclass type")
    dataclass_type, typevars = dataclass_spec
    if type(safe) is not bool:
        raise TypeError("safe must be bool")
    if on_error not in {"raise", "default"}:
        raise ValueError("on_error must be 'raise' or 'default'")
    if not isinstance(datum, cabc.Mapping):
        raise TypeError(f"{dataclass_type.__qualname__} data must be a mapping")
    if not safe:
        return dataclass_type(**dict(datum))
    hints: cabc.Mapping[str, Any]
    if typevars:
        hints = {
            name: _substitute_typevars(annotation, typevars)
            for name, annotation in resolved_type_hints(dataclass_type).items()
        }
        plan = _build_dataclass_input_plan(dataclass_type, hints)
    else:
        plan = _dataclass_input_plan(typing.cast(Any, dataclass_type))
        hints = plan.hints
    all_fields = plan.all_fields
    fields = plan.fields
    by_input = plan.by_input

    supplied: dict[str, tuple[str, Any]] = {}
    unknown: list[Any] = []
    non_init_by_input = plan.non_init_by_input
    for key, value in datum.items():
        dc_field = by_input.get(key) if isinstance(key, str) else None
        if dc_field is None:
            non_init = non_init_by_input.get(key) if isinstance(key, str) else None
            if non_init is not None:
                if non_init.name in supplied:
                    raise TypeError(
                        f"{dataclass_type.__qualname__}.{non_init.name} "
                        "was provided twice"
                    )
                supplied[non_init.name] = (key, value)
                continue
            unknown.append(key)
            continue
        if dc_field.name in supplied:
            previous = supplied[dc_field.name][0]
            raise TypeError(
                f"{dataclass_type.__qualname__}.{dc_field.name} "
                "was provided as both "
                f"{previous!r} and {key!r}"
            )
        supplied[dc_field.name] = (key, value)
    if unknown:
        rendered = ", ".join(repr(key) for key in unknown)
        raise TypeError(
            f"unexpected field(s) for {dataclass_type.__qualname__}: {rendered}"
        )

    kwargs: dict[str, Any] = {}
    for dc_field in fields:
        annotation = hints.get(dc_field.name, dc_field.type)
        path = f"{dataclass_type.__qualname__}.{dc_field.name}"
        entry = supplied.get(dc_field.name)
        if entry is None:
            if _has_declared_default(dc_field):
                continue
            if on_error == "default":
                kwargs[dc_field.name] = _fallback_value(dc_field, annotation, path)
                continue
            raise TypeError(f"missing required field {path}")
        try:
            kwargs[dc_field.name] = _coerce(
                entry[1], annotation, path, on_error=on_error
            )
        except (TypeError, ValueError, OverflowError) as exc:
            if on_error == "default":
                kwargs[dc_field.name] = _fallback_value(dc_field, annotation, path)
                continue
            if str(exc).startswith(path):
                raise
            raise TypeError(f"{path}: {exc}") from exc

    try:
        result = dataclass_type(**kwargs)
    except (TypeError, ValueError) as exc:
        raise TypeError(
            f"cannot construct {dataclass_type.__qualname__}: {exc}"
        ) from exc
    for dc_field in all_fields:
        entry = supplied.get(dc_field.name)
        if dc_field.init or entry is None:
            continue
        annotation = hints.get(dc_field.name, dc_field.type)
        value = entry[1]
        path = f"{dataclass_type.__qualname__}.{dc_field.name}"
        try:
            converted = _coerce(value, annotation, path, on_error=on_error)
        except (TypeError, ValueError, OverflowError):
            if on_error != "default":
                raise
            converted = _fallback_value(dc_field, annotation, path)
        try:
            object.__setattr__(result, dc_field.name, converted)
        except (AttributeError, TypeError) as exc:
            raise TypeError(f"cannot restore non-init field {path}: {exc}") from exc
    return result


@cache
def _dataclass_input_plan(dataclass_type: type[Any]) -> _DataclassInputPlan:
    return _build_dataclass_input_plan(
        dataclass_type, resolved_type_hints(dataclass_type)
    )


def _build_dataclass_input_plan(
    dataclass_type: type[Any], hints: cabc.Mapping[str, Any]
) -> _DataclassInputPlan:
    all_fields = tuple(dataclasses.fields(dataclass_type))
    fields = tuple(
        dc_field
        for dc_field in dataclass_type.__dataclass_fields__.values()
        if dc_field.init
        and getattr(dc_field, "_field_type", None)
        is not getattr(dataclasses, "_FIELD_CLASSVAR", ...)
    )
    by_input: dict[str, dataclasses.Field[Any]] = {}
    non_init_by_input: dict[str, dataclasses.Field[Any]] = {}
    for dc_field in all_fields:
        wire_name = serialized_field_name(
            dc_field, hints.get(dc_field.name, dc_field.type)
        )
        target = by_input if dc_field.init else non_init_by_input
        for input_name in {dc_field.name, wire_name}:
            prior = target.get(typing.cast(str, input_name))
            if prior is not None and prior is not dc_field:
                raise TypeError(
                    "duplicate input name "
                    f"{input_name!r} on {dataclass_type.__qualname__}"
                )
            target[typing.cast(str, input_name)] = dc_field
    return _DataclassInputPlan(
        dataclass_type,
        hints,
        all_fields,
        fields,
        by_input,
        non_init_by_input,
    )


def record_from_dict(
    cls: type[T],
    datum: cabc.Mapping[str, Any],
    safe: bool = True,
    on_error: str = "raise",
) -> T:
    """Construct a decorated record from a mapping."""

    if not is_record_type(cls):
        raise TypeError("cls must be a decorated record type")
    return dataclass_from_dict(cls, datum, safe=safe, on_error=on_error)


def to_dict(datum: Any, *, by_alias: bool = True) -> Any:
    """Recursively normalize records/dataclasses into plain Python values."""

    return _to_plain(datum, by_alias=by_alias, active=set(), path="$", key=False)


def _to_plain(
    value: Any,
    *,
    by_alias: bool,
    active: set[int],
    path: str,
    key: bool,
) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, enum.Enum):
        return _to_plain(
            value.value, by_alias=by_alias, active=active, path=path, key=key
        )
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()
    if isinstance(value, dt.timedelta):
        return value.total_seconds()
    if isinstance(value, (pathlib.PurePath, uuid.UUID, Decimal)):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)

    tracked = dataclasses.is_dataclass(value) or isinstance(
        value,
        (
            cabc.Mapping,
            list,
            tuple,
            set,
            frozenset,
            collections.deque,
            range,
        ),
    )
    identity = id(value)
    if tracked:
        if identity in active:
            raise ValueError(f"cyclic value at {path}")
        active.add(identity)
    try:
        if dataclasses.is_dataclass(value) and not isinstance(value, type):
            result: dict[str, Any] = {}
            try:
                hints = resolved_type_hints(type(value))
            except TypeError:
                hints = {}
            for dc_field in dataclasses.fields(value):
                name = (
                    serialized_field_name(
                        dc_field, hints.get(dc_field.name, dc_field.type)
                    )
                    if by_alias
                    else dc_field.name
                )
                if name in result:
                    raise ValueError(
                        f"duplicate serialized field name {name!r} at {path}"
                    )
                result[name] = _to_plain(
                    getattr(value, dc_field.name),
                    by_alias=by_alias,
                    active=active,
                    path=f"{path}.{name}",
                    key=False,
                )
            return result
        if isinstance(value, cabc.Mapping):
            return {
                _to_plain(
                    item_key,
                    by_alias=by_alias,
                    active=active,
                    path=f"{path}.<key>",
                    key=True,
                ): _to_plain(
                    item_value,
                    by_alias=by_alias,
                    active=active,
                    path=f"{path}[{item_key!r}]",
                    key=False,
                )
                for item_key, item_value in value.items()
            }
        if isinstance(value, (list, tuple, set, frozenset, collections.deque, range)):
            converted = [
                _to_plain(
                    item,
                    by_alias=by_alias,
                    active=active,
                    path=f"{path}[{index}]",
                    key=key,
                )
                for index, item in enumerate(value)
            ]
            return tuple(converted) if key else converted
        return value
    finally:
        if tracked:
            active.remove(identity)


def _coerce(value: Any, annotation: Any, path: str, *, on_error: str = "raise") -> Any:
    plan = _annotation_plan(annotation)
    annotation = plan.annotation
    origin = plan.origin
    args = plan.arguments

    if annotation in (Any, object) or annotation is typing.Any:
        return value
    if annotation in (None, _NONE_TYPE):
        if value is None:
            return None
        raise TypeError(f"{path}: expected None")

    if origin in _UNION_ORIGINS:
        if value is None and _NONE_TYPE in args:
            return None
        branches = tuple(item for item in args if item is not _NONE_TYPE)
        for branch in branches:
            if _matches(value, branch):
                try:
                    return _coerce(value, branch, path, on_error=on_error)
                except (TypeError, ValueError, OverflowError):
                    pass
        errors: list[str] = []
        for branch in branches:
            try:
                return _coerce(value, branch, path, on_error=on_error)
            except (TypeError, ValueError, OverflowError) as exc:
                errors.append(str(exc))
        raise TypeError(f"{path}: value does not match {annotation!r}")

    if origin is typing.Literal:
        if any(value == choice and type(value) is type(choice) for choice in args):
            return value
        raise ValueError(f"{path}: expected one of {args!r}")

    if value is None:
        raise TypeError(f"{path}: None is not allowed")

    if isinstance(annotation, typing.TypeVar):
        if annotation.__bound__ is not None:
            return _coerce(value, annotation.__bound__, path, on_error=on_error)
        # A constrained TypeVar is a union-like choice.  Prefer a branch the
        # value already inhabits before attempting casts, otherwise a string
        # such as ``"7"`` would be changed to ``7`` for ``TypeVar(T, int,
        # str)`` merely because ``int`` was declared first.
        for constraint in annotation.__constraints__:
            if _matches(value, constraint):
                return _coerce(value, constraint, path, on_error=on_error)
        for constraint in annotation.__constraints__:
            try:
                return _coerce(value, constraint, path, on_error=on_error)
            except (TypeError, ValueError, OverflowError):
                pass
        return value

    dataclass_spec = plan.dataclass_spec
    if dataclass_spec is not None:
        dataclass_type, _ = dataclass_spec
        if isinstance(value, dataclass_type):
            return value
        if isinstance(value, cabc.Mapping):
            return dataclass_from_dict(annotation, value, safe=True, on_error=on_error)
        raise TypeError(f"{path}: expected {dataclass_type.__qualname__} or a mapping")

    if isinstance(annotation, type) and issubclass(annotation, enum.Enum):
        if isinstance(value, annotation):
            return value
        try:
            return annotation(value)
        except (TypeError, ValueError):
            if isinstance(value, str):
                try:
                    return annotation[value]
                except KeyError:
                    pass
        raise ValueError(f"{path}: invalid {annotation.__qualname__} value")

    typed_dict_spec = plan.typed_dict_spec
    if typed_dict_spec is not None:
        typed_dict_type, typevars = typed_dict_spec
        if not isinstance(value, cabc.Mapping):
            raise TypeError(f"{path}: expected a mapping")
        hints = {
            name: _substitute_typevars(field_type, typevars)
            for name, field_type in typing.get_type_hints(
                typed_dict_type, include_extras=True
            ).items()
        }
        unknown = set(value) - set(hints)
        if unknown:
            raise TypeError(f"{path}: unexpected keys {sorted(unknown)!r}")
        required = _typed_dict_required_keys(typed_dict_type, hints)
        missing = required - set(value)
        if missing:
            raise TypeError(f"{path}: missing keys {sorted(missing)!r}")
        return {
            key_name: _coerce(
                item,
                hints[key_name],
                f"{path}.{key_name}",
                on_error=on_error,
            )
            for key_name, item in value.items()
        }

    named_tuple_spec = plan.named_tuple_spec
    if named_tuple_spec is not None:
        named_tuple_type, typevars = named_tuple_spec
        hints = {
            name: _substitute_typevars(field_type, typevars)
            for name, field_type in typing.get_type_hints(
                named_tuple_type, include_extras=True
            ).items()
        }
        names = named_tuple_type._fields
        if isinstance(value, cabc.Mapping):
            if set(value) != set(names):
                raise ValueError(f"{path}: expected fields {names!r}")
            value = tuple(value[name] for name in names)
        if not isinstance(value, (list, tuple)):
            raise TypeError(f"{path}: expected a sequence or field mapping")
        if len(value) != len(names):
            raise ValueError(f"{path}: expected {len(names)} tuple items")
        converted = [
            _coerce(
                item,
                hints.get(name, Any),
                f"{path}.{name}",
                on_error=on_error,
            )
            for name, item in zip(names, value)
        ]
        return named_tuple_type(*converted)

    if origin is tuple or annotation is tuple:
        if isinstance(value, cabc.Mapping):
            ordered_keys = tuple(f"_{index}" for index in range(1, len(value) + 1))
            if set(value) != set(ordered_keys):
                raise ValueError(f"{path}: expected fields {ordered_keys!r}")
            value = tuple(value[key] for key in ordered_keys)
        if not isinstance(value, (list, tuple)):
            raise TypeError(f"{path}: expected a sequence or field mapping")
        if not args:
            return tuple(value)
        if len(args) == 2 and args[1] is Ellipsis:
            return tuple(
                _coerce(item, args[0], f"{path}[{index}]", on_error=on_error)
                for index, item in enumerate(value)
            )
        if len(value) != len(args):
            raise ValueError(f"{path}: expected {len(args)} tuple items")
        return tuple(
            _coerce(item, item_type, f"{path}[{index}]", on_error=on_error)
            for index, (item, item_type) in enumerate(zip(value, args))
        )

    if plan.is_mapping:
        mapping_items: cabc.Iterable[tuple[Any, Any]]
        if isinstance(value, cabc.Mapping):
            mapping_items = value.items()
        elif isinstance(value, (list, tuple)):
            mapping_items = _strict_mapping_items(value, path)
        else:
            raise TypeError(f"{path}: expected a mapping or key-value pairs")
        mapping_type = origin or annotation
        mapping_key_type: Any
        mapping_value_type: Any
        if _safe_issubclass(mapping_type, collections.Counter) and len(args) == 1:
            mapping_key_type, mapping_value_type = args[0], int
        else:
            mapping_key_type, mapping_value_type = (
                args if len(args) == 2 else (Any, Any)
            )
        converted_mapping = {
            _coerce(
                item_key,
                mapping_key_type,
                f"{path}.<key>",
                on_error=on_error,
            ): _coerce(
                item_value,
                mapping_value_type,
                f"{path}[{item_key!r}]",
                on_error=on_error,
            )
            for item_key, item_value in mapping_items
        }
        return _construct_mapping(origin or annotation, converted_mapping)

    if plan.is_collection:
        if isinstance(value, (str, bytes, bytearray)) or not isinstance(
            value, cabc.Iterable
        ):
            raise TypeError(f"{path}: expected a collection")
        collection_type = origin or annotation
        if _safe_issubclass(collection_type, cabc.ItemsView) and len(args) == 2:
            collection_item_type: Any = types.GenericAlias(tuple, args)
        else:
            collection_item_type = args[0] if args else Any
        converted_collection = [
            _coerce(
                item,
                collection_item_type,
                f"{path}[{index}]",
                on_error=on_error,
            )
            for index, item in enumerate(value)
        ]
        return _construct_collection(origin or annotation, converted_collection)

    if annotation is bool:
        return _coerce_bool(value, path)
    if annotation in (int, float) and type(value) is bool:
        raise TypeError(f"{path}: bool is not a valid {annotation.__name__}")
    if annotation is bytes:
        if isinstance(value, bytes):
            return value
        if isinstance(value, (bytearray, memoryview)):
            return bytes(value)
        if isinstance(value, str):
            return value.encode("utf-8")
        raise TypeError(f"{path}: expected bytes or string")
    if annotation is bytearray and isinstance(
        value, (bytes, bytearray, memoryview, str)
    ):
        return bytearray(value, "utf-8") if isinstance(value, str) else bytearray(value)
    if annotation is memoryview and isinstance(value, (bytes, bytearray, memoryview)):
        return memoryview(value)
    if annotation is dt.datetime:
        converted_datetime = (
            value
            if isinstance(value, dt.datetime)
            else dt.datetime.fromisoformat(str(value))
        )
        return (
            converted_datetime
            if converted_datetime.tzinfo
            else converted_datetime.replace(tzinfo=dt.UTC)
        )
    if annotation is dt.date:
        return value if type(value) is dt.date else dt.date.fromisoformat(str(value))
    if annotation is dt.time:
        return (
            value
            if isinstance(value, dt.time)
            else dt.time.fromisoformat(str(value).replace("Z", "+00:00"))
        )
    if annotation is dt.timedelta:
        if isinstance(value, dt.timedelta):
            return value
        return dt.timedelta(seconds=float(value))
    if annotation is Decimal:
        return value if isinstance(value, Decimal) else Decimal(str(value))
    if annotation is uuid.UUID:
        return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))
    if isinstance(annotation, type) and issubclass(annotation, pathlib.PurePath):
        return value if isinstance(value, annotation) else annotation(value)

    if _matches(value, annotation):
        return value
    if isinstance(annotation, type):
        try:
            return annotation(value)
        except (TypeError, ValueError, OverflowError) as exc:
            raise TypeError(
                f"{path}: cannot cast to {annotation.__qualname__}"
            ) from exc
    return value


def _strict_mapping_items(
    value: cabc.Sequence[Any], path: str
) -> list[tuple[Any, Any]]:
    """Normalize the pair-list representation returned by older PyArrow."""

    result: list[tuple[Any, Any]] = []
    seen: set[Any] = set()
    for index, pair in enumerate(value):
        if not isinstance(pair, (list, tuple)) or len(pair) != 2:
            raise TypeError(f"{path}[{index}]: expected a key-value pair")
        key, item = pair
        try:
            if key in seen:
                raise ValueError(f"{path}: duplicate key {key!r} in mapping")
            seen.add(key)
        except TypeError as exc:
            raise TypeError(f"{path}: unhashable map key {key!r}") from exc
        result.append((key, item))
    return result


def _normalize_annotation(annotation: Any) -> Any:
    try:
        hash(annotation)
    except TypeError:
        # ``Annotated`` metadata may legitimately contain mutable mappings.
        # Normalize it without requiring hashability or freezing user data.
        return _compute_normalized_annotation(annotation)
    return _cached_normalized_annotation(annotation)


@cache
def _cached_normalized_annotation(annotation: Any) -> Any:
    return _compute_normalized_annotation(annotation)


def _compute_normalized_annotation(annotation: Any) -> Any:
    seen_aliases: set[int] = set()
    while True:
        if isinstance(annotation, dataclasses.InitVar):
            annotation = annotation.type
            continue
        origin = typing.get_origin(annotation)
        args = typing.get_args(annotation)
        alias = origin if _is_type_alias(origin) else annotation
        if _is_type_alias(alias):
            identity = id(alias)
            if identity in seen_aliases:
                return annotation
            seen_aliases.add(identity)
            parameters = getattr(alias, "__type_params__", ())
            replacements = {
                parameter: argument for parameter, argument in zip(parameters, args)
            }
            annotation = _substitute_typevars(
                alias.__value__, replacements, seen_aliases=seen_aliases
            )
            continue
        if origin is typing.Annotated:
            annotation = args[0]
            continue
        if origin in _WRAPPER_ORIGINS and args:
            annotation = args[0]
            continue
        if hasattr(annotation, "__supertype__"):
            annotation = annotation.__supertype__
            continue
        return annotation


def _substitute_typevars(
    annotation: Any,
    replacements: cabc.Mapping[typing.TypeVar, Any],
    *,
    seen_aliases: set[int] | None = None,
) -> Any:
    """Recursively bind TypeVars without importing any runtime validator."""

    if isinstance(annotation, typing.TypeVar):
        replacement = replacements.get(annotation, annotation)
        if replacement is annotation:
            return annotation
        return _substitute_typevars(
            replacement, replacements, seen_aliases=seen_aliases
        )
    if isinstance(annotation, dataclasses.InitVar):
        substituted_init = _substitute_typevars(
            annotation.type, replacements, seen_aliases=seen_aliases
        )
        return dataclasses.InitVar(substituted_init)

    origin = typing.get_origin(annotation)
    arguments = typing.get_args(annotation)
    alias = origin if _is_type_alias(origin) else annotation
    if _is_type_alias(alias):
        aliases = set() if seen_aliases is None else set(seen_aliases)
        identity = id(alias)
        if identity in aliases:
            return annotation
        aliases.add(identity)
        actual = tuple(
            _substitute_typevars(item, replacements, seen_aliases=aliases)
            for item in arguments
        )
        local = dict(replacements)
        local.update(zip(getattr(alias, "__type_params__", ()), actual))
        return _substitute_typevars(alias.__value__, local, seen_aliases=aliases)
    if not arguments:
        return annotation
    if origin is typing.Literal:
        return annotation
    if origin is typing.Annotated:
        underlying = _substitute_typevars(
            arguments[0], replacements, seen_aliases=seen_aliases
        )
        return typing.Annotated[underlying, *arguments[1:]]
    if origin in (typing.Callable, cabc.Callable) and len(arguments) == 2:
        parameters, result = arguments
        if parameters is not Ellipsis:
            parameters = [
                _substitute_typevars(item, replacements, seen_aliases=seen_aliases)
                for item in parameters
            ]
        result = _substitute_typevars(result, replacements, seen_aliases=seen_aliases)
        return typing.Callable[parameters, result]

    substituted = tuple(
        _substitute_typevars(item, replacements, seen_aliases=seen_aliases)
        for item in arguments
    )
    if substituted == arguments:
        return annotation
    if origin in _UNION_ORIGINS:
        return typing.Union[substituted]  # noqa: UP007
    copier = getattr(annotation, "copy_with", None)
    if callable(copier):
        try:
            return copier(substituted)
        except (AssertionError, TypeError, ValueError):
            pass
    try:
        parameters = substituted[0] if len(substituted) == 1 else substituted
        return origin[parameters]
    except (AttributeError, TypeError, ValueError):
        return annotation


def _dataclass_annotation_spec(
    annotation: Any,
) -> tuple[type[Any], dict[typing.TypeVar, Any]] | None:
    return _generic_type_spec(annotation, dataclasses.is_dataclass)


def _typed_dict_annotation_spec(
    annotation: Any,
) -> tuple[type[Any], dict[typing.TypeVar, Any]] | None:
    return _generic_type_spec(annotation, _is_typed_dict)


def _named_tuple_annotation_spec(
    annotation: Any,
) -> tuple[type[Any], dict[typing.TypeVar, Any]] | None:
    return _generic_type_spec(annotation, _is_named_tuple_type)


def _generic_type_spec(
    annotation: Any, predicate: cabc.Callable[[Any], bool]
) -> tuple[type[Any], dict[typing.TypeVar, Any]] | None:
    origin = typing.get_origin(annotation)
    candidate = origin if isinstance(origin, type) else annotation
    if not isinstance(candidate, type) or not predicate(candidate):
        return None
    parameters = getattr(candidate, "__parameters__", ())
    if not parameters:
        parameters = getattr(candidate, "__type_params__", ())
    arguments = typing.get_args(annotation) if origin is candidate else ()
    return candidate, dict(zip(parameters, arguments))


def _annotation_plan(annotation: Any) -> _AnnotationPlan:
    try:
        hash(annotation)
    except TypeError:
        normalized = _compute_normalized_annotation(annotation)
        try:
            hash(normalized)
        except TypeError:
            return _build_annotation_plan(normalized)
        return _cached_annotation_plan(normalized)
    return _cached_raw_annotation_plan(annotation)


@cache
def _cached_raw_annotation_plan(annotation: Any) -> _AnnotationPlan:
    """Compile a hashable annotation, including wrappers, exactly once."""

    return _build_annotation_plan(_compute_normalized_annotation(annotation))


@cache
def _cached_annotation_plan(annotation: Any) -> _AnnotationPlan:
    return _build_annotation_plan(annotation)


def _build_annotation_plan(annotation: Any) -> _AnnotationPlan:
    origin = typing.get_origin(annotation)
    arguments = typing.get_args(annotation)
    return _AnnotationPlan(
        annotation=annotation,
        origin=origin,
        arguments=arguments,
        dataclass_spec=_dataclass_annotation_spec(annotation),
        typed_dict_spec=_typed_dict_annotation_spec(annotation),
        named_tuple_spec=_named_tuple_annotation_spec(annotation),
        is_mapping=_is_mapping_annotation(annotation, origin),
        is_collection=_is_collection_annotation(annotation, origin),
    )


def _is_type_alias(value: Any) -> bool:
    # This covers both ``typing.TypeAliasType`` and the backport without
    # importing typing_extensions in the dependency-free core.
    return type(value).__name__ == "TypeAliasType" and hasattr(value, "__value__")


def _annotated_alias(annotation: Any) -> str | None:
    origin = typing.get_origin(annotation)
    args = typing.get_args(annotation)
    alias: Any = None
    if origin is typing.Annotated:
        alias = _annotated_alias(args[0])
        for extra in args[1:]:
            options: FieldOptions | None = None
            if isinstance(extra, FieldOptions):
                options = extra
            elif isinstance(extra, dataclasses.Field):
                options = field_options(extra)
            elif isinstance(extra, cabc.Mapping):
                options = _options_from_mapping(extra)
            if options is not None and options.has("alias"):
                alias = options.alias
        return alias
    if origin in _UNION_ORIGINS:
        aliases = {
            found
            for item in args
            if item is not _NONE_TYPE
            for found in (_annotated_alias(item),)
            if found is not None
        }
        if len(aliases) > 1:
            raise TypeError("union branches define conflicting field aliases")
        return next(iter(aliases), None)
    return None


def _matches(value: Any, annotation: Any) -> bool:
    plan = _annotation_plan(annotation)
    annotation = plan.annotation
    origin = plan.origin
    if annotation in (Any, object, typing.Any):
        return True
    if origin in _UNION_ORIGINS:
        return any(_matches(value, item) for item in plan.arguments)
    if origin is typing.Literal:
        return any(
            value == choice and type(value) is type(choice) for choice in plan.arguments
        )
    candidate = origin or annotation
    try:
        if candidate in (int, float) and type(value) is bool:
            return False
        return isinstance(value, candidate)
    except TypeError:
        return False


def _fallback_value(
    dc_field: dataclasses.Field[Any], annotation: Any, path: str
) -> Any:
    if dc_field.default is not dataclasses.MISSING:
        return copy.deepcopy(dc_field.default)
    if dc_field.default_factory is not dataclasses.MISSING:
        return dc_field.default_factory()
    return _fallback_for_annotation(annotation, path)


def _fallback_for_annotation(annotation: Any, path: str) -> Any:
    plan = _annotation_plan(annotation)
    normalized = plan.annotation
    origin = plan.origin
    args = plan.arguments
    if origin in _UNION_ORIGINS and _NONE_TYPE in args:
        return None
    if normalized in (None, _NONE_TYPE, Any, object, typing.Any):
        return None

    if isinstance(normalized, typing.TypeVar):
        if normalized.__bound__ is not None:
            return _fallback_for_annotation(normalized.__bound__, path)
        errors: list[Exception] = []
        for constraint in normalized.__constraints__:
            try:
                return _fallback_for_annotation(constraint, path)
            except (TypeError, ValueError) as exc:
                errors.append(exc)
        if not normalized.__constraints__:
            return None
        raise TypeError(f"{path}: no default value is available") from errors[-1]

    if origin in _UNION_ORIGINS:
        errors = []
        for branch in args:
            try:
                return _fallback_for_annotation(branch, path)
            except (TypeError, ValueError) as exc:
                errors.append(exc)
        raise TypeError(f"{path}: no default value is available") from errors[-1]

    dataclass_spec = _dataclass_annotation_spec(normalized)
    if dataclass_spec is not None:
        return _guarded_fallback(
            normalized,
            path,
            lambda: dataclass_from_dict(normalized, {}, safe=True, on_error="default"),
        )

    typed_dict_spec = _typed_dict_annotation_spec(normalized)
    if typed_dict_spec is not None:
        typed_dict_type, typevars = typed_dict_spec

        def build_typed_dict() -> dict[str, Any]:
            hints = {
                name: _substitute_typevars(field_type, typevars)
                for name, field_type in typing.get_type_hints(
                    typed_dict_type, include_extras=True
                ).items()
            }
            required = _typed_dict_required_keys(typed_dict_type, hints)
            return {
                name: _fallback_for_annotation(hints[name], f"{path}.{name}")
                for name in hints
                if name in required
            }

        return _guarded_fallback(normalized, path, build_typed_dict)

    named_tuple_spec = _named_tuple_annotation_spec(normalized)
    if named_tuple_spec is not None:
        named_tuple_type, typevars = named_tuple_spec

        def build_named_tuple() -> Any:
            hints = {
                name: _substitute_typevars(field_type, typevars)
                for name, field_type in typing.get_type_hints(
                    named_tuple_type, include_extras=True
                ).items()
            }
            defaults = getattr(named_tuple_type, "_field_defaults", {})
            values = [
                copy.deepcopy(defaults[name])
                if name in defaults
                else _fallback_for_annotation(hints.get(name, Any), f"{path}.{name}")
                for name in named_tuple_type._fields
            ]
            return named_tuple_type(*values)

        return _guarded_fallback(normalized, path, build_named_tuple)

    if _is_mapping_annotation(normalized, origin):
        return _construct_mapping(origin or normalized, {})
    if origin is tuple or normalized is tuple:
        if args and not (len(args) == 2 and args[1] is Ellipsis):
            return tuple(
                _fallback_for_annotation(item, f"{path}[{index}]")
                for index, item in enumerate(args)
            )
        return ()
    if _is_collection_annotation(normalized, origin):
        return _construct_collection(origin or normalized, [])
    if origin is typing.Literal:
        if args:
            return copy.deepcopy(args[0])
        raise TypeError(f"{path}: no default value is available")
    if isinstance(normalized, type) and issubclass(normalized, enum.Enum):
        try:
            return next(iter(normalized))
        except StopIteration:
            raise TypeError(f"{path}: no default value is available") from None
    if isinstance(normalized, type):
        try:
            return normalized()
        except (TypeError, ValueError):
            pass
    raise TypeError(f"{path}: no default value is available")


def _guarded_fallback(
    annotation: Any,
    path: str,
    factory: cabc.Callable[[], Any],
) -> Any:
    stack = _FALLBACK_STACK.get()
    if annotation in stack:
        raise TypeError(f"{path}: recursive default is not available")
    token = _FALLBACK_STACK.set((*stack, annotation))
    try:
        return factory()
    finally:
        _FALLBACK_STACK.reset(token)


def _coerce_bool(value: Any, path: str) -> bool:
    if type(value) is bool:
        return value
    if value in (0, 1) and type(value) is int:
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1"}:
            return True
        if lowered in {"false", "0"}:
            return False
    raise ValueError(f"{path}: expected true, false, 1, or 0")


def _has_declared_default(dc_field: dataclasses.Field[Any]) -> bool:
    return (
        dc_field.default is not dataclasses.MISSING
        or dc_field.default_factory is not dataclasses.MISSING
    )


def _is_typed_dict(value: Any) -> bool:
    predicate = getattr(typing, "is_typeddict", None)
    return bool(predicate and predicate(value))


def _typed_dict_required_keys(
    typed_dict_type: type[Any], hints: cabc.Mapping[str, Any]
) -> set[str]:
    """Return required keys, correcting postponed Required/NotRequired hints."""

    required = set(getattr(typed_dict_type, "__required_keys__", ()))
    optional = set(getattr(typed_dict_type, "__optional_keys__", ()))
    total = bool(getattr(typed_dict_type, "__total__", True))
    for name, annotation in hints.items():
        origin = typing.get_origin(annotation)
        if origin is getattr(typing, "Required", None):
            required.add(name)
            optional.discard(name)
        elif origin is getattr(typing, "NotRequired", None):
            required.discard(name)
            optional.add(name)
        elif name not in required and name not in optional and total:
            required.add(name)
    return required


def _mapping_origins() -> tuple[Any, ...]:
    return (
        dict,
        collections.defaultdict,
        collections.OrderedDict,
        collections.Counter,
        cabc.Mapping,
        cabc.MutableMapping,
    )


def _collection_origins() -> tuple[Any, ...]:
    return (
        list,
        set,
        frozenset,
        collections.deque,
        range,
        cabc.Sequence,
        cabc.MutableSequence,
        cabc.Set,
        cabc.MutableSet,
        cabc.Collection,
        cabc.Iterable,
    )


def _safe_issubclass(value: Any, parent: type[Any]) -> bool:
    try:
        return isinstance(value, type) and issubclass(value, parent)
    except TypeError:
        return False


def _is_mapping_annotation(annotation: Any, origin: Any = None) -> bool:
    candidate = origin or annotation
    return candidate in _mapping_origins() or _safe_issubclass(candidate, cabc.Mapping)


def _is_collection_annotation(annotation: Any, origin: Any = None) -> bool:
    candidate = origin or annotation
    if _is_mapping_annotation(annotation, origin) or candidate in (
        str,
        bytes,
        bytearray,
        memoryview,
    ):
        return False
    return candidate in _collection_origins() or _safe_issubclass(
        candidate, cabc.Collection
    )


def _construct_mapping(kind: Any, values: dict[Any, Any]) -> Any:
    if kind is collections.defaultdict:
        return collections.defaultdict(None, values)
    if kind in (
        cabc.Mapping,
        cabc.MutableMapping,
        typing.Mapping,
        typing.MutableMapping,
    ):
        return values
    try:
        return kind(values)
    except (TypeError, ValueError):
        return values


def _construct_collection(kind: Any, values: list[Any]) -> Any:
    if kind is range:
        if not values:
            return range(0)
        if all(type(value) is int for value in values):
            if len(values) == 1:
                return range(values[0], values[0] + 1)
            step = values[1] - values[0]
            if all(
                values[index] - values[index - 1] == step
                for index in range(2, len(values))
            ):
                return range(values[0], values[-1] + step, step)
        raise ValueError("range values must form an integer progression")
    if kind in (
        cabc.Sequence,
        cabc.MutableSequence,
        cabc.Collection,
        cabc.Iterable,
        typing.Sequence,
        typing.MutableSequence,
        typing.Collection,
        typing.Iterable,
    ):
        return values
    if kind in (cabc.Set, cabc.MutableSet, set, typing.MutableSet):
        return set(values)
    try:
        return kind(values)
    except (TypeError, ValueError):
        return values


def _is_named_tuple_type(value: Any) -> bool:
    return (
        isinstance(value, type)
        and issubclass(value, tuple)
        and isinstance(getattr(value, "_fields", None), tuple)
        and hasattr(value, "__annotations__")
    )
