"""The dataclass-powered record decorator."""

from __future__ import annotations

import dataclasses
import inspect
from collections.abc import Callable, Mapping
from types import EllipsisType
from typing import Any, TypeVar, dataclass_transform, overload

from ._metadata import validate_metadata_name
from .base import Record
from .fields import Field, _validate_alias, field
from .metadata import RecordMetadata
from .methods import install_record_methods

__all__ = ["record"]

R = TypeVar("R", bound=Record)


@overload
def record(
    cls: type[R],
    /,
    *,
    init: bool = True,
    repr: bool = True,
    eq: bool = True,
    order: bool = False,
    unsafe_hash: bool = False,
    frozen: bool = False,
    match_args: bool = True,
    kw_only: bool = False,
    slots: bool = False,
    weakref_slot: bool = False,
    alias: str | None = None,
    metadata: Mapping[Any, Any] | None | EllipsisType = ...,
    catalog_name: str | None | EllipsisType = ...,
    schema_name: str | None | EllipsisType = ...,
    table_name: str | None | EllipsisType = ...,
    with_yaml: bool = True,
    with_json: bool = True,
) -> type[R]: ...


@overload
def record(
    cls: None = None,
    /,
    *,
    init: bool = True,
    repr: bool = True,
    eq: bool = True,
    order: bool = False,
    unsafe_hash: bool = False,
    frozen: bool = False,
    match_args: bool = True,
    kw_only: bool = False,
    slots: bool = False,
    weakref_slot: bool = False,
    alias: str | None = None,
    metadata: Mapping[Any, Any] | None | EllipsisType = ...,
    catalog_name: str | None | EllipsisType = ...,
    schema_name: str | None | EllipsisType = ...,
    table_name: str | None | EllipsisType = ...,
    with_yaml: bool = True,
    with_json: bool = True,
) -> Callable[[type[R]], type[R]]: ...


@dataclass_transform(
    field_specifiers=(Field, field, dataclasses.Field, dataclasses.field)
)
def record(
    cls: type[R] | None = None,
    /,
    *,
    init: bool = True,
    repr: bool = True,
    eq: bool = True,
    order: bool = False,
    unsafe_hash: bool = False,
    frozen: bool = False,
    match_args: bool = True,
    kw_only: bool = False,
    slots: bool = False,
    weakref_slot: bool = False,
    alias: str | None = None,
    metadata: Mapping[Any, Any] | None | EllipsisType = ...,
    catalog_name: str | None | EllipsisType = ...,
    schema_name: str | None | EllipsisType = ...,
    table_name: str | None | EllipsisType = ...,
    with_yaml: bool = True,
    with_json: bool = True,
) -> type[R] | Callable[[type[R]], type[R]]:
    """Decorate a ``Record`` subclass as a dataclass.

    The decorator mirrors the Python 3.11 dataclass options and adds class
    aliases, dataset metadata, and configurable codec method generation.
    Protocol adapters remain lazily imported until their methods are called.
    """

    _validate_decorator_options(
        alias,
        metadata,
        catalog_name,
        schema_name,
        table_name,
        with_yaml,
        with_json,
    )
    initial_localns = _scope_locals(2)

    def decorate(candidate: type[R]) -> type[R]:
        if not isinstance(candidate, type):
            raise TypeError("@record can only decorate a class")
        if not issubclass(candidate, Record):
            raise TypeError("@record expects Record subclasses")

        localns = dict(initial_localns)
        localns.update(_scope_locals(2))
        localns[candidate.__name__] = candidate
        if alias is not None:
            candidate.alias = alias
        elif "alias" in candidate.__dict__:
            _validate_alias(candidate.__dict__["alias"])
        else:
            candidate.alias = None

        inherited_metadata = _inherited_record_metadata(candidate)
        class_metadata = inherited_metadata.merged(
            metadata,
            catalog_name=catalog_name,
            schema_name=schema_name,
            table_name=table_name,
        )

        result = dataclasses.dataclass(
            candidate,
            init=init,
            repr=repr,
            eq=eq,
            order=order,
            unsafe_hash=unsafe_hash,
            frozen=frozen,
            match_args=match_args,
            kw_only=kw_only,
            slots=slots,
            weakref_slot=weakref_slot,
        )
        localns[result.__name__] = result
        result.__rkp_localns__ = localns  # type: ignore[attr-defined]
        result.__rkp_record__ = True  # type: ignore[attr-defined]
        result.__rkp_metadata__ = class_metadata  # type: ignore[attr-defined]
        result.__record_with_yaml__ = with_yaml  # type: ignore[attr-defined]
        result.__record_with_json__ = with_json  # type: ignore[attr-defined]
        _validate_serialized_names(result)
        install_record_methods(result)

        # Make newly created function-local sibling records visible to records
        # that captured the same scope earlier.
        for value in localns.values():
            if isinstance(value, type) and getattr(value, "__rkp_record__", False):
                namespace = getattr(value, "__rkp_localns__", None)
                if isinstance(namespace, dict):
                    namespace[result.__name__] = result
        return result

    if cls is not None:
        return decorate(cls)
    return decorate


def _scope_locals(depth: int) -> Mapping[str, Any]:
    frame = inspect.currentframe()
    try:
        for _ in range(depth):
            if frame is None:
                return {}
            frame = frame.f_back
        return dict(frame.f_locals) if frame is not None else {}
    finally:
        del frame


def _validate_decorator_options(
    alias: str | None,
    metadata: Mapping[Any, Any] | None | EllipsisType,
    catalog_name: str | None | EllipsisType,
    schema_name: str | None | EllipsisType,
    table_name: str | None | EllipsisType,
    with_yaml: bool,
    with_json: bool,
) -> None:
    _validate_alias(alias)
    if metadata is not ... and metadata is not None:
        RecordMetadata(metadata)
    for name, value in (
        ("catalog_name", catalog_name),
        ("schema_name", schema_name),
        ("table_name", table_name),
    ):
        if value is not ... and value is not None:
            validate_metadata_name(value, name=name)
    if type(with_yaml) is not bool or type(with_json) is not bool:
        raise TypeError("with_yaml and with_json must be bool")


def _inherited_record_metadata(candidate: type[Any]) -> RecordMetadata:
    for base in candidate.__mro__[1:]:
        metadata = base.__dict__.get("__rkp_metadata__")
        if isinstance(metadata, RecordMetadata):
            return metadata
    return RecordMetadata()


def _validate_serialized_names(cls: type[Any]) -> None:
    from .interop import resolved_type_hints, serialized_field_name

    hints = resolved_type_hints(cls)
    serialized_names: dict[str, str] = {}
    input_names: dict[str, str] = {}
    for dc_field in dataclasses.fields(cls):
        name = serialized_field_name(dc_field, hints.get(dc_field.name, dc_field.type))
        previous = serialized_names.get(name)
        if previous is not None:
            raise TypeError(
                f"duplicate serialized field name {name!r} for "
                f"{cls.__qualname__}.{previous} and {cls.__qualname__}.{dc_field.name}"
            )
        serialized_names[name] = dc_field.name

        accepted_names = (
            (dc_field.name,) if name == dc_field.name else (dc_field.name, name)
        )
        for input_name in accepted_names:
            previous = input_names.get(input_name)
            if previous is not None and previous != dc_field.name:
                raise TypeError(
                    f"ambiguous input field name {input_name!r} for "
                    f"{cls.__qualname__}.{previous} and "
                    f"{cls.__qualname__}.{dc_field.name}"
                )
            input_names[input_name] = dc_field.name
