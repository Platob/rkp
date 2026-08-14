"""Apache Arrow interoperability for records and ordinary dataclasses.

This is intentionally the only module in :mod:`rkp.records` that imports the
required PyArrow dependency eagerly.  The records core loads the adapter only
when Arrow functionality is requested, keeping ordinary imports lightweight.
"""

from __future__ import annotations

import collections
import collections.abc as cabc
import dataclasses
import datetime as dt
import enum
import numbers
import pathlib
import re
import types
import typing
import uuid
from contextvars import ContextVar
from decimal import Decimal
from functools import cache
from typing import Any, Literal, TypeVar

import pyarrow as pa

from ._metadata import (
    CATALOG_NAME,
    INDEX_KEY,
    PARQUET_FIELD_ID,
    PARTITION_KEY,
    PRIMARY_KEY,
    SCHEMA_NAME,
    TABLE_NAME,
    metadata_name,
    normalize_metadata,
)
from .fields import (
    FieldOptions,
    _options_from_mapping,
    _validate_alias,
    field_options,
)
from .interop import (
    dataclass_from_dict,
    is_record_type,
    resolved_type_hints,
    serialized_field_name,
)
from .metadata import record_metadata

__all__ = [
    "arrow_batch_into_records",
    "arrow_into_records",
    "catalog_name",
    "dataclass_into_arrow_field",
    "dataclass_into_arrow_schema",
    "into_arrow_field",
    "into_arrow_schema",
    "into_arrow_type",
    "record_into_arrow_field",
    "record_into_arrow_schema",
    "records_into_arrow_batch",
    "records_into_arrow_batches",
    "records_into_arrow_reader",
    "schema_metadata",
    "schema_name",
    "table_name",
]

T = TypeVar("T")

_MISSING = ...
_NONE_TYPE = type(None)
_UNION_ORIGINS = (typing.Union, types.UnionType)
_DATACLASS_STACK: ContextVar[tuple[type[Any], ...]] = ContextVar(
    "rkp_arrow_dataclass_stack", default=()
)
_TYPEVAR_BINDINGS: ContextVar[cabc.Mapping[Any, Any] | None] = ContextVar(
    "rkp_arrow_typevar_bindings", default=None
)
_ANNOTATION_NAME = re.compile(r"\b[A-Za-z_]\w*\b")

_WRAPPER_ORIGINS = tuple(
    wrapper
    for wrapper in (
        getattr(typing, "Final", None),
        getattr(typing, "Required", None),
        getattr(typing, "NotRequired", None),
        getattr(typing, "ReadOnly", None),
    )
    if wrapper is not None
)

_SCALAR_TYPE_NAMES: dict[str, Any] = {
    "Any": Any,
    "typing.Any": Any,
    "None": _NONE_TYPE,
    "NoneType": _NONE_TYPE,
    "bool": bool,
    "builtins.bool": bool,
    "int": int,
    "builtins.int": int,
    "float": float,
    "builtins.float": float,
    "str": str,
    "builtins.str": str,
    "bytes": bytes,
    "builtins.bytes": bytes,
    "bytearray": bytearray,
    "builtins.bytearray": bytearray,
    "object": object,
    "builtins.object": object,
}

_MAPPING_ORIGINS = {
    dict,
    collections.defaultdict,
    collections.OrderedDict,
    collections.Counter,
    cabc.Mapping,
    cabc.MutableMapping,
}
_COLLECTION_ORIGINS = {
    list,
    set,
    frozenset,
    collections.deque,
    cabc.Sequence,
    cabc.MutableSequence,
    cabc.Set,
    cabc.MutableSet,
    cabc.Collection,
    cabc.Iterable,
    cabc.Iterator,
    cabc.Generator,
    cabc.AsyncIterable,
    cabc.AsyncIterator,
    cabc.AsyncGenerator,
    range,
}


@dataclasses.dataclass(slots=True)
class _Config:
    """Accumulated Arrow configuration for one field."""

    alias: Any = _MISSING
    alias_explicit: bool = False
    arrow_type: Any = _MISSING
    nullable: Any = _MISSING
    nullable_explicit: bool = False
    metadata: dict[Any, Any] = dataclasses.field(default_factory=dict)
    parameters: dict[str, Any] = dataclasses.field(default_factory=dict)
    doc: Any = _MISSING
    doc_explicit: bool = False
    seq: Any = _MISSING
    seq_explicit: bool = False
    primary_key: Any = _MISSING
    primary_key_explicit: bool = False
    partition_key: Any = _MISSING
    partition_key_explicit: bool = False
    index_key: Any = _MISSING
    index_key_explicit: bool = False


@dataclasses.dataclass(slots=True)
class _AnnotationSpec:
    annotation: Any
    nullable: bool = False
    config: _Config = dataclasses.field(default_factory=_Config)


def into_arrow_type(annotation: Any) -> pa.DataType:
    """Infer the closest Arrow data type for a Python annotation.

    ``Optional`` controls the containing field rather than changing its data
    type.  ``Any`` and ``object`` use Arrow's null type because no non-lossy
    physical representation can be inferred without a value sample.
    """

    spec = _annotation_spec(annotation)
    if spec.config.parameters and spec.config.arrow_type is _MISSING:
        raise TypeError("Arrow parameters require an explicit arrow_type")
    if spec.config.arrow_type is not _MISSING:
        arrow_type, _ = _resolve_arrow_override(
            spec.config.arrow_type, spec.config.parameters
        )
        return arrow_type
    return _infer_arrow_type(spec.annotation)


def into_arrow_field(
    name: str | dataclasses.Field[Any],
    annotation: Any = _MISSING,
    *,
    nullable: bool | None = None,
    owner: type[Any] | None = None,
) -> pa.Field:
    """Infer an Arrow field from a name/annotation or dataclass field.

    A custom :class:`rkp.records.Field` is also a real dataclass field, so this
    function handles both through the same path.  ``owner`` is useful for
    resolving a postponed annotation stored on a standalone field object.
    """

    dataclass_field: dataclasses.Field[Any] | None = None
    if isinstance(name, dataclasses.Field):
        if annotation is not _MISSING:
            raise TypeError("annotation must be omitted when passing a dataclass Field")
        dataclass_field = name
        field_name = name.name
        if owner is None:
            annotation = name.type
        else:
            if not isinstance(owner, type) or not dataclasses.is_dataclass(owner):
                raise TypeError("owner must be a dataclass type")
            annotation = resolved_type_hints(owner).get(name.name, name.type)
    else:
        if not isinstance(name, str):
            raise TypeError("field name must be a string or dataclasses.Field")
        if annotation is _MISSING:
            raise TypeError("annotation is required when field name is a string")
        if owner is not None:
            raise TypeError("owner is only valid when passing a dataclass Field")
        field_name = name

    try:
        return _make_arrow_field(
            field_name,
            annotation,
            dataclass_field=dataclass_field,
            nullable=nullable,
        )
    except (TypeError, ValueError) as exc:
        if str(exc).startswith(f"cannot infer Arrow field {field_name!r}"):
            raise
        raise TypeError(f"cannot infer Arrow field {field_name!r}: {exc}") from exc


def into_arrow_schema(
    value: Any,
    *,
    metadata: cabc.Mapping[str | bytes, Any] | None = None,
    localns: cabc.Mapping[str, Any] | None = None,
) -> pa.Schema:
    """Return an Arrow schema from a dataclass, field, schema, or Iceberg schema."""

    if metadata is not None and not isinstance(metadata, cabc.Mapping):
        raise TypeError("metadata must be a mapping or None")
    if localns is not None and not isinstance(localns, cabc.Mapping):
        raise TypeError("localns must be a mapping or None")

    if isinstance(value, pa.Schema):
        return _schema_with_metadata_overlay(value, metadata)
    if isinstance(value, pa.Field):
        fields = list(value.type) if pa.types.is_struct(value.type) else [value]
        schema = pa.schema(fields, metadata={TABLE_NAME: value.name.encode("utf-8")})
        return _schema_with_metadata_overlay(schema, metadata)
    if not isinstance(value, type) and dataclasses.is_dataclass(value):
        return into_arrow_schema(
            type(value),
            metadata=metadata,
            localns=localns,
        )
    if isinstance(value, type) or typing.get_origin(value) is not None:
        if localns is None and isinstance(value, type) and is_record_type(value):
            schema = record_into_arrow_schema(value)
            return _schema_with_metadata_overlay(schema, metadata)
        return dataclass_into_arrow_schema(
            value,
            metadata=metadata,
            localns=localns,
        )

    as_arrow = getattr(value, "as_arrow", None)
    if callable(as_arrow):
        converted = as_arrow()
        if isinstance(converted, pa.Schema):
            return _schema_with_metadata_overlay(converted, metadata)
    raise TypeError(
        "into_arrow_schema expects a dataclass type, Arrow Field/Schema, "
        "or an object exposing as_arrow()"
    )


def dataclass_into_arrow_field(
    dataclass_type: type[Any],
    *,
    name: str | None = None,
    nullable: bool = False,
    localns: cabc.Mapping[str, Any] | None = None,
) -> pa.Field:
    """Recursively infer an Arrow struct field from a dataclass type."""

    generic_origin = typing.get_origin(dataclass_type)
    if isinstance(generic_origin, type) and dataclasses.is_dataclass(generic_origin):
        bindings = dict(_TYPEVAR_BINDINGS.get() or {})
        bindings.update(
            _type_argument_bindings(
                generic_origin,
                typing.get_args(dataclass_type),
                bindings,
            )
        )
        bindings_token = _TYPEVAR_BINDINGS.set(bindings)
        try:
            return dataclass_into_arrow_field(
                generic_origin,
                name=name,
                nullable=nullable,
                localns=localns,
            )
        finally:
            _TYPEVAR_BINDINGS.reset(bindings_token)

    if not isinstance(dataclass_type, type) or not dataclasses.is_dataclass(
        dataclass_type
    ):
        raise TypeError("dataclass_into_arrow_field expects a dataclass type")
    # ``is_dataclass`` is inherited.  Requiring the field table on this class
    # catches subclasses which added annotations without applying a decorator.
    if "__dataclass_fields__" not in dataclass_type.__dict__:
        raise TypeError(
            f"{dataclass_type.__qualname__} must be decorated as a dataclass"
        )
    if name is not None and not isinstance(name, str):
        raise TypeError("name must be a string or None")
    if type(nullable) is not bool:
        raise TypeError("nullable must be bool")
    if localns is not None and not isinstance(localns, cabc.Mapping):
        raise TypeError("localns must be a mapping or None")

    stack = _DATACLASS_STACK.get()
    if dataclass_type in stack:
        cycle = " -> ".join(item.__qualname__ for item in (*stack, dataclass_type))
        raise TypeError(f"recursive dataclass annotations are not supported: {cycle}")

    stack_token = _DATACLASS_STACK.set((*stack, dataclass_type))
    try:
        hints = resolved_type_hints(dataclass_type, localns=localns)
        children: list[pa.Field] = []
        for dc_field in dataclasses.fields(dataclass_type):
            field_annotation = hints.get(dc_field.name, dc_field.type)
            field_annotation = _substitute_typevars(
                field_annotation, _TYPEVAR_BINDINGS.get() or {}
            )
            try:
                children.append(
                    _make_arrow_field(
                        dc_field.name,
                        field_annotation,
                        dataclass_field=dc_field,
                    )
                )
            except (TypeError, ValueError) as exc:
                prefix = f"{dataclass_type.__qualname__}.{dc_field.name}"
                if str(exc).startswith(prefix):
                    raise
                raise TypeError(f"cannot infer {prefix}: {exc}") from exc

        root_name = (
            name if name is not None else _default_dataclass_name(dataclass_type)
        )
        return pa.field(root_name, pa.struct(children), nullable=nullable)
    finally:
        _DATACLASS_STACK.reset(stack_token)


def dataclass_into_arrow_schema(
    dataclass_type: type[Any],
    *,
    metadata: cabc.Mapping[str | bytes, Any] | None = None,
    localns: cabc.Mapping[str, Any] | None = None,
) -> pa.Schema:
    """Infer a top-level Arrow schema from a dataclass type."""

    if metadata is not None and not isinstance(metadata, cabc.Mapping):
        raise TypeError("metadata must be a mapping or None")
    root = dataclass_into_arrow_field(dataclass_type, localns=localns)
    base_metadata = _dataclass_schema_metadata(dataclass_type)
    schema = pa.schema(list(root.type), metadata=base_metadata or None)
    return _schema_with_metadata_overlay(schema, metadata)


@cache
def record_into_arrow_field(
    record_type: type[Any],
    name: str | None = None,
    *,
    nullable: bool = False,
) -> pa.Field:
    """Return a cached Arrow struct field for a decorated record class."""

    if not is_record_type(record_type):
        raise TypeError("record_into_arrow_field expects a decorated record type")
    return dataclass_into_arrow_field(
        record_type,
        name=name,
        nullable=nullable,
    )


@cache
def record_into_arrow_schema(record_type: type[Any]) -> pa.Schema:
    """Return a cached top-level Arrow schema for a decorated record."""

    if not is_record_type(record_type):
        raise TypeError("record_into_arrow_schema expects a decorated record type")
    return dataclass_into_arrow_schema(record_type)


def arrow_batch_into_records(
    record_type: type[T],
    batch: pa.RecordBatch,
    *,
    safe: bool = True,
    on_error: Literal["raise", "default"] = "raise",
    validate_schema: bool = True,
) -> cabc.Iterator[T]:
    """Lazily construct records from the rows of one Arrow record batch.

    Arrow field names use the same aliases as codecs and ``from_dict``.
    Schema validation compares the complete physical field layout while
    deliberately ignoring schema and field metadata added by transports.
    """

    _validate_record_conversion_options(record_type, safe, on_error, validate_schema)
    if not isinstance(batch, pa.RecordBatch):
        raise TypeError("batch must be a pyarrow.RecordBatch")
    if validate_schema:
        _validate_batch_schema(record_type, batch.schema)
    return _batch_rows_into_records(record_type, batch, safe, on_error)


def arrow_into_records(
    record_type: type[T],
    source: (
        pa.RecordBatch | pa.Table | pa.RecordBatchReader | cabc.Iterable[pa.RecordBatch]
    ),
    *,
    safe: bool = True,
    on_error: Literal["raise", "default"] = "raise",
    validate_schema: bool = True,
) -> cabc.Iterator[T]:
    """Lazily construct records from an Arrow batch, table, reader, or batches."""

    _validate_record_conversion_options(record_type, safe, on_error, validate_schema)

    def converted() -> cabc.Iterator[T]:
        for index, batch in enumerate(_arrow_batches(source)):
            if not isinstance(batch, pa.RecordBatch):
                raise TypeError(
                    "Arrow batch iterable yielded "
                    f"{type(batch).__qualname__} at index {index}; "
                    "expected pyarrow.RecordBatch"
                )
            if validate_schema:
                _validate_batch_schema(record_type, batch.schema, batch_index=index)
            yield from _batch_rows_into_records(record_type, batch, safe, on_error)

    return converted()


def records_into_arrow_batch(
    records: cabc.Iterable[Any],
    *,
    record_type: type[Any] | None = None,
    schema: pa.Schema | None = None,
) -> pa.RecordBatch:
    """Build one Arrow record batch, inferring its record type when possible.

    Empty iterables require either ``record_type`` or ``schema``.  A supplied
    schema is authoritative, enabling mapping rows when the caller already
    owns their Arrow contract.
    """

    _validate_output_hints(record_type, schema)
    iterator = iter(records)
    try:
        first = next(iterator)
    except StopIteration:
        selected_schema = _resolve_output_schema(record_type, schema)
        return _empty_record_batch(selected_schema)
    selected_type = _resolve_output_record_type(first, record_type, schema)
    if record_type is None and selected_type is not None and schema is not None:
        _validate_output_hints(selected_type, schema)
    selected_schema = _resolve_output_schema(selected_type, schema)
    rows = [_record_into_arrow_row(first, selected_type, selected_schema, index=0)]
    rows.extend(
        _record_into_arrow_row(item, selected_type, selected_schema, index=index)
        for index, item in enumerate(iterator, start=1)
    )
    return _rows_into_record_batch(rows, selected_schema)


def records_into_arrow_batches(
    records: cabc.Iterable[Any],
    *,
    batch_size: int = 65_536,
    record_type: type[Any] | None = None,
    schema: pa.Schema | None = None,
) -> cabc.Iterator[pa.RecordBatch]:
    """Lazily convert records into bounded Arrow record batches.

    Unlike :func:`records_into_arrow_batch`, an empty input yields no batches.
    The first input record is consumed only when the returned iterator advances.
    """

    if type(batch_size) is not int or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")
    _validate_output_hints(record_type, schema)

    def converted() -> cabc.Iterator[pa.RecordBatch]:
        iterator = iter(records)
        selected_type = record_type
        selected_schema = schema
        record_index = 0
        while True:
            rows: list[dict[str, Any]] = []
            while len(rows) < batch_size:
                try:
                    item = next(iterator)
                except StopIteration:
                    break
                if selected_type is None and selected_schema is None:
                    selected_type = _resolve_output_record_type(item, None, None)
                elif selected_type is None:
                    selected_type = _resolve_output_record_type(
                        item,
                        None,
                        selected_schema,
                    )
                    if selected_type is not None:
                        _validate_output_hints(selected_type, selected_schema)
                if selected_schema is None:
                    selected_schema = _resolve_output_schema(selected_type, None)
                rows.append(
                    _record_into_arrow_row(
                        item,
                        selected_type,
                        selected_schema,
                        index=record_index,
                    )
                )
                record_index += 1
            if not rows:
                return
            if selected_schema is None:  # pragma: no cover - internal invariant
                raise RuntimeError("Arrow output schema was not resolved")
            yield _rows_into_record_batch(rows, selected_schema)

    return converted()


def records_into_arrow_reader(
    records: cabc.Iterable[Any],
    *,
    batch_size: int = 65_536,
    record_type: type[Any] | None = None,
    schema: pa.Schema | None = None,
) -> pa.RecordBatchReader:
    """Expose a record iterable as a streaming Arrow ``RecordBatchReader``.

    A reader must publish its schema before iteration starts, so unlike the
    batch iterator this helper requires ``record_type`` or ``schema``.
    """

    _validate_output_hints(record_type, schema)
    if record_type is None and schema is None:
        raise TypeError("records_into_arrow_reader requires record_type or schema")
    selected_schema = _resolve_output_schema(record_type, schema)
    batches = records_into_arrow_batches(
        records,
        batch_size=batch_size,
        record_type=record_type,
        schema=selected_schema,
    )
    return pa.RecordBatchReader.from_batches(selected_schema, batches)


def _validate_record_conversion_options(
    record_type: type[Any], safe: bool, on_error: str, validate_schema: bool
) -> None:
    if not isinstance(record_type, type) or not is_record_type(record_type):
        raise TypeError("record_type must be a decorated record type")
    if type(safe) is not bool:
        raise TypeError("safe must be bool")
    if on_error not in {"raise", "default"}:
        raise ValueError("on_error must be 'raise' or 'default'")
    if type(validate_schema) is not bool:
        raise TypeError("validate_schema must be bool")


def _validate_batch_schema(
    record_type: type[Any], schema: pa.Schema, *, batch_index: int | None = None
) -> None:
    expected = record_into_arrow_schema(typing.cast(Any, record_type))
    if schema.equals(expected, check_metadata=False):
        return
    location = "" if batch_index is None else f" at batch index {batch_index}"
    raise TypeError(
        f"Arrow schema mismatch for {record_type.__qualname__}{location}: "
        f"expected {expected}, got {schema}"
    )


def _batch_rows_into_records(
    record_type: type[T],
    batch: pa.RecordBatch,
    safe: bool,
    on_error: str,
) -> cabc.Iterator[T]:
    try:
        rows = batch.to_pylist(maps_as_pydicts="strict")
    except TypeError:
        # Compatibility with PyArrow versions predating ``maps_as_pydicts``.
        rows = batch.to_pylist()
    for row_index, row in enumerate(rows):
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
                f"from Arrow row {row_index}: {exc}"
            ) from exc


def _arrow_batches(source: Any) -> cabc.Iterator[pa.RecordBatch]:
    if isinstance(source, pa.RecordBatch):
        yield source
        return
    if isinstance(source, pa.Table):
        yield from source.to_batches()
        return
    if isinstance(source, pa.RecordBatchReader):
        yield from source
        return
    if isinstance(source, (str, bytes, bytearray, memoryview, cabc.Mapping)):
        raise TypeError(
            "source must be a RecordBatch, Table, RecordBatchReader, "
            "or iterable of RecordBatch objects"
        )
    try:
        yield from iter(source)
    except TypeError as exc:
        raise TypeError(
            "source must be a RecordBatch, Table, RecordBatchReader, "
            "or iterable of RecordBatch objects"
        ) from exc


def _validate_output_hints(
    record_type: type[Any] | None, schema: pa.Schema | None
) -> None:
    if record_type is not None and (
        not isinstance(record_type, type) or not dataclasses.is_dataclass(record_type)
    ):
        raise TypeError("record_type must be a dataclass type or None")
    if schema is not None and not isinstance(schema, pa.Schema):
        raise TypeError("schema must be a pyarrow.Schema or None")
    if record_type is not None and schema is not None:
        expected = into_arrow_schema(record_type)
        expected_names = expected.names
        if schema.names != expected_names:
            raise TypeError(
                f"schema fields do not match {record_type.__qualname__}: "
                f"expected {expected_names!r}, got {schema.names!r}"
            )
    if schema is not None:
        _validate_runtime_output_schema(schema)
    elif record_type is not None:
        _validate_runtime_output_schema(into_arrow_schema(record_type))


def _validate_runtime_output_schema(schema: pa.Schema) -> None:
    """Reject layouts PyArrow cannot construct from Python record rows.

    PyArrow can describe and consume union arrays, but its ``from_pylist``
    record-batch constructor cannot materialize them.  RKP keeps union schema
    inference available for protocol interchange while failing before it
    consumes or normalizes record rows at the runtime output boundary.
    """

    for field in schema:
        unsupported = _runtime_union_path(field.type, field.name)
        if unsupported is None:
            continue
        path, union_type = unsupported
        raise TypeError(
            "records-to-Arrow runtime conversion does not support "
            f"union type at {path!r}: {union_type}; "
            "construct a pyarrow.UnionArray explicitly for union data"
        )


def _runtime_union_path(
    value: pa.DataType,
    path: str,
) -> tuple[str, pa.DataType] | None:
    if pa.types.is_union(value):
        return path, value
    if pa.types.is_struct(value):
        for child in value:
            unsupported = _runtime_union_path(child.type, f"{path}.{child.name}")
            if unsupported is not None:
                return unsupported
        return None
    if pa.types.is_map(value):
        key = _runtime_union_path(value.key_field.type, f"{path}.key")
        if key is not None:
            return key
        return _runtime_union_path(value.item_field.type, f"{path}.value")
    if any(
        predicate(value)
        for predicate in (
            pa.types.is_list,
            pa.types.is_large_list,
            pa.types.is_fixed_size_list,
        )
    ):
        return _runtime_union_path(value.value_field.type, f"{path}[]")
    if pa.types.is_dictionary(value):
        return _runtime_union_path(value.value_type, f"{path}.dictionary")
    if isinstance(value, pa.ExtensionType):
        return _runtime_union_path(value.storage_type, f"{path}.storage")
    is_run_end_encoded = getattr(pa.types, "is_run_end_encoded", None)
    if callable(is_run_end_encoded) and is_run_end_encoded(value):
        return _runtime_union_path(value.value_field.type, f"{path}.value")
    return None


def _resolve_output_record_type(
    first: Any,
    record_type: type[Any] | None,
    schema: pa.Schema | None,
) -> type[Any] | None:
    if record_type is not None:
        if not isinstance(first, record_type):
            raise TypeError(
                f"record at index 0 must be {record_type.__qualname__}, "
                f"got {type(first).__qualname__}"
            )
        return record_type
    if dataclasses.is_dataclass(first) and not isinstance(first, type):
        return type(first)
    if schema is not None and isinstance(first, cabc.Mapping):
        return None
    raise TypeError(
        "cannot infer record_type; pass dataclass records or provide "
        "record_type/schema explicitly"
    )


def _resolve_output_schema(
    record_type: type[Any] | None, schema: pa.Schema | None
) -> pa.Schema:
    selected = schema
    if selected is None and record_type is None:
        raise TypeError("empty records require record_type or schema")
    if selected is None:
        selected = into_arrow_schema(record_type)
    _validate_runtime_output_schema(selected)
    return selected


def _record_into_arrow_row(
    value: Any,
    record_type: type[Any] | None,
    schema: pa.Schema,
    *,
    index: int,
) -> dict[str, Any]:
    if record_type is None:
        if not isinstance(value, cabc.Mapping):
            raise TypeError("all rows must be mappings when only schema is supplied")
        _validate_mapping_row(value, schema, index=index)
        return {key: _arrow_native_value(item) for key, item in value.items()}
    if not isinstance(value, record_type):
        raise TypeError(
            f"all records must be {record_type.__qualname__}; "
            f"got {type(value).__qualname__}"
        )
    return _arrow_native_dataclass(value)


def _validate_mapping_row(
    value: cabc.Mapping[Any, Any],
    schema: pa.Schema,
    *,
    index: int,
) -> None:
    non_string = [key for key in value if not isinstance(key, str)]
    if non_string:
        rendered = ", ".join(repr(key) for key in non_string)
        raise TypeError(
            f"Arrow mapping row at index {index} has non-string field name(s): "
            f"{rendered}"
        )

    actual = set(typing.cast(cabc.Mapping[str, Any], value))
    expected = set(schema.names)
    missing = expected - actual
    unexpected = actual - expected
    if not missing and not unexpected:
        return

    details: list[str] = []
    if missing:
        details.append("missing " + ", ".join(repr(name) for name in sorted(missing)))
    if unexpected:
        details.append(
            "unexpected " + ", ".join(repr(name) for name in sorted(unexpected))
        )
    raise TypeError(
        f"Arrow mapping row at index {index} does not match schema: "
        + "; ".join(details)
    )


def _arrow_native_dataclass(value: Any) -> dict[str, Any]:
    return {
        wire_name: _arrow_native_value(getattr(value, attribute))
        for wire_name, attribute in _arrow_dataclass_projection(
            typing.cast(Any, type(value))
        )
    }


@cache
def _arrow_dataclass_projection(
    dataclass_type: type[Any],
) -> tuple[tuple[str, str], ...]:
    try:
        hints = resolved_type_hints(dataclass_type)
    except TypeError:
        hints = {}
    return tuple(
        (
            serialized_field_name(dc_field, hints.get(dc_field.name, dc_field.type)),
            dc_field.name,
        )
        for dc_field in dataclasses.fields(dataclass_type)
    )


def _arrow_native_value(value: Any) -> Any:
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return _arrow_native_dataclass(value)
    if isinstance(value, enum.Enum):
        return _arrow_native_value(value.value)
    if isinstance(value, (uuid.UUID, pathlib.PurePath)):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, cabc.Mapping):
        return {
            _arrow_native_value(key): _arrow_native_value(item)
            for key, item in value.items()
        }
    if isinstance(value, tuple):
        return tuple(_arrow_native_value(item) for item in value)
    if isinstance(value, (list, set, frozenset, collections.deque, range)):
        return [_arrow_native_value(item) for item in value]
    return value


def _rows_into_record_batch(
    rows: list[dict[str, Any]], schema: pa.Schema
) -> pa.RecordBatch:
    if len(schema) == 0 and rows:
        sentinel = pa.record_batch(
            [pa.nulls(len(rows))],
            names=["__rkp_row__"],
        )
        return sentinel.select([]).replace_schema_metadata(schema.metadata)
    return pa.RecordBatch.from_pylist(rows, schema=schema)


def _empty_record_batch(schema: pa.Schema) -> pa.RecordBatch:
    return pa.RecordBatch.from_arrays(
        [pa.array([], type=field.type) for field in schema],
        schema=schema,
    )


def schema_metadata(value: Any) -> dict[bytes, bytes]:
    """Return normalized Arrow schema metadata for any supported value.

    Decorated records use their cached Arrow schema.  Arbitrary Arrow schemas
    are intentionally not cached because reading their typically tiny metadata
    mapping is faster and avoids retaining caller-owned schemas.
    """

    candidate = value if isinstance(value, type) else type(value)
    if isinstance(candidate, type) and is_record_type(candidate):
        return dict(record_into_arrow_schema(candidate).metadata or {})
    schema = into_arrow_schema(value)
    metadata = dict(schema.metadata or {})
    for key in (CATALOG_NAME, SCHEMA_NAME, TABLE_NAME):
        selected = metadata_name(metadata, key)
        if selected is not None:
            metadata[key] = selected.encode("utf-8")
            metadata.pop(b"rkp." + key, None)
    return metadata


def catalog_name(value: Any) -> str | None:
    """Return the portable catalog name associated with ``value``."""

    return _portable_name(value, CATALOG_NAME)


def schema_name(value: Any) -> str | None:
    """Return the portable namespace/schema name associated with ``value``."""

    return _portable_name(value, SCHEMA_NAME)


def table_name(value: Any) -> str | None:
    """Return the portable table name associated with ``value``."""

    return _portable_name(value, TABLE_NAME)


def _portable_name(value: Any, key: bytes) -> str | None:
    candidate = value if isinstance(value, type) else type(value)
    if isinstance(candidate, type) and is_record_type(candidate):
        metadata = record_metadata(candidate)
        if key == CATALOG_NAME:
            selected = metadata.catalog_name
        elif key == SCHEMA_NAME:
            selected = metadata.schema_name
        else:
            selected = metadata.table_name
        if selected is not None or key != TABLE_NAME:
            return selected
        return _default_dataclass_name(candidate)

    if isinstance(value, pa.Schema):
        snapshot = value.metadata or {}
        return metadata_name(snapshot, key)
    if isinstance(value, pa.Field):
        return value.name if key == TABLE_NAME else None

    origin = typing.get_origin(value)
    dataclass_type = origin if isinstance(origin, type) else value
    if not isinstance(dataclass_type, type) and dataclasses.is_dataclass(value):
        dataclass_type = type(value)
    if isinstance(dataclass_type, type) and dataclasses.is_dataclass(dataclass_type):
        if key == TABLE_NAME:
            return _default_dataclass_name(dataclass_type)
        return None

    schema = into_arrow_schema(value)
    snapshot = schema.metadata or {}
    return metadata_name(snapshot, key)


def _dataclass_schema_metadata(dataclass_type: Any) -> dict[bytes, bytes]:
    origin = typing.get_origin(dataclass_type)
    candidate = origin if isinstance(origin, type) else dataclass_type
    values: dict[Any, Any] = {}
    if isinstance(candidate, type) and is_record_type(candidate):
        values.update(record_metadata(candidate).metadata)
    normalized = normalize_metadata(values)
    if TABLE_NAME not in normalized:
        normalized[TABLE_NAME] = _default_dataclass_name(candidate).encode("utf-8")
    return normalized


def _schema_with_metadata_overlay(
    schema: pa.Schema,
    metadata: cabc.Mapping[str | bytes, Any] | None,
) -> pa.Schema:
    if metadata is None:
        return schema

    merged = dict(schema.metadata or {})
    seen: set[bytes] = set()
    additions: dict[bytes, bytes] = {}
    removals: set[bytes] = set()
    for raw_key, value in metadata.items():
        if isinstance(raw_key, bytes):
            key = raw_key
        elif isinstance(raw_key, str):
            key = raw_key.encode("utf-8")
        else:
            key = str(raw_key).encode("utf-8")
        if key.startswith(b"rkp.") and key[4:] in (
            CATALOG_NAME,
            SCHEMA_NAME,
            TABLE_NAME,
        ):
            key = key[4:]
        if key in seen:
            name = key.decode("utf-8", "replace")
            raise TypeError(f"duplicate metadata key {name!r}")
        seen.add(key)
        if value is None:
            removals.add(key)
        else:
            additions.update(normalize_metadata({key: value}))
    for key in removals:
        merged.pop(key, None)
        if key in (CATALOG_NAME, SCHEMA_NAME, TABLE_NAME):
            merged.pop(b"rkp." + key, None)
    for key in additions:
        if key in (CATALOG_NAME, SCHEMA_NAME, TABLE_NAME):
            merged.pop(b"rkp." + key, None)
    merged.update(additions)
    for key in (CATALOG_NAME, SCHEMA_NAME, TABLE_NAME):
        metadata_name(merged, key)
    return schema.with_metadata(merged or None)


def _make_arrow_field(
    default_name: str,
    annotation: Any,
    *,
    dataclass_field: dataclasses.Field[Any] | None = None,
    nullable: bool | None = None,
    force_name: bool = False,
) -> pa.Field:
    spec = _annotation_spec(annotation)
    config = spec.config

    if dataclass_field is not None:
        _apply_field_options(config, field_options(dataclass_field))

    if nullable is not None:
        if type(nullable) is not bool:
            raise TypeError("nullable must be bool or None")
        config.nullable = nullable

    if config.parameters and config.arrow_type is _MISSING:
        raise TypeError("Arrow parameters require an explicit arrow_type")

    override_field: pa.Field | None = None
    if config.arrow_type is _MISSING:
        arrow_type = _infer_arrow_type(spec.annotation)
    else:
        arrow_type, override_field = _resolve_arrow_override(
            config.arrow_type, config.parameters
        )

    if force_name:
        field_name = default_name
    elif config.alias_explicit:
        field_name = default_name if config.alias is _MISSING else config.alias
    elif config.alias is not _MISSING:
        field_name = config.alias
    elif override_field is not None:
        field_name = override_field.name
    else:
        field_name = default_name
    if not isinstance(field_name, str) or not field_name:
        raise TypeError("Arrow field names must be non-empty strings")

    if config.nullable is not _MISSING:
        field_nullable = config.nullable
    elif config.nullable_explicit:
        field_nullable = spec.nullable
    elif override_field is not None:
        field_nullable = override_field.nullable
    else:
        field_nullable = spec.nullable

    # PyArrow itself rejects ``field(null(), nullable=False)``.  Any/object and
    # other genuinely unknown annotations therefore have a real nullable
    # representation even when they were not spelled Optional.
    if pa.types.is_null(arrow_type):
        field_nullable = True

    if _key_enabled(config.primary_key) and field_nullable:
        raise TypeError("primary key fields cannot be nullable")

    metadata: dict[Any, Any] = {}
    if override_field is not None and override_field.metadata:
        metadata.update(override_field.metadata)
    metadata.update(config.metadata)
    if config.doc_explicit:
        _remove_metadata_key(metadata, b"doc")
        if config.doc is not None:
            metadata[b"doc"] = config.doc
    if config.seq_explicit:
        _remove_metadata_key(metadata, PARQUET_FIELD_ID)
        _remove_metadata_key(metadata, b"iceberg.id")
    if config.seq is not _MISSING:
        metadata[PARQUET_FIELD_ID] = config.seq
    for key, explicit in (
        (PRIMARY_KEY, config.primary_key_explicit),
        (PARTITION_KEY, config.partition_key_explicit),
        (INDEX_KEY, config.index_key_explicit),
    ):
        if explicit:
            _remove_metadata_key(metadata, key)
    _add_key_metadata(metadata, PRIMARY_KEY, config.primary_key)
    _add_key_metadata(metadata, PARTITION_KEY, config.partition_key)
    _add_key_metadata(metadata, INDEX_KEY, config.index_key)

    normalized_metadata = normalize_metadata(metadata)
    return pa.field(
        field_name,
        arrow_type,
        nullable=field_nullable,
        metadata=normalized_metadata or None,
    )


def _annotation_spec(annotation: Any) -> _AnnotationSpec:
    if annotation is None:
        annotation = _NONE_TYPE

    expanded_alias = _expand_type_alias(annotation)
    if expanded_alias is not annotation:
        return _annotation_spec(expanded_alias)

    # NewType exposes the wrapped annotation without being a normal class.
    supertype = getattr(annotation, "__supertype__", _MISSING)
    if supertype is not _MISSING:
        return _annotation_spec(supertype)

    origin = typing.get_origin(annotation)
    arguments = typing.get_args(annotation)

    if origin is typing.Annotated:
        spec = _annotation_spec(arguments[0])
        for extra in arguments[1:]:
            _apply_annotated_extra(spec.config, extra)
        return spec

    if origin in _WRAPPER_ORIGINS and arguments:
        return _annotation_spec(arguments[0])

    if origin in _UNION_ORIGINS:
        concrete = tuple(item for item in arguments if item is not _NONE_TYPE)
        has_none = len(concrete) != len(arguments)
        if not concrete:
            return _AnnotationSpec(_NONE_TYPE, nullable=True)
        if len(concrete) == 1:
            spec = _annotation_spec(concrete[0])
            spec.nullable = spec.nullable or has_none
            return spec
        return _AnnotationSpec(annotation, nullable=has_none)

    if isinstance(annotation, pa.Field):
        config = _Config(arrow_type=annotation)
        return _AnnotationSpec(
            annotation.type,
            nullable=annotation.nullable or pa.types.is_null(annotation.type),
            config=config,
        )

    if annotation in (Any, object) or annotation is typing.Any:
        return _AnnotationSpec(annotation, nullable=True)
    if annotation is _NONE_TYPE:
        return _AnnotationSpec(annotation, nullable=True)
    if origin is typing.Literal and any(item is None for item in arguments):
        return _AnnotationSpec(annotation, nullable=True)
    if isinstance(annotation, typing.TypeVar):
        bound = annotation.__bound__
        if bound is not None:
            return _annotation_spec(bound)
        if not annotation.__constraints__:
            return _AnnotationSpec(Any, nullable=True)

    return _AnnotationSpec(annotation)


def _infer_arrow_type(annotation: Any) -> pa.DataType:
    if annotation is None:
        annotation = _NONE_TYPE
    expanded_alias = _expand_type_alias(annotation)
    if expanded_alias is not annotation:
        return into_arrow_type(expanded_alias)
    if isinstance(annotation, pa.DataType):
        return annotation
    if isinstance(annotation, pa.Field):
        return annotation.type
    if isinstance(annotation, pa.Schema):
        return pa.struct(annotation)
    if isinstance(annotation, str):
        annotation = _SCALAR_TYPE_NAMES.get(annotation, annotation)
        if isinstance(annotation, str):
            # A standalone unresolved forward annotation has no owner namespace.
            # String is the least lossy interoperable representation available.
            return pa.string()
    if isinstance(annotation, typing.ForwardRef):
        return _infer_arrow_type(annotation.__forward_arg__)

    if annotation in (Any, object) or annotation is typing.Any:
        return pa.null()
    if annotation in (_NONE_TYPE, getattr(typing, "Never", ...), typing.NoReturn):
        return pa.null()
    if annotation is bool:
        return pa.bool_()
    if annotation is int:
        return pa.int64()
    if annotation is float:
        return pa.float64()
    if annotation is str or annotation is getattr(typing, "LiteralString", _MISSING):
        return pa.string()
    if annotation in (bytes, bytearray, memoryview):
        return pa.binary()
    if annotation is dt.datetime:
        return pa.timestamp("us", tz="UTC")
    if annotation is dt.date:
        return pa.date32()
    if annotation is dt.time:
        return pa.time64("us")
    if annotation is dt.timedelta:
        return pa.duration("us")
    if annotation is Decimal:
        return pa.decimal128(38, 18)
    if annotation is uuid.UUID:
        # This remains compatible with PyArrow releases predating its UUID
        # extension type and with the package's JSON/YAML normalization.
        return pa.string()
    if annotation in (bytes, bytearray, memoryview):
        return pa.binary()
    if annotation in (typing.SupportsInt, getattr(typing, "SupportsIndex", _MISSING)):
        return pa.int64()
    if annotation is typing.SupportsFloat:
        return pa.float64()
    if annotation is typing.SupportsBytes:
        return pa.binary()

    if isinstance(annotation, typing.TypeVar):
        if annotation.__bound__ is not None:
            return into_arrow_type(annotation.__bound__)
        constraints = annotation.__constraints__
        if constraints:
            return _union_arrow_type(constraints)
        return pa.null()

    origin = typing.get_origin(annotation)
    arguments = typing.get_args(annotation)

    if isinstance(origin, type) and dataclasses.is_dataclass(origin):
        return dataclass_into_arrow_field(annotation).type
    if origin in _UNION_ORIGINS:
        return _union_arrow_type(arguments)
    if origin is typing.Literal:
        return _literal_arrow_type(arguments)
    if origin is tuple:
        return _tuple_arrow_type(arguments)
    if origin is collections.Counter:
        item_annotation = arguments[0] if arguments else Any
        return _mapping_arrow_type((item_annotation, int))
    if origin is cabc.ItemsView:
        return _items_view_arrow_type(arguments)
    if origin in _MAPPING_ORIGINS:
        return _mapping_arrow_type(arguments)
    if origin in _COLLECTION_ORIGINS:
        return _collection_arrow_type(arguments)

    # Raw built-in and abstract collection annotations have no typing origin.
    if annotation is tuple:
        return pa.struct([])
    if annotation is collections.Counter:
        return _mapping_arrow_type((str, int))
    if annotation is cabc.ItemsView:
        return _items_view_arrow_type(())
    if annotation in _MAPPING_ORIGINS:
        return _mapping_arrow_type(())
    if annotation in _COLLECTION_ORIGINS:
        item = int if annotation is range else Any
        return _collection_arrow_type((item,))

    if _is_typed_dict(annotation):
        return _typed_dict_arrow_type(annotation)
    if _is_named_tuple_type(annotation):
        return _named_tuple_arrow_type(annotation)
    if isinstance(annotation, type) and dataclasses.is_dataclass(annotation):
        return dataclass_into_arrow_field(annotation).type
    if isinstance(annotation, type) and issubclass(annotation, enum.Enum):
        return _enum_arrow_type(annotation)
    if isinstance(annotation, type) and issubclass(annotation, pathlib.PurePath):
        return pa.string()
    # Preserve NumPy scalar widths rather than widening every numerical class
    # to Python's int64/float64 defaults.
    if isinstance(annotation, type):
        try:
            return pa.from_numpy_dtype(annotation)
        except (TypeError, ValueError, NotImplementedError):
            pass
    if isinstance(annotation, type) and issubclass(annotation, numbers.Integral):
        return pa.int64()
    if isinstance(annotation, type) and issubclass(annotation, numbers.Real):
        return pa.float64()

    candidate = origin or annotation
    if isinstance(candidate, type):
        try:
            if issubclass(candidate, cabc.Mapping):
                return _mapping_arrow_type(arguments)
            if issubclass(candidate, tuple):
                return _tuple_arrow_type(arguments)
            if issubclass(candidate, (bytes, bytearray, memoryview)):
                return pa.binary()
            if issubclass(candidate, cabc.Collection):
                return _collection_arrow_type(arguments)
        except TypeError:
            pass
        try:
            return pa.from_numpy_dtype(annotation)
        except (TypeError, ValueError, NotImplementedError):
            # Unknown concrete objects are representable by their stable text
            # form; callers can always provide a physical arrow_type override.
            return pa.string()

    # Typing protocols and other opaque annotations have no runtime value
    # sample.  Preserve usability with a textual physical representation.
    return pa.string()


def _collection_arrow_type(arguments: tuple[Any, ...]) -> pa.DataType:
    item_annotation = arguments[0] if arguments else Any
    item_field = _make_arrow_field("item", item_annotation, force_name=True)
    return pa.list_(item_field)


def _items_view_arrow_type(arguments: tuple[Any, ...]) -> pa.DataType:
    if arguments and len(arguments) != 2:
        raise TypeError("ItemsView annotations require key and value types")
    key_annotation, value_annotation = arguments if arguments else (Any, Any)
    pair = pa.struct(
        [
            _make_arrow_field("_1", key_annotation, force_name=True),
            _make_arrow_field("_2", value_annotation, force_name=True),
        ]
    )
    return pa.list_(pa.field("item", pair, nullable=False))


def _tuple_arrow_type(arguments: tuple[Any, ...]) -> pa.DataType:
    if len(arguments) == 2 and arguments[1] is Ellipsis:
        return _collection_arrow_type(arguments[:1])
    return pa.struct(
        [
            _make_arrow_field(f"_{index}", item, force_name=True)
            for index, item in enumerate(arguments, start=1)
        ]
    )


def _mapping_arrow_type(arguments: tuple[Any, ...]) -> pa.DataType:
    if arguments and len(arguments) != 2:
        raise TypeError("mapping annotations require key and value types")
    key_annotation, value_annotation = arguments if arguments else (str, Any)
    key_spec = _annotation_spec(key_annotation)
    if key_spec.nullable and key_spec.config.nullable is not False:
        unknown_key = (
            key_spec.annotation in (Any, object) or key_spec.annotation is typing.Any
        )
        if unknown_key and key_spec.config.arrow_type is _MISSING:
            key_annotation = str
        else:
            raise TypeError("Arrow map keys cannot be optional")

    key_field = _make_arrow_field(
        "key", key_annotation, nullable=False, force_name=True
    )
    if pa.types.is_null(key_field.type):
        key_field = pa.field(
            "key",
            pa.string(),
            nullable=False,
            metadata=key_field.metadata,
        )
    value_field = _make_arrow_field("value", value_annotation, force_name=True)
    return pa.map_(key_field, value_field)


def _union_arrow_type(arguments: tuple[Any, ...]) -> pa.DataType:
    concrete = tuple(item for item in arguments if item not in (_NONE_TYPE, None))
    if not concrete:
        return pa.null()
    if len(concrete) == 1:
        return into_arrow_type(concrete[0])

    children: list[pa.Field] = []
    seen: list[pa.DataType] = []
    for item in concrete:
        child = _make_arrow_field(f"_{len(children) + 1}", item, force_name=True)
        if any(child.type == prior for prior in seen):
            continue
        children.append(child)
        seen.append(child.type)
    if len(children) == 1:
        return children[0].type
    return pa.dense_union(children)


def _literal_arrow_type(arguments: tuple[Any, ...]) -> pa.DataType:
    annotations: list[Any] = []
    for value in arguments:
        if value is None:
            continue
        annotation = type(value.value) if isinstance(value, enum.Enum) else type(value)
        if annotation not in annotations:
            annotations.append(annotation)
    return _union_arrow_type(tuple(annotations))


def _enum_arrow_type(annotation: type[enum.Enum]) -> pa.DataType:
    value_types: list[Any] = []
    for member in annotation:
        member_type = type(member.value)
        if member_type not in value_types:
            value_types.append(member_type)
    return _union_arrow_type(tuple(value_types)) if value_types else pa.string()


def _typed_dict_arrow_type(annotation: type[Any]) -> pa.DataType:
    try:
        hints = typing.get_type_hints(annotation, include_extras=True)
    except (NameError, TypeError) as exc:
        raise TypeError(
            f"cannot resolve TypedDict {annotation.__qualname__}: {exc}"
        ) from exc
    required = getattr(annotation, "__required_keys__", frozenset(hints))
    return pa.struct(
        [
            _make_arrow_field(
                name,
                field_annotation,
                nullable=True if name not in required else None,
                force_name=True,
            )
            for name, field_annotation in hints.items()
        ]
    )


def _named_tuple_arrow_type(annotation: type[Any]) -> pa.DataType:
    try:
        hints = typing.get_type_hints(annotation, include_extras=True)
    except (NameError, TypeError) as exc:
        raise TypeError(
            f"cannot resolve NamedTuple {annotation.__qualname__}: {exc}"
        ) from exc
    return pa.struct(
        [
            _make_arrow_field(name, hints.get(name, Any), force_name=True)
            for name in annotation._fields
        ]
    )


def _resolve_arrow_override(
    override: Any, parameters: cabc.Mapping[str, Any]
) -> tuple[pa.DataType, pa.Field | None]:
    if isinstance(override, pa.Field):
        if parameters:
            raise TypeError("parameters cannot be used with a built Arrow Field")
        return override.type, override
    if isinstance(override, pa.DataType):
        if parameters:
            raise TypeError("parameters cannot be used with a built Arrow DataType")
        return override, None
    if isinstance(override, pa.Schema):
        if parameters:
            raise TypeError("parameters cannot be used with a built Arrow Schema")
        return pa.struct(override), None
    if isinstance(override, str):
        if parameters:
            raise TypeError("parameters require a callable arrow_type factory")
        try:
            return pa.type_for_alias(override), None
        except ValueError as exc:
            raise TypeError(f"unknown Arrow type alias {override!r}") from exc
    if isinstance(override, type):
        if parameters:
            raise TypeError("parameters cannot be applied to a Python type")
        return _infer_arrow_type(override), None
    if not callable(override):
        raise TypeError(
            "arrow_type must be an Arrow DataType, Arrow Field, type alias, "
            "Python type, or callable factory"
        )
    try:
        result = override(**dict(parameters))
    except TypeError as exc:
        raise TypeError(f"cannot construct Arrow type: {exc}") from exc
    if isinstance(result, pa.Field):
        return result.type, result
    if isinstance(result, pa.DataType):
        return result, None
    if isinstance(result, pa.Schema):
        return pa.struct(result), None
    raise TypeError("arrow_type factory must return an Arrow DataType or Field")


def _apply_annotated_extra(config: _Config, extra: Any) -> None:
    if isinstance(extra, (pa.DataType, pa.Field, pa.Schema)):
        config.arrow_type = extra
        config.parameters.clear()
        return
    if isinstance(extra, FieldOptions):
        _apply_field_options(config, extra)
        return
    if isinstance(extra, dataclasses.Field):
        _apply_field_options(config, field_options(extra))
        return
    if isinstance(extra, cabc.Mapping):
        _apply_config_mapping(config, extra)
        return
    # Arbitrary Annotated metadata is deliberately retained for other tools and
    # ignored here.  This preserves interoperability with validation libraries.


def _apply_config_mapping(config: _Config, values: cabc.Mapping[Any, Any]) -> None:
    _apply_field_options(config, _options_from_mapping(values))


def _apply_field_options(config: _Config, options: FieldOptions) -> None:
    config.metadata.update(options.payload_metadata)

    if options.has("alias"):
        config.alias_explicit = True
        config.alias = options.alias if options.alias is not None else _MISSING
    if options.has("type"):
        config.arrow_type = options.type
        config.parameters.clear()
    if options.has("nullable"):
        config.nullable_explicit = True
        config.nullable = options.nullable if options.nullable is not None else _MISSING
    if options.has("parameters"):
        config.parameters = dict(options.type_parameters)
    if options.has("doc"):
        config.doc_explicit = True
        config.doc = options.doc
    if options.has("seq"):
        config.seq_explicit = True
        config.seq = options.seq if options.seq is not None else _MISSING
    if options.primary_key_explicit:
        config.primary_key_explicit = True
        config.primary_key = options.primary_key
    if options.partition_key_explicit:
        config.partition_key_explicit = True
        config.partition_key = options.partition_key
    if options.index_key_explicit:
        config.index_key_explicit = True
        config.index_key = options.index_key


def _add_key_metadata(metadata: dict[Any, Any], key: bytes, value: Any) -> None:
    if _key_enabled(value):
        metadata[key] = value


def _remove_metadata_key(metadata: dict[Any, Any], key: bytes) -> None:
    metadata.pop(key, None)
    metadata.pop(key.decode("utf-8"), None)


def _key_enabled(value: Any) -> bool:
    return value is not _MISSING and value is not False


def _default_dataclass_name(dataclass_type: type[Any]) -> str:
    alias = dataclass_type.__dict__.get("alias")
    alias_is_data_field = any(
        dc_field.name == "alias" for dc_field in dataclasses.fields(dataclass_type)
    )
    if alias is not None and not alias_is_data_field:
        _validate_alias(alias)
        return alias
    return dataclass_type.__name__.lower()


def _is_type_alias(annotation: Any) -> bool:
    return type(annotation).__name__ == "TypeAliasType" and hasattr(
        annotation, "__value__"
    )


def _expand_type_alias(annotation: Any) -> Any:
    """Expand bare and specialized PEP 695/TypeAliasType aliases."""

    if _is_type_alias(annotation):
        return _substitute_typevars(annotation.__value__, _TYPEVAR_BINDINGS.get() or {})

    origin = typing.get_origin(annotation)
    if not _is_type_alias(origin):
        return annotation

    inherited = dict(_TYPEVAR_BINDINGS.get() or {})
    inherited.update(
        _type_argument_bindings(origin, typing.get_args(annotation), inherited)
    )
    return _substitute_typevars(origin.__value__, inherited)


def _type_argument_bindings(
    generic: Any,
    arguments: tuple[Any, ...],
    inherited: cabc.Mapping[Any, Any],
) -> dict[Any, Any]:
    """Bind a generic's parameters, including PEP 696 defaults."""

    parameters = tuple(getattr(generic, "__type_params__", ()))
    if not parameters:
        parameters = tuple(getattr(generic, "__parameters__", ()))
    if len(arguments) > len(parameters):
        raise TypeError(
            f"too many type arguments for {getattr(generic, '__name__', generic)!r}"
        )

    no_default = getattr(typing, "NoDefault", _MISSING)
    resolved: dict[Any, Any] = {}
    for index, parameter in enumerate(parameters):
        if index < len(arguments):
            value = arguments[index]
        else:
            value = getattr(parameter, "__default__", no_default)
            if value is no_default:
                value = Any
        namespace = dict(inherited)
        namespace.update(resolved)
        resolved[parameter] = _substitute_typevars(value, namespace)
    return resolved


def _substitute_typevars(annotation: Any, bindings: cabc.Mapping[Any, Any]) -> Any:
    """Recursively replace TypeVars while preserving typing constructs."""

    try:
        if annotation in bindings:
            return bindings[annotation]
    except TypeError:
        pass

    if isinstance(annotation, list):
        return [_substitute_typevars(item, bindings) for item in annotation]

    origin = typing.get_origin(annotation)
    arguments = typing.get_args(annotation)
    if not arguments:
        return annotation
    if origin is typing.Literal:
        return annotation
    if origin is typing.Annotated:
        base = _substitute_typevars(arguments[0], bindings)
        return typing.Annotated[base, *arguments[1:]]

    substituted = tuple(_substitute_typevars(item, bindings) for item in arguments)
    if substituted == arguments:
        return annotation
    if origin in _UNION_ORIGINS:
        return typing.Union[substituted]  # noqa: UP007

    copy_with = getattr(annotation, "copy_with", None)
    if callable(copy_with):
        try:
            return copy_with(substituted)
        except TypeError:
            pass
    try:
        if len(substituted) == 1:
            return origin[substituted[0]]
        return origin[substituted]
    except (TypeError, AttributeError):
        return annotation


def _is_typed_dict(annotation: Any) -> bool:
    predicate = getattr(typing, "is_typeddict", None)
    return bool(predicate and predicate(annotation))


def _is_named_tuple_type(annotation: Any) -> bool:
    return (
        isinstance(annotation, type)
        and issubclass(annotation, tuple)
        and isinstance(getattr(annotation, "_fields", None), tuple)
        and hasattr(annotation, "__annotations__")
    )
