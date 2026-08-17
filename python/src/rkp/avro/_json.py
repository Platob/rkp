"""Avro's JSON encoding, compiled from the same schema model as binary.

The specification's JSON encoding differs from a naive dump: unions are tagged
by branch name and ``bytes``/``fixed`` values use Latin-1 text.  Text itself is
produced by :mod:`rkp.json` so the package keeps one JSON implementation.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import uuid
from collections.abc import Callable, Mapping
from decimal import Decimal
from typing import Any

from ._binary import (
    _EPOCH_NAIVE,
    _EPOCH_UTC,
    _decimal_bytes,
    _decimal_from_bytes,
    _epoch_micros,
    _fixed_decimal_bytes,
    _time_from_micros,
    _time_of_day,
)
from ._errors import AvroDecodeError, AvroEncodeError
from ._schema import (
    ArraySchema,
    AvroSchema,
    EnumSchema,
    FixedSchema,
    MapSchema,
    PrimitiveSchema,
    RecordSchema,
    UnionSchema,
    parse_schema,
)

__all__ = [
    "compile_json_decoder",
    "compile_json_encoder",
    "dumps",
    "into_json",
    "loads",
    "out_of_json",
]

_EPOCH_DATE = dt.date(1970, 1, 1)


def compile_json_encoder(schema: Any) -> Callable[[Any], Any]:
    """Return the cached JSON projection compiled for ``schema``."""

    parsed = parse_schema(schema)
    cached = parsed.__dict__.get("_json_encoder")
    if cached is None:
        cached = _encoder(parsed)
        object.__setattr__(parsed, "_json_encoder", cached)
    return cached


def compile_json_decoder(schema: Any) -> Callable[[Any], Any]:
    """Return the cached JSON reader compiled for ``schema``."""

    parsed = parse_schema(schema)
    cached = parsed.__dict__.get("_json_decoder")
    if cached is None:
        cached = _decoder(parsed)
        object.__setattr__(parsed, "_json_decoder", cached)
    return cached


def into_json(schema: Any, value: Any) -> Any:
    """Project one value into Avro's JSON encoding as plain Python data."""

    return compile_json_encoder(schema)(value)


def out_of_json(schema: Any, value: Any) -> Any:
    """Restore one value from Avro's JSON encoding."""

    return compile_json_decoder(schema)(value)


def dumps(schema: Any, value: Any, **kwargs: Any) -> str:
    """Encode one value as Avro JSON text."""

    from ..json import dumps as _dumps

    return _dumps(into_json(schema, value), **kwargs)


def loads(
    schema: Any, data: str | bytes | bytearray | memoryview, **kwargs: Any
) -> Any:
    """Decode Avro JSON text into Python values."""

    from ..json import loads as _loads

    return out_of_json(schema, _loads(data, **kwargs))


def _branch_name(schema: AvroSchema) -> str:
    return schema.fullname


def _encoder(schema: AvroSchema) -> Callable[[Any], Any]:
    if isinstance(schema, PrimitiveSchema):
        return _primitive_encoder(schema)
    if isinstance(schema, EnumSchema):
        symbols = frozenset(schema.symbols)
        fullname = schema.fullname

        def encode_enum(value: Any) -> Any:
            text = value.value if hasattr(value, "value") else value
            if text not in symbols:
                raise AvroEncodeError(f"{value!r} is not a symbol of enum {fullname!r}")
            return text

        return encode_enum
    if isinstance(schema, FixedSchema):
        size = schema.size
        logical = schema.logical
        scale = schema.scale or 0

        def encode_fixed(value: Any) -> Any:
            if logical == "decimal" and isinstance(value, (Decimal, int, float, str)):
                payload = _fixed_decimal_bytes(value, scale, size)
            elif logical == "uuid" and isinstance(value, (uuid.UUID, str)):
                payload = uuid.UUID(str(value)).bytes
            else:
                payload = bytes(value)
            if len(payload) != size:
                raise AvroEncodeError(
                    f"fixed {schema.fullname!r} requires {size} bytes"
                )
            return payload.decode("latin-1")

        return encode_fixed
    if isinstance(schema, ArraySchema):
        encode_item = _encoder(schema.items)
        return lambda value: [encode_item(item) for item in value]
    if isinstance(schema, MapSchema):
        encode_value = _encoder(schema.values)
        return lambda value: {
            str(key): encode_value(item) for key, item in value.items()
        }
    if isinstance(schema, RecordSchema):
        plan = tuple(
            (field.name, _encoder(field.type), field.has_default, field.default)
            for field in schema.fields
        )
        fullname = schema.fullname

        def encode_record(value: Any) -> Any:
            if dataclasses.is_dataclass(value) and not isinstance(value, type):
                source: Mapping[str, Any] = {
                    field.name: getattr(value, field.name)
                    for field in dataclasses.fields(value)
                }
            elif isinstance(value, Mapping):
                source = value
            elif isinstance(value, (tuple, list)):
                if len(value) != len(plan):
                    raise AvroEncodeError(
                        f"record {fullname!r} expects {len(plan)} positional "
                        f"values, got {len(value)}"
                    )
                source = {name: item for (name, *_), item in zip(plan, value)}
            else:
                raise AvroEncodeError(
                    f"record {fullname!r} expects a mapping, dataclass, or "
                    f"sequence, got {type(value).__qualname__}"
                )
            result: dict[str, Any] = {}
            for name, encode_field, has_default, default in plan:
                if name in source:
                    result[name] = encode_field(source[name])
                elif has_default:
                    result[name] = encode_field(default)
                else:
                    raise AvroEncodeError(
                        f"record {fullname!r} is missing field {name!r}"
                    )
            return result

        return encode_record
    if isinstance(schema, UnionSchema):
        from ._binary import _matcher

        branches = tuple(
            (_branch_name(option), _encoder(option), _matcher(option))
            for option in schema.options
        )

        def encode_union(value: Any) -> Any:
            for name, encode_branch, matches in branches:
                if matches(value):
                    if name == "null":
                        return None
                    return {name: encode_branch(value)}
            raise AvroEncodeError(
                f"value of type {type(value).__qualname__} "
                "does not match any union branch"
            )

        return encode_union
    raise AvroEncodeError(f"unsupported Avro schema {schema!r}")


def _primitive_encoder(schema: PrimitiveSchema) -> Callable[[Any], Any]:
    primitive = schema.primitive
    logical = schema.logical

    if primitive == "null":
        return lambda value: None
    if primitive == "boolean":
        return bool
    if primitive in {"int", "long"}:
        if logical == "date":

            def encode_date(value: Any) -> Any:
                if isinstance(value, dt.datetime):
                    value = value.date()
                if isinstance(value, dt.date):
                    return (value - _EPOCH_DATE).days
                return int(value)

            return encode_date
        if logical in {"time-millis", "time-micros"}:
            divisor = 1000 if logical == "time-millis" else 1

            def encode_time(value: Any) -> Any:
                if isinstance(value, dt.time):
                    return _time_of_day(value) // divisor
                return int(value)

            return encode_time
        if logical is not None and "timestamp" in logical:
            local = logical.startswith("local-")
            base = logical.removeprefix("local-")

            def encode_timestamp(value: Any) -> Any:
                if not isinstance(value, dt.datetime):
                    return int(value)
                micros = _epoch_micros(value, local=local)
                if base == "timestamp-millis":
                    return micros // 1000
                if base == "timestamp-nanos":
                    return micros * 1000
                return micros

            return encode_timestamp
        return int
    if primitive in {"float", "double"}:
        return float
    if primitive == "bytes":
        if logical in {"decimal", "big-decimal"}:
            scale = schema.scale or 0
            return lambda value: _decimal_bytes(value, scale).decode("latin-1")
        return lambda value: (
            value.encode("latin-1") if isinstance(value, str) else bytes(value)
        ).decode("latin-1")
    if logical == "uuid":
        return str
    return lambda value: value if type(value) is str else str(value)


def _decoder(schema: AvroSchema) -> Callable[[Any], Any]:
    if isinstance(schema, PrimitiveSchema):
        return _primitive_decoder(schema)
    if isinstance(schema, EnumSchema):
        symbols = frozenset(schema.symbols)
        default = schema.default

        def decode_enum(value: Any) -> Any:
            if value in symbols:
                return value
            if default is not None:
                return default
            raise AvroDecodeError(f"{value!r} is not a symbol of {schema.fullname!r}")

        return decode_enum
    if isinstance(schema, FixedSchema):
        size = schema.size
        logical = schema.logical
        scale = schema.scale or 0

        def decode_fixed(value: Any) -> Any:
            payload = (
                value.encode("latin-1") if isinstance(value, str) else bytes(value)
            )
            if len(payload) != size:
                raise AvroDecodeError(
                    f"fixed {schema.fullname!r} requires {size} bytes"
                )
            if logical == "decimal":
                return _decimal_from_bytes(payload, scale)
            if logical == "uuid":
                return uuid.UUID(bytes=payload)
            return payload

        return decode_fixed
    if isinstance(schema, ArraySchema):
        decode_item = _decoder(schema.items)
        return lambda value: [decode_item(item) for item in value]
    if isinstance(schema, MapSchema):
        decode_value = _decoder(schema.values)
        return lambda value: {key: decode_value(item) for key, item in value.items()}
    if isinstance(schema, RecordSchema):
        plan = tuple(
            (field.name, _decoder(field.type), field.has_default, field.default)
            for field in schema.fields
        )
        fullname = schema.fullname

        def decode_record(value: Any) -> Any:
            if not isinstance(value, Mapping):
                raise AvroDecodeError(f"record {fullname!r} expects a JSON object")
            result: dict[str, Any] = {}
            for name, decode_field, has_default, default in plan:
                if name in value:
                    result[name] = decode_field(value[name])
                elif has_default:
                    result[name] = decode_field(default)
                else:
                    raise AvroDecodeError(
                        f"record {fullname!r} is missing field {name!r}"
                    )
            return result

        return decode_record
    if isinstance(schema, UnionSchema):
        branches = {_branch_name(option): _decoder(option) for option in schema.options}
        has_null = "null" in branches

        def decode_union(value: Any) -> Any:
            if value is None:
                if not has_null:
                    raise AvroDecodeError("union does not accept null")
                return None
            if not isinstance(value, Mapping) or len(value) != 1:
                raise AvroDecodeError(
                    "Avro JSON unions must be a single-entry object keyed by branch"
                )
            ((name, item),) = value.items()
            decode_branch = branches.get(name)
            if decode_branch is None:
                raise AvroDecodeError(f"unknown union branch {name!r}")
            return decode_branch(item)

        return decode_union
    raise AvroDecodeError(f"unsupported Avro schema {schema!r}")


def _primitive_decoder(schema: PrimitiveSchema) -> Callable[[Any], Any]:
    primitive = schema.primitive
    logical = schema.logical

    if primitive == "null":
        return lambda value: None
    if primitive == "boolean":
        return bool
    if primitive in {"int", "long"}:
        if logical == "date":
            return lambda value: _EPOCH_DATE + dt.timedelta(days=int(value))
        if logical == "time-millis":
            return lambda value: _time_from_micros(int(value) * 1000)
        if logical == "time-micros":
            return lambda value: _time_from_micros(int(value))
        if logical is not None and "timestamp" in logical:
            local = logical.startswith("local-")
            base = logical.removeprefix("local-")

            def decode_timestamp(value: Any) -> Any:
                raw = int(value)
                if base == "timestamp-millis":
                    micros = raw * 1000
                elif base == "timestamp-nanos":
                    micros = raw // 1000
                else:
                    micros = raw
                epoch = _EPOCH_NAIVE if local else _EPOCH_UTC
                return epoch + dt.timedelta(microseconds=micros)

            return decode_timestamp
        return int
    if primitive in {"float", "double"}:
        return float
    if primitive == "bytes":
        if logical in {"decimal", "big-decimal"}:
            scale = schema.scale or 0
            return lambda value: _decimal_from_bytes(
                value.encode("latin-1") if isinstance(value, str) else bytes(value),
                scale,
            )
        return lambda value: (
            value.encode("latin-1") if isinstance(value, str) else bytes(value)
        )
    if logical == "uuid":
        return lambda value: uuid.UUID(str(value))
    return str
