"""Compiled Avro binary encoders and decoders.

Every schema is compiled once into a tree of closures.  Encoding and decoding
then run without re-inspecting the schema, which is what keeps large record
streams competitive with hand-written codecs while staying dependency-free.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import struct
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from decimal import Decimal
from typing import Any

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
    fingerprint_bytes,
    parse_schema,
)

__all__ = [
    "Reader",
    "compile_decoder",
    "compile_encoder",
    "decode",
    "decode_single_object",
    "encode",
    "encode_into",
    "encode_single_object",
]

Encoder = Callable[[Any, bytearray], None]
Decoder = Callable[["Reader"], Any]

_INT32_MIN = -(2**31)
_INT32_MAX = 2**31 - 1
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1
_EPOCH_DATE = dt.date(1970, 1, 1)
_EPOCH_UTC = dt.datetime(1970, 1, 1, tzinfo=dt.UTC)
# Avro's local-timestamp logical types are deliberately zone-free.
_EPOCH_NAIVE = dt.datetime(1970, 1, 1)  # noqa: DTZ001
_PACK_FLOAT = struct.Struct("<f").pack
_PACK_DOUBLE = struct.Struct("<d").pack
_UNPACK_FLOAT = struct.Struct("<f").unpack_from
_UNPACK_DOUBLE = struct.Struct("<d").unpack_from
_PACK_DURATION = struct.Struct("<III").pack
_UNPACK_DURATION = struct.Struct("<III").unpack_from
_SINGLE_OBJECT_MARKER = b"\xc3\x01"


class Reader:
    """A cursor over encoded Avro bytes shared by compiled decoders."""

    __slots__ = ("data", "pos")

    def __init__(self, data: bytes | bytearray | memoryview, pos: int = 0) -> None:
        self.data = data if isinstance(data, (bytes, bytearray)) else bytes(data)
        self.pos = pos

    @property
    def remaining(self) -> int:
        """Return the number of bytes left in the buffer."""

        return len(self.data) - self.pos

    def read_long(self) -> int:
        """Read one zig-zag encoded variable-length integer."""

        data = self.data
        pos = self.pos
        try:
            byte = data[pos]
            pos += 1
            result = byte & 0x7F
            shift = 7
            while byte & 0x80:
                byte = data[pos]
                pos += 1
                result |= (byte & 0x7F) << shift
                shift += 7
        except IndexError as exc:
            raise AvroDecodeError("truncated Avro variable-length integer") from exc
        self.pos = pos
        return (result >> 1) ^ -(result & 1)

    def read_bytes(self, size: int) -> bytes:
        """Read a fixed number of raw bytes."""

        pos = self.pos
        end = pos + size
        if size < 0 or end > len(self.data):
            raise AvroDecodeError("truncated Avro payload")
        self.pos = end
        return bytes(self.data[pos:end])


def compile_encoder(schema: Any) -> Encoder:
    """Return the cached encoder closure compiled for ``schema``."""

    parsed = parse_schema(schema)
    cached = parsed.__dict__.get("_binary_encoder")
    if cached is None:
        cached = _encoder(parsed)
        object.__setattr__(parsed, "_binary_encoder", cached)
    return cached


def compile_decoder(schema: Any) -> Decoder:
    """Return the cached decoder closure compiled for ``schema``."""

    parsed = parse_schema(schema)
    cached = parsed.__dict__.get("_binary_decoder")
    if cached is None:
        cached = _decoder(parsed)
        object.__setattr__(parsed, "_binary_decoder", cached)
    return cached


def encode(schema: Any, value: Any) -> bytes:
    """Encode one value into Avro's binary representation."""

    out = bytearray()
    compile_encoder(schema)(value, out)
    return bytes(out)


def encode_into(schema: Any, value: Any, out: bytearray) -> bytearray:
    """Append one encoded value to a caller-owned buffer."""

    if not isinstance(out, bytearray):
        raise TypeError("out must be a bytearray")
    compile_encoder(schema)(value, out)
    return out


def decode(schema: Any, data: bytes | bytearray | memoryview | Reader) -> Any:
    """Decode one value from Avro's binary representation."""

    reader = data if isinstance(data, Reader) else Reader(data)
    return compile_decoder(schema)(reader)


def encode_single_object(schema: Any, value: Any) -> bytes:
    """Encode one value using Avro's single-object framing."""

    parsed = parse_schema(schema)
    out = bytearray(_SINGLE_OBJECT_MARKER)
    out += fingerprint_bytes(parsed)
    compile_encoder(parsed)(value, out)
    return bytes(out)


def decode_single_object(schema: Any, data: bytes | bytearray | memoryview) -> Any:
    """Decode single-object framed data, validating its schema fingerprint."""

    parsed = parse_schema(schema)
    payload = bytes(data)
    if len(payload) < 10 or payload[:2] != _SINGLE_OBJECT_MARKER:
        raise AvroDecodeError("missing Avro single-object marker")
    expected = fingerprint_bytes(parsed)
    if payload[2:10] != expected:
        raise AvroDecodeError(
            "Avro single-object fingerprint does not match the reader schema"
        )
    return compile_decoder(parsed)(Reader(payload, 10))


def _write_long(value: int, out: bytearray) -> None:
    encoded = (value << 1) ^ (value >> 63)
    while encoded & ~0x7F:
        out.append((encoded & 0x7F) | 0x80)
        encoded >>= 7
    out.append(encoded)


def _encoder(schema: AvroSchema) -> Encoder:
    if isinstance(schema, PrimitiveSchema):
        return _primitive_encoder(schema)
    if isinstance(schema, EnumSchema):
        return _enum_encoder(schema)
    if isinstance(schema, FixedSchema):
        return _fixed_encoder(schema)
    if isinstance(schema, ArraySchema):
        return _array_encoder(schema)
    if isinstance(schema, MapSchema):
        return _map_encoder(schema)
    if isinstance(schema, UnionSchema):
        return _union_encoder(schema)
    if isinstance(schema, RecordSchema):
        return _record_encoder(schema)
    raise AvroEncodeError(f"unsupported Avro schema {schema!r}")


def _primitive_encoder(schema: PrimitiveSchema) -> Encoder:
    logical = schema.logical
    primitive = schema.primitive

    if primitive == "null":

        def encode_null(value: Any, out: bytearray) -> None:
            if value is not None:
                raise AvroEncodeError(f"expected null, got {type(value).__qualname__}")

        return encode_null

    if primitive == "boolean":

        def encode_boolean(value: Any, out: bytearray) -> None:
            out.append(1 if value else 0)

        return encode_boolean

    if primitive == "int":
        if logical == "date":

            def encode_date(value: Any, out: bytearray) -> None:
                if isinstance(value, dt.datetime):
                    value = value.date()
                if isinstance(value, dt.date):
                    value = (value - _EPOCH_DATE).days
                _write_int(value, out)

            return encode_date
        if logical == "time-millis":

            def encode_time_millis(value: Any, out: bytearray) -> None:
                if isinstance(value, dt.time):
                    value = _time_of_day(value) // 1000
                _write_int(value, out)

            return encode_time_millis
        return _write_int

    if primitive == "long":
        if logical in {"time-micros"}:

            def encode_time_micros(value: Any, out: bytearray) -> None:
                if isinstance(value, dt.time):
                    value = _time_of_day(value)
                _write_int64(value, out)

            return encode_time_micros
        if logical is not None and logical.endswith(
            ("timestamp-millis", "timestamp-micros", "timestamp-nanos")
        ):
            local = logical.startswith("local-")
            scale = {
                "timestamp-millis": 1000,
                "timestamp-micros": 1,
                "timestamp-nanos": 1,
            }[logical.removeprefix("local-")]
            nanos = logical.endswith("nanos")

            def encode_timestamp(value: Any, out: bytearray) -> None:
                if isinstance(value, dt.datetime):
                    micros = _epoch_micros(value, local=local)
                    value = micros * 1000 if nanos else micros // scale
                _write_int64(value, out)

            return encode_timestamp
        return _write_int64

    if primitive == "float":

        def encode_float(value: Any, out: bytearray) -> None:
            try:
                out += _PACK_FLOAT(value)
            except (struct.error, TypeError, OverflowError) as exc:
                raise AvroEncodeError(f"invalid Avro float value {value!r}") from exc

        return encode_float

    if primitive == "double":

        def encode_double(value: Any, out: bytearray) -> None:
            try:
                out += _PACK_DOUBLE(value)
            except (struct.error, TypeError, OverflowError) as exc:
                raise AvroEncodeError(f"invalid Avro double value {value!r}") from exc

        return encode_double

    if primitive == "bytes":
        if logical in {"decimal", "big-decimal"}:
            scale = schema.scale or 0

            def encode_decimal(value: Any, out: bytearray) -> None:
                payload = (
                    _decimal_bytes(value, scale)
                    if isinstance(value, (Decimal, int, float, str))
                    else bytes(value)
                )
                _write_long(len(payload), out)
                out += payload

            return encode_decimal

        def encode_bytes(value: Any, out: bytearray) -> None:
            if not isinstance(value, bytes):
                if isinstance(value, (bytearray, memoryview)):
                    value = bytes(value)
                elif isinstance(value, str):
                    value = value.encode("latin-1")
                else:
                    raise AvroEncodeError(
                        f"expected bytes, got {type(value).__qualname__}"
                    )
            _write_long(len(value), out)
            out += value

        return encode_bytes

    if logical == "uuid":

        def encode_uuid(value: Any, out: bytearray) -> None:
            encoded = str(value).encode("utf-8")
            _write_long(len(encoded), out)
            out += encoded

        return encode_uuid

    def encode_string(value: Any, out: bytearray) -> None:
        if type(value) is not str:
            if isinstance(value, str):
                value = str(value)
            elif isinstance(value, (bytes, bytearray, memoryview)):
                value = bytes(value).decode("utf-8")
            else:
                raise AvroEncodeError(
                    f"expected string, got {type(value).__qualname__}"
                )
        encoded = value.encode("utf-8")
        _write_long(len(encoded), out)
        out += encoded

    return encode_string


def _write_int(value: Any, out: bytearray) -> None:
    if type(value) is not int:
        value = _coerce_integer(value)
    if not _INT32_MIN <= value <= _INT32_MAX:
        raise AvroEncodeError(f"value {value} does not fit in an Avro int")
    _write_long(value, out)


def _write_int64(value: Any, out: bytearray) -> None:
    if type(value) is not int:
        value = _coerce_integer(value)
    if not _INT64_MIN <= value <= _INT64_MAX:
        raise AvroEncodeError(f"value {value} does not fit in an Avro long")
    _write_long(value, out)


def _coerce_integer(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return int(value)
    raise AvroEncodeError(f"expected an integer, got {type(value).__qualname__}")


def _time_of_day(value: dt.time) -> int:
    return (
        value.hour * 3_600_000_000
        + value.minute * 60_000_000
        + value.second * 1_000_000
        + value.microsecond
    )


def _epoch_micros(value: dt.datetime, *, local: bool) -> int:
    if local:
        if value.tzinfo is not None:
            value = value.astimezone(dt.UTC).replace(tzinfo=None)
        delta = value - _EPOCH_NAIVE
    else:
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.UTC)
        delta = value - _EPOCH_UTC
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds


def _decimal_bytes(value: Any, scale: int) -> bytes:
    decimal = value if isinstance(value, Decimal) else Decimal(str(value))
    unscaled = int(decimal.scaleb(scale).to_integral_value())
    length = max(1, (unscaled.bit_length() + 8) // 8)
    return unscaled.to_bytes(length, "big", signed=True)


def _fixed_decimal_bytes(value: Any, scale: int, size: int) -> bytes:
    decimal = value if isinstance(value, Decimal) else Decimal(str(value))
    unscaled = int(decimal.scaleb(scale).to_integral_value())
    try:
        return unscaled.to_bytes(size, "big", signed=True)
    except OverflowError as exc:
        raise AvroEncodeError(f"decimal {value} does not fit in fixed({size})") from exc


def _enum_encoder(schema: EnumSchema) -> Encoder:
    indexes = {symbol: index for index, symbol in enumerate(schema.symbols)}
    default = schema.default
    fullname = schema.fullname

    def encode_enum(value: Any, out: bytearray) -> None:
        key = (
            value.value
            if hasattr(value, "value") and not isinstance(value, str)
            else value
        )
        index = indexes.get(key)
        if index is None and default is not None:
            index = indexes[default]
        if index is None:
            raise AvroEncodeError(f"{value!r} is not a symbol of enum {fullname!r}")
        _write_long(index, out)

    return encode_enum


def _fixed_encoder(schema: FixedSchema) -> Encoder:
    size = schema.size
    fullname = schema.fullname
    logical = schema.logical
    scale = schema.scale or 0

    def encode_fixed(value: Any, out: bytearray) -> None:
        if logical in {"decimal"} and isinstance(value, (Decimal, int, float, str)):
            payload = _fixed_decimal_bytes(value, scale, size)
        elif logical == "uuid" and isinstance(value, (uuid.UUID, str)):
            payload = uuid.UUID(str(value)).bytes
        elif logical == "duration" and isinstance(value, (tuple, list)):
            payload = _PACK_DURATION(*(int(item) for item in value))
        elif isinstance(value, (bytes, bytearray, memoryview)):
            payload = bytes(value)
        else:
            raise AvroEncodeError(
                f"expected bytes for fixed {fullname!r}, got {type(value).__qualname__}"
            )
        if len(payload) != size:
            raise AvroEncodeError(
                f"fixed {fullname!r} requires {size} bytes, got {len(payload)}"
            )
        out += payload

    return encode_fixed


def _array_encoder(schema: ArraySchema) -> Encoder:
    encode_item = _encoder(schema.items)

    def encode_array(value: Any, out: bytearray) -> None:
        if isinstance(value, (str, bytes, bytearray, Mapping)) or not isinstance(
            value, (Sequence, Iterable)
        ):
            raise AvroEncodeError(f"expected an array, got {type(value).__qualname__}")
        items = value if isinstance(value, (list, tuple)) else list(value)
        if items:
            _write_long(len(items), out)
            for item in items:
                encode_item(item, out)
        _write_long(0, out)

    return encode_array


def _map_encoder(schema: MapSchema) -> Encoder:
    encode_value = _encoder(schema.values)

    def encode_map(value: Any, out: bytearray) -> None:
        if not isinstance(value, Mapping):
            raise AvroEncodeError(f"expected a map, got {type(value).__qualname__}")
        if value:
            _write_long(len(value), out)
            for key, item in value.items():
                if type(key) is not str:
                    key = str(key)
                encoded = key.encode("utf-8")
                _write_long(len(encoded), out)
                out += encoded
                encode_value(item, out)
        _write_long(0, out)

    return encode_map


def _record_encoder(schema: RecordSchema) -> Encoder:
    plan = tuple(
        (field.name, _encoder(field.type), field.has_default, field.default)
        for field in schema.fields
    )
    fullname = schema.fullname

    def encode_record(value: Any, out: bytearray) -> None:
        if isinstance(value, Mapping):
            for name, encode_field, has_default, default in plan:
                item = value.get(name, _ABSENT)
                if item is _ABSENT:
                    if not has_default:
                        raise AvroEncodeError(
                            f"record {fullname!r} is missing field {name!r}"
                        )
                    item = default
                encode_field(item, out)
            return
        if dataclasses.is_dataclass(value) and not isinstance(value, type):
            for name, encode_field, has_default, default in plan:
                item = getattr(value, name, _ABSENT)
                if item is _ABSENT:
                    if not has_default:
                        raise AvroEncodeError(
                            f"record {fullname!r} is missing field {name!r}"
                        )
                    item = default
                encode_field(item, out)
            return
        if isinstance(value, (tuple, list)):
            # Positional rows keep tuple-typed record fields encodable, which
            # is how RKP projects ``tuple[...]`` annotations into a struct.
            if len(value) != len(plan):
                raise AvroEncodeError(
                    f"record {fullname!r} expects {len(plan)} positional values, "
                    f"got {len(value)}"
                )
            for item, (_name, encode_field, _has_default, _default) in zip(
                value, plan, strict=True
            ):
                encode_field(item, out)
            return
        raise AvroEncodeError(
            f"record {fullname!r} expects a mapping, dataclass, or sequence, "
            f"got {type(value).__qualname__}"
        )

    return encode_record


def _union_encoder(schema: UnionSchema) -> Encoder:
    encoders = tuple(_encoder(option) for option in schema.options)
    matchers = tuple(_matcher(option) for option in schema.options)
    null_index = next(
        (
            index
            for index, option in enumerate(schema.options)
            if isinstance(option, PrimitiveSchema) and option.primitive == "null"
        ),
        None,
    )

    if len(schema.options) == 2 and null_index is not None:
        value_index = 1 - null_index
        encode_value = encoders[value_index]
        null_tag = bytes([null_index << 1])
        value_tag = bytes([value_index << 1])

        def encode_optional(value: Any, out: bytearray) -> None:
            if value is None:
                out += null_tag
            else:
                out += value_tag
                encode_value(value, out)

        return encode_optional

    def encode_union(value: Any, out: bytearray) -> None:
        for index, matches in enumerate(matchers):
            if matches(value):
                _write_long(index, out)
                encoders[index](value, out)
                return
        raise AvroEncodeError(
            f"value of type {type(value).__qualname__} does not match any union branch"
        )

    return encode_union


class _Absent:
    __slots__ = ()

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return "<absent>"


_ABSENT = _Absent()


def _matcher(schema: AvroSchema) -> Callable[[Any], bool]:
    if isinstance(schema, PrimitiveSchema):
        primitive = schema.primitive
        logical = schema.logical
        if primitive == "null":
            return lambda value: value is None
        if primitive == "boolean":
            return lambda value: isinstance(value, bool)
        if primitive in {"int", "long"}:
            if logical in {"date"}:
                return lambda value: (
                    isinstance(value, dt.date) and not isinstance(value, dt.datetime)
                )
            if logical is not None and "time" in logical:
                return lambda value: (
                    isinstance(value, (dt.datetime, dt.time))
                    or (isinstance(value, int) and not isinstance(value, bool))
                )
            limit = _INT32_MAX if primitive == "int" else _INT64_MAX
            return lambda value: (
                isinstance(value, int)
                and not isinstance(value, bool)
                and -limit - 1 <= value <= limit
            )
        if primitive in {"float", "double"}:
            return lambda value: (
                isinstance(value, float)
                or (isinstance(value, int) and not isinstance(value, bool))
            )
        if primitive == "bytes":
            if logical in {"decimal", "big-decimal"}:
                return lambda value: isinstance(value, (Decimal, bytes, bytearray))
            return lambda value: isinstance(value, (bytes, bytearray, memoryview))
        if logical == "uuid":
            return lambda value: isinstance(value, (uuid.UUID, str))
        return lambda value: isinstance(value, str)
    if isinstance(schema, EnumSchema):
        symbols = frozenset(schema.symbols)
        return lambda value: isinstance(value, str) and value in symbols
    if isinstance(schema, FixedSchema):
        size = schema.size
        if schema.logical == "decimal":
            return lambda value: (
                isinstance(value, Decimal)
                or (isinstance(value, (bytes, bytearray)) and len(value) == size)
            )
        if schema.logical == "uuid":
            return lambda value: (
                isinstance(value, uuid.UUID)
                or (isinstance(value, (bytes, bytearray)) and len(value) == size)
            )
        return lambda value: (
            isinstance(value, (bytes, bytearray, memoryview)) and len(value) == size
        )
    if isinstance(schema, ArraySchema):
        return lambda value: isinstance(value, (list, tuple))
    if isinstance(schema, MapSchema):
        return lambda value: isinstance(value, Mapping)
    if isinstance(schema, RecordSchema):
        names = frozenset(
            field.name for field in schema.fields if not field.has_default
        )
        return lambda value: (
            (isinstance(value, Mapping) and names <= set(value))
            or (dataclasses.is_dataclass(value) and not isinstance(value, type))
        )
    raise AvroEncodeError(f"unsupported Avro union branch {schema!r}")


def _decoder(schema: AvroSchema) -> Decoder:
    if isinstance(schema, PrimitiveSchema):
        return _primitive_decoder(schema)
    if isinstance(schema, EnumSchema):
        symbols = schema.symbols
        default = schema.default

        def decode_enum(reader: Reader) -> Any:
            index = reader.read_long()
            if 0 <= index < len(symbols):
                return symbols[index]
            if default is not None:
                return default
            raise AvroDecodeError(f"enum index {index} is out of range")

        return decode_enum
    if isinstance(schema, FixedSchema):
        size = schema.size
        logical = schema.logical
        scale = schema.scale or 0
        if logical == "decimal":

            def decode_fixed_decimal(reader: Reader) -> Any:
                raw = reader.read_bytes(size)
                return _decimal_from_bytes(raw, scale)

            return decode_fixed_decimal
        if logical == "uuid":
            return lambda reader: uuid.UUID(bytes=reader.read_bytes(size))
        if logical == "duration":

            def decode_duration(reader: Reader) -> Any:
                return _UNPACK_DURATION(reader.read_bytes(size), 0)

            return decode_duration
        return lambda reader: reader.read_bytes(size)
    if isinstance(schema, ArraySchema):
        decode_item = _decoder(schema.items)

        def decode_array(reader: Reader) -> Any:
            result: list[Any] = []
            for _ in _blocks(reader):
                result.append(decode_item(reader))
            return result

        return decode_array
    if isinstance(schema, MapSchema):
        decode_value = _decoder(schema.values)

        def decode_map(reader: Reader) -> Any:
            result: dict[str, Any] = {}
            for _ in _blocks(reader):
                size = reader.read_long()
                key = reader.read_bytes(size).decode("utf-8")
                result[key] = decode_value(reader)
            return result

        return decode_map
    if isinstance(schema, UnionSchema):
        decoders = tuple(_decoder(option) for option in schema.options)

        def decode_union(reader: Reader) -> Any:
            index = reader.read_long()
            if not 0 <= index < len(decoders):
                raise AvroDecodeError(f"union branch {index} is out of range")
            return decoders[index](reader)

        return decode_union
    if isinstance(schema, RecordSchema):
        plan = tuple((field.name, _decoder(field.type)) for field in schema.fields)

        def decode_record(reader: Reader) -> Any:
            return {name: decode_field(reader) for name, decode_field in plan}

        return decode_record
    raise AvroDecodeError(f"unsupported Avro schema {schema!r}")


def _primitive_decoder(schema: PrimitiveSchema) -> Decoder:
    primitive = schema.primitive
    logical = schema.logical

    if primitive == "null":
        return lambda reader: None
    if primitive == "boolean":

        def decode_boolean(reader: Reader) -> Any:
            return reader.read_bytes(1)[0] != 0

        return decode_boolean
    if primitive in {"int", "long"}:
        if logical == "date":
            return lambda reader: _EPOCH_DATE + dt.timedelta(days=reader.read_long())
        if logical == "time-millis":
            return lambda reader: _time_from_micros(reader.read_long() * 1000)
        if logical == "time-micros":
            return lambda reader: _time_from_micros(reader.read_long())
        if logical is not None and logical.endswith(
            ("timestamp-millis", "timestamp-micros", "timestamp-nanos")
        ):
            local = logical.startswith("local-")
            base = logical.removeprefix("local-")
            multiplier = {
                "timestamp-millis": 1000,
                "timestamp-micros": 1,
                "timestamp-nanos": 1,
            }[base]
            nanos = base == "timestamp-nanos"

            def decode_timestamp(reader: Reader) -> Any:
                raw = reader.read_long()
                micros = raw // 1000 if nanos else raw * multiplier
                epoch = _EPOCH_NAIVE if local else _EPOCH_UTC
                return epoch + dt.timedelta(microseconds=micros)

            return decode_timestamp
        return lambda reader: reader.read_long()
    if primitive == "float":

        def decode_float(reader: Reader) -> Any:
            return _UNPACK_FLOAT(reader.read_bytes(4), 0)[0]

        return decode_float
    if primitive == "double":

        def decode_double(reader: Reader) -> Any:
            return _UNPACK_DOUBLE(reader.read_bytes(8), 0)[0]

        return decode_double
    if primitive == "bytes":
        if logical in {"decimal", "big-decimal"}:
            scale = schema.scale or 0

            def decode_decimal(reader: Reader) -> Any:
                size = reader.read_long()
                return _decimal_from_bytes(reader.read_bytes(size), scale)

            return decode_decimal

        def decode_bytes(reader: Reader) -> Any:
            return reader.read_bytes(reader.read_long())

        return decode_bytes
    if logical == "uuid":

        def decode_uuid(reader: Reader) -> Any:
            size = reader.read_long()
            return uuid.UUID(reader.read_bytes(size).decode("utf-8"))

        return decode_uuid

    def decode_string(reader: Reader) -> Any:
        size = reader.read_long()
        return reader.read_bytes(size).decode("utf-8")

    return decode_string


def _blocks(reader: Reader) -> Iterator[int]:
    """Yield one index per item across Avro's counted block framing."""

    while True:
        count = reader.read_long()
        if count == 0:
            return
        if count < 0:
            count = -count
            # A negative count is followed by the block size in bytes, which
            # readers may use to skip.  Decoding still walks every item.
            reader.read_long()
        yield from range(count)


def _time_from_micros(value: int) -> dt.time:
    seconds, micros = divmod(value, 1_000_000)
    minutes, second = divmod(seconds, 60)
    hour, minute = divmod(minutes, 60)
    return dt.time(hour % 24, minute, second, micros)


def _decimal_from_bytes(raw: bytes, scale: int) -> Decimal:
    unscaled = int.from_bytes(raw, "big", signed=True) if raw else 0
    return Decimal(unscaled).scaleb(-scale) if scale else Decimal(unscaled)
