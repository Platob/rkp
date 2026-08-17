"""One interpreter for the ``partition_key`` and ``index_key`` field roles.

RKP declares partitioning on the field itself, so the same wire value is read
by the Iceberg catalog adapter, by Glue, and by dataset paths.  Each of them
used to parse it separately and they disagreed: ``b"1"`` meant "position 1" to
Glue and "enabled, unordered" to Iceberg, so a single declaration produced two
different partition column orders.  This module owns the grammar so that class
of divergence cannot recur.

Nothing here imports Arrow, Iceberg, or Glue.  Roles are decoded while reading
wire metadata, which sits below every adapter, and staying dependency-free is
what lets all of them share one answer.  :meth:`Transform.into_iceberg` reaches
for PyIceberg inside the method, where the caller has already chosen Iceberg.

Parsing is deliberately lenient: an unrecognized transform name yields
:attr:`TransformKind.UNKNOWN` carrying the source text rather than raising.
Record types are declared at import time but their partition specs are
projected much later, and the error belongs at projection -- a test declares a
module-level record with ``partition_key="not-a-transform"`` and asserts the
failure surfaces from the projection call.  Leniency is only safe because
``UNKNOWN`` can never succeed: :meth:`~Transform.into_iceberg`,
:meth:`~Transform.into_glue`, :meth:`~Transform.result_type`, and
:meth:`~Transform.apply` all raise on it.  That is the difference from
PyIceberg's ``UnknownTransform``, which silently succeeded into an unusable
partition spec and had to be re-rejected by hand at every call site.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import enum
import re
from decimal import Decimal, localcontext
from typing import TYPE_CHECKING, Any, Final, cast

if TYPE_CHECKING:
    from .datatypes import DataType

__all__ = [
    "IDENTITY",
    "IndexRole",
    "PartitionRole",
    "Transform",
    "TransformKind",
]

# The disabled set is ``metadata_enabled``'s rule, kept verbatim so a role
# value that reads as "off" for every other flag also reads as "off" here.
_DISABLED = frozenset({"", "false", "no", "null"})
_ENABLED = frozenset({"true", "yes"})
_POSITION = re.compile(r"[+-]?[0-9]+")
_PARAMETERIZED = re.compile(r"(?P<name>[a-z_]+)\[(?P<parameter>[0-9]+)\]")
_AS = re.compile(r"\s+as\s+", re.IGNORECASE)
_DIRECTIONS = {"asc": False, "desc": True}
_NULL_ORDERS = {"first": True, "last": False}

_EPOCH_YEAR = 1970
_EPOCH_DATE = dt.date(_EPOCH_YEAR, 1, 1)
_EPOCH_TIME = dt.datetime(_EPOCH_YEAR, 1, 1)
_MICROS_PER_UNIT = {"s": 1_000_000, "ms": 1_000, "us": 1}

# ``DataType.precision`` is capped at 38, so this leaves room for the widest
# unscaled integer a decimal truncation can produce.
_DECIMAL_PRECISION = 42


class TransformKind(enum.Enum):
    """The partition and sort transforms RKP maps between protocols."""

    IDENTITY = "identity"
    YEAR = "year"
    MONTH = "month"
    DAY = "day"
    HOUR = "hour"
    BUCKET = "bucket"
    TRUNCATE = "truncate"
    VOID = "void"
    UNKNOWN = "unknown"

    @property
    def is_parameterized(self) -> bool:
        """Return whether the kind requires a bracketed integer parameter."""

        return self in {TransformKind.BUCKET, TransformKind.TRUNCATE}


# Source kinds are matched by ``TypeKind`` member name rather than by the enum
# itself: ``datatypes`` imports this module, so importing it back would be a
# cycle, and a name comparison needs no import at all.  ``test_roles`` asserts
# every name below is a real ``TypeKind`` member so a typo cannot go unnoticed.
_PRIMITIVE = frozenset(
    {
        "BOOLEAN",
        "INT32",
        "INT64",
        "FLOAT32",
        "FLOAT64",
        "DECIMAL",
        "DATE",
        "TIME",
        "TIMESTAMP",
        "STRING",
        "BINARY",
        "FIXED",
        "UUID",
    }
)
_DATE_LIKE = frozenset({"DATE", "TIMESTAMP"})
_HASHABLE = _PRIMITIVE - {"BOOLEAN", "FLOAT32", "FLOAT64"}
_ORDERED_PREFIX = frozenset({"INT32", "INT64", "DECIMAL", "STRING", "BINARY"})

# ``None`` means every kind, which is what Iceberg's void transform accepts.
_ACCEPTED: dict[TransformKind, frozenset[str] | None] = {
    TransformKind.IDENTITY: _PRIMITIVE,
    TransformKind.YEAR: _DATE_LIKE,
    TransformKind.MONTH: _DATE_LIKE,
    TransformKind.DAY: _DATE_LIKE,
    TransformKind.HOUR: frozenset({"TIMESTAMP"}),
    TransformKind.BUCKET: _HASHABLE,
    TransformKind.TRUNCATE: _ORDERED_PREFIX,
    TransformKind.VOID: None,
    TransformKind.UNKNOWN: frozenset(),
}

_KIND_BY_NAME = {kind.value: kind for kind in TransformKind}


@dataclasses.dataclass(frozen=True, slots=True)
class Transform:
    """One partition or sort transform, parsed from a role declaration."""

    kind: TransformKind = TransformKind.IDENTITY
    parameter: int | None = None
    text: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.kind, TransformKind):
            raise TypeError("kind must be a TransformKind")
        if self.kind.is_parameterized:
            if type(self.parameter) is not int or self.parameter < 1:
                raise ValueError(
                    f"{self.kind.value} requires a positive integer parameter"
                )
        elif self.parameter is not None:
            raise ValueError(f"{self.kind.value} takes no parameter")
        if not isinstance(self.text, str):
            raise TypeError("text must be a string")
        # Only the placeholder carries its source, so two spellings of the same
        # recognized transform stay equal and keep hashing alike.
        if self.text and self.kind is not TransformKind.UNKNOWN:
            raise ValueError("only unknown transforms carry their source text")

    @classmethod
    def parse(cls, text: str | bytes | bytearray | memoryview | None) -> Transform:
        """Parse ``name`` or ``name[parameter]``, never raising on a bad name.

        An unrecognized name, a missing parameter, or a parameter on a
        transform that takes none all yield ``Transform(UNKNOWN, text=source)``.
        The module docstring explains why the rejection is deferred and why
        that is safe.  Names are matched case-insensitively, which accepts
        strictly more declarations than the previous parser did.
        """

        source = _decode(text)
        lowered = source.lower()
        match = _PARAMETERIZED.fullmatch(lowered)
        if match is not None:
            kind = _KIND_BY_NAME.get(match["name"])
            parameter = int(match["parameter"])
            if kind is not None and kind.is_parameterized and parameter >= 1:
                return cls(kind, parameter)
            return cls(TransformKind.UNKNOWN, text=source)
        kind = _KIND_BY_NAME.get(lowered)
        if kind is None or kind.is_parameterized or kind is TransformKind.UNKNOWN:
            return cls(TransformKind.UNKNOWN, text=source)
        return cls(kind)

    def __str__(self) -> str:
        """Return the declaration text :meth:`parse` reads back to ``self``."""

        if self.kind is TransformKind.UNKNOWN:
            return self.text
        if self.parameter is None:
            return self.kind.value
        return f"{self.kind.value}[{self.parameter}]"

    def accepts(self, source: DataType) -> bool:
        """Return whether the transform can be applied to ``source``.

        ``UNKNOWN`` accepts nothing, so a caller that asks before acting gets a
        false rather than an exception; every call that would act on the value
        raises instead.
        """

        accepted = _ACCEPTED[self.kind]
        return accepted is None or source.kind.name in accepted

    def result_type(self, source: DataType) -> DataType:
        """Return the data type the transform produces from ``source``."""

        from .datatypes import DataType, TypeKind

        self._reject_unknown()
        self._reject_source(source)
        if self.kind in {TransformKind.YEAR, TransformKind.MONTH, TransformKind.HOUR}:
            return DataType(TypeKind.INT32)
        if self.kind is TransformKind.DAY:
            # Iceberg stores days since the epoch; the date is the same value
            # in the representation partition paths and readers display.
            return DataType(TypeKind.DATE)
        if self.kind is TransformKind.BUCKET:
            return DataType(TypeKind.INT32)
        return source

    def apply(self, value: Any, source: DataType) -> Any:
        """Return the partition value ``value`` maps to under this transform.

        Results match what Iceberg stores: ``year`` and ``month`` count from
        1970, ``hour`` counts hours from the epoch, and ``day`` is the calendar
        date :meth:`result_type` announces.  ``bucket`` is not computed here --
        it hashes through murmur3 over Iceberg's single-value binary encoding,
        and a one-bit disagreement would write rows into the wrong partition
        with no error, so it is delegated to PyIceberg by the caller that owns
        the Iceberg type mapping.
        """

        self._reject_unknown()
        self._reject_source(source)
        if self.kind is TransformKind.VOID:
            return None
        if value is None or self.kind is TransformKind.IDENTITY:
            return value
        if self.kind is TransformKind.BUCKET:
            raise NotImplementedError(
                "bucket values are computed by PyIceberg; use into_iceberg()"
            )
        if self.kind is TransformKind.TRUNCATE:
            return _truncate(value, cast(int, self.parameter), source)
        return _time_part(self.kind, value, source)

    def into_iceberg(self, *, field: str = "") -> Any:
        """Return the equivalent PyIceberg transform."""

        from pyiceberg.transforms import (
            BucketTransform,
            DayTransform,
            HourTransform,
            IdentityTransform,
            MonthTransform,
            TruncateTransform,
            VoidTransform,
            YearTransform,
        )

        if self.kind is TransformKind.UNKNOWN:
            raise ValueError(
                f"invalid Iceberg transform {self.text!r} for field {field!r}"
            )
        if self.kind is TransformKind.BUCKET:
            return BucketTransform(num_buckets=cast(int, self.parameter))
        if self.kind is TransformKind.TRUNCATE:
            return TruncateTransform(width=cast(int, self.parameter))
        return {
            TransformKind.IDENTITY: IdentityTransform,
            TransformKind.YEAR: YearTransform,
            TransformKind.MONTH: MonthTransform,
            TransformKind.DAY: DayTransform,
            TransformKind.HOUR: HourTransform,
            TransformKind.VOID: VoidTransform,
        }[self.kind]()

    def into_glue(self, *, field: str = "") -> None:
        """Check that Glue can express the transform, raising when it cannot.

        Glue partition keys are stored column values with no transform, so a
        non-identity role used to degrade silently -- ``"day"`` on a timestamp
        produced one catalog partition per distinct microsecond.  Refusing is
        the only honest answer.
        """

        if self.kind is TransformKind.UNKNOWN:
            raise ValueError(f"invalid Glue transform {self.text!r} for field {field!r}")
        if self.kind is not TransformKind.IDENTITY:
            raise ValueError(
                f"Glue supports only identity partition keys; field {field!r} "
                f"requests {self!s}"
            )

    def _reject_unknown(self) -> None:
        if self.kind is TransformKind.UNKNOWN:
            raise ValueError(f"invalid transform {self.text!r}")

    def _reject_source(self, source: DataType) -> None:
        if not self.accepts(source):
            raise ValueError(
                f"transform {self!s} does not accept {source.kind.value} values"
            )


IDENTITY: Final = Transform()


@dataclasses.dataclass(frozen=True, slots=True)
class PartitionRole:
    """A resolved ``partition_key`` declaration."""

    transform: Transform = IDENTITY
    position: int | None = None
    name: str | None = None

    def __post_init__(self) -> None:
        _validate_spec(self.transform, self.position)
        if self.name is None:
            return
        if not isinstance(self.name, str) or not self.name:
            raise TypeError("name must be a non-empty string")
        # The name is written back after an " as " separator, so a name that
        # contains one would not survive its own round trip.
        if _AS.search(self.name):
            raise ValueError("name must not contain an 'as' separator")

    @classmethod
    def parse(
        cls,
        raw: str | bytes | bytearray | memoryview | None,
        *,
        field: str = "",
    ) -> PartitionRole | None:
        """Interpret one ``partition_key`` wire value.

        The grammar is a strict superset of what shipped before, so every
        stored value keeps parsing::

            absent | "" | false | no | null   -> None (not a partition key)
            true | yes                        -> identity, unordered
            <int>                             -> identity at that position
            <transform>                       -> that transform, unordered
            <int>:<transform>                 -> both
            <spec> as <name>                  -> an explicit column name

        ``"1"`` is **not** a boolean synonym.  It reached this parser only from
        ``field(partition_key=1)`` -- a real ``True`` encodes as ``b"true"`` --
        and reading it as "enabled, unordered" silently dropped the requested
        order while Glue read the same bytes as position 1.

        The last two forms are new: a position used to be unrepresentable
        alongside a transform, so ``partition_key="day"`` could never be
        ordered.
        """

        source = _decode(raw, field=field)
        if source.lower() in _DISABLED:
            return None
        spec, name = _split_name(source)
        position, transform = _parse_spec(spec)
        if transform.kind is TransformKind.UNKNOWN:
            # Report the whole declaration, not the fragment that failed.
            return cls(Transform(TransformKind.UNKNOWN, text=source))
        return cls(transform, position, name)

    def __str__(self) -> str:
        """Return the declaration text :meth:`parse` reads back to ``self``."""

        spec = _spec_text(self.transform, self.position)
        return spec if self.name is None else f"{spec} as {self.name}"

    def encode(self) -> bytes:
        """Return the shortest wire value that parses back to this role."""

        return str(self).encode("utf-8")


@dataclasses.dataclass(frozen=True, slots=True)
class IndexRole:
    """A resolved ``index_key`` declaration."""

    transform: Transform = IDENTITY
    position: int | None = None
    descending: bool = False
    nulls_first: bool = False

    def __post_init__(self) -> None:
        _validate_spec(self.transform, self.position)
        if type(self.descending) is not bool or type(self.nulls_first) is not bool:
            raise TypeError("descending and nulls_first must be booleans")

    @classmethod
    def parse(
        cls,
        raw: str | bytes | bytearray | memoryview | None,
        *,
        field: str = "",
    ) -> IndexRole | None:
        """Interpret one ``index_key`` wire value.

        The spec grammar is :meth:`PartitionRole.parse`'s, minus the column
        name -- a sort field has none -- plus optional ordering words::

            <spec> [asc | desc] [nulls first | nulls last]

        The defaults, ascending with nulls last, are the order projected
        before this grammar existed.  An unreadable ordering word degrades to
        an unknown transform rather than raising, for the same reason a bad
        transform name does.
        """

        source = _decode(raw, field=field)
        if source.lower() in _DISABLED:
            return None
        tokens = source.split()
        descending = False
        nulls_first = False
        readable = True
        cursor = 1
        while cursor < len(tokens):
            word = tokens[cursor].lower()
            following = tokens[cursor + 1].lower() if cursor + 1 < len(tokens) else ""
            if word in _DIRECTIONS:
                descending = _DIRECTIONS[word]
                cursor += 1
            elif word == "nulls" and following in _NULL_ORDERS:
                nulls_first = _NULL_ORDERS[following]
                cursor += 2
            else:
                readable = False
                break
        position, transform = _parse_spec(tokens[0])
        if not readable or transform.kind is TransformKind.UNKNOWN:
            return cls(Transform(TransformKind.UNKNOWN, text=source))
        return cls(transform, position, descending, nulls_first)

    def __str__(self) -> str:
        """Return the declaration text :meth:`parse` reads back to ``self``."""

        text = _spec_text(self.transform, self.position)
        if self.descending:
            text += " desc"
        if self.nulls_first:
            text += " nulls first"
        return text

    def encode(self) -> bytes:
        """Return the shortest wire value that parses back to this role."""

        return str(self).encode("utf-8")


def _decode(
    value: str | bytes | bytearray | memoryview | None,
    *,
    field: str = "",
) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).decode("utf-8", "replace").strip()
    raise TypeError(f"role values must be text or bytes{_at(field)}")


def _at(field: str) -> str:
    return f" for field {field!r}" if field else ""


def _split_name(source: str) -> tuple[str, str | None]:
    matches = list(_AS.finditer(source))
    if not matches:
        return source, None
    separator = matches[-1]
    name = source[separator.end() :].strip()
    if not name:
        return source, None
    return source[: separator.start()].strip(), name


def _parse_spec(spec: str) -> tuple[int | None, Transform]:
    if spec.lower() in _ENABLED:
        return None, IDENTITY
    if _POSITION.fullmatch(spec):
        return int(spec), IDENTITY
    position, separator, transform = spec.partition(":")
    if separator and _POSITION.fullmatch(position):
        return int(position), Transform.parse(transform)
    return None, Transform.parse(spec)


def _spec_text(transform: Transform, position: int | None) -> str:
    identity = transform == IDENTITY
    if position is None:
        return "true" if identity else str(transform)
    if identity:
        return str(position)
    return f"{position}:{transform}"


def _validate_spec(transform: Transform, position: int | None) -> None:
    if not isinstance(transform, Transform):
        raise TypeError("transform must be a Transform")
    if position is not None and type(position) is not int:
        raise TypeError("position must be an integer")


def _truncate(value: Any, width: int, source: DataType) -> Any:
    if source.kind.name == "DECIMAL":
        if not isinstance(value, Decimal):
            raise TypeError("decimal truncation requires a Decimal value")
        scale = source.scale or 0
        with localcontext() as context:
            context.prec = _DECIMAL_PRECISION
            unscaled = int(value.scaleb(scale))
            return Decimal(unscaled - unscaled % width).scaleb(-scale)
    if source.kind.name in {"INT32", "INT64"}:
        if not isinstance(value, int) or isinstance(value, bool):
            raise TypeError("integer truncation requires an integer value")
        return value - value % width
    if not isinstance(value, (str, bytes, bytearray, memoryview)):
        raise TypeError("prefix truncation requires text or bytes")
    if isinstance(value, (bytearray, memoryview)):
        value = bytes(value)
    return value[:width]


def _time_part(kind: TransformKind, value: Any, source: DataType) -> Any:
    moment = _moment(value, source)
    if kind is TransformKind.DAY:
        return moment.date() if isinstance(moment, dt.datetime) else moment
    if kind is TransformKind.YEAR:
        return moment.year - _EPOCH_YEAR
    if kind is TransformKind.MONTH:
        return (moment.year - _EPOCH_YEAR) * 12 + moment.month - 1
    return (moment - _EPOCH_TIME) // dt.timedelta(hours=1)


def _moment(value: Any, source: DataType) -> dt.date:
    if source.kind.name == "DATE":
        if isinstance(value, dt.datetime):
            raise TypeError("a date column cannot hold a datetime value")
        if isinstance(value, dt.date):
            return value
        if isinstance(value, int) and not isinstance(value, bool):
            return _EPOCH_DATE + dt.timedelta(days=value)
        raise TypeError("date transforms require a date or a day count")
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            return value
        # A UTC-adjusted column stores one instant, so the calendar parts must
        # be read in UTC whatever zone the caller handed in.
        return value.astimezone(dt.timezone.utc).replace(tzinfo=None)
    if isinstance(value, int) and not isinstance(value, bool):
        unit = source.unit or "us"
        # Sub-microsecond precision cannot change a year, month, day, or hour.
        micros = value // 1_000 if unit == "ns" else value * _MICROS_PER_UNIT[unit]
        return _EPOCH_TIME + dt.timedelta(microseconds=micros)
    raise TypeError("timestamp transforms require a datetime or an epoch count")
