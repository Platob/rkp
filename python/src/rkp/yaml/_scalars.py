"""YAML 1.2 core scalar resolution and rendering."""

from __future__ import annotations

import json
import math
import re
from typing import Any

__all__ = [
    "decode_quoted",
    "parse_plain_scalar",
    "render_scalar",
]

_INTEGER = re.compile(r"^[+-]?(?:0|[1-9][0-9_]*|0o[0-7_]+|0x[0-9a-fA-F_]+)$")
_FLOAT = re.compile(
    r"^[+-]?(?:(?:[0-9][0-9_]*)?\.[0-9_]+(?:[eE][+-]?[0-9]+)?"
    r"|[0-9][0-9_]*\.[0-9_]*(?:[eE][+-]?[0-9]+)?"
    r"|[0-9][0-9_]*(?:[eE][+-]?[0-9]+)"
    r"|\.(?:inf|Inf|INF|nan|NaN|NAN))$"
)
_PLAIN_INDICATORS = "-?:,[]{}#&*!|>'\"%@`"
_HEX = frozenset("0123456789abcdefABCDEF")

_ESCAPES = {
    "0": "\0",
    "a": "\a",
    "b": "\b",
    "t": "\t",
    "\t": "\t",
    "n": "\n",
    "v": "\v",
    "f": "\f",
    "r": "\r",
    "e": "\x1b",
    " ": " ",
    '"': '"',
    "/": "/",
    "\\": "\\",
    "N": "\x85",
    "_": "\xa0",
    "L": "\u2028",
    "P": "\u2029",
}


def parse_plain_scalar(text: str) -> Any:
    """Resolve an unquoted scalar using the YAML 1.2 core schema."""

    value = text.strip()
    lowered = value.lower()
    if value == "~" or lowered == "null":
        return None
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if _INTEGER.fullmatch(value):
        cleaned = value.replace("_", "")
        sign = -1 if cleaned.startswith("-") else 1
        unsigned = cleaned.lstrip("+-")
        try:
            if unsigned.startswith("0o"):
                return sign * int(unsigned, 0)
            if unsigned.startswith("0x"):
                return sign * int(unsigned, 0)
            return int(cleaned, 10)
        except ValueError:
            return value
    if _FLOAT.fullmatch(value):
        cleaned = value.replace("_", "")
        lowered_cleaned = cleaned.lower()
        if lowered_cleaned in {".inf", "+.inf"}:
            return math.inf
        if lowered_cleaned == "-.inf":
            return -math.inf
        if lowered_cleaned in {".nan", "+.nan", "-.nan"}:
            return math.nan
        try:
            return float(cleaned)
        except ValueError:
            return value
    return value


def decode_quoted(
    text: str,
    start: int = 0,
    *,
    line: int = 1,
) -> tuple[str, int]:
    """Decode one single- or double-quoted scalar.

    The returned index points immediately after the closing quote.
    """

    if start >= len(text) or text[start] not in {'"', "'"}:
        raise ValueError(f"expected a quoted scalar at line {line}")
    quote = text[start]
    index = start + 1
    output: list[str] = []
    while index < len(text):
        character = text[index]
        if character == quote:
            if quote == "'" and index + 1 < len(text) and text[index + 1] == "'":
                output.append("'")
                index += 2
                continue
            return "".join(output), index + 1
        if quote == '"' and character == "\\":
            index += 1
            if index >= len(text):
                raise ValueError(f"unterminated escape at line {line}")
            escape = text[index]
            if escape in _ESCAPES:
                output.append(_ESCAPES[escape])
                index += 1
                continue
            lengths = {"x": 2, "u": 4, "U": 8}
            length = lengths.get(escape)
            if length is None:
                raise ValueError(f"unsupported YAML escape \\{escape} at line {line}")
            digits = text[index + 1 : index + 1 + length]
            if len(digits) != length or any(item not in _HEX for item in digits):
                raise ValueError(f"invalid Unicode escape at line {line}")
            try:
                codepoint = int(digits, 16)
                if escape == "u" and 0xD800 <= codepoint <= 0xDBFF:
                    suffix_start = index + length + 1
                    suffix = text[suffix_start : suffix_start + 2]
                    low_digits = text[suffix_start + 2 : suffix_start + 6]
                    if (
                        suffix != "\\u"
                        or len(low_digits) != 4
                        or any(item not in _HEX for item in low_digits)
                    ):
                        raise ValueError
                    low = int(low_digits, 16)
                    if not 0xDC00 <= low <= 0xDFFF:
                        raise ValueError
                    codepoint = 0x10000 + ((codepoint - 0xD800) << 10) + (low - 0xDC00)
                    index += 6
                elif 0xD800 <= codepoint <= 0xDFFF or codepoint > 0x10FFFF:
                    raise ValueError
                output.append(chr(codepoint))
            except ValueError as exc:
                raise ValueError(f"invalid Unicode escape at line {line}") from exc
            index += length + 1
            continue
        if character in "\r\n":
            raise ValueError(f"quoted scalar cannot span lines at line {line}")
        output.append(character)
        index += 1
    raise ValueError(f"unterminated quoted scalar at line {line}")


def render_scalar(value: Any, *, key: bool = False) -> str | None:
    """Render a scalar on one line, or return ``None`` for block text."""

    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if math.isnan(value):
            return ".nan"
        if math.isinf(value):
            return "-.inf" if value < 0 else ".inf"
        return repr(value)
    if not isinstance(value, str):
        raise TypeError(f"unsupported YAML scalar type: {type(value).__qualname__}")
    if any(0xD800 <= ord(character) <= 0xDFFF for character in value):
        raise ValueError("YAML strings cannot contain lone Unicode surrogates")
    if value in {"---", "..."}:
        return _double_quote(value)
    if any(_must_escape(character) for character in value):
        return _double_quote(value)
    if "\n" in value:
        lines = value.split("\n")
        if (
            key
            or not value.strip("\n")
            or any(line.endswith((" ", "\t")) for line in lines)
        ):
            return _double_quote(value)
        return None
    if _plain_string_is_safe(value, key=key):
        return value
    return _double_quote(value)


def _plain_string_is_safe(value: str, *, key: bool) -> bool:
    if not value or value != value.strip():
        return False
    if value[0] in _PLAIN_INDICATORS:
        return False
    if '"' in value or "'" in value:
        return False
    if value.endswith(":") or any(
        character == ":" and value[index + 1].isspace()
        for index, character in enumerate(value[:-1])
    ):
        return False
    if "\t" in value or re.search(r"\s#", value):
        return False
    if any(ord(character) < 0x20 and character != "\t" for character in value):
        return False
    if parse_plain_scalar(value) != value:
        return False
    return not (key and any(character in value for character in "{}[],"))


def _must_escape(character: str) -> bool:
    codepoint = ord(character)
    return (
        character in {"\r", "\t"}
        or (codepoint < 0x20 and character != "\n")
        or 0x7F <= codepoint <= 0x9F
        or 0xD800 <= codepoint <= 0xDFFF
        or codepoint in {0x2028, 0x2029, 0xFFFE, 0xFFFF}
    )


def _double_quote(value: str) -> str:
    return json.dumps(
        value,
        ensure_ascii=any(_must_escape(character) for character in value),
    )
