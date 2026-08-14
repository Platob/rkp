"""Small, safe, single-document YAML 1.2 parser."""

from __future__ import annotations

import base64
import binascii
import re
from collections.abc import Sequence
from itertools import pairwise
from typing import Any

from ._scalars import decode_quoted, parse_plain_scalar

__all__ = ["parse"]

_BLOCK_HEADER = re.compile(
    r"^(?:(?P<tag>!!(?:binary|str))\s+)?(?P<style>[|>])"
    r"(?P<modifiers>(?:[+-]?[1-9]?|[1-9][+-]?))?\s*$"
)
_SAFE_SCALAR_TAGS = {"!!str", "!!int", "!!float", "!!bool", "!!null", "!!binary"}


def parse(text: str) -> Any:
    """Parse exactly one safe YAML document into ordinary Python values."""

    return _Parser(text).parse()


class _Parser:
    def __init__(self, text: str) -> None:
        if not isinstance(text, str):
            raise TypeError("YAML source must be text")
        normalized = text.replace("\r\n", "\n").replace("\r", "\n")
        normalized = normalized.removeprefix("\ufeff")
        if "\0" in normalized:
            raise ValueError("NUL characters are not allowed in YAML")
        for character in normalized:
            codepoint = ord(character)
            if not _is_yaml_printable(codepoint):
                raise ValueError("forbidden control character in YAML input")
        self.lines = normalized.split("\n")
        self.ignored: set[int] = set()
        self.overrides: dict[int, str] = {}
        self.end = len(self.lines)
        self._prepare_document()

    def parse(self) -> Any:
        first = self._next_significant(0)
        if first is None:
            return None
        index, indent, _content = first
        value, following = self._parse_node(index, indent)
        trailing = self._next_significant(following)
        if trailing is not None:
            line, trailing_indent, _ = trailing
            if trailing_indent:
                raise ValueError(f"unexpected indentation at line {line + 1}")
            raise ValueError(f"unexpected YAML content at line {line + 1}")
        return value

    def _prepare_document(self) -> None:
        significant: list[tuple[int, int, str]] = []
        for index, raw in enumerate(self.lines):
            prefix = raw[: len(raw) - len(raw.lstrip(" \t"))]
            if "\t" in prefix:
                raise ValueError(
                    f"tabs cannot be used for indentation at line {index + 1}"
                )
            content = _strip_comment(raw[len(prefix) :]).strip()
            if content:
                significant.append((index, len(prefix), content))

        cursor = 0
        if significant and significant[0][2].startswith("%"):
            while cursor < len(significant) and significant[cursor][2].startswith("%"):
                directive = significant[cursor][2]
                if directive != "%YAML 1.2":
                    raise ValueError(
                        f"unsupported YAML directive at line {significant[cursor][0] + 1}"
                    )
                self.ignored.add(significant[cursor][0])
                cursor += 1
        if cursor < len(significant):
            marker_line, marker_indent, marker_content = significant[cursor]
            if marker_indent == 0 and marker_content == "---":
                self.ignored.add(marker_line)
                cursor += 1
            elif marker_indent == 0 and marker_content.startswith("--- "):
                self.overrides[marker_line] = marker_content[4:].lstrip()
                cursor += 1

        ended = False
        for index, indent, content in significant[cursor:]:
            if indent == 0 and content == "---":
                raise ValueError(
                    f"multiple YAML documents are not supported (line {index + 1})"
                )
            if indent == 0 and content == "...":
                if ended:
                    raise ValueError(f"unexpected document end at line {index + 1}")
                ended = True
                self.ignored.add(index)
                self.end = min(self.end, index)
                continue
            if ended:
                raise ValueError(f"content after document end at line {index + 1}")

    def _parse_node(self, index: int, indent: int) -> tuple[Any, int]:
        found = self._next_significant(index)
        if found is None:
            return None, self.end
        index, actual, content = found
        if actual != indent:
            raise ValueError(f"unexpected indentation at line {index + 1}")
        if _is_sequence_entry(content):
            return self._parse_sequence(index, indent)
        if _split_mapping_entry(content) is not None:
            return self._parse_mapping(index, indent)
        value, following = self._parse_inline_or_block(content, index, indent)
        return value, following

    def _parse_mapping(self, index: int, indent: int) -> tuple[dict[Any, Any], int]:
        result: dict[Any, Any] = {}
        cursor = index
        while True:
            found = self._next_significant(cursor)
            if found is None:
                return result, self.end
            line, actual, content = found
            if actual < indent:
                return result, line
            if actual > indent:
                raise ValueError(f"unexpected indentation at line {line + 1}")
            if _is_sequence_entry(content):
                return result, line
            entry = _split_mapping_entry(content)
            if entry is None:
                return result, line
            key_text, value_text = entry
            key = self._parse_key(key_text, line)
            value, cursor = self._parse_mapping_value(value_text, line, indent)
            _insert(result, key, value, line)

    def _parse_mapping_value(
        self, value_text: str, line: int, indent: int
    ) -> tuple[Any, int]:
        if value_text:
            return self._parse_inline_or_block(value_text, line, indent)
        following = self._next_significant(line + 1)
        if following is None or following[1] <= indent:
            return None, line + 1
        return self._parse_node(following[0], following[1])

    def _parse_sequence(self, index: int, indent: int) -> tuple[list[Any], int]:
        result: list[Any] = []
        cursor = index
        while True:
            found = self._next_significant(cursor)
            if found is None:
                return result, self.end
            line, actual, content = found
            if actual < indent:
                return result, line
            if actual > indent:
                raise ValueError(f"unexpected indentation at line {line + 1}")
            if not _is_sequence_entry(content):
                return result, line
            remainder = content[1:].lstrip()
            if not remainder:
                following = self._next_significant(line + 1)
                if following is None or following[1] <= indent:
                    result.append(None)
                    cursor = line + 1
                else:
                    value, cursor = self._parse_node(following[0], following[1])
                    result.append(value)
                continue

            entry = _split_mapping_entry(remainder)
            if entry is not None:
                value, cursor = self._parse_compact_mapping(line, indent, entry)
                result.append(value)
                continue
            if _is_sequence_entry(remainder):
                value, cursor = self._parse_compact_sequence(line, indent, remainder)
                result.append(value)
                continue
            value, cursor = self._parse_inline_or_block(remainder, line, indent)
            result.append(value)

    def _parse_compact_mapping(
        self,
        line: int,
        sequence_indent: int,
        first_entry: tuple[str, str],
    ) -> tuple[dict[Any, Any], int]:
        map_indent = sequence_indent + 2
        result: dict[Any, Any] = {}
        key_text, value_text = first_entry
        key = self._parse_key(key_text, line)
        value, cursor = self._parse_mapping_value(value_text, line, map_indent)
        _insert(result, key, value, line)

        while True:
            found = self._next_significant(cursor)
            if found is None:
                return result, self.end
            item_line, actual, content = found
            if actual <= sequence_indent:
                return result, item_line
            if actual != map_indent:
                raise ValueError(f"unexpected indentation at line {item_line + 1}")
            entry = _split_mapping_entry(content)
            if entry is None:
                raise ValueError(f"expected a mapping entry at line {item_line + 1}")
            key_text, value_text = entry
            key = self._parse_key(key_text, item_line)
            value, cursor = self._parse_mapping_value(value_text, item_line, map_indent)
            _insert(result, key, value, item_line)

    def _parse_compact_sequence(
        self, line: int, sequence_indent: int, first: str
    ) -> tuple[list[Any], int]:
        nested_indent = sequence_indent + 2
        result: list[Any] = []
        remainder = first[1:].lstrip()
        if remainder:
            value, cursor = self._parse_inline_or_block(remainder, line, nested_indent)
        else:
            value, cursor = None, line + 1
        result.append(value)
        while True:
            found = self._next_significant(cursor)
            if found is None:
                return result, self.end
            item_line, actual, content = found
            if actual <= sequence_indent:
                return result, item_line
            if actual != nested_indent or not _is_sequence_entry(content):
                raise ValueError(f"invalid nested sequence at line {item_line + 1}")
            remainder = content[1:].lstrip()
            if remainder:
                value, cursor = self._parse_inline_or_block(
                    remainder, item_line, nested_indent
                )
            else:
                value, cursor = None, item_line + 1
            result.append(value)

    def _parse_key(self, text: str, line: int) -> Any:
        if not text:
            raise ValueError(f"empty mapping key at line {line + 1}")
        value = _FlowParser(text, line=line + 1).parse_complete()
        try:
            hash(value)
        except TypeError as exc:
            raise ValueError(f"unhashable mapping key at line {line + 1}") from exc
        return value

    def _parse_inline_or_block(
        self, content: str, line: int, indent: int
    ) -> tuple[Any, int]:
        header = _BLOCK_HEADER.fullmatch(content)
        if header is not None:
            return self._parse_block_scalar(header, line, indent)
        if content.startswith(("[", "{")) and _flow_balance(content) > 0:
            parts = [content]
            cursor = line + 1
            balance = _flow_balance(content)
            while cursor < self.end and balance > 0:
                part = _strip_comment(self.lines[cursor].strip()).strip()
                if part:
                    parts.append(part)
                    balance += _flow_balance(part)
                cursor += 1
            if balance != 0:
                raise ValueError(f"unclosed flow collection at line {line + 1}")
            value = _FlowParser(" ".join(parts), line=line + 1).parse_complete()
            return value, cursor
        if content.startswith(("'", '"')) and not _quoted_scalar_closed(content):
            parts = [content]
            cursor = line + 1
            while cursor < self.end:
                part = self.lines[cursor].strip()
                parts.append(part)
                cursor += 1
                if _quoted_scalar_closed(" ".join(parts)):
                    value = _FlowParser(" ".join(parts), line=line + 1).parse_complete()
                    return value, cursor
            raise ValueError(f"unterminated quoted scalar at line {line + 1}")
        value = _FlowParser(content, line=line + 1).parse_complete()
        return value, line + 1

    def _parse_block_scalar(
        self, header: re.Match[str], line: int, parent_indent: int
    ) -> tuple[Any, int]:
        modifiers = header.group("modifiers") or ""
        chomping = "+" if "+" in modifiers else "-" if "-" in modifiers else ""
        digits = "".join(character for character in modifiers if character.isdigit())
        explicit_indent = parent_indent + int(digits) if digits else None

        cursor = line + 1
        content_indent = explicit_indent
        probe = cursor
        while probe < self.end:
            raw = self.lines[probe]
            if raw.strip() == "":
                probe += 1
                continue
            indentation = len(raw) - len(raw.lstrip(" "))
            if indentation <= parent_indent:
                break
            if content_indent is None:
                content_indent = indentation
            break
        if content_indent is None:
            content_indent = parent_indent + 1

        collected: list[str] = []
        while cursor < self.end:
            raw = self.lines[cursor]
            if raw.strip() == "":
                collected.append("")
                cursor += 1
                continue
            indentation = len(raw) - len(raw.lstrip(" "))
            if indentation <= parent_indent:
                break
            if indentation < content_indent:
                raise ValueError(
                    f"invalid block scalar indentation at line {cursor + 1}"
                )
            collected.append(raw[content_indent:])
            cursor += 1

        trailing = 0
        while collected and collected[-1] == "":
            collected.pop()
            trailing += 1
        if header.group("style") == "|":
            value = "\n".join(collected)
        else:
            value = _fold_lines(collected)
        if chomping == "+":
            synthetic_eof = cursor == self.end and self.lines[-1] == ""
            document_end = (
                cursor < len(self.lines) and self.lines[cursor].strip() == "..."
            )
            kept = trailing - (1 if synthetic_eof else 0) + (1 if collected else 0)
            if document_end:
                kept = trailing + (1 if collected else 0)
            value += "\n" * max(0, kept)
        elif chomping != "-" and collected:
            value += "\n"

        if header.group("tag") == "!!binary":
            return _decode_binary(value, line + 1), cursor
        return value, cursor

    def _next_significant(self, index: int) -> tuple[int, int, str] | None:
        while index < self.end:
            if index in self.ignored:
                index += 1
                continue
            raw = self.lines[index]
            stripped = raw.lstrip(" ")
            indent = len(raw) - len(stripped)
            content = self.overrides.get(index)
            if content is None:
                content = _strip_comment(stripped).rstrip()
            else:
                indent = 0
            if content.strip():
                return index, indent, content.strip()
            index += 1
        return None


class _FlowParser:
    def __init__(self, text: str, *, line: int) -> None:
        self.text = _strip_comment(text).strip()
        self.line = line
        self.index = 0

    def parse_complete(self) -> Any:
        value = self._parse_value(stop_at_colon=False, terminators="")
        self._skip_space()
        if self.index != len(self.text):
            raise ValueError(
                f"unexpected content {self.text[self.index :]!r} at line {self.line}"
            )
        return value

    def _parse_value(self, *, stop_at_colon: bool, terminators: str) -> Any:
        self._skip_space()
        if self.index >= len(self.text):
            return None
        character = self.text[self.index]
        if character == "[":
            return self._parse_sequence()
        if character == "{":
            return self._parse_mapping()
        if character in {'"', "'"}:
            value, self.index = decode_quoted(self.text, self.index, line=self.line)
            return value
        if character in "&*":
            kind = "anchor" if character == "&" else "alias"
            raise ValueError(f"YAML {kind} values are unsupported at line {self.line}")
        if character == "!":
            return self._parse_tagged(terminators)
        start = self.index
        while self.index < len(self.text):
            character = self.text[self.index]
            if character in terminators:
                break
            if (
                stop_at_colon
                and character == ":"
                and (
                    self.index + 1 == len(self.text)
                    or self.text[self.index + 1].isspace()
                    or self.text[self.index + 1] in "[],{}"
                )
            ):
                break
            self.index += 1
        token = self.text[start : self.index].strip()
        if not token:
            raise ValueError(f"expected a YAML value at line {self.line}")
        return parse_plain_scalar(token)

    def _parse_sequence(self) -> list[Any]:
        self.index += 1
        result: list[Any] = []
        self._skip_space()
        if self._consume("]"):
            return result
        while True:
            start = self.index
            value = self._parse_value(stop_at_colon=True, terminators=":,]")
            self._skip_space()
            if self.index < len(self.text) and self.text[self.index] == ":":
                self.index += 1
                self._skip_space()
                if self.index >= len(self.text) or self.text[self.index] in ",]":
                    mapped = None
                else:
                    mapped = self._parse_value(stop_at_colon=False, terminators=",]")
                try:
                    hash(value)
                except TypeError as exc:
                    raise ValueError(
                        f"unhashable mapping key at line {self.line}"
                    ) from exc
                result.append({value: mapped})
            else:
                if self.index == start:
                    raise ValueError(f"expected a YAML value at line {self.line}")
                result.append(value)
            self._skip_space()
            if self._consume("]"):
                return result
            self._expect(",")
            self._skip_space()
            if self._consume("]"):
                return result

    def _parse_mapping(self) -> dict[Any, Any]:
        self.index += 1
        result: dict[Any, Any] = {}
        self._skip_space()
        if self._consume("}"):
            return result
        while True:
            key = self._parse_flow_key()
            self._skip_space()
            if self.index >= len(self.text) or self.text[self.index] in ",}":
                value = None
            else:
                value = self._parse_value(stop_at_colon=False, terminators=",}")
            _insert(result, key, value, self.line - 1)
            self._skip_space()
            if self._consume("}"):
                return result
            self._expect(",")
            self._skip_space()
            if self._consume("}"):
                return result

    def _parse_flow_key(self) -> Any:
        self._skip_space()
        if self.index >= len(self.text):
            raise ValueError(f"expected a mapping key at line {self.line}")
        if self.text[self.index] in {'"', "'"}:
            key, self.index = decode_quoted(self.text, self.index, line=self.line)
            self._skip_space()
            self._expect(":")
            return key

        start = self.index
        quote: str | None = None
        escaped = False
        depth = 0
        while self.index < len(self.text):
            character = self.text[self.index]
            if quote == '"':
                if escaped:
                    escaped = False
                elif character == "\\":
                    escaped = True
                elif character == quote:
                    quote = None
            elif quote == "'":
                if character == quote:
                    quote = None
            elif character in {'"', "'"}:
                quote = character
            elif character in "[{":
                depth += 1
            elif character in "]}":
                if depth:
                    depth -= 1
                else:
                    break
            elif (
                character == ":"
                and depth == 0
                and (
                    self.index + 1 == len(self.text)
                    or self.text[self.index + 1].isspace()
                    or self.text[self.index + 1] in "[],{}"
                )
            ):
                raw = self.text[start : self.index].strip()
                if not raw:
                    raise ValueError(f"empty mapping key at line {self.line}")
                self.index += 1
                return parse_plain_scalar(raw)
            self.index += 1
        raise ValueError(f"expected ':' at line {self.line}, column {self.index + 1}")

    def _parse_tagged(self, terminators: str) -> Any:
        start = self.index
        while self.index < len(self.text) and not self.text[self.index].isspace():
            if self.text[self.index] in "[],{}":
                break
            self.index += 1
        tag = self.text[start : self.index]
        if tag not in _SAFE_SCALAR_TAGS:
            raise ValueError(f"unsupported YAML tag {tag!r} at line {self.line}")
        self._skip_space()
        if self.index >= len(self.text):
            raise ValueError(f"tag {tag!r} has no value at line {self.line}")
        if self.text[self.index] in "&*":
            raise ValueError(
                f"YAML anchors and aliases are unsupported at line {self.line}"
            )
        if self.text[self.index] in {'"', "'"}:
            value, self.index = decode_quoted(self.text, self.index, line=self.line)
        else:
            start = self.index
            while (
                self.index < len(self.text) and self.text[self.index] not in terminators
            ):
                self.index += 1
            raw = self.text[start : self.index].strip()
            value = raw if tag in {"!!str", "!!binary"} else parse_plain_scalar(raw)
        return _apply_tag(tag, value, self.line)

    def _skip_space(self) -> None:
        while self.index < len(self.text) and self.text[self.index] == " ":
            self.index += 1

    def _consume(self, character: str) -> bool:
        if self.index < len(self.text) and self.text[self.index] == character:
            self.index += 1
            return True
        return False

    def _expect(self, character: str) -> None:
        if not self._consume(character):
            raise ValueError(
                f"expected {character!r} at line {self.line}, column {self.index + 1}"
            )


def _apply_tag(tag: str, value: Any, line: int) -> Any:
    if tag == "!!str":
        return str(value)
    if tag == "!!binary":
        return _decode_binary(str(value), line)
    if (
        tag == "!!null"
        and isinstance(value, str)
        and (not value or value.lower() == "null" or value == "~")
    ):
        return None
    resolved = parse_plain_scalar(value) if isinstance(value, str) else value
    if tag == "!!int" and type(resolved) is int:
        return resolved
    if tag == "!!float" and type(resolved) in {int, float}:
        return float(resolved)
    if tag == "!!bool" and type(resolved) is bool:
        return resolved
    if tag == "!!null" and resolved is None:
        return None
    raise ValueError(f"value does not match {tag} at line {line}")


def _decode_binary(value: str, line: int) -> bytes:
    compact = "".join(value.split())
    try:
        return base64.b64decode(compact, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise ValueError(f"invalid !!binary value at line {line}") from exc


def _fold_lines(lines: Sequence[str]) -> str:
    if not lines:
        return ""
    output = lines[0]
    blank_run = 0
    for previous, current in pairwise(lines):
        if current == "":
            blank_run += 1
            continue
        if previous == "":
            output += "\n" * max(1, blank_run) + current
            blank_run = 0
        elif previous.startswith(" ") or current.startswith(" "):
            output += "\n" + current
        else:
            output += " " + current
    if blank_run:
        output += "\n" * blank_run
    return output


def _insert(mapping: dict[Any, Any], key: Any, value: Any, line: int) -> None:
    try:
        hash(key)
    except TypeError as exc:
        raise ValueError(f"unhashable mapping key at line {line + 1}") from exc
    if key in mapping:
        raise ValueError(f"duplicate mapping key {key!r} at line {line + 1}")
    mapping[key] = value


def _strip_comment(text: str) -> str:
    quote: str | None = None
    escaped = False
    index = 0
    while index < len(text):
        character = text[index]
        if quote == '"':
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == quote:
                quote = None
            index += 1
            continue
        elif quote == "'":
            if character == quote:
                if index + 1 < len(text) and text[index + 1] == quote:
                    index += 2
                    continue
                quote = None
            index += 1
            continue
        elif character in {'"', "'"}:
            quote = character
        elif character == "#" and (index == 0 or text[index - 1].isspace()):
            return text[:index]
        index += 1
    return text


def _split_mapping_entry(text: str) -> tuple[str, str] | None:
    quote: str | None = None
    escaped = False
    depth = 0
    index = 0
    while index < len(text):
        character = text[index]
        if quote == '"':
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == quote:
                quote = None
        elif quote == "'":
            if character == quote:
                if index + 1 < len(text) and text[index + 1] == quote:
                    index += 1
                else:
                    quote = None
        elif character in {'"', "'"}:
            quote = character
        elif character in "[{":
            depth += 1
        elif character in "]}":
            depth -= 1
        elif (
            character == ":"
            and depth == 0
            and (index + 1 == len(text) or text[index + 1].isspace())
        ):
            return text[:index].strip(), text[index + 1 :].strip()
        index += 1
    return None


def _is_sequence_entry(text: str) -> bool:
    return text == "-" or text.startswith("- ")


def _flow_balance(text: str) -> int:
    quote: str | None = None
    escaped = False
    balance = 0
    index = 0
    while index < len(text):
        character = text[index]
        if quote == '"':
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == quote:
                quote = None
        elif quote == "'":
            if character == quote:
                if index + 1 < len(text) and text[index + 1] == quote:
                    index += 1
                else:
                    quote = None
        elif character in {'"', "'"}:
            quote = character
        elif character in "[{":
            balance += 1
        elif character in "]}":
            balance -= 1
        index += 1
    return balance


def _quoted_scalar_closed(text: str) -> bool:
    if not text or text[0] not in {'"', "'"}:
        return True
    quote = text[0]
    escaped = False
    index = 1
    while index < len(text):
        character = text[index]
        if quote == '"':
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == quote:
                return True
        elif character == quote:
            if index + 1 < len(text) and text[index + 1] == quote:
                index += 1
            else:
                return True
        index += 1
    return False


def _is_yaml_printable(codepoint: int) -> bool:
    return (
        codepoint in {0x09, 0x0A, 0x0D, 0x85}
        or 0x20 <= codepoint <= 0x7E
        or 0xA0 <= codepoint <= 0xD7FF
        or 0xE000 <= codepoint <= 0xFFFD
        or 0x10000 <= codepoint <= 0x10FFFF
    )
