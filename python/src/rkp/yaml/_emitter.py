"""Fast block-style YAML emitter for normalized Python values."""

from __future__ import annotations

import base64
from collections.abc import Mapping, Sequence
from typing import Any

from ._scalars import render_scalar

__all__ = ["emit"]


def emit(
    value: Any,
    *,
    sort_keys: bool = False,
    indent: int = 2,
    explicit_start: bool = False,
    explicit_end: bool = False,
    line_break: str = "\n",
) -> str:
    """Emit one safe YAML document."""

    if type(sort_keys) is not bool:
        raise TypeError("sort_keys must be bool")
    if type(indent) is not int or not 1 <= indent <= 9:
        raise ValueError("indent must be an integer between 1 and 9")
    if type(explicit_start) is not bool or type(explicit_end) is not bool:
        raise TypeError("explicit_start and explicit_end must be bool")
    if line_break not in {"\n", "\r\n"}:
        raise ValueError("line_break must be '\\n' or '\\r\\n'")

    emitter = _Emitter(sort_keys=sort_keys, indent=indent)
    lines = emitter.render(value, level=0)
    if explicit_start:
        lines.insert(0, "---")
    if explicit_end:
        lines.append("...")
    return line_break.join(lines) + line_break


class _Emitter:
    def __init__(self, *, sort_keys: bool, indent: int) -> None:
        self.sort_keys = sort_keys
        self.indent = indent
        self.active: set[int] = set()

    def render(self, value: Any, *, level: int) -> list[str]:
        if isinstance(value, Mapping):
            return self._render_mapping(value, level=level)
        if isinstance(value, (list, tuple)):
            return self._render_sequence(value, level=level)
        prefix = " " * level
        return self._render_scalar_lines(value, prefix=prefix)

    def _render_mapping(self, value: Mapping[Any, Any], *, level: int) -> list[str]:
        if not value:
            return [" " * level + "{}"]
        self._enter(value)
        try:
            items = list(value.items())
            if self.sort_keys:
                items.sort(key=lambda item: (type(item[0]).__name__, repr(item[0])))
            lines: list[str] = []
            prefix = " " * level
            for key, item in items:
                rendered_key = self._render_key(key)
                inline = self._inline_scalar(item)
                if inline is not None:
                    lines.append(f"{prefix}{rendered_key}: {inline}")
                elif isinstance(item, Mapping) and not item:
                    lines.append(f"{prefix}{rendered_key}: {{}}")
                elif isinstance(item, (list, tuple)) and not item:
                    lines.append(f"{prefix}{rendered_key}: []")
                elif _is_container(item):
                    lines.append(f"{prefix}{rendered_key}:")
                    lines.extend(self.render(item, level=level + self.indent))
                else:
                    header, content = self._block_scalar(item)
                    lines.append(f"{prefix}{rendered_key}: {header}")
                    content_prefix = " " * (level + self.indent)
                    lines.extend(content_prefix + line for line in content)
            return lines
        finally:
            self.active.remove(id(value))

    def _render_sequence(self, value: Sequence[Any], *, level: int) -> list[str]:
        if not value:
            return [" " * level + "[]"]
        self._enter(value)
        try:
            lines: list[str] = []
            prefix = " " * level
            for item in value:
                inline = self._inline_scalar(item)
                if inline is not None:
                    lines.append(f"{prefix}- {inline}")
                elif isinstance(item, Mapping) and not item:
                    lines.append(f"{prefix}- {{}}")
                elif isinstance(item, (list, tuple)) and not item:
                    lines.append(f"{prefix}- []")
                elif _is_container(item):
                    lines.append(f"{prefix}-")
                    lines.extend(self.render(item, level=level + self.indent))
                else:
                    header, content = self._block_scalar(item)
                    lines.append(f"{prefix}- {header}")
                    content_prefix = " " * (level + self.indent)
                    lines.extend(content_prefix + line for line in content)
            return lines
        finally:
            self.active.remove(id(value))

    def _render_scalar_lines(self, value: Any, *, prefix: str) -> list[str]:
        inline = self._inline_scalar(value)
        if inline is not None:
            return [prefix + inline]
        header, content = self._block_scalar(value)
        return [
            prefix + header,
            *(prefix + " " * self.indent + line for line in content),
        ]

    def _render_key(self, value: Any) -> str:
        if isinstance(value, bytes):
            encoded = base64.b64encode(value).decode("ascii")
            return "!!binary " + (encoded if encoded else '""')
        rendered = render_scalar(value, key=True)
        if rendered is None:
            raise TypeError("multiline YAML mapping keys are not supported")
        return rendered

    def _inline_scalar(self, value: Any) -> str | None:
        if isinstance(value, bytes):
            encoded = base64.b64encode(value).decode("ascii")
            return "!!binary " + (encoded if encoded else '""')
        if _is_container(value):
            return None
        return render_scalar(value)

    def _block_scalar(self, value: Any) -> tuple[str, list[str]]:
        if not isinstance(value, str):
            raise TypeError(f"unsupported YAML value type: {type(value).__qualname__}")
        normalized = value.replace("\r\n", "\n").replace("\r", "\n")
        trailing = len(normalized) - len(normalized.rstrip("\n"))
        if trailing == 0:
            header = f"|{self.indent}-"
            body = normalized
        elif trailing == 1:
            header = f"|{self.indent}"
            body = normalized[:-1]
        else:
            header = f"|{self.indent}+"
            body = normalized[:-1]
        return header, body.split("\n")

    def _enter(self, value: Any) -> None:
        identity = id(value)
        if identity in self.active:
            raise ValueError("cyclic value while emitting YAML")
        self.active.add(identity)


def _is_container(value: Any) -> bool:
    return isinstance(value, (Mapping, list, tuple))
