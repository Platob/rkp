"""Small standard-library parsers for classic and modern OnixS pages."""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field
from html.parser import HTMLParser
from urllib.parse import unquote, urljoin

from ._errors import FixParseError
from ._models import (
    FixComponent,
    FixComponentMember,
    FixEnumValue,
    FixField,
    FixFieldMember,
    FixMessage,
    FixRepeatingGroup,
    FixStructureMember,
)

__all__: list[str] = []

_PARSER_VERSION = 3
_TAG_HREF = re.compile(r"(?:^|/)tagNum_(\d+)\.html(?:[?#].*)?$", re.IGNORECASE)
_MESSAGE_HREF = re.compile(
    r"(?:^|/)msgType_([^/?#]+?)_[^/?#]+\.html(?:[?#].*)?$", re.IGNORECASE
)
_COMPONENT_HREF = re.compile(
    r"(?:^|/)compBlock_([^/?#]+)\.html(?:[?#].*)?$", re.IGNORECASE
)
_FIELD_HEADING = re.compile(
    r"(?:^.*?:\s*)?(.+?)\s*<\s*(\d+)\s*>\s*field\b", re.IGNORECASE
)
_MESSAGE_HEADING = re.compile(
    r"(?:^.*?:\s*)?(.+?)\s*<\s*([^<>]+?)\s*>\s*message\b", re.IGNORECASE
)
_COMPONENT_HEADING = re.compile(
    r"(?:^.*?:\s*)?<?(.+?)>?\s+component(?:\s+block)?\b", re.IGNORECASE
)
_TYPE = re.compile(r"\bType\s*:\s*([A-Za-z][A-Za-z0-9]*)\b", re.IGNORECASE)
_ENUM = re.compile(r"^\s*(\S.*?)\s*=\s*(\S.*)\s*$", re.DOTALL)
_STATUS = re.compile(r"\s*\((deprecated|replaced|no longer used)\)\s*$", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class FixFieldRef:
    """A field identity discovered in an OnixS field index."""

    tag: int
    name: str
    url: str
    version: str
    status: str | None = None


@dataclass(frozen=True, slots=True)
class FixMessageRef:
    """A message identity discovered in an OnixS message index."""

    msg_type: str
    name: str
    url: str
    version: str


@dataclass(frozen=True, slots=True)
class FixComponentRef:
    """A component identity discovered in an OnixS component link/index."""

    name: str
    url: str
    version: str


@dataclass(slots=True)
class _Capture:
    tag: str
    section: str | None
    parts: list[str]


@dataclass(frozen=True, slots=True)
class _RawAnchor:
    href: str
    text: str


@dataclass(slots=True)
class _CellCapture:
    tag: str
    colspan: int
    parts: list[str] = field(default_factory=list)
    anchors: list[_RawAnchor] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class _RawCell:
    tag: str
    text: str
    colspan: int
    anchors: tuple[_RawAnchor, ...]


@dataclass(slots=True)
class _RowCapture:
    table_id: int
    cells: list[_RawCell] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class _RawRow:
    table_id: int
    cells: tuple[_RawCell, ...]


@dataclass(slots=True)
class _MemberNode:
    depth: int
    kind: str
    tag: int | None
    name: str | None
    required: bool
    comment: str
    children: list[_MemberNode] = field(default_factory=list)


class _OnixsParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.anchors: list[tuple[str, str]] = []
        self.blocks: list[tuple[str, str, str | None]] = []
        self.rows: list[_RawRow] = []
        self.canonical: str | None = None
        self._href: str | None = None
        self._anchor_parts: list[str] = []
        self._anchor_cell: _CellCapture | None = None
        self._captures: list[_Capture] = []
        self._section: str | None = None
        self._table_counter = 0
        self._tables: list[int] = []
        self._rows: list[_RowCapture] = []
        self._cells: list[_CellCapture] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        lowered = tag.casefold()
        values = {key.casefold(): value for key, value in attrs}
        relations = (values.get("rel") or "").casefold().split()
        if lowered == "link" and "canonical" in relations:
            self.canonical = values.get("href")
        if lowered == "table":
            self._table_counter += 1
            self._tables.append(self._table_counter)
        elif lowered == "tr" and self._tables:
            self._rows.append(_RowCapture(self._tables[-1]))
        elif lowered in {"td", "th"} and self._rows:
            raw_colspan = values.get("colspan") or "1"
            try:
                colspan = max(1, int(raw_colspan))
            except ValueError:
                colspan = 1
            self._cells.append(_CellCapture(lowered, colspan))
        if lowered == "a":
            self._href = values.get("href")
            self._anchor_parts = []
            self._anchor_cell = self._cells[-1] if self._cells else None
        if lowered in {"h1", "h2", "h3", "p", "li", "title"}:
            self._captures.append(_Capture(lowered, self._section, []))
        if lowered == "br":
            if self._cells:
                self._cells[-1].parts.append(" ")
            for capture in self._captures:
                capture.parts.append(" ")

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.handle_starttag(tag, attrs)
        self.handle_endtag(tag)

    def handle_data(self, data: str) -> None:
        if self._href is not None:
            self._anchor_parts.append(data)
        if self._cells:
            self._cells[-1].parts.append(data)
        for capture in self._captures:
            capture.parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        lowered = tag.casefold()
        if lowered == "a" and self._href is not None:
            text = _space("".join(self._anchor_parts))
            anchor = _RawAnchor(self._href, text)
            self.anchors.append((anchor.href, anchor.text))
            if self._anchor_cell is not None:
                self._anchor_cell.anchors.append(anchor)
            self._href = None
            self._anchor_parts = []
            self._anchor_cell = None
        if lowered in {"td", "th"} and self._cells:
            cell_capture = self._cells.pop()
            cell = _RawCell(
                cell_capture.tag,
                _space("".join(cell_capture.parts)),
                cell_capture.colspan,
                tuple(cell_capture.anchors),
            )
            if self._rows:
                self._rows[-1].cells.append(cell)
        elif lowered == "tr" and self._rows:
            row_capture = self._rows.pop()
            if row_capture.cells:
                self.rows.append(
                    _RawRow(row_capture.table_id, tuple(row_capture.cells))
                )
        elif lowered == "table" and self._tables:
            self._tables.pop()
        for index in range(len(self._captures) - 1, -1, -1):
            block_capture = self._captures[index]
            if block_capture.tag != lowered:
                continue
            del self._captures[index]
            text = _space("".join(block_capture.parts))
            if text:
                self.blocks.append((lowered, text, block_capture.section))
                if lowered == "h3":
                    self._section = text.casefold().replace(" ", "")
            break


def parse_field_index(
    body: bytes,
    *,
    page_url: str,
    version: str,
    encoding: str = "utf-8",
) -> tuple[tuple[FixFieldRef, ...], str | None]:
    parser = _parse(body, page_url, encoding)
    grouped: dict[int, list[tuple[str, str]]] = defaultdict(list)
    for href, text in parser.anchors:
        match = _TAG_HREF.search(href)
        if match:
            grouped[int(match.group(1))].append((href, text))
    if not grouped:
        raise FixParseError(f"{page_url}: no FIX field links found")

    results: list[FixFieldRef] = []
    for tag, links in grouped.items():
        names: list[tuple[str, str | None]] = []
        for _, text in links:
            if not text or text.isdecimal():
                continue
            status_match = _STATUS.search(text)
            status = status_match.group(1) if status_match else None
            name = _STATUS.sub("", text).strip()
            if name:
                names.append((name, status))
        unique_names = {name for name, _ in names}
        if len(unique_names) != 1:
            raise FixParseError(
                f"{page_url}: FIX tag {tag} has conflicting or missing names"
            )
        selected_name = next(iter(unique_names))
        statuses = {
            status for name, status in names if name == selected_name and status
        }
        status = next(iter(statuses)) if len(statuses) == 1 else None
        href = next(href for href, _ in links)
        results.append(
            FixFieldRef(tag, selected_name, urljoin(page_url, href), version, status)
        )
    return tuple(sorted(results, key=lambda item: item.tag)), parser.canonical


def parse_message_index(
    body: bytes,
    *,
    page_url: str,
    version: str,
    encoding: str = "utf-8",
) -> tuple[tuple[FixMessageRef, ...], str | None]:
    """Parse classic or modern message indexes without deriving detail URLs."""

    parser = _parse(body, page_url, encoding)
    headers = _header_maps(parser.rows)
    found: dict[str, FixMessageRef] = {}
    for row in parser.rows:
        mapping = headers.get(row.table_id)
        if not mapping or "msgtype" not in mapping or "name" not in mapping:
            continue
        if all(cell.tag == "th" for cell in row.cells):
            continue
        msg_type = _cell_at(row, mapping["msgtype"])
        name = _cell_at(row, mapping["name"])
        anchors = [
            anchor
            for cell in row.cells
            for anchor in cell.anchors
            if _MESSAGE_HREF.search(anchor.href)
        ]
        if not anchors:
            continue
        if not msg_type or not name:
            raise FixParseError(f"{page_url}: message row has no MsgType or name")
        href_match = _MESSAGE_HREF.search(anchors[0].href)
        if href_match is None:  # pragma: no cover - guarded by anchor filtering
            raise RuntimeError("message-link parser invariant failed")
        href_type = unquote(href_match.group(1))
        if href_type != msg_type:
            raise FixParseError(
                f"{page_url}: MsgType {msg_type!r} conflicts with detail URL "
                f"MsgType {href_type!r}"
            )
        selected = FixMessageRef(
            msg_type,
            name,
            urljoin(page_url, anchors[0].href),
            version,
        )
        previous = found.get(msg_type)
        if previous is not None and previous != selected:
            raise FixParseError(f"{page_url}: conflicting FIX MsgType {msg_type!r}")
        found[msg_type] = selected
    if not found:
        raise FixParseError(f"{page_url}: no FIX message links found")
    return tuple(
        sorted(found.values(), key=lambda item: item.msg_type)
    ), parser.canonical


def parse_component_index(
    body: bytes,
    *,
    page_url: str,
    version: str,
    encoding: str = "utf-8",
) -> tuple[tuple[FixComponentRef, ...], str | None]:
    """Parse a modern component index (or a classic component-link page)."""

    parser = _parse(body, page_url, encoding)
    found: dict[str, FixComponentRef] = {}
    for href, _ in parser.anchors:
        name = _component_name(href)
        if name is None:
            continue
        selected = FixComponentRef(name, urljoin(page_url, href), version)
        key = name.casefold()
        previous = found.get(key)
        if previous is not None and previous.url != selected.url:
            raise FixParseError(f"{page_url}: conflicting component {name!r}")
        found[key] = selected
    if not found:
        raise FixParseError(f"{page_url}: no FIX component links found")
    return tuple(
        sorted(found.values(), key=lambda item: item.name.casefold())
    ), parser.canonical


def parse_field_detail(
    body: bytes,
    *,
    reference: FixFieldRef,
    encoding: str = "utf-8",
) -> FixField:
    parser = _parse(body, reference.url, encoding)
    identity: tuple[str, int] | None = None
    for tag, text, _ in parser.blocks:
        if tag not in {"h1", "h2", "title"}:
            continue
        match = _FIELD_HEADING.search(text)
        if match:
            identity = (_space(match.group(1)), int(match.group(2)))
            break
    if identity is None:
        raise FixParseError(f"{reference.url}: missing FIX field heading")
    name, tag_number = identity
    if tag_number != reference.tag:
        raise FixParseError(
            f"{reference.url}: expected tag {reference.tag}, found {tag_number}"
        )
    if name.casefold() != reference.name.casefold():
        raise FixParseError(
            f"{reference.url}: expected field {reference.name!r}, found {name!r}"
        )
    _validate_page_version(parser, reference.version, reference.url)

    fix_type: str | None = None
    for _, text, _ in parser.blocks:
        match = _TYPE.search(text)
        if match:
            fix_type = match.group(1)
            break
    if fix_type is None:
        raise FixParseError(f"{reference.url}: missing FIX field type")

    description: list[str] = []
    values: list[FixEnumValue] = []
    enum_started = False
    seen_codes: set[str] = set()
    for block_tag, text, section in parser.blocks:
        if section not in {"description", None}:
            continue
        if _TYPE.search(text):
            continue
        if text.casefold().rstrip(":") == "valid values":
            enum_started = True
            continue
        if enum_started and block_tag in {"p", "li"}:
            match = _ENUM.match(text)
            if match:
                code, meaning = match.groups()
                code = _space(code)
                meaning = _space(meaning)
                if code not in seen_codes:
                    seen_codes.add(code)
                    values.append(FixEnumValue(code, meaning))
                continue
        if (
            section == "description"
            and not enum_started
            and block_tag == "p"
            and text not in description
        ):
            description.append(text)

    return FixField(
        tag=reference.tag,
        name=reference.name,
        fix_type=fix_type,
        version=reference.version,
        description="\n\n".join(description),
        values=tuple(values),
        source_url=reference.url,
        status=reference.status,
    )


def parse_message_detail(
    body: bytes,
    *,
    reference: FixMessageRef,
    encoding: str = "utf-8",
) -> FixMessage:
    """Parse one message and preserve its nested repeating/component structure."""

    return parse_message_page(body, reference=reference, encoding=encoding)[0]


def parse_message_page(
    body: bytes,
    *,
    reference: FixMessageRef,
    encoding: str = "utf-8",
) -> tuple[FixMessage, tuple[FixComponentRef, ...]]:
    """Parse one message and its forward component links in one HTML pass."""

    parser = _parse(body, reference.url, encoding)
    identity: tuple[str, str] | None = None
    for tag, text, _ in parser.blocks:
        if tag not in {"h1", "h2", "title"}:
            continue
        match = _MESSAGE_HEADING.search(text)
        if match:
            identity = (_space(match.group(1)), _space(match.group(2)))
            break
    if identity is None:
        raise FixParseError(f"{reference.url}: missing FIX message heading")
    name, msg_type = identity
    if msg_type != reference.msg_type or name.casefold() != reference.name.casefold():
        raise FixParseError(
            f"{reference.url}: expected {reference.name!r} <{reference.msg_type}>, "
            f"found {name!r} <{msg_type}>"
        )
    _validate_page_version(parser, reference.version, reference.url)
    members, component_refs = _parse_structure_page(
        parser,
        reference.url,
        version=reference.version,
    )
    return (
        FixMessage(
            reference.name,
            reference.msg_type,
            reference.version,
            members,
            _description(parser),
            reference.url,
        ),
        component_refs,
    )


def parse_component_detail(
    body: bytes,
    *,
    reference: FixComponentRef,
    encoding: str = "utf-8",
) -> FixComponent:
    """Parse one reusable component definition."""

    return parse_component_page(body, reference=reference, encoding=encoding)[0]


def parse_component_page(
    body: bytes,
    *,
    reference: FixComponentRef,
    encoding: str = "utf-8",
) -> tuple[FixComponent, tuple[FixComponentRef, ...]]:
    """Parse one component and its forward component links in one HTML pass."""

    parser = _parse(body, reference.url, encoding)
    found_identity = False
    for tag, text, _ in parser.blocks:
        if tag not in {"h1", "h2", "title"}:
            continue
        match = _COMPONENT_HEADING.search(text)
        if match:
            discovered = _space(match.group(1)).strip("<>")
            if discovered.casefold() != reference.name.casefold():
                raise FixParseError(
                    f"{reference.url}: expected component {reference.name!r}, "
                    f"found {discovered!r}"
                )
            found_identity = True
            break
        suffix = _space(text.split(":", 1)[-1]).strip("<>")
        if suffix.casefold() == reference.name.casefold():
            found_identity = True
            break
    if not found_identity:
        raise FixParseError(f"{reference.url}: missing FIX component heading")
    _validate_page_version(parser, reference.version, reference.url)
    members, component_refs = _parse_structure_page(
        parser,
        reference.url,
        version=reference.version,
        allow_empty=True,
    )
    return (
        FixComponent(
            reference.name,
            reference.version,
            members,
            _description(parser),
            reference.url,
        ),
        component_refs,
    )


def _parse_structure_page(
    parser: _OnixsParser,
    page_url: str,
    *,
    version: str,
    allow_empty: bool = False,
) -> tuple[tuple[FixStructureMember, ...], tuple[FixComponentRef, ...]]:
    table_id, header_index = _structure_table(parser.rows, page_url)
    nodes: list[_MemberNode] = []
    stack: list[_MemberNode] = []
    component_refs: dict[str, FixComponentRef] = {}
    rows = [row for row in parser.rows if row.table_id == table_id]
    for row in rows[header_index + 1 :]:
        required_index = next(
            (
                index
                for index, cell in enumerate(row.cells)
                if cell.text.casefold() in {"y", "n"}
            ),
            None,
        )
        candidate_anchors = [
            (index, anchor)
            for index, cell in enumerate(row.cells)
            for anchor in cell.anchors
            if required_index is None or index < required_index
            if _TAG_HREF.search(anchor.href) or _COMPONENT_HREF.search(anchor.href)
        ]
        if not candidate_anchors:
            continue
        if required_index is None:
            raise FixParseError(f"{page_url}: structure row has no Y/N requirement")
        field_tags = {
            int(match.group(1))
            for _, anchor in candidate_anchors
            if (match := _TAG_HREF.search(anchor.href)) is not None
        }
        component_names = {
            name
            for _, anchor in candidate_anchors
            if (name := _component_name(anchor.href)) is not None
        }
        if len(field_tags) + len(component_names) != 1:
            raise FixParseError(f"{page_url}: ambiguous structure row member")
        if field_tags:
            kind = "field"
            tag_identity: int | None = next(iter(field_tags))
            name_identity: str | None = None
        else:
            kind = "component"
            tag_identity = None
            name_identity = next(iter(component_names))
            component_anchor = next(
                anchor
                for _, anchor in candidate_anchors
                if _component_name(anchor.href) == name_identity
            )
            component_ref = FixComponentRef(
                name_identity,
                urljoin(page_url, component_anchor.href),
                version,
            )
            component_key = name_identity.casefold()
            previous = component_refs.get(component_key)
            if previous is not None and previous.url != component_ref.url:
                raise FixParseError(
                    f"{page_url}: conflicting component {name_identity!r} links"
                )
            component_refs.setdefault(component_key, component_ref)
        anchor_index = candidate_anchors[0][0]
        depth = sum(_arrow_count(cell.text) for cell in row.cells[:anchor_index])
        comment = " ".join(
            cell.text for cell in row.cells[required_index + 1 :] if cell.text
        )
        node = _MemberNode(
            depth,
            kind,
            tag_identity,
            name_identity,
            row.cells[required_index].text.casefold() == "y",
            _space(comment),
        )
        if depth > len(stack):
            raise FixParseError(f"{page_url}: invalid structure depth jump to {depth}")
        if depth:
            if depth - 1 >= len(stack):
                raise FixParseError(f"{page_url}: orphan nested structure member")
            stack[depth - 1].children.append(node)
        else:
            nodes.append(node)
        if depth < len(stack):
            del stack[depth:]
        stack.append(node)
    if not nodes and not allow_empty:
        raise FixParseError(f"{page_url}: FIX structure contains no members")
    return (
        tuple(_freeze_member(node, page_url) for node in nodes),
        tuple(component_refs.values()),
    )


def _freeze_member(node: _MemberNode, page_url: str) -> FixStructureMember:
    if node.kind == "component":
        if node.children:
            raise FixParseError(
                f"{page_url}: component {node.name!r} cannot own indented members"
            )
        if node.name is None:  # pragma: no cover - parser invariant
            raise RuntimeError("component-member parser invariant failed")
        return FixComponentMember(node.name, node.required, node.comment)
    if node.tag is None:  # pragma: no cover - parser invariant
        raise RuntimeError("field-member parser invariant failed")
    if node.children:
        return FixRepeatingGroup(
            node.tag,
            tuple(_freeze_member(child, page_url) for child in node.children),
            node.required,
            node.comment,
        )
    return FixFieldMember(node.tag, node.required, node.comment)


def _structure_table(rows: list[_RawRow], page_url: str) -> tuple[int, int]:
    grouped: dict[int, list[_RawRow]] = defaultdict(list)
    for row in rows:
        grouped[row.table_id].append(row)
    for table_id, table_rows in grouped.items():
        for index, row in enumerate(table_rows):
            headings = {_header_name(cell.text) for cell in row.cells}
            if {"tag", "fieldname", "reqd"}.issubset(headings):
                return table_id, index
    raise FixParseError(f"{page_url}: missing FIX Structure table")


def _header_maps(rows: list[_RawRow]) -> dict[int, dict[str, int]]:
    result: dict[int, dict[str, int]] = {}
    for row in rows:
        if not any(cell.tag == "th" for cell in row.cells):
            continue
        current: dict[str, int] = {}
        for index, cell in enumerate(row.cells):
            header = _header_name(cell.text)
            if header == "msgtype":
                current["msgtype"] = index
            elif header in {"name", "messagename"}:
                current["name"] = index
        if {"msgtype", "name"}.issubset(current):
            result[row.table_id] = current
    return result


def _cell_at(row: _RawRow, index: int) -> str:
    return row.cells[index].text if index < len(row.cells) else ""


def _header_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.casefold())


def _component_name(href: str) -> str | None:
    match = _COMPONENT_HREF.search(href)
    if not match:
        return None
    return unquote(match.group(1))


def _arrow_count(value: str) -> int:
    return value.count("=>") + value.count("→") + value.count("⇒")


def _description(parser: _OnixsParser) -> str:
    paragraphs: list[str] = []
    for tag, text, section in parser.blocks:
        if tag == "p" and section == "description" and text not in paragraphs:
            paragraphs.append(text)
    return "\n\n".join(paragraphs)


def _parse(body: bytes, page_url: str, encoding: str) -> _OnixsParser:
    try:
        text = body.decode(encoding)
    except (LookupError, UnicodeDecodeError) as exc:
        raise FixParseError(f"{page_url}: invalid HTML encoding {encoding!r}") from exc
    parser = _OnixsParser()
    try:
        parser.feed(text)
        parser.close()
    except Exception as exc:
        raise FixParseError(f"{page_url}: malformed HTML: {exc}") from exc
    return parser


def _space(value: str) -> str:
    return " ".join(value.replace("\xa0", " ").split())


def _validate_page_version(
    parser: _OnixsParser, expected_version: str, page_url: str
) -> None:
    if expected_version.casefold() == "latest":
        return
    expected = _edition_token(expected_version)
    headings = [
        _edition_token(text)
        for tag, text, _ in parser.blocks
        if tag in {"h1", "h2", "title"} and text.casefold().startswith("fix")
    ]
    headings = [
        heading for heading in headings if any(char.isdigit() for char in heading)
    ]
    if headings and expected not in headings:
        raise FixParseError(
            f"{page_url}: page version does not match {expected_version!r}"
        )


def _edition_token(value: str) -> str:
    edition = value.split(":", 1)[0]
    normalized = re.sub(r"[^a-z0-9]", "", edition.casefold())
    if normalized.startswith("fix") and len(normalized) > 3 and normalized[3].isdigit():
        normalized = normalized[3:]
    return normalized
