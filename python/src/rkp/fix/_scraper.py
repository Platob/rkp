"""On-demand, cache-first scraper for the OnixS FIX dictionary."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import os
import random
import re
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import closing
from typing import Any, Self
from urllib.parse import unquote, urljoin, urlparse

from ._cache import FixCache, FixCacheEntry
from ._errors import FixCacheError, FixFetchError, FixParseError, FixVersionError
from ._html import (
    _PARSER_VERSION,
    FixComponentRef,
    FixFieldRef,
    FixMessageRef,
    parse_component_index,
    parse_component_page,
    parse_field_detail,
    parse_field_index,
    parse_message_index,
    parse_message_page,
)
from ._models import (
    FixComponent,
    FixComponentMember,
    FixDictionary,
    FixEnumValue,
    FixField,
    FixFieldMember,
    FixMessage,
    FixRepeatingGroup,
    FixStructureMember,
)

__all__ = ["OnixsFixScraper", "scrape_onixs_fields"]

_DEFAULT_BASE = "https://www.onixs.biz/fix-dictionary/"
_CLASSIC = {
    "4.0": ("4.0", "4.0"),
    "4.1": ("4.1", "4.1"),
    "4.2": ("4.2", "4.2"),
    "4.3": ("4.3", "4.3"),
    "4.4": ("4.4", "4.4"),
    "5.0": ("5.0", "5.0"),
    "5.0.sp1": ("5.0.SP1", "5.0.sp1"),
    "5.0.sp2": ("5.0.SP2", "5.0.sp2"),
    "fixt1.1": ("FIXT1.1", "fixt1.1"),
}
_EP_VERSION = re.compile(r"^5\.0(?:\.|\s*)sp2(?:\.|\s*)ep(\d+)$", re.IGNORECASE)
_CANONICAL_EP = re.compile(r"/fix-dictionary/(5\.0\.sp2\.ep(\d+))/", re.IGNORECASE)
_ARTIFACT_FORMAT = 3


@dataclasses.dataclass(frozen=True, slots=True)
class _Version:
    display: str
    path: str
    modern: bool
    latest: bool = False


class OnixsFixScraper:
    """Fetch selected OnixS FIX fields and structures into RKP definitions.

    Network access is always explicit. ``dictionary`` requires a field
    selection, while the much larger complete crawl is named ``scrape_all``.
    All pages are cached in SQLite and can subsequently be parsed offline.
    """

    def __init__(
        self,
        cache: FixCache | str | os.PathLike[str] | None = None,
        *,
        base_url: str = _DEFAULT_BASE,
        opener: Any = None,
        timeout: float = 30.0,
        ttl: float = 7 * 24 * 60 * 60,
        min_interval: float = 0.5,
        max_response_bytes: int = 16 * 1024 * 1024,
        retries: int = 2,
        user_agent: str = "rkp/0 (+https://github.com/Platob/rkp)",
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        parsed = urlparse(base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise TypeError("base_url must be an absolute HTTP(S) URL")
        if not base_url.endswith("/"):
            base_url += "/"
            parsed = urlparse(base_url)
        if not callable(clock) or not callable(sleep):
            raise TypeError("clock and sleep must be callable")
        for label, value in (
            ("timeout", timeout),
            ("ttl", ttl),
            ("min_interval", min_interval),
        ):
            if not isinstance(value, (int, float)) or value < 0:
                raise TypeError(f"{label} must be a non-negative number")
        if type(max_response_bytes) is not int or max_response_bytes <= 0:
            raise TypeError("max_response_bytes must be a positive integer")
        if type(retries) is not int or retries < 0:
            raise TypeError("retries must be a non-negative integer")
        if not isinstance(user_agent, str) or not user_agent:
            raise TypeError("user_agent must be a non-empty string")

        self.base_url = base_url
        self.timeout = float(timeout)
        self.ttl = float(ttl)
        self.min_interval = float(min_interval)
        self.max_response_bytes = max_response_bytes
        self.retries = retries
        self.user_agent = user_agent
        self._clock = clock
        self._sleep = sleep
        self._origin = (parsed.scheme.casefold(), parsed.hostname, parsed.port)
        self._prefix = parsed.path
        self._own_cache = not isinstance(cache, FixCache)
        self.cache = cache if isinstance(cache, FixCache) else FixCache(cache)
        self._rate_lock = threading.Lock()
        self._last_request = float("-inf")
        self._flight_guard = threading.Lock()
        self._flights: dict[str, threading.Lock] = {}
        if opener is None:
            self._opener = urllib.request.build_opener(
                _SafeRedirectHandler(self._validate_url)
            )
        else:
            self._opener = opener

    def close(self) -> None:
        """Close a cache created by this scraper."""

        if self._own_cache:
            self.cache.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def list_fields(
        self,
        version: str = "4.4",
        *,
        refresh: bool = False,
        offline: bool = False,
    ) -> tuple[FixFieldRef, ...]:
        """Fetch/cache an index and return local lightweight field references."""

        _validate_flags(refresh, offline)
        selected = _normalize_version(version)
        page_url = self._index_url(selected)
        entry = self._fetch(page_url, refresh=refresh, offline=offline)
        family, key = _artifact_key("index", page_url, entry.body)
        if not refresh:
            cached = self.cache.get_artifact(key)
            if cached is not None:
                return _refs_from_json(cached)
        references, canonical = parse_field_index(
            entry.body,
            page_url=page_url,
            version=selected.display,
            encoding=entry.encoding or "utf-8",
        )
        if selected.latest and canonical:
            canonical = urljoin(page_url, canonical)
            try:
                self._validate_url(canonical)
                pinned = _version_from_canonical(canonical)
            except (FixFetchError, FixVersionError):
                pinned = selected
            if not pinned.latest:
                references = tuple(
                    dataclasses.replace(
                        item,
                        version=pinned.display,
                        url=urljoin(canonical, f"tagNum_{item.tag}.html"),
                    )
                    for item in references
                )
        self.cache.put_artifact(key, _refs_to_json(references), family=family)
        return references

    def field(
        self,
        version: str,
        tag_or_name: int | str,
        *,
        refresh: bool = False,
        offline: bool = False,
    ) -> FixField:
        """Load one selected field, using local raw/normalized caches first."""

        references = self.list_fields(version, refresh=refresh, offline=offline)
        reference = _lookup_reference(references, tag_or_name)
        return self._field_from_reference(reference, refresh=refresh, offline=offline)

    def list_messages(
        self,
        version: str = "4.4",
        *,
        refresh: bool = False,
        offline: bool = False,
    ) -> tuple[FixMessageRef, ...]:
        """Fetch/cache a message index without hydrating message structures."""

        _validate_flags(refresh, offline)
        selected = _normalize_version(version)
        page_url = self._message_index_url(selected)
        entry = self._fetch(page_url, refresh=refresh, offline=offline)
        family, key = _artifact_key("message-index", page_url, entry.body)
        if not refresh:
            cached = self.cache.get_artifact(key)
            if cached is not None:
                return _message_refs_from_json(cached)
        references, canonical = parse_message_index(
            entry.body,
            page_url=page_url,
            version=selected.display,
            encoding=entry.encoding or "utf-8",
        )
        references = self._pin_message_references(
            selected, references, canonical, page_url
        )
        self.cache.put_artifact(key, _message_refs_to_json(references), family=family)
        return references

    def list_components(
        self,
        version: str = "latest",
        *,
        refresh: bool = False,
        offline: bool = False,
    ) -> tuple[FixComponentRef, ...]:
        """Fetch/cache the modern component index.

        Classic OnixS dictionaries do not publish a component index. Their
        components are discovered safely from selected message/component
        structure links by :meth:`dictionary`.
        """

        _validate_flags(refresh, offline)
        selected = _normalize_version(version)
        if not selected.modern:
            raise FixVersionError(
                "classic FIX dictionaries have no component index; "
                "select a message so its components can be discovered"
            )
        page_url = self._component_index_url(selected)
        entry = self._fetch(page_url, refresh=refresh, offline=offline)
        family, key = _artifact_key("component-index", page_url, entry.body)
        if not refresh:
            cached = self.cache.get_artifact(key)
            if cached is not None:
                return _component_refs_from_json(cached)
        references, canonical = parse_component_index(
            entry.body,
            page_url=page_url,
            version=selected.display,
            encoding=entry.encoding or "utf-8",
        )
        references = self._pin_component_references(
            selected, references, canonical, page_url
        )
        self.cache.put_artifact(key, _component_refs_to_json(references), family=family)
        return references

    def message(
        self,
        version: str,
        msg_type_or_name: str,
        *,
        refresh: bool = False,
        offline: bool = False,
    ) -> FixMessage:
        """Fetch/cache one message structure without resolving components."""

        references = self.list_messages(version, refresh=refresh, offline=offline)
        reference = _lookup_message_reference(references, msg_type_or_name)
        result, _ = self._message_from_reference(
            reference, refresh=refresh, offline=offline
        )
        return result

    def component(
        self,
        version: str,
        name: str,
        *,
        refresh: bool = False,
        offline: bool = False,
    ) -> FixComponent:
        """Fetch/cache one component from a modern component index."""

        references = self.list_components(version, refresh=refresh, offline=offline)
        reference = _lookup_component_reference(references, name)
        result, _ = self._component_from_reference(
            reference, refresh=refresh, offline=offline
        )
        return result

    def _field_from_reference(
        self,
        reference: FixFieldRef,
        *,
        refresh: bool,
        offline: bool,
    ) -> FixField:
        entry = self._fetch(reference.url, refresh=refresh, offline=offline)
        family, key = _artifact_key("field", reference.url, entry.body)
        if not refresh:
            cached = self.cache.get_artifact(key)
            if cached is not None:
                return _field_from_json(cached)
        result = parse_field_detail(
            entry.body,
            reference=reference,
            encoding=entry.encoding or "utf-8",
        )
        self.cache.put_artifact(key, _field_to_json(result), family=family)
        return result

    def _message_from_reference(
        self,
        reference: FixMessageRef,
        *,
        refresh: bool,
        offline: bool,
    ) -> tuple[FixMessage, tuple[FixComponentRef, ...]]:
        entry = self._fetch(reference.url, refresh=refresh, offline=offline)
        family, key = _artifact_key("message", reference.url, entry.body)
        cached = None if refresh else self.cache.get_artifact(key)
        if cached is None:
            result, component_refs = parse_message_page(
                entry.body,
                reference=reference,
                encoding=entry.encoding or "utf-8",
            )
            self.cache.put_artifact(
                key,
                _message_to_json(result, component_refs),
                family=family,
            )
        else:
            result, component_refs = _message_from_json(cached)
        return result, component_refs

    def _component_from_reference(
        self,
        reference: FixComponentRef,
        *,
        refresh: bool,
        offline: bool,
    ) -> tuple[FixComponent, tuple[FixComponentRef, ...]]:
        entry = self._fetch(reference.url, refresh=refresh, offline=offline)
        family, key = _artifact_key("component", reference.url, entry.body)
        cached = None if refresh else self.cache.get_artifact(key)
        if cached is None:
            result, component_refs = parse_component_page(
                entry.body,
                reference=reference,
                encoding=entry.encoding or "utf-8",
            )
            self.cache.put_artifact(
                key,
                _component_to_json(result, component_refs),
                family=family,
            )
        else:
            result, component_refs = _component_from_json(cached)
        return result, component_refs

    def dictionary(
        self,
        version: str,
        tags: Iterable[int | str] = (),
        *,
        messages: Iterable[str] = (),
        components: Iterable[str] = (),
        refresh: bool = False,
        offline: bool = False,
        persist_to: str | os.PathLike[str] | None = None,
        workers: int = 4,
    ) -> FixDictionary:
        """Hydrate selected fields and recursively resolved structures.

        Selecting a message fetches only its transitive component graph and
        referenced field definitions. Classic components are therefore found
        through the exact links published in the message pages; their URLs are
        never guessed.
        """

        if isinstance(tags, (str, bytes)) or not isinstance(tags, Iterable):
            raise TypeError("tags must be an iterable of FIX tags or names")
        if isinstance(messages, (str, bytes)) or not isinstance(messages, Iterable):
            raise TypeError("messages must be an iterable of MsgTypes or names")
        if isinstance(components, (str, bytes)) or not isinstance(components, Iterable):
            raise TypeError("components must be an iterable of component names")
        if type(workers) is not int or not 1 <= workers <= 32:
            raise TypeError("workers must be an integer between 1 and 32")
        _validate_flags(refresh, offline)
        references = self.list_fields(version, refresh=refresh, offline=offline)
        message_values = tuple(messages)
        component_values = tuple(components)
        resolved_messages: list[FixMessage] = []
        component_refs: dict[str, FixComponentRef] = {}
        resolved_components: dict[str, FixComponent] = {}

        if message_values:
            message_references = self.list_messages(
                version, refresh=refresh, offline=offline
            )
            selected_messages = _select_message_references(
                message_references, message_values
            )
            for message_reference in selected_messages:
                message, discovered = self._message_from_reference(
                    message_reference, refresh=refresh, offline=offline
                )
                resolved_messages.append(message)
                _merge_component_references(component_refs, discovered)

        if component_values:
            indexed_components = self.list_components(
                version, refresh=refresh, offline=offline
            )
            _merge_component_references(
                component_refs,
                _select_component_references(indexed_components, component_values),
            )

        pending = list(component_refs.values())
        visiting: list[str] = []
        while pending:
            reference = pending.pop()
            key = reference.name.casefold()
            if key in resolved_components:
                continue
            if len(resolved_components) >= 4096:
                raise FixFetchError("FIX component graph exceeds 4096 definitions")
            visiting.append(reference.name)
            component, discovered = self._component_from_reference(
                reference, refresh=refresh, offline=offline
            )
            resolved_components[key] = component
            _merge_component_references(component_refs, discovered)
            for nested in reversed(discovered):
                if nested.name.casefold() not in resolved_components:
                    pending.append(nested)
            visiting.pop()

        selected_tags: list[int | str] = list(tags)
        for message in resolved_messages:
            selected_tags.extend(_member_tags(message.members))
        for component in resolved_components.values():
            selected_tags.extend(_member_tags(component.members))
        selected = _select_references(references, selected_tags)
        return self._hydrate_dictionary(
            selected,
            fallback_version=(
                references[0].version
                if references
                else _normalize_version(version).display
            ),
            source_url=self._field_index_source_url(references, version),
            refresh=refresh,
            offline=offline,
            persist_to=persist_to,
            workers=workers,
            components=tuple(resolved_components.values()),
            messages=tuple(resolved_messages),
        )

    def _hydrate_dictionary(
        self,
        references: tuple[FixFieldRef, ...],
        *,
        fallback_version: str,
        source_url: str,
        refresh: bool,
        offline: bool,
        persist_to: str | os.PathLike[str] | None,
        workers: int,
        components: tuple[FixComponent, ...] = (),
        messages: tuple[FixMessage, ...] = (),
    ) -> FixDictionary:
        fields: list[FixField] = []
        if workers == 1 or len(references) < 2:
            for item in references:
                fields.append(
                    self._field_from_reference(item, refresh=refresh, offline=offline)
                )
        else:
            with ThreadPoolExecutor(max_workers=min(workers, len(references))) as pool:
                futures = {
                    pool.submit(
                        self._field_from_reference,
                        item,
                        refresh=refresh,
                        offline=offline,
                    ): item
                    for item in references
                }
                for future in as_completed(futures):
                    fields.append(future.result())
        resolved_version = fields[0].version if fields else fallback_version
        result = FixDictionary(
            resolved_version,
            tuple(sorted(fields, key=lambda item: item.tag)),
            source_url,
            components=components,
            messages=messages,
        )
        if persist_to is not None:
            result.dump(persist_to)
        return result

    def scrape_all(
        self,
        version: str,
        *,
        refresh: bool = False,
        offline: bool = False,
        persist_to: str | os.PathLike[str] | None = None,
        workers: int = 4,
    ) -> FixDictionary:
        """Explicitly hydrate every field in a version (potentially thousands)."""

        if type(workers) is not int or not 1 <= workers <= 32:
            raise TypeError("workers must be an integer between 1 and 32")
        references = self.list_fields(version, refresh=refresh, offline=offline)
        return self._hydrate_dictionary(
            references,
            fallback_version=(
                references[0].version
                if references
                else _normalize_version(version).display
            ),
            source_url=self._field_index_source_url(references, version),
            refresh=refresh,
            offline=offline,
            persist_to=persist_to,
            workers=workers,
        )

    def cache_info(self) -> Mapping[str, int | str]:
        """Return local cache counts without performing network access."""

        return self.cache.info()

    def _index_url(self, version: _Version) -> str:
        filename = "fields.html" if version.modern else "fields_by_tag.html"
        return urljoin(self.base_url, f"{version.path}/{filename}")

    def _field_index_source_url(
        self, references: tuple[FixFieldRef, ...], requested_version: str
    ) -> str:
        if not references:
            return self._index_url(_normalize_version(requested_version))
        pinned = _normalize_version(references[0].version)
        filename = "fields.html" if pinned.modern else "fields_by_tag.html"
        return urljoin(references[0].url, filename)

    def _message_index_url(self, version: _Version) -> str:
        filename = "messages.html" if version.modern else "msgs_by_msg_type.html"
        return urljoin(self.base_url, f"{version.path}/{filename}")

    def _component_index_url(self, version: _Version) -> str:
        return urljoin(self.base_url, f"{version.path}/components.html")

    def _pin_message_references(
        self,
        selected: _Version,
        references: tuple[FixMessageRef, ...],
        canonical: str | None,
        page_url: str,
    ) -> tuple[FixMessageRef, ...]:
        if not selected.latest or not canonical:
            return references
        canonical = urljoin(page_url, canonical)
        try:
            self._validate_url(canonical)
            pinned = _version_from_canonical(canonical)
        except (FixFetchError, FixVersionError):
            return references
        return tuple(
            dataclasses.replace(
                item,
                version=pinned.display,
                url=urljoin(canonical, item.url.rsplit("/", 1)[-1]),
            )
            for item in references
        )

    def _pin_component_references(
        self,
        selected: _Version,
        references: tuple[FixComponentRef, ...],
        canonical: str | None,
        page_url: str,
    ) -> tuple[FixComponentRef, ...]:
        if not selected.latest or not canonical:
            return references
        canonical = urljoin(page_url, canonical)
        try:
            self._validate_url(canonical)
            pinned = _version_from_canonical(canonical)
        except (FixFetchError, FixVersionError):
            return references
        return tuple(
            dataclasses.replace(
                item,
                version=pinned.display,
                url=urljoin(canonical, item.url.rsplit("/", 1)[-1]),
            )
            for item in references
        )

    def _fetch(self, url: str, *, refresh: bool, offline: bool) -> FixCacheEntry:
        self._validate_url(url)
        lock = self._flight(url)
        with lock:
            cached = self.cache.get_response(url)
            now = self._clock()
            fresh = cached is not None and now - cached.fetched_at <= self.ttl
            if cached is not None and (fresh and not refresh or offline):
                return cached
            if offline:
                raise FixCacheError(f"offline FIX cache miss for {url}")
            headers = {
                "Accept": "text/html,application/xhtml+xml",
                "User-Agent": self.user_agent,
            }
            if cached is not None:
                if cached.etag:
                    headers["If-None-Match"] = cached.etag
                if cached.last_modified:
                    headers["If-Modified-Since"] = cached.last_modified
            request = urllib.request.Request(url, headers=headers)
            for attempt in range(self.retries + 1):
                try:
                    self._wait_for_slot()
                    response = self._open(request)
                    with closing(response):
                        final_url = _response_url(response, url)
                        self._validate_url(final_url)
                        status = _response_status(response)
                        if status == 304 and cached is not None:
                            self.cache.touch_response(url, fetched_at=now)
                            return dataclasses.replace(cached, fetched_at=now)
                        if status < 200 or status >= 300:
                            raise FixFetchError(f"{url}: HTTP {status}")
                        body = _bounded_read(response, self.max_response_bytes)
                        content_type, encoding = _response_content_type(response)
                        if content_type and not content_type.startswith(
                            ("text/html", "application/xhtml+xml")
                        ):
                            raise FixFetchError(
                                f"{url}: expected HTML, received {content_type}"
                            )
                        return self.cache.put_response(
                            url,
                            body,
                            fetched_at=now,
                            content_type=content_type,
                            encoding=encoding,
                            etag=_response_header(response, "ETag"),
                            last_modified=_response_header(response, "Last-Modified"),
                        )
                except urllib.error.HTTPError as exc:
                    try:
                        if exc.code == 304 and cached is not None:
                            self.cache.touch_response(url, fetched_at=now)
                            return dataclasses.replace(cached, fetched_at=now)
                        if exc.code not in {429, 500, 502, 503, 504}:
                            raise FixFetchError(f"{url}: HTTP {exc.code}") from exc
                        error: Exception = exc
                        retry_after = (
                            exc.headers.get("Retry-After") if exc.headers else None
                        )
                    finally:
                        exc.close()
                except (urllib.error.URLError, TimeoutError, OSError) as exc:
                    error = exc
                    retry_after = None
                except FixFetchError:
                    raise
                if attempt < self.retries:
                    self._sleep(_retry_delay(attempt, retry_after))
            if cached is not None:
                # A stale, checksummed cache is safer and more useful than
                # failing a reproducible offline build on a transient outage.
                return cached
            raise FixFetchError(f"cannot fetch {url}: {error}") from error

    def _open(self, request: urllib.request.Request) -> Any:
        target = getattr(self._opener, "open", self._opener)
        if not callable(target):
            raise TypeError("opener must be callable or expose open()")
        return target(request, timeout=self.timeout)

    def _validate_url(self, url: str) -> None:
        parsed = urlparse(url)
        try:
            current = (parsed.scheme.casefold(), parsed.hostname, parsed.port)
        except ValueError as exc:
            raise FixFetchError(
                f"refusing URL with malformed FIX dictionary authority: {url}"
            ) from exc
        decoded_path = parsed.path
        for _ in range(4):
            expanded = unquote(decoded_path)
            if expanded == decoded_path:
                break
            decoded_path = expanded
        segments = decoded_path.replace("\\", "/").split("/")
        unsafe_path = (
            "\\" in decoded_path
            or any(segment in {".", ".."} for segment in segments)
            or "%" in decoded_path
        )
        if (
            current != self._origin
            or unsafe_path
            or not decoded_path.startswith(self._prefix)
        ):
            raise FixFetchError(f"refusing URL outside the FIX dictionary: {url}")

    def _wait_for_slot(self) -> None:
        with self._rate_lock:
            now = self._clock()
            wait = self.min_interval - (now - self._last_request)
            if wait > 0:
                self._sleep(wait)
                now = self._clock()
            self._last_request = now

    def _flight(self, url: str) -> threading.Lock:
        with self._flight_guard:
            return self._flights.setdefault(url, threading.Lock())


class _SafeRedirectHandler(urllib.request.HTTPRedirectHandler):
    def __init__(self, validator: Callable[[str], None]) -> None:
        self._validator = validator

    def redirect_request(
        self, req: Any, fp: Any, code: Any, msg: Any, headers: Any, newurl: str
    ):
        self._validator(newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def scrape_onixs_fields(
    version: str = "4.4",
    *,
    tags: Iterable[int | str] | None = None,
    all: bool = False,
    cache: FixCache | str | os.PathLike[str] | None = None,
    refresh: bool = False,
    offline: bool = False,
    persist_to: str | os.PathLike[str] | None = None,
    workers: int = 4,
) -> FixDictionary:
    """Convenience selected-field scrape; a full crawl requires ``all=True``."""

    if type(all) is not bool:
        raise TypeError("all must be bool")
    if all == (tags is not None):
        raise TypeError("provide tags or set all=True, but not both")
    with OnixsFixScraper(cache) as scraper:
        if all:
            return scraper.scrape_all(
                version,
                refresh=refresh,
                offline=offline,
                persist_to=persist_to,
                workers=workers,
            )
        return scraper.dictionary(
            version,
            tags or (),
            refresh=refresh,
            offline=offline,
            persist_to=persist_to,
            workers=workers,
        )


def _normalize_version(value: str) -> _Version:
    if not isinstance(value, str) or not value.strip():
        raise FixVersionError("FIX version must be a non-empty string")
    compact = value.strip().casefold().replace(" ", ".")
    while ".." in compact:
        compact = compact.replace("..", ".")
    if compact == "latest":
        return _Version("latest", "latest", True, True)
    compact = compact.removeprefix("fix.")
    if compact in _CLASSIC:
        display, path = _CLASSIC[compact]
        return _Version(display, path, False)
    match = _EP_VERSION.match(compact)
    if match:
        number = match.group(1)
        return _Version(f"5.0.SP2 EP{number}", f"5.0.sp2.ep{number}", True)
    raise FixVersionError(f"unsupported FIX dictionary version {value!r}")


def _version_from_canonical(url: str) -> _Version:
    match = _CANONICAL_EP.search(urlparse(url).path)
    if not match:
        raise FixVersionError(f"cannot resolve FIX version from canonical URL {url!r}")
    return _Version(f"5.0.SP2 EP{match.group(2)}", match.group(1).casefold(), True)


def _lookup_reference(
    references: tuple[FixFieldRef, ...], value: int | str
) -> FixFieldRef:
    if type(value) is int:
        matches = (item for item in references if item.tag == value)
    elif isinstance(value, str) and value:
        if value.isdecimal():
            numeric = int(value)
            matches = (item for item in references if item.tag == numeric)
        else:
            folded = value.casefold()
            matches = (item for item in references if item.name.casefold() == folded)
    else:
        raise TypeError("FIX field selection expects an integer tag or non-empty name")
    try:
        return next(matches)
    except StopIteration:
        raise KeyError(f"unknown FIX field {value!r}") from None


def _select_references(
    references: tuple[FixFieldRef, ...], values: Iterable[int | str]
) -> tuple[FixFieldRef, ...]:
    by_tag = {item.tag: item for item in references}
    by_name = {item.name.casefold(): item for item in references}
    selected: dict[int, FixFieldRef] = {}
    for value in values:
        if type(value) is int:
            item = by_tag.get(value)
        elif isinstance(value, str) and value:
            item = (
                by_tag.get(int(value))
                if value.isdecimal()
                else by_name.get(value.casefold())
            )
        else:
            raise TypeError(
                "FIX field selection expects an integer tag or non-empty name"
            )
        if item is None:
            raise KeyError(f"unknown FIX field {value!r}")
        selected[item.tag] = item
    return tuple(sorted(selected.values(), key=lambda item: item.tag))


def _lookup_message_reference(
    references: tuple[FixMessageRef, ...], value: str
) -> FixMessageRef:
    if not isinstance(value, str) or not value:
        raise TypeError("FIX message selection expects a non-empty MsgType or name")
    exact = next((item for item in references if item.msg_type == value), None)
    if exact is not None:
        return exact
    folded = value.casefold()
    matches = [item for item in references if item.name.casefold() == folded]
    if len(matches) == 1:
        return matches[0]
    raise KeyError(f"unknown FIX message {value!r}")


def _select_message_references(
    references: tuple[FixMessageRef, ...], values: Iterable[str]
) -> tuple[FixMessageRef, ...]:
    selected: dict[str, FixMessageRef] = {}
    for value in values:
        item = _lookup_message_reference(references, value)
        selected[item.msg_type] = item
    return tuple(sorted(selected.values(), key=lambda item: item.msg_type))


def _lookup_component_reference(
    references: tuple[FixComponentRef, ...], value: str
) -> FixComponentRef:
    if not isinstance(value, str) or not value:
        raise TypeError("FIX component selection expects a non-empty name")
    folded = value.casefold()
    try:
        return next(item for item in references if item.name.casefold() == folded)
    except StopIteration:
        raise KeyError(f"unknown FIX component {value!r}") from None


def _select_component_references(
    references: tuple[FixComponentRef, ...], values: Iterable[str]
) -> tuple[FixComponentRef, ...]:
    selected: dict[str, FixComponentRef] = {}
    for value in values:
        item = _lookup_component_reference(references, value)
        selected[item.name.casefold()] = item
    return tuple(sorted(selected.values(), key=lambda item: item.name.casefold()))


def _merge_component_references(
    target: dict[str, FixComponentRef], values: Iterable[FixComponentRef]
) -> None:
    for item in values:
        key = item.name.casefold()
        previous = target.get(key)
        if previous is not None and previous.url != item.url:
            raise FixFetchError(
                f"conflicting URLs for FIX component {item.name!r}: "
                f"{previous.url!r} and {item.url!r}"
            )
        target[key] = item


def _member_tags(members: tuple[FixStructureMember, ...]) -> list[int]:
    result: list[int] = []
    for member in members:
        if isinstance(member, FixComponentMember):
            continue
        result.append(member.tag)
        if isinstance(member, FixRepeatingGroup):
            result.extend(_member_tags(member.members))
    return result


def _artifact_key(kind: str, identity: str, body: bytes) -> tuple[str, str]:
    digest = hashlib.sha256(body).hexdigest()
    family = f"parser:{_PARSER_VERSION}:{_ARTIFACT_FORMAT}:{kind}:{identity}:"
    return family, f"{family}{digest}"


def _refs_to_json(values: tuple[FixFieldRef, ...]) -> bytes:
    return json.dumps(
        [dataclasses.asdict(value) for value in values],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


def _refs_from_json(value: bytes) -> tuple[FixFieldRef, ...]:
    try:
        payload = json.loads(value)
        return tuple(FixFieldRef(**item) for item in payload)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise FixCacheError(f"invalid cached FIX field index: {exc}") from exc


def _message_refs_to_json(values: tuple[FixMessageRef, ...]) -> bytes:
    return _dataclasses_json(values)


def _message_refs_from_json(value: bytes) -> tuple[FixMessageRef, ...]:
    try:
        payload = json.loads(value)
        return tuple(FixMessageRef(**item) for item in payload)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise FixCacheError(f"invalid cached FIX message index: {exc}") from exc


def _component_refs_to_json(values: tuple[FixComponentRef, ...]) -> bytes:
    return _dataclasses_json(values)


def _component_refs_from_json(value: bytes) -> tuple[FixComponentRef, ...]:
    try:
        payload = json.loads(value)
        return tuple(FixComponentRef(**item) for item in payload)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise FixCacheError(f"invalid cached FIX component index: {exc}") from exc


def _dataclasses_json(values: tuple[Any, ...]) -> bytes:
    return json.dumps(
        [dataclasses.asdict(value) for value in values],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


def _field_to_json(value: FixField) -> bytes:
    payload = {
        "tag": value.tag,
        "name": value.name,
        "fix_type": value.fix_type,
        "version": value.version,
        "description": value.description,
        "values": [dataclasses.asdict(item) for item in value.values],
        "source_url": value.source_url,
        "status": value.status,
    }
    return json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()


def _field_from_json(value: bytes) -> FixField:
    try:
        payload = json.loads(value)
        payload["values"] = tuple(FixEnumValue(**item) for item in payload["values"])
        return FixField(**payload)
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise FixCacheError(f"invalid cached FIX field artifact: {exc}") from exc


def _message_to_json(
    value: FixMessage, component_refs: tuple[FixComponentRef, ...]
) -> bytes:
    return _structure_to_json(
        {
            "value": {
                "name": value.name,
                "msg_type": value.msg_type,
                "version": value.version,
                "members": [_member_to_payload(item) for item in value.members],
                "description": value.description,
                "source_url": value.source_url,
            },
            "component_refs": [dataclasses.asdict(item) for item in component_refs],
        }
    )


def _message_from_json(
    value: bytes,
) -> tuple[FixMessage, tuple[FixComponentRef, ...]]:
    try:
        envelope = json.loads(value)
        payload = envelope["value"]
        payload["members"] = tuple(
            _member_from_payload(item) for item in payload["members"]
        )
        references = _component_refs_from_payload(envelope["component_refs"])
        return FixMessage(**payload), references
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise FixCacheError(f"invalid cached FIX message artifact: {exc}") from exc


def _component_to_json(
    value: FixComponent, component_refs: tuple[FixComponentRef, ...]
) -> bytes:
    return _structure_to_json(
        {
            "value": {
                "name": value.name,
                "version": value.version,
                "members": [_member_to_payload(item) for item in value.members],
                "description": value.description,
                "source_url": value.source_url,
            },
            "component_refs": [dataclasses.asdict(item) for item in component_refs],
        }
    )


def _component_from_json(
    value: bytes,
) -> tuple[FixComponent, tuple[FixComponentRef, ...]]:
    try:
        envelope = json.loads(value)
        payload = envelope["value"]
        payload["members"] = tuple(
            _member_from_payload(item) for item in payload["members"]
        )
        references = _component_refs_from_payload(envelope["component_refs"])
        return FixComponent(**payload), references
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise FixCacheError(f"invalid cached FIX component artifact: {exc}") from exc


def _component_refs_from_payload(value: Any) -> tuple[FixComponentRef, ...]:
    if not isinstance(value, list):
        raise TypeError("FIX component references must be a list")
    references: list[FixComponentRef] = []
    for item in value:
        if not isinstance(item, Mapping):
            raise TypeError("FIX component reference must be an object")
        references.append(
            FixComponentRef(
                name=item["name"],
                url=item["url"],
                version=item["version"],
            )
        )
    return tuple(references)


def _structure_to_json(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()


def _member_to_payload(value: FixStructureMember) -> dict[str, Any]:
    if isinstance(value, FixFieldMember):
        return {
            "kind": "field",
            "tag": value.tag,
            "required": value.required,
            "comment": value.comment,
        }
    if isinstance(value, FixComponentMember):
        return {
            "kind": "component",
            "name": value.name,
            "required": value.required,
            "comment": value.comment,
        }
    return {
        "kind": "group",
        "tag": value.tag,
        "required": value.required,
        "comment": value.comment,
        "members": [_member_to_payload(item) for item in value.members],
    }


def _member_from_payload(value: Any) -> FixStructureMember:
    if not isinstance(value, Mapping):
        raise TypeError("FIX structure member must be an object")
    kind = value.get("kind")
    if kind == "field":
        return FixFieldMember(
            value["tag"], value.get("required", False), value.get("comment", "")
        )
    if kind == "component":
        return FixComponentMember(
            value["name"], value.get("required", False), value.get("comment", "")
        )
    if kind == "group":
        raw_members = value.get("members")
        if not isinstance(raw_members, list):
            raise TypeError("FIX repeating group members must be a list")
        return FixRepeatingGroup(
            value["tag"],
            tuple(_member_from_payload(item) for item in raw_members),
            value.get("required", False),
            value.get("comment", ""),
        )
    raise TypeError(f"unknown FIX structure member kind {kind!r}")


def _response_url(response: Any, fallback: str) -> str:
    getter = getattr(response, "geturl", None)
    return getter() if callable(getter) else fallback


def _response_status(response: Any) -> int:
    value = getattr(response, "status", None)
    if value is None:
        getter = getattr(response, "getcode", None)
        value = getter() if callable(getter) else 200
    return int(value)


def _response_header(response: Any, name: str) -> str | None:
    headers = getattr(response, "headers", {})
    getter = getattr(headers, "get", None)
    value = getter(name) if callable(getter) else None
    return str(value) if value is not None else None


def _response_content_type(response: Any) -> tuple[str | None, str]:
    headers = getattr(response, "headers", {})
    get_type = getattr(headers, "get_content_type", None)
    get_charset = getattr(headers, "get_content_charset", None)
    if callable(get_type):
        content_type = get_type()
        encoding = get_charset() if callable(get_charset) else None
        return content_type, encoding or "utf-8"
    raw = _response_header(response, "Content-Type")
    if not raw:
        return None, "utf-8"
    parts = [part.strip() for part in raw.split(";")]
    encoding = "utf-8"
    for part in parts[1:]:
        if part.casefold().startswith("charset="):
            encoding = part.split("=", 1)[1].strip("\"'")
    return parts[0].casefold(), encoding


def _bounded_read(response: Any, maximum: int) -> bytes:
    try:
        value = response.read(maximum + 1)
    except TypeError:
        value = response.read()
    if not isinstance(value, bytes):
        raise FixFetchError("HTTP response body must be bytes")
    if len(value) > maximum:
        raise FixFetchError(f"HTTP response exceeds {maximum} bytes")
    return value


def _retry_delay(attempt: int, retry_after: str | None) -> float:
    if retry_after:
        try:
            return min(60.0, max(0.0, float(retry_after)))
        except ValueError:
            pass
    return min(8.0, 0.5 * (2**attempt)) + random.random() * 0.1


def _validate_flags(refresh: bool, offline: bool) -> None:
    if type(refresh) is not bool or type(offline) is not bool:
        raise TypeError("refresh and offline must be bool")
    if refresh and offline:
        raise ValueError("refresh and offline cannot both be enabled")
