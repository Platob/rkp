from __future__ import annotations

import io
import json
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from urllib.error import HTTPError

import pytest
from rkp import field_options
from rkp.fix import (
    FixCache,
    FixCacheError,
    FixDictionary,
    FixFetchError,
    FixParseError,
    FixVersionError,
    OnixsFixScraper,
    load_fix_dictionary,
)

BASE_URL = "https://www.onixs.biz/fix-dictionary"


class _Response(io.BytesIO):
    status = 200

    def __init__(
        self,
        body: bytes,
        url: str,
        content_type: str = "text/html; charset=utf-8",
    ) -> None:
        super().__init__(body)
        self.url = url
        self.headers = {"Content-Type": content_type}

    def geturl(self) -> str:
        return self.url


def _scraper(
    site: Any,
    cache_path: Path,
    *,
    ttl: float = 604_800,
    opener: Any = None,
    clock: Any = None,
) -> OnixsFixScraper:
    options = {} if clock is None else {"clock": clock}
    return OnixsFixScraper(
        cache=FixCache(cache_path),
        base_url=BASE_URL,
        opener=site.urlopen if opener is None else opener,
        timeout=1,
        ttl=ttl,
        min_interval=0,
        retries=0,
        **options,
    )


def test_scraper_parses_classic_field_page_and_enum_values(
    onixs_site: Any, tmp_path: Path
) -> None:
    side = _scraper(onixs_site, tmp_path / "cache").field("4.4", 54)

    assert side.tag == 54
    assert side.name == "Side"
    assert side.fix_type == "char"
    assert side.description == "Side of order."
    assert [(item.value, item.description) for item in side.values] == [
        ("1", "Buy"),
        ("2", "Sell"),
        ("A", "Cross short exempt"),
    ]
    assert side.source_url == f"{BASE_URL}/4.4/tagNum_54.html"


def test_scraper_decodes_nested_markup_entities_and_whitespace(
    onixs_site: Any, tmp_path: Path
) -> None:
    scraper = _scraper(onixs_site, tmp_path / "cache")

    quantity = scraper.field("4.4", 38)
    symbol = scraper.field("4.4", 55)

    assert quantity.fix_type == "Qty"
    assert quantity.description == "Quantity ordered."
    assert symbol.name == "Symbol"
    assert symbol.description == "Ticker symbol. & entities must be decoded."


def test_scraper_supports_modern_ep302_index_and_enum_markup(
    onixs_site: Any, tmp_path: Path
) -> None:
    scraper = _scraper(onixs_site, tmp_path / "cache")

    references = scraper.list_fields("5.0.SP2 EP302")
    side = scraper.field("5.0.sp2.ep302", 54)
    report_type = scraper.field("5.0 SP2 EP302", 1800)

    assert [(item.tag, item.name) for item in references] == [
        (54, "Side"),
        (1800, "ApplReportType"),
    ]
    assert side.version == "5.0.SP2 EP302"
    assert [(item.value, item.description) for item in side.values] == [
        ("1", "Buy"),
        ("2", "Sell"),
        ("A", "Cross short exempt"),
    ]
    assert report_type.fix_type == "int"
    assert report_type.annotation is int


def test_latest_is_pinned_to_the_canonical_ep_for_detail_fetches(
    onixs_site: Any, tmp_path: Path
) -> None:
    def latest_opener(request: Any, **kwargs: object) -> object:
        url = getattr(request, "full_url", str(request))
        if url.endswith("/latest/fields.html"):
            payload = onixs_site.read_text("5.0.sp2.ep302/fields.html").replace(
                "https://www.onixs.biz/fix-dictionary/5.0.sp2.ep302/fields.html",
                "../5.0.sp2.ep302/fields.html",
            )
            onixs_site.calls.append(url)
            return _Response(payload.encode(), url)
        return onixs_site.urlopen(request, **kwargs)

    scraper = _scraper(
        onixs_site,
        tmp_path / "cache",
        opener=latest_opener,
    )

    side = scraper.field("latest", 54)

    assert side.version == "5.0.SP2 EP302"
    assert side.source_url.endswith("/5.0.sp2.ep302/tagNum_54.html")
    assert onixs_site.calls == [
        f"{BASE_URL}/latest/fields.html",
        f"{BASE_URL}/5.0.sp2.ep302/tagNum_54.html",
    ]


def test_latest_dictionary_persists_the_pinned_index_url(
    onixs_site: Any, tmp_path: Path
) -> None:
    def latest_opener(request: Any, **kwargs: object) -> object:
        url = getattr(request, "full_url", str(request))
        if url.endswith("/latest/fields.html"):
            payload = onixs_site.read_text("5.0.sp2.ep302/fields.html")
            onixs_site.calls.append(url)
            return _Response(payload.encode(), url)
        return onixs_site.urlopen(request, **kwargs)

    scraper = _scraper(
        onixs_site,
        tmp_path / "cache",
        opener=latest_opener,
    )

    dictionary = scraper.dictionary("latest", tags=[54])

    assert dictionary.version == "5.0.SP2 EP302"
    assert dictionary.source_url == (f"{BASE_URL}/5.0.sp2.ep302/fields.html")


def test_field_name_lookup_deduplicates_index_links(
    onixs_site: Any, tmp_path: Path
) -> None:
    scraper = _scraper(onixs_site, tmp_path / "cache")

    side = scraper.field("4.4", "Side")
    listings = scraper.list_fields("4.4")

    assert side.tag == 54
    assert len({item.tag for item in listings}) == len(listings)
    assert {item.tag for item in listings} == {
        11,
        38,
        40,
        44,
        54,
        55,
        60,
        75,
        95,
        96,
        447,
        448,
        453,
    }


def test_index_status_is_normalized_into_field_metadata(
    onixs_site: Any, tmp_path: Path
) -> None:
    source = _scraper(onixs_site, tmp_path / "cache").field("4.4", "RawData")

    assert source.status == "Deprecated"
    assert source.metadata["fix.status"] == "Deprecated"


def test_selected_dictionary_fetches_in_parallel_and_persists_snapshot(
    onixs_site: Any, tmp_path: Path
) -> None:
    snapshot = tmp_path / "snapshots" / "fix-4.4.json"
    scraper = _scraper(onixs_site, tmp_path / "cache")

    dictionary = scraper.dictionary(
        "4.4",
        (60, 11, 54, 38),
        workers=4,
        persist_to=snapshot,
    )

    assert [item.tag for item in dictionary.fields] == [11, 38, 54, 60]
    assert dictionary.field("TransactTime").fix_type == "UTCTimestamp"
    assert FixDictionary.load(snapshot) == dictionary
    assert load_fix_dictionary(snapshot) == dictionary


def test_scraped_specs_include_stable_fix_metadata(
    onixs_site: Any, tmp_path: Path
) -> None:
    side = _scraper(onixs_site, tmp_path / "cache").field("4.4", 54)
    spec = side.into_spec(required=True)
    metadata = dict(field_options(spec.field).payload_metadata)

    assert metadata["fix.version"] == "4.4"
    assert metadata["fix.tag"] == 54
    assert metadata["fix.name"] == "Side"
    assert metadata["fix.type"] == "char"
    assert metadata["fix.source"] == "OnixS FIX Dictionary"
    assert metadata["fix.source_url"] == f"{BASE_URL}/4.4/tagNum_54.html"
    assert metadata["fix.values"] == (
        ("1", "Buy"),
        ("2", "Sell"),
        ("A", "Cross short exempt"),
    )


def test_unknown_fix_type_falls_back_to_string_but_preserves_metadata() -> None:
    from rkp.fix import FixField

    source = FixField(
        tag=9000,
        name="VendorValue",
        fix_type="VendorOpaque",
        version="4.4",
    )
    spec = source.into_spec(required=True)

    assert spec.annotation is str
    assert field_options(spec.field).payload_metadata["fix.type"] == "VendorOpaque"


def test_http_response_cache_survives_new_cache_instance_without_network(
    onixs_site: Any, tmp_path: Path
) -> None:
    cache_path = tmp_path / "cache"
    first = _scraper(onixs_site, cache_path)
    expected = first.field("4.4", 54)
    calls_after_fill = list(onixs_site.calls)

    second = _scraper(
        onixs_site,
        cache_path,
        opener=onixs_site.fail_on_request,
    )
    actual = second.field("4.4", 54, offline=True)

    assert actual == expected
    assert onixs_site.calls == calls_after_fill


def test_shared_cache_keeps_artifacts_isolated_by_source_origin(
    onixs_site: Any, tmp_path: Path
) -> None:
    cache = FixCache(tmp_path / "shared.sqlite3")
    first = OnixsFixScraper(
        cache,
        base_url="https://mirror-a.test/fix-dictionary",
        opener=onixs_site.urlopen,
        min_interval=0,
    )
    second = OnixsFixScraper(
        cache,
        base_url="https://mirror-b.test/fix-dictionary",
        opener=onixs_site.urlopen,
        min_interval=0,
    )

    first_ref = first.list_fields("4.4")[0]
    second_ref = second.list_fields("4.4")[0]

    assert first_ref.url.startswith("https://mirror-a.test/")
    assert second_ref.url.startswith("https://mirror-b.test/")
    assert second.field("4.4", 54).source_url.startswith("https://mirror-b.test/")


def test_memory_cache_avoids_reopening_disk_or_network(
    onixs_site: Any, tmp_path: Path
) -> None:
    scraper = _scraper(onixs_site, tmp_path / "cache")

    first = scraper.field("4.4", 54)
    second = scraper.field("4.4", 54)

    assert first == second
    assert len(onixs_site.calls) == 2


def test_refresh_and_zero_ttl_invalidate_cached_responses(
    onixs_site: Any, tmp_path: Path
) -> None:
    scraper = _scraper(onixs_site, tmp_path / "cache")
    scraper.field("4.4", 54)
    scraper.field("4.4", 54, refresh=True)
    assert len(onixs_site.calls) == 4

    tick = 0

    def advancing_clock() -> float:
        nonlocal tick
        tick += 1
        return float(tick)

    expiring = _scraper(
        onixs_site,
        tmp_path / "stale-cache",
        ttl=0,
        clock=advancing_clock,
    )
    expiring.field("4.4", 54)
    expiring.field("4.4", 54)
    assert len(onixs_site.calls) == 8


def test_conditional_304_touches_cached_timestamp_and_closes_response(
    onixs_site: Any, tmp_path: Path
) -> None:
    clock = [100.0]
    error_body = io.BytesIO(b"")
    conditional = False

    def opener(request: Any, **kwargs: object) -> object:
        if conditional:
            url = getattr(request, "full_url", str(request))
            raise HTTPError(url, 304, "not modified", {}, error_body)
        return onixs_site.urlopen(request, **kwargs)

    cache = FixCache(tmp_path / "cache")
    scraper = OnixsFixScraper(
        cache,
        base_url=BASE_URL,
        opener=opener,
        min_interval=0,
        retries=0,
        clock=lambda: clock[0],
    )
    scraper.list_fields("4.4")
    conditional = True
    clock[0] = 200.0
    scraper.list_fields("4.4", refresh=True)

    entry = cache.get_response(f"{BASE_URL}/4.4/fields_by_tag.html")
    assert entry is not None and entry.fetched_at == 200.0
    assert error_body.closed


def test_offline_mode_uses_stale_entry_but_fails_on_a_miss(
    onixs_site: Any, tmp_path: Path
) -> None:
    cache_path = tmp_path / "cache"
    online = _scraper(onixs_site, cache_path, ttl=0)
    expected = online.field("4.4", 54)

    offline = _scraper(
        onixs_site,
        cache_path,
        ttl=0,
        opener=onixs_site.fail_on_request,
    )
    assert offline.field("4.4", 54, offline=True) == expected
    with pytest.raises(FixCacheError, match=r"(?i)offline|cache"):
        offline.field("4.4", 11, offline=True)


def test_concurrent_requests_are_coalesced_to_one_fetch(
    onixs_site: Any, tmp_path: Path
) -> None:
    entered = threading.Event()
    release = threading.Event()
    fetch_count = 0
    count_lock = threading.Lock()

    def blocking_opener(*args: object, **kwargs: object) -> object:
        nonlocal fetch_count
        with count_lock:
            fetch_count += 1
        entered.set()
        assert release.wait(timeout=2), "test did not release the fixture response"
        return onixs_site.urlopen(*args, **kwargs)

    cache = FixCache(tmp_path / "cache")
    scraper = OnixsFixScraper(
        cache=cache,
        base_url=BASE_URL,
        opener=blocking_opener,
        timeout=1,
        min_interval=0,
    )
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(scraper.field, "4.4", 54) for _ in range(8)]
        assert entered.wait(timeout=2), "fixture opener was not called"
        release.set()
        values = [future.result(timeout=2) for future in futures]

    assert all(value == values[0] for value in values)
    assert fetch_count == 2
    assert len(onixs_site.calls) == 2
    assert sum(url.endswith("fields_by_tag.html") for url in onixs_site.calls) == 1
    assert sum(url.endswith("tagNum_54.html") for url in onixs_site.calls) == 1


def test_concurrent_cache_initialization_is_serialized(tmp_path: Path) -> None:
    target = tmp_path / "shared.sqlite3"

    def open_and_close(_: int) -> dict[str, int | str]:
        with FixCache(target) as cache:
            return cache.info()

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(open_and_close, range(32)))

    assert all(result["path"] == str(target) for result in results)
    with FixCache(target) as cache:
        assert cache.info()["responses"] == 0


def test_dictionary_snapshot_replacement_is_atomic_under_concurrent_writers(
    tmp_path: Path,
) -> None:
    from rkp.fix import FixField

    target = tmp_path / "dictionary.json"
    first = FixDictionary(
        version="4.4",
        fields=(FixField(11, "ClOrdID", "String", "4.4"),),
    )
    second = FixDictionary(
        version="4.4",
        fields=(FixField(54, "Side", "char", "4.4"),),
    )

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [
            pool.submit((first if index % 2 else second).dump, target)
            for index in range(32)
        ]
        for future in futures:
            future.result(timeout=2)

    restored = FixDictionary.load(target)
    assert restored == first or restored == second
    assert isinstance(json.loads(target.read_text(encoding="utf-8")), dict)
    assert not list(target.parent.glob(f".{target.name}.*"))


def test_malformed_field_page_has_a_contextual_parse_error(
    onixs_site: Any, tmp_path: Path
) -> None:
    def malformed_opener(request: object, **__: object) -> object:
        return onixs_site.serve_fixture(request, "malformed_field.html")

    scraper = _scraper(
        onixs_site,
        tmp_path / "cache",
        opener=malformed_opener,
    )

    with pytest.raises(FixParseError, match=r"(?i)tag|type|field"):
        scraper.field("4.4", 999)


def test_field_page_version_mismatch_is_rejected(
    onixs_site: Any, tmp_path: Path
) -> None:
    def mismatched_opener(request: Any, **kwargs: object) -> object:
        if request.full_url.endswith("tagNum_54.html"):
            return onixs_site.serve_fixture(request, "5.0.sp2.ep302/tagNum_54.html")
        return onixs_site.urlopen(request, **kwargs)

    scraper = _scraper(onixs_site, tmp_path / "cache", opener=mismatched_opener)

    with pytest.raises(FixParseError, match=r"(?i)version.*4\.4"):
        scraper.field("4.4", 54)


def test_conflicting_index_names_are_rejected(onixs_site: Any, tmp_path: Path) -> None:
    def malformed_opener(request: object, **__: object) -> object:
        return onixs_site.serve_fixture(request, "malformed_index.html")

    scraper = _scraper(
        onixs_site,
        tmp_path / "cache",
        opener=malformed_opener,
    )

    with pytest.raises(FixParseError, match=r"(?i)conflicting|name|tag 11"):
        scraper.list_fields("4.4")


def test_transport_errors_are_wrapped_with_url_context(
    onixs_site: Any, tmp_path: Path
) -> None:
    def failing_opener(request: Any, **_: object) -> object:
        url = getattr(request, "full_url", str(request))
        raise HTTPError(url, 503, "unavailable", {}, None)

    scraper = _scraper(onixs_site, tmp_path / "cache", opener=failing_opener)

    with pytest.raises(FixFetchError, match=r"tagNum_54|503|fetch"):
        scraper.field("4.4", 54)


def test_retryable_errors_close_responses_and_use_verified_stale_cache(
    onixs_site: Any, tmp_path: Path
) -> None:
    failing = False
    bodies: list[io.BytesIO] = []

    def opener(request: Any, **kwargs: object) -> object:
        if not failing:
            return onixs_site.urlopen(request, **kwargs)
        url = getattr(request, "full_url", str(request))
        body = io.BytesIO(b"unavailable")
        bodies.append(body)
        raise HTTPError(url, 503, "unavailable", {}, body)

    scraper = OnixsFixScraper(
        FixCache(tmp_path / "cache"),
        base_url=BASE_URL,
        opener=opener,
        min_interval=0,
        retries=0,
    )
    expected = scraper.field("4.4", 54)
    failing = True

    assert scraper.field("4.4", 54, refresh=True) == expected
    assert bodies and all(body.closed for body in bodies)


@pytest.mark.parametrize(
    ("body", "content_type", "maximum", "message"),
    [
        (b"x" * 65, "text/html", 64, "exceeds"),
        (b"{}", "application/json", 64, "expected HTML"),
    ],
)
def test_response_limits_and_content_type_are_enforced(
    tmp_path: Path,
    body: bytes,
    content_type: str,
    maximum: int,
    message: str,
) -> None:
    def opener(request: Any, **_: object) -> _Response:
        return _Response(body, request.full_url, content_type)

    scraper = OnixsFixScraper(
        FixCache(tmp_path / "cache"),
        base_url=BASE_URL,
        opener=opener,
        min_interval=0,
        max_response_bytes=maximum,
    )

    with pytest.raises(FixFetchError, match=message):
        scraper.list_fields("4.4")


@pytest.mark.parametrize(
    "href",
    [
        "https://example.test/fix-dictionary/tagNum_54.html",
        "https://www.onixs.biz:bad/fix-dictionary/tagNum_54.html",
        "https://www.onixs.biz:99999/fix-dictionary/tagNum_54.html",
        "%2e%2e/private/tagNum_54.html",
        "%252e%252e/private/tagNum_54.html",
        "..%5cprivate/tagNum_54.html",
    ],
)
def test_untrusted_index_urls_cannot_escape_dictionary_path(
    tmp_path: Path, href: str
) -> None:
    index = (f"<html><a href='{href}'>54</a><a href='{href}'>Side</a></html>").encode()

    def opener(request: Any, **_: object) -> _Response:
        if request.full_url.endswith("fields_by_tag.html"):
            return _Response(index, request.full_url)
        raise AssertionError("unsafe detail URL reached the transport")

    scraper = OnixsFixScraper(
        FixCache(tmp_path / "cache"),
        base_url=BASE_URL,
        opener=opener,
        min_interval=0,
    )

    with pytest.raises(FixFetchError, match="refusing URL"):
        scraper.field("4.4", 54)


def test_cache_checksum_corruption_is_detected(tmp_path: Path) -> None:
    path = tmp_path / "cache.sqlite3"
    url = f"{BASE_URL}/4.4/fields_by_tag.html"
    with FixCache(path) as cache:
        cache.put_response(url, b"valid")
    with sqlite3.connect(path) as connection:
        connection.execute("UPDATE responses SET body = x'00' WHERE url = ?", (url,))

    with FixCache(path) as cache, pytest.raises(FixCacheError, match="corrupt"):
        cache.get_response(url)


def test_normalized_artifact_family_replaces_obsolete_digests(
    tmp_path: Path,
) -> None:
    with FixCache(tmp_path / "cache.sqlite3") as cache:
        cache.put_artifact("field:54:first", b"first", family="field:54:")
        cache.put_artifact("field:54:second", b"second", family="field:54:")

        assert cache.get_artifact("field:54:first") is None
        assert cache.get_artifact("field:54:second") == b"second"
        assert cache.info()["artifacts"] == 1


@pytest.mark.parametrize("version", ["", "../4.4", "4.4/../../x", "six"])
def test_invalid_versions_are_rejected_before_network(
    version: str, onixs_site: Any, tmp_path: Path
) -> None:
    scraper = _scraper(onixs_site, tmp_path / "cache")

    with pytest.raises(FixVersionError, match=r"(?i)version"):
        scraper.field(version, 54)
    assert not onixs_site.calls


def test_invalid_bulk_worker_count_is_rejected_before_network(
    onixs_site: Any, tmp_path: Path
) -> None:
    scraper = _scraper(onixs_site, tmp_path / "cache")

    with pytest.raises(TypeError, match="workers"):
        scraper.scrape_all("4.4", workers=0)
    assert not onixs_site.calls
