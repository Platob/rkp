from __future__ import annotations

import io
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar, Self
from urllib.error import HTTPError
from urllib.parse import urlsplit

import pytest

FIXTURES = Path(__file__).with_name("fixtures") / "onixs"


class FixtureResponse(io.BytesIO):
    """Small ``urlopen``-compatible response backed by a local fixture."""

    status = 200
    headers: ClassVar[dict[str, str]] = {"Content-Type": "text/html; charset=utf-8"}

    def __init__(self, payload: bytes, url: str) -> None:
        super().__init__(payload)
        self.url = url

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def geturl(self) -> str:
        return self.url


@dataclass(slots=True)
class FixtureSite:
    """Deterministic stand-in for the subset of OnixS used by the tests."""

    root: Path = FIXTURES
    calls: list[str] = field(default_factory=list)

    def read_text(self, relative_path: str) -> str:
        return (self.root / relative_path).read_text(encoding="utf-8")

    def serve_fixture(self, request: Any, relative_path: str) -> FixtureResponse:
        """Serve one chosen body while preserving the requested final URL."""

        url = getattr(request, "full_url", str(request))
        self.calls.append(url)
        return FixtureResponse((self.root / relative_path).read_bytes(), url)

    def urlopen(
        self,
        request: Any,
        data: object = None,
        timeout: float | None = None,
        **_: object,
    ) -> FixtureResponse:
        del data, timeout
        url = getattr(request, "full_url", str(request))
        self.calls.append(url)
        path = urlsplit(url).path.lstrip("/")
        if path.startswith("fix-dictionary/"):
            path = path.removeprefix("fix-dictionary/")
        target = (self.root / path).resolve()
        root = self.root.resolve()
        if root not in target.parents or not target.is_file():
            raise HTTPError(url, 404, "fixture not found", {}, None)
        return FixtureResponse(target.read_bytes(), url)

    def fail_on_request(self, *_: object, **__: object) -> FixtureResponse:
        raise AssertionError("the HTTP transport was called on a cache hit")


@pytest.fixture
def onixs_site() -> Iterator[FixtureSite]:
    yield FixtureSite()
