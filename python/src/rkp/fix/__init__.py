"""Cache-first OnixS FIX fields and structures for RKP records and Arrow.

Importing this package never performs network I/O. Use
:class:`OnixsFixScraper` for selected online hydration, then persist a portable
:class:`FixDictionary` snapshot for reproducible offline builds.
"""

from __future__ import annotations

import os

from ._cache import FixCache, FixCacheEntry
from ._errors import (
    FixCacheError,
    FixError,
    FixFetchError,
    FixParseError,
    FixVersionError,
)
from ._html import FixComponentRef, FixFieldRef, FixMessageRef
from ._models import (
    FixComponent,
    FixComponentMember,
    FixDictionary,
    FixEnumValue,
    FixField,
    FixFieldMember,
    FixFieldSpec,
    FixMessage,
    FixRepeatingGroup,
    FixStructureMember,
)
from ._paths import default_fix_cache_path, default_fix_dictionary_path, fix_home
from ._scraper import OnixsFixScraper, scrape_onixs_fields

__all__ = [
    "FixCache",
    "FixCacheEntry",
    "FixCacheError",
    "FixComponent",
    "FixComponentMember",
    "FixComponentRef",
    "FixDictionary",
    "FixEnumValue",
    "FixError",
    "FixFetchError",
    "FixField",
    "FixFieldMember",
    "FixFieldRef",
    "FixFieldSpec",
    "FixMessage",
    "FixMessageRef",
    "FixParseError",
    "FixRepeatingGroup",
    "FixStructureMember",
    "FixVersionError",
    "OnixsFixScraper",
    "default_fix_cache_path",
    "default_fix_dictionary_path",
    "fix_home",
    "load_default_fix_dictionary",
    "load_fix_dictionary",
    "scrape_onixs_fields",
]


def load_fix_dictionary(source: str | os.PathLike[str]) -> FixDictionary:
    """Load a validated portable FIX dictionary snapshot."""

    return FixDictionary.load(source)


def load_default_fix_dictionary(version: str) -> FixDictionary:
    """Load a FIX dictionary from the default persistent metadata folder."""

    return FixDictionary.load_default(version)
