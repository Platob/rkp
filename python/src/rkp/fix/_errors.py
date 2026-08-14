"""Errors raised by the FIX dictionary adapter."""

from __future__ import annotations

__all__ = [
    "FixCacheError",
    "FixError",
    "FixFetchError",
    "FixParseError",
    "FixVersionError",
]


class FixError(Exception):
    """Base error for FIX dictionary operations."""


class FixFetchError(FixError):
    """A dictionary page could not be fetched safely."""


class FixParseError(FixError):
    """A dictionary page or portable snapshot was malformed."""


class FixCacheError(FixError):
    """A local cache operation failed or an offline entry was missing."""


class FixVersionError(FixError):
    """A FIX dictionary version is unsupported or malformed."""
