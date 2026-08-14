"""Platform-neutral default locations for persistent FIX metadata."""

from __future__ import annotations

import os
import re
from pathlib import Path

__all__ = ["default_fix_cache_path", "default_fix_dictionary_path", "fix_home"]


def fix_home() -> Path:
    """Return the configured FIX metadata directory without creating it.

    ``RKP_FIX_HOME`` has highest precedence, followed by ``XDG_CONFIG_HOME``.
    The portable fallback is ``~/.config/fix`` on every supported platform.
    """

    configured = os.environ.get("RKP_FIX_HOME")
    if configured:
        return Path(configured).expanduser()
    config_root = os.environ.get("XDG_CONFIG_HOME")
    if config_root:
        return Path(config_root).expanduser() / "fix"
    return Path.home() / ".config" / "fix"


def default_fix_cache_path() -> Path:
    """Return the default SQLite response/artifact cache path."""

    configured = os.environ.get("RKP_FIX_CACHE")
    if configured:
        return Path(configured).expanduser()
    return fix_home() / "cache-v1.sqlite3"


def default_fix_dictionary_path(version: str) -> Path:
    """Return the default portable catalog snapshot path for a FIX edition."""

    if not isinstance(version, str) or not version.strip():
        raise TypeError("FIX version must be a non-empty string")
    slug = re.sub(r"[^a-z0-9]+", "-", version.casefold()).strip("-")
    return fix_home() / "dictionaries" / f"fix-{slug}.json.gz"
