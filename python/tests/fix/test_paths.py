from __future__ import annotations

from pathlib import Path

from rkp.fix import (
    FixCache,
    FixDictionary,
    default_fix_cache_path,
    default_fix_dictionary_path,
    fix_home,
)


def test_default_fix_home_is_portable_config_folder(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("RKP_FIX_HOME", raising=False)
    monkeypatch.delenv("RKP_FIX_CACHE", raising=False)
    monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)

    assert fix_home() == tmp_path / ".config" / "fix"
    assert default_fix_cache_path() == fix_home() / "cache-v1.sqlite3"
    assert default_fix_dictionary_path("5.0.SP2 EP302") == (
        fix_home() / "dictionaries" / "fix-5-0-sp2-ep302.json.gz"
    )


def test_default_cache_and_snapshot_paths_are_created_lazily(
    monkeypatch, tmp_path: Path
) -> None:
    home = tmp_path / "metadata"
    monkeypatch.setenv("RKP_FIX_HOME", str(home))
    assert not home.exists()

    with FixCache() as cache:
        assert cache.info()["path"] == str(home / "cache-v1.sqlite3")
    snapshot = FixDictionary("4.4", ()).persist()

    assert snapshot == home / "dictionaries" / "fix-4-4.json.gz"
    assert FixDictionary.load_default("4.4") == FixDictionary("4.4", ())
