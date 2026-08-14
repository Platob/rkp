from __future__ import annotations

import runpy
from pathlib import Path

import pytest

EXAMPLES = Path(__file__).resolve().parents[2] / "docs" / "examples"


@pytest.mark.parametrize("path", sorted(EXAMPLES.glob("*.py")), ids=lambda p: p.stem)
def test_documented_example_runs_without_external_services(
    path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("RKP_RUN_SPARK_EXAMPLE", raising=False)
    monkeypatch.delenv("RKP_TEST_POSTGRES_URI", raising=False)
    runpy.run_path(str(path), run_name="__main__")
