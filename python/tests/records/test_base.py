from __future__ import annotations

from datetime import UTC, datetime

import pytest
from rkp import Record, field, record


@record
class BaseChild(Record):
    value: int


@record(alias="payloads")
class BasePayload(Record):
    identifier: int = field(alias="id")
    child: BaseChild = field()
    labels: set[str] = field(default_factory=set)
    created_at: datetime | None = None


def test_from_dict_casts_nested_values_and_honors_aliases() -> None:
    value = BasePayload.from_dict(
        {
            "id": "7",
            "child": {"value": "3"},
            "labels": [1, "two"],
            "created_at": "2026-08-13T10:00:00",
        }
    )

    assert value == BasePayload(
        identifier=7,
        child=BaseChild(3),
        labels={"1", "two"},
        created_at=datetime(2026, 8, 13, 10, tzinfo=UTC),
    )


def test_from_dict_unsafe_forwards_values_and_safe_rejects_unknowns() -> None:
    unsafe = BaseChild.from_dict({"value": "unchanged"}, safe=False)
    assert unsafe.value == "unchanged"

    with pytest.raises(TypeError, match="unexpected field"):
        BaseChild.from_dict({"value": 1, "extra": 2})


def test_from_dict_default_error_policy_uses_defaults_and_zero_values() -> None:
    @record
    class Defaults(Record):
        count: int
        title: str = "fallback"
        values: list[int] = field(default_factory=lambda: [1])

    converted = Defaults.from_dict(
        {"count": "bad", "title": None, "values": object()},
        on_error="default",
    )

    assert converted == Defaults(count=0, title="fallback", values=[1])
    assert Defaults.from_dict({}, on_error="default") == Defaults(
        count=0, title="fallback", values=[1]
    )
