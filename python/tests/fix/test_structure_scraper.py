from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from rkp import into_glue_partition_values
from rkp.fix import (
    FixComponentMember,
    FixComponentRef,
    FixRepeatingGroup,
    OnixsFixScraper,
)
from rkp.fix._errors import FixParseError
from rkp.fix._html import parse_component_detail, parse_message_index

BASE_URL = "https://fixture.test/fix-dictionary/"


def _scraper(site: Any, path: Path) -> OnixsFixScraper:
    return OnixsFixScraper(
        path,
        base_url=BASE_URL,
        opener=site.urlopen,
        min_interval=0,
    )


def test_message_and_component_structures_are_fetched_recursively(
    onixs_site: Any, tmp_path: Path
) -> None:
    scraper = _scraper(onixs_site, tmp_path / "fix.sqlite3")

    references = scraper.list_messages("4.4")
    assert [(item.msg_type, item.name) for item in references] == [
        ("D", "New Order Single"),
        ("W", "Market Data Snapshot Full Refresh"),
    ]

    dictionary = scraper.dictionary("4.4", messages=["W"])
    message = dictionary.message("W")
    parties = dictionary.component("Parties")

    assert message.members[1] == FixComponentMember("Parties")
    with pytest.raises(KeyError, match="unknown FIX message"):
        dictionary.message("w")
    assert {component.name for component in dictionary.components} == {
        "Instrument",
        "Parties",
        "StandardHeader",
        "StandardTrailer",
    }
    assert not any("UnrelatedReverseReference" in url for url in onixs_site.calls)
    assert {field.tag for field in dictionary.fields} == {55, 75, 447, 448, 453}
    assert parties.members == (
        FixRepeatingGroup(
            453,
            (
                dictionary.component("Parties").members[0].members[0],
                dictionary.component("Parties").members[0].members[1],
                FixComponentMember(
                    "Instrument", comment="Optional instrument context."
                ),
            ),
            comment="Repeating party entries.",
        ),
    )


def test_scraped_structure_uses_the_record_arrow_hub(
    onixs_site: Any, tmp_path: Path
) -> None:
    scraper = _scraper(onixs_site, tmp_path / "fix.sqlite3")
    dictionary = scraper.dictionary("4.4", messages=["W"])
    record_type = dictionary.into_message_record("W")

    record_value = record_type.from_dict(
        {
            "StandardHeader": {},
            "Parties": {
                "NoPartyIDs": [
                    {
                        "PartyID": "CLIENT-1",
                        "PartyIDSource": "D",
                        "Instrument": {"Symbol": "MSFT"},
                    }
                ]
            },
            "StandardTrailer": {},
        }
    )
    batch = record_type.into_arrow_batch([record_value])
    schema = batch.schema

    group = schema.field("Parties").type.field("NoPartyIDs")
    assert pa.types.is_list(group.type)
    assert pa.types.is_struct(group.type.value_type)
    assert group.metadata[b"PARQUET:field_id"] == b"453"
    assert group.metadata[b"fix.repeating"] == b"true"
    assert (
        group.type.value_type.field("PartyID").metadata[b"PARQUET:field_id"] == b"448"
    )
    assert list(record_type.from_arrow_batch(batch)) == [record_value]
    assert record_type.loads_json(record_value.dumps_json()) == record_value
    assert record_type.loads_yaml(record_value.dumps_yaml()) == record_value
    assert record_type.loads_json(record_value.dumps_json_bytes()) == record_value
    assert record_type.loads_yaml(record_value.dumps_yaml_bytes()) == record_value


def test_fix_fields_drive_glue_partition_values_through_arrow(
    onixs_site: Any, tmp_path: Path
) -> None:
    scraper = _scraper(onixs_site, tmp_path / "fix.sqlite3")
    dictionary = scraper.dictionary("4.4", tags=[75])
    Partition = dictionary.into_record("Partition", required=[75])
    value = Partition(trade_date=date(2026, 8, 14))

    assert into_glue_partition_values(value, partition_keys=["TradeDate"]) == [
        "2026-08-14"
    ]


def test_structure_artifacts_are_available_fully_offline(
    onixs_site: Any, tmp_path: Path
) -> None:
    cache_path = tmp_path / "fix.sqlite3"
    with _scraper(onixs_site, cache_path) as online:
        expected = online.dictionary("4.4", messages=["W"])
    calls = tuple(onixs_site.calls)

    with OnixsFixScraper(
        cache_path,
        base_url=BASE_URL,
        opener=onixs_site.fail_on_request,
        min_interval=0,
    ) as offline:
        actual = offline.dictionary("4.4", messages=["W"], offline=True)

    assert actual == expected
    assert tuple(onixs_site.calls) == calls


def test_modern_reversed_index_and_unicode_group_depth_are_parsed() -> None:
    page_url = "https://www.onixs.biz/fix-dictionary/latest/messages.html"
    index = b"""<html><head><link rel='canonical'
        href='../5.0.sp2.ep302/messages.html'></head><body>
        <table><tr><th>Name</th><th>MsgType</th></tr>
        <tr><td><a href='msgType_W_87.html'>Snapshot</a></td>
        <td><a href='msgType_W_87.html'>W</a></td></tr></table></body></html>"""
    references, canonical = parse_message_index(
        index, page_url=page_url, version="latest"
    )

    assert references[0].msg_type == "W"
    assert references[0].name == "Snapshot"
    assert canonical == "../5.0.sp2.ep302/messages.html"

    detail = """<html><body><h1>PartyGrp component block</h1>
        <h2>FIX 5.0 SP2 EP302</h2><h3>Structure</h3><table>
        <tr><th colspan='2'>Tag</th><th>Field Name</th><th>Req'd</th><th>Comments</th></tr>
        <tr><td colspan='2'>453</td><td><a href='tagNum_453.html'>NoPartyIDs</a></td><td>N</td><td></td></tr>
        <tr><td>→</td><td><a href='tagNum_448.html'>PartyID</a></td><td>Y</td><td>Identifier</td></tr>
        </table></body></html>""".encode()
    component = parse_component_detail(
        detail,
        reference=FixComponentRef(
            "PartyGrp",
            "https://www.onixs.biz/fix-dictionary/5.0.sp2.ep302/compBlock_PartyGrp.html",
            "5.0.SP2 EP302",
        ),
    )

    assert component.members == (
        FixRepeatingGroup(
            453,
            (component.members[0].members[0],),
        ),
    )
    assert component.members[0].members[0].required is True
    assert component.members[0].members[0].comment == "Identifier"


def test_structure_depth_jump_is_rejected() -> None:
    detail = """<html><body><h1>Broken component block</h1><h2>FIX 5.0 SP2 EP302</h2>
        <h3>Structure</h3><table>
        <tr><th colspan='2'>Tag</th><th>Field Name</th><th>Req'd</th><th>Comments</th></tr>
        <tr><td>→</td><td>→</td><td><a href='tagNum_448.html'>PartyID</a></td><td>Y</td><td></td></tr>
        </table></body></html>""".encode()

    with pytest.raises(FixParseError, match="depth jump"):
        parse_component_detail(
            detail,
            reference=FixComponentRef(
                "Broken",
                "https://www.onixs.biz/fix-dictionary/5.0.sp2.ep302/compBlock_Broken.html",
                "5.0.SP2 EP302",
            ),
        )
