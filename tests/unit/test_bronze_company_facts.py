import gzip
import json
from datetime import datetime, timezone

from sec_filing_analytics.bronze.company_facts import (
    CompanyFactsFetchResult,
    fetch_company_facts,
    land_company_facts_payload,
)


SAMPLE_PAYLOAD = {
    "entityName": "Apple Inc.",
    "facts": {
        "us-gaap": {
            "Assets": {"label": "Assets", "units": {"USD": []}},
            "NetIncomeLoss": {"label": "Net Income", "units": {"USD": []}},
        },
        "dei": {},
    },
}


class _FakeClient:
    def get_company_facts(self, cik):
        return SAMPLE_PAYLOAD


def _result(**overrides):
    base = dict(
        cik="0000320193",
        entity_name="Apple Inc.",
        source_url="https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json",
        fetched_at=datetime(2026, 11, 8, tzinfo=timezone.utc),
        content=json.dumps(SAMPLE_PAYLOAD),
        content_hash="def456",
        response_size_bytes=200,
        us_gaap_tag_count=2,
        taxonomies=["dei", "us-gaap"],
    )
    base.update(overrides)
    return CompanyFactsFetchResult(**base)


def test_fetch_returns_correct_metadata():
    r = fetch_company_facts("0000320193", _FakeClient())
    assert r.entity_name == "Apple Inc."
    assert r.us_gaap_tag_count == 2
    assert r.taxonomies == ["dei", "us-gaap"]
    assert len(r.content_hash) == 64


def test_land_payload_writes_readable_gzip(tmp_path):
    r = _result(content=json.dumps(SAMPLE_PAYLOAD), content_hash="testhash2")
    written = land_company_facts_payload(r, tmp_path)
    assert written.exists()
    with gzip.open(written, "rt", encoding="utf-8") as fh:
        loaded = json.loads(fh.read())
    assert loaded["entityName"] == "Apple Inc."

