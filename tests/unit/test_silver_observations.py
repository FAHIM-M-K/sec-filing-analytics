import gzip
import json
import os
import tempfile

import pytest

from sec_filing_analytics.silver.observations import parse_facts_payload, _TAG_TO_METRIC, CANONICAL_TAGS


SAMPLE_FACTS = {
    "cik": "0000320193",
    "facts": {
        "us-gaap": {
            "RevenueFromContractWithCustomerExcludingAssessedTax": {
                "units": {
                    "USD": [
                        {"accn": "0000320193-24-000123", "end": "2024-09-28", "val": 391035000000,
                         "form": "10-K", "fp": "FY", "fy": 2024, "filed": "2024-11-01",
                         "start": "2023-10-01"},
                        {"accn": "0000320193-24-000123", "end": "2024-09-28", "val": 391035000000,
                         "form": "10-K", "fp": "FY", "fy": 2024, "filed": "2024-11-15",
                         "start": "2023-10-01"},
                        {"accn": "0000320193-20-000050", "end": "2009-09-26", "val": 36537000000,
                         "form": "10-K", "fp": "FY", "fy": 2009, "filed": "2009-10-27",
                         "start": "2008-09-28"},
                    ]
                }
            },
            "Assets": {
                "units": {
                    "USD": [
                        {"accn": "0000320193-24-000123", "end": "2024-09-28", "val": 364980000000,
                         "form": "10-K", "fp": "FY", "fy": 2024, "filed": "2024-11-01"},
                    ]
                }
            },
            "SomeRandomTag": {
                "units": {
                    "USD": [
                        {"end": "2024-09-28", "val": 999, "form": "10-K", "fp": "FY", "fy": 2024,
                         "filed": "2024-11-01", "start": "2023-10-01"},
                    ]
                }
            },
        }
    }
}


def _write_gz(path: str, data: dict) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as f:
        json.dump(data, f)


def test_canonical_tag_map_covers_all_metrics():
    for metric, tags in CANONICAL_TAGS.items():
        for tag in tags:
            assert tag in _TAG_TO_METRIC, f"{tag} missing from reverse map"
            assert _TAG_TO_METRIC[tag] == metric


def test_parse_excludes_unknown_tags():
    with tempfile.NamedTemporaryFile(suffix=".gz", delete=False) as tmp:
        path = tmp.name
    try:
        _write_gz(path, SAMPLE_FACTS)
        rows = parse_facts_payload(path, "h")
        tags = {r["tag"] for r in rows}
        assert "SomeRandomTag" not in tags
    finally:
        os.unlink(path)


def test_parse_excludes_pre_2010():
    with tempfile.NamedTemporaryFile(suffix=".gz", delete=False) as tmp:
        path = tmp.name
    try:
        _write_gz(path, SAMPLE_FACTS)
        rows = parse_facts_payload(path, "h")
        for r in rows:
            assert r["period_end"] >= "2010-01-01", f"Pre-2010 leaked: {r}"
    finally:
        os.unlink(path)


def test_parse_instant_vs_duration():
    with tempfile.NamedTemporaryFile(suffix=".gz", delete=False) as tmp:
        path = tmp.name
    try:
        _write_gz(path, SAMPLE_FACTS)
        rows = parse_facts_payload(path, "h")
        revenue_rows = [r for r in rows if r["tag"] == "RevenueFromContractWithCustomerExcludingAssessedTax"]
        assets_rows  = [r for r in rows if r["tag"] == "Assets"]
        assert all(r["period_type"] == "duration" for r in revenue_rows)
        assert all(r["period_type"] == "instant"  for r in assets_rows)
    finally:
        os.unlink(path)


def test_parse_sets_canonical_metric():
    with tempfile.NamedTemporaryFile(suffix=".gz", delete=False) as tmp:
        path = tmp.name
    try:
        _write_gz(path, SAMPLE_FACTS)
        rows = parse_facts_payload(path, "h")
        revenue_rows = [r for r in rows if r["tag"] == "RevenueFromContractWithCustomerExcludingAssessedTax"]
        assert all(r["metric"] == "Revenue" for r in revenue_rows)
    finally:
        os.unlink(path)


def test_parse_includes_restatements_before_dedup():
    # parse_facts_payload returns ALL rows including duplicates;
    # deduplication happens in build_silver_observations (Spark level)
    with tempfile.NamedTemporaryFile(suffix=".gz", delete=False) as tmp:
        path = tmp.name
    try:
        _write_gz(path, SAMPLE_FACTS)
        rows = parse_facts_payload(path, "h")
        revenue_rows = [r for r in rows if r["tag"] == "RevenueFromContractWithCustomerExcludingAssessedTax"
                        and r["period_end"] >= "2010-01-01"]
        # Both the original and restatement rows should be present before dedup
        assert len(revenue_rows) == 2
    finally:
        os.unlink(path)

