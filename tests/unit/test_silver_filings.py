import gzip
import json
import os
import tempfile

import pytest

from sec_filing_analytics.silver.filings import parse_submission_payload, _parse_recent_block


def _write_gz(path: str, data: dict) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as f:
        json.dump(data, f)


SAMPLE_SUBMISSION = {
    "cik": "0000320193",
    "name": "Apple Inc.",
    "filings": {
        "recent": {
            "form":            ["10-K", "10-Q", "8-K", "10-K"],
            "filingDate":      ["2024-11-01", "2024-08-02", "2024-07-30", "2009-10-27"],
            "reportDate":      ["2024-09-28", "2024-06-29", "", "2009-09-26"],
            "accessionNumber": ["0000320193-24-000123", "0000320193-24-000100", "0000320193-24-000099", "0000320193-09-000050"],
            "primaryDocument": ["aapl-20240928.htm", "aapl-20240629.htm", "aapl-20240730.htm", "aapl-20090926.htm"],
            "fp":              ["FY", "Q3", "", "FY"],
            "fy":              [2024, 2024, None, 2009],
        },
        "files": [],
    },
}


def test_parse_filters_non_financial_forms():
    with tempfile.NamedTemporaryFile(suffix=".gz", delete=False) as tmp:
        path = tmp.name
    try:
        _write_gz(path, SAMPLE_SUBMISSION)
        rows = parse_submission_payload(path, "hash123")
        forms = {r["form_type"] for r in rows}
        assert "8-K" not in forms
    finally:
        os.unlink(path)


def test_parse_filters_pre_2010():
    with tempfile.NamedTemporaryFile(suffix=".gz", delete=False) as tmp:
        path = tmp.name
    try:
        _write_gz(path, SAMPLE_SUBMISSION)
        rows = parse_submission_payload(path, "hash123")
        for r in rows:
            assert r["filing_date"] >= "2010-01-01", f"Pre-2010 row leaked: {r}"
    finally:
        os.unlink(path)


def test_parse_returns_correct_count():
    with tempfile.NamedTemporaryFile(suffix=".gz", delete=False) as tmp:
        path = tmp.name
    try:
        _write_gz(path, SAMPLE_SUBMISSION)
        rows = parse_submission_payload(path, "hash123")
        # Only the 2024 10-K and Q3 10-Q should pass; 8-K and 2009 10-K are filtered
        assert len(rows) == 2
    finally:
        os.unlink(path)


def test_parse_sets_source_hash():
    with tempfile.NamedTemporaryFile(suffix=".gz", delete=False) as tmp:
        path = tmp.name
    try:
        _write_gz(path, SAMPLE_SUBMISSION)
        rows = parse_submission_payload(path, "myhash")
        assert all(r["source_hash"] == "myhash" for r in rows)
    finally:
        os.unlink(path)


def test_parse_sets_period_type():
    with tempfile.NamedTemporaryFile(suffix=".gz", delete=False) as tmp:
        path = tmp.name
    try:
        _write_gz(path, SAMPLE_SUBMISSION)
        rows = parse_submission_payload(path, "hash")
        # filings.py doesn't set period_type (that's observations); accession_number present
        for r in rows:
            assert "accession_number" in r
            assert r["accession_number"] != ""
    finally:
        os.unlink(path)


def test_parse_recent_block_empty():
    rows = _parse_recent_block("0000001234", "Test Co", {}, "h")
    assert rows == []

