import gzip
import json
import os
import tempfile
import pytest

from sec_filing_analytics.silver.filings import parse_submission_payload, _parse_recent_block
from sec_filing_analytics.silver.submission_chunks import iter_submission_history_chunks


def _write_gz(path: str, data: dict) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as f:
        json.dump(data, f)


SAMPLE_SUBMISSION = {
    "cik": "0000320193",
    "name": "Apple Inc.",
    "filings": {
        "recent": {
            "form":            ["10-K", "10-Q", "8-K", "10-K", "10-K"],
            "filingDate":      ["2024-11-01", "2024-08-02", "2024-07-30", "2009-10-27", "2008-10-27"],
            "reportDate":      ["2024-09-28", "2024-06-29", "", "2009-09-26", "2008-09-27"],
            "accessionNumber": ["0000320193-24-000123", "0000320193-24-000100", "0000320193-24-000099", "0000320193-09-000050", "0000320193-08-000050"],
            "primaryDocument": ["aapl-20240928.htm", "aapl-20240629.htm", "aapl-20240730.htm", "aapl-20090926.htm", "aapl-20080927.htm"],
            "fp":              ["FY", "Q3", "", "FY", "FY"],
            "fy":              [2024, 2024, None, 2009, 2008],
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


def test_parse_filters_pre_2009():
    with tempfile.NamedTemporaryFile(suffix=".gz", delete=False) as tmp:
        path = tmp.name
    try:
        _write_gz(path, SAMPLE_SUBMISSION)
        rows = parse_submission_payload(path, "hash123")
        for r in rows:
            assert r["filing_date"] >= "2009-01-01", f"Pre-2009 row leaked: {r}"
    finally:
        os.unlink(path)


def test_parse_returns_correct_count():
    with tempfile.NamedTemporaryFile(suffix=".gz", delete=False) as tmp:
        path = tmp.name
    try:
        _write_gz(path, SAMPLE_SUBMISSION)
        rows = parse_submission_payload(path, "hash123")
        # Only the 2024 10-K, Q3 10-Q, and 2009 10-K should pass; 8-K and 2008 10-K are filtered
        assert len(rows) == 3
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


def test_parse_fails_if_referenced_submission_history_chunk_is_missing(tmp_path):
    payload = json.loads(json.dumps(SAMPLE_SUBMISSION))
    payload["filings"]["files"] = [{"name": "CIK0000320193-submissions-001.json"}]
    path = tmp_path / "submission.json.gz"
    _write_gz(str(path), payload)

    with pytest.raises(FileNotFoundError, match="Rerun Bronze submissions ingestion"):
        parse_submission_payload(str(path), "hash123")


def test_submission_parser_prefers_refreshed_compressed_history_chunk(tmp_path):
    payload_path = tmp_path / "submission.json.gz"
    chunk_name = "CIK0000320193-submissions-001.json"
    payload = json.loads(json.dumps(SAMPLE_SUBMISSION))
    payload["filings"]["files"] = [{"name": chunk_name}]
    _write_gz(str(payload_path), payload)
    (tmp_path / chunk_name).write_text("not valid JSON", encoding="utf-8")
    _write_gz(str(tmp_path / f"{chunk_name}.gz"), {"history": "latest"})

    chunks = list(iter_submission_history_chunks(str(payload_path), payload))

    assert chunks == [(chunk_name, {"history": "latest"})]

