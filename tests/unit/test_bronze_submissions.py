import gzip
import json
from datetime import datetime, timezone

from sec_filing_analytics.bronze.submissions import (
    SubmissionFetchResult,
    fetch_submission,
    land_submission_history_files,
    land_submission_payload,
)


SAMPLE_PAYLOAD = {
    "cik": "0000320193",
    "name": "Apple Inc.",
    "filings": {
        "recent": {"accessionNumber": ["0001234567-26-000001", "0001234567-26-000002"]},
        "files": [{"name": "CIK0000320193-submissions-001.json"}],
    },
}


class _FakeClient:
    def get_submissions(self, cik):
        return SAMPLE_PAYLOAD

    def get_submission_history_file(self, filename):
        assert filename == "CIK0000320193-submissions-001.json"
        return {"filings": {"recent": {"form": ["10-K"]}}}


def _result(**overrides):
    base = dict(
        cik="0000320193",
        entity_name="Apple Inc.",
        source_url="https://data.sec.gov/submissions/CIK0000320193.json",
        fetched_at=datetime(2026, 11, 8, tzinfo=timezone.utc),
        content=json.dumps(SAMPLE_PAYLOAD),
        content_hash="abc123",
        response_size_bytes=100,
        filing_count_recent=2,
        older_history_file_count=1,
    )
    base.update(overrides)
    return SubmissionFetchResult(**base)


def test_fetch_returns_correct_metadata():
    r = fetch_submission("0000320193", _FakeClient())
    assert r.cik == "0000320193"
    assert r.entity_name == "Apple Inc."
    assert r.filing_count_recent == 2
    assert r.older_history_file_count == 1
    assert len(r.content_hash) == 64


def test_land_payload_writes_readable_gzip(tmp_path):
    r = _result(content=json.dumps(SAMPLE_PAYLOAD), content_hash="testhash")
    written = land_submission_payload(r, tmp_path)
    assert written.exists()
    with gzip.open(written, "rt", encoding="utf-8") as fh:
        loaded = json.loads(fh.read())
    assert loaded["name"] == "Apple Inc."


def test_land_submission_history_files_downloads_reuses_and_repairs_chunk(tmp_path):
    result = _result(content=json.dumps(SAMPLE_PAYLOAD), content_hash="historyhash")
    paths = land_submission_history_files(result, _FakeClient(), tmp_path, refresh=True)

    assert len(paths) == 1
    with gzip.open(paths[0], "rt", encoding="utf-8") as chunk_file:
        assert json.load(chunk_file)["filings"]["recent"]["form"] == ["10-K"]
    assert land_submission_history_files(result, _FakeClient(), tmp_path, refresh=False) == []
    paths[0].write_bytes(b"incomplete gzip")
    repaired = land_submission_history_files(result, _FakeClient(), tmp_path, refresh=False)
    assert repaired == paths
    with gzip.open(paths[0], "rt", encoding="utf-8") as chunk_file:
        assert json.load(chunk_file)["filings"]["recent"]["form"] == ["10-K"]

