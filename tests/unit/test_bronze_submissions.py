import gzip
import json
from datetime import datetime, timezone

import pytest

from sec_filing_analytics.bronze.submissions import (
    SubmissionFetchResult,
    build_manifest_row,
    fetch_submission,
    land_submission_history_files,
    land_submission_payload,
    submission_volume_path,
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


def test_hash_is_stable_across_fetches():
    r1 = fetch_submission("0000320193", _FakeClient())
    r2 = fetch_submission("0000320193", _FakeClient())
    assert r1.content_hash == r2.content_hash


def test_volume_path_format():
    path = submission_volume_path("/Volumes/cat/s/raw", "0000320193", "abc123")
    assert str(path).replace("\\", "/") == (
        "/Volumes/cat/s/raw/submissions/cik=0000320193/abc123.json.gz"
    )


def test_land_payload_writes_readable_gzip(tmp_path):
    r = _result(content=json.dumps(SAMPLE_PAYLOAD), content_hash="testhash")
    written = land_submission_payload(r, tmp_path)
    assert written.exists()
    with gzip.open(written, "rt", encoding="utf-8") as fh:
        loaded = json.loads(fh.read())
    assert loaded["name"] == "Apple Inc."


def test_land_submission_history_files_downloads_and_compresses_referenced_chunk(tmp_path):
    result = _result(content=json.dumps(SAMPLE_PAYLOAD), content_hash="historyhash")
    paths = land_submission_history_files(result, _FakeClient(), tmp_path, refresh=True)

    assert len(paths) == 1
    with gzip.open(paths[0], "rt", encoding="utf-8") as chunk_file:
        assert json.load(chunk_file)["filings"]["recent"]["form"] == ["10-K"]


def test_land_submission_history_files_reuses_existing_chunk_when_unchanged(tmp_path):
    result = _result(content=json.dumps(SAMPLE_PAYLOAD), content_hash="historyhash")
    first = land_submission_history_files(result, _FakeClient(), tmp_path, refresh=True)
    second = land_submission_history_files(result, _FakeClient(), tmp_path, refresh=False)

    assert len(first) == 1
    assert second == []


def test_land_submission_history_files_repairs_corrupt_cached_chunk(tmp_path):
    result = _result(content=json.dumps(SAMPLE_PAYLOAD), content_hash="historyhash")
    existing = land_submission_history_files(result, _FakeClient(), tmp_path, refresh=True)[0]
    existing.write_bytes(b"incomplete gzip")

    repaired = land_submission_history_files(result, _FakeClient(), tmp_path, refresh=False)

    assert repaired == [existing]
    with gzip.open(existing, "rt", encoding="utf-8") as chunk_file:
        assert json.load(chunk_file)["filings"]["recent"]["form"] == ["10-K"]


def test_build_manifest_row_new_version():
    row = build_manifest_row(_result(), "run-001", "/Volumes/x/y/z.json.gz", True)
    assert row["run_id"] == "run-001"
    assert row["is_new_version"] is True
    assert row["payload_path"] == "/Volumes/x/y/z.json.gz"


def test_build_manifest_row_unchanged():
    row = build_manifest_row(_result(), "run-002", None, False)
    assert row["is_new_version"] is False
    assert row["payload_path"] is None


def test_latest_hash_lookup_surfaces_manifest_query_failure():
    class BrokenSpark:
        def sql(self, _query):
            raise RuntimeError("manifest unavailable")

    from sec_filing_analytics.bronze.submissions import latest_submission_hashes
    with pytest.raises(RuntimeError, match="manifest unavailable"):
        latest_submission_hashes(BrokenSpark(), "bronze.submission_manifest")

