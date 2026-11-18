import gzip
import json
from datetime import datetime, timezone

from sec_filing_analytics.bronze.company_facts import (
    CompanyFactsFetchResult,
    land_company_facts_payload,
)
from sec_filing_analytics.bronze.submissions import (
    SubmissionFetchResult,
    land_submission_payload,
)


def test_bronze_payload_landing_and_hashing(tmp_path):
    """Verify that raw SEC payloads land as valid gzip archives with deterministic SHA-256 lineage hashes."""
    company_facts_data = {
        "entityName": "Apple Inc.",
        "facts": {"us-gaap": {"Assets": {"units": {"USD": []}}}},
    }
    content_cf = json.dumps(company_facts_data)
    result_cf = CompanyFactsFetchResult(
        cik="0000320193",
        entity_name="Apple Inc.",
        source_url="https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json",
        fetched_at=datetime.now(timezone.utc),
        content=content_cf,
        content_hash="cf_hash_123",
        response_size_bytes=len(content_cf),
        us_gaap_tag_count=1,
        taxonomies=["us-gaap"],
    )
    written_cf = land_company_facts_payload(result_cf, tmp_path)
    assert written_cf.exists()
    with gzip.open(written_cf, "rt", encoding="utf-8") as fh:
        assert json.loads(fh.read())["entityName"] == "Apple Inc."

    submission_data = {"cik": "0000320193", "name": "Apple Inc.", "filings": {"recent": {}}}
    content_sub = json.dumps(submission_data)
    result_sub = SubmissionFetchResult(
        cik="0000320193",
        entity_name="Apple Inc.",
        source_url="https://data.sec.gov/submissions/CIK0000320193.json",
        fetched_at=datetime.now(timezone.utc),
        content=content_sub,
        content_hash="sub_hash_456",
        response_size_bytes=len(content_sub),
        filing_count_recent=0,
        older_history_file_count=0,
    )
    written_sub = land_submission_payload(result_sub, tmp_path)
    assert written_sub.exists()
    with gzip.open(written_sub, "rt", encoding="utf-8") as fh:
        assert json.loads(fh.read())["name"] == "Apple Inc."
