import gzip
import json
import os
import tempfile

from sec_filing_analytics.silver.events import (
    clean_event_html,
    parse_item_codes,
    parse_submission_events,
)


def _write_gz(path: str, data: dict) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as f:
        json.dump(data, f)


SAMPLE_SUBMISSION_EVENTS = {
    "cik": "0000320193",
    "name": "Apple Inc.",
    "filings": {
        "recent": {
            "form": [
                "10-K",
                "8-K",
                "8-K",
                "8-K",
                "8-K",
                "10-Q",
            ],
            "filingDate": [
                "2024-11-01",
                "2024-08-01",
                "2024-05-02",
                "2024-03-15",
                "2008-05-01",
                "2024-08-02",
            ],
            "reportDate": [
                "2024-09-28",
                "2024-08-01",
                "2024-05-02",
                "2024-03-15",
                "2008-05-01",
                "2024-06-29",
            ],
            "accessionNumber": [
                "0000320193-24-000100",
                "0000320193-24-000101",
                "0000320193-24-000102",
                "0000320193-24-000103",
                "0000320193-08-000050",
                "0000320193-24-000104",
            ],
            "primaryDocument": [
                "aapl-20240928.htm",
                "aapl-20240801.htm",
                "aapl-20240502.htm",
                "aapl-20240315.htm",
                "aapl-20080501.htm",
                "aapl-20240629.htm",
            ],
            "items": [
                "",
                "5.02,9.01",  # CEO/Executive change + exhibits
                "5.07",       # Routine shareholder vote (should be skipped)
                "4.02",       # Accounting non-reliance / restatement
                "5.02",       # Pre-2009 (should be skipped)
                "",
            ],
        },
        "files": [],
    },
}


def test_parse_item_codes():
    # Comma-separated string
    assert parse_item_codes("5.02,9.01") == ["5.02"]
    # Multiple valid items
    assert set(parse_item_codes("5.02, 4.02")) == {"5.02", "4.02"}
    # List format
    assert parse_item_codes(["5.02", "Item 1.01"]) == ["5.02", "1.01"]
    # Only routine / non-target items
    assert parse_item_codes("5.07, 7.01, 3.02") == []
    # Empty / None
    assert parse_item_codes("") == []
    assert parse_item_codes(None) == []


def test_parse_submission_events_filters_and_extracts():
    with tempfile.NamedTemporaryFile(suffix=".gz", delete=False) as tmp:
        path = tmp.name
    try:
        _write_gz(path, SAMPLE_SUBMISSION_EVENTS)
        rows = parse_submission_events(path, "hash_abc")

        # Only row 1 (5.02) and row 3 (4.02) should pass.
        # Row 0 (10-K) is not an 8-K.
        # Row 2 (5.07) is routine shareholder vote (skipped).
        # Row 4 is pre-2009 (skipped).
        # Row 5 is 10-Q (not an 8-K).
        assert len(rows) == 2

        item_codes = {r["item_code"] for r in rows}
        assert item_codes == {"5.02", "4.02"}

        for r in rows:
            assert r["cik"] == "0000320193"
            assert r["company_name"] == "Apple Inc."
            assert r["form_type"] == "8-K"
            assert r["source_hash"] == "hash_abc"

        row_502 = next(r for r in rows if r["item_code"] == "5.02")
        assert row_502["accession_number"] == "0000320193-24-000101"
        assert row_502["item_description"] == "Executive / Director Departure or Appointment"

        row_402 = next(r for r in rows if r["item_code"] == "4.02")
        assert row_402["accession_number"] == "0000320193-24-000103"
        assert row_402["item_description"] == "Accounting Non-Reliance / Restatement"
    finally:
        os.unlink(path)


def test_clean_event_html():
    raw_html = """
    <html>
      <body>
        <p><b>Item 5.02 Departure of Directors or Certain Officers.</b></p>
        <p>On August 1, 2024, the Board of Directors appointed Jane Doe as Chief Financial Officer.</p>
      </body>
    </html>
    """
    headline, body = clean_event_html(raw_html)
    assert "Item 5.02 Departure of Directors" in body
    assert "Jane Doe as Chief Financial Officer" in body
    assert "<p>" not in body
    assert len(headline) <= 250
    assert "Item 5.02" in headline
