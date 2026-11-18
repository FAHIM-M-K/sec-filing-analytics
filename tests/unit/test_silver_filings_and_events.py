from sec_filing_analytics.silver.events import _parse_recent_events_block
from sec_filing_analytics.silver.filings import _parse_recent_block


def test_silver_filings_extraction():
    """Verify filing history is filtered to financial reports (10-K, 10-Q) with fiscal period resolution."""
    recent_filings = {
        "form": ["10-K", "10-Q", "8-K", "10-K"],
        "filingDate": ["2024-11-01", "2024-08-02", "2024-07-30", "2008-10-27"],
        "reportDate": ["2024-09-28", "2024-06-29", "", "2008-09-27"],
        "accessionNumber": ["accn-10k", "accn-10q", "accn-8k", "accn-old"],
        "primaryDocument": ["doc-10k.htm", "doc-10q.htm", "doc-8k.htm", "doc-old.htm"],
        "fp": ["FY", "Q3", "", "FY"],
        "fy": [2024, 2024, None, 2008],
    }

    rows = _parse_recent_block("0000320193", "Apple Inc.", recent_filings, "source_hash_123")

    assert len(rows) == 2
    assert {r["form_type"] for r in rows} == {"10-K", "10-Q"}
    assert all(r["filing_date"] >= "2009-01-01" for r in rows)

    ten_k = next(r for r in rows if r["form_type"] == "10-K")
    assert ten_k["fiscal_year"] == 2024
    assert ten_k["fiscal_period"] == "FY"
    assert ten_k["accession_number"] == "accn-10k"


def test_silver_corporate_events_extraction():
    """Verify material 8-K corporate catalyst events (e.g., 4.02 restatements, 5.02 executive changes) are extracted."""
    recent_events = {
        "form": ["8-K", "8-K", "10-K", "8-K"],
        "filingDate": ["2024-08-01", "2024-03-15", "2024-11-01", "2008-05-01"],
        "reportDate": ["2024-08-01", "2024-03-15", "2024-09-28", "2008-05-01"],
        "accessionNumber": ["accn-8k-1", "accn-8k-2", "accn-10k", "accn-old-8k"],
        "primaryDocument": ["doc1.htm", "doc2.htm", "doc3.htm", "doc4.htm"],
        "items": [
            "5.02,9.01",  # Executive departure/appointment + exhibits
            "4.02",       # Accounting non-reliance / restatement
            "",           # 10-K (no 8-K items)
            "5.02",       # Pre-2009 (should be filtered out)
        ],
    }

    rows = _parse_recent_events_block("0000320193", "Apple Inc.", recent_events, "source_hash_789")

    assert len(rows) == 2
    item_codes = {r["item_code"] for r in rows}
    assert item_codes == {"5.02", "4.02"}

    restatement_row = next(r for r in rows if r["item_code"] == "4.02")
    assert restatement_row["item_description"] == "Accounting Non-Reliance / Restatement"
    assert restatement_row["accession_number"] == "accn-8k-2"
