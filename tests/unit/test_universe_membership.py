import pytest

from sec_filing_analytics.universe.membership import load_membership_csv


def test_roster_loader_normalizes_and_fails_closed(tmp_path):
    """Verify that CIKs are zero-padded to 10 digits and duplicate or empty inputs fail closed."""
    valid_csv = tmp_path / "sp500.csv"
    valid_csv.write_text(
        "CIK,Ticker,Company Name\n"
        "320193,AAPL,Apple Inc.\n"
        "789019,MSFT,Microsoft Corp.\n",
        encoding="utf-8",
    )
    records = load_membership_csv(valid_csv)
    assert [r["cik"] for r in records] == ["0000320193", "0000789019"]
    assert records[0]["ticker"] == "AAPL"

    duplicate_csv = tmp_path / "dup.csv"
    duplicate_csv.write_text(
        "CIK,Ticker,Company Name\n"
        "320193,AAPL,Apple Inc.\n"
        "320193,AAPL2,Apple Inc. Duplicate\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="duplicate CIK"):
        load_membership_csv(duplicate_csv)

    empty_csv = tmp_path / "empty.csv"
    empty_csv.write_text("CIK,Ticker,Company Name\n", encoding="utf-8")
    with pytest.raises(ValueError, match="no valid company rows"):
        load_membership_csv(empty_csv)
