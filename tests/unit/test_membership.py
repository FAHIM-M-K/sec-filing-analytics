import pytest

from sec_filing_analytics.universe.membership import load_membership_csv


def test_roster_loader_returns_every_company_without_membership_dates(tmp_path):
    path = tmp_path / "sp500.csv"
    path.write_text(
        "CIK,Ticker,Company Name\n"
        "1,AAA,Company A\n"
        "2,BBB,Company B\n",
        encoding="utf-8",
    )

    records = load_membership_csv(path)

    assert [record["cik"] for record in records] == ["0000000001", "0000000002"]


def test_roster_loader_fails_closed_for_duplicate_or_empty_input(tmp_path):
    path = tmp_path / "sp500.csv"
    path.write_text(
        "CIK,Ticker,Company Name\n"
        "1,AAA,Company A\n"
        "1,BBB,Company B\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="duplicate CIK"):
        load_membership_csv(path)

    path.write_text("CIK,Ticker,Company Name\n", encoding="utf-8")
    with pytest.raises(ValueError, match="no valid company rows"):
        load_membership_csv(path)
