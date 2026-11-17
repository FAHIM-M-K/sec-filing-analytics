from pathlib import Path
from datetime import date

from sec_filing_analytics.universe.membership import active_membership_ciks, load_membership_csv


def test_load_membership_csv_production_file():
    records = load_membership_csv(Path("configs/sp500.csv"))
    assert len(records) == 500
    
    seen_ciks = set()
    for record in records:
        assert len(record["cik"]) == 10
        assert record["cik"].isdigit()
        assert record["cik"] not in seen_ciks
        seen_ciks.add(record["cik"])
        
        assert record["ticker"]
        assert record["company_name"]
        assert record["membership_start_date"] is not None
        assert record["membership_end_date"] is None


def test_active_memberships_respect_start_and_end_dates():
    records = [
        {"cik": "0000000001", "membership_start_date": date(2020, 1, 1), "membership_end_date": None},
        {"cik": "0000000002", "membership_start_date": date(2027, 1, 1), "membership_end_date": None},
        {"cik": "0000000003", "membership_start_date": date(2010, 1, 1), "membership_end_date": date(2025, 12, 31)},
    ]
    assert active_membership_ciks(records, date(2026, 11, 17)) == ["0000000001"]

