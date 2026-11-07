from pathlib import Path
from sec_filing_analytics.universe.membership import load_membership_csv


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

