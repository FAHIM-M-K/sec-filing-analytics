from __future__ import annotations

import csv
from pathlib import Path

CSV_COLUMNS = ["CIK", "Ticker", "Company Name"]


def load_membership_csv(path: str | Path) -> list[dict[str, str]]:
    """Load and validate the fixed current-company roster."""
    records = []
    seen_ciks = set()

    with Path(path).open(encoding="utf-8-sig", newline="") as membership_file:
        reader = csv.DictReader(membership_file)
        if reader.fieldnames != CSV_COLUMNS:
            raise ValueError(f"S&P 500 CSV columns must be exactly: {', '.join(CSV_COLUMNS)}")

        for row_number, row in enumerate(reader, start=2):
            cik_digits = row["CIK"].strip()
            if not cik_digits.isdigit() or len(cik_digits) > 10:
                raise ValueError(f"Row {row_number}: CIK must contain 1 to 10 digits.")
            cik = cik_digits.zfill(10)
            ticker = row["Ticker"].strip().upper()
            company_name = row["Company Name"].strip()
            if not ticker or not company_name:
                raise ValueError(f"Row {row_number}: ticker and company name are required.")
            if cik in seen_ciks:
                raise ValueError(f"Row {row_number}: duplicate CIK {cik}.")

            seen_ciks.add(cik)
            records.append({"cik": cik, "ticker": ticker, "company_name": company_name})

    if not records:
        raise ValueError("S&P 500 CSV has no valid company rows; refusing to change the roster.")
    return records
