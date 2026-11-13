from __future__ import annotations

import csv
from datetime import date
from pathlib import Path
from typing import Any

CSV_COLUMNS = ["CIK", "Ticker", "Company Name", "Effective Start Date", "Effective End Date"]


def load_membership_csv(path: str | Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    seen_keys: set[tuple[str, date]] = set()

    with Path(path).open(encoding="utf-8-sig", newline="") as membership_file:
        reader = csv.DictReader(membership_file)
        if reader.fieldnames != CSV_COLUMNS:
            raise ValueError(f"Membership CSV columns must be exactly: {', '.join(CSV_COLUMNS)}")

        for row_number, row in enumerate(reader, start=2):
            cik_digits = row["CIK"].strip()
            if not cik_digits.isdigit() or len(cik_digits) > 10:
                raise ValueError(f"Row {row_number}: CIK must contain 1 to 10 digits.")
            cik = cik_digits.zfill(10)
            ticker = row["Ticker"].strip().upper()
            company_name = row["Company Name"].strip()
            if not ticker or not company_name:
                raise ValueError(f"Row {row_number}: ticker and company name are required.")

            try:
                start_date = date.fromisoformat(row["Effective Start Date"].strip())
                end_text = row["Effective End Date"].strip()
                end_date = date.fromisoformat(end_text) if end_text else None
            except ValueError as error:
                raise ValueError(f"Row {row_number}: dates must use YYYY-MM-DD.") from error

            if end_date is not None and end_date < start_date:
                raise ValueError(f"Row {row_number}: membership end precedes membership start.")
            key = (cik, start_date)
            if key in seen_keys:
                raise ValueError(f"Row {row_number}: duplicate CIK and membership start date.")
            seen_keys.add(key)
            records.append(
                {
                    "cik": cik,
                    "ticker": ticker,
                    "company_name": company_name,
                    "membership_start_date": start_date,
                    "membership_end_date": end_date,
                }
            )

    by_cik: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        by_cik.setdefault(record["cik"], []).append(record)
    for cik, periods in by_cik.items():
        periods.sort(key=lambda item: item["membership_start_date"])
        for prior, following in zip(periods, periods[1:]):
            prior_end = prior["membership_end_date"]
            if prior_end is None or following["membership_start_date"] <= prior_end:
                raise ValueError(f"CIK {cik} has overlapping membership periods.")

    return records


def merge_membership_table(spark: Any, records: list[dict[str, Any]], table_name: str) -> None:
    if not records:
        raise ValueError("Membership CSV has no verified rows; refusing to replace or clear the table.")

    spark.createDataFrame(records).createOrReplaceTempView("incoming_sp500_membership")
    spark.sql(
        f"""
        MERGE INTO {table_name} AS target
        USING incoming_sp500_membership AS source
        ON target.cik = source.cik
           AND target.membership_start_date = source.membership_start_date
        WHEN MATCHED THEN UPDATE SET
            target.ticker = source.ticker,
            target.company_name = source.company_name,
            target.membership_end_date = source.membership_end_date
        WHEN NOT MATCHED THEN INSERT (
            cik, ticker, company_name, membership_start_date, membership_end_date
        ) VALUES (
            source.cik, source.ticker, source.company_name,
            source.membership_start_date, source.membership_end_date
        )
        """
    )


def current_constituent_ciks(spark: Any, table_name: str) -> list[str]:
    rows = spark.sql(
        f"""
        SELECT DISTINCT cik
        FROM {table_name}
        WHERE membership_start_date <= current_date()
          AND (membership_end_date IS NULL OR membership_end_date >= current_date())
        ORDER BY cik
        """
    ).collect()
    return [row["cik"] for row in rows]


def membership_table_ddl(table_name: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        cik STRING NOT NULL,
        ticker STRING NOT NULL,
        company_name STRING NOT NULL,
        membership_start_date DATE NOT NULL,
        membership_end_date DATE
    ) USING DELTA
    """
