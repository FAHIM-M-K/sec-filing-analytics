import gzip
import json
import logging
import os

_log = logging.getLogger(__name__)

FILING_FORMS = {"10-K", "10-Q", "10-K/A", "10-Q/A"}
FP_MAP = {"FY": "FY", "Q1": "Q1", "Q2": "Q2", "Q3": "Q3", "Q4": "Q4"}


def _parse_recent_block(cik: str, company: str, recent: dict, source_hash: str) -> list[dict]:
    forms      = recent.get("form", [])
    dates      = recent.get("filingDate", [])
    reports    = recent.get("reportDate", [])
    accessions = recent.get("accessionNumber", [])
    docs       = recent.get("primaryDocument", [])
    fps        = recent.get("fp", [])
    fys        = recent.get("fy", [])

    rows = []
    for i, form in enumerate(forms):
        if form not in FILING_FORMS:
            continue
        filing_date = dates[i] if i < len(dates) else ""
        if filing_date < "2009-01-01":
            continue
        report_date = reports[i] if i < len(reports) else ""

        # Robust fiscal year determination
        fy_val = None
        if i < len(fys) and fys[i]:
            try:
                fy_val = int(fys[i])
            except (ValueError, TypeError):
                pass
        if fy_val is None:
            date_ref = report_date or filing_date
            if date_ref and len(date_ref) >= 4 and date_ref[:4].isdigit():
                fy_val = int(date_ref[:4])

        # Robust fiscal period determination
        fp_val = FP_MAP.get(fps[i], fps[i]) if i < len(fps) and fps[i] else ""
        if not fp_val:
            if form in ("10-K", "10-K/A"):
                fp_val = "FY"
            elif form in ("10-Q", "10-Q/A"):
                if report_date and len(report_date) >= 7:
                    try:
                        month = int(report_date[5:7])
                        fp_val = f"Q{((month - 1) // 3) + 1}"
                    except Exception:
                        fp_val = "Q"
                else:
                    fp_val = "Q"

        rows.append({
            "cik":              cik,
            "accession_number": accessions[i] if i < len(accessions) else "",
            "form_type":        form,
            "filing_date":      filing_date,
            "report_date":      report_date,
            "fiscal_year":      fy_val,
            "fiscal_period":    fp_val,
            "company_name":     company,
            "primary_document": docs[i] if i < len(docs) else "",
            "source_hash":      source_hash,
        })
    return rows


def parse_submission_payload(path: str, source_hash: str) -> list[dict]:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        data = json.load(f)

    cik     = data.get("cik", "")
    company = data.get("name", "")

    rows = _parse_recent_block(cik, company, data.get("filings", {}).get("recent", {}), source_hash)

    base_dir = os.path.dirname(path)
    for chunk_ref in data.get("filings", {}).get("files", []):
        chunk_name = chunk_ref.get("name", "")
        if not chunk_name:
            continue
        chunk_path = os.path.join(base_dir, chunk_name)
        if not os.path.exists(chunk_path):
            if os.path.exists(chunk_path + ".gz"):
                chunk_path = chunk_path + ".gz"
            else:
                continue
        try:
            if chunk_path.endswith(".gz"):
                with gzip.open(chunk_path, "rt", encoding="utf-8") as f:
                    chunk = json.load(f)
            else:
                with open(chunk_path, "rt", encoding="utf-8") as f:
                    chunk = json.load(f)
            rows.extend(_parse_recent_block(cik, company, chunk, source_hash))
        except Exception as exc:
            _log.warning("Skipping chunk %s: %s", chunk_path, exc)

    return rows


def build_silver_filing(spark, manifest_df):
    from pyspark.sql import functions as F

    paths = [
        (r["payload_path"], r["content_hash"])
        for r in manifest_df.filter(F.col("payload_path").isNotNull())
        .select("payload_path", "content_hash")
        .collect()
    ]

    all_rows = []
    for path, hash_ in paths:
        try:
            all_rows.extend(parse_submission_payload(path, hash_))
        except Exception as exc:
            _log.warning("Skipping payload %s: %s", path, exc)

    from sec_filing_analytics.silver.schema import SILVER_FILING_SCHEMA

    schema = SILVER_FILING_SCHEMA
    if not all_rows:
        return spark.createDataFrame([], schema=schema)

    df = spark.createDataFrame(all_rows, schema=schema)
    df = df.dropDuplicates(["cik", "accession_number"])
    return df


def upsert_silver_filing(spark, df, catalog: str = "sec_analytics") -> dict:
    from delta.tables import DeltaTable

    table_name = f"{catalog}.silver.filing"
    target     = DeltaTable.forName(spark, table_name)

    (
        target.alias("t")
        .merge(df.alias("s"), "t.cik = s.cik AND t.accession_number = s.accession_number")
        .whenMatchedUpdate(set={
            "form_type":        "s.form_type",
            "filing_date":      "s.filing_date",
            "report_date":      "s.report_date",
            "fiscal_year":      "s.fiscal_year",
            "fiscal_period":    "s.fiscal_period",
            "company_name":     "s.company_name",
            "primary_document": "s.primary_document",
            "source_hash":      "s.source_hash",
        })
        .whenNotMatchedInsertAll()
        .execute()
    )

    total = spark.table(table_name).count()
    return {"total_rows": total}
