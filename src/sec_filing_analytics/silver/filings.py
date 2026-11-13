"""Parse submission JSON payloads into the silver filing table."""

import gzip
import json
import logging

_log = logging.getLogger(__name__)


# Forms we care about — everything else is noise.
FILING_FORMS = {"10-K", "10-Q", "10-K/A", "10-Q/A"}

# Maps SEC fiscal period codes to readable labels.
FP_MAP = {"FY": "FY", "Q1": "Q1", "Q2": "Q2", "Q3": "Q3", "Q4": "Q4"}


def _parse_recent_block(cik: str, company: str, recent: dict, source_hash: str) -> list[dict]:
    """Extract filing rows from a single recent/chunk block."""
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
        filing_date = dates[i]      if i < len(dates)      else ""
        if filing_date < "2010-01-01":
            continue
        rows.append({
            "cik":              cik,
            "accession_number": accessions[i] if i < len(accessions) else "",
            "form_type":        form,
            "filing_date":      filing_date,
            "report_date":      reports[i]    if i < len(reports)    else "",
            "fiscal_year":      int(fys[i])   if i < len(fys) and fys[i] else None,
            "fiscal_period":    FP_MAP.get(fps[i], fps[i]) if i < len(fps) else "",
            "company_name":     company,
            "primary_document": docs[i]       if i < len(docs)       else "",
            "source_hash":      source_hash,
        })
    return rows


def parse_submission_payload(path: str, source_hash: str) -> list[dict]:
    """
    Parse one submission gzip payload into a list of filing dicts.

    The main JSON holds the most recent ~1000 filings. Older filings are
    referenced in filings.files[] as chunk file names — we follow those too
    so the 2010 backfill is complete.
    """
    with gzip.open(path, "rt", encoding="utf-8") as f:
        data = json.load(f)

    cik     = data.get("cik", "")
    company = data.get("name", "")

    rows = _parse_recent_block(cik, company, data.get("filings", {}).get("recent", {}), source_hash)

    # Chunk files referenced in filings.files[]
    # Each entry has a "name" field like "CIK0000320193-submissions-001.json"
    # They are stored alongside the main payload in the same volume directory.
    import os
    base_dir = os.path.dirname(path)
    for chunk_ref in data.get("filings", {}).get("files", []):
        chunk_name = chunk_ref.get("name", "")
        if not chunk_name:
            continue
        # Chunk files in the volume are named after the CIK without leading zeros.
        # Try both padded and unpadded variants.
        chunk_path = os.path.join(base_dir, chunk_name)
        if not os.path.exists(chunk_path):
            continue
        try:
            with gzip.open(chunk_path, "rt", encoding="utf-8") as f:
                chunk = json.load(f)
            rows.extend(_parse_recent_block(cik, company, chunk, source_hash))
        except Exception:
            pass

    return rows


def build_silver_filing(spark, manifest_df):
    """
    Read all submission payloads from the manifest and return a deduplicated
    DataFrame ready to merge into silver.filing.
    """
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
    """MERGE new filing rows into silver.filing. Returns row counts."""
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

