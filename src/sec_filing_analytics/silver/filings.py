import gzip
import json

from sec_filing_analytics.silver.schema import ANNUAL_AND_QUARTERLY_FORMS


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
        if form not in ANNUAL_AND_QUARTERLY_FORMS:
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
        fp_val = fps[i] if i < len(fps) and fps[i] else ""
        if not fp_val:
            if form in ("10-K", "10-K/A"):
                fp_val = "FY"
            elif form in ("10-Q", "10-Q/A"):
                if report_date and len(report_date) >= 7:
                    try:
                        month = int(report_date[5:7])
                        fp_val = f"Q{((month - 1) // 3) + 1}"
                    except (ValueError, TypeError):
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
    from sec_filing_analytics.silver.submission_chunks import iter_submission_history_chunks
    for _, chunk in iter_submission_history_chunks(path, data):
        rows.extend(_parse_recent_block(cik, company, chunk, source_hash))

    return rows


def build_silver_filing(spark, manifest_df):
    from sec_filing_analytics.silver.manifests import latest_payload_paths

    paths = latest_payload_paths(manifest_df)

    all_rows = []
    payload_errors = []
    for path, hash_ in paths:
        try:
            all_rows.extend(parse_submission_payload(path, hash_))
        except Exception as exc:
            payload_errors.append((path, exc))

    if payload_errors:
        sample = "; ".join(f"{path}: {error}" for path, error in payload_errors[:5])
        raise RuntimeError(f"Failed to parse {len(payload_errors)} submissions payload(s): {sample}")

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
