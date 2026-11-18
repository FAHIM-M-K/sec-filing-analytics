import gzip
import json
from typing import Any

EVENT_FORMS = {"8-K", "8-K/A"}

# High-impact corporate catalyst items tracked for executive risk and governance
TARGET_8K_ITEMS: dict[str, str] = {
    "4.02": "Accounting Non-Reliance / Restatement",
    "4.01": "Auditor Resignation or Replacement",
    "5.02": "Executive / Director Departure or Appointment",
    "1.01": "Material Definitive Agreement (M&A / Contracts)",
    "2.01": "Completion of Acquisition or Disposition",
    "2.05": "Costs Associated with Exit / Layoffs / Restructuring",
    "2.06": "Material Asset / Goodwill Impairments",
    "1.03": "Bankruptcy or Receivership",
    "2.04": "Direct Financial Obligation Acceleration / Default",
}


def parse_item_codes(raw_items: Any) -> list[str]:
    """Parse and normalize SEC 8-K items from string, list, or comma-separated format."""
    if not raw_items:
        return []
    if isinstance(raw_items, list):
        items_list = raw_items
    elif isinstance(raw_items, str):
        items_list = raw_items.split(",")
    else:
        return []

    cleaned: list[str] = []
    for item in items_list:
        code = str(item).strip()
        if code.lower().startswith("item"):
            code = code[4:].strip().lstrip(".:- ")
        if code in TARGET_8K_ITEMS and code not in cleaned:
            cleaned.append(code)
    return cleaned


def _parse_recent_events_block(
    cik: str,
    company: str,
    recent: dict[str, Any],
    source_hash: str,
) -> list[dict[str, Any]]:
    forms = recent.get("form", [])
    dates = recent.get("filingDate", [])
    reports = recent.get("reportDate", [])
    accessions = recent.get("accessionNumber", [])
    docs = recent.get("primaryDocument", [])
    items_col = recent.get("items", [])

    rows: list[dict[str, Any]] = []
    for i, form in enumerate(forms):
        if form not in EVENT_FORMS:
            continue
        filing_date = dates[i] if i < len(dates) else ""
        if filing_date < "2009-01-01":
            continue

        raw_item = items_col[i] if i < len(items_col) else ""
        matched_items = parse_item_codes(raw_item)
        if not matched_items:
            continue

        report_date = reports[i] if i < len(reports) else ""
        accession = accessions[i] if i < len(accessions) else ""
        primary_doc = docs[i] if i < len(docs) else ""

        for code in matched_items:
            rows.append({
                "cik": cik,
                "accession_number": accession,
                "form_type": form,
                "filing_date": filing_date,
                "report_date": report_date,
                "company_name": company,
                "item_code": code,
                "item_description": TARGET_8K_ITEMS.get(code, "Corporate Event"),
                "primary_document": primary_doc,
                "event_headline": "",
                "event_text": "",
                "char_count": 0,
                "source_hash": source_hash,
            })
    return rows


def parse_submission_events(path: str, source_hash: str) -> list[dict[str, Any]]:
    """Parse Bronze submissions payload to extract high-impact 8-K event records."""
    if path.endswith(".gz"):
        with gzip.open(path, "rt", encoding="utf-8") as f:
            data = json.load(f)
    else:
        with open(path, "rt", encoding="utf-8") as f:
            data = json.load(f)

    cik = data.get("cik", "")
    company = data.get("name", "")

    rows = _parse_recent_events_block(cik, company, data.get("filings", {}).get("recent", {}), source_hash)

    from sec_filing_analytics.silver.submission_chunks import iter_submission_history_chunks
    for _, chunk in iter_submission_history_chunks(path, data):
        rows.extend(_parse_recent_events_block(cik, company, chunk, source_hash))

    return rows


def build_silver_corporate_events(spark: Any, manifest_df: Any) -> Any:
    """Read all Bronze submission payloads from manifest and parse high-impact 8-K events."""
    from sec_filing_analytics.silver.manifests import latest_payload_paths
    from sec_filing_analytics.silver.schema import SILVER_CORPORATE_EVENT_SCHEMA

    paths = latest_payload_paths(manifest_df)

    all_rows: list[dict[str, Any]] = []
    payload_errors = []
    for path, hash_ in paths:
        try:
            all_rows.extend(parse_submission_events(path, hash_))
        except Exception as exc:
            payload_errors.append((path, exc))

    if payload_errors:
        sample = "; ".join(f"{path}: {error}" for path, error in payload_errors[:5])
        raise RuntimeError(f"Failed to parse {len(payload_errors)} submissions payload(s) for events: {sample}")

    if not all_rows:
        return spark.createDataFrame([], schema=SILVER_CORPORATE_EVENT_SCHEMA)

    df = spark.createDataFrame(all_rows, schema=SILVER_CORPORATE_EVENT_SCHEMA)
    df = df.dropDuplicates(["cik", "accession_number", "item_code"])
    return df


def upsert_corporate_events(spark: Any, df: Any, catalog: str = "sec_analytics") -> dict[str, int]:
    """Upsert corporate event records into silver.corporate_event Delta table."""
    from delta.tables import DeltaTable
    from sec_filing_analytics.silver.schema import corporate_event_ddl

    spark.sql(corporate_event_ddl(catalog))
    table_name = f"{catalog}.silver.corporate_event"
    target = DeltaTable.forName(spark, table_name)

    (
        target.alias("t")
        .merge(
            df.alias("s"),
            "t.cik = s.cik AND t.accession_number = s.accession_number AND t.item_code = s.item_code",
        )
        .whenMatchedUpdate(set={
            "form_type": "s.form_type",
            "filing_date": "s.filing_date",
            "report_date": "s.report_date",
            "company_name": "s.company_name",
            "item_description": "s.item_description",
            "primary_document": "s.primary_document",
            "event_headline": "s.event_headline",
            "event_text": "s.event_text",
            "char_count": "s.char_count",
            "source_hash": "s.source_hash",
        })
        .whenNotMatchedInsertAll()
        .execute()
    )

    total = spark.table(table_name).count()
    return {"total_rows": total}
