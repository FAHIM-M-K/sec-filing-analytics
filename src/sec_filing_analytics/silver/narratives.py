"""Extract narrative text sections from 10-K and 8-K filings into silver.filing_narrative."""

import hashlib
import re
import time

import requests

# SEC rate limit: 10 req/sec max. Sleep 0.11s between requests to stay safe.
_SLEEP = 0.11

_HEADERS = {"User-Agent": "sec-filing-analytics fahimmeethale@gmail.com"}

# Items to extract per form type.
# Each entry: (item_name, start_pattern, stop_pattern)
_10K_ITEMS = [
    ("Item 1A",  r"item\s+1a[\.\s]", r"item\s+1b[\.\s]|item\s+2[\.\s]"),
    ("Item 7",   r"item\s+7[\.\s]",  r"item\s+7a[\.\s]|item\s+8[\.\s]"),
]
_8K_ITEMS = [
    ("Item 2.02", r"item\s+2\.02", r"item\s+\d"),
    ("Item 8.01", r"item\s+8\.01", r"item\s+\d"),
]

_FORM_ITEMS = {
    "10-K":   _10K_ITEMS,
    "10-K/A": _10K_ITEMS,
    "8-K":    _8K_ITEMS,
    "8-K/A":  _8K_ITEMS,
}


def _fetch_text(cik: str, accession_number: str, primary_document: str) -> str | None:
    """Fetch the primary document from EDGAR and return plain text."""
    accession_clean = accession_number.replace("-", "")
    url = (
        f"https://www.sec.gov/Archives/edgar/data/{cik}/"
        f"{accession_clean}/{primary_document}"
    )
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=20)
        if resp.status_code != 200:
            return None
        # Strip HTML tags to plain text
        text = re.sub(r"<[^>]+>", " ", resp.text)
        text = re.sub(r"\s+", " ", text)
        return text
    except Exception:
        return None


def _extract_section(text: str, start_pat: str, stop_pat: str) -> str | None:
    """Extract the text block between start_pat and stop_pat (case-insensitive)."""
    m = re.search(start_pat, text, re.IGNORECASE)
    if not m:
        return None
    tail = text[m.start():]
    m2 = re.search(stop_pat, tail[len(m.group()):], re.IGNORECASE)
    if m2:
        return tail[: len(m.group()) + m2.start()].strip()
    return tail[:50_000].strip()  # cap at ~50k chars if no stop found


def build_narrative_rows(filing_rows: list[dict]) -> list[dict]:
    """
    For each filing row (from silver.filing), fetch the primary document and
    extract relevant narrative sections. Returns a list of narrative dicts.
    """
    rows = []
    for filing in filing_rows:
        form      = filing.get("form_type", "")
        items     = _FORM_ITEMS.get(form)
        if not items:
            continue

        cik       = filing["cik"]
        accn      = filing["accession_number"]
        doc       = filing.get("primary_document") or ""
        if not doc:
            continue

        text = _fetch_text(cik, accn, doc)
        time.sleep(_SLEEP)
        if not text:
            continue

        for item_name, start_pat, stop_pat in items:
            content = _extract_section(text, start_pat, stop_pat)
            if not content:
                continue
            rows.append({
                "cik":              cik,
                "accession_number": accn,
                "form_type":        form,
                "filing_date":      filing.get("filing_date", ""),
                "item_name":        item_name,
                "text_content":     content,
                "char_count":       len(content),
                "source_hash":      hashlib.md5(content.encode()).hexdigest(),
            })

    return rows


def upsert_narratives(spark, df, catalog: str = "sec_analytics") -> dict:
    """MERGE narrative rows into silver.filing_narrative."""
    from delta.tables import DeltaTable
    from sec_filing_analytics.silver.schema import narrative_ddl

    spark.sql(narrative_ddl(catalog))

    table_name = f"{catalog}.silver.filing_narrative"
    target     = DeltaTable.forName(spark, table_name)

    merge_keys = (
        "t.cik = s.cik AND "
        "t.accession_number = s.accession_number AND "
        "t.item_name = s.item_name"
    )

    (
        target.alias("t")
        .merge(df.alias("s"), merge_keys)
        .whenMatchedUpdate(set={
            "text_content": "s.text_content",
            "char_count":   "s.char_count",
            "source_hash":  "s.source_hash",
        })
        .whenNotMatchedInsertAll()
        .execute()
    )

    total = spark.table(table_name).count()
    return {"total_rows": total}

