import hashlib
import html
import logging
import re
import time

import requests

_log = logging.getLogger(__name__)
_SLEEP = 0.11
_HEADERS = {"User-Agent": "sec-filing-analytics fahimmeethale@gmail.com"}

_10K_ITEMS = [
    (
        "Item 1A",
        r"item\s+1a[\.\:\s\-\–\—\&#\d;]+(?:r\s*i\s*s\s*k\s*f\s*a\s*c\s*t\s*o\s*r\s*s|risk)",
        r"item\s+1b[\.\:\s\-\–\—\&#\d;]+(?:u\s*n\s*r\s*e\s*s\s*o\s*l\s*v|unresolved)|item\s+2[\.\:\s\-\–\—\&#\d;]+properties",
    ),
    (
        "Item 7",
        r"item\s+7[\.\:\s\-\–\—\&#\d;]+(?:m\s*a\s*n\s*a\s*g\s*e\s*m\s*e\s*n\s*t|management)",
        r"item\s+7a[\.\:\s\-\–\—\&#\d;]+(?:q\s*u\s*a\s*n\s*t\s*i\s*t\s*a\s*t\s*i\s*v\s*e|quantitative)|item\s+8[\.\:\s\-\–\—\&#\d;]+financial\s+statements",
    ),
]

_FORM_ITEMS = {
    "10-K":   _10K_ITEMS,
    "10-K/A": _10K_ITEMS,
}


def _fetch_text(cik: str, accession_number: str, primary_document: str) -> str | None:
    accession_clean = accession_number.replace("-", "")
    cik_clean = str(int(cik)) if str(cik).isdigit() else cik
    url = (
        f"https://www.sec.gov/Archives/edgar/data/{cik_clean}/"
        f"{accession_clean}/{primary_document}"
    )
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=20)
        if resp.status_code != 200:
            _log.warning("SEC fetch returned HTTP %s for %s", resp.status_code, url)
            return None
        text = re.sub(r"<[^>]+>", " ", resp.text)
        text = html.unescape(text)
        text = re.sub(r"\s+", " ", text)
        return text
    except Exception as exc:
        _log.warning("SEC fetch exception for %s: %s", url, exc)
        return None


_TOC_LINE = re.compile(r"\s+\d{1,4}\s*$")


def _extract_section(text: str, start_pat: str, stop_pat: str) -> str | None:
    candidates = []
    for m in re.finditer(start_pat, text, re.IGNORECASE):
        tail = text[m.start():]
        m2 = re.search(stop_pat, tail[len(m.group()):], re.IGNORECASE)
        sec = tail[: len(m.group()) + m2.start()].strip() if m2 else tail[:100_000].strip()
        # Table of contents entries are brief lines ending in a page number (< 1000 chars)
        if len(sec) < 1000 and _TOC_LINE.search(sec):
            continue
        if len(sec) < 200:
            continue
        candidates.append(sec)
    if not candidates:
        return None
    return max(candidates, key=len)


def build_narrative_rows(filing_rows: list[dict]) -> dict[str, list[dict]]:
    rows = []
    outcomes = []
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
            outcomes.append({
                "cik": cik,
                "accession_number": accn,
                "status": "fetch_failed",
                "item_count": 0,
            })
            continue

        item_count = 0
        for item_name, start_pat, stop_pat in items:
            content = _extract_section(text, start_pat, stop_pat)
            if not content:
                continue
            item_count += 1
            rows.append({
                "cik":              cik,
                "accession_number": accn,
                "form_type":        form,
                "filing_date":      filing.get("filing_date", ""),
                "item_name":        item_name,
                "text_content":     content,
                "char_count":       len(content),
                "source_hash":      hashlib.sha256(content.encode("utf-8")).hexdigest(),
            })

        outcomes.append({
            "cik": cik,
            "accession_number": accn,
            "status": "complete" if item_count == len(items) else "partial" if item_count else "no_sections",
            "item_count": item_count,
        })

    return {"rows": rows, "outcomes": outcomes}


def upsert_narratives(spark, df, catalog: str = "sec_analytics") -> dict:
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


def upsert_narrative_attempts(spark, outcomes: list[dict], catalog: str = "sec_analytics") -> None:
    if not outcomes:
        return

    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    from sec_filing_analytics.silver.schema import narrative_attempt_ddl

    spark.sql(narrative_attempt_ddl(catalog))
    table_name = f"{catalog}.silver.filing_narrative_attempt"
    incoming = spark.createDataFrame(outcomes).withColumn("last_attempt_at", F.current_timestamp())
    target = DeltaTable.forName(spark, table_name)

    (
        target.alias("t")
        .merge(
            incoming.alias("s"),
            "t.cik = s.cik AND t.accession_number = s.accession_number",
        )
        .whenMatchedUpdate(set={
            "status": "s.status",
            "item_count": "s.item_count",
            "attempt_count": "t.attempt_count + 1",
            "last_attempt_at": "s.last_attempt_at",
        })
        .whenNotMatchedInsert(values={
            "cik": "s.cik",
            "accession_number": "s.accession_number",
            "status": "s.status",
            "item_count": "s.item_count",
            "attempt_count": "1",
            "last_attempt_at": "s.last_attempt_at",
        })
        .execute()
    )
