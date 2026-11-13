import hashlib
import re
import time

import requests

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
            return None
        text = re.sub(r"<[^>]+>", " ", resp.text)
        text = re.sub(r"\s+", " ", text)
        return text
    except Exception:
        return None


_TOC_LINE = re.compile(r"\s+\d{1,4}\s*$")


def _extract_section(text: str, start_pat: str, stop_pat: str) -> str | None:
    candidates = []
    for m in re.finditer(start_pat, text, re.IGNORECASE):
        tail = text[m.start():]
        m2 = re.search(stop_pat, tail[len(m.group()):], re.IGNORECASE)
        sec = tail[: len(m.group()) + m2.start()].strip() if m2 else tail[:100_000].strip()
        if _TOC_LINE.search(sec):
            continue
        candidates.append(sec)
    if not candidates:
        return None
    return max(candidates, key=len)


def build_narrative_rows(filing_rows: list[dict]) -> list[dict]:
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
