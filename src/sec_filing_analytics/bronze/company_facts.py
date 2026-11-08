"""Bronze landing for SEC Company Facts."""

from __future__ import annotations

import gzip
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any

from sec_filing_analytics.sec_client.client import SecApiClient


@dataclass(frozen=True)
class CompanyFactsFetchResult:
    cik: str
    entity_name: str
    source_url: str
    fetched_at: datetime
    content: str
    content_hash: str
    response_size_bytes: int
    us_gaap_tag_count: int
    taxonomies: list[str]


def fetch_company_facts(cik: str, client: SecApiClient) -> CompanyFactsFetchResult:
    """Fetch the Company Facts endpoint for one CIK. Does not write to storage."""
    payload = client.get_company_facts(cik)
    content = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    content_hash = sha256(content.encode("utf-8")).hexdigest()
    facts = payload.get("facts", {})
    return CompanyFactsFetchResult(
        cik=cik,
        entity_name=payload.get("entityName", ""),
        source_url=f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json",
        fetched_at=datetime.now(timezone.utc),
        content=content,
        content_hash=content_hash,
        response_size_bytes=len(content.encode("utf-8")),
        us_gaap_tag_count=len(facts.get("us-gaap", {})),
        taxonomies=sorted(facts.keys()),
    )


def company_facts_volume_path(volume_root: str | Path, cik: str, content_hash: str) -> Path:
    """Return the hash-addressed volume path for one Company Facts payload."""
    return Path(volume_root) / "company_facts" / f"cik={cik}" / f"{content_hash}.json.gz"


def land_company_facts_payload(
    result: CompanyFactsFetchResult, volume_root: str | Path
) -> Path:
    """Write the payload as gzip-compressed JSON to the volume. Returns the written path."""
    path = company_facts_volume_path(volume_root, result.cik, result.content_hash)
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        fh.write(result.content)
    return path


def build_manifest_row(
    result: CompanyFactsFetchResult,
    run_id: str,
    payload_path: str | None,
    is_new_version: bool,
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "cik": result.cik,
        "entity_name": result.entity_name,
        "fetched_at": result.fetched_at,
        "source_url": result.source_url,
        "content_hash": result.content_hash,
        "response_size_bytes": result.response_size_bytes,
        "payload_path": payload_path,
        "is_new_version": is_new_version,
        "us_gaap_tag_count": result.us_gaap_tag_count,
        "taxonomies": result.taxonomies,
    }


def latest_company_facts_hashes(spark: Any, table_name: str) -> dict[str, str]:
    """Return the most recent content_hash per CIK. Returns {} if the table does not exist."""
    try:
        rows = spark.sql(
            f"""
            SELECT cik, content_hash
            FROM (
                SELECT cik, content_hash,
                       ROW_NUMBER() OVER (PARTITION BY cik ORDER BY fetched_at DESC) AS rn
                FROM {table_name}
            ) ranked
            WHERE rn = 1
            """
        ).collect()
        return {row["cik"]: row["content_hash"] for row in rows}
    except Exception:  # noqa: BLE001
        return {}


def upsert_company_facts_manifest(
    spark: Any, rows: list[dict[str, Any]], table_name: str
) -> None:
    """Merge manifest rows into the Bronze Delta table.

    Merge key: (cik, content_hash).
    WHEN MATCHED  -> update run_id and fetched_at; payload_path is preserved.
    WHEN NOT MATCHED -> insert all columns including the new payload_path.
    """
    if not rows:
        return
    from sec_filing_analytics.bronze.schema import COMPANY_FACTS_MANIFEST_SCHEMA

    df = spark.createDataFrame(rows, schema=COMPANY_FACTS_MANIFEST_SCHEMA)
    df.createOrReplaceTempView("_bronze_incoming_company_facts")
    spark.sql(
        f"""
        MERGE INTO {table_name} AS target
        USING _bronze_incoming_company_facts AS source
        ON target.cik = source.cik
           AND target.content_hash = source.content_hash
        WHEN MATCHED THEN UPDATE SET
            target.run_id         = source.run_id,
            target.fetched_at     = source.fetched_at,
            target.is_new_version = source.is_new_version
        WHEN NOT MATCHED THEN INSERT (
            run_id, cik, entity_name, fetched_at, source_url, content_hash,
            response_size_bytes, payload_path, is_new_version,
            us_gaap_tag_count, taxonomies
        ) VALUES (
            source.run_id, source.cik, source.entity_name, source.fetched_at,
            source.source_url, source.content_hash, source.response_size_bytes,
            source.payload_path, source.is_new_version, source.us_gaap_tag_count,
            source.taxonomies
        )
        """
    )
