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
class SubmissionFetchResult:
    cik: str
    entity_name: str
    source_url: str
    fetched_at: datetime
    content: str
    content_hash: str
    response_size_bytes: int
    filing_count_recent: int
    older_history_file_count: int


def fetch_submission(cik: str, client: SecApiClient) -> SubmissionFetchResult:
    payload = client.get_submissions(cik)
    content = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    content_hash = sha256(content.encode("utf-8")).hexdigest()
    filings = payload.get("filings", {})
    recent = filings.get("recent", {})
    return SubmissionFetchResult(
        cik=cik,
        entity_name=payload.get("name", ""),
        source_url=f"https://data.sec.gov/submissions/CIK{cik}.json",
        fetched_at=datetime.now(timezone.utc),
        content=content,
        content_hash=content_hash,
        response_size_bytes=len(content.encode("utf-8")),
        filing_count_recent=len(recent.get("accessionNumber", [])),
        older_history_file_count=len(filings.get("files", [])),
    )


def submission_volume_path(volume_root: str | Path, cik: str, content_hash: str) -> Path:
    return Path(volume_root) / "submissions" / f"cik={cik}" / f"{content_hash}.json.gz"


def land_submission_payload(result: SubmissionFetchResult, volume_root: str | Path) -> Path:
    path = submission_volume_path(volume_root, result.cik, result.content_hash)
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        fh.write(result.content)
    return path


def land_submission_history_files(
    result: SubmissionFetchResult,
    client: SecApiClient,
    volume_root: str | Path,
    *,
    refresh: bool = False,
) -> list[Path]:
    """Download SEC-referenced older submission chunks beside their parent payload."""
    payload = json.loads(result.content)
    history_refs = payload.get("filings", {}).get("files", [])
    chunk_dir = submission_volume_path(volume_root, result.cik, result.content_hash).parent
    chunk_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    for reference in history_refs:
        name = reference.get("name", "")
        if not isinstance(name, str) or Path(name).name != name or not name.endswith(".json"):
            raise ValueError(f"Invalid SEC submissions history filename: {name!r}")
        chunk_path = chunk_dir / f"{name}.gz"
        if chunk_path.exists() and not refresh:
            try:
                with gzip.open(chunk_path, "rt", encoding="utf-8") as chunk_file:
                    cached_payload = json.load(chunk_file)
                if isinstance(cached_payload, dict):
                    continue
            except (OSError, EOFError, json.JSONDecodeError):
                pass

        chunk_payload = client.get_submission_history_file(name)
        temporary_path = chunk_path.with_name(f"{chunk_path.name}.tmp")
        with gzip.open(temporary_path, "wt", encoding="utf-8") as chunk_file:
            json.dump(chunk_payload, chunk_file, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        temporary_path.replace(chunk_path)
        written.append(chunk_path)

    return written


def build_manifest_row(
    result: SubmissionFetchResult,
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
        "filing_count_recent": result.filing_count_recent,
        "older_history_file_count": result.older_history_file_count,
    }


def latest_submission_hashes(spark: Any, table_name: str) -> dict[str, str]:
    rows = spark.sql(
        f"""
        SELECT cik, content_hash
        FROM (
            SELECT cik, content_hash,
                   ROW_NUMBER() OVER (PARTITION BY cik ORDER BY fetched_at DESC, run_id DESC) AS rn
            FROM {table_name}
        ) ranked
        WHERE rn = 1
        """
    ).collect()
    return {row["cik"]: row["content_hash"] for row in rows}


def upsert_submission_manifest(
    spark: Any, rows: list[dict[str, Any]], table_name: str
) -> None:
    if not rows:
        return
    from sec_filing_analytics.bronze.schema import SUBMISSION_MANIFEST_SCHEMA

    df = spark.createDataFrame(rows, schema=SUBMISSION_MANIFEST_SCHEMA)
    df.createOrReplaceTempView("_bronze_incoming_submissions")
    spark.sql(
        f"""
        MERGE INTO {table_name} AS target
        USING _bronze_incoming_submissions AS source
        ON target.cik = source.cik
           AND target.content_hash = source.content_hash
        WHEN MATCHED THEN UPDATE SET
            target.run_id         = source.run_id,
            target.fetched_at     = source.fetched_at,
            target.is_new_version = source.is_new_version
        WHEN NOT MATCHED THEN INSERT (
            run_id, cik, entity_name, fetched_at, source_url, content_hash,
            response_size_bytes, payload_path, is_new_version,
            filing_count_recent, older_history_file_count
        ) VALUES (
            source.run_id, source.cik, source.entity_name, source.fetched_at,
            source.source_url, source.content_hash, source.response_size_bytes,
            source.payload_path, source.is_new_version, source.filing_count_recent,
            source.older_history_file_count
        )
        """
    )
