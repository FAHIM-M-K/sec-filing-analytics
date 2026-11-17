"""Read SEC submission history chunks referenced by a submissions payload."""

import gzip
import json
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any


def iter_submission_history_chunks(
    payload_path: str, payload: dict[str, Any]
) -> Iterator[tuple[str, dict[str, Any]]]:
    """Yield every referenced history chunk, failing clearly on missing or invalid files."""
    base_dir = Path(os.path.dirname(payload_path))
    for reference in payload.get("filings", {}).get("files", []):
        name = reference.get("name", "")
        if not isinstance(name, str) or Path(name).name != name or not name.endswith(".json"):
            raise ValueError(f"Invalid SEC submission history filename in {payload_path}: {name!r}")

        chunk_path = base_dir / name
        compressed_path = base_dir / f"{name}.gz"
        if compressed_path.exists():
            chunk_path = compressed_path
        if not chunk_path.exists():
            raise FileNotFoundError(
                f"Submission payload {payload_path} references missing history chunk {name}. "
                "Rerun Bronze submissions ingestion to download it."
            )

        try:
            if chunk_path.suffix == ".gz":
                with gzip.open(chunk_path, "rt", encoding="utf-8") as chunk_file:
                    chunk = json.load(chunk_file)
            else:
                with chunk_path.open("rt", encoding="utf-8") as chunk_file:
                    chunk = json.load(chunk_file)
        except Exception as exc:
            raise RuntimeError(f"Failed to read SEC submissions history chunk {chunk_path}: {exc}") from exc

        if not isinstance(chunk, dict):
            raise ValueError(f"SEC submissions history chunk is not a JSON object: {chunk_path}")
        yield name, chunk
