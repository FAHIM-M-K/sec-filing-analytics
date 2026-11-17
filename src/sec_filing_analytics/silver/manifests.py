"""Shared helpers for selecting the current Bronze payload per company."""

from typing import Any


def latest_payload_paths(manifest_df: Any) -> list[tuple[str, str]]:
    """Return the newest payload path per CIK or fail rather than use stale data."""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    latest_rows = (
        manifest_df
        .withColumn(
            "_manifest_rank",
            F.row_number().over(
                Window.partitionBy("cik").orderBy(
                    F.col("fetched_at").desc(),
                    F.col("run_id").desc(),
                    F.col("content_hash").desc(),
                )
            ),
        )
        .filter(F.col("_manifest_rank") == 1)
        .select("cik", "payload_path", "content_hash")
        .collect()
    )
    if not latest_rows:
        raise RuntimeError("Bronze manifest contains no payload rows; refusing a Silver rebuild.")

    missing = [row["cik"] for row in latest_rows if not row["payload_path"]]
    if missing:
        raise RuntimeError(
            "Latest Bronze payload path is missing for CIK(s): "
            f"{', '.join(missing[:10])}. Refusing to use stale payloads."
        )

    return [(row["payload_path"], row["content_hash"]) for row in latest_rows]
