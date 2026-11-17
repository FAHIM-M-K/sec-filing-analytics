"""Shared helpers for selecting the current Bronze payload per company."""

from typing import Any


def latest_payload_manifest(manifest_df: Any) -> Any:
    """Return one stored payload row per CIK, ordered by fetch time and run ID."""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    return (
        manifest_df
        .filter(F.col("payload_path").isNotNull())
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
        .drop("_manifest_rank")
    )
