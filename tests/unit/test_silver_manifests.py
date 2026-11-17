from datetime import datetime, timezone

import pytest


def test_latest_payload_paths_uses_newest_version_and_refuses_missing_pointer():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession
    from sec_filing_analytics.silver.manifests import latest_payload_paths

    try:
        spark = SparkSession.builder.master("local[1]").appName("silver-manifest-test").getOrCreate()
    except Exception as exc:
        pytest.skip(f"Local Spark runtime is unavailable: {exc}")

    fetched_earlier = datetime(2026, 1, 1, tzinfo=timezone.utc)
    fetched_later = datetime(2026, 2, 1, tzinfo=timezone.utc)
    schema = "cik STRING, fetched_at TIMESTAMP, run_id STRING, content_hash STRING, payload_path STRING"
    try:
        manifest = spark.createDataFrame(
            [
                ("0000000001", fetched_earlier, "run-1", "hash-1", "/old.json.gz"),
                ("0000000001", fetched_later, "run-2", "hash-2", "/new.json.gz"),
            ],
            schema,
        )
        assert latest_payload_paths(manifest) == [("/new.json.gz", "hash-2")]

        missing_latest = spark.createDataFrame(
            [
                ("0000000001", fetched_earlier, "run-1", "hash-1", "/old.json.gz"),
                ("0000000001", fetched_later, "run-2", "hash-2", None),
            ],
            schema,
        )
        with pytest.raises(RuntimeError, match="Refusing to use stale payloads"):
            latest_payload_paths(missing_latest)
    finally:
        spark.stop()
