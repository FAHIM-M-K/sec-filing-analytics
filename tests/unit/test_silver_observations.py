import gzip
import json
import os
import tempfile
import pytest
from sec_filing_analytics.silver.observations import (
    _parse_facts_data,
    _parse_facts_text,
    parse_facts_payload,
)


SAMPLE_FACTS = {
    "cik": "0000320193",
    "facts": {
        "us-gaap": {
            "RevenueFromContractWithCustomerExcludingAssessedTax": {
                "units": {
                    "USD": [
                        {"accn": "0000320193-24-000123", "end": "2024-09-28", "val": 391035000000,
                         "form": "10-K", "fp": "FY", "fy": 2024, "filed": "2024-11-01",
                         "start": "2023-10-01"},
                        {"accn": "0000320193-24-000123", "end": "2024-09-28", "val": 391035000000,
                         "form": "10-K", "fp": "FY", "fy": 2024, "filed": "2024-11-15",
                         "start": "2023-10-01"},
                        {"accn": "0000320193-20-000050", "end": "2009-09-26", "val": 36537000000,
                         "form": "10-K", "fp": "FY", "fy": 2009, "filed": "2009-10-27",
                         "start": "2008-09-28"},
                    ]
                }
            },
            "Assets": {
                "units": {
                    "USD": [
                        {"accn": "0000320193-24-000123", "end": "2024-09-28", "val": 364980000000,
                         "form": "10-K", "fp": "FY", "fy": 2024, "filed": "2024-11-01"},
                    ]
                }
            },
            "SomeRandomTag": {
                "units": {
                    "USD": [
                        {"end": "2024-09-28", "val": 999, "form": "10-K", "fp": "FY", "fy": 2024,
                         "filed": "2024-11-01", "start": "2023-10-01"},
                    ]
                }
            },
        }
    }
}


def _write_gz(path: str, data: dict) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as f:
        json.dump(data, f)


def test_parse_keeps_only_supported_metrics_since_2010_with_period_types():
    with tempfile.NamedTemporaryFile(suffix=".gz", delete=False) as tmp:
        path = tmp.name
    try:
        _write_gz(path, SAMPLE_FACTS)
        rows = parse_facts_payload(path, "h")
        with gzip.open(path, "rt", encoding="utf-8") as payload:
            assert _parse_facts_text(payload.read(), "h", path) == rows
        with pytest.raises(RuntimeError, match="Spark text reader returned empty content"):
            _parse_facts_text("", "h", path)
        revenue = [r for r in rows if r["metric"] == "Revenue"]
        assets = [r for r in rows if r["tag"] == "Assets"]
        assert all(r["period_end"] >= "2010-01-01" for r in rows)
        assert all(r["tag"] != "SomeRandomTag" for r in rows)
        assert len(revenue) == 2  # Restatements stay available for Silver deduplication.
        assert all(r["period_type"] == "duration" for r in revenue)
        assert all(r["period_type"] == "instant" for r in assets)
    finally:
        os.unlink(path)


def test_parse_preserves_distinct_quarter_and_ytd_starts():
    facts = {"cik": "1", "facts": {"us-gaap": {
        "Revenues": {"units": {"USD": [
            {"accn": "a", "end": "2024-06-30", "start": "2024-04-01", "val": 30,
             "form": "10-Q", "fp": "Q2", "fy": 2024, "filed": "2024-08-01"},
            {"accn": "a", "end": "2024-06-30", "start": "2024-01-01", "val": 60,
             "form": "10-Q", "fp": "Q2", "fy": 2024, "filed": "2024-08-01"},
        ]}}
    }}}
    with tempfile.NamedTemporaryFile(suffix=".gz", delete=False) as tmp:
        path = tmp.name
    try:
        _write_gz(path, facts)
        rows = parse_facts_payload(path, "h")
        assert {(r["period_start"], r["value"]) for r in rows} == {
            ("2024-04-01", 30.0), ("2024-01-01", 60.0)
        }
    finally:
        os.unlink(path)


def test_parse_sums_revenue_components_and_excludes_investments_from_cash():
    facts = {"cik": "1", "facts": {"us-gaap": {
        "SalesRevenueGoodsNet": {"units": {"USD": [{
            "accn": "a", "start": "2024-01-01", "end": "2024-12-31", "val": 100,
            "form": "10-K", "fp": "FY", "fy": 2024, "filed": "2025-02-01",
        }]}},
        "SalesRevenueServicesNet": {"units": {"USD": [{
            "accn": "a", "start": "2024-01-01", "end": "2024-12-31", "val": 25,
            "form": "10-K", "fp": "FY", "fy": 2024, "filed": "2025-02-01",
        }]}},
        "CashCashEquivalentsAndShortTermInvestments": {"units": {"USD": [{
            "accn": "a", "end": "2024-12-31", "val": 50,
            "form": "10-K", "fp": "FY", "fy": 2024, "filed": "2025-02-01",
        }]}},
    }}}

    rows = _parse_facts_data(facts, "h")
    revenue = [row for row in rows if row["metric"] == "Revenue"]

    assert len(revenue) == 1
    assert revenue[0]["tag"] == "SalesRevenueGoodsNet+SalesRevenueServicesNet"
    assert revenue[0]["value"] == 125.0
    assert all(row["metric"] != "Cash" for row in rows)


def test_spark_dedup_keeps_quarter_and_ytd_and_selects_latest_restatement():
    import pytest

    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession
    from sec_filing_analytics.silver.observations import _deduplicate_restatements
    from sec_filing_analytics.silver.schema import SILVER_OBSERVATION_SCHEMA

    try:
        spark = SparkSession.builder.master("local[1]").appName("silver-observation-test").getOrCreate()
    except Exception as exc:
        pytest.skip(f"Local Spark runtime is unavailable: {exc}")

    rows = [
        ("0000000001", "a1", "Revenues", "Revenue", "duration", "2024-04-01", "2024-06-30", 2024, "Q2", "10-Q", "USD", 30.0, "2024-08-01", "h1"),
        ("0000000001", "a2", "Revenues", "Revenue", "duration", "2024-04-01", "2024-06-30", 2024, "Q2", "10-Q/A", "USD", 35.0, "2024-09-01", "h2"),
        ("0000000001", "a1", "Revenues", "Revenue", "duration", "2024-01-01", "2024-06-30", 2024, "Q2", "10-Q", "USD", 60.0, "2024-08-01", "h1"),
        ("0000000001", "d1", "LongTermDebt", "Long-term Debt", "instant", None, "2023-12-31", 2023, "FY", "10-K", "USD", -10.0, "2024-02-01", "h3"),
        ("0000000001", "d1", "LongTermDebtAndCapitalLeaseObligations", "Long-term Debt", "instant", None, "2023-12-31", 2023, "FY", "10-K", "USD", 8.0, "2024-02-01", "h3"),
    ]
    try:
        deduped = _deduplicate_restatements(spark.createDataFrame(rows, SILVER_OBSERVATION_SCHEMA))
        actual = {(row.metric, row.tag, row.period_start, row.value) for row in deduped.collect()}
        assert actual == {
            ("Revenue", "Revenues", "2024-04-01", 35.0),
            ("Revenue", "Revenues", "2024-01-01", 60.0),
            ("Long-term Debt", "LongTermDebtAndCapitalLeaseObligations", None, 8.0),
        }
    finally:
        spark.stop()


def test_spark_observation_rebuild_refuses_empty_snapshot():
    import pytest

    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession
    from sec_filing_analytics.silver.observations import replace_silver_observations
    from sec_filing_analytics.silver.schema import SILVER_OBSERVATION_SCHEMA

    try:
        spark = SparkSession.builder.master("local[1]").appName("silver-empty-rebuild-test").getOrCreate()
    except Exception as exc:
        pytest.skip(f"Local Spark runtime is unavailable: {exc}")

    try:
        empty = spark.createDataFrame([], SILVER_OBSERVATION_SCHEMA)
        with pytest.raises(RuntimeError, match="refusing to replace the existing snapshot"):
            replace_silver_observations(spark, empty, "catalog")
    finally:
        spark.stop()

