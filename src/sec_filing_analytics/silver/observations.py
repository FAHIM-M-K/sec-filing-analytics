import gzip
import json
import logging

_log = logging.getLogger(__name__)

CANONICAL_TAGS: dict[str, list[str]] = {
    "Revenue": [
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
        "Revenues",
        "SalesRevenueNet",
        "SalesRevenueGoodsNet",
    ],
    "Net Income": [
        "NetIncomeLoss",
        "ProfitLoss",
        "NetIncomeLossAvailableToCommonStockholdersDiluted",
    ],
    "Operating Income": [
        "OperatingIncomeLoss",
    ],
    "Total Assets": [
        "Assets",
    ],
    "Total Liabilities": [
        "Liabilities",
    ],
    "Cash": [
        "CashAndCashEquivalentsAtCarryingValue",
        "CashCashEquivalentsAndShortTermInvestments",
        "Cash",
    ],
    "Long-term Debt": [
        "LongTermDebt",
        "LongTermDebtNoncurrent",
    ],
    "Operating Cash Flow": [
        "NetCashProvidedByUsedInOperatingActivities",
    ],
}

_EPS_TAG_TO_METRIC: dict[str, str] = {
    "EarningsPerShareDiluted": "EPS Diluted",
    "EarningsPerShareBasic":   "EPS Basic",
}

_TAG_TO_METRIC: dict[str, str] = {
    tag: metric
    for metric, tags in CANONICAL_TAGS.items()
    for tag in tags
}

RELEVANT_FORMS = {"10-K", "10-Q", "10-K/A", "10-Q/A"}


def parse_facts_payload(path: str, source_hash: str) -> list[dict]:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        data = json.load(f)

    cik     = str(data.get("cik", "")).zfill(10)
    us_gaap = data.get("facts", {}).get("us-gaap", {})

    rows = []
    for tag, metric in _TAG_TO_METRIC.items():
        if tag not in us_gaap:
            continue

        usd_obs = us_gaap[tag].get("units", {}).get("USD", [])
        for o in usd_obs:
            form = o.get("form", "")
            if form not in RELEVANT_FORMS:
                continue

            end = o.get("end", "")
            if end < "2010-01-01":
                continue

            start       = o.get("start")
            period_type = "duration" if start else "instant"

            rows.append({
                "cik":            cik,
                "accession_number": o.get("accn", ""),
                "tag":            tag,
                "metric":         metric,
                "period_type":    period_type,
                "period_start":   start,
                "period_end":     end,
                "fiscal_year":    o.get("fy"),
                "fiscal_period":  o.get("fp", ""),
                "form_type":      form,
                "unit":           "USD",
                "value":          float(o.get("val")) if o.get("val") is not None else None,
                "filed_date":     o.get("filed", ""),
                "source_hash":    source_hash,
            })

    for tag, metric in _EPS_TAG_TO_METRIC.items():
        if tag not in us_gaap:
            continue
        for o in us_gaap[tag].get("units", {}).get("USD/shares", []):
            form = o.get("form", "")
            if form not in RELEVANT_FORMS:
                continue
            end = o.get("end", "")
            if end < "2010-01-01":
                continue
            start = o.get("start")
            rows.append({
                "cik":              cik,
                "accession_number": o.get("accn", ""),
                "tag":              tag,
                "metric":           metric,
                "period_type":      "duration" if start else "instant",
                "period_start":     start,
                "period_end":       end,
                "fiscal_year":      o.get("fy"),
                "fiscal_period":    o.get("fp", ""),
                "form_type":        form,
                "unit":             "USD/shares",
                "value":            float(o.get("val")) if o.get("val") is not None else None,
                "filed_date":       o.get("filed", ""),
                "source_hash":      source_hash,
            })

    return rows


def build_silver_observations(spark, manifest_df):
    from pyspark.sql import functions as F

    paths = [
        (r["payload_path"], r["content_hash"])
        for r in manifest_df.filter(F.col("payload_path").isNotNull())
        .select("payload_path", "content_hash")
        .collect()
    ]

    all_rows = []
    for path, hash_ in paths:
        try:
            all_rows.extend(parse_facts_payload(path, hash_))
        except Exception as exc:
            _log.warning("Skipping payload %s: %s", path, exc)

    from sec_filing_analytics.silver.schema import SILVER_OBSERVATION_SCHEMA

    if not all_rows:
        return spark.createDataFrame([], schema=SILVER_OBSERVATION_SCHEMA)

    df = spark.createDataFrame(all_rows, schema=SILVER_OBSERVATION_SCHEMA)
    df = _deduplicate_restatements(df)
    return df


def _deduplicate_restatements(df):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    _base = sum(len(v) for v in CANONICAL_TAGS.values())
    tag_priority: dict[str, int] = {
        tag: idx
        for metric_tags in CANONICAL_TAGS.values()
        for idx, tag in enumerate(metric_tags)
    }
    for i, tag in enumerate(_EPS_TAG_TO_METRIC):
        tag_priority[tag] = _base + i
    priority_expr = F.create_map(
        *[x for tag, pri in tag_priority.items() for x in (F.lit(tag), F.lit(pri))]
    )

    w1 = Window.partitionBy(
        "cik", "tag", "period_end", "fiscal_period", "form_type"
    ).orderBy(F.col("filed_date").desc())

    df = (
        df.withColumn("_rn1", F.row_number().over(w1))
        .filter(F.col("_rn1") == 1)
        .drop("_rn1")
    )

    df = df.withColumn("_tag_pri", priority_expr[F.col("tag")])

    w2 = Window.partitionBy(
        "cik", "metric", "period_end", "fiscal_period", "form_type"
    ).orderBy(F.col("_tag_pri").asc())

    return (
        df.withColumn("_rn2", F.row_number().over(w2))
        .filter(F.col("_rn2") == 1)
        .drop("_rn2", "_tag_pri")
    )


def upsert_silver_observations(spark, df, catalog: str = "sec_analytics") -> dict:
    from delta.tables import DeltaTable

    table_name = f"{catalog}.silver.observation"
    target     = DeltaTable.forName(spark, table_name)

    merge_keys = (
        "t.cik = s.cik AND "
        "t.tag = s.tag AND "
        "t.period_end = s.period_end AND "
        "t.fiscal_period = s.fiscal_period AND "
        "t.form_type = s.form_type"
    )

    (
        target.alias("t")
        .merge(df.alias("s"), merge_keys)
        .whenMatchedUpdate(set={
            "value":       "s.value",
            "filed_date":  "s.filed_date",
            "source_hash": "s.source_hash",
        })
        .whenNotMatchedInsertAll()
        .execute()
    )

    total = spark.table(table_name).count()
    return {"total_rows": total}
