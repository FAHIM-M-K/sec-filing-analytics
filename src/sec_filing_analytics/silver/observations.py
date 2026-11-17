import gzip
import json

_REVENUE_COMPONENT_TAGS = ("SalesRevenueGoodsNet", "SalesRevenueServicesNet")
_REVENUE_COMPONENT_TAG = "+".join(_REVENUE_COMPONENT_TAGS)

CANONICAL_TAGS: dict[str, list[str]] = {
    "Revenue": [
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
        "Revenues",
        "SalesRevenueNet",
        "SalesRevenueGoodsNet",
        "RevenuesNetOfInterestExpense",
        "InterestAndDividendIncomeOperating",
        "RegulatedAndUnregulatedOperatingRevenue",
        "OperatingLeaseLeaseIncome",
        "OilAndGasRevenue",
        "SalesRevenueServicesNet",
    ],
    "Net Income": [
        "NetIncomeLoss",
        "ProfitLoss",
        "NetIncomeLossAvailableToCommonStockholdersDiluted",
        "NetIncomeLossAvailableToCommonStockholdersBasic",
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
        "Cash",
    ],
    "Long-term Debt": [
        "LongTermDebt",
        "LongTermDebtNoncurrent",
        "LongTermDebtAndCapitalLeaseObligations",
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

    return _parse_facts_data(data, source_hash)


def _parse_facts_text(content: str, source_hash: str, path: str) -> list[dict]:
    try:
        if not content:
            raise ValueError("Spark text reader returned empty content")
        data = json.loads(content)
        return _parse_facts_data(data, source_hash)
    except Exception as exc:
        raise RuntimeError(f"Failed to parse company-facts payload {path}: {exc}") from exc


def _parse_facts_data(data: dict, source_hash: str) -> list[dict]:
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

    return _combine_revenue_components(rows)


def _combine_revenue_components(rows: list[dict]) -> list[dict]:
    component_fields = (
        "cik", "accession_number", "period_start", "period_end", "fiscal_year",
        "fiscal_period", "form_type", "filed_date",
    )
    components: dict[tuple, list[dict]] = {}
    result = []
    for row in rows:
        if row["tag"] not in _REVENUE_COMPONENT_TAGS:
            result.append(row)
            continue
        key = tuple(row[field] for field in component_fields)
        components.setdefault(key, []).append(row)

    for component_rows in components.values():
        by_tag = {tag: [row for row in component_rows if row["tag"] == tag]
                  for tag in _REVENUE_COMPONENT_TAGS}
        if all(len(by_tag[tag]) == 1 and by_tag[tag][0]["value"] is not None
               for tag in _REVENUE_COMPONENT_TAGS):
            combined = dict(by_tag[_REVENUE_COMPONENT_TAGS[0]][0])
            combined["tag"] = _REVENUE_COMPONENT_TAG
            combined["value"] = sum(by_tag[tag][0]["value"] for tag in _REVENUE_COMPONENT_TAGS)
            result.append(combined)
        else:
            result.extend(component_rows)
    return result


def build_silver_observations(spark, manifest_df):
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType
    from sec_filing_analytics.silver.manifests import latest_payload_paths
    from sec_filing_analytics.silver.schema import SILVER_OBSERVATION_SCHEMA

    paths = latest_payload_paths(manifest_df)
    manifest = spark.createDataFrame(
        [(path, source_hash, path.rsplit("/", 1)[-1]) for path, source_hash in paths],
        ["payload_path", "source_hash", "payload_file_name"],
    )
    payload_files = (
        spark.read.format("text")
        .option("wholetext", True)
        .load([path for path, _ in paths])
        .select(F.col("_metadata.file_name").alias("payload_file_name"), F.col("value").alias("content"))
    )
    payloads = manifest.join(payload_files, on="payload_file_name", how="left")
    parse_payload = F.udf(_parse_facts_text, ArrayType(SILVER_OBSERVATION_SCHEMA))
    df = (
        payloads
        .select(F.explode(parse_payload("content", "source_hash", "payload_path")).alias("observation"))
        .select("observation.*")
    )
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
    tag_priority[_REVENUE_COMPONENT_TAG] = tag_priority[_REVENUE_COMPONENT_TAGS[0]]
    for i, tag in enumerate(_EPS_TAG_TO_METRIC):
        tag_priority[tag] = _base + i
    priority_expr = F.create_map(
        *[x for tag, pri in tag_priority.items() for x in (F.lit(tag), F.lit(pri))]
    )

    w1 = Window.partitionBy(
        "cik", "tag", "period_start", "period_end", "fiscal_period"
    ).orderBy(
        F.col("filed_date").desc_nulls_last(),
        F.col("accession_number").desc_nulls_last(),
        F.col("source_hash").desc_nulls_last(),
    )

    df = (
        df.withColumn("_rn1", F.row_number().over(w1))
        .filter(F.col("_rn1") == 1)
        .drop("_rn1")
    )

    df = df.withColumn("_tag_pri", priority_expr[F.col("tag")])

    w2 = Window.partitionBy(
        "cik", "metric", "period_start", "period_end", "fiscal_period"
    ).orderBy(F.col("_tag_pri").asc())

    return (
        df.withColumn("_rn2", F.row_number().over(w2))
        .filter(F.col("_rn2") == 1)
        .drop("_rn2", "_tag_pri")
    )


def replace_silver_observations(spark, df, catalog: str = "sec_analytics") -> dict:
    """Rebuild the observation snapshot so removed/restated facts cannot linger."""
    table_name = f"{catalog}.silver.observation"
    df.persist()
    try:
        total = df.count()
        if total == 0:
            raise RuntimeError("Parsed Silver observations are empty; refusing to replace the existing snapshot.")
        (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
         .partitionBy("metric").saveAsTable(table_name))
        return {"total_rows": total}
    finally:
        df.unpersist()
