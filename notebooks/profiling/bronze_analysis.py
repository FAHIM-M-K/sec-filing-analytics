# Databricks notebook source

import gzip, json
from pyspark.sql import functions as F, Window

sub_manifest   = spark.table("sec_analytics.bronze.submission_manifest")
facts_manifest = spark.table("sec_analytics.bronze.company_facts_manifest")

report = {}

# --- 1. Manifest stats ---
sub_stats = sub_manifest.select(
    F.count("*").alias("rows"),
    F.countDistinct("cik").alias("unique_ciks"),
    F.round(F.sum(F.col("response_size_bytes") / 1e6), 2).alias("total_mb"),
    F.sum(F.col("payload_path").isNull().cast("int")).alias("missing_payloads"),
).first().asDict()

facts_stats = facts_manifest.select(
    F.count("*").alias("rows"),
    F.countDistinct("cik").alias("unique_ciks"),
    F.round(F.sum(F.col("response_size_bytes") / 1e6), 2).alias("total_mb"),
    F.round(F.avg("us_gaap_tag_count"), 1).alias("avg_gaap_tags"),
    F.min("us_gaap_tag_count").alias("min_gaap_tags"),
    F.max("us_gaap_tag_count").alias("max_gaap_tags"),
    F.sum(F.col("payload_path").isNull().cast("int")).alias("missing_payloads"),
).first().asDict()

report["manifest"] = {
    "submissions": {k: str(v) for k, v in sub_stats.items()},
    "facts":       {k: str(v) for k, v in facts_stats.items()},
}

# bottom 10 by tag count — companies with minimal data
low_tag = facts_manifest.select("cik", "entity_name", "us_gaap_tag_count") \
    .orderBy("us_gaap_tag_count").limit(10).collect()
report["low_gaap_tag_companies"] = [r.asDict() for r in low_tag]

# --- 2. Filing history — all 500 companies ---
RELEVANT_FORMS = {"10-K", "10-Q", "10-K/A", "10-Q/A"}

all_sub_paths = [
    r["payload_path"]
    for r in sub_manifest.filter(F.col("payload_path").isNotNull()).select("payload_path").collect()
]

filing_rows = []
for path in all_sub_paths:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            d = json.load(f)
        cik    = d.get("cik", "")
        name   = d.get("name", "")
        recent = d.get("filings", {}).get("recent", {})
        forms  = recent.get("form", [])
        dates  = recent.get("filingDate", [])
        chunks = d.get("filings", {}).get("files", [])  # older chunk file refs

        since_2010 = [
            {"form": forms[i], "date": dates[i]}
            for i in range(min(len(forms), len(dates)))
            if forms[i] in RELEVANT_FORMS and dates[i] >= "2010-01-01"
        ]
        annual    = [f for f in since_2010 if "10-K" in f["form"]]
        quarterly = [f for f in since_2010 if "10-Q" in f["form"]]
        all_dates = [f["date"] for f in since_2010]

        filing_rows.append({
            "cik":             cik,
            "company":         name,
            "annual_count":    len(annual),
            "quarterly_count": len(quarterly),
            "oldest":          min(all_dates, default=""),
            "newest":          max(all_dates, default=""),
            "has_chunks":      len(chunks) > 0,
        })
    except Exception:
        pass

df_filings = spark.createDataFrame(filing_rows)
distribution = df_filings.groupBy("annual_count").count().orderBy("annual_count").collect()
total = df_filings.count()
thresholds = {}
for t in [0, 1, 5, 10, 14]:
    n = df_filings.filter(F.col("annual_count") >= t).count()
    thresholds[f">= {t} annual"] = f"{n}/{total} ({round(n/total*100)}%)"

companies_with_chunks = df_filings.filter(F.col("has_chunks")).count()

report["filing_coverage"] = {
    "total_companies_parsed":    total,
    "companies_with_chunk_files": companies_with_chunks,
    "annual_count_distribution": [r.asDict() for r in distribution],
    "threshold_summary":         thresholds,
}

# 10 companies with fewest annual filings
sparse = df_filings.orderBy("annual_count").limit(10).collect()
report["sparse_filing_companies"] = [r.asDict() for r in sparse]

# --- 3. XBRL tag coverage across 100 sample companies ---
METRIC_TAGS = {
    "Revenue":             ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax",
                            "RevenueFromContractWithCustomerIncludingAssessedTax",
                            "SalesRevenueNet", "SalesRevenueGoodsNet"],
    "Net Income":          ["NetIncomeLoss", "ProfitLoss"],
    "Operating Income":    ["OperatingIncomeLoss"],
    "Total Assets":        ["Assets"],
    "Total Liabilities":   ["Liabilities"],
    "Cash":                ["CashAndCashEquivalentsAtCarryingValue", "Cash"],
    "Long-term Debt":      ["LongTermDebt", "LongTermDebtNoncurrent"],
    "Operating Cash Flow": ["NetCashProvidedByUsedInOperatingActivities"],
}

facts_paths = [
    r["payload_path"]
    for r in facts_manifest.filter(F.col("payload_path").isNotNull())
    .orderBy(F.rand(seed=42)).limit(100).select("payload_path").collect()
]

tag_hits = {metric: {} for metric in METRIC_TAGS}
checked  = 0
for path in facts_paths:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            d = json.load(f)
        us_gaap = d.get("facts", {}).get("us-gaap", {})
        checked += 1
        for metric, tags in METRIC_TAGS.items():
            for tag in tags:
                if tag in us_gaap:
                    tag_hits[metric][tag] = tag_hits[metric].get(tag, 0) + 1
    except Exception:
        pass

report["tag_coverage"] = {
    "companies_sampled": checked,
    "metrics": {
        metric: {
            tag: {"count": cnt, "pct": round(cnt / checked * 100)}
            for tag, cnt in sorted(hits.items(), key=lambda x: -x[1])
        }
        for metric, hits in tag_hits.items()
    }
}

# --- 4. Apple deep-dive: actual financial numbers + quality checks ---
apple_row = facts_manifest.filter(F.col("cik") == "0000320193") \
    .select("payload_path", "entity_name").first()

with gzip.open(apple_row["payload_path"], "rt", encoding="utf-8") as f:
    apple_data = json.load(f)

us_gaap = apple_data["facts"]["us-gaap"]

APPLE_TAGS = {
    "Revenue":      "RevenueFromContractWithCustomerExcludingAssessedTax",
    "Net Income":   "NetIncomeLoss",
    "Total Assets": "Assets",
    "Operating CF": "NetCashProvidedByUsedInOperatingActivities",
    "LT Debt":      "LongTermDebt",
    "Cash":         "CashAndCashEquivalentsAtCarryingValue",
}

apple_annual = []
for metric, tag in APPLE_TAGS.items():
    if tag not in us_gaap:
        apple_annual.append({"metric": metric, "tag": tag, "status": "TAG_MISSING"})
        continue
    obs = [
        o for o in us_gaap[tag].get("units", {}).get("USD", [])
        if o.get("form") in ("10-K", "10-K/A") and o.get("fp") == "FY" and o.get("end", "") >= "2010-01-01"
    ]
    for o in obs:
        apple_annual.append({
            "metric":      metric,
            "fiscal_year": o.get("fy"),
            "period_end":  o.get("end"),
            "value_bn":    round(o.get("val", 0) / 1e9, 2),
            "filed":       o.get("filed"),
        })

report["apple_annual_financials"] = apple_annual

# Restatements: same (metric, fy, fp) filed more than once
all_obs = []
for metric, tag in APPLE_TAGS.items():
    for o in us_gaap.get(tag, {}).get("units", {}).get("USD", []):
        if o.get("form") in ("10-K", "10-K/A") and o.get("end", "") >= "2010-01-01":
            all_obs.append({"metric": metric, "fy": o.get("fy"), "fp": o.get("fp"),
                            "val": o.get("val"), "filed": o.get("filed")})

df_obs = spark.createDataFrame(all_obs)
w = Window.partitionBy("metric", "fy", "fp")
restated_rows = df_obs.withColumn("n", F.count("*").over(w)).filter(F.col("n") > 1).collect()
report["apple_restatements"] = {
    "restated_period_count": len(restated_rows),
    "examples": [r.asDict() for r in restated_rows[:20]],
}

# Instant vs duration sample
assets_sample = us_gaap.get("Assets", {}).get("units", {}).get("USD", [])[:5]
rev_tag       = "RevenueFromContractWithCustomerExcludingAssessedTax"
rev_sample    = us_gaap.get(rev_tag, {}).get("units", {}).get("USD", [])[:5]

report["observation_structure"] = {
    "assets_instant": [
        {"start": o.get("start", None), "end": o.get("end"), "val": o.get("val")}
        for o in assets_sample
    ],
    "revenue_duration": [
        {"start": o.get("start", None), "end": o.get("end"), "val": o.get("val")}
        for o in rev_sample
    ],
}

# Unit types used across Apple
all_units = sorted({u for td in us_gaap.values() for u in td.get("units", {}).keys()})
report["unit_types_in_apple"] = all_units

# --- 5. Write to volume ---
out_path = "/Volumes/sec_analytics/bronze/raw/eda_findings.json"
with open(out_path, "w") as f:
    json.dump(report, f, indent=2)

print(f"Written to {out_path}")
dbutils.notebook.exit(json.dumps({"status": "ok", "out_path": out_path}))
