# Databricks notebook source

import gzip, json
from pyspark.sql import functions as F, Window

facts_manifest = spark.table("sec_analytics.bronze.company_facts_manifest")

apple_row = facts_manifest.filter(F.col("cik") == "0000320193") \
    .select("payload_path", "entity_name").first()

with gzip.open(apple_row["payload_path"], "rt", encoding="utf-8") as f:
    apple_data = json.load(f)

us_gaap = apple_data["facts"]["us-gaap"]

TAGS = {
    "Revenue":      "RevenueFromContractWithCustomerExcludingAssessedTax",
    "Net Income":   "NetIncomeLoss",
    "Total Assets": "Assets",
    "Operating CF": "NetCashProvidedByUsedInOperatingActivities",
    "LT Debt":      "LongTermDebt",
    "Cash":         "CashAndCashEquivalentsAtCarryingValue",
}

# COMMAND ----------

# Annual 10-K financials since 2010
annual_rows = []
for metric, tag in TAGS.items():
    for o in us_gaap.get(tag, {}).get("units", {}).get("USD", []):
        if o.get("form") in ("10-K", "10-K/A") and o.get("fp") == "FY" and o.get("end", "") >= "2010-01-01":
            annual_rows.append({
                "metric": metric, "fiscal_year": o.get("fy"),
                "period_end": o.get("end"),
                "value_bn": round(o.get("val", 0) / 1e9, 2),
                "filed": o.get("filed"),
            })

# COMMAND ----------

# Restatements — same (metric, fy, fp) filed more than once
all_obs = []
for metric, tag in TAGS.items():
    for o in us_gaap.get(tag, {}).get("units", {}).get("USD", []):
        if o.get("form") in ("10-K", "10-K/A") and o.get("end", "") >= "2010-01-01":
            all_obs.append({"metric": metric, "fy": o.get("fy"), "fp": o.get("fp"),
                            "val": o.get("val"), "filed": o.get("filed")})

df_obs   = spark.createDataFrame(all_obs)
w        = Window.partitionBy("metric", "fy", "fp")
restated = df_obs.withColumn("n", F.count("*").over(w)).filter(F.col("n") > 1).collect()

# COMMAND ----------

# Instant vs duration — first 5 observations of each type
assets_sample = us_gaap.get("Assets", {}).get("units", {}).get("USD", [])[:5]
rev_tag       = "RevenueFromContractWithCustomerExcludingAssessedTax"
rev_sample    = us_gaap.get(rev_tag, {}).get("units", {}).get("USD", [])[:5]

# All unit types in Apple's facts (not just USD)
all_units = sorted({u for td in us_gaap.values() for u in td.get("units", {}).keys()})

# COMMAND ----------

result = {
    "apple_annual_financials": annual_rows,
    "restatements": {
        "count": len(restated),
        "examples": [r.asDict() for r in restated[:20]],
    },
    "observation_structure": {
        "assets_instant": [{"start": o.get("start"), "end": o.get("end"), "val": o.get("val")} for o in assets_sample],
        "revenue_duration": [{"start": o.get("start"), "end": o.get("end"), "val": o.get("val")} for o in rev_sample],
    },
    "unit_types": all_units,
}

with open("/Volumes/sec_analytics/bronze/raw/stats_quality.json", "w") as f:
    json.dump(result, f, indent=2)

dbutils.notebook.exit(json.dumps({"status": "ok", "annual_obs": len(annual_rows), "restated_periods": len(restated)}))
