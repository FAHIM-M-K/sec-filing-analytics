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
all_obs = []
for metric, tag in TAGS.items():
    for o in us_gaap.get(tag, {}).get("units", {}).get("USD", []):
        if o.get("form") in ("10-K", "10-K/A") and o.get("end", "") >= "2010-01-01":
            all_obs.append({
                "metric": metric, "end": o.get("end"),
                "fp": o.get("fp"), "filed": o.get("filed"),
            })

df_obs    = spark.createDataFrame(all_obs)
w         = Window.partitionBy("metric", "end", "fp")
restat_ct = df_obs.withColumn("n", F.count("*").over(w)).filter(F.col("n") > 1).count()

# COMMAND ----------

result = {
    "apple_annual_financials": annual_rows,
    "restatement_pattern": {
        "restated_period_obs_count": restat_ct,
        "note": "Same period re-filed across multiple 10-Ks as comparative data. Not accounting errors.",
    },
}

with open("/Volumes/sec_analytics/bronze/raw/stats_quality.json", "w") as f:
    json.dump(result, f, indent=2)

dbutils.notebook.exit(json.dumps({"status": "ok", "annual_obs": len(annual_rows), "restated_obs": restat_ct}))
