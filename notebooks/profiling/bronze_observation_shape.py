# Databricks notebook source

import gzip
import json
from pyspark.sql import functions as F, Window

facts_manifest = spark.table("sec_analytics.bronze.company_facts_manifest")

apple_row = facts_manifest.filter(F.col("cik") == "0000320193") \
    .select("payload_path", "entity_name").first()

print(f"Company: {apple_row['entity_name']}")

with gzip.open(apple_row["payload_path"], "rt", encoding="utf-8") as f:
    apple = json.load(f)

us_gaap = apple["facts"]["us-gaap"]

# COMMAND ----------

# annual 10-K financials since 2010
TAGS = {
    "Revenue":      "RevenueFromContractWithCustomerExcludingAssessedTax",
    "Net Income":   "NetIncomeLoss",
    "Total Assets": "Assets",
    "Operating CF": "NetCashProvidedByUsedInOperatingActivities",
    "LT Debt":      "LongTermDebt",
    "Cash":         "CashAndCashEquivalentsAtCarryingValue",
}

def fy_obs(tag, since="2010-01-01"):
    if tag not in us_gaap:
        return []
    return [
        o for o in us_gaap[tag].get("units", {}).get("USD", [])
        if o.get("form") in ("10-K", "10-K/A")
        and o.get("fp") == "FY"
        and o.get("end", "") >= since
    ]

rows = []
for metric, tag in TAGS.items():
    for o in fy_obs(tag):
        rows.append({
            "metric":      metric,
            "fiscal_year": o.get("fy"),
            "period_end":  o.get("end"),
            "value_bn":    round(o.get("val", 0) / 1e9, 2),
            "filed":       o.get("filed"),
        })

display(spark.createDataFrame(rows).orderBy("fiscal_year", "metric"))

# COMMAND ----------

# check for restated periods — same (metric, fy, fp) filed more than once
all_obs = []
for metric, tag in TAGS.items():
    for o in us_gaap.get(tag, {}).get("units", {}).get("USD", []):
        if o.get("form") in ("10-K", "10-K/A") and o.get("end", "") >= "2010-01-01":
            all_obs.append({
                "metric": metric,
                "fy":     o.get("fy"),
                "fp":     o.get("fp"),
                "val":    o.get("val"),
                "filed":  o.get("filed"),
            })

df_obs = spark.createDataFrame(all_obs)
w = Window.partitionBy("metric", "fy", "fp")

df_obs.withColumn("n", F.count("*").over(w)) \
      .filter(F.col("n") > 1) \
      .orderBy("metric", "fy") \
      .show(30, truncate=False)

# COMMAND ----------

# instant vs duration — do balance sheet tags have a start date or just end?
print("Assets (balance sheet):")
for o in us_gaap.get("Assets", {}).get("units", {}).get("USD", [])[:5]:
    print(f"  start={o.get('start', 'N/A'):<12}  end={o.get('end')}  val={o.get('val'):,}")

print("\nRevenue (income statement):")
rev_tag = "RevenueFromContractWithCustomerExcludingAssessedTax"
for o in us_gaap.get(rev_tag, {}).get("units", {}).get("USD", [])[:5]:
    print(f"  start={o.get('start', 'N/A'):<12}  end={o.get('end')}  val={o.get('val'):,}")
