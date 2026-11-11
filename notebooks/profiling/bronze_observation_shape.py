# Databricks notebook source
# MAGIC %md
# MAGIC # Observation Quality
# MAGIC Using Apple (most complete data since 2010) to understand what Silver will actually receive:
# MAGIC what does a financial observation look like, do restatements exist, and how do
# MAGIC instant (balance sheet) vs duration (income statement) tags differ structurally?

# COMMAND ----------

import gzip
import json
from pyspark.sql import functions as F, Window

facts_manifest = spark.table("sec_analytics.bronze.company_facts_manifest")

# COMMAND ----------

apple_row = facts_manifest.filter(F.col("cik") == "0000320193") \
    .select("payload_path", "entity_name").first()

print(f"Company: {apple_row['entity_name']}")

with gzip.open(apple_row["payload_path"], "rt", encoding="utf-8") as f:
    apple = json.load(f)

us_gaap = apple["facts"]["us-gaap"]

# COMMAND ----------
# MAGIC %md
# MAGIC ## Annual financials since 2010
# MAGIC Shows what Silver will produce for the financial dashboard — revenue, net income,
# MAGIC assets, cash flow, debt, and cash in one clean table.

# COMMAND ----------

TAGS = {
    "Revenue":       "RevenueFromContractWithCustomerExcludingAssessedTax",
    "Net Income":    "NetIncomeLoss",
    "Total Assets":  "Assets",
    "Operating CF":  "NetCashProvidedByUsedInOperatingActivities",
    "LT Debt":       "LongTermDebt",
    "Cash":          "CashAndCashEquivalentsAtCarryingValue",
}

def annual_obs(us_gaap, tag, since="2010-01-01"):
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
    for o in annual_obs(us_gaap, tag):
        rows.append({
            "metric":      metric,
            "fiscal_year": o.get("fy"),
            "period_end":  o.get("end"),
            "value_bn":    round(o.get("val", 0) / 1e9, 2),
            "filed":       o.get("filed"),
        })

display(spark.createDataFrame(rows).orderBy("fiscal_year", "metric"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Restatements — same period filed more than once
# MAGIC Silver must keep only the latest `filed` per (tag, fy, fp).

# COMMAND ----------

obs_rows = []
for metric, tag in TAGS.items():
    for o in us_gaap.get(tag, {}).get("units", {}).get("USD", []):
        if o.get("form") in ("10-K", "10-K/A") and o.get("end", "") >= "2010-01-01":
            obs_rows.append({
                "metric": metric,
                "fy":     o.get("fy"),
                "fp":     o.get("fp"),
                "end":    o.get("end"),
                "val":    o.get("val"),
                "filed":  o.get("filed"),
            })

df_obs = spark.createDataFrame(obs_rows)
w = Window.partitionBy("metric", "fy", "fp")

print("Periods with more than one filed observation (restatements):")
df_obs.withColumn("n", F.count("*").over(w)) \
      .filter(F.col("n") > 1) \
      .orderBy("metric", "fy") \
      .show(30)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Instant vs Duration — balance sheet vs income statement structure
# MAGIC Instant tags (Assets) have no `start` date — they are a point-in-time snapshot.
# MAGIC Duration tags (Revenue) have both `start` and `end` — they span a period.
# MAGIC Silver must handle these separately.

# COMMAND ----------

print("=== Assets (instant — expect no 'start') ===")
for o in us_gaap.get("Assets", {}).get("units", {}).get("USD", [])[:5]:
    print(f"  start={o.get('start', '(none)'):<12}  end={o.get('end')}  val={o.get('val'):,}")

print()
print("=== Revenue (duration — expect 'start' + 'end') ===")
rev_tag = "RevenueFromContractWithCustomerExcludingAssessedTax"
for o in us_gaap.get(rev_tag, {}).get("units", {}).get("USD", [])[:5]:
    print(f"  start={o.get('start', '(none)'):<12}  end={o.get('end')}  val={o.get('val'):,}")

