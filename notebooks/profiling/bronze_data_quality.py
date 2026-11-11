# Databricks notebook source
# Inspect the actual financial data: do the numbers make sense, are there restatements,
# what units appear, and what does the observation structure look like?

import gzip, json
from pyspark.sql import functions as F, Window

facts_manifest = spark.table("sec_analytics.bronze.company_facts_manifest")

apple = facts_manifest.filter(F.col("cik") == "0000320193").select("payload_path", "entity_name").first()
print(f"Company: {apple['entity_name']}")

with gzip.open(apple["payload_path"], "rt", encoding="utf-8") as f:
    data = json.load(f)

us_gaap = data["facts"]["us-gaap"]

# COMMAND ----------

# Annual 10-K financials since 2010 — sanity check that the numbers look right.
# If these look wrong, Silver will also be wrong.

TAGS = {
    "Revenue":      "RevenueFromContractWithCustomerExcludingAssessedTax",
    "Net Income":   "NetIncomeLoss",
    "Total Assets": "Assets",
    "Operating CF": "NetCashProvidedByUsedInOperatingActivities",
    "LT Debt":      "LongTermDebt",
    "Cash":         "CashAndCashEquivalentsAtCarryingValue",
}

rows = []
for metric, tag in TAGS.items():
    if tag not in us_gaap:
        continue
    for o in us_gaap[tag].get("units", {}).get("USD", []):
        if o.get("form") in ("10-K", "10-K/A") and o.get("fp") == "FY" and o.get("end", "") >= "2010-01-01":
            rows.append({
                "metric":      metric,
                "fiscal_year": o.get("fy"),
                "period_end":  o.get("end"),
                "value_bn":    round(o.get("val", 0) / 1e9, 2),
                "filed":       o.get("filed"),
            })

display(spark.createDataFrame(rows).orderBy("fiscal_year", "metric"))

# COMMAND ----------

# Restatements — same period filed more than once with different values.
# Check if any (metric, fy, fp) combination has multiple filed dates.

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
restated = df_obs.withColumn("n", F.count("*").over(w)).filter(F.col("n") > 1)
print(f"Restated (fy, fp) periods found: {restated.count()}")
restated.orderBy("metric", "fy").show(30, truncate=False)

# COMMAND ----------

# Instant vs duration — balance sheet items have no start date, income statement items do.
# This matters because Silver needs to model them differently.

print("Assets — balance sheet (should have no start date):")
for o in us_gaap.get("Assets", {}).get("units", {}).get("USD", [])[:5]:
    print(f"  start={o.get('start', 'N/A'):<12}  end={o.get('end')}  val={o.get('val'):>15,}")

print("\nRevenue — income statement (should have start + end):")
rev_tag = "RevenueFromContractWithCustomerExcludingAssessedTax"
for o in us_gaap.get(rev_tag, {}).get("units", {}).get("USD", [])[:5]:
    print(f"  start={o.get('start', 'N/A'):<12}  end={o.get('end')}  val={o.get('val'):>15,}")

# COMMAND ----------

# Unit variation — not every tag reports in USD.
# Silver should filter to USD only for monetary metrics.

print("All unit types used across Apple's US-GAAP facts:")
all_units = set()
for tag_data in us_gaap.values():
    for unit in tag_data.get("units", {}).keys():
        all_units.add(unit)
print(sorted(all_units))

