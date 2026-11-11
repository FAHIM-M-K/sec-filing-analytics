# Databricks notebook source

import gzip
import json
from pyspark.sql import functions as F

facts_manifest = spark.table("sec_analytics.bronze.company_facts_manifest")

# COMMAND ----------

sample_paths = [
    r["payload_path"]
    for r in facts_manifest.filter(F.col("payload_path").isNotNull())
    .orderBy(F.rand(seed=42))
    .limit(50)
    .select("payload_path")
    .collect()
]

METRIC_TAGS = {
    "Revenue": [
        "Revenues",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
        "SalesRevenueNet",
        "SalesRevenueGoodsNet",
    ],
    "Net Income": [
        "NetIncomeLoss",
        "ProfitLoss",
        "NetIncomeLossAvailableToCommonStockholdersDiluted",
    ],
    "Operating Income": ["OperatingIncomeLoss"],
    "Total Assets":     ["Assets"],
    "Total Liabilities":["Liabilities"],
    "Cash": [
        "CashAndCashEquivalentsAtCarryingValue",
        "CashCashEquivalentsAndShortTermInvestments",
        "Cash",
    ],
    "Long-term Debt":     ["LongTermDebt", "LongTermDebtNoncurrent"],
    "Operating Cash Flow":["NetCashProvidedByUsedInOperatingActivities"],
}

# COMMAND ----------

tag_hits = {metric: {} for metric in METRIC_TAGS}
checked  = 0

for path in sample_paths:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            data = json.load(f)
        us_gaap = data.get("facts", {}).get("us-gaap", {})
        checked += 1
        for metric, tags in METRIC_TAGS.items():
            for tag in tags:
                if tag in us_gaap:
                    tag_hits[metric][tag] = tag_hits[metric].get(tag, 0) + 1
    except Exception:
        pass

print(f"Companies checked: {checked}\n")
for metric, hits in tag_hits.items():
    total = sum(hits.values())
    print(f"{metric}  —  {total} tag matches across {checked} companies")
    for tag, count in sorted(hits.items(), key=lambda x: -x[1]):
        print(f"    {tag:<65} {count:>3}  ({round(count/checked*100)}%)")
    if not hits:
        print("    no matching tag found in sample")
    print()
