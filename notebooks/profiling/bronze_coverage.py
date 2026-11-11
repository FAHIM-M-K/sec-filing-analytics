# Databricks notebook source
# How much usable financial data do we actually have across the S&P 500?
# Checks filing depth, metric availability, and tag aliases per company.

import gzip, json
from pyspark.sql import functions as F

sub_manifest   = spark.table("sec_analytics.bronze.submission_manifest")
facts_manifest = spark.table("sec_analytics.bronze.company_facts_manifest")

# COMMAND ----------

# Filing history — 10-K and 10-Q counts per company since 2010
# Using all 500 to get accurate universe-wide numbers, not a sample.

RELEVANT_FORMS = {"10-K", "10-Q", "10-K/A", "10-Q/A"}

all_paths = [
    r["payload_path"]
    for r in sub_manifest.filter(F.col("payload_path").isNotNull())
    .select("payload_path")
    .collect()
]

filing_rows = []

for path in all_paths:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            data = json.load(f)

        cik    = data.get("cik", "")
        name   = data.get("name", "")
        recent = data.get("filings", {}).get("recent", {})
        forms  = recent.get("form", [])
        dates  = recent.get("filingDate", [])

        since_2010 = [
            {"form": forms[i], "date": dates[i]}
            for i in range(min(len(forms), len(dates)))
            if forms[i] in RELEVANT_FORMS and dates[i] >= "2010-01-01"
        ]

        annual    = [f for f in since_2010 if "10-K" in f["form"]]
        quarterly = [f for f in since_2010 if "10-Q" in f["form"]]
        all_dates = [f["date"] for f in since_2010]

        # note: submissions only contain the most recent ~1000 filings;
        # older companies will show fewer than expected here — chunk files hold the rest.
        filing_rows.append({
            "cik":             cik,
            "company":         name,
            "annual_count":    len(annual),
            "quarterly_count": len(quarterly),
            "oldest_filing":   min(all_dates, default=""),
            "newest_filing":   max(all_dates, default=""),
        })
    except Exception:
        pass

df_filings = spark.createDataFrame(filing_rows)
display(df_filings.orderBy("annual_count"))

# COMMAND ----------

# How many companies have at least N years of annual reports?
print("Annual 10-K count distribution:")
df_filings.groupBy("annual_count").count().orderBy("annual_count").show(25)

total = df_filings.count()
for threshold in [1, 5, 10, 14]:
    n = df_filings.filter(F.col("annual_count") >= threshold).count()
    print(f"  >= {threshold} annual filings: {n}/{total} companies ({round(n/total*100)}%)")

# COMMAND ----------

# XBRL tag coverage — which tags exist across the universe for our 8 key metrics?
# Using 100 companies for a reliable estimate.

METRIC_TAGS = {
    "Revenue": [
        "Revenues",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
        "SalesRevenueNet",
        "SalesRevenueGoodsNet",
    ],
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
    .orderBy(F.rand(seed=42))
    .limit(100)
    .select("payload_path")
    .collect()
]

tag_hits = {metric: {} for metric in METRIC_TAGS}
checked  = 0

for path in facts_paths:
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

print(f"\nTag coverage across {checked} sampled companies:\n")
for metric, hits in tag_hits.items():
    companies_covered = sum(1 for _ in hits if hits)
    print(f"{metric}  ({sum(hits.values())} total tag matches)")
    for tag, count in sorted(hits.items(), key=lambda x: -x[1]):
        print(f"    {tag:<65} {count:>3} / {checked}  ({round(count/checked*100)}%)")
    if not hits:
        print("    no matching tag found")
    print()

