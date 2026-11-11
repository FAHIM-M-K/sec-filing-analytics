# Databricks notebook source

import gzip, json
from pyspark.sql import functions as F

sub_manifest   = spark.table("sec_analytics.bronze.submission_manifest")
facts_manifest = spark.table("sec_analytics.bronze.company_facts_manifest")

RELEVANT_FORMS = {"10-K", "10-Q", "10-K/A", "10-Q/A"}
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

# COMMAND ----------

# Filing history — all 500 companies
all_sub_paths = [
    r["payload_path"]
    for r in sub_manifest.filter(F.col("payload_path").isNotNull()).select("payload_path").collect()
]

filing_rows = []
for path in all_sub_paths:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            d = json.load(f)
        forms  = d.get("filings", {}).get("recent", {}).get("form", [])
        dates  = d.get("filings", {}).get("recent", {}).get("filingDate", [])
        chunks = d.get("filings", {}).get("files", [])
        since_2010 = [
            {"form": forms[i], "date": dates[i]}
            for i in range(min(len(forms), len(dates)))
            if forms[i] in RELEVANT_FORMS and dates[i] >= "2010-01-01"
        ]
        annual    = [x for x in since_2010 if "10-K" in x["form"]]
        quarterly = [x for x in since_2010 if "10-Q" in x["form"]]
        all_dates = [x["date"] for x in since_2010]
        filing_rows.append({
            "cik": d.get("cik", ""), "company": d.get("name", ""),
            "annual_count": len(annual), "quarterly_count": len(quarterly),
            "oldest": min(all_dates, default=""), "newest": max(all_dates, default=""),
            "has_chunks": len(chunks) > 0,
        })
    except Exception:
        pass

df_filings = spark.createDataFrame(filing_rows)
distribution  = df_filings.groupBy("annual_count").count().orderBy("annual_count").collect()
total         = df_filings.count()
thresholds    = {f">={t}": f"{df_filings.filter(F.col('annual_count')>=t).count()}/{total}" for t in [0,1,5,10,14]}
chunks_count  = df_filings.filter(F.col("has_chunks")).count()
sparse        = df_filings.orderBy("annual_count").limit(10).collect()

# COMMAND ----------

# XBRL tag coverage — 100 sample companies
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

# COMMAND ----------

result = {
    "filing_coverage": {
        "total_companies": total,
        "companies_with_chunk_files": chunks_count,
        "threshold_summary": thresholds,
        "distribution": [r.asDict() for r in distribution],
        "sparse_companies": [r.asDict() for r in sparse],
    },
    "tag_coverage": {
        "companies_sampled": checked,
        "metrics": {
            metric: {
                tag: {"count": cnt, "pct": round(cnt / checked * 100)}
                for tag, cnt in sorted(hits.items(), key=lambda x: -x[1])
            }
            for metric, hits in tag_hits.items()
        }
    }
}

with open("/Volumes/sec_analytics/bronze/raw/stats_coverage.json", "w") as f:
    json.dump(result, f, indent=2)

dbutils.notebook.exit(json.dumps({"status": "ok", "companies_parsed": total, "tags_checked": checked}))
