# Databricks notebook source

import gzip, json
from pyspark.sql import functions as F

sub_manifest   = spark.table("sec_analytics.bronze.submission_manifest")
facts_manifest = spark.table("sec_analytics.bronze.company_facts_manifest")

RELEVANT_FORMS = {"10-K", "10-Q", "10-K/A", "10-Q/A"}

# COMMAND ----------

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
        all_dates = [x["date"] for x in since_2010]
        filing_rows.append({
            "cik":          d.get("cik", ""),
            "company":      d.get("name", ""),
            "annual_count": len(annual),
            "oldest":       min(all_dates, default=""),
            "has_chunks":   len(chunks) > 0,
        })
    except Exception:
        pass

df = spark.createDataFrame(filing_rows)
total        = df.count()
chunks_count = df.filter(F.col("has_chunks")).count()
thresholds   = {
    f">={t}": f"{df.filter(F.col('annual_count') >= t).count()}/{total}"
    for t in [1, 5, 10, 14]
}

# COMMAND ----------

result = {
    "total_companies": total,
    "companies_with_older_chunk_files": chunks_count,
    "annual_filing_thresholds": thresholds,
    "sparse_companies": [r.asDict() for r in df.orderBy("annual_count").limit(10).collect()],
}

with open("/Volumes/sec_analytics/bronze/raw/stats_coverage.json", "w") as f:
    json.dump(result, f, indent=2)

dbutils.notebook.exit(json.dumps({"status": "ok", "companies_parsed": total}))
