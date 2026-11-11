# Databricks notebook source

import gzip
import json
from pyspark.sql import functions as F

sub_manifest = spark.table("sec_analytics.bronze.submission_manifest")

# COMMAND ----------

sample_paths = [
    r["payload_path"]
    for r in sub_manifest.filter(F.col("payload_path").isNotNull())
    .orderBy(F.rand(seed=42))
    .limit(50)
    .select("payload_path")
    .collect()
]

RELEVANT_FORMS = {"10-K", "10-Q", "10-K/A", "10-Q/A"}

# COMMAND ----------

rows = []

for path in sample_paths:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            data = json.load(f)

        cik    = data.get("cik", "")
        name   = data.get("name", "")
        recent = data.get("filings", {}).get("recent", {})
        forms  = recent.get("form", [])
        dates  = recent.get("filingDate", [])
        docs   = recent.get("primaryDocument", [])
        accs   = recent.get("accessionNumber", [])

        filings_since_2010 = [
            {
                "form":      forms[i],
                "date":      dates[i],
                "accession": accs[i]  if i < len(accs)  else "",
                "doc":       docs[i]  if i < len(docs)  else "",
            }
            for i in range(len(forms))
            if forms[i] in RELEVANT_FORMS and i < len(dates) and dates[i] >= "2010-01-01"
        ]

        annual    = [f for f in filings_since_2010 if f["form"] in {"10-K", "10-K/A"}]
        quarterly = [f for f in filings_since_2010 if f["form"] in {"10-Q", "10-Q/A"}]
        all_dates = [f["date"] for f in filings_since_2010]

        rows.append({
            "cik":               cik,
            "company":           name,
            "annual_count":      len(annual),
            "quarterly_count":   len(quarterly),
            "oldest_filing":     min(all_dates, default=""),
            "newest_filing":     max(all_dates, default=""),
            "latest_10k_date":   max((f["date"] for f in annual), default=""),
            "latest_10k_doc":    max(annual, key=lambda x: x["date"], default={}).get("doc", ""),
            "latest_10k_acc":    max(annual, key=lambda x: x["date"], default={}).get("accession", ""),
        })
    except Exception:
        pass

df = spark.createDataFrame(rows)
display(df.orderBy(F.col("annual_count").asc()))

# COMMAND ----------

df.groupBy("annual_count").count().orderBy("annual_count").show(25)
