# Databricks notebook source
# MAGIC %md
# MAGIC # Filing Counts
# MAGIC How many 10-K and 10-Q filings do we have per company since 2010?
# MAGIC Which companies have enough history to be useful, and which are gaps?

# COMMAND ----------

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
# MAGIC %md
# MAGIC ## 10-K and 10-Q counts per company

# COMMAND ----------

rows = []

for path in sample_paths:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            data = json.load(f)

        cik  = data.get("cik", "")
        name = data.get("name", "")
        recent = data.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])

        since_2010 = [
            {"form": forms[i], "date": dates[i]}
            for i in range(len(forms))
            if forms[i] in RELEVANT_FORMS and i < len(dates) and dates[i] >= "2010-01-01"
        ]

        annual    = [f for f in since_2010 if f["form"] in {"10-K", "10-K/A"}]
        quarterly = [f for f in since_2010 if f["form"] in {"10-Q", "10-Q/A"}]
        dates_only = [f["date"] for f in since_2010]

        rows.append({
            "cik": cik,
            "company": name,
            "annual_filings": len(annual),
            "quarterly_filings": len(quarterly),
            "oldest_filing": min(dates_only, default="N/A"),
            "newest_filing": max(dates_only, default="N/A"),
        })
    except Exception:
        pass

df = spark.createDataFrame(rows)
display(df.orderBy("annual_filings"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Distribution — how many companies have a full history?

# COMMAND ----------

df.groupBy("annual_filings").count().orderBy("annual_filings").show(25)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 10-K document details — what Silver needs to fetch Item 1A

# COMMAND ----------

# Silver needs the accession number and primaryDocument to build the EDGAR URL for each 10-K.
doc_rows = []

for path in sample_paths:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            data = json.load(f)
        cik  = data.get("cik", "")
        name = data.get("name", "")
        recent = data.get("filings", {}).get("recent", {})
        forms      = recent.get("form", [])
        dates      = recent.get("filingDate", [])
        docs       = recent.get("primaryDocument", [])
        accessions = recent.get("accessionNumber", [])

        for i, form in enumerate(forms):
            if form == "10-K" and i < len(dates) and dates[i] >= "2010-01-01":
                doc_rows.append({
                    "cik": cik,
                    "company": name,
                    "filing_date": dates[i],
                    "accession": accessions[i] if i < len(accessions) else "",
                    "primary_document": docs[i] if i < len(docs) else "",
                })
    except Exception:
        pass

df_docs = spark.createDataFrame(doc_rows)
display(df_docs.orderBy(F.col("filing_date").desc()))

