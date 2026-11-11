# Databricks notebook source

import json
from pyspark.sql import functions as F

filings      = spark.table("sec_analytics.silver.filing")
observations = spark.table("sec_analytics.silver.observation")

# COMMAND ----------

# Filing stats
filing_total = filings.count()
filing_ciks  = filings.select("cik").distinct().count()

form_counts = [
    r.asDict()
    for r in filings.groupBy("form_type").count().orderBy(F.col("count").desc()).collect()
]

filing_date_range = filings.select(
    F.min("filing_date").alias("min_filing_date"),
    F.max("filing_date").alias("max_filing_date"),
).first().asDict()

# COMMAND ----------

# Observation stats
obs_total = observations.count()
obs_ciks  = observations.select("cik").distinct().count()

metric_summary = [
    r.asDict()
    for r in observations.groupBy("metric").agg(
        F.count("*").alias("observation_count"),
        F.countDistinct("cik").alias("company_count"),
        F.min("period_end").alias("oldest_period"),
        F.max("period_end").alias("newest_period"),
    ).orderBy(F.col("company_count").desc()).collect()
]

# COMMAND ----------

# Apple validation: annual Revenue and Net Income (sanity check against reported 10-Ks)
apple_annual = [
    r.asDict()
    for r in observations.filter(
        (F.col("cik") == "0000320193")
        & (F.col("fiscal_period") == "FY")
        & (F.col("form_type") == "10-K")
        & (F.col("metric").isin("Revenue", "Net Income", "Total Assets"))
    ).select(
        "fiscal_year",
        "metric",
        "period_end",
        F.round(F.col("value") / 1e9, 2).alias("value_billions"),
    ).orderBy("fiscal_year", "metric").collect()
]

# COMMAND ----------

result = {
    "silver_filing": {
        "total_rows": filing_total,
        "unique_companies": filing_ciks,
        "form_distribution": form_counts,
        "date_range": filing_date_range,
    },
    "silver_observation": {
        "total_rows": obs_total,
        "unique_companies": obs_ciks,
        "metrics": metric_summary,
    },
    "apple_sample_financials": apple_annual,
}

with open("/Volumes/sec_analytics/bronze/raw/stats_silver_quality.json", "w") as f:
    json.dump(result, f, indent=2)

dbutils.notebook.exit(json.dumps({
    "status": "ok",
    "filings_count": filing_total,
    "observations_count": obs_total,
    "companies_covered": obs_ciks,
}))
