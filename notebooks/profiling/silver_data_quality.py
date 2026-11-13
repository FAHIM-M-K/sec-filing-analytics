# Databricks notebook source
import json
from pyspark.sql import functions as F

spark.sql("DELETE FROM sec_analytics.silver.observation WHERE length(cik) < 10")

filings      = spark.table("sec_analytics.silver.filing")
observations = spark.table("sec_analytics.silver.observation")

# COMMAND ----------
filing_stats = {
    "total_rows":        filings.count(),
    "unique_companies":  filings.select("cik").distinct().count(),
    "form_distribution": [
        r.asDict()
        for r in filings.groupBy("form_type").count().orderBy(F.col("count").desc()).collect()
    ],
    "date_range": filings.select(
        F.min("filing_date").alias("min"),
        F.max("filing_date").alias("max"),
    ).first().asDict(),
}

# COMMAND ----------
metric_coverage = [
    r.asDict()
    for r in observations.groupBy("metric").agg(
        F.count("*").alias("rows"),
        F.countDistinct("cik").alias("companies"),
        F.min("period_end").alias("oldest"),
        F.max("period_end").alias("newest"),
        F.sum(F.col("value").isNull().cast("int")).alias("null_values"),
        F.sum((F.col("value") == 0).cast("int")).alias("zero_values"),
    ).orderBy(F.col("companies").desc()).collect()
]

# COMMAND ----------
apple = [
    r.asDict()
    for r in observations.filter(
        (F.col("cik") == "0000320193")
        & (F.col("fiscal_period") == "FY")
        & (F.col("form_type") == "10-K")
        & (F.col("metric").isin("Revenue", "Net Income", "Total Assets"))
        & (F.col("fiscal_year") >= 2015)
    ).select(
        "fiscal_year", "metric", "period_end",
        F.round(F.col("value") / 1e9, 2).alias("value_billions"),
    ).orderBy("fiscal_year", "metric").collect()
]

# COMMAND ----------
result = {
    "silver_filing":      filing_stats,
    "silver_observation": {"metrics": metric_coverage},
    "apple_spot_check":   apple,
}

with open("/Volumes/sec_analytics/bronze/raw/stats_silver_quality.json", "w") as f:
    json.dump(result, f, indent=2)

dbutils.notebook.exit(json.dumps({
    "status":        "ok",
    "filings":       filing_stats["total_rows"],
    "observations":  sum(m["rows"] for m in metric_coverage),
    "companies":     filing_stats["unique_companies"],
}))
