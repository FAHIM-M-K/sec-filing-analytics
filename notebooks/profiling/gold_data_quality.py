# Databricks notebook source
import json
from pyspark.sql import functions as F

CATALOG = "sec_analytics"

dim_company = spark.table(f"{CATALOG}.gold.dim_company")
dim_filing  = spark.table(f"{CATALOG}.gold.dim_filing")
dim_period  = spark.table(f"{CATALOG}.gold.dim_reporting_period")
dim_metric  = spark.table(f"{CATALOG}.gold.dim_metric")
facts       = spark.table(f"{CATALOG}.gold.fact_financial_metric")

# COMMAND ----------
dim_stats = {
    "dim_company": {
        "total_rows": dim_company.count(),
        "with_ticker": dim_company.filter(F.col("ticker").isNotNull()).count(),
        "distinct_ciks": dim_company.select("cik").distinct().count(),
    },
    "dim_filing": {
        "total_rows": dim_filing.count(),
        "amendments": dim_filing.filter(F.col("is_amendment") == True).count(),
        "distinct_accessions": dim_filing.select("accession_number").distinct().count(),
    },
    "dim_reporting_period": {
        "total_rows": dim_period.count(),
        "distinct_periods": dim_period.select("period_end", "fiscal_period", "period_type").distinct().count(),
    },
    "dim_metric": {
        "total_rows": dim_metric.count(),
        "metrics": [r["metric_name"] for r in dim_metric.select("metric_name").collect()],
    },
}

# COMMAND ----------
fact_stats = {
    "total_rows": facts.count(),
    "orphan_companies": facts.filter(F.col("company_key") == 0).count(),
    "orphan_filings": facts.filter(F.col("filing_key") == 0).count(),
    "orphan_periods": facts.filter(F.col("period_key") == 0).count(),
    "orphan_metrics": facts.filter(F.col("metric_key") == 0).count(),
    "null_values": facts.filter(F.col("value").isNull()).count(),
    "metric_breakdown": [
        r.asDict()
        for r in facts.groupBy("metric_name").agg(
            F.count("*").alias("rows"),
            F.countDistinct("cik").alias("companies"),
            F.round(F.min("value"), 2).alias("min_val"),
            F.round(F.max("value"), 2).alias("max_val"),
        ).orderBy(F.col("rows").desc()).collect()
    ],
}

# COMMAND ----------
apple = [
    r.asDict()
    for r in facts.filter(
        (F.col("cik") == "0000320193")
        & (F.col("metric_name").isin("Revenue", "Net Income", "Total Assets"))
    )
    .join(dim_period, on="period_key")
    .filter(
        (F.col("fiscal_period") == "FY")
        & (F.col("fiscal_year") >= 2020)
    )
    .select(
        "fiscal_year", "metric_name", "period_end",
        F.round(F.col("value") / 1e9, 2).alias("billions"),
    )
    .orderBy("fiscal_year", "metric_name")
    .collect()
]

# COMMAND ----------
summary = {
    "dimensions": dim_stats,
    "facts": fact_stats,
    "apple_spot_check": apple,
}

dbutils.notebook.exit(json.dumps(summary, default=str))
