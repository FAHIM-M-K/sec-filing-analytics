# Databricks notebook source
import sys
sys.path.insert(0, "/Workspace/Repos/sec-filing-analytics/src")

CATALOG = "sec_analytics"

# COMMAND ----------
from sec_filing_analytics.gold.schema import fact_ddl
spark.sql(fact_ddl(CATALOG))

# COMMAND ----------
from sec_filing_analytics.gold.facts import build_fact_financial_metric, upsert_fact_financial_metric

df = build_fact_financial_metric(spark, CATALOG)
result = upsert_fact_financial_metric(spark, df, CATALOG)
print("fact_financial_metric:", result)
