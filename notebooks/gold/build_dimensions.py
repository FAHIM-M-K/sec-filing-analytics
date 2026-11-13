# Databricks notebook source
import sys
sys.path.insert(0, "/Workspace/Repos/sec-filing-analytics/src")

CATALOG = "sec_analytics"
SP500_PATH = "/Workspace/Repos/sec-filing-analytics/configs/sp500.csv"

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")

# COMMAND ----------
from sec_filing_analytics.gold.schema import (
    dim_company_ddl, dim_filing_ddl, dim_period_ddl, dim_metric_ddl,
)

spark.sql(dim_company_ddl(CATALOG))
spark.sql(dim_filing_ddl(CATALOG))
spark.sql(dim_period_ddl(CATALOG))
spark.sql(dim_metric_ddl(CATALOG))

# COMMAND ----------
from sec_filing_analytics.gold.dimensions import (
    build_dim_company,  upsert_dim_company,
    build_dim_filing,   upsert_dim_filing,
    build_dim_period,   upsert_dim_period,
    build_dim_metric,   upsert_dim_metric,
)

result = upsert_dim_metric(spark, build_dim_metric(spark), CATALOG)
print("dim_metric:", result)

result = upsert_dim_company(spark, build_dim_company(spark, CATALOG, SP500_PATH), CATALOG)
print("dim_company:", result)

result = upsert_dim_filing(spark, build_dim_filing(spark, CATALOG), CATALOG)
print("dim_filing:", result)

result = upsert_dim_period(spark, build_dim_period(spark, CATALOG), CATALOG)
print("dim_reporting_period:", result)
