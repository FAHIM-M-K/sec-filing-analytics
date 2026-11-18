# Databricks notebook source
import os, sys

# COMMAND ----------
_nb_path   = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_repo_root = "/Workspace" + _nb_path.rsplit("/notebooks", 1)[0]
sys.path.insert(0, os.path.join(_repo_root, "src"))
os.chdir(_repo_root)

CATALOG = "sec_analytics"

# COMMAND ----------
from pyspark.sql import functions as F
from sec_filing_analytics.gold.schema import fact_ddl
spark.sql(fact_ddl(CATALOG))

# COMMAND ----------
from sec_filing_analytics.gold.facts import build_fact_financial_metric, upsert_fact_financial_metric

df = build_fact_financial_metric(spark, CATALOG)
df.persist()
try:
    fact_count = df.count()
    if fact_count == 0:
        raise RuntimeError("Financial fact rebuild is empty; refusing to replace the existing fact table.")
    invalid = df.filter(
        (F.col("company_key") == 0)
        | (F.col("filing_key") == 0)
        | (F.col("metric_key") == 0)
        | F.col("period_key").isNull()
        | (F.col("period_key") == 0)
        | F.col("value").isNull()
    )
    if invalid.limit(1).count():
        raise RuntimeError("Financial facts contain null values or unmatched dimension keys; refusing to replace the existing fact table.")
    if df.groupBy("fact_key").count().filter(F.col("count") > 1).limit(1).count():
        raise RuntimeError("Financial fact keys are not unique; refusing to replace the existing fact table.")
    period_ends = spark.table(f"{CATALOG}.gold.dim_reporting_period").select("period_key", "period_end")
    if (
        df.join(period_ends, on="period_key")
        .filter(F.col("period_end") < "2010-01-01")
        .limit(1)
        .count()
    ):
        raise RuntimeError("Financial facts include periods before 2010; refusing to replace the existing fact table.")
    result = upsert_fact_financial_metric(spark, df, CATALOG)
finally:
    df.unpersist()
print("fact_financial_metric:", result)
