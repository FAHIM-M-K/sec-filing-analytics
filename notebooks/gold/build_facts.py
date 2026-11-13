# Databricks notebook source
import os, sys

# COMMAND ----------
_nb_path   = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_repo_root = "/Workspace" + _nb_path.rsplit("/notebooks", 1)[0]
sys.path.insert(0, os.path.join(_repo_root, "src"))
os.chdir(_repo_root)

CATALOG = "sec_analytics"

# COMMAND ----------
from sec_filing_analytics.gold.schema import fact_ddl
spark.sql(fact_ddl(CATALOG))

# COMMAND ----------
from sec_filing_analytics.gold.facts import build_fact_financial_metric, upsert_fact_financial_metric

df = build_fact_financial_metric(spark, CATALOG)
result = upsert_fact_financial_metric(spark, df, CATALOG)
print("fact_financial_metric:", result)
