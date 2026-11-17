# Databricks notebook source
import os, sys

# COMMAND ----------
_nb_path   = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_repo_root = "/Workspace" + _nb_path.rsplit("/notebooks", 1)[0]
sys.path.insert(0, os.path.join(_repo_root, "src"))
os.chdir(_repo_root)

CATALOG = "sec_analytics"

# COMMAND ----------
from sec_filing_analytics.silver.schema import corporate_event_ddl
spark.sql(corporate_event_ddl(CATALOG))

# COMMAND ----------
from sec_filing_analytics.gold.schema import fact_corporate_event_ddl
spark.sql(fact_corporate_event_ddl(CATALOG))

# COMMAND ----------
from sec_filing_analytics.gold.events import build_fact_corporate_event, upsert_fact_corporate_event

df = build_fact_corporate_event(spark, CATALOG)
print(f"Corporate event rows to upsert: {df.count()}")
result = upsert_fact_corporate_event(spark, df, CATALOG)
print("fact_corporate_event:", result)
