# Databricks notebook source
import os, sys

# COMMAND ----------
_nb_path   = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_repo_root = "/Workspace" + _nb_path.rsplit("/notebooks", 1)[0]
sys.path.insert(0, os.path.join(_repo_root, "src"))
os.chdir(_repo_root)

CATALOG = "sec_analytics"

# COMMAND ----------
from sec_filing_analytics.gold.schema import fact_narrative_ddl
spark.sql(fact_narrative_ddl(CATALOG))

# COMMAND ----------
from sec_filing_analytics.gold.narratives import build_fact_narrative, upsert_fact_narrative

df = build_fact_narrative(spark, CATALOG)
print(f"Rows to upsert: {df.count()}")
result = upsert_fact_narrative(spark, df, CATALOG)
print("fact_narrative:", result)
