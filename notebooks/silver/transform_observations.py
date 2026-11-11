# Databricks notebook source

import os, sys

# COMMAND ----------

_nb_path   = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_repo_root = "/Workspace" + _nb_path.rsplit("/notebooks", 1)[0]
sys.path.insert(0, os.path.join(_repo_root, "src"))
os.chdir(_repo_root)

# COMMAND ----------

from sec_filing_analytics.silver.schema import observation_ddl
from sec_filing_analytics.silver.observations import build_silver_observations, upsert_silver_observations

catalog = "sec_analytics"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")
spark.sql(observation_ddl(catalog))

# COMMAND ----------

manifest = spark.table(f"{catalog}.bronze.company_facts_manifest")
df       = build_silver_observations(spark, manifest)

print(f"Rows parsed: {df.count()}")
display(df.limit(20))

# COMMAND ----------

stats = upsert_silver_observations(spark, df, catalog)
print(stats)


