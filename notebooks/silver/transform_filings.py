# Databricks notebook source

import os, sys

# COMMAND ----------

_nb_path   = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_repo_root = "/Workspace" + _nb_path.rsplit("/notebooks", 1)[0]
sys.path.insert(0, os.path.join(_repo_root, "src"))
os.chdir(_repo_root)

# COMMAND ----------

from sec_filing_analytics.silver.schema import filing_ddl
from sec_filing_analytics.silver.filings import build_silver_filing, upsert_silver_filing

catalog = "sec_analytics"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")
spark.sql(filing_ddl(catalog))

# COMMAND ----------

manifest = spark.table(f"{catalog}.bronze.submission_manifest")
df       = build_silver_filing(spark, manifest)

print(f"Rows parsed: {df.count()}")
display(df.limit(20))

# COMMAND ----------

stats = upsert_silver_filing(spark, df, catalog)
print(stats)


