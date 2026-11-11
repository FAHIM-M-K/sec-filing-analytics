# Databricks notebook source

import os, sys

# COMMAND ----------

%pip install -r requirements.txt

# COMMAND ----------

_nb_path   = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_repo_root = "/Workspace" + "/".join(_nb_path.split("/")[:-3])
os.chdir(_repo_root)
sys.path.insert(0, os.path.join(_repo_root, "src"))

# COMMAND ----------

from sec_filing_analytics.config.loader import load_config
from sec_filing_analytics.silver.schema import observation_ddl
from sec_filing_analytics.silver.observations import build_silver_observations, upsert_silver_observations

cfg     = load_config("configs/pipeline.yml")
catalog = cfg["unity_catalog"]["catalog"]

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

