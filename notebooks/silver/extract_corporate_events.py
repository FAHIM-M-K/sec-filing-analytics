# Databricks notebook source
import os, sys

# COMMAND ----------
_nb_path   = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_repo_root = "/Workspace" + _nb_path.rsplit("/notebooks", 1)[0]
sys.path.insert(0, os.path.join(_repo_root, "src"))
os.chdir(_repo_root)

# COMMAND ----------
from sec_filing_analytics.silver.schema import corporate_event_ddl
from sec_filing_analytics.silver.events import build_silver_corporate_events, upsert_corporate_events

catalog = "sec_analytics"

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")
spark.sql(corporate_event_ddl(catalog))

# COMMAND ----------
manifest = spark.table(f"{catalog}.bronze.submission_manifest")
events_df = build_silver_corporate_events(spark, manifest)

print(f"High-impact 8-K events parsed from manifest: {events_df.count()}")
display(events_df.limit(20))

# COMMAND ----------
stats = upsert_corporate_events(spark, events_df, catalog)
print(f"silver.corporate_event stats: {stats}")
