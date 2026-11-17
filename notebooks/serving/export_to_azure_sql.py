# Databricks notebook source
import os, sys

# COMMAND ----------
_nb_path   = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_repo_root = "/Workspace" + _nb_path.rsplit("/notebooks", 1)[0]
sys.path.insert(0, os.path.join(_repo_root, "src"))
os.chdir(_repo_root)

CATALOG = "sec_analytics"

# COMMAND ----------
dbutils.widgets.text(
    "tables_to_export",
    "dim_reporting_period,fact_financial_metric",
)
requested_tables = [
    table.strip()
    for table in dbutils.widgets.get("tables_to_export").split(",")
    if table.strip()
]
if not requested_tables:
    raise ValueError("Select export tables in the tables_to_export widget; full export is disabled by default.")

# This notebook defaults to the two tables changed by the period-grain repair.
from sec_filing_analytics.serving.azure_sql import export_gold_to_azure_sql

results = export_gold_to_azure_sql(
    spark,
    dbutils,
    catalog=CATALOG,
    scope="azure_sql",
    table_names=requested_tables,
)
for table_name, count in results.items():
    print(f"Exported {count:,} rows to Azure SQL table {table_name}")

