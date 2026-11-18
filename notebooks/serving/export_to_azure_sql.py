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
    "dim_company,dim_reporting_period,fact_financial_metric",
)
requested_tables = [
    table.strip()
    for table in dbutils.widgets.get("tables_to_export").split(",")
    if table.strip()
]
if not requested_tables:
    raise ValueError("Select export tables in the tables_to_export widget; full export is disabled by default.")

# Reload the workspace source so a notebook rerun cannot keep an older exporter
# cached after the Git folder is synced.
import importlib
import inspect
from sec_filing_analytics.serving import azure_sql

azure_sql = importlib.reload(azure_sql)
if "table_names" not in inspect.signature(azure_sql.export_gold_to_azure_sql).parameters:
    raise RuntimeError(
        f"Stale Azure SQL exporter loaded from {azure_sql.__file__}; sync the project Git folder and rerun."
    )

results = azure_sql.export_gold_to_azure_sql(
    spark,
    dbutils,
    catalog=CATALOG,
    scope="azure_sql",
    table_names=requested_tables,
)
for table_name, count in results.items():
    print(f"Exported {count:,} rows to Azure SQL table {table_name}")

