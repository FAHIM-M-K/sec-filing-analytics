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
from sec_filing_analytics.gold.facts import build_fact_financial_metric

df = build_fact_financial_metric(spark, CATALOG)
# Full replacement is required after changing period grain: old fact keys must
# not coexist with the rebuilt period dimension.
(df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .partitionBy("metric_name").saveAsTable(f"{CATALOG}.gold.fact_financial_metric"))
result = {"total_rows": spark.table(f"{CATALOG}.gold.fact_financial_metric").count()}
print("fact_financial_metric:", result)
