# Databricks notebook source
import os, sys

# COMMAND ----------
_nb_path   = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_repo_root = "/Workspace" + _nb_path.rsplit("/notebooks", 1)[0]
sys.path.insert(0, os.path.join(_repo_root, "src"))
os.chdir(_repo_root)

CATALOG = "sec_analytics"
SP500_PATH = os.path.join(_repo_root, "configs", "sp500.csv")

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")

# COMMAND ----------
from sec_filing_analytics.gold.schema import (
    dim_company_ddl, dim_filing_ddl, dim_period_ddl, dim_metric_ddl,
)

spark.sql(dim_company_ddl(CATALOG))
dim_company_table = f"{CATALOG}.gold.dim_company"
if "is_current_sp500" not in spark.table(dim_company_table).columns:
    spark.sql(f"ALTER TABLE {dim_company_table} ADD COLUMNS (is_current_sp500 BOOLEAN)")
spark.sql(dim_filing_ddl(CATALOG))
spark.sql(dim_period_ddl(CATALOG))
spark.sql(dim_metric_ddl(CATALOG))

# COMMAND ----------
from sec_filing_analytics.gold.dimensions import (
    build_dim_company,  upsert_dim_company,
    build_dim_filing,   upsert_dim_filing,
    build_dim_period,
    build_dim_metric,   upsert_dim_metric,
)

result = upsert_dim_metric(spark, build_dim_metric(spark), CATALOG)
print("dim_metric:", result)

result = upsert_dim_company(spark, build_dim_company(spark, CATALOG, SP500_PATH), CATALOG)
print("dim_company:", result)

result = upsert_dim_filing(spark, build_dim_filing(spark, CATALOG), CATALOG)
print("dim_filing:", result)

# Period grain now includes period_start. Replace this dimension so obsolete
# pre-period_start keys cannot remain alongside the rebuilt grain.
period_df = build_dim_period(spark, CATALOG)
period_df.persist()
try:
    period_count = period_df.count()
    if period_count == 0:
        raise RuntimeError("Reporting-period dimension is empty; refusing to replace the existing dimension.")
    (period_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
     .saveAsTable(f"{CATALOG}.gold.dim_reporting_period"))
    result = {"total_rows": period_count}
finally:
    period_df.unpersist()
print("dim_reporting_period:", result)
