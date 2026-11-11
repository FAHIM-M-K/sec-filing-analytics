#  Databricks notebook source

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS sec_analytics;
# MAGIC CREATE SCHEMA IF NOT EXISTS sec_analytics.bronze;
# MAGIC CREATE VOLUME IF NOT EXISTS sec_analytics.bronze.raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;

# COMMAND ----------
# MAGIC %sql
# MAGIC SHOW SCHEMAS IN sec_analytics;

# COMMAND ----------
# MAGIC %sql
# MAGIC SHOW VOLUMES IN sec_analytics.bronze;

# COMMAND ----------
# MAGIC % sql
# MAGIC -- Check Submissions manifest
# MAGIC SELECT
# MAGIC     COUNT(*)                        AS total_rows,
# MAGIC     COUNT(DISTINCT cik)             AS unique_ciks,
# MAGIC     SUM(CASE WHEN is_new_version THEN 1 ELSE 0 END) AS new_versions,
# MAGIC     MIN(fetched_at)                 AS earliest_fetch,
# MAGIC     MAX(fetched_at)                 AS latest_fetch
# MAGIC FROM sec_analytics.bronze.submission_manifest;
# MAGIC 
# MAGIC -- Check Company Facts manifest
# MAGIC SELECT
# MAGIC     COUNT(*)                        AS total_rows,
# MAGIC     COUNT(DISTINCT cik)             AS unique_ciks,
# MAGIC     SUM(response_size_bytes) / 1e9  AS total_gb_fetched,
# MAGIC     AVG(us_gaap_tag_count)          AS avg_tags_per_company
# MAGIC FROM sec_analytics.bronze.company_facts_manifest;

# COMMAND ----------
# MAGIC % sql
# MAGIC SELECT is_new_version, COUNT(*) FROM sec_analytics.bronze.submission_manifest GROUP BY 1;
