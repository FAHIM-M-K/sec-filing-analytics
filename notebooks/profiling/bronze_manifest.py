# Databricks notebook source
# MAGIC %md
# MAGIC # Manifest Check
# MAGIC Did both ingestion jobs complete successfully for all 500 companies?

# COMMAND ----------

from pyspark.sql import functions as F

sub = spark.table("sec_analytics.bronze.submission_manifest")
facts = spark.table("sec_analytics.bronze.company_facts_manifest")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Submission Manifest

# COMMAND ----------

sub.select(
    F.count("*").alias("total_rows"),
    F.countDistinct("cik").alias("unique_ciks"),
    F.round(F.sum(F.col("response_size_bytes") / 1e6), 1).alias("total_mb"),
    F.min("fetched_at").alias("first_fetched"),
    F.max("fetched_at").alias("last_fetched"),
).show()

# COMMAND ----------

# Any CIKs that failed or have no payload on disk?
print("Rows with missing payload path:")
sub.filter(F.col("payload_path").isNull()).select("cik", "entity_name", "fetched_at").show(20)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Company Facts Manifest

# COMMAND ----------

facts.select(
    F.count("*").alias("total_rows"),
    F.countDistinct("cik").alias("unique_ciks"),
    F.round(F.sum(F.col("response_size_bytes") / 1e6), 1).alias("total_mb"),
    F.round(F.avg("us_gaap_tag_count"), 0).alias("avg_gaap_tags"),
    F.min("us_gaap_tag_count").alias("min_gaap_tags"),
    F.max("us_gaap_tag_count").alias("max_gaap_tags"),
).show()

# COMMAND ----------

# Companies with suspiciously few GAAP tags — may have failed or returned empty facts.
print("Bottom 10 by GAAP tag count:")
facts.select("cik", "entity_name", "us_gaap_tag_count", "response_size_bytes") \
     .orderBy("us_gaap_tag_count") \
     .show(10)

