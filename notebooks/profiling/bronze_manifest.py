# Databricks notebook source

from pyspark.sql import functions as F

sub   = spark.table("sec_analytics.bronze.submission_manifest")
facts = spark.table("sec_analytics.bronze.company_facts_manifest")

# COMMAND ----------

sub.select(
    F.count("*").alias("rows"),
    F.countDistinct("cik").alias("unique_ciks"),
    F.round(F.sum(F.col("response_size_bytes") / 1e6), 1).alias("total_mb"),
    F.min("fetched_at").alias("first_fetched"),
    F.max("fetched_at").alias("last_fetched"),
).show(truncate=False)

# COMMAND ----------

facts.select(
    F.count("*").alias("rows"),
    F.countDistinct("cik").alias("unique_ciks"),
    F.round(F.sum(F.col("response_size_bytes") / 1e6), 1).alias("total_mb"),
    F.round(F.avg("us_gaap_tag_count"), 0).alias("avg_gaap_tags"),
    F.min("us_gaap_tag_count").alias("min_gaap_tags"),
    F.max("us_gaap_tag_count").alias("max_gaap_tags"),
).show(truncate=False)

# COMMAND ----------

# bottom 10 by tag count — companies with very few tags likely returned minimal data
facts.select("cik", "entity_name", "us_gaap_tag_count", "response_size_bytes") \
     .orderBy("us_gaap_tag_count") \
     .show(10, truncate=False)

# COMMAND ----------

# any missing payload files
sub.filter(F.col("payload_path").isNull()).select("cik", "entity_name", "fetched_at").show(truncate=False)
facts.filter(F.col("payload_path").isNull()).select("cik", "entity_name", "fetched_at").show(truncate=False)
