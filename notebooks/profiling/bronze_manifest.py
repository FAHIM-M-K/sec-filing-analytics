# Databricks notebook source

from pyspark.sql import functions as F

sub   = spark.table("sec_analytics.bronze.submission_manifest")
facts = spark.table("sec_analytics.bronze.company_facts_manifest")

# COMMAND ----------

print("=== Submissions ===")
sub.select(
    F.count("*").alias("rows"),
    F.countDistinct("cik").alias("unique_ciks"),
    F.round(F.sum(F.col("response_size_bytes") / 1e6), 1).alias("total_mb"),
    F.min("fetched_at").alias("first_fetched"),
    F.max("fetched_at").alias("last_fetched"),
).show(truncate=False)

print("=== Company Facts ===")
facts.select(
    F.count("*").alias("rows"),
    F.countDistinct("cik").alias("unique_ciks"),
    F.round(F.sum(F.col("response_size_bytes") / 1e6), 1).alias("total_mb"),
    F.round(F.avg("us_gaap_tag_count"), 0).alias("avg_gaap_tags"),
    F.min("us_gaap_tag_count").alias("min_gaap_tags"),
    F.max("us_gaap_tag_count").alias("max_gaap_tags"),
).show(truncate=False)

# COMMAND ----------

# companies with very few GAAP tags — worth investigating manually
facts.select("cik", "entity_name", "us_gaap_tag_count", "response_size_bytes") \
     .orderBy("us_gaap_tag_count") \
     .show(10, truncate=False)

# COMMAND ----------

missing_sub   = sub.filter(F.col("payload_path").isNull()).count()
missing_facts = facts.filter(F.col("payload_path").isNull()).count()
print(f"Missing submission payloads : {missing_sub}")
print(f"Missing company facts payloads: {missing_facts}")
