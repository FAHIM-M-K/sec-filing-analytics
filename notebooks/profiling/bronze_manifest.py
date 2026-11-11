# Databricks notebook source

import json
from pyspark.sql import functions as F

sub   = spark.table("sec_analytics.bronze.submission_manifest")
facts = spark.table("sec_analytics.bronze.company_facts_manifest")

# COMMAND ----------

sub_stats = sub.select(
    F.count("*").alias("rows"),
    F.countDistinct("cik").alias("unique_ciks"),
    F.round(F.sum(F.col("response_size_bytes") / 1e6), 2).alias("total_mb"),
    F.sum(F.col("payload_path").isNull().cast("int")).alias("missing_payloads"),
).first().asDict()

facts_stats = facts.select(
    F.count("*").alias("rows"),
    F.countDistinct("cik").alias("unique_ciks"),
    F.round(F.sum(F.col("response_size_bytes") / 1e6), 2).alias("total_mb"),
    F.round(F.avg("us_gaap_tag_count"), 1).alias("avg_gaap_tags"),
    F.min("us_gaap_tag_count").alias("min_gaap_tags"),
    F.max("us_gaap_tag_count").alias("max_gaap_tags"),
    F.sum(F.col("payload_path").isNull().cast("int")).alias("missing_payloads"),
).first().asDict()

low_tag = facts.select("cik", "entity_name", "us_gaap_tag_count") \
    .orderBy("us_gaap_tag_count").limit(10).collect()

result = {
    "submissions": {k: str(v) for k, v in sub_stats.items()},
    "facts":       {k: str(v) for k, v in facts_stats.items()},
    "low_gaap_tag_companies": [r.asDict() for r in low_tag],
}

with open("/Volumes/sec_analytics/bronze/raw/stats_manifest.json", "w") as f:
    json.dump(result, f, indent=2)

dbutils.notebook.exit(json.dumps({"status": "ok", "rows_sub": sub_stats["rows"], "rows_facts": facts_stats["rows"]}))
