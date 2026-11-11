# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer EDA

# COMMAND ----------

import gzip
import json
from pyspark.sql import functions as F

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Submissions Manifest

# COMMAND ----------

df_sub = spark.table("sec_analytics.bronze.submission_manifest")

print(f"Total rows: {df_sub.count()}")
df_sub.printSchema()

# COMMAND ----------

display(df_sub.limit(10))

# COMMAND ----------

display(
    df_sub.select(
        "response_size_bytes",
        "filing_count_recent",
        "older_history_file_count",
    ).summary()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Company Facts Manifest

# COMMAND ----------

df_facts = spark.table("sec_analytics.bronze.company_facts_manifest")

print(f"Total rows: {df_facts.count()}")
df_facts.printSchema()

# COMMAND ----------

display(df_facts.limit(10))

# COMMAND ----------

display(
    df_facts.select(
        "response_size_bytes",
        "us_gaap_tag_count",
    ).summary()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Inspect a Raw Submissions Payload (Volume)

# COMMAND ----------

sample_sub_row = df_sub.filter(F.col("payload_path").isNotNull()).first()
sub_path = sample_sub_row["payload_path"]
print(f"Reading: {sub_path}")

with gzip.open(sub_path, "rt", encoding="utf-8") as f:
    sub_json = json.load(f)

print(f"Company: {sub_json.get('name')} (CIK: {sub_json.get('cik')})")
print(f"Keys: {list(sub_json.keys())}")

recent = sub_json.get("filings", {}).get("recent", {})
print(f"Recent filing fields: {list(recent.keys())}")

# Convert recent filings into a DataFrame to inspect in tabular format
df_sample_filings = spark.createDataFrame(
    [
        {col: recent[col][i] for col in recent if i < len(recent[col])}
        for i in range(min(15, len(recent.get("form", []))))
    ]
)
display(df_sample_filings)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Inspect a Raw Company Facts Payload (Volume)

# COMMAND ----------

sample_fact_row = df_facts.filter(F.col("payload_path").isNotNull()).first()
facts_path = sample_fact_row["payload_path"]
print(f"Reading: {facts_path}")

with gzip.open(facts_path, "rt", encoding="utf-8") as f:
    facts_json = json.load(f)

print(f"Entity: {facts_json.get('entityName')} (CIK: {facts_json.get('cik')})")
print(f"Taxonomies: {list(facts_json.get('facts', {}).keys())}")

us_gaap = facts_json.get("facts", {}).get("us-gaap", {})
print(f"US-GAAP tag count: {len(us_gaap)}")

# Flatten a few sample facts and their observations into a DataFrame
flat_observations = []
for tag, fact_data in list(us_gaap.items())[:5]:
    label = fact_data.get("label")
    for unit, obs_list in fact_data.get("units", {}).items():
        for obs in obs_list[:5]:
            record = {
                "tag": tag,
                "label": label,
                "unit": unit,
                **obs,
            }
            flat_observations.append(record)

df_sample_obs = spark.createDataFrame(flat_observations)
display(df_sample_obs)

