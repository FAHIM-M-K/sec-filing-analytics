# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — Ingest Company Facts

# COMMAND ----------
# MAGIC %pip install pyyaml>=6.0 requests>=2.32 --quiet

# COMMAND ----------

import sys
import os

_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_repo_root = "/Workspace" + _nb_path.rsplit("/notebooks", 1)[0]
sys.path.insert(0, f"{_repo_root}/src")
os.chdir(_repo_root)

# COMMAND ----------

import uuid

from sec_filing_analytics.config.loader import load_pipeline_config
from sec_filing_analytics.sec_client.client import SecApiClient, SecClientSettings
from sec_filing_analytics.universe.membership import load_membership_csv
from sec_filing_analytics.bronze.schema import create_company_facts_manifest_table
from sec_filing_analytics.bronze.company_facts import (
    build_manifest_row,
    fetch_company_facts,
    land_company_facts_payload,
    latest_company_facts_hashes,
    upsert_company_facts_manifest,
)

CATALOG = "sec_analytics"
SCHEMA = "bronze"
VOLUME_NAME = "raw"

COMPANY_FACTS_TABLE = f"{CATALOG}.{SCHEMA}.company_facts_manifest"
VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}"
CONFIG_PATH = "configs/pipeline.yml"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

config = load_pipeline_config(CONFIG_PATH)
client = SecApiClient(SecClientSettings.from_pipeline_config(config))
run_id = str(uuid.uuid4())

create_company_facts_manifest_table(spark, COMPANY_FACTS_TABLE)
print(f"Run ID: {run_id}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Load Universe and Known Hashes

# COMMAND ----------

all_records = load_membership_csv(config["universe"]["membership_file"])
active_ciks = [r["cik"] for r in all_records if r["membership_end_date"] is None]
known_hashes = latest_company_facts_hashes(spark, COMPANY_FACTS_TABLE)

print(f"Active CIKs : {len(active_ciks)}")
print(f"Known hashes: {len(known_hashes)}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Fetch, Hash, and Land

# COMMAND ----------

manifest_rows = []
errors = []

for cik in active_ciks:
    try:
        result = fetch_company_facts(cik, client)
        is_new_version = result.content_hash != known_hashes.get(cik)

        payload_path = None
        if is_new_version:
            payload_path = str(land_company_facts_payload(result, VOLUME_ROOT))

        manifest_rows.append(build_manifest_row(result, run_id, payload_path, is_new_version))

    except Exception as exc:  # noqa: BLE001
        errors.append({"cik": cik, "error": str(exc)})
        print(f"ERROR cik={cik}: {exc}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Upsert Manifest and Summarise

# COMMAND ----------

upsert_company_facts_manifest(spark, manifest_rows, COMPANY_FACTS_TABLE)

new_count = sum(1 for r in manifest_rows if r["is_new_version"])
new_bytes = sum(r["response_size_bytes"] for r in manifest_rows if r["is_new_version"])

print(f"Run ID     : {run_id}")
print(f"Processed  : {len(manifest_rows)}")
print(f"New        : {new_count}  ({new_bytes:,} bytes)")
print(f"Unchanged  : {len(manifest_rows) - new_count}")
print(f"Errors     : {len(errors)}")

if errors:
    print("Failed CIKs:", [e["cik"] for e in errors])

if len(errors) > max(1, int(len(active_ciks) * 0.10)):
    raise RuntimeError(
        f"{len(errors)}/{len(active_ciks)} CIKs failed. Watermark not advanced."
    )
