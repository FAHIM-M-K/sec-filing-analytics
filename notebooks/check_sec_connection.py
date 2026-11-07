# Databricks notebook source
# MAGIC %md
# MAGIC # Check SEC connection
# MAGIC
# MAGIC Sends one request to the SEC submissions API and displays the response.
# MAGIC Run it from the GitHub project folder in Databricks. It uses the shared
# MAGIC configuration in `configs/pipeline.yml`.

# COMMAND ----------

from pathlib import Path
import sys

import requests
import yaml


def find_pipeline_config() -> Path:
    """Find the project configuration when this notebook runs from a Git folder."""
    search_roots = [Path.cwd(), *(Path(path) for path in sys.path if path)]
    for root in search_roots:
        config_path = root / "configs" / "pipeline.yml"
        if config_path.is_file():
            return config_path
    raise FileNotFoundError(
        "Could not find configs/pipeline.yml. Open this notebook from the connected "
        "GitHub project folder in Databricks, then run it again."
    )


with find_pipeline_config().open(encoding="utf-8") as config_file:
    sec_config = yaml.safe_load(config_file)["source"]["sec"]

TEST_CIK = "0000320193"  # Apple Inc.
URL = f"{sec_config['base_url']}/submissions/CIK{TEST_CIK}.json"

response = requests.get(
    URL,
    headers={"User-Agent": sec_config["user_agent"], "Accept-Encoding": "gzip, deflate"},
    timeout=(sec_config["connect_timeout_seconds"], sec_config["read_timeout_seconds"]),
)
response.raise_for_status()
payload = response.json()

print(
    {
        "status_code": response.status_code,
        "cik": payload["cik"],
        "entity_name": payload["name"],
        "response_content_type": response.headers.get("Content-Type"),
    }
)
