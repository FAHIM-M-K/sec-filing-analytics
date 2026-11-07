# Databricks notebook source
# MAGIC %md
# MAGIC # Check SEC connection
# MAGIC
# MAGIC Sends one request to the SEC submissions API and displays the response.
# MAGIC No SEC account or API key is needed.

# COMMAND ----------

import os
from pathlib import Path

import requests
import yaml


CONFIG_PATH = Path.cwd() / "configs" / "pipeline.yml"
if not CONFIG_PATH.exists():
    raise FileNotFoundError(
        "Could not find configs/pipeline.yml. Run this notebook from the project's Databricks Repo."
    )

with CONFIG_PATH.open(encoding="utf-8") as config_file:
    sec_config = yaml.safe_load(config_file)["source"]["sec"]

USER_AGENT = os.environ.get(sec_config["user_agent_env_var"], sec_config["user_agent_default"])
if "contact@example.com" in USER_AGENT:
    raise ValueError(
        "Set SEC_USER_AGENT to identify your project and contact email before running this check."
    )

TEST_CIK = "0000320193"  # Apple Inc.
URL = f"{sec_config['base_url']}/submissions/CIK{TEST_CIK}.json"

response = requests.get(
    URL,
    headers={"User-Agent": USER_AGENT, "Accept-Encoding": "gzip, deflate"},
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
