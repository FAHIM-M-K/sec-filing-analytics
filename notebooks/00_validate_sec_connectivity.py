# Databricks notebook source
# MAGIC %md
# MAGIC # SEC connectivity validation
# MAGIC
# MAGIC This is an Increment 0 environment test. It verifies that the configured
# MAGIC Databricks workspace can reach the SEC API with a declared User-Agent.
# MAGIC It intentionally does not ingest or persist production data.

# COMMAND ----------

import os

import requests


USER_AGENT_ENV_VAR = "SEC_USER_AGENT"
TEST_CIK = "0000320193"  # Apple Inc.; a stable public SEC API test entity.
URL = f"https://data.sec.gov/submissions/CIK{TEST_CIK}.json"

user_agent = os.environ.get(USER_AGENT_ENV_VAR)
if not user_agent:
    raise ValueError(
        f"Set {USER_AGENT_ENV_VAR} in the Databricks environment or retrieve it from a "
        "Databricks secret before running this validation."
    )

response = requests.get(
    URL,
    headers={"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"},
    timeout=(10, 30),
)
response.raise_for_status()
payload = response.json()

print(
    {
        "validation": "sec_api_connectivity",
        "status_code": response.status_code,
        "cik": payload["cik"],
        "entity_name": payload["name"],
        "response_content_type": response.headers.get("Content-Type"),
    }
)
