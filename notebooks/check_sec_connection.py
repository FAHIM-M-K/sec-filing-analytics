# Databricks notebook source
# MAGIC %md
# MAGIC # Check SEC connection
# MAGIC
# MAGIC Sends one request through the project's SEC client and displays the response.
# MAGIC Run it from the GitHub project folder in Databricks. It validates that the
# MAGIC shared configuration and reusable client work together.

# COMMAND ----------

from pathlib import Path
import sys

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


config_path = find_pipeline_config()
project_root = config_path.parent.parent
source_root = project_root / "src"
if str(source_root) not in sys.path:
    sys.path.insert(0, str(source_root))

from sec_filing_analytics.config.loader import load_pipeline_config
from sec_filing_analytics.sec_client import SecApiClient, SecClientSettings


pipeline_config = load_pipeline_config(config_path)
client_settings = SecClientSettings.from_pipeline_config(pipeline_config)
client = SecApiClient(client_settings)

TEST_CIK = "0000320193"  # Apple Inc.
payload = client.get_submissions(TEST_CIK)

print(
    {
        "cik": payload["cik"],
        "entity_name": payload["name"],
        "client": "SecApiClient",
        "source": "Company Submissions API",
    }
)
