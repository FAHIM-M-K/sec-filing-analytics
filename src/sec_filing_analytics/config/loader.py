"""Load non-secret pipeline configuration from YAML."""

from __future__ import annotations

from pathlib import Path
from typing import Any
import os

import yaml


class ConfigurationError(ValueError):
    """Raised when a pipeline configuration is missing required values."""


REQUIRED_PATHS = (
    ("source", "sec", "base_url"),
    ("source", "sec", "user_agent_env_var"),
    ("source", "sec", "max_requests_per_second"),
    ("scope", "forms"),
    ("storage", "bronze_volume"),
)


def load_pipeline_config(path: str | Path) -> dict[str, Any]:
    """Load and minimally validate the versioned pipeline configuration."""
    config_path = Path(path)
    with config_path.open(encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file)

    if not isinstance(config, dict):
        raise ConfigurationError("Pipeline configuration must contain a YAML mapping.")

    for required_path in REQUIRED_PATHS:
        current: Any = config
        for key in required_path:
            if not isinstance(current, dict) or key not in current:
                dotted_path = ".".join(required_path)
                raise ConfigurationError(f"Missing required configuration value: {dotted_path}")
            current = current[key]
        if current in (None, "", []):
            dotted_path = ".".join(required_path)
            raise ConfigurationError(f"Configuration value cannot be empty: {dotted_path}")

    return config


def resolve_sec_settings(config: dict[str, Any], environ: dict[str, str] | None = None) -> dict[str, Any]:
    """Resolve shared SEC settings and apply the optional runtime User-Agent override."""
    environment = os.environ if environ is None else environ
    settings = dict(config["source"]["sec"])
    variable_name = settings["user_agent_env_var"]
    settings["user_agent"] = environment.get(variable_name, settings["user_agent_default"])
    if "contact@example.com" in settings["user_agent"]:
        raise ConfigurationError(
            f"Replace the example contact in {variable_name} or set that environment variable."
        )
    return settings
