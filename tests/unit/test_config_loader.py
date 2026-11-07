from pathlib import Path

import pytest

from sec_filing_analytics.config.loader import ConfigurationError, load_pipeline_config


def test_load_pipeline_config_accepts_project_configuration() -> None:
    project_root = Path(__file__).resolve().parents[2]

    config = load_pipeline_config(project_root / "configs" / "pipeline.yml")

    assert config["source"]["sec"]["max_requests_per_second"] == 8
    assert config["scope"]["forms"] == ["10-K", "10-Q", "10-K/A", "10-Q/A"]


def test_load_pipeline_config_rejects_missing_required_value(tmp_path: Path) -> None:
    config_path = tmp_path / "invalid.yml"
    config_path.write_text("source: {}\n", encoding="utf-8")

    with pytest.raises(ConfigurationError, match="source.sec.base_url"):
        load_pipeline_config(config_path)
