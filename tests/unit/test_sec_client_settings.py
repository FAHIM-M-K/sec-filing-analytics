import pytest

from sec_filing_analytics.sec_client.client import SecApiClient, SecClientSettings


def _settings(**overrides):
    values = {
        "base_url": "https://data.sec.gov",
        "user_agent": "sec-filing-analytics contact@example.com",
        "max_requests_per_second": 8,
        "connect_timeout_seconds": 10,
        "read_timeout_seconds": 30,
        "max_attempts": 5,
        "initial_backoff_seconds": 1,
        "max_backoff_seconds": 30,
    }
    values.update(overrides)
    return SecClientSettings(**values)


@pytest.mark.parametrize(
    "override",
    [
        {"base_url": "data.sec.gov"},
        {"max_requests_per_second": 0},
    ],
)
def test_settings_reject_invalid_values(override):
    with pytest.raises(ValueError):
        _settings(**override)


def test_retry_jitter_never_exceeds_configured_maximum():
    client = SecApiClient(_settings(max_backoff_seconds=2))
    assert client._backoff_seconds(10, None) == 2


def test_submission_history_filename_rejects_path_traversal():
    client = SecApiClient(_settings())
    with pytest.raises(ValueError, match="Invalid SEC submissions history filename"):
        client.get_submission_history_file("../outside.json")
