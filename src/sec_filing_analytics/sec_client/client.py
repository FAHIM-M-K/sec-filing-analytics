from __future__ import annotations

from dataclasses import dataclass
from random import uniform
from threading import Lock
from time import monotonic, sleep
from typing import Any, Callable, Mapping

import requests


class SecApiError(RuntimeError):
    pass


@dataclass(frozen=True)
class SecClientSettings:
    base_url: str
    user_agent: str
    max_requests_per_second: float
    connect_timeout_seconds: float
    read_timeout_seconds: float
    max_attempts: int
    initial_backoff_seconds: float
    max_backoff_seconds: float

    @classmethod
    def from_pipeline_config(cls, config: Mapping[str, Any]) -> "SecClientSettings":
        sec = config["source"]["sec"]
        retry = sec["retry"]
        return cls(
            base_url=sec["base_url"].rstrip("/"),
            user_agent=sec["user_agent"],
            max_requests_per_second=float(sec["max_requests_per_second"]),
            connect_timeout_seconds=float(sec["connect_timeout_seconds"]),
            read_timeout_seconds=float(sec["read_timeout_seconds"]),
            max_attempts=int(retry["max_attempts"]),
            initial_backoff_seconds=float(retry["initial_backoff_seconds"]),
            max_backoff_seconds=float(retry["max_backoff_seconds"]),
        )


class RequestRateLimiter:
    def __init__(self, requests_per_second: float) -> None:
        if requests_per_second <= 0:
            raise ValueError("requests_per_second must be greater than zero.")
        self._minimum_interval = 1 / requests_per_second
        self._next_request_at = 0.0
        self._lock = Lock()

    def wait_for_turn(self) -> None:
        with self._lock:
            now = monotonic()
            scheduled_time = max(now, self._next_request_at)
            self._next_request_at = scheduled_time + self._minimum_interval

        wait_seconds = scheduled_time - now
        if wait_seconds > 0:
            sleep(wait_seconds)


class SecApiClient:
    _RETRYABLE_STATUS_CODES = {408, 429}

    def __init__(
        self,
        settings: SecClientSettings,
        session: requests.Session | None = None,
        rate_limiter: RequestRateLimiter | None = None,
        sleeper: Callable[[float], None] = sleep,
    ) -> None:
        self._settings = settings
        self._session = session or requests.Session()
        self._rate_limiter = rate_limiter or RequestRateLimiter(settings.max_requests_per_second)
        self._sleep = sleeper

    def get_submissions(self, cik: str | int) -> dict[str, Any]:
        normalized_cik = self._normalize_cik(cik)
        return self.get_json(f"/submissions/CIK{normalized_cik}.json")

    def get_company_facts(self, cik: str | int) -> dict[str, Any]:
        normalized_cik = self._normalize_cik(cik)
        return self.get_json(f"/api/xbrl/companyfacts/CIK{normalized_cik}.json")

    def get_json(self, path: str) -> dict[str, Any]:
        url = f"{self._settings.base_url}/{path.lstrip('/')}"
        last_error: Exception | None = None

        for attempt in range(1, self._settings.max_attempts + 1):
            response: requests.Response | None = None
            self._rate_limiter.wait_for_turn()
            try:
                response = self._session.get(
                    url,
                    headers={"User-Agent": self._settings.user_agent},
                    timeout=(
                        self._settings.connect_timeout_seconds,
                        self._settings.read_timeout_seconds,
                    ),
                )
                if self._should_retry_response(response):
                    last_error = SecApiError(
                        f"SEC returned HTTP {response.status_code} for {url} on attempt {attempt}."
                    )
                else:
                    if response.status_code >= 400:
                        raise SecApiError(
                            f"SEC returned non-retryable HTTP {response.status_code} for {url}."
                        )
                    try:
                        payload = response.json()
                    except ValueError as error:
                        raise SecApiError(f"SEC returned invalid JSON for {url}.") from error
                    if not isinstance(payload, dict):
                        raise SecApiError(f"SEC returned a non-object JSON payload for {url}.")
                    return payload
            except requests.RequestException as error:
                last_error = SecApiError(f"SEC request failed for {url} on attempt {attempt}: {error}")

            if attempt < self._settings.max_attempts:
                self._sleep(self._backoff_seconds(attempt, response))

        raise SecApiError(
            f"SEC request failed after {self._settings.max_attempts} attempts for {url}."
        ) from last_error

    def _should_retry_response(self, response: requests.Response) -> bool:
        return response.status_code in self._RETRYABLE_STATUS_CODES or response.status_code >= 500

    def _backoff_seconds(self, attempt: int, response: requests.Response | None) -> float:
        retry_after = response.headers.get("Retry-After") if response is not None else None
        if retry_after and retry_after.isdigit():
            return min(float(retry_after), self._settings.max_backoff_seconds)

        exponential_delay = self._settings.initial_backoff_seconds * (2 ** (attempt - 1))
        bounded_delay = min(exponential_delay, self._settings.max_backoff_seconds)
        return bounded_delay + uniform(0, bounded_delay * 0.1)

    @staticmethod
    def _normalize_cik(cik: str | int) -> str:
        try:
            numeric_cik = int(str(cik).strip())
        except ValueError as error:
            raise ValueError(f"CIK must contain digits only: {cik!r}") from error

        if numeric_cik < 0 or numeric_cik > 9_999_999_999:
            raise ValueError(f"CIK is outside the valid 10-digit range: {cik!r}")
        return f"{numeric_cik:010d}"
