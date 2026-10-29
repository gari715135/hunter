# ./src/prometheus_tools/client.py
import time
from dataclasses import dataclass
from logging import Logger
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from src.prometheus_tools.utils import elapsed_ms

REQUEST_TIMEOUT_SECONDS = 15
BODY_PREVIEW_CHARS = 500


class PromQueryError(Exception):
    """A Prometheus API request that did not produce a usable response."""

    def __init__(
        self,
        message: str,
        *,
        endpoint: str,
        status_code: Optional[int] = None,
        error_type: Optional[str] = None,
        error: Optional[str] = None,
        attempts: int = 1,
        elapsed_ms: Optional[int] = None,
        body_preview: Optional[str] = None,
    ):
        super().__init__(f"{message}: {error}" if error else message)
        self.endpoint = endpoint
        self.status_code = status_code
        self.error_type = error_type
        self.error = error or message
        self.attempts = attempts
        self.elapsed_ms = elapsed_ms
        self.body_preview = body_preview

    def as_fields(self) -> dict:
        fields = {
            "endpoint": self.endpoint,
            "http_status": self.status_code,
            "error_type": self.error_type,
            "error": self.error,
            "attempts": self.attempts,
            "http_ms": self.elapsed_ms,
            "body_preview": self.body_preview,
        }
        return {k: v for k, v in fields.items() if v is not None}


@dataclass
class PromResponse:
    data: dict
    status_code: int
    attempts: int
    elapsed_ms: int
    response_bytes: int


def _parse_prom_error(response: requests.Response):
    """Return (errorType, error) from a Prometheus-style error body, or (None, None)."""
    try:
        body = response.json()
    except ValueError:
        return None, None
    if isinstance(body, dict):
        return body.get("errorType"), body.get("error")
    return None, None


class PrometheusClient:

    def __init__(
        self,
        bearer_token: str,
        logger: Logger,
        base_url: str = "us2.app.sysdig.com",
    ):
        self.bearer_token = bearer_token
        self.logger = logger
        self.base_url = base_url.replace("https://", "")

        self.logger.debug(
            "Initializing Prometheus client",
            extra={"event": "client_init", "base_url": self.base_url},
        )

        # Create session and build headers
        headers = {"Authorization": f"Bearer {self.bearer_token}"}
        self.session = requests.Session()
        self.session.headers = headers

        # Transport-level retries: connection errors, read timeouts, and throttling/5xx responses.
        # Status retries are reported from the response's retry history in _send_get_request.
        retries = Retry(
            total=4,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.logger.debug(
            "HTTP session configured",
            extra={
                "event": "client_retry_policy",
                "retry_total": retries.total,
                "backoff_factor": retries.backoff_factor,
                "status_forcelist": sorted(retries.status_forcelist),
                "timeout_s": REQUEST_TIMEOUT_SECONDS,
            },
        )

    @staticmethod
    def _retry_history(response: requests.Response) -> tuple:
        retries = getattr(response.raw, "retries", None)
        return tuple(getattr(retries, "history", None) or ())

    def _send_get_request(self, endpoint, params, max_retries=3) -> PromResponse:
        """GET a Prometheus API endpoint. Raises PromQueryError on any failure."""
        prom_endpoint = f"prometheus/{endpoint}".replace("//", "/")
        url = f"https://{self.base_url}/{prom_endpoint}"
        # The query itself is logged once by the caller; keep request lines small.
        log_params = {k: v for k, v in params.items() if k != "query"}

        start_time = time.monotonic()
        attempts = 0

        # Outer loop only retries incomplete bodies (ChunkedEncodingError), which happen after
        # urllib3 has returned the response and so are not covered by its Retry policy.
        for attempt in range(1, max_retries + 1):
            attempts += 1
            self.logger.debug(
                "Sending GET request",
                extra={
                    "event": "http_request",
                    "url": url,
                    "params": log_params,
                    "attempt": attempt,
                    "max_attempts": max_retries,
                },
            )
            attempt_start = time.monotonic()

            try:
                response = self.session.request(
                    method="GET",
                    url=url,
                    params=params,
                    timeout=REQUEST_TIMEOUT_SECONDS,
                )

            except requests.exceptions.ChunkedEncodingError as e:
                if attempt < max_retries:
                    backoff = 0.1 * (2 ** (attempt - 1))  # 0.1s, 0.2s, 0.4s
                    self.logger.warning(
                        "Incomplete response body, retrying",
                        extra={
                            "event": "http_retry",
                            "url": url,
                            "attempt": attempt,
                            "max_attempts": max_retries,
                            "error_type": type(e).__name__,
                            "error": str(e),
                            "attempt_ms": elapsed_ms(attempt_start),
                            "retry_in_ms": int(backoff * 1000),
                        },
                    )
                    time.sleep(backoff)
                    continue
                raise PromQueryError(
                    "Incomplete response body after retries",
                    endpoint=endpoint,
                    error_type=type(e).__name__,
                    error=str(e),
                    attempts=attempts,
                    elapsed_ms=elapsed_ms(start_time),
                ) from e

            except requests.exceptions.Timeout as e:
                raise PromQueryError(
                    "Request timed out",
                    endpoint=endpoint,
                    error_type=type(e).__name__,
                    error=str(e),
                    attempts=attempts,
                    elapsed_ms=elapsed_ms(start_time),
                ) from e

            except requests.exceptions.RequestException as e:
                raise PromQueryError(
                    "Request failed",
                    endpoint=endpoint,
                    error_type=type(e).__name__,
                    error=str(e),
                    attempts=attempts,
                    elapsed_ms=elapsed_ms(start_time),
                ) from e

            history = self._retry_history(response)
            attempts += len(history)
            if history:
                self.logger.warning(
                    "Request needed transport retries",
                    extra={
                        "event": "http_transport_retries",
                        "url": url,
                        "retries": len(history),
                        "retry_statuses": [h.status for h in history],
                        "retry_errors": [str(h.error) for h in history if h.error],
                    },
                )

            total_ms = elapsed_ms(start_time)
            response_bytes = len(response.content)

            if not response.ok:
                error_type, error = _parse_prom_error(response)
                raise PromQueryError(
                    f"HTTP {response.status_code} {response.reason}",
                    endpoint=endpoint,
                    status_code=response.status_code,
                    error_type=error_type or response.reason,
                    error=error,
                    attempts=attempts,
                    elapsed_ms=total_ms,
                    body_preview=None if error else response.text[:BODY_PREVIEW_CHARS],
                )

            try:
                data = response.json()
            except ValueError as e:
                raise PromQueryError(
                    "Response was not valid JSON",
                    endpoint=endpoint,
                    status_code=response.status_code,
                    error_type="JSONDecodeError",
                    error=str(e),
                    attempts=attempts,
                    elapsed_ms=total_ms,
                    body_preview=response.text[:BODY_PREVIEW_CHARS],
                ) from e

            self.logger.debug(
                "Request successful",
                extra={
                    "event": "http_response",
                    "url": url,
                    "http_status": response.status_code,
                    "attempts": attempts,
                    "http_ms": total_ms,
                    "response_bytes": response_bytes,
                },
            )
            return PromResponse(
                data=data,
                status_code=response.status_code,
                attempts=attempts,
                elapsed_ms=total_ms,
                response_bytes=response_bytes,
            )

        # Unreachable: every loop iteration returns, continues, or raises
        raise PromQueryError("No attempts made", endpoint=endpoint, attempts=attempts)

    def query(self, prom_query: str) -> dict:
        params = dict(query=prom_query, max_series=2000, limit=2000)

        response = self._send_get_request("/api/v1/query", params)
        data = response.data
        self.logger.info(
            "Instant query completed",
            extra={
                "event": "prom_instant_query",
                "status": data.get("status"),
                "series": len(data.get("data", {}).get("result", [])),
            },
        )
        return data

    def query_range(self, prom_query: str, start: int, end: int, step: int) -> PromResponse:
        """Range query. Raises PromQueryError if the request fails."""
        if start >= end:
            self.logger.error(
                "Invalid time range",
                extra={"event": "invalid_time_range", "start": start, "end": end},
            )
            raise ValueError("Start must be before end")

        params = dict(
            query=prom_query,
            start=start,
            end=end,
            step=step,
            limit=0,
        )

        response = self._send_get_request("/api/v1/query_range", params)
        self.logger.debug(
            "Range query completed",
            extra={
                "event": "prom_range_query",
                "status": response.data.get("status"),
                "series": len(response.data.get("data", {}).get("result", [])),
                "response_bytes": response.response_bytes,
            },
        )
        return response

    def get_labels(self):
        self.logger.debug("Fetching all labels", extra={"event": "labels_fetch"})
        try:
            data = self._send_get_request("/api/v1/labels", {}).data
        except PromQueryError as e:
            self.logger.warning(
                "Failed to fetch labels", extra={"event": "labels_fetch_failed", **e.as_fields()}
            )
            return None

        if data.get("status") == "success":
            labels = data.get("data", [])
            self.logger.debug(
                "Labels fetched", extra={"event": "labels_fetched", "count": len(labels)}
            )
            return labels

        self.logger.warning(
            "Labels request returned non-success status",
            extra={
                "event": "labels_fetch_failed",
                "status": data.get("status"),
                "error": data.get("error"),
            },
        )
        return None

    def get_label_values(self, label: str):
        self.logger.debug(
            "Fetching label values", extra={"event": "label_values_fetch", "label": label}
        )
        endpoint = f"/api/v1/label/{label}/values"
        try:
            data = self._send_get_request(endpoint, {}).data
        except PromQueryError as e:
            self.logger.warning(
                "Failed to fetch label values",
                extra={"event": "label_values_fetch_failed", "label": label, **e.as_fields()},
            )
            return None

        if data.get("status") == "success":
            values = data.get("data", [])
            self.logger.debug(
                "Label values fetched",
                extra={"event": "label_values_fetched", "label": label, "count": len(values)},
            )
            return values

        self.logger.warning(
            "Label values request returned non-success status",
            extra={
                "event": "label_values_fetch_failed",
                "label": label,
                "status": data.get("status"),
                "error": data.get("error"),
            },
        )
        return None
