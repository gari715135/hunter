# ./src/prometheus_tools/client.py
import time
from logging import Logger

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


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

        self.logger.info(f"Initializing Prometheus client base_url={self.base_url}")

        # Create session and build headers
        headers = {"Authorization": f"Bearer {self.bearer_token}"}
        self.session = requests.Session()
        self.session.headers = headers

        # Configure retries
        retries = Retry(
            total=7,
            backoff_factor=0.1,
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.logger.debug("HTTP session configured with retry policy total_retries=7 backoff_factor=0.1")

    def _send_get_request(self, endpoint, params, max_retries=3):
        prom_endpoint = f"prometheus/{endpoint}".replace("//", "/")
        url = f"https://{self.base_url}/{prom_endpoint}"

        for attempt in range(max_retries):
            self.logger.debug(f"Sending GET request url={url} params={params} " f"attempt={attempt + 1}/{max_retries}")
            start_time = time.time()

            try:
                response = self.session.request(
                    method="GET",
                    url=url,
                    params=params,
                    timeout=15,
                )

                elapsed = time.time() - start_time

                if response.ok:
                    self.logger.debug(
                        f"Request successful status_code={response.status_code} " f"response_time_ms={elapsed*1000:.2f}"
                    )
                    try:
                        return response.json()
                    except requests.exceptions.JSONDecodeError as e:
                        self.logger.error(
                            f"Failed to decode JSON response status_code={response.status_code} "
                            f'error="{e}" content_preview={response.content[:200]}'
                        )
                        return None
                else:
                    self.logger.error(
                        f"Request failed status_code={response.status_code} "
                        f'reason="{response.reason}" response_time_ms={elapsed*1000:.2f}'
                    )
                    return None

            except requests.exceptions.Timeout:
                elapsed = time.time() - start_time
                self.logger.error(f"Request timeout url={url} timeout_seconds=15 " f"elapsed_seconds={elapsed:.2f}")
                return None

            except requests.exceptions.ChunkedEncodingError as e:
                elapsed = time.time() - start_time

                # Retry on chunked encoding errors (incomplete reads)
                if attempt < max_retries - 1:
                    backoff = 0.1 * (2**attempt)  # 0.1s, 0.2s, 0.4s
                    self.logger.warning(
                        f"ChunkedEncodingError on attempt {attempt + 1}/{max_retries} "
                        f"url={url} query={params.get('query')} error='{e}' elapsed_seconds={elapsed:.2f} "
                        f"retrying_in={backoff:.2f}s"
                    )
                    time.sleep(backoff)
                    continue  # Retry the request
                else:
                    # Final attempt failed
                    self.logger.error(
                        f"ChunkedEncodingError after {max_retries} attempts "
                        f"url={url} query={params.get('query')} error='{e}' elapsed_seconds={elapsed:.2f}"
                    )
                    return None

            except requests.exceptions.RequestException as e:
                elapsed = time.time() - start_time
                self.logger.error(f'Request exception url={url} error="{e}" ' f"elapsed_seconds={elapsed:.2f}")
                return None

        # Should never reach here, but just in case
        return None

    def query(self, prom_query: str):
        params = dict(query=prom_query, max_series=2000, limit=2000)

        # self.logger.info(f'Executing instant query query="{prom_query}"')
        response = self._send_get_request("/api/v1/query", params)

        if response:
            status = response.get("status")
            result_count = len(response.get("data", {}).get("result", []))
            self.logger.info(f"Instant query completed status={status} result_count={result_count}")

        return response

    def query_range(self, prom_query: str, start: int, end: int, step: int):
        if start >= end:
            self.logger.error(f"Invalid time range start={start} end={end}")
            raise ValueError("Start must be before end")

        params = dict(
            query=prom_query,
            start=start,
            end=end,
            step=step,
            limit=0,
        )

        duration = end - start

        response = self._send_get_request("/api/v1/query_range", params)

        if response:
            status = response.get("status")
            result_count = len(response.get("data", {}).get("result", []))
            # self.logger.info(
            #     f"Range query completed status={status} result_count={result_count}"
            # )

        return response

    def get_labels(self):
        self.logger.debug("Fetching all labels")
        params = {}
        response = self._send_get_request("/api/v1/labels", params)

        if response and response.get("status") == "success":
            labels = response.get("data", [])
            self.logger.info(f"Labels fetched successfully count={len(labels)}")
            return labels

        self.logger.warning("Failed to fetch labels or received non-success status")
        return None

    def get_label_values(self, label: str):
        self.logger.debug(f"Fetching values for label={label}")
        params = {}
        endpoint = f"/api/v1/label/{label}/values"
        response = self._send_get_request(endpoint, params)

        if response and response.get("status") == "success":
            values = response.get("data", [])
            self.logger.info(f"Label values fetched successfully label={label} count={len(values)}")
            return values

        self.logger.warning(f"Failed to fetch label values label={label}")
        return None