import json

import pytest
import requests

from src.prometheus_tools import client as client_module
from src.prometheus_tools.client import PrometheusClient, PromQueryError


def make_response(status_code=200, body=b"", reason="OK"):
    response = requests.Response()
    response.status_code = status_code
    response.reason = reason
    response._content = body if isinstance(body, bytes) else json.dumps(body).encode()
    return response


@pytest.fixture
def client(capture, monkeypatch):
    monkeypatch.setattr(client_module.time, "sleep", lambda s: None)
    return PrometheusClient("token", capture.logger, "https://example.test")


def stub_requests(monkeypatch, client, outcomes):
    """Each outcome is a Response to return or an exception to raise, in order."""
    outcomes = list(outcomes)

    def fake_request(**kwargs):
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(client.session, "request", fake_request)


def test_success_returns_response_metadata(monkeypatch, client, capture):
    body = {"status": "success", "data": {"result": [{}, {}]}}
    stub_requests(monkeypatch, client, [make_response(200, body)])

    response = client.query_range("up", 1, 2, 10)

    assert response.data == body
    assert response.status_code == 200 and response.attempts == 1
    assert response.response_bytes == len(json.dumps(body))
    assert capture.by_event("prom_range_query")[0]["series"] == 2


def test_prometheus_error_body_is_parsed(monkeypatch, client):
    body = {"status": "error", "errorType": "bad_data", "error": "parse error at char 5"}
    stub_requests(monkeypatch, client, [make_response(400, body, "Bad Request")])

    with pytest.raises(PromQueryError) as exc:
        client.query_range("up{", 1, 2, 10)

    e = exc.value
    assert (e.status_code, e.error_type, e.error) == (400, "bad_data", "parse error at char 5")
    assert e.body_preview is None
    assert e.as_fields()["http_status"] == 400


def test_non_json_error_keeps_body_preview(monkeypatch, client):
    stub_requests(monkeypatch, client, [make_response(502, b"<html>bad gateway</html>", "Bad Gateway")])

    with pytest.raises(PromQueryError) as exc:
        client.query_range("up", 1, 2, 10)

    assert exc.value.error_type == "Bad Gateway"
    assert exc.value.body_preview == "<html>bad gateway</html>"


def test_invalid_json_on_200(monkeypatch, client):
    stub_requests(monkeypatch, client, [make_response(200, b"not json")])

    with pytest.raises(PromQueryError) as exc:
        client.query_range("up", 1, 2, 10)

    assert exc.value.error_type == "JSONDecodeError"
    assert exc.value.body_preview == "not json"


def test_chunked_encoding_error_is_retried(monkeypatch, client, capture):
    ok = make_response(200, {"status": "success", "data": {"result": []}})
    err = requests.exceptions.ChunkedEncodingError("incomplete read")
    stub_requests(monkeypatch, client, [err, err, ok])

    response = client.query_range("up", 1, 2, 10)

    assert response.attempts == 3
    assert len(capture.by_event("http_retry")) == 2


def test_chunked_encoding_error_gives_up(monkeypatch, client):
    err = requests.exceptions.ChunkedEncodingError("incomplete read")
    stub_requests(monkeypatch, client, [err, err, err])

    with pytest.raises(PromQueryError) as exc:
        client.query_range("up", 1, 2, 10)

    assert exc.value.attempts == 3 and exc.value.error_type == "ChunkedEncodingError"


def test_timeout_raises_with_type(monkeypatch, client):
    stub_requests(monkeypatch, client, [requests.exceptions.ReadTimeout("read timed out")])

    with pytest.raises(PromQueryError) as exc:
        client.query_range("up", 1, 2, 10)

    assert exc.value.error_type == "ReadTimeout"


def test_label_values_failure_returns_none_and_logs(monkeypatch, client, capture):
    stub_requests(monkeypatch, client, [make_response(503, b"down", "Service Unavailable")])

    assert client.get_label_values("agent_tag_cluster") is None
    event = capture.by_event("label_values_fetch_failed")[0]
    assert event["http_status"] == 503 and event["label"] == "agent_tag_cluster"


def test_query_is_not_repeated_in_request_logs(monkeypatch, client, capture):
    stub_requests(monkeypatch, client, [make_response(200, {"status": "success", "data": {"result": []}})])
    client.query_range("very_long_query", 1, 2, 10)
    request_event = capture.by_event("http_request")[0]
    assert "query" not in request_event["params"]
