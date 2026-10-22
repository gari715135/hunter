import contextvars
import json
import threading

from src.logger_config import build_formatter, log_context


def test_json_line_with_quotes_and_newlines(capture):
    capture.logger.info('querying "cluster=a"\nsecond line', extra={"event": "x", "query": 'm{a="b"}'})
    assert len(capture.lines) == 1
    event = capture.events[0]
    assert event["message"] == 'querying "cluster=a"\nsecond line'
    assert event["query"] == 'm{a="b"}'
    assert event["event"] == "x"
    assert event["app"] == "test-app"
    assert event["ts"].endswith("+00:00")


def test_exception_is_one_field_on_one_line(capture):
    try:
        raise RuntimeError("boom")
    except RuntimeError:
        capture.logger.exception("failed", extra={"event": "fetch_job_error"})
    assert len(capture.lines) == 1
    event = capture.events[0]
    assert event["exc_type"] == "RuntimeError"
    assert "Traceback" in event["exc"] and "boom" in event["exc"]
    assert event["level"] == "ERROR"


def test_default_event_and_no_context_fields_when_unset(capture):
    capture.logger.info("plain")
    event = capture.events[0]
    assert event["event"] == "log"
    for name in ("run_id", "job_id", "metric", "cluster", "correlation_id"):
        assert name not in event


def test_func_and_line_point_at_caller(capture):
    def caller():
        capture.logger.warning("where am I")

    caller()
    assert capture.events[0]["func"] == "caller"
    assert capture.events[0]["module"] == "test_logger_config"


def test_context_follows_copy_context_into_threads(capture):
    with log_context(run_id="run-1"):
        ctx = contextvars.copy_context()

    def worker():
        with log_context(job_id="m:c", metric="m", cluster="c"):
            capture.logger.info("in job", extra={"event": "job"})
        capture.logger.info("after job", extra={"event": "after"})

    t = threading.Thread(target=ctx.run, args=(worker,))
    t.start()
    t.join()

    job, after = capture.events
    assert (job["run_id"], job["job_id"], job["metric"], job["cluster"]) == ("run-1", "m:c", "m", "c")
    assert after["run_id"] == "run-1" and "job_id" not in after


def test_explicit_extra_wins_over_context(capture):
    with log_context(cluster="ambient"):
        capture.logger.info("x", extra={"cluster": "explicit"})
    assert capture.events[0]["cluster"] == "explicit"


def test_non_serializable_extras_are_stringified(capture):
    capture.logger.info("x", extra={"obj": object(), "items": {1, 2}})
    event = capture.events[0]
    assert isinstance(event["obj"], str) and isinstance(event["items"], str)


def test_kv_format_still_available():
    formatter = build_formatter({"format": {"type": "kv"}})
    assert "%(correlation_id)s" in formatter._fmt
    assert json.dumps({}) == "{}"
