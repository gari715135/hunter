from src.logger_config import log_context
from src.prometheus_tools import orchestrator as orchestrator_module
from src.prometheus_tools.orchestrator import Job, MetricsFetchOrchestrator
from src.prometheus_tools.prom_ingestor import JobResult
from src.prometheus_tools.storage_backends import StorageWriteError


class StubIngestor:
    """get_sysdig_metric outcome per cluster: a status string, or an exception to raise."""

    def __init__(self, logger, outcomes):
        self.logger = logger
        self.outcomes = outcomes

    def window_fields(self):
        return {"window_start": 1, "window_end": 2, "step_s": 10, "dt": "20261021"}

    def get_sysdig_metric(self, metric, cluster, fetch_all_clusters=False):
        outcome = self.outcomes[cluster]
        if isinstance(outcome, Exception):
            raise outcome
        return JobResult(metric=metric, cluster=cluster, status=outcome, rows=5 if outcome == "success" else 0)


def run(capture, outcomes, workers=2):
    orch = MetricsFetchOrchestrator(StubIngestor(capture.logger, outcomes), logger=capture.logger)
    with log_context(run_id="run-1"):
        return orch.run_fetch_cycle(metrics=["m"], clusters=list(outcomes), workers=workers)


def test_failures_are_counted_and_listed(capture):
    summary = run(
        capture,
        {
            "ok": "success",
            "none": "empty",
            "bad": "failed",
            "boom": StorageWriteError("s3://b/k", OSError("denied")),
        },
    )

    assert (summary["total"], summary["succeeded"], summary["empty"], summary["failed"]) == (4, 1, 1, 2)
    assert summary["failed_jobs"] == ["m:bad", "m:boom"]
    assert summary["interrupted"] is False


def test_every_job_emits_one_completed_event_with_context(capture):
    run(capture, {"ok": "success", "boom": RuntimeError("kaput")})

    completed = {e["cluster"]: e for e in capture.by_event("fetch_job_completed")}
    assert set(completed) == {"ok", "boom"}
    for cluster, event in completed.items():
        assert event["run_id"] == "run-1"
        assert event["job_id"] == f"m:{cluster}"
        assert event["dt"] == "20261021" and "duration_ms" in event
        assert event["thread"].startswith("fetch")
    assert completed["ok"]["level"] == "INFO"
    assert completed["boom"]["level"] == "ERROR"
    assert completed["boom"]["error_type"] == "RuntimeError"

    error = capture.by_event("fetch_job_error")[0]
    assert error["job_id"] == "m:boom" and "kaput" in error["exc"]


def test_progress_event_is_structured(capture):
    run(capture, {"a": "success"})
    progress = capture.by_event("fetch_progress")[-1]
    assert (progress["completed"], progress["total"], progress["percent"]) == (1, 1, 100.0)


def test_exit_codes(monkeypatch):
    from scripts.fetch_data import exit_code_for

    assert exit_code_for({"failed": 0, "interrupted": False}) == 0
    assert exit_code_for({"failed": 3, "interrupted": False}) == 1
    monkeypatch.setattr(orchestrator_module, "shutdown_signal", 15)
    assert exit_code_for({"failed": 0, "interrupted": True}) == 143


def test_job_ids():
    assert orchestrator_module.job_id(Job("m", None)) == "m:all_clusters"
    assert orchestrator_module.job_id(Job("m", "c")) == "m:c"
