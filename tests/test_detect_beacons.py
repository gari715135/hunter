import json
import logging

import numpy as np
import pandas as pd
import pytest
import yaml

from scripts import detect_beacons
from src.logger_config import log_context
from src.signal_analysis.beacon_detector import BeaconCandidate, BeaconDetector


def traffic_frame():
    """One periodic external pair (spike every 60s for an hour) plus one internal pair."""
    times = pd.date_range("2026-07-18T00:00:00Z", periods=360, freq="10s")
    beacon = np.where(np.arange(len(times)) % 6 == 0, 1000.0, 1.0)
    rows = [
        {"time": t, "metric.client_ip": "10.0.0.5", "metric.server_ip": "8.8.8.8", "value": v,
         "metric.kube_pod_name": "pod-a"}
        for t, v in zip(times, beacon)
    ]
    rows += [
        {"time": t, "metric.client_ip": "10.0.0.5", "metric.server_ip": "10.0.0.6", "value": 5.0,
         "metric.kube_pod_name": "pod-a"}
        for t in times
    ]
    return pd.DataFrame(rows)


def candidate(suspicious):
    return BeaconCandidate(client_ip="10.0.0.5", server_ip="8.8.8.8", sample_size=360, is_suspicious=suspicious,
                           labels={"metric.kube_pod_name": "pod-a"})


def test_log_results_emits_one_candidate_event_each(capture):
    detector = BeaconDetector(logger=capture.logger)
    with log_context(run_id="run-1", cluster="c1"):
        written = detector.log_results([candidate(True), candidate(False)], record_logger=capture.logger)

    assert written == 2
    events = capture.by_event("beacon_candidate")
    assert len(events) == 2
    first = events[0]
    assert (first["run_id"], first["cluster"]) == ("run-1", "c1")
    assert first["server_ip"] == "8.8.8.8" and first["is_suspicious"] is True
    assert first["metric.kube_pod_name"] == "pod-a"
    assert first["app"] == "test-app" and "ts" in first and "event_time" not in first


def test_log_results_suspicious_only(capture):
    detector = BeaconDetector(logger=capture.logger)
    written = detector.log_results([candidate(True), candidate(False)], suspicious_only=True)
    assert written == 1
    assert len(capture.by_event("beacon_candidate")) == 1


def test_detect_emits_filter_and_completion_events(capture):
    detector = BeaconDetector(logger=capture.logger)
    results = detector.detect(traffic_frame())

    assert [r.server_ip for r in results] == ["8.8.8.8"]
    (filtered,) = capture.by_event("external_filter_completed")
    assert (filtered["rows_in"], filtered["rows_external"]) == (720, 360)
    assert (filtered["servers_total"], filtered["servers_external"]) == (2, 1)
    (done,) = capture.by_event("beacon_detection_completed")
    assert (done["pairs_total"], done["pairs_evaluated"], done["pairs_skipped"]) == (1, 1, 0)
    assert done["suspicious"] == int(results[0].is_suspicious)


def test_detect_no_external_traffic(capture):
    df = traffic_frame()
    df = df[df["metric.server_ip"] == "10.0.0.6"]
    assert BeaconDetector(logger=capture.logger).detect(df) == []
    assert len(capture.by_event("beacon_no_external_traffic")) == 1


def test_partition_context_from_hive_path():
    uri = "s3://b/conformed_daily/metric=sysdig_connection_net_out_bytes/cluster=eks-dev/dt=20260718/"
    assert detect_beacons.partition_context(uri) == {"metric": "sysdig_connection_net_out_bytes", "cluster": "eks-dev"}
    assert detect_beacons.partition_context("/data/file.parquet") == {}


def test_log_dir_overrides():
    cfg = {"beacon_logging": {"handlers": {
        "file": {"path": "logs/detect_beacons/app.log"},
        "error_file": {"path": "logs/detect_beacons/error.log"},
        "records": {"path": "logs/detect_beacons/beacons.log"},
    }}}
    detect_beacons.apply_log_dir_overrides(cfg, "/var/log/hunt", "out.log")
    handlers = cfg["beacon_logging"]["handlers"]
    assert handlers["file"]["path"] == "/var/log/hunt/app.log"
    assert handlers["error_file"]["path"] == "/var/log/hunt/error.log"
    assert handlers["records"]["path"] == "/var/log/hunt/out.log"


@pytest.fixture
def restore_pipeline_loggers():
    yield
    for name in ("prometheus_pipeline", "prometheus_pipeline.records", "urllib3"):
        logger = logging.getLogger(name)
        for handler in logger.handlers:
            handler.close()
        logger.handlers.clear()


def test_main_writes_json_events_end_to_end(tmp_path, monkeypatch, restore_pipeline_loggers):
    data_dir = tmp_path / "metric=sysdig_connection_net_out_bytes" / "cluster=eks-dev"
    data_dir.mkdir(parents=True)
    traffic_frame().to_parquet(data_dir / "part-0.parquet")

    with open(detect_beacons.PROJECT_ROOT + "/config.yaml") as f:
        cfg = yaml.safe_load(f)
    cfg["beacon_logging"]["handlers"]["console"]["enabled"] = False
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(cfg))

    log_dir = tmp_path / "logs"
    monkeypatch.setenv("BEACON_CONFIG", str(config_path))
    monkeypatch.setenv("BEACON_INPUT", str(data_dir))
    monkeypatch.setenv("BEACON_LOG_DIR", str(log_dir))

    assert detect_beacons.main() == 0

    app_events = [json.loads(line) for line in (log_dir / "app.log").read_text().splitlines()]
    records = [json.loads(line) for line in (log_dir / "beacons.log").read_text().splitlines()]

    names = [e["event"] for e in app_events]
    for name in ("detect_run_started", "input_read_completed", "external_filter_completed",
                 "beacon_detection_completed", "detect_run_completed"):
        assert name in names
    assert "beacon_candidate" not in names

    assert [r["event"] for r in records] == ["beacon_candidate"]
    run_ids = {e["run_id"] for e in app_events + records}
    assert len(run_ids) == 1
    for e in app_events + records:
        assert e["app"] == "hunt-beacon"
        assert (e["metric"], e["cluster"]) == ("sysdig_connection_net_out_bytes", "eks-dev")

    done = app_events[names.index("detect_run_completed")]
    assert (done["status"], done["exit_code"], done["candidates"], done["records_written"]) == ("success", 0, 1, 1)


def test_main_without_input_fails(tmp_path, monkeypatch, restore_pipeline_loggers):
    with open(detect_beacons.PROJECT_ROOT + "/config.yaml") as f:
        cfg = yaml.safe_load(f)
    cfg["beacon_logging"]["handlers"]["console"]["enabled"] = False
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(cfg))
    monkeypatch.setenv("BEACON_CONFIG", str(config_path))
    monkeypatch.setenv("BEACON_LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.delenv("BEACON_INPUT", raising=False)

    assert detect_beacons.main() == 2
    errors = [json.loads(line) for line in (tmp_path / "logs" / "error.log").read_text().splitlines()]
    assert [e["event"] for e in errors] == ["config_missing", "detect_run_completed"]
    assert errors[1]["status"] == "failed"
