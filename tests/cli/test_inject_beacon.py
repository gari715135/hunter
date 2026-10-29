import logging

import numpy as np
import pandas as pd
import pytest

from hunt.cli import inject_beacon
from hunt.signal_analysis.beacon_detector import BeaconDetector


def real_looking_frame():
    """Two hours of random (non-periodic) traffic to a public and an internal server."""
    rng = np.random.default_rng(1)
    times = pd.date_range("2026-10-20T00:00:00Z", periods=720, freq="10s")
    rows = []
    for server in ("52.1.2.3", "10.0.0.6"):
        for t, v in zip(times, rng.poisson(3, len(times))):
            rows.append({"time": t, "metric.client_ip": "10.0.0.5", "metric.server_ip": server, "value": float(v),
                         "metric.kube_pod_name": "pod-a", "metric.kube_cluster_name": "eks-dev"})
    return pd.DataFrame(rows)


@pytest.fixture
def partition(tmp_path):
    path = tmp_path / "src" / "metric=sysdig_connection_net_connection_out_count" / "cluster=eks-dev" / "dt=20261020"
    path.mkdir(parents=True)
    real_looking_frame().to_parquet(path / "part-0.parquet")
    return path


def run_injection(partition, out, *extra):
    inject_beacon.main([str(partition), "--out", str(out), *extra])
    return out / "metric=sysdig_connection_net_connection_out_count" / "cluster=eks-dev" / "dt=20261020"


def detect(path):
    candidates = BeaconDetector(logger=logging.getLogger("test.inject")).detect(pd.read_parquet(path))
    return {(c.client_ip, c.server_ip): c for c in candidates}


def test_keeps_original_rows_and_partition_folders(partition, tmp_path):
    out = run_injection(partition, tmp_path / "out")
    combined = pd.read_parquet(out)
    injected = combined[combined["metric.kube_pod_name"] == inject_beacon.INJECTED_POD]
    assert len(combined) - len(injected) == 1440
    assert len(injected) == 720
    assert set(injected["metric.kube_cluster_name"]) == {"eks-dev"}
    assert injected["value"].sum() == 120  # one check-in per minute for two hours


@pytest.mark.parametrize("period", [60, 300, 600])
def test_injected_beacon_ranks_first(partition, tmp_path, period):
    out = run_injection(partition, tmp_path / "out", "--period", str(period), "--jitter", "5")
    candidates = BeaconDetector(logger=logging.getLogger("test.inject")).detect(pd.read_parquet(out))
    assert (candidates[0].client_ip, candidates[0].server_ip) == ("10.250.250.250", "203.0.113.10")
    noise = next(c for c in candidates if c.server_ip == "52.1.2.3")
    assert candidates[0].power_ratio > 100 * noise.power_ratio


@pytest.mark.parametrize("period", [60, 300, 600])
@pytest.mark.parametrize("jitter", [0, 15])
def test_detector_flags_injected_beacon(partition, tmp_path, period, jitter):
    out = run_injection(partition, tmp_path / "out", "--period", str(period), "--jitter", str(jitter))
    found = detect(out)[("10.250.250.250", "203.0.113.10")]
    assert found.is_suspicious
    assert found.peak_period_seconds == pytest.approx(period, rel=0.05)
    assert not detect(out)[("10.0.0.5", "52.1.2.3")].is_suspicious


def test_period_too_short_is_rejected(partition, tmp_path):
    with pytest.raises(SystemExit):
        inject_beacon.main([str(partition), "--out", str(tmp_path / "out"), "--period", "10"])
