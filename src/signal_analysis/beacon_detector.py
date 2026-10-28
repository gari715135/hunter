# ./src/signal_analysis/beacon_detector.py
"""
MVP periodic-beacon detector.

Replaces the exhaustive hierarchical drill-down for the specific job of finding
periodic C2 beaconing. Instead of running an FFT at every level of a label
hierarchy (cluster -> namespace -> src_ip -> dst_ip -> ... ), this:

  1. Filters destinations to PUBLIC / internet-routable IPs only. Beacon-to-C2
     is almost always egress to a public host, and in-cluster / in-VPC traffic
     (all RFC1918 + CGNAT) is exactly the high-volume noise we want to drop.
     This is a single global rule -- identical across all clusters, no per-cluster
     config -- which is the whole point for a 60-cluster fleet.
  2. Groups to the natural unit of a beacon: the (client_ip, server_ip) pair,
     i.e. one conversation. That is where the periodic signal is cleanest.
  3. Runs the FFT once per surviving pair (reusing SignalTransformations).
  4. Uses the label hierarchy purely as enrichment/attribution on the hits.

MVP shortcuts (deliberately deferred):
  * Public/private is a coarse external test. It will MISS C2 hosted on a
    private IP inside your own VPC or a peered VPC. Refinement = per-cluster
    VPC + service CIDR table, auto-derived from describe-cluster/describe-vpcs,
    keyed on kube_cluster_name. Not built here.
  * Single-node pandas/numpy. Per-pair work is embarrassingly parallel and can
    be pushed to Spark later; the FFT itself stays per-series numpy.
  * Periodicity alone is not sufficient: lots of infra (prometheus, keda, csi,
    thanos, health probes) beacons benignly by design. An allowlist of known
    periodic (program, destination) patterns is the obvious next layer.
"""
import ipaddress
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.logger_config import ContextualLogger
from src.signal_analysis.signal_transformations import SignalTransformations

# ---------------------------------------------------------------------------
# External-IP classification (the cheap global filter)
# ---------------------------------------------------------------------------

# Anything NOT inside one of these ranges is treated as external/egress.
# Kept explicit rather than relying on ipaddress.is_global, whose semantics
# have shifted across Python versions.
_INTERNAL_NETWORKS = [
    ipaddress.ip_network(cidr)
    for cidr in (
        "10.0.0.0/8",  # RFC1918
        "172.16.0.0/12",  # RFC1918
        "192.168.0.0/16",  # RFC1918
        "100.64.0.0/10",  # CGNAT -- EKS secondary CIDRs / Fargate
        "127.0.0.0/8",  # loopback
        "169.254.0.0/16",  # link-local (IMDS, etc.)
        "0.0.0.0/8",  # this-host / unspecified
        # IPv6 internal
        "::1/128",  # loopback
        "fe80::/10",  # link-local
        "fc00::/7",  # unique local (ULA)
    )
]


def is_external_ip(ip_str: str) -> bool:
    """True if ip_str is a public, internet-routable address.

    Unparseable / null values return False (treated as internal -> ignored) so
    junk values never manufacture a false beacon.
    """
    try:
        ip = ipaddress.ip_address(str(ip_str))
    except (ValueError, TypeError):
        return False
    return not any(ip in net for net in _INTERNAL_NETWORKS)


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass
class BeaconCandidate:
    """One (client_ip, server_ip) conversation evaluated for periodicity."""

    client_ip: str
    server_ip: str
    sample_size: int
    # Estimated beacon interval (the fundamental, not whichever harmonic was strongest).
    peak_frequency_hz: Optional[float] = None
    peak_period_seconds: Optional[float] = None
    # Strongest bin, the median of its neighbouring bins, and their ratio. Suspicious when
    # power_ratio > threshold_factor, i.e. peak_power > threshold (= factor * median_power).
    peak_power: Optional[float] = None
    median_power: Optional[float] = None
    power_ratio: Optional[float] = None
    threshold: Optional[float] = None
    is_suspicious: bool = False
    # Observation window / volume, for IR triage.
    event_count: int = 0
    window_start: Optional[str] = None
    window_end: Optional[str] = None
    duration_seconds: Optional[float] = None
    # Attribution: most-common value of each enrichment column for this pair.
    labels: Dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Detector
# ---------------------------------------------------------------------------


class BeaconDetector:
    def __init__(
        self,
        logger: Union[logging.Logger, ContextualLogger],
        client_col: str = "metric.client_ip",
        server_col: str = "metric.server_ip",
        time_col: str = "time",
        value_col: str = "value",
        enrich_cols: Optional[List[str]] = None,
        step_seconds: int = 10,
        agg: str = "sum",
        min_samples: int = 32,
        window_type: str = "hamming",
        offset_pct: float = 0.1,
        threshold_factor: float = 8.0,
    ):
        if agg not in {"sum", "mean"}:
            raise ValueError("agg must be 'sum' or 'mean'.")
        self.logger = logger
        self.client_col = client_col
        self.server_col = server_col
        self.time_col = time_col
        self.value_col = value_col
        self.enrich_cols = (
            enrich_cols
            if enrich_cols is not None
            else [
                # Source identity
                "metric.kube_cluster_name",
                "metric.kube_node_name",
                "metric.kube_namespace_name",
                "metric.kube_pod_name",
                "metric.kube_workload_name",
                "metric.kube_workload_type",
                "metric.container_label_io_kubernetes_container_name",
                # Originating process (client-side) -- what to actually go look at
                "metric.program",
                "metric.program_cmd_line",
                # Destination context (present only if the connection-level metrics
                # are being scraped; _enrich skips any column that isn't there)
                "metric.net_remote_endpoint",  # resolved dest hostname, if Sysdig has it
                "metric.net_remote_service",
                "metric.net_protocol",
            ]
        )
        self.step_seconds = step_seconds
        self.agg = agg
        self.min_samples = min_samples
        self.window_type = window_type
        self.offset_pct = offset_pct
        self.threshold_factor = threshold_factor

    # -- public entrypoint -----------------------------------------------

    def detect(self, df: pd.DataFrame) -> List[BeaconCandidate]:
        """Return beacon candidates, most-suspicious first."""
        start = time.monotonic()
        df = self._filter_external(df)
        if df.empty:
            self.logger.info("No external-destination traffic after filtering", extra={"event": "beacon_no_external_traffic"})
            return []

        df = df.copy()
        df[self.time_col] = pd.to_datetime(df[self.time_col])

        results: List[BeaconCandidate] = []
        pairs = 0
        pair_cols = [self.client_col, self.server_col]
        for (client_ip, server_ip), sub_df in df.groupby(pair_cols, sort=False):
            pairs += 1
            cand = self._evaluate_pair(str(client_ip), str(server_ip), sub_df)
            if cand is not None:
                results.append(cand)

        # Suspicious first, then by how far the peak stands above the noise floor.
        results.sort(
            key=lambda c: (c.is_suspicious, c.power_ratio or 0.0),
            reverse=True,
        )
        self.logger.info(
            "Beacon detection completed",
            extra={
                "event": "beacon_detection_completed",
                "pairs_total": pairs,
                "pairs_evaluated": len(results),
                # too few samples after resample, flat series, or empty spectrum
                "pairs_skipped": pairs - len(results),
                "suspicious": sum(c.is_suspicious for c in results),
                "duration_ms": int((time.monotonic() - start) * 1000),
            },
        )
        return results

    def log_results(
        self,
        results: List[BeaconCandidate],
        record_logger: Union[logging.Logger, ContextualLogger, None] = None,
        suspicious_only: bool = False,
    ) -> int:
        """Emit one `beacon_candidate` event per candidate, its fields as `extra`.

        Writes to record_logger if given, else self.logger. Use the dedicated
        records logger (the `records` handler in setup_logging_system) so
        diagnostic chatter stays out of the records file. Returns the number of
        events written.
        """
        sink = record_logger if record_logger is not None else self.logger
        written = 0
        for c in results:
            if suspicious_only and not c.is_suspicious:
                continue
            sink.info("Beacon candidate", extra={"event": "beacon_candidate", **candidate_record(c)})
            written += 1
        return written

    # -- internals -------------------------------------------------------

    def _filter_external(self, df: pd.DataFrame) -> pd.DataFrame:
        """Keep only rows whose server_ip is public. Classify each distinct IP
        once (not per row) so this stays cheap on large frames."""
        if self.server_col not in df.columns:
            raise KeyError(f"Expected destination column '{self.server_col}' not in frame.")
        unique_ips = df[self.server_col].dropna().unique()
        external = {ip for ip in unique_ips if is_external_ip(ip)}
        filtered = df[df[self.server_col].isin(external)]
        self.logger.info(
            "Filtered to external destinations",
            extra={
                "event": "external_filter_completed",
                "rows_in": len(df),
                "rows_external": len(filtered),
                "servers_total": len(unique_ips),
                "servers_external": len(external),
            },
        )
        return filtered

    def _resample(self, sub_df: pd.DataFrame) -> pd.Series:
        series = sub_df.set_index(self.time_col)[self.value_col].astype(float)
        rule = f"{self.step_seconds}s"
        if self.agg == "sum":
            series = series.resample(rule).sum()
        else:
            series = series.resample(rule).mean()
        series = series.interpolate(method="time")
        series = series.bfill().ffill()
        return series

    def _enrich(self, sub_df: pd.DataFrame) -> Dict[str, str]:
        labels: Dict[str, str] = {}
        for col in self.enrich_cols:
            if col in sub_df.columns:
                mode = sub_df[col].mode()
                if len(mode):
                    labels[col] = str(mode.iloc[0])
        return labels

    def _evaluate_pair(self, client_ip: str, server_ip: str, sub_df: pd.DataFrame) -> Optional[BeaconCandidate]:
        series = self._resample(sub_df)
        if len(series) < self.min_samples:
            return None

        values = series.to_numpy(dtype=float)
        time_arr = series.index.to_numpy()

        centered = values - np.mean(values)
        if np.allclose(centered, 0):
            return None  # flat -- no periodic content to find

        try:
            magnitude, freqs = SignalTransformations.compute_fft(
                data=centered,
                time_arr=time_arr,
                window_type=self.window_type,
            )
        except ValueError:
            # Spacing should be uniform after resample; fall back just in case.
            freqs = np.fft.rfftfreq(len(centered), d=self.step_seconds)
            windowed = SignalTransformations.apply_window(centered, self.window_type)
            magnitude = np.abs(np.fft.rfft(windowed))

        spectrum = np.abs(magnitude)
        if len(spectrum) <= 1:
            return None

        # Score each bin against the median of its neighbours, not the whole spectrum: a beacon's
        # harmonics would inflate a global mean/std, and smooth traffic (drift, bursts) raises
        # its neighbours along with itself. Bins below offset_pct (slow drift) can't be the peak.
        floor = local_noise_floor(spectrum) + 1e-9
        snr = spectrum / floor
        offset = max(int(np.floor(self.offset_pct * len(spectrum))), 1)
        if offset >= len(spectrum):
            return None
        peak_index = offset + int(np.argmax(snr[offset:]))
        peak_power = float(spectrum[peak_index])
        median_power = float(floor[peak_index])
        ratio = float(snr[peak_index])
        threshold = self.threshold_factor * median_power
        is_suspicious = ratio > self.threshold_factor

        # A beacon's spectrum is a comb: the strongest bin may be any harmonic, so walk down to
        # the fundamental before turning it into a period.
        fundamental_bin = fundamental_frequency_bin(spectrum, snr, peak_index, self.threshold_factor)
        peak_freq = float(fundamental_bin * freqs[1]) if fundamental_bin > 0 else None
        peak_period = float(1.0 / peak_freq) if peak_freq else None

        # Observation window / volume from the raw (pre-resample) rows.
        times = pd.to_datetime(sub_df[self.time_col])
        window_start = times.min()
        window_end = times.max()
        duration = float((window_end - window_start).total_seconds())

        return BeaconCandidate(
            client_ip=client_ip,
            server_ip=server_ip,
            sample_size=len(values),
            peak_frequency_hz=peak_freq,
            peak_period_seconds=peak_period,
            peak_power=peak_power,
            median_power=median_power,
            power_ratio=ratio,
            threshold=threshold,
            is_suspicious=bool(is_suspicious),
            event_count=int(len(sub_df)),
            window_start=window_start.isoformat(),
            window_end=window_end.isoformat(),
            duration_seconds=duration,
            labels=self._enrich(sub_df),
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

NOISE_WINDOW_BINS = 25  # neighbours on each side that make up a bin's noise floor
NOISE_GUARD_BINS = 2  # left out on each side: a peak's own main lobe (Hamming: +/-2 bins)


def local_noise_floor(spectrum: np.ndarray) -> np.ndarray:
    """Median magnitude of each bin's neighbours, leaving out the bin's own main lobe."""
    width = 2 * NOISE_WINDOW_BINS + 1
    padded = np.pad(spectrum, NOISE_WINDOW_BINS, mode="reflect" if len(spectrum) > NOISE_WINDOW_BINS else "edge")
    windows = np.lib.stride_tricks.sliding_window_view(padded, width)
    keep = np.ones(width, dtype=bool)
    keep[NOISE_WINDOW_BINS - NOISE_GUARD_BINS:NOISE_WINDOW_BINS + NOISE_GUARD_BINS + 1] = False
    return np.median(windows[:, keep], axis=1)


def fundamental_frequency_bin(
    spectrum: np.ndarray,
    snr: np.ndarray,
    peak_index: int,
    threshold_factor: float,
    min_cycles: int = 3,
    min_relative_magnitude: float = 0.1,
) -> float:
    """Fractional FFT bin of the fundamental behind the peak at peak_index.

    Tries peak/m for the largest m first and takes it when a real peak sits there (at least
    `min_relative_magnitude` of the main peak, which window leakage never reaches) and at least
    half of its multiples up to the main peak are peaks too. The answer is peak/m rather than
    the fundamental's own bin, because the main peak's position is far more precise.
    """
    n = len(spectrum)
    position = float(peak_index)
    if 0 < peak_index < n - 1:
        # Parabolic interpolation on log magnitude: sub-bin position of the main peak.
        left, centre, right = np.log(spectrum[peak_index - 1:peak_index + 2] + 1e-12)
        curvature = left - 2 * centre + right
        if curvature < 0:
            position = peak_index + 0.5 * (left - right) / curvature

    is_peak = np.zeros(n, dtype=bool)
    is_peak[1:-1] = (spectrum[1:-1] >= spectrum[:-2]) & (spectrum[1:-1] >= spectrum[2:])
    peaks = np.flatnonzero(is_peak & (snr >= threshold_factor / 2))

    def peak_near(target: float) -> Optional[int]:
        i = int(np.searchsorted(peaks, target))
        close = [p for p in peaks[max(i - 1, 0):i + 1] if abs(p - target) <= 1]
        return min(close, key=lambda p: abs(p - target)) if close else None

    for m in range(int(position // min_cycles), 1, -1):
        base = position / m
        found = peak_near(base)
        if found is None or spectrum[found] < min_relative_magnitude * spectrum[peak_index]:
            continue
        hits = sum(peak_near(base * h) is not None for h in range(1, m + 1))
        if hits >= m / 2:
            return base
    return position


def candidate_record(c: BeaconCandidate) -> Dict:
    """Flatten one candidate (core fields + enrichment labels) into a plain dict.
    Used for both log events and DataFrame export. All values are JSON-native."""
    record = {
        "is_suspicious": c.is_suspicious,
        "client_ip": c.client_ip,
        "server_ip": c.server_ip,
        # Beacon characteristics
        "peak_period_seconds": c.peak_period_seconds,
        "peak_frequency_hz": c.peak_frequency_hz,
        "power_ratio": c.power_ratio,
        "peak_power": c.peak_power,
        "median_power": c.median_power,
        "threshold": c.threshold,
        "sample_size": c.sample_size,
        # Observation window / volume
        "event_count": c.event_count,
        "window_start": c.window_start,
        "window_end": c.window_end,
        "duration_seconds": c.duration_seconds,
    }
    # Enrichment labels (source identity, process, destination context)
    record.update(c.labels)
    return record


def candidates_to_frame(results: List[BeaconCandidate]) -> pd.DataFrame:
    """Flatten candidates into a DataFrame for ranking / export / Power BI."""
    return pd.DataFrame([candidate_record(c) for c in results])

