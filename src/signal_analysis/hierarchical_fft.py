# ./src/signal_analysis/hierarchical_fft.py
import json
import logging
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

from src.signal_analysis.signal_transformations import SignalTransformations


@dataclass
class FFTThresholds:
    """Configuration for FFT-based anomaly detection."""

    window_type: str = "hamming"
    offset_pct: float = 0.1
    threshold_factor: float = 4.0
    ratio_threshold: float = 4.0
    min_power: float = 0.0
    min_period: Optional[float] = None
    max_period: Optional[float] = None
    min_samples: int = 32


@dataclass
class SeriesResult:
    """Result of analysing a single time series."""

    level: str
    labels: Dict[str, str]
    sample_size: int
    mean: float
    stddev: float
    peak_frequency_hz: Optional[float] = None
    peak_period_seconds: Optional[float] = None
    peak_power: Optional[float] = None
    median_power: Optional[float] = None
    power_ratio: Optional[float] = None
    threshold: Optional[float] = None
    is_suspicious: bool = False
    children: List["SeriesResult"] = field(default_factory=list)
    t_series: Optional[Tuple[np.typing.NDArray]] = (np.array([]), np.array([]))
    f_series: Optional[Tuple[np.typing.NDArray]] = (np.array([]), np.array([]))


class HierarchicalFFTAnalyzer:
    """Run FFT-based anomaly detection across hierarchical label levels."""

    def __init__(
        self,
        thresholds: FFTThresholds,
        hierarchy: List[Dict[str, Iterable[str]]],
        logger: logging.Logger,
        step_seconds: int = 10,
        agg: str = "sum",
        return_series: bool = False,
        series_output_dir: Optional[str] = None,
    ):
        self.thresholds = thresholds
        self.hierarchy = hierarchy
        self.step_seconds = step_seconds
        if agg not in {"sum", "mean"}:
            raise ValueError("Aggregation must be 'sum' or 'mean'.")
        self.agg = agg
        self.return_series = return_series
        self.logger = logger
        self.series_output_dir = series_output_dir

    @staticmethod
    def _ndarray_to_json(arr) -> str:
        """Convert a numpy array to a JSON list string."""
        if arr is None or (isinstance(arr, np.ndarray) and arr.size == 0):
            return "[]"
        if isinstance(arr, np.ndarray):
            if np.issubdtype(arr.dtype, np.datetime64):
                return json.dumps([str(t) for t in arr])
            return json.dumps(arr.tolist())
        return "[]"

    def _analyze_level(
        self,
        df: pd.DataFrame,
        level_index: int,
        parent_labels: Dict[str, str],
    ) -> List[SeriesResult]:
        if level_index >= len(self.hierarchy):
            return []

        level_cfg = self.hierarchy[level_index]
        level_name = level_cfg.get("name", f"level_{level_index}")
        groupby_cols = list(level_cfg.get("groupby", []))

        if not groupby_cols:
            result = self._evaluate_series(df, level_name, parent_labels)
            children = []
            if level_index + 1 < len(self.hierarchy):
                children = self._analyze_level(df, level_index + 1, parent_labels)
            if result:
                result.children = children
                return [result]
            else:
                return children

        results: List[SeriesResult] = []
        for keys, sub_df in df.groupby(groupby_cols):
            if not isinstance(keys, tuple):
                keys = (keys,)
            label_values = parent_labels.copy()
            for col, val in zip(groupby_cols, keys):
                label_values[col] = str(val)

            result = self._evaluate_series(sub_df, level_name, label_values)
            if not result:
                continue

            if level_index + 1 < len(self.hierarchy):
                result.children = self._analyze_level(sub_df, level_index + 1, label_values)
            results.append(result)

        return results

    def _evaluate_series(
        self,
        df: pd.DataFrame,
        level_name: str,
        labels: Dict[str, str],
    ) -> Optional[SeriesResult]:
        series = self._resample(df)
        if len(series) < self.thresholds.min_samples:
            return None

        values = series.to_numpy()
        time_arr = series.index.to_numpy()

        centered = values - np.mean(values)
        if np.allclose(centered, 0):
            # Flat signal is unlikely to produce meaningful FFT output
            return SeriesResult(
                level=level_name,
                labels=labels,
                sample_size=len(values),
                mean=float(np.mean(values)),
                stddev=float(np.std(values)),
            )

        try:
            magnitude, freqs = SignalTransformations.compute_fft(
                data=centered,
                time_arr=time_arr,
                window_type=self.thresholds.window_type,
            )

        except ValueError:
            freqs = np.fft.fftfreq(len(centered), d=self.step_seconds)
            magnitude = np.abs(np.fft.fft(SignalTransformations.apply_window(centered, self.thresholds.window_type)))

        spectrum = np.abs(magnitude)

        if len(spectrum) <= 1:
            return None

        # Ignore the low-frequency portion using a percentage
        offset = int(np.floor(self.thresholds.offset_pct * len(magnitude)))
        tail = spectrum[offset:] if offset < len(spectrum) else spectrum

        peak_index = int(np.argmax(tail))
        peak_freq = float(tail[peak_index])
        peak_power = float(tail[peak_index])
        median_power = float(np.median(tail))
        ratio = float(peak_power / (median_power + 1e-9))
        peak_period = float(1.0 / peak_freq) if peak_freq > 0 else None

        threshold = SignalTransformations.compute_fft_threshold(
            fft_mag=spectrum,
            offset_pct=self.thresholds.offset_pct,
            threshold_factor=self.thresholds.threshold_factor,
        )
        is_suspicious = SignalTransformations.is_suspicious_signal(
            fft_mag=spectrum,
            threshold=threshold,
            offset_pct=self.thresholds.offset_pct,
        )

        result = SeriesResult(
            level=level_name,
            labels=labels,
            sample_size=len(values),
            mean=float(np.mean(values)),
            stddev=float(np.std(values)),
            peak_frequency_hz=peak_freq,
            peak_period_seconds=peak_period,
            peak_power=peak_power,
            median_power=median_power,
            power_ratio=ratio,
            threshold=threshold,
            is_suspicious=is_suspicious,
            # t_series=(time_arr, values),
            # f_series=(freqs, spectrum),
        )

        self.logger.debug(
            "FFT analysis completed",
            extra={
                "event": "fft_analysis_completed",
                "peak_period_seconds": peak_period,
                "power_ratio": ratio,
                "threshold": threshold,
                "is_suspicious": is_suspicious,
            },
        )

        if self.return_series:
            result.t_series = (time_arr, values)
            result.f_series = (freqs, spectrum)

        return result

    def _resample(self, df: pd.DataFrame) -> pd.Series:
        series = df.set_index("time")["value"].astype(float)
        rule = f"{self.step_seconds}s"
        if self.agg == "sum":
            series = series.resample(rule).sum()
        else:
            series = series.resample(rule).mean()
        series = series.interpolate(method="time")
        series = series.bfill().ffill()
        return series
